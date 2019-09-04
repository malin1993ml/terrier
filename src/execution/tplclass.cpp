#include <algorithm>
#include <csignal>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "tbb/task_scheduler_init.h"

#include "execution/ast/ast_dump.h"
#include "execution/exec/execution_context.h"
#include "execution/exec/output.h"
#include "execution/parsing/parser.h"
#include "execution/parsing/scanner.h"
#include "execution/sema/error_reporter.h"
#include "execution/sema/sema.h"
#include "execution/sql/memory_pool.h"
#include "execution/sql/table_generator/sample_output.h"

#include "execution/sql/table_generator/table_generator.h"
#include "execution/tpl.h"
#include "execution/util/cpu_info.h"
#include "execution/util/timer.h"
#include "execution/vm/bytecode_generator.h"
#include "execution/vm/bytecode_module.h"
#include "execution/vm/llvm_engine.h"
#include "execution/vm/module.h"
#include "llvm/Support/CommandLine.h"
#include "llvm/Support/MemoryBuffer.h"
#include "storage/garbage_collector.h"

#include "loggers/loggers_util.h"
#include "settings/settings_manager.h"

#define __SETTING_GFLAGS_DEFINE__      // NOLINT
#include "settings/settings_common.h"  // NOLINT
#include "settings/settings_defs.h"    // NOLINT
#undef __SETTING_GFLAGS_DEFINE__       // NOLINT

#include "execution/tplclass.h"

// ---------------------------------------------------------
// CLI options
// ---------------------------------------------------------

llvm::cl::OptionCategory kTplOptionsCategory("TPL Compiler Options",
                                             "Options for controlling the TPL compilation process.");
llvm::cl::opt<std::string> kInputFile(llvm::cl::Positional, llvm::cl::desc("<input file>"), llvm::cl::init(""),
                                      llvm::cl::cat(kTplOptionsCategory));
llvm::cl::opt<bool> kPrintAst("print-ast", llvm::cl::desc("Print the programs AST"),
                              llvm::cl::cat(kTplOptionsCategory));
llvm::cl::opt<bool> kPrintTbc("print-tbc", llvm::cl::desc("Print the generated TPL Bytecode"),
                              llvm::cl::cat(kTplOptionsCategory));
llvm::cl::opt<std::string> kOutputName("output-name", llvm::cl::desc("Print the output name"),
                                       llvm::cl::init("schema1"), llvm::cl::cat(kTplOptionsCategory));
llvm::cl::opt<bool> kIsSQL("sql", llvm::cl::desc("Is the input a SQL query?"), llvm::cl::cat(kTplOptionsCategory));

tbb::task_scheduler_init scheduler;

namespace terrier::execution {

    void TplClass::RunFile(const std::string &filename,
                         double *interp_exec_ms_sum,
                         uint64_t *interp_exec_ms_cnt,
                         double *adaptive_exec_ms_sum,
                         uint64_t *adaptive_exec_ms_cnt,
                         double *jit_exec_ms_sum,
                         uint64_t *jit_exec_ms_cnt,
                         bool interp, bool adaptive, bool jit) {

        auto *txn = txn_manager_pointer_->BeginTransaction();
        auto output_schema = sample_output_pointer_->GetSchema(kOutputName.data());
        exec::OutputPrinter printer(output_schema);
        auto accessor = std::unique_ptr<terrier::catalog::CatalogAccessor>(catalog_pointer_->GetAccessor(txn, db_oid_));

        exec::ExecutionContext exec_ctx{db_oid_, txn, printer, output_schema, std::move(accessor)};

        double parse_ms = 0.0, typecheck_ms = 0.0, codegen_ms = 0.0, interp_exec_ms = 0.0, adaptive_exec_ms = 0.0,
                jit_exec_ms = 0.0;

        //-----------------------------------------
        // Let's scan the source
        util::Region region("repl-ast");
        util::Region error_region("repl-error");
        sema::ErrorReporter error_reporter(&error_region);
        ast::Context context(&region, &error_reporter);

        auto itr = modules_.find(filename);
        if (itr == modules_.end()) {
            auto file = llvm::MemoryBuffer::getFile(filename);
            if (std::error_code error = file.getError()) {
                EXECUTION_LOG_ERROR("There was an error reading file '{}': {}", filename, error.message());
                return;
            }

            EXECUTION_LOG_INFO("Compiling and running file: {}", filename);

            const std::string &source = (*file)->getBuffer().str();

            parsing::Scanner scanner(source.data(), source.length());
            parsing::Rewriter rewriter(&context, exec_ctx.GetAccessor());
            parsing::Parser parser(&scanner, &context, &rewriter);

            //
            // Parse
            //

            ast::AstNode *root;
            {
                util::ScopedTimer<std::milli> timer(&parse_ms);
                root = parser.Parse();
            }

            if (error_reporter.HasErrors()) {
                EXECUTION_LOG_ERROR("Parsing error!");
                error_reporter.PrintErrors();
                return;
            }

            //
            // Type check
            //

            {
                util::ScopedTimer<std::milli> timer(&typecheck_ms);
                sema::Sema type_check(&context);
                type_check.Run(root);
            }

            if (error_reporter.HasErrors()) {
                EXECUTION_LOG_ERROR("Type-checking error!");
                error_reporter.PrintErrors();
                throw std::runtime_error("Type Checking Exception!");
            }

            // Dump AST
            if (kPrintAst) {
                ast::AstDump::Dump(root);
            }

            //
            // TBC generation
            //

            std::unique_ptr<vm::BytecodeModule> bytecode_module;
            {
                util::ScopedTimer<std::milli> timer(&codegen_ms);
                bytecode_module = vm::BytecodeGenerator::Compile(root, &exec_ctx, filename);
            }

            // Dump Bytecode
            if (kPrintTbc) {
                bytecode_module->PrettyPrint(&std::cout);
            }
            modules_[filename] = std::make_unique<vm::Module>(std::move(bytecode_module));
            itr = modules_.find(filename);
        }


        //
        // Interpret
        //

        if (interp) {
            util::ScopedTimer<std::milli> timer(&interp_exec_ms);

            if (kIsSQL) {
                std::function<i64(exec::ExecutionContext *)> main;
                if (!itr->second->GetFunction("main", vm::ExecutionMode::Interpret, &main)) {
                    EXECUTION_LOG_ERROR(
                            "Missing 'main' entry function with signature "
                            "(*ExecutionContext)->int64");
                    return;
                }
                auto memory = std::make_unique<sql::MemoryPool>(nullptr);
                exec_ctx.SetMemoryPool(std::move(memory));
                EXECUTION_LOG_INFO("VM main() returned: {}", main(&exec_ctx));
            } else {
                std::function<i64()> main;
                if (!itr->second->GetFunction("main", vm::ExecutionMode::Interpret, &main)) {
                    EXECUTION_LOG_ERROR("Missing 'main' entry function with signature ()->int64");
                    return;
                }
                EXECUTION_LOG_INFO("VM main() returned: {}", main());
            }
        }

        //
        // Adaptive
        //

        if (adaptive) {
            util::ScopedTimer<std::milli> timer(&adaptive_exec_ms);

            if (kIsSQL) {
                std::function<i64(exec::ExecutionContext *)> main;
                if (!itr->second->GetFunction("main", vm::ExecutionMode::Adaptive, &main)) {
                    EXECUTION_LOG_ERROR(
                            "Missing 'main' entry function with signature "
                            "(*ExecutionContext)->int64");
                    return;
                }
                auto memory = std::make_unique<sql::MemoryPool>(nullptr);
                exec_ctx.SetMemoryPool(std::move(memory));
                EXECUTION_LOG_INFO("ADAPTIVE main() returned: {}", main(&exec_ctx));
            } else {
                std::function<i64()> main;
                if (!itr->second->GetFunction("main", vm::ExecutionMode::Adaptive, &main)) {
                    EXECUTION_LOG_ERROR("Missing 'main' entry function with signature ()->int64");
                    return;
                }
                EXECUTION_LOG_INFO("ADAPTIVE main() returned: {}", main());
            }
        }

        //
        // JIT
        //
        if (jit) {

            if (kIsSQL) {
                std::function<i64(exec::ExecutionContext *)> main;
                if (!itr->second->GetFunction("main", vm::ExecutionMode::Compiled, &main)) {
                    EXECUTION_LOG_ERROR(
                            "Missing 'main' entry function with signature "
                            "(*ExecutionContext)->int64");
                    return;
                }
                auto memory = std::make_unique<sql::MemoryPool>(nullptr);
                exec_ctx.SetMemoryPool(std::move(memory));
                util::ScopedTimer<std::milli> timer(&jit_exec_ms);
                EXECUTION_LOG_INFO("JIT main() returned: {}", main(&exec_ctx));
            } else {
                std::function<i64()> main;
                if (!itr->second->GetFunction("main", vm::ExecutionMode::Compiled, &main)) {
                    EXECUTION_LOG_ERROR("Missing 'main' entry function with signature ()->int64");
                    return;
                }
                util::ScopedTimer<std::milli> timer(&jit_exec_ms);
                EXECUTION_LOG_INFO("JIT main() returned: {}", main());
            }
        }

        // Dump stats
        EXECUTION_LOG_INFO(
                "Parse: {} ms, Type-check: {} ms, Code-gen: {} ms, Interp. Exec.: {} ms, "
                "Adaptive Exec.: {} ms, Jit+Exec.: {} ms",
                parse_ms, typecheck_ms, codegen_ms, interp_exec_ms, adaptive_exec_ms, jit_exec_ms);
        txn_manager_pointer_->Commit(txn, [](void *) {}, nullptr);
        *interp_exec_ms_sum += interp_exec_ms;
        *interp_exec_ms_cnt += 1;
        *adaptive_exec_ms_sum += adaptive_exec_ms;
        *adaptive_exec_ms_cnt += 1;
        *jit_exec_ms_sum += jit_exec_ms;
        *jit_exec_ms_cnt += 1;
    }

    /**
     * Shutdown all TPL subsystems
     */
    void ShutdownTplClass() {
        vm::LLVMEngine::Shutdown();
        terrier::LoggersUtil::ShutDown();
        scheduler.terminate();
        LOG_INFO("TPL cleanly shutdown ...");
    }

    void TplClassSignalHandler(i32 sig_num) {
        if (sig_num == SIGINT) {
            ShutdownTplClass();
            exit(0);
        }
    }

    int InitTplClass(int argc, char **argv,
                            terrier::transaction::TransactionManager &txn_manager,
                            terrier::storage::BlockStore &block_store,
                            exec::SampleOutput &sample_output,
                            terrier::catalog::db_oid_t &db_oid,
                            terrier::catalog::Catalog &catalog) {  // NOLINT (bugprone-exception-escape)

        // Parse options
        llvm::cl::HideUnrelatedOptions(kTplOptionsCategory);
        llvm::cl::ParseCommandLineOptions(argc, argv);

        // Initialize a signal handler to call SignalHandler()
        struct sigaction sa;
        sa.sa_handler = &TplClassSignalHandler;
        sa.sa_flags = SA_RESTART;

        sigfillset(&sa.sa_mask);

        if (sigaction(SIGINT, &sa, nullptr) == -1) {
            EXECUTION_LOG_ERROR("Cannot handle SIGNIT: {}", strerror(errno));
            return errno;
        }

        // Init TPL
        execution::CpuInfo::Instance();
        // Actually not sure why it is not needed
        // terrier::LoggersUtil::Initialize(false);
        execution::vm::LLVMEngine::Initialize();

        EXECUTION_LOG_INFO("TPL Bytecode Count: {}", execution::vm::Bytecodes::NumBytecodes());
        EXECUTION_LOG_INFO("TPL initialized ...");
        EXECUTION_LOG_INFO("\n{}", terrier::execution::CpuInfo::Instance()->PrettyPrintInfo());
        EXECUTION_LOG_INFO("Welcome to TPL (ver. {}.{})", TPL_VERSION_MAJOR, TPL_VERSION_MINOR);

        auto txn = txn_manager.BeginTransaction();

        // Get the correct output format for this test
        sample_output.InitTestOutput();
        auto output_schema = sample_output.GetSchema(kOutputName.data());

        // Make the catalog accessor
        db_oid = catalog.CreateDatabase(txn, "test_db", true);
        auto accessor = std::unique_ptr<terrier::catalog::CatalogAccessor>(catalog.GetAccessor(txn, db_oid));
        auto ns_oid = accessor->GetDefaultNamespace();

        // Make the execution context
        exec::OutputPrinter printer(output_schema);
        exec::ExecutionContext exec_ctx{db_oid, txn, printer, output_schema, std::move(accessor)};

        // Generate test tables
        // TODO(Amadou): Read this in from a directory. That would require boost or experimental C++ though
        sql::TableGenerator table_generator{&exec_ctx, &block_store, ns_oid};
        table_generator.GenerateTestTables();
        table_generator.GenerateTPCHTables("../sample_tpl/tables/");
        table_generator.GenerateTableFromFile("../sample_tpl/tables/types1.schema", "../sample_tpl/tables/types1.data");

        txn_manager.Commit(txn, [](void *) {}, nullptr);

        return 0;
    }

}  // namespace tpl