// Whether pin to core, only for GC now. TODO : discuss it later
#define MY_PIN_TO_CORE

#include <memory>
#include <numeric>
#include <sched.h>
#include <vector>

#include "benchmark/benchmark.h"
#include "common/scoped_timer.h"
#include "util/multithread_test_util.h"

#include <util/catalog_test_util.h>
#include "parser/expression/column_value_expression.h"
#include "portable_endian/portable_endian.h"
#include "storage/garbage_collector_thread.h"
#include "storage/index/compact_ints_key.h"
#include "storage/index/index_builder.h"
#include "storage/projected_row.h"
#include "storage/sql_table.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_manager.h"
#include "type/type_id.h"
#include "type/type_util.h"
#include "util/random_test_util.h"
#include "util/storage_test_util.h"
#include "util/test_harness.h"
#include "util/transaction_test_util.h"

#include <iostream>
#include <cstdlib>
#include <ctime>
#include <unistd.h>

#include "execution/util/common.h"
#include "execution/tplclass.h"
#include "catalog/catalog.h"
#include "execution/sql/table_generator/sample_output.h"
#include "execution/util/timer.h"
#include "execution/vm/module.h"

namespace terrier {

    /*
     * Benchmark for index creation time with/without other workloads
     */
    class IndexBenchmark : public benchmark::Fixture {

    public:
        // Switches
        bool local_test_; // Whether it is a local test with small numbers
        bool scan_all_; // Whether to scan whole table; otherwise scan 1M at a time
        bool use_perf_; // Whether to use perf which needs getchar before and after main body
        bool pin_to_core_; // Whether to pin to core
        bool one_always_; // Whether always run a extra task on core 20, so always use hyper-threading
        bool single_test_; // Whether run a small test, work only if local_test_ is false
        bool need_index_; // Whether to use index
        bool need_tpch_; // Whether to use TPCH

        enum other_types {EMPTY, LOOP, ARRAY, ARRAY10M, INDEX, TPCH, SCAN} other_type_;
        std::string type_names_[7] = {"EMPTY", "LOOP", "ARRAY", "ARRAY10M", "INDEX", "TPCH", "SCAN"};
        enum workload_types {UINDEX, UTPCH} workload_type_;
        std::vector <int> tpch_list_;
        int tpch_number_;
        int tpch_repeated_times_;

        // Upper bounds
        int max_times_; // run how many experiments and record average time
        int max_num_columns_;
        uint32_t max_num_inserts_;
        uint32_t max_num_threads_;
        int big_number_for_array_test_;
        uint32_t num_inserts_per_table_;
        int scan_size_kb_; // Size for each scan when scan_all_ is false, currently 1M
        uint32_t tpch_filenum_;

        uint32_t fixed_core_id_;
        std::string scan_filename_;
        std::vector <bool> tpch_mode_;

        // List of values
        std::vector <std::string> tpch_filename_;
        std::vector <uint32_t> core_ids_;

        std::vector <uint32_t> num_inserts_list_;
        std::vector <uint32_t> num_threads_list_;
        std::vector <int> num_columns_list_;

        // Data and objects for index creation and TPCH
        std::vector<uint32_t> key_permutation_;
        std::default_random_engine generator_;

        std::chrono::milliseconds gc_period_;
        storage::GarbageCollectorThread *gc_thread_;

        storage::BlockStore block_store_{100000, 100000};
        storage::RecordBufferSegmentPool buffer_pool_{1000000, 1000000};
        std::vector <storage::SqlTable *> sql_tables_;
        transaction::TransactionManager txn_manager_{&buffer_pool_, true, LOGGING_DISABLED};
        std::vector<catalog::col_oid_t> col_oids_;
        execution::exec::SampleOutput sample_output_;
        catalog::db_oid_t db_oid_;

        // For TPCH benchmark, use another table
        execution::exec::SampleOutput sample_output_benchmark_;
        catalog::db_oid_t db_oid_benchmark_;

        std::unique_ptr<catalog::Catalog> catalog_pointer_;

        /*
         * Generate the tables for index creation
         * Used in Setup function
         */
        void GenerateTablesForIndex() {
            // Prepare data for tables for indexing
            key_permutation_.clear();
            uint32_t total_num_inserts = max_num_inserts_ * 2;
            key_permutation_.resize(total_num_inserts);
            for (uint32_t i = 0; i < total_num_inserts; i++) {
                key_permutation_[i] = i;
            }
            // for random data
            std::shuffle(key_permutation_.begin(), key_permutation_.end(), generator_);

            // Initialization of tables for indexing
            char column_name[] = "A_attribute";
            std::vector<catalog::Schema::Column> columns;
            col_oids_.clear();

            for (int i = 0; i < max_num_columns_; i++) {
                auto col = catalog::Schema::Column(
                        "attribute", type::TypeId::BIGINT, false,
                        parser::ConstantValueExpression(type::TransientValueFactory::GetNull(type::TypeId::BIGINT)));
                StorageTestUtil::ForceOid(&(col), catalog::col_oid_t(i));
                columns.push_back(col);
                col_oids_.push_back(catalog::col_oid_t(i));
                column_name[0]++;
            }

            const catalog::Schema table_schema{catalog::Schema(columns)};

            // Insert data into SqlTable for indexing
            // Currently we do not have parallel scan on one table, so we have several tables for the threads to scan concurrently
            for (int table_index = 0; table_index < (int)max_num_threads_ * 2 - 2; table_index++) {
                storage::SqlTable *sql_table = new storage::SqlTable(&block_store_, table_schema);
                //, catalog::table_oid_t(1));
                storage::ProjectedRowInitializer tuple_initializer_{
                        sql_table->InitializerForProjectedRow(col_oids_).first};

                auto *const insert_txn = txn_manager_.BeginTransaction();

                for (uint32_t k = 0; k < num_inserts_per_table_; k++) {
                    uint32_t i = key_permutation_[table_index * num_inserts_per_table_ + k];
                    auto *const insert_redo =
                            insert_txn->StageWrite(CatalogTestUtil::test_db_oid, CatalogTestUtil::test_table_oid,
                                                   tuple_initializer_);
                    auto *const insert_tuple = insert_redo->Delta();
                    for (uint16_t j = 0; j < (uint16_t)max_num_columns_; j++)
                        *reinterpret_cast<int64_t *>(insert_tuple->AccessForceNotNull(j)) = (int64_t)i;
                    sql_table->Insert(insert_txn, insert_redo);
                }
                txn_manager_.Commit(insert_txn, transaction::TransactionUtil::EmptyCallback, nullptr);
                sql_tables_[table_index] = sql_table;
            }
            std::cout << "Finished building tables for index" << std::endl;
        }

        /*
         * Set upper bounds and lists for index benchmark
         */
        void SetIndex() {
            max_num_columns_ = 6;
            num_inserts_list_.clear();
            num_threads_list_.clear();
            num_columns_list_.clear();

            if (local_test_) { // local test for PC: use very small #threads, #insertions...
                max_num_inserts_ = 10000;
                num_inserts_list_.push_back(max_num_inserts_);
                num_threads_list_.push_back(2);
                num_columns_list_.push_back(max_num_columns_);
            } else { // for the server
                max_num_inserts_ = 67108864;
                if (single_test_) { // Small test for correctness of code
                    num_inserts_list_.push_back(50000000);
                    for (int i = 17; i >= 4; i -= 3)
                        num_threads_list_.push_back(i);
                    num_columns_list_.push_back(3);
                } else { // full experiment data collection
                    const uint32_t num_inserts_list[20] = {1, 16, 256, 1024, 2048, 4096, 8192, 16384,
                                                           32768, 65536, 131072, 262144, 524288,
                                                           1048576, 2097152, 4194304, 8388608,
                                                           16777216, 33554432, 67108864};
                    for (int i = 0; i < 20; i++)
                        num_inserts_list_.push_back(num_inserts_list[i]);
                    const uint32_t num_threads_list[8] = {18, 16, 12, 10, 9, 6, 4, 1};
                    if (one_always_) { // for hyper-threading performance with small #threads
                        for (int i = 4; i < 8; i++)
                            num_threads_list_.push_back(num_threads_list[i]);
                    } else { // not use hyper-threading when #threads is small than 18
                        for (int i = 0; i < 8; i++)
                            num_threads_list_.push_back(num_threads_list[i]);
                    }
                    const int num_columns_list[4] = {1, 2, 4, 6};
                    for (int i = 0; i < 4; i++)
                        num_columns_list_.push_back(num_columns_list[i]);
                }
            }
            core_ids_.clear();
            for (int i = 0; i <= 8; i++)
                core_ids_.push_back(i);
            if (!one_always_)
                core_ids_.push_back(20);
            for (int i = 21; i <= 28; i++)
                core_ids_.push_back(i);
        }

        /*
         * Set upper bounds and lists for TPCH benchmark
         */
        void SetTPCH() {
            max_num_columns_ = 6;
            num_inserts_list_.clear();
            num_columns_list_.clear();
            if (local_test_) { // local test for PC: use very small #threads, #insertions...
                max_num_inserts_ = 10000;
            } else { // for the server
                max_num_inserts_ = 1000000;
            }
            num_inserts_list_.push_back(max_num_inserts_);
            num_threads_list_.push_back(1);
            num_columns_list_.push_back(max_num_columns_);
            core_ids_.clear();
            if (local_test_) {
                core_ids_.push_back(0);
                core_ids_.push_back(1);
                core_ids_.push_back(3);
                core_ids_.push_back(2);
            } else {
                for (int i = 0; i <= 8; i++)
                    core_ids_.push_back(i);
                for (int i = 21; i <= 28; i++)
                    core_ids_.push_back(i);
                core_ids_.push_back(20);
            }
        }

        /*
         * Setup function for google benchmark
         */
        void SetUp(const benchmark::State &state) final {

            // Switches
            local_test_ = false;
            scan_all_ = false;
            use_perf_ = false;
            pin_to_core_ = true;
            one_always_ = false;
            single_test_ = true;
            need_index_ = true;
            need_tpch_ = true;

            other_type_ = INDEX;
            workload_type_ = UTPCH;

            // Initialization of upper bounds and lists
            max_times_ = 3;
            if (local_test_) {
                max_num_threads_ = 4;
                fixed_core_id_ = 2;
                big_number_for_array_test_ = 1 << 25;
            } else {
                max_num_threads_ = 18;
                fixed_core_id_ = 20;
                big_number_for_array_test_ = 1 << 28;
            }
            if (other_type_ == ARRAY10M)
                big_number_for_array_test_ = 10000000;
            if (workload_type_ == UINDEX)
                SetIndex();
            else
                SetTPCH();

            num_inserts_per_table_ = max_num_inserts_ / max_num_threads_ + 1;
            scan_size_kb_ = 1000; // useless if scan_all_ is true

            // set up TPL file names
            const std::string filenames[4] = {"../sample_tpl/tpch/q1.tpl",
                                              "../sample_tpl/tpch/q4.tpl",
                                              "../sample_tpl/tpch/q5.tpl",
                                              "../sample_tpl/tpch/q6.tpl"};
            tpch_filename_.clear();
            tpch_filenum_ = 4;
            for (int i = 0; i < 4; i++)
                tpch_filename_.push_back(filenames[i]);
            scan_filename_ = "../sample_tpl/scanall.tpl";
            for (int i = 0; i < 4; i++)
                tpch_list_.push_back(i);
            tpch_mode_.resize(3);
            tpch_mode_[0] = false; // interpret
            tpch_mode_[1] = false; // adaptive
            tpch_mode_[2] = true; // jit
            tpch_repeated_times_ = 1;

            // set up sequence of cores and GC
            std::chrono::milliseconds gc_period{10};
            gc_period_ = gc_period;
            gc_thread_ = new storage::GarbageCollectorThread(&txn_manager_, gc_period_);
            sql_tables_.resize(max_num_threads_ * 2 - 2);

            // If using INDEX workload, generate the tables
            if (need_index_)
                GenerateTablesForIndex();
            // If using TPCH or SCAN workload, initialize the table for tpl queries
            if (need_tpch_) {
                catalog_pointer_ = std::make_unique<catalog::Catalog>(&txn_manager_, &block_store_);
                const char *cmd0 = "tpl";
                // currently cmd1 is not necessary
                // const char * cmd1 = "-output-name=tpch_q1";
                const char *cmd2 = "-sql";
                const char *cmd3 = "../sample_tpl/tpch/q1.tpl";
                const char *cmd_for_tpch[3] = {cmd0, cmd2, cmd3};

                execution::TplClass::InitTplClass(3, (char **) cmd_for_tpch);
                execution::TplClass::BuildDb(txn_manager_, block_store_, sample_output_, db_oid_,
                        *catalog_pointer_, "other_db", "../sample_tpl/tables/");
                if (workload_type_ == UTPCH) {
                    execution::TplClass::BuildDb(txn_manager_, block_store_, sample_output_benchmark_, db_oid_benchmark_,
                                                 *catalog_pointer_, "benchmark_db", "../sample_tpl/benchmark_tables/");
                }
            }

            if (workload_type_ == UTPCH && single_test_ && !local_test_) { // Small test for correctness of code
                other_type_ = SCAN;
                one_always_ = false;
                max_num_threads_ = 18;
                tpch_number_ = 0;
                tpch_repeated_times_ = 5;
            }
        }

        /*
         * Teardown function for google benchmark
         */
        void TearDown(const benchmark::State &state) final {
            for (int table_index = 0; table_index < (int)max_num_threads_ * 2 - 2; table_index++) {
                delete sql_tables_[table_index];
            }
            if (need_tpch_) {
                catalog_pointer_->TearDown();
                execution::TplClass::ShutdownTplClass();
            }
            delete gc_thread_;
        }

        /*
         * pin the thread to the core
         * core_id: logical core id
         */
        inline void MyPinToCore(uint32_t core_id) {
            cpu_set_t cpu_set;
            CPU_ZERO(&cpu_set);
            CPU_SET(core_id, &cpu_set);
            int ret = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set), &cpu_set);
            if(ret != 0) {
                fprintf(stderr, "CPU setting failed...\n");
                exit(1);
            }
        }

        /*
         * function with a loop of * and +
         */
        void LoopFunction(bool *unfinished) {
            volatile int x = 1 ;
            while(*unfinished) {
                for (int i = 0; i < (1 << 30); i++)
                    x = x * 3 + 7;
                sleep(0);
            }
            volatile int y UNUSED_ATTRIBUTE = x;
        }

        /*
         * function with array enumeration
         */
        void ArrayFunction(bool *unfinished, std::vector <int> & my_array, int array_length) {
            while(*unfinished) {
                for (int i = 0; i < array_length; i++)
                    my_array[i] = my_array[i] * 3 + 7;
                sleep(0);
            }
        }

        /*
         * Initialize an index object
         */
        storage::index::Index * IndexInit(int num_columns) {
            // Initialize the index
            catalog::IndexSchema default_schema_;
            std::vector<catalog::IndexSchema::Column> keycols;

            for (int i = 0; i < num_columns; i++) {
                keycols.emplace_back(
                        "", type::TypeId::BIGINT, false,
                        parser::ColumnValueExpression(catalog::db_oid_t(0), catalog::table_oid_t(0),
                                                      catalog::col_oid_t(i)));
                StorageTestUtil::ForceOid(&(keycols[i]), catalog::indexkeycol_oid_t(i));
            }
            default_schema_ = catalog::IndexSchema(keycols, false, false, false, true);

            // BwTreeIndex
            return (storage::index::IndexBuilder()
                    .SetConstraintType(storage::index::ConstraintType::DEFAULT)
                    .SetKeySchema(default_schema_)
                    .SetOid(catalog::index_oid_t(2)))
                    .Build();
        }

        /*
         * function of index insertion for each thread
         */
        void IndexInsertion(uint32_t worker_id, storage::index::Index * default_index,
                           uint32_t num_inserts, int num_columns, uint32_t num_threads, double *insert_time_ms) {
            double thread_run_time_ms = 0;

            // Initialize the buffer
            auto *const key_buffer =
                    common::AllocationUtil::AllocateAligned(
                            default_index->GetProjectedRowInitializer().ProjectedRowSize());
            auto *const insert_key = default_index->GetProjectedRowInitializer().InitializeRow(
                    key_buffer);
            uint32_t my_num_inserts = num_inserts / num_threads;
            if (worker_id < num_inserts - my_num_inserts * num_threads)
                my_num_inserts++;
            auto *const txn = txn_manager_.BeginTransaction();

            // enumerate the tables, scan and create index
            for (int table_cnt = 0;
                 table_cnt * num_inserts_per_table_ < my_num_inserts; table_cnt++) {
                int table_index = table_cnt * num_threads + worker_id;
                storage::SqlTable *sql_table = sql_tables_[table_index];
                int num_to_insert = my_num_inserts - table_cnt * num_inserts_per_table_;
                if (num_to_insert > (int) num_inserts_per_table_)
                    num_to_insert = (int) num_inserts_per_table_;
                int num_inserted = 0;
                auto it = sql_table->begin();

                std::vector<catalog::col_oid_t> col_oids_for_use;
                col_oids_for_use.clear();
                for (int i = 0; i < num_columns; i++)
                    col_oids_for_use.push_back(col_oids_[i]);
                int num_to_scan;
                if (scan_all_)
                    num_to_scan = num_to_insert;
                else
                    num_to_scan = scan_size_kb_ / num_columns / 8;

                storage::ProjectedColumnsInitializer initializer = sql_table->InitializerForProjectedColumns(
                        col_oids_for_use, (uint32_t) num_to_scan).first;
                auto *buffer = common::AllocationUtil::AllocateAligned(
                        initializer.ProjectedColumnsSize());
                storage::ProjectedColumns *columns = initializer.Initialize(buffer);
                do {
                    sql_table->Scan(txn, &it, columns);
                    uint32_t num_read = columns->NumTuples();
                    double run_time_ms = 0;
                    {
                        // Time for index insertion is collected within this scope
                        execution::util::ScopedTimer<std::milli> timer(&run_time_ms);
                        for (uint32_t i = 0; i < num_read; i++) {
                            storage::ProjectedColumns::RowView stored = columns->InterpretAsRow(
                                    i);
                            for (uint16_t j = 0; j < (uint16_t) num_columns; j++)
                                *reinterpret_cast<int64_t *>(insert_key->AccessForceNotNull(
                                        j)) =
                                        *reinterpret_cast<int64_t *>(stored.AccessForceNotNull(
                                                j));
                            default_index->Insert(txn, *insert_key, columns->TupleSlots()[i]);
                            ++num_inserted;
                            if (num_inserted >= num_to_insert)
                                break;
                        }
                    }
                    thread_run_time_ms += run_time_ms;
                } while (num_inserted < num_to_insert && it != sql_table->end());
                delete[] buffer;
            }
            txn_manager_.Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
            *insert_time_ms = thread_run_time_ms;

            delete[] key_buffer;
        }

        /*
         * Run a bunch of benchmark
         */
        void RunBenchmark() {
            // Initialize the time recorders for TPCH workload
            std::vector < std::vector<double> > interp_exec_ms_sum_single(tpch_filenum_);
            std::vector < std::vector<double> > adaptive_exec_ms_sum_single(tpch_filenum_);
            std::vector < std::vector<double> > jit_exec_ms_sum_single(tpch_filenum_);
            std::vector < std::vector<uint64_t> > interp_exec_ms_cnt_single(tpch_filenum_);
            std::vector < std::vector<uint64_t> > adaptive_exec_ms_cnt_single(tpch_filenum_);
            std::vector < std::vector<uint64_t> > jit_exec_ms_cnt_single(tpch_filenum_);

            for (uint32_t j = 0; j < tpch_filenum_; j++) {
                interp_exec_ms_sum_single[j].resize(max_num_threads_);
                adaptive_exec_ms_sum_single[j].resize(max_num_threads_);
                jit_exec_ms_sum_single[j].resize(max_num_threads_);
                interp_exec_ms_cnt_single[j].resize(max_num_threads_);
                adaptive_exec_ms_cnt_single[j].resize(max_num_threads_);
                jit_exec_ms_cnt_single[j].resize(max_num_threads_);
            }

            // Initialize the arrays of each thread for ARRAY workload
            std::vector <int> my_arrays[max_num_threads_];
            if (other_type_ == ARRAY or other_type_ == ARRAY10M)
                for (uint32_t i = 0; i < max_num_threads_; i++) {
                    my_arrays[i].resize(big_number_for_array_test_);
                    for (int j = 0; j < big_number_for_array_test_; j++)
                        my_arrays[i][j] = 1;
                }

            // If use perf, pause before running the experiment
            if (use_perf_) {
                std::cout << "Ready, press Enter to continue" << std::endl;
                std::getchar();
            }

            for (uint32_t num_inserts : num_inserts_list_)
                for (int num_columns : num_columns_list_)
                    for (uint32_t num_threads : num_threads_list_) {
                        // Initialize the time
                        double sum_time = 0;
                        double sum_insert_time = 0;
                        double insert_time_ms[max_num_threads_];

                        for (uint32_t i = 0; i < max_num_threads_; i++)
                            for (uint32_t j = 0; j < tpch_filenum_; j++) {
                                interp_exec_ms_sum_single[j][i] = 0;
                                adaptive_exec_ms_sum_single[j][i] = 0;
                                jit_exec_ms_sum_single[j][i] = 0;
                                interp_exec_ms_cnt_single[j][i] = 0;
                                adaptive_exec_ms_cnt_single[j][i] = 0;
                                jit_exec_ms_cnt_single[j][i] = 0;
                            }

                        // Repeat the experiments for several times
                        for (int times = 1; times <= max_times_; times++) {
                            storage::index::Index *default_index;
                            if (workload_type_ == UINDEX) {
                                default_index = IndexInit(num_columns);
                                gc_thread_->GetGarbageCollector().RegisterIndexForGC(default_index);
                            }

                            common::WorkerPool workload_thread_pool{num_threads, {}};
                            common::WorkerPool other_thread_pool{max_num_threads_ - num_threads, {}};
                            bool unfinished = true;

                            // Workload for LOOP, ARRAY, ARRAY10M, TPCH or SCAN
                            auto run_other = [&](uint32_t worker_id, uint32_t core_id) {
                                if (pin_to_core_)
                                    MyPinToCore(core_id);
                                switch (other_type_) {
                                    case EMPTY:
                                        return;
                                    case LOOP:
                                        LoopFunction(&unfinished);
                                        return;
                                    case ARRAY:
                                        ArrayFunction(&unfinished, my_arrays[worker_id], big_number_for_array_test_);
                                        return;
                                    case ARRAY10M:
                                        // the total size fo arrays is 10M
                                        ArrayFunction(&unfinished, my_arrays[worker_id], big_number_for_array_test_ /
                                                                                         (max_num_threads_ - num_threads));
                                        return;
                                    case INDEX:
                                        while(unfinished) {
                                            storage::index::Index * my_index = IndexInit(num_columns);
                                            gc_thread_->GetGarbageCollector().RegisterIndexForGC(my_index);
                                            IndexInsertion(worker_id, my_index, num_inserts, num_columns, 1, &insert_time_ms[worker_id]);
                                            gc_thread_->GetGarbageCollector().UnregisterIndexForGC(my_index);
                                            delete my_index;
                                        }
                                        return;
                                    default:
                                        execution::TplClass my_tpch(&txn_manager_, &sample_output_, db_oid_,
                                                                    *catalog_pointer_, &unfinished);
                                        // useless variables
                                        double x1, x2, x3;
                                        uint64_t y1, y2, y3;
                                        if (other_type_ == TPCH) {
                                            for (int fn = worker_id % tpch_filenum_; unfinished; fn = (fn + 1) %
                                                                                                      tpch_filenum_) {
                                                my_tpch.RunFile(tpch_filename_[fn],
                                                                &x1, &y1, &x2, &y2, &x3, &y3,
                                                                tpch_mode_[0], tpch_mode_[1], tpch_mode_[2]);
                                            }
                                        } else {
                                            my_tpch.RunFile(scan_filename_,
                                                            &x1, &y1, &x2, &y2, &x3, &y3,
                                                            tpch_mode_[0], tpch_mode_[1], tpch_mode_[2]);
                                        }
                                }
                            };

                            // Index creation workload
                            auto workload = [&](uint32_t worker_id, uint32_t core_id) {
                                if (pin_to_core_)
                                    MyPinToCore(core_id);
                                switch (workload_type_) {
                                    case UINDEX:
                                        IndexInsertion(worker_id, default_index, num_inserts, num_columns, num_threads,
                                                       &insert_time_ms[worker_id]);
                                        return;
                                    case UTPCH:
                                        execution::TplClass my_tpch(&txn_manager_, &sample_output_benchmark_, db_oid_benchmark_,
                                                                    *catalog_pointer_, &unfinished);
                                        for (int i = 0; i < tpch_repeated_times_; i++) {
                                            my_tpch.RunFile(tpch_filename_[tpch_number_],
                                                            &interp_exec_ms_sum_single[tpch_number_][worker_id],
                                                            &interp_exec_ms_cnt_single[tpch_number_][worker_id],
                                                            &adaptive_exec_ms_sum_single[tpch_number_][worker_id],
                                                            &adaptive_exec_ms_cnt_single[tpch_number_][worker_id],
                                                            &jit_exec_ms_sum_single[tpch_number_][worker_id],
                                                            &jit_exec_ms_cnt_single[tpch_number_][worker_id],
                                                            tpch_mode_[0], tpch_mode_[1], tpch_mode_[2]);
                                        }
                                }
                            };

                            // Workloads except index creation
                            if (one_always_) { // always have some workload on core 20
                                uint32_t i = 0;
                                uint32_t core_id = fixed_core_id_;
                                other_thread_pool.SubmitTask([i, core_id, &run_other] { run_other(i, core_id); });
                            } else if (other_type_ != EMPTY) {
                                for (uint32_t i = 0; i < max_num_threads_ - num_threads; i++) {
                                    uint32_t core_id = core_ids_[i + num_threads];
                                    other_thread_pool.SubmitTask(
                                            [i, core_id, &run_other] { run_other(i, core_id); });
                                }
                            }

                            double elapsed_ms;
                            {
                                // Index creation workloads
                                execution::util::ScopedTimer<std::milli> timer(&elapsed_ms);
                                // run the workload
                                for (uint32_t i = 0; i < num_threads; i++) {
                                    uint32_t core_id = core_ids_[i];
                                    workload_thread_pool.SubmitTask([i, core_id, &workload] { workload(i, core_id); });
                                }
                                workload_thread_pool.WaitUntilAllFinished();
                            }
                            unfinished = false;
                            other_thread_pool.WaitUntilAllFinished();

                            if (workload_type_ == UINDEX) {
                                gc_thread_->GetGarbageCollector().UnregisterIndexForGC(default_index);
                                delete default_index;
                            }

                            // Record the time
                            sum_time += elapsed_ms;
                            double max_insert_time = 0;
                            for (uint32_t i = 0; i < num_threads; i++)
                                if (insert_time_ms[i] > max_insert_time)
                                    max_insert_time = insert_time_ms[i];
                            sum_insert_time += max_insert_time;
                        }

                        // output format: keysize, threadnum, inertnum, time including scan, time without scan (split by \t)
                        if (workload_type_ == UINDEX) {
                            std::cout << "bwtree_time" << "\t" << num_columns << "\t" << num_threads << "\t"
                                      << num_inserts << "\t" << sum_time / max_times_ / 1000.0
                                      << "\t" << sum_insert_time / max_times_ / 1000.0 << std::endl;
                        }

                        // Compute the time of TPCH workloads
                        double interp_exec_ms_sum[tpch_filenum_], adaptive_exec_ms_sum[tpch_filenum_], jit_exec_ms_sum[tpch_filenum_];
                        uint64_t interp_exec_ms_cnt[tpch_filenum_], adaptive_exec_ms_cnt[tpch_filenum_], jit_exec_ms_cnt[tpch_filenum_];
                        for (uint32_t j = 0; j < tpch_filenum_; j++) {
                            interp_exec_ms_sum[j] = 0;
                            interp_exec_ms_cnt[j] = 0;
                            adaptive_exec_ms_sum[j] = 0;
                            adaptive_exec_ms_cnt[j] = 0;
                            jit_exec_ms_sum[j] = 0;
                            jit_exec_ms_cnt[j] = 0;
                        }
                        for (uint32_t i = 0; i < max_num_threads_; i++)
                            for (uint32_t j = 0; j < tpch_filenum_; j++) {
                                interp_exec_ms_sum[j] += interp_exec_ms_sum_single[j][i];
                                interp_exec_ms_cnt[j] += interp_exec_ms_cnt_single[j][i];
                                adaptive_exec_ms_sum[j] += adaptive_exec_ms_sum_single[j][i];
                                adaptive_exec_ms_cnt[j] += adaptive_exec_ms_cnt_single[j][i];
                                jit_exec_ms_sum[j] += jit_exec_ms_sum_single[j][i];
                                jit_exec_ms_cnt[j] += jit_exec_ms_cnt_single[j][i];
                            }

                        // output format: filename, keysize, threadnum, insertnum, interp_time, adaptive_time, jit_time(ms) (split by \t)
                        for (uint32_t j = 0; j < tpch_filenum_; j++)
                            if (interp_exec_ms_cnt[j] > 0 || adaptive_exec_ms_cnt[j] > 0 || jit_exec_ms_cnt[j] > 0) {
                                std::cout << tpch_filename_[j] << "\t" << num_columns << "\t" << num_threads
                                          << "\t"
                                          << num_inserts << "\t" << j
                                          << "\t" << interp_exec_ms_sum[j] / (double)interp_exec_ms_cnt[j]
                                          << "\t" << adaptive_exec_ms_sum[j] / (double)adaptive_exec_ms_cnt[j]
                                          << "\t" << jit_exec_ms_sum[j] / (double)jit_exec_ms_cnt[j]
                                          << std::endl;
                            }
                    }
            // If use perf, pause before TearDown
            if (use_perf_) {
                std::cout << "Finished, press Enter to continue" << std::endl;
                std::getchar();
            }
        }
    };

    /*
     * benchmark for index creation with random data, or TPCH benchmark
     */
    BENCHMARK_DEFINE_F(IndexBenchmark, RandomInsert)(benchmark::State &state) {
        for (auto _ : state) {
            switch (workload_type_) {
                case UINDEX:
                    RunBenchmark();
                    break;
                case UTPCH:
                    if (single_test_) {
                        RunBenchmark();
                        max_num_threads_ = 18; // to delete all the tables
                        break;
                    }
                    std::cout << "Empty" << std::endl;
                    other_type_ = EMPTY;
                    one_always_ = false;
                    for (int tpch_number : tpch_list_) {
                        tpch_number_ = tpch_number;
                        RunBenchmark();
                    }

                    for (other_types other_type : {LOOP, TPCH, SCAN, INDEX}) {
                        other_type_ = other_type;
                        std::cout << "Hyper-threading " << type_names_[int(other_type)] << std::endl;
                        one_always_ = true;
                        for (int tpch_number : tpch_list_) {
                            tpch_number_ = tpch_number;
                            RunBenchmark();
                        }

                        std::cout << "Other physical cores " << type_names_[int(other_type)] << std::endl;
                        one_always_ = false;
                        if (local_test_)
                            max_num_threads_ = 3;
                        else
                            max_num_threads_ = 17;
                        for (int tpch_number : tpch_list_) {
                            tpch_number_ = tpch_number;
                            RunBenchmark();
                        }

                        std::cout << "All other cores " << type_names_[int(other_type)] << std::endl;
                        max_num_threads_++;
                        for (int tpch_number : tpch_list_) {
                            tpch_number_ = tpch_number;
                            RunBenchmark();
                        }
                    }
                    max_num_threads_ = 18; // to delete all the tables
            }
        }
    }

    BENCHMARK_REGISTER_F(IndexBenchmark, RandomInsert)
            ->Unit(benchmark::kMillisecond)
            ->MinTime(1);
}  // namespace terrier
