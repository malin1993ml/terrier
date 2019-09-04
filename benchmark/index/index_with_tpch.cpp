// Whether pin to core TODO : discuss later
//#define MY_PIN_TO_CORE

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

    class IndexBenchmark : public benchmark::Fixture {

    public:
        // Switches
        bool local_test_; // Whether it is a local test with small numbers
        bool scan_all_; // Whether to scan whole table; otherwise scan 1M at a time
        bool use_perf_; // Whether to use perf which needs getchar before and after main body
        bool pin_to_core_; // Whether to pin to core
        bool one_always_; // // Whether always run one extra task

        enum {EMPTY, LOOP, ARRAY, ARRAY10M, TPCH, SCAN} workload_type_;

        int max_times_; // run how many experiments and record average time
        int max_num_columns_;
        uint32_t max_num_inserts_;
        uint32_t max_num_threads_;
        int big_number_for_array_test_;
        uint32_t num_inserts_per_table_;
        int scan_size_kb_;

        uint32_t tpch_filenum_;
        std::vector <std::string> tpch_filename_;
        std::vector <uint32_t> core_ids_;

        std::vector <uint32_t> num_inserts_list_;
        std::vector <uint32_t> num_threads_list_;
        std::vector <int> num_columns_list_;

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
        catalog::Catalog *catalog_pointer_;

        void PrepareData() {
            key_permutation_.clear();
            uint32_t total_num_inserts = max_num_inserts_ * 2;
            key_permutation_.resize(total_num_inserts);
            for (uint32_t i = 0; i < total_num_inserts; i++) {
                key_permutation_[i] = i;
            }
            std::shuffle(key_permutation_.begin(), key_permutation_.end(), generator_);

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
            gc_thread_ = new storage::GarbageCollectorThread(&txn_manager_, gc_period_);

            // SqlTable
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

            if (workload_type_ == TPCH || workload_type_ == SCAN) {
                catalog_pointer_ = new catalog::Catalog(&txn_manager_, &block_store_);

                const char *cmd0 = "tpl";
                //const char * cmd1 = "-output-name=tpch_q1";
                const char *cmd2 = "-sql";
                const char *cmd3 = "../sample_tpl/tpch/q1.tpl";
                const char *cmd_for_tpch[3] = {cmd0, cmd2, cmd3};

                execution::InitTplClass(3, (char **) cmd_for_tpch, txn_manager_, block_store_,
                                                  sample_output_, db_oid_, *catalog_pointer_);
            }
        }

        void SetUp(const benchmark::State &state) final {

            // Switches
            local_test_ = true;
            scan_all_ = false;
            use_perf_ = false;
            pin_to_core_ = false;
            one_always_ = false;

            workload_type_ = TPCH;

            // Initialization
            max_times_ = 3;
            max_num_columns_ = 6;
            num_inserts_list_.clear();
            num_threads_list_.clear();
            num_columns_list_.clear();
            if (local_test_) {
                max_num_inserts_ = 10000000;
                max_num_threads_ = 4;
                big_number_for_array_test_ = 1 << 25;

                num_inserts_list_.push_back(max_num_threads_);
                num_threads_list_.push_back(2);
                num_columns_list_.push_back(max_num_columns_);
            } else {
                max_num_inserts_ = 67108864;
                max_num_threads_ = 18;
                big_number_for_array_test_ = 1 << 28;

                const uint32_t num_inserts_list[20] = {1, 16, 256, 1024, 2048, 4096, 8192, 16384,
                                                  32768, 65536, 131072, 262144, 524288,
                                                  1048576, 2097152, 4194304, 8388608,
                                                  16777216, 33554432, 67108864};
                for (int i = 0; i < 20; i++)
                    num_inserts_list_.push_back(num_inserts_list[i]);
                const uint32_t num_threads_list[8] = {18,16,12,10,9,6,4,1};
                if (one_always_) {
                    for (int i = 4; i < 8; i++)
                        num_threads_list_.push_back(num_threads_list[i]);
                } else {
                    for (int i = 0; i < 8; i++)
                        num_threads_list_.push_back(num_threads_list[i]);
                }
                const int num_columns_list[4] = {1, 2, 4, 6};
                for (int i = 0; i < 4; i++)
                    num_columns_list_.push_back(num_columns_list[i]);
            }
            if (workload_type_ == ARRAY10M)
                big_number_for_array_test_ = 10000000;
            num_inserts_per_table_ = max_num_inserts_ / max_num_threads_ + 1;
            scan_size_kb_ = 1000; // useless if scan_all_ is true

            const std::string filenames[4] = {"../sample_tpl/tpch/q1.tpl",
                                              "../sample_tpl/tpch/q4.tpl",
                                              "../sample_tpl/tpch/q5.tpl",
                                              "../sample_tpl/tpch/q6.tpl"};
            tpch_filename_.clear();
            if (workload_type_ == TPCH) {
                tpch_filenum_ = 4;
                for (int i = 0; i < 4; i++)
                    tpch_filename_.push_back(filenames[i]);
            } else {
                tpch_filenum_ = 1;
                tpch_filename_.push_back("../sample_tpl/scanall.tpl");
            }
            core_ids_.clear();
            for (int i = 0; i <= 8; i++)
                core_ids_.push_back(i);
            if (!one_always_)
                core_ids_.push_back(20);
            for (int i = 21; i <= 28; i++)
                core_ids_.push_back(i);
            std::chrono::milliseconds gc_period{10};
            gc_period_ = gc_period;
            sql_tables_.resize(max_num_threads_ * 2 - 2);

            PrepareData();
        }

        void TearDown(const benchmark::State &state) final {
            for (int table_index = 0; table_index < (int)max_num_threads_ * 2 - 2; table_index++) {
                delete sql_tables_[table_index];
            }
            catalog_pointer_->TearDown();
            //delete catalog_pointer_;
            execution::ShutdownTplClass();
            delete gc_thread_;
        }

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

        void LoopFunction(bool *unfinished, bool always_false) {
            int x = 1;
            while(*unfinished) {
                for (int i = 0; i < (1 << 30); i++)
                    x = x * 3 + 7;
                sleep(0);
            }
            if (always_false)
                std::cout << x << std::endl;
        }

        void ArrayFunction(bool *unfinished, std::vector <int> & my_array, int array_length) {
            while(*unfinished) {
                for (int i = 0; i < array_length; i++)
                    my_array[i] = my_array[i] * 3 + 7;
                sleep(0);
            }
        }

        void IndexCreation(uint32_t worker_id, storage::index::Index * default_index,
                           uint32_t num_inserts, int num_columns, uint32_t num_threads, double *insert_time_ms) {
            double thread_run_time_ms = 0;
            auto *const key_buffer =
                    common::AllocationUtil::AllocateAligned(
                            default_index->GetProjectedRowInitializer().ProjectedRowSize());
            auto *const insert_key = default_index->GetProjectedRowInitializer().InitializeRow(
                    key_buffer);
            uint32_t my_num_inserts = num_inserts / num_threads;
            if (worker_id < num_inserts - my_num_inserts * num_threads)
                my_num_inserts++;
            auto *const txn = txn_manager_.BeginTransaction();

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
    };

// NOLINTNEXTLINE
    BENCHMARK_DEFINE_F(IndexBenchmark, RandomInsert)(benchmark::State &state) {
        for (auto _ : state) {
            std::vector < std::vector<double> > interp_exec_ms_sum_single(tpch_filenum_);
            std::vector < std::vector<double> > adaptive_exec_ms_sum_single(tpch_filenum_);
            std::vector < std::vector<double> > jit_exec_ms_sum_single(tpch_filenum_);
            std::vector < std::vector<uint64_t> > interp_exec_ms_cnt_single(tpch_filenum_);
            std::vector < std::vector<uint64_t> > adaptive_exec_ms_cnt_single(tpch_filenum_);
            std::vector < std::vector<uint64_t> > jit_exec_ms_cnt_single(tpch_filenum_);

            // Initialize the time
            for (uint32_t i = 0; i < max_num_threads_; i++)
                for (uint32_t j = 0; j < tpch_filenum_; j++) {
                    interp_exec_ms_sum_single[j].push_back(0);
                    adaptive_exec_ms_sum_single[j].push_back(0);
                    jit_exec_ms_sum_single[j].push_back(0);
                    interp_exec_ms_cnt_single[j].push_back(0);
                    adaptive_exec_ms_cnt_single[j].push_back(0);
                    jit_exec_ms_cnt_single[j].push_back(0);
                }

            std::vector <int> my_arrays[max_num_threads_];
            if (workload_type_ == ARRAY or workload_type_ == ARRAY10M)
                for (uint32_t i = 0; i < max_num_threads_; i++) {
                    my_arrays[i].resize(big_number_for_array_test_);
                    for (int j = 0; j < big_number_for_array_test_; j++)
                        my_arrays[i][j] = 1;
                }

            if (use_perf_) {
                std::cout << "Ready" << std::endl;
                std::getchar();
            }

            for (uint32_t num_inserts : num_inserts_list_)
                for (int num_columns : num_columns_list_)
                    for (uint32_t num_threads : num_threads_list_) {
                        double sum_time = 0;
                        double sum_insert_time = 0;
                        double insert_time_ms[max_num_threads_];

                        // Initialize the time
                        for (uint32_t i = num_threads; i < max_num_threads_; i++)
                            for (uint32_t j = 0; j < tpch_filenum_; j++) {
                                interp_exec_ms_sum_single[j][i] = 0;
                                adaptive_exec_ms_sum_single[j][i] = 0;
                                jit_exec_ms_sum_single[j][i] = 0;
                                interp_exec_ms_cnt_single[j][i] = 0;
                                adaptive_exec_ms_cnt_single[j][i] = 0;
                                jit_exec_ms_cnt_single[j][i] = 0;
                            }

                        for (int times = 1; times <= max_times_; times++) {

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
                            // key_schema_.push_back({catalog::indexkeycol_oid_t(i), type::TypeId::BIGINT, false});
                            default_schema_ = catalog::IndexSchema(keycols, false, false, false, true);
                            common::WorkerPool bwtree_thread_pool{num_threads, {}};

                            // BwTreeIndex
                            storage::index::Index *default_index = (storage::index::IndexBuilder()
                                    .SetConstraintType(storage::index::ConstraintType::DEFAULT)
                                    .SetKeySchema(default_schema_)
                                    .SetOid(catalog::index_oid_t(2)))
                                    .Build();

                            gc_thread_->GetGarbageCollector().RegisterIndexForGC(default_index);

                            common::WorkerPool workload_thread_pool{max_num_threads_ - num_threads, {}};
                            bool unfinished = true;
                            bool always_false = false;

                            auto run_other_workload = [&](uint32_t worker_id, uint32_t core_id) {
                                if (pin_to_core_)
                                    MyPinToCore(core_id);
                                switch (workload_type_) {
                                    case EMPTY:
                                        return;
                                    case LOOP:
                                        LoopFunction(&unfinished, always_false);
                                        return;
                                    case ARRAY:
                                        ArrayFunction(&unfinished, my_arrays[worker_id], big_number_for_array_test_);
                                        return;
                                    case ARRAY10M:
                                        ArrayFunction(&unfinished, my_arrays[worker_id], big_number_for_array_test_ /
                                                                                                     (max_num_threads_ - num_threads));
                                        return;
                                    default:
                                        execution::TplClass my_tpch(&txn_manager_, &sample_output_, db_oid_,
                                                                    catalog_pointer_,
                                                                    &unfinished);
                                        // the vectors are cleared outside the time loop
                                        for (int fn = worker_id % tpch_filenum_; unfinished; fn = (fn + 1) % tpch_filenum_) {
                                            my_tpch.RunFile(tpch_filename_[fn],
                                                            &interp_exec_ms_sum_single[fn][worker_id],
                                                            &interp_exec_ms_cnt_single[fn][worker_id],
                                                            &adaptive_exec_ms_sum_single[fn][worker_id],
                                                            &adaptive_exec_ms_cnt_single[fn][worker_id],
                                                            &jit_exec_ms_sum_single[fn][worker_id],
                                                            &jit_exec_ms_cnt_single[fn][worker_id],
                                                            false, false, true);
                                        }
                                }
                            };

                            auto workload = [&](uint32_t worker_id, uint32_t core_id) {
                                if (pin_to_core_)
                                    MyPinToCore(core_id);
                                IndexCreation(worker_id, default_index, num_inserts, num_columns, num_threads, &insert_time_ms[worker_id]);
                            };

                            if (one_always_) {
                                uint32_t i = 18;
                                uint32_t core_id = 20;
                                workload_thread_pool.SubmitTask([i, core_id, &run_other_workload] { run_other_workload(i, core_id); });
                            } else if (workload_type_ != EMPTY) {
                                for (uint32_t i = num_threads; i < max_num_threads_; i++) {
                                    uint32_t core_id = core_ids_[i];
                                    workload_thread_pool.SubmitTask(
                                            [i, core_id, &run_other_workload] { run_other_workload(i, core_id); });
                                }
                            }

                            double elapsed_ms;
                            {
                                execution::util::ScopedTimer<std::milli> timer(&elapsed_ms);
                                // run the workload
                                for (uint32_t i = 0; i < num_threads; i++) {
                                    uint32_t core_id = core_ids_[i];
                                    bwtree_thread_pool.SubmitTask([i, core_id, &workload] { workload(i, core_id); });
                                }
                                bwtree_thread_pool.WaitUntilAllFinished();
                            }
                            unfinished = false;
                            workload_thread_pool.WaitUntilAllFinished();

                            gc_thread_->GetGarbageCollector().UnregisterIndexForGC(default_index);

                            delete default_index;
                            sum_time += elapsed_ms;
                            double max_insert_time = 0;
                            for (uint32_t i = 0; i < num_threads; i++)
                                if (insert_time_ms[i] > max_insert_time)
                                    max_insert_time = insert_time_ms[i];
                            sum_insert_time += max_insert_time;
                        }

                        // keysize threadnum insertnum time(s)
                        std::cout << "bwtree_time" << "\t" << num_columns << "\t" << num_threads << "\t"
                                  << num_inserts << "\t" << sum_time / max_times_ / 1000.0
                                  << "\t" << sum_insert_time / max_times_ / 1000.0 << std::endl;

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
                        for (uint32_t i = num_threads; i < max_num_threads_; i++)
                            for (uint32_t j = 0; j < tpch_filenum_; j++) {
                                interp_exec_ms_sum[j] += interp_exec_ms_sum_single[j][i];
                                interp_exec_ms_cnt[j] += interp_exec_ms_cnt_single[j][i];
                                adaptive_exec_ms_sum[j] += adaptive_exec_ms_sum_single[j][i];
                                adaptive_exec_ms_cnt[j] += adaptive_exec_ms_cnt_single[j][i];
                                jit_exec_ms_sum[j] += jit_exec_ms_sum_single[j][i];
                                jit_exec_ms_cnt[j] += jit_exec_ms_cnt_single[j][i];
                            }
                        // keysize threadnum insertnum interp_time adaptive_time jit_time(ms)
                        for (uint32_t j = 0; j < tpch_filenum_; j++)
                            if (jit_exec_ms_cnt[j] > 0) {
                                std::cout << tpch_filename_[j] << "\t" << num_columns << "\t" << num_threads
                                          << "\t"
                                          << num_inserts << "\t" << j
                                          << "\t" << interp_exec_ms_sum[j] / (double)interp_exec_ms_cnt[j]
                                          << "\t" << adaptive_exec_ms_sum[j] / (double)adaptive_exec_ms_cnt[j]
                                          << "\t" << jit_exec_ms_sum[j] / (double)jit_exec_ms_cnt[j]
                                          << std::endl;
                            }
                    }
            if (use_perf_) {
                std::cout << "Finished" << std::endl;
                std::getchar();
            }
    }
}

    BENCHMARK_REGISTER_F(IndexBenchmark, RandomInsert)
            ->Unit(benchmark::kMillisecond)
            ->MinTime(3);
}  // namespace terrier
