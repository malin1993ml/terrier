#include <memory>
#include <vector>
#include <sched.h>

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
#include<stdlib.h>
#include<time.h>
#include <unistd.h>

#include "execution/tplclass.h"

//To run full experiment, comment the following line
#define PARTIAL_TEST

namespace terrier {

    class IndexBenchmark : public benchmark::Fixture {

    public:
// this is the maximum num_inserts, num_threads and num_columns
// for initialization and full experiment
        static const int max_num_columns_ = 5;
        static const uint32_t max_num_inserts_ = 10000000;//(2 << 27);
        static const uint32_t total_num_inserts_ = max_num_inserts_ * 2; // 2 times of maximum inserts
        static const uint32_t max_num_threads_ = 18;
        static const uint32_t num_inserts_per_table_ = max_num_inserts_ / max_num_threads_ + 1;

        static const uint32_t tpch_filenum_ = 4;
        const std::string tpch_filename_[tpch_filenum_] = {"../sample_tpl/tpch/q1.tpl",
                                                                  "../sample_tpl/tpch/q4.tpl",
                                                                  "../sample_tpl/tpch/q5.tpl",
                                                                  "../sample_tpl/tpch/q6.tpl"};
        static constexpr uint32_t core_ids_[18] = {0, 1, 2, 3, 4, 5, 6, 7, 8,
                                           20, 21, 22, 23, 24, 25, 26, 27, 28};
        const char * cmd0 = "tpl";
        const char * cmd1 = "-sql";
        const char * cmd2 = "../sample_tpl/tpch/q1.tpl";
        const char * cmd_for_tpch[3] = {cmd0, cmd1, cmd2};
        
#ifdef PARTIAL_TEST
// if not full experiment, set the list of num_inserts, num_threads and num_columns
/*
        const uint32_t num_inserts_list_[21] = {1, 16, 256, 1024, 2048, 4096, 8192, 16384,
                                               32768, 65536, 131072, 262144, 524288,
                                               1048576, 2097152, 4194304, 8388608,
                                               16777216, 33554432, 67108864, 134217728};
        const uint32_t num_threads_list_[3] = {4, 8, 12};
        const int num_columns_list_[3] = {1, 3, 5};*/
        const uint32_t num_inserts_list_[1] = {10000000};
        const uint32_t num_threads_list_[1] = {12};
        const int num_columns_list_[1] = {5};

#else
        // if run full experiment, set num_inserts_list_ only
// num_threads will range from 1 to max_num_threads_
// num_columns will range from 1 to max_num_columns_
        const uint32_t num_inserts_list_[9] = {100000,    300000,    500000,    700000,
                                               1000000,   3000000,   5000000,   7000000,
                                               10000000};
#endif

// run 3 experiments and record average time
        static const int max_times_ = 3;

        std::vector<uint32_t> key_permutation_;

        std::default_random_engine generator_;

        const std::chrono::milliseconds gc_period_{10};
        storage::GarbageCollectorThread *gc_thread_;

        storage::BlockStore block_store_{100000, 100000};
        storage::RecordBufferSegmentPool buffer_pool_{1000000, 1000000};

        storage::SqlTable * sql_tables_[max_num_threads_ * 2 - 2];

        transaction::TransactionManager txn_manager_{&buffer_pool_, true, LOGGING_DISABLED};

        std::vector<catalog::col_oid_t> col_oids_;

        tpl::exec::SampleOutput sample_output_;
        catalog::db_oid_t db_oid_;
        catalog::Catalog *catalog_pointer_;

        void SetUp(const benchmark::State &state) final {
            key_permutation_.clear();
            key_permutation_.resize(total_num_inserts_);
            for (uint32_t i = 0; i < total_num_inserts_; i++) {
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
            
            catalog_pointer_ = new catalog::Catalog(&txn_manager_, &block_store_);

            tpl::TplClass::InitTplClass(3, (char **)cmd_for_tpch, txn_manager_, block_store_,
                                        sample_output_, db_oid_, *catalog_pointer_);
        }

        void TearDown(const benchmark::State &state) final {
            for (int table_index = 0; table_index < (int)max_num_threads_ * 2 - 2; table_index++) {
                delete sql_tables_[table_index];
            }
            catalog_pointer_->TearDown();
            delete catalog_pointer_;
            tpl::TplClass::ShutdownTplClass();
            delete gc_thread_;
        }
    };

// NOLINTNEXTLINE
    BENCHMARK_DEFINE_F(IndexBenchmark, RandomInsert)(benchmark::State &state) {
        for (auto _ : state) {
#ifdef PARTIAL_TEST
            for (uint32_t num_inserts : num_inserts_list_)
                for (int num_columns : num_columns_list_)
                    for (uint32_t num_threads : num_threads_list_) {
#else
            for (uint32_t num_inserts : num_inserts_list_)
                for (int num_columns = 1; num_columns <= max_num_columns_; num_columns++)
                    for (uint32_t num_threads = 1; num_threads <= max_num_threads_; num_threads++) {
#endif
                        uint64_t sum_time = 0;
                        for (int times = 1; times <= max_times_; times++) {
                            catalog::IndexSchema default_schema_;
                            std::vector<catalog::IndexSchema::Column> keycols;

                            for (int i = 0; i < num_columns; i++) {
                                keycols.emplace_back(
                                    "", type::TypeId::BIGINT, false,
                                    parser::ColumnValueExpression(catalog::db_oid_t(0), catalog::table_oid_t(0), catalog::col_oid_t(i)));
                                StorageTestUtil::ForceOid(&(keycols[i]), catalog::indexkeycol_oid_t(i));
                            }
                                // key_schema_.push_back({catalog::indexkeycol_oid_t(i), type::TypeId::BIGINT, false});
                            default_schema_ = catalog::IndexSchema(keycols, false, false, false, true);
                            common::WorkerPool bwtree_thread_pool{num_threads, {}};
                            common::WorkerPool tpch_thread_pool{max_num_threads_ - num_threads, {}};

                            // BwTreeIndex
                            storage::index::Index * default_index = (storage::index::IndexBuilder()
                                    .SetConstraintType(storage::index::ConstraintType::DEFAULT)
                                    .SetKeySchema(default_schema_)
                                    .SetOid(catalog::index_oid_t(2)))
                                    .Build();

                            gc_thread_->GetGarbageCollector().RegisterIndexForGC(default_index);
                            bool unfinished = true;

                            auto run_my_tpch = [&](uint32_t worker_id, uint32_t core_id) {
                                // Pin to core
                                cpu_set_t cpu_set;
                                CPU_ZERO(&cpu_set);
                                CPU_SET(core_id, &cpu_set);
                                int ret = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set), &cpu_set);
                                if(ret != 0) {
                                    fprintf(stderr, "CPU setting failed...\n");
                                    exit(1);
                                }
                                //while (unfinished) sleep(1);
                                //std::cout << "Out " << worker_id << std::endl;
                                //return;

                                tpl::TplClass my_tpch(&txn_manager_, &sample_output_, db_oid_, catalog_pointer_);
                                while (unfinished) {
                                    my_tpch.RunFile(tpch_filename_[worker_id % tpch_filenum_]);
                                    std::cout << "Turn End " << worker_id << std::endl;
                                }
                            };

                            auto workload = [&](uint32_t worker_id, uint32_t core_id) {
                                // Pin to core
                                cpu_set_t cpu_set;
                                CPU_ZERO(&cpu_set);
                                CPU_SET(core_id, &cpu_set);
                                int ret = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set), &cpu_set);
                                if(ret != 0) {
                                    fprintf(stderr, "CPU setting failed...\n");
                                    exit(1);
                                }
                                sleep(10);
                                return;

                                auto *const key_buffer =
                                        common::AllocationUtil::AllocateAligned(default_index->GetProjectedRowInitializer().ProjectedRowSize());
                                auto *const insert_key = default_index->GetProjectedRowInitializer().InitializeRow(key_buffer);
                                uint32_t my_num_inserts = num_inserts / num_threads;
                                if (worker_id < num_inserts - my_num_inserts * num_threads)
                                    my_num_inserts++;
                                auto *const txn = txn_manager_.BeginTransaction();

                                for (int table_cnt = 0; table_cnt * num_inserts_per_table_ < my_num_inserts; table_cnt++) {
                                    int table_index = table_cnt * num_threads + worker_id;
                                    storage::SqlTable * sql_table = sql_tables_[table_index];
                                    int num_to_insert = my_num_inserts - table_cnt * num_inserts_per_table_;
                                    if (num_to_insert > (int)num_inserts_per_table_)
                                        num_to_insert = (int)num_inserts_per_table_;
                                    int num_inserted = 0;
                                    auto it = sql_table->begin();
                                    do {
                                        storage::ProjectedColumnsInitializer initializer = sql_table->InitializerForProjectedColumns(col_oids_, (uint32_t)num_to_insert).first;
                                        auto *buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedColumnsSize());
                                        storage::ProjectedColumns *columns = initializer.Initialize(buffer);
                                        sql_table->Scan(txn, &it, columns);
                                        uint32_t num_read = columns->NumTuples();
                                        for (uint32_t i = 0; i < num_read; i++) {
                                            storage::ProjectedColumns::RowView stored = columns->InterpretAsRow(i);
                                            for (uint16_t j = 0; j < (uint16_t)num_columns; j++)
                                                *reinterpret_cast<int64_t *>(insert_key->AccessForceNotNull(j)) =
                                                        *reinterpret_cast<int64_t *>(stored.AccessForceNotNull(j));
                                            default_index->Insert(txn, *insert_key, columns->TupleSlots()[i]);
                                            ++num_inserted;
                                            if (num_inserted >= num_to_insert)
                                                break;
                                        }
                                        delete[] buffer;
                                    } while (num_inserted < num_to_insert && it != sql_table->end());
                                }
                                txn_manager_.Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

                                delete[] key_buffer;
                            };

                            for (uint32_t i = num_threads; i < max_num_threads_; i++) {
                                tpch_thread_pool.SubmitTask([i, &run_my_tpch] { run_my_tpch(i, core_ids_[i]); });
                            }

                            uint64_t elapsed_us;
                            {
                                common::ScopedTimer<std::chrono::microseconds> timer(&elapsed_us);

                                // run the workload
                                for (uint32_t i = 0; i < num_threads; i++) {
                                    bwtree_thread_pool.SubmitTask([i, &workload] { workload(i, core_ids_[i]); });
                                }
                                bwtree_thread_pool.WaitUntilAllFinished();
                            }
                            unfinished = false;
                            std::cout << "Finished" << std::endl;
                            tpch_thread_pool.WaitUntilAllFinished();

                            gc_thread_->GetGarbageCollector().UnregisterIndexForGC(default_index);

                            delete default_index;
                            sum_time += elapsed_us;
                        }
                        std::cout << num_columns << "\t" << num_threads << "\t" << num_inserts
                                  << "\t" << (double)sum_time / max_times_ / 1000000.0 << std::endl;
                    }
        }
    }


// state.range(0) is key size
// state.range(1) is thread number
// state.range(2) is insert number
// state.range(3) is key sizeindex type id, 0 or default is bwtree

//static void CustomArguments(benchmark::internal::Benchmark* b) {
//    for (int i = 1; i <= 4; ++i)
//        for (int j = 1; j <= 4; ++j)
//            for (int k = 100000; k <= 300000; k += 100000)
//                b->Args({i, j, k, 0});
//}

    BENCHMARK_REGISTER_F(IndexBenchmark, RandomInsert)
            ->Unit(benchmark::kMillisecond)
                    //->UseManualTime()
            ->MinTime(3);
    //->Apply(CustomArguments);
    //->Args({4, 4, 100000, 0});


//// NOLINTNEXTLINE
//BENCHMARK_DEFINE_F(BwTreeBenchmark, SequentialInsert)(benchmark::State &state) {
//common::WorkerPool thread_pool(num_threads_, {});
//// NOLINTNEXTLINE
//for (auto _ : state) {
//auto *const tree = BwTreeTestUtil::GetEmptyTree();
//
//auto workload = [&](uint32_t id) {
//    const uint32_t gcid = id + 1;
//    tree->AssignGCID(gcid);
//
//    uint32_t start_key = num_keys_ / num_threads_ * id;
//    uint32_t end_key = start_key + num_keys_ / num_threads_;
//
//    for (uint32_t i = start_key; i < end_key; i++) {
//        tree->Insert(i, i);
//    }
//    tree->UnregisterThread(gcid);
//};
//
//uint64_t elapsed_ms;
//tree->UpdateThreadLocal(num_threads_ + 1);
//{
//common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
//MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads_, workload);
//}
//tree->UpdateThreadLocal(1);
//delete tree;
//state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
//}
//state.SetItemsProcessed(state.iterations() * num_keys_);
//}
//
//// NOLINTNEXTLINE
//BENCHMARK_DEFINE_F(BwTreeBenchmark, RandomInsertRandomRead)(benchmark::State &state) {
//common::WorkerPool thread_pool(num_threads_, {});
//auto *const tree = BwTreeTestUtil::GetEmptyTree();
//for (uint32_t i = 0; i < num_keys_; i++) {
//tree->Insert(key_permutation_[i], key_permutation_[i]);
//}
//
//// NOLINTNEXTLINE
//for (auto _ : state) {
//auto workload = [&](uint32_t id) {
//    const uint32_t gcid = id + 1;
//    tree->AssignGCID(gcid);
//
//    uint32_t start_key = num_keys_ / num_threads_ * id;
//    uint32_t end_key = start_key + num_keys_ / num_threads_;
//
//    std::vector<int64_t> values;
//    values.reserve(1);
//
//    for (uint32_t i = start_key; i < end_key; i++) {
//        tree->GetValue(key_permutation_[i], values);
//        values.clear();
//    }
//    tree->UnregisterThread(gcid);
//};
//
//uint64_t elapsed_ms;
//tree->UpdateThreadLocal(num_threads_ + 1);
//{
//common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
//MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads_, workload);
//}
//tree->UpdateThreadLocal(1);
//state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
//}
//
//delete tree;
//state.SetItemsProcessed(state.iterations() * num_keys_);
//}
//
//// NOLINTNEXTLINE
//BENCHMARK_DEFINE_F(BwTreeBenchmark, RandomInsertSequentialRead)(benchmark::State &state) {
//common::WorkerPool thread_pool(num_threads_, {});
//auto *const tree = BwTreeTestUtil::GetEmptyTree();
//for (uint32_t i = 0; i < num_keys_; i++) {
//tree->Insert(key_permutation_[i], key_permutation_[i]);
//}
//
//// NOLINTNEXTLINE
//for (auto _ : state) {
//auto workload = [&](uint32_t id) {
//    const uint32_t gcid = id + 1;
//    tree->AssignGCID(gcid);
//
//    uint32_t start_key = num_keys_ / num_threads_ * id;
//    uint32_t end_key = start_key + num_keys_ / num_threads_;
//
//    std::vector<int64_t> values;
//    values.reserve(1);
//
//    for (uint32_t i = start_key; i < end_key; i++) {
//        tree->GetValue(i, values);
//        values.clear();
//    }
//    tree->UnregisterThread(gcid);
//};
//
//uint64_t elapsed_ms;
//tree->UpdateThreadLocal(num_threads_ + 1);
//{
//common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
//MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads_, workload);
//}
//tree->UpdateThreadLocal(1);
//state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
//}
//
//delete tree;
//state.SetItemsProcessed(state.iterations() * num_keys_);
//}
//
//// NOLINTNEXTLINE
//BENCHMARK_DEFINE_F(BwTreeBenchmark, SequentialInsertRandomRead)(benchmark::State &state) {
//common::WorkerPool thread_pool(num_threads_, {});
//auto *const tree = BwTreeTestUtil::GetEmptyTree();
//for (uint32_t i = 0; i < num_keys_; i++) {
//tree->Insert(i, i);
//}
//
//// NOLINTNEXTLINE
//for (auto _ : state) {
//auto workload = [&](uint32_t id) {
//    const uint32_t gcid = id + 1;
//    tree->AssignGCID(gcid);
//
//    uint32_t start_key = num_keys_ / num_threads_ * id;
//    uint32_t end_key = start_key + num_keys_ / num_threads_;
//
//    std::vector<int64_t> values;
//    values.reserve(1);
//
//    for (uint32_t i = start_key; i < end_key; i++) {
//        tree->GetValue(key_permutation_[i], values);
//        values.clear();
//    }
//    tree->UnregisterThread(gcid);
//};
//
//uint64_t elapsed_ms;
//tree->UpdateThreadLocal(num_threads_ + 1);
//{
//common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
//MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads_, workload);
//}
//tree->UpdateThreadLocal(1);
//state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
//}
//
//delete tree;
//state.SetItemsProcessed(state.iterations() * num_keys_);
//}
//
//// NOLINTNEXTLINE
//BENCHMARK_DEFINE_F(BwTreeBenchmark, SequentialInsertSequentialRead)(benchmark::State &state) {
//common::WorkerPool thread_pool(num_threads_, {});
//auto *const tree = BwTreeTestUtil::GetEmptyTree();
//for (uint32_t i = 0; i < num_keys_; i++) {
//tree->Insert(i, i);
//}
//
//// NOLINTNEXTLINE
//for (auto _ : state) {
//auto workload = [&](uint32_t id) {
//    const uint32_t gcid = id + 1;
//    tree->AssignGCID(gcid);
//
//    uint32_t start_key = num_keys_ / num_threads_ * id;
//    uint32_t end_key = start_key + num_keys_ / num_threads_;
//
//    std::vector<int64_t> values;
//    values.reserve(1);
//
//    for (uint32_t i = start_key; i < end_key; i++) {
//        tree->GetValue(i, values);
//        values.clear();
//    }
//    tree->UnregisterThread(gcid);
//};
//
//uint64_t elapsed_ms;
//tree->UpdateThreadLocal(num_threads_ + 1);
//{
//common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
//MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads_, workload);
//}
//tree->UpdateThreadLocal(1);
//state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
//}
//
//delete tree;
//state.SetItemsProcessed(state.iterations() * num_keys_);
//}

//BENCHMARK_REGISTER_F(BwTreeBenchmark, RandomInsert)->Unit(benchmark::kMillisecond)->UseManualTime()->MinTime(3);
//BENCHMARK_REGISTER_F(BwTreeBenchmark, SequentialInsert)->Unit(benchmark::kMillisecond)->UseManualTime()->MinTime(3);
//BENCHMARK_REGISTER_F(BwTreeBenchmark, RandomInsertRandomRead)
//->Unit(benchmark::kMillisecond)
//->UseManualTime()
//->MinTime(3);
//BENCHMARK_REGISTER_F(BwTreeBenchmark, RandomInsertSequentialRead)
//->Unit(benchmark::kMillisecond)
//->UseManualTime()
//->MinTime(3);
//BENCHMARK_REGISTER_F(BwTreeBenchmark, SequentialInsertRandomRead)
//->Unit(benchmark::kMillisecond)
//->UseManualTime()
//->MinTime(3);
//BENCHMARK_REGISTER_F(BwTreeBenchmark, SequentialInsertSequentialRead)
//->Unit(benchmark::kMillisecond)
//->UseManualTime()
//->MinTime(3);
}  // namespace terrier
