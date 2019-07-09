#include <memory>
#include <vector>

#include "benchmark/benchmark.h"
#include "common/scoped_timer.h"
#include "util/multithread_test_util.h"

#include <util/catalog_test_util.h>

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

//#define PARTIAL_TEST

namespace terrier {

    class IndexBenchmark : public benchmark::Fixture {

    public:
        const int max_num_columns_ = 16;
        const uint32_t max_num_inserts_ = 10000000;
        const uint32_t max_num_threads_ = 36;

#ifdef PARTIAL_TEST
        const uint32_t num_inserts_list_[1] = {5000000};
        const uint32_t num_threads_list_[36] = {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,
                                               17,18,19,20,21,22,23,24,25,26,27,28,29,30,
                                               31,32,33,34,35,36};
        const int num_columns_list_[4] = {2,3,5,8};
#else
        const uint32_t num_inserts_list_[9] = {100000,    300000,    500000,    700000,
                                               1000000,   3000000,   5000000,   7000000,
                                               10000000};
#endif

        const int max_times_ = 3;

        std::vector<uint32_t> key_permutation_;

        std::default_random_engine generator_;

        const std::chrono::milliseconds gc_period_{10};
        storage::GarbageCollectorThread *gc_thread_;

        storage::BlockStore block_store_{100000, 100000};
        storage::RecordBufferSegmentPool buffer_pool_{1000000, 1000000};

        storage::SqlTable * sql_table_;

        transaction::TransactionManager txn_manager_{&buffer_pool_, true, LOGGING_DISABLED};

        std::vector<storage::TupleSlot> tuple_slots_;


        void SetUp(const benchmark::State &state) final {
            key_permutation_.clear();
            key_permutation_.reserve(max_num_inserts_);
            srand((int)time(0));
            for (uint32_t i = 0; i < max_num_inserts_; i++) {
                key_permutation_[i] = ((uint32_t)rand() << 16) | (uint32_t)rand();
            }
            std::shuffle(key_permutation_.begin(), key_permutation_.end(), generator_);

            char column_name[] = "A_attribute";
            std::vector<catalog::Schema::Column> columns;
            std::vector<catalog::col_oid_t> col_oids;

            for (int i = 0; i < max_num_columns_; i++) {
                columns.push_back({column_name, type::TypeId::INTEGER, false, catalog::col_oid_t(i)});
                col_oids.push_back(catalog::col_oid_t(i));
                column_name[0]++;
            }

            const catalog::Schema table_schema{catalog::Schema(columns)};

            // SqlTable
            sql_table_ = new storage::SqlTable(&block_store_, table_schema, catalog::table_oid_t(1));
            storage::ProjectedRowInitializer tuple_initializer_{
                    sql_table_->InitializerForProjectedRow(col_oids).first};

            gc_thread_ = new storage::GarbageCollectorThread(&txn_manager_, gc_period_);
            auto *const insert_txn = txn_manager_.BeginTransaction();

            for (uint32_t k = 0; k < max_num_inserts_; k++) {
                uint32_t i = key_permutation_[k];
                auto *const insert_redo =
                        insert_txn->StageWrite(CatalogTestUtil::test_db_oid, CatalogTestUtil::test_table_oid, tuple_initializer_);
                auto *const insert_tuple = insert_redo->Delta();
                for (int j = 0; j < max_num_columns_; j++)
                    *reinterpret_cast<int32_t *>(insert_tuple->AccessForceNotNull(j)) = i;
                tuple_slots_.push_back(sql_table_->Insert(insert_txn, insert_redo));
            }
            txn_manager_.Commit(insert_txn, transaction::TransactionUtil::EmptyCallback, nullptr);
            std::cout << "Finished building table" << std::endl;
        }

        void TearDown(const benchmark::State &state) final {
            delete gc_thread_;
            delete sql_table_;
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
                storage::index::IndexKeySchema key_schema_;

                for (int i = 0; i < num_columns; i++)
                    key_schema_.push_back({catalog::indexkeycol_oid_t(i), type::TypeId::BIGINT, false});


                common::WorkerPool thread_pool{num_threads, {}};

                // BwTreeIndex
                storage::index::Index * default_index = (storage::index::IndexBuilder()
                        .SetConstraintType(storage::index::ConstraintType::DEFAULT)
                        .SetKeySchema(key_schema_)
                        .SetOid(catalog::index_oid_t(2)))
                        .Build();

                gc_thread_->GetGarbageCollector().RegisterIndexForGC(default_index);

                auto workload = [&](uint32_t worker_id) {
                    auto *const key_buffer =
                            common::AllocationUtil::AllocateAligned(default_index->GetProjectedRowInitializer().ProjectedRowSize());
                    auto *const insert_key = default_index->GetProjectedRowInitializer().InitializeRow(key_buffer);
                    uint32_t num_inserts_each = num_inserts / num_threads;
                    uint32_t start_k = num_inserts_each * worker_id;
                    uint32_t end_k = num_inserts_each * (worker_id + 1);
                    if (worker_id == num_threads - 1)
                        end_k = num_inserts;

                    auto *const insert_txn = txn_manager_.BeginTransaction();
                    for (uint32_t k = start_k; k < end_k; k++) {
                        uint32_t i = key_permutation_[k];
                        for (int j = 0; j < num_columns; j++)
                            *reinterpret_cast<int32_t *>(insert_key->AccessForceNotNull(j)) = i;
                        EXPECT_TRUE(default_index->Insert(insert_txn, *insert_key, tuple_slots_[k]));
                    }
                    txn_manager_.Commit(insert_txn, transaction::TransactionUtil::EmptyCallback, nullptr);

                    delete[] key_buffer;
                };

                uint64_t elapsed_ms;
                {
                    common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);

                    // run the workload
                    for (uint32_t i = 0; i < num_threads; i++) {
                        thread_pool.SubmitTask([i, &workload] { workload(i); });
                    }
                    thread_pool.WaitUntilAllFinished();
                }

                gc_thread_->GetGarbageCollector().UnregisterIndexForGC(default_index);

                delete default_index;
                sum_time += elapsed_ms;
            }
            std::cout << num_columns << "\t" << num_threads << "\t" << num_inserts
                    << "\t" << sum_time / max_times_ << std::endl;
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
