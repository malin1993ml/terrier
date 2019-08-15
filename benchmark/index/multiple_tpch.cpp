// Whether it is a local test with small numbers
#define LOCAL_TEST
// Use TPCH as default, do not use more than 1 replacement
// Whether remove TPCH
//#define EMPTY_TEST
// Whether use loop instead of TPCH
//#define LOOP_TEST
// Whether use array operation instead of TPCH
#define ARRAY_TEST
// Whether use 10M array in total
//#define ARRAY10M
// Whether pin to core
//#define MY_PIN_TO_CORE
// Whether to scan whole table; otherwise scan 1M at a time
//#define SCAN_ALL
// To run full experiment, comment the following line
#define PARTIAL_TEST

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
#include<stdlib.h>
#include<time.h>
#include <unistd.h>

#include "execution/tplclass.h"

namespace terrier {

    class MultipleTpchBenchmark : public benchmark::Fixture {

    public:
#ifdef LOCAL_TEST
        static const uint32_t max_num_threads_ = 4;
        const uint32_t num_threads_list_[1] = {2};
#else
        static const uint32_t max_num_threads_ = 18;
        const uint32_t num_threads_list_[18] = {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18};
#endif
#ifdef ARRAY10M
        static const int big_number_for_array_test_ = 10000000;
#else
#ifdef LOCAL_TEST
        static const int big_number_for_array_test_ = 1 << 25;
#else
        static const int big_number_for_array_test_ = 1 << 28;
#endif
#endif
        static const uint32_t tpch_filenum_ = 4;
        const std::string tpch_filename_[tpch_filenum_] = {"../sample_tpl/tpch/q1.tpl",
                                                           "../sample_tpl/tpch/q4.tpl",
                                                           "../sample_tpl/tpch/q5.tpl",
                                                           "../sample_tpl/tpch/q6.tpl"};
        static constexpr uint32_t core_ids_[18] = {0, 1, 2, 3, 4, 5, 6, 7, 8,
                                                   20, 21, 22, 23, 24, 25, 26, 27, 28};
        const char * cmd0 = "tpl";
        //const char * cmd1 = "";//"-output-name=tpch_q1";
        const char * cmd2 = "-sql";
        const char * cmd3 = "../sample_tpl/tpch/q1.tpl";
        const char * cmd_for_tpch[3] = {cmd0, cmd2, cmd3};
        const int scan_size_kb_ = 1000;

// run 3 experiments and record average time
        static const int max_times_ = 3;

        std::default_random_engine generator_;

        const std::chrono::milliseconds gc_period_{10};
        storage::GarbageCollectorThread *gc_thread_;

        storage::BlockStore block_store_{100000, 100000};
        storage::RecordBufferSegmentPool buffer_pool_{1000000, 1000000};

        transaction::TransactionManager txn_manager_{&buffer_pool_, true, LOGGING_DISABLED};

        execution::exec::SampleOutput sample_output_;
        catalog::db_oid_t db_oid_;
        catalog::Catalog *catalog_pointer_;

        std::vector<double> interp_exec_ms_[max_num_threads_][tpch_filenum_];
        std::vector<double> adaptive_exec_ms_[max_num_threads_][tpch_filenum_];
        std::vector<double> jit_exec_ms_[max_num_threads_][tpch_filenum_];

        double interp_exec_ms_sum_[max_num_threads_][tpch_filenum_];
        double adaptive_exec_ms_sum_[max_num_threads_][tpch_filenum_];
        double jit_exec_ms_sum_[max_num_threads_][tpch_filenum_];
#ifdef ARRAY_TEST
        int array_for_array_test_[max_num_threads_][big_number_for_array_test_];
#endif

        void SetUp(const benchmark::State &state) final {
#ifdef ARRAY_TEST
            /*
            for (uint32_t i = 0; i < max_num_threads_; i++)
                for (int j = 0; j < big_number_for_array_test_; j++)
                    array_for_array_test_[i][j] = 1;
                    */
            std::memset(array_for_array_test_, (int)sizeof(array_for_array_test_), 1);
#endif

            gc_thread_ = new storage::GarbageCollectorThread(&txn_manager_, gc_period_);

            catalog_pointer_ = new catalog::Catalog(&txn_manager_, &block_store_);

            execution::TplClass::InitTplClass(3, (char **)cmd_for_tpch, txn_manager_, block_store_,
                                              sample_output_, db_oid_, *catalog_pointer_);
        }

        void TearDown(const benchmark::State &state) final {
            catalog_pointer_->TearDown();
            //delete catalog_pointer_;
            execution::TplClass::ShutdownTplClass();
            delete gc_thread_;
        }
    };

// NOLINTNEXTLINE
    BENCHMARK_DEFINE_F(MultipleTpchBenchmark, MultipleRun)(benchmark::State &state) {
        for (auto _ : state) {
#ifdef PARTIAL_TEST
            for (uint32_t num_threads : num_threads_list_) {
#else
                //for (uint32_t num_threads = 1; num_threads <= max_num_threads_; num_threads++) {
#endif

                for (uint32_t i = num_threads; i < max_num_threads_; i++)
                    for (uint32_t j = 0; j < tpch_filenum_; j++) {
                        interp_exec_ms_[i][j].clear();
                        adaptive_exec_ms_[i][j].clear();
                        jit_exec_ms_[i][j].clear();
                    }

                for (int times = 1; times <= max_times_; times++) {
                    common::WorkerPool other_thread_pool{num_threads, {}};
                    common::WorkerPool tpch_thread_pool{max_num_threads_ - num_threads, {}};

                    bool unfinished = true;
#ifdef LOOP_TEST
                    bool always_false = false;
#endif

                    auto run_other = [&](uint32_t worker_id, uint32_t core_id) {
#ifdef EMPTY_TEST
                        return;
#endif
#ifdef MY_PIN_TO_CORE
                        // Pin to core
                        cpu_set_t cpu_set;
                        CPU_ZERO(&cpu_set);
                        CPU_SET(core_id, &cpu_set);
                        int ret = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set), &cpu_set);
                        if(ret != 0) {
                            fprintf(stderr, "CPU setting failed...\n");
                            exit(1);
                        }
#endif
                        //while (unfinished) sleep(1);
                        //std::cout << "Out " << worker_id << std::endl;
                        //return;
#ifdef LOOP_TEST
                        int x = 1;
                        while(unfinished) {
                            for (int i = 0; i < (1 << 30); i++)
                                x = x * 3 + 7;
                            sleep(0);
                        }
                        if (always_false)
                            std::cout << x << std::endl;
                        return;
#endif
#ifdef ARRAY_TEST
                        while (unfinished) {
#ifdef ARRAY10M
                            for (int i = 0; unfinished; i = (i + 1) % (big_number_for_array_test_ /
                                                                       (max_num_threads_ - num_threads)))
#else
                            for (int i = 0; unfinished; i = (i + 1) % big_number_for_array_test_)
#endif
                                array_for_array_test_[worker_id][i] = array_for_array_test_[worker_id][i] * 3 + 7;
                            sleep(0);
                        }
                        return;
#endif
                    };

                    auto workload = [&](uint32_t worker_id, uint32_t core_id) {
#ifdef MY_PIN_TO_CORE
                        // Pin to core
                        cpu_set_t cpu_set;
                        CPU_ZERO(&cpu_set);
                        CPU_SET(core_id, &cpu_set);
                        int ret = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set), &cpu_set);
                        if(ret != 0) {
                            fprintf(stderr, "CPU setting failed...\n");
                            exit(1);
                        }
#endif
                        execution::TplClass my_tpch(&txn_manager_, &sample_output_, db_oid_, catalog_pointer_,
                                                    &unfinished);
                        // the vectors are cleared outside the time loop
                        for (uint32_t i = 0; i < tpch_filenum_; i++) {
                            my_tpch.RunFile(tpch_filename_[i], &interp_exec_ms_[worker_id][i],
                                            &adaptive_exec_ms_[worker_id][i],
                                            &jit_exec_ms_[worker_id][i]);
                        }
                    };


                    for (uint32_t i = 0; i < num_threads; i++) {
                        other_thread_pool.SubmitTask([i, &run_other] { run_other(i, core_ids_[i]); });
                    }

                    // run the workload
                    for (uint32_t i = num_threads; i < max_num_threads_; i++) {
                        tpch_thread_pool.SubmitTask([i, &workload] { workload(i, core_ids_[i]); });
                    }
                    tpch_thread_pool.WaitUntilAllFinished();

                    unfinished = false;
                    other_thread_pool.WaitUntilAllFinished();
                }
                double interp_exec_ms_sum[tpch_filenum_] = {0}, adaptive_exec_ms_sum[tpch_filenum_] = {
                        0}, jit_exec_ms_sum[tpch_filenum_] = {0};
                double interp_exec_ms_cnt[tpch_filenum_] = {0}, adaptive_exec_ms_cnt[tpch_filenum_] = {
                        0}, jit_exec_ms_cnt[tpch_filenum_] = {0};
                for (uint32_t i = num_threads; i < max_num_threads_; i++)
                    for (uint32_t j = 0; j < tpch_filenum_; j++) {
                        interp_exec_ms_sum_[i][j] = std::accumulate(std::begin(interp_exec_ms_[i][j]),
                                                                    std::end(interp_exec_ms_[i][j]), 0.0);
                        interp_exec_ms_sum[j] += interp_exec_ms_sum_[i][j];
                        interp_exec_ms_cnt[j] += (double) interp_exec_ms_[i][j].size();
                        adaptive_exec_ms_sum_[i][j] = std::accumulate(std::begin(adaptive_exec_ms_[i][j]),
                                                                      std::end(adaptive_exec_ms_[i][j]), 0.0);
                        adaptive_exec_ms_sum[j] += adaptive_exec_ms_sum_[i][j];
                        adaptive_exec_ms_cnt[j] += (double) adaptive_exec_ms_[i][j].size();
                        jit_exec_ms_sum_[i][j] = std::accumulate(std::begin(jit_exec_ms_[i][j]),
                                                                 std::end(jit_exec_ms_[i][j]), 0.0);
                        jit_exec_ms_sum[j] += jit_exec_ms_sum_[i][j];
                        jit_exec_ms_cnt[j] += (double) jit_exec_ms_[i][j].size();
                    }
                // keysize threadnum insertnum interp_time adaptive_time jit_time(ms)
                for (uint32_t j = 0; j < tpch_filenum_; j++) {
                    std::cout << tpch_filename_[j] << "\t" << num_threads
                              << "\t" << interp_exec_ms_sum[j] / interp_exec_ms_cnt[j]
                              << "\t" << adaptive_exec_ms_sum[j] / adaptive_exec_ms_cnt[j]
                              << "\t" << jit_exec_ms_sum[j] / jit_exec_ms_cnt[j]
                              << std::endl;
                }
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

    BENCHMARK_REGISTER_F(MultipleTpchBenchmark, MultipleRun)
            ->Unit(benchmark::kMillisecond)
                    //->UseManualTime()
            ->MinTime(3);
}  // namespace terrier
