#include <vector>

#include "benchmark/benchmark.h"
#include "common/scoped_timer.h"
#include "util/bwtree_test_util.h"
#include "util/multithread_test_util.h"

#include<stdlib.h>

namespace terrier {

    // Adapted from benchmarks in https://github.com/wangziqi2013/BwTree/blob/master/test/
    // Benchmark of third-party bw-tree insertion time
    class BwTreeIntBenchmark : public benchmark::Fixture {
    public:
        const uint32_t num_keys_ = 50000000; // number of insertion
        // maximum number of threads
        // num_threads for experiments range from 1 to max_num_threads_
        const uint32_t max_num_threads_ = 20;

        std::vector<int64_t> key_permutation_;
        std::default_random_engine generator_;

        void SetUp(const benchmark::State &state) final {
            key_permutation_.resize(num_keys_);
            for (uint32_t i = 0; i < num_keys_; i++) {
                key_permutation_[i] = i;
            }
            std::random_shuffle(key_permutation_.begin(), key_permutation_.end(), generator_);
        }

        void TearDown(const benchmark::State &state) final {}
    };

    BENCHMARK_DEFINE_F(BwTreeIntBenchmark, RandomInsert)(benchmark::State &state) {

        for (auto _ : state) {
            for (uint32_t num_threads = 1; num_threads <= max_num_threads_; num_threads++) {
                uint64_t sum_time = 0;
                for (int times = 0; times < 3; times++) {
                    common::WorkerPool thread_pool(num_threads, {});
                    auto *const tree = BwTreeTestUtil::GetEmptyTree();

                    auto workload = [&](uint32_t id) {
                        const uint32_t gcid = id + 1;
                        tree->AssignGCID(gcid);

                        uint32_t num_keys_each = num_keys_ / num_threads;
                        uint32_t start_key = num_keys_each * id;
                        uint32_t end_key = start_key + num_keys_each;

                        for (uint32_t i = start_key; i < end_key; i++) {
                            tree->Insert(key_permutation_[i], key_permutation_[i]);
                        }

                        tree->UnregisterThread(gcid);
                    };

                    tree->UpdateThreadLocal(num_threads + 1);

                    uint64_t elapsed_ms;
                    {
                        common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
                        MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads, workload);
                    }
                    tree->UpdateThreadLocal(1);
                    delete tree;
                    sum_time += elapsed_ms;
                }
                std::cout << num_threads << "\t" << (sum_time / 3) << "\t" << 50000.0 / (sum_time / 3) << std::endl;
            }
        }
    }


BENCHMARK_REGISTER_F(BwTreeIntBenchmark, RandomInsert)
->Unit(benchmark::kMillisecond)
->MinTime(3);
}  // namespace terrier
