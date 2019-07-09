# Index Benchmark

## Problem I met

Months ago, I modified the code from https://github.com/wangziqi2016/index-microbench/ , where I deleted PinToCore function and modified generic key into something similar to compact int key in terrier. Then I ran 50 million int64-key insertions with thread number 1-36. The throughput just increased with the increase of thread number, no matter the keys are pre-sorted or not. For compact int keys, the scalability was also quite good.

Now I am trying to gather data of index insertion time in terrier. The benchmark/index/bwtree_int_benchmark.cpp uses bwtree from third party, which should be some version of the previous index-microbench. I still ran 50 million int64-key insertions with thread number 1-36. It turned out that, when thread number reached 4 for pre-sorted keys and 10 for not pre-sorted, the throughput started to drop down. The result of benchmark/index/index_benchmark.cpp is similar, where I used the index wrapper of terrier and the keys have multiple columns.

## How to run the code

First follow the wiki of https://github.com/cmu-db/terrier to build everything. Then go to terrier/release/release folder and find bwtree_int_benchmark and index_benchmark.

The source code of bwtree_int_benchmark is benchmark/index/bwtree_int_benchmark.cpp . Type ./bwtree_int_benchmark to run it, and the result will be outputted through std::cout. See the lines containing 3 numbers separated by \t . They are thread number (<= 36), average time of 3 experiments (ms) and throughput (Mops/sec). This is the result of 50 million int64-key insertions (not pre-sorted) with third-party bwtree, which should be the basic data structure.

The source code of index_benchmark is benchmark/index/index_benchmark.cpp . Type ./index_benchmark to run it, and the result will also be outputted through std::cout. See the lines containing 4 numbers separated by \t . They are column number (<= 16), thread number (<= 36), insertion number (<= 10 million) and average time of 3 experiments (ms). It uses the index wrapper and takes several columns of BIGINT as key. It first builds a sql table and then inserts index with different settings.

## Some observations

When running the previous index-microbench, the maximum utilization of memory do not vary much from different thread number, and CPU utilization is close to 100% for all threads. In the latter experiments, when thread number is small, the utilization of memory and CPU are still normal. However, for those with dropped throughput, the memory utilizaion was quite large (sometimes more than 3 times of normal), while CPU utilization is big (close to 100%) at first for a short time, then quite small (20% - 60%) for a long time. The garbage collector of bwtree_int_benchmark should be from third-party, and that of index_benchmark.cpp should be from terrier system. I wonder if there were some problems.

It also turns out that, the thread number with dropped throughput is much smaller for pre-sorted keys than for not in my experiments. Actually the performance of pre-sorted index_benchmark is quite unstable when thread number is big, where the longest time can be 2 times of shortest with the same settings.


