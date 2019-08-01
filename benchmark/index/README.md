# Index Benchmark

## How to run the code

First follow the wiki of https://github.com/cmu-db/terrier to build everything. Then go to terrier/release/release folder and find bwtree_int_benchmark and index_benchmark.

The source code of bwtree_int_benchmark is benchmark/index/bwtree_int_benchmark.cpp . Type ./bwtree_int_benchmark to run it, and the result will be outputted through std::cout. See the lines containing 3 numbers separated by \t . They are thread number (<= 36), average time of 3 experiments (ms) and throughput (Mops/sec). This is the result of 50 million int64-key insertions (not pre-sorted) with third-party bwtree, which should be the basic data structure. To change the number of insertion and maximum number of threads in bwtree_int_benchmark, modify the num_keys_ and max_num_threads_ in the code.

The source code of index_benchmark is benchmark/index/index_benchmark.cpp . Type ./index_benchmark to run it, and the result will also be outputted through std::cout. See the lines containing 4 numbers separated by \t . They are column number (<= 16), thread number (<= 36), insertion number (<= 10 million) and average time of 3 experiments (ms). It uses the index wrapper and takes several columns of BIGINT as key. It first builds a sql table and then inserts index with different settings. To change the maximum number of threads and other experiment settings, modify the code according to the comments at the beginning of the class.


