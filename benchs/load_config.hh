#pragma once

#include <gflags/gflags.h>
#include <vector>
#include <map>

/* In our benchmark, we have three vectors to include all datasets/workloads:
  (1). exist_keys (64M), the keys we insert to warm up at first.
  (2). nonexit_keys (32M), the keys we run benchmark for insert/ operaitons.
  (3). bench_keys (10M), the keys we run benchmark for get/update/remove/ operations, 
       and all these three operations share the same index. 
*/

#include "statics.hh"

namespace bench {

// load datasets
DEFINE_uint64(nkeys, 20000000, "Number of keys to load");
DEFINE_uint64(non_nkeys, 1, "Number of non_keys for inserting");
DEFINE_uint64(bench_nkeys, 1, "Number of operations benchmarked for test");
DEFINE_string(workloads, "normal", "The workloads for evaluation");
DEFINE_string(dists, "uniform", "The request keys distribution only for ycsb");
DEFINE_double(zip_const, 0.99, "The default zipfian dist for skewness");
DEFINE_uint64(nic_idx, 2, "Which NIC to create QP");

// test config
DEFINE_uint64(mem_threads, 1, "Server threads.");
DEFINE_uint64(threads, 2, "Client threads.");
DEFINE_int32(coros, 2, "num client coroutine used per threads");
DEFINE_double(read_ratio, 1, "The ratio for reading");
DEFINE_double(insert_ratio, 0, "The ratio for writing");
DEFINE_double(update_ratio, 0, "The ratio for updating");
DEFINE_string(server_addr, "localhost:8888", "IP address of server");
DEFINE_int32(start_threads, 0, "threads numder used in this machine");
DEFINE_int32(seconds, 10, "time seconds used to run benchmark");

enum WORKLOAD {
  YCSB_A, YCSB_B, YCSB_C, YCSB_D, YCSB_E, YCSB_F, NORMAL, LOGNORMAL, BOOK, OSM, WIKI, FB
};

struct BenchmarkConfig {
  u64 nkeys;
  u64 non_nkeys;
  u64 bench_nkeys;
  i32 workloads;
  double zip_const;
  std::string dists;
  u64 mem_threads;
  u64 threads;
  i32 coros;
  double read_ratio;
  double insert_ratio;
  double update_ratio;
  std::vector<Statics> statics;
} BenConfig;

void load_benchmark_config() {
  BenConfig.nkeys     = FLAGS_nkeys;
  BenConfig.non_nkeys = FLAGS_non_nkeys;
  BenConfig.bench_nkeys = FLAGS_bench_nkeys;
  BenConfig.dists = FLAGS_dists;
  BenConfig.zip_const = FLAGS_zip_const;
  std::map<std::string, i32> workloads_map = {
    { "ycsba", YCSB_A },
    { "ycsbb", YCSB_B },
    { "ycsbc", YCSB_C },
    { "ycsbd", YCSB_D },
    { "ycsbe", YCSB_E },
    { "ycsbf", YCSB_F },
    { "normal", NORMAL },
    { "lognormal", LOGNORMAL },
    { "book", BOOK },
    { "osm", OSM },
    { "wiki", WIKI },
    { "fb", FB },
  };
  ASSERT (workloads_map.find(FLAGS_workloads) != workloads_map.end()) 
    << "unsupported workload type: " << FLAGS_workloads;
  BenConfig.workloads = workloads_map[FLAGS_workloads];
  
  BenConfig.mem_threads   = FLAGS_mem_threads;
  BenConfig.threads       = FLAGS_threads;
  BenConfig.coros         = FLAGS_coros;
  BenConfig.read_ratio    = FLAGS_read_ratio;
  BenConfig.insert_ratio  = FLAGS_insert_ratio;
  BenConfig.update_ratio  = FLAGS_update_ratio;

  BenConfig.statics.reserve(FLAGS_threads);
}


} // namespace bench
