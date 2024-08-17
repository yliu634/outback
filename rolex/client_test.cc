#include <random>
#include <cstring>
#include <gflags/gflags.h>

#include "r2/src/logging.hh"                  /// logging
#include "r2/src/thread.hh"                   /// Thread
#include "r2/src/libroutine.hh"               /// coroutine
#include "r2/src/rdma/async_op.hh"
#include "rlib/core/nicinfo.hh"               /// RNicInfo
#include "rlib/benchs/reporter.hh"
#include "rlib/core/lib.hh"

#include "xutils/local_barrier.hh"            /// PBarrier

#include "race/trait.hpp"
#include "benchs/rolex_util_back.hh"
#include "benchs/load_config.hh"
#include "benchs/load_data.hh"

#define READ_DATA_MODE 1
DEFINE_int64(reg_nic_name, 0, "The name to register an opened NIC at rctrl in server.");
DEFINE_int64(reg_mem_name, 73, "The name to register an MR at rctrl.");

using namespace r2;
using namespace r2::rdma;
using namespace rdmaio;
using namespace rdmaio::rmem;
using namespace rdmaio::qp;
using namespace bench;
using namespace race;

volatile bool running;
race_hash_t* race_table;
std::atomic<size_t> ready_threads(0), num(0);

rdmaio::Arc<rdmaio::RNic> nic;
rdmaio::Arc<rdmaio::qp::RC>* qps;
rdmaio::u64* auth_keys;
char** test_bufs;

namespace race {

void run_benchmark(size_t sec);
void* race_client_worker(void* param);
auto remote_read(const u64 ac_addr, rdmaio::Arc<rdmaio::qp::RC>& qp, char *test_buf, const u32 read_size, R2_ASYNC) ->::r2::Option<ValType>;
auto remote_read(const u64 ac_addr, rdmaio::Arc<rdmaio::qp::RC>& qp, char *test_buf, const u32 read_size) ->::r2::Option<ValType>;
auto remote_write(const u64 ac_addr, rdmaio::Arc<rdmaio::qp::RC>& qp, char *test_buf, const u32 read_size, R2_ASYNC) ->void;

void run_benchmark(size_t sec) {
  pthread_t threads[BenConfig.threads];
  thread_param_t thread_params[BenConfig.threads];
  // check if parameters are cacheline aligned
  for (size_t i = 0; i < BenConfig.threads; i++) {
      ASSERT ((uint64_t)(&(thread_params[i])) % CACHELINE_SIZE == 0) <<
          "wrong parameter address: " << &(thread_params[i]);
  }
  running = false;
  auth_keys = new rdmaio::u64[BenConfig.threads];
  nic = RNic::create(RNicInfo::query_dev_names().at(FLAGS_nic_idx)).value();
  
  for(size_t worker_i = 0; worker_i < BenConfig.threads; worker_i++){
      thread_params[worker_i].thread_id = worker_i;
      thread_params[worker_i].throughput = 0;
      int ret = pthread_create(&threads[worker_i], nullptr, race_client_worker,
                              (void *)&thread_params[worker_i]);
      ASSERT (ret==0) <<"Error:" << ret;
  }

  LOG(2)<<"[Wait for Connection] ...";
  while (ready_threads < BenConfig.threads) sleep(0.3);

  running = true;
  std::vector<size_t> tput_history(BenConfig.threads, 0);
  size_t current_sec = 0;
  while (current_sec < sec) {
      sleep(1);
      uint64_t tput = 0;
      double tlat = 0; // for latency, as well as in many workers modificaiton.
      for (size_t i = 0; i < BenConfig.threads; i++) {
        tput += thread_params[i].throughput - tput_history[i];
        tput_history[i] = thread_params[i].throughput;
        tlat += thread_params[i].latency; // for latency
        thread_params[i].latency = 0;     //for latency
      }
      LOG(2)<<"[micro] >>> sec " << current_sec << " throughput: " << tput << ", latency: " << tlat/tput << "us";
      ++current_sec;
  }

  running = false;
  void *status;
  for (size_t i = 0; i < BenConfig.threads; i++) {
      int rc = pthread_join(threads[i], &status);
      ASSERT (!rc) "Error:unable to join," << rc;
  }

  size_t throughput = 0;
  for (auto &p : thread_params) {
      throughput += p.throughput;
  }
  LOG(2)<<"[micro] Throughput(op/s): " << throughput / sec;

}

void* race_client_worker(void* param) {
  thread_param_t &thread_param = *(thread_param_t *)param;
  uint32_t thread_id = thread_param.thread_id;
  LOG(3) << "thread_id is: " << thread_id;

  auto qp = RC::create(nic, QPConfig()).value();
  ConnectManager cm(FLAGS_server_addr);
  if (cm.wait_ready(1000000, 2) == IOCode::Timeout) // wait 1 second for server to ready, retry 2 times
    RDMA_ASSERT(false) << "cm connect to server timeout";
  auto qp_res = cm.cc_rc("client-qp-"+std::to_string(thread_id+FLAGS_start_threads), qp, FLAGS_reg_nic_name, QPConfig());
  RDMA_ASSERT(qp_res == IOCode::Ok) << std::get<0>(qp_res.desc);
  auto key = std::get<1>(qp_res.desc);
  RDMA_LOG(4) << "client fetch QP authentical key: " << key;
  auth_keys[thread_id] = key;
  auto fetch_res = cm.fetch_remote_mr(FLAGS_reg_mem_name);
  RDMA_ASSERT(fetch_res == IOCode::Ok) << std::get<0>(fetch_res.desc);
  rmem::RegAttr remote_attr = std::get<1>(fetch_res.desc);
  qp->bind_remote_mr(remote_attr);
  RDMA_LOG(4) << "remote memory addr client gets is: " << (u64) remote_attr.buf;

  // create the local MR for usage, and create the remote MR for usage
  auto local_mem = Arc<RMem>(new RMem(1024));
  auto local_mr = RegHandler::create(local_mem, nic).value();
  qp->bind_local_mr(local_mr->get_reg_attr().value());

  //This is the example code usage of the fully created RCQP 
  char* test_buf = (char *)(qp->local_mr.value().buf);
  RDMA_LOG(4) << "local registered mem ptr: " << (u64) test_buf;


  LOG(3) << "yy: remote_read: thread: ";
  auto res = remote_read(0,qp,test_buf,64);
  LOG(3) << "remote_read passed";

  // starting benchmarking ..
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_real_distribution<> ratio_dis(0,1);

  SScheduler ssched;

  LOG(2) << "[micro] Worker: " << thread_id << " Ready.";
  ready_threads ++;

  while (!running)
  ;

  /*
   * @brief using coroutines for testing
   */ 
  for(int i=0; i<BenConfig.coros; i++) {
    ssched.spawn([&qp, test_buf, 
                  thread_id, &thread_param,
                  &ratio_dis, &gen](R2_ASYNC) {
      std::chrono::microseconds duration(0);
      RDMA_ASSERT(qp->valid());
      while(running) {
        u32 read_size(64);
        bool found(false);
        KeyType dummy_key = std::stoull(workload.NextTransactionKey().substr(4));
        auto start_time = std::chrono::high_resolution_clock::now();
        // u64 ac_addr = race_table->remote_lookup(dummy_key, read_size);
        auto res = remote_read(8,qp,test_buf,read_size,R2_ASYNC_WAIT);
        auto end_time = std::chrono::high_resolution_clock::now();
        duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
        thread_param.throughput++;
        thread_param.latency += static_cast<double>(duration.count());
      }
      if (R2_COR_ID() == BenConfig.coros) {
        R2_STOP();
      }
      R2_RET;
    });
  }
  ssched.run();
  pthread_exit(nullptr);
  
}

auto remote_read(const u64 ac_addr, 
                  rdmaio::Arc<rdmaio::qp::RC>& qp, 
                  char *test_buf,
                  const u32 read_size) ->::r2::Option<ValType> {
  auto res_s = qp->send_normal(
      {.op = IBV_WR_RDMA_READ,
        .flags = IBV_SEND_SIGNALED,
        .len = read_size,
        .wr_id = 0},
      {.local_addr = reinterpret_cast<RMem::raw_ptr_t>(test_buf),
        .remote_addr = ac_addr,
        .imm_data = 0});
  RDMA_ASSERT(res_s == IOCode::Ok);
  auto res_p = qp->wait_one_comp();
  RDMA_ASSERT(res_p == IOCode::Ok);

  ReplyValue r = *(reinterpret_cast<ReplyValue*>(test_buf));
  if (r.status) {
    return (ValType)r.val;
  }
  return {};
}

auto remote_read(const u64 ac_addr, 
                  rdmaio::Arc<rdmaio::qp::RC>& qp, 
                  char *test_buf,
                  const u32 read_size,
                  R2_ASYNC) ->::r2::Option<ValType> {
  /*AsyncOp<1> op;
  op.set_rdma_addr(reinterpret_cast<u64>(0), qp->remote_mr.value())
        .set_read()
        .set_payload(static_cast<const u64 *>(test_buf), read_size,
                     qp->local_mr.value().lkey);

    auto ret = op.execute_async(this->qp, IBV_SEND_SIGNALED, R2_ASYNC_WAIT);*/
  auto res_s = qp->send_normal(
      {.op = IBV_WR_RDMA_READ,
        .flags = IBV_SEND_SIGNALED,
        .len = read_size,
        .wr_id = 0},
      {.local_addr = reinterpret_cast<RMem::raw_ptr_t>(test_buf),
        .remote_addr = ac_addr,
        .imm_data = 0});
  RDMA_ASSERT(res_s == IOCode::Ok);
  auto res_p = qp->wait_one_comp();
  R2_YIELD;
  RDMA_ASSERT(res_p == IOCode::Ok);

  ReplyValue r = *(reinterpret_cast<ReplyValue*>(test_buf));
  if (r.status)
    return (ValType)r.val;
  return {};
}

}


int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  LOG(2) << "[Loading data] ...";
  bench::load_benchmark_config();
  bench::load_data();

  LOG(2) << "[Setup Client Hash Table] ...";
  race_table = new race_hash_t(FLAGS_nkeys, nullptr);
  exist_keys.clear();
  
  LOG(2) << "[Run benchmrk] ...";
  run_benchmark(FLAGS_seconds);

  return 0;

}