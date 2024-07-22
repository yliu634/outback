#include <random>
#include <cstring>
#include <gflags/gflags.h>

#include "r2/src/logging.hh"                  /// logging
#include "r2/src/thread.hh"                   /// Thread
#include "r2/src/libroutine.hh"               /// coroutine
#include "rlib/core/nicinfo.hh"               /// RNicInfo
#include "rlib/benchs/reporter.hh"
#include "rlib/core/lib.hh"

#include "xutils/local_barrier.hh"            /// PBarrier

#include "race/trait.hpp"
#include "benchs/rolex_util_back.hh"
#include "benchs/load_config.hh"
#include "benchs/load_data.hh"

#define READ_DATA_MODE 1

DEFINE_int64(use_nic_idx, 0, "Which NIC to create QP");
DEFINE_int64(reg_nic_name, 0, "The name to register an opened NIC at rctrl in server.");
DEFINE_int64(reg_mem_name, 73, "The name to register an MR at rctrl.");

using namespace r2;
using namespace rdmaio;
using namespace rdmaio::rmem;
using namespace rdmaio::qp;
using namespace bench;
using namespace race;

volatile bool running;
race_hash_t* race_table;
std::atomic<size_t> ready_threads(0), num(0);


namespace race {

void run_benchmark(size_t sec);
void* race_client_worker(void* param);
auto remote_read(const u64 ac_addr, rdmaio::Arc<rdmaio::qp::RC>& qp, char *test_buf, const u32 read_size, R2_ASYNC) ->::r2::Option<ValType>;
auto remote_write(const u64 ac_addr, rdmaio::Arc<rdmaio::qp::RC>& qp, char *test_buf, const u32 read_size, R2_ASYNC) ->void;

void run_benchmark(size_t sec) {
  num = FLAGS_nkeys;
  pthread_t threads[BenConfig.threads];
  thread_param_t thread_params[BenConfig.threads];
  // check if parameters are cacheline aligned
  for (size_t i = 0; i < BenConfig.threads; i++) {
      ASSERT ((uint64_t)(&(thread_params[i])) % CACHELINE_SIZE == 0) <<
          "wrong parameter address: " << &(thread_params[i]);
  }

  running = false;
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
      for (size_t i = 0; i < BenConfig.threads; i++) {
          tput += thread_params[i].throughput - tput_history[i];
          tput_history[i] = thread_params[i].throughput;
      }
      LOG(2)<<"[micro] >>> sec " << current_sec << " throughput: " << tput;
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

  // 1. create a local QP to use
  auto nic = RNic::create(RNicInfo::query_dev_names().at(FLAGS_use_nic_idx)).value();
  auto qp = RC::create(nic, QPConfig()).value();

  // 2. create the pair QP at server using CM
  ConnectManager cm(FLAGS_server_addr);
  if (cm.wait_ready(1000000, 2) ==
      IOCode::Timeout) // wait 1 second for server to ready, retry 2 times
    RDMA_ASSERT(false) << "cm connect to server timeout";

  auto qp_res = cm.cc_rc("client-qp-"+std::to_string(thread_id+FLAGS_start_threads), qp, FLAGS_reg_nic_name, QPConfig());
  RDMA_ASSERT(qp_res == IOCode::Ok) << std::get<0>(qp_res.desc);
  auto key = std::get<1>(qp_res.desc);
  RDMA_LOG(4) << "client fetch QP authentical key: " << key;

  // 3. create the local MR for usage, and create the remote MR for usage
  auto local_mem = Arc<RMem>(new RMem(1024));
  auto local_mr = RegHandler::create(local_mem, nic).value();

  auto fetch_res = cm.fetch_remote_mr(FLAGS_reg_mem_name);
  RDMA_ASSERT(fetch_res == IOCode::Ok) << std::get<0>(fetch_res.desc);
  rmem::RegAttr remote_attr = std::get<1>(fetch_res.desc);

  qp->bind_remote_mr(remote_attr);
  qp->bind_local_mr(local_mr->get_reg_attr().value());
  RDMA_LOG(4) << "remote memory addr client gets is: " << (u64) remote_attr.buf;

  /*This is the example code usage of the fully created RCQP */
  char *test_buf = (char *)(local_mem->raw_ptr);
  
  // used for other schemes
  //size_t non_exist_key_n_per_thread = nonexist_keys.size() / BenConfig.threads;
  //size_t non_exist_key_start = thread_id * non_exist_key_n_per_thread;
  //size_t non_exist_key_end = (thread_id + 1) * non_exist_key_n_per_thread;
  //std::vector<u64> op_keys(nonexist_keys.begin() + non_exist_key_start,
  //                          nonexist_keys.begin() + non_exist_key_end);
  size_t query_i = 0, insert_i = 0, remove_i = 0, update_i = 0;

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_real_distribution<> ratio_dis(0, 1);

  SScheduler ssched;

  LOG(2) << "[micro] Worker: " << thread_id << " Ready.";
  ready_threads++;
  V dummy_value = 1234;

  while (!running)
  ;

  /**
   * @brief using coroutines for testing
   * 
   */ 
  if(bench::BenConfig.workloads >= YCSB_A) {
    for(int i=0; i<BenConfig.coros; i++) {
      ssched.spawn([&qp, test_buf, 
                    thread_id, &thread_param,
                    &ratio_dis, &gen,
                    &query_i, &insert_i, &remove_i, &update_i](R2_ASYNC) {
        while(running) {
          u32 read_size(64); 
          bool found(false);
          double d = ratio_dis(gen);
          if(d <= BenConfig.read_ratio) {  // search
            KeyType dummy_key = bench_keys[query_i];
            // LOG(4) << "queried key: " << dummy_key;
            u64 ac_addr = race_table->remote_lookup(dummy_key, read_size);
            auto res = remote_read(ac_addr,qp,test_buf,read_size,R2_ASYNC_WAIT);
            for (int k = 0; k < 2; k++) {
              cuckoo_bucket_t* bucket = reinterpret_cast<cuckoo_bucket_t*>(test_buf+k*sizeof(cuckoo_bucket_t));
              if (bucket->occupied & 0x0F != 0) {
                for (int i = 0; i < 4; ++i) {
                  //printf("fps[%d]: %u\n", i, bucket->fps[i]);
                  //printf("lens[%d]: %u\n", i, bucket->lens[i]);
                  //printf("addrs[%d]: %lu\n", i, bucket->addrs[i]);
                  if (race_table->fingerprint(dummy_key) == bucket->fps[i]){// &&
                      //bucket->addrs[i] < FLAGS_nkeys) {
                    uint64_t pd_addr = race_table->total_size()+(bucket->addrs[i])*sizeof(packed_struct_t);
                    auto res = remote_read(pd_addr,qp,test_buf,sizeof(packed_struct_t),R2_ASYNC_WAIT);
                    packed_struct_t* packed_struct = reinterpret_cast<packed_struct_t*>(test_buf);
                    ValType val = packed_struct->data;
                    found = true;
                    break;
                  }
                }
              }
              if (found) break;
            }
            query_i++;
            if (unlikely(query_i == bench_keys.size())) {
              query_i = 0;
            }
          } else if(d <= BenConfig.read_ratio+BenConfig.insert_ratio) {  // insert
            KeyType dummy_key = nonexist_keys[insert_i];
            ValType dummy_value = dummy_key;
            u64 ac_addr = race_table->remote_lookup(dummy_key, read_size);
            uint8_t fp = race_table->fingerprint(dummy_key);
            auto res = remote_read(ac_addr,qp,test_buf,read_size,R2_ASYNC_WAIT);
            bool succ(false);
            for (int k = 0; k < 2; k++) {
              cuckoo_bucket_t* bucket = reinterpret_cast<cuckoo_bucket_t*>(test_buf+k*sizeof(cuckoo_bucket_t));         
              for (int i = 0; i < 4; ++i) {
                if (bucket->occupied & (1U<<i)) {
                  if ((fp==bucket->fps[i]) && (bucket->lens[i]>0)) {
                    uint64_t pd_addr = race_table->total_size()+(bucket->addrs[i])*sizeof(packed_struct_t);
                    auto res = remote_read(pd_addr,qp,test_buf+2*sizeof(cuckoo_bucket_t),sizeof(packed_struct_t),R2_ASYNC_WAIT);
                    packed_struct_t* packed_struct = reinterpret_cast<packed_struct_t*>(test_buf+2*sizeof(cuckoo_bucket_t));
                    if (packed_struct->key==dummy_key) { //UPDATE
                      packed_struct->data = dummy_value;
                      remote_write(pd_addr,qp,test_buf+2*sizeof(cuckoo_bucket_t),sizeof(packed_struct_t),R2_ASYNC_WAIT);
                      succ=true;
                      break;
                    }
                  }
                }
              }
              if (succ) break;
            }
            if (!succ) { // not update, actual insert
              for (int k = 0; k < 2; k++) {
                cuckoo_bucket_t* bucket = reinterpret_cast<cuckoo_bucket_t*>(test_buf+k*sizeof(cuckoo_bucket_t));         
                for (int i = 0; i < 4; ++i) {
                  if (!(bucket->occupied & (1U<<i))) { //empty
                    bucket->occupied |= (1U<<i);
                    bucket->fps[i] = fp;
                    bucket->lens[i] = 16;
                    bucket->addrs[i] = num;
                    // write testbuf, and the following read_size back;
                    remote_write(ac_addr,qp,test_buf,read_size,R2_ASYNC_WAIT);
                    // write packed struct back;
                    packed_struct_t packed_struct = {dummy_key,64,dummy_key};
                    memcpy(test_buf,reinterpret_cast<void*>(&packed_struct),sizeof(packed_struct_t));
                    u64 pd_addr = race_table->total_size()+num*sizeof(packed_struct_t);
                    remote_write(pd_addr,qp,test_buf,sizeof(packed_struct_t),R2_ASYNC_WAIT);
                    succ = true;
                    break;
                  }
                }
                if (succ) break;
              }
              num++;
            }
            insert_i++;
            if (unlikely(insert_i == nonexist_keys.size())) {
              insert_i = 0;
            }
          } else if(d<=BenConfig.read_ratio+BenConfig.insert_ratio+BenConfig.update_ratio) {  // update
            KeyType dummy_key = bench_keys[update_i];
            ValType dummy_value = dummy_key;
            u64 ac_addr = race_table->remote_lookup(dummy_key, read_size);
            auto res = remote_read(ac_addr, qp, test_buf, read_size, R2_ASYNC_WAIT);
            bool succ(false);
            for (int k = 0; k < 2; k++) {
              cuckoo_bucket_t* bucket = reinterpret_cast<cuckoo_bucket_t*>(test_buf+k*sizeof(cuckoo_bucket_t));
              if (bucket->occupied & 0x0F != 0) {
                for (int i = 0; i < 4; ++i) {
                  if (race_table->fingerprint(dummy_key) == bucket->fps[i]) {
                    uint64_t pd_addr = race_table->total_size()+(bucket->addrs[i])*sizeof(packed_struct_t);
                    auto res = remote_read(pd_addr, qp, test_buf, sizeof(packed_struct_t), R2_ASYNC_WAIT);
                    packed_struct_t* packed_struct = reinterpret_cast<packed_struct_t*>(test_buf);
                    packed_struct->data = dummy_key;
                    remote_write(pd_addr, qp, test_buf, sizeof(packed_struct_t), R2_ASYNC_WAIT);
                    succ = true;
                    break;
                  }
                }
              }
              if (succ) break;
            }
            update_i++;
            if (unlikely(update_i == bench_keys.size())) {
                update_i = 0;
            }
          } else {
            KeyType dummy_key = bench_keys[remove_i];
            //remote_remove(dummy_key, rpc, sender, R2_ASYNC_WAIT);
            remove_i++;
            if (unlikely(remove_i == bench_keys.size())) {
                remove_i = 0;
            }
          }
          thread_param.throughput++;
        }
        if (R2_COR_ID() == BenConfig.coros) {
          R2_STOP();
        }
        R2_RET;
      });
    }
  }
  ssched.run();
  /***********************************************************/

  // finally, some clean up, to delete my created QP at server
  auto del_res = cm.delete_remote_rc("client-qp-"+std::to_string(thread_id+FLAGS_start_threads), key);
  RDMA_ASSERT(del_res == IOCode::Ok)
      << "delete remote QP error: " << del_res.desc;

  // RDMA_LOG(4) << "client returns: " << thread_id;
  pthread_exit(nullptr);

}

auto remote_write(const u64 ac_addr, 
                rdmaio::Arc<rdmaio::qp::RC>& qp, 
                char *test_buf,
                const u32 read_size,
                R2_ASYNC) ->void {
  auto res_s = qp->send_normal(
      {.op = IBV_WR_RDMA_WRITE,
       .flags = IBV_SEND_SIGNALED,
       .len = read_size, // bytes
       .wr_id = 0},
      {.local_addr = reinterpret_cast<RMem::raw_ptr_t>(test_buf),
       .remote_addr = ac_addr,
       .imm_data = 0});
  RDMA_ASSERT(res_s == IOCode::Ok);
  auto res_p = qp->wait_one_comp();
  RDMA_ASSERT(res_p == IOCode::Ok);
}

auto remote_read(const u64 ac_addr, 
                  rdmaio::Arc<rdmaio::qp::RC>& qp, 
                  char *test_buf,
                  const u32 read_size,
                  R2_ASYNC) ->::r2::Option<ValType> {
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

  // RDMA_LOG(4) << "fetch one value from server : 0x" << std::hex <<  *test_buf;
  ReplyValue r = *(reinterpret_cast<ReplyValue*>(test_buf));
  if (r.status) {
    return (ValType)r.val;
  }
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
  
  LOG(2) << "[Run benchmrk] ...";
  run_benchmark(FLAGS_seconds);
  
  return 0;

}
