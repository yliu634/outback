#include <random>
#include <gflags/gflags.h>

#include "r2/src/logging.hh"                  /// logging
#include "r2/src/thread.hh"                   /// Thread
#include "r2/src/libroutine.hh"               /// coroutine
#include "rlib/core/lib.hh"
#include "rlib/core/nicinfo.hh"               /// RNicInfo
#include "rlib/benchs/reporter.hh"
#include "xcomm/tests/transport_util.hh"      /// SimpleAllocator
#include "xcomm/src/transport/rdma_ud_t.hh"   /// UDTranstrant, UDRecvTransport, UDSessionManager
#include "xcomm/src/rpc/mod.hh"               /// RPCCore
#include "xutils/local_barrier.hh"            /// PBarrier

#include "mica/trait.hpp"
#include "benchs/load_config.hh"
#include "benchs/load_data.hh"
#include "benchs/rolex_util_back.hh"

using namespace r2;
using namespace rdmaio;
using namespace test;
using namespace xstore::util;
using namespace xstore::rpc;
using namespace xstore::transport;
using namespace bench;
using namespace fasst;

volatile bool running;
::xstore::util::PBarrier bar(FLAGS_threads);

using XThread = ::r2::Thread<usize>;
std::atomic<size_t> ready_threads(0);
using SendTrait = UDTransport;
using RecvTrait = UDRecvTransport<2048>;
using SManager = UDSessionManager<2048>;


namespace fasst {

void run_benchmark(size_t sec);
void* drtmr_client_worker(void* param);
using RPC = RPCCore<SendTrait, RecvTrait, SManager>;

auto remote_search(const KeyType& key, RPC& rpc, UDTransport& sender, const rmem::mr_key_t& lkey, R2_ASYNC) -> ::r2::Option<ValType>;
void remote_put(const KeyType& key, const ValType& val, RPC& rpc, UDTransport& sender, R2_ASYNC);
void remote_update(const KeyType& key, const ValType& val, RPC& rpc, UDTransport& sender, R2_ASYNC);
void remote_remove(const KeyType& key, RPC& rpc, UDTransport& sender, R2_ASYNC);
void remote_scan(const KeyType& key, const u64& n, RPC& rpc, UDTransport& sender, R2_ASYNC);
}

int main(int argc, char **argv) {

  gflags::ParseCommandLineFlags(&argc, &argv, true);

  LOG(2) << "[Loading data] ...";
  bench::load_benchmark_config();
  bench::load_data();
  exist_keys.clear();
  
  LOG(2) << "[Run benchmrk] ...";
  run_benchmark(FLAGS_seconds);
  
  return 0;
}


namespace fasst {

void run_benchmark(size_t sec) {
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
        int ret = pthread_create(&threads[worker_i], nullptr, drtmr_client_worker,
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

void* drtmr_client_worker(void* param) {
  thread_param_t &thread_param = *(thread_param_t *)param;
  uint32_t thread_id = thread_param.thread_id;
  /**
   * Prepare UD qp: create NIC, UD, allocator, post
   */ 
  // create NIC and qps
  usize nic_idx = 0;
  auto nic_for_sender = RNic::create(RNicInfo::query_dev_names().at(nic_idx)).value();
  auto ud_qp = UD::create(nic_for_sender, QPConfig()).value();
  // Register the memory
  auto mem_region1 = HugeRegion::create(16 * 1024 * 1024).value();
  auto mem1 = mem_region1->convert_to_rmem().value();
  auto handler1 = RegHandler::create(mem1, nic_for_sender).value();
  SimpleAllocator alloc1(mem1, handler1->get_reg_attr().value());
  auto recv_rs_at_send = RecvEntriesFactory<SimpleAllocator, 2048, 1024>::create(alloc1);
  {
    auto res = ud_qp->post_recvs(*recv_rs_at_send, 2048);
    RDMA_ASSERT(res == IOCode::Ok);
  }

  /**
   * @brief connect with the remote machine with UD
   * 
   */
  //std::string server_addr = "localhost:8888";
  std::string server_addr = FLAGS_server_addr;
  int ud_id = thread_id;
  UDTransport sender;
  {
    r2::Timer t;
    do {
      auto res = sender.connect(
        server_addr, "b" + std::to_string(ud_id%FLAGS_mem_threads), 
        FLAGS_start_threads+thread_id, ud_qp);
      if (res == IOCode::Ok) {
        LOG(2) << "Thread " << thread_id << " connect to remote server";
        break;
      }
      if (t.passed_sec() >= 10) {
        ASSERT(false) << "conn failed at thread:" << thread_id;
      }
    } while (t.passed_sec() < 10);
  }

  /**
   * @brief Construct rpc for communication
   * 
   */
  RPCCore<SendTrait, RecvTrait, SManager> rpc(12);
  auto send_buf = std::get<0>(alloc1.alloc_one(4096).value());
  ASSERT(send_buf != nullptr);
  auto lkey = handler1->get_reg_attr().value().key;
  memset(send_buf, 0, 4096);
  // 0. connect the RPC
  // first we send the connect transport
  auto conn_op = RPCOp::get_connect_op(MemBlock(send_buf, 2048),
                                        sender.get_connect_data().value());
  ASSERT(conn_op.execute_w_key(&sender, lkey) == IOCode::Ok);
  UDRecvTransport<2048> recv_s(ud_qp, recv_rs_at_send);

  /**
   * @brief Generate test data
   *        Send RPC requests
   * 
   */
  // used for other schemes
  // size_t non_exist_key_n_per_thread = nonexist_keys.size() / BenConfig.threads;
  // size_t non_exist_key_start = thread_id * non_exist_key_n_per_thread;
  // size_t non_exist_key_end = (thread_id + 1) * non_exist_key_n_per_thread;
  // std::vector<u64> op_keys(nonexist_keys.begin() + non_exist_key_start,
  //                           nonexist_keys.begin() + non_exist_key_end);
  size_t query_i = 0, insert_i = 0, remove_i = 0, update_i = 0;

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_real_distribution<> ratio_dis(0, 1);

  SScheduler ssched;
  rpc.reg_poll_future(ssched, &recv_s);

  LOG(2) << "[micro] Worker: " << thread_id << " Ready.";
  ready_threads++;
  V dummy_value = 1234;

  while (!running)
  ;

  /**
   * @brief using coroutines for testing
   * 
   */ 
  if(bench::BenConfig.workloads >= YCSB_A){ //NORMAL) {
    for(int i=0; i<BenConfig.coros; i++) {
      ssched.spawn([send_buf, &rpc, &sender, &recv_s, lkey, 
                    thread_id, &thread_param,
                    &ratio_dis, &gen,
                    &query_i, &insert_i, &remove_i, &update_i](R2_ASYNC) {
        char reply_buf[1024];
        RPCOp op;
        std::chrono::microseconds duration(0);
        while(running) {
          double d = ratio_dis(gen);
          if(d <= BenConfig.read_ratio) {                                                   // search
            KeyType dummy_key = bench_keys[query_i];
            auto start_time = std::chrono::high_resolution_clock::now();
            auto res = remote_search(dummy_key, rpc, sender, lkey, R2_ASYNC_WAIT);
            auto end_time = std::chrono::high_resolution_clock::now();
            duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
            query_i++;
            if (unlikely(query_i == bench_keys.size())) {
              query_i = 0;
            }
          } else if(d <= BenConfig.read_ratio+BenConfig.insert_ratio) {                      // insert
            KeyType dummy_key = nonexist_keys[insert_i];
            auto start_time = std::chrono::high_resolution_clock::now();
            remote_put(dummy_key, dummy_key, rpc, sender, R2_ASYNC_WAIT);
            auto end_time = std::chrono::high_resolution_clock::now();
            duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
            insert_i++;
            if (unlikely(insert_i == nonexist_keys.size())) {
                insert_i = 0;
            }
          } else if(d<=BenConfig.read_ratio+BenConfig.insert_ratio+BenConfig.update_ratio) {      // update
            KeyType dummy_key = bench_keys[update_i];
            auto start_time = std::chrono::high_resolution_clock::now();
            remote_update(dummy_key, dummy_key, rpc, sender, R2_ASYNC_WAIT);
            auto end_time = std::chrono::high_resolution_clock::now();
            duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
            update_i++;
            if (unlikely(update_i == bench_keys.size())) {
                update_i = 0;
            }
          } else {
            KeyType dummy_key = bench_keys[remove_i];
            remote_remove(dummy_key, rpc, sender, R2_ASYNC_WAIT);
            remove_i++;
            if (unlikely(remove_i == bench_keys.size())) {
                remove_i = 0;
            }
          }
          thread_param.throughput++;
          thread_param.latency += static_cast<double>(duration.count());
        }
        if (R2_COR_ID() == BenConfig.coros) {
          R2_STOP();
        }
        R2_RET;
      });
    }
  } else {  // YCSB
    for(int i=0; i<BenConfig.coros; i++) {
      ssched.spawn([send_buf, &rpc, &sender, &recv_s, lkey, 
                    thread_id, &thread_param,
                    &ratio_dis, &gen,
                    &query_i, &insert_i, &remove_i, &update_i](R2_ASYNC) {
        char reply_buf[1024];
        RPCOp op;
        std::chrono::microseconds duration(0);
        while(running) {
          double d = ratio_dis(gen);
          if(d <= BenConfig.read_ratio) {    // search
            KeyType dummy_key = std::stoull(workload.NextTransactionKey().substr(4));
            auto start_time = std::chrono::high_resolution_clock::now();
            auto res = remote_search(dummy_key, rpc, sender, lkey, R2_ASYNC_WAIT);
            auto end_time = std::chrono::high_resolution_clock::now();
            duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
          } else if(d <= BenConfig.read_ratio+BenConfig.insert_ratio) {  // insert
            KeyType dummy_key = std::stoull(workload.NextSequenceKey().substr(4));
            remote_put(dummy_key, dummy_key, rpc, sender, R2_ASYNC_WAIT);
          } else if(d<=BenConfig.read_ratio+BenConfig.insert_ratio+BenConfig.update_ratio) { // update
            KeyType dummy_key = std::stoull(workload.NextTransactionKey().substr(4));
            remote_update(dummy_key, dummy_key, rpc, sender, R2_ASYNC_WAIT);
          } else {  // remove
            KeyType dummy_key = std::stoull(workload.NextTransactionKey().substr(4));
            remote_remove(dummy_key, rpc, sender, R2_ASYNC_WAIT);
          }
          thread_param.throughput++;
          thread_param.latency += static_cast<double>(duration.count());
        }
        if (R2_COR_ID() == BenConfig.coros) {
          R2_STOP();
        }
        R2_RET;
      });
    }
  }
  ssched.run();
  pthread_exit(nullptr);
}


auto remote_search(const KeyType& key,
          RPC& rpc,
          UDTransport& sender,
          const rmem::mr_key_t& lkey, 
          R2_ASYNC) -> ::r2::Option<ValType>
{
  std::string data;
  data += ::xstore::util::Marshal<uint32_t>::serialize_to(hashbucket(key));
  data += ::xstore::util::Marshal<uint16_t>::serialize_to(hashtag(key));

  char send_buf[64];
  char reply_buf[sizeof(ReplyValue)];

  RPCOp op;
  op.set_msg(MemBlock((char *)send_buf, 64))
    .set_req()
    .set_rpc_id(GET)
    .set_corid(R2_COR_ID())
    .add_one_reply(rpc.reply_station,
                   { .mem_ptr = reply_buf, .sz = sizeof(ReplyValue) })
    .add_opaque(data);
  ASSERT(rpc.reply_station.cor_ready(R2_COR_ID()) == false);
  auto ret = op.execute_w_key(&sender, lkey);
  ASSERT(ret == IOCode::Ok);

  // yield the coroutine to wait for reply
  R2_PAUSE_AND_YIELD;

  // check the rest
  ReplyValue r = *(reinterpret_cast<ReplyValue*>(reply_buf));
  if(r.status) {
    return (ValType)r.val;
  }
  return {};
}


void remote_put(const KeyType& key, 
          const ValType& val, 
          RPC& rpc, 
          UDTransport& sender, 
          R2_ASYNC)
{
  std::string data;
  data += ::xstore::util::Marshal<uint32_t>::serialize_to(hashbucket(key));
  data += ::xstore::util::Marshal<uint16_t>::serialize_to(hashtag(key));
  data += ::xstore::util::Marshal<ValType>::serialize_to(val);

  char send_buf[64];
  char reply_buf[sizeof(ReplyValue)];
  RPCOp op;
  op.set_msg(MemBlock(send_buf, 64))
    .set_req()
    .set_rpc_id(PUT)
    .set_corid(R2_COR_ID())
    .add_one_reply(rpc.reply_station,
                   { .mem_ptr = reply_buf, .sz = sizeof(ReplyValue) })
    .add_opaque(data);
  ASSERT(op.execute_w_key(&sender, 0) == IOCode::Ok);

  // yield to the next coroutine
  R2_PAUSE_AND_YIELD;
}


void remote_update(const KeyType& key, const ValType& val, RPC& rpc, UDTransport& sender, R2_ASYNC)
{
  std::string data;
  data += ::xstore::util::Marshal<uint32_t>::serialize_to(hashbucket(key));
  data += ::xstore::util::Marshal<uint16_t>::serialize_to(hashtag(key));
  data += ::xstore::util::Marshal<ValType>::serialize_to(val);

  char send_buf[64];
  char reply_buf[sizeof(ReplyValue)];
  RPCOp op;
  op.set_msg(MemBlock(send_buf, 64))
    .set_req()
    .set_rpc_id(UPDATE)
    .set_corid(R2_COR_ID())
    .add_one_reply(rpc.reply_station,
                   { .mem_ptr = reply_buf, .sz = sizeof(ReplyValue)})
    .add_opaque(data);
  ASSERT(op.execute_w_key(&sender, 0) == IOCode::Ok);

  // yield to the next coroutine
  R2_PAUSE_AND_YIELD;
}


void remote_remove(const KeyType& key, RPC& rpc, UDTransport& sender, R2_ASYNC)
{
  std::string data;
  data += ::xstore::util::Marshal<KeyType>::serialize_to(key);

  char send_buf[64];
  char reply_buf[sizeof(ReplyValue)];
  RPCOp op;
  op.set_msg(MemBlock(send_buf, 64))
    .set_req()
    .set_rpc_id(DELETE)
    .set_corid(R2_COR_ID())
    .add_one_reply(rpc.reply_station,
                   { .mem_ptr = reply_buf, .sz = sizeof(ReplyValue) })
    .add_opaque(data);
  ASSERT(op.execute_w_key(&sender, 0) == IOCode::Ok);

  // yield to the next coroutine
  R2_PAUSE_AND_YIELD;
}


void remote_scan(const KeyType& key, const u64& n, RPC& rpc, UDTransport& sender, R2_ASYNC)
{
  std::string data;
  data += ::xstore::util::Marshal<KeyType>::serialize_to(key);
  data += ::xstore::util::Marshal<u64>::serialize_to(n);

  char send_buf[64];
  char reply_buf[sizeof(ReplyValue)];
  RPCOp op;
  op.set_msg(MemBlock(send_buf, 64))
    .set_req()
    .set_rpc_id(SCAN)
    .set_corid(R2_COR_ID())
    .add_one_reply(rpc.reply_station,
                   { .mem_ptr = reply_buf, .sz = sizeof(ReplyValue) })
    .add_opaque(data);
  ASSERT(op.execute_w_key(&sender, 0) == IOCode::Ok);

  // yield to the next coroutine
  R2_PAUSE_AND_YIELD;
}


}
