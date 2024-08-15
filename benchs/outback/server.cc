#include <iostream>
#include <stdexcept>
#include <gflags/gflags.h>

#include "rlib/core/lib.hh"                   /// logging, RNicInfo
#include "r2/src/thread.hh"                   /// Thread

#include "outback/outback_server.hh"
#include "benchs/load_config.hh"
#include "benchs/load_data.hh"

using namespace test;
using namespace xstore::transport;
using namespace xstore::rpc;
using namespace bench;
using namespace outback;

using XThread = ::r2::Thread<usize>;

#define DEBUG_MODE_CHECK 0

auto setup_ludo_table() -> bool;
auto rolex_server_workers(const usize& nthreads) -> std::vector<std::unique_ptr<XThread>>;

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, false);

  // setup the value // 
  LOG(2) << "[Outback server loading data] ...";
  bench::load_benchmark_config();
  bench::load_data();

  LOG(2) << "[Server setup ludo table] ...";
  setup_ludo_table();

  std::vector<std::unique_ptr<XThread>> workers = 
        rolex_server_workers(FLAGS_mem_threads);

  ctrl.start_daemon();

  running = false;
  for (auto &w : workers) {
    w->start();
  }

  while(ready_threads < FLAGS_mem_threads) sleep(0.3);
  running = true;

  size_t current_sec = 0;
  while (current_sec < FLAGS_seconds+10) {
      sleep(1);
      ++current_sec;
  }

  running = false;
  for (auto &w : workers) {
    w->join();
  }

  LOG(4) << "rpc server finishes";
  return 0;
}


auto rolex_server_workers(const usize& nthreads) -> std::vector<std::unique_ptr<XThread>>{
  std::vector<std::unique_ptr<XThread>> res;
  for(int i=0; i<nthreads; i++) {
    res.push_back(std::move(std::make_unique<XThread>([i]()->usize{
      /**
       * @brief Constuct UD qp and register in the RCtrl
       */
      // create NIC and QP
      auto thread_id = i;
      auto nic_for_recv = RNic::create(RNicInfo::query_dev_names().at(FLAGS_nic_idx)).value();
      auto qp_recv = UD::create(nic_for_recv, QPConfig()).value();
      // prepare UD recv buffer
      auto mem_region = ::HugeRegion::create(64 * 1024 * 1024).value();
      auto mem = mem_region->convert_to_rmem().value();
      auto handler = RegHandler::create(mem, nic_for_recv).value();
      // Post receive buffers to QP and transition QP to RTR state
      SimpleAllocator alloc(mem, handler->get_reg_attr().value());
      auto recv_rs_at_recv =
        RecvEntriesFactory<SimpleAllocator, RECV_NUM, 4096>::create(alloc);
      {
        auto res = qp_recv->post_recvs(*recv_rs_at_recv, RECV_NUM);
        RDMA_ASSERT(res == IOCode::Ok);
      }
      // register the UD for connection
      ctrl.registered_qps.reg("b" + std::to_string(thread_id), qp_recv);
      // LOG(4) << "server thread #" << thread_id << " started!";

      /**
       * @brief Construct RPC
       */
      RPCCore<SendTrait, RecvTrait, SManager> rpc(12);
      {
        auto large_buf = alloc.alloc_one(1024).value();
        rpc_large_reply_buf = static_cast<char*>(std::get<0>(large_buf));
        rpc_large_reply_key = std::get<1>(large_buf);
      }
      
      UDRecvTransport<RECV_NUM> recv(qp_recv, recv_rs_at_recv);
      // register the callbacks before enter the main loop
      ASSERT(rpc.reg_callback(outback_get_callback) == GET);
      ASSERT(rpc.reg_callback(outback_put_callback) == PUT);
      ASSERT(rpc.reg_callback(outback_update_callback) == UPDATE);
      ASSERT(rpc.reg_callback(outback_remove_callback) == DELETE);
      ASSERT(rpc.reg_callback(outback_scan_callback) == SCAN);
      r2::compile_fence();

      ready_threads++;

      while (!running) {
        //r2::compile_fence();
      }
      while (running) {
        r2::compile_fence();
        rpc.recv_event_loop(&recv);
      }

      return 0;
    })));
  }
  return std::move(res);
}

auto setup_ludo_table() -> bool {
  packed_data = new packed_data_t(2*FLAGS_nkeys);
  lru_cache = new lru_cache_t(8192,10);
  ludo_maintenance_t ludo_maintenance_unit(1024);
  for (uint64_t i = 0; i < FLAGS_nkeys; i++) {
    KeyType key = exist_keys[i];
    V addr = packed_data->bulk_load_data(key, sizeof(V), i);
    ludo_maintenance_unit.insert(key, addr);
    r2::compile_fence();
  }
  LOG(2) << "Ludo slots warmed up...";

  // extendible hashing setup, local_depths map hash prefix 00,01 to the real table index
  local_depths.assign(8,0);
  ludo_buckets = new outback::LudoBuckets*[8];
  for (size_t i = 0; i < 8; ++i) 
    ludo_buckets[i] = nullptr;

  ludo_lookup_unit = new ludo_lookup_t(ludo_maintenance_unit);
  ludo_lookup_t ludo_lookup_table(ludo_maintenance_unit, ludo_buckets[0]);
  mutexArray = new std::mutex[ludo_lookup_unit->getBucketsNum()];
  bucketLocks.resize(ludo_lookup_unit->getBucketsNum());
  LOG(2) << "Ludo slots finished up...";

  #if DEBUG_MODE_CHECK
  for (uint64_t i = 0; i < 100; i++) {
    V dummy_value;
    auto loc = ludo_lookup_unit.lookup_slot(i);
    auto addr = ludo_buckets[0]->read_addr(loc.first, loc.second);
    // packed_struct_t* packed_struct = reinterpret_cast<packed_struct_t*>(addr);
    packed_data->read_data(addr, dummy_value);
    LOG(3) << "4: "+std::to_string(dummy_value);
  }
  #endif

  return true;
}


