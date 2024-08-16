#include <gflags/gflags.h>

#include "rlib/core/lib.hh"                   /// logging, RNicInfo
#include "r2/src/thread.hh"                   /// Thread
#include "xcomm/tests/transport_util.hh"      /// SimpleAllocator
#include "xcomm/src/transport/rdma_ud_t.hh"   /// UDTransport, UDRecvTransport, UDSessionManager
#include "xcomm/src/rpc/mod.hh"               /// RPCCore
#include "xutils/local_barrier.hh"            /// PBarrier

#include "drtmr/trait.hpp"
#include "benchs/load_config.hh"
#include "benchs/load_data.hh"
#include "benchs/rolex_util_back.hh"

using namespace test;
using namespace xstore::transport;
using namespace xstore::rpc;
using namespace drtmr;
using namespace bench;

#define CLUSTER_HASH_MODE 1
#define GOOGLE_BTREE_MODE 0
#define LEARNED_ALEX_MODE 0
#define DUMMY_INDEX_MODE 0

volatile bool running = true;
std::atomic<size_t> ready_threads(0);
::rdmaio::RCtrl ctrl(8888);

cluster_hash_t* cluster_table;
alex_map_t* alex_map_instance;
btree_map_t* btree_map_instance;
packed_data_t* packed_data;
std::mutex _mutex;

namespace drtmr {

#define RECV_NUM 2048

using SendTrait = UDTransport;
using RecvTrait = UDRecvTransport<RECV_NUM>;
using SManager = UDSessionManager<RECV_NUM>;
using XThread = ::r2::Thread<usize>;   // <usize> represents the return type of a function

thread_local char* rpc_large_reply_buf = nullptr;
thread_local u32 rpc_large_reply_key;

void setup_dummy_index() {
  packed_data = new packed_data_t(1.2*FLAGS_nkeys);
  for (uint64_t i = 0; i < FLAGS_nkeys; i++) {
    KeyType key=exist_keys[i];
    V addr=packed_data->bulk_load_data(key, sizeof(V), i);
    r2::compile_fence();
  }
  LOG(2) << "Dummy index warmed up...";
}

void setup_learned_alex_map() {
  packed_data = new packed_data_t(1.2*FLAGS_nkeys);
  alex_map_instance = new alex_map_t();
  for (uint64_t i = 0; i < FLAGS_nkeys; i++) {
    KeyType key=exist_keys[i];
    V addr=packed_data->bulk_load_data(key, sizeof(V), i);
    (*alex_map_instance).insert(key,addr);
    r2::compile_fence();
  }
  LOG(2) << "Learned index warmed up...";
}

void setup_google_btree_map() {
  packed_data = new packed_data_t(1.2*FLAGS_nkeys);
  btree_map_instance = new btree_map_t();
  for (uint64_t i = 0; i < FLAGS_nkeys; i++) {
    KeyType key=exist_keys[i];
    V addr=packed_data->bulk_load_data(key, sizeof(V), i);
    btree_map_instance->insert({key,addr});
    r2::compile_fence();
  }
  LOG(2) << "Google btree warmed up...";
}

void setup_cluster_hash_table() {
  packed_data = new packed_data_t(1.2*FLAGS_nkeys);
  uint64_t total_size = 6ULL*1024*1024*1024;  //1G //if we go 2*1G, it doesn't work you have to go 2ULL.
  char *buffer= (char*) malloc(total_size);
  uint64_t len = 1.0*FLAGS_nkeys;
  cluster_table = new cluster_hash_t(buffer, len, 8);
  for (uint64_t i=0; i<FLAGS_nkeys; i++) {
    auto key=exist_keys[i];
    V addr=packed_data->bulk_load_data(key, sizeof(V), i);
    char val[sizeof(V)];
    memcpy(val, &addr, sizeof(V));
    cluster_table->Insert(key,reinterpret_cast<void*>(val));
    if (i%100000 == 0) {
      auto addr = cluster_table->Get(key);
      LOG(4) << "cluster insert: " << *addr;
    }
  }
  LOG(4) << "Cluster hash warmed up..";
}

auto drtmr_server_workers(const usize& nthreads) -> std::vector<std::unique_ptr<XThread>>;
void drtmr_get_callback(const Header& rpc_header, const MemBlock& args, SendTrait* replyc);
void drtmr_put_callback(const Header& rpc_header, const MemBlock& args, SendTrait* replyc);
void drtmr_update_callback(const Header& rpc_header, const MemBlock& args, SendTrait* replyc);
void drtmr_remove_callback(const Header& rpc_header, const MemBlock& args, SendTrait* replyc);
void drtmr_scan_callback(const Header& rpc_header, const MemBlock& args, SendTrait* replyc);

}


using XThread = ::r2::Thread<usize>;
int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, false);

  // setup the value // 
  LOG(2) << "[Cluster hashing table loading data] ...";
  bench::load_benchmark_config();
  bench::load_data();

  #if CLUSTER_HASH_MODE
    // setup cluster hash table ..
    setup_cluster_hash_table();
  #elif GOOGLE_BTREE_MODE
    // setup google btree ..
    setup_google_btree_map();
  #elif LEARNED_ALEX_MODE
    // setup learned alex ..
    setup_learned_alex_map();
  #elif DUMMY_INDEX_MODE
    //setup dummy index ..
    setup_dummy_index();
  #endif

  std::vector<std::unique_ptr<XThread>> workers = 
        drtmr::drtmr_server_workers(FLAGS_mem_threads);

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


namespace drtmr {

auto drtmr_server_workers(const usize& nthreads) -> std::vector<std::unique_ptr<XThread>>{
  std::vector<std::unique_ptr<XThread>> res;
  for(int i=0; i<nthreads; i++) {
    res.push_back(std::move(std::make_unique<XThread>([i]()->usize{
      /**
       * @brief Constuct UD qp and register in the RCtrl
       * 
       */
      // create NIC and QP
      auto thread_id = i;
      auto nic_for_recv = RNic::create(RNicInfo::query_dev_names().at(FLAGS_nic_idx)).value();
      auto qp_recv = UD::create(nic_for_recv, QPConfig()).value();
      // prepare UD recv buffer
      auto mem_region = HugeRegion::create(64 * 1024 * 1024).value();
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
       * 
       */
      RPCCore<SendTrait, RecvTrait, SManager> rpc(12);
      {
        auto large_buf = alloc.alloc_one(1024).value();
        rpc_large_reply_buf = static_cast<char*>(std::get<0>(large_buf));
        rpc_large_reply_key = std::get<1>(large_buf);
      }
      
      UDRecvTransport<RECV_NUM> recv(qp_recv, recv_rs_at_recv);
      // register the callbacks before enter the main loop
      ASSERT(rpc.reg_callback(drtmr_get_callback) == GET);
      ASSERT(rpc.reg_callback(drtmr_put_callback) == PUT);
      ASSERT(rpc.reg_callback(drtmr_update_callback) == UPDATE);
      ASSERT(rpc.reg_callback(drtmr_remove_callback) == DELETE);
      ASSERT(rpc.reg_callback(drtmr_scan_callback) == SCAN);
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


void drtmr_get_callback(const Header& rpc_header, const MemBlock& args, SendTrait* replyc) {
	// sanity check the requests
  ASSERT(args.sz == sizeof(KeyType));
  KeyType key = *(reinterpret_cast<KeyType*>(args.mem_ptr));
	// GET
	ValType dummy_value(0);
  #if CLUSTER_HASH_MODE
    dummy_value = *(cluster_table->Get(key));
    //auto length = packed_data->read_data(addr, dummy_value);
  #elif GOOGLE_BTREE_MODE
    auto addr = (*btree_map_instance)[key];
    auto length = packed_data->read_data(addr, dummy_value);
  #elif LEARNED_ALEX_MODE
    auto addr = (*alex_map_instance).at(key);
    auto length = packed_data->read_data(addr, dummy_value);
  #elif DUMMY_INDEX_MODE
    auto addr = key%FLAGS_nkeys;
    auto length = packed_data->read_data(addr, dummy_value);
  #endif
	ReplyValue reply;
  if(dummy_value) {
    reply = { .status = true, .val = dummy_value };
  } else {
    reply = { .status = false, .val = dummy_value };
  }
  // send
  char reply_buf[64];
  RPCOp op;
  ASSERT(op.set_msg(MemBlock(reply_buf, 64)).set_reply().add_arg(reply));
  op.set_corid(rpc_header.cor_id);
  // LOG(3)<<"GET: " << *(args.interpret_as<u64>());
  ASSERT(op.execute(replyc) == IOCode::Ok);
}


void drtmr_put_callback(const Header& rpc_header, const MemBlock& args, SendTrait* replyc) {
	// sanity check the requests
  ASSERT(args.sz == sizeof(KeyType)+sizeof(ValType));
	KeyType key = *args.interpret_as<KeyType>();
  ValType val = *args.interpret_as<ValType>(sizeof(KeyType));
	// PUT
  //std::unique_lock<std::mutex> lock(_mutex);
  #if CLUSTER_HASH_MODE
    /*auto addr= cluster_table->Get(key);
    if (addr != nullptr) {
      auto length = packed_data->update_data(*addr, key, args.sz, val);
    } else {
      V addr=packed_data->bulk_load_data(key, sizeof(V), key);
      char value[sizeof(V)];
      memcpy(value, &addr, sizeof(V));
      cluster_table->Insert(key,reinterpret_cast<void*>(value));
    }*/
    char value[sizeof(V)];
    memcpy(value, &val, sizeof(V));
    cluster_table->Insert(key,reinterpret_cast<void*>(value));
  #elif GOOGLE_BTREE_MODE
    auto it = btree_map_instance->find(key);
    if (it != btree_map_instance->end()) {
      auto addr = (*btree_map_instance)[key];
      auto length = packed_data->update_data(addr, key, args.sz, val);
    } else {
      V addr=packed_data->bulk_load_data(key, sizeof(V), key);
      btree_map_instance->insert({key,addr});
    }
  #elif LEARNED_ALEX_MODE
    auto it = alex_map_instance->find(key);
    if (it != alex_map_instance->end()) {
      auto addr = (*alex_map_instance).at(key);
      auto length = packed_data->update_data(addr, key, args.sz, val);
    } else {
      V addr=packed_data->bulk_load_data(key,sizeof(V), key);
      (*alex_map_instance).insert(key,addr);
    }
  #endif
  //lock.unlock();

	ReplyValue reply;
	// send
  char reply_buf[64];
  RPCOp op;
  ASSERT(op.set_msg(MemBlock(reply_buf, 64)).set_reply().add_arg(reply));
  op.set_corid(rpc_header.cor_id);
  //LOG(3) << "Put key:" << key;
  ASSERT(op.execute(replyc) == IOCode::Ok);
}


void drtmr_update_callback(const Header& rpc_header, const MemBlock& args, SendTrait* replyc){
	// sanity check the requests
  ASSERT(args.sz == sizeof(KeyType)+sizeof(ValType));
	KeyType key = *args.interpret_as<KeyType>();
  ValType val = *args.interpret_as<ValType>(sizeof(KeyType));
	// UPDATE
  //std::unique_lock<std::mutex> lock(_mutex);
	#if CLUSTER_HASH_MODE
    char value[sizeof(V)];
    memcpy(value, &val, sizeof(V));
    cluster_table->Update(key,reinterpret_cast<void*>(value));
    //auto addr= *(cluster_table->Insert(key, value));
    //memcpy(value, &addr, sizeof(V));
    //auto length = packed_data->update_data(addr, key, args.sz, val);
  #elif GOOGLE_BTREE_MODE
    auto addr = (*btree_map_instance)[key];
    auto length = packed_data->update_data(addr, key, args.sz, val);
  #elif LEARNED_ALEX_MODE
    auto addr = (*alex_map_instance).at(key);
    auto length = packed_data->update_data(addr, key, args.sz, val);
  #elif DUMMY_INDEX_MODE
    auto addr = key%FLAGS_nkeys;
    auto length = packed_data->update_data(addr, key, args.sz, val);
  #endif
  //lock.unlock();
	ReplyValue reply;
  if (true){  //if (length > 0) {
    reply = { .status = true, .val = val };
  } else {
    reply = { .status = false, .val = val };
  }
  // send
  char reply_buf[64];
  RPCOp op;
  ASSERT(op.set_msg(MemBlock(reply_buf, 64)).set_reply().add_arg(reply));
  op.set_corid(rpc_header.cor_id);
  ASSERT(op.execute(replyc) == IOCode::Ok);
}


void drtmr_remove_callback(const Header& rpc_header, const MemBlock& args, SendTrait* replyc){
	// sanity check the requests
  ASSERT(args.sz == sizeof(KeyType));
	KeyType key = *args.interpret_as<KeyType>();
	// UPDATE
  bool res(true);
  //_mutex.lock();
	#if CLUSTER_HASH_MODE
    // Not support in cluster hash emmm
    uint8_t status=0;
  #elif GOOGLE_BTREE_MODE
    auto addr = (*btree_map_instance)[key];
    (*btree_map_instance).erase(key);
    auto status = packed_data->remove_data(addr);
  #elif LEARNED_ALEX_MODE
    auto addr = (*alex_map_instance).at(key);
    (*alex_map_instance).erase(key);
    auto status = packed_data->remove_data(addr); // status=0, succ
  #elif DUMMY_INDEX_MODE
    auto addr = key%FLAGS_nkeys;
    auto status = packed_data->remove_data(addr); // status=0, succ
  #endif
  //_mutex.lock();
	ReplyValue reply;
  if(status<=0) {
    reply = { .status = true, .val = 0 };
  } else {
    reply = { .status = false, .val = 0 };
  }
  // send
  char reply_buf[64];
  RPCOp op;
  ASSERT(op.set_msg(MemBlock(reply_buf, 64)).set_reply().add_arg(reply));
  op.set_corid(rpc_header.cor_id);
  ASSERT(op.execute(replyc) == IOCode::Ok);
}


void drtmr_scan_callback(const Header& rpc_header, const MemBlock& args, SendTrait* replyc){
	// sanity check the requests
  ASSERT(args.sz == sizeof(KeyType)+sizeof(u64));
	KeyType key = *args.interpret_as<KeyType>();
  ValType n = *args.interpret_as<ValType>(sizeof(KeyType));
	// UPDATE
  std::vector<V> result;
  
	// drtmr_index->range(key, n, result);
	ReplyValue reply;
  // send
  char reply_buf[64];
  RPCOp op;
  ASSERT(op.set_msg(MemBlock(reply_buf, 64)).set_reply().add_arg(reply));
  op.set_corid(rpc_header.cor_id);
  ASSERT(op.execute(replyc) == IOCode::Ok);
}

}