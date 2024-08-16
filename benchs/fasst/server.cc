#include <gflags/gflags.h>

#include "rlib/core/lib.hh"                   /// logging, RNicInfo
#include "r2/src/thread.hh"                   /// Thread
#include "xcomm/tests/transport_util.hh"      /// SimpleAllocator
#include "xcomm/src/transport/rdma_ud_t.hh"   /// UDTransport, UDRecvTransport, UDSessionManager
#include "xcomm/src/rpc/mod.hh"               /// RPCCore
#include "xutils/local_barrier.hh"            /// PBarrier

#include "mica/trait.hpp"
#include "benchs/load_config.hh"
#include "benchs/load_data.hh"
#include "benchs/rolex_util_back.hh"

using namespace test;
using namespace xstore::transport;
using namespace xstore::rpc;
using namespace fasst;
using namespace bench;

volatile bool running = true;
std::atomic<size_t> ready_threads(0);
::rdmaio::RCtrl ctrl(8888);
packed_data_t* packed_data;
std::mutex _mutex;
mica_kv* kv;
mica_op* op;

namespace fasst {

#define RECV_NUM 2048
#define DEBUG_MODE_CHECK 0
#define REPLY_BUF_SIZE 64

using SendTrait = UDTransport;
using RecvTrait = UDRecvTransport<RECV_NUM>;
using SManager = UDSessionManager<RECV_NUM>;
using XThread = ::r2::Thread<usize>;   // <usize> represents the return type of a function

thread_local char* rpc_large_reply_buf = nullptr;
thread_local u32 rpc_large_reply_key;

void setup_mica_table() {
  // packed_data = new packed_data_t(1.2*FLAGS_nkeys);
  kv = (mica_kv*) malloc(sizeof(mica_kv));
  mica_resp* resp = (mica_resp*) malloc(sizeof(mica_resp));
  op = (mica_op*) malloc(sizeof(mica_op));
  mica_init(kv, 0, 0, FLAGS_nkeys/4, M_1024);
  for (uint32_t i = 0; i < FLAGS_nkeys; i++) {
    KeyType key = exist_keys[i];
    op->val_len=8;
    op->value[0]=i;
    op->key.server=0;
    op->opcode = MICA_OP_PUT;
    op->key.bkt=hashbucket(key);
    op->key.tag=hashtag(key);
    mica_insert_one(kv, op, resp);
    if (i % 1000000 == 0) {
      LOG(3) << "Mica check key: " << i;
      op->opcode = MICA_OP_GET;
      mica_batch_op(kv, 1, &op, resp);
      LOG(3) << "value: " << *resp->val_ptr;
    }
  }
  LOG(3) << "MICA goes good..";
}

void check_mica_table() {
  mica_resp* respq = (mica_resp*) malloc(sizeof(mica_resp));
  mica_op* opq = (mica_op*) malloc(sizeof(mica_op));
  memcpy(opq, op, sizeof(op));
  for (uint32_t i = 0; i < FLAGS_nkeys; i++) {
    if (i % 50000 == 1) {
      KeyType key = exist_keys[i];
      opq->val_len=8;
      opq->value[0]=i;
      opq->key.server=0;
      op->opcode = MICA_OP_GET;
      op->key.bkt=hashbucket(key);
      op->key.tag=hashtag(key);
      LOG(3) << "Mica check key: " << i;
      mica_batch_op(kv, 1, &op, respq);
      LOG(3) << "value: " << (int)respq->type;
    }
  }
  LOG(3) << "MICA check good again..";
}

auto fasst_server_workers(const usize& nthreads) -> std::vector<std::unique_ptr<XThread>>;
void fasst_get_callback(const Header& rpc_header, const MemBlock& args, SendTrait* replyc);
void fasst_put_callback(const Header& rpc_header, const MemBlock& args, SendTrait* replyc);
void fasst_update_callback(const Header& rpc_header, const MemBlock& args, SendTrait* replyc);
void fasst_remove_callback(const Header& rpc_header, const MemBlock& args, SendTrait* replyc);
void fasst_scan_callback(const Header& rpc_header, const MemBlock& args, SendTrait* replyc);
}


using XThread = ::r2::Thread<usize>;
int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, false);

  // setup the value //
  LOG(2) << "[Benchmark loading data] ...";
  bench::load_benchmark_config();
  bench::load_data();

  LOG(2) << "[MICA hash loading data] ...";
  setup_mica_table();
  check_mica_table();

  std::vector<std::unique_ptr<XThread>> workers = 
        fasst::fasst_server_workers(FLAGS_mem_threads);

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


namespace fasst {

auto fasst_server_workers(const usize& nthreads) -> std::vector<std::unique_ptr<XThread>>{
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
      ASSERT(rpc.reg_callback(fasst_get_callback) == GET);
      ASSERT(rpc.reg_callback(fasst_put_callback) == PUT);
      ASSERT(rpc.reg_callback(fasst_update_callback) == UPDATE);
      ASSERT(rpc.reg_callback(fasst_remove_callback) == DELETE);
      ASSERT(rpc.reg_callback(fasst_scan_callback) == SCAN);
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


void fasst_get_callback(const Header& rpc_header, const MemBlock& args, SendTrait* replyc) {
	// sanity check the requests
  // GET
  ASSERT(args.sz == sizeof(uint32_t)+sizeof(uint16_t));
  //KeyType dummy_key = *(reinterpret_cast<KeyType*>(args.mem_ptr));
  uint32_t dummy_key_bkt = *args.interpret_as<uint32_t>();
  uint16_t dummy_key_tag = *args.interpret_as<uint16_t>(sizeof(uint32_t));
	ValType dummy_value = 0;
  bool res(true);
  // read real data value from it
  mica_resp* resp = (mica_resp*) malloc(sizeof(mica_resp));
  //mica_op* opq = (mica_op*) malloc(sizeof(mica_op));
  //memcpy(opq, op, sizeof(op));
  //opq->value[0]=0;
  //opq->key.server=0;
  //opq->val_len = 8;
  op->opcode = MICA_OP_GET;
  op->key.bkt=dummy_key_bkt;
  op->key.tag=dummy_key_tag;
  mica_batch_op(kv, 1, &op, resp);

  if(resp->type == MICA_RESP_GET_SUCCESS) {
    dummy_value = *resp->val_ptr;
  } else {
    //LOG(3) << "GET not found: " << (int)resp->type;
    res = false;
  }
	ReplyValue reply;
  if(res) {
    reply = { .status = true, .val = dummy_value };
  } else {
    reply = { .status = false, .val = dummy_value };
  }
  // send
  char reply_buf[REPLY_BUF_SIZE];
  RPCOp op;
  ASSERT(op.set_msg(MemBlock(reply_buf, REPLY_BUF_SIZE)).set_reply().add_arg(reply));
  op.set_corid(rpc_header.cor_id);
  // LOG(3)<<"GET: " << *(args.interpret_as<u64>());
  ASSERT(op.execute(replyc) == IOCode::Ok);
}


void fasst_put_callback(const Header& rpc_header, const MemBlock& args, SendTrait* replyc) {
	// sanity check the requests
  ASSERT(args.sz == sizeof(uint32_t)+sizeof(uint16_t)+sizeof(ValType));
  uint32_t dummy_key_bkt = *args.interpret_as<uint32_t>();
  uint16_t dummy_key_tag = *args.interpret_as<uint16_t>(sizeof(uint32_t));
  ValType dummy_value = *args.interpret_as<ValType>(sizeof(uint32_t)+sizeof(uint16_t));
	// PUT
  //std::unique_lock<std::mutex> lock(_mutex);
  // read data value from it
  mica_resp* respq = (mica_resp*) malloc(sizeof(mica_resp));
  //mica_op* opq = (mica_op*) malloc(sizeof(mica_op));
  //memcpy(opq, op, sizeof(op));
  op->opcode = MICA_OP_PUT;
  op->val_len=8;
  op->value[0]=dummy_value;
  op->key.server=0;
  op->key.bkt=dummy_key_bkt;
  op->key.tag=dummy_key_tag;
  mica_batch_op(kv, 1, &op, respq);
  //lock.unlock();

	ReplyValue reply;
	// send
  if (true){
    reply = { .status = true, .val = 1 };
  } else {
    reply = { .status = false, .val = 0 };
  }
  char reply_buf[REPLY_BUF_SIZE];
  RPCOp op;
  ASSERT(op.set_msg(MemBlock(reply_buf, REPLY_BUF_SIZE)).set_reply().add_arg(reply));
  op.set_corid(rpc_header.cor_id);
  //LOG(3) << "Put key:" << key;
  ASSERT(op.execute(replyc) == IOCode::Ok);
}

void fasst_update_callback(const Header& rpc_header, const MemBlock& args, SendTrait* replyc){
	// sanity check the requests
  ASSERT(args.sz == sizeof(uint32_t)+sizeof(uint16_t)+sizeof(ValType));
  uint32_t dummy_key_bkt = *args.interpret_as<uint32_t>();
  uint16_t dummy_key_tag = *args.interpret_as<uint16_t>(sizeof(uint32_t));
  ValType dummy_value = *args.interpret_as<ValType>(sizeof(uint32_t)+sizeof(uint16_t));
  
	// UPDATE
  //std::unique_lock<std::mutex> lock(_mutex);
  mica_resp* respq = (mica_resp*) malloc(sizeof(mica_resp));
  //mica_op* opq = (mica_op*) malloc(sizeof(mica_op));
  //memcpy(opq, op, sizeof(op));
  op->opcode = MICA_OP_PUT;
  op->val_len=8;
  op->value[0]=dummy_value;
  op->key.server=0;
  op->key.bkt=dummy_key_bkt;
  op->key.tag=dummy_key_tag;
  mica_batch_op(kv, 1, &op, respq);
  //lock.unlock();

  bool res(true);
	ReplyValue reply;
  if (res){
    reply = { .status = true, .val = 1 };
  } else {
    reply = { .status = false, .val = 0 };
  }
  // send
  char reply_buf[REPLY_BUF_SIZE];
  RPCOp op;
  ASSERT(op.set_msg(MemBlock(reply_buf, REPLY_BUF_SIZE)).set_reply().add_arg(reply));
  op.set_corid(rpc_header.cor_id);
  ASSERT(op.execute(replyc) == IOCode::Ok);
}


void fasst_remove_callback(const Header& rpc_header, const MemBlock& args, SendTrait* replyc){
	// sanity check the requests
  // REMOVE
  ASSERT(args.sz == sizeof(KeyType));
	KeyType key_i = *args.interpret_as<KeyType>();
  bool res(true);

	ReplyValue reply;
  if(res) {
    reply = { .status = true, .val = 0 };
  } else {
    reply = { .status = false, .val = 0 };
  }
  // send
  char reply_buf[REPLY_BUF_SIZE];
  RPCOp op;
  ASSERT(op.set_msg(MemBlock(reply_buf, REPLY_BUF_SIZE)).set_reply().add_arg(reply));
  op.set_corid(rpc_header.cor_id);
  ASSERT(op.execute(replyc) == IOCode::Ok);
}


void fasst_scan_callback(const Header& rpc_header, const MemBlock& args, SendTrait* replyc){
	// sanity check the requests
  ASSERT(args.sz == sizeof(KeyType)+sizeof(u64));
	KeyType key = *args.interpret_as<KeyType>();
  ValType n = *args.interpret_as<ValType>(sizeof(KeyType));
	// UPDATE
  std::vector<V> result;
  
	// fasst_index->range(key, n, result);
	ReplyValue reply;
  // send
  char reply_buf[REPLY_BUF_SIZE];
  RPCOp op;
  ASSERT(op.set_msg(MemBlock(reply_buf, REPLY_BUF_SIZE)).set_reply().add_arg(reply));
  op.set_corid(rpc_header.cor_id);
  ASSERT(op.execute(replyc) == IOCode::Ok);
}

}