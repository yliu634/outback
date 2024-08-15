#pragma once

#include <atomic>
#include "outback/trait.hpp"
//#include "benchs/load_config.hh"
#include "benchs/rolex_util_back.hh"

using namespace r2;
using namespace rdmaio;
using namespace test;
using namespace xstore::util;
using namespace xstore::rpc;
using namespace xstore::transport;
using namespace outback;


/*****  GLOBAL VARIABLES *******/
// ludo_seeds_t* ludo_seeds;
std::atomic<bool> reconstruct(false);
ludo_lookup_t* ludo_lookup_unit;  // act like seeds

namespace outback {

void remote_fetch_seeds();
using SendTrait = UDTransport;
using RecvTrait = UDRecvTransport<2048>;
using SManager = UDSessionManager<2048>;
using RPC = RPCCore<SendTrait, RecvTrait, SManager>;

auto remote_search(const KeyType& key, RPC& rpc, UDTransport& sender, 
                    const rmem::mr_key_t& lkey, R2_ASYNC) -> ::r2::Option<ValType>
{
  char send_buf[64];    // give a try to 32?
  char reply_buf[sizeof(ReplyValue)];

  auto loc = ludo_lookup_unit->lookup_slot(key);
  auto num = SLOTS_NUM_BUCKET * loc.first + loc.second;
  std::string data;
  data += ::xstore::util::Marshal<size_t>::serialize_to(num);
  data += ::xstore::util::Marshal<KeyType>::serialize_to(key);

  RPCOp op;
  op.set_msg(MemBlock((char *)send_buf, 64))
    .set_req()
    .set_rpc_id(GET)
    .set_corid(R2_COR_ID())
    .add_one_reply(rpc.reply_station,
                   { .mem_ptr = reply_buf, .sz = sizeof(ReplyValue) })
    .add_opaque(data);
    //.add_arg<KeyType>(num);
  ASSERT(rpc.reply_station.cor_ready(R2_COR_ID()) == false);
  auto ret = op.execute_w_key(&sender, lkey);
  ASSERT(ret == IOCode::Ok);

  // yield the coroutine to wait for reply
  R2_PAUSE_AND_YIELD;

  // check the rest
  ReplyValue r = *(reinterpret_cast<ReplyValue*>(reply_buf));
  if (r.status) {
    return (ValType)r.val;
  }
  return {};
}

void remote_put(const KeyType& key, const ValType& val, RPC& rpc, 
                UDTransport& sender, R2_ASYNC)
{
  std::string data;
  auto loc = ludo_lookup_unit->lookup_slot(key);
  auto num = SLOTS_NUM_BUCKET * loc.first + loc.second;
  data += ::xstore::util::Marshal<size_t>::serialize_to(num);
  data += ::xstore::util::Marshal<KeyType>::serialize_to(key);
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

  // check the rest
  ReplyValue r = *(reinterpret_cast<ReplyValue*>(reply_buf));
  if (r.status) {
    ValType val = r.val;
    if (val & (1UL<<8)) { // case othello insert with seeds updated
      ludo_lookup_unit->updateSeed(loc.first, r.val&0xFF);
    } else if (val>=1) {  // cache it please in client side? NO NEED!
      //TODO: cache it in server side
    }
  } else if ((r.val==3) && (!reconstruct.exchange(true))) {
    LOG(3) << "receive the false reply from server, then go reconstruct.";
    // then construct start and poll seeds from remot side with one-sided RDMA
    std::thread polling_thread(remote_fetch_seeds);
    polling_thread.detach();
  }
}

void remote_update(const KeyType& key, const ValType& val, RPC& rpc, 
                    UDTransport& sender, R2_ASYNC)
{
  std::string data;
  auto loc = ludo_lookup_unit->lookup_slot(key);
  auto num = SLOTS_NUM_BUCKET * loc.first + loc.second;
  data += ::xstore::util::Marshal<size_t>::serialize_to(num);
  data += ::xstore::util::Marshal<KeyType>::serialize_to(key);
  data += ::xstore::util::Marshal<ValType>::serialize_to(val);

  char send_buf[64];
  char reply_buf[sizeof(ReplyValue)];
  RPCOp op;
  op.set_msg(MemBlock(send_buf, 64))
    .set_req()
    .set_rpc_id(UPDATE)
    .set_corid(R2_COR_ID())
    .add_one_reply(rpc.reply_station,
                   { .mem_ptr = reply_buf, .sz = sizeof(ReplyValue) })
    .add_opaque(data);
  ASSERT(op.execute_w_key(&sender, 0) == IOCode::Ok);

  // yield to the next coroutine
  R2_PAUSE_AND_YIELD;

  ReplyValue r = *(reinterpret_cast<ReplyValue*>(reply_buf));

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


/************** One-sided RDMA part for seeds updating ************/
auto remote_write(const u64 ac_addr, rdmaio::Arc<rdmaio::qp::RC>& qp, 
                  char *test_buf, const u32 write_size) -> void {
  auto res_s = qp->send_normal(
      {.op = IBV_WR_RDMA_WRITE,
       .flags = IBV_SEND_SIGNALED,
       .len = write_size, // bytes
       .wr_id = 0},
      {.local_addr = reinterpret_cast<RMem::raw_ptr_t>(test_buf),
       .remote_addr = ac_addr,
       .imm_data = 0});
  RDMA_ASSERT(res_s == IOCode::Ok);
  auto res_p = qp->wait_one_comp();
  RDMA_ASSERT(res_p == IOCode::Ok);
}

auto remote_read(const u64 ac_addr, rdmaio::Arc<rdmaio::qp::RC>& qp, 
                  char *test_buf, const u32 read_size) ->::r2::Option<ValType> {
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

inline auto remote_fetch_and_add(const u64 ac_addr, rdmaio::Arc<rdmaio::qp::RC>& qp, 
                          char *test_buf, const int32_t imm_data) -> void {
  Op<> atomic_op;
  atomic_op.set_atomic_rbuf(
              reinterpret_cast<u64*>(qp->remote_mr.value().buf+ac_addr), 
              qp->remote_mr.value().key)
            .set_fetch_add(imm_data)
            .set_payload(test_buf, sizeof(u64), qp->local_mr.value().lkey);
  RDMA_ASSERT(atomic_op.execute(qp, IBV_SEND_SIGNALED) == ::rdmaio::IOCode::Ok);
  RDMA_ASSERT(qp->wait_one_comp() == IOCode::Ok);
}

DEFINE_uint64(nic_idx, 2, "Which NIC to create QP");
DEFINE_int64(reg_nic_name, 0, "The name to register an opened NIC at rctrl in server.");
DEFINE_int64(reg_mem_name, 73, "The name to register an MR at rctrl.");
DEFINE_int32(reg_nic_port, 8890, "The port number to do seeds fetch.");
void remote_fetch_seeds() {
  RDMA_ASSERT(reconstruct);
  LOG(3) << "enter to ready for remote fetch seeds";
  auto nic = RNic::create(RNicInfo::query_dev_names().at(FLAGS_nic_idx)).value();
  auto qp = RC::create(nic, QPConfig()).value();
  ConnectManager cm("192.168.1.2:"+std::to_string(FLAGS_reg_nic_port++));
  if (cm.wait_ready(5000000, 2) == IOCode::Timeout) // wait 50 second for server to ready, retry 2 times
    RDMA_ASSERT(false) << "cm connect to server timeout";
  auto qp_res = cm.cc_rc("client-qp-"+std::to_string(0), qp, FLAGS_reg_nic_name, QPConfig());
  RDMA_ASSERT(qp_res == IOCode::Ok) << std::get<0>(qp_res.desc);
  auto key = std::get<1>(qp_res.desc);
  RDMA_LOG(4) << "client fetch QP authentical key: " << key;
  auto local_mem = Arc<RMem>(new RMem(20*1024*1024)); //bytes?
  auto local_mr = RegHandler::create(local_mem, nic).value();
  auto fetch_res = cm.fetch_remote_mr(FLAGS_reg_mem_name);
  RDMA_ASSERT(fetch_res == IOCode::Ok) << std::get<0>(fetch_res.desc);
  rmem::RegAttr remote_attr = std::get<1>(fetch_res.desc);
  qp->bind_remote_mr(remote_attr);
  qp->bind_local_mr(local_mr->get_reg_attr().value());
  RDMA_LOG(4) << "remote memory addr client gets is: " << (u64)remote_attr.buf;

  /*  | #clients | #seeds | seed_0 | seed_1 |   */
  char *test_buf = (char *)(local_mem->raw_ptr);
  LOG(3) << "keep polling to see if data over there.";
  while (true) {
    std::this_thread::sleep_for(std::chrono::seconds(2));
    remote_read(0,qp,test_buf,2*sizeof(uint64_t));
    LOG(3) << "the number we read is:" << (*reinterpret_cast<int64_t*>(test_buf));
    if ((*reinterpret_cast<int64_t*>(test_buf))>0) break;
  }
  LOG(3) << "start copy othello and seeds.";
  // start copy data
  uint64_t seeds_num=*reinterpret_cast<int64_t*>(test_buf+8);
  LOG(3) << "seeds number are totally: " << seeds_num;
  //  *(reinterpret_cast<uint64_t*>(test_buf)) = 0;//
  //  remote_write(0,qp,test_buf,sizeof(uint64_t));//
  /*for (uint64_t i=0,start_addr=16; i<seeds_num; i++) {
    remote_read(start_addr,qp,test_buf,sizeof(uint8_t));
    start_addr += i*sizeof(uint8_t);
  }*/
  remote_read(16,qp,test_buf,10*sizeof(uint8_t));
  remote_fetch_and_add(0,qp,test_buf,-1);
  for (uint i =0; i < seeds_num; i++) {
    ludo_lookup_unit->buckets[i].seed = *reinterpret_cast<uint8_t*>(test_buf+i);
  }
  LOG(3) << "finish to copy data back.";
  reconstruct = false;
}

}

