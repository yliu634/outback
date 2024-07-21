#pragma once

#include "outback/trait.hpp"

using namespace r2;
using namespace rdmaio;
using namespace test;
using namespace xstore::util;
using namespace xstore::rpc;
using namespace xstore::transport;
using namespace outback;


/*****  GLOBAL VARIABLES *******/
// ludo_seeds_t* ludo_seeds;
ludo_lookup_t* ludo_lookup_unit;  // act like seeds

namespace outback {


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
  if(r.status) {
    return (ValType)r.val;
  }
  return {};
}

void remote_put(const KeyType& key, const ValType& val, RPC& rpc, UDTransport& sender, R2_ASYNC)
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
    } else if (val>=1) { // cache it please in client side? NO NEED!
      //TODO: cache it
    } // else if (val < 1) // insert with seeds change.
  } else { //r.status = false
    LOG(4) << "It goes to the default case and wrong";
    exit(0);
  }
  
}

void remote_update(const KeyType& key, const ValType& val, RPC& rpc, UDTransport& sender, R2_ASYNC)
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

