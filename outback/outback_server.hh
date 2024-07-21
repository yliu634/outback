#pragma once

#include "r2/src/thread.hh"                   /// Thread
#include "xutils/local_barrier.hh"            /// PBarrier
#include "ludo/hashutils/hash.h"

#include "outback/trait.hpp"

using namespace test;
using namespace xstore::transport;
using namespace xstore::rpc;
using namespace outback;


/*****  GLOBAL VARIABLES ******/
volatile bool running = true;
std::atomic<size_t> ready_threads(0);
::rdmaio::RCtrl ctrl(8888);
std::mutex* mutexArray;

/*****  LUDO BUCKETS AND UNDERLYING DATA *****/
lru_cache_t* lru_cache;
ludo_lookup_t* ludo_lookup_unit;  // act like seeds
outback::ludo_buckets_t* ludo_buckets;
outback::packed_data_t* packed_data;

namespace outback {

#define RECV_NUM 2048

using SendTrait = UDTransport;
using RecvTrait = UDRecvTransport<RECV_NUM>;
using SManager = UDSessionManager<RECV_NUM>;
using XThread = ::r2::Thread<usize>;   // <usize> represents the return type of a function

thread_local char* rpc_large_reply_buf = nullptr;
thread_local u32 rpc_large_reply_key;

void outback_get_callback(const Header& rpc_header, const MemBlock& args, SendTrait* replyc);
void outback_put_callback(const Header& rpc_header, const MemBlock& args, SendTrait* replyc);
void outback_update_callback(const Header& rpc_header, const MemBlock& args, SendTrait* replyc);
void outback_remove_callback(const Header& rpc_header, const MemBlock& args, SendTrait* replyc);
void outback_scan_callback(const Header& rpc_header, const MemBlock& args, SendTrait* replyc);

// GET
void outback_get_callback(const Header& rpc_header, const MemBlock& args, SendTrait* replyc) {
	// sanity check the requests
  ASSERT(args.sz == sizeof(KeyType)+sizeof(size_t));
  //size_t loc = *(reinterpret_cast<size_t*>(args.mem_ptr));
  size_t loc = *args.interpret_as<size_t>();
  //KeyType key = *args.interpret_as<KeyType>(sizeof(size_t));
	ValType dummy_value; //store the obtained value
  
	auto addr = ludo_buckets->read_addr(loc/SLOTS_NUM_BUCKET, loc%SLOTS_NUM_BUCKET);
  dummy_value = packed_data->rawArray[addr].data;
  // auto res = packed_data->read_data(addr,dummy_value);
  // auto res = packed_data->read_data_with_key_check(addr,key,dummy_value);

	ReplyValue reply;
  if (true) { // res
    reply = { .status = true, .val = dummy_value};
  } else {
    KeyType key = *args.interpret_as<KeyType>(sizeof(size_t));
    auto cache_bit=ludo_buckets->read_cachebit(loc/SLOTS_NUM_BUCKET, loc%SLOTS_NUM_BUCKET);
    if (cache_bit) dummy_value=lru_cache->get(key);
    reply = { .status = false, .val = dummy_value };
  }
  // send
  char reply_buf[64];
  RPCOp op;
  ASSERT(op.set_msg(MemBlock(reply_buf, 64)).set_reply().add_arg(reply));
  op.set_corid(rpc_header.cor_id);
  // LOG(3) << "GET: " << *(args.interpret_as<u64>());
  ASSERT(op.execute(replyc) == IOCode::Ok);
}

// Put
void outback_put_callback(const Header& rpc_header, const MemBlock& args, SendTrait* replyc) {
	// sanity check the requests
  ReplyValue reply;
  char reply_buf[64];
  FastHasher64<K> h;
  
  ASSERT(args.sz == sizeof(size_t)+sizeof(KeyType)+sizeof(ValType));
  size_t loc = *args.interpret_as<size_t>();
	KeyType key = *args.interpret_as<KeyType>(sizeof(size_t)); // I think it is start bytes offset
  ValType val = *args.interpret_as<ValType>(sizeof(KeyType)+sizeof(size_t));
  // LOG(3) << "Put key: " << key;

	// insert
	// rolex_index->insert(key, val);
  // three cases: (1) loc[first][second] is empty; 
  //              (2) loc[first] has at least one empty; 
  //              (3) no empty;
  size_t row=loc/SLOTS_NUM_BUCKET;
  std::unique_lock<std::mutex> lock(mutexArray[row]);
  h.setSeed(ludo_lookup_unit->buckets[row].seed);
  uint8_t slot=uint8_t(h(key)>>62);//withthenewest seed.
  // uint8_t slot = loc%SLOTS_NUM_BUCKET;
  int8_t status=ludo_buckets->check_slots(key,row,slot);
  switch (status) {
    case 0: {
      //LOG(4)<<"case 0: "<<key;
      auto addr=packed_data->bulk_load_data(key,sizeof(ValType),val);
      ludo_buckets->write_addr(key,row,slot,addr);
      reply = { .status = true, .val = 0 };
      // TODO: update to other cNodes
      break;
    }
    case 1: { //update case
      // LOG(4)<<"case 1: "<<key;
      auto addr = ludo_buckets->read_addr(row,slot);
      KeyType key_;
      packed_data->read_key(addr, key_);
      // LOG(3)<< "key_ is: "<< key_ << " while key is: "<< key;
      if (likely(key_ == key)) {
        packed_data->update_data(addr,key,64,val);
        reply = { .status = true, .val = 1 };
        //LOG(4)<<"case 1 with data update";
      } else {
        // same loc, same fp, but not same key
        lru_cache->insert(key,val);
        // TODO: rebuild needed?
        auto addr=packed_data->bulk_load_data(key,sizeof(ValType),val);
        reply = { .status = true, .val = 1 };
      }
      break;
    }
    case 2: {
      bool occupied[4];
      bool success;
      // LOG(4)<<"case 2: "<<key;
      // the actual bucket_addrs returned are the whole uint64_ts
      std::vector<uint64_t> bucket_addrs = ludo_buckets->read_bucket_addrs(row); 
      // key check here(?), yes, checked before switch
      std::vector<KeyType> bucket_keys = packed_data->read_batch_keys(bucket_addrs); 
      // write a distinct data into underlying data
      auto addr=packed_data->bulk_load_data(key,sizeof(ValType),val);
      // try to find a seed to resolve them
      bucket_keys.push_back(key);
      bucket_addrs.push_back(combine(ludo_buckets->fingerprint(key),64,addr));
      assert(bucket_keys.size() <= 4);
      uint8_t seed = 0;
      for (; seed < 255; ++seed) {
        *(uint32_t *) occupied = 0U;
        h.setSeed(seed);
        success = true;
        for (char s = 0; s < bucket_keys.size(); ++s) {
          uint8_t i = uint8_t(h(bucket_keys[s]) >> 62);
          if (occupied[i]) {
            success = false;
            break;
          } else {
            occupied[i] = true; 
          }
        }
        if (success) {
          ludo_lookup_unit->buckets[row].seed = seed;
          ludo_buckets->empty_bucket(row);
          for (char s = 0; s < bucket_keys.size(); ++s) {
            uint8_t i = uint8_t(h(bucket_keys[s]) >> 62);
            ludo_buckets->write_cell(row, i, bucket_addrs[s]);  //whole uint64_t
          }
          break;
        }
      }
      if (!success) { // if (seed >= 255){
        LOG(4) << "something wrong for forcing seeds.";
        exit(0);
      }
      // LOG(4)<<"case 2 leave with new seed: "<<(uint32_t)seed; 
      reply = { .status = true, .val = (1ULL<<8)+seed};
      break;
    }
    case 3: {
      // LOG(4)<<"case 3: "<<key;
      lru_cache->insert(key,val);
      // TODO: rebuild needed?
      auto addr=packed_data->bulk_load_data(key,sizeof(ValType),val);
      // remote cache bit set to 1;
      ludo_buckets->set_cachebit(row,loc%SLOTS_NUM_BUCKET);
      reply = { .status = true, .val = 3 };
      break;
    }
    default:
      reply = { .status = false, .val = 4 };
      LOG(4) << "something wrong happened for placing new key: " << key;
      exit(0);
  }
  lock.unlock();
  RPCOp op;
  ASSERT(op.set_msg(MemBlock(reply_buf, 64)).set_reply().add_arg(reply));
  op.set_corid(rpc_header.cor_id);
  ASSERT(op.execute(replyc) == IOCode::Ok);
}

void outback_update_callback(const Header& rpc_header, const MemBlock& args, SendTrait* replyc){
	// sanity check the requests
  ASSERT(args.sz == sizeof(size_t)+sizeof(KeyType)+sizeof(ValType));
  size_t loc = *args.interpret_as<size_t>();
	KeyType key = *args.interpret_as<KeyType>(sizeof(size_t));
  ValType val = *args.interpret_as<ValType>(sizeof(KeyType)+sizeof(size_t));
  size_t row=loc/SLOTS_NUM_BUCKET;
	// UPDATE
  std::unique_lock<std::mutex> lock(mutexArray[row]);
	auto addr = ludo_buckets->read_addr(row,loc%SLOTS_NUM_BUCKET);
  KeyType key_;
  packed_data->read_key(addr, key_);
  ReplyValue reply;
  if (likely(key_ == key)) {
    packed_data->update_data(addr,key,64,val);
    reply = { .status = true, .val = 1 };
    //LOG(4)<<"case 1 with data update";
  } else {
    lru_cache->insert(key,val);
    // TODO: rebuild needed case?
    auto addr=packed_data->bulk_load_data(key,sizeof(ValType),val);
    reply = { .status = true, .val = 2 };
  }
  lock.unlock();
  // send
  char reply_buf[64];
  RPCOp op;
  ASSERT(op.set_msg(MemBlock(reply_buf, 64)).set_reply().add_arg(reply));
  op.set_corid(rpc_header.cor_id);
  ASSERT(op.execute(replyc) == IOCode::Ok);
}

void outback_remove_callback(const Header& rpc_header, const MemBlock& args, SendTrait* replyc){
	// sanity check the requests
  ASSERT(args.sz == sizeof(KeyType)+sizeof(size_t));
  size_t loc=*args.interpret_as<size_t>();
  KeyType key=*args.interpret_as<KeyType>(sizeof(size_t));
	// UPDATE
  size_t row = loc/SLOTS_NUM_BUCKET;
  std::unique_lock<std::mutex> lock(mutexArray[row]);
  auto addr=ludo_buckets->read_addr(row, loc%SLOTS_NUM_BUCKET);
  auto res=packed_data->remove_data_with_key_check(addr, key);
  if (res) ludo_buckets->remove_addr(row, loc%SLOTS_NUM_BUCKET);
  //else {
  //  //TODO: go to check cache or just back.
  //}
  lock.unlock();
	ReplyValue reply = { .status = true, .val = 0 };
  // send
  char reply_buf[64];
  RPCOp op;
  ASSERT(op.set_msg(MemBlock(reply_buf, 64)).set_reply().add_arg(reply));
  op.set_corid(rpc_header.cor_id);
  ASSERT(op.execute(replyc) == IOCode::Ok);
}

void outback_scan_callback(const Header& rpc_header, const MemBlock& args, SendTrait* replyc){
	// sanity check the requests
  ASSERT(args.sz == sizeof(KeyType)+sizeof(u64));
	KeyType key = *args.interpret_as<KeyType>();
  ValType n = *args.interpret_as<ValType>(sizeof(KeyType));
	// UPDATE
  std::vector<V> result;
  
	// rolex_index->range(key, n, result);
	ReplyValue reply;
  // send
  char reply_buf[64];
  RPCOp op;
  ASSERT(op.set_msg(MemBlock(reply_buf, 64)).set_reply().add_arg(reply));
  op.set_corid(rpc_header.cor_id);
  ASSERT(op.execute(replyc) == IOCode::Ok);
}

}
