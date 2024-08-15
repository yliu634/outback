#pragma once

#include <absl/hash/hash.h>

#include "r2/src/thread.hh"                   /// Thread
#include "xutils/local_barrier.hh"            /// PBarrier
#include "ludo/hashutils/hash.h"

#include "benchs/load_config.hh"
#include "benchs/load_data.hh"
#include "outback/trait.hpp"

using namespace test;
using namespace bench;
using namespace xstore::transport;
using namespace xstore::rpc;
using namespace outback;

/*****  GLOBAL VARIABLES ******/
std::mutex* mutexArray;
::rdmaio::RCtrl ctrl(8888);
std::atomic<size_t> ready_threads(0);
std::atomic<uint8_t> global_depth(0);
volatile bool running(true),reconstruct(false);

/*****  LUDO BUCKETS AND UNDERLYING DATA *****/
lru_cache_t* lru_cache;
ludo_lookup_t* ludo_lookup_unit;  // act like seeds
outback::ludo_buckets_t** ludo_buckets;
outback::packed_data_t* packed_data;
std::vector<uint8_t> bucketLocks;
std::vector<uint8_t> local_depths;

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
void outback_reconstruct_table(DirType oldDir, uint64_t _size);

void outback_get_callback(const Header& rpc_header, const MemBlock& args, SendTrait* replyc) {
	// sanity check the requests
  ASSERT(args.sz == sizeof(KeyType)+sizeof(size_t));
  //size_t loc = *(reinterpret_cast<size_t*>(args.mem_ptr));
  size_t loc = *args.interpret_as<size_t>();
  KeyType key = *args.interpret_as<KeyType>(sizeof(size_t));
	ValType dummy_value; //store the obtained value
  DirType dir = 0; //*args.interpret_as<DirType>(sizeof(size_t)+sizeof(KeyType));

  ReplyValue reply;
  if (unlikely(reconstruct && bucketLocks[loc/SLOTS_NUM_BUCKET])) {
    reply = { .status = false, .val = 0};
  } else {
    auto addr = ludo_buckets[dir]->read_addr(loc/SLOTS_NUM_BUCKET, loc%SLOTS_NUM_BUCKET);
    auto res = packed_data->read_data(addr,dummy_value);
    // auto res=packed_data->read_data_with_key_check(addr,key,dummy_value);
    if (res) {
      reply = { .status = true, .val = dummy_value};
    } else {
      auto cache_bit=ludo_buckets[dir]->read_cachebit(loc/SLOTS_NUM_BUCKET, loc%SLOTS_NUM_BUCKET);
      if (cache_bit) dummy_value=lru_cache->get(key);
      reply = { .status = true, .val = dummy_value };
    }
  }
  // send
  char reply_buf[64];
  RPCOp op;
  ASSERT(op.set_msg(MemBlock(reply_buf, 64)).set_reply().add_arg(reply));
  op.set_corid(rpc_header.cor_id);
  // LOG(3) << "GET: " << *(args.interpret_as<u64>());
  ASSERT(op.execute(replyc) == IOCode::Ok);
}

void outback_put_callback(const Header& rpc_header, const MemBlock& args, SendTrait* replyc) {
	// sanity check the requests
  ReplyValue reply;
  char reply_buf[64];
  FastHasher64<K> h;
  
  ASSERT(args.sz == sizeof(size_t)+sizeof(KeyType)+sizeof(ValType));
  size_t loc = *args.interpret_as<size_t>();
	KeyType key = *args.interpret_as<KeyType>(sizeof(size_t)); // I think it is start bytes offset
  ValType val = *args.interpret_as<ValType>(sizeof(KeyType)+sizeof(size_t));
  DirType dir = 0; // *args.interpret_as<DirType>(sizeof(size_t)+sizeof(KeyType)+sizeof(ValType)) 
                      // & (1U<<local_depths[dir]-1);
  // LOG(3) << "Put key: " << key;

	// insert
	// rolex_index->insert(key, val);
  // three cases: (1) loc[first][second] is empty; 
  //              (2) loc[first] has at least one empty; 
  //              (3) no empty;
  size_t row=loc/SLOTS_NUM_BUCKET;
  if (!reconstruct) {
    std::unique_lock<std::mutex> lock(mutexArray[row]);
    h.setSeed(ludo_lookup_unit->buckets[row].seed);
    uint8_t slot=uint8_t(h(key)>>62);//withthenewest seed.
    // uint8_t slot = loc%SLOTS_NUM_BUCKET;
    int8_t status=ludo_buckets[dir]->check_slots(key,row,slot);
    switch (status) {
      case 0: {
        //LOG(4)<<"case 0: "<<key;
        auto addr=packed_data->bulk_load_data(key,sizeof(ValType),val);
        ludo_buckets[dir]->write_addr(key,row,slot,addr);
        reply = { .status = true, .val = 0 };
        // TODO: update to other cNodes
        break;
      }
      case 1: { //update case
        // LOG(4)<<"case 1: "<<key;
        auto addr = ludo_buckets[dir]->read_addr(row,slot);
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
        std::vector<uint64_t> bucket_addrs = ludo_buckets[dir]->read_bucket_addrs(row); 
        // key check here(?), yes, checked before switch
        std::vector<KeyType> bucket_keys = packed_data->read_batch_keys(bucket_addrs); 
        // write a distinct data into underlying data
        auto addr=packed_data->bulk_load_data(key,sizeof(ValType),val);
        // try to find a seed to resolve them
        bucket_keys.push_back(key);
        bucket_addrs.push_back(combine(ludo_buckets[dir]->fingerprint(key),64,addr));
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
            ludo_buckets[dir]->memset_bucket(row);
            for (char s = 0; s < bucket_keys.size(); ++s) {
              uint8_t i = uint8_t(h(bucket_keys[s]) >> 62);
              ludo_buckets[dir]->write_cell(row, i, bucket_addrs[s]);  //whole uint64_t
            }
            break;
          }
        }
        if (!success) { // if (seed >= 255){
          LOG(4) << "something wrong for forcing seeds.";
          for (const auto &el: bucket_keys) LOG(3) << el;
          exit(0);
        }
        // LOG(4)<<"case 2 leave with new seed: "<<(uint32_t)seed; 
        reply = { .status = true, .val = (1ULL<<8)+seed};
        break;
      }
      case 3: {
        if (lru_cache->size()<lru_cache->getMaxSize()) {
          // insert kv into the lru cache
          lru_cache->insert(key,val);
          auto addr=packed_data->bulk_load_data(key,sizeof(ValType),val);
          // remote cache bit set to 1
          ludo_buckets[dir]->set_cachebit(row,loc%SLOTS_NUM_BUCKET);
          reply = { .status = true, .val = 3 };
          LOG(3) << "current lru cache size: " << lru_cache->size();
        } else if (lru_cache->size()>=lru_cache->getMaxSize()) { //rebuild needed
          LOG(3) << "lru cache size reaches mx size, and reconstrcut start:";
          if (!reconstruct) {
            reconstruct = true;
            std::thread reconstrcut_thread(outback_reconstruct_table, dir, 50);
            reconstrcut_thread.detach(); //rebuild it.
          }
          reply = { .status = false, .val = 3 };
        }
        break;
      }
      default:
        reply = { .status = false, .val = 4 };
        LOG(4) << "something wrong happened for placing new key: " << key;
        exit(0);
    }
    lock.unlock();
  } else {
     bucketLocks[row] = 1;
     reply = { .status = false, .val = 5 };
  }
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
  DirType dir = 0; //*args.interpret_as<DirType>(sizeof(size_t)+sizeof(KeyType)+sizeof(ValType));
  size_t row=loc/SLOTS_NUM_BUCKET;
	// UPDATE
  ReplyValue reply;
  if (unlikely(reconstruct && bucketLocks[loc/SLOTS_NUM_BUCKET])) {
    reply = { .status = false, .val = 0};
  } else {
    std::unique_lock<std::mutex> lock(mutexArray[row]);
    auto addr = ludo_buckets[dir]->read_addr(row,loc%SLOTS_NUM_BUCKET);
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
  }
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
  DirType dir = 0; //*args.interpret_as<DirType>(sizeof(size_t)+sizeof(KeyType));
	// UPDATE
  size_t row = loc/SLOTS_NUM_BUCKET;
  std::unique_lock<std::mutex> lock(mutexArray[row]);
  auto addr=ludo_buckets[dir]->read_addr(row, loc%SLOTS_NUM_BUCKET);
  auto res=packed_data->remove_data_with_key_check(addr, key);
  if (res) ludo_buckets[dir]->remove_addr(row, loc%SLOTS_NUM_BUCKET);
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
  DirType dir = 0; //*args.interpret_as<DirType>(sizeof(size_t)+sizeof(KeyType));
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


DEFINE_int64(reg_nic_name, 0, "The name to register an opened NIC at rctrl.");
DEFINE_int64(reg_mem_name, 73, "The name to register an MR at rctrl.");
DEFINE_int64(magic_num, 0x6e, "The magic number read by the client");
DEFINE_int64(clts_num, 0x01, "The client number reads new seeds here.");
DEFINE_int32(reg_nic_port, 8890, "The port number to do seeds fetch.");
void outback_reconstruct_table(DirType oldDir, uint64_t _size){ //mb
  LOG(2) << "Outback reconstruction for Outback starts...";
  ASSERT(reconstruct);
  RCtrl* rctrl = new RCtrl(FLAGS_reg_nic_port++);
  auto nic = RNic::create(RNicInfo::query_dev_names().at(FLAGS_nic_idx)).value();
  RDMA_ASSERT(rctrl->opened_nics.reg(FLAGS_reg_nic_name, nic));
  // allocate a memory (with 1024 bytes) so that remote QP can access it
  RDMA_ASSERT(rctrl->registered_mrs.create_then_reg(
      FLAGS_reg_mem_name, Arc<RMem>(new RMem(_size*1024*1024)), //64*FLAGS_nkeys)), //FLAGS_threads*2*1024*1024
      rctrl->opened_nics.query(FLAGS_reg_nic_name).value()));
  // initialzie the value so as client can sanity check its content
  char *reg_mem = (char *)(rctrl->registered_mrs.query(FLAGS_reg_mem_name).value()->get_reg_attr().value().buf);
  RDMA_LOG(2) << "outback registered memory start addr is: " << (u64) reg_mem;
  std::memset(reg_mem, 0, sizeof(uint64_t)); // set how many clients will read it out, first set 0 as not available
  rctrl->start_daemon();
  LOG(3) << "setup one-sided rdma server.";

  // TODO: recalculate all the seeds 
  // othello only needs to copy 49-53 lines variables, especially mem
  //{
    //std::this_thread::sleep_for(std::chrono::seconds(60));//extendible hashing depths
    LOG(3) << "recompute othello and ludo with: " << exist_keys.size() << " keys";
    //ludo_maintenance_t ludo_maintenance_unit(FLAGS_nkeys,false,exist_keys,exist_keys);
    ludo_maintenance_t ludo_maintenance_unit(FLAGS_nkeys,false,
        std::vector<KeyType>(exist_keys.begin(), exist_keys.begin() + exist_keys.size()/2),
        std::vector<KeyType>(exist_keys.begin(), exist_keys.begin() + exist_keys.size()/2));
    //DirType newDir = oldDir+(1U<<(global_depth++));
    ludo_lookup_t ludo_lookup_table(ludo_maintenance_unit,ludo_buckets[++global_depth]);
    ASSERT(global_depth<9) // for debug
  //}

  lru_cache->clear();
  asm volatile ("" ::: "memory");
  auto &seeds = ludo_lookup_unit->buckets;
  //std::memcpy(reg_mem+8, &(seeds.size()), sizeof(uint64_t)); //size
  *(reinterpret_cast<uint64_t*>(reg_mem+8)) = seeds.size();
  for (uint32_t i =0; i < seeds.size(); i++) { // save all seeds to reg_mem;
    *(reg_mem+16+i) = seeds[i].seed;
  }
  *(reinterpret_cast<uint64_t*>(reg_mem)) = FLAGS_clts_num;
  //std::memset(reg_mem, FLAGS_clts_num, sizeof(uint64_t)); // can make clients to fetch it
  LOG(3) << "keep waiting client minus flag ...";
  while ((*reinterpret_cast<int64_t*>(reg_mem)) > 0) {
    std::this_thread::sleep_for(std::chrono::seconds(2));
  }
  std::this_thread::sleep_for(std::chrono::seconds(1)); // even it goes 0, wait to sure they finished
  LOG(2) << "all clients copied them away.";
  std::fill(bucketLocks.begin(), bucketLocks.end(), 0);
  rctrl->stop_daemon();

  //extendible hashing remove half data from the old table
  LOG(2) << "start moving half of keys.";
  for (uint32_t row = 0; row < ludo_buckets[oldDir]->size(); row++) {
    for (uint32_t slot = 0; slot < SLOTS_NUM_BUCKET; slot++) {
      if(ludo_buckets[oldDir]->empty_slot(row,slot) <= 0) {// not empty
        KeyType key;
        packed_data->read_key(ludo_buckets[oldDir]->read_addr(row,slot),key); //read data key
        if (absl::HashOf(key)&(1U<<local_depths[oldDir])) { // hash to see if it needs to be moved
          ludo_buckets[oldDir]->remove_mark_slot(row,slot); // move it
        }
      }
    }
  }
  LOG(2) << "finish moving half of keys, local depth++";
  local_depths[oldDir]++;
  reconstruct = false;
}

}
