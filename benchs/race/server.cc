#include <gflags/gflags.h>

#include "rlib/core/lib.hh"          /// logging, RNicInfo
#include "rlib/core/nicinfo.hh"      /// RNicInfo
#include "rlib/core/rctrl.hh"        /// RCtrl
#include "rlib/core/common.hh"       /// LOG
#include "r2/src/thread.hh"                   /// Thread
#include "xcomm/src/rpc/mod.hh"               /// RPCCore
#include "xutils/huge_region.hh"

#include "benchs/load_data.hh"
#include "benchs/load_config.hh"
#include "benchs/rolex_util_back.hh"
#include "race/trait.hpp"

#define REMOTE_MEMORY_MODE 0
#define DEBUG_MODE_CHECK 0

DEFINE_int64(use_nic_idx, 0, "Which NIC to create QP");
DEFINE_int64(reg_nic_name, 0, "The name to register an opened NIC at rctrl.");
DEFINE_int64(reg_mem_name, 73, "The name to register an MR at rctrl.");

DEFINE_int64(magic_num, 0x6e, "The magic number read by the client");

using namespace rdmaio;
using namespace rdmaio::rmem;
using namespace bench;
using namespace race;
using namespace xstore::util;

namespace race {

race::race_hash_t* race_table;
race::packed_data_t* packed_data;
std::shared_ptr<::HugeRegion> leaf_region;


void setup_race_hash_packed_data(char* reg_mem) {
  //race_table = new (reinterpret_cast<void*>(reg_mem)) race_hash_t(FLAGS_nkeys);
  //race_table = new (location) race_hash_t(FLAGS_nkeys);
  ///
  race_table = new race_hash_t(reg_mem, FLAGS_nkeys, nullptr);
  packed_data = new packed_data_t(reg_mem + race_table->total_size(),3*FLAGS_nkeys/2);
  
  LOG(3) << "setup_race_hash_table (internal)";
  for (uint64_t i = 0; i < FLAGS_nkeys; i++) {
    KeyType key = exist_keys[i];
    V addr = packed_data->bulk_load_data(key, sizeof(V), key);
    race_table->insert(key, addr);
    r2::compile_fence();
  }

  #if DEBUG_MODE_CHECK
    for (uint64_t i = 0; i < FLAGS_nkeys; i++) {
      uint32_t len(64);
      u32 ac_addr = race_table->remote_lookup(exist_keys[i], len);
      for (int k = 0; k < 2; k++) {
        cuckoo_bucket_t* bucket = reinterpret_cast<cuckoo_bucket_t*>(reg_mem+ac_addr+k*sizeof(cuckoo_bucket_t));
        LOG(4) << "key: " << exist_keys[i];
        printf("occupied: %u\n", bucket->occupied & 0xF);
        if (bucket->occupied & 0xF != 0) {
          for (int i = 0; i < 4; ++i) {
            printf("fps[%d]: %u\n", i, bucket->fps[i]);
            printf("lens[%d]: %ul\n", i, bucket->lens[i]);
            printf("addrs[%d]: %lu\n", i, bucket->addrs[i]);
          }
        }
      }
    }
  #endif

  LOG(3) << "race slots warmed up...";
}
}

int main(int argc, char **argv) {

  gflags::ParseCommandLineFlags(&argc, &argv, false);

  // start a controler, so that others may access it using UDP based channel
  RCtrl* ctrl = new RCtrl(8888);
  RDMA_LOG(4) << "Pingping server listenes at localhost: " << "8888";

  #if REMOTE_MEMORY_MODE
    RM_config conf(ctrl, FLAGS_leaf_num*sizeof(leaf_t), FLAGS_reg_mem_name, FLAGS_leaf_num); //total size, name, number
    remote_memory_t* RM = new remote_memory_t(conf);  //reg nic_name as 0 by default

    u64 *reg_mem = (u64 *)(RM->leaf_region_ptr());

    RDMA_LOG(2) << "registered memory addr is: " << (u64) reg_mem;
    // setup the value
    for(uint i = 0;i <8;++i) {
      reg_mem[i] = FLAGS_magic_num + i;
      asm volatile("" ::: "memory");
    }
    RM->start_daemon();
  #else
    {
      auto nic =
          RNic::create(RNicInfo::query_dev_names().at(FLAGS_use_nic_idx)).value();
      // register the nic with name 0 to the ctrl
      RDMA_ASSERT(ctrl->opened_nics.reg(FLAGS_reg_nic_name, nic));
    }

    // allocate a memory (with 1024 bytes) so that remote QP can access it
    RDMA_ASSERT(ctrl->registered_mrs.create_then_reg(
        FLAGS_reg_mem_name, Arc<RMem>(new RMem(64*FLAGS_nkeys)), // FLAGS_threads*2*1024*1024
        ctrl->opened_nics.query(FLAGS_reg_nic_name).value()));
    
    // initialzie the value so as client can sanity check its content
    char *reg_mem = (char *)(ctrl->registered_mrs.query(FLAGS_reg_mem_name)
                              .value()
                              ->get_reg_attr()
                              .value()
                              .buf);

    //leaf_region = HugeRegion::create(FLAGS_threads*8*1024*1024).value();
    //ctrl->registered_mrs.create_then_reg( // 0 is the name not idx
    //  FLAGS_reg_mem_name, leaf_region->convert_to_rmem().value(), ctrl->opened_nics.query(0).value());

    RDMA_LOG(2) << "registered memory start addr is: " << (u64) reg_mem; // reinterpret_cast<u64>(leaf_region.get());
    
    
    // setup the value
    LOG(2) << "[Server loading data] ...";
    bench::load_benchmark_config();
    bench::load_data();

    LOG(2) << "[Server setup race table] ...";
    setup_race_hash_packed_data(reg_mem);


    // start the listener thread so that client can communicate w it
    ctrl->start_daemon();

  #endif

  RDMA_LOG(2) << "Race pingpong server started!";
  // run for 20 seconds
  for (uint i = 0;i < FLAGS_seconds+10; ++i) {
    // server does nothing because it is one sided RDMA
    // client will read the reg_mem using RDMA
    sleep(1);
  }
  
  RDMA_LOG(4) << "server exit!";
}
