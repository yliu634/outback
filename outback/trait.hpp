#pragma once

#include "r2/src/common.hh"
#include "r2/src/logging.hh"                  /// logging
#include "r2/src/thread.hh"                   /// Thread
#include "r2/src/libroutine.hh"               /// coroutine
#include "r2/src/rdma/async_op.hh"              /// AsyncOp

#include "rlib/core/qps/rc.hh"                  /// RC
#include "rlib/core/lib.hh"
#include "rlib/core/rctrl.hh"                 /// RCtrl
#include "rlib/core/common.hh"
#include "rlib/core/naming.hh"                /// DevIdx 
#include "rlib/core/nicinfo.hh"               /// RNicInfo
#include "rlib/benchs/reporter.hh"

#include "xcomm/tests/transport_util.hh"      /// SimpleAllocator
#include "xcomm/src/transport/rdma_ud_t.hh"   /// UDTranstrant, UDRecvTransport, UDSessionManager
#include "xcomm/src/rpc/mod.hh"               /// RPCCore

#include "xutils/local_barrier.hh"            /// PBarrier
#include "xutils/marshal.hh"
#include "xutils/spin_lock.hh"
#include "xutils/huge_region.hh"

#include "ludo/ludo_cp_dp.h"
#include "outback/cache.hh"
#include "outback/packed_data.hh"
#include "outback/ludo_slot.hh"
#include "outback/ludo_seed.hh"
#include "benchs/rolex_util_back.hh"

using namespace rdmaio;
using namespace xstore;

namespace outback {

#define SLOTS_NUM_BUCKET 4

using K = u64;
using V = u64;
using F = u8;
using S = u8;
using DirType = u8;

using ludo_seeds_t = LudoSeeds<S>;
using ludo_buckets_t = LudoBuckets;

using ludo_lookup_t = DataPlaneLudo<K, V>;
using ludo_maintenance_t = ControlPlaneLudo<K, V>;
using othello_lookup_t = DataPlaneOthello<K, V>;

using lru_cache_t = Cache<K,V,std::mutex>;
using packed_data_t = PackedData<K, V>;
using packed_struct_t = PackedStruct<K, V>;

}
