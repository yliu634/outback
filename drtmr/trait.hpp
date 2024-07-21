#pragma once

#include "r2/src/common.hh"
#include "r2/src/logging.hh"                  /// logging
#include "r2/src/thread.hh"                   /// Thread
#include "r2/src/libroutine.hh"               /// coroutine

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
#include "outback/packed_data.hh"

#include "./alex/alex_map.h"
#include "./btree/btree_map.h"

#include "./drtmr/clusterhash.h"

using namespace rdmaio;
using namespace xstore;
using namespace btree;
using namespace alex;

namespace drtmr {

using K = u64;
using V = u64;

using alex_map_t = AlexMap<K,V>;
using btree_map_t = btree_map<K,V>;
using cluster_hash_t = RdmaClusterHash;

using packed_data_t = PackedData<K, V>;
using packed_struct_t = PackedStruct<K, V>;

}