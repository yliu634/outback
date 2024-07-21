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

#include "race/race_table.hh"
#include "outback/packed_data.hh"

using namespace rdmaio;
using namespace xstore;

namespace race {

using K = u64;
using V = u64;

using race_hash_t = RaceHashTable<K, V>;

using packed_data_t = PackedData<K, V>;
using packed_struct_t = PackedStruct<K, V>;

}