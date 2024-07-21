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

#include "mica/mica.h"

using namespace rdmaio;
using namespace xstore;

namespace fasst {

using K = u64;
using V = u64;

using packed_data_t = PackedData<K, V>;
using packed_struct_t = PackedStruct<K, V>;

uint64_t murmurhash(K key, unsigned int seed = 0x90123456)  {
    const uint64_t m = 0xc6a4a7935bd1e995;
    const int r = 47;
    uint64_t h = seed ^ (8 * m);
    const uint64_t * data = &key;
    const uint64_t * end = data + 1;

    while(data != end)  {
        uint64_t k = *data++;
        k *= m;
        k ^= k >> r;
        k *= m;
        h ^= k;
        h *= m;
    }

    const unsigned char * data2 = (const unsigned char*)data;

    switch(8 & 7)   {
        case 7: h ^= uint64_t(data2[6]) << 48;
        case 6: h ^= uint64_t(data2[5]) << 40;
        case 5: h ^= uint64_t(data2[4]) << 32;
        case 4: h ^= uint64_t(data2[3]) << 24;
        case 3: h ^= uint64_t(data2[2]) << 16;
        case 2: h ^= uint64_t(data2[1]) << 8;
        case 1: h ^= uint64_t(data2[0]);
        h *= m;
    };

    h ^= h >> r;
    h *= m;
    h ^= h >> r;

    return h;
}

inline uint16_t hashtag(K key, unsigned int seed = 0x7a2c531) {
    return (uint16_t) murmurhash(key, 0x7a2c531) & 0xFFFF;
}
inline uint32_t hashbucket(K key) {
    return (uint32_t) murmurhash(key, 0x90123456) & 0xFFFFFFFF;
}


}