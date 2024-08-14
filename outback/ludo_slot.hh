#pragma once

#include <iostream>
#include <cstdint>
#include <mutex>

#include "ludo/hashutils/hash.h"
#include "r2/src/logging.hh"

namespace outback {

#define SLOTS_NUM_BUCKET 4
#define AddrType uint64_t

struct alignas(32) LudoBucket {
    // version number + length + addr.
    // : 4 + 12 + 48 bits;  
    // flag + finger + length + addr;
    // : 1 + 7 + 16 + 40 bits
    uint64_t slots[SLOTS_NUM_BUCKET] = {0};
};

inline uint64_t combine(const uint8_t finger, const uint16_t length,
                        const uint64_t& addr) {
    uint64_t res(0);
    res =   (1ULL<<63) +
            ((static_cast<uint64_t>(finger&0x7F))<<56) +
            ((static_cast<uint64_t>(length&0xFFFF))<<40) +
            (addr&((1ULL<<40)-1));
    return res;
}
inline uint8_t toCachebit(const uint64_t& num) {
    return static_cast<uint8_t>((num >> 63) & 0x01);
}
inline uint8_t toFinger(const uint64_t& num) {
    return static_cast<uint8_t>((num >> 56) & 0x7F);
}
inline uint16_t toLength(const uint64_t& num) {
    return static_cast<uint16_t>((num >> 40) & 0xFFFF);
}
inline uint64_t toAddr(const uint64_t& num) {
    return num & ((1ULL<<40)-1);
}


class LudoBuckets {
    using ludo_bucket = LudoBucket;
public:
    ludo_bucket* bucketsArray;
    FastHasher64<uint64_t> hp;
    explicit LudoBuckets(size_t numElements_): numElements(numElements_) {
        hp.setSeed(0x93ea45);
        size_t structSize = sizeof(ludo_bucket);
        //mutexArray = new std::mutex[numElements];
        bucketsArray = new ludo_bucket[numElements];   //packed_slot rawArray[numElements];
    }

    inline uint8_t fingerprint(const uint64_t key) {
        return hp(key) & 0x7F;
    }

    inline int8_t check_slots(const uint64_t key, size_t row, uint8_t slot) {
        //std::unique_lock<std::mutex> lock(mutexArray[row]);
        if (!toLength(bucketsArray[row].slots[slot]))
            return 0; // good position
        if (toFinger(bucketsArray[row].slots[slot]) == fingerprint(key)) 
            return 1; // maybe update
        for (uint8_t idx=0; idx < SLOTS_NUM_BUCKET; idx++) {
            if (!toLength(bucketsArray[row].slots[idx])) 
                return 2; // empty slots exists
        }
        return 3;
    }
    
    auto inline read_addr(size_t row, uint8_t slot) -> AddrType {
        AddrType addr = toAddr(bucketsArray[row].slots[slot]);
        return std::move(addr);
    }

    auto inline empty_slot(const size_t row, const size_t slot) -> bool {
        return toLength(bucketsArray[row].slots[slot]);
    }

    auto remove_mark_slot(size_t row, uint8_t slot) -> void {
        bucketsArray[row].slots[slot] &= (~(0xFFFFULL << 40));
        //ASSERT(!toLength(bucketsArray[row].slots[slot]));
    }

    auto remove_addr(size_t row, uint8_t slot) -> void {
        bucketsArray[row].slots[slot] = 0;
    }

    auto read_cachebit(size_t row, uint8_t slot) -> bool {
        if (toCachebit(bucketsArray[row].slots[slot])) 
            return true;
        return false;
    }

    void set_cachebit(size_t row, uint8_t slot) {
        bucketsArray[row].slots[slot] |= 1UL<<63;
    }

    inline std::vector<AddrType> read_bucket_addrs(size_t row) {
        std::vector<AddrType> addrs;
        for (uint slot = 0; slot < SLOTS_NUM_BUCKET; slot++) {
            if (toLength(bucketsArray[row].slots[slot])) {
                addrs.push_back(toAddr(bucketsArray[row].slots[slot]));
            }
        }
        return std::move(addrs);
    }

    inline auto write_addr(const uint64_t key, size_t row, uint8_t slot, AddrType addr, 
            uint16_t length=64) -> int {
        // std::lock_guard<std::mutex> lock(mutexArray[row]);
        bucketsArray[row].slots[slot] = combine(fingerprint(key), length, addr);
        return 0;
    }

    inline auto memset_bucket(const size_t row) -> int {
        //memset(bucketsArray[row].slots,0,4*sizeof(uint64_t));
        for (uint8_t slot = 0; slot < SLOTS_NUM_BUCKET; ++slot) {
            bucketsArray[row].slots[slot] = 0ULL;
        }
        return 0;
    }

    inline auto write_cell(size_t row, uint8_t slot, const uint64_t val) -> int {
        // std::lock_guard<std::mutex> lock(mutexArray[row]);
        bucketsArray[row].slots[slot] = val;
        return 0;
    }

    auto size() -> uint32_t {
        return numElements;
    }

    ~LudoBuckets(){
        delete[] bucketsArray;
        delete[] mutexArray;
    }

private:
    const size_t numElements;
    std::mutex* mutexArray;

};

}

