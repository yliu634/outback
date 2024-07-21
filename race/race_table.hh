#pragma once

#include <iostream>
#include <cstdlib>
#include <cmath>
#include <algorithm>

#include "r2/src/logging.hh"

#include "race/spinlock.hh"
#include "race/trait.hpp"
#include "outback/packed_data.hh"

#include <absl/hash/hash.h>

namespace race {

#define CUCKOO_KICK_MODE 0
#define SLOTS_NUM_ASSOCIATED 2
#define SLOTS_NUM_PER_BUCKET 4

template<typename KeyType=uint8_t, 
         typename AddrType=uint64_t>
struct __attribute__((aligned(64), packed)) CuckooBucket {
    uint8_t occupied=0; // x: the lowest 12 bits refers the xxxx,1111,2222 to refer the // 1x1, 1x4, 2x4, 8x4;
    KeyType fps[SLOTS_NUM_PER_BUCKET] = {0};
    uint16_t lens[SLOTS_NUM_PER_BUCKET] = {0};
    AddrType addrs[SLOTS_NUM_PER_BUCKET] = {0};
};

using cuckoo_bucket_t = CuckooBucket<uint8_t, uint64_t>;
template<typename KeyType=uint64_t, 
         typename ValType=uint64_t>
class RaceHashTable {
public:
    cuckoo_bucket_t* cuckoo_buckets;
    explicit RaceHashTable(char* reg_mem, uint64_t entries, 
                           std::vector<KeyType>* exist_keys): bucket_bytes(64) {
        nsize = static_cast<uint64_t>(std::round(1.5*((entries/4)/load_factor)));
        msize = nsize * 2/3;
        cuckoo_buckets = reinterpret_cast<cuckoo_bucket_t*>(reg_mem);
        LOG(4) << (uint64_t)cuckoo_buckets;
        LOG(4) << "nsize 1: " << nsize << ": " << nsize*64/(1024*1024);
        if (exist_keys != nullptr) {
            auto res = bulkload(*exist_keys);
        }
        //cuckoo_bucket_t cuckoo_bucket;
        //for (size_t i = 0; i < nsize; i++) {
        //    memcpy(cuckoo_buckets+i, &(cuckoo_bucket), 64); // no 64 multiple for i
        //    asm volatile("" ::: "memory");
        //}
    }
    explicit RaceHashTable(uint64_t entries, std::vector<KeyType>* exist_keys): bucket_bytes(64) {
        nsize = static_cast<uint64_t>(std::round(1.5*((entries/4)/load_factor)));
        msize = nsize * 2/3;
        if (exist_keys != nullptr) { // unlikely
            cuckoo_buckets = new cuckoo_bucket_t[nsize];
            auto res = bulkload(*exist_keys);
        }
        LOG(4) << "nsize 2: " << nsize;
    }
    ~RaceHashTable(){};

    #if CUCKOO_KICK_MODE
        struct MoveItem{
            KeyType moveKey;
            ValType moveAddr;
            uint16_t moveLen;
        };

        inline bool place(moveItem& mv) {
            std::vector<uint64_t> buckets = cuckoo_hash(mv.moveKey);
            for (uint ibucket=0; ibucket < SLOTS_NUM_ASSOCIATED; ibucket++) {
                cuckoo_bucket_t& bucket = cuckoo_buckets[buckets[ibucket]];
                for (uint slot = 0; slot < SLOTS_NUM_PER_BUCKET; slot++) {
                    if (bucket.occupied & (1 << slot)) 
                        continue;
                    bucket[slot].addr = mv.moveAddr;
                    bucket[slot].len = mv.Len;
                    return true;
                }
            }
            cuckoo_bucket_t& bucket = cuckoo_buckets[buckets[0]];
            uint kickindex = std::rand() % SLOTS_NUM_PER_BUCKET;

            moveItem mv2;
            mv2.moveKey = bucket.keys[kickindex];
            mv2.moveVal =  bucket.vals[kickindex];
            mv2.moveLen = bucket.lens[kickindex];

            bucket.keys[kickindex] = mv.moveKey;
            bucket.vals[kickindex] = mv.moveAddr;
            bucket.lens[kickindex] = mv.moveLen;

            mv = std::move(mv2);
        }

        bool insert(KeyType key, ValType addr, 
                    const uint16_t len = sizeof(KeyType) + sizeof(ValType)){
            MoveItem moveItem = {key, addr, len};
            uint16_t times=100;
            while (times > 0) {
                bool res = place(moveItem);
                if(res) return true;
                times--;
            }

            LOG(4)<< "100 times";
            return false;
        }
    #endif

    inline bool spare_buckets_num(std::vector<uint64_t>& buckets) {
        for (uint i =0; i < SLOTS_NUM_ASSOCIATED; i++) {
            int64_t spare_bucket(-1);
            switch (buckets[i] % 3) {
                case 0:
                    spare_bucket = buckets[i]+1;
                    break;
                case 2:
                    spare_bucket = buckets[i]-1;
                    break;
                default:
                    LOG(4) << "mod impossible.";
                    break;
            }
            if (spare_bucket > 0 && spare_bucket < nsize) {
                buckets[i] = spare_bucket;
            } 
        }
        return true;
    }

    inline uint8_t fingerprint(const KeyType key) {
        return (MurmurHash64(key, 0x87654321) & 0xFF);
    }

    inline bool insert(const KeyType key, const ValType& addr, 
                        const uint16_t len = sizeof(KeyType)+sizeof(ValType)) {
        std::vector<uint64_t> buckets = cuckoo_hash(key);
        for (uint ibucket=0; ibucket < SLOTS_NUM_ASSOCIATED; ibucket++) {
            cuckoo_bucket_t& bucket = cuckoo_buckets[buckets[ibucket]];
            for (uint slot = 0; slot < SLOTS_NUM_PER_BUCKET; slot++) {
                if (bucket.occupied & (1U << slot))  {
                    continue;
                }
                bucket.fps[slot] = fingerprint(key);
                bucket.addrs[slot] = addr;
                bucket.lens[slot] = len;
                bucket.occupied |= 1U<<slot;
                return true;
            }
        }
        // LOG(3) << key << ": get full";
        bool res = spare_buckets_num(buckets);
        if (res) {
            for (uint ibucket=0; ibucket < SLOTS_NUM_ASSOCIATED; ibucket++) {
                cuckoo_bucket_t& bucket = cuckoo_buckets[buckets[ibucket]];
                for (uint slot = 0; slot < SLOTS_NUM_PER_BUCKET; slot++) {
                    if (bucket.occupied & (1U << slot)) 
                        continue;
                    bucket.fps[slot] = fingerprint(key);
                    bucket.addrs[slot] = addr;
                    bucket.lens[slot] = len;
                    bucket.occupied |= 1U << slot;
                    // LOG(3) << key << ": good full";
                    return true;
                }
            }
        }
        LOG(3) << "fallback buffer insert";
        return false;
    }

    inline bool bulkload(const std::vector<KeyType>& exist_keys) {
        size_t nkeys = exist_keys.size();
        for (uint64_t i = 0; i < nkeys; i++) {
            auto res = insert(exist_keys[i], i);
            asm volatile("" ::: "memory");
        }
        LOG(3) << "client: race slots warmed up...";
    }

    inline bool lookup(const KeyType key, ValType& addr, const uint16_t& len) {
        std::vector<uint64_t> nbuckets = cuckoo_hash(key);
        for (uint8_t i = 0; i < SLOTS_NUM_ASSOCIATED; i++) {
            cuckoo_bucket_t& bucket = cuckoo_buckets[i];
            for (uint k = 0; k < SLOTS_NUM_PER_BUCKET; k++) {
                uint8_t fp = fingerprint(key);
                if (fp == bucket.fps[k]) {
                    addr = bucket.addrs[k];
                    len = bucket.lens[k];
                    return true;
                }
            }
        }
        return false;
    }

    inline uint64_t remote_lookup(const KeyType key, uint32_t& read_size) {
        std::vector<uint64_t> nbuckets = cuckoo_hash(key);
        read_size = 0;

        // TODO: Doorbell Reading.
        uint64_t ac_addr = nbuckets[0];
        int64_t spare_bucket(-1);
        switch (ac_addr % 3) {
            case 0:
                spare_bucket = ac_addr+1;
                break;
            case 2:
                spare_bucket = ac_addr-1;
                break;
            default:
                LOG(4) << "mod impossible.";
                break;
        }
        if (spare_bucket > 0 && spare_bucket < nsize) {
            read_size = 2 * bucket_bytes;
        } else {
            read_size = bucket_bytes;
        }
        ac_addr = ac_addr < spare_bucket? ac_addr:spare_bucket;
        return ac_addr*bucket_bytes;
    }

    inline uint64_t total_size() {
        return nsize*64;
    }

private:
    uint64_t nsize;
    uint64_t msize;
    size_t bucket_bytes;
    const double load_factor=0.85;
    std::hash<std::string> hasher;

    static inline uint64_t MurmurHash64 (uint64_t key, unsigned int seed)  {
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

    uint64_t hash_to_cuckoo_index(const uint64_t hashindex) {
        if (hashindex & 1UL)
            return (3*(hashindex/2+1)-1) > 0? (3*(hashindex/2+1)-1):0;
        else return 3*(hashindex/2) < (nsize-1)? 3*(hashindex/2):nsize-1;
    }

    std::vector<uint64_t> cuckoo_hash(const KeyType key, const uint16_t klen = 8*sizeof(KeyType)) {
        uint64_t seed = MurmurHash64(key, 0x1234);
        uint64_t hash = seed^(0x9e3779b9+(seed<<6)+(seed>>2));
        return {hash_to_cuckoo_index(seed % msize),
                hash_to_cuckoo_index(hash % msize)};
    }

};


}