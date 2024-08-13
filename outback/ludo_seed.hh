#pragma once

#include <iostream>
#include <cstdint>
#include <mutex>
#include <atomic>

#include "r2/src/logging.hh"
//#include "race/spinlock.hh"

namespace outback {

template<typename SeedType = uint8_t>
class LudoSeeds {
public:
    std::atomic<uint8_t>* seedsArray;
    LudoSeeds(){}
    explicit LudoSeeds(size_t numElements_): numElements(numElements_) {
        // mutexArray = new std::mutex[numElements];
        // std::lock_guard<std::mutex> lock(mutexArray[numElements]);
        seedsArray = new std::atomic<SeedType>[numElements];
    }

    auto read_seed(const size_t num, uint8_t& val) -> int {
        val = seedsArray[num].load(std::memory_order_relaxed);
        return true;
    }

    auto write_seed(const size_t num, const uint8_t& val) -> int {
        seedsArray[num].store(val, std::memory_order_relaxed);
        return 0;
    }

    #ifdef SLOT_COMPUTATION 
        auto comp_slot(const KeyType key, const size_t num) -> uint8_t {
            return reinterpret_cast<uint8_t>(absl::HashOf(key, 0, seedsArray[num]) >> 62);
        }
    #endif

    ~LudoSeeds(){
        delete[] seedsArray;
    }
private: 
    const uint32_t numElements;
    std::mutex* mutexArray;
};

}