#pragma once

#include <iostream>
#include <cstdint>
#include <mutex>

#include "r2/src/logging.hh"


template<typename KeyType, typename ValType>
struct __attribute__((packed)) PackedStruct {
    KeyType key;
    uint32_t data_length;
    ValType data; //char* data;
};

template<typename KeyType = uint64_t, typename ValType = uint64_t>
class PackedData {
    using packed_struct_t = PackedStruct<KeyType, ValType>;
    #define ARRAY_LENGTH 20000
public:
    packed_struct_t* rawArray;
    explicit PackedData(size_t numElements_): numElements(numElements_), num(0) {
        //std::lock_guard<std::mutex> lock(mutexArray[numElements]);
        // TODO: spinlock.hh
        rawArray = new packed_struct_t[numElements];   //packed_slot rawArray[numElements];
    }
    explicit PackedData(char* reg_mem, size_t numElements_): numElements(numElements_), num(0) {
        size_t structSize = sizeof(packed_struct_t);
        //std::lock_guard<std::mutex> lock(mutexArray[numElements]);
        packed_struct_t packed_struct; 
        rawArray = reinterpret_cast<packed_struct_t*>(reg_mem);
        for (uint64_t i =0; i < numElements; i++) {
            memcpy(reg_mem + i*sizeof(packed_struct), &(packed_struct), sizeof(packed_struct));
        }
    }

    auto read_key(size_t num, KeyType& key) -> uint32_t {
        key = rawArray[num].key;
        return 0;
    }

    auto read_data(size_t num, ValType& val) -> uint32_t {
        val = rawArray[num].data;
        return rawArray[num].data_length;
    }

    auto read_data_with_key_check(const size_t& num, const KeyType& key, ValType& val) -> uint32_t {
        val = rawArray[num].data;
        if(unlikely(rawArray[num].key != key))
            return 0;
        return rawArray[num].data_length;
    }

    auto remove_data_with_key_check(const size_t& num, const KeyType& key) -> bool {
        if(unlikely(rawArray[num].key != key))
            return false;
        memset(&rawArray[num], 0, sizeof(packed_struct_t));
        return true;
    }
    auto bulk_load_data(const KeyType key, const uint32_t data_length, const ValType& data) -> uint32_t {
        if (unlikely(num >= numElements)) {
            LOG(3) << "It s time to end the benchmark, running out the space in block storage.";
            exit(0);
        }
        rawArray[num].key = key;
        rawArray[num].data = data;
        rawArray[num].data_length = data_length;
        //return reinterpret_cast<uint64_t>(&rawArray[num]);
        return num ++;
    }

    inline std::vector<KeyType> read_batch_keys(const std::vector<uint64_t>& addrs) {
        std::vector<KeyType> keys(addrs.size(), 0);
        uint8_t ik = 0;
        for (const uint64_t& addr: addrs) {
            size_t i = addr & ((1ULL<<40)-1);
            keys[ik++] = rawArray[i].key;
        }
        return std::move(keys);
    }

    auto update_data(size_t num, const KeyType key, const uint32_t data_length, const ValType& data) -> uint32_t {
        rawArray[num].key = key;
        rawArray[num].data = data;
        rawArray[num].data_length = data_length;
        return data_length;
    }

    auto remove_data(size_t num) -> uint8_t {
        rawArray[num].data_length = 0;
        return 0;
    }

    auto remote_addr(size_t num) -> uint64_t {
        return num*sizeof(packed_struct_t);
    }

    #ifdef CHAR_VARIABLE_MODE 
        auto read_data(size_t num, char* data) -> uint32_t {
            // std::lock_guard<std::mutex> lock(mutexArray[num]);
            memcpy(data, rawArray[num].data, rawArray[num].data_length);
            return rawArray[num].data_length;
        }

        auto bulk_load_data(KeyType key, uint32_t data_length, void* data) -> uint64_t {
            // std::lock_guard<std::mutex> lock(mutexArray[num]);
            rawArray[num].key = key;
            rawArray[num].data_length = data_length;
            rawArray[num].data = new char[data_length];
            LOG(3) << data_length;
            memcpy(rawArray[num].data, reinterpret_cast<char*>(data), data_length);
            LOG(3) << "3";
            return reinterpret_cast<uint64_t>(&rawArray[num]);
        }

        auto write_data(size_t num, KeyType key, uint32_t data_length, void* data) -> uint64_t {
            // std::lock_guard<std::mutex> lock(mutexArray[num]);
            rawArray[num].key = key;
            rawArray[num].data_length = data_length;
            rawArray[num].data = new char[data_length];
            memcpy(rawArray[num].data, reinterpret_cast<char*>(data), data_length);
            return (uint64_t) rawArray[num];
        }

        auto write_data(size_t num, KeyType key, uint32_t data_length, char* data) -> uint64_t {
            // std::lock_guard<std::mutex> lock(mutexArray[num]);
            rawArray[num].key = key;
            rawArray[num].data_length = data_length;
            memcpy(rawArray[num].data, data, data_length);
            return (uint64_t) rawArray[num];
        }
    #endif

    ~PackedData(){}

private:
    std::atomic<size_t> num;
    const size_t numElements;
    std::mutex mutexArray[ARRAY_LENGTH];

};


