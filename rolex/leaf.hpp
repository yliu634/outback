#pragma once

#include <limits>
#include <iostream>
#include <optional>
#include <utility> 

#include "r2/src/common.hh"


using namespace r2;


namespace rolex {

// bucket
template<usize N = 4, typename K = u64, typename V = u64>
struct __attribute__((packed)) Leaf
{
  K keys[N];
  V vals[N];

  Leaf() {
    for (uint i = 0; i < N; ++i) {
      this->keys[i] = invalidKey();
    }
  }

  inline static K invalidKey() { return std::numeric_limits<K>::max(); }

  inline static usize max_slot() { return N; }
    
    
  bool isfull() { 
    for (uint i = 0; i < N; ++i) {
      if (keys[i]==invalidKey())  return false;}
    return true;
  }

  bool isEmpty() { 
    for (uint i = 0; i < N; ++i) {
      if (keys[i]!=invalidKey())  return false;}
      return true;
  }

  K last_key() { return keys[N-1]; }

  /**
   * @brief The start size of vals, vals offset inside Leaf
   */
  static auto value_start_offset() -> usize { return offsetof(Leaf, vals); }


  // ================== API functions: search, update, insert, remove ==================
  auto search(const K &key, V &val) -> bool {
    for (uint i = 0; i < N; ++i) {
      if (this->keys[i] == key) {
        val = vals[i];
        return true;
      }
    }
    return false;
  }

  auto contain(const K &key) ->bool {
    for (uint i = 0; i < N; ++i) {
      if (this->keys[i] == key) 
        return true;
    }
    return false;
  }

  auto update(const K &key, const V &val) -> bool {
    for (uint i = 0; i < N; ++i) {
      if (this->keys[i] == key) {
        vals[i] = val;
        return true;
      }
    }
    return false;
  }


  /**
   * @brief Remove the data and reset the empty slot as invaidKey()
   * 
   * @return std::optional<u8> the idx of the removed key in the leaf if success
   */
  auto remove(const K &key) -> bool {
    for (u8 i = 0; i < N; ++i) {
      if (this->keys[i] == key) {
        //std::copy(keys+i+1, keys+N, keys+i); // put elements [i+1:N] by one pos
        this->keys[i] = invalidKey();
        return true;
      }
    }
    return false;
  }


  // ============== functions for degugging ================
  void print() {
    for(int i=0; i<N; i++) {
      std::cout<<keys[i]<<" ";
    }
    std::cout<<std::endl;
  }


  //  ============ functions for remote machines =================
  /**
   * @brief Insert the data from the compute nodes
   * 
   * @return std::pair<u8, u8> <the state of the insertion, the position>
   *    state: 0--> key exists,   1 --> insert here, 2--> insert here and create a new leaf,  3 --> insert to next leaf
   */
  auto insert_to_remote(const K &key, const V &val) -> int {
    u8 i=0;
    this->keys[i] = key;
    this->values[i] = val;
    return 1;
  }

};


} // namespace rolex