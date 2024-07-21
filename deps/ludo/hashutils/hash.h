/*!
 \file hash.h
 Describes hash functions used in this project.
 */

#pragma once

#include <functional>
#include <type_traits>
#include <cinttypes>
#include <string>
#include <iostream>

#include "farmhash.h"
#include "hashutil.h"
#include "xxhash.h"
#include <absl/hash/hash.h>

template<class K>
class Hasher32 {
public:
  uint32_t s;    //!< hash s.
  
  Hasher32()
    : s(0xe2211) {
  }
  
  explicit Hasher32(uint32_t _s)
    : s(_s) {
  }
  
  //! set bitmask and s
  void setSeed(uint32_t _s) {
    s = _s;
  }
  
  template<class K1>
  inline typename std::enable_if<!std::is_same<K1, std::string>::value, uint64_t *>::type
  getBase(const K &k0) const {
    uint64_t *base;
    return (uint64_t *) &k0;
  }
  
  template<class K1>
  inline typename std::enable_if<std::is_same<K1, std::string>::value, uint64_t *>::type
  getBase(const K &k0) const {
    uint64_t *base;
    return (uint64_t *) &k0[0];
  }
  
  template<class K1>
  inline typename std::enable_if<!std::is_same<K1, std::string>::value, uint16_t>::type
  getKeyByteLength(const K &k0) const {
    return sizeof(K);
  }
  
  template<class K1>
  inline typename std::enable_if<std::is_same<K1, std::string>::value, uint16_t>::type
  getKeyByteLength(const K &k0) const {
    return k0.length();
  }
  
  inline uint32_t operator()(const K &k0) const {
    static_assert(sizeof(K) <= 32, "K length should be 32/64/96/128/160/192/224/256 bits");
    
    uint64_t *base = getBase<K>(k0);
    const uint16_t keyByteLength = getKeyByteLength<K>(k0);
    return farmhash::Hash32WithSeed((char *) base, (size_t) keyByteLength, s);
//    return XXH32((void*) base, keyByteLength, s);
  }
};

//! \brief A hash function that hashes keyType to uint32_t. When SSE4.2 support is found, use sse4.2 instructions, otherwise use default hash function  std::hash.
template<class K>
class Hasher64 {
public:
  uint64_t s;    //!< hash s.

  Hasher64()
    : s(0xe2211e2211) {
  }
  
  explicit Hasher64(const Hasher64 &h)
    : s(h.s) {
  }
  
  explicit Hasher64(uint64_t _s)
    : s(_s) {
  }
  
  //! set bitmask and s
  void setSeed(uint64_t _s) {
    s = _s;
  }
  
  template<class K1>
  inline typename std::enable_if<!std::is_same<K1, std::string>::value, uint64_t *>::type
  getBase(const K &k0) const {
    uint64_t *base;
    return (uint64_t *) &k0;
  }
  
  template<class K1>
  inline typename std::enable_if<std::is_same<K1, std::string>::value, uint64_t *>::type
  getBase(const K &k0) const {
    uint64_t *base;
    return (uint64_t *) &k0[0];
  }
  
  template<class K1>
  inline typename std::enable_if<!std::is_same<K1, std::string>::value, uint16_t>::type
  getKeyByteLength(const K &k0) const {
    return sizeof(K);
  }
  
  template<class K1>
  inline typename std::enable_if<std::is_same<K1, std::string>::value, uint16_t>::type
  getKeyByteLength(const K &k0) const {
    return k0.length();
  }
  
  inline uint64_t operator()(const K &k0) const {
    uint64_t *base = getBase<K>(k0);
    const uint16_t keyByteLength = getKeyByteLength<K>(k0);
    return farmhash::Hash64WithSeed((char *) base, (size_t) keyByteLength, s);
  }
};

template<class K>
class xxFastHasher32 {
public:
  uint32_t s;
  xxFastHasher32()
    : s(0xe211f58a) {
  }
  explicit xxFastHasher32(uint32_t _s)
    : s(_s) {
  }
  inline void setSeed(uint32_t _s) {
    s = _s;
  }
  inline uint32_t operator()(const K &k0) const {
    XXHash32 xxh(s); 
    const uint64_t *base = (uint64_t*) &k0;
    //const size_t keyByteLength = sizeof(k0);
    size_t keyByteLength = 4;
    //return HashUtil::Crc8(base, keyByteLength, s);
    xxh.add(base, keyByteLength);
    return xxh.hash();
    //return (uint8_t) (absl::HashOf(k0 * s) & 0xff);
  }
};

//! \brief A hash function that hashes keyType to uint32_t. When SSE4.2 support is found, use sse4.2 instructions, otherwise use default hash function  std::hash.
template<class K>
class FastHasher64 : public Hasher64<K> {
public:
  FastHasher64()
    : Hasher64<K>(0xe2211e2211) {
  }
  explicit FastHasher64(uint64_t _s)
    : Hasher64<K>(_s) {
  }
  inline void setSeed(uint64_t _s) {
    this->s = _s;
  }
  inline uint64_t operator()(const K &k0) const {
    void *base = this -> template getBase<K>(k0);
    const uint16_t keyByteLength = this -> template getKeyByteLength<K>(k0);
    uint32_t h[2];
    *(uint64_t*)h = this->s;
    HashUtil::BobHash(base, keyByteLength, h, h+1);
    return *(uint64_t*)h;
  }
};

template<class K>
class FastMurmurHasher64 : public Hasher64<K>{
public:

  FastMurmurHasher64()
    : s(0xe2118a43) {
  }
  explicit FastMurmurHasher64(uint64_t _s)
    : s(_s) {
  }
  inline void setSeed(uint64_t _s) {
    s = _s;
  }
  inline uint64_t operator()(const K &k0) const {
    void *base = this -> template getBase<K>(k0);
    const uint16_t keyByteLength = this -> template getKeyByteLength<K>(k0);
    return HashUtil::MurmurHash(base, keyByteLength, s) & 0xff;
  }
private:
  uint64_t s;
};

template<class K>
class AbslFastHasherTuple {
public:
  AbslFastHasherTuple()
    : s(0xe2) {
  }
  explicit AbslFastHasherTuple(uint32_t _s)
    : s(_s) {
  }
  inline void setSeed(uint32_t _s) {
    s = _s;
  }
  inline uint8_t operator()(const K &k0) const {
    return absl::HashOf(std::tuple(k0, s, 0)) & 0xff;
  }
private:
  uint32_t s;
};

template<class K>
class AbslFastHasher64 {
public:
  AbslFastHasher64()
    : s(0xe2) {
  }
  explicit AbslFastHasher64(uint32_t _s)
    : s(_s) {
  }
  inline void setSeed(uint32_t _s) {
    s = _s;
  }
  inline uint64_t operator()(const K &k0) const {
    return absl::HashOf(std::tuple(k0, s, 0));
  }
private:
  uint32_t s;
};
