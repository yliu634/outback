#pragma once


#include "leaf.hpp"
#include "remote_memory.hh"
#include "leaf_allocator.hpp"

namespace rolex {

using K = u64;
using V = u64;
using leaf_t = Leaf<4, K, V>;
using leaf_alloc_t = LeafAllocator<leaf_t, sizeof(leaf_t)>;
using remote_memory_t = RemoteMemory<leaf_alloc_t>;



}