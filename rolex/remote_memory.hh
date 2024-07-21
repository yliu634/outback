#pragma once

#include <cassert>

#include "rlib/core/nicinfo.hh"      /// RNicInfo
#include "rlib/core/rctrl.hh"        /// RCtrl
#include "rlib/core/naming.hh"       /// DevIdx 
#include "rlib/core/lib.hh"
#include "huge_region.hh"

#include "trait.hpp"

using namespace rdmaio;

namespace rolex {

class RM_config {
public:
  rdmaio::RCtrl* ctrl;

  uint64_t reg_leaf_region;
  uint64_t leaf_region_size; // table bucket size
  uint64_t leaf_num;  // bucket number

  explicit RM_config(rdmaio::RCtrl* ctrl, uint64_t ls, uint64_t rlr, uint64_t ln) 
    : ctrl(ctrl), leaf_region_size(ls), reg_leaf_region(rlr), leaf_num(ln) {}
};


template<typename leaf_alloc_t>
class RemoteMemory {

public:
  explicit RemoteMemory(const RM_config &conf) : conf(conf), ctrl(conf.ctrl) {
    // [RCtrl] register all NICs, and create model/leaf regions
    register_nic();
    create_regions();
    // create model/leaf allocators
    init_allocator();
  }

  auto leaf_allocator() -> leaf_alloc_t* { return this->leafAlloc; }

  auto leaf_region_ptr() -> void* {return leaf_region->start_ptr();}

  void start_daemon() {
    ctrl->start_daemon();
  }

private:
  RM_config conf;
  rdmaio::RCtrl* ctrl;
  std::vector<DevIdx> all_nics;
  std::shared_ptr<rolex::HugeRegion> leaf_region;
  leaf_alloc_t* leafAlloc;

  void register_nic() {
    all_nics = RNicInfo::query_dev_names();
    {
      for (uint i = 0; i < all_nics.size(); ++i) {
        auto nic = RNic::create(all_nics.at(i)).value();
        ASSERT(ctrl->opened_nics.reg(i, nic));
      }
    }
  }

  void create_regions() {
    // [RCtrl] create leaf regions, leaf_region is total size of all leaves
    leaf_region = HugeRegion::create(conf.leaf_region_size).value();
    ctrl->registered_mrs.create_then_reg( // 0 is the name not idx
      conf.reg_leaf_region, leaf_region->convert_to_rmem().value(), ctrl->opened_nics.query(0).value());
  }

  void init_allocator() {
    ASSERT(leaf_region) << "Leaf region not exist";
    leafAlloc = new leaf_alloc_t(static_cast<char *>(leaf_region->start_ptr()), 
                      leaf_region->size());
  }
};

} // namespace rolex