/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_LIB_CONTAINER_ID_SET_
#define OCEANBASE_LIB_CONTAINER_ID_SET_ 1

#include <stdint.h>
#include "lib/hash/ob_placement_hashmap.h"
#include "lib/hash/ob_iteratable_hashset.h"

namespace oceanbase {
namespace common {
// assign each ID an index
class ObId2Idx {
public:
  ObId2Idx();
  virtual ~ObId2Idx();
  void reset();

  int add_id(uint64_t id, int32_t* idx = NULL);
  bool has_id(uint64_t id) const;
  /// @retval -1 id not exist
  int32_t get_idx(uint64_t id) const;

private:
  // types and constants
  typedef common::hash::ObPlacementHashMap<uint64_t, int32_t, common::OB_MAX_TABLE_NUM_PER_STMT> Id2IdxMap;

private:
  // disallow copy
  ObId2Idx(const ObId2Idx& other);
  ObId2Idx& operator=(const ObId2Idx& other);
  // function members
private:
  // data members
  Id2IdxMap id2idx_;
};

// A set of IDs
class ObIdSet {
public:
  typedef common::hash::ObIteratableHashSet<uint64_t, common::OB_MAX_TABLE_NUM_PER_STMT> IdSet;
  typedef ObBitSet<common::OB_MAX_TABLE_NUM_PER_STMT> BitSet;
  typedef IdSet::const_iterator_t ConstIterator;

public:
  ObIdSet();
  virtual ~ObIdSet();
  void reset();

  int add_id(uint64_t id, int32_t idx);
  bool has_id(uint64_t id);
  bool includes_id_set(const ObIdSet& other) const;
  bool intersect_id_set(const ObIdSet& other) const;

  ConstIterator begin() const
  {
    return id_set_.begin();
  };
  ConstIterator end() const
  {
    return id_set_.end();
  };
  int64_t count() const
  {
    return id_set_.count();
  };
  TO_STRING_KV(N_ID_SET, id_set_);

private:
private:
  // disallow copy
  ObIdSet(const ObIdSet& other);
  ObIdSet& operator=(const ObIdSet& other);
  // function members
private:
  // data members
  IdSet id_set_;
  BitSet bit_set_;
};
}  // end namespace common
}  // end namespace oceanbase

#endif /* OCEANBASE_LIB_CONTAINER_ID_SET_ */
