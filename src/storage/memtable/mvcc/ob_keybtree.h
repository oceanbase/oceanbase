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

#ifndef __OB_KEYBTREE_H__
#define __OB_KEYBTREE_H__

#include "lib/metrics/ob_counter.h"

#include "storage/memtable/ob_memtable_key.h"

namespace oceanbase {
namespace common {
class RetireStation;
class QClock;
class ObQSync;
class HazardList;
class ObIAllocator;
}  // namespace common
namespace memtable {
class ObStoreRowkeyWrapper;
class ObMvccRow;
}  // namespace memtable

namespace keybtree {
class BtreeNode;
class HazardLessIterator;
class Iterator;
class ObKeyBtree;
class ScanHandle;
using BtreeKey = memtable::ObStoreRowkeyWrapper;
using BtreeVal = memtable::ObMvccRow*;

struct BtreeNodeList {
  BtreeNodeList() : tail_(nullptr)
  {}
  void bulk_push(BtreeNode* first, BtreeNode* last);
  void push(BtreeNode* p)
  {
    bulk_push(p, p);
  }
  BtreeNode* pop();
  BtreeNode* load_lock();
  BtreeNode* tail_;
};

class BtreeNodeAllocator {
private:
  enum { MAX_LIST_COUNT = common::MAX_CPU_NUM };

public:
  BtreeNodeAllocator(common::ObIAllocator& allocator) : allocator_(allocator), alloc_memory_(0)
  {}
  virtual ~BtreeNodeAllocator()
  {}
  int64_t get_allocated() const
  {
    return ATOMIC_LOAD(&alloc_memory_) + sizeof(*this);
  }
  BtreeNode* alloc_node(const bool is_emergency);
  void free_node(BtreeNode* p)
  {
    if (OB_NOT_NULL(p)) {
      push(p);
    }
  }

private:
  int64_t push_idx();
  int64_t pop_idx();
  void push(BtreeNode* p)
  {
    free_list_array_[push_idx()].push(p);
  }
  int pop(BtreeNode*& p);

private:
  common::ObIAllocator& allocator_;
  int64_t alloc_memory_;
  BtreeNodeList free_list_array_[MAX_LIST_COUNT] CACHE_ALIGNED;
};

struct TScanHandle {
  TScanHandle() : iter_(NULL)
  {}
  ~TScanHandle()
  {
    reset();
  }
  void reset();
  int init(ObKeyBtree& btree);
  int get_next(BtreeKey& key, BtreeVal& val);
  bool is_reverse_scan() const;
  HazardLessIterator* get_iter()
  {
    return iter_;
  }
  HazardLessIterator* iter_;
  char buf_[4096];
};

struct TScanRawHandle {
  TScanRawHandle() : iter_(NULL)
  {}
  ~TScanRawHandle()
  {
    reset();
  }
  void reset();
  int init(ObKeyBtree& btree);
  int get_next(BtreeKey& key, BtreeVal& val);
  int estimate_key_count(int64_t top_level, int64_t& child_count, int64_t& key_count);
  int estimate_row_size(int64_t& per_row_size);
  int split_range(int64_t top_level, int64_t branch_count, int64_t part_count, BtreeKey* key_array);
  int estimate_element_count(int64_t& physical_row_count, int64_t& element_count, const double ratio);
  bool is_reverse_scan() const;
  Iterator* get_iter()
  {
    return iter_;
  }
  Iterator* iter_;
  char buf_[4096];
};

class ObKeyBtree {
public:
  ObKeyBtree(BtreeNodeAllocator& node_allocator) : node_allocator_(node_allocator), root_(nullptr)
  {}
  ~ObKeyBtree()
  {}
  int init()
  {
    return common::OB_SUCCESS;
  }
  int64_t size() const
  {
    return size_.value();
  }
  void dump(FILE* file)
  {
    print(ATOMIC_LOAD(&root_), file);
  }
  void print(const BtreeNode* root, FILE* file) const;
  int destroy();
  int del(const BtreeKey key, BtreeVal& value, int64_t version);
  int re_insert(const BtreeKey key, BtreeVal value);
  int insert(const BtreeKey key, BtreeVal value);
  int search(BtreeNode** root, ScanHandle& handle, BtreeKey key, int64_t version);
  int get_sdr(const BtreeKey key, BtreeKey& start, BtreeKey& end, int64_t& max_version);
  int skip_gap(const BtreeKey start, BtreeKey& end, int64_t version, bool reverse, int64_t& size);
  int get(const BtreeKey key, BtreeVal& value);
  int set_key_range(TScanHandle& handle, const BtreeKey min_key, int start_exclude, const BtreeKey max_key,
      int end_exclude, int64_t version);
  int set_key_range(TScanRawHandle& handle, const BtreeKey min_key, int start_exclude, const BtreeKey max_key,
      int end_exclude, int64_t version);
  BtreeNode* alloc_node(const bool is_emergency);
  void free_node(BtreeNode* p);
  void retire(common::HazardList& retire_list);
  common::RetireStation& get_retire_station();
  common::QClock& get_qclock();
  common::ObQSync& get_qsync();

private:
  void destroy(BtreeNode* root);

private:
  common::ObSimpleCounter size_;
  BtreeNodeAllocator& node_allocator_;
  BtreeNode* root_;
  DISALLOW_COPY_AND_ASSIGN(ObKeyBtree);
};

};  // namespace keybtree
};  // end namespace oceanbase

#endif /* __OB_KEYBTREE_H__ */
