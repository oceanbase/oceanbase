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

namespace oceanbase
{
namespace common
{
class RetireStation;
class QClock;
class ObQSync;
class HazardList;
class ObIAllocator;
}
namespace memtable
{
class ObStoreRowkeyWrapper;
struct ObMvccRow;
}

namespace keybtree
{
class BtreeNode;
class HazardLessIterator;
class Iterator;
class ObKeyBtree;
class ScanHandle;

using BtreeKey = memtable::ObStoreRowkeyWrapper;
using BtreeVal = memtable::ObMvccRow *;

struct BtreeKV
{
  BtreeKey key_; // 8byte
  BtreeVal val_; // 8byte
};

struct BtreeNodeList
{
  BtreeNodeList(): tail_(nullptr) {}
  void bulk_push(BtreeNode* first, BtreeNode* last);
  void push(BtreeNode* p) { bulk_push(p, p); }
  BtreeNode* pop();
  BtreeNode* load_lock();
  BtreeNode* tail_;
};

class BtreeNodeAllocator
{
private:
  enum {
    MAX_LIST_COUNT = common::MAX_CPU_NUM
  };
public:
  BtreeNodeAllocator(common::ObIAllocator &allocator) : allocator_(allocator), alloc_memory_(0) {}
  virtual ~BtreeNodeAllocator() {}
  int64_t get_allocated() const { return ATOMIC_LOAD(&alloc_memory_) + sizeof(*this); }
  BtreeNode *alloc_node(const bool is_emergency);
  void free_node(BtreeNode *p)
  {
    if (OB_NOT_NULL(p)) {
      push(p);
    }
  }
  void reset()
  {
    memset(free_list_array_, 0, sizeof(free_list_array_));
    alloc_memory_ = 0;
  }
private:
  int64_t push_idx();
  int64_t pop_idx();
  void push(BtreeNode *p) { free_list_array_[push_idx()].push(p); }
  int pop(BtreeNode*& p);
private:
  common::ObIAllocator &allocator_;
  int64_t alloc_memory_;
  BtreeNodeList free_list_array_[MAX_LIST_COUNT] CACHE_ALIGNED;
};

/*
 * Interface of Iterator 
 * it will do batch_scan, so we can keep it without release for a long time.
 */
class BtreeIterator {
  class KVQueue {
    enum {
      capacity = 225
    };
    public:
      KVQueue(): push_(0), pop_(0) {}
      ~KVQueue() {}
      void reset();
      int push(const BtreeKV &data);
      int pop(BtreeKV &data);
      int64_t size() const { return push_ - pop_; }
    private:
      int64_t idx(const int64_t x) { return x % capacity; }
    private:
      int64_t push_;
      int64_t pop_;
      BtreeKV items_[capacity];
  };
public:
  explicit BtreeIterator():
      iter_(nullptr),
      start_key_(),
      end_key_(),
      version_(INT64_MAX),
      start_exclude_(false),
      end_exclude_(false),
      is_iter_end_(false),
      scan_backward_(false),
      kv_queue_() {}
  ~BtreeIterator() { reset(); }
  int init(ObKeyBtree &btree);
  void reset();
  int set_key_range(const BtreeKey min_key, const bool start_exclude,
                    const BtreeKey max_key, const bool end_exclude, int64_t version);
  int get_next(BtreeKey &key, BtreeVal &val);
  bool is_reverse_scan() const { return scan_backward_; }
private:
  int scan_batch();
private:
  Iterator *iter_; // 8byte
  BtreeKey start_key_; // 8byte
  BtreeKey end_key_; // 8byte
  int64_t version_; // 8byte
  bool start_exclude_; // 1byte
  bool end_exclude_; // 1byte
  bool is_iter_end_; // 1byte
  bool scan_backward_; // 1byte
  KVQueue kv_queue_; // 3616 == 16 + 16 * n, n == 225
  char buf_[376]; // sizeof(Iterator) == 376
};
// sizezof(BtreeIterator) == 4032, some extra memory for QueryEngine Iterator, do not larger than 4k.

/*
 * Use for estimate row count.
 * DO NOT Keep for a long time, otherwise writing will be blocked.
 */
class BtreeRawIterator {
public:
  explicit BtreeRawIterator(): iter_(NULL) {}
  ~BtreeRawIterator() { reset(); }
  int init(ObKeyBtree &btree);
  void reset();
  int set_key_range(const BtreeKey min_key, const bool start_exclude,
                    const BtreeKey max_key, const bool end_exclude, int64_t version);
  int get_next(BtreeKey &key, BtreeVal &val);
  int estimate_key_count(int64_t top_level, int64_t& child_count, int64_t& key_count);
  int split_range(int64_t top_level, int64_t branch_count, int64_t part_count, BtreeKey* key_array);
  int estimate_element_count(int64_t &physical_row_count, int64_t &element_count, const double ratio);
  bool is_reverse_scan() const;
private:
  Iterator *iter_; // 8byte
  char buf_[376]; // sizeof(Iterator) == 376
};

class ObKeyBtree
{
  friend class BtreeIterator;
  friend class BtreeRawIterator;
  friend class Iterator;
public:
  ObKeyBtree(BtreeNodeAllocator &node_allocator)
    : split_info_(0),
      size_(),
      node_allocator_(node_allocator),
      root_(nullptr) {}
  ~ObKeyBtree() {}
  int init();
  int64_t size() const { return size_.value(); }
  void dump(FILE *file) { print(file); }
  void print(FILE *file) const;
  int destroy();
  int del(const BtreeKey key, BtreeVal &value, int64_t version);
  int re_insert(const BtreeKey key, BtreeVal value);
  int insert(const BtreeKey key, BtreeVal &value);
  int skip_gap(const BtreeKey start, BtreeKey &end, int64_t version, bool reverse, int64_t &size);
  int get(const BtreeKey key, BtreeVal &value);
  int set_key_range(BtreeIterator &iter, const BtreeKey min_key, const bool start_exclude,
                    const BtreeKey max_key, const bool end_exclude, int64_t version);
  int set_key_range(BtreeRawIterator &handle, const BtreeKey min_key, const bool start_exclude,
                    const BtreeKey max_key, bool end_exclude, int64_t version);
  BtreeNode *alloc_node(const bool is_emergency);
  void free_node(BtreeNode *p);
  void retire(common::HazardList &retire_list);
  int32_t update_split_info(int32_t split_pos);
  common::RetireStation &get_retire_station();
  common::QClock& get_qclock();
  common::ObQSync& get_qsync();
private:
  void destroy(BtreeNode *root);
private:
  union {
    struct {
      uint32_t split_pos_sum_;
      uint32_t split_count_;
    };
    uint64_t split_info_;
  };
  common::ObSimpleCounter size_;
  BtreeNodeAllocator &node_allocator_;
  BtreeNode *root_;
  DISALLOW_COPY_AND_ASSIGN(ObKeyBtree);
};

}; // end namespace common
}; // end namespace oceanbase

#endif /* __OB_KEYBTREE_H__ */
