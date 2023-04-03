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

#ifndef OCEANBASE_HASH_OB_SMALL_INT_MAP_H_
#define OCEANBASE_HASH_OB_SMALL_INT_MAP_H_
#include "lib/atomic/ob_atomic.h"
#include "lib/queue/ob_link.h"
#include "lib/hash/ob_hashutils.h"

namespace oceanbase
{
namespace common
{
class ObLinkArray
{
public:
  typedef ObLink Link;
  struct Seg: public Link
  {
    Seg(int64_t start, int64_t size): start_(start), end_(start + size) {
      UNUSED(base_);
    }
    ~Seg() {}
    int64_t compare(Seg* that) { return this->end_ - that->end_; }
    int64_t start_;
    int64_t end_;
    void* base_[0];
  };
  ObLinkArray(): label_(nullptr), tenant_id_(0), seg_size_(0), head_(0, 0) {}
  ~ObLinkArray() { destroy(); }
  void destroy() {
    Seg* p = NULL;
    while(NULL != (p = (Seg*)head_.next_)) {
      head_.next_ = p->next_;
      destroy_seg(p);
    }
  }
  int init(int64_t seg_size, const lib::ObLabel &label, int tenant_id) {
    int ret = OB_SUCCESS;
    if (seg_size * sizeof(void*) < sizeof(Seg) + sizeof(void*)) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      label_ = label;
      tenant_id_ = tenant_id;
      seg_size_ = (seg_size * sizeof(void*)- sizeof(Seg))/sizeof(void*);
    }
    return ret;
  }
  Seg* head() { return &head_; }
  void** next(Seg* &s, int64_t& idx) {
    void** ret = NULL;
    if (idx >= s->end_) {
      s = (Seg*)s->next_;
      if (NULL != s)  {
        idx = s->start_;
      }
    }
    if (NULL != s) {
      ret = s->base_ + (idx - s->start_);
      idx++;
    }
    return ret;
  }
  void** locate(int64_t idx, bool force_alloc) {
    Seg* s = locate_seg(idx, force_alloc);
    return NULL != s? s->base_ + (idx - s->start_): NULL;
  }
private:
  Seg* locate_seg(int64_t idx, bool force_alloc) {
    Seg* s = NULL;
    if (idx < 0) {
      //ret = OB_INVALID_ARGUMENT;
    } else if (NULL != (s = search(idx))) {
    } else if (!force_alloc) {
    } else if (NULL == (s = create_seg(calc_start(idx), seg_size_))) {
    } else if (0 != ol_insert(&head_, s)) {
      destroy_seg(s);
      s = search(idx);
    }
    return s;
  }
  int64_t calc_start(int64_t x) {
    return (x - (x % seg_size_));
  }
  Seg* search(int64_t idx) {
    Seg key(idx, 1);
    Seg* prev = NULL;
    Seg* next = ol_search(&head_, &key, prev);
    if (NULL != next && next->start_ > idx) {
      next = NULL;
    }
    return next;
  }
  Seg* create_seg(int64_t start, int64_t size) {
    Seg* p = (Seg*)ob_malloc(sizeof(Seg) + size * sizeof(void*), "LinkArray");
    if (NULL != p) {
      new(p)Seg(start, size);
      memset(p->base_, 0, size * sizeof(void*));
    }
    return p;
  }
  void destroy_seg(Seg* s) {
    ob_free(s);
  }
private:
  lib::ObLabel label_;
  int tenant_id_;
  int64_t seg_size_;
  Seg head_;
};

// value can only be 8bytes ptr or int, value == 0 and unset value are treated as same way.
// package LinkArray
template<typename T>
    class ObPtrArrayWrapper
{
public:
  typedef ObLinkArray Array;
  typedef Array::Seg Seg;
  typedef hash::HashMapPair<int64_t, T> Pair;
  enum { N_LOCK = 1024 };
  struct LockGuard
  {
    LockGuard(char& lock): lock_(lock) {
      while(ATOMIC_TAS(&lock_, 1)) {
        PAUSE();
      }
    }
    ~LockGuard() {
      ATOMIC_STORE(&lock_, 0);
    }
    char& lock_;
  };
  struct iterator {
    iterator(const iterator& that): array_(that.array_), seg_(that.seg_), idx_(that.idx_), addr_(that.addr_) {}
    iterator(Array& array, Seg* seg, int64_t idx, T* addr): array_(array), seg_(seg), idx_(idx), addr_(addr) {}
    ~iterator() {}
    Array& array_;
    Seg* seg_;
    int64_t idx_;
    T* addr_;
    Pair key_value_;
    Pair& operator *() {
      key_value_.init(idx_, NULL != addr_? *addr_: 0);
      return key_value_;
    }
    Pair* operator ->() {
      key_value_.init(idx_, NULL != addr_? *addr_: 0);
      return &key_value_;
    }

    bool operator ==(const iterator &iter) const {
      return addr_ == iter.addr_;
    }
    bool operator !=(const iterator &iter) const {
      return addr_ != iter.addr_;
    }

    iterator &operator ++() {
      addr_ = (T*)array_.next(seg_, idx_);
      return *this;
    }
    iterator operator ++(int) {
      iterator iter = *this;
      ++*this;
      return iter;
    }
  };
  ObPtrArrayWrapper() {
    memset(lock_, 0, sizeof(lock_));
  }
  ~ObPtrArrayWrapper() {}
  int create(int64_t bucket_num, const char *bucket_label, int tenant_id) {
    return array_.init(bucket_num * sizeof(void*), bucket_label, tenant_id);
  }
  iterator begin() {
    Seg* seg = array_.head();
    int64_t idx = 0;
    T* addr = (T*)array_.next(seg, idx);
    return iterator(array_, seg, idx, addr);
  }
  iterator end() {
    return iterator(array_, NULL, 0, NULL);
  }
  const T get(const key_t key) const {
    T val = 0;
    return 0 == get_refactored(key, val)? val: NULL;
  }
  int get_refactored(const key_t key, T& val) const {
    int ret = OB_SUCCESS;
    T* addr = (T*)const_cast<Array&>(array_).locate(key, /*alloc*/false);
    if (NULL == addr || 0 == (uint64_t)*addr) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      val = *addr;
    }
    return ret;
  }
  int set_refactored(const key_t key, T val, bool overwrite = true) {
    int ret = OB_SUCCESS;
    abort_unless(overwrite);
    T* addr = (T*)array_.locate(key, /*alloc*/true);
    if (NULL != addr) {
      *addr = val;
    } else {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
    return ret;
  }
  template<typename func_t>
      int atomic_refactored(const key_t key, func_t& func) {
    int ret = OB_SUCCESS;
    T* addr = (T*)array_.locate(key, /*alloc*/false);
    if (NULL == addr) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      LockGuard guard(lock_[((uint64_t)key) % N_LOCK]);
      func((T&)*addr);
    }
    return ret;
  }
public:
  ObLinkArray array_;
  char lock_[N_LOCK];
};
}; // end namespace common
}; // end namespace oceanbase

#endif /* OCEANBASE_HASH_OB_SMALL_INT_MAP_H_ */
