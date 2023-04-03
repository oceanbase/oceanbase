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

#ifndef OCEANBASE_ALLOCATOR_OB_HAZARD_REF_H_
#define OCEANBASE_ALLOCATOR_OB_HAZARD_REF_H_

#include "lib/ob_define.h"
#include "lib/thread_local/ob_tsi_utils.h"
namespace oceanbase
{
namespace common
{
class HazardRef
{
public:
  enum {
    MAX_THREAD_NUM = OB_MAX_THREAD_NUM,
    THREAD_REF_COUNT_LIMIT = 8,
    TOTAL_REF_COUNT_LIMIT = MAX_THREAD_NUM * THREAD_REF_COUNT_LIMIT
  };
  const static uint64_t INVALID_VERSION = UINT64_MAX;
public:
  explicit HazardRef(bool debug=false): cur_ver_(1), debug_(debug)
  {
    for (int i = 0; i < TOTAL_REF_COUNT_LIMIT; i++) {
      ref_array_[i] = INVALID_VERSION;
    }
  }
  virtual ~HazardRef()
  {}
  uint64_t *acquire_ref();
  void release_ref(uint64_t *ref);
  uint64_t new_version()
  {
    return ATOMIC_AAF(&cur_ver_, 1);
  }
  uint64_t get_hazard_version()
  {
    uint64_t min_version = ATOMIC_LOAD(&cur_ver_);
    int64_t real_used_ref = min(get_max_itid() * THREAD_REF_COUNT_LIMIT, TOTAL_REF_COUNT_LIMIT);
    for (int64_t i = 0; i < real_used_ref; i++) {
      uint64_t ver = ATOMIC_LOAD(ref_array_ + i);
      if (ver < min_version) {
        min_version = ver;
      }
    }
    return min_version;
  }
private:
  static uint64_t min(uint64_t x, uint64_t y)
  {
    return x < y ? x : y;
  }
private:
  uint64_t cur_ver_ CACHE_ALIGNED;
  bool debug_;
  uint64_t ref_array_[TOTAL_REF_COUNT_LIMIT];
};

struct HazardNode
{
  HazardNode(): next_(NULL), version_(0) {}
  ~HazardNode() {}
  void reset() { next_ = NULL; version_ = 0; }
  HazardNode *next_;
  uint64_t version_;
};

class HazardNodeList
{
public:
  HazardNodeList(): count_(0), tail_(&head_)
  {
    head_.next_ = &head_;
  }
  virtual ~HazardNodeList() {}
  int64_t size() const { return count_; }
  void push(HazardNode *node);
  HazardNode *pop()
  {
    HazardNode *p = head_.next_;
    if (&head_ == p) {
      p = NULL;
    } else {
      if (NULL != p) {
        head_.next_ = p->next_;
        if (p == tail_) {
          tail_ = p->next_;
        }
        count_--;
      }
    }
    return p;
  }
  void clear_list()
  {
    count_ = 0;
    tail_ = &head_;
    head_.next_ = tail_;
  }
  void concat(HazardNodeList &list)
  {
    if (list.size() > 0 && NULL != list.tail_ && NULL != tail_) {
      count_ += list.size();
      list.tail_->next_ = tail_->next_;
      tail_->next_ = list.head_.next_;
      tail_ = list.tail_;
      list.clear_list();
    }
  }
  HazardNode *head()
  {
    HazardNode *p = head_.next_;
    return &head_ == p ? NULL : p;
  }
  void set_version(uint64_t version)
  {
    if (size() > 0 && NULL != head_.next_) {
      head_.next_->version_ = version;
    }
  }
private:
  int64_t count_;
  HazardNode head_;
  HazardNode *tail_;
};

class RetireList
{
public:
  enum { MAX_THREAD_NUM = OB_MAX_THREAD_NUM };
  struct ThreadRetireList
  {
    ThreadRetireList() {}
    ~ThreadRetireList() {}
    HazardNodeList retire_list_;
    HazardNodeList prepare_list_;
  };
public:
  class RetireNodeIterator
  {
  public:
    explicit RetireNodeIterator(RetireList *host) : thread_offset_(0), host_(host) {}
    ~RetireNodeIterator() {}
    HazardNode *get_next()
    {
      HazardNode *ret_node = NULL;
      if (NULL != host_) {
        while (NULL == ret_node && thread_offset_ < MAX_THREAD_NUM) {
          ThreadRetireList *list = &host_->retire_list_[thread_offset_];
          if (NULL != (ret_node = list->prepare_list_.pop())) {
          } else if (NULL != (ret_node = list->retire_list_.pop())) {
          } else {
            ++thread_offset_;
          }
        }
      }
      return ret_node;
    }
  private:
    int64_t thread_offset_;
    RetireList *host_;
  };
public:
  RetireList(): hazard_version_(0)
  {}
  virtual ~RetireList()
  {}
  uint64_t get_prepare_size() { return NULL == get_thread_retire_list() ?
      0 : get_thread_retire_list()->prepare_list_.size(); }
  uint64_t get_retire_size() { return NULL == get_thread_retire_list() ?
      0 : get_thread_retire_list()->retire_list_.size(); }
  void set_reclaim_version(uint64_t version);
  void set_retire_version(uint64_t version);
  HazardNode *reclaim()
  {
    HazardNode *p = NULL;
    ThreadRetireList *retire_list = NULL;
    if (NULL != (retire_list = get_thread_retire_list())) {
      p = reclaim_node(retire_list);
    }
    return p;
  }
  void retire(HazardNode* p)
  {
    ThreadRetireList *retire_list = NULL;
    if (NULL != (retire_list = get_thread_retire_list())) {
      retire_list->prepare_list_.push(p);
    }
  }
private:
  HazardNode *reclaim_node(ThreadRetireList *retire_list)
  {
    HazardNode *p = NULL;
    if (NULL != retire_list) {
      uint64_t hazard_version = ATOMIC_LOAD(&hazard_version_);
      if (NULL != (p = retire_list->retire_list_.head()) && p->version_ <= hazard_version) {
        retire_list->retire_list_.pop();
      } else {
        p = NULL;
      }
    }
    return p;
  }
  ThreadRetireList *get_thread_retire_list()
  {
    return get_itid() < MAX_THREAD_NUM ? retire_list_ + get_itid() : NULL;
  }
private:
  uint64_t hazard_version_ CACHE_ALIGNED;
  ThreadRetireList retire_list_[MAX_THREAD_NUM];
};

class HazardHandle
{
public:
  explicit HazardHandle(HazardRef& href): href_(href), ref_(NULL) {}
  virtual ~HazardHandle() {}
  bool is_hold_ref() { return NULL != ref_; }
  uint64_t* acquire_ref()
  {
    return (ref_ = href_.acquire_ref());
  }
  void release_ref()
  {
    if (NULL != ref_) {
      href_.release_ref(ref_);
      ref_ = NULL;
    }
  }
private:
  HazardRef& href_;
  uint64_t* ref_;
};

class RetireListHandle
{
public:
  typedef HazardNode Node;
  typedef HazardNodeList NodeList;
  RetireListHandle(HazardRef& href, RetireList& retire_list):
      href_(href), retire_list_(retire_list) {}
  virtual ~RetireListHandle(){}
  void retire(int errcode, uint64_t retire_limit)
  {
    Node* p = NULL;
    if (0 != errcode) {
      while (NULL != (p = alloc_list_.pop())) {
        reclaim_list_.push(p);
      }
    } else {
      while(NULL != (p = del_list_.pop())) {
        retire_list_.retire(p);
      }
    }
    if (retire_list_.get_prepare_size() > retire_limit/2) {
      retire_list_.set_retire_version(href_.new_version());
    }
    if (retire_list_.get_retire_size() > retire_limit) {
      retire_list_.set_reclaim_version(href_.get_hazard_version());
    }
    while (NULL != (p = retire_list_.reclaim())) {
      reclaim_list_.push(p);
    }
  }
  Node* reclaim()
  {
    return reclaim_list_.pop();
  }
  void add_alloc(Node* node)
  {
    alloc_list_.push(node);
  }
  void add_del(Node* node)
  {
    del_list_.push(node);
  }
  void add_retire(Node* node)
  {
    retire_list_.retire(node);
  }
private:
  HazardRef& href_;
  RetireList& retire_list_;
  NodeList alloc_list_;
  NodeList del_list_;
  NodeList reclaim_list_;
};
}; // end namespace allocator
}; // end namespace oceanbase


#endif /* OCEANBASE_ALLOCATOR_OB_HAZARD_REF_H_ */
