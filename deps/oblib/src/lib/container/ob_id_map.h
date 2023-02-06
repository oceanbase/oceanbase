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

#ifndef  OCEANBASE_COMMON_ID_MAP_H_
#define  OCEANBASE_COMMON_ID_MAP_H_
#include "lib/ob_define.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/queue/ob_fixed_queue.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "lib/utility/utility.h"
#define IDMAP_INVALID_ID 0
namespace oceanbase
{
namespace common
{
enum FetchMod
{
  FM_SHARED = 0,
  FM_MUTEX_BLOCK = 1,
  FM_MUTEX_NONBLOCK = 2,
};

template <typename T, typename ID_TYPE = uint64_t, int64_t TSI_HOLD_NUM = 4>
class ObIDMap
{
  enum Stat
  {
    ST_FREE = 0,
    ST_USING = 1,
  };
  struct Node
  {
    common::SpinRWLock lock;
    volatile ID_TYPE id;
    volatile Stat stat;
    T *data;
  };
public:
  ObIDMap();
  ~ObIDMap();
public:
  int init(const ID_TYPE num);
  void destroy();
public:
  int assign(T *value, ID_TYPE &id);
  int get(const ID_TYPE id, T *&value) const;
  T *fetch(const ID_TYPE id, const FetchMod mod = FM_SHARED);
  void revert(const ID_TYPE id, const bool erase = false);
  void erase(const ID_TYPE id);
  int size() const;
public:
  // callback::operator()(const ID_TYPE id)
  template <typename Callback>
  void traverse(Callback &cb) const
  {
    if (NULL != array_) {
      for (ID_TYPE i = 0; i < num_; i++) {
        if (ST_USING == array_[i].stat) {
          cb(array_[i].id);
        }
      }
    }
  };
private:
  ID_TYPE num_;
  ID_TYPE overflow_limit_;
  Node *array_;
  common::ObFixedQueue<Node> free_list_;
  ObMemAttr memattr_;
};

inline int calc_clz(const uint32_t s)
{
  OB_ASSERT(0 != s);
  return __builtin_clz(s);
}

inline int calc_clz(const uint64_t s)
{
  OB_ASSERT(0ULL != s);
  return __builtin_clzl(s);
}

template<typename T, typename ID_TYPE, int64_t TSI_HOLD_NUM>
ObIDMap<T, ID_TYPE, TSI_HOLD_NUM>::ObIDMap() : num_(1),
                                               overflow_limit_(0),
                                               array_(NULL),
                                               free_list_()
{
  memattr_.tenant_id_ = OB_SERVER_TENANT_ID;
  memattr_.label_ = ObModIds::ID_MAP;
}

template<typename T, typename ID_TYPE, int64_t TSI_HOLD_NUM>
ObIDMap<T, ID_TYPE, TSI_HOLD_NUM>::~ObIDMap()
{
  destroy();
}

template<typename T, typename ID_TYPE, int64_t TSI_HOLD_NUM>
int ObIDMap<T, ID_TYPE, TSI_HOLD_NUM>::init(const ID_TYPE num)
{
  int ret = OB_SUCCESS;
  if (NULL != array_) {
    _OB_LOG(WARN, "have inited");
    ret = OB_INIT_TWICE;
  } else if (0 >= num) {
    _OB_LOG(WARN, "invalid param num=%lu", (uint64_t)num);
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == (array_ = (Node *)common::ob_malloc((num + 1) * sizeof(Node),
                                                         memattr_))) {
    _OB_LOG(WARN, "malloc array fail num=%lu", (uint64_t)(num + 1));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_SUCCESS != (ret = free_list_.init(num))) {
    _OB_LOG(WARN, "free list init fail ret=%d", ret);
  } else {
    memset(array_, 0, (num + 1) * sizeof(Node));
    for (ID_TYPE i = 0; i < (num + 1); i++) {
      array_[i].lock.set_latch_id(common::ObLatchIds::ID_MAP_NODE_LOCK);
      array_[i].id = i;
      if (0 != i
          && OB_SUCCESS != (ret = free_list_.push(&(array_[i])))) {
        _OB_LOG(WARN, "push to free list fail ret=%d i=%lu", ret, (uint64_t)i);
        break;
      }
    }
    num_ = num + 1;
    overflow_limit_ = (num_ << (calc_clz(num_) -
                                2)); // only use 30/62-low-bit, make sure integer multiples of num_
  }
  if (OB_FAIL(ret)) {
    destroy();
  }
  return ret;
}

template<typename T, typename ID_TYPE, int64_t TSI_HOLD_NUM>
void ObIDMap<T, ID_TYPE, TSI_HOLD_NUM>::destroy()
{
  free_list_.destroy();
  if (NULL != array_) {
    common::ob_free(array_);
    array_ = NULL;
  }
  num_ = 1;
}

template<typename T, typename ID_TYPE, int64_t TSI_HOLD_NUM>
int ObIDMap<T, ID_TYPE, TSI_HOLD_NUM>::assign(T *value, ID_TYPE &id)
{
  int ret = OB_SUCCESS;
  Node *node = NULL;
  if (NULL == array_) {
    _OB_LOG(WARN, "have not inited");
    ret = OB_NOT_INIT;
  } else if (OB_SUCCESS != (ret = free_list_.pop(node))
             || NULL == node) {
    _OB_LOG(WARN, "no more id free list size=%ld", free_list_.get_total());
    ret = (OB_SUCCESS == ret) ? OB_ALLOCATE_MEMORY_FAILED : ret;
  } else {
    id = node->id;
    node->data = value;
    node->stat = ST_USING;
  }
  return ret;
}

template<typename T, typename ID_TYPE, int64_t TSI_HOLD_NUM>
int ObIDMap<T, ID_TYPE, TSI_HOLD_NUM>::get(const ID_TYPE id, T *&value) const
{
  int ret = OB_SUCCESS;
  ID_TYPE pos = id % num_;
  if (NULL == array_) {
    _OB_LOG(WARN, "have not inited");
    ret = OB_NOT_INIT;
  } else {
    T *retv = NULL;
    if (id != array_[pos].id
        || ST_USING != array_[pos].stat) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      retv = array_[pos].data;
      // double check
      if (id != array_[pos].id
          || ST_USING != array_[pos].stat) {
        ret = OB_ENTRY_NOT_EXIST;
        retv = NULL;
      } else {
        value = retv;
      }
    }
  }
  return ret;
}

template<typename T, typename ID_TYPE, int64_t TSI_HOLD_NUM>
T *ObIDMap<T, ID_TYPE, TSI_HOLD_NUM>::fetch(const ID_TYPE id, const FetchMod mod)
{
  T *ret = NULL;
  if (NULL != array_) {
    ID_TYPE pos = id % num_;
    bool lock_succ = false;
    if (FM_SHARED == mod) {
      (void)array_[pos].lock.rdlock();
      lock_succ = true;
    } else if (FM_MUTEX_BLOCK == mod) {
      (void)array_[pos].lock.wrlock();
      lock_succ = true;
    } else if (FM_MUTEX_NONBLOCK == mod) {
      if (array_[pos].lock.try_wrlock()) {
        lock_succ = true;
      }
    }

    if (!lock_succ) {
      // do nothing
    } else if (id == array_[pos].id
               && ST_USING == array_[pos].stat) {
      ret = array_[pos].data;
    } else {
      (void)array_[pos].lock.unlock();
    }
  }
  return ret;
}

template<typename T, typename ID_TYPE, int64_t TSI_HOLD_NUM>
void ObIDMap<T, ID_TYPE, TSI_HOLD_NUM>::revert(const ID_TYPE id, const bool erase)
{
  if (NULL != array_) {
    ID_TYPE pos = id % num_;
    if (erase
        && id == array_[pos].id
        && ST_USING == array_[pos].stat) {
      if ((overflow_limit_ - num_) < array_[pos].id) {
        array_[pos].id = pos;
      } else {
        array_[pos].id += num_;
      }
      array_[pos].stat = ST_FREE;
      array_[pos].data = NULL;
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = free_list_.push(&(array_[pos])))) {
        _OB_LOG_RET(ERROR, tmp_ret, "push to free list fail ret=%d free list size=%ld", tmp_ret,
                  free_list_.get_total());
      }
    }
    (void)array_[pos].lock.unlock();
  }
}

template<typename T, typename ID_TYPE, int64_t TSI_HOLD_NUM>
void ObIDMap<T, ID_TYPE, TSI_HOLD_NUM>::erase(const ID_TYPE id)
{
  if (NULL != array_) {
    ID_TYPE pos = id % num_;
    common::SpinWLockGuard guard(array_[pos].lock);
    if (id == array_[pos].id
        && ST_USING == array_[pos].stat) {
      if ((overflow_limit_ - num_) < array_[pos].id) {
        array_[pos].id = pos;
      } else {
        array_[pos].id += num_;
      }
      array_[pos].stat = ST_FREE;
      array_[pos].data = NULL;
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = free_list_.push(&(array_[pos])))) {
        _OB_LOG_RET(ERROR, tmp_ret, "push to free list fail ret=%d free list size=%ld", tmp_ret,
                  free_list_.get_total());
      }
    }
  }
}

template<typename T, typename ID_TYPE, int64_t TSI_HOLD_NUM>
int ObIDMap<T, ID_TYPE, TSI_HOLD_NUM>::size() const
{
  return static_cast<int>(num_ - 1 - free_list_.get_total());
}
} // namespace common
} // namespace oceanbase

#endif //OCEANBASE_UPDATESERVER_ID_MAP_H_
