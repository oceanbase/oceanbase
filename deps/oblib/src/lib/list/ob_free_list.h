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

#ifndef __OB_COMMON_OB_FREE_LIST_H__
#define __OB_COMMON_OB_FREE_LIST_H__
#include "lib/queue/ob_fixed_queue.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/utility/utility.h"
#include "lib/list/ob_dlist.h"
namespace oceanbase
{
namespace common
{
class IFreeHandler
{
public:
  IFreeHandler() {}
  virtual ~IFreeHandler() {}
public:
  virtual void free(void *p) = 0;
};

class ObTCFreeList
{
public:
  typedef ObSpinLock Lock;
  typedef ObLockGuard<Lock> LockGuard;
  struct FreeList: public ObDLinkBase<FreeList>
  {
    FreeList(ObTCFreeList &host): thread_id_(pthread_self()), host_(host) {}
    ~FreeList() {}
    pthread_t thread_id_;
    ObTCFreeList &host_;
    ObFixedQueue<void> queue_;
    int put(void *p) { return queue_.push(p); }
    void *get()
    {
      void *p = NULL;
      if (OB_SUCCESS != queue_.pop(p)) {
        p = NULL;
      }
      return p;
    }
  };
public:
  ObTCFreeList(): inited_(false), key_(OB_INVALID_PTHREAD_KEY), free_handler_(NULL),
                  allocator_(NULL), lock_(common::ObLatchIds::TC_FREE_LIST_LOCK),
                  global_free_list_(NULL), glimit_(0), tclimit_(0)
  {}
  ~ObTCFreeList()
  {
    destroy();
  }
  void destroy()
  {
    if (inited_) {
      pthread_key_delete(key_);
      if (NULL != global_free_list_) {
        destroy_free_list(global_free_list_);
        global_free_list_ = NULL;
      }
      inited_ = false;
    }
  }
  int init(IFreeHandler *free_handler, ObIAllocator *allocator, int64_t glimit, int64_t tclimit)
  {
    int err = OB_SUCCESS;
    int syserr = 0;
    if (NULL == free_handler || NULL == allocator) {
      err = OB_INVALID_ARGUMENT;
    } else if (inited_) {
      err = OB_INIT_TWICE;
    } else if (0 != (syserr = pthread_key_create(&key_, (void (*)(void *))do_destroy_free_list))) {
      err = OB_ERR_UNEXPECTED;
      _OB_LOG_RET(ERROR, err, "pthread_key_create()=>%d", syserr);
    } else {
      free_handler_ = free_handler;
      allocator_ = allocator;
      glimit_ = glimit;
      tclimit_ = tclimit;
      inited_ = true;
    }
    if (OB_SUCCESS != err)
    {}
    else if (NULL == (global_free_list_ = create_free_list(glimit))) {
      err = OB_ERR_UNEXPECTED;
      destroy();
      _OB_LOG_RET(ERROR, err, "create_free_list(%ld)=>%d", glimit, err);
    }
    return err;
  }
  int64_t to_string(char *buf, int64_t len) const
  {
    int64_t pos = 0;
    LockGuard guard(lock_);
    databuff_printf(buf, len, pos, "[FreeList] free_list_count=%d thread/count: ",
                    free_list_set_.get_size());
    DLIST_FOREACH_NORET(cur, free_list_set_) {
      databuff_printf(buf, len, pos, "%ld/%ld ", cur->thread_id_, cur->queue_.get_total());
    }
    return pos;
  }
public:
  void *get()
  {
    void *p = NULL;
    FreeList *free_list = NULL;
    if (!inited_)
    {}
    else if (tclimit_ > 0
             && NULL != (free_list = get_thread_specific_free_list())
             && NULL != (p = free_list->get()))
    {}
    else if (NULL != global_free_list_) {
      p = global_free_list_->get();
    }
    return p;
  }
  int put(void *p)
  {
    int err = OB_SUCCESS;
    FreeList *free_list = NULL;
    if (NULL == p) {
      err = OB_INVALID_ARGUMENT;
    } else if (!inited_) {
      err = OB_NOT_INIT;
    } else if (tclimit_ > 0
               && NULL != (free_list = get_thread_specific_free_list())
               && OB_SUCCESS == (err = free_list->put(p)))
    {}
    else if (OB_SUCCESS == (err = global_free_list_->put(p)))
    {}
    return err;
  }
  int put_to_glist(void *p)
  {
    int err = OB_SUCCESS;
    if (NULL == p) {
      err = OB_INVALID_ARGUMENT;
    } else if (!inited_) {
      err = OB_NOT_INIT;
    } else if (OB_SUCCESS == (err = global_free_list_->put(p)))
    {}
    return err;
  }
protected:
  FreeList *get_thread_specific_free_list()
  {
    FreeList *free_list = NULL;
    if (!inited_)
    {}
    else if (NULL != (free_list = (FreeList *)pthread_getspecific(key_)))
    {}
    else if (NULL == (free_list = create_free_list(tclimit_)))
    {}
    else if (0 != pthread_setspecific(key_, free_list)) {
      destroy_free_list(free_list);
      free_list = NULL;
    }
    return free_list;
  }
  FreeList *create_free_list(int64_t limit)
  {
    int err = OB_SUCCESS;
    FreeList *free_list = NULL;
    if (limit < 0) {
      err = OB_INVALID_ARGUMENT;
    } else if (!inited_) {
      err = OB_NOT_INIT;
    } else if (NULL == (free_list = (FreeList *)allocator_->alloc(sizeof(FreeList))))
    {}
    else {
      new(free_list)FreeList(*this);
      if (OB_SUCCESS != (err = free_list->queue_.init(limit, allocator_))) {
        allocator_->free(free_list);
        free_list = NULL;
      }
    }
    if (NULL != free_list) {
      LockGuard guard(lock_);
      if (!free_list_set_.add_last(free_list)) {
        _OB_LOG_RET(ERROR, OB_ERROR, "add_last fail, this should not happen");
      }
    }
    return free_list;
  }
  void destroy_free_list(FreeList *free_list)
  {
    int err = OB_SUCCESS;
    if (NULL == free_list) {
      err = OB_INVALID_ARGUMENT;
    } else if (!inited_) {
      err = OB_NOT_INIT;
    } else {
      void *p = NULL;
      LockGuard guard(lock_);
      while (OB_SUCCESS == free_list->queue_.pop(p)) {
        if (free_list == global_free_list_
            || NULL == global_free_list_
            || OB_SUCCESS != (err = global_free_list_->put(p))) {
          free_handler_->free(p);
        }
      }
      free_list_set_.remove(free_list);
      free_list->~FreeList();
      allocator_->free(free_list);
      if (free_list == global_free_list_) {
        global_free_list_ = NULL;
      }
    }
  }
  static void do_destroy_free_list(FreeList *free_list)
  {
    if (NULL != free_list) {
      free_list->host_.destroy_free_list(free_list);
    }
  }
private:
  bool inited_;
  pthread_key_t key_;
  IFreeHandler *free_handler_;
  ObIAllocator *allocator_;
  mutable Lock lock_;
  ObDList<FreeList> free_list_set_;
  FreeList *global_free_list_;
  int64_t glimit_;
  int64_t tclimit_;
};
}; // end namespace common
}; // end namespace oceanbase

#endif /* __OB_COMMON_OB_FREE_LIST_H__ */
