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

#ifndef OCEANBASE_COMMON_HASH_OB_HAZARD_POINTER_
#define OCEANBASE_COMMON_HASH_OB_HAZARD_POINTER_

#include "lib/objectpool/ob_concurrency_objpool.h"
#include "lib/thread_local/ob_tsi_utils.h"
#include "lib/atomic/ob_atomic.h"

namespace oceanbase
{
namespace common
{

class ObHazardPointer
{
public:
  class ReclaimCallback  {
  public:
    ReclaimCallback() {}
    virtual ~ReclaimCallback() {}
    virtual void reclaim_ptr(uintptr_t ptr) = 0;
  };
public:
  inline ObHazardPointer() : hazard_list_(NULL),
                             retire_list_(NULL),
                             reclaim_callback_(NULL),
                             is_inited_(false) { }
  inline ~ObHazardPointer();
  inline void destroy();
  inline int init(ReclaimCallback *reclaim_callback);
  inline int protect(uintptr_t ptr);
  inline int release(uintptr_t ptr);
  inline int retire(uintptr_t ptr);
private:
  inline int reclaim();
private:
  struct Node
  {
    Node() : ptr(0), next(NULL) { }
    virtual ~Node() { ptr = 0; next = NULL; }
    uintptr_t ptr;
    Node *next;
  };
  struct ThreadLocalNodeList
  {
    ThreadLocalNodeList() { }
    Node head[OB_MAX_THREAD_NUM] CACHE_ALIGNED;
  };
private:
  ThreadLocalNodeList *hazard_list_;
  ThreadLocalNodeList *retire_list_;
  ReclaimCallback *reclaim_callback_;
  bool is_inited_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObHazardPointer);
};

ObHazardPointer::~ObHazardPointer()
{
  destroy();
}

void ObHazardPointer::destroy()
{
  if (NULL != hazard_list_) {
    op_free(hazard_list_);
    hazard_list_ = NULL;
  }
  if (NULL != retire_list_) {
    op_free(retire_list_);
    retire_list_ = NULL;
  }
  reclaim_callback_ = NULL;
  is_inited_ = false;
}

int ObHazardPointer::init(ReclaimCallback *reclaim_callback)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    COMMON_LOG(ERROR, "ObHazardPointer has been inited_", K(this));
    ret = OB_INIT_TWICE;
  } else if (NULL == reclaim_callback) {
    COMMON_LOG(ERROR, "invalid argument", K(reclaim_callback));
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == (hazard_list_ = op_alloc(ThreadLocalNodeList))) {
    COMMON_LOG(ERROR, "allocate memory for hazard_list_ failed");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (NULL == (retire_list_ = op_alloc(ThreadLocalNodeList))) {
    COMMON_LOG(ERROR, "allocate memory for retire_list_ failed");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    COMMON_LOG(INFO, "ObHazardPointer init ok:", K(this));
    reclaim_callback_ = reclaim_callback;
    is_inited_ = true;
  }
  if (OB_SUCCESS != ret && !is_inited_) {
    destroy();
  }
  return ret;
}

int ObHazardPointer::protect(uintptr_t ptr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    COMMON_LOG(WARN, "ObHazardPointer status error", K(is_inited_));
    ret = OB_NOT_INIT;
  } else if (0 == ptr) {
    COMMON_LOG(WARN, "protect ptr is NULL: ", K(ptr));
    ret = OB_INVALID_ARGUMENT;
  } else {
    bool is_set = false;
    int64_t tid = get_itid();
    if (tid >= OB_MAX_THREAD_NUM) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "thread num is beyond the max thread num limit", K(ret), K(tid));
    } else {
      Node *prev = hazard_list_->head + tid;
      while (OB_SUCCESS == ret && !is_set) {
        Node *p = prev->next;
        while (p != NULL) {
          if (p->ptr == 0) {
            ATOMIC_STORE(&p->ptr, ptr);
            is_set = true;
            break;
          }
          prev = p;
          p = p->next;
        }
        if (!is_set) {
          if (NULL == (p = op_alloc(Node))) {
            COMMON_LOG(ERROR, "allocate memory for Node failed");
            ret = OB_ALLOCATE_MEMORY_FAILED;
          } else {
            p->ptr = ptr;
            ATOMIC_STORE(&prev->next, p);
            is_set = true;
          }
        }
      }
    }
  }
  return ret;
}

int ObHazardPointer::release(uintptr_t ptr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    COMMON_LOG(WARN, "ObHazardPointer status error", K(is_inited_));
    ret = OB_NOT_INIT;
  } else if (0 == ptr) {
    COMMON_LOG(WARN, "release ptr is NULL: ", K(ptr));
    ret = OB_INVALID_ARGUMENT;
  } else {
    int64_t tid = get_itid();
    if (tid >= OB_MAX_THREAD_NUM) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "thread num is beyond the max thread num limit", K(ret), K(tid));
    } else {
      Node *p = hazard_list_->head[tid].next;
      while (OB_SUCCESS == ret && p != NULL) {
        if (p->ptr == ptr) {
          ATOMIC_STORE(&p->ptr, 0);
          break;
        }
        p = p->next;
      }
      if (NULL == p) {
        COMMON_LOG(ERROR, "release error, not found", K(ptr));
        ret = OB_SEARCH_NOT_FOUND;
      }
    }
  }
  return ret;
}

int ObHazardPointer::retire(uintptr_t ptr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    COMMON_LOG(WARN, "ObHazardPointer status error", K(is_inited_));
    ret = OB_NOT_INIT;
  } else if (0 == ptr) {
    COMMON_LOG(WARN, "retire ptr is NULL: ", K(ptr));
    ret = OB_INVALID_ARGUMENT;
  } else {
    Node * node = op_alloc(Node);
    if (NULL == node) {
      COMMON_LOG(ERROR, "allocate memory for Node failed");
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      int64_t tid = get_itid();
      if (tid >= OB_MAX_THREAD_NUM) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(ERROR, "thread num is beyond the max thread num limit", K(ret), K(tid));
      } else {
        Node *head = retire_list_->head + tid;
        node->ptr = ptr;
        node->next = head->next;
        head->next = node;
        reclaim();
      }
    }
  }
  return ret;
}

int ObHazardPointer::reclaim()
{
  int ret = OB_SUCCESS;
  int64_t max_tid = get_max_itid();
  Node *retire_pre = retire_list_->head + get_itid();
  Node *retire_node = retire_pre->next;
  while (NULL != retire_node) {
    bool is_hazard = false;
    for (int64_t i = 0; !is_hazard && i < max_tid; i++) {
      Node *p = hazard_list_->head[i].next;
      while (NULL != p) {
        if (ATOMIC_LOAD(&p->ptr) == retire_node->ptr) {
          is_hazard = true;
          break;
        }
        p = p->next;
      }
    }
    if (!is_hazard) {
      uintptr_t cp = retire_node->ptr;
      retire_pre->next = retire_node->next;
      op_free(retire_node);
      retire_node = retire_pre->next;
      reclaim_callback_->reclaim_ptr(cp);
      cp = 0;
    } else {
      retire_pre = retire_node;
      retire_node = retire_node->next;
    }
  }
  return ret;
}

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_COMMON_HASH_OB_HAZARD_POINTER_
