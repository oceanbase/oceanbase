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

#ifndef OCEANBASE_LIBOBCDC_OB_LOG_TASK_POOL_H_
#define OCEANBASE_LIBOBCDC_OB_LOG_TASK_POOL_H_

#include "lib/ob_errno.h"
#include "ob_log_utils.h"
#include "share/ob_define.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/ob_concurrent_fifo_allocator.h"   // ObConcurrentFIFOAllocator
#include "lib/queue/ob_link_queue.h"                      // ObLinkQueue
#include "lib/queue/ob_fixed_queue.h"                     // ObFixedQueue

namespace oceanbase
{
namespace libobcdc
{

template <typename T> class ObLogTransTaskPool;

// TransTaskBase.
// Base class for trans task.
// Call get() on task pool to get a task.
// Call revert() on task to return it to pool.
template <typename T>
class TransTaskBase : public common::QLink
{
  typedef TransTaskBase<T> MyType;
  typedef ObLogTransTaskPool<T> PoolType;
public:
  TransTaskBase() : pool_(NULL) {}
  virtual ~TransTaskBase() { }
public:
  void set_pool(PoolType* pool) { pool_ = pool; }
  void revert()
  {
    if (NULL != pool_) {
      pool_->revert(this);
    }
  }
  PoolType *get_pool() { return pool_; }
private:
  PoolType *pool_;
};

// TransTaskPool.
// Pool of trans task of type T.
// You get one task by calling get(), and return it using revert()
// on task base. Calling revert() on the pool is also ok.
// Use pool_size to set the pre-alloc task number.
template <typename T>
class ObLogTransTaskPool
{
  typedef T TaskType;
  typedef TransTaskBase<T> BaseTaskType;
  typedef common::QLink PoolElemType;
  typedef common::ObFixedQueue<void> PagePool;

  static const int64_t LARGE_ALLOCATOR_PAGE_SIZE = OB_MALLOC_BIG_BLOCK_SIZE;     // 2M - 17KB
  static const int64_t LARGE_ALLOCATOR_TOTAL_LIMIT = (1LL << 37);   // 127G
  static const int64_t LARGE_ALLOCATOR_HOLD_LIMIT = (1LL << 26);    // 64M

public:
  ObLogTransTaskPool() :
    inited_(false),
    prealloc_task_cnt_(0),
    task_page_size_(0),
    allow_dynamic_alloc_(false),
    prealloc_task_pool_(),
    prealloc_pool_tasks_(NULL),
    alloc_(NULL),
    prealloc_page_cnt_(0),
    prealloc_pages_(NULL),
    prealloc_page_pool_(),
    task_large_allocator_(),
    total_cnt_(0),
    dynamic_alloc_task_cnt_(0),
    used_prealloc_task_cnt_(0)
  { }
  virtual ~ObLogTransTaskPool() { }

  int64_t get_alloc_count() const { return dynamic_alloc_task_cnt_ + used_prealloc_task_cnt_; }
  int64_t get_total_count() const { return total_cnt_; }

public:
  // Init pool.
  // Should provide allocator, and the size of prealloc task number.
  int init(common::ObIAllocator *task_alloc,
      const int64_t prealloc_pool_size,
      const bool allow_dynamic_alloc,
      const int64_t prealloc_page_count)
  {
    int ret = common::OB_SUCCESS;
    const int64_t start_ts = get_timestamp();
    const int64_t trans_task_page_size = OB_MALLOC_NORMAL_BLOCK_SIZE;

    if (OB_UNLIKELY(inited_)) {
      ret = common::OB_INIT_TWICE;
      OBLOG_LOG(WARN, "already init", KR(ret));
    } else if (OB_ISNULL(alloc_ = task_alloc)
        || OB_UNLIKELY((prealloc_task_cnt_ = prealloc_pool_size) < 1)
        || OB_UNLIKELY((task_page_size_ = trans_task_page_size) <= 0)
        || OB_UNLIKELY((prealloc_page_cnt_ = prealloc_page_count) <= 0)) {
      ret = common::OB_INVALID_ARGUMENT;
      OBLOG_LOG(WARN, "invalid argument", KR(ret), K(task_alloc), K(prealloc_pool_size),
          K(trans_task_page_size), K(prealloc_page_count));
    } else if (OB_FAIL(task_large_allocator_.init(LARGE_ALLOCATOR_TOTAL_LIMIT,
        LARGE_ALLOCATOR_HOLD_LIMIT,
        LARGE_ALLOCATOR_PAGE_SIZE))) {
      OBLOG_LOG(ERROR, "init large allocator fail", KR(ret));
    } else if (OB_FAIL(prealloc_page_pool_.init(prealloc_page_count))) {
      OBLOG_LOG(ERROR, "init prealloc page pool fail", KR(ret), K(prealloc_page_count));
    } else if (OB_FAIL(prepare_prealloc_tasks_(prealloc_pool_size, trans_task_page_size))) {
      OBLOG_LOG(ERROR, "err prepare prealloc tasks", KR(ret), K(prealloc_pool_size),
          K(trans_task_page_size));
    } else if (OB_FAIL(prepare_prealloc_pages_(prealloc_page_count, trans_task_page_size))) {
      OBLOG_LOG(ERROR, "prepare prealloc pages fail", KR(ret), K(prealloc_page_count),
          K(trans_task_page_size));
    } else {
      task_large_allocator_.set_label(common::ObModIds::OB_LOG_PART_TRANS_TASK_LARGE);
      allow_dynamic_alloc_ = allow_dynamic_alloc;
      const int64_t cost_ts_usec = get_timestamp() - start_ts;
      inited_ = true;
      OBLOG_LOG(INFO, "task_pool init success", K(prealloc_page_count), K(prealloc_pool_size),
          K(trans_task_page_size), K(cost_ts_usec));
    }
    return ret;
  }

  void destroy()
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(!inited_)) {
      ret = common::OB_NOT_INIT;
    } else if (OB_FAIL(clean_prealloc_tasks_())) {
      OBLOG_LOG(ERROR, "err clean prealloc tasks", KR(ret));
    } else {
      if (0 < total_cnt_) {
        OBLOG_LOG(WARN, "user didn't return all tasks", KR(ret), K(total_cnt_),
            K(dynamic_alloc_task_cnt_), K(used_prealloc_task_cnt_), K(prealloc_task_cnt_));
      }

      task_large_allocator_.destroy();

      // Clear pre-allocated pages
      clean_prealloc_pages_();

      inited_ = false;
      prealloc_task_cnt_ = 0;
      task_page_size_ = 0;
      allow_dynamic_alloc_ = false;
      prealloc_pool_tasks_ = NULL;
      alloc_ = NULL;
      prealloc_page_cnt_ = 0;
      prealloc_pages_ = NULL;
      total_cnt_ = 0;
      dynamic_alloc_task_cnt_ = 0;
      used_prealloc_task_cnt_ = 0;

      prealloc_page_pool_.destroy();
    }
  }

public:
  // Get a task.
  // Return NULL when it runs out of memory.
  TaskType* get(const char *info, const logservice::TenantLSID &tls_id)
  {
    int ret = common::OB_SUCCESS;
    TaskType *ret_task = NULL;

    if (OB_UNLIKELY(! inited_)) {
      OBLOG_LOG(WARN, "task pool not init", K(inited_));
      ret = common::OB_NOT_INIT;
    } else {
      PoolElemType *elem = NULL;
      while (common::OB_SUCCESS == ret && NULL == elem) {
        if (common::OB_SUCCESS == (ret = prealloc_task_pool_.pop(elem))) {
          ret_task = static_cast<TaskType *>(elem);
          (void)ATOMIC_AAF(&used_prealloc_task_cnt_, 1);
        } else if (common::OB_EAGAIN == ret) {
          ret = common::OB_SUCCESS;
          if (allow_dynamic_alloc_) {
            if (OB_ISNULL(ret_task = new_task_())) {
              ret = common::OB_ALLOCATE_MEMORY_FAILED;
              OBLOG_LOG(ERROR, "alloc task fail", KR(ret));
            } else {
              elem = ret_task;
              (void)ATOMIC_AAF(&dynamic_alloc_task_cnt_, 1);
            }
          } else {
            ret = common::OB_SUCCESS;
            elem = NULL;
            OBLOG_LOG(WARN, "no trans task avaliable, wait and retry", K(allow_dynamic_alloc_),
                K(total_cnt_), K(prealloc_task_cnt_), K(used_prealloc_task_cnt_),
                K(dynamic_alloc_task_cnt_));
            ob_usleep(500 * 1000);
          }
        } else {
          OBLOG_LOG(ERROR, "fail to pop task", KR(ret));
        }
      }
    }

    if (OB_SUCC(ret) && OB_NOT_NULL(ret_task)) {
      ret_task->set_prealloc_page(get_prealloc_page_());
      ret_task->set_task_info(tls_id, info);
    }

    return ret_task;
  }

  // Return a task.
  void revert(BaseTaskType *obj)
  {
    TaskType *task = static_cast<TaskType *>(obj);

    if (OB_ISNULL(task)) {
      // pass
    } else {
      // Reset memory on recycle
      task->reset();

      // Recycling of pre-allocated pages
      void *page = NULL;
      task->revert_prealloc_page(page);
      revert_prealloc_page_(page);
      page = NULL;

      if (prealloc_task_cnt_ > 0
          && NULL != prealloc_pool_tasks_
          && prealloc_pool_tasks_ <= task && task < (prealloc_pool_tasks_ + prealloc_task_cnt_)) {
        (void)prealloc_task_pool_.push(task);
        (void)ATOMIC_AAF(&used_prealloc_task_cnt_, -1);
      } else {
        delete_task_(task);
        (void)ATOMIC_AAF(&dynamic_alloc_task_cnt_, -1);
      }

      task = NULL;
    }
  }

  void print_stat_info()
  {
    int64_t dynamic_alloc_cnt = ATOMIC_LOAD(&dynamic_alloc_task_cnt_);
    int64_t used_prealloc_cnt = ATOMIC_LOAD(&used_prealloc_task_cnt_);
    int64_t prealloc_cnt = ATOMIC_LOAD(&prealloc_task_cnt_);
    int64_t total_cnt = ATOMIC_LOAD(&total_cnt_);

    int64_t out = dynamic_alloc_cnt + used_prealloc_cnt;
    _OBLOG_LOG(INFO, "[STAT] [TRANS_TASK_POOL] OUT=%ld/%ld FIXED=%ld/%ld DYNAMIC=%ld "
        "PAGE_POOL=%ld/%ld",
        out, total_cnt, used_prealloc_cnt, prealloc_cnt, dynamic_alloc_cnt,
        prealloc_page_pool_.get_total(), prealloc_page_cnt_);
  }
private:
  TaskType* new_task_()
  {
    TaskType *ret_task = NULL;
    int64_t alloc_size = static_cast<int64_t>(sizeof(TaskType));
    if (OB_ISNULL(ret_task = static_cast<TaskType*>(alloc_->alloc(alloc_size)))) {
      OBLOG_LOG_RET(WARN, OB_ALLOCATE_MEMORY_FAILED, "failed to alloc task", K(ret_task), K(alloc_size));
    } else {
      new(ret_task)TaskType();
      ret_task->set_pool(this);
      ret_task->set_allocator(task_page_size_, task_large_allocator_);
      (void)ATOMIC_AAF(&total_cnt_, 1);
    }
    return ret_task;
  }
  void delete_task_(TaskType *task)
  {
    if (NULL != task && NULL != alloc_) {
      task->~TaskType();
      alloc_->free(task);
      (void)ATOMIC_AAF(&total_cnt_, -1);
      task = NULL;
    }
  }
  int prepare_prealloc_tasks_(const int64_t cnt, const int64_t page_size)
  {
    int ret = common::OB_SUCCESS;
    OBLOG_LOG(DEBUG, "prepare_prealloc_tasks_", K(cnt), K(page_size));
    if (OB_UNLIKELY(cnt <= 0) || OB_UNLIKELY(page_size <= 0)) {
      OBLOG_LOG(WARN, "invalid argument", K(cnt), K(page_size));
      ret = common::OB_INVALID_ARGUMENT;
    } else if (OB_UNLIKELY(NULL != prealloc_pool_tasks_)) {
      OBLOG_LOG(WARN, "prealloc task has been allocated", KP(prealloc_pool_tasks_));
      ret = common::OB_INIT_TWICE;
    } else {
      int64_t size = static_cast<int64_t>(sizeof(TaskType) * cnt);
      void *buf = common::ob_malloc(size, common::ObModIds::OB_LOG_PART_TRANS_TASK_POOL);
      if (OB_ISNULL(prealloc_pool_tasks_ = static_cast<TaskType*>(buf))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        OBLOG_LOG(ERROR, "err alloc task pool", KR(ret), K(size));
      } else {
        OBLOG_LOG(DEBUG, "ob_log_task_pool prepare_prealloc_tasks_ malloc succ", K(page_size), "task_count", cnt);
        for (int64_t idx = 0; common::OB_SUCCESS == ret && idx < cnt; ++idx) {
          TaskType *task = prealloc_pool_tasks_ + idx;
          new(task)TaskType();
          task->set_pool(this);
          task->set_allocator(page_size, task_large_allocator_);
          total_cnt_ += 1;
          if (OB_FAIL(prealloc_task_pool_.push(task))) {
            OBLOG_LOG(ERROR, "err push prealloc task pool", KR(ret), KP(task));
          }
        }
      }
    }
    return ret;
  }
  int clean_prealloc_tasks_()
  {
    int ret = common::OB_SUCCESS;

    if (prealloc_task_cnt_ > 0 && NULL != prealloc_pool_tasks_) {
      int64_t cnt = 0;
      for (int64_t idx = 0;
          common::OB_SUCCESS == ret && idx < prealloc_task_cnt_;
          ++idx) {
        TaskType *task = NULL;
        PoolElemType *elem = NULL;
        if (OB_FAIL(prealloc_task_pool_.pop(elem))) {
          if (common::OB_EAGAIN != ret) {
            OBLOG_LOG(WARN, "err pop prealloc task pool", KR(ret), K(idx));
          }
        } else if (OB_ISNULL(elem)) {
          OBLOG_LOG(WARN, "pop prealloc task fail", KR(ret), K(elem));
          ret = common::OB_ERR_UNEXPECTED;
        } else {
          cnt += 1;
          task = static_cast<TaskType *>(elem);
          task->~TaskType();
          total_cnt_ -= 1;
        }
      }
      // Allow some tasks not returned.
      if (common::OB_EAGAIN == ret) {
        ret = common::OB_SUCCESS;
      }
      if (prealloc_task_cnt_ == cnt) {
        common::ob_free(prealloc_pool_tasks_);
        prealloc_pool_tasks_ = NULL;
        prealloc_task_cnt_ = 0;
      }
    }

    return ret;
  }

  int prepare_prealloc_pages_(const int64_t cnt, const int64_t page_size)
  {
    int ret = common::OB_SUCCESS;
    OBLOG_LOG(DEBUG, "prepare_prealloc_pages_", K(cnt), K(page_size));
    if (OB_UNLIKELY(cnt <= 0) || OB_UNLIKELY(page_size <= 0)) {
      OBLOG_LOG(ERROR, "invalid argument", K(cnt), K(page_size));
      ret = common::OB_INVALID_ARGUMENT;
    } else if (OB_UNLIKELY(NULL != prealloc_pages_)) {
      OBLOG_LOG(ERROR, "prealloc pages has been allocated", K(prealloc_pages_));
      ret = common::OB_INIT_TWICE;
    } else {
      int64_t size = page_size * cnt;
      prealloc_pages_ = common::ob_malloc(size,
          common::ObModIds::OB_LOG_PART_TRANS_TASK_PREALLOC_PAGE);

      if (OB_ISNULL(prealloc_pages_)) {
        OBLOG_LOG(ERROR, "allocate prealloc-page fail", K(cnt), K(page_size), K(size));
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
      } else {
        for (int64_t index = 0; common::OB_SUCCESS == ret && index < cnt; index++) {
          char *page = reinterpret_cast<char *>(prealloc_pages_) + (index * page_size);
          // Initialize the page and ensure that physical pages are allocated
          page[0] = '\0';

          if (OB_FAIL(prealloc_page_pool_.push(page))) {
            OBLOG_LOG(ERROR, "push prealloc page into pool fail", KR(ret), K(page), K(index),
                K(page_size));
          }
        }
      }
    }
    return ret;
  }

  void clean_prealloc_pages_()
  {
    if (NULL != prealloc_pages_) {
      if (prealloc_page_pool_.get_total() < prealloc_page_cnt_) {
        OBLOG_LOG_RET(WARN, OB_ERR_UNEXPECTED, "part trans task prealloc pages are not reverted all",
            K_(prealloc_page_cnt), "pool_size", prealloc_page_pool_.get_total());
      }

      // empty the pool
      void *page = NULL;
      while (common::OB_SUCCESS == prealloc_page_pool_.pop(page)) {
        page = NULL;
      }

      // 释放预分配页
      common::ob_free(prealloc_pages_);
      prealloc_pages_ = NULL;
      prealloc_page_cnt_ = 0;
    }
  }

  void *get_prealloc_page_()
  {
    int ret = common::OB_SUCCESS;
    void *page = NULL;

    if (OB_FAIL(prealloc_page_pool_.pop(page))) {
      if (common::OB_ENTRY_NOT_EXIST == ret) {
        // No page available
        // Normal
        ret = common::OB_SUCCESS;
      } else {
        OBLOG_LOG(ERROR, "pop page from pool fail", KR(ret));
      }
    }
    return page;
  }

  void revert_prealloc_page_(void *page)
  {
    int ret = common::OB_SUCCESS;
    if (OB_NOT_NULL(page)) {
      if (OB_FAIL(prealloc_page_pool_.push(page))) {
        OBLOG_LOG(ERROR, "push prealloc page into pool fail", KR(ret), K(page));
      }
    }
  }

private:
  bool inited_;
  int64_t prealloc_task_cnt_;
  int64_t task_page_size_;
  bool allow_dynamic_alloc_;
  common::ObLinkQueue prealloc_task_pool_;
  TaskType *prealloc_pool_tasks_;
  common::ObIAllocator *alloc_;

  // Pool of pre-allocated page objects
  int64_t prealloc_page_cnt_;
  void *prealloc_pages_;
  PagePool prealloc_page_pool_;

  // Using FIFO Allocator as a dynamic chunk allocator
  common::ObConcurrentFIFOAllocator task_large_allocator_;

  int64_t total_cnt_ CACHE_ALIGNED;
  int64_t dynamic_alloc_task_cnt_ CACHE_ALIGNED;  // Number of dynamically assigned tasks
  int64_t used_prealloc_task_cnt_ CACHE_ALIGNED;  // Number of pre-assigned tasks used

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogTransTaskPool);
};

}
}

#endif
