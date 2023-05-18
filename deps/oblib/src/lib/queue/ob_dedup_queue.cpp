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

#include "lib/queue/ob_dedup_queue.h"
#include "lib/thread/ob_thread_name.h"

namespace oceanbase
{
namespace common
{
#define ERASE_FROM_LIST(head, tail, node) \
  do \
  { \
    if (NULL == node->get_prev()) \
    { \
      head = node->get_next(); \
    } \
    else \
    { \
      node->get_prev()->set_next(node->get_next()); \
    } \
    if (NULL == node->get_next()) \
    { \
      tail = node->get_prev(); \
    } \
    else \
    { \
      node->get_next()->set_prev(node->get_prev()); \
    } \
    node->set_prev(NULL); \
    node->set_next(NULL); \
  } while(0)

ObDedupQueue::ObDedupQueue() : is_inited_(false),
                               thread_num_(DEFAULT_THREAD_NUM),
                               work_thread_num_(DEFAULT_THREAD_NUM),
                               thread_dead_threshold_(DEFALT_THREAD_DEAD_THRESHOLD),
                               hash_allocator_(allocator_),
                               bucket_allocator_(&allocator_),
                               gc_queue_head_(NULL),
                               gc_queue_tail_(NULL),
                               thread_name_(nullptr)
{
}

ObDedupQueue::~ObDedupQueue()
{
  destroy();
}

int ObDedupQueue::init(const int64_t thread_num /*= DEFAULT_THREAD_NUM*/,
                       const char* thread_name /*= NULL*/,
                       const int64_t queue_size /*= TASK_QUEUE_SIZE*/,
                       const int64_t task_map_size /*= TASK_MAP_SIZE*/,
                       const int64_t total_mem_limit /*= TOTAL_LIMIT*/,
                       const int64_t hold_mem_limit /*= HOLD_LIMIT*/,
                       const int64_t page_size /*= PAGE_SIZE*/,
                       const uint64_t tenant_id /*= OB_SERVER_TENANT_ID*/,
                       const lib::ObLabel &label /*= "DedupQueue"*/)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else if (thread_num <= 0 || thread_num > MAX_THREAD_NUM
             || total_mem_limit <= 0 || hold_mem_limit <= 0
             || page_size <= 0 || OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(thread_num), K(queue_size),
               K(total_mem_limit), K(hold_mem_limit), K(page_size), K(tenant_id));
  } else if (OB_FAIL(task_queue_sync_.init(ObWaitEventIds::DEDUP_QUEUE_COND_WAIT))) {
    COMMON_LOG(WARN, "fail to init task queue sync cond, ", K(ret));
  } else if (OB_FAIL(work_thread_sync_.init(ObWaitEventIds::DEFAULT_COND_WAIT))) {
    COMMON_LOG(WARN, "fail to init work thread sync cond, ", K(ret));
  } else {
    thread_name_ = thread_name;
    thread_num_ = thread_num;
    work_thread_num_ = thread_num;
    set_thread_count(thread_num);

    if (OB_SUCCESS != (ret = allocator_.init(page_size, label, tenant_id, total_mem_limit))) {
      COMMON_LOG(WARN, "allocator init fail", K(page_size), K(label), K(tenant_id),
                K(total_mem_limit), K(ret));
    } else if (OB_SUCCESS != (ret = task_map_.create(task_map_size, &hash_allocator_,
                                                      &bucket_allocator_))) {
      COMMON_LOG(WARN, "task_map create fail", K(ret));
    } else if (OB_SUCCESS != (ret = task_queue_.init(queue_size, &allocator_))) {
      COMMON_LOG(WARN, "task_queue init fail", K(ret));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(start())) {
      COMMON_LOG(WARN, "start thread fail", K(ret));
    } else {
      is_inited_ = true;
    }
  }
  if (OB_FAIL(ret)) {
    destroy();
  } else {
    COMMON_LOG(INFO, "init dedup-queue:",
        K(thread_num), K(queue_size), K(task_map_size), K(total_mem_limit), K(hold_mem_limit),
        K(page_size), KP(this), "lbt", lbt());
  }

  return ret;
}

void ObDedupQueue::destroy()
{
  lib::Threads::stop();
  lib::Threads::wait();
  lib::Threads::destroy();

  IObDedupTask *iter = gc_queue_head_;
  while (NULL != iter) {
    IObDedupTask *next = iter->get_next();
    destroy_task_(iter);
    iter = next;
  }
  gc_queue_head_ = NULL;
  gc_queue_tail_ = NULL;
  while (OB_SUCCESS == task_queue_.pop(iter)) {
    destroy_task_(iter);
    iter = NULL;
  }
  task_queue_.destroy();
  task_map_.destroy();
  allocator_.destroy();
  task_queue_sync_.destroy();
  work_thread_sync_.destroy();
  is_inited_ = false;
}

int ObDedupQueue::set_thread_dead_threshold(const int64_t thread_dead_threshold)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (0 > thread_dead_threshold) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "thread_dead_threshold is not valid", K(ret));
  } else {
    thread_dead_threshold_ = thread_dead_threshold;
  }
  return ret;
}

int ObDedupQueue::add_task(const IObDedupTask &task)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    MapFunction func(*this, task);
    int hash_ret = task_map_.atomic_refactored(&task, func);
    if (OB_SUCCESS == hash_ret) {
      ret = func.result_code_;
    } else if (OB_HASH_NOT_EXIST == hash_ret) {
      if (OB_FAIL(add_task_(task))) {
        COMMON_LOG(WARN, "failed to add task", K(ret));
      }
    } else {
      COMMON_LOG(WARN, "unexpected hash_ret", K(hash_ret));
      ret = hash_ret;
    }
    if (OB_SUCC(ret)) {
      ObThreadCondGuard guard(task_queue_sync_);
      (void)task_queue_sync_.signal();
    }
    if (REACH_TIME_INTERVAL(THREAD_CHECK_INTERVAL)) {
      for (int64_t i = 0; i < thread_num_; i++) {
        if (thread_metas_[i].check_dead(thread_dead_threshold_)) {
          COMMON_LOG(WARN, "thread maybe dead", K(i), K(thread_metas_[i]));
        }
      }
    }
  }

  return ret;
}

IObDedupTask *ObDedupQueue::copy_task_(const IObDedupTask &task)
{
  IObDedupTask *ret = NULL;
  if (IS_NOT_INIT) {
    COMMON_LOG_RET(WARN, OB_NOT_INIT, "ObDedupQueue is not inited");
  } else {
    int64_t deep_copy_size = task.get_deep_copy_size();
    char *memory = NULL;
    if (NULL == (memory = (char *)allocator_.alloc(deep_copy_size))) {
      COMMON_LOG_RET(WARN, OB_ALLOCATE_MEMORY_FAILED, "alloc memory fail", K(deep_copy_size), K(task.get_type()));
    } else if (NULL == (ret = task.deep_copy(memory, deep_copy_size))) {
      COMMON_LOG_RET(WARN, OB_ERROR, "deep copy task object fail", K(deep_copy_size), KP(memory));
    } else {
      COMMON_LOG(DEBUG, "deep copy task succ", K(ret), KP(memory), K(deep_copy_size));
      ret->set_memory_ptr(memory);
    }
    if (NULL == ret) {
      if (NULL != memory) {
        allocator_.free(memory);
        memory = NULL;
      }
    }
  }
  return ret;
}

void ObDedupQueue::destroy_task_(IObDedupTask *task)
{
  if (IS_NOT_INIT) {
    COMMON_LOG_RET(WARN, OB_NOT_INIT, "ObDedupQueue is not inited");
  } else if (OB_ISNULL(task)) {
    COMMON_LOG_RET(WARN, OB_INVALID_ARGUMENT, "invalid argument");
  } else {
    char *memory = task->get_memory_ptr();
    task->~IObDedupTask();
    if (NULL != memory) {
      allocator_.free(memory);
      memory = NULL;
    }
  }
}

int ObDedupQueue::map_callback_(const IObDedupTask &task, TaskMapKVPair &kvpair)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    IObDedupTask *task2remove = NULL;
    IObDedupTask *task2add = NULL;
    if (kvpair.first != kvpair.second
        || NULL == (task2remove = kvpair.second)) {
      COMMON_LOG(WARN, "unexpected key null pointer", K(kvpair.first), K(kvpair.second));
      ret = OB_ERR_UNEXPECTED;
    } else if (0 != task2remove->trylock()) {
      ret = OB_EAGAIN;
    } else if (!task2remove->is_process_done()
               || ::oceanbase::common::ObTimeUtility::current_time() < task2remove->get_abs_expired_time()) {
      task2remove->unlock();
      ret = OB_EAGAIN;
    } else if (NULL == (task2add = copy_task_(task))) {
      task2remove->unlock();
      COMMON_LOG(WARN, "copy task fail");
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_SUCCESS != (ret = task_queue_.push(task2add))) {
      task2remove->unlock();
      COMMON_LOG(WARN, "push task to queue fail", K(ret));
      destroy_task_(task2add);
      task2add = NULL;
    } else {
      gc_queue_sync_.lock();
      ERASE_FROM_LIST(gc_queue_head_, gc_queue_tail_, task2remove);
      gc_queue_sync_.unlock();
      kvpair.first = task2add;
      kvpair.second = task2add;
      task2remove->unlock();
      destroy_task_(task2remove);
      task2remove = NULL;
    }
  }
  return ret;
}

int ObDedupQueue::add_task_(const IObDedupTask &task)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    IObDedupTask *task2add = NULL;
    if (NULL == (task2add = copy_task_(task))) {
      COMMON_LOG(WARN, "copy task fail");
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      // lockstat is to aviod that job not insert task_queue was deleted from map by other thread.
      task2add->lock();
      int hash_ret = task_map_.set_refactored(task2add, task2add);
      if (OB_SUCCESS != hash_ret) {
        if (OB_HASH_EXIST != hash_ret) {
          // OB_HASH_EXIST is a possible case, do not log any warn
          COMMON_LOG(WARN, "set to task map fail", K(hash_ret));
        }
        task2add->unlock();
        destroy_task_(task2add);
        task2add = NULL;
        ret = OB_EAGAIN;
      } else if (OB_SUCCESS != (ret = task_queue_.push(task2add))) {
        COMMON_LOG(WARN, "push task to queue fail", K(ret));
        if (OB_SUCCESS != (hash_ret = task_map_.erase_refactored(task2add))) {
          task2add->unlock();
          COMMON_LOG(WARN, "unexpected erase from task_map fail", K(hash_ret));
        } else {
          task2add->unlock();
          destroy_task_(task2add);
          task2add = NULL;
        }
      } else {
        task2add->unlock();
      }
    }
  }
  return ret;
}

bool ObDedupQueue::gc_()
{
  bool bret = false;
  IObDedupTask *task_list[GC_BATCH_NUM];
  int64_t task_list_size = 0;
  if (0 == gc_queue_sync_.trylock()) {
    bret = true;
    IObDedupTask *iter = gc_queue_head_;
    while (NULL != iter
           && GC_BATCH_NUM > task_list_size) {
      IObDedupTask *next = iter->get_next();
      if (iter->is_process_done()
          && ::oceanbase::common::ObTimeUtility::current_time() > iter->get_abs_expired_time()
          && 0 == iter->trylock()) {
        task_list[task_list_size++] = iter;
        ERASE_FROM_LIST(gc_queue_head_, gc_queue_tail_, iter);
      }
      iter = next;
    }
    gc_queue_sync_.unlock();
  }
  for (int64_t i = 0; i < task_list_size; i++) {
    if (NULL == task_list[i]) {
      continue;
    }
    int hash_ret = task_map_.erase_refactored(task_list[i]);
    if (OB_SUCCESS != hash_ret) {
      const int64_t type = task_list[i]->get_type();
      task_list[i]->unlock();
      COMMON_LOG_RET(WARN, OB_ERR_UNEXPECTED, "unexpected erase from task_map fail", K(hash_ret), K(type), K(task_list_size));
    } else {
      task_list[i]->unlock();
      destroy_task_(task_list[i]);
      task_list[i] = NULL;
    }
  }
  return bret;
}

void ObDedupQueue::run1()
{
  int tmp_ret = OB_SUCCESS;
  int64_t thread_pos = (int64_t)get_thread_idx();
  ThreadMeta &thread_meta = thread_metas_[thread_pos];
  thread_meta.init();
  COMMON_LOG(INFO, "dedup queue thread start", KP(this));
  if (OB_NOT_NULL(thread_name_)) {
    lib::set_thread_name(thread_name_);
  }
  while (!has_set_stop() && !(OB_NOT_NULL(&lib::Thread::current()) ? lib::Thread::current().has_set_stop() : false)) {
    IObDedupTask *task2process = NULL;
    if (thread_pos < work_thread_num_) {
      if (OB_SUCCESS != (tmp_ret = task_queue_.pop(task2process)) && OB_UNLIKELY(tmp_ret != OB_ENTRY_NOT_EXIST)) {
        COMMON_LOG_RET(WARN, tmp_ret, "task_queue_.pop error", K(tmp_ret), K(task2process));
      } else if (NULL != task2process) {
        thread_meta.on_process_start(task2process);
        task2process->process();
        thread_meta.on_process_end();

        gc_queue_sync_.lock();
        task2process->set_next(NULL);
        if (NULL == gc_queue_tail_) {
          task2process->set_prev(NULL);
          gc_queue_head_ = task2process;
          gc_queue_tail_ = task2process;
        } else {
          task2process->set_prev(gc_queue_tail_);
          gc_queue_tail_->set_next(task2process);
          gc_queue_tail_ = task2process;
        }
        gc_queue_sync_.unlock();
        task2process->set_process_done();
      }

      thread_meta.on_gc_start();
      bool gc_done = gc_();
      thread_meta.on_gc_end();

      if ((NULL != gc_queue_head_ && gc_done)
          || NULL != task2process) {
        // need not wait
      } else {
        ObThreadCondGuard guard(task_queue_sync_);
        if (0 == task_queue_.get_total()) {
          if (OB_SUCCESS != (tmp_ret = task_queue_sync_.wait(QUEUE_WAIT_TIME_MS))) {
            if (OB_TIMEOUT != tmp_ret) {
              COMMON_LOG_RET(WARN, tmp_ret, "Fail to wait task queue sync, ", K(tmp_ret));
            }
          }
        }
      }
    } else {
      ObThreadCondGuard guard(work_thread_sync_);
      if (thread_pos >= work_thread_num_) {
        if (OB_SUCCESS != (tmp_ret = work_thread_sync_.wait(MAX_QUEUE_WAIT_TIME_MS))) {
          if (OB_TIMEOUT != tmp_ret) {
            COMMON_LOG_RET(WARN, tmp_ret, "Fail to wait work thread sync, ", K(tmp_ret));
          }
        }
      }
    }
  }
}
} // namespace common
} // namespace oceanbase
