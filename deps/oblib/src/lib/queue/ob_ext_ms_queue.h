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

#ifndef OCEANBASE_LIB_QUEUE_OB_EXT_MS_QUEUE_H__
#define OCEANBASE_LIB_QUEUE_OB_EXT_MS_QUEUE_H__

#include "lib/queue/ob_link.h"
#include "lib/queue/ob_ms_queue.h"             // ObMsQueue
#include "lib/queue/ob_link_queue.h"           // ObLinkQueue
#include "lib/objectpool/ob_small_obj_pool.h"  // ObSmallObjPool
#include "lib/hash/ob_linear_hash_map.h"       // ObLinearHashMap
#include "common/ob_queue_thread.h"            // ObCond

#define MSQ_STAT(level, str, ...) LIB_LOG(level, "[STAT] [MsQueue] " str, ##__VA_ARGS__)

namespace oceanbase {
namespace common {
template <class KeyType>
class ObExtMsQueue {
public:
  static const int64_t DEFAULT_MS_QUEUE_ITEM_COUNT = 1024;

public:
  typedef ObLink Task;
  enum QueueFlag {
    IDLE = 0,        // There is no output data in the queue
    READY = 1,       // The queue has data to output
    HANDLING = 2,    // The data in the queue is being processed by threads
    TERMINATED = 3,  // Terminate state, no longer accept data
  };

  // Task context
  struct MsQueueItem;
  struct TaskCtx : public ObLink {
    int64_t queue_index_;         // Internal queue number
    MsQueueItem* ms_queue_item_;  // MsQueue object

    TaskCtx()
    {
      reset();
    }
    ~TaskCtx()
    {
      reset();
    }

    bool is_valid() const
    {
      return NULL != ms_queue_item_ && queue_index_ >= 0;
    }
    void reset()
    {
      queue_index_ = 0;
      ms_queue_item_ = NULL;
      ObLink::reset();
    }
    TO_STRING_KV(K_(queue_index), KP_(ms_queue_item));
  };

  struct MsQueueItem {
    static const int64_t PAGE_SIZE = OB_MALLOC_NORMAL_BLOCK_SIZE;

    bool inited_;
    KeyType key_;
    int8_t* flags_;                          // Internal queue status array
    int64_t terminated_qcount_;              // The number of queues in a terminating state
    int64_t next_to_terminate_queue_index_;  // The number of the next queue to be terminated, that is, the number of
                                             // the next queue to be pushed to terminate the task
    TaskCtx* task_ctx_array_;                // Task context array, a queue corresponds to a task context
    ObMsQueue queue_;                        // MsQueue
    ObArenaAllocator allocator_;             // allocator
    ObCond cond_;

    MsQueueItem()
        : inited_(false),
          key_(),
          flags_(NULL),
          terminated_qcount_(),
          next_to_terminate_queue_index_(0),
          task_ctx_array_(NULL),
          queue_(),
          allocator_(ObModIds::OB_EXT_MS_QUEUE_QITEM, PAGE_SIZE),
          cond_()
    {}
    ~MsQueueItem()
    {
      reset();
    }

    int init(const KeyType& key, const int64_t queue_count, const int64_t queue_len);
    void reset();
    int push(Task* task, const int64_t seq, const uint64_t hash, const int64_t timeout);
    int end_batch(const int64_t seq, const int64_t count);
    int get(Task*& task, const int64_t queue_index);

    TO_STRING_KV(K_(inited), K_(key), K_(flags), K_(terminated_qcount), K_(next_to_terminate_queue_index),
        KP_(task_ctx_array), K_(queue));
  };

  typedef ObLinearHashMap<KeyType, MsQueueItem*> MsQueueMap;
  typedef ObSmallObjPool<MsQueueItem> MsQueuePool;

public:
  ObExtMsQueue();
  virtual ~ObExtMsQueue();

public:
  /// Initialization function
  ///
  /// @param max_cached_ms_queue_item_count Maximum number of cached MsQueue
  /// @param queue_count_of_ms_queue The number of queues inside each MsQueue
  /// @param queue_len_of_ms_queue The length of the queue inside each MsQueue
  int init(const int64_t max_cached_ms_queue_item_count, const int64_t queue_count_of_ms_queue,
      const int64_t queue_len_of_ms_queue, const lib::ObLabel& label = ObModIds::OB_EXT_MS_QUEUE);
  void destroy();

public:
  /// Add <key, MsQueue> key-value pair
  /// @note The same key assumes single-threaded operation
  ///
  /// @param key target key value
  ///
  /// @retval OB_SUCCESS success
  /// @retval OB_ENTRY_EXIST already exists
  /// @retval Other error codes failed
  int add_ms_queue(const KeyType& key);

  /// Terminate the MsQueue corresponding to the key to ensure that no more data will be pushed afterwards
  /// @note A key queue is terminated by a thread
  ///
  /// @param key target key value
  /// @param end_seq end sequence number
  /// @param timeout timeout
  ///
  /// @retval OB_SUCCESS success
  /// @retval OB_TIMEOUT timeout
  /// @retval Other error codes failed
  int terminate_ms_queue(const KeyType& key, const int64_t end_seq, const int64_t timeout);

  /// Push a task with sequence number seq in the MsQueue of a specific key
  ///
  /// @note: Multi-threaded push, but one seq of a key can only be processed by one thread
  ///
  /// @param key target key value
  /// @param task target task
  /// @param seq task number
  /// @param hash hash value
  /// @param timeout timeout
  ///
  /// @retval OB_SUCCESS success
  /// @retval OB_TIMEOUT timeout
  /// @retval Other error codes failed
  int push(const KeyType& key, Task* task, const int64_t seq, const uint64_t hash, const int64_t timeout);

  /// Notify the MsQueue corresponding to the key that all tasks with the sequence number seq have been pushed.
  ///
  /// @param key target key value
  /// @param seq task number
  /// @param count number of tasks
  ///
  /// @retval OB_SUCCESS success
  /// @retval Other error codes failed
  int end_batch(const KeyType& key, const int64_t seq, const int64_t count);

  /// Get a task
  ///
  /// @param [out] task returned task
  /// @param [in/out] ctx task context
  /// @param [in] timeout timeout
  ///
  /// @retval OB_SUCCESS success
  /// @retval OB_TIMEOUT timeout
  /// @retval Other error codes failed
  int get(Task*& task, void*& ctx, const int64_t timeout);

  /// Get the number of sub-queues of MsQueue
  int64_t get_queue_count_of_ms_queue() const
  {
    return queue_count_of_ms_queue_;
  }

  /// Get the number of MsQueue
  int64_t get_ms_queue_count() const
  {
    return ms_queue_item_count_;
  }

private:
  int get_task_ctx_(TaskCtx*& ctx, const int64_t timeout);
  int push_msg_task_(TaskCtx* ctx);
  int get_task_(Task*& task, TaskCtx* ctx);
  int handle_end_task_(TaskCtx* ctx);

private:
  bool inited_;
  Task end_task_;                    // Terminate task
  ObLinkQueue task_ctx_queue_;       // Task context queue
  ObCond task_ctx_queue_cond_;       // Condition variables corresponding to the task context queue
  int64_t queue_len_of_ms_queue_;    // Queue length in MsQueue
  int64_t queue_count_of_ms_queue_;  // Number of queues in MsQueue
  int64_t ms_queue_item_count_;      // Number of MsQueueItem
  MsQueuePool ms_queue_pool_;        // Queue resource pool
  MsQueueMap ms_queue_map_;          // Queue Map structure

private:
  DISALLOW_COPY_AND_ASSIGN(ObExtMsQueue);
};

template <class KeyType>
int ObExtMsQueue<KeyType>::MsQueueItem::init(const KeyType& key, const int64_t queue_count, const int64_t queue_len)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
  } else if (!key.is_valid() || queue_count <= 0 || queue_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(ERROR, "invalid args", K(ret), "key_valid", key.is_valid(), K(queue_count), K(queue_len));
  } else if (OB_FAIL(queue_.init(queue_count, queue_len, &allocator_))) {
    LIB_LOG(ERROR, "init ms queue fail", K(ret), K(queue_count), K(queue_len));
  } else {
    flags_ = static_cast<int8_t*>(allocator_.alloc(queue_count * sizeof(int8_t)));

    if (NULL == flags_) {
      LIB_LOG(ERROR, "allocate memory for flags array fail", K(queue_count));
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      // Initialize Flags to IDLE
      for (int64_t index = 0; index < queue_count; index++) {
        flags_[index] = IDLE;
      }
    }
  }

  if (OB_SUCC(ret)) {
    int64_t tctx_size = queue_count * sizeof(TaskCtx);
    task_ctx_array_ = static_cast<TaskCtx*>(allocator_.alloc(tctx_size));

    if (NULL == task_ctx_array_) {
      LIB_LOG(ERROR, "allocate memory for TaskCtx array fail", "size", tctx_size);
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      for (int64_t index = 0; index < queue_count; index++) {
        new (task_ctx_array_ + index) TaskCtx();

        task_ctx_array_[index].queue_index_ = index;
        task_ctx_array_[index].ms_queue_item_ = this;
      }
    }
  }

  if (OB_SUCC(ret)) {
    terminated_qcount_ = 0;
    next_to_terminate_queue_index_ = 0;
    key_ = key;
    inited_ = true;
  }
  return ret;
}

template <class KeyType>
void ObExtMsQueue<KeyType>::MsQueueItem::reset()
{
  int64_t queue_count = queue_.get_queue_num();

  inited_ = false;
  queue_.destroy();

  if (NULL != flags_) {
    allocator_.free(reinterpret_cast<char*>(flags_));
    flags_ = NULL;
  }

  if (NULL != task_ctx_array_) {
    for (int64_t index = 0; index < queue_count; index++) {
      task_ctx_array_[index].~TaskCtx();
    }

    allocator_.free(reinterpret_cast<char*>(task_ctx_array_));
    task_ctx_array_ = NULL;
  }

  key_.reset();
  terminated_qcount_ = 0;
  next_to_terminate_queue_index_ = 0;
  allocator_.reset();
}

template <class KeyType>
int ObExtMsQueue<KeyType>::MsQueueItem::push(Task* task, const int64_t seq, const uint64_t hash, const int64_t timeout)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
  } else {
    int64_t end_time = ObTimeUtility::current_time() + timeout;

    while (true) {
      ret = queue_.push(task, seq, hash);

      if (OB_EAGAIN != ret) {
        break;
      }

      int64_t left_time = end_time - ObTimeUtility::current_time();

      if (left_time <= 0) {
        ret = OB_TIMEOUT;
        break;
      } else {
        cond_.timedwait(left_time);
      }
    }
  }

  return ret;
}

template <class KeyType>
int ObExtMsQueue<KeyType>::MsQueueItem::end_batch(const int64_t seq, const int64_t count)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
  } else {
    ret = queue_.end_batch(seq, count);
  }
  return ret;
}

template <class KeyType>
int ObExtMsQueue<KeyType>::MsQueueItem::get(Task*& task, const int64_t queue_index)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
  } else {
    ret = queue_.get(task, queue_index);

    if (OB_SUCC(ret)) {
      cond_.signal();
    }
  }
  return ret;
}

/////////////////////////////////////////////////////////////////////////////////////

template <class KeyType>
ObExtMsQueue<KeyType>::ObExtMsQueue()
    : inited_(false),
      end_task_(),
      task_ctx_queue_(),
      task_ctx_queue_cond_(),
      queue_len_of_ms_queue_(0),
      queue_count_of_ms_queue_(0),
      ms_queue_item_count_(0),
      ms_queue_pool_(),
      ms_queue_map_()
{}

template <class KeyType>
ObExtMsQueue<KeyType>::~ObExtMsQueue()
{
  destroy();
}

template <class KeyType>
int ObExtMsQueue<KeyType>::init(const int64_t max_cached_ms_queue_item_count, const int64_t queue_count_of_ms_queue,
    const int64_t queue_len_of_ms_queue, const lib::ObLabel& label)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
  } else if (queue_count_of_ms_queue <= 0 || queue_len_of_ms_queue <= 0 || max_cached_ms_queue_item_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(ERROR,
        "invalid args",
        K(ret),
        K(queue_count_of_ms_queue),
        K(queue_len_of_ms_queue),
        K(max_cached_ms_queue_item_count));
  } else if (OB_FAIL(ms_queue_pool_.init(max_cached_ms_queue_item_count, label))) {
    LIB_LOG(ERROR, "init ms_queue pool fail", K(ret), K(max_cached_ms_queue_item_count));
  } else if (OB_FAIL(ms_queue_map_.init(label))) {
    LIB_LOG(ERROR, "init ms_queue map fail", K(ret));
  } else {
    queue_len_of_ms_queue_ = queue_len_of_ms_queue;
    queue_count_of_ms_queue_ = queue_count_of_ms_queue;
    ms_queue_item_count_ = 0;

    inited_ = true;
  }
  return ret;
}

template <class KeyType>
void ObExtMsQueue<KeyType>::destroy()
{
  inited_ = false;
  queue_count_of_ms_queue_ = 0;
  ms_queue_item_count_ = 0;
  queue_len_of_ms_queue_ = 0;
  (void)ms_queue_map_.destroy();
  ms_queue_pool_.destroy();
}

template <class KeyType>
int ObExtMsQueue<KeyType>::add_ms_queue(const KeyType& key)
{
  int ret = OB_SUCCESS;
  MsQueueItem* queue = NULL;

  if (!inited_) {
    ret = OB_NOT_INIT;
  } else if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(ERROR, "invalid args", K(ret), "key_valid", key.is_valid());
  } else if (OB_SUCCESS == (ret = ms_queue_map_.get(key, queue))) {
    ret = OB_ENTRY_EXIST;
  } else if (OB_ENTRY_NOT_EXIST != ret) {
    LIB_LOG(ERROR, "get from ms_queue_map fail", K(ret), K(key));
  } else {  // OB_ENTRY_NOT_EXIST == ret
    queue = NULL;
    if (OB_FAIL(ms_queue_pool_.alloc(queue))) {
      LIB_LOG(ERROR, "alloc MsQueueItem fail");
    } else if (OB_FAIL(queue->init(key, queue_count_of_ms_queue_, queue_len_of_ms_queue_))) {
      LIB_LOG(ERROR, "init MsQueueItem fail", K(ret), K(key), K(queue_count_of_ms_queue_), K(queue_len_of_ms_queue_));
    } else if (OB_FAIL(ms_queue_map_.insert(key, queue))) {
      // TODO: If you want to support concurrent inserts, handle insert conflicts here
      LIB_LOG(ERROR, "insert queue into ms_queue_map fail", K(ret), K(key));
    } else {
      int64_t ms_queue_count = ATOMIC_AAF(&ms_queue_item_count_, 1);

      MSQ_STAT(INFO,
          "add",
          KP(queue),
          "queue_count",
          queue_count_of_ms_queue_,
          "queue_len",
          queue_len_of_ms_queue_,
          K(ms_queue_count),
          K(key));
    }

    if (OB_SUCCESS != ret && NULL != queue) {
      queue->reset();
      ms_queue_pool_.free(queue);
      queue = NULL;
    }
  }

  return ret;
}

template <class KeyType>
int ObExtMsQueue<KeyType>::terminate_ms_queue(const KeyType& key, const int64_t end_seq, const int64_t timeout)
{
  int ret = OB_SUCCESS;
  MsQueueItem* queue = NULL;

  if (!inited_) {
    ret = OB_NOT_INIT;
  } else if (!key.is_valid() || end_seq < 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(ERROR, "invalid args", K(ret), "key_valid", key.is_valid(), K(end_seq));
  } else if (OB_FAIL(ms_queue_map_.get(key, queue))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LIB_LOG(ERROR, "entry does not exist", K(key), K(ret));
    } else {
      LIB_LOG(ERROR, "get MsQueue fail", K(ret), K(key));
    }
  } else if (NULL == queue) {
    ret = OB_ERR_UNEXPECTED;
    LIB_LOG(INFO, "invalid queue", K(queue));
  } else if (queue->next_to_terminate_queue_index_ >= queue_count_of_ms_queue_) {
    LIB_LOG(INFO, "MsQueue has been terminated", K(key), K(queue));
  } else {
    // Terminate each sub-Queue in turn
    for (int64_t queue_index = queue->next_to_terminate_queue_index_;
         OB_SUCCESS == ret && queue_index < queue_count_of_ms_queue_;
         queue_index++) {
      MSQ_STAT(INFO, "terminate_push", KP(queue), K(queue_index), K(end_seq), K(key));

      // NOTE: It is required that each sub-Queue can only push end_task once, because there is only one end_task
      // globally, and repeated push will modify the next pointer of end_task
      if (OB_FAIL(queue->push(&end_task_, end_seq, queue_index, timeout))) {
        if (OB_TIMEOUT == ret) {
          MSQ_STAT(INFO, "terminate_push_timeout", KP(queue), K(queue_index), K(end_seq), K(key));
        } else {
          LIB_LOG(ERROR, "push end_task fail", K(ret), K(key), K(queue_index), K(end_seq), KP(queue));
        }
      } else {
        ATOMIC_INC(&(queue->next_to_terminate_queue_index_));
      }
    }

    if (OB_SUCC(ret) && (queue->next_to_terminate_queue_index_ >= queue_count_of_ms_queue_)) {
      MSQ_STAT(INFO, "terminate_end_batch", KP(queue), K(end_seq), K(key));

      if (OB_FAIL(end_batch(key, end_seq, queue_count_of_ms_queue_))) {
        LIB_LOG(ERROR, "end_batch end_seq fail", K(ret), K(queue), "queue", *queue, K(end_seq), K(key));
      }
    }
  }
  return ret;
}

template <class KeyType>
int ObExtMsQueue<KeyType>::push(
    const KeyType& key, Task* task, const int64_t seq, const uint64_t hash, const int64_t timeout)
{
  int ret = OB_SUCCESS;
  MsQueueItem* queue = NULL;

  if (!inited_) {
    ret = OB_NOT_INIT;
  } else if (!key.is_valid() || NULL == task || seq < 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(ERROR, "invalid args", K(ret), "key_valid", key.is_valid(), K(task), K(seq));
  } else if (OB_FAIL(ms_queue_map_.get(key, queue))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LIB_LOG(ERROR, "entry does not exist", K(key), K(ret));
    } else {
      LIB_LOG(ERROR, "get MsQueue fail", K(ret), K(key));
    }
  } else if (NULL == queue) {
    ret = OB_ERR_UNEXPECTED;
    LIB_LOG(INFO, "invalid queue", K(queue));
  } else if (OB_FAIL(queue->push(task, seq, hash, timeout))) {
    if (OB_TIMEOUT != ret) {
      LIB_LOG(ERROR, "push task into MsQueue fail", K(ret), K(key), KP(queue), K(task), K(seq), K(hash));
    }
  } else {
    MSQ_STAT(DEBUG, "push_task", KP(queue), K(seq), K(hash), K(task), K(key));
  }
  return ret;
}

template <class KeyType>
int ObExtMsQueue<KeyType>::end_batch(const KeyType& key, const int64_t seq, const int64_t count)
{
  int ret = OB_SUCCESS;
  MsQueueItem* queue = NULL;

  if (!inited_) {
    ret = OB_NOT_INIT;
  } else if (!key.is_valid() || seq < 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(ERROR, "invalid args", K(ret), "key_valid", key.is_valid(), K(seq), K(count));
  } else if (OB_FAIL(ms_queue_map_.get(key, queue))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LIB_LOG(ERROR, "entry does not exist", K(key), K(ret));
    } else {
      LIB_LOG(ERROR, "get MsQueue fail", K(ret), K(key));
    }
  } else if (NULL == queue) {
    ret = OB_ERR_UNEXPECTED;
    LIB_LOG(INFO, "invalid queue", K(queue));
  } else if (OB_FAIL(queue->end_batch(seq, count))) {
    LIB_LOG(ERROR, "end_batch fail", K(ret), K(key), KP(queue), K(seq), K(count));
  } else {
    MSQ_STAT(DEBUG, "end_batch", KP(queue), K(seq), K(count), K(key));

    // Check the status of each Queue and try to generate a message task for each Queue
    for (int64_t queue_index = 0; OB_SUCC(ret) && queue_index < queue_count_of_ms_queue_; queue_index++) {
      if (NULL == queue->flags_ || NULL == queue->task_ctx_array_) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        int8_t* flag_ptr = queue->flags_ + queue_index;
        TaskCtx* task_ctx_ptr = queue->task_ctx_array_ + queue_index;
        // When there is data in the Queue that can be output, forcibly set the status of the Queue to READY, and then
        // check the original status value:
        // 1. If the original Flag is in the IDLE state, a message task is generated to notify the worker thread to
        // consume the data of the Queue
        // 2. If the original Flag is in another state, it means that a thread is processing the data of the Queue, so
        // no processing is done
        if (queue->queue_.next_is_ready(queue_index)) {
          int8_t old_flag = ATOMIC_SET(flag_ptr, READY);
          if (IDLE == old_flag) {
            MSQ_STAT(DEBUG, "push_msg_task", KP(queue), K(queue_index), K(seq), K(old_flag), K(key));
            ret = push_msg_task_(task_ctx_ptr);
          }
        }
      }
    }
  }
  return ret;
}

template <class KeyType>
int ObExtMsQueue<KeyType>::push_msg_task_(TaskCtx* ctx)
{
  int ret = OB_SUCCESS;

  if (!(inited_ && NULL != ctx && ctx->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(task_ctx_queue_.push(ctx))) {
    LIB_LOG(ERROR, "push task ctx into msg queue fail", K(ret), "task_ctx", *ctx);
  } else {
    // succ
    task_ctx_queue_cond_.signal();
  }

  return ret;
}

template <class KeyType>
int ObExtMsQueue<KeyType>::get(Task*& task, void*& ctx, const int64_t timeout)
{
  int ret = OB_SUCCESS;
  TaskCtx* task_ctx = static_cast<TaskCtx*>(ctx);
  if (!inited_) {
    ret = OB_NOT_INIT;
  } else if (NULL != task_ctx && !task_ctx->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(ERROR, "invalid argument", K(task_ctx), "task_ctx", *task_ctx);
  } else {
    int64_t end_time = ObTimeUtility::current_time() + timeout;
    int64_t left_time = timeout;

    while (true) {
      // If there is no task context, get a context from the queue
      if (NULL == task_ctx && OB_FAIL(get_task_ctx_(task_ctx, left_time))) {
        if (OB_TIMEOUT != ret) {
          LIB_LOG(ERROR, "get_task_ctx_ fail", K(ret));
        }
        break;
        // Take tasks from the queue
      } else if (OB_EAGAIN != (ret = get_task_(task, task_ctx))) {
        break;
      } else {
        task_ctx = NULL;
        left_time = end_time - ObTimeUtility::current_time();

        if (left_time <= 0) {
          ret = OB_TIMEOUT;
          break;
        }
      }
    }

    // First reset the context
    ctx = NULL;
    if (OB_SUCC(ret)) {
      if (NULL == task || NULL == task_ctx || !task_ctx->is_valid()) {
        LIB_LOG(ERROR,
            "unexpected error: task or task_ctx is invalid",
            K(task),
            K(task_ctx),
            "task_ctx",
            NULL == task_ctx ? "NULL" : to_cstring(*task_ctx));
        ret = OB_ERR_UNEXPECTED;
      } else {
        ctx = task_ctx;
      }
    } else {  // OB_SUCCESS != ret
      ctx = NULL;
    }
  }

  return ret;
}

template <class KeyType>
int ObExtMsQueue<KeyType>::handle_end_task_(TaskCtx* ctx)
{

  int ret = OB_SUCCESS;

  if (!(inited_ && NULL != ctx && ctx->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    MSQ_STAT(DEBUG,
        "handle_end_task",
        "queue",
        ctx->ms_queue_item_,
        "queue_index",
        ctx->queue_index_,
        "terminated_qcount",
        ctx->ms_queue_item_->terminated_qcount_,
        K(ctx),
        "key",
        ctx->ms_queue_item_->key_);

    int64_t terminated_qcount = ATOMIC_AAF(&(ctx->ms_queue_item_->terminated_qcount_), 1);

    // If it is the last Queue to be terminated, the Queue will be recycled
    if (terminated_qcount >= queue_count_of_ms_queue_) {
      KeyType key = ctx->ms_queue_item_->key_;
      MsQueueItem* queue = ctx->ms_queue_item_;
      int64_t ms_queue_count = ATOMIC_AAF(&ms_queue_item_count_, -1);

      MSQ_STAT(INFO, "recycle_ms_queue", KP(queue), K(terminated_qcount), K(ms_queue_count), K(key));

      ctx = NULL;
      (void)ms_queue_map_.erase(key);
      queue->reset();
      ms_queue_pool_.free(queue);
      queue = NULL;
    }
  }

  return ret;
}

template <class KeyType>
int ObExtMsQueue<KeyType>::get_task_ctx_(TaskCtx*& ctx, const int64_t timeout)
{
  int ret = OB_SUCCESS;
  int64_t end_time = ObTimeUtility::current_time() + timeout;

  if (!inited_) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    while (true) {
      ret = task_ctx_queue_.pop((ObLink*&)ctx);

      if (OB_EAGAIN != ret) {
        break;
      }

      int64_t left_time = end_time - ObTimeUtility::current_time();

      if (left_time <= 0) {
        ret = OB_TIMEOUT;
        break;
      }

      task_ctx_queue_cond_.timedwait(left_time);
    }

    if (OB_SUCC(ret) && ctx->is_valid()) {
      MSQ_STAT(DEBUG,
          "consume_queue_begin",
          "queue",
          ctx->ms_queue_item_,
          "queue_index",
          ctx->queue_index_,
          "key",
          ctx->ms_queue_item_->key_);

      // After obtaining the task context, first set the corresponding queue status to HANDLING
      int8_t* flag_ptr = ctx->ms_queue_item_->flags_ + ctx->queue_index_;
      int8_t old_flag = ATOMIC_SET(flag_ptr, HANDLING);
      MSQ_STAT(DEBUG,
          "get_msg_task",
          "queue",
          ctx->ms_queue_item_,
          "queue_index",
          ctx->queue_index_,
          K(old_flag),
          "key",
          ctx->ms_queue_item_->key_);
    }
  }

  return ret;
}

template <class KeyType>
int ObExtMsQueue<KeyType>::get_task_(Task*& task, TaskCtx* ctx)
{
  int ret = OB_SUCCESS;
  if (!(inited_ && NULL != ctx && ctx->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    ret = ctx->ms_queue_item_->get(task, ctx->queue_index_);

    if (OB_SUCC(ret)) {
      LIB_LOG(DEBUG,
          "get_task",
          "queue",
          ctx->ms_queue_item_,
          "queue_index",
          ctx->queue_index_,
          K(task),
          "key",
          ctx->ms_queue_item_->key_);
    }

    if (OB_SUCCESS == ret && &end_task_ == task) {
      // If a termination task is encountered, terminate the Queue
      if (OB_FAIL(handle_end_task_(ctx))) {
        LIB_LOG(ERROR, "handle_end_task_ fail", K(ret), K(ctx));
      } else {
        task = NULL;
        ctx = NULL;

        // Always return to retry
        ret = OB_EAGAIN;
      }
    } else if (OB_EAGAIN == ret) {
      MSQ_STAT(DEBUG,
          "consume_queue_end",
          "queue",
          ctx->ms_queue_item_,
          "queue_index",
          ctx->queue_index_,
          "key",
          ctx->ms_queue_item_->key_);

      // After the data in the queue is fetched, check the Flag status of the queue
      // 1) If it is READY, it means that there is a thread that thinks that there is data in the Queue that can be
      // processed, so the message task of the Queue is directly generated for subsequent threads to process 2) If it is
      // still HANDLING, set Flag to IDLE state
      int8_t* flag_ptr = ctx->ms_queue_item_->flags_ + ctx->queue_index_;
      int8_t old_flag = ATOMIC_CAS(flag_ptr, HANDLING, IDLE);
      if (READY == old_flag) {
        MSQ_STAT(DEBUG,
            "push_msg_task",
            "queue",
            ctx->ms_queue_item_,
            "queue_index",
            ctx->queue_index_,
            K(old_flag),
            "key",
            ctx->ms_queue_item_->key_);

        if (OB_FAIL(push_msg_task_(ctx))) {
          LIB_LOG(ERROR, "push_msg_task_ fail", K(ret), K(ctx));
        } else {
          // Always return to retry
          ret = OB_EAGAIN;
        }
      }
    } else if (OB_FAIL(ret)) {
      LIB_LOG(ERROR, "get task from MsQueue fail", K(ret));
    }
  }

  return ret;
}

}  // namespace common
}  // namespace oceanbase
#endif /* OCEANBASE_LIB_QUEUE_OB_EXT_MS_QUEUE_H__ */
