/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 *
 * Thread pool with ObMapQueue
 */

#ifndef OCEANBASE_COMMON_MAP_QUEUE_THREAD_POOL_H__
#define OCEANBASE_COMMON_MAP_QUEUE_THREAD_POOL_H__

#include "ob_map_queue.h"      // ObMapQueue

#include "lib/utility/ob_macro_utils.h"     // UNUSED
#include "lib/oblog/ob_log_module.h"        // LIB_LOG
#include "lib/atomic/ob_atomic.h"           // ATOMIC_*
#include "lib/thread/thread_pool.h"         // lib::ThreadPool
#include "common/ob_queue_thread.h"         // ObCond

namespace oceanbase
{
namespace common
{

// Thread pool
//
// One ObMapQueue per thread
// Since ObMapQueue is scalable, push operations do not block
class ObMapQueueThreadPool
    : public lib::ThreadPool
{
  typedef ObMapQueue<void *> QueueType;
  static const int64_t DATA_OP_TIMEOUT = 1L * 1000L * 1000L;

public:
  ObMapQueueThreadPool();
  virtual ~ObMapQueueThreadPool();

public:
  // Inserting data
  // Non-blocking
  //
  // @retval OB_SUCCESS           Success
  // @retval Other_return_values  Fail
  int push(void *data, const uint64_t hash_val);

  // Thread execution function
  // Users can override this function to customize the thread execution
  void run1();

  // Data handling function
  // Users can also override this function to process data directly while keeping the run() function
  virtual void handle(void *data, volatile bool &is_stoped)
  {
    UNUSED(data);
  }

protected:
  /// pop data from a thread-specific queue
  ///
  /// @param thread_index Thread number
  /// @param data The data returned
  ///
  /// @retval OB_SUCCESS        success
  /// @retval OB_EAGAIN         empty queue
  /// @retval other_error_code  Fail
  int pop(const int64_t thread_index, void *&data);

  /// Execute cond timedwait on a specific thread's queue
  void cond_timedwait(const int64_t thread_index, const int64_t wait_time);

public:
  int init(const uint64_t tenant_id, const int64_t thread_num, const char *label);
  void destroy();
  int start();
  void stop();
  bool is_stoped() const { return lib::ThreadPool::has_set_stop(); }
  int64_t get_thread_num() const { return lib::ThreadPool::get_thread_count(); }

public:
  static const int64_t MAX_THREAD_NUM = 64;
  typedef ObMapQueueThreadPool HostType;
  struct ThreadConf
  {
    HostType    *host_;
    int64_t     thread_index_;
    QueueType   queue_;
    ObCond      cond_;

    ThreadConf();
    virtual ~ThreadConf();

    int init(const char *label, const int64_t thread_index, HostType *host);
    void destroy();
  };

private:
  int next_task_(const int64_t thread_index, void *&task);

private:
  bool          is_inited_;
  const char    *name_;
  ThreadConf    tc_[MAX_THREAD_NUM];

private:
  DISALLOW_COPY_AND_ASSIGN(ObMapQueueThreadPool);
};

/////////////////////////////////////////////////////////////////////////////////////////////////////////

} // namespace common
} // namespace oceanbase
#endif /* OCEANBASE_COMMON_OB_SIMPLE_THREAD_POOL_H_ */
