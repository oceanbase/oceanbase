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

#ifndef OB_BUILD_INDEX_TASK_EXECUTOR_H_
#define OB_BUILD_INDEX_TASK_EXECUTOR_H_

#include "lib/hash/ob_hashset.h"
#include "lib/allocator/ob_concurrent_fifo_allocator.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/lock/ob_thread_cond.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/thread/thread_mgr_interface.h"
#include "share/ob_errno.h"
#include "share/ob_thread_pool.h"

namespace oceanbase {
namespace share {

enum ObIDDLTaskType {
  DDL_TASK_CHECK_SCHEMA = 0,
  DDL_TASK_SCHEDULE_BUILD_INDEX = 1,
  DDL_TASK_RS_BUILD_INDEX = 2,
  DDL_TASK_REFRESH_MEMORY_PERCENTAGE = 3,
};

class ObIDDLTask : public common::ObDLinkBase<ObIDDLTask> {
public:
  explicit ObIDDLTask(const ObIDDLTaskType task_type)
      : need_retry_(false), task_id_(), is_inited_(false), type_(task_type)
  {}
  virtual ~ObIDDLTask()
  {}
  virtual int64_t hash() const = 0;
  virtual int process() = 0;
  virtual bool need_retry() const
  {
    return need_retry_;
  };
  virtual int64_t get_deep_copy_size() const = 0;
  virtual ObIDDLTask* deep_copy(char* buf, const int64_t size) const = 0;
  virtual bool operator==(const ObIDDLTask& other) const = 0;
  ObIDDLTaskType get_type() const
  {
    return type_;
  }
  static bool error_need_retry(const int ret_code)
  {
    return common::OB_TIMEOUT == ret_code || common::OB_STATE_NOT_MATCH == ret_code ||
           common::OB_SERVER_IS_STOPPING == ret_code || common::OB_SERVER_IS_INIT == ret_code ||
           common::OB_EAGAIN == ret_code || common::OB_NOT_MASTER == ret_code ||
           common::OB_TRANS_STMT_TIMEOUT == ret_code || common::OB_RS_NOT_MASTER == ret_code ||
           common::OB_SCHEMA_EAGAIN == ret_code || common::OB_GTS_NOT_READY == ret_code ||
           common::OB_ERR_SHARED_LOCK_CONFLICT == ret_code || common::OB_PARTITION_NOT_EXIST == ret_code ||
           common::OB_PG_IS_REMOVED == ret_code || common::OB_TENANT_NOT_EXIST == ret_code ||
           common::OB_REPLICA_NOT_READABLE == ret_code;
  }

protected:
  typedef common::ObCurTraceId::TraceId TaskId;
  bool need_retry_;
  TaskId task_id_;
  bool is_inited_;
  ObIDDLTaskType type_;
};

class ObDDLTaskQueue {
public:
  ObDDLTaskQueue();
  virtual ~ObDDLTaskQueue();
  int init(
      const int64_t bucket_num, const int64_t total_mem_limit, const int64_t hold_mem_limit, const int64_t page_size);
  int push_task(const ObIDDLTask& task);
  int get_next_task(ObIDDLTask*& task);
  int remove_task(ObIDDLTask* task);
  int add_task_to_last(ObIDDLTask* task);
  void destroy();

private:
  typedef common::ObDList<ObIDDLTask> TaskList;
  typedef common::hash::ObHashSet<const ObIDDLTask*, common::hash::NoPthreadDefendMode,
      common::hash::hash_func<const ObIDDLTask*>, common::hash::equal_to<const ObIDDLTask*> >
      TaskSet;
  TaskList task_list_;
  TaskSet task_set_;
  common::ObSpinLock lock_;
  bool is_inited_;
  common::ObConcurrentFIFOAllocator allocator_;
};

class ObDDLTaskExecutor : public lib::TGRunnable {
public:
  ObDDLTaskExecutor();
  virtual ~ObDDLTaskExecutor();
  int init(const int64_t task_num, int tg_id);
  template <typename T>
  int push_task(const T& task);
  virtual void run1();
  void destroy();
  void stop();
  void wait();

private:
  static const int64_t BATCH_EXECUTE_COUNT = 1000;
  static const int64_t CHECK_TASK_INTERVAL = 100;  // 100ms
  static const int64_t TOTAL_LIMIT = 1024L * 1024L * 1024L;
  static const int64_t HOLD_LIMIT = 8 * 1024L * 1024L;
  static const int64_t PAGE_SIZE = common::OB_MALLOC_NORMAL_BLOCK_SIZE;
  static const int64_t THREAD_NUM = 1;
  typedef ObDDLTaskQueue TaskQueue;
  bool is_inited_;
  TaskQueue task_queue_;
  common::ObThreadCond cond_;
  int tg_id_;
};

template <typename T>
int ObDDLTaskExecutor::push_task(const T& task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDDLTaskExecutor has not been inited", K(ret));
  } else if (OB_FAIL(task_queue_.push_task(task))) {
    if (common::OB_ENTRY_EXIST != ret) {
      STORAGE_LOG(WARN, "fail to push back task", K(ret));
    }
  } else if (OB_FAIL(cond_.broadcast())) {
    STORAGE_LOG(WARN, "fail to broadcase siginal", K(ret));
  }
  return ret;
}

}  // end namespace share
}  // end namespace oceanbase

#endif  // OB_BUILD_INDEX_TASK_EXECUTOR_H_
