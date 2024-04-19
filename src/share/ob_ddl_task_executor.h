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
#include "share/location_cache/ob_location_struct.h"
#include "share/ob_errno.h"
#include "share/ob_thread_pool.h"

namespace oceanbase
{
namespace share
{

enum ObIDDLTaskType
{
  DDL_TASK_CHECK_SCHEMA = 0,
  DDL_TASK_SCHEDULE_BUILD_INDEX = 1,
  DDL_TASK_RS_BUILD_INDEX = 2,
  DDL_TASK_REFRESH_MEMORY_PERCENTAGE = 3,
};

class ObIDDLTask : public common::ObDLinkBase<ObIDDLTask>
{
public:
  explicit ObIDDLTask(const ObIDDLTaskType task_type)
    : need_retry_(false), task_id_(), is_inited_(false), type_(task_type)
  {}
  virtual ~ObIDDLTask() {}
  virtual int64_t hash() const = 0;
  virtual int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
  virtual int process() = 0;
  virtual bool need_retry() const { return need_retry_; };
  virtual int64_t get_deep_copy_size() const = 0;
  virtual ObIDDLTask *deep_copy(char *buf, const int64_t size) const = 0;
  virtual bool operator == (const ObIDDLTask &other) const = 0;
  ObIDDLTaskType get_type() const { return type_; }
  static bool in_ddl_retry_white_list(const int ret_code)
  {
    return is_not_master(ret_code)
      || is_stop_state(ret_code)
      || is_not_exist(ret_code)
      || is_retry(ret_code)
      || is_timeout(ret_code)
      || is_location_service_renew_error(ret_code);
  }
  static bool in_ddl_retry_black_list(const int ret_code)
  {
    return common::OB_SERVER_OUTOF_DISK_SPACE == ret_code || common::OB_DISK_ERROR == ret_code || OB_NOT_SUPPORTED == ret_code;
  }
  static bool is_ddl_force_no_more_process(const int ret_code)
  {
    return common::OB_STANDBY_READ_ONLY == ret_code;
  }
private:
  static bool is_timeout(const int ret_code) {
    return common::OB_TIMEOUT == ret_code || common::OB_TRANS_STMT_TIMEOUT == ret_code || common::OB_CONNECT_ERROR == ret_code
        || common::OB_WAITQUEUE_TIMEOUT == ret_code || common::OB_SESSION_NOT_FOUND == ret_code || common::OB_TRANS_TIMEOUT == ret_code
        || common::OB_TRANS_UNKNOWN == ret_code || common::OB_GET_LOCATION_TIME_OUT == ret_code || common::OB_PACKET_STATUS_UNKNOWN == ret_code
        || common::OB_TRANS_NEED_ROLLBACK == ret_code || common::OB_RPC_SEND_ERROR == ret_code || common::OB_RPC_CONNECT_ERROR == ret_code
        || common::OB_RPC_POST_ERROR == ret_code || common::OB_TRANS_ROLLBACKED == ret_code || common::OB_TRANS_KILLED == ret_code
        || common::OB_GET_LOCATION_TIME_OUT == ret_code || common::OB_TRANS_RPC_TIMEOUT == ret_code || common::OB_LIBEASY_ERROR == ret_code
        || common::OB_TRANS_CTX_NOT_EXIST == ret_code || OB_ERR_SESSION_INTERRUPTED == ret_code;
  }
  static bool is_retry(const int ret_code) {
    return common::OB_EAGAIN == ret_code || common::OB_DDL_SCHEMA_VERSION_NOT_MATCH == ret_code || common::OB_TASK_EXPIRED == ret_code || common::OB_NEED_RETRY == ret_code
        || common::OB_ERR_SHARED_LOCK_CONFLICT == ret_code || common::OB_ERR_WAIT_REMOTE_SCHEMA_REFRESH == ret_code || common::OB_SCHEMA_EAGAIN == ret_code
        || common::OB_ERR_REMOTE_SCHEMA_NOT_FULL == ret_code || common::OB_ERR_EXCLUSIVE_LOCK_CONFLICT == ret_code || common::OB_ERR_EXCLUSIVE_LOCK_CONFLICT == ret_code
        || common::OB_ERR_EXCLUSIVE_LOCK_CONFLICT_NOWAIT == ret_code || common::OB_TRANS_STMT_NEED_RETRY == ret_code || common::OB_SCHEMA_NOT_UPTODATE == ret_code
        || common::OB_TRANSACTION_SET_VIOLATION == ret_code || common::OB_TRY_LOCK_ROW_CONFLICT == ret_code || common::OB_TRANS_CANNOT_SERIALIZE == ret_code || common::OB_GTI_NOT_READY == ret_code
        || common::OB_TRANS_WEAK_READ_VERSION_NOT_READY == ret_code || common::OB_REPLICA_NOT_READABLE == ret_code || common::OB_ERR_INSUFFICIENT_PX_WORKER == ret_code
        || common::OB_EXCEED_MEM_LIMIT == ret_code || common::OB_INACTIVE_SQL_CLIENT == ret_code || common::OB_INACTIVE_RPC_PROXY == ret_code || common::OB_LS_OFFLINE == ret_code;
  }
  static bool is_not_exist(const int ret_code) {
    return common::OB_LS_NOT_EXIST == ret_code || common::OB_TABLET_NOT_EXIST == ret_code || common::OB_TENANT_NOT_EXIST == ret_code
        || common::OB_TENANT_NOT_IN_SERVER == ret_code;
  }
  static bool is_stop_state(const int ret_code) {
    return common::OB_IN_STOP_STATE == ret_code || common::OB_SERVER_IS_INIT == ret_code || common::OB_SERVER_IS_STOPPING == ret_code
        || common::OB_RS_SHUTDOWN == ret_code || common::OB_PARTITION_IS_STOPPED == ret_code
        || common::OB_PARTITION_IS_BLOCKED == ret_code;
  }
  static bool is_not_master(const int ret_code) {
    return common::OB_STATE_NOT_MATCH == ret_code || common::OB_NOT_MASTER == ret_code || OB_RS_NOT_MASTER == ret_code
        || common::OB_GTS_NOT_READY == ret_code;
  }
protected:
  typedef common::ObCurTraceId::TraceId TaskId;
  bool need_retry_;
  TaskId task_id_;
  bool is_inited_;
  ObIDDLTaskType type_;
};

class ObDDLTaskQueue
{
public:
  ObDDLTaskQueue();
  virtual ~ObDDLTaskQueue();
  int init(const int64_t bucket_num, const int64_t total_mem_limit,
      const int64_t hold_mem_limit, const int64_t page_size);
  int push_task(const ObIDDLTask &task);
  int get_next_task(ObIDDLTask *&task);
  int remove_task(ObIDDLTask *task);
  int add_task_to_last(ObIDDLTask *task);
  void destroy();
private:
  typedef common::ObDList<ObIDDLTask> TaskList;
  typedef common::hash::ObHashSet<const ObIDDLTask *,
          common::hash::NoPthreadDefendMode,
          common::hash::hash_func<const ObIDDLTask *>,
          common::hash::equal_to<const ObIDDLTask *> > TaskSet;
  TaskList task_list_;
  TaskSet task_set_;
  common::ObSpinLock lock_;
  bool is_inited_;
  common::ObConcurrentFIFOAllocator allocator_;
};

class ObDDLTaskExecutor : public lib::TGRunnable
{
public:
  ObDDLTaskExecutor();
  virtual ~ObDDLTaskExecutor();
  int init(const int64_t task_num, int tg_id);
  template <typename T>
  int push_task(const T &task);
  virtual void run1();
  void destroy();
  void stop();
  void wait();
private:
  static const int64_t BATCH_EXECUTE_COUNT = 1000;
  static const int64_t CHECK_TASK_INTERVAL = 100; // 100ms
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
int ObDDLTaskExecutor::push_task(const T &task)
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

class ObAsyncTask;

class ObDDLReplicaBuilder
{
public:
  ObDDLReplicaBuilder();
  ~ObDDLReplicaBuilder();
  int start();
  void stop();
  void destroy();
  int push_task(ObAsyncTask &task);

private:
  bool is_thread_started_;
  bool is_stopped_;
};

}  // end namespace share
}  // end namespace oceanbase

#endif  // OB_BUILD_INDEX_TASK_EXECUTOR_H_
