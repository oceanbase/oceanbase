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

#ifndef OCEABASE_LOGSERVICE_OB_LOG_EXTERNAL_STORAGE_HANDLER_H_
#define OCEABASE_LOGSERVICE_OB_LOG_EXTERNAL_STORAGE_HANDLER_H_
#include <stdint.h>                                         // int64_t
#include "lib/utility/ob_macro_utils.h"                     // DISALLOW_COPY_AND_ASSIGN
#include "lib/utility/ob_print_utils.h"                     // TO_STRING_KV
#include "lib/thread/ob_simple_thread_pool.h"               // ObSimpleThreadPool
#include "lib/lock/ob_spin_lock.h"                          // ObSpinLock
#include "lib/lock/ob_tc_rwlock.h"                          // ObRWLock
#include "share/ob_errno.h"                                 // errno
namespace oceanbase
{
namespace common
{
class ObString;
}
namespace logservice
{
class ObLogExternalStorageIOTaskHandleIAdapter;
class ObLogExternalStorageIOTask;
class ObLogExternalStoragePreadTask;
class ObLogExternalStorageIOTaskCtx;
class ObLogExternalStorageHandler : public ObSimpleThreadPool {
public:
  ObLogExternalStorageHandler();
  ~ObLogExternalStorageHandler();

public:
  // @brief: Initializing ObLogExternalStorageHandler
  // @return value:
  //   OB_SUCCESS, initializing successfully.
  //   OB_NOT_INIT
  //   OB_INIT_TWICE
  //   OB_ALLOCATE_MEMORY_FAILED
  int init();

  // @brief: Starting thread pool with the specified concurrency.
  // @return value:
  //   OB_SUCCESS
  //   OB_ALLOCATE_MEMORY_FAILED, allocate memory failed.
  //   OB_INVALID_ARGUMENT, invalid argument, concurrency must be greater than or equal to 0 and less than or equal 128.
  //
  // NB: if concurrency is 0, means all request will be executed in current thread.
  int start(const int64_t concurrency);

  // @brief: Stoping thread pool.
  void stop();

  // @brief: Waiting flying task finished.
  void wait();

  // @brief: Destroying thread pool.
  void destroy();

  // Thread safe
  // @brief: Resizing the concurrency of thread pool, the new pread operation will be blocked.
  // @param[in]: new_concurrency, expected concurrency of thread pool
  // @param[in]: timeout_us.
  // @return value
  //   OB_SUCCESS
  //   OB_EAGAIN, lock timeout
  //   OB_NOT_INIT
  //   OB_NOT_RUNNING
	int resize(const int64_t new_concurrency,
	           const int64_t timeout_us = INT64_MAX);

  // NB: Thread safe and synchronous interface.
  // @brief: Reading up to count bytes from to a uri with storage info at a given offset.
  // @param[in]:  uri, a unique sequence of characters that identifies a logical or physical resource used by
  //              object storage system(e.g.: just like this 'oss://xxx/xxx/...').
  // @param[in]:  storage_info, the meta info used to access object storage system(e.g.: just like this
  //              'endpoint&access_id&access_key').
  // @param[in]:  offset, the given offset which want to read.
  // @param[in]:  read_buf, the read buffer.
  // @param[in]:  read_buf_size, the maximum read length.
  // @param[out]: real_read_size, the really read length.
  // @return value:
  //   OB_SUCCESS, read successfully.
  //   OB_INVALID_ARGUMENT, invalid argument.
  //   OB_ALLOCATE_MEMORY_FAILED, allocate memory failed.
  //   OB_BACKUP_PERMISSION_DENIED, permission denied.
  //   OB_BACKUP_FILE_NOT_EXIST, uri not exist.
  //   OB_OSS_ERROR, oss error.
  //   OB_FILE_LENGTH_INVALID, read offset is greater than file size.
  //   OB_NOT_INIT
  //   OB_NOT_RUNNING
  //
  // NB:
  // 1. A fixed number of asynchronous tasks will be generated based on the read length, and the minimum
  //    read length of single asynchronous task is 2MB.
  // 2. Don't generate asynchronous task when concurrency_ is 0, and execute pread in current thread.
  //
  // Recommend:
  // A maximum of N asynchronous tasks can be generated for a single pread operation(i.e. N =
  // MIN(read_buf_size/2M, concurrency)), we recommend the total length of concurrent read
  // requests not exceeded 2M*concurrency, in this way, pread can get better performance.
  //
  int pread(const common::ObString &uri,
            const common::ObString &storage_info,
            const int64_t offset,
			      char *read_buf,
			      const int64_t read_buf_size,
			      int64_t &real_read_size);

	void handle(void *task) override final;

	int64_t get_recommend_concurrency_in_single_file() const;

  TO_STRING_KV(K_(concurrency), K_(capacity), K_(is_running), K_(is_inited), KP(handle_adapter_), KP(this));
private:
  // CONCURRENCY LIMIT is 128.
  // NB: max thread number of ObSimpleThreadPool is 256.
  static const int64_t CONCURRENCY_LIMIT;
  static const int64_t DEFAULT_RETRY_INTERVAL;
  static const int64_t DEFAULT_TIME_GUARD_THRESHOLD;
  static const int64_t DEFAULT_PREAD_TIME_GUARD_THRESHOLD;
  static const int64_t DEFAULT_RESIZE_TIME_GUARD_THRESHOLD;
  static const int64_t CAPACITY_COEFFICIENT;
  static const int64_t SINGLE_TASK_MINIMUM_SIZE;

private:

  bool is_valid_concurrency_(const int64_t concurrency) const;

  int64_t get_async_task_count_(const int64_t total_size) const;
  int construct_async_tasks_and_push_them_into_thread_pool_(
    const common::ObString &uri,
    const common::ObString &storage_info,
    const int64_t offset,
    char *read_buf,
    const int64_t read_buf_size,
    int64_t &real_read_size,
    ObLogExternalStorageIOTaskCtx *&async_task_ctx);

  int wait_async_tasks_finished_(ObLogExternalStorageIOTaskCtx *async_task_ctx);

  void construct_async_read_task_(const common::ObString &uri,
                                  const common::ObString &storage_info,
                                  const int64_t offset,
                                  char *read_buf,
                                  const int64_t read_buf_size,
                                  int64_t &real_read_size,
                                  const int64_t task_idx,
                                  ObLogExternalStorageIOTaskCtx *async_task_ctx,
                                  ObLogExternalStoragePreadTask *&pread_task);

  void push_async_task_into_thread_pool_(ObLogExternalStorageIOTask *io_task);

  int resize_(const int64_t concurrency);

  bool check_need_resize_(const int64_t concurrency) const;

private:
  typedef common::RWLock RWLock;
  typedef RWLock::RLockGuard RLockGuard;
  typedef RWLock::WLockGuard WLockGuard;
  typedef RWLock::WLockGuardWithTimeout WLockGuardTimeout;
  int64_t concurrency_;
  int64_t capacity_;
  mutable RWLock resize_rw_lock_;
  ObSpinLock construct_async_task_lock_;
  ObLogExternalStorageIOTaskHandleIAdapter *handle_adapter_;
  bool is_running_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObLogExternalStorageHandler);
};
} // end namespace logservice
} // end namespace oceanbase
#endif
