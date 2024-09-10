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

#ifndef OCEABASE_LOGSERVICE_OB_LOG_EXTERNAL_STORAGE_H_
#define OCEABASE_LOGSERVICE_OB_LOG_EXTERNAL_STORAGE_H_
#include <stdint.h>                                         // int64_t
#include "lib/utility/ob_macro_utils.h"                     // DISALLOW_COPY_AND_ASSIGN
#include "lib/utility/ob_print_utils.h"                     // TO_STRING_KV
#include "lib/thread/ob_simple_thread_pool.h"               // ObSimpleThreadPool
#include "lib/lock/ob_spin_lock.h"                          // ObSpinLock
#include "lib/lock/ob_tc_rwlock.h"                          // ObRWLock
#include "share/ob_errno.h"                                 // errno
#include "logservice/palf/log_define.h"                     // block_id_t
#include "logservice/palf/log_iterator_storage.h"           // LogIOUser
namespace oceanbase
{
namespace common
{
class ObString;
class ObIOFd;
}
namespace share
{
class ObBackupStorageInfo;
}
namespace logservice
{
class ObLogExternalStorageHandleAdapter;
class ObLogExternalStorageCtx;
class ObLogExternalStorageCtxItem;
class ObLogExternalStorageHandler {
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
  //   OB_OBJECT_NOT_EXIST, uri not exist.
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
            const uint64_t storage_id,
            const int64_t offset,
            char *read_buf,
            const int64_t read_buf_size,
            int64_t &real_read_size,
            palf::LogIOContext &io_ctx);

  #ifdef OB_BUILD_SHARED_STORAGE
  // NB: Thread safe and synchronous interface.
  // @brief: Uploading count bytes from the buffer starting at write_buf to the file with name
  //         'tenant_id/palf_id/block_id'.
  // @param[in]:  tenant_id
  // @param[in]:  palf_id
  // @param[in]:  block_id
  // @param[in]:  write_buf, the write buffer.
  // @param[in]:  write_buf_size, the write buffer length.
  // @return value:
  //   OB_SUCCESS, read successfully.
  //   OB_INVALID_ARGUMENT, invalid argument.
  //   OB_ALLOCATE_MEMORY_FAILED, allocate memory failed.
  //   OB_BACKUP_PERMISSION_DENIED, permission denied.
  //   OB_OBJECT_NOT_EXIST, uri not exist.
  //   OB_OSS_ERROR, oss error.
  //   OB_NOT_INIT
  //   OB_NOT_RUNNING
	int upload(const uint64_t tenant_id,
	           const int64_t palf_id,
	           const palf::block_id_t block_id,
	           const char *write_buf,
	           const int64_t write_buf_size);
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
  //   OB_FILE_LENGTH_INVALID, read offset is greater than file size.
  //   OB_OSS_ERROR, oss error.
  //   OB_FILE_OR_DIRECTORY_NOT_EXIST, file not exist
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
  int pread(const uint64_t tenant_id,
            const int64_t palf_id,
            const palf::block_id_t block_id,
            const int64_t offset,
            char *read_buf,
            const int64_t read_buf_size,
            int64_t &real_read_size,
            palf::LogIOContext &io_ctx);

  // TODO by runlin: support parallel upload
  int init_multi_upload(const uint64_t tenant_id,
                        const int64_t palf_id,
                        const palf::block_id_t block_id,
                        const int64_t count,
                        ObLogExternalStorageCtx &run_ctx);
  int upload_one_part(const char *write_buff,
                      const int64_t write_buff_size,
                      const int64_t offset,
                      const int64_t part_id,
                      ObLogExternalStorageCtx &run_ctx);
  int complete_multi_upload(ObLogExternalStorageCtx &run_ctx);
  int abort_multi_upload(ObLogExternalStorageCtx &run_ctx);
  #endif

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
  static int64_t SINGLE_TASK_MINIMUM_SIZE;
  static const int64_t DEFAULT_PRINT_INTERVAL;
  int convert_ret_code_(const int ret);

private:

  bool is_valid_concurrency_(const int64_t concurrency) const;

  int64_t get_async_task_count_(const int64_t total_size) const;

  int construct_async_pread_tasks_(
    const common::ObString &uri,
    const common::ObString &storage_info,
    const uint64_t storage_id,
    const int64_t offset,
    char *read_buf,
    const int64_t read_buf_size,
    ObLogExternalStorageCtx &run_ctx);

  int resize_(const int64_t concurrency);

  bool check_need_resize_(const int64_t concurrency) const;

  #ifdef OB_BUILD_SHARED_STORAGE
  int do_upload_(const char *uri,
                 const share::ObBackupStorageInfo *info,
                 const uint64_t storage_id,
                 const char *write_buf,
                 const int64_t write_buf_size);

  enum class EnumMultiUploadResult {
    COMPLETE = 0,
    ABORT = 1,
    INVALID = 2
  };
  int finish_multi_upload_(const EnumMultiUploadResult enum_result,
                           ObLogExternalStorageCtx &run_ctx);
  #endif
private:
  typedef common::RWLock RWLock;
  typedef RWLock::RLockGuard RLockGuard;
  typedef RWLock::WLockGuard WLockGuard;
  typedef RWLock::WLockGuardWithTimeout WLockGuardTimeout;
  int64_t concurrency_;
  int64_t capacity_;
  mutable RWLock resize_rw_lock_;
  ObSpinLock construct_async_task_lock_;
  ObLogExternalStorageHandleAdapter *handle_adapter_;
  ObMiniStat::ObStatItem read_size_;
  ObMiniStat::ObStatItem read_cost_;
  bool is_running_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObLogExternalStorageHandler);
};
} // end namespace logservice
} // end namespace oceanbase
#endif
