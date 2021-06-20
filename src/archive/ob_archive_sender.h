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

#ifndef OCEANBASE_ARCHIVE_OB_ARCHIVE_SENDER_H_
#define OCEANBASE_ARCHIVE_OB_ARCHIVE_SENDER_H_
#include "ob_log_archive_struct.h"
#include "ob_archive_pg_mgr.h"  // ObArchivePGMgr
#include "ob_archive_destination_mgr.h"
#include "ob_archive_round_mgr.h"
#include "lib/allocator/ob_concurrent_fifo_allocator.h"  // ObConcurrentFIFOAllocator
#include "lib/lock/ob_spin_rwlock.h"                     // RWLock
#include "ob_archive_thread_pool.h"

namespace oceanbase {
namespace archive {
using oceanbase::common::ObConcurrentFIFOAllocator;
class ObArchiveMgr;
struct ObArchiveSendTaskStatus;
typedef common::ObSEArray<ObArchiveSendTask*, 1000> SendTaskArray;
class ObArchiveSender : public ObArchiveThreadPool {
public:
  // Thread num in mini mode
  static const int64_t MINI_MODE_SENDER_THREAD_NUM = 1;

  // Thread queue
  static const int64_t SENDER_QUEUE_SIZE = 1000;
  static const int64_t THREAD_PUSH_TASK_TIMEOUT = 1000;

  // Send retry
  static const int64_t WRITE_RETRY_INTERVAL = DEFAULT_ARCHIVE_RETRY_INTERVAL;
  static const int64_t WRITE_MAX_RETRY_TIME = ARCHIVE_IO_MAX_RETRY_TIME;

  // converge task
  static const int64_t MAX_CONVERGE_TASK_COUNT = 20 * 1000;
  static const int64_t MAX_CONVERGE_TASK_SIZE = 8 * 1024 * 1024L;

public:
  ObArchiveSender();
  ~ObArchiveSender();

public:
  int start();
  void stop();
  void wait();
  int init(ObArchiveAllocator* allocator, ObArchivePGMgr* pg_mgr, ObArchiveRoundMgr* archive_round_mgr,
      ObArchiveMgr* archive_mgr);
  void destroy();
  int notify_start(const int64_t archive_round, const int64_t incarnation);
  int notify_stop(const int64_t archive_round, const int64_t incarnation);
  int get_send_task(const int64_t buf_len, ObArchiveSendTask*& task);
  int submit_send_task(ObArchiveSendTask* send_task);
  void release_send_task(ObArchiveSendTask* task);
  int set_archive_round_info(const int64_t round, const int64_t incarnation);
  void clear_archive_info();
  int64_t get_pre_archive_task_capacity();
  int record_index_info(
      ObPGArchiveTask& pg_archive_task, const int64_t epoch, const int64_t incarnation, const int64_t round);
  int save_server_start_archive_ts(const int64_t incarnation, const int64_t round, const int64_t start_ts);
  int check_server_start_ts_exist(const int64_t incarnation, const int64_t round, int64_t& start_ts);
  int64_t cal_work_thread_num();
  void set_thread_name_str(char* str);
  int handle_task_list(ObArchiveTaskStatus* task_status);

private:
  bool is_io_error_(const int ret_code);
  bool is_not_leader_error_(const int ret_code);

  int try_retire_task_status_(ObArchiveSendTaskStatus& task_status);
  int converge_send_task_(ObArchiveSendTaskStatus& task_status, SendTaskArray& array);
  int release_task_array_(SendTaskArray& array);
  int handle(ObArchiveSendTaskStatus& status, SendTaskArray& array);
  int submit_send_task_(ObArchiveSendTask* task);
  int check_task_valid_(ObArchiveSendTask* task);
  int fill_and_build_block_meta_(const int64_t epoch, const int64_t incarnation, const int64_t round,
      ObPGArchiveTask& pg_archive_task, SendTaskArray& array);
  int do_fill_and_build_block_meta_(const uint64_t min_log_id, const int64_t min_log_ts,
      const int64_t max_checkpoint_ts, const int64_t max_log_ts, SendTaskArray& array);
  int fill_archive_block_offset_(const int64_t offset, SendTaskArray& array);
  int get_min_upload_archive_task_(ObArchiveSendTask*& task, SendTaskArray& array);
  int build_send_buf_(const int64_t buf_len, char* buf, SendTaskArray& array);
  int archive_log_(const ObPGKey& pg_key, ObPGArchiveTask& pg_archive_task, SendTaskArray& array);
  int do_archive_log_(ObPGArchiveTask& pg_archive_task, const int64_t buf_size, SendTaskArray& array);
  int check_can_send_task_(const int64_t epoch, const int64_t incarnation, const int64_t round,
      const uint64_t max_archived_log_id, const int64_t max_archived_log_ts, SendTaskArray& array);
  int check_start_archive_task_(const ObPGKey& pg_key, const uint64_t end_log_id);
  int check_checkpoint_task_(const ObPGKey& pg_key, const uint64_t end_log_id, const int64_t log_ts,
      const uint64_t max_archive_log_id, const int64_t max_archived_log_ts);
  int check_clog_split_task_(const ObPGKey& pg_key, const uint64_t start_log_id, const int64_t log_ts,
      const uint64_t max_archive_log_id, const int64_t max_archive_log_ts, const bool need_update_log_ts);
  bool check_need_switch_file_(
      const int64_t log_size, const int64_t offset, const bool force_switch, const bool is_data_file);
  int switch_file_(ObPGArchiveTask& pg_archive_task, const int64_t epoch, const int64_t incarnation,
      const int64_t round, const LogArchiveFileType file_type);
  int update_data_file_info_(const int64_t epoch, const int64_t incarnation, const int64_t round,
      const uint64_t min_log_id, const int64_t min_log_ts, ObPGArchiveTask& pg_archive_task, const int64_t buffer_len);
  int archive_log_callback_(ObPGArchiveTask& pg_archive_task, SendTaskArray& array);
  int do_archive_log_callback_(ObPGArchiveTask& pg_archive_task, ObArchiveSendTask& task);
  int record_index_info_(ObPGArchiveTask& pg_archive_task, const int64_t epoch, const int64_t incarnation,
      const int64_t round, char* buffer, const int64_t buffer_len);
  bool in_normal_status_();
  void on_fatal_error_(const int err_ret);
  void statistic(SendTaskArray& array, const int64_t cost_ts);
  int handle_archive_error_(const ObPGKey& pg_key, const int64_t epoch_id, const int64_t incarnation,
      const int64_t round, const int ret_code, ObArchiveSendTaskStatus& status);
  int check_and_mk_server_start_ts_dir_(const int64_t incarnation, const int64_t round);
  int try_touch_archive_key_(const int64_t incarnation, const int64_t round, ObPGArchiveTask& pg_task);
  int touch_archive_key_file_(const ObPGKey& pkey, const int64_t incarnation, const int64_t round);
  int build_archive_key_prefix_(const ObPGKey& pkey, const int64_t incarnation, const int64_t round);

  typedef common::SpinRWLock RWLock;
  typedef common::SpinWLockGuard WLockGuard;

private:
  int64_t log_archive_round_;
  int64_t incarnation_;

  ObArchivePGMgr* pg_mgr_;
  ObArchiveRoundMgr* archive_round_mgr_;
  ObArchiveMgr* archive_mgr_;

  ObArchiveAllocator* allocator_;

  mutable RWLock rwlock_;
};

}  // namespace archive
}  // namespace oceanbase

#endif /* OCEANBASE_ARCHIVE_OB_ARCHIVE_SENDER_H_ */
