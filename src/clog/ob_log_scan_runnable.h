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

#ifndef OCEANBASE_CLOG_OB_LOG_SCAN_RUNNABLE_H_
#define OCEANBASE_CLOG_OB_LOG_SCAN_RUNNABLE_H_

#include "share/ob_thread_pool.h"
#include "ob_log_define.h"
#include "ob_log_reader_interface.h"

namespace oceanbase {
namespace storage {
class ObPartitionService;
}
namespace clog {
class ObIPartitionLogService;
class ObILogEngine;
class ObLogScanRunnable : public share::ObThreadPool {
public:
  static const int64_t MAX_THREAD_CNT = FILE_RANGE_THREAD_CNT;
  static const int64_t MINI_MODE_THREAD_CNT = MINI_MODE_FILE_RANGE_THREAD_CNT;

public:
  ObLogScanRunnable();
  virtual ~ObLogScanRunnable();

public:
  int init(storage::ObPartitionService* partition_service, ObILogEngine* log_engine);
  int start();
  void stop();
  void wait();
  void destroy();

public:
  // ObLogScanRunnable:
  // 1. Load file_id_cache;
  // 2. Get the next_index_log_id of each partition and update it in partition log service;
  // 3. Get min last_submit_timestamp of all partition;
  // 4. Scan the InfoBlock of all clog files in binary search, and find the start_file_id of clog;
  // 5. Starting from start_file_id, traverse all clog entries, for each clog entry
  //  1). If the log_id of clog entry is smaller than last_replay_log_id of base_storage_info, skip it;
  //  2). According to pkey and log_id in clog entry, query ObIlogStorage, determine clog entry whether
  //      can be submitted to sliding_window
  //  3). sliding_window will determine whether to be replay by next_index_log_id
  // 6. Release resource
  void run1();
  bool is_before_scan() const;
  bool is_scan_finished() const;

private:
  class LocateFileRangeTimerTask : public common::ObTimerTask {
  public:
    LocateFileRangeTimerTask();
    ~LocateFileRangeTimerTask()
    {}

  public:
    int init(ObLogScanRunnable* host, const int64_t thread_index, const file_id_t start_file_id,
        const file_id_t last_file_id);
    virtual void runTimerTask();
    void set_result_file_id(file_id_t file_id)
    {
      result_file_id_ = file_id;
    }
    file_id_t get_result_file_id() const
    {
      return result_file_id_;
    }

  private:
    ObLogScanRunnable* host_;
    int64_t thread_index_;
    file_id_t start_file_id_;
    file_id_t last_file_id_;
    file_id_t result_file_id_;

  private:
    DISALLOW_COPY_AND_ASSIGN(LocateFileRangeTimerTask);
  };

  class ScanTimerTask : public common::ObTimerTask {
  public:
    ScanTimerTask();
    ~ScanTimerTask()
    {}

  public:
    int init(ObLogScanRunnable* host, const int64_t scan_thread_index, const file_id_t start_file_id,
        const file_id_t last_file_id);
    virtual void runTimerTask();

  private:
    ObLogScanRunnable* host_;
    int64_t scan_thread_index_;
    file_id_t start_file_id_;
    file_id_t last_file_id_;

  private:
    DISALLOW_COPY_AND_ASSIGN(ScanTimerTask);
  };

private:
  void do_scan_log_();
  int fill_file_id_cache_();
  int set_next_index_log_id_();
  int get_scan_file_range_(file_id_t& start_file_id, file_id_t& last_file_id);
  int get_scan_file_range_based_on_ts_(file_id_t& start_file_id, file_id_t& last_file_id);
  int get_scan_file_range_based_on_log_id_(file_id_t& start_file_id, const file_id_t last_file_id);
  int scan_all_files_(const file_id_t start_file_id, const file_id_t last_file_id);
  int do_scan_files_(const int64_t scan_thread_index, const file_id_t start_file_id, const file_id_t last_file_id);
  int scan_one_file_(const int64_t scan_thread_index, const file_id_t curr_file_id, const file_id_t last_file_id);
  int handle_process_coordinate_(const file_id_t curr_file_id, const int64_t scan_thread_index);
  int mark_task_finished_(const int64_t thread_index);
  int wait_scan_task_finished_();
  int keep_file_max_submit_timestamp_inc_();
  int notify_scan_finished_();
  //------------------------------------------------------------//
  void update_process_bar_(const file_id_t curr_file_id, const file_id_t start_file_id, const file_id_t last_file_id,
      const int64_t scan_begin_time);
  int get_cursor_with_retry_(const int64_t scan_thread_index, clog::ObIPartitionLogService* pls,
      const common::ObPartitionKey& partition_key, const uint64_t log_id, ObLogCursorExt& log_cursor_ext) const;
  int check_can_binary_search_(const file_id_t min_file_id, bool& can_binary_search);
  int get_file_max_submit_timestamp_(const file_id_t file_id, int64_t& max_submit_timestamp);
  int compare_file_submit_timestamp_(const int64_t min_submit_timestamp, const file_id_t file_id, bool& can_skip);
  int compare_file_based_on_log_id_(const file_id_t file_id, bool& can_skip);
  int check_need_submit_(const int64_t scan_thread_index, clog::ObIPartitionLogService* pls,
      const ObLogEntry& log_entry, const ObReadParam& read_param, bool& need_submit, int64_t& accum_checksum,
      bool& batch_committed, bool& is_confirmed_log) const;
  int get_need_submit_log_range_(
      const clog::ObIPartitionLogService* pls, uint64_t& min_log_id, uint64_t& max_log_id) const;

  int do_locate_file_range_(const int64_t thread_index, const file_id_t start_file_id, const file_id_t last_file_id);
  int wait_locate_task_finished_() const;

private:
  enum ScanState { BEFORE_SCAN = 1, SCANNING = 2, SCAN_FINISHED = 3 };
  bool is_inited_;
  bool is_stopped_;
  int bkg_task_ret_;
  int64_t task_finished_cnt_;
  LocateFileRangeTimerTask locate_tasks_[FILE_RANGE_THREAD_CNT];
  int64_t scan_process_coordinator_[SCAN_THREAD_CNT];
  ScanTimerTask scan_file_tasks_[SCAN_THREAD_CNT];
  ScanState scan_state_;
  storage::ObPartitionService* partition_service_;
  ObILogEngine* log_engine_;
  int file_range_th_cnt_;
  int scan_th_cnt_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogScanRunnable);
};
}  // namespace clog
}  // namespace oceanbase

#endif  // OCEANBASE_CLOG_OB_LOG_SCAN_RUNNABLE_H_
