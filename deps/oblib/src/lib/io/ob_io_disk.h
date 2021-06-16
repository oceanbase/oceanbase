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

#ifndef OB_IO_DISK_H
#define OB_IO_DISK_H

#include "lib/io/ob_io_common.h"
#include "lib/thread/thread_mgr_interface.h"

namespace oceanbase {
namespace common {

class ObIORequest;
class ObIOResourceManager;
class ObIOChannel;

enum ObDiskAdminStatus {
  DISK_FREE,
  DISK_USING,
  DISK_DELETING,  // start delete, can not submit new io request
  DISK_DELETED,   // ref hold by io_manager has been decreased
};

struct ObDiskMemoryStat {
public:
  ObDiskMemoryStat() : used_request_cnt_(0), used_buf_size_(0), used_cache_block_cnt_(0)
  {}
  virtual ~ObDiskMemoryStat()
  {}
  void reset()
  {
    *this = ObDiskMemoryStat();
  }
  bool is_valid() const
  {
    return used_request_cnt_ >= 0 && used_buf_size_ >= 0 && used_cache_block_cnt_ >= 0;
  }
  TO_STRING_KV(K_(used_request_cnt), K_(used_buf_size), K_(used_cache_block_cnt));

public:
  int64_t used_request_cnt_;
  int64_t used_buf_size_;
  int64_t used_cache_block_cnt_;
};

class ObDiskDiagnose {
public:
  ObDiskDiagnose();
  virtual ~ObDiskDiagnose();
  void record_read_fail(const int64_t retry_cnt);
  void record_write_fail();
  bool is_disk_warning() const;
  bool is_disk_error() const;
  void reset_disk_health();
  int64_t get_max_retry_cnt() const;
  int64_t get_warn_retry_cnt() const;
  int64_t get_disk_error_begin_ts() const
  {
    return disk_error_begin_ts_;
  }
  int64_t get_last_io_failure_ts() const;
  void reset();
  TO_STRING_KV(K_(is_disk_error), K_(disk_error_begin_ts), K_(disk_error_last_ts), K_(last_read_failure_warn_ts),
      K_(write_failure_cnt));

private:
  // more than 100 times of write failure per second, the disk is considered broken
  static const int64_t WRITE_FAILURE_DETECT_EVENT_CNT = 100;
  ObSpinLock lock_;
  bool is_disk_error_;
  int64_t disk_error_begin_ts_;
  int64_t disk_error_last_ts_;
  int64_t last_read_failure_warn_ts_;
  int64_t write_failure_cnt_;
  int64_t write_failure_event_ts_[WRITE_FAILURE_DETECT_EVENT_CNT];
};

class ObDisk : public lib::TGRunnable {
public:
  static const int64_t DEFAULT_SYS_IO_PERCENT = 10;
  static const int64_t DEFAULT_SYS_IOPS = 1000;
  static const int64_t DEFAULT_SYS_LOWER_IO_BANDWIDTH = 128 * 1024 * 1024;
  static const int64_t DEFAULT_LARGE_QUERY_IOPS = 10000;
  static const int64_t DELETE_DISK_TIMEOUT_MS = 5 * 1000;  // 5s
  static const int64_t MAX_DISK_CHANNEL_CNT = 16;
  static const int64_t MINI_MODE_DISK_CHANNEL_CNT = 2;
  ObDisk();
  virtual ~ObDisk();
  int init(const ObDiskFd& fd, const int64_t sys_io_percent, const int64_t channel_count, const int32_t queue_depth);
  void destroy();
  void run1() override;
  // delayed delete, wait for ref_cnt decrease to 0
  int start_delete();
  int wait_delete();

  // request enqueue and dequeue
  int send_request(ObIORequest& req);
  int add_to_list(ObIORequest& req);
  int remove_from_list(ObIORequest& req);

  // schedule related
  void inner_schedule();
  int64_t get_sys_io_low_percent(const ObIOConfig& io_conf) const;
  void update_io_stat(const enum ObIOCategory io_category, const enum ObIOMode io_mode, const uint64_t io_bytes,
      const uint64_t io_wait_us);

  // error diagnose
  void record_io_failure(const ObIORequest& req, const uint64_t timeout_ms);
  inline bool is_disk_warning() const
  {
    return diagnose_.is_disk_warning();
  }
  inline bool is_disk_error() const
  {
    return diagnose_.is_disk_error();
  }
  inline void reset_disk_health()
  {
    return diagnose_.reset_disk_health();
  }

  // simple get
  bool is_inited() const
  {
    return inited_;
  }
  const ObDiskFd& get_fd() const
  {
    return fd_;
  }
  ObDiskMemoryStat& get_memory_stat()
  {
    return memory_stat_;
  }
  ObDiskDiagnose& get_disk_diagnose()
  {
    return diagnose_;
  }
  ObDiskAdminStatus get_admin_status() const
  {
    return admin_status_;
  }

  void inc_ref();
  void dec_ref();

  TO_STRING_KV(K_(fd), K_(ref_cnt), K_(admin_status));

private:
  int update_request_deadline(ObIORequest& req);

private:
  bool inited_;
  ObDiskFd fd_;
  int64_t ref_cnt_;
  ObDiskAdminStatus admin_status_;
  ObThreadCond admin_cond_;
  ObIOChannel channels_[MAX_DISK_CHANNEL_CNT];  // each channel has a pair of thread for submit and get_event
  int64_t channel_count_;                       // number of channels

  ObDList<ObIORequest> flying_list_;
  lib::ObMutex flying_list_mutex_;

  // error detect
  ObDiskMemoryStat memory_stat_;
  ObDiskDiagnose diagnose_;

  // schedule related
  ObIOStat io_stat_[ObIOCategory::MAX_IO_CATEGORY][ObIOMode::IO_MODE_MAX];
  ObIOStatDiff io_estimator_[ObIOCategory::MAX_IO_CATEGORY][ObIOMode::IO_MODE_MAX];
  ObCpuStatDiff cpu_estimator_;
  int64_t sys_io_percent_;
  int64_t sys_io_deadline_time_;
  int64_t user_io_deadline_time_;
  int64_t sys_iops_up_limit_;
  int64_t large_query_io_deadline_time_;
  int64_t large_query_io_percent_;
  int tg_id_;
  int real_max_channel_cnt_;
};

typedef ObPointerHolder<ObDisk> DiskHolder;

class ObIOFaultDetector : public lib::TGTaskHandler {
public:
  ObIOFaultDetector();
  virtual ~ObIOFaultDetector();
  int init();
  void destroy();
  void submit_retry_task(const ObIOInfo& info, const uint64_t timeout_ms);
  void handle(void* task);

private:
  static const int64_t TASK_NUM = 100;
  static const int64_t THREAD_NUM = 1;

  struct RetryTask {
    ObIOInfo info_;
    uint64_t timeout_ms_;
  };

  bool is_inited_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObIOFaultDetector);
};

class ObDiskGuard {
public:
  ObDiskGuard() : disk_(NULL)
  {}
  virtual ~ObDiskGuard();

  void set_disk(ObDisk* disk);
  ObDisk* get_disk()
  {
    return disk_;
  };
  void reset();
  TO_STRING_KV(KP_(disk));

private:
  ObDisk* disk_;
  DISALLOW_COPY_AND_ASSIGN(ObDiskGuard);
};

class ObDiskManager {
public:
  static const int64_t MAX_DISK_NUM =
      common::OB_MAX_DISK_NUMBER;  // replace MAX_DISK_NUM with disk_number_limit_ in cpp
  ObDiskManager();
  virtual ~ObDiskManager();
  int init(const int32_t disk_number_limit, ObIOResourceManager* resource_mgr);
  void destroy();

  // add or delete disk
  int add_disk(
      const ObDiskFd& fd, const int64_t sys_io_percent, const int64_t channel_count, const int32_t queue_depth);
  int delete_disk(const ObDiskFd& fd);

  // schedule
  void schedule_all_disks();

  // error detect
  int is_disk_error(const ObDiskFd& fd, bool& disk_error);
  int is_disk_warning(const ObDiskFd& fd, bool& disk_warning);
  int get_error_disks(ObIArray<ObDiskFd>& error_disks);
  int get_warning_disks(ObIArray<ObDiskFd>& warning_disks);
  int reset_all_disks_health();
  void submit_retry_task(const ObIOInfo& info, const uint64_t timeout_ms)
  {
    return io_fault_detector_.submit_retry_task(info, timeout_ms);
  }

  // simple get
  int get_disk_with_guard(const ObDiskFd& fd, ObDiskGuard& guard);

private:
  ObDisk* get_disk(const ObDiskFd& fd);
  bool is_disk_exist(const ObDiskFd& fd);

private:
  bool inited_;
  int32_t disk_count_;
  int32_t disk_number_limit_;
  ObDisk disk_array_[MAX_DISK_NUM];
  lib::ObMutex admin_mutex_;
  ObIOFaultDetector io_fault_detector_;
  ObIOResourceManager* resource_mgr_;
};

} /* namespace common */
} /* namespace oceanbase */

#endif
