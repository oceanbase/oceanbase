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

#define USING_LOG_PREFIX COMMON
#include "lib/io/ob_io_disk.h"
#include "lib/io/ob_io_manager.h"
#include "lib/objectpool/ob_concurrency_objpool.h"
#include "lib/thread/ob_thread_name.h"
#include "lib/thread/thread_mgr.h"

namespace oceanbase {
using namespace lib;
namespace common {
/**
 * ---------------------------------- ObDiskFailure ----------------------------------------
 */
ObDiskDiagnose::ObDiskDiagnose()
{
  reset();
}

ObDiskDiagnose::~ObDiskDiagnose()
{}

void ObDiskDiagnose::reset()
{
  is_disk_error_ = false;
  disk_error_begin_ts_ = OB_INVALID_TIMESTAMP;
  disk_error_last_ts_ = OB_INVALID_TIMESTAMP;
  last_read_failure_warn_ts_ = 0;
  write_failure_cnt_ = 0;
  MEMSET(write_failure_event_ts_, 0, sizeof(write_failure_event_ts_));
}

void ObDiskDiagnose::record_read_fail(const int64_t retry_cnt)
{
  const ObIOConfig io_config = OB_IO_MANAGER.get_io_config();
  // in oder to reduce the misjudgement, here is the rules:
  // watch the continuous read timeout with the exponential growth of timeout
  // 1. for more than 3 times, record as dick warning,
  //    after that, this server is not allowed to be the paxos leader for a period,
  //    which is indicated by READ_FAILURE_IN_BLACK_LIST_INTERVAL, usually 300s.
  //
  // 2. for more than 6 times, record as disk error
  //    if the disk is confirmed normal, the administrator can reset the disk error by
  //    alter system set disk valid server [=] 'ip:port'
  //
  if (retry_cnt < io_config.retry_warn_limit_) {
    // do nothing
  } else if (retry_cnt < io_config.retry_error_limit_) {
    last_read_failure_warn_ts_ = ObTimeUtility::current_time();
  } else {
    if (!is_disk_error_) {
      disk_error_begin_ts_ = ObTimeUtility::current_time();
    }
    disk_error_last_ts_ = ObTimeUtility::current_time();
    is_disk_error_ = true;
    COMMON_LOG(ERROR, "set_disk_error: attention!!!");
  }
  COMMON_LOG(WARN, "io_manager add_read_failure_event", K_(last_read_failure_warn_ts), K_(is_disk_error));
}

void ObDiskDiagnose::record_write_fail()
{
  const ObIOConfig io_config = OB_IO_MANAGER.get_io_config();
  ObLockGuard<ObSpinLock> guard(lock_);
  write_failure_event_ts_[write_failure_cnt_ % WRITE_FAILURE_DETECT_EVENT_CNT] = ObTimeUtility::current_time();
  if (write_failure_cnt_ >= WRITE_FAILURE_DETECT_EVENT_CNT) {
    if (write_failure_event_ts_[write_failure_cnt_ % WRITE_FAILURE_DETECT_EVENT_CNT]  // last failure timestamp
            - write_failure_event_ts_[(write_failure_cnt_ + 1) % WRITE_FAILURE_DETECT_EVENT_CNT]  // first failure
                                                                                                  // timestamp
        <= io_config.write_failure_detect_interval_) {
      if (!is_disk_error_) {
        disk_error_begin_ts_ = ObTimeUtility::current_time();
      }
      disk_error_last_ts_ = ObTimeUtility::current_time();
      is_disk_error_ = true;
      COMMON_LOG(ERROR, "set_disk_error: attention!!!");
    }
  } else {
    // do nothing
  }
  write_failure_cnt_++;
  COMMON_LOG(WARN, "io_manager add_write_failure_event", K_(write_failure_cnt), K_(is_disk_error));
}

bool ObDiskDiagnose::is_disk_warning() const
{
  const ObIOConfig io_config = OB_IO_MANAGER.get_io_config();
  int64_t last_read_failure_time = last_read_failure_warn_ts_;
  int64_t period = ObTimeUtility::current_time() - last_read_failure_time;
  return is_disk_error_ || (last_read_failure_time > 0 && period < io_config.read_failure_black_list_interval_);
}

bool ObDiskDiagnose::is_disk_error() const
{
  return is_disk_error_;
}

void ObDiskDiagnose::reset_disk_health()
{
  is_disk_error_ = false;
  disk_error_begin_ts_ = OB_INVALID_TIMESTAMP;
  disk_error_last_ts_ = OB_INVALID_TIMESTAMP;
  last_read_failure_warn_ts_ = OB_INVALID_TIMESTAMP;
}

int64_t ObDiskDiagnose::get_last_io_failure_ts() const
{
  return MAX(disk_error_last_ts_, last_read_failure_warn_ts_);
}

int64_t ObDiskDiagnose::get_max_retry_cnt() const
{
  const ObIOConfig io_config = OB_IO_MANAGER.get_io_config();
  return io_config.retry_error_limit_;
}

int64_t ObDiskDiagnose::get_warn_retry_cnt() const
{
  const ObIOConfig io_config = OB_IO_MANAGER.get_io_config();
  return io_config.retry_warn_limit_;
}

/**
 * ---------------------------------------------- ObDisk ---------------------------------------------
 */

ObDisk::ObDisk()
    : inited_(false),
      fd_(),
      ref_cnt_(0),
      admin_status_(DISK_FREE),
      channel_count_(0),
      cpu_estimator_(),
      sys_io_percent_(DEFAULT_SYS_IO_PERCENT),
      sys_io_deadline_time_(0),
      user_io_deadline_time_(0),
      sys_iops_up_limit_(DEFAULT_SYS_IOPS),
      large_query_io_deadline_time_(0),
      large_query_io_percent_(0),
      real_max_channel_cnt_(-1)
{
  for (int64_t i = 0; i < ObIOCategory::MAX_IO_CATEGORY; ++i) {
    for (int64_t j = 0; j < ObIOMode::IO_MODE_MAX; ++j) {
      io_estimator_[i][j].set_io_stat(io_stat_[i][j]);
    }
  }
}

ObDisk::~ObDisk()
{
  destroy();
}

int ObDisk::init(
    const ObDiskFd& fd, const int64_t sys_io_percent, const int64_t channel_count, const int32_t queue_depth)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "The ObIOManager has been inited, ", K(ret));
  } else if (!fd.is_valid() || sys_io_percent <= 0 || queue_depth <= 0 || channel_count <= 0 ||
             channel_count > MAX_DISK_CHANNEL_CNT) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid arguments", K(ret), K(fd), K(sys_io_percent), K(queue_depth), K(channel_count));
  } else if (OB_FAIL(admin_cond_.init(ObWaitEventIds::DELETE_DISK_COND_WAIT))) {
    COMMON_LOG(WARN, "Fail to init admin condition, ", K(ret));
  } else {
    fd_ = fd;
    memory_stat_.reset();
    sys_io_percent_ = sys_io_percent;
    sys_io_deadline_time_ = 0;
    sys_iops_up_limit_ = DEFAULT_SYS_IOPS;
    ref_cnt_ = 0;
    channel_count_ = channel_count;
    for (int64_t i = 0; OB_SUCC(ret) && i < MAX_DISK_CHANNEL_CNT; ++i) {
      if (OB_FAIL(channels_[i].init(queue_depth))) {
        COMMON_LOG(WARN, "fail to init channel", K(ret), K(i), K(queue_depth));
      }
    }

    if (OB_SUCC(ret)) {
      real_max_channel_cnt_ = !lib::is_mini_mode() ? MAX_DISK_CHANNEL_CNT : MINI_MODE_DISK_CHANNEL_CNT;
      // schedule thread need use real_max_channel_cnt_
      // so we must set real_max_channel_cnt_ first, then set inited
      ATOMIC_SET(&admin_status_, DISK_USING);
      inited_ = true;
      if (OB_FAIL(TG_CREATE(lib::TGDefIDs::IODISK, tg_id_))) {
        COMMON_LOG(WARN, "create failed", K(ret));
      } else if (OB_FAIL(TG_SET_RUNNABLE_AND_START(tg_id_, *this))) {
        inited_ = false;
        COMMON_LOG(WARN, "Fail to create io thread, ", K(real_max_channel_cnt_ * 2), K(ret));
      } else {
        COMMON_LOG(INFO, "succeed to start disk channel thread", "chanel_count", MAX_DISK_CHANNEL_CNT * 1);
      }
    }

    if (OB_FAIL(ret)) {
      inited_ = false;
    } else {
      COMMON_LOG(INFO, "Success to init ObDisk");
    }
  }

  if (!inited_) {
    destroy();
  }
  return ret;
}

void ObDisk::destroy()
{
  TG_STOP(tg_id_);
  TG_WAIT(tg_id_);
  TG_DESTROY(tg_id_);
  fd_.reset();
  memory_stat_.reset();
  sys_io_percent_ = 0;
  sys_io_deadline_time_ = 0;
  for (int64_t i = 0; i < ObIOCategory::MAX_IO_CATEGORY; ++i) {
    for (int64_t j = 0; j < ObIOMode::IO_MODE_MAX; ++j) {
      io_stat_[i][j].reset();
    }
  }
  for (int64_t i = 0; i < MAX_DISK_CHANNEL_CNT; ++i) {
    channels_[i].destroy();
  }
  sys_iops_up_limit_ = 0;
  diagnose_.reset();
  // ref_cnt_ = 0;
  channel_count_ = 0;
  ATOMIC_SET(&admin_status_, DISK_FREE);
  admin_cond_.destroy();
  inited_ = false;
}

void ObDisk::run1()
{
  int ret = OB_SUCCESS;
  const int64_t thread_id = get_thread_idx();
  lib::set_thread_name("IODISK", thread_id);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObIOManager has not been inited, ", K(ret));
  } else {
    if (thread_id < real_max_channel_cnt_) {  // submit thread pool
      while (!has_set_stop()) {
        channels_[thread_id % real_max_channel_cnt_].submit();
      }
      COMMON_LOG(INFO, "submit thread stopped", K(fd_), K(thread_id));
    } else if (thread_id < real_max_channel_cnt_ * 2) {  // getevent thread pool
      while (!has_set_stop()) {
        channels_[(thread_id - real_max_channel_cnt_) % real_max_channel_cnt_].get_events();
      }
      COMMON_LOG(INFO, "getevent thread stopped", K(fd_), K(thread_id));
    } else {
      ret = OB_ERR_UNEXPECTED;
      if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
        COMMON_LOG(ERROR, "unexpected io thread", K(ret), K(thread_id), "channel count", MAX_DISK_CHANNEL_CNT * 1);
      }
      sleep(1);
    }
  }
}

void ObDisk::record_io_failure(const ObIORequest& req, const uint64_t timeout_ms)
{
  if ((!is_disk_warning()) && req.desc_.category_ != PREWARM_IO) {
    bool is_read = false;
    if (req.desc_.mode_ == IO_MODE_READ) {
      is_read = (IO_CMD_PREAD == req.iocb_.aio_lio_opcode);
    }

    if (is_read) {  // retry read failure, record failure when retry failed
      ObIOInfo info;
      ObIOPoint& io_point = info.io_points_[0];
      io_point.fd_ = req.fd_;
      io_point.offset_ = req.io_offset_;
      io_point.size_ = req.io_size_;
      info.size_ = io_point.size_;
      info.io_desc_ = req.desc_;
      info.batch_count_ = 1;
      OB_IO_MANAGER.get_disk_manager().submit_retry_task(info, timeout_ms);
    } else {  // record write failure
      diagnose_.record_write_fail();
    }
  }
}

void ObDisk::update_io_stat(const enum ObIOCategory io_category, const enum ObIOMode io_mode, const uint64_t io_bytes,
    const uint64_t io_wait_us)
{
  if (io_category >= 0 && io_category < ObIOCategory::MAX_IO_CATEGORY && io_mode >= 0 &&
      io_mode < ObIOMode::IO_MODE_MAX) {
    io_stat_[io_category][io_mode].update_io_stat(io_bytes, io_wait_us);
  }
}

int ObDisk::start_delete()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret));
  } else if (DISK_USING != admin_status_ && DISK_DELETING != admin_status_) {
    ret = OB_STATE_NOT_MATCH;
    COMMON_LOG(WARN, "status not match, can not delete", K(ret), K_(admin_status));
  } else {
    ATOMIC_SET(&admin_status_, DISK_DELETING);
    for (int64_t i = 0; i < MAX_DISK_CHANNEL_CNT; ++i) {
      channels_[i].stop_submit();
    }
  }
  return ret;
}

int ObDisk::wait_delete()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret));
  } else if (DISK_DELETING != admin_status_) {
    ret = OB_STATE_NOT_MATCH;
    COMMON_LOG(WARN, "status not match, can not wait delete", K(ret), K(admin_status_));
  } else if (0 == ref_cnt_) {
    ATOMIC_SET(&admin_status_, DISK_DELETED);
  } else {
    // wait all request finished
    ObThreadCondGuard guard(admin_cond_);
    if (OB_FAIL(guard.get_ret())) {
      COMMON_LOG(ERROR, "fail to guard admin condition", K(ret));
    } else if (OB_FAIL(admin_cond_.wait(DELETE_DISK_TIMEOUT_MS))) {
      if (OB_TIMEOUT == ret) {
        COMMON_LOG(WARN, "wait delete disk timeout", K(ret), K(fd_), K(ref_cnt_), LITERAL_K(DELETE_DISK_TIMEOUT_MS));
      } else {
        COMMON_LOG(ERROR, "fail to signal admin condition", K(ret));
      }
    } else {
      ATOMIC_SET(&admin_status_, DISK_DELETED);
    }
  }
  return ret;
}

int ObDisk::update_request_deadline(ObIORequest& req)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "Not init", K(ret));
  } else {
    // get sys_iops and user_iops
    const int64_t sys_iops = sys_iops_up_limit_ * sys_io_percent_ / 100 + 1;
    int64_t user_iops = 0;
    double tmp_iops = 0;
    if (OB_SUCCESS == (ObIOBenchmark::get_instance().get_max_iops(ObIOMode::IO_MODE_READ, req.io_size_, tmp_iops)) &&
        tmp_iops > 0) {
      user_iops = static_cast<int64_t>(tmp_iops);
    }
    int64_t large_query_iops = (IO_MODE_READ == req.master_->mode_) ? user_iops : sys_iops_up_limit_;
    if (large_query_io_percent_ > 0) {
      if (large_query_iops > 0) {
        large_query_iops = max(1, large_query_iops * large_query_io_percent_ / 100);
      } else {
        large_query_iops = DEFAULT_LARGE_QUERY_IOPS;
      }
    }
    // compute deadline time of this control
    const int64_t cur_time = ObTimeUtility::current_time();
    int64_t tmp_deadline_time = req.desc_.req_deadline_time_;
    if ((sys_io_percent_ >= 100) || (tmp_deadline_time > 0 && tmp_deadline_time < cur_time)) {
      req.deadline_time_ = cur_time;
    } else {
      if (req.desc_.category_ == SYS_IO) {  // calculate and update deadline time of SYS_IO
        do {
          tmp_deadline_time = sys_io_deadline_time_;
          req.deadline_time_ = max(cur_time, tmp_deadline_time + 1000000L / sys_iops);
        } while (!ATOMIC_BCAS(&sys_io_deadline_time_, tmp_deadline_time, req.deadline_time_));
      } else if (req.desc_.category_ == USER_IO) {  // calculate and update deadline time of USER_IO
        if (0 == user_iops) {
          req.deadline_time_ = cur_time;
        } else {
          do {
            tmp_deadline_time = user_io_deadline_time_;
            req.deadline_time_ = max(cur_time, tmp_deadline_time + 1000000L / user_iops);
          } while (!ATOMIC_BCAS(&user_io_deadline_time_, tmp_deadline_time, req.deadline_time_));
        }
      } else if (req.desc_.category_ == LARGE_QUERY_IO) {
        if (0 == large_query_iops) {
          req.deadline_time_ = cur_time;
        } else {
          do {
            tmp_deadline_time = large_query_io_deadline_time_;
            req.deadline_time_ = max(cur_time, tmp_deadline_time + 1000000L / large_query_iops);
          } while (!ATOMIC_BCAS(&large_query_io_deadline_time_, tmp_deadline_time, req.deadline_time_));
        }
      } else {  // calculate deadline time for other io
        req.deadline_time_ = cur_time;
      }
    }
  }
  return ret;
}

int ObDisk::send_request(ObIORequest& req)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(update_request_deadline(req))) {
    COMMON_LOG(WARN, "fail to update req deadline", K(ret), K(req));
  } else {
    const int64_t random_index = ObRandom::rand(0, channel_count_ - 1);
    ObIOChannel& channel = channels_[random_index];
    if (OB_FAIL(channel.enqueue_request(req))) {
      COMMON_LOG(WARN, "fail to enqueue request", K(ret), K(req), K(channel), K(random_index), K(channel_count_));
    }
  }
  return ret;
}

int ObDisk::add_to_list(ObIORequest& req)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret));
  } else {
    ObMutexGuard guard(flying_list_mutex_);
    if (!flying_list_.add_last(&req)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "add_last failed", K(ret), K(req));
    }
  }
  return ret;
}

int ObDisk::remove_from_list(ObIORequest& req)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret));
  } else {
    ObMutexGuard guard(flying_list_mutex_);
    if (&req != flying_list_.remove(&req)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "remove failed", K(ret), K(req));
    }
  }
  return ret;
}

void ObDisk::inner_schedule()
{
  int ret = OB_SUCCESS;
  double user_base_rt = 0;
  double user_max_rt = 0;
  double sys_max_iops = 0;
  ObIOConfig io_conf;

  io_conf = OB_IO_MANAGER.get_io_config();
  OB_ASSERT(real_max_channel_cnt_ > 0 && 0 == real_max_channel_cnt_ % 2);
  channel_count_ = MIN(io_conf.disk_io_thread_count_ / 2, real_max_channel_cnt_);

  for (int64_t i = 0; i < ObIOCategory::MAX_IO_CATEGORY; ++i) {
    for (int64_t j = 0; j < ObIOMode::IO_MODE_MAX; ++j) {
      io_estimator_[i][j].estimate();
    }
  }
  cpu_estimator_.estimate();
  const ObIOStatDiff& user_read_io_stat = io_estimator_[ObIOCategory::USER_IO][ObIOMode::IO_MODE_READ];
  const ObIOStatDiff& user_write_io_stat = io_estimator_[ObIOCategory::USER_IO][ObIOMode::IO_MODE_WRITE];
  const ObIOStatDiff& sys_read_io_stat = io_estimator_[ObIOCategory::SYS_IO][ObIOMode::IO_MODE_READ];
  const ObIOStatDiff& sys_write_io_stat = io_estimator_[ObIOCategory::SYS_IO][ObIOMode::IO_MODE_WRITE];
  const ObIOStatDiff& large_query_read_io_stat = io_estimator_[ObIOCategory::LARGE_QUERY_IO][ObIOMode::IO_MODE_READ];
  const ObIOStatDiff& large_query_write_io_stat = io_estimator_[ObIOCategory::LARGE_QUERY_IO][ObIOMode::IO_MODE_WRITE];

  if (user_read_io_stat.get_average_size() > 0 &&
      OB_SUCC(ObIOBenchmark::get_instance().get_min_rt(
          ObIOMode::IO_MODE_READ, user_read_io_stat.get_average_size(), user_base_rt))) {
    user_max_rt = user_base_rt * (1 + double(io_conf.user_iort_up_percent_) / 100.0);
  }

  if (sys_write_io_stat.get_average_size() > 0 &&
      OB_SUCC(ObIOBenchmark::get_instance().get_max_iops(
          ObIOMode::IO_MODE_WRITE, sys_write_io_stat.get_average_size(), sys_max_iops))) {
    sys_iops_up_limit_ = (int64_t)(sys_max_iops);
  }

  io_conf.sys_io_low_percent_ = get_sys_io_low_percent(io_conf);

  if ((user_max_rt > 1e-6 && user_read_io_stat.get_average_rt() > 0.9 * user_max_rt) ||
      cpu_estimator_.get_average_usage() > io_conf.cpu_high_water_level_) {
    sys_io_percent_ = max(io_conf.sys_io_low_percent_, sys_io_percent_ - 1);
  } else {
    sys_io_percent_ = min(io_conf.sys_io_high_percent_, sys_io_percent_ + 1);
  }
  large_query_io_percent_ = io_conf.large_query_io_percent_;
  if (REACH_TIME_INTERVAL(1000 * 1000 * 10)) {
    COMMON_LOG(INFO,
        "Current io stat, ",
        K_(fd),
        K(io_conf),
        K_(sys_io_percent),
        K_(sys_iops_up_limit),
        K(user_max_rt),
        K_(cpu_estimator),
        K(user_read_io_stat),
        K(user_write_io_stat),
        K(sys_read_io_stat),
        K(sys_write_io_stat),
        K(large_query_read_io_stat),
        K(large_query_write_io_stat));
  }
}

int64_t ObDisk::get_sys_io_low_percent(const ObIOConfig& io_conf) const
{
  int64_t sys_io_low_percent = io_conf.sys_io_low_percent_;
  if (0 == sys_io_low_percent) {
    // for automic handle sata and ssd disk parameter
    sys_io_low_percent = min(io_conf.sys_io_high_percent_,
        (DEFAULT_SYS_LOWER_IO_BANDWIDTH / OB_DEFAULT_MACRO_BLOCK_SIZE) * 100 / sys_iops_up_limit_);
    if (sys_io_low_percent < io_conf.DEFAULT_SYS_IO_LOW_PERCENT) {
      sys_io_low_percent = io_conf.DEFAULT_SYS_IO_LOW_PERCENT;
    }
    if (sys_io_low_percent > io_conf.sys_io_high_percent_) {
      sys_io_low_percent = io_conf.sys_io_high_percent_;
    }
  }
  return sys_io_low_percent;
}

void ObDisk::inc_ref()
{
  ATOMIC_INC(&ref_cnt_);
}

void ObDisk::dec_ref()
{
  int ret = OB_SUCCESS;
  ATOMIC_DEC(&ref_cnt_);
  if (0 == ref_cnt_ && DISK_DELETING == admin_status_) {
    ObThreadCondGuard guard(admin_cond_);
    if (OB_FAIL(guard.get_ret())) {
      COMMON_LOG(ERROR, "fail to guard admin condition", K(ret));
    } else if (OB_FAIL(admin_cond_.signal())) {
      COMMON_LOG(ERROR, "fail to signal admin condition", K(ret));
    }
  } else if (ref_cnt_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(ERROR, "find ref_cnt_ negative", K(ret), K(ref_cnt_));
    ob_abort();
  }
}

/**
 * ----------------------------------ObIOFaultDetector----------------------------------------
 */
ObIOFaultDetector::ObIOFaultDetector() : is_inited_(false)
{}

ObIOFaultDetector::~ObIOFaultDetector()
{}

int ObIOFaultDetector::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "ObIOFaultDetector inited twice", K(ret));
  } else if (OB_FAIL(TG_SET_HANDLER_AND_START(TGDefIDs::IOFaultDetector, *this))) {
    COMMON_LOG(WARN, "thread pool init error", K(ret));
  } else {
    is_inited_ = true;
  }
  if (!is_inited_) {
    destroy();
  }
  COMMON_LOG(INFO, "ObIOFaultDetector init finished", K(ret));
  return ret;
}

void ObIOFaultDetector::destroy()
{
  TG_STOP(TGDefIDs::IOFaultDetector);
  TG_WAIT(TGDefIDs::IOFaultDetector);
  is_inited_ = false;
}

void ObIOFaultDetector::submit_retry_task(const ObIOInfo& info, const uint64_t timeout_ms)
{
  int ret = OB_SUCCESS;
  RetryTask* task = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "io fault detector has not been inited, ", K(ret));
  } else if (OB_ISNULL(task = op_alloc(RetryTask))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(WARN, "alloc RetryTask failed", K(ret));
  } else {
    task->info_ = info;
    task->timeout_ms_ = timeout_ms;
    task->info_.io_desc_.category_ = PREWARM_IO;

    if (OB_FAIL(TG_PUSH_TASK(TGDefIDs::IOFaultDetector, task))) {
      COMMON_LOG(WARN, "ObIOFaultDetector push task failed", K(ret));
      op_free(task);
      task = NULL;
    }
  }
}

void ObIOFaultDetector::handle(void* t)
{
  ObDiskManager& disk_mgr = OB_IO_MANAGER.get_disk_manager();
  RetryTask* task = static_cast<RetryTask*>(t);
  int ret = OB_SUCCESS;
  ObDiskGuard guard;
  ObDisk* disk = NULL;
  if (OB_ISNULL(task)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(ERROR, "task is NULL", KP(task), K(ret));
  } else if (1 != task->info_.batch_count_) {
    ret = OB_ERR_SYS;
    COMMON_LOG(ERROR, "IO Fault Detector disk count must be 1", K(ret), "info", task->info_);
  } else if (OB_FAIL(disk_mgr.get_disk_with_guard(task->info_.io_points_[0].fd_, guard))) {
    COMMON_LOG(WARN, "fail to get disk", K(ret), "fd", task->info_.io_points_[0].fd_);
  } else if (OB_ISNULL(disk = guard.get_disk())) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "disk is null", K(ret));
  } else {
    ObDiskDiagnose& disk_diagnose = disk->get_disk_diagnose();
    const ObIOInfo& info = task->info_;
    ObIOHandle handle;
    uint64_t timeout_ms = task->timeout_ms_;
    int64_t retry_cnt = 0;
    const int64_t MIN_IO_WAIT_TIME_MS = 30000;  // 30s

    for (retry_cnt = 0; retry_cnt < disk_diagnose.get_max_retry_cnt(); ++retry_cnt) {
      handle.reset();
      // timeout grows exponentially
      if (retry_cnt >= disk_diagnose.get_warn_retry_cnt() - 1) {
        timeout_ms = max(timeout_ms * 2, MIN_IO_WAIT_TIME_MS);
      } else {
        timeout_ms = timeout_ms * 2;
      }

      if (retry_cnt == disk_diagnose.get_warn_retry_cnt()) {
        disk_diagnose.record_read_fail(retry_cnt);
      }

      if (disk->get_admin_status() != DISK_USING) {
        ret = OB_STATE_NOT_MATCH;
        COMMON_LOG(WARN, "check_admin_status failed, disk is deleting", K(ret), "status", disk->get_admin_status());
        break;
      } else if (OB_FAIL(OB_IO_MANAGER.read(info, handle, timeout_ms))) {
        COMMON_LOG(WARN, "ObIOManager::read failed", K(ret), K(info), K(timeout_ms));
      } else {
        break;  // stop retry if success
      }
    }
    disk_diagnose.record_read_fail(retry_cnt);

    op_free(task);
    task = NULL;
  }
}

/**
 *-------------------------------------------- ObDiskManager ---------------------------------------
 */
ObDiskManager::ObDiskManager() : inited_(false), disk_count_(0), disk_number_limit_(0), resource_mgr_(NULL)
{}

ObDiskManager::~ObDiskManager()
{}

int ObDiskManager::init(const int32_t disk_number_limit, ObIOResourceManager* resource_mgr)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "init twice", K(ret));
  } else if (disk_number_limit <= 0 || OB_ISNULL(resource_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments", K(ret), K(disk_number_limit), KP(resource_mgr));
  } else if (OB_FAIL(io_fault_detector_.init())) {
    COMMON_LOG(WARN, "Fail to init io fault detector, ", K(ret));
  } else {
    disk_number_limit_ = disk_number_limit;
    resource_mgr_ = resource_mgr;
    inited_ = true;
  }

  if (!inited_) {
    destroy();
  }

  return ret;
}

void ObDiskManager::destroy()
{
  io_fault_detector_.destroy();  // stop this thread first
  COMMON_LOG(INFO, "io_fault_detector_ stopped");

  for (int64_t i = 0; i < MAX_DISK_NUM; ++i) {
    if (DISK_FREE != disk_array_[i].get_admin_status()) {
      delete_disk(disk_array_[i].get_fd());
    }
    disk_array_[i].destroy();
  }

  disk_number_limit_ = 0;
  resource_mgr_ = NULL;
  inited_ = false;
}

int ObDiskManager::add_disk(
    const ObDiskFd& fd, const int64_t sys_io_percent, const int64_t channel_count, const int32_t queue_depth)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret));
  } else if (!fd.is_valid() || sys_io_percent <= 0 || channel_count <= 0 || queue_depth <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid arguments", K(ret), K(fd), K(sys_io_percent), K(channel_count), K(queue_depth));
  } else {
    ObMutexGuard guard(admin_mutex_);
    int64_t free_pos = -1;
    ObDisk* old_disk = NULL;
    if (NULL != (old_disk = get_disk(fd))) {
      ret = OB_ENTRY_EXIST;
      COMMON_LOG(WARN, "disk already exist, can not add twice", K(ret), K(*old_disk));
    } else if (disk_count_ >= disk_number_limit_) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "disk count reach limit", K(ret), K(disk_count_), K(disk_number_limit_));
    } else {
      // find free position
      for (int64_t i = 0; i < disk_number_limit_; ++i) {
        if (DISK_FREE == disk_array_[i].get_admin_status()) {
          free_pos = i;
          break;
        }
      }
      if (-1 == free_pos) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "find_free_pos failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(disk_array_[free_pos].init(fd, sys_io_percent, channel_count, queue_depth))) {
        COMMON_LOG(WARN, "disk init failed", K(ret), K(fd), K(sys_io_percent));
      } else {
        ATOMIC_INC(&disk_count_);
        COMMON_LOG(INFO, "succeed to add disk", K(fd), K(disk_count_));
      }
    }
  }
  return ret;
}

int ObDiskManager::delete_disk(const ObDiskFd& fd)
{
  int ret = OB_SUCCESS;
  ObDisk* disk = NULL;

  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret));
  } else if (!fd.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid arguments", K(ret), K(fd));
  } else {
    ObMutexGuard guard(admin_mutex_);
    if (OB_ISNULL(disk = get_disk(fd))) {
      ret = OB_ENTRY_NOT_EXIST;
      COMMON_LOG(WARN, "disk not exist", K(fd));
    } else if (DISK_DELETED == disk->get_admin_status()) {
      // already deleted, do nothing
    } else if (OB_FAIL(disk->start_delete())) {  // handle using and deleting
      COMMON_LOG(WARN, "start_delete failed", K(ret));
    } else if (OB_FAIL(disk->wait_delete())) {  // handle deleting
      COMMON_LOG(WARN, "wait_delete failed", K(ret), K(*disk));
      if (OB_TIMEOUT == ret && disk->is_disk_warning()) {
        // force clean ongoing io requests if disk is marked error
      }
    } else if (disk->get_admin_status() != DISK_DELETED) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "wrong disk status", K(ret));
    } else {
      disk->destroy();  // handle deleted, clear fd and set free
      ATOMIC_DEC(&disk_count_);
      if (disk_count_ < 0) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(ERROR, "disk count < 0", K(ret), K(disk_count_));
      }
    }
  }

  if (OB_ENTRY_NOT_EXIST == ret) {
    // if disk not exist, return OB_SUCCESS
    ret = OB_SUCCESS;
  }
  COMMON_LOG(INFO, "finish delete disk", K(ret), K(fd));
  return ret;
}

ObDisk* ObDiskManager::get_disk(const ObDiskFd& fd)
{
  ObDisk* ret_disk = NULL;
  for (int64_t i = 0; OB_ISNULL(ret_disk) && i < disk_number_limit_; ++i) {
    ObDisk& cur_disk = disk_array_[i];
    if (cur_disk.is_inited() && cur_disk.get_admin_status() != DISK_FREE && cur_disk.get_fd() == fd) {
      ret_disk = &cur_disk;
    }
  }
  return ret_disk;
}

int ObDiskManager::get_disk_with_guard(const ObDiskFd& fd, ObDiskGuard& guard)
{
  int ret = OB_SUCCESS;
  ObDisk* disk = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret));
  } else if (!fd.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(fd));
  } else if (OB_ISNULL(disk = get_disk(fd))) {
    ret = OB_ENTRY_NOT_EXIST;  // don't print log here, upper layer will print if need
  } else {
    DiskHolder holder(disk);
    if (disk->get_admin_status() != DISK_USING) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      guard.set_disk(disk);
    }
  }

  return ret;
}

void ObDiskManager::schedule_all_disks()
{
  for (int64_t i = 0; i < disk_number_limit_; ++i) {
    if (disk_array_[i].is_inited() && DISK_USING == disk_array_[i].get_admin_status()) {
      disk_array_[i].inner_schedule();
    }
  }  // end for-loop
}

int ObDiskManager::is_disk_error(const ObDiskFd& fd, bool& disk_error)
{
  int ret = OB_SUCCESS;
  ObDisk* disk = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret));
  } else if (!fd.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(fd));
  } else {
    ObMutexGuard guard(admin_mutex_);
    if (OB_ISNULL(disk = get_disk(fd))) {
      ret = OB_ENTRY_NOT_EXIST;
      COMMON_LOG(WARN, "disk not exist", K(ret), K(fd));
    } else {
      disk_error = disk->is_disk_error();
    }
  }
  return ret;
}

int ObDiskManager::is_disk_warning(const ObDiskFd& fd, bool& disk_warning)
{
  int ret = OB_SUCCESS;
  ObDisk* disk = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret));
  } else if (!fd.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(fd));
  } else {
    ObMutexGuard guard(admin_mutex_);
    if (OB_ISNULL(disk = get_disk(fd))) {
      ret = OB_ENTRY_NOT_EXIST;
      COMMON_LOG(WARN, "disk not exist", K(ret), K(fd));
    } else {
      disk_warning = disk->is_disk_warning();
    }
  }
  return ret;
}

int ObDiskManager::get_error_disks(ObIArray<ObDiskFd>& error_disks)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret));
  } else {
    ObMutexGuard guard(admin_mutex_);
    for (int32_t i = 0; OB_SUCC(ret) && i < disk_number_limit_; ++i) {
      ObDisk& disk = disk_array_[i];
      if (disk.is_inited() && disk.get_admin_status() == DISK_USING && disk.is_disk_error()) {
        if (OB_FAIL(error_disks.push_back(disk.get_fd()))) {
          COMMON_LOG(WARN, "fail to push into arrays", K(ret), K(error_disks));
        }
      }
    }
  }
  return ret;
}

int ObDiskManager::get_warning_disks(ObIArray<ObDiskFd>& warning_disks)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret));
  } else {
    ObMutexGuard guard(admin_mutex_);
    for (int32_t i = 0; OB_SUCC(ret) && i < disk_number_limit_; ++i) {
      ObDisk& disk = disk_array_[i];
      if (disk.is_inited() && disk.get_admin_status() == DISK_USING && disk.is_disk_warning()) {
        if (OB_FAIL(warning_disks.push_back(disk.get_fd()))) {
          COMMON_LOG(WARN, "fail to push into arrays", K(ret), K(warning_disks));
        }
      }
    }
  }
  return ret;
}

int ObDiskManager::reset_all_disks_health()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret));
  } else {
    ObMutexGuard guard(admin_mutex_);
    for (int32_t i = 0; OB_SUCC(ret) && i < disk_number_limit_; ++i) {
      disk_array_[i].reset_disk_health();
    }
  }
  return ret;
}

void ObDiskGuard::set_disk(ObDisk* disk)
{
  reset();
  if (NULL != disk) {
    disk->inc_ref();
    disk_ = disk;
  }
}

void ObDiskGuard::reset()
{
  if (NULL != disk_) {
    disk_->dec_ref();
  }
  disk_ = NULL;
}

ObDiskGuard::~ObDiskGuard()
{
  reset();
}
} /* namespace common */
} /* namespace oceanbase */
