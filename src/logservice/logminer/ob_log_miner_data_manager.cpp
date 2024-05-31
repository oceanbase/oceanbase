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
 */



#define USING_LOG_PREFIX LOGMNR

#include "ob_log_miner_record_converter.h"
#include "ob_log_miner_batch_record.h"
#include "ob_log_miner_data_manager.h"
#include "ob_log_miner_record.h"
#include "ob_log_miner_logger.h"

namespace oceanbase
{
namespace oblogminer
{
const int64_t ObLogMinerDataManager::THREAD_NUM = 1;

ObLogMinerDataManager::ObLogMinerDataManager():
    lib::ThreadPool(THREAD_NUM),
    is_inited_(false),
    stat_(),
    mode_(LogMinerMode::UNKNOWN),
    format_(RecordFileFormat::INVALID),
    logminer_record_alloc_(),
    start_progress_(OB_INVALID_TIMESTAMP),
    end_progress_(OB_INVALID_TIMESTAMP),
    output_progress_(OB_INVALID_TIMESTAMP),
    record_count_(0),
    err_handle_(nullptr) { }

ObLogMinerDataManager::~ObLogMinerDataManager()
{
  destroy();
}

int ObLogMinerDataManager::init(const LogMinerMode mode,
    const RecordFileFormat format,
    const int64_t start_progress,
    const int64_t end_progress,
    ILogMinerErrorHandler *err_handle)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("ObLogMinerDataManager has been initialized", K(is_inited_));
  } else if (OB_FAIL(logminer_record_alloc_.init(OB_MALLOC_NORMAL_BLOCK_SIZE, "MinerRecAlloc",
      OB_SERVER_TENANT_ID, LOGMINER_RECORD_LIMIT))) {
    LOG_ERROR("allocator for logminer record failed to init");
  } else if (OB_FAIL(logminer_batch_record_alloc_.init(OB_MALLOC_NORMAL_BLOCK_SIZE, "BatchRecAlloc",
      OB_SERVER_TENANT_ID, LOGMINER_RECORD_LIMIT))) {
    LOG_ERROR("allocator for logminer batch record failed to init");
  } else if (OB_FAIL(lib::ThreadPool::init())) {
    LOG_ERROR("ThreadPool for data manager failed to init");
  } else {
    stat_.reset();
    mode_ = mode;
    format_ = format;
    start_progress_ = start_progress;
    end_progress_ = end_progress;
    err_handle_ = err_handle;
    is_inited_ = true;
    LOG_INFO("ObLogMinerDataManager finished to init", K(mode), K(format), K(start_progress),
        K(end_progress));
    LOGMINER_STDOUT_V("ObLogMinerDataManager finished to init\n");
  }
  return ret;
}

int ObLogMinerDataManager::start()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("data manager hasn't been initialized", K(is_inited_));
  } else if (OB_FAIL(lib::ThreadPool::start())) {
    LOG_ERROR("thread pool for data manager failed to start");
  } else {
    LOG_INFO("ObLogMinerDataManager starts");
  }

  return ret;
}

void ObLogMinerDataManager::stop()
{
  lib::ThreadPool::stop();
  LOG_INFO("ObLogMinerDataManager stopped");
}

void ObLogMinerDataManager::wait()
{
  lib::ThreadPool::wait();
  LOG_INFO("ObLogMinerDataManager finished to wait");
}

void ObLogMinerDataManager::destroy()
{
  if (IS_INIT) {
    is_inited_ = false;
    stat_.reset();
    mode_ = LogMinerMode::UNKNOWN;
    format_ = RecordFileFormat::INVALID;
    start_progress_ = OB_INVALID_TIMESTAMP;
    end_progress_ = OB_INVALID_TIMESTAMP;
    lib::ThreadPool::destroy();
    logminer_batch_record_alloc_.destroy();
    logminer_record_alloc_.destroy();
    err_handle_ = nullptr;
    LOG_INFO("ObLogMinerDataManager destroyed");
    LOGMINER_STDOUT_V("ObLogMinerDataManager destroyed\n");
  }
}

int ObLogMinerDataManager::get_logminer_record(ObLogMinerRecord *&logminer_rec)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("get logminer record faild, data manager is not inited", K(is_inited_));
  } else {
    if (OB_ISNULL(logminer_rec = op_alloc_args(ObLogMinerRecord, &logminer_record_alloc_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("logminer record pool alloc record failed", K(logminer_rec));
    } else {
      ATOMIC_INC(&stat_.record_alloc_count_);
    }
  }
  return ret;
}

int ObLogMinerDataManager::release_log_miner_record(ObLogMinerRecord *logminer_rec)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("release logminer record faield, data manager is not inited", K(is_inited_));
  } else {
    logminer_rec->destroy();
    op_free(logminer_rec);
    ATOMIC_INC(&stat_.record_release_count_);
  }
  return ret;
}

int ObLogMinerDataManager::get_logminer_batch_record(ObLogMinerBatchRecord *&logminer_batch_rec)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("get logminer record faield, data manager is not inited", K(is_inited_));
  } else {
    ILogMinerRecordConverter *converter = ILogMinerRecordConverter::get_converter_instance(format_);
    if (OB_ISNULL(converter)) {
      ret = OB_NOT_SUPPORTED;
      LOG_ERROR("logminer doesn't support the output format", K(format_));
    } else if (OB_ISNULL(logminer_batch_rec = op_alloc_args(ObLogMinerBatchRecord,
        &logminer_batch_record_alloc_, converter))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("logminer record pool alloc record failed", K(logminer_batch_rec));
    } else {
      ATOMIC_INC(&stat_.batch_record_alloc_count_);
    }
  }
  return ret;
}

int ObLogMinerDataManager::release_logminer_batch_record(ObLogMinerBatchRecord *logminer_batch_rec)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("get logminer record faield, data manager is not inited", K(is_inited_));
  } else if (OB_ISNULL(logminer_batch_rec)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("cannot release a null logminer batch rec", K(logminer_batch_rec));
  } else {
    logminer_batch_rec->reset();
    op_free(logminer_batch_rec);
    ATOMIC_INC(&stat_.batch_record_release_count_);
  }
  return ret;
}

int ObLogMinerDataManager::update_output_progress(const int64_t timestamp)
{
  int ret = OB_SUCCESS;
  if (is_analysis_mode(mode_)) {
    if (timestamp >= output_progress_) {
      output_progress_ = timestamp;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("output progress rollbacked", K(timestamp), K(output_progress_), K(mode_));
    }
  } else if (is_flashback_mode(mode_)) {
    if (timestamp <= output_progress_) {
      output_progress_ = timestamp;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("output progress rollbacked", K(timestamp), K(output_progress_), K(mode_));
    }
  }

  if (OB_SUCC(ret)) {
    if (reach_end_progress(timestamp)) {
      if (OB_FAIL(LOGMINER_LOGGER.log_progress(record_count_, output_progress_, start_progress_, end_progress_))) {
        LOG_WARN("log progress failed", K(record_count_), K(output_progress_), K(start_progress_), K(end_progress_));
      }
      LOGMINER_STDOUT("\nObLogMinerAnalyzer completed process\n");
      err_handle_->handle_error(ret, "output progress reach end_progress");
    }
  }

  return ret;
}

int ObLogMinerDataManager::increase_record_count(int64_t count) {
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("data manager is not inited", K(is_inited_));
  } else if (count < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("increase negative count", K(count));
  } else {
    record_count_ += count;
  }
  return ret;
}

bool ObLogMinerDataManager::reach_end_progress(const int64_t progress) const
{
  bool bret = false;
  if (OB_INVALID_TIMESTAMP == progress) {
    bret = false;
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "get invalid progress when judging reach end progress",
        K(progress), K(end_progress_));
  } else if (is_analysis_mode(mode_)) {
    bret = progress > end_progress_;
  } else if (is_flashback_mode(mode_)) {
    bret = progress < end_progress_;
  }
  return bret;
}

void ObLogMinerDataManager::run1()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("data manager not init");
  } else {
    const int64_t STAT_INTERVAL = 1L * 1000 * 1000; // 1s
    const int64_t LOOP_INTERVAL = 100L * 1000; // 100 ms
    while (OB_SUCC(ret) && !has_set_stop()) {
      if (REACH_TIME_INTERVAL(STAT_INTERVAL)) {
        do_statistics_();
      }
      ob_usleep(LOOP_INTERVAL);
    }
  }
}

void ObLogMinerDataManager::do_statistics_()
{
  static LogMinerDataManagerStat last_stat;
  int ret = OB_SUCCESS;
  LogMinerDataManagerStat cur_stat = stat_;
  LogMinerDataManagerStat delta;
  delta.record_alloc_count_ = cur_stat.record_alloc_count_ - last_stat.record_alloc_count_;
  delta.record_release_count_ = cur_stat.record_release_count_ - last_stat.record_release_count_;
  delta.batch_record_alloc_count_ = cur_stat.batch_record_alloc_count_ - last_stat.batch_record_alloc_count_;
  delta.batch_record_alloc_count_ = cur_stat.batch_record_alloc_count_ - last_stat.batch_record_alloc_count_;
  LOG_INFO("[DATA_MANAGER] [STAT] [RECORD_COUNT]", K(cur_stat), K(last_stat), K(delta),
      "active_record_cnt", cur_stat.record_alloc_count_ - cur_stat.record_release_count_,
      "active_batch_record_cnt", cur_stat.batch_record_alloc_count_ - cur_stat.batch_record_release_count_);
  LOG_INFO("[DATA_MANAGER] [STAT] [OUTPUT_PROGRESS]", K(output_progress_), K(start_progress_), K(end_progress_),
      K(record_count_));
  // only print intermediate progress here.
  if (output_progress_ != OB_INVALID_TIMESTAMP && !reach_end_progress(output_progress_)) {
    if (OB_FAIL(LOGMINER_LOGGER.log_progress(record_count_, output_progress_, start_progress_, end_progress_))) {
        LOG_WARN("log progress failed", K(record_count_), K(output_progress_), K(start_progress_), K(end_progress_));
      }
  }
  last_stat = cur_stat;
}

}
}