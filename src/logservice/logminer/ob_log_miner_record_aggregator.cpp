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

#include "ob_log_miner_record_aggregator.h"
#include "ob_log_miner_batch_record_writer.h"
#include "ob_log_miner_batch_record.h"
#include "ob_log_miner_record.h"
#include "ob_log_miner_data_manager.h"
#include "ob_log_miner_resource_collector.h"

namespace oceanbase
{
namespace oblogminer
{

const int64_t ObLogMinerRecordAggregator::RECORD_AGG_THREAD_NUM = 1L;
const int64_t ObLogMinerRecordAggregator::RECORD_AGG_QUEUE_SIZE = 100000L;
const int64_t ObLogMinerRecordAggregator::RECORD_FLUSH_THRESHOLD = 1L * 1000 * 1000; // 1 sec

ObLogMinerRecordAggregator::ObLogMinerRecordAggregator():
  is_inited_(false),
  cur_batch_record_(nullptr),
  push_record_count_(0),
  aggregated_record_count_(0),
  last_trans_end_ts_(OB_INVALID_TIMESTAMP),
  writer_(nullptr),
  data_manager_(nullptr),
  resource_collector_(nullptr),
  err_handle_(nullptr) { }

ObLogMinerRecordAggregator::~ObLogMinerRecordAggregator()
{
  destroy();
}

int ObLogMinerRecordAggregator::init(const int64_t start_time_us,
    ILogMinerBatchRecordWriter *writer,
    ILogMinerDataManager *data_manager,
    ILogMinerResourceCollector *resource_collector,
    ILogMinerErrorHandler *err_handle)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("ObLogMinerRecord Aggregator has been initialized", K(is_inited_));
  } else if (OB_ISNULL(writer) || OB_ISNULL(data_manager) || OB_ISNULL(resource_collector)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("ObLogMinerRecord Aggregator get invalid argument", K(writer), K(data_manager));
  } else if (OB_FAIL(RecordAggThreadPool::init(RECORD_AGG_THREAD_NUM, RECORD_AGG_QUEUE_SIZE))) {
    LOG_ERROR("RecordAggThreadPool failed to init");
  } else {
    writer_ = writer;
    data_manager_ = data_manager;
    resource_collector_ = resource_collector;
    err_handle_ = err_handle;
    push_record_count_ = 0;
    aggregated_record_count_ = 0;
    last_trans_end_ts_ = start_time_us;
    is_inited_ = true;
    LOG_INFO("ObLogMinerRecordAggregator finished to init", K(writer_), K(data_manager_));
  }

  return ret;
}

int ObLogMinerRecordAggregator::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogMinerRecord Aggregator hasn't been initialized", K(is_inited_));
  } else if (OB_FAIL(RecordAggThreadPool::start())) {
    LOG_ERROR("RecordAggThreadPool failed to start");
  } else {
    LOG_INFO("ObLogMinerRecordAggregator starts");
  }
  return ret;
}

void ObLogMinerRecordAggregator::stop()
{
  RecordAggThreadPool::mark_stop_flag();
  LOG_INFO("ObLogMinerRecordAggregator stopped");
}

void ObLogMinerRecordAggregator::wait()
{
  RecordAggThreadPool::stop();
  LOG_INFO("ObLogMinerRecordAggregator wait end");
}

void ObLogMinerRecordAggregator::destroy()
{
  if (IS_INIT) {
    if (nullptr != cur_batch_record_ && nullptr != data_manager_) {
      data_manager_->release_logminer_batch_record(cur_batch_record_);
    }
    RecordAggThreadPool::destroy();
    cur_batch_record_ = nullptr;
    writer_ = nullptr;
    data_manager_ = nullptr;
    resource_collector_ = nullptr;
    err_handle_ = nullptr;
    is_inited_ = false;
  }
}

int ObLogMinerRecordAggregator::push(ObLogMinerRecord *record)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("aggregator has not been initialized", K(is_inited_));
  } else if (OB_ISNULL(record)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("get invalid record in aggregator", K(record));
  } else {
    LOG_TRACE("ObLogMinerRecordAggregator push record into queue", KPC(record));
    const int64_t DATA_OP_TIMEOUT = 1L * 1000 * 1000;
    int64_t hash_val = 0;
    RETRY_FUNC(is_stoped(), *(static_cast<ObMQThread*>(this)), push, record, hash_val, DATA_OP_TIMEOUT);
    if (OB_FAIL(ret)) {
      LOG_ERROR("push record into MQThread queue failed", KPC(record), "is_stopped", is_stoped());
    } else {
      ATOMIC_INC(&push_record_count_);
    }
  }
  return ret;
}

int ObLogMinerRecordAggregator::handle(void *data, const int64_t thread_index, volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("aggregator has not been initialized", K(is_inited_));
  } else if (OB_ISNULL(data)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("get invalid record in aggregator", K(data));
  } else {
    ObLogMinerRecord *record = static_cast<ObLogMinerRecord*>(data);
    bool need_flush = false;
    LOG_TRACE("aggregator process record", KPC(record));

    if (is_trans_end_record_type(record->get_record_type())) {
      last_trans_end_ts_ = record->get_commit_scn().convert_to_ts();
    }

    if (nullptr == cur_batch_record_) {
      if (OB_FAIL(data_manager_->get_logminer_batch_record(cur_batch_record_))) {
        LOG_ERROR("get batch record from data manager failed", KPC(cur_batch_record_));
      } else if (OB_ISNULL(cur_batch_record_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("get null batch record from data_manager", KPC(cur_batch_record_));
      } else if (OB_FAIL(cur_batch_record_->init_last_trans_end_ts(last_trans_end_ts_))) {
        LOG_ERROR("failed to init last trans end ts", KPC(cur_batch_record_));
      }
    }

    if (OB_FAIL(ret)) {
      LOG_ERROR("failed to get cur_batch_record");
    } else if (OB_FAIL(cur_batch_record_->append_record(*record))) {
      LOG_ERROR("append record to cur_batch_record failed", KPC(record), KPC(cur_batch_record_));
    } else if (OB_FAIL(check_need_flush_(need_flush))) {
      LOG_ERROR("check need flush failed", KPC(cur_batch_record_));
    } else if (OB_FAIL(resource_collector_->revert(record))) {
      LOG_ERROR("resource collector failed to revert record", KP(record));
    } else {
      LOG_TRACE("batch_record need flush", K(need_flush));
      if (need_flush) {
        ATOMIC_AAF(&aggregated_record_count_, cur_batch_record_->get_total_record_count());
        if (OB_FAIL(writer_->push(cur_batch_record_))) {
          LOG_ERROR("push cur_batch_record into writer failed", KPC(cur_batch_record_));
        } else {
          cur_batch_record_ = nullptr;
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    err_handle_->handle_error(ret, "ObLogMinerRecordAggregator exit unexpected\n");
  }

  return ret;
}

int ObLogMinerRecordAggregator::get_total_record_count(int64_t &record_count)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("aggregator has not been initialized", K(is_inited_));
  } else {
    record_count = ATOMIC_LOAD(&push_record_count_) - ATOMIC_LOAD(&aggregated_record_count_);
  }

  return ret;
}

int ObLogMinerRecordAggregator::check_need_flush_(bool &need_flush)
{
  int ret = OB_SUCCESS;
  const char *reason = "INVALID";
  int64_t first_access_ts = OB_INVALID_TIMESTAMP;
  need_flush = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("aggregator has not been initialized", K(is_inited_));
  } else if (OB_ISNULL(cur_batch_record_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("cur_batch_record_ is null when check_need_flush", K(cur_batch_record_));
  } else if (cur_batch_record_->is_freezed()) {
    need_flush = true;
    reason = "Freezed";
  } else if (! cur_batch_record_->get_progress_range().is_valid()) {
    need_flush = false;
    reason = "Invalid Progress Range";
  } else if (data_manager_->reach_end_progress(cur_batch_record_->get_last_trans_end_ts())) {
    need_flush = true;
    reason = "Reach End Progress";
    LOG_INFO("cur_bacth_record reached end progress", KPC(cur_batch_record_), K(need_flush));
  } else {
    first_access_ts = cur_batch_record_->get_first_access_ts();
    if (OB_INVALID_TIMESTAMP == first_access_ts) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("cur_batch_record hasn't been accessed when check_need_flush", KPC(cur_batch_record_));
    } else if (ObTimeUtility::current_time() - first_access_ts >= RECORD_FLUSH_THRESHOLD) {
      need_flush = true;
      reason = "Time Threshold";
    } else {
      reason = "Wait Flush";
    }
  }

  LOG_TRACE("check_need_flush end", KR(ret), K(need_flush), K(reason), K(first_access_ts));

  return ret;
}

}
}