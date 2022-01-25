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

#define USING_LOG_PREFIX OBLOG

#include "lib/string/ob_string.h"                // ObString
#include "ob_log_data_processor.h"
#include "ob_log_binlog_record.h"
#include "ob_log_trans_ctx.h"
#include "ob_log_committer.h"
#include "ob_log_instance.h"
#include "ob_log_store_service.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace liboblog
{

ObLogDataProcessor::ObLogDataProcessor() :
    inited_(false),
    working_mode_(WorkingMode::UNKNOWN_MODE),
    round_value_(0),
    rps_stat_(),
    last_stat_time_(0),
    row_task_count_(0),
    reader_(),
    err_handler_(NULL)
{
}

ObLogDataProcessor::~ObLogDataProcessor()
{
  destroy();
}

int ObLogDataProcessor::init(const int64_t thread_num,
    const int64_t queue_size,
    const WorkingMode working_mode,
    IObStoreService &store_service,
    IObLogErrHandler &err_handler)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("ObLogDataProcessor has been initialized");
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(thread_num <= 0)
      || OB_UNLIKELY(queue_size <= 0)
      || OB_UNLIKELY(! is_working_mode_valid(working_mode))) {
    LOG_ERROR("invalid arguments", K(thread_num), K(queue_size), K(working_mode));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(DataProcessorThread::init(thread_num, queue_size))) {
    LOG_ERROR("init DataProcessorThread queue thread fail", K(ret), K(thread_num), K(queue_size));
  } else if (OB_FAIL(reader_.init(store_service))) {
    LOG_ERROR("reader_ init fail", KR(ret));
  } else {
    working_mode_ = working_mode;
    round_value_ = 0;
    rps_stat_.reset();
    last_stat_time_ = get_timestamp();
    row_task_count_ = 0;
    err_handler_ = &err_handler;
    inited_ = true;

    LOG_INFO("DataProcessor init succ", K(working_mode_), "working_mode", print_working_mode(working_mode_),
        K(thread_num), K(queue_size));
  }

  return ret;
}

void ObLogDataProcessor::destroy()
{
  if (inited_) {
    DataProcessorThread::destroy();

    inited_ = false;
    round_value_ = 0;
    rps_stat_.reset();
    last_stat_time_ = 0;
    row_task_count_ = 0;
    reader_.destroy();
    err_handler_ = NULL;
  }
}

int ObLogDataProcessor::start()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogDataProcessor has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(DataProcessorThread::start())) {
    LOG_ERROR("start DataProcessorThread thread fail", K(ret), "thread_num", get_thread_num());
  } else {
    LOG_INFO("start DataProcessorThread threads succ", "thread_num", get_thread_num());
  }

  return ret;
}

void ObLogDataProcessor::stop()
{
  if (inited_) {
    DataProcessorThread::stop();
    LOG_INFO("stop DataProcessorThread threads succ", "thread_num", get_thread_num());
  }
}

int ObLogDataProcessor::push(ObLogRowDataIndex &task, const int64_t timeout)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogDataProcessor has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(! task.is_valid())) {
    LOG_ERROR("invalid arguments", K(task));
    ret = OB_INVALID_ARGUMENT;
  } else {
    uint64_t hash_value = ATOMIC_FAA(&round_value_, 1);
    void *push_task = static_cast<void *>(&task);
    if (OB_FAIL(DataProcessorThread::push(push_task, hash_value, timeout))) {
      if (OB_TIMEOUT != ret && OB_IN_STOP_STATE != ret) {
        LOG_ERROR("push task into DataProcessorThread fail", K(ret), K(push_task), K(hash_value));
      }
    } else {
      ATOMIC_INC(&row_task_count_);
    }
  }

  return ret;
}

void ObLogDataProcessor::print_stat_info()
{
  int64_t current_timestamp = get_timestamp();
  int64_t local_last_stat_time = last_stat_time_;
  int64_t delta_time = current_timestamp - local_last_stat_time;
  // Update last statistic value
  last_stat_time_ = current_timestamp;
  StoreServiceStatInfo &store_service_stat = reader_.get_store_stat_info();

  double storager_rps = rps_stat_.calc_rps(delta_time);
  double read_rate = store_service_stat.calc_rate(delta_time);
  double read_total_size = store_service_stat.get_total_data_size();
  _LOG_INFO("[DATA_PROCESSOR] [STAT] RPS=%.3lf READ_RATE=%.5fM/s READ_TOTAL_SIZE=%.5fG",
      storager_rps, read_rate, read_total_size);
}

int ObLogDataProcessor::handle(void *data, const int64_t thread_index, volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  ObLogRowDataIndex *task = static_cast<ObLogRowDataIndex *>(data);

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogDataProcessor has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(task) || OB_UNLIKELY(! task->is_valid())) {
    LOG_ERROR("invalid arguments", KPC(task));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(handle_task_(*task, thread_index, stop_flag))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("handle_task_ fail", KR(ret), KPC(task), K(thread_index));
    }
  } else {
    ATOMIC_DEC(&row_task_count_);
  }

  if (stop_flag) {
    ret = OB_IN_STOP_STATE;
  }

  // exit on fail
  if (OB_SUCCESS != ret && OB_IN_STOP_STATE != ret && NULL != err_handler_) {
    err_handler_->handle_error(ret, "DataProcessorThread thread exits, thread_index=%ld, err=%d",
        thread_index, ret);
    stop_flag = true;
  }

  return ret;
}

int ObLogDataProcessor::handle_task_(ObLogRowDataIndex &row_data_index,
    const int64_t thread_index,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = row_data_index.get_tenant_id();
  PartTransTask *part_trans_task = NULL;
  ObLogBR *br = NULL;
  int record_type = 0;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogDataProcessor has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(part_trans_task = static_cast<PartTransTask*>(row_data_index.get_host()))) {
    LOG_ERROR("part_trans_task is NULL", K(row_data_index));
    ret = OB_ERR_UNEXPECTED;
  } else if (is_memory_working_mode(working_mode_)) {
    // do nothing
  } else if (is_storage_working_mode(working_mode_)) {
    if (OB_FAIL(reader_.read(row_data_index))) {
      LOG_ERROR("reader_ read fail", KR(ret), K(row_data_index));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(br = row_data_index.get_binlog_record())) {
      LOG_ERROR("br is NULL", K(row_data_index), K(part_trans_task));
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(br->get_record_type(record_type))) {
      LOG_ERROR("br get_record_type fail", KR(ret),
          "record_type", print_record_type(record_type), K(row_data_index));
    } else {}
  }

  if (OB_SUCC(ret)) {
    const uint64_t cluster_id = part_trans_task->get_cluster_id();
    const ObString &trace_id = part_trans_task->get_trace_id();
    const ObString &trace_info = part_trans_task->get_trace_info();
    ObString dml_unique_id;
    const common::ObVersion &freeze_version = part_trans_task->get_freeze_version();
    const int64_t commit_version = part_trans_task->get_global_trans_version();

    rps_stat_.do_rps_stat(1);

    if (OB_FAIL(init_dml_unique_id_(row_data_index, *part_trans_task, dml_unique_id))) {
      LOG_ERROR("init_dml_unique_id_ fail", KR(ret), K(row_data_index), KPC(part_trans_task), K(dml_unique_id));
    } else if (OB_FAIL(br->init_dml_data_second(static_cast<RecordType>(record_type), cluster_id, tenant_id,
            trace_id, trace_info, dml_unique_id, freeze_version, commit_version))) {
      LOG_ERROR("ObLogBR init_dml_data_second fail", KR(ret), K(record_type), K(cluster_id), K(tenant_id), K(trace_id), K(trace_info),
          K(dml_unique_id), K(freeze_version), K(commit_version));
    } else if (OB_FAIL(push_task_into_trx_queue_(*br, thread_index, stop_flag))) {
      LOG_ERROR("push_task_into_trx_queue_ fail", KR(ret), K(row_data_index));
    } else {}
  }

  if (stop_flag) {
    ret = OB_IN_STOP_STATE;
  }

  return ret;
}

int ObLogDataProcessor::init_dml_unique_id_(ObLogRowDataIndex &row_data_index,
    PartTransTask &part_trans_task,
    common::ObString &dml_unique_id)
{
  int ret = OB_SUCCESS;
  const ObString &pkey_and_log_id_str = part_trans_task.get_pkey_and_log_id_str();
  const int32_t log_offset = row_data_index.get_log_offset();
  const uint64_t row_no = row_data_index.get_row_no();
  DmlStmtUniqueID dml_stmt_unique_id(pkey_and_log_id_str, log_offset, row_no);

  if (OB_UNLIKELY(! dml_stmt_unique_id.is_valid())) {
    LOG_ERROR("dml_stmt_unique_id is not valid", K(dml_stmt_unique_id));
    ret = OB_INVALID_ARGUMENT;
  } else {
    // TODO allocator
    common::ObIAllocator &allocator= part_trans_task.get_allocator();
    const int64_t buf_len = dml_stmt_unique_id.get_dml_unique_id_length();
    char *buf = static_cast<char*>(allocator.alloc(buf_len));
    int64_t pos = 0;

    if (OB_ISNULL(buf)) {
      LOG_ERROR("allocate memory for trans id buffer fail", K(buf));
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_FAIL(dml_stmt_unique_id.customized_to_string(buf, buf_len, pos))) {
      LOG_ERROR("dml_stmt_unique_id customized_to_string fail", KR(ret), K(buf), K(buf_len), K(pos));
    } else {
      dml_unique_id.assign_ptr(buf, static_cast<int32_t>(pos));
    }
  }

  return ret;
}

int ObLogDataProcessor::push_task_into_trx_queue_(ObLogBR &br,
    const int64_t thread_index,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  IObLogCommitter *trans_committer = TCTX.committer_;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogDataProcessor has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(trans_committer)) {
    LOG_ERROR("trans_committer is NULL");
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(trans_committer->push_br_task(br))) {
    LOG_ERROR("push_br_task fail", KR(ret), K(thread_index), K(br), K(stop_flag));
  } else {
    // succ
  }

  return ret;
}

void ObLogDataProcessor::print_task_count_()
{
  int ret = OB_SUCCESS;
  int64_t total_thread_num = get_thread_num();

  for (int64_t idx = 0; OB_SUCC(ret) && idx < total_thread_num; ++idx) {
    int64_t task_count = 0;
    if (OB_FAIL(get_task_num(idx, task_count))) {
      LOG_ERROR("get_task_num fail", K(ret));
    } else {
      _LOG_INFO("[INC_STAT] [DATA_PROCESSOR] [%ld/%ld] TASK_COUNT=%ld", idx, total_thread_num, task_count);
    }
  }
}

} // namespace liboblog
} // namespace oceanbase
