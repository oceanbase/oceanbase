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
 *
 * Reader: read data from storage.
 */

#define USING_LOG_PREFIX OBLOG

#include "lib/string/ob_string.h"                // ObString
#include "ob_log_reader.h"
#include "ob_log_instance.h"                     // TCTX
#include "ob_log_dml_parser.h"                   // IObLogDmlParser
#include "ob_log_store_service.h"                // IObStoreService
#include "ob_log_utils.h"                        // get_timestamp
#include "ob_log_factory.h"                      // ReadLogBuf, ReadLogBufFactory
#include "ob_log_trace_id.h"                     // ObLogTraceIdGuard
#include "ob_cdc_auto_config_mgr.h"              // CDC_CFG_MGR

using namespace oceanbase::common;

namespace oceanbase
{
namespace libobcdc
{

ObLogReader::ObLogReader() :
    inited_(false),
    working_mode_(WorkingMode::UNKNOWN_MODE),
    round_value_(0),
    rps_stat_(),
    last_stat_time_(0),
    log_entry_task_count_(0),
    store_service_stat_(),
    store_service_(NULL),
    err_handler_(NULL)
{
}

ObLogReader::~ObLogReader()
{
  destroy();
}

int ObLogReader::init(const int64_t thread_num,
    const int64_t queue_size,
    const WorkingMode working_mode,
    IObStoreService &store_service,
    IObLogErrHandler &err_handler)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("ObLogReader has been initialized", KR(ret));
  } else if (OB_UNLIKELY(thread_num <= 0)
      || OB_UNLIKELY(queue_size <= 0)
      || OB_UNLIKELY(! is_working_mode_valid(working_mode))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid arguments", KR(ret), K(thread_num), K(queue_size), K(working_mode));
  } else if (OB_FAIL(ReaderThread::init(thread_num, queue_size))) {
    LOG_ERROR("init ReaderThread queue thread fail", K(ret), K(thread_num), K(queue_size));
  } else {
    working_mode_ = working_mode;
    round_value_ = 0;
    rps_stat_.reset();
    last_stat_time_ = get_timestamp();
    log_entry_task_count_ = 0;
    store_service_ = &store_service;
    err_handler_ = &err_handler;
    inited_ = true;

    LOG_INFO("ObLogReader init succ", K(working_mode_), "working_mode", print_working_mode(working_mode_),
        K(thread_num), K(queue_size));
  }

  return ret;
}

void ObLogReader::destroy()
{
  if (inited_) {
    LOG_INFO("ObLogReader destroy begin");
    ReaderThread::destroy();

    inited_ = false;
    round_value_ = 0;
    rps_stat_.reset();
    last_stat_time_ = 0;
    log_entry_task_count_ = 0;

    store_service_stat_.reset();
    store_service_ = NULL;
    err_handler_ = NULL;
    LOG_INFO("ObLogReader destroy end");
  }
}

int ObLogReader::start()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogReader has not been initialized", KR(ret));
  } else if (OB_FAIL(ReaderThread::start())) {
    LOG_ERROR("start ReaderThread thread fail", K(ret), "thread_num", get_thread_num());
  } else {
    LOG_INFO("start ReaderThread threads succ", "thread_num", get_thread_num());
  }

  return ret;
}

void ObLogReader::stop()
{
  if (inited_) {
    ReaderThread::stop();
    LOG_INFO("stop ReaderThread threads succ", "thread_num", get_thread_num());
  }
}

int ObLogReader::push(ObLogEntryTask &task, const int64_t timeout)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogReader has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(! task.is_valid())) {
    LOG_ERROR("invalid arguments", K(task));
    ret = OB_INVALID_ARGUMENT;
  } else {
    uint64_t hash_value = ATOMIC_FAA(&round_value_, 1);
    const DmlRedoLogNode *redo_log_node = NULL;

    if (OB_ISNULL(redo_log_node = task.get_redo_log_node())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("redo_log_node is NULL", KR(ret), K(redo_log_node), K(task));
    } else {
      if (redo_log_node->is_direct_load_inc_log() && CDC_CFG_MGR.get_direct_load_inc_thread_num() < get_thread_num()) {
        hash_value = hash_value % CDC_CFG_MGR.get_direct_load_inc_thread_num();
      }
      void *push_task = static_cast<void *>(&task);
      if (OB_FAIL(ReaderThread::push(push_task, hash_value, timeout))) {
        if (OB_TIMEOUT != ret && OB_IN_STOP_STATE != ret) {
          LOG_ERROR("push task into ReaderThread fail", K(ret), K(push_task), K(hash_value));
        }
      } else {
        ATOMIC_INC(&log_entry_task_count_);
      }
    }
  }

  return ret;
}

void ObLogReader::print_stat_info()
{
  int64_t current_timestamp = get_timestamp();
  int64_t local_last_stat_time = last_stat_time_;
  int64_t delta_time = current_timestamp - local_last_stat_time;
  // Update last statistic value
  last_stat_time_ = current_timestamp;

  double storager_rps = rps_stat_.calc_rps(delta_time);
  double read_rate = store_service_stat_.calc_rate(delta_time);
  double read_total_size = store_service_stat_.get_total_data_size();
  _LOG_INFO("[READER] [STAT] RPS=%.3lf READ_RATE=%.5fM/s READ_TOTAL_SIZE=%.5fG",
      storager_rps, read_rate, read_total_size);
}

int ObLogReader::handle(void *data, const int64_t thread_index, volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  ObLogTraceIdGuard trace_guard;
  ObLogEntryTask *task = static_cast<ObLogEntryTask *>(data);

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogReader has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(task) || OB_UNLIKELY(! task->is_valid())) {
    LOG_ERROR("invalid arguments", KPC(task));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(handle_task_(*task, thread_index, stop_flag))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("handle_task_ fail", KR(ret), KPC(task), K(thread_index));
    }
  } else {
    LOG_DEBUG("ObLogEntryTask read succ", KP(task));
    ATOMIC_DEC(&log_entry_task_count_);
  }

  if (stop_flag) {
    ret = OB_IN_STOP_STATE;
  }

  // exit on fail
  if (OB_SUCCESS != ret && OB_IN_STOP_STATE != ret && NULL != err_handler_) {
    err_handler_->handle_error(ret, "ReaderThread thread exits, thread_index=%ld, err=%d",
        thread_index, ret);
    stop_flag = true;
  }

  return ret;
}

int ObLogReader::handle_task_(ObLogEntryTask &log_entry_task,
    const int64_t thread_index,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  ObLogTenantGuard guard;
  ObLogTenant *tenant = NULL;
  const uint64_t tenant_id = log_entry_task.get_tenant_id();
  void *column_family_handle = NULL;
  std::string key;
  std::string value;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogReader has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(TCTX.get_tenant_guard(tenant_id, guard))) {
    LOG_ERROR("get_tenant_guard fail", KR(ret));
  } else {
    tenant = guard.get_tenant();
    column_family_handle = tenant->get_redo_storage_cf_handle();
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(log_entry_task.get_storage_key(key))) {
    LOG_ERROR("get_storage_key fail", KR(ret), "key", key.c_str(), K(log_entry_task));
  } else if (stop_flag) {
    ret = OB_IN_STOP_STATE;
  } else if (OB_FAIL(read_store_service_(log_entry_task, column_family_handle, key, value))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("read_store_service_ fail", KR(ret), K(log_entry_task));
    }
  } else {
    store_service_stat_.do_data_stat(value.length());
  }

  if (OB_SUCC(ret)) {
    rps_stat_.do_rps_stat(1);

    if (OB_FAIL(push_task_into_dml_parser_(log_entry_task, thread_index, stop_flag))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("push_task_into_dml_parser_ fail", KR(ret), K(thread_index));
      }
    } else {}
  }

  if (stop_flag) {
    ret = OB_IN_STOP_STATE;
  }

  return ret;
}

int ObLogReader::read_store_service_(ObLogEntryTask &log_entry_task,
    void *column_family_handle,
    std::string &key,
    std::string &value)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogReader has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(store_service_)) {
    LOG_ERROR("store_service_ is NULL");
    ret = OB_ERR_UNEXPECTED;
  } else if (ReaderThread::is_stoped()) {
    ret = OB_IN_STOP_STATE;
  } else if (OB_FAIL(store_service_->get(column_family_handle, key, value))) {
    LOG_ERROR("StoreService get fail", KR(ret), K(key.c_str()), "value_len", value.length(), K(log_entry_task));
  } else {
    const char *value_str = value.c_str();
    const int64_t value_len = value.length();
    const bool is_big_row = (value_len >= OB_MAX_LOG_BUFFER_SIZE);
    char *data_buf = NULL;
    int64_t data_len = 0;

    if (is_big_row) {
      data_buf = static_cast<char *>(ob_malloc(value_len + 1, "CDCRdBigRow"));

      if (OB_ISNULL(data_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("data_buf allocate failed", KR(ret));
      } else {
        MEMCPY(data_buf, value_str, value_len);
        data_buf[value_len] = '\0';
        data_len = value_len;
      }
    } else {
      if (OB_ISNULL(data_buf = static_cast<char *>(log_entry_task.alloc(value_len + 1)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("ReadLogBufFactory alloc fail", KR(ret));
      } else {
        MEMCPY(data_buf, value_str, value_len);
        data_buf[value_len] = '\0';
        data_len = value_len;
        LOG_DEBUG("StoreService get succ", K(key.c_str()), K(value_len), K(log_entry_task));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(log_entry_task.set_data_and_readed_status(is_big_row, data_buf, data_len))) {
        LOG_ERROR("log_entry_task set_data_and_readed_status failed", KR(ret), K(log_entry_task));
      } else {
        // succ
      }
    }

    if (OB_FAIL(ret)) {
      if (is_big_row) {
        if (NULL != data_buf) {
          ob_free(data_buf);
          data_buf = NULL;
        }
      }

    }
  }

  return ret;
}

int ObLogReader::push_task_into_dml_parser_(ObLogEntryTask &task,
    const int64_t thread_index,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  IObLogDmlParser *dml_parser = TCTX.dml_parser_;
  UNUSED(thread_index);

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogReader has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(dml_parser)) {
    LOG_ERROR("dml_parser is NULL");
    ret = OB_ERR_UNEXPECTED;
  } else {
    RETRY_FUNC(stop_flag, *dml_parser, push, task, PUSH_PARSER_TIMEOUT);
  }

  return ret;
}

void ObLogReader::print_task_count_()
{
  int ret = OB_SUCCESS;
  int64_t total_thread_num = get_thread_num();

  for (int64_t idx = 0; OB_SUCC(ret) && idx < total_thread_num; ++idx) {
    int64_t task_count = 0;
    if (OB_FAIL(get_task_num(idx, task_count))) {
      LOG_ERROR("get_task_num fail", K(ret));
    } else {
      _LOG_INFO("[INC_STAT] [READER] [%ld/%ld] TASK_COUNT=%ld", idx, total_thread_num, task_count);
    }
  }
}

} // namespace libobcdc
} // namespace oceanbase
