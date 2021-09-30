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
#include <LogMsgBuf.h>                       // LogMsgInit, LogMsgDestroy
#include <MetaInfo.h>                            // ITableMeta
#include "ob_log_storager.h"
#include "ob_log_row_data_index.h"               // ObLogRowDataIndex
#include "ob_log_instance.h"
#include "ob_log_store_service.h"

using namespace oceanbase::common;
using namespace oceanbase::logmessage;
namespace oceanbase
{
namespace liboblog
{

ObLogStorager::ObLogStorager() :
    inited_(false),
    round_value_(0),
    store_buf_array_(),
    rps_stat_(),
    last_stat_time_(0),
    log_entry_task_count_(0),
    store_service_stat_(),
    store_service_(NULL),
    err_handler_(NULL)
{
}

ObLogStorager::~ObLogStorager()
{
  destroy();
}

int ObLogStorager::init(const int64_t thread_num,
    const int64_t queue_size,
    IObStoreService &store_service,
    IObLogErrHandler &err_handler)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("ObLogStorager has been initialized");
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(thread_num <= 0)
      || OB_UNLIKELY(queue_size <= 0)) {
    LOG_ERROR("invalid arguments", K(thread_num), K(queue_size));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(StoragerThread::init(thread_num, queue_size))) {
    LOG_ERROR("init storager queue thread fail", K(ret), K(thread_num), K(queue_size));
  } else if (OB_FAIL(init_store_buf_array_(thread_num))) {
    LOG_ERROR("init_store_buf_array_ fail", KR(ret), K(thread_num));
  } else {
    round_value_ = 0;
    rps_stat_.reset();
    last_stat_time_ = get_timestamp();
    store_service_ = &store_service;
    log_entry_task_count_ = 0;
    err_handler_ = &err_handler;
    inited_ = true;
  }

  return ret;
}

void ObLogStorager::destroy()
{
  if (inited_) {
    const int64_t thread_num = StoragerThread::get_thread_num();
    StoragerThread::destroy();

    LogMsgDestroy();

    inited_ = false;
    round_value_ = 0;
    destroy_store_buf_array_(thread_num);
    rps_stat_.reset();
    last_stat_time_ = 0;
    log_entry_task_count_ = 0;
    store_service_stat_.reset();
    store_service_ = NULL;
    err_handler_ = NULL;
  }
}

int ObLogStorager::start()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogStorager has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(StoragerThread::start())) {
    LOG_ERROR("start storager thread fail", K(ret), "thread_num", get_thread_num());
  } else {
    LOG_INFO("start storager threads succ", "thread_num", get_thread_num());
  }

  return ret;
}

void ObLogStorager::stop()
{
  if (inited_) {
    StoragerThread::stop();
    LOG_INFO("stop storager threads succ", "thread_num", get_thread_num());
  }
}

int ObLogStorager::push(ObLogEntryTask &task, const int64_t timeout)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogStorager has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(! task.is_valid())) {
    LOG_ERROR("invalid arguments", K(task));
    ret = OB_INVALID_ARGUMENT;
  } else {
    uint64_t hash_value = ATOMIC_FAA(&round_value_, 1);
    void *push_task = static_cast<void *>(&task);
    if (OB_FAIL(StoragerThread::push(push_task, hash_value, timeout))) {
      if (OB_TIMEOUT != ret) {
        LOG_ERROR("push task into storager fail", K(ret), K(push_task), K(hash_value));
      }
    } else {
      // succ
      ATOMIC_INC(&log_entry_task_count_);
    }
  }

  return ret;
}

int ObLogStorager::thread_begin()
{
  int ret = OB_SUCCESS;
  int tmp_ret = 0;

  // First call, initialization required
  if (0 != (tmp_ret = LogMsgLocalInit())) {
    LOG_ERROR("LogMsgLocalInit fail", K(tmp_ret));
    ret = OB_ERR_UNEXPECTED;
  } else {
    LOG_INFO("ObLogStorager LogMsgLocalInit succ");
  }

  return ret;
}

void ObLogStorager::thread_end()
{
  // LogMsgLocalDestroy return void
  LogMsgLocalDestroy();
  LOG_INFO("ObLogStorager LogMsgLocalDestroy succ");
}

int ObLogStorager::handle(void *data, const int64_t thread_index, volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  ObLogEntryTask *task = static_cast<ObLogEntryTask *>(data);

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogStorager has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(task) || OB_UNLIKELY(! task->is_valid())) {
    LOG_ERROR("invalid arguments", KPC(task));
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (OB_FAIL(handle_log_entry_task_(*task, thread_index, stop_flag))) {
      LOG_ERROR("handle_log_entry_task_ fail", KR(ret), KPC(task), K(thread_index));
    } else if (OB_FAIL(handle_task_callback_(*task, stop_flag))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("handle_task_callback_ fail", KR(ret), KPC(task));
      }
    } else {
      task = NULL;
      ATOMIC_DEC(&log_entry_task_count_);
    }
  }

  if (OB_SUCC(ret)) {
    if (0 == thread_index) {
      if (REACH_TIME_INTERVAL(PRINT_TASK_COUNT_INTERVAL)) {
        print_task_count_();
      }
      // TODO Improvements to monitoring items
      // rocksdb::SetPerfLevel(rocksdb::PerfLevel::kEnableTime); // enable profiling

      print_rps_();
    }
  }

  if (stop_flag) {
    ret = OB_IN_STOP_STATE;
  }

  // exit on fail
  if (OB_SUCCESS != ret && OB_IN_STOP_STATE != ret && NULL != err_handler_) {
    err_handler_->handle_error(ret, "storager thread exits, thread_index=%ld, err=%d",
        thread_index, ret);
    stop_flag = true;
  }

  return ret;
}

int ObLogStorager::handle_log_entry_task_(ObLogEntryTask &log_entry_task,
    const int64_t thread_index,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = log_entry_task.get_tenant_id();
  ObLogTenantGuard guard;
  ObLogTenant *tenant = NULL;
  DmlRedoLogMetaNode *dml_meta_node = NULL;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogStorager has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(TCTX.get_tenant_guard(tenant_id, guard))) {
    LOG_ERROR("get_tenant_guard fail", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(dml_meta_node = log_entry_task.get_meta_node())) {
    LOG_ERROR("dml_meta_node is NULL");
    ret = OB_ERR_UNEXPECTED;
  } else {
    tenant = guard.get_tenant();
    void *column_family_handle = tenant->get_cf();
    ObLogRowDataIndex *row_data_index = dml_meta_node->get_row_head();
    LOG_DEBUG("handle_log_entry_task_", K(thread_index), KPC(row_data_index), K(log_entry_task));

    while (OB_SUCC(ret) && ! stop_flag && NULL != row_data_index) {
      const bool is_rollback = row_data_index->is_rollback();

      if (is_rollback) {
        LOG_DEBUG("is_rollback stmt, do nothing", KPC(row_data_index));
      // serialize
      } else if (OB_FAIL(to_string_binlog_record_(log_entry_task, *row_data_index, thread_index))) {
        LOG_ERROR("ILogRecord to_string fail", K(ret), K(log_entry_task));
      } else if (OB_FAIL(write_store_service_(*row_data_index, column_family_handle, thread_index))) {
        LOG_ERROR("write_store_service_ fail", KR(ret), K(log_entry_task));
      } else {
        // succ
      }

      // rollback row and not-rollback row all free_br_data
      if (OB_SUCC(ret)) {
        if (OB_FAIL(row_data_index->free_br_data())) {
          LOG_ERROR("row_data_index free_br_data fail", KR(ret), K(log_entry_task));
        }
      }

      if (OB_SUCC(ret)) {
        row_data_index = row_data_index->get_next();
      }
    }
  }


  return ret;
}

int ObLogStorager::to_string_binlog_record_(ObLogEntryTask &log_entry_task,
    ObLogRowDataIndex &row_data_index,
    const int64_t thread_index)
{
  int ret = OB_SUCCESS;
  ObLogBR *task = row_data_index.get_binlog_record();

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogStorager has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(task)) {
    LOG_ERROR("invalid argument", KPC(task), K(row_data_index), K(log_entry_task));
    ret = OB_INVALID_ARGUMENT;
  } else {
    ILogRecord *binlog_record = task->get_data();

    if (OB_ISNULL(binlog_record)) {
      LOG_ERROR("invalid argument", K(binlog_record), K(row_data_index));
      ret = OB_ERR_UNEXPECTED;
    } else {
      const int record_type = binlog_record->recordType();
      const char *buf = NULL;  // Pointer to the first address of the BinlogRecord serialized byte stream
      size_t buf_len = 0;      // Length of BinlogRecord serialised byte stream
      buf = binlog_record->toString(&buf_len);
      StoreBuf &store_buf = store_buf_array_[thread_index];
      void *ptr = NULL;
      char *alloc_buf = NULL;

      if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0)) {
        LOG_ERROR("ILogRecord toString fail", KPC(task), K(record_type), K(binlog_record), K(buf), K(buf_len));
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(store_buf.alloc(buf_len + 1, ptr))) {
        LOG_ERROR("store_buf alloc fail", KR(ret), K(buf_len));
      } else if (OB_ISNULL(alloc_buf = static_cast<char *>(ptr))) {
        LOG_ERROR("alloc_buf is NULL");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        MEMCPY(alloc_buf, buf, buf_len);
        alloc_buf[buf_len] = '\0';

        // Statistics rps
        rps_stat_.do_rps_stat(1);
        store_service_stat_.do_data_stat(buf_len);
      }
    }
  }

  return ret;
}

int ObLogStorager::write_store_service_(ObLogRowDataIndex &row_data_index,
    void *column_family_handle,
    const int64_t thread_index)
{
  int ret = OB_SUCCESS;
  std::string key;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogStorager has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(row_data_index.get_storage_key(key))) {
    LOG_ERROR("get_storage_key fail", KR(ret));
  } else {
    StoreBuf &store_buf = store_buf_array_[thread_index];
    const char *br_string = store_buf.get_buf();
    const size_t br_string_len = store_buf.get_buf_len();
    // print_serilized_br_value_(row_data_index, key, br_string, br_string_len);

    if (OB_FAIL(store_service_->put(column_family_handle, key, ObSlice(br_string, br_string_len)))) {
      LOG_ERROR("store_service_ put fail", KR(ret), K(key.c_str()), K(br_string_len));
    } else {
      LOG_DEBUG("store_service_ put succ", K(key.c_str()), K(br_string_len), K(br_string));
      store_buf.free();
    }
  }

  return ret;
}

int ObLogStorager::handle_task_callback_(ObLogEntryTask &log_entry_task,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  PartTransTask *part_trans_task = static_cast<PartTransTask *>(log_entry_task.get_host());
  bool is_unserved_part_trans_task_can_be_recycled = false;

  if (OB_ISNULL(part_trans_task)) {
    LOG_ERROR("part_trans_task is NULL");
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(part_trans_task->handle_log_entry_task_callback(ObLogEntryTask::STORAGER_CB,
      log_entry_task,
      is_unserved_part_trans_task_can_be_recycled))) {
    LOG_ERROR("handle_log_entry_task_callback fail", KR(ret), K(log_entry_task), KPC(part_trans_task), K(stop_flag));
  } else if (is_unserved_part_trans_task_can_be_recycled) {
    LOG_DEBUG("handle_log_entry_task_callback: part_trans_task is revert", K(part_trans_task));
    part_trans_task->revert();
  } else {}

  return ret;
}

ObLogStorager::StoreBuf::StoreBuf() :
  data_buf_(),
  use_data_buf_(true),
  big_buf_(NULL),
  buf_len_(0)
{
}

ObLogStorager::StoreBuf::~StoreBuf()
{
  destroy();
}

int ObLogStorager::StoreBuf::init(const int64_t size)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(size <= 0)) {
    LOG_ERROR("invalid argument", K(size));
    ret = OB_INVALID_ARGUMENT;
  } else {
    void *ptr = ob_malloc(size);

    if (OB_ISNULL(ptr)) {
      LOG_ERROR("ptr is NULL");
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      data_buf_.set_data(static_cast<char *>(ptr), size);
      use_data_buf_ = true;
      buf_len_ = 0;
    }
  }

  return ret;
}

void ObLogStorager::StoreBuf::destroy()
{
  char *data = data_buf_.get_data();
  if (NULL != data) {
    ob_free(data);
    data_buf_.reset();
  }

  if (NULL != big_buf_) {
    ob_free(big_buf_);
    big_buf_ = NULL;
  }

  use_data_buf_ = true;
  buf_len_ = 0;
}

int ObLogStorager::StoreBuf::alloc(const int64_t sz, void *&ptr)
{
  int ret = OB_SUCCESS;
  ptr = data_buf_.alloc(sz);
  buf_len_ = sz;

  if (NULL != ptr) {
    use_data_buf_ = true;
  } else {
    if (OB_ISNULL(big_buf_ = static_cast<char *>(ob_malloc(sz)))) {
      LOG_ERROR("alloc big_buf_ fail");
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      use_data_buf_ = false;
      ptr = big_buf_;
    }
  }

  return ret;
}

void ObLogStorager::StoreBuf::free()
{
  if (use_data_buf_) {
    data_buf_.free();
  } else {
    if (NULL != big_buf_) {
      ob_free(big_buf_);
      big_buf_ = NULL;
    }
  }
}

const char *ObLogStorager::StoreBuf::get_buf() const
{
  const char *ret_buf = NULL;

  if (use_data_buf_) {
    ret_buf = data_buf_.get_data();
  } else {
    ret_buf = big_buf_;
  }

  return ret_buf;
}

int ObLogStorager::init_store_buf_array_(const int64_t thread_num)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(thread_num <= 0)) {
    LOG_ERROR("invalid argument", K(thread_num));
    ret = OB_INVALID_ARGUMENT;
  } else {
    for (int64_t idx = 0; OB_SUCC(ret) && idx < thread_num; ++idx) {
      if (OB_FAIL(store_buf_array_[idx].init(MAX_STORAG_DATA_BUF_SIZE + 1))) {
        LOG_ERROR("store_buf init fail", KR(ret), K(idx));
      }
    } // for
  }

  return ret;
}

void ObLogStorager::destroy_store_buf_array_(const int64_t thread_num)
{
  for (int64_t idx = 0; idx < thread_num; ++idx) {
    store_buf_array_[idx].destroy();
  } // for
}

void ObLogStorager::print_task_count_()
{
  int ret = OB_SUCCESS;
  int64_t total_thread_num = get_thread_num();

  for (int64_t idx = 0; OB_SUCC(ret) && idx < total_thread_num; ++idx) {
    int64_t task_count = 0;
    if (OB_FAIL(get_task_num(idx, task_count))) {
      LOG_ERROR("get_task_num fail", K(ret));
    } else {
      _LOG_INFO("[STAT] [STORAGER] [%ld/%ld] COUNT=%ld", idx, total_thread_num, task_count);
    }
  }
}

void ObLogStorager::print_rps_()
{
  if (REACH_TIME_INTERVAL(PRINT_RPS_STAT_INTERVAL)) {
    int64_t current_timestamp = get_timestamp();
    int64_t local_last_stat_time = last_stat_time_;
    int64_t delta_time = current_timestamp - local_last_stat_time;
    // Update last statistic
    last_stat_time_ = current_timestamp;

    // ObIncBackUpUploader
    double storager_rps = rps_stat_.calc_rps(delta_time);
    double write_rate = store_service_stat_.calc_rate(delta_time);
    double write_total_size = store_service_stat_.get_total_data_size();
    _LOG_INFO("[STORAGE] [STAT] RPS=%.3lf WRITE_RATE=%.5fM/s WRITE_TOTAL_SIZE=%.5fG",
        storager_rps, write_rate, write_total_size);

    // rocksdb::SetPerfLevel(rocksdb::PerfLevel::kDisable);
    // _LOG_INFO("[STORAGE] [STAT] perf=%s", rocksdb::get_perf_context()->ToString().c_str());
    // rocksdb::get_perf_context()->Reset();
    // rocksdb::get_iostats_context()->Reset();
    // rocksdb::SetPerfLevel(rocksdb::PerfLevel::kEnableTime); //开启profiling
  }
}

int ObLogStorager::print_unserilized_br_value_(ObLogBR &task,
    std::string &key)
{
  int ret = OB_SUCCESS;
  ILogRecord *binlog_record = task.get_data();
  ObArray<BRColElem> new_values;
  bool is_table_meta_null = false;
  ITableMeta *table_meta = NULL;

  if (0 != binlog_record->getTableMeta(table_meta)) {
    LOG_ERROR("getTableMeta fail");
    ret = OB_ERR_UNEXPECTED;
  } else {
    int64_t col_count = 0;

    if (NULL == table_meta) {
      is_table_meta_null = true;
    } else {
      col_count = table_meta->getColCount();
    }

    if (OB_ISNULL(binlog_record)) {
      LOG_ERROR("invalid argument", K(binlog_record));
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(get_mem_br_value(binlog_record, new_values))) {
      LOG_ERROR("get_mem_br_value fail", KR(ret));
    } else {
      LOG_INFO("store_service_ before serilized", "key", key.c_str(), K(new_values), K(task), K(is_table_meta_null),
          K(col_count));
    }
  }

  return ret;
}

int ObLogStorager::print_serilized_br_value_(ObLogRowDataIndex &row_data_index,
    std::string &key,
    const char *br_string,
    const size_t br_string_len)
{
  int ret = OB_SUCCESS;
  ILogRecord *binlog_record = LogMsgFactory::createLogRecord(TCTX.drc_message_factory_binlog_record_type_, false/*creating_binlog_record*/);
  ObArray<BRColElem> new_values;
  ITableMeta *table_meta = NULL;

  if (OB_ISNULL(binlog_record)) {
    LOG_ERROR("invalid argument", K(binlog_record));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(binlog_record->parse(br_string, br_string_len))) {
    LOG_ERROR("binlog_record parse fail", K(ret), K(binlog_record), K(row_data_index));
  } else if (OB_FAIL(get_br_value(binlog_record, new_values))) {
    LOG_ERROR("get_mem_br_value fail", KR(ret));
  } else if (OB_ISNULL(table_meta = LogMsgFactory::createTableMeta())) {
    LOG_ERROR("table_meta is NULL");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (0 != binlog_record->getTableMeta(table_meta)) {
    LOG_ERROR("getTableMeta fail");
    ret = OB_ERR_UNEXPECTED;
  } else {
    bool is_table_meta_null = false;
    int64_t col_count = 0;

    if (NULL == table_meta) {
      is_table_meta_null = true;
    } else {
      col_count = table_meta->getColCount();
    }

    LOG_INFO("store_service_ serilized but before put", "key", key.c_str(), K(new_values), K(row_data_index), K(is_table_meta_null),
        K(col_count));
  }

  if (NULL != table_meta) {
    LogMsgFactory::destroy(table_meta);
  }

  return ret;
}

int ObLogStorager::read_store_service_(const std::string &key)
{
  int ret = OB_SUCCESS;
  std::string br_string_res;

  if (OB_FAIL(store_service_->get(key, br_string_res))) {
    LOG_ERROR("store_service_ get fail", KR(ret), K(key.c_str()), K(br_string_res.length()));
  } else {
    LOG_DEBUG("store_service_ get succ", KR(ret), K(key.c_str()), K(br_string_res.length()), K(br_string_res.c_str()));
  }

  return ret;
}

} // namespace liboblog
} // namespace oceanbase
