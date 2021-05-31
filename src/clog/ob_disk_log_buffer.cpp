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

#include "ob_disk_log_buffer.h"
#include "ob_log_block.h"
#include "ob_log_define.h"

namespace oceanbase {
using namespace common;
namespace clog {

ObCLogItem::ObCLogItem() : host_(NULL), buffer_task_(NULL), batch_buffer_(NULL)
{}

ObCLogItem::~ObCLogItem()
{}

int ObCLogItem::init(ObLogWriterWrapper* host, ObIBatchBufferTask* buffer_task, ObBatchBuffer* batch_buffer)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(host) || OB_ISNULL(buffer_task) || OB_ISNULL(batch_buffer)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    host_ = host;
    buffer_task_ = buffer_task;
    batch_buffer_ = batch_buffer;
  }
  return ret;
}

void ObCLogItem::reset()
{
  host_ = NULL;
  buffer_task_ = NULL;
  batch_buffer_ = NULL;
}

bool ObCLogItem::is_valid() const
{
  return NULL != host_ && NULL != buffer_task_ && NULL != batch_buffer_;
}

char* ObCLogItem::get_buf()
{
  return NULL == buffer_task_ ? NULL : buffer_task_->get_batch_buffer();
}

const char* ObCLogItem::get_buf() const
{
  return NULL == buffer_task_ ? NULL : buffer_task_->get_batch_buffer();
}

int64_t ObCLogItem::get_data_len() const
{
  return NULL == buffer_task_ ? 0 : buffer_task_->get_batch_size();
}

int ObCLogItem::after_flushed(
    const file_id_t file_id, const offset_t offset, const int error_code, const ObLogWritePoolType type)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_IS_INVALID_FILE_ID(file_id) || OB_IS_INVALID_OFFSET(offset) || OB_ISNULL(buffer_task_) || OB_ISNULL(host_)) {
    CLOG_LOG(WARN, "invalid argument", K(file_id), K(offset), K_(buffer_task), K_(host));
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObIBufferTask* curr_task = buffer_task_->get_header_task();
    const int64_t seq = buffer_task_->get_seq();
    int64_t task_num = 0;
    if (OB_SUCCESS != (tmp_ret = buffer_task_->st_handle_callback_list(error_code, task_num))) {
      CLOG_LOG(ERROR, "st_handle_callback_list failed", K(tmp_ret));
    }
    buffer_task_->reuse();
    host_->add_group_size(task_num, type);
    const int64_t next_flush_block_id = seq + 1;
    batch_buffer_->update_next_flush_block_id(next_flush_block_id);
    if (OB_SUCCESS != (tmp_ret = batch_buffer_->try_freeze(next_flush_block_id))) {
      CLOG_LOG(ERROR, "batch_buffer try_freeze failed", K(tmp_ret));
    }
    if (CLOG_WRITE_POOL == type) {
      bool curr_is_aggre_task = false;
      bool last_is_aggre_task = false;
      ObLogWriterWrapper::CallbackTask* cb = NULL;
      for (int64_t i = 0; i < task_num; i++) {
        curr_is_aggre_task = curr_task->is_aggre_task();
        if (i % MAX_TASK_NUM_PER_CB == 0 || curr_is_aggre_task || last_is_aggre_task) {
          if (NULL != cb) {
            if (OB_SUCCESS != (tmp_ret = host_->after_consume(*cb))) {
              CLOG_LOG(ERROR, "after_consume failed", K(tmp_ret));
            }
          }
          while (NULL == (cb = op_alloc(ObLogWriterWrapper::CallbackTask))) {
            CLOG_LOG(WARN, "alloc callback task fail");
            usleep(100);
          }
          cb->cursor_.file_id_ = file_id;
          cb->cursor_.offset_ = offset;
          cb->error_code_ = error_code;
          cb->before_push_cb_ts_ = ObTimeUtility::current_time();
          cb->header_task_ = curr_task;
          cb->task_num_ = 0;
        }
        last_is_aggre_task = curr_is_aggre_task;
        curr_task = curr_task->next_;
        cb->task_num_++;
      }
      if (NULL != cb && OB_SUCCESS != (tmp_ret = host_->after_consume(*cb))) {
        CLOG_LOG(ERROR, "after_consume failed", K(tmp_ret));
      }
    }
  }
  return ret;
}

void ObLogWriterWrapper::CallbackTask::reset()
{
  cursor_.reset();
  error_code_ = common::OB_SUCCESS;
  before_push_cb_ts_ = OB_INVALID_TIMESTAMP;
  header_task_ = NULL;
  task_num_ = 0;
}

int ObLogWriterWrapper::CallbackTask::callback()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_ISNULL(header_task_) || task_num_ <= 0) {
    CLOG_LOG(ERROR, "invalid arguments", KP_(header_task), K_(task_num));
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObIBufferTask* curr_task = header_task_;
    ObIBufferTask* next_task = NULL;
    int64_t processed_cnt = 0;
    while (processed_cnt < task_num_) {
      next_task = curr_task->next_;
      if (OB_SUCCESS != (tmp_ret = curr_task->after_consume(error_code_, (void*)&cursor_, before_push_cb_ts_))) {
        CLOG_LOG(WARN, "after_consume failed", K(tmp_ret), K_(error_code), K_(cursor));
      }
      curr_task = next_task;
      processed_cnt++;
    }
    op_free(this);
  }
  return ret;
}

int ObLogWriterWrapper::init(
    ObCLogWriter* log_writer, ObICallbackHandler* callback_handler, ObBatchBuffer* batch_buffer)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(log_writer) || OB_ISNULL(callback_handler) || OB_ISNULL(batch_buffer)) {
    CLOG_LOG(WARN, "invalid argument", K(log_writer), K(callback_handler), K(batch_buffer));
    ret = OB_INVALID_ARGUMENT;
  } else if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else {
    log_writer_ = log_writer;
    callback_handler_ = callback_handler;
    batch_buffer_ = batch_buffer;
    is_inited_ = true;
  }
  if (OB_SUCCESS != ret && !is_inited_) {
    destroy();
  }
  return ret;
}

void ObLogWriterWrapper::destroy()
{
  log_writer_ = NULL;
  callback_handler_ = NULL;
  batch_buffer_ = NULL;
  is_inited_ = false;
}

int ObLogWriterWrapper::submit(Task* task)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(task) || OB_ISNULL(log_writer_)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(task->init(this))) {
    CLOG_LOG(WARN, "flush task init failed", K(ret));
  } else {
    // const int64_t begin_ts = ObTimeUtility::current_time();
    ObICLogItem* flush_task = task->get_flush_task();
    if (OB_FAIL(log_writer_->append_log(*flush_task, DEFAULT_CLOG_APPEND_TIMEOUT_US))) {
      CLOG_LOG(WARN, "submit_flush_log_task failed", K(ret));
    }
    // const int64_t end_ts = ObTimeUtility::current_time();
    // append_log_his_.Add(static_cast<double>(end_ts - begin_ts));
    // if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
    //  CLOG_LOG(INFO, "ObLogWriterWrapper::submit append_log", "histogram", append_log_his_.ToString().c_str());
    //  append_log_his_.Clear();
    // }
  }
  return ret;
}

int ObLogWriterWrapper::after_consume(ObICallback& task)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(callback_handler_)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ret = callback_handler_->handle_callback(&task);
  }
  if (OB_FAIL(ret)) {
    // async callback failed, use writer thread to callback directly
    ret = task.callback();
  }
  return ret;
}

void ObLogWriterWrapper::add_group_size(const int64_t task_num, const ObLogWritePoolType type)
{
  UNUSED(task_num);
  UNUSED(type);
#ifdef USE_HISTOGRAM
  his_.Add(static_cast<double>(task_num));
  if (REACH_TIME_INTERVAL(1000 * 1000)) {
    if (CLOG_WRITE_POOL == type) {
      CLOG_LOG(INFO, "clog batch group size", "clog", his_.ToString().c_str());
      his_.Clear();
    } else {
      CLOG_LOG(INFO, "ilog batch group size", "ilog", his_.ToString().c_str());
      his_.Clear();
    }
  }
#endif  // USE_HISTOGRAM
}

int ObDiskLogBuffer::submit(ObIBufferTask* task)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ret = batch_buffer_.submit(task);
  }
  return ret;
}

int ObDiskLogBuffer::init(
    const int64_t buffer_size, const int64_t buffer_cnt, ObCLogWriter* log_writer, ObICallbackHandler* callback_handler)
{
  int ret = OB_SUCCESS;
  ObLogBlockMetaV2 meta;
  int64_t header_size = meta.get_serialize_size();
  int64_t min_padding_size = 1024 - header_size;  // assume header_size + padding_entry_size < 1K
  int64_t trailer_size = min_padding_size;        // pending
  if (buffer_size <= 0 || buffer_cnt <= 0 || OB_ISNULL(log_writer) || OB_ISNULL(callback_handler)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS !=
             (ret = buffer_arena_.init(
                  ObModIds::OB_CLOG_MGR, CLOG_DIO_ALIGN_SIZE, header_size, trailer_size, buffer_size, buffer_cnt))) {
    CLOG_LOG(WARN, "buffer_pool init failed", K(ret), K(buffer_size), K(buffer_cnt));
  } else if (OB_SUCCESS != (ret = log_writer_.init(log_writer, callback_handler, &batch_buffer_))) {
    CLOG_LOG(WARN, "log_writer init failed", K(ret));
  } else if (OB_SUCCESS != (ret = batch_buffer_.init(&buffer_arena_, &log_writer_))) {
    CLOG_LOG(WARN, "batch_buffer init failed", K(ret));
  } else {
    log_buffer_cnt_ = buffer_cnt;
  }
  CLOG_LOG(INFO, "disk_log_buffer init finished", K(buffer_size), K(buffer_cnt), K(header_size), K(ret));
  return ret;
}
};  // end namespace clog
};  // end namespace oceanbase
