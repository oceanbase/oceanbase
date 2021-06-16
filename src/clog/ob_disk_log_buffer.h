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

#ifndef OCEANBASE_CLOG_OB_DISK_LOG_BUFFER_H_
#define OCEANBASE_CLOG_OB_DISK_LOG_BUFFER_H_

#include "common/ob_i_callback.h"
#include "ob_batch_buffer.h"
#include "ob_i_disk_log_buffer.h"
#include "ob_clog_writer.h"

namespace oceanbase {
namespace clog {
class ObLogWriterWrapper : public ObIBatchBufferConsumer {
public:
  class CallbackTask : public common::ObICallback {
  public:
    CallbackTask()
    {
      reset();
    }
    ~CallbackTask()
    {}
    void reset();
    int callback();

  public:
    ObLogCursor cursor_;
    int error_code_;
    int64_t before_push_cb_ts_;

    ObIBufferTask* header_task_;
    int64_t task_num_;

  private:
    DISALLOW_COPY_AND_ASSIGN(CallbackTask);
  };

public:
  ObLogWriterWrapper() : log_writer_(NULL), callback_handler_(NULL), batch_buffer_(NULL), is_inited_(false)
  {}
  virtual ~ObLogWriterWrapper()
  {
    destroy();
  }
  int init(ObCLogWriter* log_writer, common::ObICallbackHandler* callback_handler, ObBatchBuffer* batch_buffer);
  void destroy();
  int submit(Task* task);
  int after_consume(common::ObICallback& task);
  void add_group_size(const int64_t task_num, const ObLogWritePoolType type);

private:
  ObCLogWriter* log_writer_;
  common::ObICallbackHandler* callback_handler_;
  ObBatchBuffer* batch_buffer_;
#ifdef USE_HISTOGRAM
  Histogram his_;
  Histogram append_log_his_;
#endif  // USE_HISTOGRAM
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObLogWriterWrapper);
};

class ObCLogItem : public ObICLogItem {
public:
  ObCLogItem();
  virtual ~ObCLogItem();
  int init(ObLogWriterWrapper* host, ObIBatchBufferTask* buffer_task, ObBatchBuffer* batch_buffer);
  void reset();
  virtual bool is_valid() const;
  virtual char* get_buf();
  virtual const char* get_buf() const;
  virtual int64_t get_data_len() const;
  virtual int after_flushed(
      const file_id_t file_id, const offset_t offset, const int error_code, const ObLogWritePoolType type);
  TO_STRING_KV(KP_(host), KP_(buffer_task), KP_(batch_buffer));

private:
  static const int64_t MAX_TASK_NUM_PER_CB = 20;
  ObLogWriterWrapper* host_;
  ObIBatchBufferTask* buffer_task_;
  ObBatchBuffer* batch_buffer_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObCLogItem);
};

class ObDiskLogBuffer : public ObIDiskLogBuffer {
public:
  ObDiskLogBuffer() : log_buffer_cnt_(0)
  {}
  virtual ~ObDiskLogBuffer()
  {}

public:
  int init(const int64_t buffer_size, const int64_t buffer_cnt, ObCLogWriter* log_writer,
      common::ObICallbackHandler* callback_handler);
  int submit(ObIBufferTask* task);
  bool is_all_consumed() const
  {
    return batch_buffer_.is_all_consumed();
  }
  int64_t get_log_buffer_cnt() const
  {
    return log_buffer_cnt_;
  }

private:
  int64_t log_buffer_cnt_;
  ObLogWriterWrapper log_writer_;
  ObBufferArena buffer_arena_;
  ObBatchBuffer batch_buffer_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObDiskLogBuffer);
};
};  // namespace clog
};  // end namespace oceanbase

#endif /* OCEANBASE_CLOG_OB_DISK_LOG_BUFFER_H_ */
