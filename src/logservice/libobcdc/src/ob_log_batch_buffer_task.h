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

#ifndef OCEANBASE_LIBOBCDC_BATCH_BUFFER_TASK_H_
#define OCEANBASE_LIBOBCDC_BATCH_BUFFER_TASK_H_

#include "ob_log_utils.h"

namespace oceanbase
{
namespace libobcdc
{
class IObLogBufTask
{
public:
  IObLogBufTask();
  virtual ~IObLogBufTask() { reset(); }
  void reset();

  virtual bool is_valid() const = 0;
  virtual int64_t get_data_len() const = 0;
  virtual int64_t get_entry_cnt() const = 0;
  virtual int fill_buffer(char *buf, const offset_t offset) = 0;
  // single thread calls after_consume
  virtual int st_after_consume(const int handle_err) = 0;

  IObLogBufTask *next_;
  bool need_callback_;
};

class DummyBufferTask : public IObLogBufTask
{
public:
  DummyBufferTask() {}
  virtual ~DummyBufferTask() {}

  virtual bool is_valid() const;
  virtual int64_t get_data_len() const;
  virtual int64_t get_entry_cnt() const;
  virtual int fill_buffer(char *buf, const offset_t offset);
  virtual int st_after_consume(const int handle_err);

private:
  DISALLOW_COPY_AND_ASSIGN(DummyBufferTask);
};

class IObLogBatchBufTask
{
public:
  enum BlockType
  {
    UNKNOWN_TASK = 0,
    NORMAL_BLOCK = 1,
    BIG_BLOCK = 2
  };

  IObLogBatchBufTask(BlockType block_type);
  virtual ~IObLogBatchBufTask();
  void reset();
  virtual void after_consume() = 0;
  virtual void reuse() = 0;
  virtual int64_t get_seq() const = 0;
  virtual bool is_valid() const = 0;

public:
  bool is_big_block() const { return BIG_BLOCK == block_type_; }
  IObLogBatchBufTask &set_batch_buffer(char *buf, const int64_t len);
  char *get_batch_buffer() { return batch_buf_; }
  int64_t get_batch_size() { return batch_size_; }
  int64_t get_subtask_count() const { return subtask_count_; }
  void add_callback_to_list(IObLogBufTask *task);
  int st_handle_callback_list(const int handle_err, int64_t &task_num);
  IObLogBufTask *get_header_task() {return head_.next_; }

  TO_STRING_KV("buf", ((uint64_t)(batch_buf_)),
               "buf_len", batch_size_,
               "count", subtask_count_);
private:
  BlockType block_type_;
  char *batch_buf_;
  int64_t batch_size_;
  int64_t subtask_count_;
  DummyBufferTask head_;
  IObLogBufTask *task_list_tail_; // callback list

private:
  DISALLOW_COPY_AND_ASSIGN(IObLogBatchBufTask);
};

class IObBatchBufferConsumer
{
public:
  typedef IObLogBatchBufTask Task;
  IObBatchBufferConsumer() {}
  virtual ~IObBatchBufferConsumer() {}
  virtual int submit(Task *task) = 0;
};

}; // end namespace libobcdc
}; // end namespace oceanbase
#endif
