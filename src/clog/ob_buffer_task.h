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

#ifndef OCEANBASE_CLOG_OB_BUFFER_TASK_
#define OCEANBASE_CLOG_OB_BUFFER_TASK_

#include "share/ob_define.h"
#include "ob_log_define.h"

namespace oceanbase {
namespace clog {
class ObICLogItem;
class ObLogWriterWrapper;

template <int64_t limit>
class MyFixedBufferAlloc {
public:
  MyFixedBufferAlloc() : pos_(0)
  {}
  ~MyFixedBufferAlloc()
  {}
  void reset()
  {
    pos_ = 0;
  }
  void* alloc(int64_t size)
  {
    void* ret = NULL;
    int64_t pos = 0;
    if ((pos = ATOMIC_FAA(&pos_, size)) + size <= limit) {
      ret = buf_ + pos;
    }
    return ret;
  }

private:
  int64_t pos_;
  char buf_[limit];

  DISALLOW_COPY_AND_ASSIGN(MyFixedBufferAlloc);
};

// DiskLogBuffer and NetLogBuffer serilize different content to BatchBuffer by using respective BufferTask.
class ObIBufferTask {
public:
  ObIBufferTask() : next_(NULL), need_callback_(false)
  {}
  virtual ~ObIBufferTask()
  {}
  void reset();
  virtual int64_t get_data_len() const = 0;
  virtual int64_t get_entry_cnt() const = 0;
  virtual int fill_buffer(char* buf, const offset_t offset) = 0;
  // single thread calls after_consume to update LogRangeInfo for DiskLogBuffer.
  virtual int st_after_consume(const int handle_err) = 0;
  virtual int after_consume(const int handle_err, const void* arg, const int64_t before_push_cb_ts) = 0;
  virtual bool is_aggre_task() const
  {
    return false;
  }
  ObIBufferTask* next_;
  bool need_callback_;
};

class DummyBuffferTask : public ObIBufferTask {
public:
  DummyBuffferTask()
  {}
  virtual ~DummyBuffferTask()
  {}
  virtual int64_t get_data_len() const;
  virtual int64_t get_entry_cnt() const;
  virtual int fill_buffer(char* buf, const offset_t offset);
  virtual int st_after_consume(const int handle_err);
  virtual int after_consume(const int handle_err, const void* arg, const int64_t before_push_cb_ts);

private:
  DISALLOW_COPY_AND_ASSIGN(DummyBuffferTask);
};

// BatchBuffer will submit a batch to BufferConsumer, which will construct a header and submit to disk/net.
class ObIBatchBufferTask {
public:
  ObIBatchBufferTask() : batch_buf_(NULL), batch_size_(0), subtask_count_(0), head_(), task_list_tail_(&head_)
  {}
  virtual ~ObIBatchBufferTask()
  {}
  virtual int init(ObLogWriterWrapper* handler) = 0;
  void reset();
  void* alloc(int64_t size)
  {
    return alloc_.alloc(size);
  }
  ObIBatchBufferTask& set_batch_buffer(char* buf, const int64_t len);
  char* get_batch_buffer()
  {
    return batch_buf_;
  }
  int64_t get_batch_size()
  {
    return batch_size_;
  }
  int64_t get_subtask_count() const
  {
    return subtask_count_;
  }
  void add_callback_to_list(ObIBufferTask* task);
  int st_handle_callback_list(const int handle_err, int64_t& task_num);
  ObIBufferTask* get_header_task()
  {
    return head_.next_;
  }
  virtual void after_consume() = 0;
  virtual void reuse() = 0;
  virtual int64_t get_seq() const = 0;
  virtual ObICLogItem* get_flush_task() = 0;
  TO_STRING_KV(N_BUF, ((uint64_t)(batch_buf_)), N_BUF_LEN, batch_size_, N_COUNT, subtask_count_);

private:
  // alloc memory for RpcPostHandler's Task
  MyFixedBufferAlloc<256> alloc_;
  char* batch_buf_;
  int64_t batch_size_;
  int64_t subtask_count_;
  DummyBuffferTask head_;
  ObIBufferTask* task_list_tail_;  // callback list

  DISALLOW_COPY_AND_ASSIGN(ObIBatchBufferTask);
};

class ObIBufferConsumer {
public:
  typedef ObIBufferTask Task;
  ObIBufferConsumer()
  {}
  virtual ~ObIBufferConsumer()
  {}
  virtual int submit(Task* task) = 0;
};

class ObIBatchBufferConsumer {
public:
  typedef ObIBatchBufferTask Task;
  ObIBatchBufferConsumer()
  {}
  virtual ~ObIBatchBufferConsumer()
  {}
  virtual int submit(Task* task) = 0;
};
};      // end namespace clog
};      // end namespace oceanbase
#endif  // OCEANBASE_CLOG_OB_BUFFER_TASK_
