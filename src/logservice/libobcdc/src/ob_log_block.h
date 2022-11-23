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
 * Block: aggregate small log
 */

#ifndef OCEANBASE_LIBOBCDC_LOG_BLOCK_H_
#define OCEANBASE_LIBOBCDC_LOG_BLOCK_H_

#include "ob_log_batch_buffer_task.h"
#include "ob_log_buf.h"

namespace oceanbase
{
namespace libobcdc
{
// Extent
// class DataBlock
// DataBuf
class Block : public IObLogBatchBufTask
{
public:
  Block(int64_t seq, char *buf, int64_t block_count);
  virtual ~Block() {}
  int init();
  virtual void after_consume();
  virtual void reuse();
  virtual int64_t get_seq() const { return seq_; }
  virtual bool is_valid() const { return NULL != buf_; }

public:
  int64_t ref(const int64_t delta);
  int64_t wait(const int64_t seq);
  void fill(const offset_t offset, IObLogBufTask *task);
  void freeze(const offset_t offset);
  void set_submitted();
  void wait_submitted(const int64_t seq);

  TO_STRING_KV(K_(seq),
      K_(status_seq),
      K_(ref),
      "buf", ((uint64_t)(buf_)),
      K_(block_count));

private:
  int64_t seq_;
  int64_t status_seq_;
  int64_t ref_;
  char *buf_;
  int64_t block_count_;
};

// For big row
class BigBlock : public IObLogBatchBufTask
{
public:
  BigBlock();
  virtual ~BigBlock() { destroy(); }
  int init(const int64_t buf_size);
  void destroy();

  virtual void after_consume();
  virtual void reuse();
  virtual int64_t get_seq() const { return 0; }
  virtual bool is_valid() const { return true; }

public:
  void fill(const offset_t offset, IObLogBufTask *task);

  TO_STRING_KV("buf", ((uint64_t)(buf_)));

private:
  char *buf_;
};

}; // end namespace libobcdc
}; // end namespace oceanbase
#endif
