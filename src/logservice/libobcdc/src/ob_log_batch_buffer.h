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
 * implement ObLogBatchBuffer from ObBatchBuffer
 * 1. aggregate small buffer, then submit to BufferConsumer,
 *    after buffer consumed, handle callback of each small buffer
 * 2. Support big buffer for big row
 * 3. API is more simple
 */

#ifndef OCEANBASE_LIBOBCDC_BATCH_BUFFER_H_
#define OCEANBASE_LIBOBCDC_BATCH_BUFFER_H_

#include "ob_log_block.h"
#include "ob_log_buf.h"

namespace oceanbase
{
namespace libobcdc
{
class IObLogBatchBuffer
{
public:
  typedef IObLogBufTask Task;
  IObLogBatchBuffer() {}
  virtual ~IObLogBatchBuffer() {}

  virtual int submit(Task *task) = 0;

  virtual int try_freeze(const int64_t block_id) = 0;

  virtual void update_next_flush_block_id(const int64_t block_id) = 0;
};

class ObLogBatchBuffer: public IObLogBatchBuffer
{
public:
  ObLogBatchBuffer();
  virtual ~ObLogBatchBuffer();

  int init(const int64_t buffer_size,
      const int64_t buffer_count,
      IObBatchBufferConsumer *handler,
      bool auto_freeze = true);
  int submit(IObLogBufTask *task);

  int try_freeze(const int64_t block_id);
  void update_next_flush_block_id(const int64_t block_id);
  // UNUSED
  bool is_all_consumed() const;

private:
  class IncPos
  {
  public:
    IncPos(): seq_(0), offset_(0), entry_cnt_(0) {}
    ~IncPos() {}
    bool can_freeze(const int64_t seq) const { return seq_ == seq && offset_ != 0; }
    int try_freeze(IncPos &cur_pos, const int64_t seq);
    int append(IncPos &cur_pos, const int64_t len, const int64_t entry_cnt, const int64_t limit);
    IncPos &next_block();

    int64_t seq_;
    offset_t offset_;
    int32_t entry_cnt_;

    TO_STRING_KV(K_(seq),
        K_(offset),
        K_(entry_cnt));

    static const int32_t MAX_ENTRY_CNT = 5000;
  } __attribute__((__aligned__(16)));

private:
  int wait_block_(const int64_t block_id);
  Block *get_block_(const int64_t block_id);
  int fill_buffer_(const IncPos cur_pos, IObLogBufTask *task);

private:
  bool is_inited_;
  IObBatchBufferConsumer *handler_;
  Block *block_array_;
  int64_t block_count_;
  int64_t block_size_;
  int64_t next_flush_block_id_;
  IncPos next_pos_;
  bool auto_freeze_;

  DISALLOW_COPY_AND_ASSIGN(ObLogBatchBuffer);
};

}; // end namespace libobcdc
}; // end namespace oceanbase
#endif
