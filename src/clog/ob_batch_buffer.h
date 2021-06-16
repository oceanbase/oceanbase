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

#ifndef OCEANBASE_CLOG_OB_BATCH_BUFFER_
#define OCEANBASE_CLOG_OB_BATCH_BUFFER_

#include "ob_buffer_arena.h"
#include "ob_buffer_task.h"

namespace oceanbase {
namespace clog {
class ObLogWriterWrapper;
class ObBatchBuffer : public ObIBufferConsumer {
public:
  ObBatchBuffer();
  virtual ~ObBatchBuffer();

  int init(ObIBufferArena* buffer_pool, ObIBatchBufferConsumer* handler, bool auto_freeze = true);
  int submit(ObIBufferTask* task);
  int try_freeze_next_block();
  int try_freeze(const int64_t block_id);
  void update_next_flush_block_id(const int64_t block_id);
  bool is_all_consumed() const;

private:
  class IncPos {
  public:
    IncPos() : seq_(0), offset_(0), entry_cnt_(0)
    {}
    ~IncPos()
    {}
    bool can_freeze(const int64_t seq) const
    {
      return seq_ == seq && offset_ != 0;
    }
    int try_freeze(IncPos& cur_pos, const int64_t seq);
    int append(IncPos& cur_pos, const int64_t len, const int64_t entry_cnt, const int64_t limit);
    IncPos& next_block();

    int64_t seq_;
    offset_t offset_;
    int32_t entry_cnt_;

    static const int32_t MAX_ENTRY_CNT = 5000;
  } __attribute__((__aligned__(16)));
  class Block;

private:
  int wait_block(const int64_t block_id);
  Block* get_block(const int64_t block_id);
  int fill_buffer(const IncPos cur_pos, ObIBufferTask* task);

private:
  bool is_inited_;
  ObIBatchBufferConsumer* handler_;
  Block* block_array_;
  int64_t block_count_;
  int64_t block_size_;
  int64_t next_flush_block_id_;
  IncPos next_pos_;
  bool auto_freeze_;

  DISALLOW_COPY_AND_ASSIGN(ObBatchBuffer);
};
};      // end namespace clog
};      // end namespace oceanbase
#endif  // OCEANBASE_CLOG_OB_BATCH_BUFFER_
