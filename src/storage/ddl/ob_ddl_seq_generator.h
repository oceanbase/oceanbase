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

#ifndef OCEANBASE_STORAGE_DDL_OB_DDL_SEQ_GENERATOR_H
#define OCEANBASE_STORAGE_DDL_OB_DDL_SEQ_GENERATOR_H

#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{


namespace storage
{

// the sequence generator for ddl
// for example: start = 10, interal = 5, step = 100
// the sequence generated like:
// 10, 11, 12, 13, 14,
// 110, 111, 112, 113, 114,
// 210, 211, 212, 213, 214,
// ...
// when current value is the last value of this step interval , is_step_over() return true, eg 14, 114, 214

class ObDDLSeqGenerator final
{
public:
  ObDDLSeqGenerator();
  ~ObDDLSeqGenerator();
  int init(const int64_t start, const int64_t interval, const int64_t step);
  void reset();
  int get_next(int64_t &seq_val, bool &is_step_over);
  int get_next_interval(int64_t &start, int64_t &end);
  int preview_next(const int64_t current, int64_t &next_val) const;
  int64_t get_current() const;
  int64_t get_interval_size() const;
  bool is_step_over(const int64_t val) const;
  bool is_inited() const { return is_inited_; }
  TO_STRING_KV(K_(is_inited), K_(current), K_(start), K_(interval), K_(step));

private:
  bool is_inited_;
  int64_t start_;
  int64_t interval_;
  int64_t step_;
  int64_t current_;
};

// ATTENTION: this param transmit as int64_t between observer
// the member variables are NOT ALLOWED to change
struct ObTabletSliceParam final
{
public:
  static const int64_t MAX_TABLET_SLICE_COUNT = 1 << 12; // < must < 2^16 - 1, because major compaction reserved 16 bit for parallel task
  static const int64_t MACRO_BLOCK_SEQ_INTERVAL = 1 << 32; // max 8T for each tablet piece
  static const int64_t LOB_ID_SEQ_INTERVAL = 10L * 10000L; // must bigger than max column count(4096)
  ObTabletSliceParam() : slice_id_(0) {}
  ObTabletSliceParam(const ObTabletSliceParam &other) { slice_id_ = other.slice_id_; }
  ObTabletSliceParam(const int16_t slice_count, const int16_t slice_idx)
    : slice_count_(slice_count), slice_idx_(slice_idx), reserved_(0) {}
  bool is_valid() const { return slice_count_ > 0 && slice_count_ <= MAX_TABLET_SLICE_COUNT && slice_idx_ >= 0 && slice_idx_ < slice_count_; }
  void reset() { slice_id_ = 0; }
  TO_STRING_KV(K_(slice_count), K_(slice_idx), K_(reserved));
public:
  union {
    int64_t slice_id_;
    struct {
      int16_t slice_count_;
      int16_t slice_idx_;
      int32_t reserved_;
    };
  };
};

}// namespace storage
}// namespace oceanbase

#endif//OCEANBASE_STORAGE_DDL_OB_DDL_SEQ_GENERATOR_H
