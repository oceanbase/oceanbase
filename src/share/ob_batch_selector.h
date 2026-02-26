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

#ifndef _SHARE_OB_BATCH_SELECTOR_
#define _SHARE_OB_BATCH_SELECTOR_

#include "lib/utility/ob_print_utils.h"
#include "sql/engine/ob_bit_vector.h"

namespace oceanbase
{

namespace sql
{
struct ObBatchRows;
}

namespace share
{

class ObBatchSelector
{
public:
  enum Type
  {
    INVALID_TYPE,
    CONTINIOUS_LENGTH,
    SKIP_BITMAP,
    ACTIVE_ARRAY_U16,
    ACTIVE_ARRAY_U32,
    ACTIVE_ARRAY_U64,
  };

public:
  ObBatchSelector() : type_(INVALID_TYPE), offset_(0), count_(0), cursor_(0) {}
  ObBatchSelector(const sql::ObBatchRows &brs) { set_batch_rows(brs); }
  ObBatchSelector(const int64_t offset, const int64_t row_count) { set_continous_rows(offset, row_count); }
  ObBatchSelector(const uint16_t *active_array, const int64_t row_count) { set_active_array(active_array, row_count); }
  ObBatchSelector(const uint32_t *active_array, const int64_t row_count) { set_active_array(active_array, row_count); }
  ObBatchSelector(const uint64_t *active_array, const int64_t row_count) { set_active_array(active_array, row_count); }
  ~ObBatchSelector() {}
  void set_batch_rows(const sql::ObBatchRows &brs);
  void set_continous_rows(const int64_t offset, const int64_t row_count);
  void set_active_array(const uint16_t *active_array, const int64_t row_count);
  void set_active_array(const uint32_t *active_array, const int64_t row_count);
  void set_active_array(const uint64_t *active_array, const int64_t row_count);
  bool is_valid() const { return type_ != INVALID_TYPE && count_ > 0 && cursor_ >= 0; }
  bool is_prefix() const { return SKIP_BITMAP == type_ || (CONTINIOUS_LENGTH == type_ && 0 == offset_); }
  int64_t size() const { return count_; }
  int64_t get_offset() const { return offset_; }
  Type get_type() const { return type_; }
  int64_t get_end() const;
  int get_next(int64_t &offset);
  void rescan() { cursor_ = 0; }
  DECLARE_TO_STRING;

private:
  Type type_;
  union {
    const sql::ObBitVector *skip_;
    const uint16_t *u16_array_;
    const uint32_t *u32_array_;
    const uint64_t *u64_array_;
    int64_t offset_;
  };
  int64_t count_;
  int64_t cursor_;
};

}// namespace share
}// namespace oceanbase

#endif//_SHARE_OB_BATCH_SELECTOR_
