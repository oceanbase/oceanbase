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

#ifndef OCEANBASE_ENGINE_OB_BATCH_ROWS_H_
#define OCEANBASE_ENGINE_OB_BATCH_ROWS_H_

#include "lib/utility/ob_print_utils.h"
#include "lib/oblog/ob_log.h"
#include "sql/engine/ob_bit_vector.h"

namespace oceanbase
{
namespace sql
{

// operator return batch rows description
struct ObBatchRows
{
  ObBatchRows() : skip_(NULL), size_(0), end_(false), all_rows_active_(false) {}

  DECLARE_TO_STRING;

  int copy(const ObBatchRows *src)
  {
    int ret = common::OB_SUCCESS;
    size_ = src->size_;
    all_rows_active_ = src->all_rows_active_;
    end_ = src->end_;
    // TODO qubin.qb: use shallow copy instead
    skip_->deep_copy(*(src->skip_), size_);
    return ret;
  }

  void set_skip(int64_t idx) {
    skip_->set(idx);
    all_rows_active_ = false;
  }

  void reset_skip(const int64_t size) {
    skip_->reset(size);
    all_rows_active_ = true;
  }

  void set_all_rows_active(bool v) {
    all_rows_active_ = v;
  }

public:
  //TODO shengle set skip and all_rows_active_ in private members
  // bit vector for filtered rows
  ObBitVector *skip_;
  // batch size
  int64_t size_;
  // iterate end
  bool end_;
  bool all_rows_active_; // means all rows in batch not skip
};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_ENGINE_OB_BATCH_ROWS_H_
