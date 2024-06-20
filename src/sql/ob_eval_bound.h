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

#ifndef OCEANBASE_SQL_OB_EVAL_BOUND_DEFINE_H_
#define OCEANBASE_SQL_OB_EVAL_BOUND_DEFINE_H_

#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{

namespace sql
{
// eval range is [start_idx_, end_idx_)
struct EvalBound {
  EvalBound() : batch_size_(0), start_idx_(0), end_idx_(0), flag_(0) {}
  explicit EvalBound(uint16_t size, bool all_active = false)
    : batch_size_(size), start_idx_(0), end_idx_(size), all_rows_active_(all_active)  {}
  EvalBound(uint16_t size, uint16_t start_idx, uint16_t end_idx, bool all_active)
    : batch_size_(size), start_idx_(start_idx), end_idx_(end_idx), all_rows_active_(all_active)
  {}

  inline uint16_t batch_size() const { return batch_size_; }
  inline uint64_t range_size() const { return end_idx_ - start_idx_; }
  inline uint16_t start() const { return start_idx_; }
  inline uint16_t end() const { return end_idx_; }
  inline bool is_full_size() const { return 0 == start_idx_ && batch_size_ == end_idx_; }
  inline bool get_all_rows_active() const { return all_rows_active_; }
  inline void set_all_row_active(const bool all_active) { all_rows_active_ = all_active; }

  TO_STRING_KV(K_(batch_size), K_(start_idx), K_(end_idx), K_(all_rows_active));

private:
  // batch_size_ is batch size, not range size;
  // eg: batch_size_ = 256, start_idx_ = 0, end_idx_ = 5;
  // range is [0, 5), and range_size = 5;
  uint16_t batch_size_;
  uint16_t start_idx_;
  uint16_t end_idx_;
  union {
    uint16_t flag_;
    struct {
      uint16_t all_rows_active_: 1;
      uint16_t reserved: 15;
    };
  };
};

}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_OB_EVAL_BOUND_DEFINE_H_ */
