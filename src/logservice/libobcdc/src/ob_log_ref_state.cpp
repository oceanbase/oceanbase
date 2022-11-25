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

#include "ob_log_ref_state.h"

#include "lib/atomic/ob_atomic.h"     // ATOMIC_*

namespace oceanbase
{
namespace libobcdc
{

bool RefState::inc_ref(const int64_t target_state, int64_t &new_state, int64_t &new_ref)
{
  bool succ = false;
  RefState cur_st(ATOMIC_LOAD(&value_));

  while (! succ && cur_st.state_ == target_state) {
    RefState old_st = cur_st;
    RefState new_st = cur_st;
    new_st.ref_cnt_++;   // Status unchanged, reference count +1

    cur_st.value_ = ATOMIC_CAS(&value_, old_st.value_, new_st.value_);

    if (cur_st.value_ == old_st.value_) {
      succ = true;
      // The reference count is successfully increased, returning the new state and the new reference count
      new_state = new_st.state_;
      new_ref = new_st.ref_cnt_;
    } else {
      // Reference count increase failed, return current state and current reference count
      new_state = cur_st.state_;
      new_ref = cur_st.ref_cnt_;
    }
  }

  return succ;
}

void RefState::dec_ref(int64_t &new_state, int64_t &new_ref_cnt)
{
  RefState cur_st(ATOMIC_LOAD(&value_));

  bool done = false;
  while (!done) {
    RefState old_st = cur_st;
    RefState new_st = cur_st;
    new_st.ref_cnt_--; // No change in status, number of transactions minus 1

    cur_st.value_ = ATOMIC_CAS(&value_, old_st.value_, new_st.value_);

    if (old_st.value_ == cur_st.value_) {
      done = true;
      // Returns the state value and reference count after the reference count has been subtracted successfully
      new_state = new_st.state_;
      new_ref_cnt = new_st.ref_cnt_;
    }
  }
}

bool RefState::change_state(const int64_t target_state, int64_t &old_state, int64_t &ref_cnt)
{
  bool succ = false;
  RefState cur_st(ATOMIC_LOAD(&value_));

  while (! succ && cur_st.state_ != target_state) {
    RefState old_st = cur_st;
    RefState new_st = cur_st;
    new_st.state_ = target_state;   // No change in reference count, change in status

    cur_st.value_ = ATOMIC_CAS(&value_, old_st.value_, new_st.value_);

    if (cur_st.value_ == old_st.value_) {
      succ = true;
    }
  }

  // cur_st holds the old state values
  old_state = cur_st.state_;
  ref_cnt = cur_st.ref_cnt_;

  return succ;
}

}
}
