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

#ifndef OCEANBASE_LIBOBCDC_OB_LOG_REF_STATE_H_
#define OCEANBASE_LIBOBCDC_OB_LOG_REF_STATE_H_

#include <stdint.h>     // int64_t

namespace oceanbase
{
namespace libobcdc
{

/// State with its own reference count
///
//// state variable (8 bits) + reference count (56) bits => 64 bits
////
/// Applicable scenarios: atomic maintenance of object state, combining state variables with reference counts to guarantee a safe end-of-life for objects.
///
/// Example.
/// 1. initialised to the normal state (0) with a reference count of 0
/// 2. Normal state with reference count dynamically increasing or decreasing
/// 3. When the object is destroyed, the state is atomically changed to offline, after which the reference count can only be decreased, not increased
/// 4. When the state becomes offline and the reference count is 0, the object can be safely destroyed
struct RefState
{
  union
  {
    struct
    {
      int64_t state_:8;         // The lower 8 bits are status variables
      int64_t ref_cnt_:56;      // The high 56 bits are reference counters
    };

    int64_t value_;             // Full 64-bit values
  };

  /// When the state is target_state, the reference count is increased and success is returned
  /// If the state is not target_state, no reference counting is performed and a failure is returned
  ///
  /// @param [in] target_state      target_state, only match this state to increase the reference count
  /// @param [out] new_state        The state value at the end of execution
  /// @param [out] new_ref          Reference count at the end of execution
  ///
  /// @retval whether execution was successful
  bool inc_ref(const int64_t target_state, int64_t &new_state, int64_t &new_ref);

  /// Unconditionally decreasing reference count
  ///
  /// @param [out] new_state The value of the state corresponding to the successful subtraction of the reference count
  /// @param [out] new_ref_cnt The value of the reference count after it has been subtracted
  void dec_ref(int64_t &new_state, int64_t &new_ref_cnt);

  /// Ensure that the reference count remains unchanged and change the current state
  ///
  /// @param [in] target_state  The target state to be changed
  /// @param [out] old_state    The value of the state before the change
  /// @param [out] ref_cnt      Reference count at the end of execution
  ///
  /// @retval true    change of state successful, original state not equal to target_state
  /// @retval false   Failed to change state, original state equal to target_state, i.e. no change needed
  bool change_state(const int64_t target_state, int64_t &old_state, int64_t &ref_cnt);

  void reset()
  {
    state_ = 0;
    ref_cnt_ = 0;
  }

  // Initialised to specified status
  void reset(const int64_t state)
  {
    state_ = state;
    ref_cnt_ = 0;
  }

  RefState() { reset(); }
  explicit RefState(const int64_t value) : value_(value) {}
  ~RefState() { reset(); }
};

}
}

#endif
