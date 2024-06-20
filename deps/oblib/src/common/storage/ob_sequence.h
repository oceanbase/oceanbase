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

#ifndef OB_SEQUENCE_H_
#define OB_SEQUENCE_H_

#include "lib/literals/ob_literals.h"
#include "lib/ob_define.h"
#include "lib/utility/ob_macro_utils.h"
#include "common/ob_clock_generator.h"

namespace oceanbase
{
namespace common
{
class ObSequence
{
  static const int64_t MAX_STEP_US = 1_day;
public:
  static int64_t get_max_seq_no();
  static int64_t inc_and_get_max_seq_no();
  static int64_t get_and_inc_max_seq_no();
  static int get_and_inc_max_seq_no(const int64_t n, int64_t &seq);
  static void inc();
  static void update_max_seq_no(int64_t seq_no);
private:
  // change us to ns
  static int64_t max_seq_no_ CACHE_ALIGNED;
  ObSequence() = delete;
};

inline int64_t ObSequence::get_max_seq_no()
{
  return ATOMIC_LOAD64(&max_seq_no_);
}

inline int64_t ObSequence::inc_and_get_max_seq_no()
{
  return ATOMIC_AAF(&max_seq_no_, 1);
}

inline int64_t ObSequence::get_and_inc_max_seq_no()
{
  return ATOMIC_FAA(&max_seq_no_, 1);
}

inline int ObSequence::get_and_inc_max_seq_no(const int64_t n, int64_t &seq)
{
  int ret = OB_SUCCESS;
  if (n > MAX_STEP_US || n < 0) {
    ret = OB_ERR_UNEXPECTED;
    if (REACH_TIME_INTERVAL(10_s)) {
      COMMON_LOG(ERROR, "seq no update encounter fatal error.", K(ret), K(n));
    }
  } else {
    seq = ATOMIC_FAA(&max_seq_no_, n);
  }
  return ret;
}

inline void ObSequence::inc()
{
  ATOMIC_INC(&max_seq_no_);
}

inline void ObSequence::update_max_seq_no(int64_t seq_no)
{
  int ret = OB_SUCCESS;
  int64_t now = ObClockGenerator::getClock();
  int64_t new_seq_no = std::max(now, seq_no + 1);
  if (new_seq_no - now > MAX_STEP_US) {
    ret = OB_ERR_UNEXPECTED;
    if (REACH_TIME_INTERVAL(10_s)) {
      COMMON_LOG(WARN, "seq no is far from physical time.", K(ret), K(now), K(seq_no));
    }
  }
  inc_update(&max_seq_no_, new_seq_no);
}

} // common
} // oceanbase

#endif
