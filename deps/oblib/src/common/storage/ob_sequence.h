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

#include "lib/ob_define.h"
#include "lib/utility/ob_macro_utils.h"
#include "common/ob_clock_generator.h"

namespace oceanbase
{
namespace common
{
class ObSequence
{
public:
  static int64_t get_max_seq_no();
  static int64_t inc_and_get_max_seq_no();
  static int64_t get_and_inc_max_seq_no();
  static int64_t get_and_inc_max_seq_no(int64_t n);
  static void inc();
  static void update_max_seq_no(int64_t seq_no);
private:
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

inline int64_t ObSequence::get_and_inc_max_seq_no(int64_t n)
{
  return ATOMIC_FAA(&max_seq_no_, n);
}

inline void ObSequence::inc()
{
  ATOMIC_INC(&max_seq_no_);
}

inline void ObSequence::update_max_seq_no(int64_t seq_no)
{
  int64_t now = ObClockGenerator::getClock();
  int64_t new_seq_no = std::max(now, seq_no + 1);

  inc_update(&max_seq_no_, new_seq_no);
}

} // common
} // oceanbase

#endif
