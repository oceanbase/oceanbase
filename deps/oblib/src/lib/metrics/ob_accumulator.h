/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_ACCUMULATOR_H
#define _OB_ACCUMULATOR_H 1
#include <stdint.h>
#include "lib/atomic/ob_atomic.h"
#include "lib/thread_local/ob_tsi_utils.h"
namespace oceanbase
{
namespace common
{
class ObAccumulator
{
public:
  ObAccumulator() : freeze_value_(0), tmp_value_(0) {}
  ~ObAccumulator() {}

  void add(int64_t delta = 1) { ATOMIC_AAF(&tmp_value_, delta); }
  void freeze() { ATOMIC_SET(&freeze_value_, tmp_value_); ATOMIC_SET(&tmp_value_, 0); }
  int64_t get_value() const { return ATOMIC_LOAD(&freeze_value_); }
private:
  int64_t freeze_value_;
  int64_t tmp_value_;
};

} // end namespace common
} // end namespace oceanbase

#endif /* _OB_ACCUMULATOR_H */
