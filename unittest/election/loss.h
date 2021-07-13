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

#ifndef _ELECTION_TEST_LOSS_H_
#define _ELECTION_TEST_LOSS_H_

#include <stdint.h>
#include "common/ob_spin_lock.h"

namespace oceanbase {
namespace tests {
namespace election {
using namespace oceanbase::common;

class Loss {
public:
  Loss(int64_t loss_rate = 0, int64_t start = 0, int64_t end = 0);
  ~Loss();

public:
  void set_loss(int64_t loss_rate, int64_t start = INT64_MIN, int64_t end = INT64_MAX);
  bool is_loss(int64_t ts);

protected:
  int64_t loss_rate_;
  int64_t start_;
  int64_t end_;
  mutable ObSpinLock lock_;
};
}  // namespace election
}  // namespace tests
}  // namespace oceanbase

#endif
