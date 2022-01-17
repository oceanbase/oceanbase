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

#ifndef _MOCK_ELECTION_CLOCK_H_
#define _MOCK_ELECTION_CLOCK_H_

#include "election/ob_election.h"
#include "election/ob_election_clock.h"

namespace oceanbase {
namespace tests {
namespace election {
using namespace oceanbase::common;
using namespace oceanbase::election;

// for clock_skew
class MockClockSkew : public ObElectionClock {
public:
  MockClockSkew(int64_t skew = 0);
  ~MockClockSkew();
  void set_skew(int64_t skew);

public:
  virtual int64_t get_current_ts(void) const;

protected:
  int64_t skew_;
  mutable ObSpinLock lock_;
};

// for clock_skew_random
class MockClockSkewRandom : public ObElectionClock {
public:
  MockClockSkewRandom(int64_t low = 0, int64_t high = 0);
  ~MockClockSkewRandom();
  void set_skew_random(int64_t low, int64_t high);

public:
  virtual int64_t get_current_ts(void) const;

protected:
  int64_t low_;
  int64_t high_;
  mutable ObSpinLock lock_;
};
}  // namespace election
}  // namespace tests
}  // namespace oceanbase

#endif
