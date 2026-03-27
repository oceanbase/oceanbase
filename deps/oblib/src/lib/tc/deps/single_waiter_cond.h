/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

class SingleWaiterCond
{
public:
  SingleWaiterCond(): stock_(0) {}
  ~SingleWaiterCond() {}
  void wait(int64_t timeout_us) {
    if (0 == ATOMIC_LOAD(&stock_)) {
      tc_futex_wait(&stock_, 0, timeout_us);
    } else {
      ATOMIC_STORE(&stock_, 0);
    }
  }
  void signal() {
    if (0 == ATOMIC_LOAD(&stock_) && ATOMIC_BCAS(&stock_, 0, 1)) {
      tc_futex_wake(&stock_, 1);
    }
  }
private:
  int stock_;
};
