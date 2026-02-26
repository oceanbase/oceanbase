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
