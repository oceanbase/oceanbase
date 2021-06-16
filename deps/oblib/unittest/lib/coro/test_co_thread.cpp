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

#include <gtest/gtest.h>
#include <iostream>
#include <string>
#include "lib/coro/co.h"
#include "lib/ob_errno.h"

using namespace std;
using namespace oceanbase::lib;
using namespace oceanbase::common;

class MyCoRoutine : public CoRoutine {
public:
  MyCoRoutine(string name, CoRoutine& main_routine) : CoRoutine(main_routine), name_(name)
  {}

private:
  void run() override
  {
    cout << name_ << ", begin run" << endl;
    cout << name_ << ", yield1" << endl;
    CO_YIELD();
    cout << name_ << ", yield2" << endl;
    CO_YIELD();
    cout << name_ << ", exit" << endl;
  }

  void usleep(uint32_t)
  {}
  void sleep_until(int64_t)
  {}

  string name_;
};

class MyCoThread : public CoThread {
public:
private:
  CoRoutine* alloc_routine(string name)
  {
    return new MyCoRoutine(name, *this);
  }

  void run() override
  {
    cout << "sched is running... [" << get_pid() << "-" << get_tid() << "]" << endl;
    auto r1 = alloc_routine("r1");
    auto r2 = alloc_routine("r2");
    auto r3 = alloc_routine("r3");

    ASSERT_NE(nullptr, r1);
    ASSERT_NE(nullptr, r2);
    ASSERT_NE(nullptr, r3);
    ASSERT_EQ(OB_SUCCESS, r1->init());
    ASSERT_EQ(OB_SUCCESS, r2->init());
    ASSERT_EQ(OB_SUCCESS, r3->init());

    while (!r1->is_finished() || !r2->is_finished() || !r3->is_finished()) {
      if (!r1->is_finished()) {
        resume_routine(*r1);
      }
      if (!r2->is_finished()) {
        resume_routine(*r2);
      }
      if (!r3->is_finished()) {
        resume_routine(*r3);
      }
    }
    CO_YIELD();
    delete r1;
    delete r2;
    delete r3;

    while (!has_set_stop()) {
      this_routine::usleep(100 * 1000L);
    }

    cout << "sched exits..." << endl;
  }
};

TEST(TestCoThread, Main)
{
  MyCoThread ct;
  int ret = ct.start();
  EXPECT_EQ(OB_SUCCESS, ret);
  ct.stop();
  ct.wait();
}

//// This case is used to check memory leak with infinite loop.
// TEST(TestCoThread, Loop)
// {
//   MyCoThread ct;
//   while (true) {
//     ct.start();
//     ct.stop();
//     ct.wait();
//   }
// }

int main(int argc, char* argv[])
{
  // disable cout output stream
  cout.rdbuf(nullptr);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
