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
#include "lib/ob_errno.h"
#include "lib/list/ob_dlist.h"
#include "lib/coro/co.h"

using namespace std;
using namespace oceanbase::lib;
using namespace oceanbase::common;

class MyCoRoutine;
static MyCoRoutine* wait_routine = nullptr;

class MyCoSched : public CoSched {
public:
  void on_runnable(MyCoRoutine& routine)
  {
    runnables_.add_last(&routine);
  }

  void on_waiting(MyCoRoutine& routine)
  {
    waitings_.add_last(&routine);
  }
  void off_waiting(MyCoRoutine& routine)
  {
    waitings_.remove(&routine);
  }

private:
  void alloc_routine(string name);
  void run() override;

protected:
  ObDList<MyCoRoutine> runnables_;
  ObDList<MyCoRoutine> waitings_;
};

class MyCoRoutine : public CoRoutine, public ObDLinkBase<MyCoRoutine> {
public:
  MyCoRoutine(string name, CoRoutine& succ, MyCoSched& sched) : CoRoutine(succ), name_(name), sched_(sched)
  {}

  string name() const
  {
    return name_;
  }

  void usleep(uint32_t) override
  {}
  void sleep_until(int64_t) override
  {}

private:
  void run() override
  {
    CO_YIELD();
    CO_YIELD();
    if (nullptr == wait_routine) {
      wait_routine = (MyCoRoutine*)&CO_CURRENT();
      cout << name_ << ", wait" << endl;
      CO_WAIT();
    } else {
      while (wait_routine != nullptr) {
        cout << name_ << ", wakeup => " << wait_routine->name_ << endl;
        CO_WAKEUP(*wait_routine);
        wait_routine = nullptr;
        CO_YIELD();
      }
    }
  }

  void on_status_change(RunStatus prev_status)
  {
    switch (prev_status) {
      case RunStatus::RUNNABLE:
        break;
      case RunStatus::WAITING:
        sched_.off_waiting(*this);
        break;
      default:
        break;
    }
    switch (get_run_status()) {
      case RunStatus::RUNNABLE:
        sched_.on_runnable(*this);
        break;
      case RunStatus::WAITING:
        sched_.on_waiting(*this);
        break;
      default:
        break;
    }
  }

  string name_;
  MyCoSched& sched_;
};

void MyCoSched::alloc_routine(string name)
{
  auto routine = new MyCoRoutine(name, *this, *this);
  ASSERT_NE(nullptr, routine);
  ASSERT_EQ(OB_SUCCESS, routine->init());
}

void MyCoSched::run()
{
  cout << "sched is running..." << endl;
  alloc_routine("r1");
  alloc_routine("r2");
  alloc_routine("r3");

  while (auto routine = runnables_.remove_first()) {
    cout << routine->name() << ", run" << endl;
    resume_routine(*routine);
    if (!routine->is_finished()) {
      cout << routine->get_id() << ", pause" << endl;
    } else {
      cout << routine->get_id() << ", exit" << endl;
    }
  }
  cout << "sched exits..." << endl;
}

TEST(TEST_CORO_SCHED, MAIN)
{
  int ret = MyCoSched().start();
  EXPECT_EQ(OB_SUCCESS, ret);
}

int main(int argc, char* argv[])
{
  // disable cout output stream
  cout.rdbuf(nullptr);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
