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
#include <vector>
#include "lib/coro/co_thread.h"
#include "lib/coro/co_thread_mgr.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace std;

class Add1 : public CoThread {
  void run() override
  {
    ATOMIC_INC(&v_);
    while (!has_set_stop()) {
      ::usleep(100 * 1000L);
    }
  }

public:
  Add1(int& v) : v_(v)
  {}
  int& v_;
};

TEST(TestCoThreadMgr, Add)
{
  int v = 0;
  vector<Add1*> thes;
  for (int i = 0; i < 100; i++) {
    thes.push_back(new Add1(v));
  }

  CoThreadMgr ctm;
  for (auto th : thes) {
    ctm.add_thread(*th);
  }
  ctm.start();
  ctm.stop();
  ctm.wait();
  ASSERT_EQ(100, v);
  for (auto th : thes) {
    delete th;
  }
}

int main(int argc, char* argv[])
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
