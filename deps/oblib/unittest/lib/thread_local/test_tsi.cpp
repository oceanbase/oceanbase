/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "lib/thread_local/ob_tsi_factory.h"

using namespace oceanbase::common;
using namespace std;

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}


TEST(TestTsi, TestName)
{
  static int des_count = 0;
  struct S {
    ~S() {
      cout << "destructor S" << endl;
      des_count++;
    }
  };
  struct SS {
    ~SS() {
      cout << des_count << endl;
    }
  };
  GET_TSI(SS);
  S *sp = GET_TSI(S[2])[0];
  (void)(sp);
  ASSERT_TRUE(sp);
}
