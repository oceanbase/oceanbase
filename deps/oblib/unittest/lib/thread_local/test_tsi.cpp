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
