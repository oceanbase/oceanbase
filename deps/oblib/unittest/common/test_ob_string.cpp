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
#include <string>

#include "lib/string/ob_string.h"

namespace oceanbase {
namespace common {


TEST(ObString, set_length)
{
  ObString TestString;
  ASSERT_EQ(0, TestString.length());
  ASSERT_EQ(0, TestString.size());
  TestString.set_length(5);
  ASSERT_EQ(0, TestString.length());
  ASSERT_EQ(0, TestString.size());

  const char* test_str = "ABCD";
  TestString = ObString(test_str);
  ASSERT_EQ(4, TestString.length());
  TestString.set_length(5);
  ASSERT_EQ(5, TestString.length());
  ASSERT_NE(5, TestString.size());

  TestString = ObString(4,strlen(test_str) , test_str);
  TestString.set_length(5);
  ASSERT_NE(5, TestString.length());
  ASSERT_EQ(4, TestString.size());

  TestString = ObString(6,strlen(test_str) , test_str);
  TestString.set_length(5);
  ASSERT_EQ(5, TestString.length());
  ASSERT_EQ(6, TestString.size());
 
}

}  // end namespace common
}  // end namespace oceanbase

int main(int argc, char** argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
