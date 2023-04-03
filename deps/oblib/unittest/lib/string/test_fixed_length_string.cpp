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

#define USING_LOG_PREFIX LIB

#include <gtest/gtest.h>

#include "lib/ob_define.h"
#include "lib/string/ob_fixed_length_string.h"

namespace oceanbase
{
namespace common
{
TEST(TestFixedLengthString, common)
{
  // construct functions
  typedef ObFixedLengthString<32> TestString;
  TestString str;
  TestString str1("test");
  TestString str2 = str1;
  ASSERT_EQ(str1, str2);
  ASSERT_NE(str, str1);
  str = str1;
  ASSERT_EQ(str, str1);

  // compare functions
  TestString str3("test3");
  TestString str4("test4");
  ASSERT_TRUE(str3 < str4);
  ASSERT_TRUE(str4 > str3);
  ASSERT_TRUE(str3 != str4);
  ASSERT_FALSE(str3.is_empty());
  TestString str5;
  ASSERT_TRUE(str5.is_empty());

  // length
  ASSERT_EQ(5, str3.size());
  ASSERT_EQ(0, strcmp("test3", str3.ptr()));
}

TEST(TestFixedLengthString, error)
{
  typedef ObFixedLengthString<16> TestString;
  // paramenter is null
  TestString null_str(NULL);
  ASSERT_TRUE(null_str.is_empty());
  TestString str1("test");
  TestString str2("test");
  ASSERT_EQ(OB_SUCCESS, str1.assign(NULL));
  ASSERT_EQ(str1, null_str);

  // overflow
  TestString str3;
  ASSERT_EQ(OB_BUF_NOT_ENOUGH, str3.assign("oceanbase is a distributed database"));
  ASSERT_EQ(0, strcmp("oceanbase is a ", str3.ptr()));

  // to_string
  LIB_LOG(INFO, "for test", K(null_str), K(str1), K(str2), K(str3));

  // serialize and deserialize
  char buf[32];
  int64_t data_len = str3.get_serialize_size();
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, str3.serialize(buf, sizeof(buf), pos));
  ASSERT_EQ(data_len, pos);
  TestString str4;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, str4.deserialize(buf, data_len, pos));
  ASSERT_EQ(data_len, pos);
  ASSERT_EQ(str3, str4);
}
}//end namespace common
}//end namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
