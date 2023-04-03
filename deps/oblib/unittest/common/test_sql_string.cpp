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

#include "lib/string/ob_sql_string.h"
#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace common
{

TEST(ObSqlString, ObSqlString)
{
  ObSqlString sql;
  ASSERT_TRUE(sql.empty());
  ASSERT_EQ(0, sql.length());
  ASSERT_EQ(NULL, sql.ptr());

  ObSqlString sql2(common::ObModIds::OB_STRING_BUF);
  sql2.set_label(common::ObModIds::OB_SQL_STRING);
  ASSERT_TRUE(sql2.empty());
  ASSERT_EQ(0, sql2.length());
  ASSERT_EQ(NULL, sql2.ptr());

  const int BUF_LEN = 32;
  char buf[BUF_LEN] = "";

  ASSERT_EQ(0, sql.to_string(buf, sizeof(buf)));
  ASSERT_STREQ("", buf);

  ObString str = sql.string();
  ASSERT_EQ(NULL, str.ptr());
  ASSERT_EQ(0, str.length());

  ASSERT_EQ(0, sql.capacity());
  int ret = sql.reserve(BUF_LEN);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_LE(BUF_LEN, sql.capacity());

  ret = sql.reserve(-1);
  ASSERT_NE(OB_SUCCESS, ret);
}

TEST(ObSqlString, assign)
{
  const char *test_str = "Test str";
  ObSqlString sql;
  sql.assign(test_str);
  ASSERT_FALSE(sql.empty());
  ASSERT_STREQ(test_str, sql.ptr());

  sql.assign(test_str, strlen(test_str));
  ASSERT_STREQ(test_str, sql.ptr());

  sql.assign(ObString::make_string(test_str));
  ASSERT_STREQ(test_str, sql.ptr());

  sql.assign_fmt("%s%s%d", "A", "B", 1);
  ASSERT_STREQ("AB1", sql.ptr());

  char buf[] = { "ABC DEF" };
  sql.assign(buf, sizeof(buf));
  ASSERT_TRUE(sizeof(buf) == sql.length());
  buf[3] = '\0';
  sql.assign(buf, 7);
  ASSERT_STREQ("ABC", sql.ptr());
  ASSERT_STREQ("DEF", sql.ptr() + 4);

  sql.reset();
  ASSERT_TRUE(sql.empty());
  ASSERT_EQ(0, sql.length());
  ASSERT_EQ(NULL, sql.ptr());
}

TEST(ObSqlString, append)
{
  const char *test_str = "Test str";
  std::string str;
  ObSqlString sql;
  sql.append(test_str);
  str.append(test_str);
  ASSERT_STREQ(str.c_str(), sql.ptr());

  sql.append(test_str, strlen(test_str));
  str.append(test_str);
  ASSERT_STREQ(str.c_str(), sql.ptr());

  sql.append(ObString::make_string(test_str));
  str.append(test_str);
  ASSERT_STREQ(str.c_str(), sql.ptr());

  sql.append_fmt("%s%s%d", "A", "B", 1);
  str.append("AB1");
  ASSERT_STREQ(str.c_str(), sql.ptr());
}

TEST(ObSqlString, extend)
{
  ObSqlString sql1;
  ObSqlString sql2;

  std::string str;
  const uint64_t max_len = OB_MALLOC_NORMAL_BLOCK_SIZE * 17;
  str.reserve(max_len);
  const char *test_str = "ABCD";
  while (str.length() < max_len) {
    ASSERT_EQ(OB_SUCCESS, sql1.append(test_str));
    ASSERT_EQ(OB_SUCCESS, sql2.append_fmt("%s", test_str));

    str.append(test_str);
  }

  ASSERT_STREQ(str.c_str(), sql1.ptr());
  ASSERT_STREQ(str.c_str(), sql2.ptr());

  sql1.reset();
  sql2.reset();
  str.clear();
  std::string str2;
  for (int i = 0; i < 23; i++) {
    str2.append(1, 'a' + i);
  }
  while (str.length() < max_len) {
    ASSERT_EQ(OB_SUCCESS, sql1.append(str2.c_str()));
    ASSERT_EQ(OB_SUCCESS, sql2.append_fmt("%s", str2.c_str()));

    str.append(str2);
  }
  ASSERT_STREQ(str.c_str(), sql1.ptr());
  ASSERT_STREQ(str.c_str(), sql2.ptr());
}

TEST(ObSqlString, set_length)
{
  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, sql.set_length(0));
  ASSERT_EQ(0, sql.capacity());

  const char *test_str = "ABCD";
  sql.reserve(10);
  memcpy(sql.ptr(), test_str, strlen(test_str));
  sql.set_length(3);
  ASSERT_EQ(3, sql.length());
  ASSERT_STREQ("ABC", sql.ptr());

  ASSERT_NE(OB_SUCCESS, sql.set_length(-1));
  ASSERT_EQ(OB_SUCCESS, sql.set_length(sql.capacity()));
  ASSERT_NE(OB_SUCCESS, sql.set_length(sql.capacity() + 1));
}

} // end namespace common
} // end namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
