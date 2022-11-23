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
#include "sql/engine/expr/ob_expr_md5.h"
#include "ob_expr_test_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

class ObExprMd5Test : public ::testing::Test
{
public:
  ObExprMd5Test();
  virtual ~ObExprMd5Test();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  ObExprMd5Test(const ObExprMd5Test &other);
  ObExprMd5Test& operator=(const ObExprMd5Test &other);
private:
  // data members
};
ObExprMd5Test::ObExprMd5Test()
{
}

ObExprMd5Test::~ObExprMd5Test()
{
}

void ObExprMd5Test::SetUp()
{
}

void ObExprMd5Test::TearDown()
{
}

#define T(obj, t1, v1, ref_type, ref_value) EXPECT_RESULT1(obj, &buf, calc_result1, t1, v1, ref_type, ref_value)
#define F(obj, t1, v1, ref_type, ref_value) EXPECT_FAIL_RESULT1(obj, &buf, calc_result1, t1, v1, ref_type, ref_value)

TEST_F(ObExprMd5Test, basic_test)
{
  ObExprMd5 md5;
  ObExprStringBuf buf;
  ASSERT_EQ(1, md5.get_param_num());

  // null
  //T(md5, null, 0, null, 0);
  // empty
  T(md5, varchar, "", varchar, "d41d8cd98f00b204e9800998ecf8427e");
  // single char with case sensitive
  T(md5, varchar, "a", varchar, "0cc175b9c0f1b6a831c399e269772661");
  T(md5, varchar, "A", varchar, "7fc56270e7a70fa81a5935b72eacbe29");
  T(md5, varchar, "0", varchar, "cfcd208495d565ef66e7dff9f98764da");
  T(md5, varchar, "1", varchar, "c4ca4238a0b923820dcc509a6f75849b");
  T(md5, varchar, "@", varchar, "518ed29525738cebdac49c49e60ea9d3");
  T(md5, varchar, "#", varchar, "01abfc750a0c942167651c40d088531d");
  T(md5, varchar, " ", varchar, "d41d8cd98f00b204e9800998ecf8427e");
  // normal english word
  T(md5, varchar, "abcdefg",  varchar, "7ac66c0f148de9519b8bd264312c4d64");
  T(md5, varchar, "abcdefg ", varchar, "cea1a161b6f38ac9189d204a6ff0e231");
  T(md5, varchar, "Good morning", varchar, "4e44298897ed12cdc10e5302fa781688");
  T(md5, varchar, "Good evening", varchar, "0a28b30e5d84d83d385b6b9afd5661ae");
  // ip like
  T(md5, varchar, "255.255.255.255", varchar, "eea88cd0d9a7ba26282fc786713bbbb6");
  T(md5, varchar, "192.168.1.100", varchar, "d984a05fa268b7cc6ac052a38960aeb2");
  T(md5, varchar, "30.32.204.180", varchar, "8f70c7dd37a827875dd5e8bfb59b001e");
  // email like
  T(md5, varchar, "emily@163.com", varchar, "3def867c78b2b87744512a6cbbc1d730");
  T(md5, varchar, "mike@sohu.com", varchar, "91f49bb2c81568dcc32f74d9f1cb5811");
  T(md5, varchar, "joey@sina.com", varchar, "59014c527f2387d23490b78e6b973c37");
  // chinese word
  T(md5, varchar, "阿里巴巴", varchar, "49edab1cb53ba3cf77c6c3271196acbb");
  T(md5, varchar, "阿里巴巴 ", varchar, "a5ca9dd3dc1f3857eee9c2daaeba130b");
  T(md5, varchar, "淘宝", varchar, "12ad5c790444f88966c2faf90e73d8c9");
  T(md5, varchar, "淘宝 ", varchar, "e04842fc8f74042c581063da43e6adb9");
}

TEST_F(ObExprMd5Test, fail_test)
{
  ObExprMd5 md5;
  ObExprStringBuf buf;
  ASSERT_EQ(1, md5.get_param_num());

//  F(md5, max, 0, null, 0);
//  F(md5, min, 0, null, 0);
//  F(md5, double, 10.2, null, 0);
//  F(md5, bool, true, null, 0);
//  F(md5, int, 10, null, 0);
//  F(md5, int, 0, null, 0);
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

