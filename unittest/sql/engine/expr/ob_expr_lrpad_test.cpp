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

#define protected public
#include <gtest/gtest.h>
#include "sql/engine/expr/ob_expr_lrpad.h"
#include "ob_expr_test_utils.h"
#include "lib/worker.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::share;

class ObExprLRpadTest : public ::testing::Test
{
public:
  ObExprLRpadTest();
  virtual ~ObExprLRpadTest();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  ObExprLRpadTest(const ObExprLRpadTest &other);
  ObExprLRpadTest& operator=(const ObExprLRpadTest &other);
private:
  // data members
};
ObExprLRpadTest::ObExprLRpadTest()
{
}

ObExprLRpadTest::~ObExprLRpadTest()
{
}

void ObExprLRpadTest::SetUp()
{
}

void ObExprLRpadTest::TearDown()
{
}

#define T(obj, t1, v1, ref_type, ref_value) EXPECT_RESULT1(obj, &buf, calc_result1, t1, v1, ref_type, ref_value)
#define F(obj, t1, v1, ref_type, ref_value) EXPECT_FAIL_RESULT1(obj, &buf, calc_result1, t1, v1, ref_type, ref_value)

TEST_F(ObExprLRpadTest, lpad_oracle_test)
{
  ObArenaAllocator allocator;
  ObExprOracleLpad lpad(allocator);
  // LPAD('〝〔',3)
  char str[] = "〝〔";
  THIS_WORKER.set_compatibility_mode(lib::Worker::CompatMode::ORACLE);
  ObObj text;
  ObString(strlen(str), str);
  text.set_char("〝〔");
  text.set_collation_type(CS_TYPE_UTF8MB4_BIN);

  ObObj length;
  number::ObNumber num;
  num.from((int64_t)3, allocator);
  length.set_number(num);

  ObObj result;

  // ObExprStringBuf str_buf;
  ObExprCtx expr_ctx;
  expr_ctx.calc_buf_ = &allocator;

  ObObj pad_text;
  pad_text.set_string(ObVarcharType, " ", 1L);

  text.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  lpad.result_type_.set_collation_type(text.get_collation_type());
  lpad.result_type_.set_collation_level(text.get_collation_level());

  lpad.calc(ObExprBaseLRpad::LPAD_TYPE, text, length, pad_text, expr_ctx, result);
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

