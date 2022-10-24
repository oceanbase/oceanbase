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
#include "sql/engine/expr/ob_expr_lower.h"
#include "ob_expr_test_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

class ObExprLowerTest : public ::testing::Test
{
public:
  ObExprLowerTest();
  virtual ~ObExprLowerTest();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  ObExprLowerTest(const ObExprLowerTest &other);
  ObExprLowerTest& operator=(const ObExprLowerTest &other);
private:
  // data members
};
ObExprLowerTest::ObExprLowerTest()
{
}

ObExprLowerTest::~ObExprLowerTest()
{
}

void ObExprLowerTest::SetUp()
{
}

void ObExprLowerTest::TearDown()
{
}


#define EXPECT_RESULT1_NO_PARAM(str_op_object, str_buf, func, type1, ref_type) \
                                        {                               \
                                         ObObj t1;                  \
                                         ObObj r;                   \
                                         ObObj ref;                 \
                                         t1.set_##type1();            \
                                         t1.set_collation_type(CS_TYPE_UTF8MB4_BIN);         \
                                         ref.set_##ref_type(); \
                                         ref.set_collation_type(CS_TYPE_UTF8MB4_BIN);         \
                                         ObExprCtx expr_ctx(NULL, NULL, NULL, str_buf);\
                                         ObExprResType res_type;        \
                                         res_type.set_collation_type(CS_TYPE_UTF8MB4_BIN);\
                                         str_op_object.set_result_type(res_type);\
                                         int err = str_op_object.func(r, t1, expr_ctx); \
                                         _OB_LOG(INFO, "text=%s expect=%s result=%s", to_cstring(t1), to_cstring(ref), to_cstring(r)); \
                                         EXPECT_TRUE(OB_SUCCESS == err); \
                                         ASSERT_TRUE(ref.get_type() == r.get_type()); \
                                         if (ref.get_type() != ObNullType) \
                                         { \
                                           EXPECT_TRUE(ObObjCmpFuncs::compare_oper_nullsafe(ref, r, CS_TYPE_UTF8MB4_BIN, CO_EQ));\
                                         } \
                                        } while(0)
#define EXPECT_FAIL_RESULT1_NO_PARAM(str_op_object, str_buf, func, type1)                \
                                    {                                               \
                                      ObObj t1;                                     \
                                      ObObj r;                                      \
                                      ObObj ref;                                    \
                                      t1.set_##type1();                           \
                                      ObExprCtx expr_ctx(NULL, NULL, NULL, str_buf);\
                                      int err = str_op_object.func(r, t1, expr_ctx); \
                                      ASSERT_TRUE(OB_SUCCESS != err); \
                                    } while(0)
#define T(obj, t1, v1, ref_type, ref_value) EXPECT_RESULT1(obj, &buf, calc_result1, t1, v1, ref_type, ref_value)
#define T_NO_PARAM(obj, t1, ref_type) EXPECT_RESULT1_NO_PARAM(obj, &buf, calc_result1, t1, ref_type)
#define F(obj, t1, v1, ref_type, ref_value) EXPECT_FAIL_RESULT1(obj, &buf, calc_result1, t1, v1)
#define F_NO_PARAM(obj, t1) EXPECT_FAIL_RESULT1_NO_PARAM(obj, &buf, calc_result1, t1)


TEST_F(ObExprLowerTest, basic_test)
{
  ObMalloc buf;
  ObExprLower lower(buf);
  ASSERT_EQ(1, lower.get_param_num());

  // null
  T_NO_PARAM(lower, null, null);
  T(lower, varchar, "ABC", varchar, "abc");
  T(lower, varchar, "123", varchar, "123");
  T(lower, varchar, " aBcD 09 %d#$", varchar, " abcd 09 %d#$");
}

TEST_F(ObExprLowerTest, fail_test)
{
  ObArenaAllocator alloc;
  ObExprLower lower(alloc);
  ObExprStringBuf buf;
  ASSERT_EQ(1, lower.get_param_num());

  F(lower, int, 123, varchar, "123");
  F(lower, int, 10, varchar, "10");
  F(lower, int, 0, varchar, "0");
  F(lower, double, 10.2, varchar, "10.200000");
  F(lower, bool, true, varchar, "true");
  F(lower, bool, false, varchar, "false");
  F_NO_PARAM(lower, max_value);
  F_NO_PARAM(lower, min_value);
}


int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
