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
#include "sql/engine/expr/ob_expr_concat.h"
#include "sql/engine/expr/ob_expr_equal.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;

class ObExprConcatTest : public ::testing::Test
{
public:
  ObExprConcatTest();
  virtual ~ObExprConcatTest();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  ObExprConcatTest(const ObExprConcatTest &other);
  ObExprConcatTest& operator=(const ObExprConcatTest &other);
private:
  // data members
};
ObExprConcatTest::ObExprConcatTest()
{
}

ObExprConcatTest::~ObExprConcatTest()
{
}

void ObExprConcatTest::SetUp()
{
}

void ObExprConcatTest::TearDown()
{
}

#define T(obj, t1, v1, t2, v2, ref_type, ref_value) EXPECT_RESULTN(obj, &buf, calc_resultN, t1, v1, t2, v2, ref_type, ref_value)
#define T_NULL(obj, t1, t2, v2, ref_type, ref_value) EXPECT_RESULT_WITH_NULL(obj, &buf, calc_resultN, t1, t2, v2, ref_type, ref_value)
#define F(obj, t1, v1, t2, v2, ref_type, ref_value) EXPECT_FAIL_RESULTN(obj, &buf, calc_resultN, t1, v1, t2, v2, ref_type, ref_value)

#define EXPECT_RESULT_WITH_NULL(str_op_object, str_buf, func, type1, type2, v2, ref_type, ref_value) \
                                        do {                               \
                                         UNUSED(ref_value);  \
                                          ObObj array[2]; \
                                          ObObj r;                  \
                                          ObObj ref;                \
                                          array[0].set_##type1();           \
                                          array[1].set_##type2(v2);         \
                                          ref.set_##ref_type(); \
                                          ObExprCtx expr_ctx(NULL, NULL, NULL, str_buf);\
                                          int err = str_op_object.func(r, array, 2, expr_ctx); \
                                          _OB_LOG(INFO, "respect=%s result=%s", to_cstring(ref), to_cstring(r)); \
                                          EXPECT_TRUE(OB_SUCCESS == err); \
                                          EXPECT_TRUE(ref.get_type() == r.get_type()); \
                                          EXPECT_TRUE(ref.get_type() == ObNullType); \
                                        } while(0)

#define EXPECT_FAIL_RESULTN(str_op_object, str_buf, func, type1, v1, type2, v2) \
                                        do {                               \
                                         ObObj array[2];  \
                                         ObObj r;                   \
                                         ObObj ref;                 \
                                         array[0].set_##type1(v1);            \
                                         array[1].set_##type2(v2);            \
                                         ObExprCtx expr_ctx(NULL, NULL, NULL, str_buf);            \
                                         int err = str_op_object.func(r, array, 2, expr_ctx); \
                                         ASSERT_TRUE(OB_SUCCESS != err); \
                                         } while(0)


#define EXPECT_RESULTN(str_op_object, str_buf, func, type1, v1, type2, v2,ref_type, ref_value) \
                                        do {                               \
                                          ObObj array[2]; \
                                          ObObj r;                  \
                                          ObObj ref;                \
                                          array[0].set_##type1(v1);           \
                                          array[1].set_##type2(v2);         \
                                          ref.set_##ref_type(ref_value); \
                                          ObExprCtx expr_ctx(NULL, NULL, NULL, str_buf);            \
                                          int err = str_op_object.func(r, array, 2, expr_ctx); \
                                          _OB_LOG(INFO, "ref=%s r=%s", to_cstring(ref), to_cstring(r)); \
                                          EXPECT_TRUE(OB_SUCCESS == err); \
                                          EXPECT_TRUE(ref.get_type() == r.get_type()); \
                                          if (ref.get_type() != ObNullType) \
                                          { \
                                             EXPECT_TRUE(ObObjCmpFuncs::compare_oper_nullsafe(ref, r, CS_TYPE_UTF8MB4_BIN, CO_EQ));\
                                          } \
                                        } while(0)



TEST_F(ObExprConcatTest, basic_test)
{
  ObArenaAllocator buf(ObModIds::OB_SQL_SESSION);
  ObExprConcat concat(buf);

  // input not varchar
  T_NULL(concat, null, int, 0, null, 0);
  //内部不容许出现
  //T_NULL(concat, max_value, int, 0, null, 0);
  //T_NULL(concat, min_value, int, 10, null, 0);
  T_NULL(concat, null, double, 10,  null, 0);
  T_NULL(concat, null, bool, true, null, 0);
  T_NULL(concat, null, bool, false, null, 0);
  T_NULL(concat, null, varchar, "hi", null, 0);
  T_NULL(concat, null, datetime, 10, null, 0);


  // ok
  T(concat, varchar, "41424344", varchar, "ABCD", varchar, "41424344ABCD");
  T(concat, varchar, "61626364", varchar, "abcd", varchar, "61626364abcd");
  T(concat, varchar, "4E4F", varchar, "NO", varchar, "4E4FNO");
  T(concat, varchar, "4E4F", varchar, "", varchar, "4E4F");
  T(concat, varchar, "", varchar, "4E4F", varchar, "4E4F");
  T(concat, varchar, "", varchar, "", varchar, "");
  T(concat, varchar, "abc", int, 123, varchar, "abc123");
  T(concat, varchar, "abc  ", int, 123, varchar, "abc  123");
  T(concat, varchar, "a", int, -0, varchar, "a0");
  T(concat, varchar, "1", varchar, "", varchar, "1");
  T(concat, int, 1, int, 1, varchar, "11");
  T(concat, int, -1, varchar, "ab", varchar, "-1ab" );
  //精度暂时不考虑
  //T(concat, varchar, "a", float, -4.5, varchar, "a-4.5");
  //T(concat, varchar, "abc", double, 123.1200, varchar, "abc123.1200");
  //T(concat, varchar, "a", float , -0.00, varchar, "a.0.00");
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
