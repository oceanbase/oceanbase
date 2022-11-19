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
#include "sql/engine/expr/ob_expr_case.h"
#include "sql/engine/expr/ob_expr_arg_case.h"
#include "ob_expr_test_utils.h"
#include "sql/engine/expr/ob_expr_equal.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

class ObExprCaseTest : public ::testing::Test
{
public:
  ObExprCaseTest();
  virtual ~ObExprCaseTest();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  ObExprCaseTest(const ObExprCaseTest &other);
  ObExprCaseTest& operator=(const ObExprCaseTest &other);
private:
  // data members
};
ObExprCaseTest::ObExprCaseTest()
{
}

ObExprCaseTest::~ObExprCaseTest()
{
}

void ObExprCaseTest::SetUp()
{
}

void ObExprCaseTest::TearDown()
{
}

#define MAKE_OBJS_2(t1,v1,t2,v2) \
  ObObj objs[2]; \
  ObExprResType types[2]; \
  objs[0].set_##t1(v1); objs[1].set_##t2(v2);  \
  types[0].set_##t1(); types[1].set_##t2(); 

#define MAKE_OBJS_3(t1,v1,t2,v2,t3,v3) \
  ObObj objs[3]; \
  ObExprResType types[3]; \
  objs[0].set_##t1(v1); objs[1].set_##t2(v2); objs[2].set_##t3(v3); \
  types[0].set_##t1(); types[1].set_##t2(); types[2].set_##t3();

#define MAKE_OBJS_4(t1,v1,t2,v2,t3,v3,t4,v4) \
  ObObj objs[4]; \
  ObExprResType types[4]; \
  objs[0].set_##t1(v1); objs[1].set_##t2(v2); objs[2].set_##t3(v3); objs[3].set_##t4(v4); \
  types[0].set_##t1(); types[1].set_##t2(); types[2].set_##t3(); types[3].set_##t4();

#define MAKE_OBJS_5(t1,v1,t2,v2,t3,v3,t4,v4,t5,v5) \
  ObObj objs[5]; \
  ObExprResType types[5]; \
  objs[0].set_##t1(v1); objs[1].set_##t2(v2); objs[2].set_##t3(v3); objs[3].set_##t4(v4); objs[4].set_##t5(v5); \
  types[0].set_##t1(); types[1].set_##t2(); types[2].set_##t3(); types[3].set_##t4(); types[4].set_##t5();


#define CHECK_FUNC(obj, func, r, N) \
  ObExprResType type; \
  ObExprTypeCtx ctx; \
  int err = obj.calc_result_typeN(type, types, N, ctx); \
  EXPECT_TRUE(OB_SUCCESS == err); \
  obj.set_result_type(type); \
  err = obj.calc_resultN(r,objs, N, &buf); \
  _OB_LOG(INFO, "r=%s ref=%s", to_cstring(r), to_cstring(ref)); \
  EXPECT_TRUE(OB_SUCCESS == err); \
  EXPECT_TRUE(ref.get_type() == r.get_type());  \
  if (ref.get_type() != ObNullType){\
    ObExprEqual eq; \
    ObObj result; \
    EXPECT_TRUE(OB_SUCCESS == ObExprEqual::calc(result, r, ref)); \
    EXPECT_TRUE(result.get_type() == ObBoolType  && result.get_bool() == true); \
  }

#define CHECK_FUNC_FAIL(obj, func, r, N) \
  ObExprResType type; \
  ObExprTypeCtx ctx; \
  int err = obj.calc_result_typeN(type, types, N, ctx); \
  err = obj.calc_resultN(r, objs, N, &buf); \
  _OB_LOG(INFO, "r=%s ref=%s", to_cstring(r), to_cstring(ref)); \
  EXPECT_TRUE(OB_SUCCESS != err);



#define T_2(obj,func, t1, v1, t2, v2, ref_type, ref_value) \
{ \
  ObObj r, ref;      \
  MAKE_OBJS_2(t1,v1,t2,v2) \
  ref.set_##ref_type(ref_value); \
  CHECK_FUNC(obj,func, r, 2) \
}while(0)

#define F_2(obj,func, t1, v1, t2, v2, ref_type, ref_value) \
{ \
  ObObj r, ref;      \
  MAKE_OBJS_2(t1,v1,t2,v2) \
  ref.set_##ref_type(ref_value); \
  CHECK_FUNC_FAIL(obj,func, r, 2) \
}while(0)


#define T_3(obj,func, t1, v1, t2, v2, t3, v3, ref_type, ref_value) \
{ \
  ObObj r, ref;      \
  MAKE_OBJS_3(t1,v1,t2,v2,t3,v3) \
  ref.set_##ref_type(ref_value); \
  CHECK_FUNC(obj,func, r, 3) \
}while(0)

#define F_3(obj,func, t1, v1, t2, v2, t3, v3, ref_type, ref_value) \
{ \
  ObObj r, ref;      \
  MAKE_OBJS_3(t1,v1,t2,v2,t3,v3) \
  ref.set_##ref_type(ref_value); \
  CHECK_FUNC_FAIL(obj,func, r, 3) \
}while(0)



#define T_4(obj, func, t1, v1, t2, v2, t3, v3, t4,v4,ref_type, ref_value) \
{ \
  ObObj r, ref;      \
  MAKE_OBJS_4(t1,v1,t2,v2,t3,v3,t4,v4) \
  ref.set_##ref_type(ref_value); \
  CHECK_FUNC(obj, func, r, 4) \
}while(0)

#define F_4(obj,func, t1, v1, t2, v2, t3, v3,t4,v4, ref_type, ref_value) \
{ \
  ObObj r, ref;      \
  MAKE_OBJS_4(t1,v1,t2,v2,t3,v3,t4,v4) \
  ref.set_##ref_type(ref_value); \
  CHECK_FUNC_FAIL(obj,func, r, 4) \
}while(0)



#define T_5(obj, func, t1, v1, t2, v2, t3, v3, t4, v4, t5, v5, ref_type, ref_value) \
{ \
  ObObj r, ref;      \
  MAKE_OBJS_5(t1,v1,t2,v2,t3,v3,t4,v4,t5,v5) \
  ref.set_##ref_type(ref_value); \
  CHECK_FUNC(obj,func, r, 5) \
}while(0)

#define F_5(obj,func, t1, v1, t2, v2, t3, v3,t4,v4 ,t5,v5, ref_type, ref_value) \
{ \
  ObObj r, ref;      \
  MAKE_OBJS_5(t1,v1,t2,v2,t3,v3,t4,v4,t5,v5) \
  ref.set_##ref_type(ref_value); \
  CHECK_FUNC_FAIL(obj,func, r, 5) \
}while(0)




TEST_F(ObExprCaseTest, basic_test)
{
  ObExprCase case_op;
  ObExprStringBuf buf;
  ASSERT_EQ(ObExprOperator::MORE_THAN_ONE, case_op.get_param_num());


  // case when a then b [else c]
  T_2(case_op, calc_resultN, bool, true, null, 0, null, 0);
  T_2(case_op, calc_resultN, bool, false, varchar, "hello", null, 0);
  T_2(case_op, calc_resultN, bool, true, varchar, "hello", varchar, "hello");
  T_4(case_op, calc_resultN, bool, false, varchar, "hello", bool, true, int, 100, varchar, "100");
  T_4(case_op, calc_resultN, bool, true, varchar, "hello", bool, true, int, 100, varchar, "hello");
  T_4(case_op, calc_resultN, bool, false, varchar, "hello", bool, true, varchar, "hello", varchar, "hello");
  T_4(case_op, calc_resultN, bool, true, int, 100, bool, false, varchar, "hello", varchar, "100");
  T_4(case_op, calc_resultN, bool, false, varchar, "hello", bool, false, int, 100, null, 0);
  T_4(case_op, calc_resultN, bool, false, varchar, "hello", bool, false, int, 100, null, 0);

  // else
  // T_5(case_op, calc_resultN, bool, false, varchar, "hello", bool, false, int, 100, int, 99, int, 99);
  T_5(case_op, calc_resultN, bool, false, varchar, "hello", bool, false, int, 100, int, 99, varchar, "99");
  //T_5(case_op, calc_resultN, bool, false, int, 35, bool, false, int, 100, int, 99, int, 99);


  // case when a then b
  F_3(case_op, calc_resultN, int, 10, bool, true, null, 0, int, 10);
  // case when a then b else c
  T_3(case_op, calc_resultN, null, 0, null, 0, null, 0, null, 0);
  T_5(case_op, calc_resultN, bool, false, varchar, "hello", bool, false, int, 100, int, 50, varchar, "50");
}


TEST_F(ObExprCaseTest, arg_case_basic_test)
{
  ObExprArgCase case_op;
  ObExprStringBuf buf;
  ASSERT_EQ(ObExprOperator::MORE_THAN_ONE, case_op.get_param_num());

  // case x when a then b else c
  F_2(case_op, calc_resultN, int, 10, bool, true, null, 0);
  F_2(case_op, calc_resultN, bool, true, varchar, "hello", varchar, "hello");

  T_4(case_op, calc_resultN, int, 10, int, 9, null, 0, null, 0, null, 0);
  T_4(case_op, calc_resultN, int, 10, int, 9, null, 0, int, 7, int , 7);
  T_4(case_op, calc_resultN, int, 10, int, 9, null, 0, varchar, "xyz", varchar, "xyz");
  //T_4(case_op, calc_resultN, int, 10, int, 9, null, 0, bool, true, bool, true);
  T_3(case_op, calc_resultN, bool, false, bool, false, varchar, "hello",  varchar, "hello");
  T_3(case_op, calc_resultN, bool, true, bool, false, varchar, "hello",  null, 0);
  T_5(case_op, calc_resultN, bool, false, bool, true, varchar, "hello", bool, false, int, 100, varchar, "100");
  T_5(case_op, calc_resultN, bool, true, bool, true, varchar, "hello", bool, false, int, 100, varchar, "hello");

  // case x when a then b
  T_3(case_op, calc_resultN, null, 0, null, 0, null, 0, null, 0);
  // case x when a then b else c
  T_4(case_op, calc_resultN, null, 0, null, 0, int, 100, int, 99, int, 99);
  T_4(case_op, calc_resultN, null, 0, int, 0, int, 100, int, 99, int, 99);
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

