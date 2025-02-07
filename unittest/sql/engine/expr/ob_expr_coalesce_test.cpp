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
#include "sql/engine/expr/ob_expr_coalesce.h"
#include "ob_expr_test_utils.h"
#include "sql/engine/expr/ob_expr_equal.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

class ObExprCoalesceTest : public ::testing::Test
{
public:
  ObExprCoalesceTest();
  virtual ~ObExprCoalesceTest();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  ObExprCoalesceTest(const ObExprCoalesceTest &other);
  ObExprCoalesceTest& operator=(const ObExprCoalesceTest &other);
private:
  // data members
};
ObExprCoalesceTest::ObExprCoalesceTest()
{
}

ObExprCoalesceTest::~ObExprCoalesceTest()
{
}

void ObExprCoalesceTest::SetUp()
{
}

void ObExprCoalesceTest::TearDown()
{
}

#define MAKE_OBJS_1(t1,v1) \
  ObObj objs[1]; \
  ObExprResType types[1]; \
  objs[0].set_##t1(v1); \
  types[0].set_##t1(); 

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
  ObCStringHelper helper; \
  _OB_LOG(INFO, "err=%d type=%s", err, helper.convert(type)); \
  obj.set_result_type(type); \
  err = obj.calc_resultN(r,objs, N, &buf); \
  _OB_LOG(INFO, "r=%s ref=%s", helper.convert(r), helper.convert(ref)); \
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
  ObCStringHelper helper; \
  _OB_LOG(INFO, "err=%d type=%s", err, helper.convert(type)); \
  obj.set_result_type(type); \
  err = obj.calc_resultN(r, objs, N, &buf); \
  _OB_LOG(INFO, "r=%s ref=%s", helper.convert(r), helper.convert(ref)); \
  EXPECT_TRUE(OB_SUCCESS != err);



#define T_1(obj,func, t1, v1, ref_type, ref_value) \
{ \
  ObObj r, ref;      \
  MAKE_OBJS_1(t1,v1) \
  ref.set_##ref_type(ref_value); \
  CHECK_FUNC(obj,func, r, 1) \
}while(0)


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




TEST_F(ObExprCoalesceTest, basic_test)
{
  ObExprCoalesce coalesce_op;
  ObExprStringBuf buf;
  ASSERT_EQ(ObExprOperator::MORE_THAN_ZERO, coalesce_op.get_param_num());


  T_1(coalesce_op, calc_resultN, null, 0, null, 0);
  T_1(coalesce_op, calc_resultN, int, 10, int, 10);
  T_1(coalesce_op, calc_resultN, float, 10, float, 10);
  T_1(coalesce_op, calc_resultN, double, 10, double, 10);
  T_1(coalesce_op, calc_resultN, varchar, "10", varchar, "10");
  T_1(coalesce_op, calc_resultN, precise_datetime, 1000, precise_datetime, 1000);

  T_2(coalesce_op, calc_resultN, bool, true, null, 0, bool, true);
  T_2(coalesce_op, calc_resultN, null, 0, bool, true, bool, true);
  T_2(coalesce_op, calc_resultN, null, 0, bool, false, bool, false);
  T_2(coalesce_op, calc_resultN, bool, true, bool, false, bool, true);

  //T_2(coalesce_op, calc_resultN, float, 10.0, null, 0, float, 10.0);
  //T_2(coalesce_op, calc_resultN, null, 0, float, 10.0, float, 10.0);
  //T_2(coalesce_op, calc_resultN, int, 12, float, 10.0, float, 12.0);
  //T_2(coalesce_op, calc_resultN, int, 12, double, 10.0, double, 12.0);
  //T_2(coalesce_op, calc_resultN, int, 12, int, 10, int, 12);
  //T_2(coalesce_op, calc_resultN, double, 12, double, 10.0, double, 12.0);
  //T_2(coalesce_op, calc_resultN, float, 12, double, 10.0, double, 12.0);
  T_2(coalesce_op, calc_resultN, mtime,1000, precise_datetime, 10002, precise_datetime, 1000);
  T_5(coalesce_op, calc_resultN, bool, true, bool, false, bool, true, bool, false, bool, false, bool, true);
  T_5(coalesce_op, calc_resultN, int, 10, int, 20, int, 0, int, -20, int, -20, int, 10);
  //T_5(coalesce_op, calc_resultN, int, 10, int, 20, float, 0, int, -20, int, -20, float, 10);
  //T_5(coalesce_op, calc_resultN, int, 10, int, 20, float, 0, int, -20, double, -20, double, 10);
}

TEST_F(ObExprCoalesceTest, fail_test)
{
  ObExprCoalesce coalesce_op;
  ObExprStringBuf buf;

  // not support datetime type anymore
  //F_1(coalesce_op, calc_resultN, datetime,1000, datetime, 1000);
  F_2(coalesce_op, calc_resultN, varchar,"2012-11-12", precise_datetime, 10002, varchar, "2012-11-12");
  F_3(coalesce_op, calc_resultN, null, 0, varchar,"2012-11-12", precise_datetime, 10002, varchar, "2012-11-12");
  F_3(coalesce_op, calc_resultN, null, 0, precise_datetime, 10002, varchar, "xyz", varchar, "1970-01-01 08:00:00.010002");
  F_3(coalesce_op, calc_resultN, null, 0, varchar, "xyz", precise_datetime, 10002, varchar, "xyz");
  F_4(coalesce_op, calc_resultN, null, 0, bool, true, varchar, "xyz", precise_datetime, 10002, varchar, "true");
}


int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

