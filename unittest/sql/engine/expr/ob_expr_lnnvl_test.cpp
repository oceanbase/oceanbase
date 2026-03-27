/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "sql/engine/expr/ob_expr_lnnvl.h"
#include "ob_expr_test_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

class ObExprLnnvlTest: public ::testing::Test
{
  public:
    ObExprLnnvlTest();
    virtual ~ObExprLnnvlTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObExprLnnvlTest(const ObExprLnnvlTest &other);
    ObExprLnnvlTest& operator=(const ObExprLnnvlTest &other);
  protected:
    // data members
};

ObExprLnnvlTest::ObExprLnnvlTest()
{
}

ObExprLnnvlTest::~ObExprLnnvlTest()
{
}

void ObExprLnnvlTest::SetUp()
{
}

void ObExprLnnvlTest::TearDown()
{
}

#define EXPECT_EQUAL(lnnvl_func, buf, test_obj, result_obj, input_type, input_value, ret_value) \
{   \
  test_obj.set_##input_type(input_value); \
  OB_ASSERT(OB_SUCCESS == lnnvl_func.calc_result1(result_obj, test_obj, buf));\
  OB_ASSERT(ret_value == result_obj.get_bool()); \
} while(0)

#define EXPECT_EQUAL_NULL(lnnvl_func, buf, test_obj, result_obj, input_type, ret_value) \
{   \
  test_obj.set_##input_type(); \
  OB_ASSERT(OB_SUCCESS == lnnvl_func.calc_result1(result_obj, test_obj, buf));\
  OB_ASSERT(ret_value == result_obj.get_bool()); \
} while(0)
TEST_F(ObExprLnnvlTest, bool_value_test)
{
  ObExprStringBuf buf;
  ObArenaAllocator alloc;
  ObExprFuncLnnvl lnnvl_func(alloc);
  ObExprCtx expr_ctx(NULL, NULL, NULL, &buf);
  ObObj test_obj, result_obj;
  //
  test_obj.set_bool(true);
  OB_ASSERT(OB_SUCCESS == lnnvl_func.calc_result1(result_obj, test_obj, expr_ctx));
  OB_ASSERT(false == result_obj.get_bool());

  test_obj.set_bool(false);
  OB_ASSERT(OB_SUCCESS == lnnvl_func.calc_result1(result_obj, test_obj, expr_ctx));
  OB_ASSERT(true == result_obj.get_bool());

  test_obj.set_null();
  OB_ASSERT(OB_SUCCESS == lnnvl_func.calc_result1(result_obj, test_obj, expr_ctx));
  OB_ASSERT(true == result_obj.get_bool());

  test_obj.set_int(10);
  OB_ASSERT(OB_SUCCESS == lnnvl_func.calc_result1(result_obj, test_obj, expr_ctx));
  OB_ASSERT(false == result_obj.get_bool());
  //EXPECT_EQUAL(lnnvl_func, &buf, test_obj, result_obj, bool, true, false);
  //EXPECT_EQUAL(lnnvl_func, &buf, test_obj, result_obj, bool, false, true);
  //EXPECT_EQUAL_NULL(lnnvl_func, &buf, test_obj, result_obj, null, true);
}


int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
