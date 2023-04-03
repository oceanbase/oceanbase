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
#include "sql/engine/expr/ob_expr_not.h"
#include "ob_expr_test_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

class ObExprNotTest: public ::testing::Test
{
  public:
    ObExprNotTest();
    virtual ~ObExprNotTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObExprNotTest(const ObExprNotTest &other);
    ObExprNotTest& operator=(const ObExprNotTest &other);
  protected:
    // data members
};

ObExprNotTest::ObExprNotTest()
{
}

ObExprNotTest::~ObExprNotTest()
{
}

void ObExprNotTest::SetUp()
{
}

void ObExprNotTest::TearDown()
{
}

#define EV 0.00001
#define LOGIC_EXPECT_TYPE_WITH_ROW(cmp_op, func, type1, res)  \
  {                                                           \
    ObExprResType t1;                                         \
    ObExprResType vres;                                       \
    ObExprTypeCtx ctx;                                        \
    cmp_op op;                                                \
    op.set_row_dimension(1);                                  \
    t1.set_type(type1);                                       \
    int err = op.func(vres, t1, ctx);                         \
    ASSERT_EQ(res, err);                                      \
  }while(0)

#define LOGIC_EXPECT_TYPE(cmp_op, func, type1, res) \
  {                                                 \
   ObExprResType t1;                                \
   ObExprResType vres;                              \
   ObExprTypeCtx ctx;                               \
   cmp_op op;                                       \
   t1.set_type(type1);                              \
   int err = op.func(vres, t1, ctx);                \
   if (OB_SUCCESS == err)                           \
   {                                                \
    ASSERT_EQ(res, vres.get_type());                \
    }                                               \
   else                                             \
   {                                                \
    ASSERT_EQ(res, err);                            \
    }                                               \
   }while(0)

#define LOGIC_ERROR(cmp_op, func, type1, v1, res)            \
  {                                                          \
    ObObj t1;                                            \
    ObObj vres;                                          \
    cmp_op op;                                               \
    t1.set_##type1(v1);                                      \
      int err = op.func(vres, t1);                           \
      ASSERT_EQ(, err);                                      \
  }while(0)

#define LOGIC_EXPECT(cmp_op, str_buf, func, type1, v1, res)  \
  {                                                 \
    ObObj t1;                                   \
    ObObj vres;                                 \
    cmp_op op;                                      \
    t1.set_##type1(v1);                             \
      int err = op.func(vres, t1, str_buf);                  \
      if (OB_SUCCESS == err)                        \
      {                                             \
        if (ObBoolType == vres.get_type())          \
        {                                           \
          ASSERT_EQ(res, vres.get_bool());          \
        }                                           \
        else                                        \
        {                                           \
          ASSERT_EQ(res, vres.get_type());          \
        }                                           \
      }                                             \
      else                                          \
      {                                             \
        ASSERT_EQ(res, err);                        \
      }                                             \
  } while(0)
#define R(t1, v1, res)  LOGIC_EXPECT(ObExprNot, &buf, calc_result1, t1, v1, res)
#define T(t1, res) LOGIC_EXPECT_TYPE(ObExprNot, calc_result_type1, t1, res)
#define TE(t1, res) LOGIC_EXPECT_TYPE_WITH_ROW(ObExprNot, calc_result_type1, t1, res)

TEST_F(ObExprNotTest, type_test)
{
  TE(ObNullType, OB_ERR_INVALID_TYPE_FOR_OP);
  TE(ObNullType, OB_ERR_INVALID_TYPE_FOR_OP);
  TE(ObNullType, OB_ERR_INVALID_TYPE_FOR_OP);
  TE(ObIntType, OB_ERR_INVALID_TYPE_FOR_OP);
  TE(ObFloatType, OB_ERR_INVALID_TYPE_FOR_OP);
  TE(ObDoubleType, OB_ERR_INVALID_TYPE_FOR_OP);
  TE(ObDateTimeType, OB_ERR_INVALID_TYPE_FOR_OP);
  TE(ObPreciseDateTimeType, OB_ERR_INVALID_TYPE_FOR_OP);
  TE(ObVarcharType, OB_ERR_INVALID_TYPE_FOR_OP);
  TE(ObUnknownType, OB_ERR_INVALID_TYPE_FOR_OP);
  TE(ObCreateTimeType, OB_ERR_INVALID_TYPE_FOR_OP);
  TE(ObModifyTimeType, OB_ERR_INVALID_TYPE_FOR_OP);
  TE(ObExtendType, OB_ERR_INVALID_TYPE_FOR_OP);
  TE(ObBoolType, OB_ERR_INVALID_TYPE_FOR_OP);
  TE(ObDecimalType, OB_ERR_INVALID_TYPE_FOR_OP);

  T(ObNullType, ObNullType);
  T(ObIntType, ObBoolType);
  T(ObFloatType, ObBoolType);
  T(ObDoubleType, ObBoolType);
  T(ObDateTimeType, OB_ERR_INVALID_TYPE_FOR_OP);
  T(ObPreciseDateTimeType, OB_ERR_INVALID_TYPE_FOR_OP);
  T(ObVarcharType, ObBoolType);
  T(ObUnknownType, ObBoolType);
  T(ObCreateTimeType, OB_ERR_INVALID_TYPE_FOR_OP);
  T(ObModifyTimeType, OB_ERR_INVALID_TYPE_FOR_OP);
  T(ObExtendType, OB_ERR_INVALID_TYPE_FOR_OP);
  T(ObBoolType, ObBoolType);
  T(ObDecimalType, ObDecimalType);
}

TEST_F(ObExprNotTest, result_test)
{
  ObExprStringBuf buf;
  R(null, 0, ObNullType);
  R(int, 1, 0);
  R(int, -1, 0);
  R(int, 0, 1);
  R(float, float(1.2), 0);
  R(float, float(-2.1), 0);
  R(float, float(-0.0), 1);
  R(double, 1.2, 0);
  R(double, -2.1, 0);
  R(double, -0.0, 1);
  R(datetime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  R(datetime, -1, OB_ERR_INVALID_TYPE_FOR_OP);
  R(datetime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  R(precise_datetime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  R(precise_datetime, -1, OB_ERR_INVALID_TYPE_FOR_OP);
  R(precise_datetime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  R(varchar, "-34", OB_ERR_CAST_VARCHAR_TO_BOOL);
  R(varchar, "dfer", OB_ERR_CAST_VARCHAR_TO_BOOL);
  R(varchar, "-0.3434dgfg", OB_ERR_CAST_VARCHAR_TO_BOOL);
  R(varchar, "0", 1);
  R(varchar, "1", 0);
  R(varchar, "false", 1);
  R(varchar, "true", 0);
  R(ctime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  R(ctime, -1, OB_ERR_INVALID_TYPE_FOR_OP);
  R(ctime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  R(mtime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  R(mtime, -1, OB_ERR_INVALID_TYPE_FOR_OP);
  R(mtime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  R(ext, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  R(bool, 1, 0);
  R(bool, 0, 1);
  R(bool, -122, 0);
  R(bool, 122, 0);
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
