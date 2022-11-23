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
#include "sql/engine/expr/ob_expr_neg.h"
#include <math.h>
using namespace oceanbase::common;
using namespace oceanbase::sql;

class ObExprNegTest: public ::testing::Test
{
public:
  ObExprNegTest();
  virtual ~ObExprNegTest();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  ObExprNegTest(const ObExprNegTest &other);
  ObExprNegTest& operator=(const ObExprNegTest &other);
protected:
  // data members
};

ObExprNegTest::ObExprNegTest()
{
}

ObExprNegTest::~ObExprNegTest()
{
}

void ObExprNegTest::SetUp()
{
}

void ObExprNegTest::TearDown()
{
}
#define EV 0.00001
#define ARITH_EXPECT_TYPE_WITH_ROW(cmp_op, func, type1, res)  \
  {                                                           \
    ObExprResType t1;                                         \
    ObExprResType vres;                                       \
    ObExprTypeCtx ctx;                                        \
    ObArenaAllocator alloc;\
    cmp_op op(alloc);                                                \
    op.set_row_dimension(1);                                  \
    t1.set_type(type1);                                       \
    int err = op.func(vres, t1, ctx);                         \
    ASSERT_EQ(res, err);                                      \
  }while(0)

#define ARITH_EXPECT_TYPE(cmp_op, func, type1, res) \
  {                                                 \
   ObExprResType t1;                                \
   ObExprResType vres;                              \
   ObExprTypeCtx ctx;                                \
ObArenaAllocator alloc;\
   cmp_op op(alloc);                                       \
   t1.set_type(type1);                              \
   int err = op.func(vres, t1, ctx);                \
   ASSERT_EQ(OB_SUCCESS, err);                      \
   ASSERT_EQ(res, vres.get_type());                 \
   }while(0)

#define ARITH_EXPECT_TYPE_ERROR(cmp_op, func, type1)  \
  {                                                   \
    ObExprResType t1;                                 \
    ObExprResType vres;                               \
    ObExprTypeCtx ctx;                                \
    ObArenaAllocator alloc;\
    cmp_op op(alloc);                                        \
    t1.set_type(type1);                               \
    int err = op.func(vres, t1, ctx);                 \
    ASSERT_EQ(OB_ERR_INVALID_TYPE_FOR_OP, err);       \
  }while(0)

#define ARITH_ERROR(cmp_op, str_buf, func, type1, v1,  res)  \
  {                                                 \
    ObObj t1;                                   \
    ObObj vres;                                 \
    ObArenaAllocator alloc;\
    cmp_op op(alloc);                                      \
    t1.set_##type1(v1);                             \
      int err = op.func(vres, t1, str_buf);                  \
      ASSERT_EQ(res, err);                          \
  }while(0)

#define ARITH_EXPECT(cmp_op, str_buf, func, type1, v1, res)        \
      {                                                   \
       ObObj t1;                                      \
       ObObj vres;                                    \
       ObArenaAllocator alloc;\
        cmp_op op(alloc);                                         \
       t1.set_##type1(v1);                                \
       int err = op.func(vres, t1, str_buf);                       \
       if (OB_SUCCESS != err)                             \
       {                                                  \
        ASSERT_EQ(res, err);                              \
       }                                                  \
       else                                               \
       {                                                  \
         ASSERT_EQ(OB_SUCCESS, err);                      \
         switch(vres.get_type())                          \
         {                                                \
         case ObIntType:                                  \
           ASSERT_EQ(res, vres.get_int());                \
           break;                                         \
         case ObDateTimeType:                             \
           ASSERT_EQ(res, vres.get_datetime());           \
           break;                                         \
         case ObPreciseDateTimeType:                      \
           ASSERT_EQ(res, vres.get_precise_datetime());   \
           break;                                         \
         case ObCreateTimeType:                           \
           ASSERT_EQ(res, vres.get_ctime());              \
           break;                                         \
         case ObModifyTimeType:                           \
           ASSERT_EQ(res, vres.get_mtime());              \
           break;                                         \
         case ObBoolType:                                 \
           ASSERT_EQ(res, vres.get_bool());               \
           break;                                         \
         case ObFloatType:                                \
           ASSERT_TRUE(fabsf(res-vres.get_float()<EV));   \
           break;                                         \
         case ObDoubleType:                               \
           ASSERT_TRUE(fabs(res - vres.get_double())<EV); \
           break;                                         \
         case ObMaxType:                                  \
           ASSERT_EQ(res, vres.get_type());               \
           break;                                         \
         case ObNullType:                                 \
           ASSERT_EQ(res, vres.get_type());               \
         default:                                         \
           ASSERT_TRUE(vres.is_null());                   \
           break;                                         \
         }                                                \
       }                                                  \
      } while(0)

#define R(t1, v1, res) ARITH_EXPECT(ObExprNeg, &buf, calc_result1, t1, v1, res)
#define E(t1, v1, res) ARITH_ERROR(ObExprNeg, &buf, calc_result1, t1, v1, res)
#define T(t1, res) ARITH_EXPECT_TYPE(ObExprNeg, calc_result_type1, t1, res)
#define TE(t1, res) ARITH_EXPECT_TYPE_WITH_ROW(ObExprNeg, calc_result_type1, t1, res)
#define TYE(t1) ARITH_EXPECT_TYPE_ERROR(ObExprNeg, calc_result_type1, t1)
      TEST_F(ObExprNegTest, type_test)
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
        T(ObIntType, ObIntType);
        T(ObFloatType, ObFloatType);
        T(ObDoubleType, ObDoubleType);
        TYE(ObDateTimeType);
        TYE(ObPreciseDateTimeType);
        TYE(ObVarcharType);
        //TYE(ObUnknownType);
        TYE(ObCreateTimeType);
        TYE(ObModifyTimeType);
        TYE(ObExtendType);
        T(ObBoolType, ObBoolType);
        //T(ObDecimalType, ObDecimalType);
      }

TEST_F(ObExprNegTest, result_test)
{
  ObExprStringBuf buf;
  R(null, 0, ObNullType);
  R(int, 1, -1);
  R(int, -1, 1);
  R(int, 0, 0);
  R(float, float(1.2), -1.2);
  R(float, float(-2.1), 2.1);
  R(float, float(-0.0), 0.0);
  R(double, 1.2, -1.2);
  R(double, -2.1, 2.1);
  R(double, -0.0, 0.0);
  R(datetime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  R(datetime, -1, OB_ERR_INVALID_TYPE_FOR_OP);
  R(datetime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  R(precise_datetime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  R(precise_datetime, -1, OB_ERR_INVALID_TYPE_FOR_OP);
  R(precise_datetime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  R(varchar, "-34", OB_ERR_INVALID_TYPE_FOR_OP);
  R(varchar, "dfer", OB_ERR_INVALID_TYPE_FOR_OP);
  R(varchar, "-0.3434dgfg", OB_ERR_INVALID_TYPE_FOR_OP);
  R(varchar, "-0.0", OB_ERR_INVALID_TYPE_FOR_OP);
  R(ctime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  R(ctime, -1, OB_ERR_INVALID_TYPE_FOR_OP);
  R(ctime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  R(mtime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  R(mtime, -1, OB_ERR_INVALID_TYPE_FOR_OP);
  R(mtime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E(ext, 0, OB_ERR_INVALID_TYPE_FOR_OP);
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
