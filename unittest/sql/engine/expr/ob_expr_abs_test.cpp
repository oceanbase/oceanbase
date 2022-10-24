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
#include "sql/engine/expr/ob_expr_abs.h"
#include "sql/engine/expr/ob_expr_test_utils.h"
#include <math.h>

using namespace oceanbase::common;
using namespace oceanbase::sql;
TEST_OPERATOR(ObExprAbs);
class TestObExprAbsTest: public ::testing::Test
{
public:
  TestObExprAbsTest();
  virtual ~TestObExprAbsTest();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  TestObExprAbsTest(const TestObExprAbsTest &other);
  TestObExprAbsTest& operator=(const TestObExprAbsTest &other);
protected:
  // data members
};

TestObExprAbsTest::TestObExprAbsTest()
{
}

TestObExprAbsTest::~TestObExprAbsTest()
{
}

void TestObExprAbsTest::SetUp()
{
}

void TestObExprAbsTest::TearDown()
{
}
#define EV 0.00001
#define ARITH_EXPECT_TYPE_WITH_ROW(cmp_op, func, type1, res)  \
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

#define ARITH_EXPECT_TYPE(cmp_op, func, type1, res) \
  {                                                 \
   ObExprResType t1;                                \
   ObExprResType vres;                              \
   ObExprTypeCtx ctx;                              \
   cmp_op op;                                       \
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
    cmp_op op;                                        \
    t1.set_type(type1);                               \
    int err = op.func(vres, t1, ctx);                 \
    ASSERT_EQ(OB_ERR_INVALID_TYPE_FOR_OP, err);       \
  }while(0)

#define ARITH_ERROR(cmp_op, str_buf, func, type1, v1,  res)  \
  {                                                 \
    ObObj t1;                                   \
    ObObj vres;                                 \
    cmp_op op;                                      \
    t1.set_##type1(v1);                             \
    int err = op.func(vres, t1, str_buf);                    \
    ASSERT_EQ(res, err);                            \
  }while(0)

#define ARITH_EXPECT(cmp_op, str_buf, func, type1, v1, res)        \
      {                                                   \
       ObObj t1;                                      \
       ObObj vres;                                    \
       cmp_op op;                                         \
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
           break;                                         \
         default:                                         \
           ASSERT_TRUE(vres.is_null());                   \
           break;                                         \
         }                                                \
       }                                                  \
      } while(0)


#define ARITH_ABS_VARCHAR_EXPECT(cmp_op, str_buf, func, type1, v1, res, sprintf_type)        \
      {                                                   \
       ObObj t1;                                      \
       ObObj vres;                                    \
       cmp_op op;                                         \
       t1.set_##type1(v1);                                \
       ObExprStringBuf temp_buf;                          \
       char num_str[512];                                 \
       sprintf(num_str,sprintf_type,res);                 \
       number::ObNumber res_num;                          \
       res_num.from(num_str,temp_buf);                    \
                                                          \
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
         case ObNumberType:                              \
           ASSERT_EQ(res_num, vres.nmb_);                 \
           break;                                         \
         default:                                         \
           ASSERT_TRUE(vres.is_null());                   \
           break;                                         \
         }                                                \
       }                                                  \
      } while(0)

#define R(t1, v1, res) ARITH_EXPECT(TestObExprAbs, &buf, calc_result1, t1, v1, res)
#define E(t1, v1, res) ARITH_ERROR(TestObExprAbs, &buf, calc_result1, t1, v1, res)
#define T(t1, res) ARITH_EXPECT_TYPE(TestObExprAbs, calc_result_type1, t1, res)
#define TE(t1, res) ARITH_EXPECT_TYPE_WITH_ROW(TestObExprAbs, calc_result_type1, t1, res)
#define TYE(t1) ARITH_EXPECT_TYPE_ERROR(TestObExprAbs, calc_result_type1, t1)
#define AAVE(t1, v1, res,sprintf_type) ARITH_ABS_VARCHAR_EXPECT(TestObExprAbs, &buf, calc_result1, t1, v1, res, sprintf_type)

TEST_F(TestObExprAbsTest, type_test)
{
  T(ObNullType, ObDoubleType);
  T(ObTinyIntType, ObIntType);
  T(ObSmallIntType, ObIntType);
  T(ObMediumIntType, ObIntType);
  T(ObInt32Type, ObIntType);
  T(ObIntType, ObIntType);
  T(ObUTinyIntType, ObIntType);
  T(ObUSmallIntType, ObIntType);
  T(ObUMediumIntType, ObIntType);
  T(ObUInt32Type, ObIntType);
  T(ObUInt64Type, ObIntType);
  T(ObFloatType, ObDoubleType);
  T(ObDoubleType, ObDoubleType);
  T(ObUFloatType, ObDoubleType);
  T(ObUDoubleType, ObDoubleType);
  T(ObNumberType, ObNumberType);
  T(ObUNumberType, ObNumberType);
  T(ObDateTimeType, ObDoubleType);
  T(ObTimestampType, ObDoubleType);
  T(ObDateType, ObDoubleType);
  T(ObTimeType, ObDoubleType);
  T(ObYearType, ObIntType);
  T(ObVarcharType, ObDoubleType);
  T(ObCharType, ObDoubleType);
  //TE(ObHexStringType, OB_ERR_INVALID_TYPE_FOR_OP);
  //TE(ObExtendType, OB_ERR_INVALID_TYPE_FOR_OP);
  //TE(ObUnknownType, OB_ERR_INVALID_TYPE_FOR_OP);
}

TEST_F(TestObExprAbsTest, result_test)
{
  ObExprStringBuf buf;
  R(null, 0, ObNullType);
  R(int, 1, 1);
  R(int, -1, 1);
  R(int, 0, 0);
  R(float, float(1.2),  1.2);
  R(float, float(-2.1), 2.1);
  R(float, float(-0.0), 0.0);
  R(double, 1.2,  1.2);
  R(double, -2.1, 2.1);
  R(double, -0.0, 0.0);
  R(datetime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  R(datetime, -1, OB_ERR_INVALID_TYPE_FOR_OP);
  R(datetime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  R(precise_datetime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  R(precise_datetime, -1, OB_ERR_INVALID_TYPE_FOR_OP);
  R(precise_datetime, 0, OB_ERR_INVALID_TYPE_FOR_OP);

  AAVE(varchar, "-34", 34, "%d");
  AAVE(varchar, "dfer", OB_ERR_CAST_VARCHAR_TO_NUMBER,"%d");
  AAVE(varchar, "-0.3434dgfg", OB_ERR_CAST_VARCHAR_TO_NUMBER,"%d");
  AAVE(varchar, "-0.0", 0.0, "%f");
  AAVE(varchar, "-1.9999", 1.9999, "%lf");
  AAVE(varchar, "7777777777777777", 7777777777777777, "%ld");

  R(ctime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  R(ctime, -1, OB_ERR_INVALID_TYPE_FOR_OP);
  R(ctime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  R(mtime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  R(mtime, -1, OB_ERR_INVALID_TYPE_FOR_OP);
  R(mtime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E(ext, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  R(bool, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  R(bool, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  R(bool, -122, OB_ERR_INVALID_TYPE_FOR_OP);
  R(bool, 122, OB_ERR_INVALID_TYPE_FOR_OP);

}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
