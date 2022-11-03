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

#include <math.h>
#include <string.h>
#include "share/ob_define.h"
#include "common/object/ob_obj_type.h"
#include "common/expression/ob_expr_obj.h"
#include <gtest/gtest.h>
#include "sql/engine/expr/ob_expr_nvl.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

class ObExprNvlTest: public ::testing::Test
{
  public:
    ObExprNvlTest();
    virtual ~ObExprNvlTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObExprNvlTest(const ObExprNvlTest &other);
    ObExprNvlTest& operator=(const ObExprNvlTest &other);
  protected:
    // data members
};

ObExprNvlTest::ObExprNvlTest()
{
}

ObExprNvlTest::~ObExprNvlTest()
{
}

void ObExprNvlTest::SetUp()
{
}

void ObExprNvlTest::TearDown()
{
}
#define EV 0.00001
#define NVL_EXPECT(funop, str_buf, func, type1, v1, type2, v2, restype, res) \
  {                                                         \
    ObObj t1;                                           \
    ObObj t2;                                           \
    ObObj vres;                                         \
    funop op;                                               \
    op.set_result_type(restype);                            \
    t1.set_##type1(v1);                                     \
      t2.set_##type2(v2);                                   \
        int err = op.func(vres, t1, t2, str_buf);                    \
        if (OB_SUCCESS != err)                              \
        {                                                   \
          ASSERT_EQ(res, err);                              \
        }                                                   \
        else                                                \
        {                                                   \
          switch(vres.get_type())                           \
          {                                                 \
          case ObIntType:                                   \
            ASSERT_EQ(res, vres.get_int());                 \
            break;                                          \
          case ObFloatType:                                 \
            ASSERT_TRUE(fabsf(res-vres.get_float()<EV));    \
            break;                                          \
          case ObDoubleType:                                \
            ASSERT_TRUE(fabs(res - vres.get_double())<EV);  \
            break;                                          \
          case ObMaxType:                                   \
            ASSERT_EQ(res, vres.get_type());                \
            break;                                          \
          case ObNullType:                                  \
            ASSERT_EQ(res, vres.get_type());                \
          default:                                          \
            ASSERT_TRUE(vres.is_null());                    \
            break;                                          \
          }                                                 \
        }                                                   \
  } while(0)

#define NVL_EXPECT_VARCHAR(funop, str_buf, func, type1, v1, type2, v2, res) \
  {                                                         \
    ObObj t1;                                           \
    ObObj t2;                                           \
    ObObj vres;                                         \
    funop op;                                               \
    op.set_result_type(ObVarcharType);                      \
    t1.set_##type1(v1);                                     \
      t2.set_##type2(v2);                                   \
        int err = op.func(vres, t1, t2, str_buf);                    \
        if (OB_SUCCESS != err)                              \
        {                                                   \
        }                                                   \
        else                                                \
        {                                                   \
          ASSERT_EQ(ObVarcharType, vres.get_type());            \
          ASSERT_EQ(strlen(res), vres.get_varchar().length());  \
          ASSERT_EQ(0, memcmp(res, vres.get_varchar().ptr(),    \
                              strlen(res)));                    \
        }                                                       \
  } while(0)

#define R(t1, v1, t2, v2, restype, res) NVL_EXPECT(ObExprNvl, &buf, calc_result2, t1, v1, t2, v2, restype, res)
#define RV(t1, v1, t2, v2, res) NVL_EXPECT_VARCHAR(ObExprNvl, &buf, calc_result2, t1, v1, t2, v2, res)

TEST_F(ObExprNvlTest, result_test)
{
  //num with other
  ObExprStringBuf buf;
  R(int, 1, varchar, "hello", varchar, 1);
  R(int, 1, int, 3, ObIntType, 1);
  R(int, 1, float, float(3.2), ObDoubleType, 1.0);
  R(int, 1, double, 3.3, ObDoubleType, 1.0);
  R(int, 1, null, 0, ObIntType, 1);

  R(float, float(1.23), varchar, "hello", varchar, 1.23);
  R(float, float(1.23), int, 3, ObFloatType, 1.230000);
  R(float, float(1.23), float, float(3.2), ObFloatType, 1.230000);
  R(float, float(1.23), double, 3.3, ObDoubleType, 1.230000);
  R(float, float(1.23), null, 0, ObFloatType, 1.230000);

  R(double, 1.23, varchar, "hello", ObDoubleType, 1.230000);
  R(double, 1.23, int, 3, ObDoubleType, 1.230000);
  R(double, 1.23, float, float(3.2), ObDoubleType, 1.230000);
  R(double, 1.23, double, 3.3, ObDoubleType, 1.230000);
  R(double, 1.23, null, 0, ObDoubleType, 1.230000);

  //invalid type op
  R(ctime, 1, int, 2, ObNullType, OB_ERR_INVALID_TYPE_FOR_OP);
  R(mtime, 1, int, 2, ObNullType, OB_ERR_INVALID_TYPE_FOR_OP);
  R(ext, 1, int, 2, ObNullType, OB_ERR_INVALID_TYPE_FOR_OP);

  //varchar with other
  RV(varchar, "hello", int, 2, "hello");
  RV(varchar, "hello", float, float(2), "hello");
  RV(varchar, "hello", double, 2, "hello");
  RV(varchar, "hello", varchar, "2", "hello");
  RV(varchar, "hello", null, 0, "hello");
  RV(varchar, "", varchar, "hello", "");
  RV(varchar, "", double, 2.2, "");
  RV(varchar, "", int, 4, "");
  RV(varchar, "", float, float(2.3), "");
  //R(varchar, "", null, 0, ObNullType, 0);

  //null with other
  R(null, 0, int, 2, ObIntType, 2);
  R(null, 0, float, float(2.2), ObFloatType, 2.20000);
  R(null, 0, double, 2.3, ObDoubleType, 2.30000);
  RV(null, 0, varchar, "hello", "hello");
  R(null, 0, ctime, 2, ObIntType, OB_ERR_INVALID_TYPE_FOR_OP);
  R(null, 0, mtime, 2, ObIntType, OB_ERR_INVALID_TYPE_FOR_OP);
  R(null, 0, ext, 2, ObIntType, OB_ERR_INVALID_TYPE_FOR_OP);
  R(null, 0, null, 0, ObNullType, 0);
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
