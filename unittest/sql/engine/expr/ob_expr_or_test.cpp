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
#include "sql/engine/expr/ob_expr_or.h"
#include "ob_expr_test_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;


class ObExprOrTest: public ::testing::Test
{
  public:
    ObExprOrTest();
    virtual ~ObExprOrTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObExprOrTest(const ObExprOrTest &other);
    ObExprOrTest& operator=(const ObExprOrTest &other);
  protected:
    // data members
};

ObExprOrTest::ObExprOrTest()
{
}

ObExprOrTest::~ObExprOrTest()
{
}

void ObExprOrTest::SetUp()
{
}

void ObExprOrTest::TearDown()
{
}

//calc result
#define R2(t1, v1, t2, v2, rtype, res) LOGIC_EXPECT2(ObExprOr, &buf, calc_result2, t1, v1, t2, v2, rtype, res)
#define R3(t1, v1, t2, v2, t3, v3, rtype, res) LOGIC_EXPECT3(ObExprOr, &buf, calc_result3, t1, v1, t2, v2, t3, v3, rtype, res)
#define RN(num, rtype, res, ...) LOGIC_EXPECTN(ObExprOr, num, rtype, res, __VA_ARGS__)

//calc result error
#define E2(t1, v1, t2, v2, res) LOGIC_ERROR2(ObExprOr, &buf, calc_result2, t1, v1, t2, v2, res)
#define E3(t1, v1, t2, v2, t3, v3, res) LOGIC_ERROR3(ObExprOr, &buf, calc_result3, t1, v1, t2, v2, t3, v3, res)
#define EN(num, res, ...) LOGIC_ERRORN(ObExprOr, num, res,  __VA_ARGS__)

//calc type
#define T2(t1, t2, res) LOGIC_EXPECT_TYPE2(ObExprOr, calc_result_type2, t1, t2, res)
#define T3(t1, t2, t3,res) LOGIC_EXPECT_TYPE3(ObExprOr, calc_result_type3, t1, t2, t3, res)
#define TN(num, res, ...) LOGIC_EXPECT_TYPEN(ObExprOr, num, res, __VA_ARGS__)

TEST_F(ObExprOrTest, result2_test)
{
  //calc result2
  //null type
  /* lt   lv  rt   rv  restype     res.get_bool()*/
  ObExprStringBuf buf;
  R2(null, 0, null, 0, ObNullType, OB_OBJ_TYPE_ERROR);
  R2(null, 0, int, 1, ObBoolType, 1);
  R2(null, 0, float, 1.0, ObBoolType, 1);
  R2(null, 0, double, 2.0, ObBoolType, 1);
  E2(null, 0, datetime, 2, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(null, 0, precise_datetime, 2, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(null, 0, varchar, "hello", OB_ERR_CAST_VARCHAR_TO_BOOL);
  E2(null, 0, unknown, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(null, 0, ctime, 2, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(null, 0, mtime, 2, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(null, 0, ext, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  R2(null, 0, bool, 0, ObNullType, OB_OBJ_TYPE_ERROR);

  //int type
  R2(int, 0, null, 0, ObNullType, OB_OBJ_TYPE_ERROR);
  R2(int, 1, null, 0, ObBoolType, 1);
  R2(int, 0, int, 0, ObBoolType, 0);
  R2(int, 0, int, 1, ObBoolType, 1);
  R2(int, 1, int, 0, ObBoolType, 1);
  R2(int, 1, int, 1, ObBoolType, 1);
  R2(int, 0, float, 0.0, ObBoolType, 0);
  R2(int, 0, float, 1.0, ObBoolType, 1);
  R2(int, 1, float, 0.0, ObBoolType, 1);
  R2(int, 1, float, 1.0, ObBoolType, 1);
  R2(int, 0, double, 0.0, ObBoolType, 0);
  R2(int, 0, double, 1.0, ObBoolType, 1);
  R2(int, 1, double, 0.0, ObBoolType, 1);
  R2(int, 1, double, 1.0, ObBoolType, 1);
  E2(int, 0, datetime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(int, 0, datetime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(int, 1, datetime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(int, 1, datetime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(int, 0, precise_datetime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(int, 0, precise_datetime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(int, 1, precise_datetime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(int, 1, precise_datetime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  R2(int, 0, varchar, "0", ObBoolType, 0);
  R2(int, 0, varchar, "1", ObBoolType, 1);
  R2(int, 1, varchar, "0", ObBoolType, 1);
  R2(int, 1, varchar, "1", ObBoolType, 1);
  E2(int, 1, varchar, "f1", OB_ERR_CAST_VARCHAR_TO_BOOL);
  E2(int, 0, ctime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(int, 0, ctime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(int, 1, ctime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(int, 1, ctime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(int, 0, mtime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(int, 0, mtime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(int, 1, mtime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(int, 1, mtime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(int, 0, ext, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(int, 0, ext, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(int, 1, ext, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(int, 1, ext, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  R2(int, 0, bool, 0, ObBoolType, 0);
  R2(int, 0, bool, 1, ObBoolType, 1);
  R2(int, 1, bool, 0, ObBoolType, 1);
  R2(int, 1, bool, 1, ObBoolType, 1);

  //float type
  R2(float, float(0.0), null, 0, ObNullType, OB_OBJ_TYPE_ERROR);
  R2(float, float(0.1), null, 0, ObBoolType, 1);
  R2(float, float(0.0), int, 0, ObBoolType, 0);
  R2(float, float(0.0), int, 1, ObBoolType, 1);
  R2(float, float(0.1), int, 0, ObBoolType, 1);
  R2(float, float(0.1), int, 1, ObBoolType, 1);
  R2(float, float(0.0), float, 0.0, ObBoolType, 0);
  R2(float, float(0.0), float, 1.0, ObBoolType, 1);
  R2(float, float(0.1), float, 0.0, ObBoolType, 1);
  R2(float, float(0.1), float, 1.0, ObBoolType, 1);
  R2(float, float(0.0), double, 0.0, ObBoolType, 0);
  R2(float, float(0.0), double, 1.0, ObBoolType, 1);
  R2(float, float(0.1), double, 0.0, ObBoolType, 1);
  R2(float, float(0.1), double, 1.0, ObBoolType, 1);
  E2(float, float(0.0), datetime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(float, float(0.0), datetime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(float, float(0.1), datetime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(float, float(0.1), datetime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(float, float(0.0), precise_datetime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(float, float(0.0), precise_datetime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(float, float(0.1), precise_datetime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(float, float(0.1), precise_datetime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  R2(float, float(0.0), varchar, "0", ObBoolType, 0);
  R2(float, float(0.0), varchar, "1", ObBoolType, 1);
  R2(float, float(0.1), varchar, "0", ObBoolType, 1);
  R2(float, float(0.1), varchar, "1", ObBoolType, 1);
  E2(float, float(0.1), varchar, "1d", OB_ERR_CAST_VARCHAR_TO_BOOL);
  E2(float, float(0.0), ctime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(float, float(0.0), ctime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(float, float(0.1), ctime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(float, float(0.1), ctime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(float, float(0.0), mtime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(float, float(0.0), mtime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(float, float(0.1), mtime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(float, float(0.1), mtime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(float, float(0.0), ext, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(float, float(0.0), ext, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(float, float(0.1), ext, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(float, float(0.1), ext, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  R2(float, float(0.0), bool, 0, ObBoolType, 0);
  R2(float, float(0.0), bool, 1, ObBoolType, 1);
  R2(float, float(0.1), bool, 0, ObBoolType, 1);
  R2(float, float(0.1), bool, 1, ObBoolType, 1);

  //double
  R2(double, 0.0, null, 0, ObNullType, OB_OBJ_TYPE_ERROR);
  R2(double, 1.0, null, 0, ObBoolType, 1);
  R2(double, 0.0, int, 0, ObBoolType, 0);
  R2(double, 0.0, int, 1, ObBoolType, 1);
  R2(double, 1.0, int, 0, ObBoolType, 1);
  R2(double, 1.0, int, 1, ObBoolType, 1);
  R2(double, 0.0, float, 0.0, ObBoolType, 0);
  R2(double, 0.0, float, 1.0, ObBoolType, 1);
  R2(double, 1.0, float, 0.0, ObBoolType, 1);
  R2(double, 1.0, float, 1.0, ObBoolType, 1);
  R2(double, 0.0, double, 0.0, ObBoolType, 0);
  R2(double, 0.0, double, 1.0, ObBoolType, 1);
  R2(double, 1.0, double, 0.0, ObBoolType, 1);
  R2(double, 1.0, double, 1.0, ObBoolType, 1);
  E2(double, 0.0, datetime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(double, 0.0, datetime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(double, 1.0, datetime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(double, 1.0, datetime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(double, 0.0, precise_datetime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(double, 0.0, precise_datetime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(double, 1.0, precise_datetime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(double, 1.0, precise_datetime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  R2(double, 0.0, varchar, "0", ObBoolType, 0);
  R2(double, 0.0, varchar, "1", ObBoolType, 1);
  R2(double, 1.0, varchar, "0", ObBoolType, 1);
  R2(double, 1.0, varchar, "false", ObBoolType, 1);
  R2(double, 1.0, varchar, "1", ObBoolType, 1);
  R2(double, 1.0, varchar, "true", ObBoolType, 1);
  E2(double, 1.0, varchar, "21", OB_ERR_CAST_VARCHAR_TO_BOOL);
  E2(double, 0.0, ctime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(double, 0.0, ctime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(double, 1.0, ctime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(double, 1.0, ctime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(double, 0.0, mtime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(double, 0.0, mtime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(double, 1.0, mtime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(double, 1.0, mtime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(double, 0.0, ext, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(double, 0.0, ext, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(double, 1.0, ext, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(double, 1.0, ext, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  R2(double, 0.0, bool, 0, ObBoolType, 0);
  R2(double, 0.0, bool, 1, ObBoolType, 1);
  R2(double, 1.0, bool, 0, ObBoolType, 1);
  R2(double, 1.0, bool, 1, ObBoolType, 1);

  E2(datetime, 0, null, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(datetime, 1, null, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(datetime, 0, int, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(datetime, 0, int, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(datetime, 1, int, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(datetime, 1, int, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(datetime, 0, float, 0.0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(datetime, 0, float, 1.0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(datetime, 1, float, 0.0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(datetime, 1, float, 1.0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(datetime, 0, double, 0.0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(datetime, 0, double, 1.0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(datetime, 1, double, 0.0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(datetime, 1, double, 1.0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(datetime, 0, datetime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(datetime, 0, datetime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(datetime, 1, datetime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(datetime, 1, datetime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(datetime, 0, precise_datetime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(datetime, 0, precise_datetime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(datetime, 1, precise_datetime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(datetime, 1, precise_datetime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(datetime, 0, varchar, "0", OB_ERR_INVALID_TYPE_FOR_OP);
  E2(datetime, 0, varchar, "1", OB_ERR_INVALID_TYPE_FOR_OP);
  E2(datetime, 1, varchar, "0", OB_ERR_INVALID_TYPE_FOR_OP);
  E2(datetime, 1, varchar, "1", OB_ERR_INVALID_TYPE_FOR_OP);
  E2(datetime, 0, ctime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(datetime, 0, ctime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(datetime, 1, ctime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(datetime, 1, ctime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(datetime, 0, mtime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(datetime, 0, mtime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(datetime, 1, mtime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(datetime, 1, mtime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(datetime, 0, ext, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(datetime, 0, ext, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(datetime, 1, ext, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(datetime, 1, ext, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(datetime, 0, bool, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(datetime, 0, bool, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(datetime, 1, bool, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(datetime, 1, bool, 1, OB_ERR_INVALID_TYPE_FOR_OP);

  E2(precise_datetime, 0, null, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(precise_datetime, 1, null, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(precise_datetime, 0, int, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(precise_datetime, 0, int, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(precise_datetime, 1, int, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(precise_datetime, 1, int, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(precise_datetime, 0, float, 0.0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(precise_datetime, 0, float, 1.0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(precise_datetime, 1, float, 0.0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(precise_datetime, 1, float, 1.0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(precise_datetime, 0, double, 0.0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(precise_datetime, 0, double, 1.0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(precise_datetime, 1, double, 0.0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(precise_datetime, 1, double, 1.0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(precise_datetime, 0, datetime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(precise_datetime, 0, datetime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(precise_datetime, 1, datetime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(precise_datetime, 1, datetime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(precise_datetime, 0, precise_datetime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(precise_datetime, 0, precise_datetime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(precise_datetime, 1, precise_datetime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(precise_datetime, 1, precise_datetime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(precise_datetime, 0, varchar, "0", OB_ERR_INVALID_TYPE_FOR_OP);
  E2(precise_datetime, 0, varchar, "1", OB_ERR_INVALID_TYPE_FOR_OP);
  E2(precise_datetime, 1, varchar, "0", OB_ERR_INVALID_TYPE_FOR_OP);
  E2(precise_datetime, 1, varchar, "1", OB_ERR_INVALID_TYPE_FOR_OP);
  E2(precise_datetime, 0, ctime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(precise_datetime, 0, ctime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(precise_datetime, 1, ctime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(precise_datetime, 1, ctime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(precise_datetime, 0, mtime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(precise_datetime, 0, mtime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(precise_datetime, 1, mtime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(precise_datetime, 1, mtime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(precise_datetime, 0, ext, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(precise_datetime, 0, ext, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(precise_datetime, 1, ext, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(precise_datetime, 1, ext, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(precise_datetime, 0, bool, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(precise_datetime, 0, bool, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(precise_datetime, 1, bool, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(precise_datetime, 1, bool, 1, OB_ERR_INVALID_TYPE_FOR_OP);

  R2(varchar, "0", null, 0, ObNullType, OB_OBJ_TYPE_ERROR);
  R2(varchar, "1", null, 0, ObBoolType, 1);
  R2(varchar, "0", int, 0, ObBoolType, 0);
  R2(varchar, "0", int, 1, ObBoolType, 1);
  R2(varchar, "1", int, 0, ObBoolType, 1);
  R2(varchar, "1", int, 1, ObBoolType, 1);
  R2(varchar, "0", float, 0.0, ObBoolType, 0);
  R2(varchar, "0", float, 1.0, ObBoolType, 1);
  R2(varchar, "1", float, 0.0, ObBoolType, 1);
  R2(varchar, "1", float, 1.0, ObBoolType, 1);
  R2(varchar, "0", double, 0.0, ObBoolType, 0);
  R2(varchar, "0", double, 1.0, ObBoolType, 1);
  R2(varchar, "1", double, 0.0, ObBoolType, 1);
  R2(varchar, "1", double, 1.0, ObBoolType, 1);
  E2(varchar, "0", datetime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(varchar, "0", datetime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(varchar, "1", datetime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(varchar, "1", datetime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(varchar, "0", precise_datetime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(varchar, "0", precise_datetime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(varchar, "1", precise_datetime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(varchar, "1", precise_datetime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  R2(varchar, "0", varchar, "0", ObBoolType, 0);
  R2(varchar, "0", varchar, "1", ObBoolType, 1);
  R2(varchar, "1", varchar, "0", ObBoolType, 1);
  R2(varchar, "1", varchar, "1", ObBoolType, 1);
  E2(varchar, "2", varchar, "1", OB_ERR_CAST_VARCHAR_TO_BOOL);
  E2(varchar, "0", ctime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(varchar, "0", ctime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(varchar, "1", ctime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(varchar, "1", ctime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(varchar, "0", mtime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(varchar, "0", mtime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(varchar, "1", mtime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(varchar, "1", mtime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(varchar, "0", ext, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(varchar, "0", ext, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(varchar, "1", ext, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(varchar, "1", ext, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  R2(varchar, "0", bool, 0, ObBoolType, 0);
  R2(varchar, "0", bool, 1, ObBoolType, 1);
  R2(varchar, "1", bool, 0, ObBoolType, 1);
  R2(varchar, "1", bool, 1, ObBoolType, 1);

  E2(ctime, 0, null, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ctime, 1, null, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ctime, 0, int, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ctime, 0, int, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ctime, 1, int, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ctime, 1, int, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ctime, 0, float, 0.0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ctime, 0, float, 1.0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ctime, 1, float, 0.0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ctime, 1, float, 1.0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ctime, 0, double, 0.0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ctime, 0, double, 1.0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ctime, 1, double, 0.0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ctime, 1, double, 1.0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ctime, 0, datetime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ctime, 0, datetime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ctime, 1, datetime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ctime, 1, datetime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ctime, 0, precise_datetime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ctime, 0, precise_datetime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ctime, 1, precise_datetime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ctime, 1, precise_datetime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ctime, 0, varchar, "0", OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ctime, 0, varchar, "1", OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ctime, 1, varchar, "0", OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ctime, 1, varchar, "1", OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ctime, 0, ctime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ctime, 0, ctime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ctime, 1, ctime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ctime, 1, ctime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ctime, 0, mtime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ctime, 0, mtime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ctime, 1, mtime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ctime, 1, mtime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ctime, 0, ext, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ctime, 0, ext, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ctime, 1, ext, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ctime, 1, ext, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ctime, 0, bool, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ctime, 0, bool, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ctime, 1, bool, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ctime, 1, bool, 1, OB_ERR_INVALID_TYPE_FOR_OP);

  E2(mtime, 0, null, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(mtime, 1, null, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(mtime, 0, int, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(mtime, 0, int, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(mtime, 1, int, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(mtime, 1, int, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(mtime, 0, float, 0.0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(mtime, 0, float, 1.0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(mtime, 1, float, 0.0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(mtime, 1, float, 1.0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(mtime, 0, double, 0.0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(mtime, 0, double, 1.0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(mtime, 1, double, 0.0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(mtime, 1, double, 1.0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(mtime, 0, datetime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(mtime, 0, datetime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(mtime, 1, datetime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(mtime, 1, datetime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(mtime, 0, precise_datetime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(mtime, 0, precise_datetime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(mtime, 1, precise_datetime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(mtime, 1, precise_datetime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(mtime, 0, varchar, "0", OB_ERR_INVALID_TYPE_FOR_OP);
  E2(mtime, 0, varchar, "1", OB_ERR_INVALID_TYPE_FOR_OP);
  E2(mtime, 1, varchar, "0", OB_ERR_INVALID_TYPE_FOR_OP);
  E2(mtime, 1, varchar, "1", OB_ERR_INVALID_TYPE_FOR_OP);
  E2(mtime, 0, ctime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(mtime, 0, ctime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(mtime, 1, ctime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(mtime, 1, ctime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(mtime, 0, mtime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(mtime, 0, mtime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(mtime, 1, mtime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(mtime, 1, mtime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(mtime, 0, ext, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(mtime, 0, ext, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(mtime, 1, ext, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(mtime, 1, ext, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(mtime, 0, bool, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(mtime, 0, bool, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(mtime, 1, bool, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(mtime, 1, bool, 1, OB_ERR_INVALID_TYPE_FOR_OP);

  E2(ext, 1, null, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ext, 0, int, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ext, 0, int, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ext, 1, int, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ext, 1, int, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ext, 0, float, 0.0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ext, 0, float, 1.0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ext, 1, float, 0.0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ext, 1, float, 1.0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ext, 0, double, 0.0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ext, 0, double, 1.0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ext, 1, double, 0.0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ext, 1, double, 1.0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ext, 0, datetime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ext, 0, datetime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ext, 1, datetime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ext, 1, datetime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ext, 0, precise_datetime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ext, 0, precise_datetime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ext, 1, precise_datetime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ext, 1, precise_datetime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ext, 0, varchar, "0", OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ext, 0, varchar, "1", OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ext, 1, varchar, "0", OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ext, 1, varchar, "1", OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ext, 0, ctime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ext, 0, ctime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ext, 1, ctime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ext, 1, ctime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ext, 0, mtime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ext, 0, mtime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ext, 1, mtime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ext, 1, mtime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ext, 0, ext, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ext, 0, ext, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ext, 1, ext, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ext, 1, ext, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ext, 0, bool, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ext, 0, bool, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ext, 1, bool, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(ext, 1, bool, 1, OB_ERR_INVALID_TYPE_FOR_OP);

  R2(bool, 0, null, 0, ObNullType, OB_OBJ_TYPE_ERROR);
  R2(bool, 1, null, 0, ObBoolType, 1);
  R2(bool, 0, int, 0, ObBoolType, 0);
  R2(bool, 0, int, 1, ObBoolType, 1);
  R2(bool, 1, int, 0, ObBoolType, 1);
  R2(bool, 1, int, 1, ObBoolType, 1);
  R2(bool, 0, float, 0.0, ObBoolType, 0);
  R2(bool, 0, float, 1.0, ObBoolType, 1);
  R2(bool, 1, float, 0.0, ObBoolType, 1);
  R2(bool, 1, float, 1.0, ObBoolType, 1);
  R2(bool, 0, double, 0.0, ObBoolType, 0);
  R2(bool, 0, double, 1.0, ObBoolType, 1);
  R2(bool, 1, double, 0.0, ObBoolType, 1);
  R2(bool, 1, double, 1.0, ObBoolType, 1);
  E2(bool, 0, datetime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(bool, 0, datetime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(bool, 1, datetime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(bool, 1, datetime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(bool, 0, precise_datetime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(bool, 0, precise_datetime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(bool, 1, precise_datetime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(bool, 1, precise_datetime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  R2(bool, 0, varchar, "0", ObBoolType, 0);
  R2(bool, 0, varchar, "1", ObBoolType, 1);
  R2(bool, 1, varchar, "0", ObBoolType, 1);
  R2(bool, 1, varchar, "1", ObBoolType, 1);
  E2(bool, 0, ctime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(bool, 0, ctime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(bool, 1, ctime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(bool, 1, ctime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(bool, 0, mtime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(bool, 0, mtime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(bool, 1, mtime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(bool, 1, mtime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(bool, 0, ext, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(bool, 0, ext, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(bool, 1, ext, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E2(bool, 1, ext, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  R2(bool, 0, bool, 0, ObBoolType, 0);
  R2(bool, 0, bool, 1, ObBoolType, 1);
  R2(bool, 1, bool, 0, ObBoolType, 1);
  R2(bool, 1, bool, 1, ObBoolType, 1);
}

TEST_F(ObExprOrTest, result3_test)
{
  //calc result3
  //null type
  /* at   av  bt   bv  ct   cv    restype     res.get_bool()*/
  ObExprStringBuf buf;
  R3(null, 0, null, 0, bool, 1, ObBoolType, 1);
  E3(null, 0, int, 1, ext, 2, OB_ERR_INVALID_TYPE_FOR_OP);
  E3(null, 0, float, 1.0, mtime, 2, OB_ERR_INVALID_TYPE_FOR_OP);
  E3(null, 0, double, 2.0, ctime, 2, OB_ERR_INVALID_TYPE_FOR_OP);
  E3(null, 0, datetime, 2, mtime, 3, OB_ERR_INVALID_TYPE_FOR_OP);
  E3(null, 0, precise_datetime, 2, double, 2.0, OB_ERR_INVALID_TYPE_FOR_OP);
  E3(null, 0, varchar, "hello", datetime, 2, OB_ERR_INVALID_TYPE_FOR_OP);
  E3(null, 0, ctime, 2, double, 2.0, OB_ERR_INVALID_TYPE_FOR_OP);
  E3(null, 0, mtime, 2, float, 1.0, OB_ERR_INVALID_TYPE_FOR_OP);
  E3(null, 0, ext, 0, int, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  R3(null, 0, bool, 0, null, 0, ObNullType, OB_OBJ_TYPE_ERROR);

  R3(int, 0, null, 0, int, 1, ObBoolType, 1);
  R3(int, 1, null, 0, int, 1, ObBoolType, 1);
  R3(int, 0, int, 0, int, 0, ObBoolType, 0);
  R3(int, 0, int, 1, int, 1, ObBoolType, 1);
  R3(int, 1, int, 0, int, 1, ObBoolType, 1);
  R3(int, 1, int, 1, int, 1, ObBoolType, 1);
  E3(int, 0, float, float(0.0), ext, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E3(int, 0, float, float(1.0), ext, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E3(int, 1, float, float(0.0), ext, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E3(int, 1, float, float(1.0), ext, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E3(int, 0, double, 0.0, ctime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E3(int, 0, double, 0.0, ctime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E3(int, 1, double, 0.0, ctime, 2, OB_ERR_INVALID_TYPE_FOR_OP);
  E3(int, 1, double, 1.0, ctime, 2, OB_ERR_INVALID_TYPE_FOR_OP);
  E3(double, 0.0, datetime, 0, varchar, "0", OB_ERR_INVALID_TYPE_FOR_OP);
  E3(double, 0.0, datetime, 1, varchar, "0", OB_ERR_INVALID_TYPE_FOR_OP);
  E3(double, 1.0, datetime, 0, varchar, "1", OB_ERR_INVALID_TYPE_FOR_OP);
  E3(double, 1.0, datetime, 1, varchar, "1", OB_ERR_INVALID_TYPE_FOR_OP);
  E3(ctime, 0, precise_datetime, 0, float, float(0.0), OB_ERR_INVALID_TYPE_FOR_OP);
  E3(ctime, 0, precise_datetime, 0, float, float(1.0), OB_ERR_INVALID_TYPE_FOR_OP);
  E3(ctime, 1, precise_datetime, 0, float, float(2.2), OB_ERR_INVALID_TYPE_FOR_OP);
  E3(ctime, 1, precise_datetime, 1, float, float(2.2), OB_ERR_INVALID_TYPE_FOR_OP);
  R3(varchar, "0", varchar, "0", varchar, "0", ObBoolType, 0);
  R3(varchar, "1", varchar, "1", varchar, "1", ObBoolType, 1);
  R3(varchar, "1", varchar, "0", varchar, "0", ObBoolType, 1);
  R3(varchar, "1", varchar, "1", varchar, "1", ObBoolType, 1);
  E3(int, 0, ctime, 0, varchar, "0", OB_ERR_INVALID_TYPE_FOR_OP);
  E3(int, 1, ctime, 1, varchar, "1", OB_ERR_INVALID_TYPE_FOR_OP);
  E3(int, 1, ctime, 0, varchar, "0", OB_ERR_INVALID_TYPE_FOR_OP);
  E3(int, 1, ctime, 1, varchar, "1", OB_ERR_INVALID_TYPE_FOR_OP);
  E3(varchar, "0", int, 0, mtime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E3(varchar, "1", int, 0, mtime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E3(varchar, "0", int, 1, mtime, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E3(varchar, "1", int, 1, mtime, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E3(int, 0, varchar, "0", ext, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E3(int, 0, varchar, "1", ext, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E3(int, 1, varchar, "0", ext, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  E3(int, 1, varchar, "1", ext, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  R3(int, 0, varchar, "0", bool, 0, ObBoolType, 0);
  R3(int, 0, varchar, "1", bool, 1, ObBoolType, 1);
  R3(int, 1, varchar, "0", bool, 0, ObBoolType, 1);
  R3(int, 1, varchar, "1", bool, 1, ObBoolType, 1);
  E3(int, 1, varchar, "e1", bool, 1, OB_ERR_CAST_VARCHAR_TO_BOOL);
}

TEST_F(ObExprOrTest, resultN_test)
{
  //calc result3
  //null type
  /* num, rtype, res, at av  bt bv  ct cv dt dv...  */
  ObExprStringBuf buf;
  RN(2, ObNullType, OB_OBJ_TYPE_ERROR, null, 0, null, 0);
  RN(2, ObBoolType, 1, null, 0, int, 1);

  RN(2, ObBoolType, 0, int, 0, int, 0);
  RN(2, ObBoolType, 1, int, 0, int, 1);
  RN(2, ObBoolType, 1, int, 1, int, 0);
  RN(2, ObBoolType, 0, int, 0, float, 0.0);
  RN(2, ObBoolType, 1, int, 0, float, 1.0);
  RN(2, ObBoolType, 1, int, 1, float, 0.0);
  RN(2, ObBoolType, 1, int, 1, float, 1.0);
  RN(2, ObBoolType, 0, int, 0, double, 0.0);
  RN(2, ObBoolType, 1, int, 0, double, 1.0);
  RN(2, ObBoolType, 1, int, 1, double, 0.0);
  RN(2, ObBoolType, 1, int, 1, double, 1.0);
  EN(2, OB_ERR_INVALID_TYPE_FOR_OP, int, 0, datetime, 0);
  EN(2, OB_ERR_INVALID_TYPE_FOR_OP, int, 0, datetime, 1);
  EN(2, OB_ERR_INVALID_TYPE_FOR_OP, int, 1, datetime, 0);
  EN(2, OB_ERR_INVALID_TYPE_FOR_OP, int, 1, datetime, 1);
  EN(2, OB_ERR_INVALID_TYPE_FOR_OP, int, 0, precise_datetime, 0);
  EN(2, OB_ERR_INVALID_TYPE_FOR_OP, int, 0, precise_datetime, 1);
  EN(2, OB_ERR_INVALID_TYPE_FOR_OP, int, 1, precise_datetime, 0);
  EN(2, OB_ERR_INVALID_TYPE_FOR_OP, int, 1, precise_datetime, 1);
  RN(2, ObBoolType, 0, int, 0, varchar, "0");
  RN(2, ObBoolType, 1, int, 0, varchar, "1");
  RN(2, ObBoolType, 1, int, 1, varchar, "0");
  RN(2, ObBoolType, 1, int, 1, varchar, "1");
  EN(2, OB_ERR_CAST_VARCHAR_TO_BOOL, int, 1, varchar, "31");
  EN(2, OB_ERR_INVALID_TYPE_FOR_OP, int, 0, ctime, 0);
  EN(2, OB_ERR_INVALID_TYPE_FOR_OP, int, 0, ctime, 1);
  EN(2, OB_ERR_INVALID_TYPE_FOR_OP, int, 1, ctime, 0);
  EN(2, OB_ERR_INVALID_TYPE_FOR_OP, int, 1, ctime, 1);
  EN(2, OB_ERR_INVALID_TYPE_FOR_OP, int, 0, mtime, 0);
  EN(2, OB_ERR_INVALID_TYPE_FOR_OP, int, 0, mtime, 1);
  EN(2, OB_ERR_INVALID_TYPE_FOR_OP, int, 1, mtime, 0);
  EN(2, OB_ERR_INVALID_TYPE_FOR_OP, int, 1, mtime, 1);
  EN(2, OB_ERR_INVALID_TYPE_FOR_OP, int, 0, ext, 0);
  EN(2, OB_ERR_INVALID_TYPE_FOR_OP, int, 0, ext, 1);
  EN(2, OB_ERR_INVALID_TYPE_FOR_OP, int, 1, ext, 0);
  EN(2, OB_ERR_INVALID_TYPE_FOR_OP, int, 1, ext, 1);
  RN(2, ObBoolType, 0, int, 0, bool, 0);
  RN(2, ObBoolType, 1, int, 0, bool, 1);
  RN(2, ObBoolType, 1, int, 1, bool, 0);
  RN(2, ObBoolType, 1, int, 1, bool, 1);

  //RN3
  RN(3, ObNullType, OB_OBJ_TYPE_ERROR, null, 0, null, 0, int, 0);
  RN(3, ObBoolType, 1, null, 0, int, 1, int, 1);
  RN(3, ObBoolType, 0, int, 0, int, 0, int, 0);
  RN(3, ObBoolType, 1, int, 0, int, 1, int, 1);
  RN(3, ObBoolType, 1, int, 1, int, 0, int, 1);
  RN(3, ObBoolType, 0, int, 0, float, 0.0, int, 0);
  RN(3, ObBoolType, 1, int, 0, float, 1.0, int, 1);
  RN(3, ObBoolType, 1, int, 1, float, 0.0, int, 1);
  RN(3, ObBoolType, 1, int, 1, float, 1.0, int, 1);
  RN(3, ObBoolType, 0, int, 0, double, 0.0, float, float(0));
  RN(3, ObBoolType, 1, int, 1, double, 0.0, float, float(1));
  RN(3, ObBoolType, 1, int, 1, double, 1.0, float, float(1));
  EN(3, OB_ERR_INVALID_TYPE_FOR_OP, int, 0, datetime, 0, float, float(0));
  EN(3, OB_ERR_INVALID_TYPE_FOR_OP, int, 0, datetime, 1, float, float(1));
  EN(3, OB_ERR_INVALID_TYPE_FOR_OP, int, 1, datetime, 0, float, float(1));
  EN(3, OB_ERR_INVALID_TYPE_FOR_OP, int, 1, datetime, 1, float, float(1));
  EN(3, OB_ERR_INVALID_TYPE_FOR_OP, int, 0, precise_datetime, 0, datetime, 0);
  EN(3, OB_ERR_INVALID_TYPE_FOR_OP, int, 0, precise_datetime, 1, datetime, 2);
  EN(3, OB_ERR_INVALID_TYPE_FOR_OP, int, 1, precise_datetime, 0, datetime, 2);
  EN(3, OB_ERR_INVALID_TYPE_FOR_OP, int, 1, precise_datetime, 1, datetime, 2);
  RN(3, ObBoolType, 0, int, 0, varchar, "0", int, 0);
  RN(3, ObBoolType, 1, int, 0, varchar, "1", int, 2);
  RN(3, ObBoolType, 1, int, 1, varchar, "0", int, 2);
  RN(3, ObBoolType, 1, int, 1, varchar, "1", int, 2);
  EN(3, OB_ERR_INVALID_TYPE_FOR_OP, int, 1, varchar, "he", datetime, 2);
  EN(3, OB_ERR_INVALID_TYPE_FOR_OP, int, 0, ctime, 0, varchar, "0");
  EN(3, OB_ERR_INVALID_TYPE_FOR_OP, int, 0, ctime, 1, varchar, "1");
  EN(3, OB_ERR_INVALID_TYPE_FOR_OP, int, 1, ctime, 0, varchar, "1");
  EN(3, OB_ERR_INVALID_TYPE_FOR_OP, int, 1, ctime, 1, varchar, "1");
  EN(3, OB_ERR_INVALID_TYPE_FOR_OP, int, 0, mtime, 0, varchar, "0");
  EN(3, OB_ERR_INVALID_TYPE_FOR_OP, int, 0, mtime, 1, varchar, "1");
  EN(3, OB_ERR_INVALID_TYPE_FOR_OP, int, 1, mtime, 0, varchar, "1");
  EN(3, OB_ERR_INVALID_TYPE_FOR_OP, int, 1, mtime, 1, varchar, "1");
  EN(3, OB_ERR_INVALID_TYPE_FOR_OP, int, 0, ext, 0, bool, 0);
  EN(3, OB_ERR_INVALID_TYPE_FOR_OP, int, 0, ext, 1, bool, 1);
  EN(3, OB_ERR_INVALID_TYPE_FOR_OP, int, 1, ext, 0, bool, 1);
  EN(3, OB_ERR_INVALID_TYPE_FOR_OP, int, 1, ext, 1, bool, 1);
  RN(3, ObBoolType, 0, int, 0, bool, 0, bool, 0);
  RN(3, ObBoolType, 1, int, 0, bool, 1, bool, 1);
  RN(3, ObBoolType, 1, int, 1, bool, 0, int, 1);
  RN(3, ObBoolType, 1, int, 1, bool, 1, int, 2);

  //RN4
  RN(4, ObBoolType, 1, null, 0, null, 0, int, 0, double, 2.0);
  RN(4, ObBoolType, 1, null, 0, int, 1, int, 1, double, 3.0);
  RN(4, ObBoolType, 0, int, 0, int, 0, int, 0, float, float(0));
  RN(4, ObBoolType, 1, int, 0, int, 1, int, 1, float, float(1));
  RN(4, ObBoolType, 1, int, 1, int, 0, int, 1, float, float(1));
  RN(4, ObBoolType, 0, int, 0, float, 0.0, int, 0, float, float(0));
  RN(4, ObBoolType, 1, int, 0, float, 1.0, int, 1, float, float(1));
  RN(4, ObBoolType, 1, int, 1, float, 0.0, int, 1, float, float(1));
  RN(4, ObBoolType, 1, int, 1, float, 1.0, int, 1, float, float(1));
  RN(4, ObBoolType, 0, int, 0, double, 0.0, float, float(0), double, 0.0);
  RN(4, ObBoolType, 1, int, 1, double, 0.0, float, float(1), double, 2.0);
  RN(4, ObBoolType, 1, int, 1, double, 1.0, float, float(1), double, 2.0);
  EN(4, OB_ERR_INVALID_TYPE_FOR_OP, int, 0, datetime, 0, float, float(1), double, 2.0);
  EN(4, OB_ERR_INVALID_TYPE_FOR_OP, int, 0, datetime, 0, float, float(0), ext, 0);
  EN(4, OB_ERR_INVALID_TYPE_FOR_OP, int, 1, datetime, 0, float, float(1), ext, 1);
  EN(4, OB_ERR_INVALID_TYPE_FOR_OP, int, 1, datetime, 1, float, float(1), ext, 1);
  EN(4, OB_ERR_INVALID_TYPE_FOR_OP, int, 0, precise_datetime, 0, datetime, 0, ext, 0);
  EN(4, OB_ERR_INVALID_TYPE_FOR_OP, int, 0, precise_datetime, 1, datetime, 2, ext, 1);
  EN(4, OB_ERR_INVALID_TYPE_FOR_OP, int, 1, precise_datetime, 0, datetime, 2, ext, 1);
  EN(4, OB_ERR_INVALID_TYPE_FOR_OP, int, 1, precise_datetime, 1, datetime, 2, ext, 1);
  RN(4, ObBoolType, 0, int, 0, varchar, "0", int, 0, varchar, "false");
  RN(4, ObBoolType, 1, int, 0, varchar, "1", int, 2, varchar, "1");
  RN(4, ObBoolType, 1, int, 1, varchar, "0", int, 2, varchar, "1");
  RN(4, ObBoolType, 1, int, 1, varchar, "True", int, 2, varchar, "1");
  EN(4, OB_ERR_CAST_VARCHAR_TO_BOOL, int, 1, varchar, "1", int, 2, varchar, "he1");
  EN(4, OB_ERR_INVALID_TYPE_FOR_OP, int, 0, ctime, 0, varchar, "0", precise_datetime, 0);
  EN(4, OB_ERR_INVALID_TYPE_FOR_OP, int, 0, ctime, 1, varchar, "1", precise_datetime, 2);
  EN(4, OB_ERR_INVALID_TYPE_FOR_OP, int, 1, ctime, 0, varchar, "1", precise_datetime, 2);
  EN(4, OB_ERR_INVALID_TYPE_FOR_OP, int, 1, ctime, 1, varchar, "1", precise_datetime, 2);
  EN(4, OB_ERR_INVALID_TYPE_FOR_OP, int, 0, mtime, 0, varchar, "1", ctime, 3);
  EN(4, OB_ERR_INVALID_TYPE_FOR_OP, int, 0, mtime, 1, varchar, "1", ctime, 3);
  EN(4, OB_ERR_INVALID_TYPE_FOR_OP, int, 1, mtime, 0, varchar, "1", ctime, 3);
  EN(4, OB_ERR_INVALID_TYPE_FOR_OP, int, 1, mtime, 1, varchar, "1", ctime, 3);
  EN(4, OB_ERR_INVALID_TYPE_FOR_OP, int, 0, ext, 0, bool, 1, double, 3.4);
  EN(4, OB_ERR_INVALID_TYPE_FOR_OP, int, 0, ext, 1, bool, 1, double, 3.4);
  EN(4, OB_ERR_INVALID_TYPE_FOR_OP, int, 1, ext, 0, bool, 1, double, 3.4);
  EN(4, OB_ERR_INVALID_TYPE_FOR_OP, int, 1, ext, 1, bool, 1, double, 3.4);
  RN(4, ObBoolType, 1, int, 0, bool, 0, bool, 1, double, 3.4);
  RN(4, ObBoolType, 1, int, 0, bool, 1, bool, 1, double, 3.4);
  RN(4, ObBoolType, 1, int, 1, bool, 0, int, 1, double, 3.4);
  RN(4, ObBoolType, 1, int, 1, bool, 1, int, 2, double, 3.4);


  //RN5
  RN(5, ObBoolType, 1, null, 0, null, 0, int, 0, double, 2.0, null, 0);
  RN(5, ObBoolType, 1, null, 0, int, 1, int, 1, double, 3.0, null, 0);
  RN(5, ObBoolType, 0, int, 0, int, 0, int, 0, float, float(0), varchar, "0");
  RN(5, ObBoolType, 1, int, 0, int, 1, int, 1, float, float(1), varchar, "true");
  RN(5, ObBoolType, 1, int, 1, int, 0, int, 1, float, float(1), varchar, "true");
  EN(5, OB_ERR_INVALID_TYPE_FOR_OP, int, 0, float, 0.0, int, 0, float, float(0), ctime, 0);
  EN(5, OB_ERR_INVALID_TYPE_FOR_OP, int, 0, float, 1.0, int, 1, float, float(1), ctime, 4);
  EN(5, OB_ERR_INVALID_TYPE_FOR_OP, int, 1, float, 0.0, int, 1, float, float(1), ctime, 4);
  EN(5, OB_ERR_INVALID_TYPE_FOR_OP, int, 1, float, 1.0, int, 1, float, float(1), ctime, 4);
  EN(5, OB_ERR_CAST_VARCHAR_TO_BOOL, int, 0, double, 1.0, float, float(1), double, 2.0, varchar, "1he");
  EN(5, OB_ERR_CAST_VARCHAR_TO_BOOL, int, 1, double, 0.0, float, float(1), double, 2.0, varchar, "1he");
  RN(5, ObBoolType, 1, int, 1, double, 1.0, float, float(1), double, 2.0, varchar, "1");
  EN(5, OB_ERR_INVALID_TYPE_FOR_OP, int, 0, datetime, 0, float, float(1), double, 2.0, varchar, "1");
  EN(5, OB_ERR_INVALID_TYPE_FOR_OP, int, 0, datetime, 1, float, float(1), ext, 1, varchar, "1he");
  EN(5, OB_ERR_INVALID_TYPE_FOR_OP, int, 1, datetime, 0, float, float(1), ext, 1, varchar, "1he");
  EN(5, OB_ERR_INVALID_TYPE_FOR_OP, int, 1, datetime, 1, float, float(1), ext, 1, varchar, "1he");
  EN(5, OB_ERR_INVALID_TYPE_FOR_OP, int, 0, precise_datetime, 0, datetime, 2, ext, 1, double, 3.4);
  EN(5, OB_ERR_INVALID_TYPE_FOR_OP, int, 0, precise_datetime, 1, datetime, 2, ext, 1, double, 3.4);
  EN(5, OB_ERR_INVALID_TYPE_FOR_OP, int, 1, precise_datetime, 0, datetime, 2, ext, 1, double, 3.4);
  EN(5, OB_ERR_INVALID_TYPE_FOR_OP, int, 1, precise_datetime, 1, datetime, 2, ext, 1, double, 3.4);
  EN(5, OB_ERR_INVALID_TYPE_FOR_OP, int, 0, varchar, "0", datetime, 2, varchar, "2hello", double, 3.4);
  EN(5, OB_ERR_INVALID_TYPE_FOR_OP, int, 0, varchar, "1", datetime, 2, varchar, "2hello", double, 3.4);
  EN(5, OB_ERR_INVALID_TYPE_FOR_OP, int, 1, varchar, "0", datetime, 2, varchar, "2hello", double, 3.4);
  EN(5, OB_ERR_INVALID_TYPE_FOR_OP, int, 1, varchar, "1", datetime, 2, varchar, "2hello", double, 3.4);
  EN(5, OB_ERR_INVALID_TYPE_FOR_OP, int, 0, ctime, 0, varchar, "1", precise_datetime, 2, float, float(2.3));
  EN(5, OB_ERR_INVALID_TYPE_FOR_OP, int, 0, ctime, 1, varchar, "1", precise_datetime, 2, float, float(2.3));
  EN(5, OB_ERR_INVALID_TYPE_FOR_OP, int, 1, ctime, 0, varchar, "1", precise_datetime, 2, float, float(2.3));
  EN(5, OB_ERR_INVALID_TYPE_FOR_OP, int, 1, ctime, 1, varchar, "1", precise_datetime, 2, float, float(2.3));
  EN(5, OB_ERR_INVALID_TYPE_FOR_OP, int, 0, mtime, 0, varchar, "ere", ctime, 0, float, float(0.0));
  EN(5, OB_ERR_INVALID_TYPE_FOR_OP, int, 0, mtime, 1, varchar, "1", ctime, 3, float, float(2.3));
  EN(5, OB_ERR_INVALID_TYPE_FOR_OP, int, 1, mtime, 0, varchar, "1", ctime, 3, float, float(2.3));
  EN(5, OB_ERR_INVALID_TYPE_FOR_OP, int, 1, mtime, 1, varchar, "1", ctime, 3, float, float(2.3));
  EN(5, OB_ERR_INVALID_TYPE_FOR_OP, int, 0, ext, 0, bool, 0, double, 0.0, int, 0);
  EN(5, OB_ERR_INVALID_TYPE_FOR_OP, int, 0, ext, 1, bool, 1, double, 3.4, int, 3);
  EN(5, OB_ERR_INVALID_TYPE_FOR_OP, int, 1, ext, 0, bool, 1, double, 3.4, int, 3);
  EN(5, OB_ERR_INVALID_TYPE_FOR_OP, int, 1, ext, 1, bool, 1, double, 3.4, int, 3);
  RN(5, ObBoolType, 1, int, 0, bool, 0, bool, 1, double, 3.4, int, 3);
  RN(5, ObBoolType, 1, int, 0, bool, 1, bool, 1, double, 3.4, int, 3);
  RN(5, ObBoolType, 1, int, 1, bool, 0, int, 1, double, 3.4, int, 3);
  RN(5, ObBoolType, 1, int, 1, bool, 1, int, 2, double, 3.4, int, 3);

  //RN6
  RN(6, ObBoolType, 1, null, 0, null, 0, int, 0, double, 2.0, null, 0, varchar, "1");
  RN(6, ObBoolType, 1, null, 0, int, 1, int, 1, double, 3.0, null, 0, varchar, "0");
  RN(6, ObBoolType, 1, int, 0, int, 0, int, 1, float, float(1), int, 4, varchar, "1");
  RN(6, ObBoolType, 1, int, 0, int, 1, int, 1, float, float(1), int, 4, varchar, "1");
  RN(6, ObBoolType, 1, int, 1, int, 0, int, 1, float, float(1), int, 4, varchar, "1");
  RN(6, ObBoolType, 0, int, 0, float, 0.0, int, 0, float, float(0), int, 0, varchar, "false");
  RN(6, ObBoolType, 1, int, 0, float, 1.0, int, 1, float, float(1), int, 4, varchar, "1");
  RN(6, ObBoolType, 1, int, 1, float, 0.0, int, 1, float, float(1), int, 4, varchar, "1");
  RN(6, ObBoolType, 1, int, 1, float, 1.0, int, 1, float, float(1), int, 4, varchar, "1");
  RN(6, ObBoolType, 1, int, 0, double, 1.0, float, float(1), double, 2.0, varchar, "1", int, 2);
  RN(6, ObBoolType, 1, int, 1, double, 0.0, float, float(1), double, 2.0, varchar, "1", int, 2);
  RN(6, ObBoolType, 1, int, 1, double, 1.0, float, float(1), double, 2.0, varchar, "1", int, 2);
  EN(6, OB_ERR_INVALID_TYPE_FOR_OP, int, 0, datetime, 0, float, float(1), double, 2.0, varchar, "1", int, 2);
  EN(6, OB_ERR_INVALID_TYPE_FOR_OP, int, 0, datetime, 1, float, float(1), ext, 1, varchar, "1he", ctime, 2);
  EN(6, OB_ERR_INVALID_TYPE_FOR_OP, int, 1, datetime, 0, float, float(1), ext, 1, varchar, "1he", ctime, 2);
  EN(6, OB_ERR_INVALID_TYPE_FOR_OP, int, 1, datetime, 1, float, float(1), ext, 1, varchar, "1he", ctime, 2);
  EN(6, OB_ERR_INVALID_TYPE_FOR_OP, int, 0, precise_datetime, 0, datetime, 2, ext, 1, double, 3.4, varchar, "time");
  EN(6, OB_ERR_INVALID_TYPE_FOR_OP, int, 0, precise_datetime, 1, datetime, 2, ext, 1, double, 3.4, varchar, "time");
  EN(6, OB_ERR_INVALID_TYPE_FOR_OP, int, 1, precise_datetime, 0, datetime, 2, ext, 1, double, 3.4, varchar, "time");
  EN(6, OB_ERR_INVALID_TYPE_FOR_OP, int, 1, precise_datetime, 1, datetime, 2, ext, 1, double, 3.4, varchar, "time");
  RN(6, ObBoolType, 1, int, 0, varchar, "0", int, 2, varchar, "1", double, 3.4, int, 0);
  RN(6, ObBoolType, 1, int, 0, varchar, "1", int, 2, varchar, "1", double, 3.4, int, 0);
  RN(6, ObBoolType, 1, int, 1, varchar, "0", int, 2, varchar, "true", double, 3.4, int, 0);
  EN(6, OB_ERR_CAST_VARCHAR_TO_BOOL, int, 1, varchar, "1", int, 2, varchar, "2hello", double, 3.4, int, 0);
  EN(6, OB_ERR_INVALID_TYPE_FOR_OP, int, 0, ctime, 0, varchar, "0", precise_datetime, 0, float, float(0.0), mtime, 0);
  EN(6, OB_ERR_INVALID_TYPE_FOR_OP, int, 0, ctime, 1, varchar, "1", precise_datetime, 2, float, float(2.3), mtime, 4);
  EN(6, OB_ERR_INVALID_TYPE_FOR_OP, int, 1, ctime, 0, varchar, "1", precise_datetime, 2, float, float(2.3), mtime, 4);
  EN(6, OB_ERR_INVALID_TYPE_FOR_OP, int, 1, ctime, 1, varchar, "1", precise_datetime, 2, float, float(2.3), mtime, 4);
  EN(6, OB_ERR_INVALID_TYPE_FOR_OP, int, 0, mtime, 0, varchar, "1", ctime, 3, float, float(2.3), double, 0.00);
  EN(6, OB_ERR_INVALID_TYPE_FOR_OP, int, 0, mtime, 1, varchar, "1", ctime, 3, float, float(2.3), double, 0.00);
  EN(6, OB_ERR_INVALID_TYPE_FOR_OP, int, 1, mtime, 0, varchar, "1", ctime, 3, float, float(2.3), double, 0.00);
  EN(6, OB_ERR_INVALID_TYPE_FOR_OP, int, 1, mtime, 1, varchar, "1", ctime, 3, float, float(2.3), double, 0.00);
  EN(6, OB_ERR_INVALID_TYPE_FOR_OP, int, 0, ext, 0, bool, 1, double, 3.4, int, 3, double, 0.00);
  EN(6, OB_ERR_INVALID_TYPE_FOR_OP, int, 0, ext, 1, bool, 1, double, 3.4, int, 3, double, 0.00);
  EN(6, OB_ERR_INVALID_TYPE_FOR_OP, int, 1, ext, 0, bool, 1, double, 3.4, int, 3, double, 0.00);
  EN(6, OB_ERR_INVALID_TYPE_FOR_OP, int, 1, ext, 1, bool, 1, double, 3.4, int, 3, double, 0.00);
  RN(6, ObBoolType, 1, int, 0, bool, 0, bool, 1, double, 3.4, int, 3, varchar, "true");
  RN(6, ObBoolType, 1, int, 0, bool, 1, bool, 1, double, 3.4, int, 3, varchar, "1");
  RN(6, ObBoolType, 1, int, 1, bool, 0, int, 1, double, 3.4, int, 3, varchar, "true");
  RN(6, ObBoolType, 1, int, 1, bool, 1, int, 2, double, 3.4, int, 3, varchar, "1");
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
