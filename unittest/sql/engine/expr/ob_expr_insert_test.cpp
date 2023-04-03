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
#include "sql/engine/expr/ob_expr_insert.h"
#include "ob_expr_test_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

class ObExprInsertTest : public ::testing::Test
{
public:
  ObExprInsertTest();
  virtual ~ObExprInsertTest();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  ObExprInsertTest(const ObExprInsertTest &other);
  ObExprInsertTest& operator=(const ObExprInsertTest &other);
private:
  // data members
};
ObExprInsertTest::ObExprInsertTest()
{
}

ObExprInsertTest::~ObExprInsertTest()
{
}

void ObExprInsertTest::SetUp()
{
}

void ObExprInsertTest::TearDown()
{
}

#define T(obj, t1, v1, t2, v2, t3, v3, t4, v4, ref_type, ref_value) EXPECT_RESULT4(obj, &buf, calc_result, t1, v1, t2, v2, t3, v3, t4, v4, ref_type, ref_value)
#define F(obj, t1, v1, t2, v2, t3, v3, t4, v4, ref_type, ref_value) EXPECT_FAIL_RESULT4(obj, &buf, calc_result, t1, v1, t2, v2, t3, v3, t4, v4, ref_type, ref_value)

TEST_F(ObExprInsertTest, basic_test)
{
  //ObExprStringBuf buf;
  ObArenaAllocator buf(ObModIds::OB_SQL_SESSION);
  ObExprInsert insert(buf);

  ASSERT_EQ(ObExprOperator::MORE_THAN_TWO, insert.get_param_num());

  // null
  T(insert, null,  , null,  , null,  ,null,  , null,  );
  T(insert, int, 1, null,  , null,  ,null,  , null,  );
  T(insert, int, 1, varchar, "hi", null,  ,null,  , null, );
  T(insert, int, 1, null,  , varchar, "hi", null, , null, );
  T(insert, int, 1, null,  , varchar, "hi", null, , null, );
  T(insert, int, 1, null,  , varchar, "hi", int, 1, null, );

  // normal
  T(insert, varchar, "Quadratic", int, -2, int, 100, varchar, "What", varchar, "Quadratic");
  T(insert, varchar, "Quadratic", int, -1, int, 100, varchar, "What", varchar, "Quadratic");
  T(insert, varchar, "Quadratic", int, 0, int, 100, varchar, "What", varchar, "Quadratic");
  T(insert, varchar, "Quadratic", int, 1, int, 100, varchar, "What", varchar, "What");
  T(insert, varchar, "Quadratic", int, 2, int, 100, varchar, "What", varchar, "QWhat");
  T(insert, varchar, "Quadratic", int, 3, int, 100, varchar, "What", varchar, "QuWhat");
  T(insert, varchar, "Quadratic", int, 4, int, 100, varchar, "What", varchar, "QuaWhat");
  T(insert, varchar, "Quadratic", int, 5, int, 100, varchar, "What", varchar, "QuadWhat");
  T(insert, varchar, "Quadratic", int, 6, int, 100, varchar, "What", varchar, "QuadrWhat");
  T(insert, varchar, "Quadratic", int, 7, int, 100, varchar, "What", varchar, "QuadraWhat");
  T(insert, varchar, "Quadratic", int, 8, int, 100, varchar, "What", varchar, "QuadratWhat");
  T(insert, varchar, "Quadratic", int, 9, int, 100, varchar, "What", varchar, "QuadratiWhat");
  T(insert, varchar, "Quadratic", int, 10, int, 100, varchar, "What", varchar, "Quadratic");
  T(insert, varchar, "Quadratic", int, 11, int, 100, varchar, "What", varchar, "Quadratic");

  T(insert, varchar, "Quadratic", int, -2, int, 3, varchar, "What", varchar, "Quadratic");
  T(insert, varchar, "Quadratic", int, -1, int, 3, varchar, "What", varchar, "Quadratic");
  T(insert, varchar, "Quadratic", int, 0, int, 3, varchar, "What", varchar, "Quadratic");
  T(insert, varchar, "Quadratic", int, 1, int, 3, varchar, "What", varchar, "Whatdratic");
  T(insert, varchar, "Quadratic", int, 2, int, 3, varchar, "What", varchar, "QWhatratic");
  T(insert, varchar, "Quadratic", int, 3, int, 3, varchar, "What", varchar, "QuWhatatic");
  T(insert, varchar, "Quadratic", int, 4, int, 3, varchar, "What", varchar, "QuaWhattic");
  T(insert, varchar, "Quadratic", int, 5, int, 3, varchar, "What", varchar, "QuadWhatic");
  T(insert, varchar, "Quadratic", int, 6, int, 3, varchar, "What", varchar, "QuadrWhatc");
  T(insert, varchar, "Quadratic", int, 7, int, 3, varchar, "What", varchar, "QuadraWhat");
  T(insert, varchar, "Quadratic", int, 8, int, 3, varchar, "What", varchar, "QuadratWhat");
  T(insert, varchar, "Quadratic", int, 9, int, 3, varchar, "What", varchar, "QuadratiWhat");
  T(insert, varchar, "Quadratic", int, 10, int, 3, varchar, "What", varchar, "Quadratic");
  T(insert, varchar, "Quadratic", int, 11, int, 3, varchar, "What", varchar, "Quadratic");

  T(insert, varchar, "Quadratic", int, -2, int, -1, varchar, "What", varchar, "Quadratic");
  T(insert, varchar, "Quadratic", int, -1, int, -1, varchar, "What", varchar, "Quadratic");
  T(insert, varchar, "Quadratic", int, 0, int, -1, varchar, "What", varchar, "Quadratic");
  T(insert, varchar, "Quadratic", int, 1, int, -1, varchar, "What", varchar, "What");
  T(insert, varchar, "Quadratic", int, 2, int, -1, varchar, "What", varchar, "QWhat");
  T(insert, varchar, "Quadratic", int, 3, int, -1, varchar, "What", varchar, "QuWhat");
  T(insert, varchar, "Quadratic", int, 4, int, -1, varchar, "What", varchar, "QuaWhat");
  T(insert, varchar, "Quadratic", int, 5, int, -1, varchar, "What", varchar, "QuadWhat");
  T(insert, varchar, "Quadratic", int, 6, int, -1, varchar, "What", varchar, "QuadrWhat");
  T(insert, varchar, "Quadratic", int, 7, int, -1, varchar, "What", varchar, "QuadraWhat");
  T(insert, varchar, "Quadratic", int, 8, int, -1, varchar, "What", varchar, "QuadratWhat");
  T(insert, varchar, "Quadratic", int, 9, int, -1, varchar, "What", varchar, "QuadratiWhat");
  T(insert, varchar, "Quadratic", int, 10, int, -1, varchar, "What", varchar, "Quadratic");
  T(insert, varchar, "Quadratic", int, 11, int, -1, varchar, "What", varchar, "Quadratic");

  T(insert, varchar, "Quadratic", int, -1, int, -1, varchar, "", varchar, "Quadratic");
  T(insert, varchar, "Quadratic", int, 0, int, -1, varchar, "", varchar, "Quadratic");
  T(insert, varchar, "Quadratic", int, 1, int, -1, varchar, "", varchar, "");
  T(insert, varchar, "Quadratic", int, 2, int, -1, varchar, "", varchar, "Q");
  T(insert, varchar, "Quadratic", int, 3, int, -1, varchar, "", varchar, "Qu");
  T(insert, varchar, "Quadratic", int, 4, int, -1, varchar, "", varchar, "Qua");
  T(insert, varchar, "Quadratic", int, 5, int, -1, varchar, "", varchar, "Quad");
  T(insert, varchar, "Quadratic", int, 6, int, -1, varchar, "", varchar, "Quadr");
  T(insert, varchar, "Quadratic", int, 7, int, -1, varchar, "", varchar, "Quadra");
  T(insert, varchar, "Quadratic", int, 8, int, -1, varchar, "", varchar, "Quadrat");
  T(insert, varchar, "Quadratic", int, 9, int, -1, varchar, "", varchar, "Quadrati");
  T(insert, varchar, "Quadratic", int, 10, int, -1, varchar, "", varchar, "Quadratic");
  T(insert, varchar, "Quadratic", int, 11, int, -1, varchar, "", varchar, "Quadratic");

  T(insert, varchar, "Quadratic", int, -1, int, 0, varchar, "", varchar, "Quadratic");
  T(insert, varchar, "Quadratic", int, 0, int, 0, varchar, "", varchar, "Quadratic");
  T(insert, varchar, "Quadratic", int, 1, int, 0, varchar, "", varchar, "Quadratic");
  T(insert, varchar, "Quadratic", int, 2, int, 0, varchar, "", varchar, "Quadratic");
  T(insert, varchar, "Quadratic", int, 3, int, 0, varchar, "", varchar, "Quadratic");
  T(insert, varchar, "Quadratic", int, 4, int, 0, varchar, "", varchar, "Quadratic");
  T(insert, varchar, "Quadratic", int, 5, int, 0, varchar, "", varchar, "Quadratic");
  T(insert, varchar, "Quadratic", int, 6, int, 0, varchar, "", varchar, "Quadratic");
  T(insert, varchar, "Quadratic", int, 7, int, 0, varchar, "", varchar, "Quadratic");
  T(insert, varchar, "Quadratic", int, 8, int, 0, varchar, "", varchar, "Quadratic");
  T(insert, varchar, "Quadratic", int, 9, int, 0, varchar, "", varchar, "Quadratic");
  T(insert, varchar, "Quadratic", int, 10, int, 0, varchar, "", varchar, "Quadratic");
  T(insert, varchar, "Quadratic", int, 11, int, 0, varchar, "", varchar, "Quadratic");

  T(insert, varchar, "Quadratic", int, -1, int, 1, varchar, "", varchar, "Quadratic");
  T(insert, varchar, "Quadratic", int, 0, int, 1, varchar, "", varchar, "Quadratic");
  T(insert, varchar, "Quadratic", int, 1, int, 1, varchar, "", varchar, "uadratic");
  T(insert, varchar, "Quadratic", int, 2, int, 1, varchar, "", varchar, "Qadratic");
  T(insert, varchar, "Quadratic", int, 3, int, 1, varchar, "", varchar, "Qudratic");
  T(insert, varchar, "Quadratic", int, 4, int, 1, varchar, "", varchar, "Quaratic");
  T(insert, varchar, "Quadratic", int, 5, int, 1, varchar, "", varchar, "Quadatic");
  T(insert, varchar, "Quadratic", int, 6, int, 1, varchar, "", varchar, "Quadrtic");
  T(insert, varchar, "Quadratic", int, 7, int, 1, varchar, "", varchar, "Quadraic");
  T(insert, varchar, "Quadratic", int, 8, int, 1, varchar, "", varchar, "Quadratc");
  T(insert, varchar, "Quadratic", int, 9, int, 1, varchar, "", varchar, "Quadrati");
  T(insert, varchar, "Quadratic", int, 10, int, 1, varchar, "", varchar, "Quadratic");
  T(insert, varchar, "Quadratic", int, 11, int, 1, varchar, "", varchar, "Quadratic");

  T(insert, varchar, "", int, -2, int, -1, varchar, "What", varchar, "");
  T(insert, varchar, "", int, -1, int, -1, varchar, "What", varchar, "");
  T(insert, varchar, "", int, 0, int, -1, varchar, "What", varchar, "");
  T(insert, varchar, "", int, 1, int, -1, varchar, "What", varchar, "");
  T(insert, varchar, "", int, 2, int, -1, varchar, "What", varchar, "");
  T(insert, varchar, "", int, 3, int, -1, varchar, "What", varchar, "");

  T(insert, varchar, "", int, -2, int, 0, varchar, "What", varchar, "");
  T(insert, varchar, "", int, -1, int, 0, varchar, "What", varchar, "");
  T(insert, varchar, "", int, 0, int, 0, varchar, "What", varchar, "");
  T(insert, varchar, "", int, 1, int, 0, varchar, "What", varchar, "");
  T(insert, varchar, "", int, 2, int, 0, varchar, "What", varchar, "");
  T(insert, varchar, "", int, 3, int, 0, varchar, "What", varchar, "");

  T(insert, varchar, "  ", int, -2, int, 1, varchar, "What", varchar, "  ");
  T(insert, varchar, "  ", int, -1, int, 1, varchar, "What", varchar, "  ");
  T(insert, varchar, "  ", int, 0, int, 1, varchar, "What", varchar, "  ");
  T(insert, varchar, "  ", int, 1, int, 1, varchar, "What", varchar, "What ");
  T(insert, varchar, "  ", int, 2, int, 1, varchar, "What", varchar, " What");
  T(insert, varchar, "  ", int, 3, int, 1, varchar, "What", varchar, "  ");
  T(insert, varchar, "  ", int, 4, int, 1, varchar, "What", varchar, "  ");

  T(insert, varchar, "  ", int, -2, int, 0, varchar, "What", varchar, "  ");
  T(insert, varchar, "  ", int, -1, int, 0, varchar, "What", varchar, "  ");
  T(insert, varchar, "  ", int, 0, int, 0, varchar, "What", varchar, "  ");
  T(insert, varchar, "  ", int, 1, int, 0, varchar, "What", varchar, "What  ");
  T(insert, varchar, "  ", int, 2, int, 0, varchar, "What", varchar, " What ");
  T(insert, varchar, "  ", int, 3, int, 0, varchar, "What", varchar, "  ");
  T(insert, varchar, "  ", int, 4, int, 0, varchar, "What", varchar, "  ");

  T(insert, varchar, "   ", int, -2, int, -1, varchar, "What", varchar, "   ");
  T(insert, varchar, "   ", int, -1, int, -1, varchar, "What", varchar, "   ");
  T(insert, varchar, "   ", int, 0, int, -1, varchar, "What", varchar, "   ");
  T(insert, varchar, "   ", int, 1, int, -1, varchar, "What", varchar, "What");
  T(insert, varchar, "   ", int, 2, int, -1, varchar, "What", varchar, " What");
  T(insert, varchar, "   ", int, 3, int, -1, varchar, "What", varchar, "  What");
  T(insert, varchar, "   ", int, 4, int, -1, varchar, "What", varchar, "   ");
  T(insert, varchar, "   ", int, 5, int, -1, varchar, "What", varchar, "   ");
  T(insert, varchar, "   ", int, 6, int, -1, varchar, "What", varchar, "   ");
  T(insert, varchar, "   ", int, 7, int, -1, varchar, "What", varchar, "   ");
  T(insert, varchar, "   ", int, 8, int, -1, varchar, "What", varchar, "   ");
  T(insert, varchar, "   ", int, 9, int, -1, varchar, "What", varchar, "   ");
  T(insert, varchar, "   ", int, 10, int, -1, varchar, "What", varchar, "   ");
  T(insert, varchar, "   ", int, 11, int, -1, varchar, "What", varchar, "   ");

  /*
  T(insert, varchar, 1, int, -2, int, 0, varchar, "What", varchar, 1);
  T(insert, varchar, 1, int, -1, int, 0, varchar, "What", varchar, 1);
  T(insert, varchar, 1, int, 0, int, 0, varchar, "What", varchar, 1);
  T(insert, varchar, 1, int, 1, int, 0, varchar, "What", varchar, "1What");
  T(insert, varchar, 1, int, 2, int, 0, varchar, "What", varchar, "1What");
  T(insert, varchar, 1, int, 3, int, 0, varchar, "What", varchar, 1);

  T(insert, varchar, 1, int, -2, int, 1, varchar, "What", varchar, 1);
  T(insert, varchar, 1, int, -1, int, 1, varchar, "What", varchar, 1);
  T(insert, varchar, 1, int, 0, int, 1, varchar, "What", varchar, 1);
  T(insert, varchar, 1, int, 1, int, 1, varchar, "What", varchar, "What");
  T(insert, varchar, 1, int, 2, int, 1, varchar, "What", varchar, "1What");
  T(insert, varchar, 1, int, 3, int, 1, varchar, "What", varchar, 1);
  */

  T(insert, varchar, "中国是个美丽的地方", int, -2, int, -1, varchar, "北京", varchar, "中国是个美丽的地方");
  T(insert, varchar, "中国是个美丽的地方", int, -1, int, -1, varchar, "北京", varchar, "中国是个美丽的地方");
  T(insert, varchar, "中国是个美丽的地方", int, 0, int, -1, varchar, "北京", varchar, "中国是个美丽的地方");
  T(insert, varchar, "中国是个美丽的地方", int, 1, int, -1, varchar, "北京", varchar, "北京");
  T(insert, varchar, "中国是个美丽的地方", int, 2, int, -1, varchar, "北京", varchar, "中北京");
  T(insert, varchar, "中国是个美丽的地方", int, 3, int, -1, varchar, "北京", varchar, "中国北京");
  T(insert, varchar, "中国是个美丽的地方", int, 4, int, -1, varchar, "北京", varchar, "中国是北京");
  T(insert, varchar, "中国是个美丽的地方", int, 5, int, -1, varchar, "北京", varchar, "中国是个北京");
  T(insert, varchar, "中国是个美丽的地方", int, 6, int, -1, varchar, "北京", varchar, "中国是个美北京");
  T(insert, varchar, "中国是个美丽的地方", int, 7, int, -1, varchar, "北京", varchar, "中国是个美丽北京");
  T(insert, varchar, "中国是个美丽的地方", int, 8, int, -1, varchar, "北京", varchar, "中国是个美丽的北京");
  T(insert, varchar, "中国是个美丽的地方", int, 9, int, -1, varchar, "北京", varchar, "中国是个美丽的地北京");
  T(insert, varchar, "中国是个美丽的地方", int, 10, int, -1, varchar, "北京", varchar, "中国是个美丽的地方");
  T(insert, varchar, "中国是个美丽的地方", int, 11, int, -1, varchar, "北京", varchar, "中国是个美丽的地方");

  T(insert, varchar, "中国是个美丽的地方", int, -1, int, 1, varchar, "北京", varchar, "中国是个美丽的地方");
  T(insert, varchar, "中国是个美丽的地方", int, 0, int, 1, varchar, "北京", varchar, "中国是个美丽的地方");
  T(insert, varchar, "中国是个美丽的地方", int, 1, int, 1, varchar, "北京", varchar, "北京国是个美丽的地方");
  T(insert, varchar, "中国是个美丽的地方", int, 2, int, 1, varchar, "北京", varchar, "中北京是个美丽的地方");
  T(insert, varchar, "中国是个美丽的地方", int, 3, int, 1, varchar, "北京", varchar, "中国北京个美丽的地方");
  T(insert, varchar, "中国是个美丽的地方", int, 4, int, 1, varchar, "北京", varchar, "中国是北京美丽的地方");
  T(insert, varchar, "中国是个美丽的地方", int, 5, int, 1, varchar, "北京", varchar, "中国是个北京丽的地方");
  T(insert, varchar, "中国是个美丽的地方", int, 6, int, 1, varchar, "北京", varchar, "中国是个美北京的地方");
  T(insert, varchar, "中国是个美丽的地方", int, 7, int, 1, varchar, "北京", varchar, "中国是个美丽北京地方");
  T(insert, varchar, "中国是个美丽的地方", int, 8, int, 1, varchar, "北京", varchar, "中国是个美丽的北京方");
  T(insert, varchar, "中国是个美丽的地方", int, 9, int, 1, varchar, "北京", varchar, "中国是个美丽的地北京");
  T(insert, varchar, "中国是个美丽的地方", int, 10, int, 1, varchar, "北京", varchar, "中国是个美丽的地方");
  T(insert, varchar, "中国是个美丽的地方", int, 11, int, 1, varchar, "北京", varchar, "中国是个美丽的地方");

  T(insert, varchar, "中国是个美丽的地方abc", int, -2, int, -1, varchar, "北京", varchar, "中国是个美丽的地方abc");
  T(insert, varchar, "中国是个美丽的地方abc", int, -1, int, -1, varchar, "北京", varchar, "中国是个美丽的地方abc");
  T(insert, varchar, "中国是个美丽的地方abc", int, 0, int, -1, varchar, "北京", varchar, "中国是个美丽的地方abc");
  T(insert, varchar, "中国是个美丽的地方abc", int, 1, int, -1, varchar, "北京", varchar, "北京");
  T(insert, varchar, "中国是个美丽的地方abc", int, 2, int, -1, varchar, "北京", varchar, "中北京");
  T(insert, varchar, "中国是个美丽的地方abc", int, 3, int, -1, varchar, "北京", varchar, "中国北京");
  T(insert, varchar, "中国是个美丽的地方abc", int, 4, int, -1, varchar, "北京", varchar, "中国是北京");
  T(insert, varchar, "中国是个美丽的地方abc", int, 5, int, -1, varchar, "北京", varchar, "中国是个北京");
  T(insert, varchar, "中国是个美丽的地方abc", int, 6, int, -1, varchar, "北京", varchar, "中国是个美北京");
  T(insert, varchar, "中国是个美丽的地方abc", int, 7, int, -1, varchar, "北京", varchar, "中国是个美丽北京");
  T(insert, varchar, "中国是个美丽的地方abc", int, 8, int, -1, varchar, "北京", varchar, "中国是个美丽的北京");
  T(insert, varchar, "中国是个美丽的地方abc", int, 9, int, -1, varchar, "北京", varchar, "中国是个美丽的地北京");
  T(insert, varchar, "中国是个美丽的地方abc", int, 10, int, -1, varchar, "北京", varchar, "中国是个美丽的地方北京");
  T(insert, varchar, "中国是个美丽的地方abc", int, 11, int, -1, varchar, "北京", varchar, "中国是个美丽的地方a北京");
  T(insert, varchar, "中国是个美丽的地方abc", int, 12, int, -1, varchar, "北京", varchar, "中国是个美丽的地方ab北京");
  T(insert, varchar, "中国是个美丽的地方abc", int, 13, int, -1, varchar, "北京", varchar, "中国是个美丽的地方abc");
  T(insert, varchar, "中国是个美丽的地方abc", int, 14, int, -1, varchar, "北京", varchar, "中国是个美丽的地方abc");
  T(insert, varchar, "中国是个美丽的地方abc", int, 15, int, -1, varchar, "北京", varchar, "中国是个美丽的地方abc");

  T(insert, varchar, "中国是个美丽的地方abc", int, -1, int, 1, varchar, "北京", varchar, "中国是个美丽的地方abc");
  T(insert, varchar, "中国是个美丽的地方abc", int, 0, int, 1, varchar, "北京", varchar, "中国是个美丽的地方abc");
  T(insert, varchar, "中国是个美丽的地方abc", int, 1, int, 1, varchar, "北京", varchar, "北京国是个美丽的地方abc");
  T(insert, varchar, "中国是个美丽的地方abc", int, 2, int, 1, varchar, "北京", varchar, "中北京是个美丽的地方abc");
  T(insert, varchar, "中国是个美丽的地方abc", int, 3, int, 1, varchar, "北京", varchar, "中国北京个美丽的地方abc");
  T(insert, varchar, "中国是个美丽的地方abc", int, 4, int, 1, varchar, "北京", varchar, "中国是北京美丽的地方abc");
  T(insert, varchar, "中国是个美丽的地方abc", int, 5, int, 1, varchar, "北京", varchar, "中国是个北京丽的地方abc");
  T(insert, varchar, "中国是个美丽的地方abc", int, 6, int, 1, varchar, "北京", varchar, "中国是个美北京的地方abc");
  T(insert, varchar, "中国是个美丽的地方abc", int, 7, int, 1, varchar, "北京", varchar, "中国是个美丽北京地方abc");
  T(insert, varchar, "中国是个美丽的地方abc", int, 8, int, 1, varchar, "北京", varchar, "中国是个美丽的北京方abc");
  T(insert, varchar, "中国是个美丽的地方abc", int, 9, int, 1, varchar, "北京", varchar, "中国是个美丽的地北京abc");
  T(insert, varchar, "中国是个美丽的地方abc", int, 10, int, 1, varchar, "北京", varchar, "中国是个美丽的地方北京bc");
  T(insert, varchar, "中国是个美丽的地方abc", int, 11, int, 1, varchar, "北京", varchar, "中国是个美丽的地方a北京c");
  T(insert, varchar, "中国是个美丽的地方abc", int, 12, int, 1, varchar, "北京", varchar, "中国是个美丽的地方ab北京");
  T(insert, varchar, "中国是个美丽的地方abc", int, 13, int, 1, varchar, "北京", varchar, "中国是个美丽的地方abc");
  T(insert, varchar, "中国是个美丽的地方abc", int, 14, int, 1, varchar, "北京", varchar, "中国是个美丽的地方abc");

}

TEST_F(ObExprInsertTest, fail_test)
{
  ObArenaAllocator buf(ObModIds::OB_SQL_SESSION);
  ObExprInsert insert(buf);

  ASSERT_EQ(ObExprOperator::MORE_THAN_TWO, insert.get_param_num());
  F(insert, varchar, "Quadratic", int, 1, max_value, , varchar, "What", varchar, "Quadratic");
  F(insert, varchar, "Quadratic", int, 1, min_value,  , varchar, "What", varchar, "Quadratic");
  F(insert, varchar, "Quadratic", min_value,  , int, 1, varchar, "What", varchar, "Quadratic");
  F(insert, varchar, "Quadratic", max_value,  , int, 1, varchar, "What", varchar, "Quadratic");
  F(insert, int, 1, min_value,  , min_value,  , varchar, "1", varchar, "Quadratic");
  F(insert, double, 1.3, min_value,  , min_value,  , varchar, "1.300000", varchar, "Quadratic");

}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

