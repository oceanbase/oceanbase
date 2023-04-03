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
#include "sql/engine/expr/ob_expr_like.h"
#include "sql/engine/expr/ob_expr_like_context.h"
#include "ob_expr_test_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

class ObExprLikeTest : public ::testing::Test
{
 public:
  ObExprLikeTest();
  virtual ~ObExprLikeTest();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  ObExprLikeTest(const ObExprLikeTest &other);
  ObExprLikeTest& operator=(const ObExprLikeTest &other);
private:
  // data members
};
ObExprLikeTest::ObExprLikeTest()
{
}

ObExprLikeTest::~ObExprLikeTest()
{
}

void ObExprLikeTest::SetUp()
{
}

void ObExprLikeTest::TearDown()
{
}

#define T(obj, t1, v1, t2, v2, ref_type, ref_value) EXPECT_RESULT3(obj, &buf, calc_result3, t1, v1, t2, v2, varchar, "\\", ref_type, ref_value)
#define F(obj, t1, v1, t2, v2, ref_type, ref_value) EXPECT_FAIL_RESULT3(obj, &buf, calc_result3, t1, v1, t2, v2, varchar, "\\", ref_type, ref_value)
#define T_ESCAPE(obj, t1, v1, t2, v2, escape, ref_type, ref_value) EXPECT_RESULT3(obj, &buf, calc_result3, t1, v1, t2, v2, varchar, escape, ref_type, ref_value)

/*
TEST_F(ObExprLikeTest, basic_test)
{
  DefaultPageAllocator buf;
  ObExprLike like(buf);
  ObExprResType type;
  type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  like.set_result_type(type);

  ASSERT_EQ(3, like.get_param_num());

  // extend vs extend
  T(like,null, , null, , null, );
  T(like,null, , min_value, , null, );
  T(like,null, , max_value, , null, );
  T(like,min_value, , null, , null, );
  T(like,max_value, , null, , null, );
  T(like,int, 1, null, , null, );
  T(like,null, , int , 0, null, );

  T(like,varchar, "hi", varchar, "hi", int, true);
  T(like,varchar, "hi", varchar, "%i", int, true);
  T(like,varchar, "hi", varchar, "h%", int, true);
  T(like,varchar, "hello", varchar, "%1%", int, false);
  T(like,varchar, "hi", varchar, "i", int, false);
  T(like,varchar, "hi", varchar, "xx<F12>", int, false);
  T(like,varchar, "hello", varchar, "%elo%", int, false);
  T(like,varchar, "hello", varchar, "ell", int, false);
  T(like,int, 123, varchar, "123", int, true);
  T(like,date, 16601, varchar, "2015-06-15", int, true);
  T(like,binary, "hi", char, "hi", int, true);

  T(like, varchar, "abc", varchar, "%b%b_", int, false);
  T(like, varchar, "abc", varchar, "%b%b%", int, false);

  T_ESCAPE(like, varchar, "%hello", varchar, "/%hello", "/", int, true);
  T_ESCAPE(like, varchar, "_hello", varchar, "\\_hello", "\\", int, true);
  T_ESCAPE(like, varchar, "%hello", varchar, "|%hello", "|", int, true);
  T_ESCAPE(like, varchar, "aaa_a", varchar, "%|_%", "|", int, true);
}

TEST_F(ObExprLikeTest, fail_like_basic_test)
{
  DefaultPageAllocator buf;
  ObExprLike like(buf);
  ObExprResType type;
  type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  like.set_result_type(type);

  F(like,max_value, , min_value, , null, );
  F(like,max_value, , max_value, , null, );
  T(like,int, 1, int, 2, int, 0);
  T(like,int, 1, int, 1, int, 1);

  // int vs special
  F(like,int, 1, min_value, , null, );
  F(like,int, 1, max_value, , null, );
  //F(like,min_value, , int, 1, null, );
  //F(like,max_value, , int, 1, null, );

  // int vs normal
  T(like,int, 1, varchar, "1", int, 1);
  //todo
  //1.0打印成字符串时，末尾的0被去掉了
  //T(like,int, 1, double, 1.0, int, 0);
  T(like,int, 1, varchar, "h1", int, 0);
  T(like,int, 0, varchar, "", int, 0);

  T(like,varchar, "1", bool, true, int, 1);
  T(like,int, 1, bool, true, int, 1);
  F(like,min_value, , min_value, , null, );
  F(like,min_value, , max_value, , null, );
}




#define TYPE(t1, t2, res) ARITH_EXPECT_TYPE3(ObExprLike, calc_result_type3, t1, t2, ObVarcharType, res)
#define TYPE_F(t1, t2) ARITH_EXPECT_TYPE_ERROR3(ObExprLike, calc_result_type3, t1, t2, ObVarcharType)
#define TYPE_F2(t1, t2) ARITH_EXPECT_TYPE_ERROR4(ObExprLike, calc_result_type3, t1, t2, ObVarcharType)

TEST_F(ObExprLikeTest, fail_interface_test)
{
  DefaultPageAllocator buf;
  ObExprLike like(buf);
  TYPE_F2(ObExtendType, ObNullType);
  TYPE_F(ObExtendType, ObExtendType);
  TYPE_F2(ObExtendType, ObNullType);
  TYPE(ObNullType, ObNullType, ObIntType);
  TYPE(ObNullType, ObIntType, ObIntType);
}

TEST_F(ObExprLikeTest, interface_test)
{
  DefaultPageAllocator buf;
  ObExprLike like(buf);
  TYPE(ObIntType, ObIntType, ObIntType);
  TYPE(ObIntType, ObVarcharType, ObIntType);
  TYPE(ObVarcharType, ObVarcharType, ObIntType);
}

TEST_F(ObExprLikeTest, fail_not_like_interface_test)
{
  DefaultPageAllocator buf;
  ObExprLike like(buf);
  TYPE_F2(ObExtendType, ObNullType);
  TYPE_F(ObExtendType, ObExtendType);
  TYPE_F2(ObExtendType, ObNullType);
  TYPE(ObNullType, ObNullType, ObIntType);
  TYPE(ObNullType, ObIntType, ObIntType);
}

TEST_F(ObExprLikeTest, not_like_interface_test)
{
  DefaultPageAllocator buf;
  ObExprLike like(buf);
  TYPE(ObIntType, ObIntType, ObIntType);
  TYPE(ObIntType, ObVarcharType, ObIntType);
  TYPE(ObVarcharType, ObVarcharType, ObIntType);
}

TEST_F(ObExprLikeTest, is_substr_mode)
{
  DefaultPageAllocator buf;
  ObString text = "nothing";
  ObString y2= "%";
  ObString y3= "%%";
  ObString y4= "%%%";
  ObString y5= "%%%%";
  ObString y6= "%ABCD%";
  ObString y7= "A%%";
  ObString y8= "%%A";
  ObString y9= "%%A%%";
  ObString y10= "%A%%";
  ObString y11= "%AB%C%";
  ObString escape= "\\";
  SubStrInfo ssinfo;
  ObCollationType cs_type =  CS_TYPE_UTF8MB4_BIN;
  ObExprLike like(buf);
    ssinfo.substr_mode_ = INVALID_SUBSTR_MODE;
  ssinfo.substr_length_ = 0;
  ObExprLike::set_substr_info(cs_type,text,
                        y2,
                        escape,
                        cs_type, ssinfo);
  ASSERT_EQ(START_END_WITH_PERCENT_SIGN, ssinfo.substr_mode_);
  ASSERT_EQ(0, ssinfo.substr_length_);
    ssinfo.substr_mode_ = INVALID_SUBSTR_MODE;
  ssinfo.substr_length_ = 0;
  ObExprLike::set_substr_info(cs_type,text,
                        y3,
                        escape,
                        cs_type, ssinfo);
  ASSERT_EQ(START_END_WITH_PERCENT_SIGN, ssinfo.substr_mode_);
  ASSERT_EQ(0, ssinfo.substr_length_);
    ssinfo.substr_mode_ = INVALID_SUBSTR_MODE;
  ssinfo.substr_length_ = 0;
  ObExprLike::set_substr_info(cs_type,text,
                        y4,
                        escape,
                        cs_type, ssinfo);
  ASSERT_EQ(START_END_WITH_PERCENT_SIGN, ssinfo.substr_mode_);
  ASSERT_EQ(0, ssinfo.substr_length_);
    ssinfo.substr_mode_ = INVALID_SUBSTR_MODE;
  ssinfo.substr_length_ = 0;
  ObExprLike::set_substr_info(cs_type,text,
                        y5,
                        escape,
                        cs_type, ssinfo);
  ASSERT_EQ(START_END_WITH_PERCENT_SIGN, ssinfo.substr_mode_);
  ASSERT_EQ(0, ssinfo.substr_length_);
    ssinfo.substr_mode_ = INVALID_SUBSTR_MODE;
  ssinfo.substr_length_ = 0;
  ObExprLike::set_substr_info(cs_type,text,
                        y6,
                        escape,
                        cs_type, ssinfo);
  ASSERT_EQ(START_END_WITH_PERCENT_SIGN, ssinfo.substr_mode_);
  ASSERT_EQ(4, ssinfo.substr_length_);
    ssinfo.substr_mode_ = INVALID_SUBSTR_MODE;
  ssinfo.substr_length_ = 0;
  ObExprLike::set_substr_info(cs_type,text,
                        y7,
                        escape,
                        cs_type, ssinfo);
  ASSERT_EQ(END_WITH_PERCENT_SIGN, ssinfo.substr_mode_);
  ASSERT_EQ(1, ssinfo.substr_length_);
    ssinfo.substr_mode_ = INVALID_SUBSTR_MODE;
  ssinfo.substr_length_ = 0;
  ObExprLike::set_substr_info(cs_type,text,
                        y8,
                        escape,
                        cs_type, ssinfo);
  ASSERT_EQ(START_WITH_PERCENT_SIGN, ssinfo.substr_mode_);
  ASSERT_EQ(1, ssinfo.substr_length_);
    ssinfo.substr_mode_ = INVALID_SUBSTR_MODE;
  ssinfo.substr_length_ = 0;
  ObExprLike::set_substr_info(cs_type,text,
                        y9,
                        escape,
                        cs_type, ssinfo);
  ASSERT_EQ(START_END_WITH_PERCENT_SIGN, ssinfo.substr_mode_);
  ASSERT_EQ(1, ssinfo.substr_length_);
    ssinfo.substr_mode_ = INVALID_SUBSTR_MODE;
  ssinfo.substr_length_ = 0;
  ObExprLike::set_substr_info(cs_type,text,
                        y10,
                        escape,
                        cs_type, ssinfo);
  ASSERT_EQ(START_END_WITH_PERCENT_SIGN, ssinfo.substr_mode_);
  ASSERT_EQ(1, ssinfo.substr_length_);
    ssinfo.substr_mode_ = INVALID_SUBSTR_MODE;
  ssinfo.substr_length_ = 0;
  ObExprLike::set_substr_info(cs_type,text,
                        y11,
                        escape,
                        cs_type, ssinfo);
  ASSERT_EQ(INVALID_SUBSTR_MODE, ssinfo.substr_mode_);
}
*/

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
