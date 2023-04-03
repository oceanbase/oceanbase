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

#define USING_LOG_PREFIX SQL_ENG
#include <gtest/gtest.h>
#include "../../../../src/sql/engine/expr/ob_expr_reverse.h"
#include "ob_expr_test_utils.h"
#include "lib/oblog/ob_log.h"
#include <iostream>
using namespace oceanbase::common;
using namespace oceanbase::sql;


class ObExprFuncReverseTest: public ::testing::Test {
public:
  ObExprFuncReverseTest();
  virtual ~ObExprFuncReverseTest();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  ObExprFuncReverseTest(const ObExprFuncReverseTest &other);
  ObExprFuncReverseTest& operator=(const ObExprFuncReverseTest &other);
protected:
  // data members
};

ObExprFuncReverseTest::ObExprFuncReverseTest() {
}

ObExprFuncReverseTest::~ObExprFuncReverseTest() {
}

void ObExprFuncReverseTest::SetUp() {
}

void ObExprFuncReverseTest::TearDown() {
}

TEST_OPERATOR(ObExprReverse);
TEST_F(ObExprFuncReverseTest,simple)
{
  ObMalloc buf;
  TestObExprReverse rev;
  EXPECT_RESULT1(rev,&buf,calc_result1,varchar,"12345",varchar,"54321");
  EXPECT_RESULT1(rev,&buf,calc_result1,varchar,"1",varchar,"1");
  EXPECT_RESULT1(rev,&buf,calc_result1,varchar,"",varchar,"");

  EXPECT_RESULT1(rev,&buf,calc_result1,varchar,"ABCDE",varchar,"EDCBA");
  EXPECT_RESULT1(rev,&buf,calc_result1,varchar,"F",varchar,"F");

}

TEST_F(ObExprFuncReverseTest,cast)
{
  ObMalloc buf;
  TestObExprReverse rev;
  EXPECT_RESULT1(rev,&buf,calc_result1,double,123.45,varchar,"54.321");
  EXPECT_RESULT1(rev,&buf,calc_result1,int,12345,varchar,"54321");
  EXPECT_RESULT1(rev,&buf,calc_result1,float,static_cast<float>(123.45),varchar,"54.321");
}

TEST_F(ObExprFuncReverseTest,utf8)
{
  ObMalloc buf;
  TestObExprReverse rev;
  EXPECT_RESULT1(rev,&buf,calc_result1,varchar,"上山打老虎",varchar,"虎老打山上");
  EXPECT_RESULT1(rev,&buf,calc_result1,varchar,"12345上山打老虎",varchar,"虎老打山上54321");
  EXPECT_RESULT1(rev,&buf,calc_result1,varchar,"上山打老虎12345",varchar,"54321虎老打山上");
  EXPECT_RESULT1(rev,&buf,calc_result1,varchar,"123上山打老虎45",varchar,"54虎老打山上321");

  EXPECT_RESULT1(rev,&buf,calc_result1,char,"上山打老虎",char,"虎老打山上");
  EXPECT_RESULT1(rev,&buf,calc_result1,char,"12345上山打老虎",char,"虎老打山上54321");
  EXPECT_RESULT1(rev,&buf,calc_result1,char,"上山打老虎12345",char,"54321虎老打山上");
  EXPECT_RESULT1(rev,&buf,calc_result1,char,"123上山打老虎45",char,"54虎老打山上321");

  EXPECT_RESULT1(rev,&buf,calc_result1,varchar,"上",varchar,"上");
}

#define REVERSE_B(str,res_str) EXPECT_RESULT1_CT(rev,&buf,calc_result1,binary,str,CS_TYPE_BINARY,binary,res_str,CS_TYPE_BINARY)
TEST_F(ObExprFuncReverseTest,binary)
{
  ObMalloc buf;
  TestObExprReverse rev;
  REVERSE_B("\xAA\xBB\xCC","\xCC\xBB\xAA");
  REVERSE_B("\xAA","\xAA");
  REVERSE_B("","");
}

TEST_F(ObExprFuncReverseTest,null)
{
  ObMalloc buf;
  TestObExprReverse rev;
  EXPECT_RESULT1(rev,&buf,calc_result1,null,,null,);
}

TEST_F(ObExprFuncReverseTest,invalid_encoding)
{
  ObMalloc buf;
  TestObExprReverse rev;
  EXPECT_FAIL_RESULT1_CT(rev,&buf,calc_result1,char,"上山打老\xE4\xBD\x00",CS_TYPE_UTF8MB4_GENERAL_CI);
  EXPECT_FAIL_RESULT1_CT(rev,&buf,calc_result1,char,"上山打老\xE4\xBD",CS_TYPE_UTF8MB4_GENERAL_CI);
  EXPECT_FAIL_RESULT1_CT(rev,&buf,calc_result1,char,"上山\xE4打老",CS_TYPE_UTF8MB4_GENERAL_CI);
  EXPECT_FAIL_RESULT1_CT(rev,&buf,calc_result1,char,"上\xFF山打老",CS_TYPE_UTF8MB4_GENERAL_CI);
  EXPECT_FAIL_RESULT1_CT(rev,&buf,calc_result1,char,"上山打\xC0老上山打老",CS_TYPE_UTF8MB4_GENERAL_CI);
  EXPECT_FAIL_RESULT1_CT(rev,&buf,calc_result1,char,"上山打\xC0\xFF老上山打老",CS_TYPE_UTF8MB4_GENERAL_CI);
  EXPECT_FAIL_RESULT1_CT(rev,&buf,calc_result1,char,"上山打\xC0老上山打\xFF老",CS_TYPE_UTF8MB4_GENERAL_CI);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
