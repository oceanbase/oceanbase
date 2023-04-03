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
#include "../../../../src/sql/engine/expr/ob_expr_right.h"
#include "ob_expr_test_utils.h"
#include "lib/oblog/ob_log.h"
#include <iostream>
using namespace oceanbase::common;
using namespace oceanbase::sql;


class ObExprFuncRightTest: public ::testing::Test {
public:
  ObExprFuncRightTest();
  virtual ~ObExprFuncRightTest();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  ObExprFuncRightTest(const ObExprFuncRightTest &other);
  ObExprFuncRightTest& operator=(const ObExprFuncRightTest &other);
protected:
  // data members
};

ObExprFuncRightTest::ObExprFuncRightTest() {
}

ObExprFuncRightTest::~ObExprFuncRightTest() {
}

void ObExprFuncRightTest::SetUp() {
}

void ObExprFuncRightTest::TearDown() {
}

#define BINARY_RIGHT(str,count,res_str) EXPECT_RESULT2_CT(right,&buf,calc_result2,binary,str,CS_TYPE_BINARY,int,count,CS_TYPE_BINARY,binary,res_str,CS_TYPE_BINARY)

TEST_OPERATOR(ObExprRight);

TEST_F(ObExprFuncRightTest,ascii)
{
  ObMalloc buf;
  TestObExprRight rev;
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,varchar,"12345abcde",int,-4,varchar,"");
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,varchar,"12345abcde",int,0,varchar,"");
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,varchar,"12345abcde",int,2,varchar,"de");
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,varchar,"12345abcde",int,5,varchar,"abcde");
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,varchar,"12345abcde",int,8,varchar,"345abcde");
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,varchar,"12345abcde",int,10,varchar,"12345abcde");
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,varchar,"12345abcde",int,15,varchar,"12345abcde");

  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,char,"12345abcde",int,-4,char,"");
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,char,"12345abcde",int,0,char,"");
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,char,"12345abcde",int,2,char,"de");
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,char,"12345abcde",int,5,char,"abcde");
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,char,"12345abcde",int,8,char,"345abcde");
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,char,"12345abcde",int,10,char,"12345abcde");
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,char,"12345abcde",int,15,char,"12345abcde");
}

TEST_F(ObExprFuncRightTest,utf8)
{
  ObMalloc buf;
  TestObExprRight rev;
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,varchar,"上山打老虎",int,-4,varchar,"");
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,varchar,"上山打老虎",int,0,varchar,"");
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,varchar,"上山打老虎",int,2,varchar,"老虎");
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,varchar,"上山打老虎",int,4,varchar,"山打老虎");
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,varchar,"上山打老虎",int,5,varchar,"上山打老虎");
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,varchar,"上山打老虎",int,10,varchar,"上山打老虎");

  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,char,"上山打老虎",int,-4,char,"");
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,char,"上山打老虎",int,0,char,"");
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,char,"上山打老虎",int,2,char,"老虎");
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,char,"上山打老虎",int,4,char,"山打老虎");
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,char,"上山打老虎",int,5,char,"上山打老虎");
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,char,"上山打老虎",int,10,char,"上山打老虎");


}

TEST_F(ObExprFuncRightTest,utf8_mixed)
{
  ObMalloc buf;
  TestObExprRight rev;
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,varchar,"1上a2山b3打c4老d5虎e",int,-4,varchar,"");
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,varchar,"1上a2山b3打c4老d5虎e",int,0,varchar,"");
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,varchar,"1上a2山b3打c4老d5虎e",int,1,varchar,"e");
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,varchar,"1上a2山b3打c4老d5虎e",int,2,varchar,"虎e");
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,varchar,"1上a2山b3打c4老d5虎e",int,3,varchar,"5虎e");
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,varchar,"1上a2山b3打c4老d5虎e",int,7,varchar,"c4老d5虎e");
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,varchar,"1上a2山b3打c4老d5虎e",int,15,varchar,"1上a2山b3打c4老d5虎e");
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,varchar,"1上a2山b3打c4老d5虎e",int,42,varchar,"1上a2山b3打c4老d5虎e");

  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,char,"1上a2山b3打c4老d5虎e",int,-4,char,"");
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,char,"1上a2山b3打c4老d5虎e",int,0,char,"");
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,char,"1上a2山b3打c4老d5虎e",int,1,char,"e");
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,char,"1上a2山b3打c4老d5虎e",int,2,char,"虎e");
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,char,"1上a2山b3打c4老d5虎e",int,3,char,"5虎e");
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,char,"1上a2山b3打c4老d5虎e",int,7,char,"c4老d5虎e");
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,char,"1上a2山b3打c4老d5虎e",int,15,char,"1上a2山b3打c4老d5虎e");
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,char,"1上a2山b3打c4老d5虎e",int,42,char,"1上a2山b3打c4老d5虎e");
}


TEST_F(ObExprFuncRightTest,binary)
{
  ObMalloc buf;
  TestObExprRight right;
  BINARY_RIGHT("\xDD\xEE\xAA\xDD\xBB\xEE\xEE\xFF",-4,"");
  BINARY_RIGHT("\xDD\xEE\xAA\xDD\xBB\xEE\xEE\xFF",0,"");
  BINARY_RIGHT("\xDD\xEE\xAA\xDD\xBB\xEE\xEE\xFF",1,"\xFF");
  BINARY_RIGHT("\xDD\xEE\xAA\xDD\xBB\xEE\xEE\xFF",2,"\xEE\xFF");
  BINARY_RIGHT("\xDD\xEE\xAA\xDD\xBB\xEE\xEE\xFF",4,"\xBB\xEE\xEE\xFF");
  BINARY_RIGHT("\xDD\xEE\xAA\xDD\xBB\xEE\xEE\xFF",7,"\xEE\xAA\xDD\xBB\xEE\xEE\xFF");
  BINARY_RIGHT("\xDD\xEE\xAA\xDD\xBB\xEE\xEE\xFF",8,"\xDD\xEE\xAA\xDD\xBB\xEE\xEE\xFF");
  BINARY_RIGHT("\xDD\xEE\xAA\xDD\xBB\xEE\xEE\xFF",42,"\xDD\xEE\xAA\xDD\xBB\xEE\xEE\xFF");
}


 TEST_F(ObExprFuncRightTest,casted)
{
  ObMalloc buf;
  TestObExprRight rev;
  number::ObNumber num;
  num.from("-42.466778", buf);

  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,int,12345,int,-4,varchar,"");
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,int,12345,int,0,varchar,"");
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,int,12345,int,1,varchar,"5");
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,int,12345,int,3,varchar,"345");
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,int,12345,int,5,varchar,"12345");
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,int,12345,int,10,varchar,"12345");

  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,smallint,12345,int,-4,varchar,"");
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,smallint,12345,int,0,varchar,"");
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,smallint,12345,int,1,varchar,"5");
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,smallint,12345,int,3,varchar,"345");
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,smallint,12345,int,5,varchar,"12345");
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,smallint,12345,int,10,varchar,"12345");

  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,tinyint,-123,int,-4,varchar,"");
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,tinyint,-123,int,0,varchar,"");
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,tinyint,-123,int,1,varchar,"3");
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,tinyint,-123,int,3,varchar,"123");
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,tinyint,-123,int,4,varchar,"-123");
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,tinyint,-123,int,10,varchar,"-123");


  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,double,-123.45,int,-3,varchar,"");
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,double,-123.45,int,0,varchar,"");
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,double,-123.45,int,3,varchar,".45");
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,double,-123.45,int,7,varchar,"-123.45");
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,double,-123.45,int,10,varchar,"-123.45");
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,double,-123.45,int,11,varchar,"-123.45");
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,double,-123.45,int,16,varchar,"-123.45");

}

TEST_F(ObExprFuncRightTest,invalid_encoding)
{
  ObMalloc buf;
  TestObExprRight rev;
  EXPECT_FAIL_RESULT2_CT(rev,&buf,calc_result2,char,"上山打老\xE4\xBD\x00",CS_TYPE_UTF8MB4_GENERAL_CI,int,3,CS_TYPE_UTF8MB4_GENERAL_CI);
  EXPECT_FAIL_RESULT2_CT(rev,&buf,calc_result2,char,"上山打老\xE4\xBD",CS_TYPE_UTF8MB4_GENERAL_CI,int,3,CS_TYPE_UTF8MB4_GENERAL_CI);
  EXPECT_FAIL_RESULT2_CT(rev,&buf,calc_result2,char,"上山\xE4打老",CS_TYPE_UTF8MB4_GENERAL_CI,int,3,CS_TYPE_UTF8MB4_GENERAL_CI);
  EXPECT_RESULT2_UTF8MB4CI(rev,&buf,calc_result2,char,"上山\xE4打老",int,2,char,"打老");
}


int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
