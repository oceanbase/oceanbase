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
#include "sql/engine/expr/ob_expr_quote.h"
#include "lib/charset/ob_charset.h"
#include "ob_expr_test_utils.h"
#include "lib/oblog/ob_log.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

class TestAllocator: public ObIAllocator
{
public:
  TestAllocator() :
      label_(ObModIds::TEST) {}
  virtual ~TestAllocator() {}
  void *alloc(const int64_t sz) {
    UNUSED(sz);
    return NULL;
  }
  void free(void *p) {
    UNUSED(p);
  }
  void freed(const int64_t sz) {
    UNUSED(sz);
  }
  void set_label(const char *label) {
    label_ = label;
  }
  ;
private:
  const char *label_;
};

class ObExprQuoteTest: public ::testing::Test
{
public:
  ObExprQuoteTest();
  virtual ~ObExprQuoteTest();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  ObExprQuoteTest(const ObExprQuoteTest &other);
  ObExprQuoteTest& operator=(const ObExprQuoteTest &other);
private:
  // data members
};

ObExprQuoteTest::ObExprQuoteTest() {}
ObExprQuoteTest::~ObExprQuoteTest() {}
void ObExprQuoteTest::SetUp() {}
void ObExprQuoteTest::TearDown() {}

#define TEST_NULL(str_op_object, str_buf, func)\
{\
	ObObj res;\
	ObObj val;\
	val.set_null();\
	ObExprCtx expr_ctx(NULL, NULL, NULL, str_buf);      \
	int err = str_op_object.func(res, val, expr_ctx); \
	ASSERT_TRUE(OB_SUCCESS == err); \
}
#define TEST_MEMOEY_RESULT(obj,str_buf,func,t1,v1)\
{\
	ObObj obj1;\
	ObObj res;\
	obj1.set_##t1(v1);\
	ObExprCtx expr_ctx(NULL, NULL, NULL, str_buf);      \
	int ret = obj.func(res, obj1, expr_ctx);\
	ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, ret);   \
}
#define EXPECT_RESULTN1(str_op_object, str_buf, func, type1, v1,v2, ref_type, ref_value) \
{                               \
 ObObj t1;                  \
 ObObj r;                   \
 ObObj ref;                 \
 t1.set_##type1(v1,v2);            \
 ref.set_##ref_type(ref_value); \
 ObExprCtx expr_ctx(NULL, NULL, NULL, str_buf);      \
 int err = str_op_object.func(r, t1, expr_ctx); \
 _OB_LOG(INFO, "ref=%s r=%s", to_cstring(ref), to_cstring(r)); \
 EXPECT_TRUE(OB_SUCCESS == err); \
 ASSERT_TRUE(ref.get_type() == r.get_type()); \
 if (ref.get_type() != ObNullType) \
 { \
   EXPECT_TRUE(ObObjCmpFuncs::compare_oper_nullsafe(ref, r, CS_TYPE_UTF8MB4_BIN, CO_EQ));\
 } \
} while(0)
#define TTN(typ, val1, val2, res_val, res_typ) EXPECT_RESULTN1(obj, &buf, calc_result1, typ, val1, val2, res_val, res_typ)
#define TN(typ, val, res_val, res_typ) EXPECT_RESULT1(obj, &buf, calc_result1, typ, val, res_val, res_typ)
#define FN(typ, val) EXPECT_FAIL_RESULT1(obj, &buf, calc_result1, typ, val)
#define TNN() TEST_NULL(obj, &buf, calc_result1)
#define TMR(buf, t1,v1) TEST_MEMOEY_RESULT(obj, buf, calc_result1, t1, v1)
TEST_F(ObExprQuoteTest, basic_test)
{
  DefaultPageAllocator buf;
  ObExprQuote obj(buf);
  //case	1.	select quote('Don\'t');
  TN(varchar, "Don\'t", varchar, "\'Don\\'t\'");
  //case	2.	select quote('fdsfs');
  TN(varchar, "fdsfs", varchar, "\'fdsfs\'");
  //case	3.	select quote('dsfedfe\nsdfdgfsf'fdfe'sedf');
  TN(varchar, "dsfedfesdfdgfsf'fdfe'sedf", varchar,
      "\'dsfedfesdfdgfsf\\'fdfe\\'sedf\'");
  //case	4.	select quote('sdfs\rsdfs');
  TN(varchar, "sdfssdfs", varchar, "\'sdfssdfs\'");
  //case	5.	select quote();
  TN(varchar, "", varchar, "\'\'");
  //case  6.  select quote("\"342343dcd\\efe");
  TN(varchar, "34234\0323dcd\\efe", varchar, "\'34234\\Z3dcd\\\\efe\'");
  //case  7.  select quote()
  TTN(varchar, "34234\0g3defe", 12, varchar, "\'34234\\0g3defe\'");
}
TEST_F(ObExprQuoteTest, special_test)
{
  DefaultPageAllocator buf;
  ObExprQuote obj(buf);
  TestAllocator tbuf;
  //case	1.	select quote(342342342342.3434);
  TN(double, 342.3, varchar, "\'342.3\'");
  //case	2.	select quote(1213);
  TN(int, 1213, varchar, "\'1213\'");
  //case	3.	select quote(null);
  TNN();
  //case	4.	select quote('sds'); testing "allocate memory failed"
  TMR(&tbuf, varchar, "sds");
  //case	5.	select quote('dfsafewfqer32rfewfr23rf43rf3245432rf32q2rfew');testing "extend length limit"
  //FN(varchar, "dfsafewfqer32rfewfr23rf43rf3245432rf32q2rfew");
}
int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
