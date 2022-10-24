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
#include "sql/engine/expr/ob_expr_locate.h"
#include "ob_expr_test_utils.h"
#include "lib/oblog/ob_log.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

class TestAllocator: public ObIAllocator
{
public:
  TestAllocator() :
      label_(ObModIds::TEST) {
  }
  virtual ~TestAllocator() {
  }
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

class TestObExprLocateTest: public ::testing::Test
{
public:
  TestObExprLocateTest();
  virtual ~TestObExprLocateTest();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  TestObExprLocateTest(const TestObExprLocateTest &other);
  TestObExprLocateTest& operator=(const TestObExprLocateTest &other);
private:
  // data members
};

TestObExprLocateTest::TestObExprLocateTest() {}

TestObExprLocateTest::~TestObExprLocateTest() {}

void TestObExprLocateTest::SetUp() {}

void TestObExprLocateTest::TearDown() {}

#define TEST_NULL(str_op_object, str_buf, func)\
{\
  ObObj res;\
  ObObj val1;\
  ObObj val2;\
  val1.set_null();\
  val2.set_null();\
  ObExprCtx expr_ctx(NULL, NULL, NULL, str_buf);      \
  int err = str_op_object.func(res, val1, val2, expr_ctx); \
  ASSERT_TRUE(OB_SUCCESS == err); \
}
#define TEST_MEMOEY_RESULT(obj,str_buf,func,t1,v1,t2,v2)\
{\
  ObObj obj1;\
  ObObj obj2;\
  ObObj res;\
  obj1.set_##t1(v1);\
  obj2.set_##t2(v2);\
  ObExprCtx expr_ctx(NULL, NULL, NULL, str_buf);      \
  int ret = obj.func(res, obj1, obj2, expr_ctx);\
  ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, ret);   \
}while(0)
#define TN(t2, v2, t1, v1, res_val, res_typ) EXPECT_RESULT2(obj, &buf, calc_result2, t1, v1, t2, v2, res_val, res_typ)
#define FN(t2, v2, t1, v1) EXPECT_FAIL_RESULT2(obj, &buf, calc_result2, t1, v1, t2, v2)
#define TNN(t2, v2, t1, v1, t3, v3, rest, resv) EXPECT_RESULT3(obj, &buf, calc_result3, t1, v1, t2, v2, t3, v3, rest, resv)
#define FNN(t2, v2, t1, v1, t3, v3, rest, resv) EXPECT_FAIL_RESULT3(obj, &buf, calc_result3, t1, v1, t2, v2, t3, v3, rest, resv)
#define TNULL() TEST_NULL(obj, &buf, calc_result2)
#define TMR(buf, t2, v2, t1, v1) TEST_MEMOEY_RESULT(obj, buf, calc_result2, t1, v1, t2, v2)

TEST_OPERATOR(ObExprLocate);
TEST_F(TestObExprLocateTest, basic_test)
{
  TestObExprLocate obj;
  ObExprResType res_type;
  res_type.set_calc_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  obj.set_result_type(res_type);
  DefaultPageAllocator buf;
  //select locate("34231", "3423423142342");
  TN(varchar, "3423423142342", varchar, "34231", int, 4);
  //case	2.	select instr('45455435453','4');
  TN(varchar, "45455435453", varchar, "4", int, 1);
  //case	3.	select instr('342342342342','dssfd');
  TN(varchar, "342342342342", varchar, "dssfd", int, 0);
  //case	4.	select instr('', 'grgg');
  TN(varchar, "", varchar, "grgg", int, 0);
  //case	5.	select instr('','');
  TN(varchar, "", varchar, "", int, 1);
  TN(varchar, "abc", varchar, "", int, 1);
  //case	6.	select instr('dffggdsf342342342', 'dsf3');
  TN(varchar, "dffggdsf342342342", varchar, "dsf3", int, 6);
  //case	7.	select instr('drfewrwer23342342422423', 'werwrewrewrwrrwerw');
  TN(varchar, "drfewrwer23342342422423", varchar, "werwrewrewrwrrwerw", int, 0);
  TN(varchar, "阿里巴巴", varchar, "阿里", int, 1);
  TN(varchar, "阿里巴巴", varchar, "巴巴", int, 3);
  TN(varchar, "阿里巴巴", varchar, "阿里巴巴", int, 1);
  //test three params
  TNN(varchar, "3423423142342", varchar, "34231", int, 4, int, 4);
  TNN(varchar, "3423423142342", varchar, "34231", int, 5, int, 0);
  TNN(varchar, "3423423142342", varchar, "", int, 5, int, 5);
  TNN(varchar, "3423423142342", varchar, "", int, 6, int, 6);
  TNN(varchar, "3423", varchar, "", int, 6 , int, 0);
  TNN(varchar, "阿里巴巴", varchar, "", int, 2, int, 4);
  TNN(varchar, "阿里巴巴", varchar, "", int, 3, int, 7);
  TNN(varchar, "阿里巴巴", varchar, "", int, 5, int, 13);

}

TEST_F(TestObExprLocateTest, mix_type_test)
{
  TestObExprLocate obj;
  DefaultPageAllocator buf;
  ObExprResType res_type;
  res_type.set_calc_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  obj.set_result_type(res_type);

  //bool and others
  TN(bool, 0, varchar, "0", int, 1);
  TN(bool, 1, varchar, "1", int, 1);
  TN(bool, 1, varchar, "", int, 1);
  TN(bool, 1, varchar, "2", int, 0);
  TN(varchar, "0123", bool, 1, int, 2);
  TN(varchar, "0123", bool, 0, int, 1);
  TN(varchar, "123", bool, 0, int, 0);
  TN(bool, 1, bool, 1, int, 1);
  TN(bool, 1, bool, 0, int, 0);
  TN(bool, 0, bool, 1, int, 0);
  TN(bool, 0, bool, 0, int, 1);
  TN(bool, 0, int, 0, int, 1);
  TN(bool, 1, int, 1, int, 1);
  TN(bool, 0, int, 1, int, 0);
  TN(int, 12340, bool, 1, int, 1);
  TN(int, 12340, bool, 0, int, 5);
  TN(int, 1234, bool, 0, int, 0);
  TN(datetime, 1434392608130640, bool, 0, int, 2);
  TN(datetime, 1434392608130640, bool, 1, int, 3);
  TN(bool, 1, datetime, 1434392608130640, int, 0);
  TN(bool, 1, double, 123, int, 0);
  TN(double, 2123, bool, 1, int, 2);
  TN(bool, 1, varchar, "", int, 1);
  //int and others
  TN(int, 1234, int, 23, int, 2);
  TN(int, 1234, int, 5, int, 0);
  TN(int, 1234, int, 123456, int, 0);
  TN(int, 1234, int, 123456, int, 0);
  TN(int, 1234, varchar, "234", int, 2);
  TN(int, 1234, varchar, "45", int, 0);
  TN(int, 1234, varchar, "", int, 1);
  TN(int, 1234, double, 123, int, 1);
  TN(int, 1234, double, 123.4, int, 0);
  TN(datetime, 1434392608130640, int, 201, int, 1);
  TN(datetime, 1434392608130640, int, 2016, int, 0);
  TN(int, 102343452, datetime, 1434392608130640, int, 0);
  //datetime and others
  TN(datetime, 1434392608130640, varchar, "15", int, 3);
  TN(datetime, 1434392608130640, double, 15, int, 3);
  TN(double, 2014, datetime, 1434392608130640, int, 0);
  TN(datetime, 1434392608130640, varchar, "", int, 1);
  //double and others
  TN(double, 2014567, double, 4567, int, 4);
  TN(double, 2014567, double, 45678103, int, 0);
  TN(double, 2014567, varchar, "", int, 1);
}

TEST_F(TestObExprLocateTest, cal_result_type_test)
{
  TestObExprLocate obj;
  DefaultPageAllocator buf;
  ObExprResType result;
  ObExprTypeCtx ctx;
  ObExprResType obj_array[3];
  obj_array[0].set_null();
  obj_array[1].set_null();
  //normal situation
  int ret = obj.calc_result_typeN(result, obj_array, 2, ctx);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(result.is_int());
  obj_array[2].set_null();
  ret = obj.calc_result_typeN(result, obj_array, 3, ctx);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(result.is_int());
  obj_array[0].set_int();
  obj_array[1].set_int();
  obj_array[2].set_int();
  ret = obj.calc_result_typeN(result, obj_array, 2, ctx);
  ASSERT_TRUE(OB_SUCC(ret));
  ASSERT_TRUE(result.is_int());
  ret = obj.calc_result_typeN(result, obj_array, 3, ctx);
  ASSERT_TRUE(OB_SUCC(ret));
  ASSERT_TRUE(result.is_int());
  obj_array[2].set_null();
  ASSERT_TRUE(OB_SUCC(ret));
  ASSERT_TRUE(result.is_int());
  //exceptions
  obj_array[0].set_ext();
  obj_array[2].set_int();
  ret = obj.calc_result_typeN(result, obj_array, 2, ctx);
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);
  printf("YYY\n");
  ret = obj.calc_result_typeN(result, obj_array, 3, ctx);
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);
}

TEST_F(TestObExprLocateTest, cal_result_test)
{
  TestObExprLocate obj;
  ObExprResType res_type;
  res_type.set_calc_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  obj.set_result_type(res_type);
  DefaultPageAllocator buf;
  ObObj result;
  ObObj obj_array[3];
  obj_array[0].set_null();
  //normal situation
  ObExprCtx expr_ctx;
  int ret = obj.calc_resultN(result, obj_array, 2, expr_ctx);
  ASSERT_TRUE(OB_NOT_INIT == ret);
  expr_ctx.calc_buf_ = &buf;
  ret = obj.calc_resultN(result, obj_array, 2, expr_ctx);
  ASSERT_TRUE(OB_SUCC(ret));
  ASSERT_TRUE(result.is_null());
  ret = obj.calc_resultN(result, obj_array, 3, expr_ctx);
  ASSERT_TRUE(OB_SUCC(ret));
  ASSERT_TRUE(result.is_null());
  obj_array[0].set_int(100);
  obj_array[1].set_int(1000);
  obj_array[2].set_int(1);
  //exceptions
  obj_array[0].set_max_value();
  ret = obj.calc_resultN(result, obj_array, 2, expr_ctx);
  ASSERT_TRUE(OB_INVALID_ARGUMENT == ret);
  ret = obj.calc_resultN(result, obj_array, 3, expr_ctx);
  ASSERT_TRUE(OB_INVALID_ARGUMENT == ret);
  TestAllocator tbuf;
  expr_ctx.calc_buf_ = &tbuf;
  ret = obj.calc_resultN(result, obj_array, 3, expr_ctx);
  ASSERT_TRUE(OB_INVALID_ARGUMENT == ret);
  obj_array[2].set_null();
  ret = obj.calc_resultN(result, obj_array, 3, expr_ctx);
  ASSERT_TRUE(OB_SUCC(ret));

  int64_t result_int = -1;
  result.get_int(result_int);
  ASSERT_EQ(result_int, 0);
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
