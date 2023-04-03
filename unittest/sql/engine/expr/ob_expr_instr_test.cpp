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
#include "sql/engine/expr/ob_expr_instr.h"
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

class ObExprInstrTest: public ::testing::Test
{
public:
  ObExprInstrTest();
  virtual ~ObExprInstrTest();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  ObExprInstrTest(const ObExprInstrTest &other);
  ObExprInstrTest& operator=(const ObExprInstrTest &other);
private:
  // data members
};

ObExprInstrTest::ObExprInstrTest() {}

ObExprInstrTest::~ObExprInstrTest() {}

void ObExprInstrTest::SetUp() {}

void ObExprInstrTest::TearDown() {}

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
	ObExprCtx expr_ctx(NULL, NULL, NULL, str_buf);\
	int ret = obj.func(res, obj1, obj2, expr_ctx);\
	ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, ret);   \
} while(0)

#define TN(t1, v1, t2, v2, res_val, res_typ) EXPECT_RESULT2(obj, &buf, calc_result2, t1, v1, t2, v2, res_val, res_typ)
#define FN(t1, v1, t2, v2) EXPECT_FAIL_RESULT2(obj, &buf, calc_result2, t1, v1, t2, v2)
#define TNN() TEST_NULL(obj, &buf, calc_result2)
#define TMR(buf, t1, v1, t2, v2) TEST_MEMOEY_RESULT(obj, buf, calc_result2, t1, v1, t2, v2)
TEST_F(ObExprInstrTest, varchar_test)
{
  DefaultPageAllocator buf;
  ObExprInstr obj(buf);
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
}

TEST_F(ObExprInstrTest, mixt_type_test)
{
  DefaultPageAllocator buf;
  ObExprInstr obj(buf);
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

TEST_F(ObExprInstrTest, special_test)
{
  DefaultPageAllocator buf;
  ObExprInstr obj(buf);
  TestAllocator tbuf;
  //case	1.	select instr('342342342342');
  TN(int, 342342342342, varchar, "sddsd", int, 0);
  //case	2.	select instr('21435435456576534365765324234.4656df');
  TN(double, 34.234, varchar, "324", int, 0);
  //case	3.	select instr(null);
  TNN();
  //case	4.  select instr(true);
  TN(bool, true, int, 343, int, 0);
  ObObj result;
  ObObj obj1;
  ObObj obj2;
  ObExprCtx expr_ctx;
  int ret = obj.calc_result2(result, obj1, obj2, expr_ctx);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ObExprCtx ctx(NULL, NULL, NULL, &buf);
  //obj1.set_max_value();
  //ret = obj.calc_result2(result, obj1, obj2, ctx);
  //ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  obj1.set_int(10);
  obj2.set_max_value();
  ret = obj.calc_result2(result, obj1, obj2, ctx);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
