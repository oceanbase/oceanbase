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
#include "sql/engine/expr/ob_expr_bin.h"
#include "ob_expr_test_utils.h"
#include "lib/oblog/ob_log.h"
#include "objit/common/ob_item_type.h"

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

class ObExprBinTest: public ::testing::Test
{
public:
  ObExprBinTest();
  virtual ~ObExprBinTest();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  ObExprBinTest(const ObExprBinTest &other);
  ObExprBinTest& operator=(const ObExprBinTest &other);
private:
  // data members
};

ObExprBinTest::ObExprBinTest() {}
ObExprBinTest::~ObExprBinTest() {}
void ObExprBinTest::SetUp() {}
void ObExprBinTest::TearDown() {}

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
#define TN(typ, val, res_val, res_typ) EXPECT_RESULT1(obj, &buf, calc_result1, typ, val, res_val, res_typ)
#define FN(typ, val) EXPECT_FAIL_RESULT1(obj, &buf, calc_result1, typ, val)
#define TNN() TEST_NULL(obj, &buf, calc_result1)
#define TMR(buf, t1, v1) TEST_MEMOEY_RESULT(obj, buf, calc_result1, t1, v1)
TEST_F(ObExprBinTest, basic_test)
{
  DefaultPageAllocator buf;
  ObExprBin obj(buf);
  //case	1.	select bin(342342342342);
  TN(int, 342342342342, varchar, "100111110110101001100011001011011000110");
  //case	2.	select bin(5464563463463456345);
  TN(uint64, 5464563463463456345, varchar,
      "100101111010110000001111000010001111100010100010100111001011001");
  //case	3.	select bin(-2.2);
  TN(uint64, 9223372036854779807UL, varchar,
      "1000000000000000000000000000000000000000000000000000111110011111");
  //case	4.	select bin(-1);
  TN(int, -1, varchar,
      "1111111111111111111111111111111111111111111111111111111111111111");
  //case	5.	select bin(-46234123434);
  TN(int, -46234123434, varchar,
      "1111111111111111111111111111010100111100001110110100001101010110");
  //case	6.	select bin(-9223372036854779808);
  TN(int, -5223372036854779807, varchar,
      "1011011110000010110110101100111010011101100011111111000001100001");
  //case	7.	select bin(3);
  TN(int, 3, varchar, "11");

}
TEST_F(ObExprBinTest, special_test)
{
  DefaultPageAllocator buf;
  ObExprBin obj(buf);
  TestAllocator tbuf;
  //case	1.	select bin('342342342342');
  FN(char, "342342342342");
  //case	2.	select bin('21435435456576534365765324234.4656df');
  FN(char, "21435435456576534365765324234.4656df");
  //case	3.	select bin(null);
  TNN()
  ;
  //case	4.  select bin(true);
  FN(bool, true);
  //case	5.	select bin(3435)
  TMR(&tbuf, int, 3435);
}
int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
