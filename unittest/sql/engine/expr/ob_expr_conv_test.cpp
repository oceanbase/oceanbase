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
#include "sql/engine/expr/ob_expr_conv.h"
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

class ObExprConvTest: public ::testing::Test
{
public:
  ObExprConvTest();
  virtual ~ObExprConvTest();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  ObExprConvTest(const ObExprConvTest &other);
  ObExprConvTest& operator=(const ObExprConvTest &other);
private:
  // data members
};

ObExprConvTest::ObExprConvTest() {}
ObExprConvTest::~ObExprConvTest() {}
void ObExprConvTest::SetUp() {}
void ObExprConvTest::TearDown() {}

#define TEST_NULL(str_op_object, str_buf, func)\
{\
  ObObj res;\
  ObObj val1;\
  ObObj val2;\
  ObObj val3;\
  val1.set_null();\
  val2.set_null();\
  val3.set_null();\
  ObExprCtx expr_ctx(NULL, NULL, NULL, str_buf);      \
  int err = str_op_object.func(res, val1, val2, val3, expr_ctx); \
  ASSERT_TRUE(OB_SUCCESS == err); \
}
#define TEST_MEMOEY_RESULT(obj, str_buf, func, t1, v1, t2, v2, t3, v3)\
{\
  ObObj obj1;\
  ObObj obj2;\
  ObObj obj3;\
  ObObj res;\
  obj1.set_##t1(v1);\
  obj2.set_##t2(v2);\
  obj3.set_##t3(v3);\
  ObExprCtx expr_ctx(NULL, NULL, NULL, str_buf);      \
  int ret = obj.func(res, obj1, obj2, obj3, expr_ctx);\
  ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, ret); \
}
#define TN(t1, v1, t2, v2, t3, v3, rest, resv) EXPECT_RESULT3(obj, &buf, calc_result3, t1, v1, t2, v2, t3, v3, rest, resv)
#define FN(t1, v1, t2, v2, t3, v3, rest, resv) EXPECT_FAIL_RESULT3(obj, &buf, calc_result3, t1, v1, t2, v2, t3, v3, rest, resv)
#define TNN() TEST_NULL(obj, &buf, calc_result3)
#define TMR(buf, t1, v1, t2, v2, t3, v3) TEST_MEMOEY_RESULT(obj, buf, calc_result3, t1, v1, t2, v2, t3, v3)
TEST_F(ObExprConvTest, basic_test)
{
  DefaultPageAllocator buf;
  ObExprConv obj(buf);
  //case 1. select conv('45',10,-18);
  TN(varchar, "45", int, 10, int, -18, varchar, "29");
  //case 2. select conv('45',10,18);
  TN(varchar, "45", int, 10, int, 18, varchar, "29");
  //case 3. select conv('-1',10,3);
  TN(varchar, "-1", int, 10, int, 3, varchar, "11112220022122120101211020120210210211220");
  //case 4. select conv('54535H', 35, 23);
  TN(varchar, "54535H", int, 35, int, 23, varchar, "1IHF0AM");
  //case 5. select conv('54535UUH', 36, 13);
  TN(varchar, "54535UUH", int, 36, int, 13, varchar, "2BA4C7A3A62");
  //case 6. select conv('-1',10,2);
  TN(varchar, "-1", int , 10, int, 2, varchar, "1111111111111111111111111111111111111111111111111111111111111111");
  //case 7. select conv('1',10,2);
  TN(varchar, "1", int , 10, int, 2, varchar, "1");
  //case 8. select conv('1',10,-2);
  TN(varchar, "1", int , 10, int, 2, varchar, "1");
  //case 9. select conv('1',-10,-2);
  TN(varchar, "1", int , 10, int, 2, varchar, "1");
  //case 10. select conv('FFFFFFFFFFFFFFFF',16,2);
  TN(varchar, "FFFFFFFFFFFFFFFF", int , 16, int, 2, varchar, "1111111111111111111111111111111111111111111111111111111111111111");
  //case 11. select conv('FFFFFFFFFFFFFFFFFF',16,2);
  TN(varchar, "FFFFFFFFFFFFFFFFFF", int , 16, int, 2, varchar, "1111111111111111111111111111111111111111111111111111111111111111");
  //case 12. select conv('FFFFFFFFFFFFFF',16,-2);
  TN(varchar, "FFFFFFFFFFFFFFFF", int , 16, int, -2, varchar, "-1");
  //case 13. select conv('Z', 36, 10);
  TN(varchar, "Z", int , 36, int, 10, varchar, "35");
  //case 14. select conv('-Z', 36, -36);
  TN(varchar, "-Z", int , 36, int, -36, varchar, "-Z");
  //case 15. select conv('HJHKFTFL', 36, 10);
  TN(varchar, "HJHKFTFL", int , 36, int, 10, varchar, "1374611909313");
  //case 16. select conv('-43HUJGTT4', 36, -10);
  TN(varchar, "-43HUJGTT4", int , 36, int, -10, varchar, "-11558384104936");
  //case 17. select conv('0', 10, 10);
  TN(varchar, "0", int, 10, int, 10, varchar, "0");
  //case 18. select conv('03423AA', 11, 36);
  TN(varchar, "03423AA", int, 11, int, 36, varchar, "BOF2");
  //case 19. select conv('03423ADC2K', 25, 20);
  TN(varchar, "03423ADC2K", int, 25, int, 20, varchar, "IH23B8DC5");
  //case 20. select conv('LL3C2K', 26, 13);
  TN(varchar, "LL3C2K", int, 26, int, 13, varchar, "41901957");
  //case 21. select conv('ZZZZZZZZZZ45454ZZZ1342',36,2);
  TN(varchar, "ZZZZZZZZZZ45454ZZZ1342", int, 36, int, 2, varchar, "1111111111111111111111111111111111111111111111111111111111111111");
  //case 22. select conv('ZZZZZZZZZZ45454ZZZ1342',36,-2);
  TN(varchar, "ZZZZZZZZZZ45454ZZZ1342", int, 36, int, -2, varchar, "-1");
  //case 23. select conv('zasas232sdas',36,10);
  TN(varchar, "zasas232sdas", int, 36, int , 10, varchar, "4646195307502010452");
  //case 24. select conv('ZZZZZZZZZZ45454ZZZ1342',36,-10);
  TN(varchar, "ZZZZZZZZZZ45454ZZZ1342", int, 36, int, -10, varchar, "-1");
  //case 25. select conv('adxsdfeweeweqwewqsqs',36,36);
  TN(varchar, "adxsdfeweeweqwewqsqs", int, 36, int, 36, varchar, "3W5E11264SGSF");
  //case 26. select conv('A', 2, 2); mysql return 0 when input str and base_idx not match
  TN(varchar, "A", int, 2, int, 2, varchar, "0");
  //case  2. select conv('adxsdfeweeweqwewqsqs',10,36); mysql return 0
  TN(varchar, "adxsdfeweeweqwewqsqs", int, 10, int, 36, varchar, "0");
}

TEST_F(ObExprConvTest, special_test)
{
  DefaultPageAllocator buf;
  ObExprConv obj(buf);
  TestAllocator tbuf;
  //case  1.  select conv(null, null, null);
  TNN();
  //case  3.  select conv('232', 21, 12);
  TMR(&tbuf, varchar, "232", int, 21, int, 12);
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
