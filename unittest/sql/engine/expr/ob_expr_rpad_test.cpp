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
#include "sql/engine/expr/ob_expr_rpad.h"
#include "ob_expr_test_utils.h"

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


class ObExprRpadTest : public  ::testing::Test
{
public:
  ObExprRpadTest();
  virtual ~ObExprRpadTest();
  virtual void SetUp();
  virtual void TearDown();
private:
  ObExprRpadTest(const ObExprRpadTest &other);
  ObExprRpadTest& operator=(const ObExprRpadTest &other);
};
ObExprRpadTest::ObExprRpadTest()
{
}
ObExprRpadTest::~ObExprRpadTest()
{
}
void ObExprRpadTest::SetUp()
{
}
void ObExprRpadTest::TearDown()
{
}

#define EXPECT_RESULT3_NULL(str_op_object, str_buf, func, type1, v1, type2, v2, type3, v3, ref_type, ref_value) \
                           {\
	                        UNUSED(ref_value);  \
	                        ObObj t1;                 \
	                        ObObj t2;                 \
	                        ObObj t3;                 \
	                        ObObj r;                  \
	                        ObObj ref;                \
	                        if(NULL == v1) \
	                          t1.set_null();           \
	                        else \
							             t1.set_##type1(v1);		\
	                        t2.set_##type2(v2);         \
	                        if(NULL == v3) \
	                          t3.set_null();           \
	                        else \
	                          t3.set_##type3(v3);		\
                          t1.set_collation_type(CS_TYPE_UTF8MB4_BIN);   \
                          t2.set_collation_type(CS_TYPE_UTF8MB4_BIN);   \
	                        t3.set_collation_type(CS_TYPE_UTF8MB4_BIN);   \
                          r.set_collation_type(CS_TYPE_UTF8MB4_BIN);   \
                          ObExprResType res_type;                                  \
                          res_type.set_collation_level(CS_LEVEL_EXPLICIT);         \
                          res_type.set_collation_type(CS_TYPE_UTF8MB4_BIN);                  \
                          str_op_object.set_result_type(res_type);  \
                          ObExprCtx expr_ctx(NULL, NULL, NULL, str_buf);\
	                        int err = str_op_object.func(r, t1, t2, t3, expr_ctx); \
	                        _OB_LOG(INFO, "respect=%s result=%s", to_cstring(ref), to_cstring(r)); \
	                        EXPECT_TRUE(OB_SUCCESS == err); \
	                        EXPECT_TRUE(ref.get_type() == ObNullType); \
                           } while(0)

#define EXPECT_RESULT3_NULL2(str_op_object, str_buf, func, type1, v1, type2, v2, type3, v3, ref_type, ref_value) \
                           {\
	                        UNUSED(ref_value);  \
	                        ObObj t1;                 \
	                        ObObj t2;                 \
	                        ObObj t3;                 \
	                        ObObj r;                  \
	                        ObObj ref;                \
	                        t1.set_##type1(v1);		\
	                        t2.set_##type2(v2);         \
	                        t3.set_##type3(v3);		\
	                        t1.set_collation_type(CS_TYPE_UTF8MB4_BIN);   \
                          t2.set_collation_type(CS_TYPE_UTF8MB4_BIN);   \
                          t3.set_collation_type(CS_TYPE_UTF8MB4_BIN);   \
                          r.set_collation_type(CS_TYPE_UTF8MB4_BIN);   \
                          ObExprCtx expr_ctx(NULL, NULL, NULL, str_buf);\
                          ObExprResType res_type;                                  \
                          res_type.set_collation_level(CS_LEVEL_EXPLICIT);         \
                          res_type.set_collation_type(CS_TYPE_UTF8MB4_BIN);                  \
                          str_op_object.set_result_type(res_type);  \
                          int err = str_op_object.func(r, t1, t2, t3, expr_ctx); \
                          ref.set_collation_type(CS_TYPE_UTF8MB4_BIN);  \
                           _OB_LOG(INFO, "respect=%s result=%s, err=%d", to_cstring(ref), to_cstring(r), err); \
                          EXPECT_TRUE(OB_ALLOCATE_MEMORY_FAILED == err); \
	                        EXPECT_TRUE(ref.get_type() == ObNullType); \
                           } while(0)

#define EXPECT_RESULT3_NULL3(str_op_object, str_buf, func, type1, v1, type2, v2, type3, v3, ref_type, ref_value) \
                           {\
	                        UNUSED(ref_value);  \
	                        ObObj t1;                 \
	                        ObObj t2;                 \
	                        ObObj t3;                 \
	                        ObObj r;                  \
	                        ObObj ref;                \
	                        t1.set_##type1(v1);		\
	                        t2.set_##type2(v2);         \
	                        t3.set_##type3(v3);		\
	                        t1.set_collation_type(CS_TYPE_UTF8MB4_BIN);   \
                          t2.set_collation_type(CS_TYPE_UTF8MB4_BIN);   \
                          t3.set_collation_type(CS_TYPE_UTF8MB4_BIN);   \
                          r.set_collation_type(CS_TYPE_UTF8MB4_BIN);   \
                          ObExprResType res_type;                                  \
                          res_type.set_collation_level(CS_LEVEL_EXPLICIT);         \
                          res_type.set_collation_type(CS_TYPE_UTF8MB4_BIN);                  \
                          str_op_object.set_result_type(res_type);  \
                          ObExprCtx expr_ctx(NULL, NULL, NULL, str_buf);\
                                 int err = str_op_object.func(r, t1, t2, t3, expr_ctx); \
	                        _OB_LOG(INFO, "respect=%s result=%s, ret=%d", to_cstring(ref), to_cstring(r), err); \
	                        EXPECT_TRUE(OB_ALLOCATE_MEMORY_FAILED == err); \
	                        EXPECT_TRUE(ref.get_type() == ObNullType); \
                           } while(0)

#define T(obj, t1, v1, t2, v2, t3, v3, ref_type, ref_value) EXPECT_RESULT3(obj, &buf, calc_result3, t1, v1, t2, v2, t3, v3, ref_type, ref_value)
#define TN(obj, t1, v1, t2, v2, t3, v3, ref_type, ref_value) EXPECT_RESULT3_NULL(obj, &buf, calc_result3, t1, v1, t2, v2, t3, v3, ref_type, ref_value)
#define TN2(obj, t1, v1, t2, v2, t3, v3, ref_type, ref_value) EXPECT_RESULT3_NULL2(obj, &buf, calc_result3, t1, v1, t2, v2, t3, v3, ref_type, ref_value)
#define TN3(obj, t1, v1, t2, v2, t3, v3, ref_type, ref_value) EXPECT_RESULT3_NULL3(obj, &buf, calc_result3, t1, v1, t2, v2, t3, v3, ref_type, ref_value)

TEST_F(ObExprRpadTest, basic_test)
{
  DefaultPageAllocator buf;
  ObExprRpad rpad(buf);

  ASSERT_EQ(3, rpad.get_param_num());

  TN(rpad, varchar, NULL, int, 1, varchar, "b", varchar, NULL);
  TN(rpad, varchar, "abcde", int, 10, varchar, NULL, varchar, NULL);
  TN(rpad, varchar, "abcde", int, -1, varchar, "b", varchar, NULL);

  T(rpad, int, 1, int, 4, int, 6, varchar, "1666");
  T(rpad, int, 1, int, 4, varchar, "a", varchar, "1aaa");
  T(rpad, varchar, "abcde", int, 3, varchar, "", varchar, "abc");
  TN(rpad, varchar, "abcde", int, 10, varchar, "", varchar, NULL);
  T(rpad, varchar, "abcde", int, 1, varchar, "b", varchar, "a");
  T(rpad, varchar, "abcde", int, 5, varchar, "b", varchar, "abcde");
  T(rpad, varchar, "abcde", int, 6, varchar, "b", varchar, "abcdeb");
  T(rpad, varchar, "abcde", int, 11, varchar, "ghi", varchar, "abcdeghighi");
  T(rpad, varchar, "abcde", int, 10, varchar, "ghi", varchar, "abcdeghigh");
  T(rpad, varchar, "abcde", int, 0, varchar, "abcd", varchar, "");
  T(rpad, double, 1.2, int, 2, varchar, "a", varchar, "1.");
  T(rpad, double, 1.1, int, 6, varchar, "a", varchar, "1.1aaa");
}

TEST_F(ObExprRpadTest, fail_test)
{
  DefaultPageAllocator buf2;
  ObExprRpad rpad(buf2);
  TestAllocator buf;
  ASSERT_EQ(3, rpad.get_param_num());

  TN2(rpad, varchar, "abcde", int, 10, varchar, "ghi", varchar, "abcdeghigh");
  TN3(rpad, varchar, "abcde", int, 10, varchar, "ghi", varchar, "abcdeghigh");
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
