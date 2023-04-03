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

#include "sql/engine/expr/ob_postfix_expression.h"
#include "sql/engine/expr/ob_expr_operator_factory.h"
#include <gtest/gtest.h>
#include "lib/utility/ob_test_util.h"
#include "sql/engine/expr/ob_expr_add.h"
#include "sql/engine/expr/ob_expr_concat.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;
class TestPostfixExpression: public ::testing::Test
{
public:
  TestPostfixExpression();
  virtual ~TestPostfixExpression();
  virtual void SetUp();
  virtual void TearDown();
   inline static int64_t get_usec()
      {
    struct timeval time_val;
    gettimeofday(&time_val, NULL);
    return time_val.tv_sec*1000000 + time_val.tv_usec;
  }
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestPostfixExpression);
protected:
  // function members
protected:
  // data members
};

TestPostfixExpression::TestPostfixExpression()
{
}

TestPostfixExpression::~TestPostfixExpression()
{
}

void TestPostfixExpression::SetUp()
{
}

void TestPostfixExpression::TearDown()
{
}

// concat("ABC", 123 + C1, 10.1)
TEST_F(TestPostfixExpression, item_serialization)
{
  // prepare
  ObArenaAllocator allocator(ObModIds::TEST);
  ObExprOperatorFactory factory(allocator);
  ObExprOperatorFactory::register_expr_operators();
  ObPostfixExpression expr(allocator, 0);
  ObObj obj1;
  obj1.set_int(123);
  ObObj obj2;
  obj2.set_varchar(ObString::make_string("ABC"));
  obj2.set_collation_level(CS_LEVEL_IMPLICIT);
  obj2.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  ObObj obj3;
  number::ObNumber dec;
  ASSERT_EQ(OB_SUCCESS, dec.from("10.1", allocator));
  obj3.set_unumber(dec);
  COMMON_LOG(INFO, "decimal", K(dec.get_length()), K(dec));
  ObExprOperator *op_cnn = NULL;
  factory.alloc(T_OP_CNN, op_cnn);
  op_cnn->set_real_param_num(2);
  ASSERT_TRUE(NULL != op_cnn);
  ObExprOperator *op_add = NULL;
  factory.alloc(T_OP_ADD, op_add);
  op_add->set_real_param_num(2);
  ASSERT_TRUE(NULL != op_add);
  expr.set_item_count(6);
  ObPostExprItem item;
  ASSERT_EQ(OB_SUCCESS, item.assign(obj1));
  ASSERT_EQ(OB_SUCCESS, expr.add_expr_item(item));
  ASSERT_EQ(OB_SUCCESS, item.assign(obj2));
  ObAccuracy accuracy;
  accuracy.set_length(3);
  item.set_accuracy(accuracy);
  ASSERT_EQ(OB_SUCCESS, expr.add_expr_item(item));
  accuracy.set_length(-1);
  item.set_accuracy(accuracy);
  ASSERT_EQ(OB_SUCCESS, item.set_column(1));
  ASSERT_EQ(OB_SUCCESS, expr.add_expr_item(item));
  ASSERT_EQ(OB_SUCCESS, item.assign(op_add));
  ASSERT_EQ(OB_SUCCESS, expr.add_expr_item(item));
  ASSERT_EQ(OB_SUCCESS, item.assign(obj3));
  ASSERT_EQ(OB_SUCCESS, expr.add_expr_item(item));
  ASSERT_EQ(OB_SUCCESS, item.assign(op_cnn));
  ASSERT_EQ(OB_SUCCESS, expr.add_expr_item(item));
  COMMON_LOG(INFO, "expr1", K(expr));
  const char* expr_str = S(expr);
  // test
  char buf[1024];
  int64_t buf_len = 1024;
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, expr.serialize(buf, buf_len, pos));
  ASSERT_EQ(pos, expr.get_serialize_size());
  COMMON_LOG(INFO, "serialize size", K(pos));
  ObPostfixExpression expr2(allocator, 0);
  int64_t data_len = pos;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, expr2.deserialize(buf, data_len, pos));
  ASSERT_EQ(data_len, pos);
  COMMON_LOG(INFO, "expr2", K(expr));
  const char* expr2_str = S(expr2);
  ASSERT_STREQ(expr_str, expr2_str);
  // teardown
}

TEST_F(TestPostfixExpression, item_calc)
{
  // prepare
  ObArenaAllocator allocator(ObModIds::TEST);
  ObExprOperatorFactory factory(allocator);
  ObExprOperatorFactory::register_expr_operators();
  ObPostfixExpression expr(allocator, 0);
  ObObj obj1;
  obj1.set_int(123);
  ObObj obj2;
  obj2.set_int(321);

  ObExprOperator *op_add = NULL;
  factory.alloc(T_OP_ADD, op_add);
  op_add->set_real_param_num(2); 
  ObExprResType result_type;
  result_type.set_calc_type(ObIntType);
  op_add->set_result_type(result_type);
  ASSERT_TRUE(NULL != op_add);
  expr.set_item_count(3);

  ObPostExprItem item;
  ASSERT_EQ(OB_SUCCESS, item.assign(obj1));
  ASSERT_EQ(OB_SUCCESS, expr.add_expr_item(item));
  ASSERT_EQ(OB_SUCCESS, item.assign(obj2));
  ASSERT_EQ(OB_SUCCESS, expr.add_expr_item(item));

  ASSERT_EQ(OB_SUCCESS, item.assign(op_add));
  ASSERT_EQ(OB_SUCCESS, expr.add_expr_item(item));

  DefaultPageAllocator buf;
  ObExprCtx expr_ctx(NULL, NULL, NULL, &buf);
  ObNewRow input_row;
  ObObj result_val;

  int64_t time_1 = get_usec();
  int64_t t = 5000000;
  for (int64_t i = 0; i < t; i++)
     ASSERT_EQ(OB_SUCCESS, expr.calc(expr_ctx, input_row, result_val));

  
  int64_t time_2 = get_usec();
  std::cout << "# of exec:" << t<< std::endl;
  std::cout << "total time:" << time_2 - time_1 << std::endl;
  COMMON_LOG(INFO, "result", K(result_val));
  

  // teardown
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_postfix_expression.log", true);
  return RUN_ALL_TESTS();
}
