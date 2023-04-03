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
#include "lib/utility/ob_test_util.h"
#define private public
#include "sql/engine/expr/ob_expr_operator_factory.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;
class TestExprFactory: public ::testing::Test
{
public:
  TestExprFactory();
  virtual ~TestExprFactory();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestExprFactory);
protected:
  // function members
protected:
  // data members
};

TestExprFactory::TestExprFactory()
{
}

TestExprFactory::~TestExprFactory()
{
}

void TestExprFactory::SetUp()
{
}

void TestExprFactory::TearDown()
{
}

TEST_F(TestExprFactory, oracle_expr_names)
{
  // prepare
  ObArenaAllocator allocator(ObModIds::TEST);
  ObExprOperatorFactory factory(allocator);
  ObExprOperatorFactory::register_expr_operators();

  for (uint32_t i = 0; i < ARRAYSIZEOF(factory.NAME_TYPES_ORCL); i++) {
    if (factory.NAME_TYPES_ORCL[i].type_ <= T_MIN_OP || factory.NAME_TYPES_ORCL[i].type_ >= T_MAX_OP) {
      break;
    }
    std::cout<< factory.NAME_TYPES_ORCL[i].name_ <<std::endl;
  }

}

TEST_F(TestExprFactory, mysql_expr_names)
{
  // prepare
  ObArenaAllocator allocator(ObModIds::TEST);
  ObExprOperatorFactory factory(allocator);
  ObExprOperatorFactory::register_expr_operators();

  for (uint32_t i = 0; i < ARRAYSIZEOF(factory.NAME_TYPES); i++) {
    if (factory.NAME_TYPES[i].type_ <= T_MIN_OP || factory.NAME_TYPES[i].type_ >= T_MAX_OP) {
      break;
    }
    std::cout<< factory.NAME_TYPES[i].name_ <<std::endl;
  }
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_expr_operator_factory.log", true);
  return RUN_ALL_TESTS();
}
