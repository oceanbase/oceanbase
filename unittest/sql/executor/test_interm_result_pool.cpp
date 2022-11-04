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

#include "sql/executor/ob_interm_result_pool.h"
#include "sql/executor/ob_interm_result.h"
#include "sql/executor/ob_scanner_pool.h"
#include "sql/ob_sql_init.h"
#include "lib/container/ob_array.h"
#include <gtest/gtest.h>
using namespace oceanbase::common;
using namespace oceanbase::sql;

class ObIntermResultPoolTest : public ::testing::Test
{
public:
  ObIntermResultPoolTest();
  virtual ~ObIntermResultPoolTest();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  ObIntermResultPoolTest(const ObIntermResultPoolTest &other);
  ObIntermResultPoolTest& operator=(const ObIntermResultPoolTest &other);
private:
  // data members
};
ObIntermResultPoolTest::ObIntermResultPoolTest()
{
}

ObIntermResultPoolTest::~ObIntermResultPoolTest()
{
}

void ObIntermResultPoolTest::SetUp()
{
}

void ObIntermResultPoolTest::TearDown()
{
}

TEST_F(ObIntermResultPoolTest, basic_test)
{
  ObIntermResultPool tmp_ir_pool;
  UNUSED(tmp_ir_pool);
  ObIntermResultPool* ir_pool = ObIntermResultPool::get_instance();
  ObIntermResult* ir = NULL;
  ObIntermResult tmp_ir;
  tmp_ir.try_begin_recycle();

  ASSERT_TRUE(NULL != ir_pool);
  ASSERT_EQ(OB_ERR_UNEXPECTED, ir_pool->free_interm_result(&tmp_ir));
  ASSERT_EQ(OB_INVALID_ARGUMENT, ir_pool->free_interm_result(ir));
  ASSERT_EQ(OB_SUCCESS, ir_pool->alloc_interm_result(ir));
  ASSERT_TRUE(NULL != ir);
  ASSERT_EQ(OB_SUCCESS, ir_pool->free_interm_result(ir));

  ObArray<ObIntermResult*> tmp_arr;
  int64_t alloc_count = 100;
  int64_t free_count = 83;
  ASSERT_TRUE(alloc_count >= free_count);
  for (int64_t i = 0; i < alloc_count; ++i) {
    ASSERT_EQ(OB_SUCCESS, ir_pool->alloc_interm_result(ir));
    ASSERT_EQ(OB_SUCCESS, tmp_arr.push_back(ir));
  }
  int64_t left_count = ObScannerPool::get_capacity() - alloc_count;
  ASSERT_TRUE(left_count >= 0);
  for (int64_t i = 0; i < free_count; ++i) {
    ObIntermResult *tir = tmp_arr.at(i);
    ASSERT_EQ(OB_SUCCESS, ir_pool->free_interm_result(tir));
  }
  left_count = ObScannerPool::get_capacity() - alloc_count + free_count;
  ASSERT_TRUE(left_count >= 0);
}

int main(int argc, char **argv)
{
  oceanbase::sql::init_sql_executor_singletons();
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
