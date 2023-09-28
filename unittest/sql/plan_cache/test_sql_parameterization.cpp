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
#define private public
#include "sql/plan_cache/ob_sql_parameterization.h"
#include "sql/plan_cache/ob_id_manager_allocator.h"
#include "lib/allocator/page_arena.h"

using namespace oceanbase;
using namespace common;
using namespace sql;
using namespace share::schema;

#define OK(value) ASSERT_EQ(OB_SUCCESS, (value))

class TestSqlParameterization: public ::testing::Test
{
  public:
    TestSqlParameterization();
    virtual ~TestSqlParameterization();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    TestSqlParameterization(const TestSqlParameterization &other);
    TestSqlParameterization& operator=(const TestSqlParameterization &other);
  protected:
    // data members
};

TestSqlParameterization::TestSqlParameterization()
{
}

TestSqlParameterization::~TestSqlParameterization()
{
}

void TestSqlParameterization::SetUp()
{
}

void TestSqlParameterization::TearDown()
{
}

TEST_F(TestSqlParameterization, parameterize_sql)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(0);
  ObString stmt = ObString::make_string("select * from t1 where c1 = 3 group by 2 order by 1");
  ObSEArray<const ObPCParam *, OB_PC_NOT_PARAM_COUNT> not_params;
  ObFastParserResult fp_result;
  FPContext fp_ctx(ObCharsets4Parser());
  fp_ctx.sql_mode_ = SMO_DEFAULT;
  if (OB_FAIL(ObSqlParameterization::fast_parser(allocator,
                                                 fp_ctx,
                                                 stmt,
                                                 fp_result))) {
    SQL_PC_LOG(WARN, "fail to fast_parser", K(stmt));
  }

  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(fp_result.raw_params_.count(), 3);
  ASSERT_EQ(3, fp_result.raw_params_.at(0)->node_->value_);
  ASSERT_EQ(2, fp_result.raw_params_.at(1)->node_->value_);
  ASSERT_EQ(1, fp_result.raw_params_.at(2)->node_->value_);
/*  printf("no_param_sql: %s\n", fp_result.key_.ptr());*/
  //for (int64_t i = 0; i < fp_result.raw_params_.count(); i++) {
    //ParseNode *param = fp_result.raw_params_.at(i)->node_;
    //printf("    param_%ld: type:%d; value:%ld, str_value:%s, raw_text:%s, pos_:%ld\n",
                     //i, param->type_,
                     //param->value_,
                     //param->str_value_,
                     //param->raw_text_,
                     //param->pos_);
  /*}*/
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
