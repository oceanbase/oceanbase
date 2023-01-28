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

#define USING_LOG_PREFIX SQL
#include "sql/test_sql_utils.h"
#include <unistd.h>
#include "lib/json/ob_json.h"
#include "test_sql.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/plan_cache/ob_pcv_set.h"
#include "sql/ob_sql_utils.h"
#include "../optimizer/ob_mock_part_mgr.h"
#include <stdio.h>

using namespace oceanbase::common;
using namespace oceanbase::json;
using namespace oceanbase::share::schema;

namespace test
{
static TestSQL *test_sql = NULL;
static ObPlanCache *plan_cache = NULL;
static const int64_t BUKET_NUM = 102400;

void init_pcv_set()
{
  if (NULL == test_sql) {
    test_sql = new TestSQL(ObString("test_schema.sql"));
    ASSERT_TRUE(test_sql);
    test_sql->init();
  }
  if (NULL == plan_cache) {
    plan_cache = new ObPlanCache;
    plan_cache->init(BUKET_NUM, test_sql->get_addr(), test_sql->get_part_cache(), OB_SYS_TENANT_ID, NULL);
    plan_cache->set_mem_limit_pct(20);
    plan_cache->set_mem_high_pct(90);
    plan_cache->set_mem_low_pct(50);
  }
}

void generate_plan(TestSqlCtx &test_sql_ctx, const char *query,
                   ParamStore &params, ObLogPlan *&logical_plan,
                   ObPhysicalPlan *&phy_plan, const  ObSQLSessionInfo &session)
{
  LOG_INFO("generate_plan!");
  ParseResult parse_result;
  ObStmt *stmt = NULL;

  //parse
  test_sql->do_parse(*test_sql_ctx.allocator_, query, parse_result);
  LOG_INFO("tree", "tree", CSJ(ObParserResultPrintWrapper(*(parse_result.result_tree_))));
  //replace const expt with ? in syntax tree; get params;

  SqlInfo not_param_info;
  ObMaxConcurrentParam::FixParamStore fixed_param_store(OB_MALLOC_NORMAL_BLOCK_SIZE,
                                                        ObWrapperAllocator(test_sql_ctx.allocator_));
  bool is_transform_outline = false;
  ObSqlParameterization::transform_syntax_tree(*test_sql_ctx.allocator_,
                                               session,
                                               NULL,
                                               parse_result.result_tree_,
                                               not_param_info,
                                               params,
                                               NULL,
                                               fixed_param_store,
                                               is_transform_outline);

  LOG_INFO("tree", "tree trans", CSJ(ObParserResultPrintWrapper(*(parse_result.result_tree_))));
  //resolve
  test_sql->do_resolve(test_sql_ctx, parse_result, stmt, &params);
  //logical_plan
  test_sql->generate_logical_plan(test_sql_ctx, &params, stmt, logical_plan);
  //physical plan
  test_sql->generate_physical_plan(logical_plan, phy_plan);
}

class TestPCVSet : public ::testing::Test
{
public:
  TestPCVSet() {}
  virtual ~TestPCVSet() {}
  void SetUp() {}
  void TearDown() {}
private:
  //disallow copy
  TestPCVSet(const TestPCVSet &other);
  TestPCVSet& operator=(const TestPCVSet &other);
};

TEST_F(TestPCVSet, basic)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObRawExprFactory expr_factory(allocator);
  ObStmtFactory stmt_factory(allocator);
  ObLogPlanFactory log_plan_factory(allocator);
  TestSqlCtx test_sql_ctx;
  test_sql_ctx.allocator_ = &allocator;
  test_sql_ctx.expr_factory_ = &expr_factory;
  test_sql_ctx.stmt_factory_ = &stmt_factory;
  test_sql_ctx.log_plan_factory_ = &log_plan_factory;
  ObSQLSessionInfo session;
  ObExecContext exec_ctx;
  ObSqlCtx sql_ctx;
  // just empty array to avoid failure while adding plan
  ObSEArray<ObPCConstParamInfo, 4> const_params_constraints;
  ObSEArray<ObPCParamEqualInfo, 4> equal_param_constraints;
  ObSEArray<ObPCParamPreciseInfo, 4> precise_param_constraints;
  ObDList<ObPreCalcExprConstraint> all_pre_calc_constraints_;
  ObSEArray<uint64_t, 4> trans_happened_route;
  int64_t merged_version = test_sql->get_merged_version();

  sql_ctx.schema_guard_ = &(test_sql->get_schema_guard());
  sql_ctx.session_info_ = &session;
  sql_ctx.all_plan_const_param_constraints_ = &const_params_constraints;
  sql_ctx.all_possible_const_param_constraints_ = &const_params_constraints;
  sql_ctx.all_equal_param_constraints_ = &equal_param_constraints;
  sql_ctx.all_precise_param_constraints_ = &precise_param_constraints;
  sql_ctx.all_pre_calc_constraints_ = &all_pre_calc_constraints_;
  sql_ctx.trans_happened_route_ = &trans_happened_route;
  ASSERT_EQ(OB_SUCCESS, session.init_tenant(ObString(OB_SYS_TENANT_NAME), OB_SYS_TENANT_ID));
  exec_ctx.create_physical_plan_ctx();
  exec_ctx.set_my_session(&session);
  exec_ctx.set_sql_ctx(&sql_ctx);
  exec_ctx.get_sql_ctx()->session_info_ = &session;
  exec_ctx.get_task_executor_ctx()->set_min_cluster_version(CLUSTER_VERSION_1500);
  ObLogPlan *logical_plan = nullptr;
  ObPhysicalPlan *phy_plan = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObPreProcessSysVars::init_sys_var());
  EXPECT_TRUE(OB_SUCCESS == session.load_default_sys_variable(false, true));
  // when add  generate_plan() , change TEST_PLAN_NUM
  ObString sql = ObString::make_string("select * from t1 where c1 = 0");
  ObPlanCacheCtx pc_ctx(sql, PC_TEXT_MODE, allocator, sql_ctx, exec_ctx, common::OB_SYS_TENANT_ID);
  ObPCVSet pcv_set(plan_cache);
  int ret = OB_SUCCESS;
  generate_plan(test_sql_ctx,
                sql.ptr(),
                exec_ctx.get_physical_plan_ctx()->get_param_store_for_update(),
                logical_plan, phy_plan, session);

  ret = pcv_set.init(pc_ctx, phy_plan);
  ret = pcv_set.add_cache_obj(phy_plan, pc_ctx);
  EXPECT_EQ(OB_SUCCESS, ret);
}
}

int main(int argc, char **argv)
{
  system("rm -rf test_pcv_set.log");
  ::oceanbase::sql::init_sql_factories();
  ::testing::InitGoogleTest(&argc,argv);
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_pcv_set.log", true);

  ::test::init_pcv_set();
  return RUN_ALL_TESTS();
}
