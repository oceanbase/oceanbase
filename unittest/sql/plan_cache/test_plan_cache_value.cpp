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
#include "sql/ob_sql_utils.h"
#include "../optimizer/ob_mock_part_mgr.h"
#include "sql/plan_cache/ob_pcv_set.h"
#include "share/schema/ob_table_schema.h"

using namespace oceanbase::common;
using namespace oceanbase::json;
using namespace oceanbase::share::schema;

namespace test
{
static const int64_t BUKET_NUM = 102400;
static const int64_t HIGH_WATER_MARK = 10 * 100 * 1024;  // 10 plans
static const int64_t LOW_WATER_MARK = 5 * 100 * 1024;    // 5 plans
static const int64_t TEST_PLAN_NUM = 3;

static TestSQL *test_sql = NULL;
static ObPlanCache *plan_cache = NULL;
static ObPlanCacheValue *plan_cache_value = NULL;

void init_pcv()
{
  if (NULL == test_sql) {
    test_sql = new TestSQL(ObString("test_schema.sql"));
    ASSERT_TRUE(test_sql);
    test_sql->init();
  }
  if (NULL == plan_cache) {
    plan_cache = new ObPlanCache;
    plan_cache->init(BUKET_NUM, test_sql->get_addr(), test_sql->get_part_cache(), OB_SYS_TENANT_ID, NULL);
    plan_cache->set_host(test_sql->get_addr());
    plan_cache->set_location_cache(test_sql->get_part_cache());
    plan_cache->set_mem_limit_pct(20);
    plan_cache->set_mem_high_pct(90);
    plan_cache->set_mem_low_pct(50);
  }
}

class TestPlanCacheValue : public ::testing::Test
{
public:
  TestPlanCacheValue() {}
  virtual ~TestPlanCacheValue() {}
  void SetUp() {}
  void TearDown() {}
private:
  //disallow copy
  TestPlanCacheValue(const TestPlanCacheValue &other);
  TestPlanCacheValue& operator=(const TestPlanCacheValue &other);
};

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

TEST_F(TestPlanCacheValue, basic)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  MockPartitionService partition_service;
  ObRawExprFactory expr_factory(allocator);
  ObStmtFactory stmt_factory(allocator);
  ObLogPlanFactory log_plan_factory(allocator);
  TestSqlCtx test_sql_ctx;
  test_sql_ctx.allocator_ = &allocator;
  test_sql_ctx.expr_factory_ = &expr_factory;
  test_sql_ctx.stmt_factory_ = &stmt_factory;
  test_sql_ctx.log_plan_factory_ = &log_plan_factory;
  ObSQLSessionInfo session[TEST_PLAN_NUM];
  ObExecContext exec_ctx[TEST_PLAN_NUM];
  ObSqlCtx sql_ctx[TEST_PLAN_NUM];
  // just empty array to avoid failure while adding plan
  ObSEArray<ObPCConstParamInfo, 4> plan_const_param_constraints;
  ObSEArray<ObPCConstParamInfo, 4> all_const_param_constraints;
  int64_t merged_version = test_sql->get_merged_version();
  for (int i = 0; i < TEST_PLAN_NUM; i++) {
    sql_ctx[i].schema_guard_ = &(test_sql->get_schema_guard());
    sql_ctx[i].session_info_ = &(session[i]);
    sql_ctx[i].all_plan_const_param_constraints_ = &plan_const_param_constraints;
    sql_ctx[i].all_possible_const_param_constraints_ = &all_const_param_constraints;
  }
  for (int i = 0; i < TEST_PLAN_NUM; i++) {
    ASSERT_EQ(OB_SUCCESS, session[i].init_tenant(ObString(OB_SYS_TENANT_NAME), OB_SYS_TENANT_ID));
    exec_ctx[i].create_physical_plan_ctx();
    exec_ctx[i].set_my_session(&(session[i]));
    exec_ctx[i].set_sql_ctx(&sql_ctx[i]);
    GCTX.par_ser_ = &partition_service;
    exec_ctx[i].get_task_executor_ctx()->set_min_cluster_version(CLUSTER_VERSION_1500);
  }
  ObLogPlan *logical_plan[TEST_PLAN_NUM];
  ObPhysicalPlan *phy_plan[TEST_PLAN_NUM];
  ASSERT_EQ(OB_SUCCESS, ObPreProcessSysVars::init_sys_var());
  for (int64_t i = 0; i < TEST_PLAN_NUM; ++i) {
    ASSERT_EQ(OB_SUCCESS, session[i].load_default_sys_variable(false, true));
  }
  // when add  generate_plan() , change TEST_PLAN_NUM
  generate_plan(test_sql_ctx,
                "select /*no_use_px*/ * from t1 where c1 = 1",
                exec_ctx[0].get_physical_plan_ctx()->get_param_store_for_update(),
                logical_plan[0], phy_plan[0], session[0]);
  generate_plan(test_sql_ctx,
                "select /*no_use_px*/ * from t1 where c1 = '1'",
                exec_ctx[1].get_physical_plan_ctx()->get_param_store_for_update(),
                logical_plan[1], phy_plan[1], session[1]);
  generate_plan(test_sql_ctx,
                "select /*no_use_px*/ * from t1 where c1=1 and c2=2",
                exec_ctx[2].get_physical_plan_ctx()->get_param_store_for_update(),
                logical_plan[2], phy_plan[2], session[2]);

  common::ObSEArray<ObTablePartitionInfo *, 4> table_partition_info;
  // for (int i = 0; i < TEST_PLAN_NUM; i++) {
  //   phy_plan[i]->set_plan_id(i);
  // }
  for (int i = 0; i < TEST_PLAN_NUM; i++) {
    table_partition_info.reset();
    ASSERT_EQ(OB_SUCCESS, logical_plan[i]->get_global_table_partition_info(table_partition_info));
    ASSERT_EQ(OB_SUCCESS, exec_ctx[i].get_sql_ctx()->set_partition_infos(
              table_partition_info,
              allocator));
    ASSERT_EQ(OB_SUCCESS, exec_ctx[i].get_task_executor_ctx()->set_table_locations(
              table_partition_info));
  }

  ObString sql_0 = ObString::make_string("select /*no_use_px*/ * from t1 where c1 = 1");
  ObString sql_1 = ObString::make_string("select /*no_use_px*/ * from t1 where c1 = '1'");
  ObString sql_2 = ObString::make_string("select /*no_use_px*/ * from t1 where c1=1 and c2=2");
  ObPlanCacheCtx pc_ctx_0(sql_0, PC_TEXT_MODE, allocator, sql_ctx[0], exec_ctx[0], common::OB_SYS_TENANT_ID);
  ObPlanCacheCtx pc_ctx_1(sql_0, PC_TEXT_MODE, allocator, sql_ctx[1], exec_ctx[1], common::OB_SYS_TENANT_ID);
  ObPlanCacheCtx pc_ctx_2(sql_0, PC_TEXT_MODE, allocator, sql_ctx[2], exec_ctx[2], common::OB_SYS_TENANT_ID);
  pc_ctx_0.fp_result_.cache_params_ = &(pc_ctx_0.exec_ctx_.get_physical_plan_ctx()->get_param_store_for_update());
  pc_ctx_1.fp_result_.cache_params_ = &(pc_ctx_1.exec_ctx_.get_physical_plan_ctx()->get_param_store_for_update());
  pc_ctx_2.fp_result_.cache_params_ = &(pc_ctx_2.exec_ctx_.get_physical_plan_ctx()->get_param_store_for_update());
  ObPCVSet pcv_set(plan_cache);
  (void)pcv_set.init(pc_ctx_0, phy_plan[0]);
  pcv_set.create_new_pcv(plan_cache_value);
  plan_cache_value->init(&pcv_set, phy_plan[0], pc_ctx_0);

  //test reset()
  plan_cache_value->reset();
  pcv_set.free_pcv(plan_cache_value);
}
}

int main(int argc, char **argv)
{
  system("rm -rf test_plan_cache_value.log");
  ::oceanbase::sql::init_sql_factories();
  ::testing::InitGoogleTest(&argc,argv);
  OB_LOGGER.set_log_level("DEBUG");
  OB_LOGGER.set_file_name("test_plan_cache_value.log", true);

  ::test::init_pcv();
  return RUN_ALL_TESTS();
}
