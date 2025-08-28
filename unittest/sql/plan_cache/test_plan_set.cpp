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
#include <unistd.h>
#include "lib/json/ob_json.h"
#define private public
#include "test_sql.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/ob_sql_utils.h"
#include "sql/plan_cache/ob_plan_cache_util.h"
#include "sql/plan_cache/ob_pcv_set.h"
#include "../optimizer/ob_mock_part_mgr.h"
#include "sql/optimizer/mock_locality_manger.h"

using namespace oceanbase::common;
using namespace oceanbase::json;
using namespace oceanbase::share::schema;

namespace test
{
static const int64_t BUKET_NUM = 102400;
static const int64_t HIGH_WATER_MARK = 10 * 100 * 1024;  // 10 plans
static const int64_t LOW_WATER_MARK = 5 * 100 * 1024;    // 5 plans
static const int64_t TEST_PLAN_NUM = 7;

static TestSQL *test_sql = NULL;
static ObPlanSet *plan_set = NULL;
static ObPlanCache *plan_cache = NULL;
static ObPlanCacheValue *plan_cache_value = NULL;

int64_t new_sql_id = 0;

int64_t allocate_sql_id()
{
  return __sync_add_and_fetch(&new_sql_id, 1);
}

class TestPlanSet : public ::testing::Test
{
public:
  TestPlanSet(){}
  virtual ~TestPlanSet(){}
  void SetUp(){}
  void TearDown(){}
private:
  // disallow copy
  TestPlanSet(const TestPlanSet &other);
  TestPlanSet& operator=(const TestPlanSet &other);
};

void init_sql_plan_set()
{
  if (NULL == test_sql) {
    test_sql = new TestSQL(ObString("test_schema.sql"));
    ASSERT_TRUE(test_sql);
    test_sql->init();
    test_sql->get_session_info().set_user_session();
  }
  if (NULL == plan_cache) {
    plan_cache = new ObPlanCache();
    plan_cache->init(BUKET_NUM, test_sql->get_addr(), test_sql->get_part_cache(), OB_SYS_TENANT_ID, NULL);
    plan_cache->set_host(test_sql->get_addr());
    GCTX.self_addr_seq_.set_addr(test_sql->get_addr());
    plan_cache->set_location_cache(test_sql->get_part_cache());
    plan_cache->set_mem_limit_pct(20);
    plan_cache->set_mem_high_pct(90);
    plan_cache->set_mem_low_pct(50);
  }
}

//void alter_schema_mgr_version()
//{
//  // schema mgr's version
//  ObSchemaManager *schema_mgr = test_sql->get_schema_manager();
//  int64_t schema_version = schema_mgr->get_schema_version();
//  schema_mgr->set_schema_version(schema_version + 1);
//}
//
//void alter_table_schema_version()
//{
//  // schema mgr's version
//  ObSchemaManager *schema_mgr = test_sql->get_schema_manager();
//  // table's version;
//  int64_t tid = combine_id(OB_SYS_TENANT_ID, 50000);
//  ObTableSchema *table_schema = const_cast<ObTableSchema *>(schema_mgr->get_table_schema(tid));
//  int64_t schema_version = table_schema->get_schema_version();
//  table_schema->set_schema_version(schema_version + 1);
//}

void generate_plan(TestSqlCtx &test_sql_ctx, const char *query,
                   ParamStore &params, ObLogPlan *&logical_plan,
                   ObPhysicalPlan *&phy_plan, ObSQLSessionInfo &session)
{
  LOG_INFO("generate_plan!", KP(&session));
  ParseResult parse_result;
  ObStmt *stmt = NULL;
  ASSERT_EQ(OB_SUCCESS, session.test_init(0, 0, 0, test_sql_ctx.allocator_));
  ASSERT_EQ(OB_SUCCESS, session.init_system_variables(false, true));
  session.set_user_session();
  //parse
  test_sql->do_parse(*test_sql_ctx.allocator_, query, parse_result);
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
  //resolve
  test_sql->do_resolve(test_sql_ctx, parse_result, stmt, &params);
  //logical_plan
  test_sql->generate_logical_plan(test_sql_ctx, &params, stmt, logical_plan);
  ASSERT_TRUE(logical_plan != nullptr);
  //physical plan
  test_sql->generate_physical_plan(logical_plan, phy_plan);
  ASSERT_TRUE(phy_plan != nullptr);
}

}

int main(int argc, char **argv)
{
  ::oceanbase::sql::init_sql_factories();
  ::testing::InitGoogleTest(&argc,argv);
  system("rm -plan test_plan_set.log");
  OB_LOGGER.set_log_level("DEBUG");
  OB_LOGGER.set_file_name("test_plan_set.log", true);

  ::test::init_sql_plan_set();
  return RUN_ALL_TESTS();
}
