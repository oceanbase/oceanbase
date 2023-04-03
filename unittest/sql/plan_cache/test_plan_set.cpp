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

/*TEST_F(TestPlanSet, basic)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  MockPartitionService partition_service;
  MockLocalityManager locality_manager;
  ObRawExprFactory expr_factory(allocator);
  ObStmtFactory stmt_factory0(allocator);
  ObStmtFactory stmt_factory1(allocator);
  ObStmtFactory stmt_factory2(allocator);
  ObStmtFactory stmt_factory3(allocator);
  ObStmtFactory stmt_factory4(allocator);
  ObStmtFactory stmt_factory5(allocator);
  ObStmtFactory stmt_factory6(allocator);
  ObLogPlanFactory log_plan_factory(allocator);
  TestSqlCtx test_sql_ctx;
  test_sql_ctx.allocator_ = &allocator;
  test_sql_ctx.expr_factory_ = &expr_factory;
  test_sql_ctx.stmt_factory_ = &stmt_factory0;
  test_sql_ctx.log_plan_factory_ = &log_plan_factory;
  ObSQLSessionInfo session[TEST_PLAN_NUM];
  ObSqlCtx sql_ctx[TEST_PLAN_NUM];
  // just empty array to avoid failure while adding plan
  ObSEArray<ObPCConstParamInfo, 4> const_param_constraints;
  ObSEArray<ObPCParamEqualInfo, 4> equal_param_constraints;
  ObSEArray<ObPCParamPreciseInfo, 4> precise_param_constraints;
  ObDList<ObPreCalcExprConstraint> all_pre_calc_constraints_;
  ObSEArray<uint64_t, 4> trans_happened_route;
  for (int i = 0; i < TEST_PLAN_NUM; i++) {
    sql_ctx[i].schema_guard_ = &(test_sql->schema_guard_);
    sql_ctx[i].session_info_ = &(session[i]);
    sql_ctx[i].all_possible_const_param_constraints_ = &const_param_constraints;
    sql_ctx[i].all_plan_const_param_constraints_ = &const_param_constraints;
    sql_ctx[i].all_equal_param_constraints_ = &equal_param_constraints;
    sql_ctx[i].all_precise_param_constraints_ = &precise_param_constraints;
    sql_ctx[i].all_pre_calc_constraints_ = &all_pre_calc_constraints_;
    sql_ctx[i].trans_happened_route_ = &trans_happened_route;
  }
  ObExecContext exec_ctx[TEST_PLAN_NUM];
  for (int i = 0; i < TEST_PLAN_NUM; i++) {
    exec_ctx[i].create_physical_plan_ctx();
    exec_ctx[i].set_my_session(&(session[i]));
    exec_ctx[i].set_sql_ctx(&sql_ctx[i]);
    GCTX.par_ser_ = &partition_service;
    GCTX.locality_manager_ = &locality_manager;
    exec_ctx[i].get_task_executor_ctx()->set_min_cluster_version(CLUSTER_VERSION_1500);
  }
  ObLogPlan *logical_plan[TEST_PLAN_NUM];
  ObPhysicalPlan *phy_plan[TEST_PLAN_NUM];
  ObString sql[TEST_PLAN_NUM] = {
    ObString::make_string("SELECT SUM(L_EXTENDEDPRICE)/7.0 AS AVG_YEARLY FROM LINEITEM, PART WHERE P_PARTKEY = L_PARTKEY             AND P_BRAND = 'Brand#23'             AND P_CONTAINER = 'MED BOX'             AND L_QUANTITY < (SELECT 0.2*AVG(L_QUANTITY)                                              FROM LINEITEM                                              WHERE L_PARTKEY = P_PARTKEY);"),
    ObString::make_string("select * from t1 where c1 = 5"),
    ObString::make_string("select * from t1 where c1 = 2"),
    ObString::make_string("select * from t1 where c1 > 3"),
    ObString::make_string("select * from t1, t2 where t1.c1 = t2.c1"),
    ObString::make_string("select * from vt"),
    ObString::make_string("select @a union select @b")
  };
  // when add  generate_plan() , change TEST_PLAN_NUM
  generate_plan(test_sql_ctx, sql[0].ptr(),
                exec_ctx[0].get_physical_plan_ctx()->get_param_store_for_update(),
                logical_plan[0], phy_plan[0], session[0]);//local
  test_sql_ctx.stmt_factory_ = &stmt_factory1;
  generate_plan(test_sql_ctx, sql[1].ptr(),
                exec_ctx[1].get_physical_plan_ctx()->get_param_store_for_update(),
                logical_plan[1], phy_plan[1], session[1]);//local
  test_sql_ctx.stmt_factory_ = &stmt_factory2;
  generate_plan(test_sql_ctx, sql[2].ptr(),
                exec_ctx[2].get_physical_plan_ctx()->get_param_store_for_update(),
                logical_plan[2], phy_plan[2], session[2]);//remote
  test_sql_ctx.stmt_factory_ = &stmt_factory3;
  generate_plan(test_sql_ctx, sql[3].ptr(),
                exec_ctx[3].get_physical_plan_ctx()->get_param_store_for_update(),
                logical_plan[3], phy_plan[3], session[3]);//dist
  test_sql_ctx.stmt_factory_ = &stmt_factory4;
  generate_plan(test_sql_ctx, sql[4].ptr(),
                exec_ctx[4].get_physical_plan_ctx()->get_param_store_for_update(),
                logical_plan[4], phy_plan[4], session[4]);//dist
  test_sql_ctx.stmt_factory_ = &stmt_factory5;
  generate_plan(test_sql_ctx, sql[5].ptr(),
                exec_ctx[5].get_physical_plan_ctx()->get_param_store_for_update(),
                logical_plan[5], phy_plan[5], session[5]);//view
  test_sql_ctx.stmt_factory_ = &stmt_factory6;
  generate_plan(test_sql_ctx, sql[6].ptr(),
                exec_ctx[6].get_physical_plan_ctx()->get_param_store_for_update(),
                logical_plan[6], phy_plan[6], session[6]);


  LOG_INFO("phy_plan type:",K(phy_plan[0]->get_plan_type()),K(phy_plan[1]->get_plan_type()),K(phy_plan[2]->get_plan_type()),K(phy_plan[3]->get_plan_type()));

  // for (int i = 0; i < TEST_PLAN_NUM; i++) {
  //   phy_plan[i]->set_plan_id(i);
  // }
  common::ObSEArray<ObTablePartitionInfo *, 4> table_partition_info;
  for (int i = 0; i < TEST_PLAN_NUM; i++) {
    table_partition_info.reset();
    EXPECT_TRUE (OB_SUCCESS == logical_plan[i]->get_global_table_partition_info(table_partition_info));
    EXPECT_TRUE (OB_SUCCESS == exec_ctx[i].get_task_executor_ctx()->set_table_locations(
                     table_partition_info));
    EXPECT_TRUE (OB_SUCCESS == exec_ctx[i].get_sql_ctx()->set_partition_infos(
                     table_partition_info, allocator));
  }

  ObPlanCacheCtx pc_ctx[TEST_PLAN_NUM] = {
  ObPlanCacheCtx(sql[0], false, allocator, sql_ctx[0], exec_ctx[0], common::OB_SYS_TENANT_ID),
  ObPlanCacheCtx(sql[1], false, allocator, sql_ctx[1], exec_ctx[1], common::OB_SYS_TENANT_ID),
  ObPlanCacheCtx(sql[2], false, allocator, sql_ctx[2], exec_ctx[2], common::OB_SYS_TENANT_ID),
  ObPlanCacheCtx(sql[3], false, allocator, sql_ctx[3], exec_ctx[3], common::OB_SYS_TENANT_ID),
  ObPlanCacheCtx(sql[4], false, allocator, sql_ctx[4], exec_ctx[4], common::OB_SYS_TENANT_ID),
  ObPlanCacheCtx(sql[5], false, allocator, sql_ctx[5], exec_ctx[5], common::OB_SYS_TENANT_ID),
  ObPlanCacheCtx(sql[6], false, allocator, sql_ctx[6], exec_ctx[6], common::OB_SYS_TENANT_ID) };

  for (int64_t i = 0; i < TEST_PLAN_NUM; ++i) {
    pc_ctx[i].fp_result_.cache_params_ = &(exec_ctx[i].get_physical_plan_ctx()->get_param_store_for_update());
  }
  ObPCVSet pcv_set(plan_cache);
  (void)pcv_set.init(pc_ctx[0], phy_plan[0]);
  (void)pcv_set.create_new_pcv(plan_cache_value);
  plan_cache_value->init(&pcv_set, phy_plan[0], pc_ctx[0]);
  (void)plan_cache_value->create_new_plan_set(PST_SQL_CRSR, plan_set);
  plan_set->set_plan_cache_value(plan_cache_value);
  ASSERT_EQ(OB_SUCCESS, plan_set->init_new_set(pc_ctx[0], *(phy_plan[0]), OB_INVALID_INDEX, plan_cache_value->get_pc_alloc()));
  //test add_plan()
  EXPECT_EQ(OB_SUCCESS, plan_set->add_cache_obj(*(phy_plan[0]), pc_ctx[0], OB_INVALID_INDEX));
  //can't add if plan exists;
  EXPECT_NE(OB_SUCCESS, plan_set->add_cache_obj(*(phy_plan[0]), pc_ctx[0], OB_INVALID_INDEX));
  //add remote plan
  EXPECT_TRUE(OB_SUCCESS == plan_set->add_cache_obj(*(phy_plan[2]), pc_ctx[2], OB_INVALID_INDEX));


  ParamStore *params = pc_ctx[0].fp_result_.cache_params_;
  bool is_same = false;
  int ret = plan_set->match_params_info(params,
                                                        pc_ctx[0],
                                                        OB_INVALID_INDEX,
                                                        is_same);
  EXPECT_TRUE(OB_SUCCESS == ret);
  EXPECT_TRUE(true == is_same);

  common::Ob2DArray<ObParamInfo,
                    common::OB_MALLOC_BIG_BLOCK_SIZE,
                    common::ObWrapperAllocator, false> params_info ((ObWrapperAllocator(allocator)));
  sql::ObParamInfo param_info;
  for (int i = 0; i < params->count(); i++) {
    param_info.type_ =  params->at(i).get_type();
    param_info.scale_ = params->at(i).get_scale();
    param_info.flag_ = params->at(i).get_param_flag();
    params_info.push_back(param_info);
    param_info.reset();
  }

  is_same = false;
  EXPECT_TRUE(OB_SUCCESS == plan_set->match_params_info(params_info,
                                                        OB_INVALID_INDEX,
                                                        pc_ctx[0],
                                                        is_same));
  EXPECT_TRUE(true == is_same);

  // test plan_set matching with user variables
  ObSessionVariable var_a, var_b;
  ObString var_a_name("a"), var_b_name("b");

  var_a.value_.set_tinyint(6);
  var_b.value_.set_float(6.0);
  var_a.meta_.set_tinyint();
  var_b.meta_.set_float();
  ObSQLSessionInfo *session_info = sql_ctx[6].session_info_;
  session_info->replace_user_variable(var_a_name, var_a);
  session_info->replace_user_variable(var_b_name, var_b);
  plan_set->reset();

  // set related user var names
  sql_ctx[6].related_user_var_names_.reset();
  sql_ctx[6].related_user_var_names_.set_allocator(&allocator);
  sql_ctx[6].related_user_var_names_.init(2);
  sql_ctx[6].related_user_var_names_.push_back(var_a_name);
  sql_ctx[6].related_user_var_names_.push_back(var_b_name);

  plan_set->set_plan_cache_value(plan_cache_value);
  plan_set->init_new_set(pc_ctx[6], *(phy_plan[6]), OB_INVALID_INDEX, plan_cache_value->get_pc_alloc());

  params = pc_ctx[6].fp_result_.cache_params_;
  is_same = false;
  EXPECT_TRUE(OB_SUCCESS == plan_set->match_params_info(params,
                                                        pc_ctx[6],
                                                        OB_INVALID_INDEX,
                                                        is_same));
  EXPECT_TRUE(true == is_same);

  params_info.reset();
  for (int64_t i = 0; i < params->count(); i++) {
    param_info.type_ = params->at(i).get_type();
    param_info.scale_ = params->at(i).get_scale();
    param_info.flag_ = params->at(i).get_param_flag();
    params_info.push_back(param_info);
    param_info.reset();
  }
  is_same = false;
  EXPECT_TRUE(OB_SUCCESS == plan_set->match_params_info(params_info,
                                                        OB_INVALID_INDEX,
                                                        pc_ctx[6],
                                                        is_same));
  EXPECT_TRUE(true == is_same);

  // change user variable's value
  var_a.value_.set_tinyint(4);
  is_same = false;
  EXPECT_TRUE(OB_SUCCESS == plan_set->match_params_info(params,
                                                        pc_ctx[6],
                                                        OB_INVALID_INDEX,
                                                        is_same));
  EXPECT_TRUE(true == is_same);

  // change user variable's type
  var_a.value_.set_int(4);
  var_a.meta_.set_int();
  session_info->replace_user_variable(var_a_name, var_a);
  is_same = false;
  EXPECT_TRUE(OB_SUCCESS == plan_set->match_params_info(params,
                                                        pc_ctx[6],
                                                        OB_INVALID_INDEX,
                                                        is_same));
  EXPECT_TRUE(false == is_same);
  //test dist plan with single table
  plan_set->reset();
  plan_set->plan_cache_value_ = plan_cache_value;
  plan_set->init_new_set(pc_ctx[3], *(phy_plan[3]), OB_INVALID_INDEX, plan_cache_value->get_pc_alloc());
  EXPECT_TRUE(OB_SUCCESS == plan_set->add_cache_obj(*(phy_plan[3]), pc_ctx[3], OB_INVALID_INDEX));
  ObCacheObject *plan_selected = NULL;
  EXPECT_TRUE(OB_SUCCESS == plan_set->select_plan(test_sql->get_part_cache(),
                                                  pc_ctx[3],
                                                  plan_selected));
  //test dist plan with multi table, not support
  plan_set->remove_all_plan();
  plan_set->reset();
  plan_set->plan_cache_value_ = plan_cache_value;
  plan_set->init_new_set(pc_ctx[4], *(phy_plan[4]), OB_INVALID_INDEX, plan_cache_value->get_pc_alloc());
  EXPECT_TRUE(OB_SUCCESS == plan_set->add_cache_obj(*(phy_plan[4]), pc_ctx[4], OB_INVALID_INDEX));

  //test remote plan
  plan_set->reset();
  plan_set->plan_cache_value_ = plan_cache_value;
  plan_set->init_new_set(pc_ctx[2], *(phy_plan[2]), OB_INVALID_INDEX, plan_cache_value->get_pc_alloc());
  ASSERT_EQ(OB_SUCCESS, plan_set->init_new_set(pc_ctx[2], *(phy_plan[2]), OB_INVALID_INDEX, plan_cache_value->get_pc_alloc()));
  EXPECT_EQ(OB_SUCCESS, plan_set->add_cache_obj(*(phy_plan[2]), pc_ctx[2], OB_INVALID_INDEX));
  plan_selected = NULL;
  EXPECT_EQ(OB_SUCCESS, plan_set->select_plan(test_sql->get_part_cache(), pc_ctx[2], plan_selected));
  // EXPECT_TRUE(2 == plan_selected->get_object_id());

  for(int i = 0; i < 3; i++) {
    plan_set->remove_all_plan();
    plan_set->reset();
    plan_set->plan_cache_value_ = plan_cache_value;
    ASSERT_EQ(OB_SUCCESS, plan_set->init_new_set(pc_ctx[i], *(phy_plan[i]), OB_INVALID_INDEX, plan_cache_value->get_pc_alloc()));
    EXPECT_EQ(OB_SUCCESS, plan_set->add_cache_obj(*(phy_plan[i]), pc_ctx[i], OB_INVALID_INDEX));
    //EXPECT_TRUE(phy_plan[i]->get_mem_size() == plan_set->get_mem_size());
  }
}*/
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
