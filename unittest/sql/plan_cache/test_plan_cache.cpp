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

#define private public
#define protected public
#define USING_LOG_PREFIX SQL
#include "sql/test_sql_utils.h"
#include <unistd.h>
#include "lib/json/ob_json.h"
#include "test_sql.h"
#include "sql/plan_cache/ob_cache_object_factory.h"
using namespace oceanbase::common;
using namespace oceanbase::json;
using namespace oceanbase::share::schema;


namespace test
{
static int MAX_THREAD_COUNT = 1;
static int LOOP_PER_CASE = 10;
static bool PREPARE_PLAN_CACHE = false;
static bool WITH_SCHEMA_ALTER = false;
static bool WITH_CACHE_EVICT = true;
static bool WITH_PLAN_VERIFY = false;
static bool OUTPUT_EXEC_TIME = false;
static bool OUTPUT_LOGIC_PLAN = false;
static bool OUTPUT_PHY_PLAN = false;
static const int64_t BUKET_NUM = 102400;
static const int64_t MAX_FILE_NAME_LEN = 1024;
static const int64_t HIGH_WATER_MARK = 500 * 1024;  // 10 plans
static const int64_t LOW_WATER_MARK = 500 * 1024;    // 5 plans

static TestSQL *test_sql = NULL;
static ObPlanCache *plan_cache = NULL;

void init_pc()
{
  if (NULL == test_sql) {
    test_sql = new TestSQL(ObString("test_schema.sql"));
    ASSERT_TRUE(test_sql);
    test_sql->init();
  }
  if (NULL == plan_cache) {
    plan_cache = new ObPlanCache();
    plan_cache->init(BUKET_NUM, test_sql->get_addr(), test_sql->get_part_cache(), 1);
    plan_cache->inc_ref_count();
  }
}

class TestPlanCache : public ::testing::Test
{
public:
  TestPlanCache() {}
  virtual ~TestPlanCache() {}
  void SetUp() {}
  void TearDown() {}
private:
  // disallow copy
  TestPlanCache(const TestPlanCache &other);
  TestPlanCache& operator=(const TestPlanCache &other);
};

void do_operation(const char *query, std::ofstream &of_result)
{
  of_result << "***************   Case Start"<< test_sql->next_case_id() << "  ***************" << std::endl;
  of_result << std::endl;
  of_result << "SQL: " << query << std::endl;
  of_result << std::endl;

  ObArenaAllocator allocator(ObModIds::TEST);
  ObRawExprFactory expr_factory(allocator);
  ObStmtFactory stmt_factory(allocator);
  ObLogPlanFactory log_plan_factory(allocator);
  ObExecContext exec_ctx;
  ParseResult parse_result;
  ObStmt *stmt = NULL;
  ObLogPlan *logical_plan = NULL;
  ObPhysicalPlan *phy_plan = NULL;

  int64_t time_1 = 0;
  int64_t time_2 = 0;
  int64_t time_3 = 0;
  int64_t time_4 = 0;
  int64_t time_5 = 0;
  int64_t time_6 = 0;
  int64_t time_7 = 0;
  int64_t time_8 = 0;
  int64_t time_9 = 0;

  // parse
  time_1 = TestSQL::get_usec();
  time_2 = TestSQL::get_usec();
  // plan cache
  ObArray<ObObjParam> params;
  int ret = OB_SUCCESS;
  ObSQLSessionInfo session;
  ObSqlCtx context;
  //context.schema_manager_ = test_sql->get_schema_manager();
  context.schema_guard_ = &(test_sql->get_schema_guard());
  //test error
  ObPlanCacheKey key;
  exec_ctx.reset();
  exec_ctx.get_task_executor_ctx()->set_min_cluster_version(CLUSTER_VERSION_1500);
  context.session_info_ = &session;
  ret = plan_cache->get_plan(allocator,
                             context,
                             ObString::make_string(query),
                             exec_ctx,
                             phy_plan,
                             key);

  TestSqlCtx test_sql_ctx;
  test_sql_ctx.allocator_ = &allocator;
  test_sql_ctx.expr_factory_ = &expr_factory;
  test_sql_ctx.stmt_factory_ = &stmt_factory;
  test_sql_ctx.log_plan_factory_ = &log_plan_factory;
  time_3 = TestSQL::get_usec();
  if (OB_NOT_SUPPORTED == ret || OB_REACH_MEMORY_LIMIT == ret) {
    SQL_PC_LOG(DEBUG, "memory limit or distribute plan not support", K(ret));
  } else if (OB_SUCCESS != ret || NULL == phy_plan) {
    // parse
    test_sql->do_parse(allocator, query, parse_result);
    // resolve
    ret = test_sql->do_resolve(test_sql_ctx, parse_result, stmt, &params);
    EXPECT_EQ(OB_SUCCESS, ret);
    SQL_PC_LOG(INFO, "after do_resolve","parse_result.result_tree_", CSJ(ObParserResultPrintWrapper(*parse_result.result_tree_)));
    time_4 = TestSQL::get_usec();
    // logical plan
    test_sql->generate_logical_plan(test_sql_ctx, &params, stmt, logical_plan);
    time_5 = TestSQL::get_usec();
    if (OUTPUT_LOGIC_PLAN) {
      char buf[BUF_LEN];
      logical_plan->to_string(buf, BUF_LEN);
      of_result << buf << std::endl;
    }

    // physical plan
    test_sql->generate_physical_plan(logical_plan, phy_plan);
    phy_plan->set_plan_id(plan_cache->allocate_plan_id());
    time_6 = TestSQL::get_usec();
    SQL_PC_LOG(INFO, "phy_plan size", K(phy_plan->get_mem_size()), K(sizeof(ObPhysicalPlan))/*K(phy_plan->total_mem_size_)*/);
    //add_plan error
    ret = plan_cache->add_plan(key,
                               ObString::make_string(query),
                               NULL,
                               logical_plan->get_table_partition_info());
    EXPECT_TRUE(OB_FAIL(ret));

    ret = plan_cache->add_plan(key,
                               ObString::make_string(query),
                               phy_plan,
                               logical_plan->get_table_partition_info());
    EXPECT_TRUE(OB_SUCCESS == ret
                || OB_SQL_PC_PLAN_DUPLICATE == ret
                || OB_NOT_SUPPORTED == ret
                || OB_REACH_MEMORY_LIMIT == ret);
    time_7 = TestSQL::get_usec();

    if (OB_FAIL(ret)) {
      SQL_PC_LOG(WARN, "failed to add plan to plan cache", K(ret));
    } else {
      SQL_PC_LOG(INFO, "succeed to add physical plan", K(ret));
      SQL_PC_LOG(INFO, "memory usage", "mem used", plan_cache->get_mem_used(),
                                       "hwm", plan_cache->get_mem_hwm(),
                                       "lwm", plan_cache->get_mem_lwm());

      if (OB_FAIL(ret)) {
        SQL_PC_LOG(WARN, "failed to add plan statistic to plan cache", K(ret));
      }
      ret = plan_cache->remove_plan_stat_entry(phy_plan->get_plan_id());
      EXPECT_TRUE(OB_SUCC(ret));
      ret = plan_cache->add_plan_stat(test_sql->get_database_id(), ObString::make_string(query),
 phy_plan);
      EXPECT_TRUE(OB_SUCC(ret));
    }
  } else {//get plan from plan cache success
    SQL_PC_LOG(INFO, "get plan from pc", K(*phy_plan), "ref_count", phy_plan->get_ref_count());
    //plan_id->stat may_be erase by other thread
    plan_cache->update_plan_stat(phy_plan->get_plan_id(), 100 * 1000);

    ObPlanStat plan_stat;
    if (OB_SUCCESS != (ret = plan_cache->get_plan_stat(phy_plan->get_plan_id(), plan_stat))) {
      SQL_PC_LOG(WARN, "failed to get plan statistic in plan cache", K(ret));
    }
  }

  if (OUTPUT_PHY_PLAN) {
    char buf[BUF_LEN];
    phy_plan->to_string(buf, BUF_LEN);
    of_result << buf << std::endl;
  }
  time_8 = TestSQL::get_usec();
  ObCacheObjectFactory::free(phy_plan);
  //ObPhysicalPlan::free(phy_plan);
  time_9 = TestSQL::get_usec();

  if (OUTPUT_EXEC_TIME) {
    //of_result << "<<<<<<<<<<<<<<<<<<<<<<<<<<<<" << std::endl;
    of_result << "parse:     " << time_2 - time_1 << std::endl;
    of_result << "pc_get:   " << time_3 - time_2 << std::endl;
    if (time_4 != 0) {
      of_result << "resolve:  " << time_4 - time_3 << std::endl;
      of_result << "optimize:  " << time_5 - time_4 << std::endl;
      of_result << "cg:  " << time_6 - time_5 << std::endl;
      of_result << "pc_add:  " << time_7 - time_6 << std::endl;
    }
    of_result << "plan_free:  " << time_9 - time_8 << std::endl;
    //of_result << ">>>>>>>>>>>>>>>>>>>>>>>>>>>>" << std::endl;
  }

  of_result << "***************   Case END   ************** " << std::endl;
  of_result << "***************   Case END"<< test_sql->get_case_id() << "  ***************" << std::endl;
  of_result << std::endl;
}

void test_plan_cache(obsys::CThread *thread)
{
  const char *test_file = "test_plan_cache.sql";
  const char *result_file = "test_plan_cache.result";
  const char *tmp_file = "test_plan_cache.temp";
  if (NULL != thread) {
    char temp_buffer[MAX_FILE_NAME_LEN];
    sprintf(temp_buffer, "test_plan_cache.temp.%d", thread->getpid());
    tmp_file = temp_buffer;
  }
  std::ofstream of_result(tmp_file);
  ASSERT_TRUE(of_result.is_open());
  std::ifstream if_tests(test_file);
  ASSERT_TRUE(if_tests.is_open());
  std::string line;
  std::string total_line;
  while (std::getline(if_tests, line)) {
    // allow human readable formatting
    if (line.size() <= 0) continue;
    std::size_t begin = line.find_first_not_of('\t');
    if (line.at(begin) == '#') continue;
    std::size_t end = line.find_last_not_of('\t');
    std::string exact_line = line.substr(begin, end - begin + 1);
    total_line += exact_line;
    total_line += " ";
    if (exact_line.at(exact_line.length() - 1) != ';') continue;
    else {
      do_operation(total_line.c_str(), of_result);
      total_line = "";
    }
  }
  if_tests.close();
  of_result.close();

  if (WITH_PLAN_VERIFY) {
    std::ifstream if_result(tmp_file);
    ASSERT_TRUE(if_result.is_open());
    std::istream_iterator<std::string> it_result(if_result);
    std::ifstream if_expected(result_file);
    ASSERT_TRUE(if_expected.is_open());
    std::istream_iterator<std::string> it_expected(if_expected);
    ASSERT_TRUE(std::equal(it_result, std::istream_iterator<std::string>(), it_expected));
    std::remove(tmp_file);
  }
}

void alter_schema_mgr_version()
{
  // schema mgr's version
  //ObSchemaManager *schema_mgr = test_sql->get_schema_manager();
  //int64_t schema_version = schema_mgr->get_schema_version();
  //schema_mgr->set_schema_version(schema_version + 1);
}

void alter_table_schema_version()
{
  // schema mgr's version
  ObSchemaGetterGuard &schema_guard = test_sql->get_schema_guard();
  // table's version; table t1's id is 1
  int64_t tid = combine_id(OB_SYS_TENANT_ID, 50000);
  const ObTableSchema *table_schema = NULL;
  OK(schema_guard.get_table_schema(tid, table_schema));
  ObTableSchema *table_schema1 = const_cast<ObTableSchema *>(table_schema);
  OK(test_sql->add_table_schema(*table_schema1));
  //if (table_schema) {
  //  int64_t schema_version = table_schema->get_schema_version();
  //  table_schema->set_schema_version(schema_version + 1);
  //}
}

class PlanCacheRunnable : public share::ObThreadPool
{
public:
  void run1()
  {
    UNUSED(arg);
    //TestPlanCache *test_pc = reinterpret_cast<TestPlanCache *>(arg);
    LOG_INFO("start thread", K(thread));

    // alter schema
    if (WITH_SCHEMA_ALTER) {
      //alter_schema_mgr_version();
      alter_table_schema_version();
    }
    int64_t count = LOOP_PER_CASE;
    while (count--) {
      test_plan_cache(thread);
      plan_cache->cache_evict();
      plan_cache->cache_evict_all();
    }
  }
};

void run_test()
{
  // prepare
  test_plan_cache(NULL);
  // test
  PlanCacheRunnable pc_runner;
  obsys::CThread pc_threads[MAX_THREAD_COUNT];
  for (int i = 0; i < MAX_THREAD_COUNT; ++i) {
    pc_threads[i].start(&pc_runner, NULL);
  }
  for (int i = 0; i < MAX_THREAD_COUNT; ++i) {
    pc_threads[i].join();
  }
}

TEST_F(TestPlanCache, basic)
{
  int64_t time_1 = 0;
  int64_t time_2 = 0;

  // prepare
  if (PREPARE_PLAN_CACHE) {
    test_plan_cache(NULL);
  }
  time_1 = ::test::TestSQL::get_usec();
  // test
  PlanCacheRunnable pc_runner;
  obsys::CThread pc_threads[MAX_THREAD_COUNT];
  for (int i = 0; i < MAX_THREAD_COUNT; ++i) {
    pc_threads[i].start(&pc_runner, NULL);
  }
  for (int i = 0; i < MAX_THREAD_COUNT; ++i) {
    pc_threads[i].join();
  }

  // cache evict
  if (WITH_CACHE_EVICT) {
    plan_cache->cache_evict_all();
  }

  LOG_INFO("ref_count:", K(plan_cache->ref_count_));
  EXPECT_TRUE(plan_cache->get_ref_count() == 1);
  plan_cache->dec_ref_count();

  time_2 = ::test::TestSQL::get_usec();
  std::cout<<"total time: "<<time_2 - time_1<<std::endl;
}
}

int main(int argc, char **argv)
{
  ::oceanbase::sql::init_sql_factories();
  ::testing::InitGoogleTest(&argc,argv);

  int c = 0;
  while(-1 != (c = getopt(argc, argv, "t::n::pl::oev012"))) {
    switch (c) {
    case 't':
      if (NULL != optarg) {
        test::MAX_THREAD_COUNT = atoi(optarg);
      }
      break;
    case 'n':
      if (NULL != optarg) {
        test::LOOP_PER_CASE = atoi(optarg);
      }
      break;
    case 'p':
      test::PREPARE_PLAN_CACHE = true;
      break;
    case 'l':
      if (NULL != optarg) {
        OB_LOGGER.set_log_level(optarg);
      }
      break;
    case 'o':
      test::WITH_SCHEMA_ALTER = true;
      break;
    case 'e':
      test::WITH_CACHE_EVICT = true;
      break;
    case 'v':
      test::WITH_PLAN_VERIFY = true;
      break;
    case '0':
      test::OUTPUT_EXEC_TIME = true;
      break;
    case '1':
      test::OUTPUT_LOGIC_PLAN = true;
      break;
    case '2':
      test::OUTPUT_PHY_PLAN = true;
      break;
    default:
      break;
    }
  }

  ::test::init_pc();
  return RUN_ALL_TESTS();
}
