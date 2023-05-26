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
#include "test_sql.h"
#include <iostream>
#include <fstream>
#include <unistd.h>

using namespace oceanbase::common;
using namespace oceanbase::share::schema;

namespace test
{
static int THREAD_COUNT = 1;
static const int MAX_THREAD_COUNT = 1000;
static int LOOP_PER_CASE = 1;
static bool PREPARE_PLAN_CACHE = false;
static bool WITH_SCHEMA_ALTER = false;
static int64_t SCHEMA_ALTER_LOOP_FREQUENCY = 5;
static int64_t MAX_TENANT_NUM = 10;
static bool WITH_CACHE_EVICT = false;
static int64_t CACHE_EVICT_LOOP_FREQUENCY = 1;
static bool WITH_PLAN_VERIFY = false;
static bool OUTPUT_EXEC_TIME = false;
static bool OUTPUT_LOGIC_PLAN = false;
static bool OUTPUT_PHY_PLAN = false;
static const int64_t BUKET_NUM = 102400;
static const int64_t MAX_FILE_NAME_LEN = 1024;
static const int64_t HIGH_WATER_MARK = 150 * 35 * 1024;  // 15 plans
static const int64_t LOW_WATER_MARK = 50 * 35 * 1024;    // 5 plans

static TestSQL *test_sql = NULL;
static ObPlanCacheManager *plan_cache_mgr = NULL;
static int loop_counts[MAX_THREAD_COUNT] = {0};
static int runing_thread_count = 0;

void init_pc()
{
  if (NULL == test_sql) {
    test_sql = new TestSQL(ObString("test_performance.schema"));
    test_sql->init();
  }

  if (NULL == plan_cache_mgr) {
    plan_cache_mgr = new ObPlanCacheManager;
    plan_cache_mgr->init(test_sql->get_part_cache(), test_sql->get_addr());
  }
}

//void alter_schema_mgr_version()
//{
//  // schema mgr's version
//  //ObSchemaManager *schema_mgr = test_sql->get_schema_manager();
//  //int64_t schema_version = schema_mgr->get_schema_version();
//  //schema_mgr->set_schema_version(schema_version + 1);
//  ObSchemaGetterGuard schema_guard = test_sql->get_schema_guard();
//  int64_t schema_version = schema_guard.get_schema_version();
//  schema_guard.set_schema_version(schema_version + 1);
//}
//
void alter_table_schema_version()
{
  // schema mgr's version
  ObSchemaGetterGuard &schema_guard = test_sql->get_schema_guard();
  // table's version; table t1's id is 1
  int64_t tid = combine_id(OB_SYS_TENANT_ID, 50000);
  const ObTableSchema *table_schema = NULL;
  OK(schema_guard.get_table_schema(tid, table_schema));
  OK(add_table_schema(*table_schema));
}

void alter_view_schema_version()
{
  // schema mgr's version
  ObSchemaManager *schema_mgr = test_sql->get_schema_manager();
  // view's version; view vt's id is 3001
  int64_t tid = combine_id(OB_SYS_TENANT_ID, 3001);
  ObTableSchema *view_schema = const_cast<ObTableSchema *>(schema_mgr->get_table_schema(tid));
  int64_t schema_version = view_schema->get_schema_version();
  view_schema->set_schema_version(schema_version + 1);
}

//execute one sql
void do_operation(const char *query, uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObRawExprFactory expr_factory(allocator);
  ObStmtFactory stmt_factory(allocator);
  ObLogPlanFactory log_plan_factory(allocator);
  TestSqlCtx test_sql_ctx;
  test_sql_ctx.allocator_ = &allocator;
  test_sql_ctx.expr_factory_ = &expr_factory;
  test_sql_ctx.stmt_factory_ = &stmt_factory;
  test_sql_ctx.log_plan_factory_ = &log_plan_factory;
  //common::print_malloc_stats(false);
  ObExecContext exec_ctx;
  ParseResult parse_result;
  ObStmt *stmt = NULL;
  ObLogPlan *logical_plan = NULL;
  ObPhysicalPlan *phy_plan = NULL;
  ObString query_str = ObString::make_string(query);
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
  ObPlanCache plan_cache;
  if (OB_FAIL(test_sql->do_parse(allocator, query, parse_result))) {
    SQL_PC_LOG(WARN, "Generate syntax tree failed", K(query_str), K(ret));
  } else if (OB_FAIL(plan_cache.init(common::OB_PLAN_CACHE_BUCKET_NUMBER, tenant_id))) {
    LOG_WARN("failed to init request manager", K(ret));
  } else {
    _OB_LOG(INFO, "%s", CSJ(ObParserResultPrintWrapper(*parse_result.result_tree_)));
    time_2 = TestSQL::get_usec();
    // plan cache

    //ObPlanCache *plan_cache = plan_cache_mgr->get_or_create_plan_cache(tenant_id);
    if (NULL != plan_cache) {
      plan_cache.set_mem_hwm(HIGH_WATER_MARK);
      plan_cache.set_mem_lwm(LOW_WATER_MARK);
      SQL_PC_LOG(INFO, "get plan cache", K(query_str));

      ObSysVarInPC sys_var_in_pc;
      StmtKey key;
      key.mode_ = MODE_TREE;
      key.db_id_ = test_sql->get_database_id();
      key.tree_ = parse_result.result_tree_;
      key.stmt_ = common::ObString::make_string(query);
      key.system_variables_ = sys_var_in_pc;
      ObArray<ObObjParam> params;
      uint64_t sql_id = 0;
      uint32_t version = 0;
      ObSQLSessionInfo session;
      session.init(version, 0, 0, &allocator);
      if (OB_SUCCESS != session.init_system_variables(false, true)) {
        exit(-1);
      }
      ObSqlCtx context;
      context.schema_manager_ = test_sql->get_schema_manager();
      context.session_info_ = &session;
      exec_ctx.get_task_executor_ctx()->set_min_cluster_version(CLUSTER_VERSION_1500);
      if (NULL != context.schema_manager_) {
        ret = plan_cache.get_plan(allocator, key,
                                   exec_ctx,
                                   params, sql_id,
                                   phy_plan,
                                   context);
        SQL_PC_LOG(INFO, "get plan from plan cache", K(sql_id), K(phy_plan), K(ret));
        time_3 = TestSQL::get_usec();
        if (OB_SUCC(ret)) {
          plan_cache.update_plan_stat(phy_plan->get_plan_id(), true, time_7 - time_1);
          SQL_PC_LOG(INFO, "get plan from pc");
        } else if (OB_SQL_PC_NOT_EXIST == ret) {
          SQL_PC_LOG(INFO,"plan not exist in plan cache");
          plan_cache.update_plan_stat(0, false, 0);
          // resolve
          SQL_PC_LOG(INFO, "start to generate physical plan : resolve-->logical_plan-->physical_plan");
          if (OB_FAIL(test_sql->do_resolve(test_sql_ctx, parse_result, stmt, &params))) {
            SQL_PC_LOG(WARN, "fail to resolve", K(*stmt), K(ret));
          } else {
            time_4 = TestSQL::get_usec();
            // logical plan
            if (OB_FAIL(test_sql->generate_logical_plan(test_sql_ctx, &params, stmt, logical_plan))) {
              SQL_PC_LOG(WARN, "fail to generate logical plan ", K(params), K(*stmt), K(ret));
            } else {
              time_5 = TestSQL::get_usec();
              if (OUTPUT_LOGIC_PLAN) {
                SQL_PC_LOG(INFO, "ouput logical plan", K(*logical_plan));
              }
              //physical plan
              if (OB_FAIL(test_sql->generate_physical_plan(logical_plan, phy_plan))) {
                SQL_PC_LOG(WARN, "faile to generate physical plan", K(*logical_plan), K(ret));
              } else {
                phy_plan->set_plan_id(plan_cache.allocate_plan_id());
                SQL_PC_LOG(INFO, "generate physical plan end", K(phy_plan->get_plan_id()));
                if (OUTPUT_PHY_PLAN) {
                    SQL_PC_LOG(INFO, "ouput phy plan", K(*phy_plan));
                }
                time_6 = TestSQL::get_usec();
                SQL_PC_LOG(INFO, "add plan to plan cache");
                ret = plan_cache.add_plan(sql_id,
                                           ObString::make_string(query),
                                           sys_var_in_pc,
                                           phy_plan,
                                           logical_plan->get_table_partition_info());
                if (OB_SQL_PC_PLAN_DUPLICATE == ret) {
                  ret = OB_SUCCESS;
                  SQL_PC_LOG(INFO, "plan has been exised in plan_cache");
                } else if (OB_REACH_MEMORY_LIMIT == ret) {
                  ret = OB_SUCCESS;
                  LOG_DEBUG("plan cache reached memory limit, don't add plan to plan cache now");
                } else if (OB_NOT_SUPPORTED == ret) {
                  ret = OB_SUCCESS;
                  LOG_DEBUG("plan cache don't support add this kind of plan now", K(phy_plan->get_plan_type()));
                } else if (OB_FAIL(ret)) {
                  SQL_PC_LOG(INFO, "Failed to add plan to ObPlanCache", K(ret));
                } else {
                  SQL_PC_LOG(INFO, "Successed to add plan to ObPlanCache");
                  time_7 = TestSQL::get_usec();
                  if (OB_SUCCESS != plan_cache.add_plan_stat(key.db_id_, key.mode_,
                                                        sql_id, phy_plan)) {
                    SQL_PC_LOG(WARN, "Failed to add plancache stat");
                  }//add plan stat
                }//add plan
              }//generate physical plan
            }//generate logical plan
            for (int i = 0; i < log_plans.count(); ++i) {
              if (NULL != log_plans.at(i)) {
                log_plans.at(i)->~ObLogPlan();
                log_plans.at(i) = NULL;
              }
            }
            for (int i = 0; i < log_ops.count(); ++i) {
              if (NULL != log_ops.at(i)) {
                log_ops.at(i)->~ObLogicalOperator();
                log_ops.at(i) = NULL;
              }
            }
          }//resolve
        } else if (OB_SQL_DML_ONLY == ret) {
          SQL_PC_LOG(DEBUG, "sql is not dml", K(ret));
        } else if (OB_REACH_MEMORY_LIMIT == ret) {
          SQL_PC_LOG(DEBUG, "reach memory limit", K(ret));
        } else if (OB_NOT_SUPPORTED == ret) {
          SQL_PC_LOG(DEBUG, "is distr_plan, can't supported", K(ret));
        }

        if (OB_SUCC(ret)) {
          SQL_PC_LOG(INFO, "plan execution...", K(*phy_plan), "ref_count", phy_plan->get_ref_count(), K(ret));
        }
        time_8 = TestSQL::get_usec();
        if(NULL != phy_plan) {
          ObPhysicalPlan::free(phy_plan);
        }
        time_9 = TestSQL::get_usec();
      } else {
        ret =  OB_INVALID_ARGUMENT;
        SQL_PC_LOG(WARN, "schema_manager or plan_cache is NULL", K(ret));
      }
    } else {
      SQL_PC_LOG(WARN, "fail to get or create plan cache");
    }
  }
  expr_factory.destory();
  allocator.reset();
  //common::print_malloc_stats(false);
}

//execute one loop
void test_plan_cache(obsys::CThread *thread, int thread_num, std::ostream &of)
{

  uint64_t tenant_id = thread_num % MAX_TENANT_NUM;
  SQL_PC_LOG(DEBUG, "tenant entry", K(tenant_id));

  const char *test_file = "test_performance.sql";
  std::ifstream if_tests(test_file);
  if (!if_tests.is_open()) {
    SQL_PC_LOG(ERROR, "maybe reach max file open");
    exit(-1);
  }
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
      do_operation(total_line.c_str(), tenant_id);
      total_line = "";
    }
  }
  // alter version
  //schema version 和 table version变化后，总循环次数变为0，用于最终校验hit次数。
  int loop_count = ++loop_counts[thread_num];
  if (WITH_SCHEMA_ALTER && loop_count % SCHEMA_ALTER_LOOP_FREQUENCY == 0) {
    SQL_PC_LOG(INFO, "schema alter", "thread_num", thread_num, "tenant_id", tenant_id);
    //alter_schema_mgr_version();
    alter_table_schema_version();
    //loop_counts[thread->getpid() % THREAD_COUNT] = 0;
  }
  //alter view version
  if(WITH_SCHEMA_ALTER && loop_count % SCHEMA_ALTER_LOOP_FREQUENCY == 1) {
    SQL_PC_LOG(INFO, "view schema alter", "thread_num", thread_num , "tenant_id", tenant_id);
    //alter_schema_mgr_version();
    alter_table_schema_version();
  }

  if(true == WITH_CACHE_EVICT) {
    //flush plan cache
    of << "######### cache evict is true #######" << std::endl;
    of << "thread num" << thread_num
       << "tenant_id " << tenant_id
       << "loop_count" << loop_count << std::endl;
    if (tenant_id == 0 && loop_count % CACHE_EVICT_LOOP_FREQUENCY == 0) {
      of << std::endl << "########### start to flush plan cache:" << std::endl;
      plan_cache_mgr->revert_all_plan_cache();
      of << std::endl << "########### after flush plan cache:" << std::endl;
    }
    //cache evict
    if (loop_count % CACHE_EVICT_LOOP_FREQUENCY == 0) {

      ObPlanCache plan_cache;
      if (OB_FAIL(plan_cache.init(common::OB_PLAN_CACHE_BUCKET_NUMBER, tenant_id))) {
        LOG_WARN("failed to init request manager", K(ret));
      } else {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = plan_cache.cache_evict())) {
          SQL_PC_LOG(ERROR, "plan cache evict failed, please check");
        }
      }
    }
  }
  if_tests.close();
}

class PlanCacheRunnable : public share::ObThreadPool
{
public:
  void run1()
  {
    UNUSED(arg);
    LOG_INFO("start thread", K(thread));

    const char *tmp_file = "test_plan_cache.temp";
    if (NULL != thread) {
      char temp_buffer[MAX_FILE_NAME_LEN];
      sprintf(temp_buffer, "test_performance.temp.%d", thread->getpid());
      tmp_file = temp_buffer;
    }
    std::ofstream fout(tmp_file);

    int64_t count = LOOP_PER_CASE;
    //记录当前线程的线程序号；
    int thread_num = runing_thread_count++;
    while (count--) {
      test_plan_cache(thread, thread_num, fout);
      common::print_malloc_stats(false);
    }

    fout.close();
  }
};

void run_test()
{
  int64_t time_1 = 0;
  int64_t time_2 = 0;

  // prepare
  if (PREPARE_PLAN_CACHE) {
    //test_plan_cache(NULL);
  }
  // test
  time_1 = ::test::TestSQL::get_usec();
  PlanCacheRunnable pc_runner;
  obsys::CThread pc_threads[THREAD_COUNT];
  for (int i = 0; i < THREAD_COUNT; ++i) {
    pc_threads[i].start(&pc_runner, NULL);
  }
  for (int i = 0; i < THREAD_COUNT; ++i) {
    pc_threads[i].join();
  }
  time_2 = ::test::TestSQL::get_usec();
  //print_stat(std::cout);
  std::cout<<"total time: "<<time_2 - time_1<<std::endl;
}

}//namespace test end

int main(int argc, char **argv)
{
  ::oceanbase::sql::init_sql_factories();
  ::testing::InitGoogleTest(&argc,argv);

  int c = 0;
  while(-1 != (c = getopt(argc, argv, "t::n::pl::oev0123"))) {
    switch (c) {
    case 't':
      if (NULL != optarg) {
        test::THREAD_COUNT = atoi(optarg);
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

  common::reset_mem_leak_checker_label("ComArrObLogTabS");
  ::test::init_pc();
  ::test::run_test();
  return 0;
//  return RUN_ALL_TESTS();
}
