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

#define USING_LOG_PREFIX SQL_ENGINE
#include <iterator>
#include <gtest/gtest.h>
#include "sql/test_sql_utils.h"
#include "lib/container/ob_array.h"
#include "sql/ob_sql_init.h"
#include "sql/optimizer/test_optimizer_utils.h"
#include "sql/code_generator/ob_code_generator_impl.h"
#include "sql/plan_cache/ob_cache_object_factory.h"
#include "observer/ob_req_time_service.h"
namespace test
{
class TestEngine: public TestOptimizerUtils
{
public:
  TestEngine();
  virtual ~TestEngine();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestEngine);
protected:
  // function members
  void do_code_generate(const ObLogPlan &log_plan,
                        ObPhysicalPlan &phy_plan);
  int test_phy_plan(ObPhysicalPlan &plan);
protected:
  // data members
};

TestEngine::TestEngine()
{
  memcpy(schema_file_path_, "./test_engine.schema", sizeof("./test_engine.schema"));
}

TestEngine::~TestEngine()
{
}

void TestEngine::SetUp()
{
  TestOptimizerUtils::SetUp();
}

void TestEngine::TearDown()
{
  destroy();
}

void TestEngine::do_code_generate(const ObLogPlan &log_plan,
                                  ObPhysicalPlan &phy_plan)
{
  ObCodeGeneratorImpl code_gen(CLUSTER_VERSION_1500);
  OK(code_gen.generate(log_plan, phy_plan));
}

int TestEngine::test_phy_plan(ObPhysicalPlan &plan)
{
  UNUSED(plan);
  int ret = OB_SUCCESS;
  return ret;
}

TEST_F(TestEngine, basic_test)
{
  int ret = OB_SUCCESS;
  const char* test_file = "./test_engine.test";
  // run tests
  std::ifstream if_tests(test_file);
  ASSERT_TRUE(if_tests.is_open());
  std::string line;
  ObLogPlan *log_plan = NULL;
  ObPhysicalPlan *phy_plan = NULL;
  bool is_select = false;

  while (std::getline(if_tests, line)) {
    // handle query
    ObArenaAllocator allocator(ObModIds::TEST);
    ObResultSet result(session_info_, allocator);
    if (line.size() <= 0) continue;
    if (line.at(0) == '#') continue;
    ObString sql = ObString::make_string(line.c_str());
    ASSERT_NO_FATAL_FAILURE(generate_logical_plan(result, sql, line.c_str(), log_plan, is_select, true));
    ASSERT_TRUE(NULL != log_plan);
    ObCacheObjectFactory::alloc(phy_plan);
    ASSERT_TRUE(NULL != (phy_plan));
    ASSERT_NO_FATAL_FAILURE(do_code_generate(*log_plan, *phy_plan));

    _OB_LOG(DEBUG, "phy_plan=%s", CSJ(*phy_plan));

    // init exec ctx
    exec_ctx_.~ObExecContext();
    new (&exec_ctx_) ObExecContext(allocator);
    exec_ctx_.init_phy_op(phy_plan->get_phy_operator_size());
    ObTaskExecutorCtx &task_exec_ctx = exec_ctx_.get_task_exec_ctx();
    ObExecuteResult &exe_result = task_exec_ctx.get_execute_result();
    task_exec_ctx.set_min_cluster_version(CLUSTER_VERSION_1500);
    task_exec_ctx.set_partition_location_cache(&part_cache_);
    GCTX.par_ser_ = &partition_service_;
    task_exec_ctx.set_self_addr(optctx_->get_local_server_addr());
    common::ObSEArray<ObTablePartitionInfo *, 4> table_partition_info;
    EXPECT_TRUE(OB_SUCCESS == log_plan->get_global_table_partition_info(table_partition_info));
    task_exec_ctx.set_table_locations(table_partition_info);

    EXPECT_TRUE(OB_SUCCESS == exec_ctx_.create_physical_plan_ctx());
    GCTX.self_addr_seq_.set_addr(optctx_->get_local_server_addr());
    exec_ctx_.set_my_session(&session_info_);
    exec_ctx_.set_sql_ctx(&sql_ctx_);

    ObExecutor ob_exe;
    ob_exe.init(phy_plan);
    ASSERT_EQ(OB_SUCCESS, ob_exe.execute_plan(exec_ctx_));
    ASSERT_EQ(OB_SUCCESS, exe_result.open(exec_ctx_));
    if (phy_plan->get_stmt_type() == stmt::T_SELECT) {
      const ObNewRow *tmp_row = NULL;
      while(OB_SUCCESS == (ret = exe_result.get_next_row(exec_ctx_, tmp_row))) {
        SQL_EXE_LOG(INFO, "get a row", K(*tmp_row));
      }
      ASSERT_EQ(OB_ITER_END, ret);
    }
    ASSERT_EQ(OB_SUCCESS, ob_exe.close(exec_ctx_));
    ASSERT_EQ(OB_SUCCESS, exe_result.close(exec_ctx_));
    expr_factory_.destory();
    stmt_factory_.destory();
    ObCacheObjectFactory::free(phy_plan);
  }
}
}

int main(int argc, char **argv)
{
  system("rm -f test_engine.log");
  observer::ObReqTimeGuard req_timeinfo_guard;
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_engine.log", true);
  ::oceanbase::sql::init_sql_factories();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
