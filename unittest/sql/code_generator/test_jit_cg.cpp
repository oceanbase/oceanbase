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

#include <iterator>
#include <gtest/gtest.h>
#define private public
#include "sql/test_sql_utils.h"
#include "sql/code_generator/ob_code_generator_impl.h"
#include "lib/container/ob_array.h"
#include "lib/alloc/malloc_hook.h"
#include "objit/ob_jitter.h"
#include "sql/ob_sql_init.h"
#include "sql/engine/dml/ob_table_insert.h"
#include "sql/optimizer/test_optimizer_utils.h"
#include "sql/plan_cache/ob_cache_object_factory.h"
#include "sql/engine/ob_phy_operator.h"
#include "sql/engine/ob_double_children_phy_operator.h"
#include "sql/engine/ob_multi_children_phy_operator.h"
#include "observer/ob_req_time_service.h"
#undef private
using namespace oceanbase::common;

namespace test
{
class MockVisitor : public ObPhyOperatorVisitor
{
public:
  MockVisitor() : level_(0) {}
  ~MockVisitor() {}
  virtual int pre_visit(const ObSingleChildPhyOperator &op)
  {
    int ret = OB_SUCCESS;
    OB_LOG(INFO, "output:", K(level_), "type", op.get_type());
    level_++;
    return ret;
  }
  virtual int post_visit(const ObSingleChildPhyOperator &op)
  {
    level_--;
    const_cast<ObSingleChildPhyOperator&>(op).reuse();
    return OB_SUCCESS;
  }
  virtual int pre_visit(const ObDoubleChildrenPhyOperator &op)
  {
    int ret = OB_SUCCESS;
    OB_LOG(INFO, "output:", K(level_), "type", op.get_type());
    level_++;
    return ret;
  }
  virtual int post_visit(const ObDoubleChildrenPhyOperator &op)
  {
    level_--;
    const_cast<ObDoubleChildrenPhyOperator&>(op).reuse();
    return OB_SUCCESS;
  }
  virtual int pre_visit(const ObMultiChildrenPhyOperator &op)
  {
    int ret = OB_SUCCESS;
    OB_LOG(INFO, "output:", K(level_), "type", op.get_type());
    level_++;
    return ret;
  }
  virtual int post_visit(const ObMultiChildrenPhyOperator &op)
  {
    level_--;
    const_cast<ObMultiChildrenPhyOperator&>(op).reuse();
    return OB_SUCCESS;
  }
  virtual int pre_visit(const ObNoChildrenPhyOperator &op)
  {
    int ret = OB_SUCCESS;
    OB_LOG(INFO, "output:", K(level_), "type", op.get_type());
    level_++;
    return ret;
  }
  virtual int post_visit(const ObNoChildrenPhyOperator &op)
  {
    level_--;
    const_cast<ObNoChildrenPhyOperator&>(op).reuse();
    return OB_SUCCESS;
  }
  virtual int pre_visit(const ObPhyOperator &op)
  {
    int ret = OB_SUCCESS;
    OB_LOG(INFO, "output:", K(level_), "type", op.get_type());
    level_++;
    return ret;
  }
  virtual int post_visit(const ObPhyOperator &op)
  {
    level_--;
    const_cast<ObPhyOperator&>(op).reuse();
    return OB_SUCCESS;
  }
private:
  int level_;
};

class TestCodeGenerator: public TestOptimizerUtils
{
public:
  TestCodeGenerator();
  virtual ~TestCodeGenerator();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestCodeGenerator);
protected:
  // function members
  void do_optimize(ObStmt &stmt,
                   ObLogPlan *&plan,
                   ObPhyPlanType distr_method);
  void do_code_generate(const ObLogPlan &log_plan,
                        ObPhysicalPlan &phy_plan);
protected:
  // data members
  ParamStore params_;
  ObAddr addr_;
  ObOptimizerContext *optimizer_ctx_;
  ObQueryHint query_hint_;
};

TestCodeGenerator::TestCodeGenerator()
    : optimizer_ctx_(NULL)
{
  memcpy(schema_file_path_, "./test_jit_cg.schema", sizeof("./test_jit_cg.schema"));
}

TestCodeGenerator::~TestCodeGenerator()
{
}

void TestCodeGenerator::SetUp()
{
  TestOptimizerUtils::SetUp();
  exec_ctx_.get_sql_ctx()->session_info_ = &session_info_;
  optimizer_ctx_ = new ObOptimizerContext(&session_info_,
                                          &exec_ctx_,
                                          //schema_mgr_,
                                          &sql_schema_guard_,
                                          &stat_manager_,
                                          NULL,
                                          &partition_service_,
                                          allocator_,
                                          &part_cache_,
                                          &params_,
                                          addr_,
                                          NULL,
                                          query_hint_,
                                          expr_factory_,
                                          NULL,
                                          false,
                                          stmt_factory_.get_query_ctx());
  ASSERT_TRUE(optimizer_ctx_);
}

void TestCodeGenerator::TearDown()
{
  // destroy
  destroy();
  if (NULL != optimizer_ctx_) {
    delete optimizer_ctx_;
    optimizer_ctx_ = NULL;
  }
}

void TestCodeGenerator::do_optimize(ObStmt &stmt,
                                    ObLogPlan *&plan,
                                    ObPhyPlanType distr)
{
  ParamStore &params_i = *(&params_);
  int64_t count = params_i.count();
  UNUSED(count);
  ObTableLocation table_location;

  OK(optimizer_ctx_->get_table_location_list().push_back(table_location));
  if (distr == OB_PHY_PLAN_REMOTE) {
    SQL_CG_LOG(DEBUG, "setting local address to 2.2.2.2");
    optimizer_ctx_->set_local_server_addr("2.2.2.2", 8888);
  } else {
    SQL_CG_LOG(DEBUG, "setting local address to 1.1.1.1");
    optimizer_ctx_->set_local_server_addr("1.1.1.1", 8888);
  }

  ObQueryHint query_hint = dynamic_cast<ObDMLStmt&>(stmt).get_stmt_hint().get_query_hint();
  optimizer_ctx_->set_query_hint(query_hint);
  optimizer_ctx_->set_root_stmt(dynamic_cast<ObDMLStmt*>(&stmt));
  ObOptimizer optimizer(*optimizer_ctx_);
  OK(optimizer.optimize(dynamic_cast<ObDMLStmt&>(stmt), plan));
  char buf[1024];
  plan->to_string(buf, 1024);
  _OB_LOG(INFO, "logical_plan=%s", buf);
}

void TestCodeGenerator::do_code_generate(const ObLogPlan &log_plan,
                                         ObPhysicalPlan &phy_plan)
{
  ObCodeGeneratorImpl code_gen(10, true);
  OK(code_gen.generate(log_plan, phy_plan));
}

TEST_F(TestCodeGenerator, basic_test)
{
  const char* test_file = "./test_jit_cg.test";
  //const char* result_file = "./test_jit_cg.result";
  const char* tmp_file = "./test_jit_cg.tmp";
  // run tests
  std::ifstream if_tests(test_file);
  ASSERT_TRUE(if_tests.is_open());
  std::ofstream of_result(tmp_file);
  ASSERT_TRUE(of_result.is_open());
  std::string line;
  ObStmt *stmt = NULL;
  ObLogPlan *logical_plan = NULL;
  ObPhysicalPlan *phy_plan = NULL;
  int64_t line_no = 1;
  while (std::getline(if_tests, line)) {
    if (line.size() <= 0) continue;
    if (line.at(0) == '#') continue;
    _OB_LOG(DEBUG, "================================================================");
    of_result << "[" << line_no++ << "] " ;
    of_result << line << std::endl;
    bool is_print = true;
    params_.reset();
    ASSERT_NO_FATAL_FAILURE(do_resolve(line.c_str(), stmt, is_print, JSON_FORMAT, OB_SUCCESS, false));
    ASSERT_NO_FATAL_FAILURE(do_optimize(*stmt, logical_plan, OB_PHY_PLAN_REMOTE));
    ObCacheObjectFactory::alloc(phy_plan);
    ASSERT_TRUE(NULL != (phy_plan));
    ASSERT_NO_FATAL_FAILURE(do_code_generate(*logical_plan, *phy_plan));
    of_result << CSJ(*phy_plan) << std::endl;
    ObPhyOperator *main_query = phy_plan->get_main_query();
    ASSERT_TRUE(NULL != main_query);
    MockVisitor visitor;
    ASSERT_EQ(OB_SUCCESS, main_query->accept(visitor));
    //@todo ObPhysicalPlan::free(phy_plan);
    phy_plan = NULL;
    stmt_factory_.destory();
    expr_factory_.destory();
  }
  of_result.close();
  // verify results
  //ASSERT_NO_FATAL_FAILURE(TestSqlUtils::is_equal_content(tmp_file, result_file));
}
}

int main(int argc, char **argv)
{
  system("rm -rf test_jit_cg.log");
  observer::ObReqTimeGuard req_timeinfo_guard;
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_jit_cg.log", true);
  ::oceanbase::sql::init_sql_factories();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
