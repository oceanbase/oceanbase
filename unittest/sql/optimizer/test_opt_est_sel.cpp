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

#define USING_LOG_PREFIX SQL_OPT
#include <iostream>
#include <sys/time.h>
#include <iterator>
#include "lib/json/ob_json.h"
#define private public
#define protected public
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/optimizer/ob_select_log_plan.h"
#include "sql/optimizer/ob_opt_selectivity.h"
#include "sql/optimizer/test_optimizer_utils.h"
#include "observer/ob_req_time_service.h"


using namespace oceanbase::common;
namespace test
{
class TestOptEstSel:  public TestOptimizerUtils
{
  enum StatMode {
    NORMAL_MODE,
    DEFAULT_STAT_MODE,
    ZERO_DISTINCT_MODE,
    ONE_DISTINCT_MODE,
    MUTEX_MODE,
    HISTOGRAM_MODE,
  };
  static const uint8_t YEAR_MAX_VALUE = 10;
public:
  TestOptEstSel();
  virtual ~TestOptEstSel();
  virtual void SetUp();
  virtual void TearDown();
  void before_process(const char *sql_str, ObDMLStmt *&dml_stmt, ObLogPlan *&logical_plan, StatMode mode, bool no_predicate);
  void process_basic(const char *expr_str, std::ofstream &of_result);
  void process_special(const char *expr_str, std::ofstream &of_result);
  void process_string(const char *expr_str, std::ofstream &of_result);
  void process_date(const char *expr_str, std::ofstream &of_result);
  void process_default_stat(const char *sql_str, std::ofstream &of_result);
  void process_zero_distinct(const char *sql_str, std::ofstream &of_result);
  void process_one_distinct(const char *sql_str, std::ofstream &of_result);
  void process_mutex(const char *sql_str, std::ofstream &of_result);
  void process_join(const char *sql_str, std::ofstream &of_result);
  void process_hist(const char *sql_str, std::ofstream &of_result);
  void get_log_plan(ObStmt &stmt, ObLogPlan *&plan);
  void run_test(const char *test_file, const char *result_file, const char *tmp_file, int64_t flag);
protected:
  ObOptimizerContext *optctx_;
  ObAddr addr_;
  ObGlobalHint global_hint_;
  bool is_datetime_;
  oceanbase::sql::ObSchemaChecker schema_checker_;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestOptEstSel);
};

TestOptEstSel::TestOptEstSel()
  : optctx_(NULL)
{
  is_datetime_ = false;
  memcpy(schema_file_path_, "./opt_est/test_opt_est_sel.schema", sizeof("./opt_est/test_opt_est_sel.schema"));
}

TestOptEstSel::~TestOptEstSel()
{

}
void TestOptEstSel::SetUp()
{
  int ret = OB_SUCCESS;
  TestOptimizerUtils::SetUp();
  exec_ctx_.get_sql_ctx()->session_info_ = &session_info_;
  optctx_ = new ObOptimizerContext(&session_info_,
                                   &exec_ctx_,
                                   //schema_mgr_,
                                   &sql_schema_guard_,
                                   &stat_manager_,
                                   NULL,
                                   allocator_,
                                   &part_cache_,
                                   &param_list_,
                                   addr_,
                                   NULL,
                                   global_hint_,
                                   expr_factory_,
                                   NULL,
                                   false,
                                   stmt_factory_.get_query_ctx());

  ObTableLocation table_location;
  if (OB_FAIL(optctx_->get_table_location_list().push_back(table_location))) {
    LOG_WARN("failed to set table location", K(ret));
  }
}

void TestOptEstSel::TearDown()
{
  destroy();
}

void TestOptEstSel::run_test(const char *test_file,
    const char *result_file,
    const char *tmp_file,
    int64_t flag)
{
  std::ofstream of_result(tmp_file);
  ASSERT_TRUE(of_result.is_open());
  std::ifstream if_test(test_file);
  ASSERT_TRUE(if_test.is_open());
  std::string line;
  std::string total_line;
  while(std::getline(if_test, line)) {
    if (line.size() <= 0) continue;
    std::size_t begin = line.find_first_not_of('\t');
    if (line.at(begin) == '#') continue;
    std::size_t end = line.find_last_not_of('\t');
    std::string cur_line = line.substr(begin, end - begin + 1);
    switch (flag)
    {
    case 1: process_basic(cur_line.c_str(), of_result); break;
    case 2: process_special(cur_line.c_str(), of_result); break;
    case 3: process_string(cur_line.c_str(), of_result); break;
    case 4: process_date(cur_line.c_str(), of_result); break;
    case 5: process_default_stat(cur_line.c_str(), of_result); break;
    case 6: process_zero_distinct(cur_line.c_str(), of_result); break;
    case 7: process_one_distinct(cur_line.c_str(), of_result); break;
    case 8: process_mutex(cur_line.c_str(), of_result); break;
    case 9: process_join(cur_line.c_str(), of_result); break;
    case 10: process_hist(cur_line.c_str(), of_result); break;
    default: break;
    }
    cur_line = "";
  }
  if_test.close();
  of_result.close();
  std::cout << "diff -u " << tmp_file << " " << result_file << std::endl;
  TestSqlUtils::is_equal_content(tmp_file, result_file);
}

void TestOptEstSel::get_log_plan(ObStmt &stmt, ObLogPlan *&plan)
{
  ASSERT_TRUE(NULL != stmt.get_query_ctx());
  global_hint_.merge_global_hint(stmt.get_query_ctx()->get_global_hint());
  optctx_->set_root_stmt(dynamic_cast<ObDMLStmt*>(&stmt));
  // local address
  optctx_->set_local_server_addr("1.1.1.1", 8888);
  optctx_->get_all_exprs().reuse();

  ObOptimizer optimizer(*optctx_);
  optctx_->get_loc_rel_infos().reuse();
  OK(optimizer.optimize(dynamic_cast<ObDMLStmt&>(stmt), plan));
}

void TestOptEstSel::before_process(const char *sql_str, ObDMLStmt *&dml_stmt, ObLogPlan *&logical_plan, StatMode mode, bool no_predicate)
{
  MockStatManager *stat_manager = static_cast<MockStatManager *>(optctx_->get_stat_manager());
  ObObj min;
  ObObj max;
  opt_stat_.no_use_histogram();
  optctx_->set_opt_stat_manager(NULL);
  if (DEFAULT_STAT_MODE == mode) {
    min.set_min_value();
    max.set_max_value();
    stat_manager->set_column_stat(0, min, max, 0);
    stat_manager->set_table_stat(0, 0);
  } else if (ZERO_DISTINCT_MODE == mode) {
    min.set_min_value();
    max.set_max_value();
    stat_manager->set_column_stat(0, min, max, 10000);
    stat_manager->set_table_stat(10000, 10000);
  } else if (ONE_DISTINCT_MODE == mode) {
    min.set_int(1000);
    max.set_int(1000);
    stat_manager->set_column_stat(1, min, max, 3000);
    stat_manager->set_table_stat(10000, 10000);
  } else if (MUTEX_MODE == mode) {
    min.set_int(1);
    max.set_int(5);
    stat_manager->set_column_stat(5, min, max, 5);
    stat_manager->set_table_stat(10, 1000);
  } else if (HISTOGRAM_MODE == mode) {
    min.set_int(1);
    max.set_int(20);
    stat_manager->set_column_stat(20, min, max, 50);
    stat_manager->set_table_stat(250, 250);

    opt_stat_.use_histogram();
    optctx_->set_opt_stat_manager(&opt_stat_manager_);
  } else {
    min.set_int(1001);
    max.set_int(10000);
    stat_manager->set_column_stat(1000, min, max, 500);
    stat_manager->set_table_stat(10000, 2000);
  }
  param_list_.reset();

  ObStmt *stmt = NULL;
  // resolver does not generate questionmark expr, selectivity changes.
  // ONLY affect UT, observer is okay. Tracked by separated isssue
  //
  do_resolve(sql_str, stmt, false, JSON_FORMAT, OB_SUCCESS, false);

  dml_stmt = static_cast<ObDMLStmt *>(stmt);
  OB_LOG(INFO, "process stmt", "sql", sql_str);
  ObArray<ObRawExpr*> filter;
  if (no_predicate) {
    append(filter, dml_stmt->get_condition_exprs());
    dml_stmt->get_condition_exprs().reset();
  }
  get_log_plan(*stmt, logical_plan);
  schema_checker_.init(sql_schema_guard_);
  stat_manager->set_schema_checker(&schema_checker_);
  ASSERT_TRUE(NULL != (logical_plan));
  if (no_predicate) {
    append(dml_stmt->get_condition_exprs(), filter);
  }
}

void TestOptEstSel::process_basic(const char *sql_str, std::ofstream &of_result)
{

  if (++case_id_ % 2) {
    of_result << "******************CASE" <<case_id_/2<<"******************"<<std::endl;
    of_result << "min = 1001, max = 10000, ndv = 1000, null_num = 500"<< std::endl;
    of_result << "the ratio of not null row is (10000-500)/10000 = 0.95"<< std::endl;
    of_result << "for range cond : if half_open min_selectivity = 20/DNV, else min_selectivity = 10/DNV"<< std::endl;
    of_result << "EXPECTED RESULT :" << sql_str << std::endl;
    of_result << "----------------------------------------------------------" <<std::endl;
  } else {
    of_result << "EXPR_STR: "<< sql_str << std::endl;
    double selectivity = 0;
    ObLogPlan *logical_plan = NULL;
    ObDMLStmt *dml_stmt = NULL;
    before_process(sql_str, dml_stmt, logical_plan, NORMAL_MODE, true);

    ASSERT_TRUE(NULL != (dml_stmt));
    ASSERT_TRUE(NULL != (logical_plan));
    ObIArray<ObRawExpr*> &condition_exprs = dml_stmt->get_condition_exprs();
    ObOptSelectivity::calculate_selectivity(logical_plan->get_basic_table_metas(),
                                            logical_plan->get_selectivity_ctx(),
                                            condition_exprs,
                                            selectivity,
                                            logical_plan->get_predicate_selectivities());
    of_result << "SELECTIVITY = "<< selectivity << std::endl;
    of_result <<std::endl;
    // deconstruct
    expr_factory_.destory();
    stmt_factory_.destory();
    log_plan_factory_.destroy();
  }
}

// TODO(@linsheng): merge this to merge join
void TestOptEstSel::process_special(const char *sql_str, std::ofstream &of_result)
{
  of_result << "EXPR_STR: "<< sql_str << std::endl;

  ObLogPlan *logical_plan = NULL;
  ObDMLStmt *dml_stmt = NULL;
  before_process(sql_str, dml_stmt, logical_plan, NORMAL_MODE, true);

  ASSERT_TRUE(NULL != (dml_stmt));
  ASSERT_TRUE(NULL != (logical_plan));
  ObIArray<ObRawExpr*> &condition_exprs = dml_stmt->get_condition_exprs();
  double t1_sel = 1.0;
  double t2_sel = 1.0;
  ObOptSelectivity::calculate_selectivity(logical_plan->get_basic_table_metas(),
                                          logical_plan->get_selectivity_ctx(),
                                          condition_exprs, t1_sel,
                                          logical_plan->get_predicate_selectivities());
  ObOptSelectivity::calculate_selectivity(logical_plan->get_basic_table_metas(),
                                          logical_plan->get_selectivity_ctx(),
                                          condition_exprs, t2_sel,
                                          logical_plan->get_predicate_selectivities());
  of_result << "table1 SELECTIVITY = "<< t1_sel << std::endl;
  of_result << "table2 SELECTIVITY = "<< t2_sel << std::endl;
  of_result <<std::endl;
  // deconstruct
  expr_factory_.destory();
  stmt_factory_.destory();
  log_plan_factory_.destroy();
}

void TestOptEstSel::process_string(const char *sql_str, std::ofstream &of_result)
{
  if (strcmp(sql_str, "datetime") == 0) {
    is_datetime_ = true;
  } else {
    of_result << "EXPR_STR: "<< sql_str << std::endl;

    double selectivity = 0;
    ObLogPlan *logical_plan = NULL;
    ObDMLStmt *dml_stmt = NULL;
    before_process(sql_str, dml_stmt, logical_plan, NORMAL_MODE, true);
    MockStatManager *stat_manager = static_cast<MockStatManager *>(optctx_->get_stat_manager());
    ObObj min;
    ObObj max;
    if (is_datetime_) {
      min.set_varchar("10000101000000111");
      max.set_varchar("99991231235959999");
    } else {
      min.set_varchar("");
      max.set_varchar("||||||||||||||||||||");
    }
    stat_manager->set_column_stat(1000, min, max, 500);
    ASSERT_TRUE(NULL != (dml_stmt));
    ASSERT_TRUE(NULL != (logical_plan));
    ObIArray<ObRawExpr*> &condition_exprs = dml_stmt->get_condition_exprs();
    ObOptSelectivity::calculate_selectivity(logical_plan->get_basic_table_metas(),
                                            logical_plan->get_selectivity_ctx(),
                                            condition_exprs, selectivity,
                                            logical_plan->get_predicate_selectivities());
    of_result << "SELECTIVITY = "<< selectivity << std::endl;
    of_result <<std::endl;
    // deconstruct
    expr_factory_.destory();
    stmt_factory_.destory();
    log_plan_factory_.destroy();
  }
}

void TestOptEstSel::process_date(const char *sql_str, std::ofstream &of_result)
{
  of_result << "EXPR_STR: "<< sql_str << std::endl;

  double selectivity = 0;
  ObLogPlan *logical_plan = NULL;
  ObDMLStmt *dml_stmt = NULL;
  before_process(sql_str, dml_stmt, logical_plan, NORMAL_MODE, true);
  ASSERT_TRUE(NULL != (dml_stmt));
  ASSERT_TRUE(NULL != (logical_plan));
  ObIArray<ObRawExpr*> &condition_exprs = dml_stmt->get_condition_exprs();
  ObOptSelectivity::calculate_selectivity(logical_plan->get_basic_table_metas(),
                                          logical_plan->get_selectivity_ctx(),
                                          condition_exprs, selectivity,
                                          logical_plan->get_predicate_selectivities());
  of_result << "SELECTIVITY = "<< selectivity << std::endl;
  of_result <<std::endl;

  expr_factory_.destory();
  stmt_factory_.destory();
  log_plan_factory_.destroy();
}

void TestOptEstSel::process_default_stat(const char *sql_str, std::ofstream &of_result)
{

  if (++case_id_ % 2) {
    of_result << "******************CASE" <<case_id_/2<<"******************"<<std::endl;
    of_result << "min = MIN, max = MAX, ndv = 500, null_num = 10"<< std::endl;
    of_result << "the ratio of not null row is (1000-10)/1000 = 0.99"<< std::endl;
    of_result << "EXPECTED RESULT :" << sql_str << std::endl;
    of_result << "----------------------------------------------------------" <<std::endl;
  } else {
    of_result << "EXPR_STR: "<< sql_str << std::endl;
    double selectivity = 0;
    ObLogPlan *logical_plan = NULL;
    ObDMLStmt *dml_stmt = NULL;
    before_process(sql_str, dml_stmt, logical_plan, DEFAULT_STAT_MODE, true);
    ASSERT_TRUE(NULL != (dml_stmt));
    ASSERT_TRUE(NULL != (logical_plan));
    ObIArray<ObRawExpr*> &condition_exprs = dml_stmt->get_condition_exprs();
    ObOptSelectivity::calculate_selectivity(logical_plan->get_basic_table_metas(),
                                            logical_plan->get_selectivity_ctx(),
                                            condition_exprs, selectivity,
                                            logical_plan->get_predicate_selectivities());
    of_result << "SELECTIVITY = "<< selectivity << std::endl;
    of_result <<std::endl;
    // deconstruct
    expr_factory_.destory();
    stmt_factory_.destory();
    log_plan_factory_.destroy();
  }
}

void TestOptEstSel::process_zero_distinct(const char *sql_str, std::ofstream &of_result)
{

  if (++case_id_ % 2) {
    of_result << "******************CASE" <<case_id_/2<<"******************"<<std::endl;
    of_result << "min = MIN, max = MAX, ndv = 0, null_num = 10000"<< std::endl;
    of_result << "the ratio of not null row is 0.0"<< std::endl;
    of_result << "EXPECTED RESULT :" << sql_str << std::endl;
    of_result << "----------------------------------------------------------" <<std::endl;
  } else {
    of_result << "EXPR_STR: "<< sql_str << std::endl;
    double selectivity = 0;
    ObLogPlan *logical_plan = NULL;
    ObDMLStmt *dml_stmt = NULL;
    before_process(sql_str, dml_stmt, logical_plan, ZERO_DISTINCT_MODE, true);

    ASSERT_TRUE(NULL != (dml_stmt));
    ASSERT_TRUE(NULL != (logical_plan));
    ObIArray<ObRawExpr*> &condition_exprs = dml_stmt->get_condition_exprs();
    ObOptSelectivity::calculate_selectivity(logical_plan->get_basic_table_metas(),
                                            logical_plan->get_selectivity_ctx(),
                                            condition_exprs, selectivity,
                                            logical_plan->get_predicate_selectivities());
    of_result << "SELECTIVITY = "<< selectivity << std::endl;
    of_result <<std::endl;
    // deconstruct
    expr_factory_.destory();
    stmt_factory_.destory();
    log_plan_factory_.destroy();
  }
}

void TestOptEstSel::process_one_distinct(const char *sql_str, std::ofstream &of_result)
{

  if (++case_id_ % 2) {
    of_result << "******************CASE" <<case_id_/2<<"******************"<<std::endl;
    of_result << "min = 1000, max = 1000, ndv = 1, null_num = 3000"<< std::endl;
    of_result << "the ratio of not null row is (10000-3000)/10000 = 0.7"<< std::endl;
    of_result << "for range cond : if half_open min_selectivity = 20/DNV,"
                 " else min_selectivity = 10/DNV"<< std::endl;
    of_result << "EXPECTED RESULT :" << sql_str << std::endl;
    of_result << "----------------------------------------------------------" <<std::endl;
  } else {
    of_result << "EXPR_STR: "<< sql_str << std::endl;
    double selectivity = 0;
    ObLogPlan *logical_plan = NULL;
    ObDMLStmt *dml_stmt = NULL;
    before_process(sql_str, dml_stmt, logical_plan, ONE_DISTINCT_MODE, true);

    ASSERT_TRUE(NULL != (dml_stmt));
    ASSERT_TRUE(NULL != (logical_plan));
    ObIArray<ObRawExpr*> &condition_exprs = dml_stmt->get_condition_exprs();
    ObOptSelectivity::calculate_selectivity(logical_plan->get_basic_table_metas(),
                                            logical_plan->get_selectivity_ctx(),
                                            condition_exprs, selectivity,
                                            logical_plan->get_predicate_selectivities());
    of_result << "SELECTIVITY = "<< selectivity << std::endl;
    of_result <<std::endl;
    // deconstruct
    expr_factory_.destory();
    stmt_factory_.destory();
    log_plan_factory_.destroy();
  }
}

void TestOptEstSel::process_mutex(const char *sql_str, std::ofstream &of_result)
{

  if (++case_id_ % 2) {
    of_result << "******************CASE" <<case_id_/2<<"******************"<<std::endl;
    of_result << "min = 1, max = 5, ndv = 5, null_num = 5"<< std::endl;
    of_result << "the ratio of not null row is (10-5)/10 = 0.5"<< std::endl;
    of_result << "for range cond : if half_open min_selectivity = 20/DNV, else min_selectivity = 10/DNV"<< std::endl;
    of_result << "EXPECTED RESULT :" << sql_str << std::endl;
    of_result << "----------------------------------------------------------" <<std::endl;
  } else {
    of_result << "EXPR_STR: "<< sql_str << std::endl;
    double selectivity = 0;
    ObLogPlan *logical_plan = NULL;
    ObDMLStmt *dml_stmt = NULL;
    before_process(sql_str, dml_stmt, logical_plan, MUTEX_MODE, true);

    ASSERT_TRUE(NULL != (dml_stmt));
    ASSERT_TRUE(NULL != (logical_plan));
    ObIArray<ObRawExpr*> &condition_exprs = dml_stmt->get_condition_exprs();
    ObOptSelectivity::calculate_selectivity(logical_plan->get_basic_table_metas(),
                                            logical_plan->get_selectivity_ctx(),
                                            condition_exprs, selectivity,
                                            logical_plan->get_predicate_selectivities());
    of_result << "SELECTIVITY = "<< selectivity << std::endl;
    of_result <<std::endl;
    // deconstruct
    expr_factory_.destory();
    stmt_factory_.destory();
    log_plan_factory_.destroy();
  }
}

void TestOptEstSel::process_join(const char *sql_str, std::ofstream &of_result)
{
  ObResultSet result(session_info_, allocator_);
  of_result << "***************   Case "<< ++case_id_ << "   ***************" << std::endl;
  of_result << "min = 1001, max = 10000, ndv = 1000, null_num = 500"<< std::endl;
  of_result << "t1 row count = 10000, t2 row count = 2000" << std::endl;
  of_result << "t1 not null frac is (10000-500)/10000 = 0.95"<< std::endl;
  of_result << "t2 not null frac is (2000-500)/2000 = 0.75"<< std::endl;
  of_result << std::endl;
  //ObArenaAllocator mempool(ObModIds::OB_SQL_COMPILE, OB_MALLOC_NORMAL_BLOCK_SIZE);

  ObString sql = ObString::make_string(sql_str);
  of_result << "SQL: " << sql_str << std::endl;
  LOG_INFO("Case query", K_(case_id), K(sql_str));

  ObLogPlan *logical_plan = NULL;
  ObDMLStmt *dml_stmt = NULL;

  before_process(sql_str, dml_stmt, logical_plan, NORMAL_MODE, false);
  ASSERT_TRUE(NULL != (dml_stmt));
  ASSERT_TRUE(NULL != (logical_plan));

  of_result << std::endl;
  char buf[BUF_LEN];
  logical_plan->to_string(buf, BUF_LEN, explain_type_);
  //printf("%s\n", buf);
  of_result << buf << std::endl;

  of_result << "*************** Case "<< case_id_ << "(end)  ************** " << std::endl;
  of_result << std::endl;

  // deconstruct
  expr_factory_.destory();
  stmt_factory_.destory();
  //  log_plan_factory_.destroy();
}

//TEST_F(TestOptEstSel, basic_test)
//{
//  const char* test_file = "./opt_est/test_sel_basic.sql";
//  const char* result_file = "./opt_est/test_sel_basic.result";
//  const char* tmp_file = "./opt_est/test_sel_basic.tmp";
//  run_test(test_file, result_file, tmp_file, 1);
//}

//TEST_F(TestOptEstSel, special_test)
//{
//  const char* test_file = "./opt_est/test_sel_special.sql";
//  const char* result_file = "./opt_est/test_sel_special.result";
//  const char* tmp_file = "./opt_est/test_sel_special.tmp";
//  run_test(test_file, result_file, tmp_file, 2);
//}
void TestOptEstSel::process_hist(const char *sql_str, std::ofstream &of_result)
{
  if (++case_id_ % 2) {
    of_result << "******************CASE" <<case_id_/2<<"******************"<<std::endl;
    of_result << "min = 1, max = 20, ndv = 20, null_num = 50" << std::endl;
    of_result << "num_row = 250, density = 0.0025, bucket count = 200" << std::endl;
    of_result << "the ratio of not null row is (250-50)/250 = 0.8" << std::endl;
    of_result << "| val | 1 | 2  | 3  | 4  | 5  | 6  | 7  | 8  | 9   | 10  | 11  | 12  | 13  | 14  | 15  | 16  | 17  | 18  | 19  | 20  |" << std::endl;
    of_result << "| cnt | 5 | 14 | 13 | 16 | 9  | 7  | 10 | 13 | 15  | 1   | 5   | 6   | 10  | 9   | 9   | 12  | 21  | 11  | 11  | 3   |" << std::endl;
    of_result << "| acc | 5 | 19 | 32 | 48 | 57 | 64 | 74 | 87 | 102 | 103 | 108 | 114 | 124 | 133 | 142 | 154 | 175 | 186 | 197 | 200 |" << std::endl;
    of_result << "EXPECTED RESULT :" << sql_str << std::endl;
    of_result << "----------------------------------------------------------" <<std::endl;
    of_result << std::endl;
  } else {
    of_result << "EXPR_STR: "<< sql_str << std::endl;
    double selectivity = 0;
    ObLogPlan *logical_plan = NULL;
    ObDMLStmt *dml_stmt = NULL;
    before_process(sql_str, dml_stmt, logical_plan, HISTOGRAM_MODE, true);

    ASSERT_TRUE(NULL != (dml_stmt));
    ASSERT_TRUE(NULL != (logical_plan));
    ObIArray<ObRawExpr*> &condition_exprs = dml_stmt->get_condition_exprs();
    ObOptSelectivity::calculate_selectivity(logical_plan->get_basic_table_metas(),
                                            logical_plan->get_selectivity_ctx(),
                                            condition_exprs, selectivity,
                                            logical_plan->get_predicate_selectivities());
    of_result << "SELECTIVITY = "<< selectivity << std::endl;
    of_result <<std::endl;
    // deconstruct
    expr_factory_.destory();
    stmt_factory_.destory();
    log_plan_factory_.destroy();
  }
}

TEST_F(TestOptEstSel, string_test)
{
  const char* test_file = "./opt_est/test_sel_str.sql";
  const char* result_file = "./opt_est/test_sel_str.result";
  const char* tmp_file = "./opt_est/test_sel_str.tmp";
  run_test(test_file, result_file, tmp_file, 3);
}

TEST_F(TestOptEstSel, date_test)
{
  const char* test_file = "./opt_est/test_sel_date.sql";
  const char* result_file = "./opt_est/test_sel_date.result";
  const char* tmp_file = "./opt_est/test_sel_date.tmp";
  std::ofstream of_result(tmp_file);
  ASSERT_TRUE(of_result.is_open());
  run_test(test_file, result_file, tmp_file, 4);
}

// TODO(yuanzhe) enable this after remove ob_partition_service.cpp
// TEST_F(TestOptEstSel, default_est_test)
// {
//   const char* test_file = "./opt_est/test_sel_default_stat.sql";
//   const char* result_file = "./opt_est/test_sel_default_stat.result";
//   const char* tmp_file = "./opt_est/test_sel_default_stat.tmp";
//   std::ofstream of_result(tmp_file);
//   ASSERT_TRUE(of_result.is_open());
//   run_test(test_file, result_file, tmp_file, 5);
// }

//TEST_F(TestOptEstSel, zero_distinct_est_test)
//{
//  const char* test_file = "./opt_est/test_sel_zero_distinct.sql";
//  const char* result_file = "./opt_est/test_sel_zero_distinct.result";
//  const char* tmp_file = "./opt_est/test_sel_zero_distinct.tmp";
//  std::ofstream of_result(tmp_file);
//  ASSERT_TRUE(of_result.is_open());
//  run_test(test_file, result_file, tmp_file, 6);
//}

//TEST_F(TestOptEstSel, one_distinct_est_test)
//{
//  const char* test_file = "./opt_est/test_sel_one_distinct.sql";
//  const char* result_file = "./opt_est/test_sel_one_distinct.result";
//  const char* tmp_file = "./opt_est/test_sel_one_distinct.tmp";
//  std::ofstream of_result(tmp_file);
//  ASSERT_TRUE(of_result.is_open());
//  run_test(test_file, result_file, tmp_file, 7);
//}

TEST_F(TestOptEstSel, mutex_est_test)
{
  const char* test_file = "./opt_est/test_sel_mutex.sql";
  const char* result_file = "./opt_est/test_sel_mutex.result";
  const char* tmp_file = "./opt_est/test_sel_mutex.tmp";
  std::ofstream of_result(tmp_file);
  ASSERT_TRUE(of_result.is_open());
  run_test(test_file, result_file, tmp_file, 8);
}

//TEST_F(TestOptEstSel, row_count_test)
//{
//  const char* test_file = "./opt_est/test_row_count.sql";
//  const char* result_file = "./opt_est/test_row_count.result";
//  const char* tmp_file = "./opt_est/test_row_count.tmp";
//  std::ofstream of_result(tmp_file);
//  ASSERT_TRUE(of_result.is_open());
//  run_test(test_file, result_file, tmp_file, 9);
//}

TEST_F(TestOptEstSel, histogram)
{
  const char* test_file = "./opt_est/test_sel_hist.sql";
  const char* result_file = "./opt_est/test_sel_hist.result";
  const char* tmp_file = "./opt_est/test_sel_hist.tmp";
  std::ofstream of_result(tmp_file);
  ASSERT_TRUE(of_result.is_open());
  int64_t infos[][3] = {
    {0, 1, 5},
    {0, 2, 14},
    {0, 3, 13},
    {0, 4, 16},
    {0, 5, 9},
    {0, 6, 7},
    {0, 7, 10},
    {0, 8, 13},
    {0, 9, 15},
    {0, 10, 1},
    {0, 11, 5},
    {0, 12, 6},
    {0, 13, 10},
    {0, 14, 9},
    {0, 15, 9},
    {0, 16, 12},
    {0, 17, 21},
    {0, 18, 11},
    {0, 19, 11},
    {0, 20, 3}};

  common::ObSEArray<int64_t, 20> repeat_count;
  common::ObSEArray<int64_t, 20> value;
  common::ObSEArray<int64_t, 20> num_elements;
  for (int64_t i = 0; i < ARRAYSIZEOF(infos); i++) {
    repeat_count.push_back(infos[i][0]);
    value.push_back(infos[i][1]);
    num_elements.push_back(infos[i][2]);
  }

  init_histogram(allocator_, ObHistType::FREQUENCY, 100, 0.0025,
                 repeat_count, value, num_elements, opt_stat_.get_histogram());

  run_test(test_file, result_file, tmp_file, 10);
}

} // namespace test

int main(int argc, char **argv)
{
  system("rm -rf test_opt_est_sel.log");
  observer::ObReqTimeGuard req_timeinfo_guard;
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_opt_est_sel.log", true);
  init_sql_factories();
  ::testing::InitGoogleTest(&argc,argv);
  if(argc >= 2)
  {
    if (strcmp("DEBUG", argv[1]) == 0
        || strcmp("WARN", argv[1]) == 0)
    OB_LOGGER.set_log_level(argv[1]);
  }
  return RUN_ALL_TESTS();
}
