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

#define USING_LOG_PREFIX SQL_REWRITE

#include <stdlib.h>
#include <iostream>
#include <gtest/gtest.h>
#include <sys/time.h>
#include "lib/utility/ob_test_util.h"
#include "lib/json/ob_json.h"
#include "common/ob_common_utility.h"
#include "sql/ob_sql_init.h"
#include "../test_sql_utils.h"
#include "sql/parser/ob_parser.h"
#include "sql/resolver/ob_schema_checker.h"
#include "sql/rewrite/ob_transform_simplify_distinct.h"
#include "sql/rewrite/ob_transform_simplify_expr.h"
#include "sql/rewrite/ob_transform_simplify_groupby.h"
#include "sql/rewrite/ob_transform_simplify_subquery.h"
#include "sql/rewrite/ob_transform_simplify_winfunc.h"
#include "sql/rewrite/ob_transform_simplify_orderby.h"
#include "sql/rewrite/ob_transform_simplify_limit.h"
#include "sql/rewrite/ob_transform_view_merge.h"
#include "sql/rewrite/ob_transform_where_subquery_pullup.h"
#include "sql/rewrite/ob_transform_eliminate_outer_join.h"
#include "sql/rewrite/ob_transformer_impl.h"
#include "sql/rewrite/ob_transform_query_push_down.h"
#include "sql/rewrite/ob_transform_min_max.h"
#include "../optimizer/test_optimizer_utils.h"
#include "observer/ob_req_time_service.h"
//#include "../rewrite/test_trans_utils.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::obrpc;
using oceanbase::sql::ObTableLocation;
using namespace oceanbase::json;
namespace test {

/* update the result file
 *             TEMP FILe                                  RESULT FILE
 *cp  test_:
 *cp  test_transformer_plan_after_el.tmp           test_transformer_plan_after_el.result
 *cp  test_transformer_plan_after_or_expand.tmp    test_transformer_plan_after_or_expand.result
 *cp  test_transformer_plan_after_where_pullup.tmp test_transformer_plan_after_where_pullup.result
 *cp  test_transform_impl_after_plan.tmp           test_transform_impl_after_plan.result
 *
 *
 *
 * */



//test::TestTransUtils::CmdLineParam param;

class TestRewrite: public TestOptimizerUtils {
public:
  TestRewrite();
  virtual ~TestRewrite();private:
  void virtual SetUp();
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestRewrite);
protected:
  // function members
  int parse_resolve_transform(std::ofstream &of_result,
      ObTransformerImpl &trans_util, ObString &sql, ObDMLStmt *&stmt,
      ObLogPlan *& logical_plan,
      bool do_transform, bool do_gen_plan, ObIAllocator *allocator);
  void transform_sql(ObTransformerImpl &trans_util, const char* query_str,
      std::ofstream &of_result, ObDMLStmt *& stmt, ObLogPlan *& logical_plan,
      bool do_transform,
      bool do_gen_plan,
      ObIAllocator *allocator);
  void run_test(ObTransformerImpl &trans_util, const char* test_file, const char* stmt_result_file,
                const char* stmt_after_trans_file, const char* stmt_no_trans_file,
                const char* plan_result_file, const char* plan_after_tans_file,
                const char* plan_no_trans_file,
                bool print_sql_in_file = true,
                //ExplainType type = EXPLAIN_EXTENDED);
                ExplainType type = EXPLAIN_TRADITIONAL,
                bool always_rewrite = false);
  int gen_stmt_and_plan(ObTransformerImpl &trans_util,
                        const char* query_str,
                        ObDMLStmt *&stmt,
                        ObLogPlan *&logical_plan,
                        bool do_transform,
                        bool do_gen_plan);
  void multi_thread_test();
  void test_from_cmd(uint64_t rules);
  int parse_resolve(
      ObString &query_str,
      ObDMLStmt *&stmt);
  int transform(
      ObTransformerImpl &trans_util, ObDMLStmt *&stmt);
  int optimize(
      ObDMLStmt *&stmt,
      ObLogPlan *&logical_plan);
  bool do_loop_test;
protected:
  common::ObAddr local_addr_;
  ObTransformerCtx ctx_;
  ObSchemaChecker schema_checker_;
};

class TestStackCheck :public TestOptimizerUtils
{
public:
  TestStackCheck(){
    memset(schema_file_path_, '\0', 128);
    memcpy(schema_file_path_, "./schema.sql", strlen("./schema.sql"));
  }
  virtual ~TestStackCheck(){}
};


//#define tmptest

TestRewrite::TestRewrite() {
  local_addr_.set_ip_addr("1.1.1.1", 8888);
  ctx_.allocator_ = &allocator_;
  ctx_.exec_ctx_ = &exec_ctx_;
  ctx_.expr_factory_ = &expr_factory_;
  ctx_.stmt_factory_ = &stmt_factory_;
  ctx_.partition_location_cache_ = &part_cache_;
  ctx_.stat_mgr_ = &stat_manager_;
  ctx_.partition_service_ = &partition_service_;
  ctx_.sql_schema_guard_ = &sql_schema_guard_;
  ctx_.self_addr_ = &local_addr_;
  ctx_.merged_version_ = OB_MERGED_VERSION_INIT;
  memset(schema_file_path_, '\0', 128);
  memcpy(schema_file_path_, "./schema.sql", strlen("./schema.sql"));
  do_loop_test = false;
}

void TestRewrite::SetUp()
{
  init();
  int ret = OB_SUCCESS;
  if (OB_FAIL(schema_checker_.init(sql_schema_guard_))) {
    LOG_ERROR("fail to init schema_checker", K(ret));
  } else {
    session_info_.set_user_session();
    ctx_.schema_checker_ = &schema_checker_;
    ctx_.session_info_ = &session_info_;
    exec_ctx_.get_task_executor_ctx()->set_min_cluster_version(CLUSTER_VERSION_2100);
    LOG_INFO("set min cluster version", K(CLUSTER_VERSION_2100));
  }
}

TestRewrite::~TestRewrite() {
}


int TestRewrite::parse_resolve_transform(std::ofstream &of_result,
    ObTransformerImpl &trans_util, ObString &query_str, ObDMLStmt *&stmt,
    ObLogPlan *&logical_plan,
    bool do_transform,
    bool do_gen_plan,
    ObIAllocator *allocator) {
  OB_ASSERT(NULL != allocator);
  int ret = OB_SUCCESS;
  //
  ParamStore &param_store = exec_ctx_.get_physical_plan_ctx()->get_param_store_for_update();
  param_store.reuse();
  ObSQLMode mode = SMO_DEFAULT;
  ObParser parser(*allocator, mode);
  ParseResult parse_result;
  int64_t time_1 = get_usec();
  int64_t time_2 = 0;
  int64_t time_3 = 0;
  int64_t time_4 = 0;
  int64_t time_5 = 0;
  /**
   *  2. set resolver context
   */
  /**
   *  2. set resolver context
   */
  ObSchemaChecker schema_checker;
  //if (OB_ISNULL(schema_mgr_)) {
  //  ret = OB_ERR_UNEXPECTED;
  //  LOG_WARN("schema_mgr_ is NULL", K(ret));
  if (OB_FAIL(schema_checker.init(sql_schema_guard_))) {
    LOG_WARN("fail to init schema_checker", K(ret));
  } else {
    /**
     *  3. set resolver context
     */
    ObResolverParams resolver_ctx;
    resolver_ctx.allocator_ = allocator;
    resolver_ctx.schema_checker_ = &schema_checker;
    resolver_ctx.session_info_ = &session_info_;
    resolver_ctx.database_id_ = 1024;
    resolver_ctx.param_list_ = &param_store;
    resolver_ctx.expr_factory_ = &expr_factory_;
    resolver_ctx.stmt_factory_ = &stmt_factory_;
    resolver_ctx.query_ctx_ = stmt_factory_.get_query_ctx();

    LOG_INFO("begin parse sql", K(query_str));
    ObAddr addr;
    LOG_DEBUG("setting local address to 1.1.1.1");
    addr.set_ip_addr("1.1.1.1", 8888);

    time_1 = get_usec();
    if (OB_FAIL(parser.parse(query_str, parse_result))) {
      LOG_WARN("failed to parse", K(ret));
    } else {
      time_2 = get_usec();
      SqlInfo not_param_info;
      ParseNode *root = parse_result.result_tree_->children_[0];
      ObMaxConcurrentParam::FixParamStore fixed_param_store;
      bool is_transform_outline = false;
      if (T_SELECT == root->type_ || T_INSERT == root->type_ || T_UPDATE == root->type_ || T_DELETE == root->type_) {
        if (OB_FAIL(ObSqlParameterization::transform_syntax_tree(allocator_,
                                                                 session_info_,
                                                                 NULL,
                                                                 parse_result.result_tree_,
                                                                 not_param_info,
                                                                 param_store,
                                                                 NULL,
                                                                 fixed_param_store,
                                                                 is_transform_outline))) {
          LOG_WARN("failed to parameterized", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      resolver_ctx.query_ctx_->question_marks_count_ = param_store.count();
      ObResolver resolver(resolver_ctx);

      ObStmt *stmt_tree = NULL;
      if (OB_FAIL(resolver.resolve(ObResolver::IS_NOT_PREPARED_STMT,
                                   *parse_result.result_tree_->children_[0],
                                   stmt_tree))) {
        LOG_WARN("failed to resolve", K(ret));
      } else {
        time_3 = get_usec();
        if (NULL == (stmt = dynamic_cast<ObDMLStmt *>(stmt_tree))) {
          _LOG_WARN("Generate stmt error, query=%.*s", parse_result.input_sql_len_, parse_result.input_sql_);
          ret = OB_ERR_UNEXPECTED;
        } else {
          stmt->get_query_ctx()->set_sql_stmt(parse_result.input_sql_, parse_result.input_sql_len_);
          LOG_INFO("Generate stmt success", "query", stmt->get_query_ctx()->get_sql_stmt());
        }
      }
      /*
       * Transform different util use different transform util
       */
      if (OB_SUCC(ret)) {
        if (do_transform) {
          if (OB_ISNULL(trans_util.get_trans_ctx())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpect null ctx", K(ret));
          } else if (OB_FALSE_IT(trans_util.get_trans_ctx()->ignore_semi_infos_.reset())) {
          } else if (OB_SUCCESS != (ret = trans_util.transform(stmt))) {
            LOG_WARN("failed to do transform", K(ret));
          } else if (OB_FAIL(stmt->formalize_stmt(&session_info_))) {
            LOG_WARN("failed to formalize stmt", K(ret));
          } else {
            time_4 = get_usec();
            //_OB_LOG(INFO, "%s", CSJ(*stmt));
            stmt_tree = static_cast<ObStmt*>(stmt);
            OB_ASSERT(NULL != stmt_tree);
            OB_ASSERT((static_cast<ObStmt*>(stmt) == stmt_tree));
            _OB_LOG(INFO, "finish to try to transform");
          }
        }
        LOG_DEBUG("stmt to optimize", K(do_transform), K(*stmt));
        of_result << CSJ(*stmt) << std::endl;

        if (OB_SUCC(ret)) {
          if (do_gen_plan) {
            ObDMLStmt *dml_stmt = static_cast<ObDMLStmt *>(stmt_tree);
            ObPhysicalPlanCtx *pctx = exec_ctx_.get_physical_plan_ctx();
            if (NULL == pctx) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("physical plan ctx is NULL", K(ret));
            } else if (NULL != dml_stmt) {
              ObOptimizerContext *ctx_ptr =
                  static_cast<ObOptimizerContext *>(allocator_.alloc(sizeof(ObOptimizerContext)));
              exec_ctx_.get_sql_ctx()->session_info_ = &session_info_;
              optctx_ = new(ctx_ptr) ObOptimizerContext(&session_info_,
                  &exec_ctx_,
                  &sql_schema_guard_,
                  &stat_manager_, // statistics manager
                  NULL,
                  &partition_service_,
                  static_cast<ObIAllocator &>(allocator_),
                  &part_cache_,
                  &param_store,
                  addr,
                  NULL,
                  dml_stmt->get_query_ctx()->get_global_hint(),
                  expr_factory_,
                  dml_stmt,
                  false,
                  dml_stmt->get_query_ctx());
              ObTableLocation table_location;
              ret = optctx_->get_table_location_list().push_back(table_location);
              LOG_INFO("setting local address to 1.1.1.1");
              optctx_->set_local_server_addr("1.1.1.1", 8888);
              ObOptimizer optimizer(*optctx_);

              if (OB_FAIL(optimizer.optimize(*dml_stmt, logical_plan))) {
                LOG_WARN("failed to optimize", "SQL", stmt);
              } else {
                time_5 = get_usec();
                LOG_DEBUG("succ to generate logical plan");
              }
            }
          }
        } else {
          logical_plan = NULL;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    std::cout << "<<<<<<<<<<<<<<<<<<<<<<<<<<<<" << std::endl;
    std::cout << "SQL: " << query_str.ptr() << std::endl;
    std::cout << "parse:     " << time_2 - time_1 << std::endl;
    std::cout << "resolve:   " << time_3 - time_2 << std::endl;
    if (do_transform) {
      std::cout << "transform:   " << time_4 - time_3 << std::endl;
    }
    if (do_gen_plan) {
      std::cout << "optimize:  " << time_5 - time_4 << std::endl;
    }
    std::cout << ">>>>>>>>>>>>>>>>>>>>>>>>>>>>" << std::endl;
  }
  return ret;
}

//new
int TestRewrite::parse_resolve(
    ObString &query_str,
    ObDMLStmt *&stmt) {
  int ret = OB_SUCCESS;
  //
  ParamStore &param_store = exec_ctx_.get_physical_plan_ctx()->get_param_store_for_update();
  param_store.reuse();
  ObSQLMode mode = SMO_DEFAULT;
  ObParser parser(allocator_, mode);
  ParseResult parse_result;
  /**
   *  2. set resolver context
   */
  /**
   *  2. set resolver context
   */
  ObSchemaChecker schema_checker;
  //if (OB_ISNULL(schema_mgr_)) {
  //  ret = OB_ERR_UNEXPECTED;
  //  LOG_WARN("schema_mgr_ is NULL", K(ret));
  if (OB_FAIL(schema_checker.init(sql_schema_guard_))) {
    LOG_WARN("fail to init schema_checker", K(ret));
  } else {
    /**
     *  3. set resolver context
     */
    ObResolverParams resolver_ctx;
    resolver_ctx.allocator_ = &allocator_;
    resolver_ctx.schema_checker_ = &schema_checker;
    resolver_ctx.session_info_ = &session_info_;
    resolver_ctx.database_id_ = 1024;
    resolver_ctx.param_list_ = &param_store;
    resolver_ctx.expr_factory_ = &expr_factory_;
    resolver_ctx.stmt_factory_ = &stmt_factory_;
    resolver_ctx.query_ctx_ = stmt_factory_.get_query_ctx();
    LOG_INFO("begin parse sql", K(query_str));

    if (OB_FAIL(parser.parse(query_str, parse_result))) {
      LOG_WARN("failed to parse", K(ret));
    } else {
      SqlInfo not_param_info;
      ParseNode *root = parse_result.result_tree_->children_[0];
      ObMaxConcurrentParam::FixParamStore fixed_param_store(OB_MALLOC_NORMAL_BLOCK_SIZE,
                                                            ObWrapperAllocator(&allocator_));
      bool is_transform_outline = false;
      if (T_SELECT == root->type_ || T_INSERT == root->type_ || T_UPDATE == root->type_ || T_DELETE == root->type_) {
        if (OB_FAIL(ObSqlParameterization::transform_syntax_tree(allocator_,
                                                                 session_info_,
                                                                 NULL,
                                                                 parse_result.result_tree_,
                                                                 not_param_info,
                                                                 param_store,
                                                                 NULL,
                                                                 fixed_param_store,
                                                                 is_transform_outline))) {
          LOG_WARN("failed to parameterized", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      resolver_ctx.query_ctx_->question_marks_count_ = param_store.count();
      ObResolver resolver(resolver_ctx);

      ObStmt *stmt_tree = NULL;
      if (OB_FAIL(resolver.resolve(ObResolver::IS_NOT_PREPARED_STMT,
                                   *parse_result.result_tree_->children_[0],
                                   stmt_tree))) {
        LOG_WARN("failed to resolve", K(ret));
      } else {
        if (NULL == (stmt = dynamic_cast<ObDMLStmt *>(stmt_tree))) {
          _LOG_WARN("Generate stmt error, query=%.*s", parse_result.input_sql_len_, parse_result.input_sql_);
          ret = OB_ERR_UNEXPECTED;
        } else {
          stmt->get_query_ctx()->set_sql_stmt(parse_result.input_sql_, parse_result.input_sql_len_);
          LOG_INFO("Generate stmt success", "query", stmt->get_query_ctx()->get_sql_stmt());
        }
      }
    }
  }
  return ret;
}

int TestRewrite::transform(
    ObTransformerImpl &trans_util, ObDMLStmt *&stmt) {
  int ret = OB_SUCCESS;
  /*
   * Transform different util use different transform util
   */
  if (OB_SUCC(ret)) {
    //std::cout << "begin to do transform------" << std::endl;
    exec_ctx_.get_sql_ctx()->session_info_ = &session_info_;
    if (OB_ISNULL(trans_util.get_trans_ctx())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null ctx", K(ret));
    } else if (OB_FALSE_IT(trans_util.get_trans_ctx()->reset())) {
    } else if (OB_SUCCESS != (ret = trans_util.transform(stmt))) {
      LOG_WARN("failed to do transform", K(ret));
    } else if (OB_FAIL(stmt->formalize_stmt(&session_info_))) {
      LOG_WARN("failed to formalize stmt", K(ret));
    }
  }

  return ret;
}

int TestRewrite::optimize(
    ObDMLStmt *&stmt,
    ObLogPlan *&logical_plan) {
  int ret = OB_SUCCESS;

  ObDMLStmt *dml_stmt = static_cast<ObDMLStmt *>(stmt);
  ObPhysicalPlanCtx *pctx = exec_ctx_.get_physical_plan_ctx();
  if (NULL == pctx) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("physical plan ctx is NULL", K(ret));
  } else if (NULL != dml_stmt) {
    ObAddr addr;
    addr.set_ip_addr("1.1.1.1", 8888);
    ParamStore &param_store = exec_ctx_.get_physical_plan_ctx()->get_param_store_for_update();
    ObOptimizerContext *ctx_ptr =
        static_cast<ObOptimizerContext *>(allocator_.alloc(sizeof(ObOptimizerContext)));
    exec_ctx_.get_sql_ctx()->session_info_ = &session_info_;
    optctx_ = new(ctx_ptr) ObOptimizerContext(&session_info_,
        &exec_ctx_,
        &sql_schema_guard_,
        &stat_manager_, // statistics manager
        NULL,
        &partition_service_,
        static_cast<ObIAllocator &>(allocator_),
        &part_cache_,
        &param_store,
        addr,
        NULL,
        dml_stmt->get_query_ctx()->get_global_hint(),
        expr_factory_,
        dml_stmt,
        false,
        stmt_factory_.get_query_ctx());
    // note that we can't mock a table location when applying cost based transformation. Without 
    // that, estimating cost of plan will use default statistics. And this may cause a situation 
    // that the lower cost plan is different when use default statistics and mock statistics.
    ObTableLocation table_location;
    ret = optctx_->get_table_location_list().push_back(table_location);
    LOG_INFO("setting local address to 1.1.1.1");
    optctx_->set_local_server_addr("1.1.1.1", 8888);
    ObOptimizer optimizer(*optctx_);

    if (OB_FAIL(optimizer.optimize(*dml_stmt, logical_plan))) {
      LOG_WARN("failed to optimize", "SQL", stmt);
    }
  }
  return ret;
}

int TestRewrite::gen_stmt_and_plan(ObTransformerImpl &trans_util,
                                   const char* query_str,
                                   ObDMLStmt *&stmt,
                                   ObLogPlan *&logical_plan,
                                   bool do_transform,
                                   bool do_gen_plan) {
  ObString sql = ObString::make_string(query_str);
  int ret = parse_resolve(sql, stmt);
  if (OB_SUCC(ret) && do_transform) {
    ret = transform(trans_util, stmt);
  }
  if (OB_SUCC(ret) && do_gen_plan) {
    ret = optimize(stmt, logical_plan);
  }
  if (OB_FAIL(ret)) {
    LOG_INFO("FAILED SQL", K(query_str));
  }
  return ret;
}


void TestRewrite::transform_sql(ObTransformerImpl &trans_util,
                                const char* query_str, std::ofstream &of_result, ObDMLStmt *& stmt,
                                ObLogPlan *& logical_plan,
                                bool do_transform,
                                bool do_gen_plan,
                                ObIAllocator *allocator) {
  OB_ASSERT(NULL != allocator);
  of_result << "***************   Case " << case_id_ << "   ***************"
      << std::endl;
  of_result << std::endl;
  ObString sql = ObString::make_string(query_str);
  of_result << "SQL: " << query_str << std::endl;
  int ret = parse_resolve_transform(of_result, trans_util, sql, stmt,
      logical_plan, do_transform, do_gen_plan, allocator);
  if (ret != OB_SUCCESS) {
    LOG_INFO("SQL", K(query_str));
  }
  ASSERT_EQ(OB_SUCCESS, ret);
}

#define T(query)                                      \
do {                                                  \
  transform_sql(query, of_result, of_expect);         \
} while(0)

//params
//trans_util            : which rule to apply to the sql
//test_file             : input sql file
//stmt_after_trans_file : stmt_result_file after transform after this run
//stmt_no_trans_file    : stmt_no_trans_file no transform after this run
//plan_result_file      : logical_plan after transform result file
//plan_after_tans_file  : logical_plan after transform result file this run
//plan_no_trans_file    : logical_plan no transform after this run
//print_sql_in_file     : print sql in the plan file or not, default true
//                        sometimes we may need to consider the two plans
//                        completely the same generated by the complex and simple sql
//                        thus compare the two plan file by str.eg or_expand test
//every run turn, we do generate two plans, a complex sql needed to transform,
//a simple sql that equals the complex, we print the two plans to compare so
//we will see the result of transform and debug.
void TestRewrite::run_test(ObTransformerImpl &trans_util,
                           const char* test_file,
                           const char* stmt_result_file,
                           const char* stmt_after_trans_file,
                           const char* stmt_no_trans_file,
                           const char* plan_result_file,
                           const char* plan_after_tans_file,
                           const char* plan_no_trans_file,
                           bool print_sql_in_file,
                           ExplainType type/*EXPLAIN_TRADITIONAL*/,
                           bool always_rewrite) {
  UNUSED(print_sql_in_file);
  std::ofstream of_stmt_after_trans(stmt_after_trans_file);
  ASSERT_TRUE(of_stmt_after_trans.is_open());
  std::ofstream of_stmt_no_trans(stmt_no_trans_file);
  ASSERT_TRUE(of_stmt_no_trans.is_open());
  std::ofstream of_plan_after_trans(plan_after_tans_file);
  ASSERT_TRUE(of_plan_after_trans.is_open());
  std::ofstream of_plan_no_trans(plan_no_trans_file);
  ASSERT_TRUE(of_plan_no_trans.is_open());
  std::ifstream if_tests(test_file);
  ASSERT_TRUE(if_tests.is_open());
  std::string line;
  std::string total_line;
  bool do_generate_plan = true;
  ObSchemaChecker schema_checker;
  ASSERT_EQ(OB_SUCCESS, schema_checker.init(sql_schema_guard_));
  stat_manager_.set_schema_checker(&schema_checker);
  while (std::getline(if_tests, line)) {
    // allow human readable formatting
    if (line.size() <= 0)
      continue;
    std::size_t begin = line.find_first_not_of('\t');
    if (line.at(begin) == '#')
      continue;
    std::size_t end = line.find_last_not_of('\t');
    std::string exact_line = line.substr(begin, end - begin + 1);
    total_line += exact_line;
    total_line += " ";
    if (exact_line.at(exact_line.length() - 1) != ';')
      continue;
    else {
      _OB_LOG(INFO, "case number is %lu", case_id_);
      _OB_LOG(INFO, "query_str is %s", total_line.c_str());
      for (int i = 0; i < 2; ++i) {
        bool do_trans = 1 == i % 2;
        ObDMLStmt *stmt = NULL;
        ObLogPlan *logical_plan = NULL;

        ObString sql = ObString::make_string(total_line.c_str());
        //parser & resolve
        ASSERT_EQ(OB_SUCCESS, parse_resolve(sql, stmt));
        //rewerite
        if (do_trans || always_rewrite) {
          SQL_LOG(INFO, "before rewrite", "query", CSJ(*stmt));
          ASSERT_EQ(OB_SUCCESS, transform(trans_util, stmt));
          SQL_LOG(INFO, "after rewrite", "query", CSJ(*stmt));
        }
        // to file and diff
        std::ofstream *stmt_stream = !do_trans ? &of_stmt_no_trans : &of_stmt_after_trans;
        *stmt_stream << "***************   Case " << case_id_ << "   ***************" << std::endl;
        *stmt_stream << "SQL: " << total_line << std::endl;
        *stmt_stream << CSJ(*stmt) << std::endl;
        stmt_stream->flush();
        //optimize
        if (do_generate_plan) {
          ASSERT_EQ(OB_SUCCESS, optimize(stmt, logical_plan));
        }
        // std::cout << "SQL: " << total_line << std::endl;
        // std::cout << "stmt: " << CSJ(stmt) << std::endl;
        char buf[BUF_LEN];
        memset(buf, '\0', BUF_LEN);
        if (do_generate_plan) {
          logical_plan->to_string(buf, BUF_LEN, type);
          // std::cout << "plan: " << buf << std::endl;
        }

        if (do_generate_plan) {
          std::ofstream *plan_stream = !do_trans ? &of_plan_no_trans : &of_plan_after_trans;
          *plan_stream << "***************   Case "<< case_id_ << "   ***************" << std::endl;
          *plan_stream << "SQL: " << total_line << std::endl;
          *plan_stream << buf << std::endl;
          plan_stream->flush();
        }

        //release the memory allocated
        stmt_factory_.destory();
        expr_factory_.destory();
        log_plan_factory_.destroy();
        ctx_.expr_constraints_.reuse();
      }
      ++case_id_;
      total_line = "";
    }
  }

  std::cout << "diff -u " << plan_after_tans_file << " " << plan_result_file << std::endl;
  is_equal_content(plan_after_tans_file, plan_result_file);
  std::cout << "diff -u " << stmt_after_trans_file << " " <<stmt_result_file << std::endl;
  is_equal_content(stmt_after_trans_file, stmt_result_file);

}

//TEST_F(TestRewrite, test_from_cmd)
//{
//  if (param.use_it_) {
//    test_from_cmd(param.rules_);
//    exit(0);
//  }
//}
/* todo:@linjing: 由于修改的result文件太大，超过了单次CR 50M的文件上限，合入后放开测试例
TEST_F(TestRewrite, transform_simply)
{
  // result has the transformed stmt and plan
  const char* test_file = "./test_transformer_simplify.sql";
  const char* stmt_after_trans_simplify_result_file = "./result/test_transformer_stmt_after_trans_simplify.result";
  const char* stmt_after_trans_simplify_file = "./result/test_transformer_stmt_after_trans_simplify.tmp";
  const char* stmt_no_trans_simplify_file = "./tmp/test_transformer_stmt_no_trans_simplify.tmp";
  const char* plan_after_trans_simplify_result_file = "./result/test_transformer_plan_after_trans_simplify.result";
  const char* plan_after_trans_simplify_file = "./result/test_transformer_plan_after_trans_simplify.tmp";
  const char* plan_no_trans_simplify_file = "./tmp/test_transformer_plan_no_trans_simplify.tmp";

  int ret = OB_SUCCESS;

  ObPhysicalPlan *phy_plan = NULL;
  if (OB_FAIL(ObCacheObjectFactory::alloc(
              phy_plan, session_info_.get_effective_tenant_id()))) {
  } else if (OB_ISNULL(phy_plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Failed to allocate phy plan", K(ret));
  } else {
    ctx_.phy_plan_ = phy_plan;

    ObTransformerImpl trans(&ctx_);
    trans.clear_needed_types();
    trans.add_needed_types(SIMPLIFY_DISTINCT);
    trans.add_needed_types(SIMPLIFY_EXPR);
    trans.add_needed_types(SIMPLIFY_GROUPBY);
    trans.add_needed_types(SIMPLIFY_LIMIT);
    trans.add_needed_types(SIMPLIFY_ORDERBY);
    trans.add_needed_types(SIMPLIFY_SUBQUERY);
    trans.add_needed_types(SIMPLIFY_WINFUNC);


    run_test(trans, test_file, stmt_after_trans_simplify_result_file,
             stmt_after_trans_simplify_file, stmt_no_trans_simplify_file,
             plan_after_trans_simplify_result_file, plan_after_trans_simplify_file,
             plan_no_trans_simplify_file, true, EXPLAIN_TRADITIONAL, true);
  }
  OK(ret);
}
*/

/* todo:@linjing: 由于修改的result文件太大，超过了单次CR 50M的文件上限，合入后放开测试例
TEST_F(TestRewrite, transform_set_op)
{
  // result has the transformed stmt and plan
  const char* test_file = "./test_transformer_set_op.sql";
  const char* stmt_after_trans_set_op_result_file = "./result/test_transformer_stmt_after_trans_set_op.result";
  const char* stmt_after_trans_set_op_file = "./result/test_transformer_stmt_after_trans_set_op.tmp";
  const char* stmt_no_trans_set_op_file = "./tmp/test_transformer_stmt_no_trans_set_op.tmp";
  const char* plan_after_trans_set_op_result_file = "./result/test_transformer_plan_after_trans_set_op.result";
  const char* plan_after_trans_set_op_file = "./result/test_transformer_plan_after_trans_set_op.tmp";
  const char* plan_no_trans_set_op_file = "./tmp/test_transformer_plan_no_trans_set_op.tmp";
  int ret = OB_SUCCESS;

  ObPhysicalPlan *phy_plan = NULL;
  if (OB_FAIL(ObCacheObjectFactory::alloc(
              phy_plan, session_info_.get_effective_tenant_id()))) {
  } else if (OB_ISNULL(phy_plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Failed to allocate phy plan", K(ret));
  } else {
    ctx_.phy_plan_ = phy_plan;

    ObTransformerImpl trans(&ctx_);
    trans.clear_needed_types();
    trans.add_needed_types(SIMPLIFY_DISTINCT);
    trans.add_needed_types(SIMPLIFY_EXPR);
    trans.add_needed_types(SIMPLIFY_GROUPBY);
    trans.add_needed_types(SIMPLIFY_LIMIT);
    trans.add_needed_types(SIMPLIFY_ORDERBY);
    trans.add_needed_types(SIMPLIFY_SUBQUERY);
    trans.add_needed_types(SIMPLIFY_WINFUNC);
    trans.add_needed_types(SIMPLIFY_SET);

    run_test(trans, test_file, stmt_after_trans_set_op_result_file,
             stmt_after_trans_set_op_file, stmt_no_trans_set_op_file,
             plan_after_trans_set_op_result_file, plan_after_trans_set_op_file,
             plan_no_trans_set_op_file, true, EXPLAIN_TRADITIONAL, true);
  }
  OK(ret);
}
*/

#ifndef tmptest

/* todo:@linjing: 由于修改的result文件太大，超过了单次CR 50M的文件上限，合入后放开测试例
TEST_F(TestRewrite, transform_aggregate)
{
  // result has the transformed stmt and plan
  const char* test_file = "./test_transformer_aggregate.sql";
  const char* stmt_after_trans_aggr_result = "./result/test_transformer_stmt_after_trans_aggr.result";
  const char* stmt_after_trans_aggr_file = "./result/test_transformer_stmt_after_trans_aggr.tmp";
  const char* stmt_no_trans_aggr_file = "./tmp/test_transformer_stmt_no_trans_aggr.tmp";
  const char* plan_after_trans_aggr_result_file = "./result/test_transformer_plan_after_trans_aggr.result";
  const char* plan_after_trans_aggr_file = "./result/test_transformer_plan_after_trans_aggr.tmp";
  const char* plan_no_trans_aggr_file = "./tmp/test_transformer_plan_no_trans_aggr.tmp";
  int ret = OB_SUCCESS;

  ObPhysicalPlan *phy_plan = NULL;
  if (OB_FAIL(ObCacheObjectFactory::alloc(
              phy_plan, session_info_.get_effective_tenant_id()))) {
  } else if (OB_ISNULL(phy_plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Failed to allocate phy plan", K(ret));
  } else {
    ctx_.phy_plan_ = phy_plan;

    ObTransformerImpl trans(&ctx_);
    run_test(trans, test_file,stmt_after_trans_aggr_result,
             stmt_after_trans_aggr_file, stmt_no_trans_aggr_file,
             plan_after_trans_aggr_result_file, plan_after_trans_aggr_file,
             plan_no_trans_aggr_file, true, EXPLAIN_TRADITIONAL);
  }
  OK(ret);
}
*/

//TEST_F(TestRewrite, view_merge) {
//  // result has the transformed stmt and plan
//  const char* test_file = "test_transformer_vm.sql";
//  const char* stmt_merge_file = "./tmp/test_transformer_stmt_after_merge.tmp";
//  const char* stmt_no_merge_file = "./tmp/test_transformer_stmt_no_merge.tmp";
//  const char* plan_result_file = "./result/test_transformer_plan_after_view_merge.result";
//  const char* plan_after_merge_file = "./result/test_transformer_plan_after_merge.tmp";
//  const char* plan_no_merge_file = "./tmp/test_transformer_plan_no_merge.tmp";
//  //do elimination and view merge together
//  //ObTransformerImpl trans_all;
//
//  ObTransformerImpl trans(&ctx_);
//  trans.clear_needed_type();
//  trans.set_type_needed(SIMPLIFY);
//  trans.set_type_needed(VIEW_MERGE);
//
//  run_test(trans, test_file, stmt_merge_file, stmt_no_merge_file,
//      plan_result_file, plan_after_merge_file, plan_no_merge_file);
//}

/* todo:@linjing: 由于修改的result文件太大，超过了单次CR 50M的文件上限，合入后放开测试例
TEST_F(TestRewrite, query_push_down) {
  // result has the transformed stmt and plan
  const char* test_file = "test_transformer_query_push_down.sql";
  const char* stmt_merge_result_file = "./result/test_transformer_stmt_after_query_push_down.result";
  const char* stmt_merge_file = "./result/test_transformer_stmt_after_query_push_down.tmp";
  const char* stmt_no_merge_file = "./tmp/test_transformer_stmt_no_query_push_down.tmp";
  const char* plan_result_file = "./result/test_transformer_plan_after_query_push_down.result";
  const char* plan_after_merge_file = "./result/test_transformer_plan_after_query_push_down.tmp";
  const char* plan_no_merge_file = "./tmp/test_transformer_plan_no_query_push_down.tmp";
  //do elimination and view merge together
  //ObTransformerImpl trans_all;
  int ret = OB_SUCCESS;

  ObPhysicalPlan *phy_plan = NULL;
  if (OB_FAIL(ObCacheObjectFactory::alloc(
              phy_plan, session_info_.get_effective_tenant_id()))) {
  } else if (OB_ISNULL(phy_plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Failed to allocate phy plan", K(ret));
  } else {
    ctx_.phy_plan_ = phy_plan;

    ObTransformerImpl trans(&ctx_);
    trans.clear_needed_types();
    trans.add_needed_types(SIMPLIFY_DISTINCT);
    trans.add_needed_types(SIMPLIFY_EXPR);
    trans.add_needed_types(SIMPLIFY_GROUPBY);
    trans.add_needed_types(SIMPLIFY_LIMIT);
    trans.add_needed_types(SIMPLIFY_ORDERBY);
    trans.add_needed_types(SIMPLIFY_SUBQUERY);
    trans.add_needed_types(SIMPLIFY_WINFUNC);
    trans.add_needed_types(QUERY_PUSH_DOWN);
    run_test(trans, test_file, stmt_merge_result_file, stmt_merge_file, stmt_no_merge_file,
             plan_result_file, plan_after_merge_file, plan_no_merge_file, true, EXPLAIN_TRADITIONAL);
  }
  OK(ret);
}
*/

/*
static int pure_recusive_test(int64_t level, int64_t reserved_size = 10000000)
{
  int ret = OB_SUCCESS;
  if (level < 1000) {
    bool is_stack_overflow = false;
    if (OB_FAIL(check_stack_overflow(is_stack_overflow, reserved_size))) {
      LOG_WARN("failed to check", K(ret));
    } else if (is_stack_overflow) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("stack overflow", K(ret), K(is_stack_overflow), K(level));
    } else {
      int64_t cur_level = level + 1;
      char stack_mem[10 * 1000]; //test 10k/level stack mem
      stack_mem[5000] = 'a';
      UNUSED(stack_mem);
      ret = pure_recusive_test(cur_level, reserved_size);
    }
  } else {
    int64_t cur_level = level + 1;
    LOG_INFO("leaf node quit", K(cur_level));
  }
  return ret;
}

TEST_F(TestStackCheck, test_stack_check)
{
  const char* test_file = "./test_stack_check.sql";
  size_t stack_size = 0;
  pthread_attr_t attr;
  void *stack_start = NULL;
  OB_ASSERT(0 == pthread_getattr_np(pthread_self(), &attr));
  OB_ASSERT(0 == pthread_attr_getstack(&attr, &stack_start, &stack_size));
  LOG_INFO("stack size", K(stack_size));
  int64_t original_reserved_stack_size = get_reserved_stack_size();
  int64_t reserved_size = (static_cast<int64_t>(stack_size)) - 80 * 1000;
  set_reserved_stack_size(reserved_size); //set stack limit to 80kb = 10485760 - 10405760;
  run_fail_test_ret_as(test_file, OB_SIZE_OVERFLOW);
  int sret = pure_recusive_test(1, reserved_size);
  LOG_INFO("pure recursive output ", K(sret));
  OB_ASSERT(OB_SIZE_OVERFLOW == sret);
  set_reserved_stack_size(original_reserved_stack_size);
}*/

/*
TEST_F(TestStackCheck, test_resursive_level_check)
{
  const char* test_file = "./test_stack_level_check.sql";
  int64 original_level = ObTransformer::get_max_recursive_level();
  ObTransformer::set_max_recursive_level(10);
  run_fail_test_ret_as(test_file, OB_NOT_SUPPORTED);
  ObTransformer::set_max_recursive_level(original_level);
} */

/* todo:@linjing: 由于修改的result文件太大，超过了单次CR 50M的文件上限，合入后放开测试例
TEST_F(TestRewrite, eliminate_outer_join)
{
  const char* test_file = "./el.sql";
  const char* stmt_el_result_file = "./result/test_transformer_stmt_after_el.result";
  const char* stmt_el_file = "./result/test_transformer_stmt_after_el.tmp";
  const char* stmt_no_el_file = "./tmp/test_transformer_stmt_no_el.tmp";
  const char* plan_result_file = "./result/test_transformer_plan_after_el.result";
  const char* plan_after_el_file = "./result/test_transformer_plan_after_el.tmp";
  const char* plan_no_el_file = "./tmp/test_transformer_plan_no_el.tmp";

  int ret = OB_SUCCESS;

  ObPhysicalPlan *phy_plan = NULL;
  if (OB_FAIL(ObCacheObjectFactory::alloc(
              phy_plan, session_info_.get_effective_tenant_id()))) {
  } else if (OB_ISNULL(phy_plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Failed to allocate phy plan", K(ret));
  } else {
    ctx_.phy_plan_ = phy_plan;

    ObTransformerImpl trans(&ctx_);
    trans.clear_needed_types();
    trans.add_needed_types(SIMPLIFY_DISTINCT);
    trans.add_needed_types(SIMPLIFY_EXPR);
    trans.add_needed_types(SIMPLIFY_GROUPBY);
    trans.add_needed_types(SIMPLIFY_LIMIT);
    trans.add_needed_types(SIMPLIFY_ORDERBY);
    trans.add_needed_types(SIMPLIFY_SUBQUERY);
    trans.add_needed_types(SIMPLIFY_WINFUNC);
    trans.add_needed_types(ELIMINATE_OJ);
    //ObTransformEliminateOuterJoin el_join(&ctx_);
    run_test(trans, test_file,stmt_el_result_file,
             stmt_el_file, stmt_no_el_file,
             plan_result_file, plan_after_el_file, plan_no_el_file, true, EXPLAIN_TRADITIONAL, true);
  }
  OK(ret);
}
*/

/* todo(@ banliu.zyd): 这个测试目前没有放入基准的result文件，先注释了
TEST_F(TestRewrite, hualong)
{
  // result has the transformed stmt and plan
  const char* test_file = "./test_transformer_hualong.sql";
  const char* stmt_result_file= "./result/test_transformer_stmt_hualong.result";
  const char* stmt_do_trans_tmp_file= "./result/test_transformer_stmt_hualong.tmp";
  const char* stmt_no_trans_tmp_file= "./tmp/test_transformer_stmt_no_hualong.tmp";
  const char* plan_result_file = "./result/test_transformer_plan_hualong.result";
  const char* plan_do_trans_tmp_file= "./result/test_transformer_plan_hualong.tmp";
  const char* plan_no_trans_tmp_file= "./tmp/test_transformer_plan_no_hualong.tmp";
  ObTransformerImpl trans(&ctx_);
  trans.clear_needed_type();
  trans.set_type_needed(SIMPLIFY);
  trans.set_type_needed(WHERE_SQ_PULL_UP);
  run_test(trans, test_file,
           stmt_result_file,
           stmt_do_trans_tmp_file,
           stmt_no_trans_tmp_file,
           plan_result_file,
           plan_do_trans_tmp_file,
           plan_no_trans_tmp_file, true, EXPLAIN_TRADITIONAL);
}
*/

/* todo:@linjing: 由于修改的result文件太大，超过了单次CR 50M的文件上限，合入后放开测试例
TEST_F(TestRewrite, test_together)
{
  // test the transformimpl using outer-join elimination view merge and where pull up
  const char* test_file = "./test_transformer_impl.sql";
  const char* stmt_after_trans_result = "./result/test_transformer_stmt_after_together.result";
  const char* stmt_after_trans = "./result/test_transformer_stmt_after_together.tmp";
  const char* stmt_no_trans = "./tmp/test_transformer_stmt_no_together.tmp";
  const char* plan_result_file = "./result/test_transformer_plan_after_together.result";
  const char* plan_after_trans = "./result/test_transformer_plan_after_together.tmp";
  const char* plan_no_trans = "./tmp/test_transformer_plan_no_together.tmp";
  int ret = OB_SUCCESS;

  ObPhysicalPlan *phy_plan = NULL;
  if (OB_FAIL(ObCacheObjectFactory::alloc(
              phy_plan, session_info_.get_effective_tenant_id()))) {
  } else if (OB_ISNULL(phy_plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Failed to allocate phy plan", K(ret));
  } else {
    ctx_.phy_plan_ = phy_plan;

    ObTransformerImpl trans_all(&ctx_);
    //  run_test(trans_all, test_file, stmt_after_trans, stmt_no_trans, plan_result_file,
    //      plan_after_trans, plan_no_trans);
    if (!do_loop_test) {
      run_test(trans_all, test_file, stmt_after_trans_result, stmt_after_trans, stmt_no_trans, plan_result_file,
               plan_after_trans, plan_no_trans, true, EXPLAIN_TRADITIONAL, true);
    }
  }
}
*/

/* todo:@linjing: 由于修改的result文件太大，超过了单次CR 50M的文件上限，合入后放开测试例
TEST_F(TestRewrite, transform_outline)
{
  // result has the transformed stmt and plan
  //system("rm -rf test_transformer_outline.sql && cat *.sql > test_transformer_outline.test && mv test_transformer_outline.test test_transformer_outline.sql");
  system("cat test_transformer_subquery_pullup.sql test_transformer_vm.sql > test_transformer_outline.sql");
  const char* test_file = "./test_transformer_outline.sql";
  const char* stmt_after_trans_aggr_file_result = "./result/test_transformer_stmt_trans_outline.result";
  const char* stmt_after_trans_aggr_file = "./result/test_transformer_stmt_trans_outline.tmp";
  const char* stmt_no_trans_aggr_file = "./tmp/test_transformer_stmt_no_trans_outline.tmp";
  const char* plan_after_trans_aggr_result_file = "./result/test_transformer_plan_after_trans_outline.result";
  const char* plan_after_trans_aggr_file = "./result/test_transformer_plan_after_trans_outline.tmp";
  const char* plan_no_trans_aggr_file = "./tmp/test_transformer_plan_no_trans_outline.tmp";
  int ret = OB_SUCCESS;

  ObPhysicalPlan *phy_plan = NULL;
  if (OB_FAIL(ObCacheObjectFactory::alloc(
              phy_plan, session_info_.get_effective_tenant_id()))) {
  } else if (OB_ISNULL(phy_plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Failed to allocate phy plan", K(ret));
  } else {
    ctx_.phy_plan_ = phy_plan;

    ObTransformerImpl trans_all(&ctx_);
    run_test(trans_all, test_file,stmt_after_trans_aggr_file_result,
             stmt_after_trans_aggr_file, stmt_no_trans_aggr_file,
             plan_after_trans_aggr_result_file, plan_after_trans_aggr_file,
             plan_no_trans_aggr_file, true, EXPLAIN_OUTLINE, true);
  }
  OK(ret);
}
*/

#endif

#ifdef tmptest
// TEST_F(TestRewrite, tmp_test)
// {
//   // test the transformimpl using outer-join elimination view merge and where pull up
//   const char* test_file = "./test_transformer_temp_test.sql";
//   const char* stmt_after_trans = "./tmp/test_transformer_stmt_after_tmptest.tmp";
//   const char* stmt_no_trans = "./tmp/test_transformer_stmt_no_tmptest.tmp";
//   const char* plan_result_file = "./result/test_transformer_plan_result_tmptest.tmp";
//   const char* plan_after_trans = "./result/test_transformer_plan_after_tmptest.tmp";
//   const char* plan_no_trans = "./tmp/test_transformer_plan_no_tmptest.tmp";
//   //ObTransformViewMerge trans_all(&allocator_);
//   ObTransformerImpl trans(&ctx_);
//   trans.clear_needed_type();
//   trans.set_type_needed(SIMPLIFY);
//   trans.set_type_needed(ELIMINATE_OJ);
//   run_test(trans, test_file, stmt_after_trans, stmt_no_trans, plan_result_file,
//            plan_after_trans, plan_no_trans);

// }
#endif


/*TEST_F(TestRewrite, trans_win_magic)*/
//{
  //// result has the transformed stmt and plan
  //const char* test_file = "./test_transformer_win_magic.sql";
  //const char* stmt_win_magic_result_file = "./result/test_transformer_stmt_win_magic.result";
  //const char* stmt_win_magic_file = "./result/test_transformer_stmt_win_magic.tmp";
  //const char* stmt_no_win_magic_file = "./tmp/test_transformer_stmt_no_win_magic.tmp";
  //const char* plan_result_file = "./result/test_transformer_plan_after_win_magic.result";
  //const char* plan_after_win_magic_file = "./result/test_transformer_plan_after_win_magic.tmp";
  //const char* plan_no_win_magic_file = "./tmp/test_transformer_plan_no_win_magic.tmp";
  //ObTransformerImpl trans(&ctx_);
  //trans.clear_needed_type();
  //trans.set_type_needed(SIMPLIFY);
  //trans.set_type_needed(WIN_MAGIC);
  //run_test(trans, test_file,
           //stmt_win_magic_result_file,
           //stmt_win_magic_file,
           //stmt_no_win_magic_file,
           //plan_result_file,
           //plan_after_win_magic_file,
           //plan_no_win_magic_file,
           //true,
           //EXPLAIN_TRADITIONAL,
           //true);
/*}*/

void TestRewrite::test_from_cmd(uint64_t rules)
{
  while(true){
    int ret = OB_SUCCESS;

    ObPhysicalPlan *phy_plan = NULL;
    if (OB_FAIL(ObCacheObjectFactory::alloc(
                phy_plan, session_info_.get_effective_tenant_id()))) {
    } else if (OB_ISNULL(phy_plan)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Failed to allocate phy plan", K(ret));
    } else {
      ctx_.phy_plan_ = phy_plan;
    }

    ObTransformerImpl trans(&ctx_);
    //trans.set_needed_types(rules);
    std::string sql;
    std::cout << "Please Input SQL: \n>";
    if (getline(std::cin, sql)){
      std::cout << "SQL=>" << sql.c_str() << std::endl;
      bool do_generate_plan = true;
      int ret = OB_SUCCESS;
      for (int i = 0; i < 2 && OB_SUCC(ret); ++i) {
        bool do_trans = 1 == i % 2;
        ObDMLStmt *stmt = NULL;
        ObLogPlan *logical_plan = NULL;
        ret = gen_stmt_and_plan(trans, sql.c_str(), stmt,
            logical_plan, do_trans, do_generate_plan);
        if (OB_SUCC(ret)) {
          std::cout << "stmt: " << CSJ(stmt) << std::endl;
          if (do_generate_plan) {
            char buf[BUF_LEN];
            memset(buf, '\0', BUF_LEN);
            logical_plan->to_string(buf, BUF_LEN, EXPLAIN_TRADITIONAL);
            std::cout << "plan: " << buf << std::endl;
          }
        } else {
          std::cout << "generate failed, do_trans: " << do_trans << std::endl;
        }
//        trans.reset();
        //release the memory allocated
        stmt_factory_.destory();
        expr_factory_.destory();
        log_plan_factory_.destroy();
        ctx_.expr_constraints_.reuse();
      }
    }
  }
}

}//namespace test

static bool do_tmp_test = false;

int main(int argc, char **argv)
{
  int ret = 0;
  ContextParam param;
  param.set_mem_attr(1001, "Transformer", ObCtxIds::WORK_AREA)
    .set_page_size(OB_MALLOC_BIG_BLOCK_SIZE);

  system("rm -rf test_transformer.log");
  observer::ObReqTimeGuard req_timeinfo_guard;
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_transformer.log", true);
  ::testing::InitGoogleTest(&argc, argv);
  init_sql_factories();
  if(argc >= 2) {
    if (strcmp("DEBUG", argv[1]) == 0
        || strcmp("WARN", argv[1]) == 0
        || strcmp("INFO", argv[1]) == 0) {
      OB_LOGGER.set_log_level(argv[1]);
    } else if (strcmp("TMP", argv[1]) == 0) {
      do_tmp_test = true;
    }
  } else {
    OB_LOGGER.set_log_level("INFO");
  }
  //test::TestTransUtils::parse_cmd(argc, argv, test::param);
  //std::cout << test::param.rules_ << std::endl;

  CREATE_WITH_TEMP_CONTEXT(param) {
    ret = RUN_ALL_TESTS();
  }
  return ret;
}
