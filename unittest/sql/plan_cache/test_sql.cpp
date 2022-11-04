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
#include "test_sql.h"
#include "sql/plan_cache/ob_cache_object_factory.h"
#include "../optimizer/ob_mock_opt_stat_manager.h"
using std::cout;
using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::json;
using namespace oceanbase::share::schema;
using namespace oceanbase::obrpc;

namespace test
{

TestSQL::TestSQL(const ObString &schema_file_name)
    : addr_(ObAddr::IPV4, "1.1.1.1", 8888),
    merged_version_(OB_MERGED_VERSION_INIT)
{
  memset(schema_file_path_, '\0', 128);
  memcpy(schema_file_path_, schema_file_name.ptr(), schema_file_name.length());
}

TestSQL::~TestSQL()
{
  destroy();
}

int TestSQL::do_parse(ObIAllocator &allocator, const char* query_str, ParseResult &parse_result)
{
  int ret = OB_SUCCESS;
  ObSQLMode mode = SMO_DEFAULT;
  ObParser parser(allocator, mode);
  ObString query = ObString::make_string(query_str);
  if (OB_FAIL(parser.parse(query, parse_result))) {
    SQL_PC_LOG(WARN, "fail to parse query", K(ret));
  }
  return ret;
}

int TestSQL::do_resolve(TestSqlCtx &test_sql_ctx, ParseResult &parse_result, ObStmt *&stmt, ParamStore *params)
{
  int ret = OB_SUCCESS;
  ObSchemaChecker schema_checker;
  //if (OB_ISNULL(schema_mgr_)) {
  //  ret = OB_ERR_UNEXPECTED;
  //  LOG_WARN("schema_mgr_ is NULL", K(ret));
  //} else
  if (OB_FAIL(schema_checker.init(sql_schema_guard_))) {
    LOG_WARN("fail to init schema_checker", K(ret));
  } else {

    ObResolverParams resolver_ctx;
    resolver_ctx.allocator_  = test_sql_ctx.allocator_;
    resolver_ctx.schema_checker_ = &schema_checker;
    resolver_ctx.session_info_ = &session_info_;
    resolver_ctx.database_id_ = get_database_id();
    resolver_ctx.param_list_ = params;
    resolver_ctx.expr_factory_ = test_sql_ctx.expr_factory_;
    resolver_ctx.stmt_factory_ = test_sql_ctx.stmt_factory_;
    resolver_ctx.query_ctx_ = test_sql_ctx.stmt_factory_->get_query_ctx();
    ObResolver resolver(resolver_ctx);
    if (OB_FAIL(resolver.resolve(ObResolver::IS_NOT_PREPARED_STMT, *parse_result.result_tree_->children_[0], stmt))) {
      SQL_PC_LOG(WARN, "fail to resolve", K(ret));
    }
  }
  return ret;
}

int TestSQL::generate_logical_plan(TestSqlCtx &test_sql_ctx,
                                   ParamStore *params,
                                   ObStmt *stmt,
                                   ObLogPlan *&logical_plan)
{
  int ret = OB_SUCCESS;
  ObQueryHint query_hint = dynamic_cast<ObDMLStmt*>(stmt)->get_stmt_hint().get_query_hint();
  exec_ctx_.get_sql_ctx()->session_info_ = &session_info_;
  ObOptimizerContext *optctx = new ObOptimizerContext(&session_info_,
                                                      &exec_ctx_,
                                                      //schema_mgr_, // schema manager
                                                      &sql_schema_guard_, //schema guard
                                                      &stat_manager_, // statistics manager
                                                      NULL,
                                                      &partition_service_,
                                                      *test_sql_ctx.allocator_,
                                                      params,
                                                      addr_,
                                                      NULL,
                                                      query_hint,
                                                      *test_sql_ctx.expr_factory_,
                                                      dynamic_cast<ObDMLStmt*>(stmt),
                                                      false,
                                                      stmt_factory_.get_query_ctx());
  //  ObTableLocation table_location;
  //  ret = optctx.get_table_location_list().push_back(table_location);
  //  LOG_DEBUG("setting local address to 1.1.1.1");
  //  optctx.set_local_server_addr("1.1.1.1", 8888);
  ObOptimizer optimizer(*optctx);
  if (OB_FAIL(optimizer.optimize(*dynamic_cast<ObDMLStmt*>(stmt), logical_plan))) {
    SQL_PC_LOG(WARN, "fail to generate logical plan", K(ret));
  }
  return ret;
}

int TestSQL::generate_physical_plan(ObLogPlan *logical_plan, ObPhysicalPlan *&physical_plan)
{
  int ret = OB_SUCCESS;
  ObCodeGeneratorImpl code_generator(CLUSTER_VERSION_1500);
  ObCacheObjectFactory::alloc(physical_plan);
  SQL_PC_LOG(INFO, "alloc physical plan", K(physical_plan));
  if (NULL == physical_plan) {
    ret = OB_ERR_UNEXPECTED;
    SQL_PC_LOG(WARN, "fail to alloc physical plan", K(ret));
  } else if (OB_FAIL(code_generator.generate(*logical_plan, *physical_plan))) {
    SQL_PC_LOG(WARN, "fail to generate physical plan", K(ret));
    ObCacheObjectFactory::free(physical_plan);
  }
  return ret;
}

}
