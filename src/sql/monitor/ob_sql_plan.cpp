/**
 * Copyright (c) 2023 OceanBase
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

#include "sql/optimizer/ob_logical_operator.h"
#include "sql/engine/ob_physical_plan.h"
#include "observer/ob_inner_sql_connection_pool.h"
#include "observer/ob_inner_sql_connection.h"
#include "sql/resolver/ddl/ob_explain_stmt.h"
#include "sql/optimizer/ob_log_values.h"
#include "sql/optimizer/ob_log_plan.h"
#include "sql/optimizer/ob_del_upd_log_plan.h"
#include "lib/time/Time.h"
#include "pl/sys_package/ob_dbms_xplan.h"
#include "lib/json/ob_json.h"
#include "ob_plan_info_manager.h"
#include "lib/rc/ob_rc.h"
#include "ob_sql_plan.h"
using namespace oceanbase::common;
using namespace oceanbase::json;
using namespace oceanbase::observer;

#define NEW_PLAN_STR(plan_strs)                                                   \
do {                                                                              \
  if (OB_FAIL(ret)) {                                                             \
  } else if ((OB_FAIL(plan_strs.push_back(ObString(plan_text.pos_ - last_pos,     \
                                          plan_text.buf_ + last_pos))))) {        \
    LOG_WARN("failed to push back obstring", K(ret));                             \
  } else {                                                                        \
    last_pos = plan_text.pos_;                                                    \
  }                                                                               \
} while (0);                                                                      \

static const char *ExplainColumnName[] =
{
  "ID",
  "OPERATOR",
  "NAME",
  "EST.ROWS",
  "EST.TIME(us)",
  "REAL.ROWS",
  "REAL.TIME(us)",
  "IO TIME(us)",
  "CPU TIME(us)"
};
enum ExplainColumnEnumType{
  Id=0,
  Operator,
  Name,
  EstRows,
  EstCost,
  RealRows,
  RealCost,
  IoTime,
  CpuTime,
  MaxPlanColumn
};

namespace oceanbase
{
namespace sql
{

ObSqlPlan::ObSqlPlan(common::ObIAllocator &allocator)
  :allocator_(allocator)
{
}

ObSqlPlan::~ObSqlPlan()
{
}

int ObSqlPlan::store_sql_plan(ObLogPlan* log_plan, ObPhysicalPlan* phy_plan)
{
  int ret = OB_SUCCESS;
  PlanText plan_text;
  ObSEArray<ObSqlPlanItem*, 16> sql_plan_infos;
  ObLogicalPlanRawData compress_plan;
  if (OB_ISNULL(phy_plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null phy plan", K(ret));
  } else if (OB_FAIL(init_buffer(plan_text))) {
    LOG_WARN("failed to init buffer", K(ret));
  } else if (OB_FAIL(get_sql_plan_infos(plan_text,
                                        log_plan,
                                        sql_plan_infos))) {
    LOG_WARN("failed to get sql plan infos", K(ret));
  } else if (OB_FAIL(compress_plan.compress_logical_plan(allocator_, sql_plan_infos))) {
    LOG_WARN("failed to compress logical plan", K(ret));
  } else if (phy_plan->set_logical_plan(compress_plan)) {
    LOG_WARN("failed to set logical plan", K(ret));
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to store sql plan", K(ret));
    ret = OB_SUCCESS;
  }
  destroy_buffer(plan_text);
  return ret;
}

int ObSqlPlan::store_sql_plan_for_explain(ObExecContext *ctx,
                                          ObLogPlan* plan,
                                          ExplainType type,
                                          const ObString& plan_table,
                                          const ObString& statement_id,
                                          const ObExplainDisplayOpt& option,
                                          ObIArray<common::ObString> &plan_strs)
{
  int ret = OB_SUCCESS;
  PlanText plan_text;
  PlanText out_plan_text;
  plan_text.type_ = type;
  ObSEArray<ObSqlPlanItem*, 16> sql_plan_infos;
  bool allocate_mem_failed = false;
  if (OB_FAIL(init_buffer(plan_text))) {
    LOG_WARN("failed to init buffer", K(ret));
  } else if (OB_FAIL(get_sql_plan_infos(plan_text,
                                        plan,
                                        sql_plan_infos))) {
    LOG_WARN("failed to get sql plan infos", K(ret));
  }
  allocate_mem_failed |= OB_ALLOCATE_MEMORY_FAILED == ret;
  if (OB_FAIL(format_sql_plan(sql_plan_infos,
                              type,
                              option,
                              out_plan_text))) {
    LOG_WARN("failed to format sql plan", K(ret));
  }
  allocate_mem_failed |= OB_ALLOCATE_MEMORY_FAILED == ret;
  if (OB_FAIL(plan_text_to_strings(out_plan_text, plan_strs))) {
    LOG_WARN("failed to convert plan text to strings", K(ret));
  } else if (OB_FAIL(inner_store_sql_plan_for_explain(ctx,
                                                      plan_table,
                                                      statement_id,
                                                      sql_plan_infos))) {
    LOG_WARN("failed to store explain plan", K(ret));
    if (plan_table.compare("PLAN_TABLE") == 0) {
      //ignore error for default
      ret = OB_SUCCESS;
    }
  }
  if (OB_SUCC(ret)) {
    if (allocate_mem_failed) {
      if (OB_FAIL(plan_strs.push_back("Plan truncated due to insufficient memory!"))) {
        LOG_WARN("failed to push back string", K(ret));
      }
    } else if (plan_strs.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to generate plan", K(ret));
    }
  }
  destroy_buffer(plan_text);
  return ret;
}

int ObSqlPlan::print_sql_plan(ObLogPlan* plan,
                              ExplainType type,
                              const ObExplainDisplayOpt& option,
                              ObIArray<common::ObString> &plan_strs)
{
  int ret = OB_SUCCESS;
  PlanText plan_text;
  PlanText out_plan_text;
  plan_text.type_ = type;
  ObSEArray<ObSqlPlanItem*, 16> sql_plan_infos;
  if (OB_FAIL(init_buffer(plan_text))) {
    LOG_WARN("failed to init buffer", K(ret));
  } else if (OB_FAIL(get_sql_plan_infos(plan_text,
                                        plan,
                                        sql_plan_infos))) {
    LOG_WARN("failed to get sql plan infos", K(ret));
  } else if (OB_FAIL(format_sql_plan(sql_plan_infos,
                                     type,
                                     option,
                                     out_plan_text))) {
    LOG_WARN("failed to format sql plan", K(ret));
  } else if (OB_FAIL(plan_text_to_strings(out_plan_text, plan_strs))) {
    LOG_WARN("failed to convert plan text to strings", K(ret));
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to store sql plan", K(ret));
    ret = OB_SUCCESS;
  }
  destroy_buffer(plan_text);
  return ret;
}

int ObSqlPlan::get_plan_outline_info_one_line(PlanText &plan_text,
                                              ObLogPlan* plan)
{
  int ret = OB_SUCCESS;
  const ObQueryCtx *query_ctx = NULL;
  if (OB_ISNULL(plan) || OB_ISNULL(query_ctx = plan->get_optimizer_context().get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL", K(ret), K(plan), K(query_ctx));
  } else {
    plan_text.is_used_hint_ = false;
    plan_text.is_oneline_ = true;
    plan_text.is_outline_data_ = true;
    BUF_PRINT_CONST_STR("/*+BEGIN_OUTLINE_DATA", plan_text);
    const ObQueryHint &query_hint = query_ctx->get_query_hint();
    if (OB_FAIL(reset_plan_tree_outline_flag(plan->get_plan_root()))) {
      LOG_WARN("failed to reset plan tree outline flag", K(ret));
    } else if (OB_FAIL(get_plan_tree_outline(plan_text, plan->get_plan_root()))) {
      LOG_WARN("failed to get plan tree outline", K(ret));
    } else if (OB_FAIL(query_hint.print_transform_hints(plan_text))) {
      LOG_WARN("failed to print all transform hints", K(ret));
    } else if (OB_FAIL(get_global_hint_outline(plan_text, *plan))) {
      LOG_WARN("failed to get plan global hint outline", K(ret));
    } else {
      BUF_PRINT_CONST_STR(" END_OUTLINE_DATA*/", plan_text);
      plan_text.is_outline_data_ = false;
    }
    if (OB_SIZE_OVERFLOW == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObSqlPlan::get_global_hint_outline(PlanText &plan_text, ObLogPlan &plan)
{
  int ret = OB_SUCCESS;
  ObGlobalHint outline_global_hint;
  if (OB_FAIL(outline_global_hint.assign(plan.get_optimizer_context().get_global_hint()))) {
    LOG_WARN("failed to assign global hint", K(ret));
  } else if (OB_FAIL(construct_outline_global_hint(plan, outline_global_hint))) {
    LOG_WARN("failed to construct outline global hint", K(ret));
  } else if (OB_FAIL(outline_global_hint.print_global_hint(plan_text))) {
    LOG_WARN("failed to print global hint", K(ret));
  }
  return ret;
}

int ObSqlPlan::construct_outline_global_hint(ObLogPlan &plan, ObGlobalHint &outline_global_hint)
{
  int ret = OB_SUCCESS;
  ObDelUpdLogPlan *del_upd_plan = NULL;
  outline_global_hint.opt_features_version_ = ObGlobalHint::CURRENT_OUTLINE_ENABLE_VERSION;
  outline_global_hint.pdml_option_ = ObPDMLOption::NOT_SPECIFIED;
  if (OB_SUCC(ret) && NULL != (del_upd_plan = dynamic_cast<ObDelUpdLogPlan*>(&plan))
      && del_upd_plan->use_pdml()) {
    outline_global_hint.pdml_option_ = ObPDMLOption::ENABLE;
  }

  if (OB_SUCC(ret)) {
    outline_global_hint.parallel_ = ObGlobalHint::UNSET_PARALLEL;
    if (plan.get_optimizer_context().is_use_auto_dop()) {
      outline_global_hint.merge_parallel_hint(ObGlobalHint::SET_ENABLE_AUTO_DOP);
    } else if (plan.get_optimizer_context().get_max_parallel() > ObGlobalHint::DEFAULT_PARALLEL) {
      outline_global_hint.merge_parallel_hint(plan.get_optimizer_context().get_max_parallel());
    }
  }

  return ret;
}

int ObSqlPlan::inner_store_sql_plan_for_explain(ObExecContext *ctx,
                                                const ObString& plan_table,
                                                const ObString& statement_id,
                                                ObIArray<ObSqlPlanItem*> &sql_plan_infos)
{
  int ret = OB_SUCCESS;
  obutil::ObSysTime current_time = obutil::ObSysTime::now();
  std::string time_str = current_time.toDateTime();
  ObInnerSQLConnectionPool *pool = NULL;
  sql::ObSQLSessionInfo *session = NULL;
  ObInnerSQLConnection *conn = NULL;
  ObSQLSessionInfo::StmtSavedValue *saved_session = NULL;
  transaction::ObTxDesc *save_tx_desc = NULL;
  int64_t save_nested_count = 0;
  int64_t affected_rows = 0;
  ObSqlString sql;
  if (OB_ISNULL(ctx) ||
      OB_ISNULL(ctx->get_sql_proxy()) ||
      OB_ISNULL(session = ctx->get_my_session()) ||
      OB_ISNULL((pool = static_cast<ObInnerSQLConnectionPool *>
                (ctx->get_sql_proxy()->get_pool())))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null sql proxy", K(ret));
  } else if (OB_FAIL(pool->acquire_spi_conn(session, conn))) {
    LOG_WARN("failed to get sql connection", K(ret));
  } else if (OB_ISNULL(conn)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null sql connection", K(ret));
  } else if (OB_FAIL(prepare_and_store_session(session,
                                               saved_session,
                                               save_tx_desc,
                                               save_nested_count))) {
    LOG_WARN("failed to begin nested session", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < sql_plan_infos.count(); ++i) {
    ObSqlPlanItem *plan_item = sql_plan_infos.at(i);
    if (OB_ISNULL(plan_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null plan item", K(ret));
    }  else if (OB_FAIL(escape_quotes(*plan_item))) {
      LOG_WARN("failed to escape quotes", K(ret));
    } else if (OB_FAIL(sql.assign_fmt("INSERT INTO %.*s VALUES( \
      '%.*s', \
      CASE WHEN (SELECT MAX(PLAN_ID) FROM %.*s) IS NULL\
          THEN 0\
          ELSE (SELECT MAX(PLAN_ID) FROM %.*s)\
          END + %ld,  \
      '%.*s', \
      '%.*s', \
      '%.*s', \
      '%.*s', \
      '%.*s', \
      '%.*s', \
      '%.*s', \
      '%.*s', \
      %ld, \
      '%.*s', \
      '%.*s', \
      %d, \
      %d, \
      %d, \
      %d, \
      %d, \
      %d, \
      %ld, \
      %ld, \
      %ld, \
      %ld, \
      '%.*s', \
      '%.*s', \
      '%.*s', \
      %ld, \
      '%.*s', \
      '%.*s', \
      %ld, \
      %ld, \
      %ld, \
      '%.*s', \
      '%.*s', \
      '%.*s', \
      '%.*s', \
      '%.*s', \
      %ld, \
      '%.*s', \
      '%.*s' \
      )",
      plan_table.length(),
      plan_table.ptr(),
      statement_id.length(),
      statement_id.ptr(),
      plan_table.length(),
      plan_table.ptr(),
      plan_table.length(),
      plan_table.ptr(),
      (int64_t)(i > 0 ? 0 : 1),
      (int)time_str.length(),
      time_str.c_str(),
      (int)plan_item->remarks_len_,
      plan_item->remarks_,
      (int)plan_item->operation_len_,
      plan_item->operation_,
      (int)plan_item->options_len_,
      plan_item->options_,
      (int)plan_item->object_node_len_,
      plan_item->object_node_,
      (int)plan_item->object_owner_len_,
      plan_item->object_owner_,
      (int)plan_item->object_name_len_,
      plan_item->object_name_,
      (int)plan_item->object_alias_len_,
      plan_item->object_alias_,
      plan_item->object_id_,
      (int)plan_item->object_type_len_,
      plan_item->object_type_,
      (int)plan_item->optimizer_len_,
      plan_item->optimizer_,
      plan_item->search_columns_,
      plan_item->id_,
      plan_item->parent_id_,
      plan_item->depth_,
      plan_item->position_,
      (int)plan_item->is_last_child_,
      plan_item->cost_,
      plan_item->cardinality_,
      plan_item->bytes_,
      plan_item->rowset_,
      (int)plan_item->other_tag_len_,
      plan_item->other_tag_,
      (int)plan_item->partition_start_len_,
      plan_item->partition_start_,
      (int)plan_item->partition_stop_len_,
      plan_item->partition_stop_,
      plan_item->partition_id_,
      (int)plan_item->other_len_,
      plan_item->other_,
      (int)plan_item->distribution_len_,
      plan_item->distribution_,
      plan_item->cpu_cost_,
      plan_item->io_cost_,
      plan_item->temp_space_,
      (int)plan_item->access_predicates_len_,
      plan_item->access_predicates_,
      (int)plan_item->filter_predicates_len_,
      plan_item->filter_predicates_,
      (int)plan_item->startup_predicates_len_,
      plan_item->startup_predicates_,
      (int)plan_item->projection_len_,
      plan_item->projection_,
      (int)plan_item->special_predicates_len_,
      plan_item->special_predicates_,
      plan_item->time_,
      (int)plan_item->qblock_name_len_,
      plan_item->qblock_name_,
      (int)plan_item->other_xml_len_,
      plan_item->other_xml_
      ))) {
      LOG_WARN("failed to assign sql string", K(ret));
    } else if (OB_FAIL(conn->execute_write(session->get_effective_tenant_id(),
                                          sql.ptr(),
                                          affected_rows))) {
      LOG_WARN("failed to exec inner sql", K(ret));
    }
  }
  if (OB_NOT_NULL(conn) &&
      OB_NOT_NULL(ctx) &&
      OB_NOT_NULL(ctx->get_sql_proxy())) {
    ctx->get_sql_proxy()->close(conn, ret);
  }
  if (OB_NOT_NULL(session)) {
    int end_ret = restore_session(session,
                                  saved_session,
                                  save_tx_desc,
                                  save_nested_count);
    if (OB_SUCCESS != end_ret) {
      LOG_WARN("failed to restore session", K(end_ret), K(ret));
      if (OB_SUCCESS == ret) {
        ret = end_ret;
      }
    }
  }
  return ret;
}

int ObSqlPlan::escape_quotes(ObSqlPlanItem &plan_item)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_escape_quotes(plan_item.operation_,
                                  plan_item.operation_len_))) {
    LOG_WARN("failed to escape quotes", K(ret));
  } else if (OB_FAIL(inner_escape_quotes(plan_item.options_,
                                         plan_item.options_len_))) {
    LOG_WARN("failed to escape quotes", K(ret));
  } else if (OB_FAIL(inner_escape_quotes(plan_item.object_node_,
                                         plan_item.object_node_len_))) {
    LOG_WARN("failed to escape quotes", K(ret));
  } else if (OB_FAIL(inner_escape_quotes(plan_item.object_owner_,
                                         plan_item.object_owner_len_))) {
    LOG_WARN("failed to escape quotes", K(ret));
  } else if (OB_FAIL(inner_escape_quotes(plan_item.object_name_,
                                         plan_item.object_name_len_))) {
    LOG_WARN("failed to escape quotes", K(ret));
  } else if (OB_FAIL(inner_escape_quotes(plan_item.object_alias_,
                                         plan_item.object_alias_len_))) {
    LOG_WARN("failed to escape quotes", K(ret));
  } else if (OB_FAIL(inner_escape_quotes(plan_item.object_type_,
                                         plan_item.object_type_len_))) {
    LOG_WARN("failed to escape quotes", K(ret));
  } else if (OB_FAIL(inner_escape_quotes(plan_item.optimizer_,
                                         plan_item.optimizer_len_))) {
    LOG_WARN("failed to escape quotes", K(ret));
  } else if (OB_FAIL(inner_escape_quotes(plan_item.other_tag_,
                                         plan_item.other_tag_len_))) {
    LOG_WARN("failed to escape quotes", K(ret));
  } else if (OB_FAIL(inner_escape_quotes(plan_item.partition_start_,
                                         plan_item.partition_start_len_))) {
    LOG_WARN("failed to escape quotes", K(ret));
  } else if (OB_FAIL(inner_escape_quotes(plan_item.partition_stop_,
                                         plan_item.partition_stop_len_))) {
    LOG_WARN("failed to escape quotes", K(ret));
  } else if (OB_FAIL(inner_escape_quotes(plan_item.other_,
                                         plan_item.other_len_))) {
    LOG_WARN("failed to escape quotes", K(ret));
  } else if (OB_FAIL(inner_escape_quotes(plan_item.distribution_,
                                         plan_item.distribution_len_))) {
    LOG_WARN("failed to escape quotes", K(ret));
  } else if (OB_FAIL(inner_escape_quotes(plan_item.access_predicates_,
                                         plan_item.access_predicates_len_))) {
    LOG_WARN("failed to escape quotes", K(ret));
  } else if (OB_FAIL(inner_escape_quotes(plan_item.filter_predicates_,
                                         plan_item.filter_predicates_len_))) {
    LOG_WARN("failed to escape quotes", K(ret));
  } else if (OB_FAIL(inner_escape_quotes(plan_item.startup_predicates_,
                                         plan_item.startup_predicates_len_))) {
    LOG_WARN("failed to escape quotes", K(ret));
  } else if (OB_FAIL(inner_escape_quotes(plan_item.projection_,
                                         plan_item.projection_len_))) {
    LOG_WARN("failed to escape quotes", K(ret));
  } else if (OB_FAIL(inner_escape_quotes(plan_item.special_predicates_,
                                         plan_item.special_predicates_len_))) {
    LOG_WARN("failed to escape quotes", K(ret));
  } else if (OB_FAIL(inner_escape_quotes(plan_item.qblock_name_,
                                         plan_item.qblock_name_len_))) {
    LOG_WARN("failed to escape quotes", K(ret));
  } else if (OB_FAIL(inner_escape_quotes(plan_item.remarks_,
                                         plan_item.remarks_len_))) {
    LOG_WARN("failed to escape quotes", K(ret));
  } else if (OB_FAIL(inner_escape_quotes(plan_item.other_xml_,
                                         plan_item.other_xml_len_))) {
    LOG_WARN("failed to escape quotes", K(ret));
  }
  return ret;
}

/**
 * escape quotes for string value
 * oracle: '  => ''
 * mysql:  '  => \'
 */
int ObSqlPlan::inner_escape_quotes(char* &ptr, int64_t &length)
{
  int ret = OB_SUCCESS;
  int64_t num_quotes = 0;
  for (int64_t i = 0; i < length; ++i) {
    if (ptr[i] == '\'') {
      ++num_quotes;
    }
  }
  if (num_quotes > 0) {
    char *buf = NULL;
    int64_t buf_len = length + num_quotes;
    int64_t pos = 0;
    if (OB_ISNULL(buf=(char*)allocator_.alloc(buf_len))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret));
    } else {
      for (int64_t i = 0; i < length; ++i) {
        if (ptr[i] == '\'') {
          if (lib::is_mysql_mode()) {
            buf[pos++] = '\\';
          } else {
            buf[pos++] = '\'';
          }
        }
        buf[pos++] = ptr[i];
      }
      length = buf_len;
      ptr = buf;
    }
  }
  return ret;
}

int ObSqlPlan::get_sql_plan_infos(PlanText &plan_text,
                                  ObLogPlan* plan,
                                  ObIArray<ObSqlPlanItem*> &sql_plan_infos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null plan", K(ret));
  //get operator tree info
  } else if (OB_FAIL(get_plan_tree_infos(plan_text,
                                         plan->get_plan_root(),
                                         sql_plan_infos,
                                         0,
                                         1,
                                         false))) {
    LOG_WARN("failed to get plan tree infos", K(ret));
  } else if (sql_plan_infos.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null plan", K(ret));
  //get used hint、outline info
  } else if (OB_FAIL(get_plan_used_hint_info(plan_text,
                                             plan,
                                             sql_plan_infos.at(0)))) {
    LOG_WARN("failed to get plan outline info", K(ret));
  } else if (OB_FAIL(get_qb_name_trace(plan_text,
                                      plan,
                                      sql_plan_infos.at(0)))) {
    LOG_WARN("failed to get qb name trace", K(ret));
  } else if (OB_FAIL(get_plan_outline_info(plan_text,
                                           plan,
                                           sql_plan_infos.at(0)))) {
    LOG_WARN("failed to get plan outline info", K(ret));
  } else if (OB_FAIL(get_plan_other_info(plan_text,
                                         plan,
                                         sql_plan_infos.at(0)))) {
    LOG_WARN("failed to get plan other info", K(ret));
  }
  return ret;
}

int ObSqlPlan::get_plan_tree_infos(PlanText &plan_text,
                                  ObLogicalOperator* op,
                                  ObIArray<ObSqlPlanItem*> &sql_plan_infos,
                                  int depth,
                                  int position,
                                  bool is_last_child)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(plan_text.buf_) || OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null op", K(ret));
  } else if (plan_text.buf_len_ - plan_text.pos_ < sizeof(ObSqlPlanItem)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("buffer size is not enough", K(ret));
  } else {
    ObSqlPlanItem *plan_item = new(plan_text.buf_ + plan_text.pos_)ObSqlPlanItem();
    plan_text.pos_ += sizeof(ObSqlPlanItem);
    plan_item->depth_ = depth;
    plan_item->position_ = position;
    plan_item->is_last_child_ = is_last_child;
    if (OB_FAIL(op->get_plan_item_info(plan_text,
                                       *plan_item))) {
      LOG_WARN("failed to get plan item info", K(ret));
    } else if (OB_FAIL(sql_plan_infos.push_back(plan_item))) {
      LOG_WARN("failed to push back sql plan item", K(ret));
    }
    for (int i = 0; OB_SUCC(ret) && i < op->get_num_of_child(); ++i) {
      if (OB_FAIL(SMART_CALL(get_plan_tree_infos(plan_text,
                                                 op->get_child(i),
                                                 sql_plan_infos,
                                                 depth + 1,
                                                 i+1,
                                                 i+1 == op->get_num_of_child())))) {
        LOG_WARN("failed to get child plan tree infos", K(ret));
      }
    }
  }
  return ret;
}

int ObSqlPlan::get_plan_used_hint_info(PlanText &plan_text,
                                      ObLogPlan* plan,
                                      ObSqlPlanItem* sql_plan_item)
{
  int ret = OB_SUCCESS;
  const ObQueryCtx *query_ctx = NULL;
  //print_plan_tree:print_used_hint
  if (OB_ISNULL(plan) || OB_ISNULL(sql_plan_item) ||
      OB_ISNULL(query_ctx = plan->get_optimizer_context().get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL", K(ret), K(plan), K(query_ctx));
  } else {
    const ObQueryHint &query_hint = query_ctx->get_query_hint();
    PlanText temp_text;
    temp_text.is_used_hint_ = true;
    temp_text.buf_ = plan_text.buf_ + plan_text.pos_;
    temp_text.buf_len_ = plan_text.buf_len_ - plan_text.pos_;
    temp_text.pos_ = 0;
    BUF_PRINT_CONST_STR("  /*+", temp_text);
    BUF_PRINT_CONST_STR(NEW_LINE, temp_text);
    BUF_PRINT_CONST_STR(OUTPUT_PREFIX, temp_text);
    if (OB_FAIL(get_plan_tree_used_hint(temp_text, plan->get_plan_root()))) {
      LOG_WARN("failed to get plan tree used hint", K(ret));
    } else if (OB_FAIL(query_hint.print_qb_name_hints(temp_text))) {
      LOG_WARN("failed to print qb name hints", K(ret));
    } else if (OB_FAIL(query_hint.print_transform_hints(temp_text))) {
      LOG_WARN("failed to print all transform hints", K(ret));
    } else if (OB_FAIL(query_hint.get_global_hint().print_global_hint(temp_text))) {
      LOG_WARN("failed to print global hint", K(ret));
    } else {
      BUF_PRINT_CONST_STR(NEW_LINE, temp_text);
      BUF_PRINT_CONST_STR("  */", temp_text);
      sql_plan_item->other_tag_ = temp_text.buf_;
      sql_plan_item->other_tag_len_ = temp_text.pos_;
      plan_text.pos_ += temp_text.pos_;
    }
  }
  return ret;
}

int ObSqlPlan::get_plan_tree_used_hint(PlanText &plan_text,
                                       ObLogicalOperator* op)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null op", K(ret));
  } else if (OB_FAIL(op->print_used_hint(plan_text))) {
    LOG_WARN("failed to get plan used hint", K(ret));
  }
  for (int i = 0; OB_SUCC(ret) && i < op->get_num_of_child(); ++i) {
    if (OB_FAIL(SMART_CALL(get_plan_tree_used_hint(plan_text,
                                                   op->get_child(i))))) {
      LOG_WARN("failed to get child plan tree used hint", K(ret));
    }
  }
  return ret;
}

int ObSqlPlan::get_qb_name_trace(PlanText &plan_text,
                                ObLogPlan* plan,
                                ObSqlPlanItem* sql_plan_item)
{
  int ret = OB_SUCCESS;
  const ObQueryCtx *query_ctx = NULL;
  if (OB_ISNULL(plan) || OB_ISNULL(sql_plan_item) ||
      OB_ISNULL(query_ctx = plan->get_optimizer_context().get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL", K(ret), K(plan), K(query_ctx));
  } else {
    PlanText temp_text;
    temp_text.buf_ = plan_text.buf_ + plan_text.pos_;
    temp_text.buf_len_ = plan_text.buf_len_ - plan_text.pos_;
    temp_text.pos_ = 0;
    char *buf = temp_text.buf_;
    int64_t &buf_len = temp_text.buf_len_;
    int64_t &pos = temp_text.pos_;
    const ObIArray<QbNames> &stmt_id_map = query_ctx->get_query_hint().stmt_id_map_;
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt_id_map.count(); ++i) {
      if (OB_FAIL(BUF_PRINTF("  stmt_id:%ld, ", i))) {
      } else if (OB_FAIL(stmt_id_map.at(i).print_qb_names(temp_text))) {
        LOG_WARN("failed to print qb names", K(ret));
      } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
        /* Do nothing */
      }
    }
    if (OB_SUCC(ret)) {
      sql_plan_item->remarks_ = temp_text.buf_;
      sql_plan_item->remarks_len_ = temp_text.pos_;
      plan_text.pos_ += temp_text.pos_;
    }
  }
  return ret;
}

int ObSqlPlan::get_plan_outline_info(PlanText &plan_text,
                                    ObLogPlan* plan,
                                    ObSqlPlanItem* sql_plan_item)
{
  int ret = OB_SUCCESS;
  const ObQueryCtx *query_ctx = NULL;
  if (OB_ISNULL(plan) || OB_ISNULL(sql_plan_item) ||
      OB_ISNULL(query_ctx = plan->get_optimizer_context().get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL", K(ret), K(plan), K(query_ctx));
  } else {
    PlanText temp_text;
    temp_text.is_used_hint_ = false;
    temp_text.is_outline_data_ = true;
    temp_text.buf_ = plan_text.buf_ + plan_text.pos_;
    temp_text.buf_len_ = plan_text.buf_len_ - plan_text.pos_;
    temp_text.pos_ = 0;
    BUF_PRINT_CONST_STR("  /*+", temp_text);
    BUF_PRINT_CONST_STR(NEW_LINE, temp_text);
    BUF_PRINT_CONST_STR(OUTPUT_PREFIX, temp_text);
    BUF_PRINT_CONST_STR("BEGIN_OUTLINE_DATA", temp_text);
    const ObQueryHint &query_hint = query_ctx->get_query_hint();
    if (OB_FAIL(reset_plan_tree_outline_flag(plan->get_plan_root()))) {
      LOG_WARN("failed to reset plan tree outline flag", K(ret));
    } else if (OB_FAIL(get_plan_tree_outline(temp_text, plan->get_plan_root()))) {
      LOG_WARN("failed to get plan tree outline", K(ret));
    } else if (OB_FAIL(query_hint.print_transform_hints(temp_text))) {
      LOG_WARN("failed to print all transform hints", K(ret));
    } else if (OB_FAIL(get_global_hint_outline(temp_text, *plan))) {
      LOG_WARN("failed to get plan global hint outline", K(ret));
    } else {
      BUF_PRINT_CONST_STR(NEW_LINE, temp_text);
      BUF_PRINT_CONST_STR(OUTPUT_PREFIX, temp_text);
      BUF_PRINT_CONST_STR("END_OUTLINE_DATA", temp_text);
      BUF_PRINT_CONST_STR(NEW_LINE, temp_text);
      BUF_PRINT_CONST_STR("  */", temp_text);
      sql_plan_item->other_xml_ = temp_text.buf_;
      sql_plan_item->other_xml_len_ = temp_text.pos_;
      plan_text.pos_ += temp_text.pos_;
    }
  }
  return ret;
}

int ObSqlPlan::reset_plan_tree_outline_flag(ObLogicalOperator* op)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(op) || OB_ISNULL(op->get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null op", K(ret));
  } else {
    op->get_plan()->reset_outline_print_flags();
  }
  for (int i = 0; OB_SUCC(ret) && i < op->get_num_of_child(); ++i) {
    if (OB_FAIL(SMART_CALL(reset_plan_tree_outline_flag(op->get_child(i))))) {
      LOG_WARN("failed to reset child plan tree outline flag", K(ret));
    }
  }
  return ret;
}

int ObSqlPlan::get_plan_tree_outline(PlanText &plan_text,
                                     ObLogicalOperator* op)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null op", K(ret));
  } else if (OB_FAIL(op->print_outline_data(plan_text))) {
    LOG_WARN("failed to get plan outline", K(ret));
  }
  for (int i = 0; OB_SUCC(ret) && i < op->get_num_of_child(); ++i) {
    if (OB_FAIL(SMART_CALL(get_plan_tree_outline(plan_text,
                                                 op->get_child(i))))) {
      LOG_WARN("failed to get child plan tree outline", K(ret));
    }
  }
  return ret;
}

int ObSqlPlan::get_plan_other_info(PlanText &plan_text,
                                  ObLogPlan* plan,
                                  ObSqlPlanItem* sql_plan_item)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = NULL;
  const ObQueryCtx *query_ctx = NULL;
  if (OB_ISNULL(plan) || OB_ISNULL(sql_plan_item) ||
      OB_ISNULL(stmt=plan->get_stmt()) || OB_ISNULL(query_ctx=stmt->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(ret));
  } else {
    ObObjPrintParams print_params(query_ctx->get_timezone_info());
    BEGIN_BUF_PRINT
    ObPhyPlanType plan_type = plan->get_optimizer_context().get_phy_plan_type();
    if (OB_FAIL(BUF_PRINTF("  Plan Type:"))) {
    } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
    } else if (OB_FAIL(BUF_PRINTF(OUTPUT_PREFIX))) {
    } else if (OB_FAIL(BUF_PRINTF("%.*s",
                                  ob_plan_type_str(plan_type).length(),
                                  ob_plan_type_str(plan_type).ptr()))) {
    } else {
      ret = BUF_PRINTF(NEW_LINE);
    }

    const ParamStore *params = NULL;
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(params = plan->get_optimizer_context().get_params())) {
      ret = OB_ERR_UNEXPECTED;
    } else if (params->count() <= 0) {
      //do nothing
    } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
    } else if (OB_FAIL(BUF_PRINTF("  Parameters:"))) {
    } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
    } else { /* Do nothing */ }
    for (int64_t i = 0; OB_SUCC(ret) && NULL != params && i < params->count(); i++) {
      if (OB_FAIL(BUF_PRINTF(OUTPUT_PREFIX))) {
      } else if (OB_FAIL(BUF_PRINTF(":%ld => ", i))) {
      } else {
        int64_t save_pos = pos;
        if (OB_FAIL(params->at(i).print_sql_literal(buf,
                                                    buf_len,
                                                    pos,
                                                    print_params))) {
          //ignore size overflow error
          pos = save_pos;
        }
        if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
        } else { /* Do nothing */ }
      }
    }

    const ObPlanNotes &notes = plan->get_optimizer_context().get_plan_notes();
    if (notes.count() <= 0 || OB_FAIL(ret)) {
    } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) { /* Do nothing */
    } else if (OB_FAIL(BUF_PRINTF("  Note:"))) { /* Do nothing */
    } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) { /* Do nothing */
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < notes.count(); i++) {
      if (OB_FAIL(BUF_PRINTF(OUTPUT_PREFIX))) {
      } else {
        pos += notes.at(i).to_string(buf + pos, buf_len - pos);
        ret = BUF_PRINTF(NEW_LINE);
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(get_constraint_info(buf, buf_len, pos, *query_ctx))) {
        LOG_WARN("failed to get constraint info", K(ret));
      }
    }
    END_BUF_PRINT(sql_plan_item->other_, sql_plan_item->other_len_);
  }
  // statis version
  // constraint
  return ret;
}

int ObSqlPlan::get_constraint_info(char *buf,
                                  int64_t buf_len,
                                  int64_t &pos,
                                  const ObQueryCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (!ctx.all_plan_const_param_constraints_.empty()) {
    if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
    } else if (OB_FAIL(BUF_PRINTF("  "))) {
    } else if (OB_FAIL(BUF_PRINTF("Const Parameter Constraints:"))) {
    } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx.all_plan_const_param_constraints_.count(); ++i) {
      if (OB_FAIL(print_constraint_info(buf,
                                        buf_len,
                                        pos,
                                        ctx.all_plan_const_param_constraints_.at(i)))) {
        LOG_WARN("failed to print constraint info", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && !ctx.all_possible_const_param_constraints_.empty()) {
    if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
    } else if (OB_FAIL(BUF_PRINTF("  "))) {
    } else if (OB_FAIL(BUF_PRINTF("Possible Const Parameter Constraints:"))) {
    } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx.all_possible_const_param_constraints_.count(); ++i) {
      if (OB_FAIL(print_constraint_info(buf,
                                        buf_len,
                                        pos,
                                        ctx.all_possible_const_param_constraints_.at(i)))) {
        LOG_WARN("failed to print constraint info", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && !ctx.all_equal_param_constraints_.empty()) {
    if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
    } else if (OB_FAIL(BUF_PRINTF("  "))) {
    } else if (OB_FAIL(BUF_PRINTF("Equal Parameter Constraints:"))) {
    } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx.all_equal_param_constraints_.count(); ++i) {
      if (OB_FAIL(print_constraint_info(buf,
                                        buf_len,
                                        pos,
                                        ctx.all_equal_param_constraints_.at(i)))) {
        LOG_WARN("failed to print constraint info", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && !ctx.all_expr_constraints_.empty()) {
    if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
    } else if (OB_FAIL(BUF_PRINTF("  "))) {
    } else if (OB_FAIL(BUF_PRINTF("Expr Constraints:"))) {
    } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx.all_expr_constraints_.count(); ++i) {
      if (OB_FAIL(print_constraint_info(buf,
                                        buf_len,
                                        pos,
                                        ctx.all_expr_constraints_.at(i)))) {
        LOG_WARN("failed to print constraint info", K(ret));
      }
    }
  }
  return ret;
}

int ObSqlPlan::print_constraint_info(char *buf,
                                    int64_t buf_len,
                                    int64_t &pos,
                                    const ObPCConstParamInfo &info)
{
  int ret = OB_SUCCESS;
  if (info.const_idx_.count() != info.const_params_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect constraint info", K(info), K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < info.const_idx_.count(); ++i) {
    if (OB_FAIL(BUF_PRINTF(OUTPUT_PREFIX))) {
    } else if (OB_FAIL(BUF_PRINTF(":%ld = ", info.const_idx_.at(i)))) {
    } else if (OB_FAIL(info.const_params_.at(i).print_sql_literal(buf, buf_len, pos))) {
    } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
    } else { /* Do nothing */ }
  }
  return ret;
}

int ObSqlPlan::print_constraint_info(char *buf,
                                    int64_t buf_len,
                                    int64_t &pos,
                                    const ObPCParamEqualInfo &info)
{
  int ret = OB_SUCCESS;
  if (info.use_abs_cmp_) {
    if (OB_FAIL(BUF_PRINTF(OUTPUT_PREFIX))) {
    } else if (OB_FAIL(BUF_PRINTF(":%ld = abs( :%ld )",
                          info.first_param_idx_,
                          info.second_param_idx_))) {
    } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
    } else { /* Do nothing */ }
  } else {
    if (OB_FAIL(BUF_PRINTF(OUTPUT_PREFIX))) {
    } else if (OB_FAIL(BUF_PRINTF(":%ld = :%ld",
                          info.first_param_idx_,
                          info.second_param_idx_))) {
    } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
    } else { /* Do nothing */ }
  }
  return ret;
}

int ObSqlPlan::print_constraint_info(char *buf,
                                    int64_t buf_len,
                                    int64_t &pos,
                                    const ObExprConstraint &info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(info.pre_calc_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null expr", K(ret));
  } else if (OB_FAIL(BUF_PRINTF(OUTPUT_PREFIX))) {
  } else if (OB_FAIL(info.pre_calc_expr_->get_name(buf, buf_len, pos))) {
    LOG_WARN("failed to print expr", K(ret));
  } else if (OB_FAIL(BUF_PRINTF(" result is "))) {
  } else {
    if (PRE_CALC_RESULT_NULL == info.expect_result_) {
      ret = BUF_PRINTF("NULL");
    } else if (PRE_CALC_RESULT_NOT_NULL == info.expect_result_) {
      ret = BUF_PRINTF("NOT NULL");
    } else if (PRE_CALC_RESULT_TRUE == info.expect_result_) {
      ret = BUF_PRINTF("TRUE");
    } else if (PRE_CALC_RESULT_FALSE == info.expect_result_) {
      ret = BUF_PRINTF("FALSE");
    } else if (PRE_CALC_RESULT_NO_WILDCARD == info.expect_result_) {
      ret = BUF_PRINTF("NO_WILDCARD");
    } else if (PRE_CALC_ERROR == info.expect_result_) {
      ret = BUF_PRINTF("ERROR");
    } else if (PRE_CALC_PRECISE == info.expect_result_) {
      ret = BUF_PRINTF("PRECISE");
    } else if (PRE_CALC_NOT_PRECISE == info.expect_result_) {
      ret = BUF_PRINTF("NOT_PRECISE");
    } else if (PRE_CALC_ROWID == info.expect_result_) {
      ret = BUF_PRINTF("ROWID");
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
    } else { /* Do nothing */ }
  }
  return ret;
}

int ObSqlPlan::format_sql_plan(ObIArray<ObSqlPlanItem*> &sql_plan_infos,
                              ExplainType type,
                              const ObExplainDisplayOpt& option,
                              PlanText &plan_text)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_buffer(plan_text))) {
    LOG_WARN("failed to init buffer", K(ret));
  } else if (sql_plan_infos.empty()) {
    //do nothing
  } else {
    if (EXPLAIN_BASIC == type) {
      if (OB_FAIL(format_basic_plan_table(sql_plan_infos, option, plan_text))) {
        LOG_WARN("failed to print plan", K(ret));
      } else if (OB_FAIL(format_plan_output(sql_plan_infos, plan_text))) {
        LOG_WARN("failed to print plan output", K(ret));
      }
    } else if (EXPLAIN_OUTLINE == type) {
      if (OB_FAIL(format_plan_table(sql_plan_infos, option, plan_text))) {
        LOG_WARN("failed to print plan", K(ret));
      } else if (OB_FAIL(format_plan_output(sql_plan_infos, plan_text))) {
        LOG_WARN("failed to print plan output", K(ret));
      } else if (OB_FAIL(format_outline(sql_plan_infos, plan_text))) {
        LOG_WARN("failed to print outline", K(ret));
      }
    } else if (EXPLAIN_EXTENDED == type ||
               EXPLAIN_EXTENDED_NOADDR == type) {
      if (OB_FAIL(format_plan_table(sql_plan_infos, option, plan_text))) {
        LOG_WARN("failed to print plan", K(ret));
      } else if (OB_FAIL(format_plan_output(sql_plan_infos, plan_text))) {
        LOG_WARN("failed to print plan output", K(ret));
      } else if (OB_FAIL(format_used_hint(sql_plan_infos, plan_text))) {
        LOG_WARN("failed to print used hint", K(ret));
      } else if (OB_FAIL(format_qb_name_trace(sql_plan_infos, plan_text))) {
        LOG_WARN("failed to print qb name trace", K(ret));
      } else if (OB_FAIL(format_outline(sql_plan_infos, plan_text))) {
        LOG_WARN("failed to print outline", K(ret));
      } else if (OB_FAIL(format_optimizer_info(sql_plan_infos, plan_text))) {
        LOG_WARN("failed to print optimizer info", K(ret));
      } else if (OB_FAIL(format_other_info(sql_plan_infos, plan_text))) {
        LOG_WARN("failed to print other info", K(ret));
      }
    } else if (EXPLAIN_FORMAT_JSON == type) {
      if (OB_FAIL(format_plan_to_json(sql_plan_infos, plan_text))) {
        LOG_WARN("failed to print plan to json", K(ret));
      }
    } else {
      if (OB_FAIL(format_plan_table(sql_plan_infos, option, plan_text))) {
        LOG_WARN("failed to print plan", K(ret));
      } else if (OB_FAIL(format_plan_output(sql_plan_infos, plan_text))) {
        LOG_WARN("failed to print plan output", K(ret));
      }
    }
    BUF_PRINT_CONST_STR(NEW_LINE, plan_text);
  }
  return ret;
}

int ObSqlPlan::PlanFormatHelper::init()
{
  int ret = OB_SUCCESS;
  operator_prefix_.reuse();
  column_len_.reuse();
  total_len_ = 0;
  if (OB_FAIL(column_len_.push_back(strlen(ExplainColumnName[Id])))) {
    LOG_WARN("failed to push back data", K(ret));
  } else if (OB_FAIL(column_len_.push_back(strlen(ExplainColumnName[Operator])))) {
    LOG_WARN("failed to push back data", K(ret));
  } else if (OB_FAIL(column_len_.push_back(strlen(ExplainColumnName[Name])))) {
    LOG_WARN("failed to push back data", K(ret));
  } else if (OB_FAIL(column_len_.push_back(strlen(ExplainColumnName[EstRows])))) {
    LOG_WARN("failed to push back data", K(ret));
  } else if (OB_FAIL(column_len_.push_back(strlen(ExplainColumnName[EstCost])))) {
    LOG_WARN("failed to push back data", K(ret));
  } else if (OB_FAIL(column_len_.push_back(strlen(ExplainColumnName[RealRows])))) {
    LOG_WARN("failed to push back data", K(ret));
  } else if (OB_FAIL(column_len_.push_back(strlen(ExplainColumnName[RealCost])))) {
    LOG_WARN("failed to push back data", K(ret));
  } else if (OB_FAIL(column_len_.push_back(strlen(ExplainColumnName[IoTime])))) {
    LOG_WARN("failed to push back data", K(ret));
  } else if (OB_FAIL(column_len_.push_back(strlen(ExplainColumnName[CpuTime])))) {
    LOG_WARN("failed to push back data", K(ret));
  }
  return ret;
}

int ObSqlPlan::get_plan_table_formatter(ObIArray<ObSqlPlanItem*> &sql_plan_infos,
                                        const ObExplainDisplayOpt& option,
                                        ObSqlPlan::PlanFormatHelper &format_helper)
{
  int ret = OB_SUCCESS;
  int32_t length = 0;
  char buffer[50];
  if (OB_FAIL(format_helper.init())) {
    LOG_WARN("failed to init format helper", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < sql_plan_infos.count(); ++i) {
    ObSqlPlanItem *plan_item = sql_plan_infos.at(i);
    if (OB_ISNULL(plan_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null plan item", K(ret));
    }
    //ID
    if (OB_SUCC(ret)) {
      snprintf(buffer, sizeof(buffer), "%d", plan_item->id_);
      length = (int32_t) strlen(buffer);
      if (length > format_helper.column_len_.at(Id)) {
        format_helper.column_len_.at(Id) = length;
      }
    }
    //OPERATOR
    if (OB_SUCC(ret)) {
      int64_t operation_len = plan_item->operation_len_ + plan_item->depth_*2;
      if (operation_len > format_helper.column_len_.at(Operator)) {
        format_helper.column_len_.at(Operator) = operation_len;
      }
    }
    //NAME
    if (OB_SUCC(ret)) {
      if (plan_item->object_alias_len_ > format_helper.column_len_.at(Name)) {
        format_helper.column_len_.at(Name) = plan_item->object_alias_len_;
      }
    }
    //EST ROWS
    if (OB_SUCC(ret)) {
      snprintf(buffer, sizeof(buffer), "%ld", plan_item->cardinality_);
      length = (int32_t) strlen(buffer);
      if (length > format_helper.column_len_.at(EstRows)) {
        format_helper.column_len_.at(EstRows) = length;
      }
    }
    //EST COST
    if (OB_SUCC(ret)) {
      snprintf(buffer, sizeof(buffer), "%ld", plan_item->cost_);
      length = (int32_t) strlen(buffer);
      if (length > format_helper.column_len_.at(EstCost)) {
        format_helper.column_len_.at(EstCost) = length;
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(get_operator_prefix(sql_plan_infos, option, format_helper))) {
      LOG_WARN("failed to get operator prefix", K(ret));
    }
  }
  return ret;
}

int ObSqlPlan::get_real_plan_table_formatter(ObIArray<ObSqlPlanItem*> &sql_plan_infos,
                                            const ObExplainDisplayOpt& option,
                                            ObSqlPlan::PlanFormatHelper &format_helper)
{
  int ret = OB_SUCCESS;
  int32_t length = 0;
  char buffer[50];
  if (OB_FAIL(format_helper.init())) {
    LOG_WARN("failed to init format helper", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < sql_plan_infos.count(); ++i) {
    ObSqlPlanItem *plan_item = sql_plan_infos.at(i);
    if (OB_ISNULL(plan_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null plan item", K(ret));
    }
    //ID
    if (OB_SUCC(ret)) {
      snprintf(buffer, sizeof(buffer), "%d", plan_item->id_);
      length = (int32_t) strlen(buffer);
      if (length > format_helper.column_len_.at(Id)) {
        format_helper.column_len_.at(Id) = length;
      }
    }
    //OPERATOR
    if (OB_SUCC(ret)) {
      int64_t operation_len = plan_item->operation_len_ + plan_item->depth_ * 2;
      if (operation_len > format_helper.column_len_.at(Operator)) {
        format_helper.column_len_.at(Operator) = operation_len;
      }
    }
    //NAME
    if (OB_SUCC(ret)) {
      if (plan_item->object_alias_len_ > format_helper.column_len_.at(Name)) {
        format_helper.column_len_.at(Name) = plan_item->object_alias_len_;
      }
    }
    //EST ROWS
    if (OB_SUCC(ret)) {
      snprintf(buffer, sizeof(buffer), "%ld", plan_item->cardinality_);
      length = (int32_t) strlen(buffer);
      if (length > format_helper.column_len_.at(EstRows)) {
        format_helper.column_len_.at(EstRows) = length;
      }
    }
    //EST COST
    if (OB_SUCC(ret)) {
      snprintf(buffer, sizeof(buffer), "%ld", plan_item->cost_);
      length = (int32_t) strlen(buffer);
      if (length > format_helper.column_len_.at(EstCost)) {
        format_helper.column_len_.at(EstCost) = length;
      }
    }
    //REAL ROWS
    if (OB_SUCC(ret)) {
      snprintf(buffer, sizeof(buffer), "%ld", plan_item->real_cardinality_);
      length = (int32_t) strlen(buffer);
      if (length > format_helper.column_len_.at(RealRows)) {
        format_helper.column_len_.at(RealRows) = length;
      }
    }
    //REAL COST
    if (OB_SUCC(ret)) {
      snprintf(buffer, sizeof(buffer), "%ld", plan_item->real_cost_);
      length = (int32_t) strlen(buffer);
      if (length > format_helper.column_len_.at(RealCost)) {
        format_helper.column_len_.at(RealCost) = length;
      }
    }
    //IO TIME
    if (OB_SUCC(ret)) {
      snprintf(buffer, sizeof(buffer), "%ld", plan_item->io_cost_);
      length = (int32_t) strlen(buffer);
      if (length > format_helper.column_len_.at(IoTime)) {
        format_helper.column_len_.at(IoTime) = length;
      }
    }
    //CPU TIME
    if (OB_SUCC(ret)) {
      snprintf(buffer, sizeof(buffer), "%ld", plan_item->cpu_cost_);
      length = (int32_t) strlen(buffer);
      if (length > format_helper.column_len_.at(CpuTime)) {
        format_helper.column_len_.at(CpuTime) = length;
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(get_operator_prefix(sql_plan_infos, option, format_helper))) {
      LOG_WARN("failed to get operator prefix", K(ret));
    }
  }
  return ret;
}

struct PrefixHelper {
  PrefixHelper()
  :cur_color_idx_(0)
  {}

  ObSEArray<int64_t, 4> plan_item_idxs_;
  ObSEArray<int64_t, 4> color_idxs_;
  ObSEArray<bool, 4> with_line_;
  int32_t cur_color_idx_;
  int set_value(int64_t idx,
                int64_t item_idx,
                bool with_line,
                bool with_color);
  void pop_back();
};

int PrefixHelper::set_value(int64_t idx,
                            int64_t item_idx,
                            bool with_line,
                            bool with_color)
{
  int ret = OB_SUCCESS;
  if (plan_item_idxs_.count() > idx) {
    plan_item_idxs_.at(idx) = item_idx;
  } else if (idx > plan_item_idxs_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect idx", K(ret));
  } else if (OB_FAIL(plan_item_idxs_.push_back(item_idx))) {
    LOG_WARN("failed to push back idx", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (color_idxs_.count() > idx) {
    color_idxs_.at(idx) = with_color ? cur_color_idx_++ : -1;
  } else if (idx > color_idxs_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect idx", K(ret));
  } else if (!with_color &&
             OB_FAIL(color_idxs_.push_back(-1))) {
    LOG_WARN("failed to push back idx", K(ret));
  } else if (with_color &&
             OB_FAIL(color_idxs_.push_back(cur_color_idx_++))) {
    LOG_WARN("failed to push back idx", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (with_line_.count() > idx) {
    with_line_.at(idx) = with_line;
  } else if (idx > with_line_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect idx", K(ret));
  } else if (OB_FAIL(with_line_.push_back(with_line))) {
    LOG_WARN("failed to push back idx", K(ret));
  }
  return ret;
}

void PrefixHelper::pop_back()
{
  plan_item_idxs_.pop_back();
  color_idxs_.pop_back();
  with_line_.pop_back();
}

int ObSqlPlan::get_operator_prefix(ObIArray<ObSqlPlanItem*> &sql_plan_infos,
                                  const ObExplainDisplayOpt& option,
                                  PlanFormatHelper &format_helper)
{
  int ret = OB_SUCCESS;
  static const char *colors[] = {
    "\033[32m", // GREEN
    "\033[33m", // ORANGE
    "\033[35m", // PURPLE
    "\033[91m", // LIGHTRED
    "\033[92m", // LIGHTGREEN
    "\033[93m", // YELLOW
    "\033[94m", // LIGHTBLUE
    "\033[95m", // PINK
    "\033[96m", // LIGHTCYAN
    "\033[1;31m", // RED
    "\033[1;34m" // BLUE
  };
  const char *color_end = "\033[0m";
  PrefixHelper prefix_helper;
  int64_t plan_level = 0;
  char *buf = NULL;
  int64_t buf_len =0;
  int64_t pos = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < sql_plan_infos.count(); ++i) {
    ObSqlPlanItem *plan_item = sql_plan_infos.at(i);
    if (OB_ISNULL(plan_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null plan item", K(ret));
    } else {
      if (plan_item->depth_ > plan_level) {
        //child
        ++plan_level;
      } else if (plan_item->depth_ == plan_level) {
        //brother
      } else {
        //parent
        while (plan_item->depth_ <= plan_level) {
          --plan_level;
          prefix_helper.pop_back();
        }
        ++plan_level;
      }
      bool need_line = false;
      if (option.with_tree_line_) {
        if (i + 1 < sql_plan_infos.count()) {
          ObSqlPlanItem *child1 = sql_plan_infos.at(i+1);
          if (OB_ISNULL(child1)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpect null item", K(ret));
          } else if (child1->depth_ > plan_item->depth_) {
            need_line = true;
          }
        }
      }
      if (OB_SUCC(ret) &&
          OB_FAIL(prefix_helper.set_value(plan_level,
                                          i,
                                          need_line,
                                          option.with_color_))) {
        LOG_WARN("failed to set value", K(ret));
      }
      buf_len = plan_level * 25;
      pos = 0;
      if (OB_SUCC(ret) && plan_level > 0) {
        buf = static_cast<char*>(allocator_.alloc(buf_len));
        if (OB_ISNULL(buf)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("allocate buffer failed", K(ret));
        }
      }
      //get prefix info
      for (int64_t j = 0; OB_SUCC(ret) && j < plan_level; ++j) {
        ObSqlPlanItem *parent_item = NULL;
        if (j >= prefix_helper.with_line_.count() ||
            j >= prefix_helper.plan_item_idxs_.count() ||
            j >= prefix_helper.color_idxs_.count() ||
            prefix_helper.plan_item_idxs_.at(j) >= sql_plan_infos.count() ||
            OB_ISNULL(parent_item=sql_plan_infos.at(prefix_helper.plan_item_idxs_.at(j)))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect idx", K(ret));
        } else if (prefix_helper.with_line_.at(j)) {
          const char *txt = "│ ";
          if (plan_item->parent_id_ == parent_item->id_) {
            if (plan_item->is_last_child_) {
              txt = "└─";
              prefix_helper.with_line_.at(j) = false;
            } else {
              txt = "├─";
            }
          }
          if (prefix_helper.color_idxs_.at(j) >= 0) {
            ret = BUF_PRINTF("%s%s%s",
                    colors[prefix_helper.color_idxs_.at(j) % ARRAYSIZEOF(colors)],
                    txt,
                    color_end);
          } else {
            ret = BUF_PRINTF("%s", txt);
          }
        } else {
          ret = BUF_PRINTF("  ");
        }
      }
      if (OB_SUCC(ret)) {
        ObString prefix(pos, buf);
        if (OB_FAIL(format_helper.operator_prefix_.push_back(prefix))) {
          LOG_WARN("failed to push back prefix", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObSqlPlan::format_basic_plan_table(ObIArray<ObSqlPlanItem*> &sql_plan_infos,
                                 const ObExplainDisplayOpt& option,
                                 PlanText &plan_text)
{
  int ret = OB_SUCCESS;
  ObSqlPlan::PlanFormatHelper format_helper;
  if (OB_FAIL(get_plan_table_formatter(sql_plan_infos,
                                       option,
                                       format_helper))) {
    LOG_WARN("failed to get plan table formatter", K(ret));
  } else {
    BEGIN_BUF_PRINT;
    for (int64_t i = 0; OB_SUCC(ret) && i < BASIC_PLAN_TABLE_COLUMN_CNT; ++i) {
      format_helper.total_len_ += format_helper.column_len_.at(i);
    }
    format_helper.total_len_ += BASIC_PLAN_TABLE_COLUMN_CNT+1;
    // print plan text header line
    for (int i = 0; OB_SUCC(ret) && i < format_helper.total_len_; i++) {
      ret = BUF_PRINTF(PLAN_WRAPPER);
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
      /* Do nothing */
    } else {
      ret = BUF_PRINTF(COLUMN_SEPARATOR);
    }
    // print column n~ames
    for (int i = 0; OB_SUCC(ret) && i < BASIC_PLAN_TABLE_COLUMN_CNT; i++) {
      if (OB_FAIL(BUF_PRINTF("%-*s", format_helper.column_len_.at(i),
          ExplainColumnName[i]))) { /* Do nothing */
      } else {
        ret = BUF_PRINTF(COLUMN_SEPARATOR);
      }
    }
    if (OB_SUCC(ret)) {
      ret = BUF_PRINTF(NEW_LINE);
    } else { /* Do nothing */ }

    // print line separator
    for (int i = 0; OB_SUCC(ret) && i < format_helper.total_len_; i++) {
      ret = BUF_PRINTF(LINE_SEPARATOR);
    }
    if (OB_SUCC(ret)) {
      ret = BUF_PRINTF(NEW_LINE);
    } else { /* Do nothing */ }
    for (int64_t i = 0; OB_SUCC(ret) && i < sql_plan_infos.count(); ++i) {
      ObSqlPlanItem *plan_item = sql_plan_infos.at(i);
      if (OB_ISNULL(plan_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null plan item", K(ret));
      }
      //ID
      if (OB_SUCC(ret)) {
        ret = BUF_PRINTF(COLUMN_SEPARATOR);
        ret = BUF_PRINTF("%-*d",
                         format_helper.column_len_.at(Id),
                         plan_item->id_);
      }
      //OPERATOR
      if (OB_SUCC(ret)) {
        ret = BUF_PRINTF(COLUMN_SEPARATOR);
      }
      if (OB_SUCC(ret) && format_helper.operator_prefix_.at(i).length() > 0) {
        ret = BUF_PRINTF("%*s",
                         format_helper.operator_prefix_.at(i).length(),
                         format_helper.operator_prefix_.at(i).ptr());
      }
      if (OB_SUCC(ret) && plan_item->operation_len_ > 0) {
        ret = BUF_PRINTF("%-*.*s",
                         format_helper.column_len_.at(Operator) -
                         plan_item->depth_ * 2,
                         static_cast<int>(plan_item->operation_len_),
                         plan_item->operation_);
      }
      //NAME
      if (OB_SUCC(ret)) {
        ret = BUF_PRINTF(COLUMN_SEPARATOR);
      }
      if (OB_SUCC(ret) && plan_item->object_alias_len_ > 0) {
        ret = BUF_PRINTF("%-*.*s",
                         format_helper.column_len_.at(Name),
                         static_cast<int>(plan_item->object_alias_len_),
                         plan_item->object_alias_);
        ret = BUF_PRINTF(COLUMN_SEPARATOR);
      } else if (OB_SUCC(ret)) {
        ret = BUF_PRINTF("%-*s",
                         format_helper.column_len_.at(Name),
                         "");
        ret = BUF_PRINTF(COLUMN_SEPARATOR);
      }
      if (OB_SUCC(ret))  {
        ret = BUF_PRINTF(NEW_LINE);
      }
    }
    // print plan text footer line
    for (int i = 0; OB_SUCC(ret) && i < format_helper.total_len_; i++) {
      ret = BUF_PRINTF(PLAN_WRAPPER);
    }
    if (OB_SUCC(ret))  {
      ret = BUF_PRINTF(NEW_LINE);
    }
  }
  return ret;
}

int ObSqlPlan::format_plan_table(ObIArray<ObSqlPlanItem*> &sql_plan_infos,
                                 const ObExplainDisplayOpt& option,
                                 PlanText &plan_text)
{
  int ret = OB_SUCCESS;
  ObSqlPlan::PlanFormatHelper format_helper;
  if (option.with_real_info_) {
    if (OB_FAIL(format_real_plan_table(sql_plan_infos,
                                       option,
                                       plan_text))) {
      LOG_WARN("failed to format real plan table", K(ret));
    }
  } else if (OB_FAIL(get_plan_table_formatter(sql_plan_infos,
                                       option,
                                       format_helper))) {
    LOG_WARN("failed to get plan table formatter", K(ret));
  } else {
    BEGIN_BUF_PRINT;
    for (int64_t i = 0; OB_SUCC(ret) && i < PLAN_TABLE_COLUMN_CNT; ++i) {
      format_helper.total_len_ += format_helper.column_len_.at(i);
    }
    format_helper.total_len_ += PLAN_TABLE_COLUMN_CNT+1;
    // print plan text header line
    for (int i = 0; OB_SUCC(ret) && i < format_helper.total_len_; i++) {
      ret = BUF_PRINTF(PLAN_WRAPPER);
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
    } else {
      ret = BUF_PRINTF(COLUMN_SEPARATOR);
    }
    // print column n~ames
    for (int i = 0; OB_SUCC(ret) && i < PLAN_TABLE_COLUMN_CNT; i++) {
      if (OB_FAIL(BUF_PRINTF("%-*s", format_helper.column_len_.at(i),
          ExplainColumnName[i]))) {
      } else {
        ret = BUF_PRINTF(COLUMN_SEPARATOR);
      }
    }
    if (OB_SUCC(ret)) {
      ret = BUF_PRINTF(NEW_LINE);
    } else { /* Do nothing */ }

    // print line separator
    for (int i = 0; OB_SUCC(ret) && i < format_helper.total_len_; i++) {
      ret = BUF_PRINTF(LINE_SEPARATOR);
    }
    if (OB_SUCC(ret)) {
      ret = BUF_PRINTF(NEW_LINE);
    } else { /* Do nothing */ }
    for (int64_t i = 0; OB_SUCC(ret) && i < sql_plan_infos.count(); ++i) {
      ObSqlPlanItem *plan_item = sql_plan_infos.at(i);
      if (OB_ISNULL(plan_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null plan item", K(ret));
      }
      //ID
      if (OB_SUCC(ret)) {
        ret = BUF_PRINTF(COLUMN_SEPARATOR);
        ret = BUF_PRINTF("%-*d",
                         format_helper.column_len_.at(Id),
                         plan_item->id_);
      }
      //OPERATOR
      if (OB_SUCC(ret)) {
        ret = BUF_PRINTF(COLUMN_SEPARATOR);
      }
      if (OB_SUCC(ret) && format_helper.operator_prefix_.at(i).length() > 0) {
        ret = BUF_PRINTF("%*s",
                         format_helper.operator_prefix_.at(i).length(),
                         format_helper.operator_prefix_.at(i).ptr());
      }
      if (OB_SUCC(ret) && plan_item->operation_len_ > 0) {
        ret = BUF_PRINTF("%-*.*s",
                         format_helper.column_len_.at(Operator) -
                         plan_item->depth_ * 2,
                         static_cast<int>(plan_item->operation_len_),
                         plan_item->operation_);
      }
      //NAME
      if (OB_SUCC(ret)) {
        ret = BUF_PRINTF(COLUMN_SEPARATOR);
      }
      if (OB_SUCC(ret) && plan_item->object_alias_len_ > 0) {
        ret = BUF_PRINTF("%-*.*s",
                         format_helper.column_len_.at(Name),
                         static_cast<int>(plan_item->object_alias_len_),
                         plan_item->object_alias_);
      } else if (OB_SUCC(ret)) {
        ret = BUF_PRINTF("%-*s",
                         format_helper.column_len_.at(Name),
                         "");
      }
      //ROWS
      if (OB_SUCC(ret)) {
        ret = BUF_PRINTF(COLUMN_SEPARATOR);
        ret = BUF_PRINTF("%-*ld",
                         format_helper.column_len_.at(EstRows),
                         plan_item->cardinality_);
      }
      //COST
      if (OB_SUCC(ret)) {
        ret = BUF_PRINTF(COLUMN_SEPARATOR);
        ret = BUF_PRINTF("%-*ld",
                         format_helper.column_len_.at(EstCost),
                         plan_item->cost_);
        ret = BUF_PRINTF(COLUMN_SEPARATOR);
      }
      if (OB_SUCC(ret))  {
        ret = BUF_PRINTF(NEW_LINE);
      }
    }
    // print plan text footer line
    for (int i = 0; OB_SUCC(ret) && i < format_helper.total_len_; i++) {
      ret = BUF_PRINTF(PLAN_WRAPPER);
    }
    if (OB_SUCC(ret))  {
      ret = BUF_PRINTF(NEW_LINE);
    }
  }
  return ret;
}

int ObSqlPlan::format_real_plan_table(ObIArray<ObSqlPlanItem*> &sql_plan_infos,
                                      const ObExplainDisplayOpt& option,
                                      PlanText &plan_text)
{
  int ret = OB_SUCCESS;
  ObSqlPlan::PlanFormatHelper format_helper;
  if (OB_FAIL(get_real_plan_table_formatter(sql_plan_infos,
                                            option,
                                            format_helper))) {
    LOG_WARN("failed to get plan table formatter", K(ret));
  } else {
    BEGIN_BUF_PRINT;
    for (int64_t i = 0; OB_SUCC(ret) && i < REAL_PLAN_TABLE_COLUMN_CNT; ++i) {
      format_helper.total_len_ += format_helper.column_len_.at(i);
    }
    format_helper.total_len_ += REAL_PLAN_TABLE_COLUMN_CNT+1;
    // print plan text header line
    for (int i = 0; OB_SUCC(ret) && i < format_helper.total_len_; i++) {
      ret = BUF_PRINTF(PLAN_WRAPPER);
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
    } else {
      ret = BUF_PRINTF(COLUMN_SEPARATOR);
    }
    // print column n~ames
    for (int i = 0; OB_SUCC(ret) && i < REAL_PLAN_TABLE_COLUMN_CNT; i++) {
      if (OB_FAIL(BUF_PRINTF("%-*s", format_helper.column_len_.at(i),
          ExplainColumnName[i]))) {
      } else {
        ret = BUF_PRINTF(COLUMN_SEPARATOR);
      }
    }
    if (OB_SUCC(ret)) {
      ret = BUF_PRINTF(NEW_LINE);
    } else { /* Do nothing */ }

    // print line separator
    for (int i = 0; OB_SUCC(ret) && i < format_helper.total_len_; i++) {
      ret = BUF_PRINTF(LINE_SEPARATOR);
    }
    if (OB_SUCC(ret)) {
      ret = BUF_PRINTF(NEW_LINE);
    } else { /* Do nothing */ }
    for (int64_t i = 0; OB_SUCC(ret) && i < sql_plan_infos.count(); ++i) {
      ObSqlPlanItem *plan_item = sql_plan_infos.at(i);
      if (OB_ISNULL(plan_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null plan item", K(ret));
      }
      //ID
      if (OB_SUCC(ret)) {
        ret = BUF_PRINTF(COLUMN_SEPARATOR);
        ret = BUF_PRINTF("%-*d",
                         format_helper.column_len_.at(Id),
                         plan_item->id_);
      }
      //OPERATOR
      if (OB_SUCC(ret)) {
        ret = BUF_PRINTF(COLUMN_SEPARATOR);
      }
      if (OB_SUCC(ret) && format_helper.operator_prefix_.at(i).length() > 0) {
        ret = BUF_PRINTF("%*s",
                         format_helper.operator_prefix_.at(i).length(),
                         format_helper.operator_prefix_.at(i).ptr());
      }
      if (OB_SUCC(ret) && plan_item->operation_len_ > 0) {
        ret = BUF_PRINTF("%-*.*s",
                         format_helper.column_len_.at(Operator) -
                         plan_item->depth_ * 2,
                         static_cast<int>(plan_item->operation_len_),
                         plan_item->operation_);
      }
      //NAME
      if (OB_SUCC(ret)) {
        ret = BUF_PRINTF(COLUMN_SEPARATOR);
      }
      if (OB_SUCC(ret) && plan_item->object_alias_len_ > 0) {
        ret = BUF_PRINTF("%-*.*s",
                         format_helper.column_len_.at(Name),
                         static_cast<int>(plan_item->object_alias_len_),
                         plan_item->object_alias_);
      } else if (OB_SUCC(ret)) {
        ret = BUF_PRINTF("%-*s",
                         format_helper.column_len_.at(Name),
                         "");
      }
      //EST ROWS
      if (OB_SUCC(ret)) {
        ret = BUF_PRINTF(COLUMN_SEPARATOR);
        ret = BUF_PRINTF("%-*ld",
                         format_helper.column_len_.at(EstRows),
                         plan_item->cardinality_);
      }
      //EST COST
      if (OB_SUCC(ret)) {
        ret = BUF_PRINTF(COLUMN_SEPARATOR);
        ret = BUF_PRINTF("%-*ld",
                         format_helper.column_len_.at(EstCost),
                         plan_item->cost_);
      }
      //REAL ROWS
      if (OB_SUCC(ret)) {
        ret = BUF_PRINTF(COLUMN_SEPARATOR);
        ret = BUF_PRINTF("%-*ld",
                         format_helper.column_len_.at(RealRows),
                         plan_item->real_cardinality_);
      }
      //REAL COST
      if (OB_SUCC(ret)) {
        ret = BUF_PRINTF(COLUMN_SEPARATOR);
        ret = BUF_PRINTF("%-*ld",
                         format_helper.column_len_.at(RealCost),
                         plan_item->real_cost_);
      }
      //IO COST
      if (OB_SUCC(ret)) {
        ret = BUF_PRINTF(COLUMN_SEPARATOR);
        ret = BUF_PRINTF("%-*ld",
                         format_helper.column_len_.at(IoTime),
                         plan_item->io_cost_);
      }
      //CPU COST
      if (OB_SUCC(ret)) {
        ret = BUF_PRINTF(COLUMN_SEPARATOR);
        ret = BUF_PRINTF("%-*ld",
                         format_helper.column_len_.at(CpuTime),
                         plan_item->cpu_cost_);
        ret = BUF_PRINTF(COLUMN_SEPARATOR);
      }
      if (OB_SUCC(ret))  {
        ret = BUF_PRINTF(NEW_LINE);
      }
    }
    // print plan text footer line
    for (int i = 0; OB_SUCC(ret) && i < format_helper.total_len_; i++) {
      ret = BUF_PRINTF(PLAN_WRAPPER);
    }
    if (OB_SUCC(ret))  {
      ret = BUF_PRINTF(NEW_LINE);
    }
  }
  return ret;
}

int ObSqlPlan::format_plan_output(ObIArray<ObSqlPlanItem*> &sql_plan_infos, PlanText &plan_text)
{
  int ret = OB_SUCCESS;
  BEGIN_BUF_PRINT;
  if (sql_plan_infos.empty()) {
    //do nothing
  } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
  } else if (OB_FAIL(BUF_PRINTF("Outputs & filters:"))) {
  } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
  } else if (OB_FAIL(BUF_PRINTF(SEPARATOR))) {
  } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < sql_plan_infos.count(); ++i) {
    ObSqlPlanItem *plan_item = sql_plan_infos.at(i);
    int line_begin_pos = pos;
    //print output info
    if (OB_ISNULL(plan_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null plan item", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("%3ld - ", i))) {
      //do nothing
    } else if (plan_item->projection_len_ <= 0 &&
               OB_FAIL(BUF_PRINTF("output(nil)"))) {
    } else if (plan_item->projection_len_ > 0 &&
               OB_FAIL(format_one_output_expr(buf,
                                              buf_len,
                                              pos,
                                              line_begin_pos,
                                              plan_item->projection_,
                                              plan_item->projection_len_))) {
    } else if (OB_FAIL(BUF_PRINTF(", "))) {
    } else if (plan_item->filter_predicates_len_ <= 0 &&
               OB_FAIL(BUF_PRINTF("filter(nil)"))) {
    } else if (plan_item->filter_predicates_len_ > 0 &&
               OB_FAIL(format_one_output_expr(buf,
                                              buf_len,
                                              pos,
                                              line_begin_pos,
                                              plan_item->filter_predicates_,
                                              plan_item->filter_predicates_len_))) {
    } else if (plan_item->startup_predicates_len_ <= 0) {
    } else if (OB_FAIL(BUF_PRINTF(", "))) {
    } else if (OB_FAIL(format_one_output_expr(buf,
                                              buf_len,
                                              pos,
                                              line_begin_pos,
                                              plan_item->startup_predicates_,
                                              plan_item->startup_predicates_len_))) {
    }
    if (OB_FAIL(ret)) {
    } else if (plan_item->rowset_ <= 1) {
      //do nothing
    } else if (OB_FAIL(BUF_PRINTF(", "))) {
    } else if (OB_FAIL(BUF_PRINTF("rowset=%ld", plan_item->rowset_))) {
    }
    //print access info
    if (OB_FAIL(ret)) {
    } else if (plan_item->partition_start_len_ <= 0 &&
               plan_item->access_predicates_len_ <= 0) {
    } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
    } else if (OB_FALSE_IT(line_begin_pos=pos)) {
    } else if (OB_FAIL(BUF_PRINTF(OUTPUT_PREFIX))) {
    } else if (plan_item->access_predicates_len_ <= 0 &&
               OB_FAIL(BUF_PRINTF("access(nil)"))) {
    } else if (plan_item->access_predicates_len_ > 0 &&
               OB_FAIL(format_one_output_expr(buf,
                                              buf_len,
                                              pos,
                                              line_begin_pos,
                                              plan_item->access_predicates_,
                                              plan_item->access_predicates_len_))) {
    } else if (plan_item->partition_start_len_ <= 0) {
      //do nothing
    } else if (OB_FAIL(BUF_PRINTF(", "))) {
    } else if (OB_FAIL(format_one_output_expr(buf,
                                              buf_len,
                                              pos,
                                              line_begin_pos,
                                              plan_item->partition_start_,
                                              plan_item->partition_start_len_))) {
    }
    //print special info
    if (OB_FAIL(ret)) {
    } else if (plan_item->special_predicates_len_ <= 0) {
    } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
    } else if (OB_FALSE_IT(line_begin_pos=pos)) {
    } else if (OB_FAIL(BUF_PRINTF(OUTPUT_PREFIX))) {
    } else if (OB_FAIL(format_one_output_expr(buf,
                                              buf_len,
                                              pos,
                                              line_begin_pos,
                                              plan_item->special_predicates_,
                                              plan_item->special_predicates_len_))) {
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
    }
  }
  return ret;
}

bool ObSqlPlan::is_exchange_out_operator(ObSqlPlanItem *item)
{
  bool b_ret = false;
  ObString exch_out = "EXCHANGE OUT";
  if (NULL == item) {
    //do nothing
  } else if (NULL == item->operation_) {
    //do nothing
  } else if (item->operation_len_ < exch_out.length()) {
    //do nothing
  } else {
    b_ret = true;
    for (int64_t i = 0; b_ret && i < exch_out.length(); ++i) {
      if (item->operation_[i] != exch_out.ptr()[i]) {
        b_ret = false;
      }
    }
  }
  return b_ret;
}

int ObSqlPlan::format_one_output_expr(char *buf,
                                      int64_t buf_len,
                                      int64_t &pos,
                                      int &line_begin_pos,
                                      const char* expr_info,
                                      int expr_len)
{
  int ret = OB_SUCCESS;
  const int MAX_LINE_LENGTH = 150;
  const int MAX_LENGTH = 200;
  int last_pos = 0;
  int print_len = pos - line_begin_pos;
  bool need_new_line = false;
  while (OB_SUCC(ret) && expr_len > last_pos) {
    if (0 == print_len && need_new_line) {
      if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
      } else if (OB_FAIL(BUF_PRINTF(OUTPUT_PREFIX))) {
      } else {
        line_begin_pos = pos;
      }
    }
    if (OB_FAIL(ret)) {
    } else if ('\n' == *(expr_info + last_pos)) {
      need_new_line = false;
      print_len = 0;
    } else if (print_len > MAX_LINE_LENGTH) {
      if (' ' == *(expr_info + last_pos) ||
          ',' == *(expr_info + last_pos) ||
          print_len >= MAX_LENGTH) {
        need_new_line = true;
        print_len = 0;
      } else {
        ++print_len;
      }
    } else {
      ++print_len;
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(BUF_PRINTF("%.*s",
                                  1,
                                  expr_info + last_pos))) {
    } else {
      ++last_pos;
    }
  }
  return ret;
}

int ObSqlPlan::format_used_hint(ObIArray<ObSqlPlanItem*> &sql_plan_infos, PlanText &plan_text)
{
  int ret = OB_SUCCESS;
  BEGIN_BUF_PRINT;
  if (sql_plan_infos.empty()) {
    //do nothing
  } else if (OB_ISNULL(sql_plan_infos.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null plan item", K(ret));
  } else if (sql_plan_infos.at(0)->other_tag_len_ <= 0) {
    //do nothing
  } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
  } else if (OB_FAIL(BUF_PRINTF("Used Hint:"))) {
  } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
  } else if (OB_FAIL(BUF_PRINTF(SEPARATOR))) {
  } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
  } else if (OB_FAIL(BUF_PRINTF("%.*s",
                                static_cast<int>(sql_plan_infos.at(0)->other_tag_len_),
                                sql_plan_infos.at(0)->other_tag_))) {
  }
  return ret;
}

int ObSqlPlan::format_qb_name_trace(ObIArray<ObSqlPlanItem*> &sql_plan_infos, PlanText &plan_text)
{
  int ret = OB_SUCCESS;
  BEGIN_BUF_PRINT;
  if (sql_plan_infos.empty()) {
    //do nothing
  } else if (OB_ISNULL(sql_plan_infos.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null plan item", K(ret));
  } else if (sql_plan_infos.at(0)->remarks_len_ <= 0) {
    //do nothing
  } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
  } else if (OB_FAIL(BUF_PRINTF("Qb name trace:"))) {
  } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
  } else if (OB_FAIL(BUF_PRINTF(SEPARATOR))) {
  } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
  } else if (OB_FAIL(BUF_PRINTF("%.*s",
                                static_cast<int>(sql_plan_infos.at(0)->remarks_len_),
                                sql_plan_infos.at(0)->remarks_))) {
  }
  return ret;
}

int ObSqlPlan::format_outline(ObIArray<ObSqlPlanItem*> &sql_plan_infos, PlanText &plan_text)
{
  int ret = OB_SUCCESS;
  BEGIN_BUF_PRINT;
  if (sql_plan_infos.empty()) {
    //do nothing
  } else if (OB_ISNULL(sql_plan_infos.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null plan item", K(ret));
  } else if (sql_plan_infos.at(0)->other_xml_len_ <= 0) {
    //do nothing
  } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
  } else if (OB_FAIL(BUF_PRINTF("Outline Data: "))) {
  } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
  } else if (OB_FAIL(BUF_PRINTF(SEPARATOR))) {
  } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
  } else if (OB_FAIL(BUF_PRINTF("%.*s",
                                static_cast<int>(sql_plan_infos.at(0)->other_xml_len_),
                                sql_plan_infos.at(0)->other_xml_))) {
  }
  return ret;
}

int ObSqlPlan::format_optimizer_info(ObIArray<ObSqlPlanItem*> &sql_plan_infos, PlanText &plan_text)
{
  int ret = OB_SUCCESS;
  BEGIN_BUF_PRINT;
  if (sql_plan_infos.empty()) {
    //do nothing
  } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
  } else if (OB_FAIL(BUF_PRINTF("Optimization Info:"))) {
  } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
  } else if (OB_FAIL(BUF_PRINTF(SEPARATOR))) {
  } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < sql_plan_infos.count(); ++i) {
    if (OB_ISNULL(sql_plan_infos.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null plan item", K(ret));
    } else if (sql_plan_infos.at(i)->optimizer_len_ <= 0) {
      //do nothing
    } else if (OB_FAIL(BUF_PRINTF("%.*s",
                                  static_cast<int>(sql_plan_infos.at(i)->optimizer_len_),
                                  sql_plan_infos.at(i)->optimizer_))) {
    } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
    }
  }
  return ret;
}

int ObSqlPlan::format_other_info(ObIArray<ObSqlPlanItem*> &sql_plan_infos, PlanText &plan_text)
{
  int ret = OB_SUCCESS;
  BEGIN_BUF_PRINT;
  if (sql_plan_infos.empty()) {
    //do nothing
  } else if (OB_ISNULL(sql_plan_infos.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null plan item", K(ret));
  } else if (sql_plan_infos.at(0)->other_len_ <= 0) {
    //do nothing
  } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
  } else if (OB_FAIL(BUF_PRINTF("%.*s",
                                static_cast<int>(sql_plan_infos.at(0)->other_len_),
                                sql_plan_infos.at(0)->other_))) {
  }
  return ret;
}

int ObSqlPlan::format_plan_to_json(ObIArray<ObSqlPlanItem*> &sql_plan_infos, PlanText &plan_text)
{
  int ret = OB_SUCCESS;
  json::Value *ret_val = NULL;
  if (OB_FAIL(inner_format_plan_to_json(sql_plan_infos, 0, ret_val))) {
    LOG_WARN("failed to format plan to json", K(ret));
  } else {
    json::Tidy tidy(ret_val);
    plan_text.pos_ += tidy.to_string(plan_text.buf_ + plan_text.pos_,
                                     plan_text.buf_len_ - plan_text.pos_);
    if (plan_text.buf_len_ - plan_text.pos_ > 0) {
      plan_text.buf_[plan_text.pos_ + 1] = '\0';
    } else {
      plan_text.buf_[plan_text.buf_len_ - 1] = '\0';
    }
  }
  return ret;
}

int ObSqlPlan::inner_format_plan_to_json(ObIArray<ObSqlPlanItem*> &sql_plan_infos,
                                        int64_t info_idx,
                                        json::Value *&ret_val)
{
  int ret = OB_SUCCESS;
  ret_val = NULL;
  ObSqlPlanItem *plan_item = NULL;
  if (info_idx < 0 || info_idx >= sql_plan_infos.count() ||
      OB_ISNULL(plan_item=sql_plan_infos.at(info_idx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect info idx", K(ret));
  } else {
    ObIAllocator *allocator = &allocator_;
    json::Pair *id = NULL;
    json::Pair *op = NULL;
    json::Pair *name = NULL;
    json::Pair *rows = NULL;
    json::Pair *cost = NULL;
    json::Pair *output = NULL;

    Value *id_value = NULL;
    Value *op_value = NULL;
    Value *name_value = NULL;
    Value *rows_value = NULL;
    Value *cost_value = NULL;
    Value *output_value = NULL;
    if (OB_ISNULL(ret_val = (Value *)allocator->alloc(sizeof(Value)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no memory");
    } else if (OB_ISNULL(id = (Pair *)allocator->alloc(sizeof(Pair)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no memory");
    } else if (OB_ISNULL(op = (Pair *)allocator->alloc(sizeof(Pair)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no memory");
    } else if (OB_ISNULL(name = (Pair *)allocator->alloc(sizeof(Pair)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no memory");
    } else if (OB_ISNULL(rows = (Pair *)allocator->alloc(sizeof(Pair)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no memory");
    } else if (OB_ISNULL(cost = (Pair *)allocator->alloc(sizeof(Pair)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no memory");
    } else if (OB_ISNULL(output = (Pair *)allocator->alloc(sizeof(Pair)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no memory");
    } else if (OB_ISNULL(id_value = (Value *)allocator->alloc(sizeof(Value)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no memory");
    } else if (OB_ISNULL(op_value = (Value *)allocator->alloc(sizeof(Value)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no memory");
    } else if (OB_ISNULL(name_value = (Value *)allocator->alloc(sizeof(Value)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no memory");
    } else if (OB_ISNULL(rows_value = (Value *)allocator->alloc(sizeof(Value)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no memory");
    } else if (OB_ISNULL(cost_value = (Value *)allocator->alloc(sizeof(Value)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no memory");
    } else if (OB_ISNULL(output_value = (Value *)allocator->alloc(sizeof(Value)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no memory");
    } else if (OB_ISNULL(ret_val = new(ret_val) Value())) {
      ret = OB_ERROR;
      LOG_WARN("failed to new json Value");
    } else if (OB_ISNULL(id = new(id) Pair())) {
      ret = OB_ERROR;
      LOG_WARN("failed to new json Pair");
    } else if (OB_ISNULL(op = new(op) Pair())) {
      ret = OB_ERROR;
      LOG_WARN("failed to new json Pair");
    } else if (OB_ISNULL(name = new(name) Pair())) {
      ret = OB_ERROR;
      LOG_WARN("failed to new json Pair");
    } else if (OB_ISNULL(rows = new(rows) Pair())) {
      ret = OB_ERROR;
      LOG_WARN("failed to new json Pair");
    } else if (OB_ISNULL(cost = new(cost) Pair())) {
      ret = OB_ERROR;
      LOG_WARN("failed to new json Pair");
    } else if (OB_ISNULL(output = new(output) Pair())) {
      ret = OB_ERROR;
      LOG_WARN("failed to new json Pair");
    } else if (OB_ISNULL(id_value = new(id_value) Value())) {
      ret = OB_ERROR;
      LOG_WARN("failed to new json Value");
    } else if (OB_ISNULL(op_value = new(op_value) Value())) {
      ret = OB_ERROR;
      LOG_WARN("failed to new json Value");
    } else if (OB_ISNULL(name_value = new(name_value) Value())) {
      ret = OB_ERROR;
      LOG_WARN("failed to new json Value");
    } else if (OB_ISNULL(rows_value = new(rows_value) Value())) {
      ret = OB_ERROR;
      LOG_WARN("failed to new json Value");
    } else if (OB_ISNULL(cost_value = new(cost_value) Value())) {
      ret = OB_ERROR;
      LOG_WARN("failed to new json Value");
    } else if (OB_ISNULL(output_value = new(output_value) Value())) {
      ret = OB_ERROR;
      LOG_WARN("failed to new json Value");
    } else {
      ret_val->set_type(JT_OBJECT);
      id_value->set_type(JT_NUMBER);
      // TBD
      id_value->set_int(plan_item->id_);
      id->name_ = ExplainColumnName[Id];
      id->value_ = id_value;

      op_value->set_type(JT_STRING);
      // TBD
      op_value->set_string(plan_item->operation_,
                           plan_item->operation_len_);
      op->name_ = ExplainColumnName[Operator];
      op->value_ = op_value;

      name_value->set_type(JT_STRING);
      // TBD
      name_value->set_string(plan_item->object_alias_,
                             plan_item->object_alias_len_);
      name->name_ = ExplainColumnName[Name];
      name->value_ = name_value;

      rows_value->set_type(JT_NUMBER);
      // TBD
      rows_value->set_int(plan_item->cardinality_);
      rows->name_ = ExplainColumnName[EstRows];
      rows->value_ = rows_value;

      cost_value->set_type(JT_NUMBER);
      // TBD
      cost_value->set_int(plan_item->cost_);
      cost->name_ = ExplainColumnName[EstCost];
      cost->value_ = cost_value;

      output_value->set_type(JT_STRING);
      output_value->set_string(plan_item->projection_,
                               plan_item->projection_len_);

      output->name_ = "output";
      output->value_ = output_value;
      ret_val->object_add(id);
      ret_val->object_add(op);
      ret_val->object_add(name);
      ret_val->object_add(rows);
      ret_val->object_add(cost);
      ret_val->object_add(output);
      // child operator
      Pair *child = NULL;
      const uint64_t OB_MAX_JSON_CHILD_NAME_LENGTH = 64;
      char name_buf[OB_MAX_JSON_CHILD_NAME_LENGTH];
      int64_t name_buf_size = OB_MAX_JSON_CHILD_NAME_LENGTH;
      for (int64_t i = 0; OB_SUCC(ret) && i < sql_plan_infos.count(); ++i) {
        ObSqlPlanItem *child_plan = sql_plan_infos.at(i);
        if (OB_ISNULL(child_plan)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect null child plan", K(ret));
        } else if (plan_item->id_ != child_plan->parent_id_) {
          //do nothing
        } else {
          int64_t child_name_pos = snprintf(name_buf, name_buf_size, "CHILD_%d", child_plan->position_);
          ObString child_name(child_name_pos, name_buf);
          if (OB_ISNULL(child = (Pair *)allocator->alloc(sizeof(Pair)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_ERROR("no memory");
          } else if (OB_ISNULL(child = new(child) Pair())) {
            ret = OB_ERROR;
            LOG_WARN("failed to new json Pair");
          } else if (OB_FAIL(ob_write_string(*allocator, child_name, child->name_))) {
            LOG_WARN("failed to write string", K(ret));
            /* Do nothing */
          } else if (OB_FAIL(SMART_CALL(inner_format_plan_to_json(sql_plan_infos,
                                                                  i,
                                                                  child->value_)))) {
            LOG_WARN("to_json fails", K(ret), K(i));
          } else {
            ret_val->object_add(child);
          }
        }
      }
    }
  }
  return ret;
}

int ObSqlPlan::init_buffer(PlanText &plan_text)
{
  int ret = OB_SUCCESS;
  plan_text.buf_len_ = 1024 * 1024;
  plan_text.buf_ = static_cast<char*>(allocator_.alloc(plan_text.buf_len_));
  if (OB_ISNULL(plan_text.buf_)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Failed to allocate buffer", "buffer size", plan_text.buf_len_, K(ret));
  }
  return ret;
}

void ObSqlPlan::destroy_buffer(PlanText &plan_text)
{
  if (NULL != plan_text.buf_) {
    allocator_.free(plan_text.buf_);
  }
}

int ObSqlPlan::refine_buffer(PlanText &plan_text)
{
  int ret = OB_SUCCESS;
  int64_t new_len = plan_text.pos_;
  char* new_buf = static_cast<char*>(allocator_.alloc(new_len));
  if (OB_ISNULL(new_buf)) {
    //do nothing
  } else {
    MEMCPY(new_buf, plan_text.buf_, new_len);
    destroy_buffer(plan_text);
    plan_text.buf_ = new_buf;
    plan_text.buf_len_ = new_len;
    plan_text.pos_ = new_len;
  }
  return ret;
}

int ObSqlPlan::plan_text_to_string(PlanText &plan_text,
                                  common::ObString &plan_str)
{
  int ret = OB_SUCCESS;
  plan_str = ObString(plan_text.pos_, plan_text.buf_);
  return ret;
}

int ObSqlPlan::plan_text_to_strings(PlanText &plan_text,
                                    ObIArray<common::ObString> &plan_strs)
{
  int ret = OB_SUCCESS;
  int64_t last_pos = 0;
  const char line_stop_symbol = '\n';
  for (int64_t i = 0; OB_SUCC(ret) && i < plan_text.pos_; ++i) {
    if (plan_text.buf_[i] != line_stop_symbol) {
      //keep going
    } else if (i > last_pos &&
               OB_FAIL(plan_strs.push_back(ObString(i - last_pos,
                                                    plan_text.buf_ + last_pos)))) {
      LOG_WARN("failed to push back plan text", K(ret));
    } else {
      last_pos = i + 1;
    }
  }
  return ret;
}

int ObSqlPlan::prepare_and_store_session(ObSQLSessionInfo *session,
                                        ObSQLSessionInfo::StmtSavedValue *&session_value,
                                        transaction::ObTxDesc *&tx_desc,
                                        int64_t &nested_count)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  session_value = NULL;
  tx_desc = NULL;
  nested_count = 0;
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected session value", K(ret));
  } else if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSQLSessionInfo::StmtSavedValue)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory for saved session value", K(ret));
  } else {
    session_value = new(ptr) sql::ObSQLSessionInfo::StmtSavedValue();
    if (OB_FAIL(session->save_session(*session_value))) {
      LOG_WARN("failed to save session", K(ret));
    } else {
      nested_count = session->get_nested_count();
      session->set_query_start_time(ObTimeUtility::current_time());
      session->set_inner_session();
      session->set_nested_count(-1);
      //write plan to plan table
      session->set_autocommit(true);
      tx_desc = session->get_tx_desc();
      session->get_tx_desc() = NULL;
    }
  }
  return ret;
}

int ObSqlPlan::restore_session(ObSQLSessionInfo *session,
                              ObSQLSessionInfo::StmtSavedValue *&session_value,
                              transaction::ObTxDesc *tx_desc,
                              int64_t nested_count)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session) || OB_ISNULL(session_value)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected session value or saved session value", K(ret));
  } else if (OB_FAIL(session->restore_session(*session_value))) {
    LOG_WARN("failed to restore session", K(ret));
  } else {
    transaction::ObTxDesc *new_tx_desc = session->get_tx_desc();
    session->set_nested_count(nested_count);
    session->get_tx_desc() = tx_desc;
    session_value->reset();
    allocator_.free(session_value);
    session_value = 0;
    // release curr
    if (OB_NOT_NULL(new_tx_desc)) {
      auto txs = MTL(transaction::ObTransService*);
      if (OB_ISNULL(txs)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("can not acquire MTL TransService", KR(ret));
        new_tx_desc->dump_and_print_trace();
      } else if (OB_FAIL(txs->release_tx(*new_tx_desc))) {
        LOG_WARN("failed to release tx desc", K(ret));
      }
    }
  }
  return ret;
}

} // end of namespace sql
} // end of namespace oceanbase
