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

#define USING_LOG_PREFIX SQL_ENG
#include "ob_dbms_xplan.h"
#include "observer/ob_inner_sql_connection_pool.h"
#include "sql/ob_spi.h"

namespace oceanbase
{
using namespace sql;
using namespace common;
using namespace share;
using namespace observer;
using namespace sqlclient;
namespace pl {
/**
 * @brief ObDbmsXplan::enable_opt_trace
 * @param ctx
 * @param params
 *      sql_id      IN VARCHAR2,
 *      identifier  IN VARCHAR2 DEFAULT ''
 * @param result
 * @return
 */
int ObDbmsXplan::enable_opt_trace(ObExecContext &ctx, ParamStore &params, ObObj &result)
{
  int ret = OB_SUCCESS;
  UNUSED(result);
  ObString sql_id;
  ObString identifier;
  number::ObNumber level_num;
  int64_t level;
  int idx = 0;
  ObSQLSessionInfo *session = ctx.get_my_session();
  if (3 != params.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect four params", K(ret));
  } else if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null session", K(ret));
  } else if (OB_FAIL(params.at(idx++).get_varchar(sql_id))) {
    LOG_WARN("failed to get sql string", K(ret));
  } else if (OB_FAIL(params.at(idx++).get_varchar(identifier))) {
    LOG_WARN("failed to get identified", K(ret));
  } else if (OB_FAIL(params.at(idx++).get_number(level_num))) {
    LOG_WARN("failed to get number value", K(ret));
  } else if (OB_FAIL(level_num.cast_to_int64(level))) {
    LOG_WARN("failed to cast int", K(ret));
  } else if (OB_FALSE_IT(session->get_optimizer_tracer().set_session_info(session))) {
  } else if (OB_FAIL(session->get_optimizer_tracer().enable_trace(identifier,
                                                                  sql_id,
                                                                  level))) {
    LOG_WARN("failed to enable optimizer tracer", K(ret));
  }
  return ret;
}

/**
 * @brief ObDbmsXplan::disable_opt_trace
 * @param ctx
 * @param params
 * @param result
 * @return
 */
int ObDbmsXplan::disable_opt_trace(ObExecContext &ctx, ParamStore &params, ObObj &result)
{
  int ret = OB_SUCCESS;
  UNUSED(result);
  ObSQLSessionInfo *session = ctx.get_my_session();
  if (0 != params.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect four params", K(ret));
  } else if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null session", K(ret));
  } else {
    session->get_optimizer_tracer().set_enable(false);
  }
  return ret;
}

/**
 * @brief ObDbmsXplan::set_opt_trace_parameter
 * @param ctx
 * @param params
 *      sql_id      IN VARCHAR2,
 *      identifier  IN VARCHAR2 DEFAULT ''
 * @param result
 * @return
 */
int ObDbmsXplan::set_opt_trace_parameter(ObExecContext &ctx, ParamStore &params, ObObj &result)
{
  int ret = OB_SUCCESS;
  UNUSED(result);
  ObString sql_id;
  ObString identifier;
  number::ObNumber level_num;
  int64_t level;
  ObSQLSessionInfo *session = ctx.get_my_session();
  int idx = 0;
  if (3 != params.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect four params", K(ret));
  } else if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null session", K(ret));
  } else if (OB_FAIL(params.at(idx++).get_varchar(sql_id))) {
    LOG_WARN("failed to get sql string", K(ret));
  } else if (OB_FAIL(params.at(idx++).get_varchar(identifier))) {
    LOG_WARN("failed to get identified", K(ret));
  } else if (OB_FAIL(params.at(idx++).get_number(level_num))) {
    LOG_WARN("failed to get number value", K(ret));
  } else if (OB_FAIL(level_num.cast_to_int64(level))) {
    LOG_WARN("failed to cast int", K(ret));
  } else if (OB_FAIL(session->get_optimizer_tracer().set_parameters(identifier,
                                                                    sql_id,
                                                                    level))) {
    LOG_WARN("failed to init optimizer tracer", K(ret));
  }
  return ret;
}

int ObDbmsXplan::display(sql::ObExecContext &ctx,
                        sql::ParamStore &params,
                        common::ObObj &result)
{
  int ret = OB_SUCCESS;
  ObString table_name;
  ObString statement_id;
  ObString format;
  ObString filter_preds;
  ObSQLSessionInfo *session = ctx.get_my_session();
  int idx = 0;
  if (4 != params.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect four params", K(ret));
  } else if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null session", K(ret));
  } else if (OB_FAIL(params.at(idx++).get_varchar(format))) {
    LOG_WARN("failed to get format", K(ret));
  } else if (OB_FAIL(params.at(idx++).get_varchar(statement_id))) {
    LOG_WARN("failed to get statement id", K(ret));
  } else if (OB_FAIL(params.at(idx++).get_varchar(table_name))) {
    LOG_WARN("failed to get table name", K(ret));
  } else if (OB_FAIL(params.at(idx++).get_varchar(filter_preds))) {
    LOG_WARN("failed to get filter preds", K(ret));
  } else {
    PlanText plan_text;
    ExplainType type;
    ObExplainDisplayOpt option;
    int last_id = -1;
    bool alloc_buffer = true;
    ObSqlPlan sql_plan(ctx.get_allocator());
    sql_plan.set_session_info(session);
    ObSEArray<ObSqlPlanItem*, 4> plan_infos;
    ObSEArray<ObSqlPlanItem*, 4> cur_plan_infos;
    if (OB_FAIL(get_plan_info_by_plan_table(ctx,
                                            table_name,
                                            statement_id,
                                            filter_preds,
                                            plan_infos))) {
      LOG_WARN("failed to get plan info", K(ret));
    } else if (OB_FAIL(get_plan_format(format, type, option))) {
      LOG_WARN("failed to get plan format type", K(ret));
    } else if (OB_FALSE_IT(option.with_real_info_ = false)) {
    }
    for (int i = 0; OB_SUCC(ret) && i < plan_infos.count(); ++i) {
      ObSqlPlanItem *item = plan_infos.at(i);
      if (OB_ISNULL(item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null plan item", K(ret));
      } else if (item->id_ <= last_id) {
        //new plan
        if (OB_FAIL(sql_plan.format_sql_plan(cur_plan_infos,
                                            type,
                                            option,
                                            plan_text,
                                            alloc_buffer))) {
          LOG_WARN("failed to format sql plan", K(ret));
        } else {
          cur_plan_infos.reuse();
          alloc_buffer = false;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(cur_plan_infos.push_back(item))) {
        LOG_WARN("failed to push back plan item", K(ret));
      } else {
        last_id = item->id_;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sql_plan.format_sql_plan(cur_plan_infos,
                                                type,
                                                option,
                                                plan_text,
                                                alloc_buffer))) {
      LOG_WARN("failed to format sql plan", K(ret));
    } else if (OB_FAIL(set_display_result(ctx, plan_text, result))) {
      LOG_WARN("failed to convert plan text to string", K(ret));
    }
  }
  return ret;
}

int ObDbmsXplan::display_cursor(sql::ObExecContext &ctx,
                                sql::ParamStore &params,
                                common::ObObj &result)
{
  int ret = OB_SUCCESS;
  ObString sql_id;
  ObString svr_ip;
  ObString format;
  int64_t plan_id;
  int64_t tenant_id = 0;
  int64_t svr_port = 0;
  number::ObNumber num_val;
  ObSQLSessionInfo *session = ctx.get_my_session();
  int idx = 0;
  if (5 != params.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect four params", K(ret));
  } else if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null session", K(ret));
  } else if (OB_FAIL(params.at(idx++).get_number(num_val))) {
    LOG_WARN("failed to get number value", K(ret));
  } else if (OB_FAIL(num_val.cast_to_int64(plan_id))) {
    LOG_WARN("failed to cast int", K(ret));
  } else if (OB_FAIL(params.at(idx++).get_varchar(format))) {
    LOG_WARN("failed to get format", K(ret));
  } else if (OB_FAIL(params.at(idx++).get_varchar(svr_ip))) {
    LOG_WARN("failed to get sql id", K(ret));
  } else if (OB_FAIL(params.at(idx++).get_number(num_val))) {
    LOG_WARN("failed to get number value", K(ret));
  } else if (OB_FAIL(num_val.cast_to_int64(svr_port))) {
    LOG_WARN("failed to cast int", K(ret));
  } else if (OB_FAIL(params.at(idx++).get_number(num_val))) {
    LOG_WARN("failed to get number value", K(ret));
  } else if (OB_FAIL(num_val.cast_to_int64(tenant_id))) {
    LOG_WARN("failed to cast int", K(ret));
  } else {
    if (0 == plan_id) {
      plan_id = session->get_last_plan_id();
    }
    if (0 == tenant_id) {
      tenant_id = session->get_effective_tenant_id();
    }
    PlanText plan_text;
    ExplainType type;
    ObExplainDisplayOpt option;
    ObSqlPlan sql_plan(ctx.get_allocator());
    sql_plan.set_session_info(session);
    ObSEArray<ObSqlPlanItem*, 4> plan_infos;
    if (0 == svr_ip.length() &&
        OB_FAIL(get_server_ip_port(ctx, svr_ip, svr_port))) {
      LOG_WARN("failed to get svr ip and port", K(ret));
    } else if (OB_FAIL(get_plan_info_by_id(ctx,
                                           tenant_id,
                                           svr_ip,
                                           svr_port,
                                           plan_id,
                                           plan_infos))) {
      LOG_WARN("failed to get plan info", K(ret));
    } else if (OB_FAIL(get_plan_format(format, type, option))) {
      LOG_WARN("failed to get plan format type", K(ret));
    } else if (OB_FAIL(sql_plan.format_sql_plan(plan_infos,
                                                type,
                                                option,
                                                plan_text))) {
      LOG_WARN("failed to format sql plan", K(ret));
    } else if (OB_FAIL(set_display_result(ctx, plan_text, result))) {
      LOG_WARN("failed to convert plan text to string", K(ret));
    }
  }
  return ret;
}

int ObDbmsXplan::display_sql_plan_baseline(sql::ObExecContext &ctx,
                                          sql::ParamStore &params,
                                          common::ObObj &result)
{
  int ret = OB_SUCCESS;
  ObString sql_handle;
  ObString plan_name;
  number::ObNumber num_val;
  uint64_t plan_hash = 0;
  ObString format;
  int64_t tenant_id = 0;
  ObString svr_ip;
  int64_t svr_port;
  ObSQLSessionInfo *session = ctx.get_my_session();
  int idx = 0;
  if (6 != params.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect four params", K(ret));
  } else if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null session", K(ret));
  } else if (OB_FAIL(params.at(idx++).get_varchar(sql_handle))) {
    LOG_WARN("failed to get sql string", K(ret));
  } else if (OB_FAIL(params.at(idx++).get_varchar(plan_name))) {
    LOG_WARN("failed to get plan name", K(ret));
  } else if (OB_FAIL(num_val.from(plan_name.ptr(),
                                  plan_name.length(),
                                  ctx.get_allocator()))) {
    ret = OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE;
    LOG_WARN("failed to get plan hash", K(ret));
    ObString msg = "plan_name";
    LOG_USER_ERROR(OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE, msg.length(), msg.ptr());
  } else if (!num_val.is_valid_uint64(plan_hash)) {
    ret = OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE;
    LOG_WARN("failed to get uint64 value", K(ret));
    ObString msg = "plan_name";
    LOG_USER_ERROR(OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE, msg.length(), msg.ptr());
  } else if (OB_FAIL(params.at(idx++).get_varchar(format))) {
    LOG_WARN("failed to get format", K(ret));
  } else if (OB_FAIL(params.at(idx++).get_varchar(svr_ip))) {
    LOG_WARN("failed to get sql id", K(ret));
  } else if (OB_FAIL(params.at(idx++).get_number(num_val))) {
    LOG_WARN("failed to get number value", K(ret));
  } else if (OB_FAIL(num_val.cast_to_int64(svr_port))) {
    LOG_WARN("failed to cast int", K(ret));
  } else if (OB_FAIL(params.at(idx++).get_number(num_val))) {
    LOG_WARN("failed to get number value", K(ret));
  } else if (OB_FAIL(num_val.cast_to_int64(tenant_id))) {
    LOG_WARN("failed to cast int", K(ret));
  } else {
    if (0 == tenant_id) {
      tenant_id = session->get_effective_tenant_id();
    }
    PlanText plan_text;
    ExplainType type;
    ObExplainDisplayOpt option;
    ObSqlPlan sql_plan(ctx.get_allocator());
    sql_plan.set_session_info(session);
    ObSEArray<ObSqlPlanItem*, 4> plan_infos;
    if (0 == svr_ip.length() &&
        OB_FAIL(get_server_ip_port(ctx, svr_ip, svr_port))) {
      LOG_WARN("failed to get svr ip and port", K(ret));
    } else if (OB_FAIL(get_baseline_plan_info(ctx,
                                              tenant_id,
                                              svr_ip,
                                              svr_port,
                                              sql_handle,
                                              plan_hash,
                                              plan_infos))) {
      LOG_WARN("failed to get plan info", K(ret));
    } else {
      if (OB_FAIL(get_plan_format(format, type, option))) {
        LOG_WARN("failed to get plan format type", K(ret));
      } else if (OB_FAIL(sql_plan.format_sql_plan(plan_infos,
                                                  type,
                                                  option,
                                                  plan_text))) {
        LOG_WARN("failed to format sql plan", K(ret));
      } else if (EXPLAIN_EXTENDED == type &&
                 OB_FAIL(get_baseline_plan_detail(ctx,
                                                  sql_handle,
                                                  plan_name,
                                                  tenant_id,
                                                  plan_text,
                                                  true))) {
        LOG_WARN("failed to get baseline plan detail", K(ret));
      } else if (OB_FAIL(set_display_result(ctx, plan_text, result))) {
        LOG_WARN("failed to convert plan text to string", K(ret));
      }
    }
  }
  return ret;
}

int ObDbmsXplan::display_active_session_plan(sql::ObExecContext &ctx,
                                            sql::ParamStore &params,
                                            common::ObObj &result)
{
  int ret = OB_SUCCESS;
  number::ObNumber num_val;
  int64_t session_id = 0;
  ObString format;
  ObString svr_ip;
  int64_t svr_port;
  ObSQLSessionInfo *session = ctx.get_my_session();
  int idx = 0;
  if (4 != params.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect four params", K(ret));
  } else if (OB_FAIL(params.at(idx++).get_number(num_val))) {
    LOG_WARN("failed to get number value", K(ret));
  } else if (OB_FAIL(num_val.cast_to_int64(session_id))) {
    LOG_WARN("failed to cast int", K(ret));
  } else if (OB_FAIL(params.at(idx++).get_varchar(format))) {
    LOG_WARN("failed to get format", K(ret));
  } else if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null session", K(ret));
  } else if (OB_FAIL(params.at(idx++).get_varchar(svr_ip))) {
    LOG_WARN("failed to get sql id", K(ret));
  } else if (OB_FAIL(params.at(idx++).get_number(num_val))) {
    LOG_WARN("failed to get number value", K(ret));
  } else if (OB_FAIL(num_val.cast_to_int64(svr_port))) {
    LOG_WARN("failed to cast int", K(ret));
  } else {
    PlanText plan_text;
    ExplainType type;
    ObExplainDisplayOpt option;
    ObSqlPlan sql_plan(ctx.get_allocator());
    sql_plan.set_session_info(session);
    ObSEArray<ObSqlPlanItem*, 4> plan_infos;
    if (0 == svr_ip.length() &&
        OB_FAIL(get_server_ip_port(ctx, svr_ip, svr_port))) {
      LOG_WARN("failed to get svr ip and port", K(ret));
    } else if (OB_FAIL(get_plan_info_by_session_id(ctx,
                                                  session_id,
                                                  svr_ip,
                                                  svr_port,
                                                  session->get_effective_tenant_id(),
                                                  plan_infos))) {
      LOG_WARN("failed to get plan info", K(ret));
    } else if (OB_FAIL(get_plan_format(format, type, option))) {
      LOG_WARN("failed to get plan format type", K(ret));
    } else if (OB_FAIL(sql_plan.format_sql_plan(plan_infos,
                                                type,
                                                option,
                                                plan_text))) {
      LOG_WARN("failed to format sql plan", K(ret));
    } else if (OB_FAIL(set_display_result(ctx, plan_text, result))) {
      LOG_WARN("failed to convert plan text to string", K(ret));
    }
  }
  return ret;
}

int ObDbmsXplan::get_server_ip_port(sql::ObExecContext &ctx,
                                    ObString &svr_ip,
                                    int64_t &svr_port)
{
  int ret = OB_SUCCESS;
  const ObAddr &addr = GCTX.self_addr();
  svr_port = addr.get_port();
  char ip_buf[OB_IP_STR_BUFF] = {'\0'};
  if (!addr.ip_to_string(ip_buf, sizeof(ip_buf))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ip to string failed", K(ret));
  } else {
    ObString ipstr_tmp = ObString::make_string(ip_buf);
    if (OB_FAIL(ob_write_string (ctx.get_allocator(), ipstr_tmp, svr_ip))) {
      LOG_WARN("ob write string failed", K(ret));
    } else if (svr_ip.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("host ip is empty", K(ret));
    }
  }
  return ret;
}

int ObDbmsXplan::get_plan_format(const ObString &format,
                                ExplainType &type,
                                ObExplainDisplayOpt& option)
{
  int ret = OB_SUCCESS;
  option.with_color_ = false;
  option.with_tree_line_ = false;
  option.with_real_info_ = false;
  if (format.case_compare("BASIC") == 0) {
    type = EXPLAIN_BASIC;
    option.with_tree_line_ = true;
  } else if (format.case_compare("TYPICAL") == 0) {
    type = EXPLAIN_TRADITIONAL;
    option.with_tree_line_ = true;
    option.with_real_info_ = true;
  } else if (format.case_compare("ALL") == 0) {
    type = EXPLAIN_EXTENDED;
    option.with_tree_line_ = true;
    option.with_real_info_ = true;
  } else if (format.case_compare("ADVANCED") == 0) {
    type = EXPLAIN_EXTENDED;
    option.with_tree_line_ = true;
    option.with_real_info_ = true;
  }
  return ret;
}

int ObDbmsXplan::set_display_result(sql::ObExecContext &ctx,
                                    PlanText &plan_text,
                                    common::ObObj &result)
{
  int ret = OB_SUCCESS;
  if (lib::is_oracle_mode()) {
    if (OB_FAIL(set_display_result_for_oracle(ctx,
                                              plan_text,
                                              result))) {
      LOG_WARN("failed to set display result", K(ret));
    }
  } else {
    if (OB_FAIL(set_display_result_for_mysql(ctx, plan_text, result))) {
      LOG_WARN("failed to set display result", K(ret));
    }
  }
  return ret;
}

int ObDbmsXplan::set_display_result_for_oracle(sql::ObExecContext &exec_ctx,
                                              PlanText &plan_text,
                                              common::ObObj &result)
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_ORACLE_PL
  ObSEArray<ObString, 64> plan_strs;
  ObSQLSessionInfo *session = exec_ctx.get_my_session();
  if (OB_FAIL(ObSqlPlan::plan_text_to_strings(plan_text, plan_strs))) {
    LOG_WARN("failed to convert plan text to string", K(ret));
  } else if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(ret));
  } else {
    ObObj obj;
    ObSEArray<ObObj, 2> row;
    ObPLNestedTable *table = reinterpret_cast<ObPLNestedTable*>
                (exec_ctx.get_allocator().alloc(sizeof(ObPLNestedTable)));
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null param", K(ret));
    } else {
      new (table)ObPLNestedTable();
      table->set_column_count(1);
      if (OB_FAIL(table->init_allocator(exec_ctx.get_allocator(), true))) {
        LOG_WARN("failed to init allocator", K(ret));
      } else if (OB_FAIL(ObSPIService::spi_set_collection(session->get_effective_tenant_id(),
                                                  NULL,
                                                  exec_ctx.get_allocator(),
                                                  *table,
                                                  plan_strs.count()))) {
        LOG_WARN("failed to set collection size", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < plan_strs.count(); ++i) {
      row.reuse();
      obj.set_varchar(plan_strs.at(i));
      if (OB_FAIL(row.push_back(obj))) {
        LOG_WARN("failed to push back object", K(ret));
      } else if (OB_FAIL(table->set_row(row, i))) {
        LOG_WARN("failed to set row", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      table->set_first(1);
      table->set_last(plan_strs.count());
      result.set_extend(reinterpret_cast<int64_t>(table),
                        PL_NESTED_TABLE_TYPE,
                        table->get_init_size());
    }
  }
#endif //OB_BUILD_ORACLE_PL
  return ret;
}

int ObDbmsXplan::set_display_result_for_mysql(sql::ObExecContext &ctx,
                                              PlanText &plan_text,
                                              common::ObObj &result)
{
  int ret = OB_SUCCESS;
  ObString ret_str;
  ObTextStringResult text_res(ObTextType, true, &ctx.get_allocator());
  if (OB_FAIL(text_res.init(plan_text.pos_))) {
    LOG_WARN("failed to init text res", K(ret), K(text_res), K(plan_text.pos_));
  } else if (OB_FAIL(text_res.append(plan_text.buf_, plan_text.pos_))) {
    LOG_WARN("failed to append ret_str", K(ret), K(text_res));
  } else {
    text_res.get_result_buffer(ret_str);
    result.set_lob_value(ObTextType, ret_str.ptr(), ret_str.length());
    result.set_has_lob_header();
  }
  return ret;
}

int ObDbmsXplan::get_plan_info_by_plan_table(sql::ObExecContext &ctx,
                                             ObString table_name,
                                             ObString statement_id,
                                             ObString filter_preds,
                                             ObIArray<ObSqlPlanItem*> &plan_infos)
{
  int ret = OB_SUCCESS;
  UNUSED(statement_id);
  UNUSED(filter_preds);
  ObSqlString sql;
  ObSqlString filter;
  ObSqlString true_filter;
  if (OB_FAIL(true_filter.assign_fmt("1 = 1"))) {
    LOG_WARN("failed to assign string", K(ret));
  } else if (0 == filter_preds.length()) {
    if (OB_FAIL(filter.assign_fmt("PLAN_ID = (SELECT MAX(PLAN_ID) FROM %.*s)",
                                    table_name.length(),
                                    table_name.ptr()
                                    ))) {
      LOG_WARN("failed to assign string", K(ret));
    }
  } else if (0 == statement_id.length()) {
    if (OB_FAIL(filter.assign_fmt("%.*s",
                                  0 == filter_preds.length() ?
                                  (int)true_filter.length() :
                                  filter_preds.length(),
                                  0 == filter_preds.length() ?
                                  true_filter.ptr() :
                                  filter_preds.ptr()
                                  ))) {
      LOG_WARN("failed to assign string", K(ret));
    }
  } else {
    if (OB_FAIL(filter.assign_fmt("STATEMENT_ID='%.*s' AND %.*s",
                                  statement_id.length(),
                                  statement_id.ptr(),
                                  0 == filter_preds.length() ?
                                  (int)true_filter.length() :
                                  filter_preds.length(),
                                  0 == filter_preds.length() ?
                                  true_filter.ptr() :
                                  filter_preds.ptr()))) {
      LOG_WARN("failed to assign string", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(sql.assign_fmt("SELECT \
                      OPERATOR,\
                      OPTIONS,\
                      OBJECT_NODE,\
                      OBJECT_INSTANCE OBJECT_ID,\
                      OBJECT_OWNER,\
                      OBJECT_NAME,\
                      OBJECT_ALIAS,\
                      OBJECT_TYPE,\
                      OPTIMIZER,\
                      ID,\
                      PARENT_ID,\
                      DEPTH,\
                      POSITION,\
                      SEARCH_COLUMNS,\
                      IS_LAST_CHILD,\
                      COST,\
                      0 REAL_COST,\
                      CARDINALITY,\
                      0 REAL_CARDINALITY,\
                      BYTES,\
                      ROWSET,\
                      OTHER_TAG,\
                      PARTITION_START,\
                      PARTITION_STOP,\
                      PARTITION_ID,\
                      OTHER,\
                      DISTRIBUTION,\
                      CPU_COST,\
                      IO_COST,\
                      TEMP_SPACE,\
                      ACCESS_PREDICATES,\
                      FILTER_PREDICATES,\
                      STARTUP_PREDICATES,\
                      PROJECTION,\
                      SPECIAL_PREDICATES,\
                      TIME,\
                      QBLOCK_NAME,\
                      REMARKS,\
                      OTHER_XML\
                    FROM %.*s\
                    WHERE %.*s\
                    ORDER BY PLAN_ID,ID",
                    table_name.length(),
                    table_name.ptr(),
                    (int)filter.length(),
                    filter.ptr()
                    ))) {
      LOG_WARN("failed to assign string", K(ret));
    } else if (OB_FAIL(inner_get_plan_info_use_current_session(ctx, sql, plan_infos))) {
      LOG_WARN("failed to get plan info", K(ret));
    }
  }
  return ret;
}

int ObDbmsXplan::get_plan_info_by_id(sql::ObExecContext &ctx,
                                      int64_t tenant_id,
                                      const ObString &svr_ip,
                                      int64_t svr_port,
                                      uint64_t plan_id,
                                      ObIArray<ObSqlPlanItem*> &plan_infos)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_FAIL(sql.assign_fmt("SELECT \
                    OPERATOR,\
                    OPTIONS,\
                    OBJECT_NODE,\
                    OBJECT_ID,\
                    OBJECT_OWNER,\
                    OBJECT_NAME,\
                    OBJECT_ALIAS,\
                    OBJECT_TYPE,\
                    OPTIMIZER,\
                    ID,\
                    PARENT_ID,\
                    DEPTH,\
                    POSITION,\
                    SEARCH_COLUMNS,\
                    IS_LAST_CHILD,\
                    COST,\
                    REAL_COST,\
                    CARDINALITY,\
                    REAL_CARDINALITY,\
                    BYTES,\
                    ROWSET,\
                    OTHER_TAG,\
                    PARTITION_START,\
                    PARTITION_STOP,\
                    PARTITION_ID,\
                    OTHER,\
                    DISTRIBUTION,\
                    CPU_COST,\
                    IO_COST,\
                    TEMP_SPACE,\
                    ACCESS_PREDICATES,\
                    FILTER_PREDICATES,\
                    STARTUP_PREDICATES,\
                    PROJECTION,\
                    SPECIAL_PREDICATES,\
                    TIME,\
                    QBLOCK_NAME,\
                    REMARKS,\
                    OTHER_XML\
                  FROM OCEANBASE.__ALL_VIRTUAL_SQL_PLAN\
                  WHERE TENANT_ID=%ld\
                  AND SVR_IP='%.*s'\
                  AND SVR_PORT=%ld\
                  AND PLAN_ID=%lu\
                  ORDER BY ID",
                  tenant_id,
                  svr_ip.length(),
                  svr_ip.ptr(),
                  svr_port,
                  plan_id))) {
    LOG_WARN("failed to assign string", K(ret));
  } else if (OB_FAIL(inner_get_plan_info(ctx, sql, plan_infos))) {
    LOG_WARN("failed to get plan info", K(ret));
  }
  return ret;
}

int ObDbmsXplan::get_baseline_plan_info(sql::ObExecContext &ctx,
                                        int64_t tenant_id,
                                        const ObString &svr_ip,
                                        int64_t svr_port,
                                        const ObString &sql_handle,
                                        uint64_t plan_hash,
                                        ObIArray<ObSqlPlanItem*> &plan_infos)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_FAIL(sql.assign_fmt("SELECT \
                    OPERATOR,\
                    OPTIONS,\
                    OBJECT_NODE,\
                    OBJECT_ID,\
                    OBJECT_OWNER,\
                    OBJECT_NAME,\
                    OBJECT_ALIAS,\
                    OBJECT_TYPE,\
                    OPTIMIZER,\
                    ID,\
                    PARENT_ID,\
                    DEPTH,\
                    POSITION,\
                    SEARCH_COLUMNS,\
                    IS_LAST_CHILD,\
                    COST,\
                    REAL_COST,\
                    CARDINALITY,\
                    REAL_CARDINALITY,\
                    BYTES,\
                    ROWSET,\
                    OTHER_TAG,\
                    PARTITION_START,\
                    PARTITION_STOP,\
                    PARTITION_ID,\
                    OTHER,\
                    DISTRIBUTION,\
                    CPU_COST,\
                    IO_COST,\
                    TEMP_SPACE,\
                    ACCESS_PREDICATES,\
                    FILTER_PREDICATES,\
                    STARTUP_PREDICATES,\
                    PROJECTION,\
                    SPECIAL_PREDICATES,\
                    TIME,\
                    QBLOCK_NAME,\
                    REMARKS,\
                    OTHER_XML\
                  FROM OCEANBASE.__ALL_VIRTUAL_SQL_PLAN\
                  WHERE TENANT_ID=%ld\
                  AND SVR_IP='%.*s'\
                  AND SVR_PORT=%ld\
                  AND PLAN_HASH=%lu\
                  AND SQL_ID='%.*s'\
                  ORDER BY ID",
                  tenant_id,
                  svr_ip.length(),
                  svr_ip.ptr(),
                  svr_port,
                  plan_hash,
                  sql_handle.length(),
                  sql_handle.ptr()))) {
    LOG_WARN("failed to assign string", K(ret));
  } else if (OB_FAIL(inner_get_plan_info(ctx, sql, plan_infos))) {
    LOG_WARN("failed to get plan info", K(ret));
  }
  return ret;
}

int ObDbmsXplan::get_baseline_plan_detail(sql::ObExecContext &ctx,
                                          const ObString& sql_handle,
                                          const ObString& plan_name,
                                          int64_t tenant_id,
                                          PlanText &plan_text,
                                          bool from_plan_cache)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString baseline_filter;
  ObSQLSessionInfo *session = ctx.get_my_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null session", K(ret));
  } else if (is_sys_tenant(session->get_effective_tenant_id()) &&
      OB_FAIL(baseline_filter.assign_fmt("__all_virtual_plan_baseline_item where tenant_id=%ld", tenant_id))) {
    LOG_WARN("failed to assign string", K(ret));
  } else if (!is_sys_tenant(session->get_effective_tenant_id()) &&
             OB_FAIL(baseline_filter.assign_fmt("__all_plan_baseline_item"))) {
    LOG_WARN("failed to assign string", K(ret));
  } else if (OB_FAIL(sql.assign_fmt("select * from (select ORIGIN_SQL sql_text,\
                                     outline_data, \
                                     CAST(SQL_ID              AS CHAR(32)) sql_handle,\
                                     CAST(PLAN_HASH_VALUE     AS CHAR(128)) plan_name,\
                                     CAST((CASE ORIGIN WHEN 1 \
                                                       THEN 'AUTO-CAPTURE' \
                                                       WHEN 2 \
                                                       THEN 'MANUAL-LOAD' \
                                                       ELSE NULL END) AS CHAR(29)) origin,\
                                     CAST(CASE WHEN (FLAGS & 2) > 0 THEN 'YES' ELSE 'NO' END  AS CHAR(3)) accepted,\
                                     CAST(CASE WHEN (FLAGS & 4) > 0 THEN 'YES' ELSE 'NO' END  AS CHAR(3)) fixed,\
                                     CAST(CASE WHEN (FLAGS & 1) > 0 THEN 'YES' ELSE 'NO' END  AS CHAR(3)) enabled\
                              from oceanbase.%.*s) \
                              where sql_handle = '%.*s' \
                              and plan_name = '%.*s' \
                              limit 1",
                              (int)baseline_filter.length(),
                              baseline_filter.ptr(),
                              sql_handle.length(),
                              sql_handle.ptr(),
                              plan_name.length(),
                              plan_name.ptr()))) {
    LOG_WARN("failed to assign string", K(ret));
  } else if (OB_FAIL(inner_get_baseline_plan_detail(ctx,
                                                    sql,
                                                    plan_text,
                                                    from_plan_cache))) {
    LOG_WARN("failed to get plan info", K(ret));
  }
  return ret;
}

int ObDbmsXplan::inner_get_baseline_plan_detail(sql::ObExecContext &ctx,
                                                const ObSqlString& sql,
                                                PlanText &plan_text,
                                                bool from_plan_cache)
{
  int ret = OB_SUCCESS;
  common::ObISQLClient *sql_proxy = GCTX.sql_proxy_;
  if (OB_ISNULL(sql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null sql proxy", K(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObSQLSessionInfo *my_session = ctx.get_my_session();
      sqlclient::ObMySQLResult *mysql_result = NULL;
      if (OB_ISNULL(my_session)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("session is null", K(ret), K(my_session));
      } else if (OB_FAIL(sql_proxy->read(res,
                                         my_session->get_effective_tenant_id(),
                                         sql.ptr()))) {
        LOG_WARN("failed to execute recover sql", K(ret), K(sql));
      } else if (OB_ISNULL(mysql_result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("execute sql fail", K(ret), K(my_session->get_effective_tenant_id()), K(sql));
      }
      if (OB_SUCC(ret) && OB_SUCC(mysql_result->next())) {
        if (OB_FAIL(format_baseline_plan_detail(ctx,
                                                *mysql_result,
                                                plan_text,
                                                from_plan_cache))) {
          LOG_WARN("failed to format plan detail", K(ret));
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObDbmsXplan::format_baseline_plan_detail(sql::ObExecContext &ctx,
                                              sqlclient::ObMySQLResult& mysql_result,
                                              PlanText &plan_text,
                                              bool from_plan_cache)
{
  int ret = OB_SUCCESS;
  ObString varchar_val;
  ObString str_val;
  #define FORMAT_STING(str)                                                             \
  do {                                                                                  \
    str_val = str;                                                                      \
    if (OB_FAIL(ret)) {                                                                 \
    } else if (plan_text.buf_len_ - plan_text.pos_ >= str_val.length()) {               \
      int line_begin_pos = plan_text.pos_;                                              \
      if (OB_FAIL(ObSqlPlan::format_one_output_expr(plan_text.buf_,                     \
                                                    plan_text.buf_len_,                 \
                                                    plan_text.pos_,                     \
                                                    line_begin_pos,                     \
                                                    str_val.ptr(),                      \
                                                    str_val.length()))) {               \
        LOG_WARN("failed to format one string", K(ret));                                \
      }                                                                                 \
    } else {                                                                            \
      ret = OB_SIZE_OVERFLOW;                                                           \
    }                                                                                   \
  } while(0);

  #define FORMAT_VARCHAR_VALUE(IDX)                                                     \
  do {                                                                                  \
    if (OB_FAIL(ret)) {                                                                 \
    } else if (OB_FAIL(mysql_result.get_varchar((int64_t)IDX, varchar_val))) {          \
      if (OB_ERR_NULL_VALUE == ret ||                                                   \
          OB_ERR_MIN_VALUE == ret ||                                                    \
          OB_ERR_MAX_VALUE == ret) {                                                    \
        ret = OB_SUCCESS;                                                               \
      } else {                                                                          \
        LOG_WARN("failed to get varchar value", K(ret));                                \
      }                                                                                 \
    } else {                                                                            \
      FORMAT_STING(varchar_val);                                                        \
    }                                                                                   \
  } while(0);

  FORMAT_STING("SQL text:");
  FORMAT_STING(NEW_LINE);
  FORMAT_STING(SEPARATOR);
  FORMAT_STING(NEW_LINE);
  FORMAT_STING("  ");
  FORMAT_VARCHAR_VALUE(0);

  FORMAT_STING(NEW_LINE);
  FORMAT_STING("Outline data:");
  FORMAT_STING(NEW_LINE);
  FORMAT_STING(SEPARATOR);
  FORMAT_STING(NEW_LINE);
  FORMAT_STING("  ");
  FORMAT_VARCHAR_VALUE(1);

  FORMAT_STING(NEW_LINE);
  FORMAT_STING("SQL handle:");
  FORMAT_STING(NEW_LINE);
  FORMAT_STING(SEPARATOR);
  FORMAT_STING(NEW_LINE);
  FORMAT_STING("  ");
  FORMAT_VARCHAR_VALUE(2);

  FORMAT_STING(NEW_LINE);
  FORMAT_STING("Plan name:");
  FORMAT_STING(NEW_LINE);
  FORMAT_STING(SEPARATOR);
  FORMAT_STING(NEW_LINE);
  FORMAT_STING("  ");
  FORMAT_VARCHAR_VALUE(3);


  FORMAT_STING(NEW_LINE);
  FORMAT_STING("Baseline info:");
  FORMAT_STING(NEW_LINE);
  FORMAT_STING(SEPARATOR);
  FORMAT_STING(NEW_LINE);
  FORMAT_STING("  Origin: ");
  FORMAT_VARCHAR_VALUE(4);
  FORMAT_STING("    Accepted: ");
  FORMAT_VARCHAR_VALUE(5);
  FORMAT_STING("    Fixed: ");
  FORMAT_VARCHAR_VALUE(6);
  FORMAT_STING("    Enabled: ");
  FORMAT_VARCHAR_VALUE(7);
  if (from_plan_cache) {
    FORMAT_STING("\n  Plan rows: From plan cahe\n");
  } else {
    FORMAT_STING("\n  Plan rows: From outline data\n");
  }
  return ret;
}

int ObDbmsXplan::get_plan_info_by_session_id(sql::ObExecContext &ctx,
                                            int64_t session_id,
                                            const ObString &svr_ip,
                                            int64_t svr_port,
                                            int64_t tenant_id,
                                            ObIArray<ObSqlPlanItem*> &plan_infos)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString tenant_filter;
  if (is_sys_tenant(tenant_id)) {
    if (OB_FAIL(tenant_filter.assign_fmt("1 = 1"))) {
      LOG_WARN("failed to assign string", K(ret));
    }
  } else {
    if (OB_FAIL(tenant_filter.assign_fmt("A.TENANT_ID = %ld",tenant_id))) {
      LOG_WARN("failed to assign string", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(sql.assign_fmt("SELECT \
                      OPERATOR,\
                      OPTIONS,\
                      OBJECT_NODE,\
                      OBJECT_ID,\
                      OBJECT_OWNER,\
                      OBJECT_NAME,\
                      OBJECT_ALIAS,\
                      OBJECT_TYPE,\
                      OPTIMIZER,\
                      A.ID,\
                      PARENT_ID,\
                      DEPTH,\
                      POSITION,\
                      SEARCH_COLUMNS,\
                      IS_LAST_CHILD,\
                      COST,\
                      CAST(D.REAL_COST AS NUMBER(20,0)) REAL_COST,\
                      CARDINALITY,\
                      CAST(D.REAL_CARD AS NUMBER(20,0)) REAL_CARDINALITY,\
                      BYTES,\
                      ROWSET,\
                      OTHER_TAG,\
                      PARTITION_START,\
                      PARTITION_STOP,\
                      PARTITION_ID,\
                      OTHER,\
                      DISTRIBUTION,\
                      CAST(D.CPU_COST AS NUMBER(20,0)) CPU_COST,\
                      CAST(D.IO_COST AS NUMBER(20,0)) IO_COST,\
                      TEMP_SPACE,\
                      ACCESS_PREDICATES,\
                      FILTER_PREDICATES,\
                      STARTUP_PREDICATES,\
                      PROJECTION,\
                      SPECIAL_PREDICATES,\
                      TIME,\
                      QBLOCK_NAME,\
                      REMARKS,\
                      OTHER_XML\
                    FROM OCEANBASE.__ALL_VIRTUAL_SQL_PLAN A INNER JOIN\
                      (SELECT EFFECTIVE_TENANT_ID TENANT_ID,\
                                PLAN_ID \
                        FROM OCEANBASE.__ALL_VIRTUAL_PROCESSLIST \
                        WHERE ID=%ld \
                        AND SVR_IP='%.*s'\
                        AND SVR_PORT=%ld \
                        LIMIT 1) E\
                      ON A.TENANT_ID = E.TENANT_ID\
                      AND A.PLAN_ID = E.PLAN_ID\
                      LEFT JOIN\
                      (SELECT B.PLAN_LINE_ID ID, \
                              SUM(OUTPUT_ROWS) REAL_CARD,\
                              MAX(DB_TIME) CPU_COST, \
                              MAX(USER_IO_WAIT_TIME) IO_COST, \
                              (MAX(LAST_CHANGE_TIME) - MIN(FIRST_CHANGE_TIME))*1000000 REAL_COST\
                              FROM OCEANBASE.__ALL_VIRTUAL_SQL_PLAN_MONITOR B,\
                                    (SELECT TRACE_ID\
                                    FROM OCEANBASE.__ALL_VIRTUAL_PROCESSLIST \
                                    WHERE ID=%ld \
                                    AND SVR_IP='%.*s'\
                                    AND SVR_PORT=%ld \
                                    LIMIT 1) C\
                              WHERE B.TRACE_ID = C.TRACE_ID\
                              GROUP BY B.PLAN_LINE_ID) D\
                        ON A.ID = D.ID\
                    WHERE SVR_IP='%.*s'\
                    AND SVR_PORT=%ld\
                    AND %.*s\
                    ORDER BY A.ID",
                    session_id,
                    svr_ip.length(),
                    svr_ip.ptr(),
                    svr_port,
                    session_id,
                    svr_ip.length(),
                    svr_ip.ptr(),
                    svr_port,
                    svr_ip.length(),
                    svr_ip.ptr(),
                    svr_port,
                    (int)tenant_filter.length(),
                    tenant_filter.ptr()))) {
    LOG_WARN("failed to assign string", K(ret));
  } else if (OB_FAIL(inner_get_plan_info(ctx, sql, plan_infos))) {
    LOG_WARN("failed to get plan info", K(ret));
  }
  return ret;
}

int ObDbmsXplan::inner_get_plan_info(sql::ObExecContext &ctx,
                                    const ObSqlString& sql,
                                    ObIArray<ObSqlPlanItem*> &plan_infos)
{
  int ret = OB_SUCCESS;
  common::ObISQLClient *sql_proxy = GCTX.sql_proxy_;
  if (OB_ISNULL(sql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null sql proxy", K(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObSQLSessionInfo *my_session = ctx.get_my_session();
      sqlclient::ObMySQLResult *mysql_result = NULL;
      if (OB_ISNULL(my_session)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("session is null", K(ret), K(my_session));
      } else if (OB_FAIL(sql_proxy->read(res,
                                         my_session->get_effective_tenant_id(),
                                         sql.ptr()))) {
        LOG_WARN("failed to execute recover sql", K(ret), K(sql));
      } else if (OB_ISNULL(mysql_result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("execute sql fail", K(ret), K(my_session->get_effective_tenant_id()), K(sql));
      }
      while (OB_SUCC(ret) && OB_SUCC(mysql_result->next())) {
        void *buf = NULL;
        ObSqlPlanItem *plan_info = NULL;
        if (OB_ISNULL(buf=ctx.get_allocator().alloc(sizeof(ObSqlPlanItem)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(ret));
        } else {
          plan_info = new(buf)ObSqlPlanItem();
          if (OB_FAIL(read_plan_info_from_result(ctx, *mysql_result, *plan_info))) {
            LOG_WARN("failed to read plan info", K(ret));
          } else if (OB_FAIL(plan_infos.push_back(plan_info))) {
            LOG_WARN("failed to push back info", K(ret));
          }
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObDbmsXplan::inner_get_plan_info_use_current_session(sql::ObExecContext &ctx,
                                                        const ObSqlString& sql,
                                                        ObIArray<ObSqlPlanItem*> &plan_infos)
{
  int ret = OB_SUCCESS;
  ObInnerSQLConnectionPool *pool = NULL;
  ObInnerSQLConnection *conn = NULL;
  sql::ObSQLSessionInfo *session = NULL;
  if (OB_ISNULL(ctx.get_sql_proxy()) ||
      OB_ISNULL(session = ctx.get_my_session()) ||
      OB_ISNULL((pool = static_cast<ObInnerSQLConnectionPool *>(ctx.get_sql_proxy()->get_pool())))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null sql proxy", K(ret));
  } else if (OB_FAIL(pool->acquire_spi_conn(session, conn))) {
    LOG_WARN("failed to get sql connection", K(ret));
  } else if (OB_ISNULL(conn)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null sql connection", K(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *mysql_result = NULL;
      if (OB_FAIL(conn->execute_read(session->get_effective_tenant_id(), sql.ptr(), res))) {
        LOG_WARN("failed to execute recover sql", K(ret), K(sql));
      } else if (OB_ISNULL(mysql_result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("execute sql fail", K(ret));
      }
      while (OB_SUCC(ret) && OB_SUCC(mysql_result->next())) {
        void *buf = NULL;
        ObSqlPlanItem *plan_info = NULL;
        if (OB_ISNULL(buf=ctx.get_allocator().alloc(sizeof(ObSqlPlanItem)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(ret));
        } else {
          plan_info = new(buf)ObSqlPlanItem();
          if (OB_FAIL(read_plan_info_from_result(ctx, *mysql_result, *plan_info))) {
            LOG_WARN("failed to read plan info", K(ret));
          } else if (OB_FAIL(plan_infos.push_back(plan_info))) {
            LOG_WARN("failed to push back info", K(ret));
          }
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }
  }
  if (OB_NOT_NULL(conn)) {
    ctx.get_sql_proxy()->close(conn, ret);
  }
  return ret;
}

int ObDbmsXplan::read_plan_info_from_result(sql::ObExecContext &ctx,
                                            sqlclient::ObMySQLResult& mysql_result,
                                            ObSqlPlanItem &plan_info)
{
  int ret = OB_SUCCESS;
  int64_t int_value;
  ObString varchar_val;
  number::ObNumber num_val;

  #define GET_NUM_VALUE(IDX, value)                                                     \
  do {                                                                                  \
    if (OB_FAIL(ret)) {                                                                 \
    } else if (OB_FAIL(mysql_result.get_number(IDX, num_val))) {                        \
      if (OB_ERR_NULL_VALUE == ret ||                                                   \
          OB_ERR_MIN_VALUE == ret ||                                                    \
          OB_ERR_MAX_VALUE == ret) {                                                    \
        plan_info.value = 0;                                                            \
        ret = OB_SUCCESS;                                                               \
      } else {                                                                          \
        LOG_WARN("failed to get number value", K(ret));                                 \
      }                                                                                 \
    } else if (OB_FAIL(num_val.cast_to_int64(int_value))) {                             \
      LOG_WARN("failed to cast to int64", K(ret));                                      \
    } else {                                                                            \
      plan_info.value = int_value;                                                      \
    }                                                                                   \
  } while(0);

  #define GET_INT_VALUE(IDX, value)                                                     \
  do {                                                                                  \
    if (OB_FAIL(ret)) {                                                                 \
    } else if (OB_FAIL(mysql_result.get_int(IDX, int_value))) {                         \
      if (OB_ERR_NULL_VALUE == ret ||                                                   \
          OB_ERR_MIN_VALUE == ret ||                                                    \
          OB_ERR_MAX_VALUE == ret) {                                                    \
        plan_info.value = 0;                                                            \
        ret = OB_SUCCESS;                                                               \
      } else {                                                                          \
        ret = OB_SUCCESS;                                                               \
        /*retry number type*/                                                           \
        GET_NUM_VALUE(IDX, value);                                                      \
      }                                                                                 \
    } else {                                                                            \
      plan_info.value = int_value;                                                      \
    }                                                                                   \
  } while(0);

  #define GET_VARCHAR_VALUE(IDX, value)                                                 \
  do {                                                                                  \
    if (OB_FAIL(ret)) {                                                                 \
    } else if (OB_FAIL(mysql_result.get_varchar(IDX, varchar_val))) {                   \
      if (OB_ERR_NULL_VALUE == ret ||                                                   \
          OB_ERR_MIN_VALUE == ret ||                                                    \
          OB_ERR_MAX_VALUE == ret) {                                                    \
        plan_info.value = NULL;                                                         \
        plan_info.value##len_ = 0;                                                      \
        ret = OB_SUCCESS;                                                               \
      } else {                                                                          \
        LOG_WARN("failed to get varchar value", K(ret));                                \
      }                                                                                 \
    } else {                                                                            \
      char *buf = NULL;                                                                 \
      plan_info.value##len_ = varchar_val.length();                                     \
      if (0 == varchar_val.length()) {                                                  \
        plan_info.value = NULL;                                                         \
      } else if (OB_ISNULL(buf=(char*)ctx.get_allocator().alloc(varchar_val.length()))) { \
        ret = OB_ALLOCATE_MEMORY_FAILED;                                                \
        LOG_WARN("failed to allocate memory", K(ret));                                  \
      } else {                                                                          \
        MEMCPY(buf, varchar_val.ptr(), varchar_val.length());                           \
        plan_info.value = buf;                                                          \
      }                                                                                 \
    }                                                                                   \
  } while(0);
  GET_VARCHAR_VALUE(OPERATOR, operation_);
  GET_VARCHAR_VALUE(OPTIONS, options_);
  GET_VARCHAR_VALUE(OBJECT_NODE, object_node_);
  GET_INT_VALUE(OBJECT_ID, object_id_);
  GET_VARCHAR_VALUE(OBJECT_OWNER, object_owner_);
  GET_VARCHAR_VALUE(OBJECT_NAME, object_name_);
  GET_VARCHAR_VALUE(OBJECT_ALIAS, object_alias_);
  GET_VARCHAR_VALUE(OBJECT_TYPE, object_type_);
  GET_VARCHAR_VALUE(OPTIMIZER, optimizer_);
  GET_INT_VALUE(ID, id_);
  GET_INT_VALUE(PARENT_ID, parent_id_);
  GET_INT_VALUE(DEPTH, depth_);
  GET_INT_VALUE(POSITION, position_);
  GET_INT_VALUE(SEARCH_COLUMNS, search_columns_);
  GET_INT_VALUE(IS_LAST_CHILD, is_last_child_);
  GET_INT_VALUE(COST, cost_);
  GET_INT_VALUE(REAL_COST, real_cost_);
  GET_INT_VALUE(CARDINALITY, cardinality_);
  GET_INT_VALUE(REAL_CARDINALITY, real_cardinality_);
  GET_INT_VALUE(BYTES, bytes_);
  GET_INT_VALUE(ROWSET, rowset_);
  GET_VARCHAR_VALUE(OTHER_TAG, other_tag_);
  GET_VARCHAR_VALUE(PARTITION_START, partition_start_);
  GET_VARCHAR_VALUE(PARTITION_STOP, partition_stop_);
  GET_INT_VALUE(PARTITION_ID, partition_id_);
  GET_VARCHAR_VALUE(OTHER, other_);
  GET_VARCHAR_VALUE(DISTRIBUTION, distribution_);
  GET_INT_VALUE(CPU_COST, cpu_cost_);
  GET_INT_VALUE(IO_COST, io_cost_);
  GET_INT_VALUE(TEMP_SPACE, temp_space_);
  GET_VARCHAR_VALUE(ACCESS_PREDICATES, access_predicates_);
  GET_VARCHAR_VALUE(FILTER_PREDICATES, filter_predicates_);
  GET_VARCHAR_VALUE(STARTUP_PREDICATES, startup_predicates_);
  GET_VARCHAR_VALUE(PROJECTION, projection_);
  GET_VARCHAR_VALUE(SPECIAL_PREDICATES, special_predicates_);
  GET_INT_VALUE(TIME, time_);
  GET_VARCHAR_VALUE(QBLOCK_NAME, qblock_name_);
  GET_VARCHAR_VALUE(REMARKS, remarks_);
  GET_VARCHAR_VALUE(OTHER_XML, other_xml_);
  return ret;
}

}
}