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
#include "lib/json/ob_json.h"
#include "sql/optimizer/ob_explain_log_plan.h"
#include "sql/optimizer/ob_log_operator_factory.h"
#include "sql/optimizer/ob_log_plan_factory.h"
#include "sql/optimizer/ob_log_values.h"
#include "sql/code_generator/ob_code_generator.h"
#include "sql/monitor/ob_sql_plan.h"

using namespace oceanbase;
using namespace sql;
using namespace oceanbase::common;
using namespace oceanbase::json;
using namespace oceanbase::sql::log_op_def;

/**
 *  This function does two things:
 *
 *  1. generate a logical plan for the 'real' query that's being 'explained'
 *  2. generate the plan text from the logical plan and put the text in the buffer
 *     and remember the logical plan as well.
 */
int ObExplainLogPlan::generate_normal_raw_plan()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_stmt()) || OB_UNLIKELY(!get_stmt()->is_explain_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(get_stmt()), K(ret));
  } else {
    ObLogPlan *child_plan = NULL;
    const ObDMLStmt *child_stmt = NULL;
    ObLogicalOperator *top = NULL;
    ObLogValues *values_op = NULL;
    int64_t batch_size = 0;
    const ObExplainStmt *explain_stmt = static_cast<const ObExplainStmt*>(get_stmt());
    if (OB_ISNULL(child_stmt = explain_stmt->get_explain_query_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_ISNULL(child_plan = optimizer_context_.get_log_plan_factory().
                         create(optimizer_context_, *child_stmt))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to create log plan for explain stmt");
    } else if (OB_FAIL(child_plan->generate_plan())) {
      LOG_WARN("failed to generate plan tree for explain", K(ret));
    } else if (OB_FAIL(check_explain_generate_plan_with_outline(child_plan))) {
      LOG_WARN("failed to check generate plan with outline for explain", K(ret));
    } else if (OB_FAIL(ObCodeGenerator::detect_batch_size(*child_plan, batch_size))) {
      LOG_WARN("detect batch size failed", K(ret));
    } else if (OB_FAIL(allocate_values_as_top(top))) {
      LOG_WARN("failed to allocate expr values_op as top", K(ret));
    } else if (OB_ISNULL(top) || OB_UNLIKELY(LOG_VALUES != top->get_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(top), K(ret));
    } else {
      set_plan_root(top);
      top->mark_is_plan_root();
      child_plan->get_optimizer_context().set_batch_size(batch_size);
      values_op = static_cast<ObLogValues*>(top);
      values_op->set_explain_plan(child_plan);
      ObSqlPlan sql_plan(get_allocator());
      sql_plan.set_session_info(get_optimizer_context().get_session_info());
      ObExplainLogPlan *explain_plan = static_cast<ObExplainLogPlan*>(child_plan);
      ObSEArray<common::ObString, 64> plan_strs;
      const ObString& into_table = explain_stmt->get_into_table();
      const ObString& statement_id = explain_stmt->get_statement_id();
      if (OB_FAIL(sql_plan.store_sql_plan_for_explain(get_optimizer_context().get_exec_ctx(),
                                                      child_plan,
                                                      explain_stmt->get_explain_type(),
                                                      0 == into_table.length() ? "PLAN_TABLE" : into_table,
                                                      0 == statement_id.length() ? "" : statement_id,
                                                      explain_stmt->get_display_opt(),
                                                      plan_strs))) {
        LOG_WARN("failed to store sql plan", K(ret));
      } else {
        //For explain stmt, we can do pack at the stage of expr alloc,
        //But we need to use the FALSE flag to tell the driver
        //to use normal encoding instead of memcopy
        optimizer_context_.set_packed(false);
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < plan_strs.count(); ++i) {
        ObObj obj;
        obj.set_varchar(plan_strs.at(i));
        ObNewRow row;
        row.cells_ = &obj;
        row.count_ = 1;
        if (OB_FAIL(values_op->add_row(row))) {
          LOG_WARN("failed to add row", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        // set values_op operator id && set max operator id for LogPlan
        values_op->set_op_id(0);
        set_max_op_id(1);
      }
    }
    get_optimizer_context().get_all_exprs().reuse();
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(allocate_output_expr_for_values_op(*values_op))) {
      LOG_WARN("failed ot allocate output expr", K(ret));
    } else {
      get_optimizer_context().set_plan_type(ObPhyPlanType::OB_PHY_PLAN_LOCAL,
                                            ObPhyPlanType::OB_PHY_PLAN_LOCAL,
                                            false);
    }
  }
  return ret;
}

int ObExplainLogPlan::check_explain_generate_plan_with_outline(ObLogPlan *real_plan)
{
  int ret = OB_SUCCESS;
  ObExecContext *exec_ctx = NULL;
  ObSqlCtx *sql_ctx = NULL;
  ObSQLSessionInfo *session_info = NULL;
  const ObExplainStmt *explain_stmt = static_cast<const ObExplainStmt*>(get_stmt());
  if (OB_ISNULL(real_plan) || OB_ISNULL(explain_stmt)
      || OB_ISNULL(exec_ctx = get_optimizer_context().get_exec_ctx())
      || OB_ISNULL(session_info = get_optimizer_context().get_session_info())
      || OB_ISNULL(sql_ctx = exec_ctx->get_sql_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(real_plan), K(explain_stmt), K(session_info), K(exec_ctx), K(sql_ctx));
  } else if (session_info->is_inner() || sql_ctx->is_prepare_protocol_) {
    /* do not check explain for inner sql (include query in PL) */
  } else if (OB_UNLIKELY(get_optimizer_context().get_query_ctx() != NULL
                         && get_optimizer_context().get_query_ctx()->get_injected_random_status())) {
    /* do nothing */
  } else if (sql_ctx->multi_stmt_item_.is_part_of_multi_stmt()
             && sql_ctx->multi_stmt_item_.get_seq_num() > 0) {
    /* generate plan call by ObMPQuery::process_with_tmp_context use tmp context, do not check */
  } else if (EXPLAIN_UNINITIALIZED !=  explain_stmt->get_explain_type()
             && EXPLAIN_BASIC !=  explain_stmt->get_explain_type()
             && EXPLAIN_OUTLINE !=  explain_stmt->get_explain_type()) {
    /* generate plan again for explain/explain basic/explain outline,
      do not check explain extended/explain extended_noaddr */
  } else if (0 == sql_ctx->first_plan_hash_) {  /* generate plan first time */
    void *tmp_ptr = NULL;
    sql_ctx->first_outline_data_.reset();
    if (OB_UNLIKELY(0 == real_plan->get_signature()) || 0 < sql_ctx->retry_times_) {
      /* do nothing */
    } else if (OB_SUCC(OB_E(EventTable::EN_EXPLAIN_GENERATE_PLAN_WITH_OUTLINE) OB_SUCCESS)) {
      /* do nothing */
    } else if (OB_UNLIKELY(NULL == (tmp_ptr = get_optimizer_context().get_allocator().alloc(OB_MAX_SQL_LENGTH)))) {
      /* allocator in optimizer context is from ObResultSet */
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("fail to alloc memory", K(ret));
    } else {
      PlanText plan_text;
      plan_text.buf_ = static_cast<char *>(tmp_ptr);
      plan_text.buf_len_ = OB_MAX_SQL_LENGTH;
      if (OB_FAIL(ObSqlPlan::get_plan_outline_info_one_line(plan_text, real_plan))) {
        LOG_WARN("failed to get plan outline info", K(ret));
      } else {
        sql_ctx->first_outline_data_.assign_ptr(plan_text.buf_, static_cast<ObString::obstr_size_t>(plan_text.pos_));
        sql_ctx->first_plan_hash_ = real_plan->get_signature();
        ret = OB_SQL_RETRY_SPM;
        LOG_WARN("generate plan again for explain use outline", K(ret));
      }
    }
  } else {  /* check generate plan again use outline data */
    SMART_VAR(char[OB_MAX_SQL_LENGTH], buf) {
      PlanText plan_text;
      plan_text.buf_ = buf;
      plan_text.buf_len_ = OB_MAX_SQL_LENGTH;
      if (OB_FAIL(ObSqlPlan::get_plan_outline_info_one_line(plan_text, real_plan))) {
        LOG_WARN("failed to get plan outline info", K(ret));
      } else {
        ObString cur_outline_data;
        const uint64_t cur_plan_hash = real_plan->get_signature();
        cur_outline_data.assign_ptr(plan_text.buf_, static_cast<ObString::obstr_size_t>(plan_text.pos_));
        if (cur_plan_hash != sql_ctx->first_plan_hash_) {
          ret = OB_OUTLINE_NOT_REPRODUCIBLE;
          LOG_WARN("failed to generate plan use outline", K(sql_ctx->first_plan_hash_));
        }
        if (0 != (cur_outline_data.case_compare(sql_ctx->first_outline_data_))) {
          ret = OB_OUTLINE_NOT_REPRODUCIBLE;
          LOG_WARN("failed to generate plan use outline", K(sql_ctx->first_outline_data_));
        }
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to generate plan use outline", K(cur_plan_hash), K(cur_outline_data));
        }
      }
      sql_ctx->first_plan_hash_ = 0;
      sql_ctx->first_outline_data_.reset();
    }
  }
  return ret;
}
