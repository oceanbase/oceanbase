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
#include "sql/optimizer/ob_log_link.h"
#include "sql/ob_sql_utils.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::sql::log_op_def;

namespace oceanbase
{
namespace sql
{

ObLogLink::ObLogLink(ObLogPlan &plan)
  : ObLogicalOperator(plan),
    allocator_(plan.get_allocator()),
    stmt_fmt_buf_(NULL),
    stmt_fmt_len_(0),
    is_reverse_link_(false),
    tm_dblink_id_(OB_INVALID_ID),
    param_infos_()
{}

int ObLogLink::compute_sharding_info()
{
  int ret = OB_SUCCESS;
  ObOptimizerContext *opt_ctx = NULL;
  if (OB_ISNULL(get_plan()) ||
      OB_ISNULL(opt_ctx = &get_plan()->get_optimizer_context())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    strong_sharding_ = opt_ctx->get_local_sharding();
  }
  return ret;
}

int ObLogLink::compute_op_parallel_and_server_info()
{
  int ret = OB_SUCCESS;
  server_list_.reuse();
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(server_list_.push_back(get_plan()->get_optimizer_context().get_local_server_addr()))) {
    LOG_WARN("failed to assign das path server list", K(ret));
  } else {
    set_parallel(1);
    set_server_cnt(1);
  }
  return ret;
}

int ObLogLink::est_cost()
{
  int ret = OB_SUCCESS;
  card_ = 0;
  op_cost_ = 0;
  cost_ = 0;
  width_ = 0;
  return ret;
}

int ObLogLink::print_link_stmt(char *buf, int64_t buf_len)
{
  int ret = OB_SUCCESS;
  if (stmt_fmt_len_ > buf_len) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("print link stmt failed", K(ret), K(stmt_fmt_len_), K(buf_len));
  } else {
    MEMCPY(buf, stmt_fmt_buf_, stmt_fmt_len_);
    char *ch = buf;
    char *stmt_end = buf + stmt_fmt_len_ - 3;
    while (ch < stmt_end) {
      if (0 == ch[0] && 0 == ch[1]) {
        uint16_t param_idx = *(uint16_t *)(ch + 2);
        ch[0] = '$';
        if (param_idx > 999) {
          ch[1] = 'M';
          ch[2] = 'A';
          ch[3] = 'X';
        } else {
          ch[3] = static_cast<char>('0' + param_idx % 10);
          param_idx /= 10;
          ch[2] = static_cast<char>('0' + param_idx % 10);
          param_idx /= 10;
          ch[1] = static_cast<char>('0' + param_idx % 10);
        }
        ch += 4;
      } else {
        ch++;
      }
    }
  }
  return ret;
}

int ObLogLink::get_plan_item_info(PlanText &plan_text,
                                  ObSqlPlanItem &plan_item)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObLogicalOperator::get_plan_item_info(plan_text, plan_item))) {
    LOG_WARN("failed to get plan item info", K(ret));
  } else {
    BEGIN_BUF_PRINT;
    if (false && OB_FAIL(BUF_PRINTF(",dblink_id=%lu,", get_dblink_id()))) { // explain basic will print dlbink id, dblink id will change every time when case run
      LOG_WARN("BUF_PRINTF failed", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("link_stmt="))) {
      LOG_WARN("BUF_PRINTF failed", K(ret));
    } else if (OB_FAIL(print_link_stmt(buf + pos, buf_len - pos))) {
      LOG_WARN("failed to print link stmt", K(ret));
    } else {
      pos += stmt_fmt_len_;
    }
    END_BUF_PRINT(plan_item.special_predicates_,
                  plan_item.special_predicates_len_);
  }
  return ret;
}

// see comment in ObConstRawExpr::get_name_internal().
int ObLogLink::gen_link_stmt_param_infos()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt_fmt_buf_) || stmt_fmt_len_ <= 0) {
    ret = OB_NOT_INIT;
    LOG_WARN("stmt fmt is not inited", KP(stmt_fmt_buf_), K(stmt_fmt_len_));
  } else {
    param_infos_.reset();
  }
  int64_t param_pos = 0;
  int64_t param_idx = 0;
  const int64_t param_len = ObLinkStmtParam::get_param_len();
  while (OB_SUCC(ret) && param_idx >= 0) {
    int8_t type_value;
    if (OB_FAIL(ObLinkStmtParam::read_next(stmt_fmt_buf_, stmt_fmt_len_, param_pos, param_idx, type_value))) {
      LOG_WARN("failed to read next param", K(ret));
    } else if (param_idx < 0) {
      // skip.
    } else if (OB_FAIL(param_infos_.push_back(ObParamPosIdx(static_cast<int32_t>(param_pos),
                                                            static_cast<int32_t>(param_idx),
                                                            type_value)))) {
      LOG_WARN("failed to push back param pos idx", K(ret), K(param_pos), K(param_idx), K(type_value));
    } else {
      param_pos += param_len;
    }
  }
  return ret;
}

int ObLogLink::mark_exec_params(ObDMLStmt *stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else {
    common::ObIArray<ObQueryRefRawExpr*> &subquery_exprs = stmt->get_subquery_exprs();
    for (int64_t i = 0; OB_SUCC(ret) && i < subquery_exprs.count(); i ++) {
      ObQueryRefRawExpr *expr = subquery_exprs.at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < expr->get_exec_params().count(); j ++) {
          ObExecParamRawExpr *param_expr = NULL;
          if (OB_ISNULL(param_expr = expr->get_exec_params().at(j))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null", K(ret));
          } else {
            param_expr->set_ref_same_dblink(true);
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObSEArray<ObSelectStmt *, 4> child_stmts;
    if (OB_FAIL(stmt->get_child_stmts(child_stmts))) {
      LOG_WARN("failed to get child stmts", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); i ++) {
      OZ(SMART_CALL(mark_exec_params(child_stmts.at(i))));
    }
  }
  return ret;
}

int ObLogLink::set_link_stmt(const ObDMLStmt* stmt)
{
  int ret = OB_SUCCESS;
  const ObLogPlan *plan = get_plan();
  if (NULL == stmt) {
    stmt = get_stmt();
  }
  ObString sql;
  ObObjPrintParams print_param;
  print_param.for_dblink_ = 1;
  // only link scan need print flashback query for dblink table
  ObOptimizerContext *opt_ctx = NULL;
  ObSQLSessionInfo *session = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(plan) ||
      OB_ISNULL(opt_ctx = &get_plan()->get_optimizer_context()) ||
      OB_ISNULL(session = opt_ctx->get_session_info()) ||
      OB_ISNULL(print_param.exec_ctx_ = opt_ctx->get_exec_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", KP(opt_ctx), KP(stmt), KP(session), KP(plan), K(ret));
  } else if (OB_FAIL(mark_exec_params(const_cast<ObDMLStmt*>(stmt)))) {
    LOG_WARN("failed to mark exec params", K(ret));
  } else if (OB_FAIL(ObSQLUtils::reconstruct_sql(plan->get_allocator(), stmt, sql, opt_ctx->get_schema_guard(), print_param, NULL, session))) {
    LOG_WARN("failed to reconstruct link sql", KP(stmt), KP(plan), K(get_dblink_id()), K(ret));
  } else {
    stmt_fmt_buf_ = sql.ptr();
    stmt_fmt_len_ = sql.length();
    LOG_DEBUG("loglink succ to reconstruct link sql", K(sql));
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
