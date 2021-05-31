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

#include "sql/optimizer/ob_log_temp_table_access.h"
#include "sql/optimizer/ob_opt_est_cost.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/ob_sql_utils.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/optimizer/ob_log_operator_factory.h"
#include "sql/optimizer/ob_log_plan.h"
#include "sql/optimizer/ob_join_order.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::sql::log_op_def;

ObLogTempTableAccess::ObLogTempTableAccess(ObLogPlan& plan)
    : ObLogicalOperator(plan), table_id_(0), ref_table_id_(0), is_last_access_(false), access_exprs_()
{}

ObLogTempTableAccess::~ObLogTempTableAccess()
{}

int ObLogTempTableAccess::allocate_exchange_post(AllocExchContext* ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  ObShardingInfo sharding_info;
  ObQueryCtx* query_ctx = NULL;
  ObLogicalOperator* temp_table_insert = NULL;
  if (OB_ISNULL(query_ctx = get_stmt()->query_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer.", K(ret));
  } else if (OB_FAIL(query_ctx->get_temp_table_plan(ref_table_id_, temp_table_insert))) {
    LOG_WARN("failed to get temp table sharding info.", K(ret));
  } else if (OB_FAIL(sharding_info_.copy_with_part_keys(temp_table_insert->get_sharding_info()))) {
    LOG_WARN("failed to copy sharding info", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObLogTempTableAccess::allocate_expr_post(ObAllocExprContext& ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_stmt()), K(ret));
  } else if (OB_FAIL(get_stmt()->get_column_exprs(table_id_, access_exprs_))) {
    LOG_WARN("failed to get column exprs", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < access_exprs_.count(); ++i) {
      bool expr_is_required = false;
      ObRawExpr* expr = access_exprs_.at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null expr", K(ret));
      } else if (!is_plan_root() && OB_FAIL(add_var_to_array_no_dup(output_exprs_, expr))) {
        LOG_WARN("failed to add expr", K(ret));
      } else if (OB_FAIL(mark_expr_produced(expr, branch_id_, id_, ctx, expr_is_required))) {
        LOG_WARN("failed to mark expr as produced", K(branch_id_), K(ret));
      } else { /*do nothing*/
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObLogicalOperator::allocate_expr_post(ctx))) {
        LOG_WARN("failed to allocate_expr_post", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObLogTempTableAccess::transmit_op_ordering()
{
  int ret = OB_SUCCESS;
  reset_op_ordering();
  return ret;
}

int ObLogTempTableAccess::inner_append_not_produced_exprs(ObRawExprUniqueSet& raw_exprs) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(raw_exprs.append(access_exprs_))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObLogTempTableAccess::print_my_plan_annotation(char* buf, int64_t& buf_len, int64_t& pos, ExplainType type)
{
  int ret = OB_SUCCESS;
  // print access
  if (OB_FAIL(BUF_PRINTF(", "))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  } else if (OB_FAIL(BUF_PRINTF("\n      "))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  }
  if (OB_SUCC(ret)) {
    const ObIArray<ObRawExpr*>& access = get_access_exprs();
    EXPLAIN_PRINT_EXPRS(access, type);
  }
  return ret;
}
