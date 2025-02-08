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

#define USING_LOG_PREFIX SQL_RESV

#include "sql/resolver/expr/ob_raw_expr_type_demotion.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "lib/oblog/ob_log_module.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

bool ObRawExprTypeDemotion::type_can_demote(const ObExprResType &from, const ObExprResType &to,
                                            const bool is_range)
{
  bool can_demote = false;
  if (ObRelationalExprOperator::can_cmp_without_cast(from, to, CO_EQ)) {
    // No cast expr will added for comparison, no need to demote type.
  } else if (query_ctx_->enable_constant_type_demotion_) {
    if (lib::is_mysql_mode()) {
      const ObObjType from_type = from.get_type();
      const ObObjType to_type = to.get_type();
      switch (ob_obj_type_class(to_type)) {
        case ObIntTC:
        case ObUIntTC: {
          can_demote = ob_is_string_tc(from_type)
                        || ob_is_number_or_decimal_int_tc(from_type)
                        || ob_is_real_type(from_type);
          break;
        }
        case ObDecimalIntTC:
        case ObNumberTC: {
          can_demote = ob_is_real_type(from_type);
          break;
        }
        case ObFloatTC:
        case ObDoubleTC: {
          const ObPrecision precision = to.get_precision();
          const ObScale scale = to.get_scale();
          // Type demotion occurs in fixed real type
          if (scale > SCALE_UNKNOWN_YET && scale < OB_NOT_FIXED_SCALE && precision >= scale) {
            // Type demotion when we can support more range placements, temporarily disabled for now.
            can_demote = false;
          }
          break;
        }
        case ObYearTC: {
          can_demote = ob_is_numeric_type(from_type)
                        || ob_is_string_tc(from_type)
                        || ob_is_temporal_type(from_type);
          break;
        }
        case ObDateTC: {
          can_demote = ob_is_string_tc(from_type)
                        || ob_is_datetime_tc(from_type)
                        || ob_is_time_tc(from_type)
                        || ob_is_mysql_compact_dates_type(from_type);
          break;
        }
        case ObDateTimeTC: {
          can_demote = ob_is_mysql_compact_dates_type(from_type)
                        || (ObDateTimeType == from_type && ObTimestampType == to_type);
          break;
        }
        case ObMySQLDateTC: {
          can_demote = ob_is_string_tc(from_type)
                        || ob_is_datetime_tc(from_type)
                        || ob_is_time_tc(from_type)
                        || ob_is_mysql_datetime_tc(from_type);
          break;
        }
        case ObTimeTC: {
          ObObjType cmp_type = ObMaxType;
          ObExprResultTypeUtil::get_relational_cmp_type(cmp_type, from_type, to_type);
          if (ObTimeType != cmp_type) {
            can_demote = ob_is_datetime_tc(from_type)
                          || ob_is_date_tc(from_type)
                          || ob_is_mysql_datetime_tc(from_type)
                          || ob_is_mysql_date_tc(from_type);
          }
          break;
        }
        default:
          break;
      }
    }
  }
  if (!can_demote && (query_ctx_->non_standard_range_comparison_
                      || (query_ctx_->non_standard_equal_comparison_ && !is_range))) {
    // try to apply non-standard comparison rule to demote type.
    if (lib::is_mysql_mode()) {
      can_demote = ob_is_int_uint_tc(from.get_type()) && ob_is_string_tc(to.get_type());
    }
  }
  return can_demote;
}

bool ObRawExprTypeDemotion::expr_can_demote(const ObConstRawExpr &from,
                                            const ObColumnRefRawExpr &to,
                                            const bool is_range)
{
  bool can_demote = false;
  if (type_can_demote(from.get_result_type(), to.get_result_type(), is_range)) {
    // For temporal column types (e.g., year, date, timestamp), demote both literal and pre-calc
    // expressions for nice date comparison. For all other column types (e.g., integer, number),
    // we only process literal expr.
    //
    // For the following SQL:
    // SELECT * FROM tbl
    // WHERE year_col = date_add(TIMESTAMP'2008-12-20 16:25:46.635', INTERVAL 2 DAY);
    // it can demote the result of date_add to the year(temporal) type.
    //
    // However, for this SQL:
    // SELECT * FROM tbl WHERE int_col = substr('1', 1, 100);
    // it cannot downcast the result of substr to the int type.
    if (IS_DATATYPE_OR_QUESTIONMARK_OP(from.get_expr_type())) {
      // question_mark or datatype expr can always be demoted.
      can_demote = true;
    } else if (is_batched_multi_stmt_) {
      // Batched multi-stmt will replace the const param of the pre-calc expr with column ref,
      // causing the pre-calc expr to not satisfy the const property, so this scenario needs to
      // disable type demotion for pre-calc constant expr.
      can_demote = false;
    } else if (!ob_is_temporal_type(to.get_result_type().get_type())) {
      can_demote = false;
    } else if (ob_is_year_tc(to.get_result_type().get_type())) {
      // MySQL does not demote type for conditions such as year = now()
      can_demote = !from.has_flag(IS_CUR_TIME);
    } else {
      can_demote = true;
    }
  }
  return can_demote;
}

// For certain types of conversions, such as from datetime to time in mysql mode, the conversion
// will always succeed. The result of range_placement is always in-place. In these cases, we simply
// replace the comparison type without needing to add constraints.
bool ObRawExprTypeDemotion::need_constraint(const ObObjType from_type, const ObObjType &to_type)
{
  bool need_constraint = true;
  // Ensure that the type can be demoted before calling this function, it means that function
  // `type_can_demote` has been invoked.
  if (lib::is_mysql_mode()) {
    if (ObTimeType == to_type) {
      if (ob_is_datetime_tc(from_type) || ob_is_date_tc(from_type)
          || ob_is_mysql_datetime_tc(from_type) || ob_is_mysql_date_tc(from_type)) {
        need_constraint = false;
      }
    } else if (ob_is_string_tc(to_type)) {
      // String columns are non-standard comparisons and do not require constraint checking
      // constants.
      need_constraint = false;
    }
  }
  return need_constraint;
}

int ObRawExprTypeDemotion::init_query_ctx_flags(bool &disabled)
{
  int ret = OB_SUCCESS;
  disabled = false;
  ObExecContext *exec_ctx = NULL;
  if (OB_ISNULL(session_) || OB_ISNULL(expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session or expr factory is null", K(ret), KP(session_), KP(expr_factory_));
  } else if (OB_ISNULL(exec_ctx = const_cast<ObExecContext *>(session_->get_cur_exec_ctx()))
          || OB_ISNULL(query_ctx_ = exec_ctx->get_query_ctx())) {
    // exec ctx and query ctx may be null, in which case the type demotion is disabled.
    disabled = true;
    LOG_TRACE("Type demotion is disabled because of null ctx", KP(exec_ctx), KP_(query_ctx));
  } else if (exec_ctx->get_min_cluster_version() < CLUSTER_VERSION_4_3_5_1) {
    // the feature only enabled in cluster version greater and equal than 4.3.5.1
    disabled = true;
    const uint64_t cluster_version = exec_ctx->get_min_cluster_version();
    LOG_TRACE("Type demotion is disabled because of cluster version", KCV(cluster_version));
  } else if (query_ctx_->is_prepare_stmt_) {
    // the actual type of the question mark expr in prepare stage cannot be determined.
    disabled = true;
    LOG_TRACE("Type demotion is disabled because of prepare statement");
  } else if (query_ctx_->type_demotion_flag_inited_) {
    // type demotion flag has been initialized and can be accessed directly.
  } else {
    query_ctx_->type_demotion_flag_ = 0;
    // check tenant configure.
    const uint64_t effective_tenant_id = session_->get_effective_tenant_id();
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(effective_tenant_id));
    if (OB_LIKELY(tenant_config.is_valid())) {
      query_ctx_->enable_constant_type_demotion_ = tenant_config->_enable_constant_type_demotion;
      if (0 == tenant_config->_non_standard_comparison_level.case_compare("range")) {
        query_ctx_->non_standard_equal_comparison_ = 1;
        query_ctx_->non_standard_range_comparison_ = 1;
      } else if (0 == tenant_config->_non_standard_comparison_level.case_compare("equal")) {
        query_ctx_->non_standard_equal_comparison_ = 1;
      }
      query_ctx_->type_demotion_flag_inited_ = 1;
    }
    // check opt param hint.
    bool enable_constant_type_demotion = false;
    bool is_exists = false;
    if (OB_FAIL(query_ctx_->get_global_hint().opt_params_.get_bool_opt_param(
        ObOptParamHint::ENABLE_CONSTANT_TYPE_DEMOTION, enable_constant_type_demotion, is_exists))) {
      LOG_WARN("fail to get hint", K(ret));
    } else if (is_exists) {
      query_ctx_->enable_constant_type_demotion_ = enable_constant_type_demotion;
    }
    ObObj non_std_cmp_level;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(query_ctx_->get_global_hint().opt_params_.get_opt_param(
        ObOptParamHint::NON_STANDARD_COMPARISON_LEVEL, non_std_cmp_level))) {
      LOG_WARN("fail to get hint", K(ret));
    } else if (non_std_cmp_level.is_varchar()) {
      // all branch need overwrite due to the high priority of hint
      if (0 == non_std_cmp_level.get_varchar().case_compare("range")) {
        query_ctx_->non_standard_equal_comparison_ = 1;
        query_ctx_->non_standard_range_comparison_ = 1;
      } else if (0 == non_std_cmp_level.get_varchar().case_compare("equal")) {
        query_ctx_->non_standard_equal_comparison_ = 1;
        query_ctx_->non_standard_range_comparison_ = 0;
      } else {
        query_ctx_->non_standard_equal_comparison_ = 0;
        query_ctx_->non_standard_range_comparison_ = 0;
      }
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(exec_ctx) && OB_NOT_NULL(exec_ctx->get_sql_ctx())) {
    is_batched_multi_stmt_ = exec_ctx->get_sql_ctx()->multi_stmt_item_.is_batched_multi_stmt();
  }
  return ret;
}

// Type Demotion: When comparing a constant with a column, analyze the constant's range placement
// information within the column's type to choose a more suitable comparison type.
// Typically, the constant is converted to the column's type without loss, and eliminating the need
// to add a cast to the column.
// For example,
// in the query SELECT * FROM tbl WHERE int_col = '1', we choose INTEGER as the comparison type
// because the string '1' falls within the range of integers.
// for SELECT * FROM tbl WHERE int_col = '1.1', we select DECIMAL as the comparison type
// since '1.1' lies outside the range of integers.
//
// Apply the type demotion comparison operations:
// 1. common comparisons, including equal, not equal, greater than, less than, etc.;
// 2. BETWEEN and NOT BETWEEN which can be rewrite to common comparisons;
// 3. IN and NOT IN expressions.
int ObRawExprTypeDemotion::demote_type(ObOpRawExpr &expr)
{
  int ret = OB_SUCCESS;
  bool disabled = false;
  if (OB_FAIL(init_query_ctx_flags(disabled))) {
    LOG_WARN("fail to init query ctx flag", K(ret));
  } else if (OB_UNLIKELY(disabled)) {
    // The type demotion feature is disabled, no need to check expression.
  } else if (IS_COMMON_COMPARISON_OP(expr.get_expr_type())) {
    if (OB_FAIL(demote_type_common_comparison(expr))) {
      LOG_WARN("fail to type demotion common comparison", K(ret), K(expr));
    }
  } else if (T_OP_IN == expr.get_expr_type() || T_OP_NOT_IN == expr.get_expr_type()) {
    if (OB_FAIL(demote_type_in_or_not_in(expr))) {
      LOG_WARN("fail to type demotion in or notin expr", K(ret), K(expr));
    }
  }
  return ret;
}

int ObRawExprTypeDemotion::demote_type_common_comparison(ObOpRawExpr &expr)
{
  int ret = OB_SUCCESS;
  const ObRawExpr *left = NULL;
  const ObRawExpr *right = NULL;
  const bool is_range_cmp = IS_RANGE_CMP_OP(expr.get_expr_type());
  if (OB_UNLIKELY(2 != expr.get_param_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr child count mismatch", K(ret), K(expr));
  } else if (OB_ISNULL(left = expr.get_param_expr(0)) ||
             OB_ISNULL(right = expr.get_param_expr(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param expr is null", K(ret), KP(left), KP(right));
  } else if (T_OP_ROW != left->get_expr_type() && T_OP_ROW != right->get_expr_type()) {
    // scalar comparison
    const ObColumnRefRawExpr *column_ref = NULL;
    const ObConstRawExpr *const_value = NULL;
    int64_t constant_expr_idx = 0;
    if (OB_FAIL(extract_cmp_expr_pair(left, right, column_ref, const_value, constant_expr_idx))) {
      LOG_WARN("fail to extract comparison expr template", K(ret));
    } else if (OB_NOT_NULL(column_ref) && OB_NOT_NULL(const_value)) {
      if (OB_FAIL(try_demote_constant_type(*column_ref, *const_value, is_range_cmp, expr,
                                           constant_expr_idx))) {
        LOG_WARN("fail to demote const expr", K(ret));
      }
    }
  } else if (T_OP_ROW == left->get_expr_type() && T_OP_ROW == right->get_expr_type()
              && left->get_param_count() == right->get_param_count()) {
    // vector/row comparison
    const ObColumnRefRawExpr *column_ref = NULL;
    const ObConstRawExpr *const_value = NULL;
    int64_t constant_expr_idx = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < left->get_param_count(); ++i) {
      if (OB_FAIL(extract_cmp_expr_pair(left->get_param_expr(i), right->get_param_expr(i),
                                        column_ref, const_value, constant_expr_idx))) {
        LOG_WARN("fail to extract comparison expr template", K(ret));
      } else if (OB_NOT_NULL(column_ref) && OB_NOT_NULL(const_value)) {
        ObOpRawExpr *op_row = static_cast<ObOpRawExpr*>(expr.get_param_expr(constant_expr_idx));
        if (OB_FAIL(try_demote_constant_type(*column_ref, *const_value, is_range_cmp, *op_row,
                                             i))) {
          LOG_WARN("fail to demote const expr", K(ret), K(i));
        }
      }
    }
  }
  return ret;
}

int ObRawExprTypeDemotion::extract_cmp_expr_pair(const ObRawExpr *left,
                                                 const ObRawExpr *right,
                                                 const ObColumnRefRawExpr *&column_ref,
                                                 const ObConstRawExpr *&const_value,
                                                 int64_t &constant_expr_idx)
{
  int ret = OB_SUCCESS;
  // Type demotion occurs only when one side of a comparison is a `column_ref` and the other side
  // is a constant or a computable expression. Here, we first extract the expression templates
  // that satisfy these conditions.
  if (OB_ISNULL(left) || OB_ISNULL(right)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected expr input", K(ret), KP(left), KP(right));
  } else {
    constant_expr_idx = 0;
    column_ref = NULL;
    const_value = NULL;
    if (T_REF_COLUMN == left->get_expr_type()) {
      column_ref = static_cast<const ObColumnRefRawExpr*>(left);
    } else if (T_REF_COLUMN == right->get_expr_type()) {
      column_ref = static_cast<const ObColumnRefRawExpr*>(right);
    }
    if (left->is_static_const_expr()) {
      const_value = static_cast<const ObConstRawExpr *>(left);
      constant_expr_idx = 0;
    } else if (right->is_static_const_expr()) {
      const_value = static_cast<const ObConstRawExpr *>(right);
      constant_expr_idx = 1;
    }
  }
  return ret;
}

int ObRawExprTypeDemotion::try_demote_constant_type(const ObColumnRefRawExpr &column_ref,
                                                    const ObConstRawExpr &const_value,
                                                    const bool is_range_cmp,
                                                    ObOpRawExpr &op_expr,
                                                    int64_t replaced_expr_idx)
{
  int ret = OB_SUCCESS;
  ObSysFunRawExpr *demote_cast_expr = NULL;
  ObExecContext *exec_ctx = NULL;
  if (!expr_can_demote(const_value, column_ref, is_range_cmp)) {
    // The expr type or the result type of the expr does not satisfy the condition of type demotion,
    // skip to process the exprs
  } else if (OB_ISNULL(exec_ctx = const_cast<ObExecContext *>(session_->get_cur_exec_ctx()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null argument", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_demote_cast_expr(*expr_factory_,
                                                            session_,
                                                            T_FUN_SYS_DEMOTE_CAST,
                                                            &const_value,
                                                            column_ref.get_result_type(),
                                                            demote_cast_expr))) {
    LOG_WARN("fail to build range placement expr", K(ret));
  } else if (need_constraint(const_value.get_result_type().get_type(),
                             column_ref.get_result_type().get_type())) {
    ObPhysicalPlanCtx *plan_ctx = NULL;
    ObObj val;
    bool got_result = false;
    RangePlacement range_placement_value;
    if (OB_ISNULL(plan_ctx = exec_ctx->get_physical_plan_ctx()) ||
        OB_ISNULL(exec_ctx->get_sql_ctx())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("plan ctx is NULL", K(ret));
    } else if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(exec_ctx,
                                                                 demote_cast_expr,
                                                                 val,
                                                                 got_result,
                                                                 exec_ctx->get_allocator(),
                                                                 true))) {
      LOG_WARN("failed to calc const or calculable expr", K(ret));
    } else if (got_result) {
      // No errors occurred during the type demotion process, indicating that the constant's
      // range placement is inside and constant value can be demoted.
      range_placement_value = RP_INSIDE;
    } else {
      // The range placement for constants might be 'outside'. An additional constraint needs to
      // be added to ensure that the plan doesn't make an incorrect selection. Constants that hit
      // this plan cannot be type demoted.
      range_placement_value = RP_OUTSIDE;
      demote_cast_expr = NULL; // NOTE: reset to null to avoid expr replaced
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(add_range_placement_constraint(column_ref, const_value, range_placement_value))) {
        LOG_WARN("fail to add constraint", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(demote_cast_expr)) {
    // rewriting the expressions on constants side of the comparison operation.
    if (OB_FAIL(op_expr.replace_param_expr(replaced_expr_idx, demote_cast_expr))) {
      LOG_WARN("fail to replace demoted expr", K(ret));
    }
  }
  return ret;
}

int ObRawExprTypeDemotion::demote_type_in_or_not_in(ObOpRawExpr &expr)
{
  int ret = OB_SUCCESS;
  const ObRawExpr *left = NULL;
  const ObRawExpr *right = NULL;
  if (OB_UNLIKELY(2 != expr.get_param_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr child count mismatch", K(ret), K(expr));
  } else if (OB_ISNULL(left = expr.get_param_expr(0)) ||
             OB_ISNULL(right = expr.get_param_expr(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param expr is null", K(ret), KP(left), KP(right));
  } else if (T_OP_ROW != right->get_expr_type()
      || !op_row_params_is_all_const(*static_cast<const ObOpRawExpr*>(right))) {
    // if in list is not op_row and params is not all const expr, skip type demotion
  } else if (T_REF_COLUMN == left->get_expr_type()) {
    // col in (a, b, c)
    const ObColumnRefRawExpr *column_ref = static_cast<const ObColumnRefRawExpr*>(left);
    ObOpRawExpr *in_list = static_cast<ObOpRawExpr*>(expr.get_param_expr(1));
    for (int64_t i = 0; OB_SUCC(ret) && i < in_list->get_param_count(); ++i) {
      const ObRawExpr *in_item = NULL;
      const ObConstRawExpr* const_value = NULL;
      if (OB_ISNULL(in_item = in_list->get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null expr", K(ret), K(i));
      } else if (!in_item->is_static_const_expr()) {
        // expr does not satisfy the constant condition, do nothing
      } else if (FALSE_IT(const_value = static_cast<const ObConstRawExpr*>(in_item))) {
      } else if (OB_FAIL(try_demote_constant_type(*column_ref, *const_value, false/*is_range_cmp*/,
                                                  *in_list, i))) {
        LOG_WARN("fail to demote constant type", K(ret), K(i));
      }
    }
  } else if (T_OP_ROW == left->get_expr_type()) {
    // (col1, col2) in ((a, b), (c, d))
    const ObOpRawExpr *left_op_row = static_cast<const ObOpRawExpr *>(left);
    ObOpRawExpr *in_list = static_cast<ObOpRawExpr*>(expr.get_param_expr(1));
    for (int64_t l_idx = 0; OB_SUCC(ret) && l_idx < left_op_row->get_param_count(); ++l_idx) {
      if (OB_ISNULL(left_op_row->get_param_expr(l_idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret));
      } else if (T_REF_COLUMN == left_op_row->get_param_expr(l_idx)->get_expr_type()) {
        // Type demotion occurs only when the expression on the left side is column_ref.
        const ObColumnRefRawExpr *column_ref =
          static_cast<const ObColumnRefRawExpr*>(left_op_row->get_param_expr(l_idx));
        for (int64_t r_idx = 0; OB_SUCC(ret) && r_idx < in_list->get_param_count(); ++r_idx) {
          // Attempt to find a constant expression that satisfies the condition from the row_op
          // in the right branch of in/not_in expr.
          ObRawExpr *right_raw_expr = in_list->get_param_expr(r_idx);
          if (OB_ISNULL(right_raw_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("expr is null", K(ret));
          } else if (T_OP_ROW == right_raw_expr->get_expr_type()) {
            ObOpRawExpr *right_op_row = static_cast<ObOpRawExpr *>(right_raw_expr);
            if (OB_ISNULL(right_op_row->get_param_expr(l_idx))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("expr is null", K(ret));
            } else if (right_op_row->get_param_expr(l_idx)->is_static_const_expr()) {
              ObConstRawExpr* const_value =
                static_cast<ObConstRawExpr*>(right_op_row->get_param_expr(l_idx));
              if (OB_FAIL(try_demote_constant_type(*column_ref, *const_value, false/*is_range_cmp*/,
                                                   *right_op_row, l_idx))) {
                LOG_WARN("fail to demote constant type", K(ret), K(l_idx));
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

// Check whether all parameters in the right child of the IN/NOT_IN expr are constants. No longer
// verify the number and structural validity of the parameters here, as this has already been
// handled in function `ObRawExprDeduceType::check_expr_param`.
bool ObRawExprTypeDemotion::op_row_params_is_all_const(const ObOpRawExpr &op_row) const
{
  bool is_all_const = true;
  for (int64_t i = 0; is_all_const && i < op_row.get_param_count(); ++i) {
    if (OB_ISNULL(op_row.get_param_expr(i))) {
    } else if (T_OP_ROW == op_row.get_param_expr(i)->get_expr_type()) {
      const ObOpRawExpr *child_op_row = static_cast<const ObOpRawExpr *>(op_row.get_param_expr(i));
      is_all_const = op_row_params_is_all_const(*child_op_row);
    } else {
      is_all_const = op_row.get_param_expr(i)->is_static_const_expr();
    }
  }
  return is_all_const;
}

// Since plans are stored in the plan cache, for comparisons that require type demotion,
// we need to add range placement constraints. When the range placement derived from constant
// analysis differs, different comparison types will be selected.
// The constraint checks whether the result of the `range_placement` expression is equal to
// the range placement value.
// For example,
//   Expr Constraints:
//       range_placement(INT,PS:(11,0),'1') = 1 result is TRUE
// The value `1` on the right side of the equality is an enumeration value of range placement,
// equivalent to `RP_INSIDE`.
int ObRawExprTypeDemotion::add_range_placement_constraint(
    const ObColumnRefRawExpr &column_ref,
    const ObConstRawExpr &const_expr,
    const RangePlacement rp)
{
  int ret = OB_SUCCESS;
  ObSysFunRawExpr *range_placement_expr = NULL;
  ObConstRawExpr *rp_value_expr = NULL;
  ObRawExpr *equal_expr = NULL;
  if (OB_FAIL(ObRawExprUtils::build_demote_cast_expr(*expr_factory_,
                                                      session_,
                                                      T_FUN_SYS_RANGE_PLACEMENT,
                                                      &const_expr,
                                                      column_ref.get_result_type(),
                                                      range_placement_expr))) {
    LOG_WARN("fail to build range placement expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*expr_factory_,
                                                          ObInt32Type,
                                                          static_cast<int32_t>(rp),
                                                          rp_value_expr))) {
    LOG_WARN("fail to build range placement value expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(*expr_factory_, T_OP_EQ,
                                                                 range_placement_expr,
                                                                 rp_value_expr,
                                                                 equal_expr))) {
    LOG_WARN("fail to build equal expr", K(ret));
  } else if (OB_FAIL(equal_expr->formalize(session_))) {
    LOG_WARN("fail to formalize expr", K(ret));
  } else if (OB_UNLIKELY(!equal_expr->is_static_const_expr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pre calculable expr is expected here", K(ret));
  } else {
    ObExprConstraint cons(equal_expr, PreCalcExprExpectResult::PRE_CALC_RESULT_TRUE);
    cons.ignore_const_check_ = false;
    if (OB_FAIL(add_var_to_array_no_dup(query_ctx_->all_expr_constraints_, cons))) {
      LOG_WARN("failed to push back pre calc constraints", K(ret));
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
