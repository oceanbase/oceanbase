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

#define USING_LOG_PREFIX SQL_ENG

#include "ob_sql_expression.h"
#include "lib/utility/utility.h"
#include "lib/allocator/ob_cached_allocator.h"
#include "lib/alloc/malloc_hook.h"
#include "sql/parser/ob_item_type_str.h"
#include "sql/engine/ob_phy_operator_type.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase {
using namespace common;
namespace sql {
ObSqlExpression::ObSqlExpression(common::ObIAllocator& allocator, int64_t item_count)
    : inner_alloc_(allocator),
      post_expr_(allocator, item_count),
      fast_expr_(NULL),
      infix_expr_(allocator, item_count),
      array_param_index_(OB_INVALID_INDEX),
      need_construct_binding_array_(false),
      gen_infix_expr_(false)
{}

ObSqlExpression::~ObSqlExpression()
{
  reset();
}

int ObSqlExpression::set_item_count(int64_t count)
{
  int ret = OB_SUCCESS;
  if (ObSqlExpressionUtil::should_gen_postfix_expr()) {
    if (OB_FAIL(post_expr_.set_item_count(count))) {
      LOG_WARN("failed to set item count", K(ret));
    } else {
      // do nothing
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(infix_expr_.set_item_count(count))) {
    LOG_WARN("set expr item count failed", K(ret));
  }
  return ret;
}

int ObSqlExpression::assign(const ObSqlExpression& other)
{
  int ret = OB_SUCCESS;
  if (&other != this) {
    need_construct_binding_array_ = other.need_construct_binding_array_;
    array_param_index_ = other.array_param_index_;
    ret = post_expr_.assign(other.post_expr_);

    // @note we do not copy the members of DLink on purpose
    if (OB_SUCC(ret) && other.fast_expr_ != NULL) {
      ObExprOperatorFactory factory(inner_alloc_);
      if (OB_FAIL(factory.alloc_fast_expr(other.fast_expr_->get_op_type(), fast_expr_))) {
        LOG_WARN("alloc fast expr failed", K(ret));
      } else if (OB_FAIL(fast_expr_->assign(*other.fast_expr_))) {
        LOG_WARN("assign fast expr failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      gen_infix_expr_ = other.gen_infix_expr_;
      if (OB_FAIL(infix_expr_.assign(other.infix_expr_))) {
        LOG_WARN("infix expr assign failed", K(ret));
      }
    }
  }
  return ret;
}

int ObSqlExpression::add_expr_item(const ObPostExprItem& item)
{
  int ret = OB_SUCCESS;
  if (!gen_infix_expr_) {
    if (OB_FAIL(post_expr_.add_expr_item(item))) {
      LOG_WARN("failed to add post expr item", K(ret));
    } else {
      // do nothing
    }
  } else {
    ObInfixExprItem infix_item;
    *static_cast<ObPostExprItem*>(&infix_item) = item;
    if (OB_FAIL(infix_expr_.add_expr_item(infix_item))) {
      LOG_WARN("add infix expr item failed", K(ret));
    } else if (IS_EXPR_OP(item.get_item_type())) {
      int64_t last_expr_idx = infix_expr_.get_exprs().count() - 1;
      infix_expr_.get_exprs()
          .at(last_expr_idx)
          .set_param_idx(static_cast<const ObInfixExprItem*>(&item)->get_param_idx());
      infix_expr_.get_exprs()
          .at(last_expr_idx)
          .set_param_num(static_cast<const ObInfixExprItem*>(&item)->get_param_num());
    }
  }
  return ret;
}

int ObSqlExpression::calc(ObExprCtx& expr_ctx, const common::ObNewRow& row, common::ObObj& result) const
{
  int ret = OB_SUCCESS;
  if (fast_expr_ != NULL) {
    if (OB_FAIL(fast_expr_->calc(expr_ctx, row, result))) {
      LOG_WARN("cacl fast expr failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(infix_expr_.is_empty())) {
      if (post_expr_.is_empty()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(post_expr_.is_empty()), K(ret));
      } else {
        ret = post_expr_.calc(expr_ctx, row, result);
      }
    } else {
      ret = infix_expr_.calc(expr_ctx, row, result);
    }
  }
  return ret;
}

int ObSqlExpression::calc(
    ObExprCtx& expr_ctx, const common::ObNewRow& row1, const common::ObNewRow& row2, common::ObObj& result) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(infix_expr_.is_empty())) {
    if (post_expr_.is_empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(post_expr_.is_empty()), K(ret));
    } else {
      ret = post_expr_.calc(expr_ctx, row1, row2, result);
    }
  } else {
    ret = infix_expr_.calc(expr_ctx, row1, row2, result);
  }
  return ret;
}

int ObSqlExpression::generate_idx_for_regexp_ops(int16_t& cur_regexp_op_count)
{
  int ret = OB_SUCCESS;
  if (ObSqlExpressionUtil::should_gen_postfix_expr()) {
    if (OB_FAIL(post_expr_.generate_idx_for_regexp_ops(cur_regexp_op_count))) {
      LOG_WARN("generate idx for regexp failed", K(ret));
    } else {
      // do nothing
    }
  }
  if (OB_FAIL(infix_expr_.generate_idx_for_regexp_ops(cur_regexp_op_count))) {
    LOG_WARN("generate idx for regexp failed", K(ret));
  }
  return ret;
}

bool ObSqlExpression::is_equijoin_cond(int64_t& c1, int64_t& c2, common::ObObjType& cmp_type,
    common::ObCollationType& cmp_cs_type, bool& is_null_safe) const
{
  int ret = OB_SUCCESS;
  bool is = false;
  if (OB_UNLIKELY(infix_expr_.is_empty())) {
    if (post_expr_.is_empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(post_expr_.is_empty()), K(ret));
    } else {
      is = post_expr_.is_equijoin_cond(c1, c2, cmp_type, cmp_cs_type, is_null_safe);
    }
  } else {
    is = infix_expr_.is_equijoin_cond(c1, c2, cmp_type, cmp_cs_type, is_null_safe);
  }
  return is;
}

OB_DEF_SERIALIZE(ObSqlExpression)
{
  int ret = OB_SUCCESS;

  OB_UNIS_ENCODE(post_expr_);

  OB_UNIS_ENCODE(infix_expr_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObSqlExpression)
{
  int64_t len = 0;

  OB_UNIS_ADD_LEN(post_expr_);
  OB_UNIS_ADD_LEN(infix_expr_);

  return len;
}

OB_DEF_DESERIALIZE(ObSqlExpression)
{
  int ret = OB_SUCCESS;

  OB_UNIS_DECODE(post_expr_);

  OB_UNIS_DECODE(infix_expr_);

  return ret;
}

ObColumnExpression::~ObColumnExpression()
{}

ObColumnExpression::ObColumnExpression(common::ObIAllocator& allocator, int64_t item_count)
    : ObSqlExpression(allocator, item_count), result_index_(OB_INVALID_INDEX), cs_type_(CS_TYPE_INVALID), accuracy_()
{}

int ObColumnExpression::assign(const ObColumnExpression& other)
{
  int ret = OB_SUCCESS;
  if (&other != this) {
    result_index_ = other.result_index_;
    cs_type_ = other.cs_type_;
    accuracy_ = other.accuracy_;
    ret = ObSqlExpression::assign(other);
  }
  return ret;
}

int64_t ObColumnExpression::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(result_index));
  J_COMMA();
  J_KV(N_POST_EXPR, post_expr_);
  J_COMMA();
  J_KV(K_(infix_expr));
  J_OBJ_END();
  return pos;
}

int ObColumnExpression::calc_and_project(ObExprCtx& expr_ctx, ObNewRow& row) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(row.is_invalid()) || OB_UNLIKELY(result_index_ < 0) || OB_UNLIKELY(result_index_ >= row.count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid result index", K_(result_index), K_(row.count));
  } else if (OB_FAIL(ObSqlExpression::calc(expr_ctx, row, row.cells_[result_index_]))) {
    LOG_WARN("calc expr result failed", K(ret));
  }
  return ret;
}

static ObObj OBJ_ZERO;
static struct obj_zero_init {
  obj_zero_init()
  {
    OBJ_ZERO.set_int(0);
  }
} obj_zero_init;

OB_SERIALIZE_MEMBER((ObColumnExpression, ObSqlExpression), result_index_, cs_type_, accuracy_);

ObAggregateExpression::~ObAggregateExpression()
{}

ObAggregateExpression::ObAggregateExpression(common::ObIAllocator& allocator, int64_t item_count)
    : ObColumnExpression(allocator, item_count),
      aggr_func_(T_INVALID),
      is_distinct_(false),
      sort_columns_(allocator),
      separator_param_expr_(allocator, 1),
      real_param_col_count_(0),
      aggr_cs_types_(allocator),
      output_column_count_(1),
      extra_infos_(allocator)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(separator_param_expr_.set_item_count(1))) {
    LOG_WARN("failed to set item count", K(ret));
  }
}

int ObAggregateExpression::assign(const ObAggregateExpression& other)
{
  int ret = OB_SUCCESS;
  if (&other != this) {
    aggr_func_ = other.aggr_func_;
    is_distinct_ = other.is_distinct_;
    real_param_col_count_ = other.real_param_col_count_;
    if (OB_FAIL(aggr_cs_types_.assign(other.aggr_cs_types_))) {
    } else if (OB_FAIL(sort_columns_.assign(other.sort_columns_))) {
    } else if (OB_FAIL(separator_param_expr_.assign(other.separator_param_expr_))) {
    } else if (OB_FAIL(this->ObColumnExpression::assign(other))) {
    } else if (OB_FAIL(separator_param_expr_.set_item_count(1))) {
    } else if (OB_FAIL(extra_infos_.assign(other.extra_infos_))) {
    } else {
      output_column_count_ = other.output_column_count_;
    }
  }
  return ret;
}

int64_t ObAggregateExpression::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(result_index));
  J_COMMA();
  J_KV(N_AGGR_FUNC, ob_aggr_func_str(aggr_func_), N_DISTINCT, is_distinct_);
  J_COMMA();
  J_KV(N_POST_EXPR, post_expr_);
  J_COMMA();
  J_KV(K_(infix_expr));
  J_OBJ_END();
  return pos;
}

int ObAggregateExpression::calc(ObExprCtx& expr_ctx, const common::ObNewRow& row, common::ObObj& result) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY((T_FUN_COUNT == aggr_func_ || T_FUN_KEEP_COUNT == aggr_func_) && is_empty())) {
    // COUNT(*)
    // point the result to an arbitray non-null cell
    result = OBJ_ZERO;
  } else {
    if (OB_FAIL(ObSqlExpression::calc(expr_ctx, row, result))) {
      LOG_WARN("failed to calc agg expr", K(ret), K(row));
    }
  }
  return ret;
}

int ObAggregateExpression::calc(
    ObExprCtx& expr_ctx, const common::ObNewRow& row1, const common::ObNewRow& row2, common::ObObj& result) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_FUN_COUNT == aggr_func_ && is_empty())) {
    // COUNT(*)
    // point the result to an arbitray non-null cell
    result = OBJ_ZERO;
  } else {
    if (OB_FAIL(ObSqlExpression::calc(expr_ctx, row1, row2, result))) {
      LOG_WARN("failed to calc agg expr", K(ret), K(row1), K(row2));
    }
  }
  return ret;
}

int ObAggregateExpression::calc_result_row(ObExprCtx& expr_ctx, const ObNewRow& row, ObNewRow& result_row) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_FUN_COUNT == aggr_func_ && is_empty())) {
    // COUNT(*)
    // point the result to an arbitray non-null cell
    if (OB_UNLIKELY(1 != result_row.count_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cells count should be 1", K(ret));
    } else {
      result_row.cells_[0] = OBJ_ZERO;
    }
  } else {
    if (OB_UNLIKELY(infix_expr_.is_empty())) {
      if (post_expr_.is_empty()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(post_expr_.is_empty()), K(ret));
      } else if (OB_FAIL(post_expr_.calc_result_row(expr_ctx, row, result_row))) {
        LOG_WARN("failed to calc result row", K(ret), K(row));
      }
    } else {
      if (OB_FAIL(infix_expr_.calc_row(expr_ctx, row, result_row))) {
        LOG_WARN("failed to calc row", K(ret), K(row));
      }
    }
  }
  return ret;
}

int ObAggregateExpression::add_sort_column(const int64_t index, ObCollationType cs_type, bool is_ascending)
{
  int ret = OB_SUCCESS;
  ObSortColumn sort_column;
  sort_column.index_ = index;
  sort_column.cs_type_ = cs_type;
  sort_column.set_is_ascending(is_ascending);
  if (OB_FAIL(sort_columns_.push_back(sort_column))) {
    LOG_WARN("failed to push back to array", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObAggregateExpression, ObColumnExpression), aggr_func_, is_distinct_, sort_columns_,
    separator_param_expr_, real_param_col_count_, aggr_cs_types_, output_column_count_, extra_infos_);

int ObSqlExpressionUtil::make_sql_expr(ObPhysicalPlan* physical_plan, ObSqlExpression*& expr)
{
  return make_expr(physical_plan, expr);
}

int ObSqlExpressionUtil::make_sql_expr(ObPhysicalPlan* physical_plan, ObColumnExpression*& expr)
{
  return make_expr(physical_plan, expr);
}

int ObSqlExpressionUtil::make_sql_expr(ObPhysicalPlan* physical_plan, ObAggregateExpression*& expr)
{
  return make_expr(physical_plan, expr);
}
template <class T>
int ObSqlExpressionUtil::make_expr(ObPhysicalPlan* physical_plan, T*& expr)
{
  int ret = OB_SUCCESS;
  ObSqlExpressionFactory* factory = NULL;
  if (OB_ISNULL(physical_plan)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), K(physical_plan));
  } else if (OB_ISNULL(factory = physical_plan->get_sql_expression_factory())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get invalid sql expression factory", K(ret), K(factory));
  } else if (OB_FAIL(factory->alloc(expr, 0))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to alloc ObSqlExpression", K(ret));
  }
  return ret;
}
int ObSqlExpressionUtil::add_expr_to_list(ObDList<ObSqlExpression>& list, ObSqlExpression* expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret));
  } else if (!list.add_last(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("failed to add expr to list");
  }
  return ret;
}

int ObSqlExpressionUtil::copy_sql_expression(
    ObSqlExpressionFactory& sql_expression_factory, const ObSqlExpression* src_expr, ObSqlExpression*& dst_expr)
{
  int ret = OB_SUCCESS;
  if (NULL == src_expr) {
    dst_expr = NULL;
  } else if (OB_FAIL(sql_expression_factory.alloc(dst_expr))) {
    dst_expr = NULL;
    LOG_WARN("Failed to alloc sql_expr", K(ret));
  } else if (NULL == dst_expr) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Failed to allocate sql expr", K(ret));
  } else if (OB_FAIL(dst_expr->assign(*src_expr))) {
    LOG_WARN("Failed to copy expr", K(ret));
  } else {
  }
  return ret;
}

int ObSqlExpressionUtil::copy_sql_expressions(ObSqlExpressionFactory& sql_expression_factory,
    const ObIArray<ObSqlExpression*>& src_exprs, ObIArray<ObSqlExpression*>& dst_exprs)
{
  int ret = OB_SUCCESS;
  ObSqlExpression* src_sql_expr = NULL;
  ObSqlExpression* dst_sql_expr = NULL;
  for (int64_t idx = 0; OB_SUCC(ret) && idx < src_exprs.count(); ++idx) {
    src_sql_expr = src_exprs.at(idx);
    dst_sql_expr = NULL;
    if (OB_ISNULL(src_sql_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Src sql expr should not be NULL", K(ret));
    } else if (OB_FAIL(sql_expression_factory.alloc(dst_sql_expr))) {
      LOG_WARN("Failed to alloc sql_expr", K(ret));
    } else if (OB_ISNULL(dst_sql_expr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("Failed to allocate sql expr", K(ret));
    } else if (OB_FAIL(dst_sql_expr->assign(*src_sql_expr))) {
      LOG_WARN("Failed to copy expr", K(ret));
    } else if (OB_FAIL(dst_exprs.push_back(dst_sql_expr))) {
      LOG_WARN("Failed to add key expr", K(ret));
    } else {
    }  // do nothing
  }
  return ret;
}

int ObSqlExpressionUtil::expand_array_params(ObExprCtx& expr_ctx, const ObObj& src_param, const ObObj*& result)
{
  int ret = OB_SUCCESS;
  if (src_param.is_ext()) {
    CK(expr_ctx.my_session_);
    CK(expr_ctx.exec_ctx_);
    CK(expr_ctx.exec_ctx_->get_sql_ctx());
    if (expr_ctx.exec_ctx_->get_sql_ctx()->multi_stmt_item_.is_batched_multi_stmt()) {
      ret = OB_NOT_SUPPORTED;
    } else {
      result = &src_param;
    }
  } else {
    result = &src_param;
  }
  return ret;
}

bool ObSqlExpressionUtil::should_gen_postfix_expr()
{
  bool bool_ret = false;
  if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2230) {
    bool_ret = true;
  } else {
    bool_ret = false;
  }
  return bool_ret;
}
}  // namespace sql
}  // namespace oceanbase
