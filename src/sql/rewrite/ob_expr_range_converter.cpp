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
#include "sql/rewrite/ob_expr_range_converter.h"
#include "sql/rewrite/ob_range_graph_generator.h"
#include "lib/timezone/ob_time_convert.h"
#include "lib/container/ob_array_serialization.h"
#include "lib/geo/ob_geo_utils.h"
#include "lib/rc/ob_rc.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/engine/expr/ob_expr_like.h"
#include "common/ob_smart_call.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/engine/expr/ob_geo_expr_utils.h"
#include "sql/ob_sql_utils.h"
#include "sql/rewrite/ob_range_generator.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace sql
{
static const int64_t RANGE_EXPR_EQUAL = 1 << 1;
static const int64_t RANGE_EXPR_CMP = 1 << 2;
static const int64_t RANGE_EXPR_IN = 1 << 3;
static const int64_t RANGE_EXPR_NO_EQUAL = 1 << 4;
static const int64_t RANGE_EXPR_NOT_IN = 1 << 5;
static const int64_t RANGE_EXPR_OTHER = 1 << 6;

/**
 * TODO list:
 *  spatial_expr use new query range
 * */

int ObExprRangeConverter::convert_expr_to_range_node(const ObRawExpr *expr,
                                                     ObRangeNode *&range_node,
                                                     int64_t expr_depth,
                                                     bool &is_precise)
{
  int ret = OB_SUCCESS;
  range_node = nullptr;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null expr");
  } else if (expr->is_const_expr()) {
    if(OB_FAIL(convert_const_expr(expr, range_node))) {
      LOG_WARN("failed to convert const expr");
    }
  } else if (T_OP_LIKE == expr->get_expr_type()) {
    if(OB_FAIL(convert_like_expr(expr, expr_depth, range_node))) {
      LOG_WARN("failed to convert like expr");
    }
  } else if (IS_BASIC_CMP_OP(expr->get_expr_type())) {
    if (OB_FAIL(convert_basic_cmp_expr(expr, expr_depth, range_node))) {
      LOG_WARN("failed to convert basic cmp expr");
    }
  } else if (0 == expr_depth && T_OP_NE == expr->get_expr_type()) {
    if (OB_FAIL(convert_not_equal_expr(expr, expr_depth, range_node))) {
      LOG_WARN("failed to convert not equal expr");
    }
  } else if (T_OP_IS == expr->get_expr_type()) {
    if (OB_FAIL(convert_is_expr(expr, expr_depth, range_node))) {
      LOG_WARN("failed to convert is expr");
    }
  } else if (T_OP_IS_NOT == expr->get_expr_type() &&
             GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_4_1_0 &&
             ctx_.optimizer_features_enable_version_ >= COMPAT_VERSION_4_4_1) {
    if (OB_FAIL(convert_is_not_expr(expr, expr_depth, range_node))) {
      LOG_WARN("failed to convert is not expr");
    }
  } else if (T_OP_BTW == expr->get_expr_type()) {
    if (OB_FAIL(convert_between_expr(expr, expr_depth, range_node))) {
      LOG_WARN("failed to convert between expr");
    }
  } else if (0 == expr_depth && T_OP_NOT_BTW == expr->get_expr_type()) {
    if (OB_FAIL(convert_not_between_expr(expr, expr_depth, range_node))) {
      LOG_WARN("failed to convert not between expr");
    }
  } else if (T_OP_IN  == expr->get_expr_type()) {
    if (OB_FAIL(convert_in_expr(expr, expr_depth, range_node))) {
      LOG_WARN("failed to convert in expr");
    }
  } else if (0 == expr_depth && T_OP_NOT_IN  == expr->get_expr_type() &&
             ctx_.enable_not_in_range_) {
    if (OB_FAIL(convert_not_in_expr(expr, expr_depth, range_node))) {
      LOG_WARN("failed to convert not in expr");
    }
  } else if (expr->is_spatial_expr()) {
    if (OB_FAIL(convert_geo_expr(expr, expr_depth, range_node))) {
      LOG_WARN("failed to convert geo expr");
    }
  } else if (expr->is_domain_expr()) {
    if (OB_FAIL(convert_domain_expr(expr, expr_depth, range_node))) {
      LOG_WARN("extract and_or failed", K(ret));
    }
  } else {
    ctx_.cur_is_precise_ = false;
    if (OB_FAIL(generate_always_true_or_false_node(true, range_node))) {
      LOG_WARN("failed to generate always true node", KPC(expr));
    }
  }

  if (OB_SUCC(ret)) {
    is_precise = ctx_.cur_is_precise_;
    LOG_TRACE("succeed to convert one expr to range node", KPC(expr), KPC(range_node));
  }
  return ret;
}

//generate node start
int ObExprRangeConverter::alloc_range_node(ObRangeNode *&range_node)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  void *key_ptr = NULL;
  if (OB_UNLIKELY(allocator_.used() - mem_used_ > ctx_.max_mem_size_)) {
    ret = OB_ERR_QUERY_RANGE_MEMORY_EXHAUSTED;
    LOG_INFO("use too much memory when extract query range", K(ctx_.max_mem_size_));
  } else if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObRangeNode))) ||
             OB_ISNULL(key_ptr = allocator_.alloc(sizeof(int64_t) * ctx_.column_cnt_ * 2))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for range node");
  } else {
    range_node = new(ptr) ObRangeNode(allocator_);
    range_node->start_keys_ = static_cast<int64_t*>(key_ptr);
    range_node->end_keys_ = static_cast<int64_t*>(key_ptr)+ ctx_.column_cnt_;
    range_node->column_cnt_ = ctx_.column_cnt_;
    MEMSET(key_ptr, 0, sizeof(int64_t) * ctx_.column_cnt_ * 2);
  }
  return ret;
}

int ObExprRangeConverter::generate_always_true_or_false_node(bool is_true,
                                                             ObRangeNode *&range_node)
{
  int ret = OB_SUCCESS;
  ctx_.cur_is_precise_ = false;
  if (OB_FAIL(alloc_range_node(range_node))) {
    LOG_WARN("failed to alloc common range node");
  } else if (is_true) {
    range_node->always_true_ = true;
    for (int64_t i = 0; i < ctx_.column_cnt_; ++i) {
      range_node->start_keys_[i] = OB_RANGE_MIN_VALUE;
      range_node->end_keys_[i] = OB_RANGE_MAX_VALUE;
    }
  } else {
    range_node->always_false_ = true;
    for (int64_t i = 0; i < ctx_.column_cnt_; ++i) {
      range_node->start_keys_[i] = OB_RANGE_MAX_VALUE;
      range_node->end_keys_[i] = OB_RANGE_MIN_VALUE;
    }
  }
  return ret;
}

int ObExprRangeConverter::convert_const_expr(const ObRawExpr *expr,
                                             ObRangeNode *&range_node)
{
  int ret = OB_SUCCESS;
  int64_t start_val = 0;
  int64_t end_val = 0;
  bool is_valid = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null expr", K(expr));
  } else if (OB_FAIL(check_calculable_expr_valid(expr, is_valid))) {
    LOG_WARN("failed to get calculable expr val");
  } else if (!is_valid) {
    ctx_.cur_is_precise_ = false;
    if (OB_FAIL(generate_always_true_or_false_node(true, range_node))) {
      LOG_WARN("failed to generate always true node");
    }
  } else if (OB_FAIL(alloc_range_node(range_node))) {
    LOG_WARN("failed to alloc const range node");
  } else if (OB_FAIL(generate_deduce_const_expr(const_cast<ObRawExpr*>(expr), start_val, end_val))) {
    LOG_WARN("failed to generate expr for const expr", KPC(expr));
  } else {
    range_node->always_false_ = false;
    range_node->always_true_ = false;
    for (int64_t i = 0; i < ctx_.column_cnt_; ++i) {
      range_node->start_keys_[i] = start_val;
      range_node->end_keys_[i] = end_val;
    }
    ctx_.cur_is_precise_ = true;
  }
  return ret;
}

int ObExprRangeConverter::generate_deduce_const_expr(ObRawExpr *expr,
                                                     int64_t &start_val,
                                                     int64_t &end_val) {
  int ret = OB_SUCCESS;
  ObConstRawExpr *pos_expr = NULL;
  ObSysFunRawExpr *start_expr = NULL;
  ObSysFunRawExpr *end_expr = NULL;
  int64_t is_start = 1;
  if (OB_ISNULL(expr) || OB_ISNULL(ctx_.expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(expr), K(ctx_.expr_factory_));
  } else if (OB_FAIL(ctx_.expr_factory_->create_raw_expr(T_FUNC_SYS_INNER_IS_TRUE, start_expr))) {
    LOG_WARN("failed to create a new expr", K(ret));
  } else if (OB_FAIL(ctx_.expr_factory_->create_raw_expr(T_FUNC_SYS_INNER_IS_TRUE, end_expr))) {
    LOG_WARN("failed to create a new expr", K(ret));
  } else if (OB_ISNULL(start_expr) || OB_ISNULL(end_expr)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate expr", K(start_expr), K(end_expr));
  } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*ctx_.expr_factory_,
                                                          ObIntType,
                                                          1, // const value
                                                          pos_expr))) {
    LOG_WARN("Failed to build const expr", K(ret));
  } else if (OB_FAIL(start_expr->set_param_exprs(expr, pos_expr))) {
    LOG_WARN("failed to set param for is true", KPC(expr));
  } else if (OB_FAIL(start_expr->formalize(ctx_.session_info_))) {
    LOG_WARN("failed to formalize expr");
  } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*ctx_.expr_factory_,
                                                          ObIntType,
                                                          0, // const value
                                                          pos_expr))) {
    LOG_WARN("Failed to build const expr", K(ret));
  } else if (OB_FAIL(end_expr->set_param_exprs(expr, pos_expr))) {
    LOG_WARN("failed to set param for is true", KPC(expr));
  } else if (OB_FAIL(end_expr->formalize(ctx_.session_info_))) {
    LOG_WARN("failed to formalize expr");
  } else if (OB_FAIL(get_final_expr_idx(start_expr, nullptr, start_val))) {
    LOG_WARN("failed to get final expr idx");
  } else if (OB_FAIL(get_final_expr_idx(end_expr, nullptr, end_val))) {
    LOG_WARN("failed to get final expr idx");
  }
  return ret;
}


int ObExprRangeConverter::convert_basic_cmp_expr(const ObRawExpr *expr,
                                                 int64_t expr_depth,
                                                 ObRangeNode *&range_node)
{
  int ret = OB_SUCCESS;
  const ObRawExpr* l_expr = nullptr;
  const ObRawExpr* r_expr = nullptr;
  if (OB_ISNULL(expr) ||
      OB_ISNULL(l_expr = expr->get_param_expr(0)) ||
      OB_ISNULL(r_expr = expr->get_param_expr(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null expr", K(expr), K(l_expr), K(r_expr));
  } else {
    const ObOpRawExpr *op_expr = static_cast<const ObOpRawExpr*>(expr);
    // (c1, c2) = ((1, 1)) => (c1, c2) = (1, 1)
    if (lib::is_oracle_mode() && T_OP_ROW == l_expr->get_expr_type() &&
        T_OP_ROW == r_expr->get_expr_type() && 1 == r_expr->get_param_count() &&
        OB_NOT_NULL(r_expr->get_param_expr(0)) &&
        T_OP_ROW == r_expr->get_param_expr(0)->get_expr_type()) {
      r_expr = r_expr->get_param_expr(0);
    }
    if (OB_FAIL(get_basic_range_node(l_expr, r_expr,
                                      expr->get_expr_type(),
                                      expr_depth,
                                      range_node))) {
      LOG_WARN("failed to get basic range node");
    }
  }
  return ret;
}

int ObExprRangeConverter::get_basic_range_node(const ObRawExpr *l_expr,
                                               const ObRawExpr *r_expr,
                                               ObItemType cmp_type,
                                               int64_t expr_depth,
                                               ObRangeNode *&range_node)
{
  int ret = OB_SUCCESS;
  range_node = NULL;
  ctx_.cur_is_precise_ = false;
  bool use_implicit_cast_feature = true;
  if (OB_ISNULL(l_expr) || OB_ISNULL(r_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null expr", KP(l_expr), KP(r_expr));
  } else if (OB_UNLIKELY((T_OP_ROW == l_expr->get_expr_type()) != (T_OP_ROW == r_expr->get_expr_type()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("both expr must be or not be row expr", KPC(l_expr), KPC(r_expr));
  } else if (!IS_BASIC_CMP_OP(cmp_type)) {
    if (OB_FAIL(generate_always_true_or_false_node(true, range_node))) {
      LOG_WARN("failed to generate always true node");
    }
  } else if (T_OP_ROW != l_expr->get_expr_type()) {
    const ObRawExpr *l_ori_expr = l_expr;
    const ObRawExpr *r_ori_expr = r_expr;
    const ObRawExprResType &calc_type = l_expr->has_flag(CNT_COLUMN) ?
                                        l_expr->get_result_type() : r_expr->get_result_type();

    if (OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(l_expr,
                                                                l_expr,
                                                                use_implicit_cast_feature))) {
      LOG_WARN("failed to get expr without lossless cast", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(r_expr,
                                                                       r_expr,
                                                                       use_implicit_cast_feature))) {
      LOG_WARN("failed to get expr without lossless cast", K(ret));
    } else if ((l_expr->has_flag(IS_COLUMN) && r_expr->is_const_expr()) ||
               (l_expr->is_const_expr() && r_expr->has_flag(IS_COLUMN))) {
      if (OB_FAIL(gen_column_cmp_node(*l_expr,
                                      *r_expr,
                                      cmp_type,
                                      calc_type,
                                      expr_depth,
                                      T_OP_NSEQ == cmp_type,
                                      range_node))) {
        LOG_WARN("get column key part failed.", K(ret));
      }
    } else if (((l_expr->get_expr_type() == T_FUN_SYS_NVL && r_expr->is_const_expr()) ||
               (l_expr->is_const_expr() && r_expr->get_expr_type() == T_FUN_SYS_NVL))) {
      if (OB_FAIL(get_nvl_cmp_node(*l_expr,
                                   *r_expr,
                                   cmp_type,
                                   calc_type,
                                   expr_depth,
                                   range_node))) {
        LOG_WARN("failed to get nvl cmp node", K(ret));
      }
    } else if ((l_expr->has_flag(IS_ROWID) && r_expr->is_const_expr()) ||
               (r_expr->has_flag(IS_ROWID) && l_expr->is_const_expr())) {
      if (OB_FAIL(get_rowid_node(*l_expr, *r_expr, cmp_type, range_node))) {
        LOG_WARN("get rowid key part failed.", K(ret));
      }
    } else if (cmp_type == T_OP_EQ &&
               ((l_expr->get_expr_type() == T_FUN_SYS_SDO_RELATE && r_expr->is_const_expr()) ||
                (r_expr->get_expr_type() == T_FUN_SYS_SDO_RELATE && l_expr->is_const_expr()))) {
      if (OB_FAIL(get_orcl_spatial_range_node(*l_expr, *r_expr, expr_depth, range_node))) {
        LOG_WARN("failed to get orcl spatial range_node", K(ret));
      }
    } else if (ObSQLUtils::is_min_cluster_version_ge_425_or_435() &&
               ObSQLUtils::is_opt_feature_version_ge_425_or_435(ctx_.optimizer_features_enable_version_) &&
               ((l_ori_expr->get_expr_type() == T_FUN_SYS_CAST && r_ori_expr->is_const_expr()) ||
                (r_ori_expr->get_expr_type() == T_FUN_SYS_CAST && l_ori_expr->is_const_expr()))) {
      if (OB_FAIL(get_implicit_cast_range(*l_ori_expr,
                                          *r_ori_expr,
                                          cmp_type,
                                          expr_depth,
                                          range_node))) {
        LOG_WARN("failed to get implicit cast range", K(ret));
      }
    } else if (ObSQLUtils::is_min_cluster_version_ge_425_or_435() &&
               ObSQLUtils::is_opt_feature_version_ge_425_or_435(ctx_.optimizer_features_enable_version_) &&
               ((l_expr->get_expr_type() == T_FUN_SYS_SET_COLLATION && r_expr->is_const_expr()) ||
               (r_expr->get_expr_type() == T_FUN_SYS_SET_COLLATION && l_expr->is_const_expr()))) {
      if (OB_FAIL(get_implicit_set_collation_range(*l_ori_expr,
                                                   *r_ori_expr,
                                                   cmp_type,
                                                   expr_depth,
                                                   range_node))) {
        LOG_WARN("failed to get implicit set collation range", K(ret));
      }
    } else {
      if (OB_FAIL(generate_always_true_or_false_node(true, range_node))) {
        LOG_WARN("failed to generate always true node");
      }
    }
  } else if (OB_FAIL(get_row_cmp_node(*l_expr, *r_expr, cmp_type, expr_depth, range_node))) {
    LOG_WARN("get row key part failed.", K(ret));
  }

  return ret;
}

int ObExprRangeConverter::gen_column_cmp_node(const ObRawExpr &l_expr,
                                              const ObRawExpr &r_expr,
                                              ObItemType cmp_type,
                                              const ObRawExprResType &result_type,
                                              int64_t expr_depth,
                                              bool null_safe,
                                              ObRangeNode *&range_node)
{
  int ret = OB_SUCCESS;
  range_node = NULL;
  ctx_.cur_is_precise_ = true;
  const ObColumnRefRawExpr *column_expr = NULL;
  const ObRawExpr *const_expr = NULL;
  ObRangeColumnMeta *column_meta = nullptr;
  int64_t key_idx;
  int64_t const_val;
  bool is_valid = false;
  if (OB_LIKELY(l_expr.has_flag(IS_COLUMN))) {
    column_expr = static_cast<const ObColumnRefRawExpr *>(&l_expr);
    const_expr = &r_expr;
  } else {
    column_expr = static_cast<const ObColumnRefRawExpr *>(&r_expr);
    const_expr = &l_expr;
    cmp_type = get_opposite_compare_type(cmp_type);
  }

  bool always_true = true;
  if (!is_range_key(column_expr->get_column_id(), key_idx) ||
      OB_UNLIKELY(!const_expr->is_const_expr())) {
    always_true = true;
  } else if ((ctx_.column_flags_[key_idx] & RANGE_EXPR_EQUAL) != 0 &&
              const_expr->has_flag(CNT_DYNAMIC_PARAM)) {
    // Do not extract range for dynamic parameters when an equal range already exists for this column
    always_true = true;
  } else if (OB_ISNULL(column_meta = get_column_meta(key_idx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null column meta");
  } else if (!ObQueryRange::can_be_extract_range(cmp_type, column_meta->column_type_,
                                                 result_type, const_expr->get_result_type().get_type(),
                                                 always_true)) {
    // do nothing
  } else if (OB_FAIL(check_calculable_expr_valid(const_expr, is_valid))) {
    LOG_WARN("failed to get calculable expr val");
  } else if (!is_valid) {
    // do nothing
  } else if (OB_FAIL(get_final_expr_idx(const_expr, column_meta, const_val))) {
    LOG_WARN("failed to get final expr idx");
  } else if (OB_FAIL(alloc_range_node(range_node))) {
    LOG_WARN("failed to alloc common range node");
  } else {
    if (is_oracle_mode() && cmp_type == T_OP_GT &&
        ((column_meta->column_type_.get_type() == ObCharType && const_expr->get_result_type().get_type() == ObVarcharType) ||
         (column_meta->column_type_.get_type() == ObNCharType && const_expr->get_result_type().get_type() == ObNVarchar2Type))) {
        /* when char compare with varchar, same string may need return due to padding blank.
           e.g. c1(char(3)) > '1'(varchar(1)) will return '1  ' */
      cmp_type = T_OP_GE;
    }
    if (null_safe && OB_FAIL(ctx_.null_safe_value_idxs_.push_back(const_val))) {
      LOG_WARN("failed to push back null safe value index", K(const_val));
    //if current expr can be extracted to range, just store the expr
    } else if (OB_FAIL(fill_range_node_for_basic_cmp(cmp_type, key_idx, const_val, *range_node))) {
      LOG_WARN("get normal cmp keypart failed", K(ret));
    } else if (OB_FAIL(check_expr_precise(*const_expr, result_type, column_meta->column_type_))) {
      LOG_WARN("failed to check expr precise", K(ret));
    } else if (expr_depth == 0 && OB_FAIL(set_column_flags(key_idx, cmp_type))) {
      LOG_WARN("failed to set column flags", K(ret));
    }
  }
  if (OB_SUCC(ret) && nullptr == range_node) {
    ctx_.cur_is_precise_ = false;
    if (OB_FAIL(generate_always_true_or_false_node(always_true, range_node))) {
      LOG_WARN("failed to generate always true or fasle node", K(always_true));
    }
  }
  return ret;
}

int ObExprRangeConverter::gen_row_column_cmp_node(const ObIArray<const ObColumnRefRawExpr*> &l_column_exprs,
                                                  const ObIArray<const ObRawExpr*> &r_const_exprs,
                                                  ObItemType cmp_type,
                                                  const ObIArray<const ObObjMeta*> &calc_types,
                                                  int64_t expr_depth,
                                                  int64_t row_dim,
                                                  bool null_safe,
                                                  ObRangeNode *&range_node)
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 4> key_idxs;
  ObSEArray<int64_t, 4> val_idxs;
  bool always_true = true;
  ctx_.cur_is_precise_ = true;
  if (OB_UNLIKELY(l_column_exprs.count() != r_const_exprs.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected param count", K(l_column_exprs), K(r_const_exprs));
  } else if (T_OP_EQ == cmp_type || T_OP_NSEQ == cmp_type) {
    ObSEArray<int64_t, 4> ordered_key_idxs;
    ObSEArray<const ObRawExpr *, 4> const_exprs;
    ObSEArray<const ObRangeColumnMeta *, 4> column_metas;
    int64_t min_offset = ctx_.column_cnt_;
    for (int64_t i = 0; OB_SUCC(ret) && i < l_column_exprs.count(); ++i) {
      const ObObjMeta *calc_type = calc_types.at(i);
      const ObColumnRefRawExpr* column_expr = l_column_exprs.at(i);
      const ObRawExpr* const_expr = r_const_exprs.at(i);
      bool cur_always_true = true;
      bool is_valid = false;
      if (OB_NOT_NULL(column_expr) && OB_NOT_NULL(const_expr) && OB_NOT_NULL(calc_type)) {
        int64_t key_idx = -1;
        ObRangeColumnMeta *column_meta = nullptr;
        if (!is_range_key(column_expr->get_column_id(), key_idx)) {
          // do nothing
        } else if (ObOptimizerUtil::find_item(key_idxs, key_idx)) {
          // this key already exist. e.g. (c1,c1,c2) = (:1,:2,:3) will be trated as (c1,c2) = (:1,:3)
        } else if (OB_ISNULL(column_meta = get_column_meta(key_idx))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get null column meta");
        } else if (!ObQueryRange::can_be_extract_range(cmp_type, column_meta->column_type_,
                                                       *calc_type, const_expr->get_result_type().get_type(),
                                                       cur_always_true)) {
          if (!cur_always_true) {
            always_true = false;
          }
        } else if (OB_FAIL(check_calculable_expr_valid(const_expr, is_valid))) {
          LOG_WARN("failed to get calculable expr val");
        } else if (!is_valid) {
          // do nothing
        } else if (OB_FAIL(check_expr_precise(*const_expr, *calc_type, column_meta->column_type_))) {
          LOG_WARN("failed to check expr precise", K(ret));
        } else if (OB_FAIL(key_idxs.push_back(key_idx))) {
          LOG_WARN("failed to push back key idx", K(key_idx));
        } else if (OB_FAIL(const_exprs.push_back(const_expr))) {
          LOG_WARN("failed to push back const expr", KPC(const_expr));
        } else if (OB_FAIL(column_metas.push_back(column_meta))) {
          LOG_WARN("failed to push back column metas", K(ret));
        } else if (key_idx < min_offset) {
          min_offset = key_idx;
        }
      }
    }
    for (int64_t i = min_offset; OB_SUCC(ret) && i < ctx_.column_cnt_; ++i) {
      int64_t idx = -1;
      if (ObOptimizerUtil::find_item(key_idxs, i, &idx)) {
        int64_t const_val = -1;
        const ObRawExpr* const_expr = const_exprs.at(idx);
        const ObRangeColumnMeta *meta = column_metas.at(idx);
        if (OB_FAIL(get_final_expr_idx(const_expr, meta, const_val))) {
          LOG_WARN("failed to get final expr idx");
        } else if (OB_FAIL(ordered_key_idxs.push_back(i))) {
          LOG_WARN("failed to push back key idx", K(key_idxs));
        } else if (OB_FAIL(val_idxs.push_back(const_val))) {
          LOG_WARN("failed to push back key idx", K(const_val));
        } else if (null_safe && OB_FAIL(ctx_.null_safe_value_idxs_.push_back(const_val))) {
          LOG_WARN("failed to push back null safe value index", K(const_val));
        } else if (expr_depth == 0 && OB_FAIL(set_column_flags(idx, cmp_type))){
          LOG_WARN("failed to set column flags", K(ret));
        }
      } else {
        // only extract consistent column for row compare
        break;
      }
    }
    if (OB_SUCC(ret)) {
      if (ordered_key_idxs.count() < row_dim) {
        // part of row can't extract range.
        ctx_.cur_is_precise_ = false;
      }
      if (!key_idxs.empty()) {
        if (OB_FAIL(alloc_range_node(range_node))) {
          LOG_WARN("failed to alloc common range node");
        } else if (OB_FAIL(fill_range_node_for_basic_row_cmp(cmp_type, ordered_key_idxs, val_idxs, *range_node))) {
          LOG_WARN("failed to fill range node for basic row cmp");
        }
      }
    }
  } else {
    int64_t last_key_idx = -1;
    bool check_next = true;
    bool is_valid_decimal_int_range_cmp = true;
    for (int64_t i = 0; OB_SUCC(ret) && check_next && i < l_column_exprs.count(); ++i) {
      const ObObjMeta *calc_type = calc_types.at(i);
      const ObColumnRefRawExpr* column_expr = l_column_exprs.at(i);
      const ObRawExpr* const_expr = r_const_exprs.at(i);
      bool cur_always_true = true;
      bool is_valid = false;
      check_next = false;
      if (OB_NOT_NULL(column_expr) && OB_NOT_NULL(const_expr) && OB_NOT_NULL(calc_type)) {
        int64_t key_idx = -1;
        int64_t const_val = -1;
        ObRangeColumnMeta *column_meta = nullptr;
        if (!is_range_key(column_expr->get_column_id(), key_idx)) {
          // do nothing
        } else if (last_key_idx != -1 && key_idx != last_key_idx + 1) {
          // do nothing
        } else if (OB_ISNULL(column_meta = get_column_meta(key_idx))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get null column meta");
        } else if (const_expr->get_expr_type() == T_FUN_SYS_INNER_ROW_CMP_VALUE &&
                   OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(const_expr, const_expr, true))) {
          LOG_WARN("failed to get expr without lossless cast", K(ret));
        } else if (!ObQueryRange::can_be_extract_range(cmp_type, column_meta->column_type_,
                                                       *calc_type, const_expr->get_result_type().get_type(),
                                                       cur_always_true)) {
          if (i == 0 && !cur_always_true) {
            always_true = false;
          }
        } else if (OB_FAIL(check_calculable_expr_valid(const_expr, is_valid))) {
          LOG_WARN("failed to get calculable expr val");
        } else if (!is_valid) {
          // do nothing
        } else if (OB_FAIL(check_expr_precise(*const_expr, *calc_type, column_meta->column_type_))) {
          LOG_WARN("failed to check expr precise", K(ret));
        } else if (OB_FAIL(get_final_expr_idx(const_expr, column_meta, const_val))) {
          LOG_WARN("failed to get final expr idx");
        } else if (OB_FAIL(key_idxs.push_back(key_idx))) {
          LOG_WARN("failed to push back key idx", K(key_idx));
        } else if (OB_FAIL(val_idxs.push_back(const_val))) {
          LOG_WARN("failed to push back key idx", K(const_val));
        } else if (is_oracle_mode() && (cmp_type == T_OP_GT || cmp_type == T_OP_GE) &&
                   ((column_meta->column_type_.get_type() == ObCharType && const_expr->get_result_type().get_type() == ObVarcharType) ||
                    (column_meta->column_type_.get_type() == ObNCharType && const_expr->get_result_type().get_type() == ObNVarchar2Type))) {
          /* when char compare with varchar, same string may need return due to padding blank.
            e.g. c1(char(3)) > '1'(varchar(1)) will return '1  ' */
          // can not extract query range for next row item
          cmp_type = T_OP_GE;
        // The logic for c1 <=> null is reused from the original logic and will be converted to c1 >= null and c1 <= null, and it needs to be set to null safe.
        } else if (null_safe && OB_FAIL(ctx_.null_safe_value_idxs_.push_back(const_val))) {
          LOG_WARN("failed to push back null safe value index", K(const_val));
        } else if (expr_depth == 0 && OB_FAIL(set_column_flags(key_idx, cmp_type))) {
          LOG_WARN("failed to set column flags", K(ret));
        } else if (i > 0 && OB_FAIL(ctx_.non_first_in_row_value_idxs_.push_back(const_val))) {
          LOG_WARN("failed to push back value idx", K(const_val));
        } else if (i < l_column_exprs.count() - 1 &&
                   OB_NOT_NULL(r_const_exprs.at(i+1)) &&
                   OB_FAIL(check_decimal_int_range_cmp_valid(r_const_exprs.at(i+1), is_valid_decimal_int_range_cmp))) {
          LOG_WARN("fail to check can use ori cmp type", K(ret));
        } else {
          bool is_lt_with_lob = (cmp_type == T_OP_LT || cmp_type == T_OP_LE) &&
                                (ctx_.final_exprs_flag_.at(const_val)
                                     & OB_FINAL_EXPR_WITH_LOB_TRUNCATE)
                                     == OB_FINAL_EXPR_WITH_LOB_TRUNCATE;
          last_key_idx = key_idx;
          check_next = !is_lt_with_lob && is_valid_decimal_int_range_cmp;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (key_idxs.count() < row_dim) {
        ctx_.cur_is_precise_ = false;
        if (!is_valid_decimal_int_range_cmp) {
          // after single side cast, decimal int already handle range border
        } else if (T_OP_LT == cmp_type) {
          cmp_type = T_OP_LE;
        } else if (T_OP_GT == cmp_type) {
          cmp_type = T_OP_GE;
        }
      } else {
        if (T_OP_LE == cmp_type || T_OP_LT == cmp_type) {
          ctx_.cur_is_precise_ = lib::is_oracle_mode() ? true : false;
        } else {
          ctx_.cur_is_precise_ = lib::is_oracle_mode() ? false : true;
        }
      }
      if (!key_idxs.empty()) {
        if (OB_FAIL(alloc_range_node(range_node))) {
          LOG_WARN("failed to alloc common range node");
        } else if (OB_FAIL(fill_range_node_for_basic_row_cmp(cmp_type, key_idxs, val_idxs, *range_node))) {
          LOG_WARN("failed to fill range node for basic row cmp");
        }
      }
    }
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(nullptr == range_node)) {
    ctx_.cur_is_precise_ = false;
    if (OB_FAIL(generate_always_true_or_false_node(always_true, range_node))) {
      LOG_WARN("failed to generate always true or fasle node", K(always_true));
    }
  }
  return ret;
}

/**
 * convert `c1 is null` to range node
*/
int ObExprRangeConverter::convert_is_expr(const ObRawExpr *expr, int64_t expr_depth, ObRangeNode *&range_node)
{
  int ret = OB_SUCCESS;
  const ObRawExpr* l_expr = nullptr;
  const ObRawExpr* r_expr = nullptr;
  ctx_.cur_is_precise_ = false;
  if (OB_ISNULL(expr) ||
      OB_ISNULL(l_expr = expr->get_param_expr(0)) ||
      OB_ISNULL(r_expr = expr->get_param_expr(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null expr", K(expr), K(l_expr), K(r_expr));
  } else if (ObNullType == r_expr->get_result_type().get_type()) {
    if (OB_FAIL(gen_is_null_range_node(l_expr, expr_depth, range_node))) {
      LOG_WARN("failed to gen is null expr", K(ret));
    }
  }
  if (OB_SUCC(ret) && nullptr == range_node) {
    if (OB_FAIL(generate_always_true_or_false_node(true, range_node))) {
      LOG_WARN("failed to generate always true or fasle node");
    }
  }
  return ret;
}

/**
 * convert `c1 is not null` to range node
 * this is for transform "fast min/max", details are in dima-2025032400107742164
*/
int ObExprRangeConverter::convert_is_not_expr(const ObRawExpr *expr, int64_t expr_depth, ObRangeNode *&range_node)
{
  int ret = OB_SUCCESS;
  const ObRawExpr* l_expr = nullptr;
  const ObRawExpr* r_expr = nullptr;
  ctx_.cur_is_precise_ = false;
  if (OB_ISNULL(expr) ||
      OB_ISNULL(l_expr = expr->get_param_expr(0)) ||
      OB_ISNULL(r_expr = expr->get_param_expr(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null expr", K(expr), K(l_expr), K(r_expr));
  } else if (ObNullType == r_expr->get_result_type().get_type()) {
    if (OB_FAIL(gen_is_not_null_range_node(l_expr, expr_depth, range_node))) {
      LOG_WARN("failed to gen is null expr", K(ret));
    }
  }
  if (OB_SUCC(ret) && nullptr == range_node) {
    if (OB_FAIL(generate_always_true_or_false_node(true, range_node))) {
      LOG_WARN("failed to generate always true or fasle node");
    }
  }
  return ret;
}

int ObExprRangeConverter::gen_is_null_range_node(const ObRawExpr *l_expr, int64_t expr_depth, ObRangeNode *&range_node)
{
  int ret = OB_SUCCESS;
  int64_t key_idx = -1;
  range_node = nullptr;
  ctx_.cur_is_precise_ = false;
  bool use_implicit_cast_feature = true;
  if (OB_ISNULL(l_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(l_expr, l_expr, use_implicit_cast_feature))) {
    LOG_WARN("failed to get expr without lossless cast", K(ret));
  } else if (!l_expr->has_flag(IS_COLUMN)) {
    // do nothing
  } else if (!is_range_key(static_cast<const ObColumnRefRawExpr*>(l_expr)->get_column_id(), key_idx)) {
    // do nothing
  } else if (OB_FAIL(alloc_range_node(range_node))) {
    LOG_WARN("failed to alloc common range node");
  } else if (OB_FAIL(fill_range_node_for_basic_cmp(T_OP_NSEQ, key_idx, OB_RANGE_NULL_VALUE, *range_node))) {
    LOG_WARN("get normal cmp keypart failed", K(ret));
  } else if (expr_depth == 0 && OB_FAIL(set_column_flags(key_idx, T_OP_IS))) {
      LOG_WARN("failed to set column flags", K(ret));
  } else {
    ctx_.cur_is_precise_ = true;
  }

  if (OB_SUCC(ret) && nullptr == range_node) {
    if (OB_FAIL(generate_always_true_or_false_node(true, range_node))) {
      LOG_WARN("failed to generate always true or fasle node");
    }
  }
  return ret;
}

int ObExprRangeConverter::gen_is_not_null_range_node(const ObRawExpr *l_expr, int64_t expr_depth, ObRangeNode *&range_node)
{
  int ret = OB_SUCCESS;
  int64_t key_idx = -1;
  range_node = nullptr;
  ctx_.cur_is_precise_ = false;
  bool use_implicit_cast_feature = true;
  if (OB_ISNULL(l_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(l_expr, l_expr, use_implicit_cast_feature))) {
    LOG_WARN("failed to get expr without lossless cast", K(ret));
  } else if (!l_expr->has_flag(IS_COLUMN)) {
    // do nothing
  } else if (!is_range_key(static_cast<const ObColumnRefRawExpr*>(l_expr)->get_column_id(), key_idx)) {
    // do nothing
  } else if (OB_FAIL(alloc_range_node(range_node))) {
    LOG_WARN("failed to alloc common range node");
  } else if (OB_FAIL(fill_range_node_for_is_not_null(key_idx, *range_node))) {
    LOG_WARN("get normal cmp keypart failed", K(ret));
  } else if (expr_depth == 0 && OB_FAIL(set_column_flags(key_idx, T_OP_IS_NOT))) {
      LOG_WARN("failed to set column flags", K(ret));
  } else {
    ctx_.cur_is_precise_ = true;
  }

  if (OB_SUCC(ret) && nullptr == range_node) {
    if (OB_FAIL(generate_always_true_or_false_node(true, range_node))) {
      LOG_WARN("failed to generate always true or fasle node");
    }
  }
  return ret;
}

/**
 * convert `c1 between :0 and :1` to range node
*/
int ObExprRangeConverter::convert_between_expr(const ObRawExpr *expr,
                                               int64_t expr_depth,
                                               ObRangeNode *&range_node)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRangeNode*, 2> range_nodes;
  const ObRawExpr *expr1 = nullptr;
  const ObRawExpr *expr2 = nullptr;
  const ObRawExpr *expr3 = nullptr;
  ObRangeNode *tmp_node = nullptr;
  bool first_is_precise = true;
  ctx_.cur_is_precise_ = false;
  if (OB_ISNULL(expr) || OB_UNLIKELY(expr->get_param_count() != 3) ||
      OB_ISNULL(expr1 = expr->get_param_expr(0)) ||
      OB_ISNULL(expr2 = expr->get_param_expr(1)) ||
      OB_ISNULL(expr3 = expr->get_param_expr(2))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected expr", KPC(expr), KPC(expr1), KPC(expr2), KPC(expr3));
  } else if (OB_FAIL(get_basic_range_node(expr1, expr2, T_OP_GE, expr_depth, tmp_node))) {
    LOG_WARN("failed tp get basic range node", K(ret));
  } else if (OB_FAIL(range_nodes.push_back(tmp_node))) {
    LOG_WARN("failed to push back range node");
  } else if (OB_FALSE_IT(first_is_precise = ctx_.cur_is_precise_)) {
  } else if (OB_FAIL(get_basic_range_node(expr1, expr3, T_OP_LE, expr_depth, tmp_node))) {
    LOG_WARN("failed tp get basic range node", K(ret));
  } else if (OB_FAIL(range_nodes.push_back(tmp_node))) {
    LOG_WARN("failed to push back range node");
  } else if (OB_FAIL(ObRangeGraphGenerator::and_range_nodes(range_nodes, ctx_, range_node))) {
    LOG_WARN("failed to and range nodes");
  } else {
    ctx_.cur_is_precise_ = first_is_precise && ctx_.cur_is_precise_;
    if (ctx_.cur_is_precise_) {
      ctx_.refresh_max_offset_ = true;
    }
  }
  return ret;
}

int ObExprRangeConverter::convert_not_between_expr(const ObRawExpr *expr,
                                                   int64_t expr_depth,
                                                   ObRangeNode *&range_node)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRangeNode*, 2> range_nodes;
  const ObRawExpr *expr1 = nullptr;
  const ObRawExpr *expr2 = nullptr;
  const ObRawExpr *expr3 = nullptr;
  ObRangeNode *tmp_node = nullptr;
  bool first_is_precise = true;
  ctx_.cur_is_precise_ = false;
  if (OB_ISNULL(expr) || OB_UNLIKELY(expr->get_param_count() != 3) ||
      OB_ISNULL(expr1 = expr->get_param_expr(0)) ||
      OB_ISNULL(expr2 = expr->get_param_expr(1)) ||
      OB_ISNULL(expr3 = expr->get_param_expr(2))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected expr", KPC(expr), KPC(expr1), KPC(expr2), KPC(expr3));
  } else if (OB_FAIL(get_basic_range_node(expr1, expr2, T_OP_LT, expr_depth + 1, tmp_node))) {
    LOG_WARN("failed tp get basic range node", K(ret));
  } else if (OB_FAIL(range_nodes.push_back(tmp_node))) {
    LOG_WARN("failed to push back range node");
  } else if (OB_FALSE_IT(first_is_precise = ctx_.cur_is_precise_)) {
  } else if (OB_FAIL(get_basic_range_node(expr1, expr3, T_OP_GT, expr_depth + 1, tmp_node))) {
    LOG_WARN("failed tp get basic range node", K(ret));
  } else if (OB_FAIL(range_nodes.push_back(tmp_node))) {
    LOG_WARN("failed to push back range node");
  } else if (OB_FAIL(ObRangeGraphGenerator::or_range_nodes(*this, range_nodes, ctx_.column_cnt_, range_node))) {
    LOG_WARN("failed to or range nodes");
  } else {
    ctx_.cur_is_precise_ = first_is_precise && ctx_.cur_is_precise_;
    if (ctx_.cur_is_precise_) {
      ctx_.refresh_max_offset_ = true;
    }
  }
  return ret;
}

int ObExprRangeConverter::convert_not_equal_expr(const ObRawExpr *expr, int64_t expr_depth, ObRangeNode *&range_node)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRangeNode*, 2> range_nodes;
  const ObRawExpr *l_expr = nullptr;
  const ObRawExpr *r_expr = nullptr;
  ObRangeNode *tmp_node = nullptr;
  bool first_is_precise = true;
  ctx_.cur_is_precise_ = false;
  if (OB_ISNULL(expr) || OB_UNLIKELY(expr->get_param_count() != 2) ||
      OB_ISNULL(l_expr = expr->get_param_expr(0)) ||
      OB_ISNULL(r_expr = expr->get_param_expr(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected expr", KPC(expr), KPC(l_expr), KPC(r_expr));
  } else  if (T_OP_ROW == l_expr->get_expr_type()) {
    // do not convert row not equal expr
    if (OB_FAIL(generate_always_true_or_false_node(true, range_node))) {
      LOG_WARN("failed to generate always true or fasle node");
    }
  } else if (OB_FAIL(get_basic_range_node(l_expr, r_expr, T_OP_LT, expr_depth, tmp_node))) {
    LOG_WARN("failed tp get basic range node", K(ret));
  } else if (OB_FAIL(range_nodes.push_back(tmp_node))) {
    LOG_WARN("failed to push back range node");
  } else if (OB_FALSE_IT(first_is_precise = ctx_.cur_is_precise_)) {
  } else if (OB_FAIL(get_basic_range_node(l_expr, r_expr, T_OP_GT, expr_depth, tmp_node))) {
    LOG_WARN("failed tp get basic range node", K(ret));
  } else if (OB_FAIL(range_nodes.push_back(tmp_node))) {
    LOG_WARN("failed to push back range node");
  } else if (OB_FAIL(ObRangeGraphGenerator::or_range_nodes(*this, range_nodes, ctx_.column_cnt_, range_node))) {
    LOG_WARN("failed to or range nodes");
  } else {
    ctx_.cur_is_precise_ = first_is_precise && ctx_.cur_is_precise_;
  }
  return ret;
}

int ObExprRangeConverter::convert_like_expr(const ObRawExpr *expr, int64_t expr_depth, ObRangeNode *&range_node)
{
  int ret = OB_SUCCESS;
  const ObRawExpr *l_expr = nullptr;
  const ObRawExpr *pattern_expr = nullptr;
  const ObRawExpr *escape_expr = nullptr;
  ObRangeNode *tmp_node = nullptr;
  bool always_true = true;
  ctx_.cur_is_precise_ = false;
  bool use_implicit_cast_feature = true;
  if (OB_ISNULL(expr) || OB_UNLIKELY(expr->get_param_count() != 3) ||
      OB_ISNULL(l_expr = expr->get_param_expr(0)) ||
      OB_ISNULL(pattern_expr = expr->get_param_expr(1)) ||
      OB_ISNULL(escape_expr = expr->get_param_expr(2))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected expr", KPC(expr), KPC(l_expr), KPC(pattern_expr), KPC(escape_expr));
  } else if (OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(l_expr, l_expr, use_implicit_cast_feature))) {
    LOG_WARN("failed to get expr without lossless cast", K(ret));
  } else if (l_expr->has_flag(IS_COLUMN) &&
             pattern_expr->is_const_expr() &&
             escape_expr->is_const_expr() &&
             !escape_expr->has_flag(CNT_DYNAMIC_PARAM)) {
    const ObColumnRefRawExpr *column_expr = static_cast<const ObColumnRefRawExpr *>(l_expr);
    int64_t key_idx = -1;
    int64_t start_val_idx = -1;
    int64_t end_val_idx = -1;
    char escape_ch = 0x00;
    bool is_valid = false;
    ObRangeColumnMeta *column_meta = nullptr;
    if (!is_range_key(column_expr->get_column_id(), key_idx)) {
      // do nothing
    } else if (OB_ISNULL(column_meta = get_column_meta(key_idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null column meta");
    } else if (!ObQueryRange::can_be_extract_range(T_OP_LIKE, column_meta->column_type_,
                                                   expr->get_param_expr(0)->get_result_type(),
                                                   pattern_expr->get_result_type().get_type(),
                                                   always_true)) {
      // do nothing
    } else if (OB_FAIL(check_calculable_expr_valid(pattern_expr, is_valid))) {
      LOG_WARN("failed to get calculable expr val");
    } else if (!is_valid) {
      // do nothing
    } else if (OB_FAIL(check_escape_valid(escape_expr, escape_ch, is_valid))) {
      LOG_WARN("failed to check escape is valid", KPC(escape_expr));
    } else if (!is_valid) {
      // do nothing
    } else if (OB_FAIL(build_decode_like_expr(const_cast<ObRawExpr*>(pattern_expr),
                                              const_cast<ObRawExpr*>(escape_expr),
                                              escape_ch, column_meta, start_val_idx, end_val_idx))) {
      LOG_WARN("failed to get final expr idx");
    } else if (OB_FAIL(alloc_range_node(range_node))) {
      LOG_WARN("failed to alloc common range node");
    } else if (OB_FAIL(fill_range_node_for_like(key_idx, start_val_idx, end_val_idx, *range_node))) {
      LOG_WARN("get normal cmp keypart failed", K(ret));
    } else if (OB_FAIL(check_expr_precise(*pattern_expr, pattern_expr->get_result_type(),
                                          column_meta->column_type_))) {
      LOG_WARN("failed to check expr precise", K(ret));
    } else if (expr_depth == 0 && OB_FAIL(set_column_flags(key_idx, T_OP_LIKE))) {
      LOG_WARN("failed to set column flags", K(ret));
    } else if (is_oracle_mode()) {
      // NChar like Nchar, Char like Char is not precise due to padding blank characters
      ObObjType column_type = column_meta->column_type_.get_type();
      ObObjType const_type = pattern_expr->get_result_type().get_type();
      if ((ObCharType == column_type && ObCharType == const_type) ||
          (ObNCharType == column_type && ObNCharType == const_type)) {
        ctx_.cur_is_precise_ = false;
      }
    }
  }
  if (OB_SUCC(ret) && nullptr == range_node) {
    if (OB_FAIL(generate_always_true_or_false_node(always_true, range_node))) {
      LOG_WARN("failed to generate always true or fasle node");
    }
  }
  return ret;
}

int ObExprRangeConverter::check_escape_valid(const ObRawExpr *escape, char &escape_ch, bool &is_valid)
{
  int ret = OB_SUCCESS;
  ObObj escape_val;
  is_valid = false;
  escape_ch = 0x00;
  if (OB_FAIL(get_calculable_expr_val(escape, escape_val, is_valid))) {
    LOG_WARN("failed to get calculable expr val", KPC(escape));
  } else if (!is_valid) {
    // do nothing
  } else if (escape_val.is_null()) {
    escape_ch = '\\';
  } else if (ObCharset::is_cs_nonascii(escape_val.get_collation_type())) {
    ObString escape_str;
    ObString escape_dst;
    if (OB_FAIL(escape_val.get_string(escape_str))) {
      LOG_WARN("failed to get escape string", K(escape), K(ret));
    } else if (OB_FAIL(ObCharset::charset_convert(allocator_, escape_str, escape_val.get_collation_type(),
                                                  CS_TYPE_UTF8MB4_GENERAL_CI, escape_dst, true))) {
      LOG_WARN("failed to do charset convert", K(ret), K(escape_str));
    } else if (escape_dst.length() > 1) {
      is_valid = false;
    } else {
      escape_ch = (escape_dst.length() == 0) ? 0x00 : *(escape_dst.ptr());
    }
  } else if (escape_val.get_string_len() > 1) {
    is_valid = false;
  } else {
    escape_ch = (escape_val.get_string_len() == 0) ? 0x00 : *(escape_val.get_string_ptr());
  }
  return ret;
}

int ObExprRangeConverter::build_decode_like_expr(ObRawExpr *pattern,
                                                 ObRawExpr *escape,
                                                 char escape_ch,
                                                 ObRangeColumnMeta *column_meta,
                                                 int64_t &start_val_idx,
                                                 int64_t &end_val_idx)
{
  int ret = OB_SUCCESS;
  ObRawExpr *decode_like_expr = nullptr;
  ObConstRawExpr *pos_expr = NULL;
  ObConstRawExpr *column_type_expr = NULL;
  ObConstRawExpr *collation_type_expr = NULL;
  ObConstRawExpr *column_length_expr = NULL;
  ObSysFunRawExpr *start_expr = NULL;
  ObSysFunRawExpr *end_expr = NULL;
  if (OB_ISNULL(pattern) || OB_ISNULL(escape) || OB_ISNULL(column_meta) || OB_ISNULL(ctx_.expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(pattern), K(escape), K(column_meta), K(ctx_.expr_factory_));
  } else if (OB_FAIL(ctx_.expr_factory_->create_raw_expr(T_FUN_SYS_INNER_DECODE_LIKE, start_expr))) {
    LOG_WARN("failed to create a new expr", K(ret));
  } else if (OB_FAIL(ctx_.expr_factory_->create_raw_expr(T_FUN_SYS_INNER_DECODE_LIKE, end_expr))) {
    LOG_WARN("failed to create a new expr", K(ret));
  } else if (OB_ISNULL(start_expr) || OB_ISNULL(end_expr)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate expr", K(start_expr), K(end_expr));
  } else if (OB_FAIL(start_expr->init_param_exprs(6))) {
    LOG_WARN("failed to init param exprs", K(ret));
  } else if (OB_FAIL(end_expr->init_param_exprs(6))) {
    LOG_WARN("failed to init param exprs", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*ctx_.expr_factory_,
                                                          ObIntType,
                                                          static_cast<int64_t>(column_meta->column_type_.get_type()),
                                                          column_type_expr))) {
    LOG_WARN("Failed to build const expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*ctx_.expr_factory_,
                                                          ObIntType,
                                                          static_cast<int64_t>(column_meta->column_type_.get_collation_type()),
                                                          collation_type_expr))) {
    LOG_WARN("Failed to build const expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*ctx_.expr_factory_,
                                                          ObIntType,
                                                          column_meta->column_type_.get_accuracy().get_length(),
                                                          column_length_expr))) {
    LOG_WARN("Failed to build const expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*ctx_.expr_factory_,
                                                          ObIntType,
                                                          1, // const value
                                                          pos_expr))) {
    LOG_WARN("Failed to build const expr", K(ret));
  } else if (OB_FAIL(start_expr->add_param_expr(pattern)) ||
             OB_FAIL(start_expr->add_param_expr(escape)) ||
             OB_FAIL(start_expr->add_param_expr(pos_expr)) ||
             OB_FAIL(start_expr->add_param_expr(column_type_expr)) ||
             OB_FAIL(start_expr->add_param_expr(collation_type_expr)) ||
             OB_FAIL(start_expr->add_param_expr(column_length_expr))) {
    LOG_WARN("failed to set params for decode like expr", KPC(start_expr));
  } else if (OB_FAIL(start_expr->formalize(ctx_.session_info_))) {
    LOG_WARN("failed to formalize expr");
  } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*ctx_.expr_factory_,
                                                          ObIntType,
                                                          0, // const value
                                                          pos_expr))) {
    LOG_WARN("Failed to build const expr", K(ret));
  } else if (OB_FAIL(end_expr->add_param_expr(pattern)) ||
             OB_FAIL(end_expr->add_param_expr(escape)) ||
             OB_FAIL(end_expr->add_param_expr(pos_expr)) ||
             OB_FAIL(end_expr->add_param_expr(column_type_expr)) ||
             OB_FAIL(end_expr->add_param_expr(collation_type_expr)) ||
             OB_FAIL(end_expr->add_param_expr(column_length_expr))) {
    LOG_WARN("failed to set params for decode like expr", KPC(start_expr));
  } else if (OB_FAIL(end_expr->formalize(ctx_.session_info_))) {
    LOG_WARN("failed to formalize expr");
  } else if (OB_FAIL(get_final_expr_idx(start_expr, nullptr, start_val_idx))) {
    LOG_WARN("failed to get final expr idx");
  } else if (OB_FAIL(get_final_expr_idx(end_expr, nullptr, end_val_idx))) {
    LOG_WARN("failed to get final expr idx");
  }

  //generate constraint if like range is precise
  if (OB_SUCC(ret) && ctx_.params_ != nullptr &&
      !pattern->has_flag(CNT_DYNAMIC_PARAM)) {
    ObObj pattern_val;
    bool is_valid = false;
    if (OB_FAIL(get_calculable_expr_val(pattern, pattern_val, is_valid))) {
      LOG_WARN("failed to get calculable expr val", KPC(pattern));
    } else if (!is_valid) {
      ctx_.cur_is_precise_ = false;
    } else if(pattern_val.is_null()) {
      ctx_.cur_is_precise_ = false;
    } else if(OB_FAIL(ObQueryRange::is_precise_like_range(pattern_val,
                                                          escape_ch,
                                                          ctx_.cur_is_precise_))) {
      LOG_WARN("failed to jugde whether is precise", K(ret));
    } else if (OB_FAIL(add_precise_constraint(pattern, ctx_.cur_is_precise_))) {
      LOG_WARN("failed to add precise constraint", K(ret));
    } else if (OB_FAIL(add_prefix_pattern_constraint(pattern))) {
      LOG_WARN("failed to add prefix pattern constraint", K(ret));
    }
  }
  return ret;
}


/**
 * convert `c1 in (xxx)` or `(c1,c2) in (xxx)` to range node
*/
int ObExprRangeConverter::convert_in_expr(const ObRawExpr *expr, int64_t expr_depth, ObRangeNode *&range_node)
{
  int ret = OB_SUCCESS;
  const ObRawExpr* l_expr = nullptr;
  const ObRawExpr* r_expr = nullptr;
  ctx_.cur_is_precise_ = true;
  bool use_implicit_cast_feature = true;
  if (OB_ISNULL(expr) ||
      OB_ISNULL(l_expr = expr->get_param_expr(0)) ||
      OB_ISNULL(r_expr = expr->get_param_expr(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null expr", K(expr), K(l_expr), K(r_expr));
  } else if (OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(l_expr, l_expr, use_implicit_cast_feature))) {
    LOG_WARN("failed to get expr without lossless cast", K(ret));
  } else if (l_expr->get_expr_type() == T_OP_ROW) {
    if (OB_FAIL(get_row_in_range_ndoe(*l_expr, *r_expr, expr->get_result_type(), expr_depth, range_node))) {
      LOG_WARN("failed to get row in range node");
    }
  } else if (l_expr->is_column_ref_expr()) {
    if (OB_FAIL(get_single_in_range_node(static_cast<const ObColumnRefRawExpr *>(l_expr),
                                         r_expr, expr->get_param_expr(0)->get_result_type(),
                                         expr_depth, range_node))) {
      LOG_WARN("failed to get single in range node");
    }
  } else if (l_expr->has_flag(IS_ROWID)) {
    if (OB_FAIL(get_single_rowid_in_range_node(*l_expr, *r_expr, range_node))) {
      LOG_WARN("failed to get single row in range node", K(ret));
    }
  } else if (ObSQLUtils::is_min_cluster_version_ge_425_or_435() &&
             ObSQLUtils::is_opt_feature_version_ge_425_or_435(ctx_.optimizer_features_enable_version_) &&
             (l_expr->get_expr_type() == T_FUN_SYS_SET_COLLATION ||
              (l_expr->get_expr_type() == T_FUN_SYS_CAST &&
               l_expr->get_result_type().is_string_type()))) {
    if (OB_FAIL(get_implicit_set_collation_in_range(l_expr, r_expr, expr->get_param_expr(0)->get_result_type(),
                                                    expr_depth, range_node))) {
      LOG_WARN("failed to get implicit set collation range", K(ret));
    }
  }

  if (OB_SUCC(ret) && nullptr == range_node) {
    if (OB_FAIL(generate_always_true_or_false_node(true, range_node))) {
      LOG_WARN("failed to generate always true or fasle node");
    }
  }
  return ret;
}

int ObExprRangeConverter::get_single_in_range_node(const ObColumnRefRawExpr *column_expr,
                                                   const ObRawExpr *r_expr,
                                                   const ObRawExprResType &res_type,
                                                   int64_t expr_depth,
                                                   ObRangeNode *&range_node)
{
  int ret = OB_SUCCESS;
  bool always_true = false;
  ObRangeColumnMeta *column_meta = nullptr;
  int64_t key_idx = -1;
  ObSEArray<int64_t, 4> val_idxs;
  if (OB_ISNULL(column_expr) || OB_ISNULL(r_expr) ||
      OB_UNLIKELY(r_expr->get_param_count() == 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected param", KPC(column_expr), KPC(r_expr));
  } else if (!is_range_key(column_expr->get_column_id(), key_idx)) {
    always_true = true;
  } else if ((ctx_.column_flags_[key_idx] & (RANGE_EXPR_EQUAL | RANGE_EXPR_IN)) != 0) {
    always_true = true;
  } else if (OB_ISNULL(column_meta = get_column_meta(key_idx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null column meta");
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !always_true && i < r_expr->get_param_count(); ++i) {
      const ObRawExpr *const_expr = r_expr->get_param_expr(i);
      bool cur_can_be_extract = true;
      bool cur_always_true = true;
      bool is_valid = true;
      int64_t val_idx = -1;
      if (OB_ISNULL(const_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null expr");
      } else if (OB_UNLIKELY(!const_expr->is_const_expr())) {
        cur_can_be_extract = false;
        cur_always_true = true;
      } else if (!ObQueryRange::can_be_extract_range(T_OP_EQ, column_meta->column_type_,
                                                     res_type,
                                                     const_expr->get_result_type().get_type(),
                                                     cur_always_true)) {
        cur_can_be_extract = false;
      } else if (OB_FAIL(check_calculable_expr_valid(const_expr, is_valid))) {
        LOG_WARN("failed to get calculable expr val");
      } else if (!is_valid) {
        cur_can_be_extract = false;
        cur_always_true = true;
      } else if (OB_FAIL(get_final_expr_idx(const_expr, column_meta, val_idx))) {
        LOG_WARN("failed to get final expr idx", K(ret));
      } else if (OB_FAIL(check_expr_precise(*const_expr, const_expr->get_result_type(),
                                            column_meta->column_type_))) {
        LOG_WARN("failed to check expr precise", K(ret));
      } else if (OB_FAIL(val_idxs.push_back(val_idx))) {
        LOG_WARN("failed to push back val idx", K(val_idx));
      }

      if (OB_SUCC(ret) && !cur_can_be_extract && cur_always_true) {
        // for always false, just no need to add the value to in param
        always_true = true;
      }
    }
  }

  if (OB_SUCC(ret) && !always_true) {
    InParam *in_param = nullptr;
    int64_t param_idx;
    if (val_idxs.empty()) {
      // c1 in (null, null, null)
      if (OB_FAIL(generate_always_true_or_false_node(false, range_node))) {
        LOG_WARN("failed to generate always fasle node");
      }
    } else if (OB_FAIL(alloc_range_node(range_node))) {
      LOG_WARN("failed to alloc common range node");
    } else if (OB_FAIL(get_final_in_array_idx(in_param, param_idx))) {
      LOG_WARN("failed to get final in array idx");
    } else if (OB_FAIL(in_param->assign(val_idxs))) {
      LOG_WARN("failed to assign in params");
    } else if (OB_FAIL(fill_range_node_for_basic_cmp(T_OP_EQ, key_idx, param_idx, *range_node))) {
      LOG_WARN("failed to fill range node for basic cmp");
    } else if (expr_depth == 0 && set_column_flags(key_idx, T_OP_IN)) {
      LOG_WARN("failed to set column flags", K(ret));
    } else {
      range_node->contain_in_ = true;
      range_node->in_param_count_ = val_idxs.count();
    }
  }
  return ret;
}

int ObExprRangeConverter::get_row_in_range_ndoe(const ObRawExpr &l_expr,
                                                const ObRawExpr &r_expr,
                                                const ObRawExprResType &res_type,
                                                int64_t expr_depth,
                                                ObRangeNode *&range_node)
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 4> key_idxs;
  ObSEArray<int64_t, 4> val_idxs;
  ObSEArray<int64_t, 4> key_offsets;
  ObSEArray<ObRangeColumnMeta*, 4> column_metas;
  ObSEArray<int64_t, 4> tmp_key_idxs;
  ObSEArray<int64_t, 4> tmp_key_offsets;
  ObSEArray<ObRangeColumnMeta*, 4> tmp_column_metas;
  int64_t min_offset = ctx_.column_cnt_;
  bool always_true_or_false = true;
  bool use_implicit_cast_feature = true;
  // 1. get all valid key and offset
  for (int64_t i = 0; OB_SUCC(ret) && i < l_expr.get_param_count(); ++i) {
    const ObRawExpr* l_param = l_expr.get_param_expr(i);
    const ObColumnRefRawExpr* column_expr = nullptr;
    ObRangeColumnMeta *column_meta = nullptr;
    if (OB_ISNULL(l_param)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null expr", K(l_param));
    } else if (OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(l_param, l_param, use_implicit_cast_feature))) {
      LOG_WARN("failed to get expr without lossless cast", K(ret));
    } else if (l_param->is_column_ref_expr()) {
      column_expr = static_cast<const ObColumnRefRawExpr*>(l_param);
      int64_t key_idx = -1;
      if (!is_range_key(column_expr->get_column_id(), key_idx)) {
        // do nothing
      } else if (ObOptimizerUtil::find_item(key_idxs, key_idx)) {
        // this key already exist. e.g. (c1,c1,c2) in ((:1,:2,:3), (:4,:5,:6))
        // will be trated as (c1,c2) in ((:1,:3), (:4,:6))
      } else if (OB_ISNULL(column_meta = get_column_meta(key_idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null column meta");
      } else if (OB_FAIL(tmp_key_idxs.push_back(key_idx))) {
        LOG_WARN("failed to push back key idx", K(key_idx));
      } else if (OB_FAIL(tmp_key_offsets.push_back(i))) {
        LOG_WARN("failed to add member to bitmap", K(i));
      } else if (OB_FAIL(tmp_column_metas.push_back(column_meta))) {
        LOG_WARN("failed to push back column meta");
      } else if (key_idx < min_offset) {
        min_offset = key_idx;
      }
    }
  }

  // 2. get consistent column idx
  if (OB_SUCC(ret) && tmp_key_idxs.count() > 0) {
    bool need_extract = false;
    for (int64_t i = min_offset; OB_SUCC(ret) && i < ctx_.column_cnt_; ++i) {
      int64_t idx = -1;
      if (ObOptimizerUtil::find_item(tmp_key_idxs, i, &idx)) {
        if (OB_FAIL(key_idxs.push_back(i))) {
          LOG_WARN("failed to push back key idx", K(tmp_key_idxs));
        } else if (OB_FAIL(key_offsets.push_back(tmp_key_offsets.at(idx)))) {
          LOG_WARN("failed to push back key idx", K(tmp_key_offsets), K(idx));
        } else if (OB_FAIL(column_metas.push_back(tmp_column_metas.at(idx)))) {
          LOG_WARN("failed to push back key idx", K(tmp_column_metas), K(idx));
        }
      } else {
        // only extract consistent column for row compare
        break;
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && !need_extract && i < key_idxs.count(); ++i) {
      int64_t idx = key_idxs.at(i);
      if ((ctx_.column_flags_[idx] & (RANGE_EXPR_EQUAL | RANGE_EXPR_IN)) == 0) {
        need_extract = true;
      }
    }
    if (OB_SUCC(ret) && !need_extract) {
      key_idxs.reuse();
    }
  }

  // 3. get all valid in param
  if (OB_SUCC(ret) && key_idxs.count() > 0) {
    ObArenaAllocator alloc("ExprRangeAlloc", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    ObSEArray<const ObRawExpr*, 4> cur_val_exprs;
    ObSEArray<TmpExprArray*, 4> all_val_exprs;
    const int64_t row_dimension = l_expr.get_param_count();
    int64_t in_param_count = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < key_idxs.count(); ++i) {
      void *ptr = alloc.alloc(sizeof(TmpExprArray));
      if (OB_ISNULL(ptr)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for se array");
      } else {
        TmpExprArray *val_exprs = new(ptr)TmpExprArray();
        val_exprs->set_attr(ObMemAttr(MTL_ID(), "ExprRangeCvt"));
        ret = all_val_exprs.push_back(val_exprs);
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && !key_offsets.empty() && i < r_expr.get_param_count(); ++i) {
      const ObRawExpr *row_expr = r_expr.get_param_expr(i);
      const ObRawExpr *const_expr = nullptr;
      const ObRawExpr *ori_column_expr = nullptr;
      bool need_add = true;
      cur_val_exprs.reuse();
      if (OB_ISNULL(row_expr) || OB_UNLIKELY(row_expr->get_expr_type() != T_OP_ROW)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected expr", KPC(row_expr));
      }
      for (int64_t j = 0; OB_SUCC(ret) && need_add && j < key_offsets.count(); ++j) {
        int64_t val_offset = key_offsets.at(j);
        ObRangeColumnMeta* column_meta = column_metas.at(j);
        bool cur_can_be_extract = true;
        bool cur_always_true = true;
        bool is_valid = false;
        if (OB_UNLIKELY(val_offset >= row_expr->get_param_count()) ||
            OB_ISNULL(const_expr = row_expr->get_param_expr(val_offset)) ||
            OB_ISNULL(ori_column_expr = l_expr.get_param_expr(val_offset))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected expr", K(val_offset), KPC(const_expr));
        } else if (OB_UNLIKELY(!const_expr->is_const_expr())) {
          cur_can_be_extract = false;
          cur_always_true = true;
        } else if (!ObQueryRange::can_be_extract_range(T_OP_EQ,
                                                       column_meta->column_type_,
                                                       ori_column_expr->get_result_type(),
                                                       const_expr->get_result_type().get_type(),
                                                       cur_always_true)) {
          cur_can_be_extract = false;
        } else if (OB_FAIL(check_calculable_expr_valid(const_expr, is_valid))) {
          LOG_WARN("failed to get calculable expr val");
        } else if (!is_valid) {
          cur_can_be_extract = false;
          cur_always_true = true;
        } else if (OB_FAIL(OB_FAIL(cur_val_exprs.push_back(const_expr)))) {
          LOG_WARN("failed to push back expr");
        } else if (OB_FAIL(check_expr_precise(*const_expr, const_expr->get_result_type(),
                                              column_meta->column_type_))) {
          LOG_WARN("failed to check expr precise", K(ret));
        }

        if (OB_SUCC(ret) && !cur_can_be_extract) {
          if (cur_always_true) {
            // current key cannot extract range, remove current and subsequent keys
            for (int64_t k = key_offsets.count() - 1; OB_SUCC(ret) && k >= j; --k) {
              TmpExprArray *val_exprs = all_val_exprs.at(k);
              key_idxs.pop_back();
              key_offsets.pop_back();
              column_metas.pop_back();
              all_val_exprs.pop_back();
              if (OB_ISNULL(val_exprs)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("get null val exprs");
              } else {
                val_exprs->destroy();
                alloc.free(val_exprs);
              }
            }
          } else {
            need_add = false;
          }
        }
      }
      if (OB_SUCC(ret) && need_add && cur_val_exprs.count() > 0) {
        ++in_param_count;
        for (int64_t j = 0; OB_SUCC(ret) && j < cur_val_exprs.count(); ++j) {
          if (OB_ISNULL(all_val_exprs.at(j))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected expr array");
          } else if (OB_FAIL(all_val_exprs.at(j)->push_back(cur_val_exprs.at(j)))) {
            LOG_WARN("failed to push back raw expr");
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (key_idxs.empty()) {
        always_true_or_false = true;
      } else if (0 == in_param_count) {
        always_true_or_false = false;
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < key_idxs.count(); ++i) {
          InParam *in_param = nullptr;
          int64_t param_idx;
          TmpExprArray *val_exprs = all_val_exprs.at(i);
          ObRangeColumnMeta* column_meta = column_metas.at(i);
          if (OB_ISNULL(val_exprs) || OB_UNLIKELY(in_param_count != val_exprs->count())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get null val exprs", KPC(val_exprs), K(in_param_count));
          } else if (OB_FAIL(get_final_in_array_idx(in_param, param_idx))) {
            LOG_WARN("failed to get final in array idx");
          } else if (OB_FAIL(val_idxs.push_back(param_idx))) {
            LOG_WARN("failed to push back param idx");
          } else if (OB_FAIL(in_param->init(in_param_count))) {
            LOG_WARN("failed to init fix array");
          } else if (expr_depth == 0 && OB_FAIL(set_column_flags(key_idxs.at(i), T_OP_IN))) {
            LOG_WARN("failed to set column flags");
          } else {
            for (int64_t j = 0; OB_SUCC(ret) && j < in_param_count; ++j) {
              int64_t val_idx = 0;
              if (OB_FAIL(get_final_expr_idx(val_exprs->at(j), column_meta, val_idx))) {
                LOG_WARN("failed to get final expr idx", K(ret));
              } else if (OB_FAIL(in_param->push_back(val_idx))) {
                LOG_WARN("failed to push back val idx");
              }
            }
          }
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(alloc_range_node(range_node))) {
            LOG_WARN("failed to alloc common range node");
          } else if (OB_FAIL(fill_range_node_for_basic_row_cmp(T_OP_EQ, key_idxs, val_idxs, *range_node))) {
            LOG_WARN("failed to fill range node for basic cmp");
          } else {
            range_node->contain_in_ = true;
            range_node->in_param_count_ = in_param_count;
            if (key_idxs.count() < l_expr.get_param_count()) {
              // part of row can't extract range.
              ctx_.cur_is_precise_ = false;
            }
          }
        }
      }
    }

    // cleanup TmpExprArray anyway
    if (!all_val_exprs.empty()) {
      for (int64_t i = 0; i < all_val_exprs.count(); ++i) {
        TmpExprArray *val_exprs = all_val_exprs.at(i);
        if (OB_NOT_NULL(val_exprs)) {
          val_exprs->destroy();
          alloc.free(val_exprs);
        }
      }
    }
  }

  if (OB_SUCC(ret) && nullptr == range_node) {
    ctx_.cur_is_precise_ = false;
    if (OB_FAIL(generate_always_true_or_false_node(always_true_or_false, range_node))) {
      LOG_WARN("failed to generate always true or fasle node");
    }
  }
  return ret;
}

int ObExprRangeConverter::get_single_rowid_in_range_node(const ObRawExpr &rowid_expr,
                                                       const ObRawExpr &row_expr,
                                                       ObRangeNode *&range_node)
{
  int ret = OB_SUCCESS;
  ctx_.cur_is_precise_ = true;
  uint64_t part_column_id = OB_INVALID_ID;
  ObSEArray<const ObColumnRefRawExpr *, 4> pk_column_items;
  bool is_physical_rowid = false;
  bool always_true = false;
  ObSEArray<int64_t, 4> val_idxs;
  if (OB_FAIL(get_extract_rowid_range_infos(rowid_expr, pk_column_items,
                                            is_physical_rowid, part_column_id))) {
    LOG_WARN("failed to get extract rowid range infos");
  } else if (ctx_.is_global_index_ && is_physical_rowid) {
    // do nothing
  } else if (!is_physical_rowid) {
    ObSEArray<int64_t, 4> key_idxs;
    ObSEArray<int64_t, 4> pk_offsets;
    ObSEArray<int64_t, 4> tmp_key_idxs;
    ObSEArray<int64_t, 4> tmp_pk_offsets;
    TmpExprArray all_valid_exprs;
    int64_t min_offset = ctx_.column_cnt_;
    for (int64_t i = 0; OB_SUCC(ret) && i < pk_column_items.count(); ++i) {
      const ObColumnRefRawExpr *column_expr = pk_column_items.at(i);
      int64_t key_idx = 0;
      if (!is_range_key(column_expr->get_column_id(), key_idx)) {
        // do nothing
      } else if (OB_FAIL(tmp_key_idxs.push_back(key_idx))) {
        LOG_WARN("failed to push back key idx");
      } else if (OB_FAIL(tmp_pk_offsets.push_back(i))) {
        LOG_WARN("failed to push back key idx");
      } else if (key_idx < min_offset) {
        min_offset = key_idx;
      }
    }

    if (OB_SUCC(ret) && tmp_key_idxs.count() > 0) {
      for (int64_t i = min_offset; OB_SUCC(ret) && i < ctx_.column_cnt_; ++i) {
        int64_t idx = -1;
        if (ObOptimizerUtil::find_item(tmp_key_idxs, i, &idx)) {
          if (OB_FAIL(key_idxs.push_back(i))) {
            LOG_WARN("failed to push back key idx", K(key_idxs));
          } else if (OB_FAIL(pk_offsets.push_back(tmp_pk_offsets.at(idx)))) {
            LOG_WARN("failed to push back key idx", K(tmp_pk_offsets), K(idx));
          }
        } else {
          // only extract consistent column for row compare
          break;
        }
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && !always_true && i < row_expr.get_param_count(); ++i) {
      const ObRawExpr *const_expr = row_expr.get_param_expr(i);
      bool is_valid = false;
      if (OB_ISNULL(const_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null expr");
      } else if (OB_UNLIKELY(!const_expr->is_const_expr())) {
        always_true = true;
      } else if (OB_FAIL(check_calculable_expr_valid(const_expr, is_valid))) {
        LOG_WARN("failed to get calculable expr val");
      } else if (!is_valid) {
        always_true = true;
      } else if (OB_FAIL(all_valid_exprs.push_back(const_expr))) {
        LOG_WARN("failed to push back const expr");
      }
    }
    if (OB_SUCC(ret)) {
      if (key_idxs.empty()) {
        always_true = true;
      } else if (all_valid_exprs.empty()) {
        // rowid in (null, null, null)
        if (OB_FAIL(generate_always_true_or_false_node(false, range_node))) {
          LOG_WARN("failed to generate always fasle node");
        }
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < key_idxs.count(); ++i) {
          InParam *in_param = nullptr;
          int64_t param_idx;
          if (OB_FAIL(get_final_in_array_idx(in_param, param_idx))) {
            LOG_WARN("failed to get final in array idx");
          } else if (OB_FAIL(val_idxs.push_back(param_idx))) {
            LOG_WARN("failed to push back param idx");
          } else if (OB_FAIL(in_param->init(all_valid_exprs.count()))) {
            LOG_WARN("failed to init fix array");
          } else {
            for (int64_t j = 0; OB_SUCC(ret) && j < all_valid_exprs.count(); ++j) {
              int64_t val_idx = 0;
              if (OB_FAIL(get_final_expr_idx(all_valid_exprs.at(j), nullptr, val_idx))) {
                LOG_WARN("failed to get final expr idx", K(ret));
              } else if (OB_FAIL(in_param->push_back(val_idx))) {
                LOG_WARN("failed to push back val idx");
              } else if (OB_FAIL(ctx_.rowid_idxs_.push_back(std::pair<int64_t, int64_t>(val_idx, pk_offsets.at(i))))) {
                LOG_WARN("failed to push back rowid idxs", K(val_idx), K(pk_offsets.at(i)));
              }
            }
          }
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(alloc_range_node(range_node))) {
            LOG_WARN("failed to alloc common range node");
          } else if (OB_FAIL(fill_range_node_for_basic_row_cmp(T_OP_EQ, key_idxs, val_idxs, *range_node))) {
            LOG_WARN("failed to fill range node for basic cmp");
          } else {
            range_node->contain_in_ = true;
            range_node->is_rowid_node_ = true;
            range_node->in_param_count_ = all_valid_exprs.count();
          }
        }
      }
    }
  } else {
    // physical rowid
    int64_t key_idx = 0;
    int64_t const_idx = 0;
    if (common::OB_INVALID_ID == part_column_id) {
      if (OB_UNLIKELY(pk_column_items.count() != 1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpect pk count for no pk table", K(pk_column_items));
      } else if (ctx_.range_column_map_.count() != 1 ||
                !is_range_key(pk_column_items.at(0)->get_column_id(), key_idx)) {
        // only extract physical rowid range for primary index
        always_true = true;
      }
    } else if (!ctx_.phy_rowid_for_table_loc_ || !is_range_key(part_column_id, key_idx)) {
      always_true = true;
    }

    for (int64_t i = 0; OB_SUCC(ret) && !always_true && i < row_expr.get_param_count(); ++i) {
      const ObRawExpr *const_expr = row_expr.get_param_expr(i);
      bool is_valid = false;
      int64_t val_idx = -1;
      if (OB_ISNULL(const_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null expr");
      } else if (OB_UNLIKELY(!const_expr->is_const_expr())) {
        always_true = true;
      } else if (OB_FAIL(check_calculable_expr_valid(const_expr, is_valid))) {
        LOG_WARN("failed to get calculable expr val");
      } else if (!is_valid) {
        always_true = true;
      } else if (OB_FAIL(get_final_expr_idx(const_expr, nullptr, val_idx))) {
        LOG_WARN("failed to get final expr idx", K(ret));
      } else if (OB_FAIL(ctx_.rowid_idxs_.push_back(std::pair<int64_t, int64_t>(val_idx, PHYSICAL_ROWID_IDX)))) {
        LOG_WARN("failed to push back rowid idxs", K(const_idx), K(PHYSICAL_ROWID_IDX));
      } else if (OB_FAIL(val_idxs.push_back(val_idx))) {
        LOG_WARN("failed to push back val idx", K(val_idx));
      }
    }
    if (OB_SUCC(ret) && !always_true) {
      InParam *in_param = nullptr;
      int64_t param_idx;
      if (val_idxs.empty()) {
        // c1 in (null, null, null)
        if (OB_FAIL(generate_always_true_or_false_node(false, range_node))) {
          LOG_WARN("failed to generate always fasle node");
        }
      } else if (OB_FAIL(alloc_range_node(range_node))) {
        LOG_WARN("failed to alloc common range node");
      } else if (OB_FAIL(get_final_in_array_idx(in_param, param_idx))) {
        LOG_WARN("failed to get final in array idx");
      } else if (OB_FAIL(in_param->assign(val_idxs))) {
        LOG_WARN("failed to assign in params");
      } else if (OB_FAIL(fill_range_node_for_basic_cmp(T_OP_EQ, key_idx, param_idx, *range_node))) {
        LOG_WARN("failed to fill range node for basic cmp");
      } else {
        range_node->contain_in_ = true;
        range_node->is_phy_rowid_ = true;
        range_node->is_rowid_node_ = true;
        range_node->in_param_count_ = val_idxs.count();
      }
    }
  }

  if (OB_SUCC(ret) && range_node == nullptr) {
    ctx_.cur_is_precise_ = false;
    if (OB_FAIL(generate_always_true_or_false_node(true, range_node))) {
      LOG_WARN("failed to generate always true node");
    }
  }
  return ret;
}

int ObExprRangeConverter::convert_not_in_expr(const ObRawExpr *expr, int64_t expr_depth, ObRangeNode *&range_node)
{
  int ret = OB_SUCCESS;
  const ObRawExpr* l_expr = nullptr;
  const ObRawExpr* r_expr = nullptr;
  ctx_.cur_is_precise_ = false;
  bool use_implicit_cast_feature = ObSQLUtils::is_opt_feature_version_ge_425_or_435(ctx_.optimizer_features_enable_version_);
  if (OB_ISNULL(expr) ||
      OB_ISNULL(l_expr = expr->get_param_expr(0)) ||
      OB_ISNULL(r_expr = expr->get_param_expr(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null expr", K(expr), K(l_expr), K(r_expr));
  } else if (OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(l_expr, l_expr, use_implicit_cast_feature))) {
    LOG_WARN("failed to get expr without lossless cast", K(ret));
  } else if (l_expr->get_expr_type() == T_OP_ROW || r_expr->get_param_count() > NEW_MAX_NOT_IN_SIZE) {
    // do nothing
  } else if (l_expr->is_column_ref_expr()) {
    if (OB_FAIL(get_single_not_in_range_node(static_cast<const ObColumnRefRawExpr*>(l_expr),
                                             r_expr,
                                             expr->get_param_expr(0)->get_result_type(),
                                             expr_depth,
                                             range_node))) {
      LOG_WARN("failed to get single not in range node", K(ret));
    }
  } else if (r_expr->get_param_count() > MAX_NOT_IN_SIZE) {
    // do nothing
  } else if (l_expr->is_column_ref_expr()) {
    ObSEArray<ObRangeNode*, 10> and_range_nodes;
    ObSEArray<ObRangeNode*, 2> or_range_nodes;
    bool is_precise = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < r_expr->get_param_count(); ++i) {
      or_range_nodes.reuse();
      ObRangeNode *tmp_node = nullptr;
      ObRangeNode *final_node = nullptr;
      const ObRawExpr* const_expr = r_expr->get_param_expr(i);
      if (OB_ISNULL(const_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null");
      } else if (!const_expr->is_const_expr()) {
        // ignore current node
        is_precise = false;
      } else if (OB_FAIL(get_basic_range_node(l_expr, const_expr, T_OP_LT, expr_depth + 1, tmp_node))) {
        LOG_WARN("failed tp get basic range node", K(ret));
      } else if (OB_FAIL(or_range_nodes.push_back(tmp_node))) {
        LOG_WARN("failed to push back range node");
      } else if (OB_FALSE_IT(is_precise &= ctx_.cur_is_precise_)) {
      } else if (OB_FAIL(get_basic_range_node(l_expr, const_expr, T_OP_GT, expr_depth + 1, tmp_node))) {
        LOG_WARN("failed tp get basic range node", K(ret));
      } else if (OB_FAIL(or_range_nodes.push_back(tmp_node))) {
        LOG_WARN("failed to push back range node");
      } else if (OB_FALSE_IT(is_precise &= ctx_.cur_is_precise_)) {
      } else if (OB_FAIL(ObRangeGraphGenerator::or_range_nodes(*this, or_range_nodes, ctx_.column_cnt_, final_node))) {
        LOG_WARN("failed to or range nodes");
      } else if (OB_FAIL(and_range_nodes.push_back(final_node))) {
        LOG_WARN("failed to push back range node");
      }
    }
    if (OB_SUCC(ret) && !and_range_nodes.empty()) {
      if (OB_FAIL(ObRangeGraphGenerator::and_range_nodes(and_range_nodes, ctx_, range_node))) {
        LOG_WARN("failed to or range nodes");
      } else {
        ctx_.cur_is_precise_ = is_precise;
      }
    }
  }

  if (OB_SUCC(ret) && nullptr == range_node) {
    if (OB_FAIL(generate_always_true_or_false_node(true, range_node))) {
      LOG_WARN("failed to generate always true or fasle node");
    }
  }
  return ret;
}

int ObExprRangeConverter::get_single_not_in_range_node(const ObColumnRefRawExpr *column_expr,
                                                       const ObRawExpr *r_expr,
                                                       const ObRawExprResType &res_type,
                                                       int64_t expr_depth,
                                                       ObRangeNode *&range_node)
{
  int ret = OB_SUCCESS;
  ObRangeColumnMeta *column_meta = nullptr;
  int64_t key_idx = -1;
  ObSEArray<int64_t, 4> val_idxs;
  bool is_precise = true;
  range_node = nullptr;
  ctx_.cur_is_precise_ = true;
  bool always_false = false;
  if (OB_ISNULL(column_expr) || OB_ISNULL(r_expr) ||
      OB_UNLIKELY(r_expr->get_param_count() == 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected param", KPC(column_expr), KPC(r_expr));
  } else if (!is_range_key(column_expr->get_column_id(), key_idx)) {
    // do nothing
  } else if ((ctx_.column_flags_[key_idx] & (RANGE_EXPR_EQUAL |
                                             RANGE_EXPR_IN |
                                             RANGE_EXPR_NOT_IN)) != 0) {
    // do nothing
  } else if (OB_ISNULL(column_meta = get_column_meta(key_idx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null column meta");
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !always_false && i < r_expr->get_param_count(); ++i) {
      const ObRawExpr *const_expr = r_expr->get_param_expr(i);
      bool cur_always_true = true;
      bool is_valid = true;
      int64_t val_idx = -1;

      if (OB_ISNULL(const_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null expr");
      } else if (OB_UNLIKELY(!const_expr->is_const_expr())) {
        is_precise = false;
      } else if (!ObQueryRange::can_be_extract_range(T_OP_LT, column_meta->column_type_,
                                                     res_type,
                                                     const_expr->get_result_type().get_type(),
                                                     cur_always_true)) {
        always_false = !cur_always_true;
        is_precise = false;
      } else if (OB_FAIL(check_calculable_expr_valid(const_expr, is_valid))) {
        LOG_WARN("failed to get calculable expr val");
      } else if (!is_valid) {
        is_precise = false;
      } else if (OB_FAIL(get_final_expr_idx(const_expr, column_meta, val_idx))) {
        LOG_WARN("failed to get final expr idx", K(ret));
      } else if (OB_FAIL(check_expr_precise(*const_expr, const_expr->get_result_type(),
                                            column_meta->column_type_))) {
        LOG_WARN("failed to check expr precise", K(ret));
      } else if (OB_FALSE_IT(is_precise &= ctx_.cur_is_precise_)) {
      } else if (OB_FAIL(val_idxs.push_back(val_idx))) {
        LOG_WARN("failed to push back val idx", K(val_idx));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (always_false) {
      if (OB_FAIL(generate_always_true_or_false_node(always_false, range_node))) {
        LOG_WARN("failed to generate always true or fasle node");
      }
    } else if (!val_idxs.empty()) {
      InParam *in_param = nullptr;
      int64_t param_idx;
      if (OB_FAIL(alloc_range_node(range_node))) {
        LOG_WARN("failed to alloc common range node");
      } else if (OB_FAIL(get_final_in_array_idx(in_param, param_idx))) {
        LOG_WARN("failed to get final in array idx");
      } else if (OB_FAIL(in_param->assign(val_idxs))) {
        LOG_WARN("failed to assign in params");
      } else if (OB_FAIL(fill_range_node_for_basic_cmp(T_OP_EQ, key_idx, param_idx, *range_node))) {
        LOG_WARN("failed to fill range node for basic cmp");
      } else if (expr_depth == 0 && OB_FAIL(set_column_flags(key_idx, T_OP_NOT_IN))) {
        LOG_WARN("failed tp set column flags");
      } else {
        range_node->is_not_in_node_ = true;
        range_node->in_param_count_ = val_idxs.count();
        ctx_.cur_is_precise_ = is_precise;
      }
    }
  }
  return ret;
}

int ObExprRangeConverter::get_calculable_expr_val(const ObRawExpr *expr,
                                          ObObj &val,
                                          bool &is_valid,
                                          const bool ignore_error/*default true*/)
{
  int ret = OB_SUCCESS;
  if (expr->has_flag(CNT_DYNAMIC_PARAM) || expr->has_flag(CNT_FAKE_CONST_UDF)) {
    is_valid = true;
  } else if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(ctx_.exec_ctx_,
                                                               expr,
                                                               val,
                                                               is_valid,
                                                               allocator_,
                                                               ignore_error && ctx_.ignore_calc_failure_,
                                                               ctx_.expr_constraints_))) {
    LOG_WARN("failed to calc const or calculable expr", K(ret));
  }
  return ret;
}

int ObExprRangeConverter::check_calculable_expr_valid(const ObRawExpr *expr,
                                                      bool &is_valid,
                                                      const bool ignore_error/*default true*/)
{
  int ret = OB_SUCCESS;
  ObObj val;
  bool can_ignore_check = false;
  if (expr->has_flag(CNT_DYNAMIC_PARAM) || expr->has_flag(CNT_FAKE_CONST_UDF)) {
    is_valid = true;
  } else if (OB_FAIL(ignore_inner_generate_expr(expr, can_ignore_check))) {
    LOG_WARN("failedto check can ignore inner generate expr", K(ret));
  } else if (can_ignore_check) {
    is_valid = true;
  } else if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(ctx_.exec_ctx_,
                                                               expr,
                                                               val,
                                                               is_valid,
                                                               allocator_,
                                                               ignore_error && ctx_.ignore_calc_failure_,
                                                               ctx_.expr_constraints_))) {
    LOG_WARN("failed to calc const or calculable expr", K(ret));
  }
  return ret;
}

int ObExprRangeConverter::add_precise_constraint(const ObRawExpr *expr, bool is_precise)
{
  int ret = OB_SUCCESS;
  PreCalcExprExpectResult expect_result = is_precise ? PreCalcExprExpectResult::PRE_CALC_PRECISE :
                                                       PreCalcExprExpectResult::PRE_CALC_NOT_PRECISE;
  ObExprConstraint cons(const_cast<ObRawExpr*>(expr), expect_result);
  if (NULL == ctx_.expr_constraints_) {
    // do nothing
  } else if (OB_FAIL(add_var_to_array_no_dup(*ctx_.expr_constraints_, cons))) {
    LOG_WARN("failed to add precise constraint", K(ret));
  }
  return ret;
}

// TODO:@yibo.tyf.  This constraint seems like useless
int ObExprRangeConverter::add_prefix_pattern_constraint(const ObRawExpr *expr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObRawExprUtils::get_real_expr_without_cast(expr, expr))) {
    LOG_WARN("fail to get real expr", K(ret));
  } else if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (T_FUN_SYS_PREFIX_PATTERN == expr->get_expr_type()) {
    ObExprConstraint cons(const_cast<ObRawExpr*>(expr), PreCalcExprExpectResult::PRE_CALC_RESULT_NOT_NULL);
    if (NULL == ctx_.expr_constraints_) {
      // do nothing
    } else if (OB_FAIL(add_var_to_array_no_dup(*ctx_.expr_constraints_, cons))) {
      LOG_WARN("failed to add precise constraint", K(ret));
    }
  }
  return ret;
}

int ObExprRangeConverter::fill_range_node_for_basic_cmp(ObItemType cmp_type,
                                                        const int64_t key_idx,
                                                        const int64_t val_idx,
                                                        ObRangeNode &range_node) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(key_idx >= ctx_.column_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected column idx", K(key_idx), K(ctx_.column_cnt_));
  } else {
    range_node.min_offset_ = key_idx;
    range_node.max_offset_ = key_idx;
    bool need_set_flag = (key_idx == ctx_.column_cnt_ - 1);
    for (int64_t i = 0; i < key_idx; ++i) {
      range_node.start_keys_[i] = OB_RANGE_EMPTY_VALUE;
      range_node.end_keys_[i] = OB_RANGE_EMPTY_VALUE;
    }
    if (T_OP_EQ == cmp_type || T_OP_NSEQ == cmp_type) {
      range_node.start_keys_[key_idx] = val_idx;
      range_node.end_keys_[key_idx] = val_idx;
      if (need_set_flag) {
        range_node.include_start_ = true;
        range_node.include_end_ = true;
      } else {
        for (int64_t i = key_idx + 1; i < ctx_.column_cnt_; ++i) {
          range_node.start_keys_[i] = OB_RANGE_MIN_VALUE;
          range_node.end_keys_[i] = OB_RANGE_MAX_VALUE;
        }
      }
    } else if (T_OP_LE == cmp_type || T_OP_LT == cmp_type) {
      // (c1, c2, c3)
      // c1 <  10  => (min, min, min; 10, min, min)
      // c1 <= 10  => (min, min, min; 10, max, max)
      // c2 <  10  => (ept, min, min; ept, 10, min)
      // c2 <= 10  => (ept, min, min; ept, 10, max)
      // c3 <  10  => (ept, ept, min; ept, ept, 10)
      // c3 <= 10  => (ept, ept, min; ept, ept, 10]
      int64_t mysql_start_value = is_contain(ctx_.null_safe_value_idxs_, val_idx) ? OB_RANGE_MIN_VALUE : OB_RANGE_NULL_VALUE;
      range_node.start_keys_[key_idx] = lib::is_oracle_mode() ? OB_RANGE_MIN_VALUE : mysql_start_value;
      range_node.end_keys_[key_idx] = val_idx;
      if (need_set_flag) {
        range_node.include_start_ = false;
        range_node.include_end_ = (T_OP_LE == cmp_type ||
                                   (ctx_.final_exprs_flag_.at(val_idx)
                                     & OB_FINAL_EXPR_WITH_LOB_TRUNCATE)
                                     == OB_FINAL_EXPR_WITH_LOB_TRUNCATE);
      } else {
        for (int64_t i = key_idx + 1; i < ctx_.column_cnt_; ++i) {
          range_node.start_keys_[i] = lib::is_oracle_mode() ? OB_RANGE_MIN_VALUE : OB_RANGE_MAX_VALUE;
          range_node.end_keys_[i] = (T_OP_LE == cmp_type ||
                                     (ctx_.final_exprs_flag_.at(val_idx)
                                     & OB_FINAL_EXPR_WITH_LOB_TRUNCATE)
                                     == OB_FINAL_EXPR_WITH_LOB_TRUNCATE) ? OB_RANGE_MAX_VALUE : OB_RANGE_MIN_VALUE;
        }
      }
    } else if (T_OP_GE == cmp_type || T_OP_GT == cmp_type) {
      // (c1, c2, c3)
      // c1 >  10  => (10, max, max; max, max, max)
      // c1 >= 10  => (10, min, min; max, max, max)
      // c2 >  10  => (ept, 10, max; ept, max, max)
      // c2 >= 10  => (ept, 10, min; ept, max, max)
      // c3 >  10  => (ept, ept, 10; ept, ept, max)
      // c3 >= 10  => [ept, ept, 10; ept, ept, max)
      range_node.start_keys_[key_idx] = val_idx;
      int64_t oracle_end_value = is_contain(ctx_.null_safe_value_idxs_, val_idx) ? OB_RANGE_MAX_VALUE : OB_RANGE_NULL_VALUE;
      range_node.end_keys_[key_idx] = lib::is_oracle_mode() ? oracle_end_value : OB_RANGE_MAX_VALUE;
      if (need_set_flag) {
        range_node.include_start_ = (T_OP_GE == cmp_type);
        range_node.include_end_ = false;
      } else {
        for (int64_t i = key_idx + 1; i < ctx_.column_cnt_; ++i) {
          range_node.start_keys_[i] = (T_OP_GE == cmp_type) ? OB_RANGE_MIN_VALUE : OB_RANGE_MAX_VALUE;
          range_node.end_keys_[i] = lib::is_oracle_mode() ? OB_RANGE_MIN_VALUE : OB_RANGE_MAX_VALUE;
        }
      }
    }
  }
  return ret;
}

/**
 * T_OP_EQ/T_OP_NSEQ doesn't require key_idx strict increasing
 * T_OP_LE/T_OP_LT/T_OP_GE/T_OP_GT require key_idx strict increasing
*/
int ObExprRangeConverter::fill_range_node_for_basic_row_cmp(ObItemType cmp_type,
                                                            const ObIArray<int64_t> &key_idxs,
                                                            const ObIArray<int64_t> &val_idxs,
                                                            ObRangeNode &range_node) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(key_idxs.count() != val_idxs.count()) ||
      OB_UNLIKELY(key_idxs.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected count", K(key_idxs.count()), K(val_idxs.count()));
  } else if (T_OP_EQ == cmp_type || T_OP_NSEQ == cmp_type) {
    range_node.min_offset_ = key_idxs.at(0);
    range_node.max_offset_ = key_idxs.at(0);
    for (int64_t i = 0; i < ctx_.column_cnt_; ++i) {
      range_node.start_keys_[i] = OB_RANGE_MIN_VALUE;
      range_node.end_keys_[i] = OB_RANGE_MAX_VALUE;
    }
    for (int64_t i = 0; i < key_idxs.count(); ++i) {
      int64_t key_idx = key_idxs.at(i);
      int64_t val_idx = val_idxs.at(i);
      OB_ASSERT(key_idx < ctx_.column_cnt_);
      range_node.start_keys_[key_idx] = val_idx;
      range_node.end_keys_[key_idx] = val_idx;
      if (key_idx < range_node.min_offset_) {
        range_node.min_offset_ = key_idx;
      } else if (key_idx > range_node.max_offset_) {
        range_node.max_offset_ = key_idx;
      }
    }
    for (int64_t i = 0; i < ctx_.column_cnt_; ++i) {
      if (OB_RANGE_MIN_VALUE == range_node.start_keys_[i]) {
        range_node.start_keys_[i] = OB_RANGE_EMPTY_VALUE;
        range_node.end_keys_[i] = OB_RANGE_EMPTY_VALUE;
      } else {
        break;
      }
    }
    if (range_node.start_keys_[ctx_.column_cnt_ - 1] != OB_RANGE_MIN_VALUE) {
      range_node.include_start_ = true;
      range_node.include_end_ = true;
    }
  } else {
    range_node.min_offset_ = key_idxs.at(0);
    range_node.max_offset_ = key_idxs.at(key_idxs.count() - 1);
    for (int64_t i = 0; i < key_idxs.count(); ++i) {
      int64_t key_idx = key_idxs.at(i);
      int64_t val_idx = val_idxs.at(i);
      if (OB_UNLIKELY(key_idx >= ctx_.column_cnt_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected column idx", K(key_idx), K(ctx_.column_cnt_));
      } else {
        bool need_set_flag = (key_idx == ctx_.column_cnt_ - 1);
        bool need_include_end = false;
        if (0 == i) {
          for (int64_t j = 0; j < key_idx; ++j) {
            range_node.start_keys_[j] = OB_RANGE_EMPTY_VALUE;
            range_node.end_keys_[j] = OB_RANGE_EMPTY_VALUE;
          }
        }
        if (T_OP_LE == cmp_type || T_OP_LT == cmp_type) {
          // (c1, c2, c3, c4)
          // (c1,c3) <  (10,10)  => (min, min, min, min; 10, max, 10, min)
          // (c1,c3) <= (10,10)  => (min, min, min, min; 10, max, 10, max)
          // (c2,c3) <  (10,10)  => (ept, min, min, min; ept, 10, 10, min)
          // (c2,c3) <= (10,10)  => (ept, min, min, min; ept, 10, 10, max)
          // (c3,c4) <  (10,10)  => (ept, ept, min, min; ept, ept, 10, 10)
          // (c3,c4) <= (10,10)  => (ept, ept, min, min; ept, ept, 10, 10]
          /**
           * (c1,c2) <= (10, 10)
           * oracle 精确
           * (min, min, min; 10, 10, max)
           * mysql 非精确
           * (null, max, max, 10, 10, max)
           *
           * (c1,c2) < (10, 10)
           * oracle 精确
           * (min, min, min; 10, 10, min)
           * mysql 非精确
           * (null, max, max, 10, 10, min)
          */
          if (0 == i) {
            if (lib::is_oracle_mode()) {
              range_node.start_keys_[key_idx] = OB_RANGE_MIN_VALUE;
            } else {
              int64_t start_value = is_contain(ctx_.null_safe_value_idxs_, val_idx) ? OB_RANGE_MIN_VALUE : OB_RANGE_NULL_VALUE;
              range_node.start_keys_[key_idx] = start_value;
            }
          } else {
            if (lib::is_oracle_mode()) {
              range_node.start_keys_[key_idx] = OB_RANGE_MIN_VALUE;
            } else {
              range_node.start_keys_[key_idx] = OB_RANGE_MAX_VALUE;
            }
          }
          range_node.end_keys_[key_idx] = val_idx;
          if (key_idxs.count() - 1 == i) {
            need_include_end = (ctx_.final_exprs_flag_.at(val_idx)
                                 & OB_FINAL_EXPR_WITH_LOB_TRUNCATE)
                                  == OB_FINAL_EXPR_WITH_LOB_TRUNCATE;
            for (int64_t j = key_idx + 1; j < ctx_.column_cnt_; ++j) {
              range_node.start_keys_[j] = lib::is_oracle_mode() ? OB_RANGE_MIN_VALUE : OB_RANGE_MAX_VALUE;
              range_node.end_keys_[j] = (T_OP_LE == cmp_type || need_include_end) ? OB_RANGE_MAX_VALUE : OB_RANGE_MIN_VALUE;
            }
          }
          if (need_set_flag) {
            range_node.include_start_ = false;
            range_node.include_end_ = (T_OP_LE == cmp_type || need_include_end);
          }
        } else if (T_OP_GE == cmp_type || T_OP_GT == cmp_type) {
          // (c1, c2, c3)
          // (c1,c3) >  (10,10)  => (10, min, 10, max; max, max, max, max)
          // (c1,c3) >= (10,10)  => (10, min, 10, min; max, max, max, max)
          // (c2,c3) >  (10,10)  => (ept, 10, 10, max; ept, max, max, max)
          // (c2,c3) >= (10,10)  => (ept, 10, 10, min; ept, max, max, max)
          // (c3,c3) >  (10,10)  => (ept, ept, 10, 10; ept, ept, max, max)
          // (c3,c3) >= (10,10)  => [ept, ept, 10, 10; ept, ept, max, max)
          /**
           * (c1,c2) >= (10, 10)
           * oracle 非精确
           * (10, 10, min; null, min, min)
           * mysql 精确
           * (10, 10, min; max, max, max)
           *
           * (c1,c2) > (10, 10)
           * oracle 非精确
           * (10, 10, max; null, min, min)
           * mysql 精确
           * (10, 10, max; max, max, max)
          */
          range_node.start_keys_[key_idx] = val_idx;
          if (0 == i) {
            if (lib::is_oracle_mode()) {
              int64_t end_value = is_contain(ctx_.null_safe_value_idxs_, val_idx) ? OB_RANGE_MAX_VALUE : OB_RANGE_NULL_VALUE;
              range_node.end_keys_[key_idx] = end_value;
            } else {
              range_node.end_keys_[key_idx] = OB_RANGE_MAX_VALUE;
            }
          } else {
            if (lib::is_oracle_mode()) {
              range_node.end_keys_[key_idx] = OB_RANGE_MIN_VALUE;
            } else {
              range_node.end_keys_[key_idx] = OB_RANGE_MAX_VALUE;
            }
          }
          if (key_idxs.count() - 1 == i) {
            for (int64_t j = key_idx + 1; j < ctx_.column_cnt_; ++j) {
              range_node.start_keys_[j] = (T_OP_GE == cmp_type) ? OB_RANGE_MIN_VALUE : OB_RANGE_MAX_VALUE;
              range_node.end_keys_[j] = lib::is_oracle_mode() ? OB_RANGE_MIN_VALUE : OB_RANGE_MAX_VALUE;
            }
          }
          if (need_set_flag) {
            range_node.include_start_ = (T_OP_GE == cmp_type);
            range_node.include_end_ = false;
          }
        }
      }
    }
  }
  return ret;
}

int ObExprRangeConverter::fill_range_node_for_like(const int64_t key_idx,
                                                   const int64_t start_val_idx,
                                                   const int64_t end_val_idx,
                                                   ObRangeNode &range_node) const
{
  int ret = OB_SUCCESS;
  OB_ASSERT(key_idx < ctx_.column_cnt_);
  range_node.min_offset_ = key_idx;
  range_node.max_offset_ = key_idx;
  range_node.is_like_node_ = true;
  bool need_set_flag = (key_idx == ctx_.column_cnt_ - 1);
  for (int64_t i = 0; i < key_idx; ++i) {
    range_node.start_keys_[i] = OB_RANGE_EMPTY_VALUE;
    range_node.end_keys_[i] = OB_RANGE_EMPTY_VALUE;
  }
  range_node.start_keys_[key_idx] = start_val_idx;
  range_node.end_keys_[key_idx] = end_val_idx;
  if (need_set_flag) {
    range_node.include_start_ = true;
    range_node.include_end_ = true;
  } else {
    for (int64_t i = key_idx + 1; i < ctx_.column_cnt_; ++i) {
      range_node.start_keys_[i] = OB_RANGE_MIN_VALUE;
      range_node.end_keys_[i] = OB_RANGE_MAX_VALUE;
    }
  }
  return ret;
}


int ObExprRangeConverter::fill_range_node_for_is_not_null(const int64_t key_idx,
                                                          ObRangeNode &range_node) const
{
  // range of IS NOT NULL predicate:
  // mysql mode: (NULL; MAX), (NULL, MAX; MAX, MAX) ...
  // oralce mode: (MIN; NULL), (MIN, MIN; NULL, MIN) ...
  int ret = OB_SUCCESS;
  OB_ASSERT(key_idx < ctx_.column_cnt_);
  range_node.min_offset_ = key_idx;
  range_node.max_offset_ = key_idx;
  for (int64_t i = 0; i < key_idx; ++i) {
    range_node.start_keys_[i] = OB_RANGE_EMPTY_VALUE;
    range_node.end_keys_[i] = OB_RANGE_EMPTY_VALUE;
  }
  if (is_oracle_mode()) {
    range_node.start_keys_[key_idx] = OB_RANGE_MIN_VALUE;
    range_node.end_keys_[key_idx] = OB_RANGE_NULL_VALUE;
    for (int64_t i = key_idx + 1; i < ctx_.column_cnt_; ++i) {
      range_node.start_keys_[i] = OB_RANGE_MIN_VALUE;
      range_node.end_keys_[i] = OB_RANGE_MIN_VALUE;
    }
  } else if (is_mysql_mode()) {
    range_node.start_keys_[key_idx] = OB_RANGE_NULL_VALUE;
    range_node.end_keys_[key_idx] = OB_RANGE_MAX_VALUE;
    for (int64_t i = key_idx + 1; i < ctx_.column_cnt_; ++i) {
      range_node.start_keys_[i] = OB_RANGE_MAX_VALUE;
      range_node.end_keys_[i] = OB_RANGE_MAX_VALUE;
    }
  }
  return ret;
}

int ObExprRangeConverter::check_expr_precise(const ObRawExpr &const_expr,
                                             const ObObjMeta &calc_type,
                                             const ObRawExprResType &column_res_type)
{
  int ret = OB_SUCCESS;
  if (column_res_type.is_string_type() && calc_type.is_string_type()) {
    if (CS_TYPE_UTF8MB4_GENERAL_CI == column_res_type.get_collation_type()
        && CS_TYPE_UTF8MB4_GENERAL_CI != calc_type.get_collation_type()) {
      // we will set collation type of value to column's collation type,
      // however, if general bin transform to general ci,
      // the result may be N:1, the range may be amplified and turn to inprecise
      ctx_.cur_is_precise_ = false;
    }
  }
  if (is_oracle_mode()) {
    // c1 char(5), c2 varchar(5) 对于值'abc', c1 = 'abc  ', c2 = 'abc'
    // in oracle mode, 'abc  ' is not equals to 'abc', but the range of c1 = cast('abc' as varchar2(5))
    // is extracted as (abc ; abc), this is because that storage layer does not padding emptycharacter,
    // as a result, by using the range above, value 'abc  ' is selected, which is incorrect.
    // to avoid this, set the range to be not precise
    const ObObjType &column_type = column_res_type.get_type();
    const ObObjType &const_type = const_expr.get_result_type().get_type();
    if ((ObCharType == column_type && ObVarcharType == const_type) ||
        (ObNCharType == column_type && ObNVarchar2Type == const_type)) {
      ctx_.cur_is_precise_ = false;
    }
  }
  return ret;
}

int ObExprRangeConverter::get_rowid_node(const ObRawExpr &l_expr,
                                         const ObRawExpr &r_expr,
                                         ObItemType cmp_type,
                                         ObRangeNode *&range_node)
{
  int ret = OB_SUCCESS;
  range_node = nullptr;
  ctx_.cur_is_precise_ = true;
  const ObRawExpr *const_expr = NULL;
  const ObRawExpr *rowid_expr = NULL;
  uint64_t part_column_id = OB_INVALID_ID;
  ObSEArray<const ObColumnRefRawExpr *, 4> pk_column_items;
  bool is_physical_rowid = false;
  bool is_valid = false;

  if (OB_LIKELY(r_expr.is_const_expr())) {
    rowid_expr = &l_expr;
    const_expr = &r_expr;
  } else {
    rowid_expr = &r_expr;
    const_expr = &l_expr;
    cmp_type = get_opposite_compare_type(cmp_type);
  }

  if (OB_FAIL(get_extract_rowid_range_infos(*rowid_expr, pk_column_items,
                                            is_physical_rowid, part_column_id))) {
    LOG_WARN("failed to get extract rowid range infos");
  } else if (ctx_.is_global_index_ && is_physical_rowid) {
    // do nothing
  } else if (OB_FAIL(check_calculable_expr_valid(const_expr, is_valid))) {
    LOG_WARN("failed to get calculable expr val");
  } else if (!is_valid) {
    // do nothing
  } else if (!is_physical_rowid) {
    ObSEArray<int64_t, 4> key_idxs;
    ObSEArray<int64_t, 4> val_idxs;
    if (T_OP_EQ == cmp_type || T_OP_NSEQ == cmp_type) {
      ObSEArray<int64_t, 4> tmp_key_idxs;
      ObSEArray<int64_t, 4> pk_idxs;
      int64_t min_offset = ctx_.column_cnt_;
      for (int64_t i = 0; OB_SUCC(ret) && i < pk_column_items.count(); ++i) {
        const ObColumnRefRawExpr *column_expr = pk_column_items.at(i);
        int64_t key_idx = 0;
        if (is_range_key(column_expr->get_column_id(), key_idx)) {;
          if (OB_FAIL(tmp_key_idxs.push_back(key_idx))) {
            LOG_WARN("failed to push back key idx");
          } else if (OB_FAIL(pk_idxs.push_back(i))) {
            LOG_WARN("failed to push back pk idx");
          } else if (key_idx < min_offset) {
            min_offset = key_idx;
          }
        }
      }
      for (int64_t i = min_offset; OB_SUCC(ret) && i < ctx_.column_cnt_; ++i) {
        int64_t idx = -1;
        int64_t const_idx = 0;
        if (ObOptimizerUtil::find_item(tmp_key_idxs, i, &idx)) {
          if (OB_FAIL(key_idxs.push_back(i))) {
            LOG_WARN("failed to push back key idx", K(key_idxs));
          } else if (OB_FAIL(get_final_expr_idx(const_expr, nullptr, const_idx))) {
            LOG_WARN("failed to get final expr idx");
          } else if (OB_FAIL(val_idxs.push_back(const_idx))) {
            LOG_WARN("failed to push back val idx");
          } else if (OB_FAIL(ctx_.rowid_idxs_.push_back(std::pair<int64_t, int64_t>(const_idx, pk_idxs.at(idx))))) {
            LOG_WARN("failed to push back rowid idxs", K(const_idx), K(idx));
          }
        } else {
          // only extract consistent column for row compare
          break;
        }
      }
    } else {
      int64_t last_key_idx = -1;
      bool check_next = true;
      for (int64_t i = 0; OB_SUCC(ret) && check_next && i < pk_column_items.count(); ++i) {
        const ObColumnRefRawExpr *column_expr = pk_column_items.at(i);
        int64_t key_idx = 0;
        int64_t const_idx = 0;
        check_next = false;
        if (!is_range_key(column_expr->get_column_id(), key_idx)) {
          // do nothing
        } else if (last_key_idx != -1 && last_key_idx + 1 != key_idx) {
          // do nothing
        } else if (OB_FAIL(get_final_expr_idx(const_expr, nullptr, const_idx))) {
          LOG_WARN("failed to get final expr idx");
        } else if (OB_FAIL(key_idxs.push_back(key_idx))) {
          LOG_WARN("failed to push back key idx");
        } else if (OB_FAIL(val_idxs.push_back(const_idx))) {
          LOG_WARN("failed to push back val idx");
        } else if (OB_FAIL(ctx_.rowid_idxs_.push_back(std::pair<int64_t, int64_t>(const_idx, i)))) {
          LOG_WARN("failed to push back rowid idxs", K(const_idx), K(i));
        } else {
          bool is_lt_with_lob = (cmp_type == T_OP_LT || cmp_type == T_OP_LE) &&
                                (ctx_.final_exprs_flag_.at(const_idx)
                                     & OB_FINAL_EXPR_WITH_LOB_TRUNCATE)
                                     == OB_FINAL_EXPR_WITH_LOB_TRUNCATE;
          check_next = !is_lt_with_lob;
          last_key_idx = key_idx;
        }
      }
    }
    if (OB_SUCC(ret) && key_idxs.count() > 0) {
      if (OB_FAIL(alloc_range_node(range_node))) {
        LOG_WARN("failed to alloc common range node");
      } else if (OB_FAIL(fill_range_node_for_basic_row_cmp(cmp_type, key_idxs, val_idxs, *range_node))) {
        LOG_WARN("failed to get normal row cmp keypart");
      } else {
        range_node->is_rowid_node_ = true;
      }
    }
  } else {
    // physical rowid
    int64_t key_idx = 0;
    int64_t const_idx = 0;
    bool always_true = false;
    if (common::OB_INVALID_ID == part_column_id) {
      if (OB_UNLIKELY(pk_column_items.count() != 1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpect pk count for no pk table", K(pk_column_items));
      } else if (ctx_.range_column_map_.count() != 1 ||
                !is_range_key(pk_column_items.at(0)->get_column_id(), key_idx)) {
        // only extract physical rowid range for primary index
        always_true = true;
      }
    } else if (!ctx_.phy_rowid_for_table_loc_ || !is_range_key(part_column_id, key_idx)) {
      always_true = true;
    }

    if (OB_FAIL(ret) || always_true) {
      // do nothing
    } else if (OB_FAIL(get_final_expr_idx(const_expr, nullptr, const_idx))) {
      LOG_WARN("failed to get final expr idx");
    } else if (OB_FAIL(ctx_.rowid_idxs_.push_back(std::pair<int64_t, int64_t>(const_idx, PHYSICAL_ROWID_IDX)))) {
      LOG_WARN("failed to push back rowid idxs", K(const_idx), K(PHYSICAL_ROWID_IDX));
    } else if (OB_FAIL(alloc_range_node(range_node))) {
      LOG_WARN("failed to alloc common range node");
    } else if (OB_FAIL(fill_range_node_for_basic_cmp(cmp_type, key_idx, const_idx, *range_node))) {
      LOG_WARN("failed to get normal row cmp keypart");
    } else {
      range_node->is_phy_rowid_ = true;
      range_node->is_rowid_node_ = true;
    }
  }

  if (OB_SUCC(ret) && range_node == nullptr) {
    ctx_.cur_is_precise_ = false;
    if (OB_FAIL(generate_always_true_or_false_node(true, range_node))) {
      LOG_WARN("failed to generate always true node");
    }
  }
  return ret;
}

int ObExprRangeConverter::get_extract_rowid_range_infos(const ObRawExpr &calc_urowid_expr,
                                                        ObIArray<const ObColumnRefRawExpr*> &pk_columns,
                                                        bool &is_physical_rowid,
                                                        uint64_t &part_column_id)
{
  int ret = OB_SUCCESS;
  is_physical_rowid = false;
  part_column_id = common::OB_INVALID_ID;
  for (int64_t i = 0; OB_SUCC(ret) && i < calc_urowid_expr.get_param_count(); ++i) {
    const ObRawExpr *param_expr = calc_urowid_expr.get_param_expr(i);
    if (OB_ISNULL(param_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null expr");
    } else if (param_expr->has_flag(IS_COLUMN)) {
      const ObColumnRefRawExpr * col_expr = static_cast<const ObColumnRefRawExpr *>(param_expr);
      // pk_vals may store generated col which is partition key but not primary key
      if (!col_expr->is_rowkey_column()) {
        /*do nothing*/
      } else if (OB_FAIL(pk_columns.push_back(col_expr))) {
        LOG_WARN("push back pk_column item failed");
      }
    } else if (param_expr->get_expr_type() == T_FUN_SYS_CALC_TABLET_ID) {
      is_physical_rowid = true;
      ObSEArray<ObRawExpr*, 4> column_exprs;
      if (!ctx_.phy_rowid_for_table_loc_) {
        // do nothing
      } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(param_expr, column_exprs))) {
        LOG_WARN("get column exprs error", KPC(param_expr));
      } else {
        bool find = false;
        for (int64_t j = 0; OB_SUCC(ret) && !find && j < column_exprs.count(); ++j) {
          ObColumnRefRawExpr *col_expr = NULL;
          if (OB_ISNULL(col_expr = static_cast<ObColumnRefRawExpr*>(column_exprs.at(j)))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret), K(col_expr));
          } else {
            int64_t key_idx = 0;
            if (is_range_key(col_expr->get_column_id(), key_idx)) {
              find = true;
              part_column_id = col_expr->get_column_id();
            }
          }
        }
      }
    }
  }
  LOG_TRACE("get extract rowid range infos", K(is_physical_rowid), K(part_column_id),
                                             K(pk_columns), K(calc_urowid_expr));
  return ret;
}

// final info start
int ObExprRangeConverter::get_final_expr_idx(const ObRawExpr *expr,
                                             const ObRangeColumnMeta *column_meta,
                                             int64_t &idx)
{
  int ret = OB_SUCCESS;
  const ObRawExpr *wrap_const_expr = expr;
  bool can_be_range_get = true;
  idx = ctx_.final_exprs_.count();
  if (nullptr != column_meta && nullptr != expr &&
      OB_FAIL(check_can_use_range_get(*expr, *column_meta))) {
    LOG_WARN("failed to check can be use range get", K(ret));
  } else if (nullptr != column_meta &&
      OB_FAIL(try_wrap_lob_with_substr(expr, column_meta, wrap_const_expr))) {
    LOG_WARN("failed to wrap lob with substr", K(ret));
  } else if (OB_FAIL(ctx_.final_exprs_.push_back(wrap_const_expr))) {
    LOG_WARN("failed to push back final expr");
  } else if (OB_FAIL(ctx_.final_exprs_flag_.push_back(expr == wrap_const_expr ? 0
                                                      : OB_FINAL_EXPR_WITH_LOB_TRUNCATE))) {
    LOG_WARN("failed to push back final expr flag");
  }
  return ret;
}

// in array index start from -1 to INT64_MIN
int ObExprRangeConverter::get_final_in_array_idx(InParam *&in_param, int64_t &idx)
{
  int ret = OB_SUCCESS;
  idx = -(ctx_.in_params_.count() + 1);
  void *ptr = nullptr;
  if (OB_UNLIKELY(allocator_.used() - mem_used_ > ctx_.max_mem_size_)) {
    ret = OB_ERR_QUERY_RANGE_MEMORY_EXHAUSTED;
    LOG_INFO("use too much memory when extract query range", K(ctx_.max_mem_size_));
  } else if (OB_ISNULL(ptr = allocator_.alloc(sizeof(InParam)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory for InParam failed");
  } else {
    in_param = new(ptr) InParam();
    in_param->set_allocator(&allocator_);
    if (OB_FAIL(ctx_.in_params_.push_back(in_param))) {
      LOG_WARN("failed to push back final expr");
    }
  }
  return ret;
}

bool ObExprRangeConverter::is_range_key(const uint64_t column_id, int64_t &key_idx)
{
  bool is_key = false;
  int ret = ctx_.range_column_map_.get_refactored(column_id, key_idx);
  if (OB_SUCCESS == ret) {
    is_key = true;
    if (ctx_.index_prefix_ > -1 && key_idx >= ctx_.index_prefix_) {
      is_key = false;
    }
  } else if (OB_HASH_NOT_EXIST == ret) {
    is_key = false;
  } else {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "failed to get key_idx from range column map", K(column_id));
  }
  return is_key;
}

ObRangeColumnMeta* ObExprRangeConverter::get_column_meta(int64_t idx)
{
  ObRangeColumnMeta* column_meta = nullptr;
  if (idx >=0 && idx < ctx_.column_metas_.count()) {
    column_meta = ctx_.column_metas_.at(idx);
  }
  return column_meta;
}

int ObExprRangeConverter::get_nvl_cmp_node(const ObRawExpr &l_expr,
                                           const ObRawExpr &r_expr,
                                           ObItemType cmp_type,
                                           const ObRawExprResType &result_type,
                                           int64_t expr_depth,
                                           ObRangeNode *&range_node)
{
  int ret = OB_SUCCESS;
  range_node = NULL;
  ctx_.cur_is_precise_ = true;
  bool always_true = true;
  bool is_precise = true;
  bool second_is_precise = true;
  const ObOpRawExpr *nvl_expr = NULL;
  const ObRawExpr *const_expr = NULL;
  const ObRawExpr *nvl_first_expr = NULL;
  const ObRawExpr *nvl_second_expr = NULL;
  ObRawExpr *cmp_second_expr = NULL;
  ObRangeNode *cmp_range_node = NULL;
  ObRangeNode *cmp_second_node = NULL;
  ObRangeNode *is_null_node = NULL;
  ObRangeNode *null_and_node = NULL;
  ObSEArray<ObRangeNode*, 2> range_nodes;
  bool use_implicit_cast_feature = true;
  if (OB_LIKELY(l_expr.get_expr_type() == T_FUN_SYS_NVL &&
                r_expr.is_const_expr())) {
    nvl_expr = static_cast<const ObOpRawExpr*>(&l_expr);
    const_expr = &r_expr;
  } else if (OB_LIKELY(r_expr.get_expr_type() == T_FUN_SYS_NVL &&
                       l_expr.is_const_expr())) {
    nvl_expr = static_cast<const ObOpRawExpr*>(&r_expr);
    const_expr = &l_expr;
    cmp_type = get_opposite_compare_type(cmp_type);
  }

  if (OB_ISNULL(nvl_expr) || OB_ISNULL(const_expr)) {
    // do nothing
  } else if (OB_UNLIKELY(nvl_expr->get_param_count() != 2) ||
             OB_ISNULL(nvl_first_expr = nvl_expr->get_param_expr(0)) ||
             OB_ISNULL(nvl_second_expr = nvl_expr->get_param_expr(1)) ||
             OB_ISNULL(ctx_.expr_factory_) ||
             OB_ISNULL(ctx_.session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected nvl expr", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(nvl_first_expr,
                                                                     nvl_first_expr,
                                                                     use_implicit_cast_feature))) {
    LOG_WARN("failed to get expr without lossless cast", K(ret));
  } else if (!nvl_first_expr->is_column_ref_expr()) {
    // do nothing
  } else if (OB_FAIL(gen_column_cmp_node(*nvl_first_expr,
                                         *const_expr,
                                         cmp_type,
                                         result_type,
                                         expr_depth + 1,
                                         T_OP_NSEQ == cmp_type,
                                         cmp_range_node))) {
    LOG_WARN("failed to gen column cmp node", K(ret));
  } else if (OB_FALSE_IT(is_precise &= ctx_.cur_is_precise_)) {
  } else if (OB_FAIL(ObRawExprUtils::create_double_op_expr(*ctx_.expr_factory_,
                                                           ctx_.session_info_,
                                                           cmp_type,
                                                           cmp_second_expr,
                                                           nvl_second_expr,
                                                           const_expr))) {
    LOG_WARN("failed to create double op expr", K(ret));
  } else if (OB_ISNULL(cmp_second_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null expr", K(cmp_second_expr));
  } else if (OB_FAIL(convert_expr_to_range_node(cmp_second_expr,
                                                cmp_second_node,
                                                0,
                                                second_is_precise))) {
    LOG_WARN("failed to convert expr to range node", K(ret));
  } else if (OB_FALSE_IT(is_precise &= second_is_precise)) {
  } else if (OB_FAIL(gen_is_null_range_node(nvl_first_expr,
                                            expr_depth + 1,
                                            is_null_node))) {
    LOG_WARN("failed to gen is null expr", K(ret));
  } else if (OB_FALSE_IT(is_precise &= ctx_.cur_is_precise_)) {
  } else if (OB_FAIL(range_nodes.push_back(cmp_second_node))) {
    LOG_WARN("failed to push back or range nodes", K(ret));
  } else if (OB_FAIL(range_nodes.push_back(is_null_node))) {
    LOG_WARN("failed to push back or range nodes", K(ret));
  } else if (OB_FAIL(ObRangeGraphGenerator::and_range_nodes(range_nodes,
                                                            ctx_,
                                                            null_and_node))) {
    LOG_WARN("failed to and range nodes");
  } else if (OB_FALSE_IT(range_nodes.reuse())) {
  } else if (OB_FAIL(range_nodes.push_back(cmp_range_node))) {
    LOG_WARN("failed to push back range nodes", K(ret));
  } else if (OB_FAIL(range_nodes.push_back(null_and_node))) {
    LOG_WARN("failed to push back range nodes", K(ret));
  } else if (OB_FAIL(ObRangeGraphGenerator::or_range_nodes(*this,
                                                           range_nodes,
                                                           ctx_.column_cnt_,
                                                           range_node))) {
    LOG_WARN("failed to or range nodes");
  } else {
    ctx_.cur_is_precise_ = is_precise;
    ctx_.refresh_max_offset_ = true;
  }

  if (OB_SUCC(ret) && nullptr == range_node) {
    ctx_.cur_is_precise_ = false;
    if (OB_FAIL(generate_always_true_or_false_node(always_true, range_node))) {
      LOG_WARN("failed to generate always true or fasle node", K(always_true));
    }
  }
  return ret;
}

int64_t ObExprRangeConverter::get_expr_category(ObItemType type)
{
  int64_t catagory = RANGE_EXPR_OTHER;
  if (T_OP_EQ == type ||
      T_OP_NSEQ == type ||
      T_OP_IS == type) {
    catagory = RANGE_EXPR_EQUAL;
  } else if (IS_BASIC_CMP_OP(type)) {
    catagory = RANGE_EXPR_CMP;
  } else if (T_OP_IS_NOT == type) {
    // range of IS NOT NULL predicate:
    // mysql mode: (NULL; MAX), (NULL, MAX; MAX, MAX) ...
    // oralce mode: (MIN; NULL), (MIN, MIN; NULL, MIN) ...
    // so, it can be regarded as a range expr cmp
    catagory = RANGE_EXPR_CMP;
  } else if (T_OP_IN  == type) {
    catagory = RANGE_EXPR_IN;
  } else if (T_OP_NE == type ||
              T_OP_NOT_BTW == type) {
    catagory = RANGE_EXPR_NO_EQUAL;
  } else if (T_OP_NOT_IN == type) {
    catagory = RANGE_EXPR_NOT_IN;
  } else {
    catagory = RANGE_EXPR_OTHER;
  }
  return catagory;
}

struct RangeExprCategoryCmp
{

  inline bool operator()(ObRawExpr *left, ObRawExpr *right)
  {
    bool bret = false;
    if (left != nullptr && right != nullptr) {
      int64_t l_catagory = ObExprRangeConverter::get_expr_category(left->get_expr_type());
      int64_t r_catagory = ObExprRangeConverter::get_expr_category(right->get_expr_type());
      if (l_catagory != r_catagory) {
        bret = l_catagory < r_catagory;
      } else if (l_catagory == RANGE_EXPR_IN ||
                 r_catagory == RANGE_EXPR_NOT_IN) {
        if (left->get_param_expr(1) != nullptr &
            right->get_param_expr(1) != nullptr) {
          bret = left->get_param_expr(1)->get_param_count() <
                 right->get_param_expr(1)->get_param_count();
        }
      } else if (left->has_flag(CNT_DYNAMIC_PARAM) || right->has_flag(CNT_DYNAMIC_PARAM)) {
        bret = !left->has_flag(CNT_DYNAMIC_PARAM) &&
               right->has_flag(CNT_DYNAMIC_PARAM);
      } else {
        bret = false;
      }
    }
    return bret;
  }
};

int ObExprRangeConverter::sort_range_exprs(const ObIArray<ObRawExpr*> &range_exprs,
                                            ObIArray<ObRawExpr*> &out_range_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(out_range_exprs.assign(range_exprs))) {
    LOG_WARN("failed to assign range exprs", K(ret));
  } else if (!range_exprs.empty()) {
    lib::ob_sort(&out_range_exprs.at(0), &out_range_exprs.at(0) + out_range_exprs.count(),
              RangeExprCategoryCmp());
  }
  return ret;
}

int ObExprRangeConverter::try_wrap_lob_with_substr(const ObRawExpr *expr,
                                                   const ObRangeColumnMeta *column_meta,
                                                   const ObRawExpr *&out_expr)
{
  int ret = OB_SUCCESS;
  ObSysFunRawExpr *substr_expr = NULL;
  ObConstRawExpr *pos_expr = NULL;
  ObConstRawExpr *truncated_len_expr = NULL;
  int64_t truncated_str_len = 0;
  out_expr = expr;
  if (OB_ISNULL(expr) ||
      OB_ISNULL(column_meta) ||
      OB_ISNULL(ctx_.expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(expr), K(column_meta), K(ctx_.expr_factory_));
  } else if (!expr->get_result_type().is_lob_storage() ||
             !column_meta->column_type_.is_string_type() ||
             column_meta->column_type_.get_accuracy().get_length() < 0) {
    // do nothing
  } else if (OB_FALSE_IT(truncated_str_len =
                         column_meta->column_type_.get_accuracy().get_length() + 1)) {
  } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*ctx_.expr_factory_,
                                                          ObIntType,
                                                          1, // const value
                                                          pos_expr))) {
    LOG_WARN("Failed to build const expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*ctx_.expr_factory_,
                                                          ObIntType,
                                                          truncated_str_len,
                                                          truncated_len_expr))) {
    LOG_WARN("failed to build const int expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::create_substr_expr(*ctx_.expr_factory_,
                                                        ctx_.session_info_,
                                                        const_cast<ObRawExpr*>(expr),
                                                        pos_expr,
                                                        truncated_len_expr,
                                                        substr_expr))) {
    LOG_WARN("failed to create substr expr", K(ret), KPC(expr), KPC(pos_expr), KPC(truncated_len_expr));
  } else {
    out_expr = substr_expr;
    ctx_.cur_is_precise_ = false;
  }
  return ret;
}

int ObExprRangeConverter::set_column_flags(int64_t key_idx, ObItemType type)
{
  int ret = OB_SUCCESS;
  int64_t catagory = get_expr_category(type);
  if (OB_UNLIKELY(key_idx >= ctx_.column_flags_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected key idx", K(ret));
  } else {
    ctx_.column_flags_[key_idx] |= catagory;
  }
  return ret;
}

int ObExprRangeConverter::get_domain_extra_item(const common::ObDomainOpType op_type,
                                                const ObRawExpr *expr,
                                                const ObConstRawExpr *&extra_item)
{
  int ret = OB_SUCCESS;
  extra_item = NULL;
  if (op_type == ObDomainOpType::T_GEO_DWITHIN ||
      op_type == ObDomainOpType::T_GEO_RELATE) {
    if (expr->get_param_count() != 3) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid param num", K(expr->get_param_count()));
    } else {
      extra_item = static_cast<const ObConstRawExpr *>(expr->get_param_expr(2));
      if (OB_ISNULL(extra_item)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid param val", K(ret));
      }
    }
  }
  return ret;
}

int ObExprRangeConverter::fill_range_node_for_geo_node(const int64_t key_idx,
                                                       common::ObDomainOpType geo_type,
                                                       uint32_t srid,
                                                       const int64_t start_val_idx,
                                                       const int64_t end_val_idx,
                                                       ObRangeNode &range_node) const
{
  int ret = OB_SUCCESS;
  OB_ASSERT(key_idx < ctx_.column_cnt_);
  range_node.min_offset_ = key_idx;
  range_node.max_offset_ = key_idx;

  range_node.domain_extra_.domain_releation_type_ = (int32_t)geo_type;
  range_node.is_domain_node_ = 1;

  range_node.include_start_ = true;
  range_node.include_end_ = true;
  range_node.domain_extra_.srid_ = srid;

  for (int64_t i = 0; i < key_idx; ++i) {
    range_node.start_keys_[i] = OB_RANGE_EMPTY_VALUE;
    range_node.end_keys_[i] = OB_RANGE_EMPTY_VALUE;
  }

  range_node.start_keys_[key_idx] = start_val_idx;
  range_node.end_keys_[key_idx] = end_val_idx;

  for (int64_t i = key_idx + 1; i < ctx_.column_cnt_; ++i) {
    range_node.start_keys_[i] = OB_RANGE_MIN_VALUE;
    range_node.end_keys_[i] = OB_RANGE_MAX_VALUE;
  }
  return ret;
}

int ObExprRangeConverter::convert_geo_expr(const ObRawExpr *geo_expr,
                                           int64_t expr_depth,
                                           ObRangeNode *&range_node)
{
  int ret = OB_SUCCESS;
  UNUSED(expr_depth);
  if (OB_ISNULL(geo_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    const ObRawExpr *expr = ObRawExprUtils::skip_inner_added_expr(geo_expr);
    const ObRawExpr *l_expr = expr->get_param_expr(0);
    const ObRawExpr *r_expr = expr->get_param_expr(1);
    common::ObDomainOpType op_type;
    const ObConstRawExpr *extra_item = NULL;
    const ObRawExpr *const_item = NULL;
    const ObColumnRefRawExpr *column_item = NULL;
    if (OB_ISNULL(l_expr) || OB_ISNULL(r_expr) ||
        OB_ISNULL(ctx_.geo_column_id_map_)) {
      // do nothing
    } else if (l_expr->has_flag(CNT_COLUMN) && r_expr->has_flag(CNT_COLUMN)) {
      // do nothing
    } else if (l_expr->has_flag(IS_DYNAMIC_PARAM) && r_expr->has_flag(IS_DYNAMIC_PARAM)) {
      // do nothing
    } else if (!l_expr->has_flag(CNT_COLUMN) && !r_expr->has_flag(CNT_COLUMN)) {
      // do nothing
    } else {
      op_type = ObQueryRange::get_geo_relation(expr->get_expr_type());
      if (OB_UNLIKELY(r_expr->has_flag(CNT_COLUMN))) {
        column_item = ObRawExprUtils::get_column_ref_expr_recursively(r_expr);
        const_item = l_expr;
      } else if (l_expr->has_flag(CNT_COLUMN)) {
        column_item = ObRawExprUtils::get_column_ref_expr_recursively(l_expr);
        const_item = r_expr;
        op_type = (ObDomainOpType::T_GEO_COVERS == op_type ? ObDomainOpType::T_GEO_COVEREDBY :
                  (ObDomainOpType::T_GEO_COVEREDBY == op_type ? ObDomainOpType::T_GEO_COVERS : op_type));
      }
      if (OB_ISNULL(column_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to find column item", K(ret), KPC(r_expr), KPC(l_expr));
      } else if (column_item->get_data_type() != ObObjType::ObGeometryType) {
        // do nothing
      } else if (OB_FAIL(get_domain_extra_item(op_type, expr, extra_item))) {
        LOG_WARN("failed to get dwithin item", K(ret));
      } else if (OB_FAIL(get_geo_range_node(column_item,
                                            op_type,
                                            const_item,
                                            extra_item,
                                            range_node))) {
        LOG_WARN("failed to get geo range node", K(ret));
      } else if (nullptr != range_node) {
        ctx_.contail_geo_filters_ = true;
      }
    }
  }

  if (OB_SUCC(ret) && nullptr == range_node) {
    ctx_.cur_is_precise_ = false;
    if (OB_FAIL(generate_always_true_or_false_node(true, range_node))) {
      LOG_WARN("failed to generate always true or fasle node", K(ret));
    }
  }
  return ret;
}

int ObExprRangeConverter::get_geo_range_node(const ObColumnRefRawExpr *column_expr,
                                             common::ObDomainOpType geo_type,
                                             const ObRawExpr* wkb_expr,
                                             const ObRawExpr *extra_expr,
                                             ObRangeNode *&range_node)
{
  int ret = OB_SUCCESS;
  bool is_cellid_col = false;
  ObGeoColumnInfo column_info;
  int64_t key_idx = -1;
  int64_t wkb_val = -1;
  int64_t distance_val = -1;
  bool is_valid = false;
  range_node = NULL;
  bool is_geo_type = ObRangeGenerator::is_geo_type(geo_type);
  if (OB_ISNULL(column_expr) ||
      OB_ISNULL(wkb_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx_.geo_column_id_map_), K(column_expr),
                                    K(wkb_expr), K(lbt()));
  } else {
    if (is_geo_type) {
      if (OB_ISNULL(ctx_.geo_column_id_map_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ctx_.geo_column_id_map_));
      } else if (OB_FAIL(ctx_.geo_column_id_map_->get_refactored(column_expr->get_column_id(),
                                                          column_info))) {
        if (OB_NOT_INIT == ret || OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get from geo column id map", K(ret));
        }
      } else {
        is_cellid_col = true;
      }
    } else {
      column_info.srid_ = 0;
      column_info.cellid_columnId_ = column_expr->get_column_id();
    }

    if (OB_SUCC(ret)) {
      uint64_t column_id = is_cellid_col ? column_info.cellid_columnId_ : column_expr->get_column_id();
      bool can_extract_range = false;
      if (!is_range_key(column_id, key_idx)) {
        // do nothing
      } else if (OB_FAIL(check_calculable_expr_valid(wkb_expr, is_valid))) {
        LOG_WARN("failed to get calculable expr val");
      } else if (!is_valid) {
        // do nothing
      } else if (OB_FAIL(get_final_expr_idx(wkb_expr, nullptr, wkb_val))) {
        LOG_WARN("failed to get final expr idx");
      } else if (geo_type == ObDomainOpType::T_GEO_RELATE) {
        if (OB_FAIL(get_orcl_spatial_relationship(extra_expr,
                                                  can_extract_range,
                                                  geo_type))) {
          LOG_WARN("failed to get orcl spatial relationship", K(ret));
        }
      } else if (geo_type == ObDomainOpType::T_GEO_DWITHIN ) {
        if (OB_ISNULL(extra_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(check_calculable_expr_valid(extra_expr, is_valid))) {
          LOG_WARN("failed to check calculable expr valid", K(ret));
        } else if (!is_valid) {
          // do nothing
        } else if (OB_FAIL(get_final_expr_idx(extra_expr, nullptr, distance_val))) {
          LOG_WARN("failed to get final expr idx", K(ret));
        } else {
          can_extract_range = true;
        }
      } else {
        can_extract_range = true;
      }

      if (OB_FAIL(ret)) {
      } else if (!can_extract_range) {
        // do nothing
      } else if (OB_FAIL(alloc_range_node(range_node))) {
        LOG_WARN("failed to alloc range node", K(ret));
      } else if (OB_FAIL(fill_range_node_for_geo_node(key_idx,
                                                      geo_type,
                                                      column_info.srid_,
                                                      wkb_val,
                                                      distance_val,
                                                      *range_node))) {
        LOG_WARN("failed to fill range node for geo node", K(ret));
      } else {
        ctx_.cur_is_precise_ = false;
      }
    }

  }
  return ret;
}

int ObExprRangeConverter::get_implicit_cast_range(const ObRawExpr &l_expr,
                                                  const ObRawExpr &r_expr,
                                                  ObItemType cmp_type,
                                                  int64_t expr_depth,
                                                  ObRangeNode *&range_node)
{
  int ret = OB_SUCCESS;
  const ObRawExpr *inner_expr = nullptr;
  const ObRawExpr *const_expr = nullptr;
  int64_t key_idx = 0;
  bool can_extract = false;
  if (l_expr.get_expr_type() == T_FUN_SYS_CAST && r_expr.is_const_expr()) {
    inner_expr = l_expr.get_param_expr(0);
    const_expr = &r_expr;
  } else if (r_expr.get_expr_type() == T_FUN_SYS_CAST && l_expr.is_const_expr()) {
    inner_expr = r_expr.get_param_expr(0);
    const_expr = &l_expr;
    cmp_type = get_opposite_compare_type(cmp_type);
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(inner_expr)) {
    if (!inner_expr->has_flag(IS_COLUMN)) {
      // do nothing
    } else if (!is_range_key(static_cast<const ObColumnRefRawExpr*>(inner_expr)->get_column_id(),
                             key_idx)) {
      // do nothing
    } else if (OB_FAIL(can_extract_implicit_cast_range(cmp_type,
                                                       *static_cast<const ObColumnRefRawExpr*>(inner_expr),
                                                       *const_expr,
                                                       can_extract))) {
      LOG_WARN("failed to check can extract implicit range", K(ret));
    } else if (!can_extract) {
      // do nothing
    } else if (OB_FAIL(gen_implicit_cast_range(static_cast<const ObColumnRefRawExpr*>(inner_expr),
                                               const_expr,
                                               cmp_type,
                                               expr_depth,
                                               range_node))) {
      LOG_WARN("failed to get implicit range", K(ret));
    }
  }

  if (OB_SUCC(ret) && nullptr == range_node) {
    ctx_.cur_is_precise_ = false;
    if (OB_FAIL(generate_always_true_or_false_node(true, range_node))) {
      LOG_WARN("failed to generate always true or fasle node", K(ret));
    }
  }
  return ret;
}

int ObExprRangeConverter::gen_implicit_cast_range(const ObColumnRefRawExpr *column_expr,
                                                  const ObRawExpr *const_expr,
                                                  ObItemType cmp_type,
                                                  int64_t expr_depth,
                                                  ObRangeNode *&range_node)
{
  int ret = OB_SUCCESS;
  const ObRawExpr *start_expr = nullptr;
  const ObRawExpr *end_expr = nullptr;
  ObSEArray<ObRangeNode*, 2> range_nodes;
  if (OB_ISNULL(column_expr) ||
      OB_ISNULL(const_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexptected null", K(ret), K(column_expr), K(const_expr));
  } else if ((column_expr->get_result_type().get_type() ==
              const_expr->get_result_type().get_type()) &&
             column_expr->get_result_type().is_string_type()) {
    if (OB_FAIL(get_basic_range_node(column_expr,
                                     const_expr,
                                     cmp_type,
                                     expr_depth + 1,
                                     range_node))) {
      LOG_WARN("failed to get basic range node", K(ret));
    } else {
      ctx_.cur_is_precise_ = false;
    }
  } else if (T_OP_EQ == cmp_type || T_OP_NSEQ == cmp_type) {
    ObRangeNode *start_range = nullptr;
    ObRangeNode *end_range = nullptr;
    bool is_precise = true;
    if (OB_FAIL(build_implicit_cast_range_expr(column_expr,
                                               const_expr,
                                               cmp_type,
                                               true,
                                               start_expr))) {
      LOG_WARN("failed to build double to int expr", K(ret));
    } else if (OB_FAIL(build_implicit_cast_range_expr(column_expr,
                                                      const_expr,
                                                      cmp_type,
                                                      false,
                                                      end_expr))) {
      LOG_WARN("failed to build double to int expr", K(ret));
    } else if (OB_FAIL(gen_column_cmp_node(*column_expr,
                                           *start_expr,
                                           T_OP_GE,
                                           start_expr->get_result_type(),
                                           expr_depth,
                                           T_OP_NSEQ == cmp_type,
                                           start_range))) {
      LOG_WARN("failed to get basic range node", K(ret));
    } else if (OB_FAIL(range_nodes.push_back(start_range))) {
      LOG_WARN("failed to push back and range nodes", K(ret));
    } else if (OB_FAIL(gen_column_cmp_node(*column_expr,
                                           *end_expr,
                                           T_OP_LE,
                                           end_expr->get_result_type(),
                                           expr_depth,
                                           T_OP_NSEQ == cmp_type,
                                           end_range))) {
      LOG_WARN("failed to get basic range node", K(ret));
    } else if (OB_FAIL(range_nodes.push_back(end_range))) {
      LOG_WARN("failed to push back and range node", K(ret));
    } else if (OB_FAIL(ObRangeGraphGenerator::and_range_nodes(range_nodes, ctx_, range_node))) {
      LOG_WARN("failed to and range nodes");
    } else if (OB_FAIL(set_extract_implicit_is_precise(*column_expr,
                                                       *const_expr,
                                                       cmp_type,
                                                       is_precise))) {
      LOG_WARN("failed to set extract implicit is precise", K(ret));
    } else if (!is_precise) {
      ctx_.cur_is_precise_ = false;
    }
  } else if (T_OP_GT == cmp_type ||
             T_OP_GE == cmp_type ||
             T_OP_LT == cmp_type ||
             T_OP_LE == cmp_type) {
    if (T_OP_GT == cmp_type) {
      cmp_type = T_OP_GE;
    } else if (T_OP_LT == cmp_type) {
      cmp_type = T_OP_LE;
    }
    if (OB_FAIL(build_implicit_cast_range_expr(column_expr,
                                               const_expr,
                                               cmp_type,
                                               T_OP_GE == cmp_type,
                                               start_expr))) {
      LOG_WARN("failed to build double to int expr", K(ret));
    } else if (OB_FAIL(gen_column_cmp_node(*column_expr,
                                           *start_expr,
                                           cmp_type,
                                           start_expr->get_result_type(),
                                           expr_depth,
                                           false,
                                           range_node))) {
      LOG_WARN("failed to get basic range node", K(ret));
    } else {
      ctx_.cur_is_precise_ = false;
    }
  }
  return ret;
}

int ObExprRangeConverter::build_double_to_int_expr(const ObRawExpr *double_expr,
                                                   bool is_start,
                                                   ObItemType cmp_type,
                                                   bool is_unsigned,
                                                   bool is_decimal,
                                                   const ObRawExpr *&out_expr)
{
  int ret = OB_SUCCESS;
  ObSysFunRawExpr *inner_double_to_int = NULL;
  out_expr = NULL;
  int64_t extra = 0;
  bool is_equal = cmp_type == T_OP_EQ || cmp_type == T_OP_NSEQ;
  if (OB_ISNULL(double_expr) || OB_ISNULL(ctx_.expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(double_expr), K(ctx_.expr_factory_));
  } else if (OB_FAIL(ctx_.expr_factory_->create_raw_expr(T_FUN_SYS_INNER_DOUBLE_TO_INT,
                                                         inner_double_to_int))) {
    LOG_WARN("failed to create a new expr", K(ret));
  } else if (OB_ISNULL(inner_double_to_int)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate expr", K(out_expr));
  } else if (is_start && OB_FALSE_IT(extra |= 1)) {
  } else if (is_equal && OB_FALSE_IT(extra |= 2)) {
  } else if (is_unsigned && OB_FALSE_IT(extra |= 4)) {
  } else if (is_decimal && OB_FALSE_IT(extra |= 8)) {
  } else if (OB_FAIL(inner_double_to_int->set_param_expr(const_cast<ObRawExpr*>(double_expr)))) {
    LOG_WARN("failed to add param expr", K(ret));
  } else if (OB_FALSE_IT(inner_double_to_int->set_range_flag(extra))) {
  } else if (OB_FAIL(inner_double_to_int->formalize(ctx_.session_info_))) {
    LOG_WARN("failed to formalize expr");
  } else {
    out_expr = inner_double_to_int;
  }
  return ret;
}

int ObExprRangeConverter::get_row_cmp_node(const ObRawExpr &l_expr,
                                           const ObRawExpr &r_expr,
                                           ObItemType cmp_type,
                                           int64_t expr_depth,
                                           ObRangeNode *&range_node)
{
  int ret = OB_SUCCESS;
  if(OB_UNLIKELY(l_expr.get_param_count() != r_expr.get_param_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected param count", K(l_expr), K(r_expr));
  } else {
    ObSEArray<const ObColumnRefRawExpr*, 4> column_exprs;
    ObSEArray<const ObRawExpr*, 4> const_exprs;
    ObSEArray<const ObObjMeta*, 4> calc_types;
    ObSEArray<int64_t, 4> implicit_cast_idxs;
    bool can_reverse = true;
    bool is_reverse = false;
    bool use_implicit_cast_feature = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < l_expr.get_param_count(); ++i) {
      const ObRawExpr* l_param = l_expr.get_param_expr(i);
      const ObRawExpr* r_param = r_expr.get_param_expr(i);
      const ObRawExpr* ori_column_expr = nullptr;
      const ObColumnRefRawExpr* column_expr = nullptr;
      const ObRawExpr* const_expr = nullptr;
      bool is_implicit_cast = false;
      if (OB_ISNULL(l_param) || OB_ISNULL(r_param)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null expr", K(l_param), K(r_param));
      } else if (l_param->get_expr_type() != T_FUN_SYS_INNER_ROW_CMP_VALUE &&
                 OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(l_param, l_param, use_implicit_cast_feature))) {
        LOG_WARN("failed to get expr without lossless cast", K(ret));
      } else if (r_param->get_expr_type() != T_FUN_SYS_INNER_ROW_CMP_VALUE &&
                 OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(r_param, r_param, use_implicit_cast_feature))) {
        LOG_WARN("failed to get expr without lossless cast", K(ret));
      } else if (l_param->has_flag(IS_COLUMN) && r_param->is_const_expr()) {
        if (T_OP_EQ == cmp_type && T_OP_NSEQ == cmp_type) {
          column_expr = static_cast<const ObColumnRefRawExpr*>(l_param);
          ori_column_expr = l_expr.get_param_expr(i);
          const_expr = r_param;
        } else if (can_reverse) {
          can_reverse = false;
          column_expr = static_cast<const ObColumnRefRawExpr*>(l_param);
          ori_column_expr = l_expr.get_param_expr(i);
          const_expr = r_param;
        } else if (!is_reverse) {
          column_expr = static_cast<const ObColumnRefRawExpr*>(l_param);
          ori_column_expr = l_expr.get_param_expr(i);
          const_expr = r_param;
        }
      } else if (r_param->has_flag(IS_COLUMN) && l_param->is_const_expr()) {
        if (T_OP_EQ == cmp_type && T_OP_NSEQ == cmp_type) {
          column_expr = static_cast<const ObColumnRefRawExpr*>(r_param);
          ori_column_expr = r_expr.get_param_expr(i);
          const_expr = l_param;
        } else if (can_reverse) {
          can_reverse = false;
          is_reverse = true;
          column_expr = static_cast<const ObColumnRefRawExpr*>(r_param);
          ori_column_expr = r_expr.get_param_expr(i);
          const_expr = l_param;
          cmp_type = get_opposite_compare_type(cmp_type);
        } else if (is_reverse) {
          column_expr = static_cast<const ObColumnRefRawExpr*>(r_param);
          ori_column_expr = r_expr.get_param_expr(i);
          const_expr = l_param;
        }
      } else if (!(ObSQLUtils::is_min_cluster_version_ge_425_or_435() &&
                   ObSQLUtils::is_opt_feature_version_ge_425_or_435(ctx_.optimizer_features_enable_version_))) {
        // do nothing
      } else if (OB_FALSE_IT(l_param = l_expr.get_param_expr(i)) ||
                 OB_FALSE_IT(r_param = r_expr.get_param_expr(i))) {
      } else if (l_param->get_expr_type() == T_FUN_SYS_CAST && r_param->is_const_expr()) {
        l_param = l_param->get_param_expr(0);
        bool can_extract = false;
        if (!l_param->has_flag(IS_COLUMN)) {
          // do nothing
        } else if (OB_FAIL(can_extract_implicit_cast_range(cmp_type,
                                                           *static_cast<const ObColumnRefRawExpr*>(l_param),
                                                           *r_param,
                                                           can_extract))) {
          LOG_WARN("failed to check can extract implicit cast range", K(ret));
        } else if (can_extract) {
          is_implicit_cast = true;
          if (T_OP_EQ == cmp_type && T_OP_NSEQ == cmp_type) {
            column_expr = static_cast<const ObColumnRefRawExpr*>(l_param);
            ori_column_expr = l_expr.get_param_expr(i);
            const_expr = r_param;
          } else if (can_reverse) {
            can_reverse = false;
            column_expr = static_cast<const ObColumnRefRawExpr*>(l_param);
            ori_column_expr = l_expr.get_param_expr(i);
            const_expr = r_param;
          } else if (!is_reverse) {
            column_expr = static_cast<const ObColumnRefRawExpr*>(l_param);
            ori_column_expr = l_expr.get_param_expr(i);
            const_expr = r_param;
          } else {
            is_implicit_cast = false;
          }
        }
      } else if (r_param->get_expr_type() == T_FUN_SYS_CAST && l_param->is_const_expr()) {
        bool can_extract = false;
        r_param = r_param->get_param_expr(0);
        if (!r_param->has_flag(IS_COLUMN)) {
          // do nothing
        } else if (OB_FAIL(can_extract_implicit_cast_range(cmp_type,
                                                           *static_cast<const ObColumnRefRawExpr*>(r_param),
                                                           *l_param,
                                                           can_extract))) {
          LOG_WARN("failed to check can extract implicit cast range", K(ret));
        } else if (can_extract) {
          is_implicit_cast = true;
          if (T_OP_EQ == cmp_type  && T_OP_NSEQ == cmp_type) {
            column_expr = static_cast<const ObColumnRefRawExpr*>(r_param);
            ori_column_expr = r_expr.get_param_expr(i);
            const_expr = l_param;
          } else if (can_reverse) {
            can_reverse = false;
            is_reverse = true;
            column_expr = static_cast<const ObColumnRefRawExpr*>(r_param);
            ori_column_expr = r_expr.get_param_expr(i);
            const_expr = l_param;
            cmp_type = get_opposite_compare_type(cmp_type);
          } else if (is_reverse) {
            column_expr = static_cast<const ObColumnRefRawExpr*>(r_param);
            ori_column_expr = r_expr.get_param_expr(i);
            const_expr = l_param;
          } else {
            is_implicit_cast = false;
          }
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(column_exprs.push_back(column_expr))) {
          LOG_WARN("failed to push back array");
        } else if (OB_FAIL(const_exprs.push_back(const_expr))) {
          LOG_WARN("failed to push back array");
        } else if (OB_FAIL(calc_types.push_back(&ori_column_expr->get_result_type()))) {
          LOG_WARN("failed to push back array");
        } else if (is_implicit_cast && OB_FAIL(implicit_cast_idxs.push_back(i)))  {
          LOG_WARN("failed to push back array");
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (implicit_cast_idxs.empty()) {
      if (OB_FAIL(gen_row_column_cmp_node(column_exprs,
                                          const_exprs,
                                          cmp_type,
                                          calc_types,
                                          expr_depth,
                                          l_expr.get_param_count(),
                                          T_OP_NSEQ == cmp_type,
                                          range_node))) {
        LOG_WARN("failed to gen row column cmp node", K(ret));
      }
    } else {
      if (OB_FAIL(gen_row_implicit_cast_range(column_exprs,
                                              const_exprs,
                                              cmp_type,
                                              calc_types,
                                              implicit_cast_idxs,
                                              expr_depth,
                                              l_expr.get_param_count(),
                                              range_node))) {
        LOG_WARN("failed to get row real to int range", K(ret));
      }
    }
  }
  return ret;
}

int ObExprRangeConverter::gen_row_implicit_cast_range(const ObIArray<const ObColumnRefRawExpr*> &column_exprs,
                                                      const ObIArray<const ObRawExpr*> &const_exprs,
                                                      ObItemType cmp_type,
                                                      const ObIArray<const ObObjMeta*> &calc_types,
                                                      ObIArray<int64_t> &implicit_cast_idxs,
                                                      int64_t expr_depth,
                                                      int64_t row_dim,
                                                      ObRangeNode *&range_node)
{
  int ret = OB_SUCCESS;
  ObSEArray<const ObColumnRefRawExpr*, 4> ordered_column_exprs;
  ObSEArray<const ObRawExpr*, 4> ordered_const_exprs;
  ObSEArray<const ObObjMeta*, 4> ordered_calc_types;
  ObSEArray<ObRangeNode*, 2> range_nodes;
  if (OB_UNLIKELY(column_exprs.count() != const_exprs.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected param count", K(column_exprs), K(const_exprs));
  } else if (T_OP_EQ == cmp_type || T_OP_NSEQ == cmp_type) {
    ObSEArray<int64_t, 4> key_idxs;
    ObSEArray<int64_t, 4> val_idxs;
    ObSEArray<int64_t, 4> cast_idxs;
    ObSEArray<const ObRawExpr*, 4> cast_origin_const_exprs;
    bool cur_is_precise = true;
    int64_t key_idx = -1;
    int64_t min_offset = ctx_.column_cnt_;
    for (int64_t i = 0; OB_SUCC(ret) && i < column_exprs.count(); ++i) {
      const ObColumnRefRawExpr* column_expr = column_exprs.at(i);
      if (OB_NOT_NULL(column_expr)) {
        if (!is_range_key(column_expr->get_column_id(), key_idx)) {
          // do nothing
        } else if (ObOptimizerUtil::find_item(key_idxs, key_idx)) {
          // do nothing
        } else if (OB_FAIL(key_idxs.push_back(key_idx))) {
          LOG_WARN("failed to push back key idx", K(ret));
        } else if (OB_FAIL(val_idxs.push_back(i))) {
          LOG_WARN("failed to push back val idx", K(ret));
        } else if (key_idx < min_offset) {
          min_offset = key_idx;
        }
      }
    }
    for (int64_t i = min_offset; OB_SUCC(ret) && i < ctx_.column_cnt_; ++i) {
      int64_t idx = -1;
      if (ObOptimizerUtil::find_item(key_idxs, i, &idx)) {
        int64_t val_idx = val_idxs.at(idx);
        bool is_precise = true;
        if (OB_FAIL(ordered_column_exprs.push_back(column_exprs.at(val_idx)))) {
          LOG_WARN("failed to push back array", K(ret));
        } else if (OB_FAIL(ordered_const_exprs.push_back(const_exprs.at(val_idx)))) {
          LOG_WARN("failed to push back array", K(ret));
        } else if (OB_FAIL(ordered_calc_types.push_back(calc_types.at(val_idx)))) {
          LOG_WARN("failed to push back array", K(ret));
        } else if (ObOptimizerUtil::find_item(implicit_cast_idxs, val_idx)) {
          ObRawExprResType *res_type = nullptr;
          if (OB_FAIL(set_extract_implicit_is_precise(*column_exprs.at(val_idx),
                                                      *const_exprs.at(val_idx),
                                                      cmp_type,
                                                      is_precise))) {
            LOG_WARN("failed to set extract implicit is precise", K(ret));
          } else if (OB_FAIL(cast_idxs.push_back(ordered_const_exprs.count() - 1))) {
            LOG_WARN("failed to push back array", K(ret));
          } else if (OB_FAIL(cast_origin_const_exprs.push_back(const_exprs.at(val_idx)))) {
            LOG_WARN("failed to push back array", K(ret));
          } else if (!is_precise) {
            cur_is_precise = false;
          }
        }
      } else {
        break;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (ordered_column_exprs.empty()) {
      // do nothing
    } else if (cast_idxs.empty()) {
      if (OB_FAIL(gen_row_column_cmp_node(ordered_column_exprs,
                                          ordered_const_exprs,
                                          cmp_type,
                                          ordered_calc_types,
                                          expr_depth,
                                          row_dim,
                                          T_OP_NSEQ == cmp_type,
                                          range_node))) {
        LOG_WARN("failed to gen row column cmp node", K(ret));
      }
    } else {
      ObRangeNode *start_range = nullptr;
      ObRangeNode *end_range = nullptr;
      for (int64_t i = 0; OB_SUCC(ret) && i < cast_idxs.count(); ++i) {
        int64_t ordered_idx = cast_idxs.at(i);
        const ObRawExpr *out_expr = nullptr;
        if (OB_FAIL(build_implicit_cast_range_expr(ordered_column_exprs.at(ordered_idx),
                                                   cast_origin_const_exprs.at(i),
                                                   cmp_type,
                                                   true,
                                                   out_expr))) {
          LOG_WARN("failed to build implicit cast range expr", K(ret));
        } else {
          ordered_calc_types.at(ordered_idx) = &out_expr->get_result_type();
          ordered_const_exprs.at(ordered_idx) = out_expr;
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(gen_row_column_cmp_node(ordered_column_exprs,
                                                 ordered_const_exprs,
                                                 T_OP_GE,
                                                 ordered_calc_types,
                                                 expr_depth,
                                                 row_dim,
                                                 T_OP_NSEQ == cmp_type,
                                                 start_range))) {
        LOG_WARN("failed to gen row column cmp node", K(ret));
      } else if (OB_FAIL(range_nodes.push_back(start_range))) {
        LOG_WARN("failed to push back array", K(ret));
      }

      for (int64_t i = 0; OB_SUCC(ret) && i < cast_idxs.count(); ++i) {
        int64_t ordered_idx = cast_idxs.at(i);
        const ObRawExpr *out_expr = nullptr;
        if (OB_FAIL(build_implicit_cast_range_expr(ordered_column_exprs.at(ordered_idx),
                                                   cast_origin_const_exprs.at(i),
                                                   cmp_type,
                                                   false,
                                                   out_expr))) {
          LOG_WARN("failed to build implicit cast range expr", K(ret));
        } else {
          ordered_calc_types.at(ordered_idx) = &out_expr->get_result_type();
          ordered_const_exprs.at(ordered_idx) = out_expr;
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(gen_row_column_cmp_node(ordered_column_exprs,
                                                 ordered_const_exprs,
                                                 T_OP_LE,
                                                 ordered_calc_types,
                                                 expr_depth,
                                                 row_dim,
                                                 T_OP_NSEQ == cmp_type,
                                                 end_range))) {
        LOG_WARN("failed to push back array", K(ret));
      } else if (OB_FAIL(range_nodes.push_back(end_range))) {
        LOG_WARN("failed to push back array", K(ret));
      } else if (OB_FAIL(ObRangeGraphGenerator::and_range_nodes(range_nodes, ctx_, range_node))) {
        LOG_WARN("failed to and range nodes");
      } else if (!cur_is_precise) {
        ctx_.cur_is_precise_ = false;
      }
    }
  } else if (T_OP_GT == cmp_type ||
             T_OP_GE == cmp_type ||
             T_OP_LT == cmp_type ||
             T_OP_LE == cmp_type) {
    bool is_start = false;
    bool check_next = true;
    ObSEArray<ObRawExprResType*, 4> cache_calc_types;
    if (!implicit_cast_idxs.empty()) {
      if (T_OP_GT == cmp_type) {
        cmp_type = T_OP_GE;
      } else if (T_OP_LT == cmp_type) {
        cmp_type = T_OP_LE;
      }
    }
    is_start = (T_OP_GE == cmp_type);
    for (int64_t i = 0; OB_SUCC(ret) && check_next && i < column_exprs.count(); ++i) {
      const ObRawExpr *out_expr = nullptr;
      if (OB_ISNULL(column_exprs.at(i)) || OB_ISNULL(const_exprs.at(i)) ||
          OB_ISNULL(calc_types.at(i))) {
        check_next = false;
      } else if (OB_FAIL(ordered_column_exprs.push_back(column_exprs.at(i)))) {
        LOG_WARN("failed to push back array", K(ret));
      } else if (OB_FAIL(ordered_const_exprs.push_back(const_exprs.at(i)))) {
        LOG_WARN("failed to push back array", K(ret));
      } else if (OB_FAIL(ordered_calc_types.push_back(calc_types.at(i)))) {
        LOG_WARN("failed to push back array", K(ret));
      } else if (ObOptimizerUtil::find_item(implicit_cast_idxs, i)) {
        if (OB_FAIL(build_implicit_cast_range_expr(column_exprs.at(i),
                                                   const_exprs.at(i),
                                                   cmp_type,
                                                   is_start,
                                                   out_expr))) {
          LOG_WARN("failed to build implicit cast range expr", K(ret));
        } else {
          ordered_calc_types.at(i) = &out_expr->get_result_type();
          ordered_const_exprs.at(i) = out_expr;
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(gen_row_column_cmp_node(ordered_column_exprs,
                                                        ordered_const_exprs,
                                                        cmp_type,
                                                        ordered_calc_types,
                                                        expr_depth,
                                                        row_dim,
                                                        false,
                                                        range_node))) {
      LOG_WARN("faield to gen row column cmp node", K(ret));
    } else {
      ctx_.cur_is_precise_ = false;
    }

    for (int64_t i = 0; i < cache_calc_types.count(); ++i) {
      if (OB_NOT_NULL(cache_calc_types.at(i))) {
        cache_calc_types.at(i)->~ObRawExprResType();
      }
    }
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(nullptr == range_node)) {
    ctx_.cur_is_precise_ = false;
    if (OB_FAIL(generate_always_true_or_false_node(true, range_node))) {
      LOG_WARN("failed to generate always true or fasle node", K(ret));
    }
  }
  return ret;
}

int ObExprRangeConverter::build_decimal_to_year_expr(const ObRawExpr *decimal_expr,
                                                     bool is_start,
                                                     ObItemType cmp_type,
                                                     const ObRawExpr *&out_expr)
{
  int ret = OB_SUCCESS;
  ObSysFunRawExpr *inner_decimal_to_year = NULL;
  out_expr = NULL;
  int64_t extra = 0;
  bool is_equal = cmp_type == T_OP_EQ || cmp_type == T_OP_NSEQ;
  if (OB_ISNULL(decimal_expr) || OB_ISNULL(ctx_.expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(decimal_expr), K(ctx_.expr_factory_));
  } else if (OB_FAIL(ctx_.expr_factory_->create_raw_expr(T_FUN_SYS_INNER_DECIMAL_TO_YEAR,
                                                         inner_decimal_to_year))) {
    LOG_WARN("failed to create a new expr", K(ret));
  } else if (OB_ISNULL(inner_decimal_to_year)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate expr", K(out_expr));
  } else if (is_start && OB_FALSE_IT(extra |= 1)) {
  } else if (is_equal && OB_FALSE_IT(extra |= 2)) {
  } else if (OB_FAIL(inner_decimal_to_year->set_param_expr(const_cast<ObRawExpr*>(decimal_expr)))) {
    LOG_WARN("failed to add param expr", K(ret));
  } else if (OB_FALSE_IT(inner_decimal_to_year->set_range_flag(extra))) {
  } else if (OB_FAIL(inner_decimal_to_year->formalize(ctx_.session_info_))) {
    LOG_WARN("failed to formalize expr");
  } else {
    out_expr = inner_decimal_to_year;
  }
  return ret;
}

int ObExprRangeConverter::build_implicit_cast_range_expr(const ObColumnRefRawExpr *column_expr,
                                                         const ObRawExpr *const_expr,
                                                         ObItemType cmp_type,
                                                         bool is_start,
                                                         const ObRawExpr *&out_expr)
{
  int ret = OB_SUCCESS;
  ObObjTypeClass column_tc = ObNullTC;
  ObObjTypeClass const_tc = ObNullTC;
  if (OB_ISNULL(column_expr) ||
      OB_ISNULL(const_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(column_expr), K(const_expr));
  } else {
    column_tc = column_expr->get_result_type().get_type_class();
    const_tc = const_expr->get_result_type().get_type_class();
    if (ObIntTC == column_tc &&
        (const_tc == ObDoubleTC || const_tc == ObFloatTC)) {
      if (OB_FAIL(build_double_to_int_expr(const_expr,
                                           is_start,
                                           cmp_type,
                                           false,
                                           false,
                                           out_expr))) {
        LOG_WARN("failed to build double to int expr", K(ret));
      }
    } else if (ObUIntTC == column_tc &&
               (const_tc == ObDoubleTC || const_tc == ObFloatTC)) {
      if (OB_FAIL(build_double_to_int_expr(const_expr,
                                           is_start,
                                           cmp_type,
                                           true,
                                           false,
                                           out_expr))) {
        LOG_WARN("failed to build double to int expr", K(ret));
      }
    } else if (ObNumberTC == column_tc &&
               (const_tc == ObDoubleTC || const_tc == ObFloatTC)) {
      if (OB_FAIL(build_double_to_int_expr(const_expr,
                                           is_start,
                                           cmp_type,
                                           false,
                                           true,
                                           out_expr))) {
        LOG_WARN("failed to build double to int expr", K(ret));
      }
    } else if (ObYearTC == column_tc &&
               (const_tc == ObNumberTC || const_tc == ObIntTC || const_tc == ObDecimalIntTC)) {
      if (OB_FAIL(build_decimal_to_year_expr(const_expr,
                                             is_start,
                                             cmp_type,
                                             out_expr))) {
        LOG_WARN("failed to build decimal to year expr", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("not support implicit cast range extract", K(ret), K(column_tc), K(const_tc));
    }
  }
  return ret;
}

int ObExprRangeConverter::can_extract_implicit_cast_range(ObItemType cmp_type,
                                                          const ObColumnRefRawExpr &column_expr,
                                                          const ObRawExpr &const_expr,
                                                          bool &can_extract)
{
  int ret = OB_SUCCESS;
  ObObjTypeClass column_tc = column_expr.get_result_type().get_type_class();
  ObObjTypeClass const_tc = const_expr.get_result_type().get_type_class();
  can_extract = false;
  if (lib::is_oracle_mode()) {
    can_extract = false;
  } else if (column_expr.get_result_type().get_type() ==
              const_expr.get_result_type().get_type() &&
             column_expr.get_result_type().is_string_type()) {
    if (OB_FAIL(ObOptimizerUtil::is_implicit_collation_range_valid(cmp_type,
                                                  column_expr.get_result_type().get_collation_type(),
                                                  const_expr.get_result_type().get_collation_type(),
                                                  can_extract))) {
      LOG_WARN("failed to check implicit collation range", K(ret));
    }
  } else if ((ObIntTC == column_tc || ObUIntTC == column_tc) &&
             (const_tc == ObDoubleTC || const_tc == ObFloatTC)) {
    can_extract = true;
  } else if (ObNumberTC == column_tc &&
             (const_tc == ObDoubleTC || const_tc == ObFloatTC)) {
    can_extract = true;
  } else if (ObYearTC == column_tc &&
             (const_tc == ObNumberTC || const_tc == ObIntTC || const_tc == ObDecimalIntTC)) {
    can_extract = true;
  }

  if (OB_SUCC(ret) && can_extract) {
    bool is_valid = false;
    if (OB_FAIL(check_calculable_expr_valid(&const_expr, is_valid))) {
      LOG_WARN("failed to check calculable expr valid", K(ret));
    } else if (!is_valid) {
      can_extract = false;
    }
  }
  return ret;
}

int ObExprRangeConverter::check_can_use_range_get(const ObRawExpr &const_expr,
                                                  const ObRangeColumnMeta &column_meta)
{
  int ret = OB_SUCCESS;
  ObObjTypeClass child_tc = column_meta.column_type_.get_type_class();
  const ObRawExprResType &dst_type = const_expr.get_result_type();
  ObObjTypeClass dst_tc = dst_type.get_type_class();
  ObAccuracy dst_acc = dst_type.get_accuracy();
  if ((ObFloatTC == child_tc || ObDoubleTC == child_tc) &&
      (ObFloatTC == dst_tc || ObDoubleTC == dst_tc)) {
    ObAccuracy lossless_acc = column_meta.column_type_.get_accuracy();
    if (!(child_tc == dst_tc &&
          dst_acc.get_scale() == lossless_acc.get_scale())) {
      ctx_.can_range_get_ = false;
    }
  }
  return ret;
}

int ObExprRangeConverter::set_extract_implicit_is_precise(const ObColumnRefRawExpr &column_expr,
                                                          const ObRawExpr &const_expr,
                                                          ObItemType cmp_type,
                                                          bool &is_precise)
{
  int ret = OB_SUCCESS;
  ObObjTypeClass column_tc = column_expr.get_result_type().get_type_class();
  ObObjTypeClass const_tc = const_expr.get_result_type().get_type_class();
  is_precise = true;
  if (lib::is_oracle_mode()) {
    // do nothing
  } else if (cmp_type == T_OP_EQ || cmp_type == T_OP_NSEQ) {
    if (ObNumberTC == column_tc &&
        (const_tc == ObDoubleTC || const_tc == ObFloatTC)) {
      is_precise = false;
    }
  } else {
    is_precise = false;
  }
  return ret;
}

int ObExprRangeConverter::convert_domain_expr(const ObRawExpr *domain_expr,
                                              int64_t expr_depth,
                                              ObRangeNode *&range_node)
{
  int ret = OB_SUCCESS;
  range_node = NULL;
  const ObRawExpr *expr = NULL;
  bool need_extract = false;
  common::ObDomainOpType op_type = ObDomainOpType::T_DOMAIN_OP_END;
  if (OB_ISNULL(domain_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FALSE_IT(expr = ObRawExprUtils::skip_inner_added_expr(domain_expr))) {
  } else if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(need_extract_domain_range(*static_cast<const ObOpRawExpr*>(expr),
                                               need_extract))) {
    LOG_WARN("failed to check need extract domain range", K(ret));
  } else if (!need_extract) {
    // do nothing
  } else {
    common::ObDomainOpType op_type = ObQueryRange::get_domain_op_type(expr->get_expr_type());
    const ObRawExpr *l_expr = expr->get_param_expr(0);
    const ObRawExpr *r_expr = expr->get_param_expr(1);
    const ObRawExpr *const_param = nullptr;
    const ObColumnRefRawExpr *column_param = nullptr;
    ObRangeColumnMeta *column_meta = nullptr;
    int64_t key_idx;
    bool always_true = true;
    if (OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(l_expr, l_expr))) {
      LOG_WARN("failed to get expr without lossless cast", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(r_expr, r_expr))) {
      LOG_WARN("failed to get expr without lossless cast", K(ret));
    } else if (OB_UNLIKELY(r_expr->has_flag(CNT_COLUMN))) {
      column_param = ObRawExprUtils::get_column_ref_expr_recursively(r_expr);
      const_param = l_expr;
    } else if (l_expr->has_flag(CNT_COLUMN)) {
      column_param = ObRawExprUtils::get_column_ref_expr_recursively(l_expr);
      const_param = r_expr;
    }

    if (OB_SUCC(ret) && OB_NOT_NULL(column_param) && OB_NOT_NULL(const_param)) {
      if (!is_range_key(column_param->get_column_id(), key_idx) ||
          OB_UNLIKELY(!const_param->is_const_expr())) {
        // do nothing
      } else if (OB_ISNULL(column_meta = get_column_meta(key_idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null column meta");
      } else if (!ObQueryRange::can_domain_be_extract_range(op_type, column_meta->column_type_,
                                                            column_param->get_result_type().get_obj_meta(),
                                                            const_param->get_result_type().get_type(),
                                                            always_true)) {
        // do nothing
      } else if (OB_FAIL(get_geo_range_node(column_param,
                                            op_type,
                                            const_param,
                                            NULL,
                                            range_node))) {
        LOG_WARN("failed to get geo range node", K(ret));
      } else {
        ctx_.cur_is_precise_ = op_type == ObDomainOpType::T_JSON_MEMBER_OF;
      }
    }
  }

  if (OB_SUCC(ret) && nullptr == range_node) {
    ctx_.cur_is_precise_ = false;
    if (OB_FAIL(generate_always_true_or_false_node(true, range_node))) {
      LOG_WARN("failed to generate always true or fasle node", K(ret));
    }
  }
  return ret;
}

int ObExprRangeConverter::need_extract_domain_range(const ObOpRawExpr &domain_expr,
                                                    bool& need_extract)
{
  int ret = OB_SUCCESS;
  need_extract = false;
  const ObRawExpr *l_expr = domain_expr.get_param_expr(0);
  const ObRawExpr *r_expr = domain_expr.get_param_expr(1);
  if (OB_ISNULL(l_expr) || OB_ISNULL(r_expr)) {
    need_extract = false;
  } else if (l_expr->has_flag(IS_COLUMN) && r_expr->has_flag(IS_COLUMN)) {
    need_extract = false;
  } else if (l_expr->has_flag(IS_DYNAMIC_PARAM) && r_expr->has_flag(IS_DYNAMIC_PARAM)) {
    need_extract = false;
  } else if (!l_expr->has_flag(CNT_COLUMN) && !r_expr->has_flag(CNT_COLUMN)) {
    need_extract = false;
  }else {
    need_extract = true;
  }
  return ret;
}

int ObExprRangeConverter::check_decimal_int_range_cmp_valid(const ObRawExpr *const_expr,
                                                            bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  if (OB_ISNULL(const_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null const expr", KP(const_expr));
  } else if (const_expr->has_flag(CNT_DYNAMIC_PARAM) || const_expr->has_flag(CNT_FAKE_CONST_UDF)) {
    // do nothing
  } else if (T_FUN_SYS_INNER_ROW_CMP_VALUE == const_expr->get_expr_type()) {
    ObObj const_val;
    bool obj_valid = false;
    if (OB_FAIL(get_calculable_expr_val(const_expr, const_val, obj_valid))) {
      LOG_WARN("failed to get calculable expr val", K(ret));
    } else if (obj_valid && (const_val.is_min_value() || const_val.is_max_value())) {
      // if const val is min/max value, it means the previous expr value range is expanding,
      // use origin cmp type to calc row range.
      is_valid = false;
    }
  }
  return ret;
}

int ObExprRangeConverter::ignore_inner_generate_expr(const ObRawExpr *const_expr, bool &can_ignore)
{
  int ret = OB_SUCCESS;
  can_ignore = false;
  if (const_expr->get_expr_type() == T_FUN_SYS_INNER_DOUBLE_TO_INT ||
      const_expr->get_expr_type() == T_FUN_SYS_INNER_DECIMAL_TO_YEAR) {
    can_ignore = true;
  }
  return ret;
}

int ObExprRangeConverter::get_implicit_set_collation_range(const ObRawExpr &l_expr,
                                                           const ObRawExpr &r_expr,
                                                           ObItemType cmp_type,
                                                           int64_t expr_depth,
                                                           ObRangeNode *&range_node)
{
  int ret = OB_SUCCESS;
  const ObRawExpr *collation_expr = nullptr;
  const ObRawExpr *const_expr = nullptr;
  const ObRawExpr *inner_expr = nullptr;
  bool can_extract = false;
  range_node = nullptr;
  if (l_expr.get_expr_type() == T_FUN_SYS_SET_COLLATION && r_expr.is_const_expr()) {
    collation_expr = &l_expr;
    const_expr = &r_expr;
  } else if (r_expr.get_expr_type() == T_FUN_SYS_SET_COLLATION && l_expr.is_const_expr()) {
    collation_expr = &r_expr;
    const_expr = &l_expr;
    cmp_type = get_opposite_compare_type(cmp_type);
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(collation_expr) &&
      OB_NOT_NULL(const_expr)) {
    if (OB_FAIL(check_can_extract_implicit_collation_range(cmp_type,
                                                           collation_expr,
                                                           inner_expr,
                                                           can_extract))) {
      LOG_WARN("failed to check can extract implicit collation range", K(ret));
    } else if (!can_extract) {
      // do nothing
    } else if (OB_FAIL(get_basic_range_node(inner_expr,
                                            const_expr,
                                            cmp_type,
                                            expr_depth + 1,
                                            range_node))) {
      LOG_WARN("failed to get basic range node", K(ret));
    } else {
      ctx_.cur_is_precise_ = false;
    }
  }

  if (OB_SUCC(ret) && nullptr == range_node) {
    ctx_.cur_is_precise_ = false;
    if (OB_FAIL(generate_always_true_or_false_node(true, range_node))) {
      LOG_WARN("failed to generate always true or fasle node");
    }
  }
  return ret;
}

int ObExprRangeConverter::check_can_extract_implicit_collation_range(
                          ObItemType cmp_type,
                          const ObRawExpr *l_expr,
                          const ObRawExpr *&real_expr,
                          bool &can_extract)
{
  int ret = OB_SUCCESS;
  const ObRawExpr *collation_expr = nullptr;
  ObObj collation_val;
  bool is_valid = false;
  int64_t dest_collation = 0;
  real_expr = nullptr;
  can_extract = false;
  if (OB_ISNULL(l_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (l_expr->get_expr_type() == T_FUN_SYS_SET_COLLATION) {
    if (OB_UNLIKELY(l_expr->get_param_count() != 2) ||
              OB_ISNULL(real_expr = l_expr->get_param_expr(0)) ||
              OB_ISNULL(collation_expr = l_expr->get_param_expr(1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), KPC(l_expr));
    } else if (!collation_expr->is_const_expr()) {
      // do nothing
    } else if (OB_FAIL(get_calculable_expr_val(collation_expr,
                                              collation_val,
                                              is_valid))) {
      LOG_WARN("failed to get calculable expr");
    } else if (!is_valid) {
      // do nothing
    } else if (OB_FAIL(collation_val.get_int(dest_collation))) {
      LOG_WARN("failed to get collation val", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::is_implicit_collation_range_valid(
                        cmp_type,
                        real_expr->get_result_type().get_obj_meta().get_collation_type(),
                        static_cast<ObCollationType>(dest_collation),
                        can_extract))) {
      LOG_WARN("failed to chacke can extract implicit collation range", K(ret));
    }
  } else if (l_expr->get_expr_type() == T_FUN_SYS_CAST) {
    if (OB_ISNULL(real_expr = l_expr->get_param_expr(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), KPC(l_expr));
    } else if (real_expr->get_result_type().get_type() !=
                 l_expr->get_result_type().get_type() ||
               !real_expr->get_result_type().is_string_type()) {
      // do nothing
    } else if (OB_FAIL(ObOptimizerUtil::is_implicit_collation_range_valid(
                        cmp_type,
                        real_expr->get_result_type().get_obj_meta().get_collation_type(),
                        l_expr->get_result_type().get_obj_meta().get_collation_type(),
                        can_extract))) {
      LOG_WARN("failed to chacke can extract implicit collation range", K(ret));
    }
  }
  return ret;
}

int ObExprRangeConverter::get_implicit_set_collation_in_range(
                          const ObRawExpr *l_expr,
                          const ObRawExpr *r_expr,
                          const ObExprResType &result_type,
                          int64_t expr_depth,
                          ObRangeNode *&range_node)
{
  int ret = OB_SUCCESS;
  const ObRawExpr *inner_expr = nullptr;
  bool can_extract = false;
  bool use_implicit_cast_feature = true;
  if (OB_FAIL(check_can_extract_implicit_collation_range(T_OP_IN,
                                                         l_expr,
                                                         inner_expr,
                                                         can_extract))) {
    LOG_WARN("failed to check can extract implicit collation range", K(ret));
  } else if (!can_extract) {
    // do nothing
  } else if (OB_ISNULL(inner_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(inner_expr,
                                                                     inner_expr,
                                                                     use_implicit_cast_feature))) {
    LOG_WARN("failed to get expr without lossless cast", K(ret));
  } else if (!inner_expr->is_column_ref_expr()) {
    // do nothing
  } else if (OB_FAIL(get_single_in_range_node(static_cast<const ObColumnRefRawExpr *>(inner_expr),
                                              r_expr,
                                              result_type,
                                              expr_depth + 1,
                                              range_node))) {
    LOG_WARN("failed to get single row in range node", K(ret));
  } else {
    ctx_.cur_is_precise_ = false;
  }
  return ret;
}

int ObExprRangeConverter::can_be_extract_orcl_spatial_range(
                          const ObRawExpr *const_expr,
                          bool &can_extract)
{
  int ret = OB_SUCCESS;
  ObObj const_val;
  bool is_valid = false;
  can_extract = false;
  if (OB_ISNULL(const_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(get_calculable_expr_val(const_expr, const_val, is_valid))) {
    LOG_WARN("failed to get calculable expr val", K(ret));
  } else if (!is_valid || !ob_is_string_type(const_val.get_type())) {
    can_extract = false;
  } else {
    ObString str = const_val.get_string();
    can_extract = (str.compare("TRUE") == 0);
    if (can_extract) {
      if (OB_FAIL(add_string_equal_expr_constraint(const_expr, str))) {
        LOG_WARN("failed to add string equal expr constraint", K(ret));
      }
    }
  }
  return ret;
}

int ObExprRangeConverter::get_orcl_spatial_relationship(const ObRawExpr *const_expr,
                                                        bool &can_extract,
                                                        ObDomainOpType& real_op_type)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator temp_allocator(lib::ObLabel("GisIndex"));
  ObObj const_val;
  bool is_valid = false;
  char *cmp_str = NULL;
  ObString upper_str;
  can_extract = false;
  if (OB_ISNULL(const_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (!const_expr->is_const_expr()) {
    // do nothing
  } else if (OB_FAIL(get_calculable_expr_val(const_expr, const_val, is_valid))) {
    LOG_WARN("failed to get calculable expr val", K(ret));
  } else if (!is_valid || !ob_is_string_type(const_val.get_type())) {
    // do nothing
  } else if (OB_FAIL(ObCharset::toupper(const_expr->get_collation_type(),
                                        const_val.get_string(),
                                        upper_str,
                                        temp_allocator))) {
    LOG_WARN("failed to get upper string", K(ret));
  } else if (OB_ISNULL(cmp_str = static_cast<char*>(temp_allocator.alloc(upper_str.length() + 1)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else {
    cmp_str[upper_str.length()] = '\0';
    MEMCPY(cmp_str, upper_str.ptr(), upper_str.length());
    if (nullptr != strstr(cmp_str, "ANYINTERACT")) {
      real_op_type = ObDomainOpType::T_GEO_INTERSECTS;
      can_extract = true;
      if (OB_FAIL(add_string_equal_expr_constraint(const_expr, const_val.get_string()))) {
        LOG_WARN("failed to add string equal expr constraint", K(ret));
      }
    } else if (nullptr != strstr(cmp_str, "CONTAINS")) {
      // Support CONTAINS for spatial index optimization
      real_op_type = ObDomainOpType::T_GEO_COVERS;
      can_extract = true;
      if (OB_FAIL(add_string_equal_expr_constraint(const_expr, const_val.get_string()))) {
        LOG_WARN("failed to add string equal expr constraint", K(ret));
      }
    } else {
      // other spatial relationsh is not supported yet, no need to continue
      real_op_type = ObDomainOpType::T_DOMAIN_OP_END;
      can_extract = false;
    }
  }
  return ret;
}

int ObExprRangeConverter::get_orcl_spatial_range_node(const ObRawExpr &l_expr,
                                                      const ObRawExpr &r_expr,
                                                      int64_t expr_depth,
                                                      ObRangeNode *&range_node)
{
  int ret = OB_SUCCESS;
  const ObRawExpr *inner_expr = nullptr;
  const ObRawExpr *const_expr = nullptr;
  bool can_extract = false;
  if (l_expr.get_expr_type() == T_FUN_SYS_SDO_RELATE && r_expr.is_const_expr()) {
    inner_expr = &l_expr;
    const_expr = &r_expr;
  } else if (r_expr.get_expr_type() == T_FUN_SYS_SDO_RELATE && l_expr.is_const_expr()) {
    inner_expr = &r_expr;
    const_expr = &l_expr;
  }

  if (OB_FAIL(can_be_extract_orcl_spatial_range(const_expr, can_extract))) {
    LOG_WARN("failed to check can be extract orcl spatial range", K(ret));
  } else if (!can_extract) {
    // do nothing
  } else if (OB_FAIL(convert_geo_expr(inner_expr, expr_depth, range_node))) {
    LOG_WARN("failed to convert geo expr", K(ret));
  }

  if (OB_SUCC(ret) && nullptr == range_node) {
    ctx_.cur_is_precise_ = false;
    if (OB_FAIL(generate_always_true_or_false_node(true, range_node))) {
      LOG_WARN("failed to generate always true or fasle node", K(ret));
    }
  }
  return ret;
}

int ObExprRangeConverter::add_string_equal_expr_constraint(const ObRawExpr *const_expr,
                                                           const ObString &val)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr *val_expr = NULL;
  ObRawExpr *out_expr = NULL;
  PreCalcExprExpectResult expect_result = PreCalcExprExpectResult::PRE_CALC_RESULT_TRUE;
  if (OB_ISNULL(const_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx_.expr_constraints_));
  } else if (OB_ISNULL(ctx_.constraints_expr_factory_) ||
             OB_ISNULL(ctx_.expr_constraints_)) {
    // do nothing
  } else if (OB_FALSE_IT(const_expr = ObRawExprUtils::skip_inner_added_expr(const_expr))) {
  } else if (OB_FAIL(ObRawExprUtils::build_const_string_expr(*ctx_.constraints_expr_factory_,
                                                             const_expr->get_result_meta().get_type(),
                                                             val,
                                                             const_expr->get_result_meta().get_collation_type(),
                                                             val_expr))) {
    LOG_WARN("fail to build type expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::create_equal_expr(*ctx_.constraints_expr_factory_,
                                                       ctx_.session_info_,
                                                       const_expr,
                                                       val_expr,
                                                       out_expr))) {
    LOG_WARN("failed to create equal expr", K(ret));
  } else if (OB_FAIL(ctx_.expr_constraints_->push_back(ObExprConstraint(out_expr,
                                                                        expect_result)))) {
    LOG_WARN("failed to push back expr constraints", K(ret));
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
