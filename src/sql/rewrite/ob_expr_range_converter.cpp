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


namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace sql
{
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
    if(OB_FAIL(convert_like_expr(expr, range_node))) {
      LOG_WARN("failed to convert like expr");
    }
  } else if (IS_BASIC_CMP_OP(expr->get_expr_type())) {
    if (OB_FAIL(convert_basic_cmp_expr(expr, range_node))) {
      LOG_WARN("failed to convert basic cmp expr");
    }
  } else if (0 == expr_depth && T_OP_NE == expr->get_expr_type()) {
    if (OB_FAIL(convert_not_equal_expr(expr, range_node))) {
      LOG_WARN("failed to convert not equal expr");
    }
  } else if (T_OP_IS == expr->get_expr_type()) {
    if (OB_FAIL(convert_is_expr(expr, range_node))) {
      LOG_WARN("failed to convert is expr");
    }
  } else if (T_OP_BTW == expr->get_expr_type()) {
    if (OB_FAIL(convert_between_expr(expr, range_node))) {
      LOG_WARN("failed to convert between expr");
    }
  } else if (0 == expr_depth && T_OP_NOT_BTW == expr->get_expr_type()) {
    if (OB_FAIL(convert_not_between_expr(expr, range_node))) {
      LOG_WARN("failed to convert not between expr");
    }
  } else if (T_OP_IN  == expr->get_expr_type()) {
    if (OB_FAIL(convert_in_expr(expr, range_node))) {
      LOG_WARN("failed to convert in expr");
    }
  } else if (0 == expr_depth && T_OP_NOT_IN  == expr->get_expr_type()) {
    if (OB_FAIL(convert_not_in_expr(expr, range_node))) {
      LOG_WARN("failed to convert not in expr");
    }
  // } else if (expr->is_spatial_expr()) {
  //   if (OB_FAIL(pre_extract_geo_op(b_expr, out_key_part))) {
  //     LOG_WARN("extract and_or failed", K(ret));
  //   }
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
  if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObRangeNode))) ||
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
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null expr", K(expr));
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
  } else if (OB_FAIL(start_expr->add_param_expr(expr))) {
    LOG_WARN("failed to set param for is true", KPC(expr));
  } else if (OB_FAIL(start_expr->add_param_expr(pos_expr))) {
    LOG_WARN("failed to set param for is true", KPC(pos_expr));
  } else if (OB_FAIL(start_expr->formalize(ctx_.session_info_))) {
    LOG_WARN("failed to formalize expr");
  } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*ctx_.expr_factory_,
                                                          ObIntType,
                                                          0, // const value
                                                          pos_expr))) {
    LOG_WARN("Failed to build const expr", K(ret));
  } else if (OB_FAIL(end_expr->add_param_expr(expr))) {
    LOG_WARN("failed to set param for is true", KPC(expr));
  } else if (OB_FAIL(end_expr->add_param_expr(pos_expr))) {
    LOG_WARN("failed to set param for is true", KPC(pos_expr));
  } else if (OB_FAIL(end_expr->formalize(ctx_.session_info_))) {
    LOG_WARN("failed to formalize expr");
  } else if (OB_FAIL(get_final_expr_idx(start_expr, start_val))) {
    LOG_WARN("failed to get final expr idx");
  } else if (OB_FAIL(get_final_expr_idx(end_expr, end_val))) {
    LOG_WARN("failed to get final expr idx");
  }
  return ret;
}


int ObExprRangeConverter::convert_basic_cmp_expr(const ObRawExpr *expr,
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
                                      expr->get_result_type(),
                                      range_node))) {
      LOG_WARN("failed to get basic range node");
    }
  }
  return ret;
}

int ObExprRangeConverter::get_basic_range_node(const ObRawExpr *l_expr,
                                               const ObRawExpr *r_expr,
                                               ObItemType cmp_type,
                                               const ObExprResType &result_type,
                                               ObRangeNode *&range_node)
{
  int ret = OB_SUCCESS;
  range_node = NULL;
  ctx_.cur_is_precise_ = false;
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
    if (OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(l_expr, l_expr))) {
      LOG_WARN("failed to get expr without lossless cast", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(r_expr, r_expr))) {
      LOG_WARN("failed to get expr without lossless cast", K(ret));
    } else if ((l_expr->has_flag(IS_COLUMN) && r_expr->is_const_expr()) ||
               (l_expr->is_const_expr() && r_expr->has_flag(IS_COLUMN))) {
      if (OB_FAIL(gen_column_cmp_node(*l_expr, *r_expr, cmp_type, result_type, range_node))) {
        LOG_WARN("get column key part failed.", K(ret));
      }
    } else if ((l_expr->has_flag(IS_ROWID) && r_expr->is_const_expr()) ||
               (r_expr->has_flag(IS_ROWID) && l_expr->is_const_expr())) {
      if (OB_FAIL(get_rowid_node(*l_expr, *r_expr, cmp_type, range_node))) {
        LOG_WARN("get rowid key part failed.", K(ret));
      }
    } else {
      if (OB_FAIL(generate_always_true_or_false_node(true, range_node))) {
        LOG_WARN("failed to generate always true node");
      }
    }
  } else if (OB_FAIL(gen_row_column_cmp_node(*l_expr, *r_expr, cmp_type,
                                             result_type, range_node))) {
    LOG_WARN("get row key part failed.", K(ret));
  }

  return ret;
}

int ObExprRangeConverter::gen_column_cmp_node(const ObRawExpr &l_expr,
                                              const ObRawExpr &r_expr,
                                              ObItemType cmp_type,
                                              const ObExprResType &result_type,
                                              ObRangeNode *&range_node)
{
  int ret = OB_SUCCESS;
  range_node = NULL;
  ctx_.cur_is_precise_ = true;
  const ObColumnRefRawExpr *column_expr = NULL;
  const ObRawExpr *const_expr = NULL;
  const ObExprCalcType &calc_type = result_type.get_calc_meta();
  ObRangeColumnMeta *column_meta = nullptr;
  int64_t key_idx;
  int64_t const_val;
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
  } else if (OB_ISNULL(column_meta = get_column_meta(key_idx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null column meta");
  } else if (!ObQueryRange::can_be_extract_range(cmp_type, column_meta->column_type_,
                                                 calc_type, const_expr->get_result_type().get_type(),
                                                 always_true)) {
    // do nothing
  } else if (OB_FAIL(get_final_expr_idx(const_expr, const_val))) {
    LOG_WARN("failed to get final expr idx");
  } else if (OB_FAIL(alloc_range_node(range_node))) {
    LOG_WARN("failed to alloc common range node");
  } else {
    if (T_OP_NSEQ == cmp_type && OB_FAIL(ctx_.null_safe_value_idxs_.push_back(const_val))) {
      LOG_WARN("failed to push back null safe value index", K(const_val));
    //if current expr can be extracted to range, just store the expr
    } else if (OB_FAIL(fill_range_node_for_basic_cmp(cmp_type, key_idx, const_val, *range_node))) {
      LOG_WARN("get normal cmp keypart failed", K(ret));
    } else if (OB_FAIL(check_expr_precise(*const_expr, calc_type, column_meta->column_type_))) {
      LOG_WARN("failed to check expr precise", K(ret));
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

int ObExprRangeConverter::gen_row_column_cmp_node(const ObRawExpr &l_expr,
                                                  const ObRawExpr &r_expr,
                                                  ObItemType cmp_type,
                                                  const ObExprResType &result_type,
                                                  ObRangeNode *&range_node)
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 4> key_idxs;
  ObSEArray<int64_t, 4> val_idxs;
  bool always_true = true;
  ctx_.cur_is_precise_ = true;
  if(OB_UNLIKELY(l_expr.get_param_count() != r_expr.get_param_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected param count", K(l_expr), K(r_expr));
  } else if (T_OP_EQ == cmp_type || T_OP_NSEQ == cmp_type) {
    for (int64_t i = 0; OB_SUCC(ret) && i < l_expr.get_param_count(); ++i) {
      const ObRawExpr* l_param = l_expr.get_param_expr(i);
      const ObRawExpr* r_param = r_expr.get_param_expr(i);
      const ObExprCalcType &calc_type = result_type.get_row_calc_cmp_types().at(i);
      const ObColumnRefRawExpr* column_expr = nullptr;
      const ObRawExpr* const_expr = nullptr;
      bool cur_always_true = true;
      if (OB_ISNULL(l_param) || OB_ISNULL(r_param)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null expr", K(l_param), K(r_param));
      } else if (OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(l_param, l_param))) {
        LOG_WARN("failed to get expr without lossless cast", K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(r_param, r_param))) {
        LOG_WARN("failed to get expr without lossless cast", K(ret));
      } else if (l_param->has_flag(IS_COLUMN) && r_param->is_const_expr()) {
        column_expr = static_cast<const ObColumnRefRawExpr*>(l_param);
        const_expr = r_param;
      } else if (r_param->has_flag(IS_COLUMN) && l_param->is_const_expr()) {
        column_expr = static_cast<const ObColumnRefRawExpr*>(r_param);
        const_expr = l_param;
      }
      if (OB_SUCC(ret) && OB_NOT_NULL(column_expr) && OB_NOT_NULL(const_expr)) {
        int64_t key_idx = -1;
        int64_t const_val = -1;
        ObRangeColumnMeta *column_meta = nullptr;
        if (!is_range_key(column_expr->get_column_id(), key_idx)) {
          // do nothing
        } else if (ObOptimizerUtil::find_item(key_idxs, key_idx)) {
          // this key already exist. e.g. (c1,c1,c2) = (:1,:2,:3) will be trated as (c1,c2) = (:1,:3)
        } else if (OB_ISNULL(column_meta = get_column_meta(key_idx))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get null column meta");
        } else if (!ObQueryRange::can_be_extract_range(cmp_type, column_meta->column_type_,
                                                       calc_type, const_expr->get_result_type().get_type(),
                                                       cur_always_true)) {
          if (!cur_always_true) {
            always_true = false;
          }
        } else if (OB_FAIL(check_expr_precise(*const_expr, calc_type, column_meta->column_type_))) {
          LOG_WARN("failed to check expr precise", K(ret));
        } else if (OB_FAIL(get_final_expr_idx(const_expr, const_val))) {
          LOG_WARN("failed to get final expr idx");
        } else if (OB_FAIL(key_idxs.push_back(key_idx))) {
          LOG_WARN("failed to push back key idx", K(key_idx));
        } else if (OB_FAIL(val_idxs.push_back(const_val))) {
          LOG_WARN("failed to push back key idx", K(const_val));
        } else if (T_OP_NSEQ == cmp_type && OB_FAIL(ctx_.null_safe_value_idxs_.push_back(const_val))) {
          LOG_WARN("failed to push back null safe value index", K(const_val));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (key_idxs.count() < l_expr.get_param_count()) {
        // part of row can't extract range.
        ctx_.cur_is_precise_ = false;
      }
      if (!key_idxs.empty()) {
        if (OB_FAIL(alloc_range_node(range_node))) {
          LOG_WARN("failed to alloc common range node");
        } else if (OB_FAIL(fill_range_node_for_basic_row_cmp(cmp_type, key_idxs, val_idxs, *range_node))) {
          LOG_WARN("failed to fill range node for basic row cmp");
        }
      }
    }
  } else {
    bool can_reverse = true;
    bool is_reverse = false;
    int64_t last_key_idx = -1;
    bool check_next = true;
    for (int64_t i = 0; OB_SUCC(ret) && check_next && i < l_expr.get_param_count(); ++i) {
      const ObRawExpr* l_param = l_expr.get_param_expr(i);
      const ObRawExpr* r_param = r_expr.get_param_expr(i);
      const ObExprCalcType &calc_type = result_type.get_row_calc_cmp_types().at(i);
      const ObColumnRefRawExpr* column_expr = nullptr;
      const ObRawExpr* const_expr = nullptr;
      bool cur_always_true = true;
      check_next = false;
      if (OB_ISNULL(l_param) || OB_ISNULL(r_param)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null expr", K(l_param), K(r_param));
      } else if (OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(l_param, l_param))) {
        LOG_WARN("failed to get expr without lossless cast", K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(r_param, r_param))) {
        LOG_WARN("failed to get expr without lossless cast", K(ret));
      } else if (l_param->has_flag(IS_COLUMN) && r_param->is_const_expr()) {
        if (can_reverse) {
          can_reverse = false;
          column_expr = static_cast<const ObColumnRefRawExpr*>(l_param);
          const_expr = r_param;
        } else if (!is_reverse) {
          column_expr = static_cast<const ObColumnRefRawExpr*>(l_param);
          const_expr = r_param;
        }
      } else if (r_param->has_flag(IS_COLUMN) && l_param->is_const_expr()) {
        if (can_reverse) {
          can_reverse = false;
          is_reverse = true;
          column_expr = static_cast<const ObColumnRefRawExpr*>(r_param);
          const_expr = l_param;
          cmp_type = get_opposite_compare_type(cmp_type);
        } else if (is_reverse) {
          column_expr = static_cast<const ObColumnRefRawExpr*>(r_param);
          const_expr = l_param;
        }
      }
      if (OB_SUCC(ret) && OB_NOT_NULL(column_expr) && OB_NOT_NULL(const_expr)) {
        int64_t key_idx = -1;
        int64_t const_val = -1;
        ObRangeColumnMeta *column_meta = nullptr;
        if (!is_range_key(column_expr->get_column_id(), key_idx)) {
          // do nothing
        } else if (key_idx != last_key_idx + 1) {
          // do nothing
        } else if (OB_ISNULL(column_meta = get_column_meta(key_idx))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get null column meta");
        } else if (!ObQueryRange::can_be_extract_range(cmp_type, column_meta->column_type_,
                                                       calc_type, const_expr->get_result_type().get_type(),
                                                       cur_always_true)) {
          if (i == 0 && !cur_always_true) {
            always_true = false;
          }
        } else if (OB_FAIL(check_expr_precise(*const_expr, calc_type, column_meta->column_type_))) {
          LOG_WARN("failed to check expr precise", K(ret));
        } else if (OB_FAIL(get_final_expr_idx(const_expr, const_val))) {
          LOG_WARN("failed to get final expr idx");
        } else if (OB_FAIL(key_idxs.push_back(key_idx))) {
          LOG_WARN("failed to push back key idx", K(key_idx));
        } else if (OB_FAIL(val_idxs.push_back(const_val))) {
          LOG_WARN("failed to push back key idx", K(const_val));
        } else {
          last_key_idx = key_idx;
          check_next = true;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (key_idxs.count() < l_expr.get_param_count()) {
        ctx_.cur_is_precise_ = false;
        if (T_OP_LT == cmp_type) {
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
int ObExprRangeConverter::convert_is_expr(const ObRawExpr *expr, ObRangeNode *&range_node)
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
    int64_t key_idx = -1;
    if (OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(l_expr, l_expr))) {
      LOG_WARN("failed to get expr without lossless cast", K(ret));
    } else if (!l_expr->has_flag(IS_COLUMN)) {
      // do nothing
    } else if (!is_range_key(static_cast<const ObColumnRefRawExpr*>(l_expr)->get_column_id(), key_idx)) {
      // do nothing
    } else if (OB_FAIL(alloc_range_node(range_node))) {
      LOG_WARN("failed to alloc common range node");
    } else if (OB_FAIL(fill_range_node_for_basic_cmp(T_OP_NSEQ, key_idx, OB_RANGE_NULL_VALUE, *range_node))) {
      LOG_WARN("get normal cmp keypart failed", K(ret));
    } else {
      ctx_.cur_is_precise_ = true;
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
 * convert `c1 between :0 and :1` to range node
*/
int ObExprRangeConverter::convert_between_expr(const ObRawExpr *expr, ObRangeNode *&range_node)
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
  } else if (OB_FAIL(get_basic_range_node(expr1, expr2, T_OP_GE, expr->get_result_type(), tmp_node))) {
    LOG_WARN("failed tp get basic range node", K(ret));
  } else if (OB_FAIL(range_nodes.push_back(tmp_node))) {
    LOG_WARN("failed to push back range node");
  } else if (OB_FALSE_IT(first_is_precise = ctx_.cur_is_precise_)) {
  } else if (OB_FAIL(get_basic_range_node(expr1, expr3, T_OP_LE, expr->get_result_type(), tmp_node))) {
    LOG_WARN("failed tp get basic range node", K(ret));
  } else if (OB_FAIL(range_nodes.push_back(tmp_node))) {
    LOG_WARN("failed to push back range node");
  } else if (OB_FAIL(ObRangeGraphGenerator::and_range_nodes(range_nodes, ctx_.column_cnt_, range_node))) {
    LOG_WARN("failed to and range nodes");
  } else {
    ctx_.cur_is_precise_ = first_is_precise && ctx_.cur_is_precise_;
  }
  return ret;
}

int ObExprRangeConverter::convert_not_between_expr(const ObRawExpr *expr, ObRangeNode *&range_node)
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
  } else if (OB_FAIL(get_basic_range_node(expr1, expr2, T_OP_LT, expr->get_result_type(), tmp_node))) {
    LOG_WARN("failed tp get basic range node", K(ret));
  } else if (OB_FAIL(range_nodes.push_back(tmp_node))) {
    LOG_WARN("failed to push back range node");
  } else if (OB_FALSE_IT(first_is_precise = ctx_.cur_is_precise_)) {
  } else if (OB_FAIL(get_basic_range_node(expr1, expr3, T_OP_GT, expr->get_result_type(), tmp_node))) {
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

int ObExprRangeConverter::convert_not_equal_expr(const ObRawExpr *expr, ObRangeNode *&range_node)
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
  } else if (OB_FAIL(get_basic_range_node(l_expr, r_expr, T_OP_LT, expr->get_result_type(), tmp_node))) {
    LOG_WARN("failed tp get basic range node", K(ret));
  } else if (OB_FAIL(range_nodes.push_back(tmp_node))) {
    LOG_WARN("failed to push back range node");
  } else if (OB_FALSE_IT(first_is_precise = ctx_.cur_is_precise_)) {
  } else if (OB_FAIL(get_basic_range_node(l_expr, r_expr, T_OP_GT, expr->get_result_type(), tmp_node))) {
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

int ObExprRangeConverter::convert_like_expr(const ObRawExpr *expr, ObRangeNode *&range_node)
{
  int ret = OB_SUCCESS;
  const ObRawExpr *l_expr = nullptr;
  const ObRawExpr *pattern_expr = nullptr;
  const ObRawExpr *escape_expr = nullptr;
  ObRangeNode *tmp_node = nullptr;
  bool always_true = true;
  ctx_.cur_is_precise_ = false;
  if (OB_ISNULL(expr) || OB_UNLIKELY(expr->get_param_count() != 3) ||
      OB_ISNULL(l_expr = expr->get_param_expr(0)) ||
      OB_ISNULL(pattern_expr = expr->get_param_expr(1)) ||
      OB_ISNULL(escape_expr = expr->get_param_expr(2))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected expr", KPC(expr), KPC(l_expr), KPC(pattern_expr), KPC(escape_expr));
  } else if (OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(l_expr, l_expr))) {
    LOG_WARN("failed to get expr without lossless cast", K(ret));
  } else if (l_expr->has_flag(IS_COLUMN) &&
             pattern_expr->is_const_expr() &&
             escape_expr->is_const_expr() &&
             !escape_expr->has_flag(CNT_DYNAMIC_PARAM)) {
    const ObColumnRefRawExpr *column_expr = static_cast<const ObColumnRefRawExpr *>(l_expr);
    const ObExprCalcType &calc_type = expr->get_result_type().get_calc_meta();
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
                                                   calc_type, pattern_expr->get_result_type().get_type(),
                                                   always_true)) {
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
    } else if (OB_FAIL(check_expr_precise(*pattern_expr, calc_type, column_meta->column_type_))) {
      LOG_WARN("failed to check expr precise", K(ret));
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
  } else if (OB_FAIL(get_final_expr_idx(start_expr, start_val_idx))) {
    LOG_WARN("failed to get final expr idx");
  } else if (OB_FAIL(get_final_expr_idx(end_expr, end_val_idx))) {
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
int ObExprRangeConverter::convert_in_expr(const ObRawExpr *expr, ObRangeNode *&range_node)
{
  int ret = OB_SUCCESS;
  const ObRawExpr* l_expr = nullptr;
  const ObRawExpr* r_expr = nullptr;
  ctx_.cur_is_precise_ = true;
  if (OB_ISNULL(expr) ||
      OB_ISNULL(l_expr = expr->get_param_expr(0)) ||
      OB_ISNULL(r_expr = expr->get_param_expr(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null expr", K(expr), K(l_expr), K(r_expr));
  } else if (OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(l_expr, l_expr))) {
    LOG_WARN("failed to get expr without lossless cast", K(ret));
  } else if (l_expr->get_expr_type() == T_OP_ROW) {
    if (OB_FAIL(get_row_in_range_ndoe(*l_expr, *r_expr, expr->get_result_type(), range_node))) {
      LOG_WARN("failed to get row in range node");
    }
  } else if (l_expr->is_column_ref_expr()) {
    if (OB_FAIL(get_single_in_range_node(static_cast<const ObColumnRefRawExpr *>(l_expr),
                                         r_expr, expr->get_result_type(), range_node))) {
      LOG_WARN("failed to get single in range node");
    }
  } else if (l_expr->has_flag(IS_ROWID)) {
    if (OB_FAIL(get_single_row_in_range_node(*l_expr, *r_expr, range_node))) {
      LOG_WARN("failed to get single row in range node", K(ret));
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
                                                   const ObExprResType &res_type,
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
  } else if (OB_ISNULL(column_meta = get_column_meta(key_idx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null column meta");
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !always_true && i < r_expr->get_param_count(); ++i) {
      const ObRawExpr *const_expr = r_expr->get_param_expr(i);
      bool cur_can_be_extract = true;
      bool cur_always_true = true;
      bool is_val_valid = true;
      int64_t val_idx = -1;
      if (OB_ISNULL(const_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null expr");
      } else if (OB_UNLIKELY(!const_expr->is_const_expr())) {
        cur_can_be_extract = false;
        cur_always_true = true;
      } else if (!ObQueryRange::can_be_extract_range(T_OP_EQ, column_meta->column_type_,
                                                     res_type.get_row_calc_cmp_types().at(i),
                                                     const_expr->get_result_type().get_type(),
                                                     cur_always_true)) {
        cur_can_be_extract = false;
      } else if (OB_FAIL(get_final_expr_idx(const_expr, val_idx))) {
        LOG_WARN("failed to get final expr idx", K(ret));
      } else if (OB_FAIL(check_expr_precise(*const_expr, res_type.get_row_calc_cmp_types().at(i),
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
    } else {
      range_node->contain_in_ = true;
      range_node->in_param_count_ = val_idxs.count();
    }
  }
  return ret;
}

int ObExprRangeConverter::get_row_in_range_ndoe(const ObRawExpr &l_expr,
                                                const ObRawExpr &r_expr,
                                                const ObExprResType &res_type,
                                                ObRangeNode *&range_node)
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 4> key_idxs;
  ObSEArray<int64_t, 4> val_idxs;
  ObSEArray<int64_t, 4> key_offsets;
  ObSEArray<ObRangeColumnMeta*, 4> column_metas;
  bool always_true_or_false = true;
  // 1. get all valid key and offset
  for (int64_t i = 0; OB_SUCC(ret) && i < l_expr.get_param_count(); ++i) {
    const ObRawExpr* l_param = l_expr.get_param_expr(i);
    const ObColumnRefRawExpr* column_expr = nullptr;
    ObRangeColumnMeta *column_meta = nullptr;
    if (OB_ISNULL(l_param)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null expr", K(l_param));
    } else if (OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(l_param, l_param))) {
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
      } else if (OB_FAIL(key_idxs.push_back(key_idx))) {
        LOG_WARN("failed to push back key idx", K(key_idx));
      } else if (OB_FAIL(key_offsets.push_back(i))) {
        LOG_WARN("failed to add member to bitmap", K(i));
      } else if (OB_FAIL(column_metas.push_back(column_meta))) {
        LOG_WARN("failed to push back column meta");
      }
    }
  }

  // 2. get all valid in param
  if (OB_SUCC(ret) && key_idxs.count() > 0) {
    ObArenaAllocator alloc("ExprRangeAlloc", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    ObSEArray<const ObRawExpr*, 4> cur_val_exprs;
    ObSEArray<TmpExprArray*, 4> all_val_exprs;
    const int64_t row_dimension = l_expr.get_param_count();
    int64_t in_param_count = 0;

    for (int64_t i = 0; i < key_idxs.count(); ++i) {
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
      bool need_add = true;
      cur_val_exprs.reuse();
      if (OB_ISNULL(row_expr) || OB_UNLIKELY(row_expr->get_expr_type() != T_OP_ROW)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected expr", KPC(row_expr));
      }
      for (int64_t j = key_offsets.count() - 1; OB_SUCC(ret)&& need_add && j >= 0; --j) {
        int64_t val_offset = key_offsets.at(j);
        ObRangeColumnMeta* column_meta = column_metas.at(j);
        bool cur_can_be_extract = true;
        bool cur_always_true = true;
        const ObExprCalcType &calc_type = res_type.get_row_calc_cmp_types().at(row_dimension * i + val_offset);
        if (OB_UNLIKELY(val_offset >= row_expr->get_param_count()) ||
            OB_ISNULL(const_expr = row_expr->get_param_expr(val_offset))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected expr", K(val_offset), KPC(const_expr));
        } else if (OB_UNLIKELY(!const_expr->is_const_expr())) {
          cur_can_be_extract = false;
          cur_always_true = true;
        } else if (!ObQueryRange::can_be_extract_range(T_OP_EQ,
                                                       column_meta->column_type_,
                                                       calc_type,
                                                       const_expr->get_result_type().get_type(),
                                                       cur_always_true)) {
          cur_can_be_extract = false;
        } else if (OB_FAIL(OB_FAIL(cur_val_exprs.push_back(const_expr)))) {
          LOG_WARN("failed to push back expr");
        } else if (OB_FAIL(check_expr_precise(*const_expr, calc_type, column_meta->column_type_))) {
          LOG_WARN("failed to check expr precise", K(ret));
        }

        if (OB_SUCC(ret) && !cur_can_be_extract) {
          if (cur_always_true) {
            // current key cannot extract range
            TmpExprArray *val_exprs = all_val_exprs.at(j);
            if (OB_FAIL(key_idxs.remove(j))) {
              LOG_WARN("failed to remove key idx");
            } else if (OB_FAIL(key_offsets.remove(j))) {
              LOG_WARN("failed to remove key offset");
            } else if (OB_FAIL(column_metas.remove(j))) {
              LOG_WARN("failed to remove column meta");
            } else if (OB_FAIL(all_val_exprs.remove(j))) {
              LOG_WARN("failed to remove val exprs");
            } else {
              val_exprs->destroy();
              alloc.free(val_exprs);
            }
          } else {
            need_add = false;
          }
        }
      }
      if (OB_SUCC(ret) && need_add) {
        ++in_param_count;
        for (int64_t j = 0; OB_SUCC(ret) && j < cur_val_exprs.count(); ++j) {
          int64_t expr_idx = cur_val_exprs.count() - 1 - j;
          if (OB_ISNULL(all_val_exprs.at(j))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected expr array");
          } else if (OB_FAIL(all_val_exprs.at(expr_idx)->push_back(cur_val_exprs.at(j)))) {
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
          if (OB_ISNULL(val_exprs) || OB_UNLIKELY(in_param_count != val_exprs->count())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get null val exprs", K(val_exprs), K(in_param_count));
          } else if (OB_FAIL(get_final_in_array_idx(in_param, param_idx))) {
            LOG_WARN("failed to get final in array idx");
          } else if (OB_FAIL(val_idxs.push_back(param_idx))) {
            LOG_WARN("failed to push back param idx");
          } else if (OB_FAIL(in_param->init(in_param_count))) {
            LOG_WARN("failed to init fix array");
          } else {
            for (int64_t j = 0; OB_SUCC(ret) && j < in_param_count; ++j) {
              int64_t val_idx = 0;
              if (OB_FAIL(get_final_expr_idx(val_exprs->at(j), val_idx))) {
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

int ObExprRangeConverter::get_single_row_in_range_node(const ObRawExpr &rowid_expr,
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
  } else if (!is_physical_rowid) {
    ObSEArray<int64_t, 4> key_idxs;
    ObSEArray<int64_t, 4> pk_offsets;
    TmpExprArray all_valid_exprs;
    for (int64_t i = 0; OB_SUCC(ret) && i < pk_column_items.count(); ++i) {
      const ObColumnRefRawExpr *column_expr = pk_column_items.at(i);
      int64_t key_idx = 0;
      if (is_range_key(column_expr->get_column_id(), key_idx)) {
        // do nothing
      } else if (OB_FAIL(key_idxs.push_back(key_idx))) {
        LOG_WARN("failed to push back key idx");
      } else if (OB_FAIL(pk_offsets.push_back(i))) {
        LOG_WARN("failed to push back key idx");
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && !always_true && i < row_expr.get_param_count(); ++i) {
      const ObRawExpr *const_expr = row_expr.get_param_expr(i);
      if (OB_ISNULL(const_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null expr");
      } else if (OB_UNLIKELY(!const_expr->is_const_expr())) {
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
              if (OB_FAIL(get_final_expr_idx(all_valid_exprs.at(j), val_idx))) {
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
      int64_t val_idx = -1;
      if (OB_ISNULL(const_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null expr");
      } else if (OB_UNLIKELY(!const_expr->is_const_expr())) {
        always_true = true;
      } else if (OB_FAIL(get_final_expr_idx(const_expr, val_idx))) {
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

int ObExprRangeConverter::convert_not_in_expr(const ObRawExpr *expr, ObRangeNode *&range_node)
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
  } else if (OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(l_expr, l_expr))) {
    LOG_WARN("failed to get expr without lossless cast", K(ret));
  } else if (l_expr->get_expr_type() == T_OP_ROW || r_expr->get_param_count() > MAX_NOT_IN_SIZE) {
    // do nothing
  } else if (l_expr->is_column_ref_expr()) {
    ObArenaAllocator alloc;
    ObSEArray<ObRangeNode*, 10> and_range_nodes;
    ObSEArray<ObRangeNode*, 2> or_range_nodes;
    bool is_precise = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < r_expr->get_param_count(); ++i) {
      or_range_nodes.reuse();
      ObRangeNode *tmp_node = nullptr;
      ObRangeNode *final_node = nullptr;
      const ObRawExpr* const_expr = r_expr->get_param_expr(i);
      ObExprResType res_type(alloc);
      res_type.set_calc_meta(expr->get_result_type().get_row_calc_cmp_types().at(i));
      if (OB_ISNULL(const_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null");
      } else if (!const_expr->is_const_expr()) {
        // ignore current node
        is_precise = false;
      } else if (OB_FAIL(get_basic_range_node(l_expr, const_expr, T_OP_LT, res_type, tmp_node))) {
        LOG_WARN("failed tp get basic range node", K(ret));
      } else if (OB_FAIL(or_range_nodes.push_back(tmp_node))) {
        LOG_WARN("failed to push back range node");
      } else if (OB_FALSE_IT(is_precise &= ctx_.cur_is_precise_)) {
      } else if (OB_FAIL(get_basic_range_node(l_expr, const_expr, T_OP_GT, res_type, tmp_node))) {
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
      if (OB_FAIL(ObRangeGraphGenerator::and_range_nodes(and_range_nodes, ctx_.column_cnt_, range_node))) {
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

int ObExprRangeConverter::get_calculable_expr_val(const ObRawExpr *expr,
                                          ObObj &val,
                                          bool &is_valid,
                                          const bool ignore_error/*default true*/)
{
  int ret = OB_SUCCESS;
  ParamStore dummy_params;
  const ParamStore *params = NULL;
  if (expr->has_flag(CNT_DYNAMIC_PARAM)) {
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
      range_node.start_keys_[key_idx] = lib::is_oracle_mode() ? OB_RANGE_MIN_VALUE : OB_RANGE_NULL_VALUE;
      range_node.end_keys_[key_idx] = val_idx;
      if (need_set_flag) {
        range_node.include_start_ = false;
        range_node.include_end_ = (T_OP_LE == cmp_type);
      } else {
        for (int64_t i = key_idx + 1; i < ctx_.column_cnt_; ++i) {
          range_node.start_keys_[i] = lib::is_oracle_mode() ? OB_RANGE_MIN_VALUE : OB_RANGE_MAX_VALUE;
          range_node.end_keys_[i] = (T_OP_LE == cmp_type) ? OB_RANGE_MAX_VALUE : OB_RANGE_MIN_VALUE;
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
      range_node.end_keys_[key_idx] = lib::is_oracle_mode() ? OB_RANGE_NULL_VALUE : OB_RANGE_MAX_VALUE;
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
              range_node.start_keys_[key_idx] = OB_RANGE_NULL_VALUE;
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
            for (int64_t j = key_idx + 1; j < ctx_.column_cnt_; ++j) {
              range_node.start_keys_[j] = lib::is_oracle_mode() ? OB_RANGE_MIN_VALUE : OB_RANGE_MAX_VALUE;
              range_node.end_keys_[j] = (T_OP_LE == cmp_type) ? OB_RANGE_MAX_VALUE : OB_RANGE_MIN_VALUE;
            }
          }
          if (need_set_flag) {
            range_node.include_start_ = false;
            range_node.include_end_ = (T_OP_LE == cmp_type);
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
              range_node.end_keys_[key_idx] = OB_RANGE_NULL_VALUE;
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

int ObExprRangeConverter::check_expr_precise(const ObRawExpr &const_expr,
                                             const ObExprCalcType &calc_type,
                                             const ObExprResType &column_res_type)
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
  } else if (!is_physical_rowid) {
    ObSEArray<int64_t, 4> key_idxs;
    ObSEArray<int64_t, 4> val_idxs;
    for (int64_t i = 0; OB_SUCC(ret) && i < pk_column_items.count(); ++i) {
      const ObColumnRefRawExpr *column_expr = pk_column_items.at(i);
      int64_t key_idx = 0;
      int64_t const_idx = 0;
      if (is_range_key(column_expr->get_column_id(), key_idx)) {
        if (OB_FAIL(get_final_expr_idx(const_expr, const_idx))) {
          LOG_WARN("failed to get final expr idx");
        } else if (OB_FAIL(key_idxs.push_back(key_idx))) {
          LOG_WARN("failed to push back key idx");
        } else if (OB_FAIL(val_idxs.push_back(const_idx))) {
          LOG_WARN("failed to push back val idx");
        } else if (OB_FAIL(ctx_.rowid_idxs_.push_back(std::pair<int64_t, int64_t>(const_idx, i)))) {
          LOG_WARN("failed to push back rowid idxs", K(const_idx), K(i));
        }
      }
    }
    if (OB_SUCC(ret) && key_idxs.count() > 0) {
      if (OB_FAIL(alloc_range_node(range_node))) {
        LOG_WARN("failed to alloc common range node");
      } else if (OB_FAIL(fill_range_node_for_basic_row_cmp(cmp_type, key_idxs, val_idxs, *range_node))) {
        LOG_WARN("failed to get normal row cmp keypart");
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
    } else if (OB_FAIL(get_final_expr_idx(const_expr, const_idx))) {
      LOG_WARN("failed to get final expr idx");
    } else if (OB_FAIL(ctx_.rowid_idxs_.push_back(std::pair<int64_t, int64_t>(const_idx, PHYSICAL_ROWID_IDX)))) {
      LOG_WARN("failed to push back rowid idxs", K(const_idx), K(PHYSICAL_ROWID_IDX));
    } else if (OB_FAIL(alloc_range_node(range_node))) {
      LOG_WARN("failed to alloc common range node");
    } else if (OB_FAIL(fill_range_node_for_basic_cmp(cmp_type, key_idx, const_idx, *range_node))) {
      LOG_WARN("failed to get normal row cmp keypart");
    } else {
      range_node->is_phy_rowid_ = true;
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
int ObExprRangeConverter::get_final_expr_idx(const ObRawExpr *expr, int64_t &idx)
{
  int ret = OB_SUCCESS;
  idx = ctx_.final_exprs_.count();
  if (OB_FAIL(ctx_.final_exprs_.push_back(expr))) {
    LOG_WARN("failed to push back final expr");
  }
  return ret;
}

// in array index start from -1 to INT64_MIN
int ObExprRangeConverter::get_final_in_array_idx(InParam *&in_param, int64_t &idx)
{
  int ret = OB_SUCCESS;
  idx = -(ctx_.in_params_.count() + 1);
  void *ptr = nullptr;
  if (OB_ISNULL(ptr = allocator_.alloc(sizeof(InParam)))) {
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


}  // namespace sql
}  // namespace oceanbase
