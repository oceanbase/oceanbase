/**
 * Copyright (c) 2024 OceanBase
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
#include "sql/engine/expr/ob_expr_search_index_inner.h"
#include "share/search_index/ob_search_index_encoder.h"
#include "sql/engine/expr/ob_expr_json_type.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_multi_mode_func_helper.h"
#include "sql/engine/expr/ob_expr_json_func_helper.h"
#include "share/datum/ob_datum.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::share;

namespace oceanbase
{
namespace sql
{

ObExprSearchIndexInnerPath::ObExprSearchIndexInnerPath(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_SEARCH_INDEX_INNER_PATH, N_SEARCH_INDEX_INNER_PATH, 2,
                         VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprSearchIndexInnerPath::~ObExprSearchIndexInnerPath()
{
}

int ObExprSearchIndexInnerPath::calc_result_type2(ObExprResType &type,
                                                  ObExprResType &type1,
                                                  ObExprResType &type2,
                                                  ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  type.set_varbinary();
  type.set_length(common::OB_MAX_VARBINARY_LENGTH);
  type.set_collation_type(CS_TYPE_BINARY);
  if (!type1.is_varbinary()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected type", K(ret), K(type1));
  }
  return ret;
}

// Pack pick_type and bound_enc_type into a single uint64_t for ObExpr::extra_.
// This bridges compile-time (cg_expr) and runtime (calc_search_index_inner_path).
//
// Bit layout of extra:
//   [63 ............ 40] [39 ......... 32] [31 ................. 0]
//        reserved          bound_enc_type        pick_type
//
// - pick_type (lower 32 bits):  ObItemType indicating which JSON type to filter
//                                on in the search index (T_NULL means no pick).
// - bound_enc_type (bits 32-39): encoding hint for range-query bounds
//                                (e.g. LOWER_BOUND / UPPER_BOUND).
uint64_t ObExprSearchIndexInnerPath::make_extra(const ObRawExpr &raw_expr)
{
  uint64_t extra = static_cast<uint64_t>(raw_expr.get_pick());
  extra |= static_cast<uint64_t>(raw_expr.get_bound_enc_type()) << 32;
  return extra;
}

// Extract pick_type from the lower 32 bits of extra.
ObItemType ObExprSearchIndexInnerPath::get_pick_type(const uint64_t extra)
{
  const uint64_t PICK_TYPE_MASK = 0xFFFFFFFFULL;
  return static_cast<ObItemType>(extra & PICK_TYPE_MASK);
}

// Extract bound_enc_type from bits 32-39 of extra.
uint8_t ObExprSearchIndexInnerPath::get_bound_enc_type(const uint64_t extra)
{
  const uint64_t BOUND_ENC_TYPE_MASK = 0xFFULL;
  return static_cast<uint8_t>((extra >> 32) & BOUND_ENC_TYPE_MASK);
}

int ObExprSearchIndexInnerPath::cg_expr(ObExprCGCtx &op_cg_ctx,
                                        const ObRawExpr &raw_expr,
                                        ObExpr &rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (rt_expr.arg_cnt_ != 2) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("search index type expr should have one param", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0]) || OB_ISNULL(rt_expr.args_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of search index type expr is null", K(ret), K(rt_expr.args_));
  } else {
    rt_expr.eval_func_ = ObExprSearchIndexInnerPath::calc_search_index_inner_path;
    // pass pick type and bound range encoding type to rt_expr
    rt_expr.extra_ = make_extra(raw_expr);
  }
  return ret;
}

int ObExprSearchIndexInnerPath::calc_search_index_inner_path(const ObExpr &expr, ObEvalCtx &ctx,
                                                             ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *path_prefix_datum = NULL;
  ObDatum *json_datum = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, path_prefix_datum))) {
    LOG_WARN("eval param value failed", K(ret));
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, json_datum))) {
    LOG_WARN("eval param value failed", K(ret));
  } else if (OB_UNLIKELY(path_prefix_datum->is_null() || json_datum->is_null())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param value is null", K(ret));
  } else {
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    uint64_t tenant_id = ObMultiModeExprHelper::get_tenant_id(ctx.exec_ctx_.get_my_session());
    MultimodeAlloctor tmp_allocator(tmp_alloc_g.get_allocator(), expr.type_, tenant_id, ret);
    ObIJsonBase *j_base = NULL;

    // Add baseline size for LOB data
    if (OB_FAIL(tmp_allocator.add_baseline_size(json_datum, expr.args_[1]->obj_meta_.has_lob_header()))) {
      LOG_WARN("failed to add baseline size.", K(ret));
    // Convert parameter to JSON value
    } else if (OB_FAIL(ObJsonExprHelper::get_json_val(expr, ctx, &tmp_allocator, 1, j_base))) {
      LOG_WARN("get_json_val failed", K(ret));
    } else if (OB_ISNULL(j_base)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("json base is null", K(ret));
    } else {
      ObString path_prefix = path_prefix_datum->get_string();
      ObString result;
      const ObItemType pick_type = get_pick_type(expr.extra_);
      const uint8_t bound_enc_type = get_bound_enc_type(expr.extra_);
      if (T_NULL != pick_type) {
        if (OB_FAIL(calc_pick_inner_path(*j_base, path_prefix, tmp_allocator, pick_type,
                                         bound_enc_type, result))) {
          LOG_WARN("fail to calc pick inner path", K(ret));
        }
      } else {
        if (OB_FAIL(ObSearchIndexPathEncoder::encode_path(tmp_allocator, path_prefix, j_base, result))) {
          LOG_WARN("fail to calc search index path", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        char *buf = expr.get_str_res_mem(ctx, result.length());
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("get memory failed", K(ret), K(result.length()));
        } else {
          MEMCPY(buf, result.ptr(), result.length());
          expr_datum.set_string(buf, result.length());
        }
      }
    }
  }
  return ret;
}

int ObExprSearchIndexInnerPath::calc_pick_inner_path(const ObIJsonBase &j_base,
                                                     const ObString &path_prefix,
                                                     ObIAllocator &allocator,
                                                     const ObItemType pick_type,
                                                     const uint8_t bound_enc_type,
                                                     ObString &result)
{
  int ret = OB_SUCCESS;
  uint8_t const_enc_type = 0;
  uint8_t pick_enc_type = 0;
  uint8_t enc_type = 0;
  const ObJsonNodeType json_type = j_base.json_type();
  if (OB_FAIL(ObSearchIndexPathEncoder::encode_type(json_type, const_enc_type))) {
    LOG_WARN("fail to encode type", K(ret));
  } else if (OB_FAIL(ObSearchIndexPathEncoder::encode_pick_path(pick_type, pick_enc_type))) {
    LOG_WARN("fail to encode pick type", K(ret));
  } else if (ObSearchIndexPathEncoder::LOWER_BOUND == bound_enc_type) {
    // Building the lower-bound path for a "col pick_type > const" range scan.
    if (const_enc_type < pick_enc_type) {
      // const type sorts before pick type (e.g., number < string in JSON ordering).
      // "col pick_type > const" is always true for all pick-type values, so use pick_enc - 1
      // to place the bound in the gap just before pick-type entries; the actual
      // value comparison becomes irrelevant (no real data lives at this path).
      enc_type = pick_enc_type - 1;
    } else {
      // const_enc >= pick_enc. Two sub-cases, both resolved by const_enc:
      //  - equal:   types match, precise value filtering (const_enc == pick_enc).
      //  - greater: "col pick_type > const" is always false, const_enc pushes the bound
      //             past all pick-type entries, producing an empty range.
      enc_type = const_enc_type;
    }
  } else if (ObSearchIndexPathEncoder::UPPER_BOUND == bound_enc_type) {
    // Building the upper-bound path for a "col pick_type < const" range scan.
    if (const_enc_type > pick_enc_type) {
      // const type sorts after pick type (e.g., string > number in JSON ordering).
      // "col pick_type < const" is always true for all pick-type values, so use pick_enc + 1
      // to place the bound in the gap just after pick-type entries; the actual
      // value comparison becomes irrelevant (no real data lives at this path).
      enc_type = pick_enc_type + 1;
    } else {
      // const_enc <= pick_enc. Two sub-cases, both resolved by const_enc:
      //  - equal:   types match, precise value filtering (const_enc == pick_enc).
      //  - less:    "col pick_type < const" is always false, const_enc places the bound
      //             before all pick-type entries, producing an empty range.
      enc_type = const_enc_type;
    }
  } else if (ObJsonExprHelper::check_pick_type_match(json_type, pick_type)) {
    // pick type match, use pick encoding type
    enc_type = pick_enc_type;
  } else {
    // choose lower bound encoding type as impossible to match pick type
    enc_type = ObSearchIndexPathEncoder::LOWER_BOUND;
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObSearchIndexPathEncoder::generate_enc_path(allocator, path_prefix, enc_type,
                                                            result))) {
      LOG_WARN("fail to generate enc path", K(ret));
    }
  }
  return ret;
}

ObExprSearchIndexInnerValue::ObExprSearchIndexInnerValue(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_SEARCH_INDEX_INNER_VALUE, N_SEARCH_INDEX_INNER_VALUE, 2,
                         VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprSearchIndexInnerValue::~ObExprSearchIndexInnerValue()
{
}

int ObExprSearchIndexInnerValue::calc_result_type2(ObExprResType &type,
                                                   ObExprResType &type1,
                                                   ObExprResType &type2,
                                                   ObExprTypeCtx &type_ctx) const
{
  UNUSED(type1);
  UNUSED(type2);
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  type.set_varbinary();
  type.set_length(common::OB_MAX_VARBINARY_LENGTH);
  type.set_collation_type(CS_TYPE_BINARY);
  return ret;
}

int ObExprSearchIndexInnerValue::cg_expr(ObExprCGCtx &op_cg_ctx,
                                         const ObRawExpr &raw_expr,
                                         ObExpr &rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (rt_expr.arg_cnt_ != 2) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("search index value expr should have one param", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0]) || OB_ISNULL(rt_expr.args_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of search index value expr is null", K(ret), K(rt_expr.args_));
  } else {
    rt_expr.eval_func_ = ObExprSearchIndexInnerValue::calc_search_index_inner_value;
  }
  return ret;
}

int ObExprSearchIndexInnerValue::calc_search_index_inner_value(const ObExpr &expr,
                                                               ObEvalCtx &ctx,
                                                               ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *param_datum = NULL;
  ObDatum *cmp_type_datum = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, param_datum))) {
    LOG_WARN("eval param value failed", K(ret));
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, cmp_type_datum))) {
    LOG_WARN("eval param value failed", K(ret));
  } else if (OB_UNLIKELY(param_datum->is_null())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param value is null", K(ret));
  } else {
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    ObString value;
    // Check if cmp_type_datum is null - if null, use existing implementation
    if (cmp_type_datum->is_null()) {
      ObIAllocator &calc_alloc = alloc_guard.get_allocator();
      if (OB_FAIL(ObSearchIndexValueEncoder::encode_value(calc_alloc,
                                                          *param_datum,
                                                          expr.args_[0]->obj_meta_,
                                                          value))) {
        LOG_WARN("fail to calc search index value from obj", K(ret));
      }
    } else {
      // When cmp_type_datum is not null (indicating JSON type)
      // Convert param to JSON and use JSON encoding
      uint64_t tenant_id = ObMultiModeExprHelper::get_tenant_id(ctx.exec_ctx_.get_my_session());
      MultimodeAlloctor tmp_allocator(alloc_guard.get_allocator(), expr.type_, tenant_id, ret);
      ObIJsonBase *j_base = NULL;

      // Add baseline size for LOB data
      if (OB_FAIL(tmp_allocator.add_baseline_size(param_datum, expr.args_[0]->obj_meta_.has_lob_header()))) {
        LOG_WARN("failed to add baseline size.", K(ret));
      // Convert parameter to JSON value
      } else if (OB_FAIL(ObJsonExprHelper::get_json_val(expr, ctx, &tmp_allocator, 0, j_base))) {
        LOG_WARN("get_json_val failed", K(ret));
      } else if (OB_FAIL(ObSearchIndexValueEncoder::encode_value(tmp_allocator, j_base, value))) {
        LOG_WARN("fail to calc search index value from json", K(ret));
      }
    }

    // Copy the encoded value to result
    if (OB_SUCC(ret)) {
      if (value.length() > 0) {
        char *buf = expr.get_str_res_mem(ctx, value.length());
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("get memory failed", K(ret));
        } else {
          MEMCPY(buf, value.ptr(), value.length());
          expr_datum.set_string(buf, value.length());
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("value is empty", K(ret));
      }
    }
  }
  return ret;
}

}
}
