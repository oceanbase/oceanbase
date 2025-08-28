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
 * This file contains implementation for rb_contains.
 */
 
#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_rb_contains.h"
#include "sql/engine/expr/ob_expr_rb_func_helper.h"
#include "lib/roaringbitmap/ob_rb_utils.h"
#include "lib/roaringbitmap/ob_rb_bin.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprRbContains::ObExprRbContains(common::ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_RB_CONTAINS, N_RB_CONTAINS, 2, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprRbContains::~ObExprRbContains()
{
}


int ObExprRbContains::calc_result_type2(ObExprResType &type,
                                        ObExprResType &type1,
                                        ObExprResType &type2,
                                        common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  INIT_SUCC(ret);

  if (ob_is_null(type1.get_type())) {
    // do nothing
  } else if (!(type1.is_roaringbitmap() || type1.is_hex_string())) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("invalid roaringbitmap data type provided.", K(ret), K(type1.get_type()), K(type1.get_collation_type()));
  } 
  if (OB_FAIL(ret)) {
    // do nothing
  } else if(ob_is_null(type2.get_type())) {
  } else if (!(type2.is_roaringbitmap() || type2.is_hex_string() || (ob_is_integer_type(type2.get_type()) && !type2.is_tinyint()))) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("invalid roaringbitmap data / big int type provided.", K(ret), K(type1.get_type()), K(type1.get_collation_type()));
  }
  if (OB_SUCC(ret)) {
    type.set_int32();
    type.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].scale_);
    type.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].precision_);
  }
  return ret;
}

int ObExprRbContains::eval_rb_contains(const ObExpr &expr,
                                        ObEvalCtx &ctx,
                                        ObDatum &res)
{
  INIT_SUCC(ret);
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(ObRbExprHelper::get_tenant_id(ctx.exec_ctx_.get_my_session()), "ROARINGBITMAP"));

  bool is_contains = true;
  ObExpr *rb1_arg = expr.args_[0];
  ObString rb1_bin;
  bool is_rb1_null = false;
  ObExpr *rb2_arg = expr.args_[1];
  ObObj right_obj;
  bool is_null_res = false;
  right_obj.set_meta_type(rb2_arg->obj_meta_);

  if (OB_FAIL(ObRbExprHelper::get_input_roaringbitmap_bin(ctx, tmp_allocator, rb1_arg, rb1_bin, is_rb1_null))) {
    LOG_WARN("fail to get left input roaringbitmap", K(ret));
  } else if (is_rb1_null) {
    is_null_res = true;
  } else {
    if(!right_obj.is_integer_type()) {
      // rb_contains(roaringbitmap, roaringbitmap)
      ObString rb2_bin;
      bool is_rb2_null = false;
      if (OB_FAIL(ObRbExprHelper::get_input_roaringbitmap_bin(ctx, tmp_allocator, rb2_arg, rb2_bin, is_rb2_null))) {
        LOG_WARN("fail to get right input roaringbitmap", K(ret));
      } else if (is_rb2_null) {
        is_null_res = true;
      } else if (OB_FAIL(rb_contains(tmp_allocator, rb1_bin, rb2_bin, is_contains))) {
        LOG_WARN("failed to get roaringbitmap contains", K(ret));
      }
    } else {
      // rb_contains(roaringbitmap, bigint Offset)
      uint64_t offset = 0;
      ObDatum *datum = NULL;
      if (OB_FAIL(expr.args_[1]->eval(ctx, datum))) {
        LOG_WARN("fail to eval arg", K(ret));
      } else if (datum->is_null()) {
        is_null_res = true;
      } else {
        if (!right_obj.is_signed_integer()) {
          offset = datum->get_uint();
        } else {
          if (datum->get_int() < 0) {
            if(datum->get_int() < INT32_MIN) {
              ret = OB_SIZE_OVERFLOW;
              LOG_WARN("negative integer not in the range of int32", K(ret), K(datum->get_int()));
            } else {
              uint32_t uint32_val = static_cast<uint32_t>(datum->get_int());
              offset = static_cast<uint64_t>(uint32_val);
            }
          } else {
            offset = datum->get_uint();
          }
        }
        if (!OB_SUCC(ret)) {
        } else if (OB_FAIL(rb_contains(tmp_allocator, rb1_bin, rb1_bin, is_contains, true, offset))) {
          LOG_WARN("failed to get roaringbitmap contains", K(ret));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (is_null_res) {
    res.set_null();
  } else {
    res.set_bool(is_contains);
  }
  return ret;
}

int ObExprRbContains::eval_rb_contains_vector(const ObExpr &expr, 
                                              ObEvalCtx &ctx,
                                              const ObBitVector &skip, 
                                              const EvalBound &bound)
{
  INIT_SUCC(ret);
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(ObRbExprHelper::get_tenant_id(ctx.exec_ctx_.get_my_session()), "ROARINGBITMAP"));
  
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound)) || OB_FAIL(expr.args_[1]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval params", K(ret));
  } else {
    ObIVector *left_vec = expr.args_[0]->get_vector(ctx);
    ObIVector *right_vec = expr.args_[1]->get_vector(ctx);
    ObIVector *res_vec = expr.get_vector(ctx);
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    ObObj right_obj;
    right_obj.set_meta_type(expr.args_[1]->obj_meta_);
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
      bool is_null_res = false;
      ObString rb_bin;
      bool is_contains = true;

      if (skip.at(idx) || eval_flags.at(idx)) {
        continue;
      } else if (left_vec->is_null(idx) || right_vec->is_null(idx)) {
        is_null_res = true;
      } else {
        ObString arr_str;
        if (OB_FAIL(ObTextStringHelper::read_real_string_data(
              tmp_allocator,
              left_vec,
              expr.args_[0]->datum_meta_,
              expr.args_[0]->obj_meta_.has_lob_header(),
              rb_bin,
              idx))){
          LOG_WARN("fail to get real string data", K(ret), K(rb_bin));
        } else {
          if (!right_obj.is_integer_type()) {
            // rb_contains(roaringbitmap, roaringbitmap)
            ObString right_rb_bin;
            if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                  tmp_allocator,
                  right_vec,
                  expr.args_[1]->datum_meta_,
                  expr.args_[1]->obj_meta_.has_lob_header(),
                  right_rb_bin,
                  idx))){
              LOG_WARN("fail to get real string data", K(ret), K(right_rb_bin));
            } else if (OB_FAIL(rb_contains(tmp_allocator, rb_bin, right_rb_bin, is_contains))) {
              LOG_WARN("failed to get roaringbitmap contains", K(ret));
            }
          } else {
            // rb_contains(roaringbitmap, bigint Offset)
            uint64_t offset;
            if (!right_obj.is_signed_integer()) {
              offset = right_vec->get_uint(idx);
            } else {
              if (right_vec->get_int(idx) < 0){
                if(right_vec->get_int(idx) < INT32_MIN) {
                  ret = OB_SIZE_OVERFLOW;
                  LOG_WARN("negative integer not in the range of int32", K(ret), K(right_vec->get_int(idx)));
                } else {
                  uint32_t uint32_val = static_cast<uint32_t>(right_vec->get_int(idx));
                  offset = static_cast<uint64_t>(uint32_val);
                }
              } else {
                offset = right_vec->get_uint(idx);
              }
            }    
            if (!OB_SUCC(ret)) {
            } else if (OB_FAIL(rb_contains(tmp_allocator, rb_bin, rb_bin, is_contains, true, offset))) {
              LOG_WARN("failed to get roaringbitmap contains", K(ret));
            }
          }
        }
      }

      if (OB_FAIL(ret)) {
      } else if (is_null_res) {
        res_vec->set_null(idx);
        eval_flags.set(idx);
      } else {
        res_vec->set_bool(idx, is_contains);
        eval_flags.set(idx);
      }
    }

  }

  return ret;
}

int ObExprRbContains::rb_contains(ObIAllocator &allocator, ObString &rb1_bin, ObString &rb2_bin, bool &is_contains, bool is_offset, uint64_t offset)
{
  int ret = OB_SUCCESS;
  ObRbBinType rb1_type;
  ObRbBinType rb2_type;

  if (OB_FAIL(ObRbUtils::get_bin_type(rb1_bin, rb1_type))) {
    LOG_WARN("invalid left roaringbitmap binary string", K(ret));
  } else if (is_offset){
    // rb_contains(roaringbitmap, bigint Offset)
    uint32_t rb_offset = RB_VERSION_SIZE + RB_BIN_TYPE_SIZE;
    if (rb1_type == ObRbBinType::BITMAP_32) {
      if (offset > UINT32_MAX) {
        is_contains = false;
      } else {
        ObString binary_str;
        binary_str.assign_ptr(rb1_bin.ptr() + rb_offset, rb1_bin.length() - rb_offset);
        ObRoaringBin *roaring_bin = NULL;
        if (OB_ISNULL(roaring_bin = OB_NEWx(ObRoaringBin, &allocator, &allocator, binary_str))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc memory for ObRoaringBin", K(ret));
        } else if (OB_FAIL(roaring_bin->init())) {
          LOG_WARN("failed to init ObRoaringBin", K(ret), K(binary_str));
        } else if (OB_FAIL(roaring_bin->contains(static_cast<uint32_t>(offset), is_contains))) {
          LOG_WARN("failed to get roaring card", K(ret), K(binary_str));
        }
      }
    } else if (rb1_type == ObRbBinType::BITMAP_64) {
      ObString binary_str;
      binary_str.assign_ptr(rb1_bin.ptr() + rb_offset, rb1_bin.length() - rb_offset);
      ObRoaring64Bin *roaring64_bin = NULL;
      if (OB_ISNULL(roaring64_bin = OB_NEWx(ObRoaring64Bin, &allocator, &allocator, binary_str))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory for ObRoaring64Bin", K(ret));
      } else if (OB_FAIL(roaring64_bin->init())) {
        LOG_WARN("failed to init ObRoaring64Bin", K(ret), K(binary_str));
      } else if (OB_FAIL(roaring64_bin->contains(offset, is_contains))) {
        LOG_WARN("failed to get roaring card", K(ret), K(binary_str));
      }
    } else {
      // deserialize roaringbitmap
      ObRoaringBitmap *rb1 = nullptr;
      if (OB_FAIL(ObRbUtils::rb_deserialize(allocator, rb1_bin, rb1))) {
        LOG_WARN("failed to deserialize left roaringbitmap", K(ret));
      } else if (rb1->is_contains(offset)) {
        is_contains = true;
      } else is_contains = false;
      ObRbUtils::rb_destroy(rb1);
    }
    
  } else {
    // rb_contains(roaringbitmap, roaringbitmap)
    uint64_t card_and = 0;
    uint64_t rb2_card = 0;
    if (OB_FAIL(ObRbUtils::get_calc_cardinality(allocator, rb1_bin, rb2_bin, card_and, ObRbOperation::AND))) {
      LOG_WARN("failed to calculate and cardinality", K(ret));
    } else {
      ObRbUtils::get_cardinality(allocator, rb2_bin, rb2_card);
      is_contains = rb2_card == card_and ? true : false;
    }
  }
  return ret;
}

int ObExprRbContains::cg_expr(ObExprCGCtx &expr_cg_ctx,
                              const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = ObExprRbContains::eval_rb_contains;
  rt_expr.eval_vector_func_ = ObExprRbContains::eval_rb_contains_vector;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase