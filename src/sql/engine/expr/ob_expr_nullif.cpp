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

#include "sql/engine/expr/ob_expr_nullif.h"

#include "lib/charset/ob_charset.h"

#include "share/object/ob_obj_cast.h"

//#include "sql/engine/expr/ob_expr_promotion_util.h"
#include "sql/engine/expr/ob_expr_equal.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

namespace oceanbase
{
using namespace oceanbase::common;
namespace sql
{

ObExprNullif::ObExprNullif(ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_SYS_NULLIF, N_NULLIF, 2, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION),
  first_param_can_be_null_(true)
{
}

// in engine 3.0, we have 3 copy of params
// eg:
//  nullif(e0, e1) -> nullif(e0, e1, e2, e3, e4, e5)
//    1. e0 and e1 is used for type deduce, we cannot add cast on e0 or e1,
//       because type deduce maybe unstable.
//    2. cast expr will add on e2 and e3
//    3. e4 is used for result. e5 is useless
int ObExprNullif::calc_result_type2(ObExprResType &type,
                                    ObExprResType &type1,
                                    ObExprResType &type2,
                                    ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  if (type1.is_null()) {
    // eval_nullif() just return null, no need to set cmp type
    type.set_type(ObCharType);
    type.set_collation_level(type1.get_collation_level());
    type.set_default_collation_type();
    type.set_calc_type(ObCharType);
    type.set_calc_collation_type(ObCharset::get_system_collation());
  } else {
    // setup cmp type
    ObExprResType cmp_type;
    if (OB_FAIL(se_deduce_type(type, cmp_type, type1, type2, type_ctx))) {
      LOG_WARN("se deduce type failed", K(ret));
    }
  }
  return ret;
}

int ObExprNullif::se_deduce_type(ObExprResType &type,
                                 ObExprResType &cmp_type,
                                 ObExprResType &type1,
                                 ObExprResType &type2,
                                 ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  type.set_meta(type1.get_obj_meta());
  type.set_accuracy(type1.get_accuracy());
  if (ob_is_real_type(type.get_type()) && SCALE_UNKNOWN_YET != type1.get_scale()) {
    type.set_precision(static_cast<ObPrecision>(ObMySQLUtil::float_length(type1.get_scale())));
  } else if (ob_is_string_type(type.get_type()) || ob_is_enumset_tc(type.get_type())) {
    type.set_collation_level(type1.get_collation_level());
    type.set_collation_type(type1.get_collation_type());
  }
  if (ob_is_enumset_tc(type.get_type()) || ob_is_enumset_inner_tc(type.get_type())) {
    type.set_varchar();
  }
  OZ(calc_cmp_type2(cmp_type, type1, type2, type_ctx.get_coll_type()));
  if (OB_SUCC(ret)) {
    if (ob_is_enumset_tc(type1.get_type()) || ob_is_enumset_tc(type2.get_type())) {
      ObObjType calc_type = enumset_calc_types_[OBJ_TYPE_TO_CLASS[cmp_type.get_calc_type()]];
      CK(ObMaxType != calc_type);
      if (OB_SUCC(ret)) {
        cmp_type.set_calc_type(calc_type);
        cmp_type.set_calc_collation_type(ObCharset::get_system_collation());
        if (ObVarcharType == calc_type) {
          if (ob_is_enumset_tc(type1.get_type())) {
            // only set calc type when calc_type is varchar.
            // EnumWrapper will add EnumToStr when calc_type is varchar, and otherwise add EnumToInner.
            // If set calc type when calc_type is not varchar, for example, uint64,
            // implicit cast from enum_inner to uint64 will be added on EnumToInner expression.
            type1.set_calc_type(calc_type);
            type1.set_calc_collation_type(cmp_type.get_calc_collation_type());
          }
        }
        // set calc type for type2 no matter whether calc_type is varchar or not, and no matther which param is enum.
        type2.set_calc_type(calc_type);
        type2.set_calc_collation_type(cmp_type.get_calc_collation_type());
      }
    }
    if (OB_SUCC(ret)) {
      type.set_calc_meta(cmp_type.get_calc_meta());
      type.set_calc_accuracy(cmp_type.get_calc_accuracy());
    }
  }
  return ret;
}

int ObExprNullif::set_extra_info(ObExprCGCtx &expr_cg_ctx, const ObObjType cmp_type,
                                 const ObCollationType cmp_cs_type, const ObScale scale,
                                 ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  ObCastMode cm = CM_NONE;
  const bool is_explicit_cast = false;
  const int32_t result_flag = 0;
  DatumCastExtraInfo *info = OB_NEWx(DatumCastExtraInfo, expr_cg_ctx.allocator_, *(expr_cg_ctx.allocator_), type_);
  if (OB_ISNULL(info)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory failed", K(ret));
  } else if (OB_ISNULL(expr_cg_ctx.session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx.session is null", K(ret));
  } else if (OB_FAIL(ObSQLUtils::get_default_cast_mode(is_explicit_cast, result_flag,
                                                      expr_cg_ctx.session_, cm))) {
    LOG_WARN("get default cast mode failed", K(ret));
  } else {
    info->cmp_meta_.type_ = cmp_type;
    info->cmp_meta_.cs_type_ = cmp_cs_type;
    info->cmp_meta_.scale_ = scale;
    info->cm_ = cm;
    rt_expr.extra_info_ = info;
  }

  return ret;
}

int ObExprNullif::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                          ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  CK(2 == rt_expr.arg_cnt_);
  const uint32_t param_num = rt_expr.arg_cnt_;
  const ObObjMeta &cmp_meta = result_type_.get_calc_meta();
  if (ObNullType == rt_expr.args_[0]->datum_meta_.type_) {
    OX(rt_expr.eval_func_ = eval_nullif);
  } else if (OB_FAIL(set_extra_info(expr_cg_ctx, cmp_meta.get_type(),
                                    cmp_meta.get_collation_type(), cmp_meta.get_scale(),
                                    rt_expr))) {
    LOG_WARN("set extra info failed", K(ret));
  } else if (ob_is_enumset_inner_tc(rt_expr.args_[0]->datum_meta_.type_)) {
    if (OB_UNLIKELY(!ob_is_uint_tc(rt_expr.args_[1]->datum_meta_.type_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected param type", K(ret), K(rt_expr.args_[1]->datum_meta_));
    } else {
      rt_expr.eval_func_ = eval_nullif_enumset;
    }
  } else if (ob_is_enumset_inner_tc(rt_expr.args_[1]->datum_meta_.type_)) {
    if (OB_UNLIKELY(rt_expr.datum_meta_.type_ != rt_expr.args_[0]->datum_meta_.type_
                    || rt_expr.datum_meta_.cs_type_ != rt_expr.args_[0]->datum_meta_.cs_type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected type", K(ret), K(rt_expr.datum_meta_), K(rt_expr.args_[0]->datum_meta_));
    } else {
      rt_expr.eval_func_ = eval_nullif_enumset;
    }
  } else {
    OV(OB_NOT_NULL(rt_expr.inner_functions_ = reinterpret_cast<void**>(
                  expr_cg_ctx.allocator_->alloc(sizeof(DatumCmpFunc) * 1))),
      OB_ALLOCATE_MEMORY_FAILED);

    if (OB_SUCC(ret)) {
      if (!cmp_meta.is_null()) {
        DatumCmpFunc cmp_func = NULL;
        if (rt_expr.args_[0]->datum_meta_.cs_type_ == rt_expr.args_[1]->datum_meta_.cs_type_) {
          bool has_lob_header = rt_expr.args_[0]->obj_meta_.has_lob_header() ||
                                rt_expr.args_[1]->obj_meta_.has_lob_header();
          cmp_func = ObExprCmpFuncsHelper::get_datum_expr_cmp_func(
                                                            rt_expr.args_[0]->datum_meta_.type_,
                                                            rt_expr.args_[1]->datum_meta_.type_,
                                                            rt_expr.args_[0]->datum_meta_.scale_,
                                                            rt_expr.args_[1]->datum_meta_.scale_,
                                                            lib::is_oracle_mode(),
                                                            rt_expr.args_[0]->datum_meta_.cs_type_,
                                                            has_lob_header);
        }
        if (NULL != cmp_func) {
          rt_expr.inner_func_cnt_ = 1;
          rt_expr.inner_functions_[0] = reinterpret_cast<void*>(cmp_func);
          DatumCastExtraInfo *info = static_cast<DatumCastExtraInfo *>(rt_expr.extra_info_);
          // assign null type means can compare directly.
          info->cmp_meta_.type_ = ObNullType;
        } else {
          if (OB_ISNULL(cmp_func = ObExprCmpFuncsHelper::get_datum_expr_cmp_func(
                                                            cmp_meta.get_type(),
                                                            cmp_meta.get_type(),
                                                            cmp_meta.get_scale(),
                                                            cmp_meta.get_scale(),
                                                            lib::is_oracle_mode(),
                                                            cmp_meta.get_collation_type(),
                                                            cmp_meta.has_lob_header()))){
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("cmp func is null", K(ret), K(cmp_meta));
          } else {
            rt_expr.inner_func_cnt_ = 1;
            rt_expr.inner_functions_[0] = reinterpret_cast<void*>(cmp_func);
          }
        }
      }
      OX(rt_expr.eval_func_ = eval_nullif);
    }
  }
  return ret;
}

int ObExprNullif::cast_param(const ObExpr &src_expr, ObEvalCtx &ctx,
                                    const ObDatumMeta &dst_meta,
                                    const ObCastMode &cm, ObIAllocator &allocator,
                                    ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  const bool string_type = ob_is_string_type(dst_meta.type_);
  if (src_expr.datum_meta_.type_ == dst_meta.type_
      && (!string_type || src_expr.datum_meta_.cs_type_ == dst_meta.cs_type_)) {
    res_datum = src_expr.locate_expr_datum(ctx);
  } else if (OB_ISNULL(ctx.datum_caster_) && OB_FAIL(ctx.init_datum_caster())) {
    LOG_WARN("init datum caster failed", K(ret));
  } else {
    ObDatum *cast_datum = NULL;
    if (OB_FAIL(ctx.datum_caster_->to_type(dst_meta, src_expr, cm, cast_datum, ctx.get_batch_idx()))) {
      LOG_WARN("fail to dynamic cast", K(ret), K(cm));
    } else if (OB_FAIL(res_datum.deep_copy(*cast_datum, allocator))) {
      LOG_WARN("deep copy datum failed", K(ret));
    } else {
      LOG_DEBUG("cast_param", K(src_expr), KP(ctx.frames_[src_expr.frame_idx_]),
                K(&(src_expr.locate_expr_datum(ctx))),
                K(ObToStringExpr(ctx, src_expr)), K(dst_meta), K(res_datum));
    }
  }
  return ret;
}

int ObExprNullif::cast_result(const ObExpr &src_expr, const ObExpr &dst_expr, ObEvalCtx &ctx,
                                     const ObCastMode &cm, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  const bool string_type = ob_is_string_type(src_expr.datum_meta_.type_);
  if (src_expr.datum_meta_.type_ == dst_expr.datum_meta_.type_
      && (!string_type || src_expr.datum_meta_.cs_type_ == dst_expr.datum_meta_.cs_type_)) {
    ObDatum *res_datum = nullptr;
    if (OB_FAIL(src_expr.eval(ctx, res_datum))) {
      LOG_WARN("eval param value failed", K(ret));
    } else {
      expr_datum = *res_datum;
    }
  } else if (OB_ISNULL(ctx.datum_caster_) && OB_FAIL(ctx.init_datum_caster())) {
    LOG_WARN("init datum caster failed", K(ret));
  } else {
    ObDatum *cast_datum = NULL;
    if (OB_FAIL(ctx.datum_caster_->to_type(dst_expr.datum_meta_, src_expr, cm, cast_datum, ctx.get_batch_idx()))) {
      LOG_WARN("fail to dynamic cast", K(ret));
    } else if (OB_FAIL(dst_expr.deep_copy_datum(ctx, *cast_datum))) {
      LOG_WARN("deep copy datum failed", K(ret));
    }
  }
  return ret;
}

int ObExprNullif::eval_nullif(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *cmp_e0 = NULL;
  ObDatum *cmp_e1 = NULL;
  ObDatum *res_e = NULL;
  DatumCastExtraInfo *cast_info = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, cmp_e0))) {
    LOG_WARN("eval param 0 failed", K(ret));
  } else if (cmp_e0->is_null()) {
    res.set_null();
  } else if (OB_UNLIKELY(1 != expr.inner_func_cnt_) || OB_ISNULL(expr.inner_functions_)
      || OB_ISNULL(expr.inner_functions_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected param", K(ret), K(expr.inner_func_cnt_), KP(expr.inner_functions_),
                                 KP(expr.inner_functions_[0]));
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, cmp_e1))) {
    LOG_WARN("eval param failed", K(ret));
  } else if (FALSE_IT(cast_info = static_cast<DatumCastExtraInfo *>(expr.extra_info_))) {
  } else if (ObNullType == cast_info->cmp_meta_.type_) {
    // can compare directly.
    DatumCmpFunc cmp_func = reinterpret_cast<DatumCmpFunc>(expr.inner_functions_[0]);
    int cmp_ret = 0;
    bool equal = false;
    if (!cmp_e1->is_null()) {
      if (OB_FAIL(cmp_func(*cmp_e0, *cmp_e1, cmp_ret))) {
        LOG_WARN("cmp failed", K(ret));
      } else {
        equal = (0 == cmp_ret);
      }
    }
    if (OB_FAIL(ret)) {
    } else if (equal) {
      res.set_null();
    } else if (OB_FAIL(cast_result(*expr.args_[0], expr, ctx, cast_info->cm_, res))) {
      LOG_WARN("cast result failed", K(ret));
    }
  } else if (cmp_e1->is_null()) {
    // e0 is not null, e1 is null
    if (OB_FAIL(cast_result(*expr.args_[0], expr, ctx, cast_info->cm_, res))) {
      LOG_WARN("cast result failed", K(ret));
    }
  } else {
    DatumCmpFunc cmp_func = reinterpret_cast<DatumCmpFunc>(expr.inner_functions_[0]);
    ObTempExprCtx::TempAllocGuard tmp_alloc_guard(ctx);
    ObDatum datum1;
    ObDatum datum2;
    if (OB_FAIL(cast_param(*expr.args_[0], ctx, cast_info->cmp_meta_, cast_info->cm_,
                             tmp_alloc_guard.get_allocator(), datum1))) {
      LOG_WARN("cast param failed", K(ret));
    } else if (OB_FAIL(cast_param(*expr.args_[1], ctx, cast_info->cmp_meta_, cast_info->cm_,
                             tmp_alloc_guard.get_allocator(), datum2))) {
      LOG_WARN("cast param failed", K(ret));
    } else {
      int cmp_ret = 0;
      if (OB_FAIL(cmp_func(datum1, datum2, cmp_ret))) {
        LOG_WARN("cmp failed", K(ret));
      } else if (cmp_ret == 0) {
        res.set_null();
      } else if (OB_FAIL(cast_result(*expr.args_[0], expr, ctx, cast_info->cm_, res))) {
        LOG_WARN("cast result failed", K(ret));
      }
    }
  }
  return ret;
}

int ObExprNullif::eval_nullif_enumset(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *cmp_e0 = NULL;
  ObDatum *cmp_e1 = NULL;
  ObCastMode cm = CM_NONE;
  DatumCastExtraInfo *cast_info = NULL;
  bool equal = 0;
  if (OB_FAIL(expr.args_[0]->eval(ctx, cmp_e0))) {
    LOG_WARN("eval param 0 failed", K(ret));
  } else if (cmp_e0->is_null()) {
    res.set_null();
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, cmp_e1))) {
    LOG_WARN("eval param failed", K(ret));
  } else if (OB_ISNULL(expr.extra_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("extra info is null", K(ret));
  } else if (FALSE_IT(cast_info = static_cast<DatumCastExtraInfo *>(expr.extra_info_))) {
  } else if (ob_is_enumset_inner_tc(expr.args_[0]->datum_meta_.type_)) {
    ObEnumSetInnerValue inner_value;
    if (OB_FAIL(cmp_e0->get_enumset_inner(inner_value))) {
      LOG_WARN("failed to inner_value", K(ret));
    } else if (cmp_e1->is_null()) {
      res.set_string(inner_value.string_value_);
    } else if (cmp_e1->get_uint64() == inner_value.numberic_value_) {
      res.set_null();
    } else {
      res.set_string(inner_value.string_value_);
    }
  } else {
    ObDatum datum0;
    ObEnumSetInnerValue inner_value;
    ObTempExprCtx::TempAllocGuard tmp_alloc_guard(ctx);
    if (cmp_e1->is_null()) {
      res = *cmp_e0;
    } else if (OB_FAIL(cmp_e1->get_enumset_inner(inner_value))) {
      LOG_WARN("failed to inner_value", K(ret));
    } else if (OB_FAIL(cast_param(*expr.args_[0], ctx, cast_info->cmp_meta_, cast_info->cm_,
                                  tmp_alloc_guard.get_allocator(), datum0))) {
      LOG_WARN("cast param failed", K(ret));
    } else if (datum0.get_uint64() == inner_value.numberic_value_) {
      res.set_null();
    } else {
      res = *cmp_e0;
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase