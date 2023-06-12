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

#include "sql/engine/expr/ob_expr_least.h"
#include "sql/engine/expr/ob_expr_greatest.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_expr_less_than.h"
//#include "sql/engine/expr/ob_expr_promotion_util.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/session/ob_sql_session_info.h"
#include "lib/oblog/ob_log.h"
#include "share/object/ob_obj_cast.h"
#include "sql/engine/expr/ob_datum_cast.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprLeastGreatest::ObExprLeastGreatest(ObIAllocator &alloc, ObExprOperatorType type,
                                                const char *name, int32_t param_num)
    : ObMinMaxExprOperator(alloc,
                           type,
                           name,
                           param_num,
                           NOT_ROW_DIMENSION)
{
}

int ObExprLeastGreatest::calc_result_typeN(ObExprResType &type,
                                           ObExprResType *types_stack,
                                           int64_t param_num,
                                           ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  if (is_oracle_mode()) {
    ret = calc_result_typeN_oracle(type, types_stack, param_num, type_ctx);
  } else {
    ret = calc_result_typeN_mysql(type, types_stack, param_num, type_ctx);
  }
  return ret;
}

int ObExprLeastGreatest::calc_result_typeN_oracle(ObExprResType &type,
                                                  ObExprResType *types,
                                                  int64_t param_num,
                                                  ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo *session = static_cast<const ObSQLSessionInfo*>(type_ctx.get_session());
  ObExprOperator::calc_result_flagN(type, types, param_num);
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null");
  } else if (OB_ISNULL(types) || OB_UNLIKELY(param_num < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("types is null or param_num is wrong", K(types), K(param_num), K(ret));
  } else {
    ObExprResType &first_type = types[0];
    type = first_type;
    ObObjTypeClass first_type_class = first_type.get_type_class();
    /**
     * number类型和其它类型行为不一致，单独处理
     */
    if (ObIntTC == first_type_class
        || ObUIntTC == first_type_class
        || ObNumberTC == first_type_class) {
      type.set_type(ObNumberType);
      type.set_calc_type(ObNumberType);
      // scale和precision信息设置为unknown，兼容oracle的number行为
      type.set_scale(ORA_NUMBER_SCALE_UNKNOWN_YET);
      type.set_precision(PRECISION_UNKNOWN_YET);
    } else if (ObLongTextType == type.get_type()) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("lob type parameter not expected", K(ret));
    } else if (ObStringTC == first_type_class) {
      // https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/GREATEST.html
      if (ob_is_nstring(first_type.get_type())) {
        type.set_type(ObNVarchar2Type);
        type.set_calc_type(ObNVarchar2Type);
      } else {
        type.set_type(ObVarcharType);
        type.set_calc_type(ObVarcharType);
      }
    } else if (ObUserDefinedSQLTC == first_type_class) {
      ret = OB_ERR_NO_ORDER_MAP_SQL;
      LOG_WARN("cannot ORDER objects without MAP or ORDER method", K(ret));
    } else {
      /**
       * 除去number类型，经过测试，结果的scale和第一个参数的scale一样，所以
       * 这里不重新设置
       */
      type.set_type(first_type.get_type());
      type.set_calc_type(first_type.get_type());
    }

    /* 只有结果是字符串需要考虑结果的length信息，经过测试，有两种情况：
      * TODO 1，如果所有参数都是常量，那么length和对应结果参数的length相同，
      *    这一块还未相好如何实现
      * 2，如果含有列，那么长度是所有参数的最大值
      * 对于结果参数需要长度是char还是byte，还需要做特殊处理
      */

    if (ObStringTC == type.get_type_class() || ObRawTC == type.get_type_class()) {
      int64_t max_length = 0;
      int64_t all_char = 0;
      for(int64_t i =0; OB_SUCC(ret) && i < param_num; i++) {
        int64_t item_length =0;
        switch(types[i].get_type_class()) {
          case ObStringTC:{
            item_length = types[i].get_length();
            if (LS_CHAR == types[i].get_length_semantics()) {
              item_length = item_length * 4;
              all_char++;
            }
            break;
          }
          case ObTextTC:
          case ObRowIDTC: {
            item_length = types[i].get_length();
            break;
          }
          case ObRawTC: {
            // raw类型长度乘2
            item_length = types[i].get_length() * 2;
            break;
          }
          case ObNumberTC:
          case ObIntTC:
          case ObUIntTC: {
            // 处理number长度的问题，oracle最大设置成40即可，OB因为没有科学计数法，
            // 所以长度可能超过40，需要做特殊处理
            item_length = number::ObNumber::MAX_PRECISION - number::ObNumber::MIN_SCALE;
            break;
          }
          case ObFloatTC:
          case ObDoubleTC:
          case ObNullTC:
          case ObIntervalTC: {
            item_length = 40;
            break;
          }
          case ObOTimestampTC:
          case ObDateTimeTC: {
            item_length = OB_MAX_TIMESTAMP_TZ_LENGTH;
            break;
          }
          case ObUserDefinedSQLTC: {
            item_length = types[i].get_length();
            break;
          }
          default:{
            // all types in oracle mode have been handled
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid argument", K(ret), K(types[i]), K(types[i]));
          }
        }
        if (OB_SUCC(ret)) {
          max_length =  MAX(max_length, item_length);
        }
      }
      if (OB_SUCC(ret)) {
        if (all_char == param_num) {
          type.set_length(static_cast<ObLength>(max_length / 4));
          type.set_length_semantics(LS_CHAR);
        } else if (ObRawTC == type.get_type_class()) {
          type.set_length(static_cast<ObLength>(max_length / 2));
        } else {
          type.set_length(static_cast<ObLength>(max_length));
        }
      }
    }
  }

  // 在calc_result_type就开始做类型检查，和Oracle行为兼容
  if (ObNullType != types[0].get_type()) {
    for (int64_t i = 1; OB_SUCC(ret) && i < param_num; i++) {
      OZ(ObObjCaster::can_cast_in_oracle_mode(types[0].get_type(), types[0].get_collation_type(),
                                              types[i].get_type(), types[i].get_collation_type()));
    }
  }

//老执行引擎在类型推导时不为参数设置calc_type, 在执行期再对参数进行cast。
//新执行引擎需要在类型推导阶段为参数设置好calc_type, 在执行期不再显式执行cast。
  if (OB_SUCC(ret)
      && OB_LIKELY(!type.get_calc_meta().is_null())) {
    for (int i = 0; i < param_num; i++) {
      types[i].set_calc_meta(type.get_calc_meta());
    }
  }
  return ret;
}

/* MySQL中greatest行为：
 * *. cached_field_type是用标准逻辑agg_field_type()推导, 作为结果列类型。
 * *. collation的计算过程: 如果参数中包含数值类型，则为binary，否则通过规则辗转计算
 * *. 计算过程却是根据get_cmp_type()来找到一个中间结果，然后所有数值转到中间结果上在作比较运算
 *    也就是说，比较过程与cached_field_type无关。get_calc_type()的逻辑是：只有参数都是STRING的情况下，
 *    返回类型才是STRING，否则返回数值类型。返回数值类型的时候，则所有参数都转成数值类型再做比较。
 */
int ObExprLeastGreatest::calc_result_typeN_mysql(ObExprResType &type,
                                                 ObExprResType *types,
                                                 int64_t param_num,
                                                 common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(param_num <= 1)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("not enough param", K(ret));
    ObString func_name(get_name());
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, func_name.length(), func_name.ptr());
  } else {
    const ObSQLSessionInfo *session = static_cast<const ObSQLSessionInfo*>(type_ctx.get_session());
    ObExprOperator::calc_result_flagN(type, types, param_num);
    // 如果所有参数都是IntTC或UIntTC, 那么不对参数做cast，结果类型根据参数的长度做类型提升。
    // 否则将所有参数都cast到推导出都calc_type
    bool all_integer = true;
    for (int i = 0; i < param_num && all_integer; ++i) {
      ObObjType obj_type = types[i].get_type();
      if (!ob_is_integer_type(obj_type) && ObNullType != obj_type) {
        all_integer = false;
      }
    }
    const ObLengthSemantics default_length_semantics = (OB_NOT_NULL(type_ctx.get_session())
                      ? type_ctx.get_session()->get_actual_nls_length_semantics() : LS_BYTE);
    if (OB_FAIL(calc_result_meta_for_comparison(type, types, param_num, type_ctx.get_coll_type(),
                                                default_length_semantics))) {
      LOG_WARN("calc result meta for comparison failed");
    }
    // can't cast origin parameters.
    for (int64_t i = 0; i < param_num; i++) {
      types[i].set_calc_meta(types[i].get_obj_meta());
    }
    if (all_integer && type.is_integer_type()) {
      type.set_calc_type(ObNullType);
    } else {
      for (int64_t i = 0; i < param_num; i++) {
        if (ob_is_enum_or_set_type(types[i].get_type())) {
          types[i].set_calc_type(type.get_calc_type());
        }
      }
    }
  }
  return ret;
}

int ObExprLeastGreatest::cg_expr(ObExprCGCtx &op_cg_ctx,
                                 const ObRawExpr &raw_expr,
                                 ObExpr &rt_expr) const
{
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  const uint32_t param_num = rt_expr.arg_cnt_;
  bool is_oracle_mode = lib::is_oracle_mode();
  if (OB_UNLIKELY(is_oracle_mode && param_num < 1)
    || OB_UNLIKELY(!is_oracle_mode && param_num < 2)
    || OB_ISNULL(rt_expr.args_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("args_ is null or too few arguments", K(ret), K(rt_expr.args_), K(param_num));
  } else if (OB_ISNULL(op_cg_ctx.session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else {
    const bool string_result = ob_is_string_or_lob_type(rt_expr.datum_meta_.type_);
    for (int i = 0; OB_SUCC(ret) && i < param_num; ++i) {
      if (OB_ISNULL(rt_expr.args_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child of expr is null", K(ret), K(i));
      } else if (OB_ISNULL(rt_expr.args_[i]->basic_funcs_)
          || OB_ISNULL(rt_expr.args_[i]->basic_funcs_->null_first_cmp_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("basic func of or cmp func is null", K(ret), K(rt_expr.args_[i]->basic_funcs_));
      } else if (is_oracle_mode
                && rt_expr.args_[i]->datum_meta_.type_ != ObNullType
                && rt_expr.datum_meta_.type_ != ObNullType
                && (rt_expr.args_[i]->datum_meta_.type_ != rt_expr.datum_meta_.type_
                    || (string_result && rt_expr.args_[i]->datum_meta_.cs_type_ != rt_expr.datum_meta_.cs_type_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("all param meta should be same", K(ret), K(i), K(rt_expr.args_[i]->datum_meta_),
                 K(rt_expr.datum_meta_));
      }
    }
    if (OB_SUCC(ret)) {
      if (T_FUN_SYS_GREATEST == type_) {
        rt_expr.eval_func_ = ObExprGreatest::calc_greatest;
      } else {
        rt_expr.eval_func_ = ObExprLeast::calc_least;
      }
      const ObObjMeta &cmp_meta = result_type_.get_calc_meta();
      const bool is_explicit_cast = false;
      const int32_t result_flag = 0;
      ObCastMode cm = CM_NONE;
      if (!is_oracle_mode && !cmp_meta.is_null()) {
        DatumCastExtraInfo *info = OB_NEWx(DatumCastExtraInfo, op_cg_ctx.allocator_, *(op_cg_ctx.allocator_), type_);
        if (OB_ISNULL(info)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc memory failed", K(ret));
        } else if (OB_FAIL(ObSQLUtils::get_default_cast_mode(is_explicit_cast, result_flag,
                                                             op_cg_ctx.session_, cm))) {
          LOG_WARN("get default cast mode failed", K(ret));
        } else if (CS_TYPE_INVALID == cmp_meta.get_collation_type()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("compare cs type is invalid", K(ret), K(cmp_meta));
        } else {
          info->cmp_meta_.type_ = cmp_meta.get_type();
          info->cmp_meta_.cs_type_ = cmp_meta.get_collation_type();
          info->cmp_meta_.scale_ = cmp_meta.get_scale();
          info->cm_ = cm;
          rt_expr.extra_info_ = info;

          bool has_lob_header = false;
          if (is_lob_storage(info->cmp_meta_.type_)) {
            if (op_cg_ctx.session_->get_exec_min_cluster_version() >= CLUSTER_VERSION_4_1_0_0) {
              has_lob_header = true;
            }
          }
          ObDatumCmpFuncType cmp_func = ObDatumFuncs::get_nullsafe_cmp_func(cmp_meta.get_type(),
                                                                            cmp_meta.get_type(),
                                                                            NULL_LAST,
                                                                            cmp_meta.get_collation_type(),
                                                                            info->cmp_meta_.scale_,
                                                                            lib::is_oracle_mode(),
                                                                            has_lob_header);
          if (OB_ISNULL(cmp_func)) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid cmp type of params", K(ret), K(cmp_meta));
          } else if (OB_ISNULL(rt_expr.inner_functions_ =
                            reinterpret_cast<void**>(op_cg_ctx.allocator_->alloc(sizeof(void*))))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("alloc memory failed", K(ret));
          } else {
            rt_expr.inner_func_cnt_ = 1;
            rt_expr.inner_functions_[0] = reinterpret_cast<void*>(cmp_func);
          }
        }
      }
      LOG_DEBUG("least cg", K(result_type_));
    }
  }
  return ret;
}

int ObExprLeastGreatest::cast_param(const ObExpr &src_expr, ObEvalCtx &ctx,
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

int ObExprLeastGreatest::cast_result(const ObExpr &src_expr, const ObExpr &dst_expr, ObEvalCtx &ctx,
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

int ObExprLeastGreatest::calc_mysql(const ObExpr &expr, ObEvalCtx &ctx,
                                    ObDatum &expr_datum, bool least)
{
  int ret = OB_SUCCESS;
  uint32_t param_num = expr.arg_cnt_;
  bool has_null = false;
  int64_t cmp_res_offset = 0;
  //这里要对参数进行按需求值，发现有参数值为null的话不再计算后面的参数的值
  //create table t(c1 int, c2 varchar(10));
  //insert into t values(null, 'a');
  //select least(c2, c1) from t; mysql会报warning, oracle会报错
  //select least(c1, c2) from t; mysql不会报warning, oracle正常输出null
  for (int i = 0; OB_SUCC(ret) && !has_null && i < param_num; ++i) {
    ObDatum *tmp_datum = NULL;
    if (OB_FAIL(expr.args_[i]->eval(ctx, tmp_datum))) {
      LOG_WARN("eval param value failed", K(ret), K(i));
    } else {
      if (tmp_datum->is_null()) {
        has_null = true;
        expr_datum.set_null();
      }
    }
  }
  // if all params and integer and result type is also integer, there is no inner_function of least expr.
  // otherwise, inner_func_cnt_ will be one. It stores the cmp function for parameters.
  if (!has_null && OB_SUCC(ret)) {
    const bool all_integer = 0 == expr.inner_func_cnt_;
    // compare all params.
    if (all_integer) {
      if (ob_is_int_tc(expr.datum_meta_.type_)) {
        int64_t minmax_value = expr.locate_param_datum(ctx, 0).get_int();
        for (int i = 1; i < param_num; ++i) {
          int64_t new_value =  expr.locate_param_datum(ctx, i).get_int();
          if (least != (minmax_value < new_value)) {
            minmax_value = new_value;
          }
        }
        expr_datum.set_int(minmax_value);
      } else {
        uint64_t minmax_value = expr.locate_param_datum(ctx, 0).get_uint();
        for (int i = 1; i < param_num; ++i) {
          uint64_t new_value =  expr.locate_param_datum(ctx, i).get_uint();
          if (least != (minmax_value < new_value)) {
            minmax_value = new_value;
          }
        }
        expr_datum.set_uint(minmax_value);
      }
    } else if (OB_ISNULL(expr.extra_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("extra info is null", K(ret));
    } else {
      DatumCastExtraInfo *cast_info = static_cast<DatumCastExtraInfo *>(expr.extra_info_);
      int res_idx = 0;
      ObDatum minmax_datum;
      ObTempExprCtx::TempAllocGuard tmp_alloc_guard(ctx);
      ObDatumCmpFuncType cmp_func = reinterpret_cast<ObDatumCmpFuncType>(expr.inner_functions_[0]);
      if (OB_SUCC(ret) &&
          OB_FAIL(cast_param(*expr.args_[0], ctx, cast_info->cmp_meta_, cast_info->cm_,
                             tmp_alloc_guard.get_allocator(), minmax_datum))) {
        LOG_WARN("cast param failed", K(ret));
      }
      for (int i = 1; OB_SUCC(ret) && i < param_num; ++i) {
        ObDatum cur_datum;
        if (OB_FAIL(cast_param(*expr.args_[i], ctx, cast_info->cmp_meta_, cast_info->cm_,
                              tmp_alloc_guard.get_allocator(), cur_datum))) {
          LOG_WARN("cast param failed", K(ret));
        } else {
          int cmp_res = 0;
          if (OB_FAIL(cmp_func(minmax_datum, cur_datum, cmp_res))) {
            LOG_WARN("compare failed", K(ret));
          } else if((!least && cmp_res < 0) || (least && cmp_res > 0)) {
            res_idx = i;
            minmax_datum = cur_datum;
          }
        }
      }
      // ok, we got the least / greatest param.
      if (OB_SUCC(ret)) {
        if (OB_FAIL(cast_result(*expr.args_[res_idx], expr, ctx, cast_info->cm_, expr_datum))) {
          LOG_WARN("cast result failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObExprLeastGreatest::calc_oracle(const ObExpr &expr, ObEvalCtx &ctx,
                                     ObDatum &expr_datum, bool least)
{
  int ret = OB_SUCCESS;
  uint32_t param_num = expr.arg_cnt_;
  bool has_null = false;
  int64_t cmp_res_offset = 0;
  for (int i = 0; OB_SUCC(ret) && !has_null && i < param_num; ++i) {
    ObDatum *tmp_datum = NULL;
    if (OB_FAIL(expr.args_[i]->eval(ctx, tmp_datum))) {
      LOG_WARN("eval param value failed", K(ret), K(i));
    } else {
      if (tmp_datum->is_null()) {
        has_null = true;
        expr_datum.set_null();
      }
    }
  }
  if (!has_null && OB_SUCC(ret)) {
    // compare all params.
    int res_idx = 0;
    ObDatum *minmax_param = &expr.locate_param_datum(ctx, res_idx);
    for (int i = 1; OB_SUCC(ret) && i < param_num; ++i) {
      ObDatumCmpFuncType cmp_func = expr.args_[res_idx]->basic_funcs_->null_first_cmp_;
      ObDatum *cur_param = &expr.locate_param_datum(ctx, i);
      int cmp_res = 0;
      if (OB_FAIL(cmp_func(*minmax_param, *cur_param,cmp_res))) {
        LOG_WARN("compare failed", K(ret));
      } else if((!least && cmp_res < 0) || (least && cmp_res > 0)) {
        res_idx = i;
        minmax_param = static_cast<ObDatum *>(&expr.locate_param_datum(ctx, res_idx));
      }
    }
    ObDatum *res_datum = nullptr;
    if (OB_FAIL(expr.args_[res_idx]->eval(ctx, res_datum))) {
      LOG_WARN("eval param value failed", K(ret), K(res_idx));
    } else {
      ObDatum &dst_datum = static_cast<ObDatum &>(expr_datum);
      dst_datum = *res_datum;
    }
  }
  return ret;
}

ObExprLeast::ObExprLeast(common::ObIAllocator &alloc)
    : ObExprLeastGreatest(alloc,
                           T_FUN_SYS_LEAST,
                           N_LEAST,
                           MORE_THAN_ZERO)
{
}

//same type params
int ObExprLeast::calc_least(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  if (is_oracle_mode()) {
    ret = ObExprLeastGreatest::calc_oracle(expr, ctx, expr_datum, true);
  } else {
    ret = ObExprLeastGreatest::calc_mysql(expr, ctx, expr_datum, true);
  }
  return ret;
}


}
}

