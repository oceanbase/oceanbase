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

namespace oceanbase {
using namespace oceanbase::common;
namespace sql {

ObExprNullif::ObExprNullif(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_NULLIF, N_NULLIF, MORE_THAN_ONE, NOT_ROW_DIMENSION),
      first_param_can_be_null_(true)
{}

// in engine 3.0, we have 3 copy of params
// eg:
//  nullif(e0, e1) -> nullif(e0, e1, e2, e3, e4, e5)
//    1. e0 and e1 is used for type deduce, we cannot add cast on e0 or e1,
//       because type deduce maybe unstable.
//    2. cast expr will add on e2 and e3
//    3. e4 is used for result. e5 is useless
int ObExprNullif::calc_result_typeN(
    ObExprResType& type, ObExprResType* types, int64_t param_num, ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo* session = type_ctx.get_session();
  CK(OB_NOT_NULL(session));
  if (OB_SUCC(ret)) {
    if (session->use_static_typing_engine()) {
      CK(6 == param_num);
      CK(lib::is_mysql_mode());
      if (types[0].is_null()) {
        // eval_nullif() just return null, no need to set cmp type
        type.set_type(ObCharType);
        type.set_collation_level(types[0].get_collation_level());
        type.set_default_collation_type();
        types[4].set_calc_type(ObCharType);
        types[4].set_calc_collation_type(ObCharset::get_system_collation());
      } else {
        // setup cmp type
        ObExprResType cmp_type;
        OZ(se_deduce_type(type, cmp_type, types[0], types[1], type_ctx));
        OX(types[2].set_calc_meta(cmp_type.get_calc_meta()));
        OX(types[2].set_calc_accuracy(cmp_type.get_calc_accuracy()));
        OX(types[3].set_calc_meta(cmp_type.get_calc_meta()));
        OX(types[3].set_calc_accuracy(cmp_type.get_calc_accuracy()));

        // setup res type
        OX(types[4].set_calc_meta(type.get_obj_meta()));
        OX(types[4].set_calc_accuracy(type.get_calc_accuracy()));
      }
    } else {
      CK(2 == param_num);
      CK(lib::is_mysql_mode());
      OZ(deduce_type(type, types[0], types[1], type_ctx));
    }
  }
  return ret;
}

int ObExprNullif::deduce_type(
    ObExprResType& type, ObExprResType& type1, ObExprResType& type2, ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  ObExprResType cmp_type;
  const ObSQLSessionInfo* session = type_ctx.get_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else if (OB_FAIL(calc_cmp_type2(cmp_type, type1, type2, type_ctx.get_coll_type()))) {
    LOG_WARN("failed to calc cmp type", K(ret), K(type1), K(type2));
  } else {
    type.set_type(type1.get_type());
    type.set_accuracy(type1.get_accuracy());
    if (ob_is_string_type(type.get_type()) || ob_is_enumset_tc(type.get_type())) {
      ObCollationLevel res_cs_level = CS_LEVEL_INVALID;
      ObCollationType res_cs_type = CS_TYPE_INVALID;
      if (OB_FAIL(ObCharset::aggregate_collation(type1.get_collation_level(),
              type1.get_collation_type(),
              type2.get_collation_level(),
              type2.get_collation_type(),
              res_cs_level,
              res_cs_type))) {
        LOG_WARN("failed to calc collation", K(ret));
      } else {
        type.set_collation_level(res_cs_level);
        type.set_collation_type(res_cs_type);
        // deduce length
        if (type.get_collation_type() == CS_TYPE_BINARY) {
          ObLength len_in_byte = -1;
          if (OB_FAIL(type1.get_length_for_meta_in_bytes(len_in_byte))) {
            LOG_WARN("get length in bytes failed", K(ret), K(type1));
          } else {
            type.set_length(len_in_byte);
          }
        } else {
          type.set_length(type1.get_length());
        }
      }
    } else if (type.is_null()) {
      type.set_type(ObCharType);
      type.set_default_collation_type();
    }
    type.set_calc_type(cmp_type.get_calc_type());
    type.set_calc_collation(cmp_type);

    if (OB_SUCC(ret)) {
      if (ob_is_enumset_tc(type1.get_type()) || ob_is_enumset_tc(type2.get_type())) {
        ObObjType calc_type = enumset_calc_types_[OBJ_TYPE_TO_CLASS[cmp_type.get_calc_type()]];
        if (OB_UNLIKELY(ObMaxType == calc_type)) {
          ret = OB_ERR_UNEXPECTED;
          SQL_ENG_LOG(WARN, "invalid type of parameter ", K(type1), K(type2), K(cmp_type), K(ret));
        } else if (ObVarcharType == calc_type) {
          if (ob_is_enumset_tc(type1.get_type())) {
            type1.set_calc_type(calc_type);
          }
          if (ob_is_enumset_tc(type2.get_type())) {
            type2.set_calc_type(calc_type);
          }
        }
      }
    }
  }
  return ret;
}

int ObExprNullif::calc_resultN(ObObj& result, const ObObj* objs_stack, int64_t param_num, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  EXPR_DEFINE_CMP_CTX(result_type_.get_calc_meta(), true, expr_ctx);
  EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
  ObObj cmp;
  if (OB_UNLIKELY(2 != param_num && 6 != param_num) || OB_ISNULL(objs_stack)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(param_num), K(objs_stack));
  } else {
    const ObObj& obj1 = objs_stack[0];
    const ObObj& obj2 = objs_stack[1];
    if (OB_FAIL(ObExprEqual::calc(cmp, obj1, obj2, cmp_ctx, cast_ctx))) {
      LOG_WARN("failed to compare objects", K(ret), K(obj1), K(obj2));
    } else if (cmp.is_true()) {
      result.set_null();
    } else if (OB_UNLIKELY(ob_is_enumset_inner_tc(obj1.get_type()))) {
      ObObj obj_ret;
      ObEnumSetInnerValue inner_value;
      if (OB_FAIL(obj1.get_enumset_inner_value(inner_value))) {
        LOG_WARN("failed to get enumset_inner_value", K(ret), K(obj1), K(obj2));
      } else if (ObEnumInnerType == obj1.get_type()) {
        obj_ret.set_enum(inner_value.numberic_value_);
      } else {
        obj_ret.set_set(inner_value.numberic_value_);
      }
      result = obj_ret;
    } else {
      result = obj1;
    }
  }
  return ret;
}

int ObExprNullif::se_deduce_type(ObExprResType& type, ObExprResType& cmp_type, const ObExprResType& type1,
    const ObExprResType& type2, ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  type.set_meta(type1.get_obj_meta());
  type.set_accuracy(type1.get_accuracy());
  if (ob_is_string_type(type.get_type()) || ob_is_enumset_tc(type.get_type())) {
    ObCollationLevel res_cs_level = CS_LEVEL_INVALID;
    ObCollationType res_cs_type = CS_TYPE_INVALID;
    OZ(ObCharset::aggregate_collation(type1.get_collation_level(),
        type1.get_collation_type(),
        type2.get_collation_level(),
        type2.get_collation_type(),
        res_cs_level,
        res_cs_type));
    if (OB_SUCC(ret)) {
      type.set_collation_level(res_cs_level);
      type.set_collation_type(res_cs_type);
      // deduce length
      if (type.get_collation_type() == CS_TYPE_BINARY) {
        ObLength len_in_byte = -1;
        OZ(type1.get_length_for_meta_in_bytes(len_in_byte));
        OX(type.set_length(len_in_byte));
      } else {
        type.set_length(type1.get_length());
      }
    }
  }

  OZ(calc_cmp_type2(cmp_type, type1, type2, type_ctx.get_coll_type()));
  if (OB_SUCC(ret)) {
    if (ob_is_enumset_tc(type1.get_type()) || ob_is_enumset_tc(type2.get_type())) {
      ObObjType calc_type = enumset_calc_types_[OBJ_TYPE_TO_CLASS[cmp_type.get_calc_type()]];
      CK(ObMaxType != calc_type);
      if (OB_SUCC(ret) && ObVarcharType == calc_type) {
        cmp_type.set_calc_type(calc_type);
        cmp_type.set_calc_collation_type(ObCharset::get_system_collation());
      }
    }
  }
  return ret;
}

int ObExprNullif::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(raw_expr);
  CK(6 == rt_expr.arg_cnt_);
  const uint32_t param_num = rt_expr.arg_cnt_;
  const uint32_t real_param_num = param_num / 3;
  for (int64_t i = 0; OB_SUCC(ret) && i < real_param_num; i++) {
    if (OB_FAIL(ObStaticEngineExprCG::replace_var_rt_expr(
            rt_expr.args_[i], rt_expr.args_[i + real_param_num], &rt_expr, i + real_param_num))) {
      LOG_WARN("replace var rt expr failed", K(ret), K(i), K(raw_expr));
    } else if (OB_FAIL(ObStaticEngineExprCG::replace_var_rt_expr(
                   rt_expr.args_[i], rt_expr.args_[i + 2 * real_param_num], &rt_expr, i + 2 * real_param_num))) {
      LOG_WARN("replace var rt_expr failed", K(ret), K(i), K(raw_expr));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (ObNullType == rt_expr.args_[0]->datum_meta_.type_) {
    OX(rt_expr.eval_func_ = eval_nullif);
  } else {
    OV(OB_NOT_NULL(rt_expr.inner_functions_ =
                       reinterpret_cast<void**>(expr_cg_ctx.allocator_->alloc(sizeof(DatumCmpFunc) * 1))),
        OB_ALLOCATE_MEMORY_FAILED);

    if (OB_SUCC(ret)) {
      DatumCmpFunc cmp_func = NULL;
      const ObDatumMeta& left_meta = rt_expr.args_[2]->datum_meta_;
      const ObDatumMeta& right_meta = rt_expr.args_[3]->datum_meta_;
      const ObCollationType cmp_cs_type = left_meta.cs_type_;
      CK(left_meta.cs_type_ == right_meta.cs_type_);
      CK(OB_NOT_NULL(cmp_func = ObExprCmpFuncsHelper::get_datum_expr_cmp_func(
                         left_meta.type_, right_meta.type_, lib::is_oracle_mode(), cmp_cs_type)));
      OX(rt_expr.inner_func_cnt_ = 1);
      OX(rt_expr.inner_functions_[0] = reinterpret_cast<void*>(cmp_func));
      OX(rt_expr.eval_func_ = eval_nullif);
    }
  }
  return ret;
}

int ObExprNullif::eval_nullif(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res)
{
  int ret = OB_SUCCESS;
  ObDatum* cmp_e0 = NULL;
  ObDatum* cmp_e1 = NULL;
  ObDatum* res_e = NULL;
  if (OB_FAIL(expr.args_[2]->eval(ctx, cmp_e0))) {
    LOG_WARN("eval param 0 failed", K(ret));
  } else if (cmp_e0->is_null()) {
    res.set_null();
  } else if (OB_UNLIKELY(1 != expr.inner_func_cnt_) || OB_ISNULL(expr.inner_functions_) ||
             OB_ISNULL(expr.inner_functions_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN(
        "unexpected param", K(ret), K(expr.inner_func_cnt_), KP(expr.inner_functions_), KP(expr.inner_functions_[0]));
  } else if (OB_FAIL(expr.args_[3]->eval(ctx, cmp_e1)) || OB_FAIL(expr.args_[4]->eval(ctx, res_e))) {
    LOG_WARN("eval param failed", K(ret), KP(cmp_e1), KP(res_e));
  } else if (cmp_e1->is_null()) {
    // e0 is not null, e1 is null
    res.set_datum(*res_e);
  } else {
    bool equal = (0 == reinterpret_cast<DatumCmpFunc>(expr.inner_functions_[0])(*cmp_e0, *cmp_e1));
    if (equal) {
      res.set_null();
    } else {
      res.set_datum(*res_e);
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
