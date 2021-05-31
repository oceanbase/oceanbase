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

#include "sql/engine/expr/ob_expr_cmp_func.h"

#include "share/datum/ob_datum_cmp_func_def.h"
#include "share/datum/ob_datum_funcs.h"
#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase {
namespace sql {
using namespace common;
using namespace common::cmp_func_helper;

template <ObCmpOp cmp_op>
constexpr int get_cmp_ret(const int)
{
  return false;
};

template <>
constexpr int get_cmp_ret<CO_EQ>(const int ret)
{
  return ret == 0;
}
template <>
constexpr int get_cmp_ret<CO_LE>(const int ret)
{
  return ret <= 0;
}
template <>
constexpr int get_cmp_ret<CO_LT>(const int ret)
{
  return ret < 0;
}
template <>
constexpr int get_cmp_ret<CO_GE>(const int ret)
{
  return ret >= 0;
}
template <>
constexpr int get_cmp_ret<CO_GT>(const int ret)
{
  return ret > 0;
}
template <>
constexpr int get_cmp_ret<CO_NE>(const int ret)
{
  return ret != 0;
}
template <>
constexpr int get_cmp_ret<CO_CMP>(const int ret)
{
  return ret;
}

template <ObObjType type1, ObObjType type2, ObCmpOp cmp_op>
struct ObDatumExprCmpByType : public ObDatumCmpHelperByType<type1, type2> {
  inline static int cmp(const ObDatum& datum1, const ObDatum& datum2)
  {
    int cmp_res = ObDatumCmpHelperByType<type1, type2>::cmp(datum1, datum2);
    return get_cmp_ret<cmp_op>(cmp_res);
  }
};

template <ObObjType type1, ObObjType type2, ObCmpOp cmp_op>
struct ObRelationalExprEvalFunc : public ObDatumExprCmpByType<type1, type2, cmp_op> {
  inline static int eval(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
  {
    int ret = OB_SUCCESS;
    ObDatum* left = NULL;
    ObDatum* right = NULL;
    bool contain_null = false;
    if (OB_FAIL(ObRelationalExprOperator::get_comparator_operands(expr, ctx, left, right, expr_datum, contain_null))) {
      LOG_WARN("failed to eval args", K(ret));
    } else if (!contain_null) {
      if (OB_ISNULL(left) || OB_ISNULL(right)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid operands", K(ret), K(left), K(right));
      } else {
        int cmp_ret = ObDatumExprCmpByType<type1, type2, cmp_op>::cmp(*left, *right);
        expr_datum.set_int(cmp_ret);
      }
    }
    return ret;
  }
};

template <ObCollationType cs_type, bool calc_with_end_space, ObCmpOp cmp_op>
struct ObRelationalStrExprEvalFunc : public ObDatumStrCmpCore<cs_type, calc_with_end_space> {
  inline static int eval(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
  {
    int ret = OB_SUCCESS;
    ObDatum* left = NULL;
    ObDatum* right = NULL;
    bool contain_null = false;
    if (OB_FAIL(ObRelationalExprOperator::get_comparator_operands(expr, ctx, left, right, expr_datum, contain_null))) {
      LOG_WARN("failed to eval args", K(ret));
    } else if (!contain_null) {
      if (OB_ISNULL(left) || OB_ISNULL(right)) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        int cmp_res = ObDatumStrCmpCore<cs_type, calc_with_end_space>::cmp(*left, *right);
        expr_datum.set_int(get_cmp_ret<cmp_op>(cmp_res));
      }
    }
    return ret;
  }
};

static ObExpr::EvalFunc EVAL_CMP_FUNCS[ObMaxType][ObMaxType][CO_MAX];
static ObDatumCmpFuncType DATUM_CMP_FUNCS[ObMaxType][ObMaxType];
static ObExpr::EvalFunc EVAL_STR_CMP_FUNCS[CS_TYPE_MAX][CO_MAX][2];
static ObDatumCmpFuncType DATUM_STR_CMP_FUNCS[CS_TYPE_MAX][2];

template <int X, int Y>
struct ExprCmpFuncIniter {
  template <ObCmpOp cmp_op>
  using EvalCmp = ObRelationalExprEvalFunc<static_cast<ObObjType>(X), static_cast<ObObjType>(Y), cmp_op>;
  using DatumCmp = ObDatumExprCmpByType<static_cast<ObObjType>(X), static_cast<ObObjType>(Y), CO_CMP>;
  static constexpr ObObjTypeClass tc1_ = ObObjTypeTraits<static_cast<ObObjType>(X)>::tc_;
  static constexpr ObObjTypeClass tc2_ = ObObjTypeTraits<static_cast<ObObjType>(Y)>::tc_;
  static void init_array()
  {
    EVAL_CMP_FUNCS[X][Y][CO_LE] = EvalCmp<CO_LE>::defined_ ? &EvalCmp<CO_LE>::eval : NULL;
    EVAL_CMP_FUNCS[X][Y][CO_LT] = EvalCmp<CO_LT>::defined_ ? &EvalCmp<CO_LT>::eval : NULL;
    EVAL_CMP_FUNCS[X][Y][CO_EQ] = EvalCmp<CO_EQ>::defined_ ? &EvalCmp<CO_EQ>::eval : NULL;
    EVAL_CMP_FUNCS[X][Y][CO_GE] = EvalCmp<CO_GE>::defined_ ? &EvalCmp<CO_GE>::eval : NULL;
    EVAL_CMP_FUNCS[X][Y][CO_GT] = EvalCmp<CO_GT>::defined_ ? &EvalCmp<CO_GT>::eval : NULL;
    EVAL_CMP_FUNCS[X][Y][CO_NE] = EvalCmp<CO_NE>::defined_ ? &EvalCmp<CO_NE>::eval : NULL;
    EVAL_CMP_FUNCS[X][Y][CO_CMP] = EvalCmp<CO_CMP>::defined_ ? &EvalCmp<CO_CMP>::eval : NULL;

    DATUM_CMP_FUNCS[X][Y] = DatumCmp::defined_ ? &DatumCmp::cmp : NULL;
  }
};

template <int X, int Y>
struct StrExprFuncIniter {
  template <bool calc_with_end_space>
  using EvalCmp =
      ObRelationalStrExprEvalFunc<static_cast<ObCollationType>(X), calc_with_end_space, static_cast<ObCmpOp>(Y)>;
  static void init_array()
  {
    EVAL_STR_CMP_FUNCS[X][Y][0] = EvalCmp<0>::defined_ ? EvalCmp<0>::eval : NULL;
    EVAL_STR_CMP_FUNCS[X][Y][1] = EvalCmp<1>::defined_ ? EvalCmp<1>::eval : NULL;
  }
};

template <int X>
struct DatumStrExprCmpIniter {
  template <bool calc_with_end_space>
  using DatumCmp = ObDatumStrCmpCore<static_cast<ObCollationType>(X), calc_with_end_space>;
  static void init_array()
  {
    DATUM_STR_CMP_FUNCS[X][0] = DatumCmp<0>::defined_ ? DatumCmp<0>::cmp : NULL;
    DATUM_STR_CMP_FUNCS[X][1] = DatumCmp<1>::defined_ ? DatumCmp<1>::cmp : NULL;
  }
};

int init_datum_ret = Ob2DArrayConstIniter<ObMaxType, ObMaxType, ExprCmpFuncIniter>::init();
int init_str_ret = Ob2DArrayConstIniter<CS_TYPE_MAX, CO_MAX, StrExprFuncIniter>::init();
int init_datum_str_ret = ObArrayConstIniter<CS_TYPE_MAX, DatumStrExprCmpIniter>::init();

ObExpr::EvalFunc ObExprCmpFuncsHelper::get_eval_expr_cmp_func(const ObObjType type1, const ObObjType type2,
    const ObCmpOp cmp_op, const bool is_oracle_mode, const ObCollationType cs_type)
{
  OB_ASSERT(type1 >= ObNullType && type1 < ObMaxType);
  OB_ASSERT(type2 >= ObNullType && type2 < ObMaxType);
  OB_ASSERT(cmp_op >= CO_EQ && cmp_op <= CO_MAX);

  ObObjTypeClass tc1 = ob_obj_type_class(type1);
  ObObjTypeClass tc2 = ob_obj_type_class(type2);
  ObExpr::EvalFunc func_ptr = NULL;
  if (OB_UNLIKELY(ob_is_invalid_cmp_op(cmp_op)) ||
      OB_UNLIKELY(ob_is_invalid_obj_tc(tc1) || OB_UNLIKELY(ob_is_invalid_obj_tc(tc2)))) {
    func_ptr = NULL;
  } else if (!ObDatumFuncs::is_string_type(type1) || !ObDatumFuncs::is_string_type(type2)) {
    func_ptr = EVAL_CMP_FUNCS[type1][type2][cmp_op];
  } else {
    OB_ASSERT(cs_type > CS_TYPE_INVALID && cs_type < CS_TYPE_MAX);
    int64_t calc_with_end_space_idx = (is_calc_with_end_space(type1, type2, is_oracle_mode, cs_type, cs_type) ? 1 : 0);
    func_ptr = EVAL_STR_CMP_FUNCS[cs_type][cmp_op][calc_with_end_space_idx];
  }
  return func_ptr;
}

DatumCmpFunc ObExprCmpFuncsHelper::get_datum_expr_cmp_func(
    const ObObjType type1, const ObObjType type2, const bool is_oracle_mode, const ObCollationType cs_type)
{
  OB_ASSERT(type1 >= ObNullType && type1 < ObMaxType);
  OB_ASSERT(type2 >= ObNullType && type2 < ObMaxType);

  ObDatumCmpFuncType func_ptr = NULL;
  if (!ObDatumFuncs::is_string_type(type1) || !ObDatumFuncs::is_string_type(type2)) {
    func_ptr = DATUM_CMP_FUNCS[type1][type2];
  } else {
    OB_ASSERT(cs_type > CS_TYPE_INVALID && cs_type < CS_TYPE_MAX);
    int64_t calc_with_end_space_idx = (is_calc_with_end_space(type1, type2, is_oracle_mode, cs_type, cs_type) ? 1 : 0);
    func_ptr = DATUM_STR_CMP_FUNCS[cs_type][calc_with_end_space_idx];
  }
  return func_ptr;
}

// register function serialization

// EVAL_CMP_FUNCS and DATUM_CMP_FUNCS is two dimension array, need to convert to index stable
// one dimension array first.

static_assert(7 == CO_MAX, "unexpected size");
void* g_ser_eval_cmp_funcs[ObMaxType * ObMaxType * 7];
static_assert(sizeof(g_ser_eval_cmp_funcs) == sizeof(EVAL_CMP_FUNCS), "unexpected size");
bool g_ser_eval_cmp_funcs_init = ObFuncSerialization::convert_NxN_array(
    g_ser_eval_cmp_funcs, reinterpret_cast<void**>(EVAL_CMP_FUNCS), ObMaxType, 7, 0, 7);
REG_SER_FUNC_ARRAY(OB_SFA_RELATION_EXPR_EVAL, g_ser_eval_cmp_funcs, sizeof(g_ser_eval_cmp_funcs) / sizeof(void*));

void* g_ser_datum_cmp_funcs[ObMaxType * ObMaxType];
static_assert(sizeof(g_ser_datum_cmp_funcs) == sizeof(DATUM_CMP_FUNCS), "unexpected size");
bool g_ser_datum_cmp_funcs_init =
    ObFuncSerialization::convert_NxN_array(g_ser_datum_cmp_funcs, reinterpret_cast<void**>(DATUM_CMP_FUNCS), ObMaxType);
REG_SER_FUNC_ARRAY(OB_SFA_DATUM_CMP, g_ser_datum_cmp_funcs, sizeof(g_ser_datum_cmp_funcs) / sizeof(void*));

static_assert(7 == CO_MAX && CS_TYPE_MAX * 7 * 2 == sizeof(EVAL_STR_CMP_FUNCS) / sizeof(void*), "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_RELATION_EXPR_EVAL_STR, EVAL_STR_CMP_FUNCS, sizeof(EVAL_STR_CMP_FUNCS) / sizeof(void*));

static_assert(CS_TYPE_MAX * 2 == sizeof(DATUM_STR_CMP_FUNCS) / sizeof(void*), "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_DATUM_CMP_STR, DATUM_STR_CMP_FUNCS, sizeof(DATUM_STR_CMP_FUNCS) / sizeof(void*));

}  // namespace sql
}  // end namespace oceanbase
