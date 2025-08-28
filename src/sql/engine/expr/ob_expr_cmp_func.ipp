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

#ifndef _OCEANBASE_EXPR_CMP_FUNC_IPP_H_
#define _OCEANBASE_EXPR_CMP_FUNC_IPP_H_

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/expr/ob_expr_cmp_func.h"
#include "share/datum/ob_datum_cmp_func_def.h"
#include "share/datum/ob_datum_funcs.h"
// #include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_batch_eval_util.h"
// #include "share/ob_lob_access_utils.h"
#include "lib/udt/ob_array_type.h"
#include "sql/engine/ob_subschema_ctx.h"
#include "sql/engine/expr/ob_array_expr_utils.h"

namespace oceanbase
{
namespace sql
{
using namespace common;

#define IS_FIXED_DOUBLE                                         \
  !is_oracle_mode &&                                            \
  ob_is_double_type(type1) && ob_is_double_type(type2) &&       \
  SCALE_UNKNOWN_YET < scale1 && SCALE_UNKNOWN_YET < scale2 &&   \
  MAX(scale1, scale2) <= OB_MAX_DOUBLE_FLOAT_SCALE              \

template<ObCmpOp cmp_op>
constexpr int get_cmp_ret(const int)
{
  return 0;
};

template <> constexpr int get_cmp_ret<CO_EQ> (const int ret) { return ret == 0; }
template <> constexpr int get_cmp_ret<CO_LE> (const int ret) { return ret <= 0; }
template <> constexpr int get_cmp_ret<CO_LT> (const int ret) { return ret < 0; }
template <> constexpr int get_cmp_ret<CO_GE> (const int ret) { return ret >= 0; }
template <> constexpr int get_cmp_ret<CO_GT> (const int ret) { return ret > 0; }
template <> constexpr int get_cmp_ret<CO_NE> (const int ret) { return ret != 0; }
template <> constexpr int get_cmp_ret<CO_CMP> (const int ret) { return ret; }

template <typename DatumFunc, typename... Args>
int def_relational_eval_func(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum, Args &...args)
{
  int ret = OB_SUCCESS;
  ObDatum *l = NULL;
  ObDatum *r = NULL;
  bool contain_null = false;
  if (OB_FAIL(ObRelationalExprOperator::get_comparator_operands(
              expr, ctx, l, r, expr_datum, contain_null))) {
    LOG_WARN("failed to eval args", K(ret));
  } else if (!contain_null) {
    if (OB_ISNULL(l) || OB_ISNULL(r)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid operands", K(ret), K(l), K(r));
    } else {
      ret = DatumFunc()(expr_datum, *l, *r, args...);
    }
  }
  return ret;
};

template <typename DatumFunc, typename... Args>
int def_relational_eval_batch_func(BATCH_EVAL_FUNC_ARG_DECL, Args &...args)
{
  int ret = OB_SUCCESS;
  const static bool short_circuit = true;
  if (OB_FAIL(binary_operand_batch_eval(expr, ctx, skip, size, short_circuit))) {
    LOG_WARN("binary operand batch evaluate failed", K(ret), K(expr));
  } else {
    ret = call_functor_with_arg_iter<
        ObWrapArithOpNullCheck<DatumFunc>,
        ObDoArithBatchEval>(BATCH_EVAL_FUNC_ARG_LIST, args...);
  }
  return ret;
}

template <typename DatumFunc, ObCmpOp CMP_OP>
int def_oper_cmp_func(ObDatum &res, const ObDatum &l, const ObDatum &r)
{
  int cmp_ret = 0;
  int ret = DatumFunc::cmp(l, r, cmp_ret);
  if (OB_FAIL(ret)) {
    LOG_WARN("fail to compare", K(ret));
  } else {
    res.set_int(get_cmp_ret<CMP_OP>(cmp_ret));
  }
  return ret;
}

struct ObDummyRelationalFunc
{
  inline static int eval(const ObExpr &, ObEvalCtx &, ObDatum &) { return 0;};
  inline static int eval_batch(BATCH_EVAL_FUNC_ARG_DECL)
  {
    UNUSEDx(BATCH_EVAL_FUNC_ARG_LIST);
    return 0;
  }
};

template<bool, ObObjType L_T, ObObjType R_T, ObCmpOp CMP_OP>
struct ObRelationalTypeFunc {};

template<ObObjType L_T, ObObjType R_T, ObCmpOp CMP_OP>
struct ObRelationalTypeFunc<false, L_T, R_T, CMP_OP> : public ObDummyRelationalFunc {};

template<ObObjType L_T, ObObjType R_T, ObCmpOp CMP_OP>
struct ObRelationalTypeFunc<true, L_T, R_T, CMP_OP>
{
  struct DatumCmp
  {
    int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r) const
    {
      return def_oper_cmp_func<datum_cmp::ObDatumTypeCmp<L_T, R_T>, CMP_OP>(res, l, r);
    }
  };

  inline static int eval(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
  {
    return def_relational_eval_func<DatumCmp>(expr, ctx, expr_datum);
  }


  inline static int eval_batch(BATCH_EVAL_FUNC_ARG_DECL)
  {
    return def_relational_eval_batch_func<DatumCmp>(BATCH_EVAL_FUNC_ARG_LIST);
  }
};

template<bool, ObObjTypeClass L_TC, ObObjTypeClass R_TC, ObCmpOp CMP_OP>
struct ObRelationalTCFunc {};

template<ObObjTypeClass L_TC, ObObjTypeClass R_TC, ObCmpOp CMP_OP>
struct ObRelationalTCFunc<false, L_TC, R_TC, CMP_OP> : public ObDummyRelationalFunc {};

template<ObObjTypeClass L_TC, ObObjTypeClass R_TC, ObCmpOp CMP_OP>
struct ObRelationalTCFunc<true, L_TC, R_TC, CMP_OP>
{
  struct DatumCmp
  {
    int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r) const
    {
      return def_oper_cmp_func<datum_cmp::ObDatumTCCmp<L_TC, R_TC>, CMP_OP>(res, l, r);
    }
  };

  inline static int eval(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
  {
    return def_relational_eval_func<DatumCmp>(expr, ctx, expr_datum);
  }


  inline static int eval_batch(BATCH_EVAL_FUNC_ARG_DECL)
  {
    return def_relational_eval_batch_func<DatumCmp>(BATCH_EVAL_FUNC_ARG_LIST);
  }
};

template<bool, ObDecimalIntWideType l, ObDecimalIntWideType r, ObCmpOp cmp_op>
struct ObRelationalDecintFunc{};

template<ObDecimalIntWideType l, ObDecimalIntWideType r, ObCmpOp cmp_op>
struct ObRelationalDecintFunc<false, l, r, cmp_op>: ObDummyRelationalFunc {};

template<ObDecimalIntWideType lw, ObDecimalIntWideType rw, ObCmpOp cmp_op>
struct ObRelationalDecintFunc<true, lw, rw, cmp_op>
{
  struct ObDatumCmp
  {
    int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r) const
    {
      int cmp_ret = 0;
      int ret = datum_cmp::ObDecintCmp<lw, rw>::cmp(l, r, cmp_ret);
      if (OB_SUCC(ret)) {
        res.set_int(get_cmp_ret<cmp_op>(cmp_ret));
      }
      return ret;
    }
  };

  inline static int eval(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
  {
    return def_relational_eval_func<ObDatumCmp>(expr, ctx, expr_datum);
  }

  inline static int eval_batch(BATCH_EVAL_FUNC_ARG_DECL)
  {
    return def_relational_eval_batch_func<ObDatumCmp>(BATCH_EVAL_FUNC_ARG_LIST);
  }
};

template<bool, ObCollationType CS_TYPE, bool WITH_END_SPACE, ObCmpOp CMP_OP>
struct ObRelationalStrFunc{};

template<ObCollationType CS_TYPE, bool WITH_END_SPACE, ObCmpOp CMP_OP>
struct ObRelationalStrFunc<false, CS_TYPE, WITH_END_SPACE, CMP_OP> : ObDummyRelationalFunc {};

template<ObCollationType CS_TYPE, bool WITH_END_SPACE, ObCmpOp CMP_OP>
struct ObRelationalStrFunc<true, CS_TYPE, WITH_END_SPACE, CMP_OP>
{
  struct DatumCmp
  {
    int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r) const
    {
      return def_oper_cmp_func<datum_cmp::ObDatumStrCmp<CS_TYPE, WITH_END_SPACE>, CMP_OP>(res, l, r);
    }
  };

  inline static int eval(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
  {
    return def_relational_eval_func<DatumCmp>(expr, ctx, expr_datum);
  }


  // defined in ObRelationalExtraFunc
  // inline static int eval_batch(BATCH_EVAL_FUNC_ARG_DECL)
  // {
  //   return def_relational_eval_batch_func<DatumCmp>(BATCH_EVAL_FUNC_ARG_LIST);
  // }
};

template<typename T, bool WITH_END_SPACE, ObCmpOp CMP_OP>
struct ObNewRelationalStrFunc
{
  static OB_NOINLINE int eval(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
  {
    int ret = OB_SUCCESS;
    ObDatum *l = NULL;
    ObDatum *r = NULL;
    bool contain_null = false;
    int cmp_ret = 0;
    if (OB_FAIL(ObRelationalExprOperator::get_comparator_operands(
                expr, ctx, l, r, expr_datum, contain_null))) {
      LOG_WARN("failed to eval args", K(ret));
    } else if (!contain_null) {
      if (OB_ISNULL(l) || OB_ISNULL(r)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid operands", K(ret), K(l), K(r));
      } else if (OB_FAIL(T::cmp(*l, *r, cmp_ret,
                                expr.args_[0]->datum_meta_.cs_type_, WITH_END_SPACE))) {
        LOG_WARN("datum compare failed", K(*l), K(*r));
      } else {
        expr_datum.set_int(get_cmp_ret<CMP_OP>(cmp_ret));
      }
    }
    return ret;
  }
};

template <ObCollationType cs, bool WITH_END_SPACE, ObCmpOp CMP_OP>
struct ObStrRelationEvalWrap {
  static int eval(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
  {
    return ObNewRelationalStrFunc<datum_cmp::ObDatumStrCmpImpl,
           WITH_END_SPACE,
           CMP_OP>::eval(expr, ctx, expr_datum);
  }
};

template<bool, ObScale SCALE, ObCmpOp CMP_OP>
struct ObRelationFixedDoubleFunc{};

template<ObScale SCALE, ObCmpOp CMP_OP>
struct ObRelationFixedDoubleFunc<false, SCALE, CMP_OP> : ObDummyRelationalFunc {};

template<ObScale SCALE, ObCmpOp CMP_OP>
struct ObRelationFixedDoubleFunc<true, SCALE, CMP_OP>
{
  struct DatumCmp
  {
    int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r) const
    {
      return def_oper_cmp_func<datum_cmp::ObFixedDoubleCmp<SCALE>, CMP_OP>(res, l, r);
    }
  };

  inline static int eval(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
  {
    return def_relational_eval_func<DatumCmp>(expr, ctx, expr_datum);
  }

  inline static int eval_batch(BATCH_EVAL_FUNC_ARG_DECL)
  {
    return def_relational_eval_batch_func<DatumCmp>(BATCH_EVAL_FUNC_ARG_LIST);
  }
};

template<bool, ObCollationType CS_TYPE, bool WITH_END_SPACE, ObCmpOp CMP_OP>
struct ObRelationalTextFunc{};

template<ObCollationType CS_TYPE, bool WITH_END_SPACE, ObCmpOp CMP_OP>
struct ObRelationalTextFunc<false, CS_TYPE, WITH_END_SPACE, CMP_OP> : ObDummyRelationalFunc {};

template<ObCollationType CS_TYPE, bool WITH_END_SPACE, ObCmpOp CMP_OP>
struct ObRelationalTextFunc<true, CS_TYPE, WITH_END_SPACE, CMP_OP>
{
  struct DatumCmp
  {
    int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r) const
    {
      return def_oper_cmp_func<datum_cmp::ObDatumTextCmp<CS_TYPE, WITH_END_SPACE>, CMP_OP>(res, l, r);
    }
  };

  inline static int eval(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
  {
    return def_relational_eval_func<DatumCmp>(expr, ctx, expr_datum);
  }
};

template <ObCollationType cs, bool WITH_END_SPACE, ObCmpOp CMP_OP>
struct ObTextRelationEvalWrap {
  static int eval(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
  {
    return ObNewRelationalStrFunc<datum_cmp::ObDatumTextCmpImpl,
           WITH_END_SPACE,
           CMP_OP>::eval(expr, ctx, expr_datum);
  }
};

template<bool, ObCollationType CS_TYPE, bool WITH_END_SPACE, ObCmpOp CMP_OP>
struct ObRelationalTextStrFunc{};

template<ObCollationType CS_TYPE, bool WITH_END_SPACE, ObCmpOp CMP_OP>
struct ObRelationalTextStrFunc<false, CS_TYPE, WITH_END_SPACE, CMP_OP> : ObDummyRelationalFunc {};

template<ObCollationType CS_TYPE, bool WITH_END_SPACE, ObCmpOp CMP_OP>
struct ObRelationalTextStrFunc<true, CS_TYPE, WITH_END_SPACE, CMP_OP>
{
  struct DatumCmp
  {
    int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r) const
    {
      return def_oper_cmp_func<datum_cmp::ObDatumTextStringCmp<CS_TYPE, WITH_END_SPACE>, CMP_OP>(res, l, r);
    }
  };

  inline static int eval(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
  {
    return def_relational_eval_func<DatumCmp>(expr, ctx, expr_datum);
  }
};

template <ObCollationType cs, bool WITH_END_SPACE, ObCmpOp CMP_OP>
struct ObTextStrRelationEvalWrap {
  static int eval(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
  {
    return ObNewRelationalStrFunc<datum_cmp::ObDatumTextStringCmpImpl,
           WITH_END_SPACE,
           CMP_OP>::eval(expr, ctx, expr_datum);
  }
};


template<bool, ObCollationType CS_TYPE, bool WITH_END_SPACE, ObCmpOp CMP_OP>
struct ObRelationalStrTextFunc{};

template<ObCollationType CS_TYPE, bool WITH_END_SPACE, ObCmpOp CMP_OP>
struct ObRelationalStrTextFunc<false, CS_TYPE, WITH_END_SPACE, CMP_OP> : ObDummyRelationalFunc {};

template<ObCollationType CS_TYPE, bool WITH_END_SPACE, ObCmpOp CMP_OP>
struct ObRelationalStrTextFunc<true, CS_TYPE, WITH_END_SPACE, CMP_OP>
{
  struct DatumCmp
  {
    int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r) const
    {
      return def_oper_cmp_func<datum_cmp::ObDatumStringTextCmp<CS_TYPE, WITH_END_SPACE>, CMP_OP>(res, l, r);
    }
  };

  inline static int eval(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
  {
    return def_relational_eval_func<DatumCmp>(expr, ctx, expr_datum);
  }
};

template <ObCollationType cs, bool WITH_END_SPACE, ObCmpOp CMP_OP>
struct ObStrTextRelationEvalWrap {
  static int eval(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
  {
    return ObNewRelationalStrFunc<datum_cmp::ObDatumStringTextCmpImpl,
           WITH_END_SPACE,
           CMP_OP>::eval(expr, ctx, expr_datum);
  }
};


template<bool, bool HAS_LOB_HEADER, ObCmpOp CMP_OP>
struct ObRelationalJsonFunc{};

template<bool HAS_LOB_HEADER, ObCmpOp CMP_OP>
struct ObRelationalJsonFunc<false, HAS_LOB_HEADER, CMP_OP> : ObDummyRelationalFunc {};

template<bool HAS_LOB_HEADER, ObCmpOp CMP_OP>
struct ObRelationalJsonFunc<true, HAS_LOB_HEADER, CMP_OP>
{
  struct DatumCmp
  {
    int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r) const
    {
      return def_oper_cmp_func<datum_cmp::ObDatumJsonCmp<HAS_LOB_HEADER>, CMP_OP>(res, l, r);
    }
  };

  inline static int eval(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
  {
    return def_relational_eval_func<DatumCmp>(expr, ctx, expr_datum);
  }
};

template<bool, bool HAS_LOB_HEADER, ObCmpOp CMP_OP>
struct ObRelationalGeoFunc{};

template<bool HAS_LOB_HEADER, ObCmpOp CMP_OP>
struct ObRelationalGeoFunc<false, HAS_LOB_HEADER, CMP_OP> : ObDummyRelationalFunc {};

template<bool HAS_LOB_HEADER, ObCmpOp CMP_OP>
struct ObRelationalGeoFunc<true, HAS_LOB_HEADER, CMP_OP>
{
  struct DatumCmp
  {
    int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r) const
    {
      return def_oper_cmp_func<datum_cmp::ObDatumGeoCmp<HAS_LOB_HEADER>, CMP_OP>(res, l, r);
    }
  };

  inline static int eval(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
  {
    return def_relational_eval_func<DatumCmp>(expr, ctx, expr_datum);
  }
};

// cmp for collection
template<bool, bool HAS_LOB_HEADER, ObCmpOp CMP_OP>
struct ObRelationalCollectionFunc{};

template<bool HAS_LOB_HEADER, ObCmpOp CMP_OP>
struct ObRelationalCollectionFunc<false, HAS_LOB_HEADER, CMP_OP> : ObDummyRelationalFunc {};

template<bool HAS_LOB_HEADER, ObCmpOp CMP_OP>
struct ObRelationalCollectionFunc<true, HAS_LOB_HEADER, CMP_OP>
{
  struct DatumCmp
  {
    int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r, const ObExpr &expr, ObEvalCtx &ctx) const
    {
      int ret = OB_SUCCESS;
      int cmp_ret = 0;
      ObString left = l.get_string();
      ObString right = r.get_string();
      ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
      common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
      const uint16_t left_meta_id = expr.args_[0]->obj_meta_.get_subschema_id();
      const uint16_t right_meta_id = expr.args_[1]->obj_meta_.get_subschema_id();
      ObIArrayType *left_obj = NULL;
      ObIArrayType *right_obj = NULL;
      if (OB_FAIL(ObNestedArithOpBaseFunc::construct_param(tmp_allocator, ctx, left_meta_id, left, left_obj))) {
        LOG_WARN("construct left param failed", K(ret), K(left_meta_id));
      } else if (OB_FAIL(ObNestedArithOpBaseFunc::construct_param(tmp_allocator, ctx, right_meta_id, right, right_obj))) {
        LOG_WARN("construct left param failed", K(ret), K(left_meta_id));
      } else if (OB_FAIL(left_obj->compare(*right_obj, cmp_ret))) {
        LOG_WARN("array do compare failed", K(ret), K(left_meta_id), K(right_meta_id));
      } else {
        res.set_int(get_cmp_ret<CMP_OP>(cmp_ret));
      }
      return ret;
    }
  };

  inline static int eval(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
  {
    return def_relational_eval_func<DatumCmp>(expr, ctx, expr_datum, expr, ctx);
  }

  inline static int eval_batch(BATCH_EVAL_FUNC_ARG_DECL)
  {
    return def_relational_eval_batch_func<DatumCmp>(BATCH_EVAL_FUNC_ARG_LIST, expr, ctx);
  }
};

// define null, extend, string evaluate batch functions.
template<ObCmpOp CMP_OP>
struct ObRelationalExtraFunc
{
  struct NullExtendCmp
  {
    int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r, const ObExpr &expr) const
    {
      int ret = OB_SUCCESS;
      ObObjType lt = expr.args_[0]->datum_meta_.type_;
      ObObjType rt = expr.args_[1]->datum_meta_.type_;
      int cmp_ret = 0;
      if (ObExtendType == lt && ObExtendType == rt) {
        ret = datum_cmp::ObDatumTCCmp<ObExtendTC, ObExtendTC>::cmp(l, r, cmp_ret);
      } else if (ObExtendType == lt) {
        ret = datum_cmp::ObDatumTCCmp<ObExtendTC, ObIntTC>::cmp(l, r, cmp_ret);
      } else if (ObExtendType == rt) {
        ret = datum_cmp::ObDatumTCCmp<ObExtendTC, ObIntTC>::cmp(r, l, cmp_ret);
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("only extend type should reach here", K(ret));
      }
      if (OB_SUCC(ret)) {
        res.set_int(get_cmp_ret<CMP_OP>(cmp_ret));
      }
      return ret;
    }
  };

  inline static int null_extend_eval_batch(BATCH_EVAL_FUNC_ARG_DECL)
  {
    return def_relational_eval_batch_func<NullExtendCmp>(BATCH_EVAL_FUNC_ARG_LIST, expr);
  }

  struct StrCmp
  {
    int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r,
                   const ObCollationType cs_type, bool with_end_space) const
    {
      int ret = OB_SUCCESS;
      int cmp_ret = ObCharset::strcmpsp(cs_type, l.ptr_, l.len_, r.ptr_, r.len_, with_end_space);
      res.set_int(get_cmp_ret<CMP_OP>(cmp_ret > 0 ? 1 : (cmp_ret < 0 ? -1 : 0)));
      return ret;
    }
  };

  inline static int str_eval_batch(BATCH_EVAL_FUNC_ARG_DECL)
  {
    bool with_end_space = is_calc_with_end_space(
        expr.args_[0]->datum_meta_.type_,
        expr.args_[1]->datum_meta_.type_,
        lib::is_oracle_mode(),
        expr.args_[0]->datum_meta_.cs_type_,
        expr.args_[1]->datum_meta_.cs_type_);
    return def_relational_eval_batch_func<StrCmp>(BATCH_EVAL_FUNC_ARG_LIST,
                                                  expr.args_[0]->datum_meta_.cs_type_,
                                                  with_end_space);
  }

  struct TextCmp
  {
    int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r,
                   const ObCollationType cs_type, bool with_end_space) const
    {
      int ret = OB_SUCCESS;
      ObString l_data;
      ObString r_data;
      common::ObArenaAllocator allocator(ObModIds::OB_LOB_READER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
      ObTextStringIter l_instr_iter(ObLongTextType, cs_type, l.get_string(), true);
      ObTextStringIter r_instr_iter(ObLongTextType, cs_type, r.get_string(), true);
      if (OB_FAIL(l_instr_iter.init(0, NULL, &allocator))) {
        COMMON_LOG(WARN, "Lob: init left text str iter failed", K(ret), K(cs_type), K(l));
      } else if (OB_FAIL(l_instr_iter.get_full_data(l_data))) {
        COMMON_LOG(WARN, "Lob: get left text str iter full data failed ", K(ret), K(cs_type), K(l_instr_iter));
      } else if (OB_FAIL(r_instr_iter.init(0, NULL, &allocator))) {
        COMMON_LOG(WARN, "Lob: init right text str iter failed", K(ret), K(ret), K(r));
      } else if (OB_FAIL(r_instr_iter.get_full_data(r_data))) {
        COMMON_LOG(WARN, "Lob: get right text str iter full data failed ", K(ret), K(cs_type), K(r_instr_iter));
      } else {
        int cmp_ret = ObCharset::strcmpsp(cs_type, l_data.ptr(), l_data.length(), 
                                          r_data.ptr(), r_data.length(), with_end_space);
        res.set_int(get_cmp_ret<CMP_OP>(cmp_ret > 0 ? 1 : (cmp_ret < 0 ? -1 : 0)));
      }
      return ret;
    }
  };

  inline static int text_eval_batch(BATCH_EVAL_FUNC_ARG_DECL)
  {
    bool with_end_space = is_calc_with_end_space(
        expr.args_[0]->datum_meta_.type_,
        expr.args_[1]->datum_meta_.type_,
        lib::is_oracle_mode(),
        expr.args_[0]->datum_meta_.cs_type_,
        expr.args_[1]->datum_meta_.cs_type_);
    return def_relational_eval_batch_func<TextCmp>(BATCH_EVAL_FUNC_ARG_LIST,
                                                   expr.args_[0]->datum_meta_.cs_type_,
                                                   with_end_space);
  }

  struct TextStrCmp
  {
    int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r,
                   const ObCollationType cs_type, bool with_end_space) const
    {
      int ret = OB_SUCCESS;
      ObString l_data;
      common::ObArenaAllocator allocator(ObModIds::OB_LOB_READER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
      ObTextStringIter l_instr_iter(ObLongTextType, cs_type, l.get_string(), true);
      if (OB_FAIL(l_instr_iter.init(0, NULL, &allocator))) {
        COMMON_LOG(WARN, "Lob: init left text str iter failed", K(ret), K(cs_type), K(l));
      } else if (OB_FAIL(l_instr_iter.get_full_data(l_data))) {
        COMMON_LOG(WARN, "Lob: get left text str iter full data failed ", K(ret), K(cs_type), K(l_instr_iter));
      } else {
        int cmp_ret = ObCharset::strcmpsp(cs_type, l_data.ptr(), l_data.length(), 
                                          r.ptr_, r.len_, with_end_space);
        res.set_int(get_cmp_ret<CMP_OP>(cmp_ret > 0 ? 1 : (cmp_ret < 0 ? -1 : 0)));
      }
      return ret;
    }
  };

  inline static int text_str_eval_batch(BATCH_EVAL_FUNC_ARG_DECL)
  {
    bool with_end_space = is_calc_with_end_space(
        expr.args_[0]->datum_meta_.type_,
        expr.args_[1]->datum_meta_.type_,
        lib::is_oracle_mode(),
        expr.args_[0]->datum_meta_.cs_type_,
        expr.args_[1]->datum_meta_.cs_type_);
    return def_relational_eval_batch_func<TextStrCmp>(BATCH_EVAL_FUNC_ARG_LIST,
                                                      expr.args_[0]->datum_meta_.cs_type_,
                                                      with_end_space);
  }

  struct StrTextCmp
  {
    int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r,
                   const ObCollationType cs_type, bool with_end_space) const
    {
      int ret = OB_SUCCESS;
      ObString r_data;
      common::ObArenaAllocator allocator(ObModIds::OB_LOB_READER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
      ObTextStringIter r_instr_iter(ObLongTextType, cs_type, r.get_string(), true);
      if (OB_FAIL(r_instr_iter.init(0, NULL, &allocator))) {
        COMMON_LOG(WARN, "Lob: init right text str iter failed", K(ret), K(ret), K(r));
      } else if (OB_FAIL(r_instr_iter.get_full_data(r_data))) {
        COMMON_LOG(WARN, "Lob: get right text str iter full data failed ", K(ret), K(cs_type), K(r_instr_iter));
      } else {
        int cmp_ret = ObCharset::strcmpsp(cs_type, l.ptr_, l.len_, 
                                          r_data.ptr(), r_data.length(), with_end_space);
        res.set_int(get_cmp_ret<CMP_OP>(cmp_ret > 0 ? 1 : (cmp_ret < 0 ? -1 : 0)));
      }
      return ret;
    }
  };

  inline static int str_text_eval_batch(BATCH_EVAL_FUNC_ARG_DECL)
  {
    bool with_end_space = is_calc_with_end_space(
        expr.args_[0]->datum_meta_.type_,
        expr.args_[1]->datum_meta_.type_,
        lib::is_oracle_mode(),
        expr.args_[0]->datum_meta_.cs_type_,
        expr.args_[1]->datum_meta_.cs_type_);
    return def_relational_eval_batch_func<StrTextCmp>(BATCH_EVAL_FUNC_ARG_LIST,
                                                      expr.args_[0]->datum_meta_.cs_type_,
                                                      with_end_space);
  }

  struct JsonCmp
  {
    int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r,
                   bool has_lob_header) const
    {
      int ret = OB_SUCCESS;
      int result = 0;
      ObString l_data;
      ObString r_data;
      common::ObArenaAllocator allocator(ObModIds::OB_LOB_READER);
      ObTextStringIter l_instr_iter(ObJsonType, CS_TYPE_BINARY, l.get_string(), has_lob_header);
      ObTextStringIter r_instr_iter(ObJsonType, CS_TYPE_BINARY, r.get_string(), has_lob_header);
      if (OB_FAIL(l_instr_iter.init(0, NULL, &allocator))) {
        COMMON_LOG(WARN, "LobDebug: init left lob str iter failed", K(ret), K(l));
      } else if (OB_FAIL(l_instr_iter.get_full_data(l_data))) {
        COMMON_LOG(WARN, "LobDebug: get left lob str iter full data failed ", K(ret), K(l_instr_iter));
      } else if (OB_FAIL(r_instr_iter.init(0, NULL, &allocator))) {
        COMMON_LOG(WARN, "LobDebug: init right lob str iter failed", K(ret), K(ret), K(r));
      } else if (OB_FAIL(r_instr_iter.get_full_data(r_data))) {
        COMMON_LOG(WARN, "LobDebug: get right lob str iter full data failed ", K(ret), K(r_instr_iter));
      } else {
        ObJsonBin j_bin_l(l_data.ptr(), l_data.length(), &allocator);
        ObJsonBin j_bin_r(r_data.ptr(), r_data.length(), &allocator);
        ObIJsonBase *j_base_l = &j_bin_l;
        ObIJsonBase *j_base_r = &j_bin_r;

        if (OB_FAIL(j_bin_l.reset_iter())) {
          COMMON_LOG(WARN, "fail to reset left json bin iter", K(ret), K(l.len_));
        } else if (OB_FAIL(j_bin_r.reset_iter())) {
          COMMON_LOG(WARN, "fail to reset right json bin iter", K(ret), K(r.len_));
        } else if (OB_FAIL(j_base_l->compare(*j_base_r, result))) {
          COMMON_LOG(WARN, "fail to compare json", K(ret), K(*j_base_l), K(*j_base_r));
        } else {
          res.set_int(get_cmp_ret<CMP_OP>(result > 0 ? 1 : (result < 0 ? -1 : 0)));
        }
      }
      return ret;
    }
  };

  inline static int json_eval_batch(BATCH_EVAL_FUNC_ARG_DECL)
  {
    bool has_lob_header = expr.args_[0]->obj_meta_.has_lob_header();
    return def_relational_eval_batch_func<JsonCmp>(BATCH_EVAL_FUNC_ARG_LIST,
                                                   has_lob_header);
  }

  struct GeoCmp
  {
    int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r,
                   bool has_lob_header) const
    {
      int ret = OB_SUCCESS;
      int result = 0;
      ObString l_data;
      ObString r_data;
      common::ObArenaAllocator allocator(ObModIds::OB_LOB_READER);
      ObTextStringIter l_instr_iter(ObJsonType, CS_TYPE_BINARY, l.get_string(), has_lob_header);
      ObTextStringIter r_instr_iter(ObJsonType, CS_TYPE_BINARY, r.get_string(), has_lob_header);
      if (OB_FAIL(l_instr_iter.init(0, NULL, &allocator))) {
        COMMON_LOG(WARN, "LobDebug: init left lob str iter failed", K(ret), K(l));
      } else if (OB_FAIL(l_instr_iter.get_full_data(l_data))) {
        COMMON_LOG(WARN, "LobDebug: get left lob str iter full data failed ", K(ret), K(l_instr_iter));
      } else if (OB_FAIL(r_instr_iter.init(0, NULL, &allocator))) {
        COMMON_LOG(WARN, "LobDebug: init right lob str iter failed", K(ret), K(ret), K(r));
      } else if (OB_FAIL(r_instr_iter.get_full_data(r_data))) {
        COMMON_LOG(WARN, "LobDebug: get right lob str iter full data failed ", K(ret), K(r_instr_iter));
      } else {
        result = ObCharset::strcmpsp(CS_TYPE_BINARY, l_data.ptr(), l_data.length(), r_data.ptr(), r_data.length(), false);
        res.set_int(get_cmp_ret<CMP_OP>(result > 0 ? 1 : (result < 0 ? -1 : 0)));
      }
      return ret;
    }
  };

  inline static int geo_eval_batch(BATCH_EVAL_FUNC_ARG_DECL)
  {
    bool has_lob_header = expr.args_[0]->obj_meta_.has_lob_header();
    return def_relational_eval_batch_func<GeoCmp>(BATCH_EVAL_FUNC_ARG_LIST,
                                                  has_lob_header);
  }
};

template<ObCmpOp CMP_OP>
struct ObRelationalVecFunc
{
  struct DatumCmp
  {
    int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r, const ObExpr &expr, ObEvalCtx &ctx) const
    {
      int ret = OB_SUCCESS;
      int cmp_ret = 0;
      const ObExpr &left_expr = *expr.args_[0];
      const ObExpr &right_expr = *expr.args_[1];
      ObIArrayType *arr_l = NULL;
      ObIArrayType *arr_r = NULL;
      ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
      common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
      if (OB_FAIL(ObArrayExprUtils::get_type_vector(left_expr, l, ctx, tmp_allocator, arr_l))) {
        LOG_WARN("failed to get vector", K(ret));
      } else if (OB_FAIL(ObArrayExprUtils::get_type_vector(right_expr, r, ctx, tmp_allocator, arr_r))) {
        LOG_WARN("failed to get vector", K(ret));
      } else if (OB_ISNULL(arr_l) || OB_ISNULL(arr_r)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr", K(ret), K(arr_l), K(arr_r));
      } else if (OB_UNLIKELY(arr_l->size() != arr_r->size())) {
        ret = OB_ERR_INVALID_VECTOR_DIM;
        LOG_WARN("check array validty failed", K(ret), K(arr_l->size()), K(arr_r->size()));
      } else if (arr_l->contain_null() || arr_r->contain_null()) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("array with null can't cmp", K(ret));
      } else {
        const float *data_l = reinterpret_cast<const float*>(arr_l->get_data());
        const float *data_r = reinterpret_cast<const float*>(arr_r->get_data());
        const uint32_t size = arr_l->size();
        for (int64_t i = 0; i < size && cmp_ret == 0; ++i) {
          if (isnan(data_l[i]) || isnan(data_r[i])) {
            if (isnan(data_l[i]) && isnan(data_r[i])) {
              cmp_ret = 0;
            } else if (isnan(data_l[i])) {
              // l is nan, r is not nan:left always bigger than right
              cmp_ret = 1;
            } else {
              // l is not nan, r is nan, left always less than right
              cmp_ret = -1;
            }
          } else {
            cmp_ret = data_l[i] == data_r[i] ? 0 : (data_l[i] < data_r[i] ? -1 : 1);
          }
        }
        res.set_int(get_cmp_ret<CMP_OP>(cmp_ret));
      }
      return ret;
    }
  };

  inline static int eval(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
  {
    int ret = OB_SUCCESS;
    ObDatum *l = NULL;
    ObDatum *r = NULL;
    bool contain_null = false;
    if (OB_FAIL(ObRelationalExprOperator::get_comparator_operands(
                expr, ctx, l, r, expr_datum, contain_null))) {
      LOG_WARN("failed to eval args", K(ret));
    } else if (!contain_null) {
      if (OB_ISNULL(l) || OB_ISNULL(r)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid operands", K(ret), K(l), K(r));
      } else {
        ret = DatumCmp()(expr_datum, *l, *r, expr, ctx);
      }
    }
    return ret;
  }
  inline static int eval_batch(BATCH_EVAL_FUNC_ARG_DECL)
  {
    return def_relational_eval_batch_func<DatumCmp>(BATCH_EVAL_FUNC_ARG_LIST, expr, ctx);
  }
};


extern ObExpr::EvalBatchFunc EVAL_BATCH_NULL_EXTEND_CMP_FUNCS[CO_MAX];
extern ObExpr::EvalBatchFunc EVAL_BATCH_STR_CMP_FUNCS[CO_MAX];
extern ObExpr::EvalBatchFunc EVAL_BATCH_TEXT_CMP_FUNCS[CO_MAX];
extern ObExpr::EvalBatchFunc EVAL_BATCH_TEXT_STR_CMP_FUNCS[CO_MAX];
extern ObExpr::EvalBatchFunc EVAL_BATCH_STR_TEXT_CMP_FUNCS[CO_MAX];
extern ObExpr::EvalBatchFunc EVAL_BATCH_JSON_CMP_FUNCS[CO_MAX];
extern ObExpr::EvalBatchFunc EVAL_BATCH_GEO_CMP_FUNCS[CO_MAX];
extern ObExpr::EvalBatchFunc EVAL_BATCH_COLLECTION_CMP_FUNCS[CO_MAX];

extern ObExpr::EvalFunc EVAL_TYPE_CMP_FUNCS[ObMaxType][ObMaxType][CO_MAX];
extern ObExpr::EvalBatchFunc EVAL_BATCH_TYPE_CMP_FUNCS[ObMaxType][ObMaxType][CO_MAX];
extern ObDatumCmpFuncType DATUM_TYPE_CMP_FUNCS[ObMaxType][ObMaxType];

// TODO serialize
extern ObExpr::EvalFunc EVAL_TC_CMP_FUNCS[ObMaxTC][ObMaxTC][CO_MAX];
extern ObExpr::EvalBatchFunc EVAL_BATCH_TC_CMP_FUNCS[ObMaxTC][ObMaxTC][CO_MAX];
extern ObDatumCmpFuncType DATUM_TC_CMP_FUNCS[ObMaxTC][ObMaxTC];

extern ObExpr::EvalFunc EVAL_STR_CMP_FUNCS[CS_TYPE_MAX][CO_MAX][2];
extern ObDatumCmpFuncType DATUM_STR_CMP_FUNCS[CS_TYPE_MAX][2];
extern ObExpr::EvalFunc EVAL_TEXT_CMP_FUNCS[CS_TYPE_MAX][CO_MAX][2];
extern ObDatumCmpFuncType DATUM_TEXT_CMP_FUNCS[CS_TYPE_MAX][2];
extern ObExpr::EvalFunc EVAL_TEXT_STR_CMP_FUNCS[CS_TYPE_MAX][CO_MAX][2];
extern ObDatumCmpFuncType DATUM_TEXT_STR_CMP_FUNCS[CS_TYPE_MAX][2];
extern ObExpr::EvalFunc EVAL_STR_TEXT_CMP_FUNCS[CS_TYPE_MAX][CO_MAX][2];
extern ObDatumCmpFuncType DATUM_STR_TEXT_CMP_FUNCS[CS_TYPE_MAX][2];
extern ObExpr::EvalFunc EVAL_JSON_CMP_FUNCS[CO_MAX][2];
extern ObDatumCmpFuncType DATUM_JSON_CMP_FUNCS[2];
extern ObExpr::EvalFunc EVAL_GEO_CMP_FUNCS[CO_MAX][2];
extern ObDatumCmpFuncType DATUM_GEO_CMP_FUNCS[2];
extern ObExpr::EvalFunc EVAL_COLLECTION_CMP_FUNCS[CO_MAX][2];
extern ObDatumCmpFuncType DATUM_COLLECTION_CMP_FUNCS[2];

extern ObExpr::EvalFunc EVAL_FIXED_DOUBLE_CMP_FUNCS[OB_NOT_FIXED_SCALE][CO_MAX];
extern ObExpr::EvalBatchFunc EVAL_BATCH_FIXED_DOUBLE_CMP_FUNCS[OB_NOT_FIXED_SCALE][CO_MAX];
extern ObDatumCmpFuncType DATUM_FIXED_DOUBLE_CMP_FUNCS[OB_NOT_FIXED_SCALE];

extern ObExpr::EvalFunc EVAL_DECINT_CMP_FUNCS[DECIMAL_INT_MAX][DECIMAL_INT_MAX][CO_MAX];
extern ObExpr::EvalBatchFunc EVAL_BATCH_DECINT_CMP_FUNCS[DECIMAL_INT_MAX][DECIMAL_INT_MAX][CO_MAX];

extern ObDatumCmpFuncType DATUM_DECINT_CMP_FUNCS[DECIMAL_INT_MAX][DECIMAL_INT_MAX];

extern ObExpr::EvalFunc EVAL_VEC_CMP_FUNCS[CO_MAX];
extern ObExpr::EvalBatchFunc EVAL_BATCH_VEC_CMP_FUNCS[CO_MAX];

template<int X>
struct ExtraExprCmpIniter
{
  static void init_array()
  {
    using Func = ObRelationalExtraFunc<static_cast<ObCmpOp>(X)>;
    EVAL_BATCH_NULL_EXTEND_CMP_FUNCS[X] = Func::null_extend_eval_batch;
    EVAL_BATCH_STR_CMP_FUNCS[X] = Func::str_eval_batch;
    EVAL_BATCH_TEXT_CMP_FUNCS[X] = Func::text_eval_batch;
    EVAL_BATCH_TEXT_STR_CMP_FUNCS[X] = Func::text_str_eval_batch;
    EVAL_BATCH_STR_TEXT_CMP_FUNCS[X] = Func::str_text_eval_batch;
    EVAL_BATCH_JSON_CMP_FUNCS[X] = Func::json_eval_batch;
    EVAL_BATCH_GEO_CMP_FUNCS[X] = Func::geo_eval_batch;
  }
};

struct ExprDummyIniter
{
  static void init_array() {}
};

template<int X, int Y, bool defined = true>
struct TypeExprCmpFuncIniter
{
  using Def = datum_cmp::ObDatumTypeCmp<
      static_cast<ObObjType>(X),
      static_cast<ObObjType>(Y)>;

  template<ObCmpOp cmp_op>
  using EvalCmp = ObRelationalTypeFunc<Def::defined_,
        static_cast<ObObjType>(X), static_cast<ObObjType>(Y), cmp_op>;
  static void init_array()
  {
    auto &funcs = EVAL_TYPE_CMP_FUNCS;
    funcs[X][Y][CO_LE] = Def::defined_ ? &EvalCmp<CO_LE>::eval : NULL;
    funcs[X][Y][CO_LT] = Def::defined_ ? &EvalCmp<CO_LT>::eval : NULL;
    funcs[X][Y][CO_EQ] = Def::defined_ ? &EvalCmp<CO_EQ>::eval : NULL;
    funcs[X][Y][CO_GE] = Def::defined_ ? &EvalCmp<CO_GE>::eval : NULL;
    funcs[X][Y][CO_GT] = Def::defined_ ? &EvalCmp<CO_GT>::eval : NULL;
    funcs[X][Y][CO_NE] = Def::defined_ ? &EvalCmp<CO_NE>::eval : NULL;
    funcs[X][Y][CO_CMP] = Def::defined_ ? &EvalCmp<CO_CMP>::eval : NULL;

    auto &batch_funcs = EVAL_BATCH_TYPE_CMP_FUNCS;
    batch_funcs[X][Y][CO_LE] = Def::defined_ ? &EvalCmp<CO_LE>::eval_batch : NULL;
    batch_funcs[X][Y][CO_LT] = Def::defined_ ? &EvalCmp<CO_LT>::eval_batch : NULL;
    batch_funcs[X][Y][CO_EQ] = Def::defined_ ? &EvalCmp<CO_EQ>::eval_batch : NULL;
    batch_funcs[X][Y][CO_GE] = Def::defined_ ? &EvalCmp<CO_GE>::eval_batch : NULL;
    batch_funcs[X][Y][CO_GT] = Def::defined_ ? &EvalCmp<CO_GT>::eval_batch : NULL;
    batch_funcs[X][Y][CO_NE] = Def::defined_ ? &EvalCmp<CO_NE>::eval_batch : NULL;
    // CO_CMP only used in T_FUN_SYS_STRCMP, set to NULL
    batch_funcs[X][Y][CO_CMP] = NULL;

    DATUM_TYPE_CMP_FUNCS[X][Y] = Def::defined_ ? &Def::cmp : NULL;
  }
};

template<int X, int Y>
struct TypeExprCmpFuncIniter<X, Y, false>: public ExprDummyIniter {};

template<int X, int Y>
using TypeExprCmpIniter = TypeExprCmpFuncIniter<X, Y, datum_cmp::ObDatumTypeCmp<static_cast<ObObjType>(X),static_cast<ObObjType>(Y)>::defined_>;

template<int X, int Y, bool defined = true>
struct TCExprCmpFuncIniter
{
  using Def = datum_cmp::ObDatumTCCmp<
      static_cast<ObObjTypeClass>(X),
      static_cast<ObObjTypeClass>(Y)>;

  template<ObCmpOp cmp_op>
  using EvalCmp = ObRelationalTCFunc<Def::defined_,
        static_cast<ObObjTypeClass>(X), static_cast<ObObjTypeClass>(Y), cmp_op>;
  static void init_array()
  {
    auto &funcs = EVAL_TC_CMP_FUNCS;
    funcs[X][Y][CO_LE] = Def::defined_ ? &EvalCmp<CO_LE>::eval : NULL;
    funcs[X][Y][CO_LT] = Def::defined_ ? &EvalCmp<CO_LT>::eval : NULL;
    funcs[X][Y][CO_EQ] = Def::defined_ ? &EvalCmp<CO_EQ>::eval : NULL;
    funcs[X][Y][CO_GE] = Def::defined_ ? &EvalCmp<CO_GE>::eval : NULL;
    funcs[X][Y][CO_GT] = Def::defined_ ? &EvalCmp<CO_GT>::eval : NULL;
    funcs[X][Y][CO_NE] = Def::defined_ ? &EvalCmp<CO_NE>::eval : NULL;
    funcs[X][Y][CO_CMP] = Def::defined_ ? &EvalCmp<CO_CMP>::eval : NULL;

    auto &batch_funcs = EVAL_BATCH_TC_CMP_FUNCS;
    if (X == ObNullTC || X == ObExtendTC
        || Y == ObNullTC || Y == ObExtendTC) {
      batch_funcs[X][Y][CO_LE] = EVAL_BATCH_NULL_EXTEND_CMP_FUNCS[CO_LE];
      batch_funcs[X][Y][CO_LT] = EVAL_BATCH_NULL_EXTEND_CMP_FUNCS[CO_LT];
      batch_funcs[X][Y][CO_EQ] = EVAL_BATCH_NULL_EXTEND_CMP_FUNCS[CO_EQ];
      batch_funcs[X][Y][CO_GE] = EVAL_BATCH_NULL_EXTEND_CMP_FUNCS[CO_GE];
      batch_funcs[X][Y][CO_GT] = EVAL_BATCH_NULL_EXTEND_CMP_FUNCS[CO_GT];
      batch_funcs[X][Y][CO_NE] = EVAL_BATCH_NULL_EXTEND_CMP_FUNCS[CO_NE];
      batch_funcs[X][Y][CO_CMP] = EVAL_BATCH_NULL_EXTEND_CMP_FUNCS[CO_CMP];
    } else {
      batch_funcs[X][Y][CO_LE] = Def::defined_ ? &EvalCmp<CO_LE>::eval_batch : NULL;
      batch_funcs[X][Y][CO_LT] = Def::defined_ ? &EvalCmp<CO_LT>::eval_batch : NULL;
      batch_funcs[X][Y][CO_EQ] = Def::defined_ ? &EvalCmp<CO_EQ>::eval_batch : NULL;
      batch_funcs[X][Y][CO_GE] = Def::defined_ ? &EvalCmp<CO_GE>::eval_batch : NULL;
      batch_funcs[X][Y][CO_GT] = Def::defined_ ? &EvalCmp<CO_GT>::eval_batch : NULL;
      batch_funcs[X][Y][CO_NE] = Def::defined_ ? &EvalCmp<CO_NE>::eval_batch : NULL;
      // CO_CMP only used in T_FUN_SYS_STRCMP, set to NULL
      batch_funcs[X][Y][CO_CMP] = NULL;
    }

    DATUM_TC_CMP_FUNCS[X][Y] = Def::defined_ ? &Def::cmp : NULL;
  }
};

// macro to help use the old string relation eval functions, which are always inline
// we choose ObNewRelationalStrFunc here to compile faster.
#define USE_OLD_STR_RELATION_EVAL_FUNC 0
template<int X, int Y>
struct TCExprCmpFuncIniter<X, Y, false>: public ExprDummyIniter{};

template<int X, int Y>
using TCExprCmpIniter = TCExprCmpFuncIniter<X, Y, datum_cmp::ObDatumTCCmp<static_cast<ObObjTypeClass>(X),static_cast<ObObjTypeClass>(Y)>::defined_>;

template<int X, int Y>
struct StrExprFuncIniter
{
#if USE_OLD_STR_RELATION_EVAL_FUNC
  using Def = datum_cmp::ObDatumStrCmp<static_cast<ObCollationType>(X), false>;
  template<bool WITH_END_SPACE>
  using EvalCmp = ObRelationalStrFunc<Def::defined_,
        static_cast<ObCollationType>(X),
        WITH_END_SPACE,
        static_cast<ObCmpOp>(Y)>;
#else
  template<bool WITH_END_SPACE>
  using EvalCmp = ObStrRelationEvalWrap<static_cast<ObCollationType>(X),
        WITH_END_SPACE,
        static_cast<ObCmpOp>(Y)>;
#endif
  static void init_array()
  {
    EVAL_STR_CMP_FUNCS[X][Y][0] = EvalCmp<0>::eval;
    EVAL_STR_CMP_FUNCS[X][Y][1] = EvalCmp<1>::eval;
  }
};

template<int Y>
struct StrExprFuncIniter<CS_TYPE_MAX, Y>
{
  static void init_array() {}
};

template<int X, int Y>
struct TextExprFuncIniter
{
#if USE_OLD_STR_RELATION_EVAL_FUNC
  using Def = datum_cmp::ObDatumTextCmp<static_cast<ObCollationType>(X), false>;
  template<bool WITH_END_SPACE>
  using EvalCmp = ObRelationalTextFunc<Def::defined_,
        static_cast<ObCollationType>(X),
        WITH_END_SPACE,
        static_cast<ObCmpOp>(Y)>;
#else
  template<bool WITH_END_SPACE>
  using EvalCmp = ObTextRelationEvalWrap<static_cast<ObCollationType>(X),
        WITH_END_SPACE,
        static_cast<ObCmpOp>(Y)>;
#endif
  static void init_array()
  {
    EVAL_TEXT_CMP_FUNCS[X][Y][0] = EvalCmp<0>::eval;
    EVAL_TEXT_CMP_FUNCS[X][Y][1] = EvalCmp<1>::eval;
  }
};

template<int Y>
struct TextExprFuncIniter<CS_TYPE_MAX, Y>
{
  static void init_array() {}
};

template<int X, int Y>
struct TextStrExprFuncIniter
{
#if USE_OLD_STR_RELATION_EVAL_FUNC
  using Def = datum_cmp::ObDatumTextStringCmp<static_cast<ObCollationType>(X), false>;
  template<bool WITH_END_SPACE>
  using EvalCmp = ObRelationalTextStrFunc<Def::defined_,
        static_cast<ObCollationType>(X),
        WITH_END_SPACE,
        static_cast<ObCmpOp>(Y)>;
#else
  template<bool WITH_END_SPACE>
  using EvalCmp = ObTextStrRelationEvalWrap<static_cast<ObCollationType>(X),
        WITH_END_SPACE,
        static_cast<ObCmpOp>(Y)>;
#endif
  static void init_array()
  {
    EVAL_TEXT_STR_CMP_FUNCS[X][Y][0] = EvalCmp<0>::eval;
    EVAL_TEXT_STR_CMP_FUNCS[X][Y][1] = EvalCmp<1>::eval;
  }
};

#undef USE_OLD_STR_RELATION_EVAL_FUNC

template<int Y>
struct TextStrExprFuncIniter<CS_TYPE_MAX, Y>
{
  static void init_array() {}
};

template<int X, int Y>
struct StrTextExprFuncIniter
{
#if USE_OLD_STR_RELATION_EVAL_FUNC
  using Def = datum_cmp::ObDatumStringTextCmp<static_cast<ObCollationType>(X), false>;
  template<bool WITH_END_SPACE>
  using EvalCmp = ObRelationalStrTextFunc<Def::defined_,
        static_cast<ObCollationType>(X),
        WITH_END_SPACE,
        static_cast<ObCmpOp>(Y)>;
#else
  template<bool WITH_END_SPACE>
  using EvalCmp = ObStrTextRelationEvalWrap<static_cast<ObCollationType>(X),
        WITH_END_SPACE,
        static_cast<ObCmpOp>(Y)>;
#endif
  static void init_array()
  {
    EVAL_STR_TEXT_CMP_FUNCS[X][Y][0] = EvalCmp<0>::eval;
    EVAL_STR_TEXT_CMP_FUNCS[X][Y][1] = EvalCmp<1>::eval;
  }
};

template<int Y>
struct StrTextExprFuncIniter<CS_TYPE_MAX, Y>
{
  static void init_array() {}
};

template<int X>
struct DatumStrExprCmpIniter
{
  template<bool WITH_END_SPACE>
  using DatumCmp = datum_cmp::ObDatumStrCmp<static_cast<ObCollationType>(X), WITH_END_SPACE>;
  using Def = datum_cmp::ObDatumStrCmp<static_cast<ObCollationType>(X), false>;
  static void init_array()
  {
    DATUM_STR_CMP_FUNCS[X][0] = Def::defined_ ? DatumCmp<0>::cmp : NULL;
    DATUM_STR_CMP_FUNCS[X][1] = Def::defined_ ? DatumCmp<1>::cmp : NULL;
  }
};

template<>
struct DatumStrExprCmpIniter<CS_TYPE_MAX>
{
  static void init_array() {}
};

template<int X>
struct DatumTextExprCmpIniter
{
  template<bool WITH_END_SPACE>
  using DatumCmp = datum_cmp::ObDatumTextCmp<static_cast<ObCollationType>(X), WITH_END_SPACE>;
  using Def = datum_cmp::ObDatumTextCmp<static_cast<ObCollationType>(X), false>;
  static void init_array()
  {
    DATUM_TEXT_CMP_FUNCS[X][0] = Def::defined_ ? DatumCmp<0>::cmp : NULL;
    DATUM_TEXT_CMP_FUNCS[X][1] = Def::defined_ ? DatumCmp<1>::cmp : NULL;
  }
};

template<>
struct DatumTextExprCmpIniter<CS_TYPE_MAX>
{
  static void init_array() {}
};

template<int X>
struct DatumTextStrExprCmpIniter
{
  template<bool WITH_END_SPACE>
  using DatumCmp = datum_cmp::ObDatumTextStringCmp<static_cast<ObCollationType>(X), WITH_END_SPACE>;
  using Def = datum_cmp::ObDatumTextStringCmp<static_cast<ObCollationType>(X), false>;
  static void init_array()
  {
    DATUM_TEXT_STR_CMP_FUNCS[X][0] = Def::defined_ ? DatumCmp<0>::cmp : NULL;
    DATUM_TEXT_STR_CMP_FUNCS[X][1] = Def::defined_ ? DatumCmp<1>::cmp : NULL;
  }
};

template<>
struct DatumTextStrExprCmpIniter<CS_TYPE_MAX>
{
  static void init_array() {}
};

template<int X>
struct DatumStrTextExprCmpIniter
{
  template<bool WITH_END_SPACE>
  using DatumCmp = datum_cmp::ObDatumStringTextCmp<static_cast<ObCollationType>(X), WITH_END_SPACE>;
  using Def = datum_cmp::ObDatumStringTextCmp<static_cast<ObCollationType>(X), false>;
  static void init_array()
  {
    DATUM_STR_TEXT_CMP_FUNCS[X][0] = Def::defined_ ? DatumCmp<0>::cmp : NULL;
    DATUM_STR_TEXT_CMP_FUNCS[X][1] = Def::defined_ ? DatumCmp<1>::cmp : NULL;
  }
};

template<>
struct DatumStrTextExprCmpIniter<CS_TYPE_MAX>
{
  static void init_array() {}
};

template<int Y>
struct JsonExprFuncIniter
{
  using Def = datum_cmp::ObDatumJsonCmp<false>;
  template<bool HAS_LOB_HEADER>
  using EvalCmp = ObRelationalJsonFunc<Def::defined_,
        HAS_LOB_HEADER,
        static_cast<ObCmpOp>(Y)>;
  static void init_array()
  {
    EVAL_JSON_CMP_FUNCS[Y][0] = Def::defined_ ? EvalCmp<0>::eval : NULL;
    EVAL_JSON_CMP_FUNCS[Y][1] = Def::defined_ ? EvalCmp<1>::eval : NULL;
  }
};

template<int X>
struct DatumJsonExprCmpIniter
{
  template<bool HAS_LOB_HEADER>
  using DatumCmp = datum_cmp::ObDatumJsonCmp<HAS_LOB_HEADER>;
  using Def = datum_cmp::ObDatumJsonCmp<false>;
  static void init_array()
  {
    DATUM_JSON_CMP_FUNCS[0] = Def::defined_ ? DatumCmp<0>::cmp : NULL;
    DATUM_JSON_CMP_FUNCS[1] = Def::defined_ ? DatumCmp<1>::cmp : NULL;
  }
};

template<int Y>
struct GeoExprFuncIniter
{
  using Def = datum_cmp::ObDatumGeoCmp<false>;
  template<bool HAS_LOB_HEADER>
  using EvalCmp = ObRelationalGeoFunc<Def::defined_,
        HAS_LOB_HEADER,
        static_cast<ObCmpOp>(Y)>;
  static void init_array()
  {
    EVAL_GEO_CMP_FUNCS[Y][0] = Def::defined_ ? EvalCmp<0>::eval : NULL;
    EVAL_GEO_CMP_FUNCS[Y][1] = Def::defined_ ? EvalCmp<1>::eval : NULL;
  }
};

template<int X>
struct DatumGeoExprCmpIniter
{
  template<bool HAS_LOB_HEADER>
  using DatumCmp = datum_cmp::ObDatumGeoCmp<HAS_LOB_HEADER>;
  using Def = datum_cmp::ObDatumGeoCmp<false>;
  static void init_array()
  {
    DATUM_GEO_CMP_FUNCS[0] = Def::defined_ ? DatumCmp<0>::cmp : NULL;
    DATUM_GEO_CMP_FUNCS[1] = Def::defined_ ? DatumCmp<1>::cmp : NULL;
  }
};

template<int Y>
struct CollectionExprFuncIniter
{
  template<bool HAS_LOB_HEADER>
  using EvalCmp = ObRelationalCollectionFunc<true,
        HAS_LOB_HEADER,
        static_cast<ObCmpOp>(Y)>;
  static void init_array()
  {
    EVAL_COLLECTION_CMP_FUNCS[Y][0] = EvalCmp<0>::eval;
    EVAL_COLLECTION_CMP_FUNCS[Y][1] = EvalCmp<1>::eval;
    EVAL_BATCH_COLLECTION_CMP_FUNCS[Y] = EvalCmp<1>::eval_batch;
  }
};

template<int X>
struct DatumCollectionExprCmpIniter
{
  template<bool HAS_LOB_HEADER>
  using DatumCmp = datum_cmp::ObDatumCollectionCmp<HAS_LOB_HEADER>;
  using Def = datum_cmp::ObDatumCollectionCmp<false>;
  static void init_array()
  {
    DATUM_COLLECTION_CMP_FUNCS[0] = Def::defined_ ? DatumCmp<0>::cmp : NULL;
    DATUM_COLLECTION_CMP_FUNCS[1] = Def::defined_ ? DatumCmp<1>::cmp : NULL;
  }
};

template<int X>
struct FixedDoubleCmpFuncIniter
{
  using Def = datum_cmp::ObFixedDoubleCmp<static_cast<ObScale>(X)>;

  template<ObCmpOp cmp_op>
  using EvalCmp = ObRelationFixedDoubleFunc<Def::defined_, static_cast<ObScale>(X), cmp_op>;
  static void init_array()
  {
    auto &funcs = EVAL_FIXED_DOUBLE_CMP_FUNCS;
    funcs[X][CO_LE] = Def::defined_ ? &EvalCmp<CO_LE>::eval : NULL;
    funcs[X][CO_LT] = Def::defined_ ? &EvalCmp<CO_LT>::eval : NULL;
    funcs[X][CO_EQ] = Def::defined_ ? &EvalCmp<CO_EQ>::eval : NULL;
    funcs[X][CO_GE] = Def::defined_ ? &EvalCmp<CO_GE>::eval : NULL;
    funcs[X][CO_GT] = Def::defined_ ? &EvalCmp<CO_GT>::eval : NULL;
    funcs[X][CO_NE] = Def::defined_ ? &EvalCmp<CO_NE>::eval : NULL;
    funcs[X][CO_CMP] = Def::defined_ ? &EvalCmp<CO_CMP>::eval : NULL;

    auto &batch_funcs = EVAL_BATCH_FIXED_DOUBLE_CMP_FUNCS;
    batch_funcs[X][CO_LE] = Def::defined_ ? &EvalCmp<CO_LE>::eval_batch : NULL;
    batch_funcs[X][CO_LT] = Def::defined_ ? &EvalCmp<CO_LT>::eval_batch : NULL;
    batch_funcs[X][CO_EQ] = Def::defined_ ? &EvalCmp<CO_EQ>::eval_batch : NULL;
    batch_funcs[X][CO_GE] = Def::defined_ ? &EvalCmp<CO_GE>::eval_batch : NULL;
    batch_funcs[X][CO_GT] = Def::defined_ ? &EvalCmp<CO_GT>::eval_batch : NULL;
    batch_funcs[X][CO_NE] = Def::defined_ ? &EvalCmp<CO_NE>::eval_batch : NULL;
    // CO_CMP only used in T_FUN_SYS_STRCMP, set to NULL
    batch_funcs[X][CO_CMP] = NULL;

    DATUM_FIXED_DOUBLE_CMP_FUNCS[X] = Def::defined_ ? &Def::cmp : NULL;
  }
};

template<int X, int Y>
struct DecintCmpFuncIniter
{
  using Def = datum_cmp::ObDecintCmp<static_cast<ObDecimalIntWideType>(X),
                                     static_cast<ObDecimalIntWideType>(Y)>;
  template <ObCmpOp cmp_op>
  using EvalCmp = ObRelationalDecintFunc<Def::defined_, static_cast<ObDecimalIntWideType>(X),
                                         static_cast<ObDecimalIntWideType>(Y), cmp_op>;
  static void init_array()
  {
    auto &funcs = EVAL_DECINT_CMP_FUNCS;
    funcs[X][Y][CO_LE] = Def::defined_ ? &EvalCmp<CO_LE>::eval : NULL;
    funcs[X][Y][CO_LT] = Def::defined_ ? &EvalCmp<CO_LT>::eval : NULL;
    funcs[X][Y][CO_GE] = Def::defined_ ? &EvalCmp<CO_GE>::eval : NULL;
    funcs[X][Y][CO_GT] = Def::defined_ ? &EvalCmp<CO_GT>::eval : NULL;
    funcs[X][Y][CO_NE] = Def::defined_ ? &EvalCmp<CO_NE>::eval : NULL;
    funcs[X][Y][CO_EQ] = Def::defined_ ? &EvalCmp<CO_EQ>::eval : NULL;
    funcs[X][Y][CO_CMP] = Def::defined_ ? &EvalCmp<CO_CMP>::eval : NULL;

    auto &batch_funcs = EVAL_BATCH_DECINT_CMP_FUNCS;
    batch_funcs[X][Y][CO_LE] = Def::defined_ ? &EvalCmp<CO_LE>::eval_batch : NULL;
    batch_funcs[X][Y][CO_LT] = Def::defined_ ? &EvalCmp<CO_LT>::eval_batch : NULL;
    batch_funcs[X][Y][CO_GE] = Def::defined_ ? &EvalCmp<CO_GE>::eval_batch : NULL;
    batch_funcs[X][Y][CO_GT] = Def::defined_ ? &EvalCmp<CO_GT>::eval_batch : NULL;
    batch_funcs[X][Y][CO_NE] = Def::defined_ ? &EvalCmp<CO_NE>::eval_batch : NULL;
    batch_funcs[X][Y][CO_EQ] = Def::defined_ ? &EvalCmp<CO_EQ>::eval_batch : NULL;

    batch_funcs[X][Y][CO_CMP] = NULL;

    DATUM_DECINT_CMP_FUNCS[X][Y] = Def::defined_ ? Def::cmp : NULL;
  }
};

} // end namespace sql
} // end namespace oceanbase

#endif // _OCEANBASE_EXPR_CMP_FUNC_IPP_H_
