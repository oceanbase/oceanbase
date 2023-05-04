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
#include "sql/engine/expr/ob_batch_eval_util.h"
#include "share/ob_lob_access_utils.h"

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

template <typename DatumFunc>
int def_relational_eval_func(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
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
      ret = DatumFunc()(expr_datum, *l, *r);
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
        ObJsonBin j_bin_l(l_data.ptr(), l_data.length());
        ObJsonBin j_bin_r(r_data.ptr(), r_data.length());
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


static ObExpr::EvalBatchFunc EVAL_BATCH_NULL_EXTEND_CMP_FUNCS[CO_MAX];
static ObExpr::EvalBatchFunc EVAL_BATCH_STR_CMP_FUNCS[CO_MAX];
static ObExpr::EvalBatchFunc EVAL_BATCH_TEXT_CMP_FUNCS[CO_MAX];
static ObExpr::EvalBatchFunc EVAL_BATCH_TEXT_STR_CMP_FUNCS[CO_MAX];
static ObExpr::EvalBatchFunc EVAL_BATCH_STR_TEXT_CMP_FUNCS[CO_MAX];
static ObExpr::EvalBatchFunc EVAL_BATCH_JSON_CMP_FUNCS[CO_MAX];
static ObExpr::EvalBatchFunc EVAL_BATCH_GEO_CMP_FUNCS[CO_MAX];

static ObExpr::EvalFunc EVAL_TYPE_CMP_FUNCS[ObMaxType][ObMaxType][CO_MAX];
static ObExpr::EvalBatchFunc EVAL_BATCH_TYPE_CMP_FUNCS[ObMaxType][ObMaxType][CO_MAX];
static ObDatumCmpFuncType DATUM_TYPE_CMP_FUNCS[ObMaxType][ObMaxType];

// TODO serialize
static ObExpr::EvalFunc EVAL_TC_CMP_FUNCS[ObMaxTC][ObMaxTC][CO_MAX];
static ObExpr::EvalBatchFunc EVAL_BATCH_TC_CMP_FUNCS[ObMaxTC][ObMaxTC][CO_MAX];
static ObDatumCmpFuncType DATUM_TC_CMP_FUNCS[ObMaxTC][ObMaxTC];

static ObExpr::EvalFunc EVAL_STR_CMP_FUNCS[CS_TYPE_MAX][CO_MAX][2];
static ObDatumCmpFuncType DATUM_STR_CMP_FUNCS[CS_TYPE_MAX][2];
static ObExpr::EvalFunc EVAL_TEXT_CMP_FUNCS[CS_TYPE_MAX][CO_MAX][2];
static ObDatumCmpFuncType DATUM_TEXT_CMP_FUNCS[CS_TYPE_MAX][2];
static ObExpr::EvalFunc EVAL_TEXT_STR_CMP_FUNCS[CS_TYPE_MAX][CO_MAX][2];
static ObDatumCmpFuncType DATUM_TEXT_STR_CMP_FUNCS[CS_TYPE_MAX][2];
static ObExpr::EvalFunc EVAL_STR_TEXT_CMP_FUNCS[CS_TYPE_MAX][CO_MAX][2];
static ObDatumCmpFuncType DATUM_STR_TEXT_CMP_FUNCS[CS_TYPE_MAX][2];
static ObExpr::EvalFunc EVAL_JSON_CMP_FUNCS[CO_MAX][2];
static ObDatumCmpFuncType DATUM_JSON_CMP_FUNCS[2];
static ObExpr::EvalFunc EVAL_GEO_CMP_FUNCS[CO_MAX][2];
static ObDatumCmpFuncType DATUM_GEO_CMP_FUNCS[2];

static ObExpr::EvalFunc EVAL_FIXED_DOUBLE_CMP_FUNCS[OB_NOT_FIXED_SCALE][CO_MAX];
static ObExpr::EvalBatchFunc EVAL_BATCH_FIXED_DOUBLE_CMP_FUNCS[OB_NOT_FIXED_SCALE][CO_MAX];
static ObDatumCmpFuncType DATUM_FIXED_DOUBLE_CMP_FUNCS[OB_NOT_FIXED_SCALE];

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

int g_init_extra_expr_ret = ObArrayConstIniter<CO_MAX, ExtraExprCmpIniter>::init();

template<int X, int Y>
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

template<int X, int Y>
struct StrExprFuncIniter
{
  using Def = datum_cmp::ObDatumStrCmp<static_cast<ObCollationType>(X), false>;
  template<bool WITH_END_SPACE>
  using EvalCmp = ObRelationalStrFunc<Def::defined_,
        static_cast<ObCollationType>(X),
        WITH_END_SPACE,
        static_cast<ObCmpOp>(Y)>;
  static void init_array()
  {
    EVAL_STR_CMP_FUNCS[X][Y][0] = Def::defined_ ? EvalCmp<0>::eval : NULL;
    EVAL_STR_CMP_FUNCS[X][Y][1] = Def::defined_ ? EvalCmp<1>::eval : NULL;
  }
};

template<int X, int Y>
struct TextExprFuncIniter
{
  using Def = datum_cmp::ObDatumTextCmp<static_cast<ObCollationType>(X), false>;
  template<bool WITH_END_SPACE>
  using EvalCmp = ObRelationalTextFunc<Def::defined_,
        static_cast<ObCollationType>(X),
        WITH_END_SPACE,
        static_cast<ObCmpOp>(Y)>;
  static void init_array()
  {
    EVAL_TEXT_CMP_FUNCS[X][Y][0] = Def::defined_ ? EvalCmp<0>::eval : NULL;
    EVAL_TEXT_CMP_FUNCS[X][Y][1] = Def::defined_ ? EvalCmp<1>::eval : NULL;
  }
};

template<int X, int Y>
struct TextStrExprFuncIniter
{
  using Def = datum_cmp::ObDatumTextStringCmp<static_cast<ObCollationType>(X), false>;
  template<bool WITH_END_SPACE>
  using EvalCmp = ObRelationalTextStrFunc<Def::defined_,
        static_cast<ObCollationType>(X),
        WITH_END_SPACE,
        static_cast<ObCmpOp>(Y)>;
  static void init_array()
  {
    EVAL_TEXT_STR_CMP_FUNCS[X][Y][0] = Def::defined_ ? EvalCmp<0>::eval : NULL;
    EVAL_TEXT_STR_CMP_FUNCS[X][Y][1] = Def::defined_ ? EvalCmp<1>::eval : NULL;
  }
};

template<int X, int Y>
struct StrTextExprFuncIniter
{
  using Def = datum_cmp::ObDatumStringTextCmp<static_cast<ObCollationType>(X), false>;
  template<bool WITH_END_SPACE>
  using EvalCmp = ObRelationalStrTextFunc<Def::defined_,
        static_cast<ObCollationType>(X),
        WITH_END_SPACE,
        static_cast<ObCmpOp>(Y)>;
  static void init_array()
  {
    EVAL_STR_TEXT_CMP_FUNCS[X][Y][0] = Def::defined_ ? EvalCmp<0>::eval : NULL;
    EVAL_STR_TEXT_CMP_FUNCS[X][Y][1] = Def::defined_ ? EvalCmp<1>::eval : NULL;
  }
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

int g_init_type_ret = Ob2DArrayConstIniter<ObMaxType, ObMaxType, TypeExprCmpFuncIniter>::init();
int g_init_tc_ret = Ob2DArrayConstIniter<ObMaxTC, ObMaxTC, TCExprCmpFuncIniter>::init();
int g_init_str_ret = Ob2DArrayConstIniter<CS_TYPE_MAX, CO_MAX, StrExprFuncIniter>::init();
int g_init_datum_str_ret = ObArrayConstIniter<CS_TYPE_MAX, DatumStrExprCmpIniter>::init();
int g_init_text_ret = Ob2DArrayConstIniter<CS_TYPE_MAX, CO_MAX, TextExprFuncIniter>::init();
int g_init_datum_text_ret = ObArrayConstIniter<CS_TYPE_MAX, DatumTextExprCmpIniter>::init();
int g_init_text_str_ret = Ob2DArrayConstIniter<CS_TYPE_MAX, CO_MAX, TextStrExprFuncIniter>::init();
int g_init_datum_text_str_ret = ObArrayConstIniter<CS_TYPE_MAX, DatumTextStrExprCmpIniter>::init();
int g_init_str_text_ret = Ob2DArrayConstIniter<CS_TYPE_MAX, CO_MAX, StrTextExprFuncIniter>::init();
int g_init_str_datum_text_ret = ObArrayConstIniter<CS_TYPE_MAX, DatumStrTextExprCmpIniter>::init();
int g_init_json_ret = ObArrayConstIniter<CO_MAX, JsonExprFuncIniter>::init();
int g_init_json_datum_ret = ObArrayConstIniter<1, DatumJsonExprCmpIniter>::init();
int g_init_geo_ret = ObArrayConstIniter<CO_MAX, GeoExprFuncIniter>::init();
int g_init_geo_datum_ret = ObArrayConstIniter<1, DatumGeoExprCmpIniter>::init();

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

int g_init_fixed_double_ret =
  ObArrayConstIniter<OB_NOT_FIXED_SCALE, FixedDoubleCmpFuncIniter>::init();


static int64_t fill_type_with_tc_eval_func(void)
{
  int64_t cnt = 0;
  for (int64_t i = 0; i < ObMaxType; i++) {
    ObObjTypeClass i_tc = ob_obj_type_class((ObObjType)i);
    for (int64_t j = 0; j < ObMaxType; j++) {
      ObObjTypeClass j_tc = ob_obj_type_class((ObObjType)j);
      if (NULL == EVAL_TYPE_CMP_FUNCS[i][j][0]) {
        const int64_t size = sizeof(void *) * CO_MAX;
        memcpy(&EVAL_TYPE_CMP_FUNCS[i][j][0],
               &EVAL_TC_CMP_FUNCS[i_tc][j_tc][0],
               size);
        memcpy(&EVAL_BATCH_TYPE_CMP_FUNCS[i][j][0],
               &EVAL_BATCH_TC_CMP_FUNCS[i_tc][j_tc][0],
               size);
        cnt++;
      }
      if (NULL == DATUM_TYPE_CMP_FUNCS[i][j]) {
        DATUM_TYPE_CMP_FUNCS[i][j] = DATUM_TC_CMP_FUNCS[i_tc][j_tc];
        cnt++;
      }
    }
  }
  return cnt;
}

int64_t g_fill_type_with_tc_eval_func =  fill_type_with_tc_eval_func();

ObExpr::EvalFunc ObExprCmpFuncsHelper::get_eval_expr_cmp_func(const ObObjType type1,
                                                              const ObObjType type2,
                                                              const ObScale scale1,
                                                              const ObScale scale2,
                                                              const ObCmpOp cmp_op,
                                                              const bool is_oracle_mode,
                                                              const ObCollationType cs_type,
                                                              const bool has_lob_header)
{
  OB_ASSERT(type1 >= ObNullType && type1 < ObMaxType);
  OB_ASSERT(type2 >= ObNullType && type2 < ObMaxType);
  OB_ASSERT(cmp_op >= CO_EQ && cmp_op <= CO_MAX);

  ObObjTypeClass tc1 = ob_obj_type_class(type1);
  ObObjTypeClass tc2 = ob_obj_type_class(type2);
  ObExpr::EvalFunc func_ptr = NULL;
  if (OB_UNLIKELY(ob_is_invalid_cmp_op(cmp_op)) ||
      OB_UNLIKELY(ob_is_invalid_obj_tc(tc1) ||
      OB_UNLIKELY(ob_is_invalid_obj_tc(tc2)))) {
    func_ptr = NULL;
  } else if (tc1 == ObJsonTC && tc2 == ObJsonTC) {
    func_ptr = EVAL_JSON_CMP_FUNCS[cmp_op][has_lob_header];
  } else if (tc1 == ObGeometryTC && tc2 == ObGeometryTC) {
    func_ptr = EVAL_GEO_CMP_FUNCS[cmp_op][has_lob_header];
  } else if (IS_FIXED_DOUBLE) {
    func_ptr = EVAL_FIXED_DOUBLE_CMP_FUNCS[MAX(scale1, scale2)][cmp_op];
  } else if (tc1 == ObUserDefinedSQLTC || tc2 == ObUserDefinedSQLTC) {
    func_ptr = NULL; //?
  } else if (!ObDatumFuncs::is_string_type(type1) || !ObDatumFuncs::is_string_type(type2)) {
    func_ptr = EVAL_TYPE_CMP_FUNCS[type1][type2][cmp_op];
  } else {
    OB_ASSERT(cs_type > CS_TYPE_INVALID && cs_type < CS_TYPE_MAX);
    int64_t calc_with_end_space_idx = (is_calc_with_end_space(type1, type2, is_oracle_mode,
                                                              cs_type, cs_type) ? 1 : 0);
    if (has_lob_header && (ob_is_large_text(type1) || ob_is_large_text(type2))) {
      if (ob_is_large_text(type1) && ob_is_large_text(type2)) {
        func_ptr = EVAL_TEXT_CMP_FUNCS[cs_type][cmp_op][calc_with_end_space_idx];
      } else if (ob_is_large_text(type1)) { // type2 not large text
        func_ptr = EVAL_TEXT_STR_CMP_FUNCS[cs_type][cmp_op][calc_with_end_space_idx];
      } else { // type1 not large text
        func_ptr = EVAL_STR_TEXT_CMP_FUNCS[cs_type][cmp_op][calc_with_end_space_idx];
      }
    } else { // no lob header or tinytext use original str cmp func
      func_ptr = EVAL_STR_CMP_FUNCS[cs_type][cmp_op][calc_with_end_space_idx];
    }
  }
  return func_ptr;
}

ObExpr::EvalBatchFunc ObExprCmpFuncsHelper::get_eval_batch_expr_cmp_func(
    const ObObjType type1,
    const ObObjType type2,
    const ObScale scale1,
    const ObScale scale2,
    const ObCmpOp cmp_op,
    const bool is_oracle_mode,
    const ObCollationType cs_type,
    const bool has_lob_header)
{
  OB_ASSERT(type1 >= ObNullType && type1 < ObMaxType);
  OB_ASSERT(type2 >= ObNullType && type2 < ObMaxType);
  OB_ASSERT(cmp_op >= CO_EQ && cmp_op <= CO_MAX);

  ObObjTypeClass tc1 = ob_obj_type_class(type1);
  ObObjTypeClass tc2 = ob_obj_type_class(type2);
  ObExpr::EvalBatchFunc func_ptr = NULL;
  if (OB_UNLIKELY(ob_is_invalid_cmp_op(cmp_op)) ||
      OB_UNLIKELY(ob_is_invalid_obj_tc(tc1) ||
      OB_UNLIKELY(ob_is_invalid_obj_tc(tc2)))) {
    func_ptr = NULL;
  } else if (type1 == ObJsonType && type2 == ObJsonType) {
    if (NULL != EVAL_JSON_CMP_FUNCS[cmp_op][has_lob_header]) {
      func_ptr = EVAL_BATCH_JSON_CMP_FUNCS[cmp_op];
    }
  } else if (tc1 == ObGeometryTC && tc2 == ObGeometryTC) {
    if (NULL != EVAL_GEO_CMP_FUNCS[cmp_op][has_lob_header]) {
      func_ptr = EVAL_BATCH_GEO_CMP_FUNCS[cmp_op];
    }
  } else if (IS_FIXED_DOUBLE) {
    func_ptr = EVAL_BATCH_FIXED_DOUBLE_CMP_FUNCS[MAX(scale1, scale2)][cmp_op];
  } else if (tc1 == ObUserDefinedSQLTC || tc2 == ObUserDefinedSQLTC) {
    func_ptr = NULL; //?
  } else if (!ObDatumFuncs::is_string_type(type1) || !ObDatumFuncs::is_string_type(type2)) {
    func_ptr = EVAL_BATCH_TYPE_CMP_FUNCS[type1][type2][cmp_op];
  } else {
    OB_ASSERT(cs_type > CS_TYPE_INVALID && cs_type < CS_TYPE_MAX);
    int64_t calc_with_end_space_idx = (is_calc_with_end_space(type1, type2, is_oracle_mode,
                                                              cs_type, cs_type) ? 1 : 0);
    if (has_lob_header && (ob_is_large_text(type1) || ob_is_large_text(type2))) {
      if (ob_is_large_text(type1) && ob_is_large_text(type2)) {
        if (NULL != EVAL_TEXT_CMP_FUNCS[cs_type][cmp_op][calc_with_end_space_idx]) {
          func_ptr = EVAL_BATCH_TEXT_CMP_FUNCS[cmp_op];
        }
      } else if (ob_is_large_text(type1)) { // type2 not large text
        if (NULL != EVAL_TEXT_STR_CMP_FUNCS[cs_type][cmp_op][calc_with_end_space_idx]) {
          func_ptr = EVAL_BATCH_TEXT_STR_CMP_FUNCS[cmp_op];
        }
      } else { // type1 not large text
        if (NULL != EVAL_STR_TEXT_CMP_FUNCS[cs_type][cmp_op][calc_with_end_space_idx]) {
          func_ptr = EVAL_BATCH_STR_TEXT_CMP_FUNCS[cmp_op];
        }
      }
    } else { // no lob header or tinytext use original str cmp func
      if (NULL != EVAL_STR_CMP_FUNCS[cs_type][cmp_op][calc_with_end_space_idx]) {
        func_ptr = EVAL_BATCH_STR_CMP_FUNCS[cmp_op];
      }
    }
  }
  return func_ptr;
}

DatumCmpFunc ObExprCmpFuncsHelper::get_datum_expr_cmp_func(const ObObjType type1,
                                           const ObObjType type2,
                                           const ObScale scale1,
                                           const ObScale scale2,
                                           const bool is_oracle_mode,
                                           const ObCollationType cs_type,
                                           const bool has_lob_header)
{
  OB_ASSERT(type1 >= ObNullType && type1 < ObMaxType);
  OB_ASSERT(type2 >= ObNullType && type2 < ObMaxType);

  ObObjTypeClass tc1 = ob_obj_type_class(type1);
  ObObjTypeClass tc2 = ob_obj_type_class(type2);
  ObDatumCmpFuncType func_ptr = NULL;
  if (type1 == ObJsonType && type2 == ObJsonType) {
    func_ptr = DATUM_JSON_CMP_FUNCS[has_lob_header];
  } else if (type1 == ObGeometryType && type2 == ObGeometryType) {
    func_ptr = DATUM_GEO_CMP_FUNCS[has_lob_header];
  } else if (IS_FIXED_DOUBLE) {
    func_ptr = DATUM_FIXED_DOUBLE_CMP_FUNCS[MAX(scale1, scale2)];
  } else if (tc1 == ObUserDefinedSQLTC || tc2 == ObUserDefinedSQLTC) {
    func_ptr = NULL; //?
  } else if (!ObDatumFuncs::is_string_type(type1) || !ObDatumFuncs::is_string_type(type2)) {
    func_ptr = DATUM_TYPE_CMP_FUNCS[type1][type2];
    if (NULL == func_ptr) {
      func_ptr = DATUM_TC_CMP_FUNCS[tc1][tc2];
    }
  } else {
    OB_ASSERT(cs_type > CS_TYPE_INVALID && cs_type < CS_TYPE_MAX);
    int64_t calc_with_end_space_idx = (is_calc_with_end_space(type1, type2, is_oracle_mode,
                                                              cs_type, cs_type) ? 1 : 0);
    if (has_lob_header && (ob_is_large_text(type1) || ob_is_large_text(type2))) {
      if (ob_is_large_text(type1) && ob_is_large_text(type2)) {
        func_ptr = DATUM_TEXT_CMP_FUNCS[cs_type][calc_with_end_space_idx];
      } else if (ob_is_large_text(type1)) { // type2 not large text
        func_ptr = DATUM_TEXT_STR_CMP_FUNCS[cs_type][calc_with_end_space_idx];
      } else { // type1 not large text
        func_ptr = DATUM_STR_TEXT_CMP_FUNCS[cs_type][calc_with_end_space_idx];
      }
    } else { // no lob header or tinytext use original str cmp func
      func_ptr = DATUM_STR_CMP_FUNCS[cs_type][calc_with_end_space_idx];
    }
  }
  return func_ptr;
}

// register function serialization
// Need too convert two dimension arrayto index stable one dimension array first.

// register type * type evaluate functions
static_assert(7 == CO_MAX, "unexpected size");
void *g_ser_eval_type_cmp_funcs[ObMaxType * ObMaxType * 7];
static_assert(sizeof(g_ser_eval_type_cmp_funcs) == sizeof(EVAL_TYPE_CMP_FUNCS),
              "unexpected size");
bool g_ser_eval_type_cmp_funcs_init = ObFuncSerialization::convert_NxN_array(
    g_ser_eval_type_cmp_funcs,
    reinterpret_cast<void **>(EVAL_TYPE_CMP_FUNCS),
    ObMaxType,
    7,
    0,
    7);
REG_SER_FUNC_ARRAY(OB_SFA_RELATION_EXPR_EVAL,
                   g_ser_eval_type_cmp_funcs,
                   sizeof(g_ser_eval_type_cmp_funcs) / sizeof(void *));


static_assert(7 == CO_MAX, "unexpected size");
void *g_ser_eval_batch_type_cmp_funcs[ObMaxType * ObMaxType * 7];
static_assert(sizeof(g_ser_eval_batch_type_cmp_funcs) == sizeof(EVAL_BATCH_TYPE_CMP_FUNCS),
              "unexpected size");
bool g_ser_eval_batch_type_cmp_funcs_init = ObFuncSerialization::convert_NxN_array(
    g_ser_eval_batch_type_cmp_funcs,
    reinterpret_cast<void **>(EVAL_BATCH_TYPE_CMP_FUNCS),
    ObMaxType,
    7,
    0,
    7);
REG_SER_FUNC_ARRAY(OB_SFA_RELATION_EXPR_EVAL_BATCH,
                   g_ser_eval_batch_type_cmp_funcs,
                   sizeof(g_ser_eval_batch_type_cmp_funcs) / sizeof(void *));

void *g_ser_datum_type_cmp_funcs[ObMaxType * ObMaxType];
static_assert(sizeof(g_ser_datum_type_cmp_funcs) == sizeof(DATUM_TYPE_CMP_FUNCS),
              "unexpected size");
bool g_ser_datum_cmp_funcs_init = ObFuncSerialization::convert_NxN_array(
    g_ser_datum_type_cmp_funcs,
    reinterpret_cast<void **>(DATUM_TYPE_CMP_FUNCS),
    ObMaxType);
REG_SER_FUNC_ARRAY(OB_SFA_DATUM_CMP,
                   g_ser_datum_type_cmp_funcs,
                   sizeof(g_ser_datum_type_cmp_funcs) / sizeof(void *));


static_assert(7 == CO_MAX && CS_TYPE_MAX * 7 * 2 == sizeof(EVAL_STR_CMP_FUNCS) / sizeof(void *),
              "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_RELATION_EXPR_EVAL_STR,
                   EVAL_STR_CMP_FUNCS,
                   sizeof(EVAL_STR_CMP_FUNCS) / sizeof(void *));

REG_SER_FUNC_ARRAY(OB_SFA_RELATION_EXPR_STR_EVAL_BATCH,
                   EVAL_BATCH_STR_CMP_FUNCS,
                   sizeof(EVAL_BATCH_STR_CMP_FUNCS) / sizeof(void *));

static_assert(CS_TYPE_MAX * 2 == sizeof(DATUM_STR_CMP_FUNCS) / sizeof(void *),
              "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_DATUM_CMP_STR,
                   DATUM_STR_CMP_FUNCS,
                   sizeof(DATUM_STR_CMP_FUNCS) / sizeof(void *));

static_assert(7 == CO_MAX && CS_TYPE_MAX * 7 * 2 == sizeof(EVAL_TEXT_CMP_FUNCS) / sizeof(void *),
              "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_RELATION_EXPR_EVAL_TEXT,
                   EVAL_TEXT_CMP_FUNCS,
                   sizeof(EVAL_TEXT_CMP_FUNCS) / sizeof(void *));

REG_SER_FUNC_ARRAY(OB_SFA_RELATION_EXPR_TEXT_EVAL_BATCH,
                   EVAL_BATCH_TEXT_CMP_FUNCS,
                   sizeof(EVAL_BATCH_TEXT_CMP_FUNCS) / sizeof(void *));

static_assert(CS_TYPE_MAX * 2 == sizeof(DATUM_TEXT_CMP_FUNCS) / sizeof(void *),
              "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_DATUM_CMP_TEXT,
                   DATUM_TEXT_CMP_FUNCS,
                   sizeof(DATUM_TEXT_CMP_FUNCS) / sizeof(void *));

static_assert(7 == CO_MAX && CS_TYPE_MAX * 7 * 2 == sizeof(EVAL_TEXT_STR_CMP_FUNCS) / sizeof(void *),
              "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_RELATION_EXPR_EVAL_TEXT_STR,
                   EVAL_TEXT_STR_CMP_FUNCS,
                   sizeof(EVAL_TEXT_STR_CMP_FUNCS) / sizeof(void *));

REG_SER_FUNC_ARRAY(OB_SFA_RELATION_EXPR_TEXT_STR_EVAL_BATCH,
                   EVAL_BATCH_TEXT_STR_CMP_FUNCS,
                   sizeof(EVAL_BATCH_TEXT_STR_CMP_FUNCS) / sizeof(void *));

static_assert(CS_TYPE_MAX * 2 == sizeof(DATUM_TEXT_STR_CMP_FUNCS) / sizeof(void *),
              "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_DATUM_CMP_TEXT_STR,
                   DATUM_TEXT_STR_CMP_FUNCS,
                   sizeof(DATUM_TEXT_STR_CMP_FUNCS) / sizeof(void *));

static_assert(7 == CO_MAX && CS_TYPE_MAX * 7 * 2 == sizeof(EVAL_STR_TEXT_CMP_FUNCS) / sizeof(void *),
              "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_RELATION_EXPR_EVAL_STR_TEXT,
                   EVAL_STR_TEXT_CMP_FUNCS,
                   sizeof(EVAL_STR_TEXT_CMP_FUNCS) / sizeof(void *));

REG_SER_FUNC_ARRAY(OB_SFA_RELATION_EXPR_STR_TEXT_EVAL_BATCH,
                   EVAL_BATCH_STR_TEXT_CMP_FUNCS,
                   sizeof(EVAL_BATCH_STR_TEXT_CMP_FUNCS) / sizeof(void *));

static_assert(CS_TYPE_MAX * 2 == sizeof(DATUM_STR_TEXT_CMP_FUNCS) / sizeof(void *),
              "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_DATUM_CMP_STR_TEXT,
                   DATUM_STR_TEXT_CMP_FUNCS,
                   sizeof(DATUM_STR_TEXT_CMP_FUNCS) / sizeof(void *));

static_assert(7 == CO_MAX && CO_MAX * 2 == sizeof(EVAL_JSON_CMP_FUNCS) / sizeof(void *),
              "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_RELATION_EXPR_EVAL_JSON,
                   EVAL_JSON_CMP_FUNCS,
                   sizeof(EVAL_JSON_CMP_FUNCS) / sizeof(void *));

static_assert(7 == CO_MAX && CO_MAX == sizeof(EVAL_BATCH_JSON_CMP_FUNCS) / sizeof(void *),
              "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_RELATION_EXPR_JSON_EVAL_BATCH,
                   EVAL_BATCH_JSON_CMP_FUNCS,
                   sizeof(EVAL_BATCH_JSON_CMP_FUNCS) / sizeof(void *));

static_assert(2 == sizeof(DATUM_JSON_CMP_FUNCS) / sizeof(void *),
              "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_DATUM_CMP_JSON,
                   DATUM_JSON_CMP_FUNCS,
                   sizeof(DATUM_JSON_CMP_FUNCS) / sizeof(void *));

// Geo cmp functions reg
static_assert(7 == CO_MAX && CO_MAX * 2 == sizeof(EVAL_GEO_CMP_FUNCS) / sizeof(void *),
              "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_RELATION_EXPR_EVAL_GEO,
                   EVAL_GEO_CMP_FUNCS,
                   sizeof(EVAL_GEO_CMP_FUNCS) / sizeof(void *));

static_assert(7 == CO_MAX && CO_MAX == sizeof(EVAL_BATCH_GEO_CMP_FUNCS) / sizeof(void *),
              "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_RELATION_EXPR_GEO_EVAL_BATCH,
                   EVAL_BATCH_GEO_CMP_FUNCS,
                   sizeof(EVAL_BATCH_GEO_CMP_FUNCS) / sizeof(void *));

static_assert(2 == sizeof(DATUM_GEO_CMP_FUNCS) / sizeof(void *),
              "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_DATUM_CMP_GEO,
                   DATUM_GEO_CMP_FUNCS,
                   sizeof(DATUM_GEO_CMP_FUNCS) / sizeof(void *));

// Fixed double cmp functions reg
static_assert(
  OB_NOT_FIXED_SCALE * CO_MAX == sizeof(EVAL_FIXED_DOUBLE_CMP_FUNCS) / sizeof(void *),
  "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_FIXED_DOUBLE_CMP_EVAL,
                   EVAL_FIXED_DOUBLE_CMP_FUNCS,
                   sizeof(EVAL_FIXED_DOUBLE_CMP_FUNCS) / sizeof(void *));

static_assert(
  OB_NOT_FIXED_SCALE * CO_MAX == sizeof(EVAL_BATCH_FIXED_DOUBLE_CMP_FUNCS) / sizeof(void *),
  "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_FIXED_DOUBLE_CMP_EVAL_BATCH,
                   EVAL_BATCH_FIXED_DOUBLE_CMP_FUNCS,
                   sizeof(EVAL_BATCH_FIXED_DOUBLE_CMP_FUNCS) / sizeof(void *));

static_assert(
  OB_NOT_FIXED_SCALE == sizeof(DATUM_FIXED_DOUBLE_CMP_FUNCS) / sizeof(void *),
  "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_DATUM_FIXED_DOUBLE_CMP,
                   DATUM_FIXED_DOUBLE_CMP_FUNCS,
                   sizeof(DATUM_FIXED_DOUBLE_CMP_FUNCS) / sizeof(void *));

} // end namespace common;
} // end namespace oceanbase
