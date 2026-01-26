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

#include "sql/engine/expr/ob_expr_edit_distance.h"
#include "sql/session/ob_sql_session_info.h"
namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprEditDistance::ObExprEditDistance(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc,
                         T_FUN_SYS_EDIT_DISTANCE,
                         N_EDIT_DISTANCE,
                         2,
                         VALID_FOR_GENERATED_COL,
                         NOT_ROW_DIMENSION,
                         INTERNAL_IN_MYSQL_MODE,
                         INTERNAL_IN_ORACLE_MODE)
{}

ObExprEditDistance::~ObExprEditDistance()
{
}

int ObExprEditDistance::calc_result_type2(ObExprResType &type,
                                          ObExprResType &type1,
                                          ObExprResType &type2,
                                          ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);

  // Set parameter types to VARCHAR
  type1.set_calc_type(ObVarcharType);
  type2.set_calc_type(ObVarcharType);

  // Set result type to INT
  type.set_uint64();
  type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].scale_);
  type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].precision_);

  return ret;
}

int ObExprEditDistance::cg_expr(ObExprCGCtx &op_cg_ctx,
                                const ObRawExpr &raw_expr,
                                ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);

  if (rt_expr.arg_cnt_ != 2) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("editDistance expr should have two params", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0]) || OB_ISNULL(rt_expr.args_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of editDistance expr is null", K(ret), K(rt_expr.args_));
  } else {
    rt_expr.eval_func_ = ObExprEditDistance::calc_edit_distance;
    rt_expr.eval_vector_func_ = ObExprEditDistance::calc_edit_distance_vector;
  }

  return ret;
}

int ObExprEditDistance::calc_edit_distance(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *arg1 = NULL;
  ObDatum *arg2 = NULL;

  if (OB_FAIL(expr.eval_param_value(ctx, arg1, arg2))) {
    LOG_WARN("failed to eval param value", K(ret), K(expr.args_[0]), K(expr.args_[1]));
  } else if (arg1->is_null() || arg2->is_null()) {
    expr_datum.set_null();
  } else {
    const ObString &str1 = arg1->get_string();
    const ObString &str2 = arg2->get_string();
    uint64_t distance = 0;
    ObArray<uint32_t> dists;
    if (OB_FAIL(compute_levenshtein(str1.ptr(), str1.length(),
                                    str2.ptr(), str2.length(),
                                    dists, distance))) {
      LOG_WARN("failed to compute levenshtein distance", K(ret), K(str1), K(str2));
    } else {
      expr_datum.set_int(distance);
    }
  }

  return ret;
}

int ObExprEditDistance::compute_levenshtein(const char * __restrict s1, int64_t len1,
                                            const char * __restrict s2, int64_t len2,
                                            ObIArray<uint32_t> &dists,
                                            uint64_t &distance)
{
  int ret = OB_SUCCESS;
  const int64_t max_length = 1u << 16;
  distance = 0;
  if (OB_UNLIKELY(len1 == 0 || len2 == 0)) {
    // Handle empty strings
    distance = len1 + len2;
  } else if (OB_UNLIKELY(len1 > max_length || len2 > max_length)) {
    // Safety threshold against DoS, same as in ClickHouse
    ret = OB_ERR_PARAMETER_TOO_LONG;
    LOG_WARN("string length is too long for edit distance, should be at most 65536", K(ret), K(len1), K(len2));
    LOG_USER_ERROR(OB_ERR_PARAMETER_TOO_LONG, "string length is too long for edit distance, should be at most 65536");
  } else if (OB_FAIL(dists.prepare_allocate(len2 + 1))) {
    LOG_WARN("failed to init dists", K(ret));
  } else {
    uint32_t deletion = 0;
    uint32_t insertion = 0;
    uint32_t substitution = 0;
    // Initialize the first row
    for (uint32_t j = 0; j <= len2; ++j) {
      dists.at(j) = j;
    }
    // Compute edit distance using dynamic programming
    for (uint32_t i = 1; i <= len1; ++i) {
      uint32_t up_left = dists.at(0);
      char c1 = s1[i - 1];
      dists.at(0) = i;
      for (uint32_t j = 1; j <= len2; ++j) {
        deletion = dists.at(j) + 1;
        insertion = dists.at(j - 1) + 1;
        substitution = up_left + ((c1 == s2[j - 1]) ? 0 : 1);
        up_left = dists.at(j);
        dists.at(j) = std::min({deletion, insertion, substitution});
      }
    }
    distance = dists.at(len2);
  }

  return ret;
}

template <typename ArgVec1, typename ArgVec2, typename ResVec>
int ObExprEditDistance::calc_edit_distance_vector_dispatch(VECTOR_EVAL_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  ArgVec1 *arg1_vec = static_cast<ArgVec1 *>(expr.args_[0]->get_vector(ctx));
  ArgVec2 *arg2_vec = static_cast<ArgVec2 *>(expr.args_[1]->get_vector(ctx));
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ObArray<uint32_t> dists;
  for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
    if (skip.at(idx) || eval_flags.at(idx)) {
      continue;
    } else if (arg1_vec->is_null(idx) || arg2_vec->is_null(idx)) {
      res_vec->set_null(idx);
    } else {
      ObString str1 = arg1_vec->get_string(idx);
      ObString str2 = arg2_vec->get_string(idx);
      uint64_t distance = 0;
      if (OB_FAIL(compute_levenshtein(str1.ptr(), str1.length(),
                                      str2.ptr(), str2.length(),
                                      dists, distance))) {
        LOG_WARN("failed to compute levenshtein distance", K(ret), K(idx));
      } else {
        res_vec->set_int(idx, distance);
      }
    }
  }

  return ret;
}

int ObExprEditDistance::calc_edit_distance_vector(VECTOR_EVAL_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("failed to eval vector arg0", K(ret));
  } else if (OB_FAIL(expr.args_[1]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("failed to eval vector arg1", K(ret));
  } else {
    VectorFormat arg0_format = expr.args_[0]->get_format(ctx);
    VectorFormat arg1_format = expr.args_[1]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    // VEC_UNIFORM × VEC_UNIFORM combinations
    if (VEC_UNIFORM == arg0_format) {
      if (VEC_UNIFORM == arg1_format) {
        if (VEC_UNIFORM == res_format) {
          ret = calc_edit_distance_vector_dispatch<StrUniVec, StrUniVec, UIntegerUniVec>(expr, ctx, skip, bound);
        } else if (VEC_FIXED == res_format) {
          ret = calc_edit_distance_vector_dispatch<StrUniVec, StrUniVec, UIntegerFixedVec>(expr, ctx, skip, bound);
        } else {
          ret = calc_edit_distance_vector_dispatch<StrUniVec, StrUniVec, ObVectorBase>(expr, ctx, skip, bound);
        }
      } else if (VEC_DISCRETE == arg1_format) {
        if (VEC_UNIFORM == res_format) {
          ret = calc_edit_distance_vector_dispatch<StrUniVec, StrDiscVec, UIntegerUniVec>(expr, ctx, skip, bound);
        } else if (VEC_FIXED == res_format) {
          ret = calc_edit_distance_vector_dispatch<StrUniVec, StrDiscVec, UIntegerFixedVec>(expr, ctx, skip, bound);
        } else {
          ret = calc_edit_distance_vector_dispatch<StrUniVec, StrDiscVec, ObVectorBase>(expr, ctx, skip, bound);
        }
      } else if (VEC_CONTINUOUS == arg1_format) {
        if (VEC_UNIFORM == res_format) {
          ret = calc_edit_distance_vector_dispatch<StrUniVec, StrContVec, UIntegerUniVec>(expr, ctx, skip, bound);
        } else if (VEC_FIXED == res_format) {
          ret = calc_edit_distance_vector_dispatch<StrUniVec, StrContVec, UIntegerFixedVec>(expr, ctx, skip, bound);
        } else {
          ret = calc_edit_distance_vector_dispatch<StrUniVec, StrContVec, ObVectorBase>(expr, ctx, skip, bound);
        }
      } else {
        ret = calc_edit_distance_vector_dispatch<StrUniVec, ObVectorBase, ObVectorBase>(expr, ctx, skip, bound);
      }
    } else if (VEC_DISCRETE == arg0_format) {
      if (VEC_UNIFORM == arg1_format) {
        if (VEC_UNIFORM == res_format) {
          ret = calc_edit_distance_vector_dispatch<StrDiscVec, StrUniVec, UIntegerUniVec>(expr, ctx, skip, bound);
        } else if (VEC_FIXED == res_format) {
          ret = calc_edit_distance_vector_dispatch<StrDiscVec, StrUniVec, UIntegerFixedVec>(expr, ctx, skip, bound);
        } else {
          ret = calc_edit_distance_vector_dispatch<StrDiscVec, StrUniVec, ObVectorBase>(expr, ctx, skip, bound);
        }
      } else if (VEC_DISCRETE == arg1_format) {
        if (VEC_UNIFORM == res_format) {
          ret = calc_edit_distance_vector_dispatch<StrDiscVec, StrDiscVec, UIntegerUniVec>(expr, ctx, skip, bound);
        } else if (VEC_FIXED == res_format) {
          ret = calc_edit_distance_vector_dispatch<StrDiscVec, StrDiscVec, UIntegerFixedVec>(expr, ctx, skip, bound);
        } else {
          ret = calc_edit_distance_vector_dispatch<StrDiscVec, StrDiscVec, ObVectorBase>(expr, ctx, skip, bound);
        }
      } else if (VEC_CONTINUOUS == arg1_format) {
        if (VEC_UNIFORM == res_format) {
          ret = calc_edit_distance_vector_dispatch<StrDiscVec, StrContVec, UIntegerUniVec>(expr, ctx, skip, bound);
        } else if (VEC_FIXED == res_format) {
          ret = calc_edit_distance_vector_dispatch<StrDiscVec, StrContVec, UIntegerFixedVec>(expr, ctx, skip, bound);
        } else {
          ret = calc_edit_distance_vector_dispatch<StrDiscVec, StrContVec, ObVectorBase>(expr, ctx, skip, bound);
        }
      } else {
        ret = calc_edit_distance_vector_dispatch<StrDiscVec, ObVectorBase, ObVectorBase>(expr, ctx, skip, bound);
      }
    } else if (VEC_CONTINUOUS == arg0_format) {
      if (VEC_UNIFORM == arg1_format) {
        if (VEC_UNIFORM == res_format) {
          ret = calc_edit_distance_vector_dispatch<StrContVec, StrUniVec, UIntegerUniVec>(expr, ctx, skip, bound);
        } else if (VEC_FIXED == res_format) {
          ret = calc_edit_distance_vector_dispatch<StrContVec, StrUniVec, UIntegerFixedVec>(expr, ctx, skip, bound);
        } else {
          ret = calc_edit_distance_vector_dispatch<StrContVec, StrUniVec, ObVectorBase>(expr, ctx, skip, bound);
        }
      } else if (VEC_DISCRETE == arg1_format) {
        if (VEC_UNIFORM == res_format) {
          ret = calc_edit_distance_vector_dispatch<StrContVec, StrDiscVec, UIntegerUniVec>(expr, ctx, skip, bound);
        } else if (VEC_FIXED == res_format) {
          ret = calc_edit_distance_vector_dispatch<StrContVec, StrDiscVec, UIntegerFixedVec>(expr, ctx, skip, bound);
        } else {
          ret = calc_edit_distance_vector_dispatch<StrContVec, StrDiscVec, ObVectorBase>(expr, ctx, skip, bound);
        }
      } else if (VEC_CONTINUOUS == arg1_format) {
        if (VEC_UNIFORM == res_format) {
          ret = calc_edit_distance_vector_dispatch<StrContVec, StrContVec, UIntegerUniVec>(expr, ctx, skip, bound);
        } else if (VEC_FIXED == res_format) {
          ret = calc_edit_distance_vector_dispatch<StrContVec, StrContVec, UIntegerFixedVec>(expr, ctx, skip, bound);
        } else {
          ret = calc_edit_distance_vector_dispatch<StrContVec, StrContVec, ObVectorBase>(expr, ctx, skip, bound);
        }
      } else {
        ret = calc_edit_distance_vector_dispatch<StrContVec, ObVectorBase, ObVectorBase>(expr, ctx, skip, bound);
      }
    } else {
      ret = calc_edit_distance_vector_dispatch<ObVectorBase, ObVectorBase, ObVectorBase>(expr, ctx, skip, bound);
    }
  }

  return ret;
}

ObExprEditDistanceUTF8::ObExprEditDistanceUTF8(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc,
                         T_FUN_SYS_EDIT_DISTANCE_UTF8,
                         N_EDIT_DISTANCE_UTF8,
                         2,
                         VALID_FOR_GENERATED_COL,
                         NOT_ROW_DIMENSION,
                         INTERNAL_IN_MYSQL_MODE,
                         INTERNAL_IN_ORACLE_MODE)
{}

ObExprEditDistanceUTF8::~ObExprEditDistanceUTF8()
{
}

int ObExprEditDistanceUTF8::calc_result_type2(ObExprResType &type,
                                              ObExprResType &type1,
                                              ObExprResType &type2,
                                              ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);

  // Set parameter types to VARCHAR
  type1.set_calc_type(ObVarcharType);
  type2.set_calc_type(ObVarcharType);

  // Set result type to INT
  type.set_uint64();
  type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].scale_);
  type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].precision_);

  return ret;
}

int ObExprEditDistanceUTF8::cg_expr(ObExprCGCtx &op_cg_ctx,
                                    const ObRawExpr &raw_expr,
                                    ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);

  if (rt_expr.arg_cnt_ != 2) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("editDistanceUTF8 expr should have two params", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0]) || OB_ISNULL(rt_expr.args_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of editDistanceUTF8 expr is null", K(ret), K(rt_expr.args_));
  } else {
    rt_expr.eval_func_ = ObExprEditDistanceUTF8::calc_edit_distance_utf8;
    rt_expr.eval_vector_func_ = ObExprEditDistanceUTF8::calc_edit_distance_utf8_vector;
  }

  return ret;
}

int ObExprEditDistanceUTF8::calc_edit_distance_utf8(const ObExpr &expr,
                                                    ObEvalCtx &ctx,
                                                    ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *arg1 = NULL;
  ObDatum *arg2 = NULL;

  if (OB_FAIL(expr.eval_param_value(ctx, arg1, arg2))) {
    LOG_WARN("failed to eval param value", K(ret), K(expr.args_[0]), K(expr.args_[1]));
  } else if (arg1->is_null() || arg2->is_null()) {
    expr_datum.set_null();
  } else {
    const ObString &str1 = arg1->get_string();
    const ObString &str2 = arg2->get_string();
    ObCollationType cs_type_str1 = expr.args_[0]->datum_meta_.cs_type_;
    ObCollationType cs_type_str2 = expr.args_[1]->datum_meta_.cs_type_;
    uint64_t distance = 0;
    ObArray<uint32_t> dists;
    if (OB_FAIL(compute_levenshtein_utf8(str1.ptr(), str1.length(), cs_type_str1,
                                         str2.ptr(), str2.length(), cs_type_str2,
                                         dists, distance))) {
      LOG_WARN("failed to compute levenshtein distance for utf8", K(ret), K(str1), K(str2));
    } else {
      expr_datum.set_int(distance);
    }
  }

  return ret;
}

int ObExprEditDistanceUTF8::WCharCollector::operator() (const common::ObString &str, ob_wc_t wc)
{
  int ret = OB_SUCCESS;
  UNUSED(str);
  if (OB_FAIL(wchars.push_back(wc))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to push back wchar", K(ret));
  }
  return ret;
}

int ObExprEditDistanceUTF8::compute_levenshtein_utf8(const char * __restrict s1,
                                                     int64_t byte_len1,
                                                     ObCollationType cs_type_str1,
                                                     const char * __restrict s2,
                                                     int64_t byte_len2,
                                                     ObCollationType cs_type_str2,
                                                     ObIArray<uint32_t> &dists,
                                                     uint64_t &distance)
{
  int ret = OB_SUCCESS;
  const int64_t max_length = 1u << 16;
  distance = 0;

  ObString str1(byte_len1, s1);
  ObString str2(byte_len2, s2);
  ObSEArray<ob_wc_t, 64> wchars1;
  ObSEArray<ob_wc_t, 64> wchars2;

  WCharCollector collector1(wchars1);
  WCharCollector collector2(wchars2);

  if (OB_FAIL(ObFastStringScanner::foreach_char(str1, CHARSET_UTF8MB4, collector1))) {
    LOG_WARN("failed to scan str1", K(ret), K(str1));
  } else if (OB_FAIL(ObFastStringScanner::foreach_char(str2, CHARSET_UTF8MB4, collector2))) {
    LOG_WARN("failed to scan str2", K(ret), K(str2));
  } else {
    int64_t char_len1 = wchars1.count();
    int64_t char_len2 = wchars2.count();
    if (OB_UNLIKELY(char_len1 == 0 || char_len2 == 0)) {
      distance = char_len1 + char_len2;
    } else if (OB_UNLIKELY(char_len1 > max_length || char_len2 > max_length)) {
      // Safety threshold against DoS, same as in ClickHouse
      ret = OB_ERR_PARAMETER_TOO_LONG;
      LOG_WARN("string length is too long for edit distance, should be at most 65536", K(ret), K(char_len1), K(char_len2));
      LOG_USER_ERROR(OB_ERR_PARAMETER_TOO_LONG, "string length is too long for edit distance, should be at most 65536");
    } else if (OB_FAIL(dists.prepare_allocate(char_len2 + 1))) {
      LOG_WARN("failed to init dists", K(ret));
    } else {
      uint32_t deletion = 0;
      uint32_t insertion = 0;
      uint32_t substitution = 0;
      for (uint32_t j = 0; j <= char_len2; ++j) {
        dists.at(j) = j;
      }
      for (uint32_t i = 1; i <= char_len1; ++i) {
        uint32_t up_left = dists.at(0);
        dists.at(0) = i;
        for (uint32_t j = 1; j <= char_len2; ++j) {
          deletion = dists.at(j) + 1;
          insertion = dists.at(j - 1) + 1;
          substitution = up_left + ((wchars1.at(i - 1) == wchars2.at(j - 1)) ? 0 : 1);
          up_left = dists.at(j);
          dists.at(j) = std::min({deletion, insertion, substitution});
        }
      }
      distance = dists.at(char_len2);
    }
  }
  return ret;
}

template <typename ArgVec1, typename ArgVec2, typename ResVec>
int ObExprEditDistanceUTF8::calc_edit_distance_utf8_vector_dispatch(VECTOR_EVAL_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  ArgVec1 *arg1_vec = static_cast<ArgVec1 *>(expr.args_[0]->get_vector(ctx));
  ArgVec2 *arg2_vec = static_cast<ArgVec2 *>(expr.args_[1]->get_vector(ctx));
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ObCollationType cs_type_str1 = expr.args_[0]->datum_meta_.cs_type_;
  ObCollationType cs_type_str2 = expr.args_[1]->datum_meta_.cs_type_;
  ObArray<uint32_t> dists;
  for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
    if (skip.at(idx) || eval_flags.at(idx)) {
      continue;
    } else if (arg1_vec->is_null(idx) || arg2_vec->is_null(idx)) {
      res_vec->set_null(idx);
    } else {
      ObString str1 = arg1_vec->get_string(idx);
      ObString str2 = arg2_vec->get_string(idx);
      uint64_t distance = 0;
      if (OB_FAIL(compute_levenshtein_utf8(str1.ptr(), str1.length(), cs_type_str1,
                                           str2.ptr(), str2.length(), cs_type_str2,
                                           dists, distance))) {
        LOG_WARN("failed to compute levenshtein distance for utf8", K(ret), K(idx), K(str1), K(str2));
      } else {
        res_vec->set_int(idx, distance);
      }
    }
  }

  return ret;
}

int ObExprEditDistanceUTF8::calc_edit_distance_utf8_vector(VECTOR_EVAL_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("failed to eval vector arg0", K(ret));
  } else if (OB_FAIL(expr.args_[1]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("failed to eval vector arg1", K(ret));
  } else {
    VectorFormat arg0_format = expr.args_[0]->get_format(ctx);
    VectorFormat arg1_format = expr.args_[1]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    if (VEC_UNIFORM == arg0_format) {
      if (VEC_UNIFORM == arg1_format) {
        if (VEC_UNIFORM == res_format) {
          ret = calc_edit_distance_utf8_vector_dispatch<StrUniVec, StrUniVec, UIntegerUniVec>(expr, ctx, skip, bound);
        } else if (VEC_FIXED == res_format) {
          ret = calc_edit_distance_utf8_vector_dispatch<StrUniVec, StrUniVec, UIntegerFixedVec>(expr, ctx, skip, bound);
        } else {
          ret = calc_edit_distance_utf8_vector_dispatch<StrUniVec, StrUniVec, ObVectorBase>(expr, ctx, skip, bound);
        }
      } else if (VEC_DISCRETE == arg1_format) {
        if (VEC_UNIFORM == res_format) {
          ret = calc_edit_distance_utf8_vector_dispatch<StrUniVec, StrDiscVec, UIntegerUniVec>(expr, ctx, skip, bound);
        } else if (VEC_FIXED == res_format) {
          ret = calc_edit_distance_utf8_vector_dispatch<StrUniVec, StrDiscVec, UIntegerFixedVec>(expr, ctx, skip, bound);
        } else {
          ret = calc_edit_distance_utf8_vector_dispatch<StrUniVec, StrDiscVec, ObVectorBase>(expr, ctx, skip, bound);
        }
      } else if (VEC_CONTINUOUS == arg1_format) {
        if (VEC_UNIFORM == res_format) {
          ret = calc_edit_distance_utf8_vector_dispatch<StrUniVec, StrContVec, UIntegerUniVec>(expr, ctx, skip, bound);
        } else if (VEC_FIXED == res_format) {
          ret = calc_edit_distance_utf8_vector_dispatch<StrUniVec, StrContVec, UIntegerFixedVec>(expr, ctx, skip, bound);
        } else {
          ret = calc_edit_distance_utf8_vector_dispatch<StrUniVec, StrContVec, ObVectorBase>(expr, ctx, skip, bound);
        }
      } else {
        ret = calc_edit_distance_utf8_vector_dispatch<StrUniVec, ObVectorBase, ObVectorBase>(expr, ctx, skip, bound);
      }
    } else if (VEC_DISCRETE == arg0_format) {
      if (VEC_UNIFORM == arg1_format) {
        if (VEC_UNIFORM == res_format) {
          ret = calc_edit_distance_utf8_vector_dispatch<StrDiscVec, StrUniVec, UIntegerUniVec>(expr, ctx, skip, bound);
        } else if (VEC_FIXED == res_format) {
          ret = calc_edit_distance_utf8_vector_dispatch<StrDiscVec, StrUniVec, UIntegerFixedVec>(expr, ctx, skip, bound);
        } else {
          ret = calc_edit_distance_utf8_vector_dispatch<StrDiscVec, StrUniVec, ObVectorBase>(expr, ctx, skip, bound);
        }
      } else if (VEC_DISCRETE == arg1_format) {
        if (VEC_UNIFORM == res_format) {
          ret = calc_edit_distance_utf8_vector_dispatch<StrDiscVec, StrDiscVec, UIntegerUniVec>(expr, ctx, skip, bound);
        } else if (VEC_FIXED == res_format) {
          ret = calc_edit_distance_utf8_vector_dispatch<StrDiscVec, StrDiscVec, UIntegerFixedVec>(expr, ctx, skip, bound);
        } else {
          ret = calc_edit_distance_utf8_vector_dispatch<StrDiscVec, StrDiscVec, ObVectorBase>(expr, ctx, skip, bound);
        }
      } else if (VEC_CONTINUOUS == arg1_format) {
        if (VEC_UNIFORM == res_format) {
          ret = calc_edit_distance_utf8_vector_dispatch<StrDiscVec, StrContVec, UIntegerUniVec>(expr, ctx, skip, bound);
        } else if (VEC_FIXED == res_format) {
          ret = calc_edit_distance_utf8_vector_dispatch<StrDiscVec, StrContVec, UIntegerFixedVec>(expr, ctx, skip, bound);
        } else {
          ret = calc_edit_distance_utf8_vector_dispatch<StrDiscVec, StrContVec, ObVectorBase>(expr, ctx, skip, bound);
        }
      } else {
        ret = calc_edit_distance_utf8_vector_dispatch<StrDiscVec, ObVectorBase, ObVectorBase>(expr, ctx, skip, bound);
      }
    } else if (VEC_CONTINUOUS == arg0_format) {
      if (VEC_UNIFORM == arg1_format) {
        if (VEC_UNIFORM == res_format) {
          ret = calc_edit_distance_utf8_vector_dispatch<StrContVec, StrUniVec, UIntegerUniVec>(expr, ctx, skip, bound);
        } else if (VEC_FIXED == res_format) {
          ret = calc_edit_distance_utf8_vector_dispatch<StrContVec, StrUniVec, UIntegerFixedVec>(expr, ctx, skip, bound);
        } else {
          ret = calc_edit_distance_utf8_vector_dispatch<StrContVec, StrUniVec, ObVectorBase>(expr, ctx, skip, bound);
        }
      } else if (VEC_DISCRETE == arg1_format) {
        if (VEC_UNIFORM == res_format) {
          ret = calc_edit_distance_utf8_vector_dispatch<StrContVec, StrDiscVec, UIntegerUniVec>(expr, ctx, skip, bound);
        } else if (VEC_FIXED == res_format) {
          ret = calc_edit_distance_utf8_vector_dispatch<StrContVec, StrDiscVec, UIntegerFixedVec>(expr, ctx, skip, bound);
        } else {
          ret = calc_edit_distance_utf8_vector_dispatch<StrContVec, StrDiscVec, ObVectorBase>(expr, ctx, skip, bound);
        }
      } else if (VEC_CONTINUOUS == arg1_format) {
        if (VEC_UNIFORM == res_format) {
          ret = calc_edit_distance_utf8_vector_dispatch<StrContVec, StrContVec, UIntegerUniVec>(expr, ctx, skip, bound);
        } else if (VEC_FIXED == res_format) {
          ret = calc_edit_distance_utf8_vector_dispatch<StrContVec, StrContVec, UIntegerFixedVec>(expr, ctx, skip, bound);
        } else {
          ret = calc_edit_distance_utf8_vector_dispatch<StrContVec, StrContVec, ObVectorBase>(expr, ctx, skip, bound);
        }
      } else {
        ret = calc_edit_distance_utf8_vector_dispatch<StrContVec, ObVectorBase, ObVectorBase>(expr, ctx, skip, bound);
      }
    } else {
      ret = calc_edit_distance_utf8_vector_dispatch<ObVectorBase, ObVectorBase, ObVectorBase>(expr, ctx, skip, bound);
    }
  }

  return ret;
}

}  // namespace sql
}  // namespace oceanbase
