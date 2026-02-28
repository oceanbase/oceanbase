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
#include "ob_expr_sha.h"

#include "sql/engine/expr/ob_datum_cast.h"

using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase
{
namespace sql
{

ObExprSha::ObExprSha(ObIAllocator &alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_SHA, N_SHA, 1, VALID_FOR_GENERATED_COL)
{
}

ObExprSha::~ObExprSha()
{
}

int ObExprSha::calc_result_type1(ObExprResType &type,
                                 ObExprResType &type1,
                                 common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  ObExprResType tmp_type;
  int64_t length = 0;
  type.set_varchar();
  type.set_collation_type(type_ctx.get_coll_type());
  type.set_collation_level(common::CS_LEVEL_COERCIBLE);
  type1.set_calc_type(ObVarcharType);
  OZ (aggregate_charsets_for_string_result(tmp_type, &type1, 1, type_ctx));
  OX (type1.set_calc_collation_type(tmp_type.get_collation_type()));
  OX (type1.set_calc_collation_level(tmp_type.get_collation_level()));

  OZ (ObHashUtil::get_hash_output_len(OB_HASH_SH1, length));
  OX (type.set_length(length * 2));
  return ret;
}

int ObExprSha::cg_expr(ObExprCGCtx &, const ObRawExpr &, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  rt_expr.eval_func_ = &ObExprSha::eval_sha;
  rt_expr.eval_vector_func_ = &ObExprSha::eval_sha_vector;
  return ret;
}

int ObExprSha::eval_sha(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *arg = NULL;
  if (OB_FAIL(expr.eval_param_value(ctx, arg))) {
    LOG_WARN("evaluate parameter value failed", K(ret));
  } else if (OB_ISNULL(arg)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("arg is null", K(ret));
  } else if (arg->is_null()) {
    expr_datum.set_null();
  } else {
    ObString text = arg->get_string();
    ObString sha_str;
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    if (OB_FAIL(ObHashUtil::hash(OB_HASH_SH1, text, alloc_guard.get_allocator(), sha_str))) {
      LOG_WARN("fail to calc sha", K(text), K(ret));
    } else if (OB_FAIL(ObDatumHexUtils::hex(expr, sha_str, ctx, alloc_guard.get_allocator(),
                                            expr_datum, false))) {
      LOG_WARN("fail to conver sha_str to hex", K(sha_str), K(ret));
    }
  }
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprSha, raw_expr) {
  int ret = OB_SUCCESS;
  SET_LOCAL_SYSVAR_CAPACITY(1);
  EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_COLLATION_CONNECTION);
  return ret;
}

// ==================== Vector Evaluation Implementation ====================
int ObExprSha::eval_sha_vector(VECTOR_EVAL_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("evaluate vector parameter failed", K(ret));
  } else {
    VectorFormat arg_format = expr.args_[0]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);

    if (VEC_UNIFORM == arg_format) {
      if (VEC_UNIFORM == res_format) {
        ret = sha_string_vector<StrUniVec, StrUniVec>(VECTOR_EVAL_FUNC_ARG_LIST);
      } else if (VEC_DISCRETE == res_format) {
        ret = sha_string_vector<StrUniVec, StrDiscVec>(VECTOR_EVAL_FUNC_ARG_LIST);
      } else if (VEC_CONTINUOUS == res_format) {
        ret = sha_string_vector<StrUniVec, StrContVec>(VECTOR_EVAL_FUNC_ARG_LIST);
      } else {
        ret = sha_string_vector<StrUniVec, ObVectorBase>(VECTOR_EVAL_FUNC_ARG_LIST);
      }
    } else if (VEC_DISCRETE == arg_format) {
      if (VEC_UNIFORM == res_format) {
        ret = sha_string_vector<StrDiscVec, StrUniVec>(VECTOR_EVAL_FUNC_ARG_LIST);
      } else if (VEC_DISCRETE == res_format) {
        ret = sha_string_vector<StrDiscVec, StrDiscVec>(VECTOR_EVAL_FUNC_ARG_LIST);
      } else if (VEC_CONTINUOUS == res_format) {
        ret = sha_string_vector<StrDiscVec, StrContVec>(VECTOR_EVAL_FUNC_ARG_LIST);
      } else {
        ret = sha_string_vector<StrDiscVec, ObVectorBase>(VECTOR_EVAL_FUNC_ARG_LIST);
      }
    } else if (VEC_CONTINUOUS == arg_format) {
      if (VEC_UNIFORM == res_format) {
        ret = sha_string_vector<StrContVec, StrUniVec>(VECTOR_EVAL_FUNC_ARG_LIST);
      } else if (VEC_DISCRETE == res_format) {
        ret = sha_string_vector<StrContVec, StrDiscVec>(VECTOR_EVAL_FUNC_ARG_LIST);
      } else if (VEC_CONTINUOUS == res_format) {
        ret = sha_string_vector<StrContVec, StrContVec>(VECTOR_EVAL_FUNC_ARG_LIST);
      } else {
        ret = sha_string_vector<StrContVec, ObVectorBase>(VECTOR_EVAL_FUNC_ARG_LIST);
      }
    } else {
      ret = sha_string_vector<ObVectorBase, ObVectorBase>(VECTOR_EVAL_FUNC_ARG_LIST);
    }
  }

  return ret;
}

template <typename ArgVec, typename ResVec>
int ObExprSha::sha_string_vector(VECTOR_EVAL_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  ArgVec *arg_vec = static_cast<ArgVec *>(expr.args_[0]->get_vector(ctx));
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);

  ObCollationType def_cs = ObCharset::get_system_collation();
  ObCollationType dst_cs = expr.datum_meta_.cs_type_;
  bool need_convert_coll = false;
  if (OB_FAIL(ObExprUtil::need_convert_string_collation(def_cs, dst_cs, need_convert_coll))) {
    LOG_WARN("check need convert cs type failed", K(ret), K(def_cs), K(dst_cs));
  }

  for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
    if (skip.at(idx) || eval_flags.at(idx)) {
      continue;
    } else if (arg_vec->is_null(idx)) {
      res_vec->set_null(idx);
    } else {
      ObEvalCtx::TempAllocGuard alloc_guard(ctx);
      ObString input_str = arg_vec->get_string(idx);
      ObString sha_str;
      if (OB_FAIL(ObHashUtil::hash(OB_HASH_SH1, input_str, alloc_guard.get_allocator(), sha_str))) {
        LOG_WARN("fail to calc sha", K(input_str), K(ret));
      } else if (OB_FAIL(ObDatumHexUtils::hex<ResVec>(expr, sha_str, ctx, alloc_guard.get_allocator(),
                                                      *res_vec, idx, need_convert_coll, false))) {
        LOG_WARN("fail to convert sha_str to hex", K(sha_str), K(ret));
      }
    }

  }

  return ret;
}

ObExprSha2::ObExprSha2(ObIAllocator &alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_SHA2, N_SHA2, 2, VALID_FOR_GENERATED_COL)
{
}

ObExprSha2::~ObExprSha2()
{
}

int ObExprSha2::calc_result_type2(ObExprResType &type,
                                 ObExprResType &type1,
                                 ObExprResType &type2,
                                 common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  ObExprResType tmp_type;
  int64_t length = 0;
  type.set_varchar();
  type.set_collation_type(type_ctx.get_coll_type());
  type.set_collation_level(common::CS_LEVEL_COERCIBLE);
  type1.set_calc_type(ObVarcharType);
  OZ (aggregate_charsets_for_string_result(tmp_type, &type1, 1, type_ctx));
  OX (type1.set_calc_collation_type(tmp_type.get_collation_type()));
  OX (type1.set_calc_collation_level(tmp_type.get_collation_level()));
  OX (type2.set_calc_type(ObIntType));
  OZ (ObHashUtil::get_hash_output_len(OB_HASH_SH512, length));
  OX (type.set_length(length * 2));
  return ret;
}

int ObExprSha2::cg_expr(ObExprCGCtx &, const ObRawExpr &, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  rt_expr.eval_func_ = &ObExprSha2::eval_sha2;
  rt_expr.eval_vector_func_ = &ObExprSha2::eval_sha2_vector;
  return ret;
}

int ObExprSha2::eval_sha2(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *arg0 = NULL;
  ObDatum *arg1 = NULL;
  if (OB_FAIL(expr.eval_param_value(ctx, arg0, arg1))) {
    LOG_WARN("evaluate parameter value failed", K(ret));
  } else if (OB_ISNULL(arg0) || OB_ISNULL(arg1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("arg is null", K(ret));
  } else if (arg0->is_null() || arg1->is_null()) {
    expr_datum.set_null();
  } else {
    ObString text = arg0->get_string();
    int64_t sha_bit_len = arg1->get_int();
    ObHashAlgorithm algo = OB_HASH_INVALID;
    ObString sha_str;
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    if (0 == sha_bit_len) {
      sha_bit_len = 256;
    }
    if (OB_FAIL(ObHashUtil::get_sha_hash_algorightm(sha_bit_len, algo))) {
      ret = OB_SUCCESS;
      expr_datum.set_null();
      LOG_WARN("fail to get hash algorithm", K(sha_bit_len), K(ret));
    } else if (OB_FAIL(ObHashUtil::hash(algo, text, alloc_guard.get_allocator(), sha_str))) {
      LOG_WARN("fail to calc sha", K(text), K(ret));
    } else if (OB_FAIL(ObDatumHexUtils::hex(expr, sha_str, ctx, alloc_guard.get_allocator(),
                                            expr_datum, false))) {
      LOG_WARN("fail to convert sha_str to hex", K(sha_str), K(ret));
    }
  }
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprSha2, raw_expr) {
  int ret = OB_SUCCESS;
  SET_LOCAL_SYSVAR_CAPACITY(1);
  EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_COLLATION_CONNECTION);
  return ret;
}

// ==================== Vector Evaluation Implementation ====================
int ObExprSha2::eval_sha2_vector(VECTOR_EVAL_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval text param", K(ret));
  } else if (OB_FAIL(expr.args_[1]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval bit_len param", K(ret));
  } else {
    VectorFormat text_format = expr.args_[0]->get_format(ctx);
    VectorFormat bit_len_format = expr.args_[1]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);

    if (OB_LIKELY(expr.args_[1]->is_static_const_)) {
      ObIVector *sha_bit_len_vec = expr.args_[1]->get_vector(ctx);
      ObHashAlgorithm algo = OB_HASH_INVALID;
      if (sha_bit_len_vec->is_null(0)) {  // do nothing
      } else {
        int64_t sha_bit_len = sha_bit_len_vec->get_int(0);
        sha_bit_len = (0 == sha_bit_len) ? 256 : sha_bit_len;
        if (OB_FAIL(ObHashUtil::get_sha_hash_algorightm(sha_bit_len, algo))) {
          // an invalid hash algo generates null result later, no need to return error
          ret = OB_SUCCESS;
          LOG_WARN("fail to get hash algorithm", K(sha_bit_len), K(ret));
        }
      }
      if (VEC_UNIFORM == text_format) {
        if (VEC_UNIFORM == res_format) {
          ret = sha2_vector_with_hash_algo<StrUniVec, StrUniVec>(VECTOR_EVAL_FUNC_ARG_LIST, algo);
        } else if (VEC_DISCRETE == res_format) {
          ret = sha2_vector_with_hash_algo<StrUniVec, StrDiscVec>(VECTOR_EVAL_FUNC_ARG_LIST, algo);
        } else if (VEC_CONTINUOUS == res_format) {
          ret = sha2_vector_with_hash_algo<StrUniVec, StrContVec>(VECTOR_EVAL_FUNC_ARG_LIST, algo);
        } else {
          ret = sha2_vector_with_hash_algo<StrUniVec, ObVectorBase>(VECTOR_EVAL_FUNC_ARG_LIST, algo);
        }
      } else if (VEC_DISCRETE == text_format) {
        if (VEC_UNIFORM == res_format) {
          ret = sha2_vector_with_hash_algo<StrDiscVec, StrUniVec>(VECTOR_EVAL_FUNC_ARG_LIST, algo);
        } else if (VEC_DISCRETE == res_format) {
          ret = sha2_vector_with_hash_algo<StrDiscVec, StrDiscVec>(VECTOR_EVAL_FUNC_ARG_LIST, algo);
        } else if (VEC_CONTINUOUS == res_format) {
          ret = sha2_vector_with_hash_algo<StrDiscVec, StrContVec>(VECTOR_EVAL_FUNC_ARG_LIST, algo);
        } else {
          ret = sha2_vector_with_hash_algo<StrDiscVec, ObVectorBase>(VECTOR_EVAL_FUNC_ARG_LIST, algo);
        }
      } else if (VEC_CONTINUOUS == text_format) {
        if (VEC_UNIFORM == res_format) {
          ret = sha2_vector_with_hash_algo<StrContVec, StrUniVec>(VECTOR_EVAL_FUNC_ARG_LIST, algo);
        } else if (VEC_DISCRETE == res_format) {
          ret = sha2_vector_with_hash_algo<StrContVec, StrDiscVec>(VECTOR_EVAL_FUNC_ARG_LIST, algo);
        } else if (VEC_CONTINUOUS == res_format) {
          ret = sha2_vector_with_hash_algo<StrContVec, StrContVec>(VECTOR_EVAL_FUNC_ARG_LIST, algo);
        } else {
          ret = sha2_vector_with_hash_algo<StrContVec, ObVectorBase>(VECTOR_EVAL_FUNC_ARG_LIST, algo);
        }
      } else {
        ret = sha2_vector_with_hash_algo<ObVectorBase, ObVectorBase>(VECTOR_EVAL_FUNC_ARG_LIST, algo);
      }
    } else {
      if (VEC_UNIFORM == text_format && VEC_UNIFORM == bit_len_format) {
        if (VEC_UNIFORM == res_format) {
          ret = sha2_vector<StrUniVec, IntegerUniVec, StrUniVec>(VECTOR_EVAL_FUNC_ARG_LIST);
        } else if (VEC_DISCRETE == res_format) {
          ret = sha2_vector<StrUniVec, IntegerUniVec, StrDiscVec>(VECTOR_EVAL_FUNC_ARG_LIST);
        } else if (VEC_CONTINUOUS == res_format) {
          ret = sha2_vector<StrUniVec, IntegerUniVec, StrContVec>(VECTOR_EVAL_FUNC_ARG_LIST);
        } else {
          ret = sha2_vector<StrUniVec, IntegerUniVec, ObVectorBase>(VECTOR_EVAL_FUNC_ARG_LIST);
        }
      } else if (VEC_UNIFORM == text_format && VEC_FIXED == bit_len_format) {
        if (VEC_UNIFORM == res_format) {
          ret = sha2_vector<StrUniVec, IntegerFixedVec, StrUniVec>(VECTOR_EVAL_FUNC_ARG_LIST);
        } else if (VEC_DISCRETE == res_format) {
          ret = sha2_vector<StrUniVec, IntegerFixedVec, StrDiscVec>(VECTOR_EVAL_FUNC_ARG_LIST);
        } else if (VEC_CONTINUOUS == res_format) {
          ret = sha2_vector<StrUniVec, IntegerFixedVec, StrContVec>(VECTOR_EVAL_FUNC_ARG_LIST);
        } else {
          ret = sha2_vector<StrUniVec, IntegerFixedVec, ObVectorBase>(VECTOR_EVAL_FUNC_ARG_LIST);
        }
      } else if (VEC_DISCRETE == text_format && VEC_UNIFORM == bit_len_format) {
        if (VEC_UNIFORM == res_format) {
          ret = sha2_vector<StrDiscVec, IntegerUniVec, StrUniVec>(VECTOR_EVAL_FUNC_ARG_LIST);
        } else if (VEC_DISCRETE == res_format) {
          ret = sha2_vector<StrDiscVec, IntegerUniVec, StrDiscVec>(VECTOR_EVAL_FUNC_ARG_LIST);
        } else if (VEC_CONTINUOUS == res_format) {
          ret = sha2_vector<StrDiscVec, IntegerUniVec, StrContVec>(VECTOR_EVAL_FUNC_ARG_LIST);
        } else {
          ret = sha2_vector<StrDiscVec, IntegerUniVec, ObVectorBase>(VECTOR_EVAL_FUNC_ARG_LIST);
        }
      } else if (VEC_DISCRETE == text_format && VEC_FIXED == bit_len_format) {
        if (VEC_UNIFORM == res_format) {
          ret = sha2_vector<StrDiscVec, IntegerFixedVec, StrUniVec>(VECTOR_EVAL_FUNC_ARG_LIST);
        } else if (VEC_DISCRETE == res_format) {
          ret = sha2_vector<StrDiscVec, IntegerFixedVec, StrDiscVec>(VECTOR_EVAL_FUNC_ARG_LIST);
        } else if (VEC_CONTINUOUS == res_format) {
          ret = sha2_vector<StrDiscVec, IntegerFixedVec, StrContVec>(VECTOR_EVAL_FUNC_ARG_LIST);
        } else {
          ret = sha2_vector<StrDiscVec, IntegerFixedVec, ObVectorBase>(VECTOR_EVAL_FUNC_ARG_LIST);
        }
      } else if (VEC_CONTINUOUS == text_format && VEC_UNIFORM == bit_len_format) {
        if (VEC_UNIFORM == res_format) {
          ret = sha2_vector<StrContVec, IntegerUniVec, StrUniVec>(VECTOR_EVAL_FUNC_ARG_LIST);
        } else if (VEC_DISCRETE == res_format) {
          ret = sha2_vector<StrContVec, IntegerUniVec, StrDiscVec>(VECTOR_EVAL_FUNC_ARG_LIST);
        } else if (VEC_CONTINUOUS == res_format) {
          ret = sha2_vector<StrContVec, IntegerUniVec, StrContVec>(VECTOR_EVAL_FUNC_ARG_LIST);
        } else {
          ret = sha2_vector<StrContVec, IntegerUniVec, ObVectorBase>(VECTOR_EVAL_FUNC_ARG_LIST);
        }
      } else if (VEC_CONTINUOUS == text_format && VEC_FIXED == bit_len_format) {
        if (VEC_UNIFORM == res_format) {
          ret = sha2_vector<StrContVec, IntegerFixedVec, StrUniVec>(VECTOR_EVAL_FUNC_ARG_LIST);
        } else if (VEC_DISCRETE == res_format) {
          ret = sha2_vector<StrContVec, IntegerFixedVec, StrDiscVec>(VECTOR_EVAL_FUNC_ARG_LIST);
        } else if (VEC_CONTINUOUS == res_format) {
          ret = sha2_vector<StrContVec, IntegerFixedVec, StrContVec>(VECTOR_EVAL_FUNC_ARG_LIST);
        } else {
          ret = sha2_vector<StrContVec, IntegerFixedVec, ObVectorBase>(VECTOR_EVAL_FUNC_ARG_LIST);
        }
      } else {
        ret = sha2_vector<ObVectorBase, ObVectorBase, ObVectorBase>(VECTOR_EVAL_FUNC_ARG_LIST);
      }
    }
  }

  return ret;
}

template <typename ArgVec1, typename ArgVec2, typename ResVec>
int ObExprSha2::sha2_vector(VECTOR_EVAL_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  ArgVec1 *arg_vec1 = static_cast<ArgVec1 *>(expr.args_[0]->get_vector(ctx));
  ArgVec2 *arg_vec2 = static_cast<ArgVec2 *>(expr.args_[1]->get_vector(ctx));
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);

  ObCollationType def_cs = ObCharset::get_system_collation();
  ObCollationType dst_cs = expr.datum_meta_.cs_type_;
  bool need_convert_coll = false;
  if (OB_FAIL(ObExprUtil::need_convert_string_collation(def_cs, dst_cs, need_convert_coll))) {
    LOG_WARN("check need convert cs type failed", K(ret), K(def_cs), K(dst_cs));
  }

  for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
    if (skip.at(idx) || eval_flags.at(idx)) {
      continue;
    } else if (arg_vec1->is_null(idx) || arg_vec2->is_null(idx)) {
      res_vec->set_null(idx);
    } else {
      ObEvalCtx::TempAllocGuard alloc_guard(ctx);
      ObString text = arg_vec1->get_string(idx);
      int64_t sha_bit_len = arg_vec2->get_int(idx);
      ObHashAlgorithm algo = OB_HASH_INVALID;
      ObString sha_str;

      if (0 == sha_bit_len) {
        sha_bit_len = 256;
      }
      if (OB_FAIL(ObHashUtil::get_sha_hash_algorightm(sha_bit_len, algo))) {
        ret = OB_SUCCESS;
        res_vec->set_null(idx);
        LOG_WARN("fail to get hash algorithm", K(sha_bit_len), K(ret));
      } else if (OB_FAIL(ObHashUtil::hash(algo, text, alloc_guard.get_allocator(), sha_str))) {
        LOG_WARN("fail to calc sha", K(text), K(ret));
      } else if (OB_FAIL(ObDatumHexUtils::hex<ResVec>(expr, sha_str, ctx, alloc_guard.get_allocator(),
                                                      *res_vec, idx, need_convert_coll, false))) {
        LOG_WARN("fail to convert sha_str to hex", K(sha_str), K(ret));
      }
    }

  }

  return ret;
}

template <typename ArgVec1, typename ResVec>
int ObExprSha2::sha2_vector_with_hash_algo(VECTOR_EVAL_FUNC_ARG_DECL, share::ObHashAlgorithm hash_algo)
{
  int ret = OB_SUCCESS;
  ArgVec1 *arg_vec1 = static_cast<ArgVec1 *>(expr.args_[0]->get_vector(ctx));
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);

  ObCollationType def_cs = ObCharset::get_system_collation();
  ObCollationType dst_cs = expr.datum_meta_.cs_type_;
  bool need_convert_coll = false;
  if (OB_FAIL(ObExprUtil::need_convert_string_collation(def_cs, dst_cs, need_convert_coll))) {
    LOG_WARN("check need convert cs type failed", K(ret), K(def_cs), K(dst_cs));
  }

  for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
    if (skip.at(idx) || eval_flags.at(idx)) {
      continue;
    } else if (arg_vec1->is_null(idx) || OB_UNLIKELY(hash_algo == OB_HASH_INVALID)) {
      res_vec->set_null(idx);
    } else {
      ObEvalCtx::TempAllocGuard alloc_guard(ctx);
      ObString text = arg_vec1->get_string(idx);
      ObString sha_str;
      if (OB_FAIL(ObHashUtil::hash(hash_algo, text, alloc_guard.get_allocator(), sha_str))) {
        LOG_WARN("fail to calc sha", K(text), K(ret));
      } else if (OB_FAIL(ObDatumHexUtils::hex<ResVec>(expr, sha_str, ctx, alloc_guard.get_allocator(),
                                                      *res_vec, idx, need_convert_coll, false))) {
        LOG_WARN("fail to convert sha_str to hex", K(sha_str), K(ret));
      }
    }

  }

  return ret;
}


ObExprSm3::ObExprSm3(ObIAllocator &alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_SM3, N_SM3, 1, VALID_FOR_GENERATED_COL) { }

ObExprSm3::~ObExprSm3() { }

int ObExprSm3::calc_result_type1(ObExprResType &type,
                                 ObExprResType &type1,
                                 common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  ObExprResType tmp_type;
  int64_t length = 0;
  type.set_varchar();
  type.set_collation_type(type_ctx.get_coll_type());
  type.set_collation_level(common::CS_LEVEL_COERCIBLE);
  type1.set_calc_type(ObVarcharType);
  OZ (aggregate_charsets_for_string_result(tmp_type, &type1, 1, type_ctx));
  OX (type1.set_calc_collation_type(tmp_type.get_collation_type()));
  OX (type1.set_calc_collation_level(tmp_type.get_collation_level()));
  OZ (ObHashUtil::get_hash_output_len(OB_HASH_SM3, length));
  OX (type.set_length(length * 2));
  return ret;
}

int ObExprSm3::cg_expr(ObExprCGCtx &, const ObRawExpr &, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  rt_expr.eval_func_ = &ObExprSm3::eval_sm3;
  return ret;
}

int ObExprSm3::eval_sm3(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *arg = NULL;
  if (OB_FAIL(expr.eval_param_value(ctx, arg))) {
    LOG_WARN("evaluate parameter value failed", K(ret));
  } else if (OB_ISNULL(arg)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("arg is null", K(ret));
  } else if (arg->is_null()) {
    expr_datum.set_null();
  } else {
    ObString text = arg->get_string();
    ObString sha_str;
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    if (OB_FAIL(ObHashUtil::hash(OB_HASH_SM3, text, alloc_guard.get_allocator(), sha_str))) {
      LOG_WARN("fail to calc sha", K(text), K(ret));
    } else if (OB_FAIL(ObDatumHexUtils::hex(expr, sha_str, ctx, alloc_guard.get_allocator(),
                                            expr_datum, false))) {
      LOG_WARN("fail to conver sha_str to hex", K(sha_str), K(ret));
    }
  }
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprSm3, raw_expr) {
  int ret = OB_SUCCESS;
  SET_LOCAL_SYSVAR_CAPACITY(1);
  EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_COLLATION_CONNECTION);
  return ret;
}

}
}
