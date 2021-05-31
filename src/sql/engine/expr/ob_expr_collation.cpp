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
#include "ob_expr_collation.h"
#include "lib/ob_name_def.h"
#include "lib/utility/utility.h"
//#include "sql/engine/expr/ob_expr_promotion_util.h"
#include "sql/parser/ob_item_type.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase {
using namespace common;
namespace sql {
// ObExprCharset::ObExprCharset()
//    :ObStringExprOperator(T_FUN_SYS_CHARSET, N_CHARSET, 1)
//{}

ObExprCharset::ObExprCharset(ObIAllocator& alloc) : ObStringExprOperator(alloc, T_FUN_SYS_CHARSET, N_CHARSET, 1)
{}

ObExprCharset::~ObExprCharset()
{}

int ObExprCharset::calc_result_type1(ObExprResType& type, ObExprResType& type1, ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  UNUSED(type1);
  int ret = OB_SUCCESS;
  type.set_varchar();
  type.set_default_collation_type();
  type.set_collation_level(CS_LEVEL_COERCIBLE);

  int64_t mbmaxlen = 1;
  if (OB_FAIL(ObCharset::get_mbmaxlen_by_coll(type.get_collation_type(), mbmaxlen))) {
    LOG_WARN("fail get mbmaxlen", K(mbmaxlen), K(ret));
  } else {
    type.set_length(static_cast<ObLength>(64 * mbmaxlen));
  }

  return ret;
}

int ObExprCharset::calc_result1(common::ObObj& result, const common::ObObj& obj, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  ObString charset = ObString::make_string(ObCharset::collation_name(CS_TYPE_BINARY));
  if (OB_ISNULL(expr_ctx.calc_buf_) || OB_ISNULL(expr_ctx.my_session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("the pointer expr_ctx.calc_buf_ or expr_ctx.my_session_ is null",
        K(expr_ctx.my_session_),
        K(expr_ctx.calc_buf_),
        K(ret));
  } else if (obj.is_null()) {
    result.set_varchar(charset);
    result.set_collation(result_type_);
  } else {
    ObCharsetType charset_type = ObCharset::charset_type_by_coll(obj.get_collation_type());
    if (CHARSET_INVALID == charset_type) {
      LOG_WARN("invalid collation type", K(obj));
      ret = OB_ERR_UNEXPECTED;
    } else {
      charset = ObCharset::charset_name(charset_type);
      result.set_varchar(charset);
      result.set_collation(result_type_);
    }
  }
  return ret;
}

int calc_charset_expr(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* arg = NULL;
  ObString charset_str = ObString::make_string(ObCharset::collation_name(CS_TYPE_BINARY));
  if (OB_FAIL(expr.args_[0]->eval(ctx, arg))) {
    LOG_WARN("eval arg failed", K(ret));
  } else {
    const ObCollationType& cs_type = expr.args_[0]->datum_meta_.cs_type_;
    ObCharsetType charset_type = ObCharset::charset_type_by_coll(cs_type);
    if (CHARSET_INVALID == charset_type) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid collation type", K(ret), K(cs_type));
    } else {
      charset_str = ObCharset::charset_name(charset_type);
      res_datum.set_string(charset_str);
    }
  }
  return ret;
}

int ObExprCharset::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_charset_expr;
  return ret;
}

}  // end namespace sql
}  // end namespace oceanbase
////////////////////////////////////////////////////////////////
namespace oceanbase {
using namespace common;
namespace sql {
// ObExprCollation::ObExprCollation()
//    :ObStringExprOperator(T_FUN_SYS_COLLATION, N_COLLATION, 1)
//{}

ObExprCollation::ObExprCollation(ObIAllocator& alloc) : ObStringExprOperator(alloc, T_FUN_SYS_COLLATION, N_COLLATION, 1)
{
  need_charset_convert_ = false;
}

ObExprCollation::~ObExprCollation()
{}

int ObExprCollation::calc_result_type1(ObExprResType& type, ObExprResType& type1, ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  UNUSED(type1);

  int ret = OB_SUCCESS;
  type.set_varchar();
  type.set_default_collation_type();
  type.set_collation_level(CS_LEVEL_COERCIBLE);

  int64_t mbmaxlen = 1;
  if (OB_FAIL(ObCharset::get_mbmaxlen_by_coll(type.get_collation_type(), mbmaxlen))) {
    LOG_WARN("fail get mbmaxlen", K(mbmaxlen), K(ret));
  } else {
    type.set_length(static_cast<ObLength>(64 * mbmaxlen));
  }

  return ret;
}

int ObExprCollation::calc_result1(common::ObObj& result, const common::ObObj& obj, ObExprCtx& expr_ctx) const
{
  UNUSED(expr_ctx);
  int ret = OB_SUCCESS;
  ObString collation = ObCharset::collation_name(obj.get_collation_type());
  result.set_varchar(collation);
  result.set_collation(result_type_);
  return ret;
}

int calc_collation_expr(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* arg = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, arg))) {
    LOG_WARN("eval arg failed", K(ret));
  } else {
    const ObCollationType& cs_type = expr.args_[0]->datum_meta_.cs_type_;
    ObString collation = ObCharset::collation_name(cs_type);
    res_datum.set_string(collation);
  }
  return ret;
}

int ObExprCollation::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_collation_expr;
  return ret;
}

}  // end namespace sql
}  // end namespace oceanbase

////////////////////////////////////////////////////////////////
namespace oceanbase {
using namespace common;
namespace sql {
// ObExprCoercibility::ObExprCoercibility()
//    :ObExprOperator(T_FUN_SYS_COERCIBILITY,
//                    N_COERCIBILITY,
//                    1,
//                    NOT_ROW_DIMENSION)
//{}

ObExprCoercibility::ObExprCoercibility(ObIAllocator& alloc)
    : ObExprOperator(alloc, T_FUN_SYS_COERCIBILITY, N_COERCIBILITY, 1, NOT_ROW_DIMENSION)
{
  disable_operand_auto_cast();
}

ObExprCoercibility::~ObExprCoercibility()
{}

int ObExprCoercibility::calc_result_type1(ObExprResType& type, ObExprResType& type1, ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  UNUSED(type1);
  int ret = OB_SUCCESS;
  type.set_int();
  type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].precision_);
  type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].scale_);
  return ret;
}

int ObExprCoercibility::calc_result1(common::ObObj& result, const common::ObObj& obj, ObExprCtx& expr_ctx) const
{
  UNUSED(expr_ctx);
  int ret = OB_SUCCESS;
  result.set_int(obj.get_collation_level());
  return ret;
}

int calc_coercibility_expr(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  // no need to eval arg
  res_datum.set_int(expr.args_[0]->obj_meta_.get_collation_level());
  return ret;
}

int ObExprCoercibility::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_coercibility_expr;
  return ret;
}

}  // end namespace sql
}  // end namespace oceanbase

////////////////////////////////////////////////////////////////
namespace oceanbase {
using namespace common;
namespace sql {
// ObExprSetCollation::ObExprSetCollation()
//    :ObExprOperator(T_FUN_SYS_SET_COLLATION,
//                    N_SET_COLLATION,
//                    2,
//                    NOT_ROW_DIMENSION)
//{}

ObExprSetCollation::ObExprSetCollation(ObIAllocator& alloc)
    : ObExprOperator(alloc, T_FUN_SYS_SET_COLLATION, N_SET_COLLATION, 2, NOT_ROW_DIMENSION)
{}

ObExprSetCollation::~ObExprSetCollation()
{}

int ObExprSetCollation::calc_result_type2(
    ObExprResType& type, ObExprResType& type1, ObExprResType& type2, ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  UNUSED(type1);
  const common::ObLengthSemantics default_length_semantics =
      (OB_NOT_NULL(type_ctx.get_session()) ? type_ctx.get_session()->get_actual_nls_length_semantics()
                                           : common::LS_BYTE);
  int ret = OB_SUCCESS;
  type.set_varchar();
  type.set_collation_level(CS_LEVEL_EXPLICIT);
  type.set_collation_type(type2.get_collation_type());
  // deduce length
  // set collation will not change the charset but only change the collation
  type.set_length(type1.get_length());
  type.set_length_semantics(type1.is_varchar_or_char() ? type1.get_length_semantics() : default_length_semantics);
  // set calc type
  if (OB_SUCC(ret) && type1.is_string_type()) {
    ObCollationType collation_type = type2.get_collation_type();
    ObCharsetType cs_type = type1.get_charset_type();
    if (!ObCharset::is_valid_collation(cs_type, collation_type)) {
      ret = OB_ERR_COLLATION_MISMATCH;
      const char* cs_name = ObCharset::charset_name(cs_type);
      const char* coll_name = ObCharset::collation_name(collation_type);
      ObString charset = ObString::make_string(cs_name);
      ObString collation = ObString::make_string(coll_name);
      LOG_USER_ERROR(OB_ERR_COLLATION_MISMATCH, collation.length(), collation.ptr(), charset.length(), charset.ptr());
    }
  }
  if (OB_SUCC(ret)) {
    type1.set_calc_type(ObVarcharType);
    type2.set_calc_type(ObIntType);
  }
  return ret;
}

int ObExprSetCollation::calc_result2(
    common::ObObj& result, const common::ObObj& obj1, const common::ObObj& obj2, common::ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (obj1.is_null()) {
    result.set_null();
  } else {
    TYPE_CHECK(obj1, ObVarcharType);
    TYPE_CHECK(obj2, ObIntType);
    ObCollationType collation_type = static_cast<ObCollationType>(obj2.get_int());
    ObCharsetType cs_type = ObCharset::charset_type_by_coll(obj1.get_collation_type());
    if (CS_TYPE_INVALID == collation_type) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid collation type", K(collation_type));
    } else if (CHARSET_INVALID == cs_type) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid charset type", K(cs_type), K(obj1));
    } else if (cs_type != CHARSET_BINARY && !ObCharset::is_valid_collation(cs_type, collation_type)) {
      ret = OB_ERR_COLLATION_MISMATCH;
      const char* cs_name = ObCharset::charset_name(cs_type);
      const char* coll_name = ObCharset::collation_name(collation_type);
      ObString charset = ObString::make_string(cs_name);
      ObString collation = ObString::make_string(coll_name);
      LOG_USER_ERROR(OB_ERR_COLLATION_MISMATCH, collation.length(), collation.ptr(), charset.length(), charset.ptr());
    } else {
      result = obj1;  // shallow copy, not necessary to deep copy
      result.set_collation_level(CS_LEVEL_EXPLICIT);
      result.set_collation_type(collation_type);
    }
  }
  UNUSED(expr_ctx);
  return ret;
}

int calc_set_collation_expr(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  // set_collation(dst_arg, cs_arg)
  ObDatum* dst_arg = NULL;
  ObDatum* cs_arg = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, dst_arg)) || OB_FAIL(expr.args_[1]->eval(ctx, cs_arg))) {
    LOG_WARN("eval arg failed", K(ret), KP(dst_arg), KP(cs_arg));
  } else if (dst_arg->is_null()) {
    res_datum.set_null();
  } else if (cs_arg->is_null()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cs_arg cannot be null", K(ret));
  } else {
    ObCollationType dst_coll = static_cast<ObCollationType>(cs_arg->get_int());
    ObCharsetType ori_charset_type = ObCharset::charset_type_by_coll(expr.args_[0]->datum_meta_.cs_type_);
    if (OB_UNLIKELY(CS_TYPE_INVALID == dst_coll || expr.datum_meta_.cs_type_ != dst_coll)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid collation type", K(ret), K(dst_coll), K(expr.datum_meta_.cs_type_));
    } else if (CHARSET_INVALID == ori_charset_type) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid charset type", K(ori_charset_type));
    } else if (ori_charset_type != CHARSET_BINARY && !ObCharset::is_valid_collation(ori_charset_type, dst_coll)) {
      ret = OB_ERR_COLLATION_MISMATCH;
      const char* cs_name = ObCharset::charset_name(ori_charset_type);
      const char* coll_name = ObCharset::collation_name(dst_coll);
      ObString charset = ObString::make_string(cs_name);
      ObString collation = ObString::make_string(coll_name);
      LOG_USER_ERROR(OB_ERR_COLLATION_MISMATCH, collation.length(), collation.ptr(), charset.length(), charset.ptr());
    } else {
      res_datum.set_datum(*dst_arg);
    }
  }
  return ret;
}

int ObExprSetCollation::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_set_collation_expr;
  return ret;
}

}  // end namespace sql
}  // end namespace oceanbase

////////////////////////////////////////////////////////////////
namespace oceanbase {
using namespace common;
namespace sql {

ObExprCmpMeta::ObExprCmpMeta(ObIAllocator& alloc) : ObStringExprOperator(alloc, T_FUN_SYS_CMP_META, "cmp_meta", 1)
{
  disable_operand_auto_cast();
}

ObExprCmpMeta::~ObExprCmpMeta()
{}

int ObExprCmpMeta::calc_result_type1(ObExprResType& type, ObExprResType& type1, ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);

  type.set_varchar();
  type.set_default_collation_type();
  type.set_collation_level(CS_LEVEL_COERCIBLE);
  type.set_calc_meta(type1.get_calc_meta());

  return OB_SUCCESS;
}

int ObExprCmpMeta::calc_result1(common::ObObj& result, const common::ObObj& obj, ObExprCtx& expr_ctx) const
{
  UNUSED(obj);
  int ret = OB_SUCCESS;
  ObString res_str;
  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("calc_buf is NULL", K(ret));
  } else if (OB_FAIL(ob_alloc_printf(res_str, *expr_ctx.calc_buf_, "%s", S(result_type_.get_calc_meta())))) {
    LOG_WARN("failed to print", K(ret));
  } else {
    result.set_varchar(res_str);
    result.set_collation(result_type_);
  }
  return ret;
}

int calc_cmp_meta_expr(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  ObExprStrResAlloc res_alloc(expr, ctx);
  ObString res_str;
  if (OB_FAIL(ob_alloc_printf(res_str, res_alloc, "%s", S(expr.args_[0]->obj_meta_)))) {
    LOG_WARN("failed to print", K(ret));
  } else {
    res_datum.set_string(res_str);
  }
  return ret;
}

int ObExprCmpMeta::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_cmp_meta_expr;
  return OB_SUCCESS;
}

}  // end namespace sql
}  // end namespace oceanbase
