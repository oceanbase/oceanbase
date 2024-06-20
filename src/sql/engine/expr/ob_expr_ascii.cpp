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
#include "lib/charset/ob_charset.h"
#include "lib/ob_name_def.h"
#include "lib/utility/ob_macro_utils.h"

#include "share/object/ob_obj_cast.h"
#include "sql/engine/expr/ob_expr_ascii.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

namespace oceanbase
{
using namespace common;

namespace sql
{
static const int64_t DEFAULT_ASCII_PRECISION_FOR_MYSQL = 3;
static const int64_t DEFAULT_ORD_PRECISION_FOR_MYSQL = 21;

ObExprAscii::ObExprAscii(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc,
                         T_FUN_SYS_ASCII,
                         N_ASCII, 1,
                         VALID_FOR_GENERATED_COL,
                         NOT_ROW_DIMENSION)
{
}

int ObExprAscii::calc_result_type1(ObExprResType &type,
                                   ObExprResType &type1,
                                   ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;

  const ObSQLSessionInfo *session = type_ctx.get_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else {
    if (is_oracle_mode()) {
      type.set_number();
      type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY2[ORACLE_MODE][ObNumberType].get_scale());
      type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY2[ORACLE_MODE][ObNumberType].get_precision());
      ObSEArray<ObExprResType*, 1, ObNullAllocator> params;
      OZ(params.push_back(&type1));
      ObExprResType tmp_type;
      OZ(aggregate_string_type_and_charset_oracle(*session, params, tmp_type));
      OZ(deduce_string_param_calc_type_and_charset(*session, tmp_type, params));
    } else {
      type.set_int32();
      type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
      type.set_precision(DEFAULT_ASCII_PRECISION_FOR_MYSQL);
      if (ob_is_string_type(type1.get_type())) {
        type1.set_calc_type(type1.get_type());
        type1.set_calc_collation_type(type1.get_collation_type());
      } else {
        type1.set_calc_type(ObVarcharType);
        type1.set_calc_collation_type(ObCharset::get_system_collation());
      }
    }
  }

  return ret;
}

inline void calc_ascii_inner(common::ObObj &obj, ObExprCtx &expr_ctx, const ObString &str_val)
{
  uint8_t tiny_val;
  EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
  if (str_val.length() <= 0){
    tiny_val = 0;
  } else {
    tiny_val = static_cast<uint8_t>(str_val[0]);
  }
  obj.set_utinyint(tiny_val);
}

int ObExprAscii::calc(common::ObObj &obj,
                      const common::ObObj &obj1,
                      ObExprCtx &expr_ctx)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("varchar buffer not init", K(ret));
  } else if (obj1.is_null()) {
    obj.set_null();
  } else if (!ob_is_text_tc(obj1.get_type())) {
    ObString str_val = obj1.get_string();
    calc_ascii_inner(obj, expr_ctx, str_val);
  } else {
    ObString str_val = obj1.get_string();
    if (OB_FAIL(sql::ObTextStringHelper::read_prefix_string_data(expr_ctx.calc_buf_, obj1, str_val))) {
      LOG_WARN("failed to get string data", K(ret), K(obj1.get_meta()));
    } else {
      calc_ascii_inner(obj, expr_ctx, str_val);
    }
  }
  return ret;
}

inline void calc_ascii_expr_inner(ObDatum &res_datum, const ObString &str_val)
{
  if (str_val.empty()) {
    res_datum.set_int32(0);
  } else {
    res_datum.set_int32(static_cast<uint8_t>(str_val[0]));
  }
}

int ObExprAscii::calc_ascii_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  if (is_oracle_mode()) {
    if (OB_FAIL(ObExprOrd::calc_ord_expr(expr, ctx, res_datum))) {
      LOG_WARN("calc_ord_expr failed", K(ret));
    }
  } else {
    ObDatum *s_datum = NULL;
    if (OB_FAIL(expr.args_[0]->eval(ctx, s_datum))) {
      LOG_WARN("eval arg failed", K(ret));
    } else if (s_datum->is_null()) {
      res_datum.set_null();
    } else if (!ob_is_text_tc(expr.args_[0]->datum_meta_.type_)) {
      const ObString &str_val = s_datum->get_string();
      calc_ascii_expr_inner(res_datum, str_val);
    } else { // text tc only
      ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
      common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
      ObString str_val = s_datum->get_string();
      if (OB_FAIL(sql::ObTextStringHelper::read_prefix_string_data(ctx,
                                                              *s_datum,
                                                              expr.args_[0]->datum_meta_,
                                                              expr.args_[0]->obj_meta_.has_lob_header(),
                                                              &temp_allocator,
                                                              str_val))) {
        LOG_WARN("failed to get lob data", K(ret), K(expr.args_[0]->datum_meta_));
      } else {
        calc_ascii_expr_inner(res_datum, str_val);
      }
    }
  }
  return ret;
}

int ObExprAscii::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                            ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_ascii_expr;
  return ret;
}

// ObExprOrd start
ObExprOrd::ObExprOrd(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc,
                         T_FUN_SYS_ORD,
                         N_ORD, 1,
                         VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

int ObExprOrd::calc_result_type1(ObExprResType &type,
                                 ObExprResType &type1,
                                 ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  type.set_int();
  type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
  type.set_precision(DEFAULT_ORD_PRECISION_FOR_MYSQL);
  if (ob_is_string_type(type1.get_type())) {
    type1.set_calc_type(type1.get_type());
    type1.set_calc_collation_type(type1.get_collation_type());
  } else {
    type1.set_calc_type(ObVarcharType);
    type1.set_calc_collation_type(ObCharset::get_system_collation());
  }

  return ret;
}

inline int calc_ord_inner(uint8_t &type, const ObString &str_val,
                          const ObCollationType &cs_type, common::ObObj &obj)
{
  int ret = OB_SUCCESS;
  const char *str_ptr = str_val.ptr();
  if (NULL != str_ptr) {
    const char *end = str_val.ptr() + str_val.length();
    const ObCharsetInfo *cs = ObCharset::get_charset(cs_type);
    uint64_t n = 0, char_len = ob_ismbchar(cs, str_ptr, end);
    if (char_len > str_val.length()){
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ob_ismbchar return wrong value",
                K(ret), K(char_len), K(str_val.length()));
    } else if (0 == char_len){
      type = 1;
    } else {
      while (char_len--){
        n = (n << 8) | (uint64_t)((uint8_t) *str_ptr++);
      }
      obj.set_uint64(n);
    }
  } else {
    type = 1;
  }
  return ret;
}

int ObExprOrd::calc(common::ObObj &obj,
                    const common::ObObj &obj1,
                    ObExprCtx &expr_ctx)
{
  ObCollationType cs_type = obj1.get_collation_type();
  int ret = OB_SUCCESS;

  uint8_t type = 0; // 0 for Ord(MB set), 1 for ascii(byte set)
  if (OB_UNLIKELY(!ObCharset::is_valid_collation(cs_type))) {
    LOG_WARN("invalid collation type", K(ret));
    ret = OB_ERR_UNEXPECTED;
  } else if (obj1.is_null()) {
    type = 1;
  } else if (ObCharset::usemb(cs_type)) {
    // test mb then calc
    // String trans
    ObString str_val = obj1.get_string();
    if (!ob_is_text_tc(obj1.get_type())) {
      ret = calc_ord_inner(type, str_val, cs_type, obj);
    } else if (OB_FAIL(sql::ObTextStringHelper::read_prefix_string_data(expr_ctx.calc_buf_, obj1, str_val))) {
      LOG_WARN("failed to get lob data", K(ret), K(obj1.get_meta()));
    } else {
      ret = calc_ord_inner(type, str_val, cs_type, obj);
    }
  } else {
    type = 1;
  }

  if (OB_SUCCESS == ret && 1 == type){
    ret = ObExprAscii::calc(obj, obj1, expr_ctx);
    if (OB_SUCC(ret) && (false == obj.is_null())) {
      obj.set_uint64(static_cast<uint64_t>(obj.get_utinyint()));
    }
  }

  return ret;
}

static int calc_ord_expr_inner(const ObCollationType &cs_type,
                               const ObString &str_val,
                               int64_t &res_int,
                               bool &res_null)
{
  int ret = OB_SUCCESS;
  const ObCharsetInfo *cs = ObCharset::get_charset(cs_type);
  if (OB_ISNULL(cs)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid cs_type", K(ret), K(cs_type));
  } else if (str_val.empty()) {
    if (is_oracle_mode()) {
      res_null = true;
    } else {
      res_int = 0;
    }
  } else if (ObCharset::usemb(cs_type)) {
    const char *str_ptr = str_val.ptr();
    const char *end = str_val.ptr() + str_val.length();
    uint32_t n = 0;
    uint32_t char_len = ob_ismbchar(cs, str_ptr, end);
    if (char_len > str_val.length()){
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ob_ismbchar return wrong value", K(ret), K(char_len), K(str_val.length()));
    } else if (0 == char_len){
      res_int = static_cast<uint8_t>(str_val[0]);
    } else {
      while (char_len--){
        n = (n << 8) | static_cast<uint32_t>(static_cast<uint8_t>(*str_ptr++));
      }
      res_int = n;
    }
  } else {
    res_int = static_cast<uint8_t>(str_val[0]);
  }
  return ret;
}

// Oracle mode ascii expression also use this
int ObExprOrd::calc_ord_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *s_datum = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, s_datum))) {
    LOG_WARN("eval arg failed", K(ret));
  } else if (s_datum->is_null()) {
    res_datum.set_null();
  } else {
    int64_t res_int = 0;
    bool res_null = false;
    const ObCollationType cs_type = expr.args_[0]->datum_meta_.cs_type_;
    if (!ob_is_text_tc(expr.args_[0]->datum_meta_.type_)) {
      const ObString &str_val = s_datum->get_string();
      ret = calc_ord_expr_inner(cs_type, str_val, res_int, res_null);
    } else { // text tc only
      ObString str_val = s_datum->get_string();
      ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
      common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
      if (OB_FAIL(sql::ObTextStringHelper::read_prefix_string_data(ctx,
                                                              *s_datum,
                                                              expr.args_[0]->datum_meta_,
                                                              expr.args_[0]->obj_meta_.has_lob_header(),
                                                              &temp_allocator,
                                                              str_val))) {
        LOG_WARN("failed to get string data", K(ret), K(expr.args_[0]->datum_meta_));
      } else {
        ret = calc_ord_expr_inner(cs_type, str_val, res_int, res_null);
      }
      // Notice: str_val cannot be used outside of this branch, it may from temp allocator
    }

    if (OB_SUCC(ret)) {
      const ObObjType &res_type = expr.datum_meta_.type_;
      if (res_null) {
        res_datum.set_null();
      } else if (ObNumberType == res_type) {
        number::ObNumber res_nmb;
        ObNumStackOnceAlloc tmp_alloc;
        if (OB_FAIL(res_nmb.from(res_int, tmp_alloc))) {
          LOG_WARN("get nmb from int failed", K(ret), K(res_int));
        } else {
          res_datum.set_number(res_nmb);
        }
      } else if (ObIntType == res_type) {
        res_datum.set_int(res_int);
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected res_type", K(ret), K(res_type));
      }
    }
  }
  return ret;
}

int ObExprOrd::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                            ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_ord_expr;
  return ret;
}

} // sql
} // OceanBase
