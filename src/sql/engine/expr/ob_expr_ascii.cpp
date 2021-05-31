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

namespace oceanbase {
using namespace common;

namespace sql {
ObExprAscii::ObExprAscii(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_ASCII, N_ASCII, 1, NOT_ROW_DIMENSION)
{}

int ObExprAscii::calc_result_type1(ObExprResType& type, ObExprResType& type1, ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;

  const ObSQLSessionInfo* session = type_ctx.get_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else {
    if (session->use_static_typing_engine()) {
      if (is_oracle_mode()) {
        type.set_number();
        ObSEArray<ObExprResType*, 1, ObNullAllocator> params;
        OZ(params.push_back(&type1));
        ObExprResType tmp_type;
        OZ(aggregate_string_type_and_charset_oracle(*session, params, tmp_type));
        OZ(deduce_string_param_calc_type_and_charset(*session, tmp_type, params));
      } else {
        type.set_int32();
        if (ob_is_string_type(type1.get_type())) {
          type1.set_calc_type(type1.get_type());
          type1.set_calc_collation_type(type1.get_collation_type());
        } else {
          type1.set_calc_type(ObVarcharType);
          type1.set_calc_collation_type(ObCharset::get_system_collation());
        }
      }
    } else {
      if (type1.is_null()) {
        type.set_null();
      } else if (share::is_oracle_mode()) {
        type.set_number();
      } else {
        type.set_utinyint();
      }
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

int ObExprAscii::calc(common::ObObj& obj, const common::ObObj& obj1, ObExprCtx& expr_ctx)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("varchar buffer not init", K(ret));
  } else if (obj1.is_null()) {
    obj.set_null();
  } else {
    uint8_t tiny_val;
    ObString str_val = obj1.get_string();
    EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);

    if (str_val.length() <= 0) {
      tiny_val = 0;
    } else {
      tiny_val = static_cast<uint8_t>(str_val[0]);
    }
    obj.set_utinyint(tiny_val);
  }
  return ret;
}

int ObExprAscii::calc_result1(common::ObObj& obj, const common::ObObj& obj1, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (share::is_oracle_mode()) {
    common::ObObj tmp;
    number::ObNumber result;
    if (OB_FAIL(ObExprOrd::calc(tmp, obj1, expr_ctx))) {
      LOG_WARN("failed to calc expr ord", K(ret));
    } else if (tmp.is_null()) {
      obj.set_null();
    } else if (OB_FAIL(result.from(tmp.get_uint64(), *expr_ctx.calc_buf_))) {
      LOG_WARN("failed to convert uint64 to ob number", K(ret));
    } else {
      obj.set_number(result);
    }
  } else {
    ret = ObExprAscii::calc(obj, obj1, expr_ctx);
  }
  return ret;
}

int ObExprAscii::calc_ascii_expr(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  if (is_oracle_mode()) {
    if (OB_FAIL(ObExprOrd::calc_ord_expr(expr, ctx, res_datum))) {
      LOG_WARN("calc_ord_expr failed", K(ret));
    }
  } else {
    ObDatum* s_datum = NULL;
    if (OB_FAIL(expr.args_[0]->eval(ctx, s_datum))) {
      LOG_WARN("eval arg failed", K(ret));
    } else if (s_datum->is_null()) {
      res_datum.set_null();
    } else {
      const ObString& str_val = s_datum->get_string();
      if (str_val.empty()) {
        res_datum.set_int32(0);
      } else {
        res_datum.set_int32(static_cast<uint8_t>(str_val[0]));
      }
    }
  }
  return ret;
}

int ObExprAscii::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_ascii_expr;
  return ret;
}

// ObExprOrd start
ObExprOrd::ObExprOrd(ObIAllocator& alloc) : ObFuncExprOperator(alloc, T_FUN_SYS_ORD, N_ORD, 1, NOT_ROW_DIMENSION)
{}

int ObExprOrd::calc_result_type1(ObExprResType& type, ObExprResType& type1, ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo* session = type_ctx.get_session();

  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else {
    if (session->use_static_typing_engine()) {
      type.set_int();
      if (ob_is_string_type(type1.get_type())) {
        type1.set_calc_type(type1.get_type());
        type1.set_calc_collation_type(type1.get_collation_type());
      } else {
        type1.set_calc_type(ObVarcharType);
        type1.set_calc_collation_type(ObCharset::get_system_collation());
      }
    } else {
      if (type1.is_null()) {
        type.set_null();
      } else {
        type.set_uint64();
      }
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

int ObExprOrd::calc(common::ObObj& obj, const common::ObObj& obj1, ObExprCtx& expr_ctx)
{
  ObCollationType cs_type = obj1.get_collation_type();
  int ret = OB_SUCCESS;

  uint8_t type = 0;  // 0 for Ord(MB set), 1 for ascii(byte set)
  if (OB_UNLIKELY(!ObCharset::is_valid_collation(cs_type))) {
    LOG_WARN("invalid collation type", K(ret));
    ret = OB_ERR_UNEXPECTED;
  } else if (obj1.is_null()) {
    type = 1;
  } else if (ObCharset::usemb(cs_type)) {
    // test mb then calc
    // String trans
    ObString str_val = obj1.get_string();

    const char* str_ptr = str_val.ptr();
    if (NULL != str_ptr) {
      const ObCharsetInfo* cs = ObCharset::get_charset(cs_type);
      uint64_t n = 0, char_len = cs->cset->ismbchar(str_val.ptr(), str_val.length());
      if (char_len > str_val.length()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("my_ismbchar return wrong value", K(ret), K(char_len), K(str_val.length()));
      } else if (0 == char_len) {
        type = 1;
      } else {
        while (char_len--) {
          n = (n << 8) | (uint64_t)((uint8_t)*str_ptr++);
        }
        obj.set_uint64(n);
      }
    } else {
      type = 1;
    }
  } else {
    type = 1;
  }

  if (OB_SUCCESS == ret && 1 == type) {
    ret = ObExprAscii::calc(obj, obj1, expr_ctx);
    if (OB_SUCC(ret) && (false == obj.is_null())) {
      obj.set_uint64(static_cast<uint64_t>(obj.get_utinyint()));
    }
  }

  return ret;
}
int ObExprOrd::calc_result1(common::ObObj& obj, const common::ObObj& obj1, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  // test expr_ctx
  if (OB_ISNULL(expr_ctx.calc_buf_) || OB_ISNULL(expr_ctx.my_session_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("expr_ctx not init", K(ret));
  } else {
    ret = calc(obj, obj1, expr_ctx);
  }
  return ret;
}

// ascii expr in oracle mode use this function too.
int ObExprOrd::calc_ord_expr(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* s_datum = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, s_datum))) {
    LOG_WARN("eval arg failed", K(ret));
  } else if (s_datum->is_null()) {
    res_datum.set_null();
  } else {
    int64_t res_int = 0;
    bool res_null = false;
    const ObCollationType cs_type = expr.args_[0]->datum_meta_.cs_type_;
    const ObCharsetInfo* cs = ObCharset::get_charset(cs_type);
    const ObString& str_val = s_datum->get_string();
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
      const char* str_ptr = str_val.ptr();
      uint32_t n = 0;
      uint32_t char_len = cs->cset->ismbchar(str_val.ptr(), str_val.length());
      if (char_len > str_val.length()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("my_ismbchar return wrong value", K(ret), K(char_len), K(str_val.length()));
      } else if (0 == char_len) {
        res_int = static_cast<uint8_t>(str_val[0]);
      } else {
        while (char_len--) {
          n = (n << 8) | static_cast<uint32_t>(static_cast<uint8_t>(*str_ptr++));
        }
        res_int = n;
      }
    } else {
      res_int = static_cast<uint8_t>(str_val[0]);
    }

    if (OB_SUCC(ret)) {
      const ObObjType& res_type = expr.datum_meta_.type_;
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

int ObExprOrd::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_ord_expr;
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
