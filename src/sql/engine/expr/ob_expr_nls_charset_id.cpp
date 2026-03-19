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
#include "sql/engine/expr/ob_expr_nls_charset_id.h"
#include "sql/session/ob_sql_session_info.h"
#include "lib/charset/ob_charset.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "share/vector/ob_vector_define.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprNLSCharsetId::ObExprNLSCharsetId(ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_SYS_NLS_CHARSET_ID, N_NLS_CHARSET_ID, 1,
                       VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

int ObExprNLSCharsetId::calc_result_type1(ObExprResType &type,
                                          ObExprResType &type1,
                                          ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;

  // Set return type to number
  type.set_number();
  type.set_precision(PRECISION_UNKNOWN_YET);
  type.set_scale(ORA_NUMBER_SCALE_UNKNOWN_YET);

  // Set arg type to varchar with UTF8 charset
  type1.set_calc_type(ObVarcharType);
  type1.set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);

  return ret;
}

int ObExprNLSCharsetId::cg_expr(ObExprCGCtx &expr_cg_ctx,
                                const ObRawExpr &raw_expr,
                                ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_nls_charset_id;
  rt_expr.eval_vector_func_ = eval_nls_charset_id_vector;
  return ret;
}

// Check if 'prefix' is a case-insensitive prefix of 'full'.
static bool is_prefix_of_ci(const ObString &full, const ObString &prefix)
{
  bool matched = false;
  if (prefix.length() <= full.length()) {
    matched = (0 == ObString(prefix.length(), full.ptr()).case_compare(prefix));
  }
  return matched;
}

// Resolve CHAR_CS / NCHAR_CS by prefix match only. Check NCHAR_CS first (longer), then CHAR_CS.
static int resolve_char_nchar_cs_partial(const ObString &name,
                                         ObSQLSessionInfo *session,
                                         ObNlsCharsetId &charset_id,
                                         ObCharsetType &cs_type,
                                         bool &matched)
{
  int ret = OB_SUCCESS;
  matched = false;
  charset_id = CHARSET_INVALID_ID;
  cs_type = CHARSET_INVALID;

  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (!name.empty()) {
    static const char NCHAR_CS[] = "NCHAR_CS";
    static const char CHAR_CS[] = "CHAR_CS";
    ObString nchar_cs_str(sizeof(NCHAR_CS) - 1, NCHAR_CS);
    ObString char_cs_str(sizeof(CHAR_CS) - 1, CHAR_CS);

    bool is_nchar = is_prefix_of_ci(nchar_cs_str, name);
    bool is_char = is_prefix_of_ci(char_cs_str, name);
    if (is_nchar || is_char) {
      ObCollationType coll_type = is_nchar ? session->get_nls_collation_nation()
                                          : session->get_nls_collation();
      cs_type = ObCharset::charset_type_by_coll(coll_type);
      if (CHARSET_INVALID == cs_type) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN(is_nchar ? "invalid national charset" : "invalid database charset",
                 K(ret), K(coll_type));
      } else {
        charset_id = ObCharset::charset_type_to_ora_charset_id(cs_type);
        matched = true;
      }
    }
  }

  return ret;
}

// Calculate charset ID from name and convert to number. Prefix match for CHAR_CS/NCHAR_CS, else by_name lookup.
static int calc_nls_charset_id_number(const ObString &charset_name,
                                      ObSQLSessionInfo *session,
                                      ObNumStackOnceAlloc &tmp_alloc,
                                      number::ObNumber &num,
                                      bool &charset_not_found)
{
  int ret = OB_SUCCESS;
  charset_not_found = false;
  ObNlsCharsetId charset_id = CHARSET_INVALID_ID;
  ObCharsetType cs_type = CHARSET_INVALID;

  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else {
    bool char_nchar_matched = false;
    if (OB_FAIL(resolve_char_nchar_cs_partial(charset_name, session,
                                              charset_id, cs_type, char_nchar_matched))) {
      LOG_WARN("resolve_char_nchar_cs_partial failed", K(ret));
    } else if (char_nchar_matched) {
      // CHAR_CS or NCHAR_CS matched (prefix)
    } else {
      // Get charset type by name, return NULL if not found
      cs_type = ObCharset::charset_type_by_name_oracle(charset_name);
      if (CHARSET_INVALID == cs_type) {
        // Charset not found, return NULL
        charset_not_found = true;
      } else {
        charset_id = ObCharset::charset_type_to_ora_charset_id(cs_type);
      }
    }

    // Convert valid charset ID to number
    if (OB_SUCC(ret) && !charset_not_found && CHARSET_INVALID != cs_type) {
      if (OB_FAIL(num.from(static_cast<int64_t>(charset_id), tmp_alloc))) {
        LOG_WARN("failed to convert charset id to number", K(ret), K(charset_id));
      }
    }
  }

  return ret;
}

OB_NOINLINE int ObExprNLSCharsetId::eval_nls_charset_id(const ObExpr &expr,
                                            ObEvalCtx &ctx,
                                            ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *arg_datum = NULL;
  ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, arg_datum))) {
    LOG_WARN("fail to eval arg", K(ret));
  } else if (arg_datum->is_null()) {
    expr_datum.set_null();
  } else {
    ObString charset_name = arg_datum->get_string();
    ObNumStackOnceAlloc tmp_alloc;
    number::ObNumber num;
    bool charset_not_found = false;

    if (OB_FAIL(calc_nls_charset_id_number(charset_name, session, tmp_alloc, num, charset_not_found))) {
      LOG_WARN("failed to calc nls charset id number", K(ret));
    } else if (charset_not_found) {
      expr_datum.set_null();
    } else {
      expr_datum.set_number(num);
    }
  }

  return ret;
}

OB_NOINLINE int ObExprNLSCharsetId::eval_nls_charset_id_vector(const ObExpr &expr,
                                                    ObEvalCtx &ctx,
                                                    const ObBitVector &skip,
                                                    const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval vector arg", K(ret));
  } else {
    ObVectorBase *arg_vec = static_cast<ObVectorBase *>(expr.args_[0]->get_vector(ctx));
    ObVectorBase *res_vec = static_cast<ObVectorBase *>(expr.get_vector(ctx));
    ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();

    if (OB_ISNULL(session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session is null", K(ret));
    } else {
      // Define temp vars outside loop to avoid repeated allocation
      ObNumStackOnceAlloc tmp_alloc;
      number::ObNumber num;
      ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);

      const int64_t start = bound.start();
      const int64_t end = bound.end();

      for (int64_t idx = start; OB_SUCC(ret) && idx < end; ++idx) {
        if (skip.at(idx) || eval_flags.at(idx)) {
          continue;
        } else if (arg_vec->is_null(idx)) {
          res_vec->set_null(idx);
          continue;
        }

        // Process each row using base class interface
        ObLength in_len = arg_vec->get_length(idx);
        ObString charset_name(in_len, arg_vec->get_payload(idx));
        bool charset_not_found = false;

        if (OB_FAIL(calc_nls_charset_id_number(charset_name, session, tmp_alloc, num, charset_not_found))) {
          LOG_WARN("failed to calc nls charset id number", K(ret));
        } else if (charset_not_found) {
          res_vec->set_null(idx);
        } else {
          res_vec->set_number(idx, num);
          // Reset tmp_alloc to reuse stack buffer
          tmp_alloc.free();
        }
      }
    }
  }

  return ret;
}

}
}