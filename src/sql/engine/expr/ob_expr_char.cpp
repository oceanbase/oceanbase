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

#define USING_LOG_PREFIX  SQL_ENG

#include "ob_expr_char.h"
#include "lib/charset/ob_charset.h"
#include "lib/string/ob_sql_string.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/expr/ob_expr_cast.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/ob_sql_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
namespace oceanbase
{
namespace sql
{
#define INT32_TO_STR1(buf, int_val) *(reinterpret_cast(uchar*>(buf)) = reinterpret_cast<uchar>(int_val);

#define INT32_TO_STR2(buf, int_val)   { uint32_t temp = (uint32_t) (int_val) ;\
                                (reinterpret_cast<uchar *>(buf))[1] = static_cast<uchar>(temp);\
                                (reinterpret_cast<uchar *>(buf))[0] = static_cast<uchar>(temp >> 8); }
#define INT32_TO_STR3(buf, int_val)   { /* lint -save -e734 */\
                                uint64_t temp= (uint64_t) (int_val);\
                                (reinterpret_cast<uchar *>(buf))[2] = static_cast<uchar>(temp);\
                                (reinterpret_cast<uchar *>(buf))[1] = static_cast<uchar>(temp >> 8);\
                                (reinterpret_cast<uchar *>(buf))[0] = static_cast<uchar>(temp >> 16);\
                                /* lint -restore */}
#define INT32_TO_STR4(buf, int_val)   { uint64_t temp = (uint64_t) (int_val);\
                                (reinterpret_cast<uchar *>(buf))[3] = static_cast<uchar>(temp);\
                                (reinterpret_cast<uchar *>(buf))[2] = static_cast<uchar>(temp >> 8);\
                                (reinterpret_cast<uchar *>(buf))[1] = static_cast<uchar>(temp >> 16);\
                                (reinterpret_cast<uchar *>(buf))[0] = static_cast<uchar>(temp >> 24); }\


ObExprChar::ObExprChar(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_CHAR, N_CHAR, MORE_THAN_ONE, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprChar::~ObExprChar()
{
}

int ObExprChar::calc_result_typeN(ObExprResType &type, ObExprResType *types, int64_t param_num,
                                  ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (param_num <= 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument number, param should not less than 2", K(param_num), K(ret));
  } else if (OB_FAIL(calc_result_type(type, types[param_num-1]))) {
    LOG_WARN("failed to calc result type", K(ret));
  } else {
    //set calc type
    //i starts from 1 rather than 0 since the first param is obvarchar always.
    for (int64_t i = 0; i < param_num-1; ++i) {
      types[i].set_calc_type(ObIntType);
    }
    ObExprOperator::calc_result_flagN(type, types, param_num);
    type.set_length(static_cast<ObLength>(param_num * 4 - 4));
  }
  return ret;
}

int ObExprChar::calc_result_type(ObExprResType &type, ObExprResType &type1) const
{
  int ret = OB_SUCCESS;
  ObString cs_name;
  if (ObVarcharType != type1.get_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid type", K(type1), K(ret));
  } else {
    type.set_varchar();
    if (type1.is_literal()) {
      ObString charset_str = type1.get_param().get_string();
      ObCharsetType charset_type = ObCharset::charset_type(charset_str);
      if (CHARSET_INVALID == charset_type) {
        ret = OB_ERR_UNKNOWN_CHARSET;
        LOG_WARN("invalid character set", K(charset_str), K(ret));
        LOG_USER_ERROR(OB_ERR_UNKNOWN_CHARSET, charset_str.length(), charset_str.ptr());
      } else {
        type.set_collation_type(ObCharset::get_default_collation(charset_type));
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid type", K(type1), K(ret));
    }
    type.set_collation_level(CS_LEVEL_COERCIBLE);
  }
  return ret;
}

int calc_char_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObSqlString str_buf;
  ObDatum *child_res = NULL;
  ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < expr.arg_cnt_-1; ++i) {
    if (OB_FAIL(expr.args_[i]->eval(ctx, child_res))) {
      LOG_WARN("eval arg failed", K(ret), K(i));
    } else if (child_res->is_null()) {
      continue;
    } else {
      int64_t int64_val = child_res->get_int();
      int32_t int32_val = static_cast<int32_t>(int64_val);
      char buf[4]={0};
      int64_t append_len = 0;
      if (int32_val & 0xFF000000L) {
        INT32_TO_STR4(buf, int32_val);
        append_len = 4;
      } else if (int32_val & 0xFF0000L) {
        INT32_TO_STR3(buf, int32_val);
        append_len = 3;
      } else if (int32_val & 0xFF00L) {
        INT32_TO_STR2(buf, int32_val);
        append_len = 2;
      } else {
        buf[0] = static_cast<char>(int32_val);
        append_len = 1;
      }
      if (OB_FAIL(str_buf.append(buf, append_len))) {
        LOG_WARN("fail to append convert result", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    
    //
    // convert utf8 to dest collation
    //
    ObString out; 
    const ObString in_str(str_buf.length(), str_buf.ptr());
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    ObIAllocator &tmp_alloc = alloc_guard.get_allocator(); 
    if (OB_FAIL(ObExprUtil::convert_string_collation(in_str, CS_TYPE_UTF8MB4_BIN, out, 
                                                     expr.datum_meta_.cs_type_, tmp_alloc))) {
      LOG_WARN("failed to convert string from utf8 to other collation", K(ret), 
                                                                        K(expr.datum_meta_.cs_type_));
    }

    const int64_t res_len = out.length();
    char *res_str = expr.get_str_res_mem(ctx, res_len + 1);
    if (OB_UNLIKELY(NULL == res_str)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("fail to allocate memory", K(res_len), K(ret));
    } else {
      if (NULL != str_buf.ptr()) {
        MEMCPY(res_str, out.ptr(), res_len);
      }
      res_str[res_len] = '\0';
      bool is_null = false;
      ObString checked_res_str;
      bool is_strict = is_strict_mode(session->get_sql_mode());
      if (OB_FAIL(ObSQLUtils::check_well_formed_str(ObString(res_len, res_str),
                                                    expr.datum_meta_.cs_type_,
                                                    checked_res_str, is_null,
                                                    is_strict))) {
        LOG_WARN("check_well_formed_str failed", K(ret), K(res_str),
                  K(expr.datum_meta_));
      } else if (is_null) {
        res_datum.set_null();
      } else {
        res_datum.set_string(checked_res_str);
      }
    }
  }
  return ret;
}

int ObExprChar::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                        ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_char_expr;
  return ret;
}

}//end of namespace sql
}//end of namespace oceanbase
