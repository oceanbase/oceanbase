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

#include "ob_expr_char.h"
#include "lib/charset/ob_charset.h"
#include "lib/string/ob_sql_string.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/expr/ob_expr_cast.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/ob_sql_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
namespace oceanbase {
namespace sql {
#define INT32_TO_STR1(buf, int_val) *(reinterpret_cast(uchar*>(buf)) = reinterpret_cast<uchar>(int_val);

#define INT32_TO_STR2(buf, int_val)                                     \
  {                                                                     \
    uint32_t temp = (uint32_t)(int_val);                                \
    (reinterpret_cast<uchar*>(buf))[1] = static_cast<uchar>(temp);      \
    (reinterpret_cast<uchar*>(buf))[0] = static_cast<uchar>(temp >> 8); \
  }
#define INT32_TO_STR3(buf, int_val)                                      \
  { /* lint -save -e734 */                                               \
    uint64_t temp = (uint64_t)(int_val);                                 \
    (reinterpret_cast<uchar*>(buf))[2] = static_cast<uchar>(temp);       \
    (reinterpret_cast<uchar*>(buf))[1] = static_cast<uchar>(temp >> 8);  \
    (reinterpret_cast<uchar*>(buf))[0] = static_cast<uchar>(temp >> 16); \
                                /* lint -restore */}
#define INT32_TO_STR4(buf, int_val)                                      \
  {                                                                      \
    uint64_t temp = (uint64_t)(int_val);                                 \
    (reinterpret_cast<uchar*>(buf))[3] = static_cast<uchar>(temp);       \
    (reinterpret_cast<uchar*>(buf))[2] = static_cast<uchar>(temp >> 8);  \
    (reinterpret_cast<uchar*>(buf))[1] = static_cast<uchar>(temp >> 16); \
    (reinterpret_cast<uchar*>(buf))[0] = static_cast<uchar>(temp >> 24); \
  }

ObExprChar::ObExprChar(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_CHAR, N_CHAR, MORE_THAN_ONE, NOT_ROW_DIMENSION)
{}

ObExprChar::~ObExprChar()
{}

int ObExprChar::calc_result_typeN(
    ObExprResType& type, ObExprResType* types, int64_t param_num, ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (param_num <= 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument number, param should not less than 2", K(param_num), K(ret));
  } else if (OB_FAIL(calc_result_type(type, types[0]))) {
    LOG_WARN("failed to calc result type", K(ret));
  } else {
    // set calc type
    // i starts from 1 rather than 0 since the first param is obvarchar always.
    for (int64_t i = 1; i < param_num; ++i) {
      types[i].set_calc_type(ObIntType);
    }
    ObExprOperator::calc_result_flagN(type, types, param_num);
    // set length
    CK(OB_INVALID_COUNT != type_ctx.get_max_allowed_packet());
    if (OB_SUCC(ret)) {
      type.set_length(static_cast<ObLength>(type_ctx.get_max_allowed_packet()));
    }
  }
  return ret;
}

int ObExprChar::calc_resultN(ObObj& result, const ObObj* obj_array, int64_t param_num, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_ctx.my_session_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("Session is NULL", K(ret));
  } else if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("calc buf not is not inited", K(ret));
  } else if (param_num < 2) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Incorrect parameter number", K(param_num), K(ret));
  } else if (OB_ISNULL(obj_array)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("obj_array is NULL", K(ret));
  } else {
    ObSqlString str_buf;
    if (OB_SUCC(ret)) {
      for (int64_t i = 1; OB_SUCC(ret) && i < param_num; ++i) {
        const ObObj& param_obj = obj_array[i];
        if (param_obj.is_null()) {
          // ignore null obj which is syntax of char()
          continue;
        } else {
          TYPE_CHECK(param_obj, ObIntType);
          int64_t int64_val = param_obj.get_int();
          int32_t int32_val = static_cast<int32_t>(int64_val);
          char buf[4] = {0};
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
          if (OB_SUCCESS == ret && OB_FAIL(str_buf.append(buf, append_len))) {
            LOG_WARN("fail to append convert result", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      const int64_t result_len = str_buf.length();
      char* result_str = reinterpret_cast<char*>(expr_ctx.calc_buf_->alloc(result_len + 1));
      if (OB_UNLIKELY(NULL == result_str)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to allocate memory", K(result_len), K(ret));
      } else {
        if (NULL != str_buf.ptr()) {
          MEMCPY(result_str, str_buf.ptr(), result_len);
        }
        result_str[result_len] = '\0';
        result.set_varchar(result_str, static_cast<int32_t>(result_len));
        result.set_collation(result_type_);

        ObSQLMode sql_mode = expr_ctx.my_session_->get_sql_mode();
        bool is_strict = is_strict_mode(sql_mode);
        ObObj real_result;
        if (OB_FAIL(ObSQLUtils::check_well_formed_str(result, real_result, is_strict))) {
          LOG_WARN("fail to checkout_well_formed_str", K(result), K(is_strict), K(ret));
        } else {
          result = real_result;
        }
      }
    }
  }
  return ret;
}

int ObExprChar::calc_result_type(ObExprResType& type, ObExprResType& type1) const
{
  int ret = OB_SUCCESS;
  ObString cs_name;
  if (ObVarcharType != type1.get_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid type", K(type1), K(ret));
  } else {
    type.set_varchar();
    type.set_collation_type(type1.get_collation_type());
    type.set_collation_level(CS_LEVEL_COERCIBLE);
  }
  return ret;
}

int calc_char_expr(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  ObSqlString str_buf;
  ObDatum* child_res = NULL;
  ObSQLSessionInfo* session = ctx.exec_ctx_.get_my_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  }
  for (int64_t i = 1; OB_SUCC(ret) && i < expr.arg_cnt_; ++i) {
    if (OB_FAIL(expr.args_[i]->eval(ctx, child_res))) {
      LOG_WARN("eval arg failed", K(ret), K(i));
    } else if (child_res->is_null()) {
      continue;
    } else {
      int64_t int64_val = child_res->get_int();
      int32_t int32_val = static_cast<int32_t>(int64_val);
      char buf[4] = {0};
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
    const int64_t res_len = str_buf.length();
    char* res_str = expr.get_str_res_mem(ctx, res_len + 1);
    if (OB_UNLIKELY(NULL == res_str)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("fail to allocate memory", K(res_len), K(ret));
    } else {
      if (NULL != str_buf.ptr()) {
        MEMCPY(res_str, str_buf.ptr(), res_len);
      }
      res_str[res_len] = '\0';
      bool is_null = false;
      ObString checked_res_str;
      bool is_strict = is_strict_mode(session->get_sql_mode());
      if (OB_FAIL(ObSQLUtils::check_well_formed_str(
              ObString(res_len, res_str), expr.datum_meta_.cs_type_, checked_res_str, is_null, is_strict))) {
        LOG_WARN("check_well_formed_str failed", K(ret), K(res_str), K(expr.datum_meta_));
      } else if (is_null) {
        res_datum.set_null();
      } else {
        res_datum.set_string(checked_res_str);
      }
    }
  }
  return ret;
}

int ObExprChar::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_char_expr;
  return ret;
}

}  // end of namespace sql
}  // end of namespace oceanbase
