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

#include "sql/engine/expr/ob_expr_export_set.h"
#include "lib/oblog/ob_log.h"
#include "share/object/ob_obj_cast.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
using namespace oceanbase::common;
namespace oceanbase {
namespace sql {

ObExprExportSet::ObExprExportSet(ObIAllocator& alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_EXPORT_SET, N_EXPORT_SET, MORE_THAN_TWO, VALID_FOR_GENERATED_COL)
{
  need_charset_convert_ = false;
}

ObExprExportSet::~ObExprExportSet()
{}

int ObExprExportSet::calc_result_typeN(ObExprResType& type, ObExprResType* types_array,
    int64_t param_num, common::ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  if (3 != param_num && 4 != param_num && 5 != param_num) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Export_Set() should have three or four or five arguments", K(ret), K(param_num));
  } else if (OB_ISNULL(types_array) || OB_ISNULL(type_ctx.get_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error. types_array or session null", K(ret), KP(types_array),
        KP(type_ctx.get_session()));
  } else {
    // deduce length
    // Maximum occurrences of on and of in result
    const uint64_t MAX_BIT_NUM = 64;
    // Maximum occurrences of sep in result
    const uint64_t MAX_SEP_NUM = 63;
    const int64_t on_len = types_array[1].get_length();
    const int64_t off_len = types_array[2].get_length();
    int64_t sep_len = 1;
    int64_t str_num = 2;
    const uint64_t max_len = std::max(on_len, off_len);
    // when bits exceed uint_max or int_min, ob is not compatible to mysql.
    types_array[0].set_calc_type(common::ObIntType);
    types_array[1].set_calc_type(common::ObVarcharType);
    types_array[2].set_calc_type(common::ObVarcharType);
    if (3 < param_num) {
      sep_len = types_array[3].get_length();
      str_num = 3;
      types_array[3].set_calc_type(common::ObVarcharType);
      if (4 < param_num) {
        types_array[4].set_calc_type(common::ObIntType);
      }
    }
    common::ObLength len = static_cast<common::ObLength>(MAX_BIT_NUM * max_len +
        MAX_SEP_NUM * sep_len);
    type.set_length(len);
    type.set_varchar();
    // set collation_type for string type
    OZ(ObExprOperator::aggregate_charsets_for_string_result_with_comparison(
        type, &types_array[1], str_num, type_ctx.get_coll_type()));
    for (int64_t i = 1; OB_SUCC(ret) && i <= str_num; ++i) {
      types_array[i].set_calc_meta(type);
    }
  }
  return ret;
}

int ObExprExportSet::calc_export_set_inner(const int64_t max_result_size, ObString& ret_str, const uint64_t bits,
    const ObString& on, const ObString& off, const ObString& sep, const int64_t n_bits,
    ObExprStringBuf& string_buf)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(n_bits == 0)) {
    // Return empty string
    ret_str.reset();
  } else if (OB_UNLIKELY(on.length() <= 0 && off.length() <= 0 && sep.length() <= 0)) {
    ret_str.reset();
  } else {
    const uint64_t MAX_BIT_NUM = 64UL;
    uint64_t local_n_bits = static_cast<uint64_t>(n_bits);
    local_n_bits = std::min(MAX_BIT_NUM, local_n_bits);
    const int64_t length_on = on.length();
    const int64_t length_off = off.length();
    const int64_t length_sep = sep.length();
    int64_t tot_length = 0;  // total length for the result.
    uint64_t i;
    const uint64_t mask = 1;
    //compute tot_length and save ans
    tot_length += (local_n_bits - 1) * length_sep;
    for (i = 0; i < local_n_bits; ++i) {
      if (bits & (mask << i)){
        tot_length += length_on;
      } else {
        tot_length += length_off;
      }
    }
    // Avoid realloc
    if (OB_UNLIKELY(tot_length <= 0)) {
      // tot_length equals to 0 indicates that length_to is zero and "to" is empty string
      ret_str.reset();
    } else if (tot_length > max_result_size) {
      LOG_WARN("Result of export_set_inner was larger than max_allow_packet_size", K(ret), K(tot_length), K(max_result_size));
      LOG_USER_WARN(OB_ERR_FUNC_RESULT_TOO_LARGE, "export_set", static_cast<int>(max_result_size));
    } else {
      char* buf = static_cast<char*>(string_buf.alloc(tot_length));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("alloc memory failed.", K(ret), K(tot_length));
      } else {
        // Core function
        char* tmp_buf = buf;
        if (local_n_bits > 0) {
          if (bits & mask) {
            MEMCPY(tmp_buf, on.ptr(), length_on);
            tmp_buf += length_on;
          } else {
            MEMCPY(tmp_buf, off.ptr(), length_off);
            tmp_buf += length_off;
          }
        }
        for (i = 1; i < local_n_bits; ++i) {
          MEMCPY(tmp_buf, sep.ptr(), length_sep);
          tmp_buf += length_sep;
          if (bits & (mask << i)) {
            MEMCPY(tmp_buf, on.ptr(), length_on);
            tmp_buf += length_on;
          } else {
            MEMCPY(tmp_buf, off.ptr(), length_off);
            tmp_buf += length_off;
          }
        }
        ret_str.assign_ptr(buf, static_cast<int32_t>(tot_length));
      }
    }
  }
  return ret;
}

int ObExprExportSet::cg_expr(ObExprCGCtx&, const ObRawExpr&, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  CK(3 == rt_expr.arg_cnt_ || 4 == rt_expr.arg_cnt_ || 5 == rt_expr.arg_cnt_);
  rt_expr.eval_func_ = &eval_export_set;
  return ret;
}

int ObExprExportSet::eval_export_set(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* bits = NULL;
  ObDatum* on = NULL;
  ObDatum* off = NULL;
  ObDatum* sep = NULL;
  ObDatum* n_bits = NULL;
  int64_t max_size = 0;
  if (OB_FAIL(expr.eval_param_value(ctx, bits, on, off, sep, n_bits))) {
    LOG_WARN("evaluate parameters failed", K(ret));
  } else if (bits->is_null() || on->is_null() || off->is_null()) {
    expr_datum.set_null();
  } else if (OB_NOT_NULL(sep) && sep->is_null()) {
    expr_datum.set_null();
  } else if (OB_NOT_NULL(n_bits) && n_bits->is_null()) {
    expr_datum.set_null();
  } else if (OB_FAIL(ctx.exec_ctx_.get_my_session()->get_max_allowed_packet(max_size))) {
    LOG_WARN("get max length failed", K(ret));
  } else {
    ObExprStrResAlloc expr_res_alloc(expr, ctx);
    ObString output;
    //default parm
    ObString sep_parm;
    int64_t n_bits_parm;
    if (OB_ISNULL(sep)) {
      sep_parm = ObCharsetUtils::get_const_str(expr.datum_meta_.cs_type_, ',');
    } else {
      sep_parm = sep->get_string();
    }
    if (OB_ISNULL(n_bits)) {
      n_bits_parm = 64;
    } else {
      n_bits_parm = n_bits->get_uint64();
    }
    if (OB_FAIL(calc_export_set_inner(max_size, output, bits->get_uint64(), on->get_string(),
        off->get_string(), sep_parm, n_bits_parm, expr_res_alloc))) {
      LOG_WARN("do export set failed", K(ret));
    } else {
      expr_datum.set_string(output);
    }
  }
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprExportSet, raw_expr) {
  int ret = OB_SUCCESS;
  SET_LOCAL_SYSVAR_CAPACITY(1);
  EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_COLLATION_CONNECTION);
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
