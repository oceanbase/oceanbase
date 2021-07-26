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

#include <limits.h>
#include <string.h>

#include "lib/oblog/ob_log.h"
#include "share/object/ob_obj_cast.h"
#include "sql/session/ob_sql_session_info.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;
namespace oceanbase {
namespace sql {


ObExprExportSet::ObExprExportSet(ObIAllocator& alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_EXPORT_SET, N_EXPORT_SET, MORE_THAN_TWO)
{
  need_charset_convert_ = false;
}

ObExprExportSet::~ObExprExportSet()
{}

int ObExprExportSet::calc_result_typeN(
    ObExprResType& type, ObExprResType* types_array, int64_t param_num, common::ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  // export set is mysql only epxr.
  CK(lib::is_mysql_mode());
  if (3 != param_num && 4 != param_num && 5 != param_num) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Export_Set() should have three or four or five arguments", K(ret), K(param_num));
  } else if (OB_ISNULL(types_array) || OB_ISNULL(type_ctx.get_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error. types_array or session null", K(ret), KP(types_array), KP(type_ctx.get_session()));
  } else {
    // deduce length
    ObExprResType& on = types_array[1];
    ObExprResType& off = types_array[2];
    int64_t on_len = on.get_length();
    int64_t off_len = off.get_length();
    int64_t sep_len = 1;
    int64_t str_num = 2;
    uint64_t n_bits = 64;
    uint64_t max_len = easy_max(on_len, off_len);//on和off的较大长度者 TODO
    // when bits exceed uint_max or int_min, ob is not compatible to mysql.
    // it is mysql bug.
    types_array[0].set_calc_type(common::ObBitType); 
    types_array[1].set_calc_type(common::ObVarcharType);
    types_array[2].set_calc_type(common::ObVarcharType);
    types_array[3].set_calc_type(common::ObVarcharType);
    types_array[4].set_calc_type(common::ObIntType); 
    if (3 < param_num) {
      sep_len = types_array[3].get_length();
      str_num = 3;
    }
    common::ObLength len = static_cast<common::ObLength>(n_bits * (max_len + sep_len)); 
    type.set_length(len);


    if (types_array[1].is_lob()) {
      type.set_type(types_array[1].get_type());
    } else if(types_array[2].is_lob()) {
      type.set_type(types_array[2].get_type());
    } else if(types_array[3].is_lob()) {
      type.set_type(types_array[3].get_type());
    } else {
      type.set_varchar();
      type.set_length_semantics(type_ctx.get_session()->get_actual_nls_length_semantics());        
    }
    
    // set collation_type for string type
    OZ(ObExprOperator::aggregate_charsets_for_string_result_with_comparison(
        type, &types_array[1], str_num, type_ctx.get_coll_type()));
    for (int64_t i = 1; OB_SUCC(ret) && i <= str_num; ++i) {
      types_array[i].set_calc_meta(type);
    }

  }
  return ret;
}

int ObExprExportSet::calc_resultN(ObObj& result, const ObObj* objs_array, int64_t param_num, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("calc_buf of expr_ctx is NULL", K(ret));
  } else if (3 == param_num) {
    ObObj tmp_n_bits;
    ObObj tmp_sep;
    tmp_n_bits.set_int(64);
    tmp_sep.set_varchar(",");      
    ret = calc_result5(result, objs_array[0], objs_array[1], objs_array[2], tmp_sep, tmp_n_bits, expr_ctx);
  } else if (4 == param_num) {
    ObObj tmp_n_bits;
    tmp_n_bits.set_int(64); 
    ret = calc_result5(result, objs_array[0], objs_array[1], objs_array[2], objs_array[3], tmp_n_bits, expr_ctx);
  } else if (5 == param_num) {
    ret = calc_result5(result, objs_array[0], objs_array[1], objs_array[2], objs_array[3], objs_array[4], expr_ctx);
  }
  
  return ret;
}

int ObExprExportSet::calc_result5(
    ObObj& result, const ObObj& bits, const ObObj& on, const ObObj& off, const ObObj& sep, const ObObj& n_bits, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (bits.is_null() || on.is_null() || off.is_null() || sep.is_null() || n_bits.is_null()) {
    result.set_null();
  } else if (OB_ISNULL(expr_ctx.calc_buf_)) {
    LOG_WARN("varchar buffer not init");
    ret = OB_NOT_INIT;
  } else {
    uint64_t local_bits;
    ObString local_on;
    ObString local_off;
    ObString local_sep;
    int64_t local_n_bits;
    ObString res;

    local_on = on.get_string();
    local_off = off.get_string();
    local_sep = sep.get_string();
    if (OB_FAIL(bits.get_bit(local_bits))) {
      LOG_WARN("fail to get bit", K(bits), K(ret));
    } else if (OB_FAIL(n_bits.get_int(local_n_bits))) {
      LOG_WARN("fail to get int", K(n_bits), K(ret));
    } else if (OB_FAIL(export_set(res, local_bits, local_on, local_off, local_sep, local_n_bits, *expr_ctx.calc_buf_))) {
      LOG_WARN("do export set failed", K(ret));
    } else {
      result.set_string(result_type_.get_type(), res);
      result.set_collation(result_type_);
    }
  }
  return ret;
}

int ObExprExportSet::export_set(
    ObString& ret_str, const uint64_t bits, const ObString& on, const ObString& off, const ObString& sep, const int64_t n_bits, ObExprStringBuf& string_buf)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(n_bits == 0)) {
    // Return empty string
    ret_str.reset();
  } else if (OB_UNLIKELY(on.length() <= 0 && off.length() <= 0 && sep.length() <= 0)) {
    ret_str.reset();
  } else {
    uint64_t local_n_bits = static_cast<uint64_t>(n_bits);
    local_n_bits = easy_min(64, local_n_bits);
    int64_t length_on = on.length();
    int64_t length_off = off.length();
    int64_t length_sep = sep.length();
    int64_t tot_length = 0;  // total length for the result.
    uint64_t i;
    uint64_t mask;
    //compute tot_length and save ans
    tot_length += (local_n_bits - 1) * length_sep;
    for (i = 0, mask = 1; i < local_n_bits; ++i) {
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
    } else {
      char* buf = static_cast<char*>(string_buf.alloc(tot_length));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("alloc memory failed.", K(tot_length), K(ret));
      } else {
        // Core function
        const char* const on_ptr = on.ptr();
        const char* const off_ptr = off.ptr();
        const char* const sep_ptr = sep.ptr();

        char* tmp_buf = buf;
        char* copy;
        int64_t len;
        if (local_n_bits > 0) {
          if (bits & mask) {
            MEMCPY(tmp_buf, on_ptr, length_on);
            tmp_buf += length_on;
          } else {
            MEMCPY(tmp_buf, off_ptr, length_off);
            tmp_buf += length_off;
          }
        }
        for (i = 1; i < local_n_bits; ++i) {
          MEMCPY(tmp_buf, sep_ptr, length_sep);
          tmp_buf += length_sep;
          if (bits & (mask << i)) {
            MEMCPY(tmp_buf, on_ptr, length_on);
            tmp_buf += length_on;
          } else {
            MEMCPY(tmp_buf, off_ptr, length_off);
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
  } else if (bits->is_null()) {
    expr_datum.set_null();
  } else if (on->is_null()) {
    expr_datum.set_null();
  } else if (off->is_null()) {
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
    char default_sep[1] = {','};
    if (OB_ISNULL(sep)) {
      sep_parm.assign(default_sep, 1);
    } else {
      sep_parm = sep->get_string();
    }
    if (OB_ISNULL(n_bits)) {
      n_bits_parm = 64;
    } else {
      n_bits_parm = n_bits->get_uint64();
    }
    if (OB_FAIL(export_set(output, bits->get_uint64(), on->get_string(), off->get_string()
      , sep_parm, n_bits_parm, expr_res_alloc))) {
      LOG_WARN("do export set failed", K(ret));
    } else {
      expr_datum.set_string(output);
    }
  }
  return ret;
}


}  // namespace sql
}  // namespace oceanbase