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
#define OB_MAX_WEIGHT  OB_MAX_VARCHAR_LENGTH

#include <string.h>
#include "objit/common/ob_item_type.h"
#include "sql/engine/expr/ob_expr_weight_string.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "share/object/ob_obj_cast.h"
#include "lib/oblog/ob_log.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "ob_expr_util.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/session/ob_sql_session_info.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{
ObExprWeightString::ObExprWeightString(ObIAllocator &alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_WEIGHT_STRING, N_WEIGHT_STRING, MORE_THAN_ZERO, NOT_VALID_FOR_GENERATED_COL)
{
}
ObExprWeightString::~ObExprWeightString()
{
}
int ObExprWeightString::calc_result_typeN(ObExprResType &type,
                                          ObExprResType *types_stack,
                                          int64_t param_num,
                                          ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(param_num);
  CK (OB_NOT_NULL(type_ctx.get_session()));
  if (NOT_ROW_DIMENSION != row_dimension_ || ObMaxType == types_stack[0].get_type()) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
  } else {
    uint64_t max_length = OB_MAX_VARBINARY_LENGTH; // The maximum length of the result of WEIGHT_STRING()
    uint64_t result_length = types_stack[1].get_param().get_int();
    uint64_t nweight = types_stack[2].get_param().get_int();
    bool as_binary = types_stack[4].get_param().get_int();
    ObCollationLevel coll_level = CS_LEVEL_INVALID;
    if (as_binary) {
      coll_level = CS_LEVEL_IMPLICIT;
      type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_NULL_ON_WARN);
      types_stack[0].set_calc_type(ObVarcharType);
    } else {
      coll_level = types_stack[0].get_collation_level();
      if (types_stack[0].get_type() > ObUNumberType &&
          types_stack[0].get_type() != ObBitType &&
          types_stack[0].get_type() != ObYearType) {
        // When the input is not a numeric type, convert to a varchar type
        type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_NULL_ON_WARN);
        types_stack[0].set_calc_type(ObVarcharType);
      }
    }
    ObCollationType collation_type = types_stack[0].get_collation_type();
    const ObCharsetInfo *cs = ObCharset::get_charset(collation_type); 
    if (types_stack[0].get_type() == ObDateTimeType ||
        types_stack[0].get_type() == ObTimestampType || 
        types_stack[0].get_type() == ObDateType ||
        types_stack[0].get_type() == ObTimeType ) {
      // For types such as date, time, etc., the max_lenght is the length of the type entered
      max_length = types_stack[0].get_length();
    } else if (result_length > 0) {
      max_length = result_length;
    } else if (as_binary) {
      // In the case of as_binary, the max_length with nweight as the output result
      max_length = nweight; 
    } else {
      // If the input is others, use cs->mbmaxlen to calculate the max_length
      max_length = cs->mbmaxlen * MAX(nweight, types_stack[0].get_length()*cs->mbmaxlen);
    }
    type.set_varchar();
    type.set_collation_type(CS_TYPE_BINARY);
    type.set_collation_level(coll_level);
    type.set_length(max_length);
  }
  return ret;
}

int ObExprWeightString::eval_weight_string(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *arg = NULL;
  ObDatum *result_length_arg = NULL;
  ObDatum *nweights_arg = NULL;
  ObDatum *flags_arg = NULL;
  ObDatum *as_binary_arg = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, arg)) ||
      OB_FAIL(expr.args_[1]->eval(ctx, result_length_arg)) ||
      OB_FAIL(expr.args_[2]->eval(ctx, nweights_arg)) ||
      OB_FAIL(expr.args_[3]->eval(ctx, flags_arg)) ||
      OB_FAIL(expr.args_[4]->eval(ctx, as_binary_arg))) {
    LOG_WARN("eval arg failed", K(ret), 
                                KP(arg),
                                KP(result_length_arg),
                                KP(nweights_arg),
                                KP(flags_arg),
                                KP(as_binary_arg));
  } else {
    const ObString str = arg->get_string();
    uint64_t result_length = result_length_arg->get_int();
    uint64_t nweights = nweights_arg->get_int();
    uint64_t flags = flags_arg->get_int();
    bool as_binary = as_binary_arg->get_int();
    int64_t max_allowed_packet = 0;
    ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
    if (arg->is_null()) {
      res_datum.set_null();
    } else if (!as_binary && (expr.args_[0]->datum_meta_.type_ <= ObUNumberType ||
                              expr.args_[0]->datum_meta_.type_ == ObBitType ||
                              expr.args_[0]->datum_meta_.type_ == ObYearType)) {
      res_datum.set_null();
    } else if (arg->get_string().empty() && nweights == 0) {
      res_datum.set_string(nullptr,0);
    } else {
      if (OB_FAIL(session->get_max_allowed_packet(max_allowed_packet))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          max_allowed_packet = OB_MAX_VARCHAR_LENGTH;
        } else {
          LOG_WARN("Failed to get max allow packet size", K(ret));
        }
      }
      // Get the character set and collation information of the input string
      ObCollationType collation_type = CS_TYPE_INVALID;
      if (as_binary) {
        collation_type  = CS_TYPE_BINARY;
      } else {
        collation_type = expr.args_[0]->datum_meta_.cs_type_;
      }
      const ObCharsetInfo *cs = ObCharset::get_charset(collation_type);
      flags = ob_strxfrm_flag_normalize(flags, cs->levels_for_order);
      // calc the length of result
      size_t frm_length = 0;
      size_t tmp_length = 0;
      if (result_length > 0) {
        tmp_length = result_length;
      } else {
        tmp_length = cs->coll->strnxfrmlen(cs, cs->mbmaxlen*MAX(str.length() , nweights));
      }
      if (tmp_length >= max_allowed_packet) {
        // The return result exceeds the maximum limit and returns NULL.
        res_datum.set_null();
      } else {
        int used_nweights = nweights;
        size_t input_length = str.length();
        if (used_nweights) {
          //truncate input string
          input_length = std::min(input_length, cs->cset->charpos(cs, str.ptr(), str.ptr() + str.length(), nweights));
        } else {
          //calc char length
          used_nweights = cs->cset->numchars(cs, str.ptr(), str.ptr() + str.length());
        }
        bool is_valid_unicode_tmp = 1;
        char *out_buf = expr.get_str_res_mem(ctx, tmp_length);
        // For the case where the input is an empty string but the nweight is not 0,
        // the weight_string function will call strnxfrm() to padding the result
        // eg:
        // mysql> select HEX(WEIGHT_STRING('' as char(3)));
        // +-----------------------------------+
        // | HEX(WEIGHT_STRING('' as char(3))) |
        // +-----------------------------------+
        // | 002000200020                      |
        // +-----------------------------------+
        // However, the strnxfrm requires that the input cannot be a null ptr,
        // so an empty string is set as the input.
        const char* tmp_empty_str = "";
        if (OB_ISNULL(out_buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc output buf",K(ret));
        } else {
          frm_length = cs->coll->strnxfrm(cs,
                                        reinterpret_cast<uchar *>(out_buf),
                                        tmp_length,
                                        used_nweights,
                                        str.ptr() != NULL? reinterpret_cast<const uchar *>(str.ptr()) : reinterpret_cast<const uchar *>(tmp_empty_str),
                                        input_length,
                                        flags,
                                        &is_valid_unicode_tmp);
          res_datum.set_string(out_buf,frm_length);
        }
      }
    }
  }
  return ret;
}

int ObExprWeightString::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr,
    ObExpr &rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = ObExprWeightString::eval_weight_string;
  return OB_SUCCESS;
}

uint64_t ObExprWeightString::ob_strxfrm_flag_normalize(uint64_t flags, uint64_t maximum)
{
   /* If levels are omitted, then 1-maximum is assumed*/
  if (!(flags & OB_STRXFRM_LEVEL_ALL)) {
    static uint64_t def_level_flags[]= {0, 0x01, 0x03, 0x07, 0x0F, 0x1F, 0x3F };
    uint64_t flag_pad= flags & (OB_STRXFRM_PAD_WITH_SPACE | OB_STRXFRM_PAD_TO_MAXLEN);
    flags= def_level_flags[maximum] | flag_pad;
  }
  else {
    uint64_t i = 0;
    uint64_t flag_lev= flags & OB_STRXFRM_LEVEL_ALL;
    uint64_t flag_dsc= (flags >> OB_STRXFRM_DESC_SHIFT) & OB_STRXFRM_LEVEL_ALL;
    uint64_t flag_rev= (flags >> OB_STRXFRM_REVERSE_SHIFT) & OB_STRXFRM_LEVEL_ALL;
    uint64_t flag_pad= flags & (OB_STRXFRM_PAD_WITH_SPACE | OB_STRXFRM_PAD_TO_MAXLEN);
    /*
      If any level number is greater than the maximum,
      it is treated as the maximum.
    */
    maximum--;
    flags= 0;
    for (i= 0; i < OB_STRXFRM_NLEVELS; i++)
    {
      uint64_t src_bit= 1 << i;
      if (flag_lev & src_bit) {
        uint64_t dst_bit= 1 << OB_MIN(i, maximum);
        flags|= dst_bit;
        flags|= (flag_dsc & dst_bit) << OB_STRXFRM_DESC_SHIFT;
        flags|= (flag_rev & dst_bit) << OB_STRXFRM_REVERSE_SHIFT;
      }
    }
    flags|= flag_pad;
  }
  return flags;
}


}
}
