// Copyright (c) 2015-2016 Alibaba Inc. All Rights Reserved.
// Author:
//   jiajingzhe.jjz@alibaba-inc.com
// Normalizer:
//
#define USING_LOG_PREFIX SQL_ENG
#define OB_MAX_WEIGHT  OB_MAX_VARCHAR_LENGTH

#include <string.h>
#include "sql/parser/ob_item_type.h"
#include "sql/engine/expr/ob_expr_weight_string.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "share/object/ob_obj_cast.h"
#include "lib/oblog/ob_log.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "ob_expr_util.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{
ObExprWeightString::ObExprWeightString(ObIAllocator &alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_WEIGHT_STRING, N_WEIGHT_STRING, MORE_THAN_ZERO)
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
    if (types_stack[0].get_type() > ObUNumberType ) {
      // 输入不是数字类型时
      type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_NULL_ON_WARN);
      types_stack[0].set_calc_type(ObVarcharType);
    }
    int max_length = OB_MAX_VARBINARY_LENGTH; // The maximum length of the result of WEIGHT_STRING()
    int result_length = types_stack[1].get_param().get_int();
    int nweight = types_stack[2].get_param().get_int();
    bool as_binary = types_stack[4].get_param().get_int();
    ObCollationLevel coll_level = CS_LEVEL_INVALID;
    if (as_binary) {
      coll_level = CS_LEVEL_IMPLICIT;
    } else {
      coll_level = types_stack[0].get_collation_level();
    }
    ObCollationType collation_type = types_stack[0].get_collation_type();
    const ObCharsetInfo *cs = ObCharset::get_charset(collation_type); 
    if (types_stack[0].get_type() == ObDateTimeType ||
        types_stack[0].get_type() == ObTimestampType || 
        types_stack[0].get_type() == ObDateType ||
        types_stack[0].get_type() == ObTimeType ) {
      // 日期、时间等类型，max_lenght是输入的类型的长度
      max_length = types_stack[0].get_length();
    } else if (result_length > 0) {
      max_length = result_length;
    } else if (as_binary) {
      // as_binary的情况下，以nweight作为输出结果的max_length
      max_length = nweight; 
    } else {
      // 输入为 char的情况下，使用cs->mbmaxlen计算max_length
      max_length = cs->mbmaxlen * max(nweight, types_stack[0].get_length()*cs->mbmaxlen);
    }
    ObObj aaa = types_stack[0].get_param();
    // 推导结果
    type.set_varchar();
    type.set_collation_type(CS_TYPE_BINARY);
    type.set_collation_level(coll_level);
    type.set_length(max_length);
  }
  return ret;
}

int ObExprWeightString::calc_resultN(common::ObObj &result , const common::ObObj *objs_array,
                                      int64_t param_num,common::ObExprCtx &expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("varchar buffer not init", K(ret));
  }
  if (param_num == 5) {
    if (OB_UNLIKELY(objs_array[0].is_null()) ||
        OB_UNLIKELY(objs_array[1].is_null()) ||
        OB_UNLIKELY(objs_array[2].is_null()) ||
        OB_UNLIKELY(objs_array[3].is_null()) ||
        OB_UNLIKELY(objs_array[4].is_null()) ) {
      result.set_null();
    } else {
      const ObString str =  objs_array[0].get_string();
      int result_length = objs_array[1].get_int();
      int nweights = objs_array[2].get_int();
      int flags = objs_array[3].get_int();
      bool as_binary = objs_array[4].get_int();
      ObCollationType collation_type = CS_TYPE_INVALID;
      if (as_binary) {
        collation_type  = CS_TYPE_BINARY;
      } else {
        collation_type = objs_array[0].get_collation_type();
      }
      const ObCharsetInfo *cs = ObCharset::get_charset(collation_type);
      flags = ob_strxfrm_flag_normalize(flags, cs->levels_for_order);
      // calc the length of result
      size_t frm_length = 0; 
      size_t tmp_length = 0;
      if (result_length > 0) {
        tmp_length = result_length;
      } else {
        tmp_length = cs->coll->strnxfrmlen(cs, cs->mbmaxlen*max(str.length() , nweights));
      }
      int is_valid_unicode_tmp = 1;
      char *out_buf;
      if (OB_ISNULL(out_buf = static_cast<char*>(expr_ctx.calc_buf_->alloc(tmp_length)))) {
        result.set_null();
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("alloc memory failed",K(ret), K(out_buf), K(tmp_length));
      } else {
        frm_length = cs->coll->strnxfrm(cs,
                                      reinterpret_cast<uchar *>(out_buf), 
                                      tmp_length, 
                                      nweights ? nweights: tmp_length,
                                      reinterpret_cast<const uchar *>(str.ptr()), 
                                      str.length(),
                                      flags, 
                                      &is_valid_unicode_tmp);
        result.set_varchar(out_buf,frm_length);
      }
    } 
  } else {
    ret = OB_INVALID_ARGUMENT_NUM;
    result.set_null();
    LOG_ERROR("invalid argument num",K(ret), K(param_num), K(objs_array));
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
  } else if (arg->is_null() ||
             arg->get_string() == NULL ||
             expr.args_[0]->datum_meta_.type_ <= ObUNumberType ) {
    // The input string is NULL or numeric
    res_datum.set_null();
  } else {
    const ObString str = arg->get_string();
    int result_length = result_length_arg->get_int();
    int nweights = nweights_arg->get_int();
    int flags = flags_arg->get_int();
    bool as_binary = as_binary_arg->get_int();

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
      tmp_length = cs->coll->strnxfrmlen(cs, cs->mbmaxlen*max(str.length() , nweights));
    }
    int is_valid_unicode_tmp = 1;
    char *out_buf = expr.get_str_res_mem(ctx, tmp_length);
    if (OB_ISNULL(out_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      frm_length = cs->coll->strnxfrm(cs,
                                    reinterpret_cast<uchar *>(out_buf), 
                                    tmp_length, 
                                    nweights ? nweights: tmp_length,
                                    reinterpret_cast<const uchar *>(str.ptr()), 
                                    str.length(),
                                    flags, 
                                    &is_valid_unicode_tmp);
      res_datum.set_string(out_buf,frm_length);
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
        uint64_t dst_bit= 1 << std::min(i, maximum);
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
