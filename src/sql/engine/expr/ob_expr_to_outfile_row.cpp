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

#include "sql/engine/expr/ob_expr_to_outfile_row.h"
#include <string.h>
#include "lib/oblog/ob_log.h"
#include "objit/common/ob_item_type.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "share/ob_lob_access_utils.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

ObExprToOutfileRow::ObExprToOutfileRow(ObIAllocator &alloc)
    : ObStringExprOperator(alloc, T_OP_TO_OUTFILE_ROW, N_TO_OUTFILE_ROW, MORE_THAN_ZERO, VALID_FOR_GENERATED_COL, INTERNAL_IN_MYSQL_MODE)
{
  need_charset_convert_ = false;
}

ObExprToOutfileRow::~ObExprToOutfileRow()
{
}

int ObExprToOutfileRow::calc_result_typeN(ObExprResType &type,
                                    ObExprResType *types,
                                    int64_t param_num,
                                    ObExprTypeCtx &type_ctx) const
{
  //objs[0] field_str     varchar
  //objs[1] line_str      varchar
  //objs[2] closed_cht    char
  //objs[3] is_optional   bool
  //objs[4] escaped_cht   char
  //objs[5:] params
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  if (OB_UNLIKELY(param_num <= PARAM_SELECT_ITEM)) {
    ret = OB_INVALID_ARGUMENT_NUM;
    LOG_WARN("invalid argument number", K(ret), K(param_num));
  } else {
    // deduce type and length
    type.set_varbinary();
    // the result is only used to select into outile, so we don't care the accurate length
    type.set_length(OB_MAX_ROW_LENGTH);
    // field_str
    types[PARAM_FIELD].set_calc_type(ObVarcharType);
    types[PARAM_FIELD].set_calc_collation_type(types[PARAM_FIELD].get_collation_type());
    // line_str
    types[PARAM_LINE].set_calc_type(ObVarcharType);
    types[PARAM_LINE].set_calc_collation_type(types[PARAM_LINE].get_collation_type());
    // closed_cht
    types[PARAM_ENCLOSED].set_calc_type(ObVarcharType);
    types[PARAM_ENCLOSED].set_calc_collation_type(types[PARAM_ENCLOSED].get_collation_type());
    // is_optional
    types[PARAM_OPTIONAL].set_calc_type(ObTinyIntType);
    // escaped_cht
    types[PARAM_ESCAPED].set_calc_type(ObVarcharType);
    types[PARAM_ESCAPED].set_calc_collation_type(types[PARAM_ESCAPED].get_collation_type());
  }
  return ret;
}

int ObExprToOutfileRow::cg_expr(ObExprCGCtx &, const ObRawExpr &, ObExpr &expr) const
{
  int ret = OB_SUCCESS;
  CK(expr.arg_cnt_ > PARAM_SELECT_ITEM);
  if (OB_SUCC(ret)) {
    for (int i = PARAM_FIELD; i < PARAM_SELECT_ITEM; i++) {
      if (!expr.args_[i]->is_static_const_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("non-const format not supported", K(ret));
      }
    }
    expr.eval_func_ = &to_outfile_str;
  }
  return ret;
}

int ObExprToOutfileRow::extend_buffer(ObExprOutFileInfo &out_info,
                                      ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  int64_t old_len = out_info.buf_len_;
  int64_t new_len = (old_len == 0) ? OB_MALLOC_MIDDLE_BLOCK_SIZE : old_len * 2;
  if (OB_ISNULL(out_info.buf_ = static_cast<char*>(allocator.alloc(new_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret), K(old_len), K(new_len));
  } else if (OB_ISNULL(out_info.tmp_buf_ = static_cast<char*>(allocator.alloc(new_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret), K(old_len), K(new_len));
  } else {
    out_info.buf_len_ = new_len;
    out_info.tmp_buf_len_ = new_len;
  }
  return ret;
}

int ObExprToOutfileRow::calc_outfile_info(const ObExpr &expr,
                                          ObEvalCtx &ctx,
                                          ObIAllocator &allocator,
                                          ObExprOutFileInfo &out_info)
{
  int ret = OB_SUCCESS;
  ObObj objs_array[PARAM_SELECT_ITEM];
  ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_ISNULL(out_info.print_params_.tz_info_ = session->get_timezone_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get timezone info", K(ret));
  } else {
    out_info.print_params_.use_memcpy_ = true;
    out_info.print_params_.binary_string_print_hex_ = lib::is_oracle_mode();
    out_info.is_optional_ = expr.locate_param_datum(ctx, PARAM_OPTIONAL).get_bool();
  }

  for (int i = 0; OB_SUCC(ret) && i < PARAM_SELECT_ITEM; ++i) {
    OZ(expr.locate_param_datum(ctx, i).to_obj(objs_array[i], expr.args_[i]->obj_meta_,
                                              expr.args_[i]->obj_datum_map_));
  }
  if (OB_SUCC(ret)) {
    out_info.field_ = objs_array[PARAM_FIELD];
    out_info.line_ = objs_array[PARAM_LINE];
    out_info.enclose_ = objs_array[PARAM_ENCLOSED];
    out_info.escape_ = objs_array[PARAM_ESCAPED];
    out_info.print_params_.cs_type_ = static_cast<ObCollationType>(objs_array[PARAM_CHARSET].get_int());
  }

  OZ(extract_fisrt_wchar_from_varhcar(out_info.field_, out_info.wchar_field_));
  OZ(extract_fisrt_wchar_from_varhcar(out_info.line_, out_info.wchar_line_));
  OZ(extract_fisrt_wchar_from_varhcar(out_info.enclose_, out_info.wchar_enclose_));
  OZ(extract_fisrt_wchar_from_varhcar(out_info.escape_, out_info.wchar_escape_));
  OZ(extend_buffer(out_info, allocator));
  return ret;
}

int ObExprToOutfileRow::to_outfile_str(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(expr.arg_cnt_ <= PARAM_SELECT_ITEM)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret));
  } else if (OB_FAIL(expr.eval_param_value(ctx))) {
    LOG_WARN("evaluate parameters values failed", K(ret));
  } else {
    ObExprOutFileInfo *out_info = NULL;
    auto rt_ctx_id = static_cast<uint64_t>(expr.expr_ctx_id_);
    if (NULL == (out_info = static_cast<ObExprOutFileInfo *>
                 (ctx.exec_ctx_.get_expr_op_ctx(rt_ctx_id)))) {
      if (OB_FAIL(ctx.exec_ctx_.create_expr_op_ctx(rt_ctx_id, out_info))) {
        LOG_WARN("failed to create operator ctx", K(ret));
      } else if (OB_FAIL(calc_outfile_info(expr, ctx,
                                           ctx.exec_ctx_.get_allocator(), *out_info))) {
        LOG_WARN("fail calc outfile info", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      do {
        int64_t pos = 0;
        char *buf = out_info->buf_;
        int64_t buf_len =  out_info->buf_len_;
        for (int64_t i = PARAM_SELECT_ITEM; OB_SUCC(ret) && i < expr.arg_cnt_; i++) {
          ObDatum &v = expr.locate_param_datum(ctx, i);
          const ObObjMeta &obj_meta = expr.args_[i]->obj_meta_;
          ObObj obj;
          OZ(v.to_obj(obj, obj_meta, expr.args_[i]->obj_datum_map_));
          if (!obj_meta.is_lob_storage()) {
            OZ(print_field(buf, buf_len, pos, obj, *out_info));
          } else { // text tc
            ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
            common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
            if (OB_SUCC(ret)
                && OB_FAIL(ObTextStringIter::convert_outrow_lob_to_inrow_templob(obj, obj, NULL, &temp_allocator))) {
              LOG_WARN("failed to convert outrow lobs", K(ret), K(obj));
            }
            OZ(print_field(buf, buf_len, pos, obj, *out_info));
          }
          // print field terminator
          if (OB_SUCC(ret) && i != expr.arg_cnt_ - 1) {
            OZ(out_info->field_.print_plain_str_literal(buf, buf_len, pos, out_info->print_params_));
          }
        }
        OZ(out_info->line_.print_plain_str_literal(buf, buf_len, pos, out_info->print_params_));
        if (OB_SUCC(ret)) {
          char *res_buf = NULL;
          if (OB_ISNULL(res_buf = expr.get_str_res_mem(ctx, pos))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("allocate memory failed", K(ret), K(pos));
          } else {
            MEMCPY(res_buf, buf, pos);
            expr_datum.set_string(res_buf, pos);
          }
        }
      } while (OB_SIZE_OVERFLOW == ret
               && OB_SUCC(extend_buffer(*out_info, ctx.exec_ctx_.get_allocator())));
    }
  }
  return ret;
}

int ObExprToOutfileRow::extract_fisrt_wchar_from_varhcar(const ObObj &obj, int32_t &wchar)
{
  int ret = OB_SUCCESS;
  int32_t length = 0;
  if (obj.is_varying_len_char_type()) {
    ObString str = obj.get_varchar();
    if (str.length() > 0) {
      ret = ObCharset::mb_wc(obj.get_collation_type(), str.ptr(), str.length(), length, wchar);
    }
  }
  return ret;
}

// If the FIELDS ESCAPED BY character is not empty, it is used to prefix the following
// characters on output:
// 1. The FIELDS ESCAPED BY character.
// 2. The FIELDS [OPTIONALLY] ENCLOSED BY character.
// 3. The first character of the FIELDS TERMINATED BY and LINES TERMINATED BY values,
//    if the ENCLOSED BY character is empty or unspecified.
// 4. ASCII 0 (what is actually written following the escape character is ASCII 0, not a
//    zero-valued byte).
// 5. If the FIELDS ESCAPED BY character is empty, no characters are escaped and NULL is output
//    as NULL, not \N.
int ObExprToOutfileRow::print_field(char *buf, const int64_t buf_len, int64_t &pos,
                                    const ObObj &obj, ObExprOutFileInfo &out_info)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = 0;
  const bool need_enclose = 0 != out_info.wchar_enclose_ &&
                            (!out_info.is_optional_ || obj.is_string_type()) && !obj.is_null();
  if (need_enclose) {
    OZ(out_info.enclose_.print_plain_str_literal(buf, buf_len, pos, out_info.print_params_));
  }
  if (0 == out_info.wchar_escape_) {
    OZ(obj.print_plain_str_literal(buf, buf_len, pos, out_info.print_params_));
  } else if (obj.is_null()) {
    OZ(out_info.escape_.print_plain_str_literal(buf, buf_len, pos, out_info.print_params_));
    OZ(print_wchar_to_buf(buf, buf_len, pos, 'N', out_info.print_params_.cs_type_));
  } else if (obj.is_string_or_lob_locator_type() && obj.get_collation_type() == CS_TYPE_BINARY) {
    OZ(obj.print_plain_str_literal(buf, buf_len, pos, out_info.print_params_));
  } else {
    OZ(obj.print_plain_str_literal(out_info.tmp_buf_, out_info.tmp_buf_len_, tmp_pos,
                                   out_info.print_params_));
    auto escape_func =
        [buf, buf_len, &pos, need_enclose, &out_info] (ObString &code_point, int32_t wchar) -> int {
      int ret = OB_SUCCESS;
      if (wchar == '\0') {
        OZ(out_info.escape_.print_plain_str_literal(buf, buf_len, pos, out_info.print_params_));
        OZ(print_wchar_to_buf(buf, buf_len, pos, '0', out_info.print_params_.cs_type_));
      } else if (wchar == out_info.wchar_enclose_ || wchar == out_info.wchar_escape_) {
        OZ(out_info.escape_.print_plain_str_literal(buf, buf_len, pos, out_info.print_params_));
        OZ(copy_string_to_buf(buf, buf_len, pos, code_point));
      } else if (!need_enclose && (wchar == out_info.wchar_field_ ||
                                   wchar == out_info.wchar_line_)) {
        OZ(out_info.escape_.print_plain_str_literal(buf, buf_len, pos, out_info.print_params_));
        OZ(copy_string_to_buf(buf, buf_len, pos, code_point));
      } else {
        OZ(copy_string_to_buf(buf, buf_len, pos, code_point));
      }
      return ret;
    };
    ObString tmp_str(out_info.tmp_buf_len_, tmp_pos, out_info.tmp_buf_);
    OZ(ObCharsetUtils::foreach_char(tmp_str, out_info.print_params_.cs_type_, escape_func, true));
  }
  if (need_enclose) {
    OZ(out_info.enclose_.print_plain_str_literal(buf, buf_len, pos, out_info.print_params_));
  }
  return ret;
}

int ObExprToOutfileRow::print_wchar_to_buf(char *buf, const int64_t buf_len, int64_t &pos,
                                          int32_t wchar, ObCollationType coll_type)
{
  int ret = OB_SUCCESS;
  int result_len = 0;
  if (OB_FAIL(ObCharset::wc_mb(coll_type, wchar, buf + pos, buf_len - pos, result_len))) {
    LOG_WARN("failed to convert wc to mb");
  } else {
    pos += result_len;
  }
  return ret;
}

int ObExprToOutfileRow::copy_string_to_buf(char *buf, const int64_t buf_len, int64_t &pos,
                                           const ObString &str)
{
  return databuff_memcpy(buf, buf_len, pos, str.length(), str.ptr());
}

}
}
