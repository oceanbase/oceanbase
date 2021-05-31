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
#include "sql/parser/ob_item_type.h"
#include "sql/session/ob_sql_session_info.h"

using namespace oceanbase::common;

namespace oceanbase {
namespace sql {

ObExprToOutfileRow::ObExprToOutfileRow(ObIAllocator& alloc)
    : ObStringExprOperator(alloc, T_OP_TO_OUTFILE_ROW, N_TO_OUTFILE_ROW, MORE_THAN_ZERO)
{
  need_charset_convert_ = false;
}

ObExprToOutfileRow::~ObExprToOutfileRow()
{}

int ObExprToOutfileRow::calc_resultN(
    ObObj& result, const ObObj* objs_array, int64_t param_num, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  // objs[0] field_str     varchar
  // objs[1] line_str      varchar
  // objs[2] closed_cht    char
  // objs[3] is_optional   bool
  // objs[4:] params
  if (OB_ISNULL(expr_ctx.calc_buf_) || OB_UNLIKELY(param_num <= PARAM_SELECT_ITEM) || OB_ISNULL(objs_array)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(param_num), K(expr_ctx.calc_buf_), K(objs_array));
  } else {
    const ObObj& field_str = objs_array[PARAM_FIELD];
    const ObObj& line_str = objs_array[PARAM_LINE];
    char closed_cht = 0;
    bool is_optional = objs_array[PARAM_OPTIONAL].get_bool();
    if (objs_array[PARAM_ENCLOSED].is_varying_len_char_type() &&
        objs_array[PARAM_ENCLOSED].get_varchar().length() == 1) {
      closed_cht = objs_array[PARAM_ENCLOSED].get_varchar()[0];
    }
    SMART_VAR(char[OB_MAX_ROW_LENGTH], buf)
    {
      const int64_t buf_len = OB_MAX_ROW_LENGTH;
      int64_t pos = 0;
      for (int64_t i = PARAM_SELECT_ITEM; OB_SUCC(ret) && i < param_num; ++i) {
        const ObObj& obj = objs_array[i];
        if (0 != closed_cht && (!is_optional || obj.is_string_type())) {
          // closed by "a" (for all obj) or optionally by "a" (for string obj)
          if (OB_FAIL(copy_char_to_buf(buf, buf_len, pos, closed_cht))) {
            LOG_WARN("print closed character failed", K(ret), K(closed_cht));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(obj.print_plain_str_literal(buf, buf_len, pos))) {  // obj value
            LOG_WARN("print sql failed", K(ret), K(obj));
          } else if (0 != closed_cht && (!is_optional || obj.is_string_type())) {
            if (OB_FAIL(copy_char_to_buf(buf, buf_len, pos, closed_cht))) {
              LOG_WARN("print closed character failed", K(ret), K(closed_cht));
            }
          }
          // field terminated by "a"
          if (OB_SUCC(ret) && i != param_num - 1 && field_str.is_varying_len_char_type()) {
            if (OB_FAIL(copy_string_to_buf(buf, buf_len, pos, field_str.get_varchar()))) {
              LOG_WARN("print field str failed", K(ret), K(field_str));
            }
          }
        }
      }
      if (OB_SUCC(ret) && line_str.is_varying_len_char_type()) {  // lines terminated by "a"
        ret = copy_string_to_buf(buf, buf_len, pos, line_str.get_varchar());
      }
      if (OB_SUCC(ret)) {
        char* res_buf = NULL;
        if (OB_ISNULL(res_buf = static_cast<char*>(expr_ctx.calc_buf_->alloc(pos)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("alloc memory failed", K(ret), K(pos));
        } else {
          MEMCPY(res_buf, buf, pos);
          result.set_varchar(res_buf, pos);
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    result.set_meta_type(result_type_);
  }
  return ret;
}

int ObExprToOutfileRow::calc_result_typeN(
    ObExprResType& type, ObExprResType* types, int64_t param_num, ObExprTypeCtx& type_ctx) const
{
  // objs[0] field_str     varchar
  // objs[1] line_str      varchar
  // objs[2] closed_cht    char
  // objs[3] is_optional   bool
  // objs[4:] params
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  if (OB_UNLIKELY(param_num <= 4)) {
    ret = OB_INVALID_ARGUMENT_NUM;
    LOG_WARN("invalid argument number", K(ret), K(param_num));
  } else {
    type.set_varbinary();
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
  }
  return ret;
}

int ObExprToOutfileRow::cg_expr(ObExprCGCtx&, const ObRawExpr&, ObExpr& expr) const
{
  int ret = OB_SUCCESS;
  CK(expr.arg_cnt_ > PARAM_SELECT_ITEM);
  if (OB_SUCC(ret)) {
    expr.eval_func_ = &to_outfile_str;
  }
  return ret;
}

int ObExprToOutfileRow::to_outfile_str(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.eval_param_value(ctx))) {
    LOG_WARN("evaluate parameters values failed", K(ret));
  } else {
    ObDatum& field_str = expr.locate_param_datum(ctx, PARAM_FIELD);
    const ObObjMeta& field_meta = expr.args_[PARAM_FIELD]->obj_meta_;
    ObDatum& line_str = expr.locate_param_datum(ctx, PARAM_LINE);
    const ObObjMeta& line_meta = expr.args_[PARAM_LINE]->obj_meta_;
    const ObString& closed_cht_str = expr.locate_param_datum(ctx, PARAM_ENCLOSED).get_string();
    char closed_cht = 0;
    bool is_optional = expr.locate_param_datum(ctx, PARAM_OPTIONAL).get_bool();
    if (expr.args_[PARAM_ENCLOSED]->obj_meta_.is_varying_len_char_type() && closed_cht_str.length() == 1) {
      closed_cht = closed_cht_str[0];
    }
    SMART_VAR(char[OB_MAX_ROW_LENGTH], buf)
    {
      const int64_t buf_len = OB_MAX_ROW_LENGTH;
      int64_t pos = 0;

      for (int64_t i = PARAM_SELECT_ITEM; OB_SUCC(ret) && i < expr.arg_cnt_; i++) {
        ObDatum& v = expr.locate_param_datum(ctx, i);
        const ObObjMeta& obj_meta = expr.args_[i]->obj_meta_;
        if (0 != closed_cht && (!is_optional || obj_meta.is_string_type())) {
          // closed by "a" (for all obj) or optionally by "a" (for string obj)
          if (OB_FAIL(copy_char_to_buf(buf, buf_len, pos, closed_cht))) {
            LOG_WARN("print closed character failed", K(ret), K(closed_cht));
          }
        }
        if (OB_SUCC(ret)) {
          ObObj obj;
          if (OB_FAIL(v.to_obj(obj, obj_meta, expr.args_[i]->obj_datum_map_))) {
            LOG_WARN("convert datum to obj failed", K(ret));
          } else if (OB_FAIL(obj.print_plain_str_literal(buf, buf_len, pos))) {  // obj value
            LOG_WARN("print sql failed", K(ret), K(obj));
          } else if (0 != closed_cht && (!is_optional || obj.is_string_type())) {
            if (OB_FAIL(copy_char_to_buf(buf, buf_len, pos, closed_cht))) {
              LOG_WARN("print closed character failed", K(ret), K(closed_cht));
            }
          }
          // field terminated by "a"
          if (OB_SUCC(ret) && i != expr.arg_cnt_ - 1 && field_meta.is_varying_len_char_type()) {
            if (OB_FAIL(copy_string_to_buf(buf, buf_len, pos, field_str.get_string()))) {
              LOG_WARN("print field str failed", K(ret), K(field_str));
            }
          }
        }
      }

      if (OB_SUCC(ret) && line_meta.is_varying_len_char_type()) {  // lines terminated by "a"
        ret = copy_string_to_buf(buf, buf_len, pos, line_str.get_string());
      }
      if (OB_SUCC(ret)) {
        char* res_buf = NULL;
        if (OB_ISNULL(res_buf = expr.get_str_res_mem(ctx, pos))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", K(ret), K(pos));
        } else {
          MEMCPY(res_buf, buf, pos);
          expr_datum.set_string(res_buf, pos);
        }
      }
    }
  }
  return ret;
}

int ObExprToOutfileRow::copy_string_to_buf(char* buf, const int64_t buf_len, int64_t& pos, const ObString& str)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(buf) && pos + str.length() <= buf_len) {
    MEMCPY(buf + pos, str.ptr(), str.length());
    pos += str.length();
  } else {
    ret = OB_SIZE_OVERFLOW;
  }
  return ret;
}

int ObExprToOutfileRow::copy_char_to_buf(char* buf, const int64_t buf_len, int64_t& pos, const char c)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(buf) && pos < buf_len) {
    buf[pos] = c;
    pos++;
  } else {
    ret = OB_SIZE_OVERFLOW;
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
