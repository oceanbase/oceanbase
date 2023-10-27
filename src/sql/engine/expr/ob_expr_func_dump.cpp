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

#include <string.h>
#include "lib/utility/ob_print_utils.h"
#include "share/object/ob_obj_cast.h"
#include "objit/common/ob_item_type.h"
#include "sql/engine/expr/ob_expr_func_dump.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/expr/ob_expr_util.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

const int64_t MAX_DUMP_BUFFER_SIZE = 1024;
const char *CONST_HEADER = "Typ=%d Len=%ld: ";

enum ReturnFormat {
  RF_OB_SEPC = 0,
  RF_OCT     = 8,//8
  RF_DEC     = 10,//10
  RF_HEX     = 16,//16
  RF_ASCII   = 17,//17
};

ObExprFuncDump::ObExprFuncDump(ObIAllocator &alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_DUMP, N_DUMP, MORE_THAN_ZERO, NOT_VALID_FOR_GENERATED_COL)
{
}

ObExprFuncDump::~ObExprFuncDump()
{
}

int print_value(char *tmp_buf, const int64_t buff_size, int64_t &pos,
    const ObString &value_string, const int64_t fmt_enum,
    const int64_t start_pos, const int64_t print_value_len)
{
  int ret = common::OB_SUCCESS;
  ObString print_value_string;
  if (OB_UNLIKELY(print_value_len < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("print_value_len should not less then zero", K(ret), K(print_value_len));
  } else {
    if (start_pos > 1) {
      if (start_pos > value_string.length()) {
        //empty
      } else {
        print_value_string.assign_ptr(value_string.ptr() + start_pos - 1, MIN((value_string.length() - start_pos), print_value_len));
      }
    } else if (start_pos < 0) {
      if (-start_pos > value_string.length()) {
        print_value_string.assign_ptr(value_string.ptr(), MIN((value_string.length()), print_value_len));
      } else {
        print_value_string.assign_ptr(value_string.ptr() + value_string.length() + start_pos, MIN((-start_pos), print_value_len));
      }
    } else {
      print_value_string.assign_ptr(value_string.ptr(), MIN((value_string.length()), print_value_len));
    }

    if (ReturnFormat::RF_ASCII == fmt_enum) {
      for (int64_t i = 0; i < print_value_string.length() && OB_SUCC(ret); ++i) {
        if (isprint(print_value_string[i])) {
          if (OB_FAIL(databuff_printf(tmp_buf, buff_size, pos, "%c,", print_value_string[i]))) {
            LOG_WARN("failed to databuff_printf", K(ret), K(pos));
          }
        } else {
          if (OB_FAIL(databuff_printf(tmp_buf, buff_size, pos, "%x,", (unsigned)(unsigned char)print_value_string[i]))) {
            LOG_WARN("failed to databuff_printf", K(ret), K(pos));
          }
        }
      }
    } else {//%u, %x, %o
      char fmt_str[4] = {0};
      fmt_str[0] = '%';
      fmt_str[1] = (ReturnFormat::RF_HEX == fmt_enum ? 'x' : (ReturnFormat::RF_OCT == fmt_enum ? 'o' : 'u'));
      fmt_str[2] = ',';

      for (int64_t i = 0; i < print_value_string.length() && OB_SUCC(ret); ++i) {
        if (OB_FAIL(databuff_printf(tmp_buf, buff_size, pos, fmt_str, (unsigned)(unsigned char)print_value_string[i]))) {
          LOG_WARN("failed to databuff_printf", K(ret), K(pos));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    pos -= 1;
    LOG_DEBUG("succ to print_value", K(value_string), K(print_value_string), K(fmt_enum), K(start_pos), K(print_value_len), K(pos));
  }
  return ret;
}

int ObExprFuncDump::calc_result_typeN(ObExprResType &type,
                                      ObExprResType *types,
                                      int64_t param_num,
                                      common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  CK(NULL != type_ctx.get_session());
  CK(NULL != types);
  CK(param_num >= 1);
  if (OB_FAIL(ret)) {
  } else if (lib::is_oracle_mode()) {
    if (OB_UNLIKELY(types[0].is_clob() || types[0].is_blob())) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("type not support now", K(param_num), K(types[0].get_type()), K(ret));
    } else if (OB_UNLIKELY(param_num > 4)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("too many argument not support now", K(param_num), K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "too many argument");
    } else if (types[0].is_null()) {
      type.set_null();
    } else {
      type.set_varchar();
      type.set_collation_level(common::CS_LEVEL_COERCIBLE);
      type.set_collation_type(type_ctx.get_coll_type());
      type.set_length(MAX_DUMP_BUFFER_SIZE);
      const common::ObLengthSemantics default_length_semantics = (OB_NOT_NULL(type_ctx.get_session()) ? type_ctx.get_session()->get_actual_nls_length_semantics() : common::LS_BYTE);
      type.set_length_semantics(default_length_semantics);
      if (param_num > 1) {
        types[1].set_calc_type(ObNumberType);
      }
      if (param_num > 2) {
        types[2].set_calc_type(ObNumberType);
      }
      if (param_num > 3) {
        types[3].set_calc_type(ObNumberType);
      }
    }
  } else {
    if (OB_UNLIKELY(param_num > 1)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("too many argument not support now", K(param_num), K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "too many argument");
    } else if (types[0].is_null()) {
      type.set_null();
    } else {
      type.set_varchar();
      type.set_collation_level(common::CS_LEVEL_COERCIBLE);
      type.set_default_collation_type();
      type.set_length(types[0].get_length());
    }
  }
  return ret;
}

int ObExprFuncDump::calc_params(const common::ObObj *objs,
                                const int64_t param_num,
                                int64_t &fmt_enum,
                                int64_t &start_pos,
                                int64_t &print_value_len) const
{
  int ret = OB_SUCCESS;
  fmt_enum = ReturnFormat::RF_DEC;//default
  start_pos = 0;
  print_value_len = MAX_DUMP_BUFFER_SIZE;

  if (param_num > 1) {
    int64_t fmt_param = 0;
    if (objs[1].is_null()) {
      fmt_param = ReturnFormat::RF_DEC;
    } else if (OB_FAIL(objs[1].get_number().extract_valid_int64_with_trunc(fmt_param))) {
      LOG_WARN("failed to extract_valid_int64_with_trunc", K(ret), "number", objs[1].get_number());
    } else {
      if (fmt_param < ReturnFormat::RF_OB_SEPC || fmt_param >= ReturnFormat::RF_ASCII) {
        fmt_enum = ReturnFormat::RF_ASCII;
      } else if (ReturnFormat::RF_OB_SEPC == fmt_param
                 || ReturnFormat::RF_OCT == fmt_param
                 || ReturnFormat::RF_DEC == fmt_param
                 || ReturnFormat::RF_HEX == fmt_param) {
        fmt_enum = fmt_param;
      } else {
        fmt_enum = ReturnFormat::RF_DEC;//default
      }
    }
  }

  if (OB_SUCC(ret) && param_num > 2) {
    if (OB_FAIL(objs[2].get_number().extract_valid_int64_with_trunc(start_pos))) {
      LOG_WARN("failed to extract_valid_int64_with_trunc", K(ret), "number", objs[2].get_number());
    }
  }

  if (OB_SUCC(ret) && param_num > 3) {
    if (OB_FAIL(objs[3].get_number().extract_valid_int64_with_trunc(print_value_len))) {
      LOG_WARN("failed to extract_valid_int64_with_trunc", K(ret), "number", objs[3].get_number());
    } else {
      print_value_len = std::abs(print_value_len);
      if (0 == print_value_len) {
        print_value_len = MAX_DUMP_BUFFER_SIZE;
      }
    }
  }

  LOG_DEBUG("finish calc_params", K(ret), K(param_num), K(fmt_enum), K(start_pos), K(print_value_len));
  return ret;
}

int ObExprFuncDump::calc_number(const common::ObObj &input,
                                const int64_t fmt_enum,
                                int64_t &start_pos,
                                int64_t &print_value_len,
                                common::ObString &output) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!input.is_number() && !input.is_number_float())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only number arrive here", K(input), K(ret));
  } else {
    const number::ObNumber nmb = input.get_number();
    char *tmp_buf = output.ptr();
    int64_t buff_size = output.size();
    int64_t pos = output.length();
    if (OB_FAIL(databuff_printf(tmp_buf, buff_size, pos, CONST_HEADER,
                                input.get_type(), nmb.get_deep_copy_size()))) {
      LOG_WARN("failed to databuff_printf", K(ret), K(nmb));
    } else if (ReturnFormat::RF_OB_SEPC == fmt_enum) {
      if (OB_FAIL(common::databuff_print_obj(tmp_buf, buff_size, pos, nmb))) {
        LOG_WARN("failed to databuff_printf", K(ret), K(pos));
      }
    } else {
      const int64_t MAX_DATATYPE_VALUE_SIZE = number::ObNumber::MAX_BYTE_LEN;
      char tmp_value_buf[MAX_DATATYPE_VALUE_SIZE] = {0};
      int64_t value_pos = 0;
      const uint32_t tmp_desc_value = nmb.get_desc_value();
      MEMCPY(tmp_value_buf, &tmp_desc_value, sizeof(uint32_t));
      MEMCPY(tmp_value_buf + sizeof(uint32_t), nmb.get_digits(), sizeof(uint32_t) * nmb.get_length());
      value_pos = sizeof(uint32_t) + sizeof(uint32_t) * nmb.get_length();
      ObString value_string(value_pos, tmp_value_buf);

      if (OB_FAIL(print_value(tmp_buf, buff_size, pos, value_string, fmt_enum, start_pos, print_value_len))) {
        LOG_WARN("failed to print_value", K(ret), K(pos), K(fmt_enum));
      }
    }

    if (OB_SUCC(ret)) {
      output.set_length(pos);
      LOG_DEBUG("succ to dump number", K(input), K(fmt_enum), K(output));
    }
  }
  return ret;
}

int ObExprFuncDump::calc_otimestamp(const common::ObObj &input,
                                    const int64_t fmt_enum,
                                    int64_t &start_pos,
                                    int64_t &print_value_len,
                                    common::ObString &output) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!input.is_otimestamp_type() && !input.is_datetime())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only otimestamp arrive here", K(input), K(ret));
  } else {
    ObOTimestampData odata = input.get_otimestamp_value();
    char *tmp_buf = output.ptr();
    int64_t buff_size = output.size();
    int64_t pos = output.length();
    if (input.is_datetime()
        && OB_FAIL(databuff_printf(tmp_buf, buff_size, pos, CONST_HEADER,
            input.get_type(), sizeof(int64_t)))) {
      LOG_WARN("failed to databuff_printf", K(ret), K(input));
    } else if (input.is_otimestamp_type()
               && OB_FAIL(databuff_printf(tmp_buf, buff_size, pos, CONST_HEADER,
                                input.get_type(), input.get_otimestamp_store_size()))) {
      LOG_WARN("failed to databuff_printf", K(ret), K(input));
    } else if (ReturnFormat::RF_OB_SEPC == fmt_enum) {
      if (input.is_datetime()) {
        odata.time_ctx_.desc_ = 0;
      } else if (!input.is_timestamp_tz()) {
        odata.time_ctx_.tz_desc_ = 0;
      }
      if (OB_FAIL(databuff_print_obj(tmp_buf, buff_size, pos, odata))) {
        LOG_WARN("failed to databuff_printf", K(ret), K(pos));
      }
    } else {
      const int64_t MAX_DATATYPE_VALUE_SIZE = sizeof(ObOTimestampData);
      char tmp_value_buf[MAX_DATATYPE_VALUE_SIZE] = {0};
      int64_t value_pos = 0;
      if (input.is_timestamp_tz()) {
        MEMCPY(tmp_value_buf, &odata.time_ctx_.desc_, sizeof(uint32_t));
        value_pos = sizeof(uint32_t);
      } else if (!input.is_datetime()) {
        const uint16_t tail_nsec = odata.time_ctx_.tail_nsec_;
        MEMCPY(tmp_value_buf, &tail_nsec, sizeof(uint16_t));
        value_pos = sizeof(uint16_t);
      }
      MEMCPY(tmp_value_buf + value_pos, &odata.time_us_, sizeof(int64_t));
      value_pos += sizeof(int64_t);
      ObString value_string(value_pos, tmp_value_buf);

      if (OB_FAIL(print_value(tmp_buf, buff_size, pos, value_string, fmt_enum, start_pos, print_value_len))) {
        LOG_WARN("failed to print_value", K(ret), K(pos), K(fmt_enum));
      }
    }
    if (OB_SUCC(ret)) {
      output.set_length(pos);
      LOG_DEBUG("succ to dump otimestamp", K(input), K(fmt_enum), K(output));
    }
  }

  return ret;
}

int ObExprFuncDump::calc_string(const common::ObObj &input,
                                const int64_t fmt_enum,
                                int64_t &start_pos,
                                int64_t &print_value_len,
                                common::ObString &output) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ob_is_string_type(input.get_type())
                  && !ob_is_raw(input.get_type())
                  && !ob_is_rowid_tc(input.get_type()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only string type arrive here", K(input), K(ret));
  } else {
    char *tmp_buf = output.ptr();
    int64_t buff_size = output.size();
    int64_t pos = output.length();

    if (OB_FAIL(databuff_printf(tmp_buf, buff_size, pos, CONST_HEADER,
                                input.get_type(), input.get_val_len()))) {
      LOG_WARN("failed to databuff_printf", K(ret), K(input));
    } else if (OB_FAIL(print_value(tmp_buf, buff_size, pos, input.get_string(), fmt_enum, start_pos, print_value_len))) {
      LOG_WARN("failed to print_value", K(ret), K(pos), K(fmt_enum));
    } else {
      output.set_length(pos);
      LOG_DEBUG("succ to dump string", K(input), K(fmt_enum), K(output));
    }
  }

  return ret;
}

int ObExprFuncDump::calc_double(const common::ObObj &input,
                                const int64_t fmt_enum,
                                int64_t &start_pos,
                                int64_t &print_value_len,
                                common::ObString &output) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!input.is_float() && !input.is_double())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only float/double type arrive here", K(input), K(ret));
  } else {
    char *tmp_buf = output.ptr();
    int64_t buff_size = output.size();
    int64_t pos = output.length();
    const int64_t print_size = (input.is_float() ? sizeof(float) : sizeof(double));

    if (OB_FAIL(databuff_printf(tmp_buf, buff_size, pos, CONST_HEADER,
                                input.get_type(), print_size))) {
      LOG_WARN("failed to databuff_printf", K(ret), K(input));
    } else {
      const int64_t MAX_DATATYPE_VALUE_SIZE = sizeof(double);
      char tmp_value_buf[MAX_DATATYPE_VALUE_SIZE] = {0};
      int64_t value_pos = 0;
      if (input.is_double()) {
        const double tmp_value = input.get_double();
        MEMCPY(tmp_value_buf, &tmp_value, sizeof(double));
        value_pos = sizeof(double);
      } else {
        const float tmp_value = input.get_float();
        MEMCPY(tmp_value_buf, &tmp_value, sizeof(float));
        value_pos = sizeof(float);
      }
      ObString value_string(value_pos, tmp_value_buf);

      if (OB_FAIL(print_value(tmp_buf, buff_size, pos, value_string, fmt_enum, start_pos, print_value_len))) {
        LOG_WARN("failed to print_value", K(ret), K(pos), K(fmt_enum));
      }
    }

    if (OB_SUCC(ret)) {
      output.set_length(pos);
      LOG_DEBUG("succ to dump float/double", K(input), K(fmt_enum), K(output));
    }
  }
  return ret;
}


int ObExprFuncDump::calc_interval(const common::ObObj &input,
                                    const int64_t fmt_enum,
                                    int64_t &start_pos,
                                    int64_t &print_value_len,
                                    common::ObString &output) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!input.is_interval_ds() && !input.is_interval_ym())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only interval arrive here", K(input), K(ret));
  } else {
    char *tmp_buf = output.ptr();
    int64_t buff_size = output.size();
    int64_t pos = output.length();
    const ObIntervalDSValue tmp_interval_ds_value = input.get_interval_ds();
    const ObIntervalYMValue tmp_interval_ym_value = input.get_interval_ym();
    if (OB_FAIL(databuff_printf(tmp_buf, buff_size, pos, CONST_HEADER,
                                input.get_type(), input.get_interval_store_size()))) {
      LOG_WARN("failed to databuff_printf", K(ret), K(input));
    } else if (ReturnFormat::RF_OB_SEPC == fmt_enum) {
      if (input.is_interval_ds()) {
        if (OB_FAIL(databuff_print_obj(tmp_buf, buff_size, pos, (tmp_interval_ds_value)))) {
          LOG_WARN("failed to databuff_printf", K(ret), K(pos));
        }
      } else {
        if (OB_FAIL(databuff_print_obj(tmp_buf, buff_size, pos, (tmp_interval_ym_value)))) {
          LOG_WARN("failed to databuff_printf", K(ret), K(pos));
        }
      }
    } else {
      const int64_t MAX_DATATYPE_VALUE_SIZE = MAX(sizeof(ObIntervalDSValue), sizeof(ObIntervalYMValue));
      char tmp_value_buf[MAX_DATATYPE_VALUE_SIZE] = {0};
      int64_t value_pos = 0;
      if (input.is_interval_ds()) {
        MEMCPY(tmp_value_buf, &tmp_interval_ds_value.fractional_second_, sizeof(int32_t));
        MEMCPY(tmp_value_buf + sizeof(int32_t), &tmp_interval_ds_value.nsecond_, sizeof(int64_t));
        value_pos = sizeof(int32_t) + sizeof(int64_t);
      } else {
        MEMCPY(tmp_value_buf, &tmp_interval_ym_value.nmonth_, sizeof(int64_t));
        value_pos = sizeof(int64_t);
      }
      ObString value_string(value_pos, tmp_value_buf);

      if (OB_FAIL(print_value(tmp_buf, buff_size, pos, value_string, fmt_enum, start_pos, print_value_len))) {
        LOG_WARN("failed to print_value", K(ret), K(pos), K(fmt_enum));
      }
    }

    if (OB_SUCC(ret)) {
      output.set_length(pos);
      LOG_DEBUG("succ to dump interval", K(input), K(fmt_enum), K(output));
    }
  }

  return ret;
}

static int dump_ob_spec(char *buf, int64_t buf_len, int64_t &buf_pos, bool &dumped,
                        const ObExpr &expr, const ObDatum &datum)
{
  dumped = true;
  int ret = OB_SUCCESS;
  switch(expr.datum_meta_.type_) {
    case ObNumberType:
    case ObNumberFloatType: {
      number::ObNumber nmb(datum.get_number());
      OZ(common::databuff_print_obj(buf, buf_len, buf_pos, nmb));
      break;
    }
    case ObDateTimeType: {
      OZ(databuff_print_obj(buf, buf_len, buf_pos, ObOTimestampData(
                  datum.get_datetime(), ObOTimestampData::UnionTZCtx())));
      break;
    }

    case ObTimestampTZType: {
      OZ(databuff_print_obj(buf, buf_len, buf_pos, datum.get_otimestamp_tz()));
      break;
    }

    case ObTimestampLTZType:
    case ObTimestampNanoType: {
      OZ(databuff_print_obj(buf, buf_len, buf_pos, datum.get_otimestamp_tiny()));
      break;
    }
    case ObIntervalYMType: {
      const ObIntervalYMValue tmp_interval_ym_value(datum.get_interval_ym());
      OZ(databuff_print_obj(buf, buf_len, buf_pos, tmp_interval_ym_value));
      break;
    }
    case ObIntervalDSType: {
      OZ(databuff_print_obj(buf, buf_len, buf_pos, datum.get_interval_ds()));
      break;
    }
    default: {
      dumped = false;
    }
  }
  return ret;
}

int ObExprFuncDump::cg_expr(ObExprCGCtx &, const ObRawExpr &, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  CK(rt_expr.arg_cnt_ >= 1 && rt_expr.arg_cnt_ <= 4);
  rt_expr.eval_func_ = &ObExprFuncDump::eval_dump;
  return ret;
}

int ObExprFuncDump::eval_dump(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  if (is_oracle_mode()) {
    ObDatum *input = NULL;
    ObDatum *fmt = NULL;
    ObDatum *pos = NULL;
    ObDatum *len = NULL;
    if (OB_FAIL(expr.eval_param_value(ctx, input, fmt, pos, len))) {
      LOG_WARN("evaluate parameters failed", K(ret));
    } else if (input->is_null()) {
      expr_datum.set_null();
    } else {
      int64_t fmt_val = ReturnFormat::RF_DEC;//default
      int64_t pos_val = 1;
      int64_t len_val = input->len_;

      if (OB_FAIL(ObExprUtil::get_int_param_val(fmt, fmt_val))
          || OB_FAIL(ObExprUtil::get_int_param_val(pos, pos_val))
          || OB_FAIL(ObExprUtil::get_int_param_val(len, len_val))) {
        LOG_WARN("get int parameter value failed", K(ret));
      } else {
        // parameter process same with ObExprFuncDump::calc_params
        if (fmt_val < ReturnFormat::RF_OB_SEPC || fmt_val >= ReturnFormat::RF_ASCII) {
          fmt_val = ReturnFormat::RF_ASCII;
        } else if (!(ReturnFormat::RF_OB_SEPC == fmt_val
                     || ReturnFormat::RF_OCT == fmt_val
                     || ReturnFormat::RF_DEC == fmt_val
                     || ReturnFormat::RF_HEX == fmt_val)) {
          fmt_val = ReturnFormat::RF_DEC; // default
        }
        len_val = std::abs(len_val);
        if (0 == len_val) {
          len_val = input->len_;
        }

        char buf[MAX_DUMP_BUFFER_SIZE] = {0};
        int64_t buf_len = sizeof(buf);
        int64_t buf_pos = 0;
        bool dumped = false;
        if (OB_FAIL(databuff_printf(buf, buf_len, buf_pos, CONST_HEADER,
                                    expr.args_[0]->datum_meta_.type_,
                                    static_cast<int64_t>(input->len_)))) {
          LOG_WARN("data buffer print fail", K(ret));
        } else if (ReturnFormat::RF_OB_SEPC == fmt_val) {
          if (OB_FAIL(dump_ob_spec(buf, buf_len, buf_pos, dumped, *expr.args_[0], *input))) {
            LOG_WARN("dump ob spec failed", K(ret));
          } else if (!dumped) {
            // dump ob spec not supported,  fail back to ReturnFormat::RF_DEC
            fmt_val = ReturnFormat::RF_DEC;
          }
        }

        if (OB_SUCC(ret) && !dumped) {
          if (OB_FAIL(print_value(buf, buf_len, buf_pos, ObString(input->len_, input->ptr_),
                                  fmt_val, pos_val, len_val))) {
            LOG_WARN("print value failed", K(ret));
          }
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(ObExprUtil::set_expr_ascii_result(
                      expr, ctx, expr_datum, ObString(buf_pos, buf)))) {
            LOG_WARN("set ASCII result failed", K(ret));
          }
        }
      }
    }
  } else {
    ObDatum *input = NULL;
    if (OB_FAIL(expr.eval_param_value(ctx, input))) {
      LOG_WARN("evaluate parameters failed", K(ret));
    } else if (input->is_null()) {
      expr_datum.set_null();
    } else {
      switch (expr.args_[0]->datum_meta_.type_) {
        case ObNumberType: {
          number::ObNumber nmb(input->get_number());
          const char *nmb_str = to_cstring(nmb);
          ObString src_str(0, (int32_t)strlen(nmb_str), const_cast<char *>(nmb_str));
          if (OB_FAIL(ObExprUtil::set_expr_ascii_result(
                      expr, ctx, expr_datum, src_str))) {
            LOG_WARN("set ASCII result failed", K(ret));
          }
          break;
        }
        case ObNullType: {
          expr_datum.set_null();
        }
        break;
        default: {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("type not support now", K(expr.args_[0]->datum_meta_.type_), K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "The input type of the DUMP function");
        }
        break;
      }
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
