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

#define USING_LOG_PREFIX SQL

#include <string.h>
#include "lib/charset/ob_dtoa.h"
#include "share/object/ob_obj_cast_util.h"
#include "share/object/ob_obj_cast.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/engine/ob_serializable_function.h"
#include "lib/json_type/ob_json_tree.h"
#include "lib/json_type/ob_json_bin.h"
#include "lib/json_type/ob_json_base.h"
#include "lib/json_type/ob_json_parse.h"

namespace oceanbase {
namespace sql {
using namespace oceanbase::common;

//// common function and macro
#define CAST_FUNC_NAME(intype, outtype) \
  int intype##_##outtype(const sql::ObExpr& expr, sql::ObEvalCtx& ctx, sql::ObDatum& res_datum)

#define CAST_ENUMSET_FUNC_NAME(intype, outtype)             \
  int intype##_##outtype(const sql::ObExpr& expr,           \
      const common::ObIArray<common::ObString>& str_values, \
      const uint64_t cast_mode,                             \
      sql::ObEvalCtx& ctx,                                  \
      sql::ObDatum& res_datum)

// OB_ISNULL(expr.args_[0]), arg_cnt_ == 1, OB_ISNULL(child_res->ptr_) won't check
#define EVAL_ARG()                                    \
  int ret = OB_SUCCESS;                               \
  ObDatum* child_res = NULL;                          \
  if (OB_FAIL(expr.args_[0]->eval(ctx, child_res))) { \
    LOG_WARN("eval arg failed", K(ret));              \
  } else if (child_res->is_null()) {                  \
    res_datum.set_null();                             \
  } else

#define EVAL_ARG_FOR_CAST_TO_JSON()                                                                   \
  int ret = OB_SUCCESS;                                                                               \
  ObDatum *child_res = NULL;                                                                          \
  ObObjType in_type = expr.args_[0]->datum_meta_.type_;                                               \
  if (OB_FAIL(expr.args_[0]->eval(ctx, child_res))) {                                                 \
    LOG_WARN("eval arg failed", K(ret));                                                              \
  } else if (child_res->is_null()) {                                                                  \
    res_datum.set_null();                                                                             \
  } else if (CM_IS_COLUMN_CONVERT(expr.extra_) && is_mysql_unsupported_json_column_conversion(in_type)) {  \
    ret = OB_ERR_INVALID_JSON_TEXT;                                                                   \
    LOG_USER_ERROR(OB_ERR_INVALID_JSON_TEXT);                                                         \
  } else

// in oracle mode, the empty string of longtext type is not equal to null, and the empty
// string of other string types is equal to null
#define EVAL_STRING_ARG()                                                                                             \
  int ret = OB_SUCCESS;                                                                                               \
  ObDatum* child_res = NULL;                                                                                          \
  if (OB_FAIL(expr.args_[0]->eval(ctx, child_res))) {                                                                 \
    LOG_WARN("eval arg failed", K(ret));                                                                              \
  } else if (child_res->is_null() ||                                                                                  \
             (lib::is_oracle_mode() && 0 == child_res->len_ && ObLongTextType != expr.args_[0]->datum_meta_.type_)) { \
    res_datum.set_null();                                                                                             \
  } else

#define DEF_IN_OUT_TYPE()                               \
  int warning = OB_SUCCESS;                             \
  ObObjType in_type = expr.args_[0]->datum_meta_.type_; \
  ObObjType out_type = expr.datum_meta_.type_;

// if you use ObDatum::get_xxx(), you need to pass one more parameter to specify the function name
// so the following macro directly casts
#define DEF_IN_OUT_VAL(in_type, out_type, init_val)                      \
  int warning = OB_SUCCESS;                                              \
  in_type in_val = *(reinterpret_cast<const in_type*>(child_res->ptr_)); \
  out_type out_val = (init_val);

#define CAST_FAIL(stmt) (OB_UNLIKELY((OB_SUCCESS != (ret = get_cast_ret((expr.extra_), (stmt), warning)))))

// similar to CAST_FAIL, but the above macro will use expr.extra_ as cast_mode, so one less
// parameter is written when using it
#define CAST_FAIL_CM(stmt, cast_mode) (OB_UNLIKELY((OB_SUCCESS != (ret = get_cast_ret((cast_mode), (stmt), warning)))))

#define GET_SESSION()                                           \
  ObBasicSessionInfo* session = ctx.exec_ctx_.get_my_session(); \
  if (OB_ISNULL(session)) {                                     \
    ret = OB_ERR_UNEXPECTED;                                    \
    LOG_WARN("session is NULL", K(ret));                        \
  } else

static OB_INLINE int get_cast_ret(const ObCastMode& cast_mode, int ret, int& warning)
{
  // compatibility for old ob
  if (OB_UNLIKELY(OB_ERR_UNEXPECTED_TZ_TRANSITION == ret) || OB_UNLIKELY(OB_ERR_UNKNOWN_TIME_ZONE == ret)) {
    ret = OB_INVALID_DATE_VALUE;
  } else if (OB_SUCCESS != ret && CM_IS_WARN_ON_FAIL(cast_mode)) {
    warning = ret;
    ret = OB_SUCCESS;
  }
  return ret;
}

// When an error occurs, the processing is as follows:
// if only WARN_ON_FAIL is set, the error code will be overwritten
// if WARN_ON_FAIL and ZERO_ON_WARN are set, the error code will be overwritten, and the result will be set to 0
// if WARN_ON_FAIL and NULL_ON_WARN are set, the error code will be overwritten, and the result will be set to null
#define SET_RES_OBJ(cast_mode, func_val, zero_value, value)                                                      \
  do {                                                                                                           \
    if (OB_SUCC(ret)) {                                                                                          \
      if (OB_SUCCESS == warning || OB_ERR_TRUNCATED_WRONG_VALUE == warning || OB_DATA_OUT_OF_RANGE == warning || \
          OB_ERR_DATA_TRUNCATED == warning || OB_ERR_DOUBLE_TRUNCATED == warning ||                              \
          OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD == warning) {                                                   \
        res_datum.set_##func_val(value);                                                                         \
      } else if (CM_IS_ZERO_ON_WARN(cast_mode)) {                                                                \
        res_datum.set_##func_val(zero_value);                                                                    \
      } else {                                                                                                   \
        res_datum.set_null();                                                                                    \
      }                                                                                                          \
    } else {                                                                                                     \
      res_datum.set_##func_val(value);                                                                           \
    }                                                                                                            \
  } while (0)

// Macros for setting timestamp nano and timestamp ltz
#define SET_RES_OTIMESTAMP_10BYTE(value)                                                                         \
  do {                                                                                                           \
    if (OB_SUCC(ret)) {                                                                                          \
      if (OB_SUCCESS == warning || OB_ERR_TRUNCATED_WRONG_VALUE == warning || OB_DATA_OUT_OF_RANGE == warning || \
          OB_ERR_DATA_TRUNCATED == warning || OB_ERR_DOUBLE_TRUNCATED == warning ||                              \
          OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD == warning) {                                                   \
        res_datum.set_otimestamp_tiny(value);                                                                    \
      } else if (CM_IS_ZERO_ON_WARN(expr.extra_)) {                                                              \
        res_datum.set_otimestamp_tiny(ObOTimestampData());                                                       \
      } else {                                                                                                   \
        res_datum.set_null();                                                                                    \
      }                                                                                                          \
    } else {                                                                                                     \
      res_datum.set_otimestamp_tiny(value);                                                                      \
    }                                                                                                            \
  } while (0)

#define SET_RES_INT(value) SET_RES_OBJ(expr.extra_, int, 0, value)
#define SET_RES_BIT(value) SET_RES_OBJ(expr.extra_, uint, 0, value)
#define SET_RES_UINT(value) SET_RES_OBJ(expr.extra_, uint, 0, value)
#define SET_RES_DATE(value) SET_RES_OBJ(expr.extra_, date, ObTimeConverter::ZERO_DATE, value)
#define SET_RES_TIME(value) SET_RES_OBJ(expr.extra_, time, ObTimeConverter::ZERO_TIME, value)
#define SET_RES_YEAR(value) SET_RES_OBJ(expr.extra_, year, ObTimeConverter::ZERO_YEAR, value)
#define SET_RES_FLOAT(value) SET_RES_OBJ(expr.extra_, float, 0.0, value)
#define SET_RES_DOUBLE(value) SET_RES_OBJ(expr.extra_, double, 0.0, value)
#define SET_RES_ENUM(value) SET_RES_OBJ(expr.extra_, enum, 0, value)
#define SET_RES_SET(value) SET_RES_OBJ(expr.extra_, set, 0, value)
#define SET_RES_OTIMESTAMP(value) SET_RES_OBJ(expr.extra_, otimestamp_tz, ObOTimestampData(), value)
#define SET_RES_INTERVAL_YM(value) SET_RES_OBJ(expr.extra_, interval_nmonth, 0, value)
#define SET_RES_INTERVAL_DS(value) SET_RES_OBJ(expr.extra_, interval_ds, ObIntervalDSValue(), value)
#define SET_RES_DATETIME(value) SET_RES_OBJ(expr.extra_, datetime, ObTimeConverter::ZERO_DATETIME, value)

static const int64_t MAX_DOUBLE_STRICT_PRINT_SIZE = 512;

static OB_INLINE int serialize_obnumber(number::ObNumber& nmb, ObIAllocator& allocator, ObString& out_str)
{
  int ret = OB_SUCCESS;
  const number::ObNumber::Desc& nmb_desc = nmb.get_desc();
  uint32_t* digits = nmb.get_digits();
  char* buf = NULL;
  int64_t buf_len = sizeof(nmb_desc) + nmb_desc.len_ * sizeof(uint32_t);
  if (OB_ISNULL(buf = reinterpret_cast<char*>(allocator.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory failed", K(ret), K(buf_len));
  } else {
    MEMCPY(buf, &nmb_desc, sizeof(nmb_desc));
    MEMCPY(buf + sizeof(nmb_desc), digits, nmb_desc.len_ * sizeof(uint32_t));
    out_str.assign_ptr(buf, static_cast<int32_t>(buf_len));
  }
  return ret;
}

int ObDatumHexUtils::hex(
    const ObExpr& expr, const ObString& in_str, ObEvalCtx& ctx, ObIAllocator& calc_alloc, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  if (in_str.empty()) {
    if (lib::is_oracle_mode()) {
      res_datum.set_null();
    } else {
      res_datum.set_string(NULL, 0);
    }
  } else {
    char* buf = NULL;
    bool need_convert = false;
    ObCollationType def_cs = ObCharset::get_system_collation();
    ObCollationType dst_cs = expr.datum_meta_.cs_type_;
    ObIAllocator* alloc = NULL;
    ObExprStrResAlloc res_alloc(expr, ctx);
    // check if need convert first, and setup alloc. we can avoid copy converted res str.
    if (ObExprUtil::need_convert_string_collation(def_cs, dst_cs, need_convert)) {
      LOG_WARN("check need convert cs type failed", K(ret), K(def_cs), K(dst_cs));
    } else if (need_convert) {
      alloc = &calc_alloc;
    } else {
      alloc = &res_alloc;
    }
    if (OB_SUCC(ret)) {
      const int32_t alloc_length = in_str.length() * 2;
      if (OB_ISNULL(buf = reinterpret_cast<char*>(alloc->alloc(alloc_length)))) {
        res_datum.set_null();
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc memory failed", K(ret), K(alloc_length));
      } else {
        static const char* HEXCHARS = "0123456789ABCDEF";
        int32_t pos = 0;
        for (int32_t i = 0; i < in_str.length(); ++i) {
          buf[pos++] = HEXCHARS[in_str[i] >> 4 & 0xF];
          buf[pos++] = HEXCHARS[in_str[i] & 0xF];
        }
        ObString res_str;
        if (need_convert) {
          if (OB_FAIL(ObExprUtil::convert_string_collation(ObString(pos, buf), def_cs, res_str, dst_cs, res_alloc))) {
            LOG_WARN("convert string collation failed", K(ret));
          }
        } else {
          res_str.assign_ptr(buf, pos);
        }
        if (OB_SUCC(ret)) {
          res_datum.set_string(res_str);
        }
      }
    }
  }
  return ret;
}

static OB_INLINE int common_construct_otimestamp(
    const ObObjType type, const ObDatum& in_datum, ObOTimestampData& out_val);
int ObDatumHexUtils::rawtohex(const ObExpr& expr, const ObString& in_str, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  ObObjType in_type = expr.args_[0]->datum_meta_.type_;
  if (0 >= in_str.length()) {
    res_datum.set_null();
  } else if (!(ObNullType < in_type && in_type < ObMaxType)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid in type", K(ret), K(in_type));
  } else {
    ObIAllocator& tmp_alloc = ctx.get_reset_tmp_alloc();
    ObString out_str;
    ObOTimestampData time_in_val;
    switch (in_type) {
      // TODO::this should same as oracle, and support dump func
      case ObTinyIntType:
      case ObSmallIntType:
      case ObInt32Type:
      case ObIntType: {
        // although tiny/small/int32/int actually occupies less than 8 bytes,
        // but the CG stage will still allocate 8 bytes of space for it
        int64_t in_val = *(reinterpret_cast<const int64_t*>(in_str.ptr()));
        number::ObNumber nmb;
        if (OB_FAIL(nmb.from(in_val, tmp_alloc))) {
          LOG_WARN("fail to int_number", K(ret), K(in_val), "type", in_type);
        } else if (OB_FAIL(serialize_obnumber(nmb, tmp_alloc, out_str))) {
          LOG_WARN("serialize_obnumber failed", K(ret), K(nmb));
        }
        break;
      }
      case ObNumberFloatType:
      case ObNumberType: {
        out_str = in_str;
        break;
      }
      case ObDateTimeType: {
        // shallow copy is ok. unhex accept const argument
        out_str = in_str;
        break;
      }
      case ObNVarchar2Type:
      case ObNCharType:
      case ObVarcharType:
      case ObCharType:
      case ObLongTextType:
      case ObJsonType:
      case ObRawType: {
        // https://www.techonthenet.com/oracle/functions/rawtohex.php
        // NOTE:: when convert string to raw, Oracle use utl_raw.cast_to_raw(),
        //       while PL/SQL use hextoraw(), here we use utl_raw.cast_to_raw(),
        //       as we can not distinguish in which SQL
        out_str = in_str;
        break;
      }
      case ObTimestampTZType:
      case ObTimestampLTZType:
      case ObTimestampNanoType: {
        // ObTimestampTZType is 12 bytes, ObTimestampLTZType and ObTimestampNanoType is 10 bytes
        // so it needs to be distinguished
        ObDatum tmp_datum;
        tmp_datum.ptr_ = in_str.ptr();
        tmp_datum.pack_ = static_cast<uint32_t>(in_str.length());
        if (OB_FAIL(common_construct_otimestamp(in_type, tmp_datum, time_in_val))) {
          LOG_WARN("common_construct_otimestamp failed", K(ret));
        } else {
          out_str.assign_ptr(reinterpret_cast<char*>(&time_in_val), static_cast<int32_t>(in_str.length()));
        }
        break;
      }
      default: {
        ret = OB_ERR_INVALID_HEX_NUMBER;
        LOG_WARN("invalid hex number", K(ret), K(in_str), "type", in_type);
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(hex(expr, out_str, ctx, tmp_alloc, res_datum))) {
        LOG_WARN("fail to convert to hex", K(ret), K(out_str));
      }
    }
  }
  return ret;
}

int ObDatumHexUtils::hextoraw_string(const ObExpr& expr, const ObString& in_str, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  if (0 == in_str.length()) {
    res_datum.set_null();
  } else if (OB_FAIL(unhex(expr, in_str, ctx, res_datum))) {
    LOG_WARN("unhex failed", K(ret), K(in_str));
  }
  return ret;
}

static int common_copy_string(
    const ObExpr& expr, const ObString& src, ObEvalCtx& ctx, ObDatum& res_datum, const int64_t align_offset = 0);
int ObDatumHexUtils::hextoraw(const ObExpr& expr, const ObDatum& in, const ObObjType& in_type,
    const ObCollationType& in_cs_type, ObEvalCtx& ctx, ObDatum& res)
{
  int ret = OB_SUCCESS;
  if (in.is_null()) {
    res.set_null();
  } else if (ob_is_numeric_type(in_type)) {
    number::ObNumber res_nmb;
    ObNumStackOnceAlloc tmp_alloc;
    if (OB_FAIL(get_uint(in_type, in, tmp_alloc, res_nmb))) {
      LOG_WARN("fail to get uint64", K(ret));
    } else if (OB_FAIL(uint_to_raw(res_nmb, expr, ctx, res))) {
      LOG_WARN("fail to convert to hex", K(ret), K(res_nmb));
    }
  } else if (ob_is_character_type(in_type, in_cs_type) || ob_is_varbinary_or_binary(in_type, in_cs_type)) {
    const ObString& in_str = in.get_string();
    if (OB_FAIL(hextoraw_string(expr, in_str, ctx, res))) {
      LOG_WARN("hextoraw_string failed", K(ret), K(in_str));
    }
  } else if (ob_is_raw(in_type)) {
    ObString in_str(in.get_string());
    if (OB_FAIL(common_copy_string(expr, in_str, ctx, res))) {
      LOG_WARN("common_copy_string failed", K(ret), K(in_str));
    }
  } else {
    ret = OB_ERR_INVALID_HEX_NUMBER;
    LOG_WARN("invalid hex number", K(ret), K(in_type));
  }
  return ret;
}

int ObDatumHexUtils::uint_to_raw(
    const number::ObNumber& uint_num, const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  const int64_t oracle_max_avail_len = 40;
  char uint_buf[number::ObNumber::MAX_TOTAL_SCALE] = {0};
  int64_t uint_pos = 0;
  ObString uint_str;
  if (OB_FAIL(uint_num.format(uint_buf, number::ObNumber::MAX_TOTAL_SCALE, uint_pos, 0))) {
    LOG_WARN("fail to format ", K(ret), K(uint_num));
  } else if (uint_pos > oracle_max_avail_len) {
    ret = OB_ERR_INVALID_HEX_NUMBER;
    LOG_WARN("invalid hex number", K(ret), K(uint_pos), K(oracle_max_avail_len), K(uint_num));
  } else {
    uint_str.assign_ptr(uint_buf, static_cast<int32_t>(uint_pos));
    if (OB_FAIL(unhex(expr, uint_str, ctx, res_datum))) {
      LOG_WARN("fail to str_to_raw", K(ret), K(uint_str));
    }
  }
  return ret;
}

int ObDatumHexUtils::get_uint(const ObObjType& in_type, const ObDatum& in, ObIAllocator& alloc, number::ObNumber& out)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ob_is_accurate_numeric_type(in_type))) {
    ret = OB_ERR_INVALID_HEX_NUMBER;
    LOG_WARN("invalid hex number", K(ret), K(in_type));
  } else if (ObNumberType == in_type || ObUNumberType == in_type) {
    const number::ObNumber value(in.get_number());
    if (OB_FAIL(out.from(value, alloc))) {
      LOG_WARN("deep copy failed", K(ret), K(value));
    } else if (OB_UNLIKELY(!out.is_integer()) || OB_UNLIKELY(out.is_negative())) {
      ret = OB_ERR_INVALID_HEX_NUMBER;
      LOG_WARN("invalid hex number", K(ret), K(out));
    } else if (OB_FAIL(out.round(0))) {
      LOG_WARN("round failed", K(ret), K(out));
    }
  } else {
    if (OB_UNLIKELY(in.get_int() < 0)) {
      ret = OB_ERR_INVALID_HEX_NUMBER;
      LOG_WARN("invalid hex number", K(ret), K(in.get_int()));
    } else if (OB_FAIL(out.from(in.get_int(), alloc))) {
      LOG_WARN("deep copy failed", K(ret), K(in.get_int()));
    }
  }
  return ret;
}

// %align_offset is used in mysql mode to align blob to other charset.
static int common_copy_string(
    const ObExpr& expr, const ObString& src, ObEvalCtx& ctx, ObDatum& res_datum, const int64_t align_offset /* = 0 */)
{
  int ret = OB_SUCCESS;
  char* out_ptr = NULL;
  int64_t len = align_offset + src.length();
  if (expr.res_buf_len_ < src.length()) {
    if (OB_ISNULL(out_ptr = expr.get_str_res_mem(ctx, len))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    }
  } else {
    out_ptr = const_cast<char*>(res_datum.ptr_);
  }

  if (OB_SUCC(ret)) {
    MEMMOVE(out_ptr + align_offset, src.ptr(), len - align_offset);
    MEMSET(out_ptr, 0, align_offset);
    res_datum.set_string(out_ptr, len);
  }
  return ret;
}

static int common_copy_string_zf(
    const ObExpr& expr, const ObString& src, ObEvalCtx& ctx, ObDatum& res_datum, const int64_t align_offset = 0)
{
  int ret = OB_SUCCESS;
  int64_t out_len = static_cast<uint8_t>(expr.datum_meta_.scale_);
  if (out_len <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected zf length", K(ret), K(out_len), K(expr.datum_meta_.scale_));
  } else if (CM_IS_ZERO_FILL(expr.extra_) && out_len > src.length()) {
    char* out_ptr = NULL;
    // out_ptr may overlap with src, so memmove is used.
    if (expr.res_buf_len_ < out_len) {
      if (OB_ISNULL(out_ptr = expr.get_str_res_mem(ctx, out_len))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      }
    } else {
      out_ptr = const_cast<char*>(res_datum.ptr_);
    }

    if (OB_SUCC(ret)) {
      int64_t zf_len = out_len - src.length();
      if (0 < zf_len) {
        MEMMOVE(out_ptr + zf_len, src.ptr(), src.length());
        MEMSET(out_ptr, '0', zf_len);
      } else {
        MEMMOVE(out_ptr, src.ptr(), out_len);
      }
      res_datum.set_string(ObString(out_len, out_ptr));
    }
  } else {
    if (OB_FAIL(common_copy_string(expr, src, ctx, res_datum, align_offset))) {
      LOG_WARN("common_copy_string failed", K(ret), K(src), K(expr));
    }
  }
  return ret;
}

int ObDatumHexUtils::unhex(const ObExpr& expr, const ObString& in_str, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  char* buf = NULL;
  const bool need_fill_zero = (1 == in_str.length() % 2);
  const int32_t tmp_length = in_str.length() / 2 + need_fill_zero;
  int32_t alloc_length = (0 == tmp_length ? 1 : tmp_length);
  if (OB_ISNULL(buf = expr.get_str_res_mem(ctx, alloc_length))) {
    res_datum.set_null();
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory failed", K(alloc_length), K(ret));
  } else {
    int32_t i = 0;
    char c1 = 0;
    char c2 = 0;
    if (in_str.length() > 0) {
      if (need_fill_zero) {
        c1 = '0';
        c2 = in_str[0];
        i = 0;
      } else {
        c1 = in_str[0];
        c2 = in_str[1];
        i = 1;
      }
    }
    while (OB_SUCC(ret) && i < in_str.length()) {
      if (isxdigit(c1) && isxdigit(c2)) {
        buf[i / 2] = (char)((get_xdigit(c1) << 4) | get_xdigit(c2));
        if (i + 2 < in_str.length()) {
          c1 = in_str[++i];
          c2 = in_str[++i];
        } else {
          break;
        }
      } else {
        ret = OB_ERR_INVALID_HEX_NUMBER;
        LOG_WARN("invalid hex number", K(ret), K(c1), K(c2), K(in_str));
      }
    }
    if (OB_SUCC(ret)) {
      ObString str_res(tmp_length, buf);
      // There will be no zero fill in the unhex() function, so it is directly assigned here
      res_datum.pack_ = tmp_length;
      res_datum.ptr_ = buf;
    }
  }
  return ret;
}

static int common_get_nls_format(const ObBasicSessionInfo* session, const ObObjType in_type,
    const bool force_use_standard_format, ObString& format_str)
{
  int ret = OB_SUCCESS;
  const ObString* session_nls_formats = NULL;
  if (OB_ISNULL(session)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("session is NULL", K(ret));
  } else {
    session_nls_formats = session->get_local_nls_formats();
    if (OB_ISNULL(session_nls_formats)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("nls_formats is NULL", K(ret));
    }
  }

  if (OB_LIKELY(OB_SUCC(ret))) {
    switch (in_type) {
      case ObDateTimeType:
        format_str = (force_use_standard_format ? ObTimeConverter::COMPAT_OLD_NLS_DATE_FORMAT
                                                : (session_nls_formats[ObNLSFormatEnum::NLS_DATE].empty()
                                                          ? ObTimeConverter::DEFAULT_NLS_DATE_FORMAT
                                                          : session_nls_formats[ObNLSFormatEnum::NLS_DATE]));
        break;
      case ObTimestampNanoType:
      case ObTimestampLTZType:
        format_str = (force_use_standard_format ? ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_FORMAT
                                                : (session_nls_formats[ObNLSFormatEnum::NLS_TIMESTAMP].empty()
                                                          ? ObTimeConverter::DEFAULT_NLS_TIMESTAMP_FORMAT
                                                          : session_nls_formats[ObNLSFormatEnum::NLS_TIMESTAMP]));
        break;
      case ObTimestampTZType:
        format_str = (force_use_standard_format ? ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_TZ_FORMAT
                                                : (session_nls_formats[ObNLSFormatEnum::NLS_TIMESTAMP_TZ].empty()
                                                          ? ObTimeConverter::DEFAULT_NLS_TIMESTAMP_TZ_FORMAT
                                                          : session_nls_formats[ObNLSFormatEnum::NLS_TIMESTAMP_TZ]));
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected in_type", K(in_type), K(ret));
        break;
    }
  }
  return ret;
}

static int common_int_datetime(const ObExpr& expr, const int64_t in_val, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  GET_SESSION()
  {
    int64_t out_val = 0;
    int warning = OB_SUCCESS;
    if (0 > in_val) {
      ret = OB_INVALID_DATE_FORMAT;
    } else {
      ObTimeConvertCtx cvrt_ctx(session->get_timezone_info(), ObTimestampType == expr.datum_meta_.type_);
      ret = ObTimeConverter::int_to_datetime(in_val, 0, cvrt_ctx, out_val);
    }
    if (CAST_FAIL(ret)) {
      LOG_WARN("int_datetime failed", K(ret));
    } else {
      SET_RES_DATETIME(out_val);
    }
  }
  return ret;
}

static OB_INLINE int common_int_number(const ObExpr& expr, int64_t in_val, ObIAllocator& alloc, number::ObNumber& nmb)
{
  int ret = OB_SUCCESS;
  int warning = OB_SUCCESS;
  ObObjType out_type = expr.datum_meta_.type_;
  if ((ObUNumberType == out_type) && CAST_FAIL(numeric_negative_check(in_val))) {
    LOG_WARN("numeric_negative_check faield", K(ret), K(in_val));
  } else if (nmb.from(in_val, alloc)) {
    LOG_WARN("nmb.from failed", K(ret), K(in_val));
  }
  return ret;
}

static OB_INLINE int common_int_date(const ObExpr& expr, const int64_t in_val, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  int warning = OB_SUCCESS;
  int32_t out_val = 0;
  if (CAST_FAIL(ObTimeConverter::int_to_date(in_val, out_val))) {
    LOG_WARN("int_to_date failed", K(ret), K(in_val), K(out_val));
  } else {
    SET_RES_DATE(out_val);
  }
  return ret;
}

static OB_INLINE int common_int_time(const ObExpr& expr, const int64_t in_val, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  int warning = OB_SUCCESS;
  int64_t out_val = 0;
  if (CAST_FAIL(ObTimeConverter::int_to_time(in_val, out_val))) {
    LOG_WARN("int_to_date failed", K(ret), K(in_val), K(out_val));
  } else {
    SET_RES_TIME(out_val);
  }
  return ret;
}

static OB_INLINE int common_int_year(const ObExpr& expr, const int64_t in_val, ObDatum& res_datum, int& warning)
{
  int ret = OB_SUCCESS;
  uint8_t out_val = 0;
  if (CAST_FAIL(ObTimeConverter::int_to_year(in_val, out_val))) {
    LOG_WARN("int_to_date failed", K(ret), K(in_val), K(out_val));
  } else {
    SET_RES_YEAR(out_val);
  }
  return ret;
}

static OB_INLINE int common_int_year(const ObExpr& expr, const int64_t in_val, ObDatum& res_datum)
{
  int warning = OB_SUCCESS;
  return common_int_year(expr, in_val, res_datum, warning);
}

static OB_INLINE int common_uint_int(
    const ObExpr& expr, const ObObjType& out_type, uint64_t in_val, ObEvalCtx& ctx, int64_t& out_val)
{
  int ret = OB_SUCCESS;
  int warning = OB_SUCCESS;
  out_val = static_cast<int64_t>(in_val);
  UNUSED(ctx);
  if (CM_NEED_RANGE_CHECK(expr.extra_) && CAST_FAIL(int_upper_check(out_type, in_val, out_val))) {
    LOG_WARN("int_upper_check failed", K(ret), K(in_val));
  }
  return ret;
}

static int common_string_int(const ObExpr& expr, const uint64_t& extra, const ObString& in_str,
    const bool is_str_integer_cast, ObDatum& res_datum, int& warning)
{
  int ret = OB_SUCCESS;
  ObObjType in_type = expr.args_[0]->datum_meta_.type_;
  ObObjType out_type = expr.datum_meta_.type_;
  int64_t out_val = 0;
  ret = common_string_integer(extra, in_type, in_str, is_str_integer_cast, out_val);
  if (CAST_FAIL_CM(ret, extra)) {
    LOG_WARN("string_int failed", K(ret));
  } else if (out_type < ObIntType && CAST_FAIL_CM(int_range_check(out_type, out_val, out_val), extra)) {
    LOG_WARN("int_range_check failed", K(ret));
  } else {
    SET_RES_INT(out_val);
  }
  return ret;
}

static int common_string_int(const ObExpr& expr, const uint64_t& extra, const ObString& in_str,
    const bool is_str_integer_cast, ObDatum& res_datum)
{
  int warning = OB_SUCCESS;
  return common_string_int(expr, extra, in_str, is_str_integer_cast, res_datum, warning);
}

static int common_string_uint(
    const ObExpr& expr, const ObString& in_str, const bool is_str_integer_cast, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  uint64_t out_val = 0;
  DEF_IN_OUT_TYPE();
  ret = common_string_unsigned_integer(expr.extra_, in_type, in_str, is_str_integer_cast, out_val);
  if (CAST_FAIL(ret)) {
    LOG_WARN("string_int failed", K(ret));
  } else if (out_type < ObUInt64Type && CM_NEED_RANGE_CHECK(expr.extra_) &&
             CAST_FAIL(uint_upper_check(out_type, out_val))) {
    LOG_WARN("uint_upper_check failed", K(ret));
  } else {
    SET_RES_UINT(out_val);
  }
  return ret;
}

int common_string_double(
    const ObExpr& expr, const ObObjType& in_type, const ObObjType& out_type, const ObString& in_str, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  int warning = OB_SUCCESS;
  double out_val = 0.0;
  if (ObHexStringType == in_type) {
    out_val = static_cast<double>(hex_to_uint64(in_str));
  } else {
    int err = 0;
    char* endptr = NULL;
    out_val = ObCharset::strntod(in_str.ptr(), in_str.length(), &endptr, &err);
    if (EOVERFLOW == err && (-DBL_MAX == out_val || DBL_MAX == out_val)) {
      ret = OB_DATA_OUT_OF_RANGE;
    } else {
      ObString tmp_str = in_str;
      ObString trimed_str = tmp_str.trim();
      if (lib::is_mysql_mode() && 0 == trimed_str.length()) {
        if (!CM_IS_COLUMN_CONVERT(expr.extra_)) {
          /* do nothing */
        } else {
          ret = OB_ERR_DOUBLE_TRUNCATED;
          LOG_WARN("convert string to double failed", K(ret), K(in_str));
        }
      } else if (OB_FAIL(check_convert_str_err(in_str.ptr(), endptr, in_str.length(), err))) {
        LOG_WARN("failed to check_convert_str_err", K(ret), K(in_str), K(out_val), K(err));
        ret = OB_ERR_DOUBLE_TRUNCATED;
        if (CM_IS_WARN_ON_FAIL(expr.extra_)) {
          LOG_USER_WARN(OB_ERR_DOUBLE_TRUNCATED, in_str.length(), in_str.ptr());
        }
      }
    }
  }

  if (CAST_FAIL(ret)) {
    LOG_WARN("string_double failed", K(ret));
  } else if (ObUDoubleType == out_type && CAST_FAIL(numeric_negative_check(out_val))) {
    LOG_WARN("numeric_negative_check failed", K(ret), K(out_val));
  } else {
    SET_RES_DOUBLE(out_val);
  }
  LOG_DEBUG("common_string_double", K(ret), K(warning), K(out_val), K(in_str));
  return ret;
}

static OB_INLINE int common_double_float(const ObExpr& expr, const double in_val, float& out_val)
{
  int ret = OB_SUCCESS;
  int warning = OB_SUCCESS;
  out_val = static_cast<float>(in_val);
  ObObjType out_type = expr.datum_meta_.type_;
  if (CAST_FAIL(real_range_check(out_type, in_val, out_val))) {
    LOG_WARN("real_range_check failed", K(ret), K(in_val));
  }
  return ret;
}

static OB_INLINE int common_string_float(const ObExpr& expr, const ObString& in_str, float& out_val)
{
  int ret = OB_SUCCESS;
  double tmp_double = 0.0;
  ObDatum tmp_datum;
  tmp_datum.ptr_ = reinterpret_cast<const char*>(&tmp_double);
  tmp_datum.pack_ = sizeof(double);
  DEF_IN_OUT_TYPE();
  if (OB_FAIL(common_string_double(expr, in_type, out_type, in_str, tmp_datum))) {
    LOG_WARN("common_string_double failed", K(ret), K(in_str));
  } else if (OB_FAIL(common_double_float(expr, tmp_double, out_val))) {
    LOG_WARN("common_double_float failed", K(ret), K(tmp_double));
  }
  return ret;
}

static OB_INLINE int common_string_date(const ObExpr& expr, const ObString& in_str, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  int warning = OB_SUCCESS;
  int32_t out_val = 0;
  if (CAST_FAIL(ObTimeConverter::str_to_date(in_str, out_val))) {
    LOG_WARN("str_to_date failed", K(ret), K(in_str));
  } else if (CM_IS_ERROR_ON_SCALE_OVER(expr.extra_) && out_val == ObTimeConverter::ZERO_DATE) {
    // check zero date for scale over mode
    ret = OB_INVALID_DATE_VALUE;
    LOG_USER_ERROR(OB_INVALID_DATE_VALUE, in_str.length(), in_str.ptr(), "");
  } else {
    SET_RES_DATE(out_val);
  }
  return ret;
}

static OB_INLINE int common_string_time(const ObExpr& expr, const ObString& in_str, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  int warning = OB_SUCCESS;
  int64_t out_val = 0;
  ObScale res_scale;  // useless
  if (CAST_FAIL(ObTimeConverter::str_to_time(in_str, out_val, &res_scale))) {
    LOG_WARN("str_to_time failed", K(ret), K(in_str));
  } else {
    SET_RES_TIME(out_val);
  }
  return ret;
}

static OB_INLINE int common_string_year(const ObExpr& expr, const ObString& in_str, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  int warning = OB_SUCCESS;
  const bool is_str_int_cast = false;
  // eg: insert into t1(col_year) values('201a');
  // will give out of range error by common_int_year(because in_val is 201, invalid year)
  // so need to unset warn on fail when do string_int cast.
  const uint64_t extra = CM_UNSET_STRING_INTEGER_TRUNC(CM_SET_WARN_ON_FAIL(expr.extra_));
  // datum size of year type is one byte!
  int64_t tmp_int = 0;
  ObDatum tmp_res;
  tmp_res.int_ = &tmp_int;
  tmp_res.pack_ = sizeof(tmp_int);
  if (OB_FAIL(common_string_int(expr, extra, in_str, is_str_int_cast, tmp_res, warning))) {
    LOG_WARN("common_string_int failed", K(ret), K(in_str));
  } else if (0 == tmp_int) {
		// cast '0000' to year, result is 0. cast '0'/'00'/'00000' to year, result is 2000.
    if (OB_SUCCESS != warning || 4 == in_str.length()) {
      SET_RES_YEAR(ObTimeConverter::ZERO_YEAR);
    } else {
      const uint8_t base_year = 100;
      SET_RES_YEAR(base_year);
    }
    CAST_FAIL(warning);
  } else {
    if (CAST_FAIL(common_int_year(expr, tmp_int, res_datum, warning))) {
      LOG_WARN("common_int_year failed", K(ret), K(tmp_int));
    } else {
      int tmp_warning = warning;
      CAST_FAIL(tmp_warning);
    }
  }
  return ret;
}

static OB_INLINE int common_string_number(
    const ObExpr& expr, const ObString& in_str, ObIAllocator& alloc, number::ObNumber& nmb)
{
  int ret = OB_SUCCESS;
  DEF_IN_OUT_TYPE();
  if (ObHexStringType == in_type) {
    ret = nmb.from(hex_to_uint64(in_str), alloc);
  } else if (0 == in_str.length()) {
    // in mysql mode, this err will be ignored(because default cast_mode is WARN_ON_FAIL)
    nmb.set_zero();
    ret = OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD;
  } else {
    ObPrecision res_precision;  // useless
    ObScale res_scale;
    ret = nmb.from_sci_opt(in_str.ptr(), in_str.length(), alloc, &res_precision, &res_scale);
    // select cast('1e500' as decimal);  -> max_val
    // select cast('-1e500' as decimal); -> min_val
    if (OB_NUMERIC_OVERFLOW == ret) {
      int64_t i = 0;
      while (i < in_str.length() && isspace(in_str[i])) {
        ++i;
      }
      bool is_neg = (in_str[i] == '-');
      int tmp_ret = OB_SUCCESS;
      const ObAccuracy& def_acc = ObAccuracy::DDL_DEFAULT_ACCURACY2[0][out_type];
      const ObPrecision prec = def_acc.get_precision();
      const ObScale scale = def_acc.get_scale();
      const number::ObNumber* bound_num = NULL;
      if (is_neg) {
        bound_num = &(ObNumberConstValue::MYSQL_MIN[prec][scale]);
      } else {
        bound_num = &(ObNumberConstValue::MYSQL_MAX[prec][scale]);
      }
      if (OB_ISNULL(bound_num)) {
        tmp_ret = OB_ERR_UNEXPECTED;
        LOG_WARN("bound_num is NULL", K(tmp_ret), K(ret), K(is_neg));
      } else if (OB_SUCCESS != (tmp_ret = nmb.from(*bound_num, alloc))) {
        LOG_WARN("copy min number failed", K(ret), K(tmp_ret), KPC(bound_num));
      }
    }
  }

  const ObCastMode cast_mode = expr.extra_;
  if (CAST_FAIL(ret)) {
    LOG_WARN("string_number failed", K(ret), K(in_type), K(out_type), K(cast_mode), K(in_str));
  } else if (ObUNumberType == out_type && CAST_FAIL(numeric_negative_check(nmb))) {
    LOG_WARN("numeric_negative_check failed", K(ret), K(in_type), K(cast_mode), K(in_str));
  }
  return ret;
}

static OB_INLINE int common_string_datetime(
    const ObExpr& expr, const ObString& in_str, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  int warning = OB_SUCCESS;
  int64_t out_val = 0;
  GET_SESSION()
  {
    ObTimeConvertCtx cvrt_ctx(session->get_timezone_info(), ObTimestampType == expr.datum_meta_.type_);
    if (lib::is_oracle_mode()) {
      if (OB_FAIL(common_get_nls_format(session,
              expr.datum_meta_.type_,
              CM_IS_FORCE_USE_STANDARD_NLS_FORMAT(expr.extra_),
              cvrt_ctx.oracle_nls_format_))) {
        LOG_WARN("common_get_nls_format failed", K(ret));
      } else if (CAST_FAIL(ObTimeConverter::str_to_date_oracle(in_str, cvrt_ctx, out_val))) {
        LOG_WARN("str_to_date_oracle failed", K(ret));
      }
    } else {
      ObScale res_scale;  // useless
      if (CAST_FAIL(ObTimeConverter::str_to_datetime(in_str, cvrt_ctx, out_val, &res_scale))) {
        LOG_WARN("str_to_datetime failed", K(ret), K(in_str));
      } else {
        // check zero date for scale over mode
        if (CM_IS_ERROR_ON_SCALE_OVER(expr.extra_) &&
            (out_val == ObTimeConverter::ZERO_DATE ||
            out_val == ObTimeConverter::ZERO_DATETIME)) {
          ret = OB_INVALID_DATE_VALUE;
          LOG_USER_ERROR(OB_INVALID_DATE_VALUE, in_str.length(), in_str.ptr(), "");
        }
      }
      LOG_INFO("stt, commont string to datetime", K(in_str), K(out_val), K(ret));
    }
    if (OB_SUCC(ret)) {
      SET_RES_DATETIME(out_val);
    }
  }
  return ret;
}

static OB_INLINE int common_get_bit_len(const ObString& str, int32_t& bit_len)
{
  int ret = OB_SUCCESS;
  if (str.empty()) {
    bit_len = 1;
  } else {
    const char* ptr = str.ptr();
    uint32_t uneven_value = reinterpret_cast<const unsigned char&>(ptr[0]);
    int32_t len = str.length();
    if (0 == uneven_value) {
      if (len > 8) {
        // Compatible with MySQL, if the length of bit string greater than 8 Bytes,
        // it would be considered too long. We set bit_len to OB_MAX_BIT_LENGTH + 1.
        bit_len = OB_MAX_BIT_LENGTH + 1;
      } else {
        bit_len = 1;
      }
    } else {
      // Built-in Function: int __builtin_clz (unsigned int x).
      // Returns the number of leading 0-bits in x, starting at the most significant bit position.
      // If x is 0, the result is undefined.
      int32_t uneven_len = static_cast<int32_t>(sizeof(unsigned int) * 8 - __builtin_clz(uneven_value));
      bit_len = uneven_len + 8 * (len - 1);
    }
  }
  return ret;
}

static int common_string_bit(const ObExpr& expr, const ObString& in_str, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  int warning = OB_SUCCESS;
  int32_t bit_len = 0;
  if (OB_FAIL(common_get_bit_len(in_str, bit_len))) {
    LOG_WARN("common_get_bit_len failed", K(ret));
  } else {
    ObObjType in_type = expr.args_[0]->datum_meta_.type_;
    uint64_t out_val = 0;
    if (ObHexStringType == in_type && in_str.empty()) {
      ret = OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD;
      LOG_WARN("hex string is empty, can't cast to bit", K(ret), K(in_str));
    } else if (bit_len > OB_MAX_BIT_LENGTH) {
      out_val = UINT64_MAX;
      ret = OB_ERR_DATA_TOO_LONG;
      LOG_WARN("bit type length is too long", K(ret), K(in_str), K(OB_MAX_BIT_LENGTH), K(bit_len));
    } else {
      out_val = hex_to_uint64(in_str);
    }
    if (OB_FAIL(ret) && CM_IS_WARN_ON_FAIL(expr.extra_)) {
      warning = OB_DATA_OUT_OF_RANGE;
      ret = OB_SUCCESS;
    }
    if (OB_SUCC(ret)) {
      SET_RES_BIT(out_val);
    }
  }
  return ret;
}

// Copy the result from get_str_res_mem to get_reset_tmp_alloc space
int copy_datum_str_with_tmp_alloc(ObEvalCtx& ctx, ObDatum& res_datum, ObString& res_str)
{
  int ret = OB_SUCCESS;
  ObIAllocator& calc_alloc = ctx.get_reset_tmp_alloc();
  char* tmp_res = NULL;
  const ObString str = res_datum.get_string();
  if (0 == str.length()) {
    res_str.assign_ptr(NULL, 0);
  } else if (OB_ISNULL(tmp_res = static_cast<char*>(calc_alloc.alloc(str.length())))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else {
    MEMMOVE(tmp_res, str.ptr(), str.length());
    res_str.assign_ptr(tmp_res, str.length());
  }
  return ret;
}

int common_check_convert_string(const ObExpr& expr, ObEvalCtx& ctx, const ObString& in_str, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  ObObjType out_type = expr.datum_meta_.type_;
  ObCollationType out_cs_type = expr.datum_meta_.cs_type_;
  ObObjType in_type = expr.args_[0]->datum_meta_.type_;
  ObCollationType in_cs_type = expr.args_[0]->datum_meta_.cs_type_;
  if (lib::is_oracle_mode() && (ob_is_blob(out_type, out_cs_type) || ob_is_blob_locator(out_type, out_cs_type)) &&
      !(ob_is_blob(in_type, in_cs_type) || ob_is_blob_locator(in_type, in_cs_type))) {
    // !blob -> blob
    if (ObCharType == in_type || ObVarcharType == in_type) {
      if (OB_FAIL(ObDatumHexUtils::hextoraw_string(expr, in_str, ctx, res_datum))) {
        LOG_WARN("fail to hextoraw_string for blob", K(ret), K(in_str));
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_ERROR("invalid use of blob type", K(ret), K(in_str), K(out_type));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "cast to blob type");
    }
  } else {
    // When convert blob/binary/varbinary to other charset, need to align to mbminlen of destination charset
    // by add '\0' prefix in mysql mode. (see mysql String::copy)
    const ObCharsetInfo* cs = NULL;
    int64_t align_offset = 0;
    if (CS_TYPE_BINARY == in_cs_type && lib::is_mysql_mode() && (NULL != (cs = ObCharset::get_charset(out_cs_type)))) {
      if (cs->mbminlen > 0 && in_str.length() % cs->mbminlen != 0) {
        align_offset = cs->mbminlen - in_str.length() % cs->mbminlen;
      }
    }
    if (OB_FAIL(common_copy_string_zf(expr, in_str, ctx, res_datum, align_offset))) {
      LOG_WARN("common_copy_string_zf failed", K(ret), K(in_str));
    }
  }
  return ret;
}

static int common_string_string(const ObExpr& expr, const ObObjType in_type, const ObCollationType in_cs_type,
    const ObObjType out_type, const ObCollationType out_cs_type, const ObString& in_str, ObEvalCtx& ctx,
    ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  if (lib::is_oracle_mode() && ob_is_clob(in_type, in_cs_type) && (0 == in_str.length()) &&
      !ob_is_clob(out_type, out_cs_type)) {
    // When empty_clob in oracle mode is cast to other types, the result is NULL
    res_datum.set_null();
  } else if (CS_TYPE_BINARY != in_cs_type && CS_TYPE_BINARY != out_cs_type &&
             (ObCharset::charset_type_by_coll(in_cs_type) != ObCharset::charset_type_by_coll(out_cs_type))) {
    // handle !blob->!blob
    char* buf = NULL;
    const int64_t factor = 2;
    int64_t buf_len = in_str.length() * factor;
    uint32_t result_len = 0;
    buf = expr.get_str_res_mem(ctx, buf_len);
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed", K(ret));
    } else if (OB_FAIL(ObCharset::charset_convert(
                   in_cs_type, in_str.ptr(), in_str.length(), out_cs_type, buf, buf_len, result_len))) {
      if (CM_IS_IGNORE_CHARSET_CONVERT_ERR(expr.extra_)) {
        int32_t str_offset = 0;
        int64_t buf_offset = 0;
        while (str_offset < in_str.length() && buf_offset < buf_len) {
          int64_t offset = ObCharset::charpos(in_cs_type, in_str.ptr() + str_offset, in_str.length() - str_offset, 1);
          ret = ObCharset::charset_convert(in_cs_type,
              in_str.ptr() + str_offset,
              offset,
              out_cs_type,
              buf + buf_offset,
              buf_len - buf_offset,
              result_len);
          str_offset += offset;
          if (OB_SUCCESS == ret) {
            buf_offset += result_len;
          } else {
            buf[buf_offset] = '?';
            buf_offset += 1;
          }
        }
        if (buf_offset > buf_len) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("buf_offset > buf_len, unexpected", K(ret));
        } else {
          result_len = buf_offset;
          ret = OB_SUCCESS;
          LOG_WARN("charset convert failed", K(ret), K(in_cs_type), K(out_cs_type));
          res_datum.set_string(buf, result_len);
        }
      }
    } else {
      res_datum.set_string(buf, result_len);
    }
  } else {
    if (CS_TYPE_BINARY == in_cs_type || CS_TYPE_BINARY == out_cs_type) {
      // just copy string when in_cs_type or out_cs_type is binary
      if (OB_FAIL(common_check_convert_string(expr, ctx, in_str, res_datum))) {
        LOG_WARN("fail to common_check_convert_string", K(ret), K(in_str));
      }
    } else if (lib::is_oracle_mode() && ob_is_clob(in_type, in_cs_type)) {
      res_datum.set_string(in_str.ptr(), in_str.length());
    } else if (lib::is_oracle_mode() && ob_is_clob(out_type, out_cs_type)) {
      res_datum.set_string(in_str.ptr(), in_str.length());
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("same charset should not be here, just use cast_eval_arg",
          K(ret),
          K(in_type),
          K(out_type),
          K(in_cs_type),
          K(out_cs_type));
    }
  }
  LOG_DEBUG("string_string cast", K(ret), K(in_str), K(ObString(res_datum.len_, res_datum.ptr_)));
  return ret;
}

static int common_string_otimestamp(const ObExpr& expr, const ObString& in_str, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  GET_SESSION()
  {
    int warning = OB_SUCCESS;
    ObOTimestampData out_val;
    ObTimeConvertCtx cvrt_ctx(session->get_timezone_info(), true);
    ObScale res_scale = 0;  // useless
    if (OB_FAIL(common_get_nls_format(session,
            expr.datum_meta_.type_,
            CM_IS_FORCE_USE_STANDARD_NLS_FORMAT(expr.extra_),
            cvrt_ctx.oracle_nls_format_))) {
      LOG_WARN("common_get_nls_format failed", K(ret));
    } else if (CAST_FAIL(
                   ObTimeConverter::str_to_otimestamp(in_str, cvrt_ctx, expr.datum_meta_.type_, out_val, res_scale))) {
      LOG_WARN("str_to_otimestamp failed", K(ret), K(in_str));
    } else {
      if (ObTimestampTZType == expr.datum_meta_.type_) {
        SET_RES_OTIMESTAMP(out_val);
      } else {
        SET_RES_OTIMESTAMP_10BYTE(out_val);
      }
    }
  }
  return ret;
}

static int common_string_interval(const ObExpr& expr, const ObString& in_str, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  int warning = OB_SUCCESS;
  ObObjType out_type = expr.datum_meta_.type_;
  if (ObIntervalYMType == out_type) {
    ObIntervalYMValue out_val;
    // don't know the scale, use the max scale and return the real scale
    ObScale res_scale = ObAccuracy::MAX_ACCURACY2[ORACLE_MODE][ObIntervalYMType].get_scale();
    if (CAST_FAIL(ObTimeConverter::str_to_interval_ym(in_str, out_val, res_scale))) {
      LOG_WARN("str_to_interval_ym failed", K(ret), K(in_str));
    } else {
      SET_RES_INTERVAL_YM(out_val.nmonth_);
    }
  } else {
    ObIntervalDSValue out_val;
    // don't know the scale, use the max scale and return the real scale
    ObScale res_scale = ObAccuracy::MAX_ACCURACY2[ORACLE_MODE][ObIntervalDSType].get_scale();
    if (CAST_FAIL(ObTimeConverter::str_to_interval_ds(in_str, out_val, res_scale))) {
      LOG_WARN("str_to_interval_ds failed", K(ret), K(in_str));
    } else {
      SET_RES_INTERVAL_DS(out_val);
    }
  }
  return ret;
}

static int common_string_rowid(const ObExpr& expr, const ObString& base64_str, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  int64_t decoded_buf_size = ObURowIDData::needed_urowid_buf_size(base64_str.length());
  char* decoded_buf = NULL;
  ObDataBuffer data_alloc;
  ObURowIDData urowid_data;
  if (OB_ISNULL(decoded_buf = expr.get_str_res_mem(ctx, decoded_buf_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else {
    data_alloc.set_data(decoded_buf, decoded_buf_size);
    if (OB_FAIL(ObURowIDData::decode2urowid(base64_str.ptr(), base64_str.length(), data_alloc, urowid_data))) {
      LOG_WARN("failed to decode to urowid", K(ret));
    } else {
      res_datum.set_urowid(urowid_data);
    }
  }
  return ret;
}

static int common_string_lob(
    const ObExpr& expr, const ObString& in_str, ObEvalCtx& ctx, const ObLobLocator* lob_locator, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  ObObjType in_type = expr.args_[0]->datum_meta_.type_;
  ObObjType out_type = ObLongTextType;
  ObCollationType in_cs_type = expr.args_[0]->datum_meta_.cs_type_;
  ObCollationType out_cs_type = expr.datum_meta_.cs_type_;

  if (OB_FAIL(common_string_string(expr, in_type, in_cs_type, out_type, out_cs_type, in_str, ctx, res_datum))) {
    LOG_WARN("fail to cast string to longtext", K(ret), K(in_str), K(expr));
  } else if (res_datum.is_null()) {
    // do nothing
  } else {
    ObString res_str;
    if (OB_FAIL(copy_datum_str_with_tmp_alloc(ctx, res_datum, res_str))) {
      LOG_WARN("copy datum str with tmp alloc", K(ret));
    } else {
      char* buf = nullptr;
      ObLobLocator* result = nullptr;
      const int64_t buf_len =
          sizeof(ObLobLocator) + (NULL == lob_locator ? 0 : lob_locator->payload_offset_) + res_str.length();
      if (OB_ISNULL(buf = expr.get_str_res_mem(ctx, buf_len))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Failed to allocate memory for lob locator", K(ret), K(buf_len));
      } else if (FALSE_IT(result = reinterpret_cast<ObLobLocator*>(buf))) {
      } else if (NULL == lob_locator) {
        if (OB_FAIL(result->init(res_str))) {
          STORAGE_LOG(WARN, "Failed to init lob locator", K(ret), K(res_str), KPC(result));
        }
      } else if (NULL != lob_locator) {
        ObString rowid;
        if (OB_FAIL(lob_locator->get_rowid(rowid))) {
          LOG_WARN("get rowid failed", K(ret));
        } else if (OB_FAIL(result->init(lob_locator->table_id_,
                       lob_locator->column_id_,
                       lob_locator->snapshot_version_,
                       lob_locator->flags_,
                       rowid,
                       res_str))) {
          STORAGE_LOG(WARN, "Failed to init lob locator", K(ret), K(res_str), KPC(result));
        }
      }
      if (OB_SUCC(ret)) {
        res_datum.set_lob_locator(*result);
      }
    }
  }
  return ret;
}

static int common_uint_bit(const ObExpr& expr, const uint64_t& in_value, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  int warning = OB_SUCCESS;
  UNUSED(expr);
  UNUSED(ctx);
  int32_t bit_len = 0;
  if (OB_FAIL(get_bit_len(in_value, bit_len))) {
    LOG_WARN("fail to get bit len", K(ret), K(in_value), K(bit_len));
  } else {
    uint64_t out_val = in_value;
    SET_RES_BIT(out_val);
  }
  return ret;
}

static OB_INLINE int common_number_uint(const ObExpr& expr, const ObDatum& child_res, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  int warning = OB_SUCCESS;
  const number::ObNumber nmb(child_res.get_number());
  const char* nmb_buf = nmb.format();
  if (OB_ISNULL(nmb_buf)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("nmb_buf is NULL", K(ret));
  } else {
    ObString num_str(strlen(nmb_buf), nmb_buf);
    const bool is_str_int_cast = false;
    if (OB_FAIL(common_string_uint(expr, num_str, is_str_int_cast, res_datum))) {
      LOG_WARN("common_string_uint failed", K(ret), K(num_str));
    }
  }
  return ret;
}

static OB_INLINE int common_number_string(
    const ObExpr& expr, const ObDatum& child_res, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  const number::ObNumber nmb(child_res.get_number());
  char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
  ObScale in_scale = expr.args_[0]->datum_meta_.scale_;
  int64_t len = 0;
  if (lib::is_oracle_mode() && CM_IS_FORMAT_NUMBER_WITH_LIMIT(expr.extra_)) {
    if (OB_FAIL(nmb.format_with_oracle_limit(buf, sizeof(buf), len, in_scale))) {
      LOG_WARN("fail to format", K(ret), K(nmb));
    }
  } else {
    if (OB_FAIL(nmb.format(buf, sizeof(buf), len, in_scale))) {
      LOG_WARN("fail to format", K(ret), K(nmb));
    }
  }

  if (OB_SUCC(ret)) {
    ObString in_str(len, buf);
    if (OB_FAIL(common_check_convert_string(expr, ctx, in_str, res_datum))) {
      LOG_WARN("fail to common_check_convert_string", K(ret), K(in_str));
    }
  }
  return ret;
}

static const double ROUND_DOUBLE = 0.5;
template <typename IN_TYPE, typename OUT_TYPE>
static OB_INLINE int common_floating_int(IN_TYPE& in_val, OUT_TYPE& out_val)
{
  int ret = OB_SUCCESS;
  out_val = 0;
  if (in_val < 0) {
    out_val = static_cast<OUT_TYPE>(in_val - ROUND_DOUBLE);
  } else if (in_val > 0) {
    out_val = static_cast<OUT_TYPE>(in_val + ROUND_DOUBLE);
  } else {
    out_val = static_cast<OUT_TYPE>(in_val);
  }
  return ret;
}

template <typename IN_TYPE>
static int common_floating_number(
    const IN_TYPE in_val, const ob_gcvt_arg_type arg_type, ObIAllocator& alloc, number::ObNumber& number)
{
  int ret = OB_SUCCESS;
  char buf[MAX_DOUBLE_STRICT_PRINT_SIZE];
  MEMSET(buf, 0, MAX_DOUBLE_STRICT_PRINT_SIZE);
  int64_t length = 0;
  if (lib::is_oracle_mode() || OB_GCVT_ARG_DOUBLE == arg_type) {
    length = ob_gcvt_opt(in_val, arg_type, sizeof(buf) - 1, buf, NULL, lib::is_oracle_mode());
  } else {
    length = ob_gcvt(in_val, OB_GCVT_ARG_DOUBLE, sizeof(buf) - 1, buf, NULL);
  }
  ObString str(sizeof(buf), static_cast<int32_t>(length), buf);
  ObScale res_scale;  // useless
  ObPrecision res_precision;
  if (OB_FAIL(number.from_sci_opt(str.ptr(), str.length(), alloc, &res_precision, &res_scale))) {
    LOG_WARN("fail to from str to number", K(ret), K(str));
  }
  return ret;
}

template <typename IN_TYPE>
static int common_floating_string(
    const ObExpr& expr, const IN_TYPE in_val, const ob_gcvt_arg_type arg_type, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
  int64_t length = 0;
  ObScale scale = expr.args_[0]->datum_meta_.scale_;
  if (0 <= scale) {
    length = ob_fcvt(in_val, scale, sizeof(buf) - 1, buf, NULL);
  } else {
    length = ob_gcvt_opt(in_val, arg_type, sizeof(buf) - 1, buf, NULL, lib::is_oracle_mode());
  }
  ObString in_str(sizeof(buf), static_cast<int32_t>(length), buf);
  if (OB_FAIL(common_check_convert_string(expr, ctx, in_str, res_datum))) {
    LOG_WARN("fail to common_check_convert_string", K(ret), K(in_str));
  }
  return ret;
}

static int common_number_datetime(const number::ObNumber nmb,
                                  const ObTimeConvertCtx &cvrt_ctx, int64_t &out_val,
                                  const ObCastMode cast_mode);

static OB_INLINE int common_double_datetime(
    const ObExpr& expr, const double val_double, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  int warning = OB_SUCCESS;
  ObBasicSessionInfo* session = ctx.exec_ctx_.get_my_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else {
    int64_t out_val = 0;
    ObObjType out_type = expr.datum_meta_.type_;
    ObTimeConvertCtx cvrt_ctx(session->get_timezone_info(), ObTimestampType == out_type);
    ObNumStackOnceAlloc tmp_alloc;
    number::ObNumber number;
    if (OB_FAIL(common_floating_number(val_double, OB_GCVT_ARG_DOUBLE, tmp_alloc, number))) {
      LOG_WARN("cast float to number failed", K(ret), K(expr.extra_));
      if (CM_IS_WARN_ON_FAIL(expr.extra_)) {
        ret = OB_SUCCESS;
        if (CM_IS_ZERO_ON_WARN(expr.extra_)) {
          res_datum.set_datetime(ObTimeConverter::ZERO_DATETIME);
        } else {
          res_datum.set_null();
        }
      } else {
        ret = OB_INVALID_DATE_VALUE;
      }
    } else {
      ret = common_number_datetime(number, cvrt_ctx, out_val, expr.extra_);
      if (CAST_FAIL(ret)) {
        LOG_WARN("str_to_datetime failed", K(ret));
      } else {
        SET_RES_DATETIME(out_val);
      }
    }
  }
  return ret;
}

static int common_double_time(const ObExpr& expr, const double val_double, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  int warning = OB_SUCCESS;
  int64_t out_val = 0;
  char buf[MAX_DOUBLE_PRINT_SIZE];
  MEMSET(buf, 0, MAX_DOUBLE_PRINT_SIZE);
  int64_t length = ob_gcvt(val_double, OB_GCVT_ARG_DOUBLE, sizeof(buf) - 1, buf, NULL);
  ObString str(sizeof(buf), static_cast<int32_t>(length), buf);
  ObScale res_scale;
  if (CAST_FAIL(ObTimeConverter::str_to_time(str, out_val, &res_scale))) {
    LOG_WARN("str_to_time failed", K(ret));
  } else {
    SET_RES_TIME(out_val);
  }
  return ret;
}

int common_datetime_string(const ObObjType in_type, const ObObjType out_type, const ObScale in_scale,
    bool force_use_std_nls_format, const int64_t in_val, ObEvalCtx& ctx, char* buf, int64_t buf_len, int64_t& out_len)
{
  int ret = OB_SUCCESS;
  UNUSED(out_type);
  ObBasicSessionInfo* session = ctx.exec_ctx_.get_my_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else {
    const ObTimeZoneInfo* tz_info = (ObTimestampType == in_type) ? session->get_timezone_info() : NULL;
    ObString nls_format;
    if (lib::is_oracle_mode() && !force_use_std_nls_format) {
      if (OB_FAIL(common_get_nls_format(session, in_type, force_use_std_nls_format, nls_format))) {
        LOG_WARN("common_get_nls_format failed", K(ret));
      }
    }
    if (OB_SUCC(ret) &&
        OB_FAIL(ObTimeConverter::datetime_to_str(in_val, tz_info, nls_format, in_scale, buf, buf_len, out_len))) {
      LOG_WARN("failed to convert datetime to string",
          K(ret),
          K(in_val),
          KP(tz_info),
          K(nls_format),
          K(in_scale),
          K(buf),
          K(out_len));
    }
  }
  return ret;
}

static int common_year_int(const ObExpr& expr, const ObObjType& out_type, const uint8_t in_val, int64_t& out_val)
{
  int ret = OB_SUCCESS;
  int warning = OB_SUCCESS;
  if (OB_FAIL(ObTimeConverter::year_to_int(in_val, out_val))) {
    LOG_WARN("year_to_int failed", K(ret));
  } else if (out_type < ObSmallIntType && CAST_FAIL(int_range_check(out_type, out_val, out_val))) {
    LOG_WARN("int_range_check failed", K(ret));
  }
  return ret;
}

// When converting float to int, if it exceeds LLONG_MAX, it should be trunc to LLONG_MIN, and
// when it exceeds LLONG_MAX to int, it should be trunc to LLONG_MAX.
int common_double_int(const double in, int64_t& out, const int64_t trunc_min_value, const int64_t trunc_max_value)
{
  int ret = OB_SUCCESS;
  out = 0;
  if (in <= static_cast<double>(LLONG_MIN)) {
    out = trunc_min_value;
    if (in < static_cast<double>(LLONG_MIN)) {
      ret = OB_DATA_OUT_OF_RANGE;
    }
  } else if (in >= static_cast<double>(LLONG_MAX)) {
    // the case of equality into account, because the result of converting a floating-point
    // number equal to LLONG_MAX to int may be LLONG_MIN or LLONG_MAX. double to int, and float
    // to int when used as an insert value, the result is LLONG_MAX;
    // in other cases, the result of converting float to int is LLONG_MIN. The non-error is also
    // compatible with mysql
    out = trunc_max_value;
    if (in > static_cast<double>(LLONG_MAX)) {
      ret = OB_DATA_OUT_OF_RANGE;
    }
  } else {
    out = static_cast<int64_t>(rint(in));
  }
  return ret;
}

static OB_INLINE int common_construct_otimestamp(
    const ObObjType type, const ObDatum& in_datum, ObOTimestampData& out_val)
{
  int ret = OB_SUCCESS;
  if (ObTimestampTZType == type) {
    out_val = in_datum.get_otimestamp_tz();
  } else if (ObTimestampLTZType == type || ObTimestampNanoType == type) {
    out_val = in_datum.get_otimestamp_tiny();
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid in type", K(ret), K(type));
  }
  return ret;
}

static int common_number_datetime(const number::ObNumber nmb,
                                  const ObTimeConvertCtx &cvrt_ctx,
                                  int64_t &out_val,
                                  const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  int64_t int_part = 0;
  int64_t dec_part = 0;
  const int64_t three_digit_min = 100;
  const int64_t eight_digit_max = 99999999;
  if (nmb.is_negative()) {
    ret = OB_INVALID_DATE_VALUE;
    LOG_WARN("invalid datetime value", K(ret), K(nmb));
  } else if (!nmb.is_int_parts_valid_int64(int_part, dec_part)) {
    ret = OB_INVALID_DATE_VALUE;
    LOG_WARN("invalid date format", K(ret), K(nmb));
  // Maybe we need a new framework to make precise control on whether we report an error,
  // instead of calling a function and check the return value and cast_mode,
  // then we can move this logic to ObTimeConverter::int_to_datetime.
  } else if (OB_UNLIKELY(dec_part != 0
	              && ((0 == int_part && cvrt_ctx.is_timestamp_)
                  || (int_part >= three_digit_min && int_part <= eight_digit_max)))) {
    if (CM_IS_COLUMN_CONVERT(cast_mode) && !CM_IS_WARN_ON_FAIL(cast_mode)) {
      ret = OB_INVALID_DATE_VALUE;
      LOG_WARN("invalid date value", K(ret), K(nmb));
    } else {
      dec_part = 0;
    }
  }
  if (OB_SUCC(ret)) {
    ret = ObTimeConverter::int_to_datetime(int_part, dec_part, cvrt_ctx, out_val);
  }
  return ret;
}

int cast_not_expected(const sql::ObExpr& expr, sql::ObEvalCtx& ctx, sql::ObDatum& res_datum)
{
  int ret = OB_ERR_UNEXPECTED;
  UNUSED(ctx);
  UNUSED(res_datum);
  ObObjType in_type = expr.args_[0]->datum_meta_.type_;
  ObObjType out_type = expr.datum_meta_.type_;
  LOG_WARN("cast_not_expected", K(ret), K(in_type), K(out_type), K(expr.extra_));
  return ret;
}

int cast_not_support(const sql::ObExpr& expr, sql::ObEvalCtx& ctx, sql::ObDatum& res_datum)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(ctx);
  UNUSED(res_datum);
  ObObjType in_type = expr.args_[0]->datum_meta_.type_;
  ObObjType out_type = expr.datum_meta_.type_;
  LOG_WARN("cast_not_supported", K(ret), K(in_type), K(out_type), K(expr.extra_));
  return ret;
}

int cast_inconsistent_types(const sql::ObExpr& expr, sql::ObEvalCtx& ctx, sql::ObDatum& res_datum)
{
  int ret = OB_ERR_INVALID_TYPE_FOR_OP;
  UNUSED(ctx);
  UNUSED(res_datum);
  ObObjType in_type = expr.args_[0]->datum_meta_.type_;
  ObObjType out_type = expr.datum_meta_.type_;
  LOG_WARN("inconsistent datatypes", K(ret), K(in_type), K(out_type), K(expr.extra_));
  return ret;
}

int cast_identity_enum_set(const sql::ObExpr& expr, const ObIArray<ObString>& str_values, const uint64_t cast_mode,
    sql::ObEvalCtx& ctx, sql::ObDatum& res_datum)
{
  UNUSED(cast_mode);
  UNUSED(str_values);
  EVAL_ARG()
  {
    res_datum.set_enum(child_res->get_enum());
  }
  return OB_SUCCESS;
}

int cast_not_support_enum_set(const sql::ObExpr& expr, const ObIArray<ObString>& str_values, const uint64_t cast_mode,
    sql::ObEvalCtx& ctx, sql::ObDatum& res_datum)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(ctx);
  UNUSED(res_datum);
  ObObjType in_type = expr.args_[0]->datum_meta_.type_;
  ObObjType out_type = expr.datum_meta_.type_;
  LOG_WARN("not support datatypes", K(in_type), K(out_type), K(cast_mode), K(str_values), K(expr));
  return ret;
}

int cast_not_expected_enum_set(const sql::ObExpr& expr, const ObIArray<ObString>& str_values, const uint64_t cast_mode,
    sql::ObEvalCtx& ctx, sql::ObDatum& res_datum)
{
  int ret = OB_ERR_UNEXPECTED;
  UNUSED(ctx);
  UNUSED(res_datum);
  ObObjType in_type = expr.args_[0]->datum_meta_.type_;
  ObObjType out_type = expr.datum_meta_.type_;
  LOG_WARN(
      "not expected obj type convert", K(ret), K(in_type), K(out_type), K(expr), K(cast_mode), K(str_values), K(lbt()));
  return ret;
}

CAST_FUNC_NAME(unknown, other)
{
  return cast_not_support(expr, ctx, res_datum);
}

// Some casts have no actual logic, only the value of the child node needs to be calculated
// for example, int -> bit, the cast result can directly use the result of the child node, no calculation is required
// Note: if you use this function for a new type of conversion, you must update this conversion in the is_trivial_cast()
// method to ensure that the res_datum pointer of the cast expression points to the parameter space!!!
int cast_eval_arg(const sql::ObExpr& expr, sql::ObEvalCtx& ctx, sql::ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  UNUSED(res_datum);
  ObDatum* child_res = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, child_res))) {
    LOG_WARN("eval arg failed", K(ret));
  } else {
    res_datum.set_datum(*child_res);
  }
  return ret;
}

CAST_FUNC_NAME(int, int)
{
  EVAL_ARG()
  {
    DEF_IN_OUT_TYPE();
    int64_t val_int = child_res->get_int();
    if (in_type > out_type && CAST_FAIL(int_range_check(out_type, val_int, val_int))) {
      LOG_WARN("int_range_check failed", K(ret), K(out_type), K(val_int));
    } else {
      res_datum.set_int(val_int);
    }
  }
  return ret;
}

CAST_FUNC_NAME(int, uint)
{
  EVAL_ARG()
  {
    if (CM_SKIP_CAST_INT_UINT(expr.extra_)) {
      LOG_DEBUG("skip cast int uint", K(ret));
    } else {
      ObObjType out_type = expr.datum_meta_.type_;
      DEF_IN_OUT_VAL(int64_t, uint64_t, static_cast<uint64_t>(in_val));
      if (CM_NEED_RANGE_CHECK(expr.extra_) && CAST_FAIL(uint_range_check(out_type, in_val, out_val))) {
      } else {
        res_datum.set_uint(out_val);
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(int, float)
{
  EVAL_ARG()
  {
    ObObjType out_type = expr.datum_meta_.type_;
    DEF_IN_OUT_VAL(int64_t, float, static_cast<float>(static_cast<double>(in_val)));
    if (ObUFloatType == out_type && CAST_FAIL(numeric_negative_check(out_val))) {
      LOG_WARN("numeric_negative_check failed", K(ret), K(in_val));
    } else {
      res_datum.set_float(out_val);
    }
  }
  return ret;
}

CAST_FUNC_NAME(int, double)
{
  EVAL_ARG()
  {
    ObObjType out_type = expr.datum_meta_.type_;
    DEF_IN_OUT_VAL(int64_t, double, static_cast<double>(in_val));
    if (ObUDoubleType == out_type && CAST_FAIL(numeric_negative_check(out_val))) {
      LOG_WARN("numeric_negative_check failed", K(ret), K(in_val), K(out_val));
    } else {
      res_datum.set_double(out_val);
    }
  }
  return ret;
}

CAST_FUNC_NAME(int, number)
{
  EVAL_ARG()
  {
    int64_t in_val = child_res->get_int();
    ObNumStackOnceAlloc tmp_alloc;
    number::ObNumber nmb;
    OZ(common_int_number(expr, in_val, tmp_alloc, nmb), in_val);
    OX(res_datum.set_number(nmb));
  }
  return ret;
}

CAST_FUNC_NAME(int, datetime)
{
  EVAL_ARG()
  {
    int64_t in_val = child_res->get_int();
    OZ(common_int_datetime(expr, in_val, ctx, res_datum), in_val);
  }
  return ret;
}

CAST_FUNC_NAME(int, date)
{
  EVAL_ARG()
  {
    int64_t in_val = child_res->get_int();
    OZ(common_int_date(expr, in_val, res_datum), in_val);
  }
  return ret;
}

CAST_FUNC_NAME(int, time)
{
  EVAL_ARG()
  {
    int64_t in_val = child_res->get_int();
    OZ(common_int_time(expr, in_val, res_datum), in_val);
  }
  return ret;
}

CAST_FUNC_NAME(int, year)
{
  EVAL_ARG()
  {
    int64_t in_val = child_res->get_int();
    OZ(common_int_year(expr, in_val, res_datum), in_val);
  }
  return ret;
}

CAST_FUNC_NAME(int, string)
{
  EVAL_ARG()
  {
    int64_t in_val = child_res->get_int();
    ObFastFormatInt ffi(in_val);
    if (OB_FAIL(common_copy_string_zf(expr, ObString(ffi.length(), ffi.ptr()), ctx, res_datum))) {
      LOG_WARN("common_copy_string_zf failed", K(ret));
    }
  }
  return ret;
}

CAST_FUNC_NAME(int, lob)
{
  EVAL_ARG()
  {
    int64_t in_val = child_res->get_int();
    ObFastFormatInt ffi(in_val);
    ObString res_str;
    if (OB_FAIL(common_copy_string_zf(expr, ObString(ffi.length(), ffi.ptr()), ctx, res_datum))) {
      LOG_WARN("common_copy_string_zf failed", K(ret));
    } else if (OB_FAIL(copy_datum_str_with_tmp_alloc(ctx, res_datum, res_str))) {
      LOG_WARN("copy datum string with tmp allocator failed", K(ret));
    } else if (OB_FAIL(common_string_lob(expr, res_str, ctx, NULL, res_datum))) {
      LOG_WARN("cast string to lob failed", K(ret));
    }
  }
  return ret;
}

CAST_FUNC_NAME(int, json)
{
  EVAL_ARG_FOR_CAST_TO_JSON()
  {
    common::ObArenaAllocator &temp_allocator = ctx.get_reset_tmp_alloc();
    ObIJsonBase *j_base = NULL;
    int64_t in_val = child_res->get_int();
    ObObjType in_type = expr.args_[0]->datum_meta_.type_;
    bool bool_val = (in_val == 1) ? true : false;
    ObJsonBoolean j_bool(bool_val);
    ObJsonInt j_int(in_val);

    if (expr.args_[0]->is_boolean_ == 1) {
      j_base = &j_bool;
    } else {
      j_base = &j_int;
    }
  
    ObString raw_bin;
    if (OB_FAIL(j_base->get_raw_binary(raw_bin, &temp_allocator))) {
      LOG_WARN("fail to get int json binary", K(ret), K(in_type), K(in_val));
    } else {
      ret = common_copy_string(expr, raw_bin, ctx, res_datum);
    }
  }
  return ret;
}

CAST_FUNC_NAME(uint, int)
{
  EVAL_ARG()
  {
    uint64_t in_val = child_res->get_uint();
    int64_t out_val = 0;
    if (OB_FAIL(common_uint_int(expr, expr.datum_meta_.type_, in_val, ctx, out_val))) {
      LOG_WARN("common_uint_int failed", K(ret));
    } else {
      res_datum.set_int(out_val);
    }
  }
  return ret;
}

CAST_FUNC_NAME(uint, uint)
{
  EVAL_ARG()
  {
    DEF_IN_OUT_TYPE();
    uint64_t in_val = child_res->get_uint();
    if (in_type > out_type && CAST_FAIL(uint_upper_check(out_type, in_val))) {
      LOG_WARN("int_upper_check failed", K(ret), K(in_val));
    } else {
      res_datum.set_uint(in_val);
    }
  }
  return ret;
}

CAST_FUNC_NAME(uint, double)
{
  EVAL_ARG()
  {
    uint64_t in_val = child_res->get_uint();
    res_datum.set_double(static_cast<double>(in_val));
  }
  return ret;
}

CAST_FUNC_NAME(uint, float)
{
  EVAL_ARG()
  {
    uint64_t in_val = child_res->get_uint();
    res_datum.set_float(static_cast<float>(static_cast<double>(in_val)));
  }
  return ret;
}

CAST_FUNC_NAME(uint, number)
{
  EVAL_ARG()
  {
    uint64_t in_val = child_res->get_uint();
    ObNumStackOnceAlloc tmp_alloc;
    number::ObNumber number;
    if (OB_FAIL(number.from(in_val, tmp_alloc))) {
      LOG_WARN("number.from failed", K(ret), K(in_val));
    } else {
      res_datum.set_number(number);
    }
  }
  return ret;
}

CAST_FUNC_NAME(uint, datetime)
{
  EVAL_ARG()
  {
    uint64_t in_val = child_res->get_uint();
    int64_t out_val = 0;
    if (OB_FAIL(common_uint_int(expr, ObIntType, in_val, ctx, out_val))) {
      LOG_WARN("common_uint_int failed", K(ret));
    } else if (OB_FAIL(common_int_datetime(expr, out_val, ctx, res_datum))) {
      LOG_WARN("common_int_datetime failed", K(ret));
    }
  }
  return ret;
}

CAST_FUNC_NAME(uint, date)
{
  EVAL_ARG()
  {
    uint64_t in_val = child_res->get_uint();
    int64_t out_val = 0;
    if (OB_FAIL(common_uint_int(expr, ObIntType, in_val, ctx, out_val))) {
      LOG_WARN("common_uint_int failed", K(ret));
    } else if (OB_FAIL(common_int_date(expr, out_val, res_datum))) {
      LOG_WARN("common_int_date failed", K(ret));
    }
  }
  return ret;
}

CAST_FUNC_NAME(uint, time)
{
  EVAL_ARG()
  {
    uint64_t in_val = child_res->get_uint();
    int64_t val_int = 0;
    if (OB_FAIL(common_uint_int(expr, ObIntType, in_val, ctx, val_int))) {
      LOG_WARN("common_uint_int failed", K(ret), K(in_val));
    } else if (OB_FAIL(common_int_time(expr, val_int, res_datum))) {
      LOG_WARN("common_int_time failed", K(ret), K(val_int));
    }
  }
  return ret;
}

CAST_FUNC_NAME(uint, year)
{
  EVAL_ARG()
  {
    uint64_t in_val = child_res->get_uint();
    int64_t val_int = 0;
    if (OB_FAIL(common_uint_int(expr, ObIntType, in_val, ctx, val_int))) {
      LOG_WARN("common_uint_int failed", K(ret), K(in_val));
    } else if (OB_FAIL(common_int_year(expr, val_int, res_datum))) {
      LOG_WARN("common_int_time failed", K(ret), K(val_int));
    }
  }
  return ret;
}

CAST_FUNC_NAME(uint, string)
{
  EVAL_ARG()
  {
    uint64_t in_val = child_res->get_uint();
    ObFastFormatInt ffi(in_val);
    if (OB_FAIL(common_copy_string_zf(expr, ObString(ffi.length(), ffi.ptr()), ctx, res_datum))) {
      LOG_WARN("common_copy_string_zf failed", K(ret), K(ObString(ffi.length(), ffi.ptr())));
    }
  }
  return ret;
}

CAST_FUNC_NAME(uint, lob)
{
  EVAL_ARG()
  {
    uint64_t in_val = child_res->get_uint();
    ObFastFormatInt ffi(in_val);
    ObString res_str;
    if (OB_FAIL(common_copy_string_zf(expr, ObString(ffi.length(), ffi.ptr()), ctx, res_datum))) {
      LOG_WARN("common_copy_string_zf failed", K(ret), K(ObString(ffi.length(), ffi.ptr())));
    } else if (OB_FAIL(copy_datum_str_with_tmp_alloc(ctx, res_datum, res_str))) {
      LOG_WARN("copy datum string with tmp allocator failed", K(ret));
    } else if (OB_FAIL(common_string_lob(expr, res_str, ctx, NULL, res_datum))) {
      LOG_WARN("cast string to lob failed", K(ret));
    }
  }
  return ret;
}

CAST_FUNC_NAME(uint, json)
{
  EVAL_ARG_FOR_CAST_TO_JSON()
  {
    uint64_t in_val = child_res->get_uint();
    ObObjType in_type = expr.args_[0]->datum_meta_.type_;
    ObJsonUint j_uint(in_val);
    ObIJsonBase *j_base = &j_uint;
    common::ObArenaAllocator &temp_allocator = ctx.get_reset_tmp_alloc();

    ObString raw_bin;

    if (OB_FAIL(j_base->get_raw_binary(raw_bin, &temp_allocator))) {
      LOG_WARN("fail to get uint json binary", K(ret), K(in_type), K(in_val));
    } else {
      ret = common_copy_string(expr, raw_bin, ctx, res_datum);
    }
  }
  return ret;
}

CAST_FUNC_NAME(string, int)
{
  EVAL_STRING_ARG()
  {
    ObString in_str(child_res->len_, child_res->ptr_);
    const bool is_str_int_cast = true;
    OZ(common_string_int(expr, expr.extra_, in_str, is_str_int_cast, res_datum));
  }
  return ret;
}

CAST_FUNC_NAME(string, uint)
{
  EVAL_STRING_ARG()
  {
    ObString in_str(child_res->len_, child_res->ptr_);
    const bool is_str_int_cast = true;
    OZ(common_string_uint(expr, in_str, is_str_int_cast, res_datum));
  }
  return ret;
}

CAST_FUNC_NAME(string, float)
{
  EVAL_STRING_ARG()
  {
    float out_val = 0;
    OZ(common_string_float(expr, ObString(child_res->len_, child_res->ptr_), out_val));
    OX(res_datum.set_float(out_val));
  }
  return ret;
}

CAST_FUNC_NAME(string, double)
{
  EVAL_STRING_ARG()
  {
    DEF_IN_OUT_TYPE();
    ObString in_str = ObString(child_res->len_, child_res->ptr_);
    OZ(common_string_double(expr, in_type, out_type, in_str, res_datum));
  }
  return ret;
}

CAST_FUNC_NAME(string, number)
{
  EVAL_STRING_ARG()
  {
    number::ObNumber nmb;
    ObNumStackOnceAlloc tmp_alloc;
    OZ(common_string_number(expr, ObString(child_res->len_, child_res->ptr_), tmp_alloc, nmb));
    OX(res_datum.set_number(nmb));
  }
  return ret;
}

CAST_FUNC_NAME(string, datetime)
{
  EVAL_STRING_ARG()
  {
    OZ(common_string_datetime(expr, ObString(child_res->len_, child_res->ptr_), ctx, res_datum));
  }
  return ret;
}

CAST_FUNC_NAME(string, date)
{
  EVAL_STRING_ARG()
  {
    OZ(common_string_date(expr, ObString(child_res->len_, child_res->ptr_), res_datum));
  }
  return ret;
}

CAST_FUNC_NAME(string, time)
{
  EVAL_STRING_ARG()
  {
    OZ(common_string_time(expr, ObString(child_res->len_, child_res->ptr_), res_datum));
  }
  return ret;
}

CAST_FUNC_NAME(string, year)
{
  EVAL_STRING_ARG()
  {
    OZ(common_string_year(expr, ObString(child_res->len_, child_res->ptr_), res_datum));
  }
  return ret;
}

CAST_FUNC_NAME(string, bit)
{
  EVAL_STRING_ARG()
  {
    OZ(common_string_bit(expr, ObString(child_res->len_, child_res->ptr_), ctx, res_datum));
  }
  return ret;
}

CAST_FUNC_NAME(string, string)
{
  EVAL_STRING_ARG()
  {
    ObObjType in_type = expr.args_[0]->datum_meta_.type_;
    ObObjType out_type = expr.datum_meta_.type_;
    ObCollationType in_cs_type = expr.args_[0]->datum_meta_.cs_type_;
    ObCollationType out_cs_type = expr.datum_meta_.cs_type_;
    ObString in_str(child_res->len_, child_res->ptr_);
    OZ(common_string_string(expr, in_type, in_cs_type, out_type, out_cs_type, in_str, ctx, res_datum));
  }
  return ret;
}

CAST_FUNC_NAME(string, otimestamp)
{
  EVAL_STRING_ARG()
  {
    ObString in_str(child_res->len_, child_res->ptr_);
    OZ(common_string_otimestamp(expr, in_str, ctx, res_datum));
  }
  return ret;
}

CAST_FUNC_NAME(string, raw)
{
  EVAL_STRING_ARG()
  {
    ObString in_str(child_res->len_, child_res->ptr_);
    OZ(ObDatumHexUtils::hextoraw_string(expr, in_str, ctx, res_datum));
  }
  return ret;
}

CAST_FUNC_NAME(string, interval)
{
  EVAL_STRING_ARG()
  {
    ObString in_str(child_res->len_, child_res->ptr_);
    OZ(common_string_interval(expr, in_str, ctx, res_datum));
  }
  return ret;
}

CAST_FUNC_NAME(string, rowid)
{
  EVAL_STRING_ARG()
  {
    ObString in_str(child_res->len_, child_res->ptr_);
    OZ(common_string_rowid(expr, in_str, ctx, res_datum));
  }
  return ret;
}

CAST_FUNC_NAME(string, lob)
{
  int ret = OB_SUCCESS;
  ObDatum* child_res = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, child_res))) {
    LOG_WARN("eval arg failed", K(ret));
  } else if (child_res->is_null() ||
             (lib::is_oracle_mode() && ObLongTextType != expr.args_[0]->datum_meta_.type_ && 0 == child_res->len_)) {
    res_datum.set_null();
  } else {
    ObString in_str(child_res->len_, child_res->ptr_);
    OZ(common_string_lob(expr, in_str, ctx, NULL, res_datum));
  }
  return ret;
}

static int common_string_json(const ObExpr &expr,
                              const ObString &in_str,
                              ObEvalCtx &ctx,
                              ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObObjType in_type = expr.args_[0]->datum_meta_.type_;
  ObObjType out_type = ObLongTextType;
  ObCollationType in_cs_type = expr.args_[0]->datum_meta_.cs_type_;
  ObCollationType out_cs_type = expr.datum_meta_.cs_type_;
  // binary type will convert to json opaque, other types need convert charset to utf8
  bool is_need_charset_convert = ((CS_TYPE_BINARY != in_cs_type) && 
                                  (ObCharset::charset_type_by_coll(in_cs_type) != 
                                   ObCharset::charset_type_by_coll(out_cs_type)));
  if (lib::is_mysql_mode() && (out_cs_type != CS_TYPE_UTF8MB4_BIN)) {
    ret = OB_ERR_INVALID_JSON_CHARSET;
    LOG_WARN("fail to cast string to json invalid outtype", K(ret), K(out_cs_type));
  } else if (is_need_charset_convert && 
    OB_FAIL(common_string_string(expr, in_type, in_cs_type, out_type,
                                 out_cs_type, in_str, ctx, res_datum))) {
    LOG_WARN("fail to cast string to longtext", K(ret), K(in_str), K(expr));
  } else {
    ObString j_text;
    common::ObArenaAllocator &temp_allocator = ctx.get_reset_tmp_alloc();
    if (is_need_charset_convert && OB_FAIL(copy_datum_str_with_tmp_alloc(ctx, res_datum, j_text))) {
      LOG_WARN("copy datum str with tmp alloc", K(ret));
    } else {
      if (is_need_charset_convert == false) {
        j_text.assign_ptr(in_str.ptr(), in_str.length());
      }
      bool is_enumset_to_str = ((expr.args_[0]->type_ == T_FUN_SET_TO_STR)
                                || (expr.args_[0]->type_ == T_FUN_ENUM_TO_STR));
      ObIJsonBase *j_base = NULL;
      ObJsonOpaque j_opaque(j_text, in_type);
      ObJsonString j_string(j_text.ptr(), j_text.length());
      ObJsonNode *j_tree = NULL;
      if (in_cs_type == CS_TYPE_BINARY) {
        j_base = &j_opaque;
      } else if (is_enumset_to_str || (CM_IS_IMPLICIT_CAST(expr.extra_)
          && !CM_IS_COLUMN_CONVERT(expr.extra_) && !CM_IS_JSON_VALUE(expr.extra_)
          && ob_is_string_type(in_type))) {
        // consistent with mysql: TINYTEXT, TEXT, MEDIUMTEXT, and LONGTEXT. We want to treat them like strings
        j_base = &j_string;
      } else if (OB_FAIL(ObJsonParser::get_tree(&temp_allocator, j_text, j_tree))) {
        if (CM_IS_IMPLICIT_CAST(expr.extra_) && !CM_IS_COLUMN_CONVERT(expr.extra_)) {
          ret = OB_SUCCESS;
          j_base = &j_string;
        } else {
          LOG_WARN("fail to parse string as json tree", K(ret), K(in_type), K(in_str));
          if (CM_IS_COLUMN_CONVERT(expr.extra_)) {
            ret = OB_ERR_INVALID_JSON_TEXT;
            LOG_USER_ERROR(OB_ERR_INVALID_JSON_TEXT);
          } else {
            ret = OB_ERR_INVALID_JSON_TEXT_IN_PARAM;
            LOG_USER_ERROR(OB_ERR_INVALID_JSON_TEXT_IN_PARAM);
          }
        }
      } else {
        j_base = j_tree;
      }

      if (OB_SUCC(ret)) {
        ObString raw_bin;
        if (OB_FAIL(j_base->get_raw_binary(raw_bin, &temp_allocator))) {
          LOG_WARN("fail to get string json binary", K(ret), K(in_type), K(raw_bin));
        } else {
          ret = common_copy_string(expr, raw_bin, ctx, res_datum);
        }
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(string, json)
{
  EVAL_ARG_FOR_CAST_TO_JSON()
  {
    ObDatum *child_res = NULL;
    if (OB_FAIL(expr.args_[0]->eval(ctx, child_res))) {
      LOG_WARN("eval arg failed", K(ret));
    } else {
      ObString in_str = child_res->get_string();
      ret = common_string_json(expr, in_str, ctx, res_datum);
    }
  }
  return ret;
}

CAST_FUNC_NAME(number, int)
{
  EVAL_ARG()
  {
    const number::ObNumber nmb(child_res->get_number());
    const char* nmb_buf = nmb.format();
    if (OB_ISNULL(nmb_buf)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("nmb_buf is NULL", K(ret));
    } else {
      ObString num_str(strlen(nmb_buf), nmb_buf);
      const bool is_str_int_cast = false;
      if (OB_FAIL(common_string_int(expr, expr.extra_, num_str, is_str_int_cast, res_datum))) {
        LOG_WARN("common_string_int failed", K(ret), K(num_str));
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(number, uint)
{
  EVAL_ARG()
  {
    OZ(common_number_uint(expr, *child_res, res_datum));
  }
  return ret;
}

CAST_FUNC_NAME(number, float)
{
  EVAL_ARG()
  {
    const number::ObNumber nmb(child_res->get_number());
    const char* nmb_buf = nmb.format();
    if (OB_ISNULL(nmb_buf)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("nmb_buf is NULL", K(ret));
    } else {
      ObString num_str(strlen(nmb_buf), nmb_buf);
      float out_val = 0;
      if (OB_FAIL(common_string_float(expr, num_str, out_val))) {
        LOG_WARN("common_string_float failed", K(ret), K(num_str));
      } else {
        res_datum.set_float(out_val);
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(number, double)
{
  EVAL_ARG()
  {
    if (child_res->is_null()) {
      res_datum.set_null();
    } else {
      const number::ObNumber nmb(child_res->get_number());
      const char *nmb_buf = nmb.format();
      if (OB_ISNULL(nmb_buf)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("nmb_buf is NULL", K(ret));
      } else {
        ObString num_str(strlen(nmb_buf), nmb_buf);
        DEF_IN_OUT_TYPE();
        if (OB_FAIL(common_string_double(expr, in_type, out_type, num_str, res_datum))) {
          LOG_WARN("common_string_double failed", K(ret), K(num_str));
        }
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(number, number)
{
  EVAL_ARG()
  {
    int warning = OB_SUCCESS;
    const number::ObNumber nmb(child_res->get_number());
    if (ObUNumberType == expr.datum_meta_.type_) {
      ObNumStackOnceAlloc tmp_alloc;
      number::ObNumber buf_nmb;
      if (OB_FAIL(buf_nmb.from(nmb, tmp_alloc))) {
        LOG_WARN("construct buf_nmb failed", K(ret), K(nmb));
      } else if (CAST_FAIL(numeric_negative_check(buf_nmb))) {
        LOG_WARN("numeric_negative_check failed", K(ret));
      } else {
        res_datum.set_number(buf_nmb);
      }
    } else {
      res_datum.set_number(nmb);
    }
  }
  return ret;
}

CAST_FUNC_NAME(number, string)
{
  EVAL_ARG()
  {
    if (OB_FAIL(common_number_string(expr, *child_res, ctx, res_datum))) {
      LOG_WARN("common_number_string failed", K(ret));
    }
  }
  return ret;
}

CAST_FUNC_NAME(number, time)
{
  EVAL_ARG()
  {
    const number::ObNumber nmb(child_res->get_number());
    const char* nmb_buf = nmb.format();
    if (OB_ISNULL(nmb_buf)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null pointer", K(ret), K(nmb_buf));
    } else if (OB_FAIL(common_string_time(expr, ObString(strlen(nmb_buf), nmb_buf), res_datum))) {
      LOG_WARN("common_string_time failed", K(ret));
    }
  }
  return ret;
}

CAST_FUNC_NAME(number, year)
{
  EVAL_ARG()
  {
    const number::ObNumber nmb(child_res->get_number());
    const char* nmb_buf = nmb.format();
    if (OB_ISNULL(nmb_buf)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null pointer", K(ret), K(nmb_buf));
    } else if (nmb.is_negative()) {
      // the year shouldn't accept a negative number, if we use the common_string_year.
      // number like -0.4 could be converted to year, which should raise error in mysql
      if (OB_FAIL(common_int_year(expr, INT_MIN, res_datum))) {
        LOG_WARN("common_int_year failed", K(ret));
      }
    } else if (OB_FAIL(common_string_year(expr, ObString(strlen(nmb_buf), nmb_buf),
                                          res_datum))) {
      LOG_WARN("common_string_year failed", K(ret));
    }
  }
  return ret;
}

CAST_FUNC_NAME(number, datetime)
{
  EVAL_ARG()
  {
    GET_SESSION()
    {
      const number::ObNumber nmb(child_res->get_number());
      ObObjType out_type = expr.datum_meta_.type_;
      ObTimeConvertCtx cvrt_ctx(session->get_timezone_info(), ObTimestampType == out_type);
      int64_t out_val = 0;
      ret = common_number_datetime(nmb, cvrt_ctx, out_val, expr.extra_);
      int warning = OB_SUCCESS;
      if (CAST_FAIL(ret)) {
      } else {
        SET_RES_DATETIME(out_val);
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(number, date)
{
  EVAL_ARG()
  {
    int32_t out_val = 0;
    int warning = OB_SUCCESS;
    const number::ObNumber nmb(child_res->get_number());
    int64_t int_part = 0;
    int64_t dec_part = 0;
    if (nmb.is_negative()) {
      ret = OB_INVALID_DATE_VALUE;
      LOG_WARN("invalid date value", K(ret), K(nmb));
    } else if (!nmb.is_int_parts_valid_int64(int_part, dec_part)) {
      ret = OB_INVALID_DATE_VALUE;
      LOG_WARN("invalid date format", K(ret), K(nmb));
    } else {
      ret = ObTimeConverter::int_to_date(int_part, out_val);
      if (OB_SUCC(ret) && OB_UNLIKELY(dec_part > 0)) {
        if (CM_IS_COLUMN_CONVERT(expr.extra_) && !CM_IS_WARN_ON_FAIL(expr.extra_)) {
          ret = OB_INVALID_DATE_VALUE;
          LOG_WARN("invalid date value with decimal part", K(ret));
        }
      }
    }

    if (CAST_FAIL(ret)) {
    } else {
      SET_RES_DATE(out_val);
    }
  }
  return ret;
}

CAST_FUNC_NAME(number, bit)
{
  EVAL_ARG()
  {
    if (OB_FAIL(common_number_uint(expr, *child_res, res_datum))) {
      LOG_WARN("common_number_uint failed", K(ret));
    }
  }
  return ret;
}

CAST_FUNC_NAME(number, lob)
{
  EVAL_ARG()
  {
    ObString res_str;
    if (OB_FAIL(common_number_string(expr, *child_res, ctx, res_datum))) {
      LOG_WARN("common_number_string failed", K(ret));
    } else if (OB_FAIL(copy_datum_str_with_tmp_alloc(ctx, res_datum, res_str))) {
      LOG_WARN("copy datum string with tmp allocator failed", K(ret));
    } else if (OB_FAIL(common_string_lob(expr, res_str, ctx, NULL, res_datum))) {
      LOG_WARN("cast string to lob failed", K(ret));
    }
  }
  return ret;
}

CAST_FUNC_NAME(number, json)
{
  EVAL_ARG()
  {
    const number::ObNumber nmb(child_res->get_number());
    ObObjType in_type = expr.args_[0]->datum_meta_.type_;
    ObPrecision prec = expr.args_[0]->datum_meta_.precision_;
    ObScale scale = expr.args_[0]->datum_meta_.scale_;
    ObJsonDecimal j_dec(nmb, prec, scale);
    ObIJsonBase *j_base = &j_dec;
    common::ObArenaAllocator &temp_allocator = ctx.get_reset_tmp_alloc();
    ObString raw_bin;
    if (OB_FAIL(j_base->get_raw_binary(raw_bin, &temp_allocator))) {
      LOG_WARN("fail to get number json binary", K(ret), K(in_type), K(nmb));
    } else {
      ret = common_copy_string(expr, raw_bin, ctx, res_datum);
    }
  }
  return ret;
}

CAST_FUNC_NAME(float, int)
{
  EVAL_ARG()
  if (OB_SUCC(ret)) {
    DEF_IN_OUT_VAL(float, int64_t, 0);
    if (CAST_FAIL(
            common_double_int(in_val, out_val, LLONG_MIN, CM_IS_COLUMN_CONVERT(expr.extra_) ? LLONG_MAX : LLONG_MIN))) {
      LOG_WARN("common_floating_int failed", K(ret));
    } else if (CAST_FAIL(int_range_check(expr.datum_meta_.type_, out_val, out_val))) {
      LOG_WARN("int_range_check failed", K(ret));
    } else {
      res_datum.set_int(out_val);
    }
  }
  return ret;
}

CAST_FUNC_NAME(float, uint)
{
  EVAL_ARG()
  {
    DEF_IN_OUT_VAL(float, uint64_t, 0);
    if (in_val <= static_cast<double>(LLONG_MIN) || in_val >= static_cast<double>(ULLONG_MAX)) {
      out_val = static_cast<uint64_t>(LLONG_MIN);
      ret = OB_DATA_OUT_OF_RANGE;
    } else {
      if (CM_IS_COLUMN_CONVERT(expr.extra_)) {
        out_val = static_cast<uint64_t>(rint(in_val));
      } else {
        out_val = static_cast<uint64_t>(static_cast<int64_t>(rint(in_val)));
      }
      if (in_val < 0 && out_val != 0) {
        ret = OB_DATA_OUT_OF_RANGE;
      }
    }
    if (CAST_FAIL(ret)) {
      LOG_WARN("cast float to uint failed", K(ret), K(in_val), K(out_val));
    } else if (CM_NEED_RANGE_CHECK(expr.extra_) &&
               CAST_FAIL(uint_range_check(expr.datum_meta_.type_, out_val, out_val))) {
      LOG_WARN("int_range_check failed", K(ret));
    } else {
      res_datum.set_uint(out_val);
    }
  }
  return ret;
}

CAST_FUNC_NAME(float, float)
{
  EVAL_ARG()
  {
    int warning = OB_SUCCESS;
    float val_float = child_res->get_float();
    ObObjType out_type = expr.datum_meta_.type_;
    if (ObUFloatType == out_type && CAST_FAIL(numeric_negative_check(val_float))) {
      LOG_WARN("numeric_negative_check failed", K(ret));
    } else {
      res_datum.set_float(val_float);
    }
  }
  return ret;
}

CAST_FUNC_NAME(float, double)
{
  EVAL_ARG()
  {
    DEF_IN_OUT_VAL(float, double, static_cast<double>(in_val));
    ObObjType out_type = expr.datum_meta_.type_;
    if (ObUDoubleType == out_type && CAST_FAIL(numeric_negative_check(out_val))) {
      LOG_WARN("numeric_negative_check failed", K(ret));
    } else {
      res_datum.set_double(out_val);
    }
  }
  return ret;
}

CAST_FUNC_NAME(float, number)
{
  EVAL_ARG()
  {
    float in_val = child_res->get_float();
    ObObjType out_type = expr.datum_meta_.type_;
    int warning = OB_SUCCESS;
    ObNumStackOnceAlloc tmp_alloc;
    number::ObNumber number;
    if (ObUNumberType == out_type && CAST_FAIL(numeric_negative_check(in_val))) {
      LOG_WARN("numeric_negative_check failed", K(ret), K(out_type), K(in_val));
    } else if (OB_FAIL(common_floating_number(in_val, OB_GCVT_ARG_FLOAT, tmp_alloc, number))) {
      LOG_WARN("common_float_number failed", K(ret), K(in_val));
    } else {
      res_datum.set_number(number);
    }
  }
  return ret;
}

CAST_FUNC_NAME(float, datetime)
{
  EVAL_ARG()
  {
    float in_val = child_res->get_float();
    double val_double = static_cast<double>(in_val);
    if (OB_FAIL(common_double_datetime(expr, val_double, ctx, res_datum))) {
      LOG_WARN("common_double_datetime failed", K(ret), K(val_double));
    }
  }
  return ret;
}

CAST_FUNC_NAME(float, date)
{
  EVAL_ARG()
  {
    DEF_IN_OUT_VAL(float, int64_t, 0);
    if (OB_FAIL(common_floating_int(in_val, out_val))) {
      LOG_WARN("common_double_int failed", K(ret));
    } else if (OB_FAIL(common_int_date(expr, out_val, res_datum))) {
      LOG_WARN("common_int_date failed", K(ret));
    }
  }
  return ret;
}

CAST_FUNC_NAME(float, time)
{
  EVAL_ARG()
  {
    double in_val = static_cast<double>(child_res->get_float());
    if (OB_FAIL(common_double_time(expr, in_val, res_datum))) {
      LOG_WARN("common_double_time failed", K(ret));
    }
  }
  return ret;
}

CAST_FUNC_NAME(float, string)
{
  EVAL_ARG()
  {
    float in_val = child_res->get_float();
    if (OB_FAIL(common_floating_string(expr, in_val, OB_GCVT_ARG_FLOAT, ctx, res_datum))) {
      LOG_WARN("common_floating_string failed", K(ret), K(in_val));
    }
  }
  return ret;
}

CAST_FUNC_NAME(float, lob)
{
  EVAL_ARG()
  {
    float in_val = child_res->get_float();
    ObString res_str;
    if (OB_FAIL(common_floating_string(expr, in_val, OB_GCVT_ARG_FLOAT, ctx, res_datum))) {
      LOG_WARN("common_floating_string failed", K(ret), K(in_val));
    } else if (OB_FAIL(copy_datum_str_with_tmp_alloc(ctx, res_datum, res_str))) {
      LOG_WARN("copy datum string with tmp allocator failed", K(ret));
    } else if (OB_FAIL(common_string_lob(expr, res_str, ctx, NULL, res_datum))) {
      LOG_WARN("cast string to lob failed", K(ret));
    }
  }
  return ret;
}

CAST_FUNC_NAME(float, json)
{
  EVAL_ARG_FOR_CAST_TO_JSON()
  {
    float in_val = child_res->get_float();
    ObObjType in_type = expr.args_[0]->datum_meta_.type_;
    ObJsonDouble j_float(in_val);
    ObIJsonBase *j_base = &j_float;
    common::ObArenaAllocator &temp_allocator = ctx.get_reset_tmp_alloc();
    ObString raw_bin;
    
    if (OB_FAIL(j_base->get_raw_binary(raw_bin, &temp_allocator))) {
      LOG_WARN("fail to get float json binary", K(ret), K(in_type), K(in_val));
    } else {
      ret = common_copy_string(expr, raw_bin, ctx, res_datum);
    }
  }
  return ret;
}

CAST_FUNC_NAME(float, bit)
{
  EVAL_ARG()
  {
    float val_float = child_res->get_float();
    res_datum.set_bit(static_cast<uint64_t>(val_float));
  }
  return ret;
}

CAST_FUNC_NAME(double, int)
{
  EVAL_ARG()
  {
    DEF_IN_OUT_VAL(double, int64_t, 0);
    if (CAST_FAIL(common_double_int(in_val, out_val, LLONG_MIN, LLONG_MAX))) {
      LOG_WARN("common double to in failed", K(ret), K(in_val));
    } else if (CM_NEED_RANGE_CHECK(expr.extra_) &&
               CAST_FAIL(int_range_check(expr.datum_meta_.type_, out_val, out_val))) {
      LOG_WARN("int_range_check failed", K(ret));
    } else {
      res_datum.set_int(out_val);
    }
  }
  return ret;
}

CAST_FUNC_NAME(double, uint)
{
  EVAL_ARG()
  {
    DEF_IN_OUT_VAL(double, uint64_t, 0);
    if (in_val <= static_cast<double>(LLONG_MIN)) {
      out_val = static_cast<uint64_t>(LLONG_MIN);
      ret = OB_DATA_OUT_OF_RANGE;
    } else if (in_val >= static_cast<double>(ULLONG_MAX)) {
      out_val = static_cast<uint64_t>(LLONG_MAX);
      ret = OB_DATA_OUT_OF_RANGE;
    } else {
      if (CM_IS_COLUMN_CONVERT(expr.extra_)) {
        out_val = static_cast<uint64_t>(rint(in_val));
      } else if (in_val >= static_cast<double>(LLONG_MAX)) {
        out_val = static_cast<uint64_t>(LLONG_MAX);
      } else {
        out_val = static_cast<uint64_t>(static_cast<int64_t>(rint(in_val)));
      }
      if (in_val < 0 && out_val != 0) {
        ret = OB_DATA_OUT_OF_RANGE;
      }
    }
    if (CAST_FAIL(ret)) {
      LOG_WARN("cast float to uint failed", K(ret), K(in_val), K(out_val));
    } else if (CM_NEED_RANGE_CHECK(expr.extra_) &&
               CAST_FAIL(uint_range_check(expr.datum_meta_.type_, out_val, out_val))) {
      LOG_WARN("int_range_check failed", K(ret));
    } else {
      res_datum.set_uint(out_val);
    }
  }
  return ret;
}

CAST_FUNC_NAME(double, float)
{
  EVAL_ARG()
  {
    double in_val = child_res->get_double();
    float out_val = static_cast<float>(in_val);
    if (OB_FAIL(common_double_float(expr, in_val, out_val))) {
      LOG_WARN("common_double_float failed", K(ret), K(in_val));
    } else {
      res_datum.set_float(out_val);
    }
  }
  return ret;
}

CAST_FUNC_NAME(double, double)
{
  EVAL_ARG()
  {
    int warning = OB_SUCCESS;
    double val_double = child_res->get_double();
    ObObjType out_type = expr.datum_meta_.type_;
    if (ObUDoubleType == out_type && CAST_FAIL(numeric_negative_check(val_double))) {
      LOG_WARN("numeric_negative_check failed", K(ret));
    } else {
      res_datum.set_double(val_double);
    }
  }
  return ret;
}

CAST_FUNC_NAME(double, number)
{
  EVAL_ARG()
  {
    ObObjType out_type = expr.datum_meta_.type_;
    double in_val = child_res->get_double();
    int warning = OB_SUCCESS;
    ObNumStackOnceAlloc tmp_alloc;
    number::ObNumber number;
    if (ObUNumberType == out_type && CAST_FAIL(numeric_negative_check(in_val))) {
      LOG_WARN("numeric_negative_check failed", K(ret), K(out_type), K(in_val));
    } else if (OB_FAIL(common_floating_number(in_val, OB_GCVT_ARG_DOUBLE, tmp_alloc, number))) {
      LOG_WARN("common_float_number failed", K(ret), K(in_val));
    } else {
      res_datum.set_number(number);
    }
  }
  return ret;
}

CAST_FUNC_NAME(double, datetime)
{
  EVAL_ARG()
  {
    double in_val = child_res->get_double();
    if (OB_FAIL(common_double_datetime(expr, in_val, ctx, res_datum))) {
      LOG_WARN("common_double_datetime failed", K(ret));
    }
  }
  return ret;
}

CAST_FUNC_NAME(double, date)
{
  EVAL_ARG()
  {
    DEF_IN_OUT_VAL(double, int64_t, 0);
    if (OB_FAIL(common_floating_int(in_val, out_val))) {
      LOG_WARN("common_double_int failed", K(ret));
    } else if (OB_FAIL(common_int_date(expr, out_val, res_datum))) {
      LOG_WARN("common_int_date failed", K(ret));
    }
  }
  return ret;
}

CAST_FUNC_NAME(double, time)
{
  EVAL_ARG()
  {
    double in_val = child_res->get_double();
    if (OB_FAIL(common_double_time(expr, in_val, res_datum))) {
      LOG_WARN("common_double_time failed", K(ret));
    }
  }
  return ret;
}

CAST_FUNC_NAME(double, year)
{
  EVAL_ARG()
  {
    // When we insert 999999999999999999999.9(larger than max int) into a year field in mysql
    // Mysql raise the same error as we insert 100 into a year field (1264).
    // So the cast from double to int won't raise extra error. That's why we directly use
    // static_cast here. Mysql will convert the double to nearest int and insert it to the year field.
    double in_val = child_res->get_double();
    in_val = in_val < 0 ? INT_MIN : in_val + 0.5;
    int64_t val_int = static_cast<int64_t>(in_val);
    if (OB_FAIL(common_int_year(expr, val_int, res_datum))) {
      LOG_WARN("common_int_time failed", K(ret), K(val_int));
    }
  }
  return ret;
}

CAST_FUNC_NAME(double, string)
{
  EVAL_ARG()
  {
    double in_val = child_res->get_double();
    if (OB_FAIL(common_floating_string(expr, in_val, OB_GCVT_ARG_DOUBLE, ctx, res_datum))) {
      LOG_WARN("common_floating_string failed", K(ret), K(in_val));
    }
  }
  return ret;
}

CAST_FUNC_NAME(double, lob)
{
  EVAL_ARG()
  {
    double in_val = child_res->get_double();
    ObString res_str;
    if (OB_FAIL(common_floating_string(expr, in_val, OB_GCVT_ARG_DOUBLE, ctx, res_datum))) {
      LOG_WARN("common_floating_string failed", K(ret), K(in_val));
    } else if (OB_FAIL(copy_datum_str_with_tmp_alloc(ctx, res_datum, res_str))) {
      LOG_WARN("copy datum string with tmp allocator failed", K(ret));
    } else if (OB_FAIL(common_string_lob(expr, res_str, ctx, NULL, res_datum))) {
      LOG_WARN("cast string to lob failed", K(ret));
    }
  }
  return ret;
}

CAST_FUNC_NAME(double, bit)
{
  EVAL_ARG()
  {
    res_datum.set_bit(static_cast<uint64_t>(child_res->get_double()));
  }
  return ret;
}

CAST_FUNC_NAME(double, json)
{
  EVAL_ARG_FOR_CAST_TO_JSON()
  {
    double in_val = child_res->get_double();
    ObObjType in_type = expr.args_[0]->datum_meta_.type_;
    ObJsonDouble j_double(in_val);
    ObIJsonBase *j_base = &j_double;
    common::ObArenaAllocator &temp_allocator = ctx.get_reset_tmp_alloc();
    ObString raw_bin;
    if (OB_FAIL(j_base->get_raw_binary(raw_bin, &temp_allocator))) {
      LOG_WARN("fail to get double json binary", K(ret), K(in_type), K(in_val));
    } else {
      ret = common_copy_string(expr, raw_bin, ctx, res_datum);
    }
  }
  return ret;
}

CAST_FUNC_NAME(datetime, int)
{
  EVAL_ARG()
  {
    GET_SESSION()
    {
      DEF_IN_OUT_TYPE();
      const ObTimeZoneInfo* tz_info = (ObTimestampType == in_type) ? session->get_timezone_info() : NULL;
      int64_t in_val = child_res->get_int();
      int64_t out_val = 0;
      if (OB_FAIL(ObTimeConverter::datetime_to_int(in_val, tz_info, out_val))) {
        LOG_WARN("datetime_to_int failed", K(ret), K(in_val));
      } else if (out_type < ObIntType && CAST_FAIL(int_range_check(out_type, out_val, out_val))) {
        LOG_WARN("int_range_check failed", K(ret));
      } else {
        res_datum.set_int(out_val);
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(datetime, uint)
{
  EVAL_ARG()
  {
    GET_SESSION()
    {
      DEF_IN_OUT_TYPE();
      const ObTimeZoneInfo* tz_info = (ObTimestampType == in_type) ? session->get_timezone_info() : NULL;
      int64_t in_val = child_res->get_int();
      int64_t val_int = 0;
      uint64_t out_val = 0;
      if (OB_FAIL(ObTimeConverter::datetime_to_int(in_val, tz_info, val_int))) {
        LOG_WARN("datetime_to_int failed", K(ret), K(in_val));
      } else {
        out_val = static_cast<uint64_t>(val_int);
        if (out_type < ObUInt64Type && CAST_FAIL(uint_range_check(out_type, val_int, out_val))) {
          LOG_WARN("int_range_check failed", K(ret));
        } else {
          res_datum.set_uint(out_val);
        }
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(datetime, double)
{
  EVAL_ARG()
  {
    GET_SESSION()
    {
      int64_t in_val = child_res->get_int();
      double out_val = 0.0;
      ObObjType in_type = expr.args_[0]->datum_meta_.type_;
      const ObTimeZoneInfo* tz_info = (ObTimestampType == in_type) ? session->get_timezone_info() : NULL;
      if (OB_FAIL(ObTimeConverter::datetime_to_double(in_val, tz_info, out_val))) {
        LOG_WARN("datetime_to_double failed", K(ret), K(in_val));
      } else {
        res_datum.set_double(out_val);
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(datetime, float)
{
  EVAL_ARG()
  {
    GET_SESSION()
    {
      int64_t in_val = child_res->get_int();
      double out_val = 0.0;
      ObObjType in_type = expr.args_[0]->datum_meta_.type_;
      const ObTimeZoneInfo* tz_info = (ObTimestampType == in_type) ? session->get_timezone_info() : NULL;
      if (OB_FAIL(ObTimeConverter::datetime_to_double(in_val, tz_info, out_val))) {
        LOG_WARN("datetime_to_double failed", K(ret), K(in_val));
      } else {
        res_datum.set_float(static_cast<float>(out_val));
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(datetime, number)
{
  EVAL_ARG()
  {
    GET_SESSION()
    {
      int warning = OB_SUCCESS;
      ObObjType in_type = expr.args_[0]->datum_meta_.type_;
      int64_t in_val = child_res->get_int();
      ObString nls_format;
      char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
      int64_t len = 0;
      ObNumStackOnceAlloc tmp_alloc;
      number::ObNumber number;
      ObPrecision res_precision;  // useless
      ObScale res_scale;          // useless
      ObScale in_scale = expr.args_[0]->datum_meta_.scale_;
      const ObTimeZoneInfo* tz_info = (ObTimestampType == in_type) ? session->get_timezone_info() : NULL;
      if (OB_FAIL(
              ObTimeConverter::datetime_to_str(in_val, tz_info, nls_format, in_scale, buf, sizeof(buf), len, false))) {
        LOG_WARN("failed to convert datetime to string", K(ret));
      } else if (CAST_FAIL(number.from(buf, len, tmp_alloc, &res_precision, &res_scale))) {
        LOG_WARN("failed to convert string to number", K(ret));
      } else {
        res_datum.set_number(number);
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(datetime, datetime)
{
  EVAL_ARG()
  {
    GET_SESSION()
    {
      int64_t in_val = child_res->get_int();
      int64_t out_val = in_val;
      ObObjType in_type = expr.args_[0]->datum_meta_.type_;
      ObObjType out_type = expr.datum_meta_.type_;
      if (ObDateTimeType == in_type && ObTimestampType == out_type) {
        ret = ObTimeConverter::datetime_to_timestamp(in_val,
                                                     session->get_timezone_info(),
                                                     out_val);
        ret = OB_ERR_UNEXPECTED_TZ_TRANSITION == ret ? OB_INVALID_DATE_VALUE : ret;
      } else if (ObTimestampType == in_type && ObDateTimeType == out_type) {
        ret = ObTimeConverter::timestamp_to_datetime(out_val, session->get_timezone_info(), out_val);
      }
      if (OB_FAIL(ret)) {
      } else {
        res_datum.set_datetime(out_val);
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(datetime, date)
{
  EVAL_ARG()
  {
    GET_SESSION()
    {
      int64_t in_val = child_res->get_int();
      int32_t out_val = 0;
      ObObjType in_type = expr.args_[0]->datum_meta_.type_;
      const ObTimeZoneInfo* tz_info = (ObTimestampType == in_type) ? session->get_timezone_info() : NULL;
      if (OB_FAIL(ObTimeConverter::datetime_to_date(in_val, tz_info, out_val))) {
        LOG_WARN("datetime_to_date failed", K(ret), K(in_val));
      } else {
        res_datum.set_date(out_val);
      }
      LOG_DEBUG("in datetime date cast", K(ret), K(out_val), K(in_val));
    }
  }
  return ret;
}

CAST_FUNC_NAME(datetime, time)
{
  EVAL_ARG()
  {
    GET_SESSION()
    {
      int64_t in_val = child_res->get_int();
      int64_t out_val = 0;
      ObObjType in_type = expr.args_[0]->datum_meta_.type_;
      const ObTimeZoneInfo* tz_info = (ObTimestampType == in_type) ? session->get_timezone_info() : NULL;
      if (OB_FAIL(ObTimeConverter::datetime_to_time(in_val, tz_info, out_val))) {
      } else {
        res_datum.set_time(out_val);
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(datetime, year)
{
  EVAL_ARG()
  {
    GET_SESSION()
    {
      DEF_IN_OUT_VAL(int64_t, uint8_t, 0);
      ObObjType in_type = expr.args_[0]->datum_meta_.type_;
      const ObTimeZoneInfo* tz_info = (ObTimestampType == in_type) ? session->get_timezone_info() : NULL;
      if (CAST_FAIL(ObTimeConverter::datetime_to_year(in_val, tz_info, out_val))) {
      } else {
        res_datum.set_year(out_val);
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(datetime, string)
{
  EVAL_ARG()
  {
    char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
    int64_t len = 0;
    int64_t in_val = child_res->get_int();
    if (OB_FAIL(common_datetime_string(expr.args_[0]->datum_meta_.type_,
            expr.datum_meta_.type_,
            expr.args_[0]->datum_meta_.scale_,
            CM_IS_FORCE_USE_STANDARD_NLS_FORMAT(expr.extra_),
            in_val,
            ctx,
            buf,
            sizeof(buf),
            len))) {
      LOG_WARN("common_datetime_string failed", K(ret));
    } else {
      ObString str(len, buf);
      if (OB_FAIL(common_copy_string(expr, str, ctx, res_datum))) {
        LOG_WARN("common_copy_string failed", K(ret));
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(datetime, bit)
{
  EVAL_ARG()
  {
    char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
    int64_t len = 0;
    int64_t in_val = child_res->get_int();
    if (OB_FAIL(common_datetime_string(expr.args_[0]->datum_meta_.type_,
            expr.datum_meta_.type_,
            expr.args_[0]->datum_meta_.scale_,
            CM_IS_FORCE_USE_STANDARD_NLS_FORMAT(expr.extra_),
            in_val,
            ctx,
            buf,
            sizeof(buf),
            len))) {
      LOG_WARN("common_datetime_string failed", K(ret));
    } else if (OB_FAIL(common_string_bit(expr, ObString(len, buf), ctx, res_datum))) {
      LOG_WARN("common_string_bit failed", K(ret));
    }
  }
  return ret;
}

CAST_FUNC_NAME(datetime, otimestamp)
{
  EVAL_ARG()
  {
    GET_SESSION()
    {
      DEF_IN_OUT_TYPE();
      const ObTimeZoneInfo* tz_info = session->get_timezone_info();
      int64_t in_val = child_res->get_int();
      ObOTimestampData out_val;
      if (OB_FAIL(ObTimeConverter::odate_to_otimestamp(in_val, tz_info, out_type, out_val))) {
        LOG_WARN("fail to timestamp_to_timestamp_tz", K(ret), K(in_val), K(in_type), K(out_type));
      } else {
        if (ObTimestampTZType == out_type) {
          SET_RES_OTIMESTAMP(out_val);
        } else {
          SET_RES_OTIMESTAMP_10BYTE(out_val);
        }
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(datetime, lob)
{
  EVAL_ARG()
  {
    char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
    int64_t len = 0;
    int64_t in_val = child_res->get_int();
    if (OB_FAIL(common_datetime_string(expr.args_[0]->datum_meta_.type_,
            ObLongTextType,
            expr.args_[0]->datum_meta_.scale_,
            CM_IS_FORCE_USE_STANDARD_NLS_FORMAT(expr.extra_),
            in_val,
            ctx,
            buf,
            sizeof(buf),
            len))) {
      LOG_WARN("common_datetime_string failed", K(ret));
    } else {
      ObString str(len, buf);
      ObString res_str;
      if (OB_FAIL(common_copy_string(expr, str, ctx, res_datum))) {
        LOG_WARN("common_copy_string failed", K(ret));
      } else if (OB_FAIL(copy_datum_str_with_tmp_alloc(ctx, res_datum, res_str))) {
        LOG_WARN("copy datum string with tmp allocator failed", K(ret));
      } else if (OB_FAIL(common_string_lob(expr, res_str, ctx, NULL, res_datum))) {
        LOG_WARN("cast string to lob failed", K(ret));
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(datetime, json)
{
  EVAL_ARG_FOR_CAST_TO_JSON()
  {
    GET_SESSION()
    {
      int64_t in_val = child_res->get_datetime();
      ObObjType in_type = expr.args_[0]->datum_meta_.type_;
      ObTime ob_time(DT_TYPE_DATETIME);
      const ObTimeZoneInfo *tz_info = (ObTimestampType == in_type) ?
                                      session->get_timezone_info() : NULL;
      if (OB_FAIL(ObTimeConverter::datetime_to_ob_time(in_val, tz_info, ob_time))) {
        LOG_WARN("fail to create datetime from int failed", K(ret), K(in_type), K(in_val));
      } else {
        ObJsonNodeType node_type = (ObTimestampType == in_type) 
                                    ? ObJsonNodeType::J_TIMESTAMP
                                    : ObJsonNodeType::J_DATETIME;
        ObJsonDatetime j_datetime(node_type, ob_time);
        ObIJsonBase *j_base = &j_datetime;
        common::ObArenaAllocator &temp_allocator = ctx.get_reset_tmp_alloc();
        ObString raw_bin;

        if (OB_FAIL(j_base->get_raw_binary(raw_bin, &temp_allocator))) {
          LOG_WARN("fail to get datetime json binary", K(ret), K(in_type), K(in_val));
        } else {
          ret = common_copy_string(expr, raw_bin, ctx, res_datum);
        }
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(date, int)
{
  EVAL_ARG()
  {
    DEF_IN_OUT_VAL(int32_t, int64_t, 0);
    ObObjType out_type = expr.datum_meta_.type_;
    if (OB_FAIL(ObTimeConverter::date_to_int(in_val, out_val))) {
      LOG_WARN("date_to_int failed", K(ret));
    } else if (out_type < ObInt32Type && CAST_FAIL(int_range_check(out_type, out_val, out_val))) {
      LOG_WARN("int_range_check failed", K(ret));
    } else {
      res_datum.set_int(out_val);
    }
  }
  return ret;
}

CAST_FUNC_NAME(date, uint)
{
  EVAL_ARG()
  {
    DEF_IN_OUT_VAL(int32_t, uint64_t, 0);
    int64_t val_int = 0;
    ObObjType out_type = expr.datum_meta_.type_;
    if (OB_FAIL(ObTimeConverter::date_to_int(in_val, val_int))) {
      LOG_WARN("date_to_int failed", K(ret), K(in_val), K(val_int));
    } else {
      out_val = static_cast<uint64_t>(val_int);
      if (out_type < ObUInt32Type && CAST_FAIL(uint_range_check(out_type, val_int, out_val))) {
        LOG_WARN("uint_range_check failed", K(ret), K(val_int), K(out_val));
      } else {
        res_datum.set_uint(out_val);
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(date, float)
{
  EVAL_ARG()
  {
    int32_t in_val = child_res->get_int32();
    int64_t out_val = 0;
    if (OB_FAIL(ObTimeConverter::date_to_int(in_val, out_val))) {
    } else {
      res_datum.set_float(static_cast<float>(out_val));
    }
  }
  return ret;
}

CAST_FUNC_NAME(date, double)
{
  EVAL_ARG()
  {
    int32_t in_val = child_res->get_int32();
    int64_t out_val = 0;
    if (OB_FAIL(ObTimeConverter::date_to_int(in_val, out_val))) {
    } else {
      res_datum.set_double(static_cast<double>(out_val));
    }
  }
  return ret;
}

CAST_FUNC_NAME(date, number)
{
  EVAL_ARG()
  {
    DEF_IN_OUT_VAL(int32_t, int64_t, 0);
    ObNumStackOnceAlloc tmp_alloc;
    number::ObNumber number;
    if (OB_FAIL(ObTimeConverter::date_to_int(in_val, out_val))) {
      LOG_WARN("date_to_int failed", K(ret));
    } else if (CAST_FAIL(numeric_negative_check(out_val))) {
      LOG_WARN("numeric_negative_check failed", K(ret), K(out_val));
    } else if (OB_FAIL(number.from(out_val, tmp_alloc))) {
      LOG_WARN("number.from failed", K(ret), K(out_val));
    } else {
      res_datum.set_number(number);
    }
  }
  return ret;
}

CAST_FUNC_NAME(date, datetime)
{
  EVAL_ARG()
  {
    GET_SESSION()
    {
      ObObjType out_type = expr.datum_meta_.type_;
      ObTimeConvertCtx cvrt_ctx(session->get_timezone_info(), ObTimestampType == out_type);
      int32_t in_val = child_res->get_int32();
      int64_t out_val = 0;
      if (OB_FAIL(ObTimeConverter::date_to_datetime(in_val, cvrt_ctx, out_val))) {
        LOG_WARN("date_to_datetime failed", K(ret), K(in_val));
      } else {
        res_datum.set_datetime(out_val);
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(date, time)
{
  EVAL_ARG()
  {
    res_datum.set_time(ObTimeConverter::ZERO_TIME);
  }
  return ret;
}

CAST_FUNC_NAME(date, year)
{
  EVAL_ARG()
  {
    DEF_IN_OUT_VAL(int32_t, uint8_t, 0);
    if (CAST_FAIL(ObTimeConverter::date_to_year(in_val, out_val))) {
      LOG_WARN("date_to_year failed", K(ret));
    } else {
      SET_RES_YEAR(out_val);
    }
  }
  return ret;
}

CAST_FUNC_NAME(date, string)
{
  EVAL_ARG()
  {
    int32_t in_val = child_res->get_int32();
    char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
    int64_t len = 0;
    if (OB_FAIL(ObTimeConverter::date_to_str(in_val, buf, sizeof(buf), len))) {
      LOG_WARN("date_to_str failed", K(ret));
    } else {
      ObString in_str(len, buf);
      if (OB_FAIL(common_copy_string(expr, in_str, ctx, res_datum))) {
        LOG_WARN("common_copy_string failed", K(ret));
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(date, bit)
{
  EVAL_ARG()
  {
    int32_t in_val = child_res->get_int32();
    char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
    int64_t len = 0;
    if (OB_FAIL(ObTimeConverter::date_to_str(in_val, buf, sizeof(buf), len))) {
      LOG_WARN("date_to_str failed", K(ret));
    } else {
      ObString in_str(len, buf);
      if (OB_FAIL(common_string_bit(expr, in_str, ctx, res_datum))) {
        LOG_WARN("common_string_bit failed", K(ret));
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(date, json)
{
  EVAL_ARG_FOR_CAST_TO_JSON()
  {
    int32_t in_val = child_res->get_date();
    ObObjType in_type = expr.args_[0]->datum_meta_.type_;
    ObTime ob_time(DT_TYPE_DATE);
    if (OB_FAIL(ObTimeConverter::date_to_ob_time(in_val, ob_time))) {
      LOG_WARN("fail to create ob time from date failed", K(ret), K(in_type), K(in_val));
    } else {
      ObJsonDatetime j_date(ObJsonNodeType::J_DATE, ob_time);
      ObIJsonBase *j_base = &j_date;
      common::ObArenaAllocator &temp_allocator = ctx.get_reset_tmp_alloc();
      ObString raw_bin;

      if (OB_FAIL(j_base->get_raw_binary(raw_bin, &temp_allocator))) {
        LOG_WARN("fail to get date json binary", K(ret), K(in_type), K(in_val));
      } else {
        ret = common_copy_string(expr, raw_bin, ctx, res_datum);
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(year, int)
{
  EVAL_ARG()
  {
    uint8_t in_val = child_res->get_uint8();
    int64_t out_val = 0;
    ObObjType out_type = expr.datum_meta_.type_;
    if (OB_FAIL(common_year_int(expr, out_type, in_val, out_val))) {
      LOG_WARN("common_year_int failed", K(ret));
    } else {
      res_datum.set_int(out_val);
    }
  }
  return ret;
}

CAST_FUNC_NAME(year, uint)
{
  EVAL_ARG()
  {
    DEF_IN_OUT_VAL(uint8_t, uint64_t, 0);
    int64_t val_int = 0;
    ObObjType out_type = expr.datum_meta_.type_;
    if (OB_FAIL(ObTimeConverter::year_to_int(in_val, val_int))) {
      LOG_WARN("year_to_int failed", K(ret));
    } else {
      out_val = static_cast<uint64_t>(val_int);
      if (out_type < ObUSmallIntType && CAST_FAIL(uint_range_check(out_type, val_int, out_val))) {
        LOG_WARN("uint_range_check failed", K(ret));
      } else {
        res_datum.set_uint(out_val);
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(year, float)
{
  EVAL_ARG()
  {
    uint8_t in_val = child_res->get_uint8();
    int64_t val_int = 0;
    if (OB_FAIL(ObTimeConverter::year_to_int(in_val, val_int))) {
      LOG_WARN("year_to_int failed", K(ret), K(in_val));
    } else {
      res_datum.set_float(static_cast<float>(val_int));
    }
  }
  return ret;
}

CAST_FUNC_NAME(year, double)
{
  EVAL_ARG()
  {
    uint8_t in_val = child_res->get_uint8();
    int64_t val_int = 0;
    if (OB_FAIL(ObTimeConverter::year_to_int(in_val, val_int))) {
      LOG_WARN("year_to_int failed", K(ret), K(in_val));
    } else {
      res_datum.set_double(static_cast<double>(val_int));
    }
  }
  return ret;
}

CAST_FUNC_NAME(year, number)
{
  EVAL_ARG()
  {
    uint8_t in_val = child_res->get_uint8();
    int64_t val_int = 0;
    ObNumStackOnceAlloc tmp_alloc;
    number::ObNumber number;
    if (OB_FAIL(common_year_int(expr, ObIntType, in_val, val_int))) {
      LOG_WARN("common_year_int failed", K(ret), K(in_val));
    } else if (OB_FAIL(common_int_number(expr, val_int, tmp_alloc, number))) {
      LOG_WARN("common_int_number failed", K(ret), K(val_int));
    } else {
      res_datum.set_number(number);
    }
  }
  return ret;
}

CAST_FUNC_NAME(year, string)
{
  EVAL_ARG()
  {
    char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
    int64_t len = 0;
    uint8_t in_val = child_res->get_uint8();
    if (OB_FAIL(ObTimeConverter::year_to_str(in_val, buf, sizeof(buf), len))) {
      LOG_WARN("year_to_str failed", K(ret), K(in_val));
    } else {
      ObString in_str(len, buf);
      if (OB_FAIL(common_copy_string(expr, in_str, ctx, res_datum))) {
        LOG_WARN("common_copy_string failed", K(ret));
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(year, datetime)
{
  EVAL_ARG()
  {
    uint8_t in_val = child_res->get_uint8();
    int64_t val_int = 0;
    if (OB_FAIL(common_year_int(expr, ObIntType, in_val, val_int))) {
      LOG_WARN("common_year_int failed", K(ret), K(in_val));
    } else if (OB_FAIL(common_int_datetime(expr, val_int, ctx, res_datum))) {
      LOG_WARN("common_int_datetime failed", K(ret), K(val_int));
    }
  }
  return ret;
}

CAST_FUNC_NAME(year, date)
{
  EVAL_ARG()
  {
    uint8_t in_val = child_res->get_uint8();
    int64_t val_int = 0;
    if (OB_FAIL(common_year_int(expr, ObIntType, in_val, val_int))) {
      LOG_WARN("common_year_int failed", K(ret), K(in_val));
    } else if (OB_FAIL(common_int_date(expr, val_int, res_datum))) {
      LOG_WARN("common_int_date failed", K(ret), K(val_int));
    }
  }
  return ret;
}

CAST_FUNC_NAME(year, bit)
{
  EVAL_ARG()
  {
    int64_t year_int = 0;
    uint8_t in_val = child_res->get_uint8();
    if (OB_FAIL(ObTimeConverter::year_to_int(in_val, year_int))) {
      LOG_WARN("year_to_int failed", K(ret), K(in_val));
    } else if (OB_FAIL(common_uint_bit(expr, year_int, ctx, res_datum))) {
      LOG_WARN("common_uint_bit failed", K(ret), K(year_int));
    }
  }
  return ret;
}

CAST_FUNC_NAME(year, json)
{
  EVAL_ARG_FOR_CAST_TO_JSON()
  {
    uint8_t in_val = child_res->get_year();
    ObObjType in_type = expr.args_[0]->datum_meta_.type_;
    int64_t full_year = 0;
    if (OB_FAIL(ObTimeConverter::year_to_int(in_val, full_year))) {
      LOG_WARN("convert year to int failed in year to json convert", K(ret), K(in_val));
    } else {
      ObJsonInt j_year(full_year);
      ObIJsonBase *j_base = &j_year;
      common::ObArenaAllocator &temp_allocator = ctx.get_reset_tmp_alloc();
      ObString raw_bin;
      
      if (OB_FAIL(j_base->get_raw_binary(raw_bin, &temp_allocator))) {
        LOG_WARN("fail to get year json binary", K(ret), K(in_type), K(in_val));
      } else {
        ret = common_copy_string(expr, raw_bin, ctx, res_datum);
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(bit, int)
{
  EVAL_ARG()
  {
    DEF_IN_OUT_VAL(uint64_t, int64_t, static_cast<uint64_t>(in_val));
    ObObjType out_type = expr.datum_meta_.type_;
    if (out_type < ObIntType && CM_NEED_RANGE_CHECK(expr.extra_) &&
        CAST_FAIL(int_range_check(out_type, in_val, out_val))) {
      LOG_WARN("int_range_check failed", K(ret));
    } else {
      res_datum.set_int(out_val);
    }
  }
  return ret;
}

CAST_FUNC_NAME(bit, uint)
{
  EVAL_ARG()
  {
    int warning = OB_SUCCESS;
    uint64_t out_val = child_res->get_uint();
    ObObjType out_type = expr.datum_meta_.type_;
    if (out_type < ObUInt64Type && CM_NEED_RANGE_CHECK(expr.extra_) && CAST_FAIL(uint_upper_check(out_type, out_val))) {
      LOG_WARN("uint_upper_check failed", K(ret));
    } else {
      SET_RES_UINT(out_val);
    }
  }
  return ret;
}

CAST_FUNC_NAME(bit, float)
{
  EVAL_ARG()
  {
    uint64_t in_val = child_res->get_uint();
    res_datum.set_float(static_cast<float>(in_val));
  }
  return ret;
}

CAST_FUNC_NAME(bit, double)
{
  EVAL_ARG()
  {
    uint64_t in_val = child_res->get_uint();
    res_datum.set_double(static_cast<double>(in_val));
  }
  return ret;
}

CAST_FUNC_NAME(bit, number)
{
  EVAL_ARG()
  {
    int64_t in_val = child_res->get_uint();
    ObNumStackOnceAlloc tmp_alloc;
    number::ObNumber number;
    if (OB_FAIL(number.from(in_val, tmp_alloc))) {
      LOG_WARN("number.from failed", K(ret), K(in_val));
    } else {
      res_datum.set_number(number);
    }
  }
  return ret;
}

CAST_FUNC_NAME(bit, datetime)
{
  EVAL_ARG()
  {
    GET_SESSION()
    {
      DEF_IN_OUT_VAL(uint64_t, int64_t, 0);
      if (CM_IS_COLUMN_CONVERT(expr.extra_)) {
        // if cast mode is column convert, using bit as int64 to do cast.
        int64_t int64 = 0;
        if (OB_FAIL(common_uint_int(expr, ObIntType, in_val, ctx, int64))) {
          LOG_WARN("common_uint_int failed", K(ret));
        } else if (OB_UNLIKELY(0 > int64)) {
          ret = OB_INVALID_DATE_FORMAT;
          LOG_WARN("invalid date", K(ret), K(int64));
        } else {
          ObTimeConvertCtx cvrt_ctx(session->get_timezone_info(), ObTimestampType == expr.datum_meta_.type_);
          if (CAST_FAIL(ObTimeConverter::int_to_datetime(int64, 0, cvrt_ctx, out_val))) {
            LOG_WARN("int_datetime failed", K(ret), K(int64));
          }
        }
      } else {
        // using bit as char array to do cast.
        const int32_t BUF_LEN = (OB_MAX_BIT_LENGTH + 7) / 8;
        int64_t pos = 0;
        char buf[BUF_LEN] = {0};
        ObLengthSemantics length = expr.args_[0]->datum_meta_.length_semantics_;
        if (OB_FAIL(bit_to_char_array(in_val, length, buf, BUF_LEN, pos))) {
          LOG_WARN("fail to store val", K(buf), K(BUF_LEN), K(in_val), K(pos));
        } else {
          ObObjType out_type = expr.datum_meta_.type_;
          ObString str(pos, buf);
          ObTimeConvertCtx cvrt_ctx(session->get_timezone_info(), ObTimestampType == out_type);
          ObScale res_scale;
          if (CAST_FAIL(ObTimeConverter::str_to_datetime(str, cvrt_ctx, out_val, &res_scale))) {
            LOG_WARN("str_to_datetime failed", K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        SET_RES_DATETIME(out_val);
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(bit, date)
{
  EVAL_ARG()
  {
    DEF_IN_OUT_VAL(uint64_t, int32_t, 0);
    if (CM_IS_COLUMN_CONVERT(expr.extra_)) {
      // if cast mode is column convert, using bit as int64 to do cast.
      int64_t int64 = 0;
      if (OB_FAIL(common_uint_int(expr, ObIntType, in_val, ctx, int64))) {
        LOG_WARN("common_uint_int failed", K(ret));
      } else if (CAST_FAIL(ObTimeConverter::int_to_date(int64, out_val))) {
        LOG_WARN("int_to_date failed", K(ret), K(int64), K(out_val));
      }
    } else {
      // using bit as char array to do cast.
      const int32_t BUF_LEN = (OB_MAX_BIT_LENGTH + 7) / 8;
      int64_t pos = 0;
      char buf[BUF_LEN] = {0};
      ObLengthSemantics length = expr.args_[0]->datum_meta_.length_semantics_;
      if (OB_FAIL(bit_to_char_array(in_val, length, buf, BUF_LEN, pos))) {
        LOG_WARN("fail to store val", K(buf), K(BUF_LEN), K(in_val), K(pos));
      } else {
        ObString str(pos, buf);
        if (CAST_FAIL(ObTimeConverter::str_to_date(str, out_val))) {
          LOG_WARN("str_to_datetime failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      SET_RES_DATE(out_val);
    }
  }
  return ret;
}

CAST_FUNC_NAME(bit, time)
{
  EVAL_ARG()
  {
    DEF_IN_OUT_VAL(uint64_t, int64_t, 0);
    if (CM_IS_COLUMN_CONVERT(expr.extra_)) {
      // if cast mode is column convert, using bit as int64 to do cast.
      int64_t int64 = 0;
      if (OB_FAIL(common_uint_int(expr, ObIntType, in_val, ctx, int64))) {
        LOG_WARN("common_uint_int failed", K(ret), K(in_val));
      } else if (OB_FAIL(common_int_time(expr, int64, res_datum))) {
        LOG_WARN("common_int_time failed", K(ret), K(out_val));
      }
    } else {
      // using bit as char array to do cast.
      const int32_t BUF_LEN = (OB_MAX_BIT_LENGTH + 7) / 8;
      int64_t pos = 0;
      char buf[BUF_LEN] = {0};
      ObLengthSemantics length = expr.args_[0]->datum_meta_.length_semantics_;
      if (OB_FAIL(bit_to_char_array(in_val, length, buf, BUF_LEN, pos))) {
        LOG_WARN("fail to store val", K(buf), K(BUF_LEN), K(in_val), K(pos));
      } else {
        ObString str(pos, buf);
        ObScale res_scale;
        if (CAST_FAIL(ObTimeConverter::str_to_time(str, out_val, &res_scale))) {
          LOG_WARN("str_to_datetime failed", K(ret));
        } else {
          SET_RES_TIME(out_val);
        }
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(bit, year)
{
  EVAL_ARG()
  {
    // same as uint to year
    uint64_t in_val = child_res->get_uint();
    int64_t out_val = 0;
    if (OB_FAIL(common_uint_int(expr, ObIntType, in_val, ctx, out_val))) {
      LOG_WARN("common_uint_int failed", K(ret), K(in_val));
    } else if (OB_FAIL(common_int_year(expr, out_val, res_datum))) {
      LOG_WARN("common_int_year failed", K(ret), K(out_val));
    }
  }
  return ret;
}

CAST_FUNC_NAME(bit, string)
{
  EVAL_ARG()
  {
    uint64_t in_val = child_res->get_uint();
    if (CM_IS_COLUMN_CONVERT(expr.extra_)) {
      // if cast mode is column convert, using bit as int64 to do cast.
      ObFastFormatInt ffi(in_val);
      if (OB_FAIL(common_copy_string_zf(expr, ObString(ffi.length(), ffi.ptr()), ctx, res_datum))) {
        LOG_WARN("common_copy_string_zf failed", K(ret), K(ObString(ffi.length(), ffi.ptr())));
      }
    } else {
      // using bit as char array to do cast.
      const int32_t BUF_LEN = (OB_MAX_BIT_LENGTH + 7) / 8;
      int64_t pos = 0;
      char buf[BUF_LEN] = {0};
      ObLengthSemantics length = expr.args_[0]->datum_meta_.length_semantics_;
      if (OB_FAIL(bit_to_char_array(in_val, length, buf, BUF_LEN, pos))) {
        LOG_WARN("fail to store val", K(ret), K(in_val), K(length), K(buf), K(BUF_LEN), K(pos));
      } else {
        ObString str(pos, buf);
        if (OB_FAIL(common_copy_string(expr, str, ctx, res_datum))) {
          LOG_WARN("common_copy_string failed", K(ret));
        }
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(bit, json)
{
  EVAL_ARG_FOR_CAST_TO_JSON()
  {
    const int32_t BUF_LEN = (OB_MAX_BIT_LENGTH + 7) / 8;
    int64_t pos = 0;
    char buf[BUF_LEN] = {0};
    uint64_t in_val = child_res->get_uint();
    ObLengthSemantics length = expr.args_[0]->datum_meta_.length_semantics_;
    if (OB_FAIL(bit_to_char_array(in_val, length, buf, BUF_LEN, pos))) {
      LOG_WARN("fail to store val", K(ret), K(in_val), K(length), K(buf), K(BUF_LEN), K(pos));
    } else {
      common::ObArenaAllocator &temp_allocator = ctx.get_reset_tmp_alloc();
      common::ObString j_value(pos, buf);
      ObJsonOpaque j_opaque(j_value, ObBitType);
      ObIJsonBase *j_base = &j_opaque;
      ObString raw_bin;
        
      if (OB_FAIL(j_base->get_raw_binary(raw_bin, &temp_allocator))) {
        LOG_WARN("fail to get int json binary", K(ret), K(in_val), K(buf), K(BUF_LEN));
      } else if (OB_FAIL(common_copy_string(expr, raw_bin, ctx, res_datum))) {
        LOG_WARN("common_copy_string failed", K(ret));
      }
    }
  }

  return ret;
}

CAST_FUNC_NAME(enumset, int)
{
  EVAL_ARG()
  {
    uint64_t in_val = child_res->get_enumset();
    int64_t out_val = 0;
    if (OB_FAIL(common_uint_int(expr, ObIntType, in_val, ctx, out_val))) {
      LOG_WARN("common_uint_int failed", K(ret));
    } else {
      res_datum.set_int(out_val);
    }
  }
  return ret;
}

CAST_FUNC_NAME(enumset, uint)
{
  EVAL_ARG()
  {
    DEF_IN_OUT_TYPE();
    uint64_t in_val = child_res->get_enumset();
    if (CAST_FAIL(uint_upper_check(out_type, in_val))) {
      LOG_WARN("int_upper_check failed", K(ret), K(in_val));
    } else {
      res_datum.set_uint(in_val);
    }
  }
  return ret;
}

CAST_FUNC_NAME(enumset, float)
{
  EVAL_ARG()
  {
    uint64_t in_val = child_res->get_enumset();
    res_datum.set_float(static_cast<float>(in_val));
  }
  return ret;
}

CAST_FUNC_NAME(enumset, double)
{
  EVAL_ARG()
  {
    uint64_t in_val = child_res->get_enumset();
    res_datum.set_double(static_cast<double>(in_val));
  }
  return ret;
}

CAST_FUNC_NAME(enumset, number)
{
  EVAL_ARG()
  {
    uint64_t in_val = child_res->get_enumset();
    ObNumStackOnceAlloc tmp_alloc;
    number::ObNumber number;
    if (OB_FAIL(number.from(in_val, tmp_alloc))) {
      LOG_WARN("number.from failed", K(ret), K(in_val));
    } else {
      res_datum.set_number(number);
    }
  }
  return ret;
}

CAST_FUNC_NAME(enumset, year)
{
  EVAL_ARG()
  {
    uint64_t in_val = child_res->get_enumset();
    int64_t val_int = 0;
    if (OB_FAIL(common_uint_int(expr, ObIntType, in_val, ctx, val_int))) {
      LOG_WARN("common_uint_int failed", K(ret), K(in_val));
    } else if (OB_FAIL(common_int_year(expr, val_int, res_datum))) {
      LOG_WARN("common_int_time failed", K(ret), K(val_int));
    }
  }
  return ret;
}

CAST_FUNC_NAME(enumset, bit)
{
  EVAL_ARG()
  {
    uint64_t in_val = child_res->get_enumset();
    if (OB_FAIL(common_uint_bit(expr, in_val, ctx, res_datum))) {
      LOG_WARN("fail to common_uint_bit", K(ret));
    }
  }
  return ret;
}

CAST_FUNC_NAME(enumset_inner, int)
{
  EVAL_ARG()
  {
    ObEnumSetInnerValue inner_value;
    int64_t out_val = 0;
    if (OB_FAIL(child_res->get_enumset_inner(inner_value))) {
      LOG_WARN("failed to inner_value", KPC(child_res), K(ret));
    } else if (OB_FAIL(common_uint_int(expr, ObIntType, inner_value.numberic_value_, ctx, out_val))) {
      LOG_WARN("common_uint_int failed", K(ret));
    } else {
      res_datum.set_int(out_val);
    }
  }
  return ret;
}

CAST_FUNC_NAME(enumset_inner, uint)
{
  EVAL_ARG()
  {
    ObEnumSetInnerValue inner_value;
    if (OB_FAIL(child_res->get_enumset_inner(inner_value))) {
      LOG_WARN("failed to inner_value", KPC(child_res), K(ret));
    } else {
      res_datum.set_uint(inner_value.numberic_value_);
    }
  }
  return ret;
}

CAST_FUNC_NAME(enumset_inner, float)
{
  EVAL_ARG()
  {
    ObEnumSetInnerValue inner_value;
    if (OB_FAIL(child_res->get_enumset_inner(inner_value))) {
      LOG_WARN("failed to inner_value", KPC(child_res), K(ret));
    } else {
      res_datum.set_float(static_cast<float>(inner_value.numberic_value_));
    }
  }
  return ret;
}

CAST_FUNC_NAME(enumset_inner, double)
{
  EVAL_ARG()
  {
    ObEnumSetInnerValue inner_value;
    if (OB_FAIL(child_res->get_enumset_inner(inner_value))) {
      LOG_WARN("failed to inner_value", KPC(child_res), K(ret));
    } else {
      res_datum.set_float(static_cast<double>(inner_value.numberic_value_));
    }
  }
  return ret;
}

CAST_FUNC_NAME(enumset_inner, number)
{
  EVAL_ARG()
  {
    ObEnumSetInnerValue inner_value;
    ObNumStackOnceAlloc tmp_alloc;
    number::ObNumber number;
    if (OB_FAIL(child_res->get_enumset_inner(inner_value))) {
      LOG_WARN("failed to inner_value", KPC(child_res), K(ret));
    } else if (OB_FAIL(number.from(inner_value.numberic_value_, tmp_alloc))) {
      LOG_WARN("number.from failed", K(ret), K(inner_value.numberic_value_));
    } else {
      res_datum.set_number(number);
    }
  }
  return ret;
}

CAST_FUNC_NAME(enumset_inner, year)
{
  EVAL_ARG()
  {
    ObEnumSetInnerValue inner_value;
    int64_t val_int = 0;
    if (OB_FAIL(child_res->get_enumset_inner(inner_value))) {
      LOG_WARN("failed to inner_value", KPC(child_res), K(ret));
    } else if (OB_FAIL(common_uint_int(expr, ObIntType, inner_value.numberic_value_, ctx, val_int))) {
      LOG_WARN("common_uint_int failed", K(ret), K(inner_value.numberic_value_));
    } else if (OB_FAIL(common_int_year(expr, val_int, res_datum))) {
      LOG_WARN("common_int_time failed", K(ret), K(val_int));
    }
  }
  return ret;
}

CAST_FUNC_NAME(enumset_inner, bit)
{
  EVAL_ARG()
  {
    ObEnumSetInnerValue inner_value;
    if (OB_FAIL(child_res->get_enumset_inner(inner_value))) {
      LOG_WARN("failed to inner_value", KPC(child_res), K(ret));
    } else if (OB_FAIL(common_uint_bit(expr, inner_value.numberic_value_, ctx, res_datum))) {
      LOG_WARN("fail to common_uint_bit", K(ret));
    }
  }
  return ret;
}

CAST_FUNC_NAME(enumset_inner, datetime)
{
  EVAL_ARG()
  {
    ObEnumSetInnerValue inner_value;
    if (OB_FAIL(child_res->get_enumset_inner(inner_value))) {
      LOG_WARN("failed to inner_value", KPC(child_res), K(ret));
    } else if (OB_FAIL(common_string_datetime(expr, inner_value.string_value_, ctx, res_datum))) {
      LOG_WARN("failed to common_string_datetime", K(inner_value), K(ret));
    }
  }
  return ret;
}

CAST_FUNC_NAME(enumset_inner, date)
{
  EVAL_ARG()
  {
    ObEnumSetInnerValue inner_value;
    if (OB_FAIL(child_res->get_enumset_inner(inner_value))) {
      LOG_WARN("failed to inner_value", KPC(child_res), K(ret));
    } else if (OB_FAIL(common_string_date(expr, inner_value.string_value_, res_datum))) {
      LOG_WARN("failed to common_string_date", K(inner_value), K(ret));
    }
  }
  return ret;
}

CAST_FUNC_NAME(enumset_inner, time)
{
  EVAL_ARG()
  {
    ObEnumSetInnerValue inner_value;
    if (OB_FAIL(child_res->get_enumset_inner(inner_value))) {
      LOG_WARN("failed to inner_value", KPC(child_res), K(ret));
    } else if (OB_FAIL(common_string_time(expr, inner_value.string_value_, res_datum))) {
      LOG_WARN("failed to common_string_time", K(inner_value), K(ret));
    }
  }
  return ret;
}

CAST_FUNC_NAME(enumset_inner, string)
{
  EVAL_ARG()
  {
    ObEnumSetInnerValue inner_value;
    if (OB_FAIL(child_res->get_enumset_inner(inner_value))) {
      LOG_WARN("failed to inner_value", KPC(child_res), K(ret));
    } else if (OB_FAIL(common_check_convert_string(expr, ctx, inner_value.string_value_, res_datum))) {
      LOG_WARN("fail to common_check_convert_string", K(ret), K(inner_value));
    }
  }
  return ret;
}

CAST_FUNC_NAME(time, int)
{
  EVAL_ARG()
  {
    DEF_IN_OUT_VAL(int64_t, int64_t, 0);
    ObObjType out_type = expr.datum_meta_.type_;
    if (OB_FAIL(ObTimeConverter::time_to_int(in_val, out_val))) {
      LOG_WARN("time_to_int failed", K(ret), K(in_val));
    } else if (out_type < ObInt32Type && CAST_FAIL(int_range_check(out_type, out_val, out_val))) {
      LOG_WARN("int_range_check failed", K(ret), K(out_val));
    } else {
      res_datum.set_int(out_val);
    }
  }
  return ret;
}

CAST_FUNC_NAME(time, uint)
{
  EVAL_ARG()
  {
    DEF_IN_OUT_VAL(int64_t, uint64_t, 0);
    int64_t val_int = 0;
    ObObjType out_type = expr.datum_meta_.type_;
    if (OB_FAIL(ObTimeConverter::time_to_int(in_val, val_int))) {
      LOG_WARN("time_to_int failed", K(ret), K(in_val));
    } else {
      out_val = static_cast<uint64_t>(val_int);
      if (CM_NEED_RANGE_CHECK(expr.extra_) && CAST_FAIL(uint_range_check(out_type, val_int, out_val))) {
        LOG_WARN("int_range_check failed", K(ret), K(val_int));
      } else {
        res_datum.set_uint(out_val);
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(time, float)
{
  EVAL_ARG()
  {
    DEF_IN_OUT_VAL(int64_t, double, 0.0);
    ObObjType out_type = expr.datum_meta_.type_;
    if (OB_FAIL(ObTimeConverter::time_to_double(in_val, out_val))) {
      LOG_WARN("time_to_int failed", K(ret), K(in_val));
    } else if (ObUFloatType == out_type && CAST_FAIL(numeric_negative_check(out_val))) {
      LOG_WARN("int_range_check failed", K(ret), K(out_val));
    } else {
      res_datum.set_float(static_cast<float>(out_val));
    }
  }
  return ret;
}

CAST_FUNC_NAME(time, double)
{
  EVAL_ARG()
  {
    DEF_IN_OUT_VAL(int64_t, double, 0.0);
    ObObjType out_type = expr.datum_meta_.type_;
    if (OB_FAIL(ObTimeConverter::time_to_double(in_val, out_val))) {
      LOG_WARN("time_to_int failed", K(ret), K(in_val));
    } else if (ObUFloatType == out_type && CAST_FAIL(numeric_negative_check(out_val))) {
      LOG_WARN("int_range_check failed", K(ret), K(out_val));
    } else {
      res_datum.set_double(out_val);
    }
  }
  return ret;
}

CAST_FUNC_NAME(time, number)
{
  EVAL_ARG()
  {
    int64_t in_val = child_res->get_uint();
    int warning = OB_SUCCESS;
    char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
    int64_t len = 0;
    ObNumStackOnceAlloc tmp_alloc;
    number::ObNumber number;
    ObScale in_scale = expr.args_[0]->datum_meta_.scale_;
    ObScale res_scale = 0;
    ObPrecision res_precision = 0;
    if (OB_FAIL(ObTimeConverter::time_to_str(in_val, in_scale, buf, sizeof(buf), len, false))) {
      LOG_WARN("time_to_str failed", K(ret), K(in_val));
    } else if (CAST_FAIL(number.from(buf, len, tmp_alloc, &res_precision, &res_scale))) {
      LOG_WARN("number.from failed", K(ret));
    } else {
      res_datum.set_number(number);
    }
  }
  return ret;
}

CAST_FUNC_NAME(time, datetime)
{
  EVAL_ARG()
  {
    GET_SESSION()
    {
      int64_t in_val = child_res->get_int();
      int64_t out_val = 0;
      ObObjType out_type = expr.datum_meta_.type_;
      ObPhysicalPlanCtx* phy_plan_ctx = ctx.exec_ctx_.get_physical_plan_ctx();
      int64_t cur_time = phy_plan_ctx ? phy_plan_ctx->get_cur_time().get_datetime() : 0;
      if (OB_FAIL(
              ObTimeConverter::time_to_datetime(in_val, cur_time, session->get_timezone_info(), out_val, out_type))) {
        LOG_WARN("time_to_datetime failed", K(ret), K(in_val));
      } else {
        res_datum.set_datetime(out_val);
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(time, date)
{
  EVAL_ARG()
  {
    GET_SESSION()
    {
      int32_t out_val = 0;
      ObPhysicalPlanCtx *phy_plan_ctx = ctx.exec_ctx_.get_physical_plan_ctx();
      int64_t cur_time = phy_plan_ctx ? phy_plan_ctx->get_cur_time().get_datetime() : 0;
      int64_t datetime_value = 0;
      int64_t in_val = child_res->get_time();
      ObTimeConvertCtx cvrt_ctx(session->get_timezone_info(), false);
      if (OB_FAIL(ObTimeConverter::time_to_datetime(in_val, cur_time, session->get_timezone_info(),
                    datetime_value, ObDateTimeType))) {
        LOG_WARN("datetime_to_date failed", K(ret), K(cur_time));
      } else if (ObTimeConverter::datetime_to_date(datetime_value, NULL, out_val)) {
        LOG_WARN("date to datetime failed", K(ret), K(datetime_value));
      } else {
        res_datum.set_date(out_val);
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(time, string)
{
  EVAL_ARG()
  {
    int64_t in_val = child_res->get_int();
    char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
    int64_t len = 0;
    ObScale in_scale = expr.args_[0]->datum_meta_.scale_;
    if (OB_FAIL(ObTimeConverter::time_to_str(in_val, in_scale, buf, sizeof(buf), len))) {
      LOG_WARN("time_to_str failed", K(ret), K(in_val));
    } else if (OB_FAIL(common_copy_string(expr, ObString(len, buf), ctx, res_datum))) {
      LOG_WARN("common_copy_string failed", K(ret), K(ObString(len, buf)));
    }
  }
  return ret;
}

CAST_FUNC_NAME(time, bit)
{
  EVAL_ARG()
  {
    int64_t in_val = child_res->get_int();
    char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
    int64_t len = 0;
    ObScale in_scale = expr.args_[0]->datum_meta_.scale_;
    if (OB_FAIL(ObTimeConverter::time_to_str(in_val, in_scale, buf, sizeof(buf), len))) {
      LOG_WARN("time_to_str failed", K(ret), K(in_val));
    } else if (OB_FAIL(common_string_bit(expr, ObString(len, buf), ctx, res_datum))) {
      LOG_WARN("common_string_bit failed", K(ret), K(ObString(len, buf)));
    }
  }
  return ret;
}

CAST_FUNC_NAME(time, json)
{
  EVAL_ARG_FOR_CAST_TO_JSON()
  {
    int64_t in_val = child_res->get_int();
    ObObjType in_type = expr.args_[0]->datum_meta_.type_;
    ObTime ob_time(DT_TYPE_TIME);
    if (OB_FAIL(ObTimeConverter::time_to_ob_time(in_val, ob_time))) {
      LOG_WARN("fail to create ob time from time", K(ret), K(in_type), K(in_val));
    } else {
      ObJsonDatetime j_time(ObJsonNodeType::J_TIME, ob_time);
      ObIJsonBase *j_base = &j_time;
      common::ObArenaAllocator &temp_allocator = ctx.get_reset_tmp_alloc();
      ObString raw_bin;

      if (OB_FAIL(j_base->get_raw_binary(raw_bin, &temp_allocator))) {
        LOG_WARN("fail to get time json binary", K(ret), K(in_type), K(in_val));
      } else {
        ret = common_copy_string(expr, raw_bin, ctx, res_datum);
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(otimestamp, datetime)
{
  EVAL_ARG()
  {
    GET_SESSION()
    {
      int64_t usec = 0;
      ObObjType in_type = expr.args_[0]->datum_meta_.type_;
      ObOTimestampData in_val;
      if (OB_FAIL(common_construct_otimestamp(in_type, *child_res, in_val))) {
        LOG_WARN("common_construct_otimestamp failed", K(ret));
      } else if (OB_FAIL(ObTimeConverter::otimestamp_to_odate(in_type, in_val, session->get_timezone_info(), usec))) {
        LOG_WARN("fail to timestamp_tz_to_timestamp", K(ret));
      } else {
        ObTimeConverter::trunc_datetime(OB_MAX_DATE_PRECISION, usec);
        res_datum.set_datetime(usec);
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(otimestamp, string)
{
  EVAL_ARG()
  {
    GET_SESSION()
    {
      ObObjType in_type = expr.args_[0]->datum_meta_.type_;
      ObOTimestampData in_val;
      char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
      int64_t len = 0;
      ObScale in_scale = expr.args_[0]->datum_meta_.scale_;
      const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(session);
      if (OB_FAIL(common_construct_otimestamp(in_type, *child_res, in_val))) {
        LOG_WARN("common_construct_otimestamp failed", K(ret));
      } else if (OB_FAIL(ObTimeConverter::otimestamp_to_str(
                     in_val, dtc_params, in_scale, in_type, buf, OB_CAST_TO_VARCHAR_MAX_LENGTH, len))) {
        LOG_WARN("failed to convert otimestamp to string", K(ret));
      } else {
        ObString in_str(sizeof(buf), static_cast<int32_t>(len), buf);
        if (OB_FAIL(common_check_convert_string(expr, ctx, in_str, res_datum))) {
          LOG_WARN("fail to common_check_convert_string", K(ret), K(in_str));
        }
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(otimestamp, otimestamp)
{
  EVAL_ARG()
  {
    GET_SESSION()
    {
      DEF_IN_OUT_TYPE();
      ObOTimestampData in_val;
      ObOTimestampData out_val;
      if (OB_FAIL(common_construct_otimestamp(in_type, *child_res, in_val))) {
        LOG_WARN("common_construct_otimestamp failed", K(ret));
      } else if (ObTimestampNanoType == in_type) {
        if (OB_FAIL(ObTimeConverter::odate_to_otimestamp(
                in_val.time_us_, session->get_timezone_info(), out_type, out_val))) {
          LOG_WARN("fail to odate_to_otimestamp", K(ret), K(out_type));
        } else {
          out_val.time_ctx_.tail_nsec_ = in_val.time_ctx_.tail_nsec_;
        }
      } else if (ObTimestampNanoType == out_type) {
        if (OB_FAIL(ObTimeConverter::otimestamp_to_odate(
                in_type, in_val, session->get_timezone_info(), *(int64_t*)&out_val.time_us_))) {
          LOG_WARN("fail to otimestamp_to_odate", K(ret), K(out_type));
        } else {
          out_val.time_ctx_.tail_nsec_ = in_val.time_ctx_.tail_nsec_;
        }
      } else {
        if (OB_FAIL(ObTimeConverter::otimestamp_to_otimestamp(
                in_type, in_val, session->get_timezone_info(), out_type, out_val))) {
          LOG_WARN("fail to otimestamp_to_otimestamp", K(ret), K(out_type));
        }
      }
      if (OB_SUCC(ret)) {
        if (ObTimestampTZType == out_type) {
          SET_RES_OTIMESTAMP(out_val);
        } else {
          SET_RES_OTIMESTAMP_10BYTE(out_val);
        }
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(otimestamp, lob)
{
  EVAL_ARG()
  {
    GET_SESSION()
    {
      ObObjType in_type = expr.args_[0]->datum_meta_.type_;
      ObOTimestampData in_val;
      char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
      int64_t len = 0;
      ObScale in_scale = expr.args_[0]->datum_meta_.scale_;
      if (OB_FAIL(common_construct_otimestamp(in_type, *child_res, in_val))) {
        LOG_WARN("common_construct_otimestamp failed", K(ret));
      } else if (OB_FAIL(ObTimeConverter::otimestamp_to_str(in_val,
                     session->get_timezone_info(),
                     in_scale,
                     in_type,
                     buf,
                     OB_CAST_TO_VARCHAR_MAX_LENGTH,
                     len))) {
        LOG_WARN("failed to convert otimestamp to string", K(ret));
      } else {
        ObString in_str(sizeof(buf), static_cast<int32_t>(len), buf);
        ObString res_str;
        if (OB_FAIL(common_check_convert_string(expr, ctx, in_str, res_datum))) {
          LOG_WARN("fail to common_check_convert_string", K(ret), K(in_str));
        } else if (OB_FAIL(copy_datum_str_with_tmp_alloc(ctx, res_datum, res_str))) {
          LOG_WARN("copy datum string with tmp allocator failed", K(ret));
        } else if (OB_FAIL(common_string_lob(expr, res_str, ctx, NULL, res_datum))) {
          LOG_WARN("cast string to lob failed", K(ret));
        }
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(raw, string)
{
  EVAL_ARG()
  {
    ObString in_str(child_res->len_, child_res->ptr_);
    OZ(ObDatumHexUtils::rawtohex(expr, in_str, ctx, res_datum));
  }
  return ret;
}

CAST_FUNC_NAME(raw, longtext)
{
  EVAL_ARG()
  {
    ObString in_str(child_res->len_, child_res->ptr_);
    if (CS_TYPE_BINARY != expr.datum_meta_.cs_type_) {
      // raw to clob
      OZ(ObDatumHexUtils::rawtohex(expr, in_str, ctx, res_datum));
    } else {
      // raw to blob
      res_datum.set_string(in_str.ptr(), in_str.length());
    }
  }
  return ret;
}

CAST_FUNC_NAME(raw, lob)
{
  EVAL_ARG()
  {
    ObString res_str;
    if (OB_FAIL(raw_longtext(expr, ctx, res_datum))) {
      LOG_WARN("raw_longtext failed", K(ret));
    } else if (OB_FAIL(copy_datum_str_with_tmp_alloc(ctx, res_datum, res_str))) {
      LOG_WARN("copy datum string with tmp allocator failed", K(ret));
    } else if (OB_FAIL(common_string_lob(expr, res_str, ctx, NULL, res_datum))) {
      LOG_WARN("cast string to lob failed", K(ret));
    }
  }
  return ret;
}

CAST_FUNC_NAME(raw, raw)
{
  EVAL_ARG()
  {
    ObString in_str(child_res->len_, child_res->ptr_);
    OZ(common_check_convert_string(expr, ctx, in_str, res_datum));
  }
  return ret;
}

CAST_FUNC_NAME(interval, string)
{
  EVAL_ARG()
  {
    ObObjType in_type = expr.args_[0]->datum_meta_.type_;
    ObScale in_scale = expr.args_[0]->datum_meta_.scale_;
    char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
    int64_t len = 0;
    bool is_explicit_cast = CM_IS_EXPLICIT_CAST(expr.extra_);
    if (ob_is_interval_ym(in_type)) {
      ObIntervalYMValue in_val(child_res->get_interval_nmonth());
      if (OB_FAIL(ObTimeConverter::interval_ym_to_str(
              in_val, in_scale, buf, OB_CAST_TO_VARCHAR_MAX_LENGTH, len, is_explicit_cast))) {
        LOG_WARN("interval_ym_to_str failed", K(ret));
      }
    } else {
      ObIntervalDSValue in_val(child_res->get_interval_ds());
      if (OB_FAIL(ObTimeConverter::interval_ds_to_str(
              in_val, in_scale, buf, OB_CAST_TO_VARCHAR_MAX_LENGTH, len, is_explicit_cast))) {
        LOG_WARN("interval_ym_to_str failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ObString in_str(len, buf);
      if (OB_FAIL(common_check_convert_string(expr, ctx, in_str, res_datum))) {
        LOG_WARN("fail to common_check_convert_string", K(ret), K(in_str));
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(interval, interval)
{
  EVAL_ARG()
  {
    ObObjType in_type = expr.args_[0]->datum_meta_.type_;
    ObObjType out_type = expr.datum_meta_.type_;
    if (in_type != out_type) {
      ret = cast_inconsistent_types(expr, ctx, res_datum);
    } else {
      ret = OB_NOT_SUPPORTED;
    }
  }
  return ret;
}

CAST_FUNC_NAME(rowid, string)
{
  EVAL_ARG()
  {
    ObURowIDData urowid_data(child_res->len_, reinterpret_cast<const uint8_t*>(child_res->ptr_));
    char* base64_buf = NULL;
    int64_t base64_buf_size = urowid_data.needed_base64_buffer_size();
    int64_t pos = 0;
    if (OB_ISNULL(base64_buf = expr.get_str_res_mem(ctx, base64_buf_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret));
    } else if (OB_FAIL(urowid_data.get_base64_str(base64_buf, base64_buf_size, pos))) {
      LOG_WARN("failed to get base64 str", K(ret));
    } else {
      ObString in_str(pos, base64_buf);
      if (OB_FAIL(common_check_convert_string(expr, ctx, in_str, res_datum))) {
        LOG_WARN("fail to common_check_convert_string", K(ret), K(in_str));
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(rowid, rowid)
{
  EVAL_ARG()
  {
    ObObjType in_type = expr.args_[0]->datum_meta_.type_;
    ObObjType out_type = expr.datum_meta_.type_;
    if (in_type != out_type) {
      ret = cast_inconsistent_types(expr, ctx, res_datum);
    } else {
      res_datum.set_urowid(ObURowIDData(child_res->len_, reinterpret_cast<const uint8_t*>(child_res->ptr_)));
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////
// Lob -> XXX
CAST_FUNC_NAME(lob, int)
{
  EVAL_STRING_ARG()
  {
    const ObLobLocator& lob_locator = child_res->get_lob_locator();
    ObString in_str(lob_locator.payload_size_, lob_locator.get_payload_ptr());
    const bool is_str_int_cast = true;
    OZ(common_string_int(expr, expr.extra_, in_str, is_str_int_cast, res_datum));
  }
  return ret;
}

CAST_FUNC_NAME(lob, uint)
{
  EVAL_STRING_ARG()
  {
    const ObLobLocator& lob_locator = child_res->get_lob_locator();
    ObString in_str(lob_locator.payload_size_, lob_locator.get_payload_ptr());
    const bool is_str_int_cast = true;
    OZ(common_string_uint(expr, in_str, is_str_int_cast, res_datum));
  }
  return ret;
}

CAST_FUNC_NAME(lob, float)
{
  EVAL_STRING_ARG()
  {
    const ObLobLocator& lob_locator = child_res->get_lob_locator();
    ObString in_str(lob_locator.payload_size_, lob_locator.get_payload_ptr());
    float out_val = 0;
    OZ(common_string_float(expr, in_str, out_val));
    OX(res_datum.set_float(out_val));
  }
  return ret;
}

CAST_FUNC_NAME(lob, double)
{
  EVAL_STRING_ARG()
  {
    const ObLobLocator& lob_locator = child_res->get_lob_locator();
    ObString in_str(lob_locator.payload_size_, lob_locator.get_payload_ptr());
    DEF_IN_OUT_TYPE();
    OZ(common_string_double(expr, in_type, out_type, in_str, res_datum));
  }
  return ret;
}

CAST_FUNC_NAME(lob, number)
{
  EVAL_STRING_ARG()
  {
    const ObLobLocator& lob_locator = child_res->get_lob_locator();
    ObString in_str(lob_locator.payload_size_, lob_locator.get_payload_ptr());
    number::ObNumber nmb;
    ObNumStackOnceAlloc tmp_alloc;
    OZ(common_string_number(expr, ObString(child_res->len_, child_res->ptr_), tmp_alloc, nmb));
    OX(res_datum.set_number(nmb));
  }
  return ret;
}

CAST_FUNC_NAME(lob, datetime)
{
  EVAL_STRING_ARG()
  {
    const ObLobLocator& lob_locator = child_res->get_lob_locator();
    ObString in_str(lob_locator.payload_size_, lob_locator.get_payload_ptr());
    OZ(common_string_datetime(expr, in_str, ctx, res_datum));
  }
  return ret;
}

CAST_FUNC_NAME(lob, date)
{
  EVAL_STRING_ARG()
  {
    const ObLobLocator& lob_locator = child_res->get_lob_locator();
    ObString in_str(lob_locator.payload_size_, lob_locator.get_payload_ptr());
    OZ(common_string_date(expr, in_str, res_datum));
  }
  return ret;
}

CAST_FUNC_NAME(lob, time)
{
  EVAL_STRING_ARG()
  {
    const ObLobLocator& lob_locator = child_res->get_lob_locator();
    ObString in_str(lob_locator.payload_size_, lob_locator.get_payload_ptr());
    OZ(common_string_time(expr, in_str, res_datum));
  }
  return ret;
}

CAST_FUNC_NAME(lob, year)
{
  EVAL_STRING_ARG()
  {
    const ObLobLocator& lob_locator = child_res->get_lob_locator();
    ObString in_str(lob_locator.payload_size_, lob_locator.get_payload_ptr());
    OZ(common_string_year(expr, in_str, res_datum));
  }
  return ret;
}

CAST_FUNC_NAME(lob, bit)
{
  EVAL_STRING_ARG()
  {
    const ObLobLocator& lob_locator = child_res->get_lob_locator();
    ObString in_str(lob_locator.payload_size_, lob_locator.get_payload_ptr());
    OZ(common_string_bit(expr, in_str, ctx, res_datum));
  }
  return ret;
}

CAST_FUNC_NAME(lob, string)
{
  EVAL_ARG()
  {
    const ObLobLocator& lob_locator = child_res->get_lob_locator();
    ObString in_str(lob_locator.payload_size_, lob_locator.get_payload_ptr());
    ObObjType in_type = ObLongTextType;
    ObObjType out_type = expr.datum_meta_.type_;
    ObCollationType in_cs_type = expr.args_[0]->datum_meta_.cs_type_;
    ObCollationType out_cs_type = expr.datum_meta_.cs_type_;
    OZ(common_string_string(expr, in_type, in_cs_type, out_type, out_cs_type, in_str, ctx, res_datum));
  }
  return ret;
}

CAST_FUNC_NAME(lob, otimestamp)
{
  EVAL_STRING_ARG()
  {
    const ObLobLocator& lob_locator = child_res->get_lob_locator();
    ObString in_str(lob_locator.payload_size_, lob_locator.get_payload_ptr());
    OZ(common_string_otimestamp(expr, in_str, ctx, res_datum));
  }
  return ret;
}

CAST_FUNC_NAME(lob, raw)
{
  EVAL_STRING_ARG()
  {
    const ObLobLocator& lob_locator = child_res->get_lob_locator();
    ObString in_str(lob_locator.payload_size_, lob_locator.get_payload_ptr());
    OZ(ObDatumHexUtils::hextoraw_string(expr, in_str, ctx, res_datum));
  }
  return ret;
}

CAST_FUNC_NAME(lob, interval)
{
  EVAL_STRING_ARG()
  {
    const ObLobLocator& lob_locator = child_res->get_lob_locator();
    ObString in_str(lob_locator.payload_size_, lob_locator.get_payload_ptr());
    OZ(common_string_interval(expr, in_str, ctx, res_datum));
  }
  return ret;
}

CAST_FUNC_NAME(lob, rowid)
{
  EVAL_STRING_ARG()
  {
    const ObLobLocator& lob_locator = child_res->get_lob_locator();
    ObString in_str(lob_locator.payload_size_, lob_locator.get_payload_ptr());
    OZ(common_string_rowid(expr, in_str, ctx, res_datum));
  }
  return ret;
}

CAST_FUNC_NAME(lob, lob)
{
  EVAL_STRING_ARG()
  {
    const ObLobLocator& lob_locator = child_res->get_lob_locator();
    ObString in_str(lob_locator.payload_size_, lob_locator.get_payload_ptr());
    OZ(common_string_lob(expr, in_str, ctx, &lob_locator, res_datum));
  }
  return ret;
}

CAST_FUNC_NAME(lob, json)
{
  EVAL_STRING_ARG()
  {
    const ObLobLocator &lob_locator = child_res->get_lob_locator();
    ObString in_str(lob_locator.payload_size_, lob_locator.get_payload_ptr());
    OZ(common_string_json(expr, in_str, ctx, res_datum));
  }
  return ret;
}

////////////////////////////////////////////////////////////
// Json -> XXX
CAST_FUNC_NAME(json, int)
{
  EVAL_STRING_ARG()
  {
    int warning = OB_SUCCESS;
    int64_t out_val = 0;
    ObObjType out_type = expr.datum_meta_.type_;
    const uint64_t extra = CM_UNSET_STRING_INTEGER_TRUNC(CM_SET_WARN_ON_FAIL(expr.extra_));
    ObString j_text = child_res->get_string();
    ObJsonBin j_bin(j_text.ptr(), j_text.length());
    ObIJsonBase *j_base = &j_bin;

    if (OB_FAIL(j_bin.reset_iter())) {
      LOG_WARN("failed to reset json bin iter", K(ret), K(j_text));
    } else if (CAST_FAIL(j_base->to_int(out_val))) {
      LOG_WARN("fail to cast json to int type", K(ret), K(j_text));
      ret = OB_ERR_INVALID_JSON_VALUE_FOR_CAST;
      LOG_USER_ERROR(OB_ERR_INVALID_JSON_VALUE_FOR_CAST);
    } else if (out_type < ObIntType && CAST_FAIL_CM(int_range_check(out_type, out_val, out_val), extra)) {
      LOG_WARN("range check failed", K(ret), K(out_type), K(out_val));
    } else {
      SET_RES_INT(out_val);
    }
  }
  return ret;
}

CAST_FUNC_NAME(json, uint)
{
  EVAL_STRING_ARG()
  {
    int warning = OB_SUCCESS;
    uint64_t out_val = 0;
    ObObjType out_type = expr.datum_meta_.type_;
    ObString j_text = child_res->get_string();
    ObJsonBin j_bin(j_text.ptr(), j_text.length());
    ObIJsonBase *j_base = &j_bin;

    if (OB_FAIL(j_bin.reset_iter())) {
      LOG_WARN("failed to reset json bin iter", K(ret), K(j_text));
    } else if (CAST_FAIL(j_base->to_uint(out_val))) {
      LOG_WARN("fail to cast json to uint type", K(ret), K(j_text));
      ret = OB_ERR_INVALID_JSON_VALUE_FOR_CAST;
      LOG_USER_ERROR(OB_ERR_INVALID_JSON_VALUE_FOR_CAST);
    } else if (out_type < ObUInt64Type
               && CM_NEED_RANGE_CHECK(expr.extra_) 
               && CAST_FAIL(uint_upper_check(out_type, out_val))) {
      LOG_WARN("uint_upper_check failed", K(ret), K(out_type), K(out_val));
    } else {
      SET_RES_UINT(out_val);
    }
  }
  return ret;
}

CAST_FUNC_NAME(json, double)
{
  EVAL_STRING_ARG()
  {
    int warning = OB_SUCCESS;
    double out_val = 0.0;
    ObObjType out_type = expr.datum_meta_.type_;
    ObString j_text = child_res->get_string();
    ObJsonBin j_bin(j_text.ptr(), j_text.length());
    ObIJsonBase *j_base = &j_bin;

    if (OB_FAIL(j_bin.reset_iter())) {
      LOG_WARN("failed to reset json bin iter", K(ret), K(j_text));
    } else if (CAST_FAIL(j_base->to_double(out_val))) {
      LOG_WARN("fail to cast json to double type", K(ret), K(j_text));
      ret = OB_ERR_INVALID_JSON_VALUE_FOR_CAST;
      LOG_USER_ERROR(OB_ERR_INVALID_JSON_VALUE_FOR_CAST);
    } else if (ObUDoubleType == out_type && CAST_FAIL(numeric_negative_check(out_val))) {
      LOG_WARN("numeric_negative_check failed", K(ret), K(out_val));
    } else {
      SET_RES_DOUBLE(out_val);
    }
  }
  return ret;
}

CAST_FUNC_NAME(json, float)
{
  EVAL_STRING_ARG()
  {
    int warning = OB_SUCCESS;
    double tmp_val = 0.0;
    float out_val = 0.0;
    ObObjType out_type = expr.datum_meta_.type_;
    ObString j_text = child_res->get_string();
    ObJsonBin j_bin(j_text.ptr(), j_text.length());
    ObIJsonBase *j_base = &j_bin;

    if (OB_FAIL(j_bin.reset_iter())) {
      LOG_WARN("failed to reset json bin iter", K(ret), K(j_text));
    } else if (CAST_FAIL(j_base->to_double(tmp_val))) {
      LOG_WARN("fail to cast json to float type", K(ret), K(j_text));
      ret = OB_ERR_INVALID_JSON_VALUE_FOR_CAST;
      LOG_USER_ERROR(OB_ERR_INVALID_JSON_VALUE_FOR_CAST);
    } else {
      out_val = static_cast<float>(tmp_val);
      if (CAST_FAIL(real_range_check(out_type, tmp_val, out_val))) {
        LOG_WARN("real_range_check failed", K(ret), K(tmp_val), K(out_val));
      } else {
        SET_RES_DOUBLE(out_val);
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(json, number)
{
  EVAL_STRING_ARG()
  {
    int warning = OB_SUCCESS;
    number::ObNumber out_val;
    ObObjType out_type = expr.datum_meta_.type_;
    common::ObArenaAllocator &temp_allocator = ctx.get_reset_tmp_alloc();
    ObString j_text = child_res->get_string();
    ObJsonBin j_bin(j_text.ptr(), j_text.length());
    ObIJsonBase *j_base = &j_bin;

    if (OB_FAIL(j_bin.reset_iter())) {
      LOG_WARN("failed to reset json bin iter", K(ret), K(j_text));
    } else if (CAST_FAIL(j_base->to_number(&temp_allocator, out_val))) {
      LOG_WARN("fail to cast json to number type", K(ret), K(j_text));
      ret = OB_ERR_INVALID_JSON_VALUE_FOR_CAST;
      LOG_USER_ERROR(OB_ERR_INVALID_JSON_VALUE_FOR_CAST);
    } else if (ObUNumberType == out_type && CAST_FAIL(numeric_negative_check(out_val))) {
      LOG_WARN("numeric_negative_check failed", K(ret), K(out_val));
    } else {
      res_datum.set_number(out_val);
    }
  }
  return ret;
}

CAST_FUNC_NAME(json, datetime)
{
  EVAL_STRING_ARG()
  {
    int warning = OB_SUCCESS;
    int64_t out_val;
    ObObjType out_type = expr.datum_meta_.type_;
    ObString j_text = child_res->get_string();
    ObJsonBin j_bin(j_text.ptr(), j_text.length());
    ObIJsonBase *j_base = &j_bin;

    if (OB_FAIL(j_bin.reset_iter())) {
      LOG_WARN("failed to reset json bin iter", K(ret), K(j_text));
    } else if (CAST_FAIL(j_base->to_datetime(out_val))) {
      LOG_WARN("fail to cast json to datetime type", K(ret), K(j_text));
      ret = OB_ERR_INVALID_JSON_VALUE_FOR_CAST;
      LOG_USER_ERROR(OB_ERR_INVALID_JSON_VALUE_FOR_CAST);
    } else {
      SET_RES_DATETIME(out_val);
    }
  }
  return ret;
}

CAST_FUNC_NAME(json, date)
{
  EVAL_STRING_ARG()
  {
    int warning = OB_SUCCESS;
    int32_t out_val;
    ObObjType out_type = expr.datum_meta_.type_;
    ObString j_text = child_res->get_string();
    ObJsonBin j_bin(j_text.ptr(), j_text.length());
    ObIJsonBase *j_base = &j_bin;

    if (OB_FAIL(j_bin.reset_iter())) {
      LOG_WARN("failed to reset json bin iter", K(ret), K(j_text));
    } else if (CAST_FAIL(j_base->to_date(out_val))) {
      LOG_WARN("fail to cast json to date type", K(ret), K(j_text));
      ret = OB_ERR_INVALID_JSON_VALUE_FOR_CAST;
      LOG_USER_ERROR(OB_ERR_INVALID_JSON_VALUE_FOR_CAST);
    } else {
      SET_RES_DATE(out_val);
    }
  }
  return ret;
}

CAST_FUNC_NAME(json, time)
{
  EVAL_STRING_ARG()
  {
    int warning = OB_SUCCESS;
    int64_t out_val;
    ObObjType out_type = expr.datum_meta_.type_;
    ObString j_text = child_res->get_string();
    ObJsonBin j_bin(j_text.ptr(), j_text.length());
    ObIJsonBase *j_base = &j_bin;

    if (OB_FAIL(j_bin.reset_iter())) {
      LOG_WARN("failed to reset json bin iter", K(ret), K(j_text));
    } else if (CAST_FAIL(j_base->to_time(out_val))) {
      LOG_WARN("fail to cast json to time type", K(ret), K(j_text));
      ret = OB_ERR_INVALID_JSON_VALUE_FOR_CAST;
      LOG_USER_ERROR(OB_ERR_INVALID_JSON_VALUE_FOR_CAST);
    } else {
      SET_RES_TIME(out_val);
    }
  }
  return ret;
}

CAST_FUNC_NAME(json, year)
{
  EVAL_STRING_ARG()
  {
    int warning = OB_SUCCESS;
    uint8_t out_val = 0;
    int64_t int_val;
    ObObjType out_type = expr.datum_meta_.type_;
    ObString j_text = child_res->get_string();
    ObJsonBin j_bin(j_text.ptr(), j_text.length());
    ObIJsonBase *j_base = &j_bin;

    if (OB_FAIL(j_bin.reset_iter())) {
      LOG_WARN("failed to reset json bin iter", K(ret), K(j_text));
    } else if (CAST_FAIL(j_base->to_int(int_val, false, true))) {
      LOG_WARN("fail to cast json as year", K(ret), K(j_text));
      ret = OB_ERR_INVALID_JSON_VALUE_FOR_CAST;
      LOG_USER_ERROR(OB_ERR_INVALID_JSON_VALUE_FOR_CAST);
    } else if (CAST_FAIL(ObTimeConverter::int_to_year(int_val, out_val))){
      LOG_WARN("fail to cast json int to year type", K(ret), K(int_val));
    } else {
      if (lib::is_mysql_mode() && (warning == OB_DATA_OUT_OF_RANGE)) {
        res_datum.set_null(); // not change the behavior of int_year 
      } else {
        SET_RES_YEAR(out_val);
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(json, string)
{
  EVAL_STRING_ARG()
  {
    int warning = OB_SUCCESS;
    ObObjType out_type = expr.datum_meta_.type_;
    common::ObArenaAllocator &temp_allocator = ctx.get_reset_tmp_alloc();
    ObString j_text = child_res->get_string();
    ObJsonBin j_bin(j_text.ptr(), j_text.length());
    ObIJsonBase *j_base = &j_bin;
    ObJsonBuffer j_buf(&temp_allocator);

    if (OB_FAIL(j_bin.reset_iter())) {
      LOG_WARN("failed to reset json bin iter", K(ret), K(j_text));
    } else if (CAST_FAIL(j_base->print(j_buf, true))) {
      LOG_WARN("fail to convert json to string", K(ret), K(j_text));
      ret = OB_ERR_INVALID_JSON_VALUE_FOR_CAST;
      LOG_USER_ERROR(OB_ERR_INVALID_JSON_VALUE_FOR_CAST);
    } else {
      ObObjType in_type = ObLongTextType;
      ObObjType out_type = expr.datum_meta_.type_;
      ObString temp_str_val(j_buf.length(), j_buf.ptr());
      // if transfer json to binary directly, should modify binary type in others too
      ObCollationType in_cs_type = expr.args_[0]->datum_meta_.cs_type_;
      ObCollationType out_cs_type = expr.datum_meta_.cs_type_;
      bool is_need_string_string_convert = ((CS_TYPE_BINARY == out_cs_type)
          || (ObCharset::charset_type_by_coll(in_cs_type) != ObCharset::charset_type_by_coll(out_cs_type)));
      if (!is_need_string_string_convert) {
        // same collation type, just string copy result;
        OZ(common_copy_string(expr, temp_str_val, ctx, res_datum));
      } else {
        // should do collation convert;
        OZ(common_string_string(expr, in_type, in_cs_type, out_type,
                                out_cs_type, temp_str_val, ctx, res_datum));
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(json, bit)
{
  EVAL_STRING_ARG()
  {
    int warning = OB_SUCCESS;
    ObString j_bin_str = child_res->get_string();
    ObJsonBin j_bin(j_bin_str.ptr(), j_bin_str.length());
    ObIJsonBase *j_base = &j_bin;
    uint64_t out_val;
    ObObjType out_type = expr.datum_meta_.type_;

    if (OB_FAIL(j_bin.reset_iter())) {
      LOG_WARN("failed to reset json bin iter", K(ret), K(j_bin_str));
    } else if (CAST_FAIL(j_base->to_bit(out_val))) {
      LOG_WARN("fail to cast json as bit", K(ret), K(j_bin_str));
      ret = OB_ERR_INVALID_JSON_VALUE_FOR_CAST;
      LOG_USER_ERROR(OB_ERR_INVALID_JSON_VALUE_FOR_CAST);
    } else {
      SET_RES_BIT(out_val);
    }
  }
  return ret;
}

CAST_FUNC_NAME(json, otimestamp)
{
  EVAL_STRING_ARG()
  {
    int warning = OB_SUCCESS;
    int64_t datetime_val;
    ObObjType out_type = expr.datum_meta_.type_;
    ObString j_text = child_res->get_string();
    ObJsonBin j_bin(j_text.ptr(), j_text.length());
    ObIJsonBase *j_base = &j_bin;

    if (OB_FAIL(j_bin.reset_iter())) {
      LOG_WARN("failed to reset json bin iter", K(ret), K(j_text));
    } else if (CAST_FAIL(j_base->to_datetime(datetime_val))) {
      LOG_WARN("fail to cast json as datetime", K(ret), K(j_text));
      ret = OB_ERR_INVALID_JSON_VALUE_FOR_CAST;
      LOG_USER_ERROR(OB_ERR_INVALID_JSON_VALUE_FOR_CAST);
    } else {
      GET_SESSION()
      {
        const ObTimeZoneInfo *tz_info = session->get_timezone_info();
        ObOTimestampData out_val;
        if (OB_FAIL(ObTimeConverter::odate_to_otimestamp(datetime_val, tz_info, out_type, out_val))) {
          LOG_WARN("fail to timestamp_to_timestamp_tz", K(ret), K(datetime_val), K(out_type));
        } else {
          if (ObTimestampTZType == out_type) {
            SET_RES_OTIMESTAMP(out_val);
          } else {
            SET_RES_OTIMESTAMP_10BYTE(out_val);
          }
        }
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(json, json)
{
  EVAL_STRING_ARG()
  {
    ObString out_val = child_res->get_string();
    res_datum.set_string(out_val);
  }
  return ret;
}

int get_accuracy_from_parse_node(const ObExpr& expr, ObEvalCtx& ctx, ObAccuracy& accuracy, ObObjType& dest_type)
{
  int ret = OB_SUCCESS;
  ObDatum* dst_type_dat = NULL;
  if (OB_UNLIKELY(2 != expr.arg_cnt_) || OB_ISNULL(expr.args_) || OB_ISNULL(expr.args_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected expr", K(ret), K(expr.arg_cnt_), KP(expr.args_));
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, dst_type_dat))) {
    LOG_WARN("eval dst type datum failed", K(ret));
  } else {
    ParseNode node;
    node.value_ = dst_type_dat->get_int();
    dest_type = static_cast<ObObjType>(node.int16_values_[0]);
    ObObjTypeClass dest_tc = ob_obj_type_class(dest_type);
    if (ObStringTC == dest_tc) {
      // parser will abort all negative number
      // if length < 0 means DEFAULT_STR_LENGTH or OUT_OF_STR_LEN.
      accuracy.set_full_length(node.int32_values_[1], expr.datum_meta_.length_semantics_, lib::is_oracle_mode());
    } else if (ObRawTC == dest_tc) {
      accuracy.set_length(node.int32_values_[1]);
    } else if (ObTextTC == dest_tc || ObJsonTC == dest_tc) {
      accuracy.set_length(
          node.int32_values_[1] < 0 ? ObAccuracy::DDL_DEFAULT_ACCURACY[dest_type].get_length() : node.int32_values_[1]);
    } else if (ObIntervalTC == dest_tc) {
      if (OB_UNLIKELY(!ObIntervalScaleUtil::scale_check(node.int16_values_[3]) ||
                      !ObIntervalScaleUtil::scale_check(node.int16_values_[2]))) {
        ret = OB_ERR_DATETIME_INTERVAL_PRECISION_OUT_OF_RANGE;
      } else {
        ObScale scale =
            (dest_type == ObIntervalYMType)
                ? ObIntervalScaleUtil::interval_ym_scale_to_ob_scale(static_cast<int8_t>(node.int16_values_[3]))
                : ObIntervalScaleUtil::interval_ds_scale_to_ob_scale(
                      static_cast<int8_t>(node.int16_values_[2]), static_cast<int8_t>(node.int16_values_[3]));
        accuracy.set_scale(scale);
      }
    } else {
      const ObAccuracy& def_acc = ObAccuracy::DDL_DEFAULT_ACCURACY2[lib::is_oracle_mode()][dest_type];
      if (ObNumberType == dest_type && 0 == node.int16_values_[2]) {
        accuracy.set_precision(def_acc.get_precision());
      } else {
        accuracy.set_precision(node.int16_values_[2]);
      }
      accuracy.set_scale(node.int16_values_[3]);
      if (lib::is_oracle_mode() && ObDoubleType == dest_type) {
        accuracy.set_accuracy(def_acc.get_precision());
      }
    }
  }
  return ret;
}

int uint_to_enum(const uint64_t input_value, const ObIArray<ObString>& str_values, const ObCastMode& cast_mode,
    int& warning, uint64_t& output_value)
{
  int ret = OB_SUCCESS;
  uint64_t value = input_value;
  if (OB_UNLIKELY(0 == value || value > str_values.count())) {
    if (CM_IS_WARN_ON_FAIL(cast_mode)) {
      value = 0;
      warning = OB_ERR_DATA_TRUNCATED;
      LOG_INFO("input value out of range, set zero", K(input_value), K(str_values.count()), K(warning));
    } else {
      ret = OB_ERR_DATA_TRUNCATED;
      LOG_WARN("input value out of range", K(input_value), K(str_values.count()), K(ret));
    }
  }
  output_value = value;
  LOG_DEBUG("finish uint_to_enum", K(ret), K(input_value), K(str_values), K(output_value), K(lbt()));
  return ret;
}

int uint_to_set(const uint64_t input_value, const ObIArray<ObString>& str_values, const ObCastMode& cast_mode,
    int& warning, uint64_t& output_value)
{
  int ret = OB_SUCCESS;
  uint64_t value = input_value;
  int64_t val_cnt = str_values.count();
  if (val_cnt >= 64) {
    // do nothing
  } else if (val_cnt < 64 && value > ((1ULL << val_cnt) - 1)) {
    if (CM_IS_WARN_ON_FAIL(cast_mode)) {
      value = value & ((1ULL << val_cnt) - 1);
      warning = OB_ERR_DATA_TRUNCATED;
      LOG_INFO("input value out of range", K(input_value), K(value), K(val_cnt), K(warning));
    } else {
      ret = OB_ERR_DATA_TRUNCATED;
      LOG_WARN("input value out of range", K(input_value), K(value), K(val_cnt), K(ret));
    }
  }
  output_value = value;
  LOG_DEBUG("finish uint_to_set", K(ret), K(input_value), K(str_values), K(output_value), K(lbt()));
  return ret;
}

CAST_ENUMSET_FUNC_NAME(int, enum)
{
  EVAL_ARG() {
    if (child_res->is_null()) {
      res_datum.set_null();
    } else {
      int warning = 0;
      uint64_t val_uint = static_cast<uint64_t>(child_res->get_int());
      uint64_t value = 0;
      ret = uint_to_enum(val_uint, str_values, cast_mode, warning, value);
      SET_RES_ENUM(value);
    }
  }
  return ret;
}

CAST_ENUMSET_FUNC_NAME(int, set)
{
  EVAL_ARG() {
    if (child_res->is_null()) {
      res_datum.set_null();
    } else {
      int warning = 0;
      uint64_t val_uint = static_cast<uint64_t>(child_res->get_int());
      uint64_t value = 0;
      ret = uint_to_set(val_uint, str_values, cast_mode, warning, value);
      SET_RES_SET(value);
    }
  }
  return ret;
}

CAST_ENUMSET_FUNC_NAME(uint, enum)
{
  EVAL_ARG() {
    if (child_res->is_null()) {
      res_datum.set_null();
    } else {
      int warning = 0;
      uint64_t val_uint = child_res->get_uint();
      uint64_t value = 0;
      ret = uint_to_enum(val_uint, str_values, cast_mode, warning, value);
      SET_RES_ENUM(value);
    }
  }
  return ret;
}

CAST_ENUMSET_FUNC_NAME(uint, set)
{
  EVAL_ARG() {
    if (child_res->is_null()) {
      res_datum.set_null();
    } else {
      int warning = 0;
      uint64_t val_uint = child_res->get_uint();
      uint64_t value = 0;
      ret = uint_to_set(val_uint, str_values, cast_mode, warning, value);
      SET_RES_SET(value);
    }
  }
  return ret;
}


CAST_ENUMSET_FUNC_NAME(float, enum)
{
  EVAL_ARG() {
    if (child_res->is_null()) {
      res_datum.set_null();
    } else {
      int warning = 0;
      uint64_t val_uint = static_cast<uint64_t>(static_cast<int64_t>(child_res->get_float()));
      uint64_t value = 0;
      ret = uint_to_enum(val_uint, str_values, cast_mode, warning, value);
      SET_RES_ENUM(value);
    }
  }
  return ret;
}

CAST_ENUMSET_FUNC_NAME(float, set)
{
  EVAL_ARG() {
    if (child_res->is_null()) {
      res_datum.set_null();
    } else {
      int warning = 0;
      uint64_t val_uint = static_cast<uint64_t>(static_cast<int64_t>(child_res->get_float()));
      uint64_t value = 0;
      ret = uint_to_set(val_uint, str_values, cast_mode, warning, value);
      SET_RES_SET(value);
    }
  }
  return ret;
}

CAST_ENUMSET_FUNC_NAME(double, enum)
{
  EVAL_ARG() {
    if (child_res->is_null()) {
      res_datum.set_null();
    } else {
      int warning = 0;
      uint64_t val_uint = static_cast<uint64_t>(static_cast<int64_t>(child_res->get_double()));
      uint64_t value = 0;
      ret = uint_to_enum(val_uint, str_values, cast_mode, warning, value);
      SET_RES_ENUM(value);
    }
  }
  return ret;
}

CAST_ENUMSET_FUNC_NAME(double, set)
{
  EVAL_ARG() {
    if (child_res->is_null()) {
      res_datum.set_null();
    } else {
      int warning = 0;
      uint64_t val_uint = static_cast<uint64_t>(static_cast<int64_t>(child_res->get_double()));
      uint64_t value = 0;
      ret = uint_to_set(val_uint, str_values, cast_mode, warning, value);
      SET_RES_SET(value);
    }
  }
  return ret;
}

CAST_ENUMSET_FUNC_NAME(number, enum)
{
  int ret = OB_SUCCESS;
  int warning = 0;
  if (OB_FAIL(number_double(expr, ctx, res_datum))) {
    LOG_WARN("fail to cast number to double", K(expr), K(ret));
  } else if (res_datum.is_null()) {
  } else {
    uint64_t val_uint = static_cast<uint64_t>(static_cast<int64_t>(res_datum.get_double()));
    uint64_t value = 0;
    ret = uint_to_enum(val_uint, str_values, cast_mode, warning, value);
    SET_RES_ENUM(value);
  }
  return ret;
}

CAST_ENUMSET_FUNC_NAME(number, set)
{
  int ret = OB_SUCCESS;
  int warning = 0;
  if (OB_FAIL(number_double(expr, ctx, res_datum))) {
    LOG_WARN("fail to cast number to double", K(expr), K(ret));
  } else if (res_datum.is_null()) {
  } else {
    uint64_t val_uint = static_cast<uint64_t>(static_cast<int64_t>(res_datum.get_double()));
    uint64_t value = 0;
    ret = uint_to_set(val_uint, str_values, cast_mode, warning, value);
    SET_RES_SET(value);
  }
  return ret;
}

int string_to_enum(ObIAllocator& alloc, const ObString& orig_in_str, const ObCollationType in_cs_type,
    const ObIArray<ObString>& str_values, const uint64_t cast_mode, const ObExpr& expr, int& warning,
    uint64_t& output_value)
{
  int ret = OB_SUCCESS;
  const ObCollationType cs_type = expr.obj_meta_.get_collation_type();
  uint64_t value = 0;
  int32_t pos = 0;
  ObString in_str;
  OZ(ObCharset::charset_convert(alloc, orig_in_str, in_cs_type, cs_type, in_str));
  int32_t no_sp_len = static_cast<int32_t>(ObCharset::strlen_byte_no_sp(cs_type, in_str.ptr(), in_str.length()));
  ObString no_sp_val(0, static_cast<ObString::obstr_size_t>(no_sp_len), in_str.ptr());
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(find_type(str_values, cs_type, no_sp_val, pos))) {
    LOG_WARN("fail to find type", K(str_values), K(cs_type), K(no_sp_val), K(in_str), K(pos), K(ret));
  } else if (OB_UNLIKELY(pos < 0)) {
    // Bug30666903: check implicit cast logic to handle number cases
    if (!in_str.is_numeric()) {
      ret = OB_ERR_DATA_TRUNCATED;
    } else {
      int err = 0;
      int64_t val_cnt = str_values.count();
      value = ObCharset::strntoull(in_str.ptr(), in_str.length(), 10, &err);
      if (err != 0 || value > val_cnt) {
        value = 0;
        ret = OB_ERR_DATA_TRUNCATED;
        LOG_WARN("input value out of range", K(val_cnt), K(ret), K(err));
      }
    }
    if (OB_FAIL(ret) && CM_IS_WARN_ON_FAIL(cast_mode)) {
      warning = ret;
      ret = OB_SUCCESS;
    }
  } else {
    value = pos + 1;  // enum start from 1
  }
  output_value = value;
  LOG_DEBUG("finish string_enum", K(ret), K(in_str), K(str_values), K(expr), K(output_value), K(lbt()));
  return ret;
}

CAST_ENUMSET_FUNC_NAME(string, enum)
{
  EVAL_ARG()
  {
    int warning = 0;
    const ObString in_str = child_res->get_string();
    uint64_t value = 0;
    ret = string_to_enum(ctx.get_reset_tmp_alloc(),
        in_str,
        expr.args_[0]->datum_meta_.cs_type_,
        str_values,
        cast_mode,
        expr,
        warning,
        value);
    SET_RES_ENUM(value);
  }
  return ret;
}

int string_to_set(ObIAllocator& alloc, const ObString& orig_in_str, const ObCollationType in_cs_type,
    const ObIArray<ObString>& str_values, const uint64_t cast_mode, const ObExpr& expr, int& warning,
    uint64_t& output_value)
{
  int ret = OB_SUCCESS;
  uint64_t value = 0;
  ObString in_str;
  const ObCollationType cs_type = expr.obj_meta_.get_collation_type();
  OZ(ObCharset::charset_convert(alloc, orig_in_str, in_cs_type, cs_type, in_str));
  if (OB_FAIL(ret)) {
  } else if (in_str.empty()) {
    // do noting
  } else {
    bool is_last_value = false;
    const ObString& sep = ObCharsetUtils::get_const_str(cs_type, ',');
    int32_t pos = 0;
    const char* remain = in_str.ptr();
    int64_t remain_len = ObCharset::strlen_byte_no_sp(cs_type, in_str.ptr(), in_str.length());
    ObString val_str;
    do {
      pos = 0;
      const char* sep_loc = NULL;
      if (NULL == (sep_loc = static_cast<const char*>(memmem(remain, remain_len, sep.ptr(), sep.length())))) {
        is_last_value = true;
        val_str.assign_ptr(remain, remain_len);
        if (OB_FAIL(find_type(str_values, cs_type, val_str, pos))) {
          LOG_WARN("fail to find type", K(str_values), K(cs_type), K(in_str), K(pos), K(ret));
        }
      } else {
        val_str.assign_ptr(remain, sep_loc - remain);
        remain_len = remain_len - (sep_loc - remain + sep.length());
        remain = sep_loc + sep.length();
        if (OB_FAIL(find_type(str_values, cs_type, val_str, pos))) {
          LOG_WARN("fail to find type", K(str_values), K(cs_type), K(val_str), K(pos), K(ret));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_UNLIKELY(pos < 0)) {  // not found
        if (CM_IS_WARN_ON_FAIL(cast_mode)) {
          warning = OB_ERR_DATA_TRUNCATED;
          LOG_INFO(
              "input value out of range, and set out value zero", K(pos), K(expr), K(val_str), K(in_str), K(warning));
        } else {
          ret = OB_ERR_DATA_TRUNCATED;
          LOG_WARN("data truncate", K(pos), K(expr), K(val_str), K(in_str), K(ret));
        }
      } else {
        pos %= 64;  // In MySQL, if the value is duplicated, the value_count can be greater than 64
        value |= (1ULL << pos);
      }
    } while (OB_SUCC(ret) && !is_last_value);
  }

  // Bug30666903: check implicit cast logic to handle number cases
  if (in_str.is_numeric() &&
      (OB_ERR_DATA_TRUNCATED == ret || (OB_ERR_DATA_TRUNCATED == warning && CM_IS_WARN_ON_FAIL(cast_mode)))) {
    int err = 0;
    value = ObCharset::strntoull(in_str.ptr(), in_str.length(), 10, &err);
    if (err == 0) {
      ret = OB_SUCCESS;
      uint32_t val_cnt = str_values.count();
      if (OB_UNLIKELY(val_cnt <= 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect val_cnt", K(val_cnt), K(ret));
      } else if (val_cnt >= 64) {  // do nothing
      } else if (val_cnt < 64 && value > ((1ULL << val_cnt) - 1)) {
        value = 0;
        ret = OB_ERR_DATA_TRUNCATED;
        LOG_WARN("input value out of range", K(val_cnt), K(ret));
      }
      if (OB_FAIL(ret) && CM_IS_WARN_ON_FAIL(cast_mode)) {
        warning = OB_ERR_DATA_TRUNCATED;
        ret = OB_SUCCESS;
      }
    } else {
      value = 0;
    }
  }

  output_value = value;
  LOG_DEBUG("finish string_set", K(ret), K(in_str), K(str_values), K(expr), K(output_value), K(lbt()));
  return ret;
}

CAST_ENUMSET_FUNC_NAME(string, set)
{
  EVAL_ARG()
  {
    int warning = 0;
    const ObString in_str = child_res->get_string();
    uint64_t value = 0;
    ret = string_to_set(ctx.get_reset_tmp_alloc(),
        in_str,
        expr.args_[0]->datum_meta_.cs_type_,
        str_values,
        cast_mode,
        expr,
        warning,
        value);
    SET_RES_SET(value);
  }
  return ret;
}

CAST_ENUMSET_FUNC_NAME(datetime, enum)
{
  EVAL_ARG()
  {
    int warning = 0;
    char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
    int64_t len = 0;
    int64_t in_val = child_res->get_int();
    if (OB_FAIL(common_datetime_string(expr.args_[0]->datum_meta_.type_,
            ObVarcharType,
            expr.args_[0]->datum_meta_.scale_,
            false,
            in_val,
            ctx,
            buf,
            sizeof(buf),
            len))) {
      LOG_WARN("common_datetime_string failed", K(ret));
    } else {
      ObString in_str(len, buf);
      uint64_t value = 0;
      ret = string_to_enum(ctx.get_reset_tmp_alloc(),
          in_str,
          ObCharset::get_system_collation(),
          str_values,
          cast_mode,
          expr,
          warning,
          value);
      SET_RES_ENUM(value);
    }
  }
  return ret;
}

CAST_ENUMSET_FUNC_NAME(datetime, set)
{
  EVAL_ARG()
  {
    int warning = 0;
    char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
    int64_t len = 0;
    int64_t in_val = child_res->get_int();
    if (OB_FAIL(common_datetime_string(expr.args_[0]->datum_meta_.type_,
            ObVarcharType,
            expr.args_[0]->datum_meta_.scale_,
            false,
            in_val,
            ctx,
            buf,
            sizeof(buf),
            len))) {
      LOG_WARN("common_datetime_string failed", K(ret));
    } else {
      ObString in_str(len, buf);
      uint64_t value = 0;
      ret = string_to_set(ctx.get_reset_tmp_alloc(),
          in_str,
          ObCharset::get_system_collation(),
          str_values,
          cast_mode,
          expr,
          warning,
          value);
      SET_RES_SET(value);
    }
  }
  return ret;
}

CAST_ENUMSET_FUNC_NAME(date, enum)
{
  EVAL_ARG()
  {
    int warning = 0;
    int32_t in_val = child_res->get_int32();
    char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
    int64_t len = 0;
    if (OB_FAIL(ObTimeConverter::date_to_str(in_val, buf, sizeof(buf), len))) {
      LOG_WARN("date_to_str failed", K(ret));
    } else {
      ObString in_str(len, buf);
      uint64_t value = 0;
      ret = string_to_enum(ctx.get_reset_tmp_alloc(),
          in_str,
          ObCharset::get_system_collation(),
          str_values,
          cast_mode,
          expr,
          warning,
          value);
      SET_RES_ENUM(value);
    }
  }
  return ret;
}

CAST_ENUMSET_FUNC_NAME(date, set)
{
  EVAL_ARG()
  {
    int warning = 0;
    int32_t in_val = child_res->get_int32();
    char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
    int64_t len = 0;
    if (OB_FAIL(ObTimeConverter::date_to_str(in_val, buf, sizeof(buf), len))) {
      LOG_WARN("date_to_str failed", K(ret));
    } else {
      ObString in_str(len, buf);
      uint64_t value = 0;
      ret = string_to_set(ctx.get_reset_tmp_alloc(),
          in_str,
          ObCharset::get_system_collation(),
          str_values,
          cast_mode,
          expr,
          warning,
          value);
      SET_RES_SET(value);
    }
  }
  return ret;
}

CAST_ENUMSET_FUNC_NAME(time, enum)
{
  EVAL_ARG()
  {
    int warning = 0;
    int64_t in_val = child_res->get_int();
    char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
    int64_t len = 0;
    ObScale in_scale = expr.args_[0]->datum_meta_.scale_;
    if (OB_FAIL(ObTimeConverter::time_to_str(in_val, in_scale, buf, sizeof(buf), len))) {
      LOG_WARN("time_to_str failed", K(ret), K(in_val));
    } else {
      ObString in_str(len, buf);
      uint64_t value = 0;
      ret = string_to_enum(ctx.get_reset_tmp_alloc(),
          in_str,
          ObCharset::get_system_collation(),
          str_values,
          cast_mode,
          expr,
          warning,
          value);
      SET_RES_ENUM(value);
    }
  }
  return ret;
}

CAST_ENUMSET_FUNC_NAME(time, set)
{
  EVAL_ARG()
  {
    int warning = 0;
    int64_t in_val = child_res->get_int();
    char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
    int64_t len = 0;
    ObScale in_scale = expr.args_[0]->datum_meta_.scale_;
    if (OB_FAIL(ObTimeConverter::time_to_str(in_val, in_scale, buf, sizeof(buf), len))) {
      LOG_WARN("time_to_str failed", K(ret), K(in_val));
    } else {
      ObString in_str(len, buf);
      uint64_t value = 0;
      ret = string_to_set(ctx.get_reset_tmp_alloc(),
          in_str,
          ObCharset::get_system_collation(),
          str_values,
          cast_mode,
          expr,
          warning,
          value);
      SET_RES_SET(value);
    }
  }
  return ret;
}

CAST_ENUMSET_FUNC_NAME(year, enum)
{
  EVAL_ARG()
  {
    int warning = 0;
    uint8_t in_val = child_res->get_uint8();
    int64_t tmp_int = 0;
    if (OB_FAIL(ObTimeConverter::year_to_int(in_val, tmp_int))) {
      LOG_WARN("year_to_int failed", K(ret));
    } else {
      uint64_t val_uint = static_cast<uint64_t>(tmp_int);
      uint64_t value = 0;
      ret = uint_to_enum(val_uint, str_values, cast_mode, warning, value);
      SET_RES_ENUM(value);
    }
  }
  return ret;
}

CAST_ENUMSET_FUNC_NAME(year, set)
{
  EVAL_ARG()
  {
    int warning = 0;
    uint8_t in_val = child_res->get_uint8();
    int64_t tmp_int = 0;
    if (OB_FAIL(ObTimeConverter::year_to_int(in_val, tmp_int))) {
      LOG_WARN("year_to_int failed", K(ret));
    } else {
      uint64_t val_uint = static_cast<uint64_t>(tmp_int);
      uint64_t value = 0;
      ret = uint_to_set(val_uint, str_values, cast_mode, warning, value);
      SET_RES_SET(value);
    }
  }
  return ret;
}

CAST_ENUMSET_FUNC_NAME(bit, enum)
{
  EVAL_ARG()
  {
    int warning = 0;
    uint64_t val_uint = child_res->get_bit();
    uint64_t value = 0;
    ret = uint_to_enum(val_uint, str_values, cast_mode, warning, value);
    SET_RES_ENUM(value);
  }
  return ret;
}

CAST_ENUMSET_FUNC_NAME(bit, set)
{
  EVAL_ARG()
  {
    int warning = 0;
    uint64_t val_uint = child_res->get_bit();
    uint64_t value = 0;
    ret = uint_to_set(val_uint, str_values, cast_mode, warning, value);
    SET_RES_SET(value);
  }
  return ret;
}

// exclude varchar/char type
int anytype_anytype_explicit(const sql::ObExpr& expr, sql::ObEvalCtx& ctx, sql::ObDatum& res_datum)
{
  EVAL_ARG()
  {
    int warning = OB_SUCCESS;
    ObObjType out_type = ObMaxType;
    ObAccuracy out_acc;
    // tmp_datum is for datum_accuracy_check().
    ObDatum tmp_datum = res_datum;
    if (OB_ISNULL(expr.inner_functions_) || 1 != expr.inner_func_cnt_ || OB_ISNULL(expr.inner_functions_[0])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("inner_functinos is NULL or inner_func_cnt_ is not valid",
          K(ret),
          KP(expr.inner_functions_),
          K(expr.inner_func_cnt_));
    } else if (OB_FAIL(((ObExpr::EvalFunc)(expr.inner_functions_[0]))(expr, ctx, tmp_datum))) {
      LOG_WARN("inner cast failed", K(ret));
    } else if (OB_FAIL(get_accuracy_from_parse_node(expr, ctx, out_acc, out_type))) {
      LOG_WARN("get accuracy failed", K(ret));
    } else if (OB_FAIL(datum_accuracy_check(expr, expr.extra_, ctx, out_acc, tmp_datum, res_datum, warning))) {
      LOG_WARN("accuracy check failed", K(ret));
    }
  }
  return ret;
}

// padding %padding_cnt character, we also need to convert collation type here.
// eg: select cast('abc' as nchar(100)) from dual;
//     the space must be in utf16, because dst_type is nchar
int padding_char_for_cast(
    int64_t padding_cnt, const ObCollationType& padding_cs_type, ObIAllocator& alloc, ObString& padding_res)
{
  int ret = OB_SUCCESS;
  padding_res.reset();
  const ObCharsetType& cs = ObCharset::charset_type_by_coll(padding_cs_type);
  char padding_char = (CHARSET_BINARY == cs) ? OB_PADDING_BINARY : OB_PADDING_CHAR;
  int64_t padding_str_size = sizeof(padding_char) * padding_cnt;
  char* padding_str_ptr = reinterpret_cast<char*>(alloc.alloc(padding_str_size));
  if (OB_ISNULL(padding_str_ptr)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory failed", K(ret));
  } else if (CHARSET_BINARY == cs) {
    MEMSET(padding_str_ptr, padding_char, padding_str_size);
    padding_res.assign_ptr(padding_str_ptr, padding_str_size);
  } else {
    MEMSET(padding_str_ptr, padding_char, padding_str_size);
    ObString padding_str(padding_str_size, padding_str_ptr);
    if (OB_FAIL(ObExprUtil::convert_string_collation(
            padding_str, ObCharset::get_system_collation(), padding_res, padding_cs_type, alloc))) {
      LOG_WARN("convert padding str collation faield", K(ret), K(padding_str), K(padding_cs_type));
    }
  }
  LOG_DEBUG("pad char done", K(ret), K(padding_cnt), K(padding_cs_type), K(padding_res));
  return ret;
}

int anytype_to_varchar_char_explicit(const sql::ObExpr& expr, sql::ObEvalCtx& ctx, sql::ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  GET_SESSION()
  {
    int warning = OB_SUCCESS;  // useless
    ObAccuracy out_acc;
    ObObjType out_type = ObMaxType;
    ObDatum tmp_datum = res_datum;
    if (OB_ISNULL(expr.inner_functions_) || 1 != expr.inner_func_cnt_ || OB_ISNULL(expr.inner_functions_[0])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("inner_functions is NULL or inner_func_cnt_ is not valid",
          K(ret),
          KP(expr.inner_functions_),
          K(expr.inner_func_cnt_));
    } else if (OB_FAIL(((ObExpr::EvalFunc)(expr.inner_functions_[0]))(expr, ctx, tmp_datum))) {
      LOG_WARN("inner cast failed", K(ret));
    } else if (tmp_datum.is_null()) {
      res_datum.set_null();
    } else if (OB_FAIL(get_accuracy_from_parse_node(expr, ctx, out_acc, out_type))) {
      LOG_WARN("get accuracy failed", K(ret));
    } else if (OB_FAIL(datum_accuracy_check(expr, expr.extra_, ctx, out_acc, tmp_datum, res_datum, warning))) {
      if (ob_is_string_type(expr.datum_meta_.type_) && OB_ERR_DATA_TOO_LONG == ret) {
        ObDatumMeta src_meta;
        if (ObDatumCast::is_implicit_cast(*expr.args_[0])) {
          const ObExpr& grand_child = *(expr.args_[0]->args_[0]);
          if (OB_UNLIKELY(ObDatumCast::is_implicit_cast(grand_child))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("too many cast expr, max is 2", K(ret), K(expr));
          } else {
            src_meta = grand_child.datum_meta_;
          }
        } else {
          src_meta = expr.args_[0]->datum_meta_;
        }
        if (OB_LIKELY(OB_ERR_DATA_TOO_LONG == ret)) {
          if ((ob_is_character_type(src_meta.type_, src_meta.cs_type_) ||
                  ob_is_clob(src_meta.type_, src_meta.cs_type_) ||
                  ob_is_clob_locator(src_meta.type_, src_meta.cs_type_)) &&
              lib::is_oracle_mode()) {
            ret = OB_SUCCESS;
          } else {
            ret = OB_ERR_TRUNCATED_WRONG_VALUE;
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (res_datum.is_null()) {
        // do nothing
      } else if (-1 == out_acc.get_length()) {
        // do nothing
      } else if (0 > out_acc.get_length()) {
        ret = OB_ERR_TOO_LONG_COLUMN_LENGTH;
        LOG_WARN("accuracy too long", K(ret), K(out_acc.get_length()));
      } else {
        bool has_result = false;
        if (!lib::is_oracle_mode()) {
          int64_t max_allowed_packet = 0;
          if (OB_FAIL(session->get_max_allowed_packet(max_allowed_packet))) {
            if (OB_ENTRY_NOT_EXIST == ret) {  // for compatibility with server before 1470
              ret = OB_SUCCESS;
              max_allowed_packet = OB_MAX_VARCHAR_LENGTH;
            } else {
              LOG_WARN("Failed to get max allow packet size", K(ret));
            }
          } else if (out_acc.get_length() > max_allowed_packet && out_acc.get_length() <= INT32_MAX) {
            res_datum.set_null();
            has_result = true;
            LOG_USER_WARN(OB_ERR_FUNC_RESULT_TOO_LARGE, "cast", static_cast<int>(max_allowed_packet));
          } else if (out_acc.get_length() == 0) {
            res_datum.set_string(NULL, 0);
            has_result = true;
          }
        }

        if (OB_SUCC(ret) && !has_result) {
          ObString text(res_datum.len_, res_datum.ptr_);
          ObCollationType out_cs_type = expr.datum_meta_.cs_type_;
          bool oracle_char_byte_exceed = false;
          ObLengthSemantics ls = lib::is_oracle_mode() ? expr.datum_meta_.length_semantics_ : LS_CHAR;
          int32_t text_length = INT32_MAX;
          bool is_varbinary_or_binary = (out_type == ObVarcharType && CS_TYPE_BINARY == out_cs_type) ||
                                        (ObCharType == out_type && CS_TYPE_BINARY == out_cs_type);

          if (is_varbinary_or_binary) {
            text_length = text.length();
          } else {
            if (is_oracle_byte_length(lib::is_oracle_mode(), ls)) {
              text_length = text.length();
            } else {
              text_length =
                  static_cast<int32_t>(ObCharset::strlen_char(expr.datum_meta_.cs_type_, text.ptr(), text.length()));
            }
          }
          if (lib::is_oracle_mode() && ls == LS_CHAR) {
            if ((ObCharType == out_type || ObNCharType == out_type) && text.length() > OB_MAX_ORACLE_CHAR_LENGTH_BYTE) {
              oracle_char_byte_exceed = true;
            } else if ((ObVarcharType == out_type || ObNVarchar2Type == out_type) &&
                       text.length() > OB_MAX_ORACLE_VARCHAR_LENGTH) {
              oracle_char_byte_exceed = true;
            }
          }

          if (out_acc.get_length() < text_length || oracle_char_byte_exceed) {
            int64_t acc_len = !oracle_char_byte_exceed ? out_acc.get_length()
                                                       : ((ObVarcharType == out_type || ObNVarchar2Type == out_type)
                                                                 ? OB_MAX_ORACLE_VARCHAR_LENGTH
                                                                 : OB_MAX_ORACLE_CHAR_LENGTH_BYTE);
            int64_t char_len = 0;  // UNUSED
            int64_t size =
                (ls == LS_BYTE || oracle_char_byte_exceed
                        ? ObCharset::max_bytes_charpos(out_cs_type, text.ptr(), text.length(), acc_len, char_len)
                        : ObCharset::charpos(out_cs_type, text.ptr(), text.length(), acc_len));
            if (0 == size) {
              if (lib::is_oracle_mode()) {
                res_datum.set_null();
              } else {
                res_datum.set_string(NULL, 0);
              }
            } else {
              res_datum.len_ = size;
            }
          } else if (out_acc.get_length() == text_length || (ObCharType != out_type && ObNCharType != out_type) ||
                     (lib::is_mysql_mode() && ob_is_char(out_type, expr.datum_meta_.cs_type_) &&
                         !(SMO_PAD_CHAR_TO_FULL_LENGTH & session->get_sql_mode()))) {
            // do not padding
            LOG_DEBUG("no need to padding", K(ret), K(out_acc.get_length()), K(text_length), K(text));
          } else if (out_acc.get_length() > text_length) {
            int64_t padding_cnt = out_acc.get_length() - text_length;
            ObString padding_res;
            ObIAllocator& calc_alloc = ctx.get_reset_tmp_alloc();
            if (OB_FAIL(padding_char_for_cast(padding_cnt, out_cs_type, calc_alloc, padding_res))) {
              LOG_WARN("padding char failed", K(ret), K(padding_cnt), K(out_cs_type));
            } else {
              int64_t padding_size = padding_res.length() + text.length();
              char* res_ptr = expr.get_str_res_mem(ctx, padding_size);
              if (OB_ISNULL(res_ptr)) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                LOG_WARN("allocate memory failed", K(ret));
              } else {
                MEMMOVE(res_ptr, text.ptr(), text.length());
                MEMMOVE(res_ptr + text.length(), padding_res.ptr(), padding_res.length());
                res_datum.set_string(res_ptr, text.length() + padding_res.length());
              }
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("can never reach", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int interval_scale_check(const ObCastMode& cast_mode, const ObAccuracy& accuracy, const ObObjType type,
    const ObDatum& in_datum, ObDatum& res_datum, int& warning)
{
  int ret = OB_SUCCESS;
  UNUSED(cast_mode);
  UNUSED(warning);
  ObScale expected_scale = accuracy.get_scale();
  if (ob_is_interval_ym(type)) {
    ObIntervalYMValue in_val(in_datum.get_interval_nmonth());
    int8_t expected_year_scale =
        ObIntervalScaleUtil::ob_scale_to_interval_ym_year_scale(static_cast<int8_t>(expected_scale));
    int8_t input_year_scale = in_val.calc_leading_scale();
    if (OB_UNLIKELY(expected_year_scale < input_year_scale)) {
      ret = OB_ERR_THE_LEADING_PRECISION_OF_THE_INTERVAL_IS_TOO_SMALL;
      LOG_WARN("interval obj scale check", K(ret), K(expected_year_scale), K(input_year_scale));
    } else {
      res_datum.set_interval_ym(in_datum.get_interval_nmonth());
    }
  } else if (ob_is_interval_ds(type)) {
    ObIntervalDSValue in_val = in_datum.get_interval_ds();
    int8_t expected_day_scale =
        ObIntervalScaleUtil::ob_scale_to_interval_ds_day_scale(static_cast<int8_t>(expected_scale));
    int8_t expected_fs_scale =
        ObIntervalScaleUtil::ob_scale_to_interval_ds_second_scale(static_cast<int8_t>(expected_scale));

    if (OB_FAIL(ObTimeConverter::round_interval_ds(expected_fs_scale, in_val))) {
      LOG_WARN("fail to round interval ds", K(ret), K(in_val));
    } else {
      int8_t input_day_scale = in_val.calc_leading_scale();
      if (OB_UNLIKELY(expected_day_scale < input_day_scale)) {
        ret = OB_ERR_THE_LEADING_PRECISION_OF_THE_INTERVAL_IS_TOO_SMALL;
        LOG_WARN("interval obj scale check", K(ret), K(expected_day_scale), K(input_day_scale));
      } else {
        res_datum.set_interval_ds(in_val);
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected in type", K(ret), K(type));
  }
  return ret;
}

int bit_length_check(const ObCastMode& cast_mode, const ObAccuracy& accuracy, const ObObjType type,
    const ObDatum& in_datum, ObDatum& res_datum, int& warning)
{
  int ret = OB_SUCCESS;
  UNUSED(type);
  uint64_t value = in_datum.get_uint();
  int32_t bit_len = 0;
  int32_t dst_bit_len = accuracy.get_precision();
  if (OB_FAIL(get_bit_len(value, bit_len))) {
    LOG_WARN("fail to get_bit_length", K(ret), K(value), K(bit_len));
  } else if (OB_UNLIKELY(bit_len <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("bit length is negative", K(ret), K(value), K(bit_len));
  } else {
    if (OB_UNLIKELY(bit_len > dst_bit_len)) {
      ret = OB_ERR_DATA_TOO_LONG;
      LOG_WARN("bit type length is too long", K(ret), K(bit_len), K(dst_bit_len), K(value));
    } else {
      res_datum.set_bit(value);
    }
    if (OB_FAIL(ret) && CM_IS_WARN_ON_FAIL(cast_mode)) {
      warning = OB_DATA_OUT_OF_RANGE;
      ret = OB_SUCCESS;
      uint64_t max_value = (1ULL << dst_bit_len) - 1;
      res_datum.set_bit(max_value);
    }
  }
  return ret;
}

int raw_length_check(const ObCastMode& cast_mode, const ObAccuracy& accuracy, const ObObjType type,
    const ObDatum& in_datum, ObDatum& res_datum, int& warning)
{
  int ret = OB_SUCCESS;
  UNUSED(cast_mode);
  UNUSED(type);
  UNUSED(warning);
  const ObLength max_accuracy_len = accuracy.get_length();
  const int32_t str_len_byte = in_datum.len_;
  if (OB_UNLIKELY(str_len_byte > max_accuracy_len)) {
    ret = OB_ERR_DATA_TOO_LONG;
    LOG_WARN("char type length is too long", K(str_len_byte), K(max_accuracy_len));
  } else {
    res_datum.set_datum(in_datum);
  }
  return ret;
}

// check usec scale for ObTimeType, ObDateTimeType
int time_usec_scale_check(const ObCastMode &cast_mode,
                          const ObAccuracy &accuracy,
                          const int64_t value)
{
  INIT_SUCC(ret);
  UNUSED(value);
  bool need_check_zero_scale = CM_IS_ERROR_ON_SCALE_OVER(cast_mode);
  // check usec scale for time part
  if (need_check_zero_scale) {
    ObScale scale = accuracy.get_scale();
    if (OB_UNLIKELY(0 <= scale && scale < MAX_SCALE_FOR_TEMPORAL)) {
      // get usec part for value
      int64_t temp_value = value;
      ObTimeConverter::round_datetime(scale, temp_value);
      if (temp_value != value) { // round success
        ret = OB_OPERATE_OVERFLOW;
        LOG_WARN("Invalid input value.", K(value), K(scale));
      }
    }
  }
  return ret;
}

int time_scale_check(const ObCastMode& cast_mode, const ObAccuracy& accuracy, const ObObjType type,
    const ObDatum& in_datum, ObDatum& res_datum, int& warning)
{
  int ret = OB_SUCCESS;
  UNUSED(type);
  UNUSED(warning);
  ObScale scale = accuracy.get_scale();
  int64_t value = in_datum.get_int();
  if (OB_FAIL(time_usec_scale_check(cast_mode, accuracy, value))) {
    LOG_WARN("check usec scale fail.", K(ret), K(value));
  } else if (OB_LIKELY(0 <= scale && scale < MAX_SCALE_FOR_TEMPORAL)) {
    int64_t value = in_datum.get_int();
    ObTimeConverter::round_datetime(scale, value);
    res_datum.set_time(value);
  } else {
    res_datum.set_datum(in_datum);
  }
  return ret;
}

int otimestamp_scale_check(const ObCastMode& cast_mode, const ObAccuracy& accuracy, const ObObjType type,
    const ObDatum& in_datum, ObDatum& res_datum, int& warning)
{
  int ret = OB_SUCCESS;
  UNUSED(cast_mode);
  UNUSED(warning);
  ObScale scale = accuracy.get_scale();
  ObOTimestampData in_val;
  if (OB_UNLIKELY(scale > MAX_SCALE_FOR_ORACLE_TEMPORAL)) {
    ret = OB_ERR_TOO_BIG_PRECISION;
    LOG_USER_ERROR(OB_ERR_TOO_BIG_PRECISION, scale, "CAST", static_cast<int64_t>(MAX_SCALE_FOR_ORACLE_TEMPORAL));
  } else if (OB_UNLIKELY(0 <= scale && scale < MAX_SCALE_FOR_ORACLE_TEMPORAL)) {
    ObOTimestampData in_val;
    if (OB_FAIL(common_construct_otimestamp(type, in_datum, in_val))) {
      LOG_WARN("common_construct_otimestamp failed", K(ret));
    } else {
      ObOTimestampData ot_data = ObTimeConverter::round_otimestamp(scale, in_val);
      if (ObTimeConverter::is_valid_otimestamp(ot_data.time_us_, static_cast<int32_t>(ot_data.time_ctx_.tail_nsec_))) {
        if (ObTimestampTZType == type) {
          res_datum.set_otimestamp_tz(ot_data);
        } else {
          res_datum.set_otimestamp_tiny(ot_data);
        }
      } else {
        OB_LOG(DEBUG, "invalid otimestamp, set it null ", K(ot_data), K(scale), "orig_date", in_val);
        res_datum.set_null();
      }
    }
  } else {
    res_datum.set_datum(in_datum);
  }
  return ret;
}

int datetime_scale_check(const ObCastMode& cast_mode, const ObAccuracy& accuracy, const ObObjType type,
    const ObDatum& in_datum, ObDatum& res_datum, int& warning)
{
  int ret = OB_SUCCESS;
  UNUSED(type);
  UNUSED(warning);
  ObScale scale = accuracy.get_scale();
  if (OB_UNLIKELY(scale > MAX_SCALE_FOR_TEMPORAL)) {
    ret = OB_ERR_TOO_BIG_PRECISION;
    LOG_USER_ERROR(OB_ERR_TOO_BIG_PRECISION, scale, "CAST", static_cast<int64_t>(MAX_SCALE_FOR_TEMPORAL));
  } else {
    int64_t value = in_datum.get_int();
    if (OB_FAIL(time_usec_scale_check(cast_mode, accuracy, value))) {
      LOG_WARN("check zero scale fail.", K(ret), K(value), K(scale));
    } else if (OB_UNLIKELY(0 <= scale && scale < MAX_SCALE_FOR_TEMPORAL)) {
      int64_t value = in_datum.get_int();
      ObTimeConverter::round_datetime(scale, value);
      if (ObTimeConverter::is_valid_datetime(value)) {
        res_datum.set_datetime(value);
      } else {
        res_datum.set_null();
      }
    } else {
      res_datum.set_datum(in_datum);
    }
  }
  return ret;
}

int number_range_check_v2(const ObCastMode& cast_mode, const ObAccuracy& accuracy, const ObObjType type,
    const ObDatum& in_datum, ObDatum& res_datum, int& warning)
{
  int ret = OB_SUCCESS;
  int& cast_ret = CM_IS_ERROR_ON_FAIL(cast_mode) ? ret : warning;
  ObPrecision precision = accuracy.get_precision();
  ObScale scale = accuracy.get_scale();
  const number::ObNumber* min_check_num = NULL;
  const number::ObNumber* max_check_num = NULL;
  const number::ObNumber* min_num_mysql = NULL;
  const number::ObNumber* max_num_mysql = NULL;
  const number::ObNumber in_val(in_datum.get_number());
  const static int64_t num_alloc_used_times = 2;  // out val alloc will be used twice
  ObNumStackAllocator<num_alloc_used_times> out_val_alloc;
  number::ObNumber out_val;
  bool is_finish = false;
  if (ObNumberFloatType == type) {
    if (OB_MIN_NUMBER_FLOAT_PRECISION <= precision && precision <= OB_MAX_NUMBER_FLOAT_PRECISION) {
      const int64_t number_precision = static_cast<int64_t>(floor(precision * OB_PRECISION_BINARY_TO_DECIMAL_FACTOR));
      if (OB_FAIL(out_val.from(in_val, out_val_alloc))) {
      } else if (OB_FAIL(out_val.round_precision(number_precision))) {
      } else if (CM_IS_ERROR_ON_SCALE_OVER(cast_mode) &&
        in_val.compare(out_val) != 0) {
        ret = OB_OPERATE_OVERFLOW;
        LOG_WARN("input value is out of range.", K(scale), K(in_val));
      } else {
        res_datum.set_number(out_val);
        is_finish = true;
      }
      LOG_DEBUG("finish round_precision", K(in_val), K(number_precision), K(precision));
    } else if (PRECISION_UNKNOWN_YET == precision) {
      res_datum.set_number(in_val);
      is_finish = true;
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", K(ret), K(precision), K(scale));
    }
  } else if (lib::is_oracle_mode()) {
    if (OB_MAX_NUMBER_PRECISION >= precision && precision >= OB_MIN_NUMBER_PRECISION &&
        number::ObNumber::MAX_SCALE >= scale && scale >= number::ObNumber::MIN_SCALE) {
      min_check_num =
          &(ObNumberConstValue::ORACLE_CHECK_MIN[precision][scale + ObNumberConstValue::MAX_ORACLE_SCALE_DELTA]);
      max_check_num =
          &(ObNumberConstValue::ORACLE_CHECK_MAX[precision][scale + ObNumberConstValue::MAX_ORACLE_SCALE_DELTA]);
    } else if (ORA_NUMBER_SCALE_UNKNOWN_YET == scale && PRECISION_UNKNOWN_YET == precision) {
      res_datum.set_number(in_val);
      is_finish = true;
    } else if (PRECISION_UNKNOWN_YET == precision && number::ObNumber::MAX_SCALE >= scale &&
               scale >= number::ObNumber::MIN_SCALE) {
      ObNumStackOnceAlloc tmp_alloc;
      number::ObNumber num;
      if (OB_FAIL(num.from(in_val, tmp_alloc))) {
      } else if (OB_FAIL(num.round(scale))) {
      } else if (CM_IS_ERROR_ON_SCALE_OVER(cast_mode) &&
        in_val.compare(num) != 0) {
        ret = OB_OPERATE_OVERFLOW;
        LOG_WARN("input value is out of range.", K(scale), K(in_val));
      } else {
        res_datum.set_number(num);
        is_finish = true;
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", K(ret), K(precision), K(scale));
    }
  } else {
    if (OB_UNLIKELY(precision < scale)) {
      ret = OB_ERR_M_BIGGER_THAN_D;
    } else if (number::ObNumber::MAX_PRECISION >= precision && precision >= OB_MIN_DECIMAL_PRECISION &&
               number::ObNumber::MAX_SCALE >= scale && scale >= 0) {
      min_check_num = &(ObNumberConstValue::MYSQL_CHECK_MIN[precision][scale]);
      max_check_num = &(ObNumberConstValue::MYSQL_CHECK_MAX[precision][scale]);
      min_num_mysql = &(ObNumberConstValue::MYSQL_MIN[precision][scale]);
      max_num_mysql = &(ObNumberConstValue::MYSQL_MAX[precision][scale]);
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", K(ret), K(precision), K(scale));
    }
  }

  if (OB_SUCC(ret) && !is_finish) {
    if (OB_ISNULL(min_check_num) || OB_ISNULL(max_check_num) ||
        (!lib::is_oracle_mode() && (OB_ISNULL(min_num_mysql) || OB_ISNULL(max_num_mysql)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("min_num or max_num is null", K(ret), KPC(min_check_num), KPC(max_check_num));
    } else if (in_val <= *min_check_num) {
      if (lib::is_oracle_mode()) {
        cast_ret = OB_ERR_VALUE_LARGER_THAN_ALLOWED;
      } else {
        cast_ret = OB_DATA_OUT_OF_RANGE;
        res_datum.set_number(*min_num_mysql);
      }
      is_finish = true;
    } else if (in_val >= *max_check_num) {
      if (lib::is_oracle_mode()) {
        cast_ret = OB_ERR_VALUE_LARGER_THAN_ALLOWED;
      } else {
        cast_ret = OB_DATA_OUT_OF_RANGE;
        res_datum.set_number(*max_num_mysql);
      }
      is_finish = true;
    } else {
      if (OB_FAIL(out_val.from(in_val, out_val_alloc))) {
        LOG_WARN("out_val.from failed", K(ret), K(in_val));
      } else if (OB_FAIL(out_val.round(scale))) {
        LOG_WARN("out_val.round failed", K(ret), K(scale));
      } else if (CM_IS_ERROR_ON_SCALE_OVER(cast_mode) &&
        in_val.compare(out_val) != 0) {
        ret = OB_OPERATE_OVERFLOW;
        LOG_WARN("input value is out of range.", K(scale), K(in_val));
      } else {
        res_datum.set_number(out_val);
        is_finish = true;
      }
    }
  }
  if (OB_SUCC(ret) && !is_finish) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected situation, res_datum is not set", K(ret));
  }
  LOG_DEBUG("number_range_check_v2 done",
      K(ret),
      K(is_finish),
      K(accuracy),
      K(in_val),
      K(out_val),
      KPC(min_check_num),
      KPC(max_check_num));
  return ret;
}

template <typename IN_TYPE>
static int float_range_check(const ObCastMode& cast_mode, const ObAccuracy& accuracy, const ObObjType type,
    const ObDatum& in_datum, ObDatum& res_datum, int& warning)
{
  int ret = OB_SUCCESS;
  ObObjTypeClass type_class = ob_obj_type_class(type);
  if (OB_UNLIKELY(ObFloatTC != type_class && ObDoubleTC != type_class)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("obj type is invalid, must be float/double tc", K(ret), K(type), K(type_class));
  } else {
    IN_TYPE in_val = *(reinterpret_cast<const IN_TYPE*>(in_datum.ptr_));
    IN_TYPE out_val = in_val;
    if (lib::is_oracle_mode() && 0.0 == in_val) {
      if (ObFloatTC == type_class) {
        res_datum.set_float(0.0);
      } else {
        res_datum.set_double(0.0);
      }
    } else {
      if (CAST_FAIL_CM(real_range_check(accuracy, out_val), cast_mode)) {
        LOG_WARN("real_range_check failed", K(ret));
      } else if (in_val != out_val) {
        if (ObFloatTC == type_class) {
          res_datum.set_float(out_val);
        } else {
          res_datum.set_double(out_val);
        }
      } else {
        res_datum.set_datum(in_datum);
      }
    }
  }
  return ret;
}

int string_length_check(const ObExpr& expr, const ObCastMode& cast_mode, const ObAccuracy& accuracy,
    const ObObjType type, const ObCollationType cs_type, ObEvalCtx& ctx, const ObDatum& in_datum, ObDatum& res_datum,
    int& warning)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  UNUSED(ctx);
  const ObLength max_accuracy_len = accuracy.get_length();
  const int32_t str_len_byte = in_datum.len_;
  bool is_oracle = lib::is_oracle_mode();
  ObObjMeta meta;
  meta.set_type_simple(type);
  meta.set_collation_type(cs_type);
  // remember not to change in_datum
  res_datum.set_datum(in_datum);
  if (max_accuracy_len <= 0 || str_len_byte > max_accuracy_len) {
    int& cast_ret = (CM_IS_ERROR_ON_FAIL(cast_mode) && !is_oracle) ? ret : warning;
    const char* str = in_datum.ptr_;
    int32_t str_len_char = -1;
    // In parse, if the length is greater than the maximum value of int32_t, length will be set to -1
    if (max_accuracy_len == -1) {
    } else if (OB_UNLIKELY(max_accuracy_len <= 0)) {
      res_datum.set_string(NULL, 0);
      if (OB_UNLIKELY(0 == max_accuracy_len && str_len_byte > 0)) {
        cast_ret = OB_ERR_DATA_TOO_LONG;
        str_len_char =
            meta.is_lob() ? str_len_byte : static_cast<int32_t>(ObCharset::strlen_char(cs_type, str, str_len_byte));
        OB_LOG(WARN, "char type length is too long", K(max_accuracy_len), K(str_len_char));
      }
    } else {
      int32_t trunc_len_byte = -1;
      int32_t trunc_len_char = -1;
      if (meta.is_varbinary() || meta.is_binary() || meta.is_blob()) {
        str_len_char =
            meta.is_blob() ? str_len_byte : static_cast<int32_t>(ObCharset::strlen_char(cs_type, str, str_len_byte));
        if (OB_UNLIKELY(str_len_char > max_accuracy_len)) {
          cast_ret = OB_ERR_DATA_TOO_LONG;
          LOG_WARN("binary type length is too long", K(max_accuracy_len), K(str_len_char));
        }
      } else if (is_oracle_byte_length(is_oracle, accuracy.get_length_semantics())) {
        const ObLength max_len_byte = accuracy.get_length();
        if (OB_UNLIKELY(str_len_byte > max_len_byte)) {
          cast_ret = OB_ERR_DATA_TOO_LONG;
          LOG_WARN("char type length is too long", K(str_len_byte), K(max_len_byte));
        }
      } else if (is_oracle && (meta.is_char() || meta.is_nchar())) {
        const int32_t str_len_char = static_cast<int32_t>(ObCharset::strlen_char(cs_type, str, str_len_byte));
        if (OB_UNLIKELY(str_len_byte > OB_MAX_ORACLE_CHAR_LENGTH_BYTE)) {
          cast_ret = OB_ERR_DATA_TOO_LONG;
          LOG_WARN("char byte length is too long", K(str_len_byte), K(OB_MAX_ORACLE_CHAR_LENGTH_BYTE));
        } else if (OB_UNLIKELY(str_len_char > max_accuracy_len)) {
          cast_ret = OB_ERR_DATA_TOO_LONG;
          LOG_WARN("char char length is too long", K(str_len_char), K(max_accuracy_len));
        }
      } else {  // mysql, oracle varchar(char)
        // trunc_len_char > max_accuracy_len means an error or warning, without tail ' '
        // str_len_char > max_accuracy_len means only warning, even in strict mode.
        // lengthsp()  - returns the length of the given string without trailing spaces.
        // so the result returned by strlen_byte_no_sp is less than or equal to the length of str
        trunc_len_byte = static_cast<int32_t>(ObCharset::strlen_byte_no_sp(cs_type, str, str_len_byte));
        trunc_len_char =
            meta.is_lob() ? trunc_len_byte : static_cast<int32_t>(ObCharset::strlen_char(cs_type, str, trunc_len_byte));

        if (is_oracle && OB_UNLIKELY(str_len_byte > OB_MAX_ORACLE_VARCHAR_LENGTH)) {
          cast_ret = OB_ERR_DATA_TOO_LONG;
          LOG_WARN("varchar2 byte length is too long", K(str_len_byte), K(OB_MAX_ORACLE_VARCHAR_LENGTH));
        } else if (OB_UNLIKELY(trunc_len_char > max_accuracy_len)) {
          cast_ret = OB_ERR_DATA_TOO_LONG;
          LOG_WARN("char type length is too long", K(max_accuracy_len), K(trunc_len_char));
        } else {
          str_len_char =
              meta.is_lob() ? str_len_byte : static_cast<int32_t>(ObCharset::strlen_char(cs_type, str, str_len_byte));
          if (OB_UNLIKELY(str_len_char > max_accuracy_len)) {
            warning = OB_ERR_DATA_TOO_LONG;
            LOG_WARN("char type length is too long", K(max_accuracy_len), K(str_len_char));
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_UNLIKELY(OB_ERR_DATA_TOO_LONG == warning)) {
          // when warning, always trunc to max_accuracy_len first.
          // besides, if char (not binary), trunc to trunc_len_char again,
          // trim tail ' ' after first trunc.
          // the reason of two-trunc for char (not binary):
          // insert 'ab  ! ' to char(3), we get an 'ab' in column, not 'ab ':
          // first trunc: 'ab  ! ' to 'ab ',
          // second trunc: 'ab ' to 'ab'.
          if (meta.is_text() || meta.is_json()) {
            int64_t char_len = 0;
            trunc_len_byte = static_cast<int32_t>(
                ObCharset::max_bytes_charpos(cs_type, str, str_len_byte, max_accuracy_len, char_len));
          } else {
            trunc_len_byte = static_cast<int32_t>(ObCharset::charpos(cs_type, str, str_len_byte, max_accuracy_len));
          }
          if (is_oracle) {
            // Do not clean up space characters at the end in oracle mode
          } else if (meta.is_fixed_len_char_type() && !meta.is_binary()) {
            trunc_len_byte = static_cast<int32_t>(ObCharset::strlen_byte_no_sp(cs_type, str, trunc_len_byte));
          }
          res_datum.len_ = trunc_len_byte;
          if (is_oracle) {
            ret = warning;
          }
        } else if (OB_SUCC(warning) && is_oracle) {
          ret = warning;
        } else {
          // do nothing
        }
      }
    }
  } else {
    // do nothing
  }

  return ret;
}

int rowid_length_check(const ObExpr& expr, const ObCastMode& cast_mode, const ObAccuracy& accuracy,
    const ObObjType type, const ObCollationType cs_type, ObEvalCtx& ctx, const ObDatum& in_datum, ObDatum& res_datum,
    int& warning)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  UNUSED(cast_mode);
  UNUSED(type);
  UNUSED(cs_type);
  UNUSED(ctx);
  UNUSED(warning);
  ObURowIDData urowid_data(in_datum.len_, reinterpret_cast<const uint8_t*>(in_datum.ptr_));
  if (OB_UNLIKELY(share::is_mysql_mode())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("rowid is not supported in mysql mode", K(ret));
  } else if (-1 == accuracy.get_length()) {
    // select cast('xx' as rowid) from dual;
    // length will be -1
    res_datum.set_datum(in_datum);
  } else if (urowid_data.rowid_len_ > accuracy.get_length()) {
    ret = OB_ERR_DATA_TOO_LONG;
    LOG_WARN("rowid data length is too long", K(urowid_data.rowid_len_), K(accuracy));
  } else {
    res_datum.set_datum(in_datum);
  }
  return ret;
}

int datum_accuracy_check(const ObExpr& expr, const uint64_t cast_mode, ObEvalCtx& ctx, const ObDatum& in_datum,
    ObDatum& res_datum, int& warning)
{
  int ret = OB_SUCCESS;
  ObAccuracy accuracy;
  accuracy.set_length(expr.max_length_);
  accuracy.set_scale(expr.datum_meta_.scale_);
  const ObObjTypeClass& dst_tc = ob_obj_type_class(expr.datum_meta_.type_);
  if (ObStringTC == dst_tc || ObTextTC == dst_tc || ObJsonTC == dst_tc) {
    accuracy.set_length_semantics(expr.datum_meta_.length_semantics_);
  } else {
    accuracy.set_precision(expr.datum_meta_.precision_);
  }
  return datum_accuracy_check(expr, cast_mode, ctx, accuracy, in_datum, res_datum, warning);
}

int datum_accuracy_check(const ObExpr& expr, const uint64_t cast_mode, ObEvalCtx& ctx, const ObAccuracy& accuracy,
    const ObDatum& in_datum, ObDatum& res_datum, int& warning)
{
  int ret = OB_SUCCESS;
  if (!in_datum.is_null()) {
    ObObjType type = expr.datum_meta_.type_;
    ObCollationType cs_type = expr.datum_meta_.cs_type_;
    switch (ob_obj_type_class(type)) {
      case ObFloatTC: {
        ret = float_range_check<float>(cast_mode, accuracy, type, in_datum, res_datum, warning);
        break;
      }
      case ObDoubleTC: {
        ret = float_range_check<double>(cast_mode, accuracy, type, in_datum, res_datum, warning);
        break;
      }
      case ObNumberTC: {
        ret = number_range_check_v2(cast_mode, accuracy, type, in_datum, res_datum, warning);
        break;
      }
      case ObDateTimeTC: {
        ret = datetime_scale_check(cast_mode, accuracy, type, in_datum, res_datum, warning);
        break;
      }
      case ObOTimestampTC: {
        ret = otimestamp_scale_check(cast_mode, accuracy, type, in_datum, res_datum, warning);
        break;
      }
      case ObTimeTC: {
        ret = time_scale_check(cast_mode, accuracy, type, in_datum, res_datum, warning);
        break;
      }
      case ObStringTC: {
        ret = string_length_check(expr, cast_mode, accuracy, type, cs_type, ctx, in_datum, res_datum, warning);
        break;
      }
      case ObRawTC: {
        ret = raw_length_check(cast_mode, accuracy, type, in_datum, res_datum, warning);
        break;
      }
      case ObTextTC: {
        ret = string_length_check(expr, cast_mode, accuracy, type, cs_type, ctx, in_datum, res_datum, warning);
        break;
      }
      case ObBitTC: {
        ret = bit_length_check(cast_mode, accuracy, type, in_datum, res_datum, warning);
        break;
      }
      case ObIntervalTC: {
        ret = interval_scale_check(cast_mode, accuracy, type, in_datum, res_datum, warning);
        break;
      }
      case ObRowIDTC: {
        ret = rowid_length_check(expr, cast_mode, accuracy, type, cs_type, ctx, in_datum, res_datum, warning);
        break;
      }
      default: {
        res_datum.set_datum(in_datum);
        break;
      }
    }
  } else {
    res_datum.set_null();
  }
  return ret;
}

ObExpr::EvalFunc OB_DATUM_CAST_ORACLE_IMPLICIT[ObMaxTC][ObMaxTC] = {
    {
        /*null -> XXX*/
        cast_eval_arg,     /*null*/
        cast_eval_arg,     /*int*/
        cast_eval_arg,     /*uint*/
        cast_eval_arg,     /*float*/
        cast_eval_arg,     /*double*/
        cast_eval_arg,     /*number*/
        cast_eval_arg,     /*datetime*/
        cast_not_expected, /*date*/
        cast_not_expected, /*time*/
        cast_not_expected, /*year*/
        cast_eval_arg,     /*string*/
        cast_not_expected, /*extend*/
        cast_not_expected, /*unknown*/
        cast_eval_arg,     /*text*/
        cast_not_expected, /*bit*/
        cast_not_expected, /*enumset*/
        cast_not_expected, /*enumsetInner*/
        cast_eval_arg,     /*otimestamp*/
        cast_eval_arg,     /*raw*/
        cast_eval_arg,     /*interval*/
        cast_eval_arg,     /*rowid*/
        cast_eval_arg,     /*lob*/
        cast_eval_arg,     /*json*/
    },
    {
        /*int -> XXX*/
        cast_not_expected,       /*null*/
        int_int,                 /*int*/
        int_uint,                /*uint*/
        int_float,               /*float*/
        int_double,              /*double*/
        int_number,              /*number*/
        cast_inconsistent_types, /*datetime*/
        cast_not_expected,       /*date*/
        cast_not_expected,       /*time*/
        cast_not_expected,       /*year*/
        int_string,              /*string*/
        cast_not_expected,       /*extend*/
        cast_not_expected,       /*unknown*/
        cast_inconsistent_types, /*text*/
        cast_not_expected,       /*bit*/
        cast_not_expected,       /*enumset*/
        cast_not_expected,       /*enumset_inner*/
        cast_inconsistent_types, /*otimestamp*/
        cast_inconsistent_types, /*raw*/
        cast_inconsistent_types, /*interval*/
        cast_inconsistent_types, /*rowid*/
        cast_inconsistent_types, /*lob*/
        cast_inconsistent_types, /*json*/
    },
    {
        /*uint -> XXX*/
        cast_not_expected,       /*null*/
        uint_int,                /*int*/
        uint_uint,               /*uint*/
        uint_float,              /*float*/
        uint_double,             /*double*/
        uint_number,             /*number*/
        cast_inconsistent_types, /*datetime*/
        cast_not_expected,       /*date*/
        cast_not_expected,       /*time*/
        cast_not_expected,       /*year*/
        uint_string,             /*string*/
        cast_not_expected,       /*extend*/
        cast_not_expected,       /*unknown*/
        cast_inconsistent_types, /*text*/
        cast_not_expected,       /*bit*/
        cast_not_expected,       /*enumset*/
        cast_not_expected,       /*enumset_inner*/
        cast_inconsistent_types, /*otimestamp*/
        cast_inconsistent_types, /*raw*/
        cast_inconsistent_types, /*interval*/
        cast_inconsistent_types, /*rowid*/
        cast_inconsistent_types, /*lob*/
        cast_inconsistent_types, /*json*/
    },
    {
        /*float -> XXX*/
        cast_not_expected,       /*null*/
        float_int,               /*int*/
        float_uint,              /*uint*/
        float_float,             /*float*/
        float_double,            /*double*/
        float_number,            /*number*/
        cast_inconsistent_types, /*datetime*/
        cast_not_expected,       /*date*/
        cast_not_expected,       /*time*/
        cast_not_expected,       /*year*/
        float_string,            /*string*/
        cast_not_expected,       /*extend*/
        cast_not_expected,       /*unknown*/
        cast_inconsistent_types, /*text*/
        cast_not_expected,       /*bit*/
        cast_not_expected,       /*enumset*/
        cast_not_expected,       /*enumset_inner*/
        cast_inconsistent_types, /*otimestamp*/
        cast_inconsistent_types, /*raw*/
        cast_inconsistent_types, /*interval*/
        cast_inconsistent_types, /*rowid*/
        cast_inconsistent_types, /*lob*/
        cast_inconsistent_types, /*json*/
    },
    {
        /*double -> XXX*/
        cast_not_expected,       /*null*/
        double_int,              /*int*/
        double_uint,             /*uint*/
        double_float,            /*float*/
        double_double,           /*double*/
        double_number,           /*number*/
        cast_inconsistent_types, /*datetime*/
        cast_not_expected,       /*date*/
        cast_not_expected,       /*time*/
        double_year,             /*year*/
        double_string,           /*string*/
        cast_not_expected,       /*extend*/
        cast_not_expected,       /*unknown*/
        cast_inconsistent_types, /*text*/
        cast_not_expected,       /*bit*/
        cast_not_expected,       /*enumset*/
        cast_not_expected,       /*enumset_inner*/
        cast_inconsistent_types, /*otimestamp*/
        cast_inconsistent_types, /*raw*/
        cast_inconsistent_types, /*interval*/
        cast_inconsistent_types, /*rowid*/
        cast_inconsistent_types, /*lob*/
        cast_inconsistent_types, /*json*/
    },
    {
        /*number -> XXX*/
        cast_not_expected,       /*null*/
        number_int,              /*int*/
        number_uint,             /*uint*/
        number_float,            /*float*/
        number_double,           /*double*/
        number_number,           /*number*/
        cast_inconsistent_types, /*datetime*/
        cast_not_expected,       /*date*/
        cast_not_expected,       /*time*/
        cast_not_expected,       /*year*/
        number_string,           /*string*/
        cast_not_expected,       /*extend*/
        cast_not_expected,       /*unknown*/
        number_string,           /*text*/
        cast_not_expected,       /*bit*/
        cast_not_expected,       /*enumset*/
        cast_not_expected,       /*enumset_inner*/
        cast_inconsistent_types, /*otimestamp*/
        cast_inconsistent_types, /*raw*/
        cast_inconsistent_types, /*interval*/
        cast_inconsistent_types, /*rowid*/
        number_lob,              /*lob*/
        cast_inconsistent_types, /*json*/
    },
    {
        /*datetime -> XXX*/
        cast_not_expected,       /*null*/
        cast_inconsistent_types, /*int*/
        cast_inconsistent_types, /*uint*/
        cast_inconsistent_types, /*float*/
        cast_inconsistent_types, /*double*/
        cast_inconsistent_types, /*number*/
        datetime_datetime,       /*datetime*/
        cast_not_expected,       /*date*/
        cast_not_expected,       /*time*/
        cast_not_expected,       /*year*/
        datetime_string,         /*string*/
        cast_not_expected,       /*extend*/
        cast_not_expected,       /*unknown*/
        cast_inconsistent_types, /*text*/
        cast_not_expected,       /*bit*/
        cast_not_expected,       /*enumset*/
        cast_not_expected,       /*enumset_inner*/
        datetime_otimestamp,     /*otimestamp*/
        cast_inconsistent_types, /*raw*/
        cast_inconsistent_types, /*interval*/
        cast_inconsistent_types, /*rowid*/
        cast_inconsistent_types, /*lob*/
        cast_inconsistent_types, /*json*/
    },
    {
        /*date -> XXX*/
        cast_not_expected, /*null*/
        cast_not_expected, /*int*/
        cast_not_expected, /*uint*/
        cast_not_expected, /*float*/
        cast_not_expected, /*double*/
        cast_not_expected, /*number*/
        cast_not_expected, /*datetime*/
        cast_not_expected, /*date*/
        cast_not_expected, /*time*/
        cast_not_expected, /*year*/
        cast_not_expected, /*string*/
        cast_not_expected, /*extend*/
        cast_not_expected, /*unknown*/
        cast_not_expected, /*text*/
        cast_not_expected, /*bit*/
        cast_not_expected, /*enumset*/
        cast_not_expected, /*enumset_inner*/
        cast_not_expected, /*otimestamp*/
        cast_not_expected, /*raw*/
        cast_not_expected, /*interval*/
        cast_not_expected, /*rowid*/
        cast_not_expected, /*lob*/
        cast_not_expected, /*json*/
    },
    {
        /*time -> XXX*/
        cast_not_expected, /*null*/
        cast_not_expected, /*int*/
        cast_not_expected, /*uint*/
        cast_not_expected, /*float*/
        cast_not_expected, /*double*/
        cast_not_expected, /*number*/
        cast_not_expected, /*datetime*/
        cast_not_expected, /*date*/
        cast_not_expected, /*time*/
        cast_not_expected, /*year*/
        cast_not_expected, /*string*/
        cast_not_expected, /*extend*/
        cast_not_expected, /*unknown*/
        cast_not_expected, /*text*/
        cast_not_expected, /*bit*/
        cast_not_expected, /*enumset*/
        cast_not_expected, /*enumset_inner*/
        cast_not_expected, /*otimestamp*/
        cast_not_expected, /*raw*/
        cast_not_expected, /*interval*/
        cast_not_expected, /*rowid*/
        cast_not_expected, /*lob*/
        cast_not_expected, /*json*/
    },
    {
        /*year -> XXX*/
        cast_not_expected, /*null*/
        cast_not_expected, /*int*/
        cast_not_expected, /*uint*/
        cast_not_expected, /*float*/
        cast_not_expected, /*double*/
        cast_not_expected, /*number*/
        cast_not_expected, /*datetime*/
        cast_not_expected, /*date*/
        cast_not_expected, /*time*/
        cast_not_expected, /*year*/
        cast_not_expected, /*string*/
        cast_not_expected, /*extend*/
        cast_not_expected, /*unknown*/
        cast_not_expected, /*text*/
        cast_not_expected, /*bit*/
        cast_not_expected, /*enumset*/
        cast_not_expected, /*enumset_inner*/
        cast_not_expected, /*otimestamp*/
        cast_not_expected, /*raw*/
        cast_not_expected, /*interval*/
        cast_not_expected, /*rowid*/
        cast_not_expected, /*lob*/
        cast_not_expected, /*json*/
    },
    {
        /*string -> XXX*/
        cast_not_expected, /*null*/
        string_int,        /*int*/
        string_uint,       /*uint*/
        string_float,      /*float*/
        string_double,     /*double*/
        string_number,     /*number*/
        string_datetime,   /*datetime*/
        cast_not_expected, /*date*/
        cast_not_expected, /*time*/
        cast_not_expected, /*year*/
        string_string,     /*string*/
        cast_not_expected, /*extend*/
        cast_not_expected, /*unknown*/
        string_string,     /*text*/
        cast_not_expected, /*bit*/
        cast_not_expected, /*enumset*/
        cast_not_expected, /*enumset_inner*/
        string_otimestamp, /*otimestamp*/
        string_raw,        /*raw*/
        string_interval,   /*interval*/
        string_rowid,      /*rowid*/
        string_lob,        /*lob*/
        cast_not_expected, /*json*/
    },
    {
        /*extend -> XXX*/
        cast_not_expected, /*null*/
        cast_not_expected, /*int*/
        cast_not_expected, /*uint*/
        cast_not_expected, /*float*/
        cast_not_expected, /*double*/
        cast_not_expected, /*number*/
        cast_not_expected, /*datetime*/
        cast_not_expected, /*date*/
        cast_not_expected, /*time*/
        cast_not_expected, /*year*/
        cast_not_expected, /*string*/
        cast_eval_arg,     /*extend*/
        cast_not_expected, /*unknown*/
        cast_not_expected, /*text*/
        cast_not_expected, /*bit*/
        cast_not_expected, /*enumset*/
        cast_not_expected, /*enumset_inner*/
        cast_not_expected, /*otimestamp*/
        cast_not_expected, /*raw*/
        cast_not_expected, /*interval*/
        cast_not_expected, /*rowid*/
        cast_not_expected, /*lob*/
        cast_not_expected, /*json*/
    },
    {
        /*unknown -> XXX*/
        cast_not_expected, /*null*/
        cast_not_expected, /*int*/
        cast_not_expected, /*uint*/
        cast_not_expected, /*float*/
        cast_not_expected, /*double*/
        cast_not_expected, /*number*/
        cast_not_expected, /*datetime*/
        cast_not_expected, /*date*/
        cast_not_expected, /*time*/
        cast_not_expected, /*year*/
        cast_not_expected, /*string*/
        cast_not_expected, /*extend*/
        cast_not_expected, /*unknown*/
        cast_not_expected, /*text*/
        cast_not_expected, /*bit*/
        cast_not_expected, /*enumset*/
        cast_not_expected, /*enumset_inner*/
        cast_not_expected, /*otimestamp*/
        cast_not_expected, /*raw*/
        cast_not_expected, /*interval*/
        cast_not_expected, /*rowid*/
        cast_not_expected, /*lob*/
        cast_not_expected, /*json*/
    },
    {
        /*text -> XXX*/
        cast_not_expected,       /*null*/
        string_int,              /*int*/
        string_uint,             /*uint*/
        string_float,            /*float*/
        string_double,           /*double*/
        cast_not_support,        /*number*/
        string_datetime,         /*datetime*/
        cast_not_expected,       /*date*/
        cast_not_expected,       /*time*/
        cast_not_expected,       /*year*/
        string_string,           /*string*/
        cast_not_expected,       /*extend*/
        cast_not_expected,       /*unknown*/
        string_string,           /*text*/
        cast_not_expected,       /*bit*/
        cast_not_expected,       /*enumset*/
        cast_not_expected,       /*enumset_inner*/
        string_otimestamp,       /*otimestamp*/
        cast_inconsistent_types, /*raw*/
        string_interval,         /*interval*/
        string_rowid,            /*rowid*/
        string_lob,              /*lob*/
        cast_not_expected,       /*json*/
    },
    {
        /*bit -> XXX*/
        cast_not_expected, /*null*/
        cast_not_expected, /*int*/
        cast_not_expected, /*uint*/
        cast_not_expected, /*float*/
        cast_not_expected, /*double*/
        cast_not_expected, /*number*/
        cast_not_expected, /*datetime*/
        cast_not_expected, /*date*/
        cast_not_expected, /*time*/
        cast_not_expected, /*year*/
        cast_not_expected, /*string*/
        cast_not_expected, /*extend*/
        cast_not_expected, /*unknown*/
        cast_not_expected, /*text*/
        cast_not_expected, /*bit*/
        cast_not_expected, /*enumset*/
        cast_not_expected, /*enumset_inner*/
        cast_not_expected, /*otimestamp*/
        cast_not_expected, /*raw*/
        cast_not_expected, /*interval*/
        cast_not_expected, /*rowid*/
        cast_not_expected, /*lob*/
        cast_not_expected, /*json*/
    },
    {
        /*enum -> XXX*/
        cast_not_expected, /*null*/
        cast_not_expected, /*int*/
        cast_not_expected, /*uint*/
        cast_not_expected, /*float*/
        cast_not_expected, /*double*/
        cast_not_expected, /*number*/
        cast_not_expected, /*datetime*/
        cast_not_expected, /*date*/
        cast_not_expected, /*time*/
        cast_not_expected, /*year*/
        cast_not_expected, /*string*/
        cast_not_expected, /*extend*/
        cast_not_expected, /*unknown*/
        cast_not_expected, /*text*/
        cast_not_expected, /*bit*/
        cast_not_expected, /*enumset*/
        cast_not_expected, /*enumset_inner*/
        cast_not_expected, /*otimestamp*/
        cast_not_expected, /*raw*/
        cast_not_expected, /*interval*/
        cast_not_expected, /*rowid*/
        cast_not_expected, /*lob*/
        cast_not_expected, /*json*/
    },
    {
        /*enumset_inner -> XXX*/
        cast_not_expected, /*null*/
        cast_not_expected, /*int*/
        cast_not_expected, /*uint*/
        cast_not_expected, /*float*/
        cast_not_expected, /*double*/
        cast_not_expected, /*number*/
        cast_not_expected, /*datetime*/
        cast_not_expected, /*date*/
        cast_not_expected, /*time*/
        cast_not_expected, /*year*/
        cast_not_expected, /*string*/
        cast_not_expected, /*extend*/
        cast_not_expected, /*unknown*/
        cast_not_expected, /*text*/
        cast_not_expected, /*bit*/
        cast_not_expected, /*enumset*/
        cast_not_expected, /*enumset_inner*/
        cast_not_expected, /*otimestamp*/
        cast_not_expected, /*raw*/
        cast_not_expected, /*interval*/
        cast_not_expected, /*rowid*/
        cast_not_expected, /*lob*/
        cast_not_expected, /*json*/ 
    },
    {
        /*otimestamp -> XXX*/
        cast_not_expected,       /*null*/
        cast_inconsistent_types, /*int*/
        cast_inconsistent_types, /*uint*/
        cast_inconsistent_types, /*float*/
        cast_inconsistent_types, /*double*/
        cast_inconsistent_types, /*number*/
        otimestamp_datetime,     /*datetime*/
        cast_not_expected,       /*date*/
        cast_not_expected,       /*time*/
        cast_not_expected,       /*year*/
        otimestamp_string,       /*string*/
        cast_not_expected,       /*extend*/
        cast_not_expected,       /*unknown*/
        cast_inconsistent_types, /*text*/
        cast_not_expected,       /*bit*/
        cast_not_expected,       /*enumset*/
        cast_not_expected,       /*enumset_inner*/
        otimestamp_otimestamp,   /*otimestamp*/
        cast_inconsistent_types, /*raw*/
        cast_inconsistent_types, /*interval*/
        cast_inconsistent_types, /*rowid*/
        cast_inconsistent_types, /*lob*/
        cast_inconsistent_types, /*json*/
    },
    {
        /*raw -> XXX*/
        cast_not_expected,       /*null*/
        cast_inconsistent_types, /*int*/
        cast_inconsistent_types, /*uint*/
        cast_inconsistent_types, /*float*/
        cast_inconsistent_types, /*double*/
        cast_inconsistent_types, /*number*/
        cast_inconsistent_types, /*datetime*/
        cast_not_expected,       /*date*/
        cast_not_expected,       /*time*/
        cast_not_expected,       /*year*/
        raw_string,              /*string*/
        cast_not_expected,       /*extend*/
        cast_not_expected,       /*unknown*/
        raw_longtext,            /*text*/
        cast_not_expected,       /*bit*/
        cast_not_expected,       /*enumset*/
        cast_not_expected,       /*enumset_inner*/
        cast_inconsistent_types, /*otimestamp*/
        raw_raw,                 /*raw*/
        cast_inconsistent_types, /*interval*/
        cast_inconsistent_types, /*rowid*/
        raw_lob,                 /*lob*/
        cast_inconsistent_types, /*json*/
    },
    {
        /*interval -> XXX*/
        cast_not_expected,       /*null*/
        cast_inconsistent_types, /*int*/
        cast_inconsistent_types, /*uint*/
        cast_inconsistent_types, /*float*/
        cast_inconsistent_types, /*double*/
        cast_inconsistent_types, /*number*/
        cast_inconsistent_types, /*datetime*/
        cast_not_expected,       /*date*/
        cast_not_expected,       /*time*/
        cast_not_expected,       /*year*/
        interval_string,         /*string*/
        cast_not_expected,       /*extend*/
        cast_not_expected,       /*unknown*/
        cast_inconsistent_types, /*text*/
        cast_not_expected,       /*bit*/
        cast_not_expected,       /*enumset*/
        cast_not_expected,       /*enumset_inner*/
        cast_inconsistent_types, /*otimestamp*/
        cast_inconsistent_types, /*raw*/
        interval_interval,       /*interval*/
        cast_inconsistent_types, /*rowid*/
        cast_inconsistent_types, /*lob*/
        cast_inconsistent_types, /*json*/
    },
    {
        /* rowid -> XXX */
        cast_not_expected,       /*null*/
        cast_inconsistent_types, /*int*/
        cast_inconsistent_types, /*uint*/
        cast_inconsistent_types, /*float*/
        cast_inconsistent_types, /*double*/
        cast_inconsistent_types, /*number*/
        cast_inconsistent_types, /*datetime*/
        cast_not_expected,       /*date*/
        cast_not_expected,       /*time*/
        cast_not_expected,       /*year*/
        rowid_string,            /*string*/
        cast_not_expected,       /*extend*/
        cast_not_expected,       /*unknown*/
        cast_inconsistent_types, /*text*/
        cast_not_expected,       /*bit*/
        cast_not_expected,       /*enumset*/
        cast_not_expected,       /*enumset_inner*/
        cast_inconsistent_types, /*otimestamp*/
        cast_inconsistent_types, /*raw*/
        cast_inconsistent_types, /*interval*/
        rowid_rowid,             /*rowid*/
        cast_inconsistent_types, /*lob*/
        cast_inconsistent_types, /*json*/
    },
    {
        /*lob -> XXX*/
        cast_not_expected,       /*null*/
        lob_int,                 /*int*/
        lob_uint,                /*uint*/
        lob_float,               /*float*/
        lob_double,              /*double*/
        cast_not_support,        /*number*/
        lob_datetime,            /*datetime*/
        cast_not_expected,       /*date*/
        cast_not_expected,       /*time*/
        cast_not_expected,       /*year*/
        lob_string,              /*string*/
        cast_not_expected,       /*extend*/
        cast_not_expected,       /*unknown*/
        lob_string,              /*text*/
        cast_not_expected,       /*bit*/
        cast_not_expected,       /*enumset*/
        cast_not_expected,       /*enumset_inner*/
        lob_otimestamp,          /*otimestamp*/
        cast_inconsistent_types, /*raw*/
        lob_interval,            /*interval*/
        lob_rowid,               /*rowid*/
        lob_lob,                 /*lob*/
        cast_not_expected,       /*json*/
    },
    {
        /*json -> XXX*/
        cast_not_support,/*null*/
        cast_not_support,/*int*/
        cast_not_support,/*uint*/
        cast_not_support,/*float*/
        cast_not_support,/*double*/
        cast_not_support,/*number*/
        cast_not_support,/*datetime*/
        cast_not_support,/*date*/
        cast_not_support,/*time*/
        cast_not_support,/*year*/
        cast_not_support,/*string*/
        cast_not_support,/*extend*/
        cast_not_support,/*unknown*/
        cast_not_support,/*text*/
        cast_not_support,/*bit*/
        cast_not_support,/*enumset*/
        cast_not_support,/*enumset_inner*/
        cast_not_support,/*otimestamp*/
        cast_not_support,/*raw*/
        cast_not_support,/*interval*/
        cast_not_support,/*rowid*/
        cast_not_support,/*lob*/
        cast_not_support,/*json*/
   },
};

ObExpr::EvalFunc OB_DATUM_CAST_ORACLE_EXPLICIT[ObMaxTC][ObMaxTC] = {
    {
        /*null -> XXX*/
        cast_eval_arg, /*null*/
        cast_eval_arg, /*int*/
        cast_eval_arg, /*uint*/
        cast_eval_arg, /*float*/
        cast_eval_arg, /*double*/
        cast_eval_arg, /*number*/
        cast_eval_arg, /*datetime*/
        cast_eval_arg, /*date*/
        cast_eval_arg, /*time*/
        cast_eval_arg, /*year*/
        cast_eval_arg, /*string*/
        cast_eval_arg, /*extend*/
        cast_eval_arg, /*unknown*/
        cast_eval_arg, /*text*/
        cast_eval_arg, /*bit*/
        cast_eval_arg, /*enumset*/
        cast_eval_arg, /*enumsetInner*/
        cast_eval_arg, /*otimestamp*/
        cast_eval_arg, /*raw*/
        cast_eval_arg, /*interval*/
        cast_eval_arg, /*rowid*/
        cast_eval_arg, /*lob*/
        cast_eval_arg, /*json*/
    },
    {
        /*int -> XXX*/
        cast_not_support,        /*null*/
        int_int,                 /*int*/
        int_uint,                /*uint*/
        int_float,               /*float*/
        int_double,              /*double*/
        int_number,              /*number*/
        cast_not_support,        /*datetime*/
        cast_not_support,        /*date*/
        cast_not_support,        /*time*/
        cast_not_support,        /*year*/
        int_string,              /*string*/
        cast_not_support,        /*extend*/
        cast_not_support,        /*unknown*/
        int_string,              /*text*/
        cast_eval_arg,           /*bit*/
        cast_not_expected,       /*enumset*/
        cast_not_expected,       /*enumset_inner*/
        cast_not_support,        /*otimestamp*/
        cast_not_support,        /*raw*/
        cast_not_support,        /*interval*/
        cast_not_support,        /*rowid*/
        cast_inconsistent_types, /*lob*/
        cast_inconsistent_types, /*json*/
    },
    {
        /*uint -> XXX*/
        cast_not_support,        /*null*/
        uint_int,                /*int*/
        uint_uint,               /*uint*/
        uint_float,              /*float*/
        uint_double,             /*double*/
        uint_number,             /*number*/
        cast_not_support,        /*datetime*/
        cast_not_support,        /*date*/
        cast_not_support,        /*time*/
        cast_not_support,        /*year*/
        uint_string,             /*string*/
        cast_not_support,        /*extend*/
        cast_not_support,        /*unknown*/
        uint_string,             /*text*/
        cast_eval_arg,           /*bit*/
        cast_not_expected,       /*enumset*/
        cast_not_expected,       /*enumset_inner*/
        cast_not_support,        /*otimestamp*/
        cast_not_support,        /*raw*/
        cast_not_support,        /*interval*/
        cast_not_support,        /*rowid*/
        cast_inconsistent_types, /*lob*/
        cast_inconsistent_types, /*json*/
    },
    {
        /*float -> XXX*/
        cast_not_support,        /*null*/
        float_int,               /*int*/
        float_uint,              /*uint*/
        float_float,             /*float*/
        float_double,            /*double*/
        float_number,            /*number*/
        cast_not_support,        /*datetime*/
        cast_not_support,        /*date*/
        cast_not_support,        /*time*/
        cast_not_support,        /*year*/
        float_string,            /*string*/
        cast_not_support,        /*extend*/
        cast_not_support,        /*unknown*/
        float_string,            /*text*/
        float_bit,               /*bit*/
        cast_not_expected,       /*enumset*/
        cast_not_expected,       /*enumset_inner*/
        cast_not_support,        /*otimestamp*/
        cast_not_support,        /*raw*/
        cast_not_support,        /*interval*/
        cast_not_support,        /*rowid*/
        cast_inconsistent_types, /*lob*/
        cast_inconsistent_types, /*json*/
    },
    {
        /*double -> XXX*/
        cast_not_support,        /*null*/
        double_int,              /*int*/
        double_uint,             /*uint*/
        double_float,            /*float*/
        double_double,           /*double*/
        double_number,           /*number*/
        cast_not_support,        /*datetime*/
        cast_not_expected,       /*date*/
        cast_not_support,        /*time*/
        cast_not_support,        /*year*/
        double_string,           /*string*/
        cast_not_support,        /*extend*/
        cast_not_support,        /*unknown*/
        double_string,           /*text*/
        double_bit,              /*bit*/
        cast_not_expected,       /*enumset*/
        cast_not_expected,       /*enumset_inner*/
        cast_not_support,        /*otimestamp*/
        cast_not_support,        /*raw*/
        cast_not_support,        /*interval*/
        cast_not_support,        /*rowid*/
        cast_inconsistent_types, /*lob*/
        cast_inconsistent_types, /*json*/
    },
    {
        /*number -> XXX*/
        cast_not_support,        /*null*/
        number_int,              /*int*/
        number_uint,             /*uint*/
        number_float,            /*float*/
        number_double,           /*double*/
        number_number,           /*number*/
        cast_not_support,        /*datetime*/
        cast_not_support,        /*date*/
        cast_not_support,        /*time*/
        cast_not_support,        /*year*/
        number_string,           /*string*/
        cast_not_support,        /*extend*/
        cast_not_support,        /*unknown*/
        number_string,           /*text*/
        number_bit,              /*bit*/
        cast_not_expected,       /*enumset*/
        cast_not_expected,       /*enumset_inner*/
        cast_not_support,        /*otimestamp*/
        cast_not_support,        /*raw*/
        cast_not_support,        /*interval*/
        cast_not_support,        /*rowid*/
        cast_inconsistent_types, /*lob*/
        cast_inconsistent_types, /*json*/
    },
    {
        /*datetime -> XXX*/
        cast_not_support,        /*null*/
        cast_not_support,        /*int*/
        cast_not_support,        /*uint*/
        cast_not_support,        /*float*/
        cast_not_support,        /*double*/
        cast_not_support,        /*number*/
        datetime_datetime,       /*datetime*/
        cast_not_support,        /*date*/
        cast_not_support,        /*time*/
        cast_not_support,        /*year*/
        datetime_string,         /*string*/
        cast_not_support,        /*extend*/
        cast_not_support,        /*unknown*/
        datetime_string,         /*text*/
        datetime_bit,            /*bit*/
        cast_not_expected,       /*enumset*/
        cast_not_expected,       /*enumset_inner*/
        datetime_otimestamp,     /*otimestamp*/
        cast_not_support,        /*raw*/
        cast_not_support,        /*interval*/
        cast_not_support,        /*rowid*/
        cast_inconsistent_types, /*lob*/
        cast_inconsistent_types, /*json*/
    },
    {
        /*date -> XXX*/
        cast_not_expected, /*null*/
        cast_not_expected, /*int*/
        cast_not_expected, /*uint*/
        cast_not_expected, /*float*/
        cast_not_expected, /*double*/
        cast_not_expected, /*number*/
        cast_not_expected, /*datetime*/
        cast_not_expected, /*date*/
        cast_not_expected, /*time*/
        cast_not_expected, /*year*/
        cast_not_expected, /*string*/
        cast_not_expected, /*extend*/
        cast_not_expected, /*unknown*/
        cast_not_expected, /*text*/
        cast_not_expected, /*bit*/
        cast_not_expected, /*enumset*/
        cast_not_expected, /*enumset_inner*/
        cast_not_expected, /*otimestamp*/
        cast_not_expected, /*raw*/
        cast_not_expected, /*interval*/
        cast_not_support,  /*rowid*/
        cast_not_expected, /*lob*/
        cast_not_expected, /*json*/
    },
    {
        /*time -> XXX*/
        cast_not_support,  /*null*/
        cast_not_expected, /*int*/
        cast_not_expected, /*uint*/
        cast_not_expected, /*float*/
        cast_not_expected, /*double*/
        cast_not_expected, /*number*/
        cast_not_expected, /*datetime*/
        cast_not_expected, /*date*/
        cast_not_expected, /*time*/
        cast_not_expected, /*year*/
        cast_not_expected, /*string*/
        cast_not_expected, /*extend*/
        cast_not_expected, /*unknown*/
        cast_not_expected, /*text*/
        cast_not_expected, /*bit*/
        cast_not_expected, /*enumset*/
        cast_not_expected, /*enumset_inner*/
        cast_not_expected, /*otimestamp*/
        cast_not_expected, /*raw*/
        cast_not_expected, /*interval*/
        cast_not_support,  /*rowid*/
        cast_not_expected, /*lob*/
        cast_not_expected, /*json*/
    },
    {
        /*year -> XXX*/
        cast_not_expected, /*null*/
        cast_not_expected, /*int*/
        cast_not_expected, /*uint*/
        cast_not_expected, /*float*/
        cast_not_expected, /*double*/
        cast_not_expected, /*number*/
        cast_not_expected, /*datetime*/
        cast_not_expected, /*date*/
        cast_not_expected, /*time*/
        cast_not_expected, /*year*/
        cast_not_expected, /*string*/
        cast_not_expected, /*extend*/
        cast_not_expected, /*unknown*/
        cast_not_expected, /*text*/
        cast_not_expected, /*bit*/
        cast_not_expected, /*enumset*/
        cast_not_expected, /*enumset_inner*/
        cast_not_expected, /*otimestamp*/
        cast_not_expected, /*raw*/
        cast_not_expected, /*interval*/
        cast_not_support,  /*rowid*/
        cast_not_expected, /*lob*/
        cast_not_expected, /*json*/
    },
    {
        /*string -> XXX*/
        cast_not_support,        /*null*/
        string_int,              /*int*/
        string_uint,             /*uint*/
        string_float,            /*float*/
        string_double,           /*double*/
        string_number,           /*number*/
        string_datetime,         /*datetime*/
        cast_not_expected,       /*date*/
        cast_not_expected,       /*time*/
        cast_not_expected,       /*year*/
        string_string,           /*string*/
        cast_not_support,        /*extend*/
        cast_not_support,        /*unknown*/
        string_string,           /*text*/
        string_bit,              /*bit*/
        cast_not_expected,       /*enumset*/
        cast_not_expected,       /*enumset_inner*/
        string_otimestamp,       /*otimestamp*/
        string_raw,              /*raw*/
        string_interval,         /*interval*/
        string_rowid,            /*rowid*/
        cast_inconsistent_types, /*lob*/
        cast_inconsistent_types, /*json*/
    },
    {
        /*extend -> XXX*/
        cast_not_support, /*null*/
        cast_not_support, /*int*/
        cast_not_support, /*uint*/
        cast_not_support, /*float*/
        cast_not_support, /*double*/
        cast_not_support, /*number*/
        cast_not_support, /*datetime*/
        cast_not_support, /*date*/
        cast_not_support, /*time*/
        cast_not_support, /*year*/
        cast_not_support, /*string*/
        cast_eval_arg,    /*extend*/
        cast_not_support, /*unknown*/
        cast_not_support, /*text*/
        cast_not_support, /*bit*/
        cast_not_support, /*enumset*/
        cast_not_support, /*enumset_inner*/
        cast_not_support, /*otimestamp*/
        cast_not_support, /*raw*/
        cast_not_support, /*interval*/
        cast_not_support, /*rowid*/
        cast_not_support, /*lob*/
        cast_not_support, /*json*/
    },
    {
        /*unknown -> XXX*/
        unknown_other,    /*null*/
        unknown_other,    /*int*/
        unknown_other,    /*uint*/
        unknown_other,    /*float*/
        unknown_other,    /*double*/
        unknown_other,    /*number*/
        unknown_other,    /*datetime*/
        unknown_other,    /*date*/
        unknown_other,    /*time*/
        unknown_other,    /*year*/
        unknown_other,    /*string*/
        unknown_other,    /*extend*/
        cast_eval_arg,    /*unknown*/
        cast_not_support, /*text*/
        unknown_other,    /*bit*/
        unknown_other,    /*enumset*/
        unknown_other,    /*enumsetInner*/
        unknown_other,    /*otimestamp*/
        unknown_other,    /*raw*/
        unknown_other,    /*interval*/
        unknown_other,    /*rowid*/
        cast_not_support, /*lob*/
        cast_not_support, /*json*/
    },
    {
        /*text -> XXX*/
        cast_not_support,        /*null*/
        string_int,              /*int*/
        string_uint,             /*uint*/
        string_float,            /*float*/
        string_double,           /*double*/
        string_number,           /*number*/
        string_datetime,         /*datetime*/
        cast_not_expected,       /*date*/
        cast_not_expected,       /*time*/
        cast_not_expected,       /*year*/
        string_string,           /*string*/
        cast_not_support,        /*extend*/
        cast_not_support,        /*unknown*/
        string_string,           /*text*/
        string_bit,              /*bit*/
        cast_not_expected,       /*enumset*/
        cast_not_expected,       /*enumset_inner*/
        string_otimestamp,       /*otimestamp*/
        cast_not_support,        /*raw*/
        string_interval,         /*interval*/
        cast_not_support,        /*rowid*/
        cast_inconsistent_types, /*lob*/
        cast_inconsistent_types, /*json*/
    },
    {
        /*bit -> XXX*/
        cast_not_expected, /*null*/
        cast_not_expected, /*int*/
        cast_not_expected, /*uint*/
        cast_not_expected, /*float*/
        cast_not_expected, /*double*/
        cast_not_expected, /*number*/
        cast_not_expected, /*datetime*/
        cast_not_expected, /*date*/
        cast_not_expected, /*time*/
        cast_not_expected, /*year*/
        cast_not_expected, /*string*/
        cast_not_expected, /*extend*/
        cast_not_expected, /*unknown*/
        cast_not_expected, /*text*/
        cast_not_expected, /*bit*/
        cast_not_expected, /*enumset*/
        cast_not_expected, /*enumset_inner*/
        cast_not_expected, /*otimestamp*/
        cast_not_expected, /*raw*/
        cast_not_expected, /*interval*/
        cast_not_expected, /*rowid*/
        cast_not_expected, /*lob*/
        cast_not_expected, /*json*/
    },
    {
        /*enum -> XXX*/
        cast_not_expected, /*null*/
        cast_not_expected, /*int*/
        cast_not_expected, /*uint*/
        cast_not_expected, /*float*/
        cast_not_expected, /*double*/
        cast_not_expected, /*number*/
        cast_not_expected, /*datetime*/
        cast_not_expected, /*date*/
        cast_not_expected, /*time*/
        cast_not_expected, /*year*/
        cast_not_expected, /*string*/
        cast_not_expected, /*extend*/
        cast_not_expected, /*unknown*/
        cast_not_expected, /*text*/
        cast_not_expected, /*bit*/
        cast_not_expected, /*enumset*/
        cast_not_expected, /*enumset_inner*/
        cast_not_expected, /*otimestamp*/
        cast_not_expected, /*raw*/
        cast_not_expected, /*interval*/
        cast_not_expected, /*rowid*/
        cast_not_expected, /*lob*/
        cast_not_expected, /*json*/
    },
    {
        /*enumset_inner -> XXX*/
        cast_not_expected, /*null*/
        cast_not_expected, /*int*/
        cast_not_expected, /*uint*/
        cast_not_expected, /*float*/
        cast_not_expected, /*double*/
        cast_not_expected, /*number*/
        cast_not_expected, /*datetime*/
        cast_not_expected, /*date*/
        cast_not_expected, /*time*/
        cast_not_expected, /*year*/
        cast_not_expected, /*string*/
        cast_not_expected, /*extend*/
        cast_not_expected, /*unknown*/
        cast_not_expected, /*text*/
        cast_not_expected, /*bit*/
        cast_not_expected, /*enumset*/
        cast_not_expected, /*enumset_inner*/
        cast_not_expected, /*otimestamp*/
        cast_not_expected, /*raw*/
        cast_not_expected, /*interval*/
        cast_not_expected, /*rowid*/
        cast_not_expected, /*lob*/
        cast_not_expected, /*json*/
    },
    {
        /*otimestamp -> XXX*/
        cast_not_support,        /*null*/
        cast_not_support,        /*int*/
        cast_not_support,        /*uint*/
        cast_not_support,        /*float*/
        cast_not_support,        /*double*/
        cast_not_support,        /*number*/
        otimestamp_datetime,     /*datetime*/
        cast_not_expected,       /*date*/
        cast_not_expected,       /*time*/
        cast_not_expected,       /*year*/
        otimestamp_string,       /*string*/
        cast_not_support,        /*extend*/
        cast_not_support,        /*unknown*/
        otimestamp_string,       /*text*/
        cast_not_support,        /*bit*/
        cast_not_expected,       /*enumset*/
        cast_not_expected,       /*enumset_inner*/
        otimestamp_otimestamp,   /*otimestamp*/
        cast_not_support,        /*raw*/
        cast_not_support,        /*interval*/
        cast_not_support,        /*rowid*/
        cast_inconsistent_types, /*lob*/
        cast_inconsistent_types, /*json*/
    },
    {
        /*raw -> XXX*/
        cast_not_support,        /*null*/
        cast_not_support,        /*int*/
        cast_not_support,        /*uint*/
        cast_not_support,        /*float*/
        cast_not_support,        /*double*/
        cast_not_support,        /*number*/
        cast_not_support,        /*datetime*/
        cast_not_expected,       /*date*/
        cast_not_expected,       /*time*/
        cast_not_expected,       /*year*/
        raw_string,              /*string*/
        cast_not_support,        /*extend*/
        cast_not_support,        /*unknown*/
        raw_longtext,            /*text*/
        cast_not_support,        /*bit*/
        cast_not_expected,       /*enumset*/
        cast_not_expected,       /*enumset_inner*/
        cast_not_support,        /*otimestamp*/
        raw_raw,                 /*raw*/
        cast_not_support,        /*interval*/
        cast_not_support,        /*rowid*/
        cast_inconsistent_types, /*lob*/
        cast_inconsistent_types, /*json*/
    },
    {
        /*interval -> XXX*/
        cast_not_expected,       /*null*/
        cast_not_expected,       /*int*/
        cast_not_expected,       /*uint*/
        cast_not_expected,       /*float*/
        cast_not_expected,       /*double*/
        cast_not_expected,       /*number*/
        cast_not_expected,       /*datetime*/
        cast_not_expected,       /*date*/
        cast_not_expected,       /*time*/
        cast_not_expected,       /*year*/
        interval_string,         /*string*/
        cast_not_expected,       /*extend*/
        cast_not_expected,       /*unknown*/
        cast_not_expected,       /*text*/
        cast_not_expected,       /*bit*/
        cast_not_expected,       /*enumset*/
        cast_not_expected,       /*enumset_inner*/
        cast_not_expected,       /*otimestamp*/
        cast_not_expected,       /*raw*/
        cast_not_support,        /*interval*/
        cast_not_support,        /*rowid*/
        cast_inconsistent_types, /*lob*/
        cast_inconsistent_types, /*json*/
    },
    {
        /*rowid -> XXX*/
        cast_not_support,        /*null*/
        cast_not_support,        /*int*/
        cast_not_support,        /*uint*/
        cast_not_support,        /*float*/
        cast_not_support,        /*double*/
        cast_not_support,        /*number*/
        cast_not_support,        /*datetime*/
        cast_not_expected,       /*date*/
        cast_not_expected,       /*time*/
        cast_not_expected,       /*year*/
        rowid_string,            /*string*/
        cast_not_expected,       /*extend*/
        cast_not_expected,       /*unknown*/
        cast_not_expected,       /*text*/
        cast_not_expected,       /*bit*/
        cast_not_expected,       /*enumset*/
        cast_not_expected,       /*enumset_inner*/
        cast_not_support,        /*otimestamp*/
        cast_not_support,        /*raw*/
        cast_not_support,        /*interval*/
        rowid_rowid,             /*rowid*/
        cast_inconsistent_types, /*lob*/
        cast_inconsistent_types, /*json*/
    },
    {
        /*lob -> XXX*/
        cast_not_support,        /*null*/
        lob_int,                 /*int*/
        lob_uint,                /*uint*/
        lob_float,               /*float*/
        lob_double,              /*double*/
        lob_number,              /*number*/
        lob_datetime,            /*datetime*/
        cast_not_expected,       /*date*/
        cast_not_expected,       /*time*/
        cast_not_expected,       /*year*/
        lob_string,              /*string*/
        cast_not_support,        /*extend*/
        cast_not_support,        /*unknown*/
        lob_string,              /*text*/
        lob_bit,                 /*bit*/
        cast_not_expected,       /*enumset*/
        cast_not_expected,       /*enumset_inner*/
        lob_otimestamp,          /*otimestamp*/
        cast_not_support,        /*raw*/
        lob_interval,            /*interval*/
        cast_not_support,        /*rowid*/
        cast_inconsistent_types, /*lob*/
        cast_inconsistent_types, /*json*/
    },
    {
        /*json -> XXX*/
        cast_not_support,/*null*/
        cast_not_support,/*int*/
        cast_not_support,/*uint*/
        cast_not_support,/*float*/
        cast_not_support,/*double*/
        cast_not_support,/*number*/
        cast_not_support,/*datetime*/
        cast_not_support,/*date*/
        cast_not_support,/*time*/
        cast_not_support,/*year*/
        cast_not_support,/*string*/
        cast_not_support,/*extend*/
        cast_not_support,/*unknown*/
        cast_not_support,/*text*/
        cast_not_support,/*bit*/
        cast_not_support,/*enumset*/
        cast_not_support,/*enumset_inner*/
        cast_not_support,/*otimestamp*/
        cast_not_support,/*raw*/
        cast_not_support,/*interval*/
        cast_not_support,/*rowid*/
        cast_not_support,/*lob*/
        cast_not_support,/*json*/
   },
};

ObExpr::EvalFunc OB_DATUM_CAST_MYSQL_IMPLICIT[ObMaxTC][ObMaxTC] = {
    {
        /*null -> XXX*/
        cast_eval_arg,           /*null*/
        cast_eval_arg,           /*int*/
        cast_eval_arg,           /*uint*/
        cast_eval_arg,           /*float*/
        cast_eval_arg,           /*double*/
        cast_eval_arg,           /*number*/
        cast_eval_arg,           /*datetime*/
        cast_eval_arg,           /*date*/
        cast_eval_arg,           /*time*/
        cast_eval_arg,           /*year*/
        cast_eval_arg,           /*string*/
        cast_eval_arg,           /*extend*/
        cast_eval_arg,           /*unknown*/
        cast_eval_arg,           /*text*/
        cast_eval_arg,           /*bit*/
        cast_eval_arg,           /*enumset*/
        cast_eval_arg,           /*enumsetInner*/
        cast_eval_arg,           /*otimestamp*/
        cast_inconsistent_types, /*raw*/
        cast_not_expected,       /*interval*/
        cast_not_expected,       /*rowid*/
        cast_not_expected,       /*lob*/
        cast_eval_arg,           /*json*/
    },
    {
        /*int -> XXX*/
        cast_not_support,        /*null*/
        int_int,                 /*int*/
        int_uint,                /*uint*/
        int_float,               /*float*/
        int_double,              /*double*/
        int_number,              /*number*/
        int_datetime,            /*datetime*/
        int_date,                /*date*/
        int_time,                /*time*/
        int_year,                /*year*/
        int_string,              /*string*/
        cast_not_support,        /*extend*/
        cast_not_support,        /*unknown*/
        int_string,              /*text*/
        cast_eval_arg,           /*bit*/
        cast_not_expected,       /*enumset*/
        cast_not_expected,       /*enumset_inner*/
        cast_not_support,        /*otimestamp*/
        cast_inconsistent_types, /*raw*/
        cast_not_expected,       /*interval*/
        cast_not_expected,       /*rowid*/
        cast_not_expected,       /*lob*/
        int_json,                /*json*/
    },
    {
        /*uint -> XXX*/
        cast_not_support,        /*null*/
        uint_int,                /*int*/
        uint_uint,               /*uint*/
        uint_float,              /*float*/
        uint_double,             /*double*/
        uint_number,             /*number*/
        uint_datetime,           /*datetime*/
        uint_date,               /*date*/
        uint_time,               /*time*/
        uint_year,               /*year*/
        uint_string,             /*string*/
        cast_not_support,        /*extend*/
        cast_not_support,        /*unknown*/
        uint_string,             /*text*/
        cast_eval_arg,           /*bit*/
        cast_not_expected,       /*enumset*/
        cast_not_expected,       /*enumset_inner*/
        cast_not_support,        /*otimestamp*/
        cast_inconsistent_types, /*raw*/
        cast_not_expected,       /*interval*/
        cast_not_expected,       /*rowid*/
        cast_not_expected,       /*lob*/
        uint_json,               /*json*/
    },
    {
        /*float -> XXX*/
        cast_not_support,        /*null*/
        float_int,               /*int*/
        float_uint,              /*uint*/
        float_float,             /*float*/
        float_double,            /*double*/
        float_number,            /*number*/
        float_datetime,          /*datetime*/
        float_date,              /*date*/
        float_time,              /*time*/
        cast_not_support,        /*year*/
        float_string,            /*string*/
        cast_not_support,        /*extend*/
        cast_not_support,        /*unknown*/
        float_string,            /*text*/
        float_bit,               /*bit*/
        cast_not_expected,       /*enumset*/
        cast_not_expected,       /*enumset_inner*/
        cast_not_support,        /*otimestamp*/
        cast_inconsistent_types, /*raw*/
        cast_not_expected,       /*interval*/
        cast_not_expected,       /*rowid*/
        cast_not_expected,       /*lob*/
        float_json,              /*json*/
    },
    {
        /*double -> XXX*/
        cast_not_support,        /*null*/
        double_int,              /*int*/
        double_uint,             /*uint*/
        double_float,            /*float*/
        double_double,           /*double*/
        double_number,           /*number*/
        double_datetime,         /*datetime*/
        double_date,             /*date*/
        double_time,             /*time*/
        cast_not_support,        /*year*/
        double_string,           /*string*/
        cast_not_support,        /*extend*/
        cast_not_support,        /*unknown*/
        double_string,           /*text*/
        double_bit,              /*bit*/
        cast_not_expected,       /*enumset*/
        cast_not_expected,       /*enumset_inner*/
        cast_not_support,        /*otimestamp*/
        cast_inconsistent_types, /*raw*/
        cast_not_expected,       /*interval*/
        cast_not_expected,       /*rowid*/
        cast_not_expected,       /*lob*/
        double_json,             /*json*/
    },
    {
        /*number -> XXX*/
        cast_not_support,        /*null*/
        number_int,              /*int*/
        number_uint,             /*uint*/
        number_float,            /*float*/
        number_double,           /*double*/
        number_number,           /*number*/
        number_datetime,         /*datetime*/
        number_date,             /*date*/
        number_time,             /*time*/
        number_year,             /*year*/
        number_string,           /*string*/
        cast_not_support,        /*extend*/
        cast_not_support,        /*unknown*/
        number_string,           /*text*/
        number_bit,              /*bit*/
        cast_not_expected,       /*enumset*/
        cast_not_expected,       /*enumset_inner*/
        cast_not_support,        /*otimestamp*/
        cast_inconsistent_types, /*raw*/
        cast_not_expected,       /*interval*/
        cast_not_expected,       /*rowid*/
        cast_not_expected,       /*lob*/
        number_json,             /*json*/
    },
    {
        /*datetime -> XXX*/
        cast_not_support,        /*null*/
        datetime_int,            /*int*/
        datetime_uint,           /*uint*/
        datetime_float,          /*float*/
        datetime_double,         /*double*/
        datetime_number,         /*number*/
        datetime_datetime,       /*datetime*/
        datetime_date,           /*date*/
        datetime_time,           /*time*/
        datetime_year,           /*year*/
        datetime_string,         /*string*/
        cast_not_support,        /*extend*/
        cast_not_support,        /*unknown*/
        datetime_string,         /*text*/
        datetime_bit,            /*bit*/
        cast_not_expected,       /*enumset*/
        cast_not_expected,       /*enumset_inner*/
        datetime_otimestamp,     /*otimestamp*/
        cast_inconsistent_types, /*raw*/
        cast_not_expected,       /*interval*/
        cast_not_expected,       /*rowid*/
        cast_not_expected,       /*lob*/
        datetime_json,           /*json*/
    },
    {
        /*date -> XXX*/
        cast_not_support,        /*null*/
        date_int,                /*int*/
        date_uint,               /*uint*/
        date_float,              /*float*/
        date_double,             /*double*/
        date_number,             /*number*/
        date_datetime,           /*datetime*/
        cast_eval_arg,           /*date*/
        date_time,               /*time*/
        date_year,               /*year*/
        date_string,             /*string*/
        cast_not_support,        /*extend*/
        cast_not_support,        /*unknown*/
        date_string,             /*text*/
        date_bit,                /*bit*/
        cast_not_expected,       /*enumset*/
        cast_not_expected,       /*enumset_inner*/
        cast_not_support,        /*otimestamp*/
        cast_inconsistent_types, /*raw*/
        cast_not_expected,       /*interval*/
        cast_not_expected,       /*rowid*/
        cast_not_expected,       /*lob*/
        date_json,               /*json*/
    },
    {
        /*time -> XXX*/
        cast_not_support,        /*null*/
        time_int,                /*int*/
        time_uint,               /*uint*/
        time_float,              /*float*/
        time_double,             /*double*/
        time_number,             /*number*/
        time_datetime,           /*datetime*/
        time_date,               /*date*/
        cast_eval_arg,           /*time*/
        cast_not_support,        /*year*/
        time_string,             /*string*/
        cast_not_support,        /*extend*/
        cast_not_support,        /*unknown*/
        time_string,             /*text*/
        time_bit,                /*bit*/
        cast_not_expected,       /*enumset*/
        cast_not_expected,       /*enumset_inner*/
        cast_not_support,        /*otimestamp*/
        cast_inconsistent_types, /*raw*/
        cast_not_expected,       /*interval*/
        cast_not_expected,       /*rowid*/
        cast_not_expected,       /*lob*/
        time_json,               /*json*/
    },
    {
        /*year -> XXX*/
        cast_not_support,        /*null*/
        year_int,                /*int*/
        year_uint,               /*uint*/
        year_float,              /*float*/
        year_double,             /*double*/
        year_number,             /*number*/
        year_datetime,           /*datetime*/
        year_date,               /*date*/
        cast_not_support,        /*time*/
        cast_eval_arg,           /*year*/
        year_string,             /*string*/
        cast_not_support,        /*extend*/
        cast_not_support,        /*unknown*/
        year_string,             /*text*/
        year_bit,                /*bit*/
        cast_not_expected,       /*enumset*/
        cast_not_expected,       /*enumset_inner*/
        cast_not_support,        /*otimestamp*/
        cast_inconsistent_types, /*raw*/
        cast_not_expected,       /*interval*/
        cast_not_expected,       /*rowid*/
        cast_not_expected,       /*lob*/
        year_json,               /*json*/
    },
    {
        /*string -> XXX*/
        cast_not_support,        /*null*/
        string_int,              /*int*/
        string_uint,             /*uint*/
        string_float,            /*float*/
        string_double,           /*double*/
        string_number,           /*number*/
        string_datetime,         /*datetime*/
        string_date,             /*date*/
        string_time,             /*time*/
        string_year,             /*year*/
        string_string,           /*string*/
        cast_not_support,        /*extend*/
        cast_not_support,        /*unknown*/
        string_string,           /*text*/
        string_bit,              /*bit*/
        cast_not_expected,       /*enumset*/
        cast_not_expected,       /*enumset_inner*/
        string_otimestamp,       /*otimestamp*/
        cast_inconsistent_types, /*raw*/
        cast_not_expected,       /*interval*/
        cast_not_expected,       /*rowid*/
        cast_not_expected,       /*lob*/
        string_json,             /*json*/
    },
    {
        /*extend -> XXX*/
        cast_not_support,        /*null*/
        cast_not_support,        /*int*/
        cast_not_support,        /*uint*/
        cast_not_support,        /*float*/
        cast_not_support,        /*double*/
        cast_not_support,        /*number*/
        cast_not_support,        /*datetime*/
        cast_not_support,        /*date*/
        cast_not_support,        /*time*/
        cast_not_support,        /*year*/
        cast_not_support,        /*string*/
        cast_eval_arg,           /*extend*/
        cast_not_support,        /*unknown*/
        cast_not_support,        /*text*/
        cast_not_support,        /*bit*/
        cast_not_support,        /*enumset*/
        cast_not_support,        /*enumset_inner*/
        cast_not_support,        /*otimestamp*/
        cast_inconsistent_types, /*raw*/
        cast_not_expected,       /*interval*/
        cast_not_expected,       /*rowid*/
        cast_not_expected,       /*lob*/
        cast_not_support,        /*json*/
    },
    {
        /*unknown -> XXX*/
        unknown_other,           /*null*/
        unknown_other,           /*int*/
        unknown_other,           /*uint*/
        unknown_other,           /*float*/
        unknown_other,           /*double*/
        unknown_other,           /*number*/
        unknown_other,           /*datetime*/
        unknown_other,           /*date*/
        unknown_other,           /*time*/
        unknown_other,           /*year*/
        unknown_other,           /*string*/
        unknown_other,           /*extend*/
        cast_eval_arg,           /*unknown*/
        cast_not_support,        /*text*/
        unknown_other,           /*bit*/
        unknown_other,           /*enumset*/
        unknown_other,           /*enumsetInner*/
        unknown_other,           /*otimestamp*/
        cast_inconsistent_types, /*raw*/
        cast_not_expected,       /*interval*/
        cast_not_expected,       /*rowid*/
        cast_not_expected,       /*lob*/
        cast_not_expected,       /*json*/
    },
    {
        /*text -> XXX*/
        cast_not_support,        /*null*/
        string_int,              /*int*/
        string_uint,             /*uint*/
        string_float,            /*float*/
        string_double,           /*double*/
        string_number,           /*number*/
        string_datetime,         /*datetime*/
        string_date,             /*date*/
        string_time,             /*time*/
        string_year,             /*year*/
        string_string,           /*string*/
        cast_not_support,        /*extend*/
        cast_not_support,        /*unknown*/
        string_string,           /*text*/
        string_bit,              /*bit*/
        cast_not_expected,       /*enumset*/
        cast_not_expected,       /*enumset_inner*/
        string_otimestamp,       /*otimestamp*/
        cast_inconsistent_types, /*raw*/
        cast_not_expected,       /*interval*/
        cast_not_expected,       /*rowid*/
        cast_not_expected,       /*lob*/
        string_json,             /*json*/
    },
    {
        /*bit -> XXX*/
        cast_not_support,        /*null*/
        bit_int,                 /*int*/
        bit_uint,                /*uint*/
        bit_float,               /*float*/
        bit_double,              /*double*/
        bit_number,              /*number*/
        bit_datetime,            /*datetime*/
        bit_date,                /*date*/
        bit_time,                /*time*/
        bit_year,                /*year*/
        bit_string,              /*string*/
        cast_not_support,        /*extend*/
        cast_not_support,        /*unknown*/
        bit_string,              /*text*/
        cast_eval_arg,           /*bit*/
        cast_not_expected,       /*enumset*/
        cast_not_expected,       /*enumset_inner*/
        cast_not_support,        /*otimestamp*/
        cast_inconsistent_types, /*raw*/
        cast_not_expected,       /*interval*/
        cast_not_expected,       /*rowid*/
        cast_not_expected,       /*lob*/
        bit_json,                /*json*/
    },
    {
        /*enumset -> XXX*/
        cast_not_support,        /*null*/
        enumset_int,             // /*int*/
        enumset_uint,            // /*uint*/
        enumset_float,           // /*float*/
        enumset_double,          // /*double*/
        enumset_number,          // /*number*/
        cast_not_expected,       /*datetime*/
        cast_not_expected,       /*date*/
        cast_not_expected,       /*time*/
        enumset_year,            // /*year*/
        cast_not_expected,       /*string*/
        cast_not_support,        /*extend*/
        cast_not_support,        /*unknown*/
        cast_not_expected,       /*text*/
        enumset_bit,             // /*bit*/
        cast_not_expected,       /*enumset*/
        cast_not_expected,       /*enumset_inner*/
        cast_not_support,        /*otimestamp*/
        cast_inconsistent_types, /*raw*/
        cast_not_expected,       /*interval*/
        cast_not_expected,       /*rowid*/
        cast_not_expected,       /*lob*/
        cast_not_expected,       /*json*/
    },
    {
        /*enumset_inner -> XXX*/
        cast_not_support,        /*null*/
        enumset_inner_int,       // /*int*/
        enumset_inner_uint,      // /*uint*/
        enumset_inner_float,     // /*float*/
        enumset_inner_double,    // /*double*/
        enumset_inner_number,    // /*number*/
        enumset_inner_datetime,  // /*datetime*/
        enumset_inner_date,      // /*date*/
        enumset_inner_time,      // /*time*/
        enumset_inner_year,      // /*year*/
        enumset_inner_string,    // /*string*/
        cast_not_support,        /*extend*/
        cast_not_support,        /*unknown*/
        cast_not_support,        /*text*/
        enumset_inner_bit,       // /*bit*/
        cast_not_expected,       /*enumset*/
        cast_not_expected,       /*enumset_inner*/
        cast_not_expected,       /*otimestamp*/
        cast_inconsistent_types, /*raw*/
        cast_not_expected,       /*interval*/
        cast_not_expected,       /*rowid*/
        cast_not_expected,       /*lob*/
        cast_not_expected,       /*json*/
    },
    {
        /*otimestamp -> XXX*/
        cast_not_expected,       /*null*/
        cast_not_expected,       /*int*/
        cast_not_expected,       /*uint*/
        cast_not_expected,       /*float*/
        cast_not_expected,       /*double*/
        cast_not_expected,       /*number*/
        cast_not_expected,       /*datetime*/
        cast_not_expected,       /*date*/
        cast_not_expected,       /*time*/
        cast_not_expected,       /*year*/
        cast_not_expected,       /*string*/
        cast_not_expected,       /*extend*/
        cast_not_expected,       /*unknown*/
        cast_not_expected,       /*text*/
        cast_not_expected,       /*bit*/
        cast_not_expected,       /*enumset*/
        cast_not_expected,       /*enumset_inner*/
        otimestamp_otimestamp,   /*otimestamp*/
        cast_inconsistent_types, /*raw*/
        cast_not_expected,       /*interval*/
        cast_not_expected,       /*rowid*/
        cast_not_expected,       /*lob*/
        cast_not_expected,       /*json*/
    },
    {
        /*raw -> XXX*/
        cast_not_expected, /*null*/
        cast_not_expected, /*int*/
        cast_not_expected, /*uint*/
        cast_not_expected, /*float*/
        cast_not_expected, /*double*/
        cast_not_expected, /*number*/
        cast_not_expected, /*datetime*/
        cast_not_expected, /*date*/
        cast_not_expected, /*time*/
        cast_not_expected, /*year*/
        cast_not_expected, /*string*/
        cast_not_expected, /*extend*/
        cast_not_expected, /*unknown*/
        cast_not_expected, /*text*/
        cast_not_expected, /*bit*/
        cast_not_expected, /*enumset*/
        cast_not_expected, /*enumset_inner*/
        cast_not_expected, /*otimestamp*/
        cast_not_expected, /*raw*/
        cast_not_expected, /*interval*/
        cast_not_expected, /*rowid*/
        cast_not_expected, /*lob*/
        cast_not_expected, /*json*/
    },
    {
        /*interval -> XXX*/
        cast_not_expected, /*null*/
        cast_not_expected, /*int*/
        cast_not_expected, /*uint*/
        cast_not_expected, /*float*/
        cast_not_expected, /*double*/
        cast_not_expected, /*number*/
        cast_not_expected, /*datetime*/
        cast_not_expected, /*date*/
        cast_not_expected, /*time*/
        cast_not_expected, /*year*/
        cast_not_expected, /*string*/
        cast_not_expected, /*extend*/
        cast_not_expected, /*unknown*/
        cast_not_expected, /*text*/
        cast_not_expected, /*bit*/
        cast_not_expected, /*enumset*/
        cast_not_expected, /*enumset_inner*/
        cast_not_expected, /*otimestamp*/
        cast_not_expected, /*raw*/
        cast_not_expected, /*interval*/
        cast_not_expected, /*rowid*/
        cast_not_expected, /*lob*/
        cast_not_expected, /*json*/
    },
    {
        /*rowid -> XXX*/
        cast_not_expected, /*null*/
        cast_not_expected, /*int*/
        cast_not_expected, /*uint*/
        cast_not_expected, /*float*/
        cast_not_expected, /*double*/
        cast_not_expected, /*number*/
        cast_not_expected, /*datetime*/
        cast_not_expected, /*date*/
        cast_not_expected, /*time*/
        cast_not_expected, /*year*/
        cast_not_expected, /*string*/
        cast_not_expected, /*extend*/
        cast_not_expected, /*unknown*/
        cast_not_expected, /*text*/
        cast_not_expected, /*bit*/
        cast_not_expected, /*enumset*/
        cast_not_expected, /*enumset_inner*/
        cast_not_expected, /*otimestamp*/
        cast_not_expected, /*raw*/
        cast_not_expected, /*interval*/
        cast_not_expected, /*rowid*/
        cast_not_expected, /*lob*/
        cast_not_expected,  /*json*/
    },
    {
        /*lob -> XXX*/
        cast_not_expected, /*null*/
        cast_not_expected, /*int*/
        cast_not_expected, /*uint*/
        cast_not_expected, /*float*/
        cast_not_expected, /*double*/
        cast_not_expected, /*number*/
        cast_not_expected, /*datetime*/
        cast_not_expected, /*date*/
        cast_not_expected, /*time*/
        cast_not_expected, /*year*/
        cast_not_expected, /*string*/
        cast_not_expected, /*extend*/
        cast_not_expected, /*unknown*/
        cast_not_expected, /*text*/
        cast_not_expected, /*bit*/
        cast_not_expected, /*enumset*/
        cast_not_expected, /*enumset_inner*/
        cast_not_expected, /*otimestamp*/
        cast_not_expected, /*raw*/
        cast_not_expected, /*interval*/
        cast_not_expected, /*rowid*/
        cast_not_expected, /*lob*/
        cast_not_expected, /*json*/
    },
    {
        /*json -> XXX*/
        cast_not_support,/*null*/
        json_int,/*int*/
        json_uint,/*uint*/
        json_float,/*float*/
        json_double,/*double*/
        json_number,/*number*/
        json_datetime,/*datetime*/
        json_date,/*date*/
        json_time,/*time*/
        json_year,/*year*/
        json_string,/*string*/
        cast_not_support,/*extend*/
        cast_not_support,/*unknown*/
        json_string,/*text*/
        json_bit,/*bit*/
        cast_not_expected,/*enumset*/
        cast_not_expected,/*enumset_inner*/
        json_otimestamp,/*otimestamp*/
        cast_inconsistent_types,/*raw*/
        cast_not_expected,/*interval*/
        cast_not_expected,/*rowid*/
        cast_not_expected,/*lob*/
        json_json,/*json*/
   },
};

ObExpr::EvalEnumSetFunc OB_DATUM_CAST_MYSQL_ENUMSET_IMPLICIT[ObMaxTC][2] = {
    {
        /*null -> enum_or_set*/
        cast_identity_enum_set, /*enum*/
        cast_identity_enum_set, /*set*/
    },
    {
        /*int -> enum_or_set*/
        int_enum, /*enum*/
        int_set,  /*set*/
    },
    {
        /*uint -> enum_or_set*/
        uint_enum, /*enum*/
        uint_set,  /*set*/
    },
    {
        /*float -> enum_or_set*/
        float_enum, /*enum*/
        float_set,  /*set*/
    },
    {
        /*double -> enum_or_set*/
        double_enum, /*enum*/
        double_set,  /*set*/
    },
    {
        /*number -> enum_or_set*/
        number_enum, /*enum*/
        number_set,  /*set*/
    },
    {
        /*datetime -> enum_or_set*/
        datetime_enum, /*enum*/
        datetime_set,  /*set*/
    },
    {
        /*date -> enum_or_set*/
        date_enum, /*enum*/
        date_set,  /*set*/
    },
    {
        /*time -> enum_or_set*/
        time_enum, /*enum*/
        time_set,  /*set*/
    },
    {
        /*year -> enum_or_set*/
        year_enum, /*enum*/
        year_set,  /*set*/
    },
    {
        /*string -> enum_or_set*/
        string_enum, /*enum*/
        string_set,  /*set*/
    },
    {
        /*extend -> enum_or_set*/
        cast_not_support_enum_set, /*enum*/
        cast_not_support_enum_set, /*set*/
    },
    {
        /*unknow -> enum_or_set*/
        cast_not_support_enum_set, /*enum*/
        cast_not_support_enum_set, /*set*/
    },
    {
        /*text -> enum_or_set*/
        string_enum, /*enum*/
        string_set,  /*set*/
    },
    {
        /*bit -> enum_or_set*/
        bit_enum, /*enum*/
        bit_set,  /*set*/
    },
    {
        /*enumset tc -> enum_or_set*/
        cast_not_expected_enum_set, /*enum*/
        cast_not_expected_enum_set, /*set*/
    },
    {
        /*enumset_inner tc -> enum_or_set*/
        cast_not_expected_enum_set, /*enum*/
        cast_not_expected_enum_set, /*set*/
    },
    {
        /*unknow -> enum_or_set*/
        cast_not_support_enum_set, /*enum*/
        cast_not_support_enum_set, /*set*/
    },
    {
        /*unknow -> enum_or_set*/
        cast_not_support_enum_set, /*enum*/
        cast_not_support_enum_set, /*set*/
    },
    {
        /*unknow -> enum_or_set*/
        cast_not_support_enum_set, /*enum*/
        cast_not_support_enum_set, /*set*/
    },
    {
        /*unknow -> enum_or_set*/
        cast_not_support_enum_set, /*enum*/
        cast_not_support_enum_set, /*set*/
    },
    {
        /*unknow -> enum_or_set*/
        cast_not_support_enum_set, /*enum*/
        cast_not_support_enum_set, /*set*/
    },
    {
        /*json -> enum_or_set*/
        cast_not_support_enum_set, /*enum*/
        cast_not_support_enum_set, /*set*/
    }
};

int string_collation_check(
    const bool is_strict_mode, const ObCollationType check_cs_type, const ObObjType str_type, ObString& str)
{
  int ret = OB_SUCCESS;
  if (!ob_is_string_type(str_type)) {
    // nothing to do
  } else if (check_cs_type == CS_TYPE_BINARY) {
    // do nothing
  } else {
    int64_t well_formed_len = 0;
    if (OB_FAIL(ObCharset::well_formed_len(check_cs_type, str.ptr(), str.length(), well_formed_len))) {
      LOG_WARN("invalid string for charset", K(ret), K(is_strict_mode), K(check_cs_type), K(str), K(well_formed_len));
      if (is_strict_mode) {
        ret = OB_ERR_INCORRECT_STRING_VALUE;
      } else {
        ret = OB_SUCCESS;
        str.assign_ptr(str.ptr(), static_cast<ObString::obstr_size_t>(well_formed_len));
      }
    } else {
      // if check succeed, do nothing
    }
  }

  return ret;
}

int ob_datum_to_ob_time_with_date(const ObDatum& datum, const ObObjType type, const ObTimeZoneInfo* tz_info,
    ObTime& ob_time, const int64_t cur_ts_value, bool is_dayofmonth /*false*/)
{
  int ret = OB_SUCCESS;
  switch (ob_obj_type_class(type)) {
    case ObIntTC:
      // fallthrough.
    case ObUIntTC: {
      ret = ObTimeConverter::int_to_ob_time_with_date(datum.get_int(), ob_time, is_dayofmonth);
      break;
    }
    case ObOTimestampTC: {
      if (ObTimestampTZType == type) {
        ret = ObTimeConverter::otimestamp_to_ob_time(type, datum.get_otimestamp_tz(), tz_info, ob_time);
      } else {
        ret = ObTimeConverter::otimestamp_to_ob_time(type, datum.get_otimestamp_tiny(), tz_info, ob_time);
      }
      break;
    }
    case ObDateTimeTC: {
      ret = ObTimeConverter::datetime_to_ob_time(
          datum.get_datetime(), (ObTimestampType == type) ? tz_info : NULL, ob_time);
      break;
    }
    case ObDateTC: {
      ret = ObTimeConverter::date_to_ob_time(datum.get_date(), ob_time);
      break;
    }
    case ObTimeTC: {
      int64_t dt_value = 0;
      if (OB_FAIL(ObTimeConverter::timestamp_to_datetime(cur_ts_value, tz_info, dt_value))) {
        LOG_WARN("convert timestamp to datetime failed", K(ret));
      } else {
        const int64_t usec_per_day = 3600 * 24 * USECS_PER_SEC;
        // Intercept the datetime time, only keep the date, and then convert it to microseconds
        int64_t day_usecs = dt_value - dt_value % usec_per_day;
        ret = ObTimeConverter::datetime_to_ob_time(datum.get_time() + day_usecs, NULL, ob_time);
      }
      break;
    }
    case ObTextTC:  // TODO texttc share with the stringtc temporarily
    case ObStringTC: {
      ObScale res_scale = -1;
      ret = ObTimeConverter::str_to_ob_time_with_date(datum.get_string(), ob_time, &res_scale, is_dayofmonth);
      break;
    }
    case ObNumberTC: {
      int64_t int_part = 0;
      int64_t dec_part = 0;
      const number::ObNumber num(datum.get_number());
      if (num.is_negative()) {
        ret = OB_INVALID_DATE_FORMAT;
        LOG_WARN("invalid date format", K(ret), K(num));
      } else if (!num.is_int_parts_valid_int64(int_part, dec_part)) {
        ret = OB_INVALID_DATE_FORMAT;
        LOG_WARN("invalid date format", K(ret), K(num));
      } else {
        ret = ObTimeConverter::int_to_ob_time_with_date(int_part, ob_time, is_dayofmonth);
        if (OB_SUCC(ret)) {
          ob_time.parts_[DT_USEC] = (dec_part + 500) / 1000;
        }
      }
      break;
    }
    default: {
      ret = OB_NOT_SUPPORTED;
    }
  }
  LOG_DEBUG("end ob_datum_to_ob_time_with_date", K(type), K(cur_ts_value), K(ob_time), K(ret));
  return ret;
}

int ob_datum_to_ob_time_without_date(
    const ObDatum& datum, const ObObjType type, const ObTimeZoneInfo* tz_info, ObTime& ob_time)
{
  int ret = OB_SUCCESS;
  switch (ob_obj_type_class(type)) {
    case ObIntTC:
      // fallthrough.
    case ObUIntTC: {
      if (OB_FAIL(ObTimeConverter::int_to_ob_time_without_date(datum.get_int(), ob_time))) {
        LOG_WARN("int to ob time without date failed", K(ret));
      } else {
        // When converting intTC to time in mysql, if hour exceeds 838, then time should be null instead of maximum
        const int64_t time_max_val = 3020399 * 1000000LL;  // 838:59:59 .
        int64_t value = ObTimeConverter::ob_time_to_time(ob_time);
        if (value > time_max_val) {
          ret = OB_INVALID_DATE_VALUE;
        }
      }
      break;
    }
    case ObOTimestampTC: {
      if (ObTimestampTZType == type) {
        ret = ObTimeConverter::otimestamp_to_ob_time(type, datum.get_otimestamp_tz(), tz_info, ob_time);
      } else {
        ret = ObTimeConverter::otimestamp_to_ob_time(type, datum.get_otimestamp_tiny(), tz_info, ob_time);
      }
      break;
    }
    case ObDateTimeTC: {
      ret = ObTimeConverter::datetime_to_ob_time(
          datum.get_datetime(), (ObTimestampType == type) ? tz_info : NULL, ob_time);
      break;
    }
    case ObDateTC: {
      ret = ObTimeConverter::date_to_ob_time(datum.get_date(), ob_time);
      break;
    }
    case ObTimeTC: {
      ret = ObTimeConverter::time_to_ob_time(datum.get_time(), ob_time);
      break;
    }
    case ObTextTC:  // TODO texttc share with the stringtc temporarily
    case ObStringTC: {
      ret = ObTimeConverter::str_to_ob_time_without_date(datum.get_string(), ob_time);
      if (OB_SUCC(ret)) {
        int64_t value = ObTimeConverter::ob_time_to_time(ob_time);
        int64_t tmp_value = value;
        ObTimeConverter::time_overflow_trunc(value);
        if (value != tmp_value) {
          ObTimeConverter::time_to_ob_time(value, ob_time);
        }
      }
      break;
    }
    case ObNumberTC: {
      number::ObNumber num(datum.get_number());
      const char *num_format = num.format();
      if (OB_ISNULL(num_format)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("number format value is null", K(ret));
      } else {
        ObString num_str(num_format);
        if (OB_FAIL(ObTimeConverter::str_to_ob_time_without_date(num_str, ob_time))) {
          LOG_WARN("str to obtime without date failed", K(ret));
        } else {
          int64_t value = ObTimeConverter::ob_time_to_time(ob_time);
          int64_t tmp_value = value;
          ObTimeConverter::time_overflow_trunc(value);
          if (value != tmp_value) {
            ObTimeConverter::time_to_ob_time(value, ob_time);
          }
        }
      }
      break;
    }
    default: {
      ret = OB_NOT_SUPPORTED;
    }
  }
  LOG_DEBUG("end ob_datum_to_ob_time_without_date", K(type), K(ob_time), K(ret));
  return ret;
}

// Cases that cannot be cast include:
// 1. in oracle mode, string/text/lob->string/text/lob, blob does not support turning to nonblob
// 2. in oracle mode, string/text/lob->string/text/lob, nonblob when turning to blob, the input must be char/varchar/raw
int ObDatumCast::check_can_cast(const ObObjType in_type, const ObCollationType in_cs_type, const ObObjType out_type,
    const ObCollationType out_cs_type)
{
  int ret = OB_SUCCESS;
  ObObjTypeClass in_tc = ob_obj_type_class(in_type);
  ObObjTypeClass out_tc = ob_obj_type_class(out_type);
  const bool is_stringtext_tc_to_stringtext_tc = ((ObStringTC == in_tc || ObTextTC == in_tc || ObLobTC == in_tc) &&
                                                  (ObStringTC == out_tc || ObTextTC == out_tc || ObLobTC == out_tc));
  const bool is_blob_in = ob_is_blob(in_type, in_cs_type) || ob_is_blob_locator(in_type, in_cs_type);
  const bool is_blob_out = ob_is_blob(out_type, out_cs_type) || ob_is_blob_locator(out_type, out_cs_type);

  const bool is_blob_to_nonblob = is_blob_in && (!is_blob_out);
  const bool is_nonblob_to_blob = (!is_blob_in) && is_blob_out;
  const bool is_stringtext_tc_to_nonstringtext_tc =
      ((ObStringTC == in_tc || ObTextTC == in_tc || ObLobTC == in_tc) &&
          !(ObStringTC == out_tc || ObTextTC == out_tc || ObLobTC == out_tc));

  if (ObNullType == in_type || ObNullType == out_type) {
    // let null be ok
  } else if (!lib::is_oracle_mode()) {
  } else if ((ob_is_number_tc(in_type) || ob_is_clob(in_type, in_cs_type) || ob_is_clob_locator(in_type, in_cs_type)) &&
             is_blob_out) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("cast number to blob not allowed", K(ret));
  } else if (is_stringtext_tc_to_stringtext_tc && is_blob_to_nonblob) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("invalid use of blob type", K(ret), K(out_type));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "cast to blob type");
  } else if (is_stringtext_tc_to_stringtext_tc && is_nonblob_to_blob && !ob_is_raw(in_type) &&
             !ob_is_varchar_char_type(in_type, in_cs_type)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("invalid use of blob type", K(ret), K(out_type), K(out_cs_type), K(in_type), K(in_cs_type));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "cast to blob type");
    // } else if (ObTextTC == in_tc && is_lob_outrow) {
    //   ret = OB_NOT_SUPPORTED;
    //   LOG_WARN("cannot cast blob to nonblob", K(ret));
  } else if (!is_stringtext_tc_to_stringtext_tc && is_blob_in) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("cannot cast blob to nonblob", K(ret));
  }
  return ret;
}

// string/text -> string/text description of the special case of string/text:
// 1. !blob -> blob is ok. (in_type must be varchar/char/raw, varchar/char call hextoraw to cast)
// 2. !blob -> !blob is ok (just copy or convert charset)
//     a. if it is the same character set, call cast_eval_arg
//     b. if it is a different character set, and the input and output are not cs_type_binary, character set conversion
//     is required c. if any of the input and output is cs_type_binary, cast_eval_arg will be called
// 3. blob -> blob ok. call cast_eval_arg directly
// 4. blob -> !blob not ok.choose_cast_func will detect and report errors
int ObDatumCast::is_trivial_cast(const ObObjType in_type, const ObCollationType in_cs_type, const ObObjType out_type,
    const ObCollationType out_cs_type, const ObCastMode& cast_mode, bool& is_trivial_cast)
{
  is_trivial_cast = false;
  int ret = OB_SUCCESS;

  ObObjTypeClass in_tc = ob_obj_type_class(in_type);
  ObObjTypeClass out_tc = ob_obj_type_class(out_type);
  const bool is_same_charset =
      (ob_is_string_type(in_type) && ob_is_string_type(out_type) &&
          ObCharset::charset_type_by_coll(in_cs_type) == ObCharset::charset_type_by_coll(out_cs_type));
  const bool is_clob_to_nonclob = (ob_is_clob(in_type, in_cs_type) && !ob_is_clob(out_type, out_cs_type));
  const bool is_nonblob_to_blob =
      ((false == ob_is_blob(in_type, in_cs_type)) && (true == ob_is_blob(out_type, out_cs_type)));
  const bool is_blob_to_blob =
      ((true == ob_is_blob(in_type, in_cs_type)) && (true == ob_is_blob(out_type, out_cs_type)));
  const bool is_nonblob_to_nonblob =
      ((false == ob_is_blob(in_type, in_cs_type)) && (false == ob_is_blob(out_type, out_cs_type)));
  const bool is_stringtext_tc_to_stringtext_tc =
      ((ObStringTC == in_tc || ObTextTC == in_tc) && (ObStringTC == out_tc || ObTextTC == out_tc));

  if (ObNullType == in_type) {
    // cast func of xxx(not_null)-> null is cast_not_exprct() or cast_not_support()
    // cast func of null -> xxx is cast_eval_arg
    is_trivial_cast = true;
  } else if (ob_is_raw(in_type) && ob_is_blob(out_type, out_cs_type)) {
    is_trivial_cast = true;
  } else if (is_stringtext_tc_to_stringtext_tc && lib::is_oracle_mode()) {
    if ((is_same_charset && !is_nonblob_to_blob) || (is_blob_to_blob) ||
        (is_nonblob_to_nonblob && (CS_TYPE_BINARY == in_cs_type || CS_TYPE_BINARY == out_cs_type))) {
      if (!is_clob_to_nonclob) {
        is_trivial_cast = true;
      }
    }
  } else if (is_stringtext_tc_to_stringtext_tc && !lib::is_oracle_mode()) {
    if ((is_same_charset || CS_TYPE_BINARY == out_cs_type)) {
      is_trivial_cast = true;
    }
  } else if (!lib::is_oracle_mode() &&
             ((ObIntTC == in_tc && ObBitTC == out_tc) || (ObUIntTC == in_tc && ObBitTC == out_tc) ||
                 (ObDateType == in_type && ObDateType == out_type) ||
                 (ObYearType == in_type && ObYearType == out_type) ||
                 (ObExtendType == in_type && ObExtendType == out_type) ||
                 (ObBitType == in_type && ObBitType == out_type) ||
                 (ObUnknownType == in_type && ObUnknownType == out_type))) {
    is_trivial_cast = true;
  } else if (lib::is_oracle_mode() && ((ObExtendType == in_type && ObExtendType == out_type) ||
                                          (ObUnknownType == in_type && ObUnknownType == out_type))) {
    // Oracle mode does not have bit/year/date/time type (Oracle's date type is represented by ObDateTimeType in OB)
    is_trivial_cast = true;
  } else if (ObUIntTC == in_tc && ObIntTC == out_tc && CM_IS_EXTERNAL_CALL(cast_mode) &&
             CM_SKIP_CAST_INT_UINT(cast_mode)) {
    is_trivial_cast = true;
  } else {
    is_trivial_cast = false;
  }
  LOG_DEBUG("is_trivial_cast debug",
      K(ret),
      K(in_type),
      K(out_type),
      K(in_cs_type),
      K(out_cs_type),
      K(cast_mode),
      K(is_trivial_cast),
      K(lbt()));
  return ret;
}

int ObDatumCast::get_implicit_cast_function(const ObObjType in_type, const ObCollationType in_cs_type,
    const ObObjType out_type, const ObCollationType out_cs_type, const int64_t cast_mode, ObExpr::EvalFunc& eval_func)

{
  int ret = OB_SUCCESS;
  bool pass_cast = false;
  if (OB_FAIL(check_can_cast(in_type, in_cs_type, out_type, out_cs_type))) {
    LOG_WARN("check_can_cast failed", K(ret));
  } else if (OB_FAIL(is_trivial_cast(in_type, in_cs_type, out_type, out_cs_type, cast_mode, pass_cast))) {
    LOG_WARN("is_trivial_cast failed", K(ret), K(in_type), K(out_type));
  } else if (pass_cast) {
    eval_func = cast_eval_arg;
  } else {
    ObObjTypeClass in_tc = ob_obj_type_class(in_type);
    ObObjTypeClass out_tc = ob_obj_type_class(out_type);
    if (lib::is_oracle_mode()) {
      eval_func = OB_DATUM_CAST_ORACLE_IMPLICIT[in_tc][out_tc];
    } else {
      eval_func = OB_DATUM_CAST_MYSQL_IMPLICIT[in_tc][out_tc];
    }
    LOG_DEBUG("get_implicit_cast_function ", K(in_tc), K(out_tc), K(in_type), K(out_type));
  }

  return ret;
}

int ObDatumCast::choose_cast_function(const ObObjType in_type, const ObCollationType in_cs_type,
    const ObObjType out_type, const ObCollationType out_cs_type, const int64_t cast_mode, ObIAllocator& allocator,
    ObExpr& rt_expr)
{
  int ret = OB_SUCCESS;
  bool just_eval_arg = false;

  if (OB_FAIL(check_can_cast(in_type, in_cs_type, out_type, out_cs_type))) {
    LOG_WARN("check_can_cast failed", K(ret));
  } else if (OB_FAIL(is_trivial_cast(in_type, in_cs_type, out_type, out_cs_type, cast_mode, just_eval_arg))) {
    LOG_WARN("is_trivial_cast failed", K(ret), K(in_type), K(out_type));
  } else if (just_eval_arg && !CM_IS_EXPLICIT_CAST(cast_mode)) {
    // Even if they are of the same type, an accuracy check is required for explicit cast
    rt_expr.eval_func_ = cast_eval_arg;
  } else {
    ObObjTypeClass in_tc = ob_obj_type_class(in_type);
    ObObjTypeClass out_tc = ob_obj_type_class(out_type);
    if (CM_IS_EXPLICIT_CAST(cast_mode)) {
      if (OB_ISNULL(rt_expr.inner_functions_ = reinterpret_cast<void**>(allocator.alloc(sizeof(void*))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc memory failed", K(ret));
      } else {
        rt_expr.inner_func_cnt_ = 1;
        if (just_eval_arg) {
          rt_expr.inner_functions_[0] = reinterpret_cast<void*>(cast_eval_arg);
        } else {
          if (lib::is_oracle_mode()) {
            rt_expr.inner_functions_[0] = reinterpret_cast<void*>(OB_DATUM_CAST_ORACLE_IMPLICIT[in_tc][out_tc]);
          } else {
            rt_expr.inner_functions_[0] = reinterpret_cast<void*>(OB_DATUM_CAST_MYSQL_IMPLICIT[in_tc][out_tc]);
          }
        }
        if (ob_is_character_type(out_type, out_cs_type) || ob_is_varbinary_or_binary(out_type, out_cs_type)) {
          rt_expr.eval_func_ = anytype_to_varchar_char_explicit;
        } else {
          rt_expr.eval_func_ = anytype_anytype_explicit;
        }
      }
    } else if (lib::is_oracle_mode()) {
      rt_expr.eval_func_ = OB_DATUM_CAST_ORACLE_IMPLICIT[in_tc][out_tc];
    } else {
      rt_expr.eval_func_ = OB_DATUM_CAST_MYSQL_IMPLICIT[in_tc][out_tc];
    }
  }
  LOG_DEBUG("in choose_cast_function",
      K(ret),
      K(in_type),
      K(out_type),
      K(in_cs_type),
      K(out_cs_type),
      K(CM_IS_EXPLICIT_CAST(cast_mode)),
      K(CM_IS_ZERO_FILL(cast_mode)),
      K(cast_mode),
      K(lbt()));
  return ret;
}

int ObDatumCast::get_enumset_cast_function(
    const common::ObObjTypeClass in_tc, const common::ObObjType out_type, ObExpr::EvalEnumSetFunc& eval_func)
{
  int ret = OB_SUCCESS;
  // in_type can be NullType, out_type cannot be NullType
  if (OB_UNLIKELY(!(ObNullTC <= in_tc && in_tc < ObMaxTC)) ||
      OB_UNLIKELY(out_type != ObEnumType && out_type != ObSetType)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expecected intype or outtype", K(ret), K(in_tc), K(out_type));
  } else {
    eval_func = OB_DATUM_CAST_MYSQL_ENUMSET_IMPLICIT[in_tc][out_type == ObSetType];
  }
  LOG_DEBUG("in get_enumset_cast_function", K(ret), K(in_tc), K(out_type));
  return ret;
}

int ObDatumCast::cast_obj(ObEvalCtx& ctx, ObIAllocator& alloc, const ObObjType& dst_type,
    const ObCollationType& dst_cs_type, const ObObj& src_obj, ObObj& dst_obj)
{
  int ret = OB_SUCCESS;
  ObCastMode def_cm = CM_NONE;
  ObSQLSessionInfo* session = ctx.exec_ctx_.get_my_session();

  CK(OB_NOT_NULL(session));
  if (OB_SUCC(ret)) {
    ObPhysicalPlanCtx* phy_plan_ctx = ctx.exec_ctx_.get_physical_plan_ctx();
    int64_t cur_time = phy_plan_ctx ? phy_plan_ctx->get_cur_time().get_datetime() : 0;
    const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(session);
    ObCastCtx cast_ctx(&alloc, &dtc_params, get_cur_time(phy_plan_ctx), def_cm, dst_cs_type, NULL);
    if (OB_FAIL(ObSQLUtils::get_default_cast_mode(session->get_stmt_type(), session, def_cm))) {
      LOG_WARN("get_default_cast_mode failed", K(ret));
    } else if (OB_FAIL(ObObjCaster::to_type(dst_type, cast_ctx, src_obj, dst_obj))) {
      LOG_WARN("failed to cast object to ", K(ret), K(src_obj), K(dst_type));
    }
  }
  return ret;
}

int ObDatumCaster::init(ObExecContext& ctx)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    void* ctx_mem = NULL;
    void* expr_mem = NULL;
    void* extra_mem = NULL;
    void* expr_args_mem = NULL;
    void* extra_args_mem = NULL;
    void* frame_mem = NULL;
    void* frames_mem = NULL;
    ObIAllocator& alloc = ctx.get_allocator();
    int64_t res_buf_len = ObDatum::get_reserved_size(ObObjDatumMapType::OBJ_DATUM_STRING);
    const int64_t datum_eval_info_size = sizeof(ObDatum) + sizeof(ObEvalInfo);
    int64_t frame_size = (datum_eval_info_size + sizeof(ObDynReserveBuf) + res_buf_len) * 2;
    int64_t frames_size = 0;

    CK(OB_NOT_NULL(ctx.get_eval_ctx()));
    CK(OB_NOT_NULL(ctx.get_frames()));
    CK(OB_NOT_NULL(ctx.get_eval_res_mem()));
    CK(OB_NOT_NULL(ctx.get_eval_tmp_mem()));

    OX(frames_size = sizeof(char*) * (ctx.get_frame_cnt() + 1));
    OV(OB_NOT_NULL(ctx_mem = alloc.alloc(sizeof(ObEvalCtx))), OB_ALLOCATE_MEMORY_FAILED);
    OV(OB_NOT_NULL(frames_mem = alloc.alloc(frames_size)), OB_ALLOCATE_MEMORY_FAILED);
    OV(OB_NOT_NULL(frame_mem = alloc.alloc(frame_size)), OB_ALLOCATE_MEMORY_FAILED);
    OV(OB_NOT_NULL(expr_mem = alloc.alloc(sizeof(ObExpr))), OB_ALLOCATE_MEMORY_FAILED);
    OV(OB_NOT_NULL(extra_mem = alloc.alloc(sizeof(ObExpr))), OB_ALLOCATE_MEMORY_FAILED);
    OV(OB_NOT_NULL(expr_args_mem = alloc.alloc(sizeof(ObExpr*))), OB_ALLOCATE_MEMORY_FAILED);
    OV(OB_NOT_NULL(extra_args_mem = alloc.alloc(sizeof(ObExpr*))), OB_ALLOCATE_MEMORY_FAILED);
    OX(MEMCPY(frames_mem, ctx.get_frames(), sizeof(char*) * ctx.get_frame_cnt()));

    if (OB_FAIL(ret)) {
    } else {
      // init eval_ctx_
      char* frame = reinterpret_cast<char*>(frame_mem);
      MEMSET(frame, 0, frame_size);
      eval_ctx_ = new (ctx_mem)
          ObEvalCtx(ctx, ctx.get_eval_res_mem()->get_arena_allocator(), ctx.get_eval_tmp_mem()->get_arena_allocator());
      eval_ctx_->frames_ = reinterpret_cast<char**>(frames_mem);
      eval_ctx_->frames_[ctx.get_frame_cnt()] = frame;

      // init cast_expr_/extra_cast_expr and frame
      cast_expr_ = reinterpret_cast<ObExpr*>(expr_mem);
      cast_expr_->args_ = reinterpret_cast<ObExpr**>(expr_args_mem);
      extra_cast_expr_ = reinterpret_cast<ObExpr*>(extra_mem);
      extra_cast_expr_->args_ = reinterpret_cast<ObExpr**>(extra_args_mem);
      ObExpr* exprs[2] = {cast_expr_, extra_cast_expr_};

      int64_t data_off = datum_eval_info_size * 2;
      const int64_t consume_size = res_buf_len + sizeof(ObDynReserveBuf);
      for (int64_t i = 0; OB_SUCC(ret) && i < sizeof(exprs) / sizeof(ObExpr*); ++i) {
        ObExpr* e = exprs[i];
        e->type_ = T_FUN_SYS_CAST;
        e->max_length_ = -1;
        e->inner_func_cnt_ = 0;
        e->inner_functions_ = NULL;
        e->frame_idx_ = ctx.get_frame_cnt();
        e->datum_off_ = datum_eval_info_size * i;
        e->eval_info_off_ = e->datum_off_ + sizeof(ObDatum);
        e->res_buf_len_ = res_buf_len;
        data_off += consume_size;
        e->res_buf_off_ = data_off - e->res_buf_len_;
        e->arg_cnt_ = 1;

        ObDatum* expr_datum = reinterpret_cast<ObDatum*>(frame + e->datum_off_);
        expr_datum->ptr_ = frame + e->res_buf_off_;
        ObDynReserveBuf* drb = reinterpret_cast<ObDynReserveBuf*>(frame + e->res_buf_off_ - sizeof(ObDynReserveBuf));
        drb->len_ = e->res_buf_len_;
        drb->mem_ = frame + e->res_buf_off_;
      }
    }
    OX(inited_ = true);
  }
  return ret;
}

int ObDatumCaster::to_type(const ObDatumMeta& dst_type, const ObExpr& src_expr, const ObCastMode& cm, ObDatum*& res)
{
  int ret = OB_SUCCESS;
  bool need_cast = false;
  const ObDatumMeta& src_type = src_expr.datum_meta_;
  // No conversion is required when is_both_string and collation are the same
  // There must be conversion between string_type and lob, both are string_type or both are lob, and
  // do not need to be converted when the collation is the same
  const bool is_both_string = (ob_is_string_type(src_type.type_) && ob_is_string_type(dst_type.type_)) ||
                              (ob_is_lob_locator(src_type.type_) && ob_is_lob_locator(dst_type.type_));
  const ObCharsetType& src_cs = ObCharset::charset_type_by_coll(src_type.cs_type_);
  const ObCharsetType& dst_cs = ObCharset::charset_type_by_coll(dst_type.cs_type_);

  if (OB_UNLIKELY(!inited_) || OB_ISNULL(eval_ctx_) || OB_ISNULL(cast_expr_) || OB_ISNULL(extra_cast_expr_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDatumCaster is invalid", K(ret), K(inited_), KP(eval_ctx_), KP(cast_expr_), KP(extra_cast_expr_));
  } else if ((!is_both_string && src_type.type_ == dst_type.type_) || (is_both_string && src_cs == dst_cs)) {
    LOG_DEBUG("no need to cast, just eval src_expr", K(ret), K(src_expr), K(dst_type));
    if (OB_FAIL(src_expr.eval(*eval_ctx_, res))) {
      LOG_WARN("eval src_expr failed", K(ret));
    }
  } else {
    bool nonstr_to_str = !ob_is_string_or_lob_type(src_type.type_) && ob_is_string_or_lob_type(dst_type.type_);
    bool str_to_nonstr = ob_is_string_or_lob_type(src_type.type_) && !ob_is_string_or_lob_type(dst_type.type_);
    bool need_extra_cast_for_src_type = false;
    bool need_extra_cast_for_dst_type = false;

    if (str_to_nonstr) {
      if (CHARSET_BINARY != src_cs && ObCharset::get_default_charset() != src_cs) {
        need_extra_cast_for_src_type = true;
      }
    } else if (nonstr_to_str) {
      if (CHARSET_BINARY != dst_cs && ObCharset::get_default_charset() != dst_cs) {
        need_extra_cast_for_dst_type = true;
      }
    }

    ObDatumMeta extra_dst_type = src_type;
    if (need_extra_cast_for_src_type) {
      // non-utf8 -> int/num...
      extra_dst_type = src_type;
      extra_dst_type.cs_type_ = ObCharset::get_system_collation();
    } else if (need_extra_cast_for_dst_type) {
      // int/num... -> non-utf8
      extra_dst_type = dst_type;
      extra_dst_type.cs_type_ = ObCharset::get_system_collation();
    }

    if (need_extra_cast_for_src_type || need_extra_cast_for_dst_type) {
      if (OB_FAIL(setup_cast_expr(extra_dst_type, src_expr, cm, *extra_cast_expr_))) {
        LOG_WARN("setup_cast_expr failed", K(ret));
      } else if (OB_FAIL(setup_cast_expr(dst_type, *extra_cast_expr_, cm, *cast_expr_))) {
        LOG_WARN("setup_cast_expr failed", K(ret));
      }
    } else {
      if (OB_FAIL(setup_cast_expr(dst_type, src_expr, cm, *cast_expr_))) {
        LOG_WARN("setup_cast_expr failed", K(ret));
      }
    }

    if (OB_SUCC(ret) && OB_FAIL(cast_expr_->eval(*eval_ctx_, res))) {
      LOG_WARN("eval cast expr failed", K(ret));
    }
    LOG_DEBUG("ObDatumCaster::to_type done",
        K(ret),
        K(src_expr),
        K(dst_type),
        K(cm),
        K(need_extra_cast_for_src_type),
        K(need_extra_cast_for_dst_type),
        KP(eval_ctx_->frames_));
  }
  return ret;
}

int ObDatumCaster::to_type(const ObDatumMeta& dst_type, const ObIArray<ObString>& str_values, const ObExpr& src_expr,
    const ObCastMode& cm, ObDatum*& res)
{
  int ret = OB_SUCCESS;
  const ObDatumMeta& src_type = src_expr.datum_meta_;
  if (OB_UNLIKELY(!inited_) || OB_ISNULL(cast_expr_) || OB_ISNULL(extra_cast_expr_) || OB_ISNULL(eval_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDatumCaster is invalid", K(ret), K(inited_), KP(eval_ctx_), KP(cast_expr_), KP(extra_cast_expr_));
  } else if (OB_UNLIKELY(!ob_is_enumset_tc(dst_type.type_)) || OB_UNLIKELY(ob_is_invalid_obj_type(src_type.type_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid src_type or dst_type", K(ret), K(src_type), K(dst_type));
  } else {
    // enum -> enum or set -> set will give error(see OB_DATUM_CAST_MYSQL_ENUMSET_IMPLICIT)
    // we do not check if need cast
    bool need_extra_cast = false;
    const ObCharsetType& src_cs = ObCharset::charset_type_by_coll(src_type.cs_type_);
    if (ob_is_string_type(src_type.type_) && CHARSET_BINARY != src_cs && ObCharset::get_default_charset() != src_cs) {
      need_extra_cast = true;
    }
    if (need_extra_cast) {
      // non-utf8 -> enumset
      ObDatumMeta extra_dst_type = src_type;
      extra_dst_type.cs_type_ = ObCharset::get_system_collation();
      if (OB_FAIL(setup_cast_expr(extra_dst_type, src_expr, cm, *extra_cast_expr_))) {
        LOG_WARN("setup_cast_expr failed", K(ret));
      } else if (OB_FAIL(setup_cast_expr(dst_type, *extra_cast_expr_, cm, *cast_expr_))) {
        LOG_WARN("setup_cast_expr failed", K(ret));
      }
    } else {
      if (OB_FAIL(setup_cast_expr(dst_type, src_expr, cm, *cast_expr_))) {
        LOG_WARN("setup_cast_expr failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(cast_expr_->eval_enumset(*eval_ctx_, str_values, cast_expr_->extra_, res))) {
        LOG_WARN("eval_enumset failed", K(ret));
      }
    }
    LOG_DEBUG("ObDatumCaster::to_type done",
        K(ret),
        K(src_expr),
        K(dst_type),
        K(str_values),
        KP(eval_ctx_->frames_),
        K(cm),
        K(need_extra_cast),
        K(lbt()));
  }
  return ret;
}

int ObDatumCaster::destroy()
{
  int ret = OB_SUCCESS;
  if (inited_) {
    if (OB_ISNULL(cast_expr_) || OB_ISNULL(extra_cast_expr_) || OB_ISNULL(eval_ctx_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("ObDatumCaster is invalid", K(ret), K(inited_), KP(eval_ctx_), KP(cast_expr_), KP(extra_cast_expr_));
    } else {
      inited_ = false;
      ObIAllocator& alloc = eval_ctx_->exec_ctx_.get_allocator();
      eval_ctx_->~ObEvalCtx();
      // ~ObEvalCtx() is default deallocator, so free frames_ manually.
      alloc.free(eval_ctx_->frames_);
      alloc.free(eval_ctx_);
      alloc.free(cast_expr_);
      alloc.free(extra_cast_expr_);
      eval_ctx_ = NULL;
      cast_expr_ = NULL;
      extra_cast_expr_ = NULL;
    }
  }
  return ret;
}

int ObDatumCaster::setup_cast_expr(
    const ObDatumMeta& dst_type, const ObExpr& src_expr, const ObCastMode cm, ObExpr& cast_expr)
{
  int ret = OB_SUCCESS;
  const ObDatumMeta& src_type = src_expr.datum_meta_;
  if (!inited_ || OB_ISNULL(cast_expr.args_) || OB_ISNULL(eval_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("invalid cast expr", K(ret), K(inited_), KP(cast_expr.args_));
  } else if (OB_FAIL(ObDatumCast::get_implicit_cast_function(
                 src_type.type_, src_type.cs_type_, dst_type.type_, dst_type.cs_type_, cm, cast_expr.eval_func_))) {
    LOG_WARN("get_implicit_cast_function failed", K(ret));
  } else if (OB_ISNULL(cast_expr.eval_func_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid eval func", K(ret), KP(cast_expr.eval_func_));
  } else {
    cast_expr.datum_meta_ = dst_type;
    cast_expr.obj_datum_map_ = ObDatum::get_obj_datum_map_type(dst_type.type_);
    cast_expr.args_[0] = const_cast<ObExpr*>(&src_expr);
    cast_expr.extra_ = cm;
    cast_expr.obj_meta_.set_type(dst_type.type_);
    cast_expr.obj_meta_.set_collation_type(dst_type.cs_type_);
    cast_expr.obj_meta_.set_collation_level(CS_LEVEL_INVALID);
    cast_expr.obj_meta_.set_scale(-1);
    // implicit cast donot use these, so we set it all invalid.
    cast_expr.parents_ = NULL;
    cast_expr.parent_cnt_ = 0;
    cast_expr.basic_funcs_ = NULL;
    cast_expr.get_eval_info(*eval_ctx_).clear_evaluated_flag();
  }
  return ret;
}

// register function serialization

// function array is two dimension array, need to convert to index stable array first.

// ObExpr::EvalFunc OB_DATUM_CAST_ORACLE_IMPLICIT[ObMaxTC][ObMaxTC] =
// ObExpr::EvalFunc OB_DATUM_CAST_ORACLE_EXPLICIT[ObMaxTC][ObMaxTC] =
// ObExpr::EvalFunc OB_DATUM_CAST_MYSQL_IMPLICIT[ObMaxTC][ObMaxTC] =
// ObExpr::EvalEnumSetFunc OB_DATUM_CAST_MYSQL_ENUMSET_IMPLICIT[ObMaxTC][2] =

static_assert(ObMaxTC * ObMaxTC == sizeof(OB_DATUM_CAST_ORACLE_IMPLICIT) / sizeof(void*), "unexpected size");
static void* g_ser_datum_cast_oracle_implicit[ObMaxTC * ObMaxTC];
bool g_ser_datum_cast_oracle_implicit_init = ObFuncSerialization::convert_NxN_array(
    g_ser_datum_cast_oracle_implicit, reinterpret_cast<void**>(OB_DATUM_CAST_ORACLE_IMPLICIT), ObMaxTC);
REG_SER_FUNC_ARRAY(
    OB_SFA_DATUM_CAST_ORACLE_IMPLICIT, g_ser_datum_cast_oracle_implicit, ARRAYSIZEOF(g_ser_datum_cast_oracle_implicit));

static_assert(ObMaxTC * ObMaxTC == sizeof(OB_DATUM_CAST_ORACLE_EXPLICIT) / sizeof(void*), "unexpected size");
static void* g_ser_datum_cast_oracle_explicit[ObMaxTC * ObMaxTC];
bool g_ser_datum_cast_oracle_explcit_init = ObFuncSerialization::convert_NxN_array(
    g_ser_datum_cast_oracle_explicit, reinterpret_cast<void**>(OB_DATUM_CAST_ORACLE_EXPLICIT), ObMaxTC);
REG_SER_FUNC_ARRAY(
    OB_SFA_DATUM_CAST_ORACLE_EXPLICIT, g_ser_datum_cast_oracle_explicit, ARRAYSIZEOF(g_ser_datum_cast_oracle_explicit));

static_assert(ObMaxTC * ObMaxTC == sizeof(OB_DATUM_CAST_MYSQL_IMPLICIT) / sizeof(void*), "unexpected size");
static void* g_ser_datum_cast_mysql_implicit[ObMaxTC * ObMaxTC];
bool g_ser_datum_cast_mysql_implicit_init = ObFuncSerialization::convert_NxN_array(
    g_ser_datum_cast_mysql_implicit, reinterpret_cast<void**>(OB_DATUM_CAST_MYSQL_IMPLICIT), ObMaxTC);
REG_SER_FUNC_ARRAY(
    OB_SFA_DATUM_CAST_MYSQL_IMPLICIT, g_ser_datum_cast_mysql_implicit, ARRAYSIZEOF(g_ser_datum_cast_mysql_implicit));

static_assert(ObMaxTC * 2 == sizeof(OB_DATUM_CAST_MYSQL_ENUMSET_IMPLICIT) / sizeof(void*), "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_DATUM_CAST_MYSQL_ENUMSET_IMPLICIT, OB_DATUM_CAST_MYSQL_ENUMSET_IMPLICIT,
    sizeof(OB_DATUM_CAST_MYSQL_ENUMSET_IMPLICIT) / sizeof(void*));
}  // namespace sql
}  // namespace oceanbase
