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

#define USING_LOG_PREFIX COMMON

#include "share/object/ob_obj_cast.h"
#include <math.h>
#include <float.h>
#include "lib/charset/ob_dtoa.h"
#include "lib/string/ob_sql_string.h"
#include "lib/utility/ob_fast_convert.h"
#include "rpc/obmysql/ob_mysql_util.h"
#include "lib/worker.h"
#include "share/object/ob_obj_cast_util.h"
#include "share/rc/ob_tenant_base.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "observer/omt/ob_tenant_srs.h"
#include "lib/json_type/ob_json_tree.h"
#include "lib/json_type/ob_json_bin.h"
#include "lib/json_type/ob_json_base.h"
#include "lib/json_type/ob_json_parse.h"
#include "share/ob_lob_access_utils.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "lib/charset/ob_charset.h"
#include "lib/geo/ob_geometry_cast.h"
#include "sql/engine/expr/ob_datum_cast.h"

#ifdef OB_BUILD_ORACLE_XML
#include "lib/xml/ob_xml_util.h"
#include "lib/xml/ob_xml_parser.h"
#endif

// from sql_parser_base.h
#define DEFAULT_STR_LENGTH -1
namespace oceanbase
{
using namespace lib;
namespace common
{
using namespace number;

static const int64_t MAX_DOUBLE_STRICT_PRINT_SIZE = 512;

static int cast_identity(const ObObjType expect_type,
                         ObObjCastParams &params,
                         const ObObj &in,
                         ObObj &out,
                         const ObCastMode cast_mode)
{
  UNUSED(expect_type);
  UNUSED(params);
  UNUSED(cast_mode);
  if (&in != &out) {
    out = in;
  }
  return OB_SUCCESS;
}

static int cast_not_support(const ObObjType expect_type,
                            ObObjCastParams &params,
                            const ObObj &in,
                            ObObj &out,
                            const ObCastMode cast_mode)
{
  UNUSED(params);
  LOG_WARN_RET(OB_NOT_SUPPORTED, "not supported obj type convert",
            K(expect_type), K(in), K(out), K(cast_mode));
  return OB_NOT_SUPPORTED;
}

static int cast_not_expected(const ObObjType expect_type,
                             ObObjCastParams &params,
                             const ObObj &in,
                             ObObj &out,
                             const ObCastMode cast_mode)
{
  UNUSED(params);
  LOG_WARN_RET(OB_ERR_UNEXPECTED, "not expected obj type convert",
            K(expect_type), K(in), K(out), K(cast_mode));
  return lib::is_oracle_mode() ? OB_ERR_INVALID_DATATYPE : OB_ERR_UNEXPECTED;
}

static int cast_inconsistent_types(const ObObjType expect_type,
                                   ObObjCastParams &params,
                                   const ObObj &in,
                                   ObObj &out,
                                   const ObCastMode cast_mode)
{
  UNUSED(params);
  UNUSED(out);
  UNUSED(cast_mode);
  LOG_WARN_RET(OB_ERR_INVALID_TYPE_FOR_OP, "inconsistent datatypes", "expected", expect_type, "got", in.get_type());
  return OB_ERR_INVALID_TYPE_FOR_OP;
}

static int cast_extend_types_not_support(const ObObjType expect_type,
                                         ObObjCastParams &params,
                                         const ObObj &in,
                                         ObObj &out,
                                         const ObCastMode cast_mode)
{
  UNUSED(params);
  UNUSED(out);
  UNUSED(cast_mode);
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_ORACLE_XML
  if (in.is_pl_extend()) {
    if (pl::PL_OPAQUE_TYPE == in.get_meta().get_extend_type()) {
      pl::ObPLOpaque *pl_src = reinterpret_cast<pl::ObPLOpaque*>(in.get_ext());
      if (OB_ISNULL(pl_src)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("failed to get pl data type info", K(ret), K(in));
      } else if (pl_src->get_type() == pl::ObPLOpaqueType::PL_XML_TYPE) {
        LOG_WARN("inconsistent datatypes", K(ret), K(expect_type), K(in.get_type()));
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
      } else {
        LOG_WARN("not expected obj type convert", K(ret), K(expect_type), K(in), K(out), K(cast_mode));
        ret = OB_ERR_UNEXPECTED;
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected type to convert format", K(ret), K(expect_type), K(in));
  }
#else
  ret = OB_ERR_UNEXPECTED;
#endif
  return ret;
}

static int cast_inconsistent_types_json(const ObObjType expect_type,
                                            ObObjCastParams &params,
                                            const ObObj &in,
                                            ObObj &out,
                                            const ObCastMode cast_mode)
{
  UNUSED(params);
  UNUSED(out);
  int ret = OB_SUCCESS;
  if (CM_IS_IMPLICIT_CAST(cast_mode)) {
    ret = OB_ERR_INVALID_INPUT;
    LOG_WARN("invalid input in implicit cast", K(ret));
  } else {
    LOG_WARN("inconsistent datatypes", "expected", expect_type, "got", in.get_type());
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
  }
  return ret;
}

static int cast_inconsistent_type_json_explicit(const ObObjType expect_type,
                                            ObObjCastParams &params,
                                            const ObObj &in,
                                            ObObj &out,
                                            const ObCastMode cast_mode)
{
  UNUSED(params);
  UNUSED(out);
  int ret = OB_ERR_INVALID_TYPE_FOR_OP;
  LOG_WARN("inconsistent datatypes", "expected", expect_type, "got", in.get_type());
  return ret;
}

static int cast_not_support_enum_set(const ObExpectType &expect_type,
                                     ObObjCastParams &params,
                                     const ObObj &in,
                                     ObObj &out)
{
  UNUSED(params);
  LOG_WARN_RET(OB_NOT_SUPPORTED, "not supported obj type convert", K(expect_type), K(in), K(out));
  return OB_NOT_SUPPORTED;
}

static int cast_identity_enum_set(const ObExpectType &expect_type,
                                  ObObjCastParams &params,
                                  const ObObj &in,
                                  ObObj &out)
{
  UNUSED(expect_type);
  UNUSED(params);
  if (&in != &out) {
    out = in;
  }
  return OB_SUCCESS;
}

static int cast_not_expected_enum_set(const ObExpectType &expect_type,
                                      ObObjCastParams &params,
                                      const ObObj &in,
                                      ObObj &out)
{
  UNUSED(params);
  LOG_WARN_RET(common::OB_ERR_UNEXPECTED, "not expected obj type convert", K(expect_type), K(in), K(out));
  return OB_ERR_UNEXPECTED;
}

static int unknown_other(const ObObjType expect_type,
                         ObObjCastParams &params,
                         const ObObj &in,
                         ObObj &out,
                         const ObCastMode cast_mode)
{
  return cast_not_support(expect_type, params, in, out, cast_mode);
}

////////////////////////////////////////////////////////////////
// Utility func

// %align_offset is used in mysql mode to align blob to other charset.
static int copy_string(const ObObjCastParams &params,
                       const ObObjType type,
                       const char *str,
                       int64_t len,
                       ObObj &obj,
                       int64_t align_offset = 0)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  if (OB_LIKELY(len > 0 && NULL != str)) {
    if (OB_LIKELY(NULL != params.zf_info_) && params.zf_info_->need_zerofill_) {
      int64_t str_len = std::max(len, static_cast<int64_t>(params.zf_info_->max_length_));
      if (OB_UNLIKELY(NULL == (buf = static_cast<char*>(params.alloc(str_len))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        /* 如果需要进行zerofill，这里添0 */
        int64_t zf = params.zf_info_->max_length_ - len;
        if (zf > 0) {
          MEMSET(buf, '0', zf);
          MEMCPY(buf + zf, str, len);
          len = str_len; // set string length
        } else {
          MEMCPY(buf, str, len);
        }
      }
    } else {
      len += align_offset;
      if (OB_UNLIKELY(NULL == (buf = static_cast<char*>(params.alloc(len))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        MEMSET(buf, 0, align_offset);
        MEMCPY(buf + align_offset, str, len - align_offset);
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (ob_is_text_tc(type)) {
      obj.set_lob_value(type, buf, static_cast<int32_t>(len));
    } else if (ob_is_raw(type)) {
      obj.set_raw(buf, static_cast<int32_t>(len));
    } else {
      obj.set_string(type, buf, static_cast<int32_t>(len));
    }
  }
  return ret;
}

static int copy_string(const ObObjCastParams &params,
                       const ObObjType type,
                       const ObString &str,
                       ObObj &obj,
                       int64_t align_offset = 0)
{
  return copy_string(params, type, str.ptr(), str.length(), obj, align_offset);
}

/*
 * check err when a string cast to int/uint/double/float according to endptr
 *@str input string
 *@endptr result pointer to end of converted string
 *@len length of str
 *@err
 * incorrect -> "a232"->int or "a2323.23"->double
 * truncated -> "23as" -> int or "as2332.a"->double
 * @note
 *  This is called after one has called strntoull10rnd() or strntod function.
 */
int check_convert_str_err(const char *str,
                          const char *endptr,
                          const int32_t len,
                          const int err,
                          const ObCollationType &in_cs_type)
{
  int ret = OB_SUCCESS;
  // 1. only one of str and endptr is null, it is invalid input.
  if ((OB_ISNULL(str) || OB_ISNULL(endptr)) && str != endptr) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null pointer(s)", K(ret), KP(str), KP(endptr));
  } else if (is_oracle_mode() && 0 != err) {
    ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
  } else
  // 2. str == endptr include NULL == NULL.
  if (OB_UNLIKELY(str == endptr) || OB_UNLIKELY(EDOM == err)) {
    ret = OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD; //1366
  } else {
    if (is_mysql_mode() && CS_TYPE_BINARY == in_cs_type) {
    } else {
      endptr += ObCharset::scan_str(endptr, str + len, OB_SEQ_SPACES);
    }
    // 3. so here we are sure that both str and endptr are not NULL.
    if (endptr < str + len) {
      ret = OB_ERR_DATA_TRUNCATED; //1265
      LOG_DEBUG("check_convert_str_err", K(len), K(str - endptr));
    }
  }
  return ret;
}

static int convert_string_collation(const ObString &in,
                                    const ObCollationType in_collation,
                                    ObString &out,
                                    const ObCollationType out_collation,
                                    ObObjCastParams &params)
{
  int ret = OB_SUCCESS;
  if (!ObCharset::is_valid_collation(in_collation)
      || !ObCharset::is_valid_collation(out_collation)
      || ObCharset::charset_type_by_coll(in_collation) == CHARSET_BINARY
      || ObCharset::charset_type_by_coll(out_collation) == CHARSET_BINARY
      || (ObCharset::charset_type_by_coll(in_collation)
          == ObCharset::charset_type_by_coll(out_collation))) {
    out = in;
  } else if (in.empty()) {
    out.reset();
  } else {
    char *buf = NULL;
    int32_t buf_len = in.length() * ObCharset::CharConvertFactorNum;
    uint32_t result_len = 0;
    if (OB_ISNULL(buf = static_cast<char*>(params.alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc memory failed", K(ret));
    } else if (OB_FAIL(ObCharset::charset_convert(in_collation,
                                                  in.ptr(),
                                                  in.length(),
                                                  out_collation,
                                                  buf,
                                                  buf_len,
                                                  result_len))) {
      LOG_WARN("charset convert failed", K(ret));
    } else {
      out.assign_ptr(buf, result_len);
    }
  }
  LOG_DEBUG("convert_string_collation",
            K(in.length()), K(in_collation), K(out.length()), K(out_collation));
  return ret;
}

////////////////////////////////////////////////////////////////

OB_INLINE int get_cast_ret(const ObCastMode cast_mode,
                           int ret,
                           int &warning)
{
  //compatibility for old ob
  if (OB_UNLIKELY(OB_ERR_UNEXPECTED_TZ_TRANSITION == ret)
      || OB_UNLIKELY(OB_ERR_UNKNOWN_TIME_ZONE == ret)) {
    ret = OB_INVALID_DATE_VALUE;
  } else if (OB_SUCCESS != ret &&
  //OB_ERR_UNEXPECTED != ret &&
      CM_IS_WARN_ON_FAIL(cast_mode)) {
    warning = ret;
    ret = OB_SUCCESS;
  }
  return ret;
}

#define CAST_FAIL(stmt) \
  (OB_UNLIKELY((OB_SUCCESS != (ret = get_cast_ret(cast_mode, (stmt), params.warning_)))))

#define CAST_RET(stmt) \
  (ret = get_cast_ret(cast_mode, (stmt), params.warning_))

#define SET_RES_OBJ(res, func_val, obj_type, comma, val, zero_val)        \
  do {                                                                    \
    if (OB_SUCC(ret)) {                                                   \
      if (OB_SUCCESS == params.warning_                                   \
          || OB_ERR_TRUNCATED_WRONG_VALUE == params.warning_              \
          || OB_DATA_OUT_OF_RANGE == params.warning_                      \
          || OB_ERR_DATA_TRUNCATED == params.warning_                     \
          || OB_ERR_DOUBLE_TRUNCATED == params.warning_                   \
          || OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD == params.warning_) { \
        res.set_##func_val(obj_type comma val);                           \
      } else if (CM_IS_ZERO_ON_WARN(cast_mode)) {                         \
        res.set_##func_val(obj_type comma zero_val);                      \
      } else {                                                            \
        res.set_null();                                                   \
      }                                                                   \
    } else {                                                              \
      res.set_##func_val(obj_type comma val);                             \
    }                                                                     \
  } while (0)

#define COMMA ,
#define SET_RES_INT(res)          SET_RES_OBJ(res, int, expect_type, COMMA, value, 0)
#define SET_RES_UINT(res)         SET_RES_OBJ(res, uint, expect_type, COMMA, value, 0)
#define SET_RES_FLOAT(res)        SET_RES_OBJ(res, float, expect_type, COMMA, value, 0.0)
#define SET_RES_DOUBLE(res)       SET_RES_OBJ(res, double, expect_type, COMMA, value, 0.0)
#define SET_RES_NUMBER(res)       SET_RES_OBJ(res, number, expect_type, COMMA, value, (value.set_zero(), value))
#define SET_RES_DATETIME(res)     SET_RES_OBJ(res, datetime, expect_type, COMMA, value, ObTimeConverter::ZERO_DATETIME)
#define SET_RES_DATE(res)         SET_RES_OBJ(res, date, , , value, ObTimeConverter::ZERO_DATE)
#define SET_RES_TIME(res)         SET_RES_OBJ(res, time, , , value, ObTimeConverter::ZERO_TIME)
#define SET_RES_YEAR(res)         SET_RES_OBJ(res, year, , , value, ObTimeConverter::ZERO_YEAR)
#define SET_RES_BIT(res)          SET_RES_OBJ(res, bit, , , value, 0)
#define SET_RES_ENUM(res)         SET_RES_OBJ(res, enum, , , value, 0)
#define SET_RES_SET(res)          SET_RES_OBJ(res, set, , , value, 0)
#define SET_RES_OTIMESTAMP(res)   SET_RES_OBJ(res, otimestamp_value, expect_type, COMMA, value, ObOTimestampData())
#define SET_RES_INTERVAL_YM(res)  SET_RES_OBJ(res, interval_ym, , , value, ObIntervalYMValue())
#define SET_RES_INTERVAL_DS(res)  SET_RES_OBJ(res, interval_ds, , , value, ObIntervalDSValue())
#define SET_RES_UROWID(res)       SET_RES_OBJ(res, urowid, , , value, ObURowIDData());
//这里新增了SET_RES_XXXTYPE对应的宏后需要加入到ObObjCaster::get_zero_value()接口中用来获取对应类型的zero value


#define SET_RES_ACCURACY(res_precision, res_scale, res_length) \
  if (params.res_accuracy_ != NULL && OB_SUCCESS == ret) {\
    params.res_accuracy_->set_scale(res_scale);\
    params.res_accuracy_->set_precision(res_precision);\
    params.res_accuracy_->set_length(static_cast<int32_t>(res_length));\
  }

#define SET_RES_ACCURACY_STRING(type, res_precision, res_length) \
  if (params.res_accuracy_ != NULL && OB_SUCCESS == ret) {\
    params.res_accuracy_->set_precision(res_precision);\
    params.res_accuracy_->set_length(res_length);\
    if (ob_is_text_tc(type)) { \
      params.res_accuracy_->set_scale(DEFAULT_SCALE_FOR_TEXT);\
    } else {\
      params.res_accuracy_->set_scale(DEFAULT_SCALE_FOR_STRING);\
    }\
  }
////////////////////////////////////////////////////////////////
// range check function templates.


////////////////////////////////////////////////////////////////////////////////////////////////////

ObNumber ObNumberConstValue::MYSQL_MIN[ObNumber::MAX_PRECISION + 1][ObNumber::MAX_SCALE + 1] = {};
ObNumber ObNumberConstValue::MYSQL_MAX[ObNumber::MAX_PRECISION + 1][ObNumber::MAX_SCALE + 1] = {};
ObNumber ObNumberConstValue::MYSQL_CHECK_MIN[ObNumber::MAX_PRECISION + 1][ObNumber::MAX_SCALE + 1] = {};
ObNumber ObNumberConstValue::MYSQL_CHECK_MAX[ObNumber::MAX_PRECISION + 1][ObNumber::MAX_SCALE + 1] = {};
ObNumber ObNumberConstValue::ORACLE_CHECK_MIN[OB_MAX_NUMBER_PRECISION + 1][ObNumberConstValue::MAX_ORACLE_SCALE_SIZE + 1] = {};
ObNumber ObNumberConstValue::ORACLE_CHECK_MAX[OB_MAX_NUMBER_PRECISION + 1][ObNumberConstValue::MAX_ORACLE_SCALE_SIZE + 1] = {};

int ObNumberConstValue::init(ObIAllocator &allocator, const lib::ObMemAttr &attr)
{
  int ret = OB_SUCCESS;
  int64_t total_alloc_size = 0;
  const int64_t BUFFER_SIZE = 2 * (ObNumber::MAX_SCALE + ObNumber::MAX_PRECISION);
  // 字符数组的作用是字符保存，关于尾字符'\0' 的设置需要调用者自己设置，下面的逻辑经过梳理，已经确保'\0'的设置。
  // 此处为了性能优化考虑，不再强制进行{0}的操作
  char buf[BUFFER_SIZE];
  buf[BUFFER_SIZE-1] = '\0';
  // prepare string like "99.999".
  int pos = 0;
  buf[pos++] = '-';

  {
    lib::CompatModeGuard tmp_mode(lib::Worker::CompatMode::MYSQL);
    for (int16_t precision = OB_MIN_DECIMAL_PRECISION; OB_SUCC(ret) && precision <= ObNumber::MAX_PRECISION; ++precision) {
      for (int16_t scale = 0; OB_SUCC(ret) && precision >= scale && scale <= ObNumber::MAX_SCALE; ++scale) {
        pos = 1;
        MEMSET(buf + pos, 0, BUFFER_SIZE - pos);
        if (precision == scale) {
          buf[pos++] = '0';
        }
        MEMSET(buf + pos, '9', precision + 1);
        buf[pos + precision + 1] = '5';
        buf[pos + precision - scale] = '.';
        // make min and max numbers.
        ObNumber &min_check_num = MYSQL_CHECK_MIN[precision][scale];
        ObNumber &max_check_num = MYSQL_CHECK_MAX[precision][scale];
        ObString tmp_string(pos + precision + 1 + 1, buf);
        if (OB_FAIL(min_check_num.from(tmp_string.ptr(), tmp_string.length(), allocator, attr))) {
          LOG_ERROR("fail to call from", K(precision), K(scale), K(tmp_string), K(pos), K(ret));
        } else if (OB_FAIL(max_check_num.from(tmp_string.ptr() + 1, tmp_string.length() - 1, allocator, attr))) {
          LOG_ERROR("fail to call from", K(precision), K(scale), K(tmp_string), K(pos), K(ret));
        } else {
          total_alloc_size += sizeof(uint32_t) * (min_check_num.get_length() + max_check_num.get_length());
          LOG_DEBUG("succ to build mysql min max check number", K(precision), K(scale), K(tmp_string), K(total_alloc_size), K(min_check_num), K(max_check_num));
        }

        if (OB_SUCC(ret)) {
          buf[pos + precision + 1] = '0';
          // make min and max numbers.
          ObNumber &min_num = MYSQL_MIN[precision][scale];
          ObNumber &max_num = MYSQL_MAX[precision][scale];
          if (OB_FAIL(min_num.from(tmp_string.ptr(), tmp_string.length(), allocator, attr))) {
            LOG_ERROR("fail to call from", K(precision), K(scale), K(tmp_string), K(pos), K(ret));
          } else if (OB_FAIL(max_num.from(tmp_string.ptr() + 1, tmp_string.length() - 1, allocator, attr))) {
            LOG_ERROR("fail to call from", K(precision), K(scale), K(tmp_string), K(pos), K(ret));
          } else {
            total_alloc_size += sizeof(uint32_t) * (min_num.get_length() + max_num.get_length());
            LOG_DEBUG("succ to build mysql min max number", K(precision), K(scale), K(tmp_string), K(total_alloc_size), K(min_num), K(max_num));
          }
        }
      }
    }
  }

  {
    lib::CompatModeGuard tmp_mode(lib::Worker::CompatMode::ORACLE);
    for (int16_t precision = OB_MIN_NUMBER_PRECISION; OB_SUCC(ret) && precision <= OB_MAX_NUMBER_PRECISION; ++precision) {
      for (int16_t scale = ObNumber::MIN_SCALE; OB_SUCC(ret) && scale <= ObNumber::MAX_SCALE; ++scale) {
        pos = 1;
        MEMSET(buf + pos, 0, BUFFER_SIZE - pos);
        ObNumber &min_check_num = ORACLE_CHECK_MIN[precision][scale + MAX_ORACLE_SCALE_DELTA];
        ObNumber &max_check_num = ORACLE_CHECK_MAX[precision][scale + MAX_ORACLE_SCALE_DELTA];
        ObString tmp_string;

        if (precision >= scale && scale >= 0) {
          /* number(3, 1) => legal range(-99.95, 99.95) */
          if (precision == scale) {
            buf[pos++] = '0';
          }
          MEMSET(buf + pos, '9', precision + 1);
          buf[pos + precision - scale] = '.';
          buf[pos + precision + 1] = '5';
          tmp_string.assign_ptr(buf, pos + precision + 1 + 1);
        } else if (scale < 0) {
          /* number(2, -3) => legal range: (-99500, 99500) */
          MEMSET(buf + pos, '9', precision);
          buf[pos + precision] = '5';
          MEMSET(buf + pos + precision + 1, '0', 0 - scale - 1);
          tmp_string.assign_ptr(buf, pos + precision - scale);
        } else {
          //number(2, 3) => legal range:(-0.0995, 0.0995)
           buf[pos++] = '0';
           buf[pos++] = '.';
           MEMSET(buf + pos, '0', scale - precision);
           MEMSET(buf + pos + scale - precision, '9', precision);
           buf[pos + scale] = '5';
           tmp_string.assign_ptr(buf, pos + scale + 1);
        }

        // make min and max numbers.
        if (OB_FAIL(min_check_num.from(tmp_string.ptr(), tmp_string.length(), allocator, attr))) {
          LOG_ERROR("fail to call from", K(precision), K(scale), K(tmp_string), K(ret));
        } else if (OB_FAIL(max_check_num.from(tmp_string.ptr() + 1, tmp_string.length() - 1, allocator, attr))) {
          LOG_ERROR("fail to call from", K(precision), K(scale), K(tmp_string), K(ret));
        } else {
          total_alloc_size += sizeof(uint32_t) * (min_check_num.get_length() + max_check_num.get_length());
          LOG_DEBUG("succ to build min max number", K(precision), K(scale), K(tmp_string), K(total_alloc_size), K(min_check_num), K(max_check_num));
        }
      }
    }
  }
  return ret;
}

// Type is float/double
template <typename Type>
int real_range_check_only(const ObAccuracy &accuracy, Type value)
{
  int ret = OB_SUCCESS;
  const ObPrecision precision = accuracy.get_precision();
  const ObScale scale = accuracy.get_scale();
  if (OB_LIKELY(precision > 0) &&
      OB_LIKELY(scale >= 0) &&
      OB_LIKELY(precision >= scale)) {
    Type integer_part = static_cast<Type>(pow(10.0, static_cast<double>(precision - scale)));
    Type decimal_part = static_cast<Type>(pow(10.0, static_cast<double>(scale)));
    Type max_value = integer_part - 1 / decimal_part;
    Type min_value = -max_value;
    if (OB_FAIL(numeric_range_check(value, min_value, max_value, value))) {
    }
  }
  return ret;
}

#define BOUND_INFO_START_POS 18

template<typename T>
ObPrecision get_precision_for_integer(T value)
{
  static const uint64_t bound_info[] = {
      INT64_MAX + 1ULL,
      999999999999999999ULL,
      99999999999999999ULL,
      9999999999999999ULL,
      999999999999999ULL,
      99999999999999ULL,
      9999999999999ULL,
      999999999999ULL,
      99999999999ULL,
      9999999999ULL,
      999999999ULL,
      99999999ULL,
      9999999ULL,
      999999ULL,
      99999ULL,
      9999ULL,
      999ULL,
      99ULL,
      9ULL,
      99ULL,
      999ULL,
      9999ULL,
      99999ULL,
      999999ULL,
      9999999ULL,
      99999999ULL,
      999999999ULL,
      9999999999ULL,
      99999999999ULL,
      999999999999ULL,
      9999999999999ULL,
      99999999999999ULL,
      999999999999999ULL,
      9999999999999999ULL,
      99999999999999999ULL,
      999999999999999999ULL,
      9999999999999999999ULL,
      UINT64_MAX,
    };
  int64_t flag = std::less<T>()(value, 0) ? -1 : 1;
  uint64_t abs_value = value * flag;
  const uint64_t *iter = bound_info + BOUND_INFO_START_POS;
  while(abs_value > *iter) {
    iter += flag;
  }
  // *(iter - 1) < abs <= *iter
  return static_cast<ObPrecision>((iter - (bound_info + BOUND_INFO_START_POS)) * flag + 1);
}

int ObHexUtils::unhex(const ObString &text, ObCastCtx &cast_ctx, ObObj &result)
{
  int ret = OB_SUCCESS;
  ObString str_result;
  char *buf = NULL;
  const bool need_fill_zero = (1 == text.length() % 2);
  const int32_t tmp_length = text.length() / 2 + need_fill_zero;
  int32_t alloc_length = (0 == tmp_length ? 1 : tmp_length);
  if (OB_ISNULL(cast_ctx.allocator_v2_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("allocator in cast ctx is NULL", K(ret), K(text));
  } else if (OB_ISNULL(buf = static_cast<char *>(cast_ctx.allocator_v2_->alloc(alloc_length)))) {
    result.set_null();
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc memory failed", K(alloc_length), K(ret));
  } else {
    int32_t i = 0;
    char c1 = 0;
    char c2 = 0;
    if (text.length() > 0) {
      if (need_fill_zero) {
        c1 = '0';
        c2 = text[0];
        i = 0;
      } else {
        c1 = text[0];
        c2 = text[1];
        i = 1;
      }
    }
    while (OB_SUCC(ret) && i < text.length()) {
      if (isxdigit(c1) && isxdigit(c2)) {
        buf[i / 2] = (char)((get_xdigit(c1) << 4) | get_xdigit(c2));
        if (i + 2 < text.length()) {
          c1 = text[++i];
          c2 = text[++i];
        } else {
          break;
        }
      } else {
        ret = OB_ERR_INVALID_HEX_NUMBER;
        LOG_WARN("invalid hex number", K(ret), K(c1), K(c2), K(text));
      }
    }

    if (OB_SUCC(ret)) {
      str_result.assign_ptr(buf, tmp_length);
      result.set_varchar(str_result);
    }
  }
  return ret;
}

int ObHexUtils::hex(const ObString &text, ObCastCtx &cast_ctx, ObObj &result)
{
  int ret = OB_SUCCESS;
  ObString str_result;
  char *buf = NULL;
  const int32_t alloc_length = text.empty() ? 1 : text.length() * 2;
  if (OB_ISNULL(cast_ctx.allocator_v2_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("allocator in cast ctx is NULL", K(ret), K(text));
  } else if (OB_ISNULL(buf = static_cast<char *>(
              cast_ctx.allocator_v2_->alloc(alloc_length)))) {
    result.set_null();
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc memory failed", K(ret), K(alloc_length));
  } else {
    static const char *HEXCHARS = "0123456789ABCDEF";
    int32_t pos = 0;
    for (int32_t i = 0; i < text.length(); ++i) {
      buf[pos++] = HEXCHARS[text[i] >> 4 & 0xF];
      buf[pos++] = HEXCHARS[text[i] & 0xF];
    }
    str_result.assign_ptr(buf, pos);
    result.set_varchar(str_result);
    LOG_DEBUG("succ to hex", K(text), "length", text.length(), K(str_result));
  }
  return ret;
}

int ObHexUtils::hex_for_mysql(const uint64_t uint_val, common::ObCastCtx &cast_ctx, common::ObObj &result)
{

  int ret = OB_SUCCESS;
  char *buf = NULL;
  const int32_t MAX_INT64_LEN = 20;
  if (OB_ISNULL(cast_ctx.allocator_v2_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("allocator in cast ctx is NULL", K(ret), K(uint_val));
  } else if (OB_ISNULL(buf = static_cast<char *>(
              cast_ctx.allocator_v2_->alloc(MAX_INT64_LEN)))) {
    result.set_null();
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc memory failed", K(ret));
  } else {
    int pos = snprintf(buf, MAX_INT64_LEN, "%lX", uint_val);
    if (OB_UNLIKELY(pos <= 0) || OB_UNLIKELY(pos >= MAX_INT64_LEN)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_ERROR("size is overflow", K(ret), K(uint_val));
    } else {
      ObString str_result(pos, buf);
      result.set_varchar(str_result);
    }
  }
  return ret;
}


//https://docs.oracle.com/en/database/oracle/oracle-database/12.2/sqlrf/RAWTOHEX.html
//As a SQL built-in function, RAWTOHEX accepts an argument of any scalar data type other than LONG,
//LONG RAW, CLOB, NCLOB, BLOB, or BFILE. If the argument is of a data type other than RAW,
//then this function converts the argument value, which is represented using some number of data bytes,
//into a RAW value with the same number of data bytes. The data itself is not modified in any way,
//but the data type is recast to a RAW data type.
int ObHexUtils::rawtohex(const ObObj &text, ObCastCtx &cast_ctx, ObObj &result)
{
  int ret = OB_SUCCESS;

  if (text.is_null()) {
    result.set_null();
  } else {
    ObString str;
    ObObj num_obj;
    char *splice_num_str = NULL; // 用于拼接 number 类型的元数据 Desc 和 digits_ 指向的实际存储的数据
    ObOTimestampData time_value;
    switch (text.get_type()) {
      //TODO::this should same as oracle, and support dump func @yanhua
      case ObTinyIntType:
      case ObSmallIntType:
      case ObInt32Type:
      case ObIntType: {
        int64_t int_value = text.get_int();
        number::ObNumber nmb;
        if (OB_FAIL(nmb.from(int_value, cast_ctx))) {
          LOG_WARN("fail to int_number", K(ret), K(int_value), "type", text.get_type());
        } else {
          num_obj.set_number(ObNumberType, nmb);
          int32_t alloc_len = static_cast<int32_t>(sizeof(num_obj.get_number_desc()) + num_obj.get_number_byte_length());
          if (OB_ISNULL(cast_ctx.allocator_v2_)) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("allocator in cast ctx is NULL", K(ret));
          } else if (OB_ISNULL(splice_num_str = static_cast<char *>(cast_ctx.allocator_v2_->alloc(alloc_len)))) {
            result.set_null();
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_ERROR("alloc memory failed", K(ret), K(alloc_len));
          } else {
            MEMCPY(splice_num_str, &(num_obj.get_number_desc()), sizeof(num_obj.get_number_desc()));
            MEMCPY(splice_num_str + sizeof(num_obj.get_number_desc()), num_obj.get_data_ptr(), num_obj.get_number_byte_length());
            str.assign_ptr(static_cast<const char *>(splice_num_str), alloc_len);
          }
          LOG_DEBUG("succ to int_number", K(ret), K(int_value), "type", num_obj.get_type(), K(nmb), K(str));
        }
        break;
      }
      case ObFloatType: {
        float float_value = text.get_float();
        number::ObNumber nmb;
        ObPrecision res_precision = -1;
        ObScale res_scale = -1;
        char buf[MAX_DOUBLE_STRICT_PRINT_SIZE];
        MEMSET(buf, 0, MAX_DOUBLE_STRICT_PRINT_SIZE);
        int64_t length = ob_gcvt_opt(float_value, OB_GCVT_ARG_FLOAT, static_cast<int32_t>(sizeof(buf) - 1),
                                     buf, NULL, TRUE, TRUE);
        ObString float_str(sizeof(buf), static_cast<int32_t>(length), buf);
        if (OB_FAIL(nmb.from_sci_opt(float_str.ptr(), float_str.length(),
                                     *cast_ctx.allocator_v2_, &res_precision, &res_scale))) {
          LOG_WARN("fail to from str to number", K(ret), K(float_str));
        } else {
          num_obj.set_number(ObNumberType, nmb);
          int32_t alloc_len = static_cast<int32_t>(sizeof(num_obj.get_number_desc()) + num_obj.get_number_byte_length());
          if (OB_ISNULL(cast_ctx.allocator_v2_)) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("allocator in cast ctx is NULL", K(ret));
          } else if (OB_ISNULL(splice_num_str = static_cast<char *>(cast_ctx.allocator_v2_->alloc(alloc_len)))) {
            result.set_null();
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_ERROR("alloc memory failed", K(ret), K(alloc_len));
          } else {
            MEMCPY(splice_num_str, &(num_obj.get_number_desc()), sizeof(num_obj.get_number_desc()));
            MEMCPY(splice_num_str + sizeof(num_obj.get_number_desc()), num_obj.get_data_ptr(), num_obj.get_number_byte_length());
            str.assign_ptr(static_cast<const char *>(splice_num_str), alloc_len);
          }
        }
        break;
      }
      case ObDoubleType: {
        double double_value = text.get_double();
        number::ObNumber nmb;
        ObPrecision res_precision = -1;
        ObScale res_scale = -1;
        char buf[MAX_DOUBLE_STRICT_PRINT_SIZE];
        MEMSET(buf, 0, MAX_DOUBLE_STRICT_PRINT_SIZE);
        int64_t length = ob_gcvt_opt(double_value, OB_GCVT_ARG_DOUBLE, static_cast<int32_t>(sizeof(buf) - 1),
                                     buf, NULL, TRUE, TRUE);
        ObString double_str(sizeof(buf), static_cast<int32_t>(length), buf);
        if (OB_FAIL(nmb.from_sci_opt(double_str.ptr(), double_str.length(),
                                     *cast_ctx.allocator_v2_, &res_precision, &res_scale))) {
          LOG_WARN("fail to from str to number", K(ret), K(double_str));
        } else {
          num_obj.set_number(ObNumberType, nmb);
          int32_t alloc_len = static_cast<int32_t>(sizeof(num_obj.get_number_desc()) + num_obj.get_number_byte_length());
          if (OB_ISNULL(cast_ctx.allocator_v2_)) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("allocator in cast ctx is NULL", K(ret));
          } else if (OB_ISNULL(splice_num_str = static_cast<char *>(cast_ctx.allocator_v2_->alloc(alloc_len)))) {
            result.set_null();
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_ERROR("alloc memory failed", K(ret), K(alloc_len));
          } else {
            MEMCPY(splice_num_str, &(num_obj.get_number_desc()), sizeof(num_obj.get_number_desc()));
            MEMCPY(splice_num_str + sizeof(num_obj.get_number_desc()), num_obj.get_data_ptr(), num_obj.get_number_byte_length());
            str.assign_ptr(static_cast<const char *>(splice_num_str), alloc_len);
          }
        }
        break;
      }
      case ObNumberFloatType:
      case ObNumberType: {
        int32_t alloc_len = static_cast<int32_t>(sizeof(text.get_number_desc()) + text.get_number_byte_length());
        if (OB_ISNULL(cast_ctx.allocator_v2_)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("allocator in cast ctx is NULL", K(ret));
        } else if (OB_ISNULL(splice_num_str = static_cast<char *>(cast_ctx.allocator_v2_->alloc(alloc_len)))) {
          result.set_null();
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("alloc memory failed", K(ret), K(alloc_len));
        } else {
          MEMCPY(splice_num_str, &(text.get_number_desc()), sizeof(text.get_number_desc()));
          MEMCPY(splice_num_str + sizeof(text.get_number_desc()), text.get_data_ptr(), text.get_number_byte_length());
          str.assign_ptr(static_cast<const char *>(splice_num_str), alloc_len);
        }
        break;
      }
      case ObDateTimeType: {
        str.assign_ptr(static_cast<const char *>(text.get_data_ptr()), static_cast<int32_t>(sizeof(int64_t)));
        break;
      }
      case ObNVarchar2Type:
      case ObNCharType:
      case ObVarcharType:
      case ObCharType:
      case ObLongTextType:
      case ObRawType: 
      case ObJsonType:
      case ObGeometryType: {
        //https://www.techonthenet.com/oracle/functions/rawtohex.php
        //NOTE:: when convert string to raw, Oracle use utl_raw.cast_to_raw(), while PL/SQL use hextoraw()
        //       here we use utl_raw.cast_to_raw(), as we can not distinguish in which SQL
        str = text.get_varbinary();
        break;
      }
      case ObTimestampTZType:
      case ObTimestampLTZType:
      case ObTimestampNanoType: {
        time_value = text.get_otimestamp_value();
        str.assign_ptr(reinterpret_cast<char *>(&time_value), static_cast<int32_t>(text.get_otimestamp_store_size()));
        break;
      }
      case ObLobType: {
        const ObLobLocator* lob_locator = text.get_lob_locator();
        if (OB_ISNULL(lob_locator)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("lob_locator is null", K(ret));
        } else if (OB_FAIL(lob_locator->get_payload(str))) {
          LOG_WARN("get payload from lob_locator failed", K(ret), K(*lob_locator));
        }
        break;
      }
      default: {
        ret = OB_ERR_INVALID_HEX_NUMBER;
        LOG_WARN("invalid hex number", K(ret), K(text), "type", text.get_type());
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(hex(str, cast_ctx, result))) {
        LOG_WARN("fail to convert to hex", K(ret), K(str));
      } else {
        result.set_default_collation_type();
      }
    }
  }
  LOG_DEBUG("succ to rawtohex", "type", text.get_type(), K(text), K(result), K(lbt()));
  return ret;
}

int ObHexUtils::hextoraw(const ObObj &text, ObCastCtx &cast_ctx, ObObj &result)
{
  int ret = OB_SUCCESS;
  if (text.is_null()) {
    result.set_null();
  } else if (text.is_numeric_type()) {
    number::ObNumber nmb_val;
    if (OB_FAIL(get_uint(text, cast_ctx, nmb_val))) {
      LOG_WARN("fail to get uint64", K(ret), K(text));
    } else if (OB_FAIL(uint_to_raw(nmb_val, cast_ctx, result))) {
      LOG_WARN("fail to convert to hex", K(ret), K(nmb_val));
    }
  } else if (text.is_raw() || text.is_blob()) {
    //fast path
    if (OB_FAIL(copy_raw(text, cast_ctx, result))) {
      LOG_WARN("fail to convert to hex", K(ret), K(text));
    }
  } else if (text.is_character_type() || text.is_varbinary_or_binary()) {
    ObString utf8_string;
    if (OB_FAIL(convert_string_collation(text.get_string(),
                                         text.get_collation_type(),
                                         utf8_string,
                                         ObCharset::get_system_collation(),
                                         cast_ctx))) {
      LOG_WARN("convert_string_collation", K(ret));
    } else if (OB_FAIL(unhex(utf8_string, cast_ctx, result))) {
      LOG_WARN("fail to convert to hex", K(ret), K(text));
    } else {
      result.set_raw(result.get_raw());
    }
  } else {
    ret = OB_ERR_INVALID_HEX_NUMBER;
    LOG_WARN("invalid hex number", K(ret), K(text));
  }
  LOG_DEBUG("succ to hextoraw", "type", text.get_type(), K(text), K(result.get_raw()), K(result), K(lbt()));
  return ret;
}

int ObHexUtils::get_uint(const ObObj &obj, ObCastCtx &cast_ctx, number::ObNumber &out)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ob_is_accurate_numeric_type(obj.get_type()))) {
    ret = OB_ERR_INVALID_HEX_NUMBER;
    LOG_WARN("invalid hex number", K(ret), K(obj));
  } else if (obj.is_number() || obj.is_unumber()) {
    const number::ObNumber &value = obj.get_number();
    if (OB_FAIL(out.from(value, cast_ctx))) {
      LOG_WARN("deep copy failed", K(ret), K(obj));
    } else if (OB_UNLIKELY(!out.is_integer()) || OB_UNLIKELY(out.is_negative())) {
      ret = OB_ERR_INVALID_HEX_NUMBER;
      LOG_WARN("invalid hex number", K(ret), K(out));
    } else if (OB_FAIL(out.round(0))) {
      LOG_WARN("round failed", K(ret), K(out));
    }
  } else {
    if (OB_UNLIKELY(obj.get_int() < 0)) {
      ret = OB_ERR_INVALID_HEX_NUMBER;
      LOG_WARN("invalid hex number", K(ret), K(obj));
    } else if (OB_FAIL(out.from(obj.get_int(), cast_ctx))) {
      LOG_WARN("deep copy failed", K(ret), K(obj));
    }
  }
  return ret;
}

int ObHexUtils::uint_to_raw(const number::ObNumber &uint_num, ObCastCtx &cast_ctx, ObObj &result)
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
    if (OB_FAIL(unhex(uint_str, cast_ctx, result))) {
      LOG_WARN("fail to str_to_raw", K(ret), K(result));
    } else {
      result.set_raw(result.get_raw());
    }
  }
  return ret;
}

int ObHexUtils::copy_raw(const common::ObObj &obj, common::ObCastCtx &cast_ctx,
    common::ObObj &result)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  const ObString &value = obj.get_raw();
  const int32_t alloc_length = value.empty() ? 1 : value.length();
  if (OB_ISNULL(cast_ctx.allocator_v2_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("allocator in cast ctx is NULL", K(ret));
  } else if (OB_ISNULL(buf = static_cast<char *>(cast_ctx.allocator_v2_->alloc(alloc_length)))) {
    result.set_null();
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc memory failed", K(ret), K(alloc_length));
  } else {
    MEMCPY(buf, value.ptr(), value.length());
    result.set_raw(buf, value.length());
  }
  return ret;
}

static int check_convert_string(const ObObjType expect_type, ObObjCastParams &params,
    const ObObj &in, ObObj &out)
{
  int ret = OB_SUCCESS;
  //UNUSED(expect_type);
  //UNUSED(params);
//  if (lib::is_oracle_mode()) {
//    //varchar2 --> raw
//    if (in.is_varchar_or_char() && ob_is_varbinary_type(expect_type, params.expect_obj_collation_)) {
//      if (OB_FAIL(ObHexUtils::hextoraw(in, params, out))) {
//        LOG_WARN("fail to hextoraw", K(ret), K(in));
//      }
//    //raw --> varchar2
//    } else if (in.is_varbinary() && ob_is_varchar_char_type(expect_type, params.expect_obj_collation_)) {
//      if (OB_FAIL(ObHexUtils::rawtohex(in, params, out))) {
//        LOG_WARN("fail to rawtohex", K(ret), K(in));
//      }
//    } else {
//      //TODO::for lob, for long raw @yanhua, @hanhui
//      out = in;
//      LOG_DEBUG("do nothing");
//    }
//    LOG_DEBUG("finish check_convert_string", K(ret), "in_type", in.get_type(), "in_cs_type", in.get_collation_type(), K(in), K(expect_type), "expect_cs_type", params.dest_collation_, K(out));
  if (lib::is_oracle_mode()
      && ob_is_blob(expect_type, params.expect_obj_collation_)
      && !in.is_blob() && !in.is_raw()) {
    if (in.is_varchar_or_char()) {
      if (OB_FAIL(ObHexUtils::hextoraw(in, params, out))) {
        LOG_WARN("fail to hextoraw for blob", K(ret), K(in));
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_ERROR("invalid use of blob type", K(ret), K(in), K(expect_type));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "Cast to blob type");
    }
  } else {
    out = in;
  }
  return ret;
}

static int check_convert_string(const ObObjType expect_type, ObObjCastParams &params,
    const ObString &in_string, ObObj &out)
{
  ObObj tmp_obj;
  tmp_obj.set_varchar(in_string);
  return check_convert_string(expect_type, params, tmp_obj, out);
}

static int check_convert_string(const ObObjType expect_type, ObObjCastParams &params,
    const char *in_str, const int64_t len, ObObj &out)
{
  ObObj tmp_obj;
  tmp_obj.set_varchar(in_str, static_cast<ObString::obstr_size_t>(len));
  return check_convert_string(expect_type, params, tmp_obj, out);
}

static int set_json_bin_res(ObObjCastParams *params, ObObj *obj, const ObString &raw_bin)
{
  int ret = OB_SUCCESS;
  bool has_lob_header = !IS_CLUSTER_VERSION_BEFORE_4_1_0_0;
  sql::ObTextStringObObjResult text_result(ObJsonType, params, obj, has_lob_header);
  if (OB_FAIL(text_result.init(raw_bin.length(), params->allocator_v2_))) {
    LOG_WARN("init lob result failed");
  } else if (OB_FAIL(text_result.append(raw_bin.ptr(), raw_bin.length()))) {
    LOG_WARN("failed to append realdata", K(ret), K(raw_bin), K(text_result));
  } else {
    text_result.set_result();
  }
  return ret;
}

static int set_geo_res(ObObjCastParams *params, ObObj *obj, const ObString &wkb)
{
  int ret = OB_SUCCESS;
  bool has_lob_header = !IS_CLUSTER_VERSION_BEFORE_4_1_0_0;
  sql::ObTextStringObObjResult text_result(ObGeometryType, params, obj, has_lob_header);
  if (OB_FAIL(text_result.init(wkb.length(), params->allocator_v2_))) {
    LOG_WARN("init lob result failed");
  } else if (OB_FAIL(text_result.append(wkb.ptr(), wkb.length()))) {
    LOG_WARN("failed to append realdata", K(ret), K(wkb), K(text_result));
  } else {
    text_result.set_result();
  }
  return ret;
}

////////////////////////////////////////////////////////////////
// Int -> XXX

static int int_int(const ObObjType expect_type, ObObjCastParams &params, const ObObj &in,
                   ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObPrecision res_precision = -1;
  if (OB_UNLIKELY(ObIntTC != in.get_type_class()
                  || ObIntTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    int64_t value = in.get_int();
    if (in.get_type() > expect_type && CAST_FAIL(int_range_check(expect_type, value, value))) {
    } else {
      out.set_int(expect_type, value);
      res_precision = get_precision_for_integer(value);
    }
  }
  SET_RES_ACCURACY(res_precision, DEFAULT_SCALE_FOR_INTEGER, DEFAULT_LENGTH_FOR_NUMERIC);
  return ret;
}

static int int_uint(const ObObjType expect_type, ObObjCastParams &params, const ObObj &in,
                    ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObPrecision res_precision = -1;
  if (OB_UNLIKELY(ObIntTC != in.get_type_class()
                  || ObUIntTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (CM_SKIP_CAST_INT_UINT(cast_mode)) {
    out = in;
    res_precision = get_precision_for_integer(out.get_uint64());
  } else {
    uint64_t value = static_cast<uint64_t>(in.get_int());
    if (CM_NEED_RANGE_CHECK(cast_mode)
        && CAST_FAIL(uint_range_check(expect_type, in.get_int(), value))) {
    } else {
      out.set_uint(expect_type, value);
      res_precision = get_precision_for_integer(out.get_uint64());
    }
  }
  SET_RES_ACCURACY(res_precision, DEFAULT_SCALE_FOR_INTEGER, DEFAULT_LENGTH_FOR_NUMERIC);
  return ret;
}

static int int_float(const ObObjType expect_type, ObObjCastParams &params,
                     const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObIntTC != in.get_type_class()
                  || ObFloatTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    double value = static_cast<double>(in.get_int());
    if (ObUFloatType == expect_type && CAST_FAIL(numeric_negative_check(value))) {
    } else {
      out.set_float(expect_type, static_cast<float>(value));
    }
  }
  SET_RES_ACCURACY(PRECISION_UNKNOWN_YET, SCALE_UNKNOWN_YET, LENGTH_UNKNOWN_YET);
  return ret;
}

static int int_double(const ObObjType expect_type, ObObjCastParams &params,
                      const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObIntTC != in.get_type_class()
                  || ObDoubleTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    double value = static_cast<double>(in.get_int());
    if (ObUDoubleType == expect_type && CAST_FAIL(numeric_negative_check(value))) {
    } else {
      out.set_double(expect_type, value);
    }
  }
  SET_RES_ACCURACY(PRECISION_UNKNOWN_YET, SCALE_UNKNOWN_YET, LENGTH_UNKNOWN_YET);
  return ret;
}

static int int_number(const ObObjType expect_type, ObObjCastParams &params,
                      const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObPrecision res_precision = -1;
  if (OB_UNLIKELY(ObIntTC != in.get_type_class()
                  || ObNumberTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    int64_t value = in.get_int();
    number::ObNumber nmb;
    if (ObUNumberType == expect_type && CAST_FAIL(numeric_negative_check(value))) {
    } else if (OB_FAIL(nmb.from(value, params))) {
    } else {
      out.set_number(expect_type, nmb);
      res_precision = get_precision_for_integer(in.get_int());
    }
  }
  SET_RES_ACCURACY(res_precision, DEFAULT_SCALE_FOR_INTEGER, DEFAULT_LENGTH_FOR_NUMERIC);
  return ret;
}

static int int_datetime(const ObObjType expect_type, ObObjCastParams &params,
                        const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObIntTC != in.get_type_class()
                  || ObDateTimeTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    ObTimeConvertCtx cvrt_ctx(params.dtc_params_.tz_info_, ObTimestampType == expect_type);
    int64_t value = 0;
    if (in.get_int() < 0) {
      ret = OB_INVALID_DATE_FORMAT;
    } else {
      ObDateSqlMode date_sql_mode;
      date_sql_mode.allow_invalid_dates_ = CM_IS_EXPLICIT_CAST(cast_mode)
                                           ? false : CM_IS_ALLOW_INVALID_DATES(cast_mode);
      date_sql_mode.no_zero_date_ = CM_IS_EXPLICIT_CAST(cast_mode)
                                    ? false : CM_IS_NO_ZERO_DATE(cast_mode);
      ret = ObTimeConverter::int_to_datetime(in.get_int(), 0, cvrt_ctx, value, date_sql_mode);
    }
    if (CAST_FAIL(ret)) {
    } else {
      SET_RES_DATETIME(out);
    }
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, MIN_SCALE_FOR_TEMPORAL, DEFAULT_LENGTH_FOR_TEMPORAL);
  return ret;
}

static int int_datetime_interval(const ObObjType expect_type, ObObjCastParams &params,
    const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObIntTC != in.get_type_class())
      || OB_UNLIKELY(ObDateTimeTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else {

    //oracle treate int as day
    int64_t value = in.get_int() * USECS_PER_DAY;
    SET_RES_DATETIME(out);
    LOG_DEBUG("succ to int_datetime_interval", K(ret), K(in), K(value), K(expect_type));
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, MIN_SCALE_FOR_TEMPORAL, DEFAULT_LENGTH_FOR_TEMPORAL);
  return ret;
}
static int int_date(const ObObjType expect_type, ObObjCastParams &params,
                    const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObIntTC != in.get_type_class()
                  || ObDateTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    int32_t value = 0;
    ObDateSqlMode date_sql_mode;
    date_sql_mode.allow_invalid_dates_ = CM_IS_EXPLICIT_CAST(cast_mode)
                                          ? false : CM_IS_ALLOW_INVALID_DATES(cast_mode);
    date_sql_mode.no_zero_date_ = CM_IS_EXPLICIT_CAST(cast_mode)
                                  ? false : CM_IS_NO_ZERO_DATE(cast_mode);
    if (CAST_FAIL(ObTimeConverter::int_to_date(in.get_int(), value, date_sql_mode))) {
    } else {
      SET_RES_DATE(out);
    }
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, DEFAULT_SCALE_FOR_DATE, DEFAULT_LENGTH_FOR_TEMPORAL);
  return ret;
}

static int int_time(const ObObjType expect_type, ObObjCastParams &params,
                    const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObIntTC != in.get_type_class()
                  || ObTimeTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    int64_t value = 0;
    if (CAST_FAIL(ObTimeConverter::int_to_time(in.get_int(), value))) {
    } else {
      SET_RES_TIME(out);
    }
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, MIN_SCALE_FOR_TEMPORAL, DEFAULT_LENGTH_FOR_TEMPORAL);
  return ret;
}

static int int_year(const ObObjType expect_type, ObObjCastParams &params,
                    const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObIntTC != in.get_type_class()
                  || ObYearTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    uint8_t value = 0;
    if (CAST_FAIL(ObTimeConverter::int_to_year(in.get_int(), value))) {
    } else {
      SET_RES_YEAR(out);
    }
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, DEFAULT_SCALE_FOR_YEAR, DEFAULT_LENGTH_FOR_TEMPORAL);
  return ret;
}

static int int_string(const ObObjType expect_type, ObObjCastParams &params,
                      const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObLength res_length = -1;
  if (OB_UNLIKELY(ObIntTC != in.get_type_class()
                  || (ObStringTC != ob_obj_type_class(expect_type)
                  && ObTextTC != ob_obj_type_class(expect_type)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    ObObj tmp_out;
    ObFastFormatInt ffi(in.get_int());
    ObString tmp_str;
    // 进行字符集转换，使用ObObjCastParams.alloc申请结果的空间。
    if (OB_FAIL(convert_string_collation(ObString(ffi.length(), ffi.ptr()),
                                         ObCharset::get_system_collation(),
                                         tmp_str,
                                         params.dest_collation_,
                                         params))) {
      LOG_WARN("fail to convert string collation", K(ret));
    // 检查字符转换，如果是oracle模式从varchar/char转为blob，那么使用hextoraw接口对字符串进行转换，
    // 结果放在ObObjCastParams.allocator_v2_->alloc分配的空间, 与上面是同一个alloc接口
    } else if (OB_FAIL(check_convert_string(expect_type, params, tmp_str.ptr(),
                                            tmp_str.length(), tmp_out))) {
      LOG_WARN("fail to check_convert_string", K(ret), K(in), K(expect_type));
    // 根据align_offset和zerofill进行前后补，结果放在ObObjCastParams.alloc申请的空间中。
    } else if (OB_FAIL(copy_string(params, expect_type, tmp_out.get_string(), out))) {
    } else {
      res_length = static_cast<ObLength>(out.get_string_len());
      out.set_collation_type(params.dest_collation_);
    }
  }
  SET_RES_ACCURACY_STRING(expect_type, DEFAULT_PRECISION_FOR_STRING, res_length);
  UNUSED(cast_mode);
  return ret;
}

static int int_bit(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  UNUSED(cast_mode);
  int ret = OB_SUCCESS;
  uint64_t value = static_cast<uint64_t>(in.get_int());
  int32_t bit_len = 0;
  if (OB_UNLIKELY(ObIntTC != in.get_type_class()
                  || ObBitTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(get_bit_len(value, bit_len))) {
    LOG_WARN("fail to get bit len", K(ret), K(value), K(bit_len));
  } else {
    SET_RES_BIT(out);
    SET_RES_ACCURACY(static_cast<ObPrecision>(bit_len), DEFAULT_SCALE_FOR_BIT, DEFAULT_LENGTH_FOR_NUMERIC);
  }
  return ret;
}

static int uint_enum(const ObExpectType &expect_type, ObObjCastParams &params, const ObObj &in, ObObj &out);
static int int_enum(const ObExpectType &expect_type, ObObjCastParams &params, const ObObj &in, ObObj &out)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObEnumType != expect_type.get_type())
      || OB_UNLIKELY(ObIntTC != in.get_type_class())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(expect_type), K(in), K(ret));
  } else {
    uint64_t value = static_cast<uint64_t>(in.get_int());
    ObObj uint_val;
    uint_val.set_uint64(value);
    if (OB_FAIL(uint_enum(expect_type, params, uint_val, out))) {
      LOG_WARN("fail to cast uint to enum", K(expect_type), K(in), K(uint_val), K(out), K(ret));
    }
  }
  return ret;
}


static int uint_set(const ObExpectType &expect_type, ObObjCastParams &params,
                    const ObObj &in, ObObj &out);
static int int_set(const ObExpectType &expect_type, ObObjCastParams &params,
                   const ObObj &in, ObObj &out)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObSetType != expect_type.get_type())
      || OB_UNLIKELY(ObIntTC != in.get_type_class())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(expect_type), K(in), K(ret));
  } else {
    uint64_t value = static_cast<uint64_t>(in.get_int());
    ObObj uint_val;
    uint_val.set_uint64(value);
    if (OB_FAIL(uint_set(expect_type, params, uint_val, out))) {
      LOG_WARN("fail to cast uint to enum", K(expect_type), K(in), K(uint_val), K(out), K(ret));
    }
  }
  return ret;
}
static int string_lob(const ObObjType expect_type, ObObjCastParams &params,
                      const ObObj &in, ObObj &out, const ObCastMode cast_mode,
                      const ObLobLocator *lob_locator);
static int string_lob(const ObObjType expect_type, ObObjCastParams &params,
                      const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  return string_lob(expect_type, params, in, out, cast_mode, NULL);
}

#define CAST_TO_LOB_METHOD(TYPE, TYPE_CLASS)  \
  static int TYPE##_lob(const ObObjType expect_type, ObObjCastParams &params,        \
                        const ObObj &in, ObObj &out, const ObCastMode cast_mode)    \
  {                           \
    int ret = OB_SUCCESS;     \
    ObObj tmp_val;            \
    if (OB_UNLIKELY(TYPE_CLASS != in.get_type_class()                     \
                    || ObLobTC != ob_obj_type_class(expect_type))) {      \
      ret = OB_ERR_UNEXPECTED;                                            \
      LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));     \
    } else if (OB_FAIL(TYPE##_string(ObLongTextType, params, in, tmp_val, cast_mode))) {         \
      LOG_WARN("fail to cast" #TYPE "to string", K(ret), K(expect_type), K(in), K(tmp_val));  \
    } else if (OB_FAIL(string_lob(expect_type, params, tmp_val, out, cast_mode))) {           \
      LOG_WARN("fail to cast string to lob", K(ret), K(expect_type), K(in), K(tmp_val));      \
    }             \
    return ret;   \
  }

CAST_TO_LOB_METHOD(int, ObIntTC);

#define CAST_TO_TEXT_METHOD(TYPE, TYPE_CLASS)                                                 \
  static int TYPE##_text(const ObObjType expect_type, ObObjCastParams &params,                \
                         const ObObj &in, ObObj &out, const ObCastMode cast_mode)             \
  {                                                                                           \
    int ret = OB_SUCCESS;                                                                     \
    ObObj tmp_val;                                                                            \
    if (OB_UNLIKELY(TYPE_CLASS != in.get_type_class()                                         \
                    || ObTextTC != ob_obj_type_class(expect_type))) {                         \
      ret = OB_ERR_UNEXPECTED;                                                                \
      LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));                    \
    } else if (OB_FAIL(TYPE##_string(ObVarcharType, params, in, tmp_val, cast_mode))) {       \
      LOG_WARN("fail to cast" #TYPE "to string", K(ret), K(expect_type), K(in), K(tmp_val));  \
    } else if (OB_FAIL(string_text(expect_type, params, tmp_val, out, cast_mode))) {               \
      LOG_WARN("fail to cast string to lob", K(ret), K(expect_type), K(in), K(tmp_val));      \
    }                                                                                         \
    return ret;                                                                               \
  }

static int string_text(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode,
                       const ObLobLocatorV2 *lob_locator);
static int string_text(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  return string_text(expect_type, params, in, out, cast_mode, NULL);
}

CAST_TO_TEXT_METHOD(int, ObIntTC);

static int int_json(const ObObjType expect_type, ObObjCastParams &params,
                    const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObLength res_length = -1;
  if (CM_IS_COLUMN_CONVERT(cast_mode) && is_mysql_unsupported_json_column_conversion(in.get_type())) {
    ret = OB_ERR_INVALID_JSON_TEXT;
    LOG_USER_ERROR(OB_ERR_INVALID_JSON_TEXT);
  } else if (OB_UNLIKELY(ObIntTC != in.get_type_class() || (ObJsonType != expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if (OB_UNLIKELY(params.allocator_v2_ == NULL)) {
    ret= OB_ERR_UNEXPECTED;
    LOG_ERROR("NULL allocator in json cast function", K(ret), K(in), K(expect_type));
  } else {
    ObJsonInt j_int(in.get_int());
    ObIJsonBase *j_base = &j_int;
    ObString raw_bin;
    if (OB_FAIL(j_base->get_raw_binary(raw_bin, params.allocator_v2_))) {
      LOG_WARN("fail to get int json binary", K(ret), K(in), K(expect_type), K(*j_base));
    } else if (OB_FAIL(set_json_bin_res(&params, &out, raw_bin))) {
      LOG_WARN("fail to fill json bin lob locator", K(ret));
    } else {
      res_length = out.get_val_len();
    }
  }

  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_STRING, DEFAULT_SCALE_FOR_TEXT, res_length)
  return ret;
}

static int common_get_srs_item(omt::ObSrsCacheGuard &srs_guard,
                               const ObString &wkb,
                               const ObSrsItem *&srs,
                               const char *cast_name)
{
  int ret = OB_SUCCESS;
  uint32_t srid = UINT32_MAX;
  // todo : get effective tenant id
  uint64_t tenant_id = MTL_ID();
  if (wkb.length() < WKB_GEO_SRID_SIZE) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("invalid data length", K(ret), K(wkb.length()));
    if (OB_NOT_NULL(cast_name)) {
      LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, cast_name);
    }
  } else if (FALSE_IT(srid = ObGeoWkbByteOrderUtil::read<uint32_t>((const char *)wkb.ptr(),
      ObGeoWkbByteOrder::LittleEndian))) { // adapt mysql, always use little-endian
    // do nothing
  } else if (0 == srid) {
    // do nothing
  } else if (OB_FAIL(OTSRS_MGR->get_tenant_srs_guard(srs_guard))) {
    LOG_WARN("fail to get srs guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(srs_guard.get_srs_item(srid, srs))) {
    LOG_WARN("fail to get srs", K(ret), K(srid));
  }
  return ret;
}

static int common_build_geometry(ObIAllocator &allocator,
                                 const ObString &wkb,
                                 ObGeometry *&geo,
                                 const ObSrsItem *srs,
                                 const char *cast_name)
{
  int ret = OB_SUCCESS;
  ObGeoErrLogInfo log_info;
  if (OB_FAIL(ObGeoTypeUtil::build_geometry(allocator, wkb, geo, srs, log_info))) {
    if (OB_ERR_GIS_INVALID_DATA == ret && OB_NOT_NULL(cast_name)) {
      LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, cast_name);
    } else if (OB_ERR_GEOMETRY_PARAM_LONGITUDE_OUT_OF_RANGE == ret && OB_NOT_NULL(cast_name)) {
      LOG_USER_ERROR(OB_ERR_GEOMETRY_PARAM_LONGITUDE_OUT_OF_RANGE,
                     cast_name,
                     log_info.value_out_of_range_,
                     log_info.min_long_val_,
                     log_info.max_long_val_);
    } else if (OB_ERR_GEOMETRY_PARAM_LATITUDE_OUT_OF_RANGE == ret && OB_NOT_NULL(cast_name)) {
      LOG_USER_ERROR(OB_ERR_GEOMETRY_PARAM_LATITUDE_OUT_OF_RANGE,
                     cast_name,
                     log_info.value_out_of_range_,
                     log_info.min_lat_val_,
                     log_info.max_lat_val_);
    }
  }
  return ret;
}

static int int_geometry(const ObObjType expect_type, ObObjCastParams &params,
                        const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObLength res_length = -1;
  if (OB_ISNULL(params.allocator_v2_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid allocator", K(ret));
  } else {
    ObIAllocator &temp_allocator = *params.allocator_v2_;
    ObGeometry *geo = NULL;
    omt::ObSrsCacheGuard srs_guard;
    const ObSrsItem *srs = NULL;
    int64_t in_val = in.get_int();
    ObFastFormatInt ffi(in_val);
    ObGeoType dst_geo_type = ObGeoCastUtils::get_geo_type_from_cast_mode(cast_mode);
    const char *cast_name = ObGeometryTypeCastUtil::get_cast_name(dst_geo_type);
    ObString str(ffi.length(), ffi.ptr());
    if (OB_FAIL(common_get_srs_item(srs_guard, str, srs, cast_name))) {
      LOG_WARN("fail to get srs item", K(ret), K(str));
    } else if (OB_FAIL(common_build_geometry(temp_allocator, str, geo, srs, cast_name))) {
      LOG_WARN("fail to parse geometry", K(ret), K(in_val), K(dst_geo_type));
    } else {
      ObString res_wkb;
      if (OB_FAIL(ObGeoTypeUtil::to_wkb(temp_allocator, *geo, srs, res_wkb))) {
        LOG_WARN("fail to get wkb", K(ret), K(dst_geo_type));
      } else if (OB_FAIL(set_geo_res(&params, &out, res_wkb))) {
        LOG_WARN("set geo result failed", K(ret));
      } else {
        res_length = static_cast<ObLength>(out.get_string_len());
      }
    }

    if (OB_FAIL(ret) && CM_IS_COLUMN_CONVERT(cast_mode) && ObGeoType::GEOMETRY == dst_geo_type) { // adapt mysql
      ret = OB_ERR_CANT_CREATE_GEOMETRY_OBJECT;
      LOG_USER_ERROR(OB_ERR_CANT_CREATE_GEOMETRY_OBJECT);
    }
  }
  SET_RES_ACCURACY_STRING(expect_type, DEFAULT_PRECISION_FOR_STRING, res_length);
  return ret;
}

////////////////////////////////////////////////////////////////
// UInt -> XXX

static int uint_int(const ObObjType expect_type, ObObjCastParams &params,
                    const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObPrecision res_precision = -1;
  if (OB_UNLIKELY(ObUIntTC != in.get_type_class()
                  || ObIntTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (CM_IS_EXTERNAL_CALL(cast_mode) && CM_SKIP_CAST_INT_UINT(cast_mode)) {
    out = in;
    res_precision = get_precision_for_integer(out.get_int());
  } else {
    int64_t value = static_cast<int64_t>(in.get_uint64());
    if (CM_NEED_RANGE_CHECK(cast_mode)
        && CAST_FAIL(int_upper_check(expect_type, in.get_uint64(), value))) {
    } else {
      out.set_int(expect_type, value);
      res_precision = get_precision_for_integer(value);
    }
  }
  SET_RES_ACCURACY(res_precision, DEFAULT_SCALE_FOR_INTEGER, DEFAULT_LENGTH_FOR_NUMERIC);
  return ret;
}

static int uint_uint(const ObObjType expect_type, ObObjCastParams &params,
                     const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObPrecision res_precision = -1;
  if (OB_UNLIKELY(ObUIntTC != in.get_type_class()
                  || ObUIntTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    uint64_t value = in.get_uint64();
    if (in.get_type() > expect_type && CAST_FAIL(uint_upper_check(expect_type, value))) {
    } else {
      out.set_uint(expect_type, value);
      res_precision = get_precision_for_integer(value);
    }
  }
  SET_RES_ACCURACY(res_precision, DEFAULT_SCALE_FOR_INTEGER, DEFAULT_LENGTH_FOR_NUMERIC);
  return ret;
}

static int uint_float(const ObObjType expect_type, ObObjCastParams &params,
                      const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObUIntTC != in.get_type_class()
                  || ObFloatTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    // uint直接转为float的结果，与先转为double再转为float的结果不同，这里与mysql兼容。
    double value = static_cast<double>(in.get_uint64());
    out.set_float(expect_type, static_cast<float>(value));
  }
  UNUSED(cast_mode);
  SET_RES_ACCURACY(PRECISION_UNKNOWN_YET, SCALE_UNKNOWN_YET, LENGTH_UNKNOWN_YET);
  return ret;
}

static int uint_double(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObUIntTC != in.get_type_class()
                  || ObDoubleTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    out.set_double(expect_type, static_cast<double>(in.get_uint64()));
  }
  UNUSED(cast_mode);
  SET_RES_ACCURACY(PRECISION_UNKNOWN_YET, SCALE_UNKNOWN_YET, LENGTH_UNKNOWN_YET);
  return ret;
}

static int uint_number(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  number::ObNumber nmb;
  ObPrecision res_precision = -1;
  if (OB_UNLIKELY(ObUIntTC != in.get_type_class()
                  || ObNumberTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(nmb.from(in.get_uint64(), params))) {
  } else {
    out.set_number(expect_type, nmb);
    res_precision = get_precision_for_integer(in.get_uint64());
  }
  UNUSED(cast_mode);
  SET_RES_ACCURACY(res_precision, DEFAULT_SCALE_FOR_INTEGER, DEFAULT_LENGTH_FOR_NUMERIC);
  return ret;
}

static int uint_datetime(const ObObjType expect_type, ObObjCastParams &params,
                         const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObObj int64;
  if (OB_UNLIKELY(ObUIntTC != in.get_type_class()
                  || ObDateTimeTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(uint_int(ObIntType, params, in, int64, CM_UNSET_NO_CAST_INT_UINT(cast_mode)))) {
  } else if (OB_FAIL(int_datetime(expect_type, params, int64, out, cast_mode))) {
  }
  //has set the accuracy in prev int_datetime call
  return ret;
}

static int uint_date(const ObObjType expect_type, ObObjCastParams &params,
                     const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObObj int64;
  if (OB_UNLIKELY(ObUIntTC != in.get_type_class()
                  || ObDateTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(uint_int(ObIntType, params, in, int64, CM_UNSET_NO_CAST_INT_UINT(cast_mode)))) {
  } else if (OB_FAIL(int_date(expect_type, params, int64, out, cast_mode))) {
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, DEFAULT_SCALE_FOR_DATE, DEFAULT_LENGTH_FOR_TEMPORAL);
  return ret;
}

static int uint_time(const ObObjType expect_type, ObObjCastParams &params,
                     const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObObj int64;
  if (OB_UNLIKELY(ObUIntTC != in.get_type_class()
                  || ObTimeTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(uint_int(ObIntType, params, in, int64, CM_UNSET_NO_CAST_INT_UINT(cast_mode)))) {
  } else if (OB_FAIL(int_time(expect_type, params, int64, out, cast_mode))) {
  }
  //accuracy has been set in prev int_time call
  return ret;
}

static int uint_year(const ObObjType expect_type, ObObjCastParams &params,
                     const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObObj int64;
  if (OB_UNLIKELY(ObUIntTC != in.get_type_class()
                  || ObYearTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(uint_int(ObIntType, params, in, int64, CM_UNSET_NO_CAST_INT_UINT(cast_mode)))) {
  } else if (OB_FAIL(int_year(expect_type, params, int64, out, cast_mode))) {
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, DEFAULT_SCALE_FOR_YEAR, DEFAULT_LENGTH_FOR_TEMPORAL);
  return ret;
}

static int uint_string(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObLength res_length = -1;
  if (OB_UNLIKELY(ObUIntTC != in.get_type_class()
                  || (ObStringTC != ob_obj_type_class(expect_type)
                  && ObTextTC != ob_obj_type_class(expect_type)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    ObObj tmp_out;
    ObFastFormatInt ffi(in.get_uint64());
    ObString tmp_str;
    if (OB_FAIL(convert_string_collation(ObString(ffi.length(), ffi.ptr()),
        ObCharset::get_system_collation(), tmp_str, params.dest_collation_, params))) {
      LOG_WARN("fail to convert string collation", K(ret));
    } else if (OB_FAIL(check_convert_string(expect_type, params,
                      tmp_str.ptr(), tmp_str.length(), tmp_out))) {
      LOG_WARN("fail to check_convert_string", K(ret), K(in), K(expect_type));
    } else if (OB_FAIL(copy_string(params, expect_type, tmp_out.get_string(), out))) {
    } else {
      res_length = static_cast<ObLength>(out.get_string_len());
      out.set_collation_type(params.dest_collation_);
    }
  }

  UNUSED(cast_mode);
  SET_RES_ACCURACY_STRING(expect_type, DEFAULT_PRECISION_FOR_STRING, res_length);
  return ret;
}

static int uint_bit(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  UNUSED(cast_mode);
  int ret = OB_SUCCESS;
  uint64_t value = in.get_uint64();
  int32_t bit_len = 0;
  if (OB_UNLIKELY(ObUIntTC != in.get_type_class()
                  || ObBitTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(get_bit_len(value, bit_len))) {
    LOG_WARN("fail to get bit len", K(ret), K(value), K(bit_len));
  } else {
    SET_RES_BIT(out);
    SET_RES_ACCURACY(static_cast<ObPrecision>(bit_len), DEFAULT_SCALE_FOR_BIT, DEFAULT_LENGTH_FOR_NUMERIC);
  }
  return ret;
}

static int uint_enum(const ObExpectType &expect_type, ObObjCastParams &params, const ObObj &in, ObObj &out)
{
 int ret = OB_SUCCESS;
  ObCastMode cast_mode = params.cast_mode_;
  const ObIArray<ObString> * type_infos = NULL;
  if (OB_UNLIKELY(ObEnumType != expect_type.get_type())
      || OB_UNLIKELY(ObUIntTC != in.get_type_class())
      || OB_ISNULL(type_infos = expect_type.get_type_infos())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(expect_type), K(in), K(ret));
  } else {
    uint64_t value = in.get_uint64();
    if (OB_UNLIKELY(0 == value || value > type_infos->count())) {
      value = 0;
      ret = OB_ERR_DATA_TRUNCATED;
      LOG_WARN("input value out of range", K(in), K(expect_type), K(ret));
    }

    if (OB_FAIL(ret) && CM_IS_WARN_ON_FAIL(cast_mode)) {
      params.warning_ = OB_ERR_DATA_TRUNCATED;
      ret = OB_SUCCESS;
    }
    LOG_DEBUG("finish uint_enum", K(ret), K(expect_type), K(in), K(out), KPC(type_infos), K(lbt()));
    SET_RES_ENUM(out);
  }
  return ret;
}

static int uint_set(const ObExpectType &expect_type, ObObjCastParams &params, const ObObj &in, ObObj &out)
{
  int ret = OB_SUCCESS;
  ObCastMode cast_mode = params.cast_mode_;
  const ObIArray<ObString> * type_infos = NULL;
  int64_t val_cnt = 0;
  if (OB_UNLIKELY(ObSetType != expect_type.get_type())
      || OB_UNLIKELY(ObUIntTC != in.get_type_class())
      || OB_ISNULL(type_infos = expect_type.get_type_infos())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(expect_type), K(in), K(ret));
  } else {
    uint64_t value = in.get_uint64();
    val_cnt = type_infos->count();
    if (OB_UNLIKELY(val_cnt <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect val_cnt", K(in), K(out), K(expect_type), K(ret));
    } else if (val_cnt >= 64) {//do nothing
    } else if (val_cnt < 64 && value > ((1ULL << val_cnt) - 1)) {
      value = value & ((1ULL << val_cnt) - 1);
      ret = OB_ERR_DATA_TRUNCATED;
      LOG_WARN("input value out of range", K(in), K(val_cnt), K(ret));
    }

    if (OB_FAIL(ret) && CM_IS_WARN_ON_FAIL(cast_mode)) {
      params.warning_ = OB_ERR_DATA_TRUNCATED;
      ret = OB_SUCCESS;
    }
    LOG_DEBUG("finish uint_set", K(ret), K(expect_type), K(in), K(out), KPC(type_infos), K(lbt()));
    SET_RES_SET(out);
  }
  return ret;
}

CAST_TO_LOB_METHOD(uint, ObUIntTC);
CAST_TO_TEXT_METHOD(uint, ObUIntTC);

static int uint_json(const ObObjType expect_type, ObObjCastParams &params,
                     const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObLength res_length = -1;
  if (CM_IS_COLUMN_CONVERT(cast_mode) && is_mysql_unsupported_json_column_conversion(in.get_type())) {
    ret = OB_ERR_INVALID_JSON_TEXT;
    LOG_USER_ERROR(OB_ERR_INVALID_JSON_TEXT);
  } else if (OB_UNLIKELY(ObUIntTC != in.get_type_class() || (ObJsonType != expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if (OB_UNLIKELY(params.allocator_v2_ == NULL)) {
    ret= OB_ERR_UNEXPECTED;
    LOG_ERROR("NULL allocator in json cast function", K(ret), K(in), K(expect_type));
  } else {
    ObJsonUint j_uint(in.get_uint64());
    ObIJsonBase *j_base = &j_uint;
    ObString raw_bin;
    if (OB_FAIL(j_base->get_raw_binary(raw_bin, params.allocator_v2_))) {
      LOG_WARN("fail to get uint json binary", K(ret), K(in), K(expect_type), K(*j_base));
    } else if (OB_FAIL(set_json_bin_res(&params, &out, raw_bin))) {
      LOG_WARN("fail to fill json bin lob locator", K(ret));
    } else {
      res_length = out.get_val_len();
    }
  }

  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_STRING, DEFAULT_SCALE_FOR_TEXT, res_length)
  return ret;
}

static int uint_geometry(const ObObjType expect_type, ObObjCastParams &params,
                         const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObLength res_length = -1;
  if (OB_ISNULL(params.allocator_v2_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid allocator", K(ret));
  } else {
    ObIAllocator &temp_allocator = *params.allocator_v2_;
    ObGeometry *geo = NULL;
    omt::ObSrsCacheGuard srs_guard;
    const ObSrsItem *srs = NULL;
    uint64_t in_val = in.get_uint64();
    ObFastFormatInt ffi(in_val);
    ObGeoType dst_geo_type = ObGeoCastUtils::get_geo_type_from_cast_mode(cast_mode);
    const char *cast_name = ObGeometryTypeCastUtil::get_cast_name(dst_geo_type);
    ObString str(ffi.length(), ffi.ptr());
    if (OB_FAIL(common_get_srs_item(srs_guard, str, srs, cast_name))) {
      LOG_WARN("fail to get srs item", K(ret), K(str));
    } else if (OB_FAIL(common_build_geometry(temp_allocator, str, geo, srs, cast_name))) {
      LOG_WARN("fail to parse geometry", K(ret), K(in_val), K(dst_geo_type));
    } else {
      ObString res_wkb;
      if (OB_FAIL(ObGeoTypeUtil::to_wkb(temp_allocator, *geo, srs, res_wkb))) {
        LOG_WARN("fail to get wkb", K(ret), K(dst_geo_type));
      } else if (OB_FAIL(set_geo_res(&params, &out, res_wkb))) {
        LOG_WARN("set geo result failed", K(ret));
      } else {
        res_length = static_cast<ObLength>(out.get_string_len());
      }
    }

    if (OB_FAIL(ret) && CM_IS_COLUMN_CONVERT(cast_mode) && ObGeoType::GEOMETRY == dst_geo_type) { // adapt mysql
      ret = OB_ERR_CANT_CREATE_GEOMETRY_OBJECT;
      LOG_USER_ERROR(OB_ERR_CANT_CREATE_GEOMETRY_OBJECT);
    }
  }
  SET_RES_ACCURACY_STRING(expect_type, DEFAULT_PRECISION_FOR_STRING, res_length);
  return ret;
}

////////////////////////////////////////////////////////////////
// Float -> XXX

// directly stacit cast from float to int or uint:
// case 1: float = -99999896450971467776.000000, int64 = -9223372036854775808, uint64 = 9223372036854775808.
// case 2: float = 99999896450971467776.000000, int64 = -9223372036854775808, uint64 = 0.
// case 3: float = -99999904.000000, int64 = -99999904, uint64 = 18446744073609551712.
// case 4: float = 99999904.000000, int64 = 99999904, uint64 = 99999904.
// we can see that if float value is out of range of int or uint value, the casted int or uint
// value can't be used to compare with INT64_MAX and so on, see case 2.
// so we should use float value to determine weither it is in range of int or uint.

// trunc_min_value和trunc_max_value分别是in大于最大值和小于最小值时应该trunc成的值。
// float转int时，如果超过LLONG_MAX应该trunc成LLONG_MIN，而double转int时超过LLONG_MAX应该trunc成LLONG_MAX
int common_double_int(const double in, int64_t &out,
                      const int64_t trunc_min_value,
                      const int64_t trunc_max_value)
{
  int ret = OB_SUCCESS;
  out = 0;
  if (in <= static_cast<double>(LLONG_MIN)) {
    out = trunc_min_value;
    if (in < static_cast<double>(LLONG_MIN)) {
      ret = OB_DATA_OUT_OF_RANGE;
    }
  } else if (in >= static_cast<double>(LLONG_MAX)) {
    // 把相等的情况放进来处理，是因为和LLONG_MAX相等的浮点数转int时结果可能是LLONG_MIN或LLONG_MAX。
    // double转int，以及作为insert value时的float转int，结果是LLONG_MAX；
    // 其他情况下float转int结果为LLONG_MIN。 不报错的行为也是与mysql兼容
    out = trunc_max_value;
    if (in > static_cast<double>(LLONG_MAX)) {
      ret = OB_DATA_OUT_OF_RANGE;
    }
  } else {
    out = static_cast<int64_t>(rint(in));
  }
  return ret;
}

static int float_int(const ObObjType expect_type, ObObjCastParams &params,
                     const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  int64_t value = 0;
  ObPrecision res_precision = -1;
  if (OB_UNLIKELY(ObFloatTC != in.get_type_class()
                  || ObIntTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    //和LLONG_MAX相等的float值cast到int时，如果是insert value结果应该是LLONG_MAX，否则cast结果应该是LLONG_MIN
    if (CAST_FAIL(common_double_int(in.get_float(), value, LLONG_MIN,
                  CM_IS_COLUMN_CONVERT(cast_mode) ? LLONG_MAX : LLONG_MIN))) {
      LOG_WARN("cast float to int failed", K(ret), K(in), K(value));
    } else if (CAST_FAIL(int_range_check(expect_type, value, value))) {
    } else {
      out.set_int(expect_type, value);
    }
    if (OB_SUCC(ret)) {
      res_precision = get_precision_for_integer(value);
    }
  }
  SET_RES_ACCURACY(res_precision, DEFAULT_SCALE_FOR_INTEGER, DEFAULT_LENGTH_FOR_NUMERIC);
  return ret;
}

static int float_uint(const ObObjType expect_type, ObObjCastParams &params,
                      const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  uint64_t value = 0;
  ObPrecision res_precision = -1;
  double in_value = static_cast<double>(in.get_float());
  bool is_column_convert = CM_IS_COLUMN_CONVERT(cast_mode);
  if (OB_UNLIKELY(ObFloatTC != in.get_type_class()
                  || ObUIntTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    if (in_value <= static_cast<double>(LLONG_MIN) || in_value >= static_cast<double>(ULLONG_MAX)) {
      value = static_cast<uint64_t>(LLONG_MIN);
      ret = OB_DATA_OUT_OF_RANGE;
    } else {
      // 兼容mysql的行为，处于(INT64_MAX, UINT64_MAX)内的浮点数，插入到uint列中时cast结果约等于原来的float值
      // 而在其他场景如cast as unsigned时，结果应该为INT64_MAX + 1, 即rint(in_value)->int->uint
      if (is_column_convert) {
        value = static_cast<uint64_t>(rint(in_value));
      } else {
        value = static_cast<uint64_t>(static_cast<int64_t>(rint(in_value)));
      }
      if (in_value < 0 && value != 0) {
        // 这里处理[LLONG_MIN, 0)范围内的in，转换为unsigned应该报OB_DATA_OUT_OF_RANGE。
        // out不等于0避免[-0.5, 0)内的值被误判，因为它们round后的值是0，处于合法范围内。
        ret = OB_DATA_OUT_OF_RANGE;
      }
    }
    if (CAST_FAIL(ret)) {
      LOG_WARN("cast float to uint failed", K(ret), K(in), K(value));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (CM_NEED_RANGE_CHECK(cast_mode) &&
    // 使用value而不是in_value进行range check，因为原来使用in_value会导致输入为负数时，检查一定会失败，
    // cast结果被置为0. mysql的结果是负的浮点数先转为int，再转为uint。
          CAST_FAIL(uint_range_check(expect_type, value, value))) {
  } else {
    out.set_uint(expect_type, value);
  }
  if (OB_SUCC(ret)) {
    res_precision = get_precision_for_integer(value);
  }
  SET_RES_ACCURACY(res_precision, DEFAULT_SCALE_FOR_INTEGER, DEFAULT_LENGTH_FOR_NUMERIC);
  return ret;
}

static int float_float(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  float value = in.get_float();
  if (OB_UNLIKELY(ObFloatTC != in.get_type_class()
                  || ObFloatTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (ObUFloatType == expect_type && CAST_FAIL(numeric_negative_check(value))) {
  } else {
    out.set_float(expect_type, value);
  }
  SET_RES_ACCURACY(PRECISION_UNKNOWN_YET, SCALE_UNKNOWN_YET, LENGTH_UNKNOWN_YET);
  return ret;
}

static int float_double(const ObObjType expect_type, ObObjCastParams &params,
                        const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  double value = static_cast<double>(in.get_float());
  if (OB_UNLIKELY(ObFloatTC != in.get_type_class()
                  || ObDoubleTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (ObUDoubleType == expect_type && CAST_FAIL(numeric_negative_check(value))) {
  } else {
    out.set_double(expect_type, value);
  }
  SET_RES_ACCURACY(PRECISION_UNKNOWN_YET, SCALE_UNKNOWN_YET, LENGTH_UNKNOWN_YET);
  return ret;
}

static int float_number(const ObObjType expect_type, ObObjCastParams &params,
                        const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  float value = in.get_float();
  ObPrecision res_precision = -1;
  ObScale res_scale = -1;
  if (OB_UNLIKELY(ObFloatTC != in.get_type_class()
                  || ObNumberTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (ObUNumberType == expect_type && CAST_FAIL(numeric_negative_check(value))) {
  } else {
    char buf[MAX_DOUBLE_STRICT_PRINT_SIZE];
    MEMSET(buf, 0, MAX_DOUBLE_STRICT_PRINT_SIZE);
    int64_t length = 0;
    if (lib::is_oracle_mode()) {
      length = ob_gcvt_opt(in.get_float(), OB_GCVT_ARG_FLOAT, static_cast<int32_t>(sizeof(buf) - 1),
                           buf, NULL, TRUE, TRUE);
    } else {
      length = ob_gcvt(in.get_float(), OB_GCVT_ARG_DOUBLE, sizeof(buf) - 1, buf, NULL);
    }
    ObString str(sizeof(buf), static_cast<int32_t>(length), buf);
    number::ObNumber nmb;
    if (OB_FAIL(nmb.from_sci_opt(str.ptr(), str.length(), params, &res_precision, &res_scale))) {
      LOG_WARN("fail to from str to number", K(ret), K(str));
    } else {
      out.set_number(expect_type, nmb);
    }
    LOG_DEBUG("finish float to number", K(ret), K(str), K(length), K(in), K(expect_type), K(nmb), K(out), K(lbt()));
  }
  SET_RES_ACCURACY(res_precision, res_scale, DEFAULT_LENGTH_FOR_NUMERIC);
  return ret;
}

static int double_datetime(const ObObjType expect_type, ObObjCastParams &params,
                           const ObObj &in, ObObj &out, const ObCastMode cast_mode);
static int float_datetime(const ObObjType expect_type, ObObjCastParams &params,
                          const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObObj dbl;
  if (OB_UNLIKELY(ObFloatTC != in.get_type_class()
                  || ObDateTimeTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(float_double(ObDoubleType, params, in, dbl, cast_mode))) {
  } else if (OB_FAIL(double_datetime(expect_type, params, dbl, out, cast_mode))) {
  }
  //has set accuracy in prev double_datetime call
  return ret;
}

static int double_datetime_interval(const ObObjType expect_type, ObObjCastParams &params,
                                    const ObObj &in, ObObj &out, const ObCastMode cast_mode);
static int float_datetime_interval(const ObObjType expect_type, ObObjCastParams &params,
                                   const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObObj dbl;
  if (OB_UNLIKELY(ObFloatTC != in.get_type_class()
                  || ObDateTimeTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(float_double(ObDoubleType, params, in, dbl, cast_mode))) {
  } else if (OB_FAIL(double_datetime_interval(expect_type, params, dbl, out, cast_mode))) {
  }
  return ret;
}

static int double_date(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode);
static int float_date(const ObObjType expect_type, ObObjCastParams &params,
                      const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObObj dbl;
  if (OB_UNLIKELY(ObFloatTC != in.get_type_class()
                  || ObDateTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(float_double(ObDoubleType, params, in, dbl, cast_mode))) {
  } else if (OB_FAIL(double_date(expect_type, params, dbl, out, cast_mode))) {
  }
  //has set accuracy in prev double_date call
  return ret;
}

static int double_time(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode);
static int float_time(const ObObjType expect_type, ObObjCastParams &params,
                      const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObObj dbl;
  if (OB_UNLIKELY(ObFloatTC != in.get_type_class()
                  || ObTimeTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(float_double(ObDoubleType, params, in, dbl, cast_mode))) {
  } else if (OB_FAIL(double_time(expect_type, params, dbl, out, cast_mode))) {
  }
  //has set accuracy in prev double_time call
  return ret;
}

static int float_string(const ObObjType expect_type, ObObjCastParams &params,
                        const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
  ObScale scale = in.get_scale();
  int64_t length = 0;
  ObLength res_length = -1;
  if (OB_UNLIKELY(ObFloatTC != in.get_type_class()
                  || (ObStringTC != ob_obj_type_class(expect_type)
                  && ObTextTC != ob_obj_type_class(expect_type)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    if (in.get_float() == -INFINITY) {
      length = strlen("-Inf");
      strncpy(buf, "-Inf", length);
    } else if (in.get_float() == INFINITY) {
      length = strlen("Inf");
      strncpy(buf, "Inf", length);
    } else if (isnan(in.get_float())) {
      length = strlen("Nan");
      strncpy(buf, "Nan", length);
    } else {
      if (0 <= scale) {
        length = ob_fcvt(in.get_float(), scale, sizeof(buf) - 1, buf, NULL);
      } else {
        length = ob_gcvt_opt(in.get_float(), OB_GCVT_ARG_FLOAT, static_cast<int32_t>(sizeof(buf) - 1),
                             buf, NULL, lib::is_oracle_mode(), TRUE);
      }
    }
    ObString str(sizeof(buf), static_cast<int32_t>(length), buf);
    LOG_DEBUG("finish float_string", K(ret), K(in), K(expect_type), K(str));
    ObObj tmp_out;
    ObString tmp_str;
    if (OB_FAIL(convert_string_collation(str, ObCharset::get_system_collation(),
                                         tmp_str, params.dest_collation_, params))) {
      LOG_WARN("fail to convert string collation", K(ret));
    } else if (OB_FAIL(check_convert_string(expect_type, params,
                    tmp_str.ptr(), tmp_str.length(), tmp_out))) {
      LOG_WARN("fail to check_convert_string", K(ret), K(in), K(expect_type));
    } else if (OB_FAIL(copy_string(params, expect_type, tmp_out.get_string(), out))) {
    } else {
      res_length = static_cast<ObLength>(out.get_string_len());
      out.set_collation_type(params.dest_collation_);
    }
  }
  UNUSED(cast_mode);
  SET_RES_ACCURACY_STRING(expect_type, DEFAULT_PRECISION_FOR_STRING, res_length);
  return ret;
}

static int float_bit(const ObObjType expect_type, ObObjCastParams &params,
                        const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  UNUSED(cast_mode);
  int ret = OB_SUCCESS;
  uint64_t value = static_cast<uint64_t>(in.get_float());
  int32_t bit_len = 0;
  if (OB_UNLIKELY(ObFloatTC != in.get_type_class()
                  || ObBitTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(get_bit_len(value, bit_len))) {
    LOG_WARN("fail to get bit len", K(ret), K(value), K(bit_len));
  } else {
    SET_RES_BIT(out);
    SET_RES_ACCURACY(static_cast<ObPrecision>(bit_len), DEFAULT_SCALE_FOR_BIT, DEFAULT_LENGTH_FOR_NUMERIC);
  }

  return ret;
}

static int float_enum(const ObExpectType &expect_type, ObObjCastParams &params, const ObObj &in, ObObj &out)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObEnumType != expect_type.get_type())
      || OB_UNLIKELY(ObFloatTC != in.get_type_class())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(expect_type), K(in), K(ret));
  } else {
    int64_t value = static_cast<int64_t>(in.get_float());
    ObObj int_val(value);
    if (OB_FAIL(int_enum(expect_type, params, int_val, out))) {
      LOG_WARN("fail to cast int to enum", K(expect_type), K(in), K(int_val), K(out), K(ret));
    }
  }
  return ret;
}

static int float_set(const ObExpectType &expect_type, ObObjCastParams &params, const ObObj &in, ObObj &out)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObSetType != expect_type.get_type())
      || OB_UNLIKELY(ObFloatTC != in.get_type_class())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(expect_type), K(in), K(ret));
  } else {
    int64_t value = static_cast<int64_t>(in.get_float());
    ObObj int_val(value);
    if (OB_FAIL(int_set(expect_type, params, int_val, out))) {
      LOG_WARN("fail to cast int to enum", K(expect_type), K(in), K(int_val), K(out), K(ret));
    }
  }
  return ret;
}

CAST_TO_LOB_METHOD(float, ObFloatTC);
CAST_TO_TEXT_METHOD(float, ObFloatTC);

static int float_json(const ObObjType expect_type, ObObjCastParams &params,
                      const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObLength res_length = -1;
  if (CM_IS_COLUMN_CONVERT(cast_mode) && is_mysql_unsupported_json_column_conversion(in.get_type())) {
    ret = OB_ERR_INVALID_JSON_TEXT;
    LOG_USER_ERROR(OB_ERR_INVALID_JSON_TEXT);
  } else if (OB_UNLIKELY(ObFloatTC != in.get_type_class() || (ObJsonType != expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if (OB_UNLIKELY(params.allocator_v2_ == NULL)) {
    ret= OB_ERR_UNEXPECTED;
    LOG_ERROR("NULL allocator in json cast function", K(ret), K(in), K(expect_type));
  } else {
    ObJsonDouble j_double(in.get_float());
    ObIJsonBase *j_base = &j_double;
    ObString raw_bin;
    if (OB_FAIL(j_base->get_raw_binary(raw_bin, params.allocator_v2_))) {
      LOG_WARN("fail to get float json binary", K(ret), K(in), K(expect_type), K(*j_base));
    } else if (OB_FAIL(set_json_bin_res(&params, &out, raw_bin))) {
      LOG_WARN("fail to fill json bin lob locator", K(ret));
    } else {
      res_length = out.get_val_len();
    }
  }

  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_STRING, DEFAULT_SCALE_FOR_TEXT, res_length)
  return ret;
}

static int float_geometry(const ObObjType expect_type, ObObjCastParams &params,
                          const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObLength res_length = -1;
  if (OB_ISNULL(params.allocator_v2_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid allocator", K(ret));
  } else {
    ObIAllocator &temp_allocator = *params.allocator_v2_;
    ObGeometry *geo = NULL;
    omt::ObSrsCacheGuard srs_guard;
    const ObSrsItem *srs = NULL;
    ObGeoType dst_geo_type = ObGeoCastUtils::get_geo_type_from_cast_mode(cast_mode);
    const char *cast_name = ObGeometryTypeCastUtil::get_cast_name(dst_geo_type);
    int64_t length = 0;
    char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
    ObScale scale = in.get_scale();
    if (in.get_float() == -INFINITY) {
      length = strlen("-Inf");
      strncpy(buf, "-Inf", length);
    } else if (in.get_float() == INFINITY) {
      length = strlen("Inf");
      strncpy(buf, "Inf", length);
    } else if (isnan(in.get_float())) {
      length = strlen("Nan");
      strncpy(buf, "Nan", length);
    } else {
      if (0 <= scale) {
        length = ob_fcvt(in.get_float(), scale, sizeof(buf) - 1, buf, NULL);
      } else {
        length = ob_gcvt_opt(in.get_float(), OB_GCVT_ARG_FLOAT, sizeof(buf) - 1,
                             buf, NULL, lib::is_oracle_mode(), TRUE);
      }
    }
    ObString str(sizeof(buf), static_cast<int32_t>(length), buf);
    if (OB_FAIL(common_get_srs_item(srs_guard, str, srs, cast_name))) {
      LOG_WARN("fail to get srs item", K(ret), K(str));
    } else if (OB_FAIL(common_build_geometry(temp_allocator, str, geo, srs, cast_name))) {
      LOG_WARN("fail to parse geometry", K(ret), K(dst_geo_type));
    } else {
      ObString res_wkb;
      if (OB_FAIL(ObGeoTypeUtil::to_wkb(temp_allocator, *geo, srs, res_wkb))) {
        LOG_WARN("fail to get wkb", K(ret), K(dst_geo_type));
      } else if (OB_FAIL(set_geo_res(&params, &out, res_wkb))) {
        LOG_WARN("set geo result failed", K(ret));
      } else {
        res_length = static_cast<ObLength>(out.get_string_len());
      }
    }

    if (OB_FAIL(ret) && CM_IS_COLUMN_CONVERT(cast_mode) && ObGeoType::GEOMETRY == dst_geo_type) { // adapt mysql
      ret = OB_ERR_CANT_CREATE_GEOMETRY_OBJECT;
      LOG_USER_ERROR(OB_ERR_CANT_CREATE_GEOMETRY_OBJECT);
    }
  }
  SET_RES_ACCURACY_STRING(expect_type, DEFAULT_PRECISION_FOR_STRING, res_length);
  return ret;
}

////////////////////////////////////////////////////////////////
// Double -> XXX

static int double_int(const ObObjType expect_type, ObObjCastParams &params,
                      const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  int64_t value = 0;
  ObPrecision res_precision = -1;
  if (OB_UNLIKELY(ObDoubleTC != in.get_type_class()
                  || ObIntTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    if (CAST_FAIL(common_double_int(in.get_double(), value, LLONG_MIN, LLONG_MAX))) {
      LOG_WARN("common double to in failed", K(ret), K(in), K(cast_mode));
    } else if (CAST_FAIL(int_range_check(expect_type, value, value))) {
      LOG_WARN("int range check failed", K(ret), K(value));
    } else {
      out.set_int(expect_type, value);
    }
    if (OB_SUCC(ret)) {
      res_precision = get_precision_for_integer(value);
    }
  }
  SET_RES_ACCURACY(res_precision, DEFAULT_SCALE_FOR_INTEGER, DEFAULT_LENGTH_FOR_NUMERIC);
  return ret;
}

static int double_uint(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  uint64_t value = 0;
  ObPrecision res_precision = -1;
  double in_value = in.get_double();
  bool is_column_convert = CM_IS_COLUMN_CONVERT(cast_mode);
  if (OB_UNLIKELY(ObDoubleTC != in.get_type_class()
                  || ObUIntTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    if (in_value <= static_cast<double>(LLONG_MIN)) {
      value = static_cast<uint64_t>(LLONG_MIN);
      ret = OB_DATA_OUT_OF_RANGE;
    } else if (in_value >= static_cast<double>(ULLONG_MAX)) {
      value = static_cast<uint64_t>(LLONG_MAX);
      ret = OB_DATA_OUT_OF_RANGE;
    } else {
      if (is_column_convert) {
        value = static_cast<uint64_t>(rint(in_value));
      // 与float_uint略有不同，[INT64_MAX, UINT64_MAX)之间的double转uint时，在非column_convert的场景中结果
      // 应该是INT64_MAX而不是INT64_MAX + 1.
      } else if (in_value >= static_cast<double>(LLONG_MAX)) {
        value = static_cast<uint64_t>(LLONG_MAX);
      } else {
        value = static_cast<uint64_t>(static_cast<int64_t>(rint(in_value)));
      }
      if (in_value < 0 && value != 0) {
        // 这里处理[LLONG_MIN, 0)范围内的in，转换为unsigned应该报OB_DATA_OUT_OF_RANGE。
        // out不等于0避免[-0.5, 0)内的值被误判，因为它们round后的值是0，处于合法范围内。
        ret = OB_DATA_OUT_OF_RANGE;
      }
    }
    if (CAST_FAIL(ret)) {
      LOG_WARN("cast float to uint failed", K(ret), K(in), K(value));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (CM_NEED_RANGE_CHECK(cast_mode)
      && CAST_FAIL(uint_range_check(expect_type, value, value))) {
  } else {
    out.set_uint(expect_type, value);
  }
  if (OB_SUCC(ret)) {
    res_precision = get_precision_for_integer(value);
  }
  SET_RES_ACCURACY(res_precision, DEFAULT_SCALE_FOR_INTEGER, DEFAULT_LENGTH_FOR_NUMERIC);
  return ret;
}

static int double_float(const ObObjType expect_type, ObObjCastParams &params,
                        const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  float value = static_cast<float>(in.get_double());
  if (OB_UNLIKELY(ObDoubleTC != in.get_type_class()
                  || ObFloatTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (lib::is_mysql_mode()
              && CAST_FAIL(real_range_check(expect_type, in.get_double(), value))) {
  } else {
    out.set_float(expect_type, value);
    LOG_DEBUG("succ to double_float", K(ret), K(in), K(value), K(out));
  }
  SET_RES_ACCURACY(PRECISION_UNKNOWN_YET, SCALE_UNKNOWN_YET, LENGTH_UNKNOWN_YET);
  return ret;
}

static int double_double(const ObObjType expect_type, ObObjCastParams &params,
                         const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  double value = in.get_double();
  if (OB_UNLIKELY(ObDoubleTC != in.get_type_class()
                  || ObDoubleTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (ObUDoubleType == expect_type && CAST_FAIL(numeric_negative_check(value))) {
  } else {
    out.set_double(expect_type, value);
  }
  SET_RES_ACCURACY(PRECISION_UNKNOWN_YET, SCALE_UNKNOWN_YET, LENGTH_UNKNOWN_YET);
  return ret;
}

static int double_number(const ObObjType expect_type, ObObjCastParams &params,
                         const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  double value = in.get_double();
  ObPrecision res_precision = -1;
  ObScale res_scale = -1;
  if (OB_UNLIKELY(ObDoubleTC != in.get_type_class()
                  || ObNumberTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (ObUNumberType == expect_type && CAST_FAIL(numeric_negative_check(value))) {
  } else {
    char buf[MAX_DOUBLE_STRICT_PRINT_SIZE];
    MEMSET(buf, 0, MAX_DOUBLE_STRICT_PRINT_SIZE);
    int64_t length = ob_gcvt_opt(in.get_double(), OB_GCVT_ARG_DOUBLE, static_cast<int32_t>(sizeof(buf) - 1),
                                 buf, NULL, lib::is_oracle_mode(), TRUE);
    ObString str(sizeof(buf), static_cast<int32_t>(length), buf);
    number::ObNumber nmb;
    if (OB_FAIL(nmb.from_sci_opt(str.ptr(), str.length(), params, &res_precision, &res_scale))) {
    } else {
      out.set_number(expect_type, nmb);
    }
    LOG_DEBUG("finish double to number", K(ret), K(str), K(length), K(in), K(expect_type), K(nmb), K(out), K(lbt()));
  }
  SET_RES_ACCURACY(res_precision, res_scale, DEFAULT_LENGTH_FOR_NUMERIC);
  return ret;
}
static int number_datetime(const ObObjType expect_type, ObObjCastParams &params,
                           const ObObj &in, ObObj &out, const ObCastMode cast_mode);
static int double_datetime(const ObObjType expect_type, ObObjCastParams &params,
                           const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  // double to datetime must cast to number first, then cast string to datetime.
  // because double 20151016153421.8 may actually 20151016153421.801 in memory,
  // so we will get '2015-10-16 15:34:21.801' instead of '2015-10-16 15:34:21.8'.
  // this problem can be resolved by cast double to number.
  int ret = OB_SUCCESS;
  ObTimeConvertCtx cvrt_ctx(params.dtc_params_.tz_info_, ObTimestampType == expect_type);
  ObObj tmp_num_obj;
  if (OB_UNLIKELY(ObDoubleTC != in.get_type_class()
                  || ObDateTimeTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(double_number(ObNumberType, params, in, tmp_num_obj, cast_mode))) {
    LOG_WARN("cast double to number failed", K(ret), K(cast_mode));
    if (CM_IS_WARN_ON_FAIL(cast_mode)){
      ret = OB_SUCCESS;
      if (CM_IS_ZERO_ON_WARN(cast_mode)) {
        out.set_datetime(expect_type, ObTimeConverter::ZERO_DATETIME);
      } else {
        out.set_null();
      }
    } else {
      ret = OB_INVALID_DATE_VALUE;
    }
  } else if (CAST_FAIL(number_datetime(expect_type, params, tmp_num_obj, out, cast_mode))) {
    LOG_WARN("number to datetime failed", K(ret));
  }
  return ret;
}

static int double_datetime_interval(const ObObjType expect_type, ObObjCastParams &params,
                                    const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObScale res_scale = -1;
  if (OB_UNLIKELY(ObDoubleTC != in.get_type_class())
      || OB_UNLIKELY(ObDateTimeTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else {
    const int64_t value = static_cast<int64_t>(in.get_double() * static_cast<double>(USECS_PER_DAY));
    LOG_DEBUG("succ to double_datetime_interval", K(ret), K(in), K(value), K(expect_type));
    SET_RES_DATETIME(out);
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, res_scale, DEFAULT_LENGTH_FOR_TEMPORAL);
  return ret;
}

static int double_date(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  //TODO<xiaoni> In datediff,mysql does truncate NOT round
  //So,we have to revise double 2 int which act as round (right now) to truncation
  int ret = OB_SUCCESS;
  ObObj int64;
  if (OB_UNLIKELY(ObDoubleTC != in.get_type_class()
                  || ObDateTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(double_int(ObIntType, params, in, int64, cast_mode))) {
  } else if (OB_FAIL(int_date(expect_type, params, int64, out, cast_mode))) {
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, DEFAULT_SCALE_FOR_DATE, DEFAULT_LENGTH_FOR_TEMPORAL);
  return ret;
}

static int double_time(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  // double to time must cast to string first, then cast string to time.
  // see comment in double_datetime.
  int ret = OB_SUCCESS;
  ObScale res_scale = -1;
  if (OB_UNLIKELY(ObDoubleTC != in.get_type_class()
                  || ObTimeTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    int64_t value = 0;
    char buf[MAX_DOUBLE_PRINT_SIZE];
    MEMSET(buf, 0, MAX_DOUBLE_PRINT_SIZE);
    int64_t length = ob_gcvt(in.get_double(), OB_GCVT_ARG_DOUBLE, sizeof(buf) - 1, buf, NULL);
    ObString str(sizeof(buf), static_cast<int32_t>(length), buf);
    if (CAST_FAIL(ObTimeConverter::str_to_time(str, value, &res_scale))) {
    } else {
      SET_RES_TIME(out);
    }
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, res_scale, DEFAULT_LENGTH_FOR_TEMPORAL);
  return ret;
}

static int double_year(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObDoubleTC != in.get_type_class()
                  || ObYearTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    uint8_t value = 0;
    double in_val = in.get_double();
    in_val = in_val < 0 ? INT_MIN : in_val + 0.5;
    uint64_t intvalue = static_cast<uint64_t>(in_val);
    if (CAST_FAIL(ObTimeConverter::int_to_year(intvalue, value))) {
    } else {
      SET_RES_YEAR(out);
    }
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, DEFAULT_SCALE_FOR_YEAR, DEFAULT_LENGTH_FOR_TEMPORAL);
  return ret;
}

static int double_string(const ObObjType expect_type, ObObjCastParams &params,
                         const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObLength res_length = -1;
  if (OB_UNLIKELY(ObDoubleTC != in.get_type_class()
                  || (ObStringTC != ob_obj_type_class(expect_type)
                      && ObTextTC != ob_obj_type_class(expect_type)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
    ObScale scale = in.get_scale();
    int64_t length = 0;
    if (in.get_double() == -INFINITY) {
      length = strlen("-Inf");
      strncpy(buf, "-Inf", length);
    } else if (in.get_double() == INFINITY) {
      length = strlen("Inf");
      strncpy(buf, "Inf", length);
    } else if (isnan(in.get_double())) {
      length = strlen("Nan");
      strncpy(buf, "Nan", length);
    } else {
      if (0 <= scale) {
        length = ob_fcvt(in.get_double(), scale, sizeof(buf) - 1, buf, NULL);
      } else {
        length = ob_gcvt_opt(in.get_double(), OB_GCVT_ARG_DOUBLE,
                              static_cast<int32_t>(sizeof(buf) - 1), buf, NULL, lib::is_oracle_mode(), TRUE);
      }
    }
    ObString str(sizeof(buf), static_cast<int32_t>(length), buf);
    LOG_DEBUG("finish double_string", K(ret), K(in), K(expect_type), K(str));
    ObObj tmp_out;
    ObString tmp_str;
    if (OB_FAIL(convert_string_collation(str, ObCharset::get_system_collation(),
                                         tmp_str, params.dest_collation_, params))) {
      LOG_WARN("fail to convert string collation", K(ret));
    } else if (OB_FAIL(check_convert_string(expect_type, params, tmp_str, tmp_out))) {
      LOG_WARN("fail to check_convert_string", K(ret), K(in), K(expect_type));
    } else if (OB_FAIL(copy_string(params, expect_type, tmp_out.get_string(), out))) {
    } else {
      res_length = static_cast<ObLength>(out.get_string_len());
      out.set_collation_type(params.dest_collation_);
    }
  }
  SET_RES_ACCURACY_STRING(expect_type, DEFAULT_PRECISION_FOR_STRING, res_length);
  UNUSED(cast_mode);
  return ret;
}

static int double_bit(const ObObjType expect_type, ObObjCastParams &params,
                        const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  UNUSED(cast_mode);
  int ret = OB_SUCCESS;
  uint64_t value = static_cast<uint64_t>(in.get_double());
  int32_t bit_len = 0;
  if (OB_UNLIKELY(ObDoubleTC != in.get_type_class()
                  || ObBitTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(get_bit_len(value, bit_len))) {
    LOG_WARN("fail to get bit len", K(ret), K(value), K(bit_len));
  } else {
    SET_RES_BIT(out);
    SET_RES_ACCURACY(static_cast<ObPrecision>(bit_len), DEFAULT_SCALE_FOR_BIT, DEFAULT_LENGTH_FOR_NUMERIC);
  }
  return ret;
}

static int double_enum(const ObExpectType &expect_type, ObObjCastParams &params, const ObObj &in, ObObj &out)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObEnumType != expect_type.get_type())
      || OB_UNLIKELY(ObDoubleTC != in.get_type_class())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(expect_type), K(in), K(ret));
  } else {
    int64_t value = static_cast<int64_t>(in.get_double());
    ObObj int_val(value);
    if (OB_FAIL(int_enum(expect_type, params, int_val, out))) {
      LOG_WARN("fail to cast int to enum", K(expect_type), K(in), K(int_val), K(out), K(ret));
    }
  }
  return ret;
}

static int double_set(const ObExpectType &expect_type, ObObjCastParams &params, const ObObj &in, ObObj &out)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObSetType != expect_type.get_type())
      || OB_UNLIKELY(ObDoubleTC != in.get_type_class())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(expect_type), K(in), K(ret));
  } else {
    int64_t value = static_cast<int64_t>(in.get_double());
    ObObj int_val(value);
    if (OB_FAIL(int_set(expect_type, params, int_val, out))) {
      LOG_WARN("fail to cast int to enum", K(expect_type), K(in), K(int_val), K(out), K(ret));
    }
  }
  return ret;
}

CAST_TO_LOB_METHOD(double, ObDoubleTC);
CAST_TO_TEXT_METHOD(double, ObDoubleTC);

static int double_json(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObLength res_length = -1;
  if (CM_IS_COLUMN_CONVERT(cast_mode) && is_mysql_unsupported_json_column_conversion(in.get_type())) {
    ret = OB_ERR_INVALID_JSON_TEXT;
    LOG_USER_ERROR(OB_ERR_INVALID_JSON_TEXT);
  } else if (OB_UNLIKELY(ObDoubleTC != in.get_type_class() || (ObJsonType != expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if (OB_UNLIKELY(params.allocator_v2_ == NULL)) {
    ret= OB_ERR_UNEXPECTED;
    LOG_ERROR("NULL allocator in json cast function", K(ret), K(in), K(expect_type));
  } else {
    ObJsonDouble j_double(in.get_double());
    ObIJsonBase *j_base = &j_double;
    ObString raw_bin;
    if (OB_FAIL(j_base->get_raw_binary(raw_bin, params.allocator_v2_))) {
      LOG_WARN("fail to get double json binary", K(ret), K(in), K(expect_type), K(*j_base));
    } else if (OB_FAIL(set_json_bin_res(&params, &out, raw_bin))) {
      LOG_WARN("fail to fill json bin lob locator", K(ret));
    } else {
      res_length = out.get_val_len();
    }
  }

  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_STRING, DEFAULT_SCALE_FOR_TEXT, res_length)
  return ret;
}

static int double_geometry(const ObObjType expect_type, ObObjCastParams &params,
                           const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObLength res_length = -1;
  if (OB_ISNULL(params.allocator_v2_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid allocator", K(ret));
  } else {
    ObIAllocator &temp_allocator = *params.allocator_v2_;
    ObGeometry *geo = NULL;
    omt::ObSrsCacheGuard srs_guard;
    const ObSrsItem *srs = NULL;
    ObGeoType dst_geo_type = ObGeoCastUtils::get_geo_type_from_cast_mode(cast_mode);
    const char *cast_name = ObGeometryTypeCastUtil::get_cast_name(dst_geo_type);
    int64_t length = 0;
    char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
    ObScale scale = in.get_scale();
    if (in.get_double() == -INFINITY) {
      length = strlen("-Inf");
      strncpy(buf, "-Inf", length);
    } else if (in.get_double() == INFINITY) {
      length = strlen("Inf");
      strncpy(buf, "Inf", length);
    } else if (isnan(in.get_double())) {
      length = strlen("Nan");
      strncpy(buf, "Nan", length);
    } else {
      if (0 <= scale) {
        length = ob_fcvt(in.get_double(), scale, sizeof(buf) - 1, buf, NULL);
      } else {
        length = ob_gcvt_opt(in.get_double(), OB_GCVT_ARG_DOUBLE,
                              sizeof(buf) - 1, buf, NULL, lib::is_oracle_mode(), TRUE);
      }
    }
    ObString str(sizeof(buf), static_cast<int32_t>(length), buf);
    if (OB_FAIL(common_get_srs_item(srs_guard, str, srs, cast_name))) {
      LOG_WARN("fail to get srs item", K(ret), K(str));
    } else if (OB_FAIL(common_build_geometry(temp_allocator, str, geo, srs, cast_name))) {
      LOG_WARN("fail to parse geometry", K(ret), K(dst_geo_type));
    } else {
      ObString res_wkb;
      if (OB_FAIL(ObGeoTypeUtil::to_wkb(temp_allocator, *geo, srs, res_wkb))) {
        LOG_WARN("fail to get wkb", K(ret), K(dst_geo_type));
      } else if (OB_FAIL(set_geo_res(&params, &out, res_wkb))) {
        LOG_WARN("set geo result failed", K(ret));
      } else {
        res_length = static_cast<ObLength>(out.get_string_len());
      }
    }

    if (OB_FAIL(ret) && CM_IS_COLUMN_CONVERT(cast_mode) && ObGeoType::GEOMETRY == dst_geo_type) { // adapt mysql
      ret = OB_ERR_CANT_CREATE_GEOMETRY_OBJECT;
      LOG_USER_ERROR(OB_ERR_CANT_CREATE_GEOMETRY_OBJECT);
    }
  }
  SET_RES_ACCURACY_STRING(expect_type, DEFAULT_PRECISION_FOR_STRING, res_length);
  return ret;
}

////////////////////////////////////////////////////////////////
// Number -> XXX

static int string_int(const ObObjType expect_type, ObObjCastParams &params,
                      const ObObj &in, ObObj &out, const ObCastMode cast_mode);
static int number_int(const ObObjType expect_type, ObObjCastParams &params,
                      const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObPrecision res_precision = -1;
  if (OB_UNLIKELY(ObNumberTC != in.get_type_class()
                  || ObIntTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    const number::ObNumber &nmb = in.get_number();
    const char *value = nmb.format();
    if (OB_ISNULL(value)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null pointer",
               K(ret), K(value));
    } else {
      ObObj from;
      from.set_varchar(value, static_cast<ObString::obstr_size_t>(strlen(value)));
      ret = string_int(expect_type, params, from, out,
                       CM_UNSET_STRING_INTEGER_TRUNC(cast_mode));
    }
  }
  if (OB_SUCC(ret)) {
    res_precision = get_precision_for_integer(out.get_int());
  }
  SET_RES_ACCURACY(res_precision, DEFAULT_SCALE_FOR_INTEGER, DEFAULT_LENGTH_FOR_NUMERIC);
  return ret;
}

static int string_uint(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode);
static int number_uint(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObPrecision res_precision = -1;
  if (OB_UNLIKELY(ObNumberTC != in.get_type_class()
                  || ObUIntTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    const number::ObNumber &nmb = in.get_number();
    const char *value = nmb.format();
    if (OB_ISNULL(value)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null pointer",
               K(ret), K(value));
    } else {
      ObObj from;
      from.set_varchar(value, static_cast<ObString::obstr_size_t>(strlen(value)));
      ret = string_uint(expect_type, params, from, out, CM_UNSET_STRING_INTEGER_TRUNC(cast_mode));
    }
  }
  if (OB_SUCC(ret)) {
    res_precision = get_precision_for_integer(out.get_uint64());
  }
  SET_RES_ACCURACY(res_precision, DEFAULT_SCALE_FOR_INTEGER, DEFAULT_LENGTH_FOR_NUMERIC);
  return ret;
}

static int string_float(const ObObjType expect_type, ObObjCastParams &params,
                        const ObObj &in, ObObj &out, const ObCastMode cast_mode);
static int number_float(const ObObjType expect_type, ObObjCastParams &params,
                        const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObNumberTC != in.get_type_class()
                  || ObFloatTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    const char *value = in.get_number().format();
    if (OB_ISNULL(value)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null pointer",
          K(ret), K(value));
    } else {
      ObObj from;
      from.set_varchar(value, static_cast<ObString::obstr_size_t>(strlen(value)));
      ret = string_float(expect_type, params, from, out, cast_mode);
    }
  }
  SET_RES_ACCURACY(PRECISION_UNKNOWN_YET, SCALE_UNKNOWN_YET, LENGTH_UNKNOWN_YET);
  return ret;
}

static int string_double(const ObObjType expect_type, ObObjCastParams &params,
                         const ObObj &in, ObObj &out, const ObCastMode cast_mode);
static int number_double(const ObObjType expect_type, ObObjCastParams &params,
                         const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObNumberTC != in.get_type_class()
                  || ObDoubleTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    const char *value = in.get_number().format();
    if (OB_ISNULL(value)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null pointer", K(ret), K(value));
    } else {
      ObObj from;
      from.set_varchar(value, static_cast<ObString::obstr_size_t>(strlen(value)));
      ret = string_double(expect_type, params, from, out, cast_mode);
    }
  }
  SET_RES_ACCURACY(PRECISION_UNKNOWN_YET, SCALE_UNKNOWN_YET, LENGTH_UNKNOWN_YET);
  return ret;
}

static int number_number(const ObObjType expect_type, ObObjCastParams &params,
                         const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObNumberTC != in.get_type_class()
                  || ObNumberTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    const number::ObNumber &nmb = in.get_number();
    number::ObNumber value;
    if (OB_FAIL(value.from(nmb, params))) {
    } else if (ObUNumberType == expect_type && CAST_FAIL(numeric_negative_check(value))) {
    } else {
      out.set_number(expect_type, value);
    }
  }
  //todo<xiaoni> maybe we can do some dirty work to get better result.
  //for now, just set it to unknown....
  SET_RES_ACCURACY(PRECISION_UNKNOWN_YET, NUMBER_SCALE_UNKNOWN_YET, LENGTH_UNKNOWN_YET);
  return ret;
}

static int string_datetime(const ObObjType expect_type, ObObjCastParams &params,
                           const ObObj &in, ObObj &out, const ObCastMode cast_mode);
static int number_datetime(const ObObjType expect_type, ObObjCastParams &params,
                           const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  const int64_t three_digit_min = 100;
  const int64_t eight_digit_max = 99999999;
  if (OB_UNLIKELY(ObNumberTC != in.get_type_class()
                  || ObDateTimeTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    ObTimeConvertCtx cvrt_ctx(params.dtc_params_.tz_info_, ObTimestampType == expect_type);
    int64_t value = 0;
    int64_t int_part = 0;
    int64_t dec_part = 0;
    if (in.get_number().is_negative()) {
      ret = OB_INVALID_DATE_FORMAT;
      LOG_WARN("invalid date format", K(ret), K(in), K(cast_mode));
    } else if (!in.get_number().is_int_parts_valid_int64(int_part,dec_part)) {
      ret = OB_INVALID_DATE_FORMAT;
      LOG_WARN("invalid date format", K(ret), K(in), K(cast_mode));
    } else if (OB_UNLIKELY(dec_part != 0
              && ((0 == int_part && ObTimestampType == expect_type)
                  || (int_part >= three_digit_min && int_part <= eight_digit_max)))) {
      if (CM_IS_COLUMN_CONVERT(cast_mode) && !CM_IS_WARN_ON_FAIL(cast_mode)) {
        ret = OB_INVALID_DATE_VALUE;
        LOG_WARN("invalid date value", K(ret));
      } else {
        dec_part = 0;
      }
    }
    if (OB_SUCC(ret)) {
      ObDateSqlMode date_sql_mode;
      date_sql_mode.allow_invalid_dates_ = CM_IS_EXPLICIT_CAST(cast_mode)
                                           ? false : CM_IS_ALLOW_INVALID_DATES(cast_mode);
      date_sql_mode.no_zero_date_ = CM_IS_EXPLICIT_CAST(cast_mode)
                                    ? false : CM_IS_NO_ZERO_DATE(cast_mode);
      ret = ObTimeConverter::int_to_datetime(int_part, dec_part, cvrt_ctx, value, date_sql_mode);
      LOG_DEBUG("succ to number_datetime", K(ret), K(in), K(value), K(expect_type), K(int_part), K(dec_part));
    }
    if (CAST_FAIL(ret)) {
    } else {
      SET_RES_DATETIME(out);
    }
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, MIN_SCALE_FOR_TEMPORAL, DEFAULT_LENGTH_FOR_TEMPORAL);
  return ret;
}

static int number_datetime_interval(const ObObjType expect_type, ObObjCastParams &params,
                                    const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObNumberTC != in.get_type_class())
      || OB_UNLIKELY(ObDateTimeTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else {
    int64_t int_part = 0;
    int64_t dec_part = 0;
    if (!in.get_number().is_int_parts_valid_int64(int_part,dec_part)) {
      ret = OB_INVALID_DATE_FORMAT;
      LOG_WARN("invalid date format", K(ret), K(in), K(cast_mode));
    } else {
      const int64_t value = static_cast<int64_t>(int_part * USECS_PER_DAY)
                            + (in.is_negative_number() ? -1  : 1 ) * static_cast<int64_t>(static_cast<double>(dec_part) / NSECS_PER_SEC * static_cast<double>(USECS_PER_DAY));
      LOG_DEBUG("succ to number_datetime_interval", K(ret), K(in), K(value), K(expect_type), K(int_part), K(dec_part));
      SET_RES_DATETIME(out);
    }
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, MIN_SCALE_FOR_TEMPORAL, DEFAULT_LENGTH_FOR_TEMPORAL);
  return ret;
}

static int string_date(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode);
static int number_date(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObObj int64;
  int32_t value = 0;
  if (OB_UNLIKELY(ObNumberTC != in.get_type_class()
                  || ObDateTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    int64_t int_part = 0;
    int64_t dec_part = 0;
    const number::ObNumber nmb = in.get_number();
    if (nmb.is_negative()) {
      ret = OB_INVALID_DATE_VALUE;
      LOG_WARN("invalid date value", K(ret), K(nmb));
    } else if (!nmb.is_int_parts_valid_int64(int_part, dec_part)) {
      ret = OB_INVALID_DATE_VALUE;
      LOG_WARN("invalid date format", K(ret), K(nmb));
    } else {
      ObDateSqlMode date_sql_mode;
      date_sql_mode.allow_invalid_dates_ = CM_IS_EXPLICIT_CAST(cast_mode)
                                           ? false : CM_IS_ALLOW_INVALID_DATES(cast_mode);
      date_sql_mode.no_zero_date_ = CM_IS_EXPLICIT_CAST(cast_mode)
                                    ? false : CM_IS_NO_ZERO_DATE(cast_mode);
      ret = ObTimeConverter::int_to_date(int_part, value, date_sql_mode);
      if (OB_SUCC(ret) && OB_UNLIKELY(dec_part > 0)) {
        if (CM_IS_COLUMN_CONVERT(cast_mode) && !CM_IS_WARN_ON_FAIL(cast_mode)) {
          ret = OB_INVALID_DATE_VALUE;
          LOG_WARN("invalid date value with decimal part", K(ret));
        }
      }
    }
    if (CAST_FAIL(ret)) {
    } else {
      SET_RES_DATE(out);
    }
  }
  return ret;
}

static int string_time(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode);
static int number_time(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObNumberTC != in.get_type_class()
                  || ObTimeTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    const char *value = in.get_number().format();
    if (OB_ISNULL(value)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null pointer",
          K(ret), K(value));
    } else {
      ObObj from;
      from.set_varchar(value, static_cast<ObString::obstr_size_t>(strlen(value)));
      ret = string_time(expect_type, params, from, out, cast_mode);
    }
  }
  //has set accuracy in prev string_time call
  return ret;
}

static int string_year(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode);
static int number_year(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObNumberTC != in.get_type_class()
                  || ObYearTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    const char *value = in.get_number().format();
    if (OB_ISNULL(value)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null pointer",
          K(ret), K(value));
    } else if (in.get_number().is_negative()) {
      uint8_t value = 0;
      if (CAST_FAIL(ObTimeConverter::int_to_year(INT_MIN, value))) {
      } else {
        SET_RES_YEAR(out);
      }
    } else {
      ObObj from;
      from.set_varchar(value, static_cast<ObString::obstr_size_t>(strlen(value)));
      ret = string_year(expect_type, params, from, out, cast_mode);
    }
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, DEFAULT_SCALE_FOR_YEAR, DEFAULT_LENGTH_FOR_TEMPORAL);
  return ret;
}

static int number_string(const ObObjType expect_type, ObObjCastParams &params,
                         const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
  int64_t len = 0;
  ObLength res_length = -1;
  if (OB_UNLIKELY(ObNumberTC != in.get_type_class()
                  || (ObStringTC != ob_obj_type_class(expect_type)
                  && ObTextTC != ob_obj_type_class(expect_type)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (is_oracle_mode() && ob_is_blob(expect_type, params.dest_collation_)) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("cast number to blob not allowed", K(ret));
  } else {
    if (lib::is_oracle_mode() && params.format_number_with_limit_) {
      if (OB_FAIL(in.get_number().format_with_oracle_limit(buf, sizeof(buf), len, in.get_scale()))) {
        LOG_WARN("fail to format", K(ret), K(in.get_number()));
      }
    } else {
      if (OB_FAIL(in.get_number().format(buf, sizeof(buf), len, in.get_scale()))) {
        LOG_WARN("fail to format", K(ret), K(in.get_number()));
      }
    }

    if (OB_SUCC(ret)) {
      ObString tmp_str;
      ObObj tmp_out;
      if (OB_FAIL(convert_string_collation(ObString(len, buf), ObCharset::get_system_collation(),
                                           tmp_str, params.dest_collation_, params))) {
        LOG_WARN("fail to convert string collation", K(ret));
      } else if (OB_FAIL(check_convert_string(expect_type, params, tmp_str, tmp_out))) {
        LOG_WARN("fail to check_convert_string", K(ret), K(in), K(expect_type));
      } else if (OB_FAIL(copy_string(params, expect_type, tmp_out.get_string(), out))) {
      } else {
        res_length = static_cast<ObLength>(out.get_string_len());
        out.set_collation_type(params.dest_collation_);
      }
    }
  }
  UNUSED(cast_mode);
  SET_RES_ACCURACY_STRING(expect_type, DEFAULT_PRECISION_FOR_STRING, res_length);
  return ret;
}

static int number_bit(const ObObjType expect_type, ObObjCastParams &params,
                         const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObObj tmp_val;
  if (OB_UNLIKELY(ObNumberTC != in.get_type_class()
                  || ObBitTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(number_uint(ObUInt64Type, params, in, tmp_val, cast_mode))) {
    LOG_WARN("fail to cast number to uint", K(ret), K(expect_type), K(in), K(tmp_val));
  } else if (OB_FAIL(uint_bit(expect_type, params, tmp_val, out, cast_mode))) {
    LOG_WARN("fail to cast uint to bit", K(ret), K(expect_type), K(out), K(tmp_val));
  }
  return ret;
}

static int number_enum(const ObExpectType &expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out)
{
  int ret = OB_SUCCESS;
  ObObj double_val;
  ObCastMode cast_mode = params.cast_mode_;
  if (OB_UNLIKELY(ObEnumType != expect_type.get_type())
      || OB_UNLIKELY(ObNumberTC != in.get_type_class())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(expect_type), K(in), K(ret));
  } else if (OB_FAIL(number_double(ObDoubleType, params, in, double_val, cast_mode))) {
    LOG_WARN("fail to cast number to double", K(in), K(double_val), K(ret));
  } else if (OB_FAIL(double_enum(expect_type, params, double_val, out))) {
    LOG_WARN("fail to cast double to enum", K(expect_type), K(out), K(double_val), K(ret));
  }
  return ret;
}

static int number_set(const ObExpectType &expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out)
{
  int ret = OB_SUCCESS;
  ObObj double_val;
  ObCastMode cast_mode = params.cast_mode_;
  if (OB_UNLIKELY(ObSetType != expect_type.get_type())
      || OB_UNLIKELY(ObNumberTC != in.get_type_class())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(expect_type), K(in), K(ret));
  } else if (OB_FAIL(number_double(ObDoubleType, params, in, double_val, cast_mode))) {
    LOG_WARN("fail to cast number to double", K(in), K(double_val), K(ret));
  } else if (OB_FAIL(double_set(expect_type, params, double_val, out))) {
    LOG_WARN("fail to cast double to enum", K(expect_type), K(out), K(double_val), K(ret));
  }
  return ret;
}

CAST_TO_LOB_METHOD(number, ObNumberTC);
CAST_TO_TEXT_METHOD(number, ObNumberTC);

static int number_json(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObLength res_length = -1;
  if (CM_IS_COLUMN_CONVERT(cast_mode) && is_mysql_unsupported_json_column_conversion(in.get_type())) {
    ret = OB_ERR_INVALID_JSON_TEXT;
    LOG_USER_ERROR(OB_ERR_INVALID_JSON_TEXT);
  } else if (OB_UNLIKELY(ObNumberTC != in.get_type_class() || (ObJsonType != expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if (OB_UNLIKELY(params.allocator_v2_ == NULL)) {
    ret= OB_ERR_UNEXPECTED;
    LOG_ERROR("NULL allocator in json cast function", K(ret), K(in), K(expect_type));
  } else {
    const number::ObNumber nmb(in.get_number());
    ObJsonDecimal j_dec(nmb, -1, in.get_scale());
    ObIJsonBase *j_base = &j_dec;
    ObString raw_bin;
    if (OB_FAIL(j_base->get_raw_binary(raw_bin, params.allocator_v2_))) {
      LOG_WARN("fail to get decimal json binary", K(ret), K(in), K(expect_type), K(*j_base));
    } else if (OB_FAIL(set_json_bin_res(&params, &out, raw_bin))) {
      LOG_WARN("fail to fill json bin lob locator", K(ret));
    } else {
      res_length = out.get_val_len();
    }
  }

  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_STRING, DEFAULT_SCALE_FOR_TEXT, res_length)
  return ret;
}

static int number_geometry(const ObObjType expect_type, ObObjCastParams &params,
                           const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObLength res_length = -1;
  if (OB_ISNULL(params.allocator_v2_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid allocator", K(ret));
  } else {
    ObIAllocator &temp_allocator = *params.allocator_v2_;
    ObGeometry *geo = NULL;
    omt::ObSrsCacheGuard srs_guard;
    const ObSrsItem *srs = NULL;
    ObGeoType dst_geo_type = ObGeoCastUtils::get_geo_type_from_cast_mode(cast_mode);
    const char *cast_name = ObGeometryTypeCastUtil::get_cast_name(dst_geo_type);
    int64_t len = 0;
    char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
    ObScale scale = in.get_scale();
    if (OB_FAIL(in.get_number().format(buf, sizeof(buf), len, in.get_scale()))) {
      LOG_WARN("fail to format number", K(ret));
    } else if (OB_FAIL(common_get_srs_item(srs_guard, ObString(len, buf), srs, cast_name))) {
      LOG_WARN("fail to get srs item", K(ret));
    } else if (OB_FAIL(common_build_geometry(temp_allocator, ObString(len, buf), geo, srs, cast_name))) {
      LOG_WARN("fail to parse geometry", K(ret), K(dst_geo_type));
    } else {
      ObString res_wkb;
      if (OB_FAIL(ObGeoTypeUtil::to_wkb(temp_allocator, *geo, srs, res_wkb))) {
        LOG_WARN("fail to get wkb", K(ret), K(dst_geo_type));
      } else if (OB_FAIL(set_geo_res(&params, &out, res_wkb))) {
        LOG_WARN("set geo result failed", K(ret));
      } else {
        res_length = static_cast<ObLength>(out.get_string_len());
      }
    }

    if (OB_FAIL(ret) && CM_IS_COLUMN_CONVERT(cast_mode) && ObGeoType::GEOMETRY == dst_geo_type) { // adapt mysql
      ret = OB_ERR_CANT_CREATE_GEOMETRY_OBJECT;
      LOG_USER_ERROR(OB_ERR_CANT_CREATE_GEOMETRY_OBJECT);
    }
  }
  SET_RES_ACCURACY_STRING(expect_type, DEFAULT_PRECISION_FOR_STRING, res_length);
  return ret;
}

////////////////////////////////////////////////////////////
// Datetime -> XXX

static int datetime_int(const ObObjType expect_type, ObObjCastParams &params,
                        const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObPrecision res_precision = -1;
  if (OB_UNLIKELY(ObDateTimeTC != in.get_type_class()
                  || ObIntTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    const ObTimeZoneInfo *tz_info = (ObTimestampType == in.get_type()) ? params.dtc_params_.tz_info_ : NULL;
    int64_t value = 0;
    if (OB_FAIL(ObTimeConverter::datetime_to_int(in.get_datetime(), tz_info, value))) {
    } else if (expect_type < ObIntType && CAST_FAIL(int_range_check(expect_type, value, value))) {
    } else {
      out.set_int(expect_type, value);
      res_precision = get_precision_for_integer(value);
    }
  }
  SET_RES_ACCURACY(res_precision, DEFAULT_SCALE_FOR_INTEGER, DEFAULT_LENGTH_FOR_NUMERIC);
  return ret;
}

static int datetime_uint(const ObObjType expect_type, ObObjCastParams &params,
                         const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObPrecision res_precision = -1;
  if (OB_UNLIKELY(ObDateTimeTC != in.get_type_class()
                  || ObUIntTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    const ObTimeZoneInfo *tz_info = (ObTimestampType == in.get_type()) ? params.dtc_params_.tz_info_ : NULL;
    int64_t int64 = 0;
    if (OB_FAIL(ObTimeConverter::datetime_to_int(in.get_datetime(), tz_info, int64))) {
    } else {
      uint64_t value = static_cast<uint64_t>(int64);
      if (expect_type < ObUInt64Type && CAST_FAIL(uint_range_check(expect_type, int64, value))) {
      } else {
        out.set_uint(expect_type, value);
        res_precision = get_precision_for_integer(value);
      }
    }
  }
  SET_RES_ACCURACY(res_precision, DEFAULT_SCALE_FOR_INTEGER, DEFAULT_LENGTH_FOR_NUMERIC);
  return ret;
}

static int datetime_float(const ObObjType expect_type, ObObjCastParams &params,
                          const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObDateTimeTC != in.get_type_class()
                  || ObFloatTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    const ObTimeZoneInfo *tz_info = (ObTimestampType == in.get_type()) ? params.dtc_params_.tz_info_ : NULL;
    double value = 0.0;
    if (OB_FAIL(ObTimeConverter::datetime_to_double(in.get_datetime(), tz_info, value))) {
    } else {
      // if datetime_to_double return OB_SUCCESS, double value must be in (0, INT64_MAX),
      // so we can static_cast to float without range check.
      out.set_float(expect_type, static_cast<float>(value));
    }
  }
  SET_RES_ACCURACY(PRECISION_UNKNOWN_YET, SCALE_UNKNOWN_YET, LENGTH_UNKNOWN_YET);
  UNUSED(cast_mode);
  return ret;
}

static int datetime_double(const ObObjType expect_type, ObObjCastParams &params,
                           const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObDateTimeTC != in.get_type_class()
                  || ObDoubleTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    const ObTimeZoneInfo *tz_info = (ObTimestampType == in.get_type()) ? params.dtc_params_.tz_info_ : NULL;
    double value = 0.0;
    if (OB_FAIL(ObTimeConverter::datetime_to_double(in.get_datetime(), tz_info, value))) {
    } else {
      out.set_double(expect_type, value);
    }
  }
  SET_RES_ACCURACY(PRECISION_UNKNOWN_YET, SCALE_UNKNOWN_YET, LENGTH_UNKNOWN_YET);
  UNUSED(cast_mode);
  return ret;
}

static int datetime_number(const ObObjType expect_type, ObObjCastParams &params,
                           const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObPrecision res_precision = -1;
  ObScale res_scale = -1;
  if (OB_UNLIKELY(ObDateTimeTC != in.get_type_class()
                  || ObNumberTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    const ObTimeZoneInfo *tz_info = (ObTimestampType == in.get_type()) ? params.dtc_params_.tz_info_ : NULL;
    ObString nls_format;
    char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
    int64_t len = 0;
    number::ObNumber value;
    if (OB_FAIL(ObTimeConverter::datetime_to_str(in.get_datetime(), tz_info,
        nls_format, in.get_scale(), buf, sizeof(buf), len, false))) {
      LOG_WARN("failed to convert datetime to string", K(ret));
    } else if (CAST_FAIL(value.from(buf, len, params, &res_precision, &res_scale))) {
      LOG_WARN("failed to convert string to number", K(ret));
    } else {
      out.set_number(expect_type, value);
    }
  }
  SET_RES_ACCURACY(res_precision, res_scale, DEFAULT_LENGTH_FOR_NUMERIC);
  return ret;
}

static int datetime_datetime(const ObObjType expect_type, ObObjCastParams &params,
                             const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObDateTimeTC != in.get_type_class()
                  || ObDateTimeTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    int64_t value = in.get_datetime();
    if (ObDateTimeType == in.get_type() && ObTimestampType == expect_type) {
      ret = ObTimeConverter::datetime_to_timestamp(in.get_datetime(), params.dtc_params_.tz_info_, value);
      ret = OB_ERR_UNEXPECTED_TZ_TRANSITION == ret ? OB_INVALID_DATE_VALUE : ret;
    } else if (ObTimestampType == in.get_type() && ObDateTimeType == expect_type) {
      ret = ObTimeConverter::timestamp_to_datetime(in.get_datetime(), params.dtc_params_.tz_info_, value);
    }

    if (OB_FAIL(ret)) {
    } else {
      out.set_datetime(expect_type, value);
    }
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, in.get_scale(), DEFAULT_LENGTH_FOR_TEMPORAL);
  UNUSED(cast_mode);
  return ret;
}

static int datetime_date(const ObObjType expect_type, ObObjCastParams &params,
                         const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObDateTimeTC != in.get_type_class()
                  || ObDateTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    const ObTimeZoneInfo *tz_info = (ObTimestampType == in.get_type()) ? params.dtc_params_.tz_info_ : NULL;
    int32_t value = 0;
    if (OB_FAIL(ObTimeConverter::datetime_to_date(in.get_datetime(), tz_info, value))) {
    } else {
      out.set_date(value);
    }
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, DEFAULT_SCALE_FOR_DATE, DEFAULT_LENGTH_FOR_TEMPORAL);
  UNUSED(cast_mode);
  return ret;
}

static int datetime_time(const ObObjType expect_type, ObObjCastParams &params,
                         const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObDateTimeTC != in.get_type_class()
                  || ObTimeTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    const ObTimeZoneInfo *tz_info = (ObTimestampType == in.get_type()) ? params.dtc_params_.tz_info_ : NULL;
    int64_t value = 0;
    if (OB_FAIL(ObTimeConverter::datetime_to_time(in.get_datetime(), tz_info, value))) {
    } else {
      out.set_time(value);
    }
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, in.get_scale(), DEFAULT_LENGTH_FOR_TEMPORAL);
  UNUSED(cast_mode);
  return ret;
}

static int datetime_year(const ObObjType expect_type, ObObjCastParams &params,
                         const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObDateTimeTC != in.get_type_class()
                  || ObYearTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    const ObTimeZoneInfo *tz_info = (ObTimestampType == in.get_type()) ? params.dtc_params_.tz_info_ : NULL;
    uint8_t value = 0;
    if (CAST_FAIL(ObTimeConverter::datetime_to_year(in.get_datetime(), tz_info, value))) {
    } else {
      SET_RES_YEAR(out);
    }
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, DEFAULT_SCALE_FOR_YEAR, DEFAULT_LENGTH_FOR_TEMPORAL);
  UNUSED(cast_mode);
  return ret;
}

static int datetime_string(const ObObjType expect_type, ObObjCastParams &params,
                           const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObLength res_length = -1;
  if (OB_UNLIKELY(ObDateTimeTC != in.get_type_class()
                  || (ObStringTC != ob_obj_type_class(expect_type)
                  && ObTextTC != ob_obj_type_class(expect_type)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    const ObTimeZoneInfo *tz_info = (ObTimestampType == in.get_type()) ? params.dtc_params_.tz_info_ : NULL;
    ObString nls_format;
    if (lib::is_oracle_mode() && !params.dtc_params_.force_use_standard_format_) {
      nls_format = params.dtc_params_.get_nls_format(ObDateTimeType);
    }
    char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
    int64_t len = 0;
    if (OB_FAIL(ObTimeConverter::datetime_to_str(in.get_datetime(), tz_info,
            nls_format, in.get_scale(), buf, sizeof(buf), len))) {
      LOG_WARN("failed to convert datetime to string", K(ret));
    } else {
      out.set_type(expect_type);
      ObString tmp_str;
      ObObj tmp_out;
      if (OB_FAIL(convert_string_collation(ObString(len, buf), ObCharset::get_system_collation(), tmp_str, params.dest_collation_, params))) {
        LOG_WARN("fail to convert string collation", K(ret));
      } else if (OB_FAIL(check_convert_string(expect_type, params, tmp_str, tmp_out))) {
        LOG_WARN("fail to check_convert_string", K(ret), K(in), K(expect_type));
      } else if (OB_FAIL(copy_string(params, expect_type, tmp_out.get_string(), out))) {
      } else {
        res_length = static_cast<ObLength>(out.get_string_len());
        out.set_collation_type(params.dest_collation_);
      }
    }
  }
  SET_RES_ACCURACY_STRING(expect_type, DEFAULT_PRECISION_FOR_STRING, res_length);
  UNUSED(cast_mode);
  return ret;
}

static int string_bit(const ObObjType expect_type, ObObjCastParams &params,
                      const ObObj &in, ObObj &out, const ObCastMode cast_mode);
static int datetime_bit(const ObObjType expect_type, ObObjCastParams &params,
                        const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObObj tmp_val;
  if (OB_UNLIKELY(ObDateTimeTC != in.get_type_class()
                  || ObBitTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(datetime_string(ObVarcharType, params, in, tmp_val, cast_mode))) {
    LOG_WARN("fail to cast datetime to string", K(ret), K(expect_type), K(in), K(tmp_val));
  } else if (OB_FAIL(string_bit(expect_type, params, tmp_val, out, cast_mode))) {
    LOG_WARN("fail to cast string to bit", K(ret), K(expect_type), K(in), K(tmp_val));
  }
  return ret;
}


static int string_enum(const ObExpectType &expect_type, ObObjCastParams &params, const ObObj &in, ObObj &out);
static int datetime_enum(const ObExpectType &expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out)
{
  int ret = OB_SUCCESS;
  ObObj str_val;
  ObCastMode cast_mode = params.cast_mode_;
  if (OB_UNLIKELY(ObEnumType != expect_type.get_type())
      || OB_UNLIKELY(ObDateTimeTC != in.get_type_class())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(expect_type), K(in), K(ret));
  } else if (OB_FAIL(datetime_string(ObVarcharType, params, in, str_val, cast_mode))) {
    LOG_WARN("fail to cast number to double", K(in), K(str_val), K(ret));
  } else if (OB_FAIL(string_enum(expect_type, params, str_val, out))) {
    LOG_WARN("fail to cast double to enum", K(expect_type), K(out), K(str_val), K(ret));
  }
  return ret;
}
static int string_set(const ObExpectType &expect_type, ObObjCastParams &params, const ObObj &in, ObObj &out);
static int datetime_set(const ObExpectType &expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out)
{
  int ret = OB_SUCCESS;
  ObObj str_val;
  ObCastMode cast_mode = params.cast_mode_;
  if (OB_UNLIKELY(ObSetType != expect_type.get_type())
      || OB_UNLIKELY(ObDateTimeTC != in.get_type_class())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(expect_type), K(in), K(ret));
  } else if (OB_FAIL(datetime_string(ObVarcharType, params, in, str_val, cast_mode))) {
    LOG_WARN("fail to cast number to double", K(in), K(str_val), K(ret));
  } else if (OB_FAIL(string_set(expect_type, params, str_val, out))) {
    LOG_WARN("fail to cast double to enum", K(expect_type), K(out), K(str_val), K(ret));
  }
  return ret;
}

static int datetime_otimestamp(const ObObjType expect_type, ObObjCastParams &params,
    const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObDateTimeTC != in.get_type_class())
      || OB_UNLIKELY(ObOTimestampTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else {
    int64_t dt_value = 0;
    if (ObTimestampType == in.get_type()) {
      int64_t utc_value = in.get_timestamp();
      if (OB_FAIL(ObTimeConverter::timestamp_to_datetime(utc_value,
                                                         params.dtc_params_.tz_info_,
                                                         dt_value))) {
        LOG_WARN("failed to convert timestamp to datetime", K(ret));
      }
    } else {
      dt_value = in.get_datetime();
    }
    if (OB_SUCC(ret)) {
      int64_t odate_value = 0;
      ObOTimestampData value;
      ObTimeConverter::datetime_to_odate(dt_value, odate_value);
      if (OB_FAIL(ObTimeConverter::odate_to_otimestamp(odate_value,
                                                       params.dtc_params_.tz_info_,
                                                       expect_type, value))) {
        LOG_WARN("fail to timestamp_to_timestamp_tz", K(ret), K(in), K(expect_type));
      } else {
        SET_RES_OTIMESTAMP(out);
      }
    }
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, in.get_scale(), DEFAULT_LENGTH_FOR_TEMPORAL);
  UNUSED(cast_mode);
  return ret;
}

CAST_TO_LOB_METHOD(datetime, ObDateTimeTC);
CAST_TO_TEXT_METHOD(datetime, ObDateTimeTC);

static int datetime_json(const ObObjType expect_type, ObObjCastParams &params,
                         const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObLength res_length = -1;
  if (CM_IS_COLUMN_CONVERT(cast_mode) && is_mysql_unsupported_json_column_conversion(in.get_type())) {
    ret = OB_ERR_INVALID_JSON_TEXT;
    LOG_USER_ERROR(OB_ERR_INVALID_JSON_TEXT);
  } else if (OB_UNLIKELY(ObDateTimeTC != in.get_type_class() || (ObJsonType != expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if (OB_UNLIKELY(params.allocator_v2_ == NULL)) {
    ret= OB_ERR_UNEXPECTED;
    LOG_ERROR("NULL allocator in json cast function", K(ret), K(in), K(expect_type));
  } else {
    int64_t in_val = in.get_datetime();
    ObTime ob_time(DT_TYPE_DATETIME);
    const ObTimeZoneInfo *tz_info = (ObTimestampType == in.get_type()) ?
                                    params.dtc_params_.tz_info_ : NULL;
    if (OB_FAIL(ObTimeConverter::datetime_to_ob_time(in_val, tz_info, ob_time))) {
      LOG_WARN("fail to create datetime from int failed", K(ret), K(in));
    } else {
      ObJsonNodeType j_type = (ObTimestampType == in.get_type()) 
                                  ? ObJsonNodeType::J_TIMESTAMP
                                  : ObJsonNodeType::J_DATETIME;
      ObJsonDatetime j_datetime(j_type, ob_time);
      ObIJsonBase *j_base = &j_datetime;
      ObString raw_bin;
      if (OB_FAIL(j_base->get_raw_binary(raw_bin, params.allocator_v2_))) {
        LOG_WARN("fail to get datetime json binary", K(ret), K(in), K(expect_type), K(*j_base));
      } else if (OB_FAIL(set_json_bin_res(&params, &out, raw_bin))) {
        LOG_WARN("fail to fill json bin lob locator", K(ret));
      } else {
        res_length = out.get_val_len();
      }
    }
  }

  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_STRING, DEFAULT_SCALE_FOR_TEXT, res_length)
  return ret;
}

static int datetime_geometry(const ObObjType expect_type, ObObjCastParams &params,
                             const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObLength res_length = -1;
  if (OB_ISNULL(params.allocator_v2_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid allocator", K(ret));
  } else {
    ObIAllocator &temp_allocator = *params.allocator_v2_;
    ObGeometry *geo = NULL;
    omt::ObSrsCacheGuard srs_guard;
    const ObSrsItem *srs = NULL;
    ObGeoType dst_geo_type = ObGeoCastUtils::get_geo_type_from_cast_mode(cast_mode);
    const char *cast_name = ObGeometryTypeCastUtil::get_cast_name(dst_geo_type);
    const ObTimeZoneInfo *tz_info = (ObTimestampType == in.get_type()) ? params.dtc_params_.tz_info_ : NULL;
    ObString nls_format;
    char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
    int64_t len = 0;
    if (OB_FAIL(ObTimeConverter::datetime_to_str(in.get_datetime(), tz_info, nls_format,
        in.get_scale(), buf, sizeof(buf), len))) {
      LOG_WARN("failed to convert datetime to string", K(ret));
    } else if (OB_FAIL(common_get_srs_item(srs_guard, ObString(len, buf), srs, cast_name))) {
      LOG_WARN("fail to get srs item", K(ret));
    } else if (OB_FAIL(common_build_geometry(temp_allocator, ObString(len, buf), geo, srs, cast_name))) {
      LOG_WARN("fail to parse geometry", K(ret), K(dst_geo_type));
    } else {
      ObString res_wkb;
      if (OB_FAIL(ObGeoTypeUtil::to_wkb(temp_allocator, *geo, srs, res_wkb))) {
        LOG_WARN("fail to get wkb", K(ret), K(dst_geo_type));
      } else if (OB_FAIL(set_geo_res(&params, &out, res_wkb))) {
        LOG_WARN("set geo result failed", K(ret));
      } else {
        res_length = static_cast<ObLength>(out.get_string_len());
      }
    }

    if (OB_FAIL(ret) && CM_IS_COLUMN_CONVERT(cast_mode) && ObGeoType::GEOMETRY == dst_geo_type) { // adapt mysql
      ret = OB_ERR_CANT_CREATE_GEOMETRY_OBJECT;
      LOG_USER_ERROR(OB_ERR_CANT_CREATE_GEOMETRY_OBJECT);
    }
  }
  SET_RES_ACCURACY_STRING(expect_type, DEFAULT_PRECISION_FOR_STRING, res_length);
  return ret;
}

////////////////////////////////////////////////////////////
// Date -> XXX

static int date_int(const ObObjType expect_type, ObObjCastParams &params,
                    const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  int64_t value = 0;
  ObPrecision res_precision = -1;
  if (OB_UNLIKELY(ObDateTC != in.get_type_class()
                  || ObIntTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(ObTimeConverter::date_to_int(in.get_date(), value))) {
  } else if (expect_type < ObInt32Type && CAST_FAIL(int_range_check(expect_type, value, value))) {
  } else {
    out.set_int(expect_type, value);
    res_precision = get_precision_for_integer(value);
  }
  SET_RES_ACCURACY(res_precision, DEFAULT_SCALE_FOR_INTEGER, DEFAULT_LENGTH_FOR_NUMERIC);
  return ret;
}

static int date_uint(const ObObjType expect_type, ObObjCastParams &params,
                     const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObPrecision res_precision = -1;
  int64_t int64 = 0;
  uint64_t value = 0;
  if (OB_UNLIKELY(ObDateTC != in.get_type_class()
                  || ObUIntTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(ObTimeConverter::date_to_int(in.get_date(), int64))) {
  } else {
    value = static_cast<uint64_t>(int64);
    if (expect_type < ObUInt32Type && CAST_FAIL(uint_range_check(expect_type, int64, value))) {
    } else {
      out.set_uint(expect_type, value);
      res_precision = get_precision_for_integer(value);
    }
  }
  SET_RES_ACCURACY(res_precision, DEFAULT_SCALE_FOR_INTEGER, DEFAULT_LENGTH_FOR_NUMERIC);
  return ret;
}

static int date_float(const ObObjType expect_type, ObObjCastParams &params,
                      const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  int64_t value = 0;
  if (OB_UNLIKELY(ObDateTC != in.get_type_class()
                  || ObFloatTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(ObTimeConverter::date_to_int(in.get_date(), value))) {
  } else {
    out.set_float(expect_type, static_cast<float>(value));
  }
  SET_RES_ACCURACY(PRECISION_UNKNOWN_YET, SCALE_UNKNOWN_YET, LENGTH_UNKNOWN_YET);
  UNUSED(cast_mode);
  return ret;
}

static int date_double(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  int64_t value = 0;
  if (OB_UNLIKELY(ObDateTC != in.get_type_class()
                  || ObDoubleTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(ObTimeConverter::date_to_int(in.get_date(), value))) {
  } else {
    out.set_double(expect_type, static_cast<double>(value));
  }
  SET_RES_ACCURACY(PRECISION_UNKNOWN_YET, SCALE_UNKNOWN_YET, LENGTH_UNKNOWN_YET);
  UNUSED(cast_mode);
  return ret;
}

static int date_number(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObObj obj_int;
  if (OB_UNLIKELY(ObDateTC != in.get_type_class()
                  || ObNumberTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(date_int(ObIntType, params, in, obj_int, cast_mode))) {
  } else if (OB_FAIL(int_number(expect_type, params, obj_int, out, cast_mode))) {
  }
  //has set accuracy in prev int_number
  return ret;
}

static int date_datetime(const ObObjType expect_type, ObObjCastParams &params,
                         const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObDateTC != in.get_type_class()
                  || ObDateTimeTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    ObTimeConvertCtx cvrt_ctx(params.dtc_params_.tz_info_, ObTimestampType == expect_type);
    int64_t value = 0;
    if (OB_FAIL(ObTimeConverter::date_to_datetime(in.get_date(), cvrt_ctx, value))) {
    } else {
      out.set_datetime(expect_type, value);
    }
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, MIN_SCALE_FOR_TEMPORAL, DEFAULT_LENGTH_FOR_TEMPORAL);
  UNUSED(cast_mode);
  return ret;
}

static int date_time(const ObObjType expect_type, ObObjCastParams &params,
                     const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObDateTC != in.get_type_class()
                  || ObTimeTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    out.set_time(ObTimeConverter::ZERO_TIME);
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, MIN_SCALE_FOR_TEMPORAL, DEFAULT_LENGTH_FOR_TEMPORAL);
  UNUSED(cast_mode);
  return OB_SUCCESS;
}

static int date_year(const ObObjType expect_type, ObObjCastParams &params,
                     const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  uint8_t value = 0;
  if (OB_UNLIKELY(ObDateTC != in.get_type_class()
                  || ObYearTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (CAST_FAIL(ObTimeConverter::date_to_year(in.get_date(), value))) {
  } else {
    SET_RES_YEAR(out);
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, DEFAULT_SCALE_FOR_YEAR, DEFAULT_LENGTH_FOR_TEMPORAL);
  UNUSED(cast_mode);
  return ret;
}

static int date_string(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
  int64_t len = 0;
  ObLength res_length = -1;
  if (OB_UNLIKELY(ObDateTC != in.get_type_class()
                  || (ObStringTC != ob_obj_type_class(expect_type)
                  && ObTextTC != ob_obj_type_class(expect_type)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(ObTimeConverter::date_to_str(in.get_date(), buf, sizeof(buf), len))) {
  } else {
    out.set_type(expect_type);
    ret = copy_string(params, expect_type, buf, len, out);
    if (OB_SUCC(ret)) {
      res_length = static_cast<ObLength>(out.get_string_len());
      out.set_collation_type(params.dest_collation_);
    }
  }
  SET_RES_ACCURACY_STRING(expect_type, DEFAULT_PRECISION_FOR_STRING, res_length);
  UNUSED(cast_mode);
  return ret;
}

static int date_bit(const ObObjType expect_type, ObObjCastParams &params,
                        const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObObj tmp_val;
  if (OB_UNLIKELY(ObDateTC != in.get_type_class()
                  || ObBitTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(date_string(ObVarcharType, params, in, tmp_val, cast_mode))) {
    LOG_WARN("fail to cast datetime to string", K(ret), K(expect_type), K(in), K(tmp_val));
  } else if (OB_FAIL(string_bit(expect_type, params, tmp_val, out, cast_mode))) {
    LOG_WARN("fail to cast string to bit", K(ret), K(expect_type), K(in), K(tmp_val));
  }
  return ret;
}

static int date_enum(const ObExpectType &expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out)
{
  int ret = OB_SUCCESS;
  ObObj str_val;
  ObCastMode cast_mode = params.cast_mode_;
  if (OB_UNLIKELY(ObEnumType != expect_type.get_type())
      || OB_UNLIKELY(ObDateTC != in.get_type_class())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(expect_type), K(in), K(ret));
  } else if (OB_FAIL(date_string(ObVarcharType, params, in, str_val, cast_mode))) {
    LOG_WARN("fail to cast number to double", K(in), K(str_val), K(ret));
  } else if (OB_FAIL(string_enum(expect_type, params, str_val, out))) {
    LOG_WARN("fail to cast double to enum", K(expect_type), K(out), K(str_val), K(ret));
  }
  return ret;
}

static int date_set(const ObExpectType &expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out)
{
  int ret = OB_SUCCESS;
  ObObj str_val;
  ObCastMode cast_mode = params.cast_mode_;
  if (OB_UNLIKELY(ObSetType != expect_type.get_type())
      || OB_UNLIKELY(ObDateTC != in.get_type_class())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(expect_type), K(in), K(ret));
  } else if (OB_FAIL(date_string(ObVarcharType, params, in, str_val, cast_mode))) {
    LOG_WARN("fail to cast number to double", K(in), K(str_val), K(ret));
  } else if (OB_FAIL(string_set(expect_type, params, str_val, out))) {
    LOG_WARN("fail to cast double to enum", K(expect_type), K(out), K(str_val), K(ret));
  }
  return ret;
}

CAST_TO_LOB_METHOD(date, ObDateTC);
CAST_TO_TEXT_METHOD(date, ObDateTC);

static int date_json(const ObObjType expect_type, ObObjCastParams &params,
                     const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObLength res_length = -1;
  if (CM_IS_COLUMN_CONVERT(cast_mode) && is_mysql_unsupported_json_column_conversion(in.get_type())) {
    ret = OB_ERR_INVALID_JSON_TEXT;
    LOG_USER_ERROR(OB_ERR_INVALID_JSON_TEXT);
  } else if (OB_UNLIKELY(ObDateTC != in.get_type_class() || (ObJsonType != expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if (OB_UNLIKELY(params.allocator_v2_ == NULL)) {
    ret= OB_ERR_UNEXPECTED;
    LOG_ERROR("NULL allocator in json cast function", K(ret), K(in), K(expect_type));
  } else {
    int32_t in_val = in.get_date();
    ObTime ob_time(DT_TYPE_DATE);
    if (OB_FAIL(ObTimeConverter::date_to_ob_time(in_val, ob_time))) {
      LOG_WARN("fail to create datetime from int failed", K(ret), K(in));
    } else {
      ObJsonDatetime j_date(ObJsonNodeType::J_DATE, ob_time);
      ObIJsonBase *j_base = &j_date;
      ObString raw_bin;
      if (OB_FAIL(j_base->get_raw_binary(raw_bin, params.allocator_v2_))) {
        LOG_WARN("fail to get date json binary", K(ret), K(in), K(expect_type), K(*j_base));
      } else if (OB_FAIL(set_json_bin_res(&params, &out, raw_bin))) {
        LOG_WARN("fail to fill json bin lob locator", K(ret));
      } else {
        res_length = out.get_val_len();
      }
    }
  }

  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_STRING, DEFAULT_SCALE_FOR_TEXT, res_length)
  return ret;
}

static int date_geometry(const ObObjType expect_type, ObObjCastParams &params,
                        const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObLength res_length = -1;
  if (OB_ISNULL(params.allocator_v2_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid allocator", K(ret));
  } else {
    ObIAllocator &temp_allocator = *params.allocator_v2_;
    ObGeometry *geo = NULL;
    omt::ObSrsCacheGuard srs_guard;
    const ObSrsItem *srs = NULL;
    ObGeoType dst_geo_type = ObGeoCastUtils::get_geo_type_from_cast_mode(cast_mode);
    const char *cast_name = ObGeometryTypeCastUtil::get_cast_name(dst_geo_type);
    char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
    int64_t len = 0;
    if ((OB_FAIL(ObTimeConverter::date_to_str(in.get_date(), buf, sizeof(buf), len)))) {
      LOG_WARN("failed to convert datetime to string", K(ret));
    } else if (OB_FAIL(common_get_srs_item(srs_guard, ObString(len, buf), srs, cast_name))) {
      LOG_WARN("fail to get srs item", K(ret));
    } else if (OB_FAIL(common_build_geometry(temp_allocator, ObString(len, buf), geo, srs, cast_name))) {
      LOG_WARN("fail to parse geometry", K(ret), K(dst_geo_type));
    } else {
      ObString res_wkb;
      if (OB_FAIL(ObGeoTypeUtil::to_wkb(temp_allocator, *geo, srs, res_wkb))) {
        LOG_WARN("fail to get wkb", K(ret), K(dst_geo_type));
      } else if (OB_FAIL(set_geo_res(&params, &out, res_wkb))) {
        LOG_WARN("set geo result failed", K(ret));
      } else {
        res_length = static_cast<ObLength>(out.get_string_len());
      }
    }

    if (OB_FAIL(ret) && CM_IS_COLUMN_CONVERT(cast_mode) && ObGeoType::GEOMETRY == dst_geo_type) { // adapt mysql
      ret = OB_ERR_CANT_CREATE_GEOMETRY_OBJECT;
      LOG_USER_ERROR(OB_ERR_CANT_CREATE_GEOMETRY_OBJECT);
    }
  }
  SET_RES_ACCURACY_STRING(expect_type, DEFAULT_PRECISION_FOR_STRING, res_length);
  return ret;
}

////////////////////////////////////////////////////////////
// Time -> XXX

static int time_int(const ObObjType expect_type, ObObjCastParams &params,
                    const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  int64_t value = 0;
  ObPrecision res_precision = -1;
  if (OB_UNLIKELY(ObTimeTC != in.get_type_class()
                  || ObIntTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(ObTimeConverter::time_to_int(in.get_time(), value))) {
  } else if (expect_type < ObInt32Type && CAST_FAIL(int_range_check(expect_type, value, value))) {
  } else {
    out.set_int(expect_type, value);
    res_precision = get_precision_for_integer(value);
  }
  SET_RES_ACCURACY(res_precision, DEFAULT_SCALE_FOR_INTEGER, DEFAULT_LENGTH_FOR_NUMERIC);
  return ret;
}

static int time_uint(const ObObjType expect_type, ObObjCastParams &params,
                     const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  int64_t int64 = 0;
  uint64_t value = 0;
  ObPrecision res_precision = -1;
  if (OB_UNLIKELY(ObTimeTC != in.get_type_class()
                  || ObUIntTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(ObTimeConverter::time_to_int(in.get_time(), int64))) {
  } else {
    value = static_cast<uint64_t>(int64);
    if (CM_NEED_RANGE_CHECK(cast_mode) && CAST_FAIL(uint_range_check(expect_type, int64, value))) {
    } else {
      out.set_uint(expect_type, value);
      res_precision = get_precision_for_integer(value);
    }
  }
  SET_RES_ACCURACY(res_precision, DEFAULT_SCALE_FOR_INTEGER, DEFAULT_LENGTH_FOR_NUMERIC);
  return ret;
}

static int time_float(const ObObjType expect_type, ObObjCastParams &params,
                      const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  double value = 0.0;
  if (OB_UNLIKELY(ObTimeTC != in.get_type_class()
                  || ObFloatTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(ObTimeConverter::time_to_double(in.get_time(), value))) {
  } else if (ObUFloatType == expect_type && CAST_FAIL(numeric_negative_check(value))) {
  } else {
    // if time_to_double return OB_SUCCESS, double value must be in (INT64_MIN, INT64_MAX),
    // so we can static_cast to float without range check.
    out.set_float(expect_type, static_cast<float>(value));
  }
  SET_RES_ACCURACY(PRECISION_UNKNOWN_YET, SCALE_UNKNOWN_YET, LENGTH_UNKNOWN_YET);
  return ret;
}

static int time_double(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  double value = 0.0;
  if (OB_UNLIKELY(ObTimeTC != in.get_type_class()
                  || ObDoubleTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(ObTimeConverter::time_to_double(in.get_time(), value))) {
  } else if (ObUDoubleType == expect_type && CAST_FAIL(numeric_negative_check(value))) {
  } else {
    out.set_double(expect_type, value);
  }
  SET_RES_ACCURACY(PRECISION_UNKNOWN_YET, SCALE_UNKNOWN_YET, LENGTH_UNKNOWN_YET);
  UNUSED(cast_mode);
  return ret;
}

static int time_number(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
  int64_t len = 0;
  number::ObNumber value;
  ObPrecision res_precision = -1;
  ObScale res_scale = -1;
  if (OB_UNLIKELY(ObTimeTC != in.get_type_class()
                  || ObNumberTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(ObTimeConverter::time_to_str(in.get_time(), in.get_scale(), buf, sizeof(buf), len, false))) {
  } else if (CAST_FAIL(value.from(buf, len, params, &res_precision, &res_scale))) {
  } else {
    out.set_number(expect_type, value);
  }
  SET_RES_ACCURACY(res_precision, res_scale, DEFAULT_LENGTH_FOR_NUMERIC);
  return ret;
}

static int time_datetime(const ObObjType expect_type, ObObjCastParams &params,
                         const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  const ObTimeZoneInfo *tz_info = params.dtc_params_.tz_info_;
  int64_t value = 0;
  if (OB_UNLIKELY(ObTimeTC != in.get_type_class()
                  || ObDateTimeTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(ObTimeConverter::time_to_datetime(in.get_time(), params.cur_time_, tz_info, value, expect_type))) {
  } else {
    out.set_datetime(expect_type, value);
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, in.get_scale(), DEFAULT_LENGTH_FOR_TEMPORAL);
  UNUSED(cast_mode);
  return ret;
}

static int time_date(const ObObjType expect_type, ObObjCastParams &params,
                         const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  const ObTimeZoneInfo *tz_info = params.dtc_params_.tz_info_;
  int32_t value = 0;
  int64_t datetime_value = 0;
  if (OB_UNLIKELY(ObTimeTC != in.get_type_class()
                  || ObDateTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    ObTimeConvertCtx cvrt_ctx(tz_info, false);
    if (OB_FAIL(ObTimeConverter::time_to_datetime(in.get_time(), params.cur_time_, tz_info,
                                                  datetime_value, ObDateTimeType))) {
      LOG_WARN("time to datetime failed", K(ret), K(in), K(params.cur_time_));
    } else if (OB_FAIL(ObTimeConverter::datetime_to_date(datetime_value, NULL, value))) {
      LOG_WARN("date to datetime failed", K(ret), K(datetime_value));
    } else {
      out.set_date(value);
    }
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, DEFAULT_SCALE_FOR_DATE, DEFAULT_LENGTH_FOR_TEMPORAL);
  UNUSED(cast_mode);
  return ret;
}

static int time_year(const ObObjType expect_type, ObObjCastParams &params,
                         const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  int64_t int_value = 0;
  uint8_t value = 0;
  if (OB_UNLIKELY(ObTimeTC != in.get_type_class()
                  || ObYearTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(ObTimeConverter::time_to_int(in.get_time(), int_value))) {
    LOG_WARN("time to int failed", K(ret), K(in));
  } else if (CAST_FAIL(ObTimeConverter::int_to_year(int_value, value))) {
    LOG_WARN("cast int to year failed", K(ret), K(int_value));
  } else {
    SET_RES_YEAR(out);
  }
  SET_RES_ACCURACY(DEFAULT_SCALE_FOR_YEAR, DEFAULT_SCALE_FOR_INTEGER, DEFAULT_LENGTH_FOR_NUMERIC);
  return ret;
}

static int time_string(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
  int64_t len = 0;
  ObLength res_length = -1;
  if (OB_UNLIKELY(ObTimeTC != in.get_type_class()
                  || (ObStringTC != ob_obj_type_class(expect_type)
                  && ObTextTC != ob_obj_type_class(expect_type)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(ObTimeConverter::time_to_str(in.get_time(), in.get_scale(), buf, sizeof(buf), len))) {
  } else {
    out.set_type(expect_type);
    ret = copy_string(params, expect_type, buf, len, out);
    if (OB_SUCC(ret)) {
      res_length = static_cast<ObLength>(out.get_string_len());
      out.set_collation_type(params.dest_collation_);
    }
  }
  SET_RES_ACCURACY_STRING(expect_type, DEFAULT_PRECISION_FOR_STRING, res_length);
  UNUSED(cast_mode);
  return ret;
}

static int time_bit(const ObObjType expect_type, ObObjCastParams &params,
                        const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObObj tmp_val;
  if (OB_UNLIKELY(ObTimeTC != in.get_type_class()
                  || ObBitTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(time_string(ObVarcharType, params, in, tmp_val, cast_mode))) {
    LOG_WARN("fail to cast datetime to string", K(ret), K(expect_type), K(in), K(tmp_val));
  } else if (OB_FAIL(string_bit(expect_type, params, tmp_val, out, cast_mode))) {
    LOG_WARN("fail to cast string to bit", K(ret), K(expect_type), K(in), K(tmp_val));
  }
  return ret;
}

static int time_enum(const ObExpectType &expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out)
{
  int ret = OB_SUCCESS;
  ObObj str_val;
  ObCastMode cast_mode = params.cast_mode_;
  if (OB_UNLIKELY(ObEnumType != expect_type.get_type())
      || OB_UNLIKELY(ObTimeTC != in.get_type_class())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(expect_type), K(in), K(ret));
  } else if (OB_FAIL(time_string(ObVarcharType, params, in, str_val, cast_mode))) {
    LOG_WARN("fail to cast number to double", K(in), K(str_val), K(ret));
  } else if (OB_FAIL(string_enum(expect_type, params, str_val, out))) {
    LOG_WARN("fail to cast double to enum", K(expect_type), K(out), K(str_val), K(ret));
  }
  return ret;
}

static int time_set(const ObExpectType &expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out)
{
  int ret = OB_SUCCESS;
  ObObj str_val;
  ObCastMode cast_mode = params.cast_mode_;
  if (OB_UNLIKELY(ObSetType != expect_type.get_type())
      || OB_UNLIKELY(ObTimeTC != in.get_type_class())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(expect_type), K(in), K(ret));
  } else if (OB_FAIL(time_string(ObVarcharType, params, in, str_val, cast_mode))) {
    LOG_WARN("fail to cast number to double", K(in), K(str_val), K(ret));
  } else if (OB_FAIL(string_set(expect_type, params, str_val, out))) {
    LOG_WARN("fail to cast double to enum", K(expect_type), K(out), K(str_val), K(ret));
  }
  return ret;
}

CAST_TO_LOB_METHOD(time, ObTimeTC);
CAST_TO_TEXT_METHOD(time, ObTimeTC);

static int time_json(const ObObjType expect_type, ObObjCastParams &params,
                     const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObLength res_length = -1;
  if (CM_IS_COLUMN_CONVERT(cast_mode) && is_mysql_unsupported_json_column_conversion(in.get_type())) {
    ret = OB_ERR_INVALID_JSON_TEXT;
    LOG_USER_ERROR(OB_ERR_INVALID_JSON_TEXT);
  } else if (OB_UNLIKELY(ObTimeTC != in.get_type_class() || (ObJsonType != expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if (OB_UNLIKELY(params.allocator_v2_ == NULL)) {
    ret= OB_ERR_UNEXPECTED;
    LOG_ERROR("NULL allocator in json cast function", K(ret), K(in), K(expect_type));
  } else {
    int64_t in_val = in.get_int();
    ObTime ob_time(DT_TYPE_TIME);
    if (OB_FAIL(ObTimeConverter::time_to_ob_time(in_val, ob_time))) {
      LOG_WARN("fail to create datetime from int failed", K(ret), K(in));
    } else {
      ObJsonDatetime j_time(ObJsonNodeType::J_TIME, ob_time);
      ObIJsonBase *j_base = &j_time;
      ObString raw_bin;
      if (OB_FAIL(j_base->get_raw_binary(raw_bin, params.allocator_v2_))) {
        LOG_WARN("fail to get time json binary", K(ret), K(in), K(expect_type), K(*j_base));
      } else if (OB_FAIL(set_json_bin_res(&params, &out, raw_bin))) {
        LOG_WARN("fail to fill json bin lob locator", K(ret));
      } else {
        res_length = out.get_val_len();
      }
    }
  }

  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_STRING, DEFAULT_SCALE_FOR_TEXT, res_length)
  return ret;
}

static int time_geometry(const ObObjType expect_type, ObObjCastParams &params,
                         const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObLength res_length = -1;
  if (OB_ISNULL(params.allocator_v2_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid allocator", K(ret));
  } else {
    ObIAllocator &temp_allocator = *params.allocator_v2_;
    ObGeometry *geo = NULL;
    omt::ObSrsCacheGuard srs_guard;
    const ObSrsItem *srs = NULL;
    ObGeoType dst_geo_type = ObGeoCastUtils::get_geo_type_from_cast_mode(cast_mode);
    const char *cast_name = ObGeometryTypeCastUtil::get_cast_name(dst_geo_type);
    char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
    int64_t len = 0;
    if (OB_FAIL(ObTimeConverter::time_to_str(in.get_time(), in.get_scale(), buf, sizeof(buf), len))) {
      LOG_WARN("fail to time to str", K(ret));
    } else if (OB_FAIL(common_get_srs_item(srs_guard, ObString(len, buf), srs, cast_name))) {
      LOG_WARN("fail to get srs item", K(ret));
    } else if (OB_FAIL(common_build_geometry(temp_allocator, ObString(len, buf), geo, srs, cast_name))) {
      LOG_WARN("fail to parse geometry", K(ret), K(dst_geo_type));
    } else {
      ObString res_wkb;
      if (OB_FAIL(ObGeoTypeUtil::to_wkb(temp_allocator, *geo, srs, res_wkb))) {
        LOG_WARN("fail to get wkb", K(ret), K(dst_geo_type));
      } else if (OB_FAIL(set_geo_res(&params, &out, res_wkb))) {
        LOG_WARN("set geo result failed", K(ret));
      } else {
        res_length = static_cast<ObLength>(out.get_string_len());
      }
    }

    if (OB_FAIL(ret) && CM_IS_COLUMN_CONVERT(cast_mode) && ObGeoType::GEOMETRY == dst_geo_type) { // adapt mysql
      ret = OB_ERR_CANT_CREATE_GEOMETRY_OBJECT;
      LOG_USER_ERROR(OB_ERR_CANT_CREATE_GEOMETRY_OBJECT);
    }
  }
  SET_RES_ACCURACY_STRING(expect_type, DEFAULT_PRECISION_FOR_STRING, res_length);
  return ret;
}

////////////////////////////////////////////////////////////
// Year -> XXX

static int year_int(const ObObjType expect_type, ObObjCastParams &params,
                    const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  int64_t value = 0;
  ObPrecision res_precision = -1;
  if (OB_UNLIKELY(ObYearTC != in.get_type_class()
                  || ObIntTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(ObTimeConverter::year_to_int(in.get_year(), value))) {
  } else if (expect_type < ObSmallIntType && CAST_FAIL(int_range_check(expect_type, value, value))) {
  } else {
    out.set_int(expect_type, value);
    res_precision = get_precision_for_integer(value);
  }
  SET_RES_ACCURACY(res_precision, DEFAULT_SCALE_FOR_INTEGER, DEFAULT_LENGTH_FOR_NUMERIC);
  return ret;
}

static int year_uint(const ObObjType expect_type, ObObjCastParams &params,
                     const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  int64_t int64 = 0;
  uint64_t value = 0;
  ObPrecision res_precision = -1;
  if (OB_UNLIKELY(ObYearTC != in.get_type_class()
                  || ObUIntTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(ObTimeConverter::year_to_int(in.get_year(), int64))) {
  } else {
    value = static_cast<uint64_t>(int64);
    if (expect_type < ObUSmallIntType && CAST_FAIL(uint_range_check(expect_type, int64, value))) {
    } else {
      out.set_uint(expect_type, value);
      res_precision = get_precision_for_integer(value);
    }
  }
  SET_RES_ACCURACY(res_precision, DEFAULT_SCALE_FOR_INTEGER, DEFAULT_LENGTH_FOR_NUMERIC);
  return ret;
}

static int year_float(const ObObjType expect_type, ObObjCastParams &params,
                      const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  int64_t value = 0;
  if (OB_UNLIKELY(ObYearTC != in.get_type_class()
                  || ObFloatTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(ObTimeConverter::year_to_int(in.get_year(), value))) {
  } else {
    out.set_float(expect_type, static_cast<float>(value));
  }
  SET_RES_ACCURACY(PRECISION_UNKNOWN_YET, SCALE_UNKNOWN_YET, LENGTH_UNKNOWN_YET);
  UNUSED(cast_mode);
  return ret;
}

static int year_double(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  int64_t value = 0;
  if (OB_UNLIKELY(ObYearTC != in.get_type_class()
                  || ObDoubleTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(ObTimeConverter::year_to_int(in.get_year(), value))) {
  } else {
    out.set_double(expect_type, static_cast<double>(value));
  }
  SET_RES_ACCURACY(PRECISION_UNKNOWN_YET, SCALE_UNKNOWN_YET, LENGTH_UNKNOWN_YET);
  UNUSED(cast_mode);
  return ret;
}

static int year_number(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObObj obj_int;
  if (OB_UNLIKELY(ObYearTC != in.get_type_class()
                  || ObNumberTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(year_int(ObIntType, params, in, obj_int, cast_mode))) {
  } else if (OB_FAIL(int_number(expect_type, params, obj_int, out, cast_mode))) {
  }
  //has set accuracy in prev int_number
  return ret;
}

static int year_datetime(const ObObjType expect_type, ObObjCastParams &params,
                         const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  int64_t int_value = 0;
  ObObj int64;
  if (OB_UNLIKELY(ObYearTC != in.get_type_class()
                  || ObDateTimeTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(ObTimeConverter::year_to_int(in.get_year(), int_value))) {
    LOG_WARN("year to int failed", K(ret));
  } else if (FALSE_IT(int64.set_int(int_value))) {
  } else if (OB_FAIL(int_datetime(expect_type, params, int64, out, cast_mode))) {
    LOG_WARN("int_datetime failed", K(ret));
  }
  return ret;
}

static int year_date(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  int64_t in_value = 0;
  if (OB_UNLIKELY(ObYearTC != in.get_type_class()
                  || ObDateTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(ObTimeConverter::year_to_int(in.get_year(), in_value))) {
  } else {
    int32_t value = 0;
    if (CAST_FAIL(ObTimeConverter::int_to_date(in_value, value, false))) {
    } else {
      SET_RES_DATE(out);
    }
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, DEFAULT_SCALE_FOR_DATE, DEFAULT_LENGTH_FOR_TEMPORAL);
  return ret;
}

static int year_time(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObYearTC != in.get_type_class()
                  || ObTimeTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    int64_t value = 0;
    int64_t year_int = 0;
    if (OB_FAIL(ObTimeConverter::year_to_int(in.get_year(), year_int))) {
      LOG_WARN("year_to_int failed", K(ret), K(in));
    } else if (CAST_FAIL(ObTimeConverter::int_to_time(year_int, value))) {
      LOG_WARN("int to time failed", K(ret));
    } else {
      SET_RES_TIME(out);
    }
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, MIN_SCALE_FOR_TEMPORAL, DEFAULT_LENGTH_FOR_TEMPORAL);
  return ret;
}

static int year_string(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
  int64_t len = 0;
  ObLength res_length = -1;
  if (OB_UNLIKELY(ObYearTC != in.get_type_class()
                  || (ObStringTC != ob_obj_type_class(expect_type)
                  && ObTextTC != ob_obj_type_class(expect_type)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(ObTimeConverter::year_to_str(in.get_year(), buf, sizeof(buf), len))) {
  } else {
    out.set_type(expect_type);
    ret = copy_string(params, expect_type, buf, len, out);
    if (OB_SUCC(ret)) {
      res_length = static_cast<ObLength>(out.get_string_len());
      out.set_collation_type(params.dest_collation_);
    }
  }
  SET_RES_ACCURACY_STRING(expect_type, DEFAULT_PRECISION_FOR_STRING, res_length);
  UNUSED(cast_mode);
  return ret;
}

static int year_bit(const ObObjType expect_type, ObObjCastParams &params,
                        const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObObj tmp_val;
  if (OB_UNLIKELY(ObYearTC != in.get_type_class()
                  || ObBitTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(year_int(ObIntType, params, in, tmp_val, cast_mode))) {
    LOG_WARN("fail to cast datetime to int", K(ret), K(expect_type), K(in), K(tmp_val));
  } else if (OB_FAIL(int_bit(expect_type, params, tmp_val, out, cast_mode))) {
    LOG_WARN("fail to cast int to bit", K(ret), K(expect_type), K(in), K(tmp_val));
  }
  return ret;
}

static int year_enum(const ObExpectType &expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out)
{
  int ret = OB_SUCCESS;
  ObObj uint_val;
  ObCastMode cast_mode = params.cast_mode_;
  if (OB_UNLIKELY(ObEnumType != expect_type.get_type())
      || OB_UNLIKELY(ObYearTC != in.get_type_class())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(expect_type), K(in), K(ret));
  } else if (OB_FAIL(year_uint(ObUInt64Type, params, in, uint_val, cast_mode))) {
    LOG_WARN("fail to cast number to double", K(in), K(uint_val), K(ret));
  } else if (OB_FAIL(uint_enum(expect_type, params, uint_val, out))) {
    LOG_WARN("fail to cast double to enum", K(expect_type), K(out), K(uint_val), K(ret));
  }
  return ret;
}

static int year_set(const ObExpectType &expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out)
{
  int ret = OB_SUCCESS;
  ObObj uint_val;
  ObCastMode cast_mode = params.cast_mode_;
  if (OB_UNLIKELY(ObSetType != expect_type.get_type())
      || OB_UNLIKELY(ObYearTC != in.get_type_class())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(expect_type), K(in), K(ret));
  } else if (OB_FAIL(year_uint(ObUInt64Type, params, in, uint_val, cast_mode))) {
    LOG_WARN("fail to cast number to double", K(in), K(uint_val), K(ret));
  } else if (OB_FAIL(uint_set(expect_type, params, uint_val, out))) {
    LOG_WARN("fail to cast double to enum", K(expect_type), K(out), K(uint_val), K(ret));
  }
  return ret;
}

CAST_TO_LOB_METHOD(year, ObYearTC);
CAST_TO_TEXT_METHOD(year, ObYearTC);

static int year_json(const ObObjType expect_type, ObObjCastParams &params,
                     const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObLength res_length = -1;
  int64_t full_year = 0;
  if (CM_IS_COLUMN_CONVERT(cast_mode) && is_mysql_unsupported_json_column_conversion(in.get_type())) {
    ret = OB_ERR_INVALID_JSON_TEXT;
    LOG_USER_ERROR(OB_ERR_INVALID_JSON_TEXT);
  } else if (OB_UNLIKELY(ObYearTC != in.get_type_class() || (ObJsonType != expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if (OB_UNLIKELY(params.allocator_v2_ == NULL)) {
    ret= OB_ERR_UNEXPECTED;
    LOG_ERROR("NULL allocator in json cast function", K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(ObTimeConverter::year_to_int(in.get_year(), full_year))) {
    LOG_WARN("convert year to int failed in year to json convert", K(ret), K(in.get_year()));
  } else {
    ObJsonInt j_year(full_year);
    ObIJsonBase *j_base = &j_year;
    ObString raw_bin;
    if (OB_FAIL(j_base->get_raw_binary(raw_bin, params.allocator_v2_))) {
      LOG_WARN("fail to get year json binary", K(ret), K(in), K(expect_type), K(*j_base));
    } else if (OB_FAIL(set_json_bin_res(&params, &out, raw_bin))) {
      LOG_WARN("fail to fill json bin lob locator", K(ret));
    } else {
      res_length = out.get_val_len();
    }
  }

  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_STRING, DEFAULT_SCALE_FOR_TEXT, res_length)
  return ret;
}

static int year_geometry(const ObObjType expect_type, ObObjCastParams &params,
                         const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObLength res_length = -1;
  if (OB_ISNULL(params.allocator_v2_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid allocator", K(ret));
  } else {
    ObIAllocator &temp_allocator = *params.allocator_v2_;
    ObGeometry *geo = NULL;
    omt::ObSrsCacheGuard srs_guard;
    const ObSrsItem *srs = NULL;
    ObGeoType dst_geo_type = ObGeoCastUtils::get_geo_type_from_cast_mode(cast_mode);
    const char *cast_name = ObGeometryTypeCastUtil::get_cast_name(dst_geo_type);
    char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
    int64_t len = 0;
    if (OB_FAIL(ObTimeConverter::year_to_str(in.get_year(), buf, sizeof(buf), len))) {
      LOG_WARN("fail to year to str", K(ret));
    } else if (OB_FAIL(common_get_srs_item(srs_guard, ObString(len, buf), srs, cast_name))) {
      LOG_WARN("fail to get srs item", K(ret));
    } else if (OB_FAIL(common_build_geometry(temp_allocator, ObString(len, buf), geo, srs, cast_name))) {
      LOG_WARN("fail to parse geometry", K(ret), K(dst_geo_type));
    } else {
      ObString res_wkb;
      if (OB_FAIL(ObGeoTypeUtil::to_wkb(temp_allocator, *geo, srs, res_wkb))) {
        LOG_WARN("fail to get wkb", K(ret), K(dst_geo_type));
      } else if (OB_FAIL(set_geo_res(&params, &out, res_wkb))) {
        LOG_WARN("set geo result failed", K(ret));
      } else {
        res_length = static_cast<ObLength>(out.get_string_len());
      }
    }

    if (OB_FAIL(ret) && CM_IS_COLUMN_CONVERT(cast_mode) && ObGeoType::GEOMETRY == dst_geo_type) { // adapt mysql
      ret = OB_ERR_CANT_CREATE_GEOMETRY_OBJECT;
      LOG_USER_ERROR(OB_ERR_CANT_CREATE_GEOMETRY_OBJECT);
    }
  }
  SET_RES_ACCURACY_STRING(expect_type, DEFAULT_PRECISION_FOR_STRING, res_length);
  return ret;
}

////////////////////////////////////////////////////////////
// String -> XXX

int common_string_unsigned_integer(const ObCastMode &cast_mode,
                                   const ObObjType &in_type,
                                   const ObCollationType &in_cs_type,
                                   const ObString &in_str,
                                   const bool is_str_integer_cast,
                                   uint64_t &out_val)
{
  int ret = OB_SUCCESS;
  if (ObHexStringType == in_type) {
    out_val = hex_to_uint64(in_str);
  } else {
    int err = 0;
    char *endptr = NULL;
    if (is_str_integer_cast && CM_IS_STRING_INTEGER_TRUNC(cast_mode)) {
      out_val = ObCharset::strntoull(in_str.ptr(), in_str.length(), 10, &endptr, &err);
      if (ERANGE == err && (INT64_MIN == out_val || INT64_MAX == out_val)) {
        ret = OB_DATA_OUT_OF_RANGE;
      } else if (endptr == in_str.ptr() || endptr != in_str.ptr() + in_str.length()) {
        ret = OB_ERR_TRUNCATED_WRONG_VALUE;
        if (CM_IS_WARN_ON_FAIL(cast_mode)) {
          ObString tmp_msg("INTEGER");
          LOG_USER_WARN(OB_ERR_TRUNCATED_WRONG_VALUE, tmp_msg.length(), tmp_msg.ptr(),
                        in_str.length(), in_str.ptr());
        }
      }
    } else {
      out_val = ObCharset::strntoullrnd(in_str.ptr(), in_str.length(), true, &endptr, &err);
      if (ERANGE == err && (0 == out_val || UINT64_MAX == out_val)) {
        ret = OB_DATA_OUT_OF_RANGE;
      } else {
        ret = check_convert_str_err(in_str.ptr(), endptr, in_str.length(), err, in_cs_type);
      }
    }
  }
  return ret;
}

// 与MySQL有不兼容行为:
int common_string_integer(const ObCastMode &cast_mode,
                                 const ObObjType &in_type,
                                 const ObCollationType &in_cs_type,
                                 const ObString &in_str,
                                 const bool is_str_integer_cast,
                                 int64_t &out_val)
{
  int ret = OB_SUCCESS;
  if (ObHexStringType == in_type) {
    ObObj uint64_in;
    ObObj out;
    ObObjCastParams dummy;
    ObCastMode uint_int_cm = cast_mode;
    uint64_in.set_uint64(hex_to_uint64(in_str));
    if (CM_IS_EXPLICIT_CAST(cast_mode)) {
      uint_int_cm |= CM_NO_RANGE_CHECK;
    }
    if (OB_FAIL(uint_int(ObIntType, dummy, uint64_in, out, uint_int_cm))) {
      LOG_WARN("convert hex string to int64 failed", K(ret));
    } else {
      out_val = out.get_int();
    }
  } else {
    int err = 0;
    char *endptr = NULL;
    if (is_str_integer_cast && CM_IS_STRING_INTEGER_TRUNC(cast_mode)) {
      out_val = ObCharset::strntoll(in_str.ptr(), in_str.length(), 10, &endptr, &err);
      if (ERANGE == err && (INT64_MIN == out_val || INT64_MAX == out_val)) {
        ret = OB_DATA_OUT_OF_RANGE;
      } else if (endptr == in_str.ptr() || endptr != in_str.ptr() + in_str.length()) {
        ret = OB_ERR_TRUNCATED_WRONG_VALUE;
        if (CM_IS_WARN_ON_FAIL(cast_mode)) {
          ObString tmp_msg("INTEGER");
          LOG_USER_WARN(OB_ERR_TRUNCATED_WRONG_VALUE, tmp_msg.length(), tmp_msg.ptr(),
                        in_str.length(), in_str.ptr());
        }
      }
    } else {
      out_val = static_cast<int64_t>(ObCharset::strntoullrnd(in_str.ptr(), in_str.length(),
                                                             false, &endptr, &err));
      if (ERANGE == err && (INT64_MIN == out_val || INT64_MAX == out_val)) {
        ret = OB_DATA_OUT_OF_RANGE;
      } else {
        ret = check_convert_str_err(in_str.ptr(), endptr, in_str.length(), err, in_cs_type);
      }
    }
  }
  return ret;
}

static int string_int(const ObObjType expect_type, ObObjCastParams &params,
                      const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObString utf8_string;

  ObPrecision res_precision = -1;
  if (OB_UNLIKELY((ObStringTC != in.get_type_class()
                  && ObTextTC != in.get_type_class()
                  && ObGeometryTC != in.get_type_class())
                  || ObIntTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (lib::is_oracle_mode() && in.is_blob()) {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("invalid use of blob type", K(ret), K(in), K(expect_type));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Cast to blob type");
  } else if (OB_FAIL(convert_string_collation(in.get_string(), in.get_collation_type(), utf8_string, ObCharset::get_system_collation(), params))) {
      LOG_WARN("convert_string_collation", K(ret));
  } else {
    const ObString &str = utf8_string;
    int64_t value = 0;
    const bool is_str_int_cast = true;
    if (CAST_FAIL(common_string_integer(
                cast_mode, in.get_type(), in.get_collation_type(), str, is_str_int_cast, value))) {
    } else if (expect_type < ObIntType && CAST_FAIL(int_range_check(expect_type, value, value))) {
    } else {
      SET_RES_INT(out);
    }
  }
  if (OB_SUCC(ret)) {
    res_precision = get_precision_for_integer(out.get_int());
  }
  SET_RES_ACCURACY(res_precision, DEFAULT_SCALE_FOR_INTEGER, DEFAULT_LENGTH_FOR_NUMERIC);
  return ret;
}

static int string_uint(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObPrecision res_precision = -1;
  if (OB_UNLIKELY((ObStringTC != in.get_type_class()
                  && ObTextTC != in.get_type_class()
                  && ObGeometryTC != in.get_type_class())
                  || ObUIntTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (lib::is_oracle_mode() && in.is_blob()) {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("invalid use of blob type", K(ret), K(in), K(expect_type));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Cast to blob type");
  } else {
    const ObString &str = in.get_string();
    uint64_t value = 0;
    const bool is_str_int_cast = true;
    if (CAST_FAIL(common_string_unsigned_integer(
                cast_mode, in.get_type(), in.get_collation_type(), str, is_str_int_cast, value))) {
    } else if (expect_type < ObUInt64Type && CM_NEED_RANGE_CHECK(cast_mode)
               && CAST_FAIL(uint_upper_check(expect_type, value))) {
    } else {
      SET_RES_UINT(out);
    }
  }
  if (OB_SUCC(ret)) {
    res_precision = get_precision_for_integer(out.get_uint64());
  }
  SET_RES_ACCURACY(res_precision, DEFAULT_SCALE_FOR_INTEGER, DEFAULT_LENGTH_FOR_NUMERIC);
  return ret;
}

static int string_float(const ObObjType expect_type, ObObjCastParams &params,
                        const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObObj dbl;
  if (OB_UNLIKELY((ObStringTC != in.get_type_class()
                  && ObTextTC != in.get_type_class()
                  && ObGeometryTC != in.get_type_class())
                  || ObFloatTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (lib::is_oracle_mode() && in.is_blob()) {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("invalid use of blob type", K(ret), K(in), K(expect_type));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Cast to blob type");
  } else if (OB_FAIL(string_double(ObDoubleType, params, in, dbl, cast_mode))) {
  } else if (OB_FAIL(double_float(expect_type, params, dbl, out, cast_mode))) {
  }
  //has set accuracy in prev double_float call
  return ret;
}

static int string_double(const ObObjType expect_type, ObObjCastParams &params,
                         const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY((ObStringTC != in.get_type_class()
                  && ObTextTC != in.get_type_class()
                  && ObGeometryTC != in.get_type_class())
                  || ObDoubleTC != ob_obj_type_class(expect_type))) {
     ret = OB_ERR_UNEXPECTED;
     LOG_ERROR("invalid input type",
         K(ret), K(in), K(expect_type));
  } else if (lib::is_oracle_mode() && in.is_blob()) {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("invalid use of blob type", K(ret), K(in), K(expect_type));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Cast to blob type");
  } else {
    double value = 0.0;
    if (ObHexStringType == in.get_type()) {
      value = static_cast<double>(hex_to_uint64(in.get_string()));
    } else {
      int err = 0;
      char *endptr = NULL;
      ObString str_utf8;
      if (OB_FAIL(convert_string_collation(in.get_string(), in.get_collation_type(), str_utf8,
                               ObCharset::get_system_collation(), params))) {
        LOG_WARN("convert_string_collation", K(ret), K(str_utf8), K(in.get_string()));
      } else {
        value = ObCharset::strntodv2(str_utf8.ptr(), str_utf8.length(), &endptr, &err);
        if (EOVERFLOW == err && (-DBL_MAX == value || DBL_MAX == value)) {
          ret = OB_DATA_OUT_OF_RANGE;
        } else {
          ObString trimed_str = str_utf8.trim();
          if (lib::is_mysql_mode() && 0 == trimed_str.length()) {
            if (!CM_IS_COLUMN_CONVERT(cast_mode)) {
              // mysql 模式下不在 convert_column 里遇到空字符串或者全是空格的字符串转 double 时，不报错
              // skip
            } else {
              ret = OB_ERR_DOUBLE_TRUNCATED;
              LOG_WARN("convert string to double failed", K(ret), K(str_utf8));
            }
          } else if (OB_SUCCESS != (ret = check_convert_str_err(
                                          str_utf8.ptr(), endptr, str_utf8.length(), err, in.get_collation_type()))) {
            LOG_WARN("failed to check_convert_str_err", K(ret), K(str_utf8), K(value), K(err), K(in.get_collation_type()));
            if (lib::is_mysql_mode() && CM_IS_COLUMN_CONVERT(cast_mode) && ret == OB_ERR_DATA_TRUNCATED) {
              // do nothing, compatible mysql, retain OB_ERR_DATA_TRUNCATED error code in column_convert.
            } else {
              ret = OB_ERR_DOUBLE_TRUNCATED;
            }
            if (CM_IS_WARN_ON_FAIL(cast_mode)) {
              ret = OB_ERR_DOUBLE_TRUNCATED;
              LOG_USER_WARN(OB_ERR_DOUBLE_TRUNCATED, str_utf8.length(), str_utf8.ptr());
            }
          }
        }
      }
    }

    if (CAST_FAIL(ret)) {
    } else if (ObUDoubleType == expect_type && CAST_FAIL(numeric_negative_check(value))) {
    } else {
      SET_RES_DOUBLE(out);
    }
  }
  SET_RES_ACCURACY(PRECISION_UNKNOWN_YET, SCALE_UNKNOWN_YET, LENGTH_UNKNOWN_YET);
  return ret;
}

static int string_number(const ObObjType expect_type, ObObjCastParams &params,
                         const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObPrecision res_precision = PRECISION_UNKNOWN_YET;
  ObScale res_scale = NUMBER_SCALE_UNKNOWN_YET;
  ObString utf8_string;

  if (OB_ISNULL(params.allocator_v2_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid allocator", K(ret));
  } else if (OB_UNLIKELY((ObStringTC != in.get_type_class()
                          && ObTextTC != in.get_type_class()
                          && ObGeometryTC != in.get_type_class())
                          || ObNumberTC != ob_obj_type_class(expect_type))) {
     ret = OB_ERR_UNEXPECTED;
     LOG_ERROR("invalid input type",
         K(ret), K(in), K(expect_type));
  } else if (lib::is_oracle_mode() && in.is_blob()) {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("invalid use of blob type", K(ret), K(in), K(expect_type));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Cast to blob type");
  } else {
    number::ObNumber value;
    if (ObHexStringType == in.get_type()) {
      ret = value.from(hex_to_uint64(in.get_string()), params);
    } else if (OB_UNLIKELY(0 == in.get_string().length())) {
      value.set_zero();
      ret = OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD;
    } else if (OB_FAIL(convert_string_collation(in.get_string(), in.get_collation_type(), utf8_string, ObCharset::get_system_collation(), params))) {
      LOG_WARN("convert_string_collation", K(ret));
    } else {
      const ObString &str = utf8_string;
      ret = value.from_sci_opt(str.ptr(), str.length(), params, &res_precision, &res_scale);
      // bug: 4263211. 兼容mysql string转number超过最值域范围的行为
      // select cast('1e500' as decimal);  -> max_val
      // select cast('-1e500' as decimal); -> min_val
      if (ret == OB_NUMERIC_OVERFLOW) {
        int64_t i = 0;
        while (i < str.length() && isspace(str[i])) {
          ++i;
        }
        bool is_neg = (str[i] == '-');
        int tmp_ret = OB_SUCCESS;
        const ObAccuracy &def_acc = ObAccuracy::DDL_DEFAULT_ACCURACY2[0][expect_type];
        const ObPrecision prec = def_acc.get_precision();
        const ObScale scale = def_acc.get_scale();
        const ObNumber *bound_num = NULL;
        if (is_neg) {
          bound_num = &(ObNumberConstValue::MYSQL_MIN[prec][scale]);
        } else {
          bound_num = &(ObNumberConstValue::MYSQL_MAX[prec][scale]);
        }
        if (OB_SUCCESS != (tmp_ret = value.from(*bound_num, *params.allocator_v2_))) {
          LOG_WARN("copy min number failed", K(ret), K(tmp_ret), KPC(bound_num));
        }
      }
    }
    if (CAST_FAIL(ret)) {
      LOG_WARN("string_number failed", K(ret), K(in), K(expect_type), K(cast_mode));
    } else if (ObUNumberType == expect_type && CAST_FAIL(numeric_negative_check(value))) {
      LOG_WARN("numeric_negative_check failed", K(ret), K(in), K(cast_mode));
    } else {
      out.set_number(expect_type, value);
    }
  }
  SET_RES_ACCURACY(res_precision, res_scale, DEFAULT_LENGTH_FOR_NUMERIC);
  return ret;
}

static int string_datetime(const ObObjType expect_type, ObObjCastParams &params,
                           const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObScale res_scale = -1;
  ObString utf8_string;

  if (OB_UNLIKELY((ObStringTC != in.get_type_class()
                  && ObTextTC != in.get_type_class()
                  && ObGeometryTC != in.get_type_class())
                  || ObDateTimeTC != ob_obj_type_class(expect_type))) {
     ret = OB_ERR_UNEXPECTED;
     LOG_ERROR("invalid input type",
         K(ret), K(in), K(expect_type));
  } else if (lib::is_oracle_mode() && in.is_blob()) {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("invalid use of blob type", K(ret), K(in), K(expect_type));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Cast to blob type");
  } else if (OB_FAIL(convert_string_collation(in.get_string(), in.get_collation_type(), utf8_string, ObCharset::get_system_collation(), params))) {
    LOG_WARN("convert_string_collation", K(ret));
  } else {
    int64_t value = 0;
    bool need_truncate = CM_IS_COLUMN_CONVERT(cast_mode) ? CM_IS_TIME_TRUNCATE_FRACTIONAL(cast_mode) : false;
    ObTimeConvertCtx cvrt_ctx(params.dtc_params_.tz_info_, ObTimestampType == expect_type, need_truncate);
    if (lib::is_oracle_mode()) {
      cvrt_ctx.oracle_nls_format_ = params.dtc_params_.get_nls_format(ObDateTimeType);
      CAST_RET(ObTimeConverter::str_to_date_oracle(utf8_string, cvrt_ctx, value));
    } else {
      ObDateSqlMode date_sql_mode;
      date_sql_mode.allow_invalid_dates_ = CM_IS_ALLOW_INVALID_DATES(cast_mode);
      date_sql_mode.no_zero_date_ = CM_IS_NO_ZERO_DATE(cast_mode);
      CAST_RET(ObTimeConverter::str_to_datetime(utf8_string, cvrt_ctx, value, &res_scale,
                                                date_sql_mode));
    }

    // check zero date for scale over mode
    if (CM_IS_ERROR_ON_SCALE_OVER(cast_mode) &&
        (value == ObTimeConverter::ZERO_DATE ||
        value == ObTimeConverter::ZERO_DATETIME)) {
      ret = OB_INVALID_DATE_VALUE;
      LOG_USER_ERROR(OB_INVALID_DATE_VALUE, utf8_string.length(), utf8_string.ptr(), "");
    }

    if (OB_SUCC(ret)) {
      SET_RES_DATETIME(out);
    }
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, res_scale, DEFAULT_LENGTH_FOR_TEMPORAL);
  return ret;
}

static int string_date(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  int32_t value = 0;
  ObDateSqlMode date_sql_mode;
  date_sql_mode.allow_invalid_dates_ = CM_IS_ALLOW_INVALID_DATES(cast_mode);
  date_sql_mode.no_zero_date_ = CM_IS_NO_ZERO_DATE(cast_mode);
  if (OB_UNLIKELY((ObStringTC != in.get_type_class()
                  && ObTextTC != in.get_type_class()
                  && ObGeometryTC != in.get_type_class())
                  || ObDateTC != ob_obj_type_class(expect_type))) {
     ret = OB_ERR_UNEXPECTED;
     LOG_ERROR("invalid input type",
         K(ret), K(in), K(expect_type));
  } else if (lib::is_oracle_mode() && in.is_blob()) {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("invalid use of blob type", K(ret), K(in), K(expect_type));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Cast to blob type");
  } else if (CAST_FAIL(ObTimeConverter::str_to_date(in.get_string(), value, date_sql_mode))) {
  } else if (CM_IS_ERROR_ON_SCALE_OVER(cast_mode) && value == ObTimeConverter::ZERO_DATE) {
    // check zero date for scale over mode
    ret = OB_INVALID_DATE_VALUE;
    LOG_USER_ERROR(OB_INVALID_DATE_VALUE, in.get_string().length(), in.get_string().ptr(), "");
  } else {
    SET_RES_DATE(out);
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, DEFAULT_SCALE_FOR_DATE, DEFAULT_LENGTH_FOR_TEMPORAL);
  return ret;
}

static int string_time(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  int64_t value = 0;
  ObScale res_scale = -1;
  bool need_truncate = CM_IS_COLUMN_CONVERT(cast_mode) ? CM_IS_TIME_TRUNCATE_FRACTIONAL(cast_mode) : false;
  if (OB_UNLIKELY((ObStringTC != in.get_type_class()
                  && ObTextTC != in.get_type_class()
                  && ObGeometryTC != in.get_type_class())
                  || ObTimeTC != ob_obj_type_class(expect_type))) {
     ret = OB_ERR_UNEXPECTED;
     LOG_ERROR("invalid input type",
         K(ret), K(in), K(expect_type));
  } else if (lib::is_oracle_mode() && in.is_blob()) {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("invalid use of blob type", K(ret), K(in), K(expect_type));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Cast to blob type");
  } else if (CAST_FAIL(ObTimeConverter::str_to_time(in.get_string(), value, &res_scale, need_truncate))) {
  } else {
    SET_RES_TIME(out);
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, res_scale, DEFAULT_LENGTH_FOR_TEMPORAL);
  return ret;
}

static int string_year(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObObj int64;
  if (OB_UNLIKELY((ObStringTC != in.get_type_class()
                  && ObTextTC != in.get_type_class()
                  && ObGeometryTC != in.get_type_class())
                  || ObYearTC != ob_obj_type_class(expect_type))) {
     ret = OB_ERR_UNEXPECTED;
     LOG_ERROR("invalid input type",
         K(ret), K(in), K(expect_type));
  } else if (lib::is_oracle_mode() && in.is_blob()) {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("invalid use of blob type", K(ret), K(in), K(expect_type));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Cast to blob type");
  } else if (OB_FAIL(string_int(ObIntType, params, in, int64,
                                CM_UNSET_STRING_INTEGER_TRUNC(CM_SET_WARN_ON_FAIL(cast_mode))))) {
  } else if (0 == int64.get_int()) {
    const uint8_t base_year = 100;
    uint8_t value = 4 == in.get_string().length() ? ObTimeConverter::ZERO_YEAR : base_year;
    SET_RES_YEAR(out);
  } else if (CAST_FAIL(int_year(ObYearType, params, int64, out, cast_mode))) {
  } else if (CAST_FAIL(params.warning_)) {
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, DEFAULT_SCALE_FOR_YEAR, DEFAULT_LENGTH_FOR_TEMPORAL);
  return ret;
}

static int string_string(const ObObjType expect_type, ObObjCastParams &params,
                         const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObLength res_length = -1;
  if (OB_UNLIKELY((ObStringTC != in.get_type_class()
                   && ObTextTC != in.get_type_class()
                   && ObGeometryTC != in.get_type_class())
      || OB_UNLIKELY(ObStringTC != ob_obj_type_class(expect_type)
                     && ObTextTC != ob_obj_type_class(expect_type)))) {
     ret = OB_ERR_UNEXPECTED;
     LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if (lib::is_oracle_mode()
             && !(in.is_blob() && ob_is_blob(expect_type, params.expect_obj_collation_))
             && (in.is_blob())) {
    // 只允许 blob -> blob，不允许 blob -> !blob
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("invalid cast of blob type", K(ret), K(in), K(out.get_meta()), K(expect_type), K(cast_mode));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "cast to blob type");
  } else if (ObTextTC == in.get_type_class() && in.is_outrow()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("invalid cast of out row lob obj", K(ret), K(in), K(out.get_meta()), K(expect_type), K(cast_mode));
  } else if (lib::is_oracle_mode()
             && in.is_clob()
             && (0 == in.get_string().length())
             && !ob_is_clob(expect_type, params.expect_obj_collation_)) {
    // oracle 模式下的 empty_clob 被 cast 成其他类型时结果是 NULL
    out.set_null();
  } else {
    ObString str;
    // 考虑不同字符集的情况
    if (OB_FAIL(in.get_string(str))) {
      LOG_WARN("Failed to get payload from input string", K(ret), K(in));
    } else if (0 != str.length()
        && CS_TYPE_BINARY != in.get_collation_type()
        && CS_TYPE_BINARY != params.dest_collation_
        && (ObCharset::charset_type_by_coll(in.get_collation_type())
            != ObCharset::charset_type_by_coll(params.dest_collation_))) {
      char *buf = NULL;
      // buf_len 和编码长度有关，latin1使用 1 个字节编码一个字符，utf8mb4 使用 1 到 4 个字节
      // CharConvertFactorNum 是申请的内存大小的倍数
      int32_t buf_len = str.length() * ObCharset::CharConvertFactorNum;
      uint32_t result_len = 0;
      if (OB_UNLIKELY(NULL == (buf = static_cast<char*>(params.alloc(buf_len))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("alloc memory failed", K(ret));
      } else if (OB_FAIL(ObCharset::charset_convert(in.get_collation_type(),
                                                    str.ptr(),
                                                    str.length(),
                                                    params.dest_collation_,
                                                    buf,
                                                    buf_len,
                                                    result_len))) {
        LOG_WARN("charset convert failed", K(ret),
                 K(in.get_collation_type()),
                 K(params.dest_collation_), K(params.is_ignore_), K(cast_mode));
                if (params.is_ignore_ || CM_IS_IGNORE_CHARSET_CONVERT_ERR(cast_mode)) {
                    ObString question_mark = ObCharsetUtils::get_const_str(params.dest_collation_, '?');
                    int32_t str_offset = 0;
                    int64_t buf_offset = 0;
                    while (str_offset < str.length() && buf_offset + question_mark.length() <= buf_len) {
                        int64_t offset = ObCharset::charpos(in.get_collation_type(),
                str.ptr() + str_offset, str.length() - str_offset, 1);
                        if (OB_UNLIKELY(0 == offset)) {
                          break;
                        }
                        ret = ObCharset::charset_convert(in.get_collation_type(), str.ptr() + str_offset,
                static_cast<uint32_t>(offset), params.dest_collation_, buf + buf_offset, buf_len - buf_offset, result_len);
                        str_offset += offset;
                        if (OB_SUCCESS == ret) {
                            buf_offset += result_len;
                        } else {
                            MEMCPY(buf + buf_offset, question_mark.ptr(), question_mark.length());
                            buf_offset += question_mark.length();
                        }
                    }
                    if (str_offset < str.length()) {
                      ret = OB_SIZE_OVERFLOW;
                      LOG_WARN("size overflow", K(ret), K(str), KPHEX(str.ptr(), str.length()));
                    } else {
                        result_len = static_cast<uint32_t>(buf_offset);
                        ret = OB_SUCCESS;
                        LOG_WARN("charset convert failed", K(ret),
                                K(in.get_collation_type()), K(params.dest_collation_));
                    }
                }
      }

      LOG_DEBUG("convert result", K(str), "result", ObHexEscapeSqlStr(ObString(result_len, buf)));

      if (OB_SUCC(ret)) {
        if (ObTextTC == ob_obj_type_class(expect_type)) {
          out.set_lob_value(expect_type, buf, static_cast<int32_t>(result_len));
        } else {
          out.set_string(expect_type, buf, static_cast<int32_t>(result_len));
        }
        if (CS_TYPE_INVALID != in.get_collation_type()) {
          out.set_collation_type(params.dest_collation_);
        }
      }
    } else {
      // When convert blob to other charset, need to align to mbminlen of destination charset
      // by add '\0' prefix in mysql mode. (see mysql String::copy)
      int64_t align_offset = 0;
      const ObCharsetInfo *cs = NULL;
      if (CS_TYPE_BINARY == in.get_collation_type() && is_mysql_mode()
          && NULL != (cs = ObCharset::get_charset(params.dest_collation_))) {
        if (cs->mbminlen > 0 && in.get_string_len() % cs->mbminlen != 0) {
          align_offset = cs->mbminlen - in.get_string_len() % cs->mbminlen;
        }
      }
      ObObj tmp_out;
      ObString str;
      if (OB_FAIL(check_convert_string(expect_type, params, in, tmp_out))) {
        LOG_WARN("failed to check_and_convert_string", K(ret), K(in), K(expect_type));
      } else if (tmp_out.is_string_or_lob_locator_type()) { // may has locator header if it is text types
        if (OB_FAIL(tmp_out.get_string(str))) {
           LOG_WARN("failed to get string data", K(ret), K(in), K(tmp_out));
        }
      } else { // other types, e.g. raw
        str = tmp_out.get_string();
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(copy_string(params, expect_type, str, out, align_offset))) {
      } else {
        if (CS_TYPE_INVALID != tmp_out.get_collation_type()) {
          out.set_collation_type(tmp_out.get_collation_type());
        }
        if (ObTextTC == ob_obj_type_class(expect_type)) {
          out.set_inrow();
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    res_length = static_cast<ObLength>(out.get_string_len());
  }
  SET_RES_ACCURACY_STRING(expect_type, DEFAULT_PRECISION_FOR_STRING, res_length);
  UNUSED(cast_mode);
  return ret;
}

static int string_bit(const ObObjType expect_type, ObObjCastParams &params,
                         const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  uint64_t value = 0;
  if (OB_UNLIKELY((ObStringTC != in.get_type_class()
                  && ObTextTC != in.get_type_class()
                  && ObGeometryTC != in.get_type_class())
                  || ObBitTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if (lib::is_oracle_mode() && in.is_blob()) {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("invalid use of blob type", K(ret), K(in), K(expect_type));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Cast to blob type");
  } else {
    int32_t bit_len = 0;
    const ObString &str = in.get_string();
    if (OB_FAIL(get_bit_len(str, bit_len))) {
      LOG_WARN("fail to get bit length", K(ret), K(str));
    } else if (OB_UNLIKELY(bit_len <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("bit length is less than or equal to zero", K(ret), K(str), K(bit_len));
    } else {
      if (ObHexStringType == in.get_type() && str.empty()) {
        value = 0;
        ret = OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD;
        LOG_WARN("hex string is empty, can't cast to bit", K(ret), K(in));
      } else if (OB_UNLIKELY(bit_len > OB_MAX_BIT_LENGTH)) {
        value = UINT64_MAX;
        ret = OB_ERR_DATA_TOO_LONG;
        LOG_WARN("bit type length is too long", K(ret), K(str), K(OB_MAX_BIT_LENGTH), K(bit_len));
      } else {
        //将str按照二进制值转换为相应的uint64
        value = hex_to_uint64(str);
      }
      if (OB_FAIL(ret) && CM_IS_WARN_ON_FAIL(cast_mode)) {
        params.warning_ = OB_DATA_OUT_OF_RANGE;
        ret = OB_SUCCESS;
      }
      if (OB_SUCC(ret)) {
        SET_RES_BIT(out);
        SET_RES_ACCURACY(static_cast<ObPrecision>(bit_len), DEFAULT_SCALE_FOR_BIT, DEFAULT_LENGTH_FOR_NUMERIC);
      }
    }
  }
  return ret;
}

static int string_enum(const ObExpectType &expect_type, ObObjCastParams &params, const ObObj &in, ObObj &out)
{
  int ret = OB_SUCCESS;
  ObCastMode cast_mode = params.cast_mode_;
  const ObIArray<ObString> * type_infos = NULL;
  const ObCollationType in_cs_type = ObCharset::is_valid_collation(in.get_collation_type())
      ? in.get_collation_type()
      : ObCharset::get_system_collation();
  ObCollationType cs_type = expect_type.get_collation_type();
  int32_t pos = 0;
  uint64_t value = 0;
  int32_t no_sp_len = 0;
  ObString no_sp_val;
  ObString in_str;
  if (OB_UNLIKELY(ObEnumType != expect_type.get_type())
      || OB_UNLIKELY(ObStringTC != in.get_type_class() && ObTextTC != in.get_type_class())
      || OB_ISNULL(type_infos = expect_type.get_type_infos())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(expect_type), K(in), K(ret));
  } else if (lib::is_oracle_mode() && in.is_blob()) {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("invalid use of blob type", K(ret), K(in), K(expect_type));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Cast to blob type");
  } else if (OB_FAIL(ObCharset::charset_convert(*params.allocator_v2_,
                                                in.get_string(), in_cs_type,
                                                cs_type, in_str))) {
    LOG_WARN("convert charset failed", K(ret), K(in), K(cs_type));
  } else {
    no_sp_len = static_cast<int32_t>(ObCharset::strlen_byte_no_sp(cs_type, in_str.ptr(), in_str.length()));
    no_sp_val.assign_ptr(in_str.ptr(), static_cast<ObString::obstr_size_t>(no_sp_len));
    if (OB_FAIL(find_type(*type_infos, cs_type, no_sp_val, pos))) {
      LOG_WARN("fail to find type", KPC(type_infos), K(cs_type), K(no_sp_val), K(in_str), K(pos), K(ret));
    } else if (OB_UNLIKELY(pos < 0)) {
      if (!in_str.is_numeric()) {
        ret = OB_ERR_DATA_TRUNCATED;
      } else {
        int err = 0;
        int64_t val_cnt = type_infos->count();
        value = ObCharset::strntoull(in_str.ptr(), in_str.length(), 10, &err);
        if (err != 0 || value > val_cnt) {
          value = 0;
          ret = OB_ERR_DATA_TRUNCATED;
          LOG_WARN("input value out of range", K(in), K(val_cnt), K(ret), K(err));
        }
      }
      if (OB_FAIL(ret) && CM_IS_WARN_ON_FAIL(cast_mode)) {
        params.warning_ = OB_ERR_DATA_TRUNCATED;
        ret = OB_SUCCESS;
      }
    } else {
      value = pos + 1;//enum start from 1
    }
    LOG_DEBUG("finish string_enum", K(ret), K(expect_type), K(in), K(out), KPC(type_infos), K(lbt()));
    SET_RES_ENUM(out);
  }
  return ret;
}

static int string_set(const ObExpectType &expect_type, ObObjCastParams &params, const ObObj &in, ObObj &out)
{
  int ret = OB_SUCCESS;
  ObCastMode cast_mode = params.cast_mode_;
  const ObIArray<ObString> * type_infos = NULL;
  const ObCollationType in_cs_type = ObCharset::is_valid_collation(in.get_collation_type())
      ? in.get_collation_type()
      : ObCharset::get_system_collation();
  ObCollationType cs_type = expect_type.get_collation_type();
  const ObString &sep = ObCharsetUtils::get_const_str(cs_type, ',');
  int32_t pos = 0;
  uint64_t value = 0;
  ObString in_str;
  ObString val_str;
  bool is_last_value = false;
  if (OB_UNLIKELY(ObSetType != expect_type.get_type())
      || OB_UNLIKELY(ObStringTC != in.get_type_class() && ObTextTC != in.get_type_class())
      || OB_ISNULL(type_infos = expect_type.get_type_infos())
      || !ObCharset::is_valid_collation(cs_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(expect_type), K(in), K(cs_type), K(ret));
  } else if (lib::is_oracle_mode() && in.is_blob()) {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("invalid use of blob type", K(ret), K(in), K(expect_type));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Cast to blob type");
  } else if (in.get_string().empty()) {
     value = 0;
  } else if (OB_FAIL(ObCharset::charset_convert(*params.allocator_v2_,
                                                in.get_string(), in_cs_type,
                                                cs_type, in_str))) {
  } else {
    const char *remain = in_str.ptr();
    int64_t remain_len = ObCharset::strlen_byte_no_sp(cs_type, in_str.ptr(), in_str.length());
    do {
      pos = 0;
      const char *sep_loc = NULL;
      if (NULL == (sep_loc = static_cast<const char *>(memmem(
                      remain, remain_len, sep.ptr(), sep.length())))) {
        is_last_value = true;
        val_str.assign_ptr(remain, remain_len);
        if (OB_FAIL(find_type(*type_infos, cs_type, val_str, pos))) {
          LOG_WARN("fail to find type", KPC(type_infos), K(cs_type),
                   K(val_str), K(in_str), K(pos), K(ret));
        }
      } else {
        val_str.assign_ptr(remain, sep_loc - remain);
        remain_len = remain_len - (sep_loc - remain + sep.length());
        remain = sep_loc + sep.length();
        if (OB_FAIL(find_type(*type_infos, cs_type, val_str, pos))) {
          LOG_WARN("fail to find type",
                   KPC(type_infos), K(cs_type), K(val_str), K(in_str), K(pos), K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (pos < 0) {//not found
        if (CM_IS_WARN_ON_FAIL(cast_mode)) {
          params.warning_ = OB_ERR_DATA_TRUNCATED;
          LOG_INFO("input value out of range, and set out value zero",
                   K(pos), K(in), K(val_str), K(in_str), K(expect_type));
        } else {
          ret = OB_ERR_DATA_TRUNCATED;
          LOG_WARN("data truncate", K(pos), K(in), K(val_str), K(in_str), K(expect_type), K(ret));
        }
      } else {
        pos %= 64;//MySQL中，如果value存在重复，则value_count可以大于64
        value |= (1ULL << pos);
      }
    } while (OB_SUCC(ret) && !is_last_value);

    // Bug30666903: check implicit cast logic to handle number cases
    if (in_str.is_numeric() &&
        (OB_ERR_DATA_TRUNCATED == ret
        || (params.warning_ == OB_ERR_DATA_TRUNCATED && CM_IS_WARN_ON_FAIL(cast_mode)))) {
      int err = 0;
      value = ObCharset::strntoull(in_str.ptr(), in_str.length(), 10, &err);
      if(err == 0) {
        ret = OB_SUCCESS;
        uint32_t val_cnt = static_cast<uint32_t>(type_infos->count());
        if (OB_UNLIKELY(val_cnt <= 0)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect val_cnt", K(in), K(out), K(expect_type), K(ret));
        } else if (val_cnt >= 64) {//do nothing
        } else if (val_cnt < 64 && value > ((1ULL << val_cnt) - 1)) {
          value = 0;
          ret = OB_ERR_DATA_TRUNCATED;
          LOG_WARN("input value out of range", K(in), K(val_cnt), K(ret));
        }
        if (OB_FAIL(ret) && CM_IS_WARN_ON_FAIL(cast_mode)) {
          params.warning_ = OB_ERR_DATA_TRUNCATED;
          ret = OB_SUCCESS;
        }
      } else {
        value = 0;
      }
    }
  }
  LOG_DEBUG("finish string_set", K(ret), K(expect_type), K(in), K(out), KPC(type_infos), K(lbt()));
  SET_RES_SET(out);
  return ret;
}

#define CAST_TEXT_TO_ENUMSET(TYPE, OBJ_TYPE)                                        \
static int text_##TYPE(const ObExpectType &expect_type, ObObjCastParams &params,    \
                       const ObObj &in, ObObj &out)                                 \
{                                                                                   \
  int ret = OB_SUCCESS;                                                             \
  if (OB_UNLIKELY(OBJ_TYPE != expect_type.get_type())                               \
    || OB_UNLIKELY(ObTextTC != in.get_type_class())                                 \
    || OB_ISNULL(expect_type.get_type_infos())) {                                   \
    ret = OB_ERR_UNEXPECTED;                                                        \
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));                 \
  } else if (lib::is_oracle_mode() && in.is_blob()) {                               \
    ret = OB_NOT_SUPPORTED;                                                         \
    LOG_ERROR("invalid use of blob type", K(ret), K(in), K(expect_type));           \
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Blob cast to other type");                    \
  } else {                                                                          \
    ObString in_str;                                                                \
    ObTextStringIter instr_iter(in);                                                \
    if (OB_FAIL(instr_iter.init(0, NULL, params.allocator_v2_))) {                  \
      LOG_WARN("init lob str iter failed ", K(ret), K(in));                         \
    } else if (OB_FAIL(instr_iter.get_inrow_or_outrow_prefix_data(in_str))) {       \
      LOG_WARN("get full data from lob str iter failed ", K(ret), K(in));           \
    } else {                                                                        \
        ObObj tmp_obj;                                                              \
        tmp_obj.set_varchar(in_str);                                                \
        tmp_obj.set_collation_type(in.get_collation_type());                        \
        if (OB_FAIL(string_##TYPE(expect_type, params, tmp_obj, out))) {            \
          LOG_WARN("Text to " #TYPE "failed", K(ret), K(tmp_obj));                  \
        }                                                                           \
      }                                                                             \
    }                                                                               \
  return ret;                                                                       \
}

CAST_TEXT_TO_ENUMSET(enum, ObEnumType);
CAST_TEXT_TO_ENUMSET(set, ObSetType);

static int string_otimestamp(const ObObjType expect_type, ObObjCastParams &params,
                             const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  UNUSED(cast_mode);
  int ret = OB_SUCCESS;
  ObScale res_scale = -1;
  ObString utf8_string;

  if (OB_UNLIKELY(ObStringTC != in.get_type_class()
      && ObTextTC != in.get_type_class()
      && ObGeometryTC != in.get_type_class())
      || OB_UNLIKELY(ObOTimestampTC != ob_obj_type_class(expect_type))) {
     ret = OB_ERR_UNEXPECTED;
     LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if (lib::is_oracle_mode() && in.is_blob()) {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("invalid use of blob type", K(ret), K(in), K(expect_type));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Cast to blob type");
  } else if (OB_FAIL(convert_string_collation(in.get_string(), in.get_collation_type(), utf8_string, ObCharset::get_system_collation(), params))) {
    LOG_WARN("convert_string_collation", K(ret));
  } else {
    ObOTimestampData value;
    ObTimeConvertCtx cvrt_ctx(params.dtc_params_.tz_info_, true);
        cvrt_ctx.oracle_nls_format_ = params.dtc_params_.get_nls_format(expect_type);
    if (CAST_FAIL(ObTimeConverter::str_to_otimestamp(utf8_string, cvrt_ctx, expect_type, value, res_scale))) {
    } else {
      SET_RES_OTIMESTAMP(out);
    }
  }

  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, res_scale, DEFAULT_LENGTH_FOR_TEMPORAL);
  return ret;
}

static int string_raw(const ObObjType expect_type, ObObjCastParams &params,
                      const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  UNUSED(cast_mode);
  int ret = OB_SUCCESS;
  ObLength res_length = -1;

  if (OB_UNLIKELY(ObStringTC != in.get_type_class() && ObTextTC != in.get_type_class()) 
      || OB_UNLIKELY(ObRawTC != ob_obj_type_class(expect_type))) {
     ret = OB_ERR_UNEXPECTED;
     LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(ObHexUtils::hextoraw(in, params, out))) {
    LOG_WARN("fail to hextoraw", K(ret), K(in), K(expect_type));
  } else {
    res_length = static_cast<ObLength>(out.get_string_len());
  }
  SET_RES_ACCURACY_STRING(expect_type, DEFAULT_PRECISION_FOR_STRING, res_length);

  return ret;
}

static int string_interval(const ObObjType expect_type, ObObjCastParams &params,
                           const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  UNUSED(cast_mode);
  int ret = OB_SUCCESS;
  ObScale res_scale = -1;
  ObLength res_length = -1;
  ObPrecision res_precision = -1;
  ObString utf8_string;

  if (OB_UNLIKELY(!in.is_string_type())
      || OB_UNLIKELY(ObIntervalTC != ob_obj_type_class(expect_type))) {
     ret = OB_ERR_UNEXPECTED;
     LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(convert_string_collation(in.get_string(), in.get_collation_type(), utf8_string, ObCharset::get_system_collation(), params))) {
    LOG_WARN("convert_string_collation", K(ret));
  } else if (ObIntervalYMType == expect_type) {
    ObIntervalYMValue value;
    //don't know the scale, use the max scale and return the real scale
    res_scale = ObAccuracy::MAX_ACCURACY2[ORACLE_MODE][ObIntervalYMType].get_scale();
    if (CAST_FAIL(ObTimeConverter::str_to_interval_ym(utf8_string, value, res_scale))) {
    } else {
      SET_RES_INTERVAL_YM(out);
      out.set_scale(res_scale);
    }
  } else {
    ObIntervalDSValue value;
    //don't know the scale, use the max scale and return the real scale
    res_scale = ObAccuracy::MAX_ACCURACY2[ORACLE_MODE][ObIntervalDSType].get_scale();
    if (CAST_FAIL(ObTimeConverter::str_to_interval_ds(utf8_string, value, res_scale))) {
    } else {
      SET_RES_INTERVAL_DS(out);
      out.set_scale(res_scale);
    }
  }
  SET_RES_ACCURACY(res_precision, res_scale, res_length);
  return ret;
}

static int string_rowid(const ObObjType expect_type, ObObjCastParams &params,
                        const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ob_obj_type_class(expect_type) != ObRowIDTC) ||
      OB_UNLIKELY(in.get_type_class() != ObStringTC)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected arguments", K(expect_type), K(in));
  } else if (ObURowIDType != expect_type) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("only support urowid for now", K(ret), K(expect_type));
  } else {
    ObURowIDData value;
    ObString utf8_str;
    if (OB_FAIL(convert_string_collation(in.get_string(), in.get_collation_type(),
                                         utf8_str, ObCharset::get_system_collation(),
                                         params))) {
      LOG_WARN("convert_string_collation failed", K(ret), K(in), K(utf8_str));
    } else if (OB_FAIL(ObURowIDData::decode2urowid(utf8_str.ptr(), utf8_str.length(),
                                                   *params.allocator_v2_, value))) {
      LOG_WARN("failed to decode to urowid", K(ret));
    } else {
      SET_RES_UROWID(out);
    }
  }
  return ret;
}

static int string_lob(const ObObjType expect_type, ObObjCastParams &params,
                         const ObObj &in, ObObj &out, const ObCastMode cast_mode,
                         const ObLobLocator *lob_locator)
{
  int ret = OB_SUCCESS;
  ObObj tmp_out;
  if (!lib::is_oracle_mode()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected tenant mode", K(ret));
  } else if (OB_UNLIKELY((ObStringTC != in.get_type_class()
                   && ObTextTC != in.get_type_class())
      || OB_UNLIKELY(! ob_is_lob_locator(expect_type)))) {
     ret = OB_ERR_UNEXPECTED;
     LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(string_string(ObLongTextType, params, in, tmp_out, cast_mode))) {
    LOG_WARN("fail to cast string to longtext", K(ret), K(in));
  } else if (tmp_out.is_null()) {
    out.set_null();
  } else {
    ObString str;
    char *buf = nullptr;
    ObLobLocator *result = nullptr;
    if (OB_FAIL(tmp_out.get_string(str))) {
      STORAGE_LOG(WARN, "Failed to get string from obj", K(ret), K(tmp_out));
    }
    const int64_t buf_len = sizeof(ObLobLocator)
                            + (NULL == lob_locator ? 0 : lob_locator->payload_offset_)
                            + str.length();
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(buf = static_cast<char*>(params.alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to allocate memory for lob locator", K(ret), K(str));
    } else if (FALSE_IT(result = reinterpret_cast<ObLobLocator *> (buf))) {
    } else if (NULL == lob_locator) {
      if (OB_FAIL(result->init(str))) {
        STORAGE_LOG(WARN, "Failed to init lob locator", K(ret), K(str), KPC(result));
      }
    } else if (NULL != lob_locator) {
      ObString rowid;
      if (lob_locator->is_inline_mode()) { // inline mode
        if (OB_FAIL(lob_locator->get_rowid(rowid))) {
          LOG_WARN("get rowid failed", K(ret));
        } else if (OB_FAIL(result->init(lob_locator->table_id_,
                                        lob_locator->column_id_,
                                        lob_locator->snapshot_version_,
                                        lob_locator->flags_,
                                        rowid, str))) {
          STORAGE_LOG(WARN, "Failed to init lob locator", K(ret), K(str), KPC(result));
        }
      } else { // compat mode
        if (OB_FAIL(result->init(str))) {
          STORAGE_LOG(WARN, "Failed to init lob locator", K(ret), K(str), KPC(result));
        }
      }
    }
    if (OB_SUCC(ret)) {
      out = tmp_out;
      out.set_lob_locator(*result);
    }
  }
  return ret;
}

static int string_text(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode,
                       const ObLobLocatorV2 *lob_locator)
{
  int ret = OB_SUCCESS;
  ObObj tmp_out;
  ObString res_str;
  bool is_null_result = false;
  bool is_different_charset_type = (ObCharset::charset_type_by_coll(in.get_collation_type())
                                    != ObCharset::charset_type_by_coll(params.expect_obj_collation_));
  if (is_different_charset_type) {
    if (OB_FAIL(string_string(expect_type, params, in, tmp_out, cast_mode))) {
      LOG_WARN("fail to cast string to longtext", K(ret), K(in));
    } else if (tmp_out.is_null()) {
      out.set_null();
      is_null_result = true;
    } else {
      res_str = tmp_out.get_string();
    }
  } else if (OB_FAIL(in.get_string(res_str))) {
    LOG_WARN("Failed to get string from in obj", K(ret), K(tmp_out));
  } else { /* do nothing */ }

  if (OB_SUCC(ret) && !is_null_result) {
    bool has_lob_header = (!IS_CLUSTER_VERSION_BEFORE_4_1_0_0 && (expect_type != ObTinyTextType));
    sql::ObTextStringObObjResult str_result(expect_type, &params, &out, has_lob_header);
    if (lob_locator == NULL) {
      if (OB_FAIL(str_result.init(res_str.length()))) {
          LOG_WARN("init lob result failed");
        } else if (OB_FAIL(str_result.append(res_str.ptr(), res_str.length()))) {
          LOG_WARN("append lob result failed");
        } else { /* do nothing */ }
    } else if (OB_FAIL(str_result.copy(lob_locator))) {
      LOG_WARN("copy lob result failed");
    } else { /* do nothing*/ }

    if (OB_SUCC(ret)) {
      out = tmp_out; // copy meta
      str_result.set_result();
    }
  }
  return ret;
}

static int string_json(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObObj tmp_val;
  int32_t res_length = -1;
  bool need_charset_convert = ((CS_TYPE_BINARY != in.get_collation_type()) && 
                               (ObCharset::charset_type_by_coll(in.get_collation_type()) != 
                                ObCharset::charset_type_by_coll(params.dest_collation_)));
  if (lib::is_mysql_mode() && (params.dest_collation_ != CS_TYPE_UTF8MB4_BIN)) {
    ret = OB_ERR_INVALID_JSON_CHARSET;
    LOG_WARN("fail to cast string to json invalid outtype", K(ret), K(params.dest_collation_));
  } else if (CM_IS_COLUMN_CONVERT(cast_mode) && is_mysql_unsupported_json_column_conversion(in.get_type())) {
    ret = OB_ERR_INVALID_JSON_TEXT;
    LOG_USER_ERROR(OB_ERR_INVALID_JSON_TEXT); 
  } else if (OB_UNLIKELY(ObStringTC != in.get_type_class() && ObTextTC != in.get_type_class())
      || OB_UNLIKELY(ObJsonType != expect_type)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if (need_charset_convert && OB_FAIL(string_string(ObLongTextType, params, in, tmp_val, cast_mode))) {
    LOG_WARN("fail to cast string to string", K(ret), K(expect_type), K(in), K(params.dest_collation_), K(tmp_val));
  } else if(OB_UNLIKELY(params.allocator_v2_ == NULL)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid allocator in string to json cast", K(ret), K(params.allocator_v2_));
  } else {
    ObIJsonBase *j_base = NULL;
    ObString j_text = (need_charset_convert ? tmp_val.get_string() : in.get_string());
    ObJsonOpaque j_opaque(j_text, in.get_type());
    ObJsonString j_string(j_text.ptr(), j_text.length());
    ObJsonNull j_null;
    ObJsonNode *j_tree = NULL;
    uint32_t parse_flag = ObJsonParser::JSN_RELAXED_FLAG;
    bool is_oracle = lib::is_oracle_mode();
    ObObjType in_type = in.get_type();
    bool is_convert_jstr_type = (in_type == ObTinyTextType
                                 || in_type == ObTextType
                                 || in_type == ObMediumTextType
                                 || in_type == ObLongTextType);
    if (expect_type == ObJsonType && j_text.length() == 0 && cast_mode == 0) { // add column json null
      j_base = &j_null;
    } else if (is_oracle && (OB_ISNULL(j_text.ptr()) || j_text.length() == 0)) {
      j_base = &j_null;
    } else if (!is_oracle && CS_TYPE_BINARY == in.get_collation_type()) {
      j_base = &j_opaque;
    } else if (!is_oracle
                && CM_IS_IMPLICIT_CAST(cast_mode)
                && !CM_IS_COLUMN_CONVERT(cast_mode)
                && !CM_IS_JSON_VALUE(cast_mode)
                && is_convert_jstr_type) {
      // consistent with mysql: TINYTEXT, TEXT, MEDIUMTEXT, and LONGTEXT. We want to treat them like strings
      ret = OB_SUCCESS;
      j_base = &j_string;
    } else if (OB_FAIL(ObJsonParser::get_tree(params.allocator_v2_, j_text, j_tree, parse_flag))) {
      if (!is_oracle && CM_IS_IMPLICIT_CAST(cast_mode)
                     && !CM_IS_COLUMN_CONVERT(cast_mode)
                     && is_convert_jstr_type) {
        ret = OB_SUCCESS;
        j_base = &j_string;
      } else {
        LOG_WARN("fail to parse string as json", K(ret), K(expect_type), K(in), K(tmp_val));
        if (CM_IS_COLUMN_CONVERT(cast_mode)) {
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
      if (OB_FAIL(j_base->get_raw_binary(raw_bin, params.allocator_v2_))) {
        LOG_WARN("fail to get string json binary", K(ret), K(in), K(*j_base));
      } else if (OB_FAIL(set_json_bin_res(&params, &out, raw_bin))) {
        LOG_WARN("fail to fill json bin lob locator", K(ret));
      } else {
        res_length = out.get_val_len();
      }
    }
  }

  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_STRING, DEFAULT_SCALE_FOR_TEXT, res_length);
  return ret;
}

// Notice: used in PL
static int text_text(const ObObjType expect_type, ObObjCastParams &params,
                     const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObObjType in_type = in.get_type();
  ObObjType out_type = expect_type;
  ObCollationType in_cs_type = in.get_collation_type();
  ObCollationType out_cs_type = params.dest_collation_;
  ObString in_str;
  ObString res_str;
  ObObj tmp_out;
  ObObj tmp_in = in;
  ObString row_str = in.get_string();
  ObTextStringIter instr_iter(in);

  ObLobLocatorV2 in_lob_locator;
  in_lob_locator.assign_buffer(row_str.ptr(), row_str.length(), in.has_lob_header());
  bool is_persist = in_lob_locator.is_valid() && in_lob_locator.is_persist_lob();
  bool is_delta = in_lob_locator.is_valid() && in_lob_locator.is_delta_temp_lob();
  bool is_freed = in_lob_locator.is_valid() && in_lob_locator.is_freed();
  bool is_different_charset_type = (ObCharset::charset_type_by_coll(in_cs_type)
                                    != ObCharset::charset_type_by_coll(out_cs_type));
  bool is_any_cs = (in_cs_type == CS_TYPE_ANY || out_cs_type == CS_TYPE_ANY);
  const bool is_tiny_to_others = ((in_type == ObTinyTextType && out_type != ObTinyTextType)
                                  || (in_type != ObTinyTextType && out_type == ObTinyTextType));
  if (OB_UNLIKELY(ObTextTC != in.get_type_class()
      || OB_UNLIKELY(ObTextTC != ob_obj_type_class(expect_type)))) {
     ret = OB_ERR_UNEXPECTED;
     LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if ((is_different_charset_type && !is_delta && !(is_persist && is_any_cs) && !is_freed)
             || is_tiny_to_others ) {
    if (OB_FAIL(instr_iter.init(0, NULL, params.allocator_v2_))) {
      LOG_WARN("init lob str iter failed ", K(ret), K(in));
    } else if (OB_FAIL(instr_iter.get_full_data(in_str))) {
      LOG_WARN("get lob str iter full data failed ", K(ret), K(in.get_type()), K(in_str));
    } else if (lib::is_oracle_mode()
               && in.is_clob()
               && (0 == in_str.length())
               && !ob_is_clob(expect_type, params.expect_obj_collation_)) {
      // result is NULL when casting empty_clob to other types
      out.set_null();
    } else if (FALSE_IT(tmp_in.set_string(ObVarcharType, in_str))) {
    } else if (in_str.length() > 0 && OB_FAIL(string_string(ObVarcharType, params, tmp_in, tmp_out, cast_mode))) {
      LOG_WARN("fail to cast string to longtext", K(ret), K(in));
    } else if (in_str.length() == 0 &&
               FALSE_IT(tmp_out.set_string(tmp_in.get_type(), in_str)) &&
               FALSE_IT(tmp_out.set_collation_type(params.expect_obj_collation_))) {
    } else if (tmp_out.is_null()) {
      // for pl, should not get null for lob locator, return empty full tmp lob
      out = tmp_out; // copy meta
      out.set_lob_value(expect_type, row_str.ptr(), row_str.length());
    } else if (OB_FAIL(tmp_out.get_string(res_str))) {
      LOG_WARN("Failed to get string from tmp obj", K(ret), K(tmp_out));
    } else {
      bool has_lob_header = (!IS_CLUSTER_VERSION_BEFORE_4_1_0_0 && (expect_type != ObTinyTextType));
      sql::ObTextStringObObjResult str_result(expect_type, &params, &out, has_lob_header);
      if (OB_FAIL(str_result.init(res_str.length()))) {
        LOG_WARN("init lob result failed");
      } else if (OB_FAIL(str_result.append(res_str.ptr(), res_str.length()))) {
        LOG_WARN("append lob result failed");
      } else {
        out = tmp_out; // copy meta
        str_result.set_result();
      }
    }
  } else {
    out = in;
  }
  LOG_DEBUG("obj text cast to text", K(in), K(out),
    K(ret), K(in_type), K(out_type), K(in_cs_type), K(out_cs_type));

  return ret;
}

static int text_string(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObString in_str;
  ObTextStringIter instr_iter(in);
  if (OB_UNLIKELY(ObTextTC != in.get_type_class()
      || OB_UNLIKELY(ObStringTC != ob_obj_type_class(expect_type)))) {
     ret = OB_ERR_UNEXPECTED;
     LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if (lib::is_oracle_mode()
             && !(in.is_blob() && ob_is_blob(expect_type, params.expect_obj_collation_))
             && (in.is_blob())) {
    // blob can only cast to blobs
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("invalid cast of blob type", K(ret), K(in), K(out.get_meta()), K(expect_type), K(cast_mode));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "cast to blob type");
  } else if (OB_FAIL(instr_iter.init(0, NULL, params.allocator_v2_))) {
    LOG_WARN("init lob str iter failed ", K(ret), K(in));
  } else if (OB_FAIL(instr_iter.get_full_data(in_str))) {
    LOG_WARN("get lob str iter full data failed ", K(ret), K(in));
  } else if (lib::is_oracle_mode()
             && in.is_clob()
             && (0 == in_str.length())
             && !ob_is_clob(expect_type, params.expect_obj_collation_)) {
    // result is NULL when casting empty_clob to other types
    out.set_null();
  } else {
    ObObj tmp_in = in;
    tmp_in.set_string(ObVarcharType, in_str);
    if (OB_FAIL(string_string(expect_type, params, tmp_in, out, cast_mode))) {
      LOG_WARN("cast string to string failed", K(ret));
    }
  }
  return ret;
}

static int text_lob(const ObObjType expect_type, ObObjCastParams &params,
                    const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObString in_str;
  ObTextStringIter instr_iter(in);
  if (OB_UNLIKELY(ObTextTC != in.get_type_class()
      || OB_UNLIKELY(ObStringTC != ob_obj_type_class(expect_type)))) {
     ret = OB_ERR_UNEXPECTED;
     LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(instr_iter.init(0, NULL, params.allocator_v2_))) {
    LOG_WARN("init lob str iter failed ", K(ret), K(in));
  } else if (OB_FAIL(instr_iter.get_full_data(in_str))) {
    LOG_WARN("get lob str iter full data failed ", K(ret), K(in));
  } else {
    ObObj tmp_in = in;
    tmp_in.set_string(ObVarcharType, in_str);
    if (OB_FAIL(string_lob(expect_type, params, tmp_in, out, cast_mode))) {
      LOG_WARN("cast string to string failed", K(ret));
    }
  }
  return ret;
}

static int text_json(const ObObjType expect_type, ObObjCastParams &params,
                    const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObObj tmp_in = in;
  ObString in_str;
  ObTextStringIter instr_iter(in);
  if (OB_UNLIKELY(ObTextTC != in.get_type_class()
      || OB_UNLIKELY(ObJsonTC != ob_obj_type_class(expect_type)))) {
     ret = OB_ERR_UNEXPECTED;
     LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(instr_iter.init(0, NULL, params.allocator_v2_))) {
    LOG_WARN("init lob str iter failed ", K(ret), K(in));
  } else if (OB_FAIL(instr_iter.get_full_data(in_str))) {
    LOG_WARN("get lob str iter full data failed ", K(ret), K(in));
  } else {
    tmp_in.set_string(tmp_in.get_type(), in_str);
    tmp_in.set_inrow();
    if (OB_FAIL(string_json(expect_type, params, tmp_in, out, cast_mode))) {
      LOG_WARN("do string json failed", K(ret));
    }
  }
  return ret;
}

static int geometry_geometry(const ObObjType expect_type, ObObjCastParams &params,
                             const ObObj &in, ObObj &out, const ObCastMode cast_mode);
static int string_geometry(const ObObjType expect_type, ObObjCastParams &params,
                           const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(params.allocator_v2_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid allocator", K(ret));
  } else {
    ObIAllocator &temp_allocator = *params.allocator_v2_;
    ObGeometry *geo = NULL;
    omt::ObSrsCacheGuard srs_guard;
    const ObSrsItem *srs = NULL;
    ObGeoType dst_geo_type = ObGeoCastUtils::get_geo_type_from_cast_mode(cast_mode);
    const char *cast_name = ObGeometryTypeCastUtil::get_cast_name(dst_geo_type);
    ObString in_str;
    if (OB_FAIL(in.get_string(in_str))) {
      LOG_WARN("fail to get string", K(ret));
    } else if (OB_FAIL(common_get_srs_item(srs_guard, in_str, srs, cast_name))) {
      LOG_WARN("fail to get srs item", K(ret), K(in_str));
    } else if (OB_FAIL(common_build_geometry(temp_allocator, in_str, geo, srs, cast_name))) {
      LOG_WARN("fail to parse geometry", K(ret), K(in_str), K(dst_geo_type));
    } else if (ObGeoType::GEOMETRY == dst_geo_type) {
      out.set_string(expect_type, in_str.ptr(), in_str.length());
      SET_RES_ACCURACY_STRING(expect_type, DEFAULT_PRECISION_FOR_STRING, in_str.length());
    } else if (OB_FAIL(geometry_geometry(expect_type, params, in, out, cast_mode))) {
      LOG_WARN("fail to cast geometry", K(ret), K(dst_geo_type));
    }

    if (OB_FAIL(ret) && CM_IS_COLUMN_CONVERT(cast_mode) && ObGeoType::GEOMETRY == dst_geo_type) { // adapt mysql
      ret = OB_ERR_CANT_CREATE_GEOMETRY_OBJECT;
      LOG_USER_ERROR(OB_ERR_CANT_CREATE_GEOMETRY_OBJECT);
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////
// bit -> XXX
static int bit_int(const ObObjType expect_type, ObObjCastParams &params,
                      const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObPrecision res_precision = -1;
  int64_t value = 0;
  if (OB_UNLIKELY(ObBitTC != in.get_type_class()
                  || ObIntTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(in), K(expect_type), K(ret));
  } else {
    value =static_cast<int64_t>(in.get_bit());
    if (expect_type < ObIntType
        && CM_NEED_RANGE_CHECK(cast_mode)
        && CAST_FAIL(int_range_check(expect_type, value, value))) {
    } else {
      SET_RES_INT(out);
      res_precision = get_precision_for_integer(out.get_int());
      SET_RES_ACCURACY(res_precision, DEFAULT_SCALE_FOR_INTEGER, DEFAULT_LENGTH_FOR_NUMERIC);
    }
  }
  return ret;
}

static int bit_uint(const ObObjType expect_type, ObObjCastParams &params,
                      const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObPrecision res_precision = -1;
  uint64_t value = 0;
  if (OB_UNLIKELY(ObBitTC != in.get_type_class()
                  || ObUIntTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(in), K(expect_type), K(ret));
  } else {
    value = static_cast<uint64_t>(in.get_bit());
    if (expect_type < ObUInt64Type
        && CM_NEED_RANGE_CHECK(cast_mode)
        && CAST_FAIL(uint_upper_check(expect_type, value))) {
    } else {
      SET_RES_UINT(out);
      res_precision = get_precision_for_integer(out.get_uint64());
      SET_RES_ACCURACY(res_precision, DEFAULT_SCALE_FOR_INTEGER, DEFAULT_LENGTH_FOR_NUMERIC);
    }
  }
  return ret;
}

static int bit_double(const ObObjType expect_type, ObObjCastParams &params,
                      const ObObj &in, ObObj &out, const ObCastMode cast_mode);
static int bit_float(const ObObjType expect_type, ObObjCastParams &params,
                      const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObObj dbl;
  if (OB_UNLIKELY((ObBitTC != in.get_type_class()
                   || ObFloatTC != ob_obj_type_class(expect_type)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(bit_double(ObDoubleType, params, in, dbl, cast_mode))) {
  } else if (OB_FAIL(double_float(expect_type, params, dbl, out, cast_mode))) {
  }
  //has set accuracy in prev double_float call
  return ret;
}


static int bit_double(const ObObjType expect_type, ObObjCastParams &params,
                      const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  UNUSED(cast_mode);
  int ret = OB_SUCCESS;
  double value = 0.0;
  if (OB_UNLIKELY((ObBitTC != in.get_type_class()
                   || ObDoubleTC != ob_obj_type_class(expect_type)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else {
    value = static_cast<double>(in.get_bit());
    SET_RES_DOUBLE(out);
    SET_RES_ACCURACY(PRECISION_UNKNOWN_YET, SCALE_UNKNOWN_YET, LENGTH_UNKNOWN_YET);
  }
  return ret;
}

static int bit_number(const ObObjType expect_type, ObObjCastParams &params,
                        const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  UNUSED(cast_mode);
  int ret = OB_SUCCESS;
  number::ObNumber nmb;
  ObPrecision res_precision = -1;
  if (OB_UNLIKELY(ObBitTC != in.get_type_class()
                  || ObNumberTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(nmb.from(in.get_bit(), params))) {
    LOG_WARN("fail to make number", K(ret), K(in));
  } else {
    out.set_number(expect_type, nmb);
    res_precision = get_precision_for_integer(in.get_bit());
    SET_RES_ACCURACY(res_precision, DEFAULT_SCALE_FOR_INTEGER, DEFAULT_LENGTH_FOR_NUMERIC);
  }
  return ret;
}

static int bit_datetime(const ObObjType expect_type, ObObjCastParams &params,
                        const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  uint64_t bit_value = in.get_bit();
  int64_t value = 0;
  ObScale res_scale = -1;
  ObDateSqlMode date_sql_mode;
  date_sql_mode.allow_invalid_dates_ = CM_IS_ALLOW_INVALID_DATES(cast_mode);
  date_sql_mode.no_zero_date_ = CM_IS_NO_ZERO_DATE(cast_mode);
  if (OB_UNLIKELY((ObBitTC != in.get_type_class()
                   || ObDateTimeTC != ob_obj_type_class(expect_type)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if (CM_IS_COLUMN_CONVERT(cast_mode)) {
    // if cast mode is column convert, using bit as int64 to do cast.
    ObTimeConvertCtx cvrt_ctx(params.dtc_params_.tz_info_, ObTimestampType == expect_type);
    if (CAST_FAIL(ObTimeConverter::int_to_datetime(bit_value, 0, cvrt_ctx, value,
                                                   date_sql_mode))) {
      LOG_WARN("int_to_datetime failed", K(ret), K(bit_value));
    }
  } else {
    // using bit as char array to do cast.
    ObScale scale = in.get_scale();
    const int32_t BUF_LEN = (OB_MAX_BIT_LENGTH + 7) / 8;
    int64_t pos = 0;
    char buf[BUF_LEN];
    MEMSET(buf, 0, BUF_LEN);
    if (OB_FAIL(bit_to_char_array(bit_value, scale, buf, BUF_LEN, pos))) {
      LOG_WARN("fail to store val", KP(buf), K(BUF_LEN), K(bit_value), K(pos));
    } else {
      ObString str(pos, buf);
      ObTimeConvertCtx cvrt_ctx(params.dtc_params_.tz_info_, ObTimestampType == expect_type);
      if (CAST_FAIL(ObTimeConverter::str_to_datetime(str, cvrt_ctx, value, &res_scale,
                                                     date_sql_mode))) {
        LOG_WARN("int_to_datetime failed", K(ret), K(bit_value), K(str));
      }
    }
  }
  if (OB_SUCC(ret)) {
    SET_RES_DATETIME(out);
    SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, res_scale, DEFAULT_LENGTH_FOR_TEMPORAL);
  }
  return ret;
}

static int bit_date(const ObObjType expect_type, ObObjCastParams &params,
                        const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  uint64_t bit_value = in.get_bit();
  int32_t value = 0;
  ObScale res_scale = -1;
  ObDateSqlMode date_sql_mode;
  date_sql_mode.allow_invalid_dates_ = CM_IS_ALLOW_INVALID_DATES(cast_mode);
  date_sql_mode.no_zero_date_ = CM_IS_NO_ZERO_DATE(cast_mode);
  if (OB_UNLIKELY((ObBitTC != in.get_type_class()
                   || ObDateTC != ob_obj_type_class(expect_type)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if (CM_IS_COLUMN_CONVERT(cast_mode)) {
    // if cast mode is column convert, using bit as int64 to do cast.
    if (CAST_FAIL(ObTimeConverter::int_to_date(bit_value, value, date_sql_mode))) {
      LOG_WARN("int_to_date failed", K(ret), K(bit_value));
    }
  } else {
    // using bit as char array to do cast.
    ObScale scale = in.get_scale();
    const int32_t BUF_LEN = (OB_MAX_BIT_LENGTH + 7) / 8;
    int64_t pos = 0;
    char buf[BUF_LEN];
    MEMSET(buf, 0, BUF_LEN);
    if (OB_FAIL(bit_to_char_array(bit_value, scale, buf, BUF_LEN, pos))) {
      LOG_WARN("fail to store val", KP(buf), K(BUF_LEN), K(bit_value), K(pos));
    } else {
      ObString str(pos, buf);
      if (CAST_FAIL(ObTimeConverter::str_to_date(str, value, date_sql_mode))) {
        LOG_WARN("str_to_date failed", K(ret), K(bit_value), K(str));
      }
    }
  }
  if (OB_SUCC(ret)) {
    SET_RES_DATE(out);
    SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, res_scale, DEFAULT_LENGTH_FOR_TEMPORAL);
  }
  return ret;
}

static int bit_time(const ObObjType expect_type, ObObjCastParams &params,
                        const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  uint64_t bit_value = in.get_bit();
  int64_t value = 0;
  ObScale res_scale = -1;
  if (OB_UNLIKELY((ObBitTC != in.get_type_class()
                   || ObTimeTC != ob_obj_type_class(expect_type)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if (CM_IS_COLUMN_CONVERT(cast_mode)) {
    // if cast mode is column convert, using bit as int64 to do cast.
    if (CAST_FAIL(ObTimeConverter::int_to_time(bit_value, value))) {
      LOG_WARN("int_to_time failed", K(ret), K(bit_value));
    }
  } else {
    // using bit as char array to do cast.
    ObScale scale = in.get_scale();
    const int32_t BUF_LEN = (OB_MAX_BIT_LENGTH + 7) / 8;
    int64_t pos = 0;
    char buf[BUF_LEN];
    MEMSET(buf, 0, BUF_LEN);
    if (OB_FAIL(bit_to_char_array(bit_value, scale, buf, BUF_LEN, pos))) {
      LOG_WARN("fail to store val", KP(buf), K(BUF_LEN), K(bit_value), K(pos));
    } else {
      ObString str(pos, buf);
      if (CAST_FAIL(ObTimeConverter::str_to_time(str, value, &res_scale))) {
        LOG_WARN("str_to_date failed", K(ret), K(bit_value), K(str));
      }
    }
  }
  if (OB_SUCC(ret)) {
    SET_RES_TIME(out);
    SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, res_scale, DEFAULT_LENGTH_FOR_TEMPORAL);
  }
  return ret;
}

static int bit_year(const ObObjType expect_type, ObObjCastParams &params,
                        const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  uint64_t bit_value = in.get_bit();
  uint8_t value = 0;
  ObScale res_scale = -1;
  if (OB_UNLIKELY((ObBitTC != in.get_type_class()
                   || ObYearTC != ob_obj_type_class(expect_type)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if (CAST_FAIL(ObTimeConverter::int_to_year(bit_value, value))) {
    LOG_WARN("int_to_year faile", K(ret), K(bit_value));
  } else {
    SET_RES_YEAR(out);
    SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, res_scale, DEFAULT_LENGTH_FOR_TEMPORAL);
  }
  return ret;
}

static int bit_string(const ObObjType expect_type, ObObjCastParams &params,
                      const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  uint64_t value = in.get_bit();
  ObLength res_length = -1;
  if (OB_UNLIKELY((ObBitTC != in.get_type_class()
                   || (ObStringTC != ob_obj_type_class(expect_type)
                       && ObTextTC != ob_obj_type_class(expect_type))))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if (CM_IS_COLUMN_CONVERT(cast_mode)) {
    // if cast mode is column convert, using bit as int64 to do cast.
    ObFastFormatInt ffi(value);
    ObString tmp_str;
    if (OB_FAIL(convert_string_collation(ObString(ffi.length(), ffi.ptr()),
                ObCharset::get_system_collation(), tmp_str, params.dest_collation_, params))) {
      LOG_WARN("fail to convert string collation", K(ret));
    } else if (OB_FAIL(copy_string(params, expect_type, tmp_str.ptr(), tmp_str.length(), out))) {
      LOG_WARN("fail to copy string", KP(tmp_str.ptr()), K(tmp_str.length()), K(value),
                                      K(out), K(expect_type));
    }
  } else {
    // using bit as char array to do cast.
    ObScale scale = in.get_scale();
    const int32_t BUF_LEN = (OB_MAX_BIT_LENGTH + 7) / 8;
    int64_t pos = 0;
    char tmp_buf[BUF_LEN];
    MEMSET(tmp_buf, 0, BUF_LEN);
    if (OB_FAIL(bit_to_char_array(value, scale, tmp_buf, BUF_LEN, pos))) {
      LOG_WARN("fail to store val", KP(tmp_buf), K(BUF_LEN), K(value), K(pos));
    } else if (OB_FAIL(copy_string(params, expect_type, tmp_buf, pos, out))) {
      LOG_WARN("fail to copy string", KP(tmp_buf), K(pos), K(value),
                                      K(out), K(expect_type));
    }
  }
  if (OB_SUCC(ret)) {
    res_length = static_cast<ObLength>(out.get_string_len());
    out.set_collation_type(params.dest_collation_);
    SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_STRING, DEFAULT_SCALE_FOR_STRING, res_length);
  }
  return ret;
}

static int bit_bit(const ObObjType expect_type, ObObjCastParams &params,
                      const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  UNUSED(cast_mode);
  int ret = OB_SUCCESS;
  uint64_t value = in.get_bit();
  int32_t bit_len = 0;
  if (OB_UNLIKELY(ObBitTC != in.get_type_class()
                  || ObBitTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(get_bit_len(value, bit_len))) {
    LOG_WARN("fail to get bit len", K(ret), K(value), K(bit_len));
  } else {
    SET_RES_BIT(out);
    SET_RES_ACCURACY(static_cast<ObPrecision>(bit_len), DEFAULT_SCALE_FOR_BIT, DEFAULT_LENGTH_FOR_NUMERIC);
  }
  return ret;

}


static int bit_enum(const ObExpectType &expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObEnumType != expect_type.get_type())
      || OB_UNLIKELY(ObBitTC != in.get_type_class())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(expect_type), K(in), K(ret));
  } else {
    uint64_t value = in.get_bit();
    ObObj uint_val;
    uint_val.set_uint64(value);
    if (OB_FAIL(uint_enum(expect_type, params, uint_val, out))) {
      LOG_WARN("fail to cast int to enum", K(expect_type), K(in), K(uint_val), K(out), K(ret));
    }
  }
  return ret;
}

static int bit_set(const ObExpectType &expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObSetType != expect_type.get_type())
      || OB_UNLIKELY(ObBitTC != in.get_type_class())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(expect_type), K(in), K(ret));
  } else {
    uint64_t value = in.get_bit();
    ObObj uint_val;
    uint_val.set_uint64(value);
    if (OB_FAIL(uint_set(expect_type, params, uint_val, out))) {
      LOG_WARN("fail to cast int to enum", K(expect_type), K(in), K(uint_val), K(out), K(ret));
    }
  }
  return ret;
}

CAST_TO_LOB_METHOD(bit, ObBitTC);
CAST_TO_TEXT_METHOD(bit, ObBitTC);

static int bit_json(const ObObjType expect_type, ObObjCastParams &params,
                    const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  UNUSED(cast_mode);

  if (OB_UNLIKELY((ObBitTC != in.get_type_class()
      || (ObJsonTC != ob_obj_type_class(expect_type))))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else {
    ObIAllocator *allocator = params.allocator_v2_;
    uint64_t value = in.get_bit();
    ObScale scale = in.get_scale();
    const int32_t BUF_LEN = (OB_MAX_BIT_LENGTH + 7) / 8;
    char buf[BUF_LEN] = {0};
    int64_t pos = 0;
    if (OB_FAIL(bit_to_char_array(value, scale, buf, BUF_LEN, pos))) {
      LOG_WARN("fail to store val", KP(buf), K(BUF_LEN), K(value), K(pos));
    } else if (OB_ISNULL(allocator)) {
      ret= OB_ERR_UNEXPECTED;
      LOG_ERROR("NULL allocator in json cast function", K(ret), K(in), K(expect_type));
    } else {
      common::ObString j_value(pos, buf);
      ObJsonOpaque j_opaque(j_value, ObBitType);
      ObIJsonBase *j_base = &j_opaque;
      ObString raw_bin;
      if (OB_FAIL(j_base->get_raw_binary(raw_bin, params.allocator_v2_))) {
        LOG_WARN("fail to get int json binary", K(ret), K(in), K(expect_type), K(*j_base));
      } else if (OB_FAIL(copy_string(params, expect_type, raw_bin.ptr(), raw_bin.length(), out))) {
        LOG_WARN("fail to copy string", K(ret), K(expect_type), K(raw_bin));
      } else if (OB_FAIL(set_json_bin_res(&params, &out, out.get_string()))) {
        LOG_WARN("fail to fill json bin lob locator", K(ret));
      } else {
        ObLength res_length = static_cast<ObLength>(out.get_string_len());
        SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_STRING, DEFAULT_SCALE_FOR_STRING, res_length);
      }
    }
  }

  return ret;
}

static int bit_geometry(const ObObjType expect_type, ObObjCastParams &params,
                        const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObLength res_length = -1;
  if (OB_ISNULL(params.allocator_v2_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid allocator", K(ret));
  } else {
    ObIAllocator &temp_allocator = *params.allocator_v2_;
    ObGeometry *geo = NULL;
    omt::ObSrsCacheGuard srs_guard;
    const ObSrsItem *srs = NULL;
    ObGeoType dst_geo_type = ObGeoCastUtils::get_geo_type_from_cast_mode(cast_mode);
    const char *cast_name = ObGeometryTypeCastUtil::get_cast_name(dst_geo_type);
    ObFastFormatInt ffi(in.get_bit());
    if (OB_FAIL(common_get_srs_item(srs_guard, ObString(ffi.length(), ffi.ptr()), srs, cast_name))) {
      LOG_WARN("fail to get srs item", K(ret));
    } else if (OB_FAIL(common_build_geometry(temp_allocator, ObString(ffi.length(), ffi.ptr()),
        geo, srs, cast_name))) {
      LOG_WARN("fail to parse geometry", K(ret), K(dst_geo_type));
    } else {
      ObString res_wkb;
      if (OB_FAIL(ObGeoTypeUtil::to_wkb(temp_allocator, *geo, srs, res_wkb))) {
        LOG_WARN("fail to get wkb", K(ret), K(dst_geo_type));
      } else if (OB_FAIL(set_geo_res(&params, &out, res_wkb))) {
        LOG_WARN("set geo result failed", K(ret));
      } else {
        res_length = static_cast<ObLength>(out.get_string_len());
      }
    }

    if (OB_FAIL(ret) && CM_IS_COLUMN_CONVERT(cast_mode) && ObGeoType::GEOMETRY == dst_geo_type) { // adapt mysql
      ret = OB_ERR_CANT_CREATE_GEOMETRY_OBJECT;
      LOG_USER_ERROR(OB_ERR_CANT_CREATE_GEOMETRY_OBJECT);
    }
  }
  SET_RES_ACCURACY_STRING(expect_type, DEFAULT_PRECISION_FOR_STRING, res_length);
  return ret;
}

////////////////////////////////////////////////////////////////
// enum -> XXX
/*//来自同一个enum 列的obj转换使用应该使用该接口，来自不同enum列的转换使用enumsetinner_enum
static int enumset_enum(const ObExpectType &expect_type, ObObjCastParams &params, const ObObj &in, ObObj &out)
{
  int ret = OB_SUCCESS;
  uint64_t value = static_cast<uint64_t>(in.get_enum());
  ObCastMode cast_mode = params.cast_mode_;
  const ObIArray<ObString> * type_infos = NULL;
  if (OB_UNLIKELY(ObEnumType != expect_type.get_type())
      || OB_UNLIKELY(ObEnumType != in.get_type())
      || OB_ISNULL(type_infos = expect_type.get_type_infos())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(expect_type), K(in), K(ret));
  } else if (OB_UNLIKELY(value > type_infos->count())){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected value", K(in), K(out), K(expect_type), K(ret));
  } else {
    SET_RES_ENUM(out);
  }
  return ret;
}

////////////////////////////////////////////////////////////////
// set -> XXX
//来自同一个set 列的obj转换使用应该使用该接口，来自不同set列的转换使用enumsetinner_set
static int enumset_set(const ObExpectType &expect_type, ObObjCastParams &params, const ObObj &in, ObObj &out)
{
  int ret = OB_SUCCESS;
  uint64_t value = static_cast<uint64_t>(in.get_set());
  ObCastMode cast_mode = params.cast_mode_;
  const ObIArray<ObString> * type_infos = NULL;
  if (OB_UNLIKELY(ObSetType != expect_type.get_type())
      || OB_UNLIKELY(ObSetType != in.get_type())
      || OB_ISNULL(type_infos = expect_type.get_type_infos())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(expect_type), K(in), K(ret));
  } else {
    int64_t val_cnt = type_infos->count();
    if (OB_UNLIKELY(val_cnt > 64 || val_cnt <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect val_cnt", K(in), K(out), K(expect_type), K(ret));
    } else if (OB_UNLIKELY(val_cnt < 64 && value > ((1ULL << val_cnt) - 1))){
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected value", K(in), K(out), K(expect_type), K(ret));
    } else {
      SET_RES_SET(out);
    }
  }
  return ret;
}

static int enumsetinner_enum(const ObExpectType &expect_type, ObObjCastParams &params, const ObObj &in, ObObj &out)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObEnumSetInnerTC != in.get_type_class())
      || OB_UNLIKELY(ObEnumType != expect_type.get_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected type", K(expect_type), K(in), K(ret));
  } else {
    ObString inner_str = in.get_string();
    ObEnumSetInnerValue inner_val;
    int64_t pos = 0;
    if (OB_FAIL(inner_val.deserialize(inner_str.ptr(), inner_str.length(), pos))) {
      LOG_WARN("fail to deserialize", K(inner_str), K(pos), K(in), K(ret));
    } else {
      ObObj str_val;
      str_val.set_varchar(inner_val.string_value_);
      if (OB_FAIL(string_enum(expect_type, params, str_val, out))) {
        LOG_WARN("fail to cast string to varchar", K(ret));
      }
    }
  }
  return ret;
}

static int enumsetinner_set(const ObExpectType &expect_type, ObObjCastParams &params, const ObObj &in, ObObj &out)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObEnumSetInnerTC != in.get_type_class())
      || OB_UNLIKELY(ObSetType != expect_type.get_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected type", K(expect_type), K(in), K(ret));
  } else {
    ObString inner_str = in.get_string();
    ObEnumSetInnerValue inner_val;
    int64_t pos = 0;
    if (OB_FAIL(inner_val.deserialize(inner_str.ptr(), inner_str.length(), pos))) {
      LOG_WARN("fail to deserialize", K(inner_str), K(pos), K(in), K(ret));
    } else {
      ObObj str_val;
      str_val.set_varchar(inner_val.string_value_);
      if (OB_FAIL(string_set(expect_type, params, str_val, out))) {
        LOG_WARN("fail to cast string to varchar", K(ret));
      }
    }
  }
  return ret;
  }*/


ObCastEnumOrSetFunc OB_CAST_ENUM_OR_SET[ObMaxTC][2] =
{
  {
    /*null -> enum_or_set*/
    cast_identity_enum_set,/*enum*/
    cast_identity_enum_set,/*set*/
  },
  {
    /*int -> enum_or_set*/
    int_enum,/*enum*/
    int_set,/*set*/
  },
  {
    /*uint -> enum_or_set*/
    uint_enum,/*enum*/
    uint_set,/*set*/
  },
  {
    /*float -> enum_or_set*/
    float_enum,/*enum*/
    float_set,/*set*/
  },
  {
    /*double -> enum_or_set*/
    double_enum,/*enum*/
    double_set,/*set*/
  },
  {
    /*number -> enum_or_set*/
    number_enum,/*enum*/
    number_set,/*set*/
  },
  {
    /*datetime -> enum_or_set*/
    datetime_enum,/*enum*/
    datetime_set,/*set*/
  },
  {
    /*date -> enum_or_set*/
    date_enum,/*enum*/
    date_set,/*set*/
  },
  {
    /*time -> enum_or_set*/
    time_enum, /*enum*/
    time_set, /*set*/
  },
  {
    /*year -> enum_or_set*/
    year_enum,/*enum*/
    year_set,/*set*/
  },
  {
    /*string -> enum_or_set*/
    string_enum,/*enum*/
    string_set,/*set*/
  },
  {
    /*extend -> enum_or_set*/
    cast_not_support_enum_set,/*enum*/
    cast_not_support_enum_set,/*set*/
  },
  {
    /*unknow -> enum_or_set*/
    cast_not_support_enum_set,/*enum*/
    cast_not_support_enum_set,/*set*/
  },
  {
    /*text -> enum_or_set*/
    text_enum,/*enum*/
    text_set,/*set*/
  },
  {
    /*bit -> enum_or_set*/
    bit_enum,/*enum*/
    bit_set,/*set*/
  },
  {
    /*enumset tc -> enum_or_set*/
    cast_not_expected_enum_set,/*enum*/
    cast_not_expected_enum_set,/*set*/
  },
  {
    /*enumset_inner tc -> enum_or_set*/
    cast_not_expected_enum_set,/*enum*/
    cast_not_expected_enum_set,/*set*/
  },
  {
    /*OTimestamp -> enum_or_set*/
    cast_not_support_enum_set,/*enum*/
    cast_not_support_enum_set,/*set*/
  },
  {
    /*Raw -> enum_or_set*/
    cast_not_support_enum_set,/*enum*/
    cast_not_support_enum_set,/*set*/
  },
  {
    /*Interval -> enum_or_set*/
    cast_not_support_enum_set,/*enum*/
    cast_not_support_enum_set,/*set*/
  },
  {
    /*RowID -> enum_or_set*/
    cast_not_support_enum_set,/*enum*/
    cast_not_support_enum_set,/*set*/
  },
  {
    /*Lob -> enum_or_set*/
    cast_not_support_enum_set,/*enum*/
    cast_not_support_enum_set,/*set*/
  },
  {
    /*Json -> enum_or_set*/
    cast_not_support_enum_set,/*enum*/
    cast_not_support_enum_set,/*set*/
  },
  {
    /*Geometry -> enum_or_set*/
    cast_not_support_enum_set,/*enum*/
    cast_not_support_enum_set,/*set*/
  },
};

////////////////////////////////////////////////////////////
// enum -> XXX

static int enumset_int(const ObObjType expect_type, ObObjCastParams &params,
                    const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  //we regard enum as uint64 when int_value is needed
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObEnumSetTC != in.get_type_class()
                  || ObIntTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    ObObj uint64_in;
    uint64_in.set_uint64(in.get_uint64());
    ret = uint_int(expect_type, params, uint64_in, out, cast_mode);
  }
  return ret;
}

static int enumset_uint(const ObObjType expect_type, ObObjCastParams &params,
                     const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObPrecision res_precision = -1;
  if (OB_UNLIKELY(ObEnumSetTC != in.get_type_class()
                  || ObUIntTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    uint64_t value = in.get_uint64();
    if (CAST_FAIL(uint_upper_check(expect_type, value))) {
    } else {
      out.set_uint(expect_type, value);
      res_precision = get_precision_for_integer(value);
    }
  }
  SET_RES_ACCURACY(res_precision, DEFAULT_SCALE_FOR_INTEGER, DEFAULT_LENGTH_FOR_NUMERIC);
  return ret;
}

static int enumset_float(const ObObjType expect_type, ObObjCastParams &params,
                      const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObEnumSetTC != in.get_type_class()
                  || ObFloatTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    out.set_float(expect_type, static_cast<float>(in.get_uint64()));
  }
  UNUSED(cast_mode);
  SET_RES_ACCURACY(PRECISION_UNKNOWN_YET, SCALE_UNKNOWN_YET, LENGTH_UNKNOWN_YET);
  return ret;
}

static int enumset_double(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObEnumSetTC != in.get_type_class()
                  || ObDoubleTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    out.set_double(expect_type, static_cast<double>(in.get_uint64()));
  }
  UNUSED(cast_mode);
  SET_RES_ACCURACY(PRECISION_UNKNOWN_YET, SCALE_UNKNOWN_YET, LENGTH_UNKNOWN_YET);
  return ret;
}

static int enumset_number(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  number::ObNumber nmb;
  ObPrecision res_precision = -1;
  if (OB_UNLIKELY(ObEnumSetTC != in.get_type_class()
                  || ObNumberTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(nmb.from(in.get_uint64(), params))) {
    LOG_ERROR("failed to from number",
        K(ret), K(in), K(expect_type));
  } else {
    out.set_number(expect_type, nmb);
    res_precision = get_precision_for_integer(in.get_uint64());
  }
  UNUSED(cast_mode);
  SET_RES_ACCURACY(res_precision, DEFAULT_SCALE_FOR_INTEGER, DEFAULT_LENGTH_FOR_NUMERIC);
  return ret;
}

static int enumset_year(const ObObjType expect_type, ObObjCastParams &params,
                     const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObObj int64;
  if (OB_UNLIKELY(ObEnumSetTC != in.get_type_class()
                  || ObYearTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
    } else if (OB_FAIL(enumset_int(ObIntType, params, in, int64, CM_UNSET_NO_CAST_INT_UINT(cast_mode)))) {
    } else if (OB_FAIL(int_year(expect_type, params, int64, out, cast_mode))) {
    }
    SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, DEFAULT_SCALE_FOR_YEAR, DEFAULT_LENGTH_FOR_TEMPORAL);
    return ret;
  }

static int enumset_bit(const ObObjType expect_type, ObObjCastParams &params,
                    const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  UNUSED(cast_mode);
  int ret = OB_SUCCESS;
  uint64_t value = in.get_uint64();
  int32_t bit_len = 0;
  if (OB_UNLIKELY(ObEnumSetTC != in.get_type_class()
                  || ObBitTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(get_bit_len(value, bit_len))) {
    LOG_WARN("fail to get bit len", K(ret), K(value), K(bit_len));
  } else {
    SET_RES_BIT(out);
    SET_RES_ACCURACY(static_cast<ObPrecision>(bit_len), DEFAULT_SCALE_FOR_BIT, DEFAULT_LENGTH_FOR_NUMERIC);
  }
  return ret;
}


static int get_uint64_from_enumset_inner(const ObObj &in, ObObj &out)
{
  int ret = OB_SUCCESS;
  ObEnumSetInnerValue inner_value;
  if (OB_UNLIKELY(ObEnumSetInnerTC != in.get_type_class())) {
    LOG_ERROR("invalid input type", K(ret), K(in));
  } else if (OB_FAIL(in.get_enumset_inner_value(inner_value))) {
    LOG_WARN("failed to get_enumset_inner_value", K(in), K(ret));
  } else {
    out.set_uint64(inner_value.numberic_value_);
  }
  return ret;
}

static int get_string_from_enumset_inner(const ObObj &in, ObObj &out)
{
  int ret = OB_SUCCESS;
  ObEnumSetInnerValue inner_value;
  if (OB_UNLIKELY(ObEnumSetInnerTC != in.get_type_class())) {
    LOG_ERROR("invalid input type", K(ret), K(in));
  } else if (OB_FAIL(in.get_enumset_inner_value(inner_value))) {
    LOG_WARN("failed to get_enumset_inner_value", K(in), K(ret));
  } else {
    out.set_varchar(inner_value.string_value_);
    out.set_collation_type(in.get_collation_type());
    out.set_collation_level(in.get_collation_level());
  }
  return ret;
}
  /* used for case when or in , comment temporary */

  //static int enumset_inner_int(const ObObjType expect_type, ObObjCastParams &params,
  //                          const ObObj &in, ObObj &out, const ObCastMode cast_mode)
  //{
  //  //we regard enum as uint16 when int_value is needed
  //  int ret = OB_SUCCESS;
  //  ObObj uint64;
  //  if (OB_UNLIKELY(ObEnumSetInnerTC != in.get_type_class()
  //                  || ObIntTC != ob_obj_type_class(expect_type))) {
  //    ret = OB_ERR_UNEXPECTED;
  //    LOG_ERROR("invalid input type",
  //        K(ret), K(in), K(expect_type));
  //  } else if (OB_FAIL(get_uint64_from_enumset_inner(in, uint64))) {
//    LOG_WARN("failed to get_uint64_from_enumset_inner", K(in), K(ret));
//  } else if (OB_FAIL(uint_int(expect_type, params, uint64, out, cast_mode))) {
//    LOG_WARN("failed to cast uint to int", K(in), K(uint64), K(ret));
//  } else {/*do nothing*/}
//  return ret;
//}
//
//static int enumset_inner_uint(const ObObjType expect_type, ObObjCastParams &params,
//                           const ObObj &in, ObObj &out, const ObCastMode cast_mode)
//{
//  //we regard enum as uint16 when int_value is needed
//  int ret = OB_SUCCESS;
//  ObObj uint64;
//  if (OB_UNLIKELY(ObEnumSetInnerTC != in.get_type_class()
//                  || ObUIntTC != ob_obj_type_class(expect_type))) {
//    ret = OB_ERR_UNEXPECTED;
//    LOG_ERROR("invalid input type",
//        K(ret), K(in), K(expect_type));
//  } else if (OB_FAIL(get_uint64_from_enumset_inner(in, uint64))) {
//    LOG_WARN("failed to get_uint64_from_enumset_inner", K(in), K(ret));
//  } else if (OB_FAIL(uint_uint(expect_type, params, uint64, out, cast_mode))) {
//    LOG_WARN("failed to cast uint to uint", K(in), K(uint64), K(ret));
//  } else {/*do nothing*/}
//  return ret;
//}
//
//static int enumset_inner_float(const ObObjType expect_type, ObObjCastParams &params,
//                            const ObObj &in, ObObj &out, const ObCastMode cast_mode)
//{
//  //we regard enum as uint16 when int_value is needed
//  int ret = OB_SUCCESS;
//  ObObj uint64;
//  if (OB_UNLIKELY(ObEnumSetInnerTC != in.get_type_class()
//                  || ObFloatTC != ob_obj_type_class(expect_type))) {
//    ret = OB_ERR_UNEXPECTED;
//    LOG_ERROR("invalid input type",
//        K(ret), K(in), K(expect_type));
//  } else if (OB_FAIL(get_uint64_from_enumset_inner(in, uint64))) {
//    LOG_WARN("failed to get_uint64_from_enumset_inner", K(in), K(ret));
//  } else if (OB_FAIL(uint_float(expect_type, params, uint64, out, cast_mode))) {
//    LOG_WARN("failed to cast uint to float", K(in), K(uint64), K(ret));
//  } else {/*do nothing*/}
//  return ret;
//}
//
//static int enumset_inner_double(const ObObjType expect_type, ObObjCastParams &params,
//                             const ObObj &in, ObObj &out, const ObCastMode cast_mode)
//{
//  //we regard enum as uint16 when int_value is needed
//  int ret = OB_SUCCESS;
//  ObObj uint64;
//  if (OB_UNLIKELY(ObEnumSetInnerTC != in.get_type_class()
//                  || ObDoubleTC != ob_obj_type_class(expect_type))) {
//    ret = OB_ERR_UNEXPECTED;
//    LOG_ERROR("invalid input type",
//        K(ret), K(in), K(expect_type));
//  } else if (OB_FAIL(get_uint64_from_enumset_inner(in, uint64))) {
//    LOG_WARN("failed to get_uint64_from_enumset_inner", K(in), K(ret));
//  } else if (OB_FAIL(uint_double(expect_type, params, uint64, out, cast_mode))) {
//    LOG_WARN("failed to cast uint to double", K(in), K(uint64), K(ret));
//  } else {/*do nothing*/}
//  return ret;
//}
//
//static int enumset_inner_number(const ObObjType expect_type, ObObjCastParams &params,
//                             const ObObj &in, ObObj &out, const ObCastMode cast_mode)
//{
//  //we regard enum as uint16 when int_value is needed
//  int ret = OB_SUCCESS;
//  ObObj uint64;
//  if (OB_UNLIKELY(ObEnumSetInnerTC != in.get_type_class()
//                  || ObNumberTC != ob_obj_type_class(expect_type))) {
//    ret = OB_ERR_UNEXPECTED;
//    LOG_ERROR("invalid input type", K(in), K(expect_type), K(ret));
//  } else if (OB_FAIL(get_uint64_from_enumset_inner(in, uint64))) {
//    LOG_WARN("failed to get_uint64_from_enumset_inner", K(in), K(ret));
//  } else if (OB_FAIL(uint_number(expect_type, params, uint64, out, cast_mode))) {
//    LOG_WARN("failed to cast uint to number", K(in), K(uint64), K(ret));
//  } else {/*do nothing*/}
//  return ret;
//}

static int enumset_inner_int(const ObObjType expect_type, ObObjCastParams &params,
                             const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  //we regard enum as uint16 when int_value is needed
  int ret = OB_SUCCESS;
  ObObj uint64;
  if (OB_UNLIKELY(ObEnumSetInnerTC != in.get_type_class()
                  || ObIntTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(get_uint64_from_enumset_inner(in, uint64))) {
    LOG_WARN("failed to get_uint64_from_enumset_inner", K(in), K(ret));
  } else if (OB_FAIL(uint_int(expect_type, params, uint64, out, cast_mode))) {
    LOG_WARN("failed to cast uint to int", K(in), K(uint64), K(ret));
  } else {/*do nothing*/}
  return ret;
}

static int enumset_inner_uint(const ObObjType expect_type, ObObjCastParams &params,
                           const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  //we regard enum as uint16 when int_value is needed
  int ret = OB_SUCCESS;
  ObObj uint64;
  if (OB_UNLIKELY(ObEnumSetInnerTC != in.get_type_class()
                  || ObUIntTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(get_uint64_from_enumset_inner(in, uint64))) {
    LOG_WARN("failed to get_uint64_from_enumset_inner", K(in), K(ret));
  } else if (OB_FAIL(uint_uint(expect_type, params, uint64, out, cast_mode))) {
    LOG_WARN("failed to cast uint to uint", K(in), K(uint64), K(ret));
  } else {/*do nothing*/}
  return ret;
}

static int enumset_inner_float(const ObObjType expect_type, ObObjCastParams &params,
                            const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  //we regard enum as uint16 when int_value is needed
  int ret = OB_SUCCESS;
  ObObj uint64;
  if (OB_UNLIKELY(ObEnumSetInnerTC != in.get_type_class()
                  || ObFloatTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(get_uint64_from_enumset_inner(in, uint64))) {
    LOG_WARN("failed to get_uint64_from_enumset_inner", K(in), K(ret));
  } else if (OB_FAIL(uint_float(expect_type, params, uint64, out, cast_mode))) {
    LOG_WARN("failed to cast uint to float", K(in), K(uint64), K(ret));
  } else {/*do nothing*/}
  return ret;
}

static int enumset_inner_double(const ObObjType expect_type, ObObjCastParams &params,
                             const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  //we regard enum as uint16 when int_value is needed
  int ret = OB_SUCCESS;
  ObObj uint64;
  if (OB_UNLIKELY(ObEnumSetInnerTC != in.get_type_class()
                  || ObDoubleTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(get_uint64_from_enumset_inner(in, uint64))) {
    LOG_WARN("failed to get_uint64_from_enumset_inner", K(in), K(ret));
  } else if (OB_FAIL(uint_double(expect_type, params, uint64, out, cast_mode))) {
    LOG_WARN("failed to cast uint to double", K(in), K(uint64), K(ret));
  } else {/*do nothing*/}
  return ret;
}

static int enumset_inner_number(const ObObjType expect_type, ObObjCastParams &params,
                             const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  //we regard enum as uint16 when int_value is needed
  int ret = OB_SUCCESS;
  ObObj uint64;
  if (OB_UNLIKELY(ObEnumSetInnerTC != in.get_type_class()
                  || ObNumberTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(in), K(expect_type), K(ret));
  } else if (OB_FAIL(get_uint64_from_enumset_inner(in, uint64))) {
    LOG_WARN("failed to get_uint64_from_enumset_inner", K(in), K(ret));
  } else if (OB_FAIL(uint_number(expect_type, params, uint64, out, cast_mode))) {
    LOG_WARN("failed to cast uint to number", K(in), K(uint64), K(ret));
  } else {/*do nothing*/}
  return ret;
}


static int enumset_inner_year(const ObObjType expect_type, ObObjCastParams &params,
                           const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  //we regard enum as uint64 when int_value is needed
  int ret = OB_SUCCESS;
  ObObj uint64;
  if (OB_UNLIKELY(ObEnumSetInnerTC != in.get_type_class()
                  || ObYearTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(get_uint64_from_enumset_inner(in, uint64))) {
    LOG_WARN("failed to get_uint64_from_enumset_inner", K(in), K(ret));
  } else if (OB_FAIL(uint_year(expect_type, params, uint64, out, cast_mode))) {
    LOG_WARN("failed to cast uint to year", K(in), K(uint64), K(ret));
  } else {/*do nothing*/}
  return ret;
}

static int enumset_inner_bit(const ObObjType expect_type, ObObjCastParams &params,
                          const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  //we regard enum as uint16 when int_value is needed
  int ret = OB_SUCCESS;
  ObObj uint64;
  if (OB_UNLIKELY(ObEnumSetInnerTC != in.get_type_class()
                  || ObBitTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(get_uint64_from_enumset_inner(in, uint64))) {
    LOG_WARN("failed to get_uint64_from_enumset_inner", K(in), K(ret));
  } else if (OB_FAIL(uint_bit(expect_type, params, uint64, out, cast_mode))) {
    LOG_WARN("failed to cast uint to year", K(in), K(uint64), K(ret));
  } else {/*do nothing*/}
  return ret;
}

static int enumset_inner_datetime(const ObObjType expect_type, ObObjCastParams &params,
                               const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  //we regard enum as uint16 when int_value is needed
  int ret = OB_SUCCESS;
  ObObj str_value;
  if (OB_UNLIKELY(ObEnumSetInnerTC != in.get_type_class()
                  || ObDateTimeTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(get_string_from_enumset_inner(in, str_value))) {
    LOG_WARN("failed to get_string_from_enumset_inner", K(in), K(ret));
  } else if (OB_FAIL(string_datetime(expect_type, params, str_value, out, cast_mode))) {
    LOG_WARN("failed to cast string to datetime", K(in), K(str_value), K(ret));
  } else {/*do nothing*/}
  return ret;
}

static int enumset_inner_date(const ObObjType expect_type, ObObjCastParams &params,
                           const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  //we regard enum as uint16 when int_value is needed
  int ret = OB_SUCCESS;
  ObObj str_value;
  if (OB_UNLIKELY(ObEnumSetInnerTC != in.get_type_class()
                  || ObDateTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(get_string_from_enumset_inner(in, str_value))) {
    LOG_WARN("failed to get_string_from_enumset_inner", K(in), K(ret));
  } else if (OB_FAIL(string_date(expect_type, params, str_value, out, cast_mode))) {
    LOG_WARN("failed to cast string to date", K(in), K(str_value), K(ret));
  } else {/*do nothing*/}
  return ret;
}

static int enumset_inner_time(const ObObjType expect_type, ObObjCastParams &params,
                              const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  //we regard enum as uint16 when int_value is needed
  int ret = OB_SUCCESS;
  ObObj str_value;
  if (OB_UNLIKELY(ObEnumSetInnerTC != in.get_type_class()
                  || ObTimeTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(get_string_from_enumset_inner(in, str_value))) {
    LOG_WARN("failed to get_string_from_enumset_inner", K(in), K(ret));
  } else if (OB_FAIL(string_time(expect_type, params, str_value, out, cast_mode))) {
    LOG_WARN("failed to cast string to time", K(in), K(str_value), K(ret));
  } else {/*do nothing*/}
  return ret;
}

static int enumset_inner_string(const ObObjType expect_type, ObObjCastParams &params,
                             const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  //we regard enum as uint16 when int_value is needed
  int ret = OB_SUCCESS;
  ObObj str_value;
  if (OB_UNLIKELY(ObEnumSetInnerTC != in.get_type_class()
                  || ObStringTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(get_string_from_enumset_inner(in, str_value))) {
    LOG_WARN("failed to get_string_from_enumset_inner", K(in), K(ret));
  } else if (OB_FAIL(string_string(expect_type, params, str_value, out, cast_mode))) {
    LOG_WARN("failed to cast string to string", K(in), K(str_value), K(ret));
  } else {/*do nothing*/}
  return ret;
}


////////////////////////////////////////////////////////////
// OTimestamp -> XXX

static int otimestamp_datetime(const ObObjType expect_type, ObObjCastParams &params,
                                 const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  int64_t usec = 0;
  if (OB_UNLIKELY(ObOTimestampTC != in.get_type_class())
      || OB_UNLIKELY(ObDateTimeTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(ObTimeConverter::otimestamp_to_odate(in.get_type(), in.get_otimestamp_value(), params.dtc_params_.tz_info_, usec))) {
    LOG_WARN("fail to timestamp_tz_to_timestamp", K(ret), K(in), K(expect_type));
  } else {
    ObTimeConverter::trunc_datetime(OB_MAX_DATE_PRECISION, usec);
    out.set_datetime(expect_type, usec);
    out.set_scale(OB_MAX_DATE_PRECISION);
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, in.get_scale(), DEFAULT_LENGTH_FOR_TEMPORAL);
  UNUSED(cast_mode);
  return ret;
}

static int otimestamp_string(const ObObjType expect_type, ObObjCastParams &params,
                             const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObLength res_length = -1;
  if (OB_UNLIKELY(ObOTimestampTC != in.get_type_class())
      || OB_UNLIKELY(ObStringTC != ob_obj_type_class(expect_type)
                     && ObTextTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else {
    char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
    int64_t len = 0;
    if (OB_FAIL(ObTimeConverter::otimestamp_to_str(in.get_otimestamp_value(), params.dtc_params_, in.get_scale(),
                                                   in.get_type(), buf, OB_CAST_TO_VARCHAR_MAX_LENGTH, len))) {
      LOG_WARN("failed to convert otimestamp to string", K(ret));
    } else {
      ObString tmp_str;
      ObObj tmp_out;
      if (OB_FAIL(convert_string_collation(ObString(len, buf), ObCharset::get_system_collation(), tmp_str, params.dest_collation_, params))) {
        LOG_WARN("fail to convert string collation", K(ret));
      } else if (OB_FAIL(check_convert_string(expect_type, params, tmp_str, tmp_out))) {
        LOG_WARN("fail to check_convert_string", K(ret), K(in), K(expect_type));
      } else if (OB_FAIL(copy_string(params, expect_type, tmp_out.get_string(), out))) {
        LOG_WARN("failed to copy_string", K(ret), K(expect_type), K(len));
      } else {
        out.set_type(expect_type);
        res_length = static_cast<ObLength>(out.get_string_len());
        out.set_collation_type(params.dest_collation_);
      }
    }
  }
  SET_RES_ACCURACY_STRING(expect_type, DEFAULT_PRECISION_FOR_STRING, res_length);
  UNUSED(cast_mode);
  return ret;
}

CAST_TO_TEXT_METHOD(otimestamp, ObOTimestampTC);

static int otimestamp_otimestamp(const ObObjType expect_type, ObObjCastParams &params,
                                 const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObOTimestampData value;
  if (OB_UNLIKELY(ObOTimestampTC != in.get_type_class())
      || OB_UNLIKELY(ObOTimestampTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if (ObTimestampNanoType == in.get_type()) {
      if (OB_FAIL(ObTimeConverter::odate_to_otimestamp(in.get_otimestamp_value().time_us_,
                                                    params.dtc_params_.tz_info_, expect_type, value))) {
        LOG_WARN("fail to odate_to_otimestamp", K(ret), K(expect_type));
      } else {
        value.time_ctx_.tail_nsec_ = in.get_otimestamp_value().time_ctx_.tail_nsec_;
      }
  } else if (ObTimestampNanoType == expect_type) {
    if (OB_FAIL(ObTimeConverter::otimestamp_to_odate(in.get_type(), in.get_otimestamp_value(),
                                                     params.dtc_params_.tz_info_,
                                                     *(int64_t *)&value.time_us_))) {
      LOG_WARN("fail to otimestamp_to_odate", K(ret), K(expect_type));
    } else {
      value.time_ctx_.tail_nsec_ = in.get_otimestamp_value().time_ctx_.tail_nsec_;
    }
  } else {
    if (OB_FAIL(ObTimeConverter::otimestamp_to_otimestamp(in.get_type(), in.get_otimestamp_value(),
                                                                                                                    params.dtc_params_.tz_info_, expect_type, value))) {
      LOG_WARN("fail to otimestamp_to_otimestamp", K(ret), K(expect_type));
    }
  }

  if (OB_SUCC(ret)) {
    SET_RES_OTIMESTAMP(out);
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, in.get_scale(), DEFAULT_LENGTH_FOR_TEMPORAL);
  UNUSED(cast_mode);
  return ret;
}

////////////////////////////////////////////////////////////
// Interval -> XXX
static int interval_string(const ObObjType expect_type, ObObjCastParams &params,
                           const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
  int64_t len = 0;
  ObLength res_length = -1;
  bool is_explicit_cast = CM_IS_EXPLICIT_CAST(cast_mode);
  if (OB_UNLIKELY(!ob_is_string_type(expect_type) || ObIntervalTC != in.get_type_class())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else {
    ret = in.is_interval_ym() ?
          ObTimeConverter::interval_ym_to_str(in.get_interval_ym(), in.get_scale(), buf,
                              OB_CAST_TO_VARCHAR_MAX_LENGTH, len, is_explicit_cast) :
          ObTimeConverter::interval_ds_to_str(in.get_interval_ds(), in.get_scale(), buf,
                              OB_CAST_TO_VARCHAR_MAX_LENGTH, len, is_explicit_cast);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to convert interval to str");
    } else {
      ObString tmp_str;
      ObObj tmp_out;
      if (OB_FAIL(convert_string_collation(ObString(len, buf), ObCharset::get_system_collation(), tmp_str, params.dest_collation_, params))) {
        LOG_WARN("fail to convert string collation", K(ret));
      } else if (OB_FAIL(check_convert_string(expect_type, params, tmp_str, tmp_out))) {
        LOG_WARN("fail to check_convert_string", K(ret), K(in), K(expect_type));
      } else if (OB_FAIL(copy_string(params, expect_type, tmp_out.get_string(), out))) {
        LOG_WARN("failed to copy_string", K(ret), K(expect_type), K(len));
      } else {
        out.set_type(expect_type);
        out.set_collation_type(params.dest_collation_);
        res_length = static_cast<ObLength>(out.get_string_len());
      }
    }
  }
  SET_RES_ACCURACY_STRING(expect_type, DEFAULT_PRECISION_FOR_STRING, res_length);
  UNUSED(cast_mode);
  return ret;
}

static int interval_interval(const ObObjType expect_type, ObObjCastParams &params,
                             const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObIntervalTC != ob_obj_type_class(expect_type) || ObIntervalTC != in.get_type_class())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if (OB_UNLIKELY(expect_type != in.get_type())) {
    ret = cast_inconsistent_types(expect_type, params, in, out, cast_mode);
  } else {
    if (&in != &out) {
      out = in;
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////
// Raw -> XXX

// 这里的 string 不包括 lob 类型
static int raw_string(const ObObjType expect_type, ObObjCastParams &params,
                      const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  UNUSED(cast_mode);
  int ret = OB_SUCCESS;
  ObLength res_length = -1;
  ObString tmp_str;
  ObObj tmp_obj;

  if (OB_UNLIKELY((ObRawTC != in.get_type_class()) || OB_UNLIKELY(ObStringTC != ob_obj_type_class(expect_type)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(ObHexUtils::rawtohex(in, params, tmp_obj))) {
    LOG_WARN("fail to rawtohex", K(ret), K(in), K(expect_type));
  } else {
    if (!ObCharset::is_cs_nonascii(params.dest_collation_)) {
      out.set_string(expect_type, tmp_obj.get_string_ptr(), tmp_obj.get_string_len());
      out.set_collation_type(params.dest_collation_);
      res_length = static_cast<ObLength>(out.get_string_len());
    } else {
      if (OB_FAIL(convert_string_collation(tmp_obj.get_string(),
                                                  ObCharset::get_system_collation(),
                                                  tmp_str,
                                                  params.dest_collation_,
                                                  params))) {
        LOG_WARN("failed to convert string collation", K(ret));
      } else {
        out.set_string(expect_type, tmp_str.ptr(), tmp_str.length());
        out.set_collation_type(params.dest_collation_);
        res_length = static_cast<ObLength>(out.get_string_len());
      }
    }
  }

  SET_RES_ACCURACY_STRING(expect_type, DEFAULT_PRECISION_FOR_STRING, res_length);

  return ret;
}

// lob locator v1 raw to text type
static int raw_longtext(const ObObjType expect_type, ObObjCastParams &params,
                        const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  UNUSED(cast_mode);
  int ret = OB_SUCCESS;
  ObLength res_length = -1;
  ObString tmp_str;
  ObObj tmp_obj;

  if (OB_UNLIKELY((ObRawTC != in.get_type_class())
      || OB_UNLIKELY(!ob_is_text_tc(expect_type)))) {
     ret = OB_ERR_UNEXPECTED;
     LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if (ob_is_clob(expect_type, params.expect_obj_collation_)) {
    // raw to clob
    if (OB_FAIL(ObHexUtils::rawtohex(in, params, tmp_obj))) {
      LOG_WARN("fail to rawtohex", K(ret), K(in), K(expect_type));
    } else {
      if (!ObCharset::is_cs_nonascii(params.expect_obj_collation_)) {
        out.set_varchar_value(tmp_obj.get_string_ptr(), tmp_obj.get_string_len());
        out.set_collation_type(params.expect_obj_collation_);
      } else {
        if (OB_FAIL(convert_string_collation(tmp_obj.get_string(),
                                             ObCharset::get_system_collation(),
                                             tmp_str,
                                             params.expect_obj_collation_,
                                             params))) {
          LOG_WARN("failed to convert string collation", K(ret));
        } else {
          out.set_varchar_value(tmp_str.ptr(), tmp_str.length());
          out.set_collation_type(params.expect_obj_collation_);
        }
      }
    }
  } else if (ob_is_blob(expect_type, params.expect_obj_collation_)) {
    // raw to blob
    if (OB_FAIL(copy_string(params, expect_type, in.get_string(), out))) {
      LOG_WARN("failed to copy string", K(ret), K(in), K(expect_type));
    }
  }
  if (OB_SUCC(ret)) {
    out.set_type(expect_type);
    out.set_inrow();
    out.set_collation_type(params.expect_obj_collation_);
    out.set_collation_level(CS_LEVEL_IMPLICIT);
    res_length = static_cast<ObLength>(out.get_string_len());
  }
  SET_RES_ACCURACY_STRING(expect_type, DEFAULT_PRECISION_FOR_STRING, res_length);

  return ret;
}

static int raw_lob(const ObObjType expect_type, ObObjCastParams &params,
                   const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObObj tmp_val;

  if (OB_UNLIKELY((ObRawTC != in.get_type_class())
      || OB_UNLIKELY(ObLobTC != ob_obj_type_class(expect_type)))) {
     ret = OB_ERR_UNEXPECTED;
     LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(raw_longtext(ObLongTextType, params, in, tmp_val, cast_mode))) {
    LOG_WARN("fail to cast raw to longtext", K(ret), K(expect_type), K(in), K(tmp_val));
  } else if (OB_FAIL(string_lob(expect_type, params, tmp_val, out, cast_mode))) {
    LOG_WARN("fail to cast string to lob", K(ret), K(expect_type), K(in), K(tmp_val));
  }
  return ret;
}

static int raw_raw(const ObObjType expect_type, ObObjCastParams &params,
                   const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  UNUSED(cast_mode);
  int ret = OB_SUCCESS;
  ObLength res_length = -1;
  ObObj tmp_out;

  if (OB_UNLIKELY((ObRawTC != in.get_type_class()) || OB_UNLIKELY(ObRawTC != ob_obj_type_class(expect_type)))) {
     ret = OB_ERR_UNEXPECTED;
     LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(check_convert_string(expect_type, params, in, tmp_out))) {
    LOG_WARN("failed to check_and_convert_string", K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(copy_string(params, expect_type, tmp_out.get_string(), out))) {
    LOG_WARN("failed to copy string", K(ret), K(in), K(expect_type));
  } else {
    out.set_type(expect_type);
    res_length = static_cast<ObLength>(out.get_string_len());
  }
  SET_RES_ACCURACY_STRING(expect_type, DEFAULT_PRECISION_FOR_STRING, res_length);

  return ret;
}

// lob locator v2 raw to text type
static int raw_text(const ObObjType expect_type, ObObjCastParams &params,
                    const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObObj tmp_out;
  if (OB_FAIL(raw_longtext(expect_type, params, in, tmp_out, cast_mode))) {
    LOG_WARN("fail to cast raw to longtext", K(ret), K(expect_type), K(in), K(tmp_out));
  } else {
    bool has_lob_header = (!IS_CLUSTER_VERSION_BEFORE_4_1_0_0 && (expect_type != ObTinyTextType));
    // refine later, avoid mem alloc and copy
    ObString res_str = tmp_out.get_string();
    sql::ObTextStringObObjResult str_result(expect_type, &params, &out, has_lob_header);
    if (OB_FAIL(str_result.init(res_str.length()))) {
      LOG_WARN("init lob result failed");
    } else if (OB_FAIL(str_result.append(res_str.ptr(), res_str.length()))) {
      LOG_WARN("append lob result failed");
    } else {
      out = tmp_out; // copy meta
      str_result.set_result();
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////
// rowid -> XXX
static int rowid_string(const ObObjType expect_type, ObObjCastParams &params,
                        const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  UNUSED(cast_mode);
  int ret = OB_SUCCESS;
  ObLength res_length = -1;
  if (OB_UNLIKELY(ObRowIDTC != in.get_type_class()) ||
      OB_UNLIKELY(ObStringTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid input type", K(ret), K(in), K(expect_type));
  } else if (!in.is_urowid()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support rowid for now", K(ret));
  } else {
    int64_t pos = 0;
    char *base64_buf = NULL;
    int64_t base64_buf_len = in.get_urowid().needed_base64_buffer_size();
    if (OB_ISNULL(base64_buf = (char *)(params.alloc(base64_buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret));
    } else if (OB_FAIL(in.get_urowid().get_base64_str(base64_buf, base64_buf_len, pos))) {
      LOG_WARN("failed to get base64 string", K(ret));
    } else {
      ObString tmp_str;
      ObObj tmp_out;
      if (OB_FAIL(convert_string_collation(ObString(base64_buf_len, base64_buf),
                                           ObCharset::get_system_collation(),
                                           tmp_str,
                                           params.dest_collation_,
                                           params))) {
        LOG_WARN("failed to convert string collation", K(ret));
      } else if (OB_FAIL(check_convert_string(expect_type, params, tmp_str, tmp_out))) {
        LOG_WARN("failed to check convert string", K(ret));
      } else if (OB_FAIL(copy_string(params, expect_type, tmp_out.get_string(), out))) {
        LOG_WARN("failed to copy_string", K(ret), K(expect_type));
      } else {
        out.set_type(expect_type);
        out.set_collation_type(params.dest_collation_);
        res_length = static_cast<ObLength>(out.get_string_len());
      }
    }
  }
  SET_RES_ACCURACY_STRING(expect_type, DEFAULT_PRECISION_FOR_STRING, res_length);
  return ret;
}

static int rowid_rowid(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObRowIDTC != in.get_type_class()) ||
      OB_UNLIKELY(ObRowIDTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid input type", K(ret), K(in), K(expect_type));
  } else if (OB_UNLIKELY(ObURowIDType != expect_type || ObURowIDType != in.get_type())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("only support urowid type for now", K(ret));
  } else {
    UNUSED(params);
    UNUSED(cast_mode);
    if (&in != &out) {
      out = in;
    }
  }
  return ret;
}

// Text -> XXX
#define CAST_TEXT_TO_OTHER_TYPE(TYPE, TYPE_CLASS)                                   \
static int text_##TYPE(const ObObjType expect_type, ObObjCastParams &params,        \
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)     \
{                                                                                   \
  int ret = OB_SUCCESS;                                                             \
  if (OB_UNLIKELY(ObTextTC != in.get_type_class()                                   \
                  || TYPE_CLASS != ob_obj_type_class(expect_type))) {               \
    ret = OB_ERR_UNEXPECTED;                                                        \
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));       \
  } else if (lib::is_oracle_mode() && in.is_blob() && !ob_is_raw_tc(expect_type)) { \
    ret = OB_NOT_SUPPORTED;                                                         \
    LOG_ERROR("invalid use of blob type", K(ret), K(in), K(expect_type)); \
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Blob cast to other type");                    \
  } else {                                                                          \
    ObString in_str;                                                                \
    ObTextStringIter instr_iter(in);                                                \
    if (OB_FAIL(instr_iter.init(0, NULL, params.allocator_v2_))) {                  \
      LOG_WARN("init lob str iter failed ", K(ret), K(in));               \
    } else if (OB_FAIL(instr_iter.get_inrow_or_outrow_prefix_data(in_str))) {       \
      LOG_WARN("get full data from lob str iter failed ", K(ret), K(in)); \
    } else {                                                                        \
        ObObj tmp_obj;                                                              \
        tmp_obj.set_varchar(in_str);                                                \
        tmp_obj.set_collation_type(in.get_collation_type());                        \
        if (OB_FAIL(string_##TYPE(expect_type, params, tmp_obj, out, cast_mode))) { \
          LOG_WARN("Text to " #TYPE "failed", K(ret), K(tmp_obj));        \
        }                                                                           \
      }                                                                             \
    }                                                                               \
  return ret;                                                                       \
}

CAST_TEXT_TO_OTHER_TYPE(int, ObIntTC);
CAST_TEXT_TO_OTHER_TYPE(uint, ObUIntTC);
CAST_TEXT_TO_OTHER_TYPE(double, ObDoubleTC);
CAST_TEXT_TO_OTHER_TYPE(float, ObFloatTC);
CAST_TEXT_TO_OTHER_TYPE(number, ObNumberTC);
CAST_TEXT_TO_OTHER_TYPE(datetime, ObDateTimeTC);
CAST_TEXT_TO_OTHER_TYPE(date, ObDateTC);
CAST_TEXT_TO_OTHER_TYPE(time, ObTimeTC);
CAST_TEXT_TO_OTHER_TYPE(year, ObYearTC);
CAST_TEXT_TO_OTHER_TYPE(bit, ObBitTC);
CAST_TEXT_TO_OTHER_TYPE(otimestamp, ObOTimestampTC);
CAST_TEXT_TO_OTHER_TYPE(interval, ObIntervalTC);
CAST_TEXT_TO_OTHER_TYPE(rowid, ObRowIDTC);

static int text_raw(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObTextTC != in.get_type_class()
                  || ObRawTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if (lib::is_oracle_mode() && in.is_blob() && !ob_is_raw_tc(expect_type)) {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("invalid use of blob type", K(ret), K(in), K(expect_type));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Blob cast to other type");
  } else {
    ObString in_str;
    ObTextStringIter instr_iter(in);
    if (OB_FAIL(instr_iter.init(0, NULL, params.allocator_v2_))) {
      LOG_WARN("init lob str iter failed ", K(ret), K(in));
    } else if (OB_FAIL(instr_iter.get_full_data(in_str))) {
      LOG_WARN("get full data from lob str iter failed ", K(ret), K(in));
    } else {
        ObObj tmp_obj;
        tmp_obj.set_varchar(in_str);
        tmp_obj.set_collation_type(in.get_collation_type());
        if (CM_IS_BLOB_CAST_ENABLED(cast_mode)) { // col cov in pl
          if (in_str.length() > 0) {
            char *buf = nullptr;
            if (OB_ISNULL(params.allocator_v2_)) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("allocator in cast ctx is NULL", K(ret));
            } else if (OB_ISNULL(buf = static_cast<char *>(params.allocator_v2_->alloc(in_str.length())))) {
              out.set_null();
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_ERROR("alloc memory failed", K(ret), K(in_str.length()));
            } else {
              MEMCPY(buf, in_str.ptr(), in_str.length());
              out.set_raw(buf, in_str.length());
            }
          } else {
            out.set_null();
          }
        } else {
          if (OB_FAIL(string_raw(expect_type, params, tmp_obj, out, cast_mode))) {
            LOG_WARN("Text to raw failed", K(ret), K(tmp_obj));
          }
        }
      }
    }
  return ret;
}

////////////////////////////////////////////////////////////
// Lob -> XXX
#define CAST_LOB_TO_OTHER_TYPE(TYPE, TYPE_CLASS)  \
static int lob_##TYPE(const ObObjType expect_type, ObObjCastParams &params,         \
                      const ObObj &in, ObObj &out, const ObCastMode cast_mode)      \
{                                 \
  int ret = OB_SUCCESS;           \
  if (OB_UNLIKELY(ObLobTC != in.get_type_class()                          \
                  || TYPE_CLASS != ob_obj_type_class(expect_type))) {     \
    ret = OB_ERR_UNEXPECTED;                                              \
    LOG_ERROR("invalid input type",                                       \
        K(ret), K(in), K(expect_type));                                   \
  } else if (lib::is_oracle_mode() && in.is_blob_locator()) {             \
    ret = OB_NOT_SUPPORTED;                                               \
    LOG_ERROR("invalid use of blob type", K(ret), K(in), K(expect_type)); \
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Blob cast to other type");          \
  } else {                                                                \
    ObString str;                                                            \
    if (OB_FAIL(in.get_string(str))) {                          \
      STORAGE_LOG(WARN, "Failed to get payload from lob locator", K(ret), K(in)); \
    } else {                                                \
      ObObj tmp_obj;                                        \
      tmp_obj.set_varchar(str);                             \
      tmp_obj.set_collation_type(in.get_collation_type());  \
      if (OB_FAIL(string_##TYPE(expect_type, params, tmp_obj, out, cast_mode))) {   \
        LOG_WARN("string to " #TYPE "failed", K(ret), K(tmp_obj));                       \
      }                                                     \
    }                                                     \
  }                                                       \
  return ret;                                             \
}

CAST_LOB_TO_OTHER_TYPE(int, ObIntTC);
CAST_LOB_TO_OTHER_TYPE(uint, ObUIntTC);
CAST_LOB_TO_OTHER_TYPE(double, ObDoubleTC);
CAST_LOB_TO_OTHER_TYPE(float, ObFloatTC);
CAST_LOB_TO_OTHER_TYPE(number, ObNumberTC);
CAST_LOB_TO_OTHER_TYPE(datetime, ObDateTimeTC);
CAST_LOB_TO_OTHER_TYPE(date, ObDateTC);
CAST_LOB_TO_OTHER_TYPE(time, ObTimeTC);
CAST_LOB_TO_OTHER_TYPE(year, ObYearTC);
CAST_LOB_TO_OTHER_TYPE(bit, ObBitTC);
CAST_LOB_TO_OTHER_TYPE(otimestamp, ObOTimestampTC);
CAST_LOB_TO_OTHER_TYPE(interval, ObIntervalTC);
CAST_LOB_TO_OTHER_TYPE(rowid, ObRowIDTC);

static int lob_raw(const ObObjType expect_type, ObObjCastParams &params,
                   const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  UNUSED(cast_mode);
  int ret = OB_SUCCESS;
  ObObj tmp_out;
  ObString in_str;
  if (OB_UNLIKELY(ObLobTC != in.get_type_class()
      || OB_UNLIKELY(ObRawTC != ob_obj_type_class(expect_type)))) {
     ret = OB_ERR_UNEXPECTED;
     LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if (in.is_clob() || in.is_clob_locator()) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("cast clob to raw not allowed", K(ret));
  } else if (OB_FAIL(in.get_string(in_str))) {
    LOG_WARN("get string failed", K(ret));
  } else {
    out.set_string(ObRawType, in_str);
    if (OB_FAIL(ob_write_obj(*params.allocator_v2_, out, out))) {
      LOG_WARN("fail to write obj", K(ret));
    }
  }
  return ret;
}

static int lob_string(const ObObjType expect_type, ObObjCastParams &params,
                      const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObObj tmp_out;
  ObString in_str;
  if (OB_UNLIKELY(ObLobTC != in.get_type_class()
      || OB_UNLIKELY(ObStringTC != ob_obj_type_class(expect_type)
                     && ObTextTC != ob_obj_type_class(expect_type)))) {
     ret = OB_ERR_UNEXPECTED;
     LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(in.get_string(in_str))) {
    LOG_WARN("get string failed", K(ret));
  } else {
    ObObj tmp_in = in;
    tmp_in.set_string(ObLongTextType, in_str);
    tmp_in.set_inrow();
    if (OB_FAIL(string_string(expect_type, params, tmp_in, out, cast_mode))) {
      LOG_WARN("cast string to string failed", K(ret));
    }
  }
  return ret;
}

static int lob_text(const ObObjType expect_type, ObObjCastParams &params,
                    const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObObj tmp_out;
  ObString in_str;
  if (OB_UNLIKELY(ObLobTC != in.get_type_class()
      || OB_UNLIKELY(ObTextTC != ob_obj_type_class(expect_type)))) {
     ret = OB_ERR_UNEXPECTED;
     LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(in.get_string(in_str))) {
    LOG_WARN("get string failed", K(ret));
  } else {
    ObObj tmp_in = in;
    tmp_in.set_string(ObVarcharType, in_str);
    if (OB_FAIL(string_text(expect_type, params, tmp_in, out, cast_mode, NULL))) {
      LOG_WARN("cast string to text in lob_text failed", K(ret));
    }
  }
  return ret;
}

static int lob_lob(const ObObjType expect_type, ObObjCastParams &params,
                   const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObLobTC != in.get_type_class()
                  || ObLobTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (in.get_collation_type() == params.expect_obj_collation_) {
    out = in;
  } else if (lib::is_oracle_mode() && in.is_blob_locator()) {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("invalid use of blob type", K(ret), K(in), K(expect_type));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Blob cast to other type");
  } else if (lib::is_oracle_mode()
             && in.is_clob_locator()
             && CS_TYPE_BINARY == params.expect_obj_collation_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Can't convert collation type of clob to binary",
             K(ret), K(in), K(expect_type), K(params.expect_obj_collation_));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Convert collation type of clob to binary");
  } else {
    ObString str;
    if (OB_FAIL(in.get_string(str))) {
      STORAGE_LOG(WARN, "Failed to get payload from lob locator", K(ret), K(in));
    } else {
      ObObj tmp_obj;
      tmp_obj.set_varchar(str);
      tmp_obj.set_collation_type(in.get_collation_type());
      if (OB_FAIL(string_lob(expect_type, params, tmp_obj, out, cast_mode, in.get_lob_locator()))) {
        LOG_WARN("string to rowid failed", K(ret), K(tmp_obj));
      }
    }
  }
  return ret;
}

static int lob_json(const ObObjType expect_type, ObObjCastParams &params,
                    const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObObj tmp_out;
  ObString in_str;
  if (OB_UNLIKELY(ObLobTC != in.get_type_class() || ObJsonType != expect_type)) {
     ret = OB_ERR_UNEXPECTED;
     LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(in.get_string(in_str))) {
    LOG_WARN("get string failed", K(ret));
  } else {
    ObObj tmp_in = in;
    tmp_in.set_string(ObLongTextType, in_str);
    tmp_in.set_inrow();
    tmp_in.set_collation_type(params.expect_obj_collation_);
    if (OB_FAIL(string_json(expect_type, params, tmp_in, out, cast_mode))) {
      LOG_WARN("cast string to string failed", K(ret));
    }
  }
  return ret;
}

static int lob_geometry(const ObObjType expect_type, ObObjCastParams &params,
                        const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(params.allocator_v2_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid allocator", K(ret));
  } else {
    ObIAllocator &temp_allocator = *params.allocator_v2_;
    ObGeometry *geo = NULL;
    omt::ObSrsCacheGuard srs_guard;
    const ObSrsItem *srs = NULL;
    ObGeoType dst_geo_type = ObGeoCastUtils::get_geo_type_from_cast_mode(cast_mode);
    const char *cast_name = ObGeometryTypeCastUtil::get_cast_name(dst_geo_type);
    ObString in_str;
    if (OB_FAIL(in.get_string(in_str))) {
      LOG_WARN("fail to get string", K(ret));
    } else if (OB_FAIL(common_get_srs_item(srs_guard, in_str, srs, cast_name))) {
      LOG_WARN("fail to get srs item", K(ret), K(in_str));
    } else if (OB_FAIL(common_build_geometry(temp_allocator, in_str, geo, srs, cast_name))) {
      LOG_WARN("fail to parse geometry", K(ret), K(in_str), K(dst_geo_type));
    } else if (ObGeoType::GEOMETRY == dst_geo_type) {
      out.set_string(expect_type, in_str.ptr(), in_str.length());
      SET_RES_ACCURACY_STRING(expect_type, DEFAULT_PRECISION_FOR_STRING, in_str.length());
    } else if (OB_FAIL(geometry_geometry(expect_type, params, in, out, cast_mode))) {
      LOG_WARN("fail to cast geometry", K(ret), K(dst_geo_type));
    }

    if (OB_FAIL(ret) && CM_IS_COLUMN_CONVERT(cast_mode) && ObGeoType::GEOMETRY == dst_geo_type) { // adapt mysql
      ret = OB_ERR_CANT_CREATE_GEOMETRY_OBJECT;
      LOG_USER_ERROR(OB_ERR_CANT_CREATE_GEOMETRY_OBJECT);
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////
// json -> XXX

static int json_int(const ObObjType expect_type, ObObjCastParams &params,
                    const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObPrecision res_precision = -1;
  // ToDo convert json to other none string type is also not allowed.
  if (OB_UNLIKELY(ObJsonType != in.get_type() || ObIntTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if(OB_UNLIKELY(params.allocator_v2_ == NULL)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid allocator in json cast to other type", K(ret), K(params.allocator_v2_));
  } else {
    int64_t value = 0;
    ObString j_bin_str = in.get_string();
    if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(params.allocator_v2_, in, j_bin_str))) {
      LOG_WARN("fail to get real data.", K(ret), K(j_bin_str));
    } else {
      ObJsonBin j_bin(j_bin_str.ptr(), j_bin_str.length());
      ObIJsonBase *j_base = &j_bin;
      if (OB_FAIL(j_bin.reset_iter())) {
        LOG_WARN("failed to reset json bin iter", K(ret), K(j_bin_str));
      } else if (CAST_FAIL(j_base->to_int(value))) {
        LOG_WARN("fail to cast json to other type", K(ret), K(j_bin_str), K(expect_type));
        ret = OB_ERR_INVALID_JSON_VALUE_FOR_CAST;
        LOG_USER_ERROR(OB_ERR_INVALID_JSON_VALUE_FOR_CAST);
      } else if (expect_type < ObIntType && CAST_FAIL(int_range_check(expect_type, value, value))) {
        LOG_WARN("range check failed", K(ret), K(expect_type), K(value));
      } else {
        SET_RES_INT(out);
        res_precision = get_precision_for_integer(value);
      }
    }
  }
  SET_RES_ACCURACY(res_precision, DEFAULT_SCALE_FOR_INTEGER, DEFAULT_LENGTH_FOR_NUMERIC);
  return ret;
}

static int json_uint(const ObObjType expect_type, ObObjCastParams &params,
                    const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObPrecision res_precision = -1;
  // ToDo convert json to other none string type is also not allowed.
  if (OB_UNLIKELY(ObJsonType != in.get_type() || ObUIntTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if(OB_UNLIKELY(params.allocator_v2_ == NULL)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid allocator in json cast to other type", K(ret), K(params.allocator_v2_));
  } else {
    uint64_t value = 0;
    ObString j_bin_str = in.get_string();
    if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(params.allocator_v2_, in, j_bin_str))) {
      LOG_WARN("fail to get real data.", K(ret), K(j_bin_str));
    } else {
      ObJsonBin j_bin(j_bin_str.ptr(), j_bin_str.length());
      ObIJsonBase *j_base = &j_bin;
      if (OB_FAIL(j_bin.reset_iter())) {
        LOG_WARN("failed to reset json bin iter", K(ret), K(j_bin_str));
      } else if (CAST_FAIL(j_base->to_uint(value))) {
        LOG_WARN("fail to cast json to other type", K(ret), K(j_bin_str), K(expect_type));
        ret = OB_ERR_INVALID_JSON_VALUE_FOR_CAST;
        LOG_USER_ERROR(OB_ERR_INVALID_JSON_VALUE_FOR_CAST);
      } else if (CAST_FAIL(uint_upper_check(expect_type, value))) {
        LOG_WARN("range check failed", K(ret), K(expect_type), K(value));
      } else {
        SET_RES_UINT(out);
        res_precision = get_precision_for_integer(value);
      }
    }
  }
  SET_RES_ACCURACY(res_precision, DEFAULT_SCALE_FOR_INTEGER, DEFAULT_LENGTH_FOR_NUMERIC);
  return ret;
}

static int json_double(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode);
static int json_float(const ObObjType expect_type, ObObjCastParams &params,
                      const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObJsonType != in.get_type() || ObFloatTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else {
    ObObj obj_double;
    if (OB_FAIL(json_double(ObDoubleType, params, in, obj_double, cast_mode))) {
      LOG_WARN("json to double convert failed in json to float convert",
               K(ret), K(cast_mode));
    } else if (OB_FAIL(double_float(expect_type, params, obj_double, out, cast_mode))) {
      LOG_WARN("double to float convert failed in json to float convert",
               K(ret), K(obj_double), K(cast_mode));
    }
  }

  return ret;
}

static int json_double(const ObObjType expect_type, ObObjCastParams &params,
                    const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObPrecision res_precision = -1;
  // ToDo convert json to other none string type is also not allowed.
  if (OB_UNLIKELY(ObJsonType != in.get_type() || ObDoubleTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if(OB_UNLIKELY(params.allocator_v2_ == NULL)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid allocator in json cast to other type", K(ret), K(params.allocator_v2_));
  } else {
    double value = 0.0;
    ObString j_bin_str = in.get_string();
    if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(params.allocator_v2_, in, j_bin_str))) {
      LOG_WARN("fail to get real data.", K(ret), K(j_bin_str));
    } else {
      ObJsonBin j_bin(j_bin_str.ptr(), j_bin_str.length());
      ObIJsonBase *j_base = &j_bin;
      if (OB_FAIL(j_bin.reset_iter())) {
        LOG_WARN("failed to reset json bin iter", K(ret), K(j_bin_str));
      } else if (CAST_FAIL(j_base->to_double(value))) {
        LOG_WARN("fail to cast json to other type", K(ret), K(j_bin_str), K(expect_type));
        ret = OB_ERR_INVALID_JSON_VALUE_FOR_CAST;
        LOG_USER_ERROR(OB_ERR_INVALID_JSON_VALUE_FOR_CAST);
      } else if (ObUDoubleType == expect_type && CAST_FAIL(numeric_negative_check(value))) {
        LOG_WARN("numeric_negative_check failed", K(ret), K(expect_type), K(value));
      } else {
        SET_RES_DOUBLE(out);
        res_precision = get_precision_for_integer(value);
      }
    }
  }
  SET_RES_ACCURACY(PRECISION_UNKNOWN_YET, SCALE_UNKNOWN_YET, LENGTH_UNKNOWN_YET);
  return ret;
}

static int json_number(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  // ToDo convert json to other none string type is also not allowed.
  if (OB_UNLIKELY(ObJsonType != in.get_type() || ObNumberTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if(OB_UNLIKELY(params.allocator_v2_ == NULL)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid allocator in json cast to other type", K(ret), K(params.allocator_v2_));
  } else {
    number::ObNumber value;
    ObString j_bin_str = in.get_string();
    if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(params.allocator_v2_, in, j_bin_str))) {
      LOG_WARN("fail to get real data.", K(ret), K(j_bin_str));
    } else {
      ObJsonBin j_bin(j_bin_str.ptr(), j_bin_str.length());
      ObIJsonBase *j_base = &j_bin;
      if (OB_FAIL(j_bin.reset_iter())) {
        LOG_WARN("failed to reset json bin iter", K(ret), K(j_bin_str));
      } else if (CAST_FAIL(j_base->to_number(params.allocator_v2_, value))) {
        LOG_WARN("fail to cast json to other type", K(ret), K(j_bin_str), K(expect_type));
        ret = OB_ERR_INVALID_JSON_VALUE_FOR_CAST;
        LOG_USER_ERROR(OB_ERR_INVALID_JSON_VALUE_FOR_CAST);
      } else if (ObUNumberType == expect_type && CAST_FAIL(numeric_negative_check(value))) {
        LOG_WARN("numeric_negative_check failed", K(ret), K(expect_type), K(value));
      } else {
        SET_RES_NUMBER(out);
      }
    }
  }
  SET_RES_ACCURACY(PRECISION_UNKNOWN_YET, SCALE_UNKNOWN_YET, LENGTH_UNKNOWN_YET);
  return ret;
}

static int json_datetime(const ObObjType expect_type, ObObjCastParams &params,
                         const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  // ToDo convert json to other none string type is also not allowed.
  if (OB_UNLIKELY(ObJsonType != in.get_type() || ObDateTimeTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if(OB_UNLIKELY(params.allocator_v2_ == NULL)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid allocator in json cast to other type", K(ret), K(params.allocator_v2_));
  } else {
    int64_t value;
    ObString j_bin_str = in.get_string();
    if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(params.allocator_v2_, in, j_bin_str))) {
      LOG_WARN("fail to get real data.", K(ret), K(j_bin_str));
    } else {
      ObJsonBin j_bin(j_bin_str.ptr(), j_bin_str.length());
      ObIJsonBase *j_base = &j_bin;
      if (OB_FAIL(j_bin.reset_iter())) {
        LOG_WARN("failed to reset json bin iter", K(ret), K(j_bin_str));
      } else if (CAST_FAIL(j_base->to_datetime(value))) {
        LOG_WARN("fail to cast json to other type", K(ret), K(j_bin_str), K(expect_type));
        ret = OB_ERR_INVALID_JSON_VALUE_FOR_CAST;
        LOG_USER_ERROR(OB_ERR_INVALID_JSON_VALUE_FOR_CAST);
      } else {
        SET_RES_DATETIME(out);
      }
    }
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, MIN_SCALE_FOR_TEMPORAL, DEFAULT_LENGTH_FOR_TEMPORAL);
  return ret;
}

static int json_date(const ObObjType expect_type, ObObjCastParams &params,
                     const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  // ToDo convert json to other none string type is also not allowed.
  if (OB_UNLIKELY(ObJsonType != in.get_type() || ObDateTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if(OB_UNLIKELY(params.allocator_v2_ == NULL)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid allocator in json cast to other type", K(ret), K(params.allocator_v2_));
  } else {
    int32_t value;
    ObString j_bin_str = in.get_string();
    if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(params.allocator_v2_, in, j_bin_str))) {
      LOG_WARN("fail to get real data.", K(ret), K(j_bin_str));
    } else {
      ObJsonBin j_bin(j_bin_str.ptr(), j_bin_str.length());
      ObIJsonBase *j_base = &j_bin;
      if (OB_FAIL(j_bin.reset_iter())) {
        LOG_WARN("failed to reset json bin iter", K(ret), K(j_bin_str));
      } else if (CAST_FAIL(j_base->to_date(value))) {
        LOG_WARN("fail to cast json to other type", K(ret), K(j_bin_str), K(expect_type));
        ret = OB_ERR_INVALID_JSON_VALUE_FOR_CAST;
        LOG_USER_ERROR(OB_ERR_INVALID_JSON_VALUE_FOR_CAST);
      } else {
        SET_RES_DATE(out);
      }
    }
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, DEFAULT_SCALE_FOR_DATE, DEFAULT_LENGTH_FOR_TEMPORAL);
  return ret;
}

static int json_time(const ObObjType expect_type, ObObjCastParams &params,
                     const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  // ToDo convert json to other none string type is also not allowed.
  if (OB_UNLIKELY(ObJsonType != in.get_type() || ObTimeTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if(OB_UNLIKELY(params.allocator_v2_ == NULL)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid allocator in json cast to other type", K(ret), K(params.allocator_v2_));
  } else {
    int64_t value;
    ObString j_bin_str = in.get_string();
    if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(params.allocator_v2_, in, j_bin_str))) {
      LOG_WARN("fail to get real data.", K(ret), K(j_bin_str));
    } else {
      ObJsonBin j_bin(j_bin_str.ptr(), j_bin_str.length());
      ObIJsonBase *j_base = &j_bin;
      if (OB_FAIL(j_bin.reset_iter())) {
        LOG_WARN("failed to reset json bin iter", K(ret), K(j_bin_str));
      } else if (CAST_FAIL(j_base->to_time(value))) {
        LOG_WARN("fail to cast json to other type", K(ret), K(j_bin_str), K(expect_type));
        ret = OB_ERR_INVALID_JSON_VALUE_FOR_CAST;
        LOG_USER_ERROR(OB_ERR_INVALID_JSON_VALUE_FOR_CAST);
      } else {
        SET_RES_TIME(out);
      }
    }
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, MIN_SCALE_FOR_TEMPORAL, DEFAULT_LENGTH_FOR_TEMPORAL);
  return ret;
}

static int json_year(const ObObjType expect_type, ObObjCastParams &params,
                     const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  // ToDo convert json to other none string type is also not allowed.
  if (OB_UNLIKELY(ObJsonType != in.get_type() || ObYearTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if(OB_UNLIKELY(params.allocator_v2_ == NULL)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid allocator in json cast to other type", K(ret), K(params.allocator_v2_));
  } else {
    uint8_t value = 0;
    int64_t int_value = 0;
    ObString j_bin_str = in.get_string();
    if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(params.allocator_v2_, in, j_bin_str))) {
      LOG_WARN("fail to get real data.", K(ret), K(j_bin_str));
    } else {
      ObJsonBin j_bin(j_bin_str.ptr(), j_bin_str.length());
      ObIJsonBase *j_base = &j_bin;
      if (OB_FAIL(j_bin.reset_iter())) {
        LOG_WARN("failed to reset json bin iter", K(ret), K(j_bin_str));
      } else if (CAST_FAIL(j_base->to_int(int_value, false, true))) {
        LOG_WARN("fail to cast json to other type", K(ret), K(j_bin_str), K(expect_type));
        ret = OB_ERR_INVALID_JSON_VALUE_FOR_CAST;
        LOG_USER_ERROR(OB_ERR_INVALID_JSON_VALUE_FOR_CAST);
      } else if (CAST_FAIL(ObTimeConverter::int_to_year(int_value, value))){
        LOG_WARN("fail to cast json int to year type", K(ret), K(int_value), K(expect_type));
      } else {
        if (lib::is_mysql_mode() && (params.warning_ == OB_DATA_OUT_OF_RANGE)) {
          out.set_null(); // not change the behavior of int_year
        } else {
          SET_RES_YEAR(out);
        }
      }
    }
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, DEFAULT_SCALE_FOR_YEAR, DEFAULT_LENGTH_FOR_TEMPORAL);
  return ret;
}

static int json_raw(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObLength res_length = -1;
  ObObj t_out;
  // ToDo convert json to other none string type is also not allowed.
  if (OB_UNLIKELY(ObJsonType != in.get_type() ||
      OB_UNLIKELY(ObRawTC != ob_obj_type_class(expect_type)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if (in.is_json_outrow()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid cast of out row json obj", K(ret), K(in), K(out.get_meta()), K(expect_type), K(cast_mode));
  } else if(OB_UNLIKELY(params.allocator_v2_ == NULL)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid allocator in json cast to other type", K(ret), K(params.allocator_v2_));
  } else {
    ObJsonBuffer j_buf(params.allocator_v2_);
    ObString j_bin_str = in.get_string();
    ObJsonBin j_bin(j_bin_str.ptr(), j_bin_str.length());
    ObIJsonBase *j_base = &j_bin;
    if (OB_FAIL(j_bin.reset_iter())) {
      LOG_WARN("failed to reset json bin iter", K(ret), K(j_bin_str));
    } else if (CAST_FAIL(j_base->print(j_buf, true))) {
      LOG_WARN("fail to cast json to other type", K(ret), K(j_bin_str), K(expect_type));
      ret = OB_ERR_INVALID_JSON_VALUE_FOR_CAST;
      LOG_USER_ERROR(OB_ERR_INVALID_JSON_VALUE_FOR_CAST);
    } else {
      // get in cs type
      bool need_charset_convert = ((CS_TYPE_BINARY == params.dest_collation_) ||
                                   (ObCharset::charset_type_by_coll(in.get_collation_type()) !=
                                    ObCharset::charset_type_by_coll(params.dest_collation_)));
      ObString temp_str_val(j_buf.length(), j_buf.ptr());

      /* this function maybe called in json generated column
      * and the generated column may has limitaion constrain the string length */
      uint64_t accuracy_max_len = 0;
      if (params.res_accuracy_ && params.res_accuracy_->get_length()) {
        accuracy_max_len = params.res_accuracy_->get_length();
      }
      if (!need_charset_convert) {
        if (accuracy_max_len > 0 && accuracy_max_len < j_buf.length()) {
          temp_str_val.assign_ptr(j_buf.ptr(), static_cast<int32_t>(accuracy_max_len));
        }
        ret = copy_string(params, ObLongTextType, temp_str_val, t_out);
        t_out.set_collation_type(params.dest_collation_);
      } else {
        ObObj tmp_obj;
        tmp_obj.set_collation_type(in.get_collation_type());
        tmp_obj.set_collation_level(in.get_collation_level());
        tmp_obj.set_string(ObLongTextType, temp_str_val);
        ret = string_string(ObLongTextType, params, tmp_obj, t_out, cast_mode);
        if (accuracy_max_len && accuracy_max_len < t_out.get_string().length()) {
          ObString tmp_str = t_out.get_string();
          t_out.set_string(ObLongTextType, tmp_str.ptr(), static_cast<int32_t>(accuracy_max_len));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(ObHexUtils::hextoraw(t_out, params, out))) {
          LOG_WARN("fail to hextoraw", K(ret), K(in), K(expect_type));
        } else {
          res_length = static_cast<ObLength>(out.get_string_len());
        }
        SET_RES_ACCURACY_STRING(expect_type, DEFAULT_PRECISION_FOR_STRING, res_length);
      }
    }
  }
  return ret;
}

static int json_string(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  // ToDo convert json to other none string type is also not allowed.
  if (OB_UNLIKELY(ObJsonType != in.get_type() ||
      OB_UNLIKELY(ObStringTC != ob_obj_type_class(expect_type) && ObTextTC != ob_obj_type_class(expect_type)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if (in.is_json_outrow()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid cast of out row json obj", K(ret), K(in), K(out.get_meta()), K(expect_type), K(cast_mode));
  } else if(OB_UNLIKELY(params.allocator_v2_ == NULL)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid allocator in json cast to other type", K(ret), K(params.allocator_v2_));
  } else {
    ObJsonBuffer j_buf(params.allocator_v2_);
    ObString j_bin_str = in.get_string();
    if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(params.allocator_v2_, in, j_bin_str))) {
      LOG_WARN("fail to get real data.", K(ret), K(j_bin_str));
    } else {
      ObJsonBin j_bin(j_bin_str.ptr(), j_bin_str.length());
      ObIJsonBase *j_base = &j_bin;
      if (OB_FAIL(j_bin.reset_iter())) {
        LOG_WARN("failed to reset json bin iter", K(ret), K(j_bin_str));
      } else if (CAST_FAIL(j_base->print(j_buf, true))) {
        LOG_WARN("fail to cast json to other type", K(ret), K(j_bin_str), K(expect_type));
        ret = OB_ERR_INVALID_JSON_VALUE_FOR_CAST;
        LOG_USER_ERROR(OB_ERR_INVALID_JSON_VALUE_FOR_CAST);
      } else {
        // get in cs type
        bool need_charset_convert = ((CS_TYPE_BINARY == params.dest_collation_) ||
                                    (ObCharset::charset_type_by_coll(in.get_collation_type()) !=
                                      ObCharset::charset_type_by_coll(params.dest_collation_)));
        ObString temp_str_val(j_buf.length(), j_buf.ptr());

        /* this function maybe called in json generated column
        * and the generated column may has limitaion constrain the string length */
        uint64_t accuracy_max_len = 0;
        if (params.res_accuracy_ && params.res_accuracy_->get_length()) {
          accuracy_max_len = params.res_accuracy_->get_length();
        }
        if (!need_charset_convert) {
          if (accuracy_max_len > 0 && accuracy_max_len < j_buf.length()) {
            temp_str_val.assign_ptr(j_buf.ptr(), accuracy_max_len);
          }
          ret = copy_string(params, expect_type, temp_str_val, out);
          out.set_collation_type(params.dest_collation_);
        } else {
          ObObj tmp_obj;
          tmp_obj.set_collation_type(in.get_collation_type());
          tmp_obj.set_collation_level(in.get_collation_level());
          tmp_obj.set_string(ObLongTextType, temp_str_val);
          tmp_obj.set_inrow();
          ret = string_string(expect_type, params, tmp_obj, out, cast_mode);
          if (accuracy_max_len && accuracy_max_len < out.get_string().length()) {
            ObString tmp_str = out.get_string();
            out.set_string(expect_type, tmp_str.ptr(), static_cast<ObString::obstr_size_t>(accuracy_max_len));
          }
          if (ObTextTC == ob_obj_type_class(expect_type)) {
            out.set_inrow();
          }
        }
      }
    }
  }
  return ret;
}

static int common_json_string(const ObObjType expect_type,
                              ObObjCastParams &params,
                              const ObCastMode cast_mode,
                              const ObObj &in,
                              ObString& res_str)
{
  int ret = OB_SUCCESS;
  int warning = OB_SUCCESS;
  if (OB_UNLIKELY(ObJsonType != in.get_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if(OB_UNLIKELY(params.allocator_v2_ == NULL)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid allocator in json cast to other type", K(ret), K(params.allocator_v2_));
  } else {
    ObJsonBuffer j_buf(params.allocator_v2_);
    ObString j_bin_str = in.get_string();
    if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(params.allocator_v2_, in, j_bin_str))) {
      LOG_WARN("fail to get real data.", K(ret), K(j_bin_str));
    } else {
      ObJsonBin j_bin(j_bin_str.ptr(), j_bin_str.length());
      ObIJsonBase *j_base = &j_bin;
      ObString j_str;
      if (OB_FAIL(j_bin.reset_iter())) {
        LOG_WARN("failed to reset json bin iter", K(ret), K(j_bin_str));
      } else if (CAST_FAIL(j_base->print(j_buf, true))) {
        LOG_WARN("fail to cast json to other type", K(ret), K(j_bin_str), K(expect_type));
        ret = OB_ERR_INVALID_JSON_VALUE_FOR_CAST;
        LOG_USER_ERROR(OB_ERR_INVALID_JSON_VALUE_FOR_CAST);
      } else {
        // get json string
        uint64_t accuracy_max_len = 0;
        if (params.res_accuracy_ && params.res_accuracy_->get_length()) {
          accuracy_max_len = params.res_accuracy_->get_length();
        }
        if (accuracy_max_len > 0 && accuracy_max_len < j_buf.length()) {
          j_str.assign_ptr(j_buf.ptr(), accuracy_max_len);
        } else {
          j_str.assign_ptr(j_buf.ptr(), j_buf.length());
        }

        bool need_charset_convert = ((CS_TYPE_BINARY != params.dest_collation_) &&
                                    (ObCharset::charset_type_by_coll(in.get_collation_type()) !=
                                      ObCharset::charset_type_by_coll(params.dest_collation_)));
        if (!need_charset_convert) {
          res_str = j_str;
        } else {
          ObObj tmp_in;
          ObObj tmp_out;
          tmp_in.set_collation_type(in.get_collation_type());
          tmp_in.set_collation_level(in.get_collation_level());
          tmp_in.set_string(ObLongTextType, j_str);
          if(OB_FAIL(string_string(ObLongTextType, params, tmp_in, tmp_out, cast_mode))) {
            LOG_WARN("fail to cast string to longtext", K(ret), K(tmp_in));
          } else {
            if (accuracy_max_len && accuracy_max_len < tmp_out.get_string().length()) {
              res_str.assign_ptr(tmp_out.get_string().ptr(), accuracy_max_len);
            } else {
              res_str = tmp_out.get_string();
            }
          }
        }
      }
    }
  }
  return ret;
}

static int json_text(const ObObjType expect_type, ObObjCastParams &params,
                     const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObString res_str;
  if(OB_FAIL(common_json_string(expect_type, params, cast_mode, in, res_str))) {
    LOG_WARN("common json to string failed", K(ret));
  } else {
    out.set_string(expect_type, res_str);
    if (OB_FAIL(ObTextStringResult::ob_convert_obj_temporay_lob(out, *params.allocator_v2_))) {
      LOG_WARN("convert lob type obj failed", K(ret), K(out));
    }
  }
  return ret;
}

static int json_bit(const ObObjType expect_type, ObObjCastParams &params,
                    const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  uint64_t value;
  int32_t bit_len = 0;

  if (OB_UNLIKELY(ObJsonType != in.get_type()
      || ObBitTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else {
    ObString j_bin_str = in.get_string();
    if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(params.allocator_v2_, in, j_bin_str))) {
      LOG_WARN("fail to get real data.", K(ret), K(j_bin_str));
    } else {
      ObJsonBin j_bin(j_bin_str.ptr(), j_bin_str.length());
      ObIJsonBase *j_base = &j_bin;
      if (OB_FAIL(j_bin.reset_iter())) {
        LOG_WARN("failed to reset json bin iter", K(ret), K(j_bin_str));
      } else if (CAST_FAIL(j_base->to_bit(value))) {
        ret = OB_ERR_INVALID_JSON_VALUE_FOR_CAST;
        LOG_USER_ERROR(OB_ERR_INVALID_JSON_VALUE_FOR_CAST);
      } else if (OB_FAIL(get_bit_len(value, bit_len))) {
        LOG_WARN("fail to get bit len", K(ret), K(value), K(bit_len));
      } else {
        SET_RES_BIT(out);
        SET_RES_ACCURACY(static_cast<ObPrecision>(bit_len), DEFAULT_SCALE_FOR_BIT, DEFAULT_LENGTH_FOR_NUMERIC);
      }
    }
  }
  UNUSED(params);
  UNUSED(cast_mode);

  return ret;
}

static int json_otimestamp(const ObObjType expect_type, ObObjCastParams &params,
                           const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObObj datetime_obj;

  if (OB_UNLIKELY(ObJsonType != in.get_type()) || OB_UNLIKELY(ObOTimestampTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(json_datetime(ObDateTimeType, params, in, datetime_obj, cast_mode))) {
    LOG_WARN("json to datatime convert failed in json to otimestamp convert",
             K(ret), K(cast_mode));
  } else if (OB_FAIL(datetime_otimestamp(expect_type, params, datetime_obj, out, cast_mode))) {
    LOG_WARN("datatime to otimestamp convert failed in json to otimestamp convert",
             K(ret), K(datetime_obj), K(cast_mode));
  }

  return ret;
}

static int json_lob(const ObObjType expect_type, ObObjCastParams &params,
                    const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObString res_str;
  if(OB_FAIL(common_json_string(expect_type, params, cast_mode, in, res_str))) {
    LOG_WARN("common json to string failed", K(ret));
  } else {
    // add lob locator
    ObLobLocator *lob_locator = nullptr;
    const int64_t buf_len = sizeof(ObLobLocator) + res_str.length();
    char *buf = nullptr;
    if (OB_ISNULL(buf = static_cast<char*>(params.alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to allocate memory for lob locator", K(ret), K(res_str));
    } else if (FALSE_IT(lob_locator = reinterpret_cast<ObLobLocator *> (buf))) {
    } else if (OB_FAIL(lob_locator->init(res_str))) {
      STORAGE_LOG(WARN, "Failed to init lob locator", K(ret), K(res_str), KPC(lob_locator));
    } else {
      out.set_lob_locator(*lob_locator);
    }
  }
  return ret;
}


static int json_json(const ObObjType expect_type, ObObjCastParams &params,
                     const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObJsonType != in.get_type()) || OB_UNLIKELY(ObJsonTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else {
    ObString str = in.get_string();
    out.set_json_value(expect_type, str.ptr(), str.length());
    if (in.has_lob_header()) {
      out.set_has_lob_header();
    }
    ObLength res_length = static_cast<ObLength>(out.get_string_len());
    SET_RES_ACCURACY(DEFAULT_SCALE_FOR_TEXT, DEFAULT_PRECISION_FOR_STRING, res_length);
  }
  UNUSED(cast_mode);
  return ret;
}

static int json_geometry(const ObObjType expect_type, ObObjCastParams &params,
                         const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  UNUSEDx(expect_type, params, in, out);
  int ret = OB_NOT_SUPPORTED;
  if (OB_FAIL(ret) && CM_IS_COLUMN_CONVERT(cast_mode)) { // adapt mysql
    ret = OB_ERR_CANT_CREATE_GEOMETRY_OBJECT;
    LOG_USER_ERROR(OB_ERR_CANT_CREATE_GEOMETRY_OBJECT);
  } else {
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "json, geometry");
  }
  return ret;
}

////////////////////////////////////////////////////////////
// geometry -> XXX
static int geometry_int(const ObObjType expect_type, ObObjCastParams &params,
                        const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (CM_IS_IMPLICIT_CAST(cast_mode)) {
    ObString str = in.get_string();
    ObObj temp_obj;
    if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(params.allocator_v2_, in, str))) {
      LOG_WARN("fail to get real data.", K(ret), K(str));
    } else if (FALSE_IT(temp_obj.set_string(in.get_type(), str))) {
    } else if (OB_FAIL(string_int(expect_type, params, temp_obj, out, cast_mode))) {
      LOG_WARN("fail to cast string to int", K(ret));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "cast_as_signed");
  }
  return ret;
}

static int geometry_uint(const ObObjType expect_type, ObObjCastParams &params,
                         const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (CM_IS_IMPLICIT_CAST(cast_mode)) {
    ObString str = in.get_string();
    ObObj temp_obj;
    if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(params.allocator_v2_, in, str))) {
      LOG_WARN("fail to get real data.", K(ret), K(str));
    } else if (FALSE_IT(temp_obj.set_string(in.get_type(), str))) {
    } else if (OB_FAIL(string_uint(expect_type, params, temp_obj, out, cast_mode))) {
      LOG_WARN("fail to cast string to uint", K(ret));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "cast_as_unsigned");
  }
  return ret;
}

static int geometry_double(const ObObjType expect_type, ObObjCastParams &params,
                           const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (CM_IS_IMPLICIT_CAST(cast_mode) && !CM_IS_WARN_ON_FAIL(cast_mode)) {
    ObString str = in.get_string();
    ObObj temp_obj;
    if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(params.allocator_v2_, in, str))) {
      LOG_WARN("fail to get real data.", K(ret), K(str));
    } else if (FALSE_IT(temp_obj.set_string(in.get_type(), str))) {
    } else if (OB_FAIL(string_double(expect_type, params, temp_obj, out, cast_mode))) {
      LOG_WARN("fail to cast string to double", K(ret));
      ret = OB_ERR_WARN_DATA_OUT_OF_RANGE; // adapt mysql
    }
  } else {
    out.set_double(0);
  }
  return ret;
}

static int geometry_float(const ObObjType expect_type, ObObjCastParams &params,
                          const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (CM_IS_IMPLICIT_CAST(cast_mode)) {
    ObString str = in.get_string();
    ObObj temp_obj;
    if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(params.allocator_v2_, in, str))) {
      LOG_WARN("fail to get real data.", K(ret), K(str));
    } else if (FALSE_IT(temp_obj.set_string(in.get_type(), str))) {
    } else if (OB_FAIL(string_float(expect_type, params, temp_obj, out, cast_mode))) {
      LOG_WARN("fail to cast string to float", K(ret));
      ret = OB_ERR_WARN_DATA_OUT_OF_RANGE; // adapt mysql
    }
  } else {
    out.set_float(0);
  }
  return ret;
}

static int geometry_number(const ObObjType expect_type, ObObjCastParams &params,
                           const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (CM_IS_IMPLICIT_CAST(cast_mode)) {
    ObString str = in.get_string();
    ObObj temp_obj;
    if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(params.allocator_v2_, in, str))) {
      LOG_WARN("fail to get real data.", K(ret), K(str));
    } else if (FALSE_IT(temp_obj.set_string(in.get_type(), str))) {
    } else if (OB_FAIL(string_number(expect_type, params, temp_obj, out, cast_mode))) {
      LOG_WARN("fail to cast string to number", K(ret));
      ret = OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD; // adapt mysql
    }
  } else {
    number::ObNumber nmb;
    nmb.set_zero();
    out.set_number(nmb);
  }
  return ret;
}

static int geometry_datetime(const ObObjType expect_type, ObObjCastParams &params,
                             const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (CM_IS_IMPLICIT_CAST(cast_mode)) {
    ObString str = in.get_string();
    ObObj temp_obj;
    if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(params.allocator_v2_, in, str))) {
      LOG_WARN("fail to get real data.", K(ret), K(str));
    } else if (FALSE_IT(temp_obj.set_string(in.get_type(), str))) {
    } else if (OB_FAIL(string_datetime(expect_type, params, temp_obj, out, cast_mode))) {
      LOG_WARN("fail to cast string to datetime", K(ret));
    }
  } else {
    out.set_null();
  }
  return ret;
}

static int geometry_date(const ObObjType expect_type, ObObjCastParams &params,
                         const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (CM_IS_IMPLICIT_CAST(cast_mode)) {
    ObString str = in.get_string();
    ObObj temp_obj;
    if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(params.allocator_v2_, in, str))) {
      LOG_WARN("fail to get real data.", K(ret), K(str));
    } else if (FALSE_IT(temp_obj.set_string(in.get_type(), str))) {
    } else if (OB_FAIL(string_date(expect_type, params, temp_obj, out, cast_mode))) {
      LOG_WARN("fail to cast string to date", K(ret));
    }
  } else {
    out.set_null();
  }
  return ret;
}

static int geometry_time(const ObObjType expect_type, ObObjCastParams &params,
                         const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (CM_IS_NULL_ON_WARN(cast_mode)) {
    // issue/42826593
    out.set_null();
  } else if (CM_IS_IMPLICIT_CAST(cast_mode)) {
    ObString str = in.get_string();
    ObObj temp_obj;
    if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(params.allocator_v2_, in, str))) {
      LOG_WARN("fail to get real data.", K(ret), K(str));
    } else if (FALSE_IT(temp_obj.set_string(in.get_type(), str))) {
    } else if (OB_FAIL(string_time(expect_type, params, temp_obj, out, cast_mode))) {
      LOG_WARN("fail to cast string to time", K(ret));
    }
  } else {
    out.set_null();
  }
  return ret;
}

static int geometry_year(const ObObjType expect_type, ObObjCastParams &params,
                         const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (CM_IS_IMPLICIT_CAST(cast_mode)) {
    ObString str = in.get_string();
    ObObj temp_obj;
    if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(params.allocator_v2_, in, str))) {
      LOG_WARN("fail to get real data.", K(ret), K(str));
    } else if (FALSE_IT(temp_obj.set_string(in.get_type(), str))) {
    } else if (OB_FAIL(string_year(expect_type, params, temp_obj, out, cast_mode))) {
      LOG_WARN("fail to cast string to year", K(ret));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "cast_as_year");
  }
  return ret;
}

static int geometry_bit(const ObObjType expect_type, ObObjCastParams &params,
                        const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (CM_IS_IMPLICIT_CAST(cast_mode)) {
    ObString str = in.get_string();
    ObObj temp_obj;
    if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(params.allocator_v2_, in, str))) {
      LOG_WARN("fail to get real data.", K(ret), K(str));
    } else if (FALSE_IT(temp_obj.set_string(in.get_type(), str))) {
    } else if (OB_FAIL(string_bit(expect_type, params, temp_obj, out, cast_mode))) {
      LOG_WARN("fail to cast string to bit", K(ret));
    }
  } else {
    ret = OB_ERR_PARSE_SQL;
    const char *err_msg = "bit";
    int32_t str_len = static_cast<int32_t>(strlen(err_msg));
    int32_t line_no = 1;
    LOG_USER_ERROR(OB_ERR_PARSE_SQL, ob_errpkt_strerror(OB_ERR_PARSER_SYNTAX, false),
        str_len, err_msg, line_no);
  }
  return ret;
}

static int geometry_otimestamp(const ObObjType expect_type, ObObjCastParams &params,
                               const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (CM_IS_IMPLICIT_CAST(cast_mode)) {
    ObString str = in.get_string();
    ObObj temp_obj;
    if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(params.allocator_v2_, in, str))) {
      LOG_WARN("fail to get real data.", K(ret), K(str));
    } else if (FALSE_IT(temp_obj.set_string(in.get_type(), str))) {
    } else if (OB_FAIL(string_otimestamp(expect_type, params, temp_obj, out, cast_mode))) {
      LOG_WARN("fail to cast string to otimestamp", K(ret));
    }
  } else {
    ret = OB_ERR_PARSE_SQL;
    const char *err_msg = "timestamp";
    int32_t str_len = static_cast<int32_t>(strlen(err_msg));
    int32_t line_no = 1;
    LOG_USER_ERROR(OB_ERR_PARSE_SQL, ob_errpkt_strerror(OB_ERR_PARSER_SYNTAX, false),
        str_len, err_msg, line_no);
  }
  return ret;
}

static int geom_copy_string(ObObjCastParams &params,
                            const ObObjType expect_type,
                            const ObString &src,
                            ObObj &obj)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t len = src.length() - WKB_VERSION_SIZE;
  if (OB_LIKELY(len > 0)) {
    if (is_lob_storage(expect_type)) {
      bool has_lob_header = (!IS_CLUSTER_VERSION_BEFORE_4_1_0_0 && (expect_type != ObTinyTextType));
      sql::ObTextStringObObjResult str_result(expect_type, &params, &obj, has_lob_header);
      if (OB_FAIL(str_result.init(len))) {
      } else if (OB_FAIL(str_result.append(src.ptr(), WKB_GEO_SRID_SIZE))) {
      } else if (OB_FAIL(str_result.append(src.ptr() + WKB_OFFSET, len - WKB_GEO_SRID_SIZE))) {
      } else {
        str_result.set_result();
      }
    } else {
      if (OB_UNLIKELY(NULL == (buf = static_cast<char*>(params.alloc(len))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        MEMMOVE(buf, src.ptr(), WKB_GEO_SRID_SIZE);
        MEMMOVE(buf + WKB_GEO_SRID_SIZE, src.ptr() + WKB_OFFSET, len - WKB_GEO_SRID_SIZE);
        if (ob_is_raw(expect_type)) {
          obj.set_raw(buf, static_cast<int32_t>(len));
        } else {
          obj.set_string(expect_type, buf, static_cast<int32_t>(len));
        }
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid obj len", K(ret), K(len));
  }
  return ret;
}

static int geometry_string(const ObObjType expect_type, ObObjCastParams &params,
                           const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObLength res_length = -1;
  ObString wkb = in.get_string();
  if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(params.allocator_v2_, in, wkb))) {
    LOG_WARN("fail to get real data.", K(ret), K(wkb));
  } else if (OB_FAIL(geom_copy_string(params, expect_type, wkb, out))){
    LOG_WARN("fail to copy string", K(ret), K(expect_type));
  } else {
    res_length = static_cast<ObLength>(out.get_string_len());
  }
  SET_RES_ACCURACY_STRING(expect_type, DEFAULT_PRECISION_FOR_STRING, res_length);
  return ret;
}

static int geometry_json(const ObObjType expect_type, ObObjCastParams &params,
                         const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  UNUSEDx(expect_type, params, in, out, cast_mode);
  int ret = OB_NOT_SUPPORTED;
  LOG_USER_ERROR(OB_NOT_SUPPORTED, "geomerty, json");
  return ret;
}

static int geometry_geometry(const ObObjType expect_type, ObObjCastParams &params,
                             const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObLength res_length = -1;
  if (OB_ISNULL(params.allocator_v2_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid allocator", K(ret));
  } else {
    ObGeometry *src_tree = NULL;
    ObGeometry *dst_tree = NULL;
    ObString wkb;
    ObIAllocator &temp_allocator = *params.allocator_v2_;
    ObGeoType dst_geo_type = ObGeoCastUtils::get_geo_type_from_cast_mode(cast_mode);
    const char *cast_name = ObGeometryTypeCastUtil::get_cast_name(dst_geo_type);
    omt::ObSrsCacheGuard srs_guard;
    const ObSrsItem *srs = NULL;
    ObGeometryTypeCast *geo_cast = NULL;
    ObGeoErrLogInfo log_info;
    ObString instr = in.get_string();
    if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(params.allocator_v2_, in, instr))) {
      LOG_WARN("fail to get real data.", K(ret), K(instr));
    } else if (ObGeoType::GEOMETRY == dst_geo_type) {
      out = in;
    } else if (OB_FAIL(common_get_srs_item(srs_guard, instr, srs, cast_name))) {
      LOG_WARN("fail to get srs item", K(ret), K(instr));
    } else if (OB_FAIL(ObGeometryTypeCastUtil::get_tree(temp_allocator, instr, src_tree,
        srs, log_info, cast_name))) {
      LOG_WARN("fail to get tree", K(ret), K(instr));
      if (OB_ERR_GIS_INVALID_DATA == ret) {
        LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, cast_name);
      } else if (OB_ERR_GEOMETRY_PARAM_LONGITUDE_OUT_OF_RANGE == ret) {
        LOG_USER_ERROR(OB_ERR_GEOMETRY_PARAM_LONGITUDE_OUT_OF_RANGE,
                      cast_name,
                      log_info.value_out_of_range_,
                      log_info.min_long_val_,
                      log_info.max_long_val_);
      } else if (OB_ERR_GEOMETRY_PARAM_LATITUDE_OUT_OF_RANGE == ret) {
        LOG_USER_ERROR(OB_ERR_GEOMETRY_PARAM_LATITUDE_OUT_OF_RANGE,
                      cast_name,
                      log_info.value_out_of_range_,
                      log_info.min_lat_val_,
                      log_info.max_lat_val_);
      }
    } else {
      ObSrsType srs_type = src_tree->get_srid() == 0 ?
                           ObSrsType::PROJECTED_SRS :
                           srs->srs_type();
      if (OB_FAIL(ObGeoTypeUtil::create_geo_by_type(temp_allocator, dst_geo_type,
          ObSrsType::GEOGRAPHIC_SRS == srs_type, false, dst_tree, src_tree->get_srid()))) {
        LOG_WARN("fail to alloc dst geo tree", K(ret), K(dst_geo_type), K(srs_type));
      } else if (OB_FAIL(ObGeometryTypeCastFactory::alloc(temp_allocator,
          dst_geo_type, geo_cast))) {
        LOG_WARN("fail to alloc geometry cast", K(ret), K(dst_geo_type));
      } else {
        ObGeoErrLogInfo log_info;
        if (ObSrsType::PROJECTED_SRS == srs_type) {
          if (OB_FAIL(geo_cast->cast_geom(*src_tree, *dst_tree, srs, log_info, &temp_allocator))) {
            LOG_WARN("fail to cast geom", K(ret), K(src_tree->type()), K(dst_geo_type));
            ObGeoCastUtils::geo_cast_error_handle(ret, src_tree->type(), dst_geo_type, log_info);
          }
        } else if (ObSrsType::GEOGRAPHIC_SRS == srs_type) {
          if (OB_FAIL(geo_cast->cast_geog(*src_tree, *dst_tree, srs, log_info, &temp_allocator))) {
            LOG_WARN("fail to cast geog", K(ret), K(src_tree->type()), K(dst_geo_type));
            ObGeoCastUtils::geo_cast_error_handle(ret, src_tree->type(), dst_geo_type, log_info);
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unknowed srs type", K(ret), K(srs_type));
        }
      }

      if (OB_SUCC(ret)) {
        ObString res_wkb;
        if (OB_FAIL(ObGeoTypeUtil::to_wkb(temp_allocator, *dst_tree, srs, res_wkb))) {
          LOG_WARN("fail to get wkb", K(ret), K(src_tree->type()), K(dst_geo_type));
        } else if (OB_FAIL(set_geo_res(&params, &out, res_wkb))) {
          LOG_WARN("set geo result failed", K(ret));
        } else {
          res_length = static_cast<ObLength>(out.get_string_len());
        }
      }
    }
  }
  SET_RES_ACCURACY_STRING(expect_type, DEFAULT_PRECISION_FOR_STRING, res_length);
  return ret;
}

////////////////////////////////////////////////////////////
// XXX -> UDT

static int cast_to_udt_not_support(const ObObjType expect_type, ObObjCastParams &params,
                                   const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;

  if (out.is_xml_sql_type()) {
    // only allow cast basic types to invalid CAST to a type that is not a nested table or VARRAY
    ret = OB_ERR_INVALID_CAST_UDT;
    LOG_WARN_RET(ret, "invalid CAST to a type that is not a nested table or VARRAY");
  } else {
    // other udts
    // ORA-00932: inconsistent datatypes: expected PLSQL INDEX TABLE got NUMBER
    // currently other types to udt not supported
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN_RET(ret, "not expected obj type convert", K(expect_type), K(in), K(out), K(cast_mode));
  }
  return ret;
}


static int sql_udt_pl_extend(const ObObjType expect_type, ObObjCastParams &params,
                             const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_ORACLE_XML
  if (in.is_xml_sql_type()) {
    // no need to read blob full data
    pl::ObPLXmlType *xmltype = NULL;
    void *ptr = NULL;
    ObObj* data = NULL; // obobj for blob;
    if (OB_ISNULL(ptr = params.alloc(sizeof(pl::ObPLXmlType)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to allocate memory for pl xml data type", K(ret), K(sizeof(pl::ObPLXmlType)));
    } else if (FALSE_IT(xmltype = new (ptr)pl::ObPLXmlType())) {
    } else if (OB_ISNULL(data = static_cast<ObObj*>(params.alloc(sizeof(ObObj))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for pl object", K(ret));
    } else {
      ObString xml_data = in.get_string();
      int64_t xml_data_size = xml_data.length();
      char *xml_data_buff = static_cast<char *>(params.alloc(xml_data_size));
      if (OB_ISNULL(xml_data_buff)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("failed to allocate memory for xmldata", K(ret), K(xml_data_size));
      } else {
        MEMCPY(xml_data_buff, xml_data.ptr(), xml_data_size);

        data->set_string(ObLongTextType, xml_data_buff, static_cast<int32_t>(xml_data_size));
        data->set_has_lob_header(); // must has lob header
        data->set_collation_type(CS_TYPE_UTF8MB4_BIN);
        xmltype->set_data(data);
        out.set_extend(reinterpret_cast<int64_t>(xmltype), pl::PL_OPAQUE_TYPE);
        // need deep copy?
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected type to convert format", K(ret), K(expect_type), K(in));
  }
#else
  ret = OB_NOT_SUPPORTED;
#endif
  return ret;
}

static int string_sql_udt(const ObObjType expect_type, ObObjCastParams &params,
                          const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_ORACLE_XML
  if (in.is_string_type()) {
    ObMulModeMemCtx* mem_ctx = nullptr;
    ObIAllocator &temp_allocator = *params.allocator_v2_;
    bool is_null_result = false;
    ObString in_str;
    ObObj tmp_out;
    ObXmlDocument* doc = nullptr;
    ObString binary_str;
    ObString blob_locator;
    if (ObCharset::charset_type_by_coll(in.get_collation_type()) != CHARSET_UTF8MB4) {
      if (OB_FAIL(string_string(ObObjType::ObVarcharType, params, in, tmp_out, cast_mode))) {
        LOG_WARN("fail to cast string to longtext", K(ret), K(in));
      } else if (tmp_out.is_null()) {
        is_null_result = true;
      } else {
        in_str = tmp_out.get_string();
      }
    } else if (OB_FAIL(in.get_string(in_str))) {
      LOG_WARN("Failed to get string from in obj", K(ret), K(tmp_out));
    }

    if (OB_FAIL(ret)) {
    } else if (is_null_result) {
      out.set_sql_udt("", 0, ObXMLSqlType);
    } else if (OB_FAIL(ObXmlUtil::create_mulmode_tree_context(&temp_allocator, mem_ctx))) {
      LOG_WARN("fail to create tree memory context", K(ret));
    } else {
      ObXmlParser parser(mem_ctx);
      if (OB_FAIL(parser.parse_document(in_str))) {
        ret = OB_ERR_XML_PARSE;
        LOG_USER_ERROR(OB_ERR_XML_PARSE);
        LOG_WARN("parse xml plain text as document failed.", K(in_str));
      } else if (OB_ISNULL(doc = parser.document())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get doc null", K(ret));
      } else if (!doc->get_encoding().empty() || doc->get_encoding_flag()) {
        doc->set_encoding(ObXmlUtil::get_charset_name(in.get_collation_type()));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(doc->get_raw_binary(binary_str, mem_ctx->allocator_))) {
        LOG_WARN("get raw binary failed", K(ret));
      } else {
        ObTextStringResult blob_res(ObLongTextType, true, mem_ctx->allocator_);
        int64_t total_length = binary_str.length();
        if (OB_FAIL(blob_res.init(total_length))) {
          LOG_WARN("failed to init blob res", K(ret), K(blob_res), K(total_length));
        } else if (OB_FAIL(blob_res.append(binary_str))) {
          LOG_WARN("failed to append xml binary data", K(ret), K(blob_res));
        } else {
          blob_res.get_result_buffer(blob_locator);
          out.set_sql_udt(blob_locator.ptr(), blob_locator.length(), ObXMLSqlType);
        }
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected type to convert format", K(ret), K(expect_type), K(in));
  }
#else
  ret = OB_NOT_SUPPORTED;
#endif
  return ret;
}

static int pl_extend_sql_udt(const ObObjType expect_type, ObObjCastParams &params,
                             const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_ORACLE_XML
  if (in.is_pl_extend()) {
    if (pl::PL_OPAQUE_TYPE == in.get_meta().get_extend_type()) {
      pl::ObPLOpaque *pl_src = reinterpret_cast<pl::ObPLOpaque*>(in.get_ext());
      if (OB_ISNULL(pl_src)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("failed to get pl data type info", K(ret), K(in));
      } else if (pl_src->get_type() == pl::ObPLOpaqueType::PL_XML_TYPE) {
        pl::ObPLXmlType * xmltype = static_cast<pl::ObPLXmlType*>(pl_src);
        ObObj *blob_obj = xmltype->get_data();
        if (OB_ISNULL(blob_obj) || blob_obj->is_null()) {
          out.set_sql_udt("", 0, ObXMLSqlType);
        } else {
          // deep copy here or by the caller ?
          ObString xml_data = blob_obj->get_string();
          int64_t xml_data_size = xml_data.length();
          char *xml_data_buff = static_cast<char *>(params.alloc(xml_data_size));
          if (OB_ISNULL(xml_data_buff)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocate memory for xmldata",
                      K(ret), K(expect_type), K(xml_data_size));
          } else {
            MEMCPY(xml_data_buff, xml_data.ptr(), xml_data_size);
            out.set_sql_udt(xml_data_buff, static_cast<int32_t>(xml_data_size), ObXMLSqlType);
          }
        }
      } else if (pl_src->get_type() == pl::ObPLOpaqueType::PL_INVALID) {
        // possibly an un-initiated pl opaque variable
        out.set_null();
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("not expected obj type convert", K(ret), K(expect_type), K(in), K(out), K(cast_mode));
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected type to convert format", K(ret), K(expect_type), K(in));
  }
#else
  ret = OB_NOT_SUPPORTED;
#endif
  return ret;
}

static int pl_extend_string(const ObObjType expect_type, ObObjCastParams &params,
                            const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_ORACLE_XML
  if (in.is_pl_extend()) {
    if (pl::PL_OPAQUE_TYPE == in.get_meta().get_extend_type()) {
      pl::ObPLOpaque *pl_src = reinterpret_cast<pl::ObPLOpaque*>(in.get_ext());
      if (OB_ISNULL(pl_src)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("failed to get pl data type info", K(ret), K(in));
      } else if (pl_src->get_type() == pl::ObPLOpaqueType::PL_XML_TYPE) {
        pl::ObPLXmlType * xmltype = static_cast<pl::ObPLXmlType*>(pl_src);
        ObObj *blob_obj = xmltype->get_data();
        if (OB_ISNULL(blob_obj)) {
          ret = OB_ERR_NULL_VALUE;
          LOG_WARN("Unexpected xml data", K(ret), K(*xmltype));
        } else {
          ObString blob_data = blob_obj->get_string();
          ObStringBuffer xml_plain_text(params.allocator_v2_);
          common::ObArenaAllocator temp_allocator(ObModIds::OB_LOB_ACCESS_BUFFER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
          ObCollationType session_cs_type = params.dest_collation_;
          if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(params.allocator_v2_,
                                                                    ObLongTextType,
                                                                    CS_TYPE_BINARY,
                                                                    true,
                                                                    blob_data))) {
            LOG_WARN("fail to get xml data.", K(ret), K(blob_data));
          } else if (OB_FAIL(ObXmlUtil::cast_to_string(blob_data, temp_allocator, xml_plain_text, session_cs_type))) {
            LOG_WARN("failed to convert xml to string", K(ret), KP(blob_data.ptr()), K(blob_data.length()));
          } else {
            ObObj tmp_in = in;
            tmp_in.set_string(ObVarcharType, xml_plain_text.string());
            tmp_in.set_collation_type(CS_TYPE_UTF8MB4_BIN);
            if (OB_FAIL(string_string(expect_type, params, tmp_in, out, cast_mode))) {
              LOG_WARN("cast string to string in udt string failed", K(ret));
            }
          }
        }
      } else if (pl_src->get_type() == pl::ObPLOpaqueType::PL_INVALID) {
        // un-initiated pl variable, for example, xml_data and xml_data2 are declared without assignment
        // then call this directly: select replace(xml_data,xml_data2 ,'1') into stringval from dual;
        out.set_null();
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("not expected obj type convert", K(ret), K(expect_type), K(in), K(out), K(cast_mode));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("not expected obj type convert", K(ret), K(expect_type), K(in), K(out), K(cast_mode));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected type to convert format", K(ret), K(expect_type), K(in));
  }
#else
  ret = OB_NOT_SUPPORTED;
#endif
  return ret;
}

////////////////////////////////////////////////////////////
// UDT -> XXX
static int cast_udt_to_other_not_support(const ObObjType expect_type, ObObjCastParams &params,
                                         const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (in.is_xml_sql_type()) {
    // only allow cast basic types to invalid CAST to a type that is not a nested table or VARRAY
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN_RET(ret, "inconsistent datatypes", K(expect_type), K(in), K(out), K(cast_mode));
  } else {
    // other udts
    // ORA-00932: inconsistent datatypes: expected PLSQL INDEX TABLE got NUMBER
    // currently other types to udt not supported
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN_RET(ret, "not expected obj type convert", K(expect_type), K(in), K(out), K(cast_mode));
  }
  return ret;
}

static int udt_string(const ObObjType expect_type, ObObjCastParams &params,
                      const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_ORACLE_XML
  if (in.is_xml_sql_type()) {
    ObString blob_data = in.get_string();
    ObStringBuffer xml_plain_text(params.allocator_v2_);
    common::ObArenaAllocator temp_allocator(ObModIds::OB_LOB_ACCESS_BUFFER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    ObCollationType session_cs_type = params.dest_collation_;
    if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(params.allocator_v2_,
                                                               ObLongTextType,
                                                               CS_TYPE_BINARY,
                                                               true,
                                                               blob_data))) {
      LOG_WARN("fail to get xml data.", K(ret), K(blob_data));
    } else if (OB_FAIL(ObXmlUtil::cast_to_string(blob_data, temp_allocator, xml_plain_text, session_cs_type))) {
      LOG_WARN("failed to convert xml to string", K(ret), KP(blob_data.ptr()), K(blob_data.length()));
    } else {
      ObObj tmp_in = in;
      tmp_in.set_string(ObVarcharType, xml_plain_text.string());
      tmp_in.set_collation_type(CS_TYPE_UTF8MB4_BIN);
      if (OB_FAIL(string_string(expect_type, params, tmp_in, out, cast_mode))) {
        LOG_WARN("cast string to string in udt string failed", K(ret));
      }
    }
  } else {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    // get udt type name from ctx
    LOG_WARN_RET(OB_ERR_INVALID_TYPE_FOR_OP, "inconsistent datatypes",
      "expected", expect_type, "got", in.get_type(), K(in.get_udt_subschema_id()));
  }
#else
  ret = OB_NOT_SUPPORTED;
#endif
  return ret;
}

ObObjCastFunc OB_OBJ_CAST[ObMaxTC][ObMaxTC] =
{
  {
    /*null -> XXX*/
    cast_identity,/*null*/
    cast_identity,/*int*/
    cast_identity,/*uint*/
    cast_identity,/*float*/
    cast_identity,/*double*/
    cast_identity,/*number*/
    cast_identity,/*datetime*/
    cast_identity,/*date*/
    cast_identity,/*time*/
    cast_identity,/*year*/
    cast_identity,/*string*/
    cast_identity,/*extend*/
    cast_identity,/*unknown*/
    cast_identity,/*text*/
    cast_identity,/*bit*/
    cast_identity,/*enumset*/
    cast_identity,/*enumsetInner*/
    cast_identity,/*otimestamp*/
    cast_inconsistent_types,/*raw*/
    cast_not_expected,/*interval*/
    cast_not_expected,/*rowid*/
    cast_identity,/*lob*/
    cast_identity,/*json*/
    cast_identity,/*geometry*/
    cast_not_expected,/*udt, mysql mode does not have udt*/
  },
  {
    /*int -> XXX*/
    cast_not_support,/*null*/
    int_int,/*int*/
    int_uint,/*uint*/
    int_float,/*float*/
    int_double,/*double*/
    int_number,/*number*/
    int_datetime,/*datetime*/
    int_date,/*date*/
    int_time,/*time*/
    int_year,/*year*/
    int_string,/*string*/
    cast_not_support,/*extend*/
    cast_not_support,/*unknown*/
    int_text,/*text*/
    int_bit,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    cast_not_support,/*otimestamp*/
    cast_inconsistent_types,/*raw*/
    cast_not_expected,/*interval*/
    cast_not_expected,/*rowid*/
    int_lob,/*lob*/
    int_json,/*json*/
    int_geometry,/*geometry*/
    cast_not_expected,/*udt*/
  },
  {
    /*uint -> XXX*/
    cast_not_support,/*null*/
    uint_int,/*int*/
    uint_uint,/*uint*/
    uint_float,/*float*/
    uint_double,/*double*/
    uint_number,/*number*/
    uint_datetime,/*datetime*/
    uint_date,/*date*/
    uint_time,/*time*/
    uint_year,/*year*/
    uint_string,/*string*/
    cast_not_support,/*extend*/
    cast_not_support,/*unknown*/
    uint_text,/*text*/
    uint_bit,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    cast_not_support,/*otimestamp*/
    cast_inconsistent_types,/*raw*/
    cast_not_expected,/*interval*/
    cast_not_expected,/*rowid*/
    uint_lob,/*lob*/
    uint_json,/*json*/
    uint_geometry,/*geometry*/
    cast_not_expected,/*udt*/
  },
  {
    /*float -> XXX*/
    cast_not_support,/*null*/
    float_int,/*int*/
    float_uint,/*uint*/
    float_float,/*float*/
    float_double,/*double*/
    float_number,/*number*/
    float_datetime,/*datetime*/
    float_date,/*date*/
    float_time,/*time*/
    cast_not_support,/*year*/
    float_string,/*string*/
    cast_not_support,/*extend*/
    cast_not_support,/*unknown*/
    float_text,/*text*/
    float_bit,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    cast_not_support,/*otimestamp*/
    cast_inconsistent_types,/*raw*/
    cast_not_expected,/*interval*/
    cast_not_expected,/*rowid*/
    float_lob,/*lob*/
    float_json,/*json*/
    float_geometry,/*geometry*/
    cast_not_expected,/*udt*/
  },
  {
    /*double -> XXX*/
    cast_not_support,/*null*/
    double_int,/*int*/
    double_uint,/*uint*/
    double_float,/*float*/
    double_double,/*double*/
    double_number,/*number*/
    double_datetime,/*datetime*/
    double_date,/*date*/
    double_time,/*time*/
    double_year,/*year*/
    double_string,/*string*/
    cast_not_support,/*extend*/
    cast_not_support,/*unknown*/
    double_text,/*text*/
    double_bit,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    cast_not_support,/*otimestamp*/
    cast_inconsistent_types,/*raw*/
    cast_not_expected,/*interval*/
    cast_not_expected,/*rowid*/
    double_lob,/*lob*/
    double_json,/*json*/
    double_geometry,/*geometry*/
    cast_not_expected,/*udt*/
  },
  {
    /*number -> XXX*/
    cast_not_support,/*null*/
    number_int,/*int*/
    number_uint,/*uint*/
    number_float,/*float*/
    number_double,/*double*/
    number_number,/*number*/
    number_datetime,/*datetime*/
    number_date,/*date*/
    number_time,/*time*/
    number_year,/*year*/
    number_string,/*string*/
    cast_not_support,/*extend*/
    cast_not_support,/*unknown*/
    number_text,/*text*/
    number_bit,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    cast_not_support,/*otimestamp*/
    cast_inconsistent_types,/*raw*/
    cast_not_expected,/*interval*/
    cast_not_expected,/*rowid*/
    number_lob,/*lob*/
    number_json,/*lob*/
    number_geometry,/*geometry*/
    cast_not_expected,/*udt*/
  },
  {
    /*datetime -> XXX*/
    cast_not_support,/*null*/
    datetime_int,/*int*/
    datetime_uint,/*uint*/
    datetime_float,/*float*/
    datetime_double,/*double*/
    datetime_number,/*number*/
    datetime_datetime,/*datetime*/
    datetime_date,/*date*/
    datetime_time,/*time*/
    datetime_year,/*year*/
    datetime_string,/*string*/
    cast_not_support,/*extend*/
    cast_not_support,/*unknown*/
    datetime_text,/*text*/
    datetime_bit,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    datetime_otimestamp,/*otimestamp*/
    cast_inconsistent_types,/*raw*/
    cast_not_expected,/*interval*/
    cast_not_expected,/*rowid*/
    datetime_lob,/*lob*/
    datetime_json,/*json*/
    datetime_geometry,/*geometry*/
    cast_not_expected,/*udt*/
  },
  {
    /*date -> XXX*/
    cast_not_support,/*null*/
    date_int,/*int*/
    date_uint,/*uint*/
    date_float,/*float*/
    date_double,/*double*/
    date_number,/*number*/
    date_datetime,/*datetime*/
    cast_identity,/*date*/
    date_time,/*time*/
    date_year,/*year*/
    date_string,/*string*/
    cast_not_support,/*extend*/
    cast_not_support,/*unknown*/
    date_text,/*text*/
    date_bit,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    cast_not_support,/*otimestamp*/
    cast_inconsistent_types,/*raw*/
    cast_not_expected,/*interval*/
    cast_not_expected,/*rowid*/
    date_lob,/*lob*/
    date_json,/*json*/
    date_geometry,/*geometry*/
    cast_not_expected,/*udt*/
  },
  {
    /*time -> XXX*/
    cast_not_support,/*null*/
    time_int,/*int*/
    time_uint,/*uint*/
    time_float,/*float*/
    time_double,/*double*/
    time_number,/*number*/
    time_datetime,/*datetime*/
    time_date,/*date*/
    cast_identity,/*time*/
    time_year,/*year*/
    time_string,/*string*/
    cast_not_support,/*extend*/
    cast_not_support,/*unknown*/
    time_text,/*text*/
    time_bit,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    cast_not_support,/*otimestamp*/
    cast_inconsistent_types,/*raw*/
    cast_not_expected,/*interval*/
    cast_not_expected,/*rowid*/
    time_lob,/*lob*/
    time_json,/*json*/
    time_geometry,/*geometry*/
    cast_not_expected,/*udt*/
  },
  {
    /*year -> XXX*/
    cast_not_support,/*null*/
    year_int,/*int*/
    year_uint,/*uint*/
    year_float,/*float*/
    year_double,/*double*/
    year_number,/*number*/
    year_datetime,/*datetime*/
    year_date,/*date*/
    year_time,/*time*/
    cast_identity,/*year*/
    year_string,/*string*/
    cast_not_support,/*extend*/
    cast_not_support,/*unknown*/
    year_text,/*text*/
    year_bit,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    cast_not_support,/*otimestamp*/
    cast_inconsistent_types,/*raw*/
    cast_not_expected,/*interval*/
    cast_not_expected,/*rowid*/
    year_lob,/*lob*/
    year_json,/*json*/
    year_geometry,/*geometry*/
    cast_not_expected,/*udt*/
  },
  {
    /*string -> XXX*/
    cast_not_support,/*null*/
    string_int,/*int*/
    string_uint,/*uint*/
    string_float,/*float*/
    string_double,/*double*/
    string_number,/*number*/
    string_datetime,/*datetime*/
    string_date,/*date*/
    string_time,/*time*/
    string_year,/*year*/
    string_string,/*string*/
    cast_not_support,/*extend*/
    cast_not_support,/*unknown*/
    string_text,/*text*/
    string_bit,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    string_otimestamp,/*otimestamp*/
    cast_inconsistent_types,/*raw*/
    cast_not_expected,/*interval*/
    cast_not_expected,/*rowid*/
    string_lob,/*lob*/
    string_json,/*json*/
    string_geometry,/*geometry*/
    cast_not_expected,/*udt*/
  },
  {
    /*extend -> XXX*/
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
    cast_identity,/*extend*/
    cast_not_support,/*unknown*/
    cast_not_support,/*text*/
    cast_not_support,/*bit*/
    cast_not_support,/*enumset*/
    cast_not_support,/*enumset_inner*/
    cast_not_support,/*otimestamp*/
    cast_inconsistent_types,/*raw*/
    cast_not_expected,/*interval*/
    cast_not_expected,/*rowid*/
    cast_not_support,/*lob*/
    cast_not_support,/*json*/
    cast_not_support,/*geometry*/
    pl_extend_sql_udt,/*udt*/
  },
  {
    /*unknown -> XXX*/
    unknown_other,/*null*/
    unknown_other,/*int*/
    unknown_other,/*uint*/
    unknown_other,/*float*/
    unknown_other,/*double*/
    unknown_other,/*number*/
    unknown_other,/*datetime*/
    unknown_other,/*date*/
    unknown_other,/*time*/
    unknown_other,/*year*/
    unknown_other,/*string*/
    unknown_other,/*extend*/
    cast_identity,/*unknown*/
    cast_not_support,/*text*/
    unknown_other,/*bit*/
    unknown_other,/*enumset*/
    unknown_other,/*enumsetInner*/
    unknown_other,/*otimestamp*/
    cast_inconsistent_types,/*raw*/
    cast_not_expected,/*interval*/
    cast_not_expected,/*rowid*/
    cast_not_support,/*lob*/
    cast_not_support,/*json*/
    cast_not_support,/*geometry*/
    cast_not_expected,/*udt*/
  },
  {
    /*text -> XXX*/
    cast_not_support,/*null*/
    text_int,/*int*/
    text_uint,/*uint*/
    text_float,/*float*/
    text_double,/*double*/
    text_number,/*number*/
    text_datetime,/*datetime*/
    text_date,/*date*/
    text_time,/*time*/
    text_year,/*year*/
    text_string,/*string*/
    cast_not_support,/*extend*/
    cast_not_support,/*unknown*/
    text_text,/*text*/
    text_bit,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    text_otimestamp,/*otimestamp*/
    cast_inconsistent_types,/*raw*/
    cast_not_expected,/*interval*/
    cast_not_expected,/*rowid*/
    text_lob,/*lob*/
    text_json,/*json*/
    string_geometry,/*geometry*/
    cast_not_expected,/*udt*/
  },
  {
    /*bit -> XXX*/
    cast_not_support,/*null*/
    bit_int,/*int*/
    bit_uint,/*uint*/
    bit_float,/*float*/
    bit_double,/*double*/
    bit_number,/*number*/
    bit_datetime,/*datetime*/
    bit_date,/*date*/
    bit_time,/*time*/
    bit_year,/*year*/
    bit_string,/*string*/
    cast_not_support,/*extend*/
    cast_not_support,/*unknown*/
    bit_text,/*text*/
    bit_bit,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    cast_not_support,/*otimestamp*/
    cast_inconsistent_types,/*raw*/
    cast_not_expected,/*interval*/
    cast_not_expected,/*rowid*/
    bit_lob,/*lob*/
    bit_json,/*lob*/
    bit_geometry,/*geometry*/
    cast_not_expected,/*udt*/
  },
  {
    /*enum -> XXX*/
    cast_not_support,/*null*/
    enumset_int,/*int*/
    enumset_uint,/*uint*/
    enumset_float,/*float*/
    enumset_double,/*double*/
    enumset_number,/*number*/
    cast_not_expected,/*datetime*/
    cast_not_expected,/*date*/
    cast_not_expected,/*time*/
    enumset_year,/*year*/
    cast_not_expected,/*string*/
    cast_not_support,/*extend*/
    cast_not_support,/*unknown*/
    cast_not_expected,/*text*/
    enumset_bit,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    cast_not_support,/*otimestamp*/
    cast_inconsistent_types,/*raw*/
    cast_not_expected,/*interval*/
    cast_not_expected,/*rowid*/
    cast_not_expected,/*lob*/
    cast_not_expected,/*json*/
    cast_not_expected,/*geometry*/
    cast_not_expected,/*udt*/
  },
  {
    /*enumset_inner -> XXX*/
    cast_not_support,/*null*/
    enumset_inner_int,/*int*/
    enumset_inner_uint,/*uint*/
    enumset_inner_float,/*float*/
    enumset_inner_double,/*double*/
    enumset_inner_number,/*number*/
    enumset_inner_datetime,/*datetime*/
    enumset_inner_date,/*date*/
    enumset_inner_time,/*time*/
    enumset_inner_year,/*year*/
    enumset_inner_string,/*string*/
    cast_not_support,/*extend*/
    cast_not_support,/*unknown*/
    cast_not_support,/*text*/
    enumset_inner_bit,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    cast_not_expected,/*otimestamp*/
    cast_inconsistent_types,/*raw*/
    cast_not_expected,/*interval*/
    cast_not_expected,/*rowid*/
    cast_not_support,/*lob*/
    cast_not_support,/*json*/
    cast_not_support,/*geometry*/
    cast_not_expected,/*udt*/
  },
  {
    /*otimestamp -> XXX*/
    cast_not_expected,/*null*/
    cast_not_expected,/*int*/
    cast_not_expected,/*uint*/
    cast_not_expected,/*float*/
    cast_not_expected,/*double*/
    cast_not_expected,/*number*/
    cast_not_expected,/*datetime*/
    cast_not_expected,/*date*/
    cast_not_expected,/*time*/
    cast_not_expected,/*year*/
    cast_not_expected,/*string*/
    cast_not_expected,/*extend*/
    cast_not_expected,/*unknown*/
    cast_not_expected,/*text*/
    cast_not_expected,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    otimestamp_otimestamp,/*otimestamp*/
    cast_inconsistent_types,/*raw*/
    cast_not_expected,/*interval*/
    cast_not_expected,/*rowid*/
    cast_not_expected,/*lob*/
    cast_not_expected,/*json*/
    cast_not_expected,/*geometry*/
    cast_not_expected,/*udt*/
  },
  {
    /*raw -> XXX*/
    cast_not_expected,/*null*/
    cast_not_expected,/*int*/
    cast_not_expected,/*uint*/
    cast_not_expected,/*float*/
    cast_not_expected,/*double*/
    cast_not_expected,/*number*/
    cast_not_expected,/*datetime*/
    cast_not_expected,/*date*/
    cast_not_expected,/*time*/
    cast_not_expected,/*year*/
    cast_not_expected,/*string*/
    cast_not_expected,/*extend*/
    cast_not_expected,/*unknown*/
    cast_not_expected,/*text*/
    cast_not_expected,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    cast_not_expected,/*otimestamp*/
    cast_not_expected,/*raw*/
    cast_not_expected,/*interval*/
    cast_not_expected,/*rowid*/
    cast_not_expected,/*lob*/
    cast_not_expected,/*json*/
    cast_not_expected,/*geometry*/
    cast_not_expected,/*udt*/
  },
  {
    /*interval -> XXX*/
    cast_not_expected,/*null*/
    cast_not_expected,/*int*/
    cast_not_expected,/*uint*/
    cast_not_expected,/*float*/
    cast_not_expected,/*double*/
    cast_not_expected,/*number*/
    cast_not_expected,/*datetime*/
    cast_not_expected,/*date*/
    cast_not_expected,/*time*/
    cast_not_expected,/*year*/
    cast_not_expected,/*string*/
    cast_not_expected,/*extend*/
    cast_not_expected,/*unknown*/
    cast_not_expected,/*text*/
    cast_not_expected,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    cast_not_expected,/*otimestamp*/
    cast_not_expected,/*raw*/
    cast_not_expected,/*interval*/
    cast_not_expected,/*rowid*/
    cast_not_expected,/*lob*/
    cast_not_expected,/*json*/
    cast_not_expected,/*geometry*/
    cast_not_expected,/*udt*/
  },
  {
    /*rowid -> XXX*/
    cast_not_expected,/*null*/
    cast_not_expected,/*int*/
    cast_not_expected,/*uint*/
    cast_not_expected,/*float*/
    cast_not_expected,/*double*/
    cast_not_expected,/*number*/
    cast_not_expected,/*datetime*/
    cast_not_expected,/*date*/
    cast_not_expected,/*time*/
    cast_not_expected,/*year*/
    cast_not_expected,/*string*/
    cast_not_expected,/*extend*/
    cast_not_expected,/*unknown*/
    cast_not_expected,/*text*/
    cast_not_expected,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    cast_not_expected,/*otimestamp*/
    cast_not_expected,/*raw*/
    cast_not_expected,/*interval*/
    cast_not_expected,/*rowid*/
    cast_not_expected,/*lob*/
    cast_not_expected,/*json*/
    cast_not_expected,/*geometry*/
    cast_not_expected,/*udt*/
  },
  {
    /*lob -> XXX*/
    cast_not_support,/*null*/
    lob_int,/*int*/
    lob_uint,/*uint*/
    lob_float,/*float*/
    lob_double,/*double*/
    lob_number,/*number*/
    lob_datetime,/*datetime*/
    lob_date,/*date*/
    lob_time,/*time*/
    lob_year,/*year*/
    lob_string,/*string*/
    cast_not_support,/*extend*/
    cast_not_support,/*unknown*/
    lob_text,/*text*/
    lob_bit,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    lob_otimestamp,/*otimestamp*/
    cast_inconsistent_types,/*raw*/
    cast_not_expected,/*interval*/
    cast_not_expected,/*rowid*/
    lob_lob,/*lob*/
    lob_json,/*json*/
    lob_geometry,/*geometry*/
    cast_not_expected,/*udt*/
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
    json_lob,/*lob*/
    json_json,/*json*/
    json_geometry,/*geometry*/
    cast_not_expected,/*udt*/
  },
  {
    /*geometry -> XXX*/
    cast_not_support,/*null*/
    geometry_int,/*int*/
    geometry_uint,/*uint*/
    geometry_float,/*float*/
    geometry_double,/*double*/
    geometry_number,/*number*/
    geometry_datetime,/*datetime*/
    geometry_date,/*date*/
    geometry_time,/*time*/
    geometry_year,/*year*/
    geometry_string,/*string*/
    cast_not_support,/*extend*/
    cast_not_support,/*unknown*/
    geometry_string,/*text*/
    geometry_bit,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    geometry_otimestamp,/*otimestamp*/
    cast_inconsistent_types,/*raw*/
    cast_not_expected,/*interval*/
    cast_not_expected,/*rowid*/
    cast_not_expected,/*lob*/
    geometry_json,/*json*/
    geometry_geometry,/*geometry*/
    cast_not_expected,/*udt*/
  },
  {
    /*udt -> XXX*/
    cast_not_expected,/*null*/
    cast_not_expected,/*int*/
    cast_not_expected,/*uint*/
    cast_not_expected,/*float*/
    cast_not_expected,/*double*/
    cast_not_expected,/*number*/
    cast_not_expected,/*datetime*/
    cast_not_expected,/*date*/
    cast_not_expected,/*time*/
    cast_not_expected,/*year*/
    cast_not_expected,/*string*/
    cast_not_expected,/*extend*/
    cast_not_expected,/*unknown*/
    cast_not_expected,/*text*/
    cast_not_expected,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    cast_not_expected,/*otimestamp*/
    cast_not_expected,/*raw*/
    cast_not_expected,/*interval*/
    cast_not_expected,/*rowid*/
    cast_not_expected,/*lob*/
    cast_not_expected,/*json*/
    cast_not_expected,/*geometry*/
    cast_not_expected,/*udt*/
  },
};

ObObjCastFunc OBJ_CAST_ORACLE_EXPLICIT[ObMaxTC][ObMaxTC] =
{
  {
    /*null -> XXX*/
    cast_identity,/*null*/
    cast_identity,/*int*/
    cast_identity,/*uint*/
    cast_identity,/*float*/
    cast_identity,/*double*/
    cast_identity,/*number*/
    cast_identity,/*datetime*/
    cast_identity,/*date*/
    cast_identity,/*time*/
    cast_identity,/*year*/
    cast_identity,/*string*/
    cast_identity,/*extend*/
    cast_identity,/*unknown*/
    cast_identity,/*text*/
    cast_identity,/*bit*/
    cast_identity,/*enumset*/
    cast_identity,/*enumsetInner*/
    cast_identity,/*otimestamp*/
    cast_identity,/*raw*/
    cast_identity,/*interval*/
    cast_identity,/*rowid*/
    cast_identity,/*lob*/
    cast_identity,/*json*/
    cast_not_support,/*geometry*/
    cast_to_udt_not_support,/*udt*/
  },
  {
    /*int -> XXX*/
    cast_not_support,/*null*/
    int_int,/*int*/
    int_uint,/*uint*/
    int_float,/*float*/
    int_double,/*double*/
    int_number,/*number*/
    cast_not_support,/*datetime*/
    cast_not_support,/*date*/
    cast_not_support,/*time*/
    cast_not_support,/*year*/
    int_string,/*string*/
    cast_not_support,/*extend*/
    cast_not_support,/*unknown*/
    int_text,/*text*/
    int_bit,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    cast_not_support,/*otimestamp*/
    cast_not_support,/*raw*/
    cast_not_support,/*interval*/
    cast_not_support,/*rowid*/
    cast_inconsistent_types,/*lob*/
    cast_inconsistent_types,/*json not support oracle yet*/
    cast_not_support,/*geometry*/ // geometry not support oracle mode now
    cast_to_udt_not_support,/*udt*/
  },
  {
    /*uint -> XXX*/
    cast_not_support,/*null*/
    uint_int,/*int*/
    uint_uint,/*uint*/
    uint_float,/*float*/
    uint_double,/*double*/
    uint_number,/*number*/
    cast_not_support,/*datetime*/
    cast_not_support,/*date*/
    cast_not_support,/*time*/
    cast_not_support,/*year*/
    uint_string,/*string*/
    cast_not_support,/*extend*/
    cast_not_support,/*unknown*/
    uint_text,/*text*/
    uint_bit,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    cast_not_support,/*otimestamp*/
    cast_not_support,/*raw*/
    cast_not_support,/*interval*/
    cast_not_support,/*rowid*/
    cast_inconsistent_types,/*lob*/
    cast_inconsistent_types,/* json */
    cast_not_support,/*geometry*/ // geometry not support oracle mode now
    cast_to_udt_not_support,/*udt*/
  },
  {
    /*float -> XXX*/
    cast_not_support,/*null*/
    float_int,/*int*/
    float_uint,/*uint*/
    float_float,/*float*/
    float_double,/*double*/
    float_number,/*number*/
    cast_not_support,/*datetime*/
    cast_not_support,/*date*/
    cast_not_support,/*time*/
    cast_not_support,/*year*/
    float_string,/*string*/
    cast_not_support,/*extend*/
    cast_not_support,/*unknown*/
    float_text,/*text*/
    float_bit,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    cast_not_support,/*otimestamp*/
    cast_not_support,/*raw*/
    cast_not_support,/*interval*/
    cast_not_support,/*rowid*/
    cast_inconsistent_types,/*lob*/
    cast_inconsistent_types,/*json */
    cast_not_support,/*geometry*/ // geometry not support oracle mode now
    cast_to_udt_not_support,/*udt*/
  },
  {
    /*double -> XXX*/
    cast_not_support,/*null*/
    double_int,/*int*/
    double_uint,/*uint*/
    double_float,/*float*/
    double_double,/*double*/
    double_number,/*number*/
    cast_not_support,/*datetime*/
    cast_not_expected,/*date*/
    cast_not_support,/*time*/
    cast_not_support,/*year*/
    double_string,/*string*/
    cast_not_support,/*extend*/
    cast_not_support,/*unknown*/
    double_text,/*text*/
    double_bit,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    cast_not_support,/*otimestamp*/
    cast_not_support,/*raw*/
    cast_not_support,/*interval*/
    cast_not_support,/*rowid*/
    cast_inconsistent_types,/*lob*/
    cast_inconsistent_types,/*json */
    cast_not_support,/*geometry*/
    cast_to_udt_not_support,/*udt*/
  },
  {
    /*number -> XXX*/
    cast_not_support,/*null*/
    number_int,/*int*/
    number_uint,/*uint*/
    number_float,/*float*/
    number_double,/*double*/
    number_number,/*number*/
    cast_not_support,/*datetime*/
    cast_not_support,/*date*/
    cast_not_support,/*time*/
    cast_not_support,/*year*/
    number_string,/*string*/
    cast_not_support,/*extend*/
    cast_not_support,/*unknown*/
    number_text,/*text*/
    number_bit,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    cast_not_support,/*otimestamp*/
    cast_not_support,/*raw*/
    cast_not_support,/*interval*/
    cast_not_support,/*rowid*/
    cast_inconsistent_types,/*lob*/
    cast_inconsistent_types,/*json */
    cast_not_support,/*geometry*/ // geometry not support oracle mode now
    cast_to_udt_not_support,/*udt*/
  },
  {
    /*datetime -> XXX*/
    cast_not_support,/*null*/
    cast_not_support,/*int*/
    cast_not_support,/*uint*/
    cast_not_support,/*float*/
    cast_not_support,/*double*/
    cast_not_support,/*number*/
    datetime_datetime,/*datetime*/
    cast_not_support,/*date*/
    cast_not_support,/*time*/
    cast_not_support,/*year*/
    datetime_string,/*string*/
    cast_not_support,/*extend*/
    cast_not_support,/*unknown*/
    datetime_text,/*text*/
    datetime_bit,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    datetime_otimestamp,/*otimestamp*/
    cast_not_support,/*raw*/
    cast_not_support,/*interval*/
    cast_not_support,/*rowid*/
    cast_inconsistent_types,/*lob*/
    cast_inconsistent_types,/*json */
    cast_not_support,/*geometry*/ // geometry not support oracle mode now
    cast_to_udt_not_support,/*udt*/
  },
  {
    /*date -> XXX*/
    cast_not_expected,/*null*/
    cast_not_expected,/*int*/
    cast_not_expected,/*uint*/
    cast_not_expected,/*float*/
    cast_not_expected,/*double*/
    cast_not_expected,/*number*/
    cast_not_expected,/*datetime*/
    cast_not_expected,/*date*/
    cast_not_expected,/*time*/
    cast_not_expected,/*year*/
    cast_not_expected,/*string*/
    cast_not_expected,/*extend*/
    cast_not_expected,/*unknown*/
    cast_not_expected,/*text*/
    cast_not_expected,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    cast_not_expected,/*otimestamp*/
    cast_not_expected,/*raw*/
    cast_not_expected,/*interval*/
    cast_not_expected,/*rowid*/
    cast_not_expected,/*lob*/
    cast_not_expected,/*json */
    cast_not_support,/*geometry*/ // geometry not support oracle mode now
    cast_to_udt_not_support,/*udt*/
  },
  {
    /*time -> XXX*/
    cast_not_support,/*null*/
    cast_not_expected,/*int*/
    cast_not_expected,/*uint*/
    cast_not_expected,/*float*/
    cast_not_expected,/*double*/
    cast_not_expected,/*number*/
    cast_not_expected,/*datetime*/
    cast_not_expected,/*date*/
    cast_not_expected,/*time*/
    cast_not_expected,/*year*/
    cast_not_expected,/*string*/
    cast_not_expected,/*extend*/
    cast_not_expected,/*unknown*/
    cast_not_expected,/*text*/
    cast_not_expected,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    cast_not_expected,/*otimestamp*/
    cast_not_expected,/*raw*/
    cast_not_expected,/*interval*/
    cast_not_expected,/*rowid*/
    cast_not_expected,/*lob*/
    cast_not_expected,/*json */
    cast_not_support,/*geometry*/ // geometry not support oracle mode now
    cast_to_udt_not_support,/*udt*/
  },
  {
    /*year -> XXX*/
    cast_not_expected,/*null*/
    cast_not_expected,/*int*/
    cast_not_expected,/*uint*/
    cast_not_expected,/*float*/
    cast_not_expected,/*double*/
    cast_not_expected,/*number*/
    cast_not_expected,/*datetime*/
    cast_not_expected,/*date*/
    cast_not_expected,/*time*/
    cast_not_expected,/*year*/
    cast_not_expected,/*string*/
    cast_not_expected,/*extend*/
    cast_not_expected,/*unknown*/
    cast_not_expected,/*text*/
    cast_not_expected,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    cast_not_expected,/*otimestamp*/
    cast_not_expected,/*raw*/
    cast_not_expected,/*interval*/
    cast_not_expected,/*rowid*/
    cast_not_expected,/*lob*/
    cast_not_expected,/* json */
    cast_not_support,/*geometry*/ // geometry not support oracle mode now
    cast_to_udt_not_support,/*udt*/
  },
  {
    /*string -> XXX*/
    cast_not_support,/*null*/
    string_int,/*int*/
    string_uint,/*uint*/
    string_float,/*float*/
    string_double,/*double*/
    string_number,/*number*/
    string_datetime,/*datetime*/
    cast_not_expected,/*date*/
    cast_not_expected,/*time*/
    cast_not_expected,/*year*/
    string_string,/*string*/
    cast_not_support,/*extend*/
    cast_not_support,/*unknown*/
    string_text,/*text*/
    string_bit,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    string_otimestamp,/*otimestamp*/
    string_raw,/*raw*/
    string_interval,/*interval*/
    string_rowid,/*rowid*/
    cast_inconsistent_types,/*lob*/
    string_json,/*json*/
    cast_not_support,/*geometry*/ // geometry not support oracle mode now
    cast_to_udt_not_support,/*udt*/
  },
  {
    /*extend -> XXX*/
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
    cast_identity,/*extend*/
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
    cast_not_support,/*geometry*/ // geometry not support oracle mode now
    pl_extend_sql_udt,/*udt*/
  },
  {
    /*unknown -> XXX*/
    unknown_other,/*null*/
    unknown_other,/*int*/
    unknown_other,/*uint*/
    unknown_other,/*float*/
    unknown_other,/*double*/
    unknown_other,/*number*/
    unknown_other,/*datetime*/
    unknown_other,/*date*/
    unknown_other,/*time*/
    unknown_other,/*year*/
    unknown_other,/*string*/
    unknown_other,/*extend*/
    cast_identity,/*unknown*/
    cast_not_support,/*text*/
    unknown_other,/*bit*/
    unknown_other,/*enumset*/
    unknown_other,/*enumsetInner*/
    unknown_other,/*otimestamp*/
    unknown_other,/*raw*/
    unknown_other,/*interval*/
    unknown_other,/*rowid*/
    cast_not_support,/*lob*/
    cast_not_support,/*json*/
    cast_not_support,/*geometry*/ // geometry not support oracle mode now
    cast_to_udt_not_support,/*udt*/
  },
  {
    /*text -> XXX*/
    cast_not_support,/*null*/
    text_int,/*int*/
    text_uint,/*uint*/
    text_float,/*float*/
    text_double,/*double*/
    text_number,/*number*/
    text_datetime,/*datetime*/
    cast_not_expected,/*date*/
    cast_not_expected,/*time*/
    cast_not_expected,/*year*/
    text_string,/*string*/
    cast_not_support,/*extend*/
    cast_not_support,/*unknown*/
    text_text,/*text*/
    text_bit,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    text_otimestamp,/*otimestamp*/
    text_raw,/*raw*/
    text_interval,/*interval*/
    cast_not_support,/*rowid*/
    cast_inconsistent_types,/*lob*/
    text_json,/*json*/
    cast_not_support,/*geometry*/ // geometry not support oracle mode now
    cast_to_udt_not_support,/*udt*/
  },
  {
    /*bit -> XXX*/
    cast_not_expected,/*null*/
    cast_not_expected,/*int*/
    cast_not_expected,/*uint*/
    cast_not_expected,/*float*/
    cast_not_expected,/*double*/
    cast_not_expected,/*number*/
    cast_not_expected,/*datetime*/
    cast_not_expected,/*date*/
    cast_not_expected,/*time*/
    cast_not_expected,/*year*/
    cast_not_expected,/*string*/
    cast_not_expected,/*extend*/
    cast_not_expected,/*unknown*/
    cast_not_expected,/*text*/
    cast_not_expected,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    cast_not_expected,/*otimestamp*/
    cast_not_expected,/*raw*/
    cast_not_expected,/*interval*/
    cast_not_expected,/*rowid*/
    cast_not_expected,/*lob*/
    cast_not_expected,/*json*/
    cast_not_support,/*geometry*/ // geometry not support oracle mode now
    cast_to_udt_not_support,/*udt*/
  },
  {
    /*enum -> XXX*/
    cast_not_expected,/*null*/
    cast_not_expected,/*int*/
    cast_not_expected,/*uint*/
    cast_not_expected,/*float*/
    cast_not_expected,/*double*/
    cast_not_expected,/*number*/
    cast_not_expected,/*datetime*/
    cast_not_expected,/*date*/
    cast_not_expected,/*time*/
    cast_not_expected,/*year*/
    cast_not_expected,/*string*/
    cast_not_expected,/*extend*/
    cast_not_expected,/*unknown*/
    cast_not_expected,/*text*/
    cast_not_expected,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    cast_not_expected,/*otimestamp*/
    cast_not_expected,/*raw*/
    cast_not_expected,/*interval*/
    cast_not_expected,/*rowid*/
    cast_not_expected,/*lob*/
    cast_not_expected,/*json*/
    cast_not_support,/*geometry*/ // geometry not support oracle mode now
    cast_to_udt_not_support,/*udt*/
  },
  {
    /*enumset_inner -> XXX*/
    cast_not_expected,/*null*/
    cast_not_expected,/*int*/
    cast_not_expected,/*uint*/
    cast_not_expected,/*float*/
    cast_not_expected,/*double*/
    cast_not_expected,/*number*/
    cast_not_expected,/*datetime*/
    cast_not_expected,/*date*/
    cast_not_expected,/*time*/
    cast_not_expected,/*year*/
    cast_not_expected,/*string*/
    cast_not_expected,/*extend*/
    cast_not_expected,/*unknown*/
    cast_not_expected,/*text*/
    cast_not_expected,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    cast_not_expected,/*otimestamp*/
    cast_not_expected,/*raw*/
    cast_not_expected,/*interval*/
    cast_not_expected,/*rowid*/
    cast_not_expected,/*lob*/
    cast_not_expected,/*json*/
    cast_not_support,/*geometry*/ // geometry not support oracle mode now
    cast_to_udt_not_support,/*udt*/
  },
  {
    /*otimestamp -> XXX*/
    cast_not_support,/*null*/
    cast_not_support,/*int*/
    cast_not_support,/*uint*/
    cast_not_support,/*float*/
    cast_not_support,/*double*/
    cast_not_support,/*number*/
    otimestamp_datetime,/*datetime*/
    cast_not_expected,/*date*/
    cast_not_expected,/*time*/
    cast_not_expected,/*year*/
    otimestamp_string,/*string*/
    cast_not_support,/*extend*/
    cast_not_support,/*unknown*/
    otimestamp_text,/*text*/
    cast_not_support,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    otimestamp_otimestamp,/*otimestamp*/
    cast_not_support,/*raw*/
    cast_not_support,/*interval*/
    cast_not_support,/*rowid*/
    cast_inconsistent_types,/*lob*/
    cast_inconsistent_types_json,/*json*/
    cast_not_support,/*geometry*/ // geometry not support oracle mode now
    cast_to_udt_not_support,/*udt*/
  },
  {
    /*raw -> XXX*/
    cast_not_support,/*null*/
    cast_not_support,/*int*/
    cast_not_support,/*uint*/
    cast_not_support,/*float*/
    cast_not_support,/*double*/
    cast_not_support,/*number*/
    cast_not_support,/*datetime*/
    cast_not_expected,/*date*/
    cast_not_expected,/*time*/
    cast_not_expected,/*year*/
    raw_string,/*string*/
    cast_not_support,/*extend*/
    cast_not_support,/*unknown*/
    raw_text,/*text*/
    cast_not_support,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    cast_not_support,/*otimestamp*/
    raw_raw,/*raw*/
    cast_not_support,/*interval*/
    cast_not_support,/*rowid*/
    cast_inconsistent_types,/*lob*/
    cast_inconsistent_types_json,/*json*/
    cast_not_support,/*geometry*/ // geometry not support oracle mode now
    cast_to_udt_not_support,/*udt*/
  },
  {
    /*interval -> XXX*/
    cast_not_expected,/*null*/
    cast_not_expected,/*int*/
    cast_not_expected,/*uint*/
    cast_not_expected,/*float*/
    cast_not_expected,/*double*/
    cast_not_expected,/*number*/
    cast_not_expected,/*datetime*/
    cast_not_expected,/*date*/
    cast_not_expected,/*time*/
    cast_not_expected,/*year*/
    interval_string,/*string*/
    cast_not_expected,/*extend*/
    cast_not_expected,/*unknown*/
    cast_not_expected,/*text*/
    cast_not_expected,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    cast_not_expected,/*otimestamp*/
    cast_not_expected,/*raw*/
    interval_interval,/*interval*/
    cast_not_support,/*rowid*/
    cast_inconsistent_types,/*lob*/
    cast_inconsistent_types_json,/*json*/
    cast_not_support,/*geometry*/ // geometry not support oracle mode now
    cast_to_udt_not_support,/*udt*/
  },
  {
    /*rowid -> XXX*/
    cast_not_support,/*null*/
    cast_not_support,/*int*/
    cast_not_support,/*uint*/
    cast_not_support,/*float*/
    cast_not_support,/*double*/
    cast_not_support,/*number*/
    cast_not_support,/*datetime*/
    cast_not_expected,/*date*/
    cast_not_expected,/*time*/
    cast_not_expected,/*year*/
    rowid_string,/*string*/
    cast_not_expected,/*extend*/
    cast_not_expected,/*unknown*/
    cast_not_expected,/*text*/
    cast_not_expected,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    cast_not_support,/*otimestamp*/
    cast_not_support,/*raw*/
    cast_not_support,/*interval*/
    rowid_rowid,/*rowid*/
    cast_inconsistent_types,/*lob*/
    cast_inconsistent_types_json,/*json*/
    cast_not_support,/*geometry*/ // geometry not support oracle mode now
    cast_to_udt_not_support,/*udt*/
  },
  {
    /*lob -> XXX*/
    cast_not_support,/*null*/
    lob_int,/*int*/
    lob_uint,/*uint*/
    lob_float,/*float*/
    lob_double,/*double*/
    lob_number,/*number*/
    lob_datetime,/*datetime*/
    cast_not_expected,/*date*/
    cast_not_expected,/*time*/
    cast_not_expected,/*year*/
    lob_string,/*string*/
    cast_not_support,/*extend*/
    cast_not_support,/*unknown*/
    lob_text,/*text*/
    lob_bit,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    lob_otimestamp,/*otimestamp*/
    lob_raw,/*raw*/
    lob_interval,/*interval*/
    cast_not_support,/*rowid*/
    cast_inconsistent_types,/*lob*/
    lob_json,/*json*/
    cast_not_support,/*geometry*/ // geometry not support oracle mode now
    cast_to_udt_not_support,/*udt*/
  },
  {
    /*json -> XXX, not support oracle currently*/
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
    json_string,/*string*/
    cast_not_support,/*extend*/
    cast_not_support,/*unknown*/
    json_string,/*text*/
    cast_not_support,/*bit*/
    cast_not_support,/*enumset*/
    cast_not_support,/*enumset_inner*/
    cast_not_support,/*otimestamp*/
    cast_not_support,/*raw*/
    cast_not_support,/*interval*/
    cast_not_support,/*rowid*/
    json_lob,/*lob*/
    json_json,/*json*/
    cast_not_support,/*geometry*/ // geometry not support oracle mode now
    cast_to_udt_not_support,/*udt*/
  },
  {
    /*geometry -> XXX, not support oracle currently*/
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
    cast_not_support,/*geometry*/
    cast_not_support,/*udt*/
  },
  {
    /*udt -> XXX, not support oracle currently*/
    cast_udt_to_other_not_support,/*null*/
    cast_udt_to_other_not_support,/*int*/
    cast_udt_to_other_not_support,/*uint*/
    cast_udt_to_other_not_support,/*float*/
    cast_udt_to_other_not_support,/*double*/
    cast_udt_to_other_not_support,/*number*/
    cast_udt_to_other_not_support,/*datetime*/
    cast_udt_to_other_not_support,/*date*/
    cast_udt_to_other_not_support,/*time*/
    cast_udt_to_other_not_support,/*year*/
    udt_string,/*string*/
    sql_udt_pl_extend,/*extend*/
    cast_udt_to_other_not_support,/*unknown*/
    cast_udt_to_other_not_support,/*text*/
    cast_udt_to_other_not_support,/*bit*/
    cast_udt_to_other_not_support,/*enumset*/
    cast_udt_to_other_not_support,/*enumset_inner*/
    cast_udt_to_other_not_support,/*otimestamp*/
    cast_udt_to_other_not_support,/*raw*/
    cast_udt_to_other_not_support,/*interval*/
    cast_udt_to_other_not_support,/*rowid*/
    cast_udt_to_other_not_support,/*lob*/
    cast_udt_to_other_not_support,/*json*/
    cast_udt_to_other_not_support,/*geometry*/
    cast_udt_to_other_not_support,/*udt*/
  },
};

/*
 * 1. I think not_support() means something we will support in future, and cast_not_expected() means
 *    something wrong, so there is no not_support() appears in OBJ_CAST_ORACLE_IMPLICIT now.
 * 2. int and uint are needed in inner operation, for example, some system variable is uint,
 *    like auto_increment_increment, which will be cast to uint in load_default_sys_variable().
 *    so this matrix allows cast from or to int / uint.
 * 3. we can't use ObObjOType as index of this matrix, because int / uint too.
 */

ObObjCastFunc OBJ_CAST_ORACLE_IMPLICIT[ObMaxTC][ObMaxTC] =
{
  {
    /*null -> XXX*/
    cast_identity,/*null*/
    cast_identity,/*int*/
    cast_identity,/*uint*/
    cast_identity,/*float*/
    cast_identity,/*double*/
    cast_identity,/*number*/
    cast_identity,/*datetime*/
    cast_not_expected,/*date*/
    cast_not_expected,/*time*/
    cast_not_expected,/*year*/
    cast_identity,/*string*/
    cast_not_expected,/*extend*/
    cast_not_expected,/*unknown*/
    cast_identity,/*text*/
    cast_not_expected,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumsetInner*/
    cast_identity,/*otimestamp*/
    cast_identity,/*raw*/
    cast_identity,/*interval*/
    cast_identity, /*rowid*/
    cast_identity,/*lob*/
    cast_identity,/*json*/
    cast_not_support,/*geometry*/
    cast_to_udt_not_support,/*udt*/
  },
  {
    /*int -> XXX*/
    cast_not_expected,/*null*/
    int_int,/*int*/
    int_uint,/*uint*/
    int_float,/*float*/
    int_double,/*double*/
    int_number,/*number*/
    cast_inconsistent_types,/*datetime*/
    cast_not_expected,/*date*/
    cast_not_expected,/*time*/
    cast_not_expected,/*year*/
    int_string,/*string*/
    cast_not_expected,/*extend*/
    cast_not_expected,/*unknown*/
    int_text,/*text*/
    cast_not_expected,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    cast_inconsistent_types,/*otimestamp*/
    cast_inconsistent_types,/*raw*/
    cast_inconsistent_types,/*interval*/
    cast_inconsistent_types,/*rowid*/
    cast_inconsistent_types,/*lob*/
    cast_inconsistent_types_json,/*json*/
    cast_not_support,/*geometry*/
    cast_to_udt_not_support,/*udt*/
  },
  {
    /*uint -> XXX*/
    cast_not_expected,/*null*/
    uint_int,/*int*/
    uint_uint,/*uint*/
    uint_float,/*float*/
    uint_double,/*double*/
    uint_number,/*number*/
    cast_inconsistent_types,/*datetime*/
    cast_not_expected,/*date*/
    cast_not_expected,/*time*/
    cast_not_expected,/*year*/
    uint_string,/*string*/
    cast_not_expected,/*extend*/
    cast_not_expected,/*unknown*/
    cast_inconsistent_types,/*text*/
    cast_not_expected,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    cast_inconsistent_types,/*otimestamp*/
    cast_inconsistent_types,/*raw*/
    cast_inconsistent_types,/*interval*/
    cast_inconsistent_types,/*rowid*/
    cast_inconsistent_types,/*lob*/
    cast_inconsistent_types_json,/*json*/
    cast_not_support,/*geometry*/
    cast_to_udt_not_support,/*udt*/
  },
  {
    /*float -> XXX*/
    cast_not_expected,/*null*/
    float_int,/*int*/
    float_uint,/*uint*/
    float_float,/*float*/
    float_double,/*double*/
    float_number,/*number*/
    cast_inconsistent_types,/*datetime*/
    cast_not_expected,/*date*/
    cast_not_expected,/*time*/
    cast_not_expected,/*year*/
    float_string,/*string*/
    cast_not_expected,/*extend*/
    cast_not_expected,/*unknown*/
    cast_inconsistent_types,/*text*/
    cast_not_expected,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    cast_inconsistent_types,/*otimestamp*/
    cast_inconsistent_types,/*raw*/
    cast_inconsistent_types,/*interval*/
    cast_inconsistent_types,/*rowid*/
    cast_inconsistent_types,/*lob*/
    cast_inconsistent_types_json,/*json*/
    cast_not_support,/*geometry*/
    cast_to_udt_not_support,/*udt*/
  },
  {
    /*double -> XXX*/
    cast_not_expected,/*null*/
    double_int,/*int*/
    double_uint,/*uint*/
    double_float,/*float*/
    double_double,/*double*/
    double_number,/*number*/
    cast_inconsistent_types,/*datetime*/
    cast_not_expected,/*date*/
    cast_not_expected,/*time*/
    cast_not_expected,/*year*/
    double_string,/*string*/
    cast_not_expected,/*extend*/
    cast_not_expected,/*unknown*/
    cast_inconsistent_types,/*text*/
    cast_not_expected,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    cast_inconsistent_types,/*otimestamp*/
    cast_inconsistent_types,/*raw*/
    cast_inconsistent_types,/*interval*/
    cast_inconsistent_types,/*rowid*/
    cast_inconsistent_types,/*lob*/
    cast_inconsistent_types_json,/*json*/
    cast_not_support,/*geometry*/
    cast_to_udt_not_support,/*udt*/
  },
  {
    /*number -> XXX*/
    cast_not_expected,/*null*/
    number_int,/*int*/
    number_uint,/*uint*/
    number_float,/*float*/
    number_double,/*double*/
    number_number,/*number*/
    cast_inconsistent_types,/*datetime*/
    cast_not_expected,/*date*/
    cast_not_expected,/*time*/
    cast_not_expected,/*year*/
    number_string,/*string*/
    cast_not_expected,/*extend*/
    cast_not_expected,/*unknown*/
    number_text,/*text*/
    cast_not_expected,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    cast_inconsistent_types,/*otimestamp*/
    cast_inconsistent_types,/*raw*/
    cast_inconsistent_types,/*interval*/
    cast_inconsistent_types,/*rowid*/
    number_lob,/*lob*/
    cast_inconsistent_types_json,/*json*/
    cast_not_support,/*geometry*/
    cast_to_udt_not_support,/*udt*/
  },
  {
    /*datetime -> XXX*/
    cast_not_expected,/*null*/
    cast_inconsistent_types,/*int*/
    cast_inconsistent_types,/*uint*/
    cast_inconsistent_types,/*float*/
    cast_inconsistent_types,/*double*/
    cast_inconsistent_types,/*number*/
    datetime_datetime,/*datetime*/
    cast_not_expected,/*date*/
    cast_not_expected,/*time*/
    cast_not_expected,/*year*/
    datetime_string,/*string*/
    cast_not_expected,/*extend*/
    cast_not_expected,/*unknown*/
    datetime_text,/*text*/
    cast_not_expected,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    datetime_otimestamp,/*otimestamp*/
    cast_inconsistent_types,/*raw*/
    cast_inconsistent_types,/*interval*/
    cast_inconsistent_types,/*rowid*/
    cast_inconsistent_types,/*lob*/
    cast_inconsistent_types_json,/*json*/
    cast_not_support,/*geometry*/
    cast_to_udt_not_support,/*udt*/
  },
  {
    /*date -> XXX*/
    cast_not_expected,/*null*/
    cast_not_expected,/*int*/
    cast_not_expected,/*uint*/
    cast_not_expected,/*float*/
    cast_not_expected,/*double*/
    cast_not_expected,/*number*/
    cast_not_expected,/*datetime*/
    cast_not_expected,/*date*/
    cast_not_expected,/*time*/
    cast_not_expected,/*year*/
    cast_not_expected,/*string*/
    cast_not_expected,/*extend*/
    cast_not_expected,/*unknown*/
    cast_not_expected,/*text*/
    cast_not_expected,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    cast_not_expected,/*otimestamp*/
    cast_not_expected,/*raw*/
    cast_not_expected,/*interval*/
    cast_not_expected,/*rowid*/
    cast_not_expected,/*lob*/
    cast_inconsistent_types_json,/*json*/
    cast_not_support,/*geometry*/
    cast_to_udt_not_support,/*udt*/
  },
  {
    /*time -> XXX*/
    cast_not_expected,/*null*/
    cast_not_expected,/*int*/
    cast_not_expected,/*uint*/
    cast_not_expected,/*float*/
    cast_not_expected,/*double*/
    cast_not_expected,/*number*/
    cast_not_expected,/*datetime*/
    cast_not_expected,/*date*/
    cast_not_expected,/*time*/
    cast_not_expected,/*year*/
    cast_not_expected,/*string*/
    cast_not_expected,/*extend*/
    cast_not_expected,/*unknown*/
    cast_not_expected,/*text*/
    cast_not_expected,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    cast_not_expected,/*otimestamp*/
    cast_not_expected,/*raw*/
    cast_not_expected,/*interval*/
    cast_not_expected,/*rowid*/
    cast_not_expected,/*lob*/
    cast_inconsistent_types_json,/*json*/
    cast_not_support,/*geometry*/
    cast_to_udt_not_support,/*udt*/
  },
  {
    /*year -> XXX*/
    cast_not_expected,/*null*/
    cast_not_expected,/*int*/
    cast_not_expected,/*uint*/
    cast_not_expected,/*float*/
    cast_not_expected,/*double*/
    cast_not_expected,/*number*/
    cast_not_expected,/*datetime*/
    cast_not_expected,/*date*/
    cast_not_expected,/*time*/
    cast_not_expected,/*year*/
    cast_not_expected,/*string*/
    cast_not_expected,/*extend*/
    cast_not_expected,/*unknown*/
    cast_not_expected,/*text*/
    cast_not_expected,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    cast_not_expected,/*otimestamp*/
    cast_not_expected,/*raw*/
    cast_not_expected,/*interval*/
    cast_not_expected,/*rowid*/
    cast_not_expected,/*lob*/
    cast_inconsistent_types_json,/*json*/
    cast_not_support,/*geometry*/
    cast_to_udt_not_support,/*udt*/
  },
  {
    /*string -> XXX*/
    cast_not_expected,/*null*/
    string_int,/*int*/
    string_uint,/*uint*/
    string_float,/*float*/
    string_double,/*double*/
    string_number,/*number*/
    string_datetime,/*datetime*/
    cast_not_expected,/*date*/
    cast_not_expected,/*time*/
    cast_not_expected,/*year*/
    string_string,/*string*/
    cast_not_expected,/*extend*/
    cast_not_expected,/*unknown*/
    string_text,/*text*/
    cast_not_expected,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    string_otimestamp,/*otimestamp*/
    string_raw,/*raw*/
    string_interval,/*interval*/
    string_rowid,/*rowid*/
    string_lob,/*lob*/
    string_json,/*json*/
    cast_not_support,/*geometry*/
    string_sql_udt,/*udt*/
  },
  {
    /*extend -> XXX*/
    cast_not_expected,/*null*/
    cast_not_expected,/*int*/
    cast_not_expected,/*uint*/
    cast_not_expected,/*float*/
    cast_not_expected,/*double*/
    cast_not_expected,/*number*/
    cast_not_expected,/*datetime*/
    cast_not_expected,/*date*/
    cast_not_expected,/*time*/
    cast_not_expected,/*year*/
    pl_extend_string,/*string*/
    cast_identity,/*extend*/
    cast_not_expected,/*unknown*/
    cast_extend_types_not_support,/*text*/
    cast_not_expected,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    cast_not_expected,/*otimestamp*/
    cast_not_expected,/*raw*/
    cast_not_expected,/*interval*/
    cast_not_expected,/*rowid*/
    cast_extend_types_not_support,/*lob*/
    cast_inconsistent_type_json_explicit,/*json*/
    cast_not_support,/*geometry*/
    pl_extend_sql_udt,/*udt*/
  },
  {
    /*unknown -> XXX*/
    cast_not_expected,/*null*/
    cast_not_expected,/*int*/
    cast_not_expected,/*uint*/
    cast_not_expected,/*float*/
    cast_not_expected,/*double*/
    cast_not_expected,/*number*/
    cast_not_expected,/*datetime*/
    cast_not_expected,/*date*/
    cast_not_expected,/*time*/
    cast_not_expected,/*year*/
    cast_not_expected,/*string*/
    cast_not_expected,/*extend*/
    cast_not_expected,/*unknown*/
    cast_not_expected,/*text*/
    cast_not_expected,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    cast_not_expected,/*otimestamp*/
    cast_not_expected,/*raw*/
    cast_not_expected,/*interval*/
    cast_not_expected,/*rowid*/
    cast_not_expected,/*lob*/
    cast_inconsistent_types_json,/*json*/
    cast_not_support,/*geometry*/
    cast_to_udt_not_support,/*udt*/
  },
  {
    /*text -> XXX*/
    cast_not_expected,/*null*/
    text_int,/*int*/
    text_uint,/*uint*/
    text_float,/*float*/
    text_double,/*double*/
    text_number,/*number*/
    text_datetime,/*datetime*/
    cast_not_expected,/*date*/
    cast_not_expected,/*time*/
    cast_not_expected,/*year*/
    text_string,/*string*/
    cast_not_expected,/*extend*/
    cast_not_expected,/*unknown*/
    text_text,/*text*/
    cast_not_expected,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    text_otimestamp,/*otimestamp*/
    text_raw,/*raw*/
    text_interval,/*interval*/
    text_rowid,/*rowid*/
    text_lob,/*lob*/
    text_json,/*json*/
    cast_not_support,/*geometry*/
    cast_to_udt_not_support,/*udt*/
  },
  {
    /*bit -> XXX*/
    cast_not_expected,/*null*/
    cast_not_expected,/*int*/
    cast_not_expected,/*uint*/
    cast_not_expected,/*float*/
    cast_not_expected,/*double*/
    cast_not_expected,/*number*/
    cast_not_expected,/*datetime*/
    cast_not_expected,/*date*/
    cast_not_expected,/*time*/
    cast_not_expected,/*year*/
    cast_not_expected,/*string*/
    cast_not_expected,/*extend*/
    cast_not_expected,/*unknown*/
    cast_not_expected,/*text*/
    cast_not_expected,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    cast_not_expected,/*otimestamp*/
    cast_not_expected,/*raw*/
    cast_not_expected,/*interval*/
    cast_not_expected,/*rowid*/
    cast_not_expected,/*lob*/
    cast_inconsistent_types_json,/*json*/
    cast_not_support,/*geometry*/
    cast_to_udt_not_support,/*udt*/
  },
  {
    /*enum -> XXX*/
    cast_not_expected,/*null*/
    cast_not_expected,/*int*/
    cast_not_expected,/*uint*/
    cast_not_expected,/*float*/
    cast_not_expected,/*double*/
    cast_not_expected,/*number*/
    cast_not_expected,/*datetime*/
    cast_not_expected,/*date*/
    cast_not_expected,/*time*/
    cast_not_expected,/*year*/
    cast_not_expected,/*string*/
    cast_not_expected,/*extend*/
    cast_not_expected,/*unknown*/
    cast_not_expected,/*text*/
    cast_not_expected,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    cast_not_expected,/*otimestamp*/
    cast_not_expected,/*raw*/
    cast_not_expected,/*interval*/
    cast_not_expected,/*rowid*/
    cast_not_expected,/*lob*/
    cast_inconsistent_types_json,/*json*/
    cast_not_support,/*geometry*/
    cast_to_udt_not_support,/*udt*/
  },
  {
    /*enumset_inner -> XXX*/
    cast_not_expected,/*null*/
    cast_not_expected,/*int*/
    cast_not_expected,/*uint*/
    cast_not_expected,/*float*/
    cast_not_expected,/*double*/
    cast_not_expected,/*number*/
    cast_not_expected,/*datetime*/
    cast_not_expected,/*date*/
    cast_not_expected,/*time*/
    cast_not_expected,/*year*/
    cast_not_expected,/*string*/
    cast_not_expected,/*extend*/
    cast_not_expected,/*unknown*/
    cast_not_expected,/*text*/
    cast_not_expected,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    cast_not_expected,/*otimestamp*/
    cast_not_expected,/*raw*/
    cast_not_expected,/*interval*/
    cast_not_expected,/*rowid*/
    cast_not_expected,/*lob*/
    cast_inconsistent_types_json,/*json*/
    cast_not_support,/*geometry*/
  },
  {
    /*otimestamp -> XXX*/
    cast_not_expected,/*null*/
    cast_inconsistent_types,/*int*/
    cast_inconsistent_types,/*uint*/
    cast_inconsistent_types,/*float*/
    cast_inconsistent_types,/*double*/
    cast_inconsistent_types,/*number*/
    otimestamp_datetime,/*datetime*/
    cast_not_expected,/*date*/
    cast_not_expected,/*time*/
    cast_not_expected,/*year*/
    otimestamp_string,/*string*/
    cast_not_expected,/*extend*/
    cast_not_expected,/*unknown*/
    cast_inconsistent_types,/*text*/
    cast_not_expected,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    otimestamp_otimestamp,/*otimestamp*/
    cast_inconsistent_types,/*raw*/
    cast_inconsistent_types,/*interval*/
    cast_inconsistent_types,/*rowid*/
    cast_inconsistent_types,/*lob*/
    cast_inconsistent_types_json,/*json*/
    cast_not_support,/*geometry*/
  },
  {
    /*raw -> XXX*/
    cast_not_expected,/*null*/
    cast_inconsistent_types,/*int*/
    cast_inconsistent_types,/*uint*/
    cast_inconsistent_types,/*float*/
    cast_inconsistent_types,/*double*/
    cast_inconsistent_types,/*number*/
    cast_inconsistent_types,/*datetime*/
    cast_not_expected,/*date*/
    cast_not_expected,/*time*/
    cast_not_expected,/*year*/
    raw_string,/*string*/
    cast_not_expected,/*extend*/
    cast_not_expected,/*unknown*/
    raw_text,/*text*/
    cast_not_expected,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    cast_inconsistent_types,/*otimestamp*/
    raw_raw,/*raw*/
    cast_inconsistent_types,/*interval*/
    cast_inconsistent_types,/*rowid*/
    raw_lob,/*lob*/
    cast_inconsistent_types_json,/*json*/
    cast_not_support,/*geometry*/
    cast_to_udt_not_support,/*udt*/
  },
  {
    /*interval -> XXX*/
    cast_not_expected,/*null*/
    cast_inconsistent_types,/*int*/
    cast_inconsistent_types,/*uint*/
    cast_inconsistent_types,/*float*/
    cast_inconsistent_types,/*double*/
    cast_inconsistent_types,/*number*/
    cast_inconsistent_types,/*datetime*/
    cast_not_expected,/*date*/
    cast_not_expected,/*time*/
    cast_not_expected,/*year*/
    interval_string,/*string*/
    cast_not_expected,/*extend*/
    cast_not_expected,/*unknown*/
    cast_inconsistent_types,/*text*/
    cast_not_expected,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    cast_inconsistent_types,/*otimestamp*/
    cast_inconsistent_types,/*raw*/
    interval_interval,/*interval*/
    cast_inconsistent_types,/*rowid*/
    cast_inconsistent_types,/*lob*/
    cast_inconsistent_types_json,/*json*/
    cast_not_support,/*geometry*/
    cast_to_udt_not_support,/*udt*/
  },
  {
    /* rowid -> XXX */
    cast_not_expected,/*null*/
    cast_inconsistent_types,/*int*/
    cast_inconsistent_types,/*uint*/
    cast_inconsistent_types,/*float*/
    cast_inconsistent_types,/*double*/
    cast_inconsistent_types,/*number*/
    cast_inconsistent_types,/*datetime*/
    cast_not_expected,/*date*/
    cast_not_expected,/*time*/
    cast_not_expected,/*year*/
    rowid_string,/*string*/
    cast_not_expected,/*extend*/
    cast_not_expected,/*unknown*/
    cast_inconsistent_types,/*text*/
    cast_not_expected,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    cast_inconsistent_types,/*otimestamp*/
    cast_inconsistent_types,/*raw*/
    cast_inconsistent_types,/*interval*/
    rowid_rowid,/*rowid*/
    cast_inconsistent_types,/*lob*/
    cast_inconsistent_types_json,/*json*/
    cast_not_support,/*geometry*/
    cast_to_udt_not_support,/*udt*/
  },
  {
    /*lob -> XXX*/
    cast_not_expected,/*null*/
    lob_int,/*int*/
    lob_uint,/*uint*/
    lob_float,/*float*/
    lob_double,/*double*/
    lob_number,/*number*/
    lob_datetime,/*datetime*/
    cast_not_expected,/*date*/
    cast_not_expected,/*time*/
    cast_not_expected,/*year*/
    lob_string,/*string*/
    cast_not_expected,/*extend*/
    cast_not_expected,/*unknown*/
    lob_text,/*text*/
    cast_not_expected,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    lob_otimestamp,/*otimestamp*/
    lob_raw,/*raw*/
    lob_interval,/*interval*/
    lob_rowid,/*rowid*/
    lob_lob,/*lob*/
    lob_json,/*json*/
    cast_not_support,/*geometry*/
    cast_to_udt_not_support,/*udt*/
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
    json_string,/*string*/
    cast_not_support,/*extend*/
    cast_not_support,/*unknown*/
    json_text,/*text*/
    cast_not_support,/*bit*/
    cast_not_support,/*enumset*/
    cast_not_support,/*enumset_inner*/
    cast_not_support,/*otimestamp*/
    json_raw,/*raw*/
    cast_not_support,/*interval*/
    cast_not_support,/*rowid*/
    json_lob,/*lob*/
    json_json,/*json*/
    cast_not_support,/*geometry*/
    cast_to_udt_not_support,/*udt*/
  },
  {
    /*geoemtry -> XXX, not support oracle currently*/
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
    cast_not_support,/*geometry*/
    cast_to_udt_not_support,/*udt*/
  },
  {
    /*udt -> XXX*/
    cast_udt_to_other_not_support,/*null*/
    cast_udt_to_other_not_support,/*int*/
    cast_udt_to_other_not_support,/*uint*/
    cast_udt_to_other_not_support,/*float*/
    cast_udt_to_other_not_support,/*double*/
    cast_udt_to_other_not_support,/*number*/
    cast_udt_to_other_not_support,/*datetime*/
    cast_udt_to_other_not_support,/*date*/
    cast_udt_to_other_not_support,/*time*/
    cast_udt_to_other_not_support,/*year*/
    udt_string,/*string*/
    sql_udt_pl_extend,/*extend*/
    cast_udt_to_other_not_support,/*unknown*/
    cast_udt_to_other_not_support,/*text*/
    cast_udt_to_other_not_support,/*bit*/
    cast_udt_to_other_not_support,/*enumset*/
    cast_udt_to_other_not_support,/*enumset_inner*/
    cast_udt_to_other_not_support,/*otimestamp*/
    cast_udt_to_other_not_support,/*raw*/
    cast_udt_to_other_not_support,/*interval*/
    cast_udt_to_other_not_support,/*rowid*/
    cast_udt_to_other_not_support,/*lob*/
    cast_udt_to_other_not_support,/*json*/
    cast_udt_to_other_not_support,/*geometry*/
    cast_udt_to_other_not_support,/*udt*/
  },
};

////////////////////////////////////////////////////////////////
bool cast_supported(const ObObjType orig_type, const ObCollationType orig_cs_type,
                    const ObObjType expect_type, const ObCollationType expect_cs_type)
{
  bool bret = false;
  bool clob_in = ob_is_clob(orig_type, orig_cs_type) || ob_is_clob_locator(orig_type, orig_cs_type);
  bool blob_out = ob_is_blob(expect_type, expect_cs_type)
                  || ob_is_blob_locator(expect_type, expect_cs_type);
  if (OB_UNLIKELY(ob_is_invalid_obj_type(orig_type) ||
                  ob_is_invalid_obj_type(expect_type))) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid cast type", K(orig_type), K(expect_type));
  // number类型可以转换为clob但是不能转为blob, 无法通过矩阵实现这一点，所以在这里做特殊处理。
  } else if (is_oracle_mode() && (clob_in || ob_is_number_tc(orig_type) || ob_is_int_tc(orig_type)
                                  || ob_is_datetime_tc(orig_type)) && blob_out) {
    bret = false;
  } else if (is_oracle_mode() &&
             (ObTinyIntType == orig_type) &&
             ob_is_text_tc(expect_type)) {
    bret = false;
  } else {
    ObObjTypeClass orig_tc = ob_obj_type_class(orig_type);
    ObObjTypeClass expect_tc = ob_obj_type_class(expect_type);
    if (ObIntervalTC == orig_tc && ObIntervalTC == expect_tc && orig_type != expect_type) {
      bret = false;
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "cast between intervalYM and intervalDS not allowed",
                K(bret), K(orig_type), K(expect_type));
    } else {
      ObObjCastFunc cast_func = lib::is_oracle_mode() ?
                                  OBJ_CAST_ORACLE_IMPLICIT[orig_tc][expect_tc] :
                                  OB_OBJ_CAST[orig_tc][expect_tc];
      bret = (cast_func != cast_not_support && cast_func != cast_inconsistent_types);
    }
  }
  return bret;
}

int float_range_check(ObObjCastParams &params, const ObAccuracy &accuracy,
                      const ObObj &obj, ObObj &buf_obj, const ObObj *&res_obj, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  float value = obj.get_float();
  res_obj = &obj;
  if (lib::is_oracle_mode() && 0.0 == value) {
    value = 0.0;
    buf_obj.set_float(obj.get_type(), value);
    res_obj = &buf_obj;
  } else if (lib::is_oracle_mode() && isnan(value)) {
    // overwrite -NAN to NAN, OB only store NAN
    value = NAN;
    buf_obj.set_float(obj.get_type(), value);
    res_obj = &buf_obj;
  } else {
    // float/double comparison using "==" or "!=" matches MySQL
    // and Oracle doesn't support raw float/double
    if (CAST_FAIL(real_range_check(accuracy, value))) {
    } else if (obj.get_float() != value) {
      buf_obj.set_float(obj.get_type(), value);
      res_obj = &buf_obj;
    }
  }
  return ret;
}

int double_check_precision(ObObjCastParams &params, const ObAccuracy &accuracy,
                           const ObObj &obj, ObObj &buf_obj, const ObObj *&res_obj, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  double value = obj.get_double();
  res_obj = &obj;
  if (lib::is_oracle_mode() && 0.0 == value) {
    value = 0.0;
    buf_obj.set_double(obj.get_type(), value);
    res_obj = &buf_obj;
  } else if (lib::is_oracle_mode() && isnan(value)) {
    // overwrite -NAN to NAN, OB only store NAN
    value = NAN;
    buf_obj.set_double(obj.get_type(), value);
    res_obj = &buf_obj;
  } else if (CAST_FAIL(real_range_check(accuracy, value))) {
    // float/double comparison using "==" or "!=" matches MySQL
    // and Oracle doesn't support raw float/double
  } else if (obj.get_double() != value) {
    buf_obj.set_double(obj.get_type(), value);
    res_obj = &buf_obj;
  }
  return ret;
}

int number_range_check(ObObjCastParams &params, const ObAccuracy &accuracy,
                       const ObObj &obj, ObObj &buf_obj, const ObObj *&res_obj, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  res_obj = NULL;
  static const int64_t BUFFER_SIZE = 2 * (number::ObNumber::MAX_SCALE +
                                          number::ObNumber::MAX_PRECISION);
  if (OB_ISNULL(params.allocator_v2_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid allocator",
        K(ret), K(params.allocator_v2_), K(obj));
  } else {
    int &cast_ret = CM_IS_ERROR_ON_FAIL(cast_mode) ? ret : params.warning_;
    ObPrecision precision = accuracy.get_precision();
    ObScale scale = accuracy.get_scale();
    ObIAllocator &allocator = *params.allocator_v2_;
    res_obj = &obj;
    if (OB_UNLIKELY(precision < scale)) {
      ret = OB_ERR_M_BIGGER_THAN_D;
    } else if (number::ObNumber::MAX_PRECISION >= precision &&
               number::ObNumber::MAX_SCALE >= scale &&
               precision >= 0 &&
               scale >= 0) {
      // prepare string like "99.999".
      char buf[BUFFER_SIZE] = {0};
      int pos = 0;
      buf[pos++] = '-';
      if (precision == scale) {
        buf[pos++] = '0';
      }
      MEMSET(buf + pos, '9', precision + 1);
      buf[pos + precision - scale] = '.';
      // make min and max numbers.
      number::ObNumber min_num;
      number::ObNumber max_num;
      number::ObNumber in_val = obj.get_number();
      number::ObNumber out_val;
      if (OB_FAIL(min_num.from(buf, allocator))) {
      } else if (OB_FAIL(max_num.from(buf + 1, allocator))) {
      } else if (in_val < min_num) {
        cast_ret = OB_DATA_OUT_OF_RANGE;
        buf_obj.set_number(obj.get_type(), min_num);
      } else if (in_val > max_num) {
        cast_ret = OB_DATA_OUT_OF_RANGE;
        buf_obj.set_number(obj.get_type(), max_num);
      } else if (OB_FAIL(out_val.from(in_val, allocator))) {
      } else if (OB_FAIL(out_val.round(scale))) {
      } else {
        buf_obj.set_number(obj.get_type(), out_val);
      }
      res_obj = &buf_obj;
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments",
          K(ret), K(precision), K(scale));
    }
  }
  return ret;
}

int number_range_check_for_oracle(ObObjCastParams &params, const ObAccuracy &accuracy,
                       const ObObj &obj, ObObj &buf_obj, const ObObj *&res_obj, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  res_obj = NULL;
  static const int64_t BUFFER_SIZE = 2 * (number::ObNumber::MAX_SCALE +
                                          number::ObNumber::MAX_PRECISION);
  if (OB_ISNULL(params.allocator_v2_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid allocator",
        K(ret), K(params.allocator_v2_), K(obj));
  } else {
    int &cast_ret = CM_IS_ERROR_ON_FAIL(cast_mode) ? ret : params.warning_;
    ObPrecision precision = accuracy.get_precision();
    ObScale scale = accuracy.get_scale();
    ObIAllocator &allocator = *params.allocator_v2_;
    res_obj = &obj;
    if (number::ObNumber::MAX_PRECISION >= precision &&
               number::ObNumber::MAX_SCALE >= scale &&
               precision >= 1 &&
               scale >= number::ObNumber::MIN_SCALE) {
      // make min and max numbers.
      number::ObNumber min_num;
      number::ObNumber max_num;
      number::ObNumber in_val = obj.get_number();
      bool trunc_max_zero = false;
      bool trunc_min_zero = false;
      // prepare string like "99.999".
      char buf[BUFFER_SIZE] = {0};
      int pos = 0;
      buf[pos++] = '-';
      if (precision >= scale && scale >= 0) {
        /* number(3, 1) => legal range(-99.95, 99.95) */
        if (precision == scale) {
          buf[pos++] = '0';
        }
        MEMSET(buf + pos, '9', precision + 1);
        buf[pos + precision - scale] = '.';
        buf[pos + precision + 1] = '5';

        if (OB_FAIL(min_num.from(buf, allocator))) {
        } else if (OB_FAIL(max_num.from(buf + 1, allocator))) {
        }
      } else {
        if (scale < 0) {
          /* number(2, -3) => legal range: (-99500, 99500) */
          MEMSET(buf + pos, '9', precision);
          buf[pos + precision] = '5';
          MEMSET(buf + pos + precision + 1, '0', 0 - scale - 1);
          if (OB_FAIL(min_num.from(buf, allocator))) {
          } else if (OB_FAIL(max_num.from(buf + 1, allocator))) {
          }
        } else {
          //number(2, 3) => legal range:[0, 0.0995) && (-0.00995, 0]

          buf[pos++] = '0';
          buf[pos++] = '.';
          MEMSET(buf + pos, '0', scale - precision);
          MEMSET(buf + pos + scale - precision, '9', precision);
          buf[pos + scale] = '5';
          if (in_val.is_negative()) {
            trunc_max_zero = true;
            if (OB_FAIL(min_num.from(buf, allocator))) {
              LOG_DEBUG("fail to get num from ", K(buf));
            } else {
              MEMSET(buf + pos + scale - precision, '0', precision);
              if (OB_FAIL(max_num.from(buf, allocator))) {
                LOG_DEBUG("fail to get num from ", K(buf));
              }
            }
          } else {
            trunc_min_zero = true;
            if (OB_FAIL(max_num.from(buf + 1, allocator))) {
              LOG_DEBUG("fail to get num from ", K(buf + 1));
            } else {
              MEMSET(buf + pos + scale - precision, '0', precision);
              if (OB_FAIL(min_num.from(buf + 1, allocator))) {
                LOG_DEBUG("fail to get num from ", K(buf + 1));
              }
            }
          }
        }
      }
      LOG_DEBUG("BUF", KP(buf), K(precision), K(scale), K(min_num), K(max_num), K(trunc_min_zero), K(trunc_max_zero));

      number::ObNumber out_val;
      if (OB_SUCC(ret)) {
        if (in_val <= min_num) {
          if (trunc_min_zero) {
            min_num.set_zero();
            LOG_DEBUG("set min zero");
          } else {
            cast_ret = OB_DATA_OUT_OF_RANGE;
          }
          buf_obj.set_number(obj.get_type(), min_num);
        } else if (in_val >= max_num) {
          if (trunc_max_zero) {
            max_num.set_zero();
          } else {
            cast_ret = OB_DATA_OUT_OF_RANGE;
          }
          buf_obj.set_number(obj.get_type(), max_num);
        } else if (OB_FAIL(out_val.from(in_val, allocator))) {
        } else if (OB_FAIL(out_val.round(scale))) {
        } else {
          buf_obj.set_number(obj.get_type(), out_val);
        }
      }
      res_obj = &buf_obj;
    } else if (ORA_NUMBER_SCALE_UNKNOWN_YET == scale &&
        PRECISION_UNKNOWN_YET == precision) {
      buf_obj.set_number(obj.get_type(), obj.get_number());
      res_obj = &buf_obj;
      LOG_DEBUG("unknown scale and precision", K(obj.get_scale()), K(obj), K(buf_obj));
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments",
          K(ret), K(precision), K(scale));
    }
  }
  return ret;
}

int number_range_check_v2(ObObjCastParams &params, const ObAccuracy &accuracy,
                          const ObObj &obj, ObObj &buf_obj, const ObObj *&res_obj,
                          const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  res_obj = NULL;
  if (OB_ISNULL(params.allocator_v2_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid allocator", K(ret), K(params.allocator_v2_), K(obj));
  } else {
    int &cast_ret = CM_IS_ERROR_ON_FAIL(cast_mode) ? ret : params.warning_;
    ObPrecision precision = accuracy.get_precision();
    ObScale scale = accuracy.get_scale();
    ObIAllocator &allocator = *params.allocator_v2_;
    res_obj = &obj;
    const number::ObNumber &in_val = obj.get_number();
    const number::ObNumber *min_check_num = NULL;
    const number::ObNumber *max_check_num = NULL;
    const number::ObNumber *min_num_mysql = NULL;
    const number::ObNumber *max_num_mysql = NULL;
    number::ObNumber out_val;
    bool is_finish = false;
    if (obj.is_number_float()) {
      if (OB_MIN_NUMBER_FLOAT_PRECISION <= precision
          && precision <= OB_MAX_NUMBER_FLOAT_PRECISION) {
        const int64_t number_precision = static_cast<int64_t>(floor(precision * OB_PRECISION_BINARY_TO_DECIMAL_FACTOR));
        if (OB_FAIL(out_val.from(in_val, allocator))) {
        } else if (OB_FAIL(out_val.round_precision(number_precision))) {
        } else if (CM_IS_ERROR_ON_SCALE_OVER(cast_mode) &&
          in_val.compare(out_val) != 0) {
          ret = OB_OPERATE_OVERFLOW;
          LOG_WARN("input value is out of range.", K(scale), K(in_val));
        } else {
          buf_obj.set_number(obj.get_type(), out_val);
          res_obj = &buf_obj;
          is_finish = true;
        }
        LOG_DEBUG("finish round_precision", K(in_val), K(number_precision), K(precision), K(buf_obj));
      } else if (PRECISION_UNKNOWN_YET == precision) {
        buf_obj.set_number(obj.get_type(), in_val);
        res_obj = &buf_obj;
        is_finish = true;
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid arguments", K(ret), K(precision), K(scale));
      }
    } else if (lib::is_oracle_mode()) {
      if (OB_MAX_NUMBER_PRECISION >= precision
          && precision >= OB_MIN_NUMBER_PRECISION
          && number::ObNumber::MAX_SCALE >= scale
          && scale >= number::ObNumber::MIN_SCALE) {
        min_check_num = &(ObNumberConstValue::ORACLE_CHECK_MIN[precision][scale + ObNumberConstValue::MAX_ORACLE_SCALE_DELTA]);
        max_check_num = &(ObNumberConstValue::ORACLE_CHECK_MAX[precision][scale + ObNumberConstValue::MAX_ORACLE_SCALE_DELTA]);
      } else if (ORA_NUMBER_SCALE_UNKNOWN_YET == scale
                 && PRECISION_UNKNOWN_YET == precision) {
        buf_obj.set_number(obj.get_type(), in_val);
        res_obj = &buf_obj;
        is_finish = true;
      } else if (PRECISION_UNKNOWN_YET == precision
                && number::ObNumber::MAX_SCALE >= scale
                && scale >= number::ObNumber::MIN_SCALE) {
        number::ObNumber num;
        if (OB_FAIL(num.from(obj.get_number(), allocator))) {
        } else if (OB_FAIL(num.round(scale))) {
        } else if (CM_IS_ERROR_ON_SCALE_OVER(cast_mode) &&
          obj.get_number().compare(num) != 0) {
          ret = OB_OPERATE_OVERFLOW;
          LOG_WARN("input value is out of range.", K(scale), K(in_val));
        } else {
          buf_obj.set_number(obj.get_type(), num);
          res_obj = &buf_obj;
          is_finish = true;
        }
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid arguments", K(ret), K(precision), K(scale));
      }
    } else {
      if (OB_UNLIKELY(precision < scale)) {
        ret = OB_ERR_M_BIGGER_THAN_D;
      } else if (number::ObNumber::MAX_PRECISION >= precision
                && precision >= OB_MIN_DECIMAL_PRECISION
                && number::ObNumber::MAX_SCALE >= scale
                && scale >= 0) {
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
      if (OB_ISNULL(min_check_num) || OB_ISNULL(max_check_num)
          || (!lib::is_oracle_mode()
              && (OB_ISNULL(min_num_mysql) || OB_ISNULL(max_num_mysql)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("min_num or max_num is null", K(ret), KPC(min_check_num), KPC(max_check_num));
      } else if (in_val <= *min_check_num) {
        if (lib::is_oracle_mode()) {
          cast_ret = OB_ERR_VALUE_LARGER_THAN_ALLOWED;
        } else {
          cast_ret = OB_DATA_OUT_OF_RANGE;
          buf_obj.set_number(obj.get_type(), *min_num_mysql);
        }
      } else if (in_val >= *max_check_num) {
        if (lib::is_oracle_mode()) {
          cast_ret = OB_ERR_VALUE_LARGER_THAN_ALLOWED;
        } else {
          cast_ret = OB_DATA_OUT_OF_RANGE;
          buf_obj.set_number(obj.get_type(), *max_num_mysql);
        }
        //need round
      } else {
        if (OB_FAIL(out_val.from(in_val, allocator))) {
        } else if (OB_FAIL(out_val.round(scale))) {
        } else if (CM_IS_ERROR_ON_SCALE_OVER(cast_mode) &&
          in_val.compare(out_val) != 0) {
          ret = OB_OPERATE_OVERFLOW;
          LOG_WARN("input value is out of range.", K(scale), K(in_val));
        } else {
          buf_obj.set_number(obj.get_type(), out_val);
        }
      }
    }
    LOG_DEBUG("succ to number_range_check_v2", K(ret), K(cast_ret),  K(is_finish),
              K(precision), K(scale), KPC(min_check_num), KPC(max_check_num),
              KPC(min_num_mysql), KPC(max_num_mysql), K(in_val), K(obj), K(buf_obj));
    res_obj = &buf_obj;
  }
  return ret;
}

int number_range_check_only(const ObAccuracy &accuracy, const ObObj &obj)
{
  int ret = OB_SUCCESS;
  UNUSED(obj);
  ObPrecision precision = accuracy.get_precision();
  ObScale scale = accuracy.get_scale();
  if (OB_UNLIKELY(precision < scale)) {
    ret = OB_ERR_M_BIGGER_THAN_D;
  } else if (number::ObNumber::MAX_PRECISION >= precision &&
             number::ObNumber::MAX_SCALE >= scale &&
             precision >= 0 &&
             scale >= 0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("number range check not supported", K(ret));
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments",
             K(ret), K(precision), K(scale));
  }
  return ret;
}

// check usec scale for ObTimeType, ObDateTimeType
int time_usec_scale_check(const ObCastMode &cast_mode,
                          const ObAccuracy &accuracy,
                          const int64_t value)
{
  INIT_SUCC(ret);
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

int datetime_scale_check(ObObjCastParams &params, const ObAccuracy &accuracy,
                         const ObObj &obj, ObObj &buf_obj, const ObObj *&res_obj, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  res_obj = NULL;
  ObScale scale = accuracy.get_scale();
  if (OB_UNLIKELY(scale > MAX_SCALE_FOR_TEMPORAL)) {
    ret = OB_ERR_TOO_BIG_PRECISION;
    LOG_USER_ERROR(OB_ERR_TOO_BIG_PRECISION, scale, "CAST", static_cast<int64_t>(MAX_SCALE_FOR_TEMPORAL));
  } else {
    int64_t value = obj.get_datetime();
    if (OB_FAIL(time_usec_scale_check(cast_mode, accuracy, value))) {
      LOG_WARN("check zero scale fail.", K(ret), K(value), K(scale));
    } else if (OB_UNLIKELY(0 <= scale && scale < MAX_SCALE_FOR_TEMPORAL)) {
      ObTimeConverter::round_datetime(scale, value);
      if (ObTimeConverter::is_valid_datetime(value)) {
        buf_obj.set_datetime(obj.get_type(), value);
      } else {
        buf_obj.set_null();
      }
      res_obj = &buf_obj;
    } else {
      res_obj = &obj;
    }
  }
  UNUSED(params);
  return ret;
}


int otimestamp_scale_check(ObObjCastParams &params, const ObAccuracy &accuracy, const ObObj &obj,
                             ObObj &buf_obj, const ObObj *&res_obj, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  res_obj = NULL;
  ObScale scale = accuracy.get_scale();
  if (OB_UNLIKELY(scale > MAX_SCALE_FOR_ORACLE_TEMPORAL)) {
    ret = OB_ERR_TOO_BIG_PRECISION;
    LOG_USER_ERROR(OB_ERR_TOO_BIG_PRECISION, scale, "CAST", static_cast<int64_t>(MAX_SCALE_FOR_ORACLE_TEMPORAL));
  } else if (OB_UNLIKELY(0 <= scale && scale < MAX_SCALE_FOR_ORACLE_TEMPORAL)) {
    ObOTimestampData ot_data = ObTimeConverter::round_otimestamp(scale, obj.get_otimestamp_value());
    if (ObTimeConverter::is_valid_otimestamp(ot_data.time_us_, static_cast<int32_t>(ot_data.time_ctx_.tail_nsec_))) {
      buf_obj.set_otimestamp_value(obj.get_type(), ot_data);
      buf_obj.set_scale(scale);
    } else {
      OB_LOG(DEBUG, "invalid otimestamp, set it null ", K(ot_data), K(scale), "orig_date", obj.get_otimestamp_value());
      buf_obj.set_null();
    }
    res_obj = &buf_obj;
  } else {
    res_obj = &obj;
  }
  UNUSED(params);
  return ret;
}

int interval_scale_check(ObObjCastParams &params, const ObAccuracy &accuracy, const ObObj &obj,
                         ObObj &buf_obj, const ObObj *&res_obj, const ObCastMode cast_mode)
{
  UNUSED(cast_mode);
  UNUSED(params);
  int ret = OB_SUCCESS;
  res_obj = NULL;
  ObScale expected_scale = accuracy.get_scale();
  buf_obj = obj;

  if (obj.is_interval_ym()) {
    int8_t expected_year_scale = ObIntervalScaleUtil::ob_scale_to_interval_ym_year_scale(static_cast<int8_t>(expected_scale));
    int8_t input_year_scale = obj.get_interval_ym().calc_leading_scale();
    if (OB_UNLIKELY(expected_year_scale < input_year_scale)) {
      ret = OB_ERR_THE_LEADING_PRECISION_OF_THE_INTERVAL_IS_TOO_SMALL;
      LOG_WARN("interval obj scale check", K(ret), K(expected_year_scale), K(input_year_scale));
    }
  } else if (obj.is_interval_ds()) {
    ObIntervalDSValue value = obj.get_interval_ds();
    int8_t expected_day_scale = ObIntervalScaleUtil::ob_scale_to_interval_ds_day_scale(static_cast<int8_t>(expected_scale));
    int8_t expected_fs_scale = ObIntervalScaleUtil::ob_scale_to_interval_ds_second_scale(static_cast<int8_t>(expected_scale));

    if (OB_FAIL(ObTimeConverter::round_interval_ds(expected_fs_scale, value))) {
      LOG_WARN("fail to round interval ds", K(ret), K(value));
    } else {
      int8_t input_day_scale = value.calc_leading_scale();
      if (OB_UNLIKELY(expected_day_scale < input_day_scale)) {
        ret = OB_ERR_THE_LEADING_PRECISION_OF_THE_INTERVAL_IS_TOO_SMALL;
        LOG_WARN("interval obj scale check", K(ret), K(expected_day_scale), K(input_day_scale));
      } else {
        buf_obj.set_interval_ds(value);
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
  }
  if (OB_SUCC(ret)) {
    buf_obj.set_scale(expected_scale);
    res_obj = &buf_obj;
  }
  return ret;
}

int datetime_scale_check_only(const ObAccuracy &accuracy, const ObObj &obj)
{
  int ret = OB_SUCCESS;
  ObScale scale = accuracy.get_scale();
  if (OB_UNLIKELY(scale > MAX_SCALE_FOR_TEMPORAL)) {
    ret = OB_ERR_TOO_BIG_PRECISION;
    LOG_USER_ERROR(OB_ERR_TOO_BIG_PRECISION, scale, "CAST", static_cast<int64_t>(MAX_SCALE_FOR_TEMPORAL));
  } else if (OB_UNLIKELY(0 <= scale && scale < MAX_SCALE_FOR_TEMPORAL)) {
    int64_t value = obj.get_datetime();
    if (!ObTimeConverter::is_valid_datetime(value)) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid datetime value", K(ret), K(value));
    }
  }
  return ret;
}

int time_scale_check(ObObjCastParams &params, const ObAccuracy &accuracy,
                     const ObObj &obj, ObObj &buf_obj, const ObObj *&res_obj, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  res_obj = NULL;
  ObScale scale = accuracy.get_scale();
  int64_t value = obj.get_time();
  if (OB_FAIL(time_usec_scale_check(cast_mode, accuracy, value))) {
    LOG_WARN("check usec scale fail.", K(ret), K(value));
  } else if (OB_LIKELY(0 <= scale && scale < MAX_SCALE_FOR_TEMPORAL)) {
    ObTimeConverter::round_datetime(scale, value);
    buf_obj.set_time(value);
    res_obj = &buf_obj;
  } else {
    res_obj = &obj;
  }
  UNUSED(params);
  UNUSED(cast_mode);
  return ret;
}

int time_scale_check_only(const ObAccuracy &accuracy, const ObObj &obj)
{
  int ret = OB_SUCCESS;
  UNUSED(accuracy);
  UNUSED(obj);
  return ret;
}

//empty string时,bit_len 为0
//非empty string的情况，为char 对应二进制值去掉前导0后的长度, 例如'3b'的长度为15
int get_bit_len(const ObString &str, int32_t &bit_len)
{
  int ret = OB_SUCCESS;
  if (str.empty()) {
    bit_len = 1;
  } else {
    const char *ptr = str.ptr();
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
      //Built-in Function: int __builtin_clz (unsigned int x).
      //Returns the number of leading 0-bits in x, starting at the most significant bit position.
      //If x is 0, the result is undefined.
      int32_t uneven_len = static_cast<int32_t>(
          sizeof(unsigned int) * 8 - __builtin_clz(uneven_value));
      bit_len = uneven_len + 8 * (len - 1);
    }
  }
  return ret;
}

int get_bit_len(uint64_t value, int32_t &bit_len)
{
  int ret = OB_SUCCESS;
  if (0 == value) {
    bit_len = 1;
  } else {
    bit_len = static_cast<int32_t>(sizeof(unsigned long long) * 8 - __builtin_clzll(value));
  }
  return ret;
}

int string_length_check(ObObjCastParams &params, const ObAccuracy &accuracy,
    const ObCollationType cs_type, const ObObj &obj, ObObj &buf_obj, const ObObj *&res_obj,
    const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  const ObLength max_accuracy_len = accuracy.get_length();
  const int32_t str_len_byte = obj.get_string_len();
  bool is_oracle = is_oracle_mode();
  // 处理异常情况，但str_len_byte大于max_len_char不一定有问题，还需要具体判断
  if (max_accuracy_len <= 0 || str_len_byte > max_accuracy_len) {
    int &cast_ret = (CM_IS_ERROR_ON_FAIL(cast_mode) && !is_oracle)
                    ? ret
                    : params.warning_;
    const char *str = obj.get_string_ptr();
    int32_t str_len_char = -1;
    if (max_accuracy_len == DEFAULT_STR_LENGTH) {
      res_obj = &obj;
    } else if (OB_UNLIKELY(max_accuracy_len <= 0)) {
      buf_obj.set_string(obj.get_type(), NULL, 0);
      buf_obj.set_collation_level(obj.get_collation_level());
      buf_obj.set_collation_type(obj.get_collation_type());
      res_obj = &buf_obj;
      if (OB_UNLIKELY(0 == max_accuracy_len && str_len_byte > 0)) {
        cast_ret = OB_ERR_DATA_TOO_LONG;
        str_len_char = obj.is_lob() ? str_len_byte : static_cast<int32_t>(ObCharset::strlen_char(cs_type, str, str_len_byte));
        OB_LOG(WARN, "char type length is too long", K(obj), K(max_accuracy_len), K(str_len_char));
      }
    } else {
      int32_t trunc_len_byte = -1;
      int32_t trunc_len_char = -1;
      if (obj.is_varbinary() || obj.is_binary() || obj.is_blob()) {
        str_len_char = obj.is_lob() ? str_len_byte : static_cast<int32_t>(ObCharset::strlen_char(cs_type, str, str_len_byte));
        if (OB_UNLIKELY(str_len_char > max_accuracy_len)) {
          cast_ret = OB_ERR_DATA_TOO_LONG;
          LOG_WARN("binary type length is too long", K(max_accuracy_len), K(str_len_char), K(obj));
        }
      } else if (is_oracle_byte_length(is_oracle, accuracy.get_length_semantics())) {
        const ObLength max_len_byte = accuracy.get_length();
        if (OB_UNLIKELY(str_len_byte > max_len_byte)) {
          cast_ret = OB_ERR_DATA_TOO_LONG;
          LOG_WARN("char type length is too long", K(str_len_byte), K(max_len_byte), K(obj));
        }
      } else if (is_oracle && obj.is_fixed_len_char_type()) {
        const int32_t str_len_char = static_cast<int32_t>(ObCharset::strlen_char(cs_type, str, str_len_byte));
        if (OB_UNLIKELY(str_len_byte > OB_MAX_ORACLE_CHAR_LENGTH_BYTE)) {
          cast_ret = OB_ERR_DATA_TOO_LONG;
          LOG_WARN("char byte length is too long", K(str_len_byte), K(OB_MAX_ORACLE_CHAR_LENGTH_BYTE), K(obj));
        } else if (OB_UNLIKELY(str_len_char > max_accuracy_len)) {
          cast_ret = OB_ERR_DATA_TOO_LONG;
          LOG_WARN("char char length is too long", K(str_len_char), K(max_accuracy_len), K(obj));
        }
      } else {//mysql, oracle varchar(char)
        // trunc_len_char > max_accuracy_len means an error or warning, without tail ' ', otherwise
        // str_len_char > max_accuracy_len means only warning, even in strict mode.
        // lengthsp()  - returns the length of the given string without trailing spaces. 所以strlen_byte_no_sp返回的结果是小于等于str的长度
        trunc_len_byte = static_cast<int32_t>(ObCharset::strlen_byte_no_sp(cs_type, str, str_len_byte));
        trunc_len_char = obj.is_lob() ? trunc_len_byte : static_cast<int32_t>(ObCharset::strlen_char(cs_type, str, trunc_len_byte));

        if (is_oracle && OB_UNLIKELY(str_len_byte > OB_MAX_ORACLE_VARCHAR_LENGTH)) {
          cast_ret = OB_ERR_DATA_TOO_LONG;
          LOG_WARN("varchar2 byte length is too long", K(str_len_byte), K(OB_MAX_ORACLE_VARCHAR_LENGTH), K(obj));
        } else if (OB_UNLIKELY(trunc_len_char > max_accuracy_len)) {
          cast_ret = OB_ERR_DATA_TOO_LONG;
          LOG_WARN("char type length is too long", K(max_accuracy_len), K(trunc_len_char), K(obj), K(trunc_len_byte), K(obj.is_lob()));
        } else {
          str_len_char = obj.is_lob() ? str_len_byte : static_cast<int32_t>(ObCharset::strlen_char(cs_type, str, str_len_byte));
          if (OB_UNLIKELY(str_len_char > max_accuracy_len)) {
            params.warning_ = OB_ERR_DATA_TOO_LONG;
            LOG_WARN("char type length is too long", K(max_accuracy_len), K(str_len_char), K(obj));
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_UNLIKELY(OB_ERR_DATA_TOO_LONG == params.warning_)) {
          // when warning, always trunc to max_accuracy_len first.
          // besides, if char (not binary), trunc to trunc_len_char again, trim tail ' ' after first trunc.
          // the reason of two-trunc for char (not binary):
          // insert 'ab  ! ' to char(3), we get an 'ab' in column, not 'ab ':
          // first trunc: 'ab  ! ' to 'ab ',
          // second trunc: 'ab ' to 'ab'.
          if (obj.is_text() || obj.is_json()) {
            int64_t char_len = 0;
            trunc_len_byte = static_cast<int32_t>(ObCharset::max_bytes_charpos(cs_type, str, str_len_byte, max_accuracy_len, char_len));
          } else {
            trunc_len_byte = static_cast<int32_t>(ObCharset::charpos(cs_type, str, str_len_byte, max_accuracy_len));
          }
          if (is_oracle) {
          // 在oracle模式下不清理末尾的空格字符,原因如下:
          // #bug18529663:例如select cast(' a' as char) from dual;
          // 执行至此时,trunc_len_byte = 1,意思是截取到' a'的第一个字符' '
          // 如果不加判断将会直接执行strlen_byte_no_sp来清理末尾的空格字符,执行完毕后由于空格被清理掉,导致trunc_len_byte=0
          // trunc_len_byte = 0会导致最终的obchar类型的输出长度为0,在oracle模式的比较中将会判其为空,不符号预期.
          } else if (obj.is_fixed_len_char_type() && !obj.is_binary()) {
            trunc_len_byte = static_cast<int32_t>(ObCharset::strlen_byte_no_sp(cs_type, str, trunc_len_byte));
          }
          if (OB_FAIL(copy_string(params, obj.get_type(), str, trunc_len_byte, buf_obj))) {
          } else {
            buf_obj.set_collation_level(obj.get_collation_level());
            buf_obj.set_collation_type(obj.get_collation_type());
            res_obj = &buf_obj;
          }
          if (is_oracle) {
            ret = params.warning_;
          }
        } else if (OB_SUCC(params.warning_) && is_oracle) {
          ret = params.warning_;
        } else {
          res_obj = &obj;
        }
      }
    }
  } else {
    // 正常分支，直接赋值即可
    res_obj = &obj;
  }

  return ret;
}

// no truncate
int string_length_check_only(const ObAccuracy &accuracy, const ObCollationType cs_type, const ObObj &obj)
{
  int ret = OB_SUCCESS;
  const ObLength max_len_char = accuracy.get_length();
  const char *str = obj.get_string_ptr();
  const int32_t str_len_byte = obj.get_string_len();
  const int32_t str_len_char = static_cast<int32_t>(ObCharset::strlen_char(cs_type, str, str_len_byte));
  if (OB_UNLIKELY(max_len_char <= 0)) {
    if (OB_UNLIKELY(0 == max_len_char && str_len_byte > 0)) {
      ret = OB_ERR_DATA_TOO_LONG;
      OB_LOG(WARN, "char type length is too long", K(obj), K(max_len_char), K(str_len_char));
    }
  } else if (OB_UNLIKELY(str_len_char > max_len_char)) {
    ret = OB_ERR_DATA_TOO_LONG;
    LOG_WARN("string length is too long", K(max_len_char), K(str_len_char), K(obj));
  }
  return ret;
}

int raw_length_check(ObObjCastParams &params, const ObAccuracy &accuracy,
    const ObCollationType cs_type, const ObObj &obj, ObObj &buf_obj, const ObObj *&res_obj,
    const ObCastMode cast_mode)
{
  UNUSED(params);
  UNUSED(cs_type);
  UNUSED(buf_obj);
  UNUSED(cast_mode);
  int ret = OB_SUCCESS;
  const ObLength max_accuracy_len = accuracy.get_length();
  const int32_t str_len_byte = obj.get_string_len();

  if (OB_UNLIKELY(max_accuracy_len >= 0 && str_len_byte > max_accuracy_len)) {
    ret = OB_OPERATE_OVERFLOW;
    LOG_WARN("char type length is too long", K(ret), K(str_len_byte), K(max_accuracy_len), K(obj));
  } else {
    res_obj = &obj;
  }

  return ret;
}

int bit_length_check(ObObjCastParams &params, const ObAccuracy &accuracy,
                           const ObObj &obj, ObObj &buf_obj, const ObObj *&res_obj, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  res_obj = NULL;
  uint64_t value = obj.get_bit();
  int32_t bit_len = 0;
  int32_t dst_bit_len = accuracy.get_precision();
  if (OB_FAIL(get_bit_len(value, bit_len))) {
    LOG_WARN("fail to get_bit_length", K(ret), K(value), K(bit_len));
  } else if(OB_UNLIKELY(bit_len <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("bit length is negative", K(ret), K(value), K(bit_len), K(obj));
  } else {
    if (OB_UNLIKELY(bit_len > dst_bit_len)) {
      ret = OB_ERR_DATA_TOO_LONG;
      LOG_WARN("bit type length is too long", K(ret), K(bit_len), K(dst_bit_len), K(value));
    } else {
      buf_obj.set_bit(value);
    }
    if (OB_FAIL(ret) && CM_IS_WARN_ON_FAIL(cast_mode)) {
      params.warning_ = OB_DATA_OUT_OF_RANGE;
      ret = OB_SUCCESS;
      uint64_t max_value = (1ULL<<dst_bit_len) - 1;
      buf_obj.set_bit(max_value);
    }
    if (OB_SUCC(ret)) {
      res_obj = &buf_obj;
    }
  }
  return ret;
}

int bit_length_check_only(const ObAccuracy &accuracy, const ObObj &obj)
{
  int ret = OB_SUCCESS;
  uint64_t value = obj.get_bit();
  int32_t bit_len = 0;
  int32_t dst_bit_len = accuracy.get_precision();
  if (OB_FAIL(get_bit_len(value, bit_len))) {
    LOG_WARN("fail to get_bit_length", K(ret), K(value), K(bit_len));
  } else if(OB_UNLIKELY(bit_len <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("bit length is negative", K(ret), K(value), K(bit_len), K(obj));
  } else {
    if (OB_UNLIKELY(bit_len > dst_bit_len)) {
      ret = OB_ERR_DATA_TOO_LONG;
      LOG_WARN("bit type length is too long", K(ret), K(bit_len), K(dst_bit_len), K(value));
    }
  }
  return ret;
}

int obj_collation_check(const bool is_strict_mode, const ObCollationType cs_type, ObObj &obj)
{
  int ret = OB_SUCCESS;
  if (!ob_is_string_type(obj.get_type()) && ! ob_is_lob_locator(obj.get_type())) {
    //nothing to do
  } else if (cs_type == CS_TYPE_BINARY) {
    //任何类型都可以直接转成binary
    obj.set_collation_type(cs_type);
  } else if (!lib::is_mysql_mode()) {
    obj.set_collation_type(cs_type);
  } else {
    ObString str;
    int64_t well_formed_len = 0;
    common::ObArenaAllocator allocator(ObModIds::OB_LOB_READER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());

    if (ob_is_lob_locator(obj.get_type())) {
      if (OB_FAIL(obj.get_string(str))) {
        LOG_WARN("Failed to get payload from lob locator", K(ret), K(obj));
      }
    } else {
      if (!obj.has_lob_header()) {
        if (OB_FAIL(obj.get_string(str))) {
          LOG_WARN("Failed to get payload from string", K(ret), K(obj));
        }
      } else {
        ObTextStringIter instr_iter(obj);
        if (OB_FAIL(instr_iter.init(0, NULL, &allocator))) {
          LOG_WARN("fail to init lob str iter", K(ret));
        } else if (OB_FAIL(instr_iter.get_full_data(str))) {
          LOG_WARN("fail to get full data", K(ret));
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObCharset::well_formed_len(cs_type,
                                           str.ptr(),
                                           str.length(),
                                           well_formed_len))) {
      LOG_WARN("invalid string for charset",
                K(ret), K(cs_type), K(str), K(well_formed_len), KPHEX(str.ptr(), str.length()));
      if (is_strict_mode) {
        ret = OB_ERR_INCORRECT_STRING_VALUE;
        // LOG_USER_ERROR(ret, str.length(), str.ptr());
      } else {
        if (!obj.has_lob_header()) {
          ret = OB_SUCCESS;
          obj.set_collation_type(cs_type);
          str.assign_ptr(str.ptr(), static_cast<ObString::obstr_size_t>(well_formed_len));
          if (ob_is_lob_locator(obj.get_type())) {
            if (OB_ISNULL(obj.get_lob_locator())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("Unepected null lob locator", K(ret), K(obj));
            } else  {
              (const_cast<ObLobLocator*>(obj.get_lob_locator()))->payload_size_ = str.length();
            }
          } else {
            obj.set_string(obj.get_type(), str.ptr(), str.length()); // must not have lob header
          }
        }
        LOG_WARN("invalid string for charset",
                  K(ret), K(cs_type), K(str), K(well_formed_len), K(str.length()));
      }
    } else {
      obj.set_collation_type(cs_type);
    }
  }
  return ret;
}

int urwoid_length_check_only(const ObAccuracy &accuracy, const ObObj &obj)
{
  int ret = OB_SUCCESS;
  ObURowIDData urowid_data = obj.get_urowid();

  if (OB_UNLIKELY(urowid_data.rowid_len_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid urowid data length", K(ret), K(urowid_data.rowid_len_));
  } else if (OB_UNLIKELY(urowid_data.rowid_len_ > accuracy.get_precision())) {
    ret = OB_ERR_DATA_TOO_LONG;
    LOG_WARN("urowid data length us too long",
             K(ret), K(accuracy.get_precision()), K(urowid_data.rowid_len_));
  }
  return ret;
}

int obj_accuracy_check(ObCastCtx &cast_ctx, const ObAccuracy &accuracy, const ObCollationType cs_type,
                       const ObObj &obj, ObObj &buf_obj, const ObObj *&res_obj)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("obj_accuracy_check before", K(obj), K(accuracy), K(cs_type));
  switch (obj.get_type_class()) {
    case ObFloatTC: {
      ret = float_range_check(cast_ctx, accuracy, obj, buf_obj, res_obj, cast_ctx.cast_mode_);
      break;
    }
    case ObDoubleTC: {
      ret = double_check_precision(cast_ctx, accuracy, obj, buf_obj, res_obj, cast_ctx.cast_mode_);
      break;
    }
    case ObNumberTC: {
      ret = number_range_check_v2(cast_ctx, accuracy, obj, buf_obj, res_obj, cast_ctx.cast_mode_);
      break;
    }
    case ObDateTimeTC: {
      ret = datetime_scale_check(cast_ctx, accuracy, obj, buf_obj, res_obj, cast_ctx.cast_mode_);
      break;
    }
    case ObOTimestampTC: {
      ret = otimestamp_scale_check(cast_ctx, accuracy, obj, buf_obj, res_obj, cast_ctx.cast_mode_);
      break;
    }
    case ObTimeTC: {
      ret = time_scale_check(cast_ctx, accuracy, obj, buf_obj, res_obj, cast_ctx.cast_mode_);
      break;
    }
    case ObStringTC: {
      ret = string_length_check(cast_ctx, accuracy, cs_type, obj, buf_obj, res_obj, cast_ctx.cast_mode_);
      break;
    }
    case ObRawTC: {
      ret = raw_length_check(cast_ctx, accuracy, cs_type, obj, buf_obj, res_obj, cast_ctx.cast_mode_);
      break;
    }
    case ObTextTC: {
      // TODO@hanhui texttc share with stringtc temporarily
      ret = string_length_check(cast_ctx, accuracy, cs_type, obj, buf_obj, res_obj, cast_ctx.cast_mode_);
      break;
    }
    case ObBitTC: {
      ret = bit_length_check(cast_ctx, accuracy, obj, buf_obj, res_obj, cast_ctx.cast_mode_);
      break;
    }
    case ObIntervalTC: {
      ret = interval_scale_check(cast_ctx, accuracy, obj, buf_obj, res_obj, cast_ctx.cast_mode_);
      break;
    }
    case ObRowIDTC: {
      ret = string_length_check(cast_ctx, accuracy, cs_type,
                                obj, buf_obj, res_obj, cast_ctx.cast_mode_);
      break;
    }
    case ObJsonTC: {
      // ToDo: json length check
      break;
    }
    case ObLobTC: {
      // TODO:shanting lob length check
    }
    default: {
      //LOG_WARN("unexpected type class to check", K(obj));
      break;
    }
  }
  return ret;
}

int ob_obj_accuracy_check_only(const ObAccuracy &accuracy, const ObCollationType cs_type, const ObObj &obj)
{
  int ret = OB_SUCCESS;
  switch(obj.get_type_class()) {
    case ObFloatTC:
      {
        switch(obj.get_type()) {
          case ObFloatType:
            ret = real_range_check_only(accuracy, obj.get_float());
            break;
          case ObUFloatType:
            {
              double value = obj.get_ufloat();
              ret = numeric_negative_check(value);
              if (OB_SUCC(ret)) {
                ret = real_range_check_only(accuracy, obj.get_ufloat());
              }
              break;
            }
          default:
            break;
        }
        break;
      }
    case ObDoubleTC:
      {
        switch(obj.get_type()) {
          case ObDoubleType:
            ret = real_range_check_only(accuracy, obj.get_double());
            break;
          case ObUDoubleType:
            {
              double value = obj.get_udouble();
              ret = numeric_negative_check(value);
              if (OB_SUCC(ret)) {
                ret = real_range_check_only(accuracy, obj.get_udouble());
              }
              break;
            }
          default:
            break;
        }
        break;
      }
    case ObNumberTC:
      ret = number_range_check_only(accuracy, obj);
      break;
    case ObDateTimeTC:
      ret = datetime_scale_check_only(accuracy, obj);
      break;
    case ObTimeTC:
      ret = time_scale_check_only(accuracy, obj);
      break;
    case ObStringTC:
      ret = string_length_check_only(accuracy, cs_type, obj);
      break;
    case ObTextTC:
      ret = string_length_check_only(accuracy, cs_type, obj);
      break;
    case ObBitTC:
      ret = bit_length_check_only(accuracy, obj);
      break;
    case ObIntTC:
      {
        int64_t value = obj.get_int();
        ret = int_range_check(obj.get_type(), value, value);
      }
      break;
    case ObUIntTC:
      {
        uint64_t value = obj.get_uint64();
        ret = uint_upper_check(obj.get_type(), value);
      }
      break;
    default:
      break;
  }
  return ret;
}

int ob_obj_to_ob_time_with_date(const ObObj& obj,
                                const ObTimeZoneInfo* tz_info,
                                ObTime& ob_time,
                                const int64_t cur_ts_value,
                                bool is_dayofmonth /* = false */,
                                const ObDateSqlMode date_sql_mode /* = 0 */)
{
  int ret = OB_SUCCESS;
  switch (obj.get_type_class()) {
    case ObIntTC:
      // fallthrough.
    case ObUIntTC: {
      ret = ObTimeConverter::int_to_ob_time_with_date(obj.get_int(), ob_time, is_dayofmonth,
                                                      date_sql_mode);
      break;
    }
    case ObOTimestampTC: {
      ret = ObTimeConverter::otimestamp_to_ob_time(obj.get_type(), obj.get_otimestamp_value(), tz_info, ob_time);
      break;
    }
    case ObDateTimeTC: {
      ret = ObTimeConverter::datetime_to_ob_time(obj.get_datetime(),
          (ObTimestampType == obj.get_type()) ? tz_info : NULL, ob_time);
      break;
    }
    case ObDateTC: {
      ret = ObTimeConverter::date_to_ob_time(obj.get_date(), ob_time);
      break;
    }
    case ObTimeTC: {
      int64_t datetime_val = 0;
      if (OB_FAIL(ObTimeConverter::time_to_datetime(obj.get_time(), cur_ts_value, NULL,
                                                    datetime_val, ObDateTimeType))) {
        LOG_WARN("time_to_datetime failed", K(ret), K(obj), K(cur_ts_value));
      } else if (OB_FAIL(ObTimeConverter::datetime_to_ob_time(datetime_val, NULL, ob_time))) {
        LOG_WARN("datetime to time failed", K(ret));
      }
      break;

      ret = ObTimeConverter::time_to_ob_time(obj.get_time(), ob_time);
      break;
    }
    case ObTextTC: {
      ObArenaAllocator lob_allocator(ObModIds::OB_LOB_ACCESS_BUFFER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
      ObString val;
      if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(&lob_allocator, obj, val))) {
        LOG_WARN("fail to get real data.", K(ret), K(val));
      } else if (OB_FAIL(ObTimeConverter::str_to_ob_time_with_date(val, ob_time,
                         NULL, is_dayofmonth, date_sql_mode))) {
        LOG_WARN("str to time failed", K(ret), K(val), K(is_dayofmonth));
      }
      break;
    }
    case ObStringTC: {
      ret = ObTimeConverter::str_to_ob_time_with_date(
          obj.get_string(), ob_time, NULL, is_dayofmonth, date_sql_mode);
      break;
    }
    case ObLobTC: {
      ObString payload;
      if (OB_FAIL(obj.get_string(payload))) {
        STORAGE_LOG(WARN, "Failed to get payload from lob locator", K(ret), K(obj));
      } else {
        ret = ObTimeConverter::str_to_ob_time_with_date(
            payload, ob_time, NULL, is_dayofmonth, date_sql_mode);
      }
      break;
    }
    case ObNumberTC: {
      int64_t int_part = 0;
      int64_t dec_part = 0;
      const number::ObNumber num = obj.get_number();
      if (num.is_negative()) {
        ret = OB_INVALID_DATE_FORMAT;
        LOG_WARN("invalid date format", K(ret), K(num));
      } else if (!num.is_int_parts_valid_int64(int_part, dec_part)) {
        ret = OB_INVALID_DATE_FORMAT;
        LOG_WARN("invalid date format", K(ret), K(num));
      } else {
        ret = ObTimeConverter::int_to_ob_time_with_date(int_part, ob_time, is_dayofmonth,
                                                        date_sql_mode);
      }
      break;
    }
    default: {
      ret = OB_NOT_SUPPORTED;
    }
  }
  return ret;
}

int ob_obj_to_ob_time_without_date(const ObObj &obj, const ObTimeZoneInfo *tz_info, ObTime &ob_time)
{
  int ret = OB_SUCCESS;
  switch (obj.get_type_class()) {
    case ObIntTC:
      // fallthrough.
    case ObUIntTC: {
      if (OB_FAIL(ObTimeConverter::int_to_ob_time_without_date(obj.get_int(), ob_time))) {
        LOG_WARN("int to ob time without date failed", K(ret));
      } else {
        //mysql中intTC转time时，如果hour超过838，那么time应该为null，而不是最大值。
        const int64_t time_max_val = TIME_MAX_VAL;    // 838:59:59 .
        int64_t value = ObTimeConverter::ob_time_to_time(ob_time);
        if (value > time_max_val) {
          ret = OB_INVALID_DATE_VALUE;
        }
      }
      break;
    }
    case ObOTimestampTC: {
      ret = ObTimeConverter::otimestamp_to_ob_time(obj.get_type(), obj.get_otimestamp_value(), tz_info, ob_time);
      break;
    }
    case ObDateTimeTC: {
      ret = ObTimeConverter::datetime_to_ob_time(obj.get_datetime(),  (ObTimestampType == obj.get_type()) ? tz_info : NULL, ob_time);
      break;
    }
    case ObDateTC: {
      ret = ObTimeConverter::date_to_ob_time(obj.get_date(), ob_time);
      break;
    }
    case ObTimeTC: {
      ret = ObTimeConverter::time_to_ob_time(obj.get_time(), ob_time);
      break;
    }
    case ObTextTC: {
      ObArenaAllocator lob_allocator(ObModIds::OB_LOB_ACCESS_BUFFER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
      ObString val;
      if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(&lob_allocator, obj, val))) {
        LOG_WARN("fail to get real data.", K(ret), K(val));
      } else if (OB_FAIL(ObTimeConverter::str_to_ob_time_without_date(val, ob_time))) {
        LOG_WARN("str to time failed", K(ret), K(val));
      }
      break;
    }
    case ObStringTC: {
      ret = ObTimeConverter::str_to_ob_time_without_date(obj.get_string(), ob_time);
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
    case ObLobTC: {
      ObString payload;
      if (OB_FAIL(obj.get_string(payload))) {
        STORAGE_LOG(WARN, "Failed to get payload from lob locator", K(ret), K(obj));
      } else {
        ret = ObTimeConverter::str_to_ob_time_without_date(payload, ob_time);
      }
      break;
    }
    case ObNumberTC: {
      int64_t int_part = 0;
      int64_t dec_part = 0;
      const number::ObNumber num(obj.get_number());
      if (!num.is_int_parts_valid_int64(int_part, dec_part)) {
        ret = OB_INVALID_DATE_FORMAT;
        LOG_WARN("invalid date format", K(ret), K(num));
      } else {
        if (OB_FAIL(ObTimeConverter::int_to_ob_time_without_date(int_part, ob_time, dec_part))) {
          LOG_WARN("int to ob time without date failed", K(ret));
        } else {
          if ((!ob_time.parts_[DT_YEAR]) && (!ob_time.parts_[DT_MON]) && (!ob_time.parts_[DT_MDAY])) {
            //mysql中intTC转time时，如果超过838:59:59，那么time应该为null，而不是最大值。
            const int64_t time_max_val = TIME_MAX_VAL;    // 838:59:59 .
            int64_t value = ObTimeConverter::ob_time_to_time(ob_time);
            if(value > time_max_val) {
              ret = OB_INVALID_DATE_VALUE;
            }
          }
        }
      }
      break;
    }
    default: {
      ret = OB_NOT_SUPPORTED;
    }
  }
  return ret;
}

int ObObjCaster::to_type(const ObObjType expect_type, ObCastCtx &cast_ctx,
                         const ObObj &in_obj, ObObj &buf_obj, const ObObj *&res_obj)
{
  int ret = OB_SUCCESS;
  res_obj = NULL;
  cast_ctx.warning_ = OB_SUCCESS;
  ObObjType in_type = in_obj.get_type();
  bool is_string = ob_is_string_type(in_type) || ob_is_lob_locator(in_type);
  bool is_geometry = ob_is_geometry(in_type);
  if (OB_UNLIKELY((expect_type == in_type && (!is_string) && (!is_geometry))
      ||  ObNullType == in_type)) {
    buf_obj = in_obj;
    res_obj = &buf_obj;
  } else if (in_obj.get_collation_type() == cast_ctx.dest_collation_
      && (ObVarcharType == in_type || ObCharType == in_type || ob_is_nstring_type(in_type))
      && (ObVarcharType == expect_type
          || ObCharType == expect_type || ob_is_nstring_type(expect_type))
      && NULL == cast_ctx.zf_info_ && NULL == cast_ctx.res_accuracy_) {
    // fast path for char/varchar string_string cast.
    buf_obj = in_obj;
    const_cast<ObObjMeta &>(buf_obj.get_meta()).set_type_simple(expect_type);
    res_obj = &buf_obj;
  } else if (OB_FAIL(to_type(expect_type,
                             (is_string && lib::is_mysql_mode()
                                        && CS_TYPE_INVALID == cast_ctx.dest_collation_) ?
                                in_obj.get_collation_type()
                              : cast_ctx.dest_collation_,
                             cast_ctx,
                             in_obj,
                             buf_obj))) {
    LOG_WARN("failed to cast obj", K(ret), K(in_obj), K(expect_type), K(cast_ctx.cast_mode_));
  } else {
    res_obj = &buf_obj;
  }
  return ret;
}

int ObObjCaster::to_datetime(const ObObjType expect_type, ObCastCtx &cast_ctx,
    const ObObj &in_obj, ObObj &buf_obj, const ObObj *&res_obj)
{
  int ret = OB_SUCCESS;
  if (ObDateTimeType == in_obj.get_type()) {
    res_obj = &in_obj;
  } else {
    cast_ctx.warning_ = OB_SUCCESS;
    switch (in_obj.get_type_class()) {
      case ObIntTC: {
        ret = int_datetime_interval(expect_type, cast_ctx, in_obj, buf_obj, cast_ctx.cast_mode_);
        break;
      }
      case ObFloatTC: {
        ret = float_datetime_interval(expect_type, cast_ctx, in_obj, buf_obj, cast_ctx.cast_mode_);
        break;
      }
      case ObDoubleTC: {
        ret = double_datetime_interval(expect_type, cast_ctx, in_obj, buf_obj, cast_ctx.cast_mode_);
        break;
      }
      case ObNumberTC: {
        ret = number_datetime_interval(expect_type, cast_ctx, in_obj, buf_obj, cast_ctx.cast_mode_);
        break;
      }
      case ObOTimestampTC: {
        ret = otimestamp_datetime(expect_type, cast_ctx, in_obj, buf_obj, cast_ctx.cast_mode_);
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("it should not arrive here", K(ret), K(in_obj), K(expect_type), K(in_obj.get_type_class()));
      }
    }

    if (OB_FAIL(ret)) {
      LOG_WARN("failed to cast obj", K(ret), K(in_obj), K(expect_type), K(cast_ctx.cast_mode_));
    } else {
      res_obj = &buf_obj;
    }
  }
  return ret;
}

//支持xxx向所有类型的转换（包括ObEnumType/ObSetType, 不包括ObInnerEnumType/ObInnerSetType）
int ObObjCaster::to_type(const ObExpectType &expect_type, ObCastCtx &cast_ctx,
                         const ObObj &in_obj, ObObj &buf_obj, const ObObj *&res_obj)
{
  int ret = OB_SUCCESS;
  res_obj = NULL;
  cast_ctx.warning_ = OB_SUCCESS;
  ObObjType dest_type = expect_type.get_type();
  cast_ctx.expect_obj_collation_ = expect_type.get_collation_type();
  if (OB_UNLIKELY((dest_type == in_obj.get_type()
                   && cast_ctx.dest_collation_ == in_obj.get_collation_type())
                  || ObNullType == in_obj.get_type())) {
    res_obj = &in_obj;
  } else if (ObEnumType == dest_type || ObSetType == dest_type) {
    if (OB_FAIL(to_type(expect_type, cast_ctx, in_obj, buf_obj))) {
      LOG_WARN("fail to cast to enum or set", K(expect_type), K(in_obj), K(cast_ctx.cast_mode_), K(ret));
    } else {
      res_obj = &buf_obj;
    }
  } else if (OB_FAIL(to_type(dest_type, expect_type.get_collation_type(), cast_ctx, in_obj, buf_obj))) {
    LOG_WARN("failed to cast obj", K(in_obj), K(expect_type), K(cast_ctx.cast_mode_), K(ret));
  } else {
    res_obj = &buf_obj;
  }
  return ret;
}


int ObObjCaster::to_type(const ObObjType expect_type,
                         ObCastCtx &cast_ctx,
                         const ObObj &in_obj,
                         ObObj &out_obj)
{
  int ret = OB_SUCCESS;
  ObObjType in_type = in_obj.get_type();
  bool is_string = ob_is_string_type(in_type) || ob_is_lob_locator(in_type);
  bool is_geometry = ob_is_geometry(in_type);
  if (OB_UNLIKELY((expect_type == in_type && (!is_string) && (!is_geometry))
      || ObNullType == in_type)) {
    out_obj = in_obj;
  } else if (in_obj.get_collation_type() == cast_ctx.dest_collation_
      && (ObVarcharType == in_type || ObCharType == in_type || ob_is_nstring_type(in_type))
      && (ObVarcharType == expect_type
          || ObCharType == expect_type || ob_is_nstring_type(expect_type))
      && NULL == cast_ctx.zf_info_ && NULL == cast_ctx.res_accuracy_) {
    // fast path for char/varchar string_string cast.
    out_obj = in_obj;
    const_cast<ObObjMeta &>(out_obj.get_meta()).set_type_simple(expect_type);
  } else {
    if (lib::is_oracle_mode() && in_obj.is_character_type()) {
      //防御措施：转成oracle的string类型，
      //字符集是由两个NLS变量决定的，这两个值通过ObCastCtx传入
      ObCollationType dest_collation = ob_is_nstring_type(expect_type) ?
                                        cast_ctx.dtc_params_.nls_collation_nation_
                                      : cast_ctx.dtc_params_.nls_collation_;
      if (CS_TYPE_INVALID != dest_collation) {
        cast_ctx.dest_collation_ = dest_collation;
      }
    }
    ret = to_type(expect_type,
                  (is_string && CS_TYPE_INVALID == cast_ctx.dest_collation_) ?
                     in_obj.get_collation_type() : cast_ctx.dest_collation_,
                  cast_ctx,
                  in_obj,
                  out_obj);
  }
  return ret;
}

int ObObjCaster::bool_to_json(const ObObjType expect_type,
                              ObCastCtx &cast_ctx,
                              const ObObj &in_obj,
                              ObObj &buf_obj,
                              const ObObj *&res_obj)
{
  int ret = OB_SUCCESS;
  res_obj = NULL;
  cast_ctx.warning_ = OB_SUCCESS;
  ObLength res_length = -1;

  if (CM_IS_COLUMN_CONVERT(cast_ctx.cast_mode_)) {
    ret = OB_ERR_INVALID_JSON_TEXT;
    LOG_USER_ERROR(OB_ERR_INVALID_JSON_TEXT);
  } else if (OB_UNLIKELY(cast_ctx.allocator_v2_ == NULL)) {
    ret= OB_ERR_UNEXPECTED;
    LOG_ERROR("NULL allocator in json cast function", K(ret), K(in_obj), K(expect_type));
  } else { 
    int64_t in_val = in_obj.get_int();
    bool bool_val = (in_obj.get_int() == 1) ? true : false;
    ObJsonBoolean j_bool(bool_val);
    ObIJsonBase *j_base = &j_bool;
    ObString raw_bin;
    if (OB_FAIL(j_base->get_raw_binary(raw_bin, cast_ctx.allocator_v2_))) {
      LOG_WARN("fail to get bool json binary", K(ret), K(in_obj), K(expect_type));
    } else if (OB_FAIL(set_json_bin_res(&cast_ctx, &buf_obj, raw_bin))) {
      LOG_WARN("fail to fill json bin lob locator", K(ret));
    } else {
      res_length = static_cast<ObLength>(buf_obj.get_string().length());
      res_obj = &buf_obj;
    }
  }

  ObObjCastParams &params = cast_ctx;
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_STRING, DEFAULT_SCALE_FOR_TEXT, res_length);
  return ret;
}

int ObObjCaster::enumset_to_json(const ObObjType expect_type,
                                 ObCastCtx &cast_ctx,
                                 const ObObj &in_obj,
                                 ObObj &buf_obj,
                                 const ObObj *&res_obj)
{
  int ret = OB_SUCCESS;
  res_obj = NULL;
  cast_ctx.warning_ = OB_SUCCESS;
  ObLength res_length = -1;

  if (CM_IS_COLUMN_CONVERT(cast_ctx.cast_mode_)) {
    ret = OB_ERR_INVALID_JSON_TEXT;
    LOG_USER_ERROR(OB_ERR_INVALID_JSON_TEXT);
  } else if (OB_UNLIKELY(cast_ctx.allocator_v2_ == NULL)) {
    ret= OB_ERR_UNEXPECTED;
    LOG_ERROR("NULL allocator in json cast function", K(ret), K(in_obj), K(expect_type));
  } else { 
    ObIJsonBase *j_base = NULL;
    ObJsonNull j_null;
    ObString j_text = in_obj.get_string();
    ObJsonString j_string(j_text.ptr(), j_text.length());
    if (j_text.length() == 0) {
      j_base = &j_null;
    } else {
      j_base = &j_string;
      ObString raw_bin;
      if (OB_FAIL(j_base->get_raw_binary(raw_bin, cast_ctx.allocator_v2_))) {
        LOG_WARN("fail to get string json binary", K(ret), K(in_obj));
      } else if (OB_FAIL(set_json_bin_res(&cast_ctx, &buf_obj, raw_bin))) {
        LOG_WARN("fail to fill json bin lob locator", K(ret));
      } else {
        res_length = buf_obj.get_string().length();
        buf_obj.set_collation_type(cast_ctx.dest_collation_);
        res_obj = &buf_obj;
      }
    }
  }

  ObObjCastParams &params = cast_ctx;
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_STRING, DEFAULT_SCALE_FOR_TEXT, res_length);
  return ret;
}

int ObObjCaster::to_type(const ObObjType expect_type,
                         ObCollationType expect_cs_type,
                         ObCastCtx &cast_ctx,
                         const ObObj &in_obj,
                         ObObj &out_obj)
{
  int ret = OB_SUCCESS;
  const ObObjTypeClass in_tc = in_obj.get_type_class();
  const ObObjTypeClass out_tc = ob_obj_type_class(expect_type);
  cast_ctx.warning_ = OB_SUCCESS;
  /* expect_cs_type has higer priority than cast_ctx.dest_collation_
      */
  if (CS_TYPE_INVALID != expect_cs_type) {
    cast_ctx.dest_collation_ = expect_cs_type;
  } else {
    expect_cs_type = cast_ctx.dest_collation_;
  }
  /*if (CS_TYPE_INVALID != cast_ctx.dest_collation_) {
    expect_cs_type = cast_ctx.dest_collation_;
  } else {
    cast_ctx.dest_collation_ = expect_cs_type;
  }*/
  if (OB_UNLIKELY(ob_is_invalid_obj_tc(in_tc) || ob_is_invalid_obj_tc(out_tc))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected type", K(ret), K(in_obj), K(expect_type));
  } else if (lib::is_oracle_mode()) {
    /*if (CM_IS_EXPLICIT_CAST(cast_ctx.cast_mode_)) {
      if (OB_FAIL(OBJ_CAST_ORACLE_EXPLICIT[in_tc][out_tc](expect_type, cast_ctx, in_obj, out_obj, cast_ctx.cast_mode_))) {
        LOG_WARN("failed to cast obj", K(ret), K(in_obj), K(in_tc), K(out_tc), K(expect_type), K(cast_ctx.cast_mode_));
      }
    } else {*/
      if (OB_FAIL(OBJ_CAST_ORACLE_IMPLICIT[in_tc][out_tc](expect_type, cast_ctx, in_obj, out_obj, cast_ctx.cast_mode_))) {
        LOG_WARN("failed to cast obj", K(ret), K(in_obj), K(in_tc), K(out_tc), K(expect_type), K(cast_ctx.cast_mode_));
      }
    //}
  } else {
    if (OB_FAIL(OB_OBJ_CAST[in_tc][out_tc](expect_type, cast_ctx, in_obj, out_obj, cast_ctx.cast_mode_))) {
      LOG_WARN("failed to cast obj", K(ret), K(in_obj), K(in_tc), K(out_tc), K(expect_type), K(cast_ctx.cast_mode_));
    }
  }
  if (OB_SUCC(ret)) {
    if (ObStringTC == out_tc || ObTextTC == out_tc || ObLobTC == out_tc) {
      if (ObStringTC == in_tc || ObTextTC == in_tc || ObLobTC == out_tc) {
        out_obj.set_collation_level(in_obj.get_collation_level());
      } else {
        out_obj.set_collation_level(CS_LEVEL_COERCIBLE);
      }
      if (OB_LIKELY(expect_cs_type != CS_TYPE_INVALID)) {
        out_obj.set_collation_type(expect_cs_type);
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected collation type", K(ret), K(in_obj),
                           K(out_obj), K(expect_cs_type), K(common::lbt()));
      }
    }
  }

  LOG_DEBUG("succ to to_type", K(ret), "in_type", in_obj.get_type(), K(in_obj),
            K(expect_type), K(out_obj), K(lbt()));
  return ret;
}

//use for cast to enum or set type
int ObObjCaster::to_type(const ObExpectType &expect_type,
                         ObCastCtx &cast_ctx,
                         const ObObj &in_obj,
                         ObObj &out_obj)
{
  int ret = OB_SUCCESS;
  const ObObjTypeClass in_tc = in_obj.get_type_class();
  const ObObjType out_type = expect_type.get_type();
  if (OB_UNLIKELY(!ob_is_enumset_tc(expect_type.get_type()))
      || OB_UNLIKELY(ob_is_invalid_obj_tc(in_tc))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected expect_type", K(ret), K(expect_type), K(in_obj));
  } else if (OB_FAIL(OB_CAST_ENUM_OR_SET[in_tc][ObSetType == out_type](expect_type, cast_ctx, in_obj, out_obj))) {
    LOG_WARN("fail to cast to enum or set", K(ret), K(in_obj), K(expect_type));
  } else {
    LOG_DEBUG("succ to to_type", K(expect_type), K(in_obj), K(out_obj));
  }
  return ret;
}

const char OB_JSON_NULL[2] = {'\0', '\0'}; // binary json null

int ObObjCaster::get_zero_value(const ObObjType expect_type,
                                ObCollationType expect_cs_type,
                                int64_t data_len,
                                ObIAllocator &alloc,
                                ObObj &zero_obj)
{
  int ret = OB_SUCCESS;
  ObObjCastParams params; //构造一个空的cast_param对象，适配SET_RES_XXX宏定义
  ObCastMode cast_mode = CM_WARN_ON_FAIL;
  params.warning_ = 1; //将warning code设置为1，避免SET_RES_XXX宏将其当做真实的warning处理
  if (ob_is_string_tc(expect_type)) {
    //zero_obj.set_string(expect_type, "");
    ObString padding_res;
    if (ObVarcharType == expect_type) {
      zero_obj.set_string(expect_type, "");
    } else if (0 > data_len) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("The length of fix-len string is less than zero", K(ret), K(data_len));
    } else if (OB_FAIL(sql::padding_char_for_cast(data_len, expect_cs_type, alloc,
                                      padding_res))) {
      LOG_WARN("padding char failed", K(ret), K(data_len), K(expect_cs_type));
    } else {
      zero_obj.set_string(expect_type, padding_res);
    }
  } else if (ob_is_text_tc(expect_type)) {
    zero_obj.set_lob_value(expect_type, static_cast<const char *>(NULL), 0);
  } else if (ob_is_int_tc(expect_type)) {
    int64_t value = 0;
    SET_RES_INT(zero_obj);
  } else if (ob_is_uint_tc(expect_type)) {
    uint64_t value = 0;
    SET_RES_UINT(zero_obj);
  } else if (ob_is_float_tc(expect_type)) {
    double value = 0.0;
    SET_RES_FLOAT(zero_obj);
  } else if (ob_is_double_tc(expect_type)) {
    double value = 0.0;
    SET_RES_DOUBLE(zero_obj);
  } else if (ob_is_number_tc(expect_type)) {
    number::ObNumber value;
    value.set_zero();
    SET_RES_NUMBER(zero_obj);
  } else if (ob_is_datetime_tc(expect_type)) {
    int64_t value = 0;
    SET_RES_DATETIME(zero_obj);
  } else if (ob_is_date_tc(expect_type)) {
    int32_t value = 0;
    SET_RES_DATE(zero_obj);
  } else if (ob_is_time_tc(expect_type)) {
    int32_t value = 0;
    SET_RES_TIME(zero_obj);
  } else if (ob_is_year_tc(expect_type)) {
    int64_t value = 0;
    SET_RES_YEAR(zero_obj);
  } else if (ob_is_bit_tc(expect_type)) {
    uint64_t value = 0;
    SET_RES_BIT(zero_obj);
  } else if (ObEnumType == expect_type) {
    uint64_t value = 0;
    SET_RES_ENUM(zero_obj);
  } else if (ObSetType == expect_type) {
    uint64_t value = 0;
    SET_RES_SET(zero_obj);
  } else if (ob_is_otimestampe_tc(expect_type)) {
    ObOTimestampData value;
    SET_RES_OTIMESTAMP(zero_obj);
  } else if (ob_is_interval_ym(expect_type)) {
    ObIntervalYMValue value;
    SET_RES_INTERVAL_YM(zero_obj);
  } else if (ob_is_interval_ds(expect_type)) {
    ObIntervalDSValue value;
    SET_RES_INTERVAL_DS(zero_obj);
  } else if (ob_is_urowid(expect_type)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("urowid with default value not supported");
  } else if (expect_type == ObJsonType) {
    zero_obj.set_json_value(expect_type, OB_JSON_NULL, 2);
  } else if (ob_is_user_defined_sql_type(expect_type)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("default value of udt, should be it's default constructor");
  }
  if (OB_SUCC(ret)) {
    zero_obj.set_collation_type(expect_cs_type);
  }
  return ret;
}

int ObObjCaster::enumset_to_inner(const ObObjMeta &expect_meta,
    const ObObj &in_obj, ObObj &out_obj, common::ObIAllocator &allocator,
    const common::ObIArray<common::ObString> &str_values)
{
  int ret = OB_SUCCESS;
  if (ObEnumType != in_obj.get_type() && ObSetType != in_obj.get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected expect_type", K(ret), K(in_obj));
  } else {
    const ObObjType expect_type = (ObEnumType == in_obj.get_type() ? ObEnumInnerType : ObSetInnerType);
    const int64_t element_num = str_values.count();
    const uint64_t element_val = in_obj.get_enum();
    if (ObEnumInnerType == expect_type) {
      int64_t element_idx = static_cast<int64_t>(element_val -1);//enum value start from 1
      ObString element_str;
      if (OB_UNLIKELY(element_num < 1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("str_values_ should not be empty", K(str_values), K(ret));
      } else if (OB_UNLIKELY(0 == element_val)) {
        //do nothing just keep element_string empty
      } else {
        if (OB_UNLIKELY(element_idx > element_num - 1)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid enum value", K(element_idx), K(element_num), K(element_val), K(in_obj), K(ret));
        } else {
          element_str = str_values.at(element_idx);
        }
      }

      if (OB_SUCC(ret)) {
        ObEnumSetInnerValue inner_value(element_val, element_str);
        char *buf = NULL;
        const int64_t BUF_LEN = inner_value.get_serialize_size();
        int64_t pos = 0;
        if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(BUF_LEN)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc memory failed", K(ret), KP(buf), K(BUF_LEN));
        } else if (OB_FAIL(inner_value.serialize(buf, BUF_LEN, pos))) {
          LOG_WARN("failed to serialize inner_value", K(BUF_LEN), K(ret));
        } else {
          out_obj.set_varchar(buf, static_cast<ObString::obstr_size_t>(pos));
          out_obj.set_type(expect_type);
          out_obj.set_collation(expect_meta);
        }
      }
    } else {//to setinner
      static const int64_t EFFECTIVE_COUNT = 64;
      if (OB_UNLIKELY(element_num < 1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid element_num", K(str_values), K(element_num), K(ret));
      } else if (OB_UNLIKELY(element_num < EFFECTIVE_COUNT)
          && OB_UNLIKELY(element_val >= (1ULL << element_num))) {
        //check the validation of element_val
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("set value out of range", K(element_val), K(element_num));
      }

      ObSqlString sql_string;
      uint64_t index = 1ULL;
      const ObString &sep = ObCharsetUtils::get_const_str(expect_meta.get_collation_type(), ',');
      for (int64_t i = 0;
           OB_SUCC(ret) && i < element_num && i < EFFECTIVE_COUNT && element_val >= index;
           ++i, index = index << 1) {
        if (element_val & (index)) {
          const ObString &tmp_val = str_values.at(i);
          if (OB_FAIL(sql_string.append(tmp_val))) {
            LOG_WARN("fail to deep copy str", K(element_val), K(i), K(ret));
          } else if ((element_val >= (index << 1)) && (OB_FAIL(sql_string.append(sep)))) {
            LOG_WARN("fail to deep copy comma", K(element_val), K(tmp_val), K(i), K(ret));
          }
        }
      }

      if (OB_SUCC(ret)) {
        ObString string_value(sql_string.length(), sql_string.ptr());
        ObEnumSetInnerValue inner_value(element_val, string_value);
        char *buf = NULL;
        const int64_t BUF_LEN = inner_value.get_serialize_size();
        int64_t pos = 0;
        if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(BUF_LEN)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc memory failed", K(ret), KP(buf), K(BUF_LEN));
        } else if (OB_FAIL(inner_value.serialize(buf, BUF_LEN, pos))) {
          LOG_WARN("failed to serialize inner_value", K(BUF_LEN), K(ret));
        } else {
          out_obj.set_varchar(buf, static_cast<ObString::obstr_size_t>(pos));
          out_obj.set_type(expect_type);
          out_obj.set_collation(expect_meta);
        }
      }
    }
  }
  return ret;
}


/**
 * 单调的意思是指：
 * 假设A类型的两个obj：a1和a2，转为B类型后分别为：b1和b2,。
 * 若对任意a1、a2，都有：a1和a2的大小关系，与b1、b2的大小关系相同，
 * 则A类型到B类型的转换操作为单调的。
 * @param tc1 源类型
 * @param tc2 目标类型
 * @return
 */
int ObObjCaster::is_cast_monotonic(ObObjType t1, ObObjType t2, bool &is_monotonic)
{
  int ret = OB_SUCCESS;
  ObObjTypeClass tc1 = ob_obj_type_class(t1);
  ObObjTypeClass tc2 = ob_obj_type_class(t2);
  if (OB_UNLIKELY(ob_is_invalid_obj_tc(tc1) || ob_is_invalid_obj_tc(tc2))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected type", K(ret), K(t1), K(t2), K(tc1), K(tc2));
  } else {
    is_monotonic = CAST_MONOTONIC[tc1][tc2];
  }
  return ret;
}

/*
 *  c1 = 'A' check if c1 can be ordering
 */
int ObObjCaster::is_const_consistent(const ObObjMeta &const_mt,
                                     const ObObjMeta &column_mt,
                                     const ObObjType calc_type,
                                     const ObCollationType calc_collation,
                                     bool &result)
{
  int ret = OB_SUCCESS;
  result = false;
  ObObjTypeClass tc1 = const_mt.get_type_class();
  ObObjTypeClass tc2 = column_mt.get_type_class();
  if (OB_UNLIKELY(ob_is_invalid_obj_tc(tc1) || ob_is_invalid_obj_tc(tc2))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected obj type class", K(ret), K(const_mt), K(column_mt));
  } else if (const_mt.is_string_or_lob_locator_type()
              && column_mt.is_string_or_lob_locator_type()) {
    if (column_mt.get_collation_type() == calc_collation) {
      result = true;
    } else {
      // false
    }
  } else {
    if (OB_FAIL(ObObjCaster::is_cast_monotonic(column_mt.get_type(),
                                               calc_type,
                                               result))) {
         LOG_WARN("check is cast monotonic failed", K(ret));
    } else if (!result) {
      // false
    } else if (OB_FAIL(ObObjCaster::is_cast_monotonic(calc_type,
                                                      column_mt.get_type(),
                                                      result))) {
      LOG_WARN("check is cast monotonic failed", K(ret));
    } else {
      // do nothing
    }
  }
  return ret;
}

/* make sure that you have read the doc before you call these functions !
 *
 * doc:
 */

int ObObjCaster::is_order_consistent(const ObObjMeta &from,
                                     const ObObjMeta &to,
                                     bool &result)
{
  int ret = OB_SUCCESS;
  result = false;
  ObObjTypeClass tc1 = from.get_type_class();
  ObObjTypeClass tc2 = to.get_type_class();
  if (OB_UNLIKELY(ob_is_invalid_obj_tc(tc1) || ob_is_invalid_obj_tc(tc2))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected obj type class", K(ret), K(from), K(to));
  } else if (from.is_string_or_lob_locator_type() && to.is_string_or_lob_locator_type()) {
    ObCollationType res_cs_type = CS_TYPE_INVALID;
    ObCollationLevel res_cs_level = CS_LEVEL_INVALID;
    ObCollationType from_cs_type = from.get_collation_type();
    ObCollationType to_cs_type = to.get_collation_type();
    if (OB_FAIL(ObCharset::aggregate_collation(from.get_collation_level(),
                                               from_cs_type,
                                               to.get_collation_level(),
                                               to_cs_type,
                                               res_cs_level,
                                               res_cs_type))) {
      LOG_WARN("fail to aggregate collation", K(ret), K(from), K(to));
    } else {
      int64_t idx_from = get_idx_of_collate(from_cs_type);
      int64_t idx_to = get_idx_of_collate(to_cs_type);
      int64_t idx_res = get_idx_of_collate(res_cs_type);
      if (idx_from >= 0 && idx_from < VALID_OC_COLLATION_TYPES &&
          idx_to   >= 0 && idx_to   < VALID_OC_COLLATION_TYPES &&
          idx_res  >= 0 && idx_res  < VALID_OC_COLLATION_TYPES) {
        result = ORDER_CONSISTENT_WITH_BOTH_STRING[idx_from][idx_to][idx_res];
      } else {
        result = (from_cs_type == to_cs_type) && (from_cs_type == res_cs_type);
      }
    }
  } else {
    result = ORDER_CONSISTENT[tc1][tc2];
  }
  return ret;
}

/**
 * @brief ObObjCaster::oracle_number_to_char
 * 将number类型转为兼容oracle的字符串
 * 大于40字节时将会被转成科学计数法,输出40个字节的字符串
 * 小于40字节时原样输出
 */
int ObObjCaster::oracle_number_to_char(const number::ObNumber &number_val,
                                       const bool is_from_number_type,
                                       const int16_t scale,
                                       const int64_t len,
                                       char *buf,
                                       int64_t &pos)
{
  int ret = OB_SUCCESS;
  bool need_to_sci = true;
  if (OB_UNLIKELY(pos > len || len < 0 || pos < 0 || NULL == buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid value", K(ret), K(pos), K(len), KP(buf));
  } else if (is_from_number_type) {
    if (OB_FAIL(number_val.format_with_oracle_limit(buf, len, pos, scale))) {
      LOG_WARN("fail to format", K(ret));
    }
  } else {
    if (OB_FAIL(number_val.format_v2(buf, len, pos, scale, need_to_sci))) {
      LOG_WARN("failed to format number to string", K(ret));
    }
  }
  return ret;
}

int ObObjCaster::can_cast_in_oracle_mode(
    const ObObjType dest_type, const ObCollationType dest_coll_type,
    const ObObjType src_type, const ObCollationType src_coll_type)
{
  int ret = OB_SUCCESS;
  ObObjTypeClass dest_tc = ob_obj_type_class(dest_type);
  ObObjTypeClass src_tc = ob_obj_type_class(src_type);
  if (cast_not_expected == OBJ_CAST_ORACLE_IMPLICIT[src_tc][dest_tc]) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not expected obj type convert", K(src_tc), K(dest_tc), K(ret));
  } else if (cast_inconsistent_types == OBJ_CAST_ORACLE_IMPLICIT[src_tc][dest_tc]) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("inconsistent datatypes", "expected", dest_tc, "got", src_tc, K(ret));
  } else {
    const bool blob_in = (ob_is_lob_tc(src_type) || ob_is_text_tc(src_type)) && CS_TYPE_BINARY == src_coll_type;
    const bool blob_out = (ob_is_lob_tc(dest_type) || ob_is_text_tc(dest_type)) && CS_TYPE_BINARY == dest_coll_type;
    const bool clob_in = (ob_is_lob_tc(src_type) || ob_is_text_tc(src_type)) && CS_TYPE_BINARY != src_coll_type;
    const bool clob_out = (ob_is_lob_tc(dest_type) || ob_is_text_tc(dest_type)) && CS_TYPE_BINARY != dest_coll_type;
    if ((blob_in && !(blob_out || ob_is_raw_tc(dest_type)))
        || (blob_out && !(blob_in || ob_is_raw_tc(src_type)))) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("inconsistent datatypes", "expected", dest_tc, "got", src_tc, K(ret));
    } else if ((clob_in && !ob_is_string_or_lob_type(dest_type))
              || (clob_out && !ob_is_string_or_lob_type(src_type))) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("inconsistent datatypes", "expected", dest_tc, "got", src_tc, K(ret));
    }
  }
  return ret;
}

int ObObjCaster::get_obj_param_text(const ObObjParam &obj_param,
                                    const ObString raw_text, ObIAllocator &allocator,
                                    ObCollationType cs_type, ObString &res)
{
  int ret = OB_SUCCESS;
  if (obj_param.is_string_type()) {
    res = obj_param.get_string();
  } else if (obj_param.is_numeric_type()) {
    if (obj_param.get_raw_text_pos() >= 0 && obj_param.get_raw_text_len() >= 0
        && obj_param.get_raw_text_pos() + obj_param.get_raw_text_len() <= raw_text.length()) {
      res.assign_ptr(raw_text.ptr() + obj_param.get_raw_text_pos(), obj_param.get_raw_text_len());
    } else {
      // raw_text_pos_ and raw_text_len_ are not set in ps mode.
      ObCastMode cast_mode = CM_NONE;
      ObObj result;
      ObCastCtx cast_ctx(&allocator, NULL, cast_mode, cs_type);
      if (OB_FAIL(to_type(ObVarcharType, cast_ctx, obj_param, result))) {
        LOG_WARN("to varchar type failed", K(ret));
      } else {
        res = result.get_string();
      }
    }
  }
  return ret;
}

const bool ObObjCaster::CAST_MONOTONIC[ObMaxTC][ObMaxTC] =
{
  // null
  {
    false,  // null
    false,  // int
    false,  // uint
    false,  // float
    false,  // double
    false,  // number
    false,  // datetime
    false,  // date
    false,  // time
    false,  // year
    false,  // string
    false,  // extend
    false,  // unknown
    false,  // text
    false,  // bit
    false,  // enumset
    false,  // enumsetinner
    false,  // OTimestamp
    false,  // raw
    false,  // interval
    false,  // rowid
    false,  // lob
    false,  // json
    false,  // gis
    false,  // udt
  },
  // int
  {
    false,  // null
    true,   // int
    true,   // uint
    false,   // float
    false,   // double
    true,   // number
    false,  // datetime
    false,  // date
    false,  // time
    false,  // year
    false,  // string
    false,  // extend
    false,  // unknown
    false,  // text
    true,   // bit
    false,  // enumset
    false,  // enumsetInner
    false,  // OTimestamp
    false,  // raw
    false,  // interval
    false,  // rowid
    false,  // lob
    false,  // json
    false,  // gis
    false,  // udt
  },
  // uint
  {
    false,  // null
    true,   // int
    true,   // uint
    false,   // float
    false,   // double
    true,   // number
    false,  // datetime
    false,  // date
    false,  // time
    false,  // year
    false,  // string
    false,  // extend
    false,  // unknown
    false,  // text
    true,   // bit
    false,  // enumset
    false,  // enumsetInner
    false,  // OTimestamp
    false,  // raw
    false,  // interval
    false,  // rowid
    false,  // lob
    false,  // json
    false,  // gis
    false,  // udt
  },
  // float
  {
    false,  // null
    true,   // int
    true,   // uint
    true,   // float
    true,   // double
    true,   // number
    false,  // datetime
    false,  // date
    false,  // time
    false,  // year
    false,  // string
    false,  // extend
    false,  // unknown
    false,  // text
    false,  // enumset
    false,  // enumsetInner
    true,   // bit
    false,  // OTimestamp
    false,  // raw
    false,  // interval
    false,  // rowid
    false,  // lob
    false,  // json
    false,  // gis
    false,  // udt
  },
  // double
  {
    false,  // null
    true,   // int
    true,   // uint
    true,   // float
    true,   // double
    true,   // number
    false,  // datetime
    false,  // date
    false,  // time
    false,  // year
    false,  // string
    false,  // extend
    false,  // unknown
    false,  // text
    true,   // bit
    false,  // enumset
    false,  // enumsetInner
    false,  // OTimestamp
    false,  // raw
    false,  // interval
    false,  // rowid
    false,  // lob
    false,  // json
    false,  // gis
    false,  // udt
  },
  // number
  {
    false,  // null
    true,   // int
    true,   // uint
    true,   // float
    true,   // double
    true,   // number
    false,  // datetime
    false,  // date
    false,  // time
    false,  // year
    false,  // string
    false,  // extend
    false,  // unknown
    false,  // text
    true,   // bit
    false,  // enumset
    false,  // enumsetInner
    false,  // OTimestamp
    false,  // raw
    false,  // interval
    false,  // rowid
    false,  //lob
    false,  // json
    false,  // gis
    false,  // udt
  },
  // datetime
  {
    false,  // null
    true,   // int
    true,   // uint
    true,   // float
    true,   // double
    true,   // number
    true,   // datetime
    true,   // date
    false,  // time
    true,   // year
    true,   // string
    false,  // extend
    false,  // unknown
    true,   // text
    true,   // bit
    false,  // enumset
    false,  // enumsetInner
    true,   // OTimestamp
    false,  // raw
    false,  // interval
    false,  // rowid
    true,   //lob
    true,   // json
    false,  // gis
    false,  // udt
  },
  // date
  {
    false,  // null
    true,   // int
    true,   // uint
    true,   // float
    true,   // double
    true,   // number
    true,   // datetime
    true,   // date
    false,  // time
    true,   // year
    true,   // string
    false,  // extend
    false,  // unknown
    true,   // text
    true,   // bit
    false,  // enumset
    false,  // enumsetInner
    true,   // OTimestamp
    false,  // raw
    false,  // interval
    false,  // rowid
    true,   // lob
    true,   // json
    false,  // gis
    false,  // udt
  },
  // time
  {
    false,  // null
    true,   // int
    true,   // uint
    true,   // float
    true,   // double
    true,   // number
    true,   // datetime
    false,  // date
    true,   // time
    false,  // year
    false,  // string
    false,  // extend
    false,  // unknown
    true,   // text
    true,   // bit
    false,  // enumset
    false,  // enumsetInner
    true,   // OTimestamp
    false,  // raw
    false,  // interval
    false,  // rowid
    true,   //lob
    true,   // json
    false,  // gis
    false,  // udt
  },
  // year
  {
    false,  // null
    true,   // int
    true,   // uint
    true,   // float
    true,   // double
    true,   // number
    true,   // datetime
    true,   // date
    false,  // time
    true ,  // year
    true,   // string
    false,  // extend
    false,  // unknown
    true,   // text
    true,   // bit
    false,  // enumset
    false,  // enumsetInner
    true,   // OTimestamp
    false,  // raw
    false,  // interval
    false,  // rowid
    true,   // lob
    true,   // json
    false,  // gis
    false,  // udt
  },
  // string
  {
    false,  // null
    false,  // int
    false,  // uint
    false,  // float
    false,  // double
    false,  // number
    false,  // datetime
    false,  // date
    false,  // time
    false,  // year
    true,   // string
    false,  // extend
    false,  // unknown
    true,   // text
    false,  // bit
    false,  // enumset
    false,  // enumsetInner
    false,  // OTimestamp
    true,   // raw
    true,  // interval
    true,   // rowid
    true,   // lob
    false,  // json
    false,  // gis
    false,  // udt
  },
  // extend
  {
    false,  // null
    false,  // int
    false,  // uint
    false,  // float
    false,  // double
    false,  // number
    false,  // datetime
    false,  // date
    false,  // time
    false,  // year
    false,  // string
    false,  // extend
    false,  // unknown
    false,  // text
    false,  // bit
    false,  // enumset
    false,  // enumsetInner
    false,  // OTimestamp
    false,  // raw
    false,  // interval
    false,  // rowid
    false,  // lob
    false,  // json
    false,  // gis
    false,  // udt
  },
  // unknown
  {
    false,  // null
    false,  // int
    false,  // uint
    false,  // float
    false,  // double
    false,  // number
    false,  // datetime
    false,  // date
    false,  // time
    false,  // year
    false,  // string
    false,  // extend
    false,  // unknown
    false,  // text
    false,  // bit
    false,  // enumset
    false,  // enumsetInner
    false,  // OTimestamp
    false,  // raw
    false,  // interval
    false,  // rowid
    false,  // lob
    false,  // json
    false,  // gis
    false,  // udt
  },
  // text
  {
    false,  // null
    false,  // int
    false,  // uint
    false,  // float
    false,  // double
    false,  // number
    false,  // datetime
    false,  // date
    false,  // time
    false,  // year
    true,   // string
    false,  // extend
    false,  // unknown
    true,   // text
    false,  // bit
    false,  // enumset
    false,  // enumsetInner
    false,  // OTimestamp
    false,  // raw
    false,  // interval
    false,  // rowid
    true,   // lob
    false,  // json
    false,  // gis
    false,  // udt
  },
   // bit
  {
    false,  // null
    true,   // int
    true,   // uint
    true,   // float
    true,   // double
    true,   // number
    false,  // datetime
    false,  // date
    false,  // time
    false,  // year
    false,  // string
    false,  // extend
    false,  // unknown
    false,  // text
    true,   // bit
    false,  // enumset
    false,  // enumsetInner
    false,  // OTimestamp
    false,  // raw
    false,  // interval
    false,  // rowid
    false,  //lob
    false,  // json
    false,  // gis
    false,  // udt
  },
  //enumset
  {
    false,  // null
    false,  // int
    false,  // uint
    false,  // float
    false,  // double
    false,  // number
    false,  // datetime
    false,  // date
    false,  // time
    false,  // year
    false,  // string
    false,  // extend
    false,  // unknown
    false,  // text
    false,  // bit
    false,  // enumset
    false,  // enumsetInner
    false,  // OTimestamp
    false,  // raw
    false,  // interval
    false,  // rowid
    false,  // lob
    false,  // json
    false,  // gis
    false,  // udt
  },
  //enumsetinner
  {
    false,  // null
    false,  // int
    false,  // uint
    false,  // float
    false,  // double
    false,  // number
    false,  // datetime
    false,  // date
    false,  // time
    false,  // year
    false,  // string
    false,  // extend
    false,  // unknown
    false,  // text
    false,  // bit
    false,  // enumset
    false,  // enumsetInner
    false,  // OTimestamp
    false,  // raw
    false,  // interval
    false,  // rowid
    false,  // text
    false,  // json
    false,  // gis
    false,  // udt
  },
  // OTimestamp
  {
    false,  // null
    true,   // int
    true,   // uint
    true,   // float
    true,   // double
    true,   // number
    true,   // datetime
    true,   // date
    false,  // time
    true,   // year
    true,   // string
    false,  // extend
    false,  // unknown
    true,   // text
    true,   // bit
    false,  // enumset
    false,  // enumsetInner
    true,   // OTimestamp
    false,  // raw
    false,  // interval
    false,  //rowid
    true,   // lob
    true,   // json
    false,  // gis
    false,  // udt
  },
  // raw
  {
    false,  // null
    false,  // int
    false,  // uint
    false,  // float
    false,  // double
    false,  // number
    false,  // datetime
    false,  // date
    false,  // time
    false,  // year
    true,   // string
    false,  // extend
    false,  // unknown
    false,  // text
    false,  // bit
    false,  // enumset
    false,  // enumsetInner
    false,  // OTimestamp
    true,   // raw
    false,  // interval,
    false,  // rowid
    false,  // lob
    false,  // json
    false,  // gis
    false,  // udt
  },
  // interval
  {
    false,  // null
    false,  // int
    false,  // uint
    false,  // float
    false,  // double
    false,  // number
    false,  // datetime
    false,  // date
    false,  // time
    false,  // year
    false,   // string
    false,  // extend
    false,  // unknown
    false,  // text
    false,  // bit
    false,  // enumset
    false,  // enumsetInner
    false,  // OTimestamp
    false,   // raw
    true,  // interval,
    false,  // rowid
    false,  // lob
    false,  // json
    false,  // gis
    false,  // udt
  },
  // rowid
  {
    false,  // null
    false,  // int
    false,  // uint
    false,  // float
    false,  // double
    false,  // number
    false,  // datetime
    false,  // date
    false,  // time
    false,  // year
    true,   // string
    false,  // extend
    false,  // unknown
    false,  // text
    false,  // bit
    false,  // enumset
    false,  // enumsetInner
    false,  // OTimestamp
    false,   // raw
    false,  // interval,
    true,  // rowid
    false,  // lob
    false,  // json
    false,  // gis
    false,  // udt
  },
  // lob
  {
    false,  // null
    false,  // int
    false,  // uint
    false,  // float
    false,  // double
    false,  // number
    false,  // datetime
    false,  // date
    false,  // time
    false,  // year
    true,   // string
    false,  // extend
    false,  // unknown
    true,   // text
    false,  // bit
    false,  // enumset
    false,  // enumsetInner
    false,  // OTimestamp
    false,  // raw
    false,  // interval
    false,  // rowid
    true,   // lob
    false,  // json
    false,  // gis
    false,  // udt
  },
  // json
  {
    false,  // null
    false,  // int
    false,  // uint
    false,  // float
    false,  // double
    false,  // number
    false,  // datetime
    false,  // date
    false,  // time
    false,  // year
    false,  // string
    false,  // extend
    false,  // unknown
    false,  // text
    false,  // bit
    false,  // enumset
    false,  // enumsetInner
    false,  // OTimestamp
    false,  // raw
    false,  // interval
    false,  // rowid
    false,  // lob
    true,   // json
    false,  // gis
    false,  // udt
  },
  // gis
  {
    false,  // null
    false,  // int
    false,  // uint
    false,  // float
    false,  // double
    false,  // number
    false,  // datetime
    false,  // date
    false,  // time
    false,  // year
    false,  // string
    false,  // extend
    false,  // unknown
    false,  // text
    false,  // bit
    false,  // enumset
    false,  // enumsetInner
    false,  // OTimestamp
    false,  // raw
    false,  // interval
    false,  // rowid
    false,  // lob
    false,  // json
    true,   // gis
    false,  // udt
  },
  // udt
  {
    false,  // null
    false,  // int
    false,  // uint
    false,  // float
    false,  // double
    false,  // number
    false,  // datetime
    false,  // date
    false,  // time
    false,  // year
    false,  // string
    false,  // extend
    false,  // unknown
    false,  // text
    false,  // bit
    false,  // enumset
    false,  // enumsetInner
    false,  // OTimestamp
    false,  // raw
    false,  // interval
    false,  // rowid
    false,  // lob
    false,  // json
    false,  // gis
    false,  // udt
  },
};

const bool ObObjCaster::ORDER_CONSISTENT[ObMaxTC][ObMaxTC] =
{
  // null
  {
    false,  // null
    false,  // int
    false,  // uint
    false,  // float
    false,  // double
    false,  // number
    false,  // datetime
    false,  // date
    false,  // time
    false,  // year
    false,  // string
    false,  // extend
    false,  // unknown
    false,  // lob
    false,  // bit
    false,  // enumset
    false,  // enumsetInner
    false,  // OTimestamp
    false,  // raw
  },
  // int
  {
    false,  // null
    true,   // int
    true,   // uint
    true,   // float
    true,   // double
    true,   // number
    false,  // datetime
    false,  // date
    false,  // time
    true,  // year
    false,  // string
    false,  // extend
    false,  // unknown
    false,  // lob
    true,   // bit
    true,  // enumset
    false,  // enumsetInner
    false,  // OTimestamp
    false,  // raw
  },
  // uint
  {
    false,  // null
    true,   // int
    true,   // uint
    true,   // float
    true,   // double
    true,   // number
    false,  // datetime
    false,  // date
    false,  // time
    false,  // year
    false,  // string
    false,  // extend
    false,  // unknown
    false,  // lob
    true,   // bit
    true,  // enumset
    false,  // enumsetInner
    false,  // OTimestamp
    false,  // raw
  },
  // float
  {
    false,  // null
    false,   // int
    false,   // uint
    true,   // float
    true,   // double
    false,   // number
    false,  // datetime
    false,  // date
    false,  // time
    false,  // year
    false,  // string
    false,  // extend
    false,  // unknown
    false,  // lob
    false,   // bit
    false,  // enumset
    false,  // enumsetInner
    false,  // OTimestamp
    false,  // raw
  },
  // double
  {
    false,  // null
    false,   // int
    false,   // uint
    true,   // float
    true,   // double
    false,   // number
    false,  // datetime
    false,  // date
    false,  // time
    false,  // year
    false,  // string
    false,  // extend
    false,  // unknown
    false,  // lob
    false,   // bit
    false,  // enumset
    false,  // enumsetInner
    false,  // OTimestamp
    false,  // raw
  },
  // number
  {
    false,  // null
    true,   // int
    true,   // uint
    true,   // float
    true,   // double
    true,   // number
    false,  // datetime
    false,  // date
    false,  // time
    true,  // year
    false,  // string
    false,  // extend
    false,  // unknown
    false,  // lob
    true,   // bit
    true,  // enumset
    false,  // enumsetInner
    false,  // OTimestamp
    false,  // raw
  },
  // datetime
  {
    false,  // null
    false,   // int
    false,   // uint
    false,   // float
    false,   // double
    false,   // number
    true,   // datetime
    true,   // date
    true,  // time
    true,   // year
    false,   // string
    false,  // extend
    false,  // unknown
    false,   // lob
    false,   // bit
    false,  // enumset
    false,  // enumsetInner
    true,  // OTimestamp
    false,  // raw
  },
  // date
  {
    false,  // null
    false,   // int
    false,   // uint
    false,   // float
    false,   // double
    false,   // number
    true,   // datetime
    true,   // date
    true,  // time
    true,   // year
    false,   // string
    false,  // extend
    false,  // unknown
    false,   // lob
    false,   // bit
    false,  // enumset
    false,  // enumsetInner
    true,  // OTimestamp
    false,  // raw
  },
  // time
  {
    false,  // null
    false,   // int
    false,   // uint
    false,   // float
    false,   // double
    false,   // number
    true,   // datetime
    true,  // date
    true,   // time
    true,  // year
    false,  // string
    false,  // extend
    false,  // unknown
    false,   // lob
    false,   // bit
    false,  // enumset
    false,  // enumsetInner
    true,  // OTimestamp
    false,  // raw
  },
  // year //0000-9999
  {
    false,  // null
    true,   // int
    true,   // uint
    true,   // float
    true,   // double
    true,   // number
    false,   // datetime
    false,   // date
    false,  // time
    true ,  // year
    false,   // string
    false,  // extend
    false,  // unknown
    false,   // lob
    true,   // bit
    true,  // enumset
    false,  // enumsetInner
    false,  // OTimestamp
    false,  // raw
  },
  // string
  {
    false,  // null
    false,  // int
    false,  // uint
    false,  // float
    false,  // double
    false,  // number
    false,  // datetime
    false,  // date
    false,  // time
    false,  // year
    false,   // string
    false,  // extend
    false,  // unknown
    false,   // lob
    false,   // bit
    false,  // enumset
    false,  // enumsetInner
    false,  // OTimestamp
    false,  // raw
  },
  // extend
  {
    false,  // null
    false,  // int
    false,  // uint
    false,  // float
    false,  // double
    false,  // number
    false,  // datetime
    false,  // date
    false,  // time
    false,  // year
    false,  // string
    false,  // extend
    false,  // unknown
    false,  // lob
    false,   // bit
    false,  // enumset
    false,  // enumsetInner
    false,  // OTimestamp
    false,  // raw
  },
  // unknown
  {
    false,  // null
    false,  // int
    false,  // uint
    false,  // float
    false,  // double
    false,  // number
    false,  // datetime
    false,  // date
    false,  // time
    false,  // year
    false,  // string
    false,  // extend
    false,  // unknown
    false,  // lob
    false,   // bit
    false,  // enumset
    false,  // enumsetInner
    false,  // OTimestamp
    false,  // raw
  },
  // lob
  {
    false,  // null
    false,  // int
    false,  // uint
    false,  // float
    false,  // double
    false,  // number
    false,  // datetime
    false,  // date
    false,  // time
    false,  // year
    false,   // string
    false,  // extend
    false,  // unknown
    false,   // lob
    false,   // bit
    false,  // enumset
    false,  // enumsetInner
    false,  // OTimestamp
    false,  // raw
  },
    // bit
  {
    false,  // null
    true,   // int
    true,   // uint
    true,   // float
    true,   // double
    true,   // number
    false,  // datetime
    false,  // date
    false,  // time
    false,  // year
    false,  // string
    false,  // extend
    false,  // unknown
    false,  // lob
    true,   // bit
    true,  // enumset
    false,  // enumsetInner
    false,  // OTimestamp
    false,  // raw
  },
  //enumset
  {
    false,  // null
    true,   // int
    true,   // uint
    true,   // float
    true,   // double
    true,   // number
    false,  // datetime
    false,  // date
    false,  // time
    false,  // year
    false,  // string
    false,  // extend
    false,  // unknown
    false,  // lob
    true,   // bit
    true,  // enumset
    false,  // enumsetInner
    false,  // OTimestamp
    false,  // raw
  },
  //enumsetInner
  {
    false,  // null
    false,   // int
    false,   // uint
    false,   // float
    false,   // double
    false,   // number
    false,  // datetime
    false,  // date
    false,  // time
    false,  // year
    false,  // string
    false,  // extend
    false,  // unknown
    false,  // lob
    false,   // bit
    false,  // enumset
    false,  // enumsetInner
    false,  // OTimestamp
    false,  // raw
  },
  // OTimestamp
  {
    false,  // null
    false,   // int
    false,   // uint
    false,   // float
    false,   // double
    false,   // number
    true,   // datetime
    true,   // date
    true,  // time
    true,   // year
    false,   // string
    false,  // extend
    false,  // unknown
    false,   // lob
    false,   // bit
    false,  // enumset
    false,  // enumsetInner
    true,  // OTimestamp
    false,  // raw
  },
  // raw
  {
    false,  // null
    false,  // int
    false,  // uint
    false,  // float
    false,  // double
    false,  // number
    false,  // datetime
    false,  // date
    false,  // time
    false,  // year
    false,  // string
    false,  // extend
    false,  // unknown
    false,  // lob
    false,  // bit
    false,  // enumset
    false,  // enumsetInner
    false,  // OTimestamp
    false,  // raw
  },
};

const bool ObObjCaster::ORDER_CONSISTENT_WITH_BOTH_STRING[ObObjCaster::VALID_OC_COLLATION_TYPES][ObObjCaster::VALID_OC_COLLATION_TYPES][ObObjCaster::VALID_OC_COLLATION_TYPES] =
{
  //CS_TYPE_UTF8MB4_GENERAL_CI
  {
    //ci    //utf8bin //bin
    {true, true, true},
    {false, false, false},
    {false, false, false},
  },
  //CS_TYPE_UTF8MB4_BIN
  {
    //ci    //utf8bin //bin
    {true, true , true},
    {false, true , true},
    {false, true , true},
  },
  //CS_TYPE_BINARY
  {
    //ci    //utf8bin //bin
    {true, true , true},
    {false, true , true},
    {false, true , true},
  },
};

int ObObjEvaluator::is_true(const ObObj &obj, ObCastMode cast_mode, bool &result)
{
  int ret = OB_SUCCESS;
  result = false;
  if (obj.is_null()) {
    // result is unkown
    // do nothing
  } else if (obj.is_numeric_type()) {
    result = !obj.is_zero();
  } else if (lib::is_oracle_mode() && obj.is_varchar_or_char() && 0 == obj.get_string_len()) {
    result = true;
  } else {
    ObArenaAllocator allocator(ObModIds::BLOCK_ALLOC);
    ObCastCtx cast_ctx(&allocator, NULL, cast_mode, CS_TYPE_INVALID);
    ObObj buf_obj;
    const ObObj *res_obj = NULL;
    if (OB_FAIL(ObObjCaster::to_type(ObNumberType, cast_ctx, obj, buf_obj, res_obj))) {
      LOG_WARN("failed to cast object to tinyint", K(ret), K(obj));
    } else if (NULL == res_obj || (!(res_obj->is_number()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("res_obj is NULL or unexpected obj type", K(ret), K(obj), K(res_obj));
    } else {
      result = !(res_obj->is_zero_number());
    }
  }
  return ret;
}

int ObObjEvaluator::is_false(const ObObj &obj, ObCastMode cast_mode, bool &b_result)
{
  int ret = OB_SUCCESS;
  b_result = true;

  if (OB_FAIL(is_true(obj, cast_mode, b_result))) {
    LOG_WARN("fail to evaluate obj", K(ret));
  } else {
    b_result = !b_result;
  }
  if (OB_SUCC(ret)) {
    if (obj.is_null()) {
      // result is unkown, unkown != false
      b_result = false;
    }
  }

  return ret;
}

ObGeoType ObGeoCastUtils::get_geo_type_from_cast_mode(uint64_t cast_mode)
{
  ObGeoType type = ObGeoType::GEOMETRY;
  if (CM_IS_GEOMETRY_GEOMETRY(cast_mode)) {
    type = ObGeoType::GEOMETRY;
  } else if (CM_IS_GEOMETRY_POINT(cast_mode)) {
    type = ObGeoType::POINT;
  } else if (CM_IS_GEOMETRY_LINESTRING(cast_mode)) {
    type = ObGeoType::LINESTRING;
  } else if (CM_IS_GEOMETRY_POLYGON(cast_mode)) {
    type = ObGeoType::POLYGON;
  } else if (CM_IS_GEOMETRY_MULTIPOINT(cast_mode)) {
    type = ObGeoType::MULTIPOINT;
  } else if (CM_IS_GEOMETRY_MULTILINESTRING(cast_mode)) {
    type = ObGeoType::MULTILINESTRING;
  } else if (CM_IS_GEOMETRY_MULTIPOLYGON(cast_mode)) {
    type = ObGeoType::MULTIPOLYGON;
  } else if (CM_IS_GEOMETRY_GEOMETRYCOLLECTION(cast_mode)) {
    type = ObGeoType::GEOMETRYCOLLECTION;
  }
  return type;
}

int ObGeoCastUtils::set_geo_type_to_cast_mode(ObGeoType geo_type, uint64_t &cast_mode)
{
  int ret = OB_SUCCESS;
  switch (geo_type) {
    case ObGeoType::GEOMETRY: {
      CM_SET_GEOMETRY_GEOMETRY(cast_mode);
      break;
    }
    case ObGeoType::POINT: {
      CM_SET_GEOMETRY_POINT(cast_mode);
      break;
    }
    case ObGeoType::LINESTRING: {
      CM_SET_GEOMETRY_LINESTRING(cast_mode);
      break;
    }
    case ObGeoType::POLYGON: {
      CM_SET_GEOMETRY_POLYGON(cast_mode);
      break;
    }
    case ObGeoType::MULTIPOINT: {
      CM_SET_GEOMETRY_MULTIPOINT(cast_mode);
      break;
    }
    case ObGeoType::MULTILINESTRING: {
      CM_SET_GEOMETRY_MULTILINESTRING(cast_mode);
      break;
    }
    case ObGeoType::MULTIPOLYGON: {
      CM_SET_GEOMETRY_MULTIPOLYGON(cast_mode);
      break;
    }
    case ObGeoType::GEOMETRYCOLLECTION: {
      CM_SET_GEOMETRY_GEOMETRYCOLLECTION(cast_mode);
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid geometry type", K(ret), K(geo_type));
      break;
    }
  }
  return ret;
}

void ObGeoCastUtils::geo_cast_error_handle(int err_code,
                                           ObGeoType src_type,
                                           ObGeoType dst_type,
                                           const ObGeoErrLogInfo &log_info)
{
  if (OB_ERR_INVALID_CAST_TO_GEOMETRY == err_code) {
    LOG_USER_ERROR(OB_ERR_INVALID_CAST_TO_GEOMETRY,
                   ObGeoTypeUtil::get_geo_name_by_type(src_type),
                   ObGeoTypeUtil::get_geo_name_by_type(dst_type));
  } else if (OB_ERR_GEOMETRY_PARAM_LONGITUDE_OUT_OF_RANGE == err_code) {
    LOG_USER_ERROR(OB_ERR_GEOMETRY_PARAM_LONGITUDE_OUT_OF_RANGE,
                   ObGeometryTypeCastUtil::get_cast_name(dst_type),
                   log_info.value_out_of_range_,
                   -180.000000,
                   180.000000);
  } else if (OB_ERR_GEOMETRY_PARAM_LATITUDE_OUT_OF_RANGE == err_code) {
    LOG_USER_ERROR(OB_ERR_GEOMETRY_PARAM_LATITUDE_OUT_OF_RANGE,
                   ObGeometryTypeCastUtil::get_cast_name(dst_type),
                   log_info.value_out_of_range_,
                   -90.000000,
                   90.000000);
  } else if (OB_ERR_GIS_INVALID_DATA == err_code) {
    LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, ObGeometryTypeCastUtil::get_cast_name(dst_type));
  } else if (OB_ERR_INVALID_CAST_POLYGON_RING_DIRECTION == err_code) {
    LOG_USER_ERROR(OB_ERR_INVALID_CAST_POLYGON_RING_DIRECTION,
                   ObGeoTypeUtil::get_geo_name_by_type(src_type),
                   ObGeoTypeUtil::get_geo_name_by_type(dst_type));
  }
}

} // end namespace common
} // end namespace oceanbase
