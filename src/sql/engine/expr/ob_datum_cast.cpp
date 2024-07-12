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
#include "lib/utility/ob_fast_convert.h"
#include "share/object/ob_obj_cast_util.h"
#include "share/object/ob_obj_cast.h"
#include "share/ob_json_access_utils.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/engine/ob_serializable_function.h"
#include "lib/json_type/ob_json_tree.h"
#include "lib/json_type/ob_json_bin.h"
#include "lib/json_type/ob_json_base.h"
#include "lib/json_type/ob_json_parse.h"
#include "lib/roaringbitmap/ob_rb_utils.h"
#include "share/ob_lob_access_utils.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/engine/expr/ob_expr_json_func_helper.h"
#include "lib/geo/ob_geometry_cast.h"
#include "sql/engine/expr/ob_geo_expr_utils.h"
#include "lib/udt/ob_udt_type.h"
#include "sql/engine/expr/ob_expr_sql_udt_utils.h"
#include "lib/xml/ob_xml_util.h"
#include "sql/engine/expr/ob_expr_xml_func_helper.h"
#include "pl/ob_pl.h"
#include "pl/ob_pl_user_type.h"
#ifdef OB_BUILD_ORACLE_PL
#include "pl/sys_package/ob_sdo_geometry.h"
#endif
namespace oceanbase
{
namespace sql
{
using namespace oceanbase::common;
//// common function and macro
#define CAST_FUNC_NAME(intype, outtype)                    \
  int intype##_##outtype(const sql::ObExpr &expr,          \
                              sql::ObEvalCtx &ctx,         \
                              sql::ObDatum &res_datum)

#define CAST_ENUMSET_FUNC_NAME(intype, outtype)            \
  int intype##_##outtype(const sql::ObExpr &expr,          \
                         const common::ObIArray<common::ObString> &str_values,\
                         const uint64_t cast_mode,         \
                         sql::ObEvalCtx &ctx,              \
                         sql::ObDatum &res_datum)

#define DEF_BATCH_CAST_FUNC(in_tc, out_tc)                                                         \
  template <>                                                                                      \
  struct DefinedBatchCast<in_tc, out_tc>                                                           \
  {                                                                                                \
    static constexpr bool value_ = true;                                                           \
  };                                                                                               \
  template <>                                                                                      \
  int ObBatchCast::implicit_batch_cast<in_tc, out_tc>(                                             \
    const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const int64_t batch_size)

#define EVAL_ARG_FOR_CAST_TO_JSON()                                                                   \
  int ret = OB_SUCCESS;                                                                               \
  ObDatum *child_res = NULL;                                                                          \
  ObObjType in_type = expr.args_[0]->datum_meta_.type_;                                               \
  if (OB_FAIL(expr.args_[0]->eval(ctx, child_res))) {                                                 \
    LOG_WARN("eval arg failed", K(ret));                                                              \
  } else if (child_res->is_null()) {                                                                  \
    res_datum.set_null();                                                                             \
  } else if (lib::is_mysql_mode() && CM_IS_COLUMN_CONVERT(expr.extra_) && is_mysql_unsupported_json_column_conversion(in_type)) {  \
    ret = OB_ERR_INVALID_JSON_TEXT;                                                                   \
    LOG_USER_ERROR(OB_ERR_INVALID_JSON_TEXT);                                                         \
  } else

//oracle模式除了longtext类型的空串不等同于null，其他string类型的空串都等于null
#define EVAL_STRING_ARG()                                  \
  int ret = OB_SUCCESS;                                    \
  ObDatum *child_res = NULL;                               \
  if (OB_FAIL(expr.args_[0]->eval(ctx, child_res))) {      \
    LOG_WARN("eval arg failed", K(ret), K(ctx));                   \
  } else if (child_res->is_null() ||                       \
             (lib::is_oracle_mode() && 0 == child_res->len_ \
              && ObLongTextType != expr.args_[0]->datum_meta_.type_)) { \
    res_datum.set_null();                                  \
  } else

#define DEF_IN_OUT_TYPE()                                 \
  int warning = OB_SUCCESS;                               \
  ObObjType in_type = expr.args_[0]->datum_meta_.type_;   \
  ObObjType out_type = expr.datum_meta_.type_;            \
  UNUSEDx(warning, in_type, out_type);

// 如果使用ObDatum::get_xxx()，需要多传递一个参数，指明函数名字，所以下面的宏直接cast
#define DEF_IN_OUT_VAL(in_type, out_type, init_val)                      \
  int warning = OB_SUCCESS;                                              \
  UNUSED(warning);                                                       \
  in_type in_val = *(reinterpret_cast<const in_type*>(child_res->ptr_)); \
  out_type out_val = (init_val);

#define CAST_FAIL(stmt) \
  (OB_UNLIKELY((OB_SUCCESS != (ret = get_cast_ret((expr.extra_), (stmt), warning)))))

// 与CAST_FAIL类似，但是上面的宏会将expr.extra_作为cast_mode,这样使用时少写一个参数
#define CAST_FAIL_CM(stmt, cast_mode) \
  (OB_UNLIKELY((OB_SUCCESS != (ret = get_cast_ret((cast_mode), (stmt), warning)))))

#define GET_SESSION()                                           \
  ObBasicSessionInfo *session = ctx.exec_ctx_.get_my_session(); \
  if (OB_ISNULL(session)) {                                     \
    ret = OB_ERR_UNEXPECTED;                                    \
    LOG_WARN("session is NULL", K(ret));                        \
  } else

#define EVAL_BATCH_ARGS()                                                                          \
  if (OB_FAIL(expr.args_[0]->eval_batch(ctx, skip, batch_size))) {                                 \
    LOG_WARN("eval batch args failed", K(ret));                                                    \
  } else

#define EVAL_ARG()                                                                                 \
  int ret = OB_SUCCESS;                                                                            \
  ObDatum *child_res = NULL;                                                                       \
  if (OB_FAIL(expr.args_[0]->eval(ctx, child_res))) {                                              \
    LOG_WARN("eval arg failed", K(ret), K(ctx));                                                   \
  } else if (child_res->is_null()) {                                                               \
    res_datum.set_null();                                                                          \
  } else

#define DEF_BATCH_CAST_PARAMS                                                                      \
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);                                         \
  ObEvalCtx::TempAllocGuard alloc_guard(ctx);                                                      \
  ObIAllocator &alloc = alloc_guard.get_allocator();                                               \
  ObDatumVector arg_dv = expr.args_[0]->locate_expr_datumvector(ctx);                              \
  ObDatumVector result_dv = expr.locate_expr_datumvector(ctx)

template <ObObjTypeClass in_tc, ObObjTypeClass out_tc>
struct DefinedBatchCast
{
  static constexpr bool value_ = false;
};

#define INT32_MAX_DIGITS_LEN   10
static const int64_t power_of_10[INT32_MAX_DIGITS_LEN] = {
  1L,
  10L,
  100L,
  1000L,
  10000L,
  100000L,
  1000000L,
  10000000L,
  100000000L,
  1000000000L,
//2147483647
};

static OB_INLINE int get_cast_ret(const ObCastMode &cast_mode, int ret, int &warning)
{
  // compatibility for old ob
  if (OB_UNLIKELY(OB_ERR_UNEXPECTED_TZ_TRANSITION == ret) ||
      OB_UNLIKELY(OB_ERR_UNKNOWN_TIME_ZONE == ret)) {
    ret = OB_INVALID_DATE_VALUE;
  } else if (OB_SUCCESS != ret && CM_IS_WARN_ON_FAIL(cast_mode)) {
    warning = ret;
    ret = OB_SUCCESS;
  }
  return ret;
}

// 出现错误时,处理如下：
// 如果只设置了WARN_ON_FAIL，会覆盖错误码
// 如果设置了WARN_ON_FAIL和ZERO_ON_WARN,会覆盖错误码，且结果被置为0
// 如果设置了WARN_ON_FAIL和NULL_ON_WARN,会覆盖错误码，且结果被置为null
#define SET_RES_OBJ(cast_mode, func_val, zero_value, value)       \
  do {                                                            \
    if (OB_SUCC(ret)) {                                           \
      if (OB_SUCCESS == warning                                   \
          || OB_ERR_TRUNCATED_WRONG_VALUE == warning              \
          || OB_DATA_OUT_OF_RANGE == warning                      \
          || OB_ERR_DATA_TRUNCATED == warning                     \
          || OB_ERR_DOUBLE_TRUNCATED == warning                   \
          || OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD == warning) { \
        res_datum.set_##func_val(value);                          \
      } else if (CM_IS_ZERO_ON_WARN(cast_mode)) {                 \
        res_datum.set_##func_val(zero_value);                     \
      } else {                                                    \
        res_datum.set_null();                                     \
      }                                                           \
    } else {                                                      \
      res_datum.set_##func_val(value);                            \
    }                                                             \
  } while (0)

// 用于设置timestamp nano以及timestamp ltz的宏
#define SET_RES_OTIMESTAMP_10BYTE(value)                          \
  do {                                                            \
    if (OB_SUCC(ret)) {                                           \
           if (OB_SUCCESS == warning                              \
          || OB_ERR_TRUNCATED_WRONG_VALUE == warning              \
          || OB_DATA_OUT_OF_RANGE == warning                      \
          || OB_ERR_DATA_TRUNCATED == warning                     \
          || OB_ERR_DOUBLE_TRUNCATED == warning                   \
          || OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD == warning) { \
        res_datum.set_otimestamp_tiny(value);                     \
      } else if (CM_IS_ZERO_ON_WARN(expr.extra_)) {               \
        res_datum.set_otimestamp_tiny(ObOTimestampData());        \
      } else {                                                    \
        res_datum.set_null();                                     \
      }                                                           \
    } else {                                                      \
      res_datum.set_otimestamp_tiny(value);                       \
    }                                                             \
  } while (0)

#define SET_RES_INT(value)         \
  SET_RES_OBJ(expr.extra_, int, 0, value)
#define SET_RES_BIT(value)         \
  SET_RES_OBJ(expr.extra_, uint, 0, value)
#define SET_RES_UINT(value)        \
  SET_RES_OBJ(expr.extra_, uint,0, value)
#define SET_RES_DATE(value)        \
  SET_RES_OBJ(expr.extra_, date, ObTimeConverter::ZERO_DATE, value)
#define SET_RES_TIME(value)        \
  SET_RES_OBJ(expr.extra_, time, ObTimeConverter::ZERO_TIME, value)
#define SET_RES_YEAR(value)        \
  SET_RES_OBJ(expr.extra_, year, ObTimeConverter::ZERO_YEAR, value)
#define SET_RES_FLOAT(value)       \
  SET_RES_OBJ(expr.extra_, float,  0.0, value)
#define SET_RES_DOUBLE(value)      \
  SET_RES_OBJ(expr.extra_, double, 0.0, value)
#define SET_RES_ENUM(value)        \
  SET_RES_OBJ(expr.extra_, enum, 0, value)
#define SET_RES_SET(value)        \
  SET_RES_OBJ(expr.extra_, set, 0, value)
#define SET_RES_OTIMESTAMP(value)  \
  SET_RES_OBJ(expr.extra_, otimestamp_tz, ObOTimestampData(),   value)
#define SET_RES_INTERVAL_YM(value) \
  SET_RES_OBJ(expr.extra_, interval_nmonth, 0, value)
#define SET_RES_INTERVAL_DS(value) \
  SET_RES_OBJ(expr.extra_, interval_ds, ObIntervalDSValue(), value)
#define SET_RES_DATETIME(value)    \
  SET_RES_OBJ(expr.extra_, datetime,ObTimeConverter::ZERO_DATETIME, value)

static const int64_t MAX_DOUBLE_STRICT_PRINT_SIZE = 512;


static OB_INLINE int serialize_obnumber(number::ObNumber &nmb,
                                        ObIAllocator &allocator,
                                        ObString &out_str)
{
  int ret = OB_SUCCESS;
  const number::ObNumber::Desc &nmb_desc = nmb.get_desc();
  uint32_t *digits = nmb.get_digits();
  char *buf = NULL;
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

int ObDatumHexUtils::hex(const ObExpr &expr, const ObString &in_str, ObEvalCtx &ctx,
                         ObIAllocator &calc_alloc, ObDatum &res_datum, bool upper_case)
{
  int ret = OB_SUCCESS;
  if (in_str.empty()) {
    if (lib::is_oracle_mode()) {
      res_datum.set_null();
    } else {
      if (!ob_is_text_tc(expr.datum_meta_.type_)) {
        res_datum.set_string(NULL, 0);
      } else if (OB_FAIL(ObTextStringHelper::string_to_templob_result(expr, ctx, res_datum, in_str))) {
        // build temp lob;
        LOG_WARN("build empty lob failed", K(ret), K(in_str));
      }
    }
  } else {
    char *buf = NULL;
    bool need_convert = false;
    ObCollationType def_cs = ObCharset::get_system_collation();
    ObCollationType dst_cs = expr.datum_meta_.cs_type_;
    ObExprStrResAlloc res_alloc(expr, ctx);
    // check if need convert first, and setup alloc. we can avoid copy converted res str.
    if (ObExprUtil::need_convert_string_collation(def_cs, dst_cs, need_convert)) {
      LOG_WARN("check need convert cs type failed", K(ret), K(def_cs), K(dst_cs));
    } else if (OB_SUCC(ret)) {
      const int32_t alloc_length = in_str.length() * 2;
      int64_t buf_len = 0;
      ObTextStringDatumResult output_result(expr.datum_meta_.type_, &expr, &ctx, &res_datum);
      if (need_convert) {
        buf = reinterpret_cast<char*>(calc_alloc.alloc(alloc_length));
      } else {
        if (OB_FAIL(output_result.init(alloc_length))) {
          LOG_WARN("init text or string result failed", K(ret), K(alloc_length));
        } else if (OB_FAIL(output_result.get_reserved_buffer(buf, buf_len))) {
          LOG_WARN("get reserved buffer for text or string failed", K(ret), K(alloc_length), K(buf_len));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(buf)) {
        res_datum.set_null();
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc memory failed", K(ret), K(alloc_length));
      } else {
        const char *HEXCHARS = upper_case ? "0123456789ABCDEF" : "0123456789abcdef";
        int32_t pos = 0;
        for (int32_t i = 0; i < in_str.length(); ++i) {
          buf[pos++] = HEXCHARS[in_str[i] >> 4 & 0xF];
          buf[pos++] = HEXCHARS[in_str[i] & 0xF];
        }
        ObString res_str;
        if (need_convert) {
          static const int32_t CharConvertFactorNum = 4;
          const int32_t alloc_length = pos * CharConvertFactorNum; //最多使用4字节存储一个字符
          char *res_buf = NULL;
          if (output_result.is_init()) {
            OB_ASSERT(0); // should not inited if need_convert
          } else if (OB_FAIL(output_result.init(alloc_length))) {
            LOG_WARN("init text or string result failed", K(ret), K(alloc_length));
          } else if (OB_FAIL(output_result.get_reserved_buffer(res_buf, buf_len))) {
            LOG_WARN("get reserved buffer of text or string result failed",
              K(ret), K(alloc_length), K(buf_len));
          } else if (OB_ISNULL(buf)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("alloc memory failed", K(ret), K(alloc_length));
          } else {
            ObDataBuffer data_buf(res_buf, buf_len);
            if (OB_FAIL(ObExprUtil::convert_string_collation(ObString(pos, buf), def_cs,
                                                                      res_str, dst_cs, data_buf))) {
              LOG_WARN("convert string collation failed", K(ret));
            } else if (OB_FAIL(output_result.lseek(res_str.length(), 0))) {
              LOG_WARN("lseek text or string result failed", K(ret), K(res_str.length()));
            } else {
              output_result.set_result();
            }
          }
        } else {
          if (OB_FAIL(output_result.lseek(pos, 0))) {
            LOG_WARN("lseek text or string result failed", K(ret), K(pos));
          } else {
            output_result.set_result();
          }
        }
      }
    }
  }
  return ret;
}

static OB_INLINE int common_construct_otimestamp(const ObObjType type,
                                                 const ObDatum &in_datum,
                                                 ObOTimestampData &out_val);
int ObDatumHexUtils::rawtohex(const ObExpr &expr, const ObString &in_str,
             ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObObjType in_type = expr.args_[0]->datum_meta_.type_;
  if (0 >= in_str.length()) {
    res_datum.set_null();
  } else if (!(ObNullType < in_type && in_type < ObMaxType)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid in type", K(ret), K(in_type));
  } else {
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    ObIAllocator &tmp_alloc = alloc_guard.get_allocator();
    ObString out_str;
    ObOTimestampData time_in_val;
    switch (in_type) {
      //TODO::this should same as oracle, and support dump func @yanhua
      case ObTinyIntType:
      case ObSmallIntType:
      case ObInt32Type:
      case ObIntType: {
        // 虽然tiny/small/int32/int实际占用空间不到个字节,
        // 但是CG阶段还是会为其分配8个字节的空间
        // 所以这里reinterpret_cast为int64_t的指针是没问题的
        int64_t in_val = *(reinterpret_cast<const int64_t*>(in_str.ptr()));
        number::ObNumber nmb;
        if (OB_FAIL(nmb.from(in_val, tmp_alloc))) {
          LOG_WARN("fail to int_number", K(ret), K(in_val), "type", in_type);
        } else if (OB_FAIL(serialize_obnumber(nmb, tmp_alloc, out_str))) {
          LOG_WARN("serialize_obnumber failed", K(ret), K(nmb));
        }
        break;
      }
      case ObFloatType: {
        float in_val = *(reinterpret_cast<const float*>(in_str.ptr()));
        ObPrecision res_precision = -1;
        ObScale res_scale = -1;
        char buf[MAX_DOUBLE_STRICT_PRINT_SIZE];
        MEMSET(buf, 0, MAX_DOUBLE_STRICT_PRINT_SIZE);
        int64_t length = ob_gcvt_opt(in_val, OB_GCVT_ARG_FLOAT, static_cast<int32_t>(sizeof(buf) - 1),
                                     buf, NULL, TRUE, TRUE);
        ObString float_str(sizeof(buf), static_cast<int32_t>(length), buf);
        number::ObNumber nmb;
        if (OB_FAIL(nmb.from_sci_opt(float_str.ptr(), float_str.length(),
                                     tmp_alloc, &res_precision, &res_scale))) {
          LOG_WARN("fail to from str to number", K(ret), K(float_str));
        } else if (OB_FAIL(serialize_obnumber(nmb, tmp_alloc, out_str))) {
          LOG_WARN("serialize_obnumber failed", K(ret), K(nmb));
        }
        break;
      }
      case ObDoubleType: {
        double in_val = *(reinterpret_cast<const double*>(in_str.ptr()));
        ObPrecision res_precision = -1;
        ObScale res_scale = -1;
        char buf[MAX_DOUBLE_STRICT_PRINT_SIZE];
        MEMSET(buf, 0, MAX_DOUBLE_STRICT_PRINT_SIZE);
        int64_t length = ob_gcvt_opt(in_val, OB_GCVT_ARG_DOUBLE, static_cast<int32_t>(sizeof(buf) - 1),
                                     buf, NULL, TRUE, TRUE);
        ObString double_str(sizeof(buf), static_cast<int32_t>(length), buf);
        number::ObNumber nmb;
        if (OB_FAIL(nmb.from_sci_opt(double_str.ptr(), double_str.length(),
                                     tmp_alloc, &res_precision, &res_scale))) {
          LOG_WARN("fail to from str to number", K(ret), K(double_str));
        }  else if (OB_FAIL(serialize_obnumber(nmb, tmp_alloc, out_str))) {
          LOG_WARN("serialize_obnumber failed", K(ret), K(nmb));
        }
        break;
      }
      case ObNumberFloatType:
      case ObNumberType: {
        // 参数传进来的in_str是从子节点的datum空间来的，里面是已经序列化好的ObNumber
        // 直接拷贝即可
        out_str = in_str;
        break;
      }
      case ObDecimalIntType: {
        const ObDecimalInt *in_val = (reinterpret_cast<const ObDecimalInt*>(in_str.ptr()));
        const int32_t int_bytes = in_str.length();
        number::ObNumber nmb;
        if (OB_FAIL(wide::to_number(in_val, int_bytes,
                                    expr.args_[0]->datum_meta_.scale_, tmp_alloc, nmb))) {
          LOG_WARN("to_number failed", K(ret), K(int_bytes), K(expr.args_[0]->datum_meta_.scale_));
        } else if (OB_FAIL(serialize_obnumber(nmb, tmp_alloc, out_str))) {
          LOG_WARN("serialize_obnumber failed", K(ret), K(nmb));
        }
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
        //https://www.techonthenet.com/oracle/functions/rawtohex.php
        //NOTE:: when convert string to raw, Oracle use utl_raw.cast_to_raw(),
        //       while PL/SQL use hextoraw(), here we use utl_raw.cast_to_raw(),
        //       as we can not distinguish in which SQL
        out_str = in_str;
        break;
      }
      case ObTimestampTZType:
      case ObTimestampLTZType:
      case ObTimestampNanoType: {
        // ObTimestampTZType占用12字节，ObTimestampLTZType和ObTimestampNanoType占用10字节
        // 所以需要进行区分
        ObDatum tmp_datum;
        tmp_datum.ptr_ = in_str.ptr();
        tmp_datum.pack_ = static_cast<uint32_t>(in_str.length());
        if (OB_FAIL(common_construct_otimestamp(in_type, tmp_datum, time_in_val))) {
        LOG_WARN("common_construct_otimestamp failed", K(ret));
        } else {
          out_str.assign_ptr(reinterpret_cast<char *>(&time_in_val),
              static_cast<int32_t>(in_str.length()));
        }
        break;
      }
      case ObGeometryType:
      case ObRoaringBitmapType: {
        ObString lob_data = in_str;
        if (OB_FAIL(ObTextStringHelper::read_real_string_data(&tmp_alloc, in_type,
            expr.args_[0]->obj_meta_.has_lob_header(), lob_data, &ctx.exec_ctx_))) {
          LOG_WARN("fail to get real data.", K(ret), K(lob_data));
        } else {
          out_str = lob_data;
        }
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

int ObDatumHexUtils::hextoraw_string(const ObExpr &expr,
                              const ObString &in_str,
                              ObEvalCtx &ctx,
                              ObDatum &res_datum,
                              bool &has_set_res)
{
  int ret = OB_SUCCESS;
  if (0 == in_str.length()) {
    res_datum.set_null();
  } else if (OB_FAIL(unhex(expr, in_str, ctx, res_datum, has_set_res))) {
    LOG_WARN("unhex failed", K(ret), K(in_str), K(ctx));
  }
  return ret;
}

static int common_json_bin(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum, ObString &raw_bin)
{
  int ret = OB_SUCCESS;
  ObTextStringDatumResult text_result(ObJsonType, &expr, &ctx, &res_datum);
  if (OB_FAIL(text_result.init(raw_bin.length()))) {
    LOG_WARN("Lob: init lob result failed");
  } else if (OB_FAIL(text_result.append(raw_bin.ptr(), raw_bin.length()))) {
    LOG_WARN("failed to append realdata", K(ret), K(raw_bin), K(text_result));
  } else {
    text_result.set_result();
  }
  return ret;
}

static int common_gis_wkb(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum, ObString &wkb, bool skip_ver = false)
{
  int ret = OB_SUCCESS;
  ObTextStringDatumResult text_result(ObGeometryType, &expr, &ctx, &res_datum);
  uint32_t len = skip_ver ? wkb.length() - WKB_VERSION_SIZE : wkb.length();
  if (OB_FAIL(text_result.init(len))) {
    LOG_WARN("Lob: init lob result failed");
  } else if (skip_ver) {
    if (OB_FAIL(text_result.append(wkb.ptr(), WKB_GEO_SRID_SIZE))) {
      LOG_WARN("failed to append realdata", K(ret), K(wkb), K(text_result));
    } else if (OB_FAIL(text_result.append(wkb.ptr() + WKB_OFFSET, len - WKB_GEO_SRID_SIZE))) {
      LOG_WARN("failed to append realdata", K(ret), K(wkb), K(text_result));
    }
  } else if (OB_FAIL(text_result.append(wkb.ptr(), wkb.length()))) {
    LOG_WARN("failed to append realdata", K(ret), K(wkb), K(text_result));
  }
  if (OB_SUCC(ret)) {
    text_result.set_result();
  }
  return ret;
}

static int common_copy_string(const ObExpr &expr, const ObString &src, ObEvalCtx &ctx,
                       ObDatum &res_datum, const int64_t align_offset = 0);
int ObDatumHexUtils::hextoraw(const ObExpr &expr, const ObDatum &in,
                              const ObDatumMeta &in_datum_meta,
                              ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  if (ob_is_text_tc(in_datum_meta.get_type())) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("lob as param not support in hextoraw", K(ret), K(in_datum_meta.get_type()));
  } else if (in.is_null()) {
    res.set_null();
  } else if (ob_is_numeric_type(in_datum_meta.get_type())) {
    number::ObNumber res_nmb;
    ObNumStackOnceAlloc tmp_alloc;
    if (OB_FAIL(get_uint(in_datum_meta.get_type(), in_datum_meta.scale_, in, tmp_alloc, res_nmb))) {
      LOG_WARN("fail to get uint64", K(ret));
    } else if (OB_FAIL(uint_to_raw(res_nmb, expr, ctx, res))) {
      LOG_WARN("fail to convert to hex", K(ret), K(res_nmb));
    }
  } else if (ob_is_character_type(in_datum_meta.get_type(), in_datum_meta.cs_type_) ||
             ob_is_varbinary_or_binary(in_datum_meta.get_type(), in_datum_meta.cs_type_)) {
    const ObString &in_str = in.get_string();
    bool has_set_res = false;
    if (OB_FAIL(hextoraw_string(expr, in_str, ctx, res, has_set_res))) {
      LOG_WARN("hextoraw_string failed", K(ret), K(in_str), K(ctx));
    }
  } else if (ob_is_raw(in_datum_meta.get_type())) {
    ObString in_str(in.get_string());
    if (OB_FAIL(common_copy_string(expr, in_str, ctx, res))) {
      LOG_WARN("common_copy_string failed", K(ret), K(in_str), K(ctx));
    }
  } else {
    ret = OB_ERR_INVALID_HEX_NUMBER;
    LOG_WARN("invalid hex number", K(ret), K(in_datum_meta.get_type()));
  }
  return ret;
}

int ObDatumHexUtils::uint_to_raw(const number::ObNumber &uint_num, const ObExpr &expr,
                           ObEvalCtx &ctx, ObDatum &res_datum)
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
    bool has_set_res = false;
    if (OB_FAIL(unhex(expr, uint_str, ctx, res_datum, has_set_res))) {
      LOG_WARN("fail to str_to_raw", K(ret), K(uint_str));
    }
  }
  return ret;
}

int ObDatumHexUtils::get_uint(
    const ObObjType &in_type, const int8_t scale, const ObDatum &in, ObIAllocator &alloc,
    number::ObNumber &out)
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
    }
  } else if (ObDecimalIntType == in_type) {
    if (OB_FAIL(wide::to_number(in.get_decimal_int(), in.get_int_bytes(), scale, alloc, out))) {
      LOG_WARN("to_number failed", K(ret));
    } else if (OB_UNLIKELY(!out.is_integer()) || OB_UNLIKELY(out.is_negative())) {
      ret = OB_ERR_INVALID_HEX_NUMBER;
      LOG_WARN("invalid hex number", K(ret), K(out));
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

// 相比于common_copy_string_zf，不考虑zerofill。直接将src中的内容拷贝到res_datum
// %align_offset is used in mysql mode to align blob to other charset.
static int common_copy_string(const ObExpr &expr,
                              const ObString &src,
                              ObEvalCtx &ctx,
                              ObDatum &res_datum,
                              const int64_t align_offset /* = 0 */)
{
  int ret = OB_SUCCESS;
  char *out_ptr = NULL;
  int64_t len = align_offset + src.length();
  if (OB_ISNULL(out_ptr = expr.get_str_res_mem(ctx, len))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else {
    MEMMOVE(out_ptr + align_offset, src.ptr(), len - align_offset);
    MEMSET(out_ptr, 0, align_offset);
    res_datum.set_string(out_ptr, len);
  }
  return ret;
}

// 将src中的内容拷贝到res_datum中，拷贝过程需要考虑res_datum空间是否足够，以及zero fill填0
// 参数expr的作用是为了获取cast表达式的相关信息，例如res_buf_len_/max_length_/extra_
static int common_copy_string_zf(const ObExpr &expr,
                              const ObString &src,
                              ObEvalCtx &ctx,
                              ObDatum &res_datum,
                              const int64_t align_offset = 0)
{
  int ret = OB_SUCCESS;
  int64_t out_len = static_cast<uint8_t>(expr.datum_meta_.scale_);
  if (out_len <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected zf length", K(ret), K(out_len), K(expr.datum_meta_.scale_));
  //这里原来做了对oracle模式空串对处理，将oracle模式空串转为null。
  //一般情况下oracle模式空串等同于null，但是支持empty_lob之后，longtext/lob类型的空串不等同于null
  // 其他类型转到string类型时会调用common_copy_string_zf，当in_type不是string时，in_str的length不会为0
  // in_type是string时，在cast方法入口已经判断了是否要将空串转换为null，因此删去原来的转换逻辑。
  } else if (CM_IS_ZERO_FILL(expr.extra_) && out_len > src.length()) {
    char *out_ptr = NULL;
    // out_ptr may overlap with src, so memmove is used.
    if (OB_ISNULL(out_ptr = expr.get_str_res_mem(ctx, out_len))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else {
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
      LOG_WARN("common_copy_string failed", K(ret), K(src), K(expr), K(ctx));
    }
  }
  return ret;
}

int ObDatumHexUtils::unhex(const ObExpr &expr,
                           const ObString &in_str,
                           ObEvalCtx &ctx,
                           ObDatum &res_datum,
                           bool &has_set_res)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  char *buf = NULL;
  int64_t buf_len = 0;
  const bool need_fill_zero = (1 == in_str.length() % 2);
  const int32_t tmp_length = in_str.length() / 2 + need_fill_zero;
  int32_t alloc_length = (0 == tmp_length ? 1 : tmp_length);
  ObTextStringDatumResult output_result(expr.datum_meta_.type_, &expr, &ctx, &res_datum);
  if (OB_FAIL(output_result.init(alloc_length))) {
    LOG_WARN("init text or string result failed", K(ret), K(alloc_length));
  } else if (OB_FAIL(output_result.get_reserved_buffer(buf, buf_len))) {
    LOG_WARN("get reserved buffer of text or string result failed",
      K(ret), K(alloc_length), K(buf_len));
  } else if (OB_ISNULL(buf)) {
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
      if (OB_FAIL(output_result.lseek(tmp_length, 0))) {
        LOG_WARN("lseek text or string result failed", K(ret), K(tmp_length));
      } else {
        output_result.set_result();
        has_set_res = true;
      }
    }
  }
  return ret;
}

// 根据in_type,force_use_standard_format信息，从session中获取fromat_str
int common_get_nls_format(const ObBasicSessionInfo *session,
                          ObEvalCtx &ctx,
                          const ObExpr *rt_expr,
                          const ObObjType in_type,
                          const bool force_use_standard_format,
                          ObString &format_str)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session) || OB_ISNULL(rt_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("session or rt_expr is NULL", K(ret), K(session), KP(rt_expr));
  } else {
    ObString nls_format;
    ObSessionSysVar *local_var = NULL;
    ObSolidifiedVarsGetter helper(*rt_expr, ctx, session);
    switch (in_type) {
      case ObDateTimeType:
        if (OB_FAIL(helper.get_local_nls_date_format(nls_format))) {
          LOG_WARN("get nls timestamp tz format failed", K(ret));
        } else {
          format_str = (force_use_standard_format
              ? ObTimeConverter::COMPAT_OLD_NLS_DATE_FORMAT
              : (nls_format.empty()
                ? ObTimeConverter::DEFAULT_NLS_DATE_FORMAT
                : nls_format));
        }
        break;
      case ObTimestampNanoType:
      case ObTimestampLTZType:
        if (OB_FAIL(helper.get_local_nls_timestamp_format(nls_format))) {
          LOG_WARN("get nls timestamp tz format failed", K(ret));
        } else {
          format_str = (force_use_standard_format
              ? ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_FORMAT
              : (nls_format.empty()
                ? ObTimeConverter::DEFAULT_NLS_TIMESTAMP_FORMAT
                : nls_format));
        }
        break;
      case ObTimestampTZType:
        if (OB_FAIL(helper.get_local_nls_timestamp_tz_format(nls_format))) {
          LOG_WARN("get nls timestamp tz format failed", K(ret));
        } else {
          format_str = (force_use_standard_format
              ? ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_TZ_FORMAT
              : (nls_format.empty()
                ? ObTimeConverter::DEFAULT_NLS_TIMESTAMP_TZ_FORMAT
                : nls_format));
        }
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected in_type", K(in_type), K(ret));
        break;
    }
  }
  return ret;
}

static int common_int_datetime(const ObExpr &expr,
                               const int64_t in_val,
                               ObEvalCtx &ctx,
                               ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  GET_SESSION()
  {
    int64_t out_val = 0;
    int warning = OB_SUCCESS;
    if (0 > in_val) {
      ret = OB_INVALID_DATE_FORMAT;
    } else {
      ObDateSqlMode date_sql_mode;
      date_sql_mode.allow_invalid_dates_ = CM_IS_EXPLICIT_CAST(expr.extra_)
                                           ? false : CM_IS_ALLOW_INVALID_DATES(expr.extra_);
      date_sql_mode.no_zero_date_ = CM_IS_EXPLICIT_CAST(expr.extra_)
                                    ? false : CM_IS_NO_ZERO_DATE(expr.extra_);
      const common::ObTimeZoneInfo *tz_info_local = NULL;
      ObSolidifiedVarsGetter helper(expr, ctx, session);
      if (OB_FAIL(helper.get_time_zone_info(tz_info_local))) {
        LOG_WARN("get time zone info failed", K(ret));
      } else {
        ObTimeConvertCtx cvrt_ctx(tz_info_local,
                                  ObTimestampType == expr.datum_meta_.type_);
        ret = ObTimeConverter::int_to_datetime(in_val, 0, cvrt_ctx, out_val, date_sql_mode);
      }
    }
    if (CAST_FAIL(ret)) {
      LOG_WARN("int_datetime failed", K(ret));
    } else {
      SET_RES_DATETIME(out_val);
    }
  }
  return ret;
}

static OB_INLINE int common_int_number(const ObExpr &expr,
                                       int64_t in_val,
                                       ObIAllocator &alloc,
                                       number::ObNumber &nmb)
{
  int ret = OB_SUCCESS;
  int warning = OB_SUCCESS;
  ObObjType out_type = expr.datum_meta_.type_;
  if ((ObUNumberType == out_type) && CAST_FAIL(numeric_negative_check(in_val))) {
    LOG_WARN("numeric_negative_check faield", K(ret), K(in_val));
  } else if (OB_FAIL(nmb.from(in_val, alloc))) {
    LOG_WARN("nmb.from failed", K(ret), K(in_val));
  }
  return ret;
}

static OB_INLINE int common_int_date(const ObExpr &expr,
                           const int64_t in_val,
                           ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  int warning = OB_SUCCESS;
  int32_t out_val = 0;
  ObDateSqlMode date_sql_mode;
  date_sql_mode.allow_invalid_dates_ = CM_IS_EXPLICIT_CAST(expr.extra_)
                                       ? false : CM_IS_ALLOW_INVALID_DATES(expr.extra_);
  date_sql_mode.no_zero_date_ = CM_IS_EXPLICIT_CAST(expr.extra_)
                                ? false : CM_IS_NO_ZERO_DATE(expr.extra_);
  if (CAST_FAIL(ObTimeConverter::int_to_date(in_val, out_val, date_sql_mode))) {
    LOG_WARN("int_to_date failed", K(ret), K(in_val), K(out_val));
  } else {
    SET_RES_DATE(out_val);
  }
  return ret;
}

static OB_INLINE int common_int_time(const ObExpr &expr,
                           const int64_t in_val,
                           ObDatum &res_datum)
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

static OB_INLINE int common_int_year(const ObExpr &expr,
                           const int64_t in_val,
                           ObDatum &res_datum,
                           int &warning)
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

static OB_INLINE int common_int_year(const ObExpr &expr,
                           const int64_t in_val,
                           ObDatum &res_datum)
{
  int warning = OB_SUCCESS;
  return common_int_year(expr, in_val, res_datum, warning);
}

static OB_INLINE int common_uint_int(const ObExpr &expr,
                                     const ObObjType &out_type,
                                     uint64_t in_val,
                                     ObEvalCtx &ctx,
                                     int64_t &out_val)
{
  int ret = OB_SUCCESS;
  int warning = OB_SUCCESS;
  out_val = static_cast<int64_t>(in_val);
  UNUSED(ctx);
  if (CM_NEED_RANGE_CHECK(expr.extra_) &&
      CAST_FAIL(int_upper_check(out_type, in_val, out_val))) {
    LOG_WARN("int_upper_check failed", K(ret), K(in_val), K(ctx));
  }
  return ret;
}

static int common_string_int(const ObExpr &expr,
                             const uint64_t &extra,
                             const ObString &in_str,
                             const bool is_str_integer_cast,
                             ObDatum &res_datum,
                             int &warning)
{
  int ret = OB_SUCCESS;
  ObObjType in_type = expr.args_[0]->datum_meta_.type_;
  ObCollationType in_cs_type = expr.args_[0]->datum_meta_.cs_type_;
  ObObjType out_type = expr.datum_meta_.type_;
  int64_t out_val = 0;
  ret = common_string_integer(extra, in_type, in_cs_type, in_str, is_str_integer_cast, out_val);
  if (CAST_FAIL_CM(ret, extra)) {
    LOG_WARN("string_int failed", K(ret), K(in_str));
  } else if (out_type < ObIntType &&
      CAST_FAIL_CM(int_range_check(out_type, out_val, out_val), extra)) {
    LOG_WARN("int_range_check failed", K(ret));
  } else {
    SET_RES_INT(out_val);
  }
  return ret;
}

static int common_string_int(const ObExpr &expr,
                             const uint64_t &extra,
                             const ObString &in_str,
                             const bool is_str_integer_cast,
                             ObDatum &res_datum)
{
  int warning = OB_SUCCESS;
  return common_string_int(expr, extra, in_str, is_str_integer_cast, res_datum, warning);
}

static int common_string_uint(const ObExpr &expr,
                              const ObString &in_str,
                              const bool is_str_integer_cast,
                              ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  uint64_t out_val = 0;
  DEF_IN_OUT_TYPE();
  ret = common_string_unsigned_integer(expr.extra_, in_type, expr.args_[0]->datum_meta_.cs_type_, in_str, is_str_integer_cast, out_val);
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

int common_string_double(const ObExpr &expr,
                         const ObObjType &in_type,
                         const ObCollationType &in_cs_type,
                         const ObObjType &out_type,
                         const ObString &in_str,
                         ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  int warning = OB_SUCCESS;
  double out_val = 0.0;
  if (ObHexStringType == in_type) {
    out_val = static_cast<double>(hex_to_uint64(in_str));
  } else {
    int err = 0;
    char *endptr = NULL;
    out_val = ObCharset::strntodv2(in_str.ptr(), in_str.length(), &endptr, &err);
    if (EOVERFLOW == err && (-DBL_MAX == out_val || DBL_MAX == out_val)) {
      ret = OB_DATA_OUT_OF_RANGE;
    } else {
      ObString tmp_str = in_str;
      ObString trimed_str = tmp_str.trim();
      if (lib::is_mysql_mode() && 0 == trimed_str.length()) {
        if (!CM_IS_COLUMN_CONVERT(expr.extra_)) {
          // mysql 模式下不在 convert_column 里遇到空字符串或者全是空格的字符串转 double 时，不报错
          // skip
        } else {
          ret = OB_ERR_DOUBLE_TRUNCATED;
          LOG_WARN("convert string to double failed", K(ret), K(in_str));
        }
      } else if (OB_FAIL(check_convert_str_err(in_str.ptr(), endptr, in_str.length(), err, in_cs_type))) {
        LOG_WARN("failed to check_convert_str_err", K(ret), K(in_str), K(out_val), K(err), K(in_cs_type));
        if (lib::is_mysql_mode() && CM_IS_COLUMN_CONVERT(expr.extra_) && ret == OB_ERR_DATA_TRUNCATED) {
          // do nothing, compatible mysql, retain OB_ERR_DATA_TRUNCATED error code in column_convert.
        } else {
          ret = OB_ERR_DOUBLE_TRUNCATED;
        }
        if (CM_IS_WARN_ON_FAIL(expr.extra_)) {
          ret = OB_ERR_DOUBLE_TRUNCATED;
          LOG_USER_WARN(OB_ERR_DOUBLE_TRUNCATED, in_str.length(), in_str.ptr());
        }
      }
    }
  }

  if (CAST_FAIL(ret)) {
    LOG_WARN("string_double failed", K(ret));
  } else if (ObUDoubleType == out_type &&
             CAST_FAIL(numeric_negative_check(out_val))) {
    LOG_WARN("numeric_negative_check failed", K(ret), K(out_val));
  } else {
    SET_RES_DOUBLE(out_val);
  }
  LOG_DEBUG("common_string_double", K(ret), K(warning), K(out_val), K(in_str));
  return ret;
}

static OB_INLINE int common_double_float(const ObExpr &expr,
                                         const double in_val,
                                         float &out_val)
{
  int ret = OB_SUCCESS;
  int warning = OB_SUCCESS;
  out_val = static_cast<float>(in_val);
  ObObjType out_type = expr.datum_meta_.type_;
  // oracle support float/double infiniy, no need to verify data overflow.
  // C language would cast value to infinity, which is correct behavor in oracle mode
  if (lib::is_mysql_mode()) {
    double truncated_val = in_val;
    if (ob_is_float_tc(out_type) && CM_IS_COLUMN_CONVERT(expr.extra_)) {
      // truncate float value if its ps information is fixed.
      ObAccuracy accuracy(expr.datum_meta_.precision_, expr.datum_meta_.scale_);
      if (CAST_FAIL(real_range_check(accuracy, truncated_val))) {
        LOG_WARN("fail to real range check", K(ret));
      } else {
        out_val = static_cast<float>(truncated_val);
      }
    }
    if (OB_SUCC(ret) && CAST_FAIL(real_range_check(out_type, truncated_val, out_val))) {
      LOG_WARN("real_range_check failed", K(ret), K(in_val));
    }
  }
  return ret;
}

static OB_INLINE int common_string_float(const ObExpr &expr,
                               const ObString &in_str,
                               float &out_val)
{
  int ret = OB_SUCCESS;
  double tmp_double = 0.0;
  ObDatum tmp_datum;
  tmp_datum.ptr_ = reinterpret_cast<const char*>(&tmp_double);
  tmp_datum.pack_ = sizeof(double);
  DEF_IN_OUT_TYPE();
  if (OB_FAIL(common_string_double(expr, in_type, expr.args_[0]->datum_meta_.cs_type_, out_type, in_str, tmp_datum))) {
    LOG_WARN("common_string_double failed", K(ret), K(in_str));
  } else if (OB_FAIL(common_double_float(expr, tmp_double, out_val))) {
    LOG_WARN("common_double_float failed", K(ret), K(tmp_double));
  }
  return ret;
}

static OB_INLINE int common_string_date(const ObExpr &expr,
                                        const ObString &in_str,
                                        ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  int warning = OB_SUCCESS;
  int32_t out_val = 0;
  ObDateSqlMode date_sql_mode;
  date_sql_mode.allow_invalid_dates_ = CM_IS_ALLOW_INVALID_DATES(expr.extra_);
  date_sql_mode.no_zero_date_ = CM_IS_NO_ZERO_DATE(expr.extra_);
  if (CAST_FAIL(ObTimeConverter::str_to_date(in_str, out_val, date_sql_mode))) {
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

static OB_INLINE int common_string_time(const ObExpr &expr,
                                        const ObString &in_str,
                                        ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  int warning = OB_SUCCESS;
  int64_t out_val = 0;
  ObScale res_scale; // useless
  // support sqlmode TIME_TRUNCATE_FRACTIONAL
  const ObCastMode cast_mode = expr.extra_;
  bool need_truncate = CM_IS_COLUMN_CONVERT(cast_mode) ? CM_IS_TIME_TRUNCATE_FRACTIONAL(cast_mode) : false;
  if (CAST_FAIL(ObTimeConverter::str_to_time(in_str, out_val, &res_scale, need_truncate))) {
    LOG_WARN("str_to_time failed", K(ret), K(in_str));
  } else {
    SET_RES_TIME(out_val);
  }
  return ret;
}

static OB_INLINE int common_string_year(const ObExpr &expr,
                                        const ObString &in_str,
                                        ObDatum &res_datum)
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
      UNUSED(CAST_FAIL(tmp_warning));
    }
  }
  return ret;
}

static OB_INLINE int common_string_number(const ObExpr &expr,
                                          const ObString &in_str,
                                          ObIAllocator &alloc,
                                          number::ObNumber &nmb)
{
  int ret = OB_SUCCESS;
  const ObCastMode cast_mode = expr.extra_;
  DEF_IN_OUT_TYPE();
  if (ObHexStringType == in_type) {
    ret = nmb.from(hex_to_uint64(in_str), alloc);
  } else if (0 == in_str.length()) {
    // in mysql mode, this err will be ignored(because default cast_mode is WARN_ON_FAIL)
    nmb.set_zero();
    if (lib::is_oracle_mode() || CM_IS_COLUMN_CONVERT(cast_mode)) {
      ret = OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD;
    }
  } else {
    ObPrecision res_precision; // useless
    ObScale res_scale;
    ret = nmb.from_sci_opt(in_str.ptr(), in_str.length(), alloc,
                           &res_precision, &res_scale);
    // bug: 4263211. 兼容mysql string转number超过最值域范围的行为
    // select cast('1e500' as decimal);  -> max_val
    // select cast('-1e500' as decimal); -> min_val
    if (OB_NUMERIC_OVERFLOW == ret) {
      int64_t i = 0;
      while (i < in_str.length() && isspace(in_str[i])) {
        ++i;
      }
      bool is_neg = (in_str[i] == '-');
      int tmp_ret = OB_SUCCESS;
      const ObAccuracy &def_acc = ObAccuracy::DDL_DEFAULT_ACCURACY2[0][out_type];
      const ObPrecision prec = def_acc.get_precision();
      const ObScale scale = def_acc.get_scale();
      const number::ObNumber *bound_num = NULL;
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

  if (CAST_FAIL(ret)) {
    LOG_WARN("string_number failed", K(ret), K(in_type), K(out_type), K(cast_mode), K(in_str));
  } else if (ObUNumberType == out_type && CAST_FAIL(numeric_negative_check(nmb))) {
    LOG_WARN("numeric_negative_check failed", K(ret), K(in_type), K(cast_mode), K(in_str));
  }
  return ret;
}

static int common_string_decimalint(const ObExpr &expr, const ObString &in_str,
                                    const ObUserLoggingCtx *user_logging_ctx,
                                    ObDecimalIntBuilder &res_val)
{// TODO: add cases
#define SET_ZERO(int_type)                                                                         \
  int_type v = 0;                                                                                  \
  res_val.from(v);                                                                                 \
  break

  int ret = OB_SUCCESS;
  ObObjType in_type = expr.args_[0]->datum_meta_.get_type();
  int16_t in_scale = 0, in_precision = 0;
  ObScale out_scale = expr.datum_meta_.scale_;
  ObPrecision out_prec = expr.datum_meta_.precision_;
  ObDecimalIntBuilder tmp_alloc;
  ObDecimalInt *decint = nullptr;
  int32_t int_bytes = 0;
  // set default value
  switch (get_decimalint_type(out_prec)) {
  case common::DECIMAL_INT_32: {
    SET_ZERO(int32_t);
  }
  case common::DECIMAL_INT_64: {
    SET_ZERO(int64_t);
  }
  case common::DECIMAL_INT_128: {
    SET_ZERO(int128_t);
  }
  case common::DECIMAL_INT_256: {
    SET_ZERO(int256_t);
  }
  case common::DECIMAL_INT_512: {
    SET_ZERO(int512_t);
  }
  default:
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected precision", K(out_prec));
  }
  if (OB_FAIL(ret)) {
  } else {
    if (ObHexStringType == in_type) {
      uint64_t in_val = hex_to_uint64(in_str);
      in_precision = ob_fast_digits10(in_val);
      if (OB_FAIL(wide::from_integer(in_val, tmp_alloc, decint, int_bytes, in_precision))) {
        LOG_WARN("from integer failed", K(in_val), K(ret));
      } else {
        in_scale = 0;
      }
    } else if (0 == in_str.length()) {
      ret = OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD;
    } else if (OB_FAIL(wide::from_string(in_str.ptr(), in_str.length(), tmp_alloc, in_scale,
                                         in_precision, int_bytes, decint))) {
      LOG_WARN("failed to parse string", K(ret));
      if (OB_NUMERIC_OVERFLOW == ret && lib::is_mysql_mode()) {
        // bug: 4263211. compatible with mysql behavior when value overflows type range.
        // select cast('1e500' as decimal);  -> max_val
        // select cast('-1e500' as decimal); -> min_val
        int64_t i = 0;
        while (i < in_str.length() && isspace(in_str[i])) { ++i; }
        bool is_neg = (in_str[i] == '-');
        const ObDecimalInt *limit_decint = nullptr;
        if (is_neg) {
          limit_decint = wide::ObDecimalIntConstValue::get_min_value(out_prec);
          int_bytes = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(out_prec);
        } else {
          limit_decint = wide::ObDecimalIntConstValue::get_max_value(out_prec);
          int_bytes = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(out_prec);
        }
        in_scale = out_scale;
        in_precision = out_prec;
        if (OB_ISNULL(limit_decint)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null decimal int", K(ret));
        } else if (OB_ISNULL(decint = (ObDecimalInt *)tmp_alloc.alloc(int_bytes))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(ret));
        } else {
          MEMCPY(decint, limit_decint, int_bytes);
        }
      }
    }
    int warning = ret;
    ret = OB_SUCCESS;
    if (decint != nullptr && int_bytes != 0) {
      // Decimal int not null means a valid decimal int was parsed regardless of wether there's
      // error or not.We then do scale and calculate res_datum as normal in order to be compatible
      // with mysql.
      // e.g.
      //  OceanBase(root@test)>set sql_mode = '';
      //  Query OK, 0 rows affected (0.00 sec)
      //
      //  OceanBase(root@test)>insert into t2 values ('1ab');
      //  Query OK, 1 row affected (0.00 sec)
      //
      //  OceanBase(root@test)>select * from t2;
      //  +-------+
      //  | a     |
      //  +-------+
      //  | 1.000 |
      //  +-------+
      //  1 row in set (0.01 sec)
      if (ObDatumCast::need_scale_decimalint(in_scale, in_precision, out_scale, out_prec)) {
        if (OB_FAIL(ObDatumCast::common_scale_decimalint(decint, int_bytes, in_scale, out_scale,
                                                         out_prec, expr.extra_, res_val,
                                                         user_logging_ctx))) {
          LOG_WARN("scale decimal int failed", K(ret));
        }
      } else {
        res_val.from(decint, int_bytes);
      }
    }
    if (OB_SUCC(ret)) {
      const ObCastMode cast_mode = expr.extra_;
      if (CAST_FAIL(warning)) {
        LOG_WARN("string_decimalint failed", K(ret), K(in_type), K(cast_mode), K(in_str));
      }
    }
  }
  return ret;
#undef SET_ZERO
}

static OB_INLINE int common_string_datetime(const ObExpr &expr,
                                            const ObString &in_str,
                                            ObEvalCtx &ctx,
                                            ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  int warning = OB_SUCCESS;
  int64_t out_val = 0;
  GET_SESSION()
  {
    const ObCastMode cast_mode = expr.extra_;
    bool need_truncate = CM_IS_COLUMN_CONVERT(cast_mode) ? CM_IS_TIME_TRUNCATE_FRACTIONAL(cast_mode) : false;
    const common::ObTimeZoneInfo *tz_info_local = NULL;
    ObSolidifiedVarsGetter helper(expr, ctx, session);
    if (OB_FAIL(helper.get_time_zone_info(tz_info_local))) {
      LOG_WARN("get time zone info failed", K(ret));
    } else {
      ObTimeConvertCtx cvrt_ctx(tz_info_local,
                                ObTimestampType == expr.datum_meta_.type_,
                                need_truncate);
      if (lib::is_oracle_mode()) {
        if (OB_FAIL(common_get_nls_format(session, ctx, &expr, expr.datum_meta_.type_,
                                          CM_IS_FORCE_USE_STANDARD_NLS_FORMAT(expr.extra_),
                                          cvrt_ctx.oracle_nls_format_))) {
          LOG_WARN("common_get_nls_format failed", K(ret));
        } else if (CAST_FAIL(ObTimeConverter::str_to_date_oracle(in_str,
                                                                cvrt_ctx,
                                                                out_val))) {
          LOG_WARN("str_to_date_oracle failed", K(ret));
        }
      } else {
        ObScale res_scale; // useless
        ObDateSqlMode date_sql_mode;
        date_sql_mode.allow_invalid_dates_ = CM_IS_ALLOW_INVALID_DATES(expr.extra_);
        date_sql_mode.no_zero_date_ = CM_IS_NO_ZERO_DATE(expr.extra_);
        if (CAST_FAIL(ObTimeConverter::str_to_datetime(in_str, cvrt_ctx, out_val, &res_scale,
                      date_sql_mode))) {
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
      }
    }
    if (OB_SUCC(ret)) {
      SET_RES_DATETIME(out_val);
    }
  }
  return ret;
}

static OB_INLINE int common_get_bit_len(const ObString &str, int32_t &bit_len)
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

static int common_string_bit(const ObExpr &expr,
                             const ObString &in_str,
                             ObEvalCtx &ctx,
                             ObDatum &res_datum)
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
      LOG_WARN("bit type length is too long", K(ret), K(in_str),
                                              K(OB_MAX_BIT_LENGTH), K(bit_len));
    } else {
      //将str按照二进制值转换为相应的uint64
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

// lob类型和其他类型互相转换时，会把longtext类型作为中间类型，并且调用一些原有的longtext相关的转换函数
// 这些函数把转换结果放到了get_str_res_mem, 也就是string类型的表达式存放结果的地方。
// 因此添加了下面这个函数，把结果先从get_str_res_mem拷贝到get_reset_tmp_alloc空间中。
int copy_datum_str_with_tmp_alloc(ObEvalCtx &ctx, ObDatum &res_datum, ObString &res_str)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard alloc_guard(ctx);
  ObIAllocator &calc_alloc = alloc_guard.get_allocator();
  char *tmp_res = NULL;
  const ObString str = res_datum.get_string();
  if (0 == str.length()) {
    res_str.assign_ptr(NULL, 0);
  } else if (OB_ISNULL(tmp_res = static_cast<char *>(calc_alloc.alloc(str.length())))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else {
    MEMMOVE(tmp_res, str.ptr(), str.length());
    res_str.assign_ptr(tmp_res, str.length());
  }
  return ret;
}

int common_check_convert_string(const ObExpr &expr,
                                ObEvalCtx &ctx,
                                const ObString &in_str,
                                ObDatum &res_datum,
                                bool &has_set_res)
{
  int ret = OB_SUCCESS;
  ObObjType out_type = expr.datum_meta_.type_;
  ObCollationType out_cs_type = expr.datum_meta_.cs_type_;
  ObObjType in_type = expr.args_[0]->datum_meta_.type_;
  ObCollationType in_cs_type = expr.args_[0]->datum_meta_.cs_type_;
  if (lib::is_oracle_mode() &&
      (ob_is_blob(out_type, out_cs_type) || ob_is_blob_locator(out_type, out_cs_type)) &&
      !(ob_is_blob(in_type, in_cs_type) || ob_is_blob_locator(in_type, in_cs_type)
        || ob_is_raw(in_type))) {
    // !blob -> blob
    if (ObCharType == in_type || ObVarcharType == in_type) {
      if (OB_FAIL(ObDatumHexUtils::hextoraw_string(expr, in_str, ctx, res_datum, has_set_res))) {
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
    const ObCharsetInfo *cs = NULL;
    int64_t align_offset = 0;
    if (CS_TYPE_BINARY == in_cs_type && lib::is_mysql_mode()
        && (NULL != (cs = ObCharset::get_charset(out_cs_type)))) {
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

static int common_string_string(const ObExpr &expr,
                                const ObObjType in_type,
                                const ObCollationType in_cs_type,
                                const ObObjType out_type,
                                const ObCollationType out_cs_type,
                                const ObString &in_str,
                                ObEvalCtx &ctx,
                                ObDatum &res_datum,
                                bool& has_set_res)
{
  int ret = OB_SUCCESS;
  if (lib::is_oracle_mode()
      && ob_is_clob(in_type, in_cs_type)
      && (0 == in_str.length())
      && !ob_is_clob(out_type, out_cs_type)) {
    // oracle 模式下的 empty_clob 被 cast 成其他类型时结果是 NULL
    res_datum.set_null();
  } else if (CS_TYPE_BINARY != in_cs_type &&
      CS_TYPE_BINARY != out_cs_type &&
      (ObCharset::charset_type_by_coll(in_cs_type) !=
      ObCharset::charset_type_by_coll(out_cs_type))) {
    // handle !blob->!blob
    char *buf = NULL;
    //latin1 1bytes,utf8mb4 4bytes,the factor should be 4
    int64_t buf_len = in_str.length() * ObCharset::CharConvertFactorNum;
    uint32_t result_len = 0;
    buf = expr.get_str_res_mem(ctx, buf_len);
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed", K(ret));
    } else if (OB_FAIL(ObCharset::charset_convert(in_cs_type, in_str.ptr(),
                                                  in_str.length(), out_cs_type, buf,
                                                  buf_len, result_len, lib::is_mysql_mode(),
                                                  !CM_IS_IGNORE_CHARSET_CONVERT_ERR(expr.extra_) && CM_IS_IMPLICIT_CAST(expr.extra_),
                                                  ObCharset::is_cs_unicode(out_cs_type) ? 0xFFFD : '?'))) {
      LOG_WARN("charset convert failed", K(ret));
    } else {
      res_datum.set_string(buf, result_len);
    }
  } else {
    if (CS_TYPE_BINARY == in_cs_type || CS_TYPE_BINARY == out_cs_type) {
      // just copy string when in_cs_type or out_cs_type is binary
      if (OB_FAIL(common_check_convert_string(expr, ctx, in_str, res_datum, has_set_res))) {
        LOG_WARN("fail to common_check_convert_string", K(ret), K(in_str));
      }
    } else if (lib::is_oracle_mode()
                && ob_is_clob(in_type, in_cs_type)) {
      res_datum.set_string(in_str.ptr(), in_str.length());
    } else if (lib::is_oracle_mode()
                && ob_is_clob(out_type, out_cs_type)) {
      res_datum.set_string(in_str.ptr(), in_str.length());
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("same charset should not be here, just use cast_eval_arg", K(ret),
          K(in_type), K(out_type), K(in_cs_type), K(out_cs_type));
    }
  }
  LOG_DEBUG("string_string cast", K(ret), K(in_str),
              K(ObString(res_datum.len_, res_datum.ptr_)));
  return ret;
}

static int common_string_otimestamp(const ObExpr &expr,
                                    const ObString &in_str,
                                    ObEvalCtx &ctx,
                                    ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  GET_SESSION()
  {
    int warning = OB_SUCCESS;
    ObOTimestampData out_val;
    ObScale res_scale = 0; // useless
    const common::ObTimeZoneInfo *tz_info_local = NULL;
    ObSolidifiedVarsGetter helper(expr, ctx, session);
    if (OB_FAIL(helper.get_time_zone_info(tz_info_local))) {
      LOG_WARN("get time zone info failed", K(ret));
    } else {
      ObTimeConvertCtx cvrt_ctx(tz_info_local, true);
      if (OB_FAIL(common_get_nls_format(session,
              ctx,
              &expr,
              expr.datum_meta_.type_,
              CM_IS_FORCE_USE_STANDARD_NLS_FORMAT(expr.extra_),
              cvrt_ctx.oracle_nls_format_))) {
        LOG_WARN("common_get_nls_format failed", K(ret));
      } else if (CAST_FAIL(ObTimeConverter::str_to_otimestamp(in_str, cvrt_ctx,
              expr.datum_meta_.type_,
              out_val, res_scale))) {
        LOG_WARN("str_to_otimestamp failed", K(ret), K(in_str));
      } else {
        if (ObTimestampTZType == expr.datum_meta_.type_) {
          SET_RES_OTIMESTAMP(out_val);
        } else {
          SET_RES_OTIMESTAMP_10BYTE(out_val);
        }
      }
    }
  }
  return ret;
}

static int common_string_interval(const ObExpr &expr,
                                  const ObString &in_str,
                                  ObEvalCtx &ctx,
                                  ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  int warning = OB_SUCCESS;
  ObObjType out_type = expr.datum_meta_.type_;
  if (ObIntervalYMType == out_type) {
    ObIntervalYMValue out_val;
    //don't know the scale, use the max scale and return the real scale
    ObScale res_scale = ObAccuracy::MAX_ACCURACY2[ORACLE_MODE][ObIntervalYMType].get_scale();
    if (CAST_FAIL(ObTimeConverter::str_to_interval_ym(in_str, out_val, res_scale))) {
      LOG_WARN("str_to_interval_ym failed", K(ret), K(in_str));
    } else {
      SET_RES_INTERVAL_YM(out_val.nmonth_);
    }
  } else {
    ObIntervalDSValue out_val;
    //don't know the scale, use the max scale and return the real scale
    ObScale res_scale = ObAccuracy::MAX_ACCURACY2[ORACLE_MODE][ObIntervalDSType].get_scale();
    if (CAST_FAIL(ObTimeConverter::str_to_interval_ds(in_str, out_val, res_scale))) {
      LOG_WARN("str_to_interval_ds failed", K(ret), K(in_str));
    } else {
      SET_RES_INTERVAL_DS(out_val);
    }
  }
  return ret;
}

static int common_string_rowid(const ObExpr &expr,
                              const ObString &base64_str,
                              ObEvalCtx &ctx,
                              ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObURowIDData urowid_data;
  ObExprStrResAlloc alloc(expr, ctx);
  // TODO(lihongqin.lhq): distinguish between rowid and urowid.
  if (OB_FAIL(ObURowIDData::decode2urowid(base64_str.ptr(), base64_str.length(),
                                          alloc, urowid_data))) {
    LOG_WARN("failed to decode to urowid", K(ret));
  } else {
    res_datum.set_urowid(urowid_data);
  }
  return ret;
}

static int common_string_lob(const ObExpr &expr,
                              const ObString &in_str,
                              ObEvalCtx &ctx,
                              const ObLobLocator *lob_locator,
                              ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObObjType in_type = expr.args_[0]->datum_meta_.type_;
  ObObjType out_type = ObLongTextType;
  ObCollationType in_cs_type = expr.args_[0]->datum_meta_.cs_type_;
  ObCollationType out_cs_type = expr.datum_meta_.cs_type_;
  bool has_set_res = false;
  if (OB_FAIL(common_string_string(expr, in_type, in_cs_type, out_type,
                                    out_cs_type, in_str, ctx, res_datum, has_set_res))) {
    LOG_WARN("fail to cast string to longtext", K(ret), K(in_str), K(expr));
  } else if (res_datum.is_null()) {
    // do nothing
  } else {
    ObString res_str;
    if (OB_FAIL(copy_datum_str_with_tmp_alloc(ctx, res_datum, res_str))) {
      LOG_WARN("copy datum str with tmp alloc", K(ret));
    } else {
      char *buf = nullptr;
      ObLobLocator *result = nullptr;
      const int64_t buf_len = sizeof(ObLobLocator)
                              + (NULL == lob_locator ? 0 : lob_locator->payload_offset_)
                              + res_str.length();
      if (OB_ISNULL(buf = expr.get_str_res_mem(ctx, buf_len))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Failed to allocate memory for lob locator", K(ret), K(buf_len));
      } else if (FALSE_IT(result = reinterpret_cast<ObLobLocator *> (buf))) {
      } else if (NULL == lob_locator || lob_locator->is_fake_locator()) {
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
                                        lob_locator->flags_, rowid, res_str))) {
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

static int get_text_full_data(const sql::ObExpr &expr,
                              sql::ObEvalCtx &ctx,
                              ObIAllocator *allocator,
                              ObDatum* in_datum,
                              ObString &data)
{
  int ret = OB_SUCCESS;
  ObObjType in_type = expr.args_[0]->datum_meta_.type_;
  ObCollationType in_cs_type = expr.args_[0]->datum_meta_.cs_type_;
  if (ob_is_text_tc(in_type)) {
    bool has_lob_header = expr.args_[0]->obj_meta_.has_lob_header();
    ObTextStringIter instr_iter(in_type, in_cs_type, in_datum->get_string(), has_lob_header);
    if (OB_FAIL(ObTextStringHelper::build_text_iter(instr_iter, &ctx.exec_ctx_, ctx.exec_ctx_.get_my_session(), allocator))) {
      LOG_WARN("Lob: init lob str iter failed", K(ret), K(in_type), K(*in_datum));
    } else if (OB_FAIL(instr_iter.get_full_data(data))) {
      LOG_WARN("Lob: get lob str iter full data failed ", K(ret), K(in_type), K(*in_datum));
    } else {/* do nothing */}
  }
  return ret;
}

static int common_copy_string_to_text_result(const ObExpr &expr,
                                             const ObString &src,
                                             ObEvalCtx &ctx,
                                             ObDatum &res_datum,
                                             const int64_t align_offset = 0)
{
  int ret = OB_SUCCESS;
  char *out_ptr = NULL;
  int64_t len = align_offset + src.length();
  ObTextStringDatumResult str_result(expr.datum_meta_.type_, &expr, &ctx, &res_datum);
  if (OB_FAIL(str_result.init(len))) {
    LOG_WARN("Lob: init lob result failed");
  } else if (align_offset > 0) {
    if (OB_FAIL(str_result.fill(0, 0, align_offset))) {
    } else if (OB_FAIL(str_result.lseek(align_offset, 0))) {
    } else { /* do nothing */ };
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(str_result.append(src.ptr(), len - align_offset))) {
    LOG_WARN("Lob: append data to temp lob failed");
  } else {
    str_result.set_result(); // fillzero is padding before numbers, not padding after，e.g. 0050
  }
  return ret;
}

static int common_copy_string_zf_to_text_result(const ObExpr &expr,
                                                const ObString &src,
                                                ObEvalCtx &ctx,
                                                ObDatum &res_datum,
                                                const int64_t align_offset = 0)
{
  int ret = OB_SUCCESS;
  int64_t out_len = static_cast<uint8_t>(expr.datum_meta_.scale_);
  if (out_len <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Lob: unexpected zf length", K(ret), K(out_len), K(expr.datum_meta_.scale_));
  } else if (CM_IS_ZERO_FILL(expr.extra_) && out_len > src.length()) {
    ObTextStringDatumResult str_result(expr.datum_meta_.type_, &expr, &ctx, &res_datum);
    if (OB_FAIL(str_result.init(out_len))) {
      LOG_WARN("Lob: init lob result failed");
    } else {
      int64_t zf_len = out_len - src.length();
      if (0 < zf_len) {
        if (OB_FAIL(str_result.fill(0, '0', zf_len))) {
        } else if (OB_FAIL(str_result.lseek(zf_len, 0))) {
        } else { /* do nothing */ };
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(str_result.append(src.ptr(), src.length()))) {
        LOG_WARN("Lob: append data to temp lob failed");
      } else {
        str_result.set_result();
      }
    }
  } else {
    if (OB_FAIL(common_copy_string_to_text_result(expr, src, ctx, res_datum, align_offset))) {
      LOG_WARN("Lob: common_copy_string_zf_to_text_result failed", K(ret), K(src), K(expr), K(ctx));
    }
  }
  return ret;
}

static void string_lob_debug(ObObjType in_type, ObCollationType in_cs, bool has_lob_header,
    ObObjType out_type, ObCollationType out_cs,
    ObString &res_str, ObDatum &res_datum, int in_ret)
{
  int ret = OB_SUCCESS;
  if (!ob_enable_datum_cast_debug_log()) {
  } else if (OB_FAIL(in_ret)) {
    LOG_WARN("Lob: string to lob failed", K(in_type), K(out_type), K(in_ret));
  } else if (res_datum.get_string().length() == 0) {
    LOG_WARN("Lob: empty result in string to lob",
      K(in_type), K(out_type), K(res_str), K(res_datum.get_string()));
  } else {
    ObString tmp;
    ObString lobinrow_data;
    tmp.assign_ptr(res_datum.get_string().ptr(), res_datum.get_string().length());
    ObLobLocatorV2 loc(tmp, has_lob_header);
    loc.get_inrow_data(lobinrow_data);
    LOG_WARN("Lob: string to lob", K(in_type), K(out_type), K(res_str), K(lobinrow_data));
  }
}

static void lob_string_debug(ObObjType in_type, ObCollationType in_cs,
    ObObjType out_type, ObCollationType out_cs,
    ObString &in_string, ObDatum &res_datum, int in_ret)
{
  int ret = OB_SUCCESS;
  if (!ob_enable_datum_cast_debug_log()) {
  } else if (OB_FAIL(in_ret)) {
    LOG_WARN("Lob: string to lob failed", K(in_type), K(out_type), K(in_ret));
  } else if (in_string.length() == 0) {
    LOG_WARN("Lob: empty result in string to lob",
      K(in_type), K(out_type), K(in_string), K(res_datum.get_string()));
  } else {
    LOG_WARN("Lob: string to lob", K(in_type), K(out_type), K(res_datum.get_string()), K(in_string));
  }
}

static int common_string_text(const ObExpr &expr,
                              const ObString &in_str,
                              ObEvalCtx &ctx,
                              const ObLobLocatorV2 *lob_locator,
                              ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObObjType in_type = expr.args_[0]->datum_meta_.type_;
  ObObjType out_type = expr.datum_meta_.type_; // ObLongTextType
  ObCollationType in_cs_type = expr.args_[0]->datum_meta_.cs_type_;
  ObCollationType out_cs_type = expr.datum_meta_.cs_type_;
  ObString res_str = in_str;
  bool is_final_res = false;
  bool is_different_charset_type = (ObCharset::charset_type_by_coll(in_cs_type)
                                    != ObCharset::charset_type_by_coll(out_cs_type));
  OB_ASSERT(ob_is_text_tc(out_type));
  if (is_different_charset_type) {
    if (OB_FAIL(common_string_string(expr, in_type, in_cs_type, out_type,
                                     out_cs_type, in_str, ctx, res_datum, is_final_res))) {
      LOG_WARN("Lob: fail to cast string to longtext", K(ret), K(in_str), K(expr));
    } else if (res_datum.is_null()) {
      // only for blob cast to other types in oracle mode, in/out type/collation type must be different.
      is_final_res = true;
    } else if (is_final_res) {
      // is_final_res = true; // hex to text
    } else if (OB_FAIL(copy_datum_str_with_tmp_alloc(ctx, res_datum, res_str))) {
      LOG_WARN("Lob: copy datum str with tmp alloc", K(ret));
    } else { /* do nothing */ }
  }

  if (OB_SUCC(ret) && !is_final_res) {
    ObTextStringDatumResult str_result(expr.datum_meta_.type_, &expr, &ctx, &res_datum);
    if (lob_locator == NULL) {
      if (OB_FAIL(str_result.init(res_str.length()))) {
        LOG_WARN("Lob: init lob result failed");
      } else if (OB_FAIL(str_result.append(res_str.ptr(), res_str.length()))) {
        LOG_WARN("Lob: append lob result failed");
      } else { /* do nothing */ }
    } else if (OB_FAIL(str_result.copy(lob_locator))) {
      LOG_WARN("Lob: copy lob result failed");
    } else { /* do nothing*/ }
    str_result.set_result();
  }

  string_lob_debug(in_type, in_cs_type, expr.obj_meta_.has_lob_header(), out_type, out_cs_type, res_str, res_datum, ret);
  return ret;
}

static int common_uint_bit(const ObExpr &expr,
                           const uint64_t &in_value,
                           ObEvalCtx &ctx,
                           ObDatum &res_datum)
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

static OB_INLINE int common_number_uint(const ObExpr &expr,
                                        const ObDatum &child_res,
                                        ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  const number::ObNumber nmb(child_res.get_number());
  const char *nmb_buf = nmb.format();
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

static OB_INLINE int common_number_string(const ObExpr &expr,
                                          const ObDatum &child_res,
                                          ObEvalCtx &ctx,
                                          ObDatum &res_datum)
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
    bool has_set_res = false;
    if (OB_FAIL(common_check_convert_string(expr, ctx, in_str, res_datum, has_set_res))) {
      LOG_WARN("fail to common_check_convert_string", K(ret), K(in_str));
    }
  }
  return ret;
}

static OB_INLINE int common_decimalint_string(const ObExpr &expr, const ObDatum &child_res,
                                              ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  char buffer[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
  int64_t pos = 0;
  ObScale in_scale = expr.args_[0]->datum_meta_.scale_;
  if (lib::is_oracle_mode() && CM_IS_FORMAT_NUMBER_WITH_LIMIT(expr.extra_)) {
    bool need_to_sci = true;
    if (OB_FAIL(wide::to_string(child_res.get_decimal_int(), child_res.get_int_bytes(), in_scale,
                                buffer, sizeof(buffer), pos, need_to_sci))) {
      LOG_WARN("to_string failed", K(ret));
    }
  } else if (OB_FAIL(wide::to_string(child_res.get_decimal_int(), child_res.get_int_bytes(),
                                     in_scale, buffer, sizeof(buffer), pos))) {
    LOG_WARN("to_string failed", K(ret));
  }
  if (OB_SUCC(ret)) {
    ObString in_str(pos, buffer);
    bool has_set_res = false;
    if (OB_FAIL(common_check_convert_string(expr, ctx, in_str, res_datum, has_set_res))) {
      LOG_WARN("common_check_convert_string failed", K(ret), K(in_str));
    }
  }
  return ret;
}

template <typename T>
static int scale_down_decimalint(const T &x, unsigned scale, ObDecimalIntBuilder &res,
                                 const ObCastMode cm, bool &has_extra_decimals)
{
  static const int64_t pows[5] = {10, 100, 10000, 100000000, 10000000000000000};
  int ret = OB_SUCCESS;
  T result = x;
  bool is_neg = (x < 0);
  if (is_neg) {
    result = -result;
  }
  T remain;
  while (scale != 0 && result != 0) {
    for (int i = ARRAYSIZEOF(pows) - 1; scale != 0 && result != 0 && i >= 0; i--) {
      if (scale & (1 << i)) {
        if (!has_extra_decimals) {
          remain = result % pows[i];
          has_extra_decimals = (remain > 0);
        }
        result = result / pows[i];
        scale -= (1<<i);
      }
    }
    if (scale != 0) {
      if (!has_extra_decimals) {
        remain = result % 10;
        has_extra_decimals = (remain > 0);
      }
      result = result / 10;
      scale -= 1;
    }
  }
  if (is_neg) {
    result = -result;
  }
  if (has_extra_decimals) {
    if ((cm & CM_CONST_TO_DECIMAL_INT_UP) != 0) {
      if (!is_neg) { result = result + 1; }
    } else if ((cm & CM_CONST_TO_DECIMAL_INT_DOWN) != 0) {
      if (is_neg) { result = result - 1; }
    }
  }
  res.from(result);
  return ret;
}

int ObDatumCast::align_decint_precision_unsafe(const ObDecimalInt *decint, const int32_t int_bytes,
                                               const int32_t expected_int_bytes,
                                               ObDecimalIntBuilder &res)
{
  int ret = OB_SUCCESS;
  res.from(decint, int_bytes);
  if (int_bytes > expected_int_bytes) {
    res.truncate(expected_int_bytes);
  } else if (int_bytes < expected_int_bytes) {
    res.extend(expected_int_bytes);
  } else {
    // do nothing
  }
  return ret;
}


static int scale_const_decimalint_expr(const ObDecimalInt *decint, const int32_t int_bytes,
                                       const ObScale in_scale, const ObScale out_scale,
                                       const ObPrecision out_prec, const ObCastMode cast_mode,
                                       ObDecimalIntBuilder &res)
{
#define DO_SCALE(int_type)                                                                         \
  const int_type *v = reinterpret_cast<const int_type *>(decint);                                  \
  if (in_scale < out_scale) {                                                                      \
    ret = wide::scale_up_decimalint(*v, out_scale - in_scale, res);                                \
  } else if (OB_FAIL(scale_down_decimalint(*v, in_scale - out_scale, res,                          \
                                           cast_mode, has_extra_decimal))) {                       \
    LOG_WARN("scale down decimal int failed", K(ret));                                             \
  }

  int ret = OB_SUCCESS;
  bool has_extra_decimal = false;
  ObDecimalIntBuilder max_v, min_v;
  int32_t expected_int_bytes =
        wide::ObDecimalIntConstValue::get_int_bytes_by_precision(out_prec);
  min_v.from(wide::ObDecimalIntConstValue::get_min_lower(out_prec), expected_int_bytes);
  max_v.from(wide::ObDecimalIntConstValue::get_max_upper(out_prec), expected_int_bytes);
 if (in_scale != out_scale) {
    DISPATCH_WIDTH_TASK(int_bytes, DO_SCALE);
  } else {
    res.from(decint, int_bytes);
  }
  if (OB_FAIL(ret)) {
  } else if ((cast_mode & CM_CONST_TO_DECIMAL_INT_EQ) != 0 && has_extra_decimal) {
    res.from(max_v);
  } else {
    int cmp_max = 0, cmp_min = 0;
    if (OB_FAIL(wide::compare(res, min_v, cmp_min))) {
      LOG_WARN("compare failed", K(ret));
    } else if (OB_FAIL(wide::compare(res, max_v, cmp_max))) {
      LOG_WARN("compare failed", K(ret));
    } else if (cmp_max < 0 && cmp_min > 0) { // max(P, S) >= res >= min(P, S)
      if (expected_int_bytes > res.get_int_bytes()) {
        res.extend(expected_int_bytes);
      } else if (expected_int_bytes < res.get_int_bytes()) {
        res.truncate(expected_int_bytes);
      }
    } else if (cmp_max >= 0) {
      res.from(max_v);
    } else if (cmp_min <= 0) {
      res.from(min_v);
    }
  }

  return ret;
#undef DO_SCALE
}

int ObDatumCast::common_scale_decimalint(const ObDecimalInt *decint, const int32_t int_bytes,
                                         const ObScale in_scale, const ObScale out_scale,
                                         const ObPrecision out_prec, const ObCastMode cast_mode,
                                         ObDecimalIntBuilder &val,
                                         const ObUserLoggingCtx *user_logging_ctx)
{
  int ret = OB_SUCCESS;
  ObDecimalIntBuilder max_v, min_v;
  ObDecimalIntBuilder scaled_val;
  int cmp_min = 0, cmp_max = 0;
  if (OB_ISNULL(decint)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid null decimal int", K(ret), K(decint));
  } else if (CM_IS_CONST_TO_DECIMAL_INT(cast_mode)) {
    ret = scale_const_decimalint_expr(decint, int_bytes, in_scale, out_scale, out_prec, cast_mode, val);
  } else if (CM_IS_COLUMN_CONVERT(cast_mode) || CM_IS_EXPLICIT_CAST(cast_mode)) {
    int32_t check_int_bytes = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(out_prec);
    max_v.from(wide::ObDecimalIntConstValue::get_max_upper(out_prec), check_int_bytes);
    min_v.from(wide::ObDecimalIntConstValue::get_min_lower(out_prec), check_int_bytes);
    if (OB_FAIL(
          wide::common_scale_decimalint(decint, int_bytes, in_scale, out_scale, scaled_val))) {
      LOG_WARN("scale decimal int failed", K(ret));
    } else if (OB_FAIL(wide::compare(scaled_val, min_v, cmp_min))) {
      LOG_WARN("compare failed", K(ret));
    } else if (OB_FAIL(wide::compare(scaled_val, max_v, cmp_max))) {
      LOG_WARN("compare failed", K(ret));
    } else if (cmp_min > 0 && cmp_max < 0) {
      ret = ObDatumCast::align_decint_precision_unsafe(scaled_val.get_decimal_int(), scaled_val.get_int_bytes(),
                                          check_int_bytes, val);
    } else if (cmp_min <= 0) {
      val.from(min_v);
    } else if (cmp_max >= 0) {
      val.from(max_v);
    } else {
      // do nothing
    }
    if (OB_SUCC(ret)) {
      if (lib::is_mysql_mode() && CM_IS_COLUMN_CONVERT(cast_mode) && in_scale > out_scale &&
            decimal_int_truncated_check(decint, int_bytes, in_scale - out_scale)) {
        log_user_warning_truncated(user_logging_ctx);
      }
    }
  } else {
    if (OB_FAIL(
          wide::common_scale_decimalint(decint, int_bytes, in_scale, out_scale, scaled_val))) {
      LOG_WARN("scale decimal int failed", K(ret));
    }
    int32_t expected_int_bytes = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(out_prec);
    if (OB_UNLIKELY(out_prec == PRECISION_UNKNOWN_YET)) {
      // tempory value may have unknown precision(-1), just set expected int bytes to input int_bytes
      expected_int_bytes = scaled_val.get_int_bytes();
      LOG_WARN("invalid out precision", K(out_prec), K(lbt()));
    }
    if (OB_FAIL(ret)) { // do nothing
    } else if (OB_FAIL(align_decint_precision_unsafe(scaled_val.get_decimal_int(),
                                                     scaled_val.get_int_bytes(), expected_int_bytes,
                                                     val))) {
      LOG_WARN("align decimal int precision failed", K(ret));
    }
  }
  return ret;
}
int check_decimalint_accuracy(const ObCastMode cast_mode,
                              const ObDecimalInt *res_decint, const int32_t int_bytes,
                              const ObPrecision precision, const ObScale scale,
                              ObDecimalIntBuilder &res_val, int &warning)
{
  int ret = OB_SUCCESS;
  bool is_finish = false;
  int &cast_ret = CM_IS_ERROR_ON_FAIL(cast_mode) ? ret : warning;
  if (int_bytes == 0) { // default zero value
    int32_t tmp_zero = 0;
    res_val.from(tmp_zero);
    int32_t out_int_bytes = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(precision);
    if (out_int_bytes > res_val.get_int_bytes()) {
      res_val.extend(out_int_bytes);
    }
    is_finish = true;
  } else if (OB_ISNULL(res_decint)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid null decimal int", K(ret), K(res_decint));
  } else if (lib::is_oracle_mode()) {
    if (OB_UNLIKELY(precision > OB_MAX_NUMBER_PRECISION)
        || OB_UNLIKELY(scale < number::ObNumber::MIN_SCALE
                       || scale > number::ObNumber::MAX_SCALE)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid precision and scale", K(ret), K(precision), K(scale));
    } else if (precision == PRECISION_UNKNOWN_YET) {
      res_val.from(res_decint, int_bytes);
      is_finish = true;
    }
  } else if (OB_UNLIKELY(precision < OB_MIN_DECIMAL_PRECISION
                         || precision > number::ObNumber::MAX_PRECISION)
             || OB_UNLIKELY(scale < 0 || scale > number::ObNumber::MAX_SCALE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid precision and scale", K(ret), K(precision), K(scale));
  } else if (OB_UNLIKELY(precision < scale)) {
    ret = OB_ERR_M_BIGGER_THAN_D;
    LOG_WARN("invalid precision and scale", K(ret), K(precision), K(scale));
  }
  if (OB_SUCC(ret) && !is_finish) {
    int16_t delta_scale = scale;
    if (lib::is_oracle_mode()) {
      delta_scale += wide::ObDecimalIntConstValue::MAX_ORACLE_SCALE_DELTA;
    }
    const ObDecimalInt *min_decint = nullptr, *max_decint = nullptr;
    int32_t int_bytes2 = 0;
    if (lib::is_mysql_mode()) {
      min_decint = wide::ObDecimalIntConstValue::get_min_value(precision);
      max_decint = wide::ObDecimalIntConstValue::get_max_value(precision);
      int_bytes2 = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(precision);
    } else {
      min_decint = wide::ObDecimalIntConstValue::get_min_value(precision);
      max_decint = wide::ObDecimalIntConstValue::get_max_value(precision);
      int_bytes2 = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(precision);
    }

    decint_cmp_fp cmp_fp =
      wide::ObDecimalIntCmpSet::get_decint_decint_cmp_func(int_bytes, int_bytes2);
    if (OB_ISNULL(cmp_fp) || OB_ISNULL(res_decint) || OB_ISNULL(min_decint)
        || OB_ISNULL(max_decint)) {
      ret = OB_ERR_UNDEFINED;
      LOG_WARN("unexpected null cmp function", K(ret), K(int_bytes), K(int_bytes2), K(res_decint),
               K(min_decint), K(max_decint));
    } else {
      int cmp_min = cmp_fp(res_decint, min_decint);
      int cmp_max = cmp_fp(res_decint, max_decint);
      if (cmp_min >= 0 && cmp_max <= 0) { // min(p, s) <= res <= max(p, s)
        res_val.from(res_decint, int_bytes);
      } else if (lib::is_oracle_mode()) {
        cast_ret = OB_ERR_VALUE_LARGER_THAN_ALLOWED;
      } else if (cmp_min < 0) { // res < min(p, s)
        cast_ret = OB_DATA_OUT_OF_RANGE;
        res_val.from(min_decint, int_bytes2);
      } else if (cmp_max > 0) { // res > max(p, s)
        cast_ret = OB_DATA_OUT_OF_RANGE;
        res_val.from(max_decint, int_bytes2);
      }
    }
  }
  return ret;
}

void log_user_warning_truncated(const ObUserLoggingCtx *user_logging_ctx)
{
  if (OB_ISNULL(user_logging_ctx) || user_logging_ctx->skip_logging()) {
  } else {
    const ObString *column_name = user_logging_ctx->get_column_name();
    LOG_USER_WARN(OB_ERR_DATA_TRUNCATED, column_name->length(), column_name->ptr(),
                  user_logging_ctx->get_row_num());
  }
}

static int common_json_string(const ObExpr &expr,
                              ObEvalCtx &ctx,
                              ObIAllocator& allocator,
                              const ObDatum &in,
                              ObString& res_str)
{
  int ret = OB_SUCCESS;
  int warning = OB_SUCCESS;
  ObCastMode cast_mode = expr.extra_;
  ObString j_bin_str = in.get_string();
  if (OB_FAIL(ObTextStringHelper::read_real_string_data(allocator, in,
      expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(),
      j_bin_str, &ctx.exec_ctx_))) {
    LOG_WARN("fail to get real data.", K(ret), K(j_bin_str));
  } else {
    ObJsonBin j_bin(j_bin_str.ptr(), j_bin_str.length(), &allocator);
    ObIJsonBase *j_base = &j_bin;
    ObJsonBuffer j_buf(&allocator);
    ObString j_str;
    bool has_set_res = false;
    // get json string
    if (OB_FAIL(j_bin.reset_iter())) {
      LOG_WARN("failed to reset json bin iter", K(ret), K(j_bin_str));
    } else if (CAST_FAIL(j_base->print(j_buf, true))) {
      LOG_WARN("fail to convert json to string", K(ret), K(j_bin_str));
      ret = OB_ERR_INVALID_JSON_VALUE_FOR_CAST;
      LOG_USER_ERROR(OB_ERR_INVALID_JSON_VALUE_FOR_CAST);
    } else {
      j_str.assign_ptr(j_buf.ptr(), j_buf.length());
      // convert charset if nesscary
      ObObjType in_type = ObLongTextType;
      ObObjType out_type = expr.datum_meta_.type_;
      ObCollationType in_cs_type = expr.args_[0]->datum_meta_.cs_type_;
      ObCollationType out_cs_type = expr.datum_meta_.cs_type_;
      // if out not binary but charset not same, convert
      bool is_need_charset_convert = ((CS_TYPE_BINARY != out_cs_type)
          && (ObCharset::charset_type_by_coll(in_cs_type) != ObCharset::charset_type_by_coll(out_cs_type)));
      if (!is_need_charset_convert) {
        res_str = j_str;
      } else {
        ObDatum tmp_datum;
        if (OB_FAIL(common_string_string(expr, in_type, in_cs_type, out_type,
                                        out_cs_type, j_str, ctx, tmp_datum, has_set_res))) {
          LOG_WARN("fail charset convert", K(ret), K(j_str), K(expr));
        } else if (tmp_datum.is_null()) {
          // do nothing
        } else if (OB_FAIL(copy_datum_str_with_tmp_alloc(ctx, tmp_datum, res_str))) {
          LOG_WARN("copy datum str with tmp alloc", K(ret));
        }
      }
    }
  }
  return ret;
}

static const double ROUND_DOUBLE = 0.5;
template <typename IN_TYPE, typename OUT_TYPE>
static OB_INLINE int common_floating_int(IN_TYPE &in_val, OUT_TYPE &out_val)
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
static int common_floating_number(const IN_TYPE in_val,
                                  const ob_gcvt_arg_type arg_type,
                                  ObIAllocator &alloc,
                                  number::ObNumber &number)
{
  int ret = OB_SUCCESS;
  char buf[MAX_DOUBLE_STRICT_PRINT_SIZE];
  MEMSET(buf, 0, MAX_DOUBLE_STRICT_PRINT_SIZE);
  int64_t length = 0;
  if (lib::is_oracle_mode() || OB_GCVT_ARG_DOUBLE == arg_type) {
    length = ob_gcvt_opt(in_val, arg_type, static_cast<int32_t>(sizeof(buf) - 1),
                         buf, NULL, lib::is_oracle_mode(), TRUE);
  } else {
    length = ob_gcvt(in_val, OB_GCVT_ARG_DOUBLE, sizeof(buf) - 1, buf, NULL);
  }
  ObString str(sizeof(buf), static_cast<int32_t>(length), buf);
  ObScale res_scale; // useless
  ObPrecision res_precision;
  if (OB_FAIL(number.from_sci_opt(str.ptr(), str.length(), alloc,
                                  &res_precision, &res_scale))) {
    LOG_WARN("fail to from str to number", K(ret), K(str));
  }
  return ret;
}

template<typename IN_TYPE>
static int common_floating_decimalint(const IN_TYPE in_val,
                                      const ob_gcvt_arg_type arg_type,
                                      ObIAllocator &alloc,
                                      ObDecimalInt *&decint,
                                      int32_t &int_bytes,
                                      int16_t &scale,
                                      int16_t &precision)
{
  int ret = OB_SUCCESS;
  char buf[MAX_DOUBLE_STRICT_PRINT_SIZE] = {0};
  int64_t length = 0;
  if (lib::is_oracle_mode() || OB_GCVT_ARG_DOUBLE == arg_type) {
    length = ob_gcvt_opt(in_val, arg_type, sizeof(buf) - 1, buf, NULL, lib::is_oracle_mode(), TRUE);
  } else {
    length = ob_gcvt(in_val, OB_GCVT_ARG_DOUBLE, sizeof(buf) - 1, buf, NULL);
  }
  scale = 0;
  precision = 0;
  if (OB_FAIL(wide::from_string(buf, length, alloc, scale, precision, int_bytes, decint))) {
    LOG_WARN("from_string failed", K(ret), K(ObString(length, buf)));
  }
  return ret;
}

template <typename IN_TYPE>
static bool is_ieee754_nan_inf(const IN_TYPE in_val,
                               char buf[], int64_t &length)
{
  bool is_nan_inf = true;
  if (lib::is_oracle_mode()) {
    // buf size is 256, nan or infinity string length is no more than 4 bytes.
    // Never hit overflow
    if (in_val == -INFINITY) {
      length = strlen("-Inf");
      strncpy(buf, "-Inf", length);
    } else if (in_val == INFINITY) {
      length = strlen("Inf");
      strncpy(buf, "Inf", length);
    } else if (isnan(in_val)) {
      length = strlen("Nan");
      strncpy(buf, "Nan", length);
    } else {
      is_nan_inf = false;
    }
  } else {
    is_nan_inf = false;
  }
  return is_nan_inf;
}

template <typename IN_TYPE>
static int common_floating_string(const ObExpr &expr,
                                  const IN_TYPE in_val,
                                  const ob_gcvt_arg_type arg_type,
                                  ObEvalCtx &ctx,
                                  ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
  int64_t length = 0;
  ObScale scale = expr.args_[0]->datum_meta_.scale_;
  bool nan_or_inf = is_ieee754_nan_inf(in_val, buf, length);
  if (nan_or_inf) {
    LOG_DEBUG("Infinity or NaN value is", K(in_val));
  } else {
    if (0 <= scale) {
      length = ob_fcvt(in_val, scale, sizeof(buf) - 1, buf, NULL);
    } else {
      length = ob_gcvt_opt(in_val, arg_type, static_cast<int32_t>(sizeof(buf) - 1),
                           buf, NULL, lib::is_oracle_mode(), TRUE);
    }
  }
  ObString in_str(sizeof(buf), static_cast<int32_t>(length), buf);
  bool has_set_res = false;
  if (OB_FAIL(common_check_convert_string(expr, ctx, in_str, res_datum, has_set_res))) {
    LOG_WARN("fail to common_check_convert_string", K(ret), K(in_str));
  }
  return ret;
}

static int common_number_datetime(const number::ObNumber nmb,
                                  const ObTimeConvertCtx &cvrt_ctx, int64_t &out_val,
                                  const ObCastMode cast_mode);

static OB_INLINE int common_double_datetime(const ObExpr &expr,
                           const double val_double,
                           ObEvalCtx &ctx,
                           ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  int warning = OB_SUCCESS;
  ObBasicSessionInfo *session = ctx.exec_ctx_.get_my_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else {
    int64_t out_val = 0;
    ObObjType out_type = expr.datum_meta_.type_;
    ObNumStackOnceAlloc tmp_alloc;
    number::ObNumber number;
    const common::ObTimeZoneInfo *tz_info_local = NULL;
    ObSolidifiedVarsGetter helper(expr, ctx, session);
    if (OB_FAIL(helper.get_time_zone_info(tz_info_local))) {
      LOG_WARN("get time zone info failed", K(ret));
    } else {
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
        ObTimeConvertCtx cvrt_ctx(tz_info_local, ObTimestampType == out_type);
        ret = common_number_datetime(number, cvrt_ctx, out_val, expr.extra_);
        if (CAST_FAIL(ret)) {
          LOG_WARN("str_to_datetime failed", K(ret));
        } else {
          SET_RES_DATETIME(out_val);
        }
      }
    }
  }
  return ret;
}

static int common_double_time(const ObExpr &expr,
                              const double val_double,
                              ObDatum &res_datum)
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

int common_datetime_string(const ObExpr &expr, const ObObjType in_type, const ObObjType out_type,
                           const ObScale in_scale, bool force_use_std_nls_format,
                           const int64_t in_val, ObEvalCtx &ctx, char *buf,
                           int64_t buf_len, int64_t &out_len)
{
  int ret = OB_SUCCESS;
  UNUSED(out_type);
  ObBasicSessionInfo *session = ctx.exec_ctx_.get_my_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else {
    const common::ObTimeZoneInfo *tz_info_local = NULL;
    ObSolidifiedVarsGetter helper(expr, ctx, session);
    if (OB_FAIL(helper.get_time_zone_info(tz_info_local))) {
      LOG_WARN("get time zone info failed", K(ret));
    } else {
      const ObTimeZoneInfo *tz_info = (ObTimestampType == in_type) ?
                                        tz_info_local : NULL;
      ObString nls_format;
      if (lib::is_oracle_mode() && !force_use_std_nls_format) {
        if (OB_FAIL(common_get_nls_format(session, ctx, &expr, in_type,
                  force_use_std_nls_format,
                  nls_format))) {
          LOG_WARN("common_get_nls_format failed", K(ret));
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(ObTimeConverter::datetime_to_str(in_val, tz_info,
              nls_format, in_scale, buf, buf_len, out_len))) {
        LOG_WARN("failed to convert datetime to string", K(ret), K(in_val), KP(tz_info),
                  K(nls_format), K(in_scale), K(buf), K(out_len));
      }
    }
  }
  return ret;
}

static int common_year_int(const ObExpr &expr,
                           const ObObjType &out_type,
                           const uint8_t in_val,
                           int64_t &out_val)
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

// 根据type，从ptr指向的空间中构建ObOTimestampData对象
// ObTimestampTZType需要12字节，ObTimestampLTZType和ObTimestampNanoType需要10字节
// 所以构造逻辑有区别
static OB_INLINE int common_construct_otimestamp(const ObObjType type,
                                                 const ObDatum &in_datum,
                                                 ObOTimestampData &out_val)
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
    ObDateSqlMode date_sql_mode;
    date_sql_mode.allow_invalid_dates_ = CM_IS_EXPLICIT_CAST(cast_mode)
                                         ? false : CM_IS_ALLOW_INVALID_DATES(cast_mode);
    date_sql_mode.no_zero_date_ = CM_IS_EXPLICIT_CAST(cast_mode)
                                  ? false : CM_IS_NO_ZERO_DATE(cast_mode);
    ret = ObTimeConverter::int_to_datetime(
          int_part, dec_part, cvrt_ctx, out_val, date_sql_mode);
  }
  return ret;
}

static int common_number_date(const number::ObNumber &nmb, const ObCastMode cast_mode,
                              int32_t &out_val)
{
  int ret = OB_SUCCESS;
  int warning = OB_SUCCESS;
  int64_t int_part = 0, dec_part = 0;
  if (OB_UNLIKELY(nmb.is_negative())) {
    ret = OB_INVALID_DATE_VALUE;
    LOG_WARN("invalid date value", K(ret), K(nmb));
  } else if (OB_UNLIKELY(!nmb.is_int_parts_valid_int64(int_part, dec_part))) {
    ret = OB_INVALID_DATE_VALUE;
    LOG_WARN("invalid date format", K(ret), K(nmb));
  } else {
    ObDateSqlMode date_sql_mode;
    date_sql_mode.allow_invalid_dates_ = CM_IS_EXPLICIT_CAST(cast_mode)
                                         ? false : CM_IS_ALLOW_INVALID_DATES(cast_mode);
    date_sql_mode.no_zero_date_ = CM_IS_EXPLICIT_CAST(cast_mode)
                                  ? false : CM_IS_NO_ZERO_DATE(cast_mode);
    ret = ObTimeConverter::int_to_date(int_part, out_val, date_sql_mode);
    if (OB_SUCC(ret) && OB_UNLIKELY(dec_part > 0)) {
      if (CM_IS_COLUMN_CONVERT(cast_mode) && !CM_IS_WARN_ON_FAIL(cast_mode)) {
        ret = OB_INVALID_DATE_VALUE;
        LOG_WARN("invalid date value with decimal part", K(ret));
      }
    }
  }
  return ret;
}

int cast_not_expected(const sql::ObExpr &expr,
                  sql::ObEvalCtx &ctx,
                  sql::ObDatum &res_datum)
{
  int ret = lib::is_oracle_mode() ? OB_ERR_INVALID_TYPE_FOR_OP : OB_ERR_UNEXPECTED;
  UNUSED(ctx);
  UNUSED(res_datum);
  ObObjType in_type = expr.args_[0]->datum_meta_.type_;
  ObObjType out_type = expr.datum_meta_.type_;
  LOG_WARN("cast_not_expected", K(ret), K(in_type), K(out_type), K(expr.extra_));
  return ret;
}

int cast_not_support(const sql::ObExpr &expr,
                  sql::ObEvalCtx &ctx,
                  sql::ObDatum &res_datum)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(ctx);
  UNUSED(res_datum);
  ObObjType in_type = expr.args_[0]->datum_meta_.type_;
  ObObjType out_type = expr.datum_meta_.type_;
  LOG_WARN("cast_not_supported", K(ret), K(in_type), K(out_type), K(expr.extra_));
  LOG_USER_ERROR(OB_NOT_SUPPORTED, "type casting");
  return ret;
}

int cast_inconsistent_types(const sql::ObExpr &expr,
                  sql::ObEvalCtx &ctx,
                  sql::ObDatum &res_datum)
{
  int ret = OB_ERR_INVALID_TYPE_FOR_OP;
  UNUSED(ctx);
  UNUSED(res_datum);
  ObObjType in_type = expr.args_[0]->datum_meta_.type_;
  ObObjType out_type = expr.datum_meta_.type_;
  LOG_WARN("inconsistent datatypes", K(ret), K(in_type), K(out_type), K(expr.extra_));
  return ret;
}

int cast_inconsistent_types_json(const sql::ObExpr &expr,
                                    sql::ObEvalCtx &ctx,
                                    sql::ObDatum &res_datum)
{
  UNUSED(ctx);
  UNUSED(res_datum);
  int ret = OB_SUCCESS;
  ObObjType in_type = expr.args_[0]->datum_meta_.type_;
  ObObjType out_type = expr.datum_meta_.type_;
  if (CM_IS_IMPLICIT_CAST(expr.extra_)) {
    ret = OB_ERR_INVALID_INPUT;
    LOG_WARN("invalid input in implicit cast", K(ret));
  } else {
    LOG_WARN("inconsistent datatypes", K(ret), K(in_type), K(out_type), K(expr.extra_));
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
  }
  return ret;
}

int cast_identity_enum_set(const sql::ObExpr &expr,
                           const ObIArray<ObString> &str_values,
                           const uint64_t cast_mode,
                           sql::ObEvalCtx &ctx,
                           sql::ObDatum &res_datum)
{
  UNUSED(cast_mode);
  UNUSED(str_values);
  EVAL_ARG() {
    if (ob_is_null(expr.args_[0]->datum_meta_.type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null type with non-null value", K(ret), K(child_res->get_enum()));
    } else {
      res_datum.set_enum(child_res->get_enum());
    }
  }
  return ret;
}

int cast_not_support_enum_set(const sql::ObExpr &expr,
                              const ObIArray<ObString> &str_values,
                              const uint64_t cast_mode,
                              sql::ObEvalCtx &ctx,
                              sql::ObDatum &res_datum)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(ctx);
  UNUSED(res_datum);
  ObObjType in_type = expr.args_[0]->datum_meta_.type_;
  ObObjType out_type = expr.datum_meta_.type_;
  LOG_WARN("not support datatypes", K(in_type), K(out_type), K(cast_mode), K(str_values), K(expr));
  LOG_USER_ERROR(OB_NOT_SUPPORTED, "cast to enum or set type");
  return ret;
}

int cast_not_expected_enum_set(const sql::ObExpr &expr,
                               const ObIArray<ObString> &str_values,
                               const uint64_t cast_mode,
                               sql::ObEvalCtx &ctx,
                               sql::ObDatum &res_datum)
{
  int ret = OB_ERR_UNEXPECTED;
  UNUSED(ctx);
  UNUSED(res_datum);
  ObObjType in_type = expr.args_[0]->datum_meta_.type_;
  ObObjType out_type = expr.datum_meta_.type_;
  LOG_WARN("not expected obj type convert", K(ret), K(in_type), K(out_type), K(expr),
           K(cast_mode),K(str_values), K(lbt()));
  return ret;
}

CAST_FUNC_NAME(unknown, other)
{
  return cast_not_support(expr, ctx, res_datum);
}

// 有的cast没有实际的逻辑，只需要计算子节点的值即可
// 例如int -> bit，cast结果直接使用子节点的结果即可,不需要进行计算
// 注意：如果有新类型的转换使用该函数，一定要在is_trivial_cast()方法中更新这种转换
// 以保证cast表达式的res_datum的指针是指向参数的空间!!!
int cast_eval_arg(const sql::ObExpr &expr,
                  sql::ObEvalCtx &ctx,
                  sql::ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  UNUSED(res_datum);
  ObDatum *child_res = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, child_res))) {
    LOG_WARN("eval arg failed", K(ret));
  } else {
    // TODO: CG部分加了优化后, 就不用赋值
    res_datum.set_datum(*child_res);
  }
  return ret;
}

// TODO:@xiaofeng.lby, need to modify cast expr in batch mode
// cast in batch mode will degrade into single row mode now
int cast_eval_arg_batch(const ObExpr &expr,
                        ObEvalCtx &ctx,
                        const ObBitVector &skip,
                        const int64_t batch_size)
{
  LOG_DEBUG("eval cast in batch mode", K(batch_size));
  int ret = OB_SUCCESS;
  ObDatum *results = expr.locate_batch_datums(ctx);

  if (OB_FAIL(expr.args_[0]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("eval args_[0] failed", K(ret));
  } else if (OB_FAIL(expr.args_[1]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("eval args_[1] failed", K(ret));
  } else {
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
    batch_info_guard.set_batch_size(batch_size);
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
      batch_info_guard.set_batch_idx(i);
      ObDatum *result = &results[i];
      if (skip.at(i) || eval_flags.at(i)) {
        continue;
      } else if (OB_FAIL(expr.eval(ctx, result))) {
        LOG_WARN("fail to eval one row", K(ret), K(i));
      } else {
        eval_flags.set(i);
      }
    }
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
      if (CM_NEED_RANGE_CHECK(expr.extra_) &&
          CAST_FAIL(uint_range_check(out_type, in_val, out_val))) {
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

// about cast for text tc, observer 4.0 always use xxx_string and string_xxx
// 4.1 always use xxx_text and text_xxx
CAST_FUNC_NAME(int, text)
{
  EVAL_ARG()
  {
    int64_t in_val = child_res->get_int();
    ObFastFormatInt ffi(in_val);
    if (OB_FAIL(common_copy_string_zf_to_text_result(expr, ObString(ffi.length(), ffi.ptr()), ctx, res_datum))) {
      LOG_WARN("common_copy_string_zf_to_text_result failed", K(ret));
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
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
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
    if (OB_FAIL(ObJsonWrapper::get_raw_binary(j_base, raw_bin, &temp_allocator))) {
      LOG_WARN("fail to get int json binary", K(ret), K(in_type), K(in_val));
    } else if (OB_FAIL(common_json_bin(expr, ctx, res_datum, raw_bin))) {
      LOG_WARN("fail to fill json bin lob locator", K(ret));
    }
  }
  return ret;
}

CAST_FUNC_NAME(int, decimalint)
{
  EVAL_ARG()
  {
    int64_t in_val = child_res->get_int();
    ObDecimalInt *decint = nullptr;
    int32_t int_bytes = 0;
    ObDecimalIntBuilder tmp_alloc;
    ObScale out_scale = expr.datum_meta_.scale_;
    ObScale in_scale = 0;
    ObPrecision out_prec = expr.datum_meta_.precision_;
    ObPrecision in_prec =
      ObAccuracy::MAX_ACCURACY2[lib::is_oracle_mode()][expr.args_[0]->datum_meta_.type_]
        .get_precision();
    const static int64_t DECINT64_MAX = get_scale_factor<int64_t>(MAX_PRECISION_DECIMAL_INT_64);
    if (in_prec > MAX_PRECISION_DECIMAL_INT_64 && in_val < DECINT64_MAX) {
      in_prec = MAX_PRECISION_DECIMAL_INT_64;
    }
    if (OB_FAIL(wide::from_integer(in_val, tmp_alloc, decint, int_bytes, in_prec))) {
      LOG_WARN("from_integer failed", K(ret), K(in_val));
    } else if (ObDatumCast::need_scale_decimalint(in_scale, in_prec, out_scale, out_prec)) {
      ObDecimalIntBuilder res_val;
      if (OB_FAIL(ObDatumCast::common_scale_decimalint(decint, int_bytes, in_scale, out_scale,
                                                       out_prec, expr.extra_, res_val))) {
        LOG_WARN("scale decimal int failed", K(ret));
      } else {
        res_datum.set_decimal_int(res_val.get_decimal_int(), res_val.get_int_bytes());
      }
    } else {
      res_datum.set_decimal_int(decint, int_bytes);
    }
  }
  return ret;
}

CAST_FUNC_NAME(int, geometry)
{
  EVAL_ARG()
  {
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    ObGeometry *geo = NULL;
    omt::ObSrsCacheGuard srs_guard;
    const ObSrsItem *srs = NULL;
    int64_t in_val = child_res->get_int();
    ObFastFormatInt ffi(in_val);
    ObGeoType dst_geo_type = ObGeoCastUtils::get_geo_type_from_cast_mode(expr.extra_);
    const char *cast_name = ObGeometryTypeCastUtil::get_cast_name(dst_geo_type);
    ObString str(ffi.length(), ffi.ptr());
    if (OB_FAIL(ObGeoExprUtils::get_srs_item(ctx, srs_guard, str, srs, true, cast_name))) {
      LOG_WARN("fail to get srs item", K(ret), K(str));
    } else if (OB_FAIL(ObGeoExprUtils::build_geometry(temp_allocator, str, geo, srs, cast_name))) {
      LOG_WARN("fail to parse geometry", K(ret), K(in_val), K(dst_geo_type));
    } else {
      ObString res_wkb;
      if (OB_FAIL(ObGeoTypeUtil::to_wkb(temp_allocator, *geo, srs, res_wkb))) {
        LOG_WARN("fail to get wkb", K(ret), K(dst_geo_type));
      } else if (OB_FAIL(common_gis_wkb(expr, ctx, res_datum, res_wkb))){
        LOG_WARN("fail to copy string", K(ret), K(dst_geo_type));
      }
    }

    if (OB_FAIL(ret) && CM_IS_COLUMN_CONVERT(expr.extra_) && ObGeoType::GEOMETRY == dst_geo_type) { // adapt mysql
      ret = OB_ERR_CANT_CREATE_GEOMETRY_OBJECT;
      LOG_USER_ERROR(OB_ERR_CANT_CREATE_GEOMETRY_OBJECT);
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
    if (in_type > out_type &&
        CAST_FAIL(uint_upper_check(out_type, in_val))) {
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
     if (OB_FAIL(common_copy_string_zf(expr, ObString(ffi.length(), ffi.ptr()),
                                    ctx, res_datum))) {
       LOG_WARN("common_copy_string_zf failed", K(ret), K(ObString(ffi.length(), ffi.ptr())));
     }
  }
  return ret;
}

CAST_FUNC_NAME(uint, text)
{
  EVAL_ARG()
  {
    uint64_t in_val = child_res->get_uint();
    ObFastFormatInt ffi(in_val);
    if (OB_FAIL(common_copy_string_zf_to_text_result(expr, ObString(ffi.length(), ffi.ptr()),
                                                     ctx, res_datum))) {
      LOG_WARN("common_copy_string_zf_to_text_result failed",
          K(ret), K(ObString(ffi.length(), ffi.ptr())));
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
    if (OB_FAIL(common_copy_string_zf(expr, ObString(ffi.length(), ffi.ptr()),
                                  ctx, res_datum))) {
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
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    ObString raw_bin;

    if (OB_FAIL(ObJsonWrapper::get_raw_binary(j_base, raw_bin, &temp_allocator))) {
      LOG_WARN("fail to get uint json binary", K(ret), K(in_type), K(in_val));
    } else if (OB_FAIL(common_json_bin(expr, ctx, res_datum, raw_bin))) {
      LOG_WARN("fail to fill json bin lob locator", K(ret));
    }
  }
  return ret;
}

CAST_FUNC_NAME(uint, decimalint)
{
  EVAL_ARG()
  {
    uint64_t in_val = child_res->get_uint();
    ObDecimalInt *decint = nullptr;
    int32_t val_len = 0;
    ObDecimalIntBuilder tmp_alloc;
    ObScale in_scale = 0;
    ObScale out_scale = expr.datum_meta_.scale_;
    ObPrecision out_prec = expr.datum_meta_.precision_;
    ObPrecision in_prec =
      ObAccuracy::MAX_ACCURACY2[lib::is_oracle_mode()][expr.args_[0]->datum_meta_.type_]
        .get_precision();
    const static uint64_t DECINT64_MAX = get_scale_factor<uint64_t>(MAX_PRECISION_DECIMAL_INT_64);
    if (in_prec > MAX_PRECISION_DECIMAL_INT_64 && in_val < DECINT64_MAX) {
      in_prec = MAX_PRECISION_DECIMAL_INT_64;
    }
    if (OB_FAIL(wide::from_integer(in_val, tmp_alloc, decint, val_len, in_prec))) {
      LOG_WARN("from_integer failed", K(ret), K(in_val));
    } else if (ObDatumCast::need_scale_decimalint(in_scale, in_prec, out_scale, out_prec)) {
      ObDecimalIntBuilder res_val;
      if (OB_FAIL(ObDatumCast::common_scale_decimalint(decint, val_len, in_scale, out_scale, out_prec,
                                          expr.extra_, res_val))) {
        LOG_WARN("scale decimal int failed", K(ret));
      } else {
        res_datum.set_decimal_int(res_val.get_decimal_int(), res_val.get_int_bytes());
      }
    } else {
      res_datum.set_decimal_int(decint, val_len);
    }
  }
  return ret;
}

CAST_FUNC_NAME(uint, geometry)
{
  EVAL_ARG()
  {
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    ObGeometry *geo = NULL;
    omt::ObSrsCacheGuard srs_guard;
    const ObSrsItem *srs = NULL;
    uint64_t in_val = child_res->get_uint();
    ObFastFormatInt ffi(in_val);
    ObGeoType dst_geo_type = ObGeoCastUtils::get_geo_type_from_cast_mode(expr.extra_);
    const char *cast_name = ObGeometryTypeCastUtil::get_cast_name(dst_geo_type);
    ObString str(ffi.length(), ffi.ptr());
    if (OB_FAIL(ObGeoExprUtils::get_srs_item(ctx, srs_guard, str, srs, true, cast_name))) {
      LOG_WARN("fail to get srs item", K(ret), K(str));
    } else if (OB_FAIL(ObGeoExprUtils::build_geometry(temp_allocator, str, geo, srs, cast_name))) {
      LOG_WARN("fail to parse geometry", K(ret), K(in_val), K(dst_geo_type));
    } else {
      ObString res_wkb;
      if (OB_FAIL(ObGeoTypeUtil::to_wkb(temp_allocator, *geo, srs, res_wkb))) {
        LOG_WARN("fail to get wkb", K(ret), K(dst_geo_type));
      } else if (OB_FAIL(common_gis_wkb(expr, ctx, res_datum, res_wkb))){
        LOG_WARN("fail to copy string", K(ret), K(dst_geo_type));
      }
    }

    if (OB_FAIL(ret) && CM_IS_COLUMN_CONVERT(expr.extra_) && ObGeoType::GEOMETRY == dst_geo_type) { // adapt mysql
      ret = OB_ERR_CANT_CREATE_GEOMETRY_OBJECT;
      LOG_USER_ERROR(OB_ERR_CANT_CREATE_GEOMETRY_OBJECT);
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
    OZ(common_string_double(expr, in_type, expr.args_[0]->datum_meta_.cs_type_, out_type, in_str, res_datum));
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
    OZ(common_string_datetime(expr, ObString(child_res->len_, child_res->ptr_),
                              ctx, res_datum));
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
    OZ(common_string_bit(expr, ObString(child_res->len_, child_res->ptr_),
                         ctx, res_datum));
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
    bool has_set_res = false;
    OZ(common_string_string(expr, in_type, in_cs_type, out_type,
                            out_cs_type, in_str, ctx, res_datum, has_set_res));
  }
  return ret;
}

CAST_FUNC_NAME(string, text)
{
  EVAL_STRING_ARG()
  {
    ObObjType in_type = expr.args_[0]->datum_meta_.type_;
    ObObjType out_type = expr.datum_meta_.type_;
    ObCollationType in_cs_type = expr.args_[0]->datum_meta_.cs_type_;
    ObCollationType out_cs_type = expr.datum_meta_.cs_type_;
    ObString in_str(child_res->len_, child_res->ptr_);
    OZ(common_string_text(expr, in_str, ctx, NULL, res_datum));
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
    bool has_set_res = false;
    OZ(ObDatumHexUtils::hextoraw_string(expr, in_str, ctx, res_datum, has_set_res));
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
  ObDatum *child_res = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, child_res))) {
    LOG_WARN("eval arg failed", K(ret));
  } else if (child_res->is_null() ||
             (lib::is_oracle_mode() && ObLongTextType != expr.args_[0]->datum_meta_.type_
              && 0 == child_res->len_)) {
    res_datum.set_null();
  } else {
    ObString in_str(child_res->len_, child_res->ptr_);
    OZ(common_string_lob(expr, in_str, ctx, NULL, res_datum));
  }
  return ret;
}

CAST_FUNC_NAME(string, decimalint)
{
  EVAL_STRING_ARG()
  {
    ObString in_str(child_res->len_, child_res->ptr_);
    ObDecimalIntBuilder res_val;
    if (OB_FAIL(common_string_decimalint(expr, in_str, ctx.exec_ctx_.get_user_logging_ctx(),
                                         res_val))) {
      LOG_WARN("cast string to decimal int failed", K(ret));
    } else {
      res_datum.set_decimal_int(res_val.get_decimal_int(), res_val.get_int_bytes());
    }
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
  bool has_set_res = false;
  // binary type will convert to json opaque, other types need convert charset to utf8
  bool is_need_charset_convert = ((CS_TYPE_BINARY != in_cs_type) && 
                                  (ObCharset::charset_type_by_coll(in_cs_type) != 
                                   ObCharset::charset_type_by_coll(out_cs_type)));
  if (lib::is_mysql_mode() && (out_cs_type != CS_TYPE_UTF8MB4_BIN)) {
    ret = OB_ERR_INVALID_JSON_CHARSET;
    LOG_WARN("fail to cast string to json invalid outtype", K(ret), K(out_cs_type));
  } else if (is_need_charset_convert && 
    OB_FAIL(common_string_string(expr, in_type, in_cs_type, out_type,
                                 out_cs_type, in_str, ctx, res_datum, has_set_res))) {
    LOG_WARN("fail to cast string to longtext", K(ret), K(in_str), K(expr));
  } else {
    ObString j_text;
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
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
      ObJsonNull j_null;
      ObJsonNode *j_tree = NULL;
      bool is_null_res = false;
      bool is_scalar = (j_text.length()
                        && ((j_text[0] == '\'' && j_text[j_text.length() - 1] == '\'')
                            || (j_text[0] == '\"' && j_text[j_text.length() - 1] == '\"')));

      bool is_oracle = lib::is_oracle_mode();

      bool relaxed_json = lib::is_oracle_mode() && !(CM_IS_STRICT_JSON(expr.extra_));
      uint32_t parse_flag = ObJsonParser::JSN_STRICT_FLAG;

      ADD_FLAG_IF_NEED(relaxed_json, parse_flag, ObJsonParser::JSN_RELAXED_FLAG);
      ADD_FLAG_IF_NEED(lib::is_oracle_mode(), parse_flag, ObJsonParser::JSN_UNIQUE_FLAG);

      bool is_convert_jstr_type = (in_type == ObTinyTextType
                                 || in_type == ObTextType
                                 || in_type == ObMediumTextType
                                 || in_type == ObLongTextType);

      if (!is_oracle && in_cs_type == CS_TYPE_BINARY) {
        j_base = &j_opaque;
      } else if (is_oracle && CM_IS_IMPLICIT_CAST(expr.extra_) && OB_ISNULL(j_text.ptr())) {
        res_datum.set_null();
        is_null_res = true;
      } else if (!is_oracle
                  && (is_enumset_to_str
                      || (CM_IS_SQL_AS_JSON_SCALAR(expr.extra_) && ob_is_string_type(in_type))
                      || (CM_IS_IMPLICIT_CAST(expr.extra_)
                          && !CM_IS_COLUMN_CONVERT(expr.extra_)
                          && !CM_IS_JSON_VALUE(expr.extra_)
                          && is_convert_jstr_type))) {
        // consistent with mysql: TINYTEXT, TEXT, MEDIUMTEXT, and LONGTEXT. We want to treat them like strings
        j_base = &j_string;
        if ((CM_IS_SQL_AS_JSON_SCALAR(expr.extra_) && ob_is_string_type(in_type)) && j_text.compare("null") == 0) {
          j_base = &j_null;
        }
      } else if (is_oracle && (OB_ISNULL(j_text.ptr()) || j_text.length() == 0)) {
        j_base = &j_null;
      } else if (OB_FAIL(ObJsonParser::get_tree(&temp_allocator, j_text, j_tree, parse_flag))) {
        if (!is_oracle && CM_IS_IMPLICIT_CAST(expr.extra_) && !CM_IS_COLUMN_CONVERT(expr.extra_)) {
          ret = OB_SUCCESS;
          j_base = &j_string;
        } else {
          LOG_DEBUG("fail to parse string as json tree", K(ret), K(in_type), K(in_str));
          if (CM_IS_COLUMN_CONVERT(expr.extra_)) {
            if (!is_oracle) {
              ret = OB_ERR_INVALID_JSON_TEXT;
              LOG_USER_ERROR(OB_ERR_INVALID_JSON_TEXT);
            } else if (is_scalar) {
              ObString tmp;
              if (OB_FAIL(ob_write_string(temp_allocator, j_text, tmp))) {
                LOG_DEBUG("fail to write buffer", K(ret), K(in_type), K(j_text));
              } else {
                tmp.ptr()[0] = tmp.ptr()[tmp.length() - 1] = '"';
                new (&j_string)ObJsonString(tmp.ptr(), tmp.length());
                j_base = &j_string;
              }

            }
          } else {
            ret = OB_ERR_INVALID_JSON_TEXT_IN_PARAM;
            LOG_USER_ERROR(OB_ERR_INVALID_JSON_TEXT_IN_PARAM);
          }
        }
      } else {
        j_base = j_tree;
      }

      if (OB_SUCC(ret) && !is_null_res) {
        ObString raw_bin;
        if (OB_FAIL(ObJsonWrapper::get_raw_binary(j_base, raw_bin, &temp_allocator))) {
          LOG_WARN("fail to get string json binary", K(ret), K(in_type), K(raw_bin));
        } else if (OB_FAIL(common_json_bin(expr, ctx, res_datum, raw_bin))) {
          LOG_WARN("fail to fill json bin lob locator", K(ret));
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
      ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
      common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
      if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *child_res,
                  expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), in_str,
                  &ctx.exec_ctx_))) {
        LOG_WARN("fail to get real data.", K(ret), K(in_str));
      } else {
        ret = common_string_json(expr, in_str, ctx, res_datum);
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(text, int)
{
  EVAL_STRING_ARG()
  {
    ObString in_str(child_res->len_, child_res->ptr_);
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    OZ(get_text_full_data(expr, ctx, &temp_allocator, child_res, in_str));
    const bool is_str_int_cast = true;
    OZ(common_string_int(expr, expr.extra_, in_str, is_str_int_cast, res_datum));
  }
  return ret;
}

CAST_FUNC_NAME(text, uint)
{
  EVAL_STRING_ARG()
  {
    ObString in_str(child_res->len_, child_res->ptr_);
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    OZ(get_text_full_data(expr, ctx, &temp_allocator, child_res, in_str));
    const bool is_str_int_cast = true;
    OZ(common_string_uint(expr, in_str, is_str_int_cast, res_datum));
  }
  return ret;
}

CAST_FUNC_NAME(text, float)
{
  EVAL_STRING_ARG()
  {
    ObString in_str(child_res->len_, child_res->ptr_);
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    OZ(get_text_full_data(expr, ctx, &temp_allocator, child_res, in_str));
    float out_val = 0;
    OZ(common_string_float(expr, in_str, out_val));
    OX(res_datum.set_float(out_val));
  }
  return ret;
}

CAST_FUNC_NAME(text, double)
{
  EVAL_STRING_ARG()
  {
    DEF_IN_OUT_TYPE();
    ObString in_str = ObString(child_res->len_, child_res->ptr_);
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    OZ(get_text_full_data(expr, ctx, &temp_allocator, child_res, in_str));
    OZ(common_string_double(expr, in_type, expr.args_[0]->datum_meta_.cs_type_, out_type, in_str, res_datum));
  }
  return ret;
}

CAST_FUNC_NAME(text, number)
{
  EVAL_STRING_ARG()
  {
    number::ObNumber nmb;
    ObNumStackOnceAlloc tmp_alloc;
    ObString in_str(child_res->len_, child_res->ptr_);
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    OZ(get_text_full_data(expr, ctx, &temp_allocator, child_res, in_str));
    OZ(common_string_number(expr, in_str, tmp_alloc, nmb));
    OX(res_datum.set_number(nmb));
  }
  return ret;
}

CAST_FUNC_NAME(text, decimalint)
{
  EVAL_STRING_ARG()
  {
    ObString in_str(child_res->len_, child_res->ptr_);
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    ObDecimalIntBuilder res_val;
    if (OB_FAIL(get_text_full_data(expr, ctx, &temp_allocator, child_res, in_str))) {
      LOG_WARN("get string data failed", K(ret));
    } else if (OB_FAIL(common_string_decimalint(expr, in_str, ctx.exec_ctx_.get_user_logging_ctx(), res_val))) {
      LOG_WARN("cast string to decimal int failed", K(ret), K(in_str));
    } else {
      res_datum.set_decimal_int(res_val.get_decimal_int(), res_val.get_int_bytes());
    }
  }
  return ret;
}

CAST_FUNC_NAME(text, datetime)
{
  EVAL_STRING_ARG()
  {
    ObString in_str;
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    OZ(get_text_full_data(expr, ctx, &temp_allocator, child_res, in_str));
    OZ(common_string_datetime(expr, in_str, ctx, res_datum));
  }
  return ret;
}

CAST_FUNC_NAME(text, date)
{
  EVAL_STRING_ARG()
  {
    ObString in_str;
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    OZ(get_text_full_data(expr, ctx, &temp_allocator, child_res, in_str));
    OZ(common_string_date(expr, in_str, res_datum));
  }
  return ret;
}

CAST_FUNC_NAME(text, time)
{
  EVAL_STRING_ARG()
  {
    ObString in_str;
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    OZ(get_text_full_data(expr, ctx, &temp_allocator, child_res, in_str));
    OZ(common_string_time(expr, in_str, res_datum));
  }
  return ret;
}

CAST_FUNC_NAME(text, year)
{
  EVAL_STRING_ARG()
  {
    ObString in_str;
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    OZ(get_text_full_data(expr, ctx, &temp_allocator, child_res, in_str));
    OZ(common_string_year(expr, in_str, res_datum));
  }
  return ret;
}

CAST_FUNC_NAME(text, bit)
{
  EVAL_STRING_ARG()
  {
    ObString in_str;
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    OZ(get_text_full_data(expr, ctx, &temp_allocator, child_res, in_str));
    OZ(common_string_bit(expr, in_str, ctx, res_datum));
  }
  return ret;
}

CAST_FUNC_NAME(text, string)
{
  EVAL_STRING_ARG()
  {
    ObObjType in_type = expr.args_[0]->datum_meta_.type_;
    ObObjType out_type = expr.datum_meta_.type_;
    ObCollationType in_cs_type = expr.args_[0]->datum_meta_.cs_type_;
    ObCollationType out_cs_type = expr.datum_meta_.cs_type_;
    const bool has_lob_header = expr.args_[0]->obj_meta_.has_lob_header();
    const bool is_same_charset = (ObCharset::charset_type_by_coll(in_cs_type)
                                  == ObCharset::charset_type_by_coll(out_cs_type));
    ObString data;
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    ObExprStrResAlloc res_alloc(expr, ctx);
    ObTextStringIter instr_iter(in_type, in_cs_type, child_res->get_string(), has_lob_header);
    const ObLobCommon& lob = child_res->get_lob_data();
    if (child_res->len_ != 0 && !lob.is_mem_loc_ && lob.in_row_ && has_lob_header) {
      data.assign_ptr(lob.get_inrow_data_ptr(), static_cast<int32_t>(lob.get_byte_size(child_res->len_)));
    } else if (OB_FAIL(ObTextStringHelper::build_text_iter(instr_iter, &ctx.exec_ctx_, ctx.exec_ctx_.get_my_session(),
                                is_same_charset ? reinterpret_cast<ObIAllocator *>(&res_alloc) : &temp_allocator,
                                &temp_allocator))) {
      LOG_WARN("init lob str iter failed ", K(ret), K(in_type));
    } else if (OB_FAIL(instr_iter.get_full_data(data))) {
      LOG_WARN("init lob str iter failed ", K(ret), K(in_type));
    }
    if (OB_FAIL(ret)) {
    } else if (lib::is_oracle_mode()
               && ob_is_clob(in_type, in_cs_type)
               && (0 == data.length())
               && !ob_is_clob(out_type, out_cs_type)) {
      // in oracle mode, empty clob cast to other types, result is NULL
      res_datum.set_null();
    } else if (is_same_charset) {
      res_datum.set_string(data.ptr(), data.length());
    } else {
      bool has_set_res = false;
      OZ(common_string_string(expr, in_type, in_cs_type, out_type,
                              out_cs_type, data, ctx, res_datum, has_set_res));
    }
    ObString in_str(child_res->len_, child_res->ptr_);
    lob_string_debug(in_type, in_cs_type, out_type, out_cs_type, in_str, res_datum, ret);
  }
  return ret;
}

CAST_FUNC_NAME(text, text)
{
  EVAL_STRING_ARG()
  {
    ObObjType in_type = expr.args_[0]->datum_meta_.type_;
    ObObjType out_type = expr.datum_meta_.type_;
    ObCollationType in_cs_type = expr.args_[0]->datum_meta_.cs_type_;
    ObCollationType out_cs_type = expr.datum_meta_.cs_type_;
    const bool has_lob_header = expr.args_[0]->obj_meta_.has_lob_header();
    const bool is_same_charset = (ObCharset::charset_type_by_coll(in_cs_type)
                                  == ObCharset::charset_type_by_coll(out_cs_type));
    const bool is_cs_any = (in_cs_type == CS_TYPE_ANY || out_cs_type == CS_TYPE_ANY);
    const bool is_tiny_to_others = ((in_type == ObTinyTextType && out_type != ObTinyTextType)
                                   || (in_type != ObTinyTextType && out_type == ObTinyTextType));
    ObString in_str(child_res->len_, child_res->ptr_);
    ObString data;
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    ObLobLocatorV2 input_locator;
    input_locator.assign_buffer(in_str.ptr(), in_str.length(), has_lob_header);
    bool is_valid = input_locator.is_valid();
    bool is_delta_lob = is_valid && input_locator.is_delta_temp_lob();
    bool is_persist = is_valid && input_locator.is_persist_lob();
    bool is_freed = is_valid && input_locator.is_freed();
    if (!is_tiny_to_others
        && (is_same_charset || is_delta_lob || (is_persist && is_cs_any) || is_freed)) {
      // persist with cs_any only in pl?
      res_datum.set_string(in_str.ptr(), in_str.length());
    } else {
      ObTextStringIter instr_iter(in_type, in_cs_type, in_str, has_lob_header);
      if (OB_FAIL(ObTextStringHelper::build_text_iter(instr_iter, &ctx.exec_ctx_, ctx.exec_ctx_.get_my_session(), &temp_allocator))) {
        LOG_WARN("init lob str iter failed ", K(ret), K(in_type));
      } else if (OB_FAIL(instr_iter.get_full_data(data))) {
        LOG_WARN("init lob str iter failed ", K(ret), K(in_type));
      } else {
        OZ(common_string_text(expr, data, ctx, NULL, res_datum)); // ToDo: streaming convert
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(text, otimestamp)
{
  EVAL_STRING_ARG()
  {
    ObString in_str;
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    OZ(get_text_full_data(expr, ctx, &temp_allocator, child_res, in_str));
    OZ(common_string_otimestamp(expr, in_str, ctx, res_datum));
  }
  return ret;
}

CAST_FUNC_NAME(text, raw)
{
  EVAL_STRING_ARG()
  {
    ObString in_str;
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    OZ(get_text_full_data(expr, ctx, &temp_allocator, child_res, in_str));
    if (OB_FAIL(ret)) {
    } else if (expr.datum_meta_.cs_type_ != CS_TYPE_BINARY) {
      bool has_set_res = false;
      OZ(ObDatumHexUtils::hextoraw_string(expr, in_str, ctx, res_datum, has_set_res));
    } else { // blob to raw
      // empty blob treat as null in oracle
      if (lib::is_oracle_mode() && in_str.length() == 0) {
        res_datum.set_null();
      } else if (OB_FAIL(common_copy_string(expr, in_str, ctx, res_datum))) {
        LOG_WARN("common_copy_string fail", K(ret), "length", in_str.length());
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(text, interval)
{
  EVAL_STRING_ARG()
  {
    ObString in_str;
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    OZ(get_text_full_data(expr, ctx, &temp_allocator, child_res, in_str));
    OZ(common_string_interval(expr, in_str, ctx, res_datum));
  }
  return ret;
}

CAST_FUNC_NAME(text, rowid)
{
  EVAL_STRING_ARG()
  {
    ObString in_str;
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    OZ(get_text_full_data(expr, ctx, &temp_allocator, child_res, in_str));
    OZ(common_string_rowid(expr, in_str, ctx, res_datum));
  }
  return ret;
}

CAST_FUNC_NAME(text, lob)
{
  EVAL_STRING_ARG()
  {
    int ret = OB_SUCCESS;
    ObObjType in_type = expr.args_[0]->datum_meta_.type_;
    ObObjType out_type = expr.datum_meta_.type_;
    ObCollationType in_cs_type = expr.args_[0]->datum_meta_.cs_type_;
    ObCollationType out_cs_type = expr.datum_meta_.cs_type_;
    const bool has_lob_header = expr.args_[0]->obj_meta_.has_lob_header();
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    ObTextStringIter instr_iter(in_type, in_cs_type, child_res->get_string(), has_lob_header);
    ObString data_str;
    if (OB_FAIL(ObTextStringHelper::build_text_iter(instr_iter, &ctx.exec_ctx_, ctx.exec_ctx_.get_my_session(), &temp_allocator))) {
      LOG_WARN("init lob str iter failed ", K(ret), K(in_type));
    } else if (OB_FAIL(instr_iter.get_full_data(data_str))) {
      LOG_WARN("init lob str iter failed ", K(ret), K(in_type));
    }
    OZ(common_string_lob(expr, data_str, ctx, NULL, res_datum));
  }
  return ret;
}

CAST_FUNC_NAME(geometry, geometry); // declare advance
CAST_FUNC_NAME(string, geometry)
{
  EVAL_STRING_ARG()
  {
    int warning = OB_SUCCESS;
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    ObString in_str = child_res->get_string();
    ObGeoType dst_geo_type = ObGeoCastUtils::get_geo_type_from_cast_mode(expr.extra_);
    const char *cast_name = ObGeometryTypeCastUtil::get_cast_name(dst_geo_type);
    ObGeometry *geo = NULL;
    omt::ObSrsCacheGuard srs_guard;
    const ObSrsItem *srs = NULL;
    if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *child_res,
                  expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), in_str,
                  &ctx.exec_ctx_))) {
        LOG_WARN("fail to get real data.", K(ret), K(in_str));
    } else if (OB_FAIL(ObGeoExprUtils::get_srs_item(ctx, srs_guard, in_str, srs, true, cast_name))) {
      LOG_WARN("fail to get srs item", K(ret), K(in_str));
    } else if (OB_FAIL(ObGeoExprUtils::build_geometry(temp_allocator, in_str, geo, srs, cast_name, ObGeoBuildFlag::GEO_ALLOW_3D_DEFAULT))) {
      LOG_WARN("fail to parse geometry", K(ret), K(in_str), K(dst_geo_type));
    } else if (ObGeoType::GEOMETRY == dst_geo_type || ObGeoType::GEOTYPEMAX == dst_geo_type) {
      ObString res_wkb;
      if (OB_FAIL(ObGeoTypeUtil::add_geo_version(temp_allocator, in_str, res_wkb))) {
        LOG_WARN("fail to add version", K(ret), K(dst_geo_type));
      } else if (OB_FAIL(common_gis_wkb(expr, ctx, res_datum, res_wkb))) {
        LOG_WARN("fail to copy string", K(ret), K(dst_geo_type));
      }
    } else if (OB_FAIL(geometry_geometry(expr, ctx, res_datum))) {
      LOG_WARN("fail to cast geometry", K(ret), K(in_str), K(dst_geo_type));
    }

    if (OB_FAIL(ret) && CM_IS_COLUMN_CONVERT(expr.extra_) && ObGeoType::GEOMETRY == dst_geo_type) { // adapt mysql
      ret = OB_ERR_CANT_CREATE_GEOMETRY_OBJECT;
      LOG_USER_ERROR(OB_ERR_CANT_CREATE_GEOMETRY_OBJECT);
    }
  }
  return ret;
}

CAST_FUNC_NAME(string, roaringbitmap)
{
  EVAL_STRING_ARG()
  {
    ObCollationType in_cs_type = expr.args_[0]->datum_meta_.cs_type_;
    if (in_cs_type != CS_TYPE_BINARY) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("invalid in_cs_type of string to cast to roaringbitmap", K(ret), K(in_cs_type));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "cast string collation type not in binary to roaringbitmap");
    } else {
      ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
      common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
      ObString in_str = child_res->get_string();
      ObString out_str = nullptr;
      if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *child_res,
                    expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), in_str))) {
        LOG_WARN("failed to get real data.", K(ret), K(in_str));
      } else if (OB_FAIL(ObRbUtils::build_binary(temp_allocator, in_str, out_str))) {
        LOG_WARN("failed to build rb binary", K(ret));
      } else {
        ObTextStringDatumResult text_result(ObRoaringBitmapType, &expr, &ctx, &res_datum);
        if (OB_FAIL(text_result.init(out_str.length()))) {
          LOG_WARN("Lob: init lob result failed");
        } else if (OB_FAIL(text_result.append(out_str.ptr(), out_str.length()))) {
          LOG_WARN("failed to append realdata", K(ret), K(out_str), K(text_result));
        } else {
          text_result.set_result();
        }
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(number, int)
{
  EVAL_ARG()
  {
    const number::ObNumber nmb(child_res->get_number());
    const char *nmb_buf = nmb.format();
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
    const char *nmb_buf = nmb.format();
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
        if (OB_FAIL(common_string_double(expr, in_type, expr.args_[0]->datum_meta_.cs_type_, out_type, num_str, res_datum))) {
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

CAST_FUNC_NAME(number, text)
{
  EVAL_ARG()
  {
    ObString res_str;
    if (OB_FAIL(common_number_string(expr, *child_res, ctx, res_datum))) {
      LOG_WARN("common_number_string failed", K(ret));
    } else if (OB_FAIL(copy_datum_str_with_tmp_alloc(ctx, res_datum, res_str))) {
      LOG_WARN("copy datum string with tmp allocator failed", K(ret));
    } else if (OB_FAIL(common_string_text(expr, res_str, ctx, NULL, res_datum))) {
      LOG_WARN("cast string to lob failed", K(ret));
    }
  }
  return ret;
}

CAST_FUNC_NAME(number, time)
{
  EVAL_ARG()
  {
    const number::ObNumber nmb(child_res->get_number());
    const char *nmb_buf = nmb.format();
    if (OB_ISNULL(nmb_buf)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null pointer", K(ret), K(nmb_buf));
    } else if (OB_FAIL(common_string_time(expr, ObString(strlen(nmb_buf), nmb_buf),
                                          res_datum))) {
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
    const char *nmb_buf = nmb.format();
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
      const common::ObTimeZoneInfo *tz_info_local = NULL;
      ObSolidifiedVarsGetter helper(expr, ctx, session);
      if (OB_FAIL(helper.get_time_zone_info(tz_info_local))) {
        LOG_WARN("get time zone info failed", K(ret));
      } else {
        ObTimeConvertCtx cvrt_ctx(tz_info_local,
            ObTimestampType == out_type);
        int64_t out_val = 0;
        ret = common_number_datetime(nmb, cvrt_ctx, out_val, expr.extra_);
        int warning = OB_SUCCESS;
        if (CAST_FAIL(ret)) {
        } else {
          SET_RES_DATETIME(out_val);
        }
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
    ret = common_number_date(nmb, expr.extra_, out_val);
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

static int common_number_json(const number::ObNumber &nmb, const ObObjType in_type,
                              const int16_t precision, const int16_t scale, const sql::ObExpr &expr,
                              sql::ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  if (lib::is_oracle_mode() && !number::ObNumber::is_zero_number(nmb.get_desc())) {
    if (CM_IS_IMPLICIT_CAST(expr.extra_)) {
      ret = OB_ERR_INVALID_INPUT;
      LOG_WARN("invalid input in implicit cast", K(ret));
    } else {
      LOG_WARN("inconsistent datatypes", K(ret), K(in_type), K("json"), K(expr.extra_));
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
    }
  } else {
    ObJsonDecimal j_dec(nmb, precision, scale);
    ObIJsonBase *j_base = &j_dec;
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    number::ObNumber tmp_num;
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    ObString raw_bin;
    if (OB_FAIL(tmp_num.from(nmb, temp_allocator))) {
      LOG_WARN("copy number failed", K(ret));
    } else if (OB_FAIL(ObJsonWrapper::get_raw_binary(j_base, raw_bin, &temp_allocator))) {
      LOG_WARN("fail to get number json binary", K(ret), K(in_type), K(nmb));
    } else if (OB_FAIL(common_json_bin(expr, ctx, res_datum, raw_bin))) {
      LOG_WARN("fail to fill json bin lob locator", K(ret));
    }
  }
  return ret;
}

CAST_FUNC_NAME(number, json)
{
  EVAL_ARG_FOR_CAST_TO_JSON()
  {
    const number::ObNumber nmb(child_res->get_number());
    ObObjType in_type = expr.args_[0]->datum_meta_.type_;
    ObPrecision prec = expr.args_[0]->datum_meta_.precision_;
    ObScale scale = expr.args_[0]->datum_meta_.scale_;
    ret = common_number_json(nmb, in_type, prec, scale, expr, ctx, res_datum);
  }
  return ret;
}

static int common_string_geometry(const char *buf, int64_t length, const sql::ObExpr &expr,
                                  sql::ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  omt::ObSrsCacheGuard srs_guard;
  const ObSrsItem *srs = NULL;
  ObGeometry *geo = NULL;
  ObGeoType dst_geo_type = ObGeoCastUtils::get_geo_type_from_cast_mode(expr.extra_);
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
  const char *cast_name = ObGeometryTypeCastUtil::get_cast_name(dst_geo_type);
  if (OB_FAIL(ObGeoExprUtils::get_srs_item(ctx, srs_guard, ObString(length, buf), srs, true,
                                           cast_name))) {
    LOG_WARN("fail to get srs item", K(ret));
  } else if (OB_FAIL(ObGeoExprUtils::build_geometry(temp_allocator, ObString(length, buf), geo, srs,
                                                    cast_name))) {
    LOG_WARN("fail to parse geometry", K(ret), K(ObString(length, buf)), K(dst_geo_type));
  } else {
    ObString res_wkb;
    if (OB_FAIL(ObGeoTypeUtil::to_wkb(temp_allocator, *geo, srs, res_wkb))) {
      LOG_WARN("fail to get wkb", K(ret), K(dst_geo_type));
    } else if (OB_FAIL(common_gis_wkb(expr, ctx, res_datum, res_wkb))) {
      LOG_WARN("fail to copy string", K(ret), K(dst_geo_type));
    }
  }
  if (OB_FAIL(ret) && CM_IS_COLUMN_CONVERT(expr.extra_)
      && ObGeoType::GEOMETRY == dst_geo_type) { // adapt mysql
    ret = OB_ERR_CANT_CREATE_GEOMETRY_OBJECT;
    LOG_USER_ERROR(OB_ERR_CANT_CREATE_GEOMETRY_OBJECT);
  }
  return ret;
}

CAST_FUNC_NAME(number, geometry)
{
  EVAL_ARG()
  {
    const number::ObNumber nmb(child_res->get_number());
    char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
    ObScale in_scale = expr.args_[0]->datum_meta_.scale_;
    int64_t len = 0;
    if (OB_FAIL(nmb.format(buf, sizeof(buf), len, in_scale))) {
      LOG_WARN("fail to format number", K(ret), K(nmb));
    } else if (OB_FAIL(common_string_geometry(buf, len, expr, ctx, res_datum))) {
      LOG_WARN("common string geometry failed", K(ret));
    }
  }
  return ret;
}

CAST_FUNC_NAME(float, int)
{
  EVAL_ARG()
  if (OB_SUCC(ret)) {
    DEF_IN_OUT_VAL(float, int64_t, 0);
    if (CAST_FAIL(common_double_int(in_val, out_val, LLONG_MIN,
                  CM_IS_COLUMN_CONVERT(expr.extra_) ? LLONG_MAX : LLONG_MIN))) {
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
        // 这里处理[LLONG_MIN, 0)范围内的in，转换为unsigned应该报OB_DATA_OUT_OF_RANGE。
        // out不等于0避免[-0.5, 0)内的值被误判，因为它们round后的值是0，处于合法范围内。
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
    if (isnan(in_val)) {
      ret = OB_INVALID_NUMERIC;
      LOG_WARN("float_number failed ", K(ret), K(in_val));
    } else if (isinf(in_val)) {
      ret = OB_NUMERIC_OVERFLOW;
      LOG_WARN("float_number failed", K(ret), K(in_val));
    } else {
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
    if (OB_FAIL(common_floating_string(expr, in_val, OB_GCVT_ARG_FLOAT,
                                                ctx, res_datum))) {
      LOG_WARN("common_floating_string failed", K(ret), K(in_val));
    }
  }
  return ret;
}

CAST_FUNC_NAME(float, text)
{
  EVAL_ARG()
  {
    ObString res_str;
    float in_val = child_res->get_float();
    if (OB_FAIL(common_floating_string(expr, in_val, OB_GCVT_ARG_FLOAT,
                                                ctx, res_datum))) {
      LOG_WARN("common_floating_string failed", K(ret), K(in_val));
    } else if (OB_FAIL(copy_datum_str_with_tmp_alloc(ctx, res_datum, res_str))) {
      LOG_WARN("copy datum string with tmp allocator failed", K(ret));
    } else if (OB_FAIL(common_string_text(expr, res_str, ctx, NULL, res_datum))) {
      LOG_WARN("cast string to lob failed", K(ret));
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
    if (OB_FAIL(common_floating_string(expr, in_val, OB_GCVT_ARG_FLOAT,
                                                ctx, res_datum))) {
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
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    ObString raw_bin;
    
    if (OB_FAIL(ObJsonWrapper::get_raw_binary(j_base, raw_bin, &temp_allocator))) {
      LOG_WARN("fail to get float json binary", K(ret), K(in_type), K(in_val));
    } else if (OB_FAIL(common_json_bin(expr, ctx, res_datum, raw_bin))) {
      LOG_WARN("fail to fill json bin lob locator", K(ret));
    }
  }
  return ret;
}

CAST_FUNC_NAME(float, geometry)
{
  EVAL_ARG()
  {
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    ObGeometry *geo = NULL;
    omt::ObSrsCacheGuard srs_guard;
    const ObSrsItem *srs = NULL;
    float in_val = child_res->get_float();
    char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
    ObScale scale = expr.args_[0]->datum_meta_.scale_;
    int64_t len = 0;
    ObGeoType dst_geo_type = ObGeoCastUtils::get_geo_type_from_cast_mode(expr.extra_);
    bool nan_or_inf = is_ieee754_nan_inf(in_val, buf, len);
    if (nan_or_inf) {
      LOG_DEBUG("Infinity or NaN value is", K(in_val));
    } else {
      if (0 <= scale) {
        len = ob_fcvt(in_val, scale, sizeof(buf) - 1, buf, NULL);
      } else {
        len = ob_gcvt_opt(in_val, OB_GCVT_ARG_FLOAT, sizeof(buf) - 1,
                            buf, NULL, false, TRUE);
      }
    }
    ObString in_str(sizeof(buf), static_cast<int32_t>(len), buf);
    if (OB_FAIL(ObGeoExprUtils::get_srs_item(ctx, srs_guard, in_str, srs, true))) {
      LOG_WARN("fail to get srs item", K(ret), K(in_str));
    } else if (OB_FAIL(ObGeoExprUtils::build_geometry(temp_allocator, in_str,
        geo, srs, ObGeometryTypeCastUtil::get_cast_name(dst_geo_type)))) {
      LOG_WARN("fail to parse geometry", K(ret), K(in_val), K(dst_geo_type));
    } else {
      ObString res_wkb;
      if (OB_FAIL(ObGeoTypeUtil::to_wkb(temp_allocator, *geo, srs, res_wkb))) {
        LOG_WARN("fail to get wkb", K(ret), K(dst_geo_type));
      } else if (OB_FAIL(common_gis_wkb(expr, ctx, res_datum, res_wkb))){
        LOG_WARN("fail to copy string", K(ret), K(dst_geo_type));
      }
    }

    if (OB_FAIL(ret) && CM_IS_COLUMN_CONVERT(expr.extra_) && ObGeoType::GEOMETRY == dst_geo_type) { // adapt mysql
      ret = OB_ERR_CANT_CREATE_GEOMETRY_OBJECT;
      LOG_USER_ERROR(OB_ERR_CANT_CREATE_GEOMETRY_OBJECT);
    }
  }
  return ret;
}

CAST_FUNC_NAME(float, bit)
{
  EVAL_ARG()
  {
    float val_float = child_res->get_float();
    // 这里没必要调用SET_RES_BIT,因为ret一定为OB_SUCCESS
    res_datum.set_bit(static_cast<uint64_t>(val_float));
  }
  return ret;
}

CAST_FUNC_NAME(float, decimalint)
{
  EVAL_ARG()
  {
    float in_val = child_res->get_float();
    if (OB_UNLIKELY(isnan(in_val))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("float number invalid", K(ret), K(in_val));
    } else if (OB_UNLIKELY(isinf(in_val))) {
      ret = OB_NUMERIC_OVERFLOW;
      LOG_WARN("float number overflow", K(ret), K(in_val));
    } else {
      ObDecimalIntBuilder tmp_alloc;
      ObDecimalInt *decint = nullptr;
      int32_t val_len = 0;
      ObScale out_scale = expr.datum_meta_.scale_;
      int16_t in_scale = 0;
      ObPrecision out_prec = expr.datum_meta_.precision_;
      ObPrecision in_prec = 0;
      if (OB_FAIL(common_floating_decimalint(in_val, OB_GCVT_ARG_FLOAT, tmp_alloc, decint, val_len,
                                             in_scale, in_prec))) {
        LOG_WARN("common_floating_decimalint failed", K(ret), K(in_val));
      } else if (ObDatumCast::need_scale_decimalint(in_scale, in_prec, out_scale, out_prec)) {
        ObDecimalIntBuilder res_val;
        if (OB_FAIL(ObDatumCast::common_scale_decimalint(decint, val_len, in_scale, out_scale,
                                            out_prec, expr.extra_, res_val,
                                            ctx.exec_ctx_.get_user_logging_ctx()))) {
          LOG_WARN("scale decimalint failed", K(ret));
        } else {
          res_datum.set_decimal_int(res_val.get_decimal_int(), res_val.get_int_bytes());
        }
      } else {
        res_datum.set_decimal_int(decint, val_len);
      }
    }
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
        // 这里处理[LLONG_MIN, 0)范围内的in，转换为unsigned应该报OB_DATA_OUT_OF_RANGE。
        // out不等于0避免[-0.5, 0)内的值被误判，因为它们round后的值是0，处于合法范围内。
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
    if (isnan(in_val) && lib::is_oracle_mode()) {
      ret = OB_INVALID_NUMERIC;
      LOG_WARN("float_number failed ", K(ret), K(in_val));
    } else if (isinf(in_val) && lib::is_oracle_mode()) {
      ret = OB_NUMERIC_OVERFLOW;
      LOG_WARN("float_number failed", K(ret), K(in_val));
    } else {
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
  }
  return ret;
}

CAST_FUNC_NAME(double, decimalint)
{
  EVAL_ARG()
  {
    double in_val = child_res->get_double();
    if (isnan(in_val) && lib::is_oracle_mode()) {
      ret = OB_INVALID_NUMERIC;
      LOG_WARN("float number invalid", K(ret), K(in_val));
    } else if (isinf(in_val) && lib::is_oracle_mode()) {
      ret = OB_NUMERIC_OVERFLOW;
      LOG_WARN("float number overflow", K(ret), K(in_val));
    } else {
      ObDecimalInt *decint = nullptr;
      ObScale in_scale = 0;
      ObPrecision in_prec = 0;
      int32_t val_len = 0;
      ObDecimalIntBuilder tmp_alloc;
      ObPrecision out_prec = expr.datum_meta_.precision_;
      ObScale out_scale = expr.datum_meta_.scale_;
      if (OB_FAIL(common_floating_decimalint(in_val, OB_GCVT_ARG_DOUBLE, tmp_alloc, decint, val_len,
                                             in_scale, in_prec))) {
        LOG_WARN("common_floating_decimalint failed", K(ret), K(in_val));
      } else if (ObDatumCast::need_scale_decimalint(in_scale, in_prec, out_scale, out_prec)) {
        ObDecimalIntBuilder res_val;
        if (OB_FAIL(ObDatumCast::common_scale_decimalint(decint, val_len, in_scale, out_scale,
                                            out_prec, expr.extra_, res_val,
                                            ctx.exec_ctx_.get_user_logging_ctx()))) {
          LOG_WARN("scale decimal int failed", K(ret));
        } else {
          res_datum.set_decimal_int(res_val.get_decimal_int(), res_val.get_int_bytes());
        }
      } else {
        res_datum.set_decimal_int(decint, val_len);
      }
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
    if (OB_FAIL(common_floating_string(expr, in_val, OB_GCVT_ARG_DOUBLE,
                                                   ctx, res_datum))) {
      LOG_WARN("common_floating_string failed", K(ret), K(in_val));
    }
  }
  return ret;
}

CAST_FUNC_NAME(double, text)
{
  EVAL_ARG()
  {
    ObString res_str;
    double in_val = child_res->get_double();
    if (OB_FAIL(common_floating_string(expr, in_val, OB_GCVT_ARG_DOUBLE, ctx, res_datum))) {
      LOG_WARN("common_floating_string failed", K(ret), K(in_val));
    } else if (OB_FAIL(copy_datum_str_with_tmp_alloc(ctx, res_datum, res_str))) {
      LOG_WARN("copy datum string with tmp allocator failed", K(ret));
    } else if (OB_FAIL(common_string_text(expr, res_str, ctx, NULL, res_datum))) {
      LOG_WARN("cast string to lob failed", K(ret));
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
    if (OB_FAIL(common_floating_string(expr, in_val, OB_GCVT_ARG_DOUBLE,
                                                   ctx, res_datum))) {
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
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    ObString raw_bin;
    if (OB_FAIL(ObJsonWrapper::get_raw_binary(j_base, raw_bin, &temp_allocator))) {
      LOG_WARN("fail to get double json binary", K(ret), K(in_type), K(in_val));
    } else if (OB_FAIL(common_json_bin(expr, ctx, res_datum, raw_bin))) {
      LOG_WARN("fail to fill json bin lob locator", K(ret));
    }
  }
  return ret;
}

CAST_FUNC_NAME(double, geometry)
{
  EVAL_ARG()
  {
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    ObGeometry *geo = NULL;
    omt::ObSrsCacheGuard srs_guard;
    const ObSrsItem *srs = NULL;
    double in_val = child_res->get_double();
    char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
    ObScale scale = expr.args_[0]->datum_meta_.scale_;
    int64_t len = 0;
    ObGeoType dst_geo_type = ObGeoCastUtils::get_geo_type_from_cast_mode(expr.extra_);
    const char *cast_name = ObGeometryTypeCastUtil::get_cast_name(dst_geo_type);
    bool nan_or_inf = is_ieee754_nan_inf(in_val, buf, len);
    if (nan_or_inf) {
      LOG_DEBUG("Infinity or NaN value is", K(in_val));
    } else {
      if (0 <= scale) {
        len = ob_fcvt(in_val, scale, sizeof(buf) - 1, buf, NULL);
      } else {
        len = ob_gcvt_opt(in_val, OB_GCVT_ARG_DOUBLE, sizeof(buf) - 1,
                            buf, NULL, false, TRUE);
      }
    }
    ObString in_str(sizeof(buf), static_cast<int32_t>(len), buf);
    if (OB_FAIL(ObGeoExprUtils::get_srs_item(ctx, srs_guard, in_str, srs, true, cast_name))) {
      LOG_WARN("fail to get srs item", K(ret), K(in_str));
    } else if (OB_FAIL(ObGeoExprUtils::build_geometry(temp_allocator, in_str,
        geo, srs, cast_name))) {
      LOG_WARN("fail to parse geometry", K(ret), K(in_val), K(dst_geo_type));
    } else {
      ObString res_wkb;
      if (OB_FAIL(ObGeoTypeUtil::to_wkb(temp_allocator, *geo, srs, res_wkb))) {
        LOG_WARN("fail to get wkb", K(ret), K(dst_geo_type));
      } else if (OB_FAIL(common_gis_wkb(expr, ctx, res_datum, res_wkb))){
        LOG_WARN("fail to copy string", K(ret), K(dst_geo_type));
      }
    }

    if (OB_FAIL(ret) && CM_IS_COLUMN_CONVERT(expr.extra_) && ObGeoType::GEOMETRY == dst_geo_type) { // adapt mysql
      ret = OB_ERR_CANT_CREATE_GEOMETRY_OBJECT;
      LOG_USER_ERROR(OB_ERR_CANT_CREATE_GEOMETRY_OBJECT);
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
      const common::ObTimeZoneInfo *tz_info_local = NULL;
      ObSolidifiedVarsGetter helper(expr, ctx, session);
      if (OB_FAIL(helper.get_time_zone_info(tz_info_local))) {
        LOG_WARN("get time zone info failed", K(ret));
      } else {
        const ObTimeZoneInfo *tz_info = (ObTimestampType == in_type) ?
                                          tz_info_local : NULL;
        int64_t in_val = child_res->get_int();
        int64_t out_val = 0;
        if (OB_FAIL(ObTimeConverter::datetime_to_int(in_val, tz_info, out_val))) {
          LOG_WARN("datetime_to_int failed", K(ret), K(in_val));
        } else if (out_type < ObIntType && CAST_FAIL(int_range_check(out_type,
                                                    out_val, out_val))) {
          LOG_WARN("int_range_check failed", K(ret));
        } else {
          res_datum.set_int(out_val);
        }
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
      const common::ObTimeZoneInfo *tz_info_local = NULL;
      ObSolidifiedVarsGetter helper(expr, ctx, session);
      if (OB_FAIL(helper.get_time_zone_info(tz_info_local))) {
        LOG_WARN("get time zone info failed", K(ret));
      } else {
        const ObTimeZoneInfo *tz_info = (ObTimestampType == in_type) ?
                                          tz_info_local : NULL;
        int64_t in_val = child_res->get_int();
        int64_t val_int = 0;
        uint64_t out_val = 0;
        if (OB_FAIL(ObTimeConverter::datetime_to_int(in_val, tz_info, val_int))) {
          LOG_WARN("datetime_to_int failed", K(ret), K(in_val));
        } else {
          out_val = static_cast<uint64_t>(val_int);
          if (out_type < ObUInt64Type &&
              CAST_FAIL(uint_range_check(out_type, val_int, out_val))) {
            LOG_WARN("int_range_check failed", K(ret));
          } else {
            res_datum.set_uint(out_val);
          }
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
      const common::ObTimeZoneInfo *tz_info_local = NULL;
      ObSolidifiedVarsGetter helper(expr, ctx, session);
      if (OB_FAIL(helper.get_time_zone_info(tz_info_local))) {
        LOG_WARN("get time zone info failed", K(ret));
      } else {
        const ObTimeZoneInfo *tz_info = (ObTimestampType == in_type) ?
                                          tz_info_local : NULL;
        if (OB_FAIL(ObTimeConverter::datetime_to_double(in_val, tz_info, out_val))) {
          LOG_WARN("datetime_to_double failed", K(ret), K(in_val));
        } else {
          res_datum.set_double(out_val);
        }
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
      const common::ObTimeZoneInfo *tz_info_local = NULL;
      ObSolidifiedVarsGetter helper(expr, ctx, session);
      if (OB_FAIL(helper.get_time_zone_info(tz_info_local))) {
        LOG_WARN("get time zone info failed", K(ret));
      } else {
        const ObTimeZoneInfo *tz_info = (ObTimestampType == in_type) ?
                                          tz_info_local : NULL;
        if (OB_FAIL(ObTimeConverter::datetime_to_double(in_val, tz_info, out_val))) {
          LOG_WARN("datetime_to_double failed", K(ret), K(in_val));
        } else {
          res_datum.set_float(static_cast<float>(out_val));
        }
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
      ObPrecision res_precision; // useless
      ObScale res_scale; // useless
      ObScale in_scale = expr.args_[0]->datum_meta_.scale_;
      const common::ObTimeZoneInfo *tz_info_local = NULL;
      ObSolidifiedVarsGetter helper(expr, ctx, session);
      if (OB_FAIL(helper.get_time_zone_info(tz_info_local))) {
        LOG_WARN("get time zone info failed", K(ret));
      } else {
        const ObTimeZoneInfo *tz_info = (ObTimestampType == in_type) ?
          tz_info_local : NULL;
        if (OB_FAIL(ObTimeConverter::datetime_to_str(in_val, tz_info, nls_format,
                      in_scale, buf, sizeof(buf), len, false))) {
          LOG_WARN("failed to convert datetime to string", K(ret));
        } else if (CAST_FAIL(number.from(buf, len, tmp_alloc, &res_precision, &res_scale))) {
          LOG_WARN("failed to convert string to number", K(ret));
        } else {
          res_datum.set_number(number);
        }
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
      const common::ObTimeZoneInfo *tz_info_local = NULL;
      ObSolidifiedVarsGetter helper(expr, ctx, session);
      if (OB_FAIL(helper.get_time_zone_info(tz_info_local))) {
        LOG_WARN("get time zone info failed", K(ret));
      } else {
        if (ObDateTimeType == in_type && ObTimestampType == out_type) {
          ret = ObTimeConverter::datetime_to_timestamp(in_val,
                                                      tz_info_local,
                                                      out_val);
          ret = OB_ERR_UNEXPECTED_TZ_TRANSITION == ret ? OB_INVALID_DATE_VALUE : ret;
        } else if (ObTimestampType == in_type && ObDateTimeType == out_type) {
          ret = ObTimeConverter::timestamp_to_datetime(out_val,
                                                      tz_info_local,
                                                      out_val);
        }
        if (OB_FAIL(ret)) {
        } else {
          res_datum.set_datetime(out_val);
        }
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
      const common::ObTimeZoneInfo *tz_info_local = NULL;
      ObSolidifiedVarsGetter helper(expr, ctx, session);
      if (OB_FAIL(helper.get_time_zone_info(tz_info_local))) {
        LOG_WARN("get time zone info failed", K(ret));
      } else {
        const ObTimeZoneInfo *tz_info = (ObTimestampType == in_type) ?
                                          tz_info_local : NULL;
        if (OB_FAIL(ObTimeConverter::datetime_to_date(in_val, tz_info, out_val))) {
          LOG_WARN("datetime_to_date failed", K(ret), K(in_val));
        } else {
          res_datum.set_date(out_val);
        }
        LOG_DEBUG("in datetime date cast", K(ret), K(out_val), K(in_val));
      }
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
      const common::ObTimeZoneInfo *tz_info_local = NULL;
      ObSolidifiedVarsGetter helper(expr, ctx, session);
      if (OB_FAIL(helper.get_time_zone_info(tz_info_local))) {
        LOG_WARN("get time zone info failed", K(ret));
      } else {
        const ObTimeZoneInfo *tz_info = (ObTimestampType == in_type) ?
                                          tz_info_local : NULL;
        if (OB_FAIL(ObTimeConverter::datetime_to_time(in_val, tz_info, out_val))) {
        } else {
          res_datum.set_time(out_val);
        }
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
      const common::ObTimeZoneInfo *tz_info_local = NULL;
      ObSolidifiedVarsGetter helper(expr, ctx, session);
      if (OB_FAIL(helper.get_time_zone_info(tz_info_local))) {
        LOG_WARN("get time zone info failed", K(ret));
      } else {
        const ObTimeZoneInfo *tz_info = (ObTimestampType == in_type) ?
                                          tz_info_local : NULL;
        if (CAST_FAIL(ObTimeConverter::datetime_to_year(in_val, tz_info, out_val))) {
        } else {
          res_datum.set_year(out_val);
        }
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
    if (OB_FAIL(common_datetime_string(expr,
                                       expr.args_[0]->datum_meta_.type_,
                                       expr.datum_meta_.type_,
                                       expr.args_[0]->datum_meta_.scale_,
                                       CM_IS_FORCE_USE_STANDARD_NLS_FORMAT(expr.extra_),
                                       in_val, ctx, buf, sizeof(buf), len))) {
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

CAST_FUNC_NAME(datetime, text)
{
  EVAL_ARG()
  {
    char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
    int64_t len = 0;
    int64_t in_val = child_res->get_int();
    if (OB_FAIL(common_datetime_string(expr,
                                       expr.args_[0]->datum_meta_.type_,
                                       expr.datum_meta_.type_,
                                       expr.args_[0]->datum_meta_.scale_,
                                       CM_IS_FORCE_USE_STANDARD_NLS_FORMAT(expr.extra_),
                                       in_val, ctx, buf, sizeof(buf), len))) {
      LOG_WARN("common_datetime_string failed", K(ret));
    } else {
      ObString str(len, buf);
      if (OB_FAIL(common_copy_string_to_text_result(expr, str, ctx, res_datum))) {
        LOG_WARN("common_copy_string_to_lob failed", K(ret));
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
    if (OB_FAIL(common_datetime_string(expr,
                                       expr.args_[0]->datum_meta_.type_,
                                       expr.datum_meta_.type_,
                                       expr.args_[0]->datum_meta_.scale_,
                                       CM_IS_FORCE_USE_STANDARD_NLS_FORMAT(expr.extra_),
                                       in_val, ctx, buf, sizeof(buf), len))) {
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
      const common::ObTimeZoneInfo *tz_info_local = NULL;
      ObSolidifiedVarsGetter helper(expr, ctx, session);
      if (OB_FAIL(helper.get_time_zone_info(tz_info_local))) {
        LOG_WARN("get time zone info failed", K(ret));
      } else {
        int64_t in_val = child_res->get_int();
        ObOTimestampData out_val;
        if (OB_FAIL(ObTimeConverter::odate_to_otimestamp(in_val, tz_info_local,
                                                        out_type, out_val))) {
          LOG_WARN("fail to timestamp_to_timestamp_tz", K(ret), K(in_val),
                                                        K(in_type), K(out_type));
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

CAST_FUNC_NAME(datetime, lob)
{
  EVAL_ARG()
  {
    char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
    int64_t len = 0;
    int64_t in_val = child_res->get_int();
    if (OB_FAIL(common_datetime_string(expr,
                                       expr.args_[0]->datum_meta_.type_,
                                       ObLongTextType,
                                       expr.args_[0]->datum_meta_.scale_,
                                       CM_IS_FORCE_USE_STANDARD_NLS_FORMAT(expr.extra_),
                                       in_val, ctx, buf, sizeof(buf), len))) {
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
      const common::ObTimeZoneInfo *tz_info_local = NULL;
      ObSolidifiedVarsGetter helper(expr, ctx, session);
      if (OB_FAIL(helper.get_time_zone_info(tz_info_local))) {
        LOG_WARN("get time zone info failed", K(ret));
      } else {
        const ObTimeZoneInfo *tz_info = (ObTimestampType == in_type) ?
                                        tz_info_local : NULL;
        if (OB_FAIL(ObTimeConverter::datetime_to_ob_time(in_val, tz_info, ob_time))) {
          LOG_WARN("fail to create datetime from int failed", K(ret), K(in_type), K(in_val));
        } else {
          ObJsonNodeType node_type = (ObTimestampType == in_type)
                                      ? ObJsonNodeType::J_TIMESTAMP
                                      : ObJsonNodeType::J_DATETIME;
          ObJsonDatetime j_datetime(node_type, ob_time);
          ObIJsonBase *j_base = &j_datetime;
          ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
          common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
          ObString raw_bin;

          if (OB_FAIL(ObJsonWrapper::get_raw_binary(j_base, raw_bin, &temp_allocator))) {
            LOG_WARN("fail to get datetime json binary", K(ret), K(in_type), K(in_val));
          } else if (OB_FAIL(common_json_bin(expr, ctx, res_datum, raw_bin))) {
            LOG_WARN("fail to fill json bin lob locator", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(datetime, decimalint)
{
  EVAL_ARG()
  {
    GET_SESSION()
    {
      int64_t in_val = child_res->get_int();
      ObDecimalIntBuilder tmp_alloc;
      ObDecimalInt *decint = nullptr;
      int16_t scale = 0, in_precision = 0;
      int32_t int_bytes = 0;
      char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
      int64_t length = 0;
      ObScale in_scale = expr.args_[0]->datum_meta_.scale_;
      ObScale out_scale = expr.datum_meta_.scale_;
      ObPrecision out_prec = expr.datum_meta_.precision_;
      ObObjType in_type = expr.args_[0]->datum_meta_.type_;
      const common::ObTimeZoneInfo *tz_info_local = NULL;
      ObSolidifiedVarsGetter helper(expr, ctx, session);
      if (ObTimestampType == in_type) {
        if (OB_FAIL(helper.get_time_zone_info(tz_info_local))) {
          LOG_WARN("get time zone info failed", K(ret));
        }
      }
      ObString nls_format;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObTimeConverter::datetime_to_str(in_val, tz_info_local, nls_format, in_scale, buf,
                                                   sizeof(buf), length, false))) {
        LOG_WARN("datetime_to_str failed", K(ret), K(in_val), K(in_scale));
      } else if (OB_FAIL(wide::from_string(buf, length, tmp_alloc, scale, in_precision, int_bytes,
                                           decint))) {
        LOG_WARN("from_string failed", K(ret), K(ObString(length, buf)));
      } else if (ObDatumCast::need_scale_decimalint(in_scale, in_precision, out_scale, out_prec)) {
        ObDecimalIntBuilder res_val;
        if (OB_FAIL(ObDatumCast::common_scale_decimalint(decint, int_bytes, scale, out_scale,
                                            out_prec, expr.extra_, res_val,
                                            ctx.exec_ctx_.get_user_logging_ctx()))) {
          LOG_WARN("scale decimal int failed", K(ret), K(scale), K(out_scale));
        } else {
          res_datum.set_decimal_int(res_val.get_decimal_int(), res_val.get_int_bytes());
        }
      } else {
        res_datum.set_decimal_int(decint, int_bytes);
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(datetime, geometry)
{
  EVAL_ARG()
  {
    char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
    int64_t len = 0;
    int64_t in_val = child_res->get_int();
    if (OB_FAIL(common_datetime_string(expr,
                                       expr.args_[0]->datum_meta_.type_,
                                       expr.datum_meta_.type_,
                                       expr.args_[0]->datum_meta_.scale_,
                                       CM_IS_FORCE_USE_STANDARD_NLS_FORMAT(expr.extra_),
                                       in_val, ctx, buf, sizeof(buf), len))) {
      LOG_WARN("common_datetime_string failed", K(ret));
    } else {
      ObString in_str(sizeof(buf), static_cast<int32_t>(len), buf);
      ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
      common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
      ObGeometry *geo = NULL;
      omt::ObSrsCacheGuard srs_guard;
      const ObSrsItem *srs = NULL;
      ObGeoType dst_geo_type = ObGeoCastUtils::get_geo_type_from_cast_mode(expr.extra_);
      const char *cast_name = ObGeometryTypeCastUtil::get_cast_name(dst_geo_type);
      if (OB_FAIL(ObGeoExprUtils::get_srs_item(ctx, srs_guard, in_str, srs, true, cast_name))) {
        LOG_WARN("fail to get srs item", K(ret), K(in_str));
      } else if (OB_FAIL(ObGeoExprUtils::build_geometry(temp_allocator, in_str,
          geo, srs, cast_name))) {
        LOG_WARN("fail to parse geometry", K(ret), K(in_val), K(dst_geo_type));
      } else {
        ObString res_wkb;
        if (OB_FAIL(ObGeoTypeUtil::to_wkb(temp_allocator, *geo, srs, res_wkb))) {
          LOG_WARN("fail to get wkb", K(ret), K(dst_geo_type));
        } else if (OB_FAIL(common_gis_wkb(expr, ctx, res_datum, res_wkb))){
          LOG_WARN("fail to copy string", K(ret), K(dst_geo_type));
        }
      }

      if (OB_FAIL(ret) && CM_IS_COLUMN_CONVERT(expr.extra_) && ObGeoType::GEOMETRY == dst_geo_type) { // adapt mysql
        ret = OB_ERR_CANT_CREATE_GEOMETRY_OBJECT;
        LOG_USER_ERROR(OB_ERR_CANT_CREATE_GEOMETRY_OBJECT);
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
    int32_t in_val = child_res->get_date();
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
    int32_t in_val = child_res->get_date();
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
      const common::ObTimeZoneInfo *tz_info_local = NULL;
      ObSolidifiedVarsGetter helper(expr, ctx, session);
      if (OB_FAIL(helper.get_time_zone_info(tz_info_local))) {
        LOG_WARN("get time zone info failed", K(ret));
      } else {
        ObTimeConvertCtx cvrt_ctx(tz_info_local, ObTimestampType == out_type);
        int32_t in_val = child_res->get_date();
        int64_t out_val = 0;
        if (OB_FAIL(ObTimeConverter::date_to_datetime(in_val, cvrt_ctx, out_val))) {
          LOG_WARN("date_to_datetime failed", K(ret), K(in_val));
        } else {
          res_datum.set_datetime(out_val);
        }
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
    int32_t in_val = child_res->get_date();
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

CAST_FUNC_NAME(date, text)
{
  EVAL_ARG()
  {
    int32_t in_val = child_res->get_date();
    char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
    int64_t len = 0;
    if (OB_FAIL(ObTimeConverter::date_to_str(in_val, buf, sizeof(buf), len))) {
      LOG_WARN("date_to_str failed", K(ret));
    } else {
      ObString in_str(len, buf);
      if (OB_FAIL(common_copy_string_to_text_result(expr, in_str, ctx, res_datum))) {
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
    int32_t in_val = child_res->get_date();
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
    if (lib::is_oracle_mode() && in_val != 0) {
      if (CM_IS_IMPLICIT_CAST(expr.extra_)) {
        ret = OB_ERR_INVALID_INPUT;
        LOG_WARN("invalid input in implicit cast", K(ret));
      } else {
        LOG_WARN("inconsistent datatypes", K(ret), K(in_type), K("json"), K(expr.extra_));
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
      }
    } else {
      ObTime ob_time(DT_TYPE_DATE);
      if (OB_FAIL(ObTimeConverter::date_to_ob_time(in_val, ob_time))) {
        LOG_WARN("fail to create ob time from date failed", K(ret), K(in_type), K(in_val));
      } else {
        ObJsonDatetime j_date(ObJsonNodeType::J_DATE, ob_time);
        ObIJsonBase *j_base = &j_date;
        ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
        common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
        ObString raw_bin;

        if (OB_FAIL(ObJsonWrapper::get_raw_binary(j_base, raw_bin, &temp_allocator))) {
          LOG_WARN("fail to get date json binary", K(ret), K(in_type), K(in_val));
        } else if (OB_FAIL(common_json_bin(expr, ctx, res_datum, raw_bin))) {
          LOG_WARN("fail to fill json bin lob locator", K(ret));
        }
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(date, decimalint)
{
  EVAL_ARG()
  {
    DEF_IN_OUT_VAL(int32_t, int64_t, 0);
    ObDecimalIntBuilder tmp_alloc;
    ObDecimalInt *decint = nullptr;
    int32_t val_len = 0;
    ObScale out_scale = expr.datum_meta_.scale_;
    ObPrecision out_precision = expr.datum_meta_.precision_;
    ObScale in_scale = 0;
    ObPrecision out_prec = expr.datum_meta_.precision_;
    if (OB_FAIL(ObTimeConverter::date_to_int(in_val, out_val))) {
      LOG_WARN("date to in failed", K(ret), K(in_val));
    } else if (CAST_FAIL(numeric_negative_check(out_val))) {
      LOG_WARN("numeric_negative_check failed", K(ret), K(out_val));
    } else if (OB_FAIL(wide::from_integer(out_val, tmp_alloc, decint, val_len))){
      LOG_WARN("from_integer failed", K(ret), K(out_val));
    } else if (ObDatumCast::need_scale_decimalint(
                 in_scale, val_len, out_scale,
                 wide::ObDecimalIntConstValue::get_int_bytes_by_precision(out_prec))) {
      ObDecimalIntBuilder res_val;
      if (OB_FAIL(ObDatumCast::common_scale_decimalint(decint, val_len, in_scale, out_scale, out_prec,
                                          expr.extra_, res_val))) {
        LOG_WARN("failed to scale decimal int", K(ret));
      } else {
        res_datum.set_decimal_int(res_val.get_decimal_int(), res_val.get_int_bytes());
      }
    } else {
      res_datum.set_decimal_int(decint, val_len);
    }
  }
  return ret;
}

CAST_FUNC_NAME(date, geometry)
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
      ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
      common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
      ObGeometry *geo = NULL;
      omt::ObSrsCacheGuard srs_guard;
      const ObSrsItem *srs = NULL;
      ObGeoType dst_geo_type = ObGeoCastUtils::get_geo_type_from_cast_mode(expr.extra_);
      const char *cast_name = ObGeometryTypeCastUtil::get_cast_name(dst_geo_type);
      if (OB_FAIL(ObGeoExprUtils::get_srs_item(ctx, srs_guard, in_str, srs, true, cast_name))) {
        LOG_WARN("fail to get srs item", K(ret), K(in_str));
      } else if (OB_FAIL(ObGeoExprUtils::build_geometry(temp_allocator, in_str,
          geo, srs, cast_name))) {
        LOG_WARN("fail to parse geometry", K(ret), K(in_val), K(dst_geo_type));
      } else {
        ObString res_wkb;
        if (OB_FAIL(ObGeoTypeUtil::to_wkb(temp_allocator, *geo, srs, res_wkb))) {
          LOG_WARN("fail to get wkb", K(ret), K(dst_geo_type));
        } else if (OB_FAIL(common_gis_wkb(expr, ctx, res_datum, res_wkb))){
          LOG_WARN("fail to copy string", K(ret), K(dst_geo_type));
        }
      }

      if (OB_FAIL(ret) && CM_IS_COLUMN_CONVERT(expr.extra_) && ObGeoType::GEOMETRY == dst_geo_type) { // adapt mysql
        ret = OB_ERR_CANT_CREATE_GEOMETRY_OBJECT;
        LOG_USER_ERROR(OB_ERR_CANT_CREATE_GEOMETRY_OBJECT);
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(year, int)
{
  EVAL_ARG()
  {
    uint8_t in_val = child_res->get_year();
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
    uint8_t in_val = child_res->get_year();
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
    uint8_t in_val = child_res->get_year();
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
    uint8_t in_val = child_res->get_year();
    int64_t val_int = 0;
    ObNumStackOnceAlloc tmp_alloc;
    number::ObNumber number;
    if (OB_FAIL(common_year_int(expr, ObIntType, in_val, val_int))) {
      LOG_WARN("common_year_int failed", K(ret), K(in_val));
    } else if (OB_FAIL(common_int_number(expr, val_int, tmp_alloc, number))) {
      LOG_WARN("common_int_number failed",  K(ret), K(val_int));
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
    uint8_t in_val = child_res->get_year();
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

CAST_FUNC_NAME(year, text)
{
  EVAL_ARG()
  {
    char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
    int64_t len = 0;
    uint8_t in_val = child_res->get_year();
    if (OB_FAIL(ObTimeConverter::year_to_str(in_val, buf, sizeof(buf), len))) {
      LOG_WARN("year_to_str failed", K(ret), K(in_val));
    } else {
      ObString in_str(len, buf);
      if (OB_FAIL(common_copy_string_to_text_result(expr, in_str, ctx, res_datum))) {
        LOG_WARN("common_copy_string_to_text_result failed", K(ret));
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(year, datetime)
{
  EVAL_ARG()
  {
    uint8_t in_val = child_res->get_year();
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
    uint8_t in_val = child_res->get_year();
    int64_t val_int = 0;
    if (OB_FAIL(common_year_int(expr, ObIntType, in_val, val_int))) {
      LOG_WARN("common_year_int failed", K(ret), K(in_val));
    } else if (OB_FAIL(common_int_date(expr, val_int, res_datum))) {
      LOG_WARN("common_int_date failed", K(ret), K(val_int));
    }
  }
  return ret;
}

CAST_FUNC_NAME(year, time)
{
  EVAL_ARG()
  {
    int64_t year_int = 0;
    uint8_t in_val = child_res->get_year();
    if (OB_FAIL(ObTimeConverter::year_to_int(in_val, year_int))) {
      LOG_WARN("year_to_int failed", K(ret), K(in_val));
    } else if (OB_FAIL(common_int_time(expr, year_int, res_datum))) {
      LOG_WARN("int to time failed", K(ret));
    }
  }
  return ret;
}

CAST_FUNC_NAME(year, bit)
{
  EVAL_ARG()
  {
    int64_t year_int = 0;
    uint8_t in_val = child_res->get_year();
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
      ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
      common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
      ObString raw_bin;
      
      if (OB_FAIL(ObJsonWrapper::get_raw_binary(j_base, raw_bin, &temp_allocator))) {
        LOG_WARN("fail to get year json binary", K(ret), K(in_type), K(in_val));
      } else if (OB_FAIL(common_json_bin(expr, ctx, res_datum, raw_bin))) {
        LOG_WARN("fail to fill json bin lob locator", K(ret));
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(year, decimalint)
{
  EVAL_ARG()
  {
    uint8_t year = child_res->get_year();
    int64_t in_val = 0;
    ObDecimalIntBuilder tmp_alloc;
    ObDecimalInt *decint = nullptr;
    int32_t val_len;
    ObScale out_scale = expr.datum_meta_.scale_;
    ObScale in_scale = 0;
    ObPrecision out_prec = expr.datum_meta_.precision_;
    int32_t out_bytes = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(out_prec);
    if (OB_FAIL(common_year_int(expr, ObIntType, year, in_val))) {
      LOG_WARN("common_year_int faile", K(ret), K(year));
    } else if (OB_FAIL(wide::from_integer(in_val, tmp_alloc, decint, val_len))) {
      LOG_WARN("from_integer failed", K(ret), K(in_val));
    } else if (ObDatumCast::need_scale_decimalint(in_scale, val_len, out_scale, out_bytes)) {
      ObDecimalIntBuilder res_val;
      if (OB_FAIL(ObDatumCast::common_scale_decimalint(decint, val_len, in_scale, out_scale, out_prec,
                                          expr.extra_, res_val))) {
        LOG_WARN("scale decimal int failed", K(ret));
      } else {
        res_datum.set_decimal_int(res_val.get_decimal_int(), res_val.get_int_bytes());
      }
    } else {
      res_datum.set_decimal_int(decint, val_len);
    }
  }
  return ret;
}

CAST_FUNC_NAME(year, geometry)
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
      ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
      common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
      ObGeometry *geo = NULL;
      omt::ObSrsCacheGuard srs_guard;
      const ObSrsItem *srs = NULL;
      ObGeoType dst_geo_type = ObGeoCastUtils::get_geo_type_from_cast_mode(expr.extra_);
      const char *cast_name = ObGeometryTypeCastUtil::get_cast_name(dst_geo_type);
      if (OB_FAIL(ObGeoExprUtils::get_srs_item(ctx, srs_guard, in_str, srs, true, cast_name))) {
        LOG_WARN("fail to get srs item", K(ret), K(in_str));
      } else if (OB_FAIL(ObGeoExprUtils::build_geometry(temp_allocator, in_str,
          geo, srs, cast_name))) {
        LOG_WARN("fail to parse geometry", K(ret), K(in_val), K(dst_geo_type));
      } else {
        ObString res_wkb;
        if (OB_FAIL(ObGeoTypeUtil::to_wkb(temp_allocator, *geo, srs, res_wkb))) {
          LOG_WARN("fail to get wkb", K(ret), K(dst_geo_type));
        } else if (OB_FAIL(common_gis_wkb(expr, ctx, res_datum, res_wkb))){
          LOG_WARN("fail to copy string", K(ret), K(dst_geo_type));
        }
      }

      if (OB_FAIL(ret) && CM_IS_COLUMN_CONVERT(expr.extra_) && ObGeoType::GEOMETRY == dst_geo_type) { // adapt mysql
        ret = OB_ERR_CANT_CREATE_GEOMETRY_OBJECT;
        LOG_USER_ERROR(OB_ERR_CANT_CREATE_GEOMETRY_OBJECT);
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
    if (out_type < ObIntType
        && CM_NEED_RANGE_CHECK(expr.extra_)
        && CAST_FAIL(int_range_check(out_type, in_val, out_val))) {
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
    if (out_type < ObUInt64Type
        && CM_NEED_RANGE_CHECK(expr.extra_)
        && CAST_FAIL(uint_upper_check(out_type, out_val))) {
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

CAST_FUNC_NAME(bit, datetime)
{
  EVAL_ARG()
  {
    GET_SESSION()
    {
      DEF_IN_OUT_VAL(uint64_t, int64_t, 0);
      ObDateSqlMode date_sql_mode;
      date_sql_mode.allow_invalid_dates_ = CM_IS_ALLOW_INVALID_DATES(expr.extra_);
      date_sql_mode.no_zero_date_ = CM_IS_NO_ZERO_DATE(expr.extra_);
      if (CM_IS_COLUMN_CONVERT(expr.extra_)) {
        // if cast mode is column convert, using bit as int64 to do cast.
        int64_t int64 = 0;
        if (OB_FAIL(common_uint_int(expr, ObIntType, in_val, ctx, int64))) {
          LOG_WARN("common_uint_int failed", K(ret));
        } else if (OB_UNLIKELY(0 > int64)) {
          ret = OB_INVALID_DATE_FORMAT;
          LOG_WARN("invalid date", K(ret), K(int64));
        } else {
          const common::ObTimeZoneInfo *tz_info_local = NULL;
          ObSolidifiedVarsGetter helper(expr, ctx, session);
          if (OB_FAIL(helper.get_time_zone_info(tz_info_local))) {
            LOG_WARN("get time zone info failed", K(ret));
          } else {
            ObTimeConvertCtx cvrt_ctx(tz_info_local,
                                      ObTimestampType == expr.datum_meta_.type_);
            if (CAST_FAIL(ObTimeConverter::int_to_datetime(int64, 0, cvrt_ctx, out_val,
                          date_sql_mode))) {
              LOG_WARN("int_datetime failed", K(ret), K(int64));
            }
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
          const common::ObTimeZoneInfo *tz_info_local = NULL;
          ObSolidifiedVarsGetter helper(expr, ctx, session);
          if (OB_FAIL(helper.get_time_zone_info(tz_info_local))) {
            LOG_WARN("get time zone info failed", K(ret));
          } else {
            ObTimeConvertCtx cvrt_ctx(tz_info_local, ObTimestampType == out_type);
            ObScale res_scale;
            if (CAST_FAIL(ObTimeConverter::str_to_datetime(str, cvrt_ctx, out_val, &res_scale,
                          date_sql_mode))) {
              LOG_WARN("str_to_datetime failed", K(ret));
            }
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
    ObDateSqlMode date_sql_mode;
    date_sql_mode.allow_invalid_dates_ = CM_IS_ALLOW_INVALID_DATES(expr.extra_);
    date_sql_mode.no_zero_date_ = CM_IS_NO_ZERO_DATE(expr.extra_);
    if (CM_IS_COLUMN_CONVERT(expr.extra_)) {
      // if cast mode is column convert, using bit as int64 to do cast.
      int64_t int64 = 0;
      if (OB_FAIL(common_uint_int(expr, ObIntType, in_val, ctx, int64))) {
        LOG_WARN("common_uint_int failed", K(ret));
      } else if (CAST_FAIL(ObTimeConverter::int_to_date(int64, out_val, date_sql_mode))) {
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
        if (CAST_FAIL(ObTimeConverter::str_to_date(str, out_val, date_sql_mode))) {
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
      if (OB_FAIL(common_copy_string_zf(expr, ObString(ffi.length(), ffi.ptr()),
                                        ctx, res_datum))) {
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
        bool has_set_res = false;
        if (OB_FAIL(common_check_convert_string(expr, ctx, str, res_datum, has_set_res))) {
          LOG_WARN("common_check_convert_string failed", K(ret));
        }
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(bit, text)
{
  EVAL_ARG()
  {
    uint64_t in_val = child_res->get_uint();
    if (CM_IS_COLUMN_CONVERT(expr.extra_)) {
      // if cast mode is column convert, using bit as int64 to do cast.
      ObFastFormatInt ffi(in_val);
      ObString res_str(ffi.length(), ffi.ptr());
      if (OB_FAIL(common_copy_string_zf_to_text_result(expr, res_str, ctx, res_datum))) {
        LOG_WARN("common_copy_string_zf_to_text_result failed", K(ret), K(res_str));
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
        ObString res_str;
        bool has_set_res = false;
        if (OB_FAIL(common_check_convert_string(expr, ctx, str, res_datum, has_set_res))) {
          LOG_WARN("common_check_convert_string failed", K(ret));
        } else if (OB_FAIL(copy_datum_str_with_tmp_alloc(ctx, res_datum, res_str))) {
          LOG_WARN("copy datum string with tmp allocator failed", K(ret));
        } else if (OB_FAIL(common_string_text(expr, res_str, ctx, NULL, res_datum))) {
          LOG_WARN("cast string to lob failed", K(ret));
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
      ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
      common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
      common::ObString j_value(pos, buf);
      ObJsonOpaque j_opaque(j_value, ObBitType);
      ObIJsonBase *j_base = &j_opaque;
      ObString raw_bin;

      if (OB_FAIL(ObJsonWrapper::get_raw_binary(j_base, raw_bin, &temp_allocator))) {
        LOG_WARN("fail to get int json binary", K(ret), K(in_val), K(buf), K(BUF_LEN));
      } else if (OB_FAIL(common_json_bin(expr, ctx, res_datum, raw_bin))) {
        LOG_WARN("fail to fill json bin lob locator", K(ret));
      }
    }
  }

  return ret;
}

CAST_FUNC_NAME(bit, decimalint)
{
  EVAL_ARG()
  {
    uint64_t in_val = child_res->get_uint();
    ObDecimalIntBuilder tmp_alloc;
    ObDecimalInt *decint = nullptr;
    int32_t val_len = 0;
    int16_t in_scale = 0;
    ObScale out_scale = expr.datum_meta_.scale_;
    ObPrecision out_prec = expr.datum_meta_.precision_;
    int32_t out_bytes = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(out_prec);
    if (OB_FAIL(wide::from_integer(in_val, tmp_alloc, decint, val_len))) {
      LOG_WARN("from_integer failed", K(ret), K(in_val));
    } else if (ObDatumCast::need_scale_decimalint(in_scale, val_len, out_scale, out_bytes)) {
      ObDecimalIntBuilder res_val;
      if (OB_FAIL(ObDatumCast::common_scale_decimalint(decint, val_len, in_scale, out_scale, out_prec,
                                          expr.extra_, res_val))) {
        LOG_WARN("scale decimal int failed", K(ret));
      } else {
        res_datum.set_decimal_int(res_val.get_decimal_int(), res_val.get_int_bytes());
      }
    } else {
      res_datum.set_decimal_int(decint, val_len);
    }
  }
  return ret;
}

CAST_FUNC_NAME(bit, geometry)
{
  EVAL_ARG()
  {
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    ObGeometry *geo = NULL;
    omt::ObSrsCacheGuard srs_guard;
    const ObSrsItem *srs = NULL;
    uint64_t in_val = child_res->get_uint();
    ObFastFormatInt ffi(in_val);
    ObGeoType dst_geo_type = ObGeoCastUtils::get_geo_type_from_cast_mode(expr.extra_);
    const char *cast_name = ObGeometryTypeCastUtil::get_cast_name(dst_geo_type);
    ObString str(ffi.length(), ffi.ptr());
    if (OB_FAIL(ObGeoExprUtils::get_srs_item(ctx, srs_guard, str, srs, true, cast_name))) {
      LOG_WARN("fail to get srs item", K(ret), K(str));
    } else if (OB_FAIL(ObGeoExprUtils::build_geometry(temp_allocator, str, geo, srs, cast_name))) {
      LOG_WARN("fail to parse geometry", K(ret), K(in_val), K(dst_geo_type));
    } else {
      ObString res_wkb;
      if (OB_FAIL(ObGeoTypeUtil::to_wkb(temp_allocator, *geo, srs, res_wkb))) {
        LOG_WARN("fail to get wkb", K(ret), K(dst_geo_type));
      } else if (OB_FAIL(common_gis_wkb(expr, ctx, res_datum, res_wkb))){
        LOG_WARN("fail to copy string", K(ret), K(dst_geo_type));
      }
    }

    if (OB_FAIL(ret) && CM_IS_COLUMN_CONVERT(expr.extra_) && ObGeoType::GEOMETRY == dst_geo_type) { // adapt mysql
      ret = OB_ERR_CANT_CREATE_GEOMETRY_OBJECT;
      LOG_USER_ERROR(OB_ERR_CANT_CREATE_GEOMETRY_OBJECT);
    }
  }
  return ret;
}

CAST_FUNC_NAME(enumset, int)
{
  EVAL_ARG() {
    uint64_t in_val = child_res->get_enumset();
    int64_t out_val = 0;
    ObObjType out_type = expr.datum_meta_.type_;
    if (OB_FAIL(common_uint_int(expr, out_type, in_val, ctx, out_val))) {
      LOG_WARN("common_uint_int failed", K(ret));
    } else {
      res_datum.set_int(out_val);
    }
  }
  return ret;
}

CAST_FUNC_NAME(enumset, uint)
{
  EVAL_ARG() {
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
  EVAL_ARG() {
    uint64_t in_val = child_res->get_enumset();
    res_datum.set_float(static_cast<float>(in_val));
  }
  return ret;
}

CAST_FUNC_NAME(enumset, double)
{
  EVAL_ARG() {
    uint64_t in_val = child_res->get_enumset();
    res_datum.set_double(static_cast<double>(in_val));
  }
  return ret;
}

CAST_FUNC_NAME(enumset, number)
{
  EVAL_ARG() {
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
  EVAL_ARG() {
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
  EVAL_ARG() {
    uint64_t in_val = child_res->get_enumset();
    if (OB_FAIL(common_uint_bit(expr, in_val, ctx, res_datum))) {
      LOG_WARN("fail to common_uint_bit", K(ret));
    }
  }
  return ret;
}

CAST_FUNC_NAME(enumset, decimalint)
{
  EVAL_ARG()
  {
    uint64_t in_val = child_res->get_uint64();
    ObDecimalIntBuilder tmp_alloc;
    ObDecimalInt *decint = nullptr;
    int32_t val_len = 0;
    ObScale out_scale = expr.datum_meta_.scale_;
    ObScale in_scale = 0;
    ObPrecision out_prec = expr.datum_meta_.precision_;
    int32_t out_bytes = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(out_prec);
    if (OB_FAIL(wide::from_integer(in_val, tmp_alloc, decint, val_len))) {
      LOG_WARN("from_integer failed", K(ret), K(in_val));
    } else if (ObDatumCast::need_scale_decimalint(in_scale, val_len, out_scale, out_bytes)) {
      ObDecimalIntBuilder res_val;
      if (OB_FAIL(ObDatumCast::common_scale_decimalint(decint, val_len, in_scale, out_scale, out_prec,
                                          expr.extra_, res_val))) {
        LOG_WARN("scale decimal int failed", K(ret));
      } else {
        res_datum.set_decimal_int(res_val.get_decimal_int(), res_val.get_int_bytes());
      }
    } else {
      res_datum.set_decimal_int(decint, val_len);
    }
  }
  return ret;
}

CAST_FUNC_NAME(enumset_inner, int)
{
  EVAL_ARG() {
    ObEnumSetInnerValue inner_value;
    int64_t out_val = 0;
    ObObjType out_type = expr.datum_meta_.type_;
    if (OB_FAIL(child_res->get_enumset_inner(inner_value))) {
      LOG_WARN("failed to inner_value", KPC(child_res), K(ret));
    } else if (OB_FAIL(common_uint_int(expr, out_type, inner_value.numberic_value_,
                                       ctx, out_val))) {
      LOG_WARN("common_uint_int failed", K(ret));
    } else {
      res_datum.set_int(out_val);
    }
  }
  return ret;
}

CAST_FUNC_NAME(enumset_inner, uint)
{
  EVAL_ARG() {
    DEF_IN_OUT_TYPE();
    ObEnumSetInnerValue inner_value;
    if (OB_FAIL(child_res->get_enumset_inner(inner_value))) {
      LOG_WARN("failed to inner_value", KPC(child_res), K(ret));
    } else if (CAST_FAIL(uint_upper_check(out_type, inner_value.numberic_value_))) {
      LOG_WARN("int_upper_check failed", K(ret), K(inner_value.numberic_value_));
    } else {
      res_datum.set_uint(inner_value.numberic_value_);
    }
  }
  return ret;
}

CAST_FUNC_NAME(enumset_inner, float)
{
  EVAL_ARG() {
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
  EVAL_ARG() {
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
  EVAL_ARG() {
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
  EVAL_ARG() {
    ObEnumSetInnerValue inner_value;
    int64_t val_int = 0;
    if (OB_FAIL(child_res->get_enumset_inner(inner_value))) {
      LOG_WARN("failed to inner_value", KPC(child_res), K(ret));
    } else if (OB_FAIL(common_uint_int(expr, ObIntType, inner_value.numberic_value_,
                                       ctx, val_int))) {
      LOG_WARN("common_uint_int failed", K(ret), K(inner_value.numberic_value_));
    } else if (OB_FAIL(common_int_year(expr, val_int, res_datum))) {
      LOG_WARN("common_int_time failed", K(ret), K(val_int));
    }
  }
  return ret;
}


CAST_FUNC_NAME(enumset_inner, bit)
{
  EVAL_ARG() {
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
  EVAL_ARG() {
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
  EVAL_ARG() {
    ObEnumSetInnerValue inner_value;
    if (OB_FAIL(child_res->get_enumset_inner(inner_value))) {
      LOG_WARN("failed to inner_value", KPC(child_res), K(ret));
    } else if (OB_FAIL(common_string_date(
                       expr, inner_value.string_value_, res_datum))) {
      LOG_WARN("failed to common_string_date", K(inner_value), K(ret));
    }
  }
  return ret;
}

CAST_FUNC_NAME(enumset_inner, time)
{
  EVAL_ARG() {
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
  EVAL_ARG() {
    ObEnumSetInnerValue inner_value;
    bool has_set_res = false;
    if (OB_FAIL(child_res->get_enumset_inner(inner_value))) {
      LOG_WARN("failed to inner_value", KPC(child_res), K(ret));
    } else if (OB_FAIL(common_check_convert_string(expr, ctx, inner_value.string_value_, res_datum, has_set_res))) {
      LOG_WARN("fail to common_check_convert_string", K(ret), K(inner_value));
    }
  }
  return ret;
}

CAST_FUNC_NAME(enumset_inner, decimalint)
{
  EVAL_ARG()
  {
    ObEnumSetInnerValue inner_value;
    ObDecimalIntBuilder tmp_alloc;
    ObDecimalInt *decint = nullptr;
    int32_t val_len = 0;
    ObScale out_scale = expr.datum_meta_.scale_;
    ObScale in_scale = 0;
    ObPrecision out_prec = expr.datum_meta_.precision_;
    int32_t out_bytes = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(out_prec);
    if (OB_FAIL(child_res->get_enumset_inner(inner_value))) {
      LOG_WARN("get inner value failed", K(ret));
    } else if (OB_FAIL(
                 wide::from_integer(inner_value.numberic_value_, tmp_alloc, decint, val_len))) {
      LOG_WARN("from_integer failed", K(ret), K(inner_value));
    } else if (ObDatumCast::need_scale_decimalint(in_scale, val_len, out_scale, out_bytes)) {
      ObDecimalIntBuilder res_val;
      if (OB_FAIL(ObDatumCast::common_scale_decimalint(decint, val_len, in_scale, out_scale, out_prec,
                                          expr.extra_, res_val))) {
        LOG_WARN("scale decimal int failed", K(ret));
      } else {
        res_datum.set_decimal_int(res_val.get_decimal_int(), res_val.get_int_bytes());
      }
    } else {
      res_datum.set_decimal_int(decint, val_len);
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
      if (CM_NEED_RANGE_CHECK(expr.extra_) &&
          CAST_FAIL(uint_range_check(out_type, val_int, out_val))) {
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
      ObPhysicalPlanCtx *phy_plan_ctx = ctx.exec_ctx_.get_physical_plan_ctx();
      int64_t cur_time = phy_plan_ctx ? phy_plan_ctx->get_cur_time().get_datetime() : 0;
      const common::ObTimeZoneInfo *tz_info_local = NULL;
      ObSolidifiedVarsGetter helper(expr, ctx, session);
      if (OB_FAIL(helper.get_time_zone_info(tz_info_local))) {
        LOG_WARN("get time zone info failed", K(ret));
      } else {
        if (OB_FAIL(ObTimeConverter::time_to_datetime(in_val, cur_time, tz_info_local,
                                                      out_val, out_type))) {
          LOG_WARN("time_to_datetime failed", K(ret), K(in_val));
        } else {
          res_datum.set_datetime(out_val);
        }
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
      } else if (OB_FAIL(ObTimeConverter::datetime_to_date(datetime_value, NULL, out_val))) {
        LOG_WARN("date to datetime failed", K(ret), K(datetime_value));
      } else {
        res_datum.set_date(out_val);
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(time, year)
{
  EVAL_ARG()
  {
    int warning = OB_SUCCESS;
    int64_t in_val = child_res->get_time();
    int64_t int_val = 0;
    uint8_t year_val = 0;
    if (OB_FAIL(ObTimeConverter::time_to_int(in_val, int_val))) {
      LOG_WARN("time_to_int failed", K(ret), K(in_val));
    } else if (CAST_FAIL(ObTimeConverter::int_to_year(int_val, year_val))) {
      LOG_WARN("cast int to year failed", K(ret), K(int_val));
    } else {
      SET_RES_YEAR(year_val);
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
    } else if (OB_FAIL(common_copy_string(expr, ObString(len, buf),
                                                      ctx, res_datum))) {
      LOG_WARN("common_copy_string failed", K(ret), K(ObString(len, buf)));
    }
  }
  return ret;
}

CAST_FUNC_NAME(time, text)
{
  EVAL_ARG()
  {
    int64_t in_val = child_res->get_int();
    char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
    int64_t len = 0;
    ObScale in_scale = expr.args_[0]->datum_meta_.scale_;
    if (OB_FAIL(ObTimeConverter::time_to_str(in_val, in_scale, buf, sizeof(buf), len))) {
      LOG_WARN("time_to_str failed", K(ret), K(in_val));
    } else {
      ObString res_str(len, buf);
      if (OB_FAIL(common_copy_string_to_text_result(expr, res_str, ctx, res_datum))) {
        LOG_WARN("common_copy_string_to_text_result failed", K(ret), K(ObString(len, buf)));
      }
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
    if (OB_FAIL(ObTimeConverter::time_to_str(in_val, in_scale,
                                                         buf, sizeof(buf), len))) {
      LOG_WARN("time_to_str failed", K(ret), K(in_val));
    } else if (OB_FAIL(common_string_bit(expr, ObString(len, buf),
                                                     ctx, res_datum))) {
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
    if (lib::is_oracle_mode() && in_val != 0) {
      if (CM_IS_IMPLICIT_CAST(expr.extra_)) {
        ret = OB_ERR_INVALID_INPUT;
        LOG_WARN("invalid input in implicit cast", K(ret));
      } else {
        LOG_WARN("inconsistent datatypes", K(ret), K(in_type), K("json"), K(expr.extra_));
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
      }
    } else {
      ObTime ob_time(DT_TYPE_TIME);
      if (OB_FAIL(ObTimeConverter::time_to_ob_time(in_val, ob_time))) {
        LOG_WARN("fail to create ob time from time", K(ret), K(in_type), K(in_val));
      } else {
        ObJsonDatetime j_time(ObJsonNodeType::J_TIME, ob_time);
        ObIJsonBase *j_base = &j_time;
        ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
        common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
        ObString raw_bin;

        if (OB_FAIL(ObJsonWrapper::get_raw_binary(j_base, raw_bin, &temp_allocator))) {
          LOG_WARN("fail to get time json binary", K(ret), K(in_type), K(in_val));
        } else if (OB_FAIL(common_json_bin(expr, ctx, res_datum, raw_bin))) {
          LOG_WARN("fail to fill json bin lob locator", K(ret));
        }
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(time, decimalint)
{
  EVAL_ARG()
  {
    int64_t in_val = child_res->get_int();
    ObScale tmp_scale = expr.args_[0]->datum_meta_.scale_;
    ObScale out_scale = expr.datum_meta_.scale_;
    char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
    int64_t length = 0;
    ObDecimalIntBuilder tmp_alloc;
    ObDecimalInt *decint = nullptr;
    int32_t int_bytes = 0;
    int16_t in_scale = 0;
    int16_t in_prec = 0;
    ObPrecision out_prec = expr.datum_meta_.precision_;
    if (OB_FAIL(ObTimeConverter::time_to_str(in_val, tmp_scale, buf, sizeof(buf), length, false))) {
      LOG_WARN("time_to_str failed", K(ret), K(in_val), K(tmp_scale));
    } else if (OB_FAIL(
                 wide::from_string(buf, length, tmp_alloc, in_scale, in_prec, int_bytes, decint))) {
      LOG_WARN("from_string failed", K(ret), K(ObString(length, buf)));
    } else if (ObDatumCast::need_scale_decimalint(in_scale, in_prec, out_scale, out_prec)) {
      ObDecimalIntBuilder res_val;
      if (OB_FAIL(ObDatumCast::common_scale_decimalint(decint, int_bytes, in_scale, out_scale, out_prec,
                                          expr.extra_, res_val,
                                          ctx.exec_ctx_.get_user_logging_ctx()))) {
        LOG_WARN("scale decimal int failed", K(ret));
      } else {
        res_datum.set_decimal_int(res_val.get_decimal_int(), res_val.get_int_bytes());
      }
    } else {
      res_datum.set_decimal_int(decint, int_bytes);
    }
  }
  return ret;
}

CAST_FUNC_NAME(time, geometry)
{
  EVAL_ARG()
  {
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    ObGeometry *geo = NULL;
    omt::ObSrsCacheGuard srs_guard;
    const ObSrsItem *srs = NULL;
    ObGeoType dst_geo_type = ObGeoCastUtils::get_geo_type_from_cast_mode(expr.extra_);
    const char *cast_name = ObGeometryTypeCastUtil::get_cast_name(dst_geo_type);
    int64_t in_val = child_res->get_int();
    char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
    int64_t len = 0;
    ObScale in_scale = expr.args_[0]->datum_meta_.scale_;
    if (OB_FAIL(ObTimeConverter::time_to_str(in_val, in_scale, buf, sizeof(buf), len))) {
      LOG_WARN("time_to_str failed", K(ret), K(in_val));
    } else if (OB_FAIL(ObGeoExprUtils::get_srs_item(ctx, srs_guard, ObString(len, buf), srs,
        true, cast_name))) {
      LOG_WARN("fail to get srs item", K(ret));
    } else if (OB_FAIL(ObGeoExprUtils::build_geometry(temp_allocator, ObString(len, buf),
        geo, srs, cast_name))) {
      LOG_WARN("fail to parse geometry", K(ret), K(in_val), K(dst_geo_type));
    } else {
      ObString res_wkb;
      if (OB_FAIL(ObGeoTypeUtil::to_wkb(temp_allocator, *geo, srs, res_wkb))) {
        LOG_WARN("fail to get wkb", K(ret), K(dst_geo_type));
      } else if (OB_FAIL(common_gis_wkb(expr, ctx, res_datum, res_wkb))){
        LOG_WARN("fail to copy string", K(ret), K(dst_geo_type));
      }
    }

    if (OB_FAIL(ret) && CM_IS_COLUMN_CONVERT(expr.extra_) && ObGeoType::GEOMETRY == dst_geo_type) { // adapt mysql
      ret = OB_ERR_CANT_CREATE_GEOMETRY_OBJECT;
      LOG_USER_ERROR(OB_ERR_CANT_CREATE_GEOMETRY_OBJECT);
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
      const common::ObTimeZoneInfo *tz_info_local = NULL;
      ObSolidifiedVarsGetter helper(expr, ctx, session);
      if (OB_FAIL(helper.get_time_zone_info(tz_info_local))) {
        LOG_WARN("get time zone info failed", K(ret));
      } else {
        if (OB_FAIL(common_construct_otimestamp(in_type, *child_res, in_val))) {
          LOG_WARN("common_construct_otimestamp failed", K(ret));
        } else if (OB_FAIL(ObTimeConverter::otimestamp_to_odate(in_type, in_val,
                                                        tz_info_local, usec))) {
          LOG_WARN("fail to timestamp_tz_to_timestamp", K(ret));
        } else {
          ObTimeConverter::trunc_datetime(OB_MAX_DATE_PRECISION, usec);
          res_datum.set_datetime(usec);
        }
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
      ObTimeZoneInfoWrap tz_wrap;
      bool is_valid = false;
      ObDataTypeCastParams dtc_params;
      ObSolidifiedVarsGetter helper(expr, ctx, session);
      const ObLocalSessionVar *local_vars = NULL;
      if (OB_FAIL(helper.get_dtc_params(dtc_params))) {
        LOG_WARN("fail to get dtc params", K(ret));
      } else if (OB_FAIL(common_construct_otimestamp(in_type, *child_res, in_val))) {
        LOG_WARN("common_construct_otimestamp failed", K(ret));
      } else if (OB_FAIL(ObTimeConverter::otimestamp_to_str(in_val,
                                                     dtc_params,
                                                     in_scale, in_type, buf,
                                                     OB_CAST_TO_VARCHAR_MAX_LENGTH,
                                                     len))) {
        LOG_WARN("failed to convert otimestamp to string", K(ret));
      } else {
        ObString in_str(sizeof(buf), static_cast<int32_t>(len), buf);
        bool has_set_res = false;
        if (OB_FAIL(common_check_convert_string(expr, ctx, in_str, res_datum, has_set_res))) {
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
      const common::ObTimeZoneInfo *tz_info_local = NULL;
      ObSolidifiedVarsGetter helper(expr, ctx, session);
      if (OB_FAIL(helper.get_time_zone_info(tz_info_local))) {
        LOG_WARN("get time zone info failed", K(ret));
      } else {
        if (OB_FAIL(common_construct_otimestamp(in_type, *child_res, in_val))) {
          LOG_WARN("common_construct_otimestamp failed", K(ret));
        } else if (ObTimestampNanoType == in_type) {
          if (OB_FAIL(ObTimeConverter::odate_to_otimestamp(in_val.time_us_,
                  tz_info_local, out_type, out_val))) {
            LOG_WARN("fail to odate_to_otimestamp", K(ret), K(out_type));
          } else {
            out_val.time_ctx_.tail_nsec_ = in_val.time_ctx_.tail_nsec_;
          }
        } else if (ObTimestampNanoType == out_type) {
          if (OB_FAIL(ObTimeConverter::otimestamp_to_odate(in_type, in_val,
                  tz_info_local, *(int64_t *)&out_val.time_us_))) {
            LOG_WARN("fail to otimestamp_to_odate", K(ret), K(out_type));
          } else {
            out_val.time_ctx_.tail_nsec_ = in_val.time_ctx_.tail_nsec_;
          }
        } else {
          if (OB_FAIL(ObTimeConverter::otimestamp_to_otimestamp(in_type, in_val,
                  tz_info_local, out_type, out_val))) {
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
      const common::ObTimeZoneInfo *tz_info_local = NULL;
      ObSolidifiedVarsGetter helper(expr, ctx, session);
      if (OB_FAIL(helper.get_time_zone_info(tz_info_local))) {
        LOG_WARN("get time zone info failed", K(ret));
      } else {
        if (OB_FAIL(common_construct_otimestamp(in_type, *child_res, in_val))) {
          LOG_WARN("common_construct_otimestamp failed", K(ret));
        } else if (OB_FAIL(ObTimeConverter::otimestamp_to_str(in_val,
                                                      tz_info_local,
                                                      in_scale, in_type, buf,
                                                      OB_CAST_TO_VARCHAR_MAX_LENGTH,
                                                      len))) {
          LOG_WARN("failed to convert otimestamp to string", K(ret));
        } else {
          ObString in_str(sizeof(buf), static_cast<int32_t>(len), buf);
          ObString res_str;
          bool has_set_res = false;
          if (OB_FAIL(common_check_convert_string(expr, ctx, in_str, res_datum, has_set_res))) {
            LOG_WARN("fail to common_check_convert_string", K(ret), K(in_str));
          } else if (OB_FAIL(copy_datum_str_with_tmp_alloc(ctx, res_datum, res_str))) {
            LOG_WARN("copy datum string with tmp allocator failed", K(ret));
          } else if (OB_FAIL(common_string_lob(expr, res_str, ctx, NULL, res_datum))) {
            LOG_WARN("cast string to lob failed", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(otimestamp, text)
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
      const common::ObTimeZoneInfo *tz_info_local = NULL;
      ObSolidifiedVarsGetter helper(expr, ctx, session);
      if (OB_FAIL(helper.get_time_zone_info(tz_info_local))) {
        LOG_WARN("get time zone info failed", K(ret));
      } else {
        if (OB_FAIL(common_construct_otimestamp(in_type, *child_res, in_val))) {
          LOG_WARN("common_construct_otimestamp failed", K(ret));
        } else if (OB_FAIL(ObTimeConverter::otimestamp_to_str(in_val,
                                                      tz_info_local,
                                                      in_scale, in_type, buf,
                                                      OB_CAST_TO_VARCHAR_MAX_LENGTH,
                                                      len))) {
          LOG_WARN("failed to convert otimestamp to string", K(ret));
        } else {
          ObString in_str(sizeof(buf), static_cast<int32_t>(len), buf);
          ObString res_str;
          bool has_set_res = false;
          if (OB_FAIL(common_check_convert_string(expr, ctx, in_str, res_datum, has_set_res))) {
            LOG_WARN("fail to common_check_convert_string", K(ret), K(in_str));
          } else if (OB_FAIL(copy_datum_str_with_tmp_alloc(ctx, res_datum, res_str))) {
            LOG_WARN("copy datum string with tmp allocator failed", K(ret));
          } else if (OB_FAIL(common_string_text(expr, res_str, ctx, NULL, res_datum))) {
            LOG_WARN("cast string to lob failed", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(raw, json)
{
  EVAL_ARG()
  {
    ObString in_str(child_res->len_, child_res->ptr_);
    ret = common_string_json(expr, in_str, ctx, res_datum);
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
    bool has_set_res = false;
    if (CS_TYPE_BINARY != expr.datum_meta_.cs_type_) {
      // raw to clob
      OZ(ObDatumHexUtils::rawtohex(expr, in_str, ctx, res_datum));
      // mock check with content
      ObLobLocatorV2 lob(res_datum.get_string(), expr.obj_meta_.has_lob_header());
      has_set_res = lob.ptr_ != nullptr && lob.size_ > 0 && lob.is_valid() && lob.is_full_temp_lob();
    } else {
      // raw to blob
      res_datum.set_string(in_str.ptr(), in_str.length());
    }
    if (OB_FAIL(ret)) {
    } else if (has_set_res) {
      // has set lob res?
    } else if (ob_is_text_tc(expr.datum_meta_.type_)) {
      ObString res_str;
      if (OB_FAIL(copy_datum_str_with_tmp_alloc(ctx, res_datum, res_str))) {
        LOG_WARN("copy datum string with tmp allocator failed", K(ret));
      } else if (OB_FAIL(common_string_text(expr, res_str, ctx, NULL, res_datum))) {
        LOG_WARN("cast string to lob failed", K(ret));
      }
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
    bool has_set_res = false;
    OZ(common_check_convert_string(expr, ctx, in_str, res_datum, has_set_res));
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
      if (OB_FAIL(ObTimeConverter::interval_ym_to_str(in_val, in_scale, buf,
                                                  OB_CAST_TO_VARCHAR_MAX_LENGTH, len,
                                                  is_explicit_cast))) {
        LOG_WARN("interval_ym_to_str failed", K(ret));
      }
    } else {
      ObIntervalDSValue in_val(child_res->get_interval_ds());
      if (OB_FAIL(ObTimeConverter::interval_ds_to_str(in_val, in_scale, buf,
                                                  OB_CAST_TO_VARCHAR_MAX_LENGTH, len,
                                                  is_explicit_cast))) {
        LOG_WARN("interval_ym_to_str failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ObString in_str(len, buf);
      bool has_set_res = false;
      if (OB_FAIL(common_check_convert_string(expr, ctx, in_str, res_datum, has_set_res))) {
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
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "cast interval to interval type");
    }
  }
  return ret;
}

CAST_FUNC_NAME(rowid, string)
{
  EVAL_ARG()
  {
    ObURowIDData urowid_data(child_res->len_, reinterpret_cast<const uint8_t *>(child_res->ptr_));
    char *base64_buf = NULL;
    int64_t base64_buf_size = urowid_data.needed_base64_buffer_size();
    int64_t pos = 0;
    if (OB_ISNULL(base64_buf = expr.get_str_res_mem(ctx, base64_buf_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret));
    } else if (OB_FAIL(urowid_data.get_base64_str(base64_buf, base64_buf_size, pos))) {
      LOG_WARN("failed to get base64 str", K(ret));
    } else {
      ObString in_str(pos, base64_buf);
      bool has_set_res = false;
      if (OB_FAIL(common_check_convert_string(expr, ctx, in_str, res_datum, has_set_res))) {
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
      res_datum.set_urowid(ObURowIDData(child_res->len_,
                                        reinterpret_cast<const uint8_t *>(child_res->ptr_)));
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
    const ObLobLocator &lob_locator = child_res->get_lob_locator();
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
    const ObLobLocator &lob_locator = child_res->get_lob_locator();
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
    const ObLobLocator &lob_locator = child_res->get_lob_locator();
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
    const ObLobLocator &lob_locator = child_res->get_lob_locator();
    ObString in_str(lob_locator.payload_size_, lob_locator.get_payload_ptr());
    DEF_IN_OUT_TYPE();
    OZ(common_string_double(expr, in_type, expr.args_[0]->datum_meta_.cs_type_, out_type, in_str, res_datum));
  }
  return ret;
}

CAST_FUNC_NAME(lob, number)
{
  EVAL_STRING_ARG()
  {
    const ObLobLocator &lob_locator = child_res->get_lob_locator();
    ObString in_str(lob_locator.get_payload_length(), lob_locator.get_payload_ptr());
    number::ObNumber nmb;
    ObNumStackOnceAlloc tmp_alloc;
    OZ(common_string_number(expr, in_str, tmp_alloc, nmb));
    OX(res_datum.set_number(nmb));
  }
  return ret;
}

CAST_FUNC_NAME(lob, datetime)
{
  EVAL_STRING_ARG()
  {
    const ObLobLocator &lob_locator = child_res->get_lob_locator();
    ObString in_str(lob_locator.payload_size_, lob_locator.get_payload_ptr());
    OZ(common_string_datetime(expr, in_str, ctx, res_datum));
  }
  return ret;
}

CAST_FUNC_NAME(lob, date)
{
  EVAL_STRING_ARG()
  {
    const ObLobLocator &lob_locator = child_res->get_lob_locator();
    ObString in_str(lob_locator.payload_size_, lob_locator.get_payload_ptr());
    OZ(common_string_date(expr, in_str, res_datum));
  }
  return ret;
}

CAST_FUNC_NAME(lob, time)
{
  EVAL_STRING_ARG()
  {
    const ObLobLocator &lob_locator = child_res->get_lob_locator();
    ObString in_str(lob_locator.payload_size_, lob_locator.get_payload_ptr());
    OZ(common_string_time(expr, in_str, res_datum));
  }
  return ret;
}

CAST_FUNC_NAME(lob, year)
{
  EVAL_STRING_ARG()
  {
    const ObLobLocator &lob_locator = child_res->get_lob_locator();
    ObString in_str(lob_locator.payload_size_, lob_locator.get_payload_ptr());
    OZ(common_string_year(expr, in_str, res_datum));
  }
  return ret;
}

CAST_FUNC_NAME(lob, bit)
{
  EVAL_STRING_ARG()
  {
    const ObLobLocator &lob_locator = child_res->get_lob_locator();
    ObString in_str(lob_locator.payload_size_, lob_locator.get_payload_ptr());
    OZ(common_string_bit(expr, in_str, ctx, res_datum));
  }
  return ret;
}

CAST_FUNC_NAME(lob, string)
{
  EVAL_ARG()
  {
    const ObLobLocator &lob_locator = child_res->get_lob_locator();
    ObString in_str(lob_locator.payload_size_, lob_locator.get_payload_ptr());
    ObObjType in_type = ObLongTextType;
    ObObjType out_type = expr.datum_meta_.type_;
    ObCollationType in_cs_type = expr.args_[0]->datum_meta_.cs_type_;
    ObCollationType out_cs_type = expr.datum_meta_.cs_type_;
    bool has_set_res = false;
    OZ(common_string_string(expr, in_type, in_cs_type, out_type,
                            out_cs_type, in_str, ctx, res_datum, has_set_res));
  }
  return ret;
}

CAST_FUNC_NAME(lob, text)
{
  EVAL_ARG()
  {
    const ObLobLocator &lob_locator = child_res->get_lob_locator();
    ObString in_str(lob_locator.payload_size_, lob_locator.get_payload_ptr());
    ObObjType in_type = ObLongTextType;
    ObObjType out_type = expr.datum_meta_.type_;
    ObCollationType in_cs_type = expr.args_[0]->datum_meta_.cs_type_;
    ObCollationType out_cs_type = expr.datum_meta_.cs_type_;
    OZ(common_string_text(expr, in_str, ctx, NULL, res_datum));
  }
  return ret;
}

CAST_FUNC_NAME(lob, otimestamp)
{
  EVAL_STRING_ARG()
  {
    const ObLobLocator &lob_locator = child_res->get_lob_locator();
    ObString in_str(lob_locator.payload_size_, lob_locator.get_payload_ptr());
    OZ(common_string_otimestamp(expr, in_str, ctx, res_datum));
  }
  return ret;
}

CAST_FUNC_NAME(lob, raw)
{
  EVAL_STRING_ARG()
  {
    const ObLobLocator &lob_locator = child_res->get_lob_locator();
    if (lob_locator.payload_size_ > 0) {
      ObString in_str(lob_locator.payload_size_, lob_locator.get_payload_ptr());
      char *res_buf = nullptr;
      if (OB_ISNULL(res_buf = expr.get_str_res_mem(ctx, in_str.length()))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc buffer", K(in_str.length()));
      } else {
        MEMCPY(res_buf, in_str.ptr(), in_str.length());
        res_datum.set_string(ObString(in_str.length(), res_buf));
      }
    } else {
      res_datum.set_null();
    }
  }
  return ret;
}

CAST_FUNC_NAME(lob, interval)
{
  EVAL_STRING_ARG()
  {
    const ObLobLocator &lob_locator = child_res->get_lob_locator();
    ObString in_str(lob_locator.payload_size_, lob_locator.get_payload_ptr());
    OZ(common_string_interval(expr, in_str, ctx, res_datum));
  }
  return ret;
}

CAST_FUNC_NAME(lob, rowid)
{
  EVAL_STRING_ARG()
  {
    const ObLobLocator &lob_locator = child_res->get_lob_locator();
    ObString in_str(lob_locator.payload_size_, lob_locator.get_payload_ptr());
    OZ(common_string_rowid(expr, in_str, ctx, res_datum));
  }
  return ret;
}

CAST_FUNC_NAME(lob, lob)
{
  EVAL_STRING_ARG()
  {
    const ObLobLocator &lob_locator = child_res->get_lob_locator();
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

CAST_FUNC_NAME(lob, geometry)
{
  EVAL_STRING_ARG()
  {
    int warning = OB_SUCCESS;
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    const ObLobLocator &lob_locator = child_res->get_lob_locator();
    ObString in_str(lob_locator.payload_size_, lob_locator.get_payload_ptr());
    ObGeoType dst_geo_type = ObGeoCastUtils::get_geo_type_from_cast_mode(expr.extra_);
    const char *cast_name = ObGeometryTypeCastUtil::get_cast_name(dst_geo_type);
    ObGeometry *geo = NULL;
    omt::ObSrsCacheGuard srs_guard;
    const ObSrsItem *srs = NULL;
    if (OB_FAIL(ObGeoExprUtils::get_srs_item(ctx, srs_guard, in_str, srs, true, cast_name))) {
      LOG_WARN("fail to get srs item", K(ret), K(in_str));
    } else if (OB_FAIL(ObGeoExprUtils::build_geometry(temp_allocator, in_str, geo, srs, cast_name))) {
      LOG_WARN("fail to parse geometry", K(ret), K(in_str), K(dst_geo_type));
    } else if (ObGeoType::GEOMETRY == dst_geo_type || ObGeoType::GEOTYPEMAX == dst_geo_type) {
      res_datum.set_string(in_str);
    } else if (OB_FAIL(geometry_geometry(expr, ctx, res_datum))) {
      LOG_WARN("fail to cast geometry", K(ret), K(in_str), K(dst_geo_type));
    }

    if (OB_FAIL(ret) && CM_IS_COLUMN_CONVERT(expr.extra_) && ObGeoType::GEOMETRY == dst_geo_type) { // adapt mysql
      ret = OB_ERR_CANT_CREATE_GEOMETRY_OBJECT;
      LOG_USER_ERROR(OB_ERR_CANT_CREATE_GEOMETRY_OBJECT);
    }
  }
  return ret;
}
CAST_FUNC_NAME(lob, decimalint)
{
  EVAL_STRING_ARG()
  {
    // TODO cast directly into decimal int
    const ObLobLocator &lob_loc = child_res->get_lob_locator();
    ObString in_str(lob_loc.get_payload_length(), lob_loc.get_payload_ptr());
    number::ObNumber nmb;
    ObNumStackOnceAlloc tmp_nmb_alloc;
    ObDecimalIntBuilder tmp_dec_alloc;
    ObScale out_scale = expr.datum_meta_.scale_;
    ObDecimalInt *decint = nullptr;
    int32_t int_bytes = 0;
    if (OB_FAIL(common_string_number(expr, in_str, tmp_nmb_alloc, nmb))) {
      LOG_WARN("cast to number failed", K(ret));
    } else if (OB_FAIL(wide::from_number(nmb, tmp_dec_alloc, out_scale, decint, int_bytes))) {
      LOG_WARN("from number failed", K(ret));
    } else {
      res_datum.set_decimal_int(decint, int_bytes);
    }
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
    ObString j_bin_str = child_res->get_string();
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *child_res,
                expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), j_bin_str,
                &ctx.exec_ctx_))) {
      LOG_WARN("fail to get real data.", K(ret), K(j_bin_str));
    } else {
      ObJsonBin j_bin(j_bin_str.ptr(), j_bin_str.length(), &temp_allocator);
      ObIJsonBase *j_base = &j_bin;

      if (OB_FAIL(j_bin.reset_iter())) {
        LOG_WARN("failed to reset json bin iter", K(ret), K(j_bin_str));
      } else if (CAST_FAIL(j_base->to_int(out_val))) {
        LOG_WARN("fail to cast json to int type", K(ret), K(j_bin_str));
        ret = OB_ERR_INVALID_JSON_VALUE_FOR_CAST;
        LOG_USER_ERROR(OB_ERR_INVALID_JSON_VALUE_FOR_CAST);
      } else if (out_type < ObIntType && CAST_FAIL_CM(int_range_check(out_type, out_val, out_val), extra)) {
        LOG_WARN("range check failed", K(ret), K(out_type), K(out_val));
      } else {
        SET_RES_INT(out_val);
      }
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
    ObString j_bin_str = child_res->get_string();
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *child_res,
                expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), j_bin_str,
                &ctx.exec_ctx_))) {
      LOG_WARN("fail to get real data.", K(ret), K(j_bin_str));
    } else {
      ObJsonBin j_bin(j_bin_str.ptr(), j_bin_str.length(), &temp_allocator);
      ObIJsonBase *j_base = &j_bin;

      if (OB_FAIL(j_bin.reset_iter())) {
        LOG_WARN("failed to reset json bin iter", K(ret), K(j_bin_str));
      } else if (CAST_FAIL(j_base->to_uint(out_val))) {
        LOG_WARN("fail to cast json to uint type", K(ret), K(j_bin_str));
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
    ObString j_bin_str = child_res->get_string();
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *child_res,
                expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), j_bin_str,
                &ctx.exec_ctx_))) {
      LOG_WARN("fail to get real data.", K(ret), K(j_bin_str));
    } else {
      ObJsonBin j_bin(j_bin_str.ptr(), j_bin_str.length(), &temp_allocator);
      ObIJsonBase *j_base = &j_bin;

      if (OB_FAIL(j_bin.reset_iter())) {
        LOG_WARN("failed to reset json bin iter", K(ret), K(j_bin_str));
      } else if (CAST_FAIL(j_base->to_double(out_val))) {
        LOG_WARN("fail to cast json to double type", K(ret), K(j_bin_str));
        ret = OB_ERR_INVALID_JSON_VALUE_FOR_CAST;
        LOG_USER_ERROR(OB_ERR_INVALID_JSON_VALUE_FOR_CAST);
      } else if (ObUDoubleType == out_type && CAST_FAIL(numeric_negative_check(out_val))) {
        LOG_WARN("numeric_negative_check failed", K(ret), K(out_val));
      } else {
        SET_RES_DOUBLE(out_val);
      }
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
    ObString j_bin_str = child_res->get_string();
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *child_res,
                expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), j_bin_str,
                &ctx.exec_ctx_))) {
      LOG_WARN("fail to get real data.", K(ret), K(j_bin_str));
    } else {
      ObJsonBin j_bin(j_bin_str.ptr(), j_bin_str.length(), &temp_allocator);
      ObIJsonBase *j_base = &j_bin;

      if (OB_FAIL(j_bin.reset_iter())) {
        LOG_WARN("failed to reset json bin iter", K(ret), K(j_bin_str));
      } else if (CAST_FAIL(j_base->to_double(tmp_val))) {
        LOG_WARN("fail to cast json to float type", K(ret), K(j_bin_str));
        ret = OB_ERR_INVALID_JSON_VALUE_FOR_CAST;
        LOG_USER_ERROR(OB_ERR_INVALID_JSON_VALUE_FOR_CAST);
      } else {
        SET_RES_FLOAT(out_val);
      }
    }
  }
  return ret;
}

static int common_json_number(common::ObDatum &child_res, const ObExpr &expr,
                              const ObObjType out_type, ObEvalCtx &ctx, ObIAllocator &alloc,
                              number::ObNumber &out_nmb)
{
  int ret = OB_SUCCESS;
  int warning = ret;
  ObString j_bin_str = child_res.get_string();
  if (OB_FAIL(ObTextStringHelper::read_real_string_data(
        alloc, child_res, expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(),
        j_bin_str, &ctx.exec_ctx_))) {
    LOG_WARN("fail to get real data", K(ret), K(j_bin_str));
  } else {
    ObJsonBin j_bin(j_bin_str.ptr(), j_bin_str.length(), &alloc);
    ObIJsonBase *j_base = &j_bin;

    if (OB_FAIL(j_bin.reset_iter())) {
      LOG_WARN("failed to reset json bin iter", K(ret), K(j_bin_str));
    } else if (CAST_FAIL(j_base->to_number(&alloc, out_nmb))) {
      LOG_WARN("fail to cast json to number type", K(ret), K(j_bin_str));
      ret = OB_ERR_INVALID_JSON_VALUE_FOR_CAST;
      LOG_USER_ERROR(OB_ERR_INVALID_JSON_VALUE_FOR_CAST);
    } else if (ObUNumberType == out_type && CAST_FAIL(numeric_negative_check(out_nmb))) {
      LOG_WARN("numeric_negative_check failed", K(ret), K(out_nmb));
    } else {
      // do nothing
    }
  }
  return ret;
}

CAST_FUNC_NAME(json, number)
{
  EVAL_STRING_ARG()
  {
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    number::ObNumber out_nmb;
    if (OB_FAIL(common_json_number(*child_res, expr, expr.datum_meta_.type_, ctx, temp_allocator,
                                   out_nmb))) {
      LOG_WARN("fail to get real data.", K(ret));
    } else {
      res_datum.set_number(out_nmb);
    }
  }
  return ret;
}

CAST_FUNC_NAME(json, datetime)
{
  EVAL_STRING_ARG()
  {
    GET_SESSION()
    {
      int warning = OB_SUCCESS;
      int64_t out_val;
      ObObjType out_type = expr.datum_meta_.type_;
      ObString j_bin_str = child_res->get_string();
      ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
      common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
      ObTimeConvertCtx cvrt_ctx(session->get_timezone_info(), ObTimestampType == out_type);
      if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *child_res,
                  expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), j_bin_str,
                  &ctx.exec_ctx_))) {
        LOG_WARN("fail to get real data.", K(ret), K(j_bin_str));
      } else {
        ObJsonBin j_bin(j_bin_str.ptr(), j_bin_str.length(), &temp_allocator);
        ObIJsonBase *j_base = &j_bin;

        if (OB_FAIL(j_bin.reset_iter())) {
          LOG_WARN("failed to reset json bin iter", K(ret), K(j_bin_str));
        } else if (CAST_FAIL(j_base->to_datetime(out_val, &cvrt_ctx))) {
          LOG_WARN("fail to cast json to datetime type", K(ret), K(j_bin_str));
          ret = OB_ERR_INVALID_JSON_VALUE_FOR_CAST;
          LOG_USER_ERROR(OB_ERR_INVALID_JSON_VALUE_FOR_CAST);
        } else {
          SET_RES_DATETIME(out_val);
        }
      }
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
    ObString j_bin_str = child_res->get_string();
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *child_res,
                expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), j_bin_str,
                &ctx.exec_ctx_))) {
      LOG_WARN("fail to get real data.", K(ret), K(j_bin_str));
    } else {
      ObJsonBin j_bin(j_bin_str.ptr(), j_bin_str.length(), &temp_allocator);
      ObIJsonBase *j_base = &j_bin;

      if (OB_FAIL(j_bin.reset_iter())) {
        LOG_WARN("failed to reset json bin iter", K(ret), K(j_bin_str));
      } else if (CAST_FAIL(j_base->to_date(out_val))) {
        LOG_WARN("fail to cast json to date type", K(ret), K(j_bin_str));
        ret = OB_ERR_INVALID_JSON_VALUE_FOR_CAST;
        LOG_USER_ERROR(OB_ERR_INVALID_JSON_VALUE_FOR_CAST);
      } else {
        SET_RES_DATE(out_val);
      }
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
    ObString j_bin_str = child_res->get_string();
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *child_res,
                expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), j_bin_str,
                &ctx.exec_ctx_))) {
      LOG_WARN("fail to get real data.", K(ret), K(j_bin_str));
    } else {
      ObJsonBin j_bin(j_bin_str.ptr(), j_bin_str.length(), &temp_allocator);
      ObIJsonBase *j_base = &j_bin;

      if (OB_FAIL(j_bin.reset_iter())) {
        LOG_WARN("failed to reset json bin iter", K(ret), K(j_bin_str));
      } else if (CAST_FAIL(j_base->to_time(out_val))) {
        LOG_WARN("fail to cast json to time type", K(ret), K(j_bin_str));
        ret = OB_ERR_INVALID_JSON_VALUE_FOR_CAST;
        LOG_USER_ERROR(OB_ERR_INVALID_JSON_VALUE_FOR_CAST);
      } else {
        SET_RES_TIME(out_val);
      }
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
    ObString j_bin_str = child_res->get_string();
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *child_res,
                expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), j_bin_str,
                &ctx.exec_ctx_))) {
      LOG_WARN("fail to get real data.", K(ret), K(j_bin_str));
    } else {
      ObJsonBin j_bin(j_bin_str.ptr(), j_bin_str.length(), &temp_allocator);
      ObIJsonBase *j_base = &j_bin;

      if (OB_FAIL(j_bin.reset_iter())) {
        LOG_WARN("failed to reset json bin iter", K(ret), K(j_bin_str));
      } else if (CAST_FAIL(j_base->to_int(int_val, false, true))) {
        LOG_WARN("fail to cast json as year", K(ret), K(j_bin_str));
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
  }
  return ret;
}

CAST_FUNC_NAME(json, raw)
{
  EVAL_STRING_ARG()
  {
    int warning = OB_SUCCESS;
    ObObjType out_type = expr.datum_meta_.type_;
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    ObString j_bin_str = child_res->get_string();
    ObJsonBin j_bin(j_bin_str.ptr(), j_bin_str.length(), &temp_allocator);
    ObIJsonBase *j_base = &j_bin;
    ObJsonBuffer j_buf(&temp_allocator);
    ObDatum t_res_datum;
    bool has_set_res = false;

    if (OB_FAIL(j_bin.reset_iter())) {
      LOG_WARN("failed to reset json bin iter", K(ret), K(j_bin_str));
    } else if (CAST_FAIL(j_base->print(j_buf, true))) {
      LOG_WARN("fail to convert json to string", K(ret), K(j_bin_str));
      ret = OB_ERR_INVALID_JSON_VALUE_FOR_CAST;
      LOG_USER_ERROR(OB_ERR_INVALID_JSON_VALUE_FOR_CAST);
    } else {
      ObObjType in_type = ObLongTextType;
      ObObjType out_type = expr.datum_meta_.type_;
      ObString temp_str_val(j_buf.length(), j_buf.ptr());
      // 如果将json直接设置成binary，这里要做特殊处理，而且代码中的binary类型也要改了
      ObCollationType in_cs_type = expr.args_[0]->datum_meta_.cs_type_;
      ObCollationType out_cs_type = expr.datum_meta_.cs_type_;
      bool is_need_string_string_convert = ((CS_TYPE_BINARY == out_cs_type)
          || (ObCharset::charset_type_by_coll(in_cs_type) != ObCharset::charset_type_by_coll(out_cs_type)));
      if (!is_need_string_string_convert) {
        // same collation type, just string copy result;
        OZ(common_copy_string(expr, temp_str_val, ctx, t_res_datum));
      } else {
        // should do collation convert;
        OZ(common_string_string(expr, in_type, in_cs_type, out_type,
                                out_cs_type, temp_str_val, ctx, t_res_datum, has_set_res));
      }
      if (OB_SUCC(ret)) {
        ObString in_str = t_res_datum.get_string();
        OZ(ObDatumHexUtils::hextoraw_string(expr, in_str, ctx, res_datum, has_set_res));
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
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    ObString j_bin_str = child_res->get_string();
    if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *child_res,
                expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), j_bin_str,
                &ctx.exec_ctx_))) {
      LOG_WARN("fail to get real data.", K(ret), K(j_bin_str));
    } else {
      ObJsonBin j_bin(j_bin_str.ptr(), j_bin_str.length(), &temp_allocator);
      ObIJsonBase *j_base = &j_bin;
      ObJsonBuffer j_buf(&temp_allocator);

      if (OB_FAIL(j_bin.reset_iter())) {
        LOG_WARN("failed to reset json bin iter", K(ret), K(j_bin_str));
      } else if (CAST_FAIL(j_base->print(j_buf, true))) {
        LOG_WARN("fail to convert json to string", K(ret), K(j_bin_str));
        ret = OB_ERR_INVALID_JSON_VALUE_FOR_CAST;
        LOG_USER_ERROR(OB_ERR_INVALID_JSON_VALUE_FOR_CAST);
      } else {
        ObObjType in_type = ObLongTextType;
        ObObjType out_type = expr.datum_meta_.type_;
        ObString temp_str_val(j_buf.length(), j_buf.ptr());
        // 如果将json直接设置成binary，这里要做特殊处理，而且代码中的binary类型也要改了
        ObCollationType in_cs_type = expr.args_[0]->datum_meta_.cs_type_;
        ObCollationType out_cs_type = expr.datum_meta_.cs_type_;
        bool is_need_string_string_convert = ((CS_TYPE_BINARY == out_cs_type)
            || (ObCharset::charset_type_by_coll(in_cs_type) != ObCharset::charset_type_by_coll(out_cs_type)));
        if (!is_need_string_string_convert) {
          // same collation type, just string copy result;
          OZ(common_copy_string(expr, temp_str_val, ctx, res_datum));
        } else {
          // should do collation convert;
          bool has_set_res = false;
          OZ(common_string_string(expr, in_type, in_cs_type, out_type,
                                  out_cs_type, temp_str_val, ctx, res_datum,
                                  has_set_res));
        }
        if (OB_SUCC(ret) && ob_is_text_tc(out_type)) {
          ObString res_str;
          if (OB_FAIL(copy_datum_str_with_tmp_alloc(ctx, res_datum, res_str))) {
            LOG_WARN("copy datum string with tmp allocator failed", K(ret));
          } else if (OB_FAIL(common_string_text(expr, res_str, ctx, NULL, res_datum))) {
            LOG_WARN("cast string to lob failed", K(ret));
          }
        }
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
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *child_res,
                expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), j_bin_str,
                &ctx.exec_ctx_))) {
      LOG_WARN("fail to get real data.", K(ret), K(j_bin_str));
    } else {
      ObJsonBin j_bin(j_bin_str.ptr(), j_bin_str.length(), &temp_allocator);
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
    ObString j_bin_str = child_res->get_string();
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *child_res,
                expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), j_bin_str,
                &ctx.exec_ctx_))) {
      LOG_WARN("fail to get real data.", K(ret), K(j_bin_str));
    } else {
      ObJsonBin j_bin(j_bin_str.ptr(), j_bin_str.length(), &temp_allocator);
      ObIJsonBase *j_base = &j_bin;

      if (OB_FAIL(j_bin.reset_iter())) {
        LOG_WARN("failed to reset json bin iter", K(ret), K(j_bin_str));
      } else if (CAST_FAIL(j_base->to_datetime(datetime_val))) {
        LOG_WARN("fail to cast json as datetime", K(ret), K(j_bin_str));
        ret = OB_ERR_INVALID_JSON_VALUE_FOR_CAST;
        LOG_USER_ERROR(OB_ERR_INVALID_JSON_VALUE_FOR_CAST);
      } else {
        GET_SESSION()
        {
          const common::ObTimeZoneInfo *tz_info_local = NULL;
          ObSolidifiedVarsGetter helper(expr, ctx, session);
          if (OB_FAIL(helper.get_time_zone_info(tz_info_local))) {
            LOG_WARN("get time zone info failed", K(ret));
          } else {
            ObOTimestampData out_val;
            if (OB_FAIL(ObTimeConverter::odate_to_otimestamp(datetime_val, tz_info_local, out_type, out_val))) {
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
    }
  }
  return ret;
}

CAST_FUNC_NAME(json, lob)
{
  EVAL_STRING_ARG()
  {
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    ObString res_str;
    if(OB_FAIL(common_json_string(expr, ctx, temp_allocator, *child_res, res_str))) {
      LOG_WARN("common json to string failed", K(ret));
    } else {
      // add lob locator
      ObLobLocator *lob_locator = nullptr;
      const int64_t buf_len = sizeof(ObLobLocator) + res_str.length();
      char *buf = nullptr;
      if (OB_ISNULL(buf = expr.get_str_res_mem(ctx, buf_len))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Failed to allocate memory for lob locator", K(ret), K(buf_len));
      } else if (FALSE_IT(lob_locator = reinterpret_cast<ObLobLocator *> (buf))) {
      } else if (OB_FAIL(lob_locator->init(res_str))) {
        STORAGE_LOG(WARN, "Failed to init lob locator", K(ret), K(res_str), KPC(lob_locator));
      } else {
        res_datum.set_lob_locator(*lob_locator);
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(json, text)
{
  EVAL_STRING_ARG()
  {
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    ObString res_str;
    if(OB_FAIL(common_json_string(expr, ctx, temp_allocator, *child_res, res_str))) {
      LOG_WARN("common json to string failed", K(ret));
    } else if(is_lob_storage(expr.datum_meta_.type_)) {
      ObTextStringDatumResult str_result(expr.datum_meta_.type_, &expr, &ctx, &res_datum);
      if (OB_FAIL(str_result.init(res_str.length()))) {
        LOG_WARN("Lob: init lob result failed", K(ret));
      } else if (OB_FAIL(str_result.append(res_str.ptr(), res_str.length()))) {
        LOG_WARN("Lob: append lob result failed", K(ret));
      } else {
        str_result.set_result();
      }
    } else {
      res_datum.set_string(res_str);
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

CAST_FUNC_NAME(json, geometry)
{
  EVAL_ARG()
  {
    if (CM_IS_COLUMN_CONVERT(expr.extra_)) { // adapt mysql
      ret = OB_ERR_CANT_CREATE_GEOMETRY_OBJECT;
      LOG_USER_ERROR(OB_ERR_CANT_CREATE_GEOMETRY_OBJECT);
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "json, geometry");
    }
  }
  return ret;
}

CAST_FUNC_NAME(json, decimalint)
{
  // TODO: cast into decimalint directly
  EVAL_STRING_ARG()
  {
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    number::ObNumber out_nmb;
    ObScale out_scale = expr.datum_meta_.scale_;
    int32_t int_bytes = 0;
    ObDecimalInt *decint = nullptr;
    int32_t expected_int_bytes =
      wide::ObDecimalIntConstValue::get_int_bytes_by_precision(expr.datum_meta_.precision_);
    ObDecimalIntBuilder res_val;
    if (OB_FAIL(common_json_number(*child_res, expr, ObNumberType, ctx, temp_allocator, out_nmb))) {
      LOG_WARN("json_number failed", K(ret));
    } else if (OB_FAIL(wide::from_number(out_nmb, temp_allocator, out_scale, decint, int_bytes))) {
      LOG_WARN("from number failed", K(ret));
    } else if (OB_FAIL(ObDatumCast::align_decint_precision_unsafe(decint, int_bytes,
                                                                  expected_int_bytes, res_val))) {
      LOG_WARN("align decint failed", K(ret));
    } else {
      res_datum.set_decimal_int(res_val.get_decimal_int(), res_val.get_int_bytes());
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////
// geometry -> XXX
CAST_FUNC_NAME(geometry, int)
{
  EVAL_STRING_ARG()
  {
    if (CM_IS_IMPLICIT_CAST(expr.extra_)) {
      ObString in_str = child_res->get_string();
      ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
      common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
      if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *child_res,
                  expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), in_str,
                  &ctx.exec_ctx_))) {
        LOG_WARN("fail to get real data.", K(ret), K(in_str));
      } else if (OB_FAIL(common_string_int(expr, expr.extra_, in_str, true, res_datum))) {
        LOG_WARN("fail to cast string to int", K(ret));
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "cast_as_signed");
    }
  }
  return ret;
}

CAST_FUNC_NAME(geometry, uint)
{
  EVAL_STRING_ARG()
  {
    if (CM_IS_IMPLICIT_CAST(expr.extra_)) {
      ObString in_str = child_res->get_string();
      ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
      common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
      if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *child_res,
                  expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), in_str,
                  &ctx.exec_ctx_))) {
        LOG_WARN("fail to get real data.", K(ret), K(in_str));
      } else if (OB_FAIL(common_string_uint(expr, in_str, true, res_datum))) {
        LOG_WARN("fail to cast string to uint", K(ret));
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "cast_as_unsigned");
    }
  }
  return ret;
}

CAST_FUNC_NAME(geometry, double)
{
  EVAL_STRING_ARG()
  {
    // hash join will cast geometry to double, which has set CM_WARN_ON_FAIL. Skip it.
    if (CM_IS_IMPLICIT_CAST(expr.extra_) && !CM_IS_WARN_ON_FAIL(expr.extra_)) {
      ObString in_str = child_res->get_string();
      ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
      common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
      DEF_IN_OUT_TYPE();
      if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *child_res,
                  expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), in_str,
                  &ctx.exec_ctx_))) {
        LOG_WARN("fail to get real data.", K(ret), K(in_str));
      } else if (OB_FAIL(common_string_double(expr, in_type, expr.args_[0]->datum_meta_.cs_type_, out_type, in_str, res_datum))) {
        LOG_WARN("fail to cast string to double", K(ret));
        ret = OB_ERR_WARN_DATA_OUT_OF_RANGE; // adapt mysql
      }
    } else {
      res_datum.set_double(0);
    }
  }
  return ret;
}

CAST_FUNC_NAME(geometry, float)
{
  EVAL_STRING_ARG()
  {
    if (CM_IS_IMPLICIT_CAST(expr.extra_)) {
      float out_val = 0;
      ObString in_str = child_res->get_string();
      ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
      common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
      if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *child_res,
                  expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), in_str,
                  &ctx.exec_ctx_))) {
        LOG_WARN("fail to get real data.", K(ret), K(in_str));
      } else if (OB_FAIL(common_string_float(expr, in_str, out_val))) {
        LOG_WARN("fail to cast string to float", K(ret));
        ret = OB_ERR_WARN_DATA_OUT_OF_RANGE; // adapt mysql
      } else {
        res_datum.set_float(out_val);
      }
    } else {
      res_datum.set_float(0);
    }
  }
  return ret;
}

CAST_FUNC_NAME(geometry, number)
{
  EVAL_STRING_ARG()
  {
    if (CM_IS_IMPLICIT_CAST(expr.extra_)) {
      number::ObNumber nmb;
      ObNumStackOnceAlloc tmp_alloc;
      ObString in_str = child_res->get_string();
      ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
      common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
      if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *child_res,
                  expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), in_str,
                  &ctx.exec_ctx_))) {
        LOG_WARN("fail to get real data.", K(ret), K(in_str));
      } else if (OB_FAIL(common_string_number(expr, in_str, tmp_alloc, nmb))) {
        LOG_WARN("fail to cast string to number", K(ret));
        ret = OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD; // adapt mysql
      } else {
        res_datum.set_number(nmb);
      }
    } else {
      number::ObNumber nmb;
      nmb.set_zero();
      OX(res_datum.set_number(nmb));
    }
  }
  return ret;
}

CAST_FUNC_NAME(geometry, datetime)
{
  EVAL_STRING_ARG()
  {
    if (CM_IS_IMPLICIT_CAST(expr.extra_)) {
      ObString in_str = child_res->get_string();
      ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
      common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
      if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *child_res,
                  expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), in_str,
                  &ctx.exec_ctx_))) {
        LOG_WARN("fail to get real data.", K(ret), K(in_str));
      } else if (OB_FAIL(common_string_datetime(expr, in_str, ctx, res_datum))) {
        LOG_WARN("fail to cast string to datetime", K(ret));
      }
    } else {
      res_datum.set_null();
    }
  }
  return ret;
}

CAST_FUNC_NAME(geometry, date)
{
  EVAL_STRING_ARG()
  {
    if (CM_IS_IMPLICIT_CAST(expr.extra_)) {
      ObString in_str = child_res->get_string();
      ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
      common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
      if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *child_res,
                  expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), in_str,
                  &ctx.exec_ctx_))) {
        LOG_WARN("fail to get real data.", K(ret), K(in_str));
      } else if (OB_FAIL(common_string_date(expr, in_str, res_datum))) {
        LOG_WARN("fail to cast string to date", K(ret));
      }
    } else {
      res_datum.set_null();
    }
  }
  return ret;
}

CAST_FUNC_NAME(geometry, time)
{
  EVAL_STRING_ARG()
  {
    if (CM_IS_NULL_ON_WARN(expr.extra_)) {
      // issue/42826593
      res_datum.set_null();
    } else if (CM_IS_IMPLICIT_CAST(expr.extra_)) {
      ObString in_str = child_res->get_string();
      ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
      common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
      if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *child_res,
                  expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), in_str,
                  &ctx.exec_ctx_))) {
        LOG_WARN("fail to get real data.", K(ret), K(in_str));
      } else if (OB_FAIL(common_string_time(expr, in_str, res_datum))) {
        LOG_WARN("fail to cast string to time", K(ret));
      }
    } else {
      res_datum.set_null();
    }
  }
  return ret;
}

CAST_FUNC_NAME(geometry, year)
{
  EVAL_STRING_ARG()
  {
    if (CM_IS_IMPLICIT_CAST(expr.extra_)) {
      ObString in_str = child_res->get_string();
      ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
      common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
      if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *child_res,
                  expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), in_str,
                  &ctx.exec_ctx_))) {
        LOG_WARN("fail to get real data.", K(ret), K(in_str));
      } else if (OB_FAIL(common_string_year(expr, in_str, res_datum))) {
        LOG_WARN("fail to cast string to year", K(ret));
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "cast_as_year");
    }
  }
  return ret;
}

CAST_FUNC_NAME(geometry, bit)
{
  EVAL_STRING_ARG()
  {
    if (CM_IS_IMPLICIT_CAST(expr.extra_)) {
      ObString in_str = child_res->get_string();
      ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
      common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
      if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *child_res,
                  expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), in_str,
                  &ctx.exec_ctx_))) {
        LOG_WARN("fail to get real data.", K(ret), K(in_str));
      } else if (OB_FAIL(common_string_bit(expr, in_str, ctx, res_datum))) {
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
  }
  return ret;
}

CAST_FUNC_NAME(geometry, otimestamp)
{
  EVAL_STRING_ARG()
  {
    if (CM_IS_IMPLICIT_CAST(expr.extra_)) {
      ObString in_str = child_res->get_string();
      ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
      common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
      if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *child_res,
                  expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), in_str,
                  &ctx.exec_ctx_))) {
        LOG_WARN("fail to get real data.", K(ret), K(in_str));
      } else if (OB_FAIL(common_string_otimestamp(expr, in_str, ctx, res_datum))) {
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
  }
  return ret;
}

CAST_FUNC_NAME(geometry, decimalint)
{
  EVAL_STRING_ARG()
  {
    if (CM_IS_IMPLICIT_CAST(expr.extra_)) {
      ObString in_str = child_res->get_string();
      ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
      common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
      ObDecimalIntBuilder res_val;
      if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *child_res,
          expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), in_str, &ctx.exec_ctx_))) {
        LOG_WARN("failed to get real data", K(ret), K(in_str));
      } else if (OB_FAIL(common_string_decimalint(expr, in_str,
                                                  ctx.exec_ctx_.get_user_logging_ctx(), res_val))) {
        LOG_WARN("failed to cast string to decimal int", K(ret), K(in_str));
        ret = OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD; // compatible with mysql
      } else {
        res_datum.set_decimal_int(res_val.get_decimal_int(), res_val.get_int_bytes());
      }
    } else {
      ObDecimalIntBuilder res_val;
      res_val.from((int32_t)0);
      ObPrecision out_prec = expr.datum_meta_.precision_;
      res_val.extend(wide::ObDecimalIntConstValue::get_int_bytes_by_precision(out_prec));
      res_datum.set_decimal_int(res_val.get_decimal_int(), res_val.get_int_bytes());
    }
  }
  return ret;
}

static int geom_copy_string(const ObExpr &expr,
                            ObString &src,
                            ObEvalCtx &ctx,
                            ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  char *out_ptr = NULL;
  int64_t len = src.length();
  if (OB_LIKELY(len > WKB_OFFSET)) {
    uint8_t offset = WKB_GEO_SRID_SIZE;
    uint8_t version = (*(src.ptr() + WKB_GEO_SRID_SIZE));
    if (IS_GEO_VERSION(version)) {
      // version exist
      len -= WKB_VERSION_SIZE;
      offset += WKB_VERSION_SIZE;
    }
    if (expr.obj_meta_.is_lob_storage()) {
      if (OB_FAIL(common_gis_wkb(expr, ctx, res_datum, src, IS_GEO_VERSION(version)))) {
        LOG_WARN("fail to pack gis lob res", K(ret));
      }
    } else {
      if (expr.res_buf_len_ < len) {
        if (OB_ISNULL(out_ptr = expr.get_str_res_mem(ctx, len))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", K(ret));
        }
      } else {
        out_ptr = const_cast<char*>(res_datum.ptr_);
      }

      if (OB_SUCC(ret)) {
        MEMMOVE(out_ptr, src.ptr(), WKB_GEO_SRID_SIZE);
        MEMMOVE(out_ptr + WKB_GEO_SRID_SIZE, src.ptr() + offset, len - WKB_GEO_SRID_SIZE);
        res_datum.set_string(out_ptr, len);
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid obj len", K(ret), K(len));
  }
  return ret;
}

CAST_FUNC_NAME(geometry, string)
{
  EVAL_STRING_ARG()
  {
    ObString wkb = child_res->get_string();
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *child_res,
                expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), wkb,
                &ctx.exec_ctx_))) {
      LOG_WARN("fail to get real data.", K(ret), K(wkb));
    } else if (OB_FAIL(geom_copy_string(expr, wkb, ctx, res_datum))){
      LOG_WARN("fail to copy string", K(ret));
    }
  }
  return ret;
}

CAST_FUNC_NAME(geometry, json)
{
  EVAL_STRING_ARG()
  {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "geomerty, json");
  }
  return ret;
}

CAST_FUNC_NAME(geometry, geometry)
{
  EVAL_STRING_ARG()
  {
    int warning = OB_SUCCESS;
    ObGeometry *src_tree = NULL;
    ObGeometry *dst_tree = NULL;
    ObString wkb = child_res->get_string();
    ObObjType out_type = expr.datum_meta_.type_;
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    ObGeoType dst_geo_type = ObGeoCastUtils::get_geo_type_from_cast_mode(expr.extra_);
    const char *cast_name = ObGeometryTypeCastUtil::get_cast_name(dst_geo_type);
    omt::ObSrsCacheGuard srs_guard;
    const ObSrsItem *srs = NULL;
    ObGeometryTypeCast *geo_cast = NULL;
    ObGeoErrLogInfo log_info;

    if (ObGeoType::GEOMETRY == dst_geo_type) {
      res_datum.set_string(wkb);
    } else {
      if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *child_res,
                  expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), wkb,
                  &ctx.exec_ctx_))) {
        LOG_WARN("fail to get real data.", K(ret), K(wkb));
      } else if (OB_FAIL(ObGeoExprUtils::get_srs_item(ctx, srs_guard, wkb, srs, true, cast_name))) {
        LOG_WARN("fail to get srs item", K(ret), K(wkb));
      } else if (OB_FAIL(ObGeometryTypeCastUtil::get_tree(temp_allocator, wkb, src_tree, srs,
          log_info, cast_name))) {
        LOG_WARN("fail to get tree", K(ret), K(wkb));
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
      }

      if (OB_SUCC(ret)) {
        ObString res_wkb;
        if (OB_FAIL(ObGeoTypeUtil::to_wkb(temp_allocator, *dst_tree, srs, res_wkb))) {
          LOG_WARN("fail to get wkb", K(ret), K(src_tree->type()), K(dst_geo_type));
        } else if (OB_FAIL(common_gis_wkb(expr, ctx, res_datum, res_wkb))){
          LOG_WARN("fail to copy string", K(ret), K(src_tree->type()), K(dst_geo_type));
        }
      }
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////
// XXX -> udt
int cast_to_udt_not_support(const sql::ObExpr &expr, sql::ObEvalCtx &ctx, sql::ObDatum &res_datum)
{
  UNUSED(ctx);
  UNUSED(res_datum);
  int ret = OB_SUCCESS;
  const ObObjMeta &in_obj_meta = expr.args_[0]->obj_meta_;
  const ObObjMeta &out_obj_meta = expr.obj_meta_;
  if (out_obj_meta.is_xml_sql_type()) {
    ret = OB_ERR_INVALID_XML_DATATYPE;
    LOG_USER_ERROR(OB_ERR_INVALID_XML_DATATYPE, "ANYDATA", ob_obj_type_str(in_obj_meta.get_type()));
    LOG_WARN_RET(ret, "not expected obj type convert", K(in_obj_meta), K(out_obj_meta),
      K(out_obj_meta.get_subschema_id()), K(expr.extra_));
  } else {
    // other udts
    // ORA-00932: inconsistent datatypes: expected PLSQL INDEX TABLE got NUMBER
    // currently other types to udt not supported
    ret = OB_ERR_INVALID_CAST_UDT;
    LOG_WARN_RET(ret, "invalid CAST to a type that is not a nested table or VARRAY");
  }
  return ret;
}

////////////////////////////////////////////////////////////
// udt -> XXX

int cast_udt_to_other_not_support(const sql::ObExpr &expr, sql::ObEvalCtx &ctx, sql::ObDatum &res_datum)
{
  UNUSED(ctx);
  UNUSED(res_datum);
  int ret = OB_SUCCESS;
  const ObObjMeta &in_obj_meta = expr.args_[0]->obj_meta_;
  const ObObjMeta &out_obj_meta = expr.obj_meta_;
  if (in_obj_meta.is_xml_sql_type()) {
    if (out_obj_meta.is_xml_sql_type()) {
      ObDatum *child_res = NULL;
      if (OB_FAIL(expr.args_[0]->eval(ctx, child_res))) {
        LOG_WARN("eval arg failed", K(ret), K(ctx));
      } else if (child_res->is_null() ||
                (lib::is_oracle_mode() && 0 == child_res->len_
                  && ObLongTextType != expr.args_[0]->datum_meta_.type_)) {
        res_datum.set_null();
      } else {
        res_datum.set_datum(*child_res);
      }
    } else {
      // only allow cast basic types to invalid CAST to a type that is not a nested table or VARRAY
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN_RET(ret, "inconsistent datatypes", K(in_obj_meta), K(out_obj_meta),
        K(out_obj_meta.get_subschema_id()), K(expr.extra_));
    }
  } else {
    // other udts
    // ORA-00932: inconsistent datatypes: expected PLSQL INDEX TABLE got NUMBER
    // currently other types to udt not supported
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN_RET(ret, "not expected obj type convert", K(in_obj_meta), K(out_obj_meta),
      K(out_obj_meta.get_subschema_id()), K(expr.extra_));
  }
  return ret;
}

////////////////////////////////////////////////////////////
// str -> udt;
CAST_FUNC_NAME(string, udt)
{
  EVAL_STRING_ARG()
  {
  const ObObjMeta &in_obj_meta = expr.args_[0]->obj_meta_;
  ObObjType in_type = expr.args_[0]->datum_meta_.type_;
  ObObjType out_type = expr.datum_meta_.type_;
  ObCollationType in_cs_type;
  ObCollationType out_cs_type = expr.datum_meta_.cs_type_;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
  ObString in_str(child_res->len_, child_res->ptr_);
  ObDatum t_res_datum;
  ObMulModeMemCtx* mem_ctx = nullptr;
  ObXmlDocument* doc = nullptr;
  ObString xml_plain_text;
  if (OB_FAIL(ObXmlUtil::create_mulmode_tree_context(&temp_allocator, mem_ctx))) {
    LOG_WARN("fail to create tree memory context", K(ret));
  } else if (in_obj_meta.is_string_type()) {
    // first step cs_type transform
    in_cs_type = expr.args_[0]->datum_meta_.cs_type_;
    if (ObCharset::charset_type_by_coll(in_cs_type) != CHARSET_UTF8MB4) {
      bool has_set_res = false;
      OZ(common_string_string(expr, in_type, in_cs_type, ObObjType::ObVarcharType,
                              CS_TYPE_UTF8MB4_BIN, in_str, ctx, t_res_datum, has_set_res));
    } else {
      OZ(common_copy_string(expr, in_str, ctx, t_res_datum));
    }

    // second step xmlparse document
    xml_plain_text = t_res_datum.get_string();
    ObXmlParser parser(mem_ctx);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(parser.parse_document(xml_plain_text))) {
      ret = OB_ERR_XML_PARSE;
      LOG_USER_ERROR(OB_ERR_XML_PARSE);
      LOG_WARN("parse xml plain text as document failed.", K(xml_plain_text));
    } else {
      doc = parser.document();
    }

    if (OB_FAIL(ret)) {
    } else if (!doc->get_encoding().empty() || doc->get_encoding_flag()) {
      doc->set_encoding(ObXmlUtil::get_charset_name(in_cs_type));
    }
    if (OB_SUCC(ret) && OB_FAIL(ObXMLExprHelper::pack_xml_res(expr, ctx, res_datum, doc, mem_ctx,
                                              M_DOCUMENT,
                                              xml_plain_text))) {
      LOG_WARN("pack_xml_res failed", K(ret));
    }
  }
  }
  return ret;
}

CAST_FUNC_NAME(udt, string)
{
  int ret = OB_SUCCESS;
  ObDatum *child_res = NULL;
  const ObObjMeta &in_obj_meta = expr.args_[0]->obj_meta_;
  const ObObjMeta &out_obj_meta = expr.obj_meta_;
  if (!in_obj_meta.is_xml_sql_type()) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN_RET(OB_ERR_INVALID_TYPE_FOR_OP, "inconsistent datatypes",
      "expected", out_obj_meta.get_type(), "got", in_obj_meta.get_type(),
      K(in_obj_meta.get_subschema_id()));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, child_res))) {
    LOG_WARN("eval arg failed", K(ret), K(ctx));
  } else if (child_res->is_null() ||
             (lib::is_oracle_mode() && 0 == child_res->len_
              && ObLongTextType != expr.args_[0]->datum_meta_.type_)) {
    // udt(xmltype) can be null: select dump(xmlparse(document NULL)) from dual;
    res_datum.set_null();
  } else {
    ObString blob_data = child_res->get_string();
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    ObIMulModeBase *xml_root = NULL;
    ObStringBuffer xml_plain_text(&temp_allocator);
    ObCollationType session_cs_type = CS_TYPE_UTF8MB4_BIN;
    GET_SESSION() {
      session_cs_type = session->get_nls_collation();
    }
    if (OB_FAIL(ObTextStringHelper::read_real_string_data(&temp_allocator,
                                                          ObLongTextType,
                                                          CS_TYPE_BINARY,
                                                          true,
                                                          blob_data,
                                                          &ctx.exec_ctx_))) {
      LOG_WARN("fail to get real data.", K(ret), K(blob_data));
    } else if (OB_FAIL(ObXmlUtil::cast_to_string(blob_data, temp_allocator, xml_plain_text, session_cs_type))) {
      LOG_WARN("failed to convert xml to string", K(ret), KP(blob_data.ptr()), K(blob_data.length()));
    } else { // use clob before xml binary implemented
      ObObjType in_type = ObLongTextType;
      ObObjType out_type = expr.datum_meta_.type_;
      ObCollationType in_cs_type = CS_TYPE_UTF8MB4_BIN;
      ObCollationType out_cs_type = expr.datum_meta_.cs_type_;
      bool has_set_res = false;
      OZ(common_string_string(expr, in_type, in_cs_type, out_type,
                              out_cs_type, xml_plain_text.string(), ctx, res_datum, has_set_res));
      const ObString res_str = res_datum.get_string();  // res str need deep copy in pl mode
      if (OB_SUCC(ret) && OB_FAIL(common_copy_string(expr, res_str, ctx, res_datum))) {
        LOG_WARN("fail to deep copy str", K(ret));
      }
    }
  }
  return ret;
}

int get_udt_id(sql::ObEvalCtx &ctx, const ObObjMeta &obj_meta, uint64_t &udt_id) {
  int ret = OB_SUCCESS;
  const ObObjType type = obj_meta.get_type();
  if (type != ObUserDefinedSQLType) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error input type", K(ret), K(obj_meta));
  } else {
    const uint16_t subschema_id = obj_meta.get_subschema_id();
    ObSqlUDTMeta udt_meta;
    // Notice: udt_type_id (accuray) does not exist in output obj meta,
    // should set subschema_id on input obj_meta in code generation
    if (OB_FAIL(ctx.exec_ctx_.get_sqludt_meta_by_subschema_id(subschema_id, udt_meta))) {
      LOG_WARN("failed to get udt meta", K(ret), K(subschema_id));
    } else {
      udt_id = udt_meta.udt_id_;
    }
  }
  return ret;
}

CAST_FUNC_NAME(udt, udt)
{
  EVAL_STRING_ARG()
  {
    const ObObjMeta &in_obj_meta = expr.args_[0]->obj_meta_;
    const ObObjMeta &out_obj_meta = expr.obj_meta_;
    uint64_t in_udt_id = T_OBJ_NOT_SUPPORTED;
    uint64_t out_udt_id = T_OBJ_NOT_SUPPORTED;
    if (OB_FAIL(get_udt_id(ctx, in_obj_meta, in_udt_id))) {
      LOG_WARN("fail to get udt id from obj meta", K(ret));
    } else if (OB_FAIL(get_udt_id(ctx, out_obj_meta, out_udt_id))) {
      LOG_WARN("fail to get udt id from obj meta", K(ret));
    } else if (ObGeometryTypeCastUtil::is_sdo_geometry_type_compatible(in_udt_id, out_udt_id)) {
      ObDatum *child_res = NULL;
      ObExprStrResAlloc expr_res_alloc(expr, ctx);
      if (OB_FAIL(expr.args_[0]->eval(ctx, child_res))) {
        LOG_WARN("eval arg failed", K(ret), K(ctx));
      } else if (OB_FAIL(res_datum.deep_copy(*child_res, expr_res_alloc))) {
        LOG_WARN("Failed to deep copy from res datum", K(ret));
      }
    } else {
      ret = cast_udt_to_other_not_support(expr, ctx, res_datum);
    }
  }
  return ret;
}

CAST_FUNC_NAME(pl_extend, string)
{
  EVAL_STRING_ARG()
  {
#ifdef OB_BUILD_ORACLE_PL
    const ObObjMeta &in_obj_meta = expr.args_[0]->obj_meta_;
    const ObObjMeta &out_obj_meta = expr.obj_meta_;
     if (pl::PL_OPAQUE_TYPE == in_obj_meta.get_extend_type()) {
      pl::ObPLOpaque *pl_src = reinterpret_cast<pl::ObPLOpaque*>(child_res->get_ext());
      if (OB_ISNULL(pl_src)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("failed to get pl data type info", K(ret), K(in_obj_meta));
      } else if (pl_src->get_type() == pl::ObPLOpaqueType::PL_XML_TYPE) {
        pl::ObPLXmlType * xmltype = static_cast<pl::ObPLXmlType*>(pl_src);
        ObObj *blob_obj = xmltype->get_data();
        if (OB_ISNULL(blob_obj)) {
          ret = OB_ERR_NULL_VALUE;
          LOG_WARN("Unexpected xml data", K(ret), K(*xmltype));
        } else {
          ObString blob_data = blob_obj->get_string();
          ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
          common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
          ObStringBuffer xml_plain_text(&temp_allocator);
          ObCollationType session_cs_type = CS_TYPE_UTF8MB4_BIN;
          GET_SESSION() {
            session_cs_type = session->get_nls_collation();
          }
          if (OB_FAIL(ObTextStringHelper::read_real_string_data(&temp_allocator,
                                                                ObLongTextType,
                                                                CS_TYPE_BINARY,
                                                                true,
                                                                blob_data,
                                                                &ctx.exec_ctx_))) {
            LOG_WARN("fail to get real data.", K(ret), K(blob_data));
          } else if (OB_FAIL(ObXmlUtil::cast_to_string(blob_data, temp_allocator, xml_plain_text, session_cs_type))) {
            LOG_WARN("failed to convert xml to string", K(ret), KP(blob_data.ptr()), K(blob_data.length()));
          } else { // use clob before xml binary implemented
            ObObjType in_type = ObLongTextType;
            ObObjType out_type = expr.datum_meta_.type_;
            ObCollationType in_cs_type = CS_TYPE_UTF8MB4_BIN;
            ObCollationType out_cs_type = expr.datum_meta_.cs_type_;
            bool has_set_res = false;
            OZ(common_string_string(expr, in_type, in_cs_type, out_type,
                                    out_cs_type, xml_plain_text.string(), ctx, res_datum, has_set_res));
            const ObString res_str = res_datum.get_string();  // res str need deep copy in pl mode
            if (OB_SUCC(ret) && OB_FAIL(common_copy_string(expr, res_str, ctx, res_datum))) {
              LOG_WARN("fail to deep copy str", K(ret));
            }
          }
        }
      } else if (pl_src->get_type() == pl::ObPLOpaqueType::PL_INVALID) {
        // possibly an un-initiated pl variable, for example, xml_data and xml_data2 are only declared
        // then call this directly: select replace(xml_data,xml_data2 ,'1') into stringval from dual;
        res_datum.set_null();
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected type to convert pl format",
          K(ret), K(pl_src->get_type()), K(in_obj_meta), K(out_obj_meta));
      }
    } else {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN_RET(OB_ERR_INVALID_TYPE_FOR_OP, "inconsistent datatypes",
        "expected", out_obj_meta.get_type(), "got", in_obj_meta.get_type(),
        K(in_obj_meta.get_subschema_id()));
    }
#else
  ret = OB_NOT_SUPPORTED;
#endif
  }
  return ret;
}

#ifdef OB_BUILD_ORACLE_PL
int cast_sql_xml_pl_xml(const sql::ObExpr &expr,
                        sql::ObEvalCtx &ctx,
                        sql::ObDatum &res_datum,
                        sql::ObDatum *child_res)
{
  int ret = OB_SUCCESS;
  pl::ObPLXmlType *xmltype = NULL;
  void *ptr = NULL;
  ObObj* data = NULL; // obobj for blob;
  ObIAllocator &allocator = ctx.exec_ctx_.get_allocator();
  if (OB_ISNULL(data = static_cast<ObObj*>(allocator.alloc(sizeof(ObObj))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for pl object", K(ret));
  } else if (OB_ISNULL(ptr = allocator.alloc(sizeof(pl::ObPLXmlType)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to allocate memory for pl xml data type", K(ret), K(sizeof(pl::ObPLXmlType)));
  } else if (FALSE_IT(xmltype = new (ptr)pl::ObPLXmlType())) {
  } else if (OB_ISNULL(data = static_cast<ObObj*>(allocator.alloc(sizeof(ObObj))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for pl object", K(ret));
  } else {
    ObString xml_data;
    if (!child_res->is_null() && !child_res->get_string().empty()) {
      /*
      We believe that the memory in child_res is reliable,
      and the content in it can be passed to the next layer without copying.
      Therefore, there is no deep copy of row-level memory here.
      If you find that the memory of xml_data is unstable later,
      you can add a deep copy here to copy the data to the memory of expr: get_str_res_mem().
      */
      xml_data = child_res->get_string();
      data->set_string(ObLongTextType, xml_data.ptr(), xml_data.length());
      data->set_has_lob_header(); // must has lob header
      data->set_collation_type(CS_TYPE_UTF8MB4_BIN);
    } else {
      data->set_null();
    }
    if (OB_SUCC(ret)) {
      ObObj result_obj;
      xmltype->set_data(data);
      result_obj.set_extend(reinterpret_cast<int64_t>(xmltype), pl::PL_OPAQUE_TYPE);
      if (OB_FAIL(res_datum.from_obj(result_obj, expr.obj_datum_map_))) { // check obj_datum_map_
        LOG_WARN("failed to set extend datum from obj", K(ret));
      }
    }
  }

  return ret;
}
#endif

CAST_FUNC_NAME(sql_udt, pl_extend)
{
  // Convert sql udt type to pl udt type, currently only some system defined types are supported
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_ORACLE_PL
  const ObObjMeta &in_obj_meta = expr.args_[0]->obj_meta_;
  const ObObjMeta &out_obj_meta = expr.obj_meta_;
  const ObObjType in_type = in_obj_meta.get_type();
  ObDatum *child_res = NULL;
  if (in_type != ObUserDefinedSQLType && in_type != ObCollectionSQLType) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error input type", K(ret), K(in_obj_meta), K(out_obj_meta));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, child_res))) {
    LOG_WARN("eval arg failed", K(ret), K(ctx));
  } else {
    const uint16_t subschema_id = in_obj_meta.get_subschema_id();
    ObSqlUDTMeta udt_meta;
    // Notice: udt_type_id (accuray) does not exist in output obj meta,
    // should set subschema_id on input obj_meta in code generation
    if (OB_FAIL(ctx.exec_ctx_.get_sqludt_meta_by_subschema_id(subschema_id, udt_meta))) {
      LOG_WARN("failed to get udt meta", K(ret), K(subschema_id));
    } else if (udt_meta.udt_id_ == T_OBJ_XML) {
      if (OB_FAIL(cast_sql_xml_pl_xml(expr, ctx, res_datum, child_res))) {
        LOG_WARN("failed to cast sql xmltype to pl xmltype", K(ret));
      }
    } else if (udt_meta.pl_type_ == pl::PL_RECORD_TYPE || udt_meta.pl_type_ == pl::PL_VARRAY_TYPE) {
      ObString udt_data = child_res->get_string();
      ObObj result;
      if (OB_FAIL(ObSqlUdtUtils::cast_sql_record_to_pl_record(&ctx.exec_ctx_,
                                                              result,
                                                              udt_data,
                                                              udt_meta))) {
        LOG_WARN("failed to cast sql collection to pl collection", K(ret), K(udt_meta.udt_id_));
      } else if (OB_FAIL(res_datum.from_obj(result, expr.obj_datum_map_))) {
        LOG_WARN("Failed to deep copy element object value", K(ret), K(result));
      }
    } else {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("inconsistent datatypes", K(ret), K(out_obj_meta.get_type()),
               K(in_obj_meta.get_type()), K(subschema_id), K(udt_meta.udt_id_));
    }
  }
#else
  ret = OB_NOT_SUPPORTED;
#endif
  return ret;
}

#ifdef OB_BUILD_ORACLE_PL
int covert_pl_xml_to_sql_xml(const sql::ObExpr &expr,
                             sql::ObEvalCtx &ctx,
                             sql::ObDatum &res_datum,
                             const ObObjMeta &in_obj_meta,
                             sql::ObDatum *child_res)
{
  int ret = OB_SUCCESS;

  if (pl::PL_OPAQUE_TYPE == in_obj_meta.get_extend_type()) {
    pl::ObPLOpaque *pl_src = reinterpret_cast<pl::ObPLOpaque*>(child_res->get_ext());
    if (OB_ISNULL(pl_src)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("failed to get pl data type info", K(ret), K(in_obj_meta));
    } else if (pl_src->get_type() == pl::ObPLOpaqueType::PL_XML_TYPE) {
      pl::ObPLXmlType * xmltype = static_cast<pl::ObPLXmlType*>(pl_src);
      ObObj *blob_obj = xmltype->get_data();
      if (OB_ISNULL(blob_obj) || blob_obj->is_null()) {
        res_datum.set_null();
      } else {
        ObString xml_data = blob_obj->get_string();
        int64_t xml_data_size = xml_data.length();
        char *xml_data_buff = expr.get_str_res_mem(ctx, xml_data_size);
        if (OB_ISNULL(xml_data_buff)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory for xmldata", K(ret), K(xml_data_size));
        } else {
          MEMCPY(xml_data_buff, xml_data.ptr(), xml_data_size);
          res_datum.set_string(ObString(xml_data_size , xml_data_buff));
        }
      }
    } else if (pl_src->get_type() == pl::ObPLOpaqueType::PL_INVALID) {
      // un-initiated pl opaque variable
      res_datum.set_null();
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected type to convert pl format", K(ret), K(pl_src->get_type()), K(in_obj_meta));
    }
  } else {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("Unexpected type to convert pl udt to sql udt format", K(ret), K(in_obj_meta));
  }
  return ret;
}
#endif

CAST_FUNC_NAME(pl_extend, sql_udt)
{
  // Convert sql udt type to pl udt type, currently only xmltype is supported
  // Notice: udt_type_id (accuray) does not exist in input obj meta,
  // should set subschema_id on output obj_meta in code generation
  EVAL_STRING_ARG()
  {
#ifdef OB_BUILD_ORACLE_PL
    const ObObjMeta &in_obj_meta = expr.args_[0]->obj_meta_;
    const ObObjType in_type = in_obj_meta.get_type();
    const ObObjMeta &out_obj_meta = expr.obj_meta_;
    const uint16_t subschema_id = out_obj_meta.get_subschema_id();

    ObSqlUDT sql_udt;
    ObSqlUDTMeta udt_meta;
    ObObj root_obj;
    ObString res_str;
    ObExprStrResAlloc expr_res_alloc(expr, ctx);

    if (in_type != ObExtendType) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error input type", K(ret), K(in_obj_meta), K(out_obj_meta));
    } else if (OB_FAIL(child_res->to_obj(root_obj, in_obj_meta))) {
        LOG_WARN("failed to get root obj", K(ret), K(in_obj_meta), K(child_res));
    } else if (OB_FAIL(ctx.exec_ctx_.get_sqludt_meta_by_subschema_id(subschema_id, udt_meta))) {
      LOG_WARN("failed to get udt meta", K(ret), K(subschema_id));
    } else if (udt_meta.udt_id_ == T_OBJ_XML) {
      if (OB_FAIL(covert_pl_xml_to_sql_xml(expr, ctx, res_datum, in_obj_meta, child_res))) {
        LOG_WARN("failed to cast pl xml to sql xml", K(ret), K(in_obj_meta));
      }
    } else if (!ObObjUDTUtil::ob_is_supported_sql_udt(udt_meta.udt_id_)) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("inconsistent datatypes", K(ret), K(out_obj_meta.get_type()),
               K(in_obj_meta.get_type()), K(subschema_id), K(udt_meta.udt_id_));
    } else if (FALSE_IT(sql_udt.set_udt_meta(udt_meta))) {
    } else if (root_obj.get_ext() == 0) {
      res_datum.set_null();
    } else if (sql_udt.get_udt_meta().pl_type_ == pl::PL_VARRAY_TYPE) { // single varray
      pl::ObPLVArray *varray = reinterpret_cast<pl::ObPLVArray *>(root_obj.get_ext());
      if (OB_ISNULL(varray)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("failed to get pl data type info", K(ret));
      } else if (varray->is_null()) {
        res_datum.set_null();
      } else if (OB_FAIL(ObSqlUdtUtils::cast_pl_varray_to_sql_varray(expr_res_alloc, res_str, root_obj))) {
        LOG_WARN("convert pl record to sql record failed",
                 K(ret), K(subschema_id), K(udt_meta.udt_id_));
      } else {
        res_datum.set_string(res_str);
      }
    } else { // record
      ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
      common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
      pl::ObPLRecord *record = reinterpret_cast<pl::ObPLRecord *>(root_obj.get_ext());
      if (OB_ISNULL(record)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("failed to get pl data type info", K(ret));
      } else if (record->is_null()) {
        res_datum.set_null();
      } else if (OB_FAIL(ObSqlUdtUtils::cast_pl_record_to_sql_record(temp_allocator,
                                                              expr_res_alloc,
                                                              &ctx.exec_ctx_,
                                                              res_str,
                                                              sql_udt,
                                                              root_obj))) {
        LOG_WARN("convert pl record to sql record failed",
                 K(ret), K(subschema_id), K(udt_meta.udt_id_));
      } else {
        res_datum.set_string(res_str);
      }
    }
#else
  ret = OB_NOT_SUPPORTED;
#endif
  }
  return ret;
}

CAST_FUNC_NAME(pl_extend, geometry)
{
  // Convert sql udt type to pl udt type, currently only xmltype is supported
  // Notice: udt_type_id (accuray) does not exist in input obj meta,
  // should set subschema_id on output obj_meta in code generation
  EVAL_STRING_ARG()
  {
#ifdef OB_BUILD_ORACLE_PL
    const ObObjMeta &in_obj_meta = expr.args_[0]->obj_meta_;
    const ObObjType in_type = in_obj_meta.get_type();
    const ObObjMeta &out_obj_meta = expr.obj_meta_;
    ObObj root_obj;
    ObIAllocator &allocator = ctx.exec_ctx_.get_allocator();
    uint64_t tenant_id = ctx.exec_ctx_.get_my_session()->get_effective_tenant_id();
    ObString res_wkb;

    if (in_type != ObExtendType) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error input type", K(ret), K(in_obj_meta), K(out_obj_meta));
    } else if (OB_FAIL(child_res->to_obj(root_obj, in_obj_meta))) {
      LOG_WARN("failed to get root obj", K(ret), K(in_obj_meta), K(child_res));
    } else {
      pl::ObPLRecord *pl_src = reinterpret_cast<pl::ObPLRecord*>(root_obj.get_ext());
      if (pl_src->is_null()) {
        res_datum.set_null();
      } else {
        if (OB_FAIL(pl::ObSdoGeometry::pl_extend_to_wkb(&allocator, root_obj, tenant_id, res_wkb))) {
          LOG_WARN("failed to get geometry wkb from pl extend", K(ret), K(in_obj_meta), K(child_res));
        } else if (OB_FAIL(common_gis_wkb(expr, ctx, res_datum, res_wkb))){
          LOG_WARN("fail to copy string", K(ret));
        }
      }
    }
#else
  ret = OB_NOT_SUPPORTED;
#endif
  }
  return ret;
}

CAST_FUNC_NAME(geometry, pl_extend)
{
  // Convert sql udt type to pl udt type, currently only xmltype is supported
  // Notice: udt_type_id (accuray) does not exist in input obj meta,
  // should set subschema_id on output obj_meta in code generation
  EVAL_STRING_ARG()
  {
#ifdef OB_BUILD_ORACLE_PL
    ObIAllocator &allocator = ctx.exec_ctx_.get_allocator();
    ObString wkb = child_res->get_string();
    ObObj result;
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *child_res,
                expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), wkb,
                &ctx.exec_ctx_))) {
      LOG_WARN("fail to get real data.", K(ret), K(wkb));
    } else if (OB_FAIL(pl::ObSdoGeometry::wkb_to_pl_extend(allocator, &ctx.exec_ctx_, wkb, result))) {
      LOG_WARN("failed to get geometry wkb from pl extend", K(ret));
    } else if (OB_FAIL(res_datum.from_obj(result, expr.obj_datum_map_))) {
      LOG_WARN("Failed to deep copy element object value", K(ret), K(result));
    }
#else
  ret = OB_NOT_SUPPORTED;
#endif
  }
  return ret;
}

// 显式cast时，再次从parse node中获取accuracy信息(see ob_expr_cast.cpp)
// 可能是因为cast类型推导不准，导致必须再次从parse node中获取
int get_accuracy_from_parse_node(const ObExpr &expr, ObEvalCtx &ctx,
                                 ObAccuracy &accuracy, ObObjType &dest_type)
{
  int ret = OB_SUCCESS;
  ObDatum *dst_type_dat = NULL;
  ObExprResType dst_type;
  if (OB_UNLIKELY(2 != expr.arg_cnt_) || OB_ISNULL(expr.args_) ||
      OB_ISNULL(expr.args_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected expr", K(ret), K(expr.arg_cnt_), KP(expr.args_));
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, dst_type_dat))) {
    LOG_WARN("eval dst type datum failed", K(ret));
  } else {
    int64_t maxblen = ObCharset::CharConvertFactorNum;
    ParseNode node;
    node.value_ = dst_type_dat->get_int();
    ObObjType obj_type = static_cast<ObObjType>(node.int16_values_[OB_NODE_CAST_TYPE_IDX]);
    dst_type.set_collation_type(static_cast<ObCollationType>(node.int16_values_[OB_NODE_CAST_COLL_IDX]));
    dst_type.set_type(obj_type);
    int64_t text_length = node.int32_values_[1];
    if (lib::is_mysql_mode() && !dst_type.is_binary() && !dst_type.is_varbinary()) {
      dst_type.set_full_length(node.int32_values_[OB_NODE_CAST_C_LEN_IDX], expr.datum_meta_.length_semantics_);
      if (dst_type.get_length() > OB_MAX_CAST_CHAR_VARCHAR_LENGTH && dst_type.get_length() <= OB_MAX_CAST_CHAR_TEXT_LENGTH) {
        dst_type.set_type(ObTextType);
        dst_type.set_length(OB_MAX_CAST_CHAR_TEXT_LENGTH);
      } else if (dst_type.get_length() > OB_MAX_CAST_CHAR_TEXT_LENGTH && dst_type.get_length() <= OB_MAX_CAST_CHAR_MEDIUMTEXT_LENGTH) {
        dst_type.set_type(ObMediumTextType);
        dst_type.set_length(OB_MAX_CAST_CHAR_MEDIUMTEXT_LENGTH);
      } else if (dst_type.get_length() > OB_MAX_CAST_CHAR_MEDIUMTEXT_LENGTH) {
        dst_type.set_type(ObLongTextType);
        dst_type.set_length(OB_MAX_LONGTEXT_LENGTH / maxblen);
      }
      text_length = dst_type.get_length();
    }
    dest_type = dst_type.get_type();
    ObObjTypeClass dest_tc = ob_obj_type_class(dest_type);
    if (ObStringTC == dest_tc) {
      // parser will abort all negative number
      // if length < 0 means DEFAULT_STR_LENGTH or OUT_OF_STR_LEN.
      accuracy.set_full_length(node.int32_values_[1], expr.datum_meta_.length_semantics_,
                               lib::is_oracle_mode());
    } else if (ObRawTC == dest_tc) {
      accuracy.set_length(node.int32_values_[1]);
    } else if(ObTextTC == dest_tc || ObJsonTC == dest_tc) {
      accuracy.set_length(text_length < 0 ?
          ObAccuracy::DDL_DEFAULT_ACCURACY[dest_type].get_length() : text_length);
    } else if (ObIntervalTC == dest_tc) {
      if (OB_UNLIKELY(!ObIntervalScaleUtil::scale_check(node.int16_values_[3]) ||
                      !ObIntervalScaleUtil::scale_check(node.int16_values_[2]))) {
        ret = OB_ERR_DATETIME_INTERVAL_PRECISION_OUT_OF_RANGE;
      } else {
        ObScale scale = (dest_type == ObIntervalYMType) ?
          ObIntervalScaleUtil::interval_ym_scale_to_ob_scale(
              static_cast<int8_t>(node.int16_values_[3]))
          : ObIntervalScaleUtil::interval_ds_scale_to_ob_scale(
              static_cast<int8_t>(node.int16_values_[2]),
              static_cast<int8_t>(node.int16_values_[3]));
        accuracy.set_scale(scale);
      }
    } else {
      const ObAccuracy &def_acc =
        ObAccuracy::DDL_DEFAULT_ACCURACY2[lib::is_oracle_mode()][dest_type];
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

int uint_to_enum(const uint64_t input_value,
                 const ObIArray<ObString> &str_values,
                 const ObCastMode &cast_mode,
                 int &warning,
                 uint64_t &output_value)
{
  int ret = OB_SUCCESS;
  uint64_t value = input_value;
  if (OB_UNLIKELY(0 == value || value > str_values.count())) {
    if (CM_IS_WARN_ON_FAIL(cast_mode)) {
      value = 0;
      warning = OB_ERR_DATA_TRUNCATED;
      LOG_INFO("input value out of range, set zero", K(input_value), K(str_values.count()),
               K(warning));
    } else {
      ret = OB_ERR_DATA_TRUNCATED;
      LOG_WARN("input value out of range", K(input_value), K(str_values.count()), K(ret));
    }
  }
  output_value = value;
  LOG_DEBUG("finish uint_to_enum", K(ret), K(input_value), K(str_values),
            K(output_value), K(lbt()));
  return ret;
}

int uint_to_set(const uint64_t input_value,
                const ObIArray<ObString> &str_values,
                const ObCastMode &cast_mode,
                int &warning,
                uint64_t &output_value)
{
  int ret = OB_SUCCESS;
  uint64_t value = input_value;
  int64_t val_cnt = str_values.count();
  if (val_cnt >= 64) {
    //do nothing
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
  LOG_DEBUG("finish uint_to_set", K(ret), K(input_value), K(str_values),
            K(output_value), K(lbt()));
  return ret;
}

CAST_ENUMSET_FUNC_NAME(int, enum)
{
  EVAL_ARG() {
    int warning = 0;
    if (child_res->is_null()) {
      res_datum.set_null();
    } else {
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
    int warning = 0;
    uint64_t val_uint = static_cast<uint64_t>(child_res->get_int());
    uint64_t value = 0;
    ret = uint_to_set(val_uint, str_values, cast_mode, warning, value);
    SET_RES_SET(value);
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
    int warning = 0;
    uint64_t val_uint = child_res->get_uint();
    uint64_t value = 0;
    ret = uint_to_set(val_uint, str_values, cast_mode, warning, value);
    SET_RES_SET(value);
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
    int warning = 0;
    uint64_t val_uint = static_cast<uint64_t>(static_cast<int64_t>(child_res->get_float()));
    uint64_t value = 0;
    ret = uint_to_set(val_uint, str_values, cast_mode, warning, value);
    SET_RES_SET(value);
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
    int warning = 0;
    uint64_t val_uint = static_cast<uint64_t>(static_cast<int64_t>(child_res->get_double()));
    uint64_t value = 0;
    ret = uint_to_set(val_uint, str_values, cast_mode, warning, value);
    SET_RES_SET(value);
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
    //do nothing
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
    //do nothing
  } else {
    uint64_t val_uint = static_cast<uint64_t>(static_cast<int64_t>(res_datum.get_double()));
    uint64_t value = 0;
    ret = uint_to_set(val_uint, str_values, cast_mode, warning, value);
    SET_RES_SET(value);
  }
  return ret;
}

CAST_FUNC_NAME(number, decimalint)
{
  EVAL_ARG()
  {
    int16_t out_scale = expr.datum_meta_.scale_;
    ObDecimalIntBuilder tmp_alloc;
    ObDecimalInt *decint = nullptr;
    int32_t int_bytes = 0;
    number::ObNumber nmb(child_res->get_number());
    ObScale in_scale = nmb.get_scale();
    ObPrecision in_prec = expr.args_[0]->datum_meta_.precision_;
    ObPrecision out_prec = expr.datum_meta_.precision_;
    int32_t out_bytes = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(out_prec);
    if (OB_FAIL(wide::from_number(nmb, tmp_alloc, in_scale, decint, int_bytes))) {
      LOG_WARN("from_number failed", K(ret), K(out_scale));
    } else if (ObDatumCast::need_scale_decimalint(in_scale, int_bytes, out_scale, out_bytes)) {
      ObDecimalIntBuilder res_val;
      if (OB_FAIL(ObDatumCast::common_scale_decimalint(decint, int_bytes, in_scale, out_scale, out_prec,
                                          expr.extra_, res_val, ctx.exec_ctx_.get_user_logging_ctx()))) {
        LOG_WARN("scale decimal int failed", K(ret), K(in_scale), K(out_scale));
      } else {
        res_datum.set_decimal_int(res_val.get_decimal_int(), res_val.get_int_bytes());
      }
    } else {
      res_datum.set_decimal_int(decint, int_bytes);
    }
  }
  return ret;
}

int string_to_enum(ObIAllocator &alloc,
                   const ObString &orig_in_str,
                   const ObCollationType in_cs_type,
                   const ObIArray<ObString> &str_values,
                   const uint64_t cast_mode,
                   const ObExpr &expr,
                   int &warning,
                   uint64_t &output_value)
{
  int ret = OB_SUCCESS;
  const ObCollationType cs_type = expr.obj_meta_.get_collation_type();
  uint64_t value = 0;
  int32_t pos = 0;
  ObString in_str;
  OZ(ObCharset::charset_convert(alloc, orig_in_str, in_cs_type, cs_type, in_str));
  int32_t no_sp_len = static_cast<int32_t>(ObCharset::strlen_byte_no_sp(cs_type,
      in_str.ptr(), in_str.length()));
  ObString no_sp_val(0, static_cast<ObString::obstr_size_t>(no_sp_len), in_str.ptr());
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(find_type(str_values, cs_type, no_sp_val, pos))) {
    LOG_WARN("fail to find type", K(str_values), K(cs_type), K(no_sp_val),
             K(in_str), K(pos), K(ret));
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
    value = pos + 1;//enum start from 1
  }
  output_value = value;
  LOG_DEBUG("finish string_enum", K(ret), K(in_str), K(str_values), K(expr),
            K(output_value), K(lbt()));
  return ret;
}

CAST_ENUMSET_FUNC_NAME(string, enum)
{
  EVAL_ARG() {
    int warning = 0;
    const ObString in_str = child_res->get_string();
    uint64_t value = 0;
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    ret = string_to_enum(alloc_guard.get_allocator(),
                         in_str, expr.args_[0]->datum_meta_.cs_type_,
                         str_values, cast_mode, expr, warning, value);
    SET_RES_ENUM(value);
  }
  return ret;
}

CAST_ENUMSET_FUNC_NAME(text, enum)
{
  EVAL_STRING_ARG() {
    ObString in_str(child_res->len_, child_res->ptr_);
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    OZ(get_text_full_data(expr, ctx, &temp_allocator, child_res, in_str));
    if (OB_SUCC(ret)) {
      int warning = 0;
      uint64_t value = 0;
      ObEvalCtx::TempAllocGuard alloc_guard(ctx);
      ret = string_to_enum(alloc_guard.get_allocator(),
                           in_str, expr.args_[0]->datum_meta_.cs_type_,
                           str_values, cast_mode, expr, warning, value);
      SET_RES_ENUM(value);
    }
  }
  return ret;
}

int string_to_set(ObIAllocator &alloc,
                  const ObString &orig_in_str,
                  const ObCollationType in_cs_type,
                  const ObIArray<ObString> &str_values,
                  const uint64_t cast_mode,
                  const ObExpr &expr,
                  int &warning,
                  uint64_t &output_value)
{
  int ret = OB_SUCCESS;
  uint64_t value = 0;
  ObString in_str;
  const ObCollationType cs_type = expr.obj_meta_.get_collation_type();
  OZ(ObCharset::charset_convert(alloc, orig_in_str, in_cs_type, cs_type, in_str));
  if (OB_FAIL(ret)) {
  } else if (in_str.empty()) {
    //do noting
  } else {
    bool is_last_value = false;
    const ObString &sep = ObCharsetUtils::get_const_str(cs_type, ',');
    int32_t pos = 0;
    const char *remain = in_str.ptr();
    int64_t remain_len = ObCharset::strlen_byte_no_sp(cs_type, in_str.ptr(), in_str.length());
    ObString val_str;
    do {
      pos = 0;
      const char *sep_loc = NULL;
      if (NULL == (sep_loc = static_cast<const char *>(memmem(
                      remain, remain_len, sep.ptr(), sep.length())))) {
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
      } else if (OB_UNLIKELY(pos < 0)) {//not found
        if (CM_IS_WARN_ON_FAIL(cast_mode)) {
          warning = OB_ERR_DATA_TRUNCATED;
          LOG_INFO("input value out of range, and set out value zero", K(pos), K(expr),
                   K(val_str), K(in_str), K(warning));
        } else {
          ret = OB_ERR_DATA_TRUNCATED;
          LOG_WARN("data truncate", K(pos), K(expr), K(val_str), K(in_str), K(ret));
        }
      } else {
        pos %= 64;//MySQL中，如果value存在重复，则value_count可以大于64
        value |= (1ULL << pos);
      }
    } while (OB_SUCC(ret) && !is_last_value);
  }

  // Bug30666903: check implicit cast logic to handle number cases
  if (in_str.is_numeric() &&
      (OB_ERR_DATA_TRUNCATED == ret
      || (OB_ERR_DATA_TRUNCATED == warning && CM_IS_WARN_ON_FAIL(cast_mode)))) {
    int err = 0;
    value = ObCharset::strntoull(in_str.ptr(), in_str.length(), 10, &err);
    if(err == 0) {
      ret = OB_SUCCESS;
      uint32_t val_cnt = str_values.count();
      if (OB_UNLIKELY(val_cnt <= 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect val_cnt", K(val_cnt), K(ret));
      } else if (val_cnt >= 64) {//do nothing
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
  LOG_DEBUG("finish string_set", K(ret), K(in_str), K(str_values), K(expr),
            K(output_value), K(lbt()));
  return ret;
}

CAST_ENUMSET_FUNC_NAME(string, set)
{
  EVAL_ARG() {
    int warning = 0;
    const ObString in_str = child_res->get_string();
    uint64_t value = 0;
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    ret = string_to_set(alloc_guard.get_allocator(),
                        in_str, expr.args_[0]->datum_meta_.cs_type_,
                        str_values, cast_mode, expr, warning, value);
    SET_RES_SET(value);
  }
  return ret;
}

CAST_ENUMSET_FUNC_NAME(text, set)
{
  EVAL_STRING_ARG() {
    ObString in_str(child_res->len_, child_res->ptr_);
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    OZ(get_text_full_data(expr, ctx, &temp_allocator, child_res, in_str));
    if (OB_SUCC(ret)) {
      int warning = 0;
      uint64_t value = 0;
      ObEvalCtx::TempAllocGuard alloc_guard(ctx);
      ret = string_to_set(alloc_guard.get_allocator(),
                          in_str, expr.args_[0]->datum_meta_.cs_type_,
                          str_values, cast_mode, expr, warning, value);
      SET_RES_ENUM(value);
    }
  }
  return ret;
}

CAST_ENUMSET_FUNC_NAME(datetime, enum)
{
  EVAL_ARG() {
    int warning = 0;
    char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
    int64_t len = 0;
    int64_t in_val = child_res->get_int();
    if (OB_FAIL(common_datetime_string(expr,
                                       expr.args_[0]->datum_meta_.type_,
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
      ObEvalCtx::TempAllocGuard alloc_guard(ctx);
      ret = string_to_enum(alloc_guard.get_allocator(),
                           in_str, ObCharset::get_system_collation(),
                           str_values, cast_mode, expr, warning, value);
      SET_RES_ENUM(value);
    }
  }
  return ret;
}

CAST_ENUMSET_FUNC_NAME(datetime, set)
{
  EVAL_ARG() {
    int warning = 0;
    char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
    int64_t len = 0;
    int64_t in_val = child_res->get_int();
    if (OB_FAIL(common_datetime_string(expr,
                                       expr.args_[0]->datum_meta_.type_,
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
      ObEvalCtx::TempAllocGuard alloc_guard(ctx);
      ret = string_to_set(alloc_guard.get_allocator(),
                          in_str, ObCharset::get_system_collation(),
                          str_values, cast_mode, expr, warning, value);
      SET_RES_SET(value);
    }
  }
  return ret;
}


CAST_ENUMSET_FUNC_NAME(date, enum)
{
  EVAL_ARG() {
    int warning = 0;
    int32_t in_val = child_res->get_date();
    char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
    int64_t len = 0;
    if (OB_FAIL(ObTimeConverter::date_to_str(in_val, buf, sizeof(buf), len))) {
      LOG_WARN("date_to_str failed", K(ret));
    } else {
      ObString in_str(len, buf);
      uint64_t value = 0;
      ObEvalCtx::TempAllocGuard alloc_guard(ctx);
      ret = string_to_enum(alloc_guard.get_allocator(),
                           in_str, ObCharset::get_system_collation(),
                           str_values, cast_mode, expr, warning, value);
      SET_RES_ENUM(value);
    }
  }
  return ret;
}

CAST_ENUMSET_FUNC_NAME(date, set)
{
  EVAL_ARG() {
    int warning = 0;
    int32_t in_val = child_res->get_date();
    char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
    int64_t len = 0;
    if (OB_FAIL(ObTimeConverter::date_to_str(in_val, buf, sizeof(buf), len))) {
      LOG_WARN("date_to_str failed", K(ret));
    } else {
      ObString in_str(len, buf);
      uint64_t value = 0;
      ObEvalCtx::TempAllocGuard alloc_guard(ctx);
      ret = string_to_set(alloc_guard.get_allocator(),
                          in_str, ObCharset::get_system_collation(),
                          str_values, cast_mode, expr, warning, value);
      SET_RES_SET(value);
    }
  }
  return ret;
}


CAST_ENUMSET_FUNC_NAME(time, enum)
{
  EVAL_ARG() {
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
      ObEvalCtx::TempAllocGuard alloc_guard(ctx);
      ret = string_to_enum(alloc_guard.get_allocator(),
                           in_str, ObCharset::get_system_collation(),
                           str_values, cast_mode, expr, warning, value);
      SET_RES_ENUM(value);
    }
  }
  return ret;
}

CAST_ENUMSET_FUNC_NAME(time, set)
{
  EVAL_ARG() {
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
      ObEvalCtx::TempAllocGuard alloc_guard(ctx);
      ret = string_to_set(alloc_guard.get_allocator(),
                          in_str, ObCharset::get_system_collation(),
                          str_values, cast_mode, expr, warning, value);
      SET_RES_SET(value);
    }
  }
  return ret;
}

CAST_ENUMSET_FUNC_NAME(year, enum)
{
  EVAL_ARG() {
    if (child_res->is_null()) {
      res_datum.set_null();
    } else  {
      int warning = 0;
      uint8_t in_val = child_res->get_year();
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
  }
  return ret;
}

CAST_ENUMSET_FUNC_NAME(year, set)
{
  EVAL_ARG() {
    int warning = 0;
    uint8_t in_val = child_res->get_year();
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
  EVAL_ARG() {
    if (child_res->is_null()) {
      res_datum.set_null();
    } else {
      int warning = 0;
      uint64_t val_uint = child_res->get_bit();
      uint64_t value = 0;
      ret = uint_to_enum(val_uint, str_values, cast_mode, warning, value);
      SET_RES_ENUM(value);
    }
  }
  return ret;
}

CAST_ENUMSET_FUNC_NAME(bit, set)
{
  EVAL_ARG() {
    int warning = 0;
    uint64_t val_uint = child_res->get_bit();
    uint64_t value = 0;
    ret = uint_to_set(val_uint, str_values, cast_mode, warning, value);
    SET_RES_SET(value);
  }
  return ret;
}

// ================
// decimalint -> xxx
CAST_FUNC_NAME(decimalint, int)
{
  EVAL_ARG()
  {
    char buffer[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
    int64_t pos = 0;
    if (OB_FAIL(wide::to_string(child_res->get_decimal_int(), child_res->get_int_bytes(),
                                expr.args_[0]->datum_meta_.scale_, buffer, sizeof(buffer), pos))) {
      LOG_WARN("to_string failed", K(ret));
    } else {
      ObString num_str(pos, buffer);
      if (OB_FAIL(common_string_int(expr, expr.extra_, num_str, false, res_datum))) {
        LOG_WARN("common_string_int failed", K(ret));
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(decimalint, uint)
{
  EVAL_ARG()
  {
    char buffer[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
    int64_t pos = 0;
    if (OB_FAIL(wide::to_string(child_res->get_decimal_int(), child_res->get_int_bytes(),
                                expr.args_[0]->datum_meta_.scale_, buffer, sizeof(buffer), pos))) {
      LOG_WARN("to_string failed", K(ret));
    } else {
      ObString num_str(pos, buffer);
      DEF_IN_OUT_TYPE();
      if (OB_FAIL(common_string_uint(expr, num_str, false, res_datum))) {
        LOG_WARN("common_string_uint failed", K(ret));
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(decimalint, float)
{
  EVAL_ARG()
  {
    char buffer[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
    int64_t pos = 0;
    if (OB_FAIL(wide::to_string(child_res->get_decimal_int(), child_res->get_int_bytes(),
                                expr.args_[0]->datum_meta_.scale_, buffer, sizeof(buffer), pos))) {
      LOG_WARN("to_string failed", K(ret));
    } else {
      ObString num_str(pos, buffer);
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

CAST_FUNC_NAME(decimalint, double)
{
  EVAL_ARG()
  {
    char buffer[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
    int64_t pos = 0;
    if (OB_FAIL(wide::to_string(child_res->get_decimal_int(), child_res->get_int_bytes(),
                                expr.args_[0]->datum_meta_.scale_, buffer, sizeof(buffer), pos))) {
      LOG_WARN("to_string failed", K(ret));
    } else {
      ObString num_str(pos, buffer);
      DEF_IN_OUT_TYPE();
      if (OB_FAIL(common_string_double(expr, in_type, expr.args_[0]->datum_meta_.cs_type_, out_type,
                                       num_str, res_datum))) {
        LOG_WARN("common_string_double failed", K(ret));
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(decimalint, number)
{
  EVAL_ARG()
  {
    int warning = OB_SUCCESS;
    ObNumStackOnceAlloc tmp_alloc;
    number::ObNumber nmb;
    LOG_DEBUG("decimalint to number", K(lbt()));
    if (OB_FAIL(wide::to_number(child_res->get_decimal_int(), child_res->get_int_bytes(),
                                expr.args_[0]->datum_meta_.scale_, tmp_alloc, nmb))) {
      LOG_WARN("to_number failed", K(ret));
    } else if (ObUNumberType == expr.datum_meta_.type_) {
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

CAST_FUNC_NAME(decimalint, string)
{
  EVAL_ARG()
  {
    if (OB_FAIL(common_decimalint_string(expr, *child_res, ctx, res_datum))) {
      LOG_WARN("common_decimalint_string failed", K(ret));
    }
  }
  return ret;
}

CAST_FUNC_NAME(decimalint, text)
{
  EVAL_ARG()
  {
    ObString res_str;
    if (OB_FAIL(common_decimalint_string(expr, *child_res, ctx, res_datum))) {
      LOG_WARN("common_decimalint_string failed", K(ret));
    } else if (OB_FAIL(copy_datum_str_with_tmp_alloc(ctx, res_datum, res_str))) {
      LOG_WARN("copy datum string with tmp allocator failed", K(ret));
    } else if (OB_FAIL(common_string_text(expr, res_str, ctx, NULL, res_datum))) {
      LOG_WARN("cast string to lob failed", K(ret));
    }
  }
  return ret;
}

CAST_ENUMSET_FUNC_NAME(decimalint, enum)
{
  int ret = OB_SUCCESS;
  int warning = 0;
  if (OB_FAIL(decimalint_double(expr, ctx, res_datum))) {
    LOG_WARN("fail to cast decimalint to double", K(expr), K(ret));
  } else if (res_datum.is_null()) {
    // do nothing
  } else {
    uint64_t val_uint = static_cast<uint64_t>(static_cast<int64_t>(res_datum.get_double()));
    uint64_t value = 0;
    ret = uint_to_enum(val_uint, str_values, cast_mode, warning, value);
    SET_RES_ENUM(value);
  }
  return ret;
}

CAST_ENUMSET_FUNC_NAME(decimalint, set)
{
  int ret = OB_SUCCESS;
  int warning = 0;
  if (OB_FAIL(decimalint_double(expr, ctx, res_datum))) {
    LOG_WARN("fail to cast decimalint to double", K(expr), K(ret));
  } else if (res_datum.is_null()) {
    // do nothing
  } else {
    uint64_t val_uint = static_cast<uint64_t>(static_cast<int64_t>(res_datum.get_double()));
    uint64_t value = 0;
    ret = uint_to_set(val_uint, str_values, cast_mode, warning, value);
    SET_RES_SET(value);
  }
  return ret;
}

CAST_FUNC_NAME(decimalint, time)
{
  EVAL_ARG()
  {
    ObScale in_scale = expr.args_[0]->datum_meta_.scale_;
    char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
    int64_t length = 0;
    const ObDecimalInt *decint = child_res->get_decimal_int();
    const int32_t int_bytes = child_res->get_int_bytes();
    if (OB_FAIL(wide::to_string(decint, int_bytes, in_scale, buf, sizeof(buf), length))) {
      LOG_WARN("to_string failed", K(ret));
    } else if (OB_FAIL(common_string_time(expr, ObString(length, buf), res_datum))) {
      LOG_WARN("convert string to time failed", K(ret));
    } else {
      // do nothing
    }
  }
  return ret;
}

CAST_FUNC_NAME(decimalint, year)
{
  EVAL_ARG()
  {
    ObScale in_scale = expr.args_[0]->datum_meta_.scale_;
    char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
    int64_t length = 0;
    const ObDecimalInt *decint = child_res->get_decimal_int();
    const int32_t int_bytes = child_res->get_int_bytes();
    bool is_neg = wide::is_negative(decint, int_bytes);
    if (is_neg) {
      // the year shouldn't accept a negative number, if we use the common_string_year.
      // number like -0.4 could be converted to year, which should raise error in mysql
      if (OB_FAIL(common_int_year(expr, INT_MIN, res_datum))) {
        LOG_WARN("common_int_year failed", K(ret));
      }
    } else if (OB_FAIL(wide::to_string(decint, int_bytes, in_scale, buf, sizeof(buf), length))) {
      LOG_WARN("to_string failed", K(ret));
    } else if (OB_FAIL(common_string_year(expr, ObString(length, buf), res_datum))) {
      LOG_WARN("common_string_year failed", K(ret));
    } else {
      // do nothing
    }
  }
  return ret;
}

CAST_FUNC_NAME(decimalint, datetime)
{
  EVAL_ARG()
  {
    GET_SESSION()
    {
      // TODO: cast directly into date
      ObNumStackOnceAlloc tmp_alloc;
      number::ObNumber nmb;
      ObScale in_scale = expr.args_[0]->datum_meta_.scale_;
      ObObjType out_type = expr.datum_meta_.type_;
      ObSolidifiedVarsGetter helper(expr, ctx, session);
      const common::ObTimeZoneInfo *tz_info_local = NULL;
      if (OB_FAIL(helper.get_time_zone_info(tz_info_local))) {
        LOG_WARN("get time zone info failed", K(ret));
      } else {
        ObTimeConvertCtx cvt_ctx(tz_info_local, ObTimestampType == out_type);
        int64_t out_val;
        if (OB_FAIL(wide::to_number(child_res->get_decimal_int(), child_res->get_int_bytes(),
                                    in_scale, tmp_alloc, nmb))) {
          LOG_WARN("cast decimaliny to number failed", K(ret));
        } else {
          ret = common_number_datetime(nmb, cvt_ctx, out_val, expr.extra_);
          int warning = OB_SUCCESS;
          if (CAST_FAIL(ret)) {
            // do nothing
          } else {
            SET_RES_DATETIME(out_val);
            if (warning != OB_SUCCESS) { LOG_DEBUG("cast decimalint to datetime", K(warning)); }
          }
        }
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(decimalint, date)
{
  EVAL_ARG()
  {
    // TODO: cast directly into date
    ObScale in_scale = expr.args_[0]->datum_meta_.scale_;
    number::ObNumber nmb;
    ObNumStackOnceAlloc tmp_alloc;
    int32_t out_val = 0;
    int warning = OB_SUCCESS;
    if (OB_FAIL(wide::to_number(child_res->get_decimal_int(), child_res->get_int_bytes(), in_scale,
                                tmp_alloc, nmb))) {
      LOG_WARN("cast decimal int to number failed", K(ret));
    } else {
      ret = common_number_date(nmb, expr.extra_, out_val);
      if (CAST_FAIL(ret)) {
        // do nothing
      } else {
        SET_RES_DATE(out_val);
      }
    }
  }
  return ret;
}

CAST_FUNC_NAME(decimalint, bit)
{
  EVAL_ARG()
  {
    ObScale in_scale = expr.args_[0]->datum_meta_.scale_;
    char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
    int64_t length = 0;
    const ObDecimalInt *decint = child_res->get_decimal_int();
    int32_t int_bytes = child_res->get_int_bytes();
    if (OB_FAIL(wide::to_string(decint, int_bytes, in_scale, buf, sizeof(buf), length))) {
      LOG_WARN("to_string failed", K(ret));
    } else if (OB_FAIL(common_string_uint(expr, ObString(length, buf), false, res_datum))) {
      LOG_WARN("string_uint failed", K(ret));
    } else {
      // do nothing
    }
  }
  return ret;
}

CAST_FUNC_NAME(decimalint, lob)
{
  EVAL_ARG()
  {
    ObString res_str;
    if (OB_FAIL(common_decimalint_string(expr, *child_res, ctx, res_datum))) {
      LOG_WARN("common_decimalint_string failed", K(ret));
    } else if (OB_FAIL(copy_datum_str_with_tmp_alloc(ctx, res_datum, res_str))) {
      LOG_WARN("copy datum string with tmp allocator failed", K(ret));
    } else if (OB_FAIL(common_string_lob(expr, res_str, ctx, NULL, res_datum))) {
      LOG_WARN("cast string to lob failed", K(ret));
    } else {
      // do nothing
    }
  }
  return ret;
}

CAST_FUNC_NAME(decimalint, json)
{
  EVAL_ARG()
  {
    // TODO: cast directly into json
    ObNumStackOnceAlloc tmp_alloc;
    ObScale in_scale = expr.args_[0]->datum_meta_.scale_;
    ObPrecision in_precision = expr.args_[0]->datum_meta_.precision_;
    number::ObNumber nmb;
    if (OB_FAIL(wide::to_number(child_res->get_decimal_int(), child_res->get_int_bytes(), in_scale,
                                tmp_alloc, nmb))) {
      LOG_WARN("cast decimalint to number failed", K(ret));
    } else {
      ret = common_number_json(nmb, expr.args_[0]->datum_meta_.type_, in_precision, in_scale, expr,
                               ctx, res_datum);
    }
  }
  return ret;
}

CAST_FUNC_NAME(decimalint, decimalint)
{
  EVAL_ARG()
  {
    int16_t in_scale = expr.args_[0]->datum_meta_.scale_;
    ObScale out_scale = expr.datum_meta_.scale_;
    ObPrecision out_prec = expr.datum_meta_.precision_;
    ObPrecision in_prec = expr.args_[0]->datum_meta_.precision_;
    if (ObDatumCast::need_scale_decimalint(in_scale, in_prec, out_scale, out_prec)) {
      ObDecimalIntBuilder res_val;
      if (OB_FAIL(ObDatumCast::common_scale_decimalint(child_res->decimal_int_, child_res->len_, in_scale,
                                          out_scale, out_prec, expr.extra_, res_val,
                                          ctx.exec_ctx_.get_user_logging_ctx()))) {
        LOG_WARN("scale decimal int failed", K(ret));
      } else {
        res_datum.set_decimal_int(res_val.get_decimal_int(), res_val.get_int_bytes());
      }
    } else {
      res_datum.set_datum(*child_res);
    }
  }
  return ret;
}

CAST_FUNC_NAME(decimalint, geometry)
{
  EVAL_ARG()
  {
    char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
    ObScale in_scale = expr.args_[0]->datum_meta_.scale_;
    int64_t len = 0;
    if (OB_FAIL(wide::to_string(child_res->get_decimal_int(), child_res->get_int_bytes(), in_scale,
                                buf, sizeof(buf), len))) {
      LOG_WARN("to_string failed", K(ret));
    } else if (OB_FAIL(common_string_geometry(buf, len, expr, ctx, res_datum))) {
      LOG_WARN("common string geometry failed", K(ret));
    }
  }
  return ret;
}

// ================
// roaringbitmap -> xxx
static int rb_copy_string(const ObExpr &expr,
                            ObString &src,
                            ObEvalCtx &ctx,
                            ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  char *out_ptr = NULL;
  int64_t len = src.length();
  if (expr.obj_meta_.is_lob_storage()) {
    ObTextStringDatumResult text_result(ObRoaringBitmapType, &expr, &ctx, &res_datum);
    if (OB_FAIL(text_result.init(len))) {
      LOG_WARN("Lob: init lob result failed");
    } else if (OB_FAIL(text_result.append(src.ptr(), src.length()))) {
      LOG_WARN("failed to append realdata", K(ret), K(src), K(text_result));
    } else {
      text_result.set_result();
    }
  } else {
    if (expr.res_buf_len_ < len) {
      if (OB_ISNULL(out_ptr = expr.get_str_res_mem(ctx, len))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      }
    } else {
      out_ptr = const_cast<char*>(res_datum.ptr_);
    }
    if (OB_SUCC(ret)) {
      MEMMOVE(out_ptr, src.ptr(), len);
      res_datum.set_string(out_ptr, len);
    }
  }
  return ret;
}

CAST_FUNC_NAME(roaringbitmap, string)
{
  EVAL_STRING_ARG()
  {
    ObString rb_bin = child_res->get_string();
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *child_res,
                expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), rb_bin))) {
      LOG_WARN("fail to get real data.", K(ret), K(rb_bin));
    } else if (OB_FAIL(rb_copy_string(expr, rb_bin, ctx, res_datum))){
      LOG_WARN("fail to copy string", K(ret));
    }
  }
  return ret;
}

CAST_FUNC_NAME(roaringbitmap, roaringbitmap)
{
  EVAL_STRING_ARG()
  {
    ObString rb_bin = child_res->get_string();
    res_datum.set_string(rb_bin);
  }
  return ret;
}

// exclude varchar/char type
int anytype_anytype_explicit(const sql::ObExpr &expr,
                             sql::ObEvalCtx &ctx,
                             sql::ObDatum &res_datum)
{
  EVAL_ARG()
  {
    int warning = OB_SUCCESS;
    ObObjType out_type = ObMaxType;
    ObAccuracy out_acc;
    // tmp_datum is for datum_accuracy_check().
    ObDatum tmp_datum = res_datum;
    if (OB_ISNULL(expr.inner_functions_) || 1 != expr.inner_func_cnt_ ||
               OB_ISNULL(expr.inner_functions_[0])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("inner_functinos is NULL or inner_func_cnt_ is not valid", K(ret),
          KP(expr.inner_functions_), K(expr.inner_func_cnt_));
    } else if (OB_FAIL(((ObExpr::EvalFunc)(expr.inner_functions_[0]))(expr, ctx,
                                                                      tmp_datum))) {
      LOG_WARN("inner cast failed", K(ret));
    } else if (OB_FAIL(get_accuracy_from_parse_node(expr, ctx, out_acc, out_type))) {
      LOG_WARN("get accuracy failed", K(ret));
    } else if (OB_FAIL(datum_accuracy_check(expr, expr.extra_, ctx, out_acc, expr.obj_meta_.has_lob_header(),
                                            tmp_datum, res_datum, warning))) {
      LOG_WARN("accuracy check failed", K(ret));
    }
  }
  return ret;
}

// padding %padding_cnt character, we also need to convert collation type here.
// eg: select cast('abc' as nchar(100)) from dual;
//     the space must be in utf16, because dst_type is nchar
int padding_char_for_cast(int64_t padding_cnt, const ObCollationType &padding_cs_type,
                          ObIAllocator &alloc, ObString &padding_res)
{
  int ret = OB_SUCCESS;
  padding_res.reset();
  const ObCharsetType &cs = ObCharset::charset_type_by_coll(padding_cs_type);
  char padding_char = (CHARSET_BINARY == cs) ? OB_PADDING_BINARY : OB_PADDING_CHAR;
  int64_t padding_str_size = sizeof(padding_char) * padding_cnt;
  char *padding_str_ptr = reinterpret_cast<char*>(alloc.alloc(padding_str_size));
  if (OB_ISNULL(padding_str_ptr)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory failed", K(ret));
  } else if (CHARSET_BINARY == cs) {
    MEMSET(padding_str_ptr, padding_char, padding_str_size);
    padding_res.assign_ptr(padding_str_ptr, padding_str_size);
  } else {
    MEMSET(padding_str_ptr, padding_char, padding_str_size);
    ObString padding_str(padding_str_size, padding_str_ptr);
    if (OB_FAIL(ObExprUtil::convert_string_collation(padding_str,
                                                     ObCharset::get_system_collation(),
                                                     padding_res,
                                                     padding_cs_type,
                                                     alloc))) {
      LOG_WARN("convert padding str collation faield", K(ret), K(padding_str),
                K(padding_cs_type));
    }
  }
  LOG_DEBUG("pad char done", K(ret), K(padding_cnt), K(padding_cs_type), K(padding_res));
  return ret;
}

int anytype_to_varchar_char_explicit(const sql::ObExpr &expr,
                             sql::ObEvalCtx &ctx,
                             sql::ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  GET_SESSION()
  {
    int warning = OB_SUCCESS; // useless
    ObAccuracy out_acc;
    ObObjType out_type = ObMaxType;
    ObDatum tmp_datum = res_datum;
    bool is_from_pl = !expr.is_called_in_sql_;
    if (OB_ISNULL(expr.inner_functions_) || 1 != expr.inner_func_cnt_ ||
               OB_ISNULL(expr.inner_functions_[0])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("inner_functions is NULL or inner_func_cnt_ is not valid", K(ret),
          KP(expr.inner_functions_), K(expr.inner_func_cnt_));
    } else if (OB_FAIL(((ObExpr::EvalFunc)(expr.inner_functions_[0]))(expr, ctx,
                                                                      tmp_datum))) {
      LOG_WARN("inner cast failed", K(ret));
    } else if (tmp_datum.is_null()) {
      res_datum.set_null();
    } else if (OB_FAIL(get_accuracy_from_parse_node(expr, ctx, out_acc, out_type))) {
      LOG_WARN("get accuracy failed", K(ret));
    } else if (OB_FAIL(datum_accuracy_check(expr, expr.extra_, ctx, out_acc, expr.obj_meta_.has_lob_header(),
                                            tmp_datum, res_datum, warning))) {
      if (ob_is_string_type(expr.datum_meta_.type_) && OB_ERR_DATA_TOO_LONG == ret) {
        ObDatumMeta src_meta;
        if (ObDatumCast::is_implicit_cast(*expr.args_[0])) {
          const ObExpr &grand_child = *(expr.args_[0]->args_[0]);
          if (OB_UNLIKELY(ObDatumCast::is_implicit_cast(grand_child) && !grand_child.obj_meta_.is_xml_sql_type())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("too many cast expr, max is 2", K(ret), K(expr));
          } else {
            src_meta = grand_child.datum_meta_;
          }
        } else {
          src_meta = expr.args_[0]->datum_meta_;
        }
        if (OB_LIKELY(OB_ERR_DATA_TOO_LONG == ret)) {
          if (lib::is_oracle_mode() &&
              (ob_is_character_type(src_meta.type_, src_meta.cs_type_) ||
              ob_is_raw(src_meta.type_))) {
            ret = OB_SUCCESS;
          } else if ((ob_is_clob(src_meta.type_, src_meta.cs_type_)
                      || ob_is_clob_locator(src_meta.type_, src_meta.cs_type_)
                      || expr.args_[0]->obj_meta_.is_xml_sql_type()
                      || (expr.args_[0]->type_ == T_FUN_SYS_CAST && expr.args_[0]->args_[0]->obj_meta_.is_xml_sql_type())) && lib::is_oracle_mode()) {
            if (ob_is_nchar(expr.datum_meta_.type_)
                || ob_is_char(expr.datum_meta_.type_, expr.datum_meta_.cs_type_)) {
              ret = OB_OPERATE_OVERFLOW;
            } else {
              ret = OB_SUCCESS;
            }
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
            if (OB_ENTRY_NOT_EXIST == ret) { // for compatibility with server before 1470
              ret = OB_SUCCESS;
              max_allowed_packet = OB_MAX_VARCHAR_LENGTH;
            } else {
              LOG_WARN("Failed to get max allow packet size", K(ret));
            }
          } else if (out_acc.get_length() > max_allowed_packet &&
                     out_acc.get_length() <= INT32_MAX) {
            res_datum.set_null();
            has_result = true;
            LOG_USER_WARN(OB_ERR_FUNC_RESULT_TOO_LARGE, "cast",
                          static_cast<int>(max_allowed_packet));
          } else if (out_acc.get_length() == 0) {
            res_datum.set_string(NULL, 0);
            has_result = true;
          }
        }

        if (OB_SUCC(ret) && !has_result) {
          ObString text(res_datum.len_, res_datum.ptr_);
          ObCollationType out_cs_type = expr.datum_meta_.cs_type_;
          bool oracle_char_byte_exceed = false;
          ObLengthSemantics ls = lib::is_oracle_mode() ?
                                 expr.datum_meta_.length_semantics_ : LS_CHAR;
          int32_t text_length = INT32_MAX;
          bool is_varbinary_or_binary =
            (out_type == ObVarcharType && CS_TYPE_BINARY == out_cs_type) ||
            (ObCharType == out_type && CS_TYPE_BINARY == out_cs_type);

          if (is_varbinary_or_binary) {
            text_length = text.length();
          } else {
            if (is_oracle_byte_length(lib::is_oracle_mode(), ls)) {
              text_length = text.length();
            } else {
              text_length = static_cast<int32_t>(ObCharset::strlen_char(
                    expr.datum_meta_.cs_type_, text.ptr(), text.length()));
            }
          }
          if (lib::is_oracle_mode() && ls == LS_CHAR) {
            if ((ObCharType == out_type || ObNCharType == out_type) &&
                text.length() > (is_from_pl ? OB_MAX_ORACLE_PL_CHAR_LENGTH_BYTE
                                            : OB_MAX_ORACLE_CHAR_LENGTH_BYTE)) {
              oracle_char_byte_exceed = true;
            } else if ((ObVarcharType == out_type || ObNVarchar2Type == out_type) &&
                       text.length() > OB_MAX_ORACLE_VARCHAR_LENGTH) {
              oracle_char_byte_exceed = true;
            }
          }

          if (out_acc.get_length() < text_length || oracle_char_byte_exceed) {
            int64_t acc_len = !oracle_char_byte_exceed? out_acc.get_length() :
              ((ObVarcharType == out_type || ObNVarchar2Type == out_type) ?
               OB_MAX_ORACLE_VARCHAR_LENGTH: (is_from_pl ? OB_MAX_ORACLE_PL_CHAR_LENGTH_BYTE
                                                         : OB_MAX_ORACLE_CHAR_LENGTH_BYTE));
            int64_t char_len = 0; // UNUSED
            int64_t size = (ls == LS_BYTE || oracle_char_byte_exceed ?
                ObCharset::max_bytes_charpos(out_cs_type, text.ptr(), text.length(),
                                             acc_len, char_len):
                ObCharset::charpos(out_cs_type, text.ptr(), text.length(), acc_len));
            if (0 == size) {
              if (lib::is_oracle_mode()) {
                res_datum.set_null();
              } else {
                res_datum.set_string(NULL, 0);
              }
            } else {
              res_datum.len_ = size;
            }
          } else if (out_acc.get_length() == text_length
                     || (ObCharType != out_type && ObNCharType != out_type)
                     || (lib::is_mysql_mode()
                         && ob_is_char(out_type, expr.datum_meta_.cs_type_))) {
            // do not padding
            LOG_DEBUG("no need to padding", K(ret), K(out_acc.get_length()),
                                            K(text_length), K(text));
          } else if (out_acc.get_length() > text_length) {
            int64_t padding_cnt = out_acc.get_length() - text_length;
            ObString padding_res;
            ObEvalCtx::TempAllocGuard alloc_guard(ctx);
            ObIAllocator &calc_alloc = alloc_guard.get_allocator();
            if (OB_FAIL(padding_char_for_cast(padding_cnt, out_cs_type, calc_alloc,
                                              padding_res))) {
              LOG_WARN("padding char failed", K(ret), K(padding_cnt), K(out_cs_type));
            } else {
              int64_t padding_size = padding_res.length() + text.length();
              char *res_ptr = expr.get_str_res_mem(ctx, padding_size);
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

// master上在accuracy check时没有对interval tc进行check，22x上有，还未
// patch过来，这里函数先留在这里，后面patch过来后会使用该函数
int interval_scale_check(const ObCastMode &cast_mode,
                     const ObAccuracy &accuracy,
                     const ObObjType type,
                     const ObDatum &in_datum,
                     ObDatum &res_datum,
                     int &warning)
{
  int ret = OB_SUCCESS;
  UNUSED(cast_mode);
  UNUSED(warning);
  ObScale expected_scale = accuracy.get_scale();
  if (ob_is_interval_ym(type)) {
    ObIntervalYMValue in_val(in_datum.get_interval_nmonth());
    int8_t expected_year_scale =
      ObIntervalScaleUtil::ob_scale_to_interval_ym_year_scale(
          static_cast<int8_t>(expected_scale));
    int8_t input_year_scale = in_val.calc_leading_scale();
    if (OB_UNLIKELY(expected_year_scale < input_year_scale)) {
      ret = OB_ERR_THE_LEADING_PRECISION_OF_THE_INTERVAL_IS_TOO_SMALL;
      LOG_WARN("interval obj scale check", K(ret), K(expected_year_scale),
                                            K(input_year_scale));
    } else {
      res_datum.set_interval_ym(in_datum.get_interval_nmonth());
    }
  } else if (ob_is_interval_ds(type)) {
    ObIntervalDSValue in_val = in_datum.get_interval_ds();
    int8_t expected_day_scale = ObIntervalScaleUtil::ob_scale_to_interval_ds_day_scale(
        static_cast<int8_t>(expected_scale));
    int8_t expected_fs_scale = ObIntervalScaleUtil::ob_scale_to_interval_ds_second_scale(
        static_cast<int8_t>(expected_scale));

    if (OB_FAIL(ObTimeConverter::round_interval_ds(expected_fs_scale, in_val))) {
      LOG_WARN("fail to round interval ds", K(ret), K(in_val));
    } else {
      int8_t input_day_scale = in_val.calc_leading_scale();
      if (OB_UNLIKELY(expected_day_scale < input_day_scale)) {
        ret = OB_ERR_THE_LEADING_PRECISION_OF_THE_INTERVAL_IS_TOO_SMALL;
        LOG_WARN("interval obj scale check", K(ret), K(expected_day_scale),
            K(input_day_scale));
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

int bit_length_check(const ObCastMode &cast_mode,
                     const ObAccuracy &accuracy,
                     const ObObjType type,
                     const ObDatum &in_datum,
                     ObDatum &res_datum,
                     int &warning)
{
  int ret = OB_SUCCESS;
  UNUSED(type);
  uint64_t value = in_datum.get_uint();
  int32_t bit_len = 0;
  int32_t dst_bit_len = accuracy.get_precision();
  if (OB_FAIL(get_bit_len(value, bit_len))) {
    LOG_WARN("fail to get_bit_length", K(ret), K(value), K(bit_len));
  } else if(OB_UNLIKELY(bit_len <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("bit length is negative", K(ret), K(value), K(bit_len));
  } else {
    if (OB_UNLIKELY(bit_len > dst_bit_len)) {
      ret = OB_ERR_DATA_TOO_LONG;
      LOG_WARN("bit type length is too long", K(ret), K(bit_len),
          K(dst_bit_len), K(value));
    } else {
      res_datum.set_bit(value);
    }
    if (OB_FAIL(ret) && CM_IS_WARN_ON_FAIL(cast_mode)) {
      warning = OB_DATA_OUT_OF_RANGE;
      ret = OB_SUCCESS;
      uint64_t max_value = (1ULL<<dst_bit_len) - 1;
      res_datum.set_bit(max_value);
    }
  }
  return ret;
}

int raw_length_check(const ObCastMode &cast_mode,
                     const ObAccuracy &accuracy,
                     const ObObjType type,
                     const ObDatum &in_datum,
                     ObDatum &res_datum,
                     int &warning)
{
  int ret = OB_SUCCESS;
  UNUSED(cast_mode);
  UNUSED(type);
  UNUSED(warning);
  const ObLength max_accuracy_len = accuracy.get_length();
  const int32_t str_len_byte = in_datum.len_;
  if (OB_UNLIKELY(str_len_byte > max_accuracy_len)) {
    ret = OB_OPERATE_OVERFLOW;
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

int time_scale_check(const ObCastMode &cast_mode,
                     const ObAccuracy &accuracy,
                     const ObObjType type,
                     const ObDatum &in_datum,
                     ObDatum &res_datum,
                     int &warning)
{
  int ret = OB_SUCCESS;
  UNUSED(type);
  UNUSED(warning);
  ObScale scale = accuracy.get_scale();
  int64_t value = in_datum.get_int();
  // First, judge whether it is inserted or updated(CM_IS_COLUMN_CONVERT), and then judge whether the current sqlmode is truncate(CM_IS_TIME_TRUNCATE_FRACTIONAL)
  bool need_truncate = CM_IS_COLUMN_CONVERT(cast_mode) ? CM_IS_TIME_TRUNCATE_FRACTIONAL(cast_mode) : false;
  if (OB_FAIL(time_usec_scale_check(cast_mode, accuracy, value))) {
    LOG_WARN("check usec scale fail.", K(ret), K(value));
  } else if (OB_LIKELY(0 <= scale && scale < MAX_SCALE_FOR_TEMPORAL)) {
    if(need_truncate) {
      value /= power_of_10[MAX_SCALE_FOR_TEMPORAL - scale];
      value *= power_of_10[MAX_SCALE_FOR_TEMPORAL - scale];
    } else {
      ObTimeConverter::round_datetime(scale, value);
    }
    res_datum.set_time(value);
  } else {
    res_datum.set_datum(in_datum);
  }
  return ret;
}

int otimestamp_scale_check(const ObCastMode &cast_mode,
                           const ObAccuracy &accuracy,
                           const ObObjType type,
                           const ObDatum &in_datum,
                           ObDatum &res_datum,
                           int &warning)
{
  int ret = OB_SUCCESS;
  UNUSED(cast_mode);
  UNUSED(warning);
  ObScale scale = accuracy.get_scale();
  ObOTimestampData in_val;
  if (OB_UNLIKELY(scale > MAX_SCALE_FOR_ORACLE_TEMPORAL)) {
    ret = OB_ERR_TOO_BIG_PRECISION;
    LOG_USER_ERROR(OB_ERR_TOO_BIG_PRECISION, scale, "CAST",
        static_cast<int64_t>(MAX_SCALE_FOR_ORACLE_TEMPORAL));
  } else if (OB_UNLIKELY(0 <= scale && scale < MAX_SCALE_FOR_ORACLE_TEMPORAL)) {
    ObOTimestampData in_val;
    if (OB_FAIL(common_construct_otimestamp(type, in_datum, in_val))) {
      LOG_WARN("common_construct_otimestamp failed", K(ret));
    } else {
      ObOTimestampData ot_data = ObTimeConverter::round_otimestamp(scale, in_val);
      if (ObTimeConverter::is_valid_otimestamp(ot_data.time_us_,
            static_cast<int32_t>(ot_data.time_ctx_.tail_nsec_))) {
        if (ObTimestampTZType == type) {
          res_datum.set_otimestamp_tz(ot_data);
        } else {
          res_datum.set_otimestamp_tiny(ot_data);
        }
      } else {
        OB_LOG(DEBUG, "invalid otimestamp, set it null ", K(ot_data), K(scale),
            "orig_date", in_val);
        res_datum.set_null();
      }
    }
  } else {
    res_datum.set_datum(in_datum);
  }
  return ret;
}

int datetime_scale_check(const ObCastMode &cast_mode,
                         const ObAccuracy &accuracy,
                         const ObObjType type,
                         const ObDatum &in_datum,
                         ObDatum &res_datum,
                         int &warning)
{
  int ret = OB_SUCCESS;
  UNUSED(type);
  UNUSED(warning);
  ObScale scale = accuracy.get_scale();
  // First, judge whether it is inserted or updated(CM_IS_COLUMN_CONVERT), and then judge whether the current sqlmode is truncate(CM_IS_TIME_TRUNCATE_FRACTIONAL)
  bool need_truncate = CM_IS_COLUMN_CONVERT(cast_mode) ? CM_IS_TIME_TRUNCATE_FRACTIONAL(cast_mode) : false;
  if (OB_UNLIKELY(scale > MAX_SCALE_FOR_TEMPORAL)) {
    ret = OB_ERR_TOO_BIG_PRECISION;
    LOG_USER_ERROR(OB_ERR_TOO_BIG_PRECISION, scale, "CAST",
        static_cast<int64_t>(MAX_SCALE_FOR_TEMPORAL));
  } else {
    int64_t value = in_datum.get_int();
    if (OB_FAIL(time_usec_scale_check(cast_mode, accuracy, value))) {
      LOG_WARN("check zero scale fail.", K(ret), K(value), K(scale));
    } else if (OB_UNLIKELY(0 <= scale && scale < MAX_SCALE_FOR_TEMPORAL)) {
      int64_t value = in_datum.get_int();
      if(need_truncate) {
        value /= power_of_10[MAX_SCALE_FOR_TEMPORAL - scale];
        value *= power_of_10[MAX_SCALE_FOR_TEMPORAL - scale];
      } else {
        ObTimeConverter::round_datetime(scale, value);
      }
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

int number_range_check_v2(const ObCastMode &cast_mode,
                          const ObAccuracy &accuracy,
                          const ObObjType type,
                          const ObDatum &in_datum,
                          ObDatum &res_datum,
                          int &warning)
{
  int ret = OB_SUCCESS;
  int &cast_ret = CM_IS_ERROR_ON_FAIL(cast_mode) ? ret : warning;
  ObPrecision precision = accuracy.get_precision();
  ObScale scale = accuracy.get_scale();
  const number::ObNumber *min_check_num = NULL;
  const number::ObNumber *max_check_num = NULL;
  const number::ObNumber *min_num_mysql = NULL;
  const number::ObNumber *max_num_mysql = NULL;
  const number::ObNumber in_val(in_datum.get_number());
  const static int64_t num_alloc_used_times = 2; // out val alloc will be used twice
  ObNumStackAllocator<num_alloc_used_times> out_val_alloc;
  number::ObNumber out_val;
  bool is_finish = false;

  if (ObUNumberType == type && in_val.is_negative()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unsiged type with negative value", K(ret), K(in_val));
  } else if (ObNumberFloatType == type) {
    if (OB_MIN_NUMBER_FLOAT_PRECISION <= precision
        && precision <= OB_MAX_NUMBER_FLOAT_PRECISION) {
      const int64_t number_precision = static_cast<int64_t>(floor(precision *
            OB_PRECISION_BINARY_TO_DECIMAL_FACTOR));
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
    if (OB_MAX_NUMBER_PRECISION >= precision
        && precision >= OB_MIN_NUMBER_PRECISION
        && number::ObNumber::MAX_SCALE >= scale
        && scale >= number::ObNumber::MIN_SCALE) {
      min_check_num = &(ObNumberConstValue::ORACLE_CHECK_MIN[precision][scale +
          ObNumberConstValue::MAX_ORACLE_SCALE_DELTA]);
      max_check_num = &(ObNumberConstValue::ORACLE_CHECK_MAX[precision][scale +
          ObNumberConstValue::MAX_ORACLE_SCALE_DELTA]);
    } else if (ORA_NUMBER_SCALE_UNKNOWN_YET == scale
        && PRECISION_UNKNOWN_YET == precision) {
      res_datum.set_number(in_val);
      is_finish = true;
    } else if (PRECISION_UNKNOWN_YET == precision
        && number::ObNumber::MAX_SCALE >= scale
        && scale >= number::ObNumber::MIN_SCALE) {
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
      } else if (!in_val.is_equal(out_val)) {
        if (CM_IS_ERROR_ON_SCALE_OVER(cast_mode)) {
          ret = OB_OPERATE_OVERFLOW;
          LOG_WARN("input value is out of range.", K(ret), K(scale), K(in_val));
        } else if (lib::is_mysql_mode()) {
          // MySQL emits warnings for decimal column truncation, regardless of sql_mode settings.
          warning = OB_ERR_DATA_TOO_LONG;
        }
      }
      if (OB_SUCC(ret)) {
        res_datum.set_number(out_val);
        is_finish = true;
      }
    }
  }
  if (OB_SUCC(ret) && !is_finish) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected situation, res_datum is not set", K(ret));
  }
  LOG_DEBUG("number_range_check_v2 done", K(ret), K(is_finish), K(accuracy), K(in_val), K(out_val),
            KPC(min_check_num), KPC(max_check_num));
  return ret;
}

template <typename IN_TYPE>
static int float_range_check(const ObCastMode &cast_mode,
                             const ObAccuracy &accuracy,
                             const ObObjType type,
                             const ObDatum &in_datum,
                             ObDatum &res_datum,
                             int &warning)
{
  int ret = OB_SUCCESS;
  ObObjTypeClass type_class = ob_obj_type_class(type);
  if (OB_UNLIKELY(ObFloatTC != type_class && ObDoubleTC != type_class)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("obj type is invalid, must be float/double tc", K(ret), K(type), K(type_class));
  } else {
    IN_TYPE in_val = *(reinterpret_cast<const IN_TYPE*>(in_datum.ptr_));
    IN_TYPE out_val = in_val;
    if (ob_is_unsigned_type(type) && in_val < 0.0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unsiged type with negative value", K(ret), K(type), K(in_val));
    } else if (lib::is_oracle_mode() && 0.0 == in_val) {
      if (ObFloatTC == type_class) {
        res_datum.set_float(0.0);
      } else {
        res_datum.set_double(0.0);
      }
    } else if (lib::is_oracle_mode() && isnan(in_val)) {
      // overwrite -NAN to NAN, OB only store NAN
      if (ObFloatTC == type_class) {
        res_datum.set_float(NAN);
      } else {
        res_datum.set_double(NAN);
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

int string_length_check(const ObExpr &expr,
                        const ObCastMode &cast_mode,
                        const ObAccuracy &accuracy,
                        const ObObjType type,
                        const ObCollationType cs_type,
                        ObEvalCtx &ctx,
                        const ObDatum &in_datum,
                        ObDatum &res_datum,
                        int &warning)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  UNUSED(ctx);
  const ObLength max_accuracy_len = accuracy.get_length();
  const int32_t str_len_byte = in_datum.len_;
  bool is_oracle = lib::is_oracle_mode();
  bool is_from_pl = !expr.is_called_in_sql_;
  ObObjMeta meta;
  meta.set_type_simple(type);
  meta.set_collation_type(cs_type);
  // remember not to change in_datum
  res_datum.set_datum(in_datum);
  // 处理异常情况，但str_len_byte大于max_len_char不一定有问题，还需要具体判断
  if (max_accuracy_len <= 0 || str_len_byte > max_accuracy_len) {
    int &cast_ret = (CM_IS_ERROR_ON_FAIL(cast_mode) && !is_oracle)
                    ? ret
                    : warning;
    const char *str = in_datum.ptr_;
    int32_t str_len_char = -1;
    // 在parse时,如果长度大于int32_t最大值, length就会设置为-1
    if (max_accuracy_len == -1) {
    } else if (OB_UNLIKELY(max_accuracy_len <= 0)) {
      res_datum.set_string(NULL, 0);
      if (OB_UNLIKELY(0 == max_accuracy_len && str_len_byte > 0)) {
        cast_ret = OB_ERR_DATA_TOO_LONG;
        str_len_char = meta.is_lob() ?
                        str_len_byte :
                        static_cast<int32_t>(ObCharset::strlen_char(
                                                        cs_type, str, str_len_byte));
        OB_LOG(WARN, "char type length is too long", K(max_accuracy_len),
                      K(str_len_char));
      }
    } else {
      int32_t trunc_len_byte = -1;
      int32_t trunc_len_char = -1;
      if (meta.is_varbinary() || meta.is_binary() ||
          meta.is_blob()) {
        str_len_char = meta.is_blob() ?
          str_len_byte : static_cast<int32_t>(ObCharset::strlen_char(cs_type,
                                                                     str, str_len_byte));
        if (OB_UNLIKELY(str_len_char > max_accuracy_len)) {
          cast_ret = OB_ERR_DATA_TOO_LONG;
          LOG_WARN("binary type length is too long", K(max_accuracy_len),
                    K(str_len_char));
        }
      } else if (is_oracle_byte_length(is_oracle, accuracy.get_length_semantics())) {
        const ObLength max_len_byte = accuracy.get_length();
        if (OB_UNLIKELY(str_len_byte > max_len_byte)) {
          cast_ret = OB_ERR_DATA_TOO_LONG;
          LOG_WARN("char type length is too long", K(str_len_byte), K(max_len_byte));
        }
      } else if (is_oracle && (meta.is_char() || meta.is_nchar())) {
        const int32_t str_len_char =
          static_cast<int32_t>(ObCharset::strlen_char(cs_type, str, str_len_byte));
        if (OB_UNLIKELY(str_len_byte > (is_from_pl ? OB_MAX_ORACLE_PL_CHAR_LENGTH_BYTE
                                                   : OB_MAX_ORACLE_CHAR_LENGTH_BYTE))) {
          cast_ret = OB_ERR_DATA_TOO_LONG;
          LOG_WARN("char byte length is too long", K(str_len_byte),
                    K((is_from_pl ? OB_MAX_ORACLE_PL_CHAR_LENGTH_BYTE
                                  : OB_MAX_ORACLE_CHAR_LENGTH_BYTE)));
        } else if (OB_UNLIKELY(str_len_char > max_accuracy_len)) {
          cast_ret = OB_ERR_DATA_TOO_LONG;
          LOG_WARN("char char length is too long", K(str_len_char),
                    K(max_accuracy_len));
        }
      } else {//mysql, oracle varchar(char)
        // trunc_len_char > max_accuracy_len means an error or warning, without tail ' '
        // str_len_char > max_accuracy_len means only warning, even in strict mode.
        // lengthsp()  - returns the length of the given string without trailing spaces.
        // 所以strlen_byte_no_sp返回的结果是小于等于str的长度
        trunc_len_byte =
          static_cast<int32_t>(ObCharset::strlen_byte_no_sp(cs_type, str, str_len_byte));
        trunc_len_char = meta.is_lob() ?
          trunc_len_byte :
          static_cast<int32_t>(ObCharset::strlen_char(cs_type, str, trunc_len_byte));

        if (is_oracle && OB_UNLIKELY(str_len_byte > OB_MAX_ORACLE_VARCHAR_LENGTH)) {
          cast_ret = OB_ERR_DATA_TOO_LONG;
          LOG_WARN("varchar2 byte length is too long", K(str_len_byte),
                    K(OB_MAX_ORACLE_VARCHAR_LENGTH));
        } else if (OB_UNLIKELY(trunc_len_char > max_accuracy_len)) {
          cast_ret = OB_ERR_DATA_TOO_LONG;
          LOG_WARN("char type length is too long", K(max_accuracy_len),
                    K(trunc_len_char));
        } else {
          str_len_char = meta.is_lob() ?
            str_len_byte :
            static_cast<int32_t>(ObCharset::strlen_char(cs_type, str, str_len_byte));
          if (OB_UNLIKELY(str_len_char > max_accuracy_len)) {
            warning = OB_ERR_DATA_TOO_LONG;
            LOG_WARN("char type length is too long", K(max_accuracy_len),
                      K(str_len_char));
          }
        }
      }
      if (OB_SUCCESS != cast_ret) {
        LOG_WARN("string accuracy check failed", K(cast_ret), K(ret), K(warning), K(is_oracle), K(meta));
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
            trunc_len_byte = static_cast<int32_t>(ObCharset::max_bytes_charpos(cs_type,
                                                  str, str_len_byte,
                                                  max_accuracy_len, char_len));
          } else {
            trunc_len_byte = static_cast<int32_t>(ObCharset::charpos(cs_type, str,
                                                    str_len_byte, max_accuracy_len));
          }
          if (is_oracle) {
          // 在oracle模式下不清理末尾的空格字符,原因如下:
          // #bug18529663:例如select cast(' a' as char) from dual;
          // 执行至此时,trunc_len_byte = 1,意思是截取到' a'的第一个字符' '
          // 如果不加判断将会直接执行strlen_byte_no_sp来清理末尾的空格字符,
          // 执行完毕后由于空格被清理掉,导致trunc_len_byte=0
          // trunc_len_byte = 0会导致最终的obchar类型的输出长度为0,
          // 在oracle模式的比较中将会判其为空,不符号预期.
          } else if (meta.is_fixed_len_char_type() &&
                     !meta.is_binary()) {
            trunc_len_byte = static_cast<int32_t>(ObCharset::strlen_byte_no_sp(cs_type,
                                                                  str, trunc_len_byte));
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
    // 正常分支
    // do nothing
  }

  return ret;
}

int text_length_check(const ObExpr &expr,
                      const ObCastMode &cast_mode,
                      const ObAccuracy &accuracy,
                      const ObObjType type,
                      const ObCollationType cs_type,
                      ObEvalCtx &ctx,
                      bool has_lob_header,
                      const ObDatum &in_datum,
                      ObDatum &res_datum,
                      int &warning)
{
  int ret = OB_SUCCESS;
  const ObLength max_accuracy_len = accuracy.get_length();
  ObDatum tmp_in;
  if (expr.obj_meta_.is_lob_storage() && has_lob_header) {
    ObString inrow_data;
    ObLobLocatorV2 lob(in_datum.get_string(), has_lob_header);
    if (!lob.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("lob locator is invalid", K(ret));
    } else if (!lob.is_full_temp_lob() || !lob.is_inrow()) {
      // do nothing, persist/delta/outrow lob loator not support length check
      res_datum.set_datum(in_datum);
    } else if (OB_FAIL(lob.get_inrow_data(inrow_data))) {
      LOG_WARN("fail to get inrow data", K(ret));
    } else if (FALSE_IT(tmp_in.set_string(inrow_data))) {
    } else if (OB_FAIL(string_length_check(expr, cast_mode, accuracy, type,
                                           cs_type, ctx, tmp_in, res_datum, warning))) {
      LOG_WARN("fail to do string length check", K(ret));
    } else {
      int32_t lob_handle_len = lob.size_ - inrow_data.length();
      int32_t new_inrow_byte_len = lob_handle_len + res_datum.len_;
      res_datum.set_datum(in_datum);
      res_datum.len_ = new_inrow_byte_len;
    }
  } else if (OB_FAIL(string_length_check(expr, cast_mode, accuracy, type,
                                         cs_type, ctx, in_datum, res_datum, warning))) {
    LOG_WARN("fail to do string length check", K(ret));
  }
  return ret;
}

int rowid_length_check(const ObExpr &expr,
                       const ObCastMode &cast_mode,
                       const ObAccuracy &accuracy,
                       const ObObjType type,
                       const ObCollationType cs_type,
                       ObEvalCtx &ctx,
                       const ObDatum &in_datum,
                       ObDatum &res_datum,
                       int &warning)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  UNUSED(cast_mode);
  UNUSED(type);
  UNUSED(cs_type);
  UNUSED(ctx);
  UNUSED(warning);
  ObURowIDData urowid_data(in_datum.len_, reinterpret_cast<const uint8_t *>(in_datum.ptr_));
  if (OB_UNLIKELY(lib::is_mysql_mode())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("rowid is not supported in mysql mode", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "rowid in mysql mode");
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

int datum_accuracy_check(const ObExpr &expr,
                         const uint64_t cast_mode,
                         ObEvalCtx &ctx,
                         bool has_lob_header,
                         const ObDatum &in_datum,
                         ObDatum &res_datum,
                         int &warning)
{
  ObAccuracy accuracy;
  accuracy.set_length(expr.max_length_);
  accuracy.set_scale(expr.datum_meta_.scale_);
  const ObObjTypeClass &dst_tc = ob_obj_type_class(expr.datum_meta_.type_);
  if (ObStringTC == dst_tc || ObTextTC == dst_tc || ObJsonTC == dst_tc) {
    accuracy.set_length_semantics(expr.datum_meta_.length_semantics_);
  } else {
    accuracy.set_precision(expr.datum_meta_.precision_);
  }
  return datum_accuracy_check(expr, cast_mode, ctx, accuracy, has_lob_header, in_datum, res_datum, warning);
}

int datum_accuracy_check(const ObExpr &expr,
                         const uint64_t cast_mode,
                         ObEvalCtx &ctx,
                         const ObAccuracy &accuracy,
                         bool has_lob_header,
                         const ObDatum &in_datum,
                         ObDatum &res_datum,
                         int &warning)
{
  int ret = OB_SUCCESS;
  if (!in_datum.is_null()) {
    ObObjType type = expr.datum_meta_.type_;
    ObCollationType cs_type = expr.datum_meta_.cs_type_;
    switch (ob_obj_type_class(type)) {
      case ObFloatTC: {
        ret = float_range_check<float>(cast_mode, accuracy, type, in_datum,
                                       res_datum, warning);
        break;
      }
      case ObDoubleTC: {
        ret = float_range_check<double>(cast_mode, accuracy, type, in_datum,
                                        res_datum, warning);
        break;
      }
      case ObNumberTC: {
        ret = number_range_check_v2(cast_mode, accuracy, type, in_datum,
                                    res_datum, warning);
        break;
      }
      case ObDateTimeTC: {
        ret = datetime_scale_check(cast_mode, accuracy, type, in_datum,
                                   res_datum, warning);
        break;
      }
      case ObOTimestampTC: {
        ret = otimestamp_scale_check(cast_mode, accuracy, type, in_datum,
                                     res_datum, warning);
        break;
      }
      case ObTimeTC: {
        ret = time_scale_check(cast_mode, accuracy, type, in_datum, res_datum, warning);
        break;
      }
      case ObStringTC: {
        ret = string_length_check(expr, cast_mode, accuracy, type,
                                  cs_type, ctx, in_datum, res_datum, warning);
        break;
      }
      case ObRawTC: {
        ret = raw_length_check(cast_mode, accuracy, type, in_datum, res_datum, warning);
        break;
      }
      case ObTextTC: {
        ret = text_length_check(expr, cast_mode, accuracy, type,
                                cs_type, ctx, has_lob_header, in_datum, res_datum, warning);
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
        ret = rowid_length_check(expr, cast_mode, accuracy, type,
                                 cs_type, ctx, in_datum, res_datum, warning);
        break;
      }
      case ObDecimalIntTC: {
        ObDecimalIntBuilder res_val;
        ret = check_decimalint_accuracy(cast_mode, in_datum.get_decimal_int(),
                                        in_datum.get_int_bytes(), accuracy.get_precision(),
                                        accuracy.get_scale(), res_val, warning);
        if (OB_FAIL(ret)) {
          LOG_WARN("decimal int accuracy failed", K(ret), K(accuracy), K(in_datum));
        } else {
          res_datum.set_decimal_int(res_val.get_decimal_int(), res_val.get_int_bytes());
        }
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

ObExpr::EvalFunc OB_DATUM_CAST_ORACLE_IMPLICIT[ObMaxTC][ObMaxTC] =
{
  {
    /*null -> XXX*/
    cast_eval_arg,/*null*/
    cast_eval_arg,/*int*/
    cast_eval_arg,/*uint*/
    cast_eval_arg,/*float*/
    cast_eval_arg,/*double*/
    cast_eval_arg,/*number*/
    cast_eval_arg,/*datetime*/
    cast_not_expected,/*date*/
    cast_not_expected,/*time*/
    cast_not_expected,/*year*/
    cast_eval_arg,/*string*/
    cast_not_expected,/*extend*/
    cast_not_expected,/*unknown*/
    cast_eval_arg,/*text*/
    cast_not_expected,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumsetInner*/
    cast_eval_arg,/*otimestamp*/
    cast_eval_arg,/*raw*/
    cast_eval_arg,/*interval*/
    cast_eval_arg,/*rowid*/
    cast_eval_arg,/*lob*/
    cast_eval_arg,/*json*/
    cast_not_support,/*geometry*/
    cast_to_udt_not_support,/*udt*/
    cast_eval_arg,/*decimalint*/
    cast_to_udt_not_support,/*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
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
    cast_inconsistent_types,/*geometry*/
    cast_to_udt_not_support,/*udt*/
    int_decimalint,/*decimalint*/
    cast_to_udt_not_support,/*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
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
    cast_inconsistent_types,/*geometry*/
    cast_to_udt_not_support,/*udt*/
    uint_decimalint,/*decimalint*/
    cast_to_udt_not_support,/*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
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
    cast_inconsistent_types,/*geometry*/
    cast_to_udt_not_support,/*udt*/
    float_decimalint,/*decimalint*/
    cast_to_udt_not_support,/*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
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
    cast_inconsistent_types,/*geometry*/
    cast_to_udt_not_support,/*udt*/
    double_decimalint,/*decimalint*/
    cast_to_udt_not_support,/*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
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
    number_json,/*json*/
    cast_inconsistent_types,/*geometry*/
    cast_to_udt_not_support,/*udt*/
    number_decimalint,/*decimalint*/
    cast_to_udt_not_support,/*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
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
    date_json,/*json*/
    cast_inconsistent_types,/*geometry*/
    cast_to_udt_not_support,/*udt*/
    cast_inconsistent_types,/*decimalint*/
    cast_to_udt_not_support,/*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
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
    cast_not_expected,/*geometry*/
    cast_to_udt_not_support,/*udt*/
    cast_not_expected,/*decimalint*/
    cast_to_udt_not_support,/*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
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
    time_json,/*json*/
    cast_not_expected,/*geometry*/
    cast_to_udt_not_support,/*udt*/
    cast_not_expected,/*decimalint*/
    cast_to_udt_not_support,/*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
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
    cast_not_expected,/*geometry*/
    cast_to_udt_not_support,/*udt*/
    cast_not_expected,/*decimalint*/
    cast_to_udt_not_support,/*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
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
    cast_inconsistent_types,/*geometry*/
    string_udt,/*udt*/
    string_decimalint,/*decimalint*/
    cast_to_udt_not_support,/*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
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
    cast_eval_arg,/*extend*/
    cast_not_expected,/*unknown*/
    cast_inconsistent_types,/*text*/
    cast_not_expected,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    cast_not_expected,/*otimestamp*/
    cast_not_expected,/*raw*/
    cast_not_expected,/*interval*/
    cast_not_expected,/*rowid*/
    cast_not_expected,/*lob*/
    cast_inconsistent_types_json,/*json*/
    pl_extend_geometry,/*geometry*/
    pl_extend_sql_udt,/*udt*/
    cast_not_expected,/*decimalint*/
    pl_extend_sql_udt,/*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
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
    cast_inconsistent_types,/*geometry*/
    cast_to_udt_not_support,/*udt*/
    cast_not_expected,/*decimalint*/
    cast_to_udt_not_support,/*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
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
    cast_inconsistent_types,/*otimestamp*/
    text_raw,/*raw*/
    text_interval,/*interval*/
    text_rowid,/*rowid*/
    text_lob,/*lob*/
    string_json,/*json*/
    cast_inconsistent_types,/*geometry*/
    cast_to_udt_not_support,/*udt*/
    cast_to_udt_not_support,/*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
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
    cast_inconsistent_types,/*geometry*/
    cast_to_udt_not_support,/*udt*/
    cast_not_expected,/*decimalint*/
    cast_to_udt_not_support,/*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
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
    cast_inconsistent_types,/*geometry*/
    cast_to_udt_not_support,/*udt*/
    cast_not_expected, /*decimalint*/
    cast_to_udt_not_support,/*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
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
    cast_inconsistent_types,/*geometry*/
    cast_to_udt_not_support,/*udt*/
    cast_not_expected,/*decimalint*/
    cast_to_udt_not_support,/*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
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
    cast_inconsistent_types,/*geometry*/
    cast_to_udt_not_support,/*udt*/
    cast_inconsistent_types,/*decimalint*/
    cast_to_udt_not_support,/*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
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
    raw_longtext,/*text*/
    cast_not_expected,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    cast_inconsistent_types,/*otimestamp*/
    raw_raw,/*raw*/
    cast_inconsistent_types,/*interval*/
    cast_inconsistent_types,/*rowid*/
    raw_lob,/*lob*/
    raw_json,/*json*/
    cast_inconsistent_types,/*geometry*/
    cast_to_udt_not_support,/*udt*/
    cast_inconsistent_types,/*decimalint*/
    cast_to_udt_not_support,/*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
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
    cast_inconsistent_types,/*geometry*/
    cast_to_udt_not_support,/*udt*/
    cast_inconsistent_types,/*decimalint*/
    cast_to_udt_not_support,/*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
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
    cast_inconsistent_types,/*geometry*/
    cast_to_udt_not_support,/*udt*/
    cast_inconsistent_types,/*decimalint*/
    cast_to_udt_not_support,/*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
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
    cast_inconsistent_types,/*otimestamp*/
    lob_raw,/*raw*/
    lob_interval,/*interval*/
    lob_rowid,/*rowid*/
    lob_lob,/*lob*/
    lob_json,/*json*/
    lob_geometry,/*geometry*/
    cast_to_udt_not_support,/*udt*/
    lob_decimalint,/*decimalint*/
    cast_to_udt_not_support,/*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
  },
  {
    /*json -> XXX*/
    cast_not_support,/*null*/
    cast_not_support,/*int*/
    cast_not_support,/*uint*/
    cast_inconsistent_types,/*float*/
    cast_inconsistent_types,/*double*/
    cast_inconsistent_types,/*number*/
    cast_inconsistent_types,/*datetime*/
    cast_inconsistent_types,/*date*/
    cast_inconsistent_types,/*time*/
    cast_not_support,/*year*/
    json_string,/*string*/
    cast_not_support,/*extend*/
    cast_not_support,/*unknown*/
    json_text,/*text*/
    cast_not_support,/*bit*/
    cast_not_support,/*enumset*/
    cast_not_support,/*enumset_inner*/
    cast_inconsistent_types,/*otimestamp*/
    json_raw,/*raw*/
    cast_inconsistent_types,/*interval*/
    cast_not_support,/*rowid*/
    json_lob,/*lob*/
    json_json,/*json*/
    cast_inconsistent_types,/*geometry*/
    cast_to_udt_not_support,/*udt*/
    cast_not_support,/*decimalint*/
    cast_to_udt_not_support,/*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
  },
  {
    /*geometry -> XXX*/
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
    geometry_pl_extend,/*extend*/
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
    geometry_geometry,/*geometry*/
    cast_not_support,/*udt*/
    cast_not_support,/*decimalint*/
    cast_not_support,/*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
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
    udt_udt,/*udt*/
    cast_not_expected,/*decimalint*/
    cast_udt_to_other_not_support,/*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
  },
  {
    /*decimalint -> XXX*/
    cast_not_expected,/*null*/
    decimalint_int,/*int*/
    decimalint_uint,/*uint*/
    decimalint_float,/*float*/
    decimalint_double,/*double*/
    decimalint_number,/*number*/
    cast_inconsistent_types,/*datetime*/
    cast_not_expected,/*date*/
    cast_not_expected,/*time*/
    cast_not_expected,/*year*/
    decimalint_string,/*string*/
    cast_not_expected,/*extend*/
    cast_not_expected,/*unknown*/
    decimalint_text,/*text*/
    cast_not_expected,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    cast_inconsistent_types,/*otimestamp*/
    cast_inconsistent_types,/*raw*/
    cast_inconsistent_types,/*interval*/
    cast_inconsistent_types,/*rowid*/
    decimalint_lob,/*lob*/
    decimalint_json,/*json*/
    cast_inconsistent_types,/*decimalint*/
    cast_to_udt_not_support, /*udt*/
    decimalint_decimalint,/*decimalint*/
    cast_not_expected, /*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
  },
  {
    /*collection -> XXX*/
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
    cast_udt_to_other_not_support,/*string*/
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
    cast_udt_to_other_not_support,/*decimal int*/
    cast_udt_to_other_not_support,/*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
  },
  {
    /*mysql date*/
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
    cast_not_expected, /*udt*/
    cast_not_expected,/*decimalint*/
    cast_not_expected,/*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
  },
  {
    /*mysql datetime*/
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
    cast_not_expected, /*udt*/
    cast_not_expected,/*decimalint*/
    cast_not_expected,/*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
  },
  {
    /*roaringbitmap -> XXX*/
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
    cast_not_expected,/*decimalint*/
    cast_not_expected,/*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
  },
};

// 目前代码里面没有使用该矩阵，Oracle模式都是使用impilicit矩阵
// 但是新框架仍然保留，后期如果有需要可以通过cast_mode选择使用该矩阵
ObExpr::EvalFunc OB_DATUM_CAST_ORACLE_EXPLICIT[ObMaxTC][ObMaxTC] =
{
  {
    /*null -> XXX*/
    cast_eval_arg,/*null*/
    cast_eval_arg,/*int*/
    cast_eval_arg,/*uint*/
    cast_eval_arg,/*float*/
    cast_eval_arg,/*double*/
    cast_eval_arg,/*number*/
    cast_eval_arg,/*datetime*/
    cast_eval_arg,/*date*/
    cast_eval_arg,/*time*/
    cast_eval_arg,/*year*/
    cast_eval_arg,/*string*/
    cast_eval_arg,/*extend*/
    cast_eval_arg,/*unknown*/
    cast_eval_arg,/*text*/
    cast_eval_arg,/*bit*/
    cast_eval_arg,/*enumset*/
    cast_eval_arg,/*enumsetInner*/
    cast_eval_arg,/*otimestamp*/
    cast_eval_arg,/*raw*/
    cast_eval_arg,/*interval*/
    cast_eval_arg,/*rowid*/
    cast_eval_arg,/*lob*/
    cast_eval_arg,/*json*/
    cast_eval_arg,/*geometry*/
    cast_to_udt_not_support,/*udt*/
    cast_eval_arg,/*decimalint*/
    cast_to_udt_not_support,/*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_eval_arg,/*roaringbitmap*/
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
    cast_eval_arg,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    cast_not_support,/*otimestamp*/
    cast_not_support,/*raw*/
    cast_not_support,/*interval*/
    cast_not_support,/*rowid*/
    cast_inconsistent_types,/*lob*/
    cast_inconsistent_types,/*json*/
    cast_inconsistent_types,/*geometry*/
    cast_to_udt_not_support,/*udt*/
    int_decimalint,/*decimalint*/
    cast_to_udt_not_support,/*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
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
    cast_eval_arg,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    cast_not_support,/*otimestamp*/
    cast_not_support,/*raw*/
    cast_not_support,/*interval*/
    cast_not_support,/*rowid*/
    cast_inconsistent_types,/*lob*/
    cast_inconsistent_types,/*json*/
    cast_inconsistent_types,/*geometry*/
    cast_to_udt_not_support,/*udt*/
    uint_decimalint,/*decimalint*/
    cast_to_udt_not_support,/*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
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
    cast_inconsistent_types,/*json*/
    cast_inconsistent_types,/*geometry*/
    cast_to_udt_not_support,/*udt*/
    float_decimalint,/*decimalint*/
    cast_to_udt_not_support,/*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
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
    cast_inconsistent_types,/*json*/
    cast_inconsistent_types,/*geometry*/
    cast_to_udt_not_support,/*udt*/
    double_decimalint,/*decimalint*/
    cast_to_udt_not_support,/*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
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
    cast_inconsistent_types,/*json*/
    cast_inconsistent_types,/*geometry*/
    cast_to_udt_not_support,/*udt*/
    number_decimalint,/*decimalint*/
    cast_to_udt_not_support,/*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
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
    cast_inconsistent_types,/*json*/
    cast_inconsistent_types,/*geometry*/
    cast_to_udt_not_support,/*udt*/
    cast_not_support,/*decimalint*/
    cast_to_udt_not_support,/*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
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
    cast_not_support,/*rowid*/
    cast_not_expected,/*lob*/
    cast_not_expected,/*json*/
    cast_not_expected,/*geometry*/
    cast_to_udt_not_support,/*udt*/
    cast_not_expected,/*decimalint*/
    cast_to_udt_not_support,/*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
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
    cast_not_support,/*rowid*/
    cast_not_expected,/*lob*/
    cast_not_expected,/*json*/
    cast_not_expected,/*geometry*/
    cast_to_udt_not_support,/*udt*/
    cast_not_expected,/*decimalint*/
    cast_to_udt_not_support,/*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
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
    cast_not_support,/*rowid*/
    cast_not_expected,/*lob*/
    cast_not_expected,/*json*/
    cast_not_expected,/*geometry*/
    cast_to_udt_not_support,/*udt*/
    cast_not_expected,/*decimalint*/
    cast_to_udt_not_support,/*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
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
    string_rowid, /*rowid*/
    cast_inconsistent_types,/*lob*/
    cast_inconsistent_types,/*json*/
    cast_inconsistent_types,/*geometry*/
    string_udt,/*udt*/
    string_decimalint,/*decimalint*/
    cast_to_udt_not_support,/*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
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
    cast_eval_arg,/*extend*/
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
    pl_extend_geometry,/*geometry*/
    pl_extend_sql_udt,/*udt*/
    cast_not_support,/*decimalint*/
    pl_extend_sql_udt,/*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
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
    cast_eval_arg,/*unknown*/
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
    cast_not_support,/*geometry*/
    cast_to_udt_not_support,/*udt*/
    unknown_other,/*decimalint*/
    cast_to_udt_not_support,/*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
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
    string_json,/*json*/
    cast_inconsistent_types,/*geometry*/
    cast_to_udt_not_support,/*udt*/
    text_decimalint,/*decimalint*/
    cast_to_udt_not_support,/*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
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
    cast_not_expected,/*geometry*/
    cast_to_udt_not_support,/*udt*/
    cast_not_expected,/*decimalint*/
    cast_to_udt_not_support,/*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
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
    cast_not_expected,/*geometry*/
    cast_to_udt_not_support,/*udt*/
    cast_not_expected,/*decimalint*/
    cast_to_udt_not_support,/*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
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
    cast_not_expected,/*geometry*/
    cast_to_udt_not_support,/*udt*/
    cast_not_expected,/*decimalint*/
    cast_to_udt_not_support,/*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
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
    cast_not_support,/*json*/
    cast_inconsistent_types,/*geometry*/
    cast_to_udt_not_support,/*udt*/
    cast_not_support,/*decimalint*/
    cast_to_udt_not_support,/*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
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
    raw_longtext,/*text*/
    cast_not_support,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    cast_not_support,/*otimestamp*/
    raw_raw,/*raw*/
    cast_not_support,/*interval*/
    cast_not_support,/*rowid*/
    cast_inconsistent_types,/*lob*/
    cast_not_support,/*json*/
    cast_inconsistent_types,/*geometry*/
    cast_to_udt_not_support,/*udt*/
    cast_not_support,/*decimalint*/
    cast_to_udt_not_support,/*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
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
    cast_not_support,/*interval*/
    cast_not_support,/*rowid*/
    cast_inconsistent_types,/*lob*/
    cast_not_support,/*json*/
    cast_inconsistent_types,/*geometry*/
    cast_to_udt_not_support,/*udt*/
    cast_not_expected,/*decimalint*/
    cast_to_udt_not_support,/*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
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
    cast_not_support,/*json*/
    cast_inconsistent_types,/*geometry*/
    cast_to_udt_not_support,/*udt*/
    cast_not_support,/*decimalint*/
    cast_to_udt_not_support,/*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
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
    string_json,/*json*/
    lob_geometry,/*geometry*/
    cast_to_udt_not_support,/*udt*/
    lob_decimalint,/*decimalint*/
    cast_to_udt_not_support,/*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
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
    cast_not_support,/*raw*/
    cast_not_support,/*interval*/
    cast_not_support,/*rowid*/
    json_lob,/*lob*/
    json_json,/*json*/
    cast_inconsistent_types,/*geometry*/
    cast_to_udt_not_support,/*udt*/
    cast_not_support,/*decimalint*/
    cast_to_udt_not_support,/*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
  },
  {
    /*geometry -> XXX*/
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
    geometry_pl_extend,/*extend*/
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
    geometry_geometry,/*geometry*/
    cast_not_support,/*udt*/
    cast_not_support,/*decimalint*/
    cast_not_support,/*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
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
    cast_udt_to_other_not_support,/*decimal int*/
    cast_not_expected,/*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
  },
  {
    /*decimalint -> XXX*/
    cast_not_expected,/*null*/
    decimalint_int,/*int*/
    decimalint_uint,/*uint*/
    decimalint_float,/*float*/
    decimalint_double,/*double*/
    decimalint_number,/*number*/
    cast_inconsistent_types,/*datetime*/
    cast_not_expected,/*date*/
    cast_not_expected,/*time*/
    cast_not_expected,/*year*/
    decimalint_string,/*string*/
    cast_not_expected,/*extend*/
    cast_not_expected,/*unknown*/
    decimalint_text,/*text*/
    cast_not_expected,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    cast_inconsistent_types,/*otimestamp*/
    cast_inconsistent_types,/*raw*/
    cast_inconsistent_types,/*interval*/
    cast_inconsistent_types,/*rowid*/
    decimalint_lob,/*lob*/
    decimalint_json,/*json*/
    cast_not_support,/*geometry*/
    cast_to_udt_not_support, /*udt*/
    decimalint_decimalint,/*decimalint*/
    cast_to_udt_not_support, /*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
  },
  {
    /*collection -> XXX*/
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
    cast_udt_to_other_not_support,/*decimalint*/
    cast_udt_to_other_not_support,/*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
  },
  {
    /*mysql date*/
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
    cast_not_expected, /*udt*/
    cast_not_expected,/*decimalint*/
    cast_not_expected,/*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
  },
  {
    /*mysql datetime*/
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
    cast_not_expected, /*udt*/
    cast_not_expected,/*decimalint*/
    cast_not_expected,/*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
  },
};

ObExpr::EvalFunc OB_DATUM_CAST_MYSQL_IMPLICIT[ObMaxTC][ObMaxTC] =
{
  {
    /*null -> XXX*/
    cast_eval_arg,/*null*/
    cast_eval_arg,/*int*/
    cast_eval_arg,/*uint*/
    cast_eval_arg,/*float*/
    cast_eval_arg,/*double*/
    cast_eval_arg,/*number*/
    cast_eval_arg,/*datetime*/
    cast_eval_arg,/*date*/
    cast_eval_arg,/*time*/
    cast_eval_arg,/*year*/
    cast_eval_arg,/*string*/
    cast_eval_arg,/*extend*/
    cast_eval_arg,/*unknown*/
    cast_eval_arg,/*text*/
    cast_eval_arg,/*bit*/
    cast_eval_arg,/*enumset*/
    cast_eval_arg,/*enumsetInner*/
    cast_eval_arg,/*otimestamp*/
    cast_inconsistent_types,/*raw*/
    cast_not_expected,/*interval*/
    cast_not_expected,/*rowid*/
    cast_not_expected,/*lob*/
    cast_eval_arg,/*json*/
    cast_eval_arg,/*geometry*/
    cast_not_expected,/*udt, not implemented in mysql mode*/
    cast_eval_arg,/*decimalint*/
    cast_not_expected,/*collection, not implemented in mysql mode*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_eval_arg,/*roaringbitmap*/
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
    cast_eval_arg,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    cast_not_support,/*otimestamp*/
    cast_inconsistent_types,/*raw*/
    cast_not_expected,/*interval*/
    cast_not_expected,/*rowid*/
    cast_not_expected,/*lob*/
    int_json,/*json*/
    int_geometry,/*geometry*/
    cast_not_expected,/*udt, not implemented in mysql mode*/
    int_decimalint,/*decimalint*/
    cast_not_expected,/*collection, not implemented in mysql mode*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_support,/*roaringbitmap*/
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
    cast_eval_arg,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    cast_not_support,/*otimestamp*/
    cast_inconsistent_types,/*raw*/
    cast_not_expected,/*interval*/
    cast_not_expected,/*rowid*/
    cast_not_expected,/*lob*/
    uint_json,/*json*/
    uint_geometry,/*geometry*/
    cast_not_expected,/*udt, not implemented in mysql mode*/
    uint_decimalint,/*decimalint*/
    cast_not_expected,/*collection, not implemented in mysql mode*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_support,/*roaringbitmap*/
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
    cast_not_expected,/*lob*/
    float_json,/*json*/
    float_geometry,/*geometry*/
    cast_not_expected,/*udt, not implemented in mysql mode*/
    float_decimalint,/*decimalint*/
    cast_not_expected,/*collection, not implemented in mysql mode*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_support,/*roaringbitmap*/
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
    cast_not_expected,/*lob*/
    double_json,/*json*/
    double_geometry,/*geometry*/
    cast_not_expected,/*udt, not implemented in mysql mode*/
    double_decimalint,/*decimalint*/
    cast_not_expected,/*collection, not implemented in mysql mode*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_support,/*roaringbitmap*/
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
    cast_not_expected,/*lob*/
    number_json,/*json*/
    number_geometry,/*geometry*/
    cast_not_expected,/*udt, not implemented in mysql mode*/
    number_decimalint,/*decimalint*/
    cast_not_expected,/*collection, not implemented in mysql mode*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_support,/*roaringbitmap*/
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
    cast_not_expected,/*lob*/
    datetime_json,/*json*/
    datetime_geometry,/*geometry*/
    cast_not_expected,/*udt, not implemented in mysql mode*/
    datetime_decimalint,/*decimalint*/
    cast_not_expected,/*collection, not implemented in mysql mode*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_support,/*roaringbitmap*/
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
    cast_eval_arg,/*date*/
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
    cast_not_expected,/*lob*/
    date_json,/*json*/
    date_geometry,/*geometry*/
    cast_not_expected,/*udt, not implemented in mysql mode*/
    date_decimalint,/*decimalint*/
    cast_not_expected,/*collection, not implemented in mysql mode*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_support,/*roaringbitmap*/
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
    cast_eval_arg,/*time*/
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
    cast_not_expected,/*lob*/
    time_json,/*json*/
    time_geometry,/*geometry*/
    cast_not_expected,/*udt, not implemented in mysql mode*/
    time_decimalint,/*decimalint*/
    cast_not_expected,/*collection, not implemented in mysql mode*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_support,/*roaringbitmap*/
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
    cast_eval_arg,/*year*/
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
    cast_not_expected,/*lob*/
    year_json,/*json*/
    year_geometry,/*geometry*/
    cast_not_expected,/*udt, not implemented in mysql mode*/
    year_decimalint,/*decimalint*/
    cast_not_expected,/*collection, not implemented in mysql mode*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_support,/*roaringbitmap*/
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
    cast_not_expected,/*lob*/
    string_json,/*json*/
    string_geometry,/*geometry*/
    cast_not_expected,/*udt, not implemented in mysql mode*/
    string_decimalint,/*decimalint*/
    cast_not_expected,/*collection, not implemented in mysql mode*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    string_roaringbitmap,/*roaringbitmap*/
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
    cast_eval_arg,/*extend*/
    cast_not_support,/*unknown*/
    cast_not_support,/*text*/
    cast_not_support,/*bit*/
    cast_not_support,/*enumset*/
    cast_not_support,/*enumset_inner*/
    cast_not_support,/*otimestamp*/
    cast_inconsistent_types,/*raw*/
    cast_not_expected,/*interval*/
    cast_not_expected,/*rowid*/
    cast_not_expected,/*lob*/
    cast_not_support,/*json*/
    cast_not_support,/*geometry*/
    cast_not_expected,/*udt, not implemented in mysql mode*/
    cast_not_support,/*decimalint*/
    cast_not_expected,/*collection, not implemented in mysql mode*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_support,/*roaringbitmap*/
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
    cast_eval_arg,/*unknown*/
    cast_not_support,/*text*/
    unknown_other,/*bit*/
    unknown_other,/*enumset*/
    unknown_other,/*enumsetInner*/
    unknown_other,/*otimestamp*/
    cast_inconsistent_types,/*raw*/
    cast_not_expected,/*interval*/
    cast_not_expected,/*rowid*/
    cast_not_expected,/*lob*/
    cast_not_expected,/*json*/
    cast_not_expected,/*geometry*/
    cast_not_expected,/*udt, not implemented in mysql mode*/
    unknown_other,/*decimalint*/
    cast_not_expected,/*collection, not implemented in mysql mode*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
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
    cast_not_expected,/*lob*/
    string_json,/*json*/
    string_geometry,/*geometry*/
    cast_not_expected,/*udt, not implemented in mysql mode*/
    text_decimalint,/*decimalint*/
    cast_not_expected,/*collection, not implemented in mysql mode*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    string_roaringbitmap,/*roaringbitmap*/
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
    cast_eval_arg,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    cast_not_support,/*otimestamp*/
    cast_inconsistent_types,/*raw*/
    cast_not_expected,/*interval*/
    cast_not_expected,/*rowid*/
    cast_not_expected,/*lob*/
    bit_json,/*json*/
    bit_geometry,/*geometry*/
    cast_not_expected,/*udt, not implemented in mysql mode*/
    bit_decimalint,/*decimalint*/
    cast_not_expected,/*collection, not implemented in mysql mode*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_support,/*roaringbitmap*/
  },
  {
    /*enumset -> XXX*/
    cast_not_support,/*null*/
    enumset_int, // /*int*/
    enumset_uint, // /*uint*/
    enumset_float, // /*float*/
    enumset_double, // /*double*/
    enumset_number, // /*number*/
    cast_not_expected,/*datetime*/
    cast_not_expected,/*date*/
    cast_not_expected,/*time*/
    enumset_year, // /*year*/
    cast_not_expected,/*string*/
    cast_not_support,/*extend*/
    cast_not_support,/*unknown*/
    cast_not_expected,/*text*/
    enumset_bit, // /*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    cast_not_support,/*otimestamp*/
    cast_inconsistent_types,/*raw*/
    cast_not_expected,/*interval*/
    cast_not_expected,/*rowid*/
    cast_not_expected,/*lob*/
    cast_not_expected,/*json*/
    cast_not_expected,/*geometry*/
    cast_not_expected,/*udt, not implemented in mysql mode*/
    enumset_decimalint,/*decimalint*/
    cast_not_expected,/*collection, not implemented in mysql mode*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
  },
  {
    /*enumset_inner -> XXX*/
    cast_not_support,/*null*/
    enumset_inner_int, // /*int*/
    enumset_inner_uint, // /*uint*/
    enumset_inner_float, // /*float*/
    enumset_inner_double, // /*double*/
    enumset_inner_number, // /*number*/
    enumset_inner_datetime, // /*datetime*/
    enumset_inner_date, // /*date*/
    enumset_inner_time, // /*time*/
    enumset_inner_year, // /*year*/
    enumset_inner_string, // /*string*/
    cast_not_support,/*extend*/
    cast_not_support,/*unknown*/
    cast_not_support,/*text*/
    enumset_inner_bit, // /*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    cast_not_expected,/*otimestamp*/
    cast_inconsistent_types,/*raw*/
    cast_not_expected,/*interval*/
    cast_not_expected,/*rowid*/
    cast_not_expected,/*lob*/
    cast_not_expected,/*json*/
    cast_not_expected,/*geometry*/
    cast_not_expected,/*udt, not implemented in mysql mode*/
    enumset_inner_decimalint,/*decimalint*/
    cast_not_expected,/*collection, not implemented in mysql mode*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
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
    cast_not_expected,/*udt, not implemented in mysql mode*/
    cast_not_expected,/*decimalint*/
    cast_not_expected,/*collection, not implemented in mysql mode*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
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
    cast_not_expected,/*udt, not implemented in mysql mode*/
    cast_not_expected,/*decimalint*/
    cast_not_expected,/*collection, not implemented in mysql mode*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
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
    cast_not_expected,/*udt, not implemented in mysql mode*/
    cast_not_expected,/*decimalint*/
    cast_not_expected,/*collection, not implemented in mysql mode*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
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
    cast_not_expected,/*udt, not implemented in mysql mode*/
    cast_not_expected,/*decimalint*/
    cast_not_expected,/*collection, not implemented in mysql mode*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
  },
  {
    /*lob -> XXX*/
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
    cast_not_expected,/*udt, not implemented in mysql mode*/
    cast_not_expected,/*decimalint*/
    cast_not_expected,/*collection, not implemented in mysql mode*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
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
    json_geometry,/*geometry*/
    cast_not_expected,/*udt, not implemented in mysql mode*/
    json_decimalint,/*decimalint*/
    cast_not_expected,/*collection, not implemented in mysql mode*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_support,/*roaringbitmap*/
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
    cast_not_expected, /*udt*/
    geometry_decimalint,/*decimalint*/
    cast_not_expected,/*collection, not implemented in mysql mode*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_support,/*roaringbitmap*/
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
    cast_not_expected,/*udt, not implemented in mysql mode*/
    cast_not_expected, /*decimal int*/
    cast_not_expected,/*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
  },
  {
    /*decimalint -> XXX*/
    cast_not_support,/*null*/
    decimalint_int,/*int*/
    decimalint_uint,/*uint*/
    decimalint_float,/*float*/
    decimalint_double,/*double*/
    decimalint_number,/*number*/
    decimalint_datetime,/*datetime*/
    decimalint_date,/*date*/
    decimalint_time,/*time*/
    decimalint_year,/*year*/
    decimalint_string,/*string*/
    cast_not_support,/*extend*/
    cast_not_support,/*unknown*/
    decimalint_text,/*text*/
    decimalint_bit,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    cast_not_support,/*otimestamp*/
    cast_inconsistent_types,/*raw*/
    cast_not_expected,/*interval*/
    cast_not_expected,/*rowid*/
    cast_not_expected,/*lob*/
    decimalint_json,/*json*/
    decimalint_geometry,/*geometry*/
    cast_not_expected,/*udt, not implemented in mysql mode*/
    decimalint_decimalint, /*decimal int*/
    cast_not_expected,/*collection, not implemented in mysql mode*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_support,/*roaringbitmap*/
  },
  {
    /*collection -> XXX*/
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
    cast_not_expected,/*udt, not implemented in mysql mode*/
    cast_not_expected,/*decimalint*/
    cast_not_expected,/*collection, not implemented in mysql mode*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
  },
  {
    /*mysql date -> XXX*/
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
    cast_not_expected,/*udt, not implemented in mysql mode*/
    cast_not_expected,/*decimalint, place_holder*/
    cast_not_expected,/*collection, not implemented in mysql mode*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
  },
  {
    /*mysql datetime -> XXX*/
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
    cast_not_expected,/*udt, not implemented in mysql mode*/
    cast_not_expected,/*decimalint, place_holder*/
    cast_not_expected,/*collection, not implemented in mysql mode*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    cast_not_expected,/*roaringbitmap*/
  },
  {
    /*roaringbitmap -> XXX*/
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
    roaringbitmap_string,/*string*/
    cast_not_support,/*extend*/
    cast_not_support,/*unknown*/
    roaringbitmap_string,/*text*/
    cast_not_support,/*bit*/
    cast_not_expected,/*enumset*/
    cast_not_expected,/*enumset_inner*/
    cast_not_support,/*otimestamp*/
    cast_not_support,/*raw*/
    cast_not_expected,/*interval*/
    cast_not_expected,/*rowid*/
    cast_not_expected,/*lob*/
    cast_not_support,/*json*/
    cast_not_support,/*geometry*/
    cast_not_expected,/*udt*/
    cast_not_support,/*decimalint*/
    cast_not_expected,/*collection*/
    cast_not_expected,/*mysql date*/
    cast_not_expected,/*mysql datetime*/
    roaringbitmap_roaringbitmap,/*roaringbitmap*/
  },
};

ObExpr::EvalEnumSetFunc OB_DATUM_CAST_MYSQL_ENUMSET_IMPLICIT[ObMaxTC][2] =
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
  {
    /*UDT -> enum_or_set*/
    cast_not_support_enum_set,/*enum*/
    cast_not_support_enum_set,/*set*/
  },
  {
    /*decimalint -> enum_or_set*/
    decimalint_enum,/*enum*/
    decimalint_set,/*set*/
  },
  {
    /*ObCollectionSQLTC -> enum_or_set*/
    cast_not_support_enum_set,/*enum*/
    cast_not_support_enum_set,/*set*/
  },
  {
    /*ObMySQLDate -> enum_or_set*/
    cast_not_support_enum_set,/*enum*/
    cast_not_support_enum_set,/*set*/
  },
  {
    /*ObMySQLDateTime -> enum_or_set*/
    cast_not_support_enum_set,/*enum*/
    cast_not_support_enum_set,/*set*/
  },
  {
    /*ObRoaringBitmapTC -> enum_or_set*/
    cast_not_support_enum_set,/*enum*/
    cast_not_support_enum_set,/*set*/
  },
};

int string_collation_check(const bool is_strict_mode,
                           const ObCollationType check_cs_type,
                           const ObObjType str_type,
                           ObString &str)
{
  int ret = OB_SUCCESS;
  if (!ob_is_string_type(str_type)) {
    // nothing to do
  } else if (check_cs_type == CS_TYPE_BINARY) {
    //任何类型都可以直接转成binary
    // do nothing
  } else {
    int64_t well_formed_len = 0;
    if (OB_FAIL(ObCharset::well_formed_len(check_cs_type,
                                           str.ptr(),
                                           str.length(),
                                           well_formed_len))) {
      LOG_WARN("invalid string for charset",
                K(ret), K(is_strict_mode), K(check_cs_type), K(str), K(well_formed_len));
      if (lib::is_oracle_mode()) {
        // invalid character is acceptable in oracle mode
        ret = OB_SUCCESS;
      } else if (is_strict_mode) {
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

// 不能进行cast的情况包括：
// 1. Oracle模式下, string/text/lob->string/text/lob时, blob不支持转向nonblob
// 2. Oracle模式下, string/text/lob->string/text/lob, nonblob转向blob时,如果输入必须是char/varchar/raw
// TODO by shaoge
// 3. Oracle模式下, 只有string/text->string/text的转换支持blob往其他类型转,其余的不允许
// 4. TODO: lob_outrow的处理还未完善
int ObDatumCast::check_can_cast(const ObObjType in_type,
                                const ObCollationType in_cs_type,
                                const ObObjType out_type,
                                const ObCollationType out_cs_type)
{
  int ret = OB_SUCCESS;
  ObObjTypeClass in_tc = ob_obj_type_class(in_type);
  ObObjTypeClass out_tc = ob_obj_type_class(out_type);
  const bool is_stringtext_tc_to_stringtext_tc =
    ((ObStringTC == in_tc || ObTextTC == in_tc || ObLobTC == in_tc) &&
    (ObStringTC == out_tc || ObTextTC == out_tc || ObLobTC == out_tc));
  const bool is_blob_in = ob_is_blob(in_type, in_cs_type)
                          || ob_is_blob_locator(in_type, in_cs_type);
  const bool is_blob_out = ob_is_blob(out_type, out_cs_type)
                          || ob_is_blob_locator(out_type, out_cs_type);

  const bool is_blob_to_nonblob = is_blob_in && (! is_blob_out);
  const bool is_nonblob_to_blob = (! is_blob_in) && is_blob_out;

  if (ObNullType == in_type || ObNullType == out_type) {
    // let null be ok
  } else if (! lib::is_oracle_mode()) {
  } else if(is_blob_in && ObJsonTC == out_tc) {
  } else if ((ob_is_number_tc(in_type)|| ob_is_decimal_int_tc(in_type)
              || ob_is_int_tc(in_type) || ob_is_datetime_tc(in_type)
              || ob_is_clob(in_type, in_cs_type) || ob_is_clob_locator(in_type, in_cs_type))
              && is_blob_out) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("cast number to blob not allowed", K(ret));
  } else if ((ob_is_clob(in_type, in_cs_type)
              || ob_is_clob_locator(in_type, in_cs_type))
            && ObRawTC == out_tc) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("cast clob to raw not allowed", K(ret));
  } else if (is_oracle_mode() && ObTinyIntType == in_type && ob_is_text_tc(out_type)) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("cast boolean to lob not allowed", K(ret));
  } else if (is_stringtext_tc_to_stringtext_tc && is_blob_to_nonblob) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("invalid use of blob type", K(ret), K(out_type));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "blob cast to other type");
  } else if (is_stringtext_tc_to_stringtext_tc && is_nonblob_to_blob
             && !ob_is_raw(in_type) && !ob_is_varchar_char_type(in_type, in_cs_type)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("invalid use of blob type", K(ret), K(out_type), K(out_cs_type),
              K(in_type), K(in_cs_type));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "cast to blob type");
  // } else if (ObTextTC == in_tc && is_lob_outrow) {
  //   ret = OB_NOT_SUPPORTED;
  //   LOG_WARN("cannot cast blob to nonblob", K(ret));
  // shanting: is_stringtext_tc_to_nonstringtext_tc没有使用，不知道是不是有问题。@shaoge
  } else if (!is_stringtext_tc_to_stringtext_tc && is_blob_in && ObRawTC != out_tc) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("cannot cast blob to nonblob", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "cast blob to nonblob type");
  }
  return ret;
}

// string/text -> string/text的特殊情况的描述:
// 1. !blob -> blob is ok. (in_type must be varchar/char/raw, varchar/char call hextoraw to cast)
// 2. !blob -> !blob is ok (just copy or convert charset)
//      a. 如果是相同字符集，则调用cast_eval_arg
//      b. 如果是不同字符集，且输入输出都不是cs_type_binary，则需要进行字符集转换
//      c. 如果输入输出有任意一个是cs_type_binary，会调用cast_eval_arg
// 3. blob -> blob ok. 直接调用cast_eval_arg
// 4. blob -> !blob not ok.choose_cast_func会检测并报错
int ObDatumCast::is_trivial_cast(const ObObjType in_type,
                                 const ObCollationType in_cs_type,
                                 const ObObjType out_type,
                                 const ObCollationType out_cs_type,
                                 const ObCastMode &cast_mode,
                                 bool &is_trivial_cast)
{
  is_trivial_cast = false;
  int ret = OB_SUCCESS;

  ObCharsetType in_cs = ObCharset::charset_type_by_coll(in_cs_type);
  ObCharsetType out_cs = ObCharset::charset_type_by_coll(out_cs_type);

  ObObjTypeClass in_tc = ob_obj_type_class(in_type);
  ObObjTypeClass out_tc = ob_obj_type_class(out_type);
  const bool is_same_charset = (ob_is_string_type(in_type) &&
      ob_is_string_type(out_type) &&
      (in_cs == out_cs ||
      /** GB18030 and GB18030_2022 have the same code points,
       *  but they have different mapping to unicode.
       *  So, we do not do charset_convert for them in cast*/
      (in_cs == CHARSET_GB18030 && out_cs == CHARSET_GB18030_2022) ||
      (in_cs == CHARSET_GB18030_2022 && out_cs == CHARSET_GB18030)));
  const bool is_clob_to_nonclob = (ob_is_clob(in_type, in_cs_type)
                                   && !ob_is_clob(out_type, out_cs_type));
  const bool is_nonblob_to_blob = ((false == ob_is_blob(in_type, in_cs_type)) &&
      (true == ob_is_blob(out_type, out_cs_type)));
  const bool is_blob_to_blob = ((true == ob_is_blob(in_type, in_cs_type)) &&
      (true == ob_is_blob(out_type, out_cs_type)));
  const bool is_nonblob_to_nonblob = ((false == ob_is_blob(in_type, in_cs_type)) &&
      (false == ob_is_blob(out_type, out_cs_type)));
  // Notice: large text types has lob locator header, cannot be trivial when cast to tinytext or strings
  const bool is_stringtext_tc_to_stringtext_tc = ((ObStringTC == in_tc || ObTextTC == in_tc)
                                                  && (ObStringTC == out_tc || ObTextTC == out_tc));
  const bool is_large_text_to_large_text = (ob_is_large_text(in_type) && ob_is_large_text(out_type));
  const bool is_small_text_to_small_text = ((ObStringTC == in_tc || ObTinyTextType == in_type)
                                            && (ObStringTC == out_tc || ObTinyTextType == out_type));
  const bool may_be_trivial_cast = (is_large_text_to_large_text
                                    || is_small_text_to_small_text
                                    || (is_stringtext_tc_to_stringtext_tc && !ob_enable_lob_locator_v2()));
  if (ObNullType == in_type) {
    // cast func of xxx(not_null)-> null is cast_not_expected() or cast_not_support()
    // cast func of null -> xxx is cast_eval_arg
    is_trivial_cast = true;
  } else if (ob_is_raw(in_type) && ob_is_blob(out_type, out_cs_type)) {
    // if locator v2 enabled, cannot do trivial cast, or should always set to true?
    // here may be called by get_next_row, not in cg.
    is_trivial_cast = !ob_enable_lob_locator_v2();
  } else if (may_be_trivial_cast && lib::is_oracle_mode()) {
    // In order to keep physical plan the same with 4.0
    if ((is_same_charset && !is_nonblob_to_blob) ||
        (is_blob_to_blob) ||
        (is_nonblob_to_nonblob && (CS_TYPE_BINARY == in_cs_type ||
                                    CS_TYPE_BINARY == out_cs_type))) {
      if (!is_clob_to_nonclob) {
        is_trivial_cast = true;
      }
    }
  } else if (may_be_trivial_cast && !lib::is_oracle_mode()) {
    if ((is_same_charset || CS_TYPE_BINARY == out_cs_type)) {
      is_trivial_cast = true;
    }
  } else if (!lib::is_oracle_mode() &&
              ((ObIntTC == in_tc && ObBitTC == out_tc) ||
              (ObUIntTC == in_tc && ObBitTC == out_tc) ||
              (ObDateType == in_type && ObDateType == out_type) ||
              (ObYearType == in_type && ObYearType == out_type) ||
              (ObExtendType == in_type && ObExtendType == out_type) ||
              (ObBitType == in_type && ObBitType == out_type) ||
              (ObUnknownType == in_type && ObUnknownType == out_type))) {
    is_trivial_cast = true;
  } else if (lib::is_oracle_mode() &&
              ((ObExtendType == in_type && ObExtendType == out_type) ||
              (ObUnknownType == in_type && ObUnknownType == out_type) ||
              (ObIntervalYMType == in_type && ObIntervalYMType == out_type) ||
              (ObIntervalDSType == in_type && ObIntervalDSType == out_type))) {
    // Oracle模式没有bit/year/date/time类型(Oracle的date类型在OB中用ObDateTimeType表示)
    is_trivial_cast = true;
  } else if (ObUIntTC == in_tc && ObIntTC == out_tc &&
              CM_IS_EXTERNAL_CALL(cast_mode) && CM_SKIP_CAST_INT_UINT(cast_mode)) {
    is_trivial_cast = true;
  } else {
    is_trivial_cast = false;
  }
  LOG_DEBUG("is_trivial_cast debug", K(ret), K(in_type), K(out_type), K(in_cs_type),
      K(out_cs_type), K(cast_mode), K(is_trivial_cast), K(lbt()));
  return ret;
}

int ObDatumCast::get_implicit_cast_function(const ObObjType in_type,
                                            const ObCollationType in_cs_type,
                                            const ObObjType out_type,
                                            const ObCollationType out_cs_type,
                                            const int64_t cast_mode,
                                            ObExpr::EvalFunc &eval_func)

{
  int ret = OB_SUCCESS;
  bool pass_cast = false;
  if (OB_FAIL(check_can_cast(in_type, in_cs_type, out_type, out_cs_type))) {
    LOG_WARN("check_can_cast failed", K(ret));
  } else if (OB_FAIL(is_trivial_cast(in_type, in_cs_type, out_type,
                                     out_cs_type, cast_mode, pass_cast))) {
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

int ObDatumCast::choose_cast_function(const ObObjType in_type,
                                      const ObCollationType in_cs_type,
                                      const ObObjType out_type,
                                      const ObCollationType out_cs_type,
                                      const int64_t cast_mode,
                                      ObIAllocator &allocator,
                                      bool &just_eval_arg,
                                      ObExpr &rt_expr)
{
  int ret = OB_SUCCESS;
  just_eval_arg = false;
  ObObjTypeClass in_tc = ob_obj_type_class(in_type);
  ObObjTypeClass out_tc = ob_obj_type_class(out_type);
  if (OB_FAIL(check_can_cast(in_type, in_cs_type, out_type, out_cs_type))) {
    LOG_WARN("check_can_cast failed", K(ret));
  } else if (OB_FAIL(is_trivial_cast(in_type, in_cs_type, out_type,
                                     out_cs_type, cast_mode, just_eval_arg))) {
    LOG_WARN("is_trivial_cast failed", K(ret), K(in_type), K(out_type));
  } else if (just_eval_arg && !CM_IS_EXPLICIT_CAST(cast_mode)) {
    // 即使是相同类型，显式cast也需要进行accuracy check
    rt_expr.eval_func_ = cast_eval_arg;
  } else {
    if (CM_IS_EXPLICIT_CAST(cast_mode)) {
      if (OB_ISNULL(rt_expr.inner_functions_ =
            reinterpret_cast<void**>(allocator.alloc(sizeof(void*))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc memory failed", K(ret));
      } else {
        rt_expr.inner_func_cnt_ = 1;
        if (just_eval_arg) {
          rt_expr.inner_functions_[0] = reinterpret_cast<void*>(cast_eval_arg);
        } else {
          if (lib::is_oracle_mode()) {
            rt_expr.inner_functions_[0] =
              reinterpret_cast<void*>(OB_DATUM_CAST_ORACLE_IMPLICIT[in_tc][out_tc]);
          } else {
            rt_expr.inner_functions_[0] =
              reinterpret_cast<void*>(OB_DATUM_CAST_MYSQL_IMPLICIT[in_tc][out_tc]);
          }
        }
        if (ob_is_character_type(out_type, out_cs_type) ||
            ob_is_varbinary_or_binary(out_type, out_cs_type)) {
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
  if (OB_SUCC(ret)) {
    // TODO:@xiaofeng.lby, need to modify cast expr in batch mode
    // cast in batch mode will degrade into single row mode now
    if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_2_0_0) {
      if (CM_IS_EXPLICIT_CAST(cast_mode)) {
        rt_expr.eval_batch_func_ = ObBatchCast::get_explicit_cast_func(in_tc, out_tc);
      } else {
        rt_expr.eval_batch_func_ = ObBatchCast::get_implicit_cast_func(in_tc, out_tc);
      }
    } else {
      rt_expr.eval_batch_func_ = cast_eval_arg_batch;
    }
  }
  LOG_DEBUG("in choose_cast_function", K(ret), K(in_type), K(out_type),
      K(in_cs_type), K(out_cs_type), K(CM_IS_EXPLICIT_CAST(cast_mode)),
      K(CM_IS_ZERO_FILL(cast_mode)), K(cast_mode), K(lbt()));
  return ret;
}

int ObDatumCast::get_enumset_cast_function(const common::ObObjTypeClass in_tc,
    const common::ObObjType out_type, ObExpr::EvalEnumSetFunc &eval_func)
{
  int ret = OB_SUCCESS;
  // in_type可以为NullType, out_type不能为NullType
  if (OB_UNLIKELY(!(ObNullTC <= in_tc && in_tc < ObMaxTC))
      || OB_UNLIKELY(out_type != ObEnumType && out_type != ObSetType)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expecected intype or outtype", K(ret), K(in_tc), K(out_type));
  } else {
    eval_func = OB_DATUM_CAST_MYSQL_ENUMSET_IMPLICIT[in_tc][out_type == ObSetType];
  }
  LOG_DEBUG("in get_enumset_cast_function", K(ret), K(in_tc), K(out_type));
  return ret;
}

int ObDatumCast::cast_obj(ObEvalCtx &ctx, ObIAllocator &alloc, const ObObjType &dst_type,
                          common::ObAccuracy &dst_acc,
                          const ObCollationType &dst_cs_type, const ObObj &src_obj,
                          ObObj &dst_obj)
{
  int ret = OB_SUCCESS;
  ObCastMode def_cm = CM_NONE;
  ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  CK(OB_NOT_NULL(session));
  if (OB_SUCC(ret)) {
    ObPhysicalPlanCtx *phy_plan_ctx = ctx.exec_ctx_.get_physical_plan_ctx();
    const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(session);
    if (OB_FAIL(ObSQLUtils::get_default_cast_mode(session->get_stmt_type(),
                                                  session, def_cm))) {
      LOG_WARN("get_default_cast_mode failed", K(ret));
    } else {
      ObCastCtx cast_ctx(&alloc, &dtc_params, get_cur_time(phy_plan_ctx), def_cm,
                         dst_cs_type, NULL, &dst_acc);
      if (OB_FAIL(ObObjCaster::to_type(dst_type, cast_ctx, src_obj, dst_obj))) {
        LOG_WARN("failed to cast object to ", K(ret), K(src_obj), K(dst_type));
      }
    }
  }
  return ret;
}

int ObDatumCaster::init(ObExecContext &ctx)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    void *ctx_mem = NULL;
    void *expr_mem = NULL;
    void *extra_mem = NULL;
    void *expr_args_mem = NULL;
    void *extra_args_mem = NULL;
    void *frame_mem = NULL;
    void *frames_mem = NULL;
    ObIAllocator &alloc = ctx.get_allocator();
    int64_t res_buf_len = ObDatum::get_reserved_size(ObObjDatumMapType::OBJ_DATUM_STRING);
      const int64_t datum_eval_info_size = sizeof(ObDatum) + sizeof(ObEvalInfo);
    int64_t frame_size = (datum_eval_info_size + sizeof(ObDynReserveBuf) + res_buf_len) * 2;
    int64_t frames_size = 0;

    CK(OB_NOT_NULL(ctx.get_frames()));

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
      char *frame = reinterpret_cast<char*>(frame_mem);
      MEMSET(frame, 0, frame_size);
      eval_ctx_ = new(ctx_mem) ObEvalCtx(ctx);
      eval_ctx_->frames_ = reinterpret_cast<char**>(frames_mem);
      eval_ctx_->frames_[ctx.get_frame_cnt()] = frame;
      eval_ctx_->max_batch_size_ = 0;
      eval_ctx_->set_batch_size(1);
      eval_ctx_->set_batch_idx(0);

      // init cast_expr_/extra_cast_expr and frame
      cast_expr_ = new (expr_mem) ObExpr();
      cast_expr_->args_ = reinterpret_cast<ObExpr**>(expr_args_mem);
      extra_cast_expr_ = new (extra_mem) ObExpr();
      extra_cast_expr_->args_ = reinterpret_cast<ObExpr**>(extra_args_mem);
      ObExpr *exprs[2] = {cast_expr_, extra_cast_expr_};

      int64_t data_off = datum_eval_info_size * 2;
      const int64_t consume_size = res_buf_len + sizeof(ObDynReserveBuf);
      for (int64_t i = 0; OB_SUCC(ret) && i < sizeof(exprs) / sizeof(ObExpr*); ++i) {
        ObExpr *e = exprs[i];
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
        e->dyn_buf_header_offset_ = e->res_buf_off_ - sizeof(ObDynReserveBuf);
        e->arg_cnt_ = 1;

        ObDatum *expr_datum = reinterpret_cast<ObDatum*>(frame + e->datum_off_);
        expr_datum->ptr_ = frame + e->res_buf_off_;
        ObDynReserveBuf *drb = reinterpret_cast<ObDynReserveBuf*>(
                   frame + e->dyn_buf_header_offset_ + sizeof(ObDynReserveBuf));
        drb->len_ = e->res_buf_len_;
        drb->mem_ = frame + e->res_buf_off_;
      }
    }
    OX(inited_ = true);
  }
  return ret;
}

int ObDatumCaster::to_type(const ObDatumMeta &dst_type,
                           const ObExpr &src_expr,
                           const ObCastMode &cm,
                           ObDatum *&res,
                           int64_t batch_idx,
                           const uint16_t subschema_id)
{
  int ret = OB_SUCCESS;
  const ObDatumMeta &src_type = src_expr.datum_meta_;
  const ObCharsetType &src_cs = ObCharset::charset_type_by_coll(src_type.cs_type_);
  const ObCharsetType &dst_cs = ObCharset::charset_type_by_coll(dst_type.cs_type_);
  bool need_cast_decimalint =
      (src_type.type_ == dst_type.type_) &&
      ob_is_decimal_int_tc(src_type.type_) &&
      ObDatumCast::need_scale_decimalint(src_type.scale_, src_type.precision_,
                                         dst_type.scale_, dst_type.precision_);
  if (OB_UNLIKELY(!inited_) || OB_ISNULL(eval_ctx_) || OB_ISNULL(cast_expr_) ||
      OB_ISNULL(extra_cast_expr_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDatumCaster is invalid", K(ret), K(inited_), KP(eval_ctx_),
                                         KP(cast_expr_), KP(extra_cast_expr_));
  } else if (FALSE_IT(eval_ctx_->batch_idx_ = batch_idx)) {
  } else if ((ob_is_string_or_lob_type(src_type.type_) && src_type.type_ == dst_type.type_
              && src_cs == dst_cs)
             || (!ob_is_string_or_lob_type(src_type.type_) && !need_cast_decimalint
                 && src_type.type_ == dst_type.type_)) {
    LOG_DEBUG("no need to cast, just eval src_expr", K(ret), K(src_expr), K(dst_type));
    if (OB_FAIL(src_expr.eval(*eval_ctx_, res))) { LOG_WARN("eval src_expr failed", K(ret)); }
  } else {
    bool nonstr_to_str = !ob_is_string_or_lob_type(src_type.type_) &&
                         ob_is_string_or_lob_type(dst_type.type_);
    bool str_to_nonstr = ob_is_string_or_lob_type(src_type.type_) &&
                         !ob_is_string_or_lob_type(dst_type.type_);
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
      if (OB_FAIL(setup_cast_expr(dst_type, src_expr, cm, *cast_expr_, subschema_id))) {
        LOG_WARN("setup_cast_expr failed", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(cast_expr_->eval(*eval_ctx_, res))) {
      LOG_WARN("eval cast expr failed", K(ret));
    }
    LOG_DEBUG("ObDatumCaster::to_type done", K(ret), K(src_expr), K(dst_type),
              K(cm), K(need_extra_cast_for_src_type), K(need_extra_cast_for_dst_type),
              KP(eval_ctx_->frames_));
  }
  return ret;
}

int ObDatumCaster::to_type(const ObDatumMeta &dst_type,
                           const ObIArray<ObString> &str_values,
                           const ObExpr &src_expr,
                           const ObCastMode &cm,
                           ObDatum *&res,
                           int64_t batch_idx)
{
  int ret = OB_SUCCESS;
  const ObDatumMeta &src_type = src_expr.datum_meta_;
  if (OB_UNLIKELY(!inited_) || OB_ISNULL(cast_expr_) || OB_ISNULL(extra_cast_expr_) ||
      OB_ISNULL(eval_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDatumCaster is invalid", K(ret), K(inited_), KP(eval_ctx_),
                                         KP(cast_expr_), KP(extra_cast_expr_));
  } else if (OB_UNLIKELY(!ob_is_enumset_tc(dst_type.type_)) ||
             OB_UNLIKELY(ob_is_invalid_obj_type(src_type.type_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid src_type or dst_type", K(ret), K(src_type), K(dst_type));
  } else {
    // enum -> enum or set -> set will give error(see OB_DATUM_CAST_MYSQL_ENUMSET_IMPLICIT)
    // we do not check if need cast
    bool need_extra_cast = false;
    eval_ctx_->batch_idx_ = batch_idx;
    const ObCharsetType &src_cs = ObCharset::charset_type_by_coll(src_type.cs_type_);
    if (ob_is_string_type(src_type.type_) && CHARSET_BINARY != src_cs &&
        ObCharset::get_default_charset() != src_cs) {
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
      if (OB_FAIL(cast_expr_->eval_enumset(*eval_ctx_, str_values,
                                           cast_expr_->extra_, res))) {
        LOG_WARN("eval_enumset failed", K(ret));
      }
    }
    LOG_DEBUG("ObDatumCaster::to_type done", K(ret), K(src_expr), K(dst_type),
              K(str_values), KP(eval_ctx_->frames_), K(cm), K(need_extra_cast), K(lbt()));
  }
  return ret;
}

int ObDatumCaster::destroy()
{
  int ret = OB_SUCCESS;
  if (inited_) {
    if (OB_ISNULL(cast_expr_) || OB_ISNULL(extra_cast_expr_) || OB_ISNULL(eval_ctx_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("ObDatumCaster is invalid", K(ret), K(inited_), KP(eval_ctx_),
                                           KP(cast_expr_), KP(extra_cast_expr_));
    } else {
      inited_ = false;
      ObIAllocator &alloc = eval_ctx_->exec_ctx_.get_allocator();
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

int ObDatumCaster::setup_cast_expr(const ObDatumMeta &dst_type,
                                   const ObExpr &src_expr,
                                   const ObCastMode cm,
                                   ObExpr &cast_expr,
                                   const uint16_t subschema_id)
{
  int ret = OB_SUCCESS;
  const ObDatumMeta &src_type = src_expr.datum_meta_;
  if (!inited_ || OB_ISNULL(cast_expr.args_) || OB_ISNULL(eval_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("invalid cast expr", K(ret), K(inited_), KP(cast_expr.args_));
  } else if (OB_FAIL(ObDatumCast::get_implicit_cast_function(src_type.type_,
                                                             src_type.cs_type_,
                                                             dst_type.type_,
                                                             dst_type.cs_type_,
                                                             cm, cast_expr.eval_func_))) {
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
    if (is_lob_storage(dst_type.type_)) {
      if (is_lob_storage(src_expr.obj_meta_.get_type())) {
        // if src is lob types from ps (in param store), it may not have lob head,
        // and run cast_eval_arg, in this case, has no chance to add lob header, unless we modify cast_eval_arg
        if (src_expr.obj_meta_.has_lob_header()) {
          cast_expr.obj_meta_.set_has_lob_header();
        }
      } else if (!IS_CLUSTER_VERSION_BEFORE_4_1_0_0) { // other types to lobs
        cast_expr.obj_meta_.set_has_lob_header();
      }
    }
    // ObDatumMeta does not have cs_level, deduce subschema id from source
    if (cast_expr.obj_meta_.is_user_defined_sql_type() || cast_expr.obj_meta_.is_collection_sql_type()) {
      cast_expr.obj_meta_.set_subschema_id(subschema_id);
    }
    if (ob_is_user_defined_pl_type(src_expr.obj_meta_.get_type()) && dst_type.type_ == ObUserDefinedSQLType) {
      cast_expr.obj_meta_.set_subschema_id(subschema_id);
    }
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

static_assert(ObMaxTC * ObMaxTC == sizeof(OB_DATUM_CAST_ORACLE_IMPLICIT) / sizeof(void *),
              "unexpected size");
static void *g_ser_datum_cast_oracle_implicit[ObMaxTC * ObMaxTC];
bool g_ser_datum_cast_oracle_implicit_init = ObFuncSerialization::convert_NxN_array(
    g_ser_datum_cast_oracle_implicit,
    reinterpret_cast<void **>(OB_DATUM_CAST_ORACLE_IMPLICIT),
    ObMaxTC);
REG_SER_FUNC_ARRAY(OB_SFA_DATUM_CAST_ORACLE_IMPLICIT,
                   g_ser_datum_cast_oracle_implicit,
                   ARRAYSIZEOF(g_ser_datum_cast_oracle_implicit));

static_assert(ObMaxTC * ObMaxTC == sizeof(OB_DATUM_CAST_ORACLE_EXPLICIT) / sizeof(void *),
              "unexpected size");
static void *g_ser_datum_cast_oracle_explicit[ObMaxTC * ObMaxTC];
bool g_ser_datum_cast_oracle_explcit_init = ObFuncSerialization::convert_NxN_array(
    g_ser_datum_cast_oracle_explicit,
    reinterpret_cast<void **>(OB_DATUM_CAST_ORACLE_EXPLICIT),
    ObMaxTC);
REG_SER_FUNC_ARRAY(OB_SFA_DATUM_CAST_ORACLE_EXPLICIT,
                   g_ser_datum_cast_oracle_explicit,
                   ARRAYSIZEOF(g_ser_datum_cast_oracle_explicit));

static_assert(ObMaxTC * ObMaxTC == sizeof(OB_DATUM_CAST_MYSQL_IMPLICIT) / sizeof(void *),
              "unexpected size");
static void *g_ser_datum_cast_mysql_implicit[ObMaxTC * ObMaxTC];
bool g_ser_datum_cast_mysql_implicit_init = ObFuncSerialization::convert_NxN_array(
    g_ser_datum_cast_mysql_implicit,
    reinterpret_cast<void **>(OB_DATUM_CAST_MYSQL_IMPLICIT),
    ObMaxTC);
REG_SER_FUNC_ARRAY(OB_SFA_DATUM_CAST_MYSQL_IMPLICIT,
                   g_ser_datum_cast_mysql_implicit,
                   ARRAYSIZEOF(g_ser_datum_cast_mysql_implicit));

static_assert(ObMaxTC * 2 == sizeof(OB_DATUM_CAST_MYSQL_ENUMSET_IMPLICIT) / sizeof(void *),
              "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_DATUM_CAST_MYSQL_ENUMSET_IMPLICIT,
                   OB_DATUM_CAST_MYSQL_ENUMSET_IMPLICIT,
                   sizeof(OB_DATUM_CAST_MYSQL_ENUMSET_IMPLICIT) / sizeof(void *));

DEF_BATCH_CAST_FUNC(ObDecimalIntTC, ObIntTC)
{
  int ret = OB_SUCCESS;
  EVAL_BATCH_ARGS()
  {
    DEF_BATCH_CAST_PARAMS;
    if (eval_flags.accumulate_bit_cnt(batch_size) == batch_size
        || skip.accumulate_bit_cnt(batch_size) == batch_size) {
      // do nothing
    } else {
      ObScale in_scale = expr.args_[0]->datum_meta_.scale_;
      char buffer[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
      int64_t buf_len = sizeof(buffer);
      int64_t pos = 0;
      for (int i = 0; OB_SUCC(ret) && i < batch_size; i++) {
        pos = 0;
        if (skip.at(i) || eval_flags.at(i)) {
          continue;
        } else if (arg_dv.at(i)->is_null()) {
          result_dv.at(i)->set_null();
          eval_flags.set(i);
        } else if (OB_FAIL(wide::to_string(arg_dv.at(i)->get_decimal_int(),
                                           arg_dv.at(i)->get_int_bytes(), in_scale, buffer, buf_len,
                                           pos))) {
          LOG_WARN("to_string failed", K(ret));
        } else {
          ObString num_str(pos, buffer);
          if (OB_FAIL(common_string_int(expr, expr.extra_, num_str, false, *result_dv.at(i)))) {
            LOG_WARN("common_string_int failed", K(ret));
          } else {
            eval_flags.set(i);
          }
        }
      } // end for
    }
  }
  return ret;
}

DEF_BATCH_CAST_FUNC(ObDecimalIntTC, ObUIntTC)
{
  int ret = OB_SUCCESS;
  EVAL_BATCH_ARGS()
  {
    DEF_BATCH_CAST_PARAMS;
    if (eval_flags.accumulate_bit_cnt(batch_size) == batch_size
        || skip.accumulate_bit_cnt(batch_size) == batch_size) {
      // do nothing
    } else {
      ObScale in_scale = expr.args_[0]->datum_meta_.scale_;
      char buffer[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
      int64_t buf_len = sizeof(buffer);
      int64_t pos = 0;
      for (int i = 0; OB_SUCC(ret) && i < batch_size; i++) {
        pos = 0;
        if (skip.at(i) || eval_flags.at(i)) {
          continue;
        } else if (arg_dv.at(i)->is_null()) {
          result_dv.at(i)->set_null();
          eval_flags.set(i);
        } else if (OB_FAIL(wide::to_string(arg_dv.at(i)->get_decimal_int(),
                                           arg_dv.at(i)->get_int_bytes(), in_scale, buffer, buf_len,
                                           pos))) {
          LOG_WARN("to_string failed", K(ret));
        } else {
          ObString num_str(pos, buffer);
          if (OB_FAIL(common_string_uint(expr, num_str, false, *result_dv.at(i)))) {
            LOG_WARN("common_string_uint failed", K(ret));
          } else {
            eval_flags.set(i);
          }
        }
      } // end for
    }
  }
  return ret;
}

DEF_BATCH_CAST_FUNC(ObDecimalIntTC, ObNumberTC)
{
#define DO_BATCH_TRANS(int_type)                                                                   \
  for (int i = 0; OB_SUCC(ret) && i < batch_size; i++) {                                           \
    if (eval_flags.at(i) || skip.at(i)) {                                                          \
      continue;                                                                                    \
    } else if (arg_dv.at(i)->is_null()) {                                                          \
      result_dv.at(i)->set_null();                                                                 \
      eval_flags.set(i);                                                                           \
    } else {                                                                                       \
      const int_type *v = reinterpret_cast<const int_type *>(arg_dv.at(i)->get_decimal_int());     \
      number::ObNumber tmp_nmb;                                                                    \
      if (OB_FAIL(wide::to_number(*v, in_scale, alloc, tmp_nmb))) {                                \
        LOG_WARN("wide::to_number failed", K(ret));                                                \
      } else if (ObUNumberType == expr.datum_meta_.type_) {                                        \
        int warning = OB_SUCCESS;                                                                  \
        if (CAST_FAIL(numeric_negative_check(tmp_nmb))) {                                          \
          LOG_WARN("numeric_negative_check failed", K(ret));                                       \
        } else {                                                                                   \
          result_dv.at(i)->set_number(tmp_nmb);                                                    \
          eval_flags.set(i);                                                                       \
        }                                                                                          \
      } else {                                                                                     \
        result_dv.at(i)->set_number(tmp_nmb);                                                      \
        eval_flags.set(i);                                                                         \
      }                                                                                            \
    }                                                                                              \
  }

  int ret = OB_SUCCESS;
  LOG_DEBUG("eval batch cast from decimal into to number", K(lbt()), K(batch_size));
  EVAL_BATCH_ARGS()
  {
    DEF_BATCH_CAST_PARAMS;
    if (eval_flags.accumulate_bit_cnt(batch_size) == batch_size
        || skip.accumulate_bit_cnt(batch_size) == batch_size) {
      // do nothing
    } else {
      int32_t int_bytes = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(
        expr.args_[0]->datum_meta_.precision_);
      ObScale in_scale = expr.args_[0]->datum_meta_.scale_;
      ObPrecision in_prec = expr.args_[0]->datum_meta_.precision_;
      ObDecimalIntWideType dec_type = get_decimalint_type(in_prec);
      if (OB_UNLIKELY(in_scale < 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid scale", K(ret), K(in_scale));
      } else if (dec_type <= DECIMAL_INT_64 && in_scale <= 9) {
        // fast casting to number
        number::ObNumber tmp_nmb;
        uint32_t digits[3] = {0};
        auto cast_number = [&](const int idx) __attribute__((always_inline))
        {
          int ret = OB_SUCCESS;
          int64_t input = 0;
          if (dec_type == DECIMAL_INT_32) {
            input = *reinterpret_cast<const int32_t *>(arg_dv.at(idx)->get_decimal_int());
          } else {
            input = *reinterpret_cast<const int64_t *>(arg_dv.at(idx)->get_decimal_int());
          }
          if (OB_FAIL(wide::to_number(input, in_scale, (uint32_t *)digits, 3, tmp_nmb))) {
            LOG_WARN("wide::to_number failed", K(ret));
          } else if (ObUNumberType == expr.datum_meta_.type_) {
            int warning = OB_SUCCESS;
            if (CAST_FAIL(numeric_negative_check(tmp_nmb))) {
              LOG_WARN("numeric_negative_check failed", K(ret));
            } else {
              result_dv.at(idx)->set_number(tmp_nmb);
            }
          } else {
            result_dv.at(idx)->set_number(tmp_nmb);
          }
          return ret;
        };
        if (OB_LIKELY(skip.accumulate_bit_cnt(batch_size) == 0
                      && eval_flags.accumulate_bit_cnt(batch_size) == 0)) {
          for (int i = 0; OB_SUCC(ret) && i < batch_size; i++) {
            if (arg_dv.at(i)->is_null()) {
              result_dv.at(i)->set_null();
            } else {
              ret = cast_number(i);
            }
          }
          if (OB_SUCC(ret)) { eval_flags.set_all(batch_size); }
        } else {
          for (int i = 0; OB_SUCC(ret) && i < batch_size; i++) {
            if (eval_flags.at(i) || skip.at(i)) {
              continue;
            } else if (arg_dv.at(i)->is_null()) {
              result_dv.at(i)->set_null();
              eval_flags.set(i);
            } else if (OB_FAIL(cast_number(i))) {
              LOG_WARN("cast number failed", K(ret));
            } else {
              eval_flags.set(i);
            }
          }
        }
      } else {
        DISPATCH_WIDTH_TASK(int_bytes, DO_BATCH_TRANS);
      }
    }
  }
  return ret;
#undef DO_BATCH_TRANS
}

} // namespace sql
} // namespace oceanbase

#include "sql/engine/expr/ob_datum_decint_cast.h"

#undef CAST_FAIL
#undef EVAL_ARG
#undef EVAL_BATCH_ARGS
#undef DEF_BATCH_CAST_PARAMS
#undef DEF_BATCH_CAST_FUNC
#undef DO_EXPLICIT_CAST
#undef DO_IMPLICIT_CAST
#undef DO_CONST_CAST
