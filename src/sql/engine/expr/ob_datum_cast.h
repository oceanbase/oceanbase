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

#ifndef _OB_EXPR_DATUM_CAST_
#define _OB_EXPR_DATUM_CAST_

#include "common/object/ob_object.h"
#include "common/ob_zerofill_info.h"
#include "lib/timezone/ob_timezone_info.h"
#include "lib/timezone/ob_time_convert.h"
#include "sql/session/ob_sql_session_info.h"
#include "lib/charset/ob_charset.h"
#include "share/ob_errno.h"
#include "share/datum/ob_datum.h"
#include "share/object/ob_obj_cast.h"
#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

namespace oceanbase
{
namespace sql
{
class ObPhysicalPlanCtx;
struct ObUserLoggingCtx;

// extract accuracy info from %expr and call datum_accuracy_check() below.
int datum_accuracy_check(const ObExpr &expr,
                         const uint64_t cast_mode,
                         ObEvalCtx &ctx,
                         bool has_lob_header,
                         const common::ObDatum &in_datum,
                         ObDatum &res_datum,
                         int &warning);

// check if accuracy in %in_datum is ok. if ok, call res_datum.set_datum(in_datum).
// if not, will trunc data in %in_datum and put it in res_datum.
// this func makes sure data in %in_datum and %in_datum itself will not be changed.
int datum_accuracy_check(const ObExpr &expr,
                         const uint64_t cast_mode,
                         ObEvalCtx &ctx,
                         const common::ObAccuracy &acc,
                         bool has_lob_header,
                         const common::ObDatum &in_datum,
                         ObDatum &res_datum,
                         int &warning);

// 根据in_type,force_use_standard_format信息，获取fromat_str,优先从rt_expr保存的本地session变量列表获取，不存在则从session获取
int common_get_nls_format(const ObBasicSessionInfo *session,
                          ObEvalCtx &ctx,
                          const ObExpr *rt_expr,
                          const ObObjType in_type,
                          const bool force_use_standard_format,
                          ObString &format_str);

// 检查str以check_cs_type作为字符集是否合法
// strict_mode下，如果上述检查失败，返回错误码
// 否则返回以check_cs_type作为字符集的最长合法字符串
int string_collation_check(const bool is_strict_mode,
                           const common::ObCollationType check_cs_type,
                           const common::ObObjType str_type,
                           common::ObString &str);

// 将T中的值转换为ob_time结构
template<typename T>
int ob_datum_to_ob_time_with_date(const T &datum,
                                  const common::ObObjType type,
                                  const ObScale scale,
                                  const common::ObTimeZoneInfo* tz_info,
                                  common::ObTime& ob_time,
                                  const int64_t cur_ts_value,
                                  const ObDateSqlMode date_sql_mode,
                                  const bool has_lob_header)
{
  int ret = OB_SUCCESS;
  switch (ob_obj_type_class(type)) {
    case ObIntTC:
      // fallthrough.
    case ObUIntTC: {
      ret = ObTimeConverter::int_to_ob_time_with_date(datum.get_int(), ob_time, date_sql_mode);
      break;
    }
    case ObOTimestampTC: {
      if (ObTimestampTZType == type) {
        ret = ObTimeConverter::otimestamp_to_ob_time(type, datum.get_otimestamp_tz(),
                                                     tz_info, ob_time);
      } else {
        ret = ObTimeConverter::otimestamp_to_ob_time(type, datum.get_otimestamp_tiny(),
                                                     tz_info, ob_time);
      }
      break;
    }
    case ObDateTimeTC: {
      ret = ObTimeConverter::datetime_to_ob_time(datum.get_datetime(),
          (ObTimestampType == type) ? tz_info : NULL, ob_time);
      break;
    }
    case ObDateTC: {
      ret = ObTimeConverter::date_to_ob_time(datum.get_date(), ob_time);
      break;
    }
    case ObTimeTC: {
      int64_t dt_value = 0;
      if (OB_FAIL(ObTimeConverter::timestamp_to_datetime(cur_ts_value, tz_info, dt_value))) {
        SQL_ENG_LOG(WARN, "convert timestamp to datetime failed", K(ret));
      } else {
        const int64_t usec_per_day = 3600 * 24 * USECS_PER_SEC;
        // 将datetime的时间截取，只保留日期，然后转为微秒
        int64_t day_usecs = dt_value - dt_value % usec_per_day;
        ret = ObTimeConverter::datetime_to_ob_time(datum.get_time() + day_usecs, NULL, ob_time);
      }
      break;
    }
    case ObTextTC: // TODO@hanhui texttc share with the stringtc temporarily
    case ObStringTC: {
      ObScale res_scale = -1;
      ObArenaAllocator lob_allocator(ObModIds::OB_LOB_ACCESS_BUFFER,
                                     OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
      ObString str = datum.get_string();
      if (OB_FAIL(ObTextStringHelper::read_real_string_data(
              &lob_allocator, type, CS_TYPE_BINARY, has_lob_header, str))) {
        SQL_ENG_LOG(WARN, "fail to get real string data", K(ret), K(datum));
      } else {
        ret = ObTimeConverter::str_to_ob_time_with_date(str, ob_time, &res_scale, date_sql_mode);
      }
      break;
    }
    case ObDecimalIntTC:
    case ObNumberTC: {
      int64_t int_part = 0;
      int64_t dec_part = 0;
      number::ObNumber num;
      ObNumStackOnceAlloc tmp_alloc;
      if (ob_is_decimal_int(type)) {
        if (OB_FAIL(wide::to_number(datum.get_decimal_int(),
                                    datum.get_int_bytes(), scale, tmp_alloc,
                                    num))) {
          SQL_ENG_LOG(WARN, "failed to cast decimal int to number", K(ret));
        }
      } else {
        num = datum.get_number();
      }
      if (OB_FAIL(ret)) { // do nothing
      } else if (num.is_negative()) {
        ret = OB_INVALID_DATE_FORMAT;
        SQL_ENG_LOG(WARN, "invalid date format", K(ret), K(num));
      } else if (!num.is_int_parts_valid_int64(int_part, dec_part)) {
        ret = OB_INVALID_DATE_FORMAT;
        SQL_ENG_LOG(WARN, "invalid date format", K(ret), K(num));
      } else {
        ret = ObTimeConverter::int_to_ob_time_with_date(int_part, ob_time, date_sql_mode);
        if (OB_SUCC(ret)) {
          ob_time.parts_[DT_USEC] = (dec_part + 500) / 1000;
          ObTimeConverter::adjust_ob_time(ob_time);
        }
      }
      break;
    }
    default: {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "cast to time with date");
    }
  }
  SQL_ENG_LOG(DEBUG, "end ob_datum_to_ob_time_with_date", K(type),
              K(cur_ts_value), K(ob_time), K(ret));
  return ret;
}
template<typename T>
int ob_datum_to_ob_time_without_date(const T &datum,
                                    const common::ObObjType type,
                                    const ObScale scale,
                                    const common::ObTimeZoneInfo *tz_info,
                                    common::ObTime &ob_time,
                                    const bool has_lob_header)
{
  int ret = OB_SUCCESS;
  switch (ob_obj_type_class(type)) {
    case ObIntTC:
      // fallthrough.
    case ObUIntTC: {
      if (OB_FAIL(ObTimeConverter::int_to_ob_time_without_date(datum.get_int(), ob_time))) {
        SQL_ENG_LOG(WARN, "int to ob time without date failed", K(ret));
      } else {
        //mysql中intTC转time时，如果hour超过838，那么time应该为null，而不是最大值。
        const int64_t time_max_val = TIME_MAX_VAL;    // 838:59:59.
        int64_t value = ObTimeConverter::ob_time_to_time(ob_time);
        if (value > time_max_val) {
          ret = OB_INVALID_DATE_VALUE;
        }
      }
      break;
    }
    case ObOTimestampTC: {
      if (ObTimestampTZType == type) {
        ret = ObTimeConverter::otimestamp_to_ob_time(type, datum.get_otimestamp_tz(),
                                                     tz_info, ob_time);
      } else {
        ret = ObTimeConverter::otimestamp_to_ob_time(type, datum.get_otimestamp_tiny(),
                                                     tz_info, ob_time);
      }
      break;
    }
    case ObDateTimeTC: {
      ret = ObTimeConverter::datetime_to_ob_time(datum.get_datetime(),  (ObTimestampType == type)
                                                ? tz_info : NULL, ob_time);
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
    case ObTextTC: // TODO@hanhui texttc share with the stringtc temporarily
    case ObStringTC: {
      ObArenaAllocator lob_allocator(ObModIds::OB_LOB_ACCESS_BUFFER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
      ObString str = datum.get_string();
      if (OB_FAIL(ObTextStringHelper::read_real_string_data(
              &lob_allocator, type, CS_TYPE_BINARY, has_lob_header, str))) {
        SQL_ENG_LOG(WARN, "fail to get real string data", K(ret), K(datum));
      } else {
        ret = ObTimeConverter::str_to_ob_time_without_date(str, ob_time);
        if (OB_SUCC(ret)) {
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
    case ObDecimalIntTC:
    case ObNumberTC: {
      int64_t int_part = 0;
      int64_t dec_part = 0;
      ObNumStackOnceAlloc tmp_alloc;
      number::ObNumber num;
      if (ob_is_decimal_int(type)) {
        if (OB_FAIL(wide::to_number(datum.get_decimal_int(),
                                    datum.get_int_bytes(), scale, tmp_alloc,
                                    num))) {
          SQL_ENG_LOG(WARN, "failed to cast decimal int to number", K(ret));
        }
      } else {
        num = datum.get_number();
      }
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (!num.is_int_parts_valid_int64(int_part, dec_part)) {
        ret = OB_INVALID_DATE_FORMAT;
        SQL_ENG_LOG(WARN, "invalid date format", K(ret), K(num));
      } else {
        if (OB_FAIL(ObTimeConverter::int_to_ob_time_without_date(int_part, ob_time, dec_part))) {
          SQL_ENG_LOG(WARN, "int to ob time without date failed", K(ret));
        } else {
          if ((!ob_time.parts_[DT_YEAR]) && (!ob_time.parts_[DT_MON]) &&
              (!ob_time.parts_[DT_MDAY])) {
            // mysql中intTC转time时，如果超过838:59:59，那么time应该为null，而不是最大值。
            const int64_t time_max_val = TIME_MAX_VAL; // 838:59:59.
            int64_t value = ObTimeConverter::ob_time_to_time(ob_time);
            if (value > time_max_val) {
              ret = OB_INVALID_DATE_VALUE;
            }
          }
        }
      }
      break;
    }
    default: {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "cast to time without date");
    }
  }
  SQL_ENG_LOG(DEBUG, "end ob_datum_to_ob_time_without_date", K(type), K(ob_time), K(ret));
  return ret;
}
// 进行datetime到string的转换，除了ob_datum_cast.cpp需要使用，有的表达式也需要将结果
// 从datetime转为string, 例如ObExprTimeStampAdd
int common_datetime_string(const ObExpr &expr,
                           const common::ObObjType in_type,
                           const common::ObObjType out_type,
                           const common::ObScale in_scale,
                           bool force_use_std_nls_format,
                           const int64_t in_val, ObEvalCtx &ctx, char *buf,
                           int64_t buf_len, int64_t &out_len);
int padding_char_for_cast(int64_t padding_cnt, 
                          const common::ObCollationType &padding_cs_type,
                          common::ObIAllocator &alloc, 
                          common::ObString &padding_res);

int check_decimalint_accuracy(const ObCastMode cast_mode,
                              const ObDecimalInt *res_decint, const int32_t int_bytes,
                              const ObPrecision precision, const ObScale scale,
                              ObDecimalIntBuilder &res_val, int &warning);

inline bool decimal_int_truncated_check(const ObDecimalInt *decint, const int32_t int_bytes,
                                       const unsigned scale)
{
#define TRUNC_CHECK(int_type) \
  if (wide::ObDecimalIntConstValue::get_int_bytes_by_precision(scale) > int_bytes) {  \
    bret = (*reinterpret_cast<const int_type *>(decint)) != 0;              \
  } else {                                                                  \
    const int_type sf = get_scale_factor<int_type>(scale);                  \
    bret = (((*reinterpret_cast<const int_type *>(decint)) % sf) != 0);     \
  }

  int ret = OB_SUCCESS;
  bool bret = false;
  DISPATCH_WIDTH_TASK(int_bytes, TRUNC_CHECK);
  return bret;
#undef TRUNC_CHECK
}

void log_user_warning_truncated(const ObUserLoggingCtx *user_logging_ctx);

// copied from ob_obj_cast.cpp，函数逻辑没有修改，只是将输入参数从ObObj变为ObDatum
class ObDatumHexUtils
{
public:
static int hextoraw_string(const ObExpr &expr,
                    const common::ObString &in_str,
                    ObEvalCtx &ctx,
                    ObDatum &res_datum,
                    bool &has_set_res);
static int hextoraw(const ObExpr &expr, const common::ObDatum &in,
                    const ObDatumMeta &in_datum_meta,
                    ObEvalCtx &ctx, ObDatum &res);
static int get_uint(const common::ObObjType &in_type,
                    const int8_t scale,
                    const common::ObDatum &in,
                    common::ObIAllocator &alloc, common::number::ObNumber &out);
static int uint_to_raw(const common::number::ObNumber &uint_num, const ObExpr &expr,
                           ObEvalCtx &ctx, ObDatum &res_datum);
static int unhex(const ObExpr &expr,
                 const common::ObString &in_str,
                 ObEvalCtx &ctx,
                 ObDatum &res_datum,
                 bool &has_set_res);
static int rawtohex(const ObExpr &expr,
                    const common::ObString &in_str,
                    ObEvalCtx &ctx,
                    ObDatum &res_datum);
static int hex(const ObExpr &expr,
               const common::ObString &in_str,
               ObEvalCtx &ctx,
               common::ObIAllocator &calc_alloc,
               ObDatum &res_datum,
               bool upper_case = true);
};

class ObDatumCast 
{
public:
  static int get_implicit_cast_function(const common::ObObjType in_type,
                                        const common::ObCollationType in_cs_type,
                                        const common::ObObjType out_type,
                                        const common::ObCollationType out_cs_type,
                                        const int64_t cast_mode,
                                        ObExpr::EvalFunc &eval_func);
  // 根据in_type/out_type等信息，获取cast func
  static int choose_cast_function(const common::ObObjType in_type,
                                  const common::ObCollationType in_cs_type,
                                  const common::ObObjType out_type,
                                  const common::ObCollationType out_cs_type,
                                  const int64_t cast_mode,
                                  common::ObIAllocator &allocator,
                                  bool &just_eval_arg,
                                  ObExpr &rt_expr);
  static int get_enumset_cast_function(const common::ObObjTypeClass in_tc,
                                       const common::ObObjType out_type,
                                       ObExpr::EvalEnumSetFunc &eval_func);
  // 检查转换是否合法，有些检查是没办法反映在转换矩阵里面
  // 举例：string/text -> string/text时，如果是nonblob -> blob，
  // 要求nonblob必须是char/varchar类型，这种检查在转换矩阵中是没法反映出来的
  // 这些检查都会在该方法中进行
  static int check_can_cast(const common::ObObjType in_type,
                            const common::ObCollationType in_cs_type,
                            const common::ObObjType out_type,
                            const common::ObCollationType out_cs_type);
  // 有些cast是什么事情都不用做的，例如int->bit，直接调用cast_eval_arg()计算参数的值即可
  // CG阶段会使用该方法判断是否可以不用给cast表达式分配结果空间，直接指向参数的结果即可
  static int is_trivial_cast(const common::ObObjType in_type,
                                const common::ObCollationType in_cs_type,
                                const common::ObObjType out_type,
                                const common::ObCollationType out_cs_type,
                                const common::ObCastMode &cast_mode,
                                bool &just_eval_arg);

  // 功能同:
  //    EXPR_DEFINE_CAST_CTX(expr_ctx, cast_mode)
  //    EXPR_CAST_OBJ_V2(obj_type, obj, res_obj)
  static int cast_obj(ObEvalCtx &ctx, common::ObIAllocator &alloc,
                      const common::ObObjType &dst_type,
                      common::ObAccuracy &dst_acc,
                      const common::ObCollationType &dst_cs_type,
                      const common::ObObj &src_obj,
                      common::ObObj &dst_obj);

  static bool is_implicit_cast(const ObExpr &expr)
  {
    bool bret = false;
    if (T_FUN_SYS_CAST == expr.type_ && CM_IS_IMPLICIT_CAST(expr.extra_)) {
      bret = true;
    }
    return bret;
  }

  static bool is_explicit_cast(const ObExpr &expr)
  {
    bool bret = false;
    if (T_FUN_SYS_CAST == expr.type_ && CM_IS_EXPLICIT_CAST(expr.extra_)) {
      bret = true;
    }
    return bret;
  }

  inline static bool need_scale_decimalint(const ObScale in_scale,
                                           const ObPrecision in_precision,
                                           const ObScale out_scale,
                                           const ObPrecision out_precision) {
    bool ret = false;
    if (in_scale != out_scale) {
      ret = true;
    } else if (get_decimalint_type(in_precision) !=
               get_decimalint_type(out_precision)) {
      ret = true;
    }
    return ret;
  }

  inline static bool need_scale_decimalint(const ObScale in_scale,
                                           const int32_t in_bytes,
                                           const ObScale out_scale,
                                           const int32_t out_bytes) {
    bool ret = false;
    if (in_scale != out_scale) {
      ret = true;
    } else if (in_bytes != out_bytes) {
      ret = true;
    }
    return ret;
  }

  static int common_scale_decimalint(const ObDecimalInt *decint, const int32_t int_bytes,
                                     const ObScale in_scale, const ObScale out_scale,
                                     const ObPrecision out_prec,
                                     const ObCastMode cast_mode, ObDecimalIntBuilder &val,
                                     const ObUserLoggingCtx *column_conv_ctx = NULL);

  static int align_decint_precision_unsafe(const ObDecimalInt *decint, const int32_t int_bytes,
                                           const int32_t expected_int_bytes,
                                           ObDecimalIntBuilder &res);
  static void get_decint_cast(ObObjTypeClass in_tc, ObPrecision in_prec, ObScale in_scale,
                              ObPrecision out_prec, ObScale out_scale, bool is_explicit,
                              ObExpr::EvalBatchFunc &batch_cast_func, ObExpr::EvalFunc &cast_func);
};

class ObDatumCaster {
public:
  ObDatumCaster()
    : inited_(false),
      eval_ctx_(NULL),
      cast_expr_(NULL),
      extra_cast_expr_(NULL) {}
  ~ObDatumCaster() {}

  // init eval_ctx_/cast_expr_/extra_cast_expr_/frame. all mem comes from ObExecContext.
  // frame layout:
  // ObDatum | ObDatum | ObDynReserveBuf | res_buf | ObDynReserveBuf | res_buf
  // res_buf_len is 128
  int init(ObExecContext &ctx);

  // same with ObObjCaster::to_type().
  // input is ObExpr, and output is ObDatum, it's better if input is also ObDatum.
  // we will do this later if necessary.
  // subschema id from sql udt type is a combination of (cs_type_ and cs_level_),
  // datum meta does not have cs_level_, or we can use ObDatum precision_ as subschema id?
  int to_type(const ObDatumMeta &dst_type,
              const ObExpr &src_expr,
              const common::ObCastMode &cm,
              common::ObDatum *&res,
              int64_t batch_idx = 0,
              const uint16_t subschema_id = 0);
  // for xxx -> enumset.
  int to_type(const ObDatumMeta &dst_type,
              const common::ObIArray<common::ObString> &str_values,
              const ObExpr &src_expr,
              const common::ObCastMode &cm,
              common::ObDatum *&res,
              int64_t batch_idx = 0);

  int destroy();
private:
  DISALLOW_COPY_AND_ASSIGN(ObDatumCaster);

  // setup following data member of ObExpr:
  // datum_meta_, obj_meta_, obj_datum_map_, eval_func_,
  // args_, arg_cnt_, parents_, parent_cnt_, basic_funcs_.
  int setup_cast_expr(const ObDatumMeta &dst_type,
                      const ObExpr &src_expr,
                      const common::ObCastMode cm,
                      ObExpr &cast_expr,
                      const uint16_t subschema_id = 0);
  bool inited_;
  ObEvalCtx *eval_ctx_;
  ObExpr *cast_expr_;
  ObExpr *extra_cast_expr_;
};

struct ObBatchCast
{
  using batch_func_ = int (*)(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip,
                              const int64_t batch_size);
  static batch_func_ get_explicit_cast_func(const ObObjTypeClass tc1, const ObObjTypeClass tc2);

  static batch_func_ get_implicit_cast_func(const ObObjTypeClass tc1, const ObObjTypeClass tc2);

  template <ObObjTypeClass in_tc, ObObjTypeClass out_tc>
  static int explicit_batch_cast(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip,
                                 const int64_t batch_size);

  template <ObObjTypeClass in_tc, ObObjTypeClass out_tc>
  static int implicit_batch_cast(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip,
                                 const int64_t batch_size);

private:
  template<ObObjTypeClass tc1, ObObjTypeClass tc2>
  friend struct init_batch_func;
  static batch_func_ implicit_batch_funcs_[ObMaxTC][ObMaxTC];
  static batch_func_ explicit_batch_funcs_[ObMaxTC][ObMaxTC];
};

struct QuestionmarkDynEvalInfo
{
  union
  {
    struct
    {
      int16_t in_scale_;
      int16_t in_precision_;
      int32_t param_idx_;
    };
    int64_t extra_;
  };
  QuestionmarkDynEvalInfo() : extra_(0){};
  QuestionmarkDynEvalInfo(int64_t extra): extra_(extra) {}
  explicit operator int64_t()
  {
    return extra_;
  }
};

} // namespace sql
} // namespace oceanbase

#endif // _OB_EXPR_DATUM_CAST_
