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

#ifndef _OB_ENGINE_EXPR_EXPR_UTIL_H_
#define _OB_ENGINE_EXPR_EXPR_UTIL_H_

#include "lib/number/ob_number_v2.h"
#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{

//
// temporary allocator for number operation. e.g.:
//  ObNumStackAllocator num_alloc;
//  number::ObNumber num;
//  num.from(int_val, num_alloc);
//  expr_datum.set_number(num); // set_datum will deep copy num value.
//
template <int64_t NUM_CNT = 1>
class ObNumStackAllocator : public common::ObDataBuffer
{
public:
  ObNumStackAllocator() : ObDataBuffer(local_buf_, sizeof(local_buf_))
  {
  }

private:
  char local_buf_[NUM_CNT * common::number::ObNumber::MAX_BYTE_LEN];
};
typedef ObNumStackAllocator<1> ObNumStackOnceAlloc;

#define array_elements(A) ((uint) (sizeof(A)/sizeof(A[0])))

class ObExprUtil
{
public:
  static int get_trunc_int64(const common::ObObj &obj, common::ObExprCtx &expr_ctx, int64_t &out);
  static int get_round_int64(const common::ObObj &obj, common::ObExprCtx &expr_ctx, int64_t &out);

  static int get_trunc_int64(common::number::ObNumber &nmb, common::ObExprCtx &expr_ctx, int64_t &out);
  static int get_round_int64(common::number::ObNumber &nmb, common::ObExprCtx &expr_ctx, int64_t &out);

  // This function relies on `kmp_next` to do the calculation of next array
  static int kmp(const char *pattern,
                 const int64_t pattern_len,
                 const char *text,
                 const int64_t text_len,
                 const int64_t nth_appearance,
                 const int32_t *next, /* calculated, size same with pattern */
                 int64_t &result);
  static int kmp_next(const char *pattern, const int64_t pattern_len, int32_t *next);

  // This function relies on `kmp_next_reverse` to do the calculation of next array
  static int kmp_reverse(const char *pattern,
                         const int64_t pattern_len,
                         const char *text,
                         const int64_t text_len,
                         const int64_t nth_appearance,
                         const int32_t *next, /* calculated, size same with pattern */
                         int64_t &result);
  static int kmp_next_reverse(const char *pattern,
                              const int64_t pattern_len,
                              int32_t *next);

  static int get_mb_str_info(const common::ObString &str,
                             common::ObCollationType cs_type,
                             common::ObIArray<size_t> &byte_num,
                             common::ObIArray<size_t> &byte_offset);
  // 将double round到小数点后或者小数点前指定位置
  static double round_double(double val, int64_t dec);
  static uint64_t round_uint64(uint64_t val, int64_t dec);
  // 将double trunc到小数点后或者小数点前指定位置
  static double trunc_double(double val, int64_t dec);
  template <typename T>
  static T trunc_integer(T val, int64_t dec);

  // truncate the decimal part, truncate to INT64_MAX/INT64_MIN too if out of range.
  static int trunc_num2int64(const common::number::ObNumber &nmb, int64_t &v);

  static int trunc_decint2int64(const ObDecimalInt *decint, const int32_t int_bytes, const ObScale scale, int64_t &v);
  // get number out of ObDatum and truncate to int64
  static int trunc_num2int64(const common::ObDatum &datum, int64_t &v)
  {
    return trunc_num2int64(common::number::ObNumber(datum.get_number()), v);
  }

  static int round_decint2int64(const ObDecimalInt *decint, const int32_t int_bytes, const ObScale scale, int64_t &v);
  // round the decimal part, round to INT64_MAX/INT64_MIN too if out of range.
  static int round_num2int64(const common::number::ObNumber &nmb, int64_t &v);
  
  // get number out of ObDatum and round to int64
  static int round_num2int64(const common::ObDatum &datum, int64_t &v)
  {
    return round_num2int64(common::number::ObNumber(datum.get_number()), v);
  }
  // Get integer value from integer parameter which type is ObIntType in mysql
  // or ObNumberType in oracle.
  //
  // Keep %int_val unchanged if datum is NULL or datum->is_null().
  static int get_int_param_val(common::ObDatum *datum, bool is_decint, int64_t &int_val);

  // Set the ASCII string to expression result.
  // e.g.:
  //   dump() need result is NLS_CHARACTERSET, but we can only generate ASCII string in code,
  //   this function is used to convert the result characterset.
  static int set_expr_ascii_result(const ObExpr &expr,
                                ObEvalCtx &ctx,
                                common::ObDatum &expr_datum,
                                const common::ObString &ascii_str,
                                const int64_t datum_idx,
                                const bool is_ascii = true,
                                const common::ObCollationType src_coll_type = CS_TYPE_UTF8MB4_BIN);
  static int set_expr_ascii_result(const ObExpr &expr,
                                ObEvalCtx &ctx,
                                common::ObDatum &expr_datum,
                                const common::ObString &ascii_str,
                                const bool is_ascii = true,
                                const common::ObCollationType src_coll_type = CS_TYPE_UTF8MB4_BIN);
  static int convert_string_collation(const common::ObString &in_str,
                                      const common::ObCollationType &in_collation,
                                      common::ObString &out_str,
                                      const common::ObCollationType &out_collation,
                                      common::ObIAllocator &alloc);
  static int need_convert_string_collation(const common::ObCollationType &in_collation,
                                           const common::ObCollationType &out_collation,
                                           bool &need_convert);
  // deep copy src to out.
  // it is ok if src and out is same. like this deep_copy_str(str1, str1, alloc)
  static int deep_copy_str(const common::ObString &src, common::ObString &out,
                           common::ObIAllocator &alloc);

  static int eval_stack_overflow_check(const ObExpr &rt_expr,
                                       ObEvalCtx &ctx,
                                       ObDatum &expr_datum);

  static int eval_batch_stack_overflow_check(const ObExpr &expr,
                                             ObEvalCtx &ctx,
                                             const ObBitVector &skip,
                                             const int64_t batch_size);

  static int convert_utf8_charset(common::ObIAllocator& allocator,
                                  const common::ObCollationType& from_collation,
                                  const common::ObString &from_string,
                                  common::ObString &dest_string);

private:
  static int get_int64_from_num(common::number::ObNumber &nmb,
                                common::ObExprCtx &expr_ctx,
                                const bool is_trunc, //true: trunc; false: round
                                int64_t &out);
  static int get_int64_from_obj(const common::ObObj &obj,
                                common::ObExprCtx &expr_ctx,
                                const bool is_trunc, //true: trunc; false: round
                                int64_t &out);
  DISALLOW_COPY_AND_ASSIGN(ObExprUtil);
};

// make sure T is int/uint type
template <typename T>
T ObExprUtil::trunc_integer(T val, int64_t dec)
{
  volatile T res = 0;
  if (dec >= 0) {
    res = val;
  } else {
    const int64_t max_integer_desc = 19;
    if (std::abs(dec) > max_integer_desc) {
      res = 0;
    } else {
      const T pow_val = std::pow(10, static_cast<int64_t>(std::abs(dec)));
      res = (val / pow_val) * pow_val;
    }
  }
  return res;
}

// 定义三角函数的计算函数
// eg: sin/cos/tan sinh/cosh/tanh asin/acos/atan
// atan2的计算函数单独实现
#define DEF_CALC_TRIGONOMETRIC_EXPR(tritype, INVALID_DOUBLE_ARG_CHECK, INVALID_DOUBLE_ARG_ERRNO) \
int calc_##tritype##_expr(const ObExpr &expr, ObEvalCtx &ctx,                 \
                               ObDatum &res_datum)                        \
{                                                                             \
  int ret = OB_SUCCESS;                                                       \
  ObDatum *radian = NULL;                                                     \
  if (OB_FAIL(expr.args_[0]->eval(ctx, radian))) {                            \
    LOG_WARN("eval radian arg failed", K(ret), K(expr));                      \
  } else if (radian->is_null()) {                                             \
    /* radian is already be cast to number type, no need to is_null_oracle */ \
    res_datum.set_null();                                                     \
  } else if (ObNumberType == expr.args_[0]->datum_meta_.type_) {              \
    number::ObNumber res_nmb;                                                 \
    number::ObNumber radian_nmb(radian->get_number());                        \
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);                                          \
    if (OB_FAIL(radian_nmb.tritype(res_nmb, alloc_guard.get_allocator()))) {    \
      LOG_WARN("calc expr failed", K(ret), K(radian_nmb), K(expr));           \
    } else {                                                                  \
      res_datum.set_number(res_nmb);                                          \
    }                                                                         \
  } else if (ObDoubleType == expr.args_[0]->datum_meta_.type_) {              \
    const double arg = radian->get_double();                                  \
    if (INVALID_DOUBLE_ARG_CHECK) {                                           \
      if(lib::is_oracle_mode()) {                                           \
        ret = INVALID_DOUBLE_ARG_ERRNO;                                       \
      } else {                                                                \
        res_datum.set_null();                                                 \
      }                                                                       \
    } else {                                                                  \
      res_datum.set_double(tritype(arg));                                     \
    }                                                                         \
  } else {                                                                    \
    ret = OB_ERR_UNEXPECTED;                                                  \
  }                                                                           \
  return ret;                                                                 \
}

class ObSolidifiedVarsContext
{
public:
  ObSolidifiedVarsContext() :
    local_session_var_(NULL),
    alloc_(NULL),
    local_tz_wrap_(NULL)
  {
  }
  ObSolidifiedVarsContext(share::schema::ObLocalSessionVar *local_var, common::ObIAllocator *alloc) :
    local_session_var_(local_var),
    alloc_(alloc),
    local_tz_wrap_(NULL)
  {
  }
  virtual ~ObSolidifiedVarsContext()
  {
    if (NULL != local_tz_wrap_ && NULL != alloc_) {
      local_tz_wrap_->~ObTimeZoneInfoWrap();
      alloc_->free(local_tz_wrap_);
      local_tz_wrap_ = NULL;
    }
  }
  int get_local_tz_info(const sql::ObBasicSessionInfo *session, const common::ObTimeZoneInfo *&tz_info);
  share::schema::ObLocalSessionVar *get_local_vars() const { return local_session_var_; }
  DECLARE_TO_STRING;
private:
  share::schema::ObLocalSessionVar *local_session_var_;
  common::ObIAllocator *alloc_;
  //cached vars
  ObTimeZoneInfoWrap *local_tz_wrap_;
};

//Get the merged values of solidified vars and current session vars
//If a var is solidified, return the solidified value. Otherwise return currrent session value.
//e.g. :
// ObSolidifiedVarsGetter helper(expr, ctx, session);
// ObString format;
// helper.get_local_nls_date_format(format);
class ObSolidifiedVarsGetter
{
public:
  ObSolidifiedVarsGetter(const ObExpr &expr, const ObEvalCtx &ctx, const ObBasicSessionInfo *session);
  int get_dtc_params(ObDataTypeCastParams &dtc_params);
  int get_time_zone_info(const common::ObTimeZoneInfo *&tz_info);
  int get_sql_mode(ObSQLMode &sql_mode);
  int get_local_nls_date_format(ObString &format);
  int get_local_nls_timestamp_format(ObString &format);
  int get_local_nls_timestamp_tz_format(ObString &format);
  int get_local_nls_format_by_type(const ObObjType type, ObString &format_str);
  int get_max_allowed_packet(int64_t &max_size);
  int get_compat_version(uint64_t &compat_version);
  //get the specified solidified var
  int get_local_var(share::ObSysVarClassType var_type, share::schema::ObSessionSysVar *&sys_var);
private:
  const ObSolidifiedVarsContext *local_session_var_;
  const ObBasicSessionInfo *session_;
};

}
}
#endif  /* _OB_ENGINE_EXPR_EXPR_UTIL_H_ */
