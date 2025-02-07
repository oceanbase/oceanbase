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

#ifndef OCEANBASE_SQL_OB_EXPR_DATE_FORMAT_H_
#define OCEANBASE_SQL_OB_EXPR_DATE_FORMAT_H_

#include "lib/timezone/ob_time_convert.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/session/ob_sql_session_info.h"

#define MAX_FORMAT_LENGTH (128) // maybe enough
#define COMMON_PART_FORMAT_FUNC_ARG_DECL                                                           \
  ArgVec *arg_vec, int idx, int64_t tz_offset, YearType &year, MonthType &month, DateType &date,   \
    UsecType &usec, DateType &dt_yday, DateType &dt_mday, DateType &dt_wday, int32_t &hour,        \
    int32_t &minute, int32_t &sec, int32_t &fsec, ObTime &ob_time
#define FORMAT_FUNC_ARG_DECL                                                                       \
  YearType year, MonthType month, DateType dt_yday, DateType dt_mday, DateType dt_wday,            \
    int32_t hour, int32_t minute, int32_t sec, int32_t fsec, WeekType &week_sunday,                \
    WeekType &week_monday, int8_t &delta_sunday, int8_t &delta_monday, char *res_buf,              \
    int16_t &len, bool &res_null, bool no_null, const char *const *day_name,                       \
    const char *const *ab_day_name, const char *const *month_name,                                 \
    const char *const *ab_month_name

namespace oceanbase
{
namespace sql
{
class ObExprDateFormat : public ObStringExprOperator
{
public:
  explicit  ObExprDateFormat(common::ObIAllocator &alloc);
  virtual ~ObExprDateFormat();
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &date,
                                ObExprResType &format,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int calc_date_format(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int calc_date_format_invalid(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  DECLARE_SET_LOCAL_SESSION_VARS;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprDateFormat);

  static const int64_t OB_MAX_DATE_FORMAT_BUF_LEN = 1024;
  static void check_reset_status(common::ObExprCtx &expr_ctx, int &ret, common::ObObj &result);
public:
  static int calc_date_format_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound);
private:
  template <typename ArgVec, typename ResVec, typename IN_TYPE>
  static int vector_date_format(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound);
  static int get_day_month_names(ObString locale_name, const char *const *&day_name, const char *const *&ab_day_name,
                                 const char *const *&month_name, const char *const *&ab_month_name);
  static int analyze_format(const char *format_ptr, const char *end_ptr, int32_t &mode);
  static int get_from_format_Y(FORMAT_FUNC_ARG_DECL);
  static int get_from_format_y(FORMAT_FUNC_ARG_DECL);
  static int get_from_format_M(FORMAT_FUNC_ARG_DECL);
  static int get_from_format_m(FORMAT_FUNC_ARG_DECL);
  static int get_from_format_D(FORMAT_FUNC_ARG_DECL);
  static int get_from_format_d(FORMAT_FUNC_ARG_DECL);
  static int get_from_format_a(FORMAT_FUNC_ARG_DECL);
  static int get_from_format_b(FORMAT_FUNC_ARG_DECL);
  static int get_from_format_c(FORMAT_FUNC_ARG_DECL);
  static int get_from_format_e(FORMAT_FUNC_ARG_DECL);
  static int get_from_format_j(FORMAT_FUNC_ARG_DECL);
  static int get_from_format_W(FORMAT_FUNC_ARG_DECL);
  static int get_from_format_w(FORMAT_FUNC_ARG_DECL);
  static int get_from_format_U(FORMAT_FUNC_ARG_DECL);
  static int get_from_format_u(FORMAT_FUNC_ARG_DECL);
  static int get_from_format_X(FORMAT_FUNC_ARG_DECL);
  static int get_from_format_x(FORMAT_FUNC_ARG_DECL);
  static int get_from_format_V(FORMAT_FUNC_ARG_DECL);
  static int get_from_format_v(FORMAT_FUNC_ARG_DECL);
  static int get_from_format_f(FORMAT_FUNC_ARG_DECL);
  static int get_from_format_H(FORMAT_FUNC_ARG_DECL);
  static int get_from_format_h_I(FORMAT_FUNC_ARG_DECL);
  static int get_from_format_i(FORMAT_FUNC_ARG_DECL);
  static int get_from_format_k(FORMAT_FUNC_ARG_DECL);
  static int get_from_format_l(FORMAT_FUNC_ARG_DECL);
  static int get_from_format_p(FORMAT_FUNC_ARG_DECL);
  static int get_from_format_r(FORMAT_FUNC_ARG_DECL);
  static int get_from_format_S_s(FORMAT_FUNC_ARG_DECL);
  static int get_from_format_T(FORMAT_FUNC_ARG_DECL);
  template <typename ArgVec, typename IN_TYPE>
  static int calc_usec_only(COMMON_PART_FORMAT_FUNC_ARG_DECL);
  template <typename ArgVec, typename IN_TYPE>
  static int calc_year_only(COMMON_PART_FORMAT_FUNC_ARG_DECL);
  template <typename ArgVec, typename IN_TYPE>
  static int calc_yday_only(COMMON_PART_FORMAT_FUNC_ARG_DECL);
  template <typename ArgVec, typename IN_TYPE>
  static int calc_week_only(COMMON_PART_FORMAT_FUNC_ARG_DECL);
  template <typename ArgVec, typename IN_TYPE>
  static int calc_year_month(COMMON_PART_FORMAT_FUNC_ARG_DECL);
  template <typename ArgVec, typename IN_TYPE>
  static int calc_year_week(COMMON_PART_FORMAT_FUNC_ARG_DECL);
  template <typename ArgVec, typename IN_TYPE>
  static int calc_year_month_week(COMMON_PART_FORMAT_FUNC_ARG_DECL);
};

inline int ObExprDateFormat::calc_result_type2(ObExprResType &type,
                                               ObExprResType &date,
                                               ObExprResType &format,
                                               common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  UNUSED(date);
  UNUSED(format);
  int ret = common::OB_SUCCESS;
  type.set_varchar();
  type.set_collation_type(type_ctx.get_coll_type());
  type.set_collation_level(common::CS_LEVEL_COERCIBLE);
  type.set_length(DATETIME_MAX_LENGTH);

  //for enum or set obj, we need calc type
  if (ob_is_enum_or_set_type(date.get_type()) || ob_is_string_type(date.get_type())) {
    date.set_calc_type_default_varchar();
  } else if (ob_is_double_tc(date.get_type()) || ob_is_float_tc(date.get_type())) {
    date.set_calc_type(common::ObNumberType);
  }
  format.set_calc_type_default_varchar();
  return ret;
}


class ObExprGetFormat : public ObStringExprOperator
{
public:
  explicit  ObExprGetFormat(common::ObIAllocator &alloc);
  virtual ~ObExprGetFormat();
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &unit,
                                ObExprResType &format,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int calc_get_format(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);

private:
  enum Format
  {
    FORMAT_EUR = 0,
    FORMAT_INTERNAL = 1,
    FORMAT_ISO = 2,
    FORMAT_JIS = 3,
    FORMAT_USA = 4,
    FORMAT_MAX = 5,
  };
  // disallow copy
  static const int64_t GET_FORMAT_MAX_LENGTH = 17;
  static const char* FORMAT_STR[FORMAT_MAX];
  static const char* DATE_FORMAT[FORMAT_MAX + 1];
  static const char* TIME_FORMAT[FORMAT_MAX + 1];
  static const char* DATETIME_FORMAT[FORMAT_MAX + 1];
  DISALLOW_COPY_AND_ASSIGN(ObExprGetFormat);

};

} //sql
} //oceanbase

#endif //OCEANBASE_SQL_OB_EXPR_DATE_FORMAT_H_
