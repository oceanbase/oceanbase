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
  virtual int is_valid_for_generated_column(const ObRawExpr*expr, const common::ObIArray<ObRawExpr *> &exprs, bool &is_valid) const;
  DECLARE_SET_LOCAL_SESSION_VARS;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprDateFormat);

  static const int64_t OB_MAX_DATE_FORMAT_BUF_LEN = 1024;
  static void check_reset_status(common::ObExprCtx &expr_ctx, int &ret, common::ObObj &result);
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
