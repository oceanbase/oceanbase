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

#ifndef _OB_EXPR_ALIGN_DATE4CMP_H_
#define _OB_EXPR_ALIGN_DATE4CMP_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
// This expression is used to handle issues encountered when comparing datetime or date type with other types:
// the other type will be converted to an illegal date, resulting in a comparison result that is incompatible with MySQL.
// such as:
// create table t1(c1 int primary key, c2 date);
// insert into t1 values(1, '1998-11-30');
// insert into t1 values(2, '1998-12-01');
// select * from t1 where c2 > '1998-11-31';
// This expression is used to convert illegal date into logical and valid date.
// such as: 1998-11-31 -> 1998-11-30/1998-12-01 depend on compare type.
class ObExprAlignDate4Cmp : public ObFuncExprOperator
{
public:
  explicit ObExprAlignDate4Cmp(common::ObIAllocator &alloc);
  virtual ~ObExprAlignDate4Cmp() {};

  virtual int calc_result_type3(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                ObExprResType &type3,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const override;

  static int eval_align_date4cmp(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);

  static bool is_align_date4cmp_support_obj_type(const ObObjType& obj_type);

  enum DateArgType
  {
    VALID_DATE = 1,
    INVALID_DATE = 2,
    NON_DATE = 3,
    NULL_DATE = 4,
    ZERO_DATE = 5,
  };

private:
  //disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprAlignDate4Cmp);

  static bool is_day_over_limit(const ObTime &ob_time);
  static int32_t get_day_over_limit(const ObTime &ob_time);
  static void set_valid_time_floor(ObTime &ob_time);
  static void push_back_n_days(ObTime &ob_time, int32_t offset);
  static DateArgType validate_time(ObTime &ob_time);
  static int integer_to_ob_time(const int64_t &date, DateArgType &date_arg_type, ObTime &ob_time);
  static int double_to_ob_time(const double &date, DateArgType &date_arg_type, ObTime &ob_time);
  static int number_to_ob_time(const number::ObNumber &date, DateArgType &date_arg_type, ObTime &ob_time);
  static int str_to_ob_time(const ObString &date, DateArgType &date_arg_type, ObTime &ob_time);
  static int datum_to_ob_time(const ObExpr &expr,
                              const ObDatum *date_datum,
                              const ObObjType &date_arg_obj_type,
                              DateArgType &date_arg_type,
                              ObTime &ob_time);
  static bool is_zero_time(ObTime &ob_time);
  static int set_res(ObDatum &res, ObTime &ob_time,
                     const ObObjType &res_type,
                     const bool is_valid_time,
                     const bool offset,
                     const bool is_zero_on_warn,
                     const bool is_no_zero_date,
                     const bool is_warn_on_fail);
  static int set_zero_res(ObDatum &res, ObTime &ob_time,
                          const ObObjType &res_type,
                          bool is_no_zero_date);
};
}
}
#endif  /* _OB_EXPR_ALIGN_DATE4CMP_H_ */
