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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_ABS_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_ABS_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprAbs : public ObExprOperator
{
  typedef int (*abs_func)(common::ObObj &res, const common::ObObj &param, common::ObExprCtx &expr_ctx);
public:
  virtual int deserialize(const char *buf, const int64_t data_len, int64_t &pos);
public:
  explicit  ObExprAbs(common::ObIAllocator &alloc);
  ~ObExprAbs() {};

  virtual int calc_result_type1(ObExprResType &type, ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const;

  int set_func(common::ObObjType param_type);

  virtual int assign(const ObExprOperator &other);

  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                     const ObRawExpr &raw_expr,
                     ObExpr &rt_expr) const override;

private:
  int set_func_mysql(common::ObObjType param_type);
  int set_func_oracle(common::ObObjType param_type);
  //tinyint, mediumint, smallint, int32
  static int abs_int(common::ObObj &res,
                     const common::ObObj &param,
                     common::ObExprCtx &expr_ctx);
  //int64
  static int abs_int64(common::ObObj &res,
                       const common::ObObj &param,
                       common::ObExprCtx &expr_ctx);
  //utiniyint, umediumint, usmallint
  static int abs_uint(common::ObObj &res,
                     const common::ObObj &param,
                     common::ObExprCtx &expr_ctx);
  //uint32 uint64
  static int abs_uint32_uint64(common::ObObj &res,
                               const common::ObObj &param,
                               common::ObExprCtx &expr_ctx);
  //float
  static int abs_float(common::ObObj &res,
                       const common::ObObj &param,
                       common::ObExprCtx &expr_ctx);
  static int abs_float_double(common::ObObj &res,
                       const common::ObObj &param,
                       common::ObExprCtx &expr_ctx);
  //double
  static int abs_double(common::ObObj &res,
                     const common::ObObj &param,
                     common::ObExprCtx &expr_ctx);
  //ufloat
  static int abs_ufloat_udouble(common::ObObj &res,
                       const common::ObObj &param,
                       common::ObExprCtx &expr_ctx);
  //udouble
  static int abs_udouble(common::ObObj &res,
                     const common::ObObj &param,
                     common::ObExprCtx &expr_ctx);
  //number
  static int abs_number(common::ObObj &res,
                       const common::ObObj &param,
                       common::ObExprCtx &expr_ctx);
  //unumber
  static int abs_unumber(common::ObObj &res,
                        const common::ObObj &param,
                        common::ObExprCtx &expr_ctx);
  //null
  static int abs_null(common::ObObj &res,
                        const common::ObObj &param,
                        common::ObExprCtx &expr_ctx);
  //hexstring
  static int abs_hexstring(common::ObObj &res,
                      const common::ObObj &param,
                      common::ObExprCtx &expr_ctx);
  //year
  static int abs_year(common::ObObj &res,
                      const common::ObObj &param,
                      common::ObExprCtx &expr_ctx);
  //others(datetime, time, varchar,etc)
  static int abs_others_double(common::ObObj &res,
                      const common::ObObj &param,
                      common::ObExprCtx &expr_ctx);
  static int abs_others_number(common::ObObj &res,
                               const common::ObObj &param,
                               common::ObExprCtx &expr_ctx);
  //bit
  static int abs_bit(common::ObObj &res,
                     const common::ObObj &param,
                     common::ObExprCtx &expr_ctx);
  //bit
  static int abs_enum_set(common::ObObj &res,
                          const common::ObObj &param,
                          common::ObExprCtx &expr_ctx);
  //json
  static int abs_json(common::ObObj &res,
                      const common::ObObj &param,
                      common::ObExprCtx &expr_ctx);

  static common::ObObjType calc_param_type(const common::ObObjType orig_param_type,
                                          const bool is_oracle_mode);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprAbs);
private:
  abs_func func_;
};

} // namespace sql
} // namespace oceanbase
#endif // OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_ABS_
