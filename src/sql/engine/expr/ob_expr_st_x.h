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
 * This file contains implementation for st_x, st_y, st_latitude, st_longtitude.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_ST_X
#define OCEANBASE_SQL_OB_EXPR_ST_X

#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/geo/ob_geo_ibin.h"
#include "lib/geo/ob_srs_info.h"

namespace oceanbase
{
namespace sql
{
// common superclass of st_x, s_y, st_longitude and st_latitude
class ObExprSTCoordinate: public ObExprOperator
{
public:
  ObExprSTCoordinate(common::ObIAllocator &alloc,
                     ObExprOperatorType type,
                     const char *name)
    :ObExprOperator(alloc, type, name, ONE_OR_TWO, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION) { }
  virtual ~ObExprSTCoordinate() {}
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *texts,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;
  int calc_common(common::ObObj &result,
                  const common::ObObj *params,
                  int64_t param_num,
                  common::ObExprCtx &expr_ctx,
                  bool is_first_d,
                  bool only_geog) const;

  static int eval_common(const ObExpr &expr,
                         ObEvalCtx &ctx,
                         ObDatum &expr_datum,
                         bool is_first_d,
                         bool only_geog,
                         const char *func_name);

  static int check_longitude(double new_val_radian,
                             const common::ObSrsItem *srs,
                             double new_val,
                             const char *func_name);
  static int check_latitude(double new_val_radian,
                            const common::ObSrsItem *srs,
                            double new_val,
                            const char *func_name);
};

class ObExprSTX : public ObExprSTCoordinate
{
public:
  explicit ObExprSTX(common::ObIAllocator &alloc)
    : ObExprSTCoordinate(alloc, T_FUN_SYS_ST_X, N_ST_X) {}
  virtual ~ObExprSTX() {}
  static int eval_st_x(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  virtual bool need_rt_ctx() const override { return true; }
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprSTX);
};

class ObExprSTY : public ObExprSTCoordinate
{
public:
  explicit ObExprSTY(common::ObIAllocator &alloc)
    : ObExprSTCoordinate(alloc, T_FUN_SYS_ST_Y, N_ST_Y) {}
  virtual ~ObExprSTY() {}
  static int eval_st_y(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  virtual bool need_rt_ctx() const override { return true; }
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprSTY);
};

class ObExprSTLatitude : public ObExprSTCoordinate
{
public:
  explicit ObExprSTLatitude(common::ObIAllocator &alloc)
    : ObExprSTCoordinate(alloc, T_FUN_SYS_ST_LATITUDE, N_ST_LATITUDE) {}
  virtual ~ObExprSTLatitude() {}
  static int eval_st_latitude(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  virtual bool need_rt_ctx() const override { return true; }
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprSTLatitude);
};

class ObExprSTLongitude : public ObExprSTCoordinate
{
public:
  explicit ObExprSTLongitude(common::ObIAllocator &alloc)
    : ObExprSTCoordinate(alloc, T_FUN_SYS_ST_LONGITUDE, N_ST_LONGITUDE) {}
  virtual ~ObExprSTLongitude() {}
  static int eval_st_longitude(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  virtual bool need_rt_ctx() const override { return true; }
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprSTLongitude);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_ST_X