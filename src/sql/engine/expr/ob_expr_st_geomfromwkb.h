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
 * This file contains implementation for st_geomfromwkb.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_ST_GEOMFROMTWKB
#define OCEANBASE_SQL_OB_EXPR_ST_GEOMFROMTWKB
#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/geo/ob_geo_utils.h"
#include "lib/geo/ob_srs_info.h"

namespace oceanbase
{
namespace sql
{

class ObIExprSTGeomFromWKB : public ObFuncExprOperator
{
public:
  ObIExprSTGeomFromWKB(common::ObIAllocator &alloc, ObExprOperatorType type,
                      const char *name, int32_t param_num, ObValidForGeneratedColFlag valid_for_generated_col, int32_t dimension);
  virtual ~ObIExprSTGeomFromWKB() {}

  virtual int calc_result_typeN(ObExprResType& type,
                                ObExprResType* types_stack,
                                int64_t param_num,
                                common::ObExprTypeCtx& type_ctx) const override;
  int eval_geom_wkb(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override = 0;
  virtual const char *get_func_name() const = 0;

  int create_by_wkb_without_srid(common::ObIAllocator &allocator,
                                 const common::ObString &wkb,
                                 const common::ObSrsItem *srs_item,
                                 common::ObGeometry *&geo,
                                 common::ObGeoWkbByteOrder &bo) const;
  int get_type_bo_from_wkb_without_srid(const common::ObString &wkb,
                                        common::ObGeoType &type,
                                        common::ObGeoWkbByteOrder &bo) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObIExprSTGeomFromWKB);
};

class ObExprSTGeomFromWKB : public ObIExprSTGeomFromWKB
{
public:
  explicit ObExprSTGeomFromWKB(common::ObIAllocator &alloc);
  virtual ~ObExprSTGeomFromWKB() {}
  static int eval_st_geomfromwkb(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  const char *get_func_name() const override { return N_ST_GEOMFROMWKB; }
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprSTGeomFromWKB);
};

class ObExprSTGeometryFromWKB : public ObIExprSTGeomFromWKB
{
public:
  explicit ObExprSTGeometryFromWKB(common::ObIAllocator &alloc);
  virtual ~ObExprSTGeometryFromWKB() {}
  static int eval_st_geometryfromwkb(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  const char *get_func_name() const override { return N_ST_GEOMETRYFROMWKB; }

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprSTGeometryFromWKB);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_ST_GEOMFROMWKB