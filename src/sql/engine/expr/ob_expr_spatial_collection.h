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
 * This file contains implementation for spatial collection expr.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_SPATIAL_COLLECTION_
#define OCEANBASE_SQL_OB_EXPR_SPATIAL_COLLECTION_

#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/geo/ob_geo_utils.h"

namespace oceanbase
{
namespace sql
{
class ObExprSpatialCollection : public ObFuncExprOperator
{
public:
  static const uint32_t POLYGON_RING_POINT_NUM_AT_LEAST = 4;
  explicit ObExprSpatialCollection(common::ObIAllocator &alloc,
                                   ObExprOperatorType type,
                                   const char *name,
                                   int32_t param_num,
                                   int32_t dimension);
  virtual ~ObExprSpatialCollection();
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types_stack,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const override;
  virtual int calc_resultN(common::ObObj &result,
                           const common::ObObj *objs,
                           int64_t param_num,
                           common::ObExprCtx &expr_ctx) const override;
  int eval_spatial_collection(const ObExpr &expr,
                              ObEvalCtx &ctx,
                              ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override = 0;
  virtual const char *get_func_name() const = 0;
  virtual common::ObGeoType get_geo_type() const = 0;
  virtual common::ObGeoType expect_sub_type() const = 0;
private:
  int calc_multi(const common::ObString &wkb_point,
                 common::ObWkbBuffer &res_wkb_buf) const;
  int calc_linestring(const common::ObString &sub,
                      common::ObWkbBuffer &res_wkb_buf) const;
  int calc_polygon(const common::ObString wkb_linestring,
                   common::ObWkbBuffer &res_wkb_buf) const;
  DISALLOW_COPY_AND_ASSIGN(ObExprSpatialCollection);
};

class ObExprLineString : public ObExprSpatialCollection
{
public:
  explicit ObExprLineString(common::ObIAllocator &alloc);
  virtual ~ObExprLineString();
  static int eval_linestring(const ObExpr &expr,
                             ObEvalCtx &ctx,
                             ObDatum &res);
  int cg_expr(ObExprCGCtx &expr_cg_ctx,
              const ObRawExpr &raw_expr,
              ObExpr &rt_expr) const override;
  const char *get_func_name() const override { return N_LINESTRING; }
  common::ObGeoType get_geo_type() const override { return common::ObGeoType::LINESTRING; }
  common::ObGeoType expect_sub_type() const override { return common::ObGeoType::POINT; }
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprLineString);
};

class ObExprPolygon : public ObExprSpatialCollection
{
public:
  explicit ObExprPolygon(common::ObIAllocator &alloc);
  virtual ~ObExprPolygon();
  static int eval_polygon(const ObExpr &expr,
                          ObEvalCtx &ctx,
                          ObDatum &res);
  int cg_expr(ObExprCGCtx &expr_cg_ctx,
              const ObRawExpr &raw_expr,
              ObExpr &rt_expr) const override;
  const char *get_func_name() const override { return N_POLYGON; }
  common::ObGeoType get_geo_type() const override { return common::ObGeoType::POLYGON; }
  common::ObGeoType expect_sub_type() const override { return common::ObGeoType::LINESTRING; }
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprPolygon);
};

class ObExprMultiPoint : public ObExprSpatialCollection
{
public:
  explicit ObExprMultiPoint(common::ObIAllocator &alloc);
  virtual ~ObExprMultiPoint();
  static int eval_multipoint(const ObExpr &expr,
                             ObEvalCtx &ctx,
                             ObDatum &res);
  int cg_expr(ObExprCGCtx &expr_cg_ctx,
              const ObRawExpr &raw_expr,
              ObExpr &rt_expr) const override;
  const char *get_func_name() const override { return N_MULTIPOINT; }
  common::ObGeoType get_geo_type() const override { return common::ObGeoType::MULTIPOINT; }
  common::ObGeoType expect_sub_type() const override { return common::ObGeoType::POINT; }
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprMultiPoint);
};

class ObExprMultiLineString : public ObExprSpatialCollection
{
public:
  explicit ObExprMultiLineString(common::ObIAllocator &alloc);
  static int eval_multilinestring(const ObExpr &expr,
                                  ObEvalCtx &ctx,
                                  ObDatum &res);
  virtual ~ObExprMultiLineString();
  int cg_expr(ObExprCGCtx &expr_cg_ctx,
              const ObRawExpr &raw_expr,
              ObExpr &rt_expr) const override;
  const char *get_func_name() const override { return N_MULTILINESTRING; }
  common::ObGeoType get_geo_type() const override { return common::ObGeoType::MULTILINESTRING; }
  common::ObGeoType expect_sub_type() const override { return common::ObGeoType::LINESTRING; }
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprMultiLineString);
};

class ObExprMultiPolygon : public ObExprSpatialCollection
{
public:
  explicit ObExprMultiPolygon(common::ObIAllocator &alloc);
  virtual ~ObExprMultiPolygon();
  static int eval_multipolygon(const ObExpr &expr,
                               ObEvalCtx &ctx,
                               ObDatum &res);
  int cg_expr(ObExprCGCtx &expr_cg_ctx,
              const ObRawExpr &raw_expr,
              ObExpr &rt_expr) const override;
  const char *get_func_name() const override { return N_MULTIPOLYGON; }
  common::ObGeoType get_geo_type() const override { return common::ObGeoType::MULTIPOLYGON; }
  common::ObGeoType expect_sub_type() const override { return common::ObGeoType::POLYGON; }
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprMultiPolygon);
};

class ObExprGeomCollection : public ObExprSpatialCollection
{
public:
  explicit ObExprGeomCollection(common::ObIAllocator &alloc);
  virtual ~ObExprGeomCollection();
  static int eval_geomcollection(const ObExpr &expr,
                                 ObEvalCtx &ctx,
                                 ObDatum &res);
  int cg_expr(ObExprCGCtx &expr_cg_ctx,
              const ObRawExpr &raw_expr,
              ObExpr &rt_expr) const override;
  const char *get_func_name() const override { return N_GEOMCOLLECTION; }
  common::ObGeoType get_geo_type() const override { return common::ObGeoType::GEOMETRYCOLLECTION; }
  common::ObGeoType expect_sub_type() const override { return common::ObGeoType::GEOTYPEMAX; }

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprGeomCollection);
};

class ObExprGeometryCollection : public ObExprSpatialCollection
{
public:
  explicit ObExprGeometryCollection(common::ObIAllocator &alloc);
  virtual ~ObExprGeometryCollection();
  static int eval_geometrycollection(const ObExpr &expr,
                                     ObEvalCtx &ctx,
                                     ObDatum &res);
  int cg_expr(ObExprCGCtx &expr_cg_ctx,
              const ObRawExpr &raw_expr,
              ObExpr &rt_expr) const override;
  const char *get_func_name() const override { return N_GEOMETRYCOLLECTION; }
  common::ObGeoType get_geo_type() const override { return common::ObGeoType::GEOMETRYCOLLECTION; }
  common::ObGeoType expect_sub_type() const override { return common::ObGeoType::GEOTYPEMAX; }
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprGeometryCollection);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_SPATIAL_COLLECTION_