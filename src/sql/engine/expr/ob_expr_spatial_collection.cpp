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

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_spatial_collection.h"
#include "lib/geo/ob_geo_bin.h"
#include "sql/engine/expr/ob_geo_expr_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprSpatialCollection::ObExprSpatialCollection(ObIAllocator &alloc,
                                                 ObExprOperatorType type,
                                                 const char *name,
                                                 int32_t param_num,
                                                 int32_t dimension)
    : ObFuncExprOperator(alloc,
                         type,
                         name,
                         param_num,
                         VALID_FOR_GENERATED_COL,
                         dimension)
{
}

ObExprSpatialCollection::~ObExprSpatialCollection()
{
}

int ObExprSpatialCollection::calc_result_typeN(ObExprResType &type,
                                               ObExprResType *types_stack,
                                               int64_t param_num,
                                               common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  type.set_type(ObGeometryType);
  type.set_collation_level(common::CS_LEVEL_COERCIBLE);
  type.set_collation_type(CS_TYPE_BINARY);
  for (uint32_t i = 0; i < param_num && OB_SUCC(ret); i++) {
    ObObjType in_type = types_stack[i].get_type();
    if (ob_is_null(in_type)) {
      ret = OB_ERR_ILLEGAL_VALUE_FOR_TYPE;
      LOG_USER_ERROR(OB_ERR_ILLEGAL_VALUE_FOR_TYPE, "non geometric",
          static_cast<int>(strlen("NULL")), "NULL");
    } else if (!ob_is_geometry_tc(in_type)) {
      ret = OB_ERR_ILLEGAL_VALUE_FOR_TYPE;
      LOG_USER_ERROR(OB_ERR_ILLEGAL_VALUE_FOR_TYPE, "non geometric",
          static_cast<int>(strlen(ob_sql_type_str(in_type))), ob_sql_type_str(in_type));
    }
  }
  return ret;
}

int ObExprSpatialCollection::calc_resultN(common::ObObj &result,
                                          const common::ObObj *objs,
                                          int64_t param_num,
                                          common::ObExprCtx &expr_ctx) const
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator(ObModIds::OB_LOB_ACCESS_BUFFER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObWkbBuffer res_wkb_buf(tmp_allocator);
  uint32_t srid = 0;
  ObGeoType geo_type = get_geo_type();

  if (ObGeoType::GEOMETRY >= geo_type || ObGeoType::GEOTYPEMAX <= geo_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid geo type", K(ret), K(geo_type));
  } else if (OB_FAIL(res_wkb_buf.append(srid))) {
    LOG_WARN("fail to append srid to res wkb buf", K(ret), K(srid));
  } else if (OB_FAIL(res_wkb_buf.append(static_cast<char>(ENCODE_GEO_VERSION(GEO_VESION_1))))) {
      LOG_WARN("fail to append version to point wkb buf", K(ret));
  } else if (OB_FAIL(res_wkb_buf.append(static_cast<char>(ObGeoWkbByteOrder::LittleEndian)))) {
    LOG_WARN("fail to append little endian byte order to res wkb buf", K(ret));
  } else if (OB_FAIL(res_wkb_buf.append(static_cast<uint32_t>(geo_type)))) {
    LOG_WARN("fail to append geo type to res wkb buf", K(ret), K(geo_type));
  } else if (OB_FAIL(res_wkb_buf.append(static_cast<uint32_t>(param_num)))) {
    LOG_WARN("fail to append arg cnt to res wkb buf", K(ret), K(param_num));
  } else if (ObGeoType::GEOMETRYCOLLECTION == geo_type && param_num == 0) {
    // construct an empty geometry by calling GeometryCollection().
  } else {
    for (uint32_t i = 0; OB_SUCC(ret) && i < param_num; i++) {
      ObString wkb = objs[i].get_string();
      if (OB_FAIL(ObTextStringHelper::read_real_string_data(&tmp_allocator, objs[i], wkb))) {
        LOG_WARN("fail to get real wkb data.", K(ret), K(wkb));
      } else if (ObGeoType::LINESTRING == geo_type) { // linestring
        if (OB_FAIL(calc_linestring(wkb, res_wkb_buf))) {
          LOG_WARN("fail to calc linestring", K(ret), K(wkb));
        }
      } else if (ObGeoType::POLYGON == geo_type) { // polygon
        if (OB_FAIL(calc_polygon(wkb, res_wkb_buf))) {
          LOG_WARN("fail to calc polygon", K(ret), K(wkb));
        }
      } else if ((ObGeoType::MULTIPOINT == geo_type
          || ObGeoType::MULTILINESTRING == geo_type
          || ObGeoType::MULTIPOLYGON == geo_type)) { // multi
        if (OB_FAIL(calc_multi(wkb, res_wkb_buf))) {
          LOG_WARN("fail to calc multi", K(ret), K(wkb));
        }
      } else if (ObGeoType::GEOMETRYCOLLECTION == geo_type) { // geometrycollection
        const ObString wkb_sub = wkb;
        const char *data = wkb_sub.ptr() + WKB_OFFSET;
        const uint64_t len = wkb_sub.length() - WKB_OFFSET;
        if (res_wkb_buf.append(data, len)) {
          LOG_WARN("fail to append sub data to res wkb buf", K(ret), K(wkb_sub), K(len));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObTextStringObObjResult text_result(ObGeometryType, nullptr, &result, true);
    if (OB_FAIL(text_result.init(res_wkb_buf.length(), expr_ctx.calc_buf_))) {
      LOG_WARN("init lob result failed");
    } else if (OB_FAIL(text_result.append(res_wkb_buf.ptr(), res_wkb_buf.length()))) {
      LOG_WARN("failed to append realdata", K(ret), K(text_result));
    } else {
      text_result.set_result();
    }
  }

  if (OB_SUCC(ret) && ObGeoType::LINESTRING == geo_type && param_num < 2) { // adapt mysql, check arg count at last.
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, get_func_name());
    LOG_WARN("invalid linestring data", K(ret), K(param_num));
  }

  return ret;
}

int ObExprSpatialCollection::calc_linestring(const ObString &wkb_point,
                                             ObWkbBuffer &res_wkb_buf) const
{
  int ret = OB_SUCCESS;
  ObGeoType wkb_type = ObGeoType::GEOMETRY;
  const char *data = wkb_point.ptr() + WKB_INNER_POINT;

  if (wkb_point.length() < WKB_COMMON_WKB_HEADER_LEN) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ObGeoTypeUtil::get_type_from_wkb(wkb_point, wkb_type))) {
    LOG_WARN("fail to get geo type", K(ret), K(wkb_point));
  } else if (expect_sub_type() != wkb_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, get_func_name());
    LOG_WARN("unexpected sub geo type", K(ret), K(wkb_type));
  } else if (OB_FAIL(res_wkb_buf.append(data, WKB_POINT_DATA_SIZE))) {
    LOG_WARN("fail to append arg cnt to res wkb buf", K(ret), K(wkb_point));
  }

  return ret;
}

int ObExprSpatialCollection::calc_multi(const ObString &sub,
                                        ObWkbBuffer &res_wkb_buf) const
{
  int ret = OB_SUCCESS;
  ObGeoType wkb_type = ObGeoType::GEOMETRY;

  if (sub.length() < WKB_COMMON_WKB_HEADER_LEN) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ObGeoTypeUtil::get_type_from_wkb(sub, wkb_type))) {
    LOG_WARN("fail to get geo type", K(ret), K(sub));
  } else if (expect_sub_type() != wkb_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, get_func_name());
    LOG_WARN("unexpected sub geo type", K(ret), K(wkb_type));
  } else {
    const char *data = sub.ptr() + WKB_OFFSET;
    const uint32_t len = sub.length() - WKB_OFFSET;
    if (OB_FAIL(res_wkb_buf.append(data, len))) {
      LOG_WARN("fail to append point data", K(ret));
    }
  }

  return ret;
}

int ObExprSpatialCollection::calc_polygon(const ObString wkb_linestring,
                                          ObWkbBuffer &res_wkb_buf) const
{
  int ret = OB_SUCCESS;
  ObGeoType wkb_type = ObGeoType::GEOMETRY;

  if (wkb_linestring.length() < WKB_COMMON_WKB_HEADER_LEN) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ObGeoTypeUtil::get_type_from_wkb(wkb_linestring, wkb_type))) {
    LOG_WARN("fail to get geo type", K(ret), K(wkb_linestring));
  } else if (expect_sub_type() != wkb_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, get_func_name());
    LOG_WARN("unexpected sub geo type", K(ret), K(wkb_type));
  } else {
    ObGeoWkbByteOrder bo = ObGeoWkbByteOrder::LittleEndian;
    const char *data = wkb_linestring.ptr() + WKB_DATA_OFFSET + WKB_GEO_TYPE_SIZE;
    int64_t len = wkb_linestring.length() - WKB_DATA_OFFSET - WKB_GEO_TYPE_SIZE;
    const char *org_data = data;
    if (ObGeoTypeUtil::get_bo_from_wkb(wkb_linestring, bo)) {
      LOG_WARN("fail to get byte order", K(ret), K(wkb_linestring));
    } else if (len < WKB_GEO_ELEMENT_NUM_SIZE) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid data len", K(ret), K(len));
    } else {
      uint32_t point_num = ObGeoWkbByteOrderUtil::read<uint32_t>(data, bo);
      data += WKB_GEO_ELEMENT_NUM_SIZE;
      // A ring must have at least 4 points.
      if (point_num < POLYGON_RING_POINT_NUM_AT_LEAST
          || len != WKB_GEO_ELEMENT_NUM_SIZE + point_num * WKB_POINT_DATA_SIZE) {
        ret = OB_ERR_GIS_INVALID_DATA;
        LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, get_func_name());
      } else {
        double x1 = ObGeoWkbByteOrderUtil::read<double>(data, bo);
        data += WKB_GEO_DOUBLE_STORED_SIZE;
        double y1 = ObGeoWkbByteOrderUtil::read<double>(data, bo);
        data += WKB_GEO_DOUBLE_STORED_SIZE;
        data += (point_num - 2) * WKB_POINT_DATA_SIZE; // skip to the last point
        double x2 = ObGeoWkbByteOrderUtil::read<double>(data, bo);
        data += WKB_GEO_DOUBLE_STORED_SIZE;
        double y2 = ObGeoWkbByteOrderUtil::read<double>(data, bo);
        // check ring is closed or not
        if ((x1 != x2) || (y1 != y2)) {
          ret = OB_ERR_GIS_INVALID_DATA;
          LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, get_func_name());
        } else if (OB_FAIL(res_wkb_buf.append(org_data, len))) {
          LOG_WARN("fail to append data", K(ret), K(wkb_linestring), K(len));
        }
      }
    }
  }

  return ret;
}

int ObExprSpatialCollection::eval_spatial_collection(const ObExpr &expr,
                                                     ObEvalCtx &ctx,
                                                     ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  ObWkbBuffer res_wkb_buf(tmp_allocator);
  uint32_t srid = 0;
  ObGeoType geo_type = get_geo_type();

  if (ObGeoType::GEOMETRY >= geo_type || ObGeoType::GEOTYPEMAX <= geo_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid geo type", K(ret), K(geo_type));
  } else if (OB_FAIL(res_wkb_buf.append(srid))) {
    LOG_WARN("fail to append srid to res wkb buf", K(ret), K(srid));
  } else if (OB_FAIL(res_wkb_buf.append(static_cast<char>(ENCODE_GEO_VERSION(GEO_VESION_1))))) {
      LOG_WARN("fail to append version to point wkb buf", K(ret));
  } else if (OB_FAIL(res_wkb_buf.append(static_cast<char>(ObGeoWkbByteOrder::LittleEndian)))) {
    LOG_WARN("fail to append little endian byte order to res wkb buf", K(ret));
  } else if (OB_FAIL(res_wkb_buf.append(static_cast<uint32_t>(geo_type)))) {
    LOG_WARN("fail to append geo type to res wkb buf", K(ret), K(geo_type));
  } else if (OB_FAIL(res_wkb_buf.append(expr.arg_cnt_))) {
    LOG_WARN("fail to append arg cnt to res wkb buf", K(ret), K(expr.arg_cnt_));
  } else if (ObGeoType::GEOMETRYCOLLECTION == geo_type && expr.arg_cnt_ == 0) {
    // construct an empty geometry by calling GeometryCollection().
  } else {
    ObDatum *datum = NULL;
    ObString wkb;
    for (uint32_t i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; i++) {
      if (OB_FAIL(expr.args_[i]->eval(ctx, datum))) {
        LOG_WARN("fail to eval datum", K(ret));
      } else if (FALSE_IT(wkb = datum->get_string())) {
      } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(tmp_allocator, *datum,
                 expr.args_[i]->datum_meta_, expr.args_[i]->obj_meta_.has_lob_header(), wkb))) {
        LOG_WARN("fail to get real string data", K(ret), K(i), K(wkb));
      } else if (ObGeoType::LINESTRING == geo_type) { // linestring
        if (OB_FAIL(calc_linestring(wkb, res_wkb_buf))) {
          LOG_WARN("fail to calc linestring", K(ret), K(wkb));
        }
      } else if (ObGeoType::POLYGON == geo_type) { // polygon
        if (OB_FAIL(calc_polygon(wkb, res_wkb_buf))) {
          LOG_WARN("fail to calc polygon", K(ret), K(wkb));
        }
      } else if ((ObGeoType::MULTIPOINT == geo_type
          || ObGeoType::MULTILINESTRING == geo_type
          || ObGeoType::MULTIPOLYGON == geo_type)) { // multi
        if (OB_FAIL(calc_multi(wkb, res_wkb_buf))) {
          LOG_WARN("fail to calc multi", K(ret), K(wkb));
        }
      } else if (ObGeoType::GEOMETRYCOLLECTION == geo_type) { // geometrycollection
        const ObString wkb_sub = wkb;
        const char *data = wkb_sub.ptr() + WKB_OFFSET;
        const uint64_t len = wkb_sub.length() - WKB_OFFSET;
        if (res_wkb_buf.append(data, len)) {
          LOG_WARN("fail to append sub data to res wkb buf", K(ret), K(wkb_sub), K(len));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObGeoExprUtils::pack_geo_res(expr, ctx, res, res_wkb_buf.string()))) {
      LOG_WARN("fail to pack geo res", K(ret));
    }
  }

  if (OB_SUCC(ret) && ObGeoType::LINESTRING == geo_type && expr.arg_cnt_ < 2) { // adapt mysql, check arg count at last.
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, get_func_name());
    LOG_WARN("invalid linestring data", K(ret), K(expr.arg_cnt_));
  }

  return ret;
}

ObExprLineString::ObExprLineString(common::ObIAllocator &alloc)
    : ObExprSpatialCollection(alloc,
                              T_FUN_SYS_LINESTRING,
                              N_LINESTRING,
                              PARAM_NUM_UNKNOWN,
                              NOT_ROW_DIMENSION)
{
}

ObExprLineString::~ObExprLineString()
{
}

int ObExprLineString::eval_linestring(const ObExpr &expr,
                                      ObEvalCtx &ctx,
                                      ObDatum &res)
{
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  ObExprLineString line(tmp_allocator);
  ObExprSpatialCollection *p = &line;
  return p->eval_spatial_collection(expr, ctx, res);
}

int ObExprLineString::cg_expr(ObExprCGCtx &expr_cg_ctx,
                              const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_linestring;
  return OB_SUCCESS;
}

ObExprPolygon::ObExprPolygon(common::ObIAllocator &alloc)
    : ObExprSpatialCollection(alloc,
                              T_FUN_SYS_POLYGON,
                              N_POLYGON,
                              PARAM_NUM_UNKNOWN,
                              NOT_ROW_DIMENSION)
{
}

ObExprPolygon::~ObExprPolygon()
{
}

int ObExprPolygon::eval_polygon(const ObExpr &expr,
                                ObEvalCtx &ctx,
                                ObDatum &res)
{
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  ObExprPolygon poly(tmp_allocator);
  ObExprSpatialCollection *p = &poly;
  return p->eval_spatial_collection(expr, ctx, res);
}

int ObExprPolygon::cg_expr(ObExprCGCtx &expr_cg_ctx,
                           const ObRawExpr &raw_expr,
                           ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_polygon;
  return common::OB_SUCCESS;
}

ObExprMultiPoint::ObExprMultiPoint(common::ObIAllocator &alloc)
    : ObExprSpatialCollection(alloc,
                              T_FUN_SYS_MULTIPOINT,
                              N_MULTIPOINT,
                              PARAM_NUM_UNKNOWN,
                              NOT_ROW_DIMENSION)
{
}

ObExprMultiPoint::~ObExprMultiPoint()
{
}

int ObExprMultiPoint::eval_multipoint(const ObExpr &expr,
                                      ObEvalCtx &ctx,
                                      ObDatum &res)
{
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  ObExprMultiPoint mp(tmp_allocator);
  ObExprSpatialCollection *p = &mp;
  return p->eval_spatial_collection(expr, ctx, res);
}

int ObExprMultiPoint::cg_expr(ObExprCGCtx &expr_cg_ctx,
                              const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_multipoint;
  return common::OB_SUCCESS;
}

ObExprMultiLineString::ObExprMultiLineString(common::ObIAllocator &alloc)
    : ObExprSpatialCollection(alloc,
                              T_FUN_SYS_MULTILINESTRING,
                              N_MULTILINESTRING,
                              PARAM_NUM_UNKNOWN,
                              NOT_ROW_DIMENSION)
{
}

ObExprMultiLineString::~ObExprMultiLineString()
{
}

int ObExprMultiLineString::eval_multilinestring(const ObExpr &expr,
                                                ObEvalCtx &ctx,
                                                ObDatum &res)
{
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  ObExprMultiLineString ml(tmp_allocator);
  ObExprSpatialCollection *p = &ml;
  return p->eval_spatial_collection(expr, ctx, res);
}

int ObExprMultiLineString::cg_expr(ObExprCGCtx &expr_cg_ctx,
                                   const ObRawExpr &raw_expr,
                                   ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_multilinestring;
  return common::OB_SUCCESS;
}

ObExprMultiPolygon::ObExprMultiPolygon(common::ObIAllocator &alloc)
    : ObExprSpatialCollection(alloc,
                              T_FUN_SYS_MULTIPOLYGON,
                              N_MULTIPOLYGON,
                              PARAM_NUM_UNKNOWN,
                              NOT_ROW_DIMENSION)
{
}

ObExprMultiPolygon::~ObExprMultiPolygon()
{
}

int ObExprMultiPolygon::eval_multipolygon(const ObExpr &expr,
                                          ObEvalCtx &ctx,
                                          ObDatum &res)
{
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  ObExprMultiPolygon mpoly(tmp_allocator);
  ObExprSpatialCollection *p = &mpoly;
  return p->eval_spatial_collection(expr, ctx, res);
}

int ObExprMultiPolygon::cg_expr(ObExprCGCtx &expr_cg_ctx,
                              const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_multipolygon;
  return common::OB_SUCCESS;
}

ObExprGeomCollection::ObExprGeomCollection(common::ObIAllocator &alloc)
    : ObExprSpatialCollection(alloc,
                              T_FUN_SYS_GEOMCOLLECTION,
                              N_GEOMCOLLECTION,
                              PARAM_NUM_UNKNOWN,
                              NOT_ROW_DIMENSION)
{
}

ObExprGeomCollection::~ObExprGeomCollection()
{
}

int ObExprGeomCollection::eval_geomcollection(const ObExpr &expr,
                                              ObEvalCtx &ctx,
                                              ObDatum &res)
{
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  ObExprGeomCollection gc(tmp_allocator);
  ObExprSpatialCollection *p = &gc;
  return p->eval_spatial_collection(expr, ctx, res);
}

int ObExprGeomCollection::cg_expr(ObExprCGCtx &expr_cg_ctx,
                                  const ObRawExpr &raw_expr,
                                  ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_geomcollection;
  return common::OB_SUCCESS;
}

ObExprGeometryCollection::ObExprGeometryCollection(common::ObIAllocator &alloc)
    : ObExprSpatialCollection(alloc,
                              T_FUN_SYS_GEOMCOLLECTION,
                              N_GEOMETRYCOLLECTION,
                              PARAM_NUM_UNKNOWN,
                              NOT_ROW_DIMENSION)
{
}

ObExprGeometryCollection::~ObExprGeometryCollection()
{
}

int ObExprGeometryCollection::eval_geometrycollection(const ObExpr &expr,
                                                      ObEvalCtx &ctx,
                                                      ObDatum &res)
{
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  ObExprGeometryCollection gc(tmp_allocator);
  ObExprSpatialCollection *p = &gc;
  return p->eval_spatial_collection(expr, ctx, res);
}

int ObExprGeometryCollection::cg_expr(ObExprCGCtx &expr_cg_ctx,
                                      const ObRawExpr &raw_expr,
                                      ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_geometrycollection;
  return common::OB_SUCCESS;
}
} // namespace sql
} // namespace oceanbase