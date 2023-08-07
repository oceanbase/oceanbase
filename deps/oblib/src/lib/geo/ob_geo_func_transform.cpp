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
 * This file contains implementation for ob_geo_func_transform.
 */

#define USING_LOG_PREFIX LIB

#include "lib/geo/ob_geo_dispatcher.h"
#include "lib/geo/ob_geo_func_transform.h"
#include "lib/oblog/ob_log_module.h"
#include "boost/geometry/srs/projections/proj4.hpp"
#include "boost/geometry/srs/transformation.hpp"
#include "lib/geo/ob_geo_tree.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace common
{

class ObGeoFuncTransformImpl : public ObIGeoDispatcher<ObGeometry *, ObGeoFuncTransformImpl>
{
public:
  ObGeoFuncTransformImpl();
  virtual ~ObGeoFuncTransformImpl() = default;
  OB_GEO_UNARY_FUNC_DEFAULT(ObGeometry *, OB_ERR_GIS_INVALID_DATA);
  OB_GEO_TREE_UNARY_FUNC_DEFAULT(ObGeometry *, OB_ERR_GIS_INVALID_DATA);
  OB_GEO_CART_BINARY_FUNC_DEFAULT(ObGeometry *, OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS);
  OB_GEO_GEOG_BINARY_FUNC_DEFAULT(ObGeometry *, OB_SUCCESS);
  OB_GEO_CART_TREE_FUNC_DEFAULT(ObGeometry *, OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS);
  OB_GEO_GEOG_TREE_FUNC_DEFAULT(ObGeometry *, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);

private:
  template<typename PtInType, typename PtResType>
  static int apply_bg_transform_point(const ObGeometry *g, const ObGeoEvalCtx &context, ObGeometry *&result)
  {
    int ret = OB_SUCCESS;
    boost::geometry::srs::proj4 src_proj4(context.get_val_arg(0)->string_->ptr());
    boost::geometry::srs::proj4 dest_proj4(context.get_val_arg(1)->string_->ptr());
    boost::geometry::srs::transformation<> transformer(src_proj4, dest_proj4);
    const PtInType *src_geo = reinterpret_cast<const PtInType *>(g->val());
    PtResType *dest_geo = NULL;
    if (OB_ISNULL(dest_geo = OB_NEWx(PtResType, (context.get_allocator())))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret));
    } else {
      transformer.forward(*src_geo, *dest_geo);
      double x = fabs((reinterpret_cast<ObPoint*>(dest_geo))->x());
      double y = fabs((reinterpret_cast<ObPoint*>(dest_geo))->y());
      if (isnan(x) || x == INFINITY || isnan(y) || y == INFINITY) {
        ret = OB_ERROR_OUT_OF_RANGE;
        LOG_WARN("target geometry out of range", K(ret));
      } else {
        result = dest_geo;
      }
    }
    return ret;
  }

  template<typename GeometryInType, typename GeometryResType>
  static int apply_bg_transform(const ObGeometry *g, const ObGeoEvalCtx &context, ObGeometry *&result)
  {
    int ret = OB_SUCCESS;
    GeometryResType *dest_geo = NULL;
    common::ObIAllocator *alloc = context.get_allocator();
    if (OB_ISNULL(alloc)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("unexpected null alloactor for transform functor", K(ret));
    } else if (OB_ISNULL(dest_geo = OB_NEWx(GeometryResType, alloc))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to create geo by type", K(ret));
    } else {
      boost::geometry::srs::proj4 src_proj4(context.get_val_arg(0)->string_->ptr());
      boost::geometry::srs::proj4 dest_proj4(context.get_val_arg(1)->string_->ptr());
      boost::geometry::srs::transformation<> transformer(src_proj4, dest_proj4);
      const GeometryInType *src_geo = reinterpret_cast<const GeometryInType *>(g->val());
      transformer.forward(*src_geo, *dest_geo);
      result = dest_geo;
    }
    return ret;
  }

  template<typename GCInType, typename PtInType, typename LineInType, typename PolyInType,
           typename MPtInType, typename MLineInType, typename MPolyInType, typename GCOutType>
  static int apply_bg_transform_gc(const ObGeometry *g, const ObGeoEvalCtx &context, ObGeometry *&result)
  {
    int ret = OB_SUCCESS;
    GCOutType *dest_geo = NULL;
    const GCInType *src_geo = reinterpret_cast<const GCInType *>(g->val());
    common::ObIAllocator *alloc = context.get_allocator();
    if (OB_ISNULL(alloc)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("unexpected null alloactor for transform functor", K(ret));
    } else if (OB_ISNULL(dest_geo = OB_NEWx(GCOutType, alloc, 0, *alloc))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to create geometry collection", K(ret));
    } else {
      typename GCInType::iterator iter = src_geo->begin();
      typename GCInType::const_pointer sub_ptr;
      for (; iter != src_geo->end() && OB_SUCC(ret); ++iter) {
        sub_ptr = iter.operator->();
        ObGeoType sub_type = src_geo->get_sub_type(sub_ptr);
        ObGeometry *sub_res = NULL;
        ObIWkbGeomPoint sub_geo; // just for setting data, doesn't matter what type it is
        sub_geo.set_data(sub_ptr);
        switch (sub_type) {
          case ObGeoType::POINT : {
            if (OB_FAIL(Eval<PtInType>::eval(&sub_geo, context, sub_res))) {
              LOG_WARN("failed to eval point for geometry collection", K(ret));
            }
            break;
          }
          case ObGeoType::LINESTRING : {
            if (OB_FAIL(Eval<LineInType>::eval(&sub_geo, context, sub_res))) {
              LOG_WARN("failed to eval linestring for geometry collection", K(ret));
            }
            break;
          }
          case ObGeoType::POLYGON : {
            if (OB_FAIL(Eval<PolyInType>::eval(&sub_geo, context, sub_res))) {
              LOG_WARN("failed to eval polygon for geometry collection", K(ret));
            }
            break;
          }
          case ObGeoType::MULTIPOINT : {
            if (OB_FAIL(Eval<MPtInType>::eval(&sub_geo, context, sub_res))) {
              LOG_WARN("failed to eval multipoint for geometry collection", K(ret));
            }
            break;
          }
          case ObGeoType::MULTILINESTRING : {
            if (OB_FAIL(Eval<MLineInType>::eval(&sub_geo, context, sub_res))) {
              LOG_WARN("failed to eval multilinestring for geometry collection", K(ret));
            }
            break;
          }
          case ObGeoType::MULTIPOLYGON : {
            if (OB_FAIL(Eval<MPolyInType>::eval(&sub_geo, context, sub_res))) {
              LOG_WARN("failed to eval multipolygon for geometry collection", K(ret));
            }
            break;
          }
          case ObGeoType::GEOMETRYCOLLECTION : {
            if (OB_FAIL(Eval<GCInType>::eval(&sub_geo, context, sub_res))) {
              LOG_WARN("failed to eval geometrycollection for geometry collection", K(ret));
            }
            break;
          }
          default : {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected geometry type", K(ret));
          }

        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(dest_geo->push_back(*sub_res))) {
            LOG_WARN("failed to push back to geo", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      result = dest_geo;
    }
    return ret;
  }
};

/**
 * Geographical Geometry
 */

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncTransformImpl, ObWkbGeogPoint, ObGeometry *)
{
  int ret = OB_SUCCESS;
  const ObSrsItem *srs = context.get_srs();
  if (srs == NULL || !srs->is_geographical_srs()) {
    ret = apply_bg_transform_point<ObWkbGeogPoint, ObCartesianPoint>(g, context, result);
  } else {
    ret = apply_bg_transform_point<ObWkbGeogPoint, ObGeographPoint>(g, context, result);
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncTransformImpl, ObWkbGeogLineString, ObGeometry *)
{
  int ret = OB_SUCCESS;
  const ObSrsItem *srs = context.get_srs();
  if (srs == NULL || !srs->is_geographical_srs()) {
    ret = apply_bg_transform<ObWkbGeogLineString, ObCartesianLineString>(g, context, result);
  } else {
    ret = apply_bg_transform<ObWkbGeogLineString, ObGeographLineString>(g, context, result);
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncTransformImpl, ObWkbGeogPolygon, ObGeometry *)
{
  int ret = OB_SUCCESS;
  const ObSrsItem *srs = context.get_srs();
  if (srs == NULL || !srs->is_geographical_srs()) {
    ret = apply_bg_transform<ObWkbGeogPolygon, ObCartesianPolygon>(g, context, result);
  } else {
    ret = apply_bg_transform<ObWkbGeogPolygon, ObGeographPolygon>(g, context, result);
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncTransformImpl, ObWkbGeogMultiPoint, ObGeometry *)
{
  int ret = OB_SUCCESS;
  const ObSrsItem *srs = context.get_srs();
  if (srs == NULL || !srs->is_geographical_srs()) {
    ret = apply_bg_transform<ObWkbGeogMultiPoint, ObCartesianMultipoint>(g, context, result);
  } else {
    ret = apply_bg_transform<ObWkbGeogMultiPoint, ObGeographMultipoint>(g, context, result);
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncTransformImpl, ObWkbGeogMultiLineString, ObGeometry *)
{
  int ret = OB_SUCCESS;
  const ObSrsItem *srs = context.get_srs();
  if (srs == NULL || !srs->is_geographical_srs()) {
    ret = apply_bg_transform<ObWkbGeogMultiLineString, ObCartesianMultilinestring>(g, context, result);
  } else {
    ret = apply_bg_transform<ObWkbGeogMultiLineString, ObGeographMultilinestring>(g, context, result);
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncTransformImpl, ObWkbGeogMultiPolygon, ObGeometry *)
{
  int ret = OB_SUCCESS;
  const ObSrsItem *srs = context.get_srs();
  if (srs == NULL || !srs->is_geographical_srs()) {
    ret = apply_bg_transform<ObWkbGeogMultiPolygon, ObCartesianMultipolygon>(g, context, result);
  } else {
    ret = apply_bg_transform<ObWkbGeogMultiPolygon, ObGeographMultipolygon>(g, context, result);
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncTransformImpl, ObWkbGeogCollection, ObGeometry *)
{
  int ret = OB_SUCCESS;
  const ObSrsItem *srs = context.get_srs();
  if (srs == NULL || !srs->is_geographical_srs()) {
    ret = apply_bg_transform_gc<ObWkbGeogCollection, ObWkbGeogPoint, ObWkbGeogLineString,
                                ObWkbGeogPolygon, ObWkbGeogMultiPoint, ObWkbGeogMultiLineString,
                                ObWkbGeogMultiPolygon, ObCartesianGeometrycollection>(g, context, result);
  } else {
    ret = apply_bg_transform_gc<ObWkbGeogCollection, ObWkbGeogPoint, ObWkbGeogLineString,
                                ObWkbGeogPolygon, ObWkbGeogMultiPoint, ObWkbGeogMultiLineString,
                                ObWkbGeogMultiPolygon, ObGeographGeometrycollection>(g, context, result);
  }
  return ret;
} OB_GEO_FUNC_END;

/**
 * Cartesian Geometry
 */

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncTransformImpl, ObWkbGeomPoint, ObGeometry *)
{
  int ret = OB_SUCCESS;
  const ObSrsItem *srs = context.get_srs();
  if (srs == NULL || !srs->is_geographical_srs()) {
    ret = apply_bg_transform_point<ObWkbGeomPoint, ObCartesianPoint>(g, context, result);
  } else {
    ret = apply_bg_transform_point<ObWkbGeomPoint, ObGeographPoint>(g, context, result);
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncTransformImpl, ObWkbGeomLineString, ObGeometry *)
{
  int ret = OB_SUCCESS;
  const ObSrsItem *srs = context.get_srs();
  if (srs == NULL || !srs->is_geographical_srs()) {
    ret = apply_bg_transform<ObWkbGeomLineString, ObCartesianLineString>(g, context, result);
  } else {
    ret = apply_bg_transform<ObWkbGeomLineString, ObGeographLineString>(g, context, result);
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncTransformImpl, ObWkbGeomPolygon, ObGeometry *)
{
  int ret = OB_SUCCESS;
  const ObSrsItem *srs = context.get_srs();
  if (srs == NULL || !srs->is_geographical_srs()) {
    ret = apply_bg_transform<ObWkbGeomPolygon, ObCartesianPolygon>(g, context, result);
  } else {
    ret = apply_bg_transform<ObWkbGeomPolygon, ObGeographPolygon>(g, context, result);
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncTransformImpl, ObWkbGeomMultiPoint, ObGeometry *)
{
  int ret = OB_SUCCESS;
  const ObSrsItem *srs = context.get_srs();
  if (srs == NULL || !srs->is_geographical_srs()) {
    ret = apply_bg_transform<ObWkbGeomMultiPoint, ObCartesianMultipoint>(g, context, result);
  } else {
    ret = apply_bg_transform<ObWkbGeomMultiPoint, ObGeographMultipoint>(g, context, result);
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncTransformImpl, ObWkbGeomMultiLineString, ObGeometry *)
{
  int ret = OB_SUCCESS;
  const ObSrsItem *srs = context.get_srs();
  if (srs == NULL || !srs->is_geographical_srs()) {
    ret = apply_bg_transform<ObWkbGeomMultiLineString, ObCartesianMultilinestring>(g, context, result);
  } else {
    ret = apply_bg_transform<ObWkbGeomMultiLineString, ObGeographMultilinestring>(g, context, result);
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncTransformImpl, ObWkbGeomMultiPolygon, ObGeometry *)
{
  int ret = OB_SUCCESS;
  const ObSrsItem *srs = context.get_srs();
  if (srs == NULL || !srs->is_geographical_srs()) {
    ret = apply_bg_transform<ObWkbGeomMultiPolygon, ObCartesianMultipolygon>(g, context, result);
  } else {
    ret = apply_bg_transform<ObWkbGeomMultiPolygon, ObGeographMultipolygon>(g, context, result);
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncTransformImpl, ObWkbGeomCollection, ObGeometry *)
{
  int ret = OB_SUCCESS;
  const ObSrsItem *srs = context.get_srs();
  if (srs == NULL || !srs->is_geographical_srs()) {
    ret = apply_bg_transform_gc<ObWkbGeomCollection, ObWkbGeomPoint, ObWkbGeomLineString,
                                ObWkbGeomPolygon, ObWkbGeomMultiPoint, ObWkbGeomMultiLineString,
                                ObWkbGeomMultiPolygon, ObCartesianGeometrycollection>(g, context, result);
  } else {
    ret = apply_bg_transform_gc<ObWkbGeomCollection, ObWkbGeomPoint, ObWkbGeomLineString,
                                ObWkbGeomPolygon, ObWkbGeomMultiPoint, ObWkbGeomMultiLineString,
                                ObWkbGeomMultiPolygon, ObGeographGeometrycollection>(g, context, result);
  }
  return ret;
} OB_GEO_FUNC_END;

// implement of outer class eval
// use an outer class to void implement templates in header files
int ObGeoFuncTransform::eval(const ObGeoEvalCtx &gis_context, ObGeometry *&result)
{
  return ObGeoFuncTransformImpl::eval_geo_func(gis_context, result);
}

} // sql
} // oceanbase