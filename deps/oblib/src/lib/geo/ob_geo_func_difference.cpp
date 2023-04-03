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
 * This file contains implementation for ob_geo_func_difference.
 */

#define USING_LOG_PREFIX LIB

#include "lib/geo/ob_geo_dispatcher.h"
#include "lib/geo/ob_geo_func_difference.h"
#include "lib/geo/ob_geo_tree.h"
#include "lib/geo/ob_geo_to_tree_visitor.h"
#include "lib/oblog/ob_log_module.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace common
{

template <typename GeoType1, typename GeoType2, typename GeometryRes>
static int apply_bg_difference(const ObGeometry *g1, const ObGeometry *g2,
                               const ObGeoEvalCtx &context, ObGeometry *&result)
{
  INIT_SUCC(ret);

  GeometryRes *res = OB_NEWx(GeometryRes, context.get_allocator());
  if (OB_ISNULL(res)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create go by type", K(ret));
  } else {
    const GeoType1 *geo1 = NULL;
    const GeoType2 *geo2 = NULL;
    if (g1->is_tree()) {
      geo1 = reinterpret_cast<const GeoType1 *>(g1);
      geo2 = reinterpret_cast<const GeoType2 *>(g2);
    } else {
      geo1 = reinterpret_cast<const GeoType1 *>(g1->val());
      geo2 = reinterpret_cast<const GeoType2 *>(g2->val());
    }
    boost::geometry::difference(*geo1, *geo2, *res);
    result = res;
  }
  return ret;
}

template <typename GeoType1, typename GeoType2, typename GeometryRes>
static int apply_bg_difference_ll_strategy(const ObGeometry *g1, const ObGeometry *g2, const ObGeoEvalCtx &context, ObGeometry *&result)
{
  INIT_SUCC(ret);
  const ObSrsItem *srs = context.get_srs();
  GeometryRes *res = OB_NEWx(GeometryRes, context.get_allocator());
  if (OB_ISNULL(srs)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("srs is null", K(ret));
  } else if (OB_ISNULL(res)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create go by type", K(ret));
  } else {
    boost::geometry::srs::spheroid<double> geog_sphere(srs->semi_major_axis(), srs->semi_minor_axis());
    ObLlLaAaStrategy line_strategy(geog_sphere);
    const GeoType1 *geo1 = NULL;
    const GeoType2 *geo2 = NULL;
    if (g1->is_tree()) {
      geo1 = reinterpret_cast<const GeoType1 *>(g1);
      geo2 = reinterpret_cast<const GeoType2 *>(g2);
    } else {
      geo1 = reinterpret_cast<const GeoType1 *>(g1->val());
      geo2 = reinterpret_cast<const GeoType2 *>(g2->val());
    }
    boost::geometry::difference(*geo1, *geo2, *res, line_strategy);
    result = res;
  }
  return ret;
}

template <typename GeoType1, typename GeoType2, typename GeometryRes>
static int apply_bg_difference_pl_strategy(const ObGeometry *g1, const ObGeometry *g2, const ObGeoEvalCtx &context, ObGeometry *&result)
{
  INIT_SUCC(ret);
  const ObSrsItem *srs = context.get_srs();
  GeometryRes *res = OB_NEWx(GeometryRes, context.get_allocator());
  if (OB_ISNULL(srs)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("srs is null", K(ret));
  } else if (OB_ISNULL(res)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create go by type", K(ret));
  } else {
    boost::geometry::srs::spheroid<double> geog_sphere(srs->semi_major_axis(), srs->semi_minor_axis());
    ObPlPaStrategy point_strategy(geog_sphere);
    const GeoType1 *geo1 = NULL;
    const GeoType2 *geo2 = NULL;
    if (g1->is_tree()) {
      geo1 = reinterpret_cast<const GeoType1 *>(g1);
      geo2 = reinterpret_cast<const GeoType2 *>(g2);
    } else {
      geo1 = reinterpret_cast<const GeoType1 *>(g1->val());
      geo2 = reinterpret_cast<const GeoType2 *>(g2->val());
    }
    boost::geometry::difference(*geo1, *geo2, *res, point_strategy);
    result = res;
  }
  return ret;
}

template <typename GeometryType>
static int apply_bg_to_tree(const ObGeometry *g1, const ObGeometry *g2, const ObGeoEvalCtx &context, ObGeometry *&result)
{
  INIT_SUCC(ret);
  // if g1.dimension > g2.dimension, g1 - g2 is equal g1(mysql/postgis)
  // so just convert g1 to tree
  ObGeoToTreeVisitor geom_visitor(context.get_allocator());
  GeometryType *geo1 = const_cast<GeometryType *>(reinterpret_cast<const GeometryType *>(g1));
  if (OB_FAIL(geo1->do_visit(geom_visitor))) {
    LOG_WARN("failed to convert bin to tree", K(ret));
  } else {
    result = geom_visitor.get_geometry();
  }
  return ret;
}

class ObGeoFuncDifferenceImpl : public ObIGeoDispatcher<ObGeometry *, ObGeoFuncDifferenceImpl>
{
public:
  ObGeoFuncDifferenceImpl();
  virtual ~ObGeoFuncDifferenceImpl() = default;
  // template for unary
  OB_GEO_UNARY_FUNC_DEFAULT(ObGeometry *, OB_ERR_GIS_INVALID_DATA);
  OB_GEO_TREE_UNARY_FUNC_DEFAULT(ObGeometry *, OB_ERR_GIS_INVALID_DATA);
  OB_GEO_CART_TREE_FUNC_DEFAULT(ObGeometry *, OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS);
  OB_GEO_GEOG_TREE_FUNC_DEFAULT(ObGeometry *, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);

  template <typename GeometyType1, typename GeometyType2>
  struct EvalWkbBi
  {
    static int eval(const ObGeometry *g1, const ObGeometry *g2, const ObGeoEvalCtx &context, ObGeometry *&result)
    {
      UNUSEDx(g1, g2, context, result);
      return OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS;
    }
  };

  template <typename GeometyType1, typename GeometyType2>
  struct EvalWkbBiGeog
  {
    static int eval(const ObGeometry *g1, const ObGeometry *g2, const ObGeoEvalCtx &context, ObGeometry *&result)
    {
      UNUSEDx(g1, g2, context, result);
      return OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS;
    }
  };

};

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeomPoint, ObWkbGeomPoint, ObGeometry *)
{
  return apply_bg_difference<ObWkbGeomPoint, ObWkbGeomPoint, ObCartesianMultipoint>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeomPoint, ObWkbGeomLineString, ObGeometry *)
{
  return apply_bg_difference<ObWkbGeomPoint, ObWkbGeomLineString, ObCartesianMultipoint>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeomPoint, ObWkbGeomPolygon, ObGeometry *)
{
  return apply_bg_difference<ObWkbGeomPoint, ObWkbGeomPolygon, ObCartesianMultipoint>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeomPoint, ObWkbGeomMultiPoint, ObGeometry *)
{
  return apply_bg_difference<ObWkbGeomPoint, ObWkbGeomMultiPoint, ObCartesianMultipoint>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeomPoint, ObWkbGeomMultiLineString, ObGeometry *)
{
  return apply_bg_difference<ObWkbGeomPoint, ObWkbGeomMultiLineString, ObCartesianMultipoint>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeomPoint, ObWkbGeomMultiPolygon, ObGeometry *)
{
  return apply_bg_difference<ObWkbGeomPoint, ObWkbGeomMultiPolygon, ObCartesianMultipoint>(g1, g2, context, result);
} OB_GEO_FUNC_END;

// cartisian linestring
OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeomLineString, ObWkbGeomPoint, ObGeometry *)
{
  // if g1.dimension > g2.dimension, g1 - g2 is equal g1(mysql/postgis)
  // so just convert g1 to tree
  return apply_bg_to_tree<ObIWkbGeomLineString>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeomLineString, ObWkbGeomLineString, ObGeometry *)
{
  return apply_bg_difference<ObWkbGeomLineString, ObWkbGeomLineString, ObCartesianMultilinestring>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeomLineString, ObWkbGeomPolygon, ObGeometry *)
{
  return apply_bg_difference<ObWkbGeomLineString, ObWkbGeomPolygon, ObCartesianMultilinestring>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeomLineString, ObWkbGeomMultiPoint, ObGeometry *)
{
  // if g1.dimension > g2.dimension, g1 - g2 is equal g1(mysql/postgis)
  // so just convert g1 to tree
  return apply_bg_to_tree<ObIWkbGeomLineString>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeomLineString, ObWkbGeomMultiLineString, ObGeometry *)
{
  return apply_bg_difference<ObWkbGeomLineString, ObWkbGeomMultiLineString, ObCartesianMultilinestring>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeomLineString, ObWkbGeomMultiPolygon, ObGeometry *)
{
  return apply_bg_difference<ObWkbGeomLineString, ObWkbGeomMultiPolygon, ObCartesianMultilinestring>(g1, g2, context, result);
} OB_GEO_FUNC_END;

// cartisian polygon
OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeomPolygon, ObWkbGeomPoint, ObGeometry *)
{
  // if g1.dimension > g2.dimension, g1 - g2 is equal g1(mysql/postgis)
  // so just convert g1 to tree
  return apply_bg_to_tree<ObIWkbGeomPolygon>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeomPolygon, ObWkbGeomLineString, ObGeometry *)
{
  // if g1.dimension > g2.dimension, g1 - g2 is equal g1(mysql/postgis)
  // so just convert g1 to tree
  return apply_bg_to_tree<ObIWkbGeomPolygon>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeomPolygon, ObWkbGeomPolygon, ObGeometry *)
{
  return apply_bg_difference<ObWkbGeomPolygon, ObWkbGeomPolygon, ObCartesianMultipolygon>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeomPolygon, ObWkbGeomMultiPoint, ObGeometry *)
{
  // if g1.dimension > g2.dimension, g1 - g2 is equal g1(mysql/postgis)
  // so just convert g1 to tree
  return apply_bg_to_tree<ObIWkbGeomPolygon>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeomPolygon, ObWkbGeomMultiLineString, ObGeometry *)
{
  // if g1.dimension > g2.dimension, g1 - g2 is equal g1(mysql/postgis)
  // so just convert g1 to tree
  return apply_bg_to_tree<ObIWkbGeomPolygon>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeomPolygon, ObWkbGeomMultiPolygon, ObGeometry *)
{
  return apply_bg_difference<ObWkbGeomPolygon, ObWkbGeomMultiPolygon, ObCartesianMultipolygon>(g1, g2, context, result);
} OB_GEO_FUNC_END;

// cartisian multipoint
OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeomMultiPoint, ObWkbGeomPoint, ObGeometry *)
{
  return apply_bg_difference<ObWkbGeomMultiPoint, ObWkbGeomPoint, ObCartesianMultipoint>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeomMultiPoint, ObWkbGeomLineString, ObGeometry *)
{
  return apply_bg_difference<ObWkbGeomMultiPoint, ObWkbGeomLineString, ObCartesianMultipoint>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeomMultiPoint, ObWkbGeomPolygon, ObGeometry *)
{
  return apply_bg_difference<ObWkbGeomMultiPoint, ObWkbGeomPolygon, ObCartesianMultipoint>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeomMultiPoint, ObWkbGeomMultiPoint, ObGeometry *)
{
  return apply_bg_difference<ObWkbGeomMultiPoint, ObWkbGeomMultiPoint, ObCartesianMultipoint>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeomMultiPoint, ObWkbGeomMultiLineString, ObGeometry *)
{
  return apply_bg_difference<ObWkbGeomMultiPoint, ObWkbGeomMultiLineString, ObCartesianMultipoint>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeomMultiPoint, ObWkbGeomMultiPolygon, ObGeometry *)
{
  return apply_bg_difference<ObWkbGeomMultiPoint, ObWkbGeomMultiPolygon, ObCartesianMultipoint>(g1, g2, context, result);
} OB_GEO_FUNC_END;

// cartisian mutilinestring
OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeomMultiLineString, ObWkbGeomPoint, ObGeometry *)
{
  // if g1.dimension > g2.dimension, g1 - g2 is equal g1(mysql/postgis)
  // so just convert g1 to tree
  return apply_bg_to_tree<ObIWkbGeomMultiLineString>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeomMultiLineString, ObWkbGeomLineString, ObGeometry *)
{
  return apply_bg_difference<ObWkbGeomLineString, ObWkbGeomLineString, ObCartesianMultilinestring>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeomMultiLineString, ObWkbGeomPolygon, ObGeometry *)
{
  return apply_bg_difference<ObWkbGeomMultiLineString, ObWkbGeomPolygon, ObCartesianMultilinestring>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeomMultiLineString, ObWkbGeomMultiPoint, ObGeometry *)
{
  // if g1.dimension > g2.dimension, g1 - g2 is equal g1(mysql/postgis)
  // so just convert g1 to tree
  return apply_bg_to_tree<ObIWkbGeomMultiLineString>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeomMultiLineString, ObWkbGeomMultiLineString, ObGeometry *)
{
  return apply_bg_difference<ObWkbGeomMultiLineString, ObWkbGeomMultiLineString, ObCartesianMultilinestring>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeomMultiLineString, ObWkbGeomMultiPolygon, ObGeometry *)
{
  return apply_bg_difference<ObWkbGeomMultiLineString, ObWkbGeomMultiPolygon, ObCartesianMultilinestring>(g1, g2, context, result);
} OB_GEO_FUNC_END;

// cartisian multipolygon
OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeomMultiPolygon, ObWkbGeomPoint, ObGeometry *)
{
  // if g1.dimension > g2.dimension, g1 - g2 is equal g1(mysql/postgis)
  // so just convert g1 to tree
  return apply_bg_to_tree<ObIWkbGeomMultiPolygon>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeomMultiPolygon, ObWkbGeomLineString, ObGeometry *)
{
  // if g1.dimension > g2.dimension, g1 - g2 is equal g1(mysql/postgis)
  // so just convert g1 to tree
  return apply_bg_to_tree<ObIWkbGeomMultiPolygon>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeomMultiPolygon, ObWkbGeomPolygon, ObGeometry *)
{
  return apply_bg_difference<ObWkbGeomMultiPolygon, ObWkbGeomPolygon, ObCartesianMultipolygon>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeomMultiPolygon, ObWkbGeomMultiPoint, ObGeometry *)
{
  // if g1.dimension > g2.dimension, g1 - g2 is equal g1(mysql/postgis)
  // so just convert g1 to tree
  return apply_bg_to_tree<ObIWkbGeomMultiPolygon>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeomMultiPolygon, ObWkbGeomMultiLineString, ObGeometry *)
{
  // if g1.dimension > g2.dimension, g1 - g2 is equal g1(mysql/postgis)
  // so just convert g1 to tree
  return apply_bg_to_tree<ObIWkbGeomMultiPolygon>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeomMultiPolygon, ObWkbGeomMultiPolygon, ObGeometry *)
{
  return apply_bg_difference<ObWkbGeomMultiPolygon, ObWkbGeomMultiPolygon, ObCartesianMultipolygon>(g1, g2, context, result);
} OB_GEO_FUNC_END;

// geographic point
OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeogPoint, ObWkbGeogPoint, ObGeometry *)
{
  return apply_bg_difference<ObWkbGeogPoint, ObWkbGeogPoint, ObGeographMultipoint>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeogPoint, ObWkbGeogLineString, ObGeometry *)
{
  return apply_bg_difference_pl_strategy<ObWkbGeogPoint, ObWkbGeogLineString, ObGeographMultipoint>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeogPoint, ObWkbGeogPolygon, ObGeometry *)
{
  return apply_bg_difference_pl_strategy<ObWkbGeogPoint, ObWkbGeogPolygon, ObGeographMultipoint>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeogPoint, ObWkbGeogMultiPoint, ObGeometry *)
{
  return apply_bg_difference<ObWkbGeogPoint, ObWkbGeogMultiPoint, ObGeographMultipoint>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeogPoint, ObWkbGeogMultiLineString, ObGeometry *)
{
  return apply_bg_difference_pl_strategy<ObWkbGeogPoint, ObWkbGeogMultiLineString, ObGeographMultipoint>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeogPoint, ObWkbGeogMultiPolygon, ObGeometry *)
{
  return apply_bg_difference_pl_strategy<ObWkbGeogPoint, ObWkbGeogMultiPolygon, ObGeographMultipoint>(g1, g2, context, result);
} OB_GEO_FUNC_END;

// geographic linestring
OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeogLineString, ObWkbGeogPoint, ObGeometry *)
{
  // if g1.dimension > g2.dimension, g1 - g2 is equal g1(mysql/postgis)
  // so just convert g1 to tree
  return apply_bg_to_tree<ObIWkbGeogLineString>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeogLineString, ObWkbGeogLineString, ObGeometry *)
{
  return apply_bg_difference_ll_strategy<ObWkbGeogLineString, ObWkbGeogLineString, ObGeographMultilinestring>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeogLineString, ObWkbGeogPolygon, ObGeometry *)
{
  return apply_bg_difference_ll_strategy<ObWkbGeogLineString, ObWkbGeogPolygon, ObGeographMultilinestring>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeogLineString, ObWkbGeogMultiPoint, ObGeometry *)
{
  // if g1.dimension > g2.dimension, g1 - g2 is equal g1(mysql/postgis)
  // so just convert g1 to tree
  return apply_bg_to_tree<ObIWkbGeogLineString>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeogLineString, ObWkbGeogMultiLineString, ObGeometry *)
{
  return apply_bg_difference_ll_strategy<ObWkbGeogLineString, ObWkbGeogMultiLineString, ObGeographMultilinestring>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeogLineString, ObWkbGeogMultiPolygon, ObGeometry *)
{
  return apply_bg_difference_ll_strategy<ObWkbGeogLineString, ObWkbGeogMultiPolygon, ObGeographMultilinestring>(g1, g2, context, result);
} OB_GEO_FUNC_END;

// gepgraphic polygon
OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeogPolygon, ObWkbGeogPoint, ObGeometry *)
{
  // if g1.dimension > g2.dimension, g1 - g2 is equal g1(mysql/postgis)
  // so just convert g1 to tree
  return apply_bg_to_tree<ObIWkbGeogPolygon>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeogPolygon, ObWkbGeogLineString, ObGeometry *)
{
  // if g1.dimension > g2.dimension, g1 - g2 is equal g1(mysql/postgis)
  // so just convert g1 to tree
  return apply_bg_to_tree<ObIWkbGeogPolygon>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeogPolygon, ObWkbGeogPolygon, ObGeometry *)
{
  return apply_bg_difference_ll_strategy<ObWkbGeogPolygon, ObWkbGeogPolygon, ObGeographMultipolygon>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeogPolygon, ObWkbGeogMultiPoint, ObGeometry *)
{
  // if g1.dimension > g2.dimension, g1 - g2 is equal g1(mysql/postgis)
  // so just convert g1 to tree
  return apply_bg_to_tree<ObIWkbGeogPolygon>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeogPolygon, ObWkbGeogMultiLineString, ObGeometry *)
{
  // if g1.dimension > g2.dimension, g1 - g2 is equal g1(mysql/postgis)
  // so just convert g1 to tree
  return apply_bg_to_tree<ObIWkbGeogPolygon>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeogPolygon, ObWkbGeogMultiPolygon, ObGeometry *)
{
  return apply_bg_difference_ll_strategy<ObWkbGeogPolygon, ObWkbGeogMultiPolygon, ObGeographMultipolygon>(g1, g2, context, result);
} OB_GEO_FUNC_END;

// geographic multipoint
OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeogMultiPoint, ObWkbGeogPoint, ObGeometry *)
{
  return apply_bg_difference<ObWkbGeogMultiPoint, ObWkbGeogPoint, ObGeographMultipoint>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeogMultiPoint, ObWkbGeogLineString, ObGeometry *)
{
  return apply_bg_difference_pl_strategy<ObWkbGeogMultiPoint, ObWkbGeogLineString, ObGeographMultipoint>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeogMultiPoint, ObWkbGeogPolygon, ObGeometry *)
{
  return apply_bg_difference_pl_strategy<ObWkbGeogMultiPoint, ObWkbGeogPolygon, ObGeographMultipoint>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeogMultiPoint, ObWkbGeogMultiPoint, ObGeometry *)
{
  return apply_bg_difference<ObWkbGeogMultiPoint, ObWkbGeogMultiPoint, ObGeographMultipoint>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeogMultiPoint, ObWkbGeogMultiLineString, ObGeometry *)
{
  return apply_bg_difference_pl_strategy<ObWkbGeogMultiPoint, ObWkbGeogMultiLineString, ObGeographMultipoint>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeogMultiPoint, ObWkbGeogMultiPolygon, ObGeometry *)
{
  return apply_bg_difference_pl_strategy<ObWkbGeogMultiPoint, ObWkbGeogMultiPolygon, ObGeographMultipoint>(g1, g2, context, result);
} OB_GEO_FUNC_END;

// geographic multilinestring
OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeogMultiLineString, ObWkbGeogPoint, ObGeometry *)
{
  // if g1.dimension > g2.dimension, g1 - g2 is equal g1(mysql/postgis)
  // so just convert g1 to tree
  return apply_bg_to_tree<ObIWkbGeogMultiLineString>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeogMultiLineString, ObWkbGeogLineString, ObGeometry *)
{
  return apply_bg_difference_ll_strategy<ObWkbGeogLineString, ObWkbGeogLineString, ObGeographMultilinestring>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeogMultiLineString, ObWkbGeogPolygon, ObGeometry *)
{
  return apply_bg_difference_ll_strategy<ObWkbGeogMultiLineString, ObWkbGeogPolygon, ObGeographMultilinestring>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeogMultiLineString, ObWkbGeogMultiPoint, ObGeometry *)
{
  // if g1.dimension > g2.dimension, g1 - g2 is equal g1(mysql/postgis)
  // so just convert g1 to tree
  return apply_bg_to_tree<ObIWkbGeogMultiLineString>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeogMultiLineString, ObWkbGeogMultiLineString, ObGeometry *)
{
  return apply_bg_difference_ll_strategy<ObWkbGeogMultiLineString, ObWkbGeogMultiLineString, ObGeographMultilinestring>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeogMultiLineString, ObWkbGeogMultiPolygon, ObGeometry *)
{
  return apply_bg_difference_ll_strategy<ObWkbGeogMultiLineString, ObWkbGeogMultiPolygon, ObGeographMultilinestring>(g1, g2, context, result);
} OB_GEO_FUNC_END;

// geographic mutlipolygon
OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeogMultiPolygon, ObWkbGeogPoint, ObGeometry *)
{
  // if g1.dimension > g2.dimension, g1 - g2 is equal g1(mysql/postgis)
  // so just convert g1 to tree
  return apply_bg_to_tree<ObIWkbGeogMultiPolygon>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeogMultiPolygon, ObWkbGeogLineString, ObGeometry *)
{
  // if g1.dimension > g2.dimension, g1 - g2 is equal g1(mysql/postgis)
  // so just convert g1 to tree
  return apply_bg_to_tree<ObIWkbGeogMultiPolygon>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeogMultiPolygon, ObWkbGeogPolygon, ObGeometry *)
{
  return apply_bg_difference_ll_strategy<ObWkbGeogMultiPolygon, ObWkbGeogPolygon, ObGeographMultipolygon>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeogMultiPolygon, ObWkbGeogMultiPoint, ObGeometry *)
{
  // if g1.dimension > g2.dimension, g1 - g2 is equal g1(mysql/postgis)
  // so just convert g1 to tree
  return apply_bg_to_tree<ObIWkbGeogMultiPolygon>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeogMultiPolygon, ObWkbGeogMultiLineString, ObGeometry *)
{
  // if g1.dimension > g2.dimension, g1 - g2 is equal g1(mysql/postgis)
  // so just convert g1 to tree
  return apply_bg_to_tree<ObIWkbGeogMultiPolygon>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeogMultiPolygon, ObWkbGeogMultiPolygon, ObGeometry *)
{
  return apply_bg_difference_ll_strategy<ObWkbGeogMultiPolygon, ObWkbGeogMultiPolygon, ObGeographMultipolygon>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_TREE_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObCartesianMultipoint, ObCartesianMultipoint, ObGeometry *)
{
  return apply_bg_difference<ObCartesianMultipoint, ObCartesianMultipoint, ObCartesianMultipoint>(g1, g2, context, result);
} OB_GEO_FUNC_END;

// tree cartesian polygon
OB_GEO_CART_TREE_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObCartesianMultipoint, ObCartesianMultilinestring, ObGeometry *)
{
  return apply_bg_difference<ObCartesianMultipoint, ObCartesianMultilinestring, ObCartesianMultipoint>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_TREE_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObCartesianMultipoint, ObCartesianMultipolygon, ObGeometry *)
{
  return apply_bg_difference<ObCartesianMultipoint, ObCartesianMultipolygon, ObCartesianMultipoint>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_TREE_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObCartesianLineString, ObCartesianMultilinestring, ObGeometry *)
{
  return apply_bg_difference<ObCartesianLineString, ObCartesianMultilinestring, ObCartesianMultilinestring>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_TREE_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObCartesianMultilinestring, ObCartesianMultilinestring, ObGeometry *)
{
  return apply_bg_difference<ObCartesianMultilinestring, ObCartesianMultilinestring, ObCartesianMultilinestring>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_TREE_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObCartesianMultilinestring, ObCartesianMultipolygon, ObGeometry *)
{
  return apply_bg_difference<ObCartesianMultilinestring, ObCartesianMultipolygon, ObCartesianMultilinestring>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_TREE_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObCartesianMultipolygon, ObCartesianMultipolygon, ObGeometry *)
{
  return apply_bg_difference<ObCartesianMultipolygon, ObCartesianMultipolygon, ObCartesianMultipolygon>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_TREE_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObGeographMultipoint, ObGeographMultipoint, ObGeometry *)
{
  return apply_bg_difference<ObGeographMultipoint, ObGeographMultipoint, ObGeographMultipoint>(g1, g2, context, result);
} OB_GEO_FUNC_END;

// tree geograph polygon
OB_GEO_GEOG_TREE_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObGeographMultipoint, ObGeographMultilinestring, ObGeometry *)
{
  return apply_bg_difference_pl_strategy<ObGeographMultipoint, ObGeographMultilinestring, ObGeographMultipoint>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_TREE_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObGeographMultipoint, ObGeographMultipolygon, ObGeometry *)
{
  return apply_bg_difference_pl_strategy<ObGeographMultipoint, ObGeographMultipolygon, ObGeographMultipoint>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_TREE_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObGeographLineString, ObGeographMultilinestring, ObGeometry *)
{
  return apply_bg_difference_ll_strategy<ObGeographLineString, ObGeographMultilinestring, ObGeographMultilinestring>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_TREE_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObGeographMultilinestring, ObGeographMultilinestring, ObGeometry *)
{
  return apply_bg_difference_ll_strategy<ObGeographMultilinestring, ObGeographMultilinestring, ObGeographMultilinestring>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_TREE_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObGeographMultilinestring, ObGeographMultipolygon, ObGeometry *)
{
  return apply_bg_difference_ll_strategy<ObGeographMultilinestring, ObGeographMultipolygon, ObGeographMultilinestring>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_TREE_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObGeographMultipolygon, ObGeographMultipolygon, ObGeometry *)
{
  return apply_bg_difference_ll_strategy<ObGeographMultipolygon, ObGeographMultipolygon, ObGeographMultipolygon>(g1, g2, context, result);
} OB_GEO_FUNC_END;

// implement of outer class eval
// use an outer class to void implement templates in header files
int ObGeoFuncDifference::eval(const ObGeoEvalCtx &gis_context, ObGeometry *&result)
{
  return ObGeoFuncDifferenceImpl::eval_geo_func(gis_context, result);
}

} // sql
} // oceanbase
