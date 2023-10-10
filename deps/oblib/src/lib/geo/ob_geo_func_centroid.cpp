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
 * This file contains implementation for ob_geo_func_centroid.
 */

#define USING_LOG_PREFIX LIB
#include "lib/geo/ob_geo_dispatcher.h"
#include "lib/geo/ob_geo_func_centroid.h"
#include "lib/geo/ob_geo_func_utils.h"
#include "lib/oblog/ob_log_module.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace common
{

class ObGeoFuncCentroidImpl : public ObIGeoDispatcher<ObCartesianPoint, ObGeoFuncCentroidImpl>
{
public:
  ObGeoFuncCentroidImpl();
  virtual ~ObGeoFuncCentroidImpl() = default;

  OB_GEO_UNARY_FUNC_DEFAULT(ObCartesianPoint, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);
  OB_GEO_TREE_UNARY_FUNC_DEFAULT(ObCartesianPoint, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);
  OB_GEO_CART_BINARY_FUNC_DEFAULT(ObCartesianPoint, OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS);
  OB_GEO_GEOG_BINARY_FUNC_DEFAULT(ObCartesianPoint, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);
  OB_GEO_CART_TREE_FUNC_DEFAULT(ObCartesianPoint, OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS);
  OB_GEO_GEOG_TREE_FUNC_DEFAULT(ObCartesianPoint, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);
};

// cartisain
// inputs: (const ObGeometry *g, const ObGeoEvalCtx &context, ObCartesianPoint &result)
OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncCentroidImpl, ObWkbGeomPoint, ObCartesianPoint)
{
  UNUSED(context);
  INIT_SUCC(ret);
  const ObWkbGeomPoint *geo = reinterpret_cast<const ObWkbGeomPoint *>(g->val());
  boost::geometry::centroid(*geo, result);
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncCentroidImpl, ObWkbGeomMultiPoint, ObCartesianPoint)
{
  UNUSED(context);
  INIT_SUCC(ret);
  const ObWkbGeomMultiPoint *geo = reinterpret_cast<const ObWkbGeomMultiPoint *>(g->val());
  boost::geometry::centroid(*geo, result);
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncCentroidImpl, ObWkbGeomLineString, ObCartesianPoint)
{
  UNUSED(context);
  INIT_SUCC(ret);
  const ObWkbGeomLineString *geo = reinterpret_cast<const ObWkbGeomLineString *>(g->val());
  boost::geometry::centroid(*geo, result);
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncCentroidImpl, ObWkbGeomMultiLineString, ObCartesianPoint)
{
  UNUSED(context);
  INIT_SUCC(ret);
  const ObWkbGeomMultiLineString *geo = reinterpret_cast<const ObWkbGeomMultiLineString *>(g->val());
  boost::geometry::centroid(*geo, result);
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncCentroidImpl, ObWkbGeomPolygon, ObCartesianPoint)
{
  INIT_SUCC(ret);
  const ObWkbGeomPolygon *geo = reinterpret_cast<const ObWkbGeomPolygon *>(g->val());
  boost::geometry::centroid(*geo, result);
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncCentroidImpl, ObWkbGeomMultiPolygon, ObCartesianPoint)
{
  UNUSED(context);
  INIT_SUCC(ret);
  const ObWkbGeomMultiPolygon *geo = reinterpret_cast<const ObWkbGeomMultiPolygon *>(g->val());
  boost::geometry::centroid(*geo, result);
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncCentroidImpl, ObWkbGeomCollection, ObCartesianPoint)
{
  INIT_SUCC(ret);
  ObCartesianMultipoint *cart_multi_point = NULL;
  ObCartesianMultilinestring *cart_multi_line = NULL;
  ObCartesianMultipolygon *cart_multi_poly = NULL;
  ObGeometry *geo = const_cast<ObGeometry *>(g);
  if (OB_FAIL(ObGeoFuncUtils::ob_gc_prepare<ObCartesianGeometrycollection>(context, geo, cart_multi_point,
                                                                           cart_multi_line, cart_multi_poly, false))) {
    LOG_WARN("failed to prepare gc", K(ret));
  } else if (!cart_multi_poly->is_empty()) {
    boost::geometry::centroid(*cart_multi_poly, result);
  } else if (!cart_multi_line->is_empty()) {
    boost::geometry::centroid(*cart_multi_line, result);
  } else if (!cart_multi_point->is_empty()) {
    boost::geometry::centroid(*cart_multi_point, result);
  } else {
    ret = OB_EMPTY_RESULT;
  }
  return ret;
} OB_GEO_FUNC_END;

int ObGeoFuncCentroid::eval(const ObGeoEvalCtx &gis_context, ObCartesianPoint &result)
{
  return ObGeoFuncCentroidImpl::eval_geo_func(gis_context, result);
}

} // sql
} // oceanbase