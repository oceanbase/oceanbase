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
 * This file contains implementation for ob_geo_func_area.
 */

#define USING_LOG_PREFIX LIB

#include "lib/geo/ob_geo_dispatcher.h"
#include "lib/geo/ob_geo_func_area.h"
#include "lib/oblog/ob_log_module.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace common
{

class ObGeoFuncAreaImpl : public ObIGeoDispatcher<double, ObGeoFuncAreaImpl>
{
public:
  ObGeoFuncAreaImpl();
  virtual ~ObGeoFuncAreaImpl() = default;

  OB_GEO_UNARY_FUNC_DEFAULT(double, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);
  OB_GEO_TREE_UNARY_FUNC_DEFAULT(double, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);
  OB_GEO_CART_BINARY_FUNC_DEFAULT(double, OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS);
  OB_GEO_GEOG_BINARY_FUNC_DEFAULT(double, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);
  OB_GEO_CART_TREE_FUNC_DEFAULT(double, OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS);
  OB_GEO_GEOG_TREE_FUNC_DEFAULT(double, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);
};

// cartisain
// inputs: (const ObGeometry *g, const ObGeoEvalCtx &context, double &result)
OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncAreaImpl, ObWkbGeomPolygon, double)
{
  UNUSED(context);
  INIT_SUCC(ret);
  const ObWkbGeomPolygon *geo = reinterpret_cast<const ObWkbGeomPolygon *>(g->val());
  result = boost::geometry::area(*geo);
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncAreaImpl, ObWkbGeomMultiPolygon, double)
{
  UNUSED(context);
  INIT_SUCC(ret);
  const ObWkbGeomMultiPolygon *geo = reinterpret_cast<const ObWkbGeomMultiPolygon *>(g->val());
  result = boost::geometry::area(*geo);
  return ret;
} OB_GEO_FUNC_END;

// geography
OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncAreaImpl, ObWkbGeogPolygon, double)
{
  INIT_SUCC(ret);
  const ObSrsItem *srs = context.get_srs();
  if (OB_ISNULL(srs)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("srs is null", K(ret), K(g->get_srid()), K(g));
  } else {
    boost::geometry::srs::spheroid<double> geog_sphere(srs->semi_major_axis(), srs->semi_minor_axis());
    boost::geometry::strategy::area::geographic<> area_strategy(geog_sphere);
    const ObWkbGeogPolygon *geo = reinterpret_cast<const ObWkbGeogPolygon *>(g->val());
    result = boost::geometry::area(*geo, area_strategy);
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncAreaImpl, ObWkbGeogMultiPolygon, double)
{
  INIT_SUCC(ret);
  const ObSrsItem *srs = context.get_srs();
  if (OB_ISNULL(srs)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("srs is null", K(ret), K(g->get_srid()), K(g));
  } else {
    boost::geometry::srs::spheroid<double> geog_sphere(srs->semi_major_axis(), srs->semi_minor_axis());
    boost::geometry::strategy::area::geographic<> area_strategy(geog_sphere);
    const ObWkbGeogMultiPolygon *geo = reinterpret_cast<const ObWkbGeogMultiPolygon *>(g->val());
    result = boost::geometry::area(*geo, area_strategy);
  }
  return ret;
} OB_GEO_FUNC_END;

int ObGeoFuncArea::eval(const ObGeoEvalCtx &gis_context, double &result)
{
  return ObGeoFuncAreaImpl::eval_geo_func(gis_context, result);
}

} // sql
} // oceanbase