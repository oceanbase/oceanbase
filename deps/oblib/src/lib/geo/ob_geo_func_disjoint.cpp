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
 * This file contains implementation for ob_geo_func_disjoint.
 */

#define USING_LOG_PREFIX LIB

#include "lib/geo/ob_geo_dispatcher.h"
#include "lib/geo/ob_geo_func_disjoint.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace common
{

// Notice: this is not the full implementation, only cases used by intersects are implemented

// ----- ObGeoFuncDisjointImpl -----
class ObGeoFuncDisjointImpl : public ObIGeoDispatcher<bool, ObGeoFuncDisjointImpl>
{
public:
  ObGeoFuncDisjointImpl();
  virtual ~ObGeoFuncDisjointImpl() = default;

  // defaults
  OB_GEO_UNARY_FUNC_DEFAULT(bool, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);
  OB_GEO_TREE_UNARY_FUNC_DEFAULT(bool, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);
  OB_GEO_CART_BINARY_FUNC_DEFAULT(bool, OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS);
  OB_GEO_GEOG_BINARY_FUNC_DEFAULT(bool, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);
  OB_GEO_CART_TREE_FUNC_DEFAULT(bool, OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS);
  OB_GEO_GEOG_TREE_FUNC_DEFAULT(bool, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);

};

// cases for cartesian
OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeomMultiPoint, ObWkbGeomPolygon, bool)
{
  UNUSED(context);
  result = true;
  const ObWkbGeomMultiPoint *geo1 = reinterpret_cast<const ObWkbGeomMultiPoint *>(g1->val());
  const ObWkbGeomPolygon *geo2 = reinterpret_cast<const ObWkbGeomPolygon *>(g2->val());
  ObWkbGeomMultiPoint::iterator iter = geo1->begin();
  for (; (iter != geo1->end()) && (result == true); ++iter) {
    typename ObWkbGeomMultiPoint::value_type& point = *iter;
    if (boost::geometry::disjoint(point, *geo2) == false) {
      result = false;
    }
  }
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeomPolygon, ObWkbGeomMultiPoint, bool)
{
  return EvalWkbBi<ObWkbGeomMultiPoint, ObWkbGeomPolygon> :: eval(g2, g1, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeomMultiPoint, ObWkbGeomMultiPolygon, bool)
{
  UNUSED(context);
  result = true;
  const ObWkbGeomMultiPoint *geo1 = reinterpret_cast<const ObWkbGeomMultiPoint *>(g1->val());
  const ObWkbGeomMultiPolygon *geo2 = reinterpret_cast<const ObWkbGeomMultiPolygon *>(g2->val());
  ObWkbGeomMultiPoint::iterator iter = geo1->begin();
  for (; (iter != geo1->end()) && (result == true); ++iter) {
    typename ObWkbGeomMultiPoint::value_type& point = *iter;
    if (boost::geometry::disjoint(point, *geo2) == false) {
      result = false;
    }
  }
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeomMultiPolygon, ObWkbGeomMultiPoint, bool)
{
  return EvalWkbBi<ObWkbGeomMultiPoint, ObWkbGeomMultiPolygon> :: eval(g2, g1, context, result);
} OB_GEO_FUNC_END;

// cases for geographic
OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeogMultiPoint, ObWkbGeogPolygon, bool)
{
  UNUSED(context);
  INIT_SUCC(ret);
  const ObSrsItem *srs = context.get_srs();
  if (OB_ISNULL(srs)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("srs is null", K(ret), K(g1->get_srid()), K(g1), K(g2));
  } else {
    result = true;
    const ObWkbGeogMultiPoint *geo1 = reinterpret_cast<const ObWkbGeogMultiPoint *>(g1->val());
    const ObWkbGeogPolygon *geo2 = reinterpret_cast<const ObWkbGeogPolygon *>(g2->val());
    boost::geometry::srs::spheroid<double> geog_sphere(srs->semi_major_axis(), srs->semi_minor_axis());
    boost::geometry::strategy::within::geographic_winding<ObWkbGeogPoint> point_strategy(geog_sphere);
    ObWkbGeogMultiPoint::iterator iter = geo1->begin();
    for (; (iter != geo1->end()) && (result == true); ++iter) {
      typename ObWkbGeogMultiPoint::value_type& point = *iter;
      if (boost::geometry::disjoint(point, *geo2, point_strategy) == false) {
        result = false;
      }
    }
  }
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeogPolygon, ObWkbGeogMultiPoint, bool)
{
  return EvalWkbBiGeog<ObWkbGeogMultiPoint, ObWkbGeogPolygon> :: eval(g2, g1, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeogMultiPoint, ObWkbGeogMultiPolygon, bool)
{
  INIT_SUCC(ret);
  const ObSrsItem *srs = context.get_srs();
  if (OB_ISNULL(srs)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("srs is null", K(ret), K(g1->get_srid()), K(g1), K(g2));
  } else {
    result = true;
    const ObWkbGeogMultiPoint *geo1 = reinterpret_cast<const ObWkbGeogMultiPoint *>(g1->val());
    const ObWkbGeogMultiPolygon *geo2 = reinterpret_cast<const ObWkbGeogMultiPolygon *>(g2->val());
    boost::geometry::srs::spheroid<double> geog_sphere(srs->semi_major_axis(), srs->semi_minor_axis());
    boost::geometry::strategy::within::geographic_winding<ObWkbGeogPoint> point_strategy(geog_sphere);
    ObWkbGeogMultiPoint::iterator iter = geo1->begin();
    for (; (iter != geo1->end()) && (result == true); ++iter) {
      typename ObWkbGeogMultiPoint::value_type& point = *iter;
      if (boost::geometry::disjoint(point, *geo2, point_strategy) == false) {
        result = false;
      }
    }
  }
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeogMultiPolygon, ObWkbGeogMultiPoint, bool)
{
  return EvalWkbBiGeog<ObWkbGeogMultiPoint, ObWkbGeogMultiPolygon> :: eval(g2, g1, context, result);
} OB_GEO_FUNC_END;

int ObGeoFuncDisjoint::eval(const ObGeoEvalCtx &gis_context, bool &result)
{
  return ObGeoFuncDisjointImpl::eval_geo_func(gis_context, result);
}

} // sql
} // oceanbase