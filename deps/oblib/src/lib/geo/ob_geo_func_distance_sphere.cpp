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
 * This file contains implementation for ob_geo_func_intersects.
 */

#define USING_LOG_PREFIX LIB

#include "lib/geo/ob_geo_dispatcher.h"
#include "lib/geo/ob_geo_func_distance_sphere.h"
#include "lib/geo/ob_geo_func_disjoint.h"
#include "lib/geo/ob_geo_utils.h"
#include "lib/geo/ob_geo_wkb_visitor.h"
#include "lib/geo/ob_geo_wkb_size_visitor.h"
#include "common/ob_smart_call.h"
#include "lib/utility/ob_hang_fatal_error.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace common
{
namespace bg = boost::geometry;

// adapt mysql only support ObWkbGeogPoint、ObWkbGeogMultiPoint
template <typename GeoType1, typename GeoType2>
int ObGeoFuncDistanceSphereUtil::eval(const GeoType1 *g1,
                                      const GeoType2 *g2,
                                      const ObGeoEvalCtx &context,
                                      double &result)
{
  INIT_SUCC(ret);
  const double *sphere_radius = reinterpret_cast<const double *>(context.get_val_arg(0));

  if (OB_ISNULL(sphere_radius)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sphere_radius is null", K(ret), K(context.get_val_count()));
  } else if (OB_ISNULL(g1) || OB_ISNULL(g2)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("geometry is null", K(ret), KP(g1), KP(g2));
  } else if (ObGeoCRS::Geographic != g1->crs() || ObGeoCRS::Geographic != g2->crs()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid crs type", K(ret), K(g1->crs()), K(g1->crs()));
  } else if (ObGeoType::POINT != g1->type() && ObGeoType::MULTIPOINT != g1->type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid geo1 type", K(ret), K(g1->type()));
  } else if (ObGeoType::POINT != g2->type() && ObGeoType::MULTIPOINT != g2->type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid geo2 type", K(ret), K(g2->type()));
  } else if (ObGeoType::MULTIPOINT == g1->type() && ObGeoType::MULTIPOINT == g2->type()) {
    double min_dis = DBL_MAX;
    double tmp_result = 0.0;
    ObWkbGeogPoint tmp_point;
    const ObWkbGeogMultiPoint *geog_mpt1 = reinterpret_cast<const ObWkbGeogMultiPoint *>(g1);
    const ObWkbGeogMultiPoint *geog_mpt2 = reinterpret_cast<const ObWkbGeogMultiPoint *>(g2);
    ObWkbGeogMultiPoint::iterator iter = geog_mpt2->begin();
    for (; OB_SUCC(ret) && iter != geog_mpt2->end(); ++iter) {
      tmp_point.set<0>(iter->get<0>());
      tmp_point.set<1>(iter->get<1>());
      if (OB_FAIL(eval(geog_mpt1, &tmp_point, context, tmp_result))) {
        LOG_WARN("fail to calc distance", K(ret));
      } else if (tmp_result < min_dis) {
        min_dis = tmp_result;
      }
    }
    if (OB_SUCC(ret)) {
      result = min_dis;
    }
  } else {
    bg::strategy::distance::haversine<double> strategy(*sphere_radius);
    result = bg::distance(*g1, *g2, strategy);
  }

  return ret;
}

template <typename GeoType1, typename GeoType2>
int ObGeoFuncDistanceSphereUtil::eval(const ObGeometry *g1,
                                      const ObGeometry *g2,
                                      const ObGeoEvalCtx &context,
                                      double &result)
{
  INIT_SUCC(ret);

  if (OB_ISNULL(g1) || OB_ISNULL(g2)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("geometry is null", K(ret), KP(g1), KP(g2));
  } else {
    const GeoType1 *geo1 = reinterpret_cast<const GeoType1 *>(g1->val());
    const GeoType2 *geo2 = reinterpret_cast<const GeoType2 *>(g2->val());
    if (OB_FAIL(eval(geo1, geo2, context, result))) {
      LOG_WARN("fail to eval distance sphere result", K(ret), K(geo1->type()), K(geo2->type()));
    }
  }

  return ret;
}

int ObGeoFuncDistanceSphereUtil::reinterpret_as_degrees(double lon_deg,
                                                        double lat_deg,
                                                        double &x,
                                                        double &y,
                                                        double &result)
{
  INIT_SUCC(ret);

  if (!(-180.0 < lon_deg && lon_deg <= 180.0)) {
    ret = OB_ERR_LONGITUDE_OUT_OF_RANGE;
    result = lon_deg; // for LOG_USER_ERROR
  } else if (!(-90.0 <= lat_deg && lat_deg <= 90.0)) {
    ret = OB_ERR_LATITUDE_OUT_OF_RANGE;
    result = lat_deg; // for LOG_USER_ERROR
  } else {
    x = lon_deg * M_PI / 180.0;
    y = lat_deg * M_PI / 180.0;
  }

  return ret;
}

int ObGeoFuncDistanceSphereUtil::reinterpret_as_degrees(const ObWkbGeomPoint *cart_pt,
                                                        ObWkbGeogPoint &geog_pt,
                                                        double &result)
{
  INIT_SUCC(ret);
  double x = 0.0;
  double y = 0.0;

  if (OB_ISNULL(cart_pt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("geometry is null", K(ret));
  } else if (OB_FAIL(reinterpret_as_degrees(cart_pt->get<0>(), cart_pt->get<1>(), x, y, result))) {
    LOG_WARN("fail to cast to degree", K(ret), K(cart_pt->get<0>()), K(cart_pt->get<1>()));
  } else {
    geog_pt.set<0>(x);
    geog_pt.set<1>(y);
  }

  return ret;
}

int ObGeoFuncDistanceSphereUtil::reinterpret_as_degrees(ObIAllocator *allocator,
                                                        const common::ObGeometry *g,
                                                        const ObWkbGeogMultiPoint *&geog_mpt,
                                                        double &result)
{
  INIT_SUCC(ret);
  void *buf = NULL;
  ObWkbGeomMultiPoint *crat_mpt = NULL;

  if (OB_ISNULL(g)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("geometry is null", K(ret));
  } else if (OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("allocator is null", K(ret));
  } else if (ObGeoType::MULTIPOINT != g->type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid geometry type", K(ret), K(g->type()));
  } else if (ObGeoCRS::Cartesian != g->crs()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid crs type", K(ret), K(g->crs()));
  } else if (OB_ISNULL(buf = allocator->alloc(g->length()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret), K(g->length()));
  } else {
    MEMMOVE(buf, g->val(), g->length());
    crat_mpt = new (buf) ObWkbGeomMultiPoint();
    double x = 0.0;
    double y = 0.0;
    ObWkbGeomMultiPoint::iterator iter = crat_mpt->begin();
    for (; OB_SUCC(ret) && iter != crat_mpt->end(); ++iter) {
      if (OB_FAIL(reinterpret_as_degrees(iter->get<0>(), iter->get<1>(), x, y, result))) {
        LOG_WARN("fail to cast to degree", K(ret), K(iter->get<0>()), K(iter->get<1>()));
      } else {
        iter->set<0>(x);
        iter->set<1>(y);
      }
    }

    if (OB_SUCC(ret)) {
      geog_mpt = reinterpret_cast<ObWkbGeogMultiPoint *>(buf);
    }
  }

  return ret;
}

class ObGeoFuncDistanceSphereImpl : public ObIGeoDispatcher<double, ObGeoFuncDistanceSphereImpl>
{
public:
  ObGeoFuncDistanceSphereImpl();
  virtual ~ObGeoFuncDistanceSphereImpl() = default;

  OB_GEO_UNARY_FUNC_DEFAULT(double, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);
  OB_GEO_TREE_UNARY_FUNC_DEFAULT(double, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);
  OB_GEO_CART_BINARY_FUNC_DEFAULT(double, OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS);
  OB_GEO_GEOG_BINARY_FUNC_DEFAULT(double, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);
  OB_GEO_CART_TREE_FUNC_DEFAULT(double, OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS);
  OB_GEO_GEOG_TREE_FUNC_DEFAULT(double, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);
};

// cartisain
// inputs: (const ObGeometry *g1, const ObGeometry *g2, const ObGeoEvalCtx &context, double &result)
OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDistanceSphereImpl,
                              ObWkbGeomPoint,
                              ObWkbGeomPoint,
                              double)
{
  INIT_SUCC(ret);
  ObWkbGeogPoint geog_p1;
  ObWkbGeogPoint geog_p2;
  const ObWkbGeomPoint *cart_p1 = reinterpret_cast<const ObWkbGeomPoint *>(g1->val());
  const ObWkbGeomPoint *cart_p2 = reinterpret_cast<const ObWkbGeomPoint *>(g2->val());

  if (OB_FAIL(ObGeoFuncDistanceSphereUtil::reinterpret_as_degrees(cart_p1, geog_p1, result))) {
    LOG_WARN("fail to reinterpret point1 as degree", K(ret));
  } else if (OB_FAIL(ObGeoFuncDistanceSphereUtil::reinterpret_as_degrees(cart_p2, geog_p2, result))) {
    LOG_WARN("fail to reinterpret poin2 as degree", K(ret));
  } else if (OB_FAIL(ObGeoFuncDistanceSphereUtil::eval(&geog_p1, &geog_p2,
      context, result))) {
    LOG_WARN("fail to eval distance sphere result", K(ret));
  }

  return ret;
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDistanceSphereImpl,
                              ObWkbGeomPoint,
                              ObWkbGeomMultiPoint,
                              double)
{
  INIT_SUCC(ret);
  ObWkbGeogPoint geog_pt;
  const ObWkbGeomPoint *cart_pt = reinterpret_cast<const ObWkbGeomPoint *>(g1->val());
  const ObWkbGeogMultiPoint *geog_mpt = NULL;

  if (OB_FAIL(ObGeoFuncDistanceSphereUtil::reinterpret_as_degrees(cart_pt, geog_pt, result))) {
    LOG_WARN("fail to reinterpret point as degree", K(ret));
  } else if (OB_FAIL(ObGeoFuncDistanceSphereUtil::reinterpret_as_degrees(context.get_allocator(),
      g2, geog_mpt, result))) {
    LOG_WARN("fail to reinterpret multipoint as degree", K(ret));
  } else if (OB_FAIL(ObGeoFuncDistanceSphereUtil::eval(&geog_pt, geog_mpt,
      context, result))) {
    LOG_WARN("fail to eval distance sphere result", K(ret));
  }

  return ret;
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDistanceSphereImpl,
                              ObWkbGeomMultiPoint,
                              ObWkbGeomPoint,
                              double)
{
  INIT_SUCC(ret);
  ObWkbGeogPoint geog_pt;
  const ObWkbGeogMultiPoint *geog_mpt = NULL;
  const ObWkbGeomPoint *cart_pt = reinterpret_cast<const ObWkbGeomPoint *>(g2->val());

  if (OB_FAIL(ObGeoFuncDistanceSphereUtil::reinterpret_as_degrees(cart_pt, geog_pt, result))) {
    LOG_WARN("fail to reinterpret point as degree", K(ret));
  } else if (OB_FAIL(ObGeoFuncDistanceSphereUtil::reinterpret_as_degrees(context.get_allocator(),
      g1, geog_mpt, result))) {
    LOG_WARN("fail to reinterpret multipoint as degree", K(ret));
  } else if (OB_FAIL(ObGeoFuncDistanceSphereUtil::eval(geog_mpt, &geog_pt,
      context, result))) {
    LOG_WARN("fail to eval distance sphere result", K(ret));
  }

  return ret;
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDistanceSphereImpl,
                              ObWkbGeomMultiPoint,
                              ObWkbGeomMultiPoint,
                              double)
{
  INIT_SUCC(ret);
  const ObWkbGeogMultiPoint *geog_mpt1 = NULL;
  const ObWkbGeogMultiPoint *geog_mpt2 = NULL;

  if (OB_FAIL(ObGeoFuncDistanceSphereUtil::reinterpret_as_degrees(context.get_allocator(),
      g1, geog_mpt1, result))) {
    LOG_WARN("fail to reinterpret multipoint as degree", K(ret));
  } else if (OB_FAIL(ObGeoFuncDistanceSphereUtil::reinterpret_as_degrees(context.get_allocator(),
      g2, geog_mpt2, result))) {
    LOG_WARN("fail to reinterpret multipoint as degree", K(ret));
  } else if (OB_FAIL(SMART_CALL(ObGeoFuncDistanceSphereUtil::eval(geog_mpt1, geog_mpt2,
      context, result)))) {
    LOG_WARN("fail to eval distance sphere result", K(ret));
  }

  return ret;
} OB_GEO_FUNC_END;

// geography
// inputs: (const ObGeometry *g1, const ObGeometry *g2, const ObGeoEvalCtx &context, double &result)
OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDistanceSphereImpl,
                              ObWkbGeogPoint,
                              ObWkbGeogPoint,
                              double)
{
  return ObGeoFuncDistanceSphereUtil::eval<ObWkbGeogPoint, ObWkbGeogPoint>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDistanceSphereImpl,
                              ObWkbGeogPoint,
                              ObWkbGeogMultiPoint,
                              double)
{
  return ObGeoFuncDistanceSphereUtil::eval<ObWkbGeogPoint, ObWkbGeogMultiPoint>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDistanceSphereImpl,
                              ObWkbGeogMultiPoint,
                              ObWkbGeogPoint,
                              double)
{
  return ObGeoFuncDistanceSphereUtil::eval<ObWkbGeogMultiPoint, ObWkbGeogPoint>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDistanceSphereImpl,
                              ObWkbGeogMultiPoint,
                              ObWkbGeogMultiPoint,
                              double)
{
  const ObWkbGeogMultiPoint *geog_mpt1 = reinterpret_cast<const ObWkbGeogMultiPoint *>(g1->val());
  const ObWkbGeogMultiPoint *geog_mpt2 = reinterpret_cast<const ObWkbGeogMultiPoint *>(g2->val());
  return SMART_CALL(ObGeoFuncDistanceSphereUtil::eval(geog_mpt1, geog_mpt2, context, result));
} OB_GEO_FUNC_END;

int ObGeoFuncDistanceSphere::eval(const ObGeoEvalCtx &gis_context, double &result)
{
  return ObGeoFuncDistanceSphereImpl::eval_geo_func(gis_context, result);
}

} // sql
} // oceanbase