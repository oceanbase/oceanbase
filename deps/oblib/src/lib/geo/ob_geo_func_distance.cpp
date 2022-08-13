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
 * This file contains implementation for ob_geo_func_distance.
 */

#define USING_LOG_PREFIX LIB

#include "lib/geo/ob_geo_dispatcher.h"
#include "lib/geo/ob_geo_func_distance.h"
#include "lib/geo/ob_geo_utils.h"
#include "lib/oblog/ob_log_module.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace common
{
namespace bg = boost::geometry;

template <typename GeoType1, typename GeoType2>
int eval_distance_without_strategy(const ObGeometry *g1, const ObGeometry *g2, double &result)
{
  const GeoType1 *geo1 = reinterpret_cast<const GeoType1 *>(g1->val());
  const GeoType2 *geo2 = reinterpret_cast<const GeoType2 *>(g2->val());
  result = bg::distance(*geo1, *geo2);
  return OB_SUCCESS;
}

template <typename GeoType1, typename GeoType2>
int eval_distance_with_point_strategy(const ObGeometry *g1,
                                      const ObGeometry *g2,
                                      const ObGeoEvalCtx &context,
                                      double &result)
{
  INIT_SUCC(ret);
  const ObSrsItem *srs = context.get_srs();
  if (OB_ISNULL(srs)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("srs is null", K(ret), K(g1->get_srid()), K(g1), K(g2));
  } else {
    bg::srs::spheroid<double> geog_sphere(srs->semi_major_axis(), srs->semi_minor_axis());
    bg::strategy::distance::andoyer<bg::srs::spheroid<double>> point_strategy(geog_sphere);

    const GeoType1 *geo1 = reinterpret_cast<const GeoType1 *>(g1->val());
    const GeoType2 *geo2 = reinterpret_cast<const GeoType2 *>(g2->val());
    result = bg::distance(*geo1, *geo2, point_strategy);
  }
  return ret;
}

template <typename GeoType1, typename GeoType2>
int eval_distance_with_nonpoint_strategy(const ObGeometry *g1,
                                         const ObGeometry *g2,
                                         const ObGeoEvalCtx &context,
                                         double &result)
{
  INIT_SUCC(ret);
  const ObSrsItem *srs = context.get_srs();
  if (OB_ISNULL(srs)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("srs is null", K(ret), K(g1->get_srid()), K(g1), K(g2));
  } else {
    bg::srs::spheroid<double> geog_sphere(srs->semi_major_axis(), srs->semi_minor_axis());
    bg::strategy::distance::geographic_cross_track<
      bg::strategy::andoyer, bg::srs::spheroid<double>, double>
      nonpoint_strategy(geog_sphere);

    const GeoType1 *geo1 = reinterpret_cast<const GeoType1 *>(g1->val());
    const GeoType2 *geo2 = reinterpret_cast<const GeoType2 *>(g2->val());
    result = bg::distance(*geo1, *geo2, nonpoint_strategy);
  }
  return ret;
}

// ----- ObGeoFuncDistanceImpl -----
class ObGeoFuncDistanceImpl : public ObIGeoDispatcher<double, ObGeoFuncDistanceImpl>
{
public:
  ObGeoFuncDistanceImpl();
  virtual ~ObGeoFuncDistanceImpl() = default;

  // template for unary
  OB_GEO_UNARY_FUNC_DEFAULT(double, OB_ERR_GIS_INVALID_DATA);
  OB_GEO_TREE_UNARY_FUNC_DEFAULT(double, OB_ERR_GIS_INVALID_DATA);;
  OB_GEO_CART_TREE_FUNC_DEFAULT(double, OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS);
  OB_GEO_GEOG_TREE_FUNC_DEFAULT(double, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);

  // template for binary
  // default cases for cartesian
  template <typename GeoType1, typename GeoType2>
  struct EvalWkbBi
  {
    static int eval(const ObGeometry *g1, const ObGeometry *g2, const ObGeoEvalCtx &context, double &result)
    {
      UNUSED(context);
      return eval_distance_without_strategy<GeoType1, GeoType2>(g1, g2, result);
    }
  };

  // default case for geography (calc using nonpoint_strategy)
  template <typename GeoType1, typename GeoType2>
  struct EvalWkbBiGeog
  {
    static int eval(const ObGeometry *g1, const ObGeometry *g2, const ObGeoEvalCtx &context, double &result)
    {
      return eval_distance_with_nonpoint_strategy<GeoType1, GeoType2>(g1, g2, context, result);
    }
  };

private:
  // geometry collection, distance to collection is the min distance to elements in collection
  template <typename CollectonType>
  static int eval_distance_geometry_collection(const ObGeometry *g1,
                                               const ObGeometry *g2,
                                               const ObGeoEvalCtx &context,
                                               double &result)
  {
    INIT_SUCC(ret);
    common::ObIAllocator *allocator = context.get_allocator();
    double min_dist = std::numeric_limits<double>::infinity();
    double temp_res = min_dist;

    if (OB_ISNULL(allocator)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Null allocator", K(ret));
    } else if (g1->type() == ObGeoType::GEOMETRYCOLLECTION) {
      const CollectonType *geo1 = reinterpret_cast<const CollectonType *>(g1->val());
      typename CollectonType::iterator iter = geo1->begin();
      for (; iter != geo1->end() && OB_SUCC(ret); iter++) {
        typename CollectonType::const_pointer sub_ptr = iter.operator->();
        ObGeoType sub_type = geo1->get_sub_type(sub_ptr);
        ObGeometry *sub_g1 = NULL;
        bool is_geog = (g1->crs() == oceanbase::common::ObGeoCRS::Geographic);
        if (OB_FAIL(ObGeoTypeUtil::create_geo_by_type(*allocator, sub_type, is_geog, true, sub_g1))) {
          LOG_WARN("failed to create wkb", K(ret), K(sub_type));
        } else {
          // Length is not used, cannot get real length until iter move to the next
          ObString wkb_nosrid(WKB_COMMON_WKB_HEADER_LEN, reinterpret_cast<const char *>(sub_ptr));
          sub_g1->set_data(wkb_nosrid);
          sub_g1->set_srid(g1->get_srid());
        }
        ret = eval_distance_geometry_collection<CollectonType>(sub_g1, g2, context, temp_res);
        if (OB_SUCC(ret)) {
          (temp_res < min_dist) ? (min_dist = temp_res) : 0;
        }
      }
    } else if (g2->type() == ObGeoType::GEOMETRYCOLLECTION) {
      const CollectonType *geo2 = reinterpret_cast<const CollectonType *>(g2->val());
      typename CollectonType::iterator iter = geo2->begin();
      for (; iter != geo2->end() && OB_SUCC(ret); iter++) {
        typename CollectonType::const_pointer sub_ptr = iter.operator->();
        ObGeoType sub_type = geo2->get_sub_type(sub_ptr);
        ObGeometry *sub_g2 = NULL;
        bool is_geog = (g2->crs() == oceanbase::common::ObGeoCRS::Geographic);
        if (OB_FAIL(ObGeoTypeUtil::create_geo_by_type(*allocator, sub_type, is_geog, true, sub_g2))) {
          LOG_WARN("failed to create wkb", K(ret), K(sub_type));
        } else {
          // Length is not used, cannot get real length until iter move to the next
          ObString wkb_nosrid(WKB_COMMON_WKB_HEADER_LEN, reinterpret_cast<const char *>(sub_ptr));
          sub_g2->set_data(wkb_nosrid);
          sub_g2->set_srid(g2->get_srid());
        }
        ret = eval_distance_geometry_collection<CollectonType>(g1, sub_g2, context, temp_res);
        if (OB_SUCC(ret)) {
          (temp_res < min_dist) ? (min_dist = temp_res) : 0;
        }
      }
    } else {
      // none of the two geometries are collection type
      ret = eval_wkb_binary(g1, g2, context, temp_res);
      if (OB_SUCC(ret)) {
        (temp_res < min_dist) ? (min_dist = temp_res) : 0;
      }
    }
    result = min_dist;
    return ret;
  }
};

// cartisan geometrycollection
OB_GEO_CART_BINARY_FUNC_GEO2_BEGIN(ObGeoFuncDistanceImpl, ObWkbGeomCollection, double)
{
  return eval_distance_geometry_collection<ObWkbGeomCollection>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_GEO1_BEGIN(ObGeoFuncDistanceImpl, ObWkbGeomCollection, double)
{
  return eval_distance_geometry_collection<ObWkbGeomCollection>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDistanceImpl, ObWkbGeomCollection, ObWkbGeomCollection, double)
{
  return eval_distance_geometry_collection<ObWkbGeomCollection>(g1, g2, context, result);
} OB_GEO_FUNC_END;

 // geography geometrycollection
OB_GEO_GEOG_BINARY_FUNC_GEO2_BEGIN(ObGeoFuncDistanceImpl, ObWkbGeogCollection, double)
{
  return eval_distance_geometry_collection<ObWkbGeogCollection>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_GEO1_BEGIN(ObGeoFuncDistanceImpl, ObWkbGeogCollection, double)
{
  return eval_distance_geometry_collection<ObWkbGeogCollection>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDistanceImpl, ObWkbGeogCollection, ObWkbGeogCollection, double)
{
  return eval_distance_geometry_collection<ObWkbGeogCollection>(g1, g2, context, result);
} OB_GEO_FUNC_END;

// geography cases use point strategy
OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDistanceImpl, ObWkbGeogPoint, ObWkbGeogPoint, double)
{
  return eval_distance_with_point_strategy<ObWkbGeogPoint, ObWkbGeogPoint>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDistanceImpl, ObWkbGeogPoint, ObWkbGeogMultiPoint, double)
{
  return eval_distance_with_point_strategy<ObWkbGeogPoint, ObWkbGeogMultiPoint>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDistanceImpl, ObWkbGeogMultiPoint, ObWkbGeogPoint, double)
{
  return eval_distance_with_point_strategy<ObWkbGeogMultiPoint, ObWkbGeogPoint>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDistanceImpl, ObWkbGeogMultiPoint, ObWkbGeogMultiPoint, double)
{
  return eval_distance_with_point_strategy<ObWkbGeogMultiPoint, ObWkbGeogMultiPoint>(g1, g2, context, result);
} OB_GEO_FUNC_END;

int ObGeoFuncDistance::eval(const ObGeoEvalCtx &gis_context, double &result)
{
  INIT_SUCC(ret);
  if (OB_SUCC(ObGeoFuncDistanceImpl::eval_geo_func(gis_context, result))) {
    if (!std::isfinite(result) || result < 0.0) {
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_WARN("invalid distance result", K(ret), K(result));
    }
  }
  return ret;
}

} // sql
} // oceanbase