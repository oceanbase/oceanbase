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
namespace bg = boost::geometry;

template <typename GeoType1, typename GeoType2>
int eval_disjoint_without_strategy(const ObGeometry *g1, const ObGeometry *g2, bool &result)
{
  const GeoType1 *geo1 = nullptr;
  const GeoType2 *geo2 = nullptr;
  if (!g1->is_tree()) {
    geo1 = reinterpret_cast<const GeoType1 *>(g1->val());
    geo2 = reinterpret_cast<const GeoType2 *>(g2->val());
  } else {
    geo1 = reinterpret_cast<const GeoType1 *>(g1);
    geo2 = reinterpret_cast<const GeoType2 *>(g2);
  }
  result = bg::disjoint(*geo1, *geo2);
  return OB_SUCCESS;
}

template<typename GeoType1, typename GeoType2, typename Strategy>
static int eval_disjoint_with_strategy(const ObGeometry *g1, const ObGeometry *g2, 
                                       const ObGeoEvalCtx &context, bool &result)
{
  INIT_SUCC(ret);
  const ObSrsItem *srs = context.get_srs();
  if (OB_ISNULL(srs)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("srs is null", K(ret), K(g1->get_srid()), K(g1), K(g2));
  } else {
    const GeoType1 *geo1 = NULL;
    const GeoType2 *geo2 = NULL;
    if (!g1->is_tree()) {
      geo1 = reinterpret_cast<const GeoType1 *>(g1->val());
      geo2 = reinterpret_cast<const GeoType2 *>(g2->val());
    } else {
      geo1 = reinterpret_cast<const GeoType1 *>(g1);
      geo2 = reinterpret_cast<const GeoType2 *>(g2);
    }

    bg::srs::spheroid<double> geog_sphere(srs->semi_major_axis(), 
                                          srs->semi_minor_axis());
    Strategy cur_strategy(geog_sphere);
    result = bg::disjoint(*geo1, *geo2, cur_strategy);
  }
  return ret;
}

// Notice: this is not the full implementation, only cases used by intersects are implemented

// ----- ObGeoFuncDisjointImpl -----
class ObGeoFuncDisjointImpl : public ObIGeoDispatcher<bool, ObGeoFuncDisjointImpl>
{
public:
  ObGeoFuncDisjointImpl();
  virtual ~ObGeoFuncDisjointImpl() = default;

  // defaults
  OB_GEO_UNARY_FUNC_DEFAULT(bool, OB_ERR_GIS_INVALID_DATA);
  OB_GEO_TREE_UNARY_FUNC_DEFAULT(bool, OB_ERR_GIS_INVALID_DATA);

  OB_GEO_CART_BINARY_FUNC_DEFAULT(bool, OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS);
  OB_GEO_GEOG_BINARY_FUNC_DEFAULT(bool, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);

  OB_GEO_CART_TREE_FUNC_DEFAULT(bool, OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS);
  OB_GEO_GEOG_TREE_FUNC_DEFAULT(bool, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);


private:
  // geometry collection
  template <typename CollectonType>
  static int eval_disjoint_gc(const ObGeometry *g1, const ObGeometry *g2,
                              const ObGeoEvalCtx &context, bool &result)
  {
    INIT_SUCC(ret);
    result = true;
    common::ObIAllocator *allocator = context.get_allocator();
    typename CollectonType::iterator iter;
    if (OB_ISNULL(allocator)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Null allocator", K(ret));
    } else if (g1->type() == ObGeoType::GEOMETRYCOLLECTION) {
      const CollectonType *geo1 = reinterpret_cast<const CollectonType *>(g1->val());
      iter = geo1->begin();
      // if result is false, the two geometries are intersects(not disjoint)
      for (; iter != geo1->end() && OB_SUCC(ret) && (result != false); iter++) {
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
        ret = eval_disjoint_gc<CollectonType>(sub_g1, g2, context, result);
      }
    } else if (g2->type() == ObGeoType::GEOMETRYCOLLECTION) {
      const CollectonType *geo2 = reinterpret_cast<const CollectonType *>(g2->val());
      iter = geo2->begin();
      // if result is false, the two geometries are intersects(not disjoint)
      for (; iter != geo2->end() && OB_SUCC(ret) && (result != false); iter++) {
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
        ret = eval_disjoint_gc<CollectonType>(g1, sub_g2, context, result);
      }
    } else {
      // none of the two geometries are collection type
      ret = eval_wkb_binary(g1, g2, context, result);
    }
    return ret;
  }

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