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
  result = boost::geometry::disjoint(*geo1, *geo2);
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

    boost::geometry::srs::spheroid<double> geog_sphere(srs->semi_major_axis(), 
                                          srs->semi_minor_axis());
    Strategy cur_strategy(geog_sphere);
    result = boost::geometry::disjoint(*geo1, *geo2, cur_strategy);
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

//////////////////////////////////////////////////////////////////////////////

// disjoint(Cartesian_point, *)
OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeomPoint, ObWkbGeomPoint, bool)
{
  return eval_disjoint_without_strategy<ObWkbGeomPoint, ObWkbGeomPoint>(g1, g2, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeomPoint, ObWkbGeomLineString, bool)
{
  return eval_disjoint_without_strategy<ObWkbGeomPoint, ObWkbGeomLineString>(g1, g2, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeomPoint, ObWkbGeomPolygon, bool)
{
  return eval_disjoint_without_strategy<ObWkbGeomPoint, ObWkbGeomPolygon>(g1, g2, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeomPoint, ObWkbGeomMultiPoint, bool)
{
  return eval_disjoint_without_strategy<ObWkbGeomPoint, ObWkbGeomMultiPoint>(g1, g2, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeomPoint, ObWkbGeomMultiLineString, bool)
{
  return eval_disjoint_without_strategy<ObWkbGeomPoint, ObWkbGeomMultiLineString>(g1, g2, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeomPoint, ObWkbGeomMultiPolygon, bool)
{
  return eval_disjoint_without_strategy<ObWkbGeomPoint, ObWkbGeomMultiPolygon>(g1, g2, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeomPoint, ObWkbGeomCollection, bool)
{
  return eval_disjoint_gc<ObWkbGeomCollection>(g1, g2, context, result);
} OB_GEO_FUNC_END;

//////////////////////////////////////////////////////////////////////////////

// disjoint(Cartesian_linestring, *)
OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeomLineString, ObWkbGeomPoint, bool)
{
  return eval_disjoint_without_strategy<ObWkbGeomLineString, ObWkbGeomPoint>(g1, g2, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeomLineString, ObWkbGeomLineString, bool)
{
  return eval_disjoint_without_strategy<ObWkbGeomLineString, ObWkbGeomLineString>(g1, g2, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeomLineString, ObWkbGeomPolygon, bool)
{
  return eval_disjoint_without_strategy<ObWkbGeomLineString, ObWkbGeomPolygon>(g1, g2, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeomLineString, ObWkbGeomMultiPoint, bool)
{
  return eval_disjoint_without_strategy<ObWkbGeomLineString, ObWkbGeomMultiPoint>(g1, g2, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeomLineString, ObWkbGeomMultiLineString, bool)
{
  return eval_disjoint_without_strategy<ObWkbGeomLineString, ObWkbGeomMultiLineString>(g1, g2, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeomLineString, ObWkbGeomMultiPolygon, bool)
{
  return eval_disjoint_without_strategy<ObWkbGeomLineString, ObWkbGeomMultiPolygon>(g1, g2, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeomLineString, ObWkbGeomCollection, bool)
{
  return eval_disjoint_gc<ObWkbGeomCollection>(g1, g2, context, result);
} OB_GEO_FUNC_END;

//////////////////////////////////////////////////////////////////////////////

// disjoint(Cartesian_polygon, *)
OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeomPolygon, ObWkbGeomPoint, bool)
{
  return eval_disjoint_without_strategy<ObWkbGeomPolygon, ObWkbGeomPoint>(g1, g2, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeomPolygon, ObWkbGeomLineString, bool)
{
  return eval_disjoint_without_strategy<ObWkbGeomPolygon, ObWkbGeomLineString>(g1, g2, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeomPolygon, ObWkbGeomPolygon, bool)
{
  return eval_disjoint_without_strategy<ObWkbGeomPolygon, ObWkbGeomPolygon>(g1, g2, result);
} OB_GEO_FUNC_END;

// ObWkbGeomPolygon - ObWkbGeomMultiPoint

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeomPolygon, ObWkbGeomMultiLineString, bool)
{
  return eval_disjoint_without_strategy<ObWkbGeomPolygon, ObWkbGeomMultiLineString>(g1, g2, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeomPolygon, ObWkbGeomMultiPolygon, bool)
{
  return eval_disjoint_without_strategy<ObWkbGeomPolygon, ObWkbGeomMultiPolygon>(g1, g2, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeomPolygon, ObWkbGeomCollection, bool)
{
  return eval_disjoint_gc<ObWkbGeomCollection>(g1, g2, context, result);
} OB_GEO_FUNC_END;

//////////////////////////////////////////////////////////////////////////////

// disjoint(Cartesian_geometrycollection, *)
OB_GEO_CART_BINARY_FUNC_GEO1_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeomCollection, bool)
{
  return eval_disjoint_gc<ObWkbGeomCollection>(g1, g2, context, result);
} OB_GEO_FUNC_END;

//////////////////////////////////////////////////////////////////////////////

// disjoint(Cartesian_multipoint, *)
OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeomMultiPoint, ObWkbGeomPoint, bool)
{
  return eval_disjoint_without_strategy<ObWkbGeomMultiPoint, ObWkbGeomPoint>(g1, g2, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeomMultiPoint, ObWkbGeomLineString, bool)
{
  return eval_disjoint_without_strategy<ObWkbGeomMultiPoint, ObWkbGeomLineString>(g1, g2, result);
} OB_GEO_FUNC_END;

// ObWkbGeomMultiPoint - ObWkbGeomPolygon

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeomMultiPoint, ObWkbGeomMultiPoint, bool)
{
  return eval_disjoint_without_strategy<ObWkbGeomMultiPoint, ObWkbGeomMultiPoint>(g1, g2, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeomMultiPoint, ObWkbGeomMultiLineString, bool)
{
  return eval_disjoint_without_strategy<ObWkbGeomMultiPoint, ObWkbGeomMultiLineString>(g1, g2, result);
} OB_GEO_FUNC_END;

// ObWkbGeomMultiPoint - ObWkbGeomMultiPolygon

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeomMultiPoint, ObWkbGeomCollection, bool)
{
  return eval_disjoint_gc<ObWkbGeomCollection>(g1, g2, context, result);
} OB_GEO_FUNC_END;

//////////////////////////////////////////////////////////////////////////////

// disjoint(Cartesian_multilinestring, *)
OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeomMultiLineString, ObWkbGeomPoint, bool)
{
  return eval_disjoint_without_strategy<ObWkbGeomMultiLineString, ObWkbGeomPoint>(g1, g2, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeomMultiLineString, ObWkbGeomLineString, bool)
{
  return eval_disjoint_without_strategy<ObWkbGeomMultiLineString, ObWkbGeomLineString>(g1, g2, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeomMultiLineString, ObWkbGeomPolygon, bool)
{
  return eval_disjoint_without_strategy<ObWkbGeomMultiLineString, ObWkbGeomPolygon>(g1, g2, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeomMultiLineString, ObWkbGeomMultiPoint, bool)
{
  return eval_disjoint_without_strategy<ObWkbGeomMultiLineString, ObWkbGeomMultiPoint>(g1, g2, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeomMultiLineString, ObWkbGeomMultiLineString, bool)
{
  return eval_disjoint_without_strategy<ObWkbGeomMultiLineString, ObWkbGeomMultiLineString>(g1, g2, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeomMultiLineString, ObWkbGeomMultiPolygon, bool)
{
  return eval_disjoint_without_strategy<ObWkbGeomMultiLineString, ObWkbGeomMultiPolygon>(g1, g2, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeomMultiLineString, ObWkbGeomCollection, bool)
{
  return eval_disjoint_gc<ObWkbGeomCollection>(g1, g2, context, result);
} OB_GEO_FUNC_END;

//////////////////////////////////////////////////////////////////////////////

// disjoint(Cartesian_multipolygon, *)
OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeomMultiPolygon, ObWkbGeomPoint, bool)
{
  return eval_disjoint_without_strategy<ObWkbGeomMultiPolygon, ObWkbGeomPoint>(g1, g2, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeomMultiPolygon, ObWkbGeomLineString, bool)
{
  return eval_disjoint_without_strategy<ObWkbGeomMultiPolygon, ObWkbGeomLineString>(g1, g2, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeomMultiPolygon, ObWkbGeomPolygon, bool)
{
  return eval_disjoint_without_strategy<ObWkbGeomMultiPolygon, ObWkbGeomPolygon>(g1, g2, result);
} OB_GEO_FUNC_END;

// ObWkbGeomMultiPolygon - ObWkbGeomMultiPoint

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeomMultiPolygon, ObWkbGeomMultiLineString, bool)
{
  return eval_disjoint_without_strategy<ObWkbGeomMultiPolygon, ObWkbGeomMultiLineString>(g1, g2, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeomMultiPolygon, ObWkbGeomMultiPolygon, bool)
{
  return eval_disjoint_without_strategy<ObWkbGeomMultiPolygon, ObWkbGeomMultiPolygon>(g1, g2, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeomMultiPolygon, ObWkbGeomCollection, bool)
{
  return eval_disjoint_gc<ObWkbGeomCollection>(g1, g2, context, result);
} OB_GEO_FUNC_END;

//////////////////////////////////////////////////////////////////////////////

// disjoint(Geographic_point, *)
OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeogPoint, ObWkbGeogPoint, bool)
{
  return eval_disjoint_without_strategy<ObWkbGeogPoint, ObWkbGeogPoint>(g1, g2, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeogPoint, ObWkbGeogLineString, bool)
{
  return eval_disjoint_with_strategy<ObWkbGeogPoint, ObWkbGeogLineString, ObPlPaStrategy>(
      g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeogPoint, ObWkbGeogPolygon, bool)
{
  return eval_disjoint_with_strategy<ObWkbGeogPoint, ObWkbGeogPolygon, ObPlPaStrategy>(
      g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeogPoint, ObWkbGeogMultiPoint, bool)
{
  // Default strategy is OK. P/P computations do not depend on shape of ellipsoid.
  return eval_disjoint_without_strategy<ObWkbGeogPoint, ObWkbGeogMultiPoint>(g1, g2, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeogPoint, ObWkbGeogMultiLineString, bool)
{
  return eval_disjoint_with_strategy<ObWkbGeogPoint, ObWkbGeogMultiLineString, ObPlPaStrategy>(
      g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeogPoint, ObWkbGeogMultiPolygon, bool)
{
  return eval_disjoint_with_strategy<ObWkbGeogPoint, ObWkbGeogMultiPolygon, ObPlPaStrategy>(
      g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeogPoint, ObWkbGeogCollection, bool)
{
  return eval_disjoint_gc<ObWkbGeogCollection>(g1, g2, context, result);
} OB_GEO_FUNC_END;

//////////////////////////////////////////////////////////////////////////////

// disjoint(Geographic_linestring, *)
OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeogLineString, ObWkbGeogPoint, bool)
{
  return eval_disjoint_with_strategy<ObWkbGeogLineString, ObWkbGeogPoint, ObPlPaStrategy>(
      g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeogLineString, ObWkbGeogLineString, bool)
{
  return eval_disjoint_with_strategy<ObWkbGeogLineString, ObWkbGeogLineString, ObLlLaAaStrategy>(
      g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeogLineString, ObWkbGeogPolygon, bool)
{
  return eval_disjoint_with_strategy<ObWkbGeogLineString, ObWkbGeogPolygon, ObLlLaAaStrategy>(
      g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeogLineString, ObWkbGeogMultiPoint, bool)
{
  return eval_disjoint_with_strategy<ObWkbGeogLineString, ObWkbGeogMultiPoint, ObPlPaStrategy>(
      g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeogLineString, ObWkbGeogMultiLineString, bool)
{
  return eval_disjoint_with_strategy<ObWkbGeogLineString, ObWkbGeogMultiLineString, ObLlLaAaStrategy>(
      g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeogLineString, ObWkbGeogMultiPolygon, bool)
{
  return eval_disjoint_with_strategy<ObWkbGeogLineString, ObWkbGeogMultiPolygon, ObLlLaAaStrategy>(
      g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeogLineString, ObWkbGeogCollection, bool)
{
  return eval_disjoint_gc<ObWkbGeogCollection>(g1, g2, context, result);
} OB_GEO_FUNC_END;

//////////////////////////////////////////////////////////////////////////////

// disjoint(Geographic_polygon, *)
OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeogPolygon, ObWkbGeogPoint, bool)
{
  return eval_disjoint_with_strategy<ObWkbGeogPolygon, ObWkbGeogPoint, ObPlPaStrategy>(
      g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeogPolygon, ObWkbGeogLineString, bool)
{
  return eval_disjoint_with_strategy<ObWkbGeogPolygon, ObWkbGeogLineString, ObLlLaAaStrategy>(
      g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeogPolygon, ObWkbGeogPolygon, bool)
{
  return eval_disjoint_with_strategy<ObWkbGeogPolygon, ObWkbGeogPolygon, ObLlLaAaStrategy>(
      g1, g2, context, result);
} OB_GEO_FUNC_END;

// ObWkbGeogPolygon - ObWkbGeogMultiPoint

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeogPolygon, ObWkbGeogMultiLineString, bool)
{
  return eval_disjoint_with_strategy<ObWkbGeogPolygon, ObWkbGeogMultiLineString, ObLlLaAaStrategy>(
      g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeogPolygon, ObWkbGeogMultiPolygon, bool)
{
  return eval_disjoint_with_strategy<ObWkbGeogPolygon, ObWkbGeogMultiPolygon, ObLlLaAaStrategy>(
      g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeogPolygon, ObWkbGeogCollection, bool)
{
  return eval_disjoint_gc<ObWkbGeogCollection>(g1, g2, context, result);
} OB_GEO_FUNC_END;

//////////////////////////////////////////////////////////////////////////////

// disjoint(Geographic_geometrycollection, *)
OB_GEO_GEOG_BINARY_FUNC_GEO1_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeogCollection, bool)
{
  return eval_disjoint_gc<ObWkbGeogCollection>(g1, g2, context, result);
} OB_GEO_FUNC_END;

//////////////////////////////////////////////////////////////////////////////

// disjoint(Geographic_multipoint, *)
OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeogMultiPoint, ObWkbGeogPoint, bool)
{
  // Default strategy is OK. P/P computations do not depend on shape of ellipsoid.
  return eval_disjoint_without_strategy<ObWkbGeogMultiPoint, ObWkbGeogPoint>(g1, g2, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeogMultiPoint, ObWkbGeogLineString, bool)
{
  return eval_disjoint_with_strategy<ObWkbGeogMultiPoint, ObWkbGeogLineString, ObPlPaStrategy>(
      g1, g2, context, result);
} OB_GEO_FUNC_END;

// ObWkbGeogMultiPoint - ObWkbGeogPolygon

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeogMultiPoint, ObWkbGeogMultiPoint, bool)
{
  // Default strategy is OK. P/P computations do not depend on shape of ellipsoid.
  return eval_disjoint_without_strategy<ObWkbGeogMultiPoint, ObWkbGeogMultiPoint>(g1, g2, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeogMultiPoint, ObWkbGeogMultiLineString, bool)
{
  return eval_disjoint_with_strategy<ObWkbGeogMultiPoint, ObWkbGeogMultiLineString, ObPlPaStrategy>(
      g1, g2, context, result);
} OB_GEO_FUNC_END;

// ObWkbGeogMultiPoint - ObWkbGeogMultiPolygon

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeogMultiPoint, ObWkbGeogCollection, bool)
{
  return eval_disjoint_gc<ObWkbGeogCollection>(g1, g2, context, result);
} OB_GEO_FUNC_END;

//////////////////////////////////////////////////////////////////////////////

// disjoint(Geographic_multilinestring, *)
OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeogMultiLineString, ObWkbGeogPoint, bool)
{
  return eval_disjoint_with_strategy<ObWkbGeogMultiLineString, ObWkbGeogPoint, ObPlPaStrategy>(
      g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeogMultiLineString, ObWkbGeogLineString, bool)
{
  return eval_disjoint_with_strategy<ObWkbGeogMultiLineString, ObWkbGeogLineString, ObLlLaAaStrategy>(
      g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeogMultiLineString, ObWkbGeogPolygon, bool)
{
  return eval_disjoint_with_strategy<ObWkbGeogMultiLineString, ObWkbGeogPolygon, ObLlLaAaStrategy>(
      g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeogMultiLineString, ObWkbGeogMultiPoint, bool)
{
  return eval_disjoint_with_strategy<ObWkbGeogMultiLineString, ObWkbGeogMultiPoint, ObPlPaStrategy>(
      g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeogMultiLineString, ObWkbGeogMultiLineString, bool)
{
  return eval_disjoint_with_strategy<ObWkbGeogMultiLineString, ObWkbGeogMultiLineString, ObLlLaAaStrategy>(
      g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeogMultiLineString, ObWkbGeogMultiPolygon, bool)
{
  return eval_disjoint_with_strategy<ObWkbGeogMultiLineString, ObWkbGeogMultiPolygon, ObLlLaAaStrategy>(
      g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeogMultiLineString, ObWkbGeogCollection, bool)
{
  return eval_disjoint_gc<ObWkbGeogCollection>(g1, g2, context, result);
} OB_GEO_FUNC_END;

//////////////////////////////////////////////////////////////////////////////

// disjoint(Geographic_multipolygon, *)
OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeogMultiPolygon, ObWkbGeogPoint, bool)
{
  return eval_disjoint_with_strategy<ObWkbGeogMultiPolygon, ObWkbGeogPoint, ObPlPaStrategy>(
      g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeogMultiPolygon, ObWkbGeogLineString, bool)
{
  return eval_disjoint_with_strategy<ObWkbGeogMultiPolygon, ObWkbGeogLineString, ObLlLaAaStrategy>(
      g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeogMultiPolygon, ObWkbGeogPolygon, bool)
{
  return eval_disjoint_with_strategy<ObWkbGeogMultiPolygon, ObWkbGeogPolygon, ObLlLaAaStrategy>(
      g1, g2, context, result);
} OB_GEO_FUNC_END;

// ObWkbGeogMultiPolygon - ObWkbGeogMultiPoint

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeogMultiPolygon, ObWkbGeogMultiLineString, bool)
{
  return eval_disjoint_with_strategy<ObWkbGeogMultiPolygon, ObWkbGeogMultiLineString, ObLlLaAaStrategy>(
      g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeogMultiPolygon, ObWkbGeogMultiPolygon, bool)
{
  return eval_disjoint_with_strategy<ObWkbGeogMultiPolygon, ObWkbGeogMultiPolygon, ObLlLaAaStrategy>(
      g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDisjointImpl, ObWkbGeogMultiPolygon, ObWkbGeogCollection, bool)
{
  return eval_disjoint_gc<ObWkbGeogCollection>(g1, g2, context, result);
} OB_GEO_FUNC_END;

//////////////////////////////////////////////////////////////////////////////

int ObGeoFuncDisjoint::eval(const ObGeoEvalCtx &gis_context, bool &result)
{
  return ObGeoFuncDisjointImpl::eval_geo_func(gis_context, result);
}

} // sql
} // oceanbase