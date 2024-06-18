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
#include "lib/geo/ob_geo_func_intersects.h"
#include "lib/geo/ob_geo_func_disjoint.h"
#include "lib/geo/ob_geo_utils.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace common
{
namespace bg = boost::geometry;

int eval_intersects_by_disjoint(const ObGeometry *g1, const ObGeometry *g2, const ObGeoEvalCtx &context, bool &result)
{
  INIT_SUCC(ret);
  result = false;
  // Notice:should not use geoemtry from context, they are the original inputs, maybe changed
  ObGeoEvalCtx disjoint_context(context.get_allocator(), context.get_srs());
  disjoint_context.append_geo_arg(g1);
  disjoint_context.append_geo_arg(g2);
  if (OB_SUCC(ObGeoFuncDisjoint::eval(disjoint_context, result))) {
    result = !result;
  } else {
    LOG_WARN("eval disjoint for intersects failed", K(ret));
  }
  return ret;
}

template <typename GeoType1, typename GeoType2>
int eval_intersects_without_strategy(const ObGeometry *g1, const ObGeometry *g2, bool &result)
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
  result = bg::intersects(*geo1, *geo2);
  return OB_SUCCESS;
}

template <typename GeoType1, typename GeoType2>
int eval_intersects_with_point_strategy(const ObGeometry *g1,
                                        const ObGeometry *g2,
                                        const ObGeoEvalCtx &context,
                                        bool &result)
{
  INIT_SUCC(ret);
  const ObSrsItem *srs = context.get_srs();
  if (OB_ISNULL(srs)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("srs is null", K(ret), K(g1->get_srid()), K(g1), K(g2));
  } else {
    bg::srs::spheroid<double> geog_sphere(srs->semi_major_axis(), srs->semi_minor_axis());
    bg::strategy::within::geographic_winding<ObWkbGeogPoint> point_strategy(geog_sphere);
    const GeoType1 *geo1 = reinterpret_cast<const GeoType1 *>(g1->val());
    const GeoType2 *geo2 = reinterpret_cast<const GeoType2 *>(g2->val());
#ifdef USE_SPHERE_GEO
    result = bg::intersects(*geo1, *geo2, point_strategy);
#else
    result = bg::intersects(*geo1, *geo2);
#endif
  }
  return ret;
}

template <typename GeoType1, typename GeoType2>
int eval_intersects_with_nonpoint_strategy(const ObGeometry *g1,
                                          const ObGeometry *g2,
                                          const ObGeoEvalCtx &context,
                                          bool &result)
{
  INIT_SUCC(ret);
  const ObSrsItem *srs = context.get_srs();
  if (OB_ISNULL(srs)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("srs is null", K(ret), K(g1->get_srid()), K(g1), K(g2));
  } else {
    bg::srs::spheroid<double> geog_sphere(srs->semi_major_axis(), srs->semi_minor_axis());
    bg::strategy::intersection::geographic_segments<> nonpoint_strategy(geog_sphere);
    const GeoType1 *geo1 = reinterpret_cast<const GeoType1 *>(g1->val());
    const GeoType2 *geo2 = reinterpret_cast<const GeoType2 *>(g2->val());
#ifdef USE_SPHERE_GEO
    result = bg::intersects(*geo1, *geo2, nonpoint_strategy);
#else
    result = bg::intersects(*geo1, *geo2);
#endif
  }
  return ret;
}

// ----- ObGeoFuncIntersectsImpl -----
class ObGeoFuncIntersectsImpl : public ObIGeoDispatcher<bool, ObGeoFuncIntersectsImpl>
{
public:
  ObGeoFuncIntersectsImpl();
  virtual ~ObGeoFuncIntersectsImpl() = default;

  // template for unary
  OB_GEO_UNARY_FUNC_DEFAULT(bool, OB_ERR_GIS_INVALID_DATA);
  OB_GEO_TREE_UNARY_FUNC_DEFAULT(bool, OB_ERR_GIS_INVALID_DATA);
  OB_GEO_CART_TREE_FUNC_DEFAULT(bool, OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS);
  OB_GEO_GEOG_TREE_FUNC_DEFAULT(bool, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);

  // template for binary
  // default cases for cartesian
  template <typename GeoType1, typename GeoType2>
  struct EvalWkbBi
  {
    static int eval(const ObGeometry *g1, const ObGeometry *g2, const ObGeoEvalCtx &context, bool &result)
    {
      UNUSED(context);
      return eval_intersects_without_strategy<GeoType1, GeoType2>(g1, g2, result);
    }
  };

  // default case for geography (calc using nonpoint_strategy)
  template <typename GeoType1, typename GeoType2>
  struct EvalWkbBiGeog
  {
    static int eval(const ObGeometry *g1, const ObGeometry *g2, const ObGeoEvalCtx &context, bool &result)
    {
      return eval_intersects_with_nonpoint_strategy<GeoType1, GeoType2>(g1, g2, context, result);
    }
  };

private:
  // geometry collection
  template <typename CollectonType>
  static int eval_intersects_geometry_collection(const ObGeometry *g1,
                                                 const ObGeometry *g2,
                                                 const ObGeoEvalCtx &context,
                                                 bool &result)
  {
    INIT_SUCC(ret);
    result = false;
    common::ObIAllocator *allocator = context.get_allocator();
    typename CollectonType::iterator iter;
    if (OB_ISNULL(allocator)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Null allocator", K(ret));
    } else if (g1->type() == ObGeoType::GEOMETRYCOLLECTION) {
      const CollectonType *geo1 = reinterpret_cast<const CollectonType *>(g1->val());
      iter = geo1->begin();
      // if result is true, the two geometries are intersects no need to process remaining sub geometries
      for (; iter != geo1->end() && OB_SUCC(ret) && (result != true); iter++) {
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
        ret = eval_intersects_geometry_collection<CollectonType>(sub_g1, g2, context, result);
      }
    } else if (g2->type() == ObGeoType::GEOMETRYCOLLECTION) {
      const CollectonType *geo2 = reinterpret_cast<const CollectonType *>(g2->val());
      iter = geo2->begin();
      // if result is true, the two geometries are intersects no need to process remaining sub geometries
      for (; iter != geo2->end() && OB_SUCC(ret) && (result != true); iter++) {
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
        ret = eval_intersects_geometry_collection<CollectonType>(g1, sub_g2, context, result);
      }
    } else {
      // none of the two geometries are collection type
      ret = eval_wkb_binary(g1, g2, context, result);
    }
    return ret;
  }
};

// geometrycollection
OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncIntersectsImpl, ObWkbGeomCollection, ObWkbGeomCollection, bool)
{
  return eval_intersects_geometry_collection<ObWkbGeomCollection>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_GEO2_BEGIN(ObGeoFuncIntersectsImpl, ObWkbGeomCollection, bool)
{
  return eval_intersects_geometry_collection<ObWkbGeomCollection>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_GEO1_BEGIN(ObGeoFuncIntersectsImpl, ObWkbGeomCollection, bool)
{
  return eval_intersects_geometry_collection<ObWkbGeomCollection>(g1, g2, context, result);
} OB_GEO_FUNC_END;

// cases use disjoint (multi point cases)
OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncIntersectsImpl, ObWkbGeomMultiPoint, ObWkbGeomPolygon, bool)
{
  return eval_intersects_by_disjoint(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncIntersectsImpl, ObWkbGeomMultiPoint, ObWkbGeomMultiPolygon, bool)
{
  return eval_intersects_by_disjoint(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncIntersectsImpl, ObWkbGeomPolygon, ObWkbGeomMultiPoint, bool)
{
  return eval_intersects_by_disjoint(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncIntersectsImpl, ObWkbGeomMultiPolygon, ObWkbGeomMultiPoint, bool)
{
  return eval_intersects_by_disjoint(g1, g2, context, result);
} OB_GEO_FUNC_END;


 // geometrycollection for geography
OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncIntersectsImpl, ObWkbGeogCollection, ObWkbGeogCollection, bool)
{
  return eval_intersects_geometry_collection<ObWkbGeogCollection>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_GEO2_BEGIN(ObGeoFuncIntersectsImpl, ObWkbGeogCollection, bool)
{
  return eval_intersects_geometry_collection<ObWkbGeogCollection>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_GEO1_BEGIN(ObGeoFuncIntersectsImpl, ObWkbGeogCollection, bool)
{
  return eval_intersects_geometry_collection<ObWkbGeogCollection>(g1, g2, context, result);
} OB_GEO_FUNC_END;

// cases use disjoint (multi point and polygons)
OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncIntersectsImpl, ObWkbGeogMultiPoint, ObWkbGeogPolygon, bool)
{
  return eval_intersects_by_disjoint(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncIntersectsImpl, ObWkbGeogMultiPoint, ObWkbGeogMultiPolygon, bool)
{
  return eval_intersects_by_disjoint(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncIntersectsImpl, ObWkbGeogPolygon, ObWkbGeogMultiPoint, bool)
{
  return eval_intersects_by_disjoint(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncIntersectsImpl, ObWkbGeogMultiPolygon, ObWkbGeogMultiPoint, bool)
{
  return eval_intersects_by_disjoint(g1, g2, context, result);
} OB_GEO_FUNC_END;

// geograpyic cases not using strategy (point and multi point)
OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncIntersectsImpl, ObWkbGeogPoint, ObWkbGeogPoint, bool)
{
  UNUSED(context);
  return eval_intersects_without_strategy<ObWkbGeogPoint, ObWkbGeogPoint>(g1, g2, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncIntersectsImpl, ObWkbGeogMultiPoint, ObWkbGeogMultiPoint, bool)
{
  UNUSED(context);
  return eval_intersects_without_strategy<ObWkbGeogMultiPoint, ObWkbGeogMultiPoint>(g1, g2, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncIntersectsImpl, ObWkbGeogPoint, ObWkbGeogMultiPoint, bool)
{
  UNUSED(context);
  return eval_intersects_without_strategy<ObWkbGeogPoint, ObWkbGeogMultiPoint>(g1, g2, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncIntersectsImpl, ObWkbGeogMultiPoint, ObWkbGeogPoint, bool)
{
  UNUSED(context);
  return eval_intersects_without_strategy<ObWkbGeogMultiPoint, ObWkbGeogPoint>(g1, g2, result);
} OB_GEO_FUNC_END;

// geograpyic cases using point strategy (point and nonpoint types)
OB_GEO_GEOG_BINARY_FUNC_GEO2_BEGIN(ObGeoFuncIntersectsImpl, ObWkbGeogPoint, bool)
{
  return eval_intersects_with_point_strategy<GeoType1, ObWkbGeogPoint>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_GEO1_BEGIN(ObGeoFuncIntersectsImpl, ObWkbGeogPoint, bool)
{
  return eval_intersects_with_point_strategy<ObWkbGeogPoint, GeoType2>(g1, g2, context, result);
} OB_GEO_FUNC_END;

// geograpyic cases using point strategy (multipoints and line types)
OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncIntersectsImpl, ObWkbGeogMultiPoint, ObWkbGeogLineString, bool)
{
  return eval_intersects_with_point_strategy<ObWkbGeogMultiPoint, ObWkbGeogLineString>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncIntersectsImpl, ObWkbGeogLineString, ObWkbGeogMultiPoint, bool)
{
  return eval_intersects_with_point_strategy<ObWkbGeogLineString, ObWkbGeogMultiPoint>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncIntersectsImpl, ObWkbGeogMultiPoint, ObWkbGeogMultiLineString, bool)
{
  return eval_intersects_with_point_strategy<ObWkbGeogMultiPoint, ObWkbGeogMultiLineString>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncIntersectsImpl, ObWkbGeogMultiLineString, ObWkbGeogMultiPoint, bool)
{
  return eval_intersects_with_point_strategy<ObWkbGeogMultiLineString, ObWkbGeogMultiPoint>(g1, g2, context, result);
} OB_GEO_FUNC_END;

// handle ambiguous partial specializations
OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncIntersectsImpl, ObWkbGeogCollection, ObWkbGeogPoint, bool)
{
  return eval_intersects_geometry_collection<ObWkbGeogCollection>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncIntersectsImpl, ObWkbGeogPoint, ObWkbGeogCollection, bool)
{
  return eval_intersects_geometry_collection<ObWkbGeogCollection>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_TREE_FUNC_BEGIN(ObGeoFuncIntersectsImpl, ObCartesianPolygon, ObCartesianPolygon, bool)
{
  UNUSED(context);
  return eval_intersects_without_strategy<ObCartesianPolygon, ObCartesianPolygon>(g1, g2, result);
} OB_GEO_FUNC_END;
// detect polygon self-intersection
OB_GEO_UNARY_TREE_FUNC_BEGIN(ObGeoFuncIntersectsImpl, ObCartesianPolygon, bool)
{
  UNUSED(context);
  const ObCartesianPolygon *geo = reinterpret_cast<const ObCartesianPolygon *>(g);
  result = bg::intersects(*geo);
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

int ObGeoFuncIntersects::eval(const ObGeoEvalCtx &gis_context, bool &result)
{
  return ObGeoFuncIntersectsImpl::eval_geo_func(gis_context, result);
}

} // sql
} // oceanbase