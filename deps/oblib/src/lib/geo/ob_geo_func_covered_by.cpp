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
 * This file contains implementation for ob_geo_func_covered_by.
 */

#define USING_LOG_PREFIX LIB

#include "lib/geo/ob_geo_dispatcher.h"
#include "lib/geo/ob_geo_func_covered_by.h"
#include "lib/geo/ob_geo_func_utils.h"
#include "lib/geo/ob_geo_bin_traits.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace common
{

template<typename GeoType1, typename GeoType2>
static bool ob_apply_bg_covered_by(const ObGeometry *g1, const ObGeometry *g2)
{
  const GeoType1 *geo1 = reinterpret_cast<const GeoType1 *>(g1->val());
  const GeoType2 *geo2 = reinterpret_cast<const GeoType2 *>(g2->val());
  return boost::geometry::covered_by(*geo1, *geo2);
}

template<typename GeoType1, typename GeoType2>
static bool ob_apply_bg_covered_by_with_pl_strategy(const ObGeometry *g1, const ObGeometry *g2,
                                                    const ObGeoEvalCtx &context)
{
  const GeoType1 *geo1 = reinterpret_cast<const GeoType1 *>(g1->val());
  const GeoType2 *geo2 = reinterpret_cast<const GeoType2 *>(g2->val());
  const ObSrsItem *srs = context.get_srs();
  boost::geometry::srs::spheroid<double> geog_sphere(srs->semi_major_axis(), srs->semi_minor_axis());
  ObPlPaStrategy point_strategy(geog_sphere);
#ifdef USE_SPHERE_GEO
  return boost::geometry::covered_by(*geo1, *geo2, point_strategy);
#else
  return boost::geometry::covered_by(*geo1, *geo2);
#endif
}

template<typename GeoType1, typename GeoType2>
static bool ob_apply_bg_covered_by_with_ll_strategy(const ObGeometry *g1, const ObGeometry *g2,
                                                    const ObGeoEvalCtx &context)
{
  const GeoType1 *geo1 = reinterpret_cast<const GeoType1 *>(g1->val());
  const GeoType2 *geo2 = reinterpret_cast<const GeoType2 *>(g2->val());
  const ObSrsItem *srs = context.get_srs();
  boost::geometry::srs::spheroid<double> geog_sphere(srs->semi_major_axis(), srs->semi_minor_axis());
  ObLlLaAaStrategy line_strategy(geog_sphere);
#ifdef USE_SPHERE_GEO
  return boost::geometry::covered_by(*geo1, *geo2, line_strategy);
#else
  return boost::geometry::covered_by(*geo1, *geo2);
#endif
}

// ----- ObGeoFuncCoveredByImpl -----
class ObGeoFuncCoveredByImpl : public ObIGeoDispatcher<bool, ObGeoFuncCoveredByImpl>
{
public:
  ObGeoFuncCoveredByImpl();
  virtual ~ObGeoFuncCoveredByImpl() = default;

  // defaults
  OB_GEO_UNARY_FUNC_DEFAULT(bool, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);
  OB_GEO_TREE_UNARY_FUNC_DEFAULT(bool, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);
  OB_GEO_CART_TREE_FUNC_DEFAULT(bool, OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS);
  OB_GEO_GEOG_TREE_FUNC_DEFAULT(bool, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);

  template<typename PointType, typename IPointType, typename GCType>
  static int ob_caculate_mp_gc_cover_by(const ObGeometry *g1, const ObGeometry *g2,
                                        const ObGeoEvalCtx &context, bool &result)
  {
    INIT_SUCC(ret);
    const typename GCType::sub_mpt_type *geo1 = reinterpret_cast<const typename GCType::sub_mpt_type *>(g1->val());
    // travel every point in multipoint; check if any one is not covered by geo2(postgis)
    FOREACH_X(item, *geo1, (OB_SUCC(ret))) {
      PointType point;
      point.byteorder(ObGeoWkbByteOrder::LittleEndian);
      point.template set<0>(item->template get<0>());
      point.template set<1>(item->template get<1>());
      ObString data(sizeof(PointType), reinterpret_cast<char *>(&point));
      IPointType i_point;
      i_point.set_data(data);
      if (g2->crs() == ObGeoCRS::Cartesian) {
        ret = EvalWkbBi<PointType, GCType>::eval(&i_point, g2, context, result);
      } else {
        ret = EvalWkbBiGeog<PointType, GCType>::eval(&i_point, g2, context, result);
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to do point_gc covered by functor", K(ret));
      } else if (!result) {
        break;
      }
    }
    return ret;
  }

  template <typename GeoType1, typename GeoType2>
  struct EvalWkbBi
  {
    static int eval(const ObGeometry *g1, const ObGeometry *g2, const ObGeoEvalCtx &context, bool &result)
    {
      UNUSEDx(g1, g2, context);
      result = false;
      return OB_SUCCESS;
    }
  };

  template <typename GeoType1, typename GeoType2>
  struct EvalWkbBiGeog
  {
    static int eval(const ObGeometry *g1, const ObGeometry *g2, const ObGeoEvalCtx &context, bool &result)
    {
      UNUSEDx(g1, g2, context);
      result = false;
      return OB_SUCCESS;
    }
  };

};

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncCoveredByImpl, ObWkbGeomPoint, ObWkbGeomCollection, bool)
{
  INIT_SUCC(ret);
  const ObWkbGeomPoint *geo1 = reinterpret_cast<const ObWkbGeomPoint *>(g1->val());
  const ObWkbGeomCollection *geo2 = reinterpret_cast<const ObWkbGeomCollection *>(g2->val());
  ObWkbGeomCollection::iterator iter = geo2->begin();
  typename ObWkbGeomCollection::const_pointer sub_ptr;
  for (result = false; iter != geo2->end() && (result == false) && OB_SUCC(ret); ++iter) {
    sub_ptr = iter.operator->();
    ObGeoType sub_type = geo2->get_sub_type(sub_ptr);
    switch (sub_type) {
      case ObGeoType::POINT :
      case ObGeoType::LINESTRING :
      case ObGeoType::MULTIPOINT :
      case ObGeoType::MULTILINESTRING :
      case ObGeoType::GEOMETRYCOLLECTION : {
        ObGeometry *sub_g2 = NULL;
        common::ObIAllocator *allocator = context.get_allocator();
        if (OB_FAIL(ObGeoTypeUtil::create_geo_by_type(*allocator, sub_type, false, true, sub_g2))) {
          LOG_WARN("failed to create wkb", K(ret), K(sub_type));
        } else {
          // Length is not used, cannot get real length until iter move to the next
          ObString wkb_nosrid(WKB_COMMON_WKB_HEADER_LEN, reinterpret_cast<const char *>(sub_ptr));
          sub_g2->set_data(wkb_nosrid);
          sub_g2->set_srid(g2->get_srid());
          if (OB_FAIL(eval_wkb_binary(g1, sub_g2, context, result))) {
            LOG_WARN("failed to eval sub geo", K(ret), K(sub_type));
          }
        }
        break;
      }
      case ObGeoType::POLYGON : {
        const ObWkbGeomPolygon *polygon = reinterpret_cast<const ObWkbGeomPolygon*>(sub_ptr);
        ObString tmp(polygon->length(), reinterpret_cast<const char*>(sub_ptr));
        ObString pol_data;
        if (OB_FAIL(ob_write_string(*context.get_allocator(), tmp, pol_data))) {
          LOG_WARN("failed to copy polygon geo", K(ret));
        } else {
          ObWkbGeomPolygon *poly_copy = reinterpret_cast<ObWkbGeomPolygon*>(pol_data.ptr());
          boost::geometry::correct(*poly_copy);
          result = boost::geometry::covered_by(*geo1, *poly_copy);
        }
        break;
      }
      case ObGeoType::MULTIPOLYGON : {
        const ObWkbGeomMultiPolygon *multi_poly = reinterpret_cast<const ObWkbGeomMultiPolygon*>(sub_ptr);
        ObString tmp(multi_poly->length(), reinterpret_cast<const char*>(sub_ptr));
        ObString multipol_data;
        if (OB_FAIL(ob_write_string(*context.get_allocator(), tmp, multipol_data))) {
          LOG_WARN("failed to copy multi_poly geo", K(ret));
        } else {
          ObWkbGeomMultiPolygon *multipoly_copy = reinterpret_cast<ObWkbGeomMultiPolygon*>(multipol_data.ptr());
          boost::geometry::correct(*multipoly_copy);
          result = boost::geometry::covered_by(*geo1, *multipoly_copy);
        }
        break;
      }
      default : {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected geometry type", K(ret));
      }
    }
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_GEO1_BEGIN(ObGeoFuncCoveredByImpl, ObWkbGeomPoint, bool)
{
  UNUSED(context);
  result = ob_apply_bg_covered_by<ObWkbGeomPoint, GeoType2>(g1, g2);
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncCoveredByImpl, ObWkbGeomMultiPoint, ObWkbGeomPoint, bool)
{
  UNUSED(context);
  const ObWkbGeomMultiPoint *geo1 = reinterpret_cast<const ObWkbGeomMultiPoint *>(g1->val());
  const ObWkbGeomPoint *geo2 = reinterpret_cast<const ObWkbGeomPoint *>(g2->val());
  //check if multipoint is equals to point geographically(msyql)
  result = boost::geometry::equals(*geo1, *geo2);
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_GEO1_BEGIN(ObGeoFuncCoveredByImpl, ObWkbGeomMultiPoint, bool)
{
  UNUSED(context);
  INIT_SUCC(ret);
  const ObWkbGeomMultiPoint *geo1 = reinterpret_cast<const ObWkbGeomMultiPoint *>(g1->val());
  const GeoType2 *geo2 = reinterpret_cast<const GeoType2 *>(g2->val());
  // travel every point in multipoint; check if any one is not covered by geo2(postgis)
  result = true;
  FOREACH_X(item, *geo1, (result == true)) {
    result = boost::geometry::covered_by(*item, *geo2);
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncCoveredByImpl, ObWkbGeomMultiPoint, ObWkbGeomCollection, bool)
{
  return ob_caculate_mp_gc_cover_by<ObWkbGeomPoint, ObIWkbGeomPoint,
    ObWkbGeomCollection>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncCoveredByImpl, ObWkbGeomLineString, ObWkbGeomLineString, bool)
{
  UNUSED(context);
  result = ob_apply_bg_covered_by<ObWkbGeomLineString, ObWkbGeomLineString>(g1, g2);
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncCoveredByImpl, ObWkbGeomLineString, ObWkbGeomPolygon, bool)
{
  UNUSED(context);
  result = ob_apply_bg_covered_by<ObWkbGeomLineString, ObWkbGeomPolygon>(g1, g2);
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncCoveredByImpl, ObWkbGeomLineString, ObWkbGeomMultiLineString, bool)
{
  UNUSED(context);
  result = ob_apply_bg_covered_by<ObWkbGeomLineString, ObWkbGeomMultiLineString>(g1, g2);
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncCoveredByImpl, ObWkbGeomLineString, ObWkbGeomMultiPolygon, bool)
{
  UNUSED(context);
  result = ob_apply_bg_covered_by<ObWkbGeomLineString, ObWkbGeomMultiPolygon>(g1, g2);
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncCoveredByImpl, ObWkbGeomLineString, ObWkbGeomCollection, bool)
{
  INIT_SUCC(ret);
  ObCartesianMultipoint *cart_multi_point = NULL;
  ObCartesianMultilinestring *cart_multi_line = NULL;
  ObCartesianMultipolygon *cart_multi_poly = NULL;
  ObGeometry *geo2 = const_cast<ObGeometry *>(reinterpret_cast<const ObGeometry *>(g2));
  if (OB_FAIL(ObGeoFuncUtils::ob_gc_prepare<ObCartesianGeometrycollection>(context, geo2, cart_multi_point,
                                                                           cart_multi_line, cart_multi_poly))) {
    LOG_WARN("failed to prepare gc", K(ret));
  } else {
    const ObSrsItem *srs = context.get_srs();
    ObIAllocator *allocator = context.get_allocator();
    uint32_t srid = srs != NULL ? srs->get_srid() : 0;
    const ObWkbGeomLineString *geo1 = reinterpret_cast<const ObWkbGeomLineString *>(g1->val());
    ObCartesianMultilinestring res_geo1(srid, *allocator);
    boost::geometry::difference(*geo1, *cart_multi_line, res_geo1);
    ObCartesianMultilinestring res_geo2(srid, *allocator);
    boost::geometry::difference(res_geo1, *cart_multi_poly, res_geo2);
    result = res_geo2.is_empty();
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncCoveredByImpl, ObWkbGeomPolygon, ObWkbGeomPolygon, bool)
{
  UNUSED(context);
  result = ob_apply_bg_covered_by<ObWkbGeomPolygon, ObWkbGeomPolygon>(g1, g2);
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncCoveredByImpl, ObWkbGeomPolygon, ObWkbGeomMultiPolygon, bool)
{
  UNUSED(context);
  result = ob_apply_bg_covered_by<ObWkbGeomPolygon, ObWkbGeomMultiPolygon>(g1, g2);
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncCoveredByImpl, ObWkbGeomPolygon, ObWkbGeomCollection, bool)
{
  INIT_SUCC(ret);
  ObCartesianMultipoint *cart_multi_point = NULL;
  ObCartesianMultilinestring *cart_multi_line = NULL;
  ObCartesianMultipolygon *cart_multi_poly = NULL;
  ObGeometry *geo2 = const_cast<ObGeometry *>(reinterpret_cast<const ObGeometry *>(g2));

  if (OB_FAIL(ObGeoFuncUtils::ob_gc_prepare<ObCartesianGeometrycollection>(context, geo2, cart_multi_point,
                                                                           cart_multi_line, cart_multi_poly))) {
    LOG_WARN("failed to prepare gc", K(ret));
  } else {
    const ObSrsItem *srs = context.get_srs();
    ObIAllocator *allocator = context.get_allocator();
    uint32_t srid = srs != NULL ? srs->get_srid() : 0;
    const ObWkbGeomPolygon *geo1 = reinterpret_cast<const ObWkbGeomPolygon *>(g1->val());
    boost::geometry::correct(*const_cast<ObWkbGeomPolygon *>(geo1));
    result = boost::geometry::covered_by(*geo1, *cart_multi_poly);
  }
  return ret;
} OB_GEO_FUNC_END;

// wkb cart multilinestring
OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncCoveredByImpl, ObWkbGeomMultiLineString, ObWkbGeomLineString, bool)
{
  UNUSED(context);
  result = ob_apply_bg_covered_by<ObWkbGeomMultiLineString, ObWkbGeomLineString>(g1, g2);
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncCoveredByImpl, ObWkbGeomMultiLineString, ObWkbGeomPolygon, bool)
{
  UNUSED(context);
  result = ob_apply_bg_covered_by<ObWkbGeomMultiLineString, ObWkbGeomPolygon>(g1, g2);
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncCoveredByImpl, ObWkbGeomMultiLineString, ObWkbGeomMultiLineString, bool)
{
  UNUSED(context);
  result = ob_apply_bg_covered_by<ObWkbGeomMultiLineString, ObWkbGeomMultiLineString>(g1, g2);
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncCoveredByImpl, ObWkbGeomMultiLineString, ObWkbGeomMultiPolygon, bool)
{
  UNUSED(context);
  result = ob_apply_bg_covered_by<ObWkbGeomMultiLineString, ObWkbGeomMultiPolygon>(g1, g2);
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncCoveredByImpl, ObWkbGeomMultiLineString, ObWkbGeomCollection, bool)
{
  INIT_SUCC(ret);
  ObCartesianMultipoint *cart_multi_point = NULL;
  ObCartesianMultilinestring *cart_multi_line = NULL;
  ObCartesianMultipolygon *cart_multi_poly = NULL;
  ObGeometry *geo2 = const_cast<ObGeometry *>(reinterpret_cast<const ObGeometry *>(g2));
  if (OB_FAIL(ObGeoFuncUtils::ob_gc_prepare<ObCartesianGeometrycollection>(context, geo2, cart_multi_point,
                                                                           cart_multi_line, cart_multi_poly))) {
    LOG_WARN("failed to do gc prepare", K(ret));
  } else {
    const ObSrsItem *srs = context.get_srs();
    ObIAllocator *allocator = context.get_allocator();
    uint32_t srid = srs != NULL ? srs->get_srid() : 0;
    const ObWkbGeomMultiLineString *geo1 = reinterpret_cast<const ObWkbGeomMultiLineString *>(g1->val());
    ObCartesianMultilinestring res_geo1(srid, *allocator);
    boost::geometry::difference(*geo1, *cart_multi_line, res_geo1);
    ObCartesianMultilinestring res_geo2(srid, *allocator);
    boost::geometry::difference(res_geo1, *cart_multi_poly, res_geo2);
    result = res_geo2.is_empty();
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncCoveredByImpl, ObWkbGeomMultiPolygon, ObWkbGeomPolygon, bool)
{
  UNUSED(context);
  result = ob_apply_bg_covered_by<ObWkbGeomMultiPolygon, ObWkbGeomPolygon>(g1, g2);
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncCoveredByImpl, ObWkbGeomMultiPolygon, ObWkbGeomMultiPolygon, bool)
{
  UNUSED(context);
  result = ob_apply_bg_covered_by<ObWkbGeomMultiPolygon , ObWkbGeomMultiPolygon>(g1, g2);
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncCoveredByImpl, ObWkbGeomMultiPolygon, ObWkbGeomCollection, bool)
{
  INIT_SUCC(ret);
  ObCartesianMultipoint *cart_multi_point = NULL;
  ObCartesianMultilinestring *cart_multi_line = NULL;
  ObCartesianMultipolygon *cart_multi_poly = NULL;
  ObGeometry *geo2 = const_cast<ObGeometry *>(reinterpret_cast<const ObGeometry *>(g2));
  if (OB_FAIL(ObGeoFuncUtils::ob_gc_prepare<ObCartesianGeometrycollection>(context, geo2, cart_multi_point,
                                                                           cart_multi_line, cart_multi_poly))) {
    LOG_WARN("failed to do gc prepare", K(ret));
  } else {
    const ObSrsItem *srs = context.get_srs();
    ObIAllocator *allocator = context.get_allocator();
    uint32_t srid = srs != NULL ? srs->get_srid() : 0;
    const ObWkbGeomMultiPolygon *geo1 = reinterpret_cast<const ObWkbGeomMultiPolygon *>(g1->val());
    ObCartesianMultipolygon res_geo1(srid, *allocator);
    boost::geometry::correct(*const_cast<ObWkbGeomMultiPolygon *>(geo1));
    result = boost::geometry::covered_by(*geo1, *cart_multi_poly);
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncCoveredByImpl, ObWkbGeomCollection, ObWkbGeomPoint, bool)
{
  INIT_SUCC(ret);
  ObCartesianMultipoint *cart_multi_point = NULL;
  ObCartesianMultilinestring *cart_multi_line = NULL;
  ObCartesianMultipolygon *cart_multi_poly = NULL;
  ObGeometry *geo1 = const_cast<ObGeometry *>(reinterpret_cast<const ObGeometry *>(g1));
  if (OB_FAIL(ObGeoFuncUtils::ob_gc_prepare<ObCartesianGeometrycollection>(context, geo1, cart_multi_point,
                                                                           cart_multi_line, cart_multi_poly))) {
    LOG_WARN("failed to do gc prepare", K(ret));
  } else if (!cart_multi_poly->empty() || !cart_multi_line->empty()) {
    result = false;
  } else if (cart_multi_point->empty()) {
    result = true;
  } else {
    result = true;
    const ObWkbGeomPoint *geo2 = reinterpret_cast<const ObWkbGeomPoint *>(g2->val());
    FOREACH_X(item, *cart_multi_point, (result == true)) {
      result = boost::geometry::covered_by(*item, *geo2);
    }
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncCoveredByImpl, ObWkbGeomCollection, ObWkbGeomLineString, bool)
{
  INIT_SUCC(ret);
  ObCartesianMultipoint *cart_multi_point = NULL;
  ObCartesianMultilinestring *cart_multi_line = NULL;
  ObCartesianMultipolygon *cart_multi_poly = NULL;
  ObGeometry *geo1 = const_cast<ObGeometry *>(reinterpret_cast<const ObGeometry *>(g1));
  if (OB_FAIL(ObGeoFuncUtils::ob_gc_prepare<ObCartesianGeometrycollection>(context, geo1, cart_multi_point,
                                                                           cart_multi_line, cart_multi_poly))) {
    LOG_WARN("failed to do gc prepare", K(ret));
  } else if (!cart_multi_poly->empty()) {
    result = false;
  } else if (cart_multi_point->empty() && cart_multi_line->empty()) {
    result = true;
  } else {
    result = true;
    const ObWkbGeomLineString *geo2 = reinterpret_cast<const ObWkbGeomLineString *>(g2->val());
    if (!cart_multi_line->empty()) {
      result = boost::geometry::covered_by(*cart_multi_line, *geo2);
    }
    FOREACH_X(item, *cart_multi_point, (result == true)) {
      result = boost::geometry::covered_by(*item, *geo2);
    }
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncCoveredByImpl, ObWkbGeomCollection, ObWkbGeomPolygon, bool)
{
  INIT_SUCC(ret);
  ObCartesianMultipoint *cart_multi_point = NULL;
  ObCartesianMultilinestring *cart_multi_line = NULL;
  ObCartesianMultipolygon *cart_multi_poly = NULL;
  ObGeometry *geo1 = const_cast<ObGeometry *>(reinterpret_cast<const ObGeometry *>(g1));
  if (OB_FAIL(ObGeoFuncUtils::ob_gc_prepare<ObCartesianGeometrycollection>(context, geo1, cart_multi_point,
                                                                           cart_multi_line, cart_multi_poly))) {
    LOG_WARN("failed to do gc prepare", K(ret));
  } else {
    result = true;
    const ObWkbGeomPolygon *geo2 = reinterpret_cast<const ObWkbGeomPolygon *>(g2->val());
    if (!cart_multi_poly->empty()) {
      result = boost::geometry::covered_by(*cart_multi_poly, *geo2);
    }
    if (result == true && !cart_multi_line->empty()) {
      result = boost::geometry::covered_by(*cart_multi_line, *geo2);
    }
    FOREACH_X(item, *cart_multi_point, (result == true)) {
      result = boost::geometry::covered_by(*item, *geo2);
    }
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncCoveredByImpl, ObWkbGeomCollection, ObWkbGeomMultiPoint, bool)
{
  INIT_SUCC(ret);
  ObCartesianMultipoint *cart_multi_point = NULL;
  ObCartesianMultilinestring *cart_multi_line = NULL;
  ObCartesianMultipolygon *cart_multi_poly = NULL;
  ObGeometry *geo1 = const_cast<ObGeometry *>(reinterpret_cast<const ObGeometry *>(g1));
  if (OB_FAIL(ObGeoFuncUtils::ob_gc_prepare<ObCartesianGeometrycollection>(context, geo1, cart_multi_point,
                                                                           cart_multi_line, cart_multi_poly))) {
    LOG_WARN("failed to do gc prepare", K(ret));
  } else if (!cart_multi_poly->empty() || !cart_multi_line->empty()) {
    result = false;
  } else if (cart_multi_point->empty()) {
    result = true;
  } else {
    result = true;
    const ObWkbGeomMultiPoint *geo2 = reinterpret_cast<const ObWkbGeomMultiPoint *>(g2->val());
    FOREACH_X(item1, *cart_multi_point, (result == true)) {
      bool loop_res = false;
      FOREACH_X(item2, *geo2, (loop_res == false)) {
        loop_res = boost::geometry::covered_by(*item1, *item2);
      }
      result = loop_res;
    }
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncCoveredByImpl, ObWkbGeomCollection, ObWkbGeomMultiLineString, bool)
{
  INIT_SUCC(ret);
  ObCartesianMultipoint *cart_multi_point = NULL;
  ObCartesianMultilinestring *cart_multi_line = NULL;
  ObCartesianMultipolygon *cart_multi_poly = NULL;
  ObGeometry *geo1 = const_cast<ObGeometry *>(reinterpret_cast<const ObGeometry *>(g1));
  if (OB_FAIL(ObGeoFuncUtils::ob_gc_prepare<ObCartesianGeometrycollection>(context, geo1, cart_multi_point,
                                                                           cart_multi_line, cart_multi_poly))) {
    LOG_WARN("failed to do gc prepare", K(ret));
  } else if (!cart_multi_poly->empty()) {
    result = false;
  } else if (cart_multi_point->empty() && cart_multi_line->empty()) {
    result = true;
  } else {
    result = true;
    const ObWkbGeomMultiLineString *geo2 = reinterpret_cast<const ObWkbGeomMultiLineString *>(g2->val());
    if (!cart_multi_line->empty()) {
      result = boost::geometry::covered_by(*cart_multi_line, *geo2);
    }
    FOREACH_X(item, *cart_multi_point, (result == true)) {
      result = boost::geometry::covered_by(*item, *geo2);
    }
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncCoveredByImpl, ObWkbGeomCollection, ObWkbGeomMultiPolygon, bool)
{
  INIT_SUCC(ret);
  ObCartesianMultipoint *cart_multi_point = NULL;
  ObCartesianMultilinestring *cart_multi_line = NULL;
  ObCartesianMultipolygon *cart_multi_poly = NULL;
  ObGeometry *geo1 = const_cast<ObGeometry *>(reinterpret_cast<const ObGeometry *>(g1));
  if (OB_FAIL(ObGeoFuncUtils::ob_gc_prepare<ObCartesianGeometrycollection>(context, geo1, cart_multi_point,
                                                                           cart_multi_line, cart_multi_poly))) {
    LOG_WARN("failed to do gc prepare", K(ret));
  } else {
    result = true;
    const ObWkbGeomMultiPolygon *geo2 = reinterpret_cast<const ObWkbGeomMultiPolygon *>(g2->val());
    if (!cart_multi_poly->empty()) {
      result = boost::geometry::covered_by(*cart_multi_poly, *geo2);
    }
    if (result == true && !cart_multi_line->empty()) {
      result = boost::geometry::covered_by(*cart_multi_line, *geo2);
    }
    FOREACH_X(item, *cart_multi_point, (result == true)) {
      result = boost::geometry::covered_by(*item, *geo2);
    }
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncCoveredByImpl, ObWkbGeomCollection, ObWkbGeomCollection, bool)
{
  INIT_SUCC(ret);
  ObCartesianMultipoint *cart_multi_point1 = NULL;
  ObCartesianMultilinestring *cart_multi_line1 = NULL;
  ObCartesianMultipolygon *cart_multi_poly1 = NULL;
  ObCartesianMultipoint *cart_multi_point2 = NULL;
  ObCartesianMultilinestring *cart_multi_line2 = NULL;
  ObCartesianMultipolygon *cart_multi_poly2 = NULL;
  ObGeometry *geo1 = const_cast<ObGeometry *>(reinterpret_cast<const ObGeometry *>(g1));
  ObGeometry *geo2 = const_cast<ObGeometry *>(reinterpret_cast<const ObGeometry *>(g2));
  if (OB_FAIL(ObGeoFuncUtils::ob_gc_prepare<ObCartesianGeometrycollection>(context, geo1, cart_multi_point1,
                                                                           cart_multi_line1, cart_multi_poly1))) {
    LOG_WARN("failed to do gc1 prepare", K(ret));
  } else if (OB_FAIL(ObGeoFuncUtils::ob_gc_prepare<ObCartesianGeometrycollection>(context, geo2, cart_multi_point2,
                                                                                  cart_multi_line2, cart_multi_poly2))) {
    LOG_WARN("failed to do gc2 prepare", K(ret));
  } else {
    result = true;
    const ObSrsItem *srs = context.get_srs();
    ObIAllocator *allocator = context.get_allocator();
    uint32_t srid = srs != NULL ? srs->get_srid() : 0;
    ObCartesianMultipoint diff_geo1(srid, *allocator);
    boost::geometry::difference(*cart_multi_point1, *cart_multi_point2, diff_geo1);
    ObCartesianMultipoint diff_geo2(srid, *allocator);
    boost::geometry::difference(diff_geo1, *cart_multi_line2, diff_geo2);
    ObCartesianMultipoint diff_geo3(srid, *allocator);
    boost::geometry::difference(diff_geo2, *cart_multi_poly2, diff_geo3);
    if (!diff_geo3.empty()) {
      result = false;
    } else {
      ObCartesianMultilinestring diff_line1(srid, *allocator);
      boost::geometry::difference(*cart_multi_line1, *cart_multi_line2, diff_line1);
      ObCartesianMultilinestring diff_line2(srid, *allocator);
      boost::geometry::difference(diff_line1, *cart_multi_poly2, diff_line2);
      if (!diff_line2.empty()) {
        result = false;
      } else {
        ObCartesianMultipolygon diff_poly(srid, *allocator);
        boost::geometry::difference(*cart_multi_poly1, *cart_multi_poly2, diff_poly);
        if (!diff_poly.empty()) {
          result = false;
        }
      }
    }
  }
  return ret;
} OB_GEO_FUNC_END;

// geographic point
OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncCoveredByImpl, ObWkbGeogPoint, ObWkbGeogCollection, bool)
{
  INIT_SUCC(ret);
  result = false;
  const ObWkbGeogPoint *geo1 = reinterpret_cast<const ObWkbGeogPoint *>(g1->val());
  const ObWkbGeogCollection *geo2 = reinterpret_cast<const ObWkbGeogCollection *>(g2->val());
  const ObSrsItem *srs = context.get_srs();
  boost::geometry::srs::spheroid<double> geog_sphere(srs->semi_major_axis(), srs->semi_minor_axis());
  ObPlPaStrategy point_strategy(geog_sphere);
  ObWkbGeogCollection::iterator iter = geo2->begin();
  typename ObWkbGeogCollection::const_pointer sub_ptr;
  for (; iter != geo2->end() && (result == false) && OB_SUCC(ret); ++iter) {
    sub_ptr = iter.operator->();
    ObGeoType sub_type = geo2->get_sub_type(sub_ptr);
    switch (sub_type) {
      case ObGeoType::POINT :
      case ObGeoType::LINESTRING :
      case ObGeoType::MULTIPOINT :
      case ObGeoType::MULTILINESTRING :
      case ObGeoType::GEOMETRYCOLLECTION :{
        ObGeometry *sub_g2 = NULL;
        common::ObIAllocator *allocator = context.get_allocator();
        if (OB_FAIL(ObGeoTypeUtil::create_geo_by_type(*allocator, sub_type, true, true, sub_g2))) {
          LOG_WARN("failed to create wkb", K(ret), K(sub_type));
        } else {
          // Length is not used, cannot get real length until iter move to the next
          ObString wkb_nosrid(WKB_COMMON_WKB_HEADER_LEN, reinterpret_cast<const char *>(sub_ptr));
          sub_g2->set_data(wkb_nosrid);
          sub_g2->set_srid(g2->get_srid());
          if (OB_FAIL(eval_wkb_binary(g1, sub_g2, context, result))) {
            LOG_WARN("failed to eval sub geo", K(ret), K(sub_type));
          }
        }
        break;
      }

      case ObGeoType::POLYGON : {
        const ObWkbGeogPolygon *polygon = reinterpret_cast<const ObWkbGeogPolygon*>(sub_ptr);
        ObString tmp(polygon->length(), reinterpret_cast<const char*>(sub_ptr));
        ObString pol_data;
        if (OB_FAIL(ob_write_string(*context.get_allocator(), tmp, pol_data))) {
          LOG_WARN("failed to copy polygon geo", K(ret));
        } else {
          ObWkbGeogPolygon *poly_copy = reinterpret_cast<ObWkbGeogPolygon*>(pol_data.ptr());
          boost::geometry::strategy::area::geographic<> area_strategy(geog_sphere);
#ifdef USE_SPHERE_GEO
          boost::geometry::correct(*poly_copy, area_strategy);
          result = boost::geometry::covered_by(*geo1, *poly_copy, point_strategy);
#else
          boost::geometry::correct(*poly_copy);
          result = boost::geometry::covered_by(*geo1, *poly_copy);
#endif
        }
        break;
      }
      case ObGeoType::MULTIPOLYGON : {
        const ObWkbGeogMultiPolygon *multi_poly = reinterpret_cast<const ObWkbGeogMultiPolygon*>(sub_ptr);
        ObString tmp(multi_poly->length(), reinterpret_cast<const char*>(sub_ptr));
        ObString multipol_data;
        if (OB_FAIL(ob_write_string(*context.get_allocator(), tmp, multipol_data))) {
          LOG_WARN("failed to copy multi_poly geo", K(ret));
        } else {
          ObWkbGeogMultiPolygon *multipoly_copy = reinterpret_cast<ObWkbGeogMultiPolygon*>(multipol_data.ptr());
          boost::geometry::strategy::area::geographic<> area_strategy(geog_sphere);
#ifdef USE_SPHERE_GEO
          boost::geometry::correct(*multipoly_copy, area_strategy);
          result = boost::geometry::covered_by(*geo1, *multipoly_copy, point_strategy);
#else
          boost::geometry::correct(*multipoly_copy);
          result = boost::geometry::covered_by(*geo1, *multipoly_copy);
#endif
        }
        break;
      }
      default : {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected geometry type", K(ret));
      }
    }
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncCoveredByImpl, ObWkbGeogMultiPoint, ObWkbGeogCollection, bool)
{
  return ob_caculate_mp_gc_cover_by<ObWkbGeogPoint, ObIWkbGeogPoint,
    ObWkbGeogCollection>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncCoveredByImpl, ObWkbGeogPoint, ObWkbGeogPoint, bool)
{
  UNUSED(context);
  result = ob_apply_bg_covered_by<ObWkbGeogPoint, ObWkbGeogPoint>(g1, g2);
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncCoveredByImpl, ObWkbGeogPoint, ObWkbGeogMultiPoint, bool)
{
  UNUSED(context);
  result = ob_apply_bg_covered_by<ObWkbGeogPoint, ObWkbGeogMultiPoint>(g1, g2);
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_GEO1_BEGIN(ObGeoFuncCoveredByImpl, ObWkbGeogPoint, bool)
{
  result = ob_apply_bg_covered_by_with_pl_strategy<ObWkbGeogPoint, GeoType2>(g1, g2, context);
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncCoveredByImpl, ObWkbGeogMultiPoint, ObWkbGeogPoint, bool)
{
  UNUSED(context);
  const ObWkbGeogMultiPoint *geo1 = reinterpret_cast<const ObWkbGeogMultiPoint *>(g1->val());
  const ObWkbGeogPoint *geo2 = reinterpret_cast<const ObWkbGeogPoint *>(g2->val());
  //check if multipoint is equals to point geographically(msyql)
  result = boost::geometry::equals(*geo1, *geo2);
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncCoveredByImpl, ObWkbGeogMultiPoint, ObWkbGeogMultiPoint, bool)
{
  const ObWkbGeogMultiPoint *geo1 = reinterpret_cast<const ObWkbGeogMultiPoint *>(g1->val());
  const ObWkbGeogMultiPoint *geo2 = reinterpret_cast<const ObWkbGeogMultiPoint *>(g2->val());
  // travel every point in multipoint; check if any one is not covered by geo2(postgis)
  result = true;
  FOREACH_X(item, *geo1, (result == true)) {
    result = boost::geometry::covered_by(*item, *geo2);
  }
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_GEO1_BEGIN(ObGeoFuncCoveredByImpl, ObWkbGeogMultiPoint, bool)
{
  const ObSrsItem *srs = context.get_srs();
  const ObWkbGeogMultiPoint *geo1 = reinterpret_cast<const ObWkbGeogMultiPoint *>(g1->val());
  const GeoType2 *geo2 = reinterpret_cast<const GeoType2 *>(g2->val());
  // travel every point in multipoint; check if any one is not covered by geo2(postgis)
  result = true;
  boost::geometry::srs::spheroid<double> geog_sphere(srs->semi_major_axis(), srs->semi_minor_axis());
  ObPlPaStrategy point_strategy(geog_sphere);
  FOREACH_X(item, *geo1, (result == true)) {
#ifdef USE_SPHERE_GEO
  result = boost::geometry::covered_by(*item, *geo2, point_strategy);
#else
    result = boost::geometry::covered_by(*item, *geo2);
#endif
  }
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncCoveredByImpl, ObWkbGeogLineString, ObWkbGeogLineString, bool)
{
  result = ob_apply_bg_covered_by_with_ll_strategy<ObWkbGeogLineString, ObWkbGeogLineString>(g1, g2, context);
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncCoveredByImpl, ObWkbGeogLineString, ObWkbGeogPolygon, bool)
{
  result = ob_apply_bg_covered_by_with_ll_strategy<ObWkbGeogLineString, ObWkbGeogPolygon>(g1, g2, context);
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncCoveredByImpl, ObWkbGeogLineString, ObWkbGeogMultiLineString, bool)
{
  result = ob_apply_bg_covered_by_with_ll_strategy<ObWkbGeogLineString, ObWkbGeogMultiLineString>(g1, g2, context);
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncCoveredByImpl, ObWkbGeogLineString, ObWkbGeogMultiPolygon, bool)
{
  result = ob_apply_bg_covered_by_with_ll_strategy<ObWkbGeogLineString, ObWkbGeogMultiPolygon>(g1, g2, context);
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncCoveredByImpl, ObWkbGeogLineString, ObWkbGeogCollection, bool)
{
  INIT_SUCC(ret);
  ObGeographMultipoint *multi_point = NULL;
  ObGeographMultilinestring *multi_line = NULL;
  ObGeographMultipolygon *multi_poly = NULL;
  const ObSrsItem *srs = context.get_srs();
  ObGeometry *geo2 = const_cast<ObGeometry *>(reinterpret_cast<const ObGeometry *>(g2));
  if (OB_ISNULL(srs)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("srs is null", K(ret));
  } else if (OB_FAIL(ObGeoFuncUtils::ob_gc_prepare<ObGeographGeometrycollection>(context, geo2, multi_point,
                                                                                 multi_line, multi_poly))) {
    LOG_WARN("failed to do gc prepare", K(ret));
  } else {
    ObIAllocator *allocator = context.get_allocator();
    uint32_t srid = srs->get_srid();
    boost::geometry::srs::spheroid<double> geog_sphere(srs->semi_major_axis(), srs->semi_minor_axis());
    ObLlLaAaStrategy line_strategy(geog_sphere);
    const ObWkbGeogLineString *geo1 = reinterpret_cast<const ObWkbGeogLineString *>(g1->val());
    ObGeographMultilinestring res_geo1(srid, *allocator);
    ObGeographMultilinestring res_geo2(srid, *allocator);
#ifdef USE_SPHERE_GEO
    boost::geometry::difference(*geo1, *multi_line, res_geo1, line_strategy);
    boost::geometry::difference(res_geo1, *multi_poly, res_geo2, line_strategy);
#else
    boost::geometry::difference(*geo1, *multi_line, res_geo1);
    boost::geometry::difference(res_geo1, *multi_poly, res_geo2);
#endif
    result = res_geo2.is_empty();
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncCoveredByImpl, ObWkbGeogPolygon, ObWkbGeogPolygon, bool)
{
  UNUSED(context);
  result = ob_apply_bg_covered_by_with_ll_strategy<ObWkbGeogPolygon, ObWkbGeogPolygon>(g1, g2, context);
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncCoveredByImpl, ObWkbGeogPolygon, ObWkbGeogMultiPolygon, bool)
{
  UNUSED(context);
  result = ob_apply_bg_covered_by_with_ll_strategy<ObWkbGeogPolygon, ObWkbGeogMultiPolygon>(g1, g2, context);
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncCoveredByImpl, ObWkbGeogPolygon, ObWkbGeogCollection, bool)
{
  INIT_SUCC(ret);
  ObGeographMultipoint *multi_point = NULL;
  ObGeographMultilinestring *multi_line = NULL;
  ObGeographMultipolygon *multi_poly = NULL;
  ObGeometry *geo2 = const_cast<ObGeometry *>(reinterpret_cast<const ObGeometry *>(g2));
  const ObSrsItem *srs = context.get_srs();
  if (OB_ISNULL(srs)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("srs is null", K(ret));
  } else if (OB_FAIL(ObGeoFuncUtils::ob_gc_prepare<ObGeographGeometrycollection>(context, geo2, multi_point,
                                                                                 multi_line, multi_poly))) {
    LOG_WARN("failed to do gc prepare", K(ret));
  } else {
    ObIAllocator *allocator = context.get_allocator();
    uint32_t srid = srs->get_srid();
    boost::geometry::srs::spheroid<double> geog_sphere(srs->semi_major_axis(), srs->semi_minor_axis());
    ObLlLaAaStrategy line_strategy(geog_sphere);
    const ObWkbGeogPolygon *geo1 = reinterpret_cast<const ObWkbGeogPolygon *>(g1->val());
    boost::geometry::strategy::area::geographic<> area_strategy(geog_sphere);
    boost::geometry::correct(*const_cast<ObWkbGeogPolygon *>(geo1), area_strategy);
    result = boost::geometry::covered_by(*geo1, *multi_poly);
  }
  return ret;
} OB_GEO_FUNC_END;

// wkb geog multilinestring
OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncCoveredByImpl, ObWkbGeogMultiLineString, ObWkbGeogLineString, bool)
{
  result = ob_apply_bg_covered_by_with_ll_strategy<ObWkbGeogMultiLineString, ObWkbGeogLineString>(g1, g2, context);
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncCoveredByImpl, ObWkbGeogMultiLineString, ObWkbGeogPolygon, bool)
{
  result = ob_apply_bg_covered_by_with_ll_strategy<ObWkbGeogMultiLineString, ObWkbGeogPolygon>(g1, g2, context);
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncCoveredByImpl, ObWkbGeogMultiLineString, ObWkbGeogMultiLineString, bool)
{
  result = ob_apply_bg_covered_by_with_ll_strategy<ObWkbGeogMultiLineString,
                                                   ObWkbGeogMultiLineString>(g1, g2, context);
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncCoveredByImpl, ObWkbGeogMultiLineString, ObWkbGeogMultiPolygon, bool)
{
  result = ob_apply_bg_covered_by_with_ll_strategy<ObWkbGeogMultiLineString,
                                                   ObWkbGeogMultiPolygon>(g1, g2, context);
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncCoveredByImpl, ObWkbGeogMultiLineString, ObWkbGeogCollection, bool)
{
  INIT_SUCC(ret);
  ObGeographMultipoint *multi_point = NULL;
  ObGeographMultilinestring *multi_line = NULL;
  ObGeographMultipolygon *multi_poly = NULL;
  ObGeometry *geo2 = const_cast<ObGeometry *>(reinterpret_cast<const ObGeometry *>(g2));
  const ObSrsItem *srs = context.get_srs();
  if (OB_ISNULL(srs)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("srs is null", K(ret));
  } else if (OB_FAIL(ObGeoFuncUtils::ob_gc_prepare<ObGeographGeometrycollection>(context, geo2, multi_point,
                                                                                 multi_line, multi_poly))) {
    LOG_WARN("failed to do gc prepare", K(ret));
  } else {
    ObIAllocator *allocator = context.get_allocator();
    uint32_t srid = srs->get_srid();
    boost::geometry::srs::spheroid<double> geog_sphere(srs->semi_major_axis(), srs->semi_minor_axis());
    ObLlLaAaStrategy line_strategy(geog_sphere);
    const ObWkbGeogMultiLineString *geo1 = reinterpret_cast<const ObWkbGeogMultiLineString *>(g1->val());
    ObGeographMultilinestring res_geo1(srid, *allocator);
    ObGeographMultilinestring res_geo2(srid, *allocator);
#ifdef USE_SPHERE_GEO
    boost::geometry::difference(*geo1, *multi_line, res_geo1, line_strategy);
    boost::geometry::difference(res_geo1, *multi_poly, res_geo2, line_strategy);
#else
    boost::geometry::difference(*geo1, *multi_line, res_geo1);
    boost::geometry::difference(res_geo1, *multi_poly, res_geo2);
#endif
    result = res_geo2.is_empty();
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncCoveredByImpl, ObWkbGeogMultiPolygon, ObWkbGeogPolygon, bool)
{
  result = ob_apply_bg_covered_by_with_ll_strategy<ObWkbGeogMultiPolygon, ObWkbGeogPolygon>(g1, g2, context);
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncCoveredByImpl, ObWkbGeogMultiPolygon, ObWkbGeogMultiPolygon, bool)
{
  result = ob_apply_bg_covered_by_with_ll_strategy<ObWkbGeogMultiPolygon , ObWkbGeogMultiPolygon>(g1, g2, context);
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncCoveredByImpl, ObWkbGeogMultiPolygon, ObWkbGeogCollection, bool)
{
  INIT_SUCC(ret);
  ObGeographMultipoint *multi_point = NULL;
  ObGeographMultilinestring *multi_line = NULL;
  ObGeographMultipolygon *multi_poly = NULL;
  ObGeometry *geo2 = const_cast<ObGeometry *>(reinterpret_cast<const ObGeometry *>(g2));
  const ObSrsItem *srs = context.get_srs();
  if (OB_ISNULL(srs)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("srs is null", K(ret));
  } else if (OB_FAIL(ObGeoFuncUtils::ob_gc_prepare<ObGeographGeometrycollection>(context, geo2, multi_point,
                                                                                 multi_line, multi_poly))) {
    LOG_WARN("failed to do gc prepare", K(ret));
  } else {
    ObIAllocator *allocator = context.get_allocator();
    uint32_t srid = srs->get_srid();
    boost::geometry::srs::spheroid<double> geog_sphere(srs->semi_major_axis(), srs->semi_minor_axis());
    ObLlLaAaStrategy line_strategy(geog_sphere);
    const ObWkbGeogMultiPolygon *geo1 = reinterpret_cast<const ObWkbGeogMultiPolygon *>(g1->val());
    boost::geometry::strategy::area::geographic<> area_strategy(geog_sphere);
    boost::geometry::correct(*const_cast<ObWkbGeogMultiPolygon *>(geo1), area_strategy);
    result = boost::geometry::covered_by(*geo1, *multi_poly);
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncCoveredByImpl, ObWkbGeogCollection, ObWkbGeogPoint, bool)
{
  INIT_SUCC(ret);
  ObGeographMultipoint *multi_point = NULL;
  ObGeographMultilinestring *multi_line = NULL;
  ObGeographMultipolygon *multi_poly = NULL;
  ObGeometry *geo1 = const_cast<ObGeometry *>(reinterpret_cast<const ObGeometry *>(g1));
  const ObSrsItem *srs = context.get_srs();
  if (OB_ISNULL(srs)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("srs is null", K(ret));
  } else if (OB_FAIL(ObGeoFuncUtils::ob_gc_prepare<ObGeographGeometrycollection>(context, geo1, multi_point,
                                                                                 multi_line, multi_poly))) {
    LOG_WARN("failed to do gc prepare", K(ret));
  } else if (!multi_poly->empty() || !multi_line->empty()) {
    result = false;
  } else if (multi_point->empty()) {
    result = true;
  } else {
    result = true;
    const ObWkbGeogPoint *geo2 = reinterpret_cast<const ObWkbGeogPoint *>(g2->val());
    FOREACH_X(item, *multi_point, (result == true)) {
      result = boost::geometry::covered_by(*item, *geo2);
    }
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncCoveredByImpl, ObWkbGeogCollection, ObWkbGeogLineString, bool)
{
  INIT_SUCC(ret);
  ObGeographMultipoint *multi_point = NULL;
  ObGeographMultilinestring *multi_line = NULL;
  ObGeographMultipolygon *multi_poly = NULL;
  ObGeometry *geo1 = const_cast<ObGeometry *>(reinterpret_cast<const ObGeometry *>(g1));
  const ObSrsItem *srs = context.get_srs();
  if (OB_ISNULL(srs)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("srs is null", K(ret));
  } else if (OB_FAIL(ObGeoFuncUtils::ob_gc_prepare<ObGeographGeometrycollection>(context, geo1, multi_point,
                                                                                 multi_line, multi_poly))) {
    LOG_WARN("failed to do gc prepare", K(ret));
  } else if (!multi_poly->empty()) {
    result = false;
  } else if (multi_point->empty() && multi_line->empty()) {
    result = true;
  } else {
    result = true;
    const ObWkbGeogLineString *geo2 = reinterpret_cast<const ObWkbGeogLineString *>(g2->val());
    boost::geometry::srs::spheroid<double> geog_sphere(srs->semi_major_axis(), srs->semi_minor_axis());
    ObLlLaAaStrategy line_strategy(geog_sphere);
    if (!multi_line->empty()) {
#ifdef USE_SPHERE_GEO
      result = boost::geometry::covered_by(*multi_line, *geo2, line_strategy);
#else
      result = boost::geometry::covered_by(*multi_line, *geo2);
#endif
    }
    ObPlPaStrategy point_strategy(geog_sphere);
    FOREACH_X(item, *multi_point, (result == true)) {
#ifdef USE_SPHERE_GEO
      result = boost::geometry::covered_by(*item, *geo2, point_strategy);
#else
      result = boost::geometry::covered_by(*item, *geo2);
#endif
    }
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncCoveredByImpl, ObWkbGeogCollection, ObWkbGeogPolygon, bool)
{
  INIT_SUCC(ret);
  ObGeographMultipoint *multi_point = NULL;
  ObGeographMultilinestring *multi_line = NULL;
  ObGeographMultipolygon *multi_poly = NULL;
  ObGeometry *geo1 = const_cast<ObGeometry *>(reinterpret_cast<const ObGeometry *>(g1));
  const ObSrsItem *srs = context.get_srs();
  if (OB_ISNULL(srs)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("srs is null", K(ret));
  } else if (OB_FAIL(ObGeoFuncUtils::ob_gc_prepare<ObGeographGeometrycollection>(context, geo1, multi_point,
                                                                                 multi_line, multi_poly))) {
    LOG_WARN("failed to do gc prepare", K(ret));
  } else {
    result = true;
    boost::geometry::srs::spheroid<double> geog_sphere(srs->semi_major_axis(), srs->semi_minor_axis());
    ObLlLaAaStrategy line_strategy(geog_sphere);
    const ObWkbGeogPolygon *geo2 = reinterpret_cast<const ObWkbGeogPolygon *>(g2->val());
    if (!multi_poly->empty()) {
#ifdef USE_SPHERE_GEO
      result = boost::geometry::covered_by(*multi_poly, *geo2, line_strategy);
#else
      result = boost::geometry::covered_by(*multi_poly, *geo2);
#endif
    }
    if (result == true && !multi_line->empty()) {
#ifdef USE_SPHERE_GEO
      result = boost::geometry::covered_by(*multi_line, *geo2, line_strategy);
#else
      result = boost::geometry::covered_by(*multi_line, *geo2);
#endif
    }
    ObPlPaStrategy point_strategy(geog_sphere);
    FOREACH_X(item, *multi_point, (result == true)) {
#ifdef USE_SPHERE_GEO
      result = boost::geometry::covered_by(*item, *geo2, point_strategy);
#else
      result = boost::geometry::covered_by(*item, *geo2);
#endif
    }
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncCoveredByImpl, ObWkbGeogCollection, ObWkbGeogMultiPoint, bool)
{
  INIT_SUCC(ret);
  ObGeographMultipoint *multi_point = NULL;
  ObGeographMultilinestring *multi_line = NULL;
  ObGeographMultipolygon *multi_poly = NULL;
  ObGeometry *geo1 = const_cast<ObGeometry *>(reinterpret_cast<const ObGeometry *>(g1));
  const ObSrsItem *srs = context.get_srs();
  if (OB_ISNULL(srs)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("srs is null", K(ret));
  } else if (OB_FAIL(ObGeoFuncUtils::ob_gc_prepare<ObGeographGeometrycollection>(context, geo1, multi_point,
                                                                                 multi_line, multi_poly))) {
    LOG_WARN("failed to do gc prepare", K(ret));
  } else if (!multi_poly->empty() || !multi_line->empty()) {
    result = false;
  } else if (multi_point->empty()) {
    result = true;
  } else {
    result = true;
    const ObWkbGeogMultiPoint *geo2 = reinterpret_cast<const ObWkbGeogMultiPoint *>(g2->val());
    FOREACH_X(item1, *multi_point, (result == true)) {
      bool loop_res = false;
      FOREACH_X(item2, *geo2, (loop_res == false)) {
        loop_res = boost::geometry::covered_by(*item1, *item2);
      }
      result = loop_res;
    }
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncCoveredByImpl, ObWkbGeogCollection, ObWkbGeogMultiLineString, bool)
{
  INIT_SUCC(ret);
  ObGeographMultipoint *multi_point = NULL;
  ObGeographMultilinestring *multi_line = NULL;
  ObGeographMultipolygon *multi_poly = NULL;
  ObGeometry *geo1 = const_cast<ObGeometry *>(reinterpret_cast<const ObGeometry *>(g1));
      const ObSrsItem *srs = context.get_srs();
  if (OB_ISNULL(srs)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("srs is null", K(ret));
  } else if (OB_FAIL(ObGeoFuncUtils::ob_gc_prepare<ObGeographGeometrycollection>(context, geo1, multi_point,
                                                                                 multi_line, multi_poly))) {
    LOG_WARN("failed to do gc prepare", K(ret));
  } else if (!multi_poly->empty()) {
    result = false;
  } else if (multi_point->empty() && multi_line->empty()) {
    result = true;
  } else {
    result = true;
    const ObWkbGeogMultiLineString *geo2 = reinterpret_cast<const ObWkbGeogMultiLineString *>(g2->val());
    boost::geometry::srs::spheroid<double> geog_sphere(srs->semi_major_axis(), srs->semi_minor_axis());
    ObLlLaAaStrategy line_strategy(geog_sphere);
    if (!multi_line->empty()) {
#ifdef USE_SPHERE_GEO
      result = boost::geometry::covered_by(*multi_line, *geo2, line_strategy);
#else
      result = boost::geometry::covered_by(*multi_line, *geo2);
#endif
    }
    ObPlPaStrategy point_strategy(geog_sphere);
    FOREACH_X(item, *multi_point, (result == true)) {
#ifdef USE_SPHERE_GEO
      result = boost::geometry::covered_by(*item, *geo2, point_strategy);
#else
      result = boost::geometry::covered_by(*item, *geo2);
#endif
    }
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncCoveredByImpl, ObWkbGeogCollection, ObWkbGeogMultiPolygon, bool)
{
  INIT_SUCC(ret);
  ObGeographMultipoint *multi_point = NULL;
  ObGeographMultilinestring *multi_line = NULL;
  ObGeographMultipolygon *multi_poly = NULL;
  ObGeometry *geo1 = const_cast<ObGeometry *>(reinterpret_cast<const ObGeometry *>(g1));
        const ObSrsItem *srs = context.get_srs();
  if (OB_ISNULL(srs)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("srs is null", K(ret));
  } else if (OB_FAIL(ObGeoFuncUtils::ob_gc_prepare<ObGeographGeometrycollection>(context, geo1, multi_point,
                                                                                 multi_line, multi_poly))) {
    LOG_WARN("failed to do gc prepare", K(ret));
  } else {
    result = true;
    boost::geometry::srs::spheroid<double> geog_sphere(srs->semi_major_axis(), srs->semi_minor_axis());
    ObLlLaAaStrategy line_strategy(geog_sphere);
    const ObWkbGeogMultiPolygon *geo2 = reinterpret_cast<const ObWkbGeogMultiPolygon *>(g2->val());
    if (!multi_poly->empty()) {
#ifdef USE_SPHERE_GEO
      result = boost::geometry::covered_by(*multi_poly, *geo2, line_strategy);
#else
      result = boost::geometry::covered_by(*multi_poly, *geo2);
#endif
    }
    if (result == true && !multi_line->empty()) {
#ifdef USE_SPHERE_GEO
      result = boost::geometry::covered_by(*multi_line, *geo2, line_strategy);
#else
      result = boost::geometry::covered_by(*multi_line, *geo2);
#endif
    }
    ObPlPaStrategy point_strategy(geog_sphere);
    FOREACH_X(item, *multi_point, (result == true)) {
#ifdef USE_SPHERE_GEO
      result = boost::geometry::covered_by(*item, *geo2, point_strategy);
#else
      result = boost::geometry::covered_by(*item, *geo2);
#endif
    }
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncCoveredByImpl, ObWkbGeogCollection, ObWkbGeogCollection, bool)
{
  INIT_SUCC(ret);
  ObGeographMultipoint *multi_point1 = NULL;
  ObGeographMultilinestring *multi_line1 = NULL;
  ObGeographMultipolygon *multi_poly1 = NULL;
  ObGeographMultipoint *multi_point2 = NULL;
  ObGeographMultilinestring *multi_line2 = NULL;
  ObGeographMultipolygon *multi_poly2 = NULL;
  ObGeometry *geo1 = const_cast<ObGeometry *>(reinterpret_cast<const ObGeometry *>(g1));
  ObGeometry *geo2 = const_cast<ObGeometry *>(reinterpret_cast<const ObGeometry *>(g2));
  const ObSrsItem *srs = context.get_srs();
  if (OB_ISNULL(srs)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("srs is null", K(ret));
  } else if (OB_FAIL(ObGeoFuncUtils::ob_gc_prepare<ObGeographGeometrycollection>(context, geo1, multi_point1,
                                                                                 multi_line1, multi_poly1))) {
    LOG_WARN("failed to do gc1 prepare", K(ret));
  } else if (OB_FAIL(ObGeoFuncUtils::ob_gc_prepare<ObGeographGeometrycollection>(context, geo2, multi_point2,
                                                                                 multi_line2, multi_poly2))) {
    LOG_WARN("failed to do gc2 prepare", K(ret));
  } else {
    result = true;
    ObIAllocator *allocator = context.get_allocator();
    boost::geometry::srs::spheroid<double> geog_sphere(srs->semi_major_axis(), srs->semi_minor_axis());
    ObPlPaStrategy point_strategy(geog_sphere);
    uint32_t srid = srs->get_srid();
    ObGeographMultipoint diff_geo1(srid, *allocator);
    boost::geometry::difference(*multi_point1, *multi_point2, diff_geo1);
    ObGeographMultipoint diff_geo2(srid, *allocator);
    ObGeographMultipoint diff_geo3(srid, *allocator);
#ifdef USE_SPHERE_GEO
    boost::geometry::difference(diff_geo1, *multi_line2, diff_geo2, point_strategy);
    boost::geometry::difference(diff_geo2, *multi_poly2, diff_geo3, point_strategy);
#else
    boost::geometry::difference(diff_geo1, *multi_line2, diff_geo2);
    boost::geometry::difference(diff_geo2, *multi_poly2, diff_geo3);
#endif
    if (!diff_geo3.empty()) {
      result = false;
    } else {
      ObLlLaAaStrategy line_strategy(geog_sphere);
      ObGeographMultilinestring diff_line1(srid, *allocator);
      ObGeographMultilinestring diff_line2(srid, *allocator);
#ifdef USE_SPHERE_GEO
      boost::geometry::difference(*multi_line1, *multi_line2, diff_line1, line_strategy);
      boost::geometry::difference(diff_line1, *multi_poly2, diff_line2, line_strategy);
#else
      boost::geometry::difference(*multi_line1, *multi_line2, diff_line1);
      boost::geometry::difference(diff_line1, *multi_poly2, diff_line2);
#endif
      if (!diff_line2.empty()) {
        result = false;
      } else {
        ObGeographMultipolygon diff_poly(srid, *allocator);
#ifdef USE_SPHERE_GEO
        boost::geometry::difference(*multi_poly1, *multi_poly2, diff_poly, line_strategy);
#else
        boost::geometry::difference(*multi_poly1, *multi_poly2, diff_poly);
#endif
        if (!diff_poly.empty()) {
          result = false;
        }
      }
    }
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_CART_TREE_FUNC_BEGIN(ObGeoFuncCoveredByImpl, ObCartesianLineString, ObCartesianPolygon, bool)
{
  UNUSED(context);
  const ObCartesianPoint *geo1 = reinterpret_cast<const ObCartesianPoint *>(g1);
  const ObCartesianPolygon *geo2 = reinterpret_cast<const ObCartesianPolygon *>(g2);
  result = boost::geometry::covered_by(*geo1, *geo2);
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

int ObGeoFuncCoveredBy::eval(const ObGeoEvalCtx &gis_context, bool &result)
{
  return ObGeoFuncCoveredByImpl::eval_geo_func(gis_context, result);
}

} // sql
} // oceanbase