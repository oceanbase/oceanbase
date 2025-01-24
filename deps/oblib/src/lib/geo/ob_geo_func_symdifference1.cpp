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
 * This file contains implementation for ob_geo_func_symdifference.
 */

#define USING_LOG_PREFIX LIB

#include "ob_geo_func_symdifference_helper.ipp"
#include "lib/geo/ob_geo_func_utils.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace common
{

// cartesian point
OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomPoint, ObWkbGeomPoint, ObGeometry *)
{
  return apply_bg_symdifference_pt_pt<ObWkbGeomPoint, ObWkbGeomPoint, ObCartesianMultipoint>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomPoint, ObWkbGeomLineString, ObGeometry *)
{
  return apply_bg_symdifference_pl_pa<ObWkbGeomPoint,
      ObWkbGeomLineString,
      ObCartesianGeometrycollection>(g1, g2, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomPoint, ObWkbGeomPolygon, ObGeometry *)
{
  return apply_bg_symdifference_pl_pa<ObWkbGeomPoint,
      ObWkbGeomPolygon,
      ObCartesianGeometrycollection>(g1, g2, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomPoint, ObWkbGeomMultiPoint, ObGeometry *)
{
  return apply_bg_symdifference_pt_pt<ObWkbGeomPoint, ObWkbGeomMultiPoint, ObCartesianMultipoint>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomPoint, ObWkbGeomMultiLineString, ObGeometry *)
{
  return apply_bg_symdifference_pl_pa<ObWkbGeomPoint,
      ObWkbGeomMultiLineString,
      ObCartesianGeometrycollection>(g1, g2, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomPoint, ObWkbGeomMultiPolygon, ObGeometry *)
{
  return apply_bg_symdifference_pl_pa<ObWkbGeomPoint,
      ObWkbGeomMultiPolygon,
      ObCartesianGeometrycollection>(g1, g2, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomPoint, ObWkbGeomCollection, ObGeometry *)
{
  return apply_bg_symdifference_pt_coll<ObCartesianPoint, ObCartesianGeometrycollection>(
      g1, g2, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

// cartisian linestring
OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomLineString, ObWkbGeomPoint, ObGeometry *)
{
  return apply_bg_symdifference_pl_pa<ObWkbGeomPoint,
      ObWkbGeomLineString,
      ObCartesianGeometrycollection>(g2, g1, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomLineString, ObWkbGeomLineString, ObGeometry *)
{
  return apply_bg_symdifference<ObWkbGeomLineString,
      ObWkbGeomLineString,
      ObCartesianMultilinestring>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomLineString, ObWkbGeomPolygon, ObGeometry *)
{
  return apply_bg_symdifference_la<ObWkbGeomLineString,
      ObWkbGeomPolygon,
      ObCartesianGeometrycollection>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomLineString, ObWkbGeomMultiPoint, ObGeometry *)
{
  return apply_bg_symdifference_mpl_mpa<ObWkbGeomMultiPoint,
      ObWkbGeomLineString,
      ObCartesianGeometrycollection>(g2, g1, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomLineString, ObWkbGeomMultiLineString, ObGeometry *)
{
  return apply_bg_symdifference<ObWkbGeomLineString,
      ObWkbGeomMultiLineString,
      ObCartesianMultilinestring>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomLineString, ObWkbGeomMultiPolygon, ObGeometry *)
{
  return apply_bg_symdifference_la<ObWkbGeomLineString,
      ObWkbGeomMultiPolygon,
      ObCartesianGeometrycollection>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomLineString, ObWkbGeomCollection, ObGeometry *)
{
  return apply_bg_symdifference_line_coll<ObCartesianLineString, ObCartesianGeometrycollection>(
      g1, g2, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

// cartisian polygon
OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomPolygon, ObWkbGeomPoint, ObGeometry *)
{
  return apply_bg_symdifference_pl_pa<ObWkbGeomPoint,
      ObWkbGeomPolygon,
      ObCartesianGeometrycollection>(g2, g1, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomPolygon, ObWkbGeomLineString, ObGeometry *)
{
  return apply_bg_symdifference_la<ObWkbGeomLineString,
      ObWkbGeomPolygon,
      ObCartesianGeometrycollection>(g2, g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomPolygon, ObWkbGeomPolygon, ObGeometry *)
{
  return apply_bg_symdifference<ObWkbGeomPolygon, ObWkbGeomPolygon, ObCartesianMultipolygon>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomPolygon, ObWkbGeomMultiPoint, ObGeometry *)
{
  return apply_bg_symdifference_mpl_mpa<ObWkbGeomMultiPoint,
      ObWkbGeomPolygon,
      ObCartesianGeometrycollection>(g2, g1, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomPolygon, ObWkbGeomMultiLineString, ObGeometry *)
{
  return apply_bg_symdifference_la<ObWkbGeomMultiLineString,
      ObWkbGeomPolygon,
      ObCartesianGeometrycollection>(g2, g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomPolygon, ObWkbGeomMultiPolygon, ObGeometry *)
{
  return apply_bg_symdifference<ObWkbGeomPolygon, ObWkbGeomMultiPolygon, ObCartesianMultipolygon>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomPolygon, ObWkbGeomCollection, ObGeometry *)
{
  return apply_bg_symdifference_poly_coll<ObCartesianPolygon, ObCartesianGeometrycollection, ObWkbGeomPolygon>(
      g1, g2, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

// cartisian multipoint
OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomMultiPoint, ObWkbGeomPoint, ObGeometry *)
{
  return apply_bg_symdifference_pt_pt<ObWkbGeomPoint, ObWkbGeomMultiPoint, ObCartesianMultipoint>(
      g2, g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomMultiPoint, ObWkbGeomLineString, ObGeometry *)
{
  return apply_bg_symdifference_mpl_mpa<ObWkbGeomMultiPoint,
      ObWkbGeomLineString,
      ObCartesianGeometrycollection>(g1, g2, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomMultiPoint, ObWkbGeomPolygon, ObGeometry *)
{
  return apply_bg_symdifference_mpl_mpa<ObWkbGeomMultiPoint,
      ObWkbGeomPolygon,
      ObCartesianGeometrycollection>(g1, g2, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomMultiPoint, ObWkbGeomMultiPoint, ObGeometry *)
{
  return apply_bg_symdifference_pt_pt<ObWkbGeomMultiPoint,
      ObWkbGeomMultiPoint,
      ObCartesianMultipoint>(g2, g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomMultiPoint, ObWkbGeomMultiLineString, ObGeometry *)
{
  return apply_bg_symdifference_mpl_mpa<ObWkbGeomMultiPoint,
      ObWkbGeomMultiLineString,
      ObCartesianGeometrycollection>(g1, g2, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomMultiPoint, ObWkbGeomMultiPolygon, ObGeometry *)
{
  return apply_bg_symdifference_mpl_mpa<ObWkbGeomMultiPoint,
      ObWkbGeomMultiPolygon,
      ObCartesianGeometrycollection>(g1, g2, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomMultiPoint, ObWkbGeomCollection, ObGeometry *)
{
  return apply_bg_symdifference_pt_coll<ObCartesianMultipoint, ObCartesianGeometrycollection>(
      g1, g2, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

// cartisian mutilinestring
OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomMultiLineString, ObWkbGeomPoint, ObGeometry *)
{
  return apply_bg_symdifference_pl_pa<ObWkbGeomPoint,
      ObWkbGeomMultiLineString,
      ObCartesianGeometrycollection>(g2, g1, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomMultiLineString, ObWkbGeomLineString, ObGeometry *)
{
  return apply_bg_symdifference<ObWkbGeomLineString,
      ObWkbGeomMultiLineString,
      ObCartesianMultilinestring>(g2, g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomMultiLineString, ObWkbGeomPolygon, ObGeometry *)
{
  return apply_bg_symdifference_la<ObWkbGeomMultiLineString,
      ObWkbGeomPolygon,
      ObCartesianGeometrycollection>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomMultiLineString, ObWkbGeomMultiPoint, ObGeometry *)
{
  return apply_bg_symdifference_mpl_mpa<ObWkbGeomMultiPoint,
      ObWkbGeomMultiLineString,
      ObCartesianGeometrycollection>(g2, g1, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomMultiLineString, ObWkbGeomMultiLineString, ObGeometry *)
{
  return apply_bg_symdifference<ObWkbGeomMultiLineString,
      ObWkbGeomMultiLineString,
      ObCartesianMultilinestring>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomMultiLineString, ObWkbGeomMultiPolygon, ObGeometry *)
{
  return apply_bg_symdifference_la<ObWkbGeomMultiLineString,
      ObWkbGeomMultiPolygon,
      ObCartesianGeometrycollection>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomMultiLineString, ObWkbGeomCollection, ObGeometry *)
{
  return apply_bg_symdifference_line_coll<ObCartesianMultilinestring,
      ObCartesianGeometrycollection>(g1, g2, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

// cartisian multipolygon
OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomMultiPolygon, ObWkbGeomPoint, ObGeometry *)
{
  return apply_bg_symdifference_pl_pa<ObWkbGeomPoint,
      ObWkbGeomMultiPolygon,
      ObCartesianGeometrycollection>(g2, g1, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomMultiPolygon, ObWkbGeomLineString, ObGeometry *)
{
  return apply_bg_symdifference_la<ObWkbGeomLineString,
      ObWkbGeomMultiPolygon,
      ObCartesianGeometrycollection>(g2, g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomMultiPolygon, ObWkbGeomPolygon, ObGeometry *)
{
  return apply_bg_symdifference<ObWkbGeomPolygon, ObWkbGeomMultiPolygon, ObCartesianMultipolygon>(
      g2, g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomMultiPolygon, ObWkbGeomMultiPoint, ObGeometry *)
{
  return apply_bg_symdifference_mpl_mpa<ObWkbGeomMultiPoint,
      ObWkbGeomMultiPolygon,
      ObCartesianGeometrycollection>(g2, g1, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomMultiPolygon, ObWkbGeomMultiLineString, ObGeometry *)
{
  return apply_bg_symdifference_la<ObWkbGeomMultiLineString,
      ObWkbGeomMultiPolygon,
      ObCartesianGeometrycollection>(g2, g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomMultiPolygon, ObWkbGeomMultiPolygon, ObGeometry *)
{
  return apply_bg_symdifference<ObWkbGeomMultiPolygon,
      ObWkbGeomMultiPolygon,
      ObCartesianMultipolygon>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomMultiPolygon, ObWkbGeomCollection, ObGeometry *)
{
  return apply_bg_symdifference_poly_coll<ObCartesianMultipolygon, ObCartesianGeometrycollection, ObWkbGeomMultiPolygon>(
      g1, g2, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

// cartesian collection
OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomCollection, ObWkbGeomPoint, ObGeometry *)
{
  return apply_bg_symdifference_pt_coll<ObCartesianPoint, ObCartesianGeometrycollection>(
      g2, g1, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomCollection, ObWkbGeomLineString, ObGeometry *)
{
  return apply_bg_symdifference_line_coll<ObCartesianLineString, ObCartesianGeometrycollection>(
      g2, g1, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomCollection, ObWkbGeomPolygon, ObGeometry *)
{
  return apply_bg_symdifference_poly_coll<ObCartesianPolygon, ObCartesianGeometrycollection, ObWkbGeomPolygon>(
      g2, g1, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomCollection, ObWkbGeomMultiPoint, ObGeometry *)
{
  return apply_bg_symdifference_pt_coll<ObCartesianMultipoint, ObCartesianGeometrycollection>(
      g2, g1, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomCollection, ObWkbGeomMultiLineString, ObGeometry *)
{
  return apply_bg_symdifference_line_coll<ObCartesianMultilinestring,
      ObCartesianGeometrycollection>(g2, g1, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomCollection, ObWkbGeomMultiPolygon, ObGeometry *)
{
  return apply_bg_symdifference_poly_coll<ObCartesianMultipolygon, ObCartesianGeometrycollection, ObWkbGeomMultiPolygon>(
      g2, g1, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomCollection, ObWkbGeomCollection, ObGeometry *)
{
  return eval_symdifference_gc_gc<ObCartesianGeometrycollection>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

//----------------geographic-------------------//
// geograph point
OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogPoint, ObWkbGeogPoint, ObGeometry *)
{
  return apply_bg_symdifference_pt_pt<ObWkbGeogPoint, ObWkbGeogPoint, ObGeographMultipoint>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogPoint, ObWkbGeogLineString, ObGeometry *)
{
  return apply_bg_symdifference_pl_pa<ObWkbGeogPoint,
      ObWkbGeogLineString,
      ObGeographGeometrycollection>(g1, g2, context, ObBGStrategyType::PL_PA_STRATEGY, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogPoint, ObWkbGeogPolygon, ObGeometry *)
{
  return apply_bg_symdifference_pl_pa<ObWkbGeogPoint,
      ObWkbGeogPolygon,
      ObGeographGeometrycollection>(g1, g2, context, ObBGStrategyType::PL_PA_STRATEGY, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogPoint, ObWkbGeogMultiPoint, ObGeometry *)
{
  return apply_bg_symdifference_pt_pt<ObWkbGeogPoint, ObWkbGeogMultiPoint, ObGeographMultipoint>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogPoint, ObWkbGeogMultiLineString, ObGeometry *)
{
  return apply_bg_symdifference_pl_pa<ObWkbGeogPoint,
      ObWkbGeogMultiLineString,
      ObGeographGeometrycollection>(g1, g2, context, ObBGStrategyType::PL_PA_STRATEGY, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogPoint, ObWkbGeogMultiPolygon, ObGeometry *)
{
  return apply_bg_symdifference_pl_pa<ObWkbGeogPoint,
      ObWkbGeogMultiPolygon,
      ObGeographGeometrycollection>(g1, g2, context, ObBGStrategyType::PL_PA_STRATEGY, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogPoint, ObWkbGeogCollection, ObGeometry *)
{
  return apply_bg_symdifference_pt_coll<ObGeographPoint, ObGeographGeometrycollection>(
      g1, g2, context, ObBGStrategyType::PL_PA_STRATEGY, result);
}
OB_GEO_FUNC_END;

// geograph linestring
OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogLineString, ObWkbGeogPoint, ObGeometry *)
{
  return apply_bg_symdifference_pl_pa<ObWkbGeogPoint,
      ObWkbGeogLineString,
      ObGeographGeometrycollection>(g2, g1, context, ObBGStrategyType::PL_PA_STRATEGY, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogLineString, ObWkbGeogLineString, ObGeometry *)
{
  return apply_bg_symdifference<ObWkbGeogLineString,
      ObWkbGeogLineString,
      ObGeographMultilinestring>(g1, g2, context, result, ObBGStrategyType::LL_LA_AA_STRATEGY);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogLineString, ObWkbGeogPolygon, ObGeometry *)
{
  return apply_bg_symdifference_la<ObWkbGeogLineString,
      ObWkbGeogPolygon,
      ObGeographGeometrycollection>(g1, g2, context, result,
      ObBGStrategyType::LL_LA_AA_STRATEGY);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogLineString, ObWkbGeogMultiPoint, ObGeometry *)
{
  return apply_bg_symdifference_mpl_mpa<ObWkbGeogMultiPoint,
      ObWkbGeogLineString,
      ObGeographGeometrycollection>(g2, g1, context, ObBGStrategyType::PL_PA_STRATEGY, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogLineString, ObWkbGeogMultiLineString, ObGeometry *)
{
  return apply_bg_symdifference<ObWkbGeogLineString,
      ObWkbGeogMultiLineString,
      ObGeographMultilinestring>(g1, g2, context, result, ObBGStrategyType::LL_LA_AA_STRATEGY);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogLineString, ObWkbGeogMultiPolygon, ObGeometry *)
{
  return apply_bg_symdifference_la<ObWkbGeogLineString,
      ObWkbGeogMultiPolygon,
      ObGeographGeometrycollection>(g1, g2, context, result,
      ObBGStrategyType::LL_LA_AA_STRATEGY);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogLineString, ObWkbGeogCollection, ObGeometry *)
{
  return apply_bg_symdifference_line_coll<ObGeographLineString, ObGeographGeometrycollection>(
      g1, g2, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;


// geograph polygon
OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogPolygon, ObWkbGeogPoint, ObGeometry *)
{
  return apply_bg_symdifference_pl_pa<ObWkbGeogPoint,
      ObWkbGeogPolygon,
      ObGeographGeometrycollection>(g2, g1, context, ObBGStrategyType::PL_PA_STRATEGY, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogPolygon, ObWkbGeogLineString, ObGeometry *)
{
  return apply_bg_symdifference_la<ObWkbGeogLineString,
      ObWkbGeogPolygon,
      ObGeographGeometrycollection>(g2, g1, context, result,
      ObBGStrategyType::LL_LA_AA_STRATEGY);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogPolygon, ObWkbGeogPolygon, ObGeometry *)
{
  return apply_bg_symdifference<ObWkbGeogPolygon, ObWkbGeogPolygon, ObGeographMultipolygon>(
      g1, g2, context, result, ObBGStrategyType::LL_LA_AA_STRATEGY);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogPolygon, ObWkbGeogMultiPoint, ObGeometry *)
{
  return apply_bg_symdifference_mpl_mpa<ObWkbGeogMultiPoint,
      ObWkbGeogPolygon,
      ObGeographGeometrycollection>(g2, g1, context, ObBGStrategyType::PL_PA_STRATEGY, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogPolygon, ObWkbGeogMultiLineString, ObGeometry *)
{
  return apply_bg_symdifference_la<ObWkbGeogMultiLineString,
      ObWkbGeogPolygon,
      ObGeographGeometrycollection>(g2, g1, context, result,
      ObBGStrategyType::LL_LA_AA_STRATEGY);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogPolygon, ObWkbGeogMultiPolygon, ObGeometry *)
{
  return apply_bg_symdifference<ObWkbGeogPolygon, ObWkbGeogMultiPolygon, ObGeographMultipolygon>(
      g1, g2, context, result, ObBGStrategyType::LL_LA_AA_STRATEGY);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogPolygon, ObWkbGeogCollection, ObGeometry *)
{
  return apply_bg_symdifference_poly_coll<ObGeographPolygon, ObGeographGeometrycollection, ObWkbGeogPolygon>(
      g1, g2, context, ObBGStrategyType::LL_LA_AA_STRATEGY, result);
}
OB_GEO_FUNC_END;

// geograph multipoint
OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogMultiPoint, ObWkbGeogPoint, ObGeometry *)
{
  return apply_bg_symdifference_pt_pt<ObWkbGeogPoint, ObWkbGeogMultiPoint, ObGeographMultipoint>(
      g2, g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogMultiPoint, ObWkbGeogLineString, ObGeometry *)
{
  return apply_bg_symdifference_mpl_mpa<ObWkbGeogMultiPoint,
      ObWkbGeogLineString,
      ObGeographGeometrycollection>(g1, g2, context, ObBGStrategyType::PL_PA_STRATEGY, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogMultiPoint, ObWkbGeogPolygon, ObGeometry *)
{
  return apply_bg_symdifference_mpl_mpa<ObWkbGeogMultiPoint,
      ObWkbGeogPolygon,
      ObGeographGeometrycollection>(g1, g2, context, ObBGStrategyType::PL_PA_STRATEGY, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogMultiPoint, ObWkbGeogMultiPoint, ObGeometry *)
{
  return apply_bg_symdifference_pt_pt<ObWkbGeogMultiPoint,
      ObWkbGeogMultiPoint,
      ObGeographMultipoint>(g2, g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogMultiPoint, ObWkbGeogMultiLineString, ObGeometry *)
{
  return apply_bg_symdifference_mpl_mpa<ObWkbGeogMultiPoint,
      ObWkbGeogMultiLineString,
      ObGeographGeometrycollection>(g1, g2, context, ObBGStrategyType::PL_PA_STRATEGY, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogMultiPoint, ObWkbGeogMultiPolygon, ObGeometry *)
{
  return apply_bg_symdifference_mpl_mpa<ObWkbGeogMultiPoint,
      ObWkbGeogMultiPolygon,
      ObGeographGeometrycollection>(g1, g2, context, ObBGStrategyType::PL_PA_STRATEGY, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogMultiPoint, ObWkbGeogCollection, ObGeometry *)
{
  return apply_bg_symdifference_pt_coll<ObGeographMultipoint, ObGeographGeometrycollection>(
      g1, g2, context, ObBGStrategyType::PL_PA_STRATEGY, result);
}
OB_GEO_FUNC_END;

// geograph mutilinestring
OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogMultiLineString, ObWkbGeogPoint, ObGeometry *)
{
  return apply_bg_symdifference_pl_pa<ObWkbGeogPoint,
      ObWkbGeogMultiLineString,
      ObGeographGeometrycollection>(g2, g1, context, ObBGStrategyType::PL_PA_STRATEGY, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogMultiLineString, ObWkbGeogLineString, ObGeometry *)
{
  return apply_bg_symdifference<ObWkbGeogLineString,
      ObWkbGeogMultiLineString,
      ObGeographMultilinestring>(g2, g1, context, result, ObBGStrategyType::LL_LA_AA_STRATEGY);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogMultiLineString, ObWkbGeogPolygon, ObGeometry *)
{
  return apply_bg_symdifference_la<ObWkbGeogMultiLineString,
      ObWkbGeogPolygon,
      ObGeographGeometrycollection>(g1, g2, context, result,
      ObBGStrategyType::LL_LA_AA_STRATEGY);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogMultiLineString, ObWkbGeogMultiPoint, ObGeometry *)
{
  return apply_bg_symdifference_mpl_mpa<ObWkbGeogMultiPoint,
      ObWkbGeogMultiLineString,
      ObGeographGeometrycollection>(g2, g1, context, ObBGStrategyType::PL_PA_STRATEGY, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogMultiLineString, ObWkbGeogMultiLineString, ObGeometry *)
{
  return apply_bg_symdifference<ObWkbGeogMultiLineString,
      ObWkbGeogMultiLineString,
      ObGeographMultilinestring>(g1, g2, context, result, ObBGStrategyType::LL_LA_AA_STRATEGY);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogMultiLineString, ObWkbGeogMultiPolygon, ObGeometry *)
{
  return apply_bg_symdifference_la<ObWkbGeogMultiLineString,
      ObWkbGeogMultiPolygon,
      ObGeographGeometrycollection>(g1, g2, context, result,
      ObBGStrategyType::LL_LA_AA_STRATEGY);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogMultiLineString, ObWkbGeogCollection, ObGeometry *)
{
  return apply_bg_symdifference_line_coll<ObGeographMultilinestring,
  ObGeographGeometrycollection>(
      g1, g2, context, ObBGStrategyType::LL_LA_AA_STRATEGY, result);
}
OB_GEO_FUNC_END;

// geograph multipolygon
OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogMultiPolygon, ObWkbGeogPoint, ObGeometry *)
{
  return apply_bg_symdifference_pl_pa<ObWkbGeogPoint,
      ObWkbGeogMultiPolygon,
      ObGeographGeometrycollection>(g2, g1, context, ObBGStrategyType::PL_PA_STRATEGY, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogMultiPolygon, ObWkbGeogLineString, ObGeometry *)
{
  return apply_bg_symdifference_la<ObWkbGeogLineString,
      ObWkbGeogMultiPolygon,
      ObGeographGeometrycollection>(g2, g1, context, result,
      ObBGStrategyType::LL_LA_AA_STRATEGY);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogMultiPolygon, ObWkbGeogPolygon, ObGeometry *)
{
  return apply_bg_symdifference<ObWkbGeogPolygon, ObWkbGeogMultiPolygon, ObGeographMultipolygon>(
      g2, g1, context, result, ObBGStrategyType::LL_LA_AA_STRATEGY);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogMultiPolygon, ObWkbGeogMultiPoint, ObGeometry *)
{
  return apply_bg_symdifference_mpl_mpa<ObWkbGeogMultiPoint,
      ObWkbGeogMultiPolygon,
      ObGeographGeometrycollection>(g2, g1, context, ObBGStrategyType::PL_PA_STRATEGY, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogMultiPolygon, ObWkbGeogMultiLineString, ObGeometry *)
{
  return apply_bg_symdifference_la<ObWkbGeogMultiLineString,
      ObWkbGeogMultiPolygon,
      ObGeographGeometrycollection>(g2, g1, context, result,
      ObBGStrategyType::LL_LA_AA_STRATEGY);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogMultiPolygon, ObWkbGeogMultiPolygon, ObGeometry *)
{
  return apply_bg_symdifference<ObWkbGeogMultiPolygon,
      ObWkbGeogMultiPolygon,
      ObGeographMultipolygon>(g1, g2, context, result, ObBGStrategyType::LL_LA_AA_STRATEGY);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogMultiPolygon, ObWkbGeogCollection, ObGeometry *)
{
  return apply_bg_symdifference_poly_coll<ObGeographMultipolygon, ObGeographGeometrycollection, ObWkbGeogMultiPolygon>(
      g1, g2, context, ObBGStrategyType::LL_LA_AA_STRATEGY, result);
}
OB_GEO_FUNC_END;

// geograph collection
OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogCollection, ObWkbGeogPoint, ObGeometry *)
{
  return apply_bg_symdifference_pt_coll<ObGeographPoint, ObGeographGeometrycollection>(
      g2, g1, context, ObBGStrategyType::PL_PA_STRATEGY, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogCollection, ObWkbGeogLineString, ObGeometry *)
{
  return apply_bg_symdifference_line_coll<ObGeographLineString, ObGeographGeometrycollection>(
      g2, g1, context, ObBGStrategyType::LL_LA_AA_STRATEGY, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogCollection, ObWkbGeogPolygon, ObGeometry *)
{
  return apply_bg_symdifference_poly_coll<ObGeographPolygon, ObGeographGeometrycollection, ObWkbGeogPolygon>(
      g2, g1, context, ObBGStrategyType::LL_LA_AA_STRATEGY, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogCollection, ObWkbGeogMultiPoint, ObGeometry *)
{
  return apply_bg_symdifference_pt_coll<ObGeographMultipoint, ObGeographGeometrycollection>(
      g2, g1, context, ObBGStrategyType::PL_PA_STRATEGY, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogCollection, ObWkbGeogMultiLineString, ObGeometry *)
{
  return apply_bg_symdifference_line_coll<ObGeographMultilinestring,
  ObGeographGeometrycollection>(
      g2, g1, context, ObBGStrategyType::LL_LA_AA_STRATEGY, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogCollection, ObWkbGeogMultiPolygon, ObGeometry *)
{
  return apply_bg_symdifference_poly_coll<ObGeographMultipolygon, ObGeographGeometrycollection, ObWkbGeogMultiPolygon>(
      g2, g1, context, ObBGStrategyType::LL_LA_AA_STRATEGY, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogCollection, ObWkbGeogCollection, ObGeometry *)
{
  return eval_symdifference_gc_gc<ObGeographGeometrycollection>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

// tree cartesian polygon
OB_GEO_CART_TREE_FUNC_BEGIN(ObGeoFuncSymDifferenceImpl, ObCartesianPolygon, ObCartesianPolygon, ObGeometry *)
{
  return apply_bg_symdifference<ObCartesianPolygon, ObCartesianPolygon, ObCartesianMultipolygon>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_TREE_FUNC_BEGIN(ObGeoFuncSymDifferenceImpl, ObCartesianPolygon, ObCartesianMultipolygon, ObGeometry *)
{
  return apply_bg_symdifference<ObCartesianPolygon, ObCartesianMultipolygon, ObCartesianMultipolygon>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_TREE_FUNC_BEGIN(ObGeoFuncSymDifferenceImpl, ObCartesianMultipolygon, ObCartesianPolygon, ObGeometry *)
{
  return apply_bg_symdifference<ObCartesianMultipolygon, ObCartesianPolygon, ObCartesianMultipolygon>(g1, g2, context, result);
} OB_GEO_FUNC_END;

}  // namespace common
}  // namespace oceanbase
