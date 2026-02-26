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

using namespace oceanbase::common;
namespace oceanbase
{
namespace common
{

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
