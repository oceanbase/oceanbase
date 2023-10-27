/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_LIB_OB_GEO_DISPATCHER_H_
#define OCEANBASE_LIB_OB_GEO_DISPATCHER_H_

#include "ob_geo_func_common.h"
#include "lib/geo/ob_geo_ibin.h"
#include "lib/oblog/ob_log_module.h"

namespace oceanbase {
namespace common {

static inline int ob_boost_geometry_exception_handle();

// dispatcher base class
template <typename RetType, typename Functype>
class ObIGeoDispatcher
{
public:
  ObIGeoDispatcher();
  virtual ~ObIGeoDispatcher() = default;

  static inline int eval_geo_func(const common::ObGeoEvalCtx &gis_context, RetType &result);

protected:
  static inline int eval_wkb_unary(const common::ObGeometry *g,
                                   const ObGeoEvalCtx &context,
                                   RetType &result);

  static inline int eval_tree_unary(const common::ObGeometry *g,
                                    const ObGeoEvalCtx &context,
                                    RetType &result);

  static inline int eval_wkb_binary(const common::ObGeometry *g1,
                                    const common::ObGeometry *g2,
                                    const ObGeoEvalCtx &context,
                                    RetType &result);

  static inline int eval_tree_binary(const common::ObGeometry *g1,
                                     const common::ObGeometry *g2,
                                     const ObGeoEvalCtx &context,
                                     RetType &result);
};

template <typename RetType, typename Functype>
int ObIGeoDispatcher<RetType, Functype>::eval_tree_unary(const common::ObGeometry *g,
                                                         const ObGeoEvalCtx &context,
                                                         RetType &result)
{
  INIT_SUCC(ret);
  switch (g->crs()) {
  case common::ObGeoCRS::Cartesian: {
    switch (g->type()) {
    case common::ObGeoType::POINT:
      ret = Functype::template EvalTree<ObCartesianPoint> :: eval(g, context, result);
      break;
    case common::ObGeoType::LINESTRING:
      ret = Functype::template EvalTree<ObCartesianLineString> :: eval(g, context, result);
      break;
    case common::ObGeoType::POLYGON:
      ret = Functype::template EvalTree<ObCartesianPolygon> :: eval(g, context, result);
      break;
    case common::ObGeoType::GEOMETRYCOLLECTION:
      ret = Functype::template EvalTree<ObCartesianGeometrycollection> :: eval(g, context, result);
      break;
    case common::ObGeoType::MULTIPOINT:
      ret = Functype::template EvalTree<ObCartesianMultipoint> :: eval(g, context, result);
      break;
    case common::ObGeoType::MULTILINESTRING:
      ret = Functype::template EvalTree<ObCartesianMultilinestring> :: eval(g, context, result);
      break;
    case common::ObGeoType::MULTIPOLYGON:
      ret = Functype::template EvalTree<ObCartesianMultipolygon> :: eval(g, context, result);
      break;
    default:
      ret = OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS;
      break;
    }
    break;
  }
  case common::ObGeoCRS::Geographic: {
    switch (g->type()) {
    case common::ObGeoType::POINT:
      ret = Functype::template EvalTree<ObGeographPoint> :: eval(g, context, result);
      break;
    case common::ObGeoType::LINESTRING:
      ret = Functype::template EvalTree<ObGeographLineString> :: eval(g, context, result);
      break;
    case common::ObGeoType::POLYGON:
      ret = Functype::template EvalTree<ObGeographPolygon> :: eval(g, context, result);
      break;
    case common::ObGeoType::GEOMETRYCOLLECTION:
      ret = Functype::template EvalTree<ObGeographGeometrycollection> :: eval(g, context, result);
      break;
    case common::ObGeoType::MULTIPOINT:
      ret = Functype::template EvalTree<ObGeographMultipoint> :: eval(g, context, result);
      break;
    case common::ObGeoType::MULTILINESTRING:
      ret = Functype::template EvalTree<ObGeographMultilinestring> :: eval(g, context, result);
      break;
    case common::ObGeoType::MULTIPOLYGON:
      ret = Functype::template EvalTree<ObGeographMultipolygon> :: eval(g, context, result);
      break;
    default:
      ret = OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS;
      break;
    }
    break;
  }
  default: {
    ret = OB_ERR_GIS_INVALID_DATA;
    break;
  }
  }
  return ret;
}

template <typename RetType, typename Functype>
int ObIGeoDispatcher<RetType, Functype>::eval_wkb_unary(const common::ObGeometry *g,
                                                         const ObGeoEvalCtx &context,
                                                         RetType &result)
{
  INIT_SUCC(ret);
  switch (g->crs()) {
  case common::ObGeoCRS::Cartesian: {
    switch (g->type()) {
    case common::ObGeoType::POINT:
      ret = Functype::template Eval<ObWkbGeomPoint> :: eval(g, context, result);
      break;
    case common::ObGeoType::LINESTRING:
      ret = Functype::template Eval<ObWkbGeomLineString> :: eval(g, context, result);
      break;
    case common::ObGeoType::POLYGON:
      ret = Functype::template Eval<ObWkbGeomPolygon> :: eval(g, context, result);
      break;
    case common::ObGeoType::GEOMETRYCOLLECTION:
      ret = Functype::template Eval<ObWkbGeomCollection> :: eval(g, context, result);
      break;
    case common::ObGeoType::MULTIPOINT:
      ret = Functype::template Eval<ObWkbGeomMultiPoint> :: eval(g, context, result);
      break;
    case common::ObGeoType::MULTILINESTRING:
      ret = Functype::template Eval<ObWkbGeomMultiLineString> :: eval(g, context, result);
      break;
    case common::ObGeoType::MULTIPOLYGON:
      ret = Functype::template Eval<ObWkbGeomMultiPolygon> :: eval(g, context, result);
      break;
    default:
      ret = OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS;
      break;
    }
    break;
  }
  case common::ObGeoCRS::Geographic: {
    switch (g->type()) {
    case common::ObGeoType::POINT:
      ret = Functype::template Eval<ObWkbGeogPoint> :: eval(g, context, result);
      break;
    case common::ObGeoType::LINESTRING:
      ret = Functype::template Eval<ObWkbGeogLineString> :: eval(g, context, result);
      break;
    case common::ObGeoType::POLYGON:
      ret = Functype::template Eval<ObWkbGeogPolygon> :: eval(g, context, result);
      break;
    case common::ObGeoType::GEOMETRYCOLLECTION:
      ret = Functype::template Eval<ObWkbGeogCollection> :: eval(g, context, result);
      break;
    case common::ObGeoType::MULTIPOINT:
      ret = Functype::template Eval<ObWkbGeogMultiPoint> :: eval(g, context, result);
      break;
    case common::ObGeoType::MULTILINESTRING:
      ret = Functype::template Eval<ObWkbGeogMultiLineString> :: eval(g, context, result);
      break;
    case common::ObGeoType::MULTIPOLYGON:
      ret = Functype::template Eval<ObWkbGeogMultiPolygon> :: eval(g, context, result);
      break;
    default:
      ret = OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS;
      break;
    }
    break;
  }
  default: {
    ret = OB_ERR_GIS_INVALID_DATA;
    break;
  }
  }
  return ret;
}

template <typename RetType, typename Functype>
int ObIGeoDispatcher<RetType, Functype>::eval_wkb_binary(const common::ObGeometry *g1,
                                                         const common::ObGeometry *g2,
                                                         const ObGeoEvalCtx &context,
                                                         RetType &result)
{
  INIT_SUCC(ret);
  if (g1->crs() != g2->crs()) {
    ret = OB_ERR_GIS_DIFFERENT_SRIDS;
  } else {
    switch (g1->crs()) {
    case common::ObGeoCRS::Cartesian:
      switch (g1->type()) {
      case common::ObGeoType::POINT:
        switch (g2->type()) {
        case common::ObGeoType::POINT:
          ret = Functype::template EvalWkbBi<ObWkbGeomPoint, ObWkbGeomPoint> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::LINESTRING:
          ret = Functype::template EvalWkbBi<ObWkbGeomPoint, ObWkbGeomLineString> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::POLYGON:
          ret = Functype::template EvalWkbBi<ObWkbGeomPoint, ObWkbGeomPolygon> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::GEOMETRYCOLLECTION:
          ret = Functype::template EvalWkbBi<ObWkbGeomPoint, ObWkbGeomCollection> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::MULTIPOINT:
          ret = Functype::template EvalWkbBi<ObWkbGeomPoint, ObWkbGeomMultiPoint> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::MULTILINESTRING:
          ret = Functype::template EvalWkbBi<ObWkbGeomPoint, ObWkbGeomMultiLineString> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::MULTIPOLYGON:
          ret = Functype::template EvalWkbBi<ObWkbGeomPoint, ObWkbGeomMultiPolygon> :: eval(g1, g2, context, result);
          break;
        default:
          ret = OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS;
          break;
        }
        break;
      case common::ObGeoType::LINESTRING:
        switch (g2->type()) {
        case common::ObGeoType::POINT:
          ret = Functype::template EvalWkbBi<ObWkbGeomLineString, ObWkbGeomPoint> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::LINESTRING:
          ret = Functype::template EvalWkbBi<ObWkbGeomLineString, ObWkbGeomLineString> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::POLYGON:
          ret = Functype::template EvalWkbBi<ObWkbGeomLineString, ObWkbGeomPolygon> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::GEOMETRYCOLLECTION:
          ret = Functype::template EvalWkbBi<ObWkbGeomLineString, ObWkbGeomCollection> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::MULTIPOINT:
          ret = Functype::template EvalWkbBi<ObWkbGeomLineString, ObWkbGeomMultiPoint> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::MULTILINESTRING:
          ret = Functype::template EvalWkbBi<ObWkbGeomLineString, ObWkbGeomMultiLineString> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::MULTIPOLYGON:
          ret = Functype::template EvalWkbBi<ObWkbGeomLineString, ObWkbGeomMultiPolygon> :: eval(g1, g2, context, result);
          break;
        default:
          ret = OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS;
          break;
        }
        break;
      case common::ObGeoType::POLYGON:
        switch (g2->type()) {
        case common::ObGeoType::POINT:
          ret = Functype::template EvalWkbBi<ObWkbGeomPolygon, ObWkbGeomPoint> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::LINESTRING:
          ret = Functype::template EvalWkbBi<ObWkbGeomPolygon, ObWkbGeomLineString> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::POLYGON:
          ret = Functype::template EvalWkbBi<ObWkbGeomPolygon, ObWkbGeomPolygon> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::GEOMETRYCOLLECTION:
          ret = Functype::template EvalWkbBi<ObWkbGeomPolygon, ObWkbGeomCollection> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::MULTIPOINT:
          ret = Functype::template EvalWkbBi<ObWkbGeomPolygon, ObWkbGeomMultiPoint> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::MULTILINESTRING:
          ret = Functype::template EvalWkbBi<ObWkbGeomPolygon, ObWkbGeomMultiLineString> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::MULTIPOLYGON:
          ret = Functype::template EvalWkbBi<ObWkbGeomPolygon, ObWkbGeomMultiPolygon> :: eval(g1, g2, context, result);
          break;
        default:
          ret = OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS;
          break;
        }
        break;
      case common::ObGeoType::GEOMETRYCOLLECTION:
        switch (g2->type()) {
        case common::ObGeoType::POINT:
          ret = Functype::template EvalWkbBi<ObWkbGeomCollection, ObWkbGeomPoint> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::LINESTRING:
          ret = Functype::template EvalWkbBi<ObWkbGeomCollection, ObWkbGeomLineString> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::POLYGON:
          ret = Functype::template EvalWkbBi<ObWkbGeomCollection, ObWkbGeomPolygon> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::GEOMETRYCOLLECTION:
          ret = Functype::template EvalWkbBi<ObWkbGeomCollection, ObWkbGeomCollection> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::MULTIPOINT:
          ret = Functype::template EvalWkbBi<ObWkbGeomCollection, ObWkbGeomMultiPoint> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::MULTILINESTRING:
          ret = Functype::template EvalWkbBi<ObWkbGeomCollection, ObWkbGeomMultiLineString> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::MULTIPOLYGON:
          ret = Functype::template EvalWkbBi<ObWkbGeomCollection, ObWkbGeomMultiPolygon> :: eval(g1, g2, context, result);
          break;
        default:
          ret = OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS;
          break;
        }
        break;
      case common::ObGeoType::MULTIPOINT:
        switch (g2->type()) {
        case common::ObGeoType::POINT:
          ret = Functype::template EvalWkbBi<ObWkbGeomMultiPoint, ObWkbGeomPoint> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::LINESTRING:
          ret = Functype::template EvalWkbBi<ObWkbGeomMultiPoint, ObWkbGeomLineString> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::POLYGON:
          ret = Functype::template EvalWkbBi<ObWkbGeomMultiPoint, ObWkbGeomPolygon> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::GEOMETRYCOLLECTION:
          ret = Functype::template EvalWkbBi<ObWkbGeomMultiPoint, ObWkbGeomCollection> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::MULTIPOINT:
          ret = Functype::template EvalWkbBi<ObWkbGeomMultiPoint, ObWkbGeomMultiPoint> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::MULTILINESTRING:
          ret = Functype::template EvalWkbBi<ObWkbGeomMultiPoint, ObWkbGeomMultiLineString> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::MULTIPOLYGON:
          ret = Functype::template EvalWkbBi<ObWkbGeomMultiPoint, ObWkbGeomMultiPolygon> :: eval(g1, g2, context, result);
          break;
        default:
          ret = OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS;
          break;
        }
        break;
      case common::ObGeoType::MULTILINESTRING:
        switch (g2->type()) {
        case common::ObGeoType::POINT:
          ret = Functype::template EvalWkbBi<ObWkbGeomMultiLineString, ObWkbGeomPoint> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::LINESTRING:
          ret = Functype::template EvalWkbBi<ObWkbGeomMultiLineString, ObWkbGeomLineString> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::POLYGON:
          ret = Functype::template EvalWkbBi<ObWkbGeomMultiLineString, ObWkbGeomPolygon> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::GEOMETRYCOLLECTION:
          ret = Functype::template EvalWkbBi<ObWkbGeomMultiLineString, ObWkbGeomCollection> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::MULTIPOINT:
          ret = Functype::template EvalWkbBi<ObWkbGeomMultiLineString, ObWkbGeomMultiPoint> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::MULTILINESTRING:
          ret = Functype::template EvalWkbBi<ObWkbGeomMultiLineString, ObWkbGeomMultiLineString> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::MULTIPOLYGON:
          ret = Functype::template EvalWkbBi<ObWkbGeomMultiLineString, ObWkbGeomMultiPolygon> :: eval(g1, g2, context, result);
          break;
        default:
          ret = OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS;
          break;
        }
        break;
      case common::ObGeoType::MULTIPOLYGON:
        switch (g2->type()) {
        case common::ObGeoType::POINT:
          ret = Functype::template EvalWkbBi<ObWkbGeomMultiPolygon, ObWkbGeomPoint> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::LINESTRING:
          ret = Functype::template EvalWkbBi<ObWkbGeomMultiPolygon, ObWkbGeomLineString> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::POLYGON:
          ret = Functype::template EvalWkbBi<ObWkbGeomMultiPolygon, ObWkbGeomPolygon> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::GEOMETRYCOLLECTION:
          ret = Functype::template EvalWkbBi<ObWkbGeomMultiPolygon, ObWkbGeomCollection> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::MULTIPOINT:
          ret = Functype::template EvalWkbBi<ObWkbGeomMultiPolygon, ObWkbGeomMultiPoint> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::MULTILINESTRING:
          ret = Functype::template EvalWkbBi<ObWkbGeomMultiPolygon, ObWkbGeomMultiLineString> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::MULTIPOLYGON:
          ret = Functype::template EvalWkbBi<ObWkbGeomMultiPolygon, ObWkbGeomMultiPolygon> :: eval(g1, g2, context, result);
          break;
        default:
          ret = OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS;
          break;
        }
        break;
      default:
        ret = OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS;
        break;
      }
      break;
    // end case common::ObGeoCRS::Cartesian
    case common::ObGeoCRS::Geographic:
      switch (g1->type()) {
      case common::ObGeoType::POINT:
        switch (g2->type()) {
        case common::ObGeoType::POINT:
          ret = Functype::template EvalWkbBiGeog<ObWkbGeogPoint, ObWkbGeogPoint> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::LINESTRING:
          ret = Functype::template EvalWkbBiGeog<ObWkbGeogPoint, ObWkbGeogLineString> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::POLYGON:
          ret = Functype::template EvalWkbBiGeog<ObWkbGeogPoint, ObWkbGeogPolygon> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::GEOMETRYCOLLECTION:
          ret = Functype::template EvalWkbBiGeog<ObWkbGeogPoint, ObWkbGeogCollection> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::MULTIPOINT:
          ret = Functype::template EvalWkbBiGeog<ObWkbGeogPoint, ObWkbGeogMultiPoint> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::MULTILINESTRING:
          ret = Functype::template EvalWkbBiGeog<ObWkbGeogPoint, ObWkbGeogMultiLineString> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::MULTIPOLYGON:
          ret = Functype::template EvalWkbBiGeog<ObWkbGeogPoint, ObWkbGeogMultiPolygon> :: eval(g1, g2, context, result);
          break;
        default:
          ret = OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS;
          break;
        }
        break;
      case common::ObGeoType::LINESTRING:
        switch (g2->type()) {
        case common::ObGeoType::POINT:
          ret = Functype::template EvalWkbBiGeog<ObWkbGeogLineString, ObWkbGeogPoint> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::LINESTRING:
          ret = Functype::template EvalWkbBiGeog<ObWkbGeogLineString, ObWkbGeogLineString> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::POLYGON:
          ret = Functype::template EvalWkbBiGeog<ObWkbGeogLineString, ObWkbGeogPolygon> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::GEOMETRYCOLLECTION:
          ret = Functype::template EvalWkbBiGeog<ObWkbGeogLineString, ObWkbGeogCollection> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::MULTIPOINT:
          ret = Functype::template EvalWkbBiGeog<ObWkbGeogLineString, ObWkbGeogMultiPoint> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::MULTILINESTRING:
          ret = Functype::template EvalWkbBiGeog<ObWkbGeogLineString, ObWkbGeogMultiLineString> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::MULTIPOLYGON:
          ret = Functype::template EvalWkbBiGeog<ObWkbGeogLineString, ObWkbGeogMultiPolygon> :: eval(g1, g2, context, result);
          break;
        default:
          ret = OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS;
          break;
        }
        break;
      case common::ObGeoType::POLYGON:
        switch (g2->type()) {
        case common::ObGeoType::POINT:
          ret = Functype::template EvalWkbBiGeog<ObWkbGeogPolygon, ObWkbGeogPoint> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::LINESTRING:
          ret = Functype::template EvalWkbBiGeog<ObWkbGeogPolygon, ObWkbGeogLineString> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::POLYGON:
          ret = Functype::template EvalWkbBiGeog<ObWkbGeogPolygon, ObWkbGeogPolygon> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::GEOMETRYCOLLECTION:
          ret = Functype::template EvalWkbBiGeog<ObWkbGeogPolygon, ObWkbGeogCollection> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::MULTIPOINT:
          ret = Functype::template EvalWkbBiGeog<ObWkbGeogPolygon, ObWkbGeogMultiPoint> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::MULTILINESTRING:
          ret = Functype::template EvalWkbBiGeog<ObWkbGeogPolygon, ObWkbGeogMultiLineString> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::MULTIPOLYGON:
          ret = Functype::template EvalWkbBiGeog<ObWkbGeogPolygon, ObWkbGeogMultiPolygon> :: eval(g1, g2, context, result);
          break;
        default:
          ret = OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS;
          break;
        }
        break;
      case common::ObGeoType::GEOMETRYCOLLECTION:
        switch (g2->type()) {
        case common::ObGeoType::POINT:
          ret = Functype::template EvalWkbBiGeog<ObWkbGeogCollection, ObWkbGeogPoint> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::LINESTRING:
          ret = Functype::template EvalWkbBiGeog<ObWkbGeogCollection, ObWkbGeogLineString> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::POLYGON:
          ret = Functype::template EvalWkbBiGeog<ObWkbGeogCollection, ObWkbGeogPolygon> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::GEOMETRYCOLLECTION:
          ret = Functype::template EvalWkbBiGeog<ObWkbGeogCollection, ObWkbGeogCollection> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::MULTIPOINT:
          ret = Functype::template EvalWkbBiGeog<ObWkbGeogCollection, ObWkbGeogMultiPoint> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::MULTILINESTRING:
          ret = Functype::template EvalWkbBiGeog<ObWkbGeogCollection, ObWkbGeogMultiLineString> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::MULTIPOLYGON:
          ret = Functype::template EvalWkbBiGeog<ObWkbGeogCollection, ObWkbGeogMultiPolygon> :: eval(g1, g2, context, result);
          break;
        default:
          ret = OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS;
          break;
        }
        break;
      case common::ObGeoType::MULTIPOINT:
        switch (g2->type()) {
        case common::ObGeoType::POINT:
          ret = Functype::template EvalWkbBiGeog<ObWkbGeogMultiPoint, ObWkbGeogPoint> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::LINESTRING:
          ret = Functype::template EvalWkbBiGeog<ObWkbGeogMultiPoint, ObWkbGeogLineString> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::POLYGON:
          ret = Functype::template EvalWkbBiGeog<ObWkbGeogMultiPoint, ObWkbGeogPolygon> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::GEOMETRYCOLLECTION:
          ret = Functype::template EvalWkbBiGeog<ObWkbGeogMultiPoint, ObWkbGeogCollection> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::MULTIPOINT:
          ret = Functype::template EvalWkbBiGeog<ObWkbGeogMultiPoint, ObWkbGeogMultiPoint> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::MULTILINESTRING:
          ret = Functype::template EvalWkbBiGeog<ObWkbGeogMultiPoint, ObWkbGeogMultiLineString> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::MULTIPOLYGON:
          ret = Functype::template EvalWkbBiGeog<ObWkbGeogMultiPoint, ObWkbGeogMultiPolygon> :: eval(g1, g2, context, result);
          break;
        default:
          ret = OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS;
          break;
        }
        break;
      case common::ObGeoType::MULTILINESTRING:
        switch (g2->type()) {
        case common::ObGeoType::POINT:
          ret = Functype::template EvalWkbBiGeog<ObWkbGeogMultiLineString, ObWkbGeogPoint> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::LINESTRING:
          ret = Functype::template EvalWkbBiGeog<ObWkbGeogMultiLineString, ObWkbGeogLineString> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::POLYGON:
          ret = Functype::template EvalWkbBiGeog<ObWkbGeogMultiLineString, ObWkbGeogPolygon> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::GEOMETRYCOLLECTION:
          ret = Functype::template EvalWkbBiGeog<ObWkbGeogMultiLineString, ObWkbGeogCollection> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::MULTIPOINT:
          ret = Functype::template EvalWkbBiGeog<ObWkbGeogMultiLineString, ObWkbGeogMultiPoint> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::MULTILINESTRING:
          ret = Functype::template EvalWkbBiGeog<ObWkbGeogMultiLineString, ObWkbGeogMultiLineString> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::MULTIPOLYGON:
          ret = Functype::template EvalWkbBiGeog<ObWkbGeogMultiLineString, ObWkbGeogMultiPolygon> :: eval(g1, g2, context, result);
          break;
        default:
          ret = OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS;
          break;
        }
        break;
      case common::ObGeoType::MULTIPOLYGON:
        switch (g2->type()) {
        case common::ObGeoType::POINT:
          ret = Functype::template EvalWkbBiGeog<ObWkbGeogMultiPolygon, ObWkbGeogPoint> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::LINESTRING:
          ret = Functype::template EvalWkbBiGeog<ObWkbGeogMultiPolygon, ObWkbGeogLineString> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::POLYGON:
          ret = Functype::template EvalWkbBiGeog<ObWkbGeogMultiPolygon, ObWkbGeogPolygon> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::GEOMETRYCOLLECTION:
          ret = Functype::template EvalWkbBiGeog<ObWkbGeogMultiPolygon, ObWkbGeogCollection> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::MULTIPOINT:
          ret = Functype::template EvalWkbBiGeog<ObWkbGeogMultiPolygon, ObWkbGeogMultiPoint> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::MULTILINESTRING:
          ret = Functype::template EvalWkbBiGeog<ObWkbGeogMultiPolygon, ObWkbGeogMultiLineString> :: eval(g1, g2, context, result);
          break;
        case common::ObGeoType::MULTIPOLYGON:
          ret = Functype::template EvalWkbBiGeog<ObWkbGeogMultiPolygon, ObWkbGeogMultiPolygon> :: eval(g1, g2, context, result);
          break;
        default:
          ret = OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS;
          break;
        }
        break;
      default:
        ret = OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS;
        break;
      }
      break;
    default:
      ret = OB_ERR_GIS_INVALID_DATA;
      break;
    }
  }
  return ret;
}



template <typename RetType, typename Functype>
int ObIGeoDispatcher<RetType, Functype>::eval_tree_binary(const common::ObGeometry *g1,
                                                          const common::ObGeometry *g2,
                                                          const ObGeoEvalCtx &context,
                                                          RetType &result)
{
  INIT_SUCC(ret);
  if (g1->crs() != g2->crs()) {
    ret = OB_ERR_GIS_DIFFERENT_SRIDS;
  } else {
    switch (g1->crs()) {
    case common::ObGeoCRS::Cartesian:
      switch (g1->type()) {
        case common::ObGeoType::POINT:
          switch (g2->type()) {
            case common::ObGeoType::POINT:
              ret = Functype::template EvalTreeBi<ObCartesianPoint, ObCartesianPoint> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::LINESTRING:
              ret = Functype::template EvalTreeBi<ObCartesianPoint, ObCartesianLineString> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::POLYGON:
              ret = Functype::template EvalTreeBi<ObCartesianPoint, ObCartesianPolygon> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::GEOMETRYCOLLECTION:
              ret = Functype::template EvalTreeBi<ObCartesianPoint, ObCartesianGeometrycollection> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::MULTIPOINT:
              ret = Functype::template EvalTreeBi<ObCartesianPoint, ObCartesianMultipoint> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::MULTILINESTRING:
              ret = Functype::template EvalTreeBi<ObCartesianPoint, ObCartesianMultilinestring> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::MULTIPOLYGON:
              ret = Functype::template EvalTreeBi<ObCartesianPoint, ObCartesianMultipolygon> :: eval(g1, g2, context, result);
              break;
            default:
              ret = OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS;
              break;
          }
          break;
        case common::ObGeoType::LINESTRING:
          switch (g2->type()) {
            case common::ObGeoType::POINT:
              ret = Functype::template EvalTreeBi<ObCartesianLineString, ObCartesianPoint> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::LINESTRING:
              ret = Functype::template EvalTreeBi<ObCartesianLineString, ObCartesianLineString> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::POLYGON:
              ret = Functype::template EvalTreeBi<ObCartesianLineString, ObCartesianPolygon> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::GEOMETRYCOLLECTION:
              ret = Functype::template EvalTreeBi<ObCartesianLineString, ObCartesianGeometrycollection> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::MULTIPOINT:
              ret = Functype::template EvalTreeBi<ObCartesianLineString, ObCartesianMultipoint> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::MULTILINESTRING:
              ret = Functype::template EvalTreeBi<ObCartesianLineString, ObCartesianMultilinestring> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::MULTIPOLYGON:
              ret = Functype::template EvalTreeBi<ObCartesianLineString, ObCartesianMultipolygon> :: eval(g1, g2, context, result);
              break;
            default:
              ret = OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS;
              break;
          }
          break;
        case common::ObGeoType::POLYGON:
          switch (g2->type()) {
            case common::ObGeoType::POINT:
              ret = Functype::template EvalTreeBi<ObCartesianPolygon, ObCartesianPoint> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::LINESTRING:
              ret = Functype::template EvalTreeBi<ObCartesianPolygon, ObCartesianLineString> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::POLYGON:
              ret = Functype::template EvalTreeBi<ObCartesianPolygon, ObCartesianPolygon> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::GEOMETRYCOLLECTION:
              ret = Functype::template EvalTreeBi<ObCartesianPolygon, ObCartesianGeometrycollection> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::MULTIPOINT:
              ret = Functype::template EvalTreeBi<ObCartesianPolygon, ObCartesianMultipoint> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::MULTILINESTRING:
              ret = Functype::template EvalTreeBi<ObCartesianPolygon, ObCartesianMultilinestring> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::MULTIPOLYGON:
              ret = Functype::template EvalTreeBi<ObCartesianPolygon, ObCartesianMultipolygon> :: eval(g1, g2, context, result);
              break;
            default:
              ret = OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS;
              break;
          }
          break;
        case common::ObGeoType::GEOMETRYCOLLECTION:
          switch (g2->type()) {
            case common::ObGeoType::POINT:
              ret = Functype::template EvalTreeBi<ObCartesianGeometrycollection, ObCartesianPoint> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::LINESTRING:
              ret = Functype::template EvalTreeBi<ObCartesianGeometrycollection, ObCartesianLineString> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::POLYGON:
              ret = Functype::template EvalTreeBi<ObCartesianGeometrycollection, ObCartesianPolygon> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::GEOMETRYCOLLECTION:
              ret = Functype::template EvalTreeBi<ObCartesianGeometrycollection, ObCartesianGeometrycollection> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::MULTIPOINT:
              ret = Functype::template EvalTreeBi<ObCartesianGeometrycollection, ObCartesianMultipoint> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::MULTILINESTRING:
              ret = Functype::template EvalTreeBi<ObCartesianGeometrycollection, ObCartesianMultilinestring> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::MULTIPOLYGON:
              ret = Functype::template EvalTreeBi<ObCartesianGeometrycollection, ObCartesianMultipolygon> :: eval(g1, g2, context, result);
              break;
            default:
              ret = OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS;
              break;
          }
          break;
        case common::ObGeoType::MULTIPOINT:
          switch (g2->type()) {
            case common::ObGeoType::POINT:
              ret = Functype::template EvalTreeBi<ObCartesianMultipoint, ObCartesianPoint> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::LINESTRING:
              ret = Functype::template EvalTreeBi<ObCartesianMultipoint, ObCartesianLineString> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::POLYGON:
              ret = Functype::template EvalTreeBi<ObCartesianMultipoint, ObCartesianPolygon> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::GEOMETRYCOLLECTION:
              ret = Functype::template EvalTreeBi<ObCartesianMultipoint, ObCartesianGeometrycollection> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::MULTIPOINT:
              ret = Functype::template EvalTreeBi<ObCartesianMultipoint, ObCartesianMultipoint> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::MULTILINESTRING:
              ret = Functype::template EvalTreeBi<ObCartesianMultipoint, ObCartesianMultilinestring> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::MULTIPOLYGON:
              ret = Functype::template EvalTreeBi<ObCartesianMultipoint, ObCartesianMultipolygon> :: eval(g1, g2, context, result);
              break;
            default:
              ret = OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS;
              break;
          }
          break;
        case common::ObGeoType::MULTILINESTRING:
          switch (g2->type()) {
            case common::ObGeoType::POINT:
              ret = Functype::template EvalTreeBi<ObCartesianMultilinestring, ObCartesianPoint> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::LINESTRING:
              ret = Functype::template EvalTreeBi<ObCartesianMultilinestring, ObCartesianLineString> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::POLYGON:
              ret = Functype::template EvalTreeBi<ObCartesianMultilinestring, ObCartesianPolygon> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::GEOMETRYCOLLECTION:
              ret = Functype::template EvalTreeBi<ObCartesianMultilinestring, ObCartesianGeometrycollection> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::MULTIPOINT:
              ret = Functype::template EvalTreeBi<ObCartesianMultilinestring, ObCartesianMultipoint> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::MULTILINESTRING:
              ret = Functype::template EvalTreeBi<ObCartesianMultilinestring, ObCartesianMultilinestring> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::MULTIPOLYGON:
              ret = Functype::template EvalTreeBi<ObCartesianMultilinestring, ObCartesianMultipolygon> :: eval(g1, g2, context, result);
              break;
            default:
              ret = OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS;
              break;
          }
          break;
        case common::ObGeoType::MULTIPOLYGON:
          switch (g2->type()) {
            case common::ObGeoType::POINT:
              ret = Functype::template EvalTreeBi<ObCartesianMultipolygon, ObCartesianPoint> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::LINESTRING:
              ret = Functype::template EvalTreeBi<ObCartesianMultipolygon, ObCartesianLineString> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::POLYGON:
              ret = Functype::template EvalTreeBi<ObCartesianMultipolygon, ObCartesianPolygon> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::GEOMETRYCOLLECTION:
              ret = Functype::template EvalTreeBi<ObCartesianMultipolygon, ObCartesianGeometrycollection> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::MULTIPOINT:
              ret = Functype::template EvalTreeBi<ObCartesianMultipolygon, ObCartesianMultipoint> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::MULTILINESTRING:
              ret = Functype::template EvalTreeBi<ObCartesianMultipolygon, ObCartesianMultilinestring> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::MULTIPOLYGON:
              ret = Functype::template EvalTreeBi<ObCartesianMultipolygon, ObCartesianMultipolygon> :: eval(g1, g2, context, result);
              break;
            default:
              ret = OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS;
              break;
          }
          break;
        default:
          ret = OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS;
          break;
      }
      break;
    // end case common::ObGeoCRS::Cartesian
    case common::ObGeoCRS::Geographic:
      switch (g1->type()) {
        case common::ObGeoType::POINT:
          switch (g2->type()) {
            case common::ObGeoType::POINT:
              ret = Functype::template EvalTreeBiGeog<ObGeographPoint, ObGeographPoint> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::LINESTRING:
              ret = Functype::template EvalTreeBiGeog<ObGeographPoint, ObGeographLineString> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::POLYGON:
              ret = Functype::template EvalTreeBiGeog<ObGeographPoint, ObGeographPolygon> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::GEOMETRYCOLLECTION:
              ret = Functype::template EvalTreeBiGeog<ObGeographPoint, ObGeographGeometrycollection> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::MULTIPOINT:
              ret = Functype::template EvalTreeBiGeog<ObGeographPoint, ObGeographMultipoint> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::MULTILINESTRING:
              ret = Functype::template EvalTreeBiGeog<ObGeographPoint, ObGeographMultilinestring> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::MULTIPOLYGON:
              ret = Functype::template EvalTreeBiGeog<ObGeographPoint, ObGeographMultipolygon> :: eval(g1, g2, context, result);
              break;
            default:
              ret = OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS;
              break;
          }
          break;
        case common::ObGeoType::LINESTRING:
          switch (g2->type()) {
            case common::ObGeoType::POINT:
              ret = Functype::template EvalTreeBiGeog<ObGeographLineString, ObGeographPoint> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::LINESTRING:
              ret = Functype::template EvalTreeBiGeog<ObGeographLineString, ObGeographLineString> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::POLYGON:
              ret = Functype::template EvalTreeBiGeog<ObGeographLineString, ObGeographPolygon> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::GEOMETRYCOLLECTION:
              ret = Functype::template EvalTreeBiGeog<ObGeographLineString, ObGeographGeometrycollection> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::MULTIPOINT:
              ret = Functype::template EvalTreeBiGeog<ObGeographLineString, ObGeographMultipoint> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::MULTILINESTRING:
              ret = Functype::template EvalTreeBiGeog<ObGeographLineString, ObGeographMultilinestring> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::MULTIPOLYGON:
              ret = Functype::template EvalTreeBiGeog<ObGeographLineString, ObGeographMultipolygon> :: eval(g1, g2, context, result);
              break;
            default:
              ret = OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS;
              break;
          }
          break;
        case common::ObGeoType::POLYGON:
          switch (g2->type()) {
            case common::ObGeoType::POINT:
              ret = Functype::template EvalTreeBiGeog<ObGeographPolygon, ObGeographPoint> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::LINESTRING:
              ret = Functype::template EvalTreeBiGeog<ObGeographPolygon, ObGeographLineString> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::POLYGON:
              ret = Functype::template EvalTreeBiGeog<ObGeographPolygon, ObGeographPolygon> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::GEOMETRYCOLLECTION:
              ret = Functype::template EvalTreeBiGeog<ObGeographPolygon, ObGeographGeometrycollection> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::MULTIPOINT:
              ret = Functype::template EvalTreeBiGeog<ObGeographPolygon, ObGeographMultipoint> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::MULTILINESTRING:
              ret = Functype::template EvalTreeBiGeog<ObGeographPolygon, ObGeographMultilinestring> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::MULTIPOLYGON:
              ret = Functype::template EvalTreeBiGeog<ObGeographPolygon, ObGeographMultipolygon> :: eval(g1, g2, context, result);
              break;
            default:
              ret = OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS;
              break;
          }
          break;
        case common::ObGeoType::GEOMETRYCOLLECTION:
          switch (g2->type()) {
            case common::ObGeoType::POINT:
              ret = Functype::template EvalTreeBiGeog<ObGeographGeometrycollection, ObGeographPoint> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::LINESTRING:
              ret = Functype::template EvalTreeBiGeog<ObGeographGeometrycollection, ObGeographLineString> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::POLYGON:
              ret = Functype::template EvalTreeBiGeog<ObGeographGeometrycollection, ObGeographPolygon> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::GEOMETRYCOLLECTION:
              ret = Functype::template EvalTreeBiGeog<ObGeographGeometrycollection, ObGeographGeometrycollection> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::MULTIPOINT:
              ret = Functype::template EvalTreeBiGeog<ObGeographGeometrycollection, ObGeographMultipoint> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::MULTILINESTRING:
              ret = Functype::template EvalTreeBiGeog<ObGeographGeometrycollection, ObGeographMultilinestring> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::MULTIPOLYGON:
              ret = Functype::template EvalTreeBiGeog<ObGeographGeometrycollection, ObGeographMultipolygon> :: eval(g1, g2, context, result);
              break;
            default:
              ret = OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS;
              break;
          }
          break;
        case common::ObGeoType::MULTIPOINT:
          switch (g2->type()) {
            case common::ObGeoType::POINT:
              ret = Functype::template EvalTreeBiGeog<ObGeographMultipoint, ObGeographPoint> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::LINESTRING:
              ret = Functype::template EvalTreeBiGeog<ObGeographMultipoint, ObGeographLineString> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::POLYGON:
              ret = Functype::template EvalTreeBiGeog<ObGeographMultipoint, ObGeographPolygon> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::GEOMETRYCOLLECTION:
              ret = Functype::template EvalTreeBiGeog<ObGeographMultipoint, ObGeographGeometrycollection> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::MULTIPOINT:
              ret = Functype::template EvalTreeBiGeog<ObGeographMultipoint, ObGeographMultipoint> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::MULTILINESTRING:
              ret = Functype::template EvalTreeBiGeog<ObGeographMultipoint, ObGeographMultilinestring> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::MULTIPOLYGON:
              ret = Functype::template EvalTreeBiGeog<ObGeographMultipoint, ObGeographMultipolygon> :: eval(g1, g2, context, result);
              break;
            default:
              ret = OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS;
              break;
          }
          break;
        case common::ObGeoType::MULTILINESTRING:
          switch (g2->type()) {
            case common::ObGeoType::POINT:
              ret = Functype::template EvalTreeBiGeog<ObGeographMultilinestring, ObGeographPoint> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::LINESTRING:
              ret = Functype::template EvalTreeBiGeog<ObGeographMultilinestring, ObGeographLineString> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::POLYGON:
              ret = Functype::template EvalTreeBiGeog<ObGeographMultilinestring, ObGeographPolygon> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::GEOMETRYCOLLECTION:
              ret = Functype::template EvalTreeBiGeog<ObGeographMultilinestring, ObGeographGeometrycollection> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::MULTIPOINT:
              ret = Functype::template EvalTreeBiGeog<ObGeographMultilinestring, ObGeographMultipoint> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::MULTILINESTRING:
              ret = Functype::template EvalTreeBiGeog<ObGeographMultilinestring, ObGeographMultilinestring> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::MULTIPOLYGON:
              ret = Functype::template EvalTreeBiGeog<ObGeographMultilinestring, ObGeographMultipolygon> :: eval(g1, g2, context, result);
              break;
            default:
              ret = OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS;
              break;
          }
          break;
        case common::ObGeoType::MULTIPOLYGON:
          switch (g2->type()) {
            case common::ObGeoType::POINT:
              ret = Functype::template EvalTreeBiGeog<ObGeographMultipolygon, ObGeographPoint> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::LINESTRING:
              ret = Functype::template EvalTreeBiGeog<ObGeographMultipolygon, ObGeographLineString> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::POLYGON:
              ret = Functype::template EvalTreeBiGeog<ObGeographMultipolygon, ObGeographPolygon> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::GEOMETRYCOLLECTION:
              ret = Functype::template EvalTreeBiGeog<ObGeographMultipolygon, ObGeographGeometrycollection> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::MULTIPOINT:
              ret = Functype::template EvalTreeBiGeog<ObGeographMultipolygon, ObGeographMultipoint> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::MULTILINESTRING:
              ret = Functype::template EvalTreeBiGeog<ObGeographMultipolygon, ObGeographMultilinestring> :: eval(g1, g2, context, result);
              break;
            case common::ObGeoType::MULTIPOLYGON:
              ret = Functype::template EvalTreeBiGeog<ObGeographMultipolygon, ObGeographMultipolygon> :: eval(g1, g2, context, result);
              break;
            default:
              ret = OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS;
              break;
          }
          break;
        default:
          ret = OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS;
          break;
      }
      break;
    default:
      ret = OB_ERR_GIS_INVALID_DATA;
      break;
    }
  }
  return ret;
}

template <typename RetType, typename Functype>
int ObIGeoDispatcher<RetType, Functype>::eval_geo_func(
    const common::ObGeoEvalCtx &gis_context, RetType &result)
{
  INIT_SUCC(ret);
  try {
    switch (gis_context.get_geo_count()) {
    case 1: {
      const common::ObGeometry *g = gis_context.get_geo_arg(0);
      if (OB_ISNULL(g)) {
        ret = OB_ERR_NULL_VALUE;
      } else if (g->is_tree()) {
        ret = eval_tree_unary(g, gis_context, result);
      } else if (OB_ISNULL(g->val())) {
        ret = OB_ERR_NULL_VALUE; // error log at end of func
      } else {
        ret = eval_wkb_unary(g, gis_context, result);
      }
      break;
    }
    case 2: {
      const common::ObGeometry *g1 = gis_context.get_geo_arg(0);
      const common::ObGeometry *g2 = gis_context.get_geo_arg(1);
      if  (OB_ISNULL(g1) || OB_ISNULL(g2)) {
        ret = OB_ERR_NULL_VALUE; // error log at end of func
      } else if (g1->crs() != g2->crs()) {
        ret = OB_ERR_GIS_DIFFERENT_SRIDS;
      } else if (g1->crs() == common::ObGeoCRS::Geographic && OB_ISNULL(gis_context.get_srs())) {
        ret = OB_ERR_NULL_VALUE;
      } else if (g1->is_tree()) {
        ret = eval_tree_binary(g1, g2, gis_context, result);
      } else if (OB_ISNULL(g1->val()) || OB_ISNULL(g2->val())) {
        ret = OB_ERR_NULL_VALUE;
      } else {
        ret = eval_wkb_binary(g1, g2, gis_context, result);
      }
      break;
    }
    default: {
      ret = OB_ERR_GIS_INVALID_DATA;
      break;
    }
    }
  } catch (...) {
    ret = ob_boost_geometry_exception_handle();
  }

  if (OB_FAIL(ret)) {
    switch(gis_context.get_geo_count()) {
      case 1:{
        OB_LOG(WARN, "geo dispatcher failed", K(ret), K(gis_context.get_geo_arg(0)->crs()),
            K(gis_context.get_geo_arg(0)->type()));
        break;
      }
      case 2:{
        OB_LOG(WARN, "geo dispatcher failed", K(ret), K(gis_context.get_geo_arg(0)->crs()),
            K(gis_context.get_geo_arg(0)->type()), K(gis_context.get_geo_arg(1)->crs()),
            K(gis_context.get_geo_arg(1)->type()));
        break;
      }
      default:{
        OB_LOG(WARN, "geo dispatcher failed with unexpected geo arguments", K(ret),
            K(gis_context.get_geo_count()));
        break;
      }
    }
  }
  return ret;
}

static inline int ob_boost_geometry_exception_handle()
{
  INIT_SUCC(ret);
  try {
    throw;
  } catch (const std::bad_alloc &e) { // std errors
    ret = OB_ERR_STD_BAD_ALLOC_ERROR;
  } catch (const std::domain_error &e) {
    ret = OB_ERR_STD_DOMAIN_ERROR;
  } catch (const std::length_error &e) {
    ret = OB_ERR_STD_LENGTH_ERROR;
  } catch (const std::invalid_argument &e) {
    ret = OB_ERR_STD_INVALID_ARGUMENT;
  } catch (const std::out_of_range &e) {
    ret = OB_ERR_STD_OUT_OF_RANGE_ERROR;
  } catch (const std::overflow_error &e) {
    ret = OB_ERR_STD_OVERFLOW_ERROR;
  } catch (const std::range_error &e) {
    ret = OB_ERR_STD_RANGE_ERROR;
  } catch (const std::underflow_error &e) {
    ret = OB_ERR_STD_UNDERFLOW_ERROR;
  } catch (const std::logic_error &e) {
    ret = OB_ERR_STD_LOGIC_ERROR;
  } catch (const std::runtime_error &e) {
    ret = OB_ERR_STD_RUNTIME_ERROR;
  } catch (const boost::geometry::centroid_exception &) { // boost errors
    ret = OB_ERR_BOOST_GEOMETRY_CENTROID_EXCEPTION;
  } catch (const boost::geometry::overlay_invalid_input_exception &) {
    ret = OB_ERR_BOOST_GEOMETRY_OVERLAY_INVALID_INPUT_EXCEPTION;
  } catch (const boost::geometry::turn_info_exception &) {
    ret = OB_ERR_BOOST_GEOMETRY_TURN_INFO_EXCEPTION;
  } catch (const boost::geometry::empty_input_exception &) {
    ret = OB_ERR_BOOST_GEOMETRY_EMPTY_INPUT_EXCEPTION;
  } catch (const boost::geometry::inconsistent_turns_exception &) {
    ret = OB_ERR_BOOST_GEOMETRY_INCONSISTENT_TURNS_EXCEPTION;
  } catch (const boost::geometry::exception &) {
    ret = OB_ERR_BOOST_GEOMETRY_UNKNOWN_EXCEPTION;
  } catch (const boost::numeric::positive_overflow &) {
    ret = OB_OPERATE_OVERFLOW;
  } catch (const boost::numeric::negative_overflow &) {
    ret = OB_OPERATE_OVERFLOW;
  // other exceptions should add before this line
  } catch (const std::exception &e) {
    ret = OB_ERR_STD_RUNTIME_ERROR;
  } catch (...) {
    ret = OB_ERR_GIS_UNKNOWN_EXCEPTION;
  }
  return ret;
}

#define OB_GEO_UNARY_FUNC_DEFAULT(RESULT_TYPE, DEFAULT_RET)                                 \
template <typename GeometyType>                                                             \
  struct Eval {                                                                             \
    static int eval(const common::ObGeometry *g, const ObGeoEvalCtx &context, RESULT_TYPE &result)  \
    {                                                                                       \
      UNUSEDx(g, context, result);                                                          \
      return DEFAULT_RET;                                                                   \
    }                                                                                       \
  }

#define OB_GEO_TREE_UNARY_FUNC_DEFAULT(RESULT_TYPE, DEFAULT_RET)                            \
template <typename GeometyType>                                                             \
  struct EvalTree {                                                                         \
    static int eval(const common::ObGeometry *g, const ObGeoEvalCtx &context, RESULT_TYPE &result)  \
    {                                                                                       \
      UNUSEDx(g, context, result);                                                          \
      return DEFAULT_RET;                                                                   \
    }                                                                                       \
  }

#define OB_GEO_CART_BINARY_FUNC_DEFAULT(RESULT_TYPE, DEFAULT_RET)                           \
template <typename GeometyType1, typename GeometyType2>                                     \
  struct EvalWkbBi {                                                                        \
    static int eval(const common::ObGeometry *g1, const common::ObGeometry *g2,             \
                    const ObGeoEvalCtx &context, RESULT_TYPE &result)                       \
    {                                                                                       \
      UNUSEDx(g1, g2, context, result);                                                     \
      return DEFAULT_RET;                                                                   \
    }                                                                                       \
  }


#define OB_GEO_GEOG_BINARY_FUNC_DEFAULT(RESULT_TYPE, DEFAULT_RET)                           \
template <typename GeometyType1, typename GeometyType2>                                     \
  struct EvalWkbBiGeog {                                                                    \
    static int eval(const common::ObGeometry *g1, const common::ObGeometry *g2,             \
                    const ObGeoEvalCtx &context, RESULT_TYPE &result)                       \
    {                                                                                       \
      UNUSEDx(g1, g2, context, result);                                                     \
      return DEFAULT_RET;                                                                   \
    }                                                                                       \
  }

#define OB_GEO_CART_TREE_FUNC_DEFAULT(RESULT_TYPE, DEFAULT_RET)                             \
template <typename GeometyType1, typename GeometyType2>                                     \
  struct EvalTreeBi {                                                                       \
    static int eval(const common::ObGeometry *g1, const common::ObGeometry *g2,             \
                    const ObGeoEvalCtx &context, RESULT_TYPE &result)                       \
    {                                                                                       \
      UNUSEDx(g1, g2, context, result);                                                     \
      return DEFAULT_RET;                                                                   \
    }                                                                                       \
  }


#define OB_GEO_GEOG_TREE_FUNC_DEFAULT(RESULT_TYPE, DEFAULT_RET)                             \
template <typename GeometyType1, typename GeometyType2>                                     \
  struct EvalTreeBiGeog {                                                                   \
    static int eval(const common::ObGeometry *g1, const common::ObGeometry *g2,             \
                    const ObGeoEvalCtx &context, RESULT_TYPE &result)                       \
    {                                                                                       \
      UNUSEDx(g1, g2, context, result);                                                     \
      return DEFAULT_RET;                                                                   \
    }                                                                                       \
  }

#define OB_GEO_UNARY_FUNC_BEGIN(FUNC_TYPE, GEO_TYPE, RESULT_TYPE)                           \
template <>                                                                                 \
  struct FUNC_TYPE::Eval<GEO_TYPE> {                                                        \
    static int eval(const common::ObGeometry *g, const ObGeoEvalCtx &context, RESULT_TYPE &result)

#define OB_GEO_UNARY_TREE_FUNC_BEGIN(FUNC_TYPE, GEO_TYPE, RESULT_TYPE)                      \
template <>                                                                                 \
  struct FUNC_TYPE::EvalTree<GEO_TYPE> {                                                    \
    static int eval(const common::ObGeometry *g, const ObGeoEvalCtx &context, RESULT_TYPE &result)

#define OB_GEO_CART_BINARY_FUNC_BEGIN(FUNC_TYPE, GEO_TYPE1, GEO_TYPE2, RESULT_TYPE)         \
template <>                                                                                 \
  struct FUNC_TYPE::EvalWkbBi<GEO_TYPE1, GEO_TYPE2> {                                       \
    static int eval(const common::ObGeometry *g1, const common::ObGeometry *g2,             \
                    const ObGeoEvalCtx &context, RESULT_TYPE &result)

#define OB_GEO_CART_BINARY_FUNC_GEO1_BEGIN(FUNC_TYPE, GEO_TYPE1, RESULT_TYPE)               \
template <typename GeoType2>                                                                \
struct FUNC_TYPE:: EvalWkbBi<GEO_TYPE1, GeoType2> {                                         \
  static int eval(const common::ObGeometry *g1, const common::ObGeometry *g2,               \
                  const ObGeoEvalCtx &context, RESULT_TYPE &result)

#define OB_GEO_CART_BINARY_FUNC_GEO2_BEGIN(FUNC_TYPE, GEO_TYPE2, RESULT_TYPE)               \
template <typename GeoType1>                                                                \
struct FUNC_TYPE:: EvalWkbBi<GeoType1, GEO_TYPE2> {                                         \
  static int eval(const common::ObGeometry *g1, const common::ObGeometry *g2,               \
                  const ObGeoEvalCtx &context, RESULT_TYPE &result)


#define OB_GEO_GEOG_BINARY_FUNC_BEGIN(FUNC_TYPE, GEO_TYPE1, GEO_TYPE2, RESULT_TYPE)         \
template <>                                                                                 \
  struct FUNC_TYPE::EvalWkbBiGeog<GEO_TYPE1, GEO_TYPE2> {                                   \
    static int eval(const common::ObGeometry *g1, const common::ObGeometry *g2,             \
                    const ObGeoEvalCtx &context, RESULT_TYPE &result)

#define OB_GEO_GEOG_BINARY_FUNC_GEO1_BEGIN(FUNC_TYPE, GEO_TYPE1, RESULT_TYPE)               \
template <typename GeoType2>                                                                \
struct FUNC_TYPE:: EvalWkbBiGeog<GEO_TYPE1, GeoType2> {                                     \
  static int eval(const common::ObGeometry *g1, const common::ObGeometry *g2,               \
                  const ObGeoEvalCtx &context, RESULT_TYPE &result)

#define OB_GEO_GEOG_BINARY_FUNC_GEO2_BEGIN(FUNC_TYPE, GEO_TYPE2, RESULT_TYPE)               \
template <typename GeoType1>                                                                \
struct FUNC_TYPE:: EvalWkbBiGeog<GeoType1, GEO_TYPE2> {                                     \
  static int eval(const common::ObGeometry *g1, const common::ObGeometry *g2,               \
                  const ObGeoEvalCtx &context, RESULT_TYPE &result)

#define OB_GEO_CART_TREE_FUNC_BEGIN(FUNC_TYPE, GEO_TYPE1, GEO_TYPE2, RESULT_TYPE)           \
template <>                                                                                 \
  struct FUNC_TYPE::EvalTreeBi<GEO_TYPE1, GEO_TYPE2> {                                      \
    static int eval(const common::ObGeometry *g1, const common::ObGeometry *g2,             \
                    const ObGeoEvalCtx &context, RESULT_TYPE &result)

#define OB_GEO_CART_TREE_FUNC_GEO1_BEGIN(FUNC_TYPE, GEO_TYPE1, RESULT_TYPE)                 \
template <typename GeoType2>                                                                \
struct FUNC_TYPE:: EvalTreeBi<GEO_TYPE1, GeoType2> {                                        \
  static int eval(const common::ObGeometry *g1, const common::ObGeometry *g2,               \
                  const ObGeoEvalCtx &context, RESULT_TYPE &result)

#define OB_GEO_CART_TREE_FUNC_GEO2_BEGIN(FUNC_TYPE, GEO_TYPE2, RESULT_TYPE)                 \
template <typename GeoType1>                                                                \
struct FUNC_TYPE:: EvalTreeBi<GeoType1, GEO_TYPE2> {                                        \
  static int eval(const common::ObGeometry *g1, const common::ObGeometry *g2,               \
                  const ObGeoEvalCtx &context, RESULT_TYPE &result)

#define OB_GEO_GEOG_TREE_FUNC_BEGIN(FUNC_TYPE, GEO_TYPE1, GEO_TYPE2, RESULT_TYPE)           \
template <>                                                                                 \
  struct FUNC_TYPE::EvalTreeBiGeog<GEO_TYPE1, GEO_TYPE2> {                                  \
    static int eval(const common::ObGeometry *g1, const common::ObGeometry *g2,             \
                    const ObGeoEvalCtx &context, RESULT_TYPE &result)

#define OB_GEO_GEOG_TREE_FUNC_GEO1_BEGIN(FUNC_TYPE, GEO_TYPE1, RESULT_TYPE)                 \
template <typename GeoType2>                                                                \
struct FUNC_TYPE:: EvalTreeBiGeog<GEO_TYPE1, GeoType2> {                                    \
  static int eval(const common::ObGeometry *g1, const common::ObGeometry *g2,               \
                  const ObGeoEvalCtx &context, RESULT_TYPE &result)

#define OB_GEO_GEOG_TREE_FUNC_GEO2_BEGIN(FUNC_TYPE, GEO_TYPE2, RESULT_TYPE)                 \
template <typename GeoType1>                                                                \
struct FUNC_TYPE:: EvalTreeBiGeog<GeoType1, GEO_TYPE2> {                                    \
  static int eval(const common::ObGeometry *g1, const common::ObGeometry *g2,               \
                  const ObGeoEvalCtx &context, RESULT_TYPE &result)

#define OB_GEO_FUNC_END  }

} // namespace common
} // namespace oceanbase
#endif // OCEANBASE_LIB_OB_GEO_DISPATCHER_H_