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
 * This file contains implementation for ob_geo_func_equals.
 */

#define USING_LOG_PREFIX LIB

#include "lib/geo/ob_geo_dispatcher.h"
#include "lib/geo/ob_geo_func_equals.h"
#include "lib/geo/ob_geo_func_disjoint.h"
#include "lib/geo/ob_geo_utils.h"
#include "lib/geo/ob_geo_func_utils.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace common
{
namespace bg = boost::geometry;
template<typename GeoType1, typename GeoType2>
int eval_equals_without_strategy(const ObGeometry *g1, const ObGeometry *g2, bool &result)
{
  INIT_SUCC(ret);
  const GeoType1 *geo1 = nullptr;
  const GeoType2 *geo2 = nullptr;
  if (g1->is_tree()) {
    geo1 = reinterpret_cast<const GeoType1 *>(g1);
    geo2 = reinterpret_cast<const GeoType2 *>(g2);
  } else {
    geo1 = reinterpret_cast<const GeoType1 *>(g1->val());
    geo2 = reinterpret_cast<const GeoType2 *>(g2->val());
  }
  if (OB_ISNULL(geo1) || OB_ISNULL(geo2)) {
    ret = OB_ERR_INVALID_NULL_SDO_GEOMETRY;
    LOG_WARN("input geomery is null", K(ret), K(geo1), K(geo2), K(g1->is_tree()));
  } else {
    result = bg::equals(*geo1, *geo2);
  }
  return OB_SUCCESS;
}

template<typename GeoType1, typename GeoType2>
int eval_equals_with_nonpoint_strategy(
    const ObGeometry *g1, const ObGeometry *g2, const ObGeoEvalCtx &context, bool &result)
{
  INIT_SUCC(ret);
  const ObSrsItem *srs = context.get_srs();
  if (OB_ISNULL(srs)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("srs is null", K(ret), K(g1->get_srid()), K(g1), K(g2));
  } else {
    const GeoType1 *geo1 = nullptr;
    const GeoType2 *geo2 = nullptr;
    if (g1->is_tree()) {
      geo1 = reinterpret_cast<const GeoType1 *>(g1);
      geo2 = reinterpret_cast<const GeoType2 *>(g2);
    } else {
      geo1 = reinterpret_cast<const GeoType1 *>(g1->val());
      geo2 = reinterpret_cast<const GeoType2 *>(g2->val());
    }
    if (OB_ISNULL(geo1) || OB_ISNULL(geo2)) {
      ret = OB_ERR_INVALID_NULL_SDO_GEOMETRY;
      LOG_WARN("input geomery is null", K(ret), K(geo1), K(geo2), K(g1->is_tree()));
    } else {
      bg::srs::spheroid<double> geog_sphere(srs->semi_major_axis(), srs->semi_minor_axis());
      bg::strategy::intersection::geographic_segments<> nonpoint_strategy(geog_sphere);
      result = bg::equals(*geo1, *geo2, nonpoint_strategy);
    }
  }
  return ret;
}

// ----- ObGeoFuncEqualsImpl -----
class ObGeoFuncEqualsImpl : public ObIGeoDispatcher<bool, ObGeoFuncEqualsImpl>
{
public:
  ObGeoFuncEqualsImpl();
  virtual ~ObGeoFuncEqualsImpl() = default;

  // template for unary
  OB_GEO_UNARY_FUNC_DEFAULT(bool, OB_ERR_GIS_INVALID_DATA);
  OB_GEO_TREE_UNARY_FUNC_DEFAULT(bool, OB_ERR_GIS_INVALID_DATA);

  // template for binary
  // default cases for cartesian
  template<typename GeoType1, typename GeoType2>
  struct EvalWkbBi
  {
    static int eval(
        const ObGeometry *g1, const ObGeometry *g2, const ObGeoEvalCtx &context, bool &result)
    {
      UNUSEDx(g1, g2, context);
      result = false;
      return OB_SUCCESS;
    }
  };

  // default case for geography
  template<typename GeoType1, typename GeoType2>
  struct EvalWkbBiGeog
  {
    static int eval(
        const ObGeometry *g1, const ObGeometry *g2, const ObGeoEvalCtx &context, bool &result)
    {
      UNUSEDx(g1, g2, context);
      result = false;
      return OB_SUCCESS;
    }
  };

  // template for tree
  template<typename GeoType1, typename GeoType2>
  struct EvalTreeBi
  {
    static int eval(
        const ObGeometry *g1, const ObGeometry *g2, const ObGeoEvalCtx &context, bool &result)
    {
      UNUSEDx(g1, g2, context);
      result = false;
      return OB_SUCCESS;
    }
  };

  // default case for geography
  template<typename GeoType1, typename GeoType2>
  struct EvalTreeBiGeog
  {
    static int eval(
        const ObGeometry *g1, const ObGeometry *g2, const ObGeoEvalCtx &context, bool &result)
    {
      UNUSEDx(g1, g2, context);
      result = false;
      return OB_SUCCESS;
    }
  };
private:
  // geometry collection
  template<typename GcTreeType>
  static int eval_equals_geometry_collection(
      const ObGeometry *g1, const ObGeometry *g2, const ObGeoEvalCtx &context, bool &result)
  {
    int ret = OB_SUCCESS;
    result = false;
    if (g1->type() == ObGeoType::GEOMETRYCOLLECTION) {
      bool is_g1_empty = false;
      bool is_g2_empty = false;
      if (OB_FAIL(ObGeoTypeUtil::check_empty(const_cast<ObGeometry *>(g1), is_g1_empty))) {
        LOG_WARN("fail to check is geometry empty", K(ret));
      } else if (OB_FAIL(ObGeoTypeUtil::check_empty(const_cast<ObGeometry *>(g2), is_g2_empty))) {
        LOG_WARN("fail to check is geometry empty", K(ret));
      } else if (is_g1_empty || is_g2_empty) {
        result = is_g1_empty && is_g2_empty;
      } else {
        typename GcTreeType::sub_mpt_type *mpt1 = NULL;
        typename GcTreeType::sub_ml_type *mls1 = NULL;
        typename GcTreeType::sub_mp_type *mpy1 = NULL;
        ObGeometry *geo1 = const_cast<ObGeometry *>(reinterpret_cast<const ObGeometry *>(g1));
        if (OB_FAIL(ObGeoFuncUtils::ob_gc_prepare<GcTreeType>(context, geo1, mpt1, mls1, mpy1))) {
          LOG_WARN("failed to prepare gc", K(ret));
        } else if (OB_ISNULL(mpt1) || OB_ISNULL(mls1) || OB_ISNULL(mpy1)) {
          ret = OB_ERR_GIS_INVALID_DATA;
          LOG_WARN("unexpected null geometry collection split", K(ret));
        } else if (g2->type() == ObGeoType::GEOMETRYCOLLECTION) {
          // both collection
          typename GcTreeType::sub_mpt_type *mpt2 = NULL;
          typename GcTreeType::sub_ml_type *mls2 = NULL;
          typename GcTreeType::sub_mp_type *mpy2 = NULL;
          ObGeometry *geo2 = const_cast<ObGeometry *>(reinterpret_cast<const ObGeometry *>(g2));
          if (OB_FAIL(ObGeoFuncUtils::ob_gc_prepare<GcTreeType>(context, geo2, mpt2, mls2, mpy2))) {
            LOG_WARN("failed to prepare gc", K(ret));
          } else if (OB_ISNULL(mpt2) || OB_ISNULL(mls2) || OB_ISNULL(mpt2)) {
            ret = OB_ERR_GIS_INVALID_DATA;
            LOG_WARN("unexpected null geometry collection split", K(ret));
          } else if ((mpt1->is_empty() != mpt2->is_empty()) || (mls1->is_empty() != mls2->is_empty())
                    || (mpy1->is_empty() != mpy2->is_empty())) {
            result = false;
          } else {
            bool mpt_result = mpt1->is_empty() && mpt2->is_empty();
            if (!mpt_result && OB_FAIL(eval_tree_binary(mpt1, mpt2, context, mpt_result))) {
              LOG_WARN("fail to do eval", K(ret), K(result));
            } else {
              result = mpt_result;
            }
            if (OB_SUCC(ret) && result) {
              bool mls_result = mls1->is_empty() && mls2->is_empty();
              if (!mls_result && OB_FAIL(eval_tree_binary(mls1, mls2, context, mls_result))) {
                LOG_WARN("fail to do eval", K(ret), K(result));
              } else {
                result = result && mls_result;
              }
            }
            if (OB_SUCC(ret) && result) {
              bool mpy_result = mpy1->is_empty() && mpy2->is_empty();
              if (!mpy_result && OB_FAIL(eval_tree_binary(mpy1, mpy2, context, mpy_result))) {
                LOG_WARN("fail to do eval", K(ret), K(result));
              } else {
                result = result && mpy_result;
              }
            }
          }
        } else {
          switch (g2->type()) {
            case ObGeoType::POINT:
            case ObGeoType::MULTIPOINT: {
              if (!mls1->is_empty() || !mpy1->is_empty()) {
                result = false;
              } else {
                ObGeometry *mpt_bin = NULL;
                if (OB_FAIL(ObGeoTypeUtil::tree_to_bin(
                        *context.get_allocator(), mpt1, mpt_bin, context.get_srs()))) {
                  LOG_WARN("failed to convert geo tree to binary", K(ret));
                } else {
                  ret = eval_wkb_binary(mpt_bin, g2, context, result);
                }
              }
              break;
            }
            case ObGeoType::LINESTRING:
            case ObGeoType::MULTILINESTRING: {
              if (!mpt1->is_empty() || !mpy1->is_empty()) {
                result = false;
              } else {
                ObGeometry *mls_bin = NULL;
                if (OB_FAIL(ObGeoTypeUtil::tree_to_bin(
                        *context.get_allocator(), mls1, mls_bin, context.get_srs()))) {
                  LOG_WARN("failed to convert geo tree to binary", K(ret));
                } else {
                  ret = eval_wkb_binary(mls_bin, g2, context, result);
                }
              }
              break;
            }
            case ObGeoType::POLYGON:
            case ObGeoType::MULTIPOLYGON: {
              if (!mls1->is_empty() || !mpt1->is_empty()) {
                result = false;
              } else {
                ObGeometry *mpy_bin = NULL;
                if (OB_FAIL(ObGeoTypeUtil::tree_to_bin(
                        *context.get_allocator(), mpy1, mpy_bin, context.get_srs()))) {
                  LOG_WARN("failed to convert geo tree to binary", K(ret));
                } else {
                  ret = eval_wkb_binary(mpy_bin, g2, context, result);
                }
              }
              break;
            }
            default: {
              ret = OB_ERR_GIS_INVALID_DATA;
              LOG_WARN("invalid geometry type", K(ret), K(g2->type()));
            }
          }
        }
      }
    } else if (g2->type() == ObGeoType::GEOMETRYCOLLECTION) {
      ret = eval_equals_geometry_collection<GcTreeType>(g2, g1, context, result);
    } else {
      // none of the two geometries are collection type
      // not supposed to go to this branch
      ret = eval_wkb_binary(g1, g2, context, result);
    }
    return ret;
  }
};

//===========BIN GEOM==========
// Geom Point
OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncEqualsImpl, ObWkbGeomPoint, ObWkbGeomPoint, bool)
{
  return eval_equals_without_strategy<ObWkbGeomPoint, ObWkbGeomPoint>(g1, g2, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncEqualsImpl, ObWkbGeomPoint, ObWkbGeomMultiPoint, bool)
{
  return eval_equals_without_strategy<ObWkbGeomPoint, ObWkbGeomMultiPoint>(g1, g2, result);
}
OB_GEO_FUNC_END;

// Geom LineString
OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncEqualsImpl, ObWkbGeomLineString, ObWkbGeomLineString, bool)
{
  return eval_equals_without_strategy<ObWkbGeomLineString, ObWkbGeomLineString>(g1, g2, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncEqualsImpl, ObWkbGeomLineString, ObWkbGeomMultiLineString, bool)
{
  return eval_equals_without_strategy<ObWkbGeomLineString, ObWkbGeomMultiLineString>(
      g1, g2, result);
}
OB_GEO_FUNC_END;

// Geom Polygon
OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncEqualsImpl, ObWkbGeomPolygon, ObWkbGeomPolygon, bool)
{
  return eval_equals_without_strategy<ObWkbGeomPolygon, ObWkbGeomPolygon>(g1, g2, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncEqualsImpl, ObWkbGeomPolygon, ObWkbGeomMultiPolygon, bool)
{
  return eval_equals_without_strategy<ObWkbGeomPolygon, ObWkbGeomMultiPolygon>(g1, g2, result);
}
OB_GEO_FUNC_END;

// Geom MultiPoint
OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncEqualsImpl, ObWkbGeomMultiPoint, ObWkbGeomPoint, bool)
{
  return eval_equals_without_strategy<ObWkbGeomMultiPoint, ObWkbGeomPoint>(g1, g2, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncEqualsImpl, ObWkbGeomMultiPoint, ObWkbGeomMultiPoint, bool)
{
  return eval_equals_without_strategy<ObWkbGeomMultiPoint, ObWkbGeomMultiPoint>(g1, g2, result);
}
OB_GEO_FUNC_END;

// Geom MultiLineString
OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncEqualsImpl, ObWkbGeomMultiLineString, ObWkbGeomLineString, bool)
{
  return eval_equals_without_strategy<ObWkbGeomMultiLineString, ObWkbGeomLineString>(
      g1, g2, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncEqualsImpl, ObWkbGeomMultiLineString, ObWkbGeomMultiLineString, bool)
{
  return eval_equals_without_strategy<ObWkbGeomMultiLineString, ObWkbGeomMultiLineString>(
      g1, g2, result);
}
OB_GEO_FUNC_END;

// Geom MultiPolygon
OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncEqualsImpl, ObWkbGeomMultiPolygon, ObWkbGeomPolygon, bool)
{
  return eval_equals_without_strategy<ObWkbGeomMultiPolygon, ObWkbGeomPolygon>(g1, g2, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncEqualsImpl, ObWkbGeomMultiPolygon, ObWkbGeomMultiPolygon, bool)
{
  return eval_equals_without_strategy<ObWkbGeomMultiPolygon, ObWkbGeomMultiPolygon>(g1, g2, result);
}
OB_GEO_FUNC_END;

// Geom Collection
OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncEqualsImpl, ObWkbGeomCollection, ObWkbGeomCollection, bool)
{
  return eval_equals_geometry_collection<ObCartesianGeometrycollection>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_GEO2_BEGIN(ObGeoFuncEqualsImpl, ObWkbGeomCollection, bool)
{
  return eval_equals_geometry_collection<ObCartesianGeometrycollection>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_GEO1_BEGIN(ObGeoFuncEqualsImpl, ObWkbGeomCollection, bool)
{
  return eval_equals_geometry_collection<ObCartesianGeometrycollection>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

//===========BIN GEOG==========
// Geog Point
OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncEqualsImpl, ObWkbGeogPoint, ObWkbGeogPoint, bool)
{
  // Default strategy is OK. P/P computations do not depend on shape of ellipsoid.
  return eval_equals_without_strategy<ObWkbGeogPoint, ObWkbGeogPoint>(g1, g2, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncEqualsImpl, ObWkbGeogPoint, ObWkbGeogMultiPoint, bool)
{
  // Default strategy is OK. P/P computations do not depend on shape of ellipsoid.
  return eval_equals_without_strategy<ObWkbGeogPoint, ObWkbGeogMultiPoint>(g1, g2, result);
}
OB_GEO_FUNC_END;

// Geog LineString
OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncEqualsImpl, ObWkbGeogLineString, ObWkbGeogLineString, bool)
{
  return eval_equals_with_nonpoint_strategy<ObWkbGeogLineString, ObWkbGeogLineString>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncEqualsImpl, ObWkbGeogLineString, ObWkbGeogMultiLineString, bool)
{
  return eval_equals_with_nonpoint_strategy<ObWkbGeogLineString, ObWkbGeogMultiLineString>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

// Geog Polygon
OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncEqualsImpl, ObWkbGeogPolygon, ObWkbGeogPolygon, bool)
{
  return eval_equals_with_nonpoint_strategy<ObWkbGeogPolygon, ObWkbGeogPolygon>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncEqualsImpl, ObWkbGeogPolygon, ObWkbGeogMultiPolygon, bool)
{
  return eval_equals_with_nonpoint_strategy<ObWkbGeogPolygon, ObWkbGeogMultiPolygon>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

// Geog MultiPoint
OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncEqualsImpl, ObWkbGeogMultiPoint, ObWkbGeogPoint, bool)
{
  // Default strategy is OK. P/P computations do not depend on shape of ellipsoid.
  return eval_equals_without_strategy<ObWkbGeogMultiPoint, ObWkbGeogPoint>(g1, g2, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncEqualsImpl, ObWkbGeogMultiPoint, ObWkbGeogMultiPoint, bool)
{
  // Default strategy is OK. P/P computations do not depend on shape of ellipsoid.
  return eval_equals_without_strategy<ObWkbGeogMultiPoint, ObWkbGeogMultiPoint>(g1, g2, result);
}
OB_GEO_FUNC_END;

// Geog MultiLineString
OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncEqualsImpl, ObWkbGeogMultiLineString, ObWkbGeogLineString, bool)
{
  return eval_equals_with_nonpoint_strategy<ObWkbGeogMultiLineString, ObWkbGeogLineString>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncEqualsImpl, ObWkbGeogMultiLineString, ObWkbGeogMultiLineString, bool)
{
  return eval_equals_with_nonpoint_strategy<ObWkbGeogMultiLineString, ObWkbGeogMultiLineString>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

// Geog MultiPolygon
OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncEqualsImpl, ObWkbGeogMultiPolygon, ObWkbGeogPolygon, bool)
{
  return eval_equals_with_nonpoint_strategy<ObWkbGeogMultiPolygon, ObWkbGeogPolygon>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncEqualsImpl, ObWkbGeogMultiPolygon, ObWkbGeogMultiPolygon, bool)
{
  return eval_equals_with_nonpoint_strategy<ObWkbGeogMultiPolygon, ObWkbGeogMultiPolygon>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

// Geog Collection
OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncEqualsImpl, ObWkbGeogCollection, ObWkbGeogCollection, bool)
{
  return eval_equals_geometry_collection<ObGeographGeometrycollection>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_GEO2_BEGIN(ObGeoFuncEqualsImpl, ObWkbGeogCollection, bool)
{
  return eval_equals_geometry_collection<ObGeographGeometrycollection>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_GEO1_BEGIN(ObGeoFuncEqualsImpl, ObWkbGeogCollection, bool)
{
  return eval_equals_geometry_collection<ObGeographGeometrycollection>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

// cartesian tree
// Geom Point
OB_GEO_CART_TREE_FUNC_BEGIN(ObGeoFuncEqualsImpl, ObCartesianPoint, ObCartesianPoint, bool)
{
  return eval_equals_without_strategy<ObCartesianPoint, ObCartesianPoint>(g1, g2, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_TREE_FUNC_BEGIN(ObGeoFuncEqualsImpl, ObCartesianPoint, ObCartesianMultipoint, bool)
{
  return eval_equals_without_strategy<ObCartesianPoint, ObCartesianMultipoint>(g1, g2, result);
}
OB_GEO_FUNC_END;

// Geom LineString
OB_GEO_CART_TREE_FUNC_BEGIN(ObGeoFuncEqualsImpl, ObCartesianLineString, ObCartesianLineString, bool)
{
  return eval_equals_without_strategy<ObCartesianLineString, ObCartesianLineString>(g1, g2, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_TREE_FUNC_BEGIN(
    ObGeoFuncEqualsImpl, ObCartesianLineString, ObCartesianMultilinestring, bool)
{
  return eval_equals_without_strategy<ObCartesianLineString, ObCartesianMultilinestring>(
      g1, g2, result);
}
OB_GEO_FUNC_END;

// Geom Polygon
OB_GEO_CART_TREE_FUNC_BEGIN(ObGeoFuncEqualsImpl, ObCartesianPolygon, ObCartesianPolygon, bool)
{
  return eval_equals_without_strategy<ObCartesianPolygon, ObCartesianPolygon>(g1, g2, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_TREE_FUNC_BEGIN(ObGeoFuncEqualsImpl, ObCartesianPolygon, ObCartesianMultipolygon, bool)
{
  return eval_equals_without_strategy<ObCartesianPolygon, ObCartesianMultipolygon>(g1, g2, result);
}
OB_GEO_FUNC_END;

// Geom MultiPoint
OB_GEO_CART_TREE_FUNC_BEGIN(ObGeoFuncEqualsImpl, ObCartesianMultipoint, ObCartesianPoint, bool)
{
  return eval_equals_without_strategy<ObCartesianMultipoint, ObCartesianPoint>(g1, g2, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_TREE_FUNC_BEGIN(ObGeoFuncEqualsImpl, ObCartesianMultipoint, ObCartesianMultipoint, bool)
{
  return eval_equals_without_strategy<ObCartesianMultipoint, ObCartesianMultipoint>(g1, g2, result);
}
OB_GEO_FUNC_END;

// Geom MultiLineString
OB_GEO_CART_TREE_FUNC_BEGIN(
    ObGeoFuncEqualsImpl, ObCartesianMultilinestring, ObCartesianLineString, bool)
{
  return eval_equals_without_strategy<ObCartesianMultilinestring, ObCartesianLineString>(
      g1, g2, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_TREE_FUNC_BEGIN(
    ObGeoFuncEqualsImpl, ObCartesianMultilinestring, ObCartesianMultilinestring, bool)
{
  return eval_equals_without_strategy<ObCartesianMultilinestring, ObCartesianMultilinestring>(
      g1, g2, result);
}
OB_GEO_FUNC_END;

// Geom MultiPolygon
OB_GEO_CART_TREE_FUNC_BEGIN(ObGeoFuncEqualsImpl, ObCartesianMultipolygon, ObCartesianPolygon, bool)
{
  return eval_equals_without_strategy<ObCartesianMultipolygon, ObCartesianPolygon>(g1, g2, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_TREE_FUNC_BEGIN(
    ObGeoFuncEqualsImpl, ObCartesianMultipolygon, ObCartesianMultipolygon, bool)
{
  return eval_equals_without_strategy<ObCartesianMultipolygon, ObCartesianMultipolygon>(g1, g2, result);
}
OB_GEO_FUNC_END;

// Geom Collection
OB_GEO_CART_TREE_FUNC_BEGIN(ObGeoFuncEqualsImpl, ObCartesianGeometrycollection, ObCartesianGeometrycollection, bool)
{
  return eval_equals_geometry_collection<ObCartesianGeometrycollection>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_TREE_FUNC_GEO2_BEGIN(ObGeoFuncEqualsImpl, ObCartesianGeometrycollection, bool)
{
  return eval_equals_geometry_collection<ObCartesianGeometrycollection>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_TREE_FUNC_GEO1_BEGIN(ObGeoFuncEqualsImpl, ObCartesianGeometrycollection, bool)
{
  return eval_equals_geometry_collection<ObCartesianGeometrycollection>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

// Geog tree
// Geog Point
OB_GEO_GEOG_TREE_FUNC_BEGIN(ObGeoFuncEqualsImpl, ObGeographPoint, ObGeographPoint, bool)
{
  // Default strategy is OK. P/P computations do not depend on shape of ellipsoid.
  return eval_equals_without_strategy<ObGeographPoint, ObGeographPoint>(g1, g2, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_TREE_FUNC_BEGIN(ObGeoFuncEqualsImpl, ObGeographPoint, ObGeographMultipoint, bool)
{
  // Default strategy is OK. P/P computations do not depend on shape of ellipsoid.
  return eval_equals_without_strategy<ObGeographPoint, ObGeographMultipoint>(g1, g2, result);
}
OB_GEO_FUNC_END;

// Geog LineString
OB_GEO_GEOG_TREE_FUNC_BEGIN(ObGeoFuncEqualsImpl, ObGeographLineString, ObGeographLineString, bool)
{
  return eval_equals_with_nonpoint_strategy<ObGeographLineString, ObGeographLineString>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_TREE_FUNC_BEGIN(
    ObGeoFuncEqualsImpl, ObGeographLineString, ObGeographMultilinestring, bool)
{
  return eval_equals_with_nonpoint_strategy<ObGeographLineString, ObGeographMultilinestring>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

// Geog Polygon
OB_GEO_GEOG_TREE_FUNC_BEGIN(ObGeoFuncEqualsImpl, ObGeographPolygon, ObGeographPolygon, bool)
{
  return eval_equals_with_nonpoint_strategy<ObGeographPolygon, ObGeographPolygon>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_TREE_FUNC_BEGIN(ObGeoFuncEqualsImpl, ObGeographPolygon, ObGeographMultipolygon, bool)
{
  return eval_equals_with_nonpoint_strategy<ObGeographPolygon, ObGeographMultipolygon>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

// Geog MultiPoint
OB_GEO_GEOG_TREE_FUNC_BEGIN(ObGeoFuncEqualsImpl, ObGeographMultipoint, ObGeographPoint, bool)
{
  // Default strategy is OK. P/P computations do not depend on shape of ellipsoid.
  return eval_equals_without_strategy<ObGeographMultipoint, ObGeographPoint>(g1, g2, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_TREE_FUNC_BEGIN(ObGeoFuncEqualsImpl, ObGeographMultipoint, ObGeographMultipoint, bool)
{
  // Default strategy is OK. P/P computations do not depend on shape of ellipsoid.
  return eval_equals_without_strategy<ObGeographMultipoint, ObGeographMultipoint>(g1, g2, result);
}
OB_GEO_FUNC_END;

// Geog MultiLineString
OB_GEO_GEOG_TREE_FUNC_BEGIN(
    ObGeoFuncEqualsImpl, ObGeographMultilinestring, ObGeographLineString, bool)
{
  return eval_equals_with_nonpoint_strategy<ObGeographMultilinestring, ObGeographLineString>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_TREE_FUNC_BEGIN(
    ObGeoFuncEqualsImpl, ObGeographMultilinestring, ObGeographMultilinestring, bool)
{
  return eval_equals_with_nonpoint_strategy<ObGeographMultilinestring, ObGeographMultilinestring>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

// Geog MultiPolygon
OB_GEO_GEOG_TREE_FUNC_BEGIN(ObGeoFuncEqualsImpl, ObGeographMultipolygon, ObGeographPolygon, bool)
{
  return eval_equals_with_nonpoint_strategy<ObGeographMultipolygon, ObGeographPolygon>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_TREE_FUNC_BEGIN(
    ObGeoFuncEqualsImpl, ObGeographMultipolygon, ObGeographMultipolygon, bool)
{
  return eval_equals_with_nonpoint_strategy<ObGeographMultipolygon, ObGeographMultipolygon>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

// Geog Collection
OB_GEO_GEOG_TREE_FUNC_BEGIN(ObGeoFuncEqualsImpl, ObGeographGeometrycollection, ObGeographGeometrycollection, bool)
{
  return eval_equals_geometry_collection<ObGeographGeometrycollection>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_TREE_FUNC_GEO2_BEGIN(ObGeoFuncEqualsImpl, ObGeographGeometrycollection, bool)
{
  return eval_equals_geometry_collection<ObGeographGeometrycollection>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_TREE_FUNC_GEO1_BEGIN(ObGeoFuncEqualsImpl, ObGeographGeometrycollection, bool)
{
  return eval_equals_geometry_collection<ObGeographGeometrycollection>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

int ObGeoFuncEquals::eval(const ObGeoEvalCtx &gis_context, bool &result)
{
  return ObGeoFuncEqualsImpl::eval_geo_func(gis_context, result);
}

}  // namespace common
}  // namespace oceanbase