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
 * This file contains implementation for ob_geo_func_overlaps.
 */

#define USING_LOG_PREFIX LIB

#include "lib/geo/ob_geo_dispatcher.h"
#include "lib/geo/ob_geo_func_overlaps.h"
#include "lib/geo/ob_geo_utils.h"
#include "lib/geo/ob_geo_func_utils.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace common
{
namespace bg = boost::geometry;
template<typename GeoType1, typename GeoType2>
int eval_overlaps_without_strategy(const ObGeometry *g1, const ObGeometry *g2, bool &result)
{
  int ret = OB_SUCCESS;
  const GeoType1 *geo1 = NULL;
  const GeoType2 *geo2 = NULL;
  if (g1->is_tree()) {
    geo1 = reinterpret_cast<const GeoType1 *>(g1);
    geo2 = reinterpret_cast<const GeoType2 *>(g2);
  } else {
    geo1 = reinterpret_cast<const GeoType1 *>(g1->val());
    geo2 = reinterpret_cast<const GeoType2 *>(g2->val());
  }
  if (OB_ISNULL(geo1) || OB_ISNULL(geo2)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("multipoint or linestring pointer is null", K(ret), K(geo1), K(geo2));
  } else {
    result = bg::overlaps(*geo1, *geo2);
  }
  return ret;
}

template<typename GeoType1, typename GeoType2>
int eval_overlaps_with_nonpoint_strategy(
    const ObGeometry *g1, const ObGeometry *g2, const ObGeoEvalCtx &context, bool &result)
{
  INIT_SUCC(ret);
  const ObSrsItem *srs = context.get_srs();
  if (OB_ISNULL(srs)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("srs is null", K(ret), K(g1->get_srid()), K(g1), K(g2));
  } else {
    bg::srs::spheroid<double> geog_sphere(srs->semi_major_axis(), srs->semi_minor_axis());
    bg::strategy::intersection::geographic_segments<> nonpoint_strategy(geog_sphere);
    const GeoType1 *geo1 = NULL;
    const GeoType2 *geo2 = NULL;
    if (g1->is_tree()) {
      geo1 = reinterpret_cast<const GeoType1 *>(g1);
      geo2 = reinterpret_cast<const GeoType2 *>(g2);
    } else {
      geo1 = reinterpret_cast<const GeoType1 *>(g1->val());
      geo2 = reinterpret_cast<const GeoType2 *>(g2->val());
    }
    if (OB_ISNULL(geo1) || OB_ISNULL(geo2)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("multipoint or linestring pointer is null", K(ret), K(geo1), K(geo2));
    } else {
      result = bg::overlaps(*geo1, *geo2, nonpoint_strategy);
    }
  }
  return ret;
}

// ----- ObGeoFuncOverlapsImpl -----
class ObGeoFuncOverlapsImpl : public ObIGeoDispatcher<ObGeoFuncResWithNull, ObGeoFuncOverlapsImpl>
{
public:
  ObGeoFuncOverlapsImpl();
  virtual ~ObGeoFuncOverlapsImpl() = default;

  // template for unary
  OB_GEO_UNARY_FUNC_DEFAULT(ObGeoFuncResWithNull, OB_ERR_GIS_INVALID_DATA);
  OB_GEO_TREE_UNARY_FUNC_DEFAULT(ObGeoFuncResWithNull, OB_ERR_GIS_INVALID_DATA);
  OB_GEO_CART_TREE_FUNC_DEFAULT(ObGeoFuncResWithNull, OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS);
  OB_GEO_GEOG_TREE_FUNC_DEFAULT(ObGeoFuncResWithNull, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);

  // template for binary
  // default cases for cartesian
  template<typename GeoType1, typename GeoType2>
  struct EvalWkbBi
  {
    static int eval(const ObGeometry *g1, const ObGeometry *g2, const ObGeoEvalCtx &context,
        ObGeoFuncResWithNull &result)
    {
      // If dim(g1) != dim(g2), return NULL (SQL/MM 2015, Part 3, Sect. 5.1.54).
      UNUSEDx(g1, g2, context);
      result.is_null = true;
      return OB_SUCCESS;
    }
  };

  // default case for geography
  template<typename GeoType1, typename GeoType2>
  struct EvalWkbBiGeog
  {
    static int eval(const ObGeometry *g1, const ObGeometry *g2, const ObGeoEvalCtx &context,
        ObGeoFuncResWithNull &result)
    {
      // If dim(g1) != dim(g2), return NULL (SQL/MM 2015, Part 3, Sect. 5.1.54).
      UNUSEDx(g1, g2, context);
      result.is_null = true;
      return OB_SUCCESS;
    }
  };

private:
  // assume that g1 g2 both collection
  template<typename GcTreeType>
  static int eval_overlaps_gc_gc(const ObGeometry *g1, const ObGeometry *g2,
      const ObGeoEvalCtx &context, ObGeoFuncResWithNull &result)
  {
    int ret = OB_SUCCESS;
    if (g1->type() != ObGeoType::GEOMETRYCOLLECTION
        || g2->type() != ObGeoType::GEOMETRYCOLLECTION) {
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_WARN("input geometry should be GEOMETRYCOLLECTION", K(ret), K(g1->type()), K(g2->type()));
    } else {
      result.bret = false;
      typename GcTreeType::sub_mpt_type *mpt1 = NULL;
      typename GcTreeType::sub_ml_type *mls1 = NULL;
      typename GcTreeType::sub_mp_type *mpy1 = NULL;
      ObGeometry *geo1 = const_cast<ObGeometry *>(reinterpret_cast<const ObGeometry *>(g1));
      uint8_t dim1 = -1;
      uint8_t dim2 = -1;
      if (OB_FAIL(ObGeoFuncUtils::ob_gc_prepare<GcTreeType>(context, geo1, mpt1, mls1, mpy1))) {
        LOG_WARN("failed to prepare gc", K(ret));
      } else if (OB_ISNULL(mpt1) || OB_ISNULL(mls1) || OB_ISNULL(mpy1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null geometry collection split", K(ret));
      } else {
        if (!mpy1->empty()) {
          dim1 = 2;
        } else if (!mls1->empty()) {
          dim1 = 1;
        } else if (!mpt1->empty()) {
          dim1 = 0;
        } else {
          result.is_null = true;
        }
      }

      typename GcTreeType::sub_mpt_type *mpt2 = NULL;
      typename GcTreeType::sub_ml_type *mls2 = NULL;
      typename GcTreeType::sub_mp_type *mpy2 = NULL;
      ObGeometry *geo2 = const_cast<ObGeometry *>(reinterpret_cast<const ObGeometry *>(g2));
      if (OB_SUCC(ret) && !result.is_null) {
        // bool has_common_interior = false;  // Check that if g1 and g2 has common interior
        if (OB_FAIL(ObGeoFuncUtils::ob_gc_prepare<GcTreeType>(context, geo2, mpt2, mls2, mpy2))) {
          LOG_WARN("failed to prepare gc", K(ret));
        } else if (OB_ISNULL(mpt2) || OB_ISNULL(mls2) || OB_ISNULL(mpy2)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null geometry collection split", K(ret));
        } else {
          if (!mpy2->empty()) {
            dim2 = 2;
          } else if (!mls2->empty()) {
            dim2 = 1;
          } else if (!mpt2->empty()) {
            dim2 = 0;
          } else {
            result.is_null = true;
          }
        }
      }

      if (OB_FAIL(ret) || result.is_null) {
        // do nothing
      } else if (dim1 == -1 || dim1 != dim2) {
        result.is_null = true;
      } else {
        ObGeoFuncResWithNull mpt_res;
        ObGeoFuncResWithNull mls_res;
        ObGeoFuncResWithNull mpy_res;
        switch (dim1) {
          case 2:
            if (OB_FAIL(eval_tree_binary(mpy1, mpy2, context, mpy_res))) {
              LOG_WARN("fail to eval tree binary", K(ret));
            }
          case 1:
            if (OB_FAIL(eval_tree_binary(mls1, mls2, context, mls_res))) {
              LOG_WARN("fail to eval tree binary", K(ret));
            }
          case 0:
            if (OB_FAIL(eval_tree_binary(mpt1, mpt2, context, mpt_res))) {
              LOG_WARN("fail to eval tree binary", K(ret));
            }
            break;
          default: {
            // should not go here
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected dim provided to overlaps", K(ret), K(dim1));
          }
        }
        result.bret |= mpy_res.bret || mpt_res.bret || mls_res.bret;
      }
    }
    return ret;
  }

  template<typename GcTreeType>
  static int eval_overlaps_gc_other(const ObGeometry *g1, const ObGeometry *g2,
      const ObGeoEvalCtx &context, ObGeoFuncResWithNull &result)
  {
    int ret = OB_SUCCESS;
    if (g1->type() != ObGeoType::GEOMETRYCOLLECTION
        && g2->type() != ObGeoType::GEOMETRYCOLLECTION) {
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_WARN("At least one of g1 and g2 is collection", K(ret), K(g1->type()), K(g2->type()));
    } else if (g2->type() == ObGeoType::GEOMETRYCOLLECTION
               && g1->type() == ObGeoType::GEOMETRYCOLLECTION) {
      if (OB_FAIL(eval_overlaps_gc_gc<GcTreeType>(g1, g2, context, result))) {
        LOG_WARN("fail to eval overlaps with geometrycollection", K(ret));
      }
    } else if (g2->type() == ObGeoType::GEOMETRYCOLLECTION) {
      ret = eval_overlaps_gc_other<GcTreeType>(g2, g1, context, result);
    } else {
      // now assert g1 is colletion and g2 is not collection.
      result.bret = false;
      typename GcTreeType::sub_mpt_type *mpt1 = NULL;
      typename GcTreeType::sub_ml_type *mls1 = NULL;
      typename GcTreeType::sub_mp_type *mpy1 = NULL;
      ObGeometry *geo1 = const_cast<ObGeometry *>(reinterpret_cast<const ObGeometry *>(g1));
      uint8_t dim1 = -1;
      uint8_t dim2 = -1;
      if (OB_FAIL(ObGeoFuncUtils::ob_gc_prepare<GcTreeType>(context, geo1, mpt1, mls1, mpy1))) {
        LOG_WARN("failed to prepare gc", K(ret));
      } else if (OB_ISNULL(mpt1) || OB_ISNULL(mls1) || OB_ISNULL(mpy1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null geometry collection split", K(ret));
      } else {
        if (!mpy1->empty()) {
          dim1 = 2;
        } else if (!mls1->empty()) {
          dim1 = 1;
        } else if (!mpt1->empty()) {
          dim1 = 0;
        } else {
          result.is_null = true;
        }
      }

      ObGeoToTreeVisitor to_tree(context.get_allocator());
      ObGeometry *geo2 = const_cast<ObGeometry *>(g2);
      if (OB_FAIL(ret) || result.is_null) {
        // do nothing
      } else if (OB_FAIL(geo2->do_visit(to_tree))) {
        LOG_WARN("fail to do visit with ObGeoToTreeVisitor", K(ret));
      } else {
        ObGeometry *g2_tree = to_tree.get_geometry();
        switch (g2_tree->type()) {
          case ObGeoType::POINT:
          case ObGeoType::MULTIPOINT: {
            if (dim1 != 0) {
              result.is_null = true;
            } else if (OB_FAIL(eval_tree_binary(mpt1, g2_tree, context, result))) {
              LOG_WARN("fail to do eval_tree_binary", K(ret), K(g2_tree->type()));
            }
            break;
          }
          case ObGeoType::LINESTRING:
          case ObGeoType::MULTILINESTRING: {
            if (dim1 != 1) {
              result.is_null = true;
            } else if (OB_FAIL(eval_tree_binary(mls1, g2_tree, context, result))) {
              LOG_WARN("fail to do eval_tree_binary", K(ret), K(g2_tree->type()));
            }
            break;
          }
          case ObGeoType::POLYGON:
          case ObGeoType::MULTIPOLYGON: {
            if (dim1 != 2) {
              result.is_null = true;
            } else if (OB_FAIL(eval_tree_binary(mpy1, g2_tree, context, result))) {
              LOG_WARN("fail to do eval_tree_binary", K(ret), K(g2_tree->type()));
            }
            break;
          }
          default: {
            // should not go here
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected dim provided to overlaps", K(ret), K(dim1));
            break;
          }
        }
      }
    }
    return ret;
  }
};

//===========BIN CART==========
/*Point*/
OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncOverlapsImpl, ObWkbGeomPoint, ObWkbGeomPoint, ObGeoFuncResWithNull)
{
  // point is completely contained by another geometry, so they are not overlaps
  UNUSEDx(g1, g2, context);
  result.bret = false;
  return OB_SUCCESS;
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncOverlapsImpl, ObWkbGeomPoint, ObWkbGeomMultiPoint, ObGeoFuncResWithNull)
{
  // point is completely contained by another geometry, so they are not overlaps
  UNUSEDx(g1, g2, context);
  result.bret = false;
  return OB_SUCCESS;
}
OB_GEO_FUNC_END;

/*Linestring*/
OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncOverlapsImpl, ObWkbGeomLineString, ObWkbGeomLineString, ObGeoFuncResWithNull)
{
  UNUSED(context);
  return eval_overlaps_without_strategy<ObWkbGeomLineString, ObWkbGeomLineString>(
      g1, g2, result.bret);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncOverlapsImpl, ObWkbGeomLineString, ObWkbGeomMultiLineString, ObGeoFuncResWithNull)
{
  UNUSED(context);
  return eval_overlaps_without_strategy<ObWkbGeomLineString, ObWkbGeomMultiLineString>(
      g1, g2, result.bret);
}
OB_GEO_FUNC_END;

/*Polygon*/
OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncOverlapsImpl, ObWkbGeomPolygon, ObWkbGeomPolygon, ObGeoFuncResWithNull)
{
  UNUSED(context);
  return eval_overlaps_without_strategy<ObWkbGeomPolygon, ObWkbGeomPolygon>(g1, g2, result.bret);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncOverlapsImpl, ObWkbGeomPolygon, ObWkbGeomMultiPolygon, ObGeoFuncResWithNull)
{
  UNUSED(context);
  return eval_overlaps_without_strategy<ObWkbGeomPolygon, ObWkbGeomMultiPolygon>(
      g1, g2, result.bret);
}
OB_GEO_FUNC_END;

/*MultiPoint*/
OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncOverlapsImpl, ObWkbGeomMultiPoint, ObWkbGeomPoint, ObGeoFuncResWithNull)
{
  // point is completely contained by another geometry, so they are not overlaps
  UNUSEDx(g1, g2, context);
  result.bret = false;
  return OB_SUCCESS;
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncOverlapsImpl, ObWkbGeomMultiPoint, ObWkbGeomMultiPoint, ObGeoFuncResWithNull)
{
  UNUSED(context);
  return eval_overlaps_without_strategy<ObWkbGeomMultiPoint, ObWkbGeomMultiPoint>(
      g1, g2, result.bret);
}
OB_GEO_FUNC_END;

/*MultiLineString*/
OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncOverlapsImpl, ObWkbGeomMultiLineString, ObWkbGeomLineString, ObGeoFuncResWithNull)
{
  UNUSED(context);
  return eval_overlaps_without_strategy<ObWkbGeomMultiLineString, ObWkbGeomLineString>(
      g1, g2, result.bret);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncOverlapsImpl, ObWkbGeomMultiLineString, ObWkbGeomMultiLineString, ObGeoFuncResWithNull)
{
  UNUSED(context);
  return eval_overlaps_without_strategy<ObWkbGeomMultiLineString, ObWkbGeomMultiLineString>(
      g1, g2, result.bret);
}
OB_GEO_FUNC_END;

/*MultiPolygon*/
OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncOverlapsImpl, ObWkbGeomMultiPolygon, ObWkbGeomPolygon, ObGeoFuncResWithNull)
{
  UNUSED(context);
  return eval_overlaps_without_strategy<ObWkbGeomMultiPolygon, ObWkbGeomPolygon>(
      g1, g2, result.bret);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncOverlapsImpl, ObWkbGeomMultiPolygon, ObWkbGeomMultiPolygon, ObGeoFuncResWithNull)
{
  UNUSED(context);
  return eval_overlaps_without_strategy<ObWkbGeomMultiPolygon, ObWkbGeomMultiPolygon>(
      g1, g2, result.bret);
}
OB_GEO_FUNC_END;

/*Collection*/
OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncOverlapsImpl, ObWkbGeomCollection, ObWkbGeomCollection, ObGeoFuncResWithNull)
{
  return eval_overlaps_gc_other<ObCartesianGeometrycollection>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_GEO1_BEGIN(ObGeoFuncOverlapsImpl, ObWkbGeomCollection, ObGeoFuncResWithNull)
{
  return eval_overlaps_gc_other<ObCartesianGeometrycollection>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_GEO2_BEGIN(ObGeoFuncOverlapsImpl, ObWkbGeomCollection, ObGeoFuncResWithNull)
{
  return eval_overlaps_gc_other<ObCartesianGeometrycollection>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

//===========BIN GEOG==========
/*Point*/
OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncOverlapsImpl, ObWkbGeogPoint, ObWkbGeogPoint, ObGeoFuncResWithNull)
{
  // point is completely contained by another geometry, so they are not overlaps
  UNUSEDx(g1, g2, context);
  result.bret = false;
  return OB_SUCCESS;
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncOverlapsImpl, ObWkbGeogPoint, ObWkbGeogMultiPoint, ObGeoFuncResWithNull)
{
  // point is completely contained by another geometry, so they are not overlaps
  UNUSEDx(g1, g2, context);
  result.bret = false;
  return OB_SUCCESS;
}
OB_GEO_FUNC_END;

/*Linestring*/
OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncOverlapsImpl, ObWkbGeogLineString, ObWkbGeogLineString, ObGeoFuncResWithNull)
{
  UNUSED(context);
  return eval_overlaps_with_nonpoint_strategy<ObWkbGeogLineString, ObWkbGeogLineString>(
      g1, g2, context, result.bret);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncOverlapsImpl, ObWkbGeogLineString, ObWkbGeogMultiLineString, ObGeoFuncResWithNull)
{
  UNUSED(context);
  return eval_overlaps_with_nonpoint_strategy<ObWkbGeogLineString, ObWkbGeogMultiLineString>(
      g1, g2, context, result.bret);
}
OB_GEO_FUNC_END;

/*Polygon*/
OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncOverlapsImpl, ObWkbGeogPolygon, ObWkbGeogPolygon, ObGeoFuncResWithNull)
{
  UNUSED(context);
  return eval_overlaps_with_nonpoint_strategy<ObWkbGeogPolygon, ObWkbGeogPolygon>(
      g1, g2, context, result.bret);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncOverlapsImpl, ObWkbGeogPolygon, ObWkbGeogMultiPolygon, ObGeoFuncResWithNull)
{
  UNUSED(context);
  return eval_overlaps_with_nonpoint_strategy<ObWkbGeogPolygon, ObWkbGeogMultiPolygon>(
      g1, g2, context, result.bret);
}
OB_GEO_FUNC_END;

/*MultiPoint*/
OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncOverlapsImpl, ObWkbGeogMultiPoint, ObWkbGeogPoint, ObGeoFuncResWithNull)
{
  // point is completely contained by another geometry, so they are not overlaps
  UNUSEDx(g1, g2, context);
  result.bret = false;
  return OB_SUCCESS;
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncOverlapsImpl, ObWkbGeogMultiPoint, ObWkbGeogMultiPoint, ObGeoFuncResWithNull)
{
  UNUSED(context);
  return eval_overlaps_with_nonpoint_strategy<ObWkbGeogMultiPoint, ObWkbGeogMultiPoint>(
      g1, g2, context, result.bret);
}
OB_GEO_FUNC_END;

/*MultiLineString*/
OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncOverlapsImpl, ObWkbGeogMultiLineString, ObWkbGeogLineString, ObGeoFuncResWithNull)
{
  UNUSED(context);
  return eval_overlaps_with_nonpoint_strategy<ObWkbGeogMultiLineString, ObWkbGeogLineString>(
      g1, g2, context, result.bret);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncOverlapsImpl, ObWkbGeogMultiLineString, ObWkbGeogMultiLineString, ObGeoFuncResWithNull)
{
  UNUSED(context);
  return eval_overlaps_with_nonpoint_strategy<ObWkbGeogMultiLineString, ObWkbGeogMultiLineString>(
      g1, g2, context, result.bret);
}
OB_GEO_FUNC_END;

/*MultiPolygon*/
OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncOverlapsImpl, ObWkbGeogMultiPolygon, ObWkbGeogPolygon, ObGeoFuncResWithNull)
{
  UNUSED(context);
  return eval_overlaps_with_nonpoint_strategy<ObWkbGeogMultiPolygon, ObWkbGeogPolygon>(
      g1, g2, context, result.bret);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncOverlapsImpl, ObWkbGeogMultiPolygon, ObWkbGeogMultiPolygon, ObGeoFuncResWithNull)
{
  UNUSED(context);
  return eval_overlaps_with_nonpoint_strategy<ObWkbGeogMultiPolygon, ObWkbGeogMultiPolygon>(
      g1, g2, context, result.bret);
}
OB_GEO_FUNC_END;

/*Collection*/
OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncOverlapsImpl, ObWkbGeogCollection, ObWkbGeogCollection, ObGeoFuncResWithNull)
{
  return eval_overlaps_gc_other<ObGeographGeometrycollection>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_GEO1_BEGIN(ObGeoFuncOverlapsImpl, ObWkbGeogCollection, ObGeoFuncResWithNull)
{
  return eval_overlaps_gc_other<ObGeographGeometrycollection>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_GEO2_BEGIN(ObGeoFuncOverlapsImpl, ObWkbGeogCollection, ObGeoFuncResWithNull)
{
  return eval_overlaps_gc_other<ObGeographGeometrycollection>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

//===========TREE CART (not completely)==========
// multipoint
OB_GEO_CART_TREE_FUNC_BEGIN(
    ObGeoFuncOverlapsImpl, ObCartesianMultipoint, ObCartesianMultipoint, ObGeoFuncResWithNull)
{
  // point is completely contained by another geometry, so they are not overlaps
  UNUSEDx(g1, g2, context);
  result.bret = false;
  return OB_SUCCESS;
}
OB_GEO_FUNC_END;

OB_GEO_CART_TREE_FUNC_BEGIN(
    ObGeoFuncOverlapsImpl, ObCartesianMultipoint, ObCartesianPoint, ObGeoFuncResWithNull)
{
  // point is completely contained by another geometry, so they are not overlaps
  UNUSEDx(g1, g2, context);
  result.bret = false;
  return OB_SUCCESS;
}
OB_GEO_FUNC_END;

// multilinestring
OB_GEO_CART_TREE_FUNC_BEGIN(ObGeoFuncOverlapsImpl, ObCartesianMultilinestring,
    ObCartesianMultilinestring, ObGeoFuncResWithNull)
{
  UNUSED(context);
  return eval_overlaps_without_strategy<ObCartesianMultilinestring, ObCartesianMultilinestring>(
      g1, g2, result.bret);
}
OB_GEO_FUNC_END;

OB_GEO_CART_TREE_FUNC_BEGIN(
    ObGeoFuncOverlapsImpl, ObCartesianMultilinestring, ObCartesianLineString, ObGeoFuncResWithNull)
{
  UNUSED(context);
  return eval_overlaps_without_strategy<ObCartesianMultilinestring, ObCartesianLineString>(
      g1, g2, result.bret);
}
OB_GEO_FUNC_END;

// multipolygon
OB_GEO_CART_TREE_FUNC_BEGIN(
    ObGeoFuncOverlapsImpl, ObCartesianMultipolygon, ObCartesianMultipolygon, ObGeoFuncResWithNull)
{
  UNUSED(context);
  return eval_overlaps_without_strategy<ObCartesianMultipolygon, ObCartesianMultipolygon>(
      g1, g2, result.bret);
}
OB_GEO_FUNC_END;

OB_GEO_CART_TREE_FUNC_BEGIN(
    ObGeoFuncOverlapsImpl, ObCartesianMultipolygon, ObCartesianPolygon, ObGeoFuncResWithNull)
{
  UNUSED(context);
  return eval_overlaps_without_strategy<ObCartesianMultipolygon, ObCartesianPolygon>(
      g1, g2, result.bret);
}
OB_GEO_FUNC_END;

//===========TREE GEOG (not completely)==========
// multipoint
OB_GEO_GEOG_TREE_FUNC_BEGIN(
    ObGeoFuncOverlapsImpl, ObGeographMultipoint, ObGeographMultipoint, ObGeoFuncResWithNull)
{
  // point is completely contained by another geometry, so they are not overlaps
  UNUSEDx(g1, g2, context);
  result.bret = false;
  return OB_SUCCESS;
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_TREE_FUNC_BEGIN(
    ObGeoFuncOverlapsImpl, ObGeographMultipoint, ObGeographPoint, ObGeoFuncResWithNull)
{
  // point is completely contained by another geometry, so they are not overlaps
  UNUSEDx(g1, g2, context);
  result.bret = false;
  return OB_SUCCESS;
}
OB_GEO_FUNC_END;

// multilinestring
OB_GEO_GEOG_TREE_FUNC_BEGIN(ObGeoFuncOverlapsImpl, ObGeographMultilinestring,
    ObGeographMultilinestring, ObGeoFuncResWithNull)
{
  UNUSED(context);
  return eval_overlaps_with_nonpoint_strategy<ObGeographMultilinestring, ObGeographMultilinestring>(
      g1, g2, context, result.bret);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_TREE_FUNC_BEGIN(
    ObGeoFuncOverlapsImpl, ObGeographMultilinestring, ObGeographLineString, ObGeoFuncResWithNull)
{
  UNUSED(context);
  return eval_overlaps_with_nonpoint_strategy<ObGeographMultilinestring, ObGeographLineString>(
      g1, g2, context, result.bret);
}
OB_GEO_FUNC_END;

// multipolygon
OB_GEO_GEOG_TREE_FUNC_BEGIN(
    ObGeoFuncOverlapsImpl, ObGeographMultipolygon, ObGeographMultipolygon, ObGeoFuncResWithNull)
{
  UNUSED(context);
  return eval_overlaps_with_nonpoint_strategy<ObGeographMultipolygon, ObGeographMultipolygon>(
      g1, g2, context, result.bret);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_TREE_FUNC_BEGIN(
    ObGeoFuncOverlapsImpl, ObGeographMultipolygon, ObGeographPolygon, ObGeoFuncResWithNull)
{
  UNUSED(context);
  return eval_overlaps_with_nonpoint_strategy<ObGeographMultipolygon, ObGeographPolygon>(
      g1, g2, context, result.bret);
}
OB_GEO_FUNC_END;

int ObGeoFuncOverlaps::eval(const ObGeoEvalCtx &gis_context, ObGeoFuncResWithNull &result)
{
  return ObGeoFuncOverlapsImpl::eval_geo_func(gis_context, result);
}
}  // namespace common
}  // namespace oceanbase