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
 * This file contains implementation for ob_geo_func_correct.
 */

#define USING_LOG_PREFIX LIB

#include "lib/geo/ob_geo_dispatcher.h"
#include "lib/geo/ob_geo_func_correct.h"
#include "lib/geo/ob_geo_utils.h"
#include "lib/oblog/ob_log_module.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace common
{
class ObGeoFuncCorrectImpl : public ObIGeoDispatcher<int, ObGeoFuncCorrectImpl>
{
  // do nothing for geo types other then polygons and boxes
public:
  ObGeoFuncCorrectImpl();
  virtual ~ObGeoFuncCorrectImpl() = default;
  // default templates
  OB_GEO_UNARY_FUNC_DEFAULT(int, OB_SUCCESS);
  OB_GEO_TREE_UNARY_FUNC_DEFAULT(int, OB_SUCCESS);
  OB_GEO_CART_BINARY_FUNC_DEFAULT(int, OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS);
  OB_GEO_GEOG_BINARY_FUNC_DEFAULT(int, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);
  OB_GEO_CART_TREE_FUNC_DEFAULT(int, OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS);
  OB_GEO_GEOG_TREE_FUNC_DEFAULT(int, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);
private:
  template <typename GeoType>
  static int correct_default(const ObGeometry *g, const ObGeoEvalCtx &context)
  {
    UNUSED(context);
    GeoType *geo_candidate = NULL;
    if (!g->is_tree()) {
      geo_candidate = reinterpret_cast<GeoType *>(const_cast<char *>(g->val()));
    } else {
      geo_candidate = reinterpret_cast<GeoType *>(const_cast<ObGeometry *>(g));
    }
    boost::geometry::correct(*geo_candidate);
    return OB_SUCCESS;
  }

  template <typename GeoType>
  static int correct_with_strategy(const ObGeometry *g, const ObGeoEvalCtx &context)
  {
    INIT_SUCC(ret);
    const ObSrsItem *srs = context.get_srs();
    if (OB_ISNULL(srs)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("srs is null", K(ret), K(g->get_srid()), K(g));
    } else {
      boost::geometry::srs::spheroid<double> geog_sphere(srs->semi_major_axis(), srs->semi_minor_axis());
      boost::geometry::strategy::area::geographic<> area_strategy(geog_sphere);
      GeoType *geo_candidate = NULL;
      if (!g->is_tree()) {
        geo_candidate = reinterpret_cast<GeoType *>(const_cast<char *>(g->val()));
      } else {
        geo_candidate = reinterpret_cast<GeoType *>(const_cast<ObGeometry *>(g));
      }
      boost::geometry::correct(*geo_candidate, area_strategy);
    }
    return ret;
  }

  template <typename GeoTypeCollection, bool IsGeog>
  static int correct_collection(const ObGeometry *g, const ObGeoEvalCtx &context)
  {
    INIT_SUCC(ret);
    common::ObIAllocator *allocator = context.get_allocator();
    typename GeoTypeCollection::iterator iter;
    if (OB_ISNULL(allocator)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Null allocator", K(ret));
    } else if (g->type() == ObGeoType::GEOMETRYCOLLECTION) {
      const GeoTypeCollection *geo = reinterpret_cast<const GeoTypeCollection *>(g->val());
      iter = geo->begin();
      for (; iter != geo->end() && OB_SUCC(ret); iter++) {
        typename GeoTypeCollection::const_pointer sub_ptr = iter.operator->();
        ObGeoType sub_type = geo->get_sub_type(sub_ptr);
        ObGeometry *sub_g = NULL;
        if (OB_FAIL(ObGeoTypeUtil::create_geo_by_type(*allocator, sub_type, IsGeog, true, sub_g))) {
          LOG_WARN("failed to create wkb", K(ret), K(sub_type));
        } else {
          // Length is not used, cannot get real length until iter move to the next
          ObString wkb_nosrid(WKB_COMMON_WKB_HEADER_LEN, reinterpret_cast<const char *>(sub_ptr));
          sub_g->set_data(wkb_nosrid);
          sub_g->set_srid(g->get_srid());
        }
        if (OB_FAIL(ret)) {
        } else if (sub_type == ObGeoType::GEOMETRYCOLLECTION) {
          ret = correct_collection<GeoTypeCollection, IsGeog>(sub_g, context);
        } else if (sub_type == ObGeoType::POLYGON) {
          if (IsGeog) {
            ret = correct_with_strategy<ObWkbGeogPolygon>(sub_g, context);
          } else {
            ret = correct_default<ObWkbGeomPolygon>(sub_g, context);
          }
        } else if (sub_type == ObGeoType::MULTIPOLYGON) {
          if (IsGeog) {
            ret = correct_with_strategy<ObWkbGeogMultiPolygon>(sub_g, context);
          } else {
            ret = correct_default<ObWkbGeomMultiPolygon>(sub_g, context);
          }
        } else { /* do nothing */ }
      }
    }
    return ret;
  }

  template <typename GeoTypeCollection, bool IsGeog>
  static int correct_tree_collection(const ObGeometry *g, const ObGeoEvalCtx &context)
  {
    INIT_SUCC(ret);
    const GeoTypeCollection *geo = reinterpret_cast<const GeoTypeCollection *>(const_cast<ObGeometry *>(g));
    const ObGeometry *sub_g = NULL;
    for (uint64_t i = 0; i < geo->size() && OB_SUCC(ret); i++) {
      sub_g = &(*geo)[i];
      ObGeoType sub_type = sub_g->type();
      if (sub_type == ObGeoType::GEOMETRYCOLLECTION) {
        ret = correct_tree_collection<GeoTypeCollection, IsGeog>(sub_g, context);
      } else if (sub_type == ObGeoType::POLYGON) {
        if (IsGeog) {
          ret = correct_with_strategy<ObGeographPolygon>(sub_g, context);
        } else {
          ret = correct_default<ObCartesianPolygon>(sub_g, context);
        }
      } else if (sub_type == ObGeoType::MULTIPOLYGON) {
        if (IsGeog) {
          ret = correct_with_strategy<ObGeographMultipolygon>(sub_g, context);
        } else {
          ret = correct_default<ObCartesianMultipolygon>(sub_g, context);
        }
      } else { /* do nothing */ }
    }
    return ret;
  }
};

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncCorrectImpl, ObWkbGeomPolygon, int)
{
  UNUSED(result);
  return correct_default<ObWkbGeomPolygon>(g, context);
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncCorrectImpl, ObWkbGeomMultiPolygon, int)
{
  UNUSED(result);
  return correct_default<ObWkbGeomMultiPolygon>(g, context);
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncCorrectImpl, ObWkbGeomCollection, int)
{
  UNUSED(result);
  return correct_collection<ObWkbGeomCollection, false>(g, context);
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncCorrectImpl, ObWkbGeogPolygon, int)
{
  UNUSED(result);
  return correct_with_strategy<ObWkbGeogPolygon>(g, context);
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncCorrectImpl, ObWkbGeogMultiPolygon, int)
{
  UNUSED(result);
  return correct_with_strategy<ObWkbGeogMultiPolygon>(g, context);
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncCorrectImpl, ObWkbGeogCollection, int)
{
  UNUSED(result);
  return correct_collection<ObWkbGeogCollection, true>(g, context);
} OB_GEO_FUNC_END;

// tree
OB_GEO_UNARY_TREE_FUNC_BEGIN(ObGeoFuncCorrectImpl, ObCartesianPolygon, int)
{
  UNUSED(result);
  return correct_default<ObCartesianPolygon>(g, context);
} OB_GEO_FUNC_END;

OB_GEO_UNARY_TREE_FUNC_BEGIN(ObGeoFuncCorrectImpl, ObCartesianMultipolygon, int)
{
  UNUSED(result);
  return correct_default<ObCartesianMultipolygon>(g, context);
} OB_GEO_FUNC_END;

OB_GEO_UNARY_TREE_FUNC_BEGIN(ObGeoFuncCorrectImpl, ObCartesianGeometrycollection, int)
{
  UNUSED(result);
  return correct_tree_collection<ObCartesianGeometrycollection, false>(g, context);
} OB_GEO_FUNC_END;

OB_GEO_UNARY_TREE_FUNC_BEGIN(ObGeoFuncCorrectImpl, ObGeographPolygon, int)
{
  UNUSED(result);
  return correct_with_strategy<ObGeographPolygon>(g, context);
} OB_GEO_FUNC_END;

OB_GEO_UNARY_TREE_FUNC_BEGIN(ObGeoFuncCorrectImpl, ObGeographMultipolygon, int)
{
  UNUSED(result);
  return correct_with_strategy<ObGeographMultipolygon>(g, context);
} OB_GEO_FUNC_END;

OB_GEO_UNARY_TREE_FUNC_BEGIN(ObGeoFuncCorrectImpl, ObGeographGeometrycollection, int)
{
  UNUSED(result);
  return correct_tree_collection<ObGeographGeometrycollection, true>(g, context);
} OB_GEO_FUNC_END;

int ObGeoFuncCorrect::eval(const ObGeoEvalCtx &gis_context, int &result)
{
  return ObGeoFuncCorrectImpl::eval_geo_func(gis_context, result);
}

} // sql
} // oceanbase