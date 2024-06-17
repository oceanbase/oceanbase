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
 * This file contains implementation for ob_geo_func_centroid.
 */

#define USING_LOG_PREFIX LIB

#include "lib/geo/ob_geo_dispatcher.h"
#include "lib/geo/ob_geo_func_centroid.h"
#include "lib/geo/ob_geo_utils.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/geo/ob_geo_func_utils.h"
#include "lib/geo/ob_geo_to_tree_visitor.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace common
{
class ObGeoFuncCentroidImpl : public ObIGeoDispatcher<ObGeometry *, ObGeoFuncCentroidImpl>
{
  // do nothing for geo types other then polygons and boxes
public:
  ObGeoFuncCentroidImpl();
  virtual ~ObGeoFuncCentroidImpl() = default;
  // default templates
  OB_GEO_UNARY_FUNC_DEFAULT(ObGeometry *, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);
  OB_GEO_TREE_UNARY_FUNC_DEFAULT(ObGeometry *, OB_ERR_GIS_INVALID_DATA);
  OB_GEO_CART_BINARY_FUNC_DEFAULT(ObGeometry *, OB_ERR_GIS_INVALID_DATA);
  OB_GEO_GEOG_BINARY_FUNC_DEFAULT(ObGeometry *, OB_ERR_GIS_INVALID_DATA);
  OB_GEO_CART_TREE_FUNC_DEFAULT(ObGeometry *, OB_ERR_GIS_INVALID_DATA);
  OB_GEO_GEOG_TREE_FUNC_DEFAULT(ObGeometry *, OB_ERR_GIS_INVALID_DATA);

private:
  static int centroid_collection(const ObGeometry *g, const ObGeoEvalCtx &context, ObGeometry *&result)
  {
    int ret = OB_SUCCESS;
    if (g->type() != ObGeoType::GEOMETRYCOLLECTION) {
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_WARN("input geometry is not collection type", K(ret), K(g->type()));
    } else {
      ObCartesianMultipoint *mpt = nullptr;
      ObCartesianMultilinestring *ml = nullptr;
      ObCartesianMultipolygon *mpo = nullptr;
      ObIAllocator *allocator = context.get_allocator();
      ObCartesianPoint *res_geo = OB_NEWx(ObCartesianPoint, allocator, 0, 0, g->get_srid(), allocator);
      ObGeoToTreeVisitor tree_visitor(allocator);
      if (OB_ISNULL(res_geo)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory for result grometry", K(ret));
      } else if (OB_FAIL(const_cast<ObGeometry *>(g)->do_visit(tree_visitor))) {
        LOG_WARN("failed to transform gc to tree", K(ret));
      } else if (OB_FAIL(ObGeoFuncUtils::ob_geo_gc_split(*allocator,
              *static_cast<const ObCartesianGeometrycollection *>(tree_visitor.get_geometry()),
                  mpt, ml, mpo))) { // only split, do not union
        LOG_WARN("failed to do gc split", K(ret));
      } else if (OB_ISNULL(mpt) || OB_ISNULL(ml) || OB_ISNULL(mpo)) {
        ret = OB_ERR_GIS_INVALID_DATA;
        LOG_WARN("unexpected null geometry collection split", K(ret), KP(mpt), KP(ml), KP(mpo));
      } else {
        if (!mpo->empty()) {
          boost::geometry::centroid(*mpo, *res_geo);
        } else if (!ml->empty()) {
          boost::geometry::centroid(*ml, *res_geo);
        } else {
          // return OB_ERR_BOOST_GEOMETRY_CENTROID_EXCEPTION if mpt is empty too
          // need caller function to decide what to do
          boost::geometry::centroid(*mpt, *res_geo);
        }
        result = res_geo;
      }
    }
    return ret;
  }

  template<typename GeoType>
  static int centroid_default(const ObGeometry *g, const ObGeoEvalCtx &context, ObGeometry *&result)
  {
    int ret = OB_SUCCESS;
    ObCartesianPoint *res_geo = OB_NEWx(ObCartesianPoint, context.get_allocator(), 0, 0, g->get_srid(), context.get_allocator());
    if (OB_ISNULL(res_geo)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory for result grometry", K(ret));
    } else {
      GeoType *geo_candidate = nullptr;
      if (!g->is_tree()) {
        geo_candidate = reinterpret_cast<GeoType *>(const_cast<char *>(g->val()));
      } else {
        geo_candidate = reinterpret_cast<GeoType *>(const_cast<ObGeometry *>(g));
      }
      if (OB_ISNULL(geo_candidate)) {
        ret = OB_ERR_INVALID_NULL_SDO_GEOMETRY;
        LOG_WARN("invalid null geometry", K(ret));
      } else {
        boost::geometry::centroid(*geo_candidate, *res_geo);
        result = res_geo;
      }
    }
    return ret;
  }
};

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncCentroidImpl, ObWkbGeomPoint, ObGeometry *)
{
  return centroid_default<ObWkbGeomPoint>(g, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncCentroidImpl, ObWkbGeomMultiPoint, ObGeometry *)
{
  return centroid_default<ObWkbGeomMultiPoint>(g, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncCentroidImpl, ObWkbGeomLineString, ObGeometry *)
{
  return centroid_default<ObWkbGeomLineString>(g, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncCentroidImpl, ObWkbGeomMultiLineString, ObGeometry *)
{
  return centroid_default<ObWkbGeomMultiLineString>(g, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncCentroidImpl, ObWkbGeomPolygon, ObGeometry *)
{
  return centroid_default<ObWkbGeomPolygon>(g, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncCentroidImpl, ObWkbGeomMultiPolygon, ObGeometry *)
{
  return centroid_default<ObWkbGeomMultiPolygon>(g, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncCentroidImpl, ObWkbGeomCollection, ObGeometry *)
{
  return centroid_collection(g, context, result);
}
OB_GEO_FUNC_END;

int ObGeoFuncCentroid::eval(const ObGeoEvalCtx &gis_context, ObGeometry *&result)
{
  return ObGeoFuncCentroidImpl::eval_geo_func(gis_context, result);
}

}  // namespace common
}  // namespace oceanbase