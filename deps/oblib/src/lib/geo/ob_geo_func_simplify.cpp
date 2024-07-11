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
 * This file contains implementation for ob_geo_func_simplify.
 */

#define USING_LOG_PREFIX LIB

#include "lib/geo/ob_geo_dispatcher.h"
#include "lib/geo/ob_geo_func_simplify.h"
#include "boost/geometry/algorithms/simplify.hpp"
#include "lib/oblog/ob_log_module.h"
#include "lib/geo/ob_geo_tree.h"
#include "lib/geo/ob_geo_to_tree_visitor.h"

using namespace oceanbase::common;
namespace bg = boost::geometry;

namespace oceanbase
{
namespace common
{

class ObGeoFuncSimplifyImpl : public ObIGeoDispatcher<ObGeometry *, ObGeoFuncSimplifyImpl>
{
public:
  ObGeoFuncSimplifyImpl();
  virtual ~ObGeoFuncSimplifyImpl() = default;
  OB_GEO_UNARY_FUNC_DEFAULT(ObGeometry *, OB_ERR_GIS_INVALID_DATA);

  // default templates
  OB_GEO_TREE_UNARY_FUNC_DEFAULT(ObGeometry *, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);
  OB_GEO_CART_BINARY_FUNC_DEFAULT(ObGeometry *, OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS);
  OB_GEO_GEOG_BINARY_FUNC_DEFAULT(ObGeometry *, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);
  OB_GEO_CART_TREE_FUNC_DEFAULT(ObGeometry *, OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS);
  OB_GEO_GEOG_TREE_FUNC_DEFAULT(ObGeometry *, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);

private:
  template<typename GeometryInType, typename GeometryResType>
  static int eval_simplify_without_strategy(const ObGeometry *g, 
                                            const ObGeoEvalCtx &context, 
                                            ObGeometry *&result)
  {
    INIT_SUCC(ret);
    GeometryResType *dest_geo = nullptr;
    common::ObIAllocator *alloc = context.get_allocator();
    if (OB_ISNULL(alloc)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("unexpected null alloactor for simplify functor", K(ret));
    } else if (OB_ISNULL(dest_geo = OB_NEWx(GeometryResType, alloc))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to create geo by type", K(ret));
    } else {
      double max_dist = context.get_val_arg(0)->double_;
      // const GeometryInType *src_geo = reinterpret_cast<const GeometryInType *>(g->val());

      // Change src_geo's type from bin to tree, to fit the bg::simplify
      ObGeoToTreeVisitor tree_visitor(alloc);
      if (OB_FAIL(const_cast<ObGeometry *>(g)->do_visit(tree_visitor))) {
        LOG_WARN("fail to convert geometry to tree", K(ret));
      } else {
        const GeometryResType *src_geo_tree = static_cast<const GeometryResType *>(tree_visitor.get_geometry());
        bg::simplify(*src_geo_tree, *dest_geo, max_dist);
        result = dest_geo;
      }
    }
    return ret;
  }

  template<typename GCInType, typename GCOutType>
  static int eval_simplify_gc(const ObGeometry *g, 
                              const ObGeoEvalCtx &context, 
                              ObGeometry *&result)
  {
    INIT_SUCC(ret);
    GCOutType *dest_geo = nullptr;
    common::ObIAllocator *alloc = context.get_allocator();
    if (OB_ISNULL(alloc)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Null alloc", K(ret));
    } else if (OB_ISNULL(dest_geo = OB_NEWx(GCOutType, alloc, 0, *alloc))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to create geometry collection", K(ret));
    } else {
      const GCInType *src_geo = reinterpret_cast<const GCInType *>(const_cast<char *>(g->val()));
      typename GCInType::iterator iter = src_geo->begin();
      typename GCInType::const_pointer sub_ptr;
      for (; iter != src_geo->end() && OB_SUCC(ret); iter++) {
        sub_ptr = iter.operator->();
        ObGeoType sub_type = src_geo->get_sub_type(sub_ptr);
        ObGeometry *sub_geo = nullptr;
        if (OB_FAIL(ObGeoTypeUtil::create_geo_by_type(*alloc, sub_type, false, true, sub_geo))) {
          LOG_WARN("failed to create wkb", K(ret), K(sub_type));
        } else {
          ObString wkb_nosrid(WKB_COMMON_WKB_HEADER_LEN, reinterpret_cast<const char *>(sub_ptr));
          sub_geo->set_data(wkb_nosrid);
          sub_geo->set_srid(g->get_srid());
        }
        if (OB_SUCC(ret)) {
          ObGeometry *sub_res = nullptr;
          ret = eval_wkb_unary(sub_geo, context, sub_res);
          if (OB_SUCC(ret)) {
            if (OB_ISNULL(sub_res) || sub_res->is_empty()){
              continue;
            } else if (OB_FAIL(dest_geo->push_back(*sub_res))) {
              LOG_WARN("failed to push back to geo", K(ret));
            }
          }
        }
      } // for end
    } // else end
    if (OB_SUCC(ret)) {
      result = dest_geo;
    }
    return ret;
  }

};

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncSimplifyImpl, ObWkbGeomCollection, ObGeometry * )
{
  UNUSED(context);
  return eval_simplify_gc<ObWkbGeomCollection, ObCartesianGeometrycollection>(g, context, result);
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncSimplifyImpl, ObWkbGeomPoint, ObGeometry *)
{
  return eval_simplify_without_strategy<ObWkbGeomPoint, ObCartesianPoint>(g, context, result);
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncSimplifyImpl, ObWkbGeomMultiPoint, ObGeometry *)
{
  return eval_simplify_without_strategy<ObWkbGeomMultiPoint, ObCartesianMultipoint>(g, context, result);
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncSimplifyImpl, ObWkbGeomLineString, ObGeometry *)
{
  INIT_SUCC(ret);
  ret = eval_simplify_without_strategy<ObWkbGeomLineString, ObCartesianLineString>(g, context, result);
  if (OB_SUCC(ret)) {
    ObCartesianLineString* tmp = static_cast<ObCartesianLineString*>(result);
    if (tmp->size() < 2) {
      tmp->clear();
    }
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncSimplifyImpl, ObWkbGeomMultiLineString, ObGeometry *)
{
  INIT_SUCC(ret);
  ret = eval_simplify_without_strategy<ObWkbGeomMultiLineString, ObCartesianMultilinestring>(g, context, result);
  if (OB_SUCC(ret)) {
    // bg::simplify may create geometries with too few points. Filter out those.
    ObCartesianMultilinestring *tmp = static_cast<ObCartesianMultilinestring *>(result);
    ObCartesianMultilinestring *res = nullptr;
    if (OB_ISNULL(res = OB_NEWx(ObCartesianMultilinestring, context.get_allocator()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to create geo by type", K(ret));
    } else {
      for (int i = 0; i < tmp->size(); ++i) {
        if ((*tmp)[i].size() >= 2) {
          res->push_back((*tmp)[i]);
        }
      }
      tmp->clear();
      result = res;
    }
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncSimplifyImpl, ObWkbGeomPolygon, ObGeometry *)
{
  INIT_SUCC(ret);
  ret = eval_simplify_without_strategy<ObWkbGeomPolygon, ObCartesianPolygon>(g, context, result);
  if (OB_SUCC(ret)) {
    ObCartesianPolygon *tmp = static_cast<ObCartesianPolygon *>(result);
    if (tmp->exterior_ring().size() < 4) {
      context.get_allocator()->free(tmp);
      result = OB_NEWx(ObCartesianPolygon, context.get_allocator());
    }
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncSimplifyImpl, ObWkbGeomMultiPolygon, ObGeometry *)
{
  INIT_SUCC(ret);
  ret = eval_simplify_without_strategy<ObWkbGeomMultiPolygon, ObCartesianMultipolygon>(g, context, result);
  if (OB_SUCC(ret)) {
    // bg::simplify may create geometries with too few points. Filter out those.
    ObCartesianMultipolygon *tmp = static_cast<ObCartesianMultipolygon *>(result);
    ObCartesianMultipolygon *res = nullptr;
    if (OB_ISNULL(res = OB_NEWx(ObCartesianMultipolygon, context.get_allocator()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to create geo by type", K(ret));
    } else {
      for (int i = 0; i < tmp->size(); ++i) {
        if ((*tmp)[i].exterior_ring().size() >= 4) {
          res->push_back((*tmp)[i]);
        }
      }
      tmp->clear();
      result = res;
    }
  }
  return ret;
} OB_GEO_FUNC_END;

// implement of outer class eval
// use an outer class to void implement templates in header files
int ObGeoFuncSimplify::eval(const ObGeoEvalCtx &gis_context, ObGeometry *&result)
{
  return ObGeoFuncSimplifyImpl::eval_geo_func(gis_context, result);
}

} // sql
} // oceanbase