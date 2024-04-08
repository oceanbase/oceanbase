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
 * This file contains implementation for ob_geo_func_dissolve_polygon.
 */

#define USING_LOG_PREFIX LIB

#include "lib/geo/ob_geo_dispatcher.h"
#include "lib/geo/ob_geo_func_dissolve_polygon.h"
#include "lib/geo/ob_geo_func_disjoint.h"
#include "lib/geo/ob_geo_utils.h"
#include "lib/geo/ob_geo_func_utils.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace common
{
namespace bg = boost::geometry;
template<typename GeometryType, typename MPY>
static int eval_dissolve_polygon(
    const ObGeometry *g1, const ObGeoEvalCtx &context, ObGeometry *&result)
{
  int ret = OB_SUCCESS;

  GeometryType *geo1 = nullptr;
  if (!g1->is_tree()) {
    geo1 = reinterpret_cast<GeometryType *>(const_cast<char *>(g1->val()));
  } else {
    geo1 = reinterpret_cast<GeometryType *>(const_cast<ObGeometry *>(g1));
  }
  if (OB_ISNULL(geo1)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create go by type", K(ret), K(geo1));
  } else {
    bool is_valid = bg::is_valid(*geo1);
    if (!is_valid) {
      bool is_self_intersects = bg::intersects(*geo1);
      MPY *res = OB_NEWx(MPY, context.get_allocator(), g1->get_srid(), *context.get_allocator());
      if (OB_ISNULL(res)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to create go by type", K(ret), K(res));
      } else if (is_self_intersects) {
        MPY *left_part =
            OB_NEWx(MPY, context.get_allocator(), g1->get_srid(), *context.get_allocator());
        MPY *right_part =
            OB_NEWx(MPY, context.get_allocator(), g1->get_srid(), *context.get_allocator());
        if (OB_ISNULL(res) || OB_ISNULL(left_part) || OB_ISNULL(right_part)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to create go by type", K(ret), K(res), K(left_part), K(right_part));
        } else {
          GeometryType *g1_rev = OB_NEWx(GeometryType, context.get_allocator(), *geo1);
          if (OB_ISNULL(g1_rev)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to create go by type", K(ret));
          } else {
            bg::reverse(*g1_rev);
            bg::sym_difference(*geo1, *g1_rev, *res);
          }
        }
      } else {
        bg::intersection(*geo1, *geo1, *res);
        if (res->is_empty() && !geo1->is_empty()) {
          bg::reverse(*geo1);
          bg::intersection(*geo1, *geo1, *res);
        }
      }
      if (res->size() == 1) {
        result = &res->front();
      } else {
        result = res;
      }
    } else {
      result = geo1;
    }
  }
  return ret;
}

template<typename GeometryType, typename MPY>
static int eval_dissolve_multipolygon(
    const ObGeometry *g1, const ObGeoEvalCtx &context, ObGeometry *&result)
{
  int ret = OB_SUCCESS;
  GeometryType *geo1 = nullptr;
  if (!g1->is_tree()) {
    geo1 = reinterpret_cast<GeometryType *>(const_cast<char *>(g1->val()));
  } else {
    geo1 = reinterpret_cast<GeometryType *>(const_cast<ObGeometry *>(g1));
  }
  if (OB_ISNULL(geo1)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create go by type", K(ret), K(geo1));
  } else {
    bool is_valid = bg::is_valid(*geo1);
    if (!is_valid) {
      bool is_self_intersects = bg::intersects(*geo1);
      MPY *res = OB_NEWx(MPY, context.get_allocator(), g1->get_srid(), *context.get_allocator());
      if (OB_ISNULL(res)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to create go by type", K(ret), K(res));
      } else if (is_self_intersects) {
        MPY *left_part =
            OB_NEWx(MPY, context.get_allocator(), g1->get_srid(), *context.get_allocator());
        MPY *right_part =
            OB_NEWx(MPY, context.get_allocator(), g1->get_srid(), *context.get_allocator());
        if (OB_ISNULL(res) || OB_ISNULL(left_part) || OB_ISNULL(right_part)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to create go by type", K(ret), K(res), K(left_part), K(right_part));
        } else {
          bg::intersection(*geo1, *geo1, *left_part);
          bg::reverse(*geo1);
          bg::intersection(*geo1, *geo1, *right_part);
          bg::union_(*left_part, *right_part, *res);
        }
      } else {
        bg::intersection(*geo1, *geo1, *res);
        if (res->is_empty() && !geo1->is_empty()) {
          bg::reverse(*geo1);
          bg::intersection(*geo1, *geo1, *res);
        }
      }
      if (OB_FAIL(ret)) {
      } else if (res->size() == 1) {
        result = &res->front();
      } else {
        result = res;
      }
    } else {
      result = geo1;
    }
  }
  return ret;
}

class ObGeoFuncDissolvePolygonImpl
    : public ObIGeoDispatcher<ObGeometry *, ObGeoFuncDissolvePolygonImpl>
{
public:
  ObGeoFuncDissolvePolygonImpl();
  virtual ~ObGeoFuncDissolvePolygonImpl() = default;
  OB_GEO_UNARY_FUNC_DEFAULT(ObGeometry *, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);
  OB_GEO_TREE_UNARY_FUNC_DEFAULT(ObGeometry *, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);
  OB_GEO_CART_BINARY_FUNC_DEFAULT(ObGeometry *, OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS);
  OB_GEO_GEOG_BINARY_FUNC_DEFAULT(ObGeometry *, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);
  OB_GEO_CART_TREE_FUNC_DEFAULT(ObGeometry *, OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS);
  OB_GEO_GEOG_TREE_FUNC_DEFAULT(ObGeometry *, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);
};

OB_GEO_UNARY_TREE_FUNC_BEGIN(ObGeoFuncDissolvePolygonImpl, ObCartesianPolygon, ObGeometry *)
{
  return eval_dissolve_polygon<ObCartesianPolygon, ObCartesianMultipolygon>(g, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_UNARY_TREE_FUNC_BEGIN(ObGeoFuncDissolvePolygonImpl, ObCartesianMultipolygon, ObGeometry *)
{
  return eval_dissolve_multipolygon<ObCartesianMultipolygon, ObCartesianMultipolygon>(
      g, context, result);
}
OB_GEO_FUNC_END;

int ObGeoFuncDissolvePolygon::eval(
    const common::ObGeoEvalCtx &gis_context, common::ObGeometry *&result)
{
  return ObGeoFuncDissolvePolygonImpl::eval_geo_func(gis_context, result);
}

}  // namespace common
}  // namespace oceanbase