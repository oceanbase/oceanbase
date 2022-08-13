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
 * This file contains implementation for ob_geo_func_box.
 */

#define USING_LOG_PREFIX LIB

#include "lib/geo/ob_geo_dispatcher.h"
#include "lib/geo/ob_geo_func_box.h"
#include "lib/geo/ob_geo_utils.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace common
{

class ObGeoFuncBoxImpl : public ObIGeoDispatcher<ObGeogBox *, ObGeoFuncBoxImpl>
{
public:
  ObGeoFuncBoxImpl();
  virtual ~ObGeoFuncBoxImpl() = default;

  OB_GEO_UNARY_FUNC_DEFAULT(ObGeogBox *, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);
  OB_GEO_TREE_UNARY_FUNC_DEFAULT(ObGeogBox *, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);
  OB_GEO_CART_BINARY_FUNC_DEFAULT(ObGeogBox *, OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS);
  OB_GEO_GEOG_BINARY_FUNC_DEFAULT(ObGeogBox *, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);
  OB_GEO_CART_TREE_FUNC_DEFAULT(ObGeogBox *, OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS);
  OB_GEO_GEOG_TREE_FUNC_DEFAULT(ObGeogBox *, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);

};

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncBoxImpl, ObWkbGeogPoint, ObGeogBox *)
{
  INIT_SUCC(ret);
  ObGeogBox *res = OB_NEWx(ObGeogBox, context.get_allocator());
  if (OB_ISNULL(res)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create geo box", K(ret));
  } else {
    const ObWkbGeogPoint *geo = reinterpret_cast<const ObWkbGeogPoint *>(g->val());
    ObWkbGeogInnerPoint point_tmp(geo->get<0>(), geo->get<1>());
    if (OB_FAIL(ObGeoBoxUtil::get_geog_point_box(point_tmp, *res))) {
      LOG_WARN("fail to get point box", K(ret));
    } else {
      result = res;
    }
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncBoxImpl, ObWkbGeogLineString, ObGeogBox *)
{
  INIT_SUCC(ret);
  ObGeogBox *res = OB_NEWx(ObGeogBox, context.get_allocator());
  if (OB_ISNULL(res)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create geo box", K(ret));
  } else {
    const ObWkbGeogLineString *line = reinterpret_cast<const ObWkbGeogLineString *>(g->val());
    if (OB_FAIL(ObGeoBoxUtil::get_geog_line_box(*line, *res))) {
      LOG_WARN("fail to get line box", K(ret));
    } else {
      result = res;
    }
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncBoxImpl, ObWkbGeogPolygon, ObGeogBox *)
{
  INIT_SUCC(ret);
  ObGeogBox *res = OB_NEWx(ObGeogBox, context.get_allocator());
  if (OB_ISNULL(res)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create geo box", K(ret));
  } else {
    const ObWkbGeogPolygon *poly = reinterpret_cast<const ObWkbGeogPolygon *>(g->val());
    if (OB_FAIL(ObGeoBoxUtil::get_geog_poly_box(*poly, *res))) {
      LOG_WARN("fail to get poly box", K(ret));
    } else {
      result = res;
    }
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncBoxImpl, ObWkbGeogMultiPoint, ObGeogBox *)
{
  INIT_SUCC(ret);
  ObGeogBox *res = OB_NEWx(ObGeogBox, context.get_allocator());
  if (OB_ISNULL(res)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create geo box", K(ret));
  } else {
    const ObWkbGeogMultiPoint *geo = reinterpret_cast<const ObWkbGeogMultiPoint *>(g->val());
    ObWkbGeogMultiPoint::iterator iter = geo->begin();
    bool is_start = false;
    for (; iter != geo->end() && OB_SUCC(ret); iter++) {
      ObGeogBox tmp;
      if (OB_FAIL(ObGeoBoxUtil::get_geog_point_box(*iter, tmp))) {
        LOG_WARN("fail to get point box", K(ret));
      } else if (!is_start) {
        *res = tmp;
        is_start = true;
      } else {
        ObGeoBoxUtil::box_uion(tmp, *res);
      }
    }
    if (OB_SUCC(ret)) {
      result = res;
    }
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncBoxImpl, ObWkbGeogMultiLineString, ObGeogBox *)
{
  INIT_SUCC(ret);
  ObGeogBox *res = OB_NEWx(ObGeogBox, context.get_allocator());
  if (OB_ISNULL(res)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create geo box", K(ret));
  } else {
    const ObWkbGeogMultiLineString *geo = reinterpret_cast<const ObWkbGeogMultiLineString *>(g->val());
    ObWkbGeogMultiLineString::iterator iter = geo->begin();
    bool is_start = false;
    for (; iter != geo->end() && OB_SUCC(ret); iter++) {
      ObGeogBox tmp;
      if (OB_FAIL(ObGeoBoxUtil::get_geog_line_box(*iter, tmp))) {
        LOG_WARN("fail to get line box", K(ret));
      } else if (!is_start) {
        *res = tmp;
        is_start = true;
      } else {
        ObGeoBoxUtil::box_uion(tmp, *res);
      }
    }
    if (OB_SUCC(ret)) {
      result = res;
    }
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncBoxImpl, ObWkbGeogMultiPolygon, ObGeogBox *)
{
  INIT_SUCC(ret);
  ObGeogBox *res = OB_NEWx(ObGeogBox, context.get_allocator());
  if (OB_ISNULL(res)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create geo box", K(ret));
  } else {
    const ObWkbGeogMultiPolygon *geo = reinterpret_cast<const ObWkbGeogMultiPolygon *>(g->val());
    ObWkbGeogMultiPolygon::iterator iter = geo->begin();
    bool is_start = false;
    for (; iter != geo->end() && OB_SUCC(ret); iter++) {
      ObGeogBox tmp;
      if (OB_FAIL(ObGeoBoxUtil::get_geog_poly_box(*iter, tmp))) {
        LOG_WARN("fail to get poly box", K(ret));
      } else if (!is_start) {
        *res = tmp;
        is_start = true;
      } else {
        ObGeoBoxUtil::box_uion(tmp, *res);
      }
    }
    if (OB_SUCC(ret)) {
      result = res;
    }
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncBoxImpl, ObWkbGeogCollection, ObGeogBox *)
{
  INIT_SUCC(ret);
  common::ObIAllocator *allocator = context.get_allocator();
  ObGeogBox *res = OB_NEWx(ObGeogBox, allocator);
  if (OB_ISNULL(res)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create geo box", K(ret));
  } else {
    const ObWkbGeogCollection *geo = reinterpret_cast<const ObWkbGeogCollection *>(g->val());
    ObWkbGeogCollection::iterator iter;
    bool is_start = false;
    iter = geo->begin();
    for (; iter != geo->end() && OB_SUCC(ret); iter++) {
      ObWkbGeogCollection::const_pointer sub_ptr = iter.operator->();
      ObGeoType sub_type = geo->get_sub_type(sub_ptr);
      ObGeometry *sub_g = NULL;
      if (OB_FAIL(ObGeoTypeUtil::create_geo_by_type(*allocator, sub_type, true, true, sub_g))) {
        LOG_WARN("failed to create wkb", K(ret), K(sub_type));
      } else {
        // Length is not used, cannot get real length until iter move to the next
        ObString wkb_nosrid(WKB_COMMON_WKB_HEADER_LEN, reinterpret_cast<const char *>(sub_ptr));
        sub_g->set_data(wkb_nosrid);
        sub_g->set_srid(g->get_srid());
        ObGeogBox *subres = NULL;
        if (sub_type == ObGeoType::GEOMETRYCOLLECTION) {
          ret = eval(sub_g, context, subres);
        } else {
          ret = eval_wkb_unary(sub_g, context, subres);
        }
        if (OB_SUCC(ret)) {
          if (OB_ISNULL(subres)) {
            ret = OB_ERR_NULL_VALUE;
            LOG_WARN("subres is null", K(ret), K(sub_type));
          } else {
            if (!is_start) {
              *res = *subres;
              is_start = true;
            } else {
              ObGeoBoxUtil::box_uion(*subres, *res);
            }
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      result = res;
    }
  }
  return ret;
} OB_GEO_FUNC_END;


int ObGeoFuncBox::eval(const ObGeoEvalCtx &gis_context, ObGeogBox *&result)
{
  return ObGeoFuncBoxImpl::eval_geo_func(gis_context, result);
}

} // sql
} // oceanbase