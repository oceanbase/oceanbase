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
 */

#define USING_LOG_PREFIX LIB
#include "ob_geo_pointn_visitor.h"

namespace oceanbase {
namespace common {

template<typename T>
int ObGeoPointNVisitor::alloc_point_obj(T *&obj)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("allocator is null", K(ret));
  } else {
    void *buf = allocator_->alloc(sizeof(T));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc geo tree obj");
    } else {
      obj = reinterpret_cast<T *>(buf);
    }
  }
  return ret;
}

template<typename T_POINT, typename T_TREE, typename T_IBIN, typename T_BIN>
int ObGeoPointNVisitor::search_pointn(T_IBIN *geo_ibin)
{
  int ret = OB_SUCCESS;
  const T_BIN *geo_bin = reinterpret_cast<const T_BIN *>(geo_ibin->val());
  if (OB_ISNULL(geo_bin)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("geo value is null", K(ret));
  } else {
    typename T_BIN::iterator iter = geo_bin->begin();
    int index = 1;
    for ( ; iter != geo_bin->end() && OB_SUCC(ret); iter++) {
      if (index == position_) {
        T_POINT* point = NULL;
        if (OB_FAIL(alloc_point_obj(point))) {
          LOG_WARN("failed to alloc point string", K(ret));
        } else {
          point = new(point)T_POINT(iter->template get<0>(), iter->template get<1>(), geo_ibin->get_srid());
          point_ = point;
        }
        break;
      }
      index++;
    }
  }
  return ret;
}

int ObGeoPointNVisitor::visit(ObIWkbGeogLineString *geo)
{
  int ret = OB_SUCCESS;  
  if (OB_FAIL((search_pointn<ObGeographPoint, ObGeographLineString,
      ObIWkbGeogLineString, ObWkbGeogLineString>(geo)))) {
    LOG_WARN("failed to create tree geog linestring", K(ret));
  }
  return ret;
}

int ObGeoPointNVisitor::visit(ObIWkbGeomLineString *geo)
{
  int ret = OB_SUCCESS;  
  if (OB_FAIL((search_pointn<ObCartesianPoint, ObCartesianLineString,
    ObIWkbGeomLineString, ObWkbGeomLineString>(geo)))) {
    LOG_WARN("failed to create tree geom linestring", K(ret));
  }
  return ret;
}

} // namespace common
} // namespace oceanbase