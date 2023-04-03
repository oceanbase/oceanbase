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
#include "ob_geo_to_tree_visitor.h"

namespace oceanbase {
namespace common {

template<typename T>
int ObGeoToTreeVisitor::alloc_geo_tree_obj(T *&obj)
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

template<typename T>
int ObGeoToTreeVisitor::update_root_and_parent(T *geo)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(root_)) {
    root_ = geo;
  }
  uint32_t coll_num = parent_.size();
  if (coll_num > 0) {
    ObGeometrycollection *cur = parent_[coll_num - 1];
    if (OB_FAIL(cur->push_back(*static_cast<ObGeometry *>(geo)))) {
      LOG_WARN("failed to push geo to parent", K(ret));
    }
  }
  return ret;
}

template<typename T, typename T_IBIN>
int ObGeoToTreeVisitor::create_geo_tree_collection(T_IBIN *i_geo)
{
  int ret = OB_SUCCESS;
  T* geo = NULL;
  if (OB_FAIL(alloc_geo_tree_obj(geo))) {
    LOG_WARN("failed to alloc tree geog geo string", K(ret));
  } else {
    geo = new(geo)T(i_geo->get_srid(), *allocator_);
    if (OB_FAIL(update_root_and_parent(geo))) {
      LOG_WARN("failed to update parent", K(ret));
    } else if (OB_FAIL(parent_.push_back(geo))) {
      LOG_WARN("failed to set self to parent", K(ret));
    }
  }
  return ret;
}

template<typename T_POINT, typename T_TREE, typename T_IBIN, typename T_BIN>
int ObGeoToTreeVisitor::create_geo_multi_point(T_TREE *&geo, T_IBIN *geo_ibin)
{
  int ret = OB_SUCCESS;
  const T_BIN *geo_bin = reinterpret_cast<const T_BIN *>(geo_ibin->val());
  if (OB_ISNULL(geo_bin)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("geo value is null", K(ret));
  } else {
    geo = new(geo)T_TREE(geo_ibin->get_srid(), *allocator_);
    typename T_BIN::iterator iter = geo_bin->begin();
    for ( ; iter != geo_bin->end() && OB_SUCC(ret); iter++) {
      T_POINT p(iter->template get<0>(), iter->template get<1>());
      if (OB_FAIL(geo->push_back(p))) {
        LOG_WARN("failed to add point", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(update_root_and_parent(geo))) {
      LOG_WARN("failed to update parent", K(ret));
    }
  }
  return ret;
}

template<typename T, typename T_IBIN>
int ObGeoToTreeVisitor::point_visit(T_IBIN *geo)
{
  int ret = OB_SUCCESS;
  T *point = NULL;
  if (OB_FAIL(alloc_geo_tree_obj(point))) {
    LOG_WARN("failed to alloc tree Point", K(ret));
  } else {
    point = new(point)T(geo->get_srid(), allocator_);
    point->x(geo->x());
    point->y(geo->y());
    if (OB_FAIL(update_root_and_parent(point))) {
      LOG_WARN("failed to update parent", K(ret));
    }
  }
  return ret;
}

int ObGeoToTreeVisitor::visit(ObIWkbGeogPoint *geo)
{
  return point_visit<ObGeographPoint>(geo);
}

int ObGeoToTreeVisitor::visit(ObIWkbGeomPoint *geo)
{
  return point_visit<ObCartesianPoint>(geo);
}

int ObGeoToTreeVisitor::visit(ObIWkbGeogMultiPoint *geo)
{
  int ret = OB_SUCCESS;
  ObGeographMultipoint *multi_point = NULL;
  if (OB_FAIL(alloc_geo_tree_obj(multi_point))) {
    LOG_WARN("failed to alloc tree geog multi_point", K(ret));
  } else if (OB_FAIL((create_geo_multi_point<ObWkbGeogInnerPoint, ObGeographMultipoint,
    ObIWkbGeogMultiPoint, ObWkbGeogMultiPoint>(multi_point, geo)))) {
    LOG_WARN("failed to create tree geog multi_point", K(ret));
  }
  return ret;
}

int ObGeoToTreeVisitor::visit(ObIWkbGeomMultiPoint *geo)
{
  int ret = OB_SUCCESS;
  ObCartesianMultipoint *multi_point = NULL;
  if (OB_FAIL(alloc_geo_tree_obj(multi_point))) {
    LOG_WARN("failed to alloc tree cartesian multi_point", K(ret));
  } else if (OB_FAIL((create_geo_multi_point<ObWkbGeomInnerPoint, ObCartesianMultipoint,
    ObIWkbGeomMultiPoint, ObWkbGeomMultiPoint>(multi_point, geo)))) {
    LOG_WARN("failed to create tree geom multi_point", K(ret));
  }
  return ret;
}

int ObGeoToTreeVisitor::visit(ObIWkbGeogMultiPolygon *geo)
{
  return create_geo_tree_collection<ObGeographMultipolygon>(geo);
}

int ObGeoToTreeVisitor::visit(ObIWkbGeomMultiPolygon *geo)
{
  return create_geo_tree_collection<ObCartesianMultipolygon>(geo);
}

int ObGeoToTreeVisitor::visit(ObIWkbGeogMultiLineString *geo)
{
  return create_geo_tree_collection<ObGeographMultilinestring>(geo);
}

int ObGeoToTreeVisitor::visit(ObIWkbGeomMultiLineString *geo)
{
  return create_geo_tree_collection<ObCartesianMultilinestring>(geo);
}

template<typename P_TYPE, typename P_BIN_TYPE, typename L_TYPE, typename L_BIN_TYPE,
         typename POINT_TYPE, typename RINGS_TYPE, typename p_ibin_type>
int ObGeoToTreeVisitor::polygon_visit(p_ibin_type *geo)
{
  int ret = OB_SUCCESS;
  P_TYPE *polygon = NULL;
  const P_BIN_TYPE *polygon_bin = reinterpret_cast<const P_BIN_TYPE*>(geo->val());
  if (OB_ISNULL(polygon_bin)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("geo value is null", K(ret));
  } else if (OB_FAIL(alloc_geo_tree_obj(polygon))) {
    LOG_WARN("failed to alloc tree polygon", K(ret));
  } else {
    polygon = new(polygon)P_TYPE(geo->get_srid(), *allocator_);
    uint32_t ring_num = polygon_bin->size();
    if (ring_num > 0) {
      // construct and add tree exterior ring
      L_TYPE& tree_ext_ring = polygon->exterior_ring();
      const L_BIN_TYPE &ext_ring = polygon_bin->exterior_ring();
      typename L_BIN_TYPE::iterator iter = ext_ring.begin();
      for ( ; iter != ext_ring.end() && OB_SUCC(ret); iter++) {
        POINT_TYPE p(iter->template get<0>(), iter->template get<1>());
        if (OB_FAIL(tree_ext_ring.push_back(p))) {
          LOG_WARN("failed to push point to ring", K(ret));
        }
      }
      if (OB_SUCC(ret) && ring_num > 1) {
        // construct and add tree inner ring
        const RINGS_TYPE& inner_rings = polygon_bin->inner_rings();
        typename RINGS_TYPE::iterator iter = inner_rings.begin();
        for (; iter != inner_rings.end() && OB_SUCC(ret); iter++) {
          L_TYPE *tree_linearring = NULL;
          if (OB_FAIL(alloc_geo_tree_obj(tree_linearring))) {
            LOG_WARN("failed to alloc tree linearring", K(ret));
          } else {
            tree_linearring = new(tree_linearring)L_TYPE(geo->get_srid(), *allocator_);
            typename L_BIN_TYPE::iterator point_iter = iter->begin();
            for ( ; point_iter != iter->end() && OB_SUCC(ret); point_iter++) {
              POINT_TYPE p(point_iter->template get<0>(), point_iter->template get<1>());
              if (OB_FAIL(tree_linearring->push_back(p))) {
                LOG_WARN("failed to push point to ring", K(ret));
              }
            }
            if (OB_SUCC(ret) && OB_FAIL(polygon->push_back(*tree_linearring))) {
              LOG_WARN("failed to push ring to polygon", K(ret));
            }
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(update_root_and_parent(polygon))) {
      LOG_WARN("failed to update parent", K(ret));
    }
  }
  return ret;
}

int ObGeoToTreeVisitor::visit(ObIWkbGeogPolygon *geo)
{
  return polygon_visit<ObGeographPolygon, ObWkbGeogPolygon, ObGeographLinearring,
    ObWkbGeogLinearRing, ObWkbGeogInnerPoint, ObWkbGeogPolygonInnerRings>(geo);
}

int ObGeoToTreeVisitor::visit(ObIWkbGeomPolygon *geo)
{
  return polygon_visit<ObCartesianPolygon, ObWkbGeomPolygon, ObCartesianLinearring,
    ObWkbGeomLinearRing, ObWkbGeomInnerPoint, ObWkbGeomPolygonInnerRings>(geo);
}

int ObGeoToTreeVisitor::visit(ObIWkbGeogLineString *geo)
{
  int ret = OB_SUCCESS;
  ObGeographLineString *line = NULL;
  if (OB_FAIL(alloc_geo_tree_obj(line))) {
    LOG_WARN("failed to alloc tree geog line string", K(ret));
  } else if (OB_FAIL((create_geo_multi_point<ObWkbGeogInnerPoint, ObGeographLineString,
      ObIWkbGeogLineString, ObWkbGeogLineString>(line, geo)))) {
    LOG_WARN("failed to create tree geog linestring", K(ret));
  }
  return ret;
}

int ObGeoToTreeVisitor::visit(ObIWkbGeomLineString *geo)
{
  int ret = OB_SUCCESS;
  ObCartesianLineString *line = NULL;
  if (OB_FAIL(alloc_geo_tree_obj(line))) {
    LOG_WARN("failed to alloc tree cartesian line string", K(ret));
  } else if (OB_FAIL((create_geo_multi_point<ObWkbGeomInnerPoint, ObCartesianLineString,
    ObIWkbGeomLineString, ObWkbGeomLineString>(line, geo)))) {
    LOG_WARN("failed to create tree geom linestring", K(ret));
  }
  return ret;
}

int ObGeoToTreeVisitor::visit(ObIWkbGeogCollection *geo)
{
  return create_geo_tree_collection<ObGeographGeometrycollection>(geo);
}

int ObGeoToTreeVisitor::visit(ObIWkbGeomCollection *geo)
{
  return create_geo_tree_collection<ObCartesianGeometrycollection>(geo);
}

} // namespace common
} // namespace oceanbase