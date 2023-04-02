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

#ifndef OCEANBASE_LIB_GEO_OB_GEO_TO_TREE_VISITOR_
#define OCEANBASE_LIB_GEO_OB_GEO_TO_TREE_VISITOR_
#include "lib/geo/ob_geo_visitor.h"

namespace oceanbase
{
namespace common
{

class ObGeoToTreeVisitor : public ObEmptyGeoVisitor
{
public:
  ObGeoToTreeVisitor(ObIAllocator *allocator)
    : allocator_(allocator), root_(NULL),
      page_allocator_(*allocator, common::ObModIds::OB_MODULE_PAGE_ALLOCATOR),
      mode_arena_(DEFAULT_PAGE_SIZE_GEO, page_allocator_),
      parent_(&mode_arena_, common::ObModIds::OB_MODULE_PAGE_ALLOCATOR) {}
  virtual ~ObGeoToTreeVisitor() { parent_.clear(); }
  ObGeometry *get_geometry() const { return root_; }
  template<typename T>
  int alloc_geo_tree_obj(T *&obj);
  template<typename T, typename T_IBIN>
  int create_geo_tree_collection(T_IBIN *i_geo);
  template<typename T_POINT, typename T_TREE, typename T_IBIN, typename T_BIN>
  int create_geo_multi_point(T_TREE *&geo, T_IBIN *geo_ibin);
  template<typename T, typename T_BIN>
  int point_visit(T_BIN *geo);

  template<typename P_TYPE, typename P_BIN_TYPE, typename L_TYPE, typename L_BIN_TYPE,
           typename POINT_TYPE, typename RINGS_TYPE, typename p_ibin_type>
  int polygon_visit(p_ibin_type *geo);

  template<typename T>
  int update_root_and_parent(T *geo);

  bool prepare(ObGeometry *geo) override { UNUSED(geo); return true; }
  // wkb
  int visit(ObIWkbGeogPoint *geo) override;
  int visit(ObIWkbGeomPoint *geo) override;
  int visit(ObIWkbGeogLineString *geo) override;
  int visit(ObIWkbGeomLineString *geo) override;
  int visit(ObIWkbGeogMultiPoint *geo) override;
  int visit(ObIWkbGeomMultiPoint *geo) override;
  int visit(ObIWkbGeogMultiLineString *geo) override;
  int visit(ObIWkbGeomMultiLineString *geo) override;
  int visit(ObIWkbGeogPolygon *geo) override;
  int visit(ObIWkbGeomPolygon *geo) override;
  int visit(ObIWkbGeogMultiPolygon *geo) override;
  int visit(ObIWkbGeomMultiPolygon *geo) override;
  int visit(ObIWkbGeogCollection *geo) override;
  int visit(ObIWkbGeomCollection *geo) override;

  bool is_end(ObIWkbGeogLineString *geo) override { UNUSED(geo); return true; }
  bool is_end(ObIWkbGeomLineString *geo) override { UNUSED(geo); return true; }
  bool is_end(ObIWkbGeogMultiPoint *geo) override { UNUSED(geo); return true; }
  bool is_end(ObIWkbGeomMultiPoint *geo) override { UNUSED(geo); return true; }
  bool is_end(ObIWkbGeogLinearRing *geo) override { UNUSED(geo); return true; }
  bool is_end(ObIWkbGeomLinearRing *geo) override { UNUSED(geo); return true; }
  bool is_end(ObIWkbGeogPolygon *geo) override { UNUSED(geo); return true; }
  bool is_end(ObIWkbGeomPolygon *geo) override { UNUSED(geo); return true; }

  int finish(ObIWkbGeogMultiLineString *geo) override { UNUSED(geo); return parent_.remove(parent_.last()); }
  int finish(ObIWkbGeomMultiLineString *geo) override { UNUSED(geo); return parent_.remove(parent_.last()); }
  int finish(ObIWkbGeogMultiPolygon *geo) override { UNUSED(geo); return parent_.remove(parent_.last()); }
  int finish(ObIWkbGeomMultiPolygon *geo) override { UNUSED(geo); return parent_.remove(parent_.last()); }
  int finish(ObIWkbGeogCollection *geo) override { UNUSED(geo); return parent_.remove(parent_.last()); }
  int finish(ObIWkbGeomCollection *geo) override { UNUSED(geo); return parent_.remove(parent_.last()); }


private:
  typedef PageArena<ObGeometrycollection *, ModulePageAllocator> ObCGeoModuleArena;
  ObIAllocator *allocator_;
  ObGeometry *root_;
  ModulePageAllocator page_allocator_;
  ObCGeoModuleArena mode_arena_;
  ObVector<ObGeometrycollection *, ObCGeoModuleArena> parent_;
  DISALLOW_COPY_AND_ASSIGN(ObGeoToTreeVisitor);
};

} // namespace common
} // namespace oceanbase

#endif