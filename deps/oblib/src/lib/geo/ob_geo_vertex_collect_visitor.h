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

#ifndef OCEANBASE_LIB_GEO_OB_GEO_VERTEX_COLLECT_VISITOR_
#define OCEANBASE_LIB_GEO_OB_GEO_VERTEX_COLLECT_VISITOR_
#include "lib/geo/ob_geo_visitor.h"
#include "lib/geo/ob_geo_utils.h"
#include "lib/geo/ob_geo_3d.h"

namespace oceanbase
{
namespace common
{

class ObGeoVertexCollectVisitor : public ObEmptyGeoVisitor
{
public:
  ObGeoVertexCollectVisitor(ObVertexes &vertexes)
    : vertexes_(vertexes), x_min_(NAN), x_max_(NAN), y_min_(NAN), y_max_(NAN) {}
  virtual ~ObGeoVertexCollectVisitor() {}
  bool prepare(ObGeometry *geo);  
  int visit(ObIWkbPoint *geo);
  int visit(ObIWkbGeometry *geo) { UNUSED(geo); return OB_SUCCESS; }
  inline double get_x_min() { return x_min_; }
  inline double get_x_max() { return x_max_; }
  inline double get_y_min() { return y_min_; }
  inline double get_y_max() { return y_max_; }

private:
  ObVertexes &vertexes_;
  double x_min_;
  double x_max_;
  double y_min_;
  double y_max_;
  DISALLOW_COPY_AND_ASSIGN(ObGeoVertexCollectVisitor);
};

typedef struct
{
  double x;
  double y;
  double z;
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    J_KV(K(x), K(y), K(z));
    return pos;
  }
} ObPoint3dVertex;

typedef PageArena<ObPoint3dVertex, ModulePageAllocator> ObCachePoint3dModuleArena;
typedef ObVector<ObPoint3dVertex, ObCachePoint3dModuleArena> ObVertexes3d;

class ObGeo3DVertexCollectVisitor : public ObGeo3DVisitor
{
public:
  ObGeo3DVertexCollectVisitor(ObVertexes3d &vertexes) : vertexes_(vertexes) {}
  virtual ~ObGeo3DVertexCollectVisitor() {}
  
  virtual int visit_pointz_inner(double x, double y, double z) override;
  
  int visit_pointz_start(ObGeometry3D *geo, bool is_inner = false) override { UNUSED(geo); UNUSED(is_inner); return OB_SUCCESS; }
  int visit_pointz_end(ObGeometry3D *geo, bool is_inner = false) override { UNUSED(geo); UNUSED(is_inner); return OB_SUCCESS; }
  int visit_linestringz_start(ObGeometry3D *geo, uint32_t nums, ObLineType line_type) override { UNUSED(geo); UNUSED(nums); UNUSED(line_type); return OB_SUCCESS; }
  int visit_linestringz_end(ObGeometry3D *geo, uint32_t nums, ObLineType line_type) override { UNUSED(geo); UNUSED(nums); UNUSED(line_type); return OB_SUCCESS; }
  int visit_polygonz_start(ObGeometry3D *geo, uint32_t nums) override { UNUSED(geo); UNUSED(nums); return OB_SUCCESS; }
  int visit_polygonz_end(ObGeometry3D *geo, uint32_t nums) override { UNUSED(geo); UNUSED(nums); return OB_SUCCESS; }
  int visit_multi_geom_start(ObGeoType geo_type, ObGeometry3D *geo, uint32_t nums) override { UNUSED(geo_type); UNUSED(geo); UNUSED(nums); return OB_SUCCESS; }
  int visit_multi_geom_end(ObGeoType geo_type, ObGeometry3D *geo, uint32_t nums) override { UNUSED(geo_type); UNUSED(geo); UNUSED(nums); return OB_SUCCESS; }
  int visit_collectionz_start(ObGeometry3D *geo, uint32_t nums) override { UNUSED(geo); UNUSED(nums); return OB_SUCCESS; }
  int visit_collectionz_end(ObGeometry3D *geo, uint32_t nums) override { UNUSED(geo); UNUSED(nums); return OB_SUCCESS; }
  
private:
  ObVertexes3d &vertexes_;
  DISALLOW_COPY_AND_ASSIGN(ObGeo3DVertexCollectVisitor);
};

} // namespace common
} // namespace oceanbase

#endif