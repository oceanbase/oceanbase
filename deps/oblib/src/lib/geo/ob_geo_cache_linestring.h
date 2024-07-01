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

#ifndef OCEANBASE_LIB_GEO_OB_CACHE_LINESTRING_
#define OCEANBASE_LIB_GEO_OB_CACHE_LINESTRING_

#include "lib/geo/ob_geo_cache.h"
#include "lib/geo/ob_geo_rstar_tree.h"
#include "lib/geo/ob_geo_segment_intersect_analyzer.h"

namespace oceanbase {
namespace common {
class ObLineSegments;

class ObCachedGeoLinestring : public ObCachedGeomBase {
public:
  ObCachedGeoLinestring(ObGeometry *geom, ObIAllocator &allocator, const ObSrsItem *srs)
    : ObCachedGeomBase(geom, allocator, srs),
      rtree_(this),
      lAnalyzer_(nullptr),
      line_segments_(page_allocator_, point_mode_arena_),
      input_vertexes_(&point_mode_arena_, common::ObModIds::OB_MODULE_PAGE_ALLOCATOR) {}
  virtual ~ObCachedGeoLinestring() {};
  // get segments from origin_geo_
  virtual int init();
  virtual ObGeoCacheType get_cache_type() { return ObGeoCacheType::GEO_LINESTRING_CACHE;}
  virtual int intersects(ObGeometry& geo, ObGeoEvalCtx& gis_context, bool &res) override;
  virtual ObLineSegments* get_line_segments() { return &line_segments_; }
  virtual void destroy_cache() { this->~ObCachedGeoLinestring(); }
private:
  ObRstarTree<ObLineSegment> rtree_;
  ObLineIntersectionAnalyzer *lAnalyzer_;
  ObLineSegments line_segments_;
  ObVertexes input_vertexes_;
};

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_LIB_GEO_OB_CACHE_LINESTRING_