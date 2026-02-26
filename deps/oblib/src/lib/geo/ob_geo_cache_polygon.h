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

#ifndef OCEANBASE_LIB_GEO_OB_CACHE_POLYGON_
#define OCEANBASE_LIB_GEO_OB_CACHE_POLYGON_

#include "lib/geo/ob_geo_cache.h"
#include "lib/geo/ob_geo_rstar_tree.h"

namespace oceanbase {
namespace common {

class ObLineIntersectionAnalyzer;
class ObPointLocationAnalyzer;
typedef ObRstarTree<ObSegment> ObSegRtree;
typedef PageArena<ObSegRtree*, ModulePageAllocator> ObRtreeVecArena;
typedef PageArena<int, ModulePageAllocator> ObIntArena;
static const int64_t COUNT_DEFAULT_PAGE_SIZE = (1LL << 10); // 1k
class ObRingsRtree {
public:
  ObRingsRtree() {}
  ObRingsRtree(ModulePageAllocator& page_allocator,  ObCacheSegModuleArena& seg_mode_arena) :
  rtrees_arena_(DEFAULT_PAGE_SIZE_GEO, page_allocator),
  int_arena_(COUNT_DEFAULT_PAGE_SIZE, page_allocator),
  rtrees_(&rtrees_arena_, common::ObModIds::OB_MODULE_PAGE_ALLOCATOR),
  ring_count_(&int_arena_, common::ObModIds::OB_MODULE_PAGE_ALLOCATOR),
  ring_segments_(&seg_mode_arena, common::ObModIds::OB_MODULE_PAGE_ALLOCATOR),
  poly_count_(0),
  inited_(false) {}
  ~ObRingsRtree() {
    for (int i = 0; i < rtrees_.size(); ++i) {
      if (OB_NOT_NULL(rtrees_[i])) {
        rtrees_[i]->~ObRstarTree();
      }
    }
  }
  int get_ring_strat_idx(int p_idx, int &start, int& end);
  ObRtreeVecArena rtrees_arena_;
  ObIntArena int_arena_;
  ObVector<ObSegRtree*, ObRtreeVecArena> rtrees_;
  ObVector<int, ObIntArena> ring_count_;
  ObSegments ring_segments_;
  int poly_count_;
  bool inited_;
};

class ObCachedGeoPolygon : public ObCachedGeomBase {
public:
  ObCachedGeoPolygon(ObGeometry *geom, ObIAllocator& allocator, const ObSrsItem *srs)
    : ObCachedGeomBase(geom, allocator, srs),
      rtree_(this),
      seg_rtree_(this),
      lAnalyzer_(nullptr),
      pAnalyzer_(nullptr),
      seg_mode_arena_(DEFAULT_PAGE_SIZE_GEO, page_allocator_),
      segments_(&seg_mode_arena_, common::ObModIds::OB_MODULE_PAGE_ALLOCATOR),
      line_segments_(page_allocator_, point_mode_arena_),
      input_vertexes_(&point_mode_arena_, common::ObModIds::OB_MODULE_PAGE_ALLOCATOR),
      rings_rtree_(page_allocator_, seg_mode_arena_),
      check_valid_(false),
      is_valid_(false) {}
  virtual ~ObCachedGeoPolygon() {};
  // get segments from origin_geo_
  virtual int init();
  virtual ObGeoCacheType get_cache_type() { return ObGeoCacheType::GEO_POLYGON_CACHE;}
  virtual int intersects(ObGeometry& geo, ObGeoEvalCtx& gis_context, bool &res) override;
  virtual int contains(ObGeometry& geo, ObGeoEvalCtx& gis_context, bool &res) override;
  virtual int cover(ObGeometry& geo, ObGeoEvalCtx& gis_context, bool &res) override;
  virtual ObLineSegments* get_line_segments() { return &line_segments_; }
  virtual ObSegments* get_segments() { return &segments_; }
  virtual void destroy_cache() {this->~ObCachedGeoPolygon();}
private:
  int get_farthest_point_position(ObVertexes& vertexes, ObPointLocation& farthest_position, bool& has_point_internal);
  int init_line_analyzer();
  int init_point_analyzer();
  int init_rings_rtree();
  int inner_eval_contains(ObGeometry& geo, ObGeoEvalCtx& gis_context, bool &res, bool eval_contains = true);
  int inner_eval_intersects(ObGeometry& geo, ObGeoEvalCtx& gis_context, bool &res);
  int eval_point_intersects(ObGeometry& geo, bool &res);
  int eval_point_contains(ObGeometry& geo, bool &res, bool is_cover = false);
  int get_point_position_in_polygon(int p_idx, const ObPoint2d &test_point, ObPointLocation &pos);
  bool has_inner_rings();
  template<typename T_IBIN, typename T_BIN, typename T_IITEM, typename T_ITEM>
  bool multi_polygons_has_inner_rings();
  template<typename T_IBIN, typename T_BIN, typename T_IRING, typename T_RING, typename T_INNER_RING>
  int polygon_init_rings_rtree(T_IBIN *geo);
  template<typename T_IBIN, typename T_BIN, typename T_IITEM, typename T_ITEM, typename T_IRING, typename T_RING, typename T_INNER_RING>
  int multipolygon_init_rings_rtree();
  int alloc_rtree(ObSegRtree*& rtree_ptr);
  int check_valid(ObGeoEvalCtx& gis_context);
private:
  ObRstarTree<ObLineSegment> rtree_;
  ObRstarTree<ObSegment> seg_rtree_;
  ObLineIntersectionAnalyzer *lAnalyzer_;
  ObPointLocationAnalyzer *pAnalyzer_;
  ObCacheSegModuleArena seg_mode_arena_;
  ObSegments segments_;
  ObLineSegments line_segments_;
  ObVertexes input_vertexes_;
  ObRingsRtree rings_rtree_;
  bool check_valid_;
  bool is_valid_;
};

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_LIB_GEO_OB_CACHE_POLYGON_
