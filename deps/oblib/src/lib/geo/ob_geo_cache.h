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

#ifndef OCEANBASE_LIB_GEO_OB_CACHE_
#define OCEANBASE_LIB_GEO_OB_CACHE_

#include "lib/string/ob_string.h"
#include "lib/geo/ob_geo_common.h"
#include "lib/geo/ob_geo_utils.h"

namespace oceanbase {
namespace common {
class ObGeoEvalCtx;
enum ObGeoCacheType
{
  GEO_BASE_CACHE = 0,
  GEO_POINT_CACHE = 1,
  GEO_LINESTRING_CACHE = 2,
  GEO_POLYGON_CACHE = 3,
};

// only 2d geometry is supported
// interface
class ObCachedGeom {
public:
    // constructor
    virtual ~ObCachedGeom() {};
    virtual int intersects(ObGeometry& geo, ObGeoEvalCtx& gis_context, bool &res) = 0;
    virtual int contains(ObGeometry& geo, ObGeoEvalCtx& gis_context, bool &res) = 0;
    virtual int cover(ObGeometry& geo, ObGeoEvalCtx& gis_context, bool &res) = 0;
    virtual int within(ObGeometry& geo, ObGeoEvalCtx& gis_context, bool &res) = 0;
    virtual bool is_inited() = 0;
    virtual double get_x_min() = 0;
    virtual double get_x_max() = 0;
    virtual ObGeoCacheType get_cache_type() = 0;
    virtual ObGeometry* get_cached_geom() = 0;
    virtual void set_cached_geom(ObGeometry* geo) = 0;
    virtual ObVertexes& get_vertexes() = 0;
    virtual ObLineSegments* get_line_segments() = 0;
    virtual ObSegments* get_segments() = 0;
    virtual void destroy_cache() = 0;
};

class ObCachedGeomBase : public ObCachedGeom {
public:
  ObCachedGeomBase(ObGeometry *geom, ObIAllocator &allocator, const ObSrsItem *srs)
    : origin_geo_(geom),
      allocator_(&allocator),
      page_allocator_(allocator, common::ObModIds::OB_MODULE_PAGE_ALLOCATOR),
      point_mode_arena_(DEFAULT_PAGE_SIZE_GEO, page_allocator_),
      vertexes_(&point_mode_arena_, common::ObModIds::OB_MODULE_PAGE_ALLOCATOR),
      srs_(srs),
      x_min_(NAN), x_max_(NAN), is_inited_(false) {}
  virtual ~ObCachedGeomBase() {};
  // get vertex from origin_geo_
  virtual int init();
  // use boost::geometry functor
  virtual int intersects(ObGeometry& geo, ObGeoEvalCtx& gis_context, bool &res) override;
  virtual int contains(ObGeometry& geo, ObGeoEvalCtx& gis_context, bool &res) override;
  virtual int cover(ObGeometry& geo, ObGeoEvalCtx& gis_context, bool &res) override;
  virtual int within(ObGeometry& geo, ObGeoEvalCtx& gis_context, bool &res) override;
  virtual void destroy_cache() {this->~ObCachedGeomBase();}
  virtual ObGeometry* get_cached_geom() { return origin_geo_;}
  virtual ObGeoCacheType get_cache_type() { return ObGeoCacheType::GEO_BASE_CACHE;}
  virtual void set_cached_geom(ObGeometry* geo) { origin_geo_ =  geo; }
  virtual ObVertexes& get_vertexes() { return vertexes_; }
  virtual ObLineSegments* get_line_segments() { return nullptr;}
  virtual ObSegments* get_segments() { return nullptr; }
  virtual inline double get_x_min() { return x_min_; }
  virtual inline double get_x_max() { return x_max_; }
  virtual inline ObIAllocator *get_allocator() { return allocator_; }
  virtual inline bool is_inited() { return is_inited_; }
  int check_any_vertexes_in_geo(ObGeometry& geo, bool &res);
protected:
  ObGeometry *origin_geo_;
  ObIAllocator *allocator_;
  ModulePageAllocator page_allocator_;
  ObCachePointModuleArena point_mode_arena_;
  ObVertexes vertexes_;
  const ObSrsItem *srs_;
  double x_min_;
  double x_max_;
  bool is_inited_;
};

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_LIB_GEO_OB_CACHE_
