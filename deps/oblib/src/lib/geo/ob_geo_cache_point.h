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

#ifndef OCEANBASE_LIB_GEO_OB_CACHE_POINT_
#define OCEANBASE_LIB_GEO_OB_CACHE_POINT_

#include "lib/geo/ob_geo_cache.h"
#include "lib/geo/ob_geo_rstar_tree.h"

namespace oceanbase {
namespace common {

class ObCachedGeoPoint : public ObCachedGeomBase {
public:
  ObCachedGeoPoint(ObGeometry *geom, ObIAllocator &allocator, const ObSrsItem *srs)
    : ObCachedGeomBase(geom, allocator, srs) {}
  virtual ~ObCachedGeoPoint() {};
  virtual ObGeoCacheType get_cache_type() { return ObGeoCacheType::GEO_POINT_CACHE;}
  virtual void destroy_cache() { this->~ObCachedGeoPoint(); }
  virtual int intersects(ObGeometry& geo, ObGeoEvalCtx& gis_context, bool &res) override;
};

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_LIB_GEO_OB_CACHE_Point_
