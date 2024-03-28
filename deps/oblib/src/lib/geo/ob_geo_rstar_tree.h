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

#ifndef OCEANBASE_LIB_GEO_RSTAR_TREE_
#define OCEANBASE_LIB_GEO_RSTAR_TREE_

#include "lib/geo/ob_geo_cache.h"
#include "lib/geo/ob_geo_dispatcher.h"
#include <boost/geometry.hpp>

namespace oceanbase {
namespace common {

namespace bg = boost::geometry;
namespace bgi = boost::geometry::index;
enum QueryRelation {
  INTERSECTS = 0,
};

template <typename T>
class ObRstarTree {
public:
typedef T value_type;
typedef PageArena<value_type, ModulePageAllocator> ObModuleArenaType;
typedef std::pair<ObCartesianBox, value_type *> RtreeNodeValue;
// create the rtree using default constructor
typedef bgi::rtree<RtreeNodeValue, bgi::rstar<16, 8>, bgi::indexable<RtreeNodeValue>,
                  bgi::equal_to<RtreeNodeValue>> RStarTree;

public:
  ObRstarTree(ObCachedGeomBase *cache_geo)
    : cache_geo_(cache_geo),
      rtree_index_(),
      is_built_(false) {}
  virtual ~ObRstarTree() {}
  int construct_rtree_index(ObVector<T, ObModuleArenaType> &elements, int start = 0)
  {
    int ret = OB_SUCCESS;
    ObVector<RtreeNodeValue> rtree_nodes;
    for (uint32_t i = start; i < elements.size() && OB_SUCC(ret); i++) {
      ObCartesianBox box;
      if (OB_FAIL(elements[i].get_box(box))) {
        LOG_WARN("failed to get segment box", K(ret));
      } else if (OB_FAIL(rtree_nodes.push_back(std::make_pair(box, &elements[i])))) {
        LOG_WARN("failed to record rtree node", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else {
      try {
        rtree_index_.~RStarTree();
        new (&rtree_index_) RStarTree(rtree_nodes.begin(), rtree_nodes.end());
        is_built_ = true;
      } catch (...) {
        ret = ob_boost_geometry_exception_handle();
      }
    }
    return ret;
  }

  int query(QueryRelation relation, const ObCartesianBox &box, std::vector<RtreeNodeValue> &res)
  {
    int ret = OB_SUCCESS;
    if (!is_built_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rtree isn't built", K(ret));
    } else if (relation == QueryRelation::INTERSECTS) {
      try {
        rtree_index_.query(bgi::intersects(box), std::back_inserter(res));
      } catch (...) {
        ret = ob_boost_geometry_exception_handle();
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("query type isn't supported", K(ret), K(relation));
    }
    return ret;
  }
  inline bool is_built() { return is_built_; }
private:
  ObVector<RtreeNodeValue> rtree_nodes_;
  ObCachedGeomBase *cache_geo_;
  RStarTree rtree_index_;
  bool is_built_;
};

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_LIB_GEO_RSTAR_TREE_
