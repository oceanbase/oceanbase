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
#include "ob_geo_simplify_visitor.h"
#include "lib/ob_errno.h"
#include "lib/container/ob_se_array.h"
#include "lib/geo/ob_geo_utils.h"

namespace oceanbase
{
namespace common
{
bool ObGeoSimplifyVisitor::prepare(ObGeometry *geo)
{
  bool bret = true;
  if (OB_ISNULL(geo) || !geo->is_tree()) {
    bret = false;
  }
  return bret;
}

int ObGeoSimplifyVisitor::visit(ObCartesianPoint *geo)
{
  // do not simplify point
  return OB_SUCCESS;
}

template<typename LINE>
void ObGeoSimplifyVisitor::find_split_point(
    LINE &line, int32_t l, int32_t r, double max_distance, int32_t &split_idx)
{
  split_idx = l;
  if ((r - l) >= 2) {
    ObWkbGeomInnerPoint &first_pt = line[l];
    ObWkbGeomInnerPoint &last_pt = line[r];
    double ab_distance = ObGeoTypeUtil::distance_point_squre(first_pt, last_pt);
    if (ab_distance < DBL_EPSILON) {
      // p1 == p2, only calculate distance between first_pr and other point
      for (int32_t i = l + 1; i < r; ++i) {
        double distance = ObGeoTypeUtil::distance_point_squre(first_pt, line[i]);
        if (distance > max_distance) {
          max_distance = distance;
          split_idx = i;
        }
      }
    } else {
      max_distance *= ab_distance;
      double ba_x = last_pt.get<0>() - first_pt.get<0>();
      double ba_y = last_pt.get<1>() - first_pt.get<1>();
      double distance = 0.0;
      for (int32_t i = l + 1; i < r; ++i) {
        ObWkbGeomInnerPoint &cur_pt = line[i];
        double ca_x = cur_pt.get<0>() - first_pt.get<0>();
        double ca_y = cur_pt.get<1>() - first_pt.get<1>();
        double dot_ac_ab = ca_x * ba_x + ca_y * ba_y;
        if (dot_ac_ab <= 0.0) {
          distance = ObGeoTypeUtil::distance_point_squre(first_pt, cur_pt) * ab_distance;
        } else if (dot_ac_ab >= ab_distance) {
          distance = ObGeoTypeUtil::distance_point_squre(last_pt, cur_pt) * ab_distance;
        } else {
          double s = ca_x * ba_y - ca_y * ba_x;
          distance = s * s;
        }

        if (distance > max_distance) {
          max_distance = distance;
          split_idx = i;
        }
      }
    }
  }
}

template<typename LINE>
void ObGeoSimplifyVisitor::quick_simplify(LINE &line, ObArray<bool> &kept_idx, int32_t &kept_sz,
    int32_t min_point, int32_t l, int32_t r, double tolerance)
{
  if (l < r) {
    double cur_tol = kept_sz >= min_point ? tolerance : -1.0;
    int32_t split_idx = l;
    find_split_point(line, l, r, cur_tol, split_idx);
    if (split_idx != l) {
      kept_idx[split_idx] = true;
      ++kept_sz;
      quick_simplify(line, kept_idx, kept_sz, min_point, l, split_idx, tolerance);
      quick_simplify(line, kept_idx, kept_sz, min_point, split_idx, r, tolerance);
    }
  }
}

template<typename LINE>
int ObGeoSimplifyVisitor::multi_point_visit(LINE &geo, int32_t min_point)
{
  int ret = OB_SUCCESS;
  int32_t sz = geo.size();
  if (sz < 3 || sz <= min_point) {
    // do not simplify
  } else if (tolerance_ == 0 && min_point <= 2) {
    // O(n) simple version with tolerance 0
    int32_t kept_idx = 0;  // keep first point
    int32_t last_idx = sz - 1;
    ObWkbGeomInnerPoint *kept_pt = &geo[kept_idx];
    for (int32_t i = 1; i < last_idx; ++i) {
      ObWkbGeomInnerPoint *cur_pt = &geo[i];
      ObWkbGeomInnerPoint *next_pt = &geo[i + 1];
      // if straight geo has point kept_pt(a) -> cur_pt(c) -> next_pt(b)
      // then remove cur_pt(c), simplify as kept_pt(a) -> next_pt(b)
      double ba_x = next_pt->get<0>() - kept_pt->get<0>();
      double ba_y = next_pt->get<1>() - kept_pt->get<1>();
      double ba_length = ba_x * ba_x + ba_y * ba_y;

      double ca_x = cur_pt->get<0>() - kept_pt->get<0>();
      double ca_y = cur_pt->get<1>() - kept_pt->get<1>();
      double ca_ba = ca_x * ba_x + ca_y * ba_y;
      double diff = ca_x * ba_y - ca_y * ba_x;
      if (ca_ba < 0 || ca_ba > ba_length || diff != 0) {
        ++kept_idx;
        kept_pt = cur_pt;
        if (kept_idx != i) {
          geo[kept_idx] = *cur_pt;
        }
      }
    }
    ++kept_idx;
    if (kept_idx != last_idx) {
      // keep last point
      geo[kept_idx] = geo[last_idx];
      geo.resize(kept_idx + 1);
    }
  } else {
    ObArray<bool> kept_idx;
    if (OB_FAIL(kept_idx.prepare_allocate(sz))) {
      LOG_WARN("fail to alloc memory", K(ret), K(sz));
    } else {
      // must keep first and last point (kept_idx default false)
      kept_idx[0] = true;
      kept_idx[sz - 1] = true;
      int32_t kept_sz = 2;

      // similar quick sort
      int32_t left = 0;
      int32_t right = sz - 1;
      quick_simplify(geo, kept_idx, kept_sz, min_point, left, right, tolerance_ * tolerance_);
      int32_t valid_idx = 0;
      int32_t last_idx = sz - 1;
      for (int32_t i = 1; i < last_idx; ++i) {
        if (kept_idx[i]) {
          ++valid_idx;
          if (valid_idx != i) {
            geo[valid_idx] = geo[i];
          }
        }
      }
      if (valid_idx != (last_idx - 1)) {
        geo[++valid_idx] = geo[last_idx];
        geo.resize(valid_idx + 1);
      }
    }
  }
  return ret;
}

int ObGeoSimplifyVisitor::visit(ObCartesianLineString *geo)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(multi_point_visit(*geo, LINESTRING_MIN_POINT))) {
    LOG_WARN("fail to do multi point visit", K(ret));
  } else if (geo->size() == 1) {
    if (keep_collapsed_) {
      if (OB_FAIL(geo->push_back(geo->front()))) {
        LOG_WARN("fail to push back geometry", K(ret), K(geo->size()));
      }
    } else {
      geo->clear();
    }
  } else if (geo->size() == 2 && !keep_collapsed_ && (*geo)[0].equals((*geo)[1])) {
    geo->clear();
  }
  return ret;
}

template<typename POLYGON, typename RING>
int ObGeoSimplifyVisitor::polygon_visit(POLYGON &geo)
{
  int ret = OB_SUCCESS;
  if (geo.size() > 0) {
    RING &ext_ring = geo.exterior_ring();
    uint32_t min_point = keep_collapsed_ ? RING_MIN_POINT : 0;
    if (OB_FAIL(multi_point_visit(ext_ring, min_point))) {
      LOG_WARN("fail to do multi point visit", K(ret));
    } else if (ext_ring.size() < RING_MIN_POINT) {
      // if ext ring is invalid, then total polygon is invalid
      ext_ring.clear();
      geo.interior_rings().clear();
    } else if (geo.inner_ring_size() > 0) {
      uint64_t inner_sz = geo.inner_ring_size();
      int32_t valid_inner_ring = 0;
      for (uint32_t i = 0; OB_SUCC(ret) && i < inner_sz; ++i) {
        RING &inner_ring = geo.inner_ring(i);
        if (OB_FAIL(multi_point_visit(inner_ring, min_point))) {
          LOG_WARN("fail to do multi point visit", K(ret));
        } else if (inner_ring.size() >= RING_MIN_POINT) {
          if (valid_inner_ring != i) {
            geo.interior_rings()[valid_inner_ring] = inner_ring;
          }
          ++valid_inner_ring;
        }
      }
      geo.interior_rings().resize(valid_inner_ring);
    }
  }
  return ret;
}

int ObGeoSimplifyVisitor::visit(ObCartesianPolygon *geo)
{
  return polygon_visit<ObCartesianPolygon, ObCartesianLinearring>(*geo);
}

int ObGeoSimplifyVisitor::visit(ObCartesianMultipoint *geo)
{
  // do not simplify
  return OB_SUCCESS;
}

int ObGeoSimplifyVisitor::visit(ObCartesianMultilinestring *geo)
{
  int ret = OB_SUCCESS;
  int32_t valid_line = 0;
  ObCartesianMultilinestring &line = *geo;
  uint64_t sz = line.size();
  for (int32_t i = 0; i < sz && OB_SUCC(ret); ++i) {
    if (OB_FAIL(multi_point_visit(line[i], LINESTRING_MIN_POINT))) {
      LOG_WARN("fail to do multi point visit", K(ret), K(i), K(sz));
    } else if (line[i].size() != 0) {
      if (valid_line != i) {
        line[valid_line] = line[i];
      }
      ++valid_line;
    }
  }
  if (valid_line) {
    line.resize(valid_line);
  } else {
    line.clear();
  }
  return ret;
}

int ObGeoSimplifyVisitor::visit(ObCartesianMultipolygon *geo)
{
  int ret = OB_SUCCESS;
  int32_t valid_poly = 0;
  ObCartesianMultipolygon &poly = *geo;
  uint64_t sz = poly.size();
  for (int32_t i = 0; i < sz && OB_SUCC(ret); ++i) {
    if (OB_FAIL((polygon_visit<ObCartesianPolygon, ObCartesianLinearring>(poly[i])))) {
      LOG_WARN("fail to do polygon visit", K(ret));
    } else if (poly[i].size() != 0) {
      if (valid_poly != i) {
        poly[valid_poly] = poly[i];
      }
      ++valid_poly;
    }
  }
  if (valid_poly) {
    poly.resize(valid_poly);
  } else {
    poly.clear();
  }
  return ret;
}

int ObGeoSimplifyVisitor::visit(ObCartesianGeometrycollection *geo)
{
  int ret = OB_SUCCESS;
  int32_t valid_geo = 0;
  for (int32_t i = 0; i < geo->size() && OB_SUCC(ret); i++) {
    if (OB_FAIL((*geo)[i].do_visit(*this))) {
      LOG_WARN("failed to do tree item visit", K(ret));
    } else if (!(*geo)[i].is_empty()) {
      if (valid_geo != i) {
        (*geo)[valid_geo] = (*geo)[i];
      }
      ++valid_geo;
    }
  }
  if (valid_geo) {
    geo->resize(valid_geo);
  } else {
    geo->clear();
  }
  return ret;
}

int ObGeoSimplifyVisitor::finish(ObGeometry *geo)
{
  UNUSED(geo);
  return OB_SUCCESS;
}

}  // namespace common
}  // namespace oceanbase