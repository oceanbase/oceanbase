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
#include "lib/ob_errno.h"
#include "ob_geo_interior_point_visitor.h"
#include "ob_srs_info.h"
#include "lib/geo/ob_geo_func_register.h"
#include "lib/geo/ob_geo_utils.h"

namespace oceanbase
{
namespace common
{
int ObGeoInteriorPointVisitor::init(ObGeometry *geo)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObGeoTypeUtil::check_empty(geo, is_geo_empty_))) {
    LOG_WARN("fail to check geometry is empty", K(ret));
  } else {
    ObGeoEvalCtx centroid_context(allocator_);
    ObGeometry *res_geo = nullptr;
    if (OB_FAIL(centroid_context.append_geo_arg(geo))) {
      LOG_WARN("build geo gis context failed", K(ret));
    } else if (OB_FAIL(ObGeoFunc<ObGeoFuncType::Centroid>::geo_func::eval(centroid_context, res_geo))) {
      if (ret == OB_ERR_BOOST_GEOMETRY_CENTROID_EXCEPTION) {
        exist_centroid_ = false;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("eval geo centroid failed", K(ret));
      }
    } else {
      centroid_pt_ = reinterpret_cast<ObCartesianPoint *>(res_geo);
    }
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

// support ObWkbGeomPoint/ObWkbGeomInnerPoint
template<typename PointType>
double ObGeoInteriorPointVisitor::calculate_euclidean_distance(ObCartesianPoint &p1, PointType &p2)
{
  double x_dist = p1.x() - p2.template get<0>();
  double y_dist = p1.y() - p2.template get<1>();
  return sqrt(x_dist * x_dist + y_dist * y_dist);
}

int ObGeoInteriorPointVisitor::assign_interior_point(double x, double y)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(interior_point_)) {
    if (OB_ISNULL(interior_point_ = OB_NEWx(ObCartesianPoint, allocator_, x, y, srid_, allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory for collection", K(ret));
    }
  } else {
    interior_point_->x(x);
    interior_point_->y(y);
  }

  return ret;
}

int ObGeoInteriorPointVisitor::assign_interior_endpoint(double x, double y)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(interior_endpoint_)) {
    if (OB_ISNULL(interior_endpoint_ = OB_NEWx(ObCartesianPoint, allocator_, x, y, srid_, allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory for collection", K(ret));
    }
  } else {
    interior_endpoint_->x(x);
    interior_endpoint_->y(y);
  }

  return ret;
}

int ObGeoInteriorPointVisitor::visit(ObIWkbGeomPoint *geo)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    if (OB_FAIL(init(geo))) {
      LOG_WARN("fail to get centroid point", K(ret));
    }
  }

  if (OB_SUCC(ret) && !is_geo_empty_ && exist_centroid_ && (dimension_ == -1 || dimension_ == 0)) {
    ObWkbGeomPoint *point = reinterpret_cast<ObWkbGeomPoint *>(geo->val());
    double dist = calculate_euclidean_distance(*centroid_pt_, *point);
    if (dist < min_dist_) {
      min_dist_ = dist;
      if (OB_FAIL(assign_interior_point(geo->x(), geo->y()))) {
        LOG_WARN("fail to assign interior point", K(ret), K(geo->x()), K(geo->y()));
      }
    }
  }
  return ret;
}

int ObGeoInteriorPointVisitor::visit(ObIWkbGeomMultiPoint *geo)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    if (OB_FAIL(init(geo))) {
      LOG_WARN("fail to get centroid point", K(ret));
    }
  }

  if (OB_SUCC(ret) && !is_geo_empty_ && exist_centroid_) {
    const ObWkbGeomMultiPoint *line = reinterpret_cast<const ObWkbGeomMultiPoint *>(geo->val());
    ObWkbGeomMultiPoint::iterator iter = line->begin();
    for (; iter != line->end() && OB_SUCC(ret); iter++) {
      double dist = calculate_euclidean_distance(*centroid_pt_, *iter);
      if (dist < min_dist_) {
        min_dist_ = dist;
        if (OB_FAIL(assign_interior_point(iter->get<0>(), iter->get<1>()))) {
          LOG_WARN("fail to assign interior point", K(ret), K(iter->get<0>()), K(iter->get<1>()));
        }
      }
    }
  }
  return ret;
}

int ObGeoInteriorPointVisitor::visit(ObIWkbGeomLineString *geo)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    if (OB_FAIL(init(geo))) {
      LOG_WARN("fail to get centroid point", K(ret));
    }
  }

  if (OB_SUCC(ret) && !is_geo_empty_) {
    const ObWkbGeomLineString *line = reinterpret_cast<const ObWkbGeomLineString *>(geo->val());
    ObWkbGeomLineString::const_iterator iter = line->begin();
    ObWkbGeomLineString::const_iterator iter2 = line->begin();
    if (exist_centroid_) {
      if (line->size() <= 2) {
        if (OB_ISNULL(interior_point_)) {
          // interior point is first point
          double dist = calculate_euclidean_distance(*centroid_pt_, *iter);
          if (dist < min_endpoint_dist_) {
            min_endpoint_dist_ = dist;
            if (OB_FAIL(assign_interior_endpoint(iter->get<0>(), iter->get<1>()))) {
              LOG_WARN("fail to assign interior point", K(ret), K(iter->get<0>()), K(iter->get<1>()));
            }
          }
        }
      } else {
        // skip first and last point
        iter++;
        iter2++;
        iter2++;
        for (; iter2 != line->end() && OB_SUCC(ret); iter++, iter2++) {
          double dist = calculate_euclidean_distance(*centroid_pt_, *iter);
          if (dist < min_dist_) {
            min_dist_ = dist;
            if (OB_FAIL(assign_interior_point(iter->get<0>(), iter->get<1>()))) {
              LOG_WARN("fail to assign interior point", K(ret), K(iter->get<0>()), K(iter->get<1>()));
            }
          }
        }
      }
    } else {
      // only choose first and last point
      // centroid_pt_ default POINT(0 0) when centroid not exist
      double dist = calculate_euclidean_distance(*centroid_pt_, *iter);
      if (dist < min_dist_) {
        min_dist_ = dist;
        if (OB_FAIL(assign_interior_point(iter->get<0>(), iter->get<1>()))) {
          LOG_WARN("fail to assign interior point", K(ret), K(iter->get<0>()), K(iter->get<1>()));
        }
      }
      if (line->size() > 1) {
        ++iter2;
        for (; iter2 != line->end() && OB_SUCC(ret); iter++, iter2++) {
          // find last point
        }
        dist = calculate_euclidean_distance(*centroid_pt_, *iter);
        if (dist < min_dist_) {
          min_dist_ = dist;
          if (OB_FAIL(assign_interior_point(iter->get<0>(), iter->get<1>()))) {
            LOG_WARN("fail to assign interior point", K(ret), K(iter->get<0>()), K(iter->get<1>()));
          }
        }
      }
    }
  }
  return ret;
}

int ObGeoInteriorPointVisitor::visit(ObIWkbGeomMultiLineString *geo)
{
  int ret = OB_SUCCESS;
  // unused
  if (!is_inited_) {
    if (OB_FAIL(init(geo))) {
      LOG_WARN("fail to get centroid point", K(ret));
    }
  }

  return ret;
}

int ObGeoInteriorPointVisitor::inner_calculate_interior_y(
    const ObWkbGeomLinearRing &ring, double centre_y, double &ymax, double &ymin)
{
  int ret = OB_SUCCESS;
  ObWkbGeomLinearRing::iterator iter = ring.begin();
  for (; iter != ring.end() && OB_SUCC(ret); iter++) {
    double y = iter->get<1>();
    if ((y > ymin) && (y <= centre_y)) {
      ymin = y;
    } else if ((y > centre_y) && (y < ymax)) {
      ymax = y;
    }
  }

  return ret;
}

int ObGeoInteriorPointVisitor::calculate_interior_y(ObIWkbGeomPolygon *geo, double &interior_y)
{
  int ret = OB_SUCCESS;
  ObGeoEvalCtx box_ctx(allocator_);
  ObGeogBox *gbox = nullptr;
  const ObWkbGeomPolygon *polygon = reinterpret_cast<const ObWkbGeomPolygon *>(geo->val());
  ObWkbGeomLinearRing::iterator iter = polygon->exterior_ring().begin();
  double ymin = iter->get<1>();
  double ymax = iter->get<1>();
  for (iter++; iter != polygon->exterior_ring().end(); iter++) {
    double y = iter->get<1>();
    if (y < ymin) {
      ymin = y;
    }
    if (y > ymax) {
      ymax = y;
    }
  }
  double centre_y = ymin + (ymax - ymin) / 2;
  if (OB_FAIL(inner_calculate_interior_y(polygon->exterior_ring(), centre_y, ymax, ymin))) {
    LOG_WARN("failed to do geom polygon exterior ring visit", K(ret));
  } else {
    const ObWkbGeomPolygonInnerRings &rings = polygon->inner_rings();
    ObWkbGeomPolygonInnerRings::const_iterator iter = rings.begin();
    for (; iter != rings.end() && OB_SUCC(ret); iter++) {
      if (OB_FAIL(inner_calculate_interior_y(*iter, centre_y, ymax, ymin))) {
        LOG_WARN("failed to do geom polygon inner ring visit", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    interior_y = ymin + (ymax - ymin) / 2;
  }
  return ret;
}

bool ObGeoInteriorPointVisitor::is_crossing_line(double start_y, double end_y, double interior_y)
{
  bool bret = true;
  if (start_y == end_y) {
    bret = false;  // horizontal line
  } else if ((start_y > interior_y) && (end_y > interior_y)) {
    bret = false;  // above line
  } else if ((start_y < interior_y) && (end_y < interior_y)) {
    bret = false;  // below line
  } else if ((start_y == interior_y) && (end_y < interior_y)) {
    bret = false;  // downward line start at interior_y (below line)
  } else if ((end_y == interior_y) && (start_y < interior_y)) {
    bret = false;  // upward line end at interior_y (below line)
  }
  return bret;
}

int ObGeoInteriorPointVisitor::inner_calculate_crossing_points(
    const ObWkbGeomLinearRing &ring, double interior_y, ObArray<double> &crossing_points_x)
{
  int ret = OB_SUCCESS;
  ObWkbGeomLinearRing::iterator iter = ring.begin();
  double ymin = iter->get<1>();
  double ymax = iter->get<1>();
  for (iter++; iter < ring.end(); iter++) {
    if (iter->get<1>() < ymin) {
      ymin = iter->get<1>();
    }
    if (iter->get<1>() > ymax) {
      ymax = iter->get<1>();
    }
  }
  if (interior_y >= ymin && interior_y <= ymax) {
    ObWkbGeomLinearRing::iterator p0_iter = ring.begin();  // segment start point
    ObWkbGeomLinearRing::iterator p1_iter = ring.begin();  // segment end point
    p1_iter++;
    uint32_t sz = ring.size() - 1; // to avoid calculate first segment twice
    uint32_t i = 0;
    for (; i < sz && OB_SUCC(ret); p0_iter++, p1_iter++, ++i) {
      double y0 = p0_iter->get<1>();
      double y1 = p1_iter->get<1>();
      if (is_crossing_line(y0, y1, interior_y)) {
        double x0 = p0_iter->get<0>();
        double x1 = p1_iter->get<0>();
        double x = 0.0;
        if (x1 == x0) {
          x = x0;
        } else {
          // check y1 != y0 in is_crossing_line
          double percent = (interior_y - y0) / (y1 - y0);
          x = x0 + percent * (x1 - x0);
        }
        if (OB_FAIL(crossing_points_x.push_back(x))) {
          LOG_WARN("fail to push double data into array", K(ret), K(x));
        }
      }
    }
  }
  return ret;
}

int ObGeoInteriorPointVisitor::calculate_crossing_points(
    ObIWkbGeomPolygon *geo, double interior_y, ObArray<double> &crossing_points_x)
{
  int ret = OB_SUCCESS;
  const ObWkbGeomPolygon *polygon = reinterpret_cast<const ObWkbGeomPolygon *>(geo->val());
  if (OB_FAIL(inner_calculate_crossing_points(polygon->exterior_ring(), interior_y, crossing_points_x))) {
    LOG_WARN("failed to do geom polygon exterior ring visit", K(ret));
  } else {
    const ObWkbGeomPolygonInnerRings &rings = polygon->inner_rings();
    ObWkbGeomPolygonInnerRings::const_iterator iter = rings.begin();
    for (; iter != rings.end() && OB_SUCC(ret); iter++) {
      if (OB_FAIL(inner_calculate_crossing_points(*iter, interior_y, crossing_points_x))) {
        LOG_WARN("failed to do geom polygon inner ring visit", K(ret));
      }
    }
  }
  return ret;
}

int ObGeoInteriorPointVisitor::visit(ObIWkbGeomPolygon *geo)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    if (OB_FAIL(ObGeoTypeUtil::check_empty(geo, is_geo_empty_))) {
      LOG_WARN("fail to check geometry is empty", K(ret));
    } else {
      is_inited_ = true;
    }
  }

  if (OB_SUCC(ret) && !is_geo_empty_) {
    double interior_y = 0;
    ObArray<double> crossing_points;
    if (OB_FAIL(calculate_interior_y(geo, interior_y))) {
      LOG_WARN("fail to calculate interior point's y coordinate", K(ret));
    } else if (OB_FAIL(calculate_crossing_points(geo, interior_y, crossing_points))) {
      LOG_WARN("fail to calculae crossing points", K(ret));
    } else if (crossing_points.size() % 2) {
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_WARN("crossing_points size should be even", K(ret), K(crossing_points.size()));
    } else {
      double interior_x = 0;
      lib::ob_sort(crossing_points.begin(), crossing_points.end());
      for (int64_t i = 0; OB_SUCC(ret) && i < crossing_points.size(); i += 2) {
        double width = crossing_points[i + 1] - crossing_points[i];
        if (width != 0 && width > max_width_) {
          max_width_ = width;
          interior_x = crossing_points[i] + width / 2;
          if (OB_FAIL(assign_interior_point(interior_x, interior_y))) {
            LOG_WARN("fail to assign interior point", K(ret), K(interior_x), K(interior_y));
          }
        }
      }

      if (OB_SUCC(ret) && (max_width_ == -1)) {
        // set default interior point, in case polygon has zero area
        const ObWkbGeomPolygon *polygon = reinterpret_cast<const ObWkbGeomPolygon *>(geo->val());
        const ObWkbGeomInnerPoint &first_point = *polygon->exterior_ring().begin();
        if (OB_FAIL(assign_interior_point(first_point.get<0>(), first_point.get<1>()))) {
          LOG_WARN("fail to assign interior point", K(ret), K(first_point.get<0>()), K(first_point.get<1>()));
        } else {
          max_width_ = 0.0;
        }
      }
    }
  }
  return ret;
}

int ObGeoInteriorPointVisitor::visit(ObIWkbGeomMultiPolygon *geo)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    if (OB_FAIL(ObGeoTypeUtil::check_empty(geo, is_geo_empty_))) {
      LOG_WARN("fail to check geometry is empty", K(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObGeoInteriorPointVisitor::visit(ObIWkbGeomCollection *geo)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObGeoTypeUtil::check_empty(geo, is_geo_empty_))) {
    LOG_WARN("fail to check geometry is empty", K(ret));
  } else if (!is_geo_empty_) {
    if (OB_FAIL(ObGeoTypeUtil::get_coll_dimension(geo, dimension_))) {
      LOG_WARN("fail to calculate collection dimension_", K(ret));
    } else if (dimension_ == 0 || dimension_ == 1) {
      ObGeoEvalCtx centroid_context(allocator_);
      ObGeometry *res_geo = nullptr;
      if (OB_FAIL(centroid_context.append_geo_arg(geo))) {
        LOG_WARN("build geo gis context failed", K(ret));
      } else if (OB_FAIL(ObGeoFunc<ObGeoFuncType::Centroid>::geo_func::eval(centroid_context, res_geo))) {
        if (ret == OB_ERR_BOOST_GEOMETRY_CENTROID_EXCEPTION) {
          exist_centroid_ = false;
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("eval geo centroid failed", K(ret));
        }
      } else {
        centroid_pt_ = reinterpret_cast<ObCartesianPoint *>(res_geo);
      }
      if (OB_SUCC(ret)) {
        is_inited_ = true;
      }
    }
  }
  return ret;
}

int ObGeoInteriorPointVisitor::get_interior_point(ObGeometry *&interior_point)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(interior_point_)) {
    if (OB_ISNULL(interior_endpoint_)) {
      // return ObCartesianGeometrycollection EMPTY
      if (OB_ISNULL(interior_point = OB_NEWx(ObCartesianGeometrycollection, allocator_, srid_, *allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory for ObCartesianGeometrycollection", K(ret));
      }
    } else {
      interior_point = interior_endpoint_;
    }
  } else {
    interior_point = interior_point_;
  }
  return ret;
}

}  // namespace common
}  // namespace oceanbase
