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

#ifndef OCEANBASE_LIB_GEO_OB_GEO_INTERIOR_POINT_VISITOR_
#define OCEANBASE_LIB_GEO_OB_GEO_INTERIOR_POINT_VISITOR_
#include "lib/geo/ob_geo_visitor.h"

namespace oceanbase
{
namespace common
{

class ObGeoInteriorPointVisitor : public ObEmptyGeoVisitor
{
public:
  explicit ObGeoInteriorPointVisitor(ObIAllocator *allocator)
      : allocator_(allocator),
        interior_point_(nullptr),
        min_dist_(DBL_MAX),
        interior_endpoint_(nullptr),
        min_endpoint_dist_(DBL_MAX),
        srid_(0),
        exist_centroid_(true),
        centroid_pt_(nullptr),
        max_width_(-1),
        is_geo_empty_(true),
        is_inited_(false),
        dimension_(-1)
  {}
  ~ObGeoInteriorPointVisitor()
  {}
  bool prepare(ObGeometry *geo)
  {
    UNUSED(geo);
    return true;
  }

  bool prepare(ObIWkbGeomMultiPoint *geo)
  {
    UNUSED(geo);
    return (dimension_ == -1 || dimension_ == 0);
  }

  bool prepare(ObIWkbGeomLineString *geo)
  {
    UNUSED(geo);
    return (dimension_ == -1 || dimension_ == 1);
  }

  bool prepare(ObIWkbGeomMultiLineString *geo)
  {
    UNUSED(geo);
    return (dimension_ == -1 || dimension_ == 1);
  }

  bool prepare(ObIWkbGeomPolygon *geo)
  {
    UNUSED(geo);
    return (dimension_ == -1 || dimension_ == 2);
  }

  bool prepare(ObIWkbGeomMultiPolygon *geo)
  {
    UNUSED(geo);
    return (dimension_ == -1 || dimension_ == 2);
  }

  // wkb
  int visit(ObIWkbGeomPoint *geo);
  int visit(ObIWkbGeomLineString *geo);
  int visit(ObIWkbGeomMultiPoint *geo);
  int visit(ObIWkbGeomMultiLineString *geo);
  int visit(ObIWkbGeomPolygon *geo);
  int visit(ObIWkbGeomMultiPolygon *geo);
  int visit(ObIWkbGeomCollection *geo);

  bool is_end(ObIWkbGeomPoint *geo)
  {
    UNUSED(geo);
    return true;
  }
  bool is_end(ObIWkbGeomLineString *geo)
  {
    UNUSED(geo);
    return true;
  }
  bool is_end(ObIWkbGeomMultiPoint *geo)
  {
    UNUSED(geo);
    return true;
  }
  bool is_end(ObIWkbGeomPolygon *geo)
  {
    UNUSED(geo);
    return true;
  }
  bool is_end(ObIWkbGeomCollection *geo)
  {
    UNUSED(geo);
    return false;
  }

  virtual int finish(ObGeometry *geo) override
  {
    UNUSED(geo);
    return OB_SUCCESS;
  }

  int get_interior_point(ObGeometry *&interior_point);

private:
  int init(ObGeometry *geo);
  int assign_interior_point(double x, double y);
  int assign_interior_endpoint(double x, double y);
  template<typename PointType>
  double calculate_euclidean_distance(ObCartesianPoint &p1, PointType &p2);
  int calculate_interior_y(ObIWkbGeomPolygon *geo, double &interior_y);
  int inner_calculate_interior_y(const ObWkbGeomLinearRing &ring, double centre_y, double &ymax, double &ymin);
  int calculate_crossing_points(ObIWkbGeomPolygon *geo, double interior_y, ObArray<double> &crossing_points_x);
  int inner_calculate_crossing_points(
      const ObWkbGeomLinearRing &ring, double interior_y, ObArray<double> &crossing_points_x);
  bool is_crossing_line(double start_y, double end_y, double interior_y);

  ObIAllocator *allocator_;
  ObCartesianPoint *interior_point_;
  double min_dist_;  // for point/multipoint/line/multiline
  ObCartesianPoint *interior_endpoint_;
  double min_endpoint_dist_;  // for line/multiline
  uint32_t srid_;
  bool exist_centroid_;
  ObCartesianPoint *centroid_pt_;
  double max_width_;  // for polygon/multipolygon
  bool is_geo_empty_;
  bool is_inited_;
  int8_t dimension_; // for collection
  DISALLOW_COPY_AND_ASSIGN(ObGeoInteriorPointVisitor);
};

}  // namespace common
}  // namespace oceanbase

#endif