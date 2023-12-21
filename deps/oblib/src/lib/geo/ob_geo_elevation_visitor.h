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

#ifndef OCEANBASE_LIB_GEO_OB_GEO_ELEVATION_VISITOR_
#define OCEANBASE_LIB_GEO_OB_GEO_ELEVATION_VISITOR_
#include "lib/geo/ob_geo_visitor.h"
#include "lib/geo/ob_geo_utils.h"

namespace oceanbase
{

namespace common
{
class ObGeoElevationCell
{
public:
  explicit ObGeoElevationCell() : z_num_(0), z_sum_(0.0), z_avg_(NAN)
  {}
  void add_z(double z);
  bool is_empty()
  {
    return z_num_ == 0;
  }
  double get_avg_z();
  TO_STRING_KV(K_(z_num), K_(z_sum), K_(z_avg));

private:
  int32_t z_num_;
  double z_sum_;
  double z_avg_;
};

class ObGeoElevationExtent
{
public:
  explicit ObGeoElevationExtent(
      const ObGeogBox *extent, int32_t cell_num_x = 3, int32_t cell_num_y = 3);
  virtual ~ObGeoElevationExtent() { cells_.destroy(); }
  int add_geometry(const ObGeometry &g);
  int add_point(double x, double y, double z);
  double get_z(double x, double y);

private:
  int64_t get_cell_idx(double x, double y);
  void calculate_z();

  const ObGeogBox *extent_;
  int32_t cell_num_x_;
  int32_t cell_num_y_;
  double cell_size_x_;
  double cell_size_y_;
  ObArray<ObGeoElevationCell> cells_;
  bool is_z_calculated_;
  double extent_z_avg_;
};

class ObGeoElevationVisitor : public ObEmptyGeoVisitor
{
public:
  explicit ObGeoElevationVisitor(ObIAllocator &allocator, const common::ObSrsItem *srs);
  virtual ~ObGeoElevationVisitor()
  { if (OB_NOT_NULL(extent_))
    { extent_->~ObGeoElevationExtent(); }
  }
  int init(const ObGeometry &geo1, const ObGeometry &geo2);
  int init(const ObGeometry &geo);
  bool prepare(ObGeometry *geo);
  // wkb
  int visit(ObIWkbPoint *geo);

  int visit(ObIWkbGeogLineString *geo);
  int visit(ObIWkbGeomLineString *geo);

  int visit(ObIWkbGeogMultiPoint *geo);
  int visit(ObIWkbGeomMultiPoint *geo);

  int visit(ObIWkbGeogPolygon *geo);
  int visit(ObIWkbGeomPolygon *geo);

  int visit(ObIWkbGeogMultiLineString *geo);
  int visit(ObIWkbGeomMultiLineString *geo);

  int visit(ObIWkbGeogMultiPolygon *geo);
  int visit(ObIWkbGeomMultiPolygon *geo);

  int visit(ObIWkbGeogCollection *geo);
  int visit(ObIWkbGeomCollection *geo);

  // is_end default false
  bool is_end(ObIWkbGeogLineString *geo)
  {
    UNUSED(geo);
    return true;
  }

  bool is_end(ObIWkbGeomLineString *geo)
  {
    UNUSED(geo);
    return true;
  }

  bool is_end(ObIWkbGeogPolygon *geo)
  {
    UNUSED(geo);
    return true;
  }

  bool is_end(ObIWkbGeomPolygon *geo)
  {
    UNUSED(geo);
    return true;
  }

  bool is_end(ObIWkbGeogMultiPoint *geo)
  {
    UNUSED(geo);
    return true;
  }

  bool is_end(ObIWkbGeomMultiPoint *geo)
  {
    UNUSED(geo);
    return true;
  }

  int get_geometry_3D(ObGeometry *&geo);

private:
  int add_geometry(const ObGeometry &geo, ObGeogBox *&extent, bool &is_geo_empty);
  int append_point(double x, double y, double z);
  template<typename T>
  int append_head_info(T *geo, int reserve_len);
  template<typename T_IBIN, typename T_BIN>
  int append_line(T_IBIN *geo);
  template<typename T_IBIN, typename T_BIN, typename T_BIN_RING, typename T_BIN_INNER_RING>
  int append_polygon(T_IBIN *geo);
  template<typename T_IBIN, typename T_BIN>
  int append_multipoint(T_IBIN *geo);

  ObGeoElevationExtent *extent_;
  bool is_inited_;
  ObIAllocator *allocator_;
  ObWkbBuffer buffer_;
  const common::ObSrsItem *srs_;
  ObGeoType type_3D_;
  ObGeoCRS crs_;
  uint32_t srid_;

  DISALLOW_COPY_AND_ASSIGN(ObGeoElevationVisitor);
};

}  // namespace common
}  // namespace oceanbase

#endif