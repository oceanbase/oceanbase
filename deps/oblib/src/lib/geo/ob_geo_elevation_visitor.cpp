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
#include "ob_geo_elevation_visitor.h"
#include "lib/geo/ob_geo_func_register.h"
#include "lib/geo/ob_geo_3d.h"

namespace oceanbase
{
namespace common
{
void ObGeoElevationCell::add_z(double z)
{
  ++z_num_;
  z_sum_ += z;
}

double ObGeoElevationCell::get_avg_z()
{
  if (z_num_ > 0) {
    z_avg_ = z_sum_ / z_num_;
  }
  return z_avg_;
}

ObGeoElevationExtent::ObGeoElevationExtent(
    const ObGeogBox *extent, int32_t cell_num_x /* = 3*/, int32_t cell_num_y /* = 3*/)
    : extent_(extent),
      cell_num_x_(cell_num_x),
      cell_num_y_(cell_num_y),
      is_z_calculated_(false),
      extent_z_avg_(NAN)
{
  if (OB_ISNULL(extent_)) {
    cell_size_x_ = 0;
    cell_size_y_ = 0;
  } else {
    cell_size_x_ = (extent_->xmax - extent_->xmin) / cell_num_x_;
    cell_size_y_ = (extent_->ymax - extent_->ymin) / cell_num_y_;
  }
  cell_num_x_ = cell_size_x_ <= 0.0 ? 1 : cell_num_x_;
  cell_num_y_ = cell_size_y_ <= 0.0 ? 1 : cell_num_y_;
}

int ObGeoElevationExtent::add_geometry(const ObGeometry &g)
{
  int ret = OB_SUCCESS;
  if (!ObGeoTypeUtil::is_3d_geo_type(g.type())) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("geometry should be 3D type", K(ret), K(g.type()));
  } else {
    ObGeometry3D *geo3d = const_cast<ObGeometry3D *>(reinterpret_cast<const ObGeometry3D *>(&g));
    if (OB_FAIL(geo3d->create_elevation_extent(*this))) {
      LOG_WARN("fail to create elevation extent", K(ret));
    }
  }
  return ret;
}

int64_t ObGeoElevationExtent::get_cell_idx(double x, double y)
{
  int64_t x_idx = 0;
  int64_t y_idx = 0;
  if (OB_NOT_NULL(extent_)) {
    x_idx = static_cast<int64_t>((x - extent_->xmin) / cell_size_x_);
    if (x_idx < 0) {
      x_idx = 0;
    } else if (x_idx >= cell_num_x_) {
      x_idx = cell_num_x_ - 1;
    }
    y_idx = static_cast<int64_t>((y - extent_->ymin) / cell_size_y_);
    if (y_idx < 0) {
      y_idx = 0;
    } else if (y_idx >= cell_num_y_) {
      y_idx = cell_num_y_ - 1;
    }
  }
  return cell_num_x_ * y_idx + x_idx;
}

int ObGeoElevationExtent::add_point(double x, double y, double z)
{
  int ret = OB_SUCCESS;
  if (cells_.empty() && OB_FAIL(cells_.prepare_allocate(cell_num_x_ * cell_num_y_))) {
    LOG_WARN("fail to reserve cells", K(ret), K(cell_num_x_), K(cell_num_y_));
  } else {
    int64_t cell_idx = get_cell_idx(x, y);
    ObGeoElevationCell &cell = cells_[cell_idx];
    cell.add_z(z);
  }
  return ret;
}

double ObGeoElevationExtent::get_z(double x, double y)
{
  if (!is_z_calculated_) {
    calculate_z();
  }
  int64_t cell_idx = get_cell_idx(x, y);
  ObGeoElevationCell &cell = cells_[cell_idx];
  return cell.is_empty() ? extent_z_avg_ : cell.get_avg_z();
}

void ObGeoElevationExtent::calculate_z()
{
  int64_t not_empty_cell = 0;
  double z_sum = 0.0;
  for (int64_t i = 0; i < cells_.size(); ++i) {
    if (!cells_[i].is_empty()) {
      ++not_empty_cell;
      z_sum += cells_[i].get_avg_z();
    }
  }
  extent_z_avg_ = not_empty_cell > 0 ? (z_sum / not_empty_cell) : NAN;
  is_z_calculated_ = true;
}

ObGeoElevationVisitor::ObGeoElevationVisitor(ObIAllocator &allocator, const common::ObSrsItem *srs)
      : extent_(nullptr),
        is_inited_(false),
        allocator_(&allocator),
        buffer_(allocator),
        srs_(srs),
        type_3D_(ObGeoType::GEO3DTYPEMAX),
        crs_(ObGeoCRS::Cartesian),
        srid_(0)
{
  if (OB_NOT_NULL(srs_)) {
    crs_ = (srs_->srs_type() == ObSrsType::PROJECTED_SRS) ? ObGeoCRS::Cartesian
                                                          : ObGeoCRS::Geographic;
    srid_ = srs_->get_srid();
  }
}

int ObGeoElevationVisitor::add_geometry(
    const ObGeometry &geo, ObGeogBox *&extent, bool &is_geo_empty)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator;
  if (!ObGeoTypeUtil::is_3d_geo_type(geo.type())) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("geometry should be 3D type", K(ret), K(geo.type()), K(geo.type()));
  } else {
    ObGeometry3D &geo_3D = reinterpret_cast<ObGeometry3D &>(const_cast<ObGeometry &>(geo));
    if (OB_FAIL(geo_3D.check_empty(is_geo_empty))) {
      LOG_WARN("fail to check is geometry empty", K(ret));
    } else if (!is_geo_empty) {
      ObGeometry *geo_2D = nullptr;
      ObGeoEvalCtx geo_ctx(allocator_, srs_);
      geo_ctx.set_is_called_in_pg_expr(true);
      ObGeogBox *box = nullptr;
      if (OB_FAIL(geo_3D.to_2d_geo(tmp_allocator, geo_2D))) {
        LOG_WARN("fail to transfer to 2D geometry", K(ret));
      } else if (OB_FAIL(geo_ctx.append_geo_arg(geo_2D))) {
        LOG_WARN("build gis context failed", K(ret), K(geo_ctx.get_geo_count()));
      } else if (OB_FAIL(ObGeoFunc<ObGeoFuncType::Box>::geo_func::eval(geo_ctx, box))) {
        LOG_WARN("failed to do box functor failed", K(ret));
      } else if (OB_ISNULL(extent)) {
        extent = box;
      } else {
        ObGeoBoxUtil::box_union(*box, *extent);
      }
    }
  }
  return ret;
}

int ObGeoElevationVisitor::init(const ObGeometry &geo)
{
  int ret = OB_SUCCESS;
  ObGeogBox *extent = nullptr;
  bool is_geo_empty = false;
  if (OB_FAIL(add_geometry(geo, extent, is_geo_empty))) {
    LOG_WARN("fail to add geometry to visitor", K(ret));
  } else if (OB_ISNULL(extent_ = OB_NEWx(ObGeoElevationExtent, allocator_, extent))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret));
  } else if (!is_geo_empty && OB_FAIL(extent_->add_geometry(geo))) {
    LOG_WARN("fail to add geometry to extent", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObGeoElevationVisitor::init(const ObGeometry &geo1, const ObGeometry &geo2)
{
  int ret = OB_SUCCESS;
  ObGeogBox *extent = nullptr;
  bool is_geo1_empty = false;
  bool is_geo2_empty = false;
  if (OB_FAIL(add_geometry(geo1, extent, is_geo1_empty))) {
    LOG_WARN("fail to add geometry to visitor", K(ret));
  } else if (OB_FAIL(add_geometry(geo2, extent, is_geo2_empty))) {
    LOG_WARN("fail to add geometry to visitor", K(ret));
  } else if (OB_ISNULL(extent_ = OB_NEWx(ObGeoElevationExtent, allocator_, extent))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret));
  } else if (!is_geo1_empty && OB_FAIL(extent_->add_geometry(geo1))) {
    LOG_WARN("fail to add geometry to extent", K(ret));
  } else if (!is_geo2_empty && OB_FAIL(extent_->add_geometry(geo2))) {
    LOG_WARN("fail to add geometry to extent", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

bool ObGeoElevationVisitor::prepare(ObGeometry *geo)
{
  bool bret = true;
  if (OB_ISNULL(geo) || !is_inited_ || geo->length() < WKB_COMMON_WKB_HEADER_LEN
      || ObGeoTypeUtil::is_3d_geo_type(geo->type())) {
    bret = false;
  }
  return bret;
}

int ObGeoElevationVisitor::append_point(double x, double y, double z)
{
  int ret = OB_SUCCESS;
  double val_x = x;
  double val_y = y;
  if (crs_ == ObGeoCRS::Geographic) {
    if (OB_FAIL(srs_->longtitude_convert_from_radians(x, val_x))) {
      LOG_WARN("fail to convert radians to longtitude", K(ret));
    } else if (OB_FAIL(srs_->latitude_convert_from_radians(y, val_y))) {
      LOG_WARN("fail to convert radians to latitude", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(buffer_.append(val_x))) {
    LOG_WARN("failed to append point value x", K(ret), K(x));
  } else if (OB_FAIL(buffer_.append(val_y))) {
    LOG_WARN("failed to append point value y", K(ret), K(y));
  } else if (OB_FAIL(buffer_.append(z))) {
    LOG_WARN("failed to append point value z", K(ret), K(z));
  }
  return ret;
}

template<typename T>
int ObGeoElevationVisitor::append_head_info(T *geo, int reserve_len)
{
  int ret = OB_SUCCESS;
  type_3D_ = static_cast<ObGeoType>(
      static_cast<uint32_t>(geo->type()) + ObGeoTypeUtil::WKB_3D_TYPE_OFFSET);
  if (OB_FAIL(buffer_.reserve(reserve_len))) {
    LOG_WARN("fail to alloc memory", K(ret), K(reserve_len));
  } else if (OB_FAIL(buffer_.append(static_cast<char>(ObGeoWkbByteOrder::LittleEndian)))) {
    LOG_WARN("failed to append little endian", K(ret));
  } else if (OB_FAIL(buffer_.append(static_cast<uint32_t>(type_3D_)))) {
    LOG_WARN("failed to append type", K(ret), K(geo->type()), K(type_3D_));
  } else if (OB_FAIL(buffer_.append(static_cast<uint32_t>(geo->size())))) {
    LOG_WARN("failed to append num value", K(ret), K(geo->size()));
  }
  return ret;
}

int ObGeoElevationVisitor::visit(ObIWkbPoint *geo)
{
  int ret = OB_SUCCESS;
  double x = geo->x();
  double y = geo->y();
  uint32_t reserve_len = EWKB_COMMON_WKB_HEADER_LEN + WKB_GEO_DOUBLE_STORED_SIZE * 3;
  type_3D_ = static_cast<ObGeoType>(
      static_cast<uint32_t>(geo->type()) + ObGeoTypeUtil::WKB_3D_TYPE_OFFSET);
  if (OB_FAIL(buffer_.reserve(reserve_len))) {
    LOG_WARN("fail to alloc memory", K(ret), K(reserve_len));
  } else if (OB_FAIL(buffer_.append(static_cast<char>(ObGeoWkbByteOrder::LittleEndian)))) {
    LOG_WARN("failed to append point little endian", K(ret));
  } else if (OB_FAIL(buffer_.append(static_cast<uint32_t>(type_3D_)))) {
    LOG_WARN("failed to append point type", K(ret), K(type_3D_), K(geo->type()));
  } else if (OB_FAIL(append_point(x, y, extent_->get_z(x, y)))) {
    LOG_WARN("failed to point value", K(ret), K(geo->x()), K(geo->y()));
  }
  return ret;
}

template<typename T_IBIN, typename T_BIN>
int ObGeoElevationVisitor::append_line(T_IBIN *geo)
{
  int ret = OB_SUCCESS;
  uint32_t reserve_len = WKB_COMMON_WKB_HEADER_LEN + geo->size() * 3 * WKB_GEO_DOUBLE_STORED_SIZE;
  if (OB_FAIL(append_head_info(geo, reserve_len))) {
    LOG_WARN("failed to append line string head info", K(ret));
  } else {
    const T_BIN *line = reinterpret_cast<const T_BIN *>(geo->val());
    typename T_BIN::iterator iter = line->begin();
    for (; OB_SUCC(ret) && iter != line->end(); iter++) {
      double x = iter->template get<0>();
      double y = iter->template get<1>();
      if (OB_FAIL(append_point(x, y, extent_->get_z(x, y)))) {
        LOG_WARN("failed to point value", K(ret));
      }
    }
  }
  return ret;
}

int ObGeoElevationVisitor::visit(ObIWkbGeomLineString *geo)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL((append_line<ObIWkbGeomLineString, ObWkbGeomLineString>(geo)))) {
    LOG_WARN("fail to append line", K(ret));
  }
  return ret;
}

int ObGeoElevationVisitor::visit(ObIWkbGeogLineString *geo)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL((append_line<ObIWkbGeogLineString, ObWkbGeogLineString>(geo)))) {
    LOG_WARN("fail to append line", K(ret));
  }
  return ret;
}

template<typename T_IBIN, typename T_BIN, typename T_BIN_RING, typename T_BIN_INNER_RING>
int ObGeoElevationVisitor::append_polygon(T_IBIN *geo)
{
  int ret = OB_SUCCESS;
  uint32_t reserve_len = 0;
  if (OB_FAIL(append_head_info(geo, WKB_COMMON_WKB_HEADER_LEN))) {
    LOG_WARN("failed to append head info", K(ret));
  } else {
    T_BIN &poly = *(T_BIN *)(geo->val());
    T_BIN_RING &exterior = poly.exterior_ring();
    if (poly.size() != 0) {
      uint32_t ext_num = exterior.size();
      reserve_len = WKB_GEO_ELEMENT_NUM_SIZE + ext_num * 3 * WKB_GEO_DOUBLE_STORED_SIZE;
      if (OB_FAIL(buffer_.reserve(reserve_len))) {
        LOG_WARN("fail to alloc memory", K(ret), K(reserve_len));
      } else if (OB_FAIL(buffer_.append(ext_num))) {
        LOG_WARN("fail to append ring size", K(ret));
      }
      typename T_BIN_RING::iterator iter = exterior.begin();
      for (; OB_SUCC(ret) && iter != exterior.end(); ++iter) {
        double x = iter->template get<0>();
        double y = iter->template get<1>();
        if (OB_FAIL(append_point(x, y, extent_->get_z(x, y)))) {
          LOG_WARN("failed to point value", K(ret));
        }
      }
    }

    T_BIN_INNER_RING &inner_rings = poly.inner_rings();
    typename T_BIN_INNER_RING::iterator iterInnerRing = inner_rings.begin();
    for (; OB_SUCC(ret) && iterInnerRing != inner_rings.end(); ++iterInnerRing) {
      uint32_t inner_num = iterInnerRing->size();
      reserve_len = WKB_GEO_ELEMENT_NUM_SIZE + inner_num * 3 * WKB_GEO_DOUBLE_STORED_SIZE;
      if (OB_FAIL(buffer_.reserve(reserve_len))) {
        LOG_WARN("fail to alloc memory", K(ret), K(reserve_len));
      } else if (OB_FAIL(buffer_.append(inner_num))) {
        LOG_WARN("fail to append ring size", K(ret));
      }
      typename T_BIN_RING::iterator iter = (*iterInnerRing).begin();
      for (; OB_SUCC(ret) && iter != (*iterInnerRing).end(); ++iter) {
        double x = iter->template get<0>();
        double y = iter->template get<1>();
        if (OB_FAIL(append_point(x, y, extent_->get_z(x, y)))) {
          LOG_WARN("failed to point value", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObGeoElevationVisitor::visit(ObIWkbGeogPolygon *geo)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL((append_polygon<ObIWkbGeogPolygon,
          ObWkbGeogPolygon,
          ObWkbGeogLinearRing,
          ObWkbGeogPolygonInnerRings>(geo)))) {
    LOG_WARN("fail to append polygon", K(ret));
  }
  return ret;
}

int ObGeoElevationVisitor::visit(ObIWkbGeomPolygon *geo)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL((append_polygon<ObIWkbGeomPolygon,
          ObWkbGeomPolygon,
          ObWkbGeomLinearRing,
          ObWkbGeomPolygonInnerRings>(geo)))) {
    LOG_WARN("fail to append polygon", K(ret));
  }
  return ret;
}

template<typename T_IBIN, typename T_BIN>
int ObGeoElevationVisitor::append_multipoint(T_IBIN *geo)
{
  int ret = OB_SUCCESS;
  uint32_t size = geo->size();
  uint32_t reserve_len = size * (EWKB_COMMON_WKB_HEADER_LEN + WKB_GEO_DOUBLE_STORED_SIZE * 3);
  if (OB_FAIL(buffer_.reserve(reserve_len))) {
    LOG_WARN("fail to alloc memory", K(ret), K(reserve_len));
  } else {
    const T_BIN *multi_point = reinterpret_cast<const T_BIN *>(geo->val());
    typename T_BIN::iterator iter = multi_point->begin();
    for (; iter != multi_point->end() && OB_SUCC(ret); iter++) {
      double x = iter->template get<0>();
      double y = iter->template get<1>();
      if (OB_FAIL(buffer_.append(static_cast<char>(ObGeoWkbByteOrder::LittleEndian)))) {
        LOG_WARN("failed to append point little endian", K(ret));
      } else if (OB_FAIL(buffer_.append(static_cast<uint32_t>(ObGeoType::POINTZ)))) {
        LOG_WARN("failed to append point type", K(ret));
      } else if (OB_FAIL(append_point(x, y, extent_->get_z(x, y)))) {
        LOG_WARN("failed to point value", K(ret));
      }
    }
  }
  return ret;
}

int ObGeoElevationVisitor::visit(ObIWkbGeogMultiPoint *geo)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(append_head_info(geo, WKB_COMMON_WKB_HEADER_LEN))) {
    LOG_WARN("failed to append head info", K(ret));
  } else if (OB_FAIL((append_multipoint<ObIWkbGeogMultiPoint, ObWkbGeogMultiPoint>(geo)))) {
    LOG_WARN("fail to append multipoint", K(ret));
  }
  return ret;
}

int ObGeoElevationVisitor::visit(ObIWkbGeomMultiPoint *geo)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(append_head_info(geo, WKB_COMMON_WKB_HEADER_LEN))) {
    LOG_WARN("failed to append head info", K(ret));
  } else if (OB_FAIL((append_multipoint<ObIWkbGeomMultiPoint, ObWkbGeomMultiPoint>(geo)))) {
    LOG_WARN("fail to append multipoint", K(ret));
  }
  return ret;
}

int ObGeoElevationVisitor::visit(ObIWkbGeogMultiLineString *geo)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(append_head_info(geo, WKB_COMMON_WKB_HEADER_LEN))) {
    LOG_WARN("failed to append head info", K(ret));
  }
  return ret;
}

int ObGeoElevationVisitor::visit(ObIWkbGeomMultiLineString *geo)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(append_head_info(geo, WKB_COMMON_WKB_HEADER_LEN))) {
    LOG_WARN("failed to append head info", K(ret));
  }
  return ret;
}

int ObGeoElevationVisitor::visit(ObIWkbGeogMultiPolygon *geo)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(append_head_info(geo, WKB_COMMON_WKB_HEADER_LEN))) {
    LOG_WARN("failed to append head info", K(ret));
  }
  return ret;
}

int ObGeoElevationVisitor::visit(ObIWkbGeomMultiPolygon *geo)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(append_head_info(geo, WKB_COMMON_WKB_HEADER_LEN))) {
    LOG_WARN("failed to append head info", K(ret));
  }
  return ret;
}

int ObGeoElevationVisitor::visit(ObIWkbGeogCollection *geo)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(append_head_info(geo, WKB_COMMON_WKB_HEADER_LEN))) {
    LOG_WARN("failed to append head info", K(ret));
  }
  return ret;
}

int ObGeoElevationVisitor::visit(ObIWkbGeomCollection *geo)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(append_head_info(geo, WKB_COMMON_WKB_HEADER_LEN))) {
    LOG_WARN("failed to append head info", K(ret));
  }
  return ret;
}

int ObGeoElevationVisitor::get_geometry_3D(ObGeometry *&geo)
{
  int ret = OB_SUCCESS;
  if (!is_inited_ || !ObGeoTypeUtil::is_3d_geo_type(type_3D_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("visitor is not inited or not executed", K(ret), K(is_inited_), K(type_3D_));
  } else if (OB_FAIL(ObGeoTypeUtil::create_geo_by_type(
          *allocator_, type_3D_, ObGeoCRS::Geographic == crs_, true, geo, srid_))) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("failed to create swkb", K(ret), K(crs_), K(type_3D_));
  } else {
    geo->set_data(buffer_.string());
  }
  return ret;
}
}  // namespace common
}  // namespace oceanbase