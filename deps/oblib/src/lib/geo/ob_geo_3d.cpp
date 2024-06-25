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
#include "ob_geo_3d.h"
#include "common/ob_smart_call.h"
#include "ob_geo_to_wkt_visitor.h"
#include "lib/utility/ob_fast_convert.h"
#include "lib/geo/ob_geo_latlong_check_visitor.h"
namespace oceanbase
{
namespace common
{

ObGeoWkbByteOrder ObGeometry3D::byteorder() const
{
  return byteorder(0);
}

ObGeoWkbByteOrder ObGeometry3D::byteorder(uint64_t pos) const
{
  ObGeoWkbByteOrder bo = ObGeoWkbByteOrder::INVALID;
  const char* ptr = val();
  if (OB_NOT_NULL(ptr) && pos + WKB_GEO_BO_SIZE <= length()) {
    uint8_t *p = reinterpret_cast<uint8_t*>(const_cast<char*>(ptr + pos));
    if (*p == 0 || *p == 1) {
      bo = static_cast<ObGeoWkbByteOrder>(*p);
    }
  }
  return bo;
}

ObGeoType ObGeometry3D::type() const
{
  return type(0);
}

ObGeoType ObGeometry3D::type(uint64_t pos) const
{
  ObGeoType geo_type = ObGeoType::GEO3DTYPEMAX;
  const char* ptr = val();
  if (OB_NOT_NULL(ptr) && (pos + WKB_GEO_BO_SIZE + WKB_GEO_TYPE_SIZE <= length())) {
    ObGeoWkbByteOrder bo = byteorder(pos);
    geo_type = static_cast<ObGeoType>(ObGeoWkbByteOrderUtil::read<uint32_t>(ptr + pos + WKB_GEO_BO_SIZE, bo));
  }
  return geo_type;
}

int ObGeometry3D::to_2d_geo(ObIAllocator &allocator, ObGeometry *&res, uint32_t srid)
{
  int ret = OB_SUCCESS;
  ObString wkb_2d;
  ObGeoType geo_type = type();
  if (geo_type > ObGeoType::GEOMETRYCOLLECTION && geo_type < ObGeoType::GEO3DTYPEMAX) {
    ObGeo3DTo2DVisitor visitor;
    ObGeoWkbByteOrder bo = byteorder();
    ObWkbBuffer *wkb_buf = NULL;
    set_pos(0);
    if (OB_ISNULL(wkb_buf = OB_NEWx(ObWkbBuffer, &allocator, allocator, bo))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to create wkb buffer", K(ret));
    } else if (FALSE_IT(visitor.set_wkb_buf(wkb_buf))) {
    } else if (OB_FAIL(visit_wkb_inner(visitor))) {
      LOG_WARN("fail to convert to 2d wkb", K(ret));
    } else if (!is_end()) {
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_WARN("has extra byte unparse", K(ret), K(cur_pos_), K(length()));
    } else {
      wkb_2d.assign_ptr(wkb_buf->ptr(), wkb_buf->length());
      geo_type = static_cast<ObGeoType>(static_cast<uint32_t>(geo_type) - ObGeoTypeUtil::WKB_3D_TYPE_OFFSET);
    }
  } else {
    wkb_2d = data_;
  }
  if (OB_SUCC(ret)) {
    bool is_geog = (crs_ == ObGeoCRS::Geographic) ? true : false;
    if (OB_FAIL(ObGeoTypeUtil::create_geo_by_type(*allocator_, geo_type, is_geog, true, res, srid))) {
      LOG_WARN("fail to create 2d geo obj", K(ret), K(geo_type), K(is_geog));
    } else {
      res->set_data(wkb_2d);
    }
  }

  return ret;
}

int ObGeometry3D::read_header(ObGeoWkbByteOrder &bo, ObGeoType &geo_type)
{
  int ret = OB_SUCCESS;
  const char *ptr = val();
  uint64_t header_len = EWKB_COMMON_WKB_HEADER_LEN;
  if (OB_ISNULL(ptr) || (cur_pos_ + header_len > length())) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("pointer or position is wrong", K(ret), K(ptr));
  } else {
    bo = byteorder(cur_pos_);
    const char *geo_type_ptr = ptr + cur_pos_ + WKB_GEO_BO_SIZE;
    geo_type = static_cast<ObGeoType>(ObGeoWkbByteOrderUtil::read<uint32_t>(geo_type_ptr, bo));
    cur_pos_ += header_len;
  }
  return ret;
}

int ObGeometry3D::read_nums_value(ObGeoWkbByteOrder bo, uint32_t &nums)
{
  int ret = OB_SUCCESS;
  const char *ptr = val();
  if (OB_ISNULL(ptr) || (cur_pos_ + WKB_GEO_ELEMENT_NUM_SIZE > length())) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("unexpect ptr or pos to read nums value");
  } else {
    nums = ObGeoWkbByteOrderUtil::read<uint32_t>(ptr + cur_pos_, bo);
    cur_pos_ += WKB_GEO_ELEMENT_NUM_SIZE;
  }
  return ret;
}

int ObGeometry3D::to_wkt(ObIAllocator &allocator, ObString &wkt, uint32_t srid/* = 0*/, int64_t maxdecimaldigits/* = -1*/)
{
  int ret = OB_SUCCESS;
  ObStringBuffer *buf = NULL;
  ObGeo3DToWktVisitor visitor(maxdecimaldigits);
  set_pos(0);
  if (OB_ISNULL(buf = OB_NEWx(ObStringBuffer, &allocator, (&allocator)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate buffer", K(ret));
  } else if (srid != 0) {
    ObFastFormatInt ffi(srid);
    uint64_t reserve_len = strlen("srid") + 1 + ffi.length() + 1;
    // [srid][=][1][2][3][4][;]
    if (OB_FAIL(buf->reserve(reserve_len))) {
      LOG_WARN("fail to reserve memory for buffer_", K(ret), K(reserve_len));
    } else if (OB_FAIL(buf->append("SRID="))) {
      LOG_WARN("fail to append buffer", K(ret));
    } else if (OB_FAIL(buf->append(ffi.ptr(), ffi.length()))) {
      LOG_WARN("fail to append buffer", K(ret), K(ffi.length()));
    } else if (OB_FAIL(buf->append(";"))) {
      LOG_WARN("fail to append buffer", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (FALSE_IT(visitor.set_wkt_buf(buf))) {
  } else if (OB_FAIL(visit_wkb_inner(visitor))) {
    LOG_WARN("fail to convert to wkt", K(ret));
  } else if (!is_end()) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("has extra buffer in wkb", K(ret), K(cur_pos_), K(length()));
  } else {
    wkt.assign_ptr(buf->ptr(), buf->length());
  }
  return ret;
}

int ObGeometry3D::check_wkb_valid()
{
  int ret = OB_SUCCESS;
  ObGeo3DChecker checker;
  set_pos(0);
  if (OB_FAIL(visit_wkb_inner(checker))) {
    LOG_WARN("fail to check wkb valid", K(ret));
  } else if (!is_end()) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("has extra buffer in wkb", K(ret), K(cur_pos_), K(length()));
  }
  return ret;
}

int ObGeometry3D::check_3d_coordinate_range(const ObSrsItem *srs, const bool is_normalized, ObGeoCoordRangeResult &result)
{
  int ret = OB_SUCCESS;
  ObGeo3DCoordinateRangeVisitor range_visitor(srs, is_normalized);
  set_pos(0);
  if (OB_ISNULL(srs)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("srs is NULL", K(ret));
  } else if (OB_FAIL(visit_wkb_inner(range_visitor))) {
    LOG_WARN("fail to check wkb valid", K(ret));
  } else if (!is_end()) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("has extra buffer in wkb", K(ret), K(cur_pos_), K(length()));
  } else {
    range_visitor.get_coord_range_result(result);
  }
  return ret;
}


int ObGeometry3D::reverse_coordinate()
{
  int ret = OB_SUCCESS;
  ObGeo3DReserverCoordinate visitor;
  set_pos(0);
  if (OB_FAIL(visit_wkb_inner(visitor))) {
    LOG_WARN("fail to check wkb valid", K(ret));
  } else if (!is_end()) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("has extra buffer in wkb", K(ret), K(cur_pos_), K(length()));
  }
  return ret;
}

int ObGeometry3D::visit_wkb_inner(ObGeo3DVisitor &visitor)
{
  int ret = OB_SUCCESS;
  ObGeoWkbByteOrder bo = ObGeoWkbByteOrder::INVALID;
  ObGeoType geo_type = ObGeoType::GEOTYPEMAX;
  if (OB_FAIL(read_header(bo, geo_type))) {
    LOG_WARN("fail to read header", K(ret));
  } else if (OB_FAIL(visitor.visit_header(bo, geo_type))) {
    LOG_WARN("bo or geo type is invalid", K(ret), K(bo), K(geo_type));
  } else {
    switch (geo_type) {
      case ObGeoType::POINTZ: {
        if (OB_FAIL(visit_pointz(bo, visitor, false))) {
          LOG_WARN("fail to visit pointz", K(ret));
        }
        break;
      }
      case ObGeoType::LINESTRINGZ: {
        if (OB_FAIL(visit_linestringz(bo, visitor))) {
          LOG_WARN("fail to visit linestringz", K(ret));
        }
        break;
      }
      case ObGeoType::POLYGONZ: {
        if (OB_FAIL(visit_polygonz(bo, visitor))) {
          LOG_WARN("fail to visit polygonz", K(ret));
        }
        break;
      }
      case ObGeoType::MULTIPOINTZ:
      case ObGeoType::MULTILINESTRINGZ:
      case ObGeoType::MULTIPOLYGONZ: {
        if (OB_FAIL(visit_multi_geomz(bo, geo_type, visitor))) {
          LOG_WARN("fail to visit multi geo", K(ret));
        }
        break;
      }
      case ObGeoType::GEOMETRYCOLLECTIONZ: {
        if (OB_FAIL(visit_collectionz(bo, visitor))) {
          LOG_WARN("fail to visit collection", K(ret));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid geo type", K(ret), K(geo_type));
        break;
      }
    }
  }
  return ret;
}

int ObGeometry3D::visit_pointz_inner(ObGeoWkbByteOrder bo, ObGeo3DVisitor &visitor)
{
  int ret = OB_SUCCESS;
  const char *ptr = val();
  uint64_t pointz_len = WKB_POINT_DATA_SIZE + WKB_GEO_DOUBLE_STORED_SIZE;
  if (OB_ISNULL(ptr) || (cur_pos_ + pointz_len > length())) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("copy pointer or position is wrong", K(ret), K(ptr));
  } else {
    double x = ObGeoWkbByteOrderUtil::read<double>(ptr + cur_pos_, bo);
    double y = ObGeoWkbByteOrderUtil::read<double>(ptr + cur_pos_ + WKB_GEO_DOUBLE_STORED_SIZE, bo);
    double z = ObGeoWkbByteOrderUtil::read<double>(ptr + cur_pos_ + 2 * WKB_GEO_DOUBLE_STORED_SIZE, bo);
    if (OB_FAIL(visitor.visit_pointz_inner(x, y, z))) {
      LOG_WARN("fail to visit pointz", K(ret));
    } else {
      cur_pos_ += 3 * WKB_GEO_DOUBLE_STORED_SIZE;
    }
  }
  return ret;
}

int ObGeometry3D::visit_pointz(ObGeoWkbByteOrder bo, ObGeo3DVisitor &visitor, bool is_inner /* =false*/)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(visitor.visit_pointz_start(this, is_inner))) {
    LOG_WARN("fail to visit pointz start", K(ret));
  } else if (OB_FAIL(visit_pointz_inner(bo, visitor))) {
    LOG_WARN("fail to visit point", K(ret));
  } else if (OB_FAIL(visitor.visit_pointz_end(this, is_inner))) {
    LOG_WARN("fail to visit pointz end", K(ret));
  }
  return ret;
}

int ObGeometry3D::visit_linestringz(ObGeoWkbByteOrder bo, ObGeo3DVisitor &visitor, ObLineType line_type /* =false */)
{
  int ret = OB_SUCCESS;
  uint32_t nums = 0;
  if (OB_FAIL(read_nums_value(bo, nums))) {
    LOG_WARN("fail to read nums value", K(ret));
  } else if (OB_FAIL(visitor.visit_linestringz_start(this, nums, line_type))) {
    LOG_WARN("fail to visit linestring start", K(ret));
  }
  for (uint32_t i = 0; OB_SUCC(ret) && i < nums; i++) {
    if (OB_FAIL(visit_pointz(bo, visitor, true))) {
      LOG_WARN("fail to visit inner point", K(ret));
    } else if (OB_FAIL(visitor.visit_linestringz_item_after(this, i, line_type))) {
      LOG_WARN("fail to visit linestring inner", K(ret), K(i));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(visitor.visit_linestringz_end(this, nums, line_type))) {
    LOG_WARN("fail to visit linestring end", K(ret));
  }
  return ret;
}

int ObGeometry3D::visit_polygonz(ObGeoWkbByteOrder bo, ObGeo3DVisitor &visitor)
{
  int ret = OB_SUCCESS;
  uint32_t ring_nums = 0;
  if (OB_FAIL(read_nums_value(bo, ring_nums))) {
    LOG_WARN("fail to read ring nums", K(ret));
  } else if (OB_FAIL(visitor.visit_polygonz_start(this, ring_nums))) {
    LOG_WARN("fail to visit polygonz start", K(ret));
  }
  for (uint32_t i = 0; OB_SUCC(ret) && i < ring_nums; i++) {
    if (OB_FAIL(visit_linestringz(bo, visitor, i == 0 ? ObLineType::ExterRing : ObLineType::InnerRing))) {
      LOG_WARN("fail to visit linestring", K(ret));
    } else if (OB_FAIL(visitor.visit_polygonz_item_after(this, i))) {
      LOG_WARN("fail to visit polygonz innert", K(ret), K(i));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(visitor.visit_polygonz_end(this, ring_nums))) {
    LOG_WARN("fail to visit polygonz end", K(ret));
  }
  return ret;
}

int ObGeometry3D::visit_multi_geomz(ObGeoWkbByteOrder bo, ObGeoType geo_type, ObGeo3DVisitor &visitor)
{
  int ret = OB_SUCCESS;
  ObFunction<int(ObGeoWkbByteOrder bo, ObGeo3DVisitor &)> visit_func;
  uint32_t geo_nums = 0;
  ObGeoWkbByteOrder sub_bo = ObGeoWkbByteOrder::INVALID;
  ObGeoType sub_geo_type = ObGeoType::GEO3DTYPEMAX;
  if (geo_type == ObGeoType::MULTIPOINTZ) {
    visit_func = std::bind(&ObGeometry3D::visit_pointz, this, std::placeholders::_1, std::placeholders::_2, true);
  } else if (geo_type == ObGeoType::MULTILINESTRINGZ) {
    visit_func = std::bind(&ObGeometry3D::visit_linestringz, this, std::placeholders::_1, std::placeholders::_2, ObLineType::Line);
  } else if (geo_type == ObGeoType::MULTIPOLYGONZ) {
    visit_func = std::bind(&ObGeometry3D::visit_polygonz, this, std::placeholders::_1, std::placeholders::_2);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected geo type", K(ret), K(geo_type));
  }

  visitor.set_is_multi(true);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(read_nums_value(bo, geo_nums))) {
    LOG_WARN("fail to read nums", K(ret), K(geo_type));
  } else if (OB_FAIL(visitor.visit_multi_geom_start(geo_type, this, geo_nums))) {
    LOG_WARN("fail to visit multi geom", K(ret));
  }
  for (uint32_t i = 0; i < geo_nums && OB_SUCC(ret); i++) {
    if (OB_FAIL(read_header(sub_bo, sub_geo_type))) {
      LOG_WARN("fail to read header", K(ret));
    } else if (OB_FAIL(visitor.visit_header(sub_bo, sub_geo_type, true))) {
      LOG_WARN("fail to visit header", K(ret), K(sub_bo), K(sub_geo_type));
    } else if (OB_FAIL(visit_func(bo, visitor))) {
      LOG_WARN("fail to visit geom", K(ret));
    } else if (OB_FAIL(visitor.visit_multi_geom_item_after(geo_type, this, i))) {
      LOG_WARN("fail to visit multi geom inner", K(ret), K(i));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(visitor.visit_multi_geom_end(geo_type, this, geo_nums))) {
    LOG_WARN("fail to visit multi geom", K(ret));
  } else {
    visitor.set_is_multi(false);
  }
  return ret;
}

int ObGeometry3D::visit_collectionz(ObGeoWkbByteOrder bo, ObGeo3DVisitor &visitor)
{
  int ret = OB_SUCCESS;
  uint32_t geo_nums = 0;
  if (OB_FAIL(read_nums_value(bo, geo_nums))) {
    LOG_WARN("fail to read nums", K(ret));
  } else if (OB_FAIL(visitor.visit_collectionz_start(this, geo_nums))) {
    LOG_WARN("fail to visit geometrycollection start", K(ret));
  }
  for (uint32_t i = 0; OB_SUCC(ret) && i < geo_nums; i++) {
    if (OB_FAIL(SMART_CALL(visit_wkb_inner(visitor)))) {
      LOG_WARN("fail to convert geoms in collection", K(ret), K(i));
    } else if (OB_FAIL(visitor.visit_collectionz_item_after(this, i))) {
      LOG_WARN("fail to visit collection innert", K(ret), K(i));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(visitor.visit_collectionz_end(this, geo_nums))) {
    LOG_WARN("fail to visit geometrycollection end", K(ret));
  }
  return ret;
}

int ObGeometry3D::to_sdo_geometry(ObSdoGeoObject &sdo_geo)
{
  int ret = OB_SUCCESS;
  ObGeo3DWkbToSdoGeoVisitor visitor;
  set_pos(0);
  if (OB_FAIL(visitor.init(&sdo_geo))) {
    LOG_WARN("fail to init geo3d wkb to sdo geo visitor", K(ret));
  } else if (OB_FAIL(visit_wkb_inner(visitor))) {
    LOG_WARN("fail to check wkb valid", K(ret));
  } else if (!is_end()) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("has extra buffer in wkb", K(ret), K(cur_pos_), K(length()));
  }
  return ret;
}

int ObGeometry3D::create_elevation_extent(ObGeoElevationExtent &extent)
{
  int ret = OB_SUCCESS;
  ObGeo3DElevationVisitor visitor(extent);
  set_pos(0);
  if (OB_FAIL(visit_wkb_inner(visitor))) {
    LOG_WARN("fail to check wkb valid", K(ret));
  } else if (!is_end()) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("has extra buffer in wkb", K(ret), K(cur_pos_), K(length()));
  }
  return ret;
}

int ObGeometry3D::normalize(const ObSrsItem *srs, uint32_t &zoom_in_value)
{
  int ret = OB_SUCCESS;
  ObGeo3DNormalizeVisitor visitor(srs);
  set_pos(0);
  if (OB_FAIL(visit_wkb_inner(visitor))) {
    LOG_WARN("fail to check wkb valid", K(ret));
  } else if (!is_end()) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("has extra buffer in wkb", K(ret), K(cur_pos_), K(length()));
  } else {
    zoom_in_value = visitor.get_zoom_in_value();
  }
  return ret;
}

bool ObGeometry3D::is_empty() const
{
  bool bret = false;
  ObGeoType type = this->type();
  if (type >= ObGeoType::GEO3DTYPEMAX || type <= ObGeoType::GEOTYPEMAX) {
    bret = true;
  } else if (type == ObGeoType::POINTZ) {
    uint32_t bo_type = WKB_GEO_BO_SIZE + WKB_GEO_TYPE_SIZE;
    uint32_t lng = bo_type + 3 * WKB_GEO_DOUBLE_STORED_SIZE;
    if (lng > this->length()) {
      bret = true;
    } else {
      const char *ptr = this->val();
      ObGeoWkbByteOrder bo = this->byteorder();
      double x = ObGeoWkbByteOrderUtil::read<double>(ptr + bo_type, bo);
      double y = ObGeoWkbByteOrderUtil::read<double>(ptr + bo_type + WKB_GEO_DOUBLE_STORED_SIZE, bo);
      double z = ObGeoWkbByteOrderUtil::read<double>(ptr + bo_type + 2 * WKB_GEO_DOUBLE_STORED_SIZE, bo);
      bret = std::isnan(x) || std::isnan(y) || std::isnan(z);
    }
  } else {
    uint32_t bo_type = WKB_GEO_BO_SIZE + WKB_GEO_TYPE_SIZE;
    if (WKB_COMMON_WKB_HEADER_LEN > this->length()) {
      bret = true;
    } else {
      const char *ptr = this->val();
      ObGeoWkbByteOrder bo = this->byteorder();
      uint32_t size = ObGeoWkbByteOrderUtil::read<uint32_t>(ptr + bo_type, bo);
      bret = size <= 0;
    }
  }
  return bret;
}

int ObGeometry3D::check_empty(bool &is_empty)
{
  int ret = OB_SUCCESS;
  ObGeo3DEmptyVisitor visitor;
  set_pos(0);
  if (OB_FAIL(visit_wkb_inner(visitor))) {
    LOG_WARN("fail to check wkb valid", K(ret));
  } else if (!is_end()) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("has extra buffer in wkb", K(ret), K(cur_pos_), K(length()));
  } else {
    is_empty = visitor.is_empty();
  }
  return ret;
}

/**************************************ObGeo3DVisitor**************************************/

int ObGeo3DVisitor::visit_header(ObGeoWkbByteOrder bo, ObGeoType geo_type, bool is_sub_type /* = false */)
{
  UNUSED(bo);
  UNUSED(geo_type);
  return OB_SUCCESS;
}

int ObGeo3DVisitor::visit_pointz_start(ObGeometry3D *geo, bool is_inner)
{
  UNUSED(geo);
  return OB_SUCCESS;
}

int ObGeo3DVisitor::visit_pointz_inner(double x, double y, double z)
{
  UNUSED(x);
  UNUSED(y);
  UNUSED(z);
  return OB_SUCCESS;
}

int ObGeo3DVisitor::visit_pointz_end(ObGeometry3D *geo, bool is_inner)
{
  UNUSED(geo);
  return OB_SUCCESS;
}

int ObGeo3DVisitor::visit_linestringz_start(ObGeometry3D *geo, uint32_t nums, ObLineType line_type)
{
  UNUSED(geo);
  UNUSED(line_type);
  return OB_SUCCESS;
}

int ObGeo3DVisitor::visit_linestringz_item_after(ObGeometry3D *geo, uint32_t idx, ObLineType line_type)
{
  UNUSED(geo);
  UNUSED(idx);
  UNUSED(line_type);
  return OB_SUCCESS;
}

int ObGeo3DVisitor::visit_linestringz_end(ObGeometry3D *geo, uint32_t nums, ObLineType line_type)
{
  UNUSED(geo);
  UNUSED(line_type);
  return OB_SUCCESS;
}

int ObGeo3DVisitor::visit_polygonz_start(ObGeometry3D *geo, uint32_t nums)
{
  UNUSED(geo);
  return OB_SUCCESS;
}

int ObGeo3DVisitor::visit_polygonz_item_after(ObGeometry3D *geo, uint32_t idx)
{
  UNUSED(geo);
  UNUSED(idx);
  return OB_SUCCESS;
}

int ObGeo3DVisitor::visit_polygonz_end(ObGeometry3D *geo, uint32_t nums)
{
  UNUSED(geo);
  return OB_SUCCESS;
}

int ObGeo3DVisitor::visit_multi_geom_start(ObGeoType geo_type, ObGeometry3D *geo, uint32_t nums)
{
  UNUSED(geo);
  return OB_SUCCESS;
}

int ObGeo3DVisitor::visit_multi_geom_item_after(ObGeoType geo_type, ObGeometry3D *geo, uint32_t idx)
{
  UNUSED(geo);
  UNUSED(idx);
  return OB_SUCCESS;
}

int ObGeo3DVisitor::visit_multi_geom_end(ObGeoType geo_type, ObGeometry3D *geo, uint32_t nums)
{
  UNUSED(geo);
  return OB_SUCCESS;
}

int ObGeo3DVisitor::visit_collectionz_start(ObGeometry3D *geo, uint32_t nums)
{
  UNUSED(geo);
  return OB_SUCCESS;
}

int ObGeo3DVisitor::visit_collectionz_item_after(ObGeometry3D *geo, uint32_t idx)
{
  UNUSED(geo);
  UNUSED(idx);
  return OB_SUCCESS;
}

int ObGeo3DVisitor::visit_collectionz_end(ObGeometry3D *geo, uint32_t nums)
{
  UNUSED(geo);
  return OB_SUCCESS;
}

int ObGeometry3D::correct_lon_lat(const ObSrsItem *srs)
{
  int ret = OB_SUCCESS;
  ObGeo3DLonLatChecker checker(srs);
  set_pos(0);
  if (OB_FAIL(visit_wkb_inner(checker))) {
    LOG_WARN("fail to check wkb valid", K(ret));
  } else if (!is_end()) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("has extra buffer in wkb", K(ret), K(cur_pos_), K(length()));
  }
  return ret;
}

/**************************************ObGeo3DChecker**************************************/

int ObGeo3DChecker::visit_header(ObGeoWkbByteOrder bo, ObGeoType geo_type, bool is_sub_type)
{
  UNUSED(is_sub_type);
  int ret = OB_SUCCESS;
  if (bo == ObGeoWkbByteOrder::INVALID || !ObGeoTypeUtil::is_3d_geo_type(geo_type)) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("bo or geo type is invalid", K(ret), K(bo), K(geo_type));
  }
  return ret;
}

int ObGeo3DChecker::visit_pointz_start(ObGeometry3D *geo, bool is_inner)
{
  UNUSED(is_inner);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(geo)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("geo is NULL", K(ret));
  } else {
    const char *ptr = geo->val();
    uint64_t cur_pos = geo->get_pos();
    if (cur_pos + 3 * WKB_GEO_DOUBLE_STORED_SIZE > geo->length()) {
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_WARN("invalid wkb point", K(ret));
    }
  }
  return ret;
}

int ObGeo3DChecker::visit_linestringz_start(ObGeometry3D *geo, uint32_t nums, ObLineType line_type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(geo)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("geo is NULL", K(ret));
  } else {
    const char *ptr = geo->val();
    uint64_t cur_pos = geo->get_pos();
    if (OB_ISNULL(ptr) || (cur_pos + WKB_GEO_ELEMENT_NUM_SIZE > geo->length())) {
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_WARN("unexpect ptr or pos to read nums value");
    } else {
      ObGeoWkbByteOrder bo = geo->byteorder();
      if (line_type != ObLineType::Line && nums < 4) {
        ret = OB_ERR_GIS_INVALID_DATA;
        LOG_WARN("invalid nums point for ring", K(ret), K(nums));
      } else if (line_type == ObLineType::Line && nums < 2) {
        ret = OB_ERR_GIS_INVALID_DATA;
        LOG_WARN("invalid nums point for linestring", K(ret), K(nums));
      } else if (cur_pos + nums * 3 * WKB_GEO_DOUBLE_STORED_SIZE > geo->length()) {
        ret = OB_ERR_GIS_INVALID_DATA;
        LOG_WARN("points nums is not match", K(ret), K(line_type), K(nums));
      }
    }
  }
  return ret;
}

/**************************************ObGeo3DTo2DVisitor**************************************/

int ObGeo3DTo2DVisitor::visit_header(ObGeoWkbByteOrder bo, ObGeoType geo_type, bool is_sub_type)
{
  int ret = OB_SUCCESS;
  uint64_t header_len = EWKB_COMMON_WKB_HEADER_LEN;
  if (bo == ObGeoWkbByteOrder::INVALID || !ObGeoTypeUtil::is_3d_geo_type(geo_type)) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("bo or geo type is invalid", K(ret), K(bo), K(geo_type));
  } else if (OB_ISNULL(wkb_buf_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wkb_buf_ is NULL", K(ret));
  } else if (OB_FAIL(wkb_buf_->reserve(header_len))) {
    LOG_WARN("fail to resverse buffer", K(ret));
  } else if (OB_FAIL(wkb_buf_->append(static_cast<char>(bo)))) {
    LOG_WARN("fail to append byte order", K(ret));
  } else if (OB_FAIL(wkb_buf_->append(static_cast<uint32_t>(geo_type) - ObGeoTypeUtil::WKB_3D_TYPE_OFFSET))) {
    LOG_WARN("fail to append type", K(ret));
  }
  return ret;
}

int ObGeo3DTo2DVisitor::append_nums(uint32_t nums)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(wkb_buf_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wkb_buf_ is NULL", K(ret));
  } else if (OB_FAIL(wkb_buf_->append(nums))) {
    LOG_WARN("fail to append nums value", K(ret));
  }
  return ret;
}

int ObGeo3DTo2DVisitor::visit_pointz_inner(double x, double y, double z)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(wkb_buf_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wkb_buf_ is NULL", K(ret));
  } else if (OB_FAIL(wkb_buf_->append(x))) {
    LOG_WARN("fail to append x", K(ret), K(x));
  } else if (OB_FAIL(wkb_buf_->append(y))) {
    LOG_WARN("fail to append y", K(ret), K(y));
  }
  return ret;
}

int ObGeo3DTo2DVisitor::visit_linestringz_start(ObGeometry3D *geo, uint32_t nums, ObLineType line_type)
{
  UNUSED(geo);
  UNUSED(line_type);
  return append_nums(nums);
}

int ObGeo3DTo2DVisitor::visit_polygonz_start(ObGeometry3D *geo, uint32_t nums)
{
  UNUSED(geo);
  return append_nums(nums);
}

int ObGeo3DTo2DVisitor::visit_multi_geom_start(ObGeoType geo_type, ObGeometry3D *geo, uint32_t nums)
{
  UNUSED(geo_type);
  UNUSED(geo);
  return append_nums(nums);
}

int ObGeo3DTo2DVisitor::visit_collectionz_start(ObGeometry3D *geo, uint32_t nums)
{
  UNUSED(geo);
  return append_nums(nums);
}

/**************************************ObGeo3DToWktVisitor**************************************/

ObGeo3DToWktVisitor::ObGeo3DToWktVisitor(int64_t maxdecimaldigits/* = -1*/)
    : wkt_buf_(NULL), is_oracle_mode_(lib::is_oracle_mode()), is_mpt_visit_(false)
{
  if (maxdecimaldigits >= 0 && maxdecimaldigits < ObGeoToWktVisitor::MAX_DIGITS_IN_DOUBLE) {
    scale_ = maxdecimaldigits;
    has_scale_ = true;
  } else {
    scale_ = ObGeoToWktVisitor::MAX_DIGITS_IN_DOUBLE;
    has_scale_ = false;
  }
}

int ObGeo3DToWktVisitor::visit_header(ObGeoWkbByteOrder bo, ObGeoType geo_type, bool is_sub_type)
{
  int ret = OB_SUCCESS;
  if (!is_sub_type) {
    if (OB_ISNULL(wkt_buf_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("wkt_buf_ is NULL", K(ret));
    } else if (!is_oracle_mode_ && OB_FAIL(wkt_buf_->append(ObGeoTypeUtil::get_geo_name_by_type(geo_type)))) {
      LOG_WARN("fail to append type name", K(ret), K(geo_type));
    } else if (is_oracle_mode_ && OB_FAIL(wkt_buf_->append(ObGeoTypeUtil::get_geo_name_by_type_oracle(geo_type)))) {
      LOG_WARN("fail to append type name", K(ret), K(geo_type));
    } else if (OB_FAIL(wkt_buf_->append(" "))) {
      LOG_WARN("fail to append comma", K(ret));
    }
  }
  return ret;
}

int ObGeo3DToWktVisitor::append_paren(bool is_left)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(wkt_buf_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wkt_buf_ is NULL", K(ret));
  } else if (is_left && OB_FAIL(wkt_buf_->append("("))) {
    LOG_WARN("fail to append left paren", K(ret));
  } else if (!is_left && OB_FAIL(wkt_buf_->append(")"))) {
    LOG_WARN("fail to append right paren", K(ret));
  }
  return ret;
}

int ObGeo3DToWktVisitor::visit_pointz_start(ObGeometry3D *geo, bool is_inner)
{
  bool ret = OB_SUCCESS;
  if (is_mpt_visit_ || (!is_multi() && !is_inner)) {
    ret = append_paren(true);
  }
  return ret;
}

int ObGeo3DToWktVisitor::visit_pointz_end(ObGeometry3D *geo, bool is_inner)
{
  bool ret = OB_SUCCESS;
  if (is_mpt_visit_ || (!is_multi() && !is_inner)) {
    ret = append_paren(false);
  }
  return ret;
}

int ObGeo3DToWktVisitor::visit_pointz_inner(double x, double y, double z)
{
  int ret = OB_SUCCESS;
  uint64_t double_buff_size = ObGeoToWktVisitor::MAX_DIGITS_IN_DOUBLE;
  if (OB_ISNULL(wkt_buf_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wkt_buf_ is NULL", K(ret));
  } else {
    uint64_t len_x = 0;
    uint64_t len_y = 0;
    uint64_t len_z = 0;
    char *buff_ptr = NULL;
    if (OB_FAIL(wkt_buf_->reserve(3 * double_buff_size + 2))) {
      LOG_WARN("fail to reserve buffer", K(ret));
    } else if (FALSE_IT(buff_ptr = wkt_buf_->ptr() + wkt_buf_->length())) {
    } else if (OB_FAIL(ObGeoToWktVisitor::convert_double_to_str(buff_ptr, double_buff_size, x, has_scale_, scale_, is_oracle_mode_, len_x))) {
      LOG_WARN("fail to append x val to buffer", K(ret));
    } else if (OB_FAIL(wkt_buf_->set_length(wkt_buf_->length() + len_x))) {
      LOG_WARN("fail to set buffer x len", K(ret), K(len_x));
    } else if (OB_FAIL(wkt_buf_->append(" "))) {
      LOG_WARN("fail to append space", K(ret));
    } else if (FALSE_IT(buff_ptr = wkt_buf_->ptr() + wkt_buf_->length())) {
    } else if (OB_FAIL(ObGeoToWktVisitor::convert_double_to_str(buff_ptr, double_buff_size, y, has_scale_, scale_, is_oracle_mode_, len_y))) {
      LOG_WARN("fail to append y val to buffer", K(ret));
    }  else if (OB_FAIL(wkt_buf_->set_length(wkt_buf_->length() + len_y))) {
      LOG_WARN("fail to set buffer y len", K(ret), K(len_y));
    } else if (OB_FAIL(wkt_buf_->append(" "))) {
      LOG_WARN("fail to append space", K(ret));
    } else if (FALSE_IT(buff_ptr = wkt_buf_->ptr() + wkt_buf_->length())) {
    } else if (OB_FAIL(ObGeoToWktVisitor::convert_double_to_str(buff_ptr, double_buff_size, z, has_scale_, scale_, is_oracle_mode_, len_z))) {
      LOG_WARN("fail to append z val to buffer", K(ret));
    }  else if (OB_FAIL(wkt_buf_->set_length(wkt_buf_->length() + len_z))) {
      LOG_WARN("fail to set buffer x len", K(ret), K(len_z));
    }
  }
  return ret;
}

int ObGeo3DToWktVisitor::remove_comma()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(wkt_buf_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wkt_buf_ is NULL", K(ret));
  } else if (is_oracle_mode_) {
    if (wkt_buf_->length() > 1 && wkt_buf_->ptr()[wkt_buf_->length() - 1] == ' '
        && wkt_buf_->ptr()[wkt_buf_->length() - 2] == ',') {
      if (OB_FAIL(wkt_buf_->set_length(wkt_buf_->length() - 2))) {
        LOG_WARN("fail to set length", K(ret));
      }
    }
  } else {
    if (wkt_buf_->length() > 0 && wkt_buf_->ptr()[wkt_buf_->length() - 1] == ',') {
      if (OB_FAIL(wkt_buf_->set_length(wkt_buf_->length() - 1))) {
        LOG_WARN("fail to set length", K(ret));
      }
    }
  }
  return ret;
}

int ObGeo3DToWktVisitor::append_comma()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(wkt_buf_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wkt_buf_ is NULL", K(ret));
  } else if (OB_FAIL(wkt_buf_->append(","))) {
    LOG_WARN("fail to append left paren", K(ret));
  } else if (is_oracle_mode_) {
    if (OB_FAIL(wkt_buf_->append(" "))) {
      LOG_WARN("fail to append comma", K(ret));
    }
  }
  return ret;
}

int ObGeo3DToWktVisitor::visit_linestringz_start(ObGeometry3D *geo, uint32_t nums, ObLineType line_type)
{
  UNUSED(geo);
  UNUSED(line_type);
  return append_paren(true);
}

int ObGeo3DToWktVisitor::visit_linestringz_item_after(ObGeometry3D *geo, uint32_t idx, ObLineType line_type)
{
  UNUSED(geo);
  UNUSED(idx);
  UNUSED(line_type);
  return append_comma();
}

int ObGeo3DToWktVisitor::visit_linestringz_end(ObGeometry3D *geo, uint32_t nums, ObLineType line_type)
{
  UNUSED(geo);
  UNUSED(line_type);
  int ret = OB_SUCCESS;
  if (OB_FAIL(remove_comma())) {
    LOG_WARN("fail to remove comma", K(ret));
  } else if (OB_FAIL(append_paren(false))) {
    LOG_WARN("fail to append paren", K(ret));
  }
  return ret;
}

int ObGeo3DToWktVisitor::visit_polygonz_start(ObGeometry3D *geo, uint32_t nums)
{
  UNUSED(geo);
  return append_paren(true);
}

int ObGeo3DToWktVisitor::visit_polygonz_item_after(ObGeometry3D *geo, uint32_t idx)
{
  UNUSED(geo);
  UNUSED(idx);
  return append_comma();
}

int ObGeo3DToWktVisitor::visit_polygonz_end(ObGeometry3D *geo, uint32_t nums)
{
  UNUSED(geo);
  int ret = OB_SUCCESS;
  if (OB_FAIL(remove_comma())) {
    LOG_WARN("fail to remove comma", K(ret));
  } else if (OB_FAIL(append_paren(false))) {
    LOG_WARN("fail to append paren", K(ret));
  }
  return ret;
}

int ObGeo3DToWktVisitor::visit_multi_geom_start(ObGeoType geo_type, ObGeometry3D *geo, uint32_t nums)
{
  UNUSED(geo);
  if (geo_type == ObGeoType::MULTIPOINTZ) {
    is_mpt_visit_ = true;
  }
  return append_paren(true);
}

int ObGeo3DToWktVisitor::visit_multi_geom_item_after(ObGeoType geo_type, ObGeometry3D *geo, uint32_t idx)
{
  UNUSED(geo);
  UNUSED(idx);
  UNUSED(geo_type);
  return append_comma();
}

int ObGeo3DToWktVisitor::visit_multi_geom_end(ObGeoType geo_type, ObGeometry3D *geo, uint32_t nums)
{
  UNUSED(geo);
  int ret = OB_SUCCESS;
  if (geo_type == ObGeoType::MULTIPOINTZ) {
    is_mpt_visit_ = false;
  }
  if (OB_FAIL(remove_comma())) {
    LOG_WARN("fail to remove comma", K(ret));
  } else if (OB_FAIL(append_paren(false))) {
    LOG_WARN("fail to append paren", K(ret));
  }
  return ret;
}

int ObGeo3DToWktVisitor::visit_collectionz_start(ObGeometry3D *geo, uint32_t nums)
{
  UNUSED(geo);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(wkt_buf_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wkt_buf_ is NULL", K(ret));
  } else if (nums == 0 && OB_FAIL(wkt_buf_->append("EMPTY"))) {
    LOG_WARN("fail to append empty", K(ret));
  } else if (nums > 0 && OB_FAIL(append_paren(true))) {
    LOG_WARN("fail to append left paren", K(ret));
  }
  return ret;
}

int ObGeo3DToWktVisitor::visit_collectionz_item_after(ObGeometry3D *geo, uint32_t idx)
{
  UNUSED(geo);
  UNUSED(idx);
  return append_comma();
}

int ObGeo3DToWktVisitor::visit_collectionz_end(ObGeometry3D *geo, uint32_t nums)
{
  UNUSED(geo);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(wkt_buf_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wkt_buf_ is NULL", K(ret));
  } else if (nums == 0) {
  } else if (nums > 0) {
    if (OB_FAIL(remove_comma())) {
      LOG_WARN("fail to remove comma", K(ret));
    } else if (OB_FAIL(append_paren(false))) {
      LOG_WARN("fail to append paren", K(ret));
    }
  }
  return ret;
}

/**************************************ObGeo3DReserverCoordinate**************************************/

int ObGeo3DReserverCoordinate::visit_pointz_start(ObGeometry3D *geo, bool is_inner)
{
  UNUSED(is_inner);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(geo)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("geo is NULL", K(ret));
  } else {
    char *ptr = const_cast<char *>(geo->val());
    uint32_t cur_pos = geo->get_pos();
    ObGeoWkbByteOrder bo = geo->byteorder();
    double x = ObGeoWkbByteOrderUtil::read<double>(ptr + cur_pos, bo);
    double y = ObGeoWkbByteOrderUtil::read<double>(ptr + cur_pos + WKB_GEO_DOUBLE_STORED_SIZE, bo);
    ObGeoWkbByteOrderUtil::write<double>(ptr + cur_pos, y, bo);
    ObGeoWkbByteOrderUtil::write<double>(ptr + cur_pos + WKB_GEO_DOUBLE_STORED_SIZE, x, bo);
  }
  return ret;
}

/**************************************ObGeo3DCoordinateRangeVisitor**************************************/

void ObGeo3DCoordinateRangeVisitor::get_coord_range_result(ObGeoCoordRangeResult &result)
{
  result.is_lati_out_range_ = is_lati_out_range_;
  result.is_long_out_range_ = is_long_out_range_;
  result.value_out_range_ = value_out_range_;
}

int ObGeo3DCoordinateRangeVisitor::visit_pointz_inner(double x, double y, double z)
{
  int ret = OB_SUCCESS;
  ObGeoCoordRangeResult result;
  if (OB_ISNULL(srs_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("srs is null", K(ret));
  } else if (srs_->srs_type() == ObSrsType::PROJECTED_SRS) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("srs is projected type", K(srs_));
  } else if (OB_FAIL(ObGeoCoordinateRangeVisitor::calculate_point_range(srs_, x, y,
          is_normalized_, result))){
    LOG_WARN("failed to calculate point range", K(ret), K(x), K(y));
  } else {
    is_lati_out_range_ = result.is_lati_out_range_;
    is_long_out_range_ = result.is_long_out_range_;
    value_out_range_ = result.value_out_range_;
  }
  return ret;
}

/**************************************ObGeo3DWkbToSdoGeoVisitor**************************************/

int ObGeo3DWkbToSdoGeoVisitor::init(ObSdoGeoObject *geo, uint32_t srid)
{
  INIT_SUCC(ret);
  sdo_geo_ = geo;
  if (OB_ISNULL(sdo_geo_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ObSdoGeoObject ptr is null", K(ret));
  } else {
    sdo_geo_->set_srid(srid);
  }
  return ret;
}

int ObGeo3DWkbToSdoGeoVisitor::append_elem_info(uint64_t offset, uint64_t etype, uint64_t interpretation)
{
  INIT_SUCC(ret);
  if (OB_FAIL(sdo_geo_->append_elem(offset))) {
    LOG_WARN("fail to append offset to sdo_elem_info", K(ret), K(offset));
  } else if (OB_FAIL(sdo_geo_->append_elem(etype))) {
    LOG_WARN("fail to append etype to sdo_elem_info", K(ret), K(etype));
  } else if (OB_FAIL(sdo_geo_->append_elem(interpretation))) {
    LOG_WARN("fail to append interpretation to sdo_elem_info", K(ret), K(interpretation));
  }
  return ret;
}

int ObGeo3DWkbToSdoGeoVisitor::visit_pointz_start(ObGeometry3D *geo, bool is_inner)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(sdo_geo_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ObSdoGeoObject ptr is null", K(ret));
  } else if (!is_multi_visit_ && !is_collection_visit_) {
    sdo_geo_->set_gtype(geo->type());
  }
  return ret;
}

int  ObGeo3DWkbToSdoGeoVisitor::visit_pointz_inner(double x, double y, double z)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(sdo_geo_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ObSdoGeoObject ptr is null", K(ret));
  } else if (is_multi_visit_ || is_collection_visit_ || is_inner_element_) {
    // elem_info = (start_idx, 1, 1)
    if (!is_multi_visit_ && !is_inner_element_ && OB_FAIL(append_elem_info(sdo_geo_->get_ordinates().size() + 1, 1, 1))) {
      LOG_WARN("fail to append sdo_elem_info", K(ret), K(sdo_geo_->get_ordinates().size()));
    } else if (OB_FAIL(sdo_geo_->append_ori(x))) {
      LOG_WARN("fail to append sdo_elem_info", K(ret), K(x));
    } else if (OB_FAIL(sdo_geo_->append_ori(y))) {
      LOG_WARN("fail to append sdo_elem_info", K(ret), K(y));
    } else if (OB_FAIL(sdo_geo_->append_ori(z))) {
      LOG_WARN("fail to append sdo_elem_info", K(ret), K(z));
    }
  } else {
    sdo_geo_->get_point().set_x(x);
    sdo_geo_->get_point().set_y(y);
    sdo_geo_->get_point().set_z(z);
  }
  return ret;
}

int ObGeo3DWkbToSdoGeoVisitor::visit_linestringz_start(ObGeometry3D *geo, uint32_t nums, ObLineType line_type)
{
  INIT_SUCC(ret);
  if (!is_multi_visit_ && !is_collection_visit_ && !is_inner_element_ && line_type == ObLineType::Line) {
    sdo_geo_->set_gtype(geo->type());
  }

  uint64_t etype = line_type == ObLineType::Line ? 2 : (line_type == ObLineType::ExterRing ? 1003 : 2003);
  if (OB_FAIL(append_elem_info(sdo_geo_->get_ordinates().size() + 1, etype, 1))) {
    LOG_WARN("fail to append sdo_elem_info", K(ret), K(sdo_geo_->get_ordinates().size()));
  } else {
    is_inner_element_ = true;
  }
  return ret;
}

int ObGeo3DWkbToSdoGeoVisitor::visit_linestringz_end(ObGeometry3D *geo, uint32_t nums, ObLineType line_type)
{
  UNUSED(geo);
  UNUSED(line_type);
  is_inner_element_ = false;
  return OB_SUCCESS;
}

int ObGeo3DWkbToSdoGeoVisitor::visit_polygonz_start(ObGeometry3D *geo, uint32_t nums)
{
  INIT_SUCC(ret);
  if (!is_multi_visit_ && !is_collection_visit_ && !is_inner_element_) {
    sdo_geo_->set_gtype(geo->type());
  }
  return ret;
}

int ObGeo3DWkbToSdoGeoVisitor::visit_multi_geom_start(ObGeoType geo_type, ObGeometry3D *geo, uint32_t nums)
{
  INIT_SUCC(ret);
  sdo_geo_->set_gtype(geo->type());
  if (geo_type == ObGeoType::MULTIPOINTZ) {
    if (OB_FAIL(append_elem_info(sdo_geo_->get_ordinates().size() + 1, 1, nums))) {
      LOG_WARN("fail to append sdo_elem_info", K(ret), K(sdo_geo_->get_ordinates().size()));
    } else {
      is_multi_visit_ = true;
    }
  } else if (geo_type == ObGeoType::MULTILINESTRINGZ) {
    // do nothing
  } else if (geo_type == ObGeoType::MULTIPOLYGONZ) {
    // do nothing
  } else {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("invalid geo type", K(ret), K(geo_type));
  }
  return ret;
}

int ObGeo3DWkbToSdoGeoVisitor::visit_multi_geom_end(ObGeoType geo_type, ObGeometry3D *geo, uint32_t nums)
{
  INIT_SUCC(ret);
  UNUSED(geo);
  UNUSED(geo_type);
  UNUSED(nums);

  is_multi_visit_ = false;
  return ret;
}

int ObGeo3DWkbToSdoGeoVisitor::visit_collectionz_start(ObGeometry3D *geo, uint32_t nums)
{
  INIT_SUCC(ret);
  sdo_geo_->set_gtype(geo->type());
  is_collection_visit_ = true;
  return ret;
}

int ObGeo3DWkbToSdoGeoVisitor::visit_collectionz_end(ObGeometry3D *geo, uint32_t nums)
{
  INIT_SUCC(ret);
  UNUSED(geo);
  UNUSED(nums);
  is_collection_visit_ = false;
  return ret;
}

int ObGeometry3D::to_geo_json(ObIAllocator *allocator, common::ObString &geo_json)
{
  int ret = OB_SUCCESS;
  ObGeo3DWkbToJsonVisitor visitor(allocator);
  set_pos(0);
  if (OB_FAIL(visit_wkb_inner(visitor))) {
    LOG_WARN("fail to check wkb valid", K(ret));
  } else if (!is_end()) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("has extra buffer in wkb", K(ret), K(cur_pos_), K(length()));
  } else {
    geo_json.assign(visitor.get_result().ptr(), static_cast<int32_t>(visitor.get_result().length()));
  }
  return ret;
}

  // pointz
int ObGeo3DWkbToJsonVisitor::visit_pointz_start(ObGeometry3D *geo, bool is_inner)
{
  INIT_SUCC(ret);
  const char *type_name = "Point";
  // { "type": "Point", "coordinates": [x, y, z] }
  if (!in_multi_visit_ && inner_element_level_ <= 0 && OB_FAIL(appendJsonFields(ObGeoType::POINTZ, type_name))) {
    LOG_WARN("fail to append buffer_", K(ret), K(in_multi_visit_), K(type_name));
  }
  return ret;
}

int ObGeo3DWkbToJsonVisitor::visit_pointz_inner(double x, double y, double z)
{
  INIT_SUCC(ret);
  // [x, y, Z]
  if (OB_FAIL(buffer_.append("["))) {
    LOG_WARN("fail to append to buffer_", K(ret));
  } else if (OB_FAIL(appendDouble(x))) {
    LOG_WARN("fail to append x", K(ret), K(x));
  } else if (OB_FAIL(buffer_.append(", "))) {
    LOG_WARN("fail to append to buffer_", K(ret));
  } else if (OB_FAIL(appendDouble(y))) {
    LOG_WARN("fail to append y", K(ret), K(y));
  } else if (OB_FAIL(buffer_.append(", "))) {
    LOG_WARN("fail to append to buffer_", K(ret));
  } else if (OB_FAIL(appendDouble(z))) {
    LOG_WARN("fail to append z", K(ret), K(z));
  } else if (OB_FAIL(buffer_.append("]"))) {
    LOG_WARN("fail to append to buffer_", K(ret));
  }
  return ret;
}

int ObGeo3DWkbToJsonVisitor::visit_pointz_end(ObGeometry3D *geo, bool is_inner)
{
  INIT_SUCC(ret);
  if (!in_multi_visit_ && inner_element_level_ <= 0 && OB_FAIL(buffer_.append(" }"))) {
    LOG_WARN("fail to append buffer_", K(ret), K(in_multi_visit_));
  } else if ((in_multi_visit_ || in_colloction_visit() || inner_element_level_ > 0) && OB_FAIL(buffer_.append(", "))) {
    LOG_WARN("fail to append buffer_", K(ret));
  }
  return ret;
}
  // linestringz
int ObGeo3DWkbToJsonVisitor::visit_linestringz_start(ObGeometry3D *geo, uint32_t nums, ObLineType line_type)
{
  INIT_SUCC(ret);
  const char *type_name = "LineString";
  if ((inner_element_level_ <= 0) && OB_FAIL(appendJsonFields(ObGeoType::LINESTRINGZ, type_name))) {
    LOG_WARN("fail to append buffer_", K(ret), K(in_multi_visit_), K(type_name));
  } else if (OB_FAIL(buffer_.append("[ "))) {
    LOG_WARN("fail to append buffer_", K(ret));
  } else {
    inner_element_level_++;
  }
  return ret;
}
int ObGeo3DWkbToJsonVisitor::visit_linestringz_end(ObGeometry3D *geo, uint32_t nums, ObLineType line_type)
{
  INIT_SUCC(ret);
  inner_element_level_--;
  if (OB_FAIL(buffer_.set_length(buffer_.length() - 2))) {
    LOG_WARN("fail to set buffer_ len", K(ret), K(buffer_.length()));
  } else if (OB_FAIL(buffer_.append(" ]"))) {
    LOG_WARN("fail to append buffer_", K(ret));
  } else if ((inner_element_level_ <= 0 || (in_colloction_visit() && line_type == ObLineType::Line)) &&
             OB_FAIL(buffer_.append(" }"))) {
    LOG_WARN("fail to append buffer_", K(ret), K(in_multi_visit_));
  } else if ((inner_element_level_ > 0 || in_colloction_visit()) && OB_FAIL(buffer_.append(", "))) {
    LOG_WARN("fail to append buffer_", K(ret));
  }
  return ret;
}
  // polygonz
int ObGeo3DWkbToJsonVisitor::visit_polygonz_start(ObGeometry3D *geo, uint32_t nums)
{
  INIT_SUCC(ret);
  const char *type_name = "Polygon";
  if ((inner_element_level_ <= 0) && OB_FAIL(appendJsonFields(ObGeoType::POLYGONZ, type_name))) {
    LOG_WARN("fail to append buffer_", K(ret), K(inner_element_level_), K(type_name));
  } else if (OB_FAIL(buffer_.append("[ "))) {
    LOG_WARN("fail to append buffer_", K(ret));
  } else {
    inner_element_level_++;
  }
  return ret;
}

int ObGeo3DWkbToJsonVisitor::visit_polygonz_end(ObGeometry3D *geo, uint32_t nums)
{
  INIT_SUCC(ret);
  inner_element_level_--;
  if (OB_FAIL(buffer_.set_length(buffer_.length() - 2))) {
    LOG_WARN("fail to set buffer_ len", K(ret), K(buffer_.length()));
  } else if (OB_FAIL(buffer_.append(" ]"))) {
    LOG_WARN("fail to append buffer_", K(ret));
  } else if ((inner_element_level_ <= 0  || in_colloction_visit()) && OB_FAIL(buffer_.append(" }"))) {
    LOG_WARN("fail to append buffer_", K(ret));
  } else if ((inner_element_level_ > 0  || in_colloction_visit()) && OB_FAIL(buffer_.append(", "))) {
    LOG_WARN("fail to append buffer_", K(ret));
  }
  return ret;
}
  // multi geomz
int ObGeo3DWkbToJsonVisitor::visit_multi_geom_start(ObGeoType geo_type, ObGeometry3D *geo, uint32_t nums)
{
  INIT_SUCC(ret);
  const char *type_name = "MultiPoint";
  if (ObGeoType::MULTILINESTRINGZ == geo_type) {
    type_name = "MultiLineString";
  } else if (ObGeoType::MULTIPOLYGONZ == geo_type) {
    type_name = "MultiPolygon";
  }
  if (OB_FAIL(appendJsonFields(geo_type, type_name))) {
    LOG_WARN("fail to append buffer_", K(ret), K(in_multi_visit_), K(type_name));
  } else if (OB_FAIL(buffer_.append("[ "))) {
    LOG_WARN("fail to append buffer_", K(ret));
  } else {
    inner_element_level_++;
  }
  return ret;
}
int ObGeo3DWkbToJsonVisitor::visit_multi_geom_end(ObGeoType geo_type, ObGeometry3D *geo, uint32_t nums)
{
  INIT_SUCC(ret);
  inner_element_level_--;
  if (OB_FAIL(appendMultiSuffix(geo_type))) {
    LOG_WARN("fail to append multi suffix", K(ret));
  }
  return ret;
}
  // geometrycollection
int ObGeo3DWkbToJsonVisitor::visit_collectionz_start(ObGeometry3D *geo, uint32_t nums)
{
  INIT_SUCC(ret);
  const char *type_name = "GeometryCollection";
  if (OB_FAIL(appendJsonFields(geo->type(), type_name))) {
    LOG_WARN("fail to append buffer_", K(ret), K(in_multi_visit_), K(type_name));
  } else if (OB_FAIL(buffer_.append("[ "))) {
    LOG_WARN("fail to append buffer_", K(ret));
  } else {
    in_collection_level_++;
  }
  return ret;
}
int ObGeo3DWkbToJsonVisitor::visit_collectionz_end(ObGeometry3D *geo, uint32_t nums)
{
  return appendCollectionSuffix();
}

int ObGeo3DWkbToJsonVisitor::appendDouble(double x)
{
  INIT_SUCC(ret);
  int16_t scale = ObGeoToWktVisitor::MAX_DIGITS_IN_DOUBLE;
  uint64_t double_buff_size = ObGeoToWktVisitor::MAX_DIGITS_IN_DOUBLE;
  uint64_t len_x = 0;
  char *buff_ptr = NULL;
  if (OB_FAIL(buffer_.reserve(double_buff_size))) {
    LOG_WARN("fail to reserve buffer", K(ret));
  } else if (FALSE_IT(buff_ptr = buffer_.ptr() + buffer_.length())) {
  } else if (OB_FAIL(ObGeoToWktVisitor::convert_double_to_str(buff_ptr, double_buff_size, x, false, scale, true, len_x))) {
    LOG_WARN("fail to append x val to buffer", K(ret));
  } else if (OB_FAIL(buffer_.set_length(buffer_.length() + len_x))) {
    LOG_WARN("fail to set buffer x len", K(ret), K(len_x));
  }
  return ret;
}

int ObGeo3DWkbToJsonVisitor::appendJsonFields(ObGeoType type, const char *type_name)
{
  int ret = OB_SUCCESS;
  if (type < ObGeoType::POINTZ || type > ObGeoType::GEOMETRYCOLLECTIONZ) {
    LOG_WARN("invalid geo type", K(ret), K(type));
  } else if (OB_FAIL(buffer_.append("{ \"type\": \""))) {
    LOG_WARN("fail to append type field", K(ret));
  } else if (OB_FAIL(buffer_.append(type_name))) {
    LOG_WARN("fail to append type value", K(ret), K(type_name));
  } else if (type != ObGeoType::GEOMETRYCOLLECTIONZ &&
               OB_FAIL(buffer_.append("\", \"coordinates\": "))) {
    LOG_WARN("fail to append coordinates field", K(ret));
  } else if (type == ObGeoType::GEOMETRYCOLLECTIONZ &&
               OB_FAIL(buffer_.append("\", \"geometries\": "))) {
    LOG_WARN("fail to append geometries field", K(ret));
  }
  return ret;
}

int ObGeo3DWkbToJsonVisitor::appendMultiSuffix(ObGeoType geo_type)
{
  INIT_SUCC(ret);
  if (OB_FAIL(buffer_.set_length(buffer_.length() - 2))) {
      LOG_WARN("fail to set buffer_ len", K(ret), K(buffer_.length()));
  } else if ((geo_type == ObGeoType::MULTIPOINTZ || !in_colloction_visit()) && OB_FAIL(buffer_.append(" ] }"))) {
    LOG_WARN("fail to append buffer_", K(ret));
  } else if ((in_colloction_visit() || inner_element_level_ > 0) && OB_FAIL(buffer_.append(", "))) {
    LOG_WARN("fail to append buffer_", K(ret));
  }
  return ret;
}

int ObGeo3DWkbToJsonVisitor::appendCollectionSuffix()
{
  INIT_SUCC(ret);
  ObString comma(2, buffer_.ptr() + buffer_.length() - 2);
  if ((comma.compare(", ") == 0) && OB_FAIL(buffer_.set_length(buffer_.length() - 2))) {
      LOG_WARN("fail to set buffer_ len", K(ret), K(buffer_.length()));
  } else if (OB_FAIL(buffer_.append(" ] }"))) {
    LOG_WARN("fail to append buffer_", K(ret));
  }

  in_collection_level_--;
  if (OB_FAIL(ret)) {
  } else if (in_colloction_visit() && OB_FAIL(buffer_.append(", "))) {
    LOG_WARN("fail to append buffer_", K(ret));
  }
  return ret;
}

int ObGeo3DElevationVisitor::visit_pointz_start(ObGeometry3D *geo, bool is_inner)
{
  UNUSED(is_inner);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(geo)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("geo is NULL", K(ret));
  } else {
    uint32_t cur_pos = geo->get_pos();
    uint64_t pointz_len = WKB_POINT_DATA_SIZE + WKB_GEO_DOUBLE_STORED_SIZE;
    char *ptr = const_cast<char *>(geo->val());
    if (OB_ISNULL(ptr) || (cur_pos + pointz_len > geo->length())) {
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_WARN("3D geometry position is wrong", K(ret));
    } else {
      ObGeoWkbByteOrder bo = geo->byteorder();
      double x = ObGeoWkbByteOrderUtil::read<double>(ptr + cur_pos, bo);
      double y = ObGeoWkbByteOrderUtil::read<double>(ptr + cur_pos + WKB_GEO_DOUBLE_STORED_SIZE, bo);
      double z = ObGeoWkbByteOrderUtil::read<double>(ptr + cur_pos + 2 * WKB_GEO_DOUBLE_STORED_SIZE, bo);
      if (OB_FAIL(extent_->add_point(x, y, z))) {
        LOG_WARN("fail to add point into extent", K(ret));
      }
    }
  }
  return ret;
}

int ObGeo3DNormalizeVisitor::visit_pointz_start(ObGeometry3D *geo, bool is_inner)
{
  UNUSED(is_inner);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(geo)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("geo is NULL", K(ret));
  } else {
    uint32_t cur_pos = geo->get_pos();
    uint64_t pointz_len = WKB_POINT_DATA_SIZE + WKB_GEO_DOUBLE_STORED_SIZE;
    char *ptr = const_cast<char *>(geo->val());
    if (OB_ISNULL(ptr) || (cur_pos + pointz_len > geo->length())) {
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_WARN("3D geometry position is wrong", K(ret));
    } else {
      ObGeoWkbByteOrder bo = geo->byteorder();
      double x = ObGeoWkbByteOrderUtil::read<double>(ptr + cur_pos, bo);
      double y = ObGeoWkbByteOrderUtil::read<double>(ptr + cur_pos + WKB_GEO_DOUBLE_STORED_SIZE, bo);
      double z = ObGeoWkbByteOrderUtil::read<double>(ptr + cur_pos + 2 * WKB_GEO_DOUBLE_STORED_SIZE, bo);
      double nx = 1.0;
      double ny = 1.0;
      double nz = 1.0;
      if (no_srs_) {
        nx = x * M_PI / 180.0;
        ny = y * M_PI / 180.0;
        nz = z * M_PI / 180.0;
      } else {
        if (OB_FAIL(srs_->latitude_convert_to_radians(y, ny))) {
          LOG_WARN("normalize y failed", K(ret));
        } else if (OB_FAIL(srs_->longtitude_convert_to_radians(x, nx))) {
          LOG_WARN("normalize x failed", K(ret));
        } else {
          uint32_t count = 0;
          double nx_tmp = nx;
          double ny_tmp = ny;
          double nz_tmp = nz;
          while (nx_tmp != 0.0 && std::fabs(nx_tmp) < ZOOM_IN_THRESHOLD) {
            nx_tmp *= 10;
            count++;
          }
          zoom_in_value_ = count > zoom_in_value_ ? count : zoom_in_value_;
          count = 0;
          while (ny_tmp != 0.0 && std::fabs(ny_tmp) < ZOOM_IN_THRESHOLD) {
            ny_tmp *= 10;
            count++;
          }
          zoom_in_value_ = count > zoom_in_value_ ? count : zoom_in_value_;
          count = 0;
          while (nz_tmp != 0.0 && std::fabs(nz_tmp) < ZOOM_IN_THRESHOLD) {
            nz_tmp *= 10;
            count++;
          }
          zoom_in_value_ = count > zoom_in_value_ ? count : zoom_in_value_;
        }
      }
      if (OB_SUCC(ret)) {
        ObGeoWkbByteOrderUtil::write<double>(ptr + cur_pos, nx, bo);
        ObGeoWkbByteOrderUtil::write<double>(ptr + cur_pos + WKB_GEO_DOUBLE_STORED_SIZE, ny, bo);
        ObGeoWkbByteOrderUtil::write<double>(ptr + cur_pos + WKB_GEO_DOUBLE_STORED_SIZE * 2, nz, bo);
      }
    }
  }
  return ret;
}

int ObGeo3DEmptyVisitor::visit_pointz_start(ObGeometry3D *geo, bool is_inner)
{
  UNUSED(is_inner);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(geo)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("geo is NULL", K(ret));
  } else {
    uint32_t cur_pos = geo->get_pos();
    uint64_t pointz_len = WKB_POINT_DATA_SIZE + WKB_GEO_DOUBLE_STORED_SIZE;
    char *ptr = const_cast<char *>(geo->val());
    if (OB_ISNULL(ptr) || (cur_pos + pointz_len > geo->length())) {
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_WARN("3D geometry position is wrong", K(ret));
    } else {
      ObGeoWkbByteOrder bo = geo->byteorder();
      double x = ObGeoWkbByteOrderUtil::read<double>(ptr + cur_pos, bo);
      double y = ObGeoWkbByteOrderUtil::read<double>(ptr + cur_pos + WKB_GEO_DOUBLE_STORED_SIZE, bo);
      double z = ObGeoWkbByteOrderUtil::read<double>(ptr + cur_pos + 2 * WKB_GEO_DOUBLE_STORED_SIZE, bo);
      is_empty_ = std::isnan(x) || std::isnan(y) || std::isnan(z);
    }
  }
  return ret;
}

int ObGeo3DLonLatChecker::visit_pointz_start(ObGeometry3D *geo, bool is_inner)
{
  UNUSED(is_inner);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(geo)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("geo is NULL", K(ret));
  } else if (OB_ISNULL(srs_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("srs is null", K(ret));
  } else if (srs_->srs_type() == ObSrsType::PROJECTED_SRS) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("srs is projected type", K(srs_));
  } else {
    uint32_t cur_pos = geo->get_pos();
    uint64_t pointz_len = WKB_POINT_DATA_SIZE + WKB_GEO_DOUBLE_STORED_SIZE;
    char *ptr = const_cast<char *>(geo->val());
    if (OB_ISNULL(ptr) || (cur_pos + pointz_len > geo->length())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("3D geometry position is wrong", K(ret));
    } else {
      ObGeoWkbByteOrder bo = geo->byteorder();
      double lon = ObGeoWkbByteOrderUtil::read<double>(ptr + cur_pos, bo);
      lon = ObGeoLatlongCheckVisitor::ob_normalize_longitude(lon);
      double lat = ObGeoWkbByteOrderUtil::read<double>(ptr + cur_pos + WKB_GEO_DOUBLE_STORED_SIZE, bo);
      lat = ObGeoLatlongCheckVisitor::ob_normalize_latitude(lat);
      if (OB_SUCC(ret)) {
        ObGeoWkbByteOrderUtil::write<double>(ptr + cur_pos, lon, bo);
        ObGeoWkbByteOrderUtil::write<double>(ptr + cur_pos + WKB_GEO_DOUBLE_STORED_SIZE, lat, bo);
      }
    }
  }
  return ret;
}

} // namespace common
} // namespace oceanbase