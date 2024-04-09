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
#include "ob_geo_wkb_check_visitor.h"

namespace oceanbase {
namespace common {


template<typename T>
int ObGeoWkbCheckVisitor::check_common_header(T *geo, ObGeoType geo_type, ObGeoWkbByteOrder bo)
{
  // [bo][type][num]
  int ret = OB_SUCCESS;
  if (wkb_.length() < pos_ + WKB_COMMON_WKB_HEADER_LEN) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invaid wkb geo", K(ret), K(wkb_.length()), K(pos_), K(geo_type));
  } else {
    bo = static_cast<ObGeoWkbByteOrder>(*(wkb_.ptr() + pos_));
    if ((ObGeoWkbByteOrder::BigEndian != bo && ObGeoWkbByteOrder::LittleEndian != bo)
        || (bo != bo_ && !lib::is_oracle_mode())) { // different byte order in wkb isn't supported in mysql mode
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid byte order", K(ret), K(pos_), K(geo_type), K(wkb_), K(bo), K(bo_));
    } else {
      pos_ += WKB_GEO_BO_SIZE;
      ObGeoType type = static_cast<ObGeoType>(ObGeoWkbByteOrderUtil::read<uint32_t>((wkb_.ptr() + pos_), bo));
      if (type != geo_type) {
        ret = OB_INVALID_DATA;
        LOG_WARN("invalid geo type", K(ret), K(pos_), K(type), K(geo_type), K(wkb_));
      } else {
        pos_ += WKB_GEO_TYPE_SIZE;
        bo_ = bo; // for bigendian, liitle endian mixed
      }
    }
  }
  return ret;
}

template<typename T>
int ObGeoWkbCheckVisitor::check_line_string(T *geo)
{
  // [bo][type][num][X][Y][...]
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_common_header(geo, ObGeoType::LINESTRING, bo_))) {
    LOG_WARN("invaid wkb linestring", K(ret), K(wkb_.length()), K(pos_), K(ObGeoType::LINESTRING));
  } else {
    uint32_t po_num = ObGeoWkbByteOrderUtil::read<uint32_t>((wkb_.ptr() + pos_), bo_);
    int32_t len = wkb_.length() - pos_;
    if ((po_num > MAX_N_POINTS) || (po_num < 2)) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid point number", K(ret), K(pos_), K(po_num), K(ObGeoType::LINESTRING), K(wkb_));
    } else if (po_num * WKB_POINT_DATA_SIZE > len) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid wkb length", K(ret), K(pos_), K(po_num), K(wkb_.length()), K(ObGeoType::LINESTRING));
    } else {
      pos_ += WKB_GEO_ELEMENT_NUM_SIZE;
      if (wkb_.length() < po_num * WKB_POINT_DATA_SIZE + pos_) {
        ret = OB_INVALID_DATA;
        LOG_WARN("invaid wkb linestring", K(ret), K(wkb_.length()), K(pos_), K(po_num), K(geo->type()));
      } else {
        pos_ += po_num * WKB_POINT_DATA_SIZE;
      }
    }
  }
  return ret;
}

template<typename T>
int ObGeoWkbCheckVisitor::check_ring(T *geo)
{
  // [num][X][Y][...]
  int ret = OB_SUCCESS;
  if (wkb_.length() < pos_ + WKB_GEO_ELEMENT_NUM_SIZE) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invaid wkb geo", K(ret), K(wkb_.length()), K(pos_));
  } else {
    uint32_t po_num = ObGeoWkbByteOrderUtil::read<uint32_t>((wkb_.ptr() + pos_), bo_);
    if (po_num > MAX_N_POINTS) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid point number of ring", K(ret), K(pos_), K(po_num), K(ObGeoType::LINESTRING), K(wkb_));
    } else if (po_num < 4) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid point number of ring", K(ret), K(pos_), K(po_num), K(ObGeoType::LINESTRING), K(wkb_));
    } else {
      pos_ += WKB_GEO_ELEMENT_NUM_SIZE;
      if (wkb_.length() < po_num * WKB_POINT_DATA_SIZE + pos_) {
        ret = OB_INVALID_DATA;
        LOG_WARN("invaid wkb ring", K(ret), K(wkb_.length()), K(pos_), K(po_num), K(geo->type()));
      } else if (memcmp(wkb_.ptr() + pos_, wkb_.ptr() + pos_ + (po_num - 1) * WKB_POINT_DATA_SIZE,
                        WKB_POINT_DATA_SIZE)) {
        is_ring_closed_ = false;
        if (need_check_ring_) {
          ret = OB_INVALID_DATA;
          LOG_WARN("wkb ring is not closed", K(ret), K(wkb_.length()), K(pos_), K(po_num), K(geo->type()));
        } else {
          pos_ += po_num * WKB_POINT_DATA_SIZE;
        }
      } else {
        pos_ += po_num * WKB_POINT_DATA_SIZE;
      }
    }
  }

  return ret;
}

template<typename T>
int ObGeoWkbCheckVisitor::check_point(T *geo)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_common_header(geo, ObGeoType::POINT, bo_))) {
    LOG_WARN("invaid wkb point", K(ret), K(wkb_), K(pos_));
  } else if (wkb_.length() < pos_ + WKB_POINT_DATA_SIZE) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invaid wkb point", K(ret), K(wkb_), K(pos_));
  } else {
    pos_ += WKB_POINT_DATA_SIZE;
  }
  return ret;
}

template<typename T>
int ObGeoWkbCheckVisitor::check_multipoint(T *geo)
{
  // [bo][type][num][point][...]
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_common_header(geo, ObGeoType::MULTIPOINT, bo_))) {
    LOG_WARN("invaid wkb linestring", K(ret), K(wkb_.length()), K(pos_), K(ObGeoType::MULTIPOINT));
  } else {
    uint32_t po_num = ObGeoWkbByteOrderUtil::read<uint32_t>((wkb_.ptr() + pos_), bo_);
    uint32_t len = wkb_.length() - pos_;
    if (po_num > MAX_MULIT_POINTS || po_num < 1) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid point number of multi_point", K(ret), K(pos_), K(po_num), K(wkb_));
    } else if (len < po_num * (WKB_GEO_BO_SIZE + WKB_GEO_TYPE_SIZE + WKB_POINT_DATA_SIZE)) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid wkb length", K(ret), K(pos_), K(po_num), K(wkb_.length()), K(ObGeoType::MULTIPOINT));
    } else {
      pos_ += WKB_GEO_ELEMENT_NUM_SIZE;
      for (uint32_t i = 0; i < po_num && OB_SUCC(ret); i++) {
        if (OB_FAIL(check_point(geo))) {
          LOG_WARN("invalid wkb point", K(ret), K(i), K(po_num));
        }
      }
    }
  }
  return ret;
}

template<typename T>
int ObGeoWkbCheckVisitor::check_geometrycollection(T *geo)
{
  // [bo][type][num][line][...]
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_common_header(geo, ObGeoType::GEOMETRYCOLLECTION, bo_))) {
    LOG_WARN("invaid wkb geo", K(ret), K(wkb_), K(pos_), K(ObGeoType::GEOMETRYCOLLECTION));
  } else {
    pos_ += WKB_GEO_ELEMENT_NUM_SIZE;
  }
  return ret;
}

template<typename T>
int ObGeoWkbCheckVisitor::check_multi_geo(T *geo, ObGeoType geo_type)
{
  // [bo][type][num][line][...]
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_common_header(geo, geo_type, bo_))) {
    LOG_WARN("invaid wkb geo", K(ret), K(wkb_), K(pos_), K(geo_type));
  } else {
    uint32_t geo_num = ObGeoWkbByteOrderUtil::read<uint32_t>((wkb_.ptr() + pos_), bo_);
    uint32_t len = wkb_.length() - pos_;
    if (geo_num < 1) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invaid geo number", K(ret), K(wkb_), K(pos_), K(geo_num), K(geo_type));
    } else if (len < geo_num * (WKB_GEO_BO_SIZE + WKB_GEO_TYPE_SIZE + WKB_POINT_DATA_SIZE)) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid wkb length", K(ret), K(pos_), K(geo_num), K(wkb_.length()), K(geo_type));
    }
    pos_ += WKB_GEO_ELEMENT_NUM_SIZE;
  }
  return ret;
}

int ObGeoWkbCheckVisitor::visit(ObIWkbPoint *geo)
{
  // [bo][type][X][Y]
  return check_point(geo);
}

int ObGeoWkbCheckVisitor::visit(ObIWkbGeomMultiPoint *geo)
{
  // [num][point][...]
  return check_multipoint(geo);
}

int ObGeoWkbCheckVisitor::visit(ObIWkbGeogMultiPoint *geo)
{
  // [num][linestring][...]
  return check_multipoint(geo);
}

int ObGeoWkbCheckVisitor::visit(ObIWkbGeomLineString *geo)
{
  // [num][X][Y][...]
  return check_line_string(geo);
}

int ObGeoWkbCheckVisitor::visit(ObIWkbGeogLineString *geo)
{
  // [num][X][Y][...]
  return check_line_string(geo);
}

int ObGeoWkbCheckVisitor::visit(ObIWkbGeomMultiLineString *geo)
{
  // [num][linestring][...]
  return check_multi_geo(geo, ObGeoType::MULTILINESTRING);
}

int ObGeoWkbCheckVisitor::visit(ObIWkbGeogMultiLineString *geo)
{
  // [num][linestring][...]
  return check_multi_geo(geo, ObGeoType::MULTILINESTRING);
}

int ObGeoWkbCheckVisitor::visit(ObIWkbGeomLinearRing *geo)
{
  // [num][X][Y][...]
  return check_ring(geo);
}

int ObGeoWkbCheckVisitor::visit(ObIWkbGeogLinearRing *geo)
{
  // [num][X][Y][...]
  return check_ring(geo);
}

int ObGeoWkbCheckVisitor::visit(ObIWkbGeomPolygon *geo)
{
  // [num][ex][inner_rings]
  return check_multi_geo(geo, ObGeoType::POLYGON);
}

int ObGeoWkbCheckVisitor::visit(ObIWkbGeogPolygon *geo)
{
  // [num][ex][inner_rings]
  return check_multi_geo(geo, ObGeoType::POLYGON);
}

int ObGeoWkbCheckVisitor::visit(ObIWkbGeomMultiPolygon *geo)
{
  // [num][poly][...]
  return check_multi_geo(geo, ObGeoType::MULTIPOLYGON);
}

int ObGeoWkbCheckVisitor::visit(ObIWkbGeogMultiPolygon *geo)
{
  // [num][poly][...]
  return check_multi_geo(geo, ObGeoType::MULTIPOLYGON);
}

int ObGeoWkbCheckVisitor::visit(ObIWkbGeomCollection *geo)
{
  // [num][wkb_geo][...]
  return check_geometrycollection(geo);
}

int ObGeoWkbCheckVisitor::visit(ObIWkbGeogCollection *geo)
{
  // [num][wkb_geo][...]
  return check_geometrycollection(geo);
}

} // namespace common
} // namespace oceanbase