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
#include "ob_wkb_byte_order_visitor.h"
#include "lib/ob_errno.h"

namespace oceanbase
{
namespace common
{
int ObWkbByteOrderVisitor::init(ObGeometry *geo)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(geo) || geo->length() < WKB_COMMON_WKB_HEADER_LEN) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("input geometry is invalid", K(ret), K(geo));
  } else {
    from_bo_ = static_cast<ObGeoWkbByteOrder>(*(geo->val()));
    if (from_bo_ == ObGeoWkbByteOrder::INVALID) {
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_WARN("input geometry's byte order is invalid", K(ret), K(from_bo_), K(to_bo_));
    }
  }
  return ret;
}

int ObWkbByteOrderVisitor::append_point(double x, double y)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(buffer_.append(x))) {
    LOG_WARN("failed to append point value x", K(ret), K(x));
  } else if (OB_FAIL(buffer_.append(y))) {
    LOG_WARN("failed to append point value y", K(ret), K(y));
  }
  return ret;
}

template<typename T>
int ObWkbByteOrderVisitor::append_head_info(T *geo, int reserve_len)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(buffer_.reserve(reserve_len))) {
    LOG_WARN("fail to alloc memory", K(ret), K(reserve_len));
  } else if (OB_FAIL(buffer_.append(static_cast<char>(to_bo_)))) {
    LOG_WARN("failed to append little endian", K(ret));
  } else if (OB_FAIL(buffer_.append(static_cast<uint32_t>(geo->type())))) {
    LOG_WARN("failed to append type", K(ret), K(geo->type()));
  } else if (OB_FAIL(buffer_.append(static_cast<uint32_t>(geo->size())))) {
    LOG_WARN("failed to append num value", K(ret), K(geo->size()));
  }
  return ret;
}

int ObWkbByteOrderVisitor::visit(ObIWkbPoint *geo)
{
  int ret = OB_SUCCESS;
  uint32_t reserve_len = EWKB_COMMON_WKB_HEADER_LEN + WKB_GEO_DOUBLE_STORED_SIZE * 2;
  if (OB_FAIL(init(geo))) {
    LOG_WARN("fail to init geometry", K(ret));
  } else if (OB_FAIL(buffer_.reserve(reserve_len))) {
    LOG_WARN("fail to alloc memory", K(ret), K(reserve_len));
  } else if (OB_FAIL(buffer_.append(static_cast<char>(to_bo_)))) {
    LOG_WARN("failed to append point little endian", K(ret));
  } else if (OB_FAIL(buffer_.append(static_cast<uint32_t>(geo->type())))) {
    LOG_WARN("failed to append point type", K(ret));
  } else if (OB_FAIL(append_point(geo->x(), geo->y()))) {
    LOG_WARN("failed to point value", K(ret), K(geo->x()), K(geo->y()));
  }
  return ret;
}

template<typename T_IBIN, typename T_BIN>
int ObWkbByteOrderVisitor::append_line(T_IBIN *geo)
{
  int ret = OB_SUCCESS;
  uint32_t reserve_len = WKB_COMMON_WKB_HEADER_LEN + geo->size() * 2 * WKB_GEO_DOUBLE_STORED_SIZE;
  if (OB_FAIL(append_head_info(geo, reserve_len))) {
    LOG_WARN("failed to append line string head info", K(ret));
  } else {
    const T_BIN *line = reinterpret_cast<const T_BIN *>(geo->val());
    typename T_BIN::iterator iter = line->begin();
    for (; OB_SUCC(ret) && iter != line->end(); iter++) {
      if (OB_FAIL(append_point(iter->template get<0>(from_bo_),
              iter->template get<1>(from_bo_)))) {
        LOG_WARN("failed to point value", K(ret));
      }
    }
  }
  return ret;
}

int ObWkbByteOrderVisitor::visit(ObIWkbGeomLineString *geo)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init(geo))) {
    LOG_WARN("fail to init geometry", K(ret));
  } else if (OB_FAIL((append_line<ObIWkbGeomLineString, ObWkbGeomLineString>(geo)))) {
    LOG_WARN("fail to append line", K(ret));
  }
  return ret;
}

int ObWkbByteOrderVisitor::visit(ObIWkbGeogLineString *geo)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init(geo))) {
    LOG_WARN("fail to init geometry", K(ret));
  } else if (OB_FAIL((append_line<ObIWkbGeogLineString, ObWkbGeogLineString>(geo)))) {
    LOG_WARN("fail to append line", K(ret));
  }
  return ret;
}

template<typename T_IBIN, typename T_BIN, typename T_BIN_RING, typename T_BIN_INNER_RING>
int ObWkbByteOrderVisitor::append_polygon(T_IBIN *geo)
{
  int ret = OB_SUCCESS;
  uint32_t reserve_len = 0;
  if (OB_FAIL(append_head_info(geo, WKB_COMMON_WKB_HEADER_LEN))) {
    LOG_WARN("failed to append head info", K(ret));
  } else {
    T_BIN &poly = *(T_BIN *)(geo->val());
    T_BIN_RING &exterior = poly.exterior_ring();
    if (poly.size() != 0) {
      uint32_t ext_num = exterior.size(from_bo_);
      reserve_len = WKB_GEO_ELEMENT_NUM_SIZE + ext_num * 2 * WKB_GEO_DOUBLE_STORED_SIZE;
      if (OB_FAIL(buffer_.reserve(reserve_len))) {
        LOG_WARN("fail to alloc memory", K(ret), K(reserve_len));
      } else if (OB_FAIL(buffer_.append(ext_num))) {
        LOG_WARN("fail to append ring size", K(ret));
      }
      typename T_BIN_RING::iterator iter = exterior.begin();
      for (; OB_SUCC(ret) && iter != exterior.end(from_bo_); ++iter) {
        if (OB_FAIL(append_point(iter->template get<0>(from_bo_),
                iter->template get<1>(from_bo_)))) {
          LOG_WARN("fail to appendInnerPoint", K(ret));
        }
      }
    }

    T_BIN_INNER_RING &inner_rings = poly.inner_rings();
    typename T_BIN_INNER_RING::iterator iterInnerRing = inner_rings.begin();
    for (; OB_SUCC(ret) && iterInnerRing != inner_rings.end(); ++iterInnerRing) {
      uint32_t inner_num = iterInnerRing->size(from_bo_);
      reserve_len = WKB_GEO_ELEMENT_NUM_SIZE + inner_num * 2 * WKB_GEO_DOUBLE_STORED_SIZE;
      if (OB_FAIL(buffer_.reserve(reserve_len))) {
        LOG_WARN("fail to alloc memory", K(ret), K(reserve_len));
      } else if (OB_FAIL(buffer_.append(inner_num))) {
        LOG_WARN("fail to append ring size", K(ret));
      }
      typename T_BIN_RING::iterator iter = (*iterInnerRing).begin();
      for (; OB_SUCC(ret) && iter != (*iterInnerRing).end(from_bo_); ++iter) {
        if (OB_FAIL(append_point(iter->template get<0>(from_bo_),
                iter->template get<1>(from_bo_)))) {
          LOG_WARN("fail to appendInnerPoint", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObWkbByteOrderVisitor::visit(ObIWkbGeogPolygon *geo)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init(geo))) {
    LOG_WARN("fail to init geometry", K(ret));
  } else if (OB_FAIL(
                 (append_polygon<ObIWkbGeogPolygon, ObWkbGeogPolygon, ObWkbGeogLinearRing, ObWkbGeogPolygonInnerRings>(
                     geo)))) {
    LOG_WARN("fail to append polygon", K(ret));
  }
  return ret;
}

int ObWkbByteOrderVisitor::visit(ObIWkbGeomPolygon *geo)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init(geo))) {
    LOG_WARN("fail to init geometry", K(ret));
  } else if (OB_FAIL(
                 (append_polygon<ObIWkbGeomPolygon, ObWkbGeomPolygon, ObWkbGeomLinearRing, ObWkbGeomPolygonInnerRings>(
                     geo)))) {
    LOG_WARN("fail to append polygon", K(ret));
  }
  return ret;
}

template<typename T_IBIN, typename T_BIN, typename T_IPOINT, typename T_POINT>
int ObWkbByteOrderVisitor::append_multipoint(T_IBIN *geo)
{
  int ret = OB_SUCCESS;
  uint32_t size = geo->size();
  uint32_t reserve_len = size * (EWKB_COMMON_WKB_HEADER_LEN + WKB_GEO_DOUBLE_STORED_SIZE * 2);
  if (OB_FAIL(buffer_.reserve(reserve_len))) {
    LOG_WARN("fail to alloc memory", K(ret), K(reserve_len));
  } else {
    char *ptr = reinterpret_cast<char*>(geo->val());
    uint32_t sz = geo->size();
    for (uint32_t i = 0; i < sz; ++i) {
      uint64_t offset = WKB_COMMON_WKB_HEADER_LEN;
      T_POINT p;
      offset += i * p.length();
      ObString data(sizeof(T_POINT), (ptr + offset));
      T_IPOINT point;
      point.set_data(data);
      if (OB_FAIL(visit(&point))) {
        LOG_WARN("fail to do point visit", K(ret));
      }
    }
  }
  return ret;
}

int ObWkbByteOrderVisitor::visit(ObIWkbGeogMultiPoint *geo)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init(geo))) {
    LOG_WARN("fail to init geometry", K(ret));
  } else if (OB_FAIL(append_head_info(geo, WKB_COMMON_WKB_HEADER_LEN))) {
    LOG_WARN("failed to append head info", K(ret));
  } else if (OB_FAIL((append_multipoint<ObIWkbGeogMultiPoint,
                 ObWkbGeogMultiPoint,
                 ObIWkbGeogPoint,
                 ObWkbGeogPoint>(geo)))) {
    LOG_WARN("fail to append multipoint", K(ret));
  }
  return ret;
}

int ObWkbByteOrderVisitor::visit(ObIWkbGeomMultiPoint *geo)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init(geo))) {
    LOG_WARN("fail to init geometry", K(ret));
  } else if (OB_FAIL(append_head_info(geo, WKB_COMMON_WKB_HEADER_LEN))) {
    LOG_WARN("failed to append head info", K(ret));
  } else if (OB_FAIL((append_multipoint<ObIWkbGeomMultiPoint,
                 ObWkbGeomMultiPoint,
                 ObIWkbGeomPoint,
                 ObWkbGeomPoint>(geo)))) {
    LOG_WARN("fail to append multipoint", K(ret));
  }
  return ret;
}

int ObWkbByteOrderVisitor::visit(ObIWkbGeogMultiLineString *geo)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init(geo))) {
    LOG_WARN("fail to init geometry", K(ret));
  } else if (OB_FAIL(append_head_info(geo, WKB_COMMON_WKB_HEADER_LEN))) {
    LOG_WARN("failed to append head info", K(ret));
  }
  return ret;
}

int ObWkbByteOrderVisitor::visit(ObIWkbGeomMultiLineString *geo)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init(geo))) {
    LOG_WARN("fail to init geometry", K(ret));
  } else if (OB_FAIL(append_head_info(geo, WKB_COMMON_WKB_HEADER_LEN))) {
    LOG_WARN("failed to append head info", K(ret));
  }
  return ret;
}

int ObWkbByteOrderVisitor::visit(ObIWkbGeogMultiPolygon *geo)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init(geo))) {
    LOG_WARN("fail to init geometry", K(ret));
  } else if (OB_FAIL(append_head_info(geo, WKB_COMMON_WKB_HEADER_LEN))) {
    LOG_WARN("failed to append head info", K(ret));
  }
  return ret;
}

int ObWkbByteOrderVisitor::visit(ObIWkbGeomMultiPolygon *geo)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init(geo))) {
    LOG_WARN("fail to init geometry", K(ret));
  } else if (OB_FAIL(append_head_info(geo, WKB_COMMON_WKB_HEADER_LEN))) {
    LOG_WARN("failed to append head info", K(ret));
  }
  return ret;
}

int ObWkbByteOrderVisitor::visit(ObIWkbGeogCollection *geo)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init(geo))) {
    LOG_WARN("fail to init geometry", K(ret));
  } else if (OB_FAIL(append_head_info(geo, WKB_COMMON_WKB_HEADER_LEN))) {
    LOG_WARN("failed to append head info", K(ret));
  }
  return ret;
}

int ObWkbByteOrderVisitor::visit(ObIWkbGeomCollection *geo)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init(geo))) {
    LOG_WARN("fail to init geometry", K(ret));
  } else if (OB_FAIL(append_head_info(geo, WKB_COMMON_WKB_HEADER_LEN))) {
    LOG_WARN("failed to append head info", K(ret));
  }
  return ret;
}

}  // namespace common
}  // namespace oceanbase