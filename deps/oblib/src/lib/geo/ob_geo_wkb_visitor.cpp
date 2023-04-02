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
#include "ob_geo_wkb_visitor.h"


namespace oceanbase {
namespace common {

template<typename T>
int ObGeoWkbVisitor::write_head_info(T *geo)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(write_to_buffer( ObGeoWkbByteOrder::LittleEndian, 1))) {
    LOG_WARN("failed to write little endian", K(ret));
  } else if (OB_FAIL(write_to_buffer(geo->type(), sizeof(uint32_t)))) {
    LOG_WARN("failed to write type", K(ret));
  } else if (OB_FAIL(write_to_buffer(geo->size(), sizeof(uint32_t)))) {
    LOG_WARN("failed to write num value", K(ret));
  }
  return ret;
}

int ObGeoWkbVisitor::write_cartesian_point(double x, double y)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(write_to_buffer(x, sizeof(double)))) {
    LOG_WARN("failed to write x value", K(ret), K(x));
  } else if (OB_FAIL(write_to_buffer(y, sizeof(double)))) {
    LOG_WARN("failed to write y value", K(ret), K(y));
  }
  return ret;
}

int ObGeoWkbVisitor::write_geograph_point(double x, double y)
{
  int ret = OB_SUCCESS;
  double val_x = x;
  double val_y = y;
  if (OB_FAIL(need_convert_ && srs_->longtitude_convert_from_radians(x, val_x))) {
    LOG_WARN("failed to convert from longtitude value", K(ret), K(x));
  } else if (OB_FAIL(write_to_buffer(val_x, sizeof(double)))) {
    LOG_WARN("failed to write point x value", K(ret), K(val_x));
  } else if (need_convert_ && OB_FAIL(srs_->latitude_convert_from_radians(y, val_y))) {
    LOG_WARN("failed to convert from latitude value", K(ret), K(y));
  } else if (OB_FAIL(write_to_buffer(val_y, sizeof(double)))) {
    LOG_WARN("failed to write point y value", K(ret), K(val_y));
  }
  return ret;
}

int ObGeoWkbVisitor::visit(ObPoint *geo)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(write_to_buffer(ObGeoWkbByteOrder::LittleEndian, 1))) {
    LOG_WARN("failed to write point little endian", K(ret));
  } else if (OB_FAIL(write_to_buffer(geo->type(), sizeof(uint32_t)))) {
    LOG_WARN("failed to write point type", K(ret));
  } else if (srs_ == NULL || srs_->srs_type() == ObSrsType::PROJECTED_SRS) {
    if (OB_FAIL(write_cartesian_point(geo->x(), geo->y()))) {
      LOG_WARN("failed to cartesian point value", K(ret));
    }
  } else if (OB_FAIL(write_geograph_point(geo->x(), geo->y()))) {
    LOG_WARN("failed to geograph point value", K(ret));
  }
  return ret;
}

int ObGeoWkbVisitor::visit(ObCartesianLineString *geo)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(write_head_info(geo))) {
    LOG_WARN("failed to write line string head info", K(ret));
  } else {
    for (uint32_t i = 0; i < geo->size() && OB_SUCC(ret); i++) {
      if (OB_FAIL(write_cartesian_point((*geo)[i].get<0>(), (*geo)[i].get<1>()))) {
        LOG_WARN("failed to cartesian point value", K(ret), K(i));
      }
    }
  }
  return ret;
}

int ObGeoWkbVisitor::visit(ObGeographLineString *geo)
{
  int ret = OB_SUCCESS;
  if (srs_ == NULL || srs_->srs_type() == ObSrsType::PROJECTED_SRS) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid srs info", K(ret), K(srs_));
  } else if (OB_FAIL(write_head_info(geo))) {
    LOG_WARN("failed to write line string head info", K(ret));
  } else {
    for (uint32_t i = 0; i < geo->size() && OB_SUCC(ret); i++) {
      if (OB_FAIL(write_geograph_point((*geo)[i].get<0>(), (*geo)[i].get<1>()))) {
        LOG_WARN("failed to geograph point value", K(ret), K(i));
      }
    }
  }
  return ret;
}

int ObGeoWkbVisitor::visit(ObGeographPolygon *geo)
{
  int ret = OB_SUCCESS;
  if (srs_ == NULL || srs_->srs_type() == ObSrsType::PROJECTED_SRS) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid srs info", K(ret), K(srs_));
  } else if (OB_FAIL(write_head_info(geo))) {
    LOG_WARN("failed to write polygon head info", K(ret));
  } else {
    const ObGeographLinearring& ring = geo->exterior_ring();
    if (OB_FAIL(write_to_buffer(ring.size(), sizeof(uint32_t)))) {
      LOG_WARN("failed to write num value", K(ret));
    }
    for (uint32_t i = 0; i < ring.size() && OB_SUCC(ret); i++) {
      if (OB_FAIL(write_geograph_point(ring[i].get<0>(), ring[i].get<1>()))) {
        LOG_WARN("failed to geograph point value", K(ret), K(i));
      }
    }
    for (uint32_t i = 0; i < geo->inner_ring_size() && OB_SUCC(ret); i++) {
      const ObGeographLinearring& inner_ring = geo->inner_ring(i);
      if (OB_FAIL(write_to_buffer(inner_ring.size(), sizeof(uint32_t)))) {
        LOG_WARN("failed to write num value", K(ret));
      }
      for (uint32_t j = 0; j < inner_ring.size() && OB_SUCC(ret); j++) {
        if (OB_FAIL(write_geograph_point(inner_ring[j].get<0>(), inner_ring[j].get<1>()))) {
          LOG_WARN("failed to geograph point value", K(ret), K(i), K(j));
        }
      }
    }
  }
  return ret;
}

int ObGeoWkbVisitor::visit(ObCartesianPolygon *geo)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(write_head_info(geo))) {
    LOG_WARN("failed to write polygon head info", K(ret));
  } else {
    const ObCartesianLinearring& ring = geo->exterior_ring();
    if (OB_FAIL(write_to_buffer(ring.size(), sizeof(uint32_t)))) {
      LOG_WARN("failed to write num value", K(ret));
    }
    for (uint32_t i = 0; i < ring.size() && OB_SUCC(ret); i++) {
      if (OB_FAIL(write_cartesian_point(ring[i].get<0>(), ring[i].get<1>()))) {
        LOG_WARN("failed to cartesian point value", K(ret), K(i));
      }
    }
    for (uint32_t i = 0; i < geo->inner_ring_size() && OB_SUCC(ret); i++) {
      const ObCartesianLinearring& inner_ring = geo->inner_ring(i);
      if (OB_FAIL(write_to_buffer(inner_ring.size(), sizeof(uint32_t)))) {
        LOG_WARN("failed to write num value", K(ret), K(i));
      }
      for (uint32_t j = 0; j < inner_ring.size() && OB_SUCC(ret); j++) {
        if (OB_FAIL(write_cartesian_point(inner_ring[j].get<0>(), inner_ring[j].get<1>()))) {
          LOG_WARN("failed to cartesian point value", K(ret), K(i), K(j));
        }
      }
    }
  }
  return ret;
}

int ObGeoWkbVisitor::visit(ObGeometrycollection *geo)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(write_head_info(geo))) {
    LOG_WARN("failed to write geograph multi point head info", K(ret));
  }
  return ret;
}

int ObGeoWkbVisitor::visit(ObIWkbGeometry *geo)
{
  int ret = OB_SUCCESS;
  const char *wkb_no_srid = geo->val();
  uint32_t wkb_no_srid_len = geo->length();
  if (buffer_->write(wkb_no_srid, wkb_no_srid_len) != wkb_no_srid_len) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("failed to write buffer", K(ret));
  }
  return ret;

}

} // namespace common
} // namespace oceanbase