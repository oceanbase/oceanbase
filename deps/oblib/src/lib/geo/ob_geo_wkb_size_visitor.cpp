/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX LIB
#include "ob_geo_wkb_size_visitor.h"


namespace oceanbase {
namespace common {

bool ObGeoWkbSizeVisitor::prepare(ObIWkbGeometry *geo)
{
  geo_size_ += geo->length();
  return false;
}

int ObGeoWkbSizeVisitor::visit(ObIWkbGeometry *geo)
{
  geo_size_ += geo->length();
  return OB_SUCCESS;
}

int ObGeoWkbSizeVisitor::visit(ObPoint *geo)
{
  // [bo][type][X][Y]
  UNUSED(geo);
  geo_size_ += sizeof(uint8_t) + sizeof(uint32_t)
               + 2 * sizeof(double);
  return OB_SUCCESS;
}

int ObGeoWkbSizeVisitor::visit(ObLineString *geo)
{
  // [bo][type][num][X][Y][...]
  geo_size_ += WKB_COMMON_WKB_HEADER_LEN + geo->size() * 2 * sizeof(double);
  return OB_SUCCESS;
}

int ObGeoWkbSizeVisitor::visit(ObLinearring *geo)
{
  // [num][X][Y][...]
  geo_size_ += sizeof(uint32_t) + geo->size() * 2 * sizeof(double);
  return OB_SUCCESS;
}

int ObGeoWkbSizeVisitor::visit(ObPolygon *geo)
{
  // [bo][type][num][ex][inner_rings]
  UNUSED(geo);
  geo_size_ += WKB_COMMON_WKB_HEADER_LEN;
  return OB_SUCCESS;
}

int ObGeoWkbSizeVisitor::visit(ObGeometrycollection *geo)
{
  // [bo][type][num][ex][inner_rings]
  UNUSED(geo);
  geo_size_ += WKB_COMMON_WKB_HEADER_LEN;
  return OB_SUCCESS;
}

} // namespace common
} // namespace oceanbase