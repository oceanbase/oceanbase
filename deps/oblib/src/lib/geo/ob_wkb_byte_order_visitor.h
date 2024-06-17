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

#ifndef OCEANBASE_LIB_GEO_OB_WKB_BYTE_ORDER_VISITOR_
#define OCEANBASE_LIB_GEO_OB_WKB_BYTE_ORDER_VISITOR_
#include "lib/geo/ob_geo_visitor.h"
#include "lib/geo/ob_geo_common.h"

namespace oceanbase
{
namespace common
{
// All BIG ENDIAN wkb needs to go through this visitor transformation before being used
class ObWkbByteOrderVisitor : public ObEmptyGeoVisitor
{
public:
  ObWkbByteOrderVisitor(ObIAllocator *allocator, ObGeoWkbByteOrder to_bo)
      : buffer_(*allocator, to_bo), to_bo_(to_bo), from_bo_(ObGeoWkbByteOrder::INVALID)
  {}

  virtual ~ObWkbByteOrderVisitor()
  {}

  bool prepare(ObGeometry *geo)
  {
    UNUSED(geo);
    return true;
  }

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

  const ObString get_wkb()
  {
    return buffer_.string();
  }

private:
  int init(ObGeometry *geo);
  int append_point(double x, double y);
  template<typename T>
  int append_head_info(T *geo, int reserve_len);
  template<typename T_IBIN, typename T_BIN>
  int append_line(T_IBIN *geo);
  template<typename T_IBIN, typename T_BIN, typename T_BIN_RING, typename T_BIN_INNER_RING>
  int append_polygon(T_IBIN *geo);
  template<typename T_IBIN, typename T_BIN, typename T_IPOINT, typename T_POINT>
  int append_multipoint(T_IBIN *geo);

  ObWkbBuffer buffer_;
  ObGeoWkbByteOrder to_bo_;
  ObGeoWkbByteOrder from_bo_;
  DISALLOW_COPY_AND_ASSIGN(ObWkbByteOrderVisitor);
};
}  // namespace common
}  // namespace oceanbase

#endif