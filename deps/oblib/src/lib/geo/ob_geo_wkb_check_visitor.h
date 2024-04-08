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
#ifndef OCEANBASE_LIB_GEO_OB_GEO_WKB_CHECK_VISITOR_
#define OCEANBASE_LIB_GEO_OB_GEO_WKB_CHECK_VISITOR_
#include "lib/geo/ob_geo_visitor.h"
#include "lib/geo/ob_srs_info.h"
namespace oceanbase
{
namespace common
{

class ObGeoWkbCheckVisitor : public ObEmptyGeoVisitor
{
public:
  ObGeoWkbCheckVisitor(ObString wkb, ObGeoWkbByteOrder bo)
    : wkb_(wkb),
      bo_(bo),
      pos_(0),
      need_check_ring_(true),
      is_ring_closed_(true) {}
  ObGeoWkbCheckVisitor(ObString wkb, ObGeoWkbByteOrder bo, bool need_check_ring)
    : wkb_(wkb),
      bo_(bo),
      pos_(0),
      need_check_ring_(need_check_ring),
      is_ring_closed_(true) {}
  virtual ~ObGeoWkbCheckVisitor() {}

  bool prepare(ObGeometry *geo) override { UNUSED(geo); return true; }
  int visit(ObIWkbPoint *geo) override;
  int visit(ObIWkbGeomMultiPoint *geo) override;
  int visit(ObIWkbGeogMultiPoint *geo) override;
  int visit(ObIWkbGeomLineString *geo) override;
  int visit(ObIWkbGeogLineString *geo) override;
  int visit(ObIWkbGeomMultiLineString *geo) override;
  int visit(ObIWkbGeogMultiLineString *geo) override;
  int visit(ObIWkbGeomLinearRing *geo) override;
  int visit(ObIWkbGeogLinearRing *geo) override;
  int visit(ObIWkbGeomPolygon *geo) override;
  int visit(ObIWkbGeogPolygon *geo) override;
  int visit(ObIWkbGeomMultiPolygon *geo) override;
  int visit(ObIWkbGeogMultiPolygon *geo) override;
  int visit(ObIWkbGeomCollection *geo) override;
  int visit(ObIWkbGeogCollection *geo) override;

  bool is_end(ObIWkbGeomLineString *geo) override { UNUSED(geo); return true; }
  bool is_end(ObIWkbGeogLineString *geo) override { UNUSED(geo); return true; }
  bool is_end(ObIWkbGeomMultiPoint *geo) override { UNUSED(geo); return true; }
  bool is_end(ObIWkbGeogMultiPoint *geo) override { UNUSED(geo); return true; }
  bool is_end(ObIWkbGeomLinearRing *geo) override { UNUSED(geo); return true; }
  bool is_end(ObIWkbGeogLinearRing *geo) override { UNUSED(geo); return true; }
  inline bool is_ring_closed() { return is_ring_closed_; }

private:
  static const uint32_t MAX_N_POINTS = (uint32_t)(UINT_MAX32 -  WKB_COMMON_WKB_HEADER_LEN) / WKB_POINT_DATA_SIZE;
  static const uint32_t MAX_MULIT_POINTS = (uint32_t)(UINT_MAX32 - WKB_COMMON_WKB_HEADER_LEN) /
                                                   (WKB_GEO_BO_SIZE + WKB_GEO_TYPE_SIZE + WKB_POINT_DATA_SIZE);

  ObString wkb_;
  ObGeoWkbByteOrder bo_;
  uint64_t pos_;
  bool need_check_ring_;
  bool is_ring_closed_;

  template<typename T>
  int check_common_header(T *geo, ObGeoType geo_type, ObGeoWkbByteOrder bo);
  template<typename T>
  int check_point(T *geo);
  template<typename T>
  int check_line_string(T *geo);
  template<typename T>
  int check_ring(T *geo);
  template<typename T>
  int check_geometrycollection(T *geo);
  template<typename T>
  int check_multi_geo(T *geo, ObGeoType geo_type);
  template<typename T>
  int check_multipoint(T *geo);

  DISALLOW_COPY_AND_ASSIGN(ObGeoWkbCheckVisitor);
};

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_LIB_GEO_OB_GEO_WKB_CHECK_VISITOR_