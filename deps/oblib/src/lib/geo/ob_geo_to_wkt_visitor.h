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

#ifndef OCEANBASE_LIB_GEO_OB_GEO_TO_WKT_VISITOR_
#define OCEANBASE_LIB_GEO_OB_GEO_TO_WKT_VISITOR_
#include "lib/geo/ob_geo_visitor.h"


namespace oceanbase
{
namespace common
{

class ObGeoToWktVisitor : public ObEmptyGeoVisitor
{
public:
  static const int MAX_DIGITS_IN_DOUBLE = 25;
  explicit ObGeoToWktVisitor(ObIAllocator *allocator)
  : buffer_(allocator),
    has_scale_(false),
    in_multi_visit_(false),
    colloction_level_(0) {}
  ~ObGeoToWktVisitor() {}
  bool prepare(ObGeometry *geo) { UNUSED(geo); return true; }
  bool prepare(ObIWkbGeogMultiPoint *geo);
  bool prepare(ObIWkbGeomMultiPoint *geo);
  bool prepare(ObIWkbGeogMultiLineString *geo);
  bool prepare(ObIWkbGeomMultiLineString *geo);
  bool prepare(ObIWkbGeogMultiPolygon *geo);
  bool prepare(ObIWkbGeomMultiPolygon *geo);
  bool prepare(ObIWkbGeogCollection *geo);
  bool prepare(ObIWkbGeomCollection *geo);
  // wkb
  int visit(ObIWkbGeogPoint *geo);
  int visit(ObIWkbGeomPoint *geo);
  int visit(ObIWkbGeogLineString *geo);
  int visit(ObIWkbGeomLineString *geo);
  int visit(ObIWkbGeogMultiPoint *geo);
  int visit(ObIWkbGeomMultiPoint *geo);
  int visit(ObIWkbGeogMultiLineString *geo);
  int visit(ObIWkbGeomMultiLineString *geo);
  int visit(ObIWkbGeogPolygon *geo);
  int visit(ObIWkbGeomPolygon *geo);
  int visit(ObIWkbGeogMultiPolygon *geo);
  int visit(ObIWkbGeomMultiPolygon *geo);
  int visit(ObIWkbGeogCollection *geo);
  int visit(ObIWkbGeomCollection *geo);

  bool is_end(ObIWkbGeogLineString *geo) { UNUSED(geo); return true; }
  bool is_end(ObIWkbGeomLineString *geo) { UNUSED(geo); return true; }
  bool is_end(ObIWkbGeogLinearRing *geo) { UNUSED(geo); return true; }
  bool is_end(ObIWkbGeomLinearRing *geo) { UNUSED(geo); return true; }
  bool is_end(ObIWkbGeogPolygon *geo) { UNUSED(geo); return true; }
  bool is_end(ObIWkbGeomPolygon *geo) { UNUSED(geo); return true; }

  virtual int finish(ObIWkbGeogMultiPoint *geo) override;
  virtual int finish(ObIWkbGeomMultiPoint *geo) override;
  virtual int finish(ObIWkbGeogMultiLineString *geo) override;
  virtual int finish(ObIWkbGeomMultiLineString *geo) override;
  virtual int finish(ObIWkbGeogMultiPolygon *geo) override;
  virtual int finish(ObIWkbGeomMultiPolygon *geo) override;
  virtual int finish(ObIWkbGeogCollection *geo) override;
  virtual int finish(ObIWkbGeomCollection *geo) override;

  void get_wkt(ObString &wkt);
  int init(uint32_t srid, int64_t maxdecimaldigits);

private:
  template<typename T_IBIN>
  int appendPoint(T_IBIN *geo);
  template<typename T_IBIN, typename T_BIN>
  int appendLine(T_IBIN *geo);
  template<typename T_IBIN, typename T_BIN,
           typename T_BIN_RING, typename T_BIN_INNER_RING>
  int appendPolygon(T_IBIN *geo);
  int appendInnerPoint(double x, double y);
  int append_double_with_prec(char *buff, const int32_t buff_size, uint64_t &out_len, double value);

  template<typename T_IBIN>
  int appendMultiPrefix(T_IBIN *geo);
  int appendMultiSuffix();
  template<typename T_IBIN>
  int appendCollectionPrefix(T_IBIN *geo);
  template<typename T_IBIN>
  int appendCollectionSuffix(T_IBIN *geo);
  bool in_colloction_visit() { return colloction_level_ > 0; }

  ObGeoStringBuffer buffer_;
  bool has_scale_;
  int64_t scale_;
  bool in_multi_visit_;
  int colloction_level_;
  DISALLOW_COPY_AND_ASSIGN(ObGeoToWktVisitor);
};

} // namespace common
} // namespace oceanbase

#endif