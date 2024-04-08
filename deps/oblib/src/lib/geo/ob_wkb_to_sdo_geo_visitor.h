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

#ifndef OCEANBASE_LIB_GEO_OB_WKB_TO_SDO_GEO_VISITOR_
#define OCEANBASE_LIB_GEO_OB_WKB_TO_SDO_GEO_VISITOR_

#include "lib/geo/ob_geo_visitor.h"
#include "lib/geo/ob_sdo_geo_object.h"

namespace oceanbase
{
namespace common
{
class ObWkbToSdoGeoVisitor : public ObEmptyGeoVisitor
{
public:
  explicit ObWkbToSdoGeoVisitor()
      : is_multi_visit_(false), is_collection_visit_(false)
  {}
  ~ObWkbToSdoGeoVisitor()
  {}
  int init(ObSdoGeoObject *geo, uint32_t srid = UINT32_MAX);

  bool prepare(ObIWkbGeomLineString *geo);
  bool prepare(ObIWkbGeomPolygon *geo);
  bool prepare(ObIWkbGeomMultiPoint *geo);
  bool prepare(ObIWkbGeomMultiLineString *geo);
  bool prepare(ObIWkbGeomMultiPolygon *geo);
  bool prepare(ObIWkbGeomCollection *geo);
  bool prepare(ObIWkbGeogLineString *geo);
  bool prepare(ObIWkbGeogPolygon *geo);
  bool prepare(ObIWkbGeogMultiPoint *geo);
  bool prepare(ObIWkbGeogMultiLineString *geo);
  bool prepare(ObIWkbGeogMultiPolygon *geo);
  bool prepare(ObIWkbGeogCollection *geo);

  int visit(ObIWkbGeomPoint *geo);
  int visit(ObIWkbGeomLineString *geo);
  int visit(ObIWkbGeomPolygon *geo);
  int visit(ObIWkbGeomMultiPoint *geo);
  int visit(ObIWkbGeomMultiLineString *geo);
  int visit(ObIWkbGeomMultiPolygon *geo);
  int visit(ObIWkbGeomCollection *geo);
  int visit(ObIWkbGeogPoint *geo);
  int visit(ObIWkbGeogLineString *geo);
  int visit(ObIWkbGeogPolygon *geo);
  int visit(ObIWkbGeogMultiPoint *geo);
  int visit(ObIWkbGeogMultiLineString *geo);
  int visit(ObIWkbGeogMultiPolygon *geo);
  int visit(ObIWkbGeogCollection *geo);

  virtual int finish(ObIWkbGeomMultiPoint *geo) override;
  virtual int finish(ObIWkbGeomMultiLineString *geo) override;
  virtual int finish(ObIWkbGeomMultiPolygon *geo) override;
  virtual int finish(ObIWkbGeomCollection *geo) override;
  virtual int finish(ObIWkbGeogMultiPoint *geo) override;
  virtual int finish(ObIWkbGeogMultiLineString *geo) override;
  virtual int finish(ObIWkbGeogMultiPolygon *geo) override;
  virtual int finish(ObIWkbGeogCollection *geo) override;

  bool is_end(ObIWkbGeomLineString *geo) { UNUSED(geo); return true; }
  bool is_end(ObIWkbGeomLinearRing *geo) { UNUSED(geo); return true; }
  bool is_end(ObIWkbGeomPolygon *geo) { UNUSED(geo); return true; }
  bool is_end(ObIWkbGeogLineString *geo) { UNUSED(geo); return true; }
  bool is_end(ObIWkbGeogLinearRing *geo) { UNUSED(geo); return true; }
  bool is_end(ObIWkbGeogPolygon *geo) { UNUSED(geo); return true; }

  // void get_sdo_geo(ObSdoGeoObject *geo);

private:
  static constexpr double MAX_NUMBER_ORACLE = 1e+126;
  static constexpr double MIN_NUMBER_ORACLE = 1e-130;

  template<typename T_IBIN>
  int append_point(T_IBIN *geo);
  template<typename T_IBIN, typename T_BIN>
  int append_linestring(T_IBIN *geo);
  template<typename T_IBIN, typename T_BIN, typename T_BIN_RING, typename T_BIN_INNER_RING>
  int append_polygon(T_IBIN *geo);
  template<typename T_IBIN>
  void append_gtype(T_IBIN *geo);
  int append_inner_point(double x, double y);
  int append_elem_info(uint64_t offset, uint64_t etype, uint64_t interpretation);
  bool is_valid_to_represent(double num);

  bool is_multi_visit_;
  bool is_collection_visit_;
  ObSdoGeoObject *sdo_geo_;
};
}  // namespace common
}  // namespace oceanbase

#endif