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

#ifndef OCEANBASE_LIB_GEO_OB_GEO_WKB_VISITOR_
#define OCEANBASE_LIB_GEO_OB_GEO_WKB_VISITOR_
#include "lib/geo/ob_geo_visitor.h"
#include "lib/geo/ob_srs_info.h"

namespace oceanbase
{
namespace common
{

class ObGeoWkbVisitor : public ObEmptyGeoVisitor
{
public:
  // need_convert: Configure whether conversion is required when srs type is GEOGRAPHIC_SRS.
  ObGeoWkbVisitor(const ObSrsItem *srs, ObString *buf, bool need_convert = true)
      : srs_(srs),
        buffer_(buf),
        need_convert_(need_convert)
  {}
  virtual ~ObGeoWkbVisitor() {}
  bool prepare(ObGeometry *geo) override { UNUSED(geo); return true; }

  // wkb
  int visit(ObIWkbGeometry *geo) override;
  bool is_end(ObIWkbGeometry *geo) override { UNUSED(geo); return true; }

  // tree
  int visit(ObPoint *geo) override;
  int visit(ObCartesianLineString *geo) override;
  int visit(ObGeographLineString *geo) override;
  int visit(ObGeographPolygon *geo) override;
  int visit(ObCartesianPolygon *geo) override;

  int visit(ObGeometrycollection *geo) override;

  bool is_end(ObCartesianLineString *geo) override { UNUSED(geo); return true; }
  bool is_end(ObGeographLineString *geo) override { UNUSED(geo); return true; }
  bool is_end(ObGeographPolygon *geo) override { UNUSED(geo); return true; }
  bool is_end(ObCartesianPolygon *geo) override { UNUSED(geo); return true; }

  template<typename T>
  int write_to_buffer(T data, uint64_t len)
  {
    int ret = OB_SUCCESS;
    if (buffer_->write(reinterpret_cast<char *>(&data), len) != len) {
      ret = OB_BUF_NOT_ENOUGH;
      OB_LOG(WARN, "failed to write buffer", K(ret));
    }
    return ret;
  }

  template<typename T>
  int write_head_info(T *geo);

  int write_cartesian_point(double x, double y);
  int write_geograph_point(double x, double y);

  void set_wkb_buffer(ObString *res) { buffer_ = res; }
  void set_srs(const ObSrsItem *srs) { srs_ = srs; }
private:
  const ObSrsItem *srs_;
  ObString *buffer_;
  bool need_convert_;
  uint32_t curr_pos_;
  DISALLOW_COPY_AND_ASSIGN(ObGeoWkbVisitor);

};

} // namespace common
} // namespace oceanbase

#endif