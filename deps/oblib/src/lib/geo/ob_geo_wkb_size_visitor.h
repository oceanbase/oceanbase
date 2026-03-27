/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LIB_GEO_OB_GEO_WKB_SIZE_VISITOR_
#define OCEANBASE_LIB_GEO_OB_GEO_WKB_SIZE_VISITOR_
#include "lib/geo/ob_geo_visitor.h"

namespace oceanbase
{
namespace common
{

class ObGeoWkbSizeVisitor : public ObEmptyGeoVisitor
{
public:
  ObGeoWkbSizeVisitor() : geo_size_(0) {}
  virtual ~ObGeoWkbSizeVisitor() {}
  uint64_t geo_size() { return geo_size_; }
  void reset() { geo_size_ = 0; }
  // wkb
  bool prepare(ObIWkbGeometry *geo) override;
  int visit(ObIWkbGeometry *geo) override;

  // tree
  bool prepare(ObGeometry *geo) override { UNUSED(geo); return true;}
  int visit(ObPoint *geo) override;
  int visit(ObLineString *geo) override;
  int visit(ObLinearring *geo) override;
  int visit(ObPolygon *geo) override;
  int visit(ObGeometrycollection *geo) override;
  bool is_end(ObPoint *geo) override {UNUSED(geo); return true; }
  bool is_end(ObLineString *geo) override {UNUSED(geo); return true; }
  bool is_end(ObLinearring *geo) override {UNUSED(geo); return true; }

private:
  uint64_t geo_size_;

};

} // namespace common
} // namespace oceanbase

#endif