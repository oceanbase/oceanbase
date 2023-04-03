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