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
#ifndef OCEANBASE_LIB_GEO_OB_GEO_CHECK_EMPTY_VISITOR_
#define OCEANBASE_LIB_GEO_OB_GEO_CHECK_EMPTY_VISITOR_
#include "lib/geo/ob_geo_visitor.h"
#include "lib/geo/ob_srs_info.h"
namespace oceanbase
{
namespace common
{

// ST_GEOMFROMTEXT('GEOMETRYCOLLECTION(GEOMETRYCOLLECTION())')) is empty
class ObGeoCheckEmptyVisitor : public ObEmptyGeoVisitor
{
public:
  ObGeoCheckEmptyVisitor() : is_empty_(true) {}
  virtual ~ObGeoCheckEmptyVisitor() {}
  bool prepare(ObGeometry *geo) { return (OB_NOT_NULL(geo)); }
  int visit(ObIWkbPoint *geo) { return check_empty(geo); }
  int visit(ObPoint *geo) { return check_empty(geo); }
  int visit(ObGeometry *geo) { UNUSED(geo); return OB_SUCCESS; }
  // stop when found first non-empty point
  bool is_end(ObGeometry *geo) { UNUSED(geo); return (is_empty_ == false); }
  bool get_result() { return is_empty_; };
  bool set_after_visitor() { return false; }

private:
  int check_empty(ObIWkbPoint *geo) { is_empty_ = geo->is_empty(); return OB_SUCCESS; }
  int check_empty(ObPoint *geo) { is_empty_ = geo->is_empty(); return OB_SUCCESS; }

private:
  bool is_empty_;

  DISALLOW_COPY_AND_ASSIGN(ObGeoCheckEmptyVisitor);
};

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_LIB_GEO_OB_GEO_CHECK_EMPTY_VISITOR_