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

#ifndef OCEANBASE_LIB_GEO_OB_GEO_WKB_POINTN_VISITOR_
#define OCEANBASE_LIB_GEO_OB_GEO_WKB_POINTN_VISITOR_
#include "lib/geo/ob_geo_visitor.h"

namespace oceanbase
{
namespace common
{

class ObGeoPointNVisitor : public ObEmptyGeoVisitor
{
public:
  ObGeoPointNVisitor(ObIAllocator *allocator, int position)
    : allocator_(allocator), point_(NULL), position_(position) {}

  template<typename T>
  int alloc_point_obj(T *&obj);

  ObGeometry *get_point() const { return point_; }

  template<typename T_POINT, typename T_TREE, typename T_IBIN, typename T_BIN>
  int search_pointn(T_IBIN *geo_ibin);

  bool prepare(ObGeometry *geo) override { UNUSED(geo); return true; }
  // wkb
  int visit(ObIWkbGeogLineString *geo) override;
  int visit(ObIWkbGeomLineString *geo) override;

  bool is_end(ObIWkbGeogLineString *geo) override { UNUSED(geo); return true; }
  bool is_end(ObIWkbGeomLineString *geo) override { UNUSED(geo); return true; }

private:
  ObIAllocator *allocator_;
  ObGeometry *point_;
  int position_;
  DISALLOW_COPY_AND_ASSIGN(ObGeoPointNVisitor);
};

} // namespace common
} // namespace oceanbase

#endif