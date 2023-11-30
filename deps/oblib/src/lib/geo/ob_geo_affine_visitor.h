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
#ifndef OCEANBASE_LIB_GEO_OB_GEO_AFFINE_VISITOR_
#define OCEANBASE_LIB_GEO_OB_GEO_AFFINE_VISITOR_
#include "lib/geo/ob_geo_visitor.h"
#include "lib/geo/ob_geo_utils.h"
namespace oceanbase
{
namespace common
{

class ObGeoAffineVisitor : public ObEmptyGeoVisitor
{
public:
  explicit ObGeoAffineVisitor(const ObAffineMatrix *affine) : affine_(affine)
  {}
  virtual ~ObGeoAffineVisitor()
  {}
  bool prepare(ObGeometry *geo);
  int visit(ObCartesianPoint *geo);
  int visit(ObGeographPoint *geo);
  int visit(ObGeometry *geo)
  {
    UNUSED(geo);
    return OB_SUCCESS;
  }
  template<typename PtType>
  void affine(PtType *point);

private:
  const ObAffineMatrix *affine_;
  DISALLOW_COPY_AND_ASSIGN(ObGeoAffineVisitor);
};

}  // namespace common
}  // namespace oceanbase

#endif  // OCEANBASE_LIB_GEO_OB_GEO_AFFINE_VISITOR_