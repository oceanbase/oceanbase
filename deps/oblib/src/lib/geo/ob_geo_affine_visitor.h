/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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