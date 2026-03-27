/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LIB_OB_GEO_FUNC_DISTANCE_SPHERE_
#define OCEANBASE_LIB_OB_GEO_FUNC_DISTANCE_SPHERE_

#include "lib/geo/ob_geo_func_common.h"

namespace oceanbase
{
namespace common
{

class ObGeoFuncDistanceSphereUtil
{
public:
  template <typename GeoType1, typename GeoType2>
  static int eval(const GeoType1 *g1,
                  const GeoType2 *g2,
                  const common::ObGeoEvalCtx &context,
                  double &result);
  template <typename GeoType1, typename GeoType2>
  static int eval(const common::ObGeometry *g1,
                  const common::ObGeometry *g2,
                  const common::ObGeoEvalCtx &context,
                  double &result);
  static int reinterpret_as_degrees(const common::ObWkbGeomPoint *cart_pt,
                                    common::ObWkbGeogPoint &geog_pt,
                                    double &result);
  static int reinterpret_as_degrees(common::ObIAllocator *allocator,
                                    const common::ObGeometry *g,
                                    const common::ObWkbGeogMultiPoint *&geog_mpt,
                                    double &result);
private:
  static int reinterpret_as_degrees(double lon_deg,
                                    double lat_deg,
                                    double &x,
                                    double &y,
                                    double &result);
};

class ObGeoFuncDistanceSphere
{
public:
  ObGeoFuncDistanceSphere();
  virtual ~ObGeoFuncDistanceSphere() = default;
  static int eval(const common::ObGeoEvalCtx &gis_context, double &result);
};

} // sql
} // oceanbase
#endif // OCEANBASE_LIB_OB_GEO_FUNC_DISTANCE_SPHERE_