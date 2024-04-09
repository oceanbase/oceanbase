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
 * This file contains implementation for ob_geo_func_register.
 */

#ifndef OCEANBASE_LIB_OB_GEO_FUNC_REGISTER_
#define OCEANBASE_LIB_OB_GEO_FUNC_REGISTER_

#include "lib/geo/ob_geo_dispatcher.h"
#include "lib/geo/ob_geo_func_area.h"
#include "lib/geo/ob_geo_func_correct.h"
#include "lib/geo/ob_geo_func_intersects.h"
#include "lib/geo/ob_geo_func_difference.h"
#include "lib/geo/ob_geo_func_disjoint.h"
#include "lib/geo/ob_geo_func_union.h"
#include "lib/geo/ob_geo_func_transform.h"
#include "lib/geo/ob_geo_func_covered_by.h"
#include "lib/geo/ob_geo_func_box.h"
#include "lib/geo/ob_geo_func_buffer.h"
#include "lib/geo/ob_geo_func_distance.h"
#include "lib/geo/ob_geo_func_isvalid.h"
#include "lib/geo/ob_geo_func_distance_sphere.h"
#include "lib/geo/ob_geo_func_within.h"
#include "lib/geo/ob_geo_func_equals.h"
#include "lib/geo/ob_geo_func_touches.h"
#include "lib/geo/ob_geo_func_centroid.h"
#include "lib/geo/ob_geo_func_crosses.h"
#include "lib/geo/ob_geo_func_overlaps.h"
#include "lib/geo/ob_geo_func_length.h"
#include "lib/geo/ob_geo_func_symdifference.h"
#include "lib/geo/ob_geo_func_dissolve_polygon.h"

namespace oceanbase
{
namespace common
{

// register geometry function adapters, only boost::geometry functions currently
// Add a new adapter:
// 1. implement adapter class and adapter ctx class
// 2. register the adapter here.
enum class ObGeoFuncType
{
  NotImplemented = 0,
  Area = 1,
  Correct = 2,
  Intersects = 3,
  Difference = 4,
  Disjoint = 5,
  Union = 6,
  Transform = 7,
  CoveredBy = 8,
  Box = 9,
  Buffer = 10,
  Distance = 11,
  IsValid = 12,
  DistanceSphere = 13,
  Within = 14,
  Equals = 15,
  Touches = 16,
  Centroid = 17,
  Crosses = 18,
  Overlaps = 19,
  Length = 20,
  SymDifference = 21,
  DissolvePolygon = 22,
  ObGisFuncTypeMax
};
class ObGeoFuncNotImplemented
{
public:
  ObGeoFuncNotImplemented();
  virtual ~ObGeoFuncNotImplemented() = default;
  static int eval(const common::ObGeoEvalCtx &gis_context, int32_t &result) {
    UNUSEDx(gis_context, result);
    return common::OB_ERR_GIS_INVALID_DATA;
  };
};

template <ObGeoFuncType func_type>
struct ObGeoFunc
{
  typedef ObGeoFuncNotImplemented geo_func;
};

template <>
struct ObGeoFunc<ObGeoFuncType::Area>
{
  typedef ObGeoFuncArea geo_func;
};

template <>
struct ObGeoFunc<ObGeoFuncType::Correct>
{
  typedef ObGeoFuncCorrect geo_func;
};

template <>
struct ObGeoFunc<ObGeoFuncType::Intersects>
{
  typedef ObGeoFuncIntersects geo_func;
};

template <>
struct ObGeoFunc<ObGeoFuncType::Difference>
{
  typedef ObGeoFuncDifference geo_func;
};

template <>
struct ObGeoFunc<ObGeoFuncType::Disjoint>
{
  typedef ObGeoFuncDisjoint geo_func;
};

template <>
struct ObGeoFunc<ObGeoFuncType::Union>
{
  typedef ObGeoFuncUnion geo_func;
};

template <>
struct ObGeoFunc<ObGeoFuncType::Transform>
{
  typedef ObGeoFuncTransform geo_func;
};

template <>
struct ObGeoFunc<ObGeoFuncType::CoveredBy>
{
  typedef ObGeoFuncCoveredBy geo_func;
};

template <>
struct ObGeoFunc<ObGeoFuncType::Box>
{
  typedef ObGeoFuncBox geo_func;
};

template <>
struct ObGeoFunc<ObGeoFuncType::Buffer>
{
  typedef ObGeoFuncBuffer geo_func;
};

template <>
struct ObGeoFunc<ObGeoFuncType::Distance>
{
  typedef ObGeoFuncDistance geo_func;
};

template <>
struct ObGeoFunc<ObGeoFuncType::IsValid>
{
  typedef ObGeoFuncIsValid geo_func;
};

template <>
struct ObGeoFunc<ObGeoFuncType::DistanceSphere>
{
  typedef ObGeoFuncDistanceSphere gis_func;
};

template <>
struct ObGeoFunc<ObGeoFuncType::Within>
{
  typedef ObGeoFuncWithin gis_func;
};

template <>
struct ObGeoFunc<ObGeoFuncType::Equals>
{
  typedef ObGeoFuncEquals geo_func;
};

template <>
struct ObGeoFunc<ObGeoFuncType::Touches>
{
  typedef ObGeoFuncTouches geo_func;
};

template <>
struct ObGeoFunc<ObGeoFuncType::Centroid>
{
  typedef ObGeoFuncCentroid geo_func;
};

template <>
struct ObGeoFunc<ObGeoFuncType::Crosses>
{
  typedef ObGeoFuncCrosses geo_func;
};

template <>
struct ObGeoFunc<ObGeoFuncType::Overlaps>
{
  typedef ObGeoFuncOverlaps geo_func;
};

template <>
struct ObGeoFunc<ObGeoFuncType::Length>
{
  typedef ObGeoFuncLength geo_func;
};

template <>
struct ObGeoFunc<ObGeoFuncType::SymDifference>
{
  typedef ObGeoFuncSymDifference geo_func;
};

template <>
struct ObGeoFunc<ObGeoFuncType::DissolvePolygon>
{
  typedef ObGeoFuncDissolvePolygon geo_func;
};

} // sql
} // oceanbase
#endif // OCEANBASE_LIB_OB_GEO_FUNC_REGISTER_
