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

#ifndef OCEANBASE_LIB_GEO_OB_GEO_TO_S2_VISITOR_
#define OCEANBASE_LIB_GEO_OB_GEO_TO_S2_VISITOR_
#include "lib/container/ob_vector.h"
#include "lib/geo/ob_geo_visitor.h"
#include "s2/s2earth.h"
#include "s2/s2cell.h"
#include "s2/s2cap.h"
#include "s2/s2latlng.h"
#include "s2/s2loop.h"
#include "s2/s2polyline.h"
#include "s2/s2polygon.h"
#include "s2/s2region_coverer.h"
#include "s2/s2latlng_rect.h"
#include "s2/s2latlng_rect_bounder.h"

#include <memory>

namespace oceanbase
{
namespace common
{
typedef common::ObVector<uint64_t> ObS2Cellids;
struct ObSrsBoundsItem;
const uint64_t exceedsBoundsCellID = 0xFFFFFFFFFFFFFFFF;
const int32_t OB_GEO_S2REGION_OPTION_MAX_CELL = 4;
const int32_t OB_GEO_S2REGION_OPTION_MAX_LEVEL = 30;
const int32_t OB_GEO_S2REGION_OPTION_LEVEL_MOD = 1;

class ObWkbToS2Visitor : public ObEmptyGeoVisitor
{
public:
  ObWkbToS2Visitor(const ObSrsBoundsItem *bound, S2RegionCoverer::Options options, bool is_geog)
    : bound_(bound),
      options_(options),
      is_geog_(is_geog),
      invalid_(false),
      has_reset_(false)
    {
      mbr_ = S2LatLngRect::Empty();
    }
  ~ObWkbToS2Visitor() { reset(); }
  template<typename T_IBIN>
  S2Cell* MakeS2Point(T_IBIN *geo);
  template<typename T_IBIN, typename T_BIN>
  S2Polyline* MakeS2Polyline(T_IBIN *geo);
  template<typename T_IBIN, typename T_BIN,
           typename T_BIN_RING, typename T_BIN_INNER_RING>
  S2Polygon* MakeS2Polygon(T_IBIN *geo);

  template<typename T_IBIN>
  S2Cell* MakeProjS2Point(T_IBIN *geo);
  template<typename T_IBIN, typename T_BIN>
  S2Polyline* MakeProjS2Polyline(T_IBIN *geo);
  template<typename T_IBIN, typename T_BIN,
           typename T_BIN_RING, typename T_BIN_INNER_RING>
  S2Polygon* MakeProjS2Polygon(T_IBIN *geo);

  bool prepare(ObGeometry *geo);
  // wkb
  int visit(ObIWkbGeometry *geo) { INIT_SUCC(ret); return ret; }
  int visit(ObIWkbGeogPoint *geo);
  int visit(ObIWkbGeomPoint *geo);
  int visit(ObIWkbGeogLineString *geo);
  int visit(ObIWkbGeomLineString *geo);
  int visit(ObIWkbGeogPolygon *geo);
  int visit(ObIWkbGeomPolygon *geo);

  bool is_end(ObIWkbGeogLineString *geo) { UNUSED(geo); return true; }
  bool is_end(ObIWkbGeomLineString *geo) { UNUSED(geo); return true; }
  bool is_end(ObIWkbGeogLinearRing *geo) { UNUSED(geo); return true; }
  bool is_end(ObIWkbGeomLinearRing *geo) { UNUSED(geo); return true; }
  bool is_end(ObIWkbGeogPolygon *geo) { UNUSED(geo); return true; }
  bool is_end(ObIWkbGeomPolygon *geo) { UNUSED(geo); return true; }

  int finish(ObIWkbGeogMultiLineString *geo) { UNUSED(geo); return OB_SUCCESS; }
  int finish(ObIWkbGeomMultiLineString *geo) { UNUSED(geo); return OB_SUCCESS; }
  int finish(ObIWkbGeogMultiPolygon *geo) { UNUSED(geo); return OB_SUCCESS; }
  int finish(ObIWkbGeomMultiPolygon *geo) { UNUSED(geo); return OB_SUCCESS; }
  int finish(ObIWkbGeogCollection *geo) { UNUSED(geo); return OB_SUCCESS; }
  int finish(ObIWkbGeomCollection *geo) { UNUSED(geo); return OB_SUCCESS; }

  int64_t get_cellids(ObS2Cellids &cells, bool is_query, bool need_buffer, S1Angle distance);
  int64_t get_inner_cover_cellids(ObS2Cellids &cells);
  int64_t get_mbr(S2LatLngRect &mbr, bool need_buffer, S1Angle distance);
  bool is_invalid() { return invalid_; }
  void reset();
private:
  double stToUV(double s);
  bool exceedsBounds(double x, double y);
  S2Point MakeS2PointFromXy(double x, double y);
  void add_cell_from_point(S2Point point);
  void add_cell_from_point(S2LatLng point);
  // S2对象内部使用了std::vector实现，在这里统一使用std::vector管理这些对象
  std::vector<std::unique_ptr<S2Region>> s2v_;
  S2LatLngRect mbr_;
  std::vector<S2CellId> S2cells_;
  const ObSrsBoundsItem *bound_;
  S2RegionCoverer::Options options_;
  bool is_geog_;
  bool invalid_;
  bool has_reset_;
  DISALLOW_COPY_AND_ASSIGN(ObWkbToS2Visitor);
};

} // namespace common
} // namespace oceanbase

#endif