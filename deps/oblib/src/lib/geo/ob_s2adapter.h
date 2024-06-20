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

#ifndef OCEANBASE_LIB_GEO_OB_S2ADAPTER_
#define OCEANBASE_LIB_GEO_OB_S2ADAPTER_

#include "lib/utility/ob_unify_serialize.h"
#include "lib/container/ob_vector.h" // for ObVector
#include "s2/s2cell.h"
#include "s2/s2cap.h"
#include "s2/s2latlng.h"
#include "s2/s2loop.h"
#include "s2/s2polyline.h"
#include "s2/s2polygon.h"
#include "s2/s2region_coverer.h"
#include "s2/s2latlng_rect.h"
#include "lib/geo/ob_geo_to_s2_visitor.h"
#include "lib/geo/ob_geo_common.h"


namespace oceanbase {
namespace common {

struct ObSrsBoundsItem;
typedef common::ObVector<uint64_t> ObS2Cellids;
static const int64_t OB_DEFAULT_MBR_SIZE = 32;


class ObSpatialMBR
{
  OB_UNIS_VERSION(4);
public:
  ObSpatialMBR()
      : x_min_(NAN),
        x_max_(NAN),
        y_min_(NAN),
        y_max_(NAN) {};
  ObSpatialMBR(ObDomainOpType rel_type)
      : x_min_(NAN),
        x_max_(NAN),
        y_min_(NAN),
        y_max_(NAN),
        mbr_type_(rel_type),
        is_point_(false),
        is_geog_(false) {}
  ObSpatialMBR(double x_min, double x_max, double y_min, double y_max, ObDomainOpType rel_type)
      : x_min_(x_min),
        x_max_(x_max),
        y_min_(y_min),
        y_max_(y_max),
        mbr_type_(rel_type),
        is_point_(false),
        is_geog_(false) {}
  ~ObSpatialMBR() {};
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    databuff_printf(buf,
        buf_len,
        pos,
        "x_min_=%lf, x_max_=%lf, y_min_=%lf, y_max_=%lf, mbr_type_=%d",
        x_min_, x_max_, y_min_, y_max_, static_cast<int>(mbr_type_));
    return pos;
  }
  int to_char(char *buf, int64_t &buf_len) const;
  static int from_string(ObString &mbr_str,
                         ObDomainOpType type,
                         ObSpatialMBR &spa_mbr,
                         bool is_point = false);
  int filter(const ObSpatialMBR &other, ObDomainOpType type, bool &pass_through) const;
  OB_INLINE bool is_point() const { return is_point_; };
  OB_INLINE bool is_geog() const { return is_geog_; };
  OB_INLINE ObDomainOpType get_type() const { return mbr_type_; };
  OB_INLINE double get_xmin() const { return x_min_; };
  OB_INLINE double get_xmax() const { return x_max_; };
  OB_INLINE double get_ymin() const { return y_min_; };
  OB_INLINE double get_ymax() const { return y_max_; };
  OB_INLINE bool is_empty() const { return std::isnan(x_min_) && std::isnan(x_max_)
                                        && std::isnan(y_min_) && std::isnan(y_max_); };
public:
  int generate_latlng_rect(S2LatLngRect &rect) const;
  int generate_box(ObCartesianBox &rect) const;
  double x_min_;
  double x_max_;
  double y_min_;
  double y_max_;
  ObDomainOpType mbr_type_;
  bool is_point_;
  bool is_geog_;
};

class ObS2Adapter final
{
public:
  ObS2Adapter(ObIAllocator *allocator, bool is_geog, bool is_query_window = false)
    : allocator_(allocator),
      visitor_(NULL),
      geo_(NULL),
      is_geog_(is_geog),
      need_buffer_(false),
      distance_()
  {
    if (!is_query_window) {
      options_.set_max_cells(OB_GEO_S2REGION_OPTION_MAX_CELL);
      options_.set_max_level(OB_GEO_S2REGION_OPTION_MAX_LEVEL);
      options_.set_level_mod(OB_GEO_S2REGION_OPTION_LEVEL_MOD);
    } else {
      options_.set_max_cells(50);
      options_.set_max_level(30);
      options_.set_level_mod(OB_GEO_S2REGION_OPTION_LEVEL_MOD);
    }
  }
  ObS2Adapter(ObIAllocator *allocator, bool is_geog, double distance)
    : allocator_(allocator),
      visitor_(NULL),
      geo_(NULL),
      is_geog_(is_geog),
      need_buffer_(true),
      distance_(S1Angle::Radians(distance))
  {
    options_.set_max_cells(OB_GEO_S2REGION_OPTION_MAX_CELL);
    options_.set_max_level(OB_GEO_S2REGION_OPTION_MAX_LEVEL);
    options_.set_level_mod(OB_GEO_S2REGION_OPTION_LEVEL_MOD);
  }

  ~ObS2Adapter();
  static void get_child_of_cellid(uint64_t id, uint64_t &child_start, uint64_t &child_end);
  int64_t get_ancestors(uint64_t cell, ObS2Cellids &cells);
  int64_t init(const ObString &wkb, const ObSrsBoundsItem *bound = NULL);
  int64_t get_cellids(ObS2Cellids &cells, bool is_query);
  int64_t get_cellids_and_unrepeated_ancestors(ObS2Cellids &cells, ObS2Cellids &ancestors);
  int64_t get_inner_cover_cellids(ObS2Cellids &cells);
  int64_t get_mbr(ObSpatialMBR &mbr);
private:
  S2RegionCoverer::Options options_;
  ObIAllocator *allocator_;
  ObWkbToS2Visitor *visitor_;
  ObGeometry *geo_;
  bool is_geog_;
  bool need_buffer_;
  S1Angle distance_;
  DISALLOW_COPY_AND_ASSIGN(ObS2Adapter);
};
} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_LIB_OB_S2ADAPTER_
