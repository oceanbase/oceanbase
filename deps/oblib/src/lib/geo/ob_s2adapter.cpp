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
 * This file contains implementation support for the s2 geometry adapter abstraction.
 */

#define USING_LOG_PREFIX LIB
#include "ob_s2adapter.h"
#include "lib/geo/ob_geo_func_utils.h"
#include "lib/geo/ob_geo_func_envelope.h"
#include "lib/geo/ob_geo_utils.h"
#include "lib/geo/ob_geo_bin.h"
#include "lib/geo/ob_geo_ibin.h"

#include <vector>
#include <memory>
namespace oceanbase {
namespace common {

int ObSpatialMBR::filter(const ObSpatialMBR &other, ObGeoRelationType type, bool &pass_through) const
{
  INIT_SUCC(ret);
  if (is_geog_) {
    S2LatLngRect this_rect;
    S2LatLngRect other_rect;
    if (OB_FAIL(generate_latlng_rect(this_rect))) {
      LOG_WARN("fail to generate this latlng rectangle", K(ret));
    } else if (OB_FAIL(other.generate_latlng_rect(other_rect))) {
      LOG_WARN("fail to generate other latlng rectangle", K(ret));
    } else {
      switch (type) {
        case ObGeoRelationType::T_COVERS: {
          pass_through = !other_rect.Contains(this_rect);
          break;
        }

        case ObGeoRelationType::T_DWITHIN:
        case ObGeoRelationType::T_INTERSECTS: {
          pass_through = !this_rect.Intersects(other_rect);
          break;
        }

        case ObGeoRelationType::T_COVEREDBY: {
          pass_through = !this_rect.Contains(other_rect);
          break;
        }

        case ObGeoRelationType::T_DFULLYWITHIN: {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not support within geo relation type", K(ret), K(type));
          break;
        }

        default: {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("undefined geo relation type", K(ret), K(type));
          break;
        }
      }
    }
  } else {
    ObCartesianBox this_rect;
    ObCartesianBox other_rect;
    if (OB_FAIL(generate_box(this_rect))) {
      LOG_WARN("fail to generate this latlng rectangle", K(ret));
    } else if (OB_FAIL(other.generate_box(other_rect))) {
      LOG_WARN("fail to generate other latlng rectangle", K(ret));
    } else {
      switch (type) {
        case ObGeoRelationType::T_COVERS: {
          pass_through = !other_rect.Contains(this_rect);
          break;
        }

        case ObGeoRelationType::T_DWITHIN:
        case ObGeoRelationType::T_INTERSECTS: {
          pass_through = !this_rect.Intersects(other_rect);
          break;
        }

        case ObGeoRelationType::T_COVEREDBY: {
          pass_through = !this_rect.Contains(other_rect);
          break;
        }

        case ObGeoRelationType::T_DFULLYWITHIN: {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not support within geo relation type", K(ret), K(type));
          break;
        }

        default: {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("undefined geo relation type", K(ret), K(type));
          break;
        }
      }
    }
  }

  return ret;
}

int ObSpatialMBR::to_char(char *buf, int64_t &buf_len) const
{
  INIT_SUCC(ret);
  int32_t pos = 0;
  if (is_point_) {
    MEMCPY(buf + pos, reinterpret_cast<const char *>(&x_min_), sizeof(x_min_));
    pos += sizeof(double);
    MEMCPY(buf + pos, reinterpret_cast<const char *>(&y_min_), sizeof(y_min_));
    pos += sizeof(double);
  } else {
    MEMCPY(buf + pos, reinterpret_cast<const char *>(&y_min_), sizeof(y_min_));
    pos += sizeof(double);
    MEMCPY(buf + pos, reinterpret_cast<const char *>(&y_max_), sizeof(y_max_));
    pos += sizeof(double);
    MEMCPY(buf + pos, reinterpret_cast<const char *>(&x_min_), sizeof(x_min_));
    pos += sizeof(double);
    MEMCPY(buf + pos, reinterpret_cast<const char *>(&x_max_), sizeof(x_max_));
    pos += sizeof(double);
  }
  buf_len = pos;
  return ret;
}

int ObSpatialMBR::from_string(ObString &mbr_str,
                              ObGeoRelationType type,
                              ObSpatialMBR &spa_mbr,
                              bool is_point)
{
  INIT_SUCC(ret);

  const char *data = mbr_str.ptr();
  if (mbr_str.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("mbr string is empty", K(ret), K(mbr_str));
  } else if (is_point) {
    double x_min = *reinterpret_cast<const double*>(data); // lng_lo
    data += sizeof(double);
    double y_min = *reinterpret_cast<const double*>(data); // lat_lo
    new (&spa_mbr) ObSpatialMBR(x_min, x_min, y_min, y_min, type);
  } else {
    double y_min = *reinterpret_cast<const double*>(data); // lat_lo
    data += sizeof(double);
    double y_max = *reinterpret_cast<const double*>(data); // lat_hi
    data += sizeof(double);
    double x_min = *reinterpret_cast<const double*>(data); // lng_lo
    data += sizeof(double);
    double x_max = *reinterpret_cast<const double*>(data); // lng_hi
    new (&spa_mbr) ObSpatialMBR(x_min, x_max, y_min, y_max, type);
  }

  return ret;
}

int ObSpatialMBR::generate_latlng_rect(S2LatLngRect &rect) const
{
  INIT_SUCC(ret);
  S1Angle lat_lo = S1Angle::Degrees(y_min_);
  S1Angle lat_hi = S1Angle::Degrees(y_max_);
  S1Angle lng_lo = S1Angle::Degrees(x_min_);
  S1Angle lng_hi = S1Angle::Degrees(x_max_);
  S2LatLng lo(lat_lo, lng_lo);
  S2LatLng hi(lat_hi, lng_hi);
  new (&rect) S2LatLngRect(lo, hi);
  return ret;
}

int ObSpatialMBR::generate_box(ObCartesianBox &rect) const
{
  INIT_SUCC(ret);
  ObWkbGeomInnerPoint min_point(x_min_, y_min_);
  ObWkbGeomInnerPoint max_point(x_max_, y_max_);
  new (&rect) ObCartesianBox(min_point, max_point);
  return ret;
}

OB_DEF_SERIALIZE(ObSpatialMBR)
{
  INIT_SUCC(ret);
  OB_UNIS_ENCODE(y_min_);
  OB_UNIS_ENCODE(y_max_);
  OB_UNIS_ENCODE(x_min_);
  OB_UNIS_ENCODE(x_max_);
  OB_UNIS_ENCODE(static_cast<int64_t>(mbr_type_));
  OB_UNIS_ENCODE(is_point_);
  OB_UNIS_ENCODE(is_geog_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObSpatialMBR)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(y_min_);
  OB_UNIS_ADD_LEN(y_max_);
  OB_UNIS_ADD_LEN(x_min_);
  OB_UNIS_ADD_LEN(x_max_);
  OB_UNIS_ADD_LEN(static_cast<int64_t>(mbr_type_));
  OB_UNIS_ADD_LEN(is_point_);
  OB_UNIS_ADD_LEN(is_geog_);
  return len;
}

OB_DEF_DESERIALIZE(ObSpatialMBR)
{
  INIT_SUCC(ret);
  int64_t mbr_type = 0;
  OB_UNIS_DECODE(y_min_);
  OB_UNIS_DECODE(y_max_);
  OB_UNIS_DECODE(x_min_);
  OB_UNIS_DECODE(x_max_);
  OB_UNIS_DECODE(mbr_type);
  if (OB_SUCC(ret)) {
    mbr_type_ = static_cast<ObGeoRelationType>(mbr_type);
  }
  OB_UNIS_DECODE(is_point_);
  OB_UNIS_DECODE(is_geog_);
  return ret;
}

void ObS2Adapter::get_child_of_cellid(uint64_t id, uint64_t &child_start, uint64_t &child_end)
{
  S2CellId parent(id);
  child_start = parent.range_min().id();
  child_end = parent.range_max().id();
}

int64_t ObS2Adapter::get_cellids(ObS2Cellids &cells, bool is_query)
{
  INIT_SUCC(ret);
  if(OB_FAIL(visitor_->get_cellids(cells, is_query, need_buffer_, distance_))) {
    LOG_WARN("fail to get cellid from visitor", K(ret));
  }
  return ret;
}

int64_t ObS2Adapter::get_inner_cover_cellids(ObS2Cellids &cells)
{
  INIT_SUCC(ret);
  if(OB_FAIL(visitor_->get_inner_cover_cellids(cells))) {
    LOG_WARN("fail to get cellid from visitor", K(ret));
  }
  return ret;
}

int64_t ObS2Adapter::get_ancestors(uint64_t cell, ObS2Cellids &cells)
{
  INIT_SUCC(ret);
  S2CellId cellid(cell);
  int level = cellid.level();
  while (cellid.is_valid() && OB_SUCC(ret) && (level -= options_.level_mod()) >= options_.min_level()) {
    S2CellId ancestor_id = cellid.parent(level);
    if (OB_FAIL(cells.push_back(ancestor_id.id()))) {
      LOG_WARN("fail to push_back cellid", K(ret));
    }
  }
  return ret;
}

int64_t ObS2Adapter::get_mbr(ObSpatialMBR &mbr)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(geo_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("input geo is null", K(ret));
  } else {
    mbr.is_geog_ = is_geog_;
    mbr.is_point_ = (geo_->type() == ObGeoType::POINT);
    if (is_geog_) {
      S2LatLngRect rect;
      if (OB_FAIL(visitor_->get_mbr(rect, need_buffer_, distance_))) {
        LOG_WARN("fail to get cellid from visitor", K(ret));
      } else {
        mbr.y_min_ = rect.lat_lo().degrees();
        mbr.y_max_ = rect.lat_hi().degrees();
        mbr.x_min_ = rect.lng_lo().degrees();
        mbr.x_max_ = rect.lng_hi().degrees();
      }
    } else {
      ObCartesianBox box;
      ObArenaAllocator tmp_allocator;
      ObGeoEvalCtx gis_context(&tmp_allocator, NULL);
      if (OB_FAIL(gis_context.append_geo_arg(geo_))) {
        LOG_WARN("build gis context failed", K(ret), K(gis_context.get_geo_count()));
      } else if (OB_FAIL(ObGeoFuncEnvelope::eval(gis_context, box))) {
        LOG_WARN("get mbr box failed", K(ret));
      } else if (box.is_empty()) {
        LOG_DEBUG("It's might be empty geometry collection", K(geo_->type()), K(geo_->is_empty()));
      } else {
        mbr.x_min_ = box.min_corner().get<0>();
        mbr.y_min_ = box.min_corner().get<1>();
        mbr.x_max_ = box.max_corner().get<0>();
        mbr.y_max_ = box.max_corner().get<1>();
      }
    }
  }
  return ret;
}

int64_t ObS2Adapter::init(const ObString &swkb, const ObSrsBoundsItem *bound)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(visitor_)) {
   if (OB_ISNULL(visitor_ = new ObWkbToS2Visitor(bound, options_, is_geog_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", K(ret));
    } else if (OB_ISNULL(swkb.ptr())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("input swkb is empty", K(ret));
    } else {
      ObGeoType type = ObGeoType::GEOTYPEMAX;
      ObGeometry *geo = NULL;
      ObString wkb;
      uint32_t offset;
      if (OB_FAIL(ObGeoTypeUtil::get_type_from_wkb(swkb, type))) {
        LOG_WARN("fail to get geo type by swkb", K(ret));
      } else if (OB_FAIL(ObGeoTypeUtil::create_geo_by_type(*allocator_, type, is_geog_, true, geo))) {
        LOG_WARN("fail to create_geo_by_type by swkb", K(ret));
      } else if (OB_FAIL(ObGeoTypeUtil::get_wkb_from_swkb(swkb, wkb, offset))) {
        LOG_WARN("fail to get wkb from swkb", K(ret), K(swkb));
      } else {
        geo->set_data(wkb);
        geo_ = geo;
        if (OB_FAIL(geo->do_visit(*visitor_))) {
          LOG_WARN("fail to do_visit by ObWkbToS2Visitor", K(ret));
        } else if (visitor_->is_invalid()) {
          // 1. get valid geo inside bounds
          ObGeometry *corrected_geo = NULL;
          bool need_do_visit = true;
          if (OB_FAIL(ObGeoTypeUtil::get_mbr_polygon(*allocator_, bound, *geo, corrected_geo))) {
            if (ret == OB_EMPTY_RESULT) {
              ret = OB_SUCCESS;
              need_do_visit = false;
            } else {
              LOG_WARN("fail to do_visit by ObWkbToS2Visitor", K(ret));
            }
          }

          if (OB_FAIL(ret)) {
          } else if (need_do_visit) {
            // 2. reset visitor_
            visitor_->reset();
            // 3. do_visit again
            if (OB_FAIL(corrected_geo->do_visit(*visitor_))) {
              LOG_WARN("fail to do_visit by ObWkbToS2Visitor", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

ObS2Adapter::~ObS2Adapter()
{
  delete visitor_;
}

} // namespace common
} // namespace oceanbase