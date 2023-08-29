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
 * This file contains implementation for ob_geo_expr_utils.
 */

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/expr/ob_geo_expr_utils.h"
#include "observer/omt/ob_tenant_srs.h"
#include "lib/geo/ob_geo_func_register.h"
#include "lib/geo/ob_geo_coordinate_range_visitor.h"
#include "lib/geo/ob_geo_wkb_size_visitor.h"
#include "lib/geo/ob_geo_wkb_visitor.h"
#include "lib/geo/ob_geo_to_tree_visitor.h"
#include "lib/geo/ob_geo_normalize_visitor.h"
#include "lib/geo/ob_geo_wkb_check_visitor.h"
#include "lib/geo/ob_geo_denormalize_visitor.h"
#include "lib/geo/ob_geo_check_empty_visitor.h"
#include "lib/geo/ob_geo_latlong_check_visitor.h"
#include "lib/geo/ob_geo_zoom_in_visitor.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace sql
{

int ObGeoExprUtils::get_srs_item(uint64_t tenant_id,
                                 omt::ObSrsCacheGuard &srs_guard,
                                 const uint32_t srid,
                                 const ObSrsItem *&srs)
{
  int ret = OB_SUCCESS;

  if (0 != srid && OB_FAIL(OTSRS_MGR->get_tenant_srs_guard(srs_guard))) {
    LOG_WARN("fail to get srs guard", K(ret), K(tenant_id));
  } else if (0 != srid && OB_FAIL(srs_guard.get_srs_item(srid, srs))) {
    LOG_WARN("fail to get srs", K(ret), K(srid));
  }

  return ret;
}

int ObGeoExprUtils::get_srs_item(ObEvalCtx &ctx,
                                 omt::ObSrsCacheGuard &srs_guard,
                                 const ObString &wkb,
                                 const ObSrsItem *&srs,
                                 bool use_little_bo, /* = false */
                                 const char *func_name /* = NULL */)
{
  int ret = OB_SUCCESS;
  uint32_t srid = UINT32_MAX;
  ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  if (use_little_bo) {
    if (wkb.length() < WKB_GEO_SRID_SIZE) {
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_WARN("invalid data length", K(ret), K(wkb.length()));
      if (OB_NOT_NULL(func_name)) {
        LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, func_name);
      }
    } else if (FALSE_IT(srid = ObGeoWkbByteOrderUtil::read<uint32_t>((const char *)wkb.ptr(),
        ObGeoWkbByteOrder::LittleEndian))) { // adapt mysql, always use little-endian
      // do nothing
    }
  } else if (OB_FAIL(ObGeoTypeUtil::get_srid_from_wkb(wkb, srid))) {
    LOG_WARN("fail to get srid", K(ret), K(wkb));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get session", K(ret));
  } else if (OB_FAIL(get_srs_item(session->get_effective_tenant_id(), srs_guard, srid, srs))) {
    LOG_WARN("fail to get srs inner", K(ret), K(srid));
  }

  return ret;
}

int ObGeoExprUtils::build_geometry(ObIAllocator &allocator,
                                   const ObString &wkb,
                                   ObGeometry *&geo,
                                   const ObSrsItem *srs,
                                   const char *func_name,
                                   const bool need_normlize /* = true */,
                                   const bool need_check_ring /* = false */,
                                   const bool need_correct /* = true */)
{
  int ret = OB_SUCCESS;
  ObGeoErrLogInfo log_info;

  if (OB_FAIL(ObGeoTypeUtil::build_geometry(allocator, wkb, geo, srs, log_info,
      need_normlize, need_check_ring, need_correct))) {
    if (OB_ERR_GIS_INVALID_DATA == ret && OB_NOT_NULL(func_name)) {
      LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, func_name);
    } else if (OB_ERR_GEOMETRY_PARAM_LONGITUDE_OUT_OF_RANGE == ret && OB_NOT_NULL(func_name)) {
      LOG_USER_ERROR(OB_ERR_GEOMETRY_PARAM_LONGITUDE_OUT_OF_RANGE,
                     func_name,
                     log_info.value_out_of_range_,
                     log_info.min_long_val_,
                     log_info.max_long_val_);
    } else if (OB_ERR_GEOMETRY_PARAM_LATITUDE_OUT_OF_RANGE == ret && OB_NOT_NULL(func_name)) {
      LOG_USER_ERROR(OB_ERR_GEOMETRY_PARAM_LATITUDE_OUT_OF_RANGE,
                     func_name,
                     log_info.value_out_of_range_,
                     log_info.min_lat_val_,
                     log_info.max_lat_val_);
    }
  }

  return ret;
}

int ObGeoExprUtils::construct_geometry(ObIAllocator &allocator,
                                       const ObString &wkb,
                                       omt::ObSrsCacheGuard &srs_guard,
                                       const ObSrsItem *&srs,
                                       ObGeometry *&geo,
                                       const char *func_name,
                                       bool has_srid /* = true */)
{
  int ret = OB_SUCCESS;
  const int64_t len = wkb.length();
  const char *data = wkb.ptr();
  ObGeoSrid srid = 0;
  ObGeoWkbByteOrder bo = ObGeoWkbByteOrder::INVALID;
  ObGeoErrLogInfo log_info;

  if (len < WKB_GEO_SRID_SIZE) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("invalid data length", K(ret), K(len));
    if (OB_NOT_NULL(func_name)) {
      LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, func_name);
    }
  } else if (FALSE_IT(srid = static_cast<ObGeoSrid>(ObGeoWkbByteOrderUtil::read<uint32_t>(data, ObGeoWkbByteOrder::LittleEndian)))) {
  } else if (OB_FAIL(get_srs_item(MTL_ID(), srs_guard, srid, srs))) {
    if (OB_ERR_SRS_NOT_FOUND == ret) {
      LOG_USER_WARN(OB_ERR_SRS_NOT_FOUND, srid); // adapt mysql
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get srs", K(ret), K(srid));
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(ObGeoTypeUtil::construct_geometry(allocator, wkb, srs, geo, has_srid))) {
    if (OB_ERR_GIS_INVALID_DATA == ret && OB_NOT_NULL(func_name)) {
      LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, func_name);
    } else if (OB_ERR_INVALID_GEOMETRY_TYPE == ret && OB_NOT_NULL(func_name)) {
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, func_name);
    } else if (OB_ERR_GEOMETRY_PARAM_LONGITUDE_OUT_OF_RANGE == ret && OB_NOT_NULL(func_name)) {
      LOG_USER_ERROR(OB_ERR_GEOMETRY_PARAM_LONGITUDE_OUT_OF_RANGE,
                     func_name,
                     log_info.value_out_of_range_,
                     log_info.min_long_val_,
                     log_info.max_long_val_);
    } else if (OB_ERR_GEOMETRY_PARAM_LATITUDE_OUT_OF_RANGE == ret && OB_NOT_NULL(func_name)) {
      LOG_USER_ERROR(OB_ERR_GEOMETRY_PARAM_LATITUDE_OUT_OF_RANGE,
                     func_name,
                     log_info.value_out_of_range_,
                     log_info.min_lat_val_,
                     log_info.max_lat_val_);
    } else if (OB_ERR_LONGITUDE_OUT_OF_RANGE == ret && OB_NOT_NULL(func_name)) {
      LOG_USER_ERROR(OB_ERR_LONGITUDE_OUT_OF_RANGE,
                     log_info.value_out_of_range_,
                     func_name,
                     log_info.min_long_val_,
                     log_info.max_long_val_);
    } else if (OB_ERR_LATITUDE_OUT_OF_RANGE == ret && OB_NOT_NULL(func_name)) {
      LOG_USER_ERROR(OB_ERR_LATITUDE_OUT_OF_RANGE,
                     log_info.value_out_of_range_,
                     func_name,
                     log_info.min_lat_val_,
                     log_info.max_lat_val_);
    } else if (OB_ERR_GIS_DATA_WRONG_ENDIANESS == ret) {
      LOG_USER_ERROR(OB_ERR_GIS_DATA_WRONG_ENDIANESS);
    }
  }

  return ret;
}

int ObGeoExprUtils::check_coordinate_range(const ObSrsItem *srs,
                                           ObGeometry *geo,
                                           const char *func_name,
                                           const bool is_param /* = false */,
                                           const bool is_normalized /* = false */)
{
  int ret = OB_SUCCESS;
  ObGeoErrLogInfo log_info;

  if (OB_FAIL(ObGeoTypeUtil::check_coordinate_range(srs, geo, log_info, is_param, is_normalized))) {
    if (OB_ERR_GEOMETRY_PARAM_LONGITUDE_OUT_OF_RANGE == ret && OB_NOT_NULL(func_name)) {
      LOG_USER_ERROR(OB_ERR_GEOMETRY_PARAM_LONGITUDE_OUT_OF_RANGE,
                     func_name,
                     log_info.value_out_of_range_,
                     log_info.min_long_val_,
                     log_info.max_long_val_);
    } else if (OB_ERR_GEOMETRY_PARAM_LATITUDE_OUT_OF_RANGE == ret && OB_NOT_NULL(func_name)) {
      LOG_USER_ERROR(OB_ERR_GEOMETRY_PARAM_LATITUDE_OUT_OF_RANGE,
                     func_name,
                     log_info.value_out_of_range_,
                     log_info.min_lat_val_,
                     log_info.max_lat_val_);
    } else if (OB_ERR_LONGITUDE_OUT_OF_RANGE == ret && OB_NOT_NULL(func_name)) {
      LOG_USER_ERROR(OB_ERR_LONGITUDE_OUT_OF_RANGE,
                     log_info.value_out_of_range_,
                     func_name,
                     log_info.min_long_val_,
                     log_info.max_long_val_);
    } else if (OB_ERR_LATITUDE_OUT_OF_RANGE == ret && OB_NOT_NULL(func_name)) {
      LOG_USER_ERROR(OB_ERR_LATITUDE_OUT_OF_RANGE,
                     log_info.value_out_of_range_,
                     func_name,
                     log_info.min_lat_val_,
                     log_info.max_lat_val_);
    }
  }

  return ret;
}

int ObGeoExprUtils::correct_coordinate_range(const ObSrsItem *srs_item,
                                             ObGeometry *geo,
                                             const char *func_name)
{
  int ret = OB_SUCCESS;
  ObGeoLatlongCheckVisitor range_visitor(srs_item);

  if (OB_FAIL(geo->do_visit(range_visitor))) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, func_name);
    LOG_WARN("failed to reverse geometry coordinate", K(ret));
  } else if (range_visitor.has_changed()) {
    LOG_WARN("Coordinate values were coerced into range [-180 -90, 180 90] for GEOGRAPHY");
  }

  return ret;
}

int ObGeoExprUtils::parse_axis_order(const ObString option_str,
                                     const char *func_name,
                                     ObGeoAxisOrder &axis_order)
{
  int ret = OB_SUCCESS;
  axis_order = ObGeoAxisOrder::INVALID;
  uint64_t pos = 0;
  const uint64_t end = option_str.length();
  uint64_t str_start_pos = 0;
  ObString axis_order_key;
  ObString axis_order_val;
  bool empty_string = false;
  const uint64_t STR_LEN_MAX = 512;
  char err_str[STR_LEN_MAX] = {0};

  if (0 == option_str.length()) {
    empty_string = true;
    axis_order = ObGeoAxisOrder::SRID_DEFINED;
  } else {
    // expect two word, 'axis-order' and its value
    for (int i = 0; i < 2 && OB_SUCC(ret); i++) {
      while(pos < end && isspace(option_str[pos])) {
        pos++;
      }

      // option_str is empty
      if (0 == i && pos == end) {
        empty_string = true;
        axis_order = ObGeoAxisOrder::SRID_DEFINED;
        break;
      }

      if (1 == i) {
        if ('=' != option_str[pos++]) {
          ret = OB_INVALID_OPTION;
          LOG_WARN("invalid axis-order str", K(ret), K(pos), K(option_str[pos - 1]));
        } else {
          while(pos < end && isspace(option_str[pos])) {
            pos++;
          }
        }
      }

      str_start_pos = pos;
      while(OB_SUCC(ret) && pos < end && (isalpha(option_str[pos]) || '-' == option_str[pos])) {
        pos++;
      }

      if (OB_FAIL(ret)) {
        // do nothing
      } else if (str_start_pos == pos) {
        // not valid string or reach end
        ret = OB_INVALID_OPTION;
        LOG_WARN("invalid axis-order str", K(ret), K(str_start_pos), K(pos));
      } else if (0 == i) {
        if (pos == end) {
          // no value
          ret = OB_INVALID_OPTION;
          LOG_WARN("value is empty", K(ret), K(str_start_pos), K(pos));
        } else {
          axis_order_key.assign_ptr(option_str.ptr()+str_start_pos, pos-str_start_pos);
          if (axis_order_key.case_compare(ObString("axis-order"))) {
            ret = OB_ERR_INVALID_OPTION_KEY;
            strncpy(err_str, axis_order_key.ptr(), axis_order_key.length() < STR_LEN_MAX ? axis_order_key.length() : STR_LEN_MAX);
            LOG_USER_ERROR(OB_ERR_INVALID_OPTION_KEY, err_str, func_name);
            LOG_WARN("failed to parse axis-order, the key must be axis-order", K(ret));
          }
        }
      } else {
        axis_order_val.assign_ptr(option_str.ptr()+str_start_pos, pos-str_start_pos);
      }
    }

    if (empty_string) {
      // do nothing
    } else if (OB_SUCC(ret)) {
      while(pos < end && isspace(option_str[pos])) {
        pos++;
      }
      if (pos < end) {
      // with extra tail char
        ret = OB_ERR_INVALID_OPTION_VALUE;
        LOG_WARN("failed to parse axis-order, has extra tailing character", K(ret));
      } else {
        if (0 == axis_order_val.case_compare(ObString("long-lat"))) {
          axis_order = ObGeoAxisOrder::LONG_LAT;
        } else if (0 == axis_order_val.case_compare(ObString("lat-long"))) {
          axis_order = ObGeoAxisOrder::LAT_LONG;
        } else if (0 == axis_order_val.case_compare(ObString("srid-defined"))) {
          axis_order = ObGeoAxisOrder::SRID_DEFINED;
        } else {
          ret = OB_ERR_INVALID_OPTION_VALUE;
          LOG_WARN("failed to parse axis-order, the value must be one of long-lat, lat-long and srid-defined", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
        strncpy(err_str, axis_order_key.ptr(), axis_order_key.length() < STR_LEN_MAX ? axis_order_key.length() : STR_LEN_MAX);
        LOG_USER_ERROR(OB_ERR_INVALID_OPTION_VALUE, axis_order_val.ptr(), err_str, func_name);
      }
    } else if (ret == OB_INVALID_OPTION) {
      ret = OB_ERR_INVALID_OPTION_KEY_VALUE_PAIR;
      strncpy(err_str, option_str.ptr(), option_str.length() < STR_LEN_MAX ? option_str.length() : STR_LEN_MAX);
      LOG_USER_ERROR(OB_ERR_INVALID_OPTION_KEY_VALUE_PAIR, err_str, '=', func_name);
    }
  }

  return ret;
}

int ObGeoExprUtils::check_need_reverse(ObGeoAxisOrder axis_order,
                                       bool &need_reverse)
{
  int ret = OB_SUCCESS;

  switch (axis_order) {
    case ObGeoAxisOrder::LONG_LAT: {
      need_reverse = false;
      break;
    }
    case ObGeoAxisOrder::LAT_LONG: {
      need_reverse = true;
      break;
    }
    case ObGeoAxisOrder::SRID_DEFINED: {
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected axis order parse result", K(ret), K(axis_order));
      break;
    }
  }

  return ret;
}

int ObGeoExprUtils::check_empty(ObGeometry *geo,
                                bool &is_empty)
{
  INIT_SUCC(ret);
  is_empty = false;
  ObGeoCheckEmptyVisitor check_empty_visitor;
  if (OB_FAIL(geo->do_visit(check_empty_visitor))) {
    LOG_WARN("check empty geo failed", K(ret));
  } else {
    is_empty = check_empty_visitor.get_result();
  }
  return ret;
}

int ObGeoExprUtils::parse_srid(const ObString &srid_str,
                               uint32_t &srid)
{
  int ret = OB_SUCCESS;
  uint64_t pos = 0;
  const uint64_t end = srid_str.length();
  uint64_t str_start_pos = 0;
  ObString srid_keyword;
  ObString srid_value;

  while(pos < end && isspace(srid_str[pos])) {
    pos++;
  }
  str_start_pos = pos;
  // parse srid keyword
  while(pos < end && (isalpha(srid_str[pos]))) {
    pos++;
  }
  if (str_start_pos == pos) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("failed to parse srid, not valid string or reach end", K(ret));
  } else {
    srid_keyword.assign_ptr(srid_str.ptr()+str_start_pos, pos-str_start_pos);
  }

  // parse srid value
  if (OB_FAIL(ret)) {
  } else if ('=' != srid_str[pos++]) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("failed to parse srid, should have equal in srid str", K(ret));
  } else {
    str_start_pos = pos;
    while(pos < end && (isdigit(srid_str[pos]))) {
      pos++;
    }
    if (str_start_pos == pos) {
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_WARN("failed to parse srid, not valid string or reach end", K(ret));
    } else {
      srid_value.assign_ptr(srid_str.ptr()+str_start_pos, pos-str_start_pos);
    }
  }

  if (OB_SUCC(ret)) {
    while(pos < end && isspace(srid_str[pos])) {
      pos++;
    }
    if (pos < end) {
      // with extra tail char
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_WARN("failed to parse srid, has extra tailing character", K(ret));
    } else if (srid_keyword.case_compare(ObString("srid"))) {
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_WARN("failed to parse srid, the key must be srid", K(ret));
    } else {
      int err = 0;
      uint64_t val = 0;
      val = ObCharset::strntoull(srid_value.ptr(), srid_value.length(), 10, &err);
      if (err == 0) {
        if (val > UINT_MAX32) {
          ret = OB_ERR_GIS_INVALID_DATA;
          LOG_WARN("srid input value out of range", K(ret), K(val));
        } else {
          srid = val;
        }
      } else {
        ret = OB_ERR_GIS_INVALID_DATA;
        LOG_WARN("failed to parse srid, has extra tailing character", K(ret));
      }
    }
  }

  return ret;
}

int ObGeoExprUtils::get_box_bestsrid(ObGeogBox *geo_box1,
                                     ObGeogBox *geo_box2,
                                     int32 &bestsrid)
{
  int ret = OB_SUCCESS;
  double width = 0.0;
  double height = 0.0;
  ObPoint2d center;
  ObGeogBox geo_box;

  if (OB_ISNULL(geo_box1)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("geo_box1 is NULL", K(ret));
  } else {
    geo_box = *geo_box1;
    if (geo_box2 != NULL) {
      ObGeoBoxUtil::box_uion(*geo_box2, geo_box);
    }
    ObGeoBoxUtil::get_box_center(geo_box, center);
    width = 180.0 * ObGeoBoxUtil::caculate_box_angular_width(geo_box) / M_PI;
    height = 180.0 * ObGeoBoxUtil::caculate_box_angular_height(geo_box) / M_PI;
  }

  const double UTM_ZONE_GAP = 6.0;
  const uint8_t UTM_ZONE_NUM = 60;
  bestsrid = SRID_WORLD_MERCATOR_PG;
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (height < 45.0 && center.y > 70.0) {
    bestsrid = SRID_NORTH_LAMBERT_PG;
  } else if (height < 45.0 && center.y <= -70.0) {
    bestsrid = SRID_SOUTH_LAMBERT_PG;
  } else if (width < UTM_ZONE_GAP) {
    uint32_t utm_zone = floor((center.x + 180.0) / UTM_ZONE_GAP);
    if (utm_zone >= UTM_ZONE_NUM) {
      utm_zone = UTM_ZONE_NUM - 1;
    }
    if (center.y < 0.0) {
      bestsrid = SRID_SOUTH_UTM_START_PG + utm_zone;
    } else {
      bestsrid = SRID_NORTH_UTM_START_PG + utm_zone;
    }
  } else if (height < 25.0) {
    int32_t x_zone = -1;
    int32_t y_zone = floor(center.y / 30.0) + 3;
    if (width < 30.0 && (y_zone == 2 || y_zone == 3)) {
      x_zone = floor(center.x / 30.0) + 6;
    } else if (width < 45.0 && (y_zone == 1 || y_zone == 4)) {
      x_zone = floor(center.x / 45.0) + 4;
    } else if (width < 90.0 && (y_zone == 0 || y_zone == 5)) {
      x_zone = floor(center.x / 90.0) + 2;
    }
    if (x_zone != -1) {
      bestsrid = SRID_LAEA_START_PG + 20 * y_zone + x_zone;
    }
  }

  return ret;
}

int ObGeoExprUtils::normalize_wkb(const ObSrsItem *srs,
                                  ObString &wkb,
                                  ObArenaAllocator &allocator,
                                  ObGeometry *&geo)
{
  int ret = OB_SUCCESS;

  if (OB_NOT_NULL(srs)
      && (srs->is_geographical_srs() || srs->is_lat_long_order())) {
    // points in geom need to be normalized
    ObString calc_wkb;
    char *calc_buf = (char *)allocator.alloc(wkb.length());
    if (OB_ISNULL(calc_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory for normalize wkb failed", K(wkb.length()), K(ret));
    } else {
      MEMCPY(calc_buf, wkb.ptr(), wkb.length());
      calc_wkb.assign(calc_buf, wkb.length());
      ObGeoNormalizeVisitor normalize_visitor(srs);
      if (OB_FAIL(ObGeoTypeUtil::create_geo_by_wkb(allocator, calc_wkb, srs, geo, false))) {
        LOG_WARN("get geo by wkb failed", K(ret));
      } else if (OB_FAIL(geo->do_visit(normalize_visitor))) {
        LOG_WARN("normalize geo failed", K(ret));
      } else {
        // do nothing
      }
    }
  }

  return ret;
}

// for st_transform, normalize wkb inplace accord to proj4text
int ObGeoExprUtils::normalize_wkb(ObString &proj4text,
                                  ObGeometry *geo)
{
  int ret = OB_SUCCESS;
  ObString trimed_proj4text = proj4text.trim();
  const char *lon_lat_alias[] = {"+proj=latlon", "+proj=latlong", "+proj=longlat", "+proj=lonlat"};
  bool is_lon_lat = false;

  for(int i = 0; i < ARRAYSIZEOF(lon_lat_alias) && !is_lon_lat; i++) {
    is_lon_lat = trimed_proj4text.prefix_match_ci(lon_lat_alias[i]);
  }
  if (is_lon_lat) {
    ObGeoNormalizeVisitor normalize_visitor(NULL, true);
    if (OB_FAIL(geo->do_visit(normalize_visitor))) {
      LOG_WARN("normalize geo failed", K(ret));
    }
  }

  return ret;
}

// for st_transform, denormalize wkb inplace accord to proj4text
int ObGeoExprUtils::denormalize_wkb(ObString &proj4text,
                                    ObGeometry *geo)
{
  int ret = OB_SUCCESS;
  ObString trimed_proj4text = proj4text.trim();
  const char *lon_lat_alias[] = {"+proj=latlon", "+proj=latlong", "+proj=longlat", "+proj=lonlat"};
  bool is_lon_lat = false;

  for(int i = 0; i < ARRAYSIZEOF(lon_lat_alias) && !is_lon_lat; i++) {
    is_lon_lat = trimed_proj4text.prefix_match_ci(lon_lat_alias[i]);
  }
  if (is_lon_lat) {
    ObGeoDeNormalizeVisitor denormalize_visitor;
    if (OB_FAIL(geo->do_visit(denormalize_visitor))) {
      LOG_WARN("denormalize geo failed", K(ret));
    }
  }

  return ret;
}

int ObGeoExprUtils::geo_to_wkb(ObGeometry &geo,
                               const ObExpr &expr,
                               ObEvalCtx &ctx,
                               const ObSrsItem *srs_item,
                               ObString &res_wkb,
                               uint32_t srs_id /* = 0 */)
{
  int ret = OB_SUCCESS;
  ObGeoWkbSizeVisitor wkb_size_visitor;

  if (OB_FAIL(geo.do_visit(wkb_size_visitor))) {
    LOG_WARN("failed to do wkb size visitor", K(ret));
  } else {
    // swkb format : srid + version + wkb
    int64_t res_size = WKB_OFFSET + wkb_size_visitor.geo_size();
    char *res_buf = nullptr;
    int64_t res_buf_len = 0;
    ObDatum tmp_res;
    ObTextStringDatumResult str_result(expr.datum_meta_.type_, &expr, &ctx, &tmp_res);
    if (OB_FAIL(str_result.init(res_size, nullptr))) {
      LOG_WARN("fail to init result", K(ret), K(res_size));
    } else if (OB_FAIL(str_result.get_reserved_buffer(res_buf, res_buf_len))) {
      LOG_WARN("");
    } else if (res_buf_len < res_size) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid res buf len", K(ret), K(res_buf_len), K(res_size));
    } else {
      uint32_t srid = srs_item == NULL ? srs_id : srs_item->get_srid();
      ObGeoWkbByteOrderUtil::write<uint32_t>(res_buf, srid);
      uint8_t encode_ver = ENCODE_GEO_VERSION(geo.get_version());
      *(res_buf + WKB_GEO_SRID_SIZE) = encode_ver;
      ObString wkb_nosrid_buf(res_size, WKB_OFFSET, res_buf);
      ObGeoWkbVisitor wkb_visitor(srs_item, &wkb_nosrid_buf);
      if (OB_FAIL(geo.do_visit(wkb_visitor))) {
        LOG_WARN("failed to do wkb visit", K(ret), K(srid));
      } else if (OB_FAIL(str_result.lseek(res_size, 0))) {
        LOG_WARN("failed to lseek res.", K(ret), K(str_result), K(res_size));
      } else {
        str_result.get_result_buffer(res_wkb);
      }
    }
  }

  return ret;
}

int ObGeoExprUtils::geo_to_wkb(ObGeometry &geo,
                               ObExprCtx &expr_ctx,
                               const ObSrsItem *srs_item,
                               ObString &res_wkb,
                               uint32_t srs_id /* = 0 */)
{
  return ObGeoTypeUtil::to_wkb(*expr_ctx.calc_buf_, geo, srs_item, res_wkb);
}

void ObGeoExprUtils::geo_func_error_handle(int error_ret, const char* func_name)
{
  if (error_ret == OB_INVALID_ARGUMENT) {
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, func_name);
  } else if (error_ret == OB_OPERATE_OVERFLOW) {
    // about (-9.2233720368547758e+18, 9.2233720368547758e+18)
    LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "coordinate", func_name);
  }
}

int ObGeoExprUtils::zoom_in_geos_for_relation(ObGeometry &geo1, ObGeometry &geo2)
{
  int ret = OB_SUCCESS;
  if (geo1.get_zoom_in_value() > 0 || geo2.get_zoom_in_value() > 0) {
    uint32_t zoom_in = geo1.get_zoom_in_value();
    zoom_in = zoom_in > geo2.get_zoom_in_value() ? zoom_in : geo2.get_zoom_in_value();
    ObGeoZoomInVisitor zoom_in_visitor(zoom_in);
    if (OB_FAIL(geo1.do_visit(zoom_in_visitor))) {
      LOG_WARN("failed to zoom in visit", K(ret), K(zoom_in));
    } else if (OB_FAIL(geo2.do_visit(zoom_in_visitor))) {
      LOG_WARN("failed to zoom in visit", K(ret), K(zoom_in));
    }
  }
  return ret;
}

int ObGeoExprUtils::pack_geo_res(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res, const ObString &str)
{
  int ret = OB_SUCCESS;
  ObTextStringDatumResult text_result(expr.datum_meta_.type_, &expr, &ctx, &res);
  if (OB_FAIL(text_result.init(str.length()))) {
    LOG_WARN("init lob result failed");
  } else if (OB_FAIL(text_result.append(str.ptr(), str.length()))) {
    LOG_WARN("failed to append realdata", K(ret), K(text_result));
  } else {
    text_result.set_result();
  }
  return ret;
}

} // sql
} // oceanbase
