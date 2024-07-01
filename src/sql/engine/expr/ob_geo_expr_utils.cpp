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
#include "lib/geo/ob_geo_utils.h"
#include "lib/geo/ob_geo_3d.h"
#include "lib/geo/ob_geo_reverse_coordinate_visitor.h"
#include "lib/geo/ob_geo_cache_polygon.h"

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

  if (!ObGeoTypeUtil::need_get_srs(srid)) {
    // do nothing
  } else if (OB_FAIL(OTSRS_MGR->get_tenant_srs_guard(srs_guard))) {
    LOG_WARN("fail to get srs guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(srs_guard.get_srs_item(srid, srs))) {
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
                                   uint8_t build_flag)
{
  int ret = OB_SUCCESS;
  ObGeoErrLogInfo log_info;

  if (OB_FAIL(ObGeoTypeUtil::build_geometry(allocator, wkb, geo, srs, log_info, build_flag))) {
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

// 3d-wkb is allow, and will return ObGeometry3D obj
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
    if ((OB_ERR_GIS_INVALID_DATA == ret && OB_NOT_NULL(func_name))) {
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
  if (ObGeoTypeUtil::is_3d_geo_type(geo->type())) {
    ObGeometry3D *geo_3d = reinterpret_cast<ObGeometry3D *>(geo);
    if (OB_FAIL(geo_3d->correct_lon_lat(srs_item))) {
      LOG_WARN("fail to correct lon lat of 3D geometry", K(ret));
    }
  } else {
    ObGeoLatlongCheckVisitor range_visitor(srs_item);

    if (OB_FAIL(geo->do_visit(range_visitor))) {
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, func_name);
      LOG_WARN("failed to reverse geometry coordinate", K(ret));
    } else if (range_visitor.has_changed()) {
      LOG_WARN("Coordinate values were coerced into range [-180 -90, 180 90] for GEOGRAPHY");
    }
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
            strncpy(err_str, axis_order_key.ptr(), axis_order_key.length() < (STR_LEN_MAX - 1) ? axis_order_key.length() : (STR_LEN_MAX - 1));
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
        strncpy(err_str, axis_order_key.ptr(), axis_order_key.length() < (STR_LEN_MAX - 1) ? axis_order_key.length() : (STR_LEN_MAX - 1));
        char val_err_str[STR_LEN_MAX] = {0}; // cstyle err string
        strncpy(val_err_str, axis_order_val.ptr(), axis_order_val.length() < (STR_LEN_MAX - 1) ? axis_order_val.length() : (STR_LEN_MAX - 1));
        LOG_USER_ERROR(OB_ERR_INVALID_OPTION_VALUE, val_err_str, err_str, func_name);
      }
    } else if (ret == OB_INVALID_OPTION) {
      ret = OB_ERR_INVALID_OPTION_KEY_VALUE_PAIR;
      strncpy(err_str, option_str.ptr(), option_str.length() < (STR_LEN_MAX - 1) ? option_str.length() : (STR_LEN_MAX - 1));
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
  if (ObGeoTypeUtil::is_3d_geo_type(geo->type())) {
    ObGeometry3D *geo3D = reinterpret_cast<ObGeometry3D *>(geo);
    if (OB_FAIL(geo3D->check_empty(is_empty))) {
      LOG_WARN("fail to check 3D geometry empty", K(ret));
    }
  } else if (ObGeoTypeUtil::is_multi_geo_type(geo->type())) {
    ObGeoCheckEmptyVisitor check_empty_visitor;
    if (OB_FAIL(geo->do_visit(check_empty_visitor))) {
      LOG_WARN("check empty geo failed", K(ret));
    } else {
      is_empty = check_empty_visitor.get_result();
    }
  } else {
    is_empty = geo->is_empty();
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
      ObGeoBoxUtil::box_union(*geo_box2, geo_box);
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
    ObGeoNormalizeVisitor normalize_visitor(srs);
    ObGeoErrLogInfo log_info;
    if (OB_FAIL(ObGeoTypeUtil::build_geometry(allocator, wkb, geo, srs, log_info, ObGeoBuildFlag::GEO_ALLOW_3D))) {
      LOG_WARN("fail to build geometry", K(ret));
    } else if (OB_FAIL(geo->do_visit(normalize_visitor))) {
      LOG_WARN("normalize geo failed", K(ret));
    } else {
      // do nothing
      ObString wkb(geo->length(), geo->val());
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
  if (ObGeoTypeUtil::is_3d_geo_type(geo.type())) {
    if (OB_FAIL(geo_to_3d_wkb(geo, expr, ctx, srs_item, res_wkb, srs_id))) {
      LOG_WARN("fail to write 3d geo to wkb", K(ret));
    }
  } else {
    if (OB_FAIL(geo_to_2d_wkb(geo, expr, ctx, srs_item, res_wkb, srs_id))) {
      LOG_WARN("fail to write 2d geo to wkb", K(ret));
    }
  }
  return ret;
}

int ObGeoExprUtils::geo_to_2d_wkb(ObGeometry &geo,
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
      LOG_WARN("fail to get reserver buffer", K(ret));
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

int ObGeoExprUtils::geo_to_3d_wkb(common::ObGeometry &geo,
                                  const ObExpr &expr,
                                  ObEvalCtx &ctx,
                                  const common::ObSrsItem *srs_item,
                                  common::ObString &res_wkb,
                                  uint32_t srs_id /*= 0 */)
{
  int ret = OB_SUCCESS;
  if (!ObGeoTypeUtil::is_3d_geo_type(geo.type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("geometry is not 3d type", K(ret), K(geo.type()));
  } else {
    ObString no_srid_wkb = geo.to_wkb();
    int64_t res_size = WKB_OFFSET + no_srid_wkb.length();
    ObDatum tmp_res;  // not used
    char *res_buf = nullptr;
    int64_t res_buf_len = 0;
    ObTextStringDatumResult str_result(expr.datum_meta_.type_, &expr, &ctx, &tmp_res);
    if (OB_FAIL(str_result.init(res_size, nullptr))) {
      LOG_WARN("fail to init result", K(ret), K(res_size));
    } else if (OB_FAIL(str_result.get_reserved_buffer(res_buf, res_buf_len))) {
      LOG_WARN("fail to get reserved buffer", K(ret));
    } else if (res_buf_len < res_size) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid res buf len", K(ret), K(res_buf_len), K(res_size));
    } else {
      uint32_t srid = srs_item == NULL ? srs_id : srs_item->get_srid();
      ObGeoWkbByteOrderUtil::write<uint32_t>(res_buf, srid);
      uint8_t encode_ver = ENCODE_GEO_VERSION(geo.get_version());
      *(res_buf + WKB_GEO_SRID_SIZE) = encode_ver;
      if (OB_FAIL(str_result.lseek(WKB_OFFSET, 0))) {
        LOG_WARN("fail to lseek res", K(ret), K(str_result));
      } else if (OB_FAIL(str_result.append(no_srid_wkb))) {
        LOG_WARN("fail to append wkb", K(ret));
      } else {
        str_result.get_result_buffer(res_wkb);
      }
    }
  }
  return ret;
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

int ObGeoExprUtils::zoom_in_geos_for_relation(ObGeometry &geo1, ObGeometry &geo2,
                                              bool is_geo1_cached, bool is_geo2_cached)
{
  int ret = OB_SUCCESS;
  if (geo1.get_zoom_in_value() > 0 || geo2.get_zoom_in_value() > 0) {
    uint32_t zoom_in = geo1.get_zoom_in_value();
    zoom_in = zoom_in > geo2.get_zoom_in_value() ? zoom_in : geo2.get_zoom_in_value();
    ObGeoZoomInVisitor zoom_in_visitor(zoom_in);
    if (!is_geo1_cached && OB_FAIL(geo1.do_visit(zoom_in_visitor))) {
      LOG_WARN("failed to zoom in visit", K(ret), K(zoom_in));
    } else if (!is_geo2_cached && OB_FAIL(geo2.do_visit(zoom_in_visitor))) {
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

int ObGeoExprUtils::reverse_coordinate(ObGeometry *geo, const char *func_name)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(geo)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null geo", K(ret));
  } else if (ObGeoTypeUtil::is_3d_geo_type(geo->type())) {
    ObGeometry3D *geo_3d  = static_cast<ObGeometry3D *>(geo);
    if (OB_FAIL(geo_3d->reverse_coordinate())) {
      LOG_WARN("fail to reserver coordiante in geo 3d", K(ret));
    }
  } else {
    ObGeoReverseCoordinateVisitor rcoord_visitor;
    if (OB_FAIL(geo->do_visit(rcoord_visitor))) {
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, func_name);
      LOG_WARN("failed to reverse geometry coordinate", K(ret));
    }
  }
  return ret;
}

int ObGeoExprUtils::ob_geo_find_unit(const ObGeoUnit *units, const ObString &name, double &factor)
{
  INIT_SUCC(ret);
  int begin = 0;
  int end = sizeof(OB_GEO_UNITS)/sizeof(ObGeoUnit) - 1;
  bool is_found = false;
  while (begin <= end && !is_found) {
    int mid = begin + (end - begin) / 2;
    const int cmp_len = MIN(strlen(units[mid].name), name.length());
    int cmp_res = strncasecmp(units[mid].name, name.ptr(), cmp_len);
    if (cmp_res > 0) {
      end = mid - 1;
    } else if (cmp_res < 0) {
      begin = mid + 1;
    } else {
      if (name.length() == strlen(units[mid].name)) {
        is_found = true;
        factor = units[mid].factor;
      } else if (name.length() > strlen(units[mid].name)) {
        begin = mid + 1;
      } else {
        end = mid - 1;
      }
    }
  }

  if (!is_found) {
    ret = OB_ERR_UNIT_NOT_FOUND;
    char name_str[name.length() + 1];
    name_str[name.length()] = '\0';
    MEMCPY(name_str, name.ptr(), name.length());
    LOG_USER_ERROR(OB_ERR_UNIT_NOT_FOUND, name_str);
  }
  return ret;
}

int ObGeoExprUtils::length_unit_conversion(const ObString &unit_str, const ObSrsItem *srs,
                                          double in_num, double &out_num)
{
  int ret = OB_SUCCESS;
  double factor = 0.0;
  if (OB_ISNULL(srs) || srs->get_srid() == 0) {
    ret = OB_ERR_GEOMETRY_IN_UNKNOWN_LENGTH_UNIT;
    char name_str[unit_str.length() + 1];
    name_str[unit_str.length()] = '\0';
    MEMCPY(name_str, unit_str.ptr(), unit_str.length());
    LOG_USER_ERROR(OB_ERR_GEOMETRY_IN_UNKNOWN_LENGTH_UNIT, N_ST_DISTANCE, name_str);
  } else if (OB_FAIL(ob_geo_find_unit(OB_GEO_UNITS, unit_str, factor))) {
    if (lib::is_oracle_mode() && ret == OB_ERR_UNIT_NOT_FOUND) {
      ret =OB_ERR_CONVERSION_OF_UNIT;
      LOG_WARN("conversion error between the specified unit and standard unit", K(ret), K(unit_str));
    } else {
      LOG_WARN("invalid geo unit name", K(ret), K(unit_str));
    }
  } else {
    out_num = in_num * (srs->linear_uint() / factor);
  }
  return ret;
}

int ObGeoExprUtils::get_input_geometry(ObIAllocator &allocator, ObDatum *gis_datum, ObEvalCtx &ctx, ObExpr *gis_arg,
    omt::ObSrsCacheGuard &srs_guard, const char *func_name,
    const ObSrsItem *&srs, ObGeometry *&geo)
{
  int ret = OB_SUCCESS;
  ObString wkb = gis_datum->get_string();
  ObGeoType type = ObGeoType::GEOTYPEMAX;
  uint32_t srid = -1;
  if (OB_FAIL(ObTextStringHelper::read_real_string_data(allocator,
          *gis_datum,
          gis_arg->datum_meta_,
          gis_arg->obj_meta_.has_lob_header(),
          wkb))) {
    LOG_WARN("fail to get real string data", K(ret), K(wkb));
  } else if (OB_FAIL(ObGeoTypeUtil::get_type_srid_from_wkb(wkb, type, srid))) {
    if (ret == OB_ERR_GIS_INVALID_DATA) {
      LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, func_name);
    }
    LOG_WARN("get type and srid from wkb failed", K(wkb), K(ret));
  } else if (OB_FAIL(ObGeoExprUtils::get_srs_item(
                  ctx, srs_guard, wkb, srs, true, func_name))) {
    LOG_WARN("fail to get srs item", K(ret), K(wkb));
  } else if (OB_FAIL(ObGeoExprUtils::build_geometry(allocator,
                  wkb,
                  geo,
                  srs,
                  func_name,
                  ObGeoBuildFlag::GEO_ALLOW_3D_DEFAULT))) {
    LOG_WARN("get first geo by wkb failed", K(ret));
  }
  return ret;
}

int ObGeoExprUtils::union_polygons(
    ObIAllocator &allocator, const ObGeometry &poly, ObGeometry *&polygons_union)
{
  int ret = OB_SUCCESS;
  ObGeometry *union_res = nullptr;
  ObGeoEvalCtx union_ctx(&allocator);
  if (OB_FAIL(union_ctx.append_geo_arg(&poly))) {
    LOG_WARN("failed to append geo arg to gis context", K(ret), K(union_ctx.get_geo_count()));
  } else if (OB_FAIL(union_ctx.append_geo_arg(polygons_union))) {
    LOG_WARN("failed to append geo arg to gis context", K(ret), K(union_ctx.get_geo_count()));
  } else if (OB_FAIL(ObGeoFunc<ObGeoFuncType::Union>::geo_func::eval(union_ctx, union_res))) {
    LOG_WARN("eval boost union failed", K(ret));
  } else {
    allocator.free(polygons_union);
    polygons_union = union_res;
  }
  return ret;
}

int ObGeoExprUtils::make_valid_polygon(ObGeometry *poly, ObIAllocator &allocator, ObGeometry *&valid_poly)
{
  int ret = OB_SUCCESS;
  ObGeoEvalCtx gis_context(&allocator);
  int res_unused = 0;
  if (OB_ISNULL(poly)
      || (poly->type() != ObGeoType::POLYGON && poly->type() != ObGeoType::MULTIPOLYGON)) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("invalid geometry input", K(ret), K(poly));
  } else if (OB_FAIL(gis_context.append_geo_arg(poly))) {
    LOG_WARN("failed to append geo arg to gis context", K(ret), K(gis_context.get_geo_count()));
  } else if (OB_FAIL(ObGeoFunc<ObGeoFuncType::Correct>::geo_func::eval(
                 gis_context, res_unused))) {
    LOG_WARN("eval boost dissolve polygon failed", K(ret));
  } else if (poly->type() == ObGeoType::POLYGON) {
    if (OB_FAIL(make_valid_polygon_inner(static_cast<ObCartesianPolygon&>(*poly),
                                                        allocator, valid_poly))) {
      LOG_WARN("make polygon valid failed", K(ret));
    }
  } else {
    ObCartesianMultipolygon &mpy = *reinterpret_cast<ObCartesianMultipolygon *>(poly);
    for (uint32_t i = 0; OB_SUCC(ret) && i < mpy.size(); ++i) {
      ObGeometry *valid_inner_poly = nullptr;
      if (OB_FAIL(make_valid_polygon_inner(mpy[i], allocator, valid_inner_poly))) {
        LOG_WARN("fail to make polygon valid", K(ret));
      } else if (OB_NOT_NULL(valid_inner_poly)) {
        if (OB_ISNULL(valid_poly)) {
          valid_poly = valid_inner_poly;
        } else if (!valid_inner_poly->is_empty()
                   && OB_FAIL(union_polygons(allocator, *valid_inner_poly, valid_poly))) {
          LOG_WARN("fail to union holes", K(ret));
        } else {
          allocator.free(valid_inner_poly);
        }
      }
    }
  }
  return ret;
}

int ObGeoExprUtils::make_valid_polygon_inner(
    ObCartesianPolygon &poly, ObIAllocator &allocator, ObGeometry *&valid_poly)
{
  int ret = OB_SUCCESS;
  if (!poly.empty() && poly.inner_ring_size() != 0) {
    ObCartesianPolygon tmp_ext_poly;
    tmp_ext_poly.exterior_ring() = poly.exterior_ring();
    ObGeometry *holes_union = nullptr;
    ObGeometry *shells_union = nullptr;
    for (uint32_t i = 0; OB_SUCC(ret) && i < poly.inner_ring_size(); ++i) {
      ObCartesianPolygon tmp_poly;
      tmp_poly.exterior_ring() = poly.inner_ring(i);
      bool is_intersects = false;
      ObGeoEvalCtx correct_context(&allocator);
      ObGeoEvalCtx intersects_ctx(&allocator);
      int res_unused;
      if (OB_FAIL(correct_context.append_geo_arg(&tmp_poly))) {
        LOG_WARN("build geo gis context failed", K(ret));
      } else if (OB_FAIL(ObGeoFunc<ObGeoFuncType::Correct>::geo_func::eval(correct_context, res_unused))) {
        LOG_WARN("eval geo correct failed", K(ret));
      } else if (OB_FAIL(intersects_ctx.append_geo_arg(&tmp_ext_poly))) {
        LOG_WARN("failed to append geo arg to gis context",
            K(ret),
            K(intersects_ctx.get_geo_count()));
      } else if (OB_FAIL(intersects_ctx.append_geo_arg(&tmp_poly))) {
        LOG_WARN("failed to append geo arg to gis context",
            K(ret),
            K(intersects_ctx.get_geo_count()));
      } else if (OB_FAIL(ObGeoFunc<ObGeoFuncType::Intersects>::geo_func::eval(
                    intersects_ctx, is_intersects))) {
        LOG_WARN("eval boost intersects failed", K(ret));
      } else if (is_intersects) {
        // holes
        if (OB_ISNULL(holes_union)) {
          ObCartesianPolygon *holes = OB_NEWx(ObCartesianPolygon, &allocator, tmp_poly);
          if (OB_ISNULL(holes)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc memory", K(ret));
          } else {
            holes_union = holes;
          }
        } else if (OB_FAIL(union_polygons(allocator, *&tmp_poly, holes_union))) {
          LOG_WARN("fail to union holes", K(ret));
        }
      } else {
        // shells
        if (OB_ISNULL(shells_union)) {
          ObCartesianPolygon *shells = OB_NEWx(ObCartesianPolygon, &allocator, tmp_poly);
          if (OB_ISNULL(shells)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc memory", K(ret));
          } else {
            shells_union = shells;
          }
        } else if (OB_FAIL(union_polygons(allocator, *&tmp_poly, shells_union))) {
          LOG_WARN("fail to union shells", K(ret));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(holes_union)) {
      holes_union = &tmp_ext_poly;
    } else {
      ObGeoEvalCtx diff_ctx(&allocator);
      ObGeometry *diff_holes = nullptr;
      if (OB_FAIL(diff_ctx.append_geo_arg(&tmp_ext_poly))) {
        LOG_WARN("failed to append geo arg to gis context", K(ret), K(diff_ctx.get_geo_count()));
      } else if (OB_FAIL(diff_ctx.append_geo_arg(holes_union))) {
        LOG_WARN("failed to append geo arg to gis context", K(ret), K(diff_ctx.get_geo_count()));
      } else if (OB_FAIL(ObGeoFunc<ObGeoFuncType::SymDifference>::geo_func::eval(diff_ctx, diff_holes))) {
        LOG_WARN("eval boost intersects failed", K(ret));
      } else {
        holes_union = diff_holes;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_NOT_NULL(shells_union)
              && OB_FAIL(union_polygons(allocator, *shells_union, holes_union))) {
      LOG_WARN("fail to union shells", K(ret));
    } else {
      if (holes_union->type() == ObGeoType::MULTIPOLYGON
         && static_cast<ObCartesianMultipolygon*>(holes_union)->size() == 1) {
        valid_poly = &static_cast<ObCartesianMultipolygon*>(holes_union)->front();
      } else {
        valid_poly = holes_union;
      }
    }
  } else if (!poly.empty() && poly.inner_ring_size() == 0) {
    ObGeoEvalCtx dissol_ctx(&allocator);
    ObGeometry *diff_holes;
    if (OB_FAIL(dissol_ctx.append_geo_arg(&poly))) {
      LOG_WARN("failed to append geo arg to gis context", K(ret), K(dissol_ctx.get_geo_count()));
    } else if (OB_FAIL(ObGeoFunc<ObGeoFuncType::DissolvePolygon>::geo_func::eval(dissol_ctx, diff_holes))) {
      LOG_WARN("eval boost dissolve polygon failed", K(ret));
    } else {
      ObGeoEvalCtx gis_context(&allocator);
      int res_unused;
      if (OB_FAIL(gis_context.append_geo_arg(diff_holes))) {
        LOG_WARN("failed to append geo arg to gis context", K(ret), K(gis_context.get_geo_count()));
      } else if (OB_FAIL(ObGeoFunc<ObGeoFuncType::Correct>::geo_func::eval(gis_context, res_unused))) {
        LOG_WARN("eval boost correct polygon failed", K(ret));
      } else {
        valid_poly = diff_holes;
      }
    }
  } else if (poly.empty()) {
    valid_poly = &poly;
  } else {
    int unused = 0;
    ObGeoEvalCtx gis_context(&allocator);
    if (OB_FAIL(gis_context.append_geo_arg(&poly))) {
      LOG_WARN("failed to append geo arg to gis context", K(ret), K(gis_context.get_geo_count()));
    } else if (OB_FAIL(
            ObGeoFunc<ObGeoFuncType::Correct>::geo_func::eval(gis_context, unused))) {
      LOG_WARN("eval boost correct failed", K(ret));
    } else {
      valid_poly = &poly;
    }
  }
  return ret;
}
ObGeoConstParamCache* ObGeoExprUtils::get_geo_constParam_cache(const uint64_t& id, ObExecContext *exec_ctx)
{
  INIT_SUCC(ret);
  ObGeoConstParamCache* cache_ctx = NULL;
  uint64_t data_version = 0;
  if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_2_1_2
  || (GET_MIN_CLUSTER_VERSION() >=  CLUSTER_VERSION_4_2_2_0 && GET_MIN_CLUSTER_VERSION() < MOCK_CLUSTER_VERSION_4_2_3_0)
  || (GET_MIN_CLUSTER_VERSION() > CLUSTER_VERSION_4_3_0_0 && GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_3_2_0)) {
    // geo para cache not available, return null
  } else if (ObExpr::INVALID_EXP_CTX_ID != id) {
    cache_ctx = static_cast<ObGeoConstParamCache*>(exec_ctx->get_expr_op_ctx(id));
    if (OB_ISNULL(cache_ctx)) {
      // if geoparamcache not exist, create one
      void *cache_ctx_buf = NULL;
      if (OB_FAIL(exec_ctx->create_expr_op_ctx(id, sizeof(ObGeoConstParamCache), cache_ctx_buf))) {
        LOG_WARN("failed to create expr op ctx", K(ret));
      } else if (OB_ISNULL(cache_ctx_buf)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cache ctx is null", K(ret));
      } else {
        cache_ctx = new (cache_ctx_buf) ObGeoConstParamCache(&exec_ctx->get_allocator());
      }
    }
  }
  return cache_ctx;
}

void ObGeoExprUtils::expr_get_const_param_cache(ObGeoConstParamCache* const_param_cache, ObGeometry *&geo, uint32_t& srid, bool& is_geo_cached, int cache_idx)
{
  if (OB_NOT_NULL(const_param_cache)) {
    geo = const_param_cache->get_const_param_cache(cache_idx);
    if (geo != NULL) {
      srid = geo->get_srid();
      is_geo_cached = true;
    }
  }
}

int ObGeoExprUtils::expr_prepare_build_geometry(ObIAllocator &allocator, const ObDatum &datum, const ObExpr &gis_arg, ObString& wkb, ObGeoType& type, uint32_t& srid)
{
  INIT_SUCC(ret);
  if (OB_FAIL(ObTextStringHelper::read_real_string_data(allocator, datum,
            gis_arg.datum_meta_, gis_arg.obj_meta_.has_lob_header(), wkb))) {
    LOG_WARN("fail to get real string data", K(ret), K(wkb));
  } else if (OB_FAIL(ObGeoTypeUtil::get_type_srid_from_wkb(wkb, type, srid))) {
    LOG_WARN("get type and srid from wkb failed", K(wkb), K(ret));
  }
  return ret;
}

void ObGeoExprUtils::init_box_by_cache(ObGeogBox *&box_ptr, ObGeogBox& box, ObCachedGeom* cache_geo)
{
  if (OB_NOT_NULL(cache_geo)) {
    box.xmax = cache_geo->get_x_max();
    box.xmin = cache_geo->get_x_min();
    box_ptr = &box;
  }
}

void ObGeoExprUtils::init_boxes_by_cache(ObGeogBox *&box_ptr1, ObGeogBox& box1,
                                        ObGeogBox *&box_ptr2, ObGeogBox& box2,
                                        ObGeoConstParamCache* const_param_cache,
                                        bool is_geo1_cached, bool is_geo2_cached)
{
  if (OB_NOT_NULL(const_param_cache) && (is_geo1_cached || is_geo2_cached)) {
    if (is_geo1_cached) {
      ObCachedGeom *cache_geo = const_param_cache->get_cached_geo(0);
      if (cache_geo != nullptr && cache_geo->is_inited()) {
        init_box_by_cache(box_ptr1, box1, cache_geo);
      }
    }
    if (is_geo2_cached) {
      ObCachedGeom *cache_geo = const_param_cache->get_cached_geo(1);
      if (cache_geo != nullptr && cache_geo->is_inited()) {
        init_box_by_cache(box_ptr2, box2, cache_geo);
      }
    }
  }
}

int ObGeoExprUtils::init_box_by_geo(ObGeometry &geo, ObIAllocator &allocator, ObGeogBox *&box_ptr)
{
  int ret = OB_SUCCESS;
  ObGeoEvalCtx gis_context(&allocator);
  bool result = false;
  if (OB_FAIL(gis_context.append_geo_arg(&geo))) {
    LOG_WARN("build gis context failed", K(ret), K(gis_context.get_geo_count()));
  } else if (OB_FAIL(ObGeoFunc<ObGeoFuncType::Box>::geo_func::eval(gis_context, box_ptr))) {
    LOG_WARN("get box failed", K(ret));
  }
  return ret;
}

int ObGeoExprUtils::check_box_intersects(ObGeometry &geo1, ObGeometry &geo2, ObIAllocator &allocator,
                                          ObGeoConstParamCache* const_param_cache,
                                          bool is_geo1_cached, bool is_geo2_cached, bool& box_intersects)
{
  int ret = OB_SUCCESS;
  box_intersects = true;
  ObGeogBox* box_ptr1 = nullptr;
  ObGeogBox* box_ptr2 = nullptr;
  ObGeogBox box1;
  ObGeogBox box2;
  init_boxes_by_cache(box_ptr1, box1, box_ptr2, box2, const_param_cache, is_geo1_cached, is_geo2_cached);

  if ((OB_ISNULL(box_ptr1) && OB_FAIL(init_box_by_geo(geo1, allocator, box_ptr1)))
    || (OB_ISNULL(box_ptr2) && OB_FAIL(init_box_by_geo(geo2, allocator, box_ptr2)))) {
    LOG_WARN("get failed", K(ret));
  } else if (OB_ISNULL(box_ptr1) || OB_ISNULL(box_ptr2)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret));
  } else {
    box_intersects = ObGeoBoxUtil::boxes_overlaps(*box_ptr1, *box_ptr2);
  }
  return ret;
}

ObGeoConstParamCache::~ObGeoConstParamCache()
{
    if (OB_NOT_NULL(cached_param1_)) {
      cached_param1_->destroy_cache();
      cached_param1_ = nullptr;
    }
    if (OB_NOT_NULL(cached_param2_)) {
      cached_param2_->destroy_cache();
      cached_param1_ = nullptr;
    }
  }

ObGeometry * ObGeoConstParamCache::get_const_param_cache(int arg_idx)
{
  ObGeometry * res = nullptr;
  INIT_SUCC(ret);
  if (arg_idx == 1) {
    res = param2_;
  } else if (arg_idx == 0) {
    res = param1_;
  }
  return res;
}

ObCachedGeom *ObGeoConstParamCache::get_cached_geo(int arg_idx)
{
  ObCachedGeom *res = nullptr;
  if (arg_idx == 1) {
    res = cached_param2_;
  } else if (arg_idx == 0) {
    res = cached_param1_;
  }
  return res;
}

int ObGeoConstParamCache::add_const_param_cache(int arg_idx, const common::ObGeometry &cache)
{
  INIT_SUCC(ret);
  ObGeometry *geo = nullptr;
  ObString data;
  if (OB_FAIL(ObGeoTypeUtil::create_geo_by_type(*allocator_, cache.type(), cache.crs() == ObGeoCRS::Geographic, true, geo))) {
    LOG_WARN("fail to create geo by type", K(ret));
  } else if (OB_FAIL(ob_write_string(*allocator_, ObString(cache.length(), cache.val()), data))) {
    LOG_WARN("fail to copy geo data", K(ret));
  } else {
    geo->set_data(data);
    geo->set_srid(cache.get_srid());
    geo->set_zoom_in_value(cache.get_zoom_in_value());
    if (arg_idx == 0) {
      param1_ = geo;
    } else if (arg_idx == 1) {
      param2_ = geo;
    }
  }
  return ret;
}

int ObGeoExprUtils::create_3D_empty_collection(ObIAllocator &allocator, uint32_t srid, bool is_3d, bool is_geog, ObGeometry *&geo)
{
  int ret = OB_SUCCESS;
  ObString empty_wkt = is_3d ? "GEOMETRYCOLLECTION Z EMPTY" : "GEOMETRYCOLLECTION EMPTY";
  if (OB_FAIL(ObWktParser::parse_wkt(allocator, empty_wkt, geo, true, is_geog))) {
    LOG_WARN("failed to parse wkt", K(ret));
  } else {
    geo->set_srid(srid);
  }
  return ret;
}

void ObGeoConstParamCache::add_cached_geo(int arg_idx, common::ObCachedGeom *cache)
{
  if (arg_idx == 0) {
    cached_param1_ = cache;
  } else if (arg_idx == 1) {
    cached_param2_ = cache;
  }
}

int ObGeoExprUtils::get_intersects_res(ObGeometry &geo1, ObGeometry &geo2,
                                      ObExpr *gis_arg1, ObExpr *gis_arg2,
                                      ObGeoConstParamCache* const_param_cache,
                                      const ObSrsItem *srs,
                                      ObArenaAllocator& temp_allocator, bool& res)
{
  INIT_SUCC(ret);
  ObGeoEvalCtx gis_context(&temp_allocator);
  bool result = false;
  if (OB_FAIL(gis_context.append_geo_arg(&geo1)) || OB_FAIL(gis_context.append_geo_arg(&geo2))) {
    LOG_WARN("build gis context failed", K(ret), K(gis_context.get_geo_count()));
  } else {
    ObCachedGeom *cache_geo = NULL;
    ObGeometry *geo;
    if (OB_NOT_NULL(const_param_cache)) {
      if (gis_arg1->is_static_const_) {
        cache_geo = const_param_cache->get_cached_geo(0);
        if (cache_geo == NULL
          && OB_FAIL(ObGeoTypeUtil::create_cached_geometry(*const_param_cache->get_allocator(),
                                                            temp_allocator,
                                                            const_param_cache->get_const_param_cache(0),
                                                            srs,
                                                            cache_geo))) {
          LOG_WARN("add geo2 to const cache failed", K(ret));
        } else {
          geo = &geo2;
          const_param_cache->add_cached_geo(0, cache_geo);
        }
      } else if (gis_arg2->is_static_const_) {
        cache_geo = const_param_cache->get_cached_geo(1);
        if (cache_geo == NULL
          && OB_FAIL(ObGeoTypeUtil::create_cached_geometry(*const_param_cache->get_allocator(),
                                                            temp_allocator,
                                                            const_param_cache->get_const_param_cache(1),
                                                            srs,
                                                            cache_geo))) {
          LOG_WARN("add geo2 to const cache failed", K(ret));
        } else {
          geo = &geo1;
          const_param_cache->add_cached_geo(1, cache_geo);
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_NOT_NULL(cache_geo)) {
      if (OB_FAIL(cache_geo->intersects(*geo, gis_context, result))) {
        LOG_WARN("get intersects result failed", K(ret));
      } else {
        res = result;
      }
    } else if (ObGeoTypeUtil::use_point_polygon_short_circuit(geo1, geo2, T_FUN_SYS_ST_INTERSECTS)) {
      result = false;
      if (OB_FAIL(ObGeoTypeUtil::get_point_polygon_res(&geo1, &geo2, T_FUN_SYS_ST_INTERSECTS, result))) {
        LOG_WARN("fail to get res.", K(ret));
      }
    } else if (OB_FAIL(ObGeoFunc<ObGeoFuncType::Intersects>::geo_func::eval(gis_context, result))) {
      LOG_WARN("eval st intersection failed", K(ret));
      ObGeoExprUtils::geo_func_error_handle(ret, N_ST_INTERSECTS);
    } else if (lib::is_mysql_mode() && geo1.type() == ObGeoType::POINT
                    && geo2.type() == ObGeoType::POINT
                    && result == true
                    && OB_FAIL(ObGeoTypeUtil::eval_point_box_intersects(srs, &geo1, &geo2, result))) {
      LOG_WARN("eval box intersection failed", K(ret));
    }
    if (OB_FAIL(ret)) {
    } else {
      res = result;
    }
  }
  return ret;
}

} // sql
} // oceanbase
