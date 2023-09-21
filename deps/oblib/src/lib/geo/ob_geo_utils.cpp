
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
 * This file contains implementation support for the geometry utils abstraction.
 */

#include "lib/geo/ob_geo.h"
#include "lib/geo/ob_srs_info.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include <cstdint>
#define USING_LOG_PREFIX LIB

#include "lib/geo/ob_geo_utils.h"
#include "lib/geo/ob_geo_tree.h"
#include "lib/geo/ob_geo_wkb_visitor.h"
#include "lib/geo/ob_geo_wkb_size_visitor.h"
#include "lib/geo/ob_geo_to_wkt_visitor.h"
#include "lib/geo/ob_geo_func_register.h"
#include "lib/geo/ob_geo_func_common.h"
#include "lib/geo/ob_geo_func_envelope.h"
#include "lib/geo/ob_geo_wkb_check_visitor.h"
#include "lib/geo/ob_geo_normalize_visitor.h"
#include "lib/geo/ob_geo_coordinate_range_visitor.h"

namespace oceanbase
{
namespace common
{

// 这里只考虑进行非multi类型的比对，multi类型需额外处理
bool ObGeoTypeUtil::is_geo1_dimension_higher_than_geo2(ObGeoType type1, ObGeoType type2)
{
  bool res = false;
	if ( (type2 == ObGeoType::POINT && type1 == ObGeoType::LINESTRING)
		|| (type2 == ObGeoType::POINT && type1 == ObGeoType::POLYGON)
		|| (type2 == ObGeoType::LINESTRING && type1 == ObGeoType::POLYGON))
	{
    res = true;
	}
  return res;
}

bool ObGeoTypeUtil::is_pg_reserved_srid(uint32_t srid)
{
  return srid >= SRID_WORLD_MERCATOR_PG && srid < SRID_LAEA_END_PG;
}

int ObGeoTypeUtil::get_pg_reserved_prj4text(ObIAllocator *allocator, uint32_t srid, ObString &prj4_param)
{
  int ret = OB_SUCCESS;
  const uint32_t MAX_PRJ4_LEN = 512;
  char tmp_buf[MAX_PRJ4_LEN] = {0};
  if (srid == SRID_WORLD_MERCATOR_PG) {
    strncpy(tmp_buf, "+proj=merc +lon_0=0 +k=1 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs",
            MAX_PRJ4_LEN);
  } else if (srid >= SRID_NORTH_UTM_START_PG && srid <= SRID_NORTH_UTM_END_PG) {
    snprintf(tmp_buf, MAX_PRJ4_LEN, "+proj=utm +zone=%d +ellps=WGS84 +datum=WGS84 +units=m +no_defs",
             srid - SRID_NORTH_UTM_START_PG + 1 );
  } else if (srid == SRID_NORTH_LAMBERT_PG) {
		strncpy(tmp_buf, "+proj=laea +lat_0=90 +lon_0=-40 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs",
            MAX_PRJ4_LEN);
  } else if (srid == SRID_NORTH_STEREO_PG) {
		strncpy(tmp_buf, "+proj=stere +lat_0=90 +lat_ts=71 +lon_0=0 +k=1 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs",
            MAX_PRJ4_LEN);
  } else if (srid >= SRID_SOUTH_UTM_START_PG &&
            srid <= SRID_SOUTH_UTM_END_PG) {
    snprintf(tmp_buf, MAX_PRJ4_LEN, "+proj=utm +zone=%d +south +ellps=WGS84 +datum=WGS84 +units=m +no_defs",
             srid - SRID_SOUTH_UTM_START_PG + 1 );
  } else if (srid == SRID_SOUTH_LAMBERT_PG) {
		strncpy(tmp_buf, "+proj=laea +lat_0=-90 +lon_0=0 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs",
            MAX_PRJ4_LEN);
  } else if (srid >= SRID_LAEA_START_PG && srid < SRID_LAEA_END_PG) {
			int zone = srid - SRID_LAEA_START_PG;
			int xzone = zone % 20;
			int yzone = zone / 20;
			double lat_0 = 30.0 * (yzone - 3) + 15.0;
			double lon_0 = 0.0;
			if  ( yzone == 2 || yzone == 3 ) {
        lon_0 = 30.0 * (xzone - 6) + 15.0;
      } else if ( yzone == 1 || yzone == 4 ) {
        lon_0 = 45.0 * (xzone - 4) + 22.5;
      } else if ( yzone == 0 || yzone == 5 ) {
        lon_0 = 90.0 * (xzone - 2) + 45.0;
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid pg srid", K(ret), K(srid), K(xzone), K(yzone));
      }
      if (OB_SUCC(ret)) {
        while (lon_0 > 180) {
          lon_0 -= 360;
        }
        while (lon_0 < -180) {
          lon_0 += 360;
        }
        snprintf(tmp_buf, MAX_PRJ4_LEN, "+proj=laea +ellps=WGS84 +datum=WGS84 +lat_0=%g +lon_0=%g +units=m +no_defs",
                 lat_0, lon_0);
      }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid pg srid", K(ret), K(srid));
  }

  if (OB_SUCC(ret)) {
    ObString prj4_tmp = ObString::make_string(tmp_buf);
    if (OB_FAIL(OB_FAIL(ob_write_string(*allocator, prj4_tmp, prj4_param, true)))) {
      LOG_WARN("failed to write string to buffer", K(ret), K(srid));
    }
  }
  return ret;
}

ObGeoType ObGeoTypeUtil::get_geo_type_by_name(ObString &name)
{
  ObGeoType geo_type = ObGeoType::GEOTYPEMAX;

  if (0 == name.case_compare("point")) {
    geo_type = ObGeoType::POINT;
  } else if (0 == name.case_compare("linestring")) {
    geo_type = ObGeoType::LINESTRING;
  } else if (0 == name.case_compare("polygon")) {
    geo_type = ObGeoType::POLYGON;
  } else if (0 == name.case_compare("multipoint")) {
    geo_type = ObGeoType::MULTIPOINT;
  } else if (0 == name.case_compare("multilinestring")) {
    geo_type = ObGeoType::MULTILINESTRING;
  } else if (0 == name.case_compare("multipolygon")) {
    geo_type = ObGeoType::MULTIPOLYGON;
  } else if (0 == name.case_compare("geometrycollection")
             || 0 == name.case_compare("geomcollection")) {
    geo_type = ObGeoType::GEOMETRYCOLLECTION;
  } else if (0 == name.case_compare("geometry")) {
    geo_type = ObGeoType::GEOMETRY;
  } else {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "get type by name failed", K(geo_type));
  }

  return geo_type;
}

const char *ObGeoTypeUtil::get_geo_name_by_type(ObGeoType type)
{
  const char *type_name = "UNKNOWN";
  switch (type) {
    case ObGeoType::POINT:{
      type_name =  "POINT";
      break;
    }
    case ObGeoType::LINESTRING:{
      type_name =  "LINESTRING";
      break;
    }
    case ObGeoType::POLYGON:{
      type_name = "POLYGON";
      break;
    }
    case ObGeoType::MULTIPOINT:{
      type_name = "MULTIPOINT";
      break;
    }
    case ObGeoType::MULTILINESTRING:{
      type_name = "MULTILINESTRING";
      break;
    }
    case ObGeoType::MULTIPOLYGON:{
      type_name = "MULTIPOLYGON";
      break;
    }
    case ObGeoType::GEOMETRYCOLLECTION:{
      type_name = "GEOMETRYCOLLECTION";
      break;
    }
    default:{
      LOG_WARN_RET(OB_INVALID_ARGUMENT, "unknown geometry type", K(type));
      break;
    }
  }
  return type_name;
}

int ObGeoTypeUtil::create_geo_by_type(ObIAllocator &allocator,
                                      ObGeoType geo_type,
                                      bool is_geographical,
                                      bool is_geo_bin,
                                      ObGeometry *&geo,
                                      uint32_t srid/* = 0 */)
{
  int ret = OB_SUCCESS;

  if(is_geo_bin) {
    if (is_geographical) {
      ret = create_geo_bin_by_type<ObIWkbGeogPoint, ObIWkbGeogLineString,
          ObIWkbGeogPolygon, ObIWkbGeogMultiPoint, ObIWkbGeogMultiLineString,
          ObIWkbGeogMultiPolygon, ObIWkbGeogCollection>(allocator, geo_type, geo, srid);
    } else {
      ret = create_geo_bin_by_type<ObIWkbGeomPoint, ObIWkbGeomLineString,
          ObIWkbGeomPolygon, ObIWkbGeomMultiPoint, ObIWkbGeomMultiLineString,
          ObIWkbGeomMultiPolygon, ObIWkbGeomCollection>(allocator, geo_type, geo, srid);
    }
  } else {
    if (is_geographical) {
      ret = create_geo_tree_by_type<ObGeographPoint, ObGeographLineString,
          ObGeographPolygon, ObGeographMultipoint, ObGeographMultilinestring,
          ObGeographMultipolygon, ObGeographGeometrycollection>(allocator, geo_type, geo, srid);
    } else {
      ret = create_geo_tree_by_type<ObCartesianPoint, ObCartesianLineString,
          ObCartesianPolygon, ObCartesianMultipoint, ObCartesianMultilinestring,
          ObCartesianMultipolygon, ObCartesianGeometrycollection>(allocator, geo_type, geo, srid);
    }
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("fail to create geo by type", K(ret), K(geo_type), K(is_geographical), K(is_geo_bin));
  }

  return ret;
}

int ObGeoTypeUtil::create_geo_by_wkb(ObIAllocator &allocator,
                                     const ObString &swkb,
                                     const ObSrsItem *srs,
                                     ObGeometry *&geo,
                                     bool need_check, /* = true */
                                     bool need_copy /* = true */)
{
  int ret = OB_SUCCESS;

  if (swkb.length() < WKB_COMMON_WKB_HEADER_LEN) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("invalid swkb length", K(ret), K(swkb.length()));
  } else {
    ObGeoWkbHeader header;
    ObGeoCRS crs = ObGeoCRS::Cartesian;
    if (OB_FAIL(ObGeoTypeUtil::get_header_info_from_wkb(swkb, header))) {
      LOG_WARN("fail to get swkb header info from swkb", K(ret), K(swkb));
    } else if (OB_NOT_NULL(srs)) {
      crs = (srs->srs_type() == ObSrsType::PROJECTED_SRS)
          ? ObGeoCRS::Cartesian : ObGeoCRS::Geographic;
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(ObGeoTypeUtil::create_geo_by_type(allocator, header.type_,
        crs == ObGeoCRS::Geographic, true, geo))) {
      LOG_WARN("fail to create geo by type", K(ret), K(crs), K(header));
    } else {
      geo->set_srid(header.srid_);
      uint32_t offset = 0;
      ObString wkb;
      if (OB_FAIL(ObGeoTypeUtil::get_wkb_from_swkb(swkb, wkb, offset))) {
        LOG_WARN("fail to get wkb from swkb", K(ret), K(swkb));
      } else {
        if (need_copy) {
          ObString wkb_data;
          if (OB_FAIL(ob_write_string(allocator, wkb, wkb_data))) {
            LOG_WARN("Failed to copy swkb memory", K(ret));
          } else {
            geo->set_data(wkb_data);
          }
        } else {
          geo->set_data(wkb);
        }
      }

      if (OB_FAIL(ret)) {
        // do nothing
      } else if (need_check) {
        ObGeoWkbCheckVisitor wkb_check(wkb, header.bo_, false);
        ObIWkbGeometry *geo_bin = static_cast<ObIWkbGeometry *>(geo);
        if (OB_FAIL(geo->do_visit(wkb_check))) {
          ret = OB_ERR_GIS_INVALID_DATA;
          LOG_WARN("fail to check swkb by swkb checker", K(ret), K(swkb), K(header), K(crs));
        } else if (geo->length() + offset != swkb.length()) {
          ret = OB_ERR_GIS_INVALID_DATA;
          LOG_WARN("invalid swkb length", K(ret), K(geo->length()), K(swkb.length()), K(offset));
        }
      }
    }
  }

  return ret;
}

int ObGeoTypeUtil::build_geometry(ObIAllocator &allocator,
                                  const ObString &swkb,
                                  ObGeometry *&geo,
                                  const ObSrsItem *srs,
                                  ObGeoErrLogInfo &log_info,
                                  const bool need_normalize, /* = true */
                                  const bool need_check_ring, /* = false */
                                  const bool need_correct /* = true */)
{
  int ret = OB_SUCCESS;
  ObGeoWkbHeader header;
  ObGeoCRS crs = ObGeoCRS::Cartesian;
  if (OB_NOT_NULL(srs)) {
    crs = (srs->srs_type() == ObSrsType::PROJECTED_SRS) ?
        ObGeoCRS::Cartesian : ObGeoCRS::Geographic;
  }

  // parse wkb_nosrid
  if (OB_FAIL(ObGeoTypeUtil::get_header_info_from_wkb(swkb, header))) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("get swkb header info from swkb failed", K(swkb), K(ret));
  } else if (ObGeoWkbByteOrder::BigEndian != header.bo_ && ObGeoWkbByteOrder::LittleEndian != header.bo_) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("invalid byte order", K(swkb), K(ret));
  } else if (ObGeoType::GEOMETRY > header.type_ || ObGeoType::GEOTYPEMAX < header.type_) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("invalid geo type", K(ret), K(header.type_));
  } else if (OB_FAIL(ObGeoTypeUtil::create_geo_by_type(allocator,
                                                        header.type_,
                                                        ObGeoCRS::Geographic == crs,
                                                        true,
                                                        geo))) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("failed to create swkb", K(ret), K(crs), K(header.type_));
  } else {
    geo->set_srid(header.srid_);
    ObString wkb;
    uint32_t offset = 0;
    ObString wkb_data;
    if (OB_FAIL(ObGeoTypeUtil::get_wkb_from_swkb(swkb, wkb, offset))) {
        LOG_WARN("fail to get wkb from swkb", K(ret), K(swkb));
    } else if (OB_FAIL(ob_write_string(allocator, wkb, wkb_data))) {
      LOG_WARN("Failed to copy swkb memory", K(ret));
    } else {
      geo->set_data(wkb_data);
      ObGeoWkbCheckVisitor wkb_check(wkb, header.bo_, need_check_ring);
      if (OB_FAIL(geo->do_visit(wkb_check))) {
        ret = OB_ERR_GIS_INVALID_DATA;
        LOG_WARN("invalid swkb", K(swkb), K(header.type_), K(header.srid_), K(crs));
      } else if (geo->length() + offset != swkb.length()) {
        ret = OB_ERR_GIS_INVALID_DATA;
        LOG_WARN("invalid swkb length", K(ret), K(geo->length()), K(swkb.length()), K(offset));
      } else if (need_normalize && ObGeoCRS::Geographic == crs) {
        ObGeoNormalizeVisitor normalize_visitor(srs);
        if (OB_FAIL(geo->do_visit(normalize_visitor))) {
          LOG_WARN("normalize geo failed", K(ret));
        } else {
          geo->set_zoom_in_value(normalize_visitor.get_zoom_in_value());
        }
      }

      if (OB_SUCC(ret) && need_correct && OB_FAIL(correct_polygon(allocator, srs,
          wkb_check.is_ring_closed(), *geo))) {
        LOG_WARN("correct geo failed", K(ret), K(geo));
      }
    }
  }

  // check coordinate range
  if (OB_SUCC(ret) && ObGeoCRS::Geographic == crs) {
    if (need_normalize && OB_FAIL(check_coordinate_range(srs, geo, log_info, true, true))) {
      LOG_WARN("failed to check coordinate range", K(ret));
    } else if (!need_normalize && OB_FAIL(check_coordinate_range(srs, geo, log_info, true))) {
      LOG_WARN("failed to check coordinate range", K(ret));
    }
  }

  return ret;
}

// for st_aswkb/st_asbinary
int ObGeoTypeUtil::construct_geometry(ObIAllocator &allocator,
                                      const ObString &swkb,
                                      const ObSrsItem *srs,
                                      ObGeometry *&geo,
                                      bool has_srid /* = true */)
{
  int ret = OB_SUCCESS;
  // has_srid is always true currently
  const uint32_t wkb_header_sz = WKB_GEO_BO_SIZE + WKB_GEO_TYPE_SIZE;
  const int64_t len = swkb.length();
  const char *data = swkb.ptr();
  ObGeoWkbByteOrder bo = ObGeoWkbByteOrder::INVALID;
  ObGeoType type = ObGeoType::GEOTYPEMAX;
  ObString wkb;
  uint32_t offset = 0;
  if (OB_FAIL(ObGeoTypeUtil::get_wkb_from_swkb(swkb, wkb, offset))) {
    // adapt mysql
    ret = OB_ERR_GIS_DATA_WRONG_ENDIANESS;
    LOG_WARN("fail to get wkb from swkb", K(ret), K(swkb));
  } else if (FALSE_IT(bo = static_cast<ObGeoWkbByteOrder>(*(data + offset)))) {
  } else if (ObGeoWkbByteOrder::LittleEndian != bo) {
    ret = OB_ERR_GIS_DATA_WRONG_ENDIANESS;
    LOG_WARN("invalid byte order", K(ret), K(bo));
  } else if (FALSE_IT(type = static_cast<ObGeoType>(ObGeoWkbByteOrderUtil::read<uint32_t>(
        data + offset + WKB_GEO_BO_SIZE, bo)))) {
  } else if (ObGeoType::POINT > type || ObGeoType::GEOTYPEMAX < type) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("invalid geo type", K(ret), K(type));
  } else {
    ObGeoCRS crs = ObGeoCRS::Cartesian;
    ObGeoSrid srid = 0;
    int tmp_ret = OB_SUCCESS;
    if (has_srid) {
      srid = static_cast<ObGeoSrid>(ObGeoWkbByteOrderUtil::read<uint32_t>(data, bo));
    }
    if (0 == srid) {
      crs = ObGeoCRS::Cartesian;
    } else if (OB_NOT_NULL(srs)) {
      crs = (srs->srs_type() == ObSrsType::PROJECTED_SRS)
          ? ObGeoCRS::Cartesian : ObGeoCRS::Geographic;
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(ObGeoTypeUtil::create_geo_by_type(allocator, type,
        crs == ObGeoCRS::Geographic, true, geo))) {
      LOG_WARN("fail to create geo by type", K(ret), K(crs), K(type));
    } else if (FALSE_IT(geo->set_srid(srid))) {
    } else if (ObGeoType::POINT == type && (len - offset - wkb_header_sz < WKB_POINT_DATA_SIZE)) {
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_WARN("invalid point data length", K(ret), K(len));
    } else {
      ObString wkb_copy;
      if (OB_FAIL(ob_write_string(allocator, wkb, wkb_copy))) {
        LOG_WARN("Failed to copy wkb memory", K(ret));
      } else {
        geo->set_data(wkb_copy);
        if (has_srid) {
          geo->set_srid(srid);
        }
      }

      if (OB_FAIL(ret)) {
        // do nothing
      } else {
        ObGeoWkbCheckVisitor wkb_check(wkb, bo, false);
        ObIWkbGeometry *geo_bin = static_cast<ObIWkbGeometry *>(geo);
        if (OB_FAIL(geo->do_visit(wkb_check))) {
          LOG_WARN("fail to check wkb by wkb checker", K(ret), K(wkb), K(crs));
          ret = OB_ERR_GIS_INVALID_DATA;
        } else if (has_srid && (geo->length() + offset) != len) {
          ret = OB_ERR_GIS_INVALID_DATA;
          LOG_WARN("fail to check wkb data len", K(ret), K(len), K(geo->length()));
        }
      }
    }
  }

  return ret;
}

int ObGeoTypeUtil::correct_polygon(ObIAllocator &alloc,
                                   const ObSrsItem *srs,
                                   bool is_ring_closed,
                                   ObGeometry &geo)
{
  int ret = OB_SUCCESS;

  if (geo.type() == ObGeoType::POLYGON
      || geo.type() == ObGeoType::MULTIPOLYGON
      || geo.type() == ObGeoType::GEOMETRYCOLLECTION) {
    if (!is_ring_closed && !geo.is_tree() &&
        OB_FAIL(ObGeoTypeUtil::geo_close_ring(geo, alloc))) {
      LOG_WARN("wkb close ring failed", K(ret));
    }
    if (OB_SUCC(ret)) {
      ObGeoEvalCtx correct_context(&alloc, srs);
      int res_unused;
      if (OB_FAIL(correct_context.append_geo_arg(&geo))) {
        LOG_WARN("build geo gis context failed", K(ret));
      } else if (OB_FAIL(ObGeoFunc<ObGeoFuncType::Correct>::geo_func::eval(correct_context, res_unused))) {
        LOG_WARN("eval geo correct failed", K(ret));
      }
    }
  }

  return ret;
}

int ObGeoTypeUtil::check_coordinate_range(const ObSrsItem *srs,
                                          ObGeometry *geo,
                                          ObGeoErrLogInfo &log_info,
                                          const bool is_param, /* = false */
                                          const bool is_normalized /* = false */)
{
  int ret = OB_SUCCESS;
  ObGeoCoordinateRangeVisitor range_visitor(srs, is_normalized);

  if (OB_FAIL(geo->do_visit(range_visitor))) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("failed to reverse geometry coordinate", K(ret));
  } else if (range_visitor.is_longtitude_out_of_range()) {
    // handle log user error message
    if (OB_FAIL(srs->longtitude_convert_from_radians(-M_PI, log_info.min_long_val_))) {
      LOG_WARN("failed to convert longitude from radians", K(ret));
    } else if (OB_FAIL(srs->longtitude_convert_from_radians(M_PI, log_info.max_long_val_))) {
      LOG_WARN("failed to convert longitude from radians", K(ret));
    } else {
      log_info.value_out_of_range_ = range_visitor.value_out_of_range();
      if (is_param) {
        ret = OB_ERR_GEOMETRY_PARAM_LONGITUDE_OUT_OF_RANGE;
        LOG_WARN("parameter geometry longitude is out of range", "longitude value",
            range_visitor.value_out_of_range());
      } else {
        ret = OB_ERR_LONGITUDE_OUT_OF_RANGE;
        LOG_WARN("geometry longitude is out of range", "longitude value",
            range_visitor.value_out_of_range());
      }
    }
  } else if (range_visitor.is_latitude_out_of_range()) {
    // handle log user error message
    if (OB_FAIL(srs->latitude_convert_from_radians(-M_PI_2, log_info.min_lat_val_))) {
      LOG_WARN("failed to convert latitude from radians", K(ret));
    } else if (OB_FAIL(srs->latitude_convert_from_radians(M_PI_2, log_info.max_lat_val_))) {
      LOG_WARN("failed to convert latitude from radians", K(ret));
    } else {
      log_info.value_out_of_range_ = range_visitor.value_out_of_range();
      if (is_param) {
        ret = OB_ERR_GEOMETRY_PARAM_LATITUDE_OUT_OF_RANGE;
        LOG_WARN("parameter geometry latitude is out of range", "latitude value",
            range_visitor.value_out_of_range());
      } else {
        ret = OB_ERR_LATITUDE_OUT_OF_RANGE;
        LOG_WARN("geometry latitude is out of range", "latitude value",
            range_visitor.value_out_of_range());
      }
    }
  }

  return ret;
}

int ObGeoTypeUtil::get_buffered_geo(ObArenaAllocator *allocator,
                                    const ObString &wkb_str,
                                    double distance,
                                    const ObSrsItem *srs,
                                    ObString &res_wkb)
{
  ObGeoBufferStrategy buf_strat;
  ObGeoEvalCtx gis_context(allocator, srs);
  int correct_result;
  int ret = OB_SUCCESS;
  ObGeometry *geo = NULL;
  ObGeometry *res_geo = NULL;
  buf_strat.distance_val_ = distance;

  if (OB_FAIL(ObGeoTypeUtil::create_geo_by_wkb(*allocator, wkb_str, srs, geo, false))) {
    LOG_WARN("failed to create geo by wkb", K(ret));
  } else if (OB_FAIL(gis_context.append_geo_arg(geo))) {
    LOG_WARN("failed to append geo arg to gis context", K(ret), K(gis_context.get_geo_count()));
  } else if (OB_FAIL(ObGeoFunc<ObGeoFuncType::Correct>::geo_func::eval(gis_context, correct_result))) {
    LOG_WARN("eval boost correct failed", K(ret));
  } else if (OB_FAIL(gis_context.append_val_arg(&buf_strat))) {
    LOG_WARN("failed to append buffer strategy to gis context", K(ret), K(gis_context.get_geo_count()));
  } else if (OB_FAIL(ObGeoFunc<ObGeoFuncType::Buffer>::geo_func::eval(gis_context, res_geo))) {
    LOG_WARN("eval st_buffer failed", K(ret));
  } else if (OB_ISNULL(res_geo)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("eval st_buffer null result", K(ret));
  } else if (OB_FAIL(ObGeoTypeUtil::to_wkb(*allocator,
                                           *res_geo,
                                           srs,
                                           res_wkb))) {
    LOG_WARN("transform to binary failed", K(ret));
  }

  return ret;
}

int ObGeoTypeUtil::get_header_info_from_wkb(const ObString &wkb, ObGeoWkbHeader &header) {
  int ret = OB_SUCCESS;
  if (wkb.length() < WKB_GEO_SRID_SIZE + WKB_GEO_BO_SIZE + WKB_GEO_TYPE_SIZE) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("invalid wkb length", K(wkb.length()));
  } else {
    uint32_t offset = WKB_GEO_SRID_SIZE;
    uint8_t version = (*(wkb.ptr() + offset));
    if (IS_GEO_VERSION(version)) {
      if (GET_GEO_VERSION(version) != GEO_VESION_1) {
        ret = OB_ERR_GIS_INVALID_DATA;
        LOG_WARN("invalid wkb version", K(version));
      } else {
        offset +=  WKB_VERSION_SIZE;
        header.ver_ = GET_GEO_VERSION(version);
      }
    }
    if (OB_SUCC(ret)) {
      header.bo_ = static_cast<ObGeoWkbByteOrder>(*(wkb.ptr() + offset));
      if (ObGeoWkbByteOrder::BigEndian != header.bo_ && ObGeoWkbByteOrder::LittleEndian != header.bo_) {
        ret = OB_ERR_GIS_INVALID_DATA;
        LOG_WARN("invalid byte order", K(ret), K(header.bo_));
      } else {
        const char *ptr = (const char *)(wkb.ptr() + offset + WKB_GEO_BO_SIZE);
        header.srid_ = ObGeoWkbByteOrderUtil::read<uint32_t>((const char *)wkb.ptr(), header.bo_);
        header.type_ = static_cast<ObGeoType>(ObGeoWkbByteOrderUtil::read<uint32_t>(ptr, header.bo_));
      }
    }
  }
  return ret;
}

int ObGeoTypeUtil::get_type_srid_from_wkb(const ObString &wkb, ObGeoType &type, uint32_t &srid)
{
  int ret = OB_SUCCESS;
  ObGeoWkbHeader header;
  if (OB_FAIL(get_header_info_from_wkb(wkb, header))) {
    LOG_WARN("failed to get info from wkb", K(ret));
  } else {
    type = header.type_;
    srid = header.srid_;
  }
  return ret;
}

int ObGeoTypeUtil::get_srid_from_wkb(const ObString &wkb, uint32_t &srid)
{
  int ret = OB_SUCCESS;
  ObGeoWkbHeader header;
  if (OB_FAIL(get_header_info_from_wkb(wkb, header))) {
    LOG_WARN("failed to get info from wkb", K(ret));
  } else {
    srid = header.srid_;
  }
  return ret;
}

int ObGeoTypeUtil::get_type_from_wkb(const ObString &wkb, ObGeoType &type)
{
  int ret = OB_SUCCESS;
  ObGeoWkbHeader header;
  if (OB_FAIL(get_header_info_from_wkb(wkb, header))) {
    LOG_WARN("failed to get info from wkb", K(ret));
  } else {
    type = header.type_;
  }
  return ret;
}

int ObGeoTypeUtil::get_bo_from_wkb(const ObString &wkb, ObGeoWkbByteOrder &bo)
{
  int ret = OB_SUCCESS;
  ObGeoWkbHeader header;
  if (OB_FAIL(get_header_info_from_wkb(wkb, header))) {
    LOG_WARN("failed to get info from wkb", K(ret));
  } else {
    bo = header.bo_;
  }
  return ret;
}

int ObGeoTypeUtil::to_wkb(ObIAllocator &allocator,
                          ObGeometry &geo,
                          const ObSrsItem *srs_item,
                          ObString &res_wkb,
                          bool need_convert /* = true */)
{
  int ret = OB_SUCCESS;
  ObGeoWkbSizeVisitor wkb_size_visitor;

  if (OB_FAIL(geo.do_visit(wkb_size_visitor))) {
    LOG_WARN("failed to do wkb size visitor", K(ret));
  } else {
    // swkb format : srid + version + wkb
    uint64_t res_size = WKB_GEO_SRID_SIZE + WKB_VERSION_SIZE + wkb_size_visitor.geo_size();
    char *res_buf = reinterpret_cast<char *>(allocator.alloc(res_size));
    if (OB_ISNULL(res_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory", K(ret), K(res_size));
    } else {
      uint32_t srid = srs_item == NULL ? 0 : srs_item->get_srid();
      ObGeoWkbByteOrderUtil::write<uint32_t>(res_buf, srid);
      uint8_t encode_ver = geo.get_version() | GEO_VER_MASK;
      *(res_buf + WKB_GEO_SRID_SIZE) = encode_ver;
      ObString wkb_nosrid_buf(res_size, WKB_GEO_SRID_SIZE + WKB_VERSION_SIZE, res_buf);
      ObGeoWkbVisitor wkb_visitor(srs_item, &wkb_nosrid_buf, need_convert);
      if (OB_FAIL(geo.do_visit(wkb_visitor))) {
        LOG_WARN("failed to do wkb visit", K(ret), K(srid));
      } else {
        res_wkb.assign_ptr(res_buf, res_size);
      }
    }
  }

  return ret;
}

int ObGeoTypeUtil::get_wkb_from_swkb(const ObString &swkb, ObString &wkb, uint32_t &offset)
{
  int ret = OB_SUCCESS;
  if (swkb.length() < WKB_GEO_SRID_SIZE + WKB_GEO_BO_SIZE + WKB_GEO_TYPE_SIZE) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("invalid swkb length", K(swkb.length()));
  } else {
    uint8_t version = (*(swkb.ptr() + WKB_GEO_SRID_SIZE));
    if (IS_GEO_VERSION(version)) {
      if (GET_GEO_VERSION(version) != GEO_VESION_1) {
        // adapt mysql
        ret = OB_ERR_GIS_INVALID_DATA;
        LOG_WARN("invalid swkb version", K(version));
      } else {
        offset = WKB_GEO_SRID_SIZE + WKB_VERSION_SIZE;
      }
    } else {
      offset = WKB_GEO_SRID_SIZE;
    }

    if (OB_SUCC(ret)) {
      wkb.assign_ptr(swkb.ptr() + offset, swkb.length() - offset);
    }
  }
  return ret;
}

// ObGeoBoxUtil
double ObGeoBoxUtil::vector_dot_product(const ObPoint3d &p3d1, const ObPoint3d &p3d2)
{
  return (p3d1.x * p3d2.x) + (p3d1.y * p3d2.y) + (p3d2.z * p3d2.z);
}

void ObGeoBoxUtil::vector_cross_product(const ObPoint3d &p3d1, const ObPoint3d &p3d2, ObPoint3d &res)
{
  res.x = p3d1.y * p3d2.z - p3d1.z * p3d2.y;
  res.y = p3d1.z * p3d2.x - p3d1.x * p3d2.z;
  res.z = p3d1.x * p3d2.y - p3d1.y * p3d2.x;
}

void ObGeoBoxUtil::vector_add(const ObPoint3d &p3d1, const ObPoint3d &p3d2, ObPoint3d &res)
{
  res.x = p3d1.x + p3d2.x;
  res.y = p3d1.y + p3d2.y;
  res.z = p3d1.z + p3d2.z;
}

void ObGeoBoxUtil::vector_minus(const ObPoint3d &p3d1, const ObPoint3d &p3d2, ObPoint3d &res)
{
  res.x = p3d1.x - p3d2.x;
  res.y = p3d1.y - p3d2.y;
  res.z = p3d1.z - p3d2.z;
}

bool ObGeoBoxUtil::is_float_equal(double left, double right)
{
  return fabs(left - right) <= FP_TOLERANCE;
}

bool ObGeoBoxUtil::is_same_point3d(const ObPoint3d &p3d1, const ObPoint3d &p3d2)
{
  bool res = true;
  if (is_float_equal(p3d1.x, p3d2.x) || is_float_equal(p3d1.y, p3d2.y) ||
      is_float_equal(p3d1.z, p3d2.z)) {
    res = false;
  }
  return res;
}

void ObGeoBoxUtil::get_unit_normal_vector(const ObPoint3d &p3d1, const ObPoint3d &p3d2, ObPoint3d &res)
{
  double dot_res = vector_dot_product(p3d1, p3d2);
  const double opposite_value = 0.95;
  ObPoint3d p3d_tmp;

  if (dot_res < 0) {
    vector_add(p3d1, p3d2, p3d_tmp);
    vector_3d_normalize(p3d_tmp);
  } else if (dot_res > opposite_value) {
    vector_minus(p3d1, p3d2, p3d_tmp);
    vector_3d_normalize(p3d_tmp);
  } else {
    p3d_tmp = p3d2;
  }
  vector_cross_product(p3d1, p3d_tmp, res);
  vector_3d_normalize(res);
}

void ObGeoBoxUtil::vector_2d_normalize(ObPoint2d &p2d)
{
  double len = sqrt(p2d.x * p2d.x + p2d.y * p2d.y);
  if (is_float_equal(len, 0.0)) {
    p2d.x = 0.0;
    p2d.y = 0.0;
  } else {
    p2d.x /= len;
    p2d.y /= len;
  }
}

void ObGeoBoxUtil::vector_3d_normalize(ObPoint3d &p3d)
{
  double len = sqrt(p3d.x * p3d.x + p3d.y * p3d.y + p3d.z * p3d.z);
  if (is_float_equal(len, 0.0)) {
    p3d.x = 0.0;
    p3d.y = 0.0;
    p3d.z = 0.0;
  } else {
    p3d.x /= len;
    p3d.y /= len;
    p3d.z /= len;
  }
}

int ObGeoBoxUtil::get_point_relative_location(const ObPoint2d &p1, const ObPoint2d &p2, const ObPoint2d &point)
{
  double rate_minus = ((p2.y - p1.y) * (point.x - p1.x) - (p2.x - p1.x) * (point.y - p1.y));
  int ret = 0;
  if (rate_minus > 0) {
    ret = 1; // on right side
  } else if (rate_minus < 0) {
    ret = -1; // on left side
  }
  return ret;
}

void ObGeoBoxUtil::convert_ll_to_cartesian3d(const ObWkbGeogInnerPoint &point, ObPoint3d &p3d)
{
  double radian_x = M_PI * point.get<0>() / 180.0;
  double radian_y = M_PI * point.get<1>() / 180.0;
  double cos_radian_y = cos(radian_y);
  p3d.x = cos(radian_x) * cos_radian_y;
  p3d.y = sin(radian_x) * cos_radian_y;
  p3d.z = sin(radian_y);
}

bool ObGeoBoxUtil::is_completely_opposite(const ObPoint3d &p1, const ObPoint3d &p2)
{
  return is_float_equal(p1.x, -1 * p2.x) && is_float_equal(p1.y, -1 * p2.y)
         && is_float_equal(p1.z, -1 * p2.z);
}


int ObGeoBoxUtil::caculate_line_box(ObPoint3d &start, ObPoint3d &end, ObGeogBox &box)
{
  int ret = OB_SUCCESS;
  box.xmin = start.x;
  box.xmax = start.x;
  box.ymin = start.y;
  box.ymax = start.y;
  box.zmin = start.z;
  box.zmax = start.z;
  point_box_uion(end, box);

  if (is_same_point3d(start, end)) {
    // do nothing
  } else if (is_completely_opposite(start, end)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid point", K(ret));
  } else {
    ObPoint3d tmp;
    ObPoint3d point3d;
    get_unit_normal_vector(start, end, tmp);
    get_unit_normal_vector(tmp, start, point3d);

    ObPoint2d p2d1 = {1.0, 0.0};
    ObPoint2d origin = {0.0, 0.0};
    ObPoint2d p2d2;
    p2d2.x = vector_dot_product(start, end);
    p2d2.y = vector_dot_product(end, point3d);
    int location = get_point_relative_location(p2d1, p2d2, origin);

    ObPoint3d axis_points[6];
    memset(axis_points, 0, 6 * sizeof(ObPoint3d));
    axis_points[0].x = 1.0;
    axis_points[1].x = -1.0;
    axis_points[2].y = 1.0;
    axis_points[3].y = -1.0;
    axis_points[4].z = 1.0;
    axis_points[5].z = -1.0;

    for (uint8_t i = 0; i < 6; i++) {
      ObPoint2d p2d_tmp;
      p2d_tmp.x = vector_dot_product(axis_points[i], start);
      p2d_tmp.x = vector_dot_product(axis_points[i], point3d);
      vector_2d_normalize(p2d_tmp);
      if (get_point_relative_location(p2d1, p2d2, p2d_tmp) != location) {
        ObPoint3d p3d;
        p3d.x = p2d_tmp.x * start.x + p2d_tmp.y * point3d.x;
        p3d.y = p2d_tmp.x * start.y + p2d_tmp.y * point3d.y;
        p3d.z = p2d_tmp.x * start.z + p2d_tmp.y * point3d.z;
        point_box_uion(p3d, box);
      }
    }
  }
  return OB_SUCCESS;
}

void ObGeoBoxUtil::do_set_poles(const double &xmin, const double &xmax,
                            const double &ymin, const double &ymax,
                            double &zmin, double &zmax)
{
  if (xmin < 0.0 && xmax > 0.0 && ymin < 0.0 && ymax > 0.0) {
    if ((zmin > 0.0) && (zmax > 0.0)) {
      zmax = 1.0;
    } else if ((zmin < 0.0) && (zmax < 0.0)) {
      zmin = -1.0;
    } else {
      zmin = -1.0;
      zmax = 1.0;
    }
  }
}

void ObGeoBoxUtil::ob_geo_box_check_poles(ObGeogBox &box)
{
  do_set_poles(box.xmin, box.xmax, box.ymin, box.ymax, box.zmin, box.zmax);
  do_set_poles(box.xmin, box.xmax, box.zmin, box.zmax, box.ymin, box.ymax);
  do_set_poles(box.ymin, box.ymax, box.zmin, box.zmax, box.xmin, box.xmax);
}

void ObGeoBoxUtil::point_box_uion(ObPoint3d &point, ObGeogBox &box)
{
  box.xmin = OB_MIN(point.x, box.xmin);
  box.ymin = OB_MIN(point.y, box.ymin);
  box.zmin = OB_MIN(point.z, box.zmin);
  box.xmax = OB_MAX(point.x, box.xmax);
  box.ymax = OB_MAX(point.y, box.ymax);
  box.zmax = OB_MAX(point.z, box.zmax);
}

void ObGeoBoxUtil::box_uion(ObGeogBox &box_tmp, ObGeogBox &box)
{
  box.xmin = OB_MIN(box_tmp.xmin, box.xmin);
  box.ymin = OB_MIN(box_tmp.ymin, box.ymin);
  box.zmin = OB_MIN(box_tmp.zmin, box.zmin);
  box.xmax = OB_MAX(box_tmp.xmax, box.xmax);
  box.ymax = OB_MAX(box_tmp.ymax, box.ymax);
  box.zmax = OB_MAX(box_tmp.zmax, box.zmax);
}

double ObGeoBoxUtil::correct_longitude(double longitude)
{
  if (longitude > 360.0) {
    longitude = remainder(longitude, 360.0);
  } else if (longitude < -360.0) {
    longitude = remainder(longitude, -360.0);
  }

  if (longitude > 180.0) {
    longitude -= 360.0;
  } else if (longitude < -180.0) {
    longitude += 360.0;
  }

  if (longitude == -180.0) {
    longitude = 180.0;
  } else if (longitude == -360.0) {
    longitude = 0.0;
  }
  return longitude;
}

double ObGeoBoxUtil::correct_latitude(double latitude)
{
  if (latitude > 360.0) {
    latitude = remainder(latitude, 360.0);
  } else if (latitude < -360.0) {
    latitude = remainder(latitude, -360.0);
  }

  if (latitude > 180.0) {
    latitude = 180.0 - latitude;
  } else if (latitude < -180.0) {
    latitude = -180.0 - latitude;
  }

  if (latitude > 90.0) {
    latitude = 180.0 - latitude;
  } else if (latitude < -90.0) {
    latitude = -180.0 - latitude;
  }
  return latitude;
}

void ObGeoBoxUtil::get_box_center(const ObGeogBox &box, ObPoint2d &center)
{
  double box_serialized[6];
  memcpy(box_serialized, &(box.xmin), sizeof(double) * 6);
  ObPoint3d p3d = {0.0, 0.0, 0.0};

  for (uint8_t i = 0; i < 8; i++) {
    ObPoint3d tmp;
    tmp.x = box_serialized[i / 4];
    tmp.y = box_serialized[2 + ((i % 4) / 2)];
    tmp.z = box_serialized[4 + (i % 2)];
    vector_3d_normalize(tmp);
    p3d.x += tmp.x;
    p3d.y += tmp.y;
    p3d.z += tmp.z;
  }
  p3d.x /= 8.0;
  p3d.y /= 8.0;
  p3d.z /= 8.0;
  vector_3d_normalize(p3d);
  double longitude = atan2(p3d.y, p3d.x);
  double latitude = asin(p3d.z);
  center.x = correct_longitude(180.0 * longitude / M_PI);
  center.y = correct_latitude(180.0 * latitude / M_PI);
}

double ObGeoBoxUtil::caculate_box_angular_height(const ObGeogBox &box)
{
  double box_serialized[6];
  memcpy(box_serialized, &(box.xmin), sizeof(double) * 6);
  double height_min = FLT_MAX;
  double height_max = -1 * FLT_MAX;
  uint8_t corner_num = 8;

  for (uint8_t i = 0; i < corner_num; i++) {
    ObPoint3d tmp;
    tmp.x = box_serialized[i / 4];
    tmp.y = box_serialized[2 + ((i % 4) / 2)];
    tmp.z = box_serialized[4 + (i % 2)];
    vector_3d_normalize(tmp);
    if (height_min > tmp.z) {
      height_min = tmp.z;
    }
    if (height_max < tmp.z) {
      height_max = tmp.z;
    }
  }
  return asin(height_max) - asin(height_min);
}

double ObGeoBoxUtil::caculate_box_angular_width(const ObGeogBox &box)
{
  double box_serialized[6];
  memcpy(box_serialized, &(box.xmin), sizeof(double) * 6);
  ObPoint3d p3d[3];
  double max_angular = -1 * FLT_MAX;

  double product = sqrt(box.xmin * box.xmin + box.ymin * box.ymin);
  p3d[0].x = box.xmin / product;
  p3d[0].y = box.ymin / product;

  for (uint8_t l = 0; l < 2; l++) {
    max_angular = -1 * FLT_MAX;
    for (uint8_t c = 0; c < 4; c++) {
      ObPoint3d tmp;
      tmp.x = box_serialized[c / 2];
      tmp.y = box_serialized[2 + (c % 2)];
      tmp.z = 0.0;
      product = sqrt(tmp.x * tmp.x + tmp.y * tmp.y);
      tmp.x /= product;
      tmp.y /= product;
      double dot_product = tmp.x * p3d[l].x + tmp.y * p3d[l].y;
      double angle = acos(dot_product > 1.0 ? 1.0 : dot_product);
      if (max_angular < angle) {
        p3d[l + 1] = tmp;
        max_angular = angle;
      }
    }
  }
  return max_angular;
}

int ObGeoBoxUtil::get_geog_point_box(const ObWkbGeogInnerPoint &point, ObGeogBox &box)
{
  ObPoint3d p3d;
  convert_ll_to_cartesian3d(point, p3d);
  box.xmin = p3d.x;
  box.xmax = p3d.x;
  box.ymin = p3d.y;
  box.ymax = p3d.y;
  box.zmin = p3d.z;
  box.zmax = p3d.z;
  return OB_SUCCESS;
}

int ObGeoBoxUtil::get_geog_poly_box(const ObWkbGeogPolygon &poly, ObGeogBox &box)
{
  int ret = OB_SUCCESS;
  ObGeogBox box_tmp;

  const ObWkbGeogLinearRing& exterior = poly.exterior_ring();
  if (OB_FAIL(get_geog_line_box(exterior, box_tmp))) {
    LOG_WARN("failed to caculate exterior ring box", K(ret));
  } else {
    box = box_tmp;
    const ObWkbGeogPolygonInnerRings& inner_rings = poly.inner_rings();
    ObWkbGeogPolygonInnerRings::iterator iter = inner_rings.begin();
    for ( ; iter != inner_rings.end() && OB_SUCC(ret); iter++) {
      if (OB_FAIL(get_geog_line_box(*iter, box_tmp))) {
        LOG_WARN("failed to caculate line box", K(ret));
      } else {
        box_uion(box_tmp, box);
      }
    }
    ob_geo_box_check_poles(box);
  }
  return ret;
}

// 1. geometry类型可以存储所有其他空间类型;
// 2. POINT, LINESTRING, 和 POLYGON只能存储各自对应的类型(由表达式校验);
// 3. GEOMETRYCOLLECTION可以存储任何类型的对象的集合，
//    MULTIPOINT, MULTILINESTRING, 和 MULTIPOLYGON将集合成员限制为具有特定几何类型的成员
int ObGeoTypeUtil::check_geo_type(const ObGeoType column_type, const ObString &wkb_str)
{
  int ret = OB_SUCCESS;
  ObGeoType inser_type = ObGeoType::GEOTYPEMAX;

  if (wkb_str.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wkb str is empty", K(ret), K(wkb_str));
  } else if (OB_FAIL(ObGeoTypeUtil::get_type_from_wkb(wkb_str, inser_type))) {
    LOG_WARN("fail to get geo type by wkb", K(ret), K(wkb_str));
  } else if (column_type >= ObGeoType::GEOTYPEMAX || inser_type >= ObGeoType::GEOTYPEMAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid geo type", K(ret), K(column_type), K(inser_type));
  } else if (ObGeoType::GEOMETRY == column_type) {
    // do nothing, geometry type can store all sub type.
  } else if (ObGeoType::GEOMETRYCOLLECTION == column_type) {
    if (ObGeoType::MULTIPOINT != inser_type &&
        ObGeoType::MULTILINESTRING != inser_type &&
        ObGeoType::MULTIPOLYGON != inser_type &&
        ObGeoType::GEOMETRYCOLLECTION != inser_type) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("column geo type not match insert geo type",
          K(ret), K(column_type), K(inser_type));
    }
  } else if (column_type != inser_type) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("column geo type not match insert geo type",
        K(ret), K(column_type), K(inser_type));
  }

  return ret;
}

// need_covert set to true, will do denormalize while convert tree to bin
int ObGeoTypeUtil::tree_to_bin(ObIAllocator &allocator,
                               ObGeometry *geo_tree,
                               ObGeometry *&geo_bin,
                               const ObSrsItem *srs_item,
                               bool need_convert/* = false */)
{
  int ret = OB_SUCCESS;
  ObString wkb_buf;
  if (OB_FAIL(ObGeoTypeUtil::to_wkb(allocator, *geo_tree, srs_item,
                                    wkb_buf, need_convert))) {
    LOG_WARN("failed to transform geo to binary", K(ret));
  } else {
    bool is_geographical = false;
    uint32_t srid = (NULL == srs_item ? 0 : srs_item->get_srid());
    if (OB_NOT_NULL(srs_item)) {
      is_geographical = srs_item->srs_type() == ObSrsType::GEOGRAPHIC_SRS;
    }
    if (OB_FAIL(ObGeoTypeUtil::create_geo_by_type(allocator, geo_tree->type(),
        is_geographical, true, geo_bin, srid))) {
      LOG_WARN("fail to create geo by type", K(ret), K(is_geographical), K(srid));
    } else {
      uint32_t offset = WKB_GEO_SRID_SIZE + WKB_VERSION_SIZE;
      ObString wkb_nosrid((wkb_buf.length() - offset), (wkb_buf.ptr() + offset));
      geo_bin->set_data(wkb_nosrid);
    }
  }
  return ret;
}

// (s)wkb to ewkt
// 1. axis-order in ob_storage/ewkt/ewkb always long-lat.
// 2. uint of geographic srs should be degree
// 3. should not srs in this function, because srs will not implement in liboblog
int ObGeoTypeUtil::geo_to_ewkt(const ObString &swkb,
                               ObString &ewkt,
                               ObIAllocator &allocator,
                               int64_t max_decimal_digits)
{
  int ret = OB_SUCCESS;
  ObGeometry *geo = NULL;
  ObGeoToWktVisitor wkt_visitor(&allocator);
  ObGeoWkbHeader header;
  ObString wkb;
  uint32_t offset = 0;
  if (max_decimal_digits == 0) {
    max_decimal_digits = ObGeoToWktVisitor::MAX_DIGITS_IN_DOUBLE;
  }

  if (OB_FAIL(ObGeoTypeUtil::get_header_info_from_wkb(swkb, header))) {
    LOG_WARN("get wkb header info from wkb failed", K(ret));
  } else if (OB_FAIL(ObGeoTypeUtil::create_geo_by_type(allocator, header.type_,
                                                       false, true, geo, header.srid_))) {
    LOG_WARN("failed to create geo by wkb", K(ret), K(header.type_), K(header.srid_));
  } else if (OB_FAIL(ObGeoTypeUtil::get_wkb_from_swkb(swkb, wkb, offset))) {
    LOG_WARN("fail to get wkb from swkb", K(ret), K(swkb));
  } else if (FALSE_IT(geo->set_data(wkb))) {
  } else if (geo->length() + offset != swkb.length()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid wkb", K(ret), K(geo->length()), K(swkb.length()), K(offset));
  } else if (OB_FAIL(wkt_visitor.init(header.srid_, max_decimal_digits))) {
    LOG_WARN("failed to init wkt_visitor with srid_", K(ret), K(header.srid_));
  } else if (OB_FAIL(geo->do_visit(wkt_visitor))) {
    LOG_WARN("failed to transform geo to wkt", K(ret));
  } else {
    wkt_visitor.get_wkt(ewkt);
    LOG_DEBUG("eval ob geometry type to ewkt", K(ewkt));
  }

  return ret;
}

int ObGeoTypeUtil::poly_close_ring(const ObString &wkb_in, ObGeoStringBuffer &res, uint32_t &offset)
{
  int ret = OB_SUCCESS;
  const char *val = wkb_in.ptr();
  ObGeoWkbByteOrder bo = static_cast<ObGeoWkbByteOrder>(*(val));
  ObGeoType type;
  uint32_t ring_num = 0;
  uint32_t pos = 0;
  if (ObGeoWkbByteOrder::BigEndian != bo && ObGeoWkbByteOrder::LittleEndian != bo) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("invalid byte order", K(ret), K(bo));
  } else if (FALSE_IT(type = static_cast<ObGeoType>(ObGeoWkbByteOrderUtil::read<uint32_t>(val + WKB_GEO_BO_SIZE, bo)))) {
  } else if (type != ObGeoType::POLYGON) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid geo type", K(type));
  } else if (FALSE_IT(ring_num = ObGeoWkbByteOrderUtil::read<uint32_t>(val + WKB_GEO_BO_SIZE + WKB_GEO_TYPE_SIZE, bo))) {
  } else if (ring_num == 0) {
    // do nothing
  } else {
    if (OB_FAIL(res.append(val, WKB_COMMON_WKB_HEADER_LEN))) {
      LOG_WARN("failed to append geo common header", K(ret), K(res.length()));
    } else {
      pos = WKB_COMMON_WKB_HEADER_LEN;
      for (uint32_t i = 0; i < ring_num && OB_SUCC(ret); i++) {
        // point_num will check before this function (ObGeoWkbCheckVisitor), must more than 4
        uint32_t point_num = ObGeoWkbByteOrderUtil::read<uint32_t>(val + pos, bo);
        uint32_t point_num_correct = point_num;
        pos += WKB_GEO_ELEMENT_NUM_SIZE;
        if (memcmp(val + pos, val + pos + ((point_num - 1) * WKB_POINT_DATA_SIZE), WKB_POINT_DATA_SIZE)) {
          // ring is not closed, append a point which is same as start point
          point_num_correct = point_num + 1;
        }
        char raw_data[WKB_GEO_ELEMENT_NUM_SIZE] = {0};
        ObGeoWkbByteOrderUtil::write<uint32_t>(const_cast<char *>(raw_data), point_num_correct, bo);
        if (OB_FAIL(res.append(const_cast<char *>(raw_data), WKB_GEO_ELEMENT_NUM_SIZE))) {
          LOG_WARN("failed to append point num", K(ret), K(point_num));
        } else if (OB_FAIL(res.append(val + pos, point_num * WKB_POINT_DATA_SIZE))) {
          LOG_WARN("failed to append geo ring", K(ret), K(point_num), K(pos));
        } else if (point_num_correct != point_num && // ring is not closed
                  OB_FAIL(res.append(val + pos, WKB_POINT_DATA_SIZE))) {
          LOG_WARN("failed to close geo ring", K(ret), K(point_num), K(pos));
        } else {
          pos += (point_num * WKB_POINT_DATA_SIZE);
        }
      }
      offset = pos;
    }
  }

  return ret;
}

int ObGeoTypeUtil::multipoly_close_ring(const ObString &wkb_in, ObGeoStringBuffer &res, uint32_t &geo_len)
{
  int ret = OB_SUCCESS;
  const char *val = wkb_in.ptr();
  ObGeoWkbByteOrder bo = static_cast<ObGeoWkbByteOrder>(*(val));
  ObGeoType type;
  uint32_t poly_num = 0;
  if (ObGeoWkbByteOrder::BigEndian != bo && ObGeoWkbByteOrder::LittleEndian != bo) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("invalid byte order", K(ret), K(bo));
  } else if (FALSE_IT(type = static_cast<ObGeoType>(ObGeoWkbByteOrderUtil::read<uint32_t>(val + WKB_GEO_BO_SIZE, bo)))) {
  } else if (type != ObGeoType::MULTIPOLYGON) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid geo type", K(type));
  } else if (FALSE_IT(poly_num = ObGeoWkbByteOrderUtil::read<uint32_t>(val + WKB_GEO_BO_SIZE + WKB_GEO_TYPE_SIZE, bo))) {
  } else if (poly_num == 0) {
    // do nothing
  } else {
    if (OB_FAIL(res.append(val, WKB_COMMON_WKB_HEADER_LEN))) {
      LOG_WARN("failed to append geo common header", K(ret), K(res.length()));
    } else {
      int pos = WKB_COMMON_WKB_HEADER_LEN;
      for (uint32_t i = 0; i < poly_num && OB_SUCC(ret); i++) {
        // len is not correct, but it doesn't matter
        ObString poly(wkb_in.length() - pos, val + pos);
        uint32_t offset = 0;
        if (OB_FAIL(ObGeoTypeUtil::poly_close_ring(poly, res, offset))) {
          LOG_WARN("failed to close poly in multipoly", K(type));
        } else {
          pos += offset;
        }
      }
      if (OB_SUCC(ret)) {
        geo_len = pos;
      }
    }
  }
  return ret;
}

int ObGeoTypeUtil::collection_close_ring(ObIAllocator &allocator, const ObString &wkb_in, ObGeoStringBuffer &res, uint32_t &geo_len)
{
  int ret = OB_SUCCESS;
  const char *val = wkb_in.ptr();
  ObGeoWkbByteOrder bo = static_cast<ObGeoWkbByteOrder>(*(val));
  ObGeoType type;
  uint32_t geo_num = 0;
  if (ObGeoWkbByteOrder::BigEndian != bo && ObGeoWkbByteOrder::LittleEndian != bo) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("invalid byte order", K(ret), K(bo));
  } else if (FALSE_IT(type = static_cast<ObGeoType>(ObGeoWkbByteOrderUtil::read<uint32_t>(val + WKB_GEO_BO_SIZE, bo)))) {
  } else if (type != ObGeoType::GEOMETRYCOLLECTION) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid geo type", K(type));
  } else if (OB_FAIL(res.append(val, WKB_COMMON_WKB_HEADER_LEN))) {
    LOG_WARN("failed to append geo common header", K(ret), K(res.length()));
  } else if (FALSE_IT(geo_num = ObGeoWkbByteOrderUtil::read<uint32_t>(val + WKB_GEO_BO_SIZE + WKB_GEO_TYPE_SIZE, bo))) {
  } else if (geo_num == 0) {
    // do nothing, empty collection
  } else {
    int pos = WKB_COMMON_WKB_HEADER_LEN;
    for (uint32_t i = 0; i < geo_num && OB_SUCC(ret); i++) {
      // len is not correct, but it doesn't matter
      ObString sub_geo(wkb_in.length() - pos, val + pos);
      ObGeoType sub_type = static_cast<ObGeoType>(ObGeoWkbByteOrderUtil::read<uint32_t>(val + pos + WKB_GEO_BO_SIZE, bo));
      uint32_t offset = 0;
      switch (sub_type) {
        case ObGeoType::MULTIPOLYGON : {
          if (OB_FAIL(ObGeoTypeUtil::multipoly_close_ring(sub_geo, res, offset))) {
            LOG_WARN("failed to close multipoly ring", K(ret));
          }
          break;
        }
        case ObGeoType::POLYGON : {
          if (OB_FAIL(ObGeoTypeUtil::poly_close_ring(sub_geo, res, offset))) {
            LOG_WARN("failed to close multipoly ring", K(ret));
          }
          break;
        }
        case ObGeoType::GEOMETRYCOLLECTION : {
          if (OB_FAIL(ObGeoTypeUtil::collection_close_ring(allocator, sub_geo, res, offset))) {
            LOG_WARN("failed to close multipoly ring", K(ret));
          }
          break;
        }
        case ObGeoType::POINT :
        case ObGeoType::LINESTRING :
        case ObGeoType::MULTIPOINT :
        case ObGeoType::MULTILINESTRING : {
          ObGeometry *sub_g = NULL;
          // whether is geog  doesn't matter
          if (OB_FAIL(ObGeoTypeUtil::create_geo_by_type(allocator, sub_type, false, true, sub_g))) {
            LOG_WARN("failed to create wkb", K(ret), K(sub_type));
          } else {
            sub_g->set_data(sub_geo);
            if (OB_FAIL(res.append(sub_geo.ptr(), sub_g->length()))) {
              LOG_WARN("failed to append geo to buffer", K(ret), K(sub_type));
            } else {
              offset = sub_g->length();
            }
          }
          break;
        }
        default : {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid geo type", K(sub_type));
        }
      }
      if (OB_SUCC(ret)) {
        pos += offset;
      }
    }
    if (OB_SUCC(ret)) {
      geo_len = pos;
    }
  }
  return ret;
}

int ObGeoTypeUtil::geo_close_ring(ObGeometry &geo, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObGeoStringBuffer res(&allocator);
  if (geo.is_tree()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tree geo is not support", K(ret));
  } else if (OB_ISNULL(geo.val())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("geo value is null", K(ret));
  } else if (geo.type() == ObGeoType::POLYGON) {
    ObString wkb(geo.length(), geo.val());
    uint32_t offset = 0;
    // at least add one point
    if (OB_FAIL(res.extend(geo.length() + WKB_POINT_DATA_SIZE))) {
      LOG_WARN("failed to extend buffer", K(geo.length()));
    } else if (OB_FAIL(ObGeoTypeUtil::poly_close_ring(wkb, res, offset))) {
      LOG_WARN("failed to close poly ring", K(ret));
    }
  } else if (geo.type() == ObGeoType::MULTIPOLYGON) {
    ObString wkb(geo.length(), geo.val());
    uint32_t offset = 0;
    // at least add one point
    if (OB_FAIL(res.extend(geo.length() + WKB_POINT_DATA_SIZE))) {
      LOG_WARN("failed to extend buffer", K(geo.length()));
    } else if (OB_FAIL(ObGeoTypeUtil::multipoly_close_ring(wkb, res, offset))) {
      LOG_WARN("failed to close multipoly ring", K(ret));
    }
  } else if (geo.type() == ObGeoType::GEOMETRYCOLLECTION) {
    ObString wkb(geo.length(), geo.val());
    uint32_t offset = 0;
    // at least add one point
    if (OB_FAIL(res.extend(geo.length() + WKB_POINT_DATA_SIZE))) {
      LOG_WARN("failed to extend buffer", K(geo.length()));
    } else if (OB_FAIL(ObGeoTypeUtil::collection_close_ring(allocator, wkb, res, offset))) {
      LOG_WARN("failed to close multipoly ring", K(ret));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("geo type is not support", K(geo.type()));
  }

  if (OB_SUCC(ret)) {
    ObString wkb_nosrid(res.length(), res.ptr());
    geo.set_data(wkb_nosrid);
  }
  return ret;
}

int ObGeoTypeUtil::get_cellid_mbr_from_geom(const ObString &wkb_str,
                                            const ObSrsItem *srs_item,
                                            const ObSrsBoundsItem *srs_bound,
                                            ObS2Cellids &cellids,
                                            ObString &mbr_val)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator(lib::ObLabel("GisIndexRebuild"));
  ObS2Adapter s2object(&tmp_allocator, srs_item == NULL ? false : srs_item->is_geographical_srs());
  ObSpatialMBR spa_mbr;
  char *mbr = mbr_val.ptr();
  int64_t mbr_len = 0;
  if (OB_FAIL(s2object.init(wkb_str, srs_bound))) {
    STORAGE_LOG(WARN, "Init s2object failed", K(ret));
  } else if (OB_FAIL(s2object.get_cellids(cellids, false))) {
    STORAGE_LOG(WARN, "Get cellids from s2object failed", K(ret));
  } else if (OB_FAIL(s2object.get_mbr(spa_mbr))) {
    STORAGE_LOG(WARN, "Get mbr from s2object failed", K(ret));
  } else if (spa_mbr.is_empty()) {
    if (cellids.size() == 0) {
      LOG_DEBUG("it's might be empty geometry collection", K(wkb_str));
    } else {
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_WARN("invalid geometry", K(ret), K(wkb_str));
    }
  } else if (OB_ISNULL(mbr)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc memory for spatial index row mbr", K(ret), K(mbr));
  } else if (OB_FAIL(spa_mbr.to_char(mbr, mbr_len))) {
    STORAGE_LOG(WARN, "failed transfrom ObSpatialMBR to string", K(ret));
  } else {
    mbr_val.assign_ptr(mbr, mbr_len);
  }
  return ret;
}

static int srs_bounds_to_mbr_box(const ObSrsBoundsItem *bounds, ObCartesianBox &box)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(bounds)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("bounds is empty");
  } else {
    double deltaX = OB_GEO_BOUNDS_DELTA * (bounds->maxX_ - bounds->minX_);
    double deltaY = OB_GEO_BOUNDS_DELTA * (bounds->maxY_ - bounds->minY_);
    box.min_corner().set<0>(bounds->minX_ + deltaX);
    box.min_corner().set<1>(bounds->minY_ + deltaY);
    box.max_corner().set<0>(bounds->maxX_ - deltaX);
    box.max_corner().set<1>(bounds->maxY_ - deltaY);
  }
  if (OB_SUCC(ret) && box.is_empty()) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("get srs bound box failed", K(ret), K(*bounds));
  }
  return ret;
}

static int mbr_box_to_geometry(uint32_t srid,
                               ObIAllocator &allocator,
                               ObCartesianBox &box,
                               ObGeometry *&geo_bin_out)
{
  const uint32 mbr_polygon_point_num = 5;
  const uint32 mbr_linestring_point_num = 2;
  const uint32 mbr_polygon_ring_num = 1;

  int ret = OB_SUCCESS;
  uint32_t head_len = WKB_GEO_SRID_SIZE + WKB_VERSION_SIZE;
  uint64_t res_size = head_len;
  ObGeoType type;

  double min_x = box.min_corner().get<0>();
  double min_y = box.min_corner().get<1>();
  double max_x = box.max_corner().get<0>();
  double max_y = box.max_corner().get<1>();

  if (min_x == max_x && min_y == max_y) {
    type = ObGeoType::POINT;
    res_size += WKB_GEO_BO_SIZE + WKB_GEO_TYPE_SIZE + WKB_POINT_DATA_SIZE;
  } else if (min_x == max_x || min_y == max_y) {
    type = ObGeoType::LINESTRING;
    res_size += WKB_COMMON_WKB_HEADER_LEN + mbr_linestring_point_num * WKB_POINT_DATA_SIZE;
  } else {
    type = ObGeoType::POLYGON;
    res_size += (WKB_COMMON_WKB_HEADER_LEN
                 + WKB_GEO_ELEMENT_NUM_SIZE
                 + mbr_polygon_point_num * WKB_POINT_DATA_SIZE);
  }

  char *res_buf = static_cast<char *>(allocator.alloc(res_size));
  ObString wkb_buf(res_size, 0, res_buf);
  ObGeoWkbVisitor wkb_visitor(NULL, &wkb_buf, false);

  if (OB_ISNULL(res_buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory for mbr geo failed", K(ret), K(res_size), K(type));
  } else {
    if (OB_FAIL(wkb_visitor.write_to_buffer(srid, WKB_GEO_SRID_SIZE))) {
      LOG_WARN("failed to append srid", K(ret));
    } else if (OB_FAIL(wkb_visitor.write_to_buffer(ENCODE_GEO_VERSION(GEO_VESION_1), WKB_VERSION_SIZE))) {
      LOG_WARN("failed to append version", K(ret));
    } else if (OB_FAIL(wkb_visitor.write_to_buffer(ObGeoWkbByteOrder::LittleEndian, 1))) {
      LOG_WARN("failed to write little endian", K(ret));
    } else if (OB_FAIL(wkb_visitor.write_to_buffer(static_cast<uint32_t>(type),
                                                    sizeof(uint32_t)))) {
      LOG_WARN("failed to write type", K(ret));
    } else { /* do nothing */ }
  }

  if (OB_SUCC(ret)) {
    if (type == ObGeoType::POINT) {
      if (OB_FAIL(wkb_visitor.write_cartesian_point(min_x, min_y))) {
        LOG_WARN("failed to write point(x, y)", K(ret));
      }
    } else if (type == ObGeoType::LINESTRING) {
      if (OB_FAIL(wkb_visitor.write_to_buffer(mbr_linestring_point_num, sizeof(uint32_t)))) {
        LOG_WARN("failed to write ring number", K(ret));
      } else if (OB_FAIL(wkb_visitor.write_cartesian_point(min_x, min_y))) {
        LOG_WARN("failed to write point(min_x, min_y)", K(ret));
      } else if (OB_FAIL(wkb_visitor.write_cartesian_point(max_x, max_y))) {
         LOG_WARN("failed to write point(max_x, min_y)", K(ret));
      } else { /* do nothing */ }
    } else {
      // must be polygon
      if (OB_FAIL(wkb_visitor.write_to_buffer(mbr_polygon_ring_num, sizeof(uint32_t)))) {
        LOG_WARN("failed to write ring number", K(ret));
      } else if (OB_FAIL(wkb_visitor.write_to_buffer(mbr_polygon_point_num, sizeof(uint32_t)))) {
        LOG_WARN("failed to write external ring point number", K(ret));
      } else if (OB_FAIL(wkb_visitor.write_cartesian_point(min_x, min_y))) {
        LOG_WARN("failed to write point(min_x, min_y)", K(ret));
      } else if (OB_FAIL(wkb_visitor.write_cartesian_point(max_x, min_y))) {
        LOG_WARN("failed to write point(max_x, min_y)", K(ret));
      } else if (OB_FAIL(wkb_visitor.write_cartesian_point(max_x, max_y))) {
        LOG_WARN("failed to write point(max_x, max_y)", K(ret));
      } else if (OB_FAIL(wkb_visitor.write_cartesian_point(min_x, max_y))) {
        LOG_WARN("failed to write point(min_x, max_y)", K(ret));
      } else if (OB_FAIL(wkb_visitor.write_cartesian_point(min_x, min_y))) {
        LOG_WARN("failed to write point(min_x, min_y)", K(ret));
      } else { /* do nothing */ }
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(ObGeoTypeUtil::create_geo_by_type(allocator,
                                                       type,
                                                       false,
                                                       true,
                                                       geo_bin_out,
                                                       srid))) {
    LOG_WARN("failed to create geo", K(ret), K(type));
  } else if (OB_ISNULL(geo_bin_out)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate geo point", K(ret));
  } else {
    ObString wkb_nosrid((res_size - head_len), (res_buf + head_len));
    geo_bin_out->set_data(wkb_nosrid);
    if (OB_SUCC(ret) && geo_bin_out->length() + head_len != res_size) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid wkb geo", K(ret), K(type), K(geo_bin_out->length()), K(res_size));
    }
  }

  return ret;
}

// only support cartisain
int ObGeoTypeUtil::get_mbr_polygon(ObIAllocator &allocator,
                                   const ObSrsBoundsItem *bounds,
                                   const ObGeometry &geo_bin,
                                   ObGeometry *&geo_bin_out)
{
  int ret = OB_SUCCESS;
  ObCartesianBox box; // mbr box of input geo
  ObCartesianBox bounds_box; // bounds box of corresponding srs
  ObGeoEvalCtx gis_context(&allocator, NULL);
  if (OB_FAIL(srs_bounds_to_mbr_box(bounds, bounds_box))) {
    LOG_WARN("get srs bounds box failed", K(ret));
  } else if (OB_FAIL(gis_context.append_geo_arg(&geo_bin))) {
    LOG_WARN("build gis context failed", K(ret), K(gis_context.get_geo_count()));
  } else if (OB_FAIL(ObGeoFuncEnvelope::eval(gis_context, box))) {
    LOG_WARN("get mbr box failed", K(ret));
  } else if (box.is_empty()) {
    ret = OB_ERR_GIS_INVALID_DATA;
    // how about some box points is nan?
    LOG_WARN("get mbr box failed", K(ret));
  } else {
    ObCartesianBox final; // result box
    boost::geometry::intersection(box, bounds_box, final);
    if (final.is_empty()) {
      ret = OB_EMPTY_RESULT;
      LOG_WARN("no intersection in bounds", K(ret), K(*bounds), K(box));
    } else if (OB_FAIL(mbr_box_to_geometry(geo_bin.get_srid(), allocator, final, geo_bin_out))) {
      LOG_WARN("failed to convert box to geo", K(ret), K(final));
    } else { /* do nothing */ }
  }
  return ret;
}

// fix
int ObGeoTypeUtil::eval_point_box_intersects(const ObSrsItem *srs_item,
                                             const ObGeometry *geo1,
                                             const ObGeometry *geo2,
                                             bool &result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(geo1) || OB_ISNULL(geo2) || OB_ISNULL(geo1->val()) || OB_ISNULL(geo2->val())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Null input", KP(geo1), KP(geo2));
  } else if (geo1->crs() != geo2->crs()) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("crs inconsistancy", K(geo1->crs()), K(geo2->crs()));
  } else if (geo1->get_srid() != geo2->get_srid()) {
    ret = OB_ERR_GIS_DIFFERENT_SRIDS;
    LOG_WARN("srid inconsistancy", K(geo1->get_srid()), K(geo2->get_srid()));
  } else if (geo1->crs() == ObGeoCRS::Cartesian) {
    const ObWkbGeomPoint *p1 = reinterpret_cast<const ObWkbGeomPoint *>(geo1->val());
    const ObWkbGeomPoint *p2 = reinterpret_cast<const ObWkbGeomPoint *>(geo2->val());
    ObWkbGeomInnerPoint innerp1(p1->get<0>(), p1->get<1>());
    ObWkbGeomInnerPoint innerp2(p2->get<0>(), p2->get<1>());
    ObCartesianBox box1(innerp1, innerp1);
    ObCartesianBox box2(innerp2, innerp2);
    result = boost::geometry::intersects(box1, box2);
  } else if (geo1->crs() == ObGeoCRS::Geographic) {
    const ObWkbGeogPoint *p1 = reinterpret_cast<const ObWkbGeogPoint *>(geo1->val());
    const ObWkbGeogPoint *p2 = reinterpret_cast<const ObWkbGeogPoint *>(geo2->val());
    // do not need normalize, already normalized in st_expr
    ObWkbGeogInnerPoint innerp1(p1->get<0>(), p1->get<1>());
    ObWkbGeogInnerPoint innerp2(p2->get<0>(), p2->get<1>());
    ObGeographBox box1(innerp1, innerp1);
    ObGeographBox box2(innerp2, innerp2);
    result = boost::geometry::intersects(box1, box2); // no strategy
  } else { /* do nothing */ }

  return ret;
}

} // namespace common
} // namespace oceanbase
