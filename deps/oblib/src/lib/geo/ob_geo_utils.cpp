
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
#include "lib/geo/ob_sdo_geo_object.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/udt/ob_udt_type.h"
#include "lib/geo/ob_sdo_geo_func_to_wkb.h"
#include "lib/geo/ob_wkb_byte_order_visitor.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/geo/ob_geo_3d.h"
#include "lib/geo/ob_geo_check_empty_visitor.h"
#include "lib/geo/ob_geo_box_clip_visitor.h"
#include "lib/geo/ob_geo_affine_visitor.h"
#include "lib/geo/ob_geo_simplify_visitor.h"
#include "lib/geo/ob_geo_grid_visitor.h"
#include "lib/geo/ob_geo_cache.h"
#include "lib/geo/ob_geo_cache_polygon.h"
#include "lib/geo/ob_geo_cache_point.h"
#include "lib/geo/ob_geo_cache_linestring.h"
#include "lib/geo/ob_geo_vertex_collect_visitor.h"
#include "lib/geo/ob_geo_point_location_visitor.h"
#include "lib/geo/ob_geo_zoom_in_visitor.h"

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
    if (OB_FAIL(ob_write_string(*allocator, prj4_tmp, prj4_param, true))) {
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
  } else if (0 == name.case_compare("pointz") || 0 == name.case_compare("point z")) {
    geo_type = ObGeoType::POINTZ;
  } else if (0 == name.case_compare("linestring")) {
    geo_type = ObGeoType::LINESTRING;
  } else if (0 == name.case_compare("linestringz") || 0 == name.case_compare("linestring z")) {
    geo_type = ObGeoType::LINESTRINGZ;
  } else if (0 == name.case_compare("polygon")) {
    geo_type = ObGeoType::POLYGON;
  } else if (0 == name.case_compare("polygonz") || 0 == name.case_compare("polygon z")) {
    geo_type = ObGeoType::POLYGONZ;
  } else if (0 == name.case_compare("multipoint")) {
    geo_type = ObGeoType::MULTIPOINT;
  } else if (0 == name.case_compare("multipointz") || 0 == name.case_compare("multipoint z")) {
    geo_type = ObGeoType::MULTIPOINTZ;
  } else if (0 == name.case_compare("multilinestring")) {
    geo_type = ObGeoType::MULTILINESTRING;
  } else if (0 == name.case_compare("multilinestringz") || 0 == name.case_compare("multilinestring z")) {
    geo_type = ObGeoType::MULTILINESTRINGZ;
  } else if (0 == name.case_compare("multipolygon")) {
    geo_type = ObGeoType::MULTIPOLYGON;
  } else if (0 == name.case_compare("multipolygonz") || 0 == name.case_compare("multipolygon z")) {
    geo_type = ObGeoType::MULTIPOLYGONZ;
  } else if (0 == name.case_compare("geometrycollection")
             || 0 == name.case_compare("geomcollection")) {
    geo_type = ObGeoType::GEOMETRYCOLLECTION;
  } else if (0 == name.case_compare("geometrycollectionz") || 0 == name.case_compare("geometrycollection z")) {
    geo_type = ObGeoType::GEOMETRYCOLLECTIONZ;
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
    case ObGeoType::POINTZ:{
      type_name =  "POINT Z";
      break;
    }
    case ObGeoType::LINESTRING:{
      type_name =  "LINESTRING";
      break;
    }
    case ObGeoType::LINESTRINGZ:{
      type_name =  "LINESTRING Z";
      break;
    }
    case ObGeoType::POLYGON:{
      type_name = "POLYGON";
      break;
    }
    case ObGeoType::POLYGONZ:{
      type_name = "POLYGON Z";
      break;
    }
    case ObGeoType::MULTIPOINT:{
      type_name = "MULTIPOINT";
      break;
    }
    case ObGeoType::MULTIPOINTZ:{
      type_name = "MULTIPOINT Z";
      break;
    }
    case ObGeoType::MULTILINESTRING:{
      type_name = "MULTILINESTRING";
      break;
    }
    case ObGeoType::MULTILINESTRINGZ:{
      type_name = "MULTILINESTRING Z";
      break;
    }
    case ObGeoType::MULTIPOLYGON:{
      type_name = "MULTIPOLYGON";
      break;
    }
     case ObGeoType::MULTIPOLYGONZ:{
      type_name = "MULTIPOLYGON Z";
      break;
    }
    case ObGeoType::GEOMETRYCOLLECTION:{
      type_name = "GEOMETRYCOLLECTION";
      break;
    }
    case ObGeoType::GEOMETRYCOLLECTIONZ:{
      type_name = "GEOMETRYCOLLECTION Z";
      break;
    }
    default:{
      LOG_WARN_RET(OB_INVALID_ARGUMENT, "unknown geometry type", K(type));
      break;
    }
  }
  return type_name;
}

const char *ObGeoTypeUtil::get_geo_name_by_type_oracle(ObGeoType type)
{
  const char *type_name = "UNKNOWN";
  switch (type) {
    case ObGeoType::POINT:
    case ObGeoType::POINTZ:{
      type_name =  "POINT";
      break;
    }
    case ObGeoType::LINESTRING:
    case ObGeoType::LINESTRINGZ:{
      type_name =  "LINESTRING";
      break;
    }
    case ObGeoType::POLYGON:
    case ObGeoType::POLYGONZ:{
      type_name = "POLYGON";
      break;
    }
    case ObGeoType::MULTIPOINT:
    case ObGeoType::MULTIPOINTZ:{
      type_name = "MULTIPOINT";
      break;
    }
    case ObGeoType::MULTILINESTRING:
    case ObGeoType::MULTILINESTRINGZ:{
      type_name = "MULTILINESTRING";
      break;
    }
    case ObGeoType::MULTIPOLYGON:
    case ObGeoType::MULTIPOLYGONZ:{
      type_name = "MULTIPOLYGON";
      break;
    }
    case ObGeoType::GEOMETRYCOLLECTION:
    case ObGeoType::GEOMETRYCOLLECTIONZ:{
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

int ObGeoTypeUtil::get_st_geo_name_by_type(ObGeoType type, ObString &res)
{
  int ret = OB_SUCCESS;
  res = "UNKNOWN";
  switch (type) {
    case ObGeoType::POINT: {
      res = "ST_Point";
      break;
    }
    case ObGeoType::LINESTRING: {
      res = "ST_LineString";
      break;
    }
    case ObGeoType::POLYGON: {
      res = "ST_Polygon";
      break;
    }
    case ObGeoType::MULTIPOINT: {
      res = "ST_MultiPoint";
      break;
    }
    case ObGeoType::MULTILINESTRING: {
      res = "ST_MultiLineString";
      break;
    }
    case ObGeoType::MULTIPOLYGON: {
      res = "ST_MultiPolygon";
      break;
    }
    case ObGeoType::GEOMETRYCOLLECTION: {
      res = "ST_GeometryCollection";
      break;
    }
    default: {
      ret = OB_ERR_INVALID_GEOMETRY_TYPE;
      LOG_WARN("unknown geometry type", K(ret), K(type));
      break;
    }
  }
  return ret;
}

int ObGeoTypeUtil::create_geo_by_type(ObIAllocator &allocator,
                                      ObGeoType geo_type,
                                      bool is_geographical,
                                      bool is_geo_bin,
                                      ObGeometry *&geo,
                                      uint32_t srid/* = 0 */)
{
  int ret = OB_SUCCESS;

  if (is_2d_geo_type(geo_type)) {
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
  } else if (is_3d_geo_type(geo_type)) {
    if (OB_ISNULL(geo = OB_NEWx(ObGeometry3D, (&allocator), srid, (&allocator)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to create 3d geo object", K(ret), K(geo_type));
    } else {
      ObGeoCRS crs = is_geographical ? ObGeoCRS::Geographic : ObGeoCRS::Cartesian;
      static_cast<ObGeometry3D*>(geo)->set_crs(crs);
    }
  } else {
    ret = OB_ERR_INVALID_GEOMETRY_TYPE;
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
                                     bool need_copy, /* = true */
                                     bool allow_3d /* = false */)
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
    } else if (!allow_3d && is_3d_geo_type(header.type_)) {
      ret = OB_ERR_INVALID_GEOMETRY_TYPE;
      LOG_WARN("3d geo type is not allow", K(ret), K(header.type_));
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
        if (ObGeoTypeUtil::is_3d_geo_type(geo->type())) {
          ObGeometry3D *geo_3d = static_cast<ObGeometry3D *>(geo);
          if (OB_FAIL(geo_3d->check_wkb_valid())) {
            LOG_WARN("check wkb is invalid", K(ret));
          }
        } else {
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
  }

  return ret;
}

int ObGeoTypeUtil::convert_geometry_3D_to_2D(
    const ObSrsItem *srs, ObIAllocator &allocator, ObGeometry *g3d, uint8_t build_flag, ObGeometry *&geo)
{
  // 3d to 2d geo
  int ret = OB_SUCCESS;
  if (is_3d_geo_type(g3d->type())) {
    ObGeoCRS crs = ObGeoCRS::Cartesian;
    if (OB_NOT_NULL(srs)) {
      crs = (srs->srs_type() == ObSrsType::PROJECTED_SRS) ?
          ObGeoCRS::Cartesian : ObGeoCRS::Geographic;
    }
    ObString wkb_data;
    ObGeometry3D *geo_3d = static_cast<ObGeometry3D *>(g3d);
    uint32_t srid = (g3d == geo) ? geo->get_srid() : 0;
    if (OB_FAIL(geo_3d->to_2d_geo(allocator, geo, srid))) {
      LOG_WARN("fail to convert 3d to 2d geo", K(ret), K(geo_3d->type()));
    } else {
      // geo has been transform to 2d
      wkb_data.assign_ptr(geo->val(), geo->length());
    }
    if (OB_SUCC(ret)) {
      ObGeoWkbByteOrder bo = static_cast<ObGeoWkbByteOrder>(*(wkb_data.ptr()));
      bool need_check_ring = build_flag & ObGeoBuildFlag::GEO_CHECK_RING;
      ObGeoWkbCheckVisitor wkb_check(wkb_data, bo, need_check_ring);
      if (OB_FAIL(geo->do_visit(wkb_check))) {
        ret = OB_ERR_GIS_INVALID_DATA;
        LOG_WARN("invalid swkb", K(g3d->type()), K(g3d->get_srid()), K(crs));
      }
      if (OB_SUCC(ret) && (build_flag & ObGeoBuildFlag::GEO_CORRECT) &&
              OB_FAIL(correct_polygon(allocator, srs, wkb_check.is_ring_closed(), *geo))) {
        LOG_WARN("correct geo failed", K(ret), K(geo));
      }
    }
  }
  return ret;
}

int ObGeoTypeUtil::normalize_geometry(ObGeometry &geo, const ObSrsItem *srs)
{
  int ret = OB_SUCCESS;
  uint32_t zoom_in_value = 0;
  if (is_3d_geo_type(geo.type())) {
    ObGeometry3D *geo_3d = static_cast<ObGeometry3D *>(&geo);
    if (OB_FAIL(geo_3d->normalize(srs, zoom_in_value))) {
      LOG_WARN("fail to check coordinate range", K(ret));
    }
  } else {
    ObGeoNormalizeVisitor normalize_visitor(srs);
    if (OB_FAIL(geo.do_visit(normalize_visitor))) {
      LOG_WARN("normalize geo failed", K(ret));
    } else {
      zoom_in_value = normalize_visitor.get_zoom_in_value();
    }
  }
  if (OB_SUCC(ret)) {
    geo.set_zoom_in_value(zoom_in_value);
  }
  return ret;
}

int ObGeoTypeUtil::build_geometry(ObIAllocator &allocator,
                                  const ObString &swkb,
                                  ObGeometry *&geo,
                                  const ObSrsItem *srs,
                                  ObGeoErrLogInfo &log_info,
                                  uint8_t build_flag /* = ObGeoBuildFlag::DEFAULT */)
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
  } else if (ObGeoWkbByteOrder::LittleEndian != header.bo_) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("invalid byte order", K(ret), K(header.bo_));
  } else if (ObGeoType::GEOMETRY > header.type_ || ObGeoType::GEOMETRYCOLLECTION < header.type_) {
    if ((build_flag & ObGeoBuildFlag::GEO_ALLOW_3D) && is_3d_geo_type(header.type_)) {
      // skip
    } else {
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_WARN("invalid geo type", K(ret), K(header.type_));
    }
  }
  bool is_ring_closed = true;
  if (OB_FAIL(ret)) {
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
      if (!is_3d_geo_type(header.type_)) {
        bool need_check_ring = build_flag & ObGeoBuildFlag::GEO_CHECK_RING;
        ObGeoWkbCheckVisitor wkb_check(wkb_data, header.bo_, need_check_ring);
        if (OB_FAIL(geo->do_visit(wkb_check))) {
          ret = OB_ERR_GIS_INVALID_DATA;
          LOG_WARN("invalid swkb", K(swkb), K(header.type_), K(header.srid_), K(crs));
        } else if (geo->length() + offset != swkb.length()) {
          ret = OB_ERR_GIS_INVALID_DATA;
          LOG_WARN("invalid swkb length", K(ret), K(geo->length()), K(swkb.length()), K(offset));
        } else {
          is_ring_closed = wkb_check.is_ring_closed();
        }
      } else if (build_flag & GEO_RESERVE_3D) {
        // do not convert to 2D geometry
      } else if (OB_FAIL(convert_geometry_3D_to_2D(srs, allocator, geo, build_flag, geo))) {
        // will do wkb check and polygon correct in convert_geometry_3D_to_2D
        LOG_WARN("fail to convert 3D geometry to 2D", K(ret));
      }
    }
  }
  // both 3D and 2D are available
  if (OB_SUCC(ret) && (build_flag & ObGeoBuildFlag::GEO_NORMALIZE) && ObGeoCRS::Geographic == crs) {
    if (OB_FAIL(normalize_geometry(*geo, srs))) {
      LOG_WARN("fail to normalize geometry", K(ret));
    }
  }
  // only for 2D geometry
  if (OB_SUCC(ret) && !is_3d_geo_type(geo->type()) && (build_flag & ObGeoBuildFlag::GEO_CORRECT)) {
    if (OB_FAIL(correct_polygon(allocator, srs, is_ring_closed, *geo))) {
      LOG_WARN("correct geo failed", K(ret), K(geo));
    }
  }
  // check coordinate range
  if (OB_SUCC(ret) && ObGeoCRS::Geographic == crs) {
    if (!(build_flag & ObGeoBuildFlag::GEO_CHECK_RANGE)) {
      // do nothing
    } else if ((build_flag & ObGeoBuildFlag::GEO_NORMALIZE) && OB_FAIL(check_coordinate_range(srs, geo, log_info, true, true))) {
      LOG_WARN("failed to check coordinate range", K(ret));
    } else if (!(build_flag & ObGeoBuildFlag::GEO_NORMALIZE) && OB_FAIL(check_coordinate_range(srs, geo, log_info, true))) {
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
  ObGeoType type = ObGeoType::GEO3DTYPEMAX;
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
  } else if (!is_2d_geo_type(type) && !is_3d_geo_type(type)) {
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
      } else if (is_3d_geo_type(geo->type())) {
        ObGeometry3D *geo_3d = static_cast<ObGeometry3D *>(geo);
        if (OB_FAIL(geo_3d->check_wkb_valid())) {
          LOG_WARN("fail to check geo 3d is valid", K(ret));
        }
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

int ObGeoTypeUtil::copy_geometry(ObIAllocator &allocator,
                                  ObGeometry &origin_geo,
                                  ObGeometry *&copy_geo)
{
  int ret = OB_SUCCESS;
  copy_geo = nullptr;
  ObString data;
  if (OB_FAIL(ObGeoTypeUtil::create_geo_by_type(allocator, origin_geo.type(), origin_geo.crs() == ObGeoCRS::Geographic, true, copy_geo))) {
    LOG_WARN("fail to create geo by type", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator, ObString(origin_geo.length(), origin_geo.val()), data))) {
    LOG_WARN("fail to copy geo data", K(ret));
  } else {
    copy_geo->set_data(data);
    copy_geo->set_srid(origin_geo.get_srid());
    copy_geo->set_zoom_in_value(origin_geo.get_zoom_in_value());
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
  ObGeoCoordRangeResult result;
  if (OB_ISNULL(geo)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("geo is NULL", K(ret));
  } else if (is_3d_geo_type(geo->type())) {
    ObGeometry3D *geo_3d = static_cast<ObGeometry3D *>(geo);
    if (OB_FAIL(geo_3d->check_3d_coordinate_range(srs, is_normalized, result))) {
      LOG_WARN("fail to check coordinate range", K(ret));
    }
  } else {
    ObGeoCoordinateRangeVisitor range_visitor(srs, is_normalized);
    if (OB_FAIL(geo->do_visit(range_visitor))) {
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_WARN("failed to reverse geometry coordinate", K(ret));
    } else {
      range_visitor.get_coord_range_result(result);
    }
  }

  if (OB_FAIL(ret)) {
  } else if (result.is_long_out_range_) {
    // handle log user error message
    if (OB_FAIL(srs->longtitude_convert_from_radians(-M_PI, log_info.min_long_val_))) {
      LOG_WARN("failed to convert longitude from radians", K(ret));
    } else if (OB_FAIL(srs->longtitude_convert_from_radians(M_PI, log_info.max_long_val_))) {
      LOG_WARN("failed to convert longitude from radians", K(ret));
    } else {
      log_info.value_out_of_range_ = result.value_out_range_;
      if (is_param) {
        ret = OB_ERR_GEOMETRY_PARAM_LONGITUDE_OUT_OF_RANGE;
        LOG_WARN("parameter geometry longitude is out of range", "longitude value",
            result.value_out_range_);
      } else {
        ret = OB_ERR_LONGITUDE_OUT_OF_RANGE;
        LOG_WARN("geometry longitude is out of range", "longitude value",
            result.value_out_range_);
      }
    }
  } else if (result.is_lati_out_range_) {
    // handle log user error message
    if (OB_FAIL(srs->latitude_convert_from_radians(-M_PI_2, log_info.min_lat_val_))) {
      LOG_WARN("failed to convert latitude from radians", K(ret));
    } else if (OB_FAIL(srs->latitude_convert_from_radians(M_PI_2, log_info.max_lat_val_))) {
      LOG_WARN("failed to convert latitude from radians", K(ret));
    } else {
      log_info.value_out_of_range_ = result.value_out_range_;
      if (is_param) {
        ret = OB_ERR_GEOMETRY_PARAM_LATITUDE_OUT_OF_RANGE;
        LOG_WARN("parameter geometry latitude is out of range", "latitude value",
            result.value_out_range_);
      } else {
        ret = OB_ERR_LATITUDE_OUT_OF_RANGE;
        LOG_WARN("geometry latitude is out of range", "latitude value",
            result.value_out_range_);
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
  ObGeoErrLogInfo log_info;
  if (OB_FAIL(ObGeoTypeUtil::build_geometry(*allocator, wkb_str, geo, srs, log_info, ObGeoBuildFlag::GEO_ALLOW_3D))) {
    LOG_WARN("fail to build geometry", K(ret));
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

int ObGeoTypeUtil::add_geo_version(ObIAllocator &allocator, const ObString &src, ObString &res_wkb)
{
  int ret = OB_SUCCESS;
  uint8_t version = (*(src.ptr() + WKB_GEO_SRID_SIZE));
  if (ObGeoWkbByteOrder::BigEndian == static_cast<ObGeoWkbByteOrder>(version)
     || ObGeoWkbByteOrder::LittleEndian == static_cast<ObGeoWkbByteOrder>(version)) {
    // without version, add version
    uint64_t res_size = src.length() + WKB_VERSION_SIZE;
    char *res_buf = reinterpret_cast<char *>(allocator.alloc(res_size));
    if (OB_ISNULL(res_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory", K(ret), K(res_size));
    } else {
      MEMCPY(res_buf, src.ptr(), WKB_GEO_SRID_SIZE);
      *(res_buf + WKB_GEO_SRID_SIZE) = ENCODE_GEO_VERSION(GEO_VESION_1);
      MEMCPY(res_buf + WKB_OFFSET, src.ptr() + WKB_GEO_SRID_SIZE, src.length() - WKB_GEO_SRID_SIZE);
      res_wkb.assign_ptr(res_buf, res_size);
    }
  } else if (IS_GEO_VERSION(version) && GET_GEO_VERSION(version) == GEO_VESION_1) {
    res_wkb = src;
  } else {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("invalid byte order", K(ret), K(version));
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

double ObGeoTypeUtil::round_double(double x, int32_t dec, bool truncate)
{
  double dret = 0.0;
  double pow_val = std::pow(10, static_cast<double>(std::abs(dec)));
  double val_mul_tmp = x * pow_val;
  if (dec < 0 && std::isinf(pow_val)) {
    dret = 0.0;
  } else if (dec >= 0 && std::isinf(val_mul_tmp)) {
    dret = x;
  } else {
    double val_div_tmp = x / pow_val;
    if (truncate) {
      if (x < 0) {
        dret = dec < 0 ? std::ceil(val_div_tmp) * pow_val : std::ceil(val_mul_tmp) / pow_val;
      } else {
        dret = dec < 0 ? std::floor(val_div_tmp) * pow_val : std::floor(val_mul_tmp) / pow_val;
      }
    } else {
      dret = dec < 0 ? std::rint(val_div_tmp) * pow_val : std::rint(val_mul_tmp) / pow_val;
    }
  }
  return dret;
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
  point_box_union(end, box);

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
        point_box_union(p3d, box);
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

void ObGeoBoxUtil::point_box_union(ObPoint3d &point, ObGeogBox &box)
{
  box.xmin = OB_MIN(point.x, box.xmin);
  box.ymin = OB_MIN(point.y, box.ymin);
  box.zmin = OB_MIN(point.z, box.zmin);
  box.xmax = OB_MAX(point.x, box.xmax);
  box.ymax = OB_MAX(point.y, box.ymax);
  box.zmax = OB_MAX(point.z, box.zmax);
}

void ObGeoBoxUtil::box_union(const ObGeogBox &box_tmp, ObGeogBox &box)
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
        box_union(box_tmp, box);
      }
    }
    ob_geo_box_check_poles(box);
  }
  return ret;
}

bool ObGeoBoxUtil::boxes_overlaps(const ObGeogBox &box1, const ObGeogBox &box2) {
  bool bret = true;
  if (box1.xmax < box2.xmin || box1.ymax < box2.ymin ||
      box1.xmin > box2.xmax || box1.ymin > box2.ymax) {
    bret = false;
  }
  return bret;
}

bool ObGeoBoxUtil::boxes_contains(const ObGeogBox &box1, const ObGeogBox &box2)
{
  bool bret = true;
  if (box1.xmax < box2.xmax || box1.ymax < box2.ymax ||
      box1.xmin > box2.xmin || box1.ymin > box2.ymin) {
    bret = false;
  }
  return bret;
}

void ObGeoBoxUtil::get_point2d_from_geom_point(const ObWkbGeomInnerPoint &point, ObPoint2d &p2d)
{
  p2d.x = point.get<0>();
  p2d.y = point.get<1>();
}

int ObGeoBoxUtil::clip_by_box(ObGeometry &geo_in, ObIAllocator &allocator, const ObGeogBox &gbox, ObGeometry *&geo_out, bool is_called_in_pg_expr)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator;
  ObGeoEvalCtx box_ctx(&tmp_allocator);
  box_ctx.set_is_called_in_pg_expr(is_called_in_pg_expr); // clip only used in PG expr currently
  ObGeogBox *gbox_in = nullptr;
  ObGeometry *geo_tree = nullptr;
  ObGeometry *geo_bin = nullptr;
  if (geo_in.is_tree()) {
    geo_tree = &geo_in;
    if (OB_FAIL(ObGeoTypeUtil::tree_to_bin(allocator, geo_tree, geo_bin, nullptr))) {
      LOG_WARN("fail to do tree to bin", K(ret));
    }
  } else {
    geo_bin = &geo_in;
    ObGeoToTreeVisitor tree_visitor(&allocator);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(geo_in.do_visit(tree_visitor))) {
      LOG_WARN("fail to do tree visitor", K(ret));
    } else {
      geo_tree = tree_visitor.get_geometry();
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(geo_tree) || OB_ISNULL(geo_bin)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for geometry", K(ret));
  }
  // calculate 2d box of geo2, then convert the box to a rectangle geo
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(box_ctx.append_geo_arg(geo_bin))) {
    LOG_WARN("build gis context failed", K(ret), K(box_ctx.get_geo_count()));
  } else if (OB_FAIL(ObGeoFunc<ObGeoFuncType::Box>::geo_func::eval(box_ctx, gbox_in))) {
    LOG_WARN("failed to do box functor failed", K(ret));
  } else if (boxes_contains(gbox, *gbox_in)) {
    geo_out = &geo_in;
  } else if (!boxes_overlaps(*gbox_in, gbox)) {
    geo_out = OB_NEWx(ObCartesianGeometrycollection, &allocator, geo_in.get_srid(), allocator);
    if (OB_ISNULL(geo_out)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory for geometry", K(ret));
    }
  } else if (!ObGeoBoxUtil::is_box_valid(gbox)) {
    geo_out = nullptr;
  } else {
    ObGeoBoxClipVisitor clip_visitor(gbox, allocator);
    if (OB_FAIL(geo_tree->do_visit(clip_visitor))) {
      if (ret == OB_ERR_GIS_INVALID_DATA) {
        // pg behavior: return null
        ret = OB_SUCCESS;
        geo_out = nullptr;
      } else {
        LOG_WARN("fail to do tree visitor", K(ret));
      }
    } else if (OB_FAIL(clip_visitor.get_geometry(geo_out))) {
      LOG_WARN("fail to get geometry", K(ret));
    } else {
      geo_out->set_srid(geo_in.get_srid());
    }
  }
  return ret;
}

bool ObGeoBoxUtil::is_box_valid(const ObGeogBox &box)
{
  return !is_float_equal(box.xmin, box.xmax) && !is_float_equal(box.ymin, box.ymax);
}

int ObGeoBoxUtil::get_geom_poly_box(const ObWkbGeomPolygon &poly, bool not_calc_inner_ring, ObGeogBox &res)
{
  int ret = OB_SUCCESS;
  if (poly.size() <= 0) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("empty polygon has not box", K(ret), K(poly.size()));
  } else if (OB_FAIL(get_geom_line_box(poly.exterior_ring(), res))) {
    LOG_WARN("fail to get poly box", K(ret));
  } else if (!not_calc_inner_ring) {
    const ObWkbGeomPolygonInnerRings &inners = poly.inner_rings();
    ObWkbGeomPolygonInnerRings::const_iterator iter = inners.begin();
    for (; OB_SUCC(ret) && iter != inners.end(); ++iter) {
      ObGeogBox tmp_box;
      if (OB_FAIL(get_geom_line_box(*iter, tmp_box))) {
        LOG_WARN("fail to get poly box", K(ret));
      } else {
        box_union(tmp_box, res);
      }
    }
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
  ObGeoType inser_type = ObGeoType::GEO3DTYPEMAX;

  if (wkb_str.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wkb str is empty", K(ret), K(wkb_str));
  } else if (OB_FAIL(ObGeoTypeUtil::get_type_from_wkb(wkb_str, inser_type))) {
    LOG_WARN("fail to get geo type by wkb", K(ret), K(wkb_str));
  } else if (column_type >= ObGeoType::GEOTYPEMAX || inser_type >= ObGeoType::GEO3DTYPEMAX) {
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
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (is_3d_geo_type(geo->type())) {
    ObGeometry3D *geo_3d = static_cast<ObGeometry3D *>(geo);
    if (OB_FAIL(geo_3d->to_wkt(allocator, ewkt, header.srid_))) {
      LOG_WARN("fail to transform ewkt from 3d-wkb", K(ret));
    }
  } else {
    if (OB_FAIL(wkt_visitor.init(header.srid_, max_decimal_digits))) {
      LOG_WARN("failed to init wkt_visitor with srid_", K(ret), K(header.srid_));
    } else if (OB_FAIL(geo->do_visit(wkt_visitor))) {
      LOG_WARN("failed to transform geo to wkt", K(ret));
    } else {
      wkt_visitor.get_wkt(ewkt);
      LOG_DEBUG("eval ob geometry type to ewkt", K(ewkt));
    }
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

int ObGeoTypeUtil::geo_type_in_collection(uint64_t etype, uint64_t interpretation, ObGeoType &type)
{
  INIT_SUCC(ret);
  type = ObGeoType::GEOMETRY;

  switch (etype) {
    case 1: {
      if (interpretation == 1) {
        type = ObGeoType::POINT;
      } else if (interpretation > 1) {
        type = ObGeoType::MULTIPOINT;
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("the type is not supported", K(ret), K(interpretation), K(etype));
      }
      break;
    }

    case 2: {
      if (interpretation == 1) {
        type = ObGeoType::LINESTRING;
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("the type is not supported", K(ret), K(interpretation), K(etype));
      }
      break;
    }

    case 1003: {
      if (interpretation == 1 || interpretation == 3) {
        type = ObGeoType::POLYGON;
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("the type is not supported", K(ret), K(interpretation), K(etype));
      }
      break;
    }

    case 2003: {
      if (interpretation == 1 || interpretation == 3) {
        type = ObGeoType::POLYGON;
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("the type is not supported", K(ret), K(interpretation), K(etype));
      }
      break;
    }

    default: {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("the type is not supported", K(ret), K(interpretation), K(etype));
      break;
    }
  }

  return ret;
}

int ObGeoTypeUtil::get_num_by_gtype(ObGeoType geo_type, uint64_t &num)
{
  int ret = OB_SUCCESS;
  switch (geo_type) {
    case ObGeoType::POINT :
      num = 2001;
      break;
    case ObGeoType::POINTZ :
      num = 3001;
      break;
    case ObGeoType::LINESTRING :
      num = 2002;
      break;
    case ObGeoType::LINESTRINGZ :
      num = 3002;
      break;
    case ObGeoType::POLYGON :
      num = 2003;
      break;
    case ObGeoType::POLYGONZ :
      num = 3003;
      break;
    case ObGeoType::MULTIPOINT :
      num = 2005;
      break;
    case ObGeoType::MULTIPOINTZ :
      num = 3005;
      break;
    case ObGeoType::MULTILINESTRING :
      num = 2006;
      break;
    case ObGeoType::MULTILINESTRINGZ :
      num = 3006;
      break;
    case ObGeoType::MULTIPOLYGON :
      num = 2007;
      break;
    case ObGeoType::MULTIPOLYGONZ :
      num = 3007;
      break;
    case ObGeoType::GEOMETRYCOLLECTION :
      num = 2004;
      break;
    case ObGeoType::GEOMETRYCOLLECTIONZ :
      num = 3004;
      break;
    default :
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("get num by gtype failed", K(ret), K(geo_type));
  }
  return ret;
}

int ObGeoTypeUtil::append_point(double x, double y, ObWkbBuffer &wkb_buf)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(wkb_buf.append(x))) {
    LOG_WARN("fail to append x to point wkb buf", K(ret), K(x));
  } else if (OB_FAIL(wkb_buf.append(y))) {
    LOG_WARN("fail to append y to point wkb buf", K(ret), K(y));
  }
  return ret;
}

int ObGeoTypeUtil::get_gtype_by_num(uint64_t num, ObGeoType &geo_type)
{
  int ret = OB_SUCCESS;
  geo_type = ObGeoType::GEOTYPEMAX;
  if (2001 == num || 3001 == num) {
    geo_type = (2001 == num) ? ObGeoType::POINT : ObGeoType::POINTZ;
  } else if (2002 == num || 3002 == num) {
    geo_type = (2002 == num) ? ObGeoType::LINESTRING : ObGeoType::LINESTRINGZ;
  } else if (2003 == num || 3003 == num) {
    geo_type = (2003 == num) ? ObGeoType::POLYGON : ObGeoType::POLYGONZ;
  } else if (2004 == num || 3004 == num) {
    geo_type =(2004 == num) ?  ObGeoType::GEOMETRYCOLLECTION : ObGeoType::GEOMETRYCOLLECTIONZ;
  } else if (2005 == num || 3005 == num) {
    geo_type = (2005 == num) ? ObGeoType::MULTIPOINT : ObGeoType::MULTIPOINTZ;
  } else if (2006 == num || 3006 == num) {
    geo_type = (2006 == num) ? ObGeoType::MULTILINESTRING : ObGeoType::MULTILINESTRINGZ;
  } else if (2007 == num || 3007 == num) {
    geo_type = (2007 == num) ? ObGeoType::MULTIPOLYGON : ObGeoType::MULTIPOLYGONZ;
  } else {
    ret = OB_ERR_INVALID_GTYPE_IN_SDO_GEROMETRY;
    LOG_WARN("get type by num failed", K(ret), K(geo_type));
  }

  return ret;
}

int ObGeoTypeUtil::rectangle_to_swkb(double xmin, double ymin, double xmax, double ymax, ObGeoSrid srid, bool with_version, ObWkbBuffer &wkb_buf)
{
  int ret = OB_SUCCESS;
  const uint32_t RING_NUM = 1;
  const uint32_t RING_POINT_NUM = 5;
  if (OB_FAIL(wkb_buf.append(srid))) {
    LOG_WARN("fail to append srid to polygon wkb buf", K(ret), K(srid));
  } else if (with_version && OB_FAIL(wkb_buf.append(static_cast<char>(ENCODE_GEO_VERSION(GEO_VESION_1))))) {
    // must have version info, or it will fail in ob_datum_cast
    LOG_WARN("fail to append version to point wkb buf", K(ret));
  } else if (OB_FAIL(wkb_buf.append(static_cast<char>(ObGeoWkbByteOrder::LittleEndian)))) {
    LOG_WARN("fail to append little endian byte order to point wkb buf", K(ret));
  } else if (OB_FAIL(wkb_buf.append(static_cast<uint32_t>(ObGeoType::POLYGON)))) {
    LOG_WARN("fail to append geo type to polygon wkb buf", K(ret));
  } else if (OB_FAIL(wkb_buf.append(static_cast<uint32_t>(RING_NUM)))) {
    LOG_WARN("fail to append ring num to polygon wkb buf", K(ret));
  } else if (OB_FAIL(wkb_buf.append(static_cast<uint32_t>(RING_POINT_NUM)))) {
    LOG_WARN("fail to append ring point num to polygon wkb buf", K(ret));
  } else if (OB_FAIL(append_point(xmin, ymin, wkb_buf))) {
    LOG_WARN("fail to append point to polygon wkb buf", K(ret), K(xmin), K(ymin));
  } else if (OB_FAIL(append_point(xmin, ymax, wkb_buf))) {
    LOG_WARN("fail to append point to polygon wkb buf", K(ret), K(xmax), K(ymin));
  } else if (OB_FAIL(append_point(xmax, ymax, wkb_buf))) {
    LOG_WARN("fail to append point to polygon wkb buf", K(ret), K(xmax), K(ymax));
  } else if (OB_FAIL(append_point(xmax, ymin, wkb_buf))) {
    LOG_WARN("fail to append point to polygon wkb buf", K(ret), K(xmin), K(ymax));
  } else if (OB_FAIL(append_point(xmin, ymin, wkb_buf))) {
    LOG_WARN("fail to append point to polygon wkb buf", K(ret), K(xmin), K(ymin));
  }

  return ret;
}

int ObGeoTypeUtil::wkb_to_sdo_geo(const ObString &swkb, ObSdoGeoObject &sdo_geo, bool with_srid)
{
  int ret = OB_SUCCESS;
  ObGeoWkbByteOrder bo = ObGeoWkbByteOrder::INVALID;
  ObGeoType gtype = ObGeoType::GEOMETRY;
  ObArenaAllocator tmp_allocator;
  uint32_t srid_offset = with_srid ? WKB_GEO_SRID_SIZE : 0;
  ObString wkb = swkb;
  if (with_srid) {
    if (wkb.length() < (WKB_GEO_SRID_SIZE + WKB_GEO_BO_SIZE + WKB_GEO_TYPE_SIZE)) {
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_WARN("invalid wkb length", K(wkb.length()));
    } else {
      wkb.assign_ptr(wkb.ptr() + srid_offset, wkb.length() - srid_offset);
    }
  }
  // read bo and gtype
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (wkb.length() < (WKB_GEO_BO_SIZE + WKB_GEO_TYPE_SIZE)) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("invalid wkb length", K(wkb.length()));
  } else if (FALSE_IT(bo = static_cast<ObGeoWkbByteOrder>(*(wkb.ptr())))) {
  } else if ((bo != ObGeoWkbByteOrder::LittleEndian) && (bo != ObGeoWkbByteOrder::BigEndian)) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("Byte order can only be either LITTLE_ENDIAN (encoded as 1) or BIG_ENDIAN (encoded as 0)",
            K(ret), K(bo));
  } else {
    const char *ptr = (const char *)(wkb.ptr() + WKB_GEO_BO_SIZE);
    gtype = static_cast<ObGeoType>(ObGeoWkbByteOrderUtil::read<uint32_t>(ptr, bo));
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (gtype >= ObGeoType::POINTZ && gtype <= ObGeoType::GEOMETRYCOLLECTIONZ) {
    // 3d wkb
    ObGeometry3D geo_3d;
    geo_3d.set_data(wkb);
    if (OB_FAIL(geo_3d.check_wkb_valid())) {
      LOG_WARN("invalid 3d wkb", K(ret));
    } else if (OB_FAIL(geo_3d.to_sdo_geometry(sdo_geo))) {
      LOG_WARN("failed to transform 3d geo to sdo geometry", K(ret));
    }
  } else if (gtype >= ObGeoType::POINT && gtype <= ObGeoType::GEOMETRYCOLLECTION) {
    // 2d wkb
    ObGeometry *geo = NULL;
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(ObGeoTypeUtil::create_geo_by_type(tmp_allocator, gtype, false, true, geo))) {
      LOG_WARN("failed to create geo by wkb", K(ret), K(gtype));
    } else {
      geo->set_data(wkb);
      ObGeoWkbCheckVisitor wkb_check(wkb, bo, false);
      if (OB_FAIL(geo->do_visit(wkb_check))) {
        ret = OB_ERR_GIS_INVALID_DATA;
        LOG_WARN("fail to check wkb by wkb checker", K(ret), K(wkb));
      } else if (gtype >= ObGeoType::MULTIPOINT || bo == ObGeoWkbByteOrder::BigEndian) {
        // bugfix:
        // transform to LittleEndian
        ObWkbByteOrderVisitor be_visitor(&tmp_allocator, ObGeoWkbByteOrder::LittleEndian);
        if (OB_FAIL(geo->do_visit(be_visitor))) {
          LOG_WARN("fail to transform big endian to little endian", K(ret));
        } else {
          wkb = be_visitor.get_wkb();
          geo->set_data(wkb);
        }
      }
    }

    if (OB_SUCC(ret)) {
      ObIWkbGeometry *geo_bin = static_cast<ObIWkbGeometry *>(geo);
      ObWkbToSdoGeoVisitor sdo_visitor;
      if (OB_FAIL(sdo_visitor.init(&sdo_geo))) {
        LOG_WARN("failed to init sdo_visitor", K(ret));
      } else if (OB_FAIL(geo->do_visit(sdo_visitor))) {
        LOG_WARN("failed to transform geo to sdo geometry", K(ret));
      }
    }
  } else {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("Unknown WKB label", K(ret), K(gtype));
  }
  if (OB_SUCC(ret) && with_srid) {
    uint32_t srid = ObGeoWkbByteOrderUtil::read<uint32_t>(swkb.ptr(), bo);
    sdo_geo.set_srid(srid);
  }
  return ret;
}

int ObGeoTypeUtil::wkt_to_sdo_geo(const ObString &wkt, ObSdoGeoObject &sdo_geo)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator;
  ObGeometry *geo = NULL;
  ObString wkb;

  // wkt -> geo -> wkb -> sdo_geo
  // both geom and geog are ok
  if (OB_FAIL(ObWktParser::parse_wkt(tmp_allocator, wkt, geo, true, false))) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("failed to parse wkt", K(ret));
  } else if (OB_ISNULL(geo)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null geo after parse_wkt", K(ret), K(wkt));
  } else if (FALSE_IT(wkb.assign_ptr(geo->val(), geo->length()))) {
  } else {
    if (OB_FAIL(ObGeoTypeUtil::wkb_to_sdo_geo(wkb, sdo_geo))) {
      LOG_WARN("fail to transfer wkb to sdo_geoetry", K(ret), K(wkb.length()));
    }
  }

  return ret;
}

int ObGeoTypeUtil::number_to_double(const number::ObNumber &num_val, double &res)
{
  int ret = OB_SUCCESS;
  char buf[MAX_DOUBLE_PRINT_SIZE] = {0};
  int64_t pos = 0;
  if (OB_FAIL(num_val.format(buf, sizeof(buf), pos, -1))) {
    LOG_WARN("fail to format number", K(ret), K(num_val));
  } else {
    char *endptr = NULL;
    int err = 0;
    double val = ObCharset::strntod(buf, pos, &endptr, &err);
    if (EOVERFLOW == err && (-DBL_MAX == res || DBL_MAX == res)) {
      ret = OB_DATA_OUT_OF_RANGE;
      LOG_WARN("faild to cast string to double, cause data is out of range",
          K(ret), K(val), K(pos), K(num_val));
    } else {
      res = val;
    }
  }
  return ret;
}

template <typename RetType>
int ObGeoTypeUtil::get_number_obj_from_map(const QualifiedMap &map, const ObString &key, NumberObjType type, bool &is_null_result, RetType &res)
{
  int ret = OB_SUCCESS;
  ObObj *const *obj_ptr;
  ObObj *obj;
  is_null_result = false;

  if (OB_ISNULL(obj_ptr = map.get(key))) {
    is_null_result = true;
  } else if (FALSE_IT(obj = *obj_ptr)) {
  } else if (OB_ISNULL(obj)) {
    is_null_result = true;
  } else if (!obj->is_number()) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("obj should be number type", K(ret), K(key), K(obj->get_type()));
  } else if (type == NumberObjType::DOUBLE) {
    double d_num = 0.0;
    if (OB_FAIL(number_to_double(obj->get_number(), d_num))) {
      LOG_WARN("fail to cast number to double", K(ret), K(key), K(d_num));
    } else {
      res = d_num;
    }
  } else {
    uint64_t i_num;
    if (!obj->get_number().is_valid_uint64(i_num)) {
      LOG_WARN("invalid srs type in sdo_geometry", K(ret), K(key), K(i_num));
    } else {
      res = i_num;
    }
  }

  return ret;
}

template <typename ArrayType>
int ObGeoTypeUtil::get_varry_obj_from_map(const QualifiedMap &map, const ObString &key, const ObObjMeta &num_meta, NumberObjType type, ArrayType &array)
{
  int ret = OB_SUCCESS;
  ObObj *const *obj_ptr;
  ObObj *obj;
  if (OB_ISNULL(obj_ptr = map.get(key))) {
    // do nothing
  } else if (FALSE_IT(obj = *obj_ptr)) {
  } else if (OB_ISNULL(obj)) {
    // do nothing
  } else if (!obj->is_collection_sql_type()) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("obj should be collection sql type", K(ret), K(obj->get_type()));
  } else {
    ObString varray_data = obj->get_string();
    ObLobLocatorV2 lob_locator(varray_data, true);
    if (OB_FAIL(lob_locator.get_inrow_data(varray_data))) {
      COMMON_LOG(WARN, "Lob: get lob inrow data failed", K(ret));
    } else if (varray_data.empty()) {
    } else {
      ObSqlUDT varray_handler;
      varray_handler.set_data(varray_data);
      uint64_t element_count = varray_handler.get_varray_element_count();
      ObString ser_element_data;
      for (int64_t i = 0; OB_SUCC(ret) && i < element_count; i++) {
        if (OB_FAIL(varray_handler.access_attribute(i, ser_element_data, true))) {
          LOG_WARN("fail to access attribute buffer", K(ret));
        } else if (ser_element_data.empty()) {
          ret = OB_ERR_GIS_INVALID_DATA;
          LOG_WARN("number in sdo_elem_info should not be null", K(ret));
        } else {
          ObObj obj;
          obj.set_meta_type(num_meta);
          int64_t pos = 0;
          if (OB_FAIL(ObObjUDTUtil::ob_udt_obj_value_deserialize(obj,
                                                                ser_element_data.ptr(),
                                                                ser_element_data.length(),
                                                                pos))) {
            LOG_WARN("Failed to serialize object value", K(ret), K(ser_element_data));
          } else if (type == NumberObjType::UINT64) {
            uint64_t num;
            if (!obj.get_number().is_valid_uint64(num)) {
              ret = OB_ERR_INVALID_DATA_IN_SDO_ELEM_INFO_ARRAY;
              LOG_WARN("is_valid_uint64 failed", K(ret), K(num));
            } else if (OB_FAIL(array.push_back(num))) {
              LOG_WARN("fail to push item into array", K(ret), K(num));
            }
          } else  {
            double d_num;
            if (OB_FAIL(number_to_double(obj.get_number(), d_num))) {
              ret = OB_ERR_INVALID_DATA_IN_SDO_ORDINATE_ARRAY;
              LOG_WARN("is_valid_uint64 failed", K(ret), K(d_num));
            } else if (OB_FAIL(array.push_back(d_num))) {
              LOG_WARN("fail to push item into array", K(ret), K(d_num));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObGeoTypeUtil::sql_geo_obj_to_ewkt(const QualifiedMap &map, ObIAllocator &allocator, ObString &ewkt)
{
  int ret = OB_SUCCESS;
  ObSdoGeoObject sdo_geometry;
  bool is_null_result = false;

  uint64_t gtype_num;
  ObGeoType gtype;
  if (OB_FAIL(get_number_obj_from_map(map, "SDO_GTYPE", NumberObjType::UINT64, is_null_result, gtype_num)) || is_null_result) {
    LOG_WARN("fail to get not null sdo_gtype", K(ret));
  } else if (OB_FAIL(ObGeoTypeUtil::get_gtype_by_num(gtype_num, gtype))) {
    LOG_WARN("fail to get_gtype_by_num", K(ret), K(gtype_num), K(gtype));
  } else {
    sdo_geometry.set_gtype(gtype);
  }

  uint64_t srid_tmp;
  uint32_t srid;
  bool srid_is_null = true;
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(get_number_obj_from_map(map, "SDO_SRID", NumberObjType::UINT64, srid_is_null, srid_tmp))) {
    LOG_WARN("fail to get SDO_SRID", K(ret));
  } else if (!srid_is_null) {
    if (srid_tmp > UINT32_MAX) {
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_WARN("srid is out of range", K(ret));
    } else {
      srid = srid_tmp;
      sdo_geometry.set_srid(srid);
    }
  }

  ObSdoPoint &point = sdo_geometry.get_point();
  double d_num;
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(get_number_obj_from_map(map, "SDO_POINT.X", NumberObjType::DOUBLE, is_null_result, d_num))) {
    LOG_WARN("fail to get not null SDO_POINT.X", K(ret));
  } else if (is_null_result) {
    // do nothing
  } else if (FALSE_IT(point.set_x(d_num))) {
  } else if (OB_FAIL(get_number_obj_from_map(map, "SDO_POINT.Y", NumberObjType::DOUBLE, is_null_result, d_num)) || is_null_result) {
    LOG_WARN("fail to get not null SDO_POINT.Y", K(ret));
  } else if (FALSE_IT(point.set_y(d_num))) {
  } else if (OB_FAIL(get_number_obj_from_map(map, "SDO_POINT.Z", NumberObjType::DOUBLE, is_null_result, d_num))) {
    LOG_WARN("fail to get not null SDO_POINT.Z", K(ret));
  } else if (!is_null_result) {
    point.set_z(d_num);
  }

  ObArray<uint64_t> &elem_info = sdo_geometry.get_elem_info();
  ObArray<double> &ordinates = sdo_geometry.get_ordinates();
  if (OB_SUCC(ret)) {
    const ObObjMeta &num_meta = (*map.get("SDO_GTYPE"))->get_meta();
    if (OB_FAIL(get_varry_obj_from_map(map, "SDO_ELEM_INFO", num_meta, NumberObjType::UINT64, elem_info))) {
      LOG_WARN("fail to get SDO_ELEM_INFO from obj", K(ret), K(elem_info.size()));
    } else if (OB_FAIL(get_varry_obj_from_map(map, "SDO_ORDINATES", num_meta, NumberObjType::DOUBLE, ordinates))) {
      LOG_WARN("fail to get SDO_ORDINATES from obj", K(ret), K(ordinates.size()));
    }
  }

  if (OB_SUCC(ret)) {
    ObSdoGeoToWkb trans(&allocator);
    ObString swkb;
    const int64_t WKT_DOUBLE_SCALE = 14;
    if (OB_FAIL(trans.translate(&sdo_geometry, swkb, true, ObGeoWkbByteOrder::LittleEndian))) {
      LOG_WARN("fail to transfer sdo_geometry to wkb", K(ret), K(swkb));
    } else if (OB_FAIL(ObGeoTypeUtil::geo_to_ewkt(swkb, ewkt, allocator, WKT_DOUBLE_SCALE))) {
      LOG_WARN("fail to get ewkt from swkb", K(ret), K(swkb));
    }
  }

  return ret;
}

bool ObGeoTypeUtil::is_3d_geo_type(ObGeoType geo_type) {
  return geo_type == ObGeoType::POINTZ || geo_type == ObGeoType::LINESTRINGZ ||
         geo_type == ObGeoType::POLYGONZ || geo_type == ObGeoType::MULTIPOINTZ ||
         geo_type == ObGeoType::MULTILINESTRINGZ || geo_type == ObGeoType::MULTIPOLYGONZ ||
         geo_type == ObGeoType::GEOMETRYCOLLECTIONZ;
}

bool ObGeoTypeUtil::is_2d_geo_type(ObGeoType geo_type) {
  return geo_type == ObGeoType::POINT || geo_type == ObGeoType::LINESTRING ||
         geo_type == ObGeoType::POLYGON || geo_type == ObGeoType::MULTIPOINT ||
         geo_type == ObGeoType::MULTILINESTRING || geo_type == ObGeoType::MULTIPOLYGON ||
         geo_type == ObGeoType::GEOMETRYCOLLECTION;
}

bool ObGeoTypeUtil::is_multi_geo_type(ObGeoType geo_type) {
  return geo_type == ObGeoType::MULTIPOINT ||
         geo_type == ObGeoType::MULTILINESTRING || geo_type == ObGeoType::MULTIPOLYGON ||
         geo_type == ObGeoType::GEOMETRYCOLLECTION;
}

int ObGeoTypeUtil::get_coll_dimension(ObIWkbGeomCollection *geo, int8_t &dimension)
{
  int ret = OB_SUCCESS;
  const ObWkbGeomCollection *collection = reinterpret_cast<const ObWkbGeomCollection *>(geo->val());
  ObWkbGeomCollection::iterator iter = collection->begin();
  uint64_t total_len = collection->length();
  uint64_t pos = WKB_COMMON_WKB_HEADER_LEN;
  for (; iter != collection->end() && OB_SUCC(ret); iter++) {
    typename ObWkbGeomCollection::const_pointer sub_ptr = iter.operator->();
    if (pos + WKB_GEO_TYPE_SIZE + WKB_GEO_BO_SIZE > total_len) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid wkb geo", K(ret), K(total_len), K(pos));
    } else {
      ObGeoType sub_type = collection->get_sub_type(sub_ptr);
      switch (sub_type) {
        case ObGeoType::POINT:
        case ObGeoType::MULTIPOINT: {
          dimension = dimension >= 0 ? dimension : 0;
          break;
        }
        case ObGeoType::LINESTRING:
        case ObGeoType::MULTILINESTRING: {
          dimension = dimension >= 1 ? dimension : 1;
          break;
        }
        case ObGeoType::POLYGON:
        case ObGeoType::MULTIPOLYGON: {
          dimension = dimension >= 2 ? dimension : 2;
          break;
        }
        case ObGeoType::GEOMETRYCOLLECTION: {
          const ObWkbGeomCollection *subgc = reinterpret_cast<const ObWkbGeomCollection *>(sub_ptr);
          ObIWkbGeomCollection sub_collection;
          ObString data(total_len - pos, reinterpret_cast<const char *>(subgc));
          sub_collection.set_data(data);
          if (OB_FAIL(get_coll_dimension(&sub_collection, dimension))) {
            LOG_WARN("failed to do wkb geom sub collection visit", K(ret));
          } else {
            pos += sub_collection.length();
          }
          break;
        }
        default: {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid geo type", K(ret), K(sub_type));
          break;
        }
      }
    }
  }
  return ret;
}
int ObLineSegment::get_box(ObCartesianBox &box)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(verts)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("vertexes is null", K(ret));
  } else if (begin >= end) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("line segment don't have enough points", K(begin), K(end), K(ret));
  } else {
    double min_x = (*verts)[begin].x;
    double min_y = (*verts)[begin].y;
    double max_x = (*verts)[begin].x;
    double max_y = (*verts)[begin].y;
    for (uint32_t i = begin + 1; i <= end; i++) {
      min_x = std::min(min_x, (*verts)[i].x);
      min_y = std::min(min_y, (*verts)[i].y);
      max_x = std::max(max_x, (*verts)[i].x);
      max_y = std::max(max_y, (*verts)[i].y);
    }
    box.min_corner().set<0>(min_x);
    box.min_corner().set<1>(min_y);
    box.max_corner().set<0>(max_x);
    box.max_corner().set<1>(max_y);
  }
  return ret;
}

int ObSegment::get_box(ObCartesianBox &box)
{
  double min_x = std::min(begin.x, end.x);
  double min_y = std::min(begin.y, end.y);
  double max_x = std::max(begin.x, end.x);
  double max_y = std::max(begin.y, end.y);
  box.min_corner().set<0>(min_x);
  box.min_corner().set<1>(min_y);
  box.max_corner().set<0>(max_x);
  box.max_corner().set<1>(max_y);
  return OB_SUCCESS;
}

int ObGeoTypeUtil::get_quadrant_direction(const ObPoint2d &start, const ObPoint2d &end, QuadDirection &res)
{
  int ret = OB_SUCCESS;
  if(start.x == end.x && start.y == end.y) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("start and end is same", K(start.x), K(start.y));
  } else {
    if (end.x >= start.x) {
      if (end.y >= start.y) {
        res = QuadDirection::NORTH_EAST;
      } else {
        res = QuadDirection::SOUTH_EAST;
      }
    } else {
      if (end.y >= start.y) {
        res = QuadDirection::NORTH_WEST;
      } else {
        res = QuadDirection::SOUTH_WEST;
      }
    }
  }
  return ret;
}

double ObGeoTypeUtil::distance_point_squre(const ObWkbGeomInnerPoint& p1, const ObWkbGeomInnerPoint& p2)
{
  double x = p1.get<0>() - p2.get<0>();
  double y = p1.get<1>() - p2.get<1>();
  return x * x + y * y;
}

int ObGeoTypeUtil::check_empty(ObGeometry *geo, bool &is_empty)
{
  INIT_SUCC(ret);
  is_empty = false;
  if (is_3d_geo_type(geo->type())) {
    ObGeometry3D *geo3D = reinterpret_cast<ObGeometry3D *>(geo);
    if (OB_FAIL(geo3D->check_empty(is_empty))) {
      LOG_WARN("fail to check 3D geometry empty", K(ret));
    }
  } else {
    ObGeoCheckEmptyVisitor check_empty_visitor;
    if (OB_FAIL(geo->do_visit(check_empty_visitor))) {
      LOG_WARN("check empty geo failed", K(ret));
    } else {
      is_empty = check_empty_visitor.get_result();
    }
  }
  return ret;
}

int ObGeoMVTUtil::affine_transformation(ObGeometry *geo, const ObAffineMatrix &affine)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(geo)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("geometry can not be null", K(ret));
  } else {
    ObGeoAffineVisitor affine_visitor(&affine);
    if (OB_FAIL(geo->do_visit(affine_visitor))) {
      LOG_WARN("fail to do affine visitor", K(ret));
    }
  }
  return ret;
}

template<typename CachedGeoType>
int ObGeoTypeUtil::create_cached_geometry(ObIAllocator &allocator, ObGeometry *geo, ObCachedGeom *&cached_geo, const ObSrsItem *srs)
{
  int ret = OB_SUCCESS;
  CachedGeoType *tmp_cached = reinterpret_cast<CachedGeoType *>(allocator.alloc(sizeof(CachedGeoType)));
  if (OB_ISNULL(tmp_cached)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc cached polygon", K(ret));
  } else {
    cached_geo = new(tmp_cached) CachedGeoType(geo, allocator, srs);
  }
  return ret;
}

int ObGeoTypeUtil::get_polygon_size(ObGeometry &geo)
{
  int size = 0;
  if (geo.crs() == ObGeoCRS::Cartesian) {
    if (geo.type() == ObGeoType::POLYGON) {
      size = (static_cast<ObIWkbGeomPolygon*>(&geo))->size();
    } else {
      const ObWkbGeomMultiPolygon *multi_poly = reinterpret_cast<const ObWkbGeomMultiPolygon*>(&geo);
      size = multi_poly->size();
    }
  } else {
    if (geo.type() == ObGeoType::POLYGON) {
      size = (static_cast<ObIWkbGeogPolygon*>(&geo))->size();
    } else {
      const ObWkbGeogMultiPolygon *multi_poly = reinterpret_cast<const ObWkbGeogMultiPolygon*>(&geo);
      size = multi_poly->size();
    }
  }
  return size;
}

int ObGeoTypeUtil::magnify_and_recheck(ObIAllocator &allocator, ObGeometry &geo, ObGeoEvalCtx& gis_context, bool& invalid_for_cache)
{
  int ret = OB_SUCCESS;
  ObGeometry *tmp_geo = nullptr;
  bool valid = false;
  bool need_recheck = false;
  ObGeoZoomInVisitor zoom_in_visitor(RECHECK_ZOOM_IN_VALUE);
  const ObGeoNormalVal *val_arg = nullptr;
  if (OB_FAIL(copy_geometry(allocator, geo, tmp_geo)) || OB_ISNULL(tmp_geo)) {
    // do nothing, return invalid_for_cache
  } else if (OB_FAIL(tmp_geo->do_visit(zoom_in_visitor))) {
    LOG_WARN("failed to zoom in visit", K(ret));
  } else {
    gis_context.ut_set_geo_arg(0, tmp_geo);
    ret = check_valid_and_self_intersects(gis_context, invalid_for_cache, need_recheck);
  }
  return ret;
}

int ObGeoTypeUtil::check_valid_and_self_intersects(ObGeoEvalCtx& gis_context, bool& invalid_for_cache, bool& need_recheck)
{
  int ret = OB_SUCCESS;
  bool valid = false;
  const ObGeoNormalVal *val_arg = nullptr;
  if (OB_FAIL(ObGeoFunc<ObGeoFuncType::IsValid>::geo_func::eval(gis_context, valid))) {
    LOG_WARN("eval geo func isvalid failed", K(ret));
  } else if (valid) {
    invalid_for_cache = false;
  } else if (OB_FALSE_IT(val_arg = gis_context.get_val_arg(0))) {
  } else if (OB_ISNULL(val_arg)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should not be null", K(ret));
  } else if (static_cast<int64_t>(bg::validity_failure_type::failure_self_intersections) == val_arg->int64_) {
    invalid_for_cache = true;
    need_recheck = true;
  } else if (static_cast<int64_t>(bg::validity_failure_type::failure_wrong_topological_dimension) == val_arg->int64_) {
    invalid_for_cache = true;
    need_recheck = false;
  } else {
    invalid_for_cache = false;
  }
  return ret;
}

int ObGeoTypeUtil::polygon_check_self_intersections(ObIAllocator &allocator, ObGeometry &geo, const ObSrsItem *srs, bool& invalid_for_cache)
{
  int ret = OB_SUCCESS;
  ObGeoEvalCtx gis_context(&allocator, srs);
  ObGeoNormalVal reason;
  bool valid = false;
  const ObGeoNormalVal *val_arg = nullptr;
  bool need_recheck = false;
  if (OB_FAIL(gis_context.append_geo_arg(&geo))) {
    LOG_WARN("build geo gis context failed", K(ret));
  } else if (OB_FAIL(gis_context.append_val_arg(reason))) {
    LOG_WARN("add reason val to context failed", K(ret));
  } else if (OB_FAIL(check_valid_and_self_intersects(gis_context, invalid_for_cache, need_recheck))) {
    LOG_WARN("fail to check_valid_if_self_intersects.", K(ret));
  } else if (invalid_for_cache && need_recheck && OB_FAIL(magnify_and_recheck(allocator, geo, gis_context, invalid_for_cache))) {
    LOG_WARN("fail to magnify_and_recheck.", K(ret));
  } else if (invalid_for_cache) {
    LOG_WARN("self intersects, use geo cache base.", K(ret));
  }
  return ret;
}

int ObGeoTypeUtil::create_cached_geometry(ObIAllocator &allocator,
                                          ObIAllocator &tmp_allocator,
                                          ObGeometry *geo,
                                          const ObSrsItem *srs,
                                          ObCachedGeom *&cached_geo)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(geo)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should not be null", K(ret));
  } else {
    switch (geo->type()) {
      case ObGeoType::POINT:
      case ObGeoType::MULTIPOINT: {
        ret = create_cached_geometry<ObCachedGeoPoint>(allocator, geo, cached_geo, srs);
        break;
      }
      case ObGeoType::LINESTRING:
      case ObGeoType::MULTILINESTRING:
      {
        ret = create_cached_geometry<ObCachedGeoLinestring>(allocator, geo, cached_geo, srs);
        break;
      }
      case ObGeoType::POLYGON:
      case ObGeoType::MULTIPOLYGON: {
        ret = create_cached_geometry<ObCachedGeoPolygon>(allocator, geo, cached_geo, srs);
        break;
      }
      case ObGeoType::GEOMETRYCOLLECTION: {
        ret = create_cached_geometry<ObCachedGeomBase>(allocator, geo, cached_geo, srs);
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("type not supported yet.", K(geo->type()) ,K(ret));
        break;
      }
    }
  }
  return ret;
}

int ObGeoTypeUtil::get_geo_dimension(ObGeometry *geo, ObGeoDimension& dim)
{
  int ret = OB_SUCCESS;
  dim = ObGeoDimension::MAX_DIMENSION;
  if (OB_ISNULL(geo)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null.", K(ret));
  } else {
    switch (geo->type()) {
      case ObGeoType::POINT:
      case ObGeoType::MULTIPOINT: {
        dim = ObGeoDimension::ZERO_DIMENSION;
        break;
      }
      case ObGeoType::LINESTRING:
      case ObGeoType::MULTILINESTRING:
      {
        dim = ObGeoDimension::ONE_DIMENSION;
        break;
      }
      case ObGeoType::POLYGON:
      case ObGeoType::MULTIPOLYGON: {
        dim = ObGeoDimension::TWO_DIMENSION;
        break;
      }
      case ObGeoType::GEOMETRYCOLLECTION: {
        if (geo->crs() == ObGeoCRS::Cartesian) {
          ret = get_collection_dimension<ObIWkbGeomCollection, ObWkbGeomCollection>(static_cast<ObIWkbGeomCollection*>(geo), dim);
        } else {
          ret = get_collection_dimension<ObIWkbGeogCollection, ObWkbGeogCollection>(static_cast<ObIWkbGeogCollection*>(geo), dim);
        }
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("type not supported yet.", K(geo->type()), K(ret));
        break;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (dim == ObGeoDimension::MAX_DIMENSION) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get geo dimension.", K(ret));
    }
  }
  return ret;
}

int ObGeoTypeUtil::has_dimension(ObGeometry& geo, ObGeoDimension dim, bool& res)
{
  int ret = OB_SUCCESS;
  res = false;
  if (geo.type() == ObGeoType::GEOMETRYCOLLECTION) {
    if (geo.crs() == ObGeoCRS::Cartesian) {
      ret = collection_has_dimension<ObIWkbGeomCollection, ObWkbGeomCollection>(static_cast<ObIWkbGeomCollection*>(&geo), dim, res);
    } else {
      ret = collection_has_dimension<ObIWkbGeogCollection, ObWkbGeogCollection>(static_cast<ObIWkbGeogCollection*>(&geo), dim, res);
    }
  } else if (dim == ObGeoDimension::ZERO_DIMENSION) {
    res = is_point(geo);
  } else if (dim == ObGeoDimension::ONE_DIMENSION) {
    res = is_line(geo);
  } else if (dim == ObGeoDimension::TWO_DIMENSION) {
    res = is_polygon(geo);
  }
  return ret;
}

bool ObGeoTypeUtil::use_point_polygon_short_circuit(const ObGeometry& geo1, const ObGeometry& geo2, ObItemType func_type)
{
  bool ret_bool = false;
  if (func_type == ObItemType::T_FUN_SYS_ST_INTERSECTS) {
    ret_bool = (is_point(geo1) && is_polygon(geo2)) || (is_point(geo2) && is_polygon(geo1));
  } else if (func_type == ObItemType::T_FUN_SYS_ST_COVERS || func_type == ObItemType::T_FUN_SYS_ST_CONTAINS){
    ret_bool = is_polygon(geo1) && is_point(geo2);
  } else if (func_type == ObItemType::T_FUN_SYS_ST_WITHIN) {
    ret_bool = is_point(geo1) && is_polygon(geo2);
  }
  return ret_bool;
}

int ObGeoTypeUtil::point_polygon_short_circuit(ObGeometry *poly, ObGeometry *point, ObPointLocation& loc, bool& has_internal, bool get_fartest)
{
  int ret = OB_SUCCESS;
  if (point->type() == ObGeoType::POINT) {
    ObIWkbPoint* tmp_p = static_cast<ObIWkbPoint*>(point);
    ObPoint2d p;
    p.x = tmp_p->x();
    p.y = tmp_p->y();
    ObGeoPointLocationVisitor point_loc_visitor(p);
    if (OB_FAIL(poly->do_visit(point_loc_visitor))) {
      LOG_WARN("failed to do point location visitor", K(ret));
    } else {
      loc = point_loc_visitor.get_point_location();
      has_internal = (loc == ObPointLocation::INTERIOR);
    }
  } else {
    // check if geo point on cached line
    ObVertexes input_vertexes;
    ObGeoVertexCollectVisitor vertex_visitor(input_vertexes);
    if (OB_FAIL(point->do_visit(vertex_visitor))) {
      LOG_WARN("failed to collect geo vertexes", K(ret));
    } else {
      ObPointLocation tmp_loc = ObPointLocation::INVALID;
      int size = input_vertexes.size();
      for (uint32_t i = 0; i < size && OB_SUCC(ret) && (loc == ObPointLocation::INVALID); i++) {
        ObGeoPointLocationVisitor point_loc_visitor(input_vertexes[i]);
        if (OB_FAIL(poly->do_visit(point_loc_visitor))) {
          LOG_WARN("failed to do point location visitor", K(ret));
        } else if (!get_fartest && (point_loc_visitor.get_point_location() == ObPointLocation::INTERIOR
                  || point_loc_visitor.get_point_location() == ObPointLocation::BOUNDARY)) {
          loc = point_loc_visitor.get_point_location();
        } else if (get_fartest) {
          if (!has_internal && point_loc_visitor.get_point_location() == ObPointLocation::INTERIOR) {
              has_internal = true;
            }
          if (tmp_loc == ObPointLocation::INVALID || tmp_loc < point_loc_visitor.get_point_location()) {
            tmp_loc = point_loc_visitor.get_point_location();
          }
          if (tmp_loc == ObPointLocation::EXTERIOR) {
            loc = tmp_loc;
          }
        }
      }

      if (OB_SUCC(ret) && loc == ObPointLocation::INVALID) {
        loc = get_fartest ? tmp_loc : ObPointLocation::EXTERIOR;
      }
    }
  }
  return ret;
}

int ObGeoTypeUtil::get_point_polygon_res(ObGeometry *geo1, ObGeometry *geo2, ObItemType func_type, bool& result)
{
  int ret = OB_SUCCESS;
  ObPointLocation loc = ObPointLocation::INVALID;
  ObGeometry *poly = nullptr;
  ObGeometry *point = nullptr;
  if (OB_ISNULL(geo1) || OB_ISNULL(geo2)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null.", K(ret));
  } else {
    point = is_point(*geo1) ? geo1 : geo2;
    poly = is_point(*geo1) ? geo2 : geo1;
    bool get_fartest = (func_type != ObItemType::T_FUN_SYS_ST_INTERSECTS);
    bool has_internal = false;
    if (OB_FAIL(point_polygon_short_circuit(poly, point, loc, has_internal, get_fartest))) {
      LOG_WARN("fail to get point polygon position.", K(ret));
    } else if (!get_fartest) {
      result = (loc == ObPointLocation::INTERIOR || loc == ObPointLocation::BOUNDARY);
    } else if (get_fartest) {
      if (loc == ObPointLocation::EXTERIOR) {
        result = false;
      } else if (loc == ObPointLocation::INTERIOR || (loc == ObPointLocation::BOUNDARY && has_internal)) {
        result = true;
      } else {
        result = (func_type == ObItemType::T_FUN_SYS_ST_COVERS);
      }
    }
  }
  return ret;
}

template<typename T_IBIN, typename T_BIN>
int ObGeoTypeUtil::get_collection_dimension(T_IBIN *geo, ObGeoDimension& dim)
{
  int ret = OB_SUCCESS;
  const T_BIN *collection = reinterpret_cast<const T_BIN*>(geo->val());
  typename T_BIN::iterator iter = collection->begin();
  uint64_t total_len = geo->length();
  uint64_t pos = WKB_COMMON_WKB_HEADER_LEN;
  dim = ObGeoDimension::MAX_DIMENSION;
  for ( ; iter != collection->end() && OB_SUCC(ret) && !(dim == ObGeoDimension::TWO_DIMENSION); iter++) {
    ObGeoDimension tmp_dim = ObGeoDimension::ZERO_DIMENSION;
    typename ObWkbGeomCollection::const_pointer sub_ptr = iter.operator->();
    if (pos + WKB_GEO_TYPE_SIZE + WKB_GEO_BO_SIZE > total_len) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid wkb geo", K(ret), K(total_len), K(pos), K(geo->val()));
    } else {
      ObGeoType sub_type = collection->get_sub_type(sub_ptr);
      switch (sub_type) {
        case ObGeoType::POINT:
        case ObGeoType::MULTIPOINT : {
          tmp_dim = ObGeoDimension::ZERO_DIMENSION;
          break;
        }
        case ObGeoType::LINESTRING:
        case ObGeoType::MULTILINESTRING : {
          tmp_dim = ObGeoDimension::ONE_DIMENSION;
          break;
        }
        case ObGeoType::POLYGON:
        case ObGeoType::MULTIPOLYGON : {
          tmp_dim = ObGeoDimension::TWO_DIMENSION;
          break;
        }
        case ObGeoType::GEOMETRYCOLLECTION : {
          const T_BIN *subgc = reinterpret_cast<const T_BIN *>(sub_ptr);
          T_IBIN sub_collection;
          ObString data(total_len - pos, reinterpret_cast<const char *>(subgc));
          sub_collection.set_data(data);
          ret = get_collection_dimension<T_IBIN, T_BIN>(&sub_collection, tmp_dim);
          if (OB_FAIL(ret)) {
            LOG_WARN("failed to do wkb geom sub collection visit", K(ret));
          } else {
            pos += sub_collection.length();
          }
          break;
        }
        default : {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid geo type", K(ret), K(sub_type));
          break;
        }
      }
      if (OB_FAIL(ret) || tmp_dim == ObGeoDimension::MAX_DIMENSION) {
      } else if (dim == ObGeoDimension::MAX_DIMENSION
              || dim < tmp_dim) {
        dim = tmp_dim;
      }
    }
  }
  return ret;
}

template<typename T_IBIN, typename T_BIN>
int ObGeoTypeUtil::collection_has_dimension(T_IBIN *geo, ObGeoDimension dim, bool& res)
{
  int ret = OB_SUCCESS;
  const T_BIN *collection = reinterpret_cast<const T_BIN*>(geo->val());
  typename T_BIN::iterator iter = collection->begin();
  uint64_t total_len = geo->length();
  uint64_t pos = WKB_COMMON_WKB_HEADER_LEN;
  for ( ; iter != collection->end() && OB_SUCC(ret) && !res; iter++) {
    ObGeoDimension tmp_dim = ObGeoDimension::MAX_DIMENSION;
    typename ObWkbGeomCollection::const_pointer sub_ptr = iter.operator->();
    if (pos + WKB_GEO_TYPE_SIZE + WKB_GEO_BO_SIZE > total_len) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid wkb geo", K(ret), K(total_len), K(pos), K(geo->val()));
    } else {
      ObGeoType sub_type = collection->get_sub_type(sub_ptr);
      switch (sub_type) {
        case ObGeoType::POINT:
        case ObGeoType::MULTIPOINT : {
          res = (dim == ObGeoDimension::ZERO_DIMENSION);
          break;
        }
        case ObGeoType::LINESTRING:
        case ObGeoType::MULTILINESTRING : {
          res = (dim == ObGeoDimension::ONE_DIMENSION);
          break;
        }
        case ObGeoType::POLYGON:
        case ObGeoType::MULTIPOLYGON : {
          res = (dim == ObGeoDimension::TWO_DIMENSION);
          break;
        }
        case ObGeoType::GEOMETRYCOLLECTION : {
          ret = get_collection_dimension<T_IBIN, T_BIN>(geo, dim);
          break;
        }
        default : {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid geo type", K(ret), K(sub_type));
          break;
        }
      }
    }
  }
  return ret;
}

bool ObGeoTypeUtil::need_get_srs(const uint32_t srid)
{
  bool ret_bool = true;
  if (lib::is_oracle_mode() && srid == UINT_MAX32) {
    ret_bool = false;
  } else if (0 == srid) {
    ret_bool = false;
  }
  return ret_bool;
}

int ObGeoMVTUtil::snap_to_grid(ObGeometry *geo, const ObGeoGrid &grid, bool use_floor)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(geo)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("geometry can not be null", K(ret));
  } else {
    ObGeoGridVisitor grid_visitor(&grid, use_floor);
    if (OB_FAIL(geo->do_visit(grid_visitor))) {
      LOG_WARN("fail to do affine visitor", K(ret));
    }
  }
  return ret;
}

int ObGeoMVTUtil::simplify_geometry(ObGeometry *geo, double tolerance, bool keep_collapsed)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(geo)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("geometry can not be null", K(ret));
  } else {
    ObGeoSimplifyVisitor simplify_visitor(tolerance, keep_collapsed);
    if (OB_FAIL(geo->do_visit(simplify_visitor))) {
      LOG_WARN("fail to do affine visitor", K(ret));
    }
  }
  return ret;
}
} // namespace common
} // namespace oceanbase
