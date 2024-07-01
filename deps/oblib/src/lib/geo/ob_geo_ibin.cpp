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
 * This file contains implementation support for the geo bin interface.
 */

#define USING_LOG_PREFIX LIB
#include "lib/geo/ob_geo_ibin.h"
#include "lib/geo/ob_geo_visitor.h"

namespace oceanbase
{
namespace common
{

bool ObIWkbGeometry::is_empty() const
{
  const char* ptr = val();
  bool empty = true;
  if (OB_NOT_NULL(ptr)) {
    empty = this->is_empty_inner();
  } else {
    LOG_WARN_RET(OB_ERR_NULL_VALUE, "Try to access NULL pointer.", K(OB_ERR_NULL_VALUE));
  }
  return empty;
}

uint64_t ObIWkbGeometry::length() const
{
  const char* ptr = val();
  uint64_t s = 0;
  if (OB_NOT_NULL(ptr)) {
    s = this->length_inner();
  } else {
    LOG_WARN_RET(OB_ERR_NULL_VALUE, "Try to access NULL pointer.", K(OB_ERR_NULL_VALUE));
  }
  return s;
}

double ObIWkbGeomPoint::x() const
{
  const ObWkbGeomPoint* ptr = reinterpret_cast<const ObWkbGeomPoint*>(val());
  double val = NAN;
  if (OB_NOT_NULL(ptr)) {
    val = ptr->get<0>();
  } else {
    LOG_WARN_RET(OB_ERR_NULL_VALUE, "Try to access NULL pointer.", K(OB_ERR_NULL_VALUE));
  }
  return val;
}

double ObIWkbGeomPoint::y() const
{
  const ObWkbGeomPoint* ptr = reinterpret_cast<const ObWkbGeomPoint*>(val());
  double val = NAN;
  if (OB_NOT_NULL(ptr)) {
    val = ptr->get<1>();
  } else {
    LOG_WARN_RET(OB_ERR_NULL_VALUE, "Try to access NULL pointer.", K(OB_ERR_NULL_VALUE));
  }
  return val;
}

void ObIWkbGeomPoint::x(double d)
{
  ObWkbGeomPoint* ptr = reinterpret_cast<ObWkbGeomPoint*>(val());
  if (OB_NOT_NULL(ptr)) {
    ptr->set<0>(d);
  } else {
    LOG_WARN_RET(OB_ERR_NULL_VALUE, "Try to access NULL pointer.", K(OB_ERR_NULL_VALUE));
  }
}

void ObIWkbGeomPoint::y(double d)
{
  ObWkbGeomPoint* ptr = reinterpret_cast<ObWkbGeomPoint*>(val());
  if (OB_NOT_NULL(ptr)) {
    ptr->set<1>(d);
  } else {
    LOG_WARN_RET(OB_ERR_NULL_VALUE, "Try to access NULL pointer.", K(OB_ERR_NULL_VALUE));
  }
}

bool ObIWkbGeomPolygon::is_empty_inner() const
{
  const ObWkbGeomPolygon* poly = reinterpret_cast<const ObWkbGeomPolygon*>(val());
  return (poly->exterior_ring().size(static_cast<ObGeoWkbByteOrder>(poly->get_bo())) == 0) && (poly->inner_rings().size() == 0);
}

int ObIWkbGeomCollection::get_sub(uint32_t idx, ObGeometry*& geo) const
{
  INIT_SUCC(ret);
  ObWkbGeomCollection* g = reinterpret_cast<ObWkbGeomCollection*>(const_cast<char*>(val()));
  if (OB_ISNULL(this->allocator_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("Null allocator.", K(ret));
  } else if (idx >= g->size()) {
    ret = OB_ERR_INTERVAL_INVALID;
    LOG_WARN("Invalid index.", K(ret), K(idx), K(g->size()));
  } else {
    ObWkbGeomCollection::iterator iter = g->begin();
    iter += idx;
    const char* ptr = iter.operator->();
    uint64_t size = g->get_sub_size(ptr);
    // TODO  use create_obj_by_type
    switch (g->get_sub_type(ptr)) {
      case ObGeoType::POINT: {
        geo = OB_NEWx(ObIWkbGeomPoint, this->allocator_);
        break;
      }
      default: {
        ret = OB_ERR_INTERVAL_INVALID;
        break;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(geo)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Alloc sub geo falied.", K(ret));
      } else if (size <= WKB_COMMON_WKB_HEADER_LEN) {
        ret = OB_ERR_INTERVAL_INVALID;
        LOG_WARN("get sub geo size falied.", K(ret), K(size));
      } else {
        ObString sub_wkb(size, ptr);
        geo->set_data(sub_wkb);
      }
    }
  }
  return ret;
}





double ObIWkbGeogPoint::x() const
{
  const ObWkbGeogPoint* ptr = reinterpret_cast<const ObWkbGeogPoint*>(val());
  double val = NAN;
  if (OB_NOT_NULL(ptr)) {
    val = ptr->get<0>();
  } else {
    LOG_WARN_RET(OB_ERR_NULL_VALUE, "Try to access NULL pointer.", K(OB_ERR_NULL_VALUE));
  }
  return val;
}

double ObIWkbGeogPoint::y() const
{
  const ObWkbGeogPoint* ptr = reinterpret_cast<const ObWkbGeogPoint*>(val());
  double val = NAN;
  if (OB_NOT_NULL(ptr)) {
    val = ptr->get<1>();
  } else {
    LOG_WARN_RET(OB_ERR_NULL_VALUE, "Try to access NULL pointer.", K(OB_ERR_NULL_VALUE));
  }
  return val;
}

void ObIWkbGeogPoint::x(double d)
{
  ObWkbGeogPoint* ptr = reinterpret_cast<ObWkbGeogPoint*>(val());
  if (OB_NOT_NULL(ptr)) {
    ptr->set<0>(d);
  } else {
    LOG_WARN_RET(OB_ERR_NULL_VALUE, "Try to access NULL pointer.", K(OB_ERR_NULL_VALUE));
  }
}

void ObIWkbGeogPoint::y(double d)
{
  ObWkbGeogPoint* ptr = reinterpret_cast<ObWkbGeogPoint*>(val());
  if (OB_NOT_NULL(ptr)) {
    ptr->set<1>(d);
  } else {
    LOG_WARN_RET(OB_ERR_NULL_VALUE, "Try to access NULL pointer.", K(OB_ERR_NULL_VALUE));
  }
}

bool ObIWkbGeogPolygon::is_empty_inner() const
{
  const ObWkbGeogPolygon* poly = reinterpret_cast<const ObWkbGeogPolygon*>(val());
  bool bret = true;
  if (OB_NOT_NULL(poly)) {
    bret = (poly->exterior_ring().size(static_cast<ObGeoWkbByteOrder>(poly->get_bo())) == 0) && (poly->inner_rings().size() == 0);
  }
  return bret;
}

int ObIWkbGeogCollection::get_sub(uint32_t idx, ObGeometry*& geo) const
{
  INIT_SUCC(ret);
  ObWkbGeogCollection* g = reinterpret_cast<ObWkbGeogCollection*>(const_cast<char*>(val()));
  if (OB_ISNULL(this->allocator_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("Null allocator.", K(ret));
  } else if (idx >= g->size()) {
    ret = OB_ERR_INTERVAL_INVALID;
    LOG_WARN("Invalid index.", K(ret), K(idx), K(g->size()));
  } else {
    ObWkbGeogCollection::iterator iter = g->begin();
    iter += idx;
    const char* ptr = iter.operator->();
    uint64_t size = g->get_sub_size(ptr);
    // TODO  use create_obj_by_type
    switch (g->get_sub_type(ptr)) {
      case ObGeoType::POINT: {
        geo = OB_NEWx(ObIWkbGeomPoint, this->allocator_);
        break;
      }
      default: {
        ret = OB_ERR_INTERVAL_INVALID;
        break;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(geo)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Alloc sub geo falied.", K(ret));
      } else if (size <= WKB_COMMON_WKB_HEADER_LEN) {
        ret = OB_ERR_INTERVAL_INVALID;
        LOG_WARN("get sub geo size falied.", K(ret), K(size));
      } else {
        ObString sub_wkb(size, ptr);
        geo->set_data(sub_wkb);
      }
    }
  }
  return ret;
}

int ObIWkbGeogPoint::do_visit(ObIGeoVisitor &visitor)
{
  return visitor.visit(this);
}

int ObIWkbGeogLineString::do_visit(ObIGeoVisitor &visitor)
{
  return ObIWkbVisitorImplement::linestring_do_visitor<ObIWkbGeogLineString,
    ObWkbGeogLineString, ObIWkbGeogPoint, ObWkbGeogPoint>(this, visitor);
}

int ObIWkbGeogLinearRing::do_visit(ObIGeoVisitor &visitor)
{
  return ObIWkbVisitorImplement::linestring_do_visitor<ObIWkbGeogLinearRing,
    ObWkbGeogLinearRing, ObIWkbGeogPoint, ObWkbGeogPoint>(this, visitor);
}

int ObIWkbGeogPolygon::do_visit(ObIGeoVisitor &visitor)
{
  return ObIWkbVisitorImplement::polygon_do_visitor<ObIWkbGeogPolygon,
    ObWkbGeogPolygon, ObIWkbGeogLinearRing, ObWkbGeogLinearRing, ObWkbGeogPolygonInnerRings>(this, visitor);
}

int ObIWkbGeogMultiPoint::do_visit(ObIGeoVisitor &visitor)
{
  return ObIWkbVisitorImplement::linestring_do_visitor<ObIWkbGeogMultiPoint,
    ObWkbGeogMultiPoint, ObIWkbGeogPoint, ObWkbGeogPoint>(this, visitor);
}

int ObIWkbGeogMultiLineString::do_visit(ObIGeoVisitor &visitor)
{
  return ObIWkbVisitorImplement::collection_do_visitor<ObIWkbGeogMultiLineString,
    ObWkbGeogMultiLineString, ObIWkbGeogLineString, ObWkbGeogLineString>(this, visitor);
}

int ObIWkbGeogMultiPolygon::do_visit(ObIGeoVisitor &visitor)
{
  return ObIWkbVisitorImplement::collection_do_visitor<ObIWkbGeogMultiPolygon,
    ObWkbGeogMultiPolygon, ObIWkbGeogPolygon, ObWkbGeogPolygon>(this, visitor);
}

int ObIWkbGeogCollection::do_visit(ObIGeoVisitor &visitor)
{
  INIT_SUCC(ret);
  if (visitor.prepare(this)) {
    if (OB_FAIL(visitor.visit(this))) {
      LOG_WARN("failed to do wkb geog multi polygon visit", K(ret));
    } else if (visitor.is_end(this) || is_empty_inner()) {
      // do nothing
    } else {
      const ObWkbGeogCollection *collection = reinterpret_cast<const ObWkbGeogCollection*>(val());
      ObWkbGeogCollection::iterator iter = collection->begin();
      uint64_t total_len = data_.length();
      uint64_t pos = WKB_COMMON_WKB_HEADER_LEN;
      for ( ; iter != collection->end() && OB_SUCC(ret) && !visitor.is_end(this); ++iter) {
        typename ObWkbGeogCollection::const_pointer sub_ptr = iter.operator->();
         if (pos + WKB_GEO_TYPE_SIZE + WKB_GEO_BO_SIZE > total_len) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid wkb geo", K(ret), K(data_.length()), K(val()));
         } else {
          ObGeoType sub_type = collection->get_sub_type(sub_ptr);
          switch (sub_type) {
            case ObGeoType::POINT : {
              const ObWkbGeogPoint* point = reinterpret_cast<const ObWkbGeogPoint*>(sub_ptr);
              ObIWkbGeogPoint i_point;
              ObString data(total_len - pos, reinterpret_cast<const char *>(point));
              i_point.set_data(data);
              if (OB_FAIL(i_point.do_visit(visitor))) {
                LOG_WARN("failed to do wkb collection_point visit", K(ret));
              } else {
                pos += i_point.length();
              }
              break;
            }
            case ObGeoType::LINESTRING : {
              const ObWkbGeogLineString* line = reinterpret_cast<const ObWkbGeogLineString*>(sub_ptr);
              ObIWkbGeogLineString i_line;
              ObString data(total_len - pos, reinterpret_cast<const char *>(line));
              i_line.set_data(data);
              if (OB_FAIL(i_line.do_visit(visitor))) {
                LOG_WARN("failed to do wkb collection_line visit", K(ret));
              } else {
                pos += i_line.length();
              }
              break;
            }
            case ObGeoType::POLYGON : {
              const ObWkbGeogPolygon* poly = reinterpret_cast<const ObWkbGeogPolygon*>(sub_ptr);
              ObIWkbGeogPolygon polygon;
              ObString data(total_len - pos, reinterpret_cast<const char *>(poly));
              polygon.set_data(data);
              if (OB_FAIL(polygon.do_visit(visitor))) {
                LOG_WARN("failed to do wkb geog collection_polygon visit", K(ret));
              } else {
                pos += polygon.length();
              }
              break;
            }
            case ObGeoType::MULTIPOINT : {
              const ObWkbGeogMultiPoint* mp = reinterpret_cast<const ObWkbGeogMultiPoint*>(sub_ptr);
              ObIWkbGeogMultiPoint multi_points;
              ObString data(total_len - pos, reinterpret_cast<const char *>(mp));
              multi_points.set_data(data);
              if (OB_FAIL(multi_points.do_visit(visitor))) {
                LOG_WARN("failed to do wkb geog collection_multi_points visit", K(ret));
              } else {
                pos += multi_points.length();
              }
              break;
            }
            case ObGeoType::MULTILINESTRING : {
              const ObWkbGeogMultiLineString* ml = reinterpret_cast<const ObWkbGeogMultiLineString*>(sub_ptr);
              ObIWkbGeogMultiLineString multi_lines;
              ObString data(total_len - pos, reinterpret_cast<const char *>(ml));
              multi_lines.set_data(data);
              if (OB_FAIL(multi_lines.do_visit(visitor))) {
                LOG_WARN("failed to do wkb geog collection_multi_lines visit", K(ret));
              } else {
                pos += multi_lines.length();
              }
              break;
            }
            case ObGeoType::MULTIPOLYGON : {
              const ObWkbGeogMultiPolygon* mp = reinterpret_cast<const ObWkbGeogMultiPolygon*>(sub_ptr);
              ObIWkbGeogMultiPolygon multi_polygons;
              ObString data(total_len - pos, reinterpret_cast<const char *>(mp));
              multi_polygons.set_data(data);
              if (OB_FAIL(multi_polygons.do_visit(visitor))) {
                LOG_WARN("failed to do wkb geog collection_multi_polygons visit", K(ret));
              } else {
                pos += multi_polygons.length();
              }
              break;
            }
            case ObGeoType::GEOMETRYCOLLECTION : {
              const ObWkbGeogCollection* subgc = reinterpret_cast<const ObWkbGeogCollection*>(sub_ptr);
              ObIWkbGeogCollection sub_collection;
              ObString data(total_len - pos, reinterpret_cast<const char *>(subgc));
              sub_collection.set_data(data);
              if (OB_FAIL(sub_collection.do_visit(visitor))) {
                LOG_WARN("failed to do wkb geog sub collection visit", K(ret));
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
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(visitor.finish(this))) {
      LOG_WARN("failed to finish visit", K(ret));
    }
  }
  return ret;
}


int ObIWkbGeomPoint::do_visit(ObIGeoVisitor &visitor)
{
  return visitor.visit(this);
}

int ObIWkbGeomLineString::do_visit(ObIGeoVisitor &visitor)
{
  return ObIWkbVisitorImplement::linestring_do_visitor<ObIWkbGeomLineString,
    ObWkbGeomLineString, ObIWkbGeomPoint, ObWkbGeomPoint>(this, visitor);
}

int ObIWkbGeomLinearRing::do_visit(ObIGeoVisitor &visitor)
{
  return ObIWkbVisitorImplement::linestring_do_visitor<ObIWkbGeomLinearRing,
    ObWkbGeomLinearRing, ObIWkbGeomPoint, ObWkbGeomPoint>(this, visitor);
}

int ObIWkbGeomPolygon::do_visit(ObIGeoVisitor &visitor)
{
  return ObIWkbVisitorImplement::polygon_do_visitor<ObIWkbGeomPolygon,
    ObWkbGeomPolygon, ObIWkbGeomLinearRing, ObWkbGeomLinearRing, ObWkbGeomPolygonInnerRings>(this, visitor);
}

int ObIWkbGeomMultiPoint::do_visit(ObIGeoVisitor &visitor)
{
  return ObIWkbVisitorImplement::linestring_do_visitor<ObIWkbGeomMultiPoint,
    ObWkbGeomMultiPoint, ObIWkbGeomPoint, ObWkbGeomPoint>(this, visitor);
}

int ObIWkbGeomMultiLineString::do_visit(ObIGeoVisitor &visitor)
{
  return ObIWkbVisitorImplement::collection_do_visitor<ObIWkbGeomMultiLineString,
    ObWkbGeomMultiLineString, ObIWkbGeomLineString, ObWkbGeomLineString>(this, visitor);
}

int ObIWkbGeomMultiPolygon::do_visit(ObIGeoVisitor &visitor)
{
  return ObIWkbVisitorImplement::collection_do_visitor<ObIWkbGeomMultiPolygon,
    ObWkbGeomMultiPolygon, ObIWkbGeomPolygon, ObWkbGeomPolygon>(this, visitor);
}

int ObIWkbGeomCollection::do_visit(ObIGeoVisitor &visitor)
{
  INIT_SUCC(ret);
  if (visitor.prepare(this)) {
    if (OB_FAIL(visitor.visit(this))) {
      LOG_WARN("failed to do wkb geom collection visit", K(ret));
    } else if (visitor.is_end(this) || is_empty_inner()) {
      // do nothing
    } else {
      const ObWkbGeomCollection *collection = reinterpret_cast<const ObWkbGeomCollection*>(val());
      ObWkbGeomCollection::iterator iter = collection->begin();
      uint64_t total_len = data_.length();
      uint64_t pos = WKB_COMMON_WKB_HEADER_LEN;
      for ( ; iter != collection->end() && OB_SUCC(ret) && !visitor.is_end(this); ++iter) {
        typename ObWkbGeomCollection::const_pointer sub_ptr = iter.operator->();
        if (pos + WKB_GEO_TYPE_SIZE + WKB_GEO_BO_SIZE > total_len) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid wkb geo", K(ret), K(total_len), K(pos), K(val()));
        } else {
          ObGeoType sub_type = collection->get_sub_type(sub_ptr);
          switch (sub_type) {
              case ObGeoType::POINT : {
              const ObWkbGeomPoint* point = reinterpret_cast<const ObWkbGeomPoint*>(sub_ptr);
              ObIWkbGeomPoint i_point;
              ObString data(total_len - pos, reinterpret_cast<const char *>(point));
              i_point.set_data(data);
              if (OB_FAIL(i_point.do_visit(visitor))) {
                LOG_WARN("failed to do wkb collection_point visit", K(ret));
              } else {
                pos += i_point.length();
              }
              break;
            }
            case ObGeoType::LINESTRING : {
              const ObWkbGeomLineString* line = reinterpret_cast<const ObWkbGeomLineString*>(sub_ptr);
              ObIWkbGeomLineString i_line;
              ObString data(total_len - pos, reinterpret_cast<const char *>(line));
              i_line.set_data(data);
              if (OB_FAIL(i_line.do_visit(visitor))) {
                LOG_WARN("failed to do wkb collection_line visit", K(ret));
              } else {
                pos += i_line.length();
              }
              break;
            }
            case ObGeoType::POLYGON : {
              const ObWkbGeomPolygon* poly = reinterpret_cast<const ObWkbGeomPolygon*>(sub_ptr);
              ObIWkbGeomPolygon polygon;
              ObString data(total_len - pos, reinterpret_cast<const char *>(poly));
              polygon.set_data(data);
              if (OB_FAIL(polygon.do_visit(visitor))) {
                LOG_WARN("failed to do wkb geom collection_polygon visit", K(ret));
              } else {
                pos += polygon.length();
              }
              break;
            }
            case ObGeoType::MULTIPOINT : {
              const ObWkbGeomMultiPoint* mp = reinterpret_cast<const ObWkbGeomMultiPoint*>(sub_ptr);
              ObIWkbGeomMultiPoint multi_points;
              ObString data(total_len - pos, reinterpret_cast<const char *>(mp));
              multi_points.set_data(data);
              if (OB_FAIL(multi_points.do_visit(visitor))) {
                LOG_WARN("failed to do wkb geom collection_multi_points visit", K(ret));
              } else {
                pos += multi_points.length();
              }
              break;
            }
            case ObGeoType::MULTILINESTRING : {
              const ObWkbGeomMultiLineString* ml = reinterpret_cast<const ObWkbGeomMultiLineString*>(sub_ptr);
              ObIWkbGeomMultiLineString multi_lines;
              ObString data(total_len - pos, reinterpret_cast<const char *>(ml));
              multi_lines.set_data(data);
              if (OB_FAIL(multi_lines.do_visit(visitor))) {
                LOG_WARN("failed to do wkb geom collection_multi_lines visit", K(ret));
              } else {
                pos += multi_lines.length();
              }
              break;
            }
            case ObGeoType::MULTIPOLYGON : {
              const ObWkbGeomMultiPolygon* mp = reinterpret_cast<const ObWkbGeomMultiPolygon*>(sub_ptr);
              ObIWkbGeomMultiPolygon multi_polygons;
              ObString data(total_len - pos, reinterpret_cast<const char *>(mp));
              multi_polygons.set_data(data);
              if (OB_FAIL(multi_polygons.do_visit(visitor))) {
                LOG_WARN("failed to do wkb geom collection_multi_polygons visit", K(ret));
              } else {
                pos += multi_polygons.length();
              }
              break;
            }
            case ObGeoType::GEOMETRYCOLLECTION : {
              const ObWkbGeomCollection* subgc = reinterpret_cast<const ObWkbGeomCollection*>(sub_ptr);
              ObIWkbGeomCollection sub_collection;
              ObString data(total_len - pos, reinterpret_cast<const char *>(subgc));
              sub_collection.set_data(data);
              if (OB_FAIL(sub_collection.do_visit(visitor))) {
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
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(visitor.finish(this))) {
      LOG_WARN("failed to finish visit", K(ret));
    }
  }
  return ret;
}

} // namespace common
} // namespace oceanbase
