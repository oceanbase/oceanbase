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

#include "lib/ob_errno.h"
#include <cstdint>
#define USING_LOG_PREFIX LIB
#include "ob_wkb_to_json_bin_visitor.h"
#include "lib/geo/ob_geo_func_register.h"
#include "lib/geo/ob_wkb_to_json_visitor.h"

namespace oceanbase {
namespace common {

enum TypeNameMap
{
  Point = 0,
  MultiPoint = 1,
  LineString = 2,
  MultiLineString = 3,
  Polygon = 4,
  MultiPolygon = 5,
  GeometryCollection = 6
};
const ObString type_name_table[]
{
  "Point",
  "MultiPoint",
  "LineString",
  "MultiLineString",
  "Polygon",
  "MultiPolygon",
  "GeometryCollection"
};
enum KeyNameMap
{
  crs = 0,
  bbox = 1,
  type = 2,
  geometries = 3,
  coordinates = 4,
  properties = 5,
  name = 6
};
const ObString key_name_table[]
{
  "crs",
  "bbox",
  "type",
  "geometries",
  "coordinates",
  "properties",
  "name"
};
ObWkbToJsonBinVisitor::ObWkbToJsonBinVisitor(
    ObIAllocator *allocator,
    uint32_t max_dec_digits,
    uint8_t flag,
    const ObGeoSrid srid)
    : json_buf_(allocator),
      flag_(flag),
      srid_(srid),
      allocator_(allocator),
      max_dec_digits_(max_dec_digits),
      is_appendCrs(false)
{
}

int ObWkbToJsonBinVisitor::visit(ObIWkbGeogPoint *geo)
{
  INIT_SUCC(ret);
  if (OB_FAIL(appendPointObj<ObIWkbGeogPoint>(geo, type_name_table[TypeNameMap::Point]))) {
    LOG_WARN("fail to append point", K(ret));
  }
  return ret;
}

int ObWkbToJsonBinVisitor::visit(ObIWkbGeomPoint *geo)
{
  INIT_SUCC(ret);
  ObString type_name(type_name_table[0]); //"Point"
  if (OB_FAIL(appendPointObj<ObIWkbGeomPoint>(geo, type_name))) {
    LOG_WARN("fail to append point", K(ret));
  }
  return ret;
}

int ObWkbToJsonBinVisitor::visit(ObIWkbGeogMultiPoint *geo)
{
  INIT_SUCC(ret);
  if (OB_FAIL((appendLineObj<ObIWkbGeogMultiPoint, ObWkbGeogMultiPoint>(geo, type_name_table[TypeNameMap::MultiPoint])))) {
    LOG_WARN("fail to append line", K(ret));
  }
  return ret;
}

int ObWkbToJsonBinVisitor::visit(ObIWkbGeomMultiPoint *geo)
{
  INIT_SUCC(ret);
  if (OB_FAIL((appendLineObj<ObIWkbGeomMultiPoint, ObWkbGeomMultiPoint>(geo, type_name_table[TypeNameMap::MultiPoint])))) {
    LOG_WARN("fail to append line", K(ret));
  }
  return ret;
}

int ObWkbToJsonBinVisitor::visit(ObIWkbGeogLineString *geo)
{
  INIT_SUCC(ret);
  // ObString type_name(type_name_table[2]); //"LineString"
  if (OB_FAIL((appendLineObj<ObIWkbGeogLineString, ObWkbGeogLineString>(geo, type_name_table[TypeNameMap::LineString])))) {
    LOG_WARN("fail to append line", K(ret));
  }
  return ret;
}

int ObWkbToJsonBinVisitor::visit(ObIWkbGeomLineString *geo)
{
  INIT_SUCC(ret);
  if (OB_FAIL((appendLineObj<ObIWkbGeomLineString, ObWkbGeomLineString>(geo, type_name_table[TypeNameMap::LineString])))) {
    LOG_WARN("fail to append line", K(ret));
  }
  return ret;
}

int ObWkbToJsonBinVisitor::visit(ObIWkbGeogMultiLineString *geo)
{
  INIT_SUCC(ret);
  if (OB_FAIL((appendMultiLineObj<
                          ObIWkbGeogMultiLineString,
                          ObWkbGeogMultiLineString,
                          ObWkbGeogLineString>(geo, type_name_table[TypeNameMap::MultiLineString])))) {
    LOG_WARN("fail to append line", K(ret));
  }
  return ret;
}

int ObWkbToJsonBinVisitor::visit(ObIWkbGeomMultiLineString *geo)
{
  INIT_SUCC(ret);
  if (OB_FAIL((appendMultiLineObj<
                          ObIWkbGeomMultiLineString,
                          ObWkbGeomMultiLineString,
                          ObWkbGeomLineString>(geo, type_name_table[TypeNameMap::MultiLineString])))) {
    LOG_WARN("fail to append line", K(ret));
  }
  return ret;
}

int ObWkbToJsonBinVisitor::visit(ObIWkbGeogPolygon *geo)
{
  INIT_SUCC(ret);
  if (OB_FAIL((appendPolygonObj<
                        ObIWkbGeogPolygon,
                        ObWkbGeogPolygon,
                        ObWkbGeogLinearRing,
                        ObWkbGeogPolygonInnerRings>(geo, type_name_table[TypeNameMap::Polygon])))) {
    LOG_WARN("fail to append line", K(ret));
  }
  return ret;
}

int ObWkbToJsonBinVisitor::visit(ObIWkbGeomPolygon *geo)
{
  INIT_SUCC(ret);
  if (OB_FAIL((appendPolygonObj<
                        ObIWkbGeomPolygon,
                        ObWkbGeomPolygon,
                        ObWkbGeomLinearRing,
                        ObWkbGeomPolygonInnerRings>(geo, type_name_table[TypeNameMap::Polygon])))) {
    LOG_WARN("fail to append line", K(ret));
  }
  return ret;
}

int ObWkbToJsonBinVisitor::visit(ObIWkbGeogMultiPolygon *geo)
{
  INIT_SUCC(ret);
  if (OB_FAIL((appendMultiPolygonObj<
                          ObIWkbGeogMultiPolygon,
                          ObWkbGeogMultiPolygon,
                          ObWkbGeogPolygon,
                          ObWkbGeogLinearRing,
                          ObWkbGeogPolygonInnerRings>(geo, type_name_table[TypeNameMap::MultiPolygon])))) {
    LOG_WARN("fail to append line", K(ret));
  }
  return ret;
}

int ObWkbToJsonBinVisitor::visit(ObIWkbGeomMultiPolygon *geo)
{
  INIT_SUCC(ret);
  if (OB_FAIL((appendMultiPolygonObj<
                          ObIWkbGeomMultiPolygon,
                          ObWkbGeomMultiPolygon,
                          ObWkbGeomPolygon,
                          ObWkbGeomLinearRing,
                          ObWkbGeomPolygonInnerRings>(geo, type_name_table[TypeNameMap::MultiPolygon])))) {
    LOG_WARN("fail to append line", K(ret));
  }
  return ret;
}

int ObWkbToJsonBinVisitor::visit(ObIWkbGeogCollection *geo)
{
  INIT_SUCC(ret);
  if (OB_FAIL((appendCollectionObj<
                  ObIWkbGeogCollection,
                  ObWkbGeogCollection>(geo, type_name_table[TypeNameMap::GeometryCollection])))) {
    LOG_WARN("fail to append collection", K(ret));
  }
  return ret;
}

int ObWkbToJsonBinVisitor::visit(ObIWkbGeomCollection *geo)
{
  INIT_SUCC(ret);
  if (OB_FAIL((appendCollectionObj<
                  ObIWkbGeomCollection,
                  ObWkbGeomCollection>(geo, type_name_table[TypeNameMap::GeometryCollection])))) {
    LOG_WARN("fail to append collection", K(ret));
  }
  return ret;
}

int ObWkbToJsonBinVisitor::to_jsonbin(ObGeometry *geo, ObString &geojsonbin)
{
  INIT_SUCC(ret);
  json_buf_.reset();
  if (OB_FAIL(json_buf_.reserve(geo->length() * 2))) {
    LOG_WARN("fail to reserve json_buf_", K(ret), K(geo->length() * 2));
  } else if (OB_FAIL(ObJsonBin::add_doc_header_v0(json_buf_))) {
    LOG_WARN("fail to add doc header", K(ret));
  } else if (OB_FAIL(geo->do_visit(*this))) {
    LOG_WARN("fail to geo->do_visit", K(ret));
  } else if (OB_FAIL(ObJsonBin::set_doc_header_v0(json_buf_, json_buf_.length(), false/*use_lexicographical_order*/))) {
    LOG_WARN("fail to set doc header", K(ret));
  } else if (OB_FAIL(json_buf_.get_result_string(geojsonbin))) {
    LOG_WARN("fail to get_result_string", K(ret));
  }
  return ret;
}

int ObWkbToJsonBinVisitor:: appendCoordinatePoint(
    double x,
    double y,
    ObJsonBin &bin,
    uint64_t start_pos,
    uint64_t &val_idx)
{
  INIT_SUCC(ret);
  uint64_t value_offset = json_buf_.length() - start_pos;
  uint8_t value_type = static_cast<uint8_t>(ObJsonNodeType::J_ARRAY);
  if (OB_FAIL(bin.set_value_entry(val_idx, value_offset, value_type, false))) {
    LOG_WARN("fail to set value entry", K(ret), K(value_offset), K(value_type));
  } else {
    uint64_t array_pos = 0;
    ObJsonBin array_bin;
    uint64_t array_idx = 0;
    if (OB_FAIL(appendMeta(array_bin, array_pos, 1, 2, 0))) {
      LOG_WARN("fail to appendMeta", K(ret), K(array_pos));
    } else if (OB_FAIL(appendDouble(x, array_bin, array_pos, array_idx))) {
      LOG_WARN("fail to appendDouble", K(ret), K(x), K(array_pos), K(array_idx));
    } else if (OB_FAIL(appendDouble(y, array_bin, array_pos, array_idx))) {
      LOG_WARN("fail to appendDouble", K(ret), K(y), K(array_pos), K(array_idx));
    } else if (OB_FAIL(fillHeaderSize(array_bin, array_pos))) {
      LOG_WARN("fail to fillHeaderSize", K(ret), K(array_pos));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(bin.set_current(json_buf_.string(), start_pos))) {
    LOG_WARN("fail to set_current", K(ret), K(start_pos));
  } else {
    val_idx++;
  }
  return ret;
}

template<typename T_IBIN>
int ObWkbToJsonBinVisitor::appendPointObj(T_IBIN *geo, const ObString &type_name)
{
  // { "type": "Point", "coordinates": [x, y] }
  INIT_SUCC(ret);
  ObJsonBin bin;
  uint64_t start_pos = 0;
  uint64_t val_idx = 0;
  if (OB_FAIL(appendJsonCommon(type_name, geo, bin, start_pos, val_idx))) {
    LOG_WARN("fail to appendJsonCommon", K(ret), K(type_name));
  } else if (OB_FAIL(appendCoordinatePoint(geo->x(), geo->y(), bin, start_pos, val_idx))) {
    LOG_WARN("fail to appendCoordinatePoint", K(ret), K(geo->x()), K(geo->y()));
  } else if (OB_FAIL(fillHeaderSize(bin, start_pos))) {
    LOG_WARN("fail to fillHeaderSize", K(ret));
  }
  return ret;
}

template<typename T_BIN>
int ObWkbToJsonBinVisitor::appendLine(const T_BIN *line, ObJsonBin &bin, uint64_t &start_pos, uint64_t &val_idx)
{
  INIT_SUCC(ret);
  typename T_BIN::iterator iter = line->begin();
  uint64_t line_size = line->iter_idx_max();
  ObJsonBin line_bin;
  uint64_t line_val_idx = 0;
  uint64_t line_start_pos = 0;
  if (OB_FAIL(appendArrayHeader(bin, start_pos, val_idx, line_size, line_bin, line_start_pos))) {
    LOG_WARN("fail to appendArrayHeader", K(ret), K(start_pos), K(val_idx));
  } else {
    for (; OB_SUCC(ret) && iter != line->end(); iter++) {
      if(OB_FAIL(appendCoordinatePoint(
                    iter->template get<0>(),
                    iter->template get<1>(),
                    line_bin,
                    line_start_pos,
                    line_val_idx))) {
        LOG_WARN("fail to appendCoordinatePoint", K(ret), K(iter->template get<0>()), K(iter->template get<1>()));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(fillHeaderSize(line_bin, line_start_pos))) {
      LOG_WARN("fail to fillHeaderSize", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(bin.set_current(json_buf_.string(), start_pos))) {
    LOG_WARN("fail to set_current", K(ret), K(start_pos));
  } else {
    val_idx++;
  }
  
  return ret;
}

template<typename T_IBIN, typename T_BIN>
int ObWkbToJsonBinVisitor::appendLineObj(T_IBIN *geo, const ObString &type_name)
{
  INIT_SUCC(ret);
  ObJsonBin bin;
  uint64_t start_pos = 0;
  uint64_t val_idx = 0;
  const T_BIN *line = reinterpret_cast<const T_BIN *>(geo->val());
  if (OB_FAIL(appendJsonCommon(type_name, geo, bin, start_pos, val_idx))) {
    LOG_WARN("fail to appendJsonCommon", K(ret), K(type_name));
  } else if (OB_FAIL(appendLine<T_BIN>(line, bin, start_pos, val_idx))) {
    LOG_WARN("fail to appendLine", K(ret), K(start_pos), K(val_idx));
  } else if (OB_FAIL(fillHeaderSize(bin, start_pos))) {
    LOG_WARN("fail to fillHeaderSize", K(ret));
  }
  return ret;
}

template<typename T_IBIN, typename T_BIN, typename T_BIN_LINE>
int ObWkbToJsonBinVisitor::appendMultiLineObj(T_IBIN *geo, const ObString &type_name)
{
  INIT_SUCC(ret);
  ObJsonBin bin;
  uint64_t start_pos = 0;
  uint64_t val_idx = 0;
  if (OB_FAIL(appendJsonCommon(type_name, geo, bin, start_pos, val_idx))) {
    LOG_WARN("fail to appendJsonCommon", K(ret), K(type_name));
  } else {
    const T_BIN *multi_line = reinterpret_cast<const T_BIN *>(geo->val());
    typename T_BIN::iterator line = multi_line->begin();
    uint64_t multi_size = multi_line->iter_idx_max();
    ObJsonBin multi_bin;
    uint64_t multi_val_idx = 0;
    uint64_t multi_start_pos = 0;
    if (OB_FAIL(appendArrayHeader(bin, start_pos, val_idx, multi_size, multi_bin, multi_start_pos))) {
      LOG_WARN("fail to appendArrayHeader", K(ret), K(start_pos), K(val_idx));
    } else {
      for (; OB_SUCC(ret) && line != multi_line->end(); line++) {
        if (OB_FAIL(appendLine<T_BIN_LINE>(&(*line), multi_bin, multi_start_pos, multi_val_idx))) {
          LOG_WARN("fail to appendLine", K(ret), K(multi_start_pos), K(multi_val_idx));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(fillHeaderSize(multi_bin, multi_start_pos))) {
        LOG_WARN("fail to fillHeaderSize", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(bin.set_current(json_buf_.string(), start_pos))) {
      LOG_WARN("fail to set_current", K(ret), K(start_pos));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(fillHeaderSize(bin, start_pos))) {
    LOG_WARN("fail to fillHeaderSize", K(ret));
  }
  return ret;
}

template<typename T_BIN, typename T_BIN_RING, typename T_BIN_INNER_RING>
int ObWkbToJsonBinVisitor::appendPolygon(const T_BIN *poly, ObJsonBin &bin, uint64_t &start_pos, uint64_t &val_idx)
{
  INIT_SUCC(ret);
  uint64_t poly_size = poly->size();
  const T_BIN_RING& exterior = poly->exterior_ring();
  const T_BIN_INNER_RING &inner_rings = poly->inner_rings();
  ObJsonBin poly_bin;
  uint64_t poly_val_idx = 0;
  uint64_t poly_start_pos = 0;
  if (OB_FAIL(appendArrayHeader(bin, start_pos, val_idx, poly_size, poly_bin, poly_start_pos))) {
    LOG_WARN("fail to appendArrayHeader", K(ret), K(start_pos), K(val_idx));
  } else {
    if (poly_size > 0) {
      // exterior poly
      if (OB_FAIL(appendLine<T_BIN_RING>(&exterior, poly_bin, poly_start_pos, poly_val_idx))) {
        LOG_WARN("fail to appendLine", K(ret), K(poly_start_pos), K(poly_val_idx));
      }
    }
    // interior poly
    typename T_BIN_INNER_RING::iterator iterInnerRing = inner_rings.begin();
    for (; OB_SUCC(ret) && iterInnerRing != inner_rings.end(); iterInnerRing++) {
      if (OB_FAIL(appendLine<T_BIN_RING>(&(*iterInnerRing), poly_bin, poly_start_pos, poly_val_idx))) {
        LOG_WARN("fail to appendLine", K(ret), K(poly_start_pos), K(poly_val_idx));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(fillHeaderSize(poly_bin, poly_start_pos))) {
      LOG_WARN("fail to fillHeaderSize", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(bin.set_current(json_buf_.string(), start_pos))) {
    LOG_WARN("fail to set_current", K(ret), K(start_pos));
  } else {
    val_idx++;
  }

  return ret;
}

template<typename T_IBIN, typename T_BIN, typename T_BIN_RING, typename T_BIN_INNER_RING>
int ObWkbToJsonBinVisitor::appendPolygonObj(T_IBIN *geo, const ObString &type_name)
{
  INIT_SUCC(ret);
  ObJsonBin bin;
  uint64_t start_pos = 0;
  uint64_t val_idx = 0;
  const T_BIN *poly = reinterpret_cast<const T_BIN *>(geo->val());
  if (OB_FAIL(appendJsonCommon(type_name, geo, bin, start_pos, val_idx))) {
    LOG_WARN("fail to appendJsonCommon", K(ret), K(type_name));
  } else if (OB_FAIL((appendPolygon<
                        T_BIN,
                        T_BIN_RING,
                        T_BIN_INNER_RING>(poly, bin, start_pos, val_idx)))) {
    LOG_WARN("appendPolygon fail", K(ret), K(start_pos), K(val_idx));
  } else if (OB_FAIL(fillHeaderSize(bin, start_pos))) {
    LOG_WARN("fail to fillHeaderSize", K(ret));
  }
  return ret;
}

template<typename T_IBIN,
         typename T_BIN,
         typename T_BIN_POLY,
         typename T_BIN_RING,
         typename T_BIN_INNER_RING>
int ObWkbToJsonBinVisitor::appendMultiPolygonObj(T_IBIN *geo, const ObString &type_name)
{
  INIT_SUCC(ret);
  ObJsonBin bin;
  uint64_t start_pos = 0;
  uint64_t val_idx = 0;
  if (OB_FAIL(appendJsonCommon(type_name, geo, bin, start_pos, val_idx))) {
    LOG_WARN("fail to appendJsonCommon", K(ret), K(type_name));
  } else {
    const T_BIN *multi_poly = reinterpret_cast<const T_BIN *>(geo->val());
    typename T_BIN::iterator poly = multi_poly->begin();
    uint64_t multi_size = multi_poly->iter_idx_max();
    ObJsonBin multi_bin;
    uint64_t multi_val_idx = 0;
    uint64_t multi_start_pos = 0;
    if (OB_FAIL(appendArrayHeader(bin, start_pos, val_idx, multi_size, multi_bin, multi_start_pos))) {
      LOG_WARN("fail to appendArrayHeader", K(ret), K(start_pos), K(val_idx));
    } else {
      for (; OB_SUCC(ret) && poly != multi_poly->end(); poly++) {
        if (OB_FAIL((appendPolygon<T_BIN_POLY, T_BIN_RING, T_BIN_INNER_RING>(&(*poly), multi_bin, multi_start_pos, multi_val_idx)))) {
          LOG_WARN("fail to appendPolygon", K(ret), K(multi_start_pos), K(multi_val_idx));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(fillHeaderSize(multi_bin, multi_start_pos))) {
        LOG_WARN("fail to fillHeaderSize", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(bin.set_current(json_buf_.string(), start_pos))) {
      LOG_WARN("fail to set_current", K(ret), K(start_pos));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(fillHeaderSize(bin, start_pos))) {
    LOG_WARN("fail to fillHeaderSize", K(ret));
  }
  return ret;
}

template<typename T_IPOINT,
          typename T_IMULTIPOINT,
          typename T_ILINE,
          typename T_IMULTILINE,
          typename T_IPOLY,
          typename T_IMULTIPOLY, 
          typename T_ICOLLC,
          typename T_POINT,
          typename T_MULTIPOINT,
          typename T_LINE,
          typename T_MULTILINE,
          typename T_POLY,
          typename T_LINEARRING,
          typename T_INNERRING,
          typename T_MULTIPOLY,
          typename T_COLLC>
int ObWkbToJsonBinVisitor::appendCollectionSub(
    typename T_COLLC::const_pointer sub_ptr,
    const T_COLLC *collection,
    ObJsonBin &bin,
    uint64_t &start_pos,
    uint64_t &val_idx)
{
  INIT_SUCC(ret);
  ObGeoType sub_type = collection->get_sub_type(sub_ptr);
  uint64_t value_offset = json_buf_.length() - start_pos;
  uint8_t value_type = static_cast<uint8_t>(ObJsonNodeType::J_OBJECT);
  if (OB_FAIL(bin.set_value_entry(val_idx, value_offset, value_type, false))) {
    LOG_WARN("fail to set value entry", K(ret), K(value_offset), K(value_type));
  } else {
    switch (sub_type) {
      case ObGeoType::POINT : {
        const T_POINT* geo = reinterpret_cast<const T_POINT*>(sub_ptr);
        T_IPOINT igeo;
        ObString data(sizeof(T_POINT), reinterpret_cast<const char *>(geo));
        igeo.set_data(data);
        if (OB_FAIL(appendPointObj<T_IPOINT>(&igeo, type_name_table[TypeNameMap::Point]))) {
          LOG_WARN("fail to append point", K(ret));
        }
        break;
      }
      case ObGeoType::MULTIPOINT : {
        const T_MULTIPOINT* geo = reinterpret_cast<const T_MULTIPOINT*>(sub_ptr);
        T_IMULTIPOINT igeo;
        ObString data(sizeof(T_MULTIPOINT), reinterpret_cast<const char *>(geo));
        igeo.set_data(data);
        if (OB_FAIL((appendLineObj<T_IMULTIPOINT, T_MULTIPOINT>(&igeo, type_name_table[TypeNameMap::MultiPoint])))) {
          LOG_WARN("fail to append multi_point", K(ret));
        }
        break;
      }
      case ObGeoType::LINESTRING : {
        const T_LINE* geo = reinterpret_cast<const T_LINE*>(sub_ptr);
        T_ILINE igeo;
        ObString data(sizeof(T_LINE), reinterpret_cast<const char *>(geo));
        igeo.set_data(data);
        if (OB_FAIL((appendLineObj<T_ILINE, T_LINE>(&igeo, type_name_table[TypeNameMap::LineString])))) {
          LOG_WARN("fail to append line", K(ret));
        }
        break;
      }
      case ObGeoType::MULTILINESTRING : {
        const T_MULTILINE* geo = reinterpret_cast<const T_MULTILINE*>(sub_ptr);
        T_IMULTILINE igeo;
        ObString data(sizeof(T_MULTILINE), reinterpret_cast<const char *>(geo));
        igeo.set_data(data);
        if (OB_FAIL((appendMultiLineObj<
                        T_IMULTILINE,
                        T_MULTILINE,
                        T_LINE>(&igeo, type_name_table[TypeNameMap::MultiLineString])))) {
          LOG_WARN("fail to append multi_line", K(ret));
        }
        break;
      }
      case ObGeoType::POLYGON : {
        const T_POLY* geo = reinterpret_cast<const T_POLY*>(sub_ptr);
        T_IPOLY igeo;
        ObString data(sizeof(T_POLY), reinterpret_cast<const char *>(geo));
        igeo.set_data(data);
        if (OB_FAIL((appendPolygonObj<
                        T_IPOLY,
                        T_POLY,
                        T_LINEARRING,
                        T_INNERRING>(&igeo, type_name_table[TypeNameMap::Polygon])))) {
          LOG_WARN("fail to append polygon", K(ret));
        }
        break;
      }
      case ObGeoType::MULTIPOLYGON : {
        const T_MULTIPOLY* geo = reinterpret_cast<const T_MULTIPOLY*>(sub_ptr);
        T_IMULTIPOLY igeo;
        ObString data(sizeof(T_MULTIPOLY), reinterpret_cast<const char *>(geo));
        igeo.set_data(data);
        if (OB_FAIL((appendMultiPolygonObj<
                        T_IMULTIPOLY,
                        T_MULTIPOLY,
                        T_POLY,
                        T_LINEARRING,
                        T_INNERRING>(&igeo, type_name_table[TypeNameMap::MultiPolygon])))) {
          LOG_WARN("fail to append multi_polygon", K(ret));
        }
        break;
      }
      case ObGeoType::GEOMETRYCOLLECTION : {
        const T_COLLC* geo = reinterpret_cast<const T_COLLC*>(sub_ptr);
        T_ICOLLC igeo;
        ObString data(sizeof(T_COLLC), reinterpret_cast<const char *>(geo));
        igeo.set_data(data);
        if (OB_FAIL((appendCollectionObj<T_ICOLLC, T_COLLC>(&igeo, type_name_table[TypeNameMap::GeometryCollection])))) {
          LOG_WARN("fail to append collection", K(ret));
        }
        break;
      }
      default : {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid geo type", K(ret), K(sub_type));
        break;
      }
    } // switch end
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(bin.set_current(json_buf_.string(), start_pos))) {
    LOG_WARN("fail to set_current", K(ret), K(start_pos));
  } else {
    val_idx++;
  }
  return ret;
}

int ObWkbToJsonBinVisitor::appendCollectionSubWrapper(
      ObWkbGeogCollection::const_pointer sub_ptr,
      const ObWkbGeogCollection *collection,
      ObJsonBin &bin,
      uint64_t &start_pos,
      uint64_t &val_idx)
{
  INIT_SUCC(ret);
  if (OB_FAIL((appendCollectionSub<
                ObIWkbGeogPoint,
                ObIWkbGeogMultiPoint,
                ObIWkbGeogLineString,
                ObIWkbGeogMultiLineString,
                ObIWkbGeogPolygon,
                ObIWkbGeogMultiPolygon,
                ObIWkbGeogCollection,
                ObWkbGeogPoint,
                ObWkbGeogMultiPoint,
                ObWkbGeogLineString,
                ObWkbGeogMultiLineString,
                ObWkbGeogPolygon,
                ObWkbGeogLinearRing,
                ObWkbGeogPolygonInnerRings,
                ObWkbGeogMultiPolygon,
                ObWkbGeogCollection>(
                    sub_ptr,
                    collection,
                    bin,
                    start_pos,
                    val_idx)))) {
    LOG_WARN("fail to appendJsonCommon", K(ret));
    }
  return ret;
}

int ObWkbToJsonBinVisitor::appendCollectionSubWrapper(
      ObWkbGeomCollection::const_pointer sub_ptr,
      const ObWkbGeomCollection *collection,
      ObJsonBin &bin,
      uint64_t &start_pos,
      uint64_t &val_idx)
{
  INIT_SUCC(ret);
  if (OB_FAIL((appendCollectionSub<
                ObIWkbGeomPoint,
                ObIWkbGeomMultiPoint,
                ObIWkbGeomLineString,
                ObIWkbGeomMultiLineString,
                ObIWkbGeomPolygon,
                ObIWkbGeomMultiPolygon,
                ObIWkbGeomCollection,
                ObWkbGeomPoint,
                ObWkbGeomMultiPoint,
                ObWkbGeomLineString,
                ObWkbGeomMultiLineString,
                ObWkbGeomPolygon,
                ObWkbGeomLinearRing,
                ObWkbGeomPolygonInnerRings,
                ObWkbGeomMultiPolygon,
                ObWkbGeomCollection>(
                    sub_ptr,
                    collection,
                    bin,
                    start_pos,
                    val_idx)))) {
    LOG_WARN("fail to appendJsonCommon", K(ret));
    }
  return ret;
}

template<typename T_IBIN, typename T_BIN>
int ObWkbToJsonBinVisitor::appendCollectionObj(T_IBIN *geo, const ObString &type_name)
{
  INIT_SUCC(ret);
  ObJsonBin bin;
  uint64_t start_pos = 0;
  uint64_t val_idx = 0;
  if (OB_FAIL(appendJsonCommon(type_name, geo, bin, start_pos, val_idx))) {
    LOG_WARN("fail to appendJsonCommon", K(ret), K(type_name));
  } else {
    const T_BIN *collection = reinterpret_cast<const T_BIN *>(geo->val());
    typename T_BIN::iterator collection_iter = collection->begin();
    uint64_t collection_size = collection->iter_idx_max();
    ObJsonBin collection_bin;
    uint64_t collection_val_idx = 0;
    uint64_t collection_start_pos = 0;
    if (OB_FAIL(appendArrayHeader(
                    bin,
                    start_pos,
                    val_idx,
                    collection_size,
                    collection_bin,
                    collection_start_pos))) {
      LOG_WARN("fail to appendArrayHeader", K(ret), K(start_pos), K(val_idx));
    } else {
      for (; OB_SUCC(ret) && collection_iter != collection->end(); collection_iter++) {
        typename T_BIN::const_pointer sub_ptr = collection_iter.operator->();
        if (OB_FAIL(appendCollectionSubWrapper(
                        sub_ptr,
                        collection,
                        collection_bin,
                        collection_start_pos,
                        collection_val_idx))) {
          LOG_WARN("fail to appendCollectionSub", K(ret), K(collection_start_pos), K(collection_val_idx));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(fillHeaderSize(collection_bin, collection_start_pos))) {
        LOG_WARN("fail to fillHeaderSize", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(bin.set_current(json_buf_.string(), start_pos))) {
      LOG_WARN("fail to set_current", K(ret), K(start_pos));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(fillHeaderSize(bin, start_pos))) {
    LOG_WARN("fail to fillHeaderSize", K(ret));
  }
  return ret;
}

int ObWkbToJsonBinVisitor::appendJsonCommon(
    const ObString type_name,
    ObGeometry *geo,
    ObJsonBin &bin,
    uint64_t &start_pos,
    uint64_t &val_idx)
{
  INIT_SUCC(ret);
  uint64_t key_count = 2; // minimun count is 2
  int key_idx = 0;
  ObGeoType type = geo->type();
  if (type <= ObGeoType::GEOMETRY || type >= ObGeoType::GEOTYPEMAX) {
    int ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("invalid geo type", K(ret), K(type));
  }
  // append obj meta
  if (OB_FAIL(ret)) {
  } else if (!is_appendCrs && srid_ != 0 
        && ((flag_ & ObGeoJsonFormat::SHORT_SRID) || (flag_ & ObGeoJsonFormat::LONG_SRID))) {
    key_count++;
  }
  if (OB_FAIL(ret)) {
  } else if (flag_ & ObGeoJsonFormat::BBOX && !geo->is_empty()) {
    key_count++;
  }
  // append obj key
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(appendMeta(bin, start_pos, 0, key_count))) {
    LOG_WARN("fail to appendMeta", K(ret), K(start_pos), K(key_count));
  } else if (!is_appendCrs && srid_ != 0 
                && ((flag_ & ObGeoJsonFormat::SHORT_SRID) || (flag_ & ObGeoJsonFormat::LONG_SRID))
                && OB_FAIL(appendObjKey(key_name_table[KeyNameMap::crs], bin, start_pos, key_idx))) {
    LOG_WARN("fail to appendObjKey", K(ret), K(key_name_table[KeyNameMap::crs]), K(start_pos), K(key_idx));
  } else if (flag_ & ObGeoJsonFormat::BBOX && !geo->is_empty()
                && OB_FAIL(appendObjKey(key_name_table[KeyNameMap::bbox], bin, start_pos, key_idx))) {
    LOG_WARN("fail to appendObjKey", K(ret), K(key_name_table[KeyNameMap::bbox]), K(start_pos), K(key_idx));
  } else if (OB_FAIL(appendObjKey(key_name_table[KeyNameMap::type], bin, start_pos, key_idx))) {
    LOG_WARN("fail to appendObjKey", K(ret), K(key_name_table[KeyNameMap::type]), K(start_pos), K(key_idx));
  } else if (geo->type() == ObGeoType::GEOMETRYCOLLECTION
                && OB_FAIL(appendObjKey(key_name_table[KeyNameMap::geometries], bin, start_pos, key_idx))) {
    LOG_WARN("fail to appendObjKey", K(ret), K(key_name_table[KeyNameMap::geometries]), K(start_pos), K(key_idx));
  } else if (geo->type() != ObGeoType::GEOMETRYCOLLECTION
                && OB_FAIL(appendObjKey(key_name_table[KeyNameMap::coordinates], bin, start_pos, key_idx))) {
    LOG_WARN("fail to appendObjKey", K(ret), K(key_name_table[KeyNameMap::coordinates]), K(start_pos), K(key_idx));
  }
  // append obj value
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(appendCrs(bin, start_pos, val_idx))) {
    LOG_WARN("fail to appendCrs", K(ret), K(start_pos), K(val_idx));
  } else if (OB_FAIL(appendBbox(geo, bin, start_pos, val_idx))) {
    LOG_WARN("fail to appendBbox", K(ret), K(start_pos), K(val_idx));
  } else if (OB_FAIL(appendString(type_name, bin, start_pos, val_idx))) {
    LOG_WARN("fail to appendString", K(ret), K(type_name), K(start_pos), K(val_idx));
  }
  return ret;
}

int ObWkbToJsonBinVisitor::appendObjKey(const ObString key_str, ObJsonBin &bin, uint64_t &start_pos, int &key_idx)
{
  INIT_SUCC(ret);
  uint64_t key_offset = json_buf_.length() - start_pos;
  uint64_t key_len = key_str.length();
  if (OB_FAIL(bin.set_key_entry(key_idx, key_offset, key_len, false))) {
    LOG_WARN("fail to set_key_entry", K(ret), K(key_idx), K(key_offset), K(key_len));
  } else if (OB_FAIL(json_buf_.append(key_str, 0))) {
    LOG_WARN("fail to append key", K(ret), K(key_str));
  } else if (OB_FAIL(bin.set_current(json_buf_.string(), start_pos))) {
    LOG_WARN("fail to set_current", K(ret), K(start_pos));
  } else {
    key_idx++;
  }
  return ret;
}


int ObWkbToJsonBinVisitor::appendMeta(ObJsonBin &bin, uint64_t &start_pos, int type_flag, uint64_t element_count, uint64_t size_size)
{
  INIT_SUCC(ret);
  ObJsonBinMeta meta;
  ObJBVerType vertype = J_NULL_V0;
  if (type_flag == 0) {
    vertype = ObJsonBin::get_object_vertype();
  } else if (type_flag == 1) {
    vertype = ObJsonBin::get_array_vertype();
  }
  start_pos = json_buf_.length();
  meta.set_type(vertype, false);
  meta.set_element_count(element_count);
  meta.set_element_count_var_type(ObJsonVar::get_var_type(element_count));
  meta.set_obj_size_var_type(size_size);
  meta.set_entry_var_type(meta.obj_size_var_type());
  meta.set_is_continuous(true);
  meta.calc_entry_array();
  if (OB_FAIL(meta.to_header(json_buf_))) {
    LOG_WARN("meta to_header failed", K(ret));
  } else if (OB_FAIL(bin.reset(json_buf_.string(), start_pos, nullptr))) {
    LOG_WARN("init bin with meta failed", K(ret), K(meta));
  }
  return ret;
}

int ObWkbToJsonBinVisitor::fillHeaderSize(ObJsonBin &bin, uint64_t start_pos)
{
  INIT_SUCC(ret);
  uint64_t real_obj_size = static_cast<uint64_t>(json_buf_.length() - start_pos);
  if (OB_FAIL(bin.set_obj_size(real_obj_size))) {
    LOG_WARN("fail to set_obj_size", K(ret), K(real_obj_size)); 
  }
  return ret;
}

int ObWkbToJsonBinVisitor::appendCrs(ObJsonBin &bin, uint64_t start_pos, uint64_t &val_idx)
{
  INIT_SUCC(ret);
  if (!is_appendCrs && srid_ != 0 && ((flag_ & ObGeoJsonFormat::SHORT_SRID) || (flag_ & ObGeoJsonFormat::LONG_SRID))) {
    uint64_t value_offset = json_buf_.length() - start_pos;
    uint8_t value_type = static_cast<uint8_t>(ObJsonNodeType::J_OBJECT);
    if (OB_FAIL(bin.set_value_entry(val_idx, value_offset, value_type, false))) {
      LOG_WARN("fail to set value entry", K(ret), K(value_offset), K(value_type));
    } else {
      // {"crs": {"type": "name", "properties": {"name": "EPSG:4269"}}
      // {"crs": {"type": "name", "properties": {"name": "urn:ogc:def:crs:EPSG::4269"}}
      ObJsonBin crs_bin;
      uint64_t crs_pos = 0;
      uint64_t crs_idx = 0;
      int key_idx = 0;
      if (OB_FAIL(appendMeta(crs_bin, crs_pos, 0, 2, 0))) {
        LOG_WARN("fail to appendMeta", K(ret), K(crs_pos));
      } else if (OB_FAIL(appendObjKey(key_name_table[KeyNameMap::type], crs_bin, crs_pos, key_idx))) {
        LOG_WARN("fail to appendObjKey", K(ret), K(key_name_table[KeyNameMap::type]), K(crs_pos), K(key_idx));
      } else if (OB_FAIL(appendObjKey(key_name_table[KeyNameMap::properties], crs_bin, crs_pos, key_idx))) {
        LOG_WARN("fail to appendObjKey", K(ret), K(key_name_table[KeyNameMap::properties]), K(crs_pos), K(key_idx));
      } else if (OB_FAIL(appendString(key_name_table[KeyNameMap::name], crs_bin, crs_pos, crs_idx))) {
        LOG_WARN("fail to appendString", K(ret), K(crs_pos));
      } else if (OB_FAIL(appendCrsProp(crs_bin, crs_pos, crs_idx))){
        LOG_WARN("fail to appendCrsProp", K(ret), K(crs_pos));
      } else if (OB_FAIL(fillHeaderSize(crs_bin, crs_pos))) {
        LOG_WARN("fail to fillHeaderSize", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(bin.set_current(json_buf_.string(), start_pos))) {
      LOG_WARN("j_bin_.set_current fail", K(ret));
    } else {
      val_idx++;
      is_appendCrs = true;
    }
  }
  return ret;
}

int ObWkbToJsonBinVisitor::appendCrsProp(
    ObJsonBin &bin,
    uint64_t start_pos,
    uint64_t &val_idx)
{
  INIT_SUCC(ret);
  uint64_t value_offset = json_buf_.length() - start_pos;
  uint8_t value_type = static_cast<uint8_t>(ObJsonNodeType::J_OBJECT);
  if (OB_FAIL(bin.set_value_entry(val_idx, value_offset, value_type, false))) {
    LOG_WARN("fail to set value entry", K(ret), K(value_offset), K(value_type));
  } else {
    ObJsonBin prop_bin;
    uint64_t prop_pos = 0;
    uint64_t prop_idx = 0;
    int key_idx = 0;
    char srid_buf[ObFastFormatInt::MAX_DIGITS10_STR_SIZE] = {0};
    uint64_t srid_len = ObFastFormatInt::format_signed(srid_, srid_buf);
    ObStringBuffer prop_value_buf(allocator_);
    ObString prop_value;
    if (flag_ & ObGeoJsonFormat::LONG_SRID) {
      // {"name": "urn:ogc:def:crs:EPSG::[SRID]"}
      if (OB_FAIL(prop_value_buf.append("urn:ogc:def:crs:EPSG::"))) {
        LOG_WARN("fail to append long srid string", K(ret));
      }
    } else {
      // {"name": "EPSG:[SRID]"}
      if (OB_FAIL(prop_value_buf.append("EPSG:"))) {
        LOG_WARN("fail to append short srid string", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(prop_value_buf.append(srid_buf, srid_len, 0))) {
      LOG_WARN("fail to append srid", K(ret), K(srid_));
    } else if (OB_FAIL(prop_value_buf.get_result_string(prop_value))) {
      LOG_WARN("fail to get prop_value stringd", K(ret));
    }
  
    // append object
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(appendMeta(prop_bin, prop_pos, 0, 1, 0))) {
      LOG_WARN("fail to appendMeta", K(ret), K(prop_pos));
    } else if (OB_FAIL(appendObjKey(key_name_table[KeyNameMap::name], prop_bin, prop_pos, key_idx))) {
      LOG_WARN("fail to appendObjKey", K(ret), K(key_name_table[KeyNameMap::name]), K(prop_pos), K(key_idx));
    } else if (OB_FAIL(appendString(prop_value, prop_bin, prop_pos, prop_idx))) {
      LOG_WARN("fail to appendString", K(ret), K(prop_pos));
    } else if (OB_FAIL(fillHeaderSize(prop_bin, prop_pos))) {
      LOG_WARN("fail to fillHeaderSize", K(ret), K(prop_pos));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(bin.set_current(json_buf_.string(), start_pos))) {
    LOG_WARN("fail to set_current", K(ret), K(start_pos));
  } else {
    val_idx++;
  }
  return ret;
}

int ObWkbToJsonBinVisitor::appendBbox(ObGeometry *geo, ObJsonBin &bin, uint64_t start_pos, uint64_t &val_idx)
{
  INIT_SUCC(ret);
  if ((flag_ & ObGeoJsonFormat::BBOX) && !geo->is_empty()) {
    // [xmin, ymin, xmax, ymax]
    ObGeogBox *box = nullptr;
    ObArenaAllocator tmp_allocator;
    ObGeometry *box_geo = nullptr;
    if (geo->crs() == ObGeoCRS::Geographic) {
      if (OB_FAIL(ObGeoTypeUtil::create_geo_by_type(tmp_allocator, geo->type(), false, true, box_geo, geo->get_srid()))) {
        LOG_WARN("fail to create geo by type", K(ret), K(geo->type()));
      } else {
        box_geo->set_data(geo->val());
      }
    } else {
      box_geo = geo;
    }
    CREATE_WITH_TEMP_CONTEXT(lib::ContextParam().set_mem_attr(MTL_ID(), "GISModule", ObCtxIds::DEFAULT_CTX_ID)) {
      ObGeoEvalCtx geo_ctx(CURRENT_CONTEXT);
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(geo_ctx.append_geo_arg(box_geo))) {
        LOG_WARN("build gis context failed", K(ret));
      } else if (OB_FAIL(ObGeoFunc<ObGeoFuncType::Box>::geo_func::eval(geo_ctx, box))) {
        LOG_WARN("failed to do box functor failed", K(ret));
      } else {
        // append Bbox
        uint64_t value_offset = json_buf_.length() - start_pos;
        uint8_t value_type = static_cast<uint8_t>(ObJsonNodeType::J_ARRAY);
        if (OB_FAIL(bin.set_value_entry(val_idx, value_offset, value_type, false))) {
          LOG_WARN("fail to set value entry", K(ret), K(value_offset), K(value_type));
        } else {
          uint64_t array_pos = 0;
          ObJsonBin array_bin;
          uint64_t array_idx = 0;
          if (OB_FAIL(appendMeta(array_bin, array_pos, 1, 4, 0))) {
            LOG_WARN("fail to appendMeta", K(ret), K(array_pos));
          } else if (OB_FAIL(appendDouble(box->xmin, array_bin, array_pos, array_idx))) {
            LOG_WARN("fail to appendDouble", K(ret), K(box->xmin), K(array_pos), K(array_idx));
          } else if (OB_FAIL(appendDouble(box->ymin, array_bin, array_pos, array_idx))) {
            LOG_WARN("fail to appendDouble", K(ret), K(box->ymin), K(array_pos), K(array_idx));
          } else if (OB_FAIL(appendDouble(box->xmax, array_bin, array_pos, array_idx))) {
            LOG_WARN("fail to appendDouble", K(ret), K(box->xmax), K(array_pos), K(array_idx));
          } else if (OB_FAIL(appendDouble(box->ymax, array_bin, array_pos, array_idx))) {
            LOG_WARN("fail to appendDouble", K(ret), K(box->ymax), K(array_pos), K(array_idx));
          } else if (OB_FAIL(fillHeaderSize(array_bin, array_pos))) {
            LOG_WARN("fail to fillHeaderSize", K(ret), K(array_pos));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(bin.set_current(json_buf_.string(), start_pos))) {
          LOG_WARN("fail to set_current", K(ret), K(start_pos));
        } else {
          val_idx++;
        }
      }
    }
  }
  return ret;
}

int ObWkbToJsonBinVisitor::appendString(const ObString &str, ObJsonBin &bin, uint64_t start_pos, uint64_t &val_idx)
{
  INIT_SUCC(ret);
  uint64_t value_offset = json_buf_.length() - start_pos;
  uint8_t value_type = static_cast<uint8_t>(ObJsonNodeType::J_STRING);
  if (OB_FAIL(bin.set_value_entry(val_idx, value_offset, value_type, false))) {
    LOG_WARN("fail to set value entry", K(ret), K(value_offset), K(value_type));
  } else if (OB_FAIL(ObJsonBinSerializer::serialize_json_string(ObJBVerType::J_STRING_V0, str, json_buf_))) {
    LOG_WARN("failed to serialize json string", K(ret), K(str));
  } else if (OB_FAIL(bin.set_current(json_buf_.string(), start_pos))) {
    LOG_WARN("fail to set_current", K(ret), K(start_pos));
  } else {
    val_idx++;
  }
  return ret;
}

int ObWkbToJsonBinVisitor::appendDouble(double value, ObJsonBin &bin, uint64_t start_pos, uint64_t &val_idx)
{
  INIT_SUCC(ret);
  double dec_value = value;
  uint64_t value_offset = json_buf_.length() - start_pos;
  uint8_t value_type = static_cast<uint8_t>(ObJsonNodeType::J_DOUBLE);
  if (OB_FAIL(bin.set_value_entry(val_idx, value_offset, value_type, false))) {
    LOG_WARN("fail to set value entry", K(ret), K(value_offset), K(value_type));
  } else if (max_dec_digits_ < INT_MAX32 
              && OB_FALSE_IT(dec_value = ObGeoTypeUtil::round_double(value, max_dec_digits_, false))) {
  } else if (OB_FAIL(ObJsonBinSerializer::serialize_json_double(dec_value, json_buf_))) {
    LOG_WARN("failed to append double to json_buf_", K(ret), K(dec_value), K(value));
  } else if (OB_FAIL(bin.set_current(json_buf_.string(), start_pos))) {
    LOG_WARN("failed to set_current", K(ret));
  } else {
    val_idx++;
  }
  return ret;
}

int ObWkbToJsonBinVisitor::appendArrayHeader(
    ObJsonBin &bin,
    uint64_t start_pos,
    uint64_t val_idx,
    uint64_t size,
    ObJsonBin &array_bin,
    uint64_t &array_start_pos)
{
  INIT_SUCC(ret);
  uint64_t value_offset = json_buf_.length() - start_pos;
  uint8_t value_type = static_cast<uint8_t>(ObJsonNodeType::J_ARRAY);
  if (OB_FAIL(bin.set_value_entry(val_idx, value_offset, value_type, false))) {
    LOG_WARN("fail to set value entry", K(ret), K(value_offset), K(value_type));
  } else if (OB_FAIL(appendMeta(array_bin, array_start_pos, 1, size))) {
    LOG_WARN("fail to appendMeta", K(ret), K(array_start_pos), K(size));
  }
  return ret;
}

void ObWkbToJsonBinVisitor::reset() 
{ 
  json_buf_.reset();
  flag_ = 0;
  srid_ = 0;
  max_dec_digits_ = UINT_MAX32;
  is_appendCrs = false;
}

} // namespace common
} // namespace oceanbase
