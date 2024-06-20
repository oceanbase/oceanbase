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
#include "ob_wkb_to_json_visitor.h"
#include "ob_srs_info.h"
#include "lib/number/ob_number_v2.h"
#include "lib/charset/ob_dtoa.h"
#include "lib/utility/ob_fast_convert.h"
#include "lib/geo/ob_geo_utils.h"
#include "lib/geo/ob_geo_to_wkt_visitor.h"
#include "lib/geo/ob_geo_func_register.h"
#include "lib/utility/ob_fast_convert.h"
#include "rpc/obmysql/ob_mysql_global.h"

namespace oceanbase {
namespace common {

ObWkbToJsonVisitor::ObWkbToJsonVisitor(ObIAllocator *allocator, uint32_t max_dec_digits/* = UINT_MAX32*/, uint8_t flag/* = 0*/, const ObGeoSrid srid/* = 0*/)
  : buffer_(allocator),
    in_multi_visit_(false),
    colloction_level_(0),
    is_mysql_mode_(lib::is_mysql_mode()),
    flag_(flag),
    max_dec_digits_(max_dec_digits),
    srid_(srid),
    allocator_(allocator),
    append_crs_(false)
{
  left_curly_bracket_ = lib::is_mysql_mode() ? "{" : "{ ";
  right_curly_bracket_ = lib::is_mysql_mode() ? "}" : " }";
  left_sq_bracket_ = lib::is_mysql_mode() ? "[" : "[ ";
  right_sq_bracket_ = lib::is_mysql_mode() ? "]" : " ]";
}

bool ObWkbToJsonVisitor::prepare(ObIWkbGeogMultiPoint *geo)
{
  UNUSED(geo);
  in_multi_visit_ = true;
  return true;
}

bool ObWkbToJsonVisitor::prepare(ObIWkbGeomMultiPoint *geo)
{
  UNUSED(geo);
  in_multi_visit_ = true;
  return true;
}

bool ObWkbToJsonVisitor::prepare(ObIWkbGeogMultiLineString *geo)
{
  UNUSED(geo);
  in_multi_visit_ = true;
  return true;
}

bool ObWkbToJsonVisitor::prepare(ObIWkbGeomMultiLineString *geo)
{
  UNUSED(geo);
  in_multi_visit_ = true;
  return true;
}

bool ObWkbToJsonVisitor::prepare(ObIWkbGeogMultiPolygon *geo)
{
  UNUSED(geo);
  in_multi_visit_ = true;
  return true;
}

bool ObWkbToJsonVisitor::prepare(ObIWkbGeomMultiPolygon *geo)
{
  UNUSED(geo);
  in_multi_visit_ = true;
  return true;
}

bool ObWkbToJsonVisitor::prepare(ObIWkbGeogCollection *geo)
{
  UNUSED(geo);
  colloction_level_++;
  return true;
}

bool ObWkbToJsonVisitor::prepare(ObIWkbGeomCollection *geo)
{
  UNUSED(geo);
  colloction_level_++;
  return true;
}

int ObWkbToJsonVisitor::visit(ObIWkbGeogPoint *geo)
{
  INIT_SUCC(ret);
  if (OB_FAIL(appendPoint<ObIWkbGeogPoint>(geo))) {
    LOG_WARN("fail to append point", K(ret));
  }
  return ret;
}

int ObWkbToJsonVisitor::visit(ObIWkbGeomPoint *geo)
{
  INIT_SUCC(ret);
  if (OB_FAIL(appendPoint<ObIWkbGeomPoint>(geo))) {
    LOG_WARN("fail to append point", K(ret));
  }
  return ret;
}

template<typename T_IBIN>
int ObWkbToJsonVisitor::appendPoint(T_IBIN *geo)
{
  INIT_SUCC(ret);
  const char *type_name = "Point";
  // { "type": "Point", "coordinates": [x, y] }
  if (!in_multi_visit_ && OB_FAIL(appendJsonFields(geo->type(), type_name, geo))) {
    LOG_WARN("fail to append buffer_", K(ret), K(in_multi_visit_), K(type_name));
  } else if (OB_FAIL(appendInnerPoint(geo->x(), geo->y()))) {
    LOG_WARN("fail to appendInnerPoint", K(ret), K(geo->x()), K(geo->y()));
  } else if (!in_multi_visit_ && OB_FAIL(buffer_.append(right_curly_bracket_))) {
    LOG_WARN("fail to append buffer_", K(ret), K(in_multi_visit_));
  } else if ((in_multi_visit_ || in_colloction_visit()) && OB_FAIL(buffer_.append(", "))) {
    LOG_WARN("fail to append buffer_", K(ret));
  }
  return ret;
}

int ObWkbToJsonVisitor::appendInnerPoint(double x, double y)
{
  // [x, y]
  INIT_SUCC(ret);
  if (OB_FAIL(buffer_.append("["))) {
    LOG_WARN("fail to append to buffer_", K(ret));
  } else if (OB_FAIL(appendDouble(x))) {
    LOG_WARN("fail to append x", K(ret), K(x));
  } else if (OB_FAIL(buffer_.append(", "))) {
    LOG_WARN("fail to append to buffer_", K(ret));
  } else if (OB_FAIL(appendDouble(y))) {
    LOG_WARN("fail to append y", K(ret), K(y));
  } else if (OB_FAIL(buffer_.append("]"))) {
    LOG_WARN("fail to append to buffer_", K(ret));
  }

  return ret;
}

int ObWkbToJsonVisitor::appendDouble(double x)
{
  INIT_SUCC(ret);
  uint64_t double_buff_size = is_mysql_mode_ ? DOUBLE_TO_STRING_CONVERSION_BUFFER_SIZE : MAX_DIGITS_IN_DOUBLE;
  uint64_t len_x = 0;
  char *buff_ptr = NULL;
  if (OB_FAIL(buffer_.reserve(double_buff_size))) {
    LOG_WARN("fail to reserve buffer", K(ret));
  } else if (FALSE_IT(buff_ptr = buffer_.ptr() + buffer_.length())) {
  } else if (is_mysql_mode_) {
    x = ObGeoTypeUtil::round_double(x, max_dec_digits_, false);
    len_x = ob_gcvt(x, ob_gcvt_arg_type::OB_GCVT_ARG_DOUBLE,
        DOUBLE_TO_STRING_CONVERSION_BUFFER_SIZE, buff_ptr, NULL);
  } else if (OB_FAIL(ObGeoToWktVisitor::convert_double_to_str(buff_ptr, double_buff_size, x, true,
                          MAX_DIGITS_IN_DOUBLE, !is_mysql_mode_, len_x))) {
    LOG_WARN("fail to append x val to buffer", K(ret));
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(buffer_.set_length(buffer_.length() + len_x))) {
    LOG_WARN("fail to set buffer x len", K(ret), K(len_x));
  } else if (is_mysql_mode_) {
    // add '.0' to interger
    bool has_sci_or_dot = false;
    for (int i = 0; i < len_x && !has_sci_or_dot; ++i) {
      if (buff_ptr[i] == 'e' || buff_ptr[i] == '.') {
        has_sci_or_dot = true;
      }
    }
    if (!has_sci_or_dot) {
      if (OB_FAIL(buffer_.append("."))) {
        LOG_WARN("fail to append '.' to buffer", K(ret), K(len_x));
      } else if (OB_FAIL(buffer_.append("0"))) {
        LOG_WARN("fail to append '0' to buffer", K(ret), K(len_x));
      }
    }
  }
  return ret;
}

int ObWkbToJsonVisitor::appendJsonFields(ObGeoType type, const char *type_name, ObGeometry *geo) {
  int ret = OB_SUCCESS;
  if (type <= ObGeoType::GEOMETRY || type >= ObGeoType::GEOTYPEMAX) {
    LOG_WARN("invalid geo type", K(ret), K(type));
  } else if (OB_FAIL(buffer_.append(left_curly_bracket_))) {
    LOG_WARN("fail to append left curly bracket", K(ret));
  } else if (is_mysql_mode_ && OB_FAIL(appendMySQLFlagInfo(geo))) {
    LOG_WARN("fail to append mysql geojson flag info", K(ret));
  } else if (OB_FAIL(buffer_.append("\"type\": \""))) {
    LOG_WARN("fail to append type field", K(ret));
  } else if (OB_FAIL(buffer_.append(type_name))) {
    LOG_WARN("fail to append type value", K(ret), K(type_name));
  } else if (type != ObGeoType::GEOMETRYCOLLECTION &&
               OB_FAIL(buffer_.append("\", \"coordinates\": "))) {
    LOG_WARN("fail to append coordinates field", K(ret));
  } else if (type == ObGeoType::GEOMETRYCOLLECTION &&
               OB_FAIL(buffer_.append("\", \"geometries\": "))) {
    LOG_WARN("fail to append geometries field", K(ret));
  }
  return ret;
}

int ObWkbToJsonVisitor::visit(ObIWkbGeogLineString *geo)
{
  INIT_SUCC(ret);
  if (OB_FAIL((appendLine<ObIWkbGeogLineString, ObWkbGeogLineString>(geo)))) {
    LOG_WARN("fail to append line", K(ret));
  }
  return ret;
}

int ObWkbToJsonVisitor::visit(ObIWkbGeomLineString *geo)
{
  INIT_SUCC(ret);
  if (OB_FAIL((appendLine<ObIWkbGeomLineString, ObWkbGeomLineString>(geo)))) {
    LOG_WARN("fail to append line", K(ret));
  }
  return ret;
}

template<typename T_IBIN, typename T_BIN>
int ObWkbToJsonVisitor::appendLine(T_IBIN *geo)
{
  INIT_SUCC(ret);
  const char *type_name = "LineString";
  if ((!in_multi_visit_ || in_oracle_colloction_visit()) && OB_FAIL(appendJsonFields(geo->type(), type_name, geo))) {
    LOG_WARN("fail to append buffer_", K(ret), K(in_multi_visit_), K(type_name));
  } else if (OB_FAIL(buffer_.append(left_sq_bracket_))) {
    LOG_WARN("fail to append buffer_", K(ret));
  } else {
    const T_BIN *line = reinterpret_cast<const T_BIN *>(geo->val());
    typename T_BIN::iterator iter = line->begin();
    for ( ; OB_SUCC(ret) && iter != line->end(); iter++) {
      if (OB_FAIL(appendInnerPoint(iter->template get<0>(), iter->template get<1>()))) {
        LOG_WARN("fail to appendInnerPoint", K(ret), K(iter->template get<0>()), K(iter->template get<1>()));
      } else if (OB_FAIL(buffer_.append(", "))) {
        LOG_WARN("fail to append buffer_", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(buffer_.set_length(buffer_.length() - 2))) {
      LOG_WARN("fail to set buffer_ len", K(ret), K(buffer_.length()));
    } else if (OB_FAIL(buffer_.append(right_sq_bracket_))) {
      LOG_WARN("fail to append buffer_", K(ret));
    } else if ((!in_multi_visit_ || in_oracle_colloction_visit()) && OB_FAIL(buffer_.append(right_curly_bracket_))) {
      LOG_WARN("fail to append buffer_", K(ret));
    } else if ((in_multi_visit_ || in_colloction_visit()) && OB_FAIL(buffer_.append(", "))) {
      LOG_WARN("fail to append buffer_", K(ret));
    }
  }
  return ret;
}

int ObWkbToJsonVisitor::visit(ObIWkbGeogMultiPoint *geo)
{
  INIT_SUCC(ret);
  const char *type_name = "MultiPoint";
  if (OB_FAIL(appendJsonFields(geo->type(), type_name, geo))) {
    LOG_WARN("fail to append buffer_", K(ret), K(in_multi_visit_), K(type_name));
  } else if (OB_FAIL(buffer_.append(left_sq_bracket_))) {
    LOG_WARN("fail to append buffer_", K(ret));
  }
  return ret;
}

int ObWkbToJsonVisitor::visit(ObIWkbGeomMultiPoint *geo)
{
  INIT_SUCC(ret);
  const char *type_name = "MultiPoint";
  if (OB_FAIL(appendJsonFields(geo->type(), type_name, geo))) {
    LOG_WARN("fail to append buffer_", K(ret), K(in_multi_visit_), K(type_name));
  } else if (OB_FAIL(buffer_.append(left_sq_bracket_))) {
    LOG_WARN("fail to append buffer_", K(ret));
  }
  return ret;
}

int ObWkbToJsonVisitor::visit(ObIWkbGeogMultiLineString *geo)
{
  INIT_SUCC(ret);
  if (!in_oracle_colloction_visit()) {
    const char *type_name = "MultiLineString";
    if (OB_FAIL(appendJsonFields(geo->type(), type_name, geo))) {
      LOG_WARN("fail to append buffer_", K(ret), K(in_multi_visit_), K(type_name));
    } else if (OB_FAIL(buffer_.append(left_sq_bracket_))) {
      LOG_WARN("fail to append buffer_", K(ret));
    }
  }
  return ret;
}

int ObWkbToJsonVisitor::visit(ObIWkbGeomMultiLineString *geo)
{
  INIT_SUCC(ret);
  if (!in_oracle_colloction_visit()) {
    const char *type_name = "MultiLineString";
    if (OB_FAIL(appendJsonFields(geo->type(), type_name, geo))) {
      LOG_WARN("fail to append buffer_", K(ret), K(in_multi_visit_), K(type_name));
    } else if (OB_FAIL(buffer_.append(left_sq_bracket_))) {
      LOG_WARN("fail to append buffer_", K(ret));
    }
  }
  return ret;
}

int ObWkbToJsonVisitor::visit(ObIWkbGeogPolygon *geo)
{
  INIT_SUCC(ret);
  if (OB_FAIL((appendPolygon<ObIWkbGeogPolygon, ObWkbGeogPolygon,
                             ObWkbGeogLinearRing, ObWkbGeogPolygonInnerRings>(geo)))) {
    LOG_WARN("fail to append polygon", K(ret));
  }
  return ret;
}

int ObWkbToJsonVisitor::visit(ObIWkbGeomPolygon *geo)
{
  INIT_SUCC(ret);
  if (OB_FAIL((appendPolygon<ObIWkbGeomPolygon, ObWkbGeomPolygon,
                             ObWkbGeomLinearRing, ObWkbGeomPolygonInnerRings>(geo)))) {
    LOG_WARN("fail to append polygon", K(ret));
  }
  return ret;
}

template<typename T_IBIN, typename T_BIN,
         typename T_BIN_RING, typename T_BIN_INNER_RING>
int ObWkbToJsonVisitor::appendPolygon(T_IBIN *geo)
{
  INIT_SUCC(ret);
  const char *type_name = "Polygon";
  if (geo->length() < WKB_COMMON_WKB_HEADER_LEN) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("invalid wkb length", K(ret), K(geo->length()));
  } else if ((!in_multi_visit_ || in_oracle_colloction_visit()) && OB_FAIL(appendJsonFields(geo->type(), type_name, geo))) {
    LOG_WARN("fail to append buffer_", K(ret), K(in_multi_visit_), K(type_name));
  } else if (OB_FAIL(buffer_.append(left_sq_bracket_))) {
    LOG_WARN("fail to append buffer_", K(ret));
  } else {
    T_BIN& poly = *(T_BIN *)(geo->val());
    T_BIN_RING& exterior = poly.exterior_ring();
    T_BIN_INNER_RING& inner_rings = poly.inner_rings();
    if (poly.size() != 0) {
      typename T_BIN_RING::iterator iter = exterior.begin();
      if (OB_FAIL(buffer_.append(left_sq_bracket_))) {
        LOG_WARN("fail to append buffer_", K(ret));
      }
      for (; OB_SUCC(ret) && iter != exterior.end(); ++iter) {
        if (OB_FAIL(appendInnerPoint(iter->template get<0>(), iter->template get<1>()))) {
          LOG_WARN("fail to appendInnerPoint", K(ret), K(iter->template get<0>()), K(iter->template get<1>()));
        } else if (OB_FAIL(buffer_.append(", "))) {
          LOG_WARN("fail to append buffer_", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(buffer_.set_length(buffer_.length() - 2))) {
        LOG_WARN("fail to set buffer_ len", K(ret), K(buffer_.length()));
      } else if (OB_FAIL(buffer_.append(right_sq_bracket_))) {
        LOG_WARN("fail to append buffer_", K(ret));
      } else if (OB_FAIL(buffer_.append(", "))) {
        LOG_WARN("fail to append buffer_", K(ret));
      }
    }

    typename T_BIN_INNER_RING::iterator iterInnerRing = inner_rings.begin();
    for (; OB_SUCC(ret) && iterInnerRing != inner_rings.end(); ++iterInnerRing) {
      typename T_BIN_RING::iterator iter = (*iterInnerRing).begin();
      if (OB_FAIL(buffer_.append(left_sq_bracket_))) {
        LOG_WARN("fail to append buffer_", K(ret));
      }
      for (; OB_SUCC(ret) && iter != (*iterInnerRing).end(); ++iter) {
        if (OB_FAIL(appendInnerPoint(iter->template get<0>(), iter->template get<1>()))) {
          LOG_WARN("fail to appendInnerPoint", K(ret), K(iter->template get<0>()), K(iter->template get<1>()));
        } else if (OB_FAIL(buffer_.append(", "))) {
          LOG_WARN("fail to append buffer_", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(buffer_.set_length(buffer_.length() - 2))) {
        LOG_WARN("fail to set buffer_ len", K(ret), K(buffer_.length()));
      } else if (OB_FAIL(buffer_.append(right_sq_bracket_))) {
        LOG_WARN("fail to append buffer_", K(ret));
      } else if (OB_FAIL(buffer_.append(", "))) {
        LOG_WARN("fail to append buffer_", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(buffer_.set_length(buffer_.length() - 2))) {
      LOG_WARN("fail to set buffer_ len", K(ret), K(buffer_.length()));
    } else if (OB_FAIL(buffer_.append(right_sq_bracket_))) {
      LOG_WARN("fail to append buffer_", K(ret));
    } else if ((!in_multi_visit_ || in_oracle_colloction_visit()) && OB_FAIL(buffer_.append(right_curly_bracket_))) {
      LOG_WARN("fail to append buffer_", K(ret));
    } else if ((in_multi_visit_ || in_colloction_visit()) && OB_FAIL(buffer_.append(", "))) {
      LOG_WARN("fail to append buffer_", K(ret));
    }
  }
  return ret;
}

int ObWkbToJsonVisitor::visit(ObIWkbGeogMultiPolygon *geo)
{
  INIT_SUCC(ret);
  if (!in_oracle_colloction_visit()) {
    const char *type_name = "MultiPolygon";
    if (OB_ISNULL(geo)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("geo is NULL", K(ret));
    } else if (OB_FAIL(appendMultiPrefix(geo->type(), type_name, geo))){
      LOG_WARN("fail to append multi prefix", K(ret));
    }
  }
  return ret;
}

int ObWkbToJsonVisitor::visit(ObIWkbGeomMultiPolygon *geo)
{
  INIT_SUCC(ret);
  if (!in_oracle_colloction_visit()) {
    const char *type_name = "MultiPolygon";
    if (OB_ISNULL(geo)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("geo is NULL", K(ret));
    } else if (OB_FAIL(appendMultiPrefix(geo->type(), type_name, geo))){
      LOG_WARN("fail to append multi prefix", K(ret));
    }
  }
  return ret;
}

int ObWkbToJsonVisitor::visit(ObIWkbGeogCollection *geo)
{
  INIT_SUCC(ret);
  const char *type_name = "GeometryCollection";
  if (OB_ISNULL(geo)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("geo is NULL", K(ret));
  } else if (OB_FAIL(appendMultiPrefix(geo->type(), type_name, geo))){
    LOG_WARN("fail to append multi prefix", K(ret));
  }
  return ret;
}

int ObWkbToJsonVisitor::visit(ObIWkbGeomCollection *geo)
{
  INIT_SUCC(ret);
  const char *type_name = "GeometryCollection";
  if (OB_ISNULL(geo)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("geo is NULL", K(ret));
  } else if (OB_FAIL(appendMultiPrefix(geo->type(), type_name, geo))){
    LOG_WARN("fail to append multi prefix", K(ret));
  }
  return ret;
}

int ObWkbToJsonVisitor::appendMultiPrefix(ObGeoType geo_type, const char *type_name, ObGeometry *geo)
{
  INIT_SUCC(ret);
  if (OB_FAIL(appendJsonFields(geo_type, type_name, geo))) {
    LOG_WARN("fail to append buffer_", K(ret), K(in_multi_visit_), K(type_name));
  } else if (OB_FAIL(buffer_.append(left_sq_bracket_))) {
    LOG_WARN("fail to append buffer_", K(ret));
  }
  return ret;
}

int ObWkbToJsonVisitor::appendMultiSuffix(ObGeoType geo_type)
{
  INIT_SUCC(ret);
  if (OB_FAIL(buffer_.set_length(buffer_.length() - 2))) {
      LOG_WARN("fail to set buffer_ len", K(ret), K(buffer_.length()));
  } else if ((geo_type == ObGeoType::MULTIPOINT || !in_oracle_colloction_visit()) && OB_FAIL(buffer_.append(right_sq_bracket_))) {
    LOG_WARN("fail to append buffer_", K(ret));
  } else if (OB_FAIL(buffer_.append(right_curly_bracket_))) {
    LOG_WARN("fail to append buffer_", K(ret), K(right_curly_bracket_));
  } else if ((in_colloction_visit()) && OB_FAIL(buffer_.append(", "))) {
    LOG_WARN("fail to append buffer_", K(ret));
  }
  return ret;
}

int ObWkbToJsonVisitor::finish(ObIWkbGeogMultiPoint *geo)
{
  INIT_SUCC(ret);
  in_multi_visit_ = false;
  if (OB_ISNULL(geo)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("geo is NULL", K(ret));
  } else if (OB_FAIL(appendMultiSuffix(geo->type()))) {
    LOG_WARN("fail to append multi suffix", K(ret));
  }
  return ret;
}

int ObWkbToJsonVisitor::finish(ObIWkbGeomMultiPoint *geo)
{
  INIT_SUCC(ret);
  in_multi_visit_ = false;
  if (OB_ISNULL(geo)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("geo is NULL", K(ret));
  } else if (OB_FAIL(appendMultiSuffix(geo->type()))) {
    LOG_WARN("fail to append multi suffix", K(ret));
  }
  return ret;
}

int ObWkbToJsonVisitor::finish(ObIWkbGeogMultiLineString *geo)
{
  INIT_SUCC(ret);
  in_multi_visit_ = false;
  if (OB_ISNULL(geo)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("geo is NULL", K(ret));
  } else if (OB_FAIL(appendMultiSuffix(geo->type()))) {
    LOG_WARN("fail to append multi suffix", K(ret));
  }
  return ret;
}

int ObWkbToJsonVisitor::finish(ObIWkbGeomMultiLineString *geo)
{
  INIT_SUCC(ret);
  in_multi_visit_ = false;
  if (OB_ISNULL(geo)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("geo is NULL", K(ret));
  } else if (OB_FAIL(appendMultiSuffix(geo->type()))) {
    LOG_WARN("fail to append multi suffix", K(ret));
  }
  return ret;
}

int ObWkbToJsonVisitor::finish(ObIWkbGeogMultiPolygon *geo)
{
  INIT_SUCC(ret);
  in_multi_visit_ = false;
  if (OB_ISNULL(geo)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("geo is NULL", K(ret));
  } else if (OB_FAIL(appendMultiSuffix(geo->type()))) {
    LOG_WARN("fail to append multi suffix", K(ret));
  }
  return ret;
}

int ObWkbToJsonVisitor::finish(ObIWkbGeomMultiPolygon *geo)
{
  INIT_SUCC(ret);
  in_multi_visit_ = false;
  if (OB_ISNULL(geo)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("geo is NULL", K(ret));
  } else if (OB_FAIL(appendMultiSuffix(geo->type()))) {
    LOG_WARN("fail to append multi suffix", K(ret));
  }
  return ret;
}

template<typename T_IBIN>
int ObWkbToJsonVisitor::appendCollectionSuffix(T_IBIN *geo)
{
  INIT_SUCC(ret);
  ObString comma(2, buffer_.ptr() + buffer_.length() - 2);
  if ((comma.compare(", ") == 0) && OB_FAIL(buffer_.set_length(buffer_.length() - 2))) {
      LOG_WARN("fail to set buffer_ len", K(ret), K(buffer_.length()));
  } else if (OB_FAIL(buffer_.append(right_sq_bracket_))) {
    LOG_WARN("fail to append buffer_", K(ret));
  } else if (OB_FAIL(buffer_.append(right_curly_bracket_))) {
    LOG_WARN("fail to append buffer_", K(ret), K(right_curly_bracket_));
  }

  colloction_level_--;
  if (OB_FAIL(ret)) {
  } else if (in_colloction_visit() && OB_FAIL(buffer_.append(", "))) {
    LOG_WARN("fail to append buffer_", K(ret));
  }
  return ret;
}

int ObWkbToJsonVisitor::finish(ObIWkbGeomCollection *geo)
{
  return appendCollectionSuffix(geo);
}

int ObWkbToJsonVisitor::finish(ObIWkbGeogCollection *geo)
{
  return appendCollectionSuffix(geo);
}

void ObWkbToJsonVisitor::get_geojson(ObString &geojson)
{
  geojson.assign(buffer_.ptr(), static_cast<int32_t>(buffer_.length()));
}

void ObWkbToJsonVisitor::reset()
{
  buffer_.reset();
  in_multi_visit_ = false;
  colloction_level_ = 0;
  max_dec_digits_ = UINT_MAX32;
  flag_ = 0;
  srid_ = 0;
}

int ObWkbToJsonVisitor::appendBox(ObGeogBox &box)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(buffer_.append("\"bbox\": ["))) {
    LOG_WARN("fail to append bbox field", K(ret));
  } else if (OB_FAIL(appendDouble(box.xmin))) {
    LOG_WARN("fail to append x", K(ret), K(box.xmin));
  } else if (OB_FAIL(buffer_.append(", "))) {
    LOG_WARN("fail to append comma", K(ret));
  } else if (OB_FAIL(appendDouble(box.ymin))) {
    LOG_WARN("fail to append x", K(ret), K(box.ymin));
  } else if (OB_FAIL(buffer_.append(", "))) {
    LOG_WARN("fail to append comma", K(ret));
  } else if (OB_FAIL(appendDouble(box.xmax))) {
    LOG_WARN("fail to append x", K(ret), K(box.xmax));
  } else if (OB_FAIL(buffer_.append(", "))) {
    LOG_WARN("fail to append comma", K(ret));
  } else if (OB_FAIL(appendDouble(box.ymax))) {
    LOG_WARN("fail to append x", K(ret), K(box.ymax));
  } else if (OB_FAIL(buffer_.append("]"))) {
    LOG_WARN("fail to append comma", K(ret));
  }
  return ret;
}

int ObWkbToJsonVisitor::appendMySQLFlagInfo(ObGeometry *geo)
{
  int ret = OB_SUCCESS;
  if (!append_crs_ && srid_ != 0
      && ((flag_ & ObGeoJsonFormat::SHORT_SRID) || (flag_ & ObGeoJsonFormat::LONG_SRID))) {
    // "crs": {"type": "name", "properties": {"name": "[EPSG srid]"}},
    if (OB_FAIL(buffer_.append("\"crs\": {\"type\": \"name\", \"properties\": {\"name\": \""))) {
      LOG_WARN("fail to append crs field", K(ret));
    } else if (flag_ & ObGeoJsonFormat::LONG_SRID) {
      // urn:ogc:def:crs:EPSG::[srid]
      // long srid flag will overwrite short srid
      if (OB_FAIL(buffer_.append("urn:ogc:def:crs:EPSG::"))) {
        LOG_WARN("fail to append crs value", K(ret));
      }
    } else {
      // EPSG:[srid]
      if (OB_FAIL(buffer_.append("EPSG:"))) {
        LOG_WARN("fail to append crs value", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else {
      char flag_buf[ObFastFormatInt::MAX_DIGITS10_STR_SIZE] = {0};
      uint64_t len = ObFastFormatInt::format_signed(srid_, flag_buf);
      if (OB_FAIL(buffer_.append(flag_buf, len))) {
        LOG_WARN("fail to append srid value", K(ret), K(srid_));
      } else if (OB_FAIL(buffer_.append("\"}}, "))) {
        LOG_WARN("fail to append crs ending str", K(ret));
      } else {
        append_crs_ = true;
      }
    }
  }

  if (OB_SUCC(ret) && (flag_ & ObGeoJsonFormat::BBOX) && !geo->is_empty()) {
    // "bbox": [ymin, xmin, ymax, xmax],
    ObGeogBox *box = nullptr;
    // geographic geometry also represent it's cartesian box in mysql
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
    ObGeoEvalCtx geo_ctx(allocator_);
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(geo_ctx.append_geo_arg(box_geo))) {
      LOG_WARN("build gis context failed", K(ret));
    } else if (OB_FAIL(ObGeoFunc<ObGeoFuncType::Box>::geo_func::eval(geo_ctx, box))) {
      LOG_WARN("failed to do box functor failed", K(ret));
    } else if (OB_FAIL(appendBox(*box))) {
      LOG_WARN("fail to append bbox field", K(ret));
    } else if (OB_FAIL(buffer_.append(", "))) {
      LOG_WARN("fail to append comma", K(ret));
    }
  }
  return ret;
}

} // namespace common
} // namespace oceanbase
