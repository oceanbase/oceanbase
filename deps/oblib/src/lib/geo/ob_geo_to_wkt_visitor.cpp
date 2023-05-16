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
#include "ob_geo_to_wkt_visitor.h"
#include "ob_srs_info.h"
#include "lib/number/ob_number_v2.h"
#include "lib/charset/ob_dtoa.h"
#include "lib/utility/ob_fast_convert.h"
#include "lib/geo/ob_geo_utils.h"


namespace oceanbase {
namespace common {

const double NOSCI_MIN_DOUBLE = 1E-8;
const double NOSCI_MAX_DOUBLE = 1E15;

int ObGeoToWktVisitor::append_double_with_prec(char *buff,
                                               const int32_t buff_size,
                                               uint64_t &out_len,
                                               double value)
{
  const int64_t number_str_size = 256;
  const int64_t number_val_size = number::ObNumber::MAX_BYTE_LEN;
  const int64_t expr_max_size = 10; // 6 is enough, e-323 or e+308

  int ret = OB_SUCCESS;
  char number_str[number_str_size] = {0};
  char expr_str[expr_max_size] = {0};
  char buf_alloc[number_val_size];

  double abs_value = fabs(value);
  // set force scientific notation
  bool force_sci = (abs_value < NOSCI_MIN_DOUBLE) || (abs_value > NOSCI_MAX_DOUBLE);

  out_len = ob_gcvt_strict(value, ob_gcvt_arg_type::OB_GCVT_ARG_DOUBLE, number_str_size,
                           number_str, NULL, FALSE, TRUE, force_sci);
  int64_t expr_pos = 0; // expr start pos;
  if (out_len > 0) {
    for (int64_t i = 0; (i < out_len) && (number_str[i] != 0) && (expr_pos == 0); i++) {
      if (number_str[i] == 'e' || number_str[i] == 'E') {
        expr_pos = i;
      }
    }
  }
  int64_t expr_len = (expr_pos == 0) ? 0 : out_len - expr_pos; // expr length
  int64_t decimal_len = (expr_pos == 0) ? out_len : expr_pos;
  int64_t new_decimal_len = 0;
  if (expr_len > 0 && expr_len < expr_max_size) {
    MEMCPY(expr_str, number_str + expr_pos, expr_len);
    int64_t expr_non_zero_pos = expr_len;
    for (; (expr_non_zero_pos > 1) && (expr_str[expr_non_zero_pos - 1] == '0'); expr_non_zero_pos--) {
       // do nothing
    }
    if (expr_non_zero_pos == 1) {
      expr_len = 0; // xe0, remove e;
    }
  }

  ObDataBuffer tmp_allocator(buf_alloc, number_val_size);
  number::ObNumber number_value;
  // round decimal part to assigned precision
  if (expr_len > expr_max_size) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error expr length", K(ret), K(out_len), K(expr_pos), K(expr_len), K(decimal_len));
  } else if (OB_FAIL(number_value.from(number_str, decimal_len, tmp_allocator))) {
    LOG_WARN("number from str failed", K(ret));
  } else if (OB_FAIL(number_value.format(number_str,
                                         number_str_size,
                                         new_decimal_len,
                                         static_cast<int16_t>(scale_)))) {
    LOG_WARN("failed to format number to string", K(ret));
  } else if (new_decimal_len == 1 && number_str[0] == '0' && number_value.is_negative()) {
    // -0.4 round to 0 => -0
    number_str[0] = '-';
    number_str[1] = '0';
    new_decimal_len = 2;
  } else if (number_str[new_decimal_len - 1] == '0') {
    // remove padding 1.00000 => 1, 1.00001000 -> 1.0001
    for (; (new_decimal_len > 1) && (number_str[new_decimal_len - 1] == '0'); new_decimal_len--) {
      /* do nothing */
    }
    if (number_str[new_decimal_len - 1] == '.') {
      new_decimal_len--;
    }
  }

  if (OB_SUCC(ret)) {
    out_len = new_decimal_len + expr_len;
    if (out_len > buff_size) {
      ret = OB_SIZE_OVERFLOW;
       LOG_WARN("string size overflow", K(ret), K(value), K(out_len), K(expr_len), K(new_decimal_len));
    } else {
      MEMCPY(buff, number_str, new_decimal_len);
      if (expr_len > 0) {
        MEMCPY(buff + new_decimal_len, expr_str, expr_len);
      }
    }
  }

  return ret;
}

int ObGeoToWktVisitor::appendInnerPoint(double x, double y)
{
  // [x][ ][y]
  INIT_SUCC(ret);
  char* start = buffer_.ptr() + buffer_.length();
  uint64_t len_x = 0;
  if (has_scale_) {
    if (OB_FAIL(append_double_with_prec(start, MAX_DIGITS_IN_DOUBLE, len_x, x))) {
      LOG_WARN("fail to append double to buffer with precsion", K(ret), K(x));
    }
  } else {
    len_x = ob_gcvt(x, ob_gcvt_arg_type::OB_GCVT_ARG_DOUBLE, buffer_.remain(), start, NULL);
    if (len_x == 0) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("fail to convert double to string", K(ret), K(x), K(y), K(buffer_.length()), K(buffer_.remain()));
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(buffer_.set_length(buffer_.length() + len_x))) {
    LOG_WARN("fail to set buffer_ len", K(ret), K(buffer_.length()), K(len_x));
  } else if (OB_FAIL(buffer_.append(" "))) {
    LOG_WARN("fail to append buffer_", K(ret));
  } else {
    start = buffer_.ptr() + buffer_.length();
    uint64_t len_y = 0;
    if (has_scale_) {
      if (OB_FAIL(append_double_with_prec(start, MAX_DIGITS_IN_DOUBLE, len_y, y))) {
        LOG_WARN("fail to append double to buffer with precsion", K(ret), K(y));
      }
    } else {
      len_y = ob_gcvt(y, ob_gcvt_arg_type::OB_GCVT_ARG_DOUBLE, buffer_.remain(), start, NULL);
      if (len_y == 0) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("fail to convert double to string", K(ret), K(x), K(y), K(buffer_.length()), K(buffer_.remain()));
      }
    }
    if (OB_FAIL(ret)) {
    // do nothing
    } else if (OB_FAIL(buffer_.set_length(buffer_.length() + len_y))) {
      LOG_WARN("fail to set buffer_ len", K(ret), K(buffer_.length()), K(len_y));
    }
  }
  return ret;
}

template<typename T_IBIN>
int ObGeoToWktVisitor::appendPoint(T_IBIN *geo)
{
  INIT_SUCC(ret);
  const char *type_name = ObGeoTypeUtil::get_geo_name_by_type(geo->type());
  uint64_t reserve_len = MAX_DIGITS_IN_DOUBLE * 2 + 3;
  reserve_len += in_multi_visit_ ? 0 : strlen(type_name);
  reserve_len += (in_multi_visit_ || in_colloction_visit()) ? 1 : 0;
  // [type_name][(][x][ ][y][)]
  if (OB_FAIL(buffer_.reserve(reserve_len))) {
    LOG_WARN("fail to reserve memory for buffer_", K(ret), K(reserve_len));
  } else if (!in_multi_visit_ && OB_FAIL(buffer_.append(type_name))) {
    LOG_WARN("fail to append buffer_", K(ret), K(in_multi_visit_), K(type_name));
  } else if (OB_FAIL(buffer_.append("("))) {
    LOG_WARN("fail to append buffer_", K(ret));
  } else if (OB_FAIL(appendInnerPoint(geo->x(), geo->y()))) {
    LOG_WARN("fail to appendInnerPoint", K(ret), K(geo->x()), K(geo->y()));
  } else if (OB_FAIL(buffer_.append(")"))) {
    LOG_WARN("fail to append buffer_", K(ret));
  } else if ((in_multi_visit_ || in_colloction_visit())  && OB_FAIL(buffer_.append(","))) {
    LOG_WARN("fail to append buffer_", K(ret));
  }
  return ret;
}

template<typename T_IBIN, typename T_BIN>
int ObGeoToWktVisitor::appendLine(T_IBIN *geo)
{
  INIT_SUCC(ret);
  uint64_t size = geo->size();
  uint64_t reserve_len = (MAX_DIGITS_IN_DOUBLE * 2 + 1 + 1) * size;
  const char *type_name = ObGeoTypeUtil::get_geo_name_by_type(geo->type());
  reserve_len += in_multi_visit_ ? 0 : (strlen(type_name) + 2);
  reserve_len += (in_multi_visit_ || in_colloction_visit()) ? 1 : 0;
  // [type_name][(][x1][ ][y1][,][x2][ ][y2][)]
  if (OB_FAIL(buffer_.reserve(reserve_len))) {
    LOG_WARN("fail to reserve memory for buffer_", K(ret), K(reserve_len));
  } else if (!in_multi_visit_ && OB_FAIL(buffer_.append(type_name))) {
    LOG_WARN("fail to append buffer_", K(ret), K(in_multi_visit_), K(type_name));
  } else if (OB_FAIL(buffer_.append("("))) {
    LOG_WARN("fail to append buffer_", K(ret));
  } else {
    const T_BIN *line = reinterpret_cast<const T_BIN *>(geo->val());
    typename T_BIN::iterator iter = line->begin();
    for ( ; OB_SUCC(ret) && iter != line->end(); iter++) {
      if (OB_FAIL(appendInnerPoint(iter->template get<0>(), iter->template get<1>()))) {
        LOG_WARN("fail to appendInnerPoint", K(ret), K(iter->template get<0>()), K(iter->template get<1>()));
      } else if (OB_FAIL(buffer_.append(","))) {
        LOG_WARN("fail to append buffer_", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(buffer_.set_length(buffer_.length() - 1))) {
      LOG_WARN("fail to set buffer_ len", K(ret), K(buffer_.length()));
    } else if (OB_FAIL(buffer_.append(")"))) {
      LOG_WARN("fail to append buffer_", K(ret));
    } else if ((in_multi_visit_ || in_colloction_visit()) && OB_FAIL(buffer_.append(","))) {
      LOG_WARN("fail to append buffer_", K(ret));
    }
  }
  return ret;
}

template<typename T_IBIN, typename T_BIN,
         typename T_BIN_RING, typename T_BIN_INNER_RING>
int ObGeoToWktVisitor::appendPolygon(T_IBIN *geo)
{
  INIT_SUCC(ret);
  const char *type_name = ObGeoTypeUtil::get_geo_name_by_type(geo->type());
  uint64_t reserve_len = 2;
  reserve_len += in_multi_visit_ ? 0 : strlen(type_name);
  reserve_len += (in_multi_visit_ || in_colloction_visit()) ? 1 : 0;
  // [type_name][(][(][x1][ ][y1][,][x2][ ][y2][,][x3][ ][y3][)][)]
  if (geo->length() < WKB_COMMON_WKB_HEADER_LEN) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("invalid wkb length", K(ret), K(geo->length()));
  } else if (OB_FAIL(buffer_.reserve(reserve_len))) {
    LOG_WARN("fail to reserve memory for buffer_", K(ret), K(reserve_len));
  } else if (!in_multi_visit_ && OB_FAIL(buffer_.append(type_name))) {
    LOG_WARN("fail to append buffer_", K(ret), K(in_multi_visit_), K(type_name));
  } else if (OB_FAIL(buffer_.append("("))) {
    LOG_WARN("fail to append buffer_", K(ret));
  } else {
    T_BIN& poly = *(T_BIN *)(geo->val());
    T_BIN_RING& exterior = poly.exterior_ring();
    T_BIN_INNER_RING& inner_rings = poly.inner_rings();
    if (poly.size() != 0) {
      typename T_BIN_RING::iterator iter = exterior.begin();
      // [(][x1][ ][y1][,][x2][ ][y2][,][x3][ ][y3][)]
      if (OB_FAIL(buffer_.reserve(reserve_len))) {
        LOG_WARN("fail to reserve memory for buffer_", K(ret), K(reserve_len));
      } else if (OB_FAIL(buffer_.append("("))) {
        LOG_WARN("fail to append buffer_", K(ret));
      }
      for (; OB_SUCC(ret) && iter != exterior.end(); ++iter) {
        if (OB_FAIL(appendInnerPoint(iter->template get<0>(), iter->template get<1>()))) {
          LOG_WARN("fail to appendInnerPoint", K(ret), K(iter->template get<0>()), K(iter->template get<1>()));
        } else if (OB_FAIL(buffer_.append(","))) {
          LOG_WARN("fail to append buffer_", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(buffer_.set_length(buffer_.length() - 1))) {
        LOG_WARN("fail to set buffer_ len", K(ret), K(buffer_.length()));
      } else if (OB_FAIL(buffer_.append(")"))) {
        LOG_WARN("fail to append buffer_", K(ret));
      } else if (OB_FAIL(buffer_.append(","))) {
        LOG_WARN("fail to append buffer_", K(ret));
      }
    }

    typename T_BIN_INNER_RING::iterator iterInnerRing = inner_rings.begin();
    for (; OB_SUCC(ret) && iterInnerRing != inner_rings.end(); ++iterInnerRing) {
      uint32_t size = iterInnerRing->size();
      uint64_t ring_len = 1 + (MAX_DIGITS_IN_DOUBLE * 2 + 1 + 1) * size + 1;
      typename T_BIN_RING::iterator iter = (*iterInnerRing).begin();
      if (OB_FAIL(buffer_.reserve(ring_len))) {
        LOG_WARN("fail to reserve memory for buffer_", K(ret), K(reserve_len));
      } else if (OB_FAIL(buffer_.append("("))) {
        LOG_WARN("fail to append buffer_", K(ret));
      }
      for (; OB_SUCC(ret) && iter != (*iterInnerRing).end(); ++iter) {
        if (OB_FAIL(appendInnerPoint(iter->template get<0>(), iter->template get<1>()))) {
          LOG_WARN("fail to appendInnerPoint", K(ret), K(iter->template get<0>()), K(iter->template get<1>()));
        } else if (OB_FAIL(buffer_.append(","))) {
          LOG_WARN("fail to append buffer_", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(buffer_.set_length(buffer_.length() - 1))) {
        LOG_WARN("fail to set buffer_ len", K(ret), K(buffer_.length()));
      } else if (OB_FAIL(buffer_.append(")"))) {
        LOG_WARN("fail to append buffer_", K(ret));
      } else if (OB_FAIL(buffer_.append(","))) {
        LOG_WARN("fail to append buffer_", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(buffer_.set_length(buffer_.length() - 1))) {
      LOG_WARN("fail to set buffer_ len", K(ret), K(buffer_.length()));
    } else if (OB_FAIL(buffer_.append(")"))) {
      LOG_WARN("fail to append buffer_", K(ret));
    } else if ((in_multi_visit_ || in_colloction_visit()) && OB_FAIL(buffer_.append(","))) {
      LOG_WARN("fail to append buffer_", K(ret));
    }
  }
  return ret;
}


template<typename T_IBIN>
int ObGeoToWktVisitor::appendMultiPrefix(T_IBIN *geo)
{
  INIT_SUCC(ret);
  const char *type_name = ObGeoTypeUtil::get_geo_name_by_type(geo->type());
  uint64_t reserve_len = strlen(type_name) + 2;
  // [type_name][(][x][ ][y][)]
  if (OB_FAIL(buffer_.reserve(reserve_len))) {
    LOG_WARN("fail to reserve memory for buffer_", K(ret), K(reserve_len));
  } else if (OB_FAIL(buffer_.append(type_name))) {
    LOG_WARN("fail to append buffer_", K(ret), K(type_name));
  } else if (OB_FAIL(buffer_.append("("))) {
    LOG_WARN("fail to append buffer_", K(ret));
  }
  return ret;
}

int ObGeoToWktVisitor::appendMultiSuffix()
{
  INIT_SUCC(ret);
  if (OB_FAIL(buffer_.set_length(buffer_.length() - 1))) {
      LOG_WARN("fail to set buffer_ len", K(ret), K(buffer_.length()));
  } else if (OB_FAIL(buffer_.append(")"))) {
    LOG_WARN("fail to append buffer_", K(ret));
  } else if ((in_colloction_visit()) && OB_FAIL(buffer_.append(","))) {
    LOG_WARN("fail to append buffer_", K(ret));
  }
  return ret;
}


template<typename T_IBIN>
int ObGeoToWktVisitor::appendCollectionPrefix(T_IBIN *geo)
{
  INIT_SUCC(ret);
  const char *type_name = ObGeoTypeUtil::get_geo_name_by_type(geo->type());
  bool is_empty = (geo->size() == 0);
  uint64_t reserve_len = strlen(type_name) + 2;
  // [type_name][(][x][ ][y][)]
  if (OB_FAIL(buffer_.reserve(reserve_len))) {
    LOG_WARN("fail to reserve memory for buffer_", K(ret), K(reserve_len));
  } else if (OB_FAIL(buffer_.append(type_name))) {
    LOG_WARN("fail to append buffer_", K(ret), K(type_name));
  } else if (is_empty && OB_FAIL(buffer_.append(" EMPTY"))) {
    LOG_WARN("fail to append buffer_", K(ret));
  } else if (!is_empty && OB_FAIL(buffer_.append("("))) {
    LOG_WARN("fail to append buffer_", K(ret));
  }
  return ret;
}

template<typename T_IBIN>
int ObGeoToWktVisitor::appendCollectionSuffix(T_IBIN *geo)
{
  INIT_SUCC(ret);
  bool is_empty = (geo->size() == 0);
  if (!is_empty) {
    if (buffer_.ptr()[buffer_.length() - 1] == ',' && OB_FAIL(buffer_.set_length(buffer_.length() - 1))) {
        LOG_WARN("fail to set buffer_ len", K(ret), K(buffer_.length()));
    } else if (OB_FAIL(buffer_.append(")"))) {
      LOG_WARN("fail to append buffer_", K(ret));
    }
  }
  colloction_level_--;
  if (OB_FAIL(ret)) {
  } else if ((in_colloction_visit()) && OB_FAIL(buffer_.append(","))) {
    LOG_WARN("fail to append buffer_", K(ret));
  }
  return ret;
}

bool ObGeoToWktVisitor::prepare(ObIWkbGeogMultiPoint *geo)
{
  UNUSED(geo);
  in_multi_visit_ = true;
  return true;
}

bool ObGeoToWktVisitor::prepare(ObIWkbGeomMultiPoint *geo)
{
  UNUSED(geo);
  in_multi_visit_ = true;
  return true;
}

bool ObGeoToWktVisitor::prepare(ObIWkbGeogMultiLineString *geo)
{
  UNUSED(geo);
  in_multi_visit_ = true;
  return true;
}

bool ObGeoToWktVisitor::prepare(ObIWkbGeomMultiLineString *geo)
{
  UNUSED(geo);
  in_multi_visit_ = true;
  return true;
}

bool ObGeoToWktVisitor::prepare(ObIWkbGeogMultiPolygon *geo)
{
  UNUSED(geo);
  in_multi_visit_ = true;
  return true;
}

bool ObGeoToWktVisitor::prepare(ObIWkbGeomMultiPolygon *geo)
{
  UNUSED(geo);
  in_multi_visit_ = true;
  return true;
}

bool ObGeoToWktVisitor::prepare(ObIWkbGeogCollection *geo)
{
  UNUSED(geo);
  colloction_level_++;
  return true;
}

bool ObGeoToWktVisitor::prepare(ObIWkbGeomCollection *geo)
{
  UNUSED(geo);
  colloction_level_++;
  return true;
}

int ObGeoToWktVisitor::visit(ObIWkbGeogPoint *geo)
{
  INIT_SUCC(ret);
  if (OB_FAIL(appendPoint<ObIWkbGeogPoint>(geo))) {
    LOG_WARN("fail to append point", K(ret));
  }
  return ret;
}

int ObGeoToWktVisitor::visit(ObIWkbGeomPoint *geo)
{
  INIT_SUCC(ret);
  if (OB_FAIL(appendPoint<ObIWkbGeomPoint>(geo))) {
    LOG_WARN("fail to append point", K(ret));
  }
  return ret;
}

int ObGeoToWktVisitor::visit(ObIWkbGeogLineString *geo)
{
  INIT_SUCC(ret);
  if (OB_FAIL((appendLine<ObIWkbGeogLineString, ObWkbGeogLineString>(geo)))) {
    LOG_WARN("fail to append line", K(ret));
  }
  return ret;
}

int ObGeoToWktVisitor::visit(ObIWkbGeomLineString *geo)
{
  INIT_SUCC(ret);
  if (OB_FAIL((appendLine<ObIWkbGeomLineString, ObWkbGeomLineString>(geo)))) {
    LOG_WARN("fail to append line", K(ret));
  }
  return ret;
}

int ObGeoToWktVisitor::visit(ObIWkbGeogMultiPoint *geo)
{
  INIT_SUCC(ret);
  if (OB_FAIL(appendMultiPrefix(geo))) {
    LOG_WARN("fail to append Multi-Prefix", K(ret));
  }
  return ret;
}

int ObGeoToWktVisitor::visit(ObIWkbGeomMultiPoint *geo)
{
  INIT_SUCC(ret);
  if (OB_FAIL(appendMultiPrefix(geo))) {
    LOG_WARN("fail to append Multi-Prefix", K(ret));
  }
  return ret;
}

int ObGeoToWktVisitor::visit(ObIWkbGeogMultiLineString *geo)
{
  INIT_SUCC(ret);
  if (OB_FAIL(appendMultiPrefix(geo))) {
    LOG_WARN("fail to append Multi-Prefix", K(ret));
  }
  return ret;
}

int ObGeoToWktVisitor::visit(ObIWkbGeomMultiLineString *geo)
{
  INIT_SUCC(ret);
  if (OB_FAIL(appendMultiPrefix(geo))) {
    LOG_WARN("fail to append Multi-Prefix", K(ret));
  }
  return ret;
}

int ObGeoToWktVisitor::visit(ObIWkbGeogPolygon *geo)
{
  INIT_SUCC(ret);
  if (OB_FAIL((appendPolygon<ObIWkbGeogPolygon, ObWkbGeogPolygon,
                             ObWkbGeogLinearRing, ObWkbGeogPolygonInnerRings>(geo)))) {
    LOG_WARN("fail to append polygon", K(ret));
  }
  return ret;
}

int ObGeoToWktVisitor::visit(ObIWkbGeomPolygon *geo)
{
  INIT_SUCC(ret);
  if (OB_FAIL((appendPolygon<ObIWkbGeomPolygon, ObWkbGeomPolygon,
                             ObWkbGeomLinearRing, ObWkbGeomPolygonInnerRings>(geo)))) {
    LOG_WARN("fail to append polygon", K(ret));
  }
  return ret;
}

int ObGeoToWktVisitor::visit(ObIWkbGeogMultiPolygon *geo)
{
  INIT_SUCC(ret);
  if (OB_FAIL(appendMultiPrefix(geo))) {
    LOG_WARN("fail to append Multi-Prefix", K(ret));
  }
  return ret;
}

int ObGeoToWktVisitor::visit(ObIWkbGeomMultiPolygon *geo)
{
  INIT_SUCC(ret);
  if (OB_FAIL(appendMultiPrefix(geo))) {
    LOG_WARN("fail to append Multi-Prefix", K(ret));
  }
  return ret;
}

int ObGeoToWktVisitor::visit(ObIWkbGeogCollection *geo)
{
  INIT_SUCC(ret);
  if (OB_FAIL(appendCollectionPrefix(geo))) {
    LOG_WARN("fail to append Collection-Prefix", K(ret));
  }
  return ret;
}

int ObGeoToWktVisitor::visit(ObIWkbGeomCollection *geo)
{
  INIT_SUCC(ret);
  if (OB_FAIL(appendCollectionPrefix(geo))) {
    LOG_WARN("fail to append Collection-Prefix", K(ret));
  }
  return ret;
}

int ObGeoToWktVisitor::finish(ObIWkbGeogMultiPoint *geo)
{
  UNUSED(geo);
  in_multi_visit_ = false;
  return appendMultiSuffix();
}

int ObGeoToWktVisitor::finish(ObIWkbGeomMultiPoint *geo)
{
  UNUSED(geo);
  in_multi_visit_ = false;
  return appendMultiSuffix();
}

int ObGeoToWktVisitor::finish(ObIWkbGeogMultiLineString *geo)
{
  UNUSED(geo);
  in_multi_visit_ = false;
  return appendMultiSuffix();
}

int ObGeoToWktVisitor::finish(ObIWkbGeomMultiLineString *geo)
{
  UNUSED(geo);
  in_multi_visit_ = false;
  return appendMultiSuffix();
}

int ObGeoToWktVisitor::finish(ObIWkbGeogMultiPolygon *geo)
{
  UNUSED(geo);
  in_multi_visit_ = false;
  return appendMultiSuffix();
}

int ObGeoToWktVisitor::finish(ObIWkbGeomMultiPolygon *geo)
{
  UNUSED(geo);
  in_multi_visit_ = false;
  return appendMultiSuffix();
}

int ObGeoToWktVisitor::finish(ObIWkbGeomCollection *geo)
{
  UNUSED(geo);
  return appendCollectionSuffix(geo);
}

int ObGeoToWktVisitor::finish(ObIWkbGeogCollection *geo)
{
  UNUSED(geo);
  return appendCollectionSuffix(geo);
}

void ObGeoToWktVisitor::get_wkt(ObString &wkt)
{
  wkt.assign(buffer_.ptr(), static_cast<int32_t>(buffer_.length()));
}

int ObGeoToWktVisitor::init(uint32_t srid, int64_t maxdecimaldigits)
{
  INIT_SUCC(ret);
  if (srid != 0) {
    ObFastFormatInt ffi(srid);
    uint64_t reserve_len = strlen("srid") + 1 + ffi.length() + 1;
    // [srid][=][1][2][3][4][;]
    if (OB_FAIL(buffer_.reserve(reserve_len))) {
      LOG_WARN("fail to reserve memory for buffer_", K(ret), K(reserve_len));
    } else if (OB_FAIL(buffer_.append("SRID="))) {
      LOG_WARN("fail to append buffer_", K(ret));
    } else if (OB_FAIL(buffer_.append(ffi.ptr(), ffi.length()))) {
      LOG_WARN("fail to append buffer_", K(ret), K(ffi.length()));
    } else if (OB_FAIL(buffer_.append(";"))) {
      LOG_WARN("fail to append buffer_", K(ret));
    }
  }

  if (maxdecimaldigits >= 0 && maxdecimaldigits < MAX_DIGITS_IN_DOUBLE) {
    scale_ = maxdecimaldigits;
    has_scale_ = true;
  }
  return ret;
}

} // namespace common
} // namespace oceanbase
