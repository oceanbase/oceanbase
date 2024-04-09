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

#define USING_LOG_PREFIX LIB
#include "lib/geo/ob_sdo_geo_func_to_wkb.h"

namespace oceanbase
{
namespace common
{
int ObSdoGeoToWkb::normalize_point(double &lon, double &lat)
{
  // If the value entered is outside LONG/LAT range,
  // the value is wrapped around to fit into the range
  INIT_SUCC(ret);
  double lat_right_margin = 90;
  double lon_right_margin = 180;

  if (lat < -90.0 || lat > 90.0) {
    // bool need_offset = (lat >= 270.0 || lat <= -270.0) ? true : false;
    bool need_offset = false;
    // wrapped around to [-90, 90]
    int64_t quad = std::floor(std::abs(lat) / lat_right_margin);
    quad %= 4;
    double pole = (lat > 0) ? 90.0 : -90.0;
    double offset = fmod(lat, lat_right_margin);
    switch(quad)
    {
      case 0: {
        lat = offset;
        need_offset = true;
        break;
      }
      case 1: {
        lat = pole - offset;
        if (offset == 0) {
          need_offset = true;
        }
        break;
      }
      case 2: {
        lat = -offset;
        break;
      }
      case 3: {
        lat = offset - pole;
        need_offset = true;
        break;
      }
      default: {
        ret = OB_ERR_GIS_INVALID_DATA;
        LOG_WARN("fail to normalize latitude", K(ret), K(lat));
      }
    }
    // wrapped around to [-180, 180]
    lon += need_offset ? 180.0 : 0.0;
    if (fmod(lon, 360.0) == 0) {
      lon = 180;
    } else if (fmod(lon, 180.0) == 0) {
      lon = 0;
    } else {
      double sign_lon = lon > 0.0 ? 1.0 : -1.0;
      lon = (fmod(abs(lon), 360.0) - 180.0) * sign_lon;
    }
  } else if (lon > 180.0 || lon < -180.0) {
    // wrapped around to [-180, 180]
    lon += 180.0;
    double sign = lon > 0.0 ? 1.0 : -1.0;
    lon = (fmod(abs(lon), 360.0) - 180.0) * sign;
    if (lon == -180) {
      lon = -lon;
    }
  }

  return ret;
}

template<typename T>
int ObSdoGeoToWkb::append_num_with_endian(T data, uint64_t len)
{
  INIT_SUCC(ret);
  char *p = reinterpret_cast<char *>(&data);
  if (iorder_ == static_cast<uint8_t>(ObGeoWkbByteOrder::BigEndian)) {
    for(uint64_t i = 0; i < len / 2; i++) {
      char tmp = p[i];
      p[i] = p[len - 1 - i];
      p[len - 1 - i] = tmp;
    }
  }

  if (OB_FAIL(buffer_.append(p, len))) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("failed to write buffer", K(ret), K(len));
  }
  return ret;
}

int ObSdoGeoToWkb::append_inner_point(double x, double y, double z, int swap_idx)
{
  INIT_SUCC(ret);
  if (swap_idx == 0) {
    double tmp = z;
    z = x;
    x = tmp;
  } else if (swap_idx == 1) {
    double tmp = z;
    z = y;
    y = tmp;
  }
  if (srs_type_ == ObSrsType::GEOGRAPHIC_SRS && need_normalize_ && OB_FAIL(normalize_point(x, y))) {
    LOG_WARN("failed to normalize point in GEOGRAPHIC_SRS", K(ret), K(x), K(y), K(srs_type_));
  } else if (OB_FAIL(append_num_with_endian(x, WKB_GEO_DOUBLE_STORED_SIZE))) {
    LOG_WARN("failed to write x value", K(ret), K(x));
  } else if (OB_FAIL(append_num_with_endian(y, WKB_GEO_DOUBLE_STORED_SIZE))) {
    LOG_WARN("failed to write y value", K(ret), K(y));
  } else if (is_3d_geo_) {
    if (isnan(z)) {
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_WARN("invalid z value", K(ret));
    } else if (OB_FAIL(append_num_with_endian(z, WKB_GEO_DOUBLE_STORED_SIZE))) {
      LOG_WARN("failed to write y value", K(ret), K(z));
    }
  }

  return ret;
}

int ObSdoGeoToWkb::append_inner_point(const ObArray<double> &ordinates, const uint64_t idx)
{
  INIT_SUCC(ret);
  uint8_t len = is_3d_geo_ ? 3 : 2;
  if (idx + len > ordinates.size()) {
    ret = OB_ERR_INVALID_DATA_IN_SDO_ORDINATE_ARRAY;
    LOG_WARN("Invalid data in the SDO_ORDINATE_ARRAY in SDO_GEOMETRY object",
        K(ret),
        K(idx),
        K(len),
        K(ordinates.size()));
  } else {
    double x = ordinates[idx];
    double y = ordinates[idx + 1];
    double z = NAN;
    if (is_3d_geo_) {
      z = ordinates[idx + 2];
    }
    if (OB_FAIL(append_inner_point(x, y, z))) {
      LOG_WARN("fail to append innerpoint to buffer_", K(ret));
    }
  }
  return ret;
}

int ObSdoGeoToWkb::append_point(ObSdoGeoObject *geo, int64_t idx)
{
  INIT_SUCC(ret);
  uint32_t wkb_type = (is_3d_geo_ && !oracle_3d_format_) ?
                      static_cast<uint32_t>(ObGeoType::POINTZ) : static_cast<uint32_t>(ObGeoType::POINT);
  uint64_t reserve_len = WKB_GEO_DOUBLE_STORED_SIZE * (is_3d_geo_ ? 3 : 2);  // x y
  reserve_len += is_multi_visit_ ? EWKB_COMMON_WKB_HEADER_LEN : WKB_COMMON_WKB_HEADER_LEN;

  if (OB_FAIL(buffer_.reserve(reserve_len))) {
    LOG_WARN("fail to reserve memory to buffer_", K(ret), K(reserve_len));
  } else if (!is_multi_visit_
             && OB_FAIL(append_num_with_endian(geo->get_srid(), WKB_GEO_ELEMENT_NUM_SIZE))) {
    LOG_WARN("fail to append srid to buffer_", K(ret), K(geo->get_srid()));
  } else if (OB_FAIL(append_num_with_endian(iorder_, WKB_GEO_BO_SIZE))) {
    LOG_WARN("fail to append order to buffer_", K(ret), K(iorder_));
  } else if (OB_FAIL(append_num_with_endian(wkb_type, WKB_GEO_TYPE_SIZE))) {
    LOG_WARN("fail to append wkb_type to buffer_", K(ret), K(wkb_type));
  } else if (idx >= 0) {
    size_t ori_size = geo->get_ordinates().size();
    if (idx >= ori_size) {
      ret = OB_ERR_INVALID_DATA_IN_SDO_ORDINATE_ARRAY;
      LOG_WARN("Invalid data in the SDO_ORDINATE_ARRAY in SDO_GEOMETRY object",
          K(ret),
          K(idx),
          K(ori_size));
    } else if (OB_FAIL(append_inner_point(geo->get_ordinates(), idx))) {
      LOG_WARN("fail to append innerpoint to buffer_", K(ret));
    }
  } else if (geo->has_point()) {
    if (OB_FAIL(append_inner_point(geo->get_point().get_x(), geo->get_point().get_y(),
                                   geo->get_point().has_z() ? geo->get_point().get_z() : NAN))) {
      LOG_WARN("fail to append innerpoint to buffer_", K(ret));
    }
  } else {
    ret = OB_ERR_INVALID_DATA_IN_SDO_ORDINATE_ARRAY;
    LOG_WARN("Invalid data in the SDO_ORDINATE_ARRAY in SDO_GEOMETRY object", K(ret), K(idx));
  }

  return ret;
}

// ccw: Counter ClockWise
int ObSdoGeoToWkb::inner_append_rectangle(double lower_left_x, double lower_left_y, double upper_right_x,
                                          double upper_right_y, uint64_t sdo_etype, bool is_ccw, double fix_z, int swap_idx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(append_inner_point(lower_left_x, lower_left_y, fix_z, swap_idx))) {
    LOG_WARN("fail to append innerpoint to buffer_", K(ret), K(lower_left_x), K(lower_left_y));
  } else if (is_ccw) {
    // append lower left -> lower right -> upper right -> upper left -> lower left
    if (OB_FAIL(append_inner_point(upper_right_x, lower_left_y, fix_z, swap_idx))) {
      LOG_WARN("fail to append innerpoint to buffer_", K(ret), K(upper_right_x), K(lower_left_y));
    } else if (OB_FAIL(append_inner_point(upper_right_x, upper_right_y, fix_z, swap_idx))) {
      LOG_WARN("fail to append innerpoint to buffer_", K(ret), K(upper_right_x), K(upper_right_y));
    } else if (OB_FAIL(append_inner_point(lower_left_x, upper_right_y, fix_z, swap_idx))) {
      LOG_WARN("fail to append innerpoint to buffer_", K(ret), K(lower_left_x), K(upper_right_y));
    }
  } else {
    // append lower left -> upper left -> upper right -> lower right -> lower left
    if (OB_FAIL(append_inner_point(lower_left_x, upper_right_y, fix_z, swap_idx))) {
      LOG_WARN("fail to append innerpoint to buffer_", K(ret), K(upper_right_x), K(lower_left_y));
    } else if (OB_FAIL(append_inner_point(upper_right_x, upper_right_y, fix_z, swap_idx))) {
      LOG_WARN("fail to append innerpoint to buffer_", K(ret), K(upper_right_x), K(upper_right_y));
    } else if (OB_FAIL(append_inner_point(upper_right_x, lower_left_y, fix_z, swap_idx))) {
      LOG_WARN("fail to append innerpoint to buffer_", K(ret), K(lower_left_x), K(upper_right_y));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(append_inner_point(lower_left_x, lower_left_y, fix_z, swap_idx))) {
    LOG_WARN("fail to append innerpoint to buffer_", K(ret), K(lower_left_x), K(lower_left_y));
  }
  return ret;
}

int ObSdoGeoToWkb::append_3D_rectangle(double x1, double y1, double x2, double y2, double fix_z,
                                      uint64_t sdo_etype, int fix_idx)
{
  int ret = OB_SUCCESS;
  bool is_ccw = true;
  if (x2 > x1 && y2 > y1) {
    is_ccw = true;
  } else if (x2 < x1 && y2 < y1) {
    is_ccw = false;
  } else {
    ret = OB_ERR_INVALID_DATA_IN_SDO_ORDINATE_ARRAY;
    LOG_WARN("Invalid data in the SDO_ORDINATE_ARRAY in SDO_GEOMETRY object", K(ret));
  }
  if (OB_SUCC(ret)) {
    is_ccw = (fix_idx == 1) ? !is_ccw : is_ccw;
    if (OB_FAIL(inner_append_rectangle(x1, y1, x2, y2, sdo_etype, is_ccw, fix_z, fix_idx))) {
      LOG_WARN("fail to do inner append rectangle", K(ret), K(x1), K(y1), K(x2), K(y2));
    }
  }
  return ret;
}

int ObSdoGeoToWkb::append_rectangle(ObSdoGeoObject *geo, size_t idx, uint64_t sdo_etype)
{
  INIT_SUCC(ret);
  size_t step = is_3d_geo_ ? 3 : 2;
  uint64_t reserve_len =
      WKB_GEO_ELEMENT_NUM_SIZE + RECTANGLE_POINT_NUM * step * WKB_GEO_DOUBLE_STORED_SIZE;
  const ObArray<double> &ori = geo->get_ordinates();
  if ((idx + 2 * step - 1) >= ori.size()) {
    ret = OB_ERR_INVALID_DATA_IN_SDO_ORDINATE_ARRAY;
    LOG_WARN("Invalid data in the SDO_ORDINATE_ARRAY in SDO_GEOMETRY object",
        K(ret),
        K(ori.size()),
        K(idx));
  } else  {
    double x1 = ori[idx];
    double x2 = ori[idx + step];
    double y1 = ori[idx + 1];
    double y2 = ori[idx + step + 1];
    // bugfix: 52443172
    // rectangle should normalize first
    if (srs_type_ == ObSrsType::GEOGRAPHIC_SRS && need_normalize_) {
      if (OB_FAIL(normalize_point(x1, y1))) {
        LOG_WARN("failed to normalize point in GEOGRAPHIC_SRS", K(ret), K(x1), K(y1), K(srs_type_));
      } else if (OB_FAIL(normalize_point(x2, y2))) {
        LOG_WARN("failed to normalize point in GEOGRAPHIC_SRS", K(ret), K(x2), K(y2), K(srs_type_));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(buffer_.reserve(reserve_len))) {
      LOG_WARN("fail to reserve memory for buffer_", K(ret), K(reserve_len));
    } else if (OB_FAIL(append_num_with_endian(RECTANGLE_POINT_NUM, WKB_GEO_ELEMENT_NUM_SIZE))) {
      LOG_WARN("fail to append point num buffer_", K(ret));
    } else if (is_3d_geo_) {
      // a legal 3D rectangle should have only one same axis
      double z1 = ori[idx + 2];
      double z2 = ori[idx + step + 2];
      int32_t same_axis_num = x1 == x2 ? 1 : 0;
      same_axis_num = y1 == y2 ? same_axis_num + 1: same_axis_num;
      same_axis_num = z1 == z2 ? same_axis_num + 1 : same_axis_num;
      if (same_axis_num != 1) {
        ret = OB_ERR_INVALID_DATA_IN_SDO_ORDINATE_ARRAY;
        LOG_WARN("Invalid data in the SDO_ORDINATE_ARRAY in SDO_GEOMETRY object",
            K(ret),
            K(same_axis_num));
      } else if (x1 == x2 && OB_FAIL(append_3D_rectangle(y1, z1, y2, z2, x1, sdo_etype, 0))) {
        LOG_WARN("fail to append 3D rectangle", K(ret));
      } else if (y1 == y2 && OB_FAIL(append_3D_rectangle(x1, z1, x2, z2, y1, sdo_etype, 1))) {
        LOG_WARN("fail to append 3D rectangle", K(ret));
      } else if (z1 == z2 && OB_FAIL(append_3D_rectangle(x1, y1, x2, y2, z1, sdo_etype, 2))) {
        LOG_WARN("fail to append 3D rectangle", K(ret));
      }
    } else {
      if (OB_FAIL(inner_append_rectangle(x1, y1, x2, y2, sdo_etype, sdo_etype == EXTRING_ETYPE))) {
        LOG_WARN("fail to do inner append rectangle", K(ret), K(x1), K(y1), K(x2), K(y2));
      }
    }
  }
  return ret;
}

int ObSdoGeoToWkb::append_inner_ring(ObSdoGeoObject *geo, size_t ori_begin, size_t ori_end)
{
  INIT_SUCC(ret);
  uint64_t reserve_len = WKB_GEO_ELEMENT_NUM_SIZE;
  reserve_len += (ori_end - ori_begin) * WKB_GEO_DOUBLE_STORED_SIZE;
  size_t step = is_3d_geo_ ? 3 : 2;
  uint32_t num_points = (ori_end - ori_begin + step - 1) / step;

  if (ori_begin >= geo->get_ordinates().size() || ((ori_end - ori_begin) % step)) {
    ret = OB_ERR_INVALID_DATA_IN_SDO_ORDINATE_ARRAY;
    LOG_WARN("Invalid data in the SDO_ORDINATE_ARRAY in SDO_GEOMETRY object",
        K(ret),
        K(geo->get_ordinates().size()),
        K(ori_begin),
        K(ori_end));
  } else if (OB_FAIL(buffer_.reserve(reserve_len))) {
    LOG_WARN("fail to reserve memory for buffer_", K(ret), K(reserve_len));
  } else if (OB_FAIL(append_num_with_endian(num_points, WKB_GEO_ELEMENT_NUM_SIZE))) {
    LOG_WARN("fail to append buffer_", K(ret), K(num_points));
  } else {
    // x y
    const ObArray<double> &ordinates = geo->get_ordinates();
    for (size_t i = 0; OB_SUCC(ret) && i < num_points; ++i) {
      if (OB_FAIL(append_inner_point(ordinates, i * step + ori_begin))) {
        LOG_WARN("fail to append_inner_point", K(ret));
      }
    }
  }

  return ret;
}

int ObSdoGeoToWkb::append_linestring(ObSdoGeoObject *geo, size_t ori_begin, size_t ori_end)
{
  INIT_SUCC(ret);
  uint32_t wkb_type = (is_3d_geo_ && !oracle_3d_format_) ?
                      static_cast<uint32_t>(ObGeoType::LINESTRINGZ) : static_cast<uint32_t>(ObGeoType::LINESTRING);
  uint64_t reserve_len = is_multi_visit_ ? EWKB_COMMON_WKB_HEADER_LEN : WKB_COMMON_WKB_HEADER_LEN;
  size_t step = is_3d_geo_ ? 3 : 2;
  uint32_t num_points = (ori_end - ori_begin + step - 1) / step;

  if ((ori_begin >= geo->get_ordinates().size()) || (num_points < 1)) {
    ret = OB_ERR_INVALID_DATA_IN_SDO_ORDINATE_ARRAY;
    LOG_WARN("Invalid data in the SDO_ORDINATE_ARRAY in SDO_GEOMETRY object",
        K(ret),
        K(geo->get_ordinates().size()),
        K(ori_begin),
        K(ori_end));
  } else if (OB_FAIL(buffer_.reserve(reserve_len))) {
    LOG_WARN("fail to reserve memory for buffer_", K(ret), K(reserve_len));
  } else if (!is_multi_visit_
             && OB_FAIL(append_num_with_endian(geo->get_srid(), WKB_GEO_ELEMENT_NUM_SIZE))) {
    LOG_WARN("fail to append buffer_", K(ret));
  } else if (OB_FAIL(append_num_with_endian(iorder_, WKB_GEO_BO_SIZE))) {
    LOG_WARN("fail to append buffer_", K(ret));
  } else if (OB_FAIL(append_num_with_endian(wkb_type, WKB_GEO_TYPE_SIZE))) {
    LOG_WARN("fail to append buffer_", K(ret), K(wkb_type));
  } else if (OB_FAIL(append_inner_ring(geo, ori_begin, ori_end))) {
    LOG_WARN("fail to append inner ring", K(ret));
  }

  return ret;
}

int ObSdoGeoToWkb::append_polygon(ObSdoGeoObject *geo, size_t elem_begin, size_t elem_end)
{
  INIT_SUCC(ret);
  const ObArray<uint64_t> &elem_info = geo->get_elem_info();
  if (elem_end > elem_info.size() || elem_begin >= elem_end) {
    ret = OB_ERR_INVALID_DATA_IN_SDO_ELEM_INFO_ARRAY;
    LOG_WARN("Invalid data in the SDO_ORDINATE_ARRAY in SDO_GEOMETRY object",
        K(ret),
        K(elem_info.size()),
        K(elem_begin),
        K(elem_end));
  } else if (!is_multi_visit_) {
    if (OB_FAIL(buffer_.reserve(WKB_GEO_SRID_SIZE))) {
      LOG_WARN("fail to reserve memory for buffer_", K(ret));
    } else if (OB_FAIL(append_num_with_endian(geo->get_srid(), WKB_GEO_SRID_SIZE))) {
      LOG_WARN("fail to append buffer_", K(ret), K(geo->get_srid()));
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (elem_info[elem_begin + 1] != EXTRING_ETYPE) {
    ret = OB_ERR_INVALID_DATA_IN_SDO_ELEM_INFO_ARRAY;
    LOG_WARN("the first ring in polygon should be exterior ring", K(ret), K(elem_info[elem_begin + 1]));
  } else {
    int32_t wkb_type = (is_3d_geo_ && !oracle_3d_format_) ?
                       static_cast<uint32_t>(ObGeoType::POLYGONZ) : static_cast<uint32_t>(ObGeoType::POLYGON);
    uint64_t reserve_len = WKB_GEO_ELEMENT_NUM_SIZE + EWKB_COMMON_WKB_HEADER_LEN;
    uint32_t num_rings = (elem_end - elem_begin) / 3;

    if (num_rings < 1) {
      ret = OB_ERR_INVALID_DATA_IN_SDO_ORDINATE_ARRAY;
      LOG_WARN("invalid input arguments", K(ret), K(num_rings), K(is_multi_visit_));
    } else if (OB_FAIL(buffer_.reserve(reserve_len))) {
      LOG_WARN("fail to reserve memory for buffer_", K(ret), K(reserve_len));
    } else if (OB_FAIL(append_num_with_endian(iorder_, WKB_GEO_BO_SIZE))) {
      LOG_WARN("fail to append buffer_", K(ret));
    } else if (OB_FAIL(append_num_with_endian(wkb_type, WKB_GEO_TYPE_SIZE))) {
      LOG_WARN("fail to append buffer_", K(ret), K(wkb_type));
    } else if (OB_FAIL(append_num_with_endian(num_rings, WKB_GEO_ELEMENT_NUM_SIZE))) {
      LOG_WARN("fail to append buffer_", K(ret), K(num_rings));
    }
  }

  size_t ori_end = 0;
  for (size_t i = elem_begin; OB_SUCC(ret) && i < elem_end; i += 3) {
    uint64_t sdo_etype = elem_info[i + 1];
    uint64_t expected_etype = (i == elem_begin) ? EXTRING_ETYPE : INNERRING_ETYPE;
    if (sdo_etype != expected_etype) {
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_WARN("polygon ring with wrong sdo_etype", K(ret), K(sdo_etype), K(expected_etype));
    } else {
      size_t ori_begin = elem_info[i] - 1;
      if (elem_end >= elem_info.size()) {
        ori_end = (i + 3) < elem_end ? (elem_info[i + 3] - 1) : geo->get_ordinates().size();
      } else {
        ori_end = (i + 3) <= elem_end ? (elem_info[i + 3] - 1) : geo->get_ordinates().size();
      }
      size_t step = is_3d_geo_ ? 3 : 2;
      uint32_t num_points = (ori_end - ori_begin + step - 1) / step;
      uint64_t sdo_interpre = elem_info[i + 2];
      if (sdo_interpre == 3) {
        if (num_points != 2) {
          ret = OB_ERR_INVALID_DATA_IN_SDO_ELEM_INFO_ARRAY;
          LOG_WARN("invalid input arguments", K(ret), K(ori_begin), K(ori_end), K(is_3d_geo_));
        } else if (OB_FAIL(append_rectangle(geo, ori_begin, sdo_etype))) {
          LOG_WARN("fail to append Rectangle", K(ret), K(ori_begin));
        }
      } else if (num_points <= 0 || sdo_interpre != 1) {
        ret = OB_ERR_INVALID_DATA_IN_SDO_ORDINATE_ARRAY;
        LOG_WARN("invalid input arguments", K(ret), K(ori_begin), K(ori_end));
      } else if (OB_FAIL(append_inner_ring(geo, ori_begin, ori_end))) {
        LOG_WARN("fail to append inner ring", K(ret), K(ori_begin), K(ori_end));
      }
    }
  }

  return ret;
}

int ObSdoGeoToWkb::append_multi_point(ObSdoGeoObject *geo, size_t elem_begin, size_t elem_end)
{
  INIT_SUCC(ret);
  uint32_t num_points = 0;
  const ObArray<uint64_t> &elem_info = geo->get_elem_info();
  for (size_t i = elem_begin; i < elem_end; i += 3) {
    num_points += elem_info[i + 2];
  }
  size_t step = is_3d_geo_ ? 3 : 2;
  if ((elem_end - elem_begin) % 3 != 0) {
    ret = OB_ERR_INVALID_DATA_IN_SDO_ORDINATE_ARRAY;
    LOG_WARN("Invalid data in the SDO_ORDINATE_ARRAY in SDO_GEOMETRY object",
        K(ret),
        K(geo->get_ordinates().size()));
  } else if (num_points == 1 && is_deduce_dim_) {
    // bugfix:
    if (geo->get_ordinates().size() > step) {
      ret = OB_ERR_INVALID_DATA_IN_SDO_ELEM_INFO_ARRAY;
      LOG_WARN("Invalid data in the SDO_ELEM_INFO_ARRAY in SDO_GEOMETRY object", K(ret), K(step), K(geo->get_ordinates().size()));
    } else if (OB_FAIL(append_point(geo, elem_info[elem_begin] - 1))) {
      LOG_WARN("fail to append point");
    }
  } else {
    uint32_t wkb_type = (is_3d_geo_ && !oracle_3d_format_) ?
                    static_cast<uint32_t>(ObGeoType::MULTIPOINTZ) : static_cast<uint32_t>(ObGeoType::MULTIPOINT);
    uint64_t reserve_len = WKB_GEO_ELEMENT_NUM_SIZE;
    reserve_len += is_multi_visit_ ? EWKB_COMMON_WKB_HEADER_LEN : WKB_COMMON_WKB_HEADER_LEN;
    if (OB_FAIL(buffer_.reserve(reserve_len))) {
      LOG_WARN("fail to reserve memory for buffer_", K(ret), K(reserve_len));
    } else if (!is_multi_visit_
              && OB_FAIL(append_num_with_endian(geo->get_srid(), WKB_GEO_ELEMENT_NUM_SIZE))) {
      LOG_WARN("fail to append srid to buffer_", K(ret), K(geo->get_srid()));
    } else if (OB_FAIL(append_num_with_endian(iorder_, WKB_GEO_BO_SIZE))) {
      LOG_WARN("fail to append order to buffer_", K(ret), K(iorder_));
    } else if (OB_FAIL(append_num_with_endian(wkb_type, WKB_GEO_TYPE_SIZE))) {
      LOG_WARN("fail to append buffer_", K(ret), K(wkb_type));
    } else if (OB_FAIL(append_num_with_endian(num_points, WKB_GEO_ELEMENT_NUM_SIZE))) {
      LOG_WARN("fail to append buffer_", K(ret), K(wkb_type));
    } else {
      // not sub geom in collection, but still a multi visit
      if (!is_multi_visit_) {
        is_multi_visit_ = true;
      }
      size_t idx = 0;
      for (size_t i = elem_begin; OB_SUCC(ret) && i < elem_end; i += 3) {
        uint32_t num_pt = elem_info[i + 2];
        size_t ori_begin = elem_info[i] - 1;
        for (size_t j = 0; OB_SUCC(ret) && j < num_pt; ++j) {
          idx = ori_begin + j * step;
          if (OB_FAIL(append_point(geo, idx))) {
            LOG_WARN("fail to append_point", K(ret), K(j));
          }
        }
      }
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (elem_end >= elem_info.size() && (idx + step) != geo->get_ordinates().size()) {
        ret = OB_ERR_INVALID_DATA_IN_SDO_ELEM_INFO_ARRAY;
        LOG_WARN("Invalid data in the SDO_ELEM_INFO_ARRAY in SDO_GEOMETRY object", K(ret), K(idx), K(step));
      }
    }
  }

  return ret;
}

int ObSdoGeoToWkb::append_multi_linestring(ObSdoGeoObject *geo)
{
  INIT_SUCC(ret);
  uint32_t wkb_type = (is_3d_geo_ && !oracle_3d_format_) ?
                      static_cast<uint32_t>(ObGeoType::MULTILINESTRINGZ) : static_cast<uint32_t>(ObGeoType::MULTILINESTRING);
  uint64_t reserve_len = WKB_GEO_ELEMENT_NUM_SIZE + WKB_COMMON_WKB_HEADER_LEN;
  uint32_t num_linestring = geo->get_elem_info().size() / 3;

  if (num_linestring < 1) {
    ret = OB_ERR_INVALID_DATA_IN_SDO_ORDINATE_ARRAY;
    LOG_WARN("invalid input arguments", K(ret), K(num_linestring));
  } else if (OB_FAIL(buffer_.reserve(reserve_len))) {
    LOG_WARN("fail to reserve memory for buffer_", K(ret), K(reserve_len));
  } else if (OB_FAIL(append_num_with_endian(geo->get_srid(), WKB_GEO_ELEMENT_NUM_SIZE))) {
    LOG_WARN("fail to append srid to buffer_", K(ret), K(geo->get_srid()));
  } else if (OB_FAIL(append_num_with_endian(iorder_, WKB_GEO_BO_SIZE))) {
    LOG_WARN("fail to append order to buffer_", K(ret), K(iorder_));
  } else if (OB_FAIL(append_num_with_endian(wkb_type, WKB_GEO_TYPE_SIZE))) {
    LOG_WARN("fail to append buffer_", K(ret), K(wkb_type));
  } else if (OB_FAIL(append_num_with_endian(num_linestring, WKB_GEO_ELEMENT_NUM_SIZE))) {
    LOG_WARN("fail to append buffer_", K(ret), K(num_linestring));
  } else {
    // x y
    is_multi_visit_ = true;
    const ObArray<uint64_t> &elem_info = geo->get_elem_info();
    for (size_t i = 0; OB_SUCC(ret) && i < elem_info.size(); i += 3) {
      size_t ori_begin = elem_info[i] - 1;
      size_t ori_end =
          (i + 3) < elem_info.size() ? (elem_info[i + 3] - 1) : geo->get_ordinates().size();
      if (OB_FAIL(append_linestring(geo, ori_begin, ori_end))) {
        LOG_WARN("fail to append linestring", K(ret), K(ori_begin), K(ori_end));
      }
    }
  }

  return ret;
}

int ObSdoGeoToWkb::append_multi_polygon(ObSdoGeoObject *geo)
{
  INIT_SUCC(ret);
  uint32_t wkb_type = (is_3d_geo_ && !oracle_3d_format_) ?
                      static_cast<uint32_t>(ObGeoType::MULTIPOLYGONZ) : static_cast<uint32_t>(ObGeoType::MULTIPOLYGON);
  uint64_t reserve_len = WKB_GEO_ELEMENT_NUM_SIZE + WKB_COMMON_WKB_HEADER_LEN;
  uint32_t num_polygon = 0;
  const ObArray<uint64_t> &elem_info = geo->get_elem_info();
  if (elem_info.size() % 3) {
    ret = OB_ERR_INVALID_DATA_IN_SDO_ELEM_INFO_ARRAY;
    LOG_WARN("invalid size of elem_info", K(ret), K(elem_info.size()));
  } else {
    for (size_t i = 1; i < elem_info.size(); i += 3) {
      if (elem_info[i] == 1003) {
        num_polygon++;  // ext_ring
      }
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (num_polygon < 1) {
    ret = OB_ERR_INVALID_DATA_IN_SDO_ORDINATE_ARRAY;
    LOG_WARN("invalid input arguments", K(ret), K(num_polygon));
  } else if (OB_FAIL(buffer_.reserve(reserve_len))) {
    LOG_WARN("fail to reserve memory for buffer_", K(ret), K(reserve_len));
  } else if (OB_FAIL(append_num_with_endian(geo->get_srid(), WKB_GEO_ELEMENT_NUM_SIZE))) {
    LOG_WARN("fail to append srid to buffer_", K(ret), K(geo->get_srid()));
  } else if (OB_FAIL(append_num_with_endian(iorder_, WKB_GEO_BO_SIZE))) {
    LOG_WARN("fail to append order to buffer_", K(ret), K(iorder_));
  } else if (OB_FAIL(append_num_with_endian(wkb_type, WKB_GEO_TYPE_SIZE))) {
    LOG_WARN("fail to append buffer_", K(ret), K(wkb_type));
  } else if (OB_FAIL(append_num_with_endian(num_polygon, WKB_GEO_ELEMENT_NUM_SIZE))) {
    LOG_WARN("fail to append buffer_", K(ret), K(num_polygon));
  } else {
    // x y
    is_multi_visit_ = true;
    for (size_t i = 0; OB_SUCC(ret) && i < elem_info.size();) {
      size_t elem_begin = i;
      bool start_with_ext_ring = false;
      if (elem_info[elem_begin + 1] == EXTRING_ETYPE) {
        start_with_ext_ring = true;
      }
      for (i += 3; i < elem_info.size() && elem_info[i + 1] == INNERRING_ETYPE; i += 3) {
        // to find next ext ring
      }
      if (start_with_ext_ring) {
        if (OB_FAIL(append_polygon(geo, elem_begin, i))) {
          LOG_WARN("fail to append linestring", K(ret), K(elem_begin), K(i));
        }
      } else {
        if (OB_FAIL(append_polygon(geo, elem_begin, i + 3))) {
          LOG_WARN("fail to append linestring", K(ret), K(elem_begin), K(i));
        }
      }
    }
  }

  return ret;
}

int ObSdoGeoToWkb::append_collection(ObSdoGeoObject *geo)
{
  INIT_SUCC(ret);
  uint32_t wkb_type = (is_3d_geo_ && !oracle_3d_format_) ?
                      static_cast<uint32_t>(ObGeoType::GEOMETRYCOLLECTIONZ) : static_cast<uint32_t>(ObGeoType::GEOMETRYCOLLECTION);
  uint64_t reserve_len = WKB_GEO_ELEMENT_NUM_SIZE + WKB_COMMON_WKB_HEADER_LEN;
  uint32_t num_wkb = 0;

  if (OB_FAIL(buffer_.reserve(reserve_len))) {
    LOG_WARN("fail to reserve memory for buffer_", K(ret), K(reserve_len));
  } else if (OB_FAIL(append_num_with_endian(geo->get_srid(), WKB_GEO_ELEMENT_NUM_SIZE))) {
    LOG_WARN("fail to append srid to buffer_", K(ret), K(geo->get_srid()));
  } else if (OB_FAIL(append_num_with_endian(iorder_, WKB_GEO_BO_SIZE))) {
    LOG_WARN("fail to append order to buffer_", K(ret), K(iorder_));
  } else if (OB_FAIL(append_num_with_endian(wkb_type, WKB_GEO_TYPE_SIZE))) {
    LOG_WARN("fail to append buffer_", K(ret), K(wkb_type));
  } else if (OB_FAIL(append_num_with_endian(num_wkb, WKB_GEO_ELEMENT_NUM_SIZE))) {
    LOG_WARN("fail to append buffer_", K(ret), K(num_wkb));
  } else {
    is_multi_visit_ = true;
    const ObArray<uint64_t> &elem_info = geo->get_elem_info();
    for (size_t i = 0; OB_SUCC(ret) && i < elem_info.size(); i += 3) {
      ++num_wkb;
      size_t ori_begin = elem_info[i] - 1;
      size_t ori_end =
          (i + 3) < elem_info.size() ? (elem_info[i + 3] - 1) : geo->get_ordinates().size();
      ObGeoType type;
      if (OB_FAIL(
              ObGeoTypeUtil::geo_type_in_collection(elem_info[i + 1], elem_info[i + 2], type))) {
        LOG_WARN("unsupported sdo type", K(ret), K(elem_info[i]), K(elem_info[i + 1]), K(type));
      } else if (type == ObGeoType::POINT && OB_FAIL(append_point(geo, ori_begin))) {
        LOG_WARN("fail to append point", K(ret), K(ori_begin));
      } else if (type == ObGeoType::MULTIPOINT
                 && OB_FAIL(append_multi_point(geo, i, i + 3))) {

        LOG_WARN("fail to append multi point", K(ret), K(ori_end));
      } else if (type == ObGeoType::LINESTRING
                 && OB_FAIL(append_linestring(geo, ori_begin, ori_end))) {
        LOG_WARN("fail to append linestring", K(ret), K(ori_end));
      } else if (type == ObGeoType::POLYGON) {
        size_t elem_begin = i;
        bool start_with_ext_ring = false;
        if (elem_info[elem_begin + 1] == EXTRING_ETYPE) {
          start_with_ext_ring = true;
        }
        for (i += 3; i < elem_info.size() && elem_info[i + 1] == INNERRING_ETYPE; i += 3) {
          // to find next ext ring
        }
        if (start_with_ext_ring) {
          if (OB_FAIL(append_polygon(geo, elem_begin, i))) {
            LOG_WARN("fail to append linestring", K(ret), K(elem_begin), K(i));
          } else {
            i -= 3;  // next for i += 3
          }
        } else {
          if (OB_FAIL(append_polygon(geo, elem_begin, i + 3))) {
            LOG_WARN("fail to append linestring", K(ret), K(elem_begin), K(i));
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    // change num_wkb
    ObArenaAllocator tmp_allocator;
    ObWkbBuffer tmp_buffer(tmp_allocator, static_cast<ObGeoWkbByteOrder>(iorder_));
    if (OB_FAIL(tmp_buffer.reserve(WKB_GEO_ELEMENT_NUM_SIZE))) {
      LOG_WARN("fail to reserve memory for tmp_buffer", K(ret));
    } else if (OB_FAIL(tmp_buffer.write(tmp_buffer.length(), num_wkb))) {
      LOG_WARN("fail to write tmp_buffer", K(ret), K(num_wkb));
    } else {
      MEMCPY(buffer_.ptr() + WKB_COMMON_WKB_HEADER_LEN, tmp_buffer.ptr(), WKB_GEO_ELEMENT_NUM_SIZE);
    }
  }

  return ret;
}

void ObSdoGeoToWkb::reset()
{
  is_multi_visit_ = false;
  buffer_.reset();
}

int ObSdoGeoToWkb::translate(
    ObSdoGeoObject *geo, ObString &wkb, bool with_srid, ObGeoWkbByteOrder order)
{
  INIT_SUCC(ret);
  iorder_ = static_cast<uint8_t>(order);
  uint64_t elem_sz = geo->get_elem_info().size();
  is_3d_geo_ = geo->is_3d_geo();

  switch (geo->get_gtype()) {
    case ObGeoType::POINT:
    case ObGeoType::POINTZ: {
      uint64_t ori_size = geo->get_ordinates().size();
      if (elem_sz >= 3 && ori_size > 0) {
        // use elem_info and sdo_ordinate
        if (geo->get_elem_info()[2] > 1 ||
            (geo->get_gtype() == ObGeoType::POINT && ori_size != 2) ||
            (geo->get_gtype() == ObGeoType::POINTZ && ori_size != 3)) {
          ret = OB_ERR_INVALID_DATA_IN_SDO_ELEM_INFO_ARRAY;
          LOG_WARN("Invalid data in the SDO_ORDINATE_ARRAY in SDO_GEOMETRY object", K(ret), K(elem_sz));
        } else {
          ret = append_point(geo, geo->get_elem_info()[0] - 1);
        }
      } else if (geo->has_point()) {
        // use sdo_point
        ret = append_point(geo, -1);
      } else {
        ret = OB_ERR_INVALID_DATA_IN_SDO_ELEM_INFO_ARRAY;
        LOG_WARN("sdo_geometry has not point and elem_info", K(ret));
      }
      break;
    }

    case ObGeoType::LINESTRING:
    case ObGeoType::LINESTRINGZ: {
      if (elem_sz != 3) {
        ret = OB_ERR_INVALID_DATA_IN_SDO_ELEM_INFO_ARRAY;
        LOG_WARN("sdo_geometry has not point and elem_info", K(ret));
      } else {
        ret = append_linestring(geo, geo->get_elem_info()[0] - 1, geo->get_ordinates().size());
      }
      break;
    }

    case ObGeoType::POLYGON:
    case ObGeoType::POLYGONZ: {
      ret = append_polygon(geo, 0, elem_sz);
      break;
    }

    case ObGeoType::GEOMETRYCOLLECTION:
    case ObGeoType::GEOMETRYCOLLECTIONZ: {
      ret = append_collection(geo);
      break;
    }

    case ObGeoType::MULTIPOINT:
    case ObGeoType::MULTIPOINTZ: {
      if (elem_sz < 3) {
        ret = OB_ERR_INVALID_DATA_IN_SDO_ELEM_INFO_ARRAY;
        LOG_WARN("sdo_geometry has not point and elem_info", K(ret));
      } else {
        ret = append_multi_point(geo, 0, geo->get_elem_info().size());
      }
      break;
    }

    case ObGeoType::MULTILINESTRING:
    case ObGeoType::MULTILINESTRINGZ: {
      ret = append_multi_linestring(geo);
      break;
    }

    case ObGeoType::MULTIPOLYGON:
    case ObGeoType::MULTIPOLYGONZ: {
      ret = append_multi_polygon(geo);
      break;
    }

    default: {
      ret = OB_ERR_INVALID_GTYPE_IN_SDO_GEROMETRY;
      LOG_WARN("invalid geo type", K(ret), K(geo->get_gtype()));
      break;
    }
  }

  if (OB_SUCC(ret)) {
    if (with_srid) {
      wkb.assign(buffer_.ptr(), static_cast<int32_t>(buffer_.length()));
    } else {
      wkb.assign(buffer_.ptr() + WKB_GEO_SRID_SIZE,
          static_cast<int32_t>(buffer_.length() - WKB_GEO_SRID_SIZE));
    }
  }

  return ret;
}

}  // namespace common
}  // namespace oceanbase
