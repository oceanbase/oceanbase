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
 * This file contains implementation support for the sdo geometry object abstraction.
 */
#define USING_LOG_PREFIX LIB
#include "ob_sdo_geo_object.h"
#include "ob_geo_utils.h"

namespace oceanbase
{
namespace common
{

bool ObSdoPoint::operator==(const ObSdoPoint &other) const
{
  bool bret = false;
  if (is_null_ == other.is_null() && has_z_ == other.has_z()) {
    if (fabs(x_ - other.get_x()) < OB_DOUBLE_EPSINON &&
        fabs(y_ - other.get_y()) < OB_DOUBLE_EPSINON &&
        (!has_z_ || fabs(z_ - other.get_z()) < OB_DOUBLE_EPSINON)) {
      bret = true;
    }
  }
  return bret;
}

int ObSdoPoint::to_text(ObStringBuffer &buf)
{
  int ret = OB_SUCCESS;
  int64_t len = 0;
  char number_buf[128] = {0};
  ObString format_str_x = need_sci_format(x_) ? "%.4E" : "%.9g";
  ObString format_str_y = need_sci_format(y_) ? "%.4E" : "%.9g";
  ObString format_str_z = need_sci_format(z_) ? "%.4E" : "%.9g";
  if (is_null_) {
    if (OB_FAIL(buf.append("NULL"))) {
      LOG_WARN("fail to print null", K(ret));
    }
  } else if (OB_FAIL(buf.append("SDO_POINT_TYPE"))) {
    LOG_WARN("fail to print point type start", K(ret));
  } else if (OB_FAIL(buf.append("("))){
    LOG_WARN("fail to print (", K(ret));
  } else if ((len = snprintf(number_buf, 128, format_str_x.ptr(), x_)) < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to val to string", K(ret), K(x_));
  } else if (OB_FAIL(buf.append(number_buf, len))) {
    LOG_WARN("fail to print gtype", K(ret));
  } else if (OB_FAIL(buf.append(", "))) {
    LOG_WARN("fail to print ,", K(ret));
  } else if ((len = snprintf(number_buf, 128, format_str_y.ptr(), y_)) < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to val to string", K(ret), K(y_));
  } else if (OB_FAIL(buf.append(number_buf, len))) {
    LOG_WARN("fail to print gtype", K(ret));
  } else if (OB_FAIL(buf.append(", "))) {
    LOG_WARN("fail to print ,", K(ret));
  } else if (has_z_) {
    if ((len = snprintf(number_buf, 128, format_str_z.ptr(), z_)) < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to val to string", K(ret), K(x_));
    } else if (OB_FAIL(buf.append(number_buf, len))) {
      LOG_WARN("fail to print gtype", K(ret));
    } else if (OB_FAIL(buf.append(")"))){
      LOG_WARN("fail to print (", K(ret));
    }
  } else if (OB_FAIL(buf.append("NULL"))) {
    LOG_WARN("fail to print null", K(ret));
  } else if (OB_FAIL(buf.append(")"))){
    LOG_WARN("fail to print (", K(ret));
  }
  return ret;
}

int ObSdoGeoObject::to_text(ObStringBuffer &buf)
{
  int ret = OB_SUCCESS;
  uint64_t gtype_num;
  int64_t len = 0;
  char number_buf[128] = {0};
  if (OB_FAIL(buf.append("SDO_GEOMETRY"))) {
    LOG_WARN("fail to print point type start", K(ret));
  } else if (OB_FAIL(buf.append("("))){
    LOG_WARN("fail to print (", K(ret));
  } else if (OB_FAIL(ObGeoTypeUtil::get_num_by_gtype(gtype_, gtype_num))) {
    LOG_WARN("fail to get_num_by_gtype", K(ret), K(gtype_), K(gtype_num));
  } else if ((len = snprintf(number_buf, 128, "%lu", gtype_num)) < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to val to string", K(ret));
  } else if (OB_FAIL(buf.append(number_buf, len))) {
    LOG_WARN("fail to print gtype", K(ret));
  } else if (OB_FAIL(buf.append(", "))) {
    LOG_WARN("fail to print ,", K(ret));
  } else if (srid_ == UINT32_MAX && OB_FAIL(buf.append("NULL"))) {
    LOG_WARN("fail to print srid", K(ret));
  } else if (srid_ != UINT32_MAX &&
             ((len = snprintf(number_buf, 128, "%u", srid_)) < 0 ||
              OB_FAIL(buf.append(number_buf, len)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to val to string", K(ret), K(len));
  } else if (OB_FAIL(buf.append(", "))) {
    LOG_WARN("fail to print ,", K(ret));
  } else if (OB_FAIL(point_.to_text(buf))) {
    LOG_WARN("fail to print point", K(ret));
  } else if (OB_FAIL(buf.append(", "))) {
    LOG_WARN("fail to print ,", K(ret));
  } else if (elem_info_.count() == 0) {
    if (OB_FAIL(buf.append("NULL"))) {
      LOG_WARN("fail to print null", K(ret));
    }
  } else {
    if (OB_FAIL(buf.append("SDO_ELEM_INFO_ARRAY"))) {
      LOG_WARN("fail to print point type start", K(ret));
    } else if (OB_FAIL(buf.append("("))){
      LOG_WARN("fail to print (", K(ret));
    }
    for (uint64_t i = 0; i < elem_info_.count() && OB_SUCC(ret); i++) {
      if ((len = snprintf(number_buf, 128, "%lu", elem_info_.at(i))) < 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to val to string", K(ret));
      } else if (OB_FAIL(buf.append(number_buf, len))) {
        LOG_WARN("fail to print gtype", K(ret));
      } else if (i + 1 != elem_info_.count() && OB_FAIL(buf.append(", "))) {
        LOG_WARN("fail to print ,", K(ret));
      } else if (i + 1 == elem_info_.count() && OB_FAIL(buf.append(")"))) {
        LOG_WARN("fail to print )", K(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(buf.append(", "))) {
    LOG_WARN("fail to print ,", K(ret));
  } else if (ordinates_.count() == 0) {
    if (OB_FAIL(buf.append("NULL"))) {
      LOG_WARN("fail to print null", K(ret));
    }
  } else {
    if (OB_FAIL(buf.append("SDO_ORDINATE_ARRAY"))) {
      LOG_WARN("fail to print point type start", K(ret));
    } else if (OB_FAIL(buf.append("("))){
      LOG_WARN("fail to print (", K(ret));
    }
    for (uint64_t i = 0; i < ordinates_.count() && OB_SUCC(ret); i++) {
      ObString format_str = need_sci_format(ordinates_.at(i)) ? "%.4E" : "%.9g";
      if ((len = snprintf(number_buf, 128, format_str.ptr(), ordinates_.at(i))) < 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to val to string", K(ret));
      } else if (OB_FAIL(buf.append(number_buf, len))) {
        LOG_WARN("fail to print gtype", K(ret));
      } else if (i + 1 != ordinates_.count() && OB_FAIL(buf.append(", "))) {
        LOG_WARN("fail to print ,", K(ret));
      } else if (i + 1 == ordinates_.count() && OB_FAIL(buf.append(")"))) {
        LOG_WARN("fail to print )", K(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(buf.append(")"))) {
    LOG_WARN("fail to print ,", K(ret));
  }
  return ret;
}

bool ObSdoGeoObject::operator==(const ObSdoGeoObject &other) const
{
  bool bret = false;
  if (gtype_ == other.get_gtype() && srid_ == other.get_srid() &&
      point_ == other.get_point() && elem_info_.count() == other.get_elem_info().count() &&
      ordinates_.count() == other.get_ordinates().count()) {
    bret = true;
    for (int64_t i = 0; i < elem_info_.count() && bret; i++) {
      if (elem_info_[i] != other.get_elem_info()[i]) {
        bret = false;
      }
    }
    for (int64_t i = 0; i < ordinates_.count() && bret; i++) {
      if (fabs(ordinates_[i] - other.get_ordinates()[i]) > OB_DOUBLE_EPSINON) {
        bret = false;
      }
    }
  }
  return bret;
}

}  // namespace common
}  // namespace oceanbase