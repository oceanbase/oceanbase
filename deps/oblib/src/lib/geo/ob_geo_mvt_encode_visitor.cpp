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
#include "ob_geo_mvt_encode_visitor.h"

namespace oceanbase {
namespace common {

int ObGeoMvtEncodeVisitor::visit(ObIWkbGeomPoint *geo)
{
  int ret = OB_SUCCESS;
  int32_t x = static_cast<int32_t>(geo->x()) - curr_x_;
  int32_t y = static_cast<int32_t>(geo->y()) - curr_y_;
  if (type_ == ObMVTType::MVT_POINT) {
    if (point_idx_ == 0 && OB_FAIL(encode_buffer_.push_back(encode_command(ObMVTCommand::CMD_MOVE_TO, point_num_ == 0 ? 1 : point_num_)))) {
      LOG_WARN("failed to push back move to cmd", K(ret));
    } else if (OB_FAIL(encode_buffer_.push_back(encode_param(x)))) {
      LOG_WARN("failed to push back x value", K(ret));
    } else if (OB_FAIL(encode_buffer_.push_back(encode_param(y)))) {
      LOG_WARN("failed to push back y value", K(ret));
    } else {
      curr_x_ = static_cast<int32_t>(geo->x());
      curr_y_ = static_cast<int32_t>(geo->y());
    }
  } else if (type_ == ObMVTType::MVT_LINE || type_ == ObMVTType::MVT_RING) {
    if (type_ == ObMVTType::MVT_RING && point_idx_ + 1 == point_num_) {
      if (OB_FAIL(encode_buffer_.push_back(encode_command(ObMVTCommand::CMD_CLOSE_PATH, 1)))) {
        LOG_WARN("failed to push back move to cmd", K(ret));
      }
    } else {
      if (value_offset_ < line_to_offset_) {
        encode_buffer_[value_offset_++] = encode_param(x);
        encode_buffer_[value_offset_++] = encode_param(y);
      } else if (OB_FAIL(encode_buffer_.push_back(encode_param(x)))) {
        LOG_WARN("failed to push back x value", K(ret));
      } else if (OB_FAIL(encode_buffer_.push_back(encode_param(y)))) {
        LOG_WARN("failed to push back y value", K(ret));
      }
      curr_x_ = static_cast<int32_t>(geo->x());
      curr_y_ = static_cast<int32_t>(geo->y());
    }
  }
  if (OB_SUCC(ret)) {
    point_idx_++;
  }
  return ret;
}
int ObGeoMvtEncodeVisitor::visit(ObIWkbGeomMultiPoint *geo)
{
  int ret = OB_SUCCESS;
  type_ = ObMVTType::MVT_POINT;
  point_num_ = geo->size();
  return ret;
}
int ObGeoMvtEncodeVisitor::visit(ObIWkbGeomLineString *geo)
{
  int ret = OB_SUCCESS;
  point_num_ = geo->size();
  type_ = ObMVTType::MVT_LINE;
  move_to_offset_ = encode_buffer_.size();
  if (OB_FAIL(encode_buffer_.push_back(encode_command(ObMVTCommand::CMD_MOVE_TO, 1)))) {
    LOG_WARN("failed to push back move to cmd", K(ret));
  } else if (OB_FAIL(encode_buffer_.push_back(UINT32_MAX))) {
    LOG_WARN("failed to push back value", K(ret));
  } else if (OB_FAIL(encode_buffer_.push_back(UINT32_MAX))) {
    LOG_WARN("failed to push back value", K(ret));
  } else if (OB_FAIL(encode_buffer_.push_back(encode_command(ObMVTCommand::CMD_LINE_TO, point_num_ - 1)))) {
    LOG_WARN("failed to push back line to cmd", K(ret));
  } else {
    line_to_offset_ = encode_buffer_.size() - 1;
    value_offset_ = move_to_offset_ + 1;
    point_idx_ = 0;
  }
  return ret;
}

int ObGeoMvtEncodeVisitor::visit(ObIWkbGeomLinearRing *geo)
{
  int ret = OB_SUCCESS;
  type_ = ObMVTType::MVT_RING;
  point_num_ = geo->size();
  move_to_offset_ = encode_buffer_.size();
  if (OB_FAIL(encode_buffer_.push_back(encode_command(ObMVTCommand::CMD_MOVE_TO, 1)))) {
    LOG_WARN("failed to push back move to cmd", K(ret));
  } else if (OB_FAIL(encode_buffer_.push_back(UINT32_MAX))) {
    LOG_WARN("failed to push back value", K(ret));
  } else if (OB_FAIL(encode_buffer_.push_back(UINT32_MAX))) {
    LOG_WARN("failed to push back value", K(ret));
  } else if (OB_FAIL(encode_buffer_.push_back(encode_command(ObMVTCommand::CMD_LINE_TO, point_num_ - 2)))) { // exclude start/end point
    LOG_WARN("failed to push back line to cmd", K(ret));
  } else {
    line_to_offset_ = encode_buffer_.size() - 1;
    value_offset_ = move_to_offset_ + 1;
    point_idx_ = 0;
  }
  return ret;
}

} // namespace common
} // namespace oceanbase