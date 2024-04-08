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
#ifndef OCEANBASE_LIB_GEO_OB_MVT_ENCODE_VISITOR_
#define OCEANBASE_LIB_GEO_OB_MVT_ENCODE_VISITOR_

#include "lib/geo/ob_geo_visitor.h"
namespace oceanbase
{
namespace common
{

enum class ObMVTType {
  MVT_POINT = 0,
  MVT_LINE = 1,
  MVT_RING = 2,
  MVT_MAX = 3,
};

enum class ObMVTCommand
{
  CMD_MOVE_TO = 1,
  CMD_LINE_TO = 2,
  CMD_CLOSE_PATH = 7,
};

class ObGeoMvtEncodeVisitor : public ObEmptyGeoVisitor
{
public:
  ObGeoMvtEncodeVisitor() : type_(ObMVTType::MVT_POINT),
    move_to_offset_(UINT32_MAX), line_to_offset_(UINT32_MAX), value_offset_(UINT32_MAX),
    point_num_(0), point_idx_(0),
    curr_x_(0), curr_y_(0) {}
  virtual ~ObGeoMvtEncodeVisitor() {}
  bool prepare(ObGeometry *geo) { UNUSED(geo); return true; }
  int visit(ObIWkbGeomPoint *geo);
  int visit(ObIWkbGeomMultiPoint *geo);
  int visit(ObIWkbGeomLineString *geo);
  int visit(ObIWkbGeomLinearRing *geo);
  int visit(ObIWkbGeometry *geo) { UNUSED(geo); return OB_SUCCESS; }
  inline ObVector<uint32_t> &get_encode_buffer() { return encode_buffer_; }
private:
  inline uint32_t encode_command(ObMVTCommand id, uint32_t count) { return (static_cast<uint32_t>(id) & 0x7) | (count << 3); }
  inline uint32_t encode_param(int32_t value) { return (value << 1) ^ (value >> 31); }
  ObMVTType type_;
  ObVector<uint32_t> encode_buffer_;
  uint32_t move_to_offset_;
  uint32_t line_to_offset_;
  uint32_t value_offset_;
  uint32_t point_num_;
  uint32_t point_idx_;
  int32_t curr_x_;
  int32_t curr_y_;
};

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_LIB_GEO_OB_MVT_ENCODE_VISITOR_