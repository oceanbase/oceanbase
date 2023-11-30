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

#ifndef OCEANBASE_LIB_GEO_OB_SDO_GEO_FUNC_TO_WKB_
#define OCEANBASE_LIB_GEO_OB_SDO_GEO_FUNC_TO_WKB_

#include <algorithm>

#include "lib/allocator/ob_allocator.h"
#include "lib/geo/ob_geo_bin.h"
#include "lib/geo/ob_geo_common.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/geo/ob_sdo_geo_object.h"
#include "lib/geo/ob_geo_utils.h"

namespace oceanbase
{
namespace common
{
class ObSdoGeoToWkb
{
public:
  static const uint32_t RECTANGLE_POINT_NUM = 5;
  static const uint64_t INNERRING_ETYPE = 2003;
  static const uint64_t EXTRING_ETYPE = 1003;
  explicit ObSdoGeoToWkb(ObIAllocator *allocator, ObSrsType srs_type = ObSrsType::PROJECTED_SRS,
                         bool normalize = true, bool oracle_3d_format = false, bool is_deduce_dim = false)
      : buffer_(allocator), is_multi_visit_(false), allocator_(allocator),
        srs_type_(srs_type), need_normalize_(normalize), is_3d_geo_(false),
        oracle_3d_format_(oracle_3d_format), is_deduce_dim_(is_deduce_dim)
  {}
  ~ObSdoGeoToWkb()
  {}
  int translate(ObSdoGeoObject *geo, ObString &wkb, bool with_srid = false, ObGeoWkbByteOrder order = ObGeoWkbByteOrder::BigEndian);
  void reset();

private:
  int append_point(ObSdoGeoObject *geo, int64_t idx);
  int append_inner_point(double x, double y, double z = NAN, int swap_idx = 2);
  int append_inner_point(const ObArray<double> &ordinates, const uint64_t idx);
  int append_inner_ring(ObSdoGeoObject *geo, size_t ori_begin, size_t ori_end);
  int append_linestring(ObSdoGeoObject *geo, size_t ori_begin, size_t ori_end);
  int append_polygon(ObSdoGeoObject *geo, size_t elem_begin, size_t elem_end);
  int append_multi_point(ObSdoGeoObject *geo, size_t elem_begin, size_t elem_end);
  int append_multi_linestring(ObSdoGeoObject *geo);
  int append_multi_polygon(ObSdoGeoObject *geo);
  int append_rectangle(ObSdoGeoObject *geo, size_t idx, uint64_t sdo_etype);
  template<typename T>
  int append_num_with_endian(T data, uint64_t len);
  int normalize_point(double &lon, double &lat);
  int append_collection(ObSdoGeoObject *geo);
  int inner_append_rectangle(double lower_left_x, double lower_left_y, double upper_right_x,
                            double upper_right_y, uint64_t sdo_etype, bool is_ccw, double fix_z = NAN, int swap_idx = 2);
  int append_3D_rectangle(double x1, double y1, double x2, double y2, double fix_z, uint64_t sdo_etype, int fix_idx);
  ObGeoStringBuffer buffer_;
  uint8_t iorder_;
  bool is_multi_visit_;
  ObIAllocator *allocator_;
  ObSrsType srs_type_;
  bool need_normalize_;
  bool is_3d_geo_;
  bool oracle_3d_format_;
  bool is_deduce_dim_;
  DISALLOW_COPY_AND_ASSIGN(ObSdoGeoToWkb);
};

}  // namespace common
}  // namespace oceanbase

#endif