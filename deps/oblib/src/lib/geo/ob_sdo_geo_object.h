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

#ifndef OCEANBASE_LIB_GEO_OB_SDO_GEO_OBJECT_
#define OCEANBASE_LIB_GEO_OB_SDO_GEO_OBJECT_

#include "lib/allocator/ob_allocator.h"
#include "lib/geo/ob_geo_bin.h"
#include "lib/utility/ob_print_utils.h"


namespace oceanbase
{
namespace common
{

enum ObSdoGeoAttrIdx
{
  ObGtype = 1,
  ObSrid = 2,
  ObPointX = 3,
  ObPointY = 4,
  ObPointZ = 5,
  ObElemArray = 6,
  ObOrdArray = 7,
};

struct ObSdoPoint
{
public:
  ObSdoPoint(double x, double y)
    : x_(x), y_(y), has_z_(false), is_null_(false) {}
  ObSdoPoint(double x, double y, double z)
    : x_(x), y_(y), has_z_(true), z_(z), is_null_(false) {}
  ObSdoPoint() : has_z_(false), is_null_(true) {}
  bool operator==(const ObSdoPoint &other) const;
  void set_x(double x) { is_null_ = false; x_ = x; }
  void set_y(double y) { is_null_ = false; y_ = y; }
  void set_z(double z) { has_z_ = true; z_ = z; }
  inline double get_x() const { return x_; }
  inline double get_y() const { return y_; }
  inline double get_z() const { return z_; }
  inline bool has_z() const { return has_z_; }
  inline bool is_null() const { return is_null_; }
  int to_text(ObStringBuffer &buf);
  TO_STRING_KV(K_(x), K_(y), K_(has_z), K_(z), K_(is_null));

private:
  bool need_sci_format(double num) { return num <= -1e9 || num >= 1e10; }

  double x_;
  double y_;
  bool has_z_;
  double z_;
  bool is_null_;
};

class ObSdoGeoObject
{
public:
  ObSdoGeoObject(ObGeoType gtype, ObSdoPoint point, uint32_t srid = UINT32_MAX)
    : gtype_(gtype), point_(point), srid_(srid) {}

  ObSdoGeoObject(ObGeoType gtype, ObArray<uint64_t> elem_info,
      ObArray<double> ordinates, uint32_t srid = UINT32_MAX)
      : gtype_(gtype), elem_info_(elem_info), ordinates_(ordinates), srid_(srid)
  {}
  ObSdoGeoObject() : gtype_(ObGeoType::GEOTYPEMAX), srid_(UINT32_MAX) {}
  ~ObSdoGeoObject()
  {}
  bool operator==(const ObSdoGeoObject &other) const;
  int to_text(ObStringBuffer &buf);
  inline const ObArray<uint64_t> &get_elem_info() const { return elem_info_; }
  inline const ObArray<double> &get_ordinates() const { return ordinates_; }
  inline ObArray<uint64_t> &get_elem_info() { return elem_info_; }
  inline ObArray<double> &get_ordinates() { return ordinates_; }
  inline ObGeoType get_gtype() const { return gtype_; }
  inline uint64_t get_srid() const { return srid_; }
  inline const ObSdoPoint &get_point() const { return point_; }
  inline ObSdoPoint &get_point() { return point_; }
  inline void set_srid(uint32_t srid) { srid_ = srid; }
  inline void set_gtype(ObGeoType gtype) { gtype_ = gtype; }
  inline bool has_point() { return !point_.is_null(); }
  inline bool is_3d_geo() { return gtype_ >= ObGeoType::POINTZ && gtype_ <= ObGeoType::GEOMETRYCOLLECTIONZ; }
  int append_elem(uint64_t elem) { return elem_info_.push_back(elem); }
  int append_ori(double ori) { return ordinates_.push_back(ori); }
  TO_STRING_KV(K_(gtype), K_(srid), K_(point), K_(elem_info), K_(ordinates));

private:
  bool need_sci_format(double num) { return num <= -1e9 || num >= 1e10; }

  ObGeoType gtype_;
  ObSdoPoint point_;
  ObArray<uint64_t> elem_info_;
  ObArray<double> ordinates_;
  uint32_t srid_;
  DISALLOW_COPY_AND_ASSIGN(ObSdoGeoObject);
};
}  // namespace common
}  // namespace oceanbase

#endif