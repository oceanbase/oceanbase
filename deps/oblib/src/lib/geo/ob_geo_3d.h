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

#ifndef OCEANBASE_LIB_GEO_OB_GEO_3D_
#define OCEANBASE_LIB_GEO_OB_GEO_3D_
#include "lib/string/ob_string.h"
#include "ob_geo_ibin.h"
#include "ob_geo_common.h"
#include "ob_geo_utils.h"
#include "lib/function/ob_function.h"
#include "ob_geo_coordinate_range_visitor.h"
#include "ob_sdo_geo_object.h"
#include "lib/geo/ob_geo_elevation_visitor.h"

namespace oceanbase {
namespace common {
class ObGeo3DVisitor;

enum ObLineType {
  Line = 0,
  ExterRing = 1,
  InnerRing = 2
};
class ObGeometry3D: public ObGeometry
{
public:
  ObGeometry3D(uint32_t srid = 0, ObIAllocator *allocator = NULL)
          : ObGeometry(srid, allocator), cur_pos_(0) {}
  virtual ~ObGeometry3D() = default;
  ObGeometry3D(const ObGeometry3D& g) = default;
  ObGeometry3D& operator=(const ObGeometry3D& g) = default;
  // interface
  virtual void set_data(const ObString& data) { data_ = data; }
  virtual ObString to_wkb() const override { return data_; }
  virtual int do_visit(ObIGeoVisitor &visitor) { UNUSED(visitor); return OB_NOT_SUPPORTED; };
  virtual uint64_t length() const override { return data_.length(); }
  virtual const char* val() const override { return data_.ptr(); }
  virtual ObGeoType type() const override;
  virtual ObGeoCRS crs() const override { return crs_; };
  virtual bool is_tree() const override { return false; };
  virtual bool is_empty() const override;
  // new interface
  void set_pos(uint64_t pos) { cur_pos_ = pos; }
  uint64_t get_pos() { return cur_pos_; }
  ObGeoWkbByteOrder byteorder() const;
  ObGeoWkbByteOrder byteorder(uint64_t pos) const;
  ObGeoType type(uint64_t pos) const;
  void set_crs(ObGeoCRS crs) { crs_ = crs; }
  int to_2d_geo(ObIAllocator &allocator, ObGeometry *&res, uint32_t srid = 0);
  int to_wkt(ObIAllocator &allocator, ObString &wkt, uint32_t srid = 0, int64_t maxdecimaldigits = -1);
  int reverse_coordinate();
  int check_wkb_valid();
  int check_3d_coordinate_range(const ObSrsItem *srs, const bool is_normalized, ObGeoCoordRangeResult &result);
  int to_sdo_geometry(ObSdoGeoObject &sdo_geo);
  int to_geo_json(ObIAllocator *allocator, common::ObString &geo_json);
  int create_elevation_extent(ObGeoElevationExtent &extent);
  int normalize(const ObSrsItem *srs, uint32_t &zoom_in_value);
  int check_empty(bool &is_empty);
  int correct_lon_lat(const ObSrsItem *srs);
private:
  int visit_wkb_inner(ObGeo3DVisitor &visitor);
  bool is_end() { return cur_pos_ >= data_.length(); }
  int read_header(ObGeoWkbByteOrder &bo, ObGeoType &geo_type);
  int read_nums_value(ObGeoWkbByteOrder bo, uint32_t &nums);
  // visit wkb
  int visit_pointz_inner(ObGeoWkbByteOrder bo, ObGeo3DVisitor &visitor);
  int visit_pointz(ObGeoWkbByteOrder bo, ObGeo3DVisitor &visitor, bool is_inner = false);
  int visit_linestringz(ObGeoWkbByteOrder bo, ObGeo3DVisitor &visitor, ObLineType line_type = ObLineType::Line);
  int visit_polygonz(ObGeoWkbByteOrder bo, ObGeo3DVisitor &visitor);
  int visit_multi_geomz(ObGeoWkbByteOrder bo, ObGeoType geo_type, ObGeo3DVisitor &visitor);
  int visit_collectionz(ObGeoWkbByteOrder bo, ObGeo3DVisitor &visitor);

protected:
  ObGeoCRS crs_;
  ObString data_; // wkb without srid
private:
  uint64_t cur_pos_;
};

class ObGeo3DVisitor
{
public:
  ObGeo3DVisitor() : is_multi_(false) {}
  virtual int visit_header(ObGeoWkbByteOrder bo, ObGeoType geo_type, bool is_sub_type = false);
  // pointz
  virtual int visit_pointz_start(ObGeometry3D *geo, bool is_inner = false);
  virtual int visit_pointz_inner(double x, double y, double z);
  virtual int visit_pointz_end(ObGeometry3D *geo, bool is_inner = false);
  // linestringz
  virtual int visit_linestringz_start(ObGeometry3D *geo, uint32_t nums, ObLineType line_type);
  virtual int visit_linestringz_item_after(ObGeometry3D *geo, uint32_t idx, ObLineType line_type);
  virtual int visit_linestringz_end(ObGeometry3D *geo, uint32_t nums, ObLineType line_type);
  // polygonz
  virtual int visit_polygonz_start(ObGeometry3D *geo, uint32_t nums);
  virtual int visit_polygonz_item_after(ObGeometry3D *geo, uint32_t idx);
  virtual int visit_polygonz_end(ObGeometry3D *geo, uint32_t nums);
  // multi geomz
  virtual int visit_multi_geom_start(ObGeoType geo_type, ObGeometry3D *geo, uint32_t nums);
  virtual int visit_multi_geom_item_after(ObGeoType geo_type, ObGeometry3D *geo, uint32_t idx);
  virtual int visit_multi_geom_end(ObGeoType geo_type, ObGeometry3D *geo, uint32_t nums);
  // geometrycollection
  virtual int visit_collectionz_start(ObGeometry3D *geo, uint32_t nums);
  virtual int visit_collectionz_item_after(ObGeometry3D *geo, uint32_t idx);
  virtual int visit_collectionz_end(ObGeometry3D *geo, uint32_t nums);
  bool is_multi() { return is_multi_; }
  void set_is_multi(bool is_multi) { is_multi_ = is_multi; }
private:
  bool is_multi_;
};

class ObGeo3DChecker : public ObGeo3DVisitor
{
public:
  virtual int visit_header(ObGeoWkbByteOrder bo, ObGeoType geo_type, bool is_sub_type = false) override;
  virtual int visit_pointz_start(ObGeometry3D *geo, bool is_inner = false);
  virtual int visit_linestringz_start(ObGeometry3D *geo, uint32_t nums, ObLineType line_type) override;
};

class ObGeo3DTo2DVisitor : public ObGeo3DVisitor
{
public:
  ObGeo3DTo2DVisitor(): wkb_buf_(NULL) {}
  void set_wkb_buf(ObWkbBuffer *wkb_buf) { wkb_buf_ = wkb_buf; }
  virtual int visit_header(ObGeoWkbByteOrder bo, ObGeoType geo_type, bool is_sub_type = false) override;
  virtual int visit_pointz_inner(double x, double y, double z);
  virtual int visit_linestringz_start(ObGeometry3D *geo, uint32_t nums, ObLineType line_type);
  virtual int visit_polygonz_start(ObGeometry3D *geo, uint32_t nums);
  virtual int visit_multi_geom_start(ObGeoType geo_type, ObGeometry3D *geo, uint32_t nums);
  virtual int visit_collectionz_start(ObGeometry3D *geo, uint32_t nums);
private:
  int append_nums(uint32_t nums);
private:
  ObWkbBuffer *wkb_buf_;
};

class ObGeo3DToWktVisitor : public ObGeo3DVisitor
{
public:
  ObGeo3DToWktVisitor(int64_t maxdecimaldigits = -1);
  void set_wkt_buf(ObStringBuffer *wkt_buf) { wkt_buf_ = wkt_buf; }
  virtual int visit_header(ObGeoWkbByteOrder bo, ObGeoType geo_type, bool is_sub_type = false);
  // pointz
  virtual int visit_pointz_start(ObGeometry3D *geo, bool is_inner);
  virtual int visit_pointz_inner(double x, double y, double z);
  virtual int visit_pointz_end(ObGeometry3D *geo, bool is_inner);
  // linestringz
  virtual int visit_linestringz_start(ObGeometry3D *geo, uint32_t nums, ObLineType line_type);
  virtual int visit_linestringz_item_after(ObGeometry3D *geo, uint32_t idx, ObLineType line_type);
  virtual int visit_linestringz_end(ObGeometry3D *geo, uint32_t nums, ObLineType line_type);
  // polygonz
  virtual int visit_polygonz_start(ObGeometry3D *geo, uint32_t nums);
  virtual int visit_polygonz_item_after(ObGeometry3D *geo, uint32_t idx);
  virtual int visit_polygonz_end(ObGeometry3D *geo, uint32_t nums);
  // multi geomz
  virtual int visit_multi_geom_start(ObGeoType geo_type, ObGeometry3D *geo, uint32_t nums);
  virtual int visit_multi_geom_item_after(ObGeoType geo_type, ObGeometry3D *geo, uint32_t idx);
  virtual int visit_multi_geom_end(ObGeoType geo_type, ObGeometry3D *geo, uint32_t nums);
  // geometrycollection
  virtual int visit_collectionz_start(ObGeometry3D *geo, uint32_t nums);
  virtual int visit_collectionz_item_after(ObGeometry3D *geo, uint32_t idx);
  virtual int visit_collectionz_end(ObGeometry3D *geo, uint32_t nums);
private:
  int remove_comma();
  int append_comma();
  int append_paren(bool is_left);
private:
  ObStringBuffer *wkt_buf_;
  bool is_oracle_mode_;
  bool is_mpt_visit_;
  bool has_scale_;
  int64_t scale_;
};

class ObGeo3DReserverCoordinate : public ObGeo3DVisitor
{
public:
  ObGeo3DReserverCoordinate() {}
  virtual int visit_pointz_start(ObGeometry3D *geo, bool is_inner = false);
};

class ObGeo3DCoordinateRangeVisitor : public ObGeo3DVisitor
{
public:
  ObGeo3DCoordinateRangeVisitor(const ObSrsItem *srs, bool is_normalized = true)
    : srs_(srs), is_lati_out_range_(false), is_long_out_range_(false),
      value_out_range_(NAN), is_normalized_(is_normalized) {}
  virtual int visit_pointz_inner(double x, double y, double z);
  void get_coord_range_result(ObGeoCoordRangeResult& result);
private:
  const ObSrsItem *srs_;
  bool is_lati_out_range_;
  bool is_long_out_range_;
  double value_out_range_;
  bool is_normalized_;
  DISALLOW_COPY_AND_ASSIGN(ObGeo3DCoordinateRangeVisitor);
};

class ObGeo3DWkbToSdoGeoVisitor : public ObGeo3DVisitor
{
public:
  ObGeo3DWkbToSdoGeoVisitor()
    : is_multi_visit_(false), is_collection_visit_(false),
      is_inner_element_(false), sdo_geo_(nullptr) {}
  ~ObGeo3DWkbToSdoGeoVisitor() {}
  int init(ObSdoGeoObject *geo, uint32_t srid = UINT32_MAX);
  // pointz
  virtual int visit_pointz_start(ObGeometry3D *geo, bool is_inner = false);
  virtual int visit_pointz_inner(double x, double y, double z);
  // linestringz
  virtual int visit_linestringz_start(ObGeometry3D *geo, uint32_t nums, ObLineType line_type);
  virtual int visit_linestringz_end(ObGeometry3D *geo, uint32_t nums, ObLineType line_type);
  // polygonz
  virtual int visit_polygonz_start(ObGeometry3D *geo, uint32_t nums);
  // multi geomz
  virtual int visit_multi_geom_start(ObGeoType geo_type, ObGeometry3D *geo, uint32_t nums);
  virtual int visit_multi_geom_end(ObGeoType geo_type, ObGeometry3D *geo, uint32_t nums);
  // geometrycollection
  virtual int visit_collectionz_start(ObGeometry3D *geo, uint32_t nums);
  virtual int visit_collectionz_end(ObGeometry3D *geo, uint32_t nums);

private:
  int append_elem_info(uint64_t offset, uint64_t etype, uint64_t interpretation);
  bool is_multi_visit_;
  bool is_collection_visit_;
  bool is_inner_element_;
  ObSdoGeoObject *sdo_geo_;
};

class ObGeo3DWkbToJsonVisitor : public ObGeo3DVisitor
{
public:
  ObGeo3DWkbToJsonVisitor(ObIAllocator *allocator)
    : in_multi_visit_(false), in_collection_level_(0),
      inner_element_level_(0), buffer_(allocator) {}
  ~ObGeo3DWkbToJsonVisitor() {}
  // pointz
  virtual int visit_pointz_start(ObGeometry3D *geo, bool is_inner = false);
  virtual int visit_pointz_inner(double x, double y, double z);
  virtual int visit_pointz_end(ObGeometry3D *geo, bool is_inner = false);
  // linestringz
  virtual int visit_linestringz_start(ObGeometry3D *geo, uint32_t nums, ObLineType line_type);
  virtual int visit_linestringz_end(ObGeometry3D *geo, uint32_t nums, ObLineType line_type);
  // polygonz
  virtual int visit_polygonz_start(ObGeometry3D *geo, uint32_t nums);
  virtual int visit_polygonz_end(ObGeometry3D *geo, uint32_t nums);
  // multi geomz
  virtual int visit_multi_geom_start(ObGeoType geo_type, ObGeometry3D *geo, uint32_t nums);
  virtual int visit_multi_geom_end(ObGeoType geo_type, ObGeometry3D *geo, uint32_t nums);
  // geometrycollection
  virtual int visit_collectionz_start(ObGeometry3D *geo, uint32_t nums);
  virtual int visit_collectionz_end(ObGeometry3D *geo, uint32_t nums);
  ObGeoStringBuffer &get_result() { return buffer_; }

private:
  static const int MAX_DIGITS_IN_DOUBLE = 25;
  int appendDouble(double x);
  int appendJsonFields(ObGeoType type, const char *type_name);
  int appendMultiSuffix(ObGeoType geo_type);
  int appendCollectionSuffix();
  inline bool in_colloction_visit() { return in_collection_level_ > 0; }
  bool in_multi_visit_;
  int in_collection_level_;
  int inner_element_level_;
  ObGeoStringBuffer buffer_;
};

class ObGeo3DElevationVisitor : public ObGeo3DVisitor
{
public:
  explicit ObGeo3DElevationVisitor(ObGeoElevationExtent &extent) : extent_(&extent) {}
  virtual int visit_pointz_start(ObGeometry3D *geo, bool is_inner = false);
private:
  ObGeoElevationExtent *extent_;
  DISALLOW_COPY_AND_ASSIGN(ObGeo3DElevationVisitor);
};

class ObGeo3DNormalizeVisitor : public ObGeo3DVisitor
{
public:
  explicit ObGeo3DNormalizeVisitor(const ObSrsItem *srs, bool no_srs = false)
    : srs_(srs), no_srs_(no_srs), zoom_in_value_(0) {}
  virtual int visit_pointz_start(ObGeometry3D *geo, bool is_inner = false);
  uint32_t get_zoom_in_value() { return zoom_in_value_; }
private:
  static constexpr double ZOOM_IN_THRESHOLD = 0.00000001;
  const ObSrsItem *srs_;
  bool no_srs_; // for st_transform, only proj4text is given
  uint32_t zoom_in_value_;
  DISALLOW_COPY_AND_ASSIGN(ObGeo3DNormalizeVisitor);
};

class ObGeo3DEmptyVisitor : public ObGeo3DVisitor
{
public:
  explicit ObGeo3DEmptyVisitor() : is_empty_(true) {}
  virtual int visit_pointz_start(ObGeometry3D *geo, bool is_inner = false);
  bool is_empty() { return is_empty_; }
private:
  bool is_empty_;
  DISALLOW_COPY_AND_ASSIGN(ObGeo3DEmptyVisitor);
};

class ObGeo3DLonLatChecker : public ObGeo3DVisitor
{
public:
  explicit ObGeo3DLonLatChecker(const ObSrsItem *srs) : srs_(srs) {}
  virtual int visit_pointz_start(ObGeometry3D *geo, bool is_inner = false);
private:
  const ObSrsItem *srs_;
  DISALLOW_COPY_AND_ASSIGN(ObGeo3DLonLatChecker);
};

} // namespace common
} // namespace oceanbase
#endif // OCEANBASE_LIB_GEO_OB_GEO_3D_