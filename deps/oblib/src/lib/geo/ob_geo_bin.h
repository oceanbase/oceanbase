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

#ifndef OCEANBASE_LIB_GEO_OB_GEO_BIN_
#define OCEANBASE_LIB_GEO_OB_GEO_BIN_

#include "ob_geo.h"
#include "ob_geo_bin_iter.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_array.h"
#include "lib/geo/ob_geo_common.h"

namespace oceanbase {
namespace common {

// [srid]
static const uint32_t WKB_GEO_SRID_SIZE = sizeof(uint32_t);
// [version]
static const uint32_t WKB_VERSION_SIZE = sizeof(uint8_t);
// swkb = [srid][version]+wkb
static const uint32_t WKB_OFFSET = WKB_GEO_SRID_SIZE + WKB_VERSION_SIZE;
// [bo]
static const uint32_t WKB_GEO_BO_SIZE = sizeof(uint8_t);
// [type]
static const uint32_t WKB_GEO_TYPE_SIZE = sizeof(uint32_t);
// [num]
static const uint32_t WKB_GEO_ELEMENT_NUM_SIZE = sizeof(uint32_t);
// [double]
static const uint32_t WKB_GEO_DOUBLE_STORED_SIZE = sizeof(double);
// [bo][type][num]
static const uint32_t WKB_COMMON_WKB_HEADER_LEN = WKB_GEO_BO_SIZE + WKB_GEO_TYPE_SIZE + WKB_GEO_ELEMENT_NUM_SIZE;
// ewkb [bo][type]
static const uint32_t EWKB_COMMON_WKB_HEADER_LEN = WKB_GEO_BO_SIZE + WKB_GEO_TYPE_SIZE;
// ewkb [bo][type][srid]
static const uint32_t EWKB_WITH_SRID_LEN = WKB_COMMON_WKB_HEADER_LEN;
// [double][double]
static const uint32_t WKB_POINT_DATA_SIZE = WKB_GEO_DOUBLE_STORED_SIZE + WKB_GEO_DOUBLE_STORED_SIZE; // x + y
// skip [srid][bo]
static const uint32_t WKB_DATA_OFFSET = WKB_OFFSET + WKB_GEO_BO_SIZE;
// skip [srid][bo][type] only used for inner points
static const uint32_t WKB_INNER_POINT = WKB_DATA_OFFSET + WKB_GEO_TYPE_SIZE;
// Cartesian
// [bo][type][X][Y]
#pragma pack(1)
class ObWkbGeomPoint {
public:
  ObWkbGeomPoint() {}
  ObWkbGeomPoint(const ObWkbGeomPoint& p);
  ~ObWkbGeomPoint() {}
  uint64_t length() const;
  inline ObGeoType type() const { return ObGeoType::POINT; }
  inline ObGeoCRS crs() const { return ObGeoCRS::Cartesian; }
  ObGeoWkbByteOrder byteorder() const;
  void byteorder(ObGeoWkbByteOrder bo);
  template<std::size_t K>
  double get() const;
  template<std::size_t K>
  void set(double d);
  ObWkbGeomPoint& operator=(const ObWkbGeomPoint& p);
private:
  void refresh_type();
  uint8_t bo_;
  uint32_t type_;
  double x_;
  double y_;
};
#pragma pack()

// Inner Point for little endian
// WKB in database should be little endian
// [X][Y]
class ObWkbGeomInnerPoint {
public:
  ObWkbGeomInnerPoint() : x_(NAN), y_(NAN) {}
  ObWkbGeomInnerPoint(const ObWkbGeomInnerPoint& p);
  ObWkbGeomInnerPoint(double x, double y) : x_(x), y_(y) {}
  ObWkbGeomInnerPoint(uint32_t srid, ObIAllocator *alloc) : x_(NAN), y_(NAN) { UNUSEDx(srid, alloc); }
  ~ObWkbGeomInnerPoint() {}
  uint64_t length() const;
  template<std::size_t K>
  double get() const;
  template<std::size_t K>
  void set(double d);
  // candidate function not viable: 'this' argument has type 'point_type' (aka 'const oceanbase::common::ObWkbGeomInnerPoint'), but method is not marked const
  ObWkbGeomInnerPoint& operator=(const ObWkbGeomInnerPoint& p);
  ObWkbGeomInnerPoint& operator=(const ObWkbGeomInnerPoint& p) const;
  // TODO
  int64_t to_string(char *buffer, const int64_t length) const{
    UNUSED(buffer);
    UNUSED(length);
    return 0;
  }
private:
  double x_;
  double y_;
};

// Cartesian linestring
// [bo][type][num][X][Y][...]
class ObWkbGeomLineString {
public:
  // type define
  typedef ObWkbGeomLineString self;
  typedef ptrdiff_t index_type;
  typedef ObWkbGeomInnerPoint value_type;
  typedef ObWkbConstIterator<value_type, self> const_iterator;
  typedef ObWkbIterator<value_type, self> iterator;
  typedef uint64_t size_type;
  typedef const value_type& const_reference;
  typedef const value_type* const_pointer;
  typedef value_type& reference;
  typedef value_type* pointer;
  typedef ptrdiff_t difference_type;
public:
  ObWkbGeomLineString() {}
  ~ObWkbGeomLineString() {}
  uint32_t size() const;
  size_type length() const;
  inline ObGeoType type() const { return ObGeoType::LINESTRING; }
  inline ObGeoCRS crs() const { return ObGeoCRS::Cartesian; }
  // iter adaptor
  index_type iter_idx_max() const { return size(); }
  index_type iter_idx_min() const { return 0; }
  void get_sub_addr(const_pointer last_addr, index_type last_idx, index_type cur_idx,
    ObWkbIterOffsetArray* offsets, pointer& data);
  iterator begin() { return iterator(iter_idx_min(), this); }
  const_iterator begin() const { return const_iterator(iter_idx_min(), this); }
  iterator end() { return iterator(iter_idx_max(), this); }
  const_iterator end() const { return const_iterator(iter_idx_max(), this); }
  DISABLE_COPY_ASSIGN(ObWkbGeomLineString);
};

// Cartesian linearring
// [num][X][Y][...]
class ObWkbGeomLinearRing {
public:
  // type define
  typedef ObWkbGeomLinearRing self;
  typedef ptrdiff_t index_type;
  typedef ObWkbGeomInnerPoint value_type;
  typedef ObWkbConstIterator<value_type, self> const_iterator;
  typedef ObWkbIterator<value_type, self> iterator;
  typedef uint64_t size_type;
  typedef const value_type* const_pointer;
  typedef const value_type& const_reference;
  typedef value_type* pointer;
  typedef value_type& reference;
  typedef ptrdiff_t difference_type;
public:
  ObWkbGeomLinearRing() {}
  ~ObWkbGeomLinearRing() {}
  uint32_t size() const;
  size_type length() const;
  // iter adaptor
  index_type iter_idx_max() const { return size(); }
  index_type iter_idx_min() const { return 0; }
  void get_sub_addr(const_pointer last_addr, index_type last_idx, index_type cur_idx,
    ObWkbIterOffsetArray* offsets, pointer& data);
  // iter adapt
  iterator begin() { return iterator(iter_idx_min(), this); }
  const_iterator begin() const { return const_iterator(iter_idx_min(), this); }
  iterator end() { return iterator(iter_idx_max(), this); }
  const_iterator end() const { return const_iterator(iter_idx_max(), this); }
  bool empty() const {return false;}
  bool empty() {return false;}

  void push_back(const ObWkbGeomInnerPoint &pt) {
    UNUSED(pt);
    throw ObGeoNotImplementedException(ObGeoCRS::Geographic, ObGeoType::POINT, "ObWkbGeomLinearRing::push_back");
  }
  void resize(uint64_t count) {
    UNUSED(count);
    throw ObGeoNotImplementedException(ObGeoCRS::Geographic, ObGeoType::POINT, "ObWkbGeomLinearRing::resize");
  }
  void clear() {
    throw ObGeoNotImplementedException(ObGeoCRS::Geographic, ObGeoType::POINT, "ObWkbGeomLinearRing::clear");
  }
  DISABLE_COPY_ASSIGN(ObWkbGeomLinearRing);
};


// Cartesian InnerRings
// [bo][type][num][ex][inner_rings]
// for get num
class ObWkbGeomPolygonInnerRings {
public:
  typedef ObWkbGeomPolygonInnerRings self;
  typedef ptrdiff_t index_type;
  typedef ObWkbGeomLinearRing value_type;
  typedef ObWkbConstIterator<value_type, self> const_iterator;
  typedef ObWkbIterator<value_type, self> iterator;
  typedef uint64_t size_type;
  typedef const value_type* const_pointer;
  typedef const value_type& const_reference;
  typedef value_type* pointer;
  typedef value_type& reference;
  typedef ptrdiff_t difference_type;
public:
  ObWkbGeomPolygonInnerRings() {}
  ~ObWkbGeomPolygonInnerRings() {}
  uint32_t size() const;
  size_type length() const;
  // iter adaptor
  index_type iter_idx_max() const { return size() + 1; }
  index_type iter_idx_min() const { return 1; }
  char* ptr() const { return reinterpret_cast<char*>(const_cast<self*>(this)); }
  uint32_t data_offset() const { return WKB_COMMON_WKB_HEADER_LEN; }
  index_type et(index_type curidx) const { return curidx; }
  void get_sub_addr(const_pointer last_addr, index_type last_idx, index_type cur_idx,
    ObWkbIterOffsetArray* offsets, pointer& data);
  size_type get_sub_size(const_pointer data) const { return data->length(); };
  iterator begin() { return iterator(iter_idx_min(), this); } // for move over exterior
  const_iterator begin() const { return const_iterator(iter_idx_min(), this); } // for move over exterior
  iterator end() { return iterator(iter_idx_max(), this); }
  const_iterator end() const { return const_iterator(iter_idx_max(), this); }
  DISABLE_COPY_ASSIGN(ObWkbGeomPolygonInnerRings);
};

// Cartesian Polygon
// [bo][type][num][ex][inner_rings]
class ObWkbGeomPolygon {
public:
  ObWkbGeomPolygon() {}
  ~ObWkbGeomPolygon() {}
  uint32_t size() const;
  uint64_t length() const;
  inline ObGeoType type() const { return ObGeoType::POLYGON; }
  inline ObGeoCRS crs() const { return ObGeoCRS::Cartesian; }
  ObWkbGeomLinearRing& exterior_ring();
  const ObWkbGeomLinearRing& exterior_ring() const;
  ObWkbGeomPolygonInnerRings& inner_rings();
  const ObWkbGeomPolygonInnerRings& inner_rings() const;
  DISABLE_COPY_ASSIGN(ObWkbGeomPolygon);
};

// Cartesian MultiPoint
// [bo][type][num][point]
class ObWkbGeomMultiPoint {
public:
  typedef ObWkbGeomMultiPoint self;
  typedef ptrdiff_t index_type;
  typedef ObWkbGeomInnerPoint value_type;
  typedef ObWkbConstIterator<value_type, self> const_iterator;
  typedef ObWkbIterator<value_type, self> iterator;
  typedef uint64_t size_type;
  typedef const value_type* const_pointer;
  typedef const value_type& const_reference;
  typedef value_type* pointer;
  typedef value_type& reference;
  typedef ptrdiff_t difference_type;
public:
  ObWkbGeomMultiPoint() {}
  ~ObWkbGeomMultiPoint() {}
  uint32_t size() const;
  size_type length() const;
  inline ObGeoType type() const { return ObGeoType::MULTIPOINT; }
  inline ObGeoCRS crs() const { return ObGeoCRS::Cartesian; }
  // iter adaptor
  index_type iter_idx_max() const { return size(); }
  index_type iter_idx_min() const { return 0; }
  void get_sub_addr(const_pointer last_addr, index_type last_idx, index_type cur_idx,
    ObWkbIterOffsetArray* offsets, pointer& data);
  iterator begin() { return iterator(iter_idx_min(), this); }
  const_iterator begin() const { return const_iterator(iter_idx_min(), this); }
  iterator end() { return iterator(iter_idx_max(), this); }
  const_iterator end() const { return const_iterator(iter_idx_max(), this); }
  DISABLE_COPY_ASSIGN(ObWkbGeomMultiPoint);
};


// Cartesian MultiLineString
// [bo][type][num][LineString...]
class ObWkbGeomMultiLineString {
public:
  typedef ObWkbGeomMultiLineString self;
  typedef ptrdiff_t index_type;
  typedef ObWkbGeomLineString value_type;
  typedef ObWkbConstIterator<value_type, self> const_iterator;
  typedef ObWkbIterator<value_type, self> iterator;
  typedef size_t size_type;
  typedef const value_type* const_pointer;
  typedef const value_type& const_reference;
  typedef value_type* pointer;
  typedef value_type& reference;
  typedef ptrdiff_t difference_type;
public:
  ObWkbGeomMultiLineString() {}
  ~ObWkbGeomMultiLineString() {}
  uint32_t size() const;
  size_type length() const;
  inline ObGeoType type() const { return ObGeoType::MULTILINESTRING; }
  inline ObGeoCRS crs() const { return ObGeoCRS::Cartesian; }
  // iter adaptor
  index_type iter_idx_max() const { return size(); }
  index_type iter_idx_min() const { return 0; }
  char* ptr() const { return reinterpret_cast<char*>(const_cast<self*>(this)); }
  uint32_t data_offset() const { return WKB_COMMON_WKB_HEADER_LEN; }
  index_type et(index_type curidx) const { return curidx; }
  void get_sub_addr(const_pointer last_addr, index_type last_idx, index_type cur_idx,
    ObWkbIterOffsetArray* offsets, pointer& data);
  size_type get_sub_size(const_pointer data) const { return data->length(); };
  iterator begin() { return iterator(iter_idx_min(), this); }
  const_iterator begin() const { return const_iterator(iter_idx_min(), this); }
  iterator end() { return iterator(iter_idx_max(), this); }
  const_iterator end() const { return const_iterator(iter_idx_max(), this); }
  DISABLE_COPY_ASSIGN(ObWkbGeomMultiLineString);
};


// Cartesian MultiPolygon
// [bo][type][num][Polygon...]
class ObWkbGeomMultiPolygon {
public:
  typedef ObWkbGeomMultiPolygon self;
  typedef ptrdiff_t index_type;
  typedef ObWkbGeomPolygon value_type;
  typedef ObWkbConstIterator<value_type, self> const_iterator;
  typedef ObWkbIterator<value_type, self> iterator;
  typedef size_t size_type;
  typedef const value_type* const_pointer;
  typedef const value_type& const_reference;
  typedef value_type* pointer;
  typedef value_type& reference;
  typedef ptrdiff_t difference_type;
public:
  ObWkbGeomMultiPolygon() {}
  ~ObWkbGeomMultiPolygon() {}
  uint32_t size() const;
  size_type length() const;
  inline ObGeoType type() const { return ObGeoType::MULTIPOLYGON; }
  inline ObGeoCRS crs() const { return ObGeoCRS::Cartesian; }
  // iter adaptor
  index_type iter_idx_max() const { return size(); }
  index_type iter_idx_min() const { return 0; }
  char* ptr() const { return reinterpret_cast<char*>(const_cast<self*>(this)); }
  uint32_t data_offset() const { return WKB_COMMON_WKB_HEADER_LEN; }
  index_type et(index_type curidx) const { return curidx; }
  void get_sub_addr(const_pointer last_addr, index_type last_idx, index_type cur_idx,
    ObWkbIterOffsetArray* offsets, pointer& data);
  size_type get_sub_size(const_pointer data) const { return data->length(); };
  iterator begin() { return iterator(iter_idx_min(), this); }
  const_iterator begin() const { return const_iterator(iter_idx_min(), this); }
  iterator end() { return iterator(iter_idx_max(), this); }
  const_iterator end() const { return const_iterator(iter_idx_max(), this); }
  DISABLE_COPY_ASSIGN(ObWkbGeomMultiPolygon);
};


// Cartesian GeometryCollection
// [bo][type][num][wkb...]
class ObWkbGeomCollection {
public:
  typedef ObWkbGeomCollection self;
  typedef ptrdiff_t index_type;
  typedef char value_type;
  typedef ObWkbConstIterator<value_type, self> const_iterator;
  typedef ObWkbIterator<value_type, self> iterator;
  typedef uint64_t size_type;
  typedef const value_type* const_pointer;
  typedef const value_type& const_reference;
  typedef value_type* pointer;
  typedef value_type& reference;
  typedef ptrdiff_t difference_type;
  typedef ObWkbGeomMultiPolygon sub_mp_type;
  typedef ObWkbGeomMultiPoint sub_mpt_type;
  typedef ObWkbGeomMultiLineString sub_ml_type;
public:
  ObWkbGeomCollection() {}
  ~ObWkbGeomCollection() {}
  uint32_t size() const;
  size_type length() const;
  inline ObGeoType type() const { return ObGeoType::GEOMETRYCOLLECTION; }
  inline ObGeoCRS crs() const { return ObGeoCRS::Cartesian; }
  // iter adapt
  index_type iter_idx_max() const { return size(); }
  index_type iter_idx_min() const { return 0; }
  char* ptr() const { return reinterpret_cast<char*>(const_cast<self*>(this)); }
  uint32_t data_offset() const { return WKB_COMMON_WKB_HEADER_LEN; }
  index_type et(index_type curidx) const { return curidx; }
  void get_sub_addr(const_pointer last_addr, index_type last_idx, index_type cur_idx,
      ObWkbIterOffsetArray* offsets, pointer& data);
  // sub obj interface
  size_type get_sub_size(const_pointer data) const;
  ObGeoType get_sub_type(const_pointer data) const;
  // iter interface
  iterator begin() { return iterator(iter_idx_min(), this); }
  const_iterator begin() const { return const_iterator(iter_idx_min(), this); }
  iterator end() { return iterator(iter_idx_max(), this); }
  const_iterator end() const { return const_iterator(iter_idx_max(), this); }
  DISABLE_COPY_ASSIGN(ObWkbGeomCollection);
};


// Geographic
#pragma pack(1)
class ObWkbGeogPoint {
public:
  ObWkbGeogPoint()
      : bo_(static_cast<uint8_t>(ObGeoWkbByteOrder::LittleEndian)),
        type_(static_cast<uint32_t>(ObGeoType::POINT)),
        x_(NAN),
        y_(NAN)
  {}
  ObWkbGeogPoint(const ObWkbGeogPoint& p);
  ~ObWkbGeogPoint() {}
  uint64_t length() const;
  inline ObGeoType type() const { return ObGeoType::POINT; }
  inline ObGeoCRS crs() const { return ObGeoCRS::Geographic; }
  ObGeoWkbByteOrder byteorder() const;
  void byteorder(ObGeoWkbByteOrder bo);
  template<std::size_t K>
  double get() const;
  template<std::size_t K>
  void set(double d);
  ObWkbGeogPoint& operator=(const ObWkbGeogPoint& p);
private:
  void refresh_type();
  uint8_t bo_;
  uint32_t type_;
  double x_;
  double y_;
};
#pragma pack()

// Inner Point for little endian
class ObWkbGeogInnerPoint {
public:
  ObWkbGeogInnerPoint() {}
  ObWkbGeogInnerPoint(const ObWkbGeogInnerPoint& p);
  ObWkbGeogInnerPoint(double x, double y) : x_(x), y_(y) {}
  ObWkbGeogInnerPoint(uint32_t srid, ObIAllocator *alloc) : x_(NAN), y_(NAN) { UNUSEDx(srid, alloc); }
  ~ObWkbGeogInnerPoint() {}
  uint64_t length() const;
  template<std::size_t K>
  double get() const;
  template<std::size_t K>
  void set(double d);
  ObWkbGeogInnerPoint& operator=(const ObWkbGeogInnerPoint& p);
  // TODO
  int64_t to_string(char *buffer, const int64_t length) const{
    UNUSED(buffer);
    UNUSED(length);
    return 0;
  }
private:
  double x_;
  double y_;
};

// Geograph linestring
// [bo][type][num][X][Y][...]
class ObWkbGeogLineString {
public:
  // type define
  typedef ObWkbGeogLineString self;
  typedef ptrdiff_t index_type;
  typedef ObWkbGeogInnerPoint value_type;
  typedef ObWkbConstIterator<value_type, self> const_iterator;
  typedef ObWkbIterator<value_type, self> iterator;
  typedef uint64_t size_type;
  typedef const value_type& const_reference;
  typedef const value_type* const_pointer;
  typedef value_type& reference;
  typedef value_type* pointer;
  typedef ptrdiff_t difference_type;
public:
  ObWkbGeogLineString() {}
  ~ObWkbGeogLineString() {}
  uint32_t size() const;
  size_type length() const;
  inline ObGeoType type() const { return ObGeoType::LINESTRING; }
  inline ObGeoCRS crs() const { return ObGeoCRS::Geographic; }
  // iter adaptor
  index_type iter_idx_max() const { return size(); }
  index_type iter_idx_min() const { return 0; }
  void get_sub_addr(const_pointer last_addr, index_type last_idx, index_type cur_idx,
      ObWkbIterOffsetArray* offsets, pointer& data);
  iterator begin() { return iterator(iter_idx_min(), this); }
  const_iterator begin() const { return const_iterator(iter_idx_min(), this); }
  iterator end() { return iterator(iter_idx_max(), this); }
  const_iterator end() const { return const_iterator(iter_idx_max(), this); }
  DISABLE_COPY_ASSIGN(ObWkbGeogLineString);
};

// Geograph linearring
// [num][X][Y][...]
class ObWkbGeogLinearRing {
public:
  // type define
  typedef ObWkbGeogLinearRing self;
  typedef ptrdiff_t index_type;
  typedef ObWkbGeogInnerPoint value_type;
  typedef ObWkbConstIterator<value_type, self> const_iterator;
  typedef ObWkbIterator<value_type, self> iterator;
  typedef uint64_t size_type;
  typedef const value_type* const_pointer;
  typedef const value_type& const_reference;
  typedef value_type* pointer;
  typedef value_type& reference;
  typedef ptrdiff_t difference_type;
public:
  ObWkbGeogLinearRing() {}
  ~ObWkbGeogLinearRing() {}
  uint32_t size() const;
  size_type length() const;
  // iter adaptor
  index_type iter_idx_max() const { return size(); }
  index_type iter_idx_min() const { return 0; }
  void get_sub_addr(const_pointer last_addr, index_type last_idx, index_type cur_idx,
      ObWkbIterOffsetArray* offsets, pointer& data);
  // iter adapt
  iterator begin() { return iterator(iter_idx_min(), this); }
  const_iterator begin() const { return const_iterator(iter_idx_min(), this); }
  iterator end() { return iterator(iter_idx_max(), this); }
  const_iterator end() const { return const_iterator(iter_idx_max(), this); }
  // for bg::correct
  void push_back(const ObWkbGeogInnerPoint &pt) {
    UNUSED(pt);
    throw ObGeoNotImplementedException(ObGeoCRS::Geographic, ObGeoType::POINT, "ObWkbGeogLinearRing::push_back");
  }
  void resize(uint64_t count) {
    UNUSED(count);
    throw ObGeoNotImplementedException(ObGeoCRS::Geographic, ObGeoType::POINT, "ObWkbGeogLinearRing::resize");
  }
  void clear() {
    throw ObGeoNotImplementedException(ObGeoCRS::Geographic, ObGeoType::POINT, "ObWkbGeogLinearRing::clear");
  }
  DISABLE_COPY_ASSIGN(ObWkbGeogLinearRing);
};

// Geograph InnerRings
// [bo][type][num][ex][inner_rings]
// for get num
class ObWkbGeogPolygonInnerRings {
public:
  typedef ObWkbGeogPolygonInnerRings self;
  typedef ptrdiff_t index_type;
  typedef ObWkbGeogLinearRing value_type;
  typedef ObWkbConstIterator<value_type, self> const_iterator;
  typedef ObWkbIterator<value_type, self> iterator;
  typedef uint64_t size_type;
  typedef const value_type* const_pointer;
  typedef const value_type& const_reference;
  typedef value_type* pointer;
  typedef value_type& reference;
  typedef ptrdiff_t difference_type;
public:
  ObWkbGeogPolygonInnerRings() {}
  ~ObWkbGeogPolygonInnerRings() {}
  uint32_t size() const;
  size_type length() const;
  // iter adaptor
  index_type iter_idx_max() const { return size() + 1; }  // for move over exterior
  index_type iter_idx_min() const { return 1; }   // for move over exterior
  char* ptr() const { return reinterpret_cast<char*>(const_cast<self*>(this)); }
  uint32_t data_offset() const { return WKB_COMMON_WKB_HEADER_LEN; }
  index_type et(index_type curidx) const { return curidx; }
  void get_sub_addr(const_pointer last_addr, index_type last_idx, index_type cur_idx,
      ObWkbIterOffsetArray* offsets, pointer& data);
  size_type get_sub_size(const_pointer data) const { return data->length(); };
  // iter interface
  iterator begin() { return iterator(iter_idx_min(), this); }
  const_iterator begin() const { return const_iterator(iter_idx_min(), this); }
  iterator end() { return iterator(iter_idx_max(), this); }
  const_iterator end() const { return const_iterator(iter_idx_max(), this); }
  DISABLE_COPY_ASSIGN(ObWkbGeogPolygonInnerRings);
};

// Geograph Polygon
// [bo][type][num][ex][inner_rings]
class ObWkbGeogPolygon {
public:
  ObWkbGeogPolygon() {}
  ~ObWkbGeogPolygon() {}
  uint32_t size() const;
  uint64_t length() const;
  inline ObGeoType type() const { return ObGeoType::POLYGON; }
  inline ObGeoCRS crs() const { return ObGeoCRS::Geographic; }
  ObWkbGeogLinearRing& exterior_ring();
  const ObWkbGeogLinearRing& exterior_ring() const;
  ObWkbGeogPolygonInnerRings& inner_rings();
  const ObWkbGeogPolygonInnerRings& inner_rings() const;
  DISABLE_COPY_ASSIGN(ObWkbGeogPolygon);
};


// Geograph MultiPoint
// [bo][type][num][point]
class ObWkbGeogMultiPoint {
public:
  typedef ObWkbGeogMultiPoint self;
  typedef ptrdiff_t index_type;
  typedef ObWkbGeogInnerPoint value_type;
  typedef ObWkbConstIterator<value_type, self> const_iterator;
  typedef ObWkbIterator<value_type, self> iterator;
  typedef uint64_t size_type;
  typedef const value_type* const_pointer;
  typedef const value_type& const_reference;
  typedef value_type* pointer;
  typedef value_type& reference;
  typedef ptrdiff_t difference_type;
public:
  ObWkbGeogMultiPoint() {}
  ~ObWkbGeogMultiPoint() {}
  uint32_t size() const;
  size_type length() const;
  inline ObGeoType type() const { return ObGeoType::MULTIPOINT; }
  inline ObGeoCRS crs() const { return ObGeoCRS::Geographic; }
  // iter adaptor
  index_type iter_idx_max() const { return size(); }
  index_type iter_idx_min() const { return 0; }
  void get_sub_addr(const_pointer last_addr, index_type last_idx, index_type cur_idx,
      ObWkbIterOffsetArray* offsets, pointer& data);
  iterator begin() { return iterator(iter_idx_min(), this); }
  const_iterator begin() const { return const_iterator(iter_idx_min(), this); }
  iterator end() { return iterator(iter_idx_max(), this); }
  const_iterator end() const { return const_iterator(iter_idx_max(), this); }
  DISABLE_COPY_ASSIGN(ObWkbGeogMultiPoint);
};


// Geograph MultiLineString
// [bo][type][num][LineString...]
class ObWkbGeogMultiLineString {
public:
  typedef ObWkbGeogMultiLineString self;
  typedef ptrdiff_t index_type;
  typedef ObWkbGeogLineString value_type;
  typedef ObWkbConstIterator<value_type, self> const_iterator;
  typedef ObWkbIterator<value_type, self> iterator;
  typedef size_t size_type;
  typedef const value_type* const_pointer;
  typedef const value_type& const_reference;
  typedef value_type* pointer;
  typedef value_type& reference;
  typedef ptrdiff_t difference_type;
public:
  ObWkbGeogMultiLineString() {}
  ~ObWkbGeogMultiLineString() {}
  uint32_t size() const;
  size_type length() const;
  inline ObGeoType type() const { return ObGeoType::MULTILINESTRING; }
  inline ObGeoCRS crs() const { return ObGeoCRS::Geographic; }
  // iter adaptor
  index_type iter_idx_max() const { return size(); }
  index_type iter_idx_min() const { return 0; }
  char* ptr() const { return reinterpret_cast<char*>(const_cast<self*>(this)); }
  uint32_t data_offset() const { return WKB_COMMON_WKB_HEADER_LEN; }
  index_type et(index_type curidx) const { return curidx; }
  void get_sub_addr(const_pointer last_addr, index_type last_idx, index_type cur_idx,
      ObWkbIterOffsetArray* offsets, pointer& data);
  size_type get_sub_size(const_pointer data) const { return data->length(); };
  iterator begin() { return iterator(iter_idx_min(), this); }
  const_iterator begin() const { return const_iterator(iter_idx_min(), this); }
  iterator end() { return iterator(iter_idx_max(), this); }
  const_iterator end() const { return const_iterator(iter_idx_max(), this); }
  DISABLE_COPY_ASSIGN(ObWkbGeogMultiLineString);
};


// Geograph MultiPolygon
// [bo][type][num][Polygon...]
class ObWkbGeogMultiPolygon {
public:
  typedef ObWkbGeogMultiPolygon self;
  typedef ptrdiff_t index_type;
  typedef ObWkbGeogPolygon value_type;
  typedef ObWkbConstIterator<value_type, self> const_iterator;
  typedef ObWkbIterator<value_type, self> iterator;
  typedef size_t size_type;
  typedef const value_type* const_pointer;
  typedef const value_type& const_reference;
  typedef value_type* pointer;
  typedef value_type& reference;
  typedef ptrdiff_t difference_type;
public:
  ObWkbGeogMultiPolygon() {}
  ~ObWkbGeogMultiPolygon() {}
  uint32_t size() const;
  size_type length() const;
  inline ObGeoType type() const { return ObGeoType::MULTIPOLYGON; }
  inline ObGeoCRS crs() const { return ObGeoCRS::Geographic; }
  // iter adaptor
  index_type iter_idx_max() const { return size(); }
  index_type iter_idx_min() const { return 0; }
  char* ptr() const { return reinterpret_cast<char*>(const_cast<self*>(this)); }
  uint32_t data_offset() const { return WKB_COMMON_WKB_HEADER_LEN; }
  index_type et(index_type curidx) const { return curidx; }
  void get_sub_addr(const_pointer last_addr, index_type last_idx, index_type cur_idx,
      ObWkbIterOffsetArray* offsets, pointer& data);
  size_type get_sub_size(const_pointer data) const { return data->length(); };
  iterator begin() { return iterator(iter_idx_min(), this); }
  const_iterator begin() const { return const_iterator(iter_idx_min(), this); }
  iterator end() { return iterator(iter_idx_max(), this); }
  const_iterator end() const { return const_iterator(iter_idx_max(), this); }
  DISABLE_COPY_ASSIGN(ObWkbGeogMultiPolygon);
};


// Geograph GeometryCollection
// [bo][type][num][wkb...]
class ObWkbGeogCollection {
public:
  typedef ObWkbGeogCollection self;
  typedef ptrdiff_t index_type;
  typedef char value_type;
  typedef ObWkbConstIterator<value_type, self> const_iterator;
  typedef ObWkbIterator<value_type, self> iterator;
  typedef uint64_t size_type;
  typedef const value_type* const_pointer;
  typedef const value_type& const_reference;
  typedef value_type* pointer;
  typedef value_type& reference;
  typedef ptrdiff_t difference_type;
  typedef ObWkbGeogMultiPolygon sub_mp_type;
  typedef ObWkbGeogMultiPoint sub_mpt_type;
  typedef ObWkbGeogMultiLineString sub_ml_type;
public:
  ObWkbGeogCollection() {}
  ~ObWkbGeogCollection() {}
  uint32_t size() const;
  size_type length() const;
  inline ObGeoType type() const { return ObGeoType::GEOMETRYCOLLECTION; }
  inline ObGeoCRS crs() const { return ObGeoCRS::Geographic; }
  // iter adapt
  index_type iter_idx_max() const { return size(); }
  index_type iter_idx_min() const { return 0; }
  char* ptr() const { return reinterpret_cast<char*>(const_cast<self*>(this)); }
  uint32_t data_offset() const { return WKB_COMMON_WKB_HEADER_LEN; }
  index_type et(index_type curidx) const { return curidx; }
  void get_sub_addr(const_pointer last_addr, index_type last_idx, index_type cur_idx,
      ObWkbIterOffsetArray* offsets, pointer& data);
  size_type get_sub_size(const_pointer data) const;
  ObGeoType get_sub_type(const_pointer data) const;
  // iter interface
  iterator begin() { return iterator(iter_idx_min(), this); }
  const_iterator begin() const { return const_iterator(iter_idx_min(), this); }
  iterator end() { return iterator(iter_idx_max(), this); }
  const_iterator end() const { return const_iterator(iter_idx_max(), this); }
  DISABLE_COPY_ASSIGN(ObWkbGeogCollection);
};


} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_LIB_GEO_OB_GEO_BIN_
