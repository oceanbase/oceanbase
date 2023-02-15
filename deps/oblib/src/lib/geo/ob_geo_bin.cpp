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
 * This file contains implementation support for the geo bin abstraction.
 */

#define USING_LOG_PREFIX LIB
#include "ob_geo_bin.h"

namespace oceanbase {
namespace common {

/* CartesianPoint */
uint64_t ObWkbGeomPoint::length() const
{
  return sizeof(double) * 2 + sizeof(uint32_t) + sizeof(uint8_t);
}

ObGeoWkbByteOrder ObWkbGeomPoint::byteorder() const
{
  char* ptr = reinterpret_cast<char*>(const_cast<ObWkbGeomPoint*>(this));
  return static_cast<ObGeoWkbByteOrder>(*reinterpret_cast<uint8_t*>(ptr));
}

// only set byteorder for copy situation, no need to transform X,Y
void ObWkbGeomPoint::byteorder(ObGeoWkbByteOrder bo)
{
  char* ptr = reinterpret_cast<char*>(const_cast<ObWkbGeomPoint*>(this));
  *reinterpret_cast<uint8_t*>(ptr) = static_cast<uint8_t>(bo);
}

template<>
double ObWkbGeomPoint::get<0>() const
{
  ObGeoWkbByteOrder bo = byteorder();
  char* ptr = reinterpret_cast<char*>(const_cast<ObWkbGeomPoint*>(this));
  ptr = ptr + sizeof(uint32_t) + sizeof(uint8_t);
  return ObGeoWkbByteOrderUtil::read<double>(ptr, bo);
}

template<>
double ObWkbGeomPoint::get<1>() const
{
  ObGeoWkbByteOrder bo = byteorder();
  char* ptr = reinterpret_cast<char*>(const_cast<ObWkbGeomPoint*>(this));
  ptr = ptr + sizeof(uint32_t) + sizeof(uint8_t) + sizeof(double);
  return ObGeoWkbByteOrderUtil::read<double>(ptr, bo);
}

template<>
void ObWkbGeomPoint::set<0>(double d)
{
  ObGeoWkbByteOrder bo = byteorder();
  char* ptr = reinterpret_cast<char*>(const_cast<ObWkbGeomPoint*>(this));
  ptr = ptr + sizeof(uint32_t) + sizeof(uint8_t);
  ObGeoWkbByteOrderUtil::write<double>(ptr, d, bo);
}

template<>
void ObWkbGeomPoint::set<1>(double d)
{
  ObGeoWkbByteOrder bo = byteorder();
  char* ptr = reinterpret_cast<char*>(const_cast<ObWkbGeomPoint*>(this));
  ptr = ptr + sizeof(uint32_t) + sizeof(uint8_t) + sizeof(double);
  ObGeoWkbByteOrderUtil::write<double>(ptr, d, bo);
}

ObWkbGeomPoint::ObWkbGeomPoint(const ObWkbGeomPoint& p)
{
  byteorder(p.byteorder());
  refresh_type();
  set<0>(p.get<0>());
  set<1>(p.get<1>());
}

ObWkbGeomPoint& ObWkbGeomPoint::operator=(const ObWkbGeomPoint& p)
{
  // byte order
  this->byteorder(p.byteorder());
  this->refresh_type();
  this->set<0>(p.get<0>());
  this->set<1>(p.get<1>());
  return *this;
}

void ObWkbGeomPoint::refresh_type()
{
  ObGeoWkbByteOrder bo = byteorder();
  char* ptr = reinterpret_cast<char*>(const_cast<ObWkbGeomPoint*>(this));
  ptr += sizeof(uint8_t);
  uint32_t type = static_cast<uint32_t>(ObGeoType::POINT);
  ObGeoWkbByteOrderUtil::write<uint32_t>(ptr, type, bo);
}

// ObWkbGeomInnerPoint
uint64_t ObWkbGeomInnerPoint::length() const {
  return sizeof(double) * 2;
}

template<>
double ObWkbGeomInnerPoint::get<0>() const
{
  ObGeoWkbByteOrder bo = ObGeoWkbByteOrder::LittleEndian;
  char* ptr = reinterpret_cast<char*>(const_cast<ObWkbGeomInnerPoint*>(this));
  return ObGeoWkbByteOrderUtil::read<double>(ptr, bo);
}

template<>
double ObWkbGeomInnerPoint::get<1>() const
{
  ObGeoWkbByteOrder bo = ObGeoWkbByteOrder::LittleEndian;
  char* ptr = reinterpret_cast<char*>(const_cast<ObWkbGeomInnerPoint*>(this));
  ptr = ptr +  sizeof(double);
  return ObGeoWkbByteOrderUtil::read<double>(ptr, bo);
}

template<>
void ObWkbGeomInnerPoint::set<0>(double d)
{
  ObGeoWkbByteOrder bo = ObGeoWkbByteOrder::LittleEndian;
  char* ptr = reinterpret_cast<char*>(const_cast<ObWkbGeomInnerPoint*>(this));
  ObGeoWkbByteOrderUtil::write<double>(ptr, d, bo);
}

template<>
void ObWkbGeomInnerPoint::set<1>(double d)
{
  ObGeoWkbByteOrder bo = ObGeoWkbByteOrder::LittleEndian;
  char* ptr = reinterpret_cast<char*>(const_cast<ObWkbGeomInnerPoint*>(this));
  ptr = ptr + sizeof(double);
  ObGeoWkbByteOrderUtil::write<double>(ptr, d, bo);
}

ObWkbGeomInnerPoint& ObWkbGeomInnerPoint::operator=(const ObWkbGeomInnerPoint& p){
  // byte order
  this->set<0>(p.get<0>());
  this->set<1>(p.get<1>());
  return *this;
}

ObWkbGeomInnerPoint::ObWkbGeomInnerPoint(const ObWkbGeomInnerPoint& p)
{
  set<0>(p.get<0>());
  set<1>(p.get<1>());
}

// Cartesian linestring
uint32_t ObWkbGeomLineString::size() const
{
  char *ptr = reinterpret_cast<char*>(const_cast<ObWkbGeomLineString*>(this));
  ObGeoWkbByteOrder bo = static_cast<ObGeoWkbByteOrder>(*ptr);
  ptr += sizeof(uint8_t) + sizeof(uint32_t); // move [bo][type]
  return ObGeoWkbByteOrderUtil::read<uint32_t>(ptr, bo);
}

ObWkbGeomLineString::size_type ObWkbGeomLineString::length() const
{
  size_type s = WKB_COMMON_WKB_HEADER_LEN;
  s += size() * (sizeof(double) * 2);
  return s;
}

// iter adaptor
void ObWkbGeomLineString::get_sub_addr(const_pointer last_addr, index_type last_idx, index_type cur_idx,
  ObWkbIterOffsetArray* offsets, pointer& data)
{
  UNUSED(last_addr);
  UNUSED(last_idx);
  UNUSED(offsets);
  char *ptr = reinterpret_cast<char*>(const_cast<ObWkbGeomLineString*>(this));
  uint64_t offset = WKB_COMMON_WKB_HEADER_LEN;
  offset += cur_idx * sizeof(double) * 2;
  data = reinterpret_cast<pointer>(ptr + offset);
}

// Cartesian linearring
uint32_t ObWkbGeomLinearRing::size() const
{
  char *ptr = reinterpret_cast<char*>(const_cast<ObWkbGeomLinearRing*>(this));
  return ObGeoWkbByteOrderUtil::read<uint32_t>(ptr, ObGeoWkbByteOrder::LittleEndian);
}

ObWkbGeomLinearRing::size_type ObWkbGeomLinearRing::length() const
{
  size_type s = sizeof(uint32_t);
  s += size() * (sizeof(double) * 2);
  return s;
}

// iter adaptor
void ObWkbGeomLinearRing::get_sub_addr(const_pointer last_addr, index_type last_idx, index_type cur_idx,
  ObWkbIterOffsetArray* offsets, pointer& data)
{
  UNUSED(last_addr);
  UNUSED(last_idx);
  UNUSED(offsets);
  char *ptr = reinterpret_cast<char*>(const_cast<ObWkbGeomLinearRing*>(this));
  uint64_t offset = sizeof(uint32_t);
  offset += cur_idx * sizeof(double) * 2;
  data = reinterpret_cast<pointer>(ptr + offset);
}


// Cartesian InnerRings
// [bo][type][num][ex][inner_rings]
uint32_t ObWkbGeomPolygonInnerRings::size() const
{
  char *ptr = reinterpret_cast<char*>(const_cast<ObWkbGeomPolygonInnerRings*>(this));
  ObGeoWkbByteOrder bo = static_cast<ObGeoWkbByteOrder>(*ptr);
  ptr += sizeof(uint8_t) + sizeof(uint32_t); // move [bo][type]
  return ObGeoWkbByteOrderUtil::read<uint32_t>(ptr, bo) - 1;
}

ObWkbGeomPolygonInnerRings::size_type ObWkbGeomPolygonInnerRings::length() const
{
  return ObWkbUtils::sum_sub_size(*this);
}

// iter adaptor
void ObWkbGeomPolygonInnerRings::get_sub_addr(const_pointer last_addr, index_type last_idx, index_type cur_idx,
  ObWkbIterOffsetArray* offsets, pointer& data)
{
  ObWkbUtils::get_sub_addr_common(*this, last_addr, last_idx, cur_idx, offsets, data);
}


// Cartesian Polygon
// [bo][type][num][ex][inner_rings]
uint32_t ObWkbGeomPolygon::size() const
{
  char *ptr = reinterpret_cast<char*>(const_cast<ObWkbGeomPolygon*>(this));
  ObGeoWkbByteOrder bo = static_cast<ObGeoWkbByteOrder>(*ptr);
  ptr += sizeof(uint8_t) + sizeof(uint32_t); // move [bo][type]
  return ObGeoWkbByteOrderUtil::read<uint32_t>(ptr, bo);
}

uint64_t ObWkbGeomPolygon::length() const
{
  uint64_t s = WKB_COMMON_WKB_HEADER_LEN;
  s += exterior_ring().length() + inner_rings().length();
  return s;
}

ObWkbGeomLinearRing& ObWkbGeomPolygon::exterior_ring()
{
  char *ptr = reinterpret_cast<char*>(const_cast<ObWkbGeomPolygon*>(this));
  ptr += WKB_COMMON_WKB_HEADER_LEN; // move [bo][type][num]
  return *reinterpret_cast<ObWkbGeomLinearRing*>(ptr);
}

const ObWkbGeomLinearRing& ObWkbGeomPolygon::exterior_ring() const
{
  const char *ptr = reinterpret_cast<const char*>(this);
  ptr += WKB_COMMON_WKB_HEADER_LEN; // move [bo][type][num]
  return *reinterpret_cast<const ObWkbGeomLinearRing*>(ptr);
}

ObWkbGeomPolygonInnerRings& ObWkbGeomPolygon::inner_rings()
{
  char *ptr = reinterpret_cast<char*>(const_cast<ObWkbGeomPolygon*>(this));
  return *reinterpret_cast<ObWkbGeomPolygonInnerRings*>(ptr);
}

const ObWkbGeomPolygonInnerRings& ObWkbGeomPolygon::inner_rings() const
{
  const char *ptr = reinterpret_cast<const char*>(this);
  return *reinterpret_cast<const ObWkbGeomPolygonInnerRings*>(ptr);
}


// Cartesian MultiPoint
uint32_t ObWkbGeomMultiPoint::size() const
{
  char *ptr = reinterpret_cast<char*>(const_cast<ObWkbGeomMultiPoint*>(this));
  ObGeoWkbByteOrder bo = static_cast<ObGeoWkbByteOrder>(*ptr);
  ptr += sizeof(uint8_t) + sizeof(uint32_t); // move [bo][type]
  return ObGeoWkbByteOrderUtil::read<uint32_t>(ptr, bo);
}

ObWkbGeomMultiPoint::size_type ObWkbGeomMultiPoint::length() const
{
  size_type s = WKB_COMMON_WKB_HEADER_LEN;
  ObWkbGeomPoint p;
  s += size() * p.length();
  return s;
}

// iter adaptor
void ObWkbGeomMultiPoint::get_sub_addr(const_pointer last_addr, index_type last_idx, index_type cur_idx,
  ObWkbIterOffsetArray* offsets, pointer& data)
{
  UNUSED(last_addr);
  UNUSED(last_idx);
  UNUSED(offsets);
  char *ptr = reinterpret_cast<char*>(const_cast<ObWkbGeomMultiPoint*>(this));
  uint64_t offset = WKB_COMMON_WKB_HEADER_LEN;
  ObWkbGeomPoint p;
  offset += cur_idx * p.length();
  offset += sizeof(uint8_t) + sizeof(uint32_t);
  data = reinterpret_cast<pointer>(ptr + offset);
}


// Cartesian MultiLineString
uint32_t ObWkbGeomMultiLineString::size() const
{
  char *ptr = reinterpret_cast<char*>(const_cast<ObWkbGeomMultiLineString*>(this));
  ObGeoWkbByteOrder bo = static_cast<ObGeoWkbByteOrder>(*ptr);
  ptr += sizeof(uint8_t) + sizeof(uint32_t); // move [bo][type]
  return ObGeoWkbByteOrderUtil::read<uint32_t>(ptr, bo);
}

ObWkbGeomMultiLineString::size_type ObWkbGeomMultiLineString::length() const
{
  size_type s = WKB_COMMON_WKB_HEADER_LEN;
  s += ObWkbUtils::sum_sub_size(*this);
  return s;
}

// iter adaptor
void ObWkbGeomMultiLineString::get_sub_addr(const_pointer last_addr, index_type last_idx, index_type cur_idx,
  ObWkbIterOffsetArray* offsets, pointer& data)
{
  ObWkbUtils::get_sub_addr_common(*this, last_addr, last_idx, cur_idx, offsets, data);
}


// Cartesian MultiPolygon
uint32_t ObWkbGeomMultiPolygon::size() const
{
  char *ptr = reinterpret_cast<char*>(const_cast<ObWkbGeomMultiPolygon*>(this));
  ObGeoWkbByteOrder bo = static_cast<ObGeoWkbByteOrder>(*ptr);
  ptr += sizeof(uint8_t) + sizeof(uint32_t); // move [bo][type]
  return ObGeoWkbByteOrderUtil::read<uint32_t>(ptr, bo);
}

ObWkbGeomMultiPolygon::size_type ObWkbGeomMultiPolygon::length() const
{
  size_type s = WKB_COMMON_WKB_HEADER_LEN;
  s += ObWkbUtils::sum_sub_size(*this);
  return s;
}

// iter adaptor
void ObWkbGeomMultiPolygon::get_sub_addr(const_pointer last_addr, index_type last_idx, index_type cur_idx,
  ObWkbIterOffsetArray* offsets, pointer& data)
{
  ObWkbUtils::get_sub_addr_common(*this, last_addr, last_idx, cur_idx, offsets, data);
}


// Cartesian GeometryCollection
uint32_t ObWkbGeomCollection::size() const
{
  char *ptr = reinterpret_cast<char*>(const_cast<ObWkbGeomCollection*>(this));
  ObGeoWkbByteOrder bo = static_cast<ObGeoWkbByteOrder>(*ptr);
  ptr += sizeof(uint8_t) + sizeof(uint32_t); // move [bo][type]
  return ObGeoWkbByteOrderUtil::read<uint32_t>(ptr, bo);
}

ObWkbGeomCollection::size_type ObWkbGeomCollection::length() const
{
  size_type s = WKB_COMMON_WKB_HEADER_LEN;
  s += ObWkbUtils::sum_sub_size(*this);
  return s;
}

// iter adaptor
void ObWkbGeomCollection::get_sub_addr(const_pointer last_addr, index_type last_idx, index_type cur_idx,
  ObWkbIterOffsetArray* offsets, pointer& data)
{
  ObWkbUtils::get_sub_addr_common(*this, last_addr, last_idx, cur_idx, offsets, data);
}

ObWkbGeomCollection::size_type ObWkbGeomCollection::get_sub_size(const_pointer data) const
{
  size_type s = 0;
  ObGeoWkbByteOrder bo = static_cast<ObGeoWkbByteOrder>(*data);
  pointer ptr = const_cast<pointer>(data) + sizeof(uint8_t);
  ObGeoType type = static_cast<ObGeoType>(ObGeoWkbByteOrderUtil::read<uint32_t>(ptr, bo));
  switch (type) {
    case ObGeoType::POINT: {
      const ObWkbGeomPoint* sub_geo = reinterpret_cast<const ObWkbGeomPoint*>(data);
      s = sub_geo->length();
      break;
    }
    case ObGeoType::LINESTRING: {
      const ObWkbGeomLineString* sub_geo = reinterpret_cast<const ObWkbGeomLineString*>(data);
      s = sub_geo->length();
      break;
    }
    case ObGeoType::POLYGON: {
      const ObWkbGeomPolygon* sub_geo = reinterpret_cast<const ObWkbGeomPolygon*>(data);
      s = sub_geo->length();
      break;
    }
    case ObGeoType::MULTIPOINT: {
      const ObWkbGeomMultiPoint* sub_geo = reinterpret_cast<const ObWkbGeomMultiPoint*>(data);
      s = sub_geo->length();
      break;
    }
    case ObGeoType::MULTILINESTRING: {
      const ObWkbGeomMultiLineString* sub_geo = reinterpret_cast<const ObWkbGeomMultiLineString*>(data);
      s = sub_geo->length();
      break;
    }
    case ObGeoType::MULTIPOLYGON: {
      const ObWkbGeomMultiPolygon* sub_geo = reinterpret_cast<const ObWkbGeomMultiPolygon*>(data);
      s = sub_geo->length();
      break;
    }
    case ObGeoType::GEOMETRYCOLLECTION: {
      const ObWkbGeomCollection* sub_geo = reinterpret_cast<const ObWkbGeomCollection*>(data);
      s = sub_geo->length();
      break;
    }
    default: {
      LOG_WARN_RET(OB_INVALID_ARGUMENT, "Invalid geo type.", K(type));
    }
  }
  return s;
}

ObGeoType ObWkbGeomCollection::get_sub_type(const_pointer data) const
{
  ObGeoWkbByteOrder bo = static_cast<ObGeoWkbByteOrder>(*data);
  pointer ptr = const_cast<pointer>(data) + sizeof(uint8_t);
  ObGeoType type = static_cast<ObGeoType>(ObGeoWkbByteOrderUtil::read<uint32_t>(ptr, bo));
  return type;
}


/* GeographicPoint */
uint64_t ObWkbGeogPoint::length() const {
  return sizeof(double) * 2 + sizeof(uint32_t) + sizeof(uint8_t);
}

ObGeoWkbByteOrder ObWkbGeogPoint::byteorder() const {
  char* ptr = reinterpret_cast<char*>(const_cast<ObWkbGeogPoint*>(this));
  return static_cast<ObGeoWkbByteOrder>(*reinterpret_cast<uint8_t*>(ptr));
}

// only set byteorder for copy situation, no need to transform X,Y
void ObWkbGeogPoint::byteorder(ObGeoWkbByteOrder bo) {
  char* ptr = reinterpret_cast<char*>(const_cast<ObWkbGeogPoint*>(this));
  *reinterpret_cast<uint8_t*>(ptr) = static_cast<uint8_t>(bo);
}

template<>
double ObWkbGeogPoint::get<0>() const
{
  ObGeoWkbByteOrder bo = byteorder();
  char* ptr = reinterpret_cast<char*>(const_cast<ObWkbGeogPoint*>(this));
  ptr = ptr + sizeof(uint32_t) + sizeof(uint8_t);
  return ObGeoWkbByteOrderUtil::read<double>(ptr, bo);
}

template<>
double ObWkbGeogPoint::get<1>() const
{
  ObGeoWkbByteOrder bo = byteorder();
  char* ptr = reinterpret_cast<char*>(const_cast<ObWkbGeogPoint*>(this));
  ptr = ptr + sizeof(uint32_t) + sizeof(uint8_t) + sizeof(double);
  return ObGeoWkbByteOrderUtil::read<double>(ptr, bo);
}

template<>
void ObWkbGeogPoint::set<0>(double d)
{
  ObGeoWkbByteOrder bo = byteorder();
  char* ptr = reinterpret_cast<char*>(const_cast<ObWkbGeogPoint*>(this));
  ptr = ptr + sizeof(uint32_t) + sizeof(uint8_t);
  ObGeoWkbByteOrderUtil::write<double>(ptr, d, bo);
}

template<>
void ObWkbGeogPoint::set<1>(double d)
{
  ObGeoWkbByteOrder bo = byteorder();
  char* ptr = reinterpret_cast<char*>(const_cast<ObWkbGeogPoint*>(this));
  ptr = ptr + sizeof(uint32_t) + sizeof(uint8_t) + sizeof(double);
  ObGeoWkbByteOrderUtil::write<double>(ptr, d, bo);
}

ObWkbGeogPoint& ObWkbGeogPoint::operator=(const ObWkbGeogPoint& p) {
  // byte order
  this->byteorder(p.byteorder());
  this->refresh_type();
  this->set<0>(p.get<0>());
  this->set<1>(p.get<1>());
  return *this;
}

void ObWkbGeogPoint::refresh_type() {
  ObGeoWkbByteOrder bo = byteorder();
  char* ptr = reinterpret_cast<char*>(const_cast<ObWkbGeogPoint*>(this));
  ptr += sizeof(uint8_t);
  uint32_t type = static_cast<uint32_t>(ObGeoType::POINT);
  ObGeoWkbByteOrderUtil::write<uint32_t>(ptr, type, bo);
}

ObWkbGeogPoint::ObWkbGeogPoint(const ObWkbGeogPoint& p) {
  byteorder(p.byteorder());
  refresh_type();
  set<0>(p.get<0>());
  set<1>(p.get<1>());
}

// ObWkbGeogInnerPoint
uint64_t ObWkbGeogInnerPoint::length() const {
  return sizeof(double) * 2;
}

template<>
double ObWkbGeogInnerPoint::get<0>() const
{
  ObGeoWkbByteOrder bo = ObGeoWkbByteOrder::LittleEndian;
  char* ptr = reinterpret_cast<char*>(const_cast<ObWkbGeogInnerPoint*>(this));
  return ObGeoWkbByteOrderUtil::read<double>(ptr, bo);
}

template<>
double ObWkbGeogInnerPoint::get<1>() const
{
  ObGeoWkbByteOrder bo = ObGeoWkbByteOrder::LittleEndian;
  char* ptr = reinterpret_cast<char*>(const_cast<ObWkbGeogInnerPoint*>(this));
  ptr = ptr +  sizeof(double);
  return ObGeoWkbByteOrderUtil::read<double>(ptr, bo);
}

template<>
void ObWkbGeogInnerPoint::set<0>(double d)
{
  ObGeoWkbByteOrder bo = ObGeoWkbByteOrder::LittleEndian;
  char* ptr = reinterpret_cast<char*>(const_cast<ObWkbGeogInnerPoint*>(this));
  ObGeoWkbByteOrderUtil::write<double>(ptr, d, bo);
}

template<>
void ObWkbGeogInnerPoint::set<1>(double d)
{
  ObGeoWkbByteOrder bo = ObGeoWkbByteOrder::LittleEndian;
  char* ptr = reinterpret_cast<char*>(const_cast<ObWkbGeogInnerPoint*>(this));
  ptr = ptr + sizeof(double);
  ObGeoWkbByteOrderUtil::write<double>(ptr, d, bo);
}

ObWkbGeogInnerPoint::ObWkbGeogInnerPoint(const ObWkbGeogInnerPoint& p) {
  // byte order
  this->set<0>(p.get<0>());
  this->set<1>(p.get<1>());
}

ObWkbGeogInnerPoint& ObWkbGeogInnerPoint::operator=(const ObWkbGeogInnerPoint& p) {
  // byte order
  this->set<0>(p.get<0>());
  this->set<1>(p.get<1>());
  return *this;
}

// Geograph linestring
uint32_t ObWkbGeogLineString::size() const
{
  char *ptr = reinterpret_cast<char*>(const_cast<ObWkbGeogLineString*>(this));
  ObGeoWkbByteOrder bo = static_cast<ObGeoWkbByteOrder>(*ptr);
  ptr += sizeof(uint8_t) + sizeof(uint32_t); // move [bo][type]
  return ObGeoWkbByteOrderUtil::read<uint32_t>(ptr, bo);
}

ObWkbGeogLineString::size_type ObWkbGeogLineString::length() const
{
  size_type s = WKB_COMMON_WKB_HEADER_LEN;
  s += size() * (sizeof(double) * 2);
  return s;
}

// iter adaptor
void ObWkbGeogLineString::get_sub_addr(const_pointer last_addr, index_type last_idx, index_type cur_idx,
  ObWkbIterOffsetArray* offsets, pointer& data)
{
  UNUSED(last_addr);
  UNUSED(last_idx);
  UNUSED(offsets);
  char *ptr = reinterpret_cast<char*>(const_cast<ObWkbGeogLineString*>(this));
  uint64_t offset = WKB_COMMON_WKB_HEADER_LEN;
  offset += cur_idx * sizeof(double) * 2;
  data = reinterpret_cast<pointer>(ptr + offset);
}

// Geograph linearring
uint32_t ObWkbGeogLinearRing::size() const
{
  char *ptr = reinterpret_cast<char*>(const_cast<ObWkbGeogLinearRing*>(this));
  return ObGeoWkbByteOrderUtil::read<uint32_t>(ptr, ObGeoWkbByteOrder::LittleEndian);
}

ObWkbGeogLinearRing::size_type ObWkbGeogLinearRing::length() const
{
  size_type s = sizeof(uint32_t);
  s += size() * (sizeof(double) * 2);
  return s;
}

// iter adaptor
void ObWkbGeogLinearRing::get_sub_addr(const_pointer last_addr, index_type last_idx, index_type cur_idx,
  ObWkbIterOffsetArray* offsets, pointer& data)
{
  UNUSED(last_addr);
  UNUSED(last_idx);
  UNUSED(offsets);
  char *ptr = reinterpret_cast<char*>(const_cast<ObWkbGeogLinearRing*>(this));
  uint64_t offset = sizeof(uint32_t);
  offset += cur_idx * sizeof(double) * 2;
  data = reinterpret_cast<pointer>(ptr + offset);
}


// Geograph InnerRings
// [bo][type][num][ex][inner_rings]
uint32_t ObWkbGeogPolygonInnerRings::size() const
{
  char *ptr = reinterpret_cast<char*>(const_cast<ObWkbGeogPolygonInnerRings*>(this));
  ObGeoWkbByteOrder bo = static_cast<ObGeoWkbByteOrder>(*ptr);
  ptr += sizeof(uint8_t) + sizeof(uint32_t); // move [bo][type]
  return ObGeoWkbByteOrderUtil::read<uint32_t>(ptr, bo) - 1;
}

ObWkbGeogPolygonInnerRings::size_type ObWkbGeogPolygonInnerRings::length() const
{
  return ObWkbUtils::sum_sub_size(*this);
}

// iter adaptor
void ObWkbGeogPolygonInnerRings::get_sub_addr(const_pointer last_addr, index_type last_idx, index_type cur_idx,
  ObWkbIterOffsetArray* offsets, pointer& data)
{
  ObWkbUtils::get_sub_addr_common(*this, last_addr, last_idx, cur_idx, offsets, data);
}


// Geograph Polygon
// [bo][type][num][ex][inner_rings]
uint32_t ObWkbGeogPolygon::size() const
{
  char *ptr = reinterpret_cast<char*>(const_cast<ObWkbGeogPolygon*>(this));
  ObGeoWkbByteOrder bo = static_cast<ObGeoWkbByteOrder>(*ptr);
  ptr += sizeof(uint8_t) + sizeof(uint32_t); // move [bo][type]
  return ObGeoWkbByteOrderUtil::read<uint32_t>(ptr, bo);
}

uint64_t ObWkbGeogPolygon::length() const
{
  uint64_t s = WKB_COMMON_WKB_HEADER_LEN;
  s += exterior_ring().length() + inner_rings().length();
  return s;
}

ObWkbGeogLinearRing& ObWkbGeogPolygon::exterior_ring()
{
  char *ptr = reinterpret_cast<char*>(const_cast<ObWkbGeogPolygon*>(this));
  ptr += WKB_COMMON_WKB_HEADER_LEN; // move [bo][type][num]
  return *reinterpret_cast<ObWkbGeogLinearRing*>(ptr);
}

const ObWkbGeogLinearRing& ObWkbGeogPolygon::exterior_ring() const
{
  const char *ptr = reinterpret_cast<const char*>(this);
  ptr += WKB_COMMON_WKB_HEADER_LEN; // move [bo][type][num]
  return *reinterpret_cast<const ObWkbGeogLinearRing*>(ptr);
}

ObWkbGeogPolygonInnerRings& ObWkbGeogPolygon::inner_rings()
{
  char *ptr = reinterpret_cast<char*>(const_cast<ObWkbGeogPolygon*>(this));
  return *reinterpret_cast<ObWkbGeogPolygonInnerRings*>(ptr);
}

const ObWkbGeogPolygonInnerRings& ObWkbGeogPolygon::inner_rings() const
{
  const char *ptr = reinterpret_cast<const char*>(this);
  return *reinterpret_cast<const ObWkbGeogPolygonInnerRings*>(ptr);
}


// Geograph MultiPoint
uint32_t ObWkbGeogMultiPoint::size() const
{
  char *ptr = reinterpret_cast<char*>(const_cast<ObWkbGeogMultiPoint*>(this));
  ObGeoWkbByteOrder bo = static_cast<ObGeoWkbByteOrder>(*ptr);
  ptr += sizeof(uint8_t) + sizeof(uint32_t); // move [bo][type]
  return ObGeoWkbByteOrderUtil::read<uint32_t>(ptr, bo);
}

ObWkbGeogMultiPoint::size_type ObWkbGeogMultiPoint::length() const
{
  size_type s = WKB_COMMON_WKB_HEADER_LEN;
  ObWkbGeogPoint p;
  s += size() * p.length();
  return s;
}

// iter adaptor
void ObWkbGeogMultiPoint::get_sub_addr(const_pointer last_addr, index_type last_idx, index_type cur_idx,
  ObWkbIterOffsetArray* offsets, pointer& data)
{
  UNUSED(last_addr);
  UNUSED(last_idx);
  UNUSED(offsets);
  char *ptr = reinterpret_cast<char*>(const_cast<ObWkbGeogMultiPoint*>(this));
  uint64_t offset = WKB_COMMON_WKB_HEADER_LEN;
  ObWkbGeogPoint p;
  offset += cur_idx * p.length();
  offset += sizeof(uint8_t) + sizeof(uint32_t); // bo + type
  data = reinterpret_cast<pointer>(ptr + offset);
}


// Geograph MultiLineString
uint32_t ObWkbGeogMultiLineString::size() const
{
  char *ptr = reinterpret_cast<char*>(const_cast<ObWkbGeogMultiLineString*>(this));
  ObGeoWkbByteOrder bo = static_cast<ObGeoWkbByteOrder>(*ptr);
  ptr += sizeof(uint8_t) + sizeof(uint32_t); // move [bo][type]
  return ObGeoWkbByteOrderUtil::read<uint32_t>(ptr, bo);
}

ObWkbGeogMultiLineString::size_type ObWkbGeogMultiLineString::length() const
{
  size_type s = WKB_COMMON_WKB_HEADER_LEN;
  s += ObWkbUtils::sum_sub_size(*this);
  return s;
}

// iter adaptor
void ObWkbGeogMultiLineString::get_sub_addr(const_pointer last_addr, index_type last_idx, index_type cur_idx,
  ObWkbIterOffsetArray* offsets, pointer& data)
{
  ObWkbUtils::get_sub_addr_common(*this, last_addr, last_idx, cur_idx, offsets, data);
}

// Geograph MultiPolygon
uint32_t ObWkbGeogMultiPolygon::size() const
{
  char *ptr = reinterpret_cast<char*>(const_cast<ObWkbGeogMultiPolygon*>(this));
  ObGeoWkbByteOrder bo = static_cast<ObGeoWkbByteOrder>(*ptr);
  ptr += sizeof(uint8_t) + sizeof(uint32_t); // move [bo][type]
  return ObGeoWkbByteOrderUtil::read<uint32_t>(ptr, bo);
}

ObWkbGeogMultiPolygon::size_type ObWkbGeogMultiPolygon::length() const
{
  size_type s = WKB_COMMON_WKB_HEADER_LEN;
  s += ObWkbUtils::sum_sub_size(*this);
  return s;
}

// iter adaptor
void ObWkbGeogMultiPolygon::get_sub_addr(const_pointer last_addr, index_type last_idx, index_type cur_idx,
  ObWkbIterOffsetArray* offsets, pointer& data)
{
  ObWkbUtils::get_sub_addr_common(*this, last_addr, last_idx, cur_idx, offsets, data);
}


// Geograph GeometryCollection
uint32_t ObWkbGeogCollection::size() const
{
  char *ptr = reinterpret_cast<char*>(const_cast<ObWkbGeogCollection*>(this));
  ObGeoWkbByteOrder bo = static_cast<ObGeoWkbByteOrder>(*ptr);
  ptr += sizeof(uint8_t) + sizeof(uint32_t); // move [bo][type]
  return ObGeoWkbByteOrderUtil::read<uint32_t>(ptr, bo);
}

ObWkbGeogCollection::size_type ObWkbGeogCollection::length() const
{
  size_type s = WKB_COMMON_WKB_HEADER_LEN;
  s += ObWkbUtils::sum_sub_size(*this);
  return s;
}

// iter adaptor
void ObWkbGeogCollection::get_sub_addr(const_pointer last_addr, index_type last_idx, index_type cur_idx,
  ObWkbIterOffsetArray* offsets, pointer& data)
{
  ObWkbUtils::get_sub_addr_common(*this, last_addr, last_idx, cur_idx, offsets, data);
}

ObWkbGeogCollection::size_type ObWkbGeogCollection::get_sub_size(const_pointer data) const
{
  size_type s = 0;
  ObGeoWkbByteOrder bo = static_cast<ObGeoWkbByteOrder>(*data);
  pointer ptr = const_cast<pointer>(data) + sizeof(uint8_t);
  ObGeoType type = static_cast<ObGeoType>(ObGeoWkbByteOrderUtil::read<uint32_t>(ptr, bo));
  switch (type) {
    case ObGeoType::POINT: {
      const ObWkbGeogPoint* sub_geo = reinterpret_cast<const ObWkbGeogPoint*>(data);
      s = sub_geo->length();
      break;
    }
    case ObGeoType::LINESTRING: {
      const ObWkbGeogLineString* sub_geo = reinterpret_cast<const ObWkbGeogLineString*>(data);
      s = sub_geo->length();
      break;
    }
    case ObGeoType::POLYGON: {
      const ObWkbGeogPolygon* sub_geo = reinterpret_cast<const ObWkbGeogPolygon*>(data);
      s = sub_geo->length();
      break;
    }
    case ObGeoType::MULTIPOINT: {
      const ObWkbGeogMultiPoint* sub_geo = reinterpret_cast<const ObWkbGeogMultiPoint*>(data);
      s = sub_geo->length();
      break;
    }
    case ObGeoType::MULTILINESTRING: {
      const ObWkbGeogMultiLineString* sub_geo = reinterpret_cast<const ObWkbGeogMultiLineString*>(data);
      s = sub_geo->length();
      break;
    }
    case ObGeoType::MULTIPOLYGON: {
      const ObWkbGeogMultiPolygon* sub_geo = reinterpret_cast<const ObWkbGeogMultiPolygon*>(data);
      s = sub_geo->length();
      break;
    }
    case ObGeoType::GEOMETRYCOLLECTION: {
      const ObWkbGeogCollection* sub_geo = reinterpret_cast<const ObWkbGeogCollection*>(data);
      s = sub_geo->length();
      break;
    }
    default: {
      LOG_WARN_RET(OB_INVALID_ARGUMENT, "Invalid geo type.", K(type));
    }
  }
  return s;
}

ObGeoType ObWkbGeogCollection::get_sub_type(const_pointer data) const
{
  ObGeoWkbByteOrder bo = static_cast<ObGeoWkbByteOrder>(*data);
  pointer ptr = const_cast<pointer>(data) + sizeof(uint8_t);
  ObGeoType type = static_cast<ObGeoType>(ObGeoWkbByteOrderUtil::read<uint32_t>(ptr, bo));
  return type;
}

} // namespace common
} // namespace oceanbase
