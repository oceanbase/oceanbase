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

#ifndef OCEANBASE_LIB_GEO_OB_GEO_IBIN_
#define OCEANBASE_LIB_GEO_OB_GEO_IBIN_

#include "ob_geo_bin.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_array.h"

namespace oceanbase {
namespace common {

class ObIGeoVisitor;

// interface
class ObIWkbGeometry : public ObGeometry
{
public:
  ObIWkbGeometry(uint32_t srid = 0, ObIAllocator *allocator = NULL)
    : ObGeometry(srid, allocator) {}
  ~ObIWkbGeometry() = default;
  ObIWkbGeometry(const ObIWkbGeometry& g) = default;
  ObIWkbGeometry& operator=(const ObIWkbGeometry& g) = default;
  // Geo interface
  bool is_tree() const override { return false; }
  void set_data(const ObString& data) override { data_ = data; }
  const char* val() const override { return data_.ptr(); }
  char* val() { return data_.ptr(); }
  // empty interface
  bool is_empty() const override;
  // size interface
  uint64_t length() const override;
  // visitor
  virtual int do_visit(ObIGeoVisitor &visitor) = 0;
protected:
  virtual bool is_empty_inner() const = 0;
  virtual uint64_t length_inner() const = 0;
protected:
  ObString data_; // wkb without srid
};


class ObIWkbPoint : public ObIWkbGeometry
{
public:
  ObIWkbPoint(uint32_t srid = 0, ObIAllocator *allocator = NULL)
    : ObIWkbGeometry(srid, allocator) {}
  ~ObIWkbPoint() = default;
  // point interface
  virtual double x() const = 0;
  virtual double y() const = 0;
  virtual void x(double d) = 0;
  virtual void y(double d) = 0;
  // interface
  ObGeoType type() const override { return ObGeoType::POINT; }
protected:
  bool is_empty_inner() const override { return (std::isnan(x()) || std::isnan(y())); }
};


/*****************Cartesian******************/
/********************************************/
/********************************************/
/********************************************/
/********************************************/
/********************************************/
class ObIWkbGeomPoint : public ObIWkbPoint
{
public:
  typedef ObWkbGeomPoint value_type;
public:
  // constructor
  ObIWkbGeomPoint(uint32_t srid = 0, ObIAllocator *allocator = NULL)
    : ObIWkbPoint(srid, allocator) {}
  ~ObIWkbGeomPoint() = default;
  ObIWkbGeomPoint(const ObIWkbGeomPoint& g) = default;
  ObIWkbGeomPoint& operator=(const ObIWkbGeomPoint& g) = default;
  // interface
  ObGeoCRS crs() const override { return ObGeoCRS::Cartesian; }
  double x() const override;
  double y() const override;
  int do_visit(ObIGeoVisitor &visitor);
  void x(double d) override;
  void y(double d) override;
private:
  uint64_t length_inner() const override { return reinterpret_cast<const ObWkbGeomPoint*>(val())->length(); }
};

class ObIWkbGeomLineString : public ObIWkbGeometry {
public:
  typedef ObWkbGeomLineString value_type;
public:
  // constructor
  ObIWkbGeomLineString(uint32_t srid = 0, ObIAllocator *allocator = NULL)
    : ObIWkbGeometry(srid, allocator) {}
  ~ObIWkbGeomLineString() = default;
  ObIWkbGeomLineString(const ObIWkbGeomLineString& g) = default;
  ObIWkbGeomLineString& operator=(const ObIWkbGeomLineString& g) = default;
  // interface
  ObGeoType type() const override { return ObGeoType::LINESTRING; }
  ObGeoCRS crs() const override { return ObGeoCRS::Cartesian; }
  int do_visit(ObIGeoVisitor &visitor);
  uint32_t size() const
  { return reinterpret_cast<const ObWkbGeomLineString*>(val())->size(); }
private:
  uint64_t length_inner() const override
  { return reinterpret_cast<const ObWkbGeomLineString*>(val())->length(); }
  bool is_empty_inner() const override
  { return (reinterpret_cast<const ObWkbGeomLineString*>(val())->size() == 0); }
};

class ObIWkbGeomPolygon : public ObIWkbGeometry {
public:
  typedef ObWkbGeomPolygon value_type;
public:
  // constructor
  ObIWkbGeomPolygon(uint32_t srid = 0, ObIAllocator *allocator = NULL)
    : ObIWkbGeometry(srid, allocator) {}
  ~ObIWkbGeomPolygon() = default;
  ObIWkbGeomPolygon(const ObIWkbGeomPolygon& g) = default;
  ObIWkbGeomPolygon& operator=(const ObIWkbGeomPolygon& g) = default;
  // interface
  ObGeoType type() const override { return ObGeoType::POLYGON; }
  ObGeoCRS crs() const override { return ObGeoCRS::Cartesian; }
  int do_visit(ObIGeoVisitor &visitor);
  uint32_t size() const
  { return reinterpret_cast<const ObWkbGeomPolygon*>(val())->size(); }
private:
  uint64_t length_inner() const override
  { return reinterpret_cast<const ObWkbGeomPolygon*>(val())->length(); }
  bool is_empty_inner() const override;
};

class ObIWkbGeomMultiPoint : public ObIWkbGeometry {
public:
  // constructor
  ObIWkbGeomMultiPoint(uint32_t srid = 0, ObIAllocator *allocator = NULL)
    : ObIWkbGeometry(srid, allocator) {}
  ~ObIWkbGeomMultiPoint() = default;
  ObIWkbGeomMultiPoint(const ObIWkbGeomMultiPoint& g) = default;
  ObIWkbGeomMultiPoint& operator=(const ObIWkbGeomMultiPoint& g) = default;
  // interface
  ObGeoType type() const override { return ObGeoType::MULTIPOINT; }
  ObGeoCRS crs() const override { return ObGeoCRS::Cartesian; }
  uint64_t length_inner() const override
  { return reinterpret_cast<const ObWkbGeomMultiPoint*>(val())->length(); }
  bool is_empty_inner() const override
  { return (reinterpret_cast<const ObWkbGeomMultiPoint*>(val())->size() == 0); }
  int do_visit(ObIGeoVisitor &visitor);
  uint32_t size() const
  { return reinterpret_cast<const ObWkbGeomMultiPoint*>(val())->size(); }
};

class ObIWkbGeomMultiLineString : public ObIWkbGeometry {
public:
  // constructor
  ObIWkbGeomMultiLineString(uint32_t srid = 0, ObIAllocator *allocator = NULL)
    : ObIWkbGeometry(srid, allocator) {}
  ~ObIWkbGeomMultiLineString() = default;
  ObIWkbGeomMultiLineString(const ObIWkbGeomMultiLineString& g) = default;
  ObIWkbGeomMultiLineString& operator=(const ObIWkbGeomMultiLineString& g) = default;
  // interface
  ObGeoType type() const override { return ObGeoType::MULTILINESTRING; }
  ObGeoCRS crs() const override { return ObGeoCRS::Cartesian; }
  int do_visit(ObIGeoVisitor &visitor);
  uint32_t size() const
  { return reinterpret_cast<const ObWkbGeomMultiLineString*>(val())->size(); }
private:
  uint64_t length_inner() const override
  { return reinterpret_cast<const ObWkbGeomMultiLineString*>(val())->length(); }
  bool is_empty_inner() const override
  { return (reinterpret_cast<const ObWkbGeomMultiLineString*>(val())->size() == 0); }
};

class ObIWkbGeomLinearRing : public ObIWkbGeometry {
public:
  typedef ObWkbGeomLinearRing value_type;

public:
  // constructor
  ObIWkbGeomLinearRing(uint32_t srid = 0, ObIAllocator *allocator = NULL)
    : ObIWkbGeometry(srid, allocator) {}
  ~ObIWkbGeomLinearRing() = default;
  ObIWkbGeomLinearRing(const ObIWkbGeomLinearRing& g) = default;
  ObIWkbGeomLinearRing& operator=(const ObIWkbGeomLinearRing& g) = default;
  // interface
  ObGeoType type() const override { return ObGeoType::LINESTRING; }
  ObGeoCRS crs() const override { return ObGeoCRS::Cartesian; }
  int do_visit(ObIGeoVisitor &visitor);
  uint32_t size() const
  { return reinterpret_cast<const ObWkbGeomLinearRing*>(val())->size(); }
private:
  uint64_t length_inner() const override
  { return reinterpret_cast<const ObWkbGeomLinearRing*>(val())->length(); }
  bool is_empty_inner() const override
  { return (reinterpret_cast<const ObWkbGeomLinearRing*>(val())->size() == 0); }
};

class ObIWkbGeomMultiPolygon : public ObIWkbGeometry {
public:
  // constructor
  ObIWkbGeomMultiPolygon(uint32_t srid = 0, ObIAllocator *allocator = NULL)
    : ObIWkbGeometry(srid, allocator) {}
  ~ObIWkbGeomMultiPolygon() = default;
  ObIWkbGeomMultiPolygon(const ObIWkbGeomMultiPolygon& g) = default;
  ObIWkbGeomMultiPolygon& operator=(const ObIWkbGeomMultiPolygon& g) = default;
  // interface
  ObGeoType type() const override { return ObGeoType::MULTIPOLYGON; }
  ObGeoCRS crs() const override { return ObGeoCRS::Cartesian; }
  int do_visit(ObIGeoVisitor &visitor);
  uint32_t size() const
  { return reinterpret_cast<const ObWkbGeomMultiPolygon*>(val())->size(); }
private:
  uint64_t length_inner() const override
  { return reinterpret_cast<const ObWkbGeomMultiPolygon*>(val())->length(); }
  bool is_empty_inner() const override
  { return (reinterpret_cast<const ObWkbGeomMultiPolygon*>(val())->size() == 0); }
};


// use in functor
// process geometrycollection
class ObIWkbGeomCollection : public ObIWkbGeometry {
public:
  // constructor
  ObIWkbGeomCollection(uint32_t srid = 0, ObIAllocator *allocator = NULL)
    : ObIWkbGeometry(srid, allocator) {}
  ~ObIWkbGeomCollection() = default;
  ObIWkbGeomCollection(const ObIWkbGeomCollection& g) = default;
  ObIWkbGeomCollection& operator=(const ObIWkbGeomCollection& g) = default;
  // interface
  ObGeoType type() const override { return ObGeoType::GEOMETRYCOLLECTION; }
  ObGeoCRS crs() const override { return ObGeoCRS::Cartesian; }
  // iter interface
  int get_sub(uint32_t idx, ObGeometry*& geo) const;
  int do_visit(ObIGeoVisitor &visitor);
  uint32_t size() const
  { return reinterpret_cast<const ObWkbGeomCollection*>(val())->size(); }
private:
  uint64_t length_inner() const override
  { return reinterpret_cast<const ObWkbGeomCollection*>(val())->length(); }
  bool is_empty_inner() const override
  { return (reinterpret_cast<const ObWkbGeomCollection*>(val())->size() == 0); }
};


/*****************Geographic******************/
/********************************************/
/********************************************/
/********************************************/
/********************************************/
/********************************************/
class ObIWkbGeogPoint : public ObIWkbPoint {
public:
  typedef ObWkbGeogPoint value_type;
public:
  // constructor
  ObIWkbGeogPoint(uint32_t srid = 0, ObIAllocator *allocator = NULL)
    : ObIWkbPoint(srid, allocator) {}
  ~ObIWkbGeogPoint() = default;
  ObIWkbGeogPoint(const ObIWkbGeogPoint& g) = default;
  ObIWkbGeogPoint& operator=(const ObIWkbGeogPoint& g) = default;
  // interface
  ObGeoCRS crs() const override { return ObGeoCRS::Geographic; }
  double x() const override;
  double y() const override;
  int do_visit(ObIGeoVisitor &visitor);
  void x(double d) override;
  void y(double d) override;
private:
  uint64_t length_inner() const override { return reinterpret_cast<const ObWkbGeogPoint*>(val())->length(); }
};

class ObIWkbGeogLineString : public ObIWkbGeometry {
public:
  typedef ObWkbGeogLineString value_type;
public:
  // constructor
  ObIWkbGeogLineString(uint32_t srid = 0, ObIAllocator *allocator = NULL)
    : ObIWkbGeometry(srid, allocator) {}
  ~ObIWkbGeogLineString() = default;
  ObIWkbGeogLineString(const ObIWkbGeogLineString& g) = default;
  ObIWkbGeogLineString& operator=(const ObIWkbGeogLineString& g) = default;
  // interface
  ObGeoType type() const override { return ObGeoType::LINESTRING; }
  ObGeoCRS crs() const override { return ObGeoCRS::Geographic; }
  int do_visit(ObIGeoVisitor &visitor);
  uint32_t size() const
  { return reinterpret_cast<const ObWkbGeogLineString*>(val())->size(); }
private:
  uint64_t length_inner() const override
  { return reinterpret_cast<const ObWkbGeogLineString*>(val())->length(); }
  bool is_empty_inner() const override
  { return (reinterpret_cast<const ObWkbGeogLineString*>(val())->size() == 0); }

};

class ObIWkbGeogLinearRing : public ObIWkbGeometry {
public:
  typedef ObWkbGeogLinearRing value_type;

public:
  // constructor
  ObIWkbGeogLinearRing(uint32_t srid = 0, ObIAllocator *allocator = NULL)
    : ObIWkbGeometry(srid, allocator) {}
  ~ObIWkbGeogLinearRing() = default;
  ObIWkbGeogLinearRing(const ObIWkbGeogLinearRing& g) = default;
  ObIWkbGeogLinearRing& operator=(const ObIWkbGeogLinearRing& g) = default;
  // interface
  ObGeoType type() const override { return ObGeoType::LINESTRING; }
  ObGeoCRS crs() const override { return ObGeoCRS::Geographic; }
  int do_visit(ObIGeoVisitor &visitor);
  uint32_t size() const
  { return reinterpret_cast<const ObWkbGeogLinearRing*>(val())->size(); }
private:
  uint64_t length_inner() const override
  { return reinterpret_cast<const ObWkbGeogLinearRing*>(val())->length(); }
  bool is_empty_inner() const override
  { return (reinterpret_cast<const ObWkbGeogLinearRing*>(val())->size() == 0); }
};

class ObIWkbGeogPolygon : public ObIWkbGeometry {
public:
  typedef ObWkbGeogPolygon value_type;
public:
  // constructor
  ObIWkbGeogPolygon(uint32_t srid = 0, ObIAllocator *allocator = NULL)
    : ObIWkbGeometry(srid, allocator) {}
  ~ObIWkbGeogPolygon() = default;
  ObIWkbGeogPolygon(const ObIWkbGeogPolygon& g) = default;
  ObIWkbGeogPolygon& operator=(const ObIWkbGeogPolygon& g) = default;
  // interface
  int do_visit(ObIGeoVisitor &visitor);
  ObGeoType type() const override { return ObGeoType::POLYGON; }
  ObGeoCRS crs() const override { return ObGeoCRS::Geographic; }
  uint32_t size() const
  { return reinterpret_cast<const ObWkbGeogPolygon*>(val())->size(); }
private:
  uint64_t length_inner() const override
  { return reinterpret_cast<const ObWkbGeogPolygon*>(val())->length(); }
  bool is_empty_inner() const override;
};

class ObIWkbGeogMultiPoint : public ObIWkbGeometry {
public:
  // constructor
  ObIWkbGeogMultiPoint(uint32_t srid = 0, ObIAllocator *allocator = NULL)
    : ObIWkbGeometry(srid, allocator) {}
  ~ObIWkbGeogMultiPoint() = default;
  ObIWkbGeogMultiPoint(const ObIWkbGeogMultiPoint& g) = default;
  ObIWkbGeogMultiPoint& operator=(const ObIWkbGeogMultiPoint& g) = default;
  // interface
  ObGeoType type() const override { return ObGeoType::MULTIPOINT; }
  ObGeoCRS crs() const override { return ObGeoCRS::Geographic; }
  int do_visit(ObIGeoVisitor &visitor);
  uint32_t size() const
  { return reinterpret_cast<const ObWkbGeogMultiPoint*>(val())->size(); }
private:
  uint64_t length_inner() const override
  { return reinterpret_cast<const ObWkbGeogMultiPoint*>(val())->length(); }
  bool is_empty_inner() const override
  { return (reinterpret_cast<const ObWkbGeogMultiPoint*>(val())->size() == 0); }
};

class ObIWkbGeogMultiLineString : public ObIWkbGeometry {
public:
  // constructor
  ObIWkbGeogMultiLineString(uint32_t srid = 0, ObIAllocator *allocator = NULL)
    : ObIWkbGeometry(srid, allocator) {}
  ~ObIWkbGeogMultiLineString() = default;
  ObIWkbGeogMultiLineString(const ObIWkbGeogMultiLineString& g) = default;
  ObIWkbGeogMultiLineString& operator=(const ObIWkbGeogMultiLineString& g) = default;
  // interface
  ObGeoType type() const override { return ObGeoType::MULTILINESTRING; }
  ObGeoCRS crs() const override { return ObGeoCRS::Geographic; }
  int do_visit(ObIGeoVisitor &visitor);
  uint32_t size() const
  { return reinterpret_cast<const ObWkbGeogMultiLineString*>(val())->size(); }
private:
  uint64_t length_inner() const override
  { return reinterpret_cast<const ObWkbGeogMultiLineString*>(val())->length(); }
  bool is_empty_inner() const override
  { return (reinterpret_cast<const ObWkbGeogMultiLineString*>(val())->size() == 0); }
};

class ObIWkbGeogMultiPolygon : public ObIWkbGeometry {
public:
  // constructor
  ObIWkbGeogMultiPolygon(uint32_t srid = 0, ObIAllocator *allocator = NULL)
    : ObIWkbGeometry(srid, allocator) {}
  ~ObIWkbGeogMultiPolygon() = default;
  ObIWkbGeogMultiPolygon(const ObIWkbGeogMultiPolygon& g) = default;
  ObIWkbGeogMultiPolygon& operator=(const ObIWkbGeogMultiPolygon& g) = default;
  // interface
  ObGeoType type() const override { return ObGeoType::MULTIPOLYGON; }
  ObGeoCRS crs() const override { return ObGeoCRS::Geographic; }
  int do_visit(ObIGeoVisitor &visitor);
  uint32_t size() const
  { return reinterpret_cast<const ObWkbGeogMultiPolygon*>(val())->size(); }
private:
  uint64_t length_inner() const override
  { return reinterpret_cast<const ObWkbGeogMultiPolygon*>(val())->length(); }
  bool is_empty_inner() const override
  { return (reinterpret_cast<const ObWkbGeogMultiPolygon*>(val())->size() == 0); }
};

class ObIWkbGeogCollection : public ObIWkbGeometry {
public:
  // constructor
  ObIWkbGeogCollection(uint32_t srid = 0, ObIAllocator *allocator = NULL)
    : ObIWkbGeometry(srid, allocator) {}
  ~ObIWkbGeogCollection() = default;
  ObIWkbGeogCollection(const ObIWkbGeogCollection& g) = default;
  ObIWkbGeogCollection& operator=(const ObIWkbGeogCollection& g) = default;
  // interface
  ObGeoType type() const override { return ObGeoType::GEOMETRYCOLLECTION; }
  ObGeoCRS crs() const override { return ObGeoCRS::Geographic; }
  // iter interface
  int get_sub(uint32_t idx, ObGeometry*& geo) const;
  int do_visit(ObIGeoVisitor &visitor);
  uint32_t size() const
  { return reinterpret_cast<const ObWkbGeogCollection*>(val())->size(); }
private:
  uint64_t length_inner() const override
  { return reinterpret_cast<const ObWkbGeogCollection*>(val())->length(); }
  bool is_empty_inner() const override
  { return (reinterpret_cast<const ObWkbGeogCollection*>(val())->size() == 0); }
};

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_LIB_GEO_OB_GEO_IBIN_
