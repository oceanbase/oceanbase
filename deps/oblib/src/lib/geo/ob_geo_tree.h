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

#ifndef OCEANBASE_LIB_GEO_OB_GEO_TREE_
#define OCEANBASE_LIB_GEO_OB_GEO_TREE_

#include "ob_geo.h"
#include "ob_geo_bin.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_vector.h"
#include "lib/container/ob_array_iterator.h"

namespace oceanbase {
namespace common {

class ObIGeoVisitor;

template <typename T>
class ObGeomConstIterator : public array::Iterator<common::ObArray<T, ModulePageAllocator, true>, T>
{
  typedef array::Iterator<common::ObArray<T, ModulePageAllocator, true>, T> base_t;
  typedef ObGeomConstIterator<T> self_t;
public:
  typedef typename std::random_access_iterator_tag iterator_category;
  typedef int64_t difference_type;
  typedef T value_type;
  typedef T *value_ptr_t;
  typedef T *pointer;
  typedef T &reference;
public:
  ObGeomConstIterator() : base_t() {}
  ObGeomConstIterator(const self_t &other ) : base_t(other) {}
  // ObGeomConstIterator(const base_t &other_base) : base_t(other_base) {}
  self_t &operator = (const self_t &other)
  { // const return?
    base_t::operator = (other);
    return *this;
  }
  explicit ObGeomConstIterator(value_ptr_t value_ptr) : base_t(value_ptr) {}

  // override shift functions
  difference_type operator- (const self_t &rhs)
  {
    return base_t::operator-(rhs);
  };
  self_t operator-(difference_type step)
  {
    base_t tmp = base_t::operator-(step);
    return self_t(tmp);
  };
  self_t operator+(difference_type step)
  {
    base_t tmp = base_t::operator+(step);
    return self_t(tmp);
  };
  self_t &operator+=(difference_type step)
  {
    base_t::operator+=(step);
    return *this;
  };
  self_t &operator-=(difference_type step)
  {
    base_t::operator-=(step);
    return *this;
  };
  self_t &operator ++()
  {
    base_t::operator++();
    return *this;
  };
  self_t operator ++(int)
  {
    self_t tmp = *this;
    base_t::operator++();
    return tmp;
  };
  self_t &operator --()
  {
    base_t::operator--();
    return *this;
  };
  self_t operator --(int)
  {
    self_t tmp = *this;
    base_t::operator--();
    return tmp;
  };

  // override retrieve functions
  reference operator *() const
  {
    return base_t::operator *();
  }

  value_ptr_t operator ->() const
  {
    return base_t::operator->();
  }

  operator value_ptr_t() const
  {
    return base_t::operator value_ptr_t();
  }
};

template <typename T>
ObGeomConstIterator<T> operator+(
  typename ObGeomConstIterator<T>::difference_type diff,
  const ObGeomConstIterator<T>& iter)
{
  ObGeomConstIterator<T> iter2 = iter;
  iter2 += diff;
  return iter2;
}

template <typename T>
class ObGeomVector
{
public:
  typedef T value_type;
  typedef ObGeomConstIterator<T> const_iterator;
  typedef typename ObArray<T, ModulePageAllocator, true>::iterator iterator;
  typedef int64_t size_type;
  typedef const T *const_pointer;
  typedef const T &const_reference;
  typedef T *pointer;
  typedef T &reference;
  typedef int64_t difference_type;

public:
  ObGeomVector(ModulePageAllocator &page_allocator)
    : vec_(OB_MALLOC_NORMAL_BLOCK_SIZE, page_allocator) {}
  ObGeomVector(const ObGeomVector<T> &v) = default;
  ObGeomVector<T> &operator=(const ObGeomVector<T> &rhs) = default;
  ~ObGeomVector() {};

  int push_back(const value_type& elem) { return vec_.push_back(static_cast<const value_type &>(elem)); }
  size_type size() const { return vec_.size(); }
  bool empty() const { return vec_.size() == 0;}
  void pop_front() { vec_.remove(0); }
  void resize(int32_t size) {
    int ret = OB_SUCCESS;
    if (size > vec_.size()) {
      if (OB_FAIL(vec_.prepare_allocate(size))) {
        OB_LOG(WARN, "failed to resize ObGeomVector", K(ret));
      }
    } else {
      while (size != vec_.size()) {
        vec_.pop_back();
      }
    }
  }
  void clear() { vec_.reuse(); }
  value_type &back() { return *(vec_.end()-1); }
  const value_type &back() const { return *(vec_.end()-1); };
  value_type &front() { return *(vec_.begin()); }
  const value_type &front() const { return *(vec_.begin()); }
  value_type &operator[](int64_t i) { return vec_[i]; }
  const value_type &operator[](int64_t i) const { return vec_[i]; }
  // iterator
  iterator begin() { return vec_.begin(); }
  const_iterator begin() const { return const_iterator(&*(const_cast<common::ObArray<T, ModulePageAllocator, true> *>(&vec_))->begin()); }
  iterator end() { return vec_.end(); }
  const_iterator end() const { return const_iterator(&*(const_cast<common::ObArray<T, ModulePageAllocator, true> *>(&vec_))->end()); }
  // ObArray<T, ModulePageAllocator, true>& get_vec_() const { return vec_; }
  int remove(int64_t idx) { return vec_.remove(idx); }

private:
  common::ObArray<T, ModulePageAllocator, true> vec_;
};

// ObPoint is an abstract class
class ObPoint : public ObGeometry
{
public:
  // constructor
  ObPoint(uint32_t srid = 0, ObIAllocator *allocator = NULL) :
    ObGeometry(srid, allocator) {};
  ~ObPoint() {};
  ObPoint(const ObPoint&) = default;
  ObPoint &operator=(const ObPoint&) = default;
  // interface
  ObGeoType type() const override { return ObGeoType::POINT; }
  // visitor interface
  virtual int do_visit(ObIGeoVisitor &visitor) = 0;
  const char* val() const { return NULL; }
  bool is_tree() const override { return true; }
  void set_data(const ObString& wkb) override { UNUSED(wkb); }
  bool is_empty() const override {
    return (std::isnan(x()) || std::isnan(y()));
  }
  virtual double x() const = 0;
  virtual void x(double d) = 0;
  virtual double y() const = 0;
  virtual void y(double d) = 0;
  TO_STRING_KV("type", "ObPoint",
               "x", x(),
               "y", y());
};

class ObCartesianPoint : public ObPoint
{
public:
  // constructor
  ObCartesianPoint(uint32_t srid = 0, ObIAllocator *allocator = NULL) : ObPoint(srid, allocator) {
    point_.set<0>(std::nan(""));
    point_.set<1>(std::nan(""));
  };
  ObCartesianPoint(double x, double y, uint32_t srid = 0, ObIAllocator *allocator = NULL) : ObPoint(srid, allocator) {
    point_.set<0>(x);
    point_.set<1>(y);
  };
  ~ObCartesianPoint() {};
  operator ObWkbGeomInnerPoint() const { return ObWkbGeomInnerPoint(x(), y());}
  // interface
  ObGeoCRS crs() const override { return ObGeoCRS::Cartesian; }
  double x() const override { return point_.get<0>(); }
  void x(double d) override { point_.set<0>(d); }
  double y() const override { return point_.get<1>(); }
  void y(double d) override { point_.set<1>(d); }
  // visitor
  int do_visit(ObIGeoVisitor &visitor);
  // boost geometry adaptor
  template<std::size_t K>
  double get() const;
  template<std::size_t K>
  void set(double d);
  ObWkbGeomInnerPoint data() const { return point_; }
  const char* val() const { return reinterpret_cast<const char *>(&point_); }
  void set_data(const ObWkbGeomInnerPoint &inner_point) { point_ = inner_point; }
private:
  ObWkbGeomInnerPoint point_;
};

class ObGeographPoint : public ObPoint
{
public:
  // constructor
  ObGeographPoint(uint32_t srid = 0, ObIAllocator *allocator = NULL) : ObPoint(srid, allocator) {
    point_.set<0>(std::nan(""));
    point_.set<1>(std::nan(""));
  };
  ObGeographPoint(double x, double y, uint32_t srid = 0, ObIAllocator *allocator = NULL) : ObPoint(srid, allocator) {
    point_.set<0>(x);
    point_.set<1>(y);
  };
  ~ObGeographPoint() {};
  operator ObWkbGeogInnerPoint() const { return ObWkbGeogInnerPoint(x(), y());}
  // interface
  ObGeoCRS crs() const override { return ObGeoCRS::Geographic; }
  double x() const override { return point_.get<0>(); }
  void x(double d) override { point_.set<0>(d); }
  double y() const override { return point_.get<1>(); }
  void y(double d) override { point_.set<1>(d); }
  // visitor
  int do_visit(ObIGeoVisitor &visitor);
  // boost geometry adaptor
  template<std::size_t K>
  double get() const;
  template<std::size_t K>
  void set(double d);
  ObWkbGeogInnerPoint data() const { return point_; }
  void set_data(const ObWkbGeogInnerPoint &inner_point) { point_ = inner_point; }
  const char* val() const { return reinterpret_cast<const char *>(&point_); }

private:
  ObWkbGeogInnerPoint point_;
};

class ObCurve : public ObGeometry
{
public:
  // do nothing
  // constructor
  ObCurve(uint32_t srid = 0, ObIAllocator *allocator = NULL) :
    ObGeometry(srid, allocator){};
  ~ObCurve() {};
};

class ObLineString : public ObCurve
{
public:
  ObLineString(uint32_t srid = 0, ObIAllocator *allocator = NULL)
    : ObCurve(srid, allocator)
    {}

  ~ObLineString() {}
  ObGeoType type() const override { return ObGeoType::LINESTRING; }
  uint32_t dimension() const { return 1; }
  const char* val() const override { return NULL; }
  bool is_tree() const override { return true; }
  void set_data(const ObString& wkb) override { UNUSED(wkb); }
  bool is_empty() const override { return empty(); }

  virtual bool empty() const = 0;
  virtual void pop_front() = 0;
  virtual int64_t size() const = 0;
  virtual void clear() = 0;
  static int create_linestring(ObGeoCRS crs, uint32_t srid,
                               ObIAllocator &allocator, ObLineString*& output);
};

static const int64_t DEFAULT_PAGE_SIZE_GEO = 8192;
class ObCartesianLineString : public ObLineString
{
public:
  // iterator
  typedef ObGeomVector<ObWkbGeomInnerPoint>::value_type value_type;
  typedef ObGeomVector<ObWkbGeomInnerPoint>::iterator iterator;
  typedef ObGeomVector<ObWkbGeomInnerPoint>::const_iterator const_iterator;
public:
  ObCartesianLineString(uint32_t srid, ObIAllocator &allocator)
    : ObLineString(srid, &allocator),
      page_allocator_(allocator, common::ObModIds::OB_MODULE_PAGE_ALLOCATOR),
      points_(page_allocator_) {}

  ObCartesianLineString()
    : ObLineString(0, NULL),
      page_allocator_(),
      points_(page_allocator_) {}

  ObCartesianLineString(const ObCartesianLineString&) = default;
  ObCartesianLineString &operator=(const ObCartesianLineString&) = default;
  ~ObCartesianLineString() {}

  // Geometry interface
  ObGeoCRS crs() const override { return ObGeoCRS::Cartesian; }
  bool empty() const { return points_.empty(); }

  // vector interface
  int push_back(ObWkbGeomInnerPoint& point) { return points_.push_back(point); }
  int push_back(const ObWkbGeomInnerPoint& point) { return points_.push_back(point); }
  int64_t size() const override { return points_.size(); }
  void pop_front() override { points_.pop_front(); }
  void resize(int32_t size) { points_.resize(size); }
  void clear() override { points_.clear(); }
  ObWkbGeomInnerPoint &back() { return points_.back(); }
  const ObWkbGeomInnerPoint &back() const { return points_.back(); }
  ObWkbGeomInnerPoint &front() { return points_.front(); }
  const ObWkbGeomInnerPoint &front() const { return points_.front(); }
  ObWkbGeomInnerPoint &operator[](int32_t i) { return points_[i]; }
  const ObWkbGeomInnerPoint &operator[](int32_t i) const { return points_[i]; }
  // visitor interface
  int do_visit(ObIGeoVisitor &visitor);
  iterator begin() { return points_.begin(); }
  const_iterator begin() const { return points_.begin(); }
  iterator end() { return points_.end(); }
  const_iterator end() const { return points_.end(); }
  const ObGeomVector<ObWkbGeomInnerPoint> &get_points() const {return points_;}
  ObGeomVector<ObWkbGeomInnerPoint> &get_points() {return points_;}
  TO_STRING_KV("type", "ObCartesianLineString",
               "size", size());

private:
  ModulePageAllocator page_allocator_;
  ObGeomVector<ObWkbGeomInnerPoint> points_;
};

class ObGeographLineString : public ObLineString
{
public:
  typedef ObGeomVector<ObWkbGeogInnerPoint>::value_type value_type;
  typedef ObGeomVector<ObWkbGeogInnerPoint>::iterator iterator;
  typedef ObGeomVector<ObWkbGeogInnerPoint>::const_iterator const_iterator;
public:
  ObGeographLineString(uint32_t srid, ObIAllocator &allocator)
    : ObLineString(srid, &allocator),
      page_allocator_(allocator, common::ObModIds::OB_MODULE_PAGE_ALLOCATOR),
      points_(page_allocator_) {}

  ObGeographLineString()
    : ObLineString(0, NULL),
      page_allocator_(),
      points_(page_allocator_) {}

  ObGeographLineString(const ObGeographLineString&) = default;
  ObGeographLineString &operator=(const ObGeographLineString&) = default;
  ~ObGeographLineString() {}

  ObGeoCRS crs() const override { return ObGeoCRS::Geographic; }
  bool empty() const { return points_.empty(); }

  // vector interface
  int push_back(ObWkbGeogInnerPoint& point) { return points_.push_back(point); }
  int push_back(const ObWkbGeogInnerPoint& point) { return points_.push_back(point); }
  int64_t size() const override { return points_.size(); }
  void pop_front() override { points_.pop_front(); }
  void resize(int32_t size) { points_.resize(size); }
  void clear() override { points_.clear(); }
  ObWkbGeogInnerPoint &back() { return points_.back(); }
  const ObWkbGeogInnerPoint &back() const { return points_.back(); }
  ObWkbGeogInnerPoint &front() { return points_.front(); }
  const ObWkbGeogInnerPoint &front() const { return points_.front(); }
  ObWkbGeogInnerPoint &operator[](int32_t i) { return points_[i]; }
  const ObWkbGeogInnerPoint &operator[](int32_t i) const { return points_[i]; }
  // visitor interface
  int do_visit(ObIGeoVisitor &visitor);

  iterator begin() { return points_.begin(); }
  const_iterator begin() const { return points_.begin(); }
  iterator end() { return points_.end(); }
  const_iterator end() const { return points_.end(); }
  const ObGeomVector<ObWkbGeogInnerPoint> &get_points() const {return points_;}
  ObGeomVector<ObWkbGeogInnerPoint> &get_points() {return points_;}
  TO_STRING_KV("type", "ObGeographLineString",
               "size", size());

private:
  ModulePageAllocator page_allocator_;
  ObGeomVector<ObWkbGeogInnerPoint> points_;
};

class ObLinearring : public ObLineString
{
public:
  static int create_linearring(ObGeoCRS crs, uint32_t srid,
                               ObIAllocator &allocator, ObLinearring*& output);
};

class ObCartesianLinearring : public ObCartesianLineString, public ObLinearring
{
public:
  // constructor
  ObCartesianLinearring(uint32_t srid, ObIAllocator &allocator)  :
    ObCartesianLineString(srid, allocator) {}

  ObCartesianLinearring()  :
    ObCartesianLineString() {}

  int do_visit(ObIGeoVisitor &visitor);
  ObGeoCRS crs() const override { return ObGeoCRS::Cartesian; }
  int push_back(ObWkbGeomInnerPoint& point) {
    return ObCartesianLineString::push_back(point);
  }
  int push_back(const ObWkbGeomInnerPoint& point) {
    return ObCartesianLineString::push_back(point);
  }
  int64_t size() const override { return ObCartesianLineString::size(); }
  bool is_empty() const override { return ObCartesianLineString::is_empty(); }
  bool empty() const { return ObCartesianLineString::empty(); }
  void pop_front() override { ObCartesianLineString::pop_front(); }
  void resize(int32_t size) { ObCartesianLineString::resize(size); }
  void clear() override { ObCartesianLineString::clear(); }
  ObWkbGeomInnerPoint &back() { return ObCartesianLineString::back(); }
  const ObWkbGeomInnerPoint &back() const { return ObCartesianLineString::back(); };
  ObWkbGeomInnerPoint &front() { return ObCartesianLineString::front(); }
  const ObWkbGeomInnerPoint &front() const { return ObCartesianLineString::front(); }
  ObWkbGeomInnerPoint &operator[](int32_t i) { return ObCartesianLineString::operator[](i); }
  const ObWkbGeomInnerPoint &operator[](int32_t i) const { return ObCartesianLineString::operator[](i); }
};

class ObGeographLinearring : public ObGeographLineString, public ObLinearring
{
public:
  // constructor
  ObGeographLinearring(uint32_t srid, ObIAllocator &allocator)  :
    ObGeographLineString(srid, allocator) {}

  ObGeographLinearring()  :
    ObGeographLineString() {}
  int do_visit(ObIGeoVisitor &visitor);
  ObGeoCRS crs() const override { return ObGeoCRS::Geographic; }
  int push_back(ObWkbGeogInnerPoint& point) {
    return ObGeographLineString::push_back(point);
  }
  int push_back(const ObWkbGeogInnerPoint& point) {
    return ObGeographLineString::push_back(point);
  }
  int64_t size() const override { return ObGeographLineString::size(); }
  bool is_empty() const override { return ObGeographLineString::is_empty(); }
  bool empty() const { return ObGeographLineString::empty(); }
  void pop_front() override { ObGeographLineString::pop_front(); }
  void resize(int32_t size) { ObGeographLineString::resize(size); }
  void clear() override { ObGeographLineString::clear(); }
  ObWkbGeogInnerPoint &back() { return ObGeographLineString::back(); }
  const ObWkbGeogInnerPoint &back() const { return ObGeographLineString::back(); };
  ObWkbGeogInnerPoint &front() { return ObGeographLineString::front(); }
  const ObWkbGeogInnerPoint &front() const { return ObGeographLineString::front(); }
  ObWkbGeogInnerPoint &operator[](int32_t i) { return ObGeographLineString::operator[](i); }
  const ObWkbGeogInnerPoint &operator[](int32_t i) const { return ObGeographLineString::operator[](i); }
};

class ObSurface : public ObGeometry
{
public:
  // do nothing
  // constructor
  ObSurface(uint32_t srid = 0, ObIAllocator *allocator = NULL) :
    ObGeometry(srid, allocator){};
  ~ObSurface() {};
};

class ObPolygon : public ObSurface
{
public:
  // contructor
  ObPolygon(uint32_t srid = 0, ObIAllocator *allocator = NULL)
    : ObSurface(srid, allocator)
    {}
  ~ObPolygon() {}
	ObGeoType type() const override { return ObGeoType::POLYGON; }
  bool is_empty() const override { return empty(); }
  const char* val() const override { return NULL; }
  bool is_tree() const override { return true; }
  void set_data(const ObString& wkb) override { UNUSED(wkb); }
  uint32_t dimension() const { return 2; }
  static int create_polygon(ObGeoCRS crs, uint32_t srid,
                            ObIAllocator &allocator, ObPolygon*& output);
  virtual bool empty() const = 0;
  // 内外边界总数
  virtual uint64_t size() const = 0;
  // 内边界总数
  virtual uint64_t inner_ring_size() const = 0;
  // 获取外边界
  virtual ObLinearring& exterior_ring() = 0;
  virtual const ObLinearring& exterior_ring() const = 0;
  // 获取第n个内边界
  virtual ObLinearring& inner_ring(uint32_t n) = 0;
  virtual const ObLinearring& inner_ring(uint32_t n) const = 0;
  // 添加Linearring到polygon中
  virtual int push_back(const ObLinearring &lr) = 0;
  // visitor
  // virtual int do_visit(ObIGeoVisitor &visitor) = 0;
};

class ObCartesianPolygon : public ObPolygon
{
public:
  // constructor
  ObCartesianPolygon(uint32_t srid, ObIAllocator &allocator)
    : ObPolygon(srid, &allocator),
      page_allocator_(allocator, common::ObModIds::OB_MODULE_PAGE_ALLOCATOR),
      exterior_(srid, allocator),
      inner_rings_(page_allocator_) {}

  ObCartesianPolygon()
    : ObPolygon(),
      page_allocator_(),
      exterior_(),
      inner_rings_(page_allocator_) {}
  ObCartesianPolygon(const ObCartesianPolygon &v) = default;
  ObCartesianPolygon &operator=(const ObCartesianPolygon &rhs) = default;
  ~ObCartesianPolygon() {};
  ObGeoCRS crs() const override { return ObGeoCRS::Cartesian; }

  // visitor interface
  int do_visit(ObIGeoVisitor &visitor);

  int push_back(const ObLinearring &ring) override;
  bool empty() const override { return (exterior_.empty() && inner_rings_.size() == 0); }
  uint64_t size() const override { return !exterior_.empty() ? inner_ring_size() + 1 : inner_ring_size(); } // 内外边界总数
  uint64_t inner_ring_size() const override { return inner_rings_.size(); }// 内边界总数
  const ObCartesianLinearring& exterior_ring() const override { return exterior_; } // 获取外边界
  ObCartesianLinearring& exterior_ring() override { return exterior_; } // 获取外边界
  const ObCartesianLinearring& inner_ring(uint32_t n) const override { return inner_rings_[n]; } // 获取第n个内边界
  ObCartesianLinearring& inner_ring(uint32_t n) override { return inner_rings_[n]; } // 获取第n个内边界
  // used for Boost
  ObCartesianLinearring &cartesian_exterior_ring() const { return const_cast<ObCartesianLinearring &>(exterior_); }
  ObGeomVector<ObCartesianLinearring> &interior_rings() { return inner_rings_; }
  ObGeomVector<ObCartesianLinearring> const &const_interior_rings() const { return inner_rings_; }
  TO_STRING_KV("type", "ObCartesianPolygon",
               "size", size());

private:
  ModulePageAllocator page_allocator_;
  ObCartesianLinearring exterior_;
  ObGeomVector<ObCartesianLinearring> inner_rings_;
};

class ObGeographPolygon : public ObPolygon
{
public:
  // constructor
  ObGeographPolygon(uint32_t srid, ObIAllocator &allocator)
    : ObPolygon(srid, &allocator),
      page_allocator_(allocator, common::ObModIds::OB_MODULE_PAGE_ALLOCATOR),
      exterior_(srid, allocator),
      inner_rings_(page_allocator_) {}

  ObGeographPolygon()
    : ObPolygon(0, NULL),
      page_allocator_(),
      exterior_(),
      inner_rings_(page_allocator_) {}
  ObGeographPolygon(const ObGeographPolygon &v) = default;
  ObGeographPolygon &operator=(const ObGeographPolygon &rhs) = default;
  ~ObGeographPolygon() {};
  // Geometry adapator
  ObGeoCRS crs() const override { return ObGeoCRS::Geographic; }

  // visitor interface
  int do_visit(ObIGeoVisitor &visitor);

  int push_back(const ObLinearring &ring) override;
  bool empty() const override { return (exterior_.empty() && inner_rings_.size() == 0); }
  uint64_t size() const override { return !exterior_.empty() ? inner_ring_size() + 1 : inner_ring_size(); } // 内外边界总数
  uint64_t inner_ring_size() const override { return inner_rings_.size(); }// 内边界总数
  const ObGeographLinearring& exterior_ring() const override { return exterior_; } // 获取外边界
  ObGeographLinearring& exterior_ring() override { return exterior_; } // 获取外边界
  const ObGeographLinearring& inner_ring(uint32_t n) const override { return inner_rings_[n]; } // 获取第n个内边界
  ObGeographLinearring& inner_ring(uint32_t n) override { return inner_rings_[n]; } // 获取第n个内边界
  // used for Boost
  ObGeographLinearring &geographic_exterior_ring() const { return const_cast<ObGeographLinearring &>(exterior_); }
  ObGeomVector<ObGeographLinearring> &interior_rings() { return inner_rings_; }
  ObGeomVector<ObGeographLinearring> const &const_interior_rings() const { return inner_rings_; }
  TO_STRING_KV("type", "ObGeographPolygon",
               "size", size());

private:
  ModulePageAllocator page_allocator_;
  ObGeographLinearring exterior_;
  ObGeomVector<ObGeographLinearring> inner_rings_;
};

class ObGeometrycollection : public ObGeometry
{
public:
  // constructor
  ObGeometrycollection(uint32_t srid, ObIAllocator &allocator)
    : ObGeometry(srid, &allocator),
      page_allocator_(allocator, common::ObModIds::OB_MODULE_PAGE_ALLOCATOR)
    {}

  ObGeometrycollection()
    : ObGeometry(0, NULL),
      page_allocator_()
    {}
  ~ObGeometrycollection() {};

	ObGeoType type() const override { return ObGeoType::GEOMETRYCOLLECTION; }
  const char* val() const override { return NULL; }
  bool is_tree() const override { return true; }
  void set_data(const ObString& wkb) override { UNUSED(wkb); }
  virtual void pop_front() = 0;
  virtual bool empty() const = 0;
  virtual uint64_t size() const = 0;
  virtual void resize(int32_t count) = 0;
  virtual void clear() = 0;
  virtual int push_back(const ObGeometry &g) = 0;
  static int create_collection(ObGeoCRS crs, uint32_t srid,
                               ObIAllocator &allocator, ObGeometrycollection*& output);
protected:
  ModulePageAllocator page_allocator_;
};

class ObMultipoint : public ObGeometrycollection
{
public:
  ObMultipoint(uint32_t srid, ObIAllocator &allocator) :
    ObGeometrycollection(srid, allocator){};

  ObMultipoint() :
    ObGeometrycollection(){};

  ~ObMultipoint() {};
	ObGeoType type() const override { return ObGeoType::MULTIPOINT; }
  int push_back(const ObGeometry& g) { UNUSED(g); return OB_SUCCESS; }
  static int create_multipoint(ObGeoCRS crs, uint32_t srid,
                               ObIAllocator &allocator, ObMultipoint*& output);
};


class ObCartesianMultipoint : public ObMultipoint
{
public:
  typedef ObGeomVector<ObWkbGeomInnerPoint>::value_type value_type;
  typedef ObGeomVector<ObWkbGeomInnerPoint>::iterator iterator;
  typedef ObGeomVector<ObWkbGeomInnerPoint>::const_iterator const_iterator;

public:
  ObCartesianMultipoint(uint32_t srid, ObIAllocator &allocator)
    : ObMultipoint(srid, allocator),
      points_(page_allocator_) {}

  ObCartesianMultipoint()
    : ObMultipoint(),
      points_(page_allocator_) {}

  ~ObCartesianMultipoint() {}

  ObGeoCRS crs() const override { return ObGeoCRS::Cartesian; }

  int push_back(const ObWkbGeomInnerPoint& point) { return points_.push_back(point); }
  void pop_front() override { points_.pop_front(); }
  bool empty() const override { return points_.empty(); }
  bool is_empty() const override {
    for (uint64_t i = 0; i < size(); i++) {
      if (!(std::isnan(points_[i].get<0>()) || std::isnan(points_[i].get<1>()))) {
        return false;
      }
    }
    return true;
  }
  uint64_t size() const override { return points_.size(); }
  void resize(int32_t size) { points_.resize(size); }
  void clear() override { points_.clear(); }
  ObWkbGeomInnerPoint &front() { return points_.front(); }
  const ObWkbGeomInnerPoint &front() const { return points_.front(); }
  ObWkbGeomInnerPoint &operator[](int32_t i) { return points_[i]; }
  const ObWkbGeomInnerPoint &operator[](int32_t i) const { return points_[i]; }

  // visitor interface
  int do_visit(ObIGeoVisitor &visitor);

  iterator begin() { return points_.begin(); }
  const_iterator begin() const { return points_.begin(); }
  iterator end() { return points_.end(); }
  const_iterator end() const { return points_.end(); }
  TO_STRING_KV("type", "ObCartesianMultipoint",
               "size", size());

private:
  DISALLOW_COPY_AND_ASSIGN(ObCartesianMultipoint);
  ObGeomVector<ObWkbGeomInnerPoint> points_;
};

class ObGeographMultipoint : public ObMultipoint
{
public:
  typedef ObGeomVector<ObWkbGeogInnerPoint>::value_type value_type;
  typedef ObGeomVector<ObWkbGeogInnerPoint>::iterator iterator;
  typedef ObGeomVector<ObWkbGeogInnerPoint>::const_iterator const_iterator;

public:
  ObGeographMultipoint(uint32_t srid, ObIAllocator &allocator)
    : ObMultipoint(srid, allocator),
      points_(page_allocator_) {}

  ObGeographMultipoint()
    : ObMultipoint(),
      points_(page_allocator_) {}
  ~ObGeographMultipoint() {}

  ObGeoCRS crs() const override { return ObGeoCRS::Geographic; }

  int push_back(const ObWkbGeogInnerPoint& point) { return points_.push_back(point); }
  void pop_front() override { points_.pop_front(); }
  bool empty() const override { return points_.empty(); }
  bool is_empty() const override {
    for (uint64_t i = 0; i < size(); i++) {
      if (!(std::isnan(points_[i].get<0>()) || std::isnan(points_[i].get<1>()))) {
        return false;
      }
    }
    return true;
  }
  uint64_t size() const override { return points_.size(); }
  void resize(int32_t size) { points_.resize(size); }
  void clear() override { points_.clear(); }
  ObWkbGeogInnerPoint &front() { return points_.front(); }
  const ObWkbGeogInnerPoint &front() const { return points_.front(); }
  ObWkbGeogInnerPoint &operator[](int32_t i) { return points_[i]; }
  const ObWkbGeogInnerPoint &operator[](int32_t i) const { return points_[i]; }

  // visitor interface
  int do_visit(ObIGeoVisitor &visitor);

  iterator begin() { return points_.begin(); }
  const_iterator begin() const { return points_.begin(); }
  iterator end() { return points_.end(); }
  const_iterator end() const { return points_.end(); }
  TO_STRING_KV("type", "ObGeographMultipoint",
               "size", size());

private:
  DISALLOW_COPY_AND_ASSIGN(ObGeographMultipoint);
  ObGeomVector<ObWkbGeogInnerPoint> points_;
};

class ObMulticurve : public ObGeometrycollection
{
public:
  // do nothing
  // constructor
  ObMulticurve(uint32_t srid, ObIAllocator &allocator) :
    ObGeometrycollection(srid, allocator){};
  ObMulticurve() :
    ObGeometrycollection(){};
  ~ObMulticurve() {};
};

class ObMultilinestring : public ObMulticurve
{
 public:
  ObMultilinestring(uint32_t srid, ObIAllocator &allocator) :
    ObMulticurve(srid, allocator){};

  ObMultilinestring() :
    ObMulticurve(){};

  ~ObMultilinestring() {};
	ObGeoType type() const override { return ObGeoType::MULTILINESTRING; }

  static int create_multilinestring(ObGeoCRS crs, uint32_t srid,
                                    ObIAllocator &allocator, ObMultilinestring*& output);
};

class ObCartesianMultilinestring : public ObMultilinestring
{
public:
  typedef ObGeomVector<ObCartesianLineString>::value_type value_type;
  typedef ObGeomVector<ObCartesianLineString>::iterator iterator;
  typedef ObGeomVector<ObCartesianLineString>::const_iterator const_iterator;

public:
  ObCartesianMultilinestring(uint32_t srid, ObIAllocator &allocator)
    : ObMultilinestring(srid, allocator),
      lines_(page_allocator_) {}

  ObCartesianMultilinestring()
    : ObMultilinestring(),
      lines_(page_allocator_) {}

  ~ObCartesianMultilinestring() {}
  // Geometry interface
  ObGeoCRS crs() const override { return ObGeoCRS::Cartesian; }
  // vector interface
  int push_back(const ObGeometry& line) { return lines_.push_back(static_cast<const ObCartesianLineString &>(line)); }
  void pop_front() override { lines_.pop_front(); }
  bool empty() const override { return lines_.empty(); }
  bool is_empty() const override {
    for (uint64_t i = 0; i < size(); i++) {
      if (!lines_[i].is_empty()) {
        return false;
      }
    }
    return true;
  }
  uint64_t size() const override { return lines_.size(); }
  void resize(int32_t size) { lines_.resize(size); }
  void clear() override { lines_.clear(); }
  ObCartesianLineString &front() { return *(lines_.begin()); }
  const ObCartesianLineString &front() const { return *(lines_.begin()); }
  ObCartesianLineString &operator[](int32_t i) { return lines_[i]; }
  const ObCartesianLineString &operator[](int32_t i) const { return lines_[i]; }

  // visitor interface
  int do_visit(ObIGeoVisitor &visitor);

  iterator begin() { return lines_.begin(); }
  const_iterator begin() const { return lines_.begin(); }
  iterator end() { return lines_.end(); }
  const_iterator end() const { return lines_.end(); }
  int remove(int64_t idx) { return lines_.remove(idx); }
  TO_STRING_KV("type", "ObCartesianMultilinestring",
               "size", size());

private:
  ObGeomVector<ObCartesianLineString> lines_;
  DISALLOW_COPY_AND_ASSIGN(ObCartesianMultilinestring);
};

class ObGeographMultilinestring : public ObMultilinestring
{
public:
  typedef ObGeomVector<ObGeographLineString>::value_type value_type;
  typedef ObGeomVector<ObGeographLineString>::iterator iterator;
  typedef ObGeomVector<ObGeographLineString>::const_iterator const_iterator;

public:
  ObGeographMultilinestring(uint32_t srid, ObIAllocator &allocator)
    : ObMultilinestring(srid, allocator),
      lines_(page_allocator_) {}

  ObGeographMultilinestring()
    : ObMultilinestring(),
      lines_(page_allocator_) {}

  ~ObGeographMultilinestring() {}

  // Geometry interface
  ObGeoCRS crs() const override { return ObGeoCRS::Geographic; }

  // vector interface
  int push_back(const ObGeometry& line) { return lines_.push_back(static_cast<const ObGeographLineString &>(line)); }
  void pop_front() override { lines_.pop_front(); }
  bool empty() const override { return lines_.empty(); }
  bool is_empty() const override {
    for (uint64_t i = 0; i < size(); i++) {
      if (!lines_[i].is_empty()) {
        return false;
      }
    }
    return true;
  }
  uint64_t size() const override { return lines_.size(); }
  void resize(int32_t size) { lines_.resize(size); }
  void clear() override { lines_.clear(); }
  ObGeographLineString &front() { return *(lines_.begin()); }
  const ObGeographLineString &front() const { return *(lines_.begin()); }
  ObGeographLineString &operator[](int32_t i) { return lines_[i]; }
  const ObGeographLineString &operator[](int32_t i) const { return lines_[i]; }

  // visitor interface
  int do_visit(ObIGeoVisitor &visitor);

  iterator begin() { return lines_.begin(); }
  const_iterator begin() const { return lines_.begin(); }
  iterator end() { return lines_.end(); }
  const_iterator end() const { return lines_.end(); }
  TO_STRING_KV("type", "ObGeographMultilinestring",
               "size", size());

private:
  DISALLOW_COPY_AND_ASSIGN(ObGeographMultilinestring);
  ObGeomVector<ObGeographLineString> lines_;
};

class ObMultisurface : public ObGeometrycollection
{
public:
  // do nothing
  // constructor
  ObMultisurface(uint32_t srid, ObIAllocator &allocator) :
    ObGeometrycollection(srid, allocator){};

  ObMultisurface() :
    ObGeometrycollection(){};
  ~ObMultisurface() {};
};

class ObMultipolygon : public ObMultisurface
{
public:
  ObMultipolygon(uint32_t srid, ObIAllocator &allocator) :
    ObMultisurface(srid, allocator){};

  ObMultipolygon() :
    ObMultisurface(){};

  ~ObMultipolygon() {};
	ObGeoType type() const override { return ObGeoType::MULTIPOLYGON; }

  static int create_multipolygon(ObGeoCRS crs, uint32_t srid,
                                 ObIAllocator &allocator, ObMultipolygon*& output);
};

class ObCartesianMultipolygon : public ObMultipolygon
{
public:
  typedef ObGeomVector<ObCartesianPolygon>::value_type value_type;
  typedef ObGeomVector<ObCartesianPolygon>::iterator iterator;
  typedef ObGeomVector<ObCartesianPolygon>::const_iterator const_iterator;

public:
  ObCartesianMultipolygon(uint32_t srid, ObIAllocator &allocator)
    : ObMultipolygon(srid, allocator),
      polygons_(page_allocator_) {}

  ObCartesianMultipolygon()
    : ObMultipolygon(),
      polygons_(page_allocator_) {}

  ~ObCartesianMultipolygon() {}

  // Geometry interface
  ObGeoCRS crs() const override { return ObGeoCRS::Cartesian; }

  // vector interface
  int push_back(const ObGeometry& poly) { return polygons_.push_back(static_cast<const ObCartesianPolygon &>(poly)); }
  void pop_front() override { polygons_.pop_front(); }
  bool empty() const override { return polygons_.empty(); }
  bool is_empty() const override {
    for (uint64_t i = 0; i < size(); i++) {
      if (!polygons_[i].is_empty()) {
        return false;
      }
    }
    return true;
  }
  uint64_t size() const override { return polygons_.size(); }
  void resize(int32_t size) { polygons_.resize(size); }
  void clear() override { polygons_.clear(); }
  ObCartesianPolygon &front() { return *(polygons_.begin()); }
  const ObCartesianPolygon &front() const { return *(polygons_.begin()); }
  ObCartesianPolygon &operator[](int32_t i) { return polygons_[i]; }
  const ObCartesianPolygon &operator[](int32_t i) const { return polygons_[i]; }

  int do_visit(ObIGeoVisitor &visitor);

  iterator begin() { return polygons_.begin(); }
  const_iterator begin() const { return polygons_.begin(); }
  iterator end() { return polygons_.end(); }
  const_iterator end() const { return polygons_.end(); }
  TO_STRING_KV("type", "ObCartesianMultipolygon",
               "size", size());

private:
  DISALLOW_COPY_AND_ASSIGN(ObCartesianMultipolygon);
  ObGeomVector<ObCartesianPolygon> polygons_;
};

class ObGeographMultipolygon : public ObMultipolygon
{
public:
  typedef ObGeomVector<ObGeographPolygon>::value_type value_type;
  typedef ObGeomVector<ObGeographPolygon>::iterator iterator;
  typedef ObGeomVector<ObGeographPolygon>::const_iterator const_iterator;

public:
  ObGeographMultipolygon(uint32_t srid, ObIAllocator &allocator)
    : ObMultipolygon(srid, allocator),
      polygons_(page_allocator_) {}

  ObGeographMultipolygon()
    : ObMultipolygon(),
      polygons_(page_allocator_) {}
  ~ObGeographMultipolygon() {}

  // Geometry interface
  ObGeoCRS crs() const override { return ObGeoCRS::Geographic; }

  // vector interface
  int push_back(const ObGeometry& poly) { return polygons_.push_back(static_cast<const ObGeographPolygon &>(poly)); }
  void pop_front() override { polygons_.pop_front(); }
  bool empty() const override { return polygons_.empty(); }
  bool is_empty() const override {
    for (uint64_t i = 0; i < size(); i++) {
      if (!polygons_[i].is_empty()) {
        return false;
      }
    }
    return true;
  }
  uint64_t size() const override { return polygons_.size(); }
  void resize(int32_t size) { polygons_.resize(size); }
  void clear() override { polygons_.clear(); }
  ObGeographPolygon &front() { return *(polygons_.begin()); }
  const ObGeographPolygon &front() const { return *(polygons_.begin()); }
  ObGeographPolygon &operator[](int32_t i) { return polygons_[i]; }
  const ObGeographPolygon &operator[](int32_t i) const { return polygons_[i]; }

  // visitor interface
  int do_visit(ObIGeoVisitor &visitor);

  iterator begin() { return polygons_.begin(); }
  const_iterator begin() const { return polygons_.begin(); }
  iterator end() { return polygons_.end(); }
  const_iterator end() const { return polygons_.end(); }
  TO_STRING_KV("type", "ObGeographMultipolygon",
               "size", size());

private:
  DISALLOW_COPY_AND_ASSIGN(ObGeographMultipolygon);
  ObGeomVector<ObGeographPolygon> polygons_;
};

class ObGeoTreeUtil
{
public:
  template <typename GeometryT,
            typename CartesianT,
            typename GeographT>
  static int create_geometry(ObGeoCRS crs, uint32_t srid,
                             ObIAllocator &allocator, GeometryT*& output);

  template <typename GeometryT>
  static int create_geometry(uint32_t srid, ObIAllocator &allocator, GeometryT*& output);
};

class ObGeoTreeVisitorImplement
{
public:
  template <typename T_GEO, typename T_ITEM>
  static int line_do_visit(T_GEO *geo, ObIGeoVisitor &visitor);

  template <typename T_GEO>
  static int polygon_do_visit(T_GEO *geo, ObIGeoVisitor &visitor);

  template <typename T_GEO>
  static int collection_do_visit(T_GEO *geo, ObIGeoVisitor &visitor);

};

// box class for bg
class ObCartesianBox
{
public:
  ObCartesianBox() = default;
  ObCartesianBox(ObWkbGeomInnerPoint &min_point, ObWkbGeomInnerPoint &max_point)
      : min_p_(min_point), max_p_(max_point) {}
  ObCartesianBox(double min_x, double min_y, double max_x, double max_y);
  void set_box(double min_x, double min_y, double max_x, double max_y);
  ObGeoCRS coordinate_system() const {
    return ObGeoCRS::Cartesian;
  }

  ObWkbGeomInnerPoint const &min_corner() const { return min_p_; }
  ObWkbGeomInnerPoint &min_corner() { return min_p_; }

  ObWkbGeomInnerPoint const &max_corner() const { return max_p_; }
  ObWkbGeomInnerPoint &max_corner() { return max_p_; }
  bool is_empty()
  { return std::isnan(min_p_.get<0>()) && std::isnan(min_p_.get<1>())
           && std::isnan(max_p_.get<0>()) && std::isnan(max_p_.get<1>()); }
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    J_KV(K(min_p_.get<0>()), K(min_p_.get<1>()), K(max_p_.get<0>()), K(max_p_.get<1>()));
    return pos;
  }
  bool Contains(ObCartesianBox &other);
  bool Intersects(ObCartesianBox &other);
private:
  ObWkbGeomInnerPoint min_p_; // point of minimal x,y
  ObWkbGeomInnerPoint max_p_; // point of maxinum x,y
};

class ObGeographBox
{
public:
  ObGeographBox() = default;
  ObGeographBox(ObWkbGeogInnerPoint &min_point, ObWkbGeogInnerPoint &max_point)
      : min_p_(min_point), max_p_(max_point) {}
  ObGeoCRS coordinate_system() const {
    return ObGeoCRS::Cartesian;
  }

  ObWkbGeogInnerPoint const &min_corner() const { return min_p_; }
  ObWkbGeogInnerPoint &min_corner() { return min_p_; }

  ObWkbGeogInnerPoint const &max_corner() const { return max_p_; }
  ObWkbGeogInnerPoint &max_corner() { return max_p_; }
  bool is_empty()
  { return std::isnan(min_p_.get<0>()) && std::isnan(min_p_.get<1>())
           && std::isnan(max_p_.get<0>()) && std::isnan(max_p_.get<1>()); }
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    J_KV(K(min_p_.get<0>()), K(min_p_.get<1>()), K(max_p_.get<0>()), K(max_p_.get<1>()));
    return pos;
  }
private:
  ObWkbGeogInnerPoint min_p_; // point of minimal x,y
  ObWkbGeogInnerPoint max_p_; // point of maxinum x,y
};

class ObCartesianGeometrycollection : public ObGeometrycollection
{
public:
  typedef PageArena<ObGeometry *, ModulePageAllocator> ObCGeoModuleArena;
  typedef ObVector<ObGeometry *, ObCGeoModuleArena>::iterator iterator;
  typedef ObVector<ObGeometry *, ObCGeoModuleArena>::const_iterator const_iterator;
  typedef ObCartesianMultipolygon sub_mp_type;
  typedef ObCartesianMultipoint sub_mpt_type;
  typedef ObCartesianMultilinestring sub_ml_type;
  typedef ObCartesianPoint sub_pt_type;

public:
  ObCartesianGeometrycollection(uint32_t srid, ObIAllocator &allocator)
    : ObGeometrycollection(srid, allocator),
      mode_arena_(DEFAULT_PAGE_SIZE_GEO, page_allocator_),
      geoms_(&mode_arena_, common::ObModIds::OB_MODULE_PAGE_ALLOCATOR) {}

  ObCartesianGeometrycollection()
    : ObGeometrycollection(),
      mode_arena_(DEFAULT_PAGE_SIZE_GEO, page_allocator_),
      geoms_(&mode_arena_, common::ObModIds::OB_MODULE_PAGE_ALLOCATOR) {}
  ~ObCartesianGeometrycollection() { geoms_.clear(); }
  ObGeoCRS crs() const override { return ObGeoCRS::Cartesian; }
  // visitor interface
  int do_visit(ObIGeoVisitor &visitor);
  void pop_front() override {
    geoms_.remove(geoms_.begin());
  }
  int push_back(const ObGeometry &g) { return geoms_.push_back(&g); }
  bool empty() const override { return geoms_.size() == 0; }
  bool is_empty() const override {
    for (uint64_t i = 0; i < size(); i++) {
      if (!geoms_[i]->is_empty()) {
        return false;
      }
    }
    return true;
  }
  uint64_t size() const override { return geoms_.size(); }
  void resize(int32_t size) override { geoms_.reserve(size); }
  void clear() override { geoms_.clear(); }

  iterator begin() { return geoms_.begin(); }
  const_iterator begin() const { return geoms_.begin(); }

  iterator end() { return geoms_.end(); }
  const_iterator end() const { return geoms_.end(); }

  ObGeometry &front() { return **begin(); }
  const ObGeometry &front() const { return **begin(); }

  ObGeometry &operator[](int32_t i) { return *geoms_[i]; }
  const ObGeometry &operator[](int32_t i) const {
    return *geoms_[i];
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObCartesianGeometrycollection);
  ObCGeoModuleArena mode_arena_;
  ObVector<ObGeometry *, ObCGeoModuleArena> geoms_;
};

class ObGeographGeometrycollection : public ObGeometrycollection
{
public:
  typedef PageArena<ObGeometry *, ModulePageAllocator> ObCGeoModuleArena;
  typedef ObVector<ObGeometry *, ObCGeoModuleArena>::iterator iterator;
  typedef ObVector<ObGeometry *, ObCGeoModuleArena>::const_iterator const_iterator;
  typedef ObGeographMultipolygon sub_mp_type;
  typedef ObGeographMultipoint sub_mpt_type;
  typedef ObGeographMultilinestring sub_ml_type;
  typedef ObGeographPoint sub_pt_type;

public:
  ObGeographGeometrycollection(uint32_t srid, ObIAllocator &allocator)
    : ObGeometrycollection(srid, allocator),
      mode_arena_(DEFAULT_PAGE_SIZE_GEO, page_allocator_),
      geoms_(&mode_arena_, common::ObModIds::OB_MODULE_PAGE_ALLOCATOR) {}

  ObGeographGeometrycollection()
    : ObGeometrycollection(),
      mode_arena_(DEFAULT_PAGE_SIZE_GEO, page_allocator_),
      geoms_(&mode_arena_, common::ObModIds::OB_MODULE_PAGE_ALLOCATOR) {}
  ~ObGeographGeometrycollection() { geoms_.clear(); }
  ObGeoCRS crs() const override { return ObGeoCRS::Geographic; }
  // visitor interface
  int do_visit(ObIGeoVisitor &visitor);
  void pop_front() override {
    geoms_.remove(geoms_.begin());
  }
  int push_back(const ObGeometry &g) { return geoms_.push_back(&g); }
  bool empty() const override { return geoms_.size() == 0; }
  bool is_empty() const override {
    for (uint64_t i = 0; i < size(); i++) {
      if (!geoms_[i]->is_empty()) {
        return false;
      }
    }
    return true;
  }
  uint64_t size() const override { return geoms_.size(); }
  void resize(int32_t size) override { geoms_.reserve(size); }
  void clear() override { geoms_.clear(); }

  iterator begin() { return geoms_.begin(); }
  const_iterator begin() const { return geoms_.begin(); }

  iterator end() { return geoms_.end(); }
  const_iterator end() const { return geoms_.end(); }

  ObGeometry &front() { return **begin(); }
  const ObGeometry &front() const { return **begin(); }

  ObGeometry &operator[](int32_t i) { return *geoms_[i]; }
  const ObGeometry &operator[](int32_t i) const {
    return *geoms_[i];
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObGeographGeometrycollection);
  ObCGeoModuleArena mode_arena_;
  ObVector<ObGeometry *, ObCGeoModuleArena> geoms_;
};

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_LIB_OB_GEO_TREE_
