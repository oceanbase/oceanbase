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
 * This file contains implementation support for the Geometry tree abstraction.
 */

#define USING_LOG_PREFIX LIB
#include "ob_geo_tree.h"
#include "ob_geo_visitor.h"


namespace oceanbase {
namespace common {

template<>
double ObCartesianPoint::get<0>() const
{
  return x();
}

template<>
double ObCartesianPoint::get<1>() const
{
  return y();
}

template<>
void ObCartesianPoint::set<0>(double d)
{
  return x(d);
}

template<>
void ObCartesianPoint::set<1>(double d)
{
  return y(d);
}

template<>
double ObGeographPoint::get<0>() const
{
  return x();
}

template<>
double ObGeographPoint::get<1>() const
{
  return y();
}

template<>
void ObGeographPoint::set<0>(double d)
{
  return x(d);
}

template<>
void ObGeographPoint::set<1>(double d)
{
  return y(d);
}

int ObCartesianPolygon::push_back(const ObLinearring &ring)
{
  INIT_SUCC(ret);
  if (exterior_.empty() && inner_rings_.size() == 0) {
    exterior_ = static_cast<const ObCartesianLinearring &>(ring);
  } else {
    if(OB_FAIL(inner_rings_.push_back(static_cast<const ObCartesianLinearring &>(ring)))) {
      LOG_WARN("fail to push_back to ObCartesianPolygon rings.", K(ret));
    }
  }
  return ret;
}

int ObGeographPolygon::push_back(const ObLinearring &ring)
{
  INIT_SUCC(ret);
  if (exterior_.empty() && inner_rings_.size() == 0) {
    exterior_ = static_cast<const ObGeographLinearring &>(ring);
  } else {
    if(OB_FAIL(inner_rings_.push_back(static_cast<const ObGeographLinearring &>(ring)))) {
      LOG_WARN("fail to push_back to ObGeographPolygon rings.", K(ret));
    }
  }
  return ret;
}

template <typename GeometryT,
          typename CartesianT,
          typename GeographT>
int ObGeoTreeUtil::create_geometry(ObGeoCRS crs, uint32_t srid,
                                   ObIAllocator &allocator, GeometryT*& output)
{
  INIT_SUCC(ret);
  if (crs == ObGeoCRS::Cartesian) {
    void *buf = allocator.alloc(sizeof(CartesianT));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory for ObGeometry", K(ret), K(sizeof(CartesianT)));
    } else {
      CartesianT *node= new (buf) CartesianT(srid, allocator);
      output = node;
    }
  } else {
    void *buf = allocator.alloc(sizeof(GeographT));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory for ObGeometry", K(ret), K(sizeof(GeographT)));
    } else {
      GeographT *node= new (buf) GeographT(srid, allocator);
      output = node;
    }
  }
  return ret;
}

template <typename GeometryT>
int ObGeoTreeUtil::create_geometry(uint32_t srid,
                                   ObIAllocator &allocator, GeometryT*& output)
{
  INIT_SUCC(ret);
  void *buf = allocator.alloc(sizeof(GeometryT));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for ObGeometry", K(ret), K(sizeof(GeometryT)));
  } else {
    GeometryT *node= new (buf) GeometryT(srid, allocator);
    output = node;
  }
  return ret;
}

template int ObGeoTreeUtil::create_geometry<ObGeographLineString>(uint32_t srid, ObIAllocator &allocator, ObGeographLineString*& output);
template int ObGeoTreeUtil::create_geometry<ObGeographPolygon>(uint32_t srid, ObIAllocator &allocator, ObGeographPolygon*& output);
template int ObGeoTreeUtil::create_geometry<ObGeographMultipoint>(uint32_t srid, ObIAllocator &allocator, ObGeographMultipoint*& output);
template int ObGeoTreeUtil::create_geometry<ObGeographMultilinestring>(uint32_t srid, ObIAllocator &allocator, ObGeographMultilinestring*& output);
template int ObGeoTreeUtil::create_geometry<ObGeographMultipolygon>(uint32_t srid, ObIAllocator &allocator, ObGeographMultipolygon*& output);
template int ObGeoTreeUtil::create_geometry<ObGeographGeometrycollection>(uint32_t srid, ObIAllocator &allocator, ObGeographGeometrycollection*& output);

int ObLineString::create_linestring(ObGeoCRS crs, uint32_t srid,
                                    ObIAllocator &allocator, ObLineString*& output)
{
  return ObGeoTreeUtil::create_geometry<ObLineString,
                                        ObCartesianLineString,
                                        ObGeographLineString>(crs, srid, allocator, output);
}

int ObLinearring::create_linearring(ObGeoCRS crs, uint32_t srid,
                                    ObIAllocator &allocator, ObLinearring*& output)
{
  return ObGeoTreeUtil::create_geometry<ObLinearring,
                                        ObCartesianLinearring,
                                        ObGeographLinearring>(crs, srid, allocator, output);
}

int ObPolygon::create_polygon(ObGeoCRS crs, uint32_t srid,
                              ObIAllocator &allocator, ObPolygon*& output)
{
  return ObGeoTreeUtil::create_geometry<ObPolygon,
                                        ObCartesianPolygon,
                                        ObGeographPolygon>(crs, srid, allocator, output);
}

int ObGeometrycollection::create_collection(ObGeoCRS crs, uint32_t srid,
                                            ObIAllocator &allocator, ObGeometrycollection*& output)
{
  return ObGeoTreeUtil::create_geometry<ObGeometrycollection,
                                        ObCartesianGeometrycollection,
                                        ObGeographGeometrycollection>(crs, srid, allocator, output);
}

int ObMultipoint::create_multipoint(ObGeoCRS crs, uint32_t srid,
                                    ObIAllocator &allocator, ObMultipoint*& output)
{
  return ObGeoTreeUtil::create_geometry<ObMultipoint,
                                        ObCartesianMultipoint,
                                        ObGeographMultipoint>(crs, srid, allocator, output);
}

int ObMultilinestring::create_multilinestring(ObGeoCRS crs, uint32_t srid,
                                              ObIAllocator &allocator, ObMultilinestring*& output)
{
  return ObGeoTreeUtil::create_geometry<ObMultilinestring,
                                        ObCartesianMultilinestring,
                                        ObGeographMultilinestring>(crs, srid, allocator, output);
}

int ObMultipolygon::create_multipolygon(ObGeoCRS crs, uint32_t srid,
                                        ObIAllocator &allocator, ObMultipolygon*& output)
{
  return ObGeoTreeUtil::create_geometry<ObMultipolygon,
                                        ObCartesianMultipolygon,
                                        ObGeographMultipolygon>(crs, srid, allocator, output);
}

int ObCartesianPoint::do_visit(ObIGeoVisitor &visitor)
{
  return visitor.visit(this);
}

int ObGeographPoint::do_visit(ObIGeoVisitor &visitor)
{
  return visitor.visit(this);
}

template <typename T_GEO, typename T_ITEM>
int ObGeoTreeVisitorImplement::line_do_visit(T_GEO *geo, ObIGeoVisitor &visitor)
{
  int ret = OB_SUCCESS;
  if (visitor.prepare(geo)) {
    if (OB_FAIL(visitor.visit(geo))) {
      LOG_WARN("failed to do tree geog line string visit", K(ret));
    } else if (visitor.is_end(geo) || geo->empty()) {
      // do nothing
    } else {
      for (int32_t i = 0; i < geo->size() && OB_SUCC(ret) && !visitor.is_end(geo); i++) {
        T_ITEM item((*geo)[i].template get<0>(), (*geo)[i].template get<1>());
        if (OB_FAIL(item.do_visit(visitor))) {
          LOG_WARN("failed to do tree geog line string visit", K(ret));
        } else {
          (*geo)[i].template set<0>(item.x());
          (*geo)[i].template set<1>(item.y());
        }
      }
    }
  }
  return ret;
}

template <typename T_GEO>
int ObGeoTreeVisitorImplement::polygon_do_visit(T_GEO *geo, ObIGeoVisitor &visitor)
{
  int ret = OB_SUCCESS;
  if (visitor.prepare(geo)) {
    if (OB_FAIL(visitor.visit(geo))) {
      LOG_WARN("failed to do tree polygon visit", K(ret));
    } else if (visitor.is_end(geo) || geo->empty()) {
      // do nothing
    } else {
      if (OB_FAIL(geo->exterior_ring().do_visit(visitor))) {
        LOG_WARN("failed to do tree polygon exterior ring visit", K(ret));
      }
      for (uint64_t i = 0; i < geo->inner_ring_size() && OB_SUCC(ret) && !visitor.is_end(geo); i++) {
        if (OB_FAIL(geo->inner_ring(i).do_visit(visitor))) {
          LOG_WARN("failed to do tree polygon inner ring visit", K(ret));
        }
      }
    }
  }
  return ret;
}

template <typename T_GEO>
int ObGeoTreeVisitorImplement::collection_do_visit(T_GEO *geo, ObIGeoVisitor &visitor)
{
  int ret = OB_SUCCESS;
  if (visitor.prepare(geo)) {
    if (OB_FAIL(visitor.visit(geo))) {
      LOG_WARN("failed to do tree collection geo visit", K(ret));
    } else if (visitor.is_end(geo) || geo->empty()) {
      // do nothing
    } else {
      for (int32_t i = 0; i < geo->size() && OB_SUCC(ret) && !visitor.is_end(geo); i++) {
        if (OB_FAIL((*geo)[i].do_visit(visitor))) {
          LOG_WARN("failed to do tree item visit", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObGeographLineString::do_visit(ObIGeoVisitor &visitor)
{
  return ObGeoTreeVisitorImplement::line_do_visit<ObGeographLineString,
    ObGeographPoint>(this, visitor);
}

int ObCartesianLineString::do_visit(ObIGeoVisitor &visitor)
{
  return ObGeoTreeVisitorImplement::line_do_visit<ObCartesianLineString,
    ObCartesianPoint>(this, visitor);
}

int ObGeographLinearring::do_visit(ObIGeoVisitor &visitor)
{
  return ObGeoTreeVisitorImplement::line_do_visit<ObGeographLinearring,
    ObGeographPoint>(this, visitor);
}

int ObCartesianLinearring::do_visit(ObIGeoVisitor &visitor)
{
  return ObGeoTreeVisitorImplement::line_do_visit<ObCartesianLinearring,
    ObCartesianPoint>(this, visitor);
}

int ObGeographPolygon::do_visit(ObIGeoVisitor &visitor)
{
  return ObGeoTreeVisitorImplement::polygon_do_visit(this, visitor);
}

int ObCartesianPolygon::do_visit(ObIGeoVisitor &visitor)
{
  return ObGeoTreeVisitorImplement::polygon_do_visit(this, visitor);
}

int ObGeographMultipoint::do_visit(ObIGeoVisitor &visitor)
{
  return ObGeoTreeVisitorImplement::line_do_visit<ObGeographMultipoint,
    ObGeographPoint>(this, visitor);
}

int ObCartesianMultipoint::do_visit(ObIGeoVisitor &visitor)
{
  return ObGeoTreeVisitorImplement::line_do_visit<ObCartesianMultipoint,
    ObCartesianPoint>(this, visitor);
}

int ObGeographMultilinestring::do_visit(ObIGeoVisitor &visitor)
{
  return ObGeoTreeVisitorImplement::collection_do_visit(this, visitor);
}

int ObCartesianMultilinestring::do_visit(ObIGeoVisitor &visitor)
{
  return ObGeoTreeVisitorImplement::collection_do_visit(this, visitor);
}

int ObGeographMultipolygon::do_visit(ObIGeoVisitor &visitor)
{
  return ObGeoTreeVisitorImplement::collection_do_visit(this, visitor);
}

int ObCartesianMultipolygon::do_visit(ObIGeoVisitor &visitor)
{
  return ObGeoTreeVisitorImplement::collection_do_visit(this, visitor);
}

int ObGeographGeometrycollection::do_visit(ObIGeoVisitor &visitor)
{
  return ObGeoTreeVisitorImplement::collection_do_visit(this, visitor);
}

int ObCartesianGeometrycollection::do_visit(ObIGeoVisitor &visitor)
{
  return ObGeoTreeVisitorImplement::collection_do_visit(this, visitor);
}

ObCartesianBox::ObCartesianBox(double min_x, double min_y, double max_x, double max_y)
{
  min_p_.set<0>(min_x);
  min_p_.set<1>(min_y);
  max_p_.set<0>(max_x);
  max_p_.set<1>(max_y);
}

void ObCartesianBox::set_box(double min_x, double min_y, double max_x, double max_y)
{
  min_p_.set<0>(min_x);
  min_p_.set<1>(min_y);
  max_p_.set<0>(max_x);
  max_p_.set<1>(max_y);
}

bool ObCartesianBox::Contains(ObCartesianBox &other)
{
  double this_min_x = min_corner().get<0>();
  double this_min_y = min_corner().get<1>();
  double this_max_x = max_corner().get<0>();
  double this_max_y = max_corner().get<1>();
  double other_min_x = other.min_corner().get<0>();
  double other_min_y = other.min_corner().get<1>();
  double other_max_x = other.max_corner().get<0>();
  double other_max_y = other.max_corner().get<1>();
  return this_min_x <= other_min_x && other_min_x <= this_max_x &&
         this_min_x <= other_max_x && other_max_x <= this_max_x &&
         this_min_y <= other_min_y && other_min_y <= this_max_y &&
         this_min_y <= other_max_y && other_max_y <= this_max_y;
}

bool ObCartesianBox::Intersects(ObCartesianBox &other)
{
  bool ret = true;
  if (min_corner().get<1>() > other.max_corner().get<1>() ||
      other.min_corner().get<1>() > max_corner().get<1>() ||
      min_corner().get<0>() > other.max_corner().get<0>() ||
      other.min_corner().get<0>() > max_corner().get<0>()) {
    ret = false;
  }
  return ret;
}

} // namespace common
} // namespace oceanbase