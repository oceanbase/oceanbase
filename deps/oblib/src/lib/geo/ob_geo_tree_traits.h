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


#ifndef OCEANBASE_LIB_GEO_OB_GEO_TREE_TRAITS_
#define OCEANBASE_LIB_GEO_OB_GEO_TREE_TRAITS_

#include <boost/geometry/core/access.hpp>
#include <boost/geometry/core/closure.hpp>
#include <boost/geometry/core/coordinate_dimension.hpp>
#include <boost/geometry/core/coordinate_system.hpp>
#include <boost/geometry/core/coordinate_type.hpp>
#include <boost/geometry/core/cs.hpp>
#include <boost/geometry/core/exterior_ring.hpp>
#include <boost/geometry/core/interior_rings.hpp>
#include <boost/geometry/core/interior_type.hpp>
#include <boost/geometry/core/point_order.hpp>
#include <boost/geometry/core/ring_type.hpp>
#include <boost/geometry/core/tags.hpp>
#include <boost/geometry/geometries/concepts/linestring_concept.hpp>
#include <boost/geometry/geometries/concepts/point_concept.hpp>
#include <boost/geometry/geometries/concepts/polygon_concept.hpp>
#include <boost/geometry/multi/core/tags.hpp>

#include "ob_geo_tree.h"

namespace boost {
namespace geometry {
namespace traits {

////////////////////////////////////////////////////////////////////////////////

// Cartesian

// Point

template <>
struct tag<oceanbase::common::ObCartesianPoint> {
  typedef boost::geometry::point_tag type;
};

template <>
struct coordinate_type<oceanbase::common::ObCartesianPoint> {
  typedef double type;
};

template <>
struct coordinate_system<oceanbase::common::ObCartesianPoint> {
  typedef boost::geometry::cs::cartesian type;
};

template <>
struct dimension<oceanbase::common::ObCartesianPoint> : boost::mpl::int_<2> {};

template <std::size_t Dimension>
struct access<oceanbase::common::ObCartesianPoint, Dimension> {
  static inline double get(oceanbase::common::ObCartesianPoint const &p) {
    return p.get<Dimension>();
  }

  static inline void set(oceanbase::common::ObCartesianPoint &p, double const &value) {
    p.set<Dimension>(value);
  }
};

// Linestring

template <>
struct tag<oceanbase::common::ObCartesianLineString> {
  typedef boost::geometry::linestring_tag type;
};

// Linearring

template <>
struct tag<oceanbase::common::ObCartesianLinearring> {
  typedef boost::geometry::ring_tag type;
};

template <>
struct point_order<oceanbase::common::ObCartesianLinearring> {
  static const order_selector value = counterclockwise;
};

template <>
struct closure<oceanbase::common::ObCartesianLinearring> {
  static const closure_selector value = closed;
};

// Polygon

template <>
struct tag<oceanbase::common::ObCartesianPolygon> {
  typedef boost::geometry::polygon_tag type;
};

template <>
struct ring_const_type<oceanbase::common::ObCartesianPolygon> {
  typedef oceanbase::common::ObCartesianLinearring const &type;
};

template <>
struct ring_mutable_type<oceanbase::common::ObCartesianPolygon> {
  typedef oceanbase::common::ObCartesianLinearring &type;
};

template <>
struct interior_const_type<oceanbase::common::ObCartesianPolygon> {
  typedef oceanbase::common::ObGeomVector<oceanbase::common::ObCartesianLinearring> const &type;
};

template <>
struct interior_mutable_type<oceanbase::common::ObCartesianPolygon> {
  typedef oceanbase::common::ObGeomVector<oceanbase::common::ObCartesianLinearring> &type;
};

template <>
struct exterior_ring<oceanbase::common::ObCartesianPolygon> {
  static inline oceanbase::common::ObCartesianLinearring &get(oceanbase::common::ObCartesianPolygon &py) {
    return py.cartesian_exterior_ring();
  }

  static inline oceanbase::common::ObCartesianLinearring const &get(
      oceanbase::common::ObCartesianPolygon const &py) {
    return py.cartesian_exterior_ring();
  }
};

template <>
struct interior_rings<oceanbase::common::ObCartesianPolygon> {
  static inline oceanbase::common::ObGeomVector<oceanbase::common::ObCartesianLinearring>
  &get(oceanbase::common::ObCartesianPolygon &py) {
    return py.interior_rings();
  }

  static inline oceanbase::common::ObGeomVector<oceanbase::common::ObCartesianLinearring> const &
  get(oceanbase::common::ObCartesianPolygon const &py) {
    return py.const_interior_rings();
  }
};

// Multipoint

template <>
struct tag<oceanbase::common::ObCartesianMultipoint> {
  typedef boost::geometry::multi_point_tag type;
};

// Multilinestring

template <>
struct tag<oceanbase::common::ObCartesianMultilinestring> {
  typedef boost::geometry::multi_linestring_tag type;
};

// Multipolygon

template <>
struct tag<oceanbase::common::ObCartesianMultipolygon> {
  typedef boost::geometry::multi_polygon_tag type;
};

// Box

template <>
struct tag<oceanbase::common::ObCartesianBox> {
  typedef box_tag type;
};

template <>
struct point_type<oceanbase::common::ObCartesianBox> {
  typedef oceanbase::common::ObWkbGeomInnerPoint type;
};

template <std::size_t Dimension>
struct indexed_access<oceanbase::common::ObCartesianBox, min_corner, Dimension> {
  static inline double get(oceanbase::common::ObCartesianBox const &b) {
    return b.min_corner().get<Dimension>();
  }

  static inline void set(oceanbase::common::ObCartesianBox &b, double const &value) {
    b.min_corner().set<Dimension>(value);
  }
};

template <std::size_t Dimension>
struct indexed_access<oceanbase::common::ObCartesianBox, max_corner, Dimension> {
  static inline double get(oceanbase::common::ObCartesianBox const &b) {
    return b.max_corner().get<Dimension>();
  }

  static inline void set(oceanbase::common::ObCartesianBox &b, double const &value) {
    b.max_corner().set<Dimension>(value);
  }
};

////////////////////////////////////////////////////////////////////////////////

// Geographic

// Point

template <>
struct tag<oceanbase::common::ObGeographPoint> {
  typedef boost::geometry::point_tag type;
};

template <>
struct coordinate_type<oceanbase::common::ObGeographPoint> {
  typedef double type;
};

template <>
struct coordinate_system<oceanbase::common::ObGeographPoint> {
  typedef boost::geometry::cs::geographic<radian> type;
};

template <>
struct dimension<oceanbase::common::ObGeographPoint> : boost::mpl::int_<2> {};

template <std::size_t Dimension>
struct access<oceanbase::common::ObGeographPoint, Dimension> {
  static inline double get(oceanbase::common::ObGeographPoint const &p) {
    return p.get<Dimension>();
  }

  static inline void set(oceanbase::common::ObGeographPoint &p, double const &value) {
    p.set<Dimension>(value);
  }
};

// Linestring

template <>
struct tag<oceanbase::common::ObGeographLineString> {
  typedef boost::geometry::linestring_tag type;
};

// Linearring

template <>
struct tag<oceanbase::common::ObGeographLinearring> {
  typedef boost::geometry::ring_tag type;
};

template <>
struct point_order<oceanbase::common::ObGeographLinearring> {
  static const order_selector value = counterclockwise;
};

template <>
struct closure<oceanbase::common::ObGeographLinearring> {
  static const closure_selector value = closed;
};

// Polygon

template <>
struct tag<oceanbase::common::ObGeographPolygon> {
  typedef boost::geometry::polygon_tag type;
};

template <>
struct ring_const_type<oceanbase::common::ObGeographPolygon> {
  typedef oceanbase::common::ObGeographLinearring const &type;
};

template <>
struct ring_mutable_type<oceanbase::common::ObGeographPolygon> {
  typedef oceanbase::common::ObGeographLinearring &type;
};

template <>
struct interior_const_type<oceanbase::common::ObGeographPolygon> {
  typedef oceanbase::common::ObGeomVector<oceanbase::common::ObGeographLinearring> const &type;
};

template <>
struct interior_mutable_type<oceanbase::common::ObGeographPolygon> {
  typedef oceanbase::common::ObGeomVector<oceanbase::common::ObGeographLinearring> &type;
};

template <>
struct exterior_ring<oceanbase::common::ObGeographPolygon> {
  static inline oceanbase::common::ObGeographLinearring &get(oceanbase::common::ObGeographPolygon &py) {
    return py.geographic_exterior_ring();
  }

  static inline oceanbase::common::ObGeographLinearring const &get(
      oceanbase::common::ObGeographPolygon const &py) {
    return py.geographic_exterior_ring();
  }
};

template <>
struct interior_rings<oceanbase::common::ObGeographPolygon> {
  static inline oceanbase::common::ObGeomVector<oceanbase::common::ObGeographLinearring>
      &get(oceanbase::common::ObGeographPolygon &py) {
    return py.interior_rings();
  }

  static inline oceanbase::common::ObGeomVector<oceanbase::common::ObGeographLinearring> const&
      get(oceanbase::common::ObGeographPolygon const &py) {
    return py.const_interior_rings();
  }
};

// Multipoint

template <>
struct tag<oceanbase::common::ObGeographMultipoint> {
  typedef boost::geometry::multi_point_tag type;
};

// Multilinestring

template <>
struct tag<oceanbase::common::ObGeographMultilinestring> {
  typedef boost::geometry::multi_linestring_tag type;
};

// Multipolygon

template <>
struct tag<oceanbase::common::ObGeographMultipolygon> {
  typedef boost::geometry::multi_polygon_tag type;
};

// Box

template <>
struct tag<oceanbase::common::ObGeographBox> {
  typedef box_tag type;
};

template <>
struct point_type<oceanbase::common::ObGeographBox> {
  typedef oceanbase::common::ObWkbGeogInnerPoint type;
};

template <std::size_t Dimension>
struct indexed_access<oceanbase::common::ObGeographBox, min_corner, Dimension> {
  static inline double get(oceanbase::common::ObGeographBox const &b) {
    return b.min_corner().get<Dimension>();
  }

  static inline void set(oceanbase::common::ObGeographBox &b, double const &value) {
    b.min_corner().set<Dimension>(value);
  }
};

template <std::size_t Dimension>
struct indexed_access<oceanbase::common::ObGeographBox, max_corner, Dimension> {
  static inline double get(oceanbase::common::ObGeographBox const &b) {
    return b.max_corner().get<Dimension>();
  }

  static inline void set(oceanbase::common::ObGeographBox &b, double const &value) {
    b.max_corner().set<Dimension>(value);
  }
};

}  // namespace traits
}  // namespace geometry
}  // namespace boost

#endif  // OCEANBASE_LIB_GEO_OB_GEO_TREE_TRAITS_
