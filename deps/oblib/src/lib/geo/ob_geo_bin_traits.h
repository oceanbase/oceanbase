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

#ifndef OCEANBASE_LIB_GEO_OB_GEO_BIN_TRAITS_
#define OCEANBASE_LIB_GEO_OB_GEO_BIN_TRAITS_

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

#include "ob_geo_bin.h"

namespace boost {
namespace geometry {
namespace traits {

/***********************************************************************/

// Cartesian

// Point
template <>
struct tag<oceanbase::common::ObWkbGeomPoint> {
  typedef boost::geometry::point_tag type;
};

template <>
struct coordinate_type<oceanbase::common::ObWkbGeomPoint> {
  typedef double type;
};

template <>
struct coordinate_system<oceanbase::common::ObWkbGeomPoint> {
  typedef boost::geometry::cs::cartesian type;
};

template <>
struct dimension<oceanbase::common::ObWkbGeomPoint> : boost::mpl::int_<2> {};

template <std::size_t Dimension>
struct access<oceanbase::common::ObWkbGeomPoint, Dimension> {
  static inline double get(oceanbase::common::ObWkbGeomPoint const &p) {
    return p.get<Dimension>();
  }

  static inline void set(oceanbase::common::ObWkbGeomPoint &p, double const &value) {
    p.set<Dimension>(value);
  }
};


// InnerPoint
template <>
struct tag<oceanbase::common::ObWkbGeomInnerPoint> {
  typedef boost::geometry::point_tag type;
};

template <>
struct coordinate_type<oceanbase::common::ObWkbGeomInnerPoint> {
  typedef double type;
};

template <>
struct coordinate_system<oceanbase::common::ObWkbGeomInnerPoint> {
  typedef boost::geometry::cs::cartesian type;
};

template <>
struct dimension<oceanbase::common::ObWkbGeomInnerPoint> : boost::mpl::int_<2> {};

template <std::size_t Dimension>
struct access<oceanbase::common::ObWkbGeomInnerPoint, Dimension> {
  static inline double get(oceanbase::common::ObWkbGeomInnerPoint const &p) {
    return p.get<Dimension>();
  }

  static inline void set(oceanbase::common::ObWkbGeomInnerPoint &p, double const &value) {
    p.set<Dimension>(value);
  }
};


// Linestring
template <>
struct tag<oceanbase::common::ObWkbGeomLineString> {
  typedef boost::geometry::linestring_tag type;
};


// Linearring
template <>
struct tag<oceanbase::common::ObWkbGeomLinearRing> {
  typedef boost::geometry::ring_tag type;
};

template <>
struct point_order<oceanbase::common::ObWkbGeomLinearRing> {
  static const order_selector value = counterclockwise;
};

template <>
struct closure<oceanbase::common::ObWkbGeomLinearRing> {
  static const closure_selector value = closed;
};


// Polygon
template <>
struct tag<oceanbase::common::ObWkbGeomPolygon> {
  typedef boost::geometry::polygon_tag type;
};

template <>
struct ring_const_type<oceanbase::common::ObWkbGeomPolygon> {
  typedef oceanbase::common::ObWkbGeomLinearRing const &type;
};

template <>
struct ring_mutable_type<oceanbase::common::ObWkbGeomPolygon> {
  typedef oceanbase::common::ObWkbGeomLinearRing &type;
};

template <>
struct interior_const_type<oceanbase::common::ObWkbGeomPolygon> {
  typedef oceanbase::common::ObWkbGeomPolygonInnerRings const &type;
};

template <>
struct interior_mutable_type<oceanbase::common::ObWkbGeomPolygon> {
  typedef oceanbase::common::ObWkbGeomPolygonInnerRings &type;
};

template <>
struct exterior_ring<oceanbase::common::ObWkbGeomPolygon> {
  static inline oceanbase::common::ObWkbGeomLinearRing &get(oceanbase::common::ObWkbGeomPolygon &py) {
    return py.exterior_ring();
  }

  static inline oceanbase::common::ObWkbGeomLinearRing const &get(
      oceanbase::common::ObWkbGeomPolygon const &py) {
    return py.exterior_ring();
  }
};

template <>
struct interior_rings<oceanbase::common::ObWkbGeomPolygon> {
  static inline oceanbase::common::ObWkbGeomPolygonInnerRings
      &get(oceanbase::common::ObWkbGeomPolygon &py) {
    return py.inner_rings();
  }

  static inline oceanbase::common::ObWkbGeomPolygonInnerRings const &
  get(oceanbase::common::ObWkbGeomPolygon const &py) {
    return py.inner_rings();
  }
};


// Multipoint
template <>
struct tag<oceanbase::common::ObWkbGeomMultiPoint> {
  typedef boost::geometry::multi_point_tag type;
};

// Multilinestring
template <>
struct tag<oceanbase::common::ObWkbGeomMultiLineString> {
  typedef boost::geometry::multi_linestring_tag type;
};

// Multipolygon
template <>
struct tag<oceanbase::common::ObWkbGeomMultiPolygon> {
  typedef boost::geometry::multi_polygon_tag type;
};


/***********************************************************************/
// Geographic

// Point
template <>
struct tag<oceanbase::common::ObWkbGeogPoint> {
  typedef boost::geometry::point_tag type;
};

template <>
struct coordinate_type<oceanbase::common::ObWkbGeogPoint> {
  typedef double type;
};

template <>
struct coordinate_system<oceanbase::common::ObWkbGeogPoint> {
  typedef boost::geometry::cs::geographic<radian> type; // TODO geog point x,y system?
};

template <>
struct dimension<oceanbase::common::ObWkbGeogPoint> : boost::mpl::int_<2> {};

template <std::size_t Dimension>
struct access<oceanbase::common::ObWkbGeogPoint, Dimension> {
  static inline double get(oceanbase::common::ObWkbGeogPoint const &p) {
    return p.get<Dimension>();
  }

  static inline void set(oceanbase::common::ObWkbGeogPoint &p, double const &value) {
    p.set<Dimension>(value);
  }
};


// InnerPoint
template <>
struct tag<oceanbase::common::ObWkbGeogInnerPoint> {
  typedef boost::geometry::point_tag type;
};

template <>
struct coordinate_type<oceanbase::common::ObWkbGeogInnerPoint> {
  typedef double type;
};

template <>
struct coordinate_system<oceanbase::common::ObWkbGeogInnerPoint> {
  typedef boost::geometry::cs::geographic<radian> type; // TODO geog point x,y system?
};

template <>
struct dimension<oceanbase::common::ObWkbGeogInnerPoint> : boost::mpl::int_<2> {};

template <std::size_t Dimension>
struct access<oceanbase::common::ObWkbGeogInnerPoint, Dimension> {
  static inline double get(oceanbase::common::ObWkbGeogInnerPoint const &p) {
    return p.get<Dimension>();
  }

  static inline void set(oceanbase::common::ObWkbGeogInnerPoint &p, double const &value) {
    p.set<Dimension>(value);
  }
};


// Linestring
template <>
struct tag<oceanbase::common::ObWkbGeogLineString> {
  typedef boost::geometry::linestring_tag type;
};


// Linearring
template <>
struct tag<oceanbase::common::ObWkbGeogLinearRing> {
  typedef boost::geometry::ring_tag type;
};

template <>
struct point_order<oceanbase::common::ObWkbGeogLinearRing> {
  static const order_selector value = counterclockwise;
};

template <>
struct closure<oceanbase::common::ObWkbGeogLinearRing> {
  static const closure_selector value = closed;
};


// Polygon
template <>
struct tag<oceanbase::common::ObWkbGeogPolygon> {
  typedef boost::geometry::polygon_tag type;
};

template <>
struct ring_const_type<oceanbase::common::ObWkbGeogPolygon> {
  typedef oceanbase::common::ObWkbGeogLinearRing const &type;
};

template <>
struct ring_mutable_type<oceanbase::common::ObWkbGeogPolygon> {
  typedef oceanbase::common::ObWkbGeogLinearRing &type;
};

template <>
struct interior_const_type<oceanbase::common::ObWkbGeogPolygon> {
  typedef oceanbase::common::ObWkbGeogPolygonInnerRings const &type;
};

template <>
struct interior_mutable_type<oceanbase::common::ObWkbGeogPolygon> {
  typedef oceanbase::common::ObWkbGeogPolygonInnerRings &type;
};

template <>
struct exterior_ring<oceanbase::common::ObWkbGeogPolygon> {
  static inline oceanbase::common::ObWkbGeogLinearRing &get(oceanbase::common::ObWkbGeogPolygon &py) {
    return py.exterior_ring();
  }

  static inline oceanbase::common::ObWkbGeogLinearRing const &get(
      oceanbase::common::ObWkbGeogPolygon const &py) {
    return py.exterior_ring();
  }
};

template <>
struct interior_rings<oceanbase::common::ObWkbGeogPolygon> {
  static inline oceanbase::common::ObWkbGeogPolygonInnerRings
      &get(oceanbase::common::ObWkbGeogPolygon &py) {
    return py.inner_rings();
  }

  static inline oceanbase::common::ObWkbGeogPolygonInnerRings const &
  get(oceanbase::common::ObWkbGeogPolygon const &py) {
    return py.inner_rings();
  }
};


// Multipoint
template <>
struct tag<oceanbase::common::ObWkbGeogMultiPoint> {
  typedef boost::geometry::multi_point_tag type;
};

// Multilinestring
template <>
struct tag<oceanbase::common::ObWkbGeogMultiLineString> {
  typedef boost::geometry::multi_linestring_tag type;
};

// Multipolygon
template <>
struct tag<oceanbase::common::ObWkbGeogMultiPolygon> {
  typedef boost::geometry::multi_polygon_tag type;
};


} // namespace traits
} // namespace geometry
} // namespace boost


#endif // OCEANBASE_LIB_GEO_OB_GEO_BIN_TRAITS_
