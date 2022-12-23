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
 * This file contains implementation support for the geometry utils abstraction.
 */

#ifndef OCEANBASE_LIB_GEO_OB_GEO_UTILS_
#define OCEANBASE_LIB_GEO_OB_GEO_UTILS_
#include "lib/geo/ob_geo.h"
#include "lib/geo/ob_geo_common.h"
#include "lib/geo/ob_geo_bin.h"
#include "lib/geo/ob_geo_ibin.h"
#include "lib/geo/ob_srs_info.h"
#include "lib/geo/ob_s2adapter.h"

namespace oceanbase
{
namespace common
{
class ObGeoTypeUtil
{
public:
  static const uint32_t EWKB_SRID_FLAG = 0x20000000;
  static const uint32_t EWKB_M_FLAG = 0x40000000;
  static const uint32_t EWKB_Z_FLAG = 0x80000000;
  static int create_geo_by_type(ObIAllocator &allocator,
                                ObGeoType geo_type,
                                bool is_geographical,
                                bool is_geo_bin,
                                ObGeometry *&geo,
                                uint32_t srid = 0);
  static int create_geo_by_wkb(ObIAllocator &allocator,
                               const ObString &wkb,
                               const ObSrsItem *srs,
                               ObGeometry *&geo,
                               bool need_check = true,
                               bool need_copy = true);
  static int build_geometry(ObIAllocator &allocator,
                            const ObString &wkb,
                            ObGeometry *&geo,
                            const ObSrsItem *srs,
                            ObGeoErrLogInfo &log_info,
                            const bool need_normlize = true,
                            const bool need_check_ring = false,
                            const bool need_correct = true);
  static int construct_geometry(ObIAllocator &allocator,
                                const ObString &wkb,
                                const ObSrsItem *srs,
                                ObGeometry *&geo,
                                bool has_srid = true);
  static int correct_polygon(ObIAllocator &alloc,
                             const ObSrsItem *srs,
                             bool is_ring_closed,
                             ObGeometry &geo);
  static int check_coordinate_range(const ObSrsItem *srs,
                                    ObGeometry *geo,
                                    ObGeoErrLogInfo &log_info,
                                    const bool is_param = false,
                                    const bool is_normalized = false);
    static int get_buffered_geo(ObArenaAllocator *allocator,
                                const ObString &wkb_str,
                                double distance,
                                const ObSrsItem *srs,
                                ObString &res_wkb);
  static ObGeoType get_geo_type_by_name(ObString &name);
  static const char *get_geo_name_by_type(ObGeoType type);
  static int get_header_info_from_wkb(const ObString &wkb,
                                      ObGeoWkbHeader &header);
  static int get_type_srid_from_wkb(const ObString &wkb,
                                    ObGeoType &type,
                                    uint32_t &srid);
  static int get_srid_from_wkb(const ObString &wkb,
                               uint32_t &srid);
  static int get_type_from_wkb(const ObString &wkb,
                               ObGeoType &type);
  static int get_bo_from_wkb(const ObString &wkb,
                             ObGeoWkbByteOrder &bo);
  static bool is_geo1_dimension_higher_than_geo2(ObGeoType type1,
                                                 ObGeoType type2);
  static int check_geo_type(const ObGeoType column_type,
                            const ObString &wkb_str);
  static int get_pg_reserved_prj4text(ObIAllocator *allocator,
                                      uint32_t srid,
                                      ObString &prj4_param);
  static bool is_pg_reserved_srid(uint32_t srid);
  static int to_wkb(ObIAllocator &allocator,
                    ObGeometry &geo,
                    const ObSrsItem *srs_item,
                    ObString &res_wkb,
                    bool need_convert = true);
  static int tree_to_bin(ObIAllocator &allocator,
                         ObGeometry *geo_tree,
                         ObGeometry *&geo_bin,
                         const ObSrsItem *srs_item,
                         bool need_convert = false);
  static int geo_to_ewkt(const ObString &geo_wkb,
                         ObString &ewkt,
                         ObIAllocator &allocator,
                         int64_t max_decimal_digits);
  static int geo_close_ring(ObGeometry &geo, ObIAllocator &allocator);
  static int get_mbr_polygon(ObIAllocator &allocator,
                             const ObSrsBoundsItem *bounds,
                             const ObGeometry &geo_bin,
                             ObGeometry *&mbr_polygon);
  static int eval_point_box_intersects(const ObSrsItem *srs_item,
                                       const ObGeometry *geo1,
                                       const ObGeometry *geo2,
                                       bool &result);
  static int get_cellid_mbr_from_geom(const ObString &wkb_str,
                                      const ObSrsItem *srs_item,
                                      const ObSrsBoundsItem *srs_bound,
                                      ObS2Cellids &cellids,
                                      ObString &mbr_val);
  static int get_wkb_from_swkb(const ObString &swkb, ObString &wkb, uint32_t &offset);
private:
  template<typename PT, typename LN, typename PY, typename MPT, typename MLN, typename MPY, typename GC>
  static int create_geo_bin_by_type(ObIAllocator &allocator,
                                    ObGeoType geo_type,
                                    ObGeometry *&geo,
                                    uint32_t srid = 0);
  template<typename PT, typename LN, typename PY, typename MPT, typename MLN, typename MPY, typename GC>
  static int create_geo_tree_by_type(ObIAllocator &allocator,
                                     ObGeoType geo_type,
                                     ObGeometry *&geo,
                                     uint32_t srid = 0);
  static int multipoly_close_ring(const ObString &wkb_in,
                                  ObGeoStringBuffer &res,
                                  uint32_t &geo_len);
  static int poly_close_ring(const ObString &wkb_in,
                             ObGeoStringBuffer &res,
                             uint32_t &offset);
  static int collection_close_ring(ObIAllocator &allocator, const ObString &wkb_in,
                                   ObGeoStringBuffer &res, uint32_t &geo_len);
};

typedef struct
{
  double xmin;
  double xmax;
  double ymin;
  double ymax;
  double zmin;
  double zmax;
} ObGeogBox;

typedef struct
{
  double x;
  double y;
  double z;
} ObPoint3d;

typedef struct
{
  double x;
  double y;
} ObPoint2d;

enum class PG_SRID
{
  WORLD_MERCATOR = 999000,
  NORTH_UTM_START = 999001,
  NORTH_UTM_END = 999060,
  NORTH_LAMBERT = 999061,
  NORTH_STEREO = 999062,
  SOUTH_UTM_START = 999101,
  SOUTH_UTM_END = 999160,
  SOUTH_LAMBERT = 999161,
  SOUTH_STEREO = 999162,
  LAEA_START = 999163,
  LAEA_END = 999283,
};

class ObGeoBoxUtil
{
public:
  static int get_geog_point_box(const ObWkbGeogInnerPoint &point, ObGeogBox &box);
  template<typename GeometryType>
  static int get_geog_line_box(const GeometryType &line, ObGeogBox &box);
  static int get_geog_poly_box(const ObWkbGeogPolygon &poly, ObGeogBox &box);
  static int caculate_line_box(ObPoint3d &start, ObPoint3d &end, ObGeogBox &box);
  static void get_box_center(const ObGeogBox &box, ObPoint2d &center);
  static bool is_float_equal(double left, double right);
  static bool is_same_point3d(const ObPoint3d &p3d1, const ObPoint3d &p3d2);
  static bool is_completely_opposite(const ObPoint3d &p1, const ObPoint3d &p2);
  static void point_box_uion(ObPoint3d &point, ObGeogBox &box);
  static void box_uion(ObGeogBox &box_tmp, ObGeogBox &box);
  static void convert_ll_to_cartesian3d(const ObWkbGeogInnerPoint &point, ObPoint3d &p3d);
  static double vector_dot_product(const ObPoint3d &p3d1, const ObPoint3d &p3d2);
  static void vector_cross_product(const ObPoint3d &p3d1, const ObPoint3d &p3d2, ObPoint3d &res);
  static void vector_add(const ObPoint3d &p3d1, const ObPoint3d &p3d2, ObPoint3d &res);
  static void vector_minus(const ObPoint3d &p3d1, const ObPoint3d &p3d2, ObPoint3d &res);
  static void get_unit_normal_vector(const ObPoint3d &p3d1, const ObPoint3d &p3d2, ObPoint3d &res);
  static void vector_3d_normalize(ObPoint3d &p3d);
  static void vector_2d_normalize(ObPoint2d &p2d);
  static int get_point_relative_location(const ObPoint2d &p1, const ObPoint2d &p2, const ObPoint2d &point);
  static void ob_geo_box_check_poles(ObGeogBox &box);
  static double correct_longitude(double longitude);
  static double correct_latitude(double latitude);
  static double caculate_box_angular_width(const ObGeogBox &box);
  static double caculate_box_angular_height(const ObGeogBox &box);
  static void do_set_poles(const double &xmin, const double &xmax,
                       const double &ymin, const double &ymax,
                       double &zmin, double &zmax);

  static constexpr double FP_TOLERANCE = 5e-14;
};

template<typename GeometryType>
int ObGeoBoxUtil::get_geog_line_box(const GeometryType &line, ObGeogBox &box)
{
  int ret = OB_SUCCESS;
  bool start = false;
  ObPoint3d p3d1;
  ObPoint3d p3d2;
  ObGeogBox box_tmp;

  typename GeometryType::iterator iter = line.begin();
  convert_ll_to_cartesian3d(*iter, p3d1);
  iter++;
  for ( ; iter != line.end() && OB_SUCC(ret); iter++) {
    convert_ll_to_cartesian3d(*iter, p3d2);
    if(OB_FAIL(caculate_line_box(p3d1, p3d2, box_tmp))) {
      OB_LOG(WARN, "failed to caculate line box", K(ret));
    } else {
      if (!start) {
        start = true;
        box = box_tmp;
      } else {
        box_uion(box_tmp, box);
      }
      p3d1 = p3d2;
    }
  }
  return ret;
}

template<typename PT, typename LN, typename PY, typename MPT, typename MLN, typename MPY, typename GC>
int ObGeoTypeUtil::create_geo_bin_by_type(ObIAllocator &allocator,
                                          ObGeoType geo_type,
                                          ObGeometry *&geo,
                                          uint32_t srid/* = 0 */)
{
  int ret = OB_SUCCESS;
  ObGeometry *tmp_geo = NULL;

  switch(geo_type) {
    case ObGeoType::POINT: {
      tmp_geo = OB_NEWx(PT, (&allocator), srid, (&allocator));
      break;
    }
    case ObGeoType::LINESTRING: {
      tmp_geo = OB_NEWx(LN, (&allocator), srid, (&allocator));
      break;
    }
    case ObGeoType::POLYGON: {
      tmp_geo = OB_NEWx(PY, (&allocator), srid, (&allocator));
      break;
    }
    case ObGeoType::MULTIPOINT: {
      tmp_geo = OB_NEWx(MPT, (&allocator), srid, (&allocator));
      break;
    }
    case ObGeoType::MULTILINESTRING: {
      tmp_geo = OB_NEWx(MLN, (&allocator), srid, (&allocator));
      break;
    }
    case ObGeoType::MULTIPOLYGON: {
      tmp_geo = OB_NEWx(MPY, (&allocator), srid, (&allocator));
      break;
    }
    case ObGeoType::GEOMETRYCOLLECTION: {
      tmp_geo = OB_NEWx(GC, (&allocator), srid, (&allocator));
      break;
    }
    default: {
      ret = OB_ERR_INVALID_GEOMETRY_TYPE;
      OB_LOG(WARN, "unexpected geo type", K(ret), K(srid), K(geo_type));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(tmp_geo)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(WARN, "fail to create geo bin by type", K(ret), K(srid), K(geo_type));
    } else {
      geo = tmp_geo;
    }
  }

  return ret;
}

template<typename PT, typename LN, typename PY, typename MPT, typename MLN, typename MPY, typename GC>
int ObGeoTypeUtil::create_geo_tree_by_type(ObIAllocator &allocator,
                                           ObGeoType geo_type,
                                           ObGeometry *&geo,
                                           uint32_t srid/* = 0 */)
{
  int ret = OB_SUCCESS;
  ObGeometry *tmp_geo = NULL;

  switch(geo_type) {
    case ObGeoType::POINT: {
      tmp_geo = OB_NEWx(PT, (&allocator), srid, (&allocator));
      break;
    }
    case ObGeoType::LINESTRING: {
      tmp_geo = OB_NEWx(LN, (&allocator), srid, allocator);
      break;
    }
    case ObGeoType::POLYGON: {
      tmp_geo = OB_NEWx(PY, (&allocator), srid, allocator);
      break;
    }
    case ObGeoType::MULTIPOINT: {
      tmp_geo = OB_NEWx(MPT, (&allocator), srid, allocator);
      break;
    }
    case ObGeoType::MULTILINESTRING: {
      tmp_geo = OB_NEWx(MLN, (&allocator), srid, allocator);
      break;
    }
    case ObGeoType::MULTIPOLYGON: {
      tmp_geo = OB_NEWx(MPY, (&allocator), srid, allocator);
      break;
    }
    case ObGeoType::GEOMETRYCOLLECTION: {
      tmp_geo = OB_NEWx(GC, (&allocator), srid, allocator);
      break;
    }
    default: {
      ret = OB_ERR_INVALID_GEOMETRY_TYPE;
      OB_LOG(WARN, "unexpected geo type", K(ret), K(srid), K(geo_type));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(tmp_geo)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(WARN, "fail to create geo tree by type", K(ret), K(srid), K(geo_type));
    } else {
      geo = tmp_geo;
    }
  }

  return ret;
}

} // namespace common
} // namespace oceanbase

#endif
