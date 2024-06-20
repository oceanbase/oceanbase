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
#include "lib/geo/ob_wkb_to_sdo_geo_visitor.h"
#include "lib/geo/ob_wkt_parser.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/number/ob_number_v2.h"
#include "common/object/ob_object.h"
#include "lib/geo/ob_geo_to_tree_visitor.h"
#include "objit/common/ob_item_type.h"

namespace oceanbase
{
namespace common
{
typedef common::hash::ObHashMap<common::ObString, common::ObObj*> QualifiedMap;
enum class NumberObjType {
  DOUBLE,
  UINT64
};

enum ObGeoBuildFlag: uint8_t {
  GEO_ALL_DISABLE = 0x00,
  GEO_NORMALIZE = 0x01,
  GEO_CHECK_RING = 0x02,
  GEO_CORRECT = 0x04,
  GEO_ALLOW_3D = 0x08,
  GEO_CHECK_RANGE = 0x10,
  GEO_RESERVE_3D = 0x20, // do not convert 3D Geometry to 2D
  GEO_DEFAULT = GEO_NORMALIZE | GEO_CORRECT | GEO_CHECK_RANGE,
  GEO_ALLOW_3D_DEFAULT = GEO_DEFAULT | GEO_ALLOW_3D,
  GEO_CARTESIAN = GEO_CORRECT,
  GEO_ALLOW_3D_CARTESIAN = GEO_CARTESIAN | GEO_ALLOW_3D
};

enum QuadDirection {
  NORTH_WEST = 0,
  NORTH_EAST = 1,
  SOUTH_WEST = 2,
  SOUTH_EAST =3,
  INVALID_QUAD = 4,
};

enum ObGeoDimension {
  ZERO_DIMENSION = 0,
  ONE_DIMENSION = 1,
  TWO_DIMENSION = 2,
  MAX_DIMENSION,
};

typedef struct
{
  double x;
  double y;
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    J_KV(K(x), K(y));
    return pos;
  }
} ObPoint2d;

// line with 2 points
typedef struct
{
  ObPoint2d begin;
  ObPoint2d end;
  int get_box(ObCartesianBox &box);
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    J_KV(K(begin), K(end));
    return pos;
  }
} ObSegment;

typedef PageArena<ObPoint2d, ModulePageAllocator> ObCachePointModuleArena;
typedef PageArena<ObSegment, ModulePageAllocator> ObCacheSegModuleArena;
typedef ObVector<ObPoint2d, ObCachePointModuleArena> ObVertexes;
typedef ObVector<ObSegment, ObCacheSegModuleArena> ObSegments;

// line with points in same quad_direction
typedef struct
{
  ObVertexes *verts;
  uint32_t begin;
  uint32_t end;
  int get_box(ObCartesianBox &box);
} ObLineSegment;

typedef PageArena<ObLineSegment, ModulePageAllocator> ObCacheSegmentModuleArena;
class ObCachedGeom;
class ObGeoEvalCtx;
class ObLineSegments{
public:
  ObLineSegments() {}
  ObLineSegments(ModulePageAllocator& page_allocator, ObCachePointModuleArena& point_mode_arena) :
  segs_arena_(DEFAULT_PAGE_SIZE_GEO, page_allocator),
  verts_(&point_mode_arena, common::ObModIds::OB_MODULE_PAGE_ALLOCATOR),
  segs_(&segs_arena_, common::ObModIds::OB_MODULE_PAGE_ALLOCATOR) {}
  ~ObLineSegments() {}
  ObCacheSegmentModuleArena segs_arena_;
  ObVertexes verts_;
  ObVector<ObLineSegment, ObCacheSegmentModuleArena> segs_;
};

class ObGeoTypeUtil
{
public:
  static const uint32_t EWKB_SRID_FLAG = 0x20000000;
  static const uint32_t EWKB_M_FLAG = 0x40000000;
  static const uint32_t EWKB_Z_FLAG = 0x80000000;
  static const uint32_t WKB_3D_TYPE_OFFSET = 1000;
  static const uint32_t RECHECK_ZOOM_IN_VALUE = 10;
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
                               bool need_copy = true,
                               bool allow_3d = false);
  static int build_geometry(ObIAllocator &allocator,
                            const ObString &wkb,
                            ObGeometry *&geo,
                            const ObSrsItem *srs,
                            ObGeoErrLogInfo &log_info,
                            uint8_t build_flag = ObGeoBuildFlag::GEO_DEFAULT);
  static int construct_geometry(ObIAllocator &allocator,
                                const ObString &wkb,
                                const ObSrsItem *srs,
                                ObGeometry *&geo,
                                bool has_srid = true);
  static int copy_geometry(ObIAllocator &allocator,
                          ObGeometry &origin_geo,
                          ObGeometry *&copy_geo);
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
  static int get_gtype_by_num(uint64_t num, ObGeoType &geo_type);
  static const char *get_geo_name_by_type(ObGeoType type);
  static const char *get_geo_name_by_type_oracle(ObGeoType type);
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
  static int wkb_to_sdo_geo(const ObString &swkb, ObSdoGeoObject &geo, bool with_srid = false);
  static int get_num_by_gtype(ObGeoType geo_type, uint64_t &num);
  static int geo_type_in_collection(uint64_t etype, uint64_t interpretation, ObGeoType &type);
  static int wkt_to_sdo_geo(const ObString &wkt, ObSdoGeoObject &sdo_geo);
  static int sql_geo_obj_to_ewkt(const QualifiedMap &map, common::ObIAllocator &allocator, common::ObString &ewkt);
  static int number_to_double(const number::ObNumber &num, double &res);
  static bool is_3d_geo_type(ObGeoType geo_type);
  static bool is_2d_geo_type(ObGeoType geo_type);
  static bool is_multi_geo_type(ObGeoType geo_type);
  static int rectangle_to_swkb(double xmin, double ymin, double xmax, double ymax, ObGeoSrid srid, bool with_version, ObWkbBuffer &wkb_buf);
  static int check_empty(ObGeometry *geo, bool &is_empty);
  static int get_st_geo_name_by_type(ObGeoType type, ObString &res);
  static int get_coll_dimension(ObIWkbGeomCollection *geo, int8_t &dimension);
  static int convert_geometry_3D_to_2D(const ObSrsItem *srs, ObIAllocator &allocator, ObGeometry *g3d,
                                      uint8_t build_flag, ObGeometry *&geo);
  static int normalize_geometry(ObGeometry &geo, const ObSrsItem *srs);
  static double round_double(double x, int32_t dec, bool truncate);
  static double distance_point_squre(const ObWkbGeomInnerPoint& p1, const ObWkbGeomInnerPoint& p2);
  static int add_geo_version(ObIAllocator &allocator, const ObString &src, ObString &res_wkb);
  // only check if polygon is a line or it's valid points are lesser than 4.
  template<typename PyTree, typename MpyTree, typename CollTree>
  static int is_polygon_valid_simple(const ObGeometry *geo, bool &res);
  // caculate end point quadrant direction relative to start point
  static int get_quadrant_direction(const ObPoint2d &start, const ObPoint2d &end, QuadDirection &res);
  static int get_polygon_size(ObGeometry &geo);
  static int polygon_check_self_intersections(ObIAllocator &allocator, ObGeometry &geo, const ObSrsItem *srs, bool& invalid_for_cache);
  static int create_cached_geometry(ObIAllocator &allocator, ObIAllocator &tmp_allocator, ObGeometry *geo,
                                    const ObSrsItem *srs, ObCachedGeom *&cached_geo);
  template<typename CachedGeoType>
  static int create_cached_geometry(ObIAllocator &allocator, ObGeometry *geo, ObCachedGeom *&cached_geo, const ObSrsItem *srs);
  static int get_geo_dimension(ObGeometry *geo, ObGeoDimension& dim);
  static int has_dimension(ObGeometry& geo, ObGeoDimension dim, bool& res);
  static bool is_point(const ObGeometry& geo) { return geo.type() == ObGeoType::POINT || geo.type() == ObGeoType::MULTIPOINT;}
  static bool is_line(const ObGeometry& geo) { return geo.type() == ObGeoType::LINESTRING || geo.type() == ObGeoType::MULTILINESTRING;}
  static bool is_polygon(const ObGeometry& geo) { return geo.type() == ObGeoType::POLYGON || geo.type() == ObGeoType::MULTIPOLYGON;}
  static bool use_point_polygon_short_circuit(const ObGeometry& geo1, const ObGeometry& geo2, ObItemType func_type);
  static int get_point_polygon_res(ObGeometry *geo1, ObGeometry *geo2, ObItemType func_type, bool& result);
  static bool need_get_srs(const uint32_t srid);
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
  template <typename RetType>
  static int get_number_obj_from_map(const QualifiedMap &map, const common::ObString &key, NumberObjType type,
                                    bool &is_null_result, RetType &res);
  template <typename ArrayType>
  static int get_varry_obj_from_map(const QualifiedMap &map, const common::ObString &key,
                                    const common::ObObjMeta &num_meta, NumberObjType type, ArrayType &array);
  static int append_point(double x, double y, ObWkbBuffer &wkb_buf);
  template<typename RingTree>
  static bool is_valid_ring_simple(const RingTree &ring);
  template<typename T_IBIN, typename T_BIN>
  static int get_collection_dimension(T_IBIN *geo, ObGeoDimension& dim);
  template<typename T_IBIN, typename T_BIN>
  static int collection_has_dimension(T_IBIN *geo, ObGeoDimension dim, bool& has);
  static int point_polygon_short_circuit(ObGeometry *poly, ObGeometry *point, ObPointLocation& loc, bool& has_internal, bool get_fartest);
  static int magnify_and_recheck(ObIAllocator &allocator, ObGeometry &geo, ObGeoEvalCtx& gis_context, bool& invalid_for_cache);
  static int check_valid_and_self_intersects(ObGeoEvalCtx& gis_context, bool& invalid_for_cache, bool& need_recheck);

  DISALLOW_COPY_AND_ASSIGN(ObGeoTypeUtil);
};

// also used for geom (Cartesian) type
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
  static int get_geom_poly_box(const ObWkbGeomPolygon &poly, bool not_calc_inner_ring, ObGeogBox &res);
  static int caculate_line_box(ObPoint3d &start, ObPoint3d &end, ObGeogBox &box);
  static void get_box_center(const ObGeogBox &box, ObPoint2d &center);
  static bool is_same_point3d(const ObPoint3d &p3d1, const ObPoint3d &p3d2);
  static bool is_completely_opposite(const ObPoint3d &p1, const ObPoint3d &p2);
  static void point_box_union(ObPoint3d &point, ObGeogBox &box);
  static void box_union(const ObGeogBox &box_tmp, ObGeogBox &box);
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
  static void get_point2d_from_geom_point(const ObWkbGeomInnerPoint &point, ObPoint2d &p2d);
  template<typename GeometryType>
  static int get_geom_line_box(const GeometryType &line, ObGeogBox &box);
  static int clip_by_box(ObGeometry &geo_in, ObIAllocator &allocator, const ObGeogBox &box, ObGeometry *&geo_out, bool is_called_in_pg_expr);
  static bool boxes_overlaps(const ObGeogBox &box1, const ObGeogBox &box2);
  static bool boxes_contains(const ObGeogBox &box1, const ObGeogBox &box2);
  template<typename GeometryType>
  static int fast_box(const GeometryType *g, ObGeogBox &box, bool &has_fast_box);
  static inline bool is_float_equal(double left, double right) { return fabs(left - right) <= OB_GEO_TOLERANCE; }
  static inline bool is_float_neq(double left, double right) { return fabs(left - right) > OB_GEO_TOLERANCE; }
  static inline bool is_float_lt(double left, double right) { return (left + OB_GEO_TOLERANCE) < right; }
  static inline bool is_float_lteq(double left, double right) { return (left - OB_GEO_TOLERANCE) <= right; }
  static inline bool is_float_gt(double left, double right) { return (left - OB_GEO_TOLERANCE) > right; }
  static inline bool is_float_gteq(double left, double right) { return (left + OB_GEO_TOLERANCE) >= right; }
  static inline bool is_float_zero(double ft) { return fabs(ft) <= OB_GEO_TOLERANCE; }
  static bool is_box_valid(const ObGeogBox &box);

  static constexpr double OB_GEO_TOLERANCE = 5e-14;
};

/*
/ x_fac1 y_fac1 z_fac1 x_off \  / x \
| x_fac2 y_fac2 z_fac2 y_off |  | y |
| x_fac3 y_fac3 z_fac3 z_off |  | z |
\   0      0       0     1   /  \ 1 /
*/
typedef struct
{
  double x_fac1;
  double y_fac1;
  double z_fac1;
  double x_fac2;
  double y_fac2;
  double z_fac2;
  double x_fac3;
  double y_fac3;
  double z_fac3;
  double x_off;
  double y_off;
  double z_off;
} ObAffineMatrix;
typedef struct
{
  // point of grid (x_ip, y_ip, z_ip)
  double x_ip;
  double y_ip;
  double z_ip;
  // length of grid
  double x_size;
  double y_size;
  double z_size;
} ObGeoGrid;

class ObGeoMVTUtil
{
public:
  // affine geometry in place
  static int affine_transformation(ObGeometry *geo, const ObAffineMatrix &affine);
  // snap to grid (aligned)
  static int snap_to_grid(ObGeometry *geo, const ObGeoGrid &grid, bool use_floor);
  static int simplify_geometry(ObGeometry *geo, double tolerance = 0.0, bool keep_collapsed = false);
  DISALLOW_COPY_AND_ASSIGN(ObGeoMVTUtil);
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
        box_union(box_tmp, box);
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

// only support PLINESTRING/MULTILINESTRING in catesian bin (ObWkbGeom)
template<typename GeometryType>
int ObGeoBoxUtil::fast_box(const GeometryType *g, ObGeogBox &box, bool &has_fast_box)
{
  int ret = OB_SUCCESS;
  has_fast_box = true;
  switch (g->type()) {
    case ObGeoType::POINT: {
      const ObWkbGeomPoint *pt = reinterpret_cast<const ObWkbGeomPoint *>(g);
      if (OB_ISNULL(pt)) {
        ret = OB_ERR_GIS_INVALID_DATA;
        OB_LOG(WARN, "invalid null geometry", K(ret));
      } else {
        box.xmin = box.xmax = pt->get<0>();
        box.ymin = box.ymax = pt->get<1>();
      }
      break;
    }
    case ObGeoType::MULTIPOINT: {
      const ObWkbGeomMultiPoint *pts = reinterpret_cast<const ObWkbGeomMultiPoint *>(g);
      if (OB_ISNULL(pts)) {
        ret = OB_ERR_GIS_INVALID_DATA;
        OB_LOG(WARN, "invalid null geometry", K(ret));
      } else if (pts->size() != 1) {
        has_fast_box = false;
      } else {
        ObWkbGeomMultiPoint::iterator iter = pts->begin();
        box.xmin = box.xmax = iter->get<0>();
        box.ymin = box.ymax = iter->get<1>();
      }
      break;
    }
    case ObGeoType::LINESTRING: {
      const ObWkbGeomLineString *line = reinterpret_cast<const ObWkbGeomLineString *>(g);
      if (OB_ISNULL(line)) {
        ret = OB_ERR_GIS_INVALID_DATA;
        OB_LOG(WARN, "invalid null geometry", K(ret));
      } else if (line->size() != 2) {
        has_fast_box = false;
      } else {
        ObWkbGeomLineString::iterator iter = line->begin();
        ObPoint2d pt;
        get_point2d_from_geom_point(*iter, pt);
        box.xmin = box.xmax = pt.x;
        box.ymin = box.ymax = pt.y;
        ++iter;
        get_point2d_from_geom_point(*iter, pt);
        box.xmin = OB_MIN(box.xmin, pt.x);
        box.xmax = OB_MAX(box.xmax, pt.x);
        box.ymin = OB_MIN(box.ymin, pt.y);
        box.ymax = OB_MAX(box.ymax, pt.y);
      }
      break;
    }
    case ObGeoType::MULTILINESTRING: {
      const ObWkbGeomMultiLineString *line = reinterpret_cast<const ObWkbGeomMultiLineString *>(g);
      if (OB_ISNULL(line)) {
        ret = OB_ERR_GIS_INVALID_DATA;
        OB_LOG(WARN, "invalid null geometry", K(ret));
      } else if (line->size() != 1) {
        has_fast_box = false;
      } else {
        ObWkbGeomMultiLineString::iterator iter = line->begin();
        ret = fast_box(iter.operator->(), box, has_fast_box);
      }
      break;
    }
    default: {
      // can not calculate fast box, but not a error
      has_fast_box = false;
    }
  }
  return ret;
}

template<typename GeometryType>
int ObGeoBoxUtil::get_geom_line_box(const GeometryType &line, ObGeogBox &box)
{
  int ret = OB_SUCCESS;
  typename GeometryType::iterator iter = line.begin();
  ObPoint2d point;
  get_point2d_from_geom_point(*iter, point);
  box.xmax = point.x;
  box.xmin = point.x;
  box.ymax = point.y;
  box.ymin = point.y;
  for (++iter; OB_SUCC(ret) && iter != line.end(); ++iter) {
    get_point2d_from_geom_point(*iter, point);
    box.xmin = OB_MIN(point.x, box.xmin);
    box.xmax = OB_MAX(point.x, box.xmax);
    box.ymin = OB_MIN(point.y, box.ymin);
    box.ymax = OB_MAX(point.y, box.ymax);
  }

  return ret;
}

template<typename RingTree>
bool ObGeoTypeUtil::is_valid_ring_simple(const RingTree &ring)
{
  // const ObCartesianLinearring &ring
  bool is_valid = true;
  int64_t sz = ring.size();
  if (sz < 3) {
    is_valid = false;
  } else {
    int32_t min_pos = 0;
    // find the point closest to the bottom left
    for (uint32_t i = 1; i < sz; ++i) {
      if (ring[i].template get<0>() < ring[min_pos].template get<0>()) {
        min_pos = i;
      } else if (ring[i].template get<0>() == ring[min_pos].template get<0>()
                && ring[i].template get<1>() < ring[min_pos].template get<1>()) {
        min_pos = i;
      }
    }
    int64_t prev_pos = min_pos - 1;
    if (min_pos == sz - 1) {
      is_valid = false;
    } else if (min_pos == 0) {
      if (ring[sz - 1].template get<0>() == ring[min_pos].template get<0>()
          && ring[sz - 1].template get<1>() == ring[min_pos].template get<1>()) {
        prev_pos = sz - 2;
        while (prev_pos >= 0 && ring[prev_pos].template get<0>() == ring[min_pos].template get<0>()
          && ring[prev_pos].template get<1>() == ring[min_pos].template get<1>()) {
          --prev_pos;
        }
        if (prev_pos < 0) {
          is_valid = false;
        }
      }
    }
    if (is_valid) {
      int64_t post_pos = min_pos + 1;
      while (post_pos < sz && ring[post_pos].template get<0>() == ring[min_pos].template get<0>()
        && ring[post_pos].template get<1>() == ring[min_pos].template get<1>()) {
        ++post_pos;
      }
      if (post_pos == sz) {
        is_valid = false;
      } else {
        double x1 = ring[min_pos].template get<0>() - ring[prev_pos].template get<0>();
        double y1 = ring[min_pos].template get<1>() - ring[prev_pos].template get<1>();
        double x2 = ring[post_pos].template get<0>() - ring[min_pos].template get<0>();
        double y2 = ring[post_pos].template get<1>() - ring[min_pos].template get<1>();
        double sign = x1 * y2 - x2 * y1;
        if (sign == 0) {
          is_valid = false;
        }
      }
    }
  }

  return is_valid;
}

template<typename PyTree, typename MpyTree, typename CollTree>
int ObGeoTypeUtil::is_polygon_valid_simple(const ObGeometry *geo, bool &res)
{
  int ret = OB_SUCCESS;
  res = true;
  const ObGeometry *geo_tree = nullptr;
  ObArenaAllocator tmp_allocator;
  if (!geo->is_tree()) {
    ObGeoToTreeVisitor tree_visit(&tmp_allocator);
    if (OB_FAIL(const_cast<ObGeometry *>(geo)->do_visit(tree_visit))) {
      OB_LOG(WARN, "fail to do tree visitor", K(ret));
    } else {
      geo_tree = tree_visit.get_geometry();
    }
  } else {
    geo_tree = geo;
  }
  if (OB_FAIL(ret)) {
  } else if (geo_tree->type() == ObGeoType::POLYGON) {
    const PyTree &poly = reinterpret_cast<const PyTree &>(*geo_tree);
    res = is_valid_ring_simple(poly.exterior_ring());
    for (int32_t i = 0; res && i < poly.inner_ring_size(); ++i) {
      res = is_valid_ring_simple(poly.inner_ring(i));
    }
  } else if (geo_tree->type() == ObGeoType::MULTIPOLYGON) {
    const MpyTree &mpy = *reinterpret_cast<const MpyTree *>(geo_tree);
    for (int32_t i = 0; i < mpy.size() && res; ++i) {
      res = is_valid_ring_simple(mpy[i].exterior_ring());
      for (int32_t j = 0; res && j < mpy[i].inner_ring_size(); ++j) {
        res = is_valid_ring_simple(mpy[i].inner_ring(j));
      }
    }
  } else if (geo_tree->type() == ObGeoType::GEOMETRYCOLLECTION) {
    const CollTree &coll = reinterpret_cast<const CollTree &>(*geo_tree);
    for (int32_t i = 0; i < coll.size() && OB_SUCC(ret) && res; i++) {
      if (OB_FAIL((is_polygon_valid_simple<PyTree, MpyTree, CollTree>(&coll[i], res)))) {
        OB_LOG(WARN, "failed to do tree item visit", K(ret));
      }
    }
  }
  return ret;
}


} // namespace common
} // namespace oceanbase

#endif
