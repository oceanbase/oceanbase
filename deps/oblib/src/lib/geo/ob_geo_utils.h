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
};

class ObGeoTypeUtil
{
public:
  static const uint32_t EWKB_SRID_FLAG = 0x20000000;
  static const uint32_t EWKB_M_FLAG = 0x40000000;
  static const uint32_t EWKB_Z_FLAG = 0x80000000;
  static const uint32_t WKB_3D_TYPE_OFFSET = 1000;
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
  static int rectangle_to_swkb(double xmin, double ymin, double xmax, double ymax, ObGeoSrid srid, bool with_version, ObWkbBuffer &wkb_buf);
  static int check_empty(ObGeometry *geo, bool &is_empty);
  static int get_st_geo_name_by_type(ObGeoType type, ObString &res);
  static int get_coll_dimension(ObIWkbGeomCollection *geo, int8_t &dimension);
  template<typename GcType>
  static int simplify_multi_geo(ObGeometry *&geo, common::ObIAllocator &allocator);
  static int convert_geometry_3D_to_2D(const ObSrsItem *srs, ObIAllocator &allocator, ObGeometry *g3d,
                                      uint8_t build_flag, ObGeometry *&geo);
  static int normalize_geometry(ObGeometry &geo, const ObSrsItem *srs);
  static double round_double(double x, int32_t dec, bool truncate);
  static double distance_point_squre(const ObWkbGeomInnerPoint& p1, const ObWkbGeomInnerPoint& p2);
  static int add_geo_version(ObIAllocator &allocator, const ObString &src, ObString &res_wkb);
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
  static int clip_by_box(ObGeometry &geo_in, ObIAllocator &allocator, const ObGeogBox &box, ObGeometry *&geo_out);
  static bool boxes_overlaps(const ObGeogBox &box1, const ObGeogBox &box2);
  static bool boxes_contains(const ObGeogBox &box1, const ObGeogBox &box2);
  template<typename GeometryType>
  static int fast_box(const GeometryType *g, ObGeogBox &box, bool &has_fast_box);

  static constexpr double FP_TOLERANCE = 5e-14;
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

template<typename GcType>
int ObGeoTypeUtil::simplify_multi_geo(ObGeometry *&geo, common::ObIAllocator &allocator)
{
  // e.g. MULTILINESTRING((0 0, 1 1)) -> LINESTRING(0 0, 1 1)
  int ret= OB_SUCCESS;
  switch (geo->type()) {
    case ObGeoType::MULTILINESTRING: {
      typename GcType::sub_ml_type *mp = reinterpret_cast<typename GcType::sub_ml_type *>(geo);
      if (OB_ISNULL(mp)) {
        ret = OB_ERR_GIS_INVALID_DATA;
        OB_LOG(WARN, "invalid null pointer", K(ret));
      } else if (mp->size() == 1) {
        geo = &(mp->front());
      }
      break;
    }
    case ObGeoType::MULTIPOINT: {
      typename GcType::sub_mpt_type  *mpt = reinterpret_cast<typename GcType::sub_mpt_type  *>(geo);
      if (OB_ISNULL(mpt)) {
        ret = OB_ERR_GIS_INVALID_DATA;
        OB_LOG(WARN, "invalid null pointer", K(ret));
      } else if (mpt->size() == 1) {
        typename GcType::sub_pt_type *p = OB_NEWx(typename GcType::sub_pt_type, &allocator);
        if (OB_ISNULL(p)) {
          ret = OB_ERR_GIS_INVALID_DATA;
          OB_LOG(WARN, "invalid null pointer", K(ret));
        } else {
          p->set_data(mpt->front());
          geo = p;
        }
      }
      break;
    }
    case ObGeoType::MULTIPOLYGON: {
      typename GcType::sub_mp_type *mp = reinterpret_cast<typename GcType::sub_mp_type *>(geo);
      if (OB_ISNULL(mp)) {
        ret = OB_ERR_GIS_INVALID_DATA;
        OB_LOG(WARN, "invalid null pointer", K(ret));
      } else if (mp->size() == 1) {
        geo = &(mp->front());
      }
      break;
    }
    case ObGeoType::GEOMETRYCOLLECTION: {
      GcType *mp = reinterpret_cast<GcType *>(geo);
      if (OB_ISNULL(mp)) {
        ret = OB_ERR_GIS_INVALID_DATA;
        OB_LOG(WARN, "invalid null pointer", K(ret));
      } else if (mp->size() == 1) {
        geo = &(mp->front());
        if (OB_FAIL((simplify_multi_geo<GcType>(geo, allocator)))) {
          OB_LOG(WARN, "fail to simplify geometry", K(ret));
        }
      }
      break;
    }
    default: {
      break;  // do nothing
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

} // namespace common
} // namespace oceanbase

#endif
