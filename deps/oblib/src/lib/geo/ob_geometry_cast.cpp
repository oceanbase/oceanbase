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
 * This file contains implementation for geometry cast.
 */

#include "lib/ob_errno.h"
#define USING_LOG_PREFIX LIB

#include "lib/geo/ob_geometry_cast.h"
#include "lib/geo/ob_geo_func_register.h"
#include "lib/geo/ob_geo_func_common.h"
#include "lib/geo/ob_geo_to_tree_visitor.h"
#include "lib/geo/ob_geo_wkb_visitor.h"
#include "lib/geo/ob_geo_wkb_size_visitor.h"
#include "lib/geo/ob_geo_utils.h"

namespace oceanbase
{
namespace common
{
using namespace oceanbase::common;

const char *ObGeometryTypeCastUtil::get_cast_name(ObGeoType type)
{
  const char *type_name = "cast_as_unknown";
  switch (type) {
    case ObGeoType::GEOMETRY:{
      type_name =  "cast_as_geometry";
      break;
    }
    case ObGeoType::POINT:{
      type_name =  "cast_as_point";
      break;
    }
    case ObGeoType::LINESTRING:{
      type_name =  "cast_as_linestring";
      break;
    }
    case ObGeoType::POLYGON:{
      type_name = "cast_as_polygon";
      break;
    }
    case ObGeoType::MULTIPOINT:{
      type_name = "cast_as_multipoint";
      break;
    }
    case ObGeoType::MULTILINESTRING:{
      type_name = "cast_as_multilinestring";
      break;
    }
    case ObGeoType::MULTIPOLYGON:{
      type_name = "cast_as_multipolygon";
      break;
    }
    case ObGeoType::GEOMETRYCOLLECTION:{
      type_name = "cast_as_geometrycollection";
      break;
    }
    default:{
      LOG_WARN_RET(OB_INVALID_ARGUMENT, "unknown geometry type", K(type));
      break;
    }
  }
  return type_name;
}

int ObGeometryTypeCastUtil::check_longitude(double val_radian,
                                            const ObSrsItem *srs,
                                            double val)
{
  int ret = OB_SUCCESS;
  double max_long_val = 0.0;
  double min_long_val = 0.0;

  if (val_radian <= -M_PI || val_radian > M_PI) {
    if (OB_FAIL(srs->longtitude_convert_from_radians(-M_PI, min_long_val))) {
      LOG_WARN("fail to convert longitude from radians", K(ret));
    } else if (OB_FAIL(srs->longtitude_convert_from_radians(M_PI, max_long_val))) {
      LOG_WARN("fail to convert longitude from radians", K(ret));
    } else {
      ret = OB_ERR_LONGITUDE_OUT_OF_RANGE;
      LOG_WARN("longitude value is out of range", K(ret), K(val), K(val_radian));
    }
  }

  return ret;
}

int ObGeometryTypeCastUtil::check_latitude(double val_radian,
                                           const ObSrsItem *srs,
                                           double val)
{
  int ret = OB_SUCCESS;
  double max_lat_val = 0.0;
  double min_lat_val = 0.0;

  if (val_radian < -M_PI_2 || val_radian > M_PI_2) {
    if (OB_FAIL(srs->latitude_convert_from_radians(-M_PI_2, min_lat_val))) {
      LOG_WARN("fail to convert latitude from radians", K(ret));
    } else if (OB_FAIL(srs->latitude_convert_from_radians(M_PI_2, max_lat_val))) {
      LOG_WARN("fail to convert latitude from radians", K(ret));
    } else {
      ret = OB_ERR_LATITUDE_OUT_OF_RANGE;
      LOG_WARN("latitude value is out of range", K(ret), K(val), K(val_radian));
    }
  }

  return ret;
}

int ObGeometryTypeCastUtil::get_tree(ObIAllocator &allocator,
                                     const ObString &wkb,
                                     ObGeometry *&geo_tree,
                                     const ObSrsItem *srs,
                                     ObGeoErrLogInfo &log_info,
                                     const char *func_name)
{
  int ret = OB_SUCCESS;
  ObGeometry *geo = NULL;

  if (OB_FAIL(ObGeoTypeUtil::build_geometry(allocator, wkb, geo, srs, log_info))) {
    LOG_WARN("fail to build geometry from wkb", K(ret));
  } else if (geo->is_tree()) {
    geo_tree = geo;
  } else {
    ObGeoToTreeVisitor visitor(&allocator);
    if (OB_FAIL(geo->do_visit(visitor))) {
      LOG_WARN("fail to do visit", K(ret), K(geo->type()));
    } else {
      geo_tree = visitor.get_geometry();
      geo_tree->set_srid(geo->get_srid());
    }
  }

  return ret;
}

int ObGeometryTypeCastUtil::check_polygon_direction(ObIAllocator &allocator,
                                                    const common::ObSrsItem *srs,
                                                    ObGeometry *geo_tree)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(geo_tree)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("geo_tree is null", K(ret));
  } else if (OB_FAIL(ObGeoTypeUtil::correct_polygon(allocator, srs, true, *geo_tree))) {
    LOG_WARN("fail to check correct in functor", K(ret), K(geo_tree->get_srid()));
    ret = OB_ERR_GIS_INVALID_DATA;
  }

  return ret;
}

template<typename P>
bool ObGeometryTypeCastUtil::is_point_equal(const P &p_left, const P &p_right)
{
  bool is_equal = true;

  if ((p_left.template get<0>() != p_right.template get<0>()) ||
      (p_left.template get<1>() != p_right.template get<1>())) {
    is_equal = false;
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "the back and front of the linestring are not the same point",
        K(p_left.template get<0>()), K(p_right.template get<0>()),
        K(p_left.template get<1>()), K(p_right.template get<1>()));
  }

  return is_equal;
}

template<typename L>
bool ObGeometryTypeCastUtil::is_line_can_ring(const L &line)
{
  typename L::const_iterator begin = line.begin();
  typename L::const_iterator last = line.end() - 1;

  return is_point_equal(*begin, *last);
}

bool ObGeometryTypeCastUtil::is_sdo_geometry_varray_type(uint64_t udt_id)
{
  return udt_id == T_OBJ_SDO_ELEMINFO_ARRAY || udt_id == T_OBJ_SDO_ORDINATE_ARRAY;
}

bool ObGeometryTypeCastUtil::is_sdo_geometry_udt(uint64_t udt_id)
{
  return udt_id >= T_OBJ_SDO_POINT && udt_id <= T_OBJ_SDO_ORDINATE_ARRAY;
}

bool ObGeometryTypeCastUtil::is_sdo_geometry_type_compatible(uint64_t src_udt_id, uint64_t dst_udt_id)
{
  bool bret = true;
  if (!is_sdo_geometry_varray_type(src_udt_id) || !is_sdo_geometry_varray_type(dst_udt_id)) {
    // only allow varray (elem_info/ordiante_array) cast to each other
    bret = false;
  }
  return bret;
}

int ObGeometryTypeCastFactory::alloc(ObIAllocator &alloc,
                                     ObGeoType geo_type,
                                     ObGeometryTypeCast *&geo_cast)
{
  int ret = OB_SUCCESS;
  void *buf = NULL;

  switch (geo_type)
  {
    case ObGeoType::POINT: {
      if (OB_ISNULL(buf = alloc.alloc(sizeof(ObPointTypeCast)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory for ObPointTypeCast", K(ret));
      } else {
        geo_cast = new (buf) ObPointTypeCast();
      }
      break;
    }

    case ObGeoType::LINESTRING: {
      if (OB_ISNULL(buf = alloc.alloc(sizeof(ObLineStringTypeCast)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory for ObLineStringTypeCast", K(ret));
      } else {
        geo_cast = new (buf) ObLineStringTypeCast();
      }
      break;
    }

    case ObGeoType::POLYGON: {
      if (OB_ISNULL(buf = alloc.alloc(sizeof(ObPolygonTypeCast)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory for ObPolygonTypeCast", K(ret));
      } else {
        geo_cast = new (buf) ObPolygonTypeCast();
      }
      break;
    }

    case ObGeoType::MULTIPOINT: {
      if (OB_ISNULL(buf = alloc.alloc(sizeof(ObMultiPointTypeCast)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory for ObMultiPointTypeCast", K(ret));
      } else {
        geo_cast = new (buf) ObMultiPointTypeCast();
      }
      break;
    }

    case ObGeoType::MULTILINESTRING: {
      if (OB_ISNULL(buf = alloc.alloc(sizeof(ObMultiLineStringTypeCast)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory for ObMultiLineStringTypeCast", K(ret));
      } else {
        geo_cast = new (buf) ObMultiLineStringTypeCast();
      }
      break;
    }

    case ObGeoType::MULTIPOLYGON: {
      if (OB_ISNULL(buf = alloc.alloc(sizeof(ObMultiPolygonTypeCast)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory for ObMultiPolygonTypeCast", K(ret));
      } else {
        geo_cast = new (buf) ObMultiPolygonTypeCast();
      }
      break;
    }

    case ObGeoType::GEOMETRYCOLLECTION: {
      if (OB_ISNULL(buf = alloc.alloc(sizeof(ObGeomcollectionTypeCast)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory for ObGeomcollectionTypeCast", K(ret));
      } else {
        geo_cast = new (buf) ObGeomcollectionTypeCast();
      }
      break;
    }

    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid geometry type for cast", K(ret), K(geo_type));
      break;
    }
  }
  return ret;
}

template<typename P, typename MPT, typename GC>
int ObPointTypeCast::cast(const ObGeometry &src,
                          ObGeometry &dst,
                          const ObSrsItem *srs,
                          ObGeoErrLogInfo &log_info,
                          ObIAllocator *allocator/* = NULL */) const
{
  UNUSED(allocator);
  int ret = OB_SUCCESS;
  ObGeoType wkb_type = src.type();
  P &res = *static_cast<P *>(&dst);

  if (ObGeoType::POINT != wkb_type &&
      ObGeoType::MULTIPOINT != wkb_type &&
      ObGeoType::GEOMETRYCOLLECTION != wkb_type) {
    ret = OB_ERR_INVALID_CAST_TO_GEOMETRY;
    LOG_WARN("unexpected type", K(ret), K(wkb_type));
  } else {
    switch (wkb_type) {
      // POINT -> POINT
      case ObGeoType::POINT: {
        const P &po = *static_cast<const P *>(&src);
        uint32_t srid = src.get_srid();
        if (srid == 0) {
          res.x(po.x());
          res.y(po.y());
        } else if (OB_ISNULL(srs)) {
          ret = OB_ERR_NULL_VALUE;
          LOG_WARN("srs is null", K(ret));
        } else {
          double x = po.x();
          double y = po.y();
          double x_radian = 0.0;
          double y_radian = 0.0;
          if (OB_FAIL(srs->from_srs_unit_to_radians(x, x_radian))) {
            LOG_WARN("fail to convert longitude to radians", K(x));
          } else if (OB_FAIL(srs->from_srs_unit_to_radians(y, y_radian))) {
            LOG_WARN("fail to convert latitude to radians", K(y));
          } else if (OB_FAIL(ObGeometryTypeCastUtil::check_longitude(x_radian, srs, x))) {
            ret = OB_ERR_GEOMETRY_PARAM_LONGITUDE_OUT_OF_RANGE;
            log_info.value_out_of_range_ = x;
          } else if (OB_FAIL(ObGeometryTypeCastUtil::check_latitude(y_radian, srs, y))) {
            ret = OB_ERR_GEOMETRY_PARAM_LATITUDE_OUT_OF_RANGE;
            log_info.value_out_of_range_ = y;
          } else {
            res.x(po.x());
            res.y(po.y());
          }
        }
        break;
      }

      // MULTIPOINT -> POINT
      case ObGeoType::MULTIPOINT: {
        const MPT &mp = *static_cast<const MPT *>(&src);
        if (mp.size() != 1) {
          ret = OB_ERR_INVALID_CAST_TO_GEOMETRY;
          LOG_WARN("unexpected size", K(ret), K(mp.size()));
        } else {
          res.set_data(mp.front());
        }
        break;
      }

      // GEOMETRYCOLLECTION -> POINT
      case ObGeoType::GEOMETRYCOLLECTION: {
        const GC &gc = *static_cast<const GC *>(&src);
        if (gc.size() != 1) {
          ret = OB_ERR_INVALID_CAST_TO_GEOMETRY;
          LOG_WARN("unexpected size", K(ret), K(gc.size()));
        } else if (ObGeoType::POINT != gc.front().type()) {
          ret = OB_ERR_INVALID_CAST_TO_GEOMETRY;
          LOG_WARN("unexpected type", K(ret), K(gc.front().type()));
        } else {
          res.x(static_cast<const P *>(&gc.front())->x());
          res.y(static_cast<const P *>(&gc.front())->y());
        }
        break;
      }

      default: {
        ret = OB_ERR_INVALID_CAST_TO_GEOMETRY;
        LOG_WARN("unexpected type", K(ret), K(wkb_type));
        break;
      }
    }
  }

  return ret;
}

template<typename L, typename PL, typename MPT, typename ML, typename GC>
int ObLineStringTypeCast::cast(const ObGeometry &src,
                               ObGeometry &dst,
                               const ObSrsItem *srs,
                               ObGeoErrLogInfo &log_info,
                               ObIAllocator *allocator/* = NULL */) const
{
  UNUSEDx(srs, log_info, allocator);
  int ret = OB_SUCCESS;
  ObGeoType wkb_type = src.type();
  L &res = *static_cast<L *>(&dst);

  if (ObGeoType::LINESTRING != wkb_type &&
      ObGeoType::POLYGON != wkb_type &&
      ObGeoType::MULTIPOINT != wkb_type &&
      ObGeoType::MULTILINESTRING != wkb_type &&
      ObGeoType::GEOMETRYCOLLECTION != wkb_type) {
    ret = OB_ERR_INVALID_CAST_TO_GEOMETRY;
    LOG_WARN("unexpected type", K(ret), K(wkb_type));
  } else {
    switch (wkb_type) {
      // LINESTRING -> LINESTRING
      case ObGeoType::LINESTRING: {
        const L &line = *static_cast<const L *>(&src);
        res = line;
        break;
      }

      // POLYGON -> LINESTRING
      case ObGeoType::POLYGON: {
        const PL &poly = *static_cast<const PL *>(&src);
        if (poly.size() != 1) {
          ret = OB_ERR_INVALID_CAST_TO_GEOMETRY;
          LOG_WARN("unexpected size", K(ret), K(poly.size()));
        } else if (poly.exterior_ring().empty()) {
          ret = OB_ERR_INVALID_CAST_TO_GEOMETRY;
          LOG_WARN("ring is empty", K(ret), K(poly.exterior_ring().empty()));
        } else {
          for (int32_t i = 0; OB_SUCC(ret) && i < poly.exterior_ring().size(); i++) {
            if (OB_FAIL(res.push_back(poly.exterior_ring()[i]))) {
              LOG_WARN("fail to push back inner point", K(ret));
            }
          }
        }
        break;
      }

      // MULTIPOINT -> LINESTRING
      case ObGeoType::MULTIPOINT: {
        const MPT &mp = *static_cast<const MPT *>(&src);
        if (mp.size() < 2) {
          ret = OB_ERR_INVALID_CAST_TO_GEOMETRY;
          LOG_WARN("unexpected size", K(ret), K(mp.size()));
        } else {
          typename MPT::const_iterator iter = mp.begin();
          for (; OB_SUCC(ret) && iter != mp.end(); ++iter) {
            if (OB_FAIL(res.push_back(*iter))) {
              LOG_WARN("fail to push back inner point", K(ret));
            }
          }
        }
        break;
      }

      // MULTILINESTRING -> LINESTRING
      case ObGeoType::MULTILINESTRING: {
        const ML &ml = *static_cast<const ML *>(&src);
        if (ml.size() != 1) {
          ret = OB_ERR_INVALID_CAST_TO_GEOMETRY;
          LOG_WARN("unexpected size", K(ret), K(ml.size()));
        } else {
          res = ml.front();
        }
        break;
      }

      // GEOMETRYCOLLECTION -> LINESTRING
      case ObGeoType::GEOMETRYCOLLECTION: {
        const GC &gc = *static_cast<const GC *>(&src);
        if (gc.size() != 1) {
          ret = OB_ERR_INVALID_CAST_TO_GEOMETRY;
          LOG_WARN("unexpected size or type", K(ret), K(gc.size()));
        } else if (ObGeoType::LINESTRING != gc.front().type()) {
          ret = OB_ERR_INVALID_CAST_TO_GEOMETRY;
          LOG_WARN("unexpected type", K(ret), K(gc.front().type()));
        } else {
          res = *static_cast<const L *>(&gc.front());
        }
        break;
      }

      default: {
        ret = OB_ERR_INVALID_CAST_TO_GEOMETRY;
        LOG_WARN("unexpected type", K(ret), K(wkb_type));
        break;
      }
    }
  }

  return ret;
}

template<typename PL, typename L, typename ML, typename MPL, typename GC, typename LR>
int ObPolygonTypeCast::cast(const ObGeometry &src,
                            ObGeometry &dst,
                            const ObSrsItem *srs,
                            ObGeoErrLogInfo &log_info,
                            ObIAllocator *allocator) const
{
  UNUSED(log_info);
  int ret = OB_SUCCESS;
  ObGeoType wkb_type = src.type();
  PL &res = *static_cast<PL *>(&dst);

  if (OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("allocator is null", K(ret), K(wkb_type));
  } else if (ObGeoType::LINESTRING != wkb_type &&
             ObGeoType::POLYGON != wkb_type &&
             ObGeoType::MULTILINESTRING != wkb_type &&
             ObGeoType::MULTIPOLYGON != wkb_type &&
             ObGeoType::GEOMETRYCOLLECTION != wkb_type) {
    ret = OB_ERR_INVALID_CAST_TO_GEOMETRY;
    LOG_WARN("unexpected type", K(ret), K(wkb_type));
  } else {
    switch (wkb_type) {
      // LINESTRING -> POLYGON
      case ObGeoType::LINESTRING: {
        const L &line = *static_cast<const L *>(&src);
        // If the back and front of the linestring are not the same point, this cannot be a linear ring.
        if (line.size() < 4 || !ObGeometryTypeCastUtil::is_line_can_ring(line)) {
          ret = OB_ERR_INVALID_CAST_TO_GEOMETRY;
          LOG_WARN("unexpected size", K(ret), K(line.size()));
        } else {
          typename L::const_iterator iter = line.begin();
          for (; OB_SUCC(ret) && iter != line.end(); ++iter) {
            if (OB_FAIL(res.exterior_ring().push_back(*iter))) {
              LOG_WARN("fail to push back inner point", K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(ObGeometryTypeCastUtil::check_polygon_direction(*allocator, srs, &res))) {
              LOG_WARN("fail to check correct", K(ret));
            } else {
              PL *new_poly = static_cast<PL *>(&res);
              // Check each point of the linestring against the exterior ring of the polygon
              for (int32_t i = 0; OB_SUCC(ret) && i < line.size(); i++) {
                if (!ObGeometryTypeCastUtil::is_point_equal(line[i], new_poly->exterior_ring()[i])) {
                  ret = OB_ERR_INVALID_CAST_POLYGON_RING_DIRECTION;
                  LOG_WARN("invalid ring direction", K(ret));
                }
              }
            }
          }
        }
        break;
      }

      // POLYGON -> POLYGON
      case ObGeoType::POLYGON: {
        const PL &poly = *static_cast<const PL *>(&src);
        res = poly;
        break;
      }

      // MULTILINESTRING -> POLYGON
      case ObGeoType::MULTILINESTRING: {
        void *buf = NULL;
        const ML &ml = *static_cast<const ML *>(&src);
        for (int32_t i = 0; OB_SUCC(ret) && i < ml.size(); i++) {
          const L &line = *static_cast<const L *>(&ml[i]);
          // If the back and front of the linestring are not the same point, this cannot be a linear ring.
          if (line.size() < 4 || !ObGeometryTypeCastUtil::is_line_can_ring(line)) {
            ret = OB_ERR_INVALID_CAST_TO_GEOMETRY;
            LOG_WARN("unexpected size", K(ret), K(line.size()));
          } else if (OB_ISNULL(buf = allocator->alloc(sizeof(LR)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc memory", K(ret), K(sizeof(LR)));
          } else {
            LR *lr = new (buf) LR (src.get_srid(), *allocator);
            typename L::const_iterator l_iter = line.begin();
            for (; OB_SUCC(ret) && l_iter != line.end(); ++l_iter) {
              if (OB_FAIL(lr->push_back(*l_iter))) {
                LOG_WARN("fail to push back inner point", K(ret));
              }
            }
            if (OB_SUCC(ret) && OB_FAIL(res.push_back(*lr))) {
              LOG_WARN("fail to push back linearring", K(ret));
            }
            // Flip polygon rings and compare order of points with original
            // linestring, to check whether input linestring was valid
            if (OB_SUCC(ret)) {
              if (OB_FAIL(ObGeometryTypeCastUtil::check_polygon_direction(*allocator, srs, &res))) {
                LOG_WARN("fail to check correct", K(ret));
              } else {
                PL *new_poly = static_cast<PL *>(&res);
                // Check each point of the current linestring against the current polygon ring
                const LR *tmp_lr = (i == 0 ? &new_poly->exterior_ring() : &new_poly->inner_ring(i - 1));
                for (int32_t i = 0; OB_SUCC(ret) && i < line.size(); i++) {
                  if (!ObGeometryTypeCastUtil::is_point_equal(line[i], (*tmp_lr)[i])) {
                    ret = OB_ERR_INVALID_CAST_POLYGON_RING_DIRECTION;
                    LOG_WARN("invalid ring direction", K(ret));
                  }
                }
              }
            }
          }
        }
        break;
      }

      // MULTIPOLYGON -> POLYGON
      case ObGeoType::MULTIPOLYGON: {
        const MPL &mpoly = *static_cast<const MPL *>(&src);
        if (mpoly.size() != 1) {
          ret = OB_ERR_INVALID_CAST_TO_GEOMETRY;
          LOG_WARN("unexpected size", K(ret), K(mpoly.size()));
        } else {
          res = mpoly.front();
        }
        break;
      }

      // GEOMETRYCOLLECTION -> POLYGON
      case ObGeoType::GEOMETRYCOLLECTION: {
        const GC &gc = *static_cast<const GC *>(&src);
        if (gc.size() != 1) {
          ret = OB_ERR_INVALID_CAST_TO_GEOMETRY;
          LOG_WARN("unexpected size", K(ret), K(gc.size()));
        } else if (ObGeoType::POLYGON != gc.front().type()) {
          ret = OB_ERR_INVALID_CAST_TO_GEOMETRY;
          LOG_WARN("unexpected type", K(ret), K(gc.front().type()));
        } else {
          res = *static_cast<const PL *>(&gc.front());
        }
        break;
      }

      default: {
        ret = OB_ERR_INVALID_CAST_TO_GEOMETRY;
        LOG_WARN("invalid type", K(ret), K(wkb_type));
        break;
      }
    }
  }

  return ret;
}

template<typename MPT, typename P, typename L, typename GC>
int ObMultiPointTypeCast::cast(const ObGeometry &src,
                               ObGeometry &dst,
                               const ObSrsItem *srs,
                               ObGeoErrLogInfo &log_info,
                               ObIAllocator *allocator/* = NULL */) const
{
  UNUSEDx(srs, log_info, allocator);
  int ret = OB_SUCCESS;
  ObGeoType wkb_type = src.type();
  MPT &res = *static_cast<MPT *>(&dst);

  if (ObGeoType::POINT != wkb_type &&
      ObGeoType::LINESTRING != wkb_type &&
      ObGeoType::MULTIPOINT != wkb_type &&
      ObGeoType::GEOMETRYCOLLECTION != wkb_type) {
    ret = OB_ERR_INVALID_CAST_TO_GEOMETRY;
    LOG_WARN("unexpected type", K(ret), K(wkb_type));
  } else {
    switch (wkb_type) {
      // POINT -> MULTIPOINT
      case ObGeoType::POINT: {
        const P &po = *static_cast<const P *>(&src);
        if (OB_FAIL(res.push_back(po.data()))) {
          LOG_WARN("fail to push back inner point", K(ret));
        }
        break;
      }

      // LINESTRING -> MULTIPOINT
      case ObGeoType::LINESTRING: {
        const L &line = *static_cast<const L *>(&src);
        typename L::const_iterator iter = line.begin();
        for (; OB_SUCC(ret) && iter != line.end(); ++iter) {
          if (OB_FAIL(res.push_back(*iter))) {
            LOG_WARN("fail to push back inner point", K(ret));
          }
        }
        break;
      }

      // MULTIPOINT -> MULTIPOINT
      case ObGeoType::MULTIPOINT: {
        const MPT &mp = *static_cast<const MPT *>(&src);
        typename MPT::const_iterator iter = mp.begin();
        for (; OB_SUCC(ret) && iter != mp.end(); ++iter) {
          if (OB_FAIL(res.push_back(*iter))) {
            LOG_WARN("fail to push back inner point", K(ret));
          }
        }
        break;
      }

      // GEOMETRYCOLLECTION -> MULTIPOINT
      case ObGeoType::GEOMETRYCOLLECTION: {
        const GC &gc = *static_cast<const GC *>(&src);
        if (gc.is_empty()) {
          ret = OB_ERR_INVALID_CAST_TO_GEOMETRY;
          LOG_WARN("gc is empty", K(ret), K(gc.is_empty()));
        } else {
          typename GC::const_iterator iter = gc.begin();
          for (; OB_SUCC(ret) && iter != gc.end(); ++iter) {
            if (ObGeoType::POINT != (*iter)->type()) {
              ret = OB_ERR_INVALID_CAST_TO_GEOMETRY;
              LOG_WARN("invalid type", K(ret), K((*iter)->type()));
            } else {
              const P &po = *static_cast<const P *>(*iter);
              if (OB_FAIL(res.push_back(po.data()))) {
                LOG_WARN("fail to push back inner point", K(ret));
              }
            }
          }
        }
        break;
      }

      default: {
        ret = OB_ERR_INVALID_CAST_TO_GEOMETRY;
        LOG_WARN("invalid type", K(ret), K(wkb_type));
        break;
      }
    }
  }

  return ret;
}

template<typename ML, typename L, typename PL, typename MPL, typename GC, typename LR>
int ObMultiLineStringTypeCast::cast(const ObGeometry &src,
                                    ObGeometry &dst,
                                    const ObSrsItem *srs,
                                    ObGeoErrLogInfo &log_info,
                                    ObIAllocator *allocator) const
{
  UNUSEDx(srs, log_info);
  int ret = OB_SUCCESS;
  ObGeoType wkb_type = src.type();
  ML &res = *static_cast<ML *>(&dst);

  if (OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("allocator is null", K(ret), K(wkb_type));
  } else if (ObGeoType::LINESTRING != wkb_type &&
             ObGeoType::POLYGON != wkb_type &&
             ObGeoType::MULTILINESTRING != wkb_type &&
             ObGeoType::MULTIPOLYGON != wkb_type &&
             ObGeoType::GEOMETRYCOLLECTION != wkb_type) {
    ret = OB_ERR_INVALID_CAST_TO_GEOMETRY;
    LOG_WARN("unexpected type", K(ret), K(wkb_type));
  } else {
    switch (wkb_type) {
      // LINESTRING -> MULTILINESTRING
      case ObGeoType::LINESTRING: {
        const L &line = *static_cast<const L *>(&src);
        if (OB_FAIL(res.push_back(line))) {
          LOG_WARN("fail to push back linestring", K(ret));
        }
        break;
      }

      // POLYGON -> MULTILINESTRING
      case ObGeoType::POLYGON: {
        void *buf = NULL;
        const PL &poly = *static_cast<const PL *>(&src);
        for (int32_t i = 0; OB_SUCC(ret) && i < poly.size(); i++) {
          const LR *lr = NULL;
          if (i == 0) {
            lr = &poly.exterior_ring();
          } else {
            lr = &poly.inner_ring(i - 1);
          }
          if (OB_ISNULL(buf = allocator->alloc(sizeof(L)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc memory", K(ret), K(sizeof(L)));
          } else {
            L *line = new (buf) L (src.get_srid(), *allocator);
            for (int32_t j = 0; OB_SUCC(ret) && j < lr->size(); j++) {
              if (OB_FAIL(line->push_back((*lr)[j]))) {
                LOG_WARN("fail to push back inner point", K(ret), K(j));
              }
            }
            if (OB_SUCC(ret) && OB_FAIL(res.push_back(*line))) {
              LOG_WARN("fail to push back linestring", K(ret), K(i));
            }
          }
        }
        break;
      }

      // MULTILINESTRING -> MULTILINESTRING
      case ObGeoType::MULTILINESTRING: {
        const ML &ml = *static_cast<const ML *>(&src);
        for (int32_t i = 0; OB_SUCC(ret) && i < ml.size(); i++) {
          if (OB_FAIL(res.push_back(ml[i]))) {
            LOG_WARN("fail to push back linestring", K(ret), K(i), K(ml.size()));
          }
        }
        break;
      }

      // MULTIPOLYGON -> MULTILINESTRING
      case ObGeoType::MULTIPOLYGON: {
        const MPL &mpoly = *static_cast<const MPL *>(&src);
        for (int32_t i = 0; OB_SUCC(ret) && i < mpoly.size(); i++) {
          if (mpoly[i].size() != 1) {
            ret = OB_ERR_INVALID_CAST_TO_GEOMETRY;
            LOG_WARN("unexpected size", K(ret), K(mpoly[i].size()));
          } else {
            void *buf = NULL;
            if (OB_ISNULL(buf = allocator->alloc(sizeof(L)))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("fail to alloc memory", K(ret), K(sizeof(L)));
            } else {
              L *line = new (buf) L (src.get_srid(), *allocator);
              for (int32_t j = 0; OB_SUCC(ret) && j < mpoly[i].exterior_ring().size(); j++) {
                if (OB_FAIL(line->push_back(mpoly[i].exterior_ring()[j]))) {
                  LOG_WARN("fail to push back inner point", K(ret), K(j));
                }
              }
              if (OB_SUCC(ret) && OB_FAIL(res.push_back(*line))) {
                LOG_WARN("fail to push back linestring", K(ret), K(i));
              }
            }
          }
        }
        break;
      }

      // GEOMETRYCOLLECTION -> MULTILINESTRING
      case ObGeoType::GEOMETRYCOLLECTION: {
        const GC &gc = *static_cast<const GC *>(&src);
        if (gc.is_empty()) {
          ret = OB_ERR_INVALID_CAST_TO_GEOMETRY;
          LOG_WARN("gc is empty", K(ret), K(gc.is_empty()));
        } else {
          typename GC::const_iterator iter = gc.begin();
          for (; OB_SUCC(ret) && iter != gc.end(); ++iter) {
            if (ObGeoType::LINESTRING != (*iter)->type()) {
              ret = OB_ERR_INVALID_CAST_TO_GEOMETRY;
              LOG_WARN("invalid type", K(ret), K((*iter)->type()));
            } else {
              const L &line = *static_cast<const L *>(*iter);
              if (OB_FAIL(res.push_back(line))) {
                LOG_WARN("fail to push back linestring", K(ret));
              }
            }
          }
        }
        break;
      }

      default: {
        ret = OB_ERR_INVALID_CAST_TO_GEOMETRY;
        LOG_WARN("invalid type", K(ret), K(wkb_type));
        break;
      }
    }
  }

  return ret;
}

template<typename MPL, typename PL, typename ML, typename GC, typename L, typename LR>
int ObMultiPolygonTypeCast::cast(const ObGeometry &src,
                                 ObGeometry &dst,
                                 const ObSrsItem *srs,
                                 ObGeoErrLogInfo &log_info,
                                 ObIAllocator *allocator/* = NULL */) const
{
  UNUSED(log_info);
  int ret = OB_SUCCESS;
  ObGeoType wkb_type = src.type();
  MPL &res = *static_cast<MPL *>(&dst);

  if (OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("allocator is null", K(ret), K(wkb_type));
  } else if (ObGeoType::POLYGON != wkb_type &&
             ObGeoType::MULTILINESTRING != wkb_type &&
             ObGeoType::MULTIPOLYGON != wkb_type &&
             ObGeoType::GEOMETRYCOLLECTION != wkb_type) {
    ret = OB_ERR_INVALID_CAST_TO_GEOMETRY;
    LOG_WARN("unexpected type", K(ret), K(wkb_type));
  } else {
    switch (wkb_type) {
      // POLYGON -> MULTIPOLYGON
      case ObGeoType::POLYGON: {
        const PL &poly = *static_cast<const PL *>(&src);
        if (OB_FAIL(res.push_back(poly))) {
          LOG_WARN("fail to push back linearring", K(ret));
        }
        break;
      }

      // MULTILINESTRING -> MULTIPOLYGON
      case ObGeoType::MULTILINESTRING: {
        void *la_buf = NULL;
        void *poly_buf = NULL;
        const ML &ml = *static_cast<const ML *>(&src);
        for (int32_t i = 0; OB_SUCC(ret) && i < ml.size(); ++i) {
          const L &line = *static_cast<const L *>(&ml[i]);
          // If the back and front of the linestring are not the same point, this cannot be a linear ring.
          if (line.size() < 4 || !ObGeometryTypeCastUtil::is_line_can_ring(line)) {
            ret = OB_ERR_INVALID_CAST_TO_GEOMETRY;
            LOG_WARN("unexpected size", K(ret), K(line.size()));
          } else if (OB_ISNULL(la_buf = allocator->alloc(sizeof(LR)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc memory", K(ret), K(sizeof(LR)));
          } else if (OB_ISNULL(poly_buf = allocator->alloc(sizeof(PL)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc memory", K(ret), K(sizeof(PL)));
          } else {
            LR *lr = new (la_buf) LR (src.get_srid(), *allocator);
            PL *poly = new (poly_buf) PL (src.get_srid(), *allocator);
            typename L::const_iterator l_iter = line.begin();
            for (; OB_SUCC(ret) && l_iter != line.end(); ++l_iter) {
              if (OB_FAIL(lr->push_back(*l_iter))) {
                LOG_WARN("fail to push back inner point", K(ret));
              }
            }
            if (OB_SUCC(ret) && OB_FAIL(poly->push_back(*lr))) {
              LOG_WARN("fail to push back linearring", K(ret));
            }

            // Flip polygon rings and compare order of points with original
            // linestring, to check whether input linestring was valid
            if (OB_SUCC(ret)) {
              if (OB_FAIL(ObGeometryTypeCastUtil::check_polygon_direction(*allocator, srs, poly))) {
                LOG_WARN("fail to check correct", K(ret));
              } else {
                PL *new_poly = static_cast<PL *>(poly);
                // Check each point of the current linestring against the current polygon
                // Unequal points means ring was reversed, which means source
                // linestring had wrong direction for cast (not counter clockwise)
                for (int32_t j = 0; OB_SUCC(ret) && j < line.size(); j++) {
                  if (!ObGeometryTypeCastUtil::is_point_equal(line[j], new_poly->exterior_ring()[j])) {
                    ret = OB_ERR_INVALID_CAST_POLYGON_RING_DIRECTION;
                    LOG_WARN("invalid ring direction", K(ret));
                  }
                }
                if (OB_SUCC(ret) && OB_FAIL(res.push_back(*new_poly))) {
                  LOG_WARN("fail to push back polygon", K(ret), K(i));
                }
              }
            }
          }
        }
        break;
      }

      // MULTIPOLYGON -> MULTIPOLYGON
      case ObGeoType::MULTIPOLYGON: {
        const MPL &mpoly = *static_cast<const MPL *>(&src);
        for (int32_t i = 0; OB_SUCC(ret) && i < mpoly.size(); i++) {
          if (OB_FAIL(res.push_back(mpoly[i]))) {
            LOG_WARN("fail to push back polygon", K(ret));
          }
        }
        break;
      }

      // GEOMETRYCOLLECTION -> MULTIPOLYGON
      case ObGeoType::GEOMETRYCOLLECTION: {
        const GC &gc = *static_cast<const GC *>(&src);
        if (gc.is_empty()) {
          ret = OB_ERR_INVALID_CAST_TO_GEOMETRY;
          LOG_WARN("gc is empty", K(ret), K(gc.is_empty()));
        } else {
          typename GC::const_iterator iter = gc.begin();
          for (; OB_SUCC(ret) && iter != gc.end(); ++iter) {
            if (ObGeoType::POLYGON != (*iter)->type()) {
              ret = OB_ERR_INVALID_CAST_TO_GEOMETRY;
              LOG_WARN("invalid type", K(ret), K((*iter)->type()));
            } else {
              const PL &poly = *static_cast<const PL *>(*iter);
              if (OB_FAIL(res.push_back(poly))) {
                LOG_WARN("fail to push back polygon", K(ret));
              }
            }
          }
        }
        break;
      }

      default: {
        ret = OB_ERR_INVALID_CAST_TO_GEOMETRY;
        LOG_WARN("invalid type", K(ret), K(wkb_type));
        break;
      }
    }
  }

  return ret;
}

template<typename GC, typename P, typename MPT, typename ML, typename MPL>
int ObGeomcollectionTypeCast::cast(const ObGeometry &src,
                                   ObGeometry &dst,
                                   const ObSrsItem *srs,
                                   ObGeoErrLogInfo &log_info,
                                   ObIAllocator *allocator) const
{
  UNUSEDx(srs, log_info);
  int ret = OB_SUCCESS;
  ObGeoType wkb_type = src.type();
  GC &res = *static_cast<GC *>(&dst);

  if (OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("allocator is null", K(ret), K(wkb_type));
  } else {
    switch (wkb_type) {
      // POINT/LINESTRING/POLYGON -> GEOMETRYCOLLECTION
      case ObGeoType::POINT:
      case ObGeoType::LINESTRING:
      case ObGeoType::POLYGON: {
        if (OB_FAIL(res.push_back(src))) {
          LOG_WARN("fail to push back sub data", K(ret), K(wkb_type));
        }
        break;
      }

      // MULTIPOINT -> GEOMETRYCOLLECTION
      case ObGeoType::MULTIPOINT: {
        const MPT &mp = *static_cast<const MPT *>(&src);
        typename MPT::const_iterator iter = mp.begin();
        for (; OB_SUCC(ret) && iter != mp.end(); ++iter) {
          void *buf = NULL;
          if (OB_ISNULL(buf = allocator->alloc(sizeof(P)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc memory", K(ret), K(sizeof(P)));
          } else {
            double x = iter->template get<0>();
            double y = iter->template get<1>();
            P *point = new (buf) P (x, y, src.get_srid(), allocator);
            if (OB_FAIL(res.push_back(*point))) {
              LOG_WARN("fail to push back point", K(ret));
            }
          }
        }
        break;
      }

      // MULTILINESTRING -> GEOMETRYCOLLECTION
      case ObGeoType::MULTILINESTRING: {
        const ML &ml = *static_cast<const ML *>(&src);
        typename ML::const_iterator iter = ml.begin();
        for (; OB_SUCC(ret) && iter != ml.end(); ++iter) {
          if (OB_FAIL(res.push_back(*iter))) {
            LOG_WARN("fail to push back linestring", K(ret));
          }
        }
        break;
      }

      // MULTIPOLYGON -> GEOMETRYCOLLECTION
      case ObGeoType::MULTIPOLYGON: {
        const MPL &mpoly = *static_cast<const MPL *>(&src);
        typename MPL::const_iterator iter = mpoly.begin();
        for (; OB_SUCC(ret) && iter != mpoly.end(); ++iter) {
          if (OB_FAIL(res.push_back(*iter))) {
            LOG_WARN("fail to push back polygon", K(ret));
          }
        }
        break;
      }

      // GEOMETRYCOLLECTION -> GEOMETRYCOLLECTION
      case ObGeoType::GEOMETRYCOLLECTION: {
        const GC &gc = *static_cast<const GC *>(&src);
        typename GC::const_iterator iter = gc.begin();
        for (; OB_SUCC(ret) && iter != gc.end(); ++iter) {
          if (OB_FAIL(res.push_back(**iter))) {
            LOG_WARN("fail to push back sub data", K(ret));
          }
        }
        break;
      }

      default: {
        ret = OB_ERR_INVALID_CAST_TO_GEOMETRY;
        LOG_WARN("invalid type", K(ret), K(wkb_type));
        break;
      }
    }
  }

  return ret;
}

} // namespace common
} // namespace oceanbase
