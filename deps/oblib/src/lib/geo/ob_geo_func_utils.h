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
 * This file contains implementation for ob_geo_func_utils.
 */

#ifndef OCEANBASE_LIB_OB_GEO_FUNC_UTILS_H_
#define OCEANBASE_LIB_OB_GEO_FUNC_UTILS_H_

#include "ob_geo_func_register.h"
#include "lib/utility/ob_hang_fatal_error.h"
#include "lib/geo/ob_geo_to_tree_visitor.h"
#include "common/ob_smart_call.h"

namespace oceanbase
{
namespace common
{

class ObGeoFuncUtils
{
public:
  ObGeoFuncUtils();
  virtual ~ObGeoFuncUtils() = default;

  template<typename MultiPointType, typename MultiLineType, typename MultiPolygonType>
  static int ob_geo_gc_union(common::ObIAllocator &allocator,
                             const common::ObSrsItem &srs,
                             MultiPointType *&mps,
                             MultiLineType *&mls,
                             MultiPolygonType *&mpols);

  template<typename GcTreeType>
  static int ob_geo_gc_split(ObIAllocator &allocator,
                             const GcTreeType &gc,
                             typename GcTreeType::sub_mpt_type *&mpt,
                             typename GcTreeType::sub_ml_type *&ml,
                             typename GcTreeType::sub_mp_type *&mpo);

  template<typename GcTreeType, typename GcType>
  static int ob_gc_prepare(const common::ObGeoEvalCtx &context,
                           GcType *gc,
                           typename GcTreeType::sub_mpt_type *&multi_point,
                           typename GcTreeType::sub_ml_type *&multi_line,
                           typename GcTreeType::sub_mp_type *&multi_poly);

private:
  template<typename GcTreeType>
  static int ob_geo_gc_split_inner(const GcTreeType &gc,
                                   typename GcTreeType::sub_mpt_type &mpt,
                                   typename GcTreeType::sub_ml_type &ml,
                                   typename GcTreeType::sub_mp_type &mpo);
};

template<typename MultiPointType, typename MultiLineType, typename MultiPolygonType>
int ObGeoFuncUtils::ob_geo_gc_union(ObIAllocator &allocator,
                                    const ObSrsItem &srs,
                                    MultiPointType *&mps,
                                    MultiLineType *&mls,
                                    MultiPolygonType *&mpols)
{
  INIT_SUCC(ret);
  ObGeometry *func_result = NULL;
  if (!mps->is_tree() || !mls->is_tree() || !mpols->is_tree()) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid geometry type, must be geotree", K(ret));
  } else {
    MultiPolygonType *mpols_res = OB_NEWx(MultiPolygonType, (&allocator));
    if (OB_ISNULL(mpols_res)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      FOREACH_X(pol, *mpols, OB_SUCC(ret)) {
        if (pol->crs() == ObGeoCRS::Cartesian) {
          boost::geometry::correct(*pol);
        } else {
          boost::geometry::srs::spheroid<double> geog_sphere(srs.semi_major_axis(), srs.semi_minor_axis());
          boost::geometry::strategy::area::geographic<> area_strategy(geog_sphere);
          boost::geometry::correct(*pol, area_strategy);
        }

        ObGeoEvalCtx gis_context(&allocator, &srs);
        if (OB_FAIL(gis_context.append_geo_arg(&(*pol)))
            || OB_FAIL(gis_context.append_geo_arg(&(*mpols_res)))) {
          OB_LOG(WARN, "failed to append geo to ctx", K(ret));
        } else if (OB_FAIL(ObGeoFunc<ObGeoFuncType::Union>::geo_func::eval(gis_context, func_result))) {
          OB_LOG(WARN, "failed to do func union", K(ret));
        } else if (func_result->type() != ObGeoType::MULTIPOLYGON) {
          ret = OB_INVALID_ARGUMENT;
          OB_LOG(WARN, "result of func union might be invalid", K(ret), K(func_result->type()));
        } else if (func_result->crs() == ObGeoCRS::Geographic
                   && func_result->is_empty() && !mpols->is_empty()) {
          ret = OB_INVALID_ARGUMENT;
          OB_LOG(WARN, "result of func union might be invalid", K(ret));
        } else {
          mpols_res = static_cast<MultiPolygonType *>(func_result);
        }
      }
      if (OB_SUCC(ret)) {
        mpols = mpols_res;
      }
    }

    if (OB_SUCC(ret)) {
      ObGeoEvalCtx line_diff_context(&allocator, &srs);
      if (OB_FAIL(line_diff_context.append_geo_arg(mls))
          || OB_FAIL(line_diff_context.append_geo_arg(mpols))) {
        OB_LOG(WARN, "failed to append geo to ctx", K(ret));
      } else if (OB_FAIL(ObGeoFunc<ObGeoFuncType::Difference>::geo_func::eval(line_diff_context, func_result))) {
        OB_LOG(WARN, "failed to do func difference", K(ret));
      } else if (func_result->type() != ObGeoType::MULTILINESTRING) {
        ret = OB_INVALID_ARGUMENT;
        OB_LOG(WARN, "result of func difference might be invalid", K(ret), K(func_result->type()));
      } else {
        mls = static_cast<MultiLineType *>(func_result);
      }
    }

    for (uint8_t i = 0; i < 2 && OB_SUCC(ret); i++) {
      ObGeoEvalCtx point_diff_context(&allocator, &srs);
      ObGeometry *tmp_geo = NULL;
      if (i == 0) {
        tmp_geo = mls;
      } else {
        tmp_geo = mpols;
      }
      if (tmp_geo->is_empty()) {
        // do nothing, just return mps
      } else if (OB_FAIL(point_diff_context.append_geo_arg(mps))
          || OB_FAIL(point_diff_context.append_geo_arg(tmp_geo))) {
        OB_LOG(WARN, "failed to append geo to ctx", K(ret));
      } else if (OB_FAIL(ObGeoFunc<ObGeoFuncType::Difference>::geo_func::eval(point_diff_context, func_result))) {
        OB_LOG(WARN, "failed to do func difference", K(ret));
      } else if (func_result->type() != ObGeoType::MULTIPOINT) {
        ret = OB_INVALID_ARGUMENT;
        OB_LOG(WARN, "result of func difference might be invalid", K(ret), K(func_result->type()));
      } else {
        mps = static_cast<MultiPointType *>(func_result);
      }
    }
  }

  // If all multi-geo are empty, we've encountered at least one invalid geometry.
  if (OB_SUCC(ret) && mps->empty() && mls->empty() && mpols->empty()) {
    ret = OB_ERR_GIS_INVALID_DATA;
    OB_LOG(WARN, "there is at least one invalid geometry in collection", K(ret));
  }

  return ret;
}

// flatten geometrycollection to tree multipoint, multilinestring, multipolygon
// in this func, the geometry is create
template<typename GcTreeType>
int ObGeoFuncUtils::ob_geo_gc_split(ObIAllocator &allocator,
                                    const GcTreeType &gc,
                                    typename GcTreeType::sub_mpt_type *&mpt,
                                    typename GcTreeType::sub_ml_type *&ml,
                                    typename GcTreeType::sub_mp_type *&mpo)
{
  INIT_SUCC(ret);
  typedef typename GcTreeType::sub_mpt_type MPT;
  typedef typename GcTreeType::sub_ml_type ML;
  typedef typename GcTreeType::sub_mp_type MPO;

  if (!gc.is_tree()) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid geometry type, must be geotree", K(ret));
  } else {
    uint32_t srid = gc.get_srid();
    mpt = OB_NEWx(MPT, (&allocator), srid, allocator);
    ml = OB_NEWx(ML, (&allocator), srid, allocator);
    mpo = OB_NEWx(MPO, (&allocator), srid, allocator);
    if (OB_ISNULL(mpt) || OB_ISNULL(ml) || OB_ISNULL(mpo)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(WARN, "failed to allocate memory", K(ret));
    } else if (OB_FAIL(ob_geo_gc_split_inner(gc, *mpt, *ml, *mpo))) {
      OB_LOG(WARN, "failed to falatten geometrycollection", K(ret));
    }
  }

  return ret;
}

template<typename GcTreeType, typename GcType>
int ObGeoFuncUtils::ob_gc_prepare(const ObGeoEvalCtx &context,
                                  GcType *gc,
                                  typename GcTreeType::sub_mpt_type *&multi_point,
                                  typename GcTreeType::sub_ml_type *&multi_line,
                                  typename GcTreeType::sub_mp_type *&multi_poly)
{
  INIT_SUCC(ret);
  ObIAllocator *allocator = context.get_allocator();
  const ObSrsItem *srs = context.get_srs();
  ObGeoToTreeVisitor tree_visitor(allocator);

  if (OB_FAIL(gc->do_visit(tree_visitor))) {
    OB_LOG(WARN, "failed to transform gc to tree", K(ret));
  } else if (OB_FAIL(ObGeoFuncUtils::ob_geo_gc_split(*allocator,
              *static_cast<const GcTreeType *>(tree_visitor.get_geometry()),
              multi_point, multi_line, multi_poly))) {
    OB_LOG(WARN, "failed to do gc split", K(ret));
  } else if (OB_FAIL(ObGeoFuncUtils::ob_geo_gc_union(*allocator, *srs, multi_point,
                                                      multi_line, multi_poly))) {
    OB_LOG(WARN, "failed to do gc union", K(ret));
  } else { /* do nothing */ }
  return ret;
}


// flatten tree gc to tree multipoint, multilinestring, multipolygon
template<typename GcTreeType>
int ObGeoFuncUtils::ob_geo_gc_split_inner(const GcTreeType &gc,
                                          typename GcTreeType::sub_mpt_type &mpt,
                                          typename GcTreeType::sub_ml_type &ml,
                                          typename GcTreeType::sub_mp_type &mpo)
{
  INIT_SUCC(ret);
  typedef typename GcTreeType::sub_mpt_type MPT;
  typedef typename GcTreeType::sub_ml_type ML;
  typedef typename GcTreeType::sub_mp_type MPO;

  ObGeoCRS crs = gc.crs();
  if (crs != mpt.crs() || crs != ml.crs() || crs != mpo.crs()) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "geometries should in same crs", K(ret), K(crs));
  }

  for (uint64_t i = 0; i < gc.size() && OB_SUCC(ret); i++) {
    switch (gc[i].type()) {
      case ObGeoType::POINT: {
        if (OB_FAIL(mpt.push_back(*reinterpret_cast<typename MPT::value_type const *>(gc[i].val())))) {
          OB_LOG(WARN, "failed to add point to multipoint", K(ret));
        }
        break;
      }
      case ObGeoType::LINESTRING: {
        if (OB_FAIL(ml.push_back(*reinterpret_cast<typename ML::value_type const *>(&gc[i])))) {
          OB_LOG(WARN, "failed to add line to multiline", K(ret));
        }
        break;
      }
      case ObGeoType::POLYGON: {
        if (OB_FAIL(mpo.push_back(*reinterpret_cast<typename MPO::value_type const *>(&gc[i])))) {
          OB_LOG(WARN, "failed to add polygon to multipoly", K(ret));
        }
        break;
      }
      case ObGeoType::MULTIPOINT: {
        const MPT &tmp_mpt = *reinterpret_cast<MPT const *>(&gc[i]);
        for (uint64_t j = 0; j < tmp_mpt.size() && OB_SUCC(ret); j++) {
          if (OB_FAIL(mpt.push_back(tmp_mpt[j]))) {
            OB_LOG(WARN, "failed to add point to multipoint", K(ret));
          }
        }
        break;
      }
      case ObGeoType::MULTILINESTRING: {
        const ML &tmp_ml = *reinterpret_cast<ML const *>(&gc[i]);
        for (uint64_t j = 0; j < tmp_ml.size() && OB_SUCC(ret); j++) {
          if (OB_FAIL(ml.push_back(tmp_ml[j]))) {
            OB_LOG(WARN, "failed to add line to multiline", K(ret));
          }
        }
        break;
      }
      case ObGeoType::MULTIPOLYGON: {
        const MPO &tmp_mpo = *reinterpret_cast<MPO const *>(&gc[i]);
        for (uint64_t j = 0; j < tmp_mpo.size() && OB_SUCC(ret); j++) {
          if (OB_FAIL(mpo.push_back(tmp_mpo[j]))) {
            OB_LOG(WARN, "failed to add poly to multipoly", K(ret));
          }
        }
        break;
      }
      case ObGeoType::GEOMETRYCOLLECTION: {
        const GcTreeType &tmp_gc = *reinterpret_cast<GcTreeType const *>(&gc[i]);
        if (OB_FAIL(SMART_CALL(ob_geo_gc_split_inner(tmp_gc,mpt,ml,mpo)))) {
          OB_LOG(WARN, "failed to faltten geometrycollection", K(ret), K(i));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "unexpected geometry type", K(ret));
      }
    }
  }

  return ret;
}

} // sql
} // oceanbase
#endif // OCEANBASE_LIB_OB_GEO_FUNC_UTILS_H_