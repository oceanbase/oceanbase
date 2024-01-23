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
enum class ObBGStrategyType {
  DEFAULT_NONE = 0,
  PL_PA_STRATEGY,
  LL_LA_AA_STRATEGY,
};

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

  static int apply_bg_to_tree(const ObGeometry *g1, const ObGeoEvalCtx &context, ObGeometry *&result);
  template<typename GcTreeType>
  static int remove_duplicate_multi_geo(ObGeometry *&geo, common::ObIAllocator &allocator, const ObSrsItem *srs);
  template<typename GcTreeType>
  static int simplify_geo_collection(ObGeometry *&geo, common::ObIAllocator &allocator, const ObSrsItem *srs);
  template<typename GcType>
  static int simplify_multi_geo(ObGeometry *&geo, common::ObIAllocator &allocator);

private:
  template<typename GcTreeType>
  static int ob_geo_gc_split_inner(const GcTreeType &gc,
                                   typename GcTreeType::sub_mpt_type &mpt,
                                   typename GcTreeType::sub_ml_type &ml,
                                   typename GcTreeType::sub_mp_type &mpo);
  template<typename MpType>
  static int is_in_geometry(const ObGeometry &geo, const MpType &multi_geo, const ObSrsItem *srs, bool &res);
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
  const GcTreeType *gc_tree = nullptr;
  if (gc->is_tree()) {
    gc_tree = static_cast<const GcTreeType *>(gc);
  } else {
    ObGeoToTreeVisitor tree_visitor(allocator);
    if (OB_FAIL(gc->do_visit(tree_visitor))) {
      OB_LOG(WARN, "failed to transform gc to tree", K(ret));
    } else {
      gc_tree = static_cast<const GcTreeType *>(tree_visitor.get_geometry());
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObGeoFuncUtils::ob_geo_gc_split(*allocator,
              *gc_tree,
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

template<typename MpType>
int ObGeoFuncUtils::is_in_geometry(const ObGeometry &geo, const MpType &multi_geo, const ObSrsItem *srs, bool &res)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator temp_allocator;
  for (int32_t j = 0; j < multi_geo.size() && OB_SUCC(ret) && !res; j++) {
    ObGeoEvalCtx gis_context(&temp_allocator, srs);
    if (OB_FAIL(gis_context.append_geo_arg(&geo)) || OB_FAIL(gis_context.append_geo_arg(&multi_geo[j]))) {
      OB_LOG(WARN, "build gis context failed", K(ret), K(gis_context.get_geo_count()));
    } else if (OB_FAIL(ObGeoFunc<ObGeoFuncType::Equals>::geo_func::eval(gis_context, res))) {
      OB_LOG(WARN, "eval st intersection failed", K(ret));
    }
  }
  return ret;
}

template<typename GcType>
int ObGeoFuncUtils::simplify_multi_geo(ObGeometry *&geo, common::ObIAllocator &allocator)
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

// for geo tree
template<typename GcTreeType>
int ObGeoFuncUtils::simplify_geo_collection(ObGeometry *&geo, common::ObIAllocator &allocator, const ObSrsItem *srs)
{
  int ret = OB_SUCCESS;
  if (geo->type() != ObGeoType::GEOMETRYCOLLECTION) {
    // do nothing
  } else {
    GcTreeType *&geo_coll = reinterpret_cast<GcTreeType *&>(geo);
    ObGeoType front_type;
    bool need_simplify = true;
    if (geo_coll->size() < 2) {
      need_simplify = false;
    } else {
      front_type = geo_coll->front().type();
      for (uint32_t i = 1; need_simplify && i < geo_coll->size(); ++i) {
        if (((*geo_coll)[i]).type() != front_type) {
          need_simplify = false;
        }
      }
    }
    if (need_simplify) {
      switch(front_type) {
        case ObGeoType::POINT: {
          typename GcTreeType::sub_mpt_type *res_geo = OB_NEWx(typename GcTreeType::sub_mpt_type, &allocator, geo->get_srid(), allocator);
          if (OB_ISNULL(res_geo)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            OB_LOG(WARN, "fail to alloc memory", K(ret));
          }
          for (uint32_t i = 0; OB_SUCC(ret) && i < geo_coll->size(); ++i) {
            typename GcTreeType::sub_pt_type &geo_point = reinterpret_cast<typename GcTreeType::sub_pt_type &>((*geo_coll)[i]);
            if (OB_FAIL(res_geo->push_back(geo_point))) {
              OB_LOG(WARN, "failed to add point to multipoint", K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            geo = res_geo;
          }
          break;
        }
        case ObGeoType::LINESTRING: {
          typename GcTreeType::sub_ml_type *res_geo = OB_NEWx(typename GcTreeType::sub_ml_type, &allocator, geo->get_srid(), allocator);
          if (OB_ISNULL(res_geo)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            OB_LOG(WARN, "fail to alloc memory", K(ret));
          }
          for (uint32_t i = 0; OB_SUCC(ret) && i < geo_coll->size(); ++i) {
            if (OB_FAIL(res_geo->push_back((*geo_coll)[i]))) {
              OB_LOG(WARN, "failed to add linestring to multilinestring", K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            geo = res_geo;
          }
          break;
        }
        case ObGeoType::POLYGON: {
          typename GcTreeType::sub_mp_type *res_geo = OB_NEWx(typename GcTreeType::sub_mp_type, &allocator, geo->get_srid(), allocator);
          if (OB_ISNULL(res_geo)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            OB_LOG(WARN, "fail to alloc memory", K(ret));
          }
          for (uint32_t i = 0; OB_SUCC(ret) && i < geo_coll->size(); ++i) {
            if (OB_FAIL(res_geo->push_back((*geo_coll)[i]))) {
              OB_LOG(WARN, "failed to add polygon to multipolygon", K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            geo = res_geo;
          }
          break;
        }
        default: {
          // do nothing
          break;
        }
      }
    }
  }
  return ret;
}

// for geo tree
template<typename GcTreeType>
int ObGeoFuncUtils::remove_duplicate_multi_geo(ObGeometry *&geo, common::ObIAllocator &allocator, const ObSrsItem *srs)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator temp_allocator;
  switch (geo->type()) {
    case ObGeoType::POINT:
    case ObGeoType::LINESTRING:
    case ObGeoType::POLYGON: {
      break;
    }
    case ObGeoType::MULTIPOINT: {
      typename GcTreeType::sub_mpt_type *res_geo = OB_NEWx(typename GcTreeType::sub_mpt_type, &allocator, geo->get_srid(), allocator);
      typename GcTreeType::sub_mpt_type &sp_geo = reinterpret_cast<typename GcTreeType::sub_mpt_type &>(*geo);
      if (OB_ISNULL(res_geo)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        OB_LOG(WARN, "failt to allocate memory for geometry", K(ret), K(geo->type()));
      }
      for (int32_t i = 0; i < sp_geo.size() && OB_SUCC(ret); i++) {
        bool in_res_geo = false;
        for (int32_t j = 0; j < res_geo->size() && OB_SUCC(ret) && !in_res_geo; j++) {
          if ((sp_geo[i].template get<0>() == (*res_geo)[j].template get<0>())
              && (sp_geo[i].template get<1>() == (*res_geo)[j].template get<1>())) {
            in_res_geo = true;
          }
        }
        if (OB_SUCC(ret) && !in_res_geo) {
          typename GcTreeType::sub_pt_type pt(sp_geo[i].template get<0>(), sp_geo[i].template get<1>());
          if (OB_FAIL(res_geo->push_back(pt))) {
            OB_LOG(WARN, "fail to push back geometry", K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        geo = res_geo;
      }
      break;
    }
    case ObGeoType::MULTILINESTRING: {
      typename GcTreeType::sub_ml_type *res_geo = OB_NEWx(typename GcTreeType::sub_ml_type, &allocator, geo->get_srid(), allocator);
      typename GcTreeType::sub_ml_type &sp_geo = reinterpret_cast<typename GcTreeType::sub_ml_type &>(*geo);
      if (OB_ISNULL(res_geo)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        OB_LOG(WARN, "failt to allocate memory for geometry", K(ret), K(geo->type()));
      }
      for (int32_t i = 0; i < sp_geo.size() && OB_SUCC(ret); i++) {
        bool in_res_geo = false;
        if (OB_FAIL(is_in_geometry(sp_geo[i], *res_geo, srs, in_res_geo))) {
          OB_LOG(WARN, "fail to check is in geometry", K(ret));
        } else if (!in_res_geo && res_geo->push_back(sp_geo[i])) {
          OB_LOG(WARN, "fail to push back geometry", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        geo = res_geo;
      }
      break;
    }
    case ObGeoType::MULTIPOLYGON: {
      typename GcTreeType::sub_mp_type *res_geo = OB_NEWx(typename GcTreeType::sub_mp_type, &allocator, geo->get_srid(), allocator);
      typename GcTreeType::sub_mp_type &sp_geo = reinterpret_cast<typename GcTreeType::sub_mp_type &>(*geo);
      if (OB_ISNULL(res_geo)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        OB_LOG(WARN, "failt to allocate memory for geometry", K(ret), K(geo->type()));
      }
      for (int32_t i = 0; i < sp_geo.size() && OB_SUCC(ret); i++) {
        bool in_res_geo = false;
        if (OB_FAIL(is_in_geometry(sp_geo[i], *res_geo, srs, in_res_geo))) {
          OB_LOG(WARN, "fail to check is in geometry", K(ret));
        } else if (!in_res_geo && res_geo->push_back(sp_geo[i])) {
          OB_LOG(WARN, "fail to push back geometry", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        geo = res_geo;
      }
      break;
    }
    case ObGeoType::GEOMETRYCOLLECTION: {
      GcTreeType *res_geo = OB_NEWx(GcTreeType, &allocator, geo->get_srid(), allocator);
      GcTreeType *&sp_geo = reinterpret_cast<GcTreeType *&>(geo);
      if (OB_ISNULL(res_geo)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        OB_LOG(WARN, "failt to allocate memory for geometry", K(ret), K(geo->type()));
      }
      for (int32_t i = 0; i < sp_geo->size() && OB_SUCC(ret); i++) {
        bool in_res_geo = false;
        ObGeometry *cur_geo = &(*sp_geo)[i];
        if (OB_FAIL(remove_duplicate_multi_geo<GcTreeType>(cur_geo, allocator, srs))) {
          OB_LOG(WARN, "fail to remove dupilicate multi geometry", K(ret));
        } else if (OB_FAIL(is_in_geometry(*cur_geo, *res_geo, srs, in_res_geo))) {
          OB_LOG(WARN, "fail to check is in geometry", K(ret));
        } else if (!in_res_geo && res_geo->push_back(*cur_geo)) {
          OB_LOG(WARN, "fail to push back geometry", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        geo = res_geo;
      }
      break;
    }
    default: {
      ret = OB_NOT_SUPPORTED;
      OB_LOG(WARN, "geometry type not supported", K(ret), K(geo->type()));
      break;
    }
  }

  if (OB_SUCC(ret) && OB_FAIL((simplify_multi_geo<GcTreeType>(geo, allocator)))) {
    OB_LOG(WARN, "fail to simplify result", K(ret));
  }
  return ret;
}
} // sql
} // oceanbase
#endif // OCEANBASE_LIB_OB_GEO_FUNC_UTILS_H_