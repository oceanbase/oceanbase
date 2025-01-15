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
 * This file contains implementation for ob_geo_func_symdifference.
 */

#define USING_LOG_PREFIX LIB

#include "lib/geo/ob_geo_dispatcher.h"
#include "lib/geo/ob_geo_func_symdifference.h"
#include "lib/geo/ob_geo_tree.h"
#include "lib/geo/ob_geo_to_tree_visitor.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/geo/ob_geo_func_utils.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace common
{

namespace bg = boost::geometry;
template<typename GeoType1, typename GeoType2, typename GeometryRes>
static int get_specific_geos(const ObGeometry *g1, const ObGeometry *g2,
    const ObGeoEvalCtx &context, const GeoType1 *&geo1, const GeoType2 *&geo2, GeometryRes *&res)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(g1) || OB_ISNULL(g2)) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("wrong geometry type", K(ret));
  } else if (g1->is_tree()) {
    geo1 = reinterpret_cast<const GeoType1 *>(g1);
    geo2 = reinterpret_cast<const GeoType2 *>(g2);
  } else {
    geo1 = reinterpret_cast<const GeoType1 *>(g1->val());
    geo2 = reinterpret_cast<const GeoType2 *>(g2->val());
  }
  if (OB_SUCC(ret)) {
    res = OB_NEWx(GeometryRes, context.get_allocator(), g1->get_srid(), *context.get_allocator());
    if (OB_ISNULL(res) || OB_ISNULL(geo1) || OB_ISNULL(geo2)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("wrong geometry type or create geometry failed", K(ret), K(res), K(geo1), K(geo2));
    }
  }
  return ret;
}

template<typename GeoType1, typename GeoType2, typename GeometryRes>
static int apply_bg_symdifference_pt_pt(
    const ObGeometry *g1, const ObGeometry *g2, const ObGeoEvalCtx &context, ObGeometry *&result)
{
  INIT_SUCC(ret);
  GeometryRes *res = nullptr;
  const GeoType1 *geo1 = nullptr;
  const GeoType2 *geo2 = nullptr;
  if (OB_FAIL(get_specific_geos(g1, g2, context, geo1, geo2, res))) {
    LOG_WARN("fail to get specific geometry", K(ret));
  } else {
    ObArenaAllocator tmp_alloc;
    GeometryRes *union_res = OB_NEWx(GeometryRes, &tmp_alloc, g1->get_srid(), tmp_alloc);
    GeometryRes *intersection_res = OB_NEWx(GeometryRes, &tmp_alloc, g1->get_srid(), tmp_alloc);
    if (OB_ISNULL(union_res) || OB_ISNULL(intersection_res)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to create geometry", K(ret), K(union_res), K(intersection_res));
    } else {
      bg::union_(*geo1, *geo2, *union_res);
      bg::intersection(*geo1, *geo2, *intersection_res);
      bg::difference(*union_res, *intersection_res, *res);
      result = res;
    }
  }
  return ret;
}

template<typename PtBinType, typename GeoType, typename GeoCollType>
static int push_disjoint_point(PtBinType &geo1, GeoType &geo2, const ObGeoEvalCtx &context,
    ObBGStrategyType strategy, GeoCollType &res)
{
  int ret = OB_SUCCESS;
  bool is_disjoint = false;
  if (strategy == ObBGStrategyType::DEFAULT_NONE) {
    is_disjoint = bg::disjoint(geo1, geo2);
  } else if (OB_ISNULL(context.get_srs())) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null srs", K(ret));
  } else {
    const ObSrsItem *srs = context.get_srs();
    boost::geometry::srs::spheroid<double> geog_sphere(srs->semi_major_axis(), srs->semi_minor_axis());
    ObPlPaStrategy point_strategy(geog_sphere);
    is_disjoint = bg::disjoint(geo1, geo2, point_strategy);
  }
  if (OB_SUCC(ret) && is_disjoint) {
    ObIAllocator *allocator = context.get_allocator();
    typename GeoCollType::sub_pt_type *pt = OB_NEWx(typename GeoCollType::sub_pt_type,
        allocator,
        geo1.template get<0>(),
        geo1.template get<1>(),
        res.get_srid());
    if (OB_ISNULL(pt)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory for geometry", K(ret));
    } else if (OB_FAIL(res.push_back(*reinterpret_cast<ObGeometry *>(pt)))) {
      LOG_WARN("fail to push back geometry", K(ret));
    }
  }
  return ret;
}

template<typename GeometryRes>
static int push_not_empty_geo(ObGeometry &geo, const ObGeoEvalCtx &context, GeometryRes &res)
{
  int ret = OB_SUCCESS;
  bool is_geo_empty = false;
  if (OB_FAIL(ObGeoTypeUtil::check_empty(&geo, is_geo_empty))) {
    LOG_WARN("fail to check is geometry empty", K(ret));
  } else if (!is_geo_empty) {
    ObGeoToTreeVisitor geom_visitor(context.get_allocator());
    if (OB_FAIL(geo.do_visit(geom_visitor))) {
      LOG_WARN("failed to convert bin to tree", K(ret));
    } else if (OB_FAIL(res.push_back(*geom_visitor.get_geometry()))) {
      LOG_WARN("fail to push back geometry", K(ret));
    }
  }
  return ret;
}

template<typename PtBinType, typename GeoType, typename GeoCollType>
static int apply_bg_symdifference_pl_pa(const ObGeometry *g1, const ObGeometry *g2,
    const ObGeoEvalCtx &context, ObBGStrategyType strategy, ObGeometry *&result)
{
  INIT_SUCC(ret);
  GeoCollType *res = nullptr;
  const PtBinType *pt = nullptr;
  const GeoType *geo2 = nullptr;
  if (OB_FAIL(get_specific_geos(g1, g2, context, pt, geo2, res))) {
    LOG_WARN("fail to get specific geometry", K(ret));
  } else if (OB_FAIL(push_not_empty_geo(*const_cast<ObGeometry *>(g2), context, *res))) {
    LOG_WARN("fail to push back not empty geometry", K(ret));
  } else if (OB_FAIL(push_disjoint_point(*pt, *geo2, context, strategy, *res))) {
    LOG_WARN("fail to push disjoint point", K(ret));
  } else {
    result = res;
  }
  return ret;
}

template<typename GeoType1, typename GeoType2, typename GeometryRes>
static int apply_bg_symdifference(const ObGeometry *g1, const ObGeometry *g2,
    const ObGeoEvalCtx &context, ObGeometry *&result,
    ObBGStrategyType strategy = ObBGStrategyType::DEFAULT_NONE)
{
  INIT_SUCC(ret);
  GeometryRes *res = nullptr;
  const GeoType1 *geo1 = nullptr;
  const GeoType2 *geo2 = nullptr;
  if (OB_FAIL(get_specific_geos(g1, g2, context, geo1, geo2, res))) {
    LOG_WARN("fail to get specific geometry", K(ret));
  } else if (strategy == ObBGStrategyType::DEFAULT_NONE) {
    bg::sym_difference(*geo1, *geo2, *res);
  } else if (OB_ISNULL(context.get_srs())) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null srs", K(ret));
  } else {
    const ObSrsItem *srs = context.get_srs();
    boost::geometry::srs::spheroid<double> geog_sphere(
        srs->semi_major_axis(), srs->semi_minor_axis());
    ObLlLaAaStrategy line_strategy(geog_sphere);
    bg::sym_difference(*geo1, *geo2, *res, line_strategy);
  }
  if (OB_SUCC(ret)) {
    result = res;
  }
  return ret;
}

template<typename GeoType1, typename GeoType2, typename GeoCollType>
static int apply_bg_symdifference_la(const ObGeometry *g1, const ObGeometry *g2,
    const ObGeoEvalCtx &context, ObGeometry *&result,
    ObBGStrategyType strategy = ObBGStrategyType::DEFAULT_NONE)
{
  INIT_SUCC(ret);
  GeoCollType *res = nullptr;
  const GeoType1 *geo1 = nullptr;
  const GeoType2 *geo2 = nullptr;
  ObIAllocator *allocator = context.get_allocator();
  if (OB_FAIL(get_specific_geos(g1, g2, context, geo1, geo2, res))) {
    LOG_WARN("fail to get specific geometry", K(ret));
  } else if (OB_FAIL(push_not_empty_geo(*const_cast<ObGeometry *>(g2), context, *res))) {
    LOG_WARN("fail to push back not empty geometry", K(ret));
  }
  if (OB_SUCC(ret)) {
    typename GeoCollType::sub_ml_type *diff_res =
        OB_NEWx(typename GeoCollType::sub_ml_type, allocator, g1->get_srid(), *allocator);
    if (OB_ISNULL(diff_res)) {
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_WARN("wrong geometry type or create geometry failed", K(ret));
    } else if (strategy == ObBGStrategyType::DEFAULT_NONE) {
      bg::difference(*geo1, *geo2, *diff_res);
    } else if (OB_ISNULL(context.get_srs())) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("invalid null srs", K(ret));
    } else {
      const ObSrsItem *srs = context.get_srs();
      boost::geometry::srs::spheroid<double> geog_sphere(
          srs->semi_major_axis(), srs->semi_minor_axis());
      ObLlLaAaStrategy line_strategy(geog_sphere);
      bg::difference(*geo1, *geo2, *diff_res, line_strategy);
    }
    if (OB_SUCC(ret)) {
      typename GeoCollType::sub_ml_type::iterator iter = diff_res->begin();
      for (; OB_SUCC(ret) && iter != diff_res->end(); iter++) {
        if (OB_FAIL(res->push_back(*iter))) {
          LOG_WARN("fail to push back geometry to collection", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    result = res;
  }
  return ret;
}

template<typename MptType, typename GeoType, typename GeoCollType>
static int apply_bg_symdifference_mpl_mpa(const ObGeometry *g1, const ObGeometry *g2,
    const ObGeoEvalCtx &context, ObBGStrategyType strategy, ObGeometry *&result)
{
  INIT_SUCC(ret);
  GeoCollType *res = nullptr;
  const MptType *mpt_bin = nullptr;
  const GeoType *geo2 = nullptr;
  if (OB_FAIL(get_specific_geos(g1, g2, context, mpt_bin, geo2, res))) {
    LOG_WARN("fail to get specific geometry", K(ret));
  } else if (OB_FAIL(push_not_empty_geo(*const_cast<ObGeometry *>(g2), context, *res))) {
    LOG_WARN("fail to push back not empty geometry", K(ret));
  }
  typename MptType::const_iterator iter = mpt_bin->begin();
  for (; OB_SUCC(ret) && iter != mpt_bin->end(); iter++) {
    if (OB_FAIL(push_disjoint_point(*iter, *geo2, context, strategy, *res))) {
      LOG_WARN("fail to push disjoint point", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    result = res;
  }

  return ret;
}

template<typename GcTreeType>
static int apply_bg_symdifference_coll_common(const ObGeometry *g1, const ObGeometry *g2,
    const ObGeoEvalCtx &context, ObGeometry *&result, typename GcTreeType::sub_mpt_type *&mpt,
    typename GcTreeType::sub_ml_type *&mls, typename GcTreeType::sub_mp_type *&mpy)
{
  int ret = OB_SUCCESS;
  bool is_g2_empty = false;
  if (g1->type() == ObGeoType::GEOMETRYCOLLECTION || g2->type() != ObGeoType::GEOMETRYCOLLECTION) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("input geometry is wrong", K(ret), K(g1->type()), K(g2->type()));
  } else if (OB_FAIL(ObGeoTypeUtil::check_empty(const_cast<ObGeometry *>(g2), is_g2_empty))) {
    LOG_WARN("check geo empty failed", K(ret));
  } else if (is_g2_empty) {
    if (OB_FAIL(ObGeoFuncUtils::apply_bg_to_tree(g1, context, result))) {
      LOG_WARN("fail to apply bg to tree", K(ret));
    }
  } else {
    ObGeometry *geo2 = const_cast<ObGeometry *>(g2);
    if (OB_FAIL(ObGeoFuncUtils::ob_gc_prepare<GcTreeType>(context, geo2, mpt, mls, mpy))) {
      LOG_WARN("failed to prepare gc", K(ret));
    } else if (OB_ISNULL(mpt) || OB_ISNULL(mls) || OB_ISNULL(mpt)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("unexpected null geometry collection split", K(ret), K(mpt), K(mls), K(mpt));
    }
  }
  return ret;
}

template<typename GcTreeType>
static int simplify_and_push_geometry(
    ObIAllocator &allocator, ObGeometry *push_geo, GcTreeType &geo_coll)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL((ObGeoTypeUtil::simplify_multi_geo<GcTreeType>(push_geo, allocator)))) {
    LOG_WARN("fail to simplify result", K(ret));
  } else if (OB_FAIL(geo_coll.push_back(*push_geo))) {
    LOG_WARN("fail to push back geometry", K(ret));
  }
  return ret;
}

template<typename PtTreeType, typename GcTreeType>
static int apply_bg_symdifference_pt_coll(const ObGeometry *g1, const ObGeometry *g2,
    const ObGeoEvalCtx &context, ObBGStrategyType strategy, ObGeometry *&result)
{
  int ret = OB_SUCCESS;
  typename GcTreeType::sub_mpt_type *mpt = nullptr;
  typename GcTreeType::sub_ml_type *mls = nullptr;
  typename GcTreeType::sub_mp_type *mpy = nullptr;
  if (OB_FAIL(
          apply_bg_symdifference_coll_common<GcTreeType>(g1, g2, context, result, mpt, mls, mpy))) {
    LOG_WARN("fail to apply symdifference collection common", K(ret));
  } else if (OB_ISNULL(result)) {
    ObIAllocator *allocator = context.get_allocator();
    ObGeometry *g1_tree = nullptr;
    ObGeoToTreeVisitor tree_visitor(allocator);
    ObGeometry *mpt_res = nullptr;
    if (OB_FAIL(const_cast<ObGeometry *>(g1)->do_visit(tree_visitor))) {
      LOG_WARN("fail to convert geometry to tree", K(ret));
    } else if (FALSE_IT(g1_tree = tree_visitor.get_geometry())) {
    } else if (OB_FAIL((apply_bg_symdifference_pt_pt<PtTreeType,
                   typename GcTreeType::sub_mpt_type,
                   typename GcTreeType::sub_mpt_type>(g1_tree, mpt, context, mpt_res)))) {
      LOG_WARN("fail to do symdifference between pointlike and pointlike type", K(ret));
    } else {
      uint32_t srid = g1->get_srid();
      ObArenaAllocator tmp_alloc;
      typename GcTreeType::sub_mpt_type *mls_res =
          OB_NEWx(typename GcTreeType::sub_mpt_type, &tmp_alloc, srid, tmp_alloc);
      typename GcTreeType::sub_mpt_type *mpy_res =
          OB_NEWx(typename GcTreeType::sub_mpt_type, allocator, srid, *allocator);
      GcTreeType *res = OB_NEWx(GcTreeType, allocator, srid, *allocator);
      if (OB_ISNULL(mls_res) || OB_ISNULL(mpy_res) || OB_ISNULL(res)) {
        ret = OB_ERR_GIS_INVALID_DATA;
        LOG_WARN("wrong geometry type or create geometry failed",
            K(ret),
            K(mls_res),
            K(mpy_res),
            K(res));
      } else if (strategy == ObBGStrategyType::DEFAULT_NONE) {
        bg::difference(
            *reinterpret_cast<typename GcTreeType::sub_mpt_type *>(mpt_res), *mls, *mls_res);
        bg::difference(*mls_res, *mpy, *mpy_res);
      } else if (OB_ISNULL(context.get_srs())) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("invalid null srs", K(ret));
      } else {
        const ObSrsItem *srs = context.get_srs();
        boost::geometry::srs::spheroid<double> geog_sphere(srs->semi_major_axis(), srs->semi_minor_axis());
        ObPlPaStrategy point_strategy(geog_sphere);
        ObLlLaAaStrategy line_strategy(geog_sphere);
        bg::difference(
            *reinterpret_cast<typename GcTreeType::sub_mpt_type *>(mpt_res), *mls, *mls_res, point_strategy);
        bg::difference(*mls_res, *mpy, *mpy_res, point_strategy);
      }
      if (OB_SUCC(ret) && !mpy_res->is_empty()
          && OB_FAIL(simplify_and_push_geometry(
                 *allocator, reinterpret_cast<ObGeometry *>(mpy_res), *res))) {
        LOG_WARN("fail to push back geometry", K(ret));
      }
      if (OB_SUCC(ret) && !mls->is_empty()
          && OB_FAIL(simplify_and_push_geometry(
                 *allocator, reinterpret_cast<ObGeometry *>(mls), *res))) {
        LOG_WARN("fail to push back geometry", K(ret));
      }
      if (OB_SUCC(ret) && !mpy->is_empty()
          && OB_FAIL(simplify_and_push_geometry(
                 *allocator, reinterpret_cast<ObGeometry *>(mpy), *res))) {
        LOG_WARN("fail to push back geometry", K(ret));
      }
      if (OB_SUCC(ret)) {
        result = res;
      }
    }
  }
  return ret;
}

template<typename LineTreeType, typename GcTreeType>
static int apply_bg_symdifference_line_coll(const ObGeometry *g1, const ObGeometry *g2,
    const ObGeoEvalCtx &context, ObBGStrategyType strategy, ObGeometry *&result)
{
  int ret = OB_SUCCESS;
  typename GcTreeType::sub_mpt_type *mpt = nullptr;
  typename GcTreeType::sub_ml_type *mls = nullptr;
  typename GcTreeType::sub_mp_type *mpy = nullptr;
  if (OB_FAIL(
          apply_bg_symdifference_coll_common<GcTreeType>(g1, g2, context, result, mpt, mls, mpy))) {
    LOG_WARN("fail to apply symdifference collection common", K(ret));
  } else if (OB_ISNULL(result)) {
    ObIAllocator *allocator = context.get_allocator();
    ObGeometry *g1_tree = nullptr;
    ObGeoToTreeVisitor tree_visitor(allocator);
    if (OB_FAIL(const_cast<ObGeometry *>(g1)->do_visit(tree_visitor))) {
      LOG_WARN("fail to convert geometry to tree", K(ret));
    } else if (FALSE_IT(g1_tree = tree_visitor.get_geometry())) {
    } else {
      ObArenaAllocator tmp_alloc;
      uint32_t srid = g1->get_srid();
      typename GcTreeType::sub_ml_type *mls_res =
          OB_NEWx(typename GcTreeType::sub_ml_type, &tmp_alloc, srid, tmp_alloc);
      GcTreeType *res = OB_NEWx(GcTreeType, allocator, srid, *allocator);
      typename GcTreeType::sub_ml_type *mls_diff_res =
          OB_NEWx(typename GcTreeType::sub_ml_type, allocator, srid, *allocator);
      typename GcTreeType::sub_mpt_type *mpt_res =
          OB_NEWx(typename GcTreeType::sub_mpt_type, allocator, srid, *allocator);
      if (OB_ISNULL(mpt_res) || OB_ISNULL(mls_res) || OB_ISNULL(res) || OB_ISNULL(mls_diff_res)) {
        ret = OB_ERR_GIS_INVALID_DATA;
        LOG_WARN("wrong geometry type or create geometry failed",
            K(ret),
            K(mpt_res),
            K(mls_res),
            K(res),
            K(mls_diff_res));
      } else if (strategy == ObBGStrategyType::DEFAULT_NONE) {
        bg::difference(*reinterpret_cast<LineTreeType *>(g1_tree), *mpy, *mls_res);
        bg::sym_difference(*mls, *mls_res, *mls_diff_res);
        bg::difference(*mpt, *reinterpret_cast<LineTreeType *>(g1_tree), *mpt_res);
      } else if (OB_ISNULL(context.get_srs())) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("invalid null srs", K(ret));
      } else {
        const ObSrsItem *srs = context.get_srs();
        boost::geometry::srs::spheroid<double> geog_sphere(
            srs->semi_major_axis(), srs->semi_minor_axis());
        ObLlLaAaStrategy line_strategy(geog_sphere);
        bg::difference(*reinterpret_cast<LineTreeType *>(g1_tree), *mpy, *mls_res, line_strategy);
        bg::sym_difference(*mls, *mls_res, *mls_diff_res, line_strategy);
        ObPlPaStrategy point_strategy(geog_sphere);
        bg::difference(*mpt, *reinterpret_cast<LineTreeType *>(g1_tree), *mpt_res, point_strategy);
      }
      if (OB_SUCC(ret) && !mpy->is_empty()
          && OB_FAIL(simplify_and_push_geometry(
                 *allocator, reinterpret_cast<ObGeometry *>(mpy), *res))) {
        LOG_WARN("fail to simplify and push geometry", K(ret));
      }
      if (OB_SUCC(ret) && !mls_diff_res->is_empty()
          && OB_FAIL(simplify_and_push_geometry(
                 *allocator, reinterpret_cast<ObGeometry *>(mls_diff_res), *res))) {
        LOG_WARN("fail to simplify and push geometry", K(ret));
      }
      if (OB_SUCC(ret) && !mpt_res->is_empty()
          && OB_FAIL(simplify_and_push_geometry(
                 *allocator, reinterpret_cast<ObGeometry *>(mpt_res), *res))) {
        LOG_WARN("fail to simplify and push geometry", K(ret));
      }
      if (OB_SUCC(ret)) {
        result = res;
      }
    }
  }
  return ret;
}

template<typename PolyTreeType, typename GcTreeType, typename GeomType>
static int apply_bg_symdifference_poly_coll(const ObGeometry *g1, const ObGeometry *g2,
    const ObGeoEvalCtx &context, ObBGStrategyType strategy, ObGeometry *&result)
{
  int ret = OB_SUCCESS;
  typename GcTreeType::sub_mpt_type *mpt = nullptr;
  typename GcTreeType::sub_ml_type *mls = nullptr;
  typename GcTreeType::sub_mp_type *mpy = nullptr;
  if (OB_FAIL(
          apply_bg_symdifference_coll_common<GcTreeType>(g1, g2, context, result, mpt, mls, mpy))) {
    LOG_WARN("fail to apply symdifference collection common", K(ret));
  } else if (OB_ISNULL(result)) {
    ObIAllocator *allocator = context.get_allocator();
    const GeomType *g1_bin = reinterpret_cast<const GeomType *>(g1->val());
    uint32_t srid = g1->get_srid();
    typename GcTreeType::sub_ml_type *mls_res =
        OB_NEWx(typename GcTreeType::sub_ml_type, allocator, srid, *allocator);
    GcTreeType *res = OB_NEWx(GcTreeType, allocator, srid, *allocator);
    typename GcTreeType::sub_mp_type *mpy_res =
        OB_NEWx(typename GcTreeType::sub_mp_type, allocator, srid, *allocator);
    typename GcTreeType::sub_mpt_type *mpt_res =
        OB_NEWx(typename GcTreeType::sub_mpt_type, allocator, srid, *allocator);
    if (OB_ISNULL(mpt_res) || OB_ISNULL(mls_res) || OB_ISNULL(res) || OB_ISNULL(mpy_res)) {
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_WARN("wrong geometry type or create geometry failed",
          K(ret),
          K(mpt_res),
          K(mls_res),
          K(res),
          K(mpy_res));
    } else if (strategy == ObBGStrategyType::DEFAULT_NONE) {
      bg::sym_difference(*g1_bin, *mpy, *mpy_res);
      bg::difference(*mls, *g1_bin, *mls_res);
      bg::difference(*mpt, *g1_bin, *mpt_res);
    } else if (OB_ISNULL(context.get_srs())) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("invalid null srs", K(ret));
    } else {
      const ObSrsItem *srs = context.get_srs();
      boost::geometry::srs::spheroid<double> geog_sphere(
          srs->semi_major_axis(), srs->semi_minor_axis());
      ObLlLaAaStrategy line_strategy(geog_sphere);
      ObPlPaStrategy point_strategy(geog_sphere);
      bg::sym_difference(
          *g1_bin, *mpy, *mpy_res, line_strategy);
      bg::difference(*mls, *g1_bin, *mls_res, line_strategy);
      bg::difference(*mpt, *g1_bin, *mpt_res, point_strategy);
    }
    if (OB_SUCC(ret) && !mpy_res->is_empty()
        && OB_FAIL(simplify_and_push_geometry(
                *allocator, reinterpret_cast<ObGeometry *>(mpy_res), *res))) {
      LOG_WARN("fail to push back geometry", K(ret));
    }
    if (OB_SUCC(ret) && !mls_res->is_empty()
        && OB_FAIL(simplify_and_push_geometry(
                *allocator, reinterpret_cast<ObGeometry *>(mls_res), *res))) {
      LOG_WARN("fail to push back geometry", K(ret));
    }
    if (OB_SUCC(ret) && !mpt_res->is_empty()
        && OB_FAIL(simplify_and_push_geometry(
                *allocator, reinterpret_cast<ObGeometry *>(mpt_res), *res))) {
      LOG_WARN("fail to push back geometry", K(ret));
    }
    if (OB_SUCC(ret)) {
      result = res;
    }
  }
  return ret;
}

class ObGeoFuncSymDifferenceImpl : public ObIGeoDispatcher<ObGeometry *, ObGeoFuncSymDifferenceImpl>
{
public:
  ObGeoFuncSymDifferenceImpl();
  virtual ~ObGeoFuncSymDifferenceImpl() = default;
  // template for unary
  OB_GEO_UNARY_FUNC_DEFAULT(ObGeometry *, OB_ERR_GIS_INVALID_DATA);
  OB_GEO_TREE_UNARY_FUNC_DEFAULT(ObGeometry *, OB_ERR_GIS_INVALID_DATA);
  OB_GEO_CART_TREE_FUNC_DEFAULT(ObGeometry *, OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS);
  OB_GEO_GEOG_TREE_FUNC_DEFAULT(ObGeometry *, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);

  template<typename GeometyType1, typename GeometyType2>
  struct EvalWkbBi
  {
    static int eval(const ObGeometry *g1, const ObGeometry *g2, const ObGeoEvalCtx &context,
        ObGeometry *&result)
    {
      UNUSEDx(g1, g2, context, result);
      return OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS;
    }
  };

  template<typename GeometyType1, typename GeometyType2>
  struct EvalWkbBiGeog
  {
    static int eval(const ObGeometry *g1, const ObGeometry *g2, const ObGeoEvalCtx &context,
        ObGeometry *&result)
    {
      UNUSEDx(g1, g2, context, result);
      return OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS;
    }
  };

private:
  // assume that g1 g2 both collection
  template<typename GcTreeType>
  static int eval_symdifference_gc_gc(
      const ObGeometry *g1, const ObGeometry *g2, const ObGeoEvalCtx &context, ObGeometry *&result)
  {
    int ret = OB_SUCCESS;
    ObIAllocator *allocator = context.get_allocator();
    result = nullptr;
    bool is_g1_empty = false;
    bool is_g2_empty = false;
    if (g1->type() != ObGeoType::GEOMETRYCOLLECTION
        || g2->type() != ObGeoType::GEOMETRYCOLLECTION) {
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_WARN("input geometry should be GEOMETRYCOLLECTION", K(ret), K(g1->type()), K(g2->type()));
    } else if (OB_FAIL(ObGeoTypeUtil::check_empty(const_cast<ObGeometry *>(g1), is_g1_empty))) {
      LOG_WARN("check geo empty failed", K(ret));
    } else if (OB_FAIL(ObGeoTypeUtil::check_empty(const_cast<ObGeometry *>(g2), is_g2_empty))) {
      LOG_WARN("check geo empty failed", K(ret));
    } else if (is_g1_empty) {
      if (is_g2_empty) {
        GcTreeType *res_coll = OB_NEWx(GcTreeType, allocator, g1->get_srid(), *allocator);
        if (OB_ISNULL(res_coll)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate memory for gometry", K(ret));
        } else {
          result = res_coll;
        }
      } else if (OB_FAIL(ObGeoFuncUtils::apply_bg_to_tree(g2, context, result))) {
        LOG_WARN("fail to apply bg to tree", K(ret));
      }
    } else {
      typename GcTreeType::sub_mpt_type *mpt1 = nullptr;
      typename GcTreeType::sub_ml_type *mls1 = nullptr;
      typename GcTreeType::sub_mp_type *mpy1 = nullptr;
      ObGeometry *geo1 = const_cast<ObGeometry *>(g1);
      ObGeometry *mpy_bin = nullptr;
      ObGeometry *mls_bin = nullptr;
      ObGeometry *mpt_bin = nullptr;
      ObGeometry *mpy_res = nullptr;
      ObGeometry *mpy_res_bin = nullptr;
      ObGeometry *mls_res = nullptr;
      ObGeometry *mls_res_bin = nullptr;
      const ObSrsItem *srs = context.get_srs();
      if (OB_FAIL(ObGeoFuncUtils::ob_gc_prepare<GcTreeType>(context, geo1, mpt1, mls1, mpy1))) {
        LOG_WARN("failed to prepare gc", K(ret));
      } else if (OB_ISNULL(mpt1) || OB_ISNULL(mls1) || OB_ISNULL(mpy1)) {
        ret = OB_ERR_GIS_INVALID_DATA;
        LOG_WARN("unexpected null geometry collection split", K(ret));
      } else if (OB_FAIL(ObGeoTypeUtil::tree_to_bin(*allocator, mpy1, mpy_bin, srs))) {
        LOG_WARN("fail to convert geometry tree to bin", K(ret));
      } else if (OB_FAIL(ObGeoTypeUtil::tree_to_bin(*allocator, mls1, mls_bin, srs))) {
        LOG_WARN("fail to convert geometry tree to bin", K(ret));
      } else if (OB_FAIL(ObGeoTypeUtil::tree_to_bin(*allocator, mpt1, mpt_bin, srs))) {
        LOG_WARN("fail to convert geometry tree to bin", K(ret));
      } else if (OB_FAIL(eval_wkb_binary(mpy_bin, g2, context, mpy_res))) {
        LOG_WARN("fail to eval wkb binary", K(ret));
      } else if (OB_FAIL(!mpy_res->is_empty() && (ObGeoTypeUtil::simplify_multi_geo<GcTreeType>(mpy_res, *allocator)))) {
        LOG_WARN("fail to simplify result", K(ret));
      } else if (OB_FAIL(ObGeoTypeUtil::tree_to_bin(*allocator, mpy_res, mpy_res_bin, srs))) {
        LOG_WARN("fail to convert geometry tree to bin", K(ret));
      } else if (OB_FAIL(eval_wkb_binary(mls_bin, mpy_res_bin, context, mls_res))) {
        LOG_WARN("fail to eval wkb binary", K(ret));
      } else if (OB_FAIL(!mls_res->is_empty() && (ObGeoTypeUtil::simplify_multi_geo<GcTreeType>(mls_res, *allocator)))) {
        LOG_WARN("fail to simplify result", K(ret));
      } else if (OB_FAIL(ObGeoTypeUtil::tree_to_bin(*allocator, mls_res, mls_res_bin, srs))) {
        LOG_WARN("fail to convert geometry tree to bin", K(ret));
      } else if (OB_FAIL(eval_wkb_binary(mpt_bin, mls_res_bin, context, result))) {
        LOG_WARN("fail to eval wkb binary", K(ret));
      } else if (OB_FAIL(!result->is_empty() && (ObGeoTypeUtil::simplify_multi_geo<GcTreeType>(result, *allocator)))) {
        LOG_WARN("fail to simplify result", K(ret));
      }
    }
    return ret;
  }
};

}  // namespace common
}  // namespace oceanbase