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
    ObIAllocator *allocator = context.get_allocator();
    GeometryRes *union_res = OB_NEWx(GeometryRes, allocator, g1->get_srid(), *allocator);
    GeometryRes *intersection_res = OB_NEWx(GeometryRes, allocator, g1->get_srid(), *allocator);
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
        0,
        allocator);
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
  if (OB_FAIL((ObGeoFuncUtils::simplify_multi_geo<GcTreeType>(push_geo, allocator)))) {
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
      typename GcTreeType::sub_mpt_type *mls_res =
          OB_NEWx(typename GcTreeType::sub_mpt_type, allocator, srid, *allocator);
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
      uint32_t srid = g1->get_srid();
      typename GcTreeType::sub_ml_type *mls_res =
          OB_NEWx(typename GcTreeType::sub_ml_type, allocator, srid, *allocator);
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

template<typename PolyTreeType, typename GcTreeType>
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
    ObGeometry *g1_tree = nullptr;
    ObGeoToTreeVisitor tree_visitor(allocator);
    if (OB_FAIL(const_cast<ObGeometry *>(g1)->do_visit(tree_visitor))) {
      LOG_WARN("fail to convert geometry to tree", K(ret));
    } else if (FALSE_IT(g1_tree = tree_visitor.get_geometry())) {
    } else {
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
        bg::sym_difference(*reinterpret_cast<PolyTreeType *>(g1_tree), *mpy, *mpy_res);
        bg::difference(*mls, *reinterpret_cast<PolyTreeType *>(g1_tree), *mls_res);
        bg::difference(*mpt, *reinterpret_cast<PolyTreeType *>(g1_tree), *mpt_res);
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
            *reinterpret_cast<PolyTreeType *>(g1_tree), *mpy, *mpy_res, line_strategy);
        bg::difference(*mls, *reinterpret_cast<PolyTreeType *>(g1_tree), *mls_res, line_strategy);
        bg::difference(*mpt, *reinterpret_cast<PolyTreeType *>(g1_tree), *mpt_res, point_strategy);
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
      } else if (OB_FAIL(!mpy_res->is_empty() && (ObGeoFuncUtils::simplify_multi_geo<GcTreeType>(mpy_res, *allocator)))) {
        LOG_WARN("fail to simplify result", K(ret));
      } else if (OB_FAIL(ObGeoTypeUtil::tree_to_bin(*allocator, mpy_res, mpy_res_bin, srs))) {
        LOG_WARN("fail to convert geometry tree to bin", K(ret));
      } else if (OB_FAIL(eval_wkb_binary(mls_bin, mpy_res_bin, context, mls_res))) {
        LOG_WARN("fail to eval wkb binary", K(ret));
      } else if (OB_FAIL(!mls_res->is_empty() && (ObGeoFuncUtils::simplify_multi_geo<GcTreeType>(mls_res, *allocator)))) {
        LOG_WARN("fail to simplify result", K(ret));
      } else if (OB_FAIL(ObGeoTypeUtil::tree_to_bin(*allocator, mls_res, mls_res_bin, srs))) {
        LOG_WARN("fail to convert geometry tree to bin", K(ret));
      } else if (OB_FAIL(eval_wkb_binary(mpt_bin, mls_res_bin, context, result))) {
        LOG_WARN("fail to eval wkb binary", K(ret));
      } else if (OB_FAIL(!result->is_empty() && (ObGeoFuncUtils::simplify_multi_geo<GcTreeType>(result, *allocator)))) {
        LOG_WARN("fail to simplify result", K(ret));
      }
    }
    return ret;
  }
};

// cartesian point
OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomPoint, ObWkbGeomPoint, ObGeometry *)
{
  return apply_bg_symdifference_pt_pt<ObWkbGeomPoint, ObWkbGeomPoint, ObCartesianMultipoint>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomPoint, ObWkbGeomLineString, ObGeometry *)
{
  return apply_bg_symdifference_pl_pa<ObWkbGeomPoint,
      ObWkbGeomLineString,
      ObCartesianGeometrycollection>(g1, g2, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomPoint, ObWkbGeomPolygon, ObGeometry *)
{
  return apply_bg_symdifference_pl_pa<ObWkbGeomPoint,
      ObWkbGeomPolygon,
      ObCartesianGeometrycollection>(g1, g2, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomPoint, ObWkbGeomMultiPoint, ObGeometry *)
{
  return apply_bg_symdifference_pt_pt<ObWkbGeomPoint, ObWkbGeomMultiPoint, ObCartesianMultipoint>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomPoint, ObWkbGeomMultiLineString, ObGeometry *)
{
  return apply_bg_symdifference_pl_pa<ObWkbGeomPoint,
      ObWkbGeomMultiLineString,
      ObCartesianGeometrycollection>(g1, g2, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomPoint, ObWkbGeomMultiPolygon, ObGeometry *)
{
  return apply_bg_symdifference_pl_pa<ObWkbGeomPoint,
      ObWkbGeomMultiPolygon,
      ObCartesianGeometrycollection>(g1, g2, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomPoint, ObWkbGeomCollection, ObGeometry *)
{
  return apply_bg_symdifference_pt_coll<ObCartesianPoint, ObCartesianGeometrycollection>(
      g1, g2, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

// cartisian linestring
OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomLineString, ObWkbGeomPoint, ObGeometry *)
{
  return apply_bg_symdifference_pl_pa<ObWkbGeomPoint,
      ObWkbGeomLineString,
      ObCartesianGeometrycollection>(g2, g1, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomLineString, ObWkbGeomLineString, ObGeometry *)
{
  return apply_bg_symdifference<ObWkbGeomLineString,
      ObWkbGeomLineString,
      ObCartesianMultilinestring>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomLineString, ObWkbGeomPolygon, ObGeometry *)
{
  return apply_bg_symdifference_la<ObWkbGeomLineString,
      ObWkbGeomPolygon,
      ObCartesianGeometrycollection>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomLineString, ObWkbGeomMultiPoint, ObGeometry *)
{
  return apply_bg_symdifference_mpl_mpa<ObWkbGeomMultiPoint,
      ObWkbGeomLineString,
      ObCartesianGeometrycollection>(g2, g1, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomLineString, ObWkbGeomMultiLineString, ObGeometry *)
{
  return apply_bg_symdifference<ObWkbGeomLineString,
      ObWkbGeomMultiLineString,
      ObCartesianMultilinestring>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomLineString, ObWkbGeomMultiPolygon, ObGeometry *)
{
  return apply_bg_symdifference_la<ObWkbGeomLineString,
      ObWkbGeomMultiPolygon,
      ObCartesianGeometrycollection>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomLineString, ObWkbGeomCollection, ObGeometry *)
{
  return apply_bg_symdifference_line_coll<ObCartesianLineString, ObCartesianGeometrycollection>(
      g1, g2, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

// cartisian polygon
OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomPolygon, ObWkbGeomPoint, ObGeometry *)
{
  return apply_bg_symdifference_pl_pa<ObWkbGeomPoint,
      ObWkbGeomPolygon,
      ObCartesianGeometrycollection>(g2, g1, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomPolygon, ObWkbGeomLineString, ObGeometry *)
{
  return apply_bg_symdifference_la<ObWkbGeomLineString,
      ObWkbGeomPolygon,
      ObCartesianGeometrycollection>(g2, g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomPolygon, ObWkbGeomPolygon, ObGeometry *)
{
  return apply_bg_symdifference<ObWkbGeomPolygon, ObWkbGeomPolygon, ObCartesianMultipolygon>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomPolygon, ObWkbGeomMultiPoint, ObGeometry *)
{
  return apply_bg_symdifference_mpl_mpa<ObWkbGeomMultiPoint,
      ObWkbGeomPolygon,
      ObCartesianGeometrycollection>(g2, g1, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomPolygon, ObWkbGeomMultiLineString, ObGeometry *)
{
  return apply_bg_symdifference_la<ObWkbGeomMultiLineString,
      ObWkbGeomPolygon,
      ObCartesianGeometrycollection>(g2, g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomPolygon, ObWkbGeomMultiPolygon, ObGeometry *)
{
  return apply_bg_symdifference<ObWkbGeomPolygon, ObWkbGeomMultiPolygon, ObCartesianMultipolygon>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomPolygon, ObWkbGeomCollection, ObGeometry *)
{
  return apply_bg_symdifference_poly_coll<ObCartesianPolygon, ObCartesianGeometrycollection>(
      g1, g2, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

// cartisian multipoint
OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomMultiPoint, ObWkbGeomPoint, ObGeometry *)
{
  return apply_bg_symdifference_pt_pt<ObWkbGeomPoint, ObWkbGeomMultiPoint, ObCartesianMultipoint>(
      g2, g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomMultiPoint, ObWkbGeomLineString, ObGeometry *)
{
  return apply_bg_symdifference_mpl_mpa<ObWkbGeomMultiPoint,
      ObWkbGeomLineString,
      ObCartesianGeometrycollection>(g1, g2, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomMultiPoint, ObWkbGeomPolygon, ObGeometry *)
{
  return apply_bg_symdifference_mpl_mpa<ObWkbGeomMultiPoint,
      ObWkbGeomPolygon,
      ObCartesianGeometrycollection>(g1, g2, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomMultiPoint, ObWkbGeomMultiPoint, ObGeometry *)
{
  return apply_bg_symdifference_pt_pt<ObWkbGeomMultiPoint,
      ObWkbGeomMultiPoint,
      ObCartesianMultipoint>(g2, g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomMultiPoint, ObWkbGeomMultiLineString, ObGeometry *)
{
  return apply_bg_symdifference_mpl_mpa<ObWkbGeomMultiPoint,
      ObWkbGeomMultiLineString,
      ObCartesianGeometrycollection>(g1, g2, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomMultiPoint, ObWkbGeomMultiPolygon, ObGeometry *)
{
  return apply_bg_symdifference_mpl_mpa<ObWkbGeomMultiPoint,
      ObWkbGeomMultiPolygon,
      ObCartesianGeometrycollection>(g1, g2, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomMultiPoint, ObWkbGeomCollection, ObGeometry *)
{
  return apply_bg_symdifference_pt_coll<ObCartesianMultipoint, ObCartesianGeometrycollection>(
      g1, g2, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

// cartisian mutilinestring
OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomMultiLineString, ObWkbGeomPoint, ObGeometry *)
{
  return apply_bg_symdifference_pl_pa<ObWkbGeomPoint,
      ObWkbGeomMultiLineString,
      ObCartesianGeometrycollection>(g2, g1, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomMultiLineString, ObWkbGeomLineString, ObGeometry *)
{
  return apply_bg_symdifference<ObWkbGeomLineString,
      ObWkbGeomMultiLineString,
      ObCartesianMultilinestring>(g2, g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomMultiLineString, ObWkbGeomPolygon, ObGeometry *)
{
  return apply_bg_symdifference_la<ObWkbGeomMultiLineString,
      ObWkbGeomPolygon,
      ObCartesianGeometrycollection>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomMultiLineString, ObWkbGeomMultiPoint, ObGeometry *)
{
  return apply_bg_symdifference_mpl_mpa<ObWkbGeomMultiPoint,
      ObWkbGeomMultiLineString,
      ObCartesianGeometrycollection>(g2, g1, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomMultiLineString, ObWkbGeomMultiLineString, ObGeometry *)
{
  return apply_bg_symdifference<ObWkbGeomMultiLineString,
      ObWkbGeomMultiLineString,
      ObCartesianMultilinestring>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomMultiLineString, ObWkbGeomMultiPolygon, ObGeometry *)
{
  return apply_bg_symdifference_la<ObWkbGeomMultiLineString,
      ObWkbGeomMultiPolygon,
      ObCartesianGeometrycollection>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomMultiLineString, ObWkbGeomCollection, ObGeometry *)
{
  return apply_bg_symdifference_line_coll<ObCartesianMultilinestring,
      ObCartesianGeometrycollection>(g1, g2, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

// cartisian multipolygon
OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomMultiPolygon, ObWkbGeomPoint, ObGeometry *)
{
  return apply_bg_symdifference_pl_pa<ObWkbGeomPoint,
      ObWkbGeomMultiPolygon,
      ObCartesianGeometrycollection>(g2, g1, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomMultiPolygon, ObWkbGeomLineString, ObGeometry *)
{
  return apply_bg_symdifference_la<ObWkbGeomLineString,
      ObWkbGeomMultiPolygon,
      ObCartesianGeometrycollection>(g2, g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomMultiPolygon, ObWkbGeomPolygon, ObGeometry *)
{
  return apply_bg_symdifference<ObWkbGeomPolygon, ObWkbGeomMultiPolygon, ObCartesianMultipolygon>(
      g2, g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomMultiPolygon, ObWkbGeomMultiPoint, ObGeometry *)
{
  return apply_bg_symdifference_mpl_mpa<ObWkbGeomMultiPoint,
      ObWkbGeomMultiPolygon,
      ObCartesianGeometrycollection>(g2, g1, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomMultiPolygon, ObWkbGeomMultiLineString, ObGeometry *)
{
  return apply_bg_symdifference_la<ObWkbGeomMultiLineString,
      ObWkbGeomMultiPolygon,
      ObCartesianGeometrycollection>(g2, g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomMultiPolygon, ObWkbGeomMultiPolygon, ObGeometry *)
{
  return apply_bg_symdifference<ObWkbGeomMultiPolygon,
      ObWkbGeomMultiPolygon,
      ObCartesianMultipolygon>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomMultiPolygon, ObWkbGeomCollection, ObGeometry *)
{
  return apply_bg_symdifference_poly_coll<ObCartesianMultipolygon, ObCartesianGeometrycollection>(
      g1, g2, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

// cartesian collection
OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomCollection, ObWkbGeomPoint, ObGeometry *)
{
  return apply_bg_symdifference_pt_coll<ObCartesianPoint, ObCartesianGeometrycollection>(
      g2, g1, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomCollection, ObWkbGeomLineString, ObGeometry *)
{
  return apply_bg_symdifference_line_coll<ObCartesianLineString, ObCartesianGeometrycollection>(
      g2, g1, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomCollection, ObWkbGeomPolygon, ObGeometry *)
{
  return apply_bg_symdifference_poly_coll<ObCartesianPolygon, ObCartesianGeometrycollection>(
      g2, g1, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomCollection, ObWkbGeomMultiPoint, ObGeometry *)
{
  return apply_bg_symdifference_pt_coll<ObCartesianMultipoint, ObCartesianGeometrycollection>(
      g2, g1, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomCollection, ObWkbGeomMultiLineString, ObGeometry *)
{
  return apply_bg_symdifference_line_coll<ObCartesianMultilinestring,
      ObCartesianGeometrycollection>(g2, g1, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomCollection, ObWkbGeomMultiPolygon, ObGeometry *)
{
  return apply_bg_symdifference_poly_coll<ObCartesianMultipolygon, ObCartesianGeometrycollection>(
      g2, g1, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeomCollection, ObWkbGeomCollection, ObGeometry *)
{
  return eval_symdifference_gc_gc<ObCartesianGeometrycollection>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

//----------------geographic-------------------//
// geograph point
OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogPoint, ObWkbGeogPoint, ObGeometry *)
{
  return apply_bg_symdifference_pt_pt<ObWkbGeogPoint, ObWkbGeogPoint, ObGeographMultipoint>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogPoint, ObWkbGeogLineString, ObGeometry *)
{
  return apply_bg_symdifference_pl_pa<ObWkbGeogPoint,
      ObWkbGeogLineString,
      ObGeographGeometrycollection>(g1, g2, context, ObBGStrategyType::PL_PA_STRATEGY, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogPoint, ObWkbGeogPolygon, ObGeometry *)
{
  return apply_bg_symdifference_pl_pa<ObWkbGeogPoint,
      ObWkbGeogPolygon,
      ObGeographGeometrycollection>(g1, g2, context, ObBGStrategyType::PL_PA_STRATEGY, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogPoint, ObWkbGeogMultiPoint, ObGeometry *)
{
  return apply_bg_symdifference_pt_pt<ObWkbGeogPoint, ObWkbGeogMultiPoint, ObGeographMultipoint>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogPoint, ObWkbGeogMultiLineString, ObGeometry *)
{
  return apply_bg_symdifference_pl_pa<ObWkbGeogPoint,
      ObWkbGeogMultiLineString,
      ObGeographGeometrycollection>(g1, g2, context, ObBGStrategyType::PL_PA_STRATEGY, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogPoint, ObWkbGeogMultiPolygon, ObGeometry *)
{
  return apply_bg_symdifference_pl_pa<ObWkbGeogPoint,
      ObWkbGeogMultiPolygon,
      ObGeographGeometrycollection>(g1, g2, context, ObBGStrategyType::PL_PA_STRATEGY, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogPoint, ObWkbGeogCollection, ObGeometry *)
{
  return apply_bg_symdifference_pt_coll<ObGeographPoint, ObGeographGeometrycollection>(
      g1, g2, context, ObBGStrategyType::PL_PA_STRATEGY, result);
}
OB_GEO_FUNC_END;

// geograph linestring
OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogLineString, ObWkbGeogPoint, ObGeometry *)
{
  return apply_bg_symdifference_pl_pa<ObWkbGeogPoint,
      ObWkbGeogLineString,
      ObGeographGeometrycollection>(g2, g1, context, ObBGStrategyType::PL_PA_STRATEGY, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogLineString, ObWkbGeogLineString, ObGeometry *)
{
  return apply_bg_symdifference<ObWkbGeogLineString,
      ObWkbGeogLineString,
      ObGeographMultilinestring>(g1, g2, context, result, ObBGStrategyType::LL_LA_AA_STRATEGY);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogLineString, ObWkbGeogPolygon, ObGeometry *)
{
  return apply_bg_symdifference_la<ObWkbGeogLineString,
      ObWkbGeogPolygon,
      ObGeographGeometrycollection>(g1, g2, context, result,
      ObBGStrategyType::LL_LA_AA_STRATEGY);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogLineString, ObWkbGeogMultiPoint, ObGeometry *)
{
  return apply_bg_symdifference_mpl_mpa<ObWkbGeogMultiPoint,
      ObWkbGeogLineString,
      ObGeographGeometrycollection>(g2, g1, context, ObBGStrategyType::PL_PA_STRATEGY, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogLineString, ObWkbGeogMultiLineString, ObGeometry *)
{
  return apply_bg_symdifference<ObWkbGeogLineString,
      ObWkbGeogMultiLineString,
      ObGeographMultilinestring>(g1, g2, context, result, ObBGStrategyType::LL_LA_AA_STRATEGY);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogLineString, ObWkbGeogMultiPolygon, ObGeometry *)
{
  return apply_bg_symdifference_la<ObWkbGeogLineString,
      ObWkbGeogMultiPolygon,
      ObGeographGeometrycollection>(g1, g2, context, result,
      ObBGStrategyType::LL_LA_AA_STRATEGY);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogLineString, ObWkbGeogCollection, ObGeometry *)
{
  return apply_bg_symdifference_line_coll<ObGeographLineString, ObGeographGeometrycollection>(
      g1, g2, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

// geograph polygon
OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogPolygon, ObWkbGeogPoint, ObGeometry *)
{
  return apply_bg_symdifference_pl_pa<ObWkbGeogPoint,
      ObWkbGeogPolygon,
      ObGeographGeometrycollection>(g2, g1, context, ObBGStrategyType::PL_PA_STRATEGY, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogPolygon, ObWkbGeogLineString, ObGeometry *)
{
  return apply_bg_symdifference_la<ObWkbGeogLineString,
      ObWkbGeogPolygon,
      ObGeographGeometrycollection>(g2, g1, context, result,
      ObBGStrategyType::LL_LA_AA_STRATEGY);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogPolygon, ObWkbGeogPolygon, ObGeometry *)
{
  return apply_bg_symdifference<ObWkbGeogPolygon, ObWkbGeogPolygon, ObGeographMultipolygon>(
      g1, g2, context, result, ObBGStrategyType::LL_LA_AA_STRATEGY);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogPolygon, ObWkbGeogMultiPoint, ObGeometry *)
{
  return apply_bg_symdifference_mpl_mpa<ObWkbGeogMultiPoint,
      ObWkbGeogPolygon,
      ObGeographGeometrycollection>(g2, g1, context, ObBGStrategyType::PL_PA_STRATEGY, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogPolygon, ObWkbGeogMultiLineString, ObGeometry *)
{
  return apply_bg_symdifference_la<ObWkbGeogMultiLineString,
      ObWkbGeogPolygon,
      ObGeographGeometrycollection>(g2, g1, context, result,
      ObBGStrategyType::LL_LA_AA_STRATEGY);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogPolygon, ObWkbGeogMultiPolygon, ObGeometry *)
{
  return apply_bg_symdifference<ObWkbGeogPolygon, ObWkbGeogMultiPolygon, ObGeographMultipolygon>(
      g1, g2, context, result, ObBGStrategyType::LL_LA_AA_STRATEGY);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogPolygon, ObWkbGeogCollection, ObGeometry *)
{
  return apply_bg_symdifference_poly_coll<ObGeographPolygon, ObGeographGeometrycollection>(
      g1, g2, context, ObBGStrategyType::LL_LA_AA_STRATEGY, result);
}
OB_GEO_FUNC_END;

// geograph multipoint
OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogMultiPoint, ObWkbGeogPoint, ObGeometry *)
{
  return apply_bg_symdifference_pt_pt<ObWkbGeogPoint, ObWkbGeogMultiPoint, ObGeographMultipoint>(
      g2, g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogMultiPoint, ObWkbGeogLineString, ObGeometry *)
{
  return apply_bg_symdifference_mpl_mpa<ObWkbGeogMultiPoint,
      ObWkbGeogLineString,
      ObGeographGeometrycollection>(g1, g2, context, ObBGStrategyType::PL_PA_STRATEGY, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogMultiPoint, ObWkbGeogPolygon, ObGeometry *)
{
  return apply_bg_symdifference_mpl_mpa<ObWkbGeogMultiPoint,
      ObWkbGeogPolygon,
      ObGeographGeometrycollection>(g1, g2, context, ObBGStrategyType::PL_PA_STRATEGY, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogMultiPoint, ObWkbGeogMultiPoint, ObGeometry *)
{
  return apply_bg_symdifference_pt_pt<ObWkbGeogMultiPoint,
      ObWkbGeogMultiPoint,
      ObGeographMultipoint>(g2, g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogMultiPoint, ObWkbGeogMultiLineString, ObGeometry *)
{
  return apply_bg_symdifference_mpl_mpa<ObWkbGeogMultiPoint,
      ObWkbGeogMultiLineString,
      ObGeographGeometrycollection>(g1, g2, context, ObBGStrategyType::PL_PA_STRATEGY, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogMultiPoint, ObWkbGeogMultiPolygon, ObGeometry *)
{
  return apply_bg_symdifference_mpl_mpa<ObWkbGeogMultiPoint,
      ObWkbGeogMultiPolygon,
      ObGeographGeometrycollection>(g1, g2, context, ObBGStrategyType::PL_PA_STRATEGY, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogMultiPoint, ObWkbGeogCollection, ObGeometry *)
{
  return apply_bg_symdifference_pt_coll<ObGeographMultipoint, ObGeographGeometrycollection>(
      g1, g2, context, ObBGStrategyType::PL_PA_STRATEGY, result);
}
OB_GEO_FUNC_END;

// geograph mutilinestring
OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogMultiLineString, ObWkbGeogPoint, ObGeometry *)
{
  return apply_bg_symdifference_pl_pa<ObWkbGeogPoint,
      ObWkbGeogMultiLineString,
      ObGeographGeometrycollection>(g2, g1, context, ObBGStrategyType::PL_PA_STRATEGY, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogMultiLineString, ObWkbGeogLineString, ObGeometry *)
{
  return apply_bg_symdifference<ObWkbGeogLineString,
      ObWkbGeogMultiLineString,
      ObGeographMultilinestring>(g2, g1, context, result, ObBGStrategyType::LL_LA_AA_STRATEGY);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogMultiLineString, ObWkbGeogPolygon, ObGeometry *)
{
  return apply_bg_symdifference_la<ObWkbGeogMultiLineString,
      ObWkbGeogPolygon,
      ObGeographGeometrycollection>(g1, g2, context, result,
      ObBGStrategyType::LL_LA_AA_STRATEGY);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogMultiLineString, ObWkbGeogMultiPoint, ObGeometry *)
{
  return apply_bg_symdifference_mpl_mpa<ObWkbGeogMultiPoint,
      ObWkbGeogMultiLineString,
      ObGeographGeometrycollection>(g2, g1, context, ObBGStrategyType::PL_PA_STRATEGY, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogMultiLineString, ObWkbGeogMultiLineString, ObGeometry *)
{
  return apply_bg_symdifference<ObWkbGeogMultiLineString,
      ObWkbGeogMultiLineString,
      ObGeographMultilinestring>(g1, g2, context, result, ObBGStrategyType::LL_LA_AA_STRATEGY);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogMultiLineString, ObWkbGeogMultiPolygon, ObGeometry *)
{
  return apply_bg_symdifference_la<ObWkbGeogMultiLineString,
      ObWkbGeogMultiPolygon,
      ObGeographGeometrycollection>(g1, g2, context, result,
      ObBGStrategyType::LL_LA_AA_STRATEGY);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogMultiLineString, ObWkbGeogCollection, ObGeometry *)
{
  return apply_bg_symdifference_line_coll<ObGeographMultilinestring,
  ObGeographGeometrycollection>(
      g1, g2, context, ObBGStrategyType::LL_LA_AA_STRATEGY, result);
}
OB_GEO_FUNC_END;

// geograph multipolygon
OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogMultiPolygon, ObWkbGeogPoint, ObGeometry *)
{
  return apply_bg_symdifference_pl_pa<ObWkbGeogPoint,
      ObWkbGeogMultiPolygon,
      ObGeographGeometrycollection>(g2, g1, context, ObBGStrategyType::PL_PA_STRATEGY, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogMultiPolygon, ObWkbGeogLineString, ObGeometry *)
{
  return apply_bg_symdifference_la<ObWkbGeogLineString,
      ObWkbGeogMultiPolygon,
      ObGeographGeometrycollection>(g2, g1, context, result,
      ObBGStrategyType::LL_LA_AA_STRATEGY);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogMultiPolygon, ObWkbGeogPolygon, ObGeometry *)
{
  return apply_bg_symdifference<ObWkbGeogPolygon, ObWkbGeogMultiPolygon, ObGeographMultipolygon>(
      g2, g1, context, result, ObBGStrategyType::LL_LA_AA_STRATEGY);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogMultiPolygon, ObWkbGeogMultiPoint, ObGeometry *)
{
  return apply_bg_symdifference_mpl_mpa<ObWkbGeogMultiPoint,
      ObWkbGeogMultiPolygon,
      ObGeographGeometrycollection>(g2, g1, context, ObBGStrategyType::PL_PA_STRATEGY, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogMultiPolygon, ObWkbGeogMultiLineString, ObGeometry *)
{
  return apply_bg_symdifference_la<ObWkbGeogMultiLineString,
      ObWkbGeogMultiPolygon,
      ObGeographGeometrycollection>(g2, g1, context, result,
      ObBGStrategyType::LL_LA_AA_STRATEGY);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogMultiPolygon, ObWkbGeogMultiPolygon, ObGeometry *)
{
  return apply_bg_symdifference<ObWkbGeogMultiPolygon,
      ObWkbGeogMultiPolygon,
      ObGeographMultipolygon>(g1, g2, context, result, ObBGStrategyType::LL_LA_AA_STRATEGY);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogMultiPolygon, ObWkbGeogCollection, ObGeometry *)
{
  return apply_bg_symdifference_poly_coll<ObGeographMultipolygon, ObGeographGeometrycollection>(
      g1, g2, context, ObBGStrategyType::LL_LA_AA_STRATEGY, result);
}
OB_GEO_FUNC_END;

// geograph collection
OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogCollection, ObWkbGeogPoint, ObGeometry *)
{
  return apply_bg_symdifference_pt_coll<ObGeographPoint, ObGeographGeometrycollection>(
      g2, g1, context, ObBGStrategyType::PL_PA_STRATEGY, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogCollection, ObWkbGeogLineString, ObGeometry *)
{
  return apply_bg_symdifference_line_coll<ObGeographLineString, ObGeographGeometrycollection>(
      g2, g1, context, ObBGStrategyType::LL_LA_AA_STRATEGY, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogCollection, ObWkbGeogPolygon, ObGeometry *)
{
  return apply_bg_symdifference_poly_coll<ObGeographPolygon, ObGeographGeometrycollection>(
      g2, g1, context, ObBGStrategyType::LL_LA_AA_STRATEGY, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogCollection, ObWkbGeogMultiPoint, ObGeometry *)
{
  return apply_bg_symdifference_pt_coll<ObGeographMultipoint, ObGeographGeometrycollection>(
      g2, g1, context, ObBGStrategyType::PL_PA_STRATEGY, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogCollection, ObWkbGeogMultiLineString, ObGeometry *)
{
  return apply_bg_symdifference_line_coll<ObGeographMultilinestring,
  ObGeographGeometrycollection>(
      g2, g1, context, ObBGStrategyType::LL_LA_AA_STRATEGY, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogCollection, ObWkbGeogMultiPolygon, ObGeometry *)
{
  return apply_bg_symdifference_poly_coll<ObGeographMultipolygon, ObGeographGeometrycollection>(
      g2, g1, context, ObBGStrategyType::LL_LA_AA_STRATEGY, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncSymDifferenceImpl, ObWkbGeogCollection, ObWkbGeogCollection, ObGeometry *)
{
  return eval_symdifference_gc_gc<ObGeographGeometrycollection>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

// tree cartesian polygon
OB_GEO_CART_TREE_FUNC_BEGIN(ObGeoFuncSymDifferenceImpl, ObCartesianPolygon, ObCartesianPolygon, ObGeometry *)
{
  return apply_bg_symdifference<ObCartesianPolygon, ObCartesianPolygon, ObCartesianMultipolygon>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_TREE_FUNC_BEGIN(ObGeoFuncSymDifferenceImpl, ObCartesianPolygon, ObCartesianMultipolygon, ObGeometry *)
{
  return apply_bg_symdifference<ObCartesianPolygon, ObCartesianMultipolygon, ObCartesianMultipolygon>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_TREE_FUNC_BEGIN(ObGeoFuncSymDifferenceImpl, ObCartesianMultipolygon, ObCartesianPolygon, ObGeometry *)
{
  return apply_bg_symdifference<ObCartesianMultipolygon, ObCartesianPolygon, ObCartesianMultipolygon>(g1, g2, context, result);
} OB_GEO_FUNC_END;

// implement of outer class eval
// use an outer class to void implement templates in header files
int ObGeoFuncSymDifference::eval(const ObGeoEvalCtx &gis_context, ObGeometry *&result)
{
  return ObGeoFuncSymDifferenceImpl::eval_geo_func(gis_context, result);
}

}  // namespace common
}  // namespace oceanbase
