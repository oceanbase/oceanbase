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
 * This file contains implementation for ob_geo_func_touches.
 */

#define USING_LOG_PREFIX LIB

#include "lib/geo/ob_geo_dispatcher.h"
#include "lib/geo/ob_geo_func_touches.h"
#include "lib/geo/ob_geo_func_disjoint.h"
#include "lib/geo/ob_geo_utils.h"
#include "lib/geo/ob_geo_func_utils.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace common
{
namespace bg = boost::geometry;
template<typename GeometryType1, typename GeometryType2>
static void touches_with_point(const GeometryType1 *geo1, const GeometryType2 *geo2,
    ObBGStrategyType strategy, const ObGeoEvalCtx &context, bool &res)
{
  if (strategy == ObBGStrategyType::DEFAULT_NONE) {
    res = bg::touches(*geo1, *geo2);
  } else {
    const ObSrsItem *srs = context.get_srs();
    bg::srs::spheroid<double> geog_sphere(srs->semi_major_axis(), srs->semi_minor_axis());
    bg::strategy::within::geographic_winding<ObWkbGeogPoint> point_strategy(geog_sphere);
    res = bg::touches(*geo1, *geo2, point_strategy);
  }
}

template<typename GeometryType1, typename GeometryType2>
static void within_with_point(const GeometryType1 *geo1, const GeometryType2 *geo2,
    ObBGStrategyType strategy, const ObGeoEvalCtx &context, bool &res)
{
  if (strategy == ObBGStrategyType::DEFAULT_NONE) {
    res = bg::within(*geo1, *geo2);
  } else {
    const ObSrsItem *srs = context.get_srs();
    bg::srs::spheroid<double> geog_sphere(srs->semi_major_axis(), srs->semi_minor_axis());
    bg::strategy::within::geographic_winding<ObWkbGeogPoint> point_strategy(geog_sphere);
    res = bg::within(*geo1, *geo2, point_strategy);
  }
}

template<typename MptType, typename GeoType>
static int eval_touches_mpt(const ObGeometry *mpt, const ObGeometry *geo, const ObGeoEvalCtx &context,
    ObBGStrategyType strategy, bool &result)
{
  int ret = OB_SUCCESS;
  const MptType *mpt_bin = NULL;
  const GeoType *geo_bin = NULL;
  if (mpt->is_tree()) {
    mpt_bin = reinterpret_cast<const MptType *>(mpt);
    geo_bin = reinterpret_cast<const GeoType *>(geo);
  } else {
    mpt_bin = reinterpret_cast<const MptType *>(mpt->val());
    geo_bin = reinterpret_cast<const GeoType *>(geo->val());
  }
  result = false;
  if (OB_ISNULL(mpt_bin) || OB_ISNULL(geo_bin)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("multipoint or linestring pointer is null", K(ret), K(mpt_bin), K(geo_bin));
  } else {
    // At least one point in g1 has to touch g2,
    // and none of the points in g1 may be within g2
    typename MptType::const_iterator iter = mpt_bin->begin();
    bool pt_within = false;
    for (; OB_SUCC(ret) && iter != mpt_bin->end() && !pt_within; iter++) {
      typename MptType::value_type &pt = *iter;
      bool pt_touches = false;
      if (!result) {
        touches_with_point(&pt, geo_bin, strategy, context, pt_touches);
        result = pt_touches;
      }
      if (!pt_touches) {
        within_with_point(&pt, geo_bin, strategy, context, pt_within);
      }
    }
    if (pt_within) {
      result = false;
    }
  }
  return ret;
}

template<typename GeoType1, typename GeoType2>
static int eval_touches_with_point_strategy(
    const ObGeometry *g1, const ObGeometry *g2, const ObGeoEvalCtx &context, bool &result)
{
  INIT_SUCC(ret);
  const ObSrsItem *srs = context.get_srs();
  if (OB_ISNULL(srs)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("srs is null", K(ret), K(g1->get_srid()), K(g1), K(g2));
  } else {
    bg::srs::spheroid<double> geog_sphere(srs->semi_major_axis(), srs->semi_minor_axis());
    bg::strategy::within::geographic_winding<ObWkbGeogPoint> point_strategy(geog_sphere);
    const GeoType1 *geo1 = reinterpret_cast<const GeoType1 *>(g1->val());
    const GeoType2 *geo2 = reinterpret_cast<const GeoType2 *>(g2->val());
    if (OB_ISNULL(geo1) || OB_ISNULL(geo2)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("multipoint or linestring pointer is null", K(ret), K(geo1), K(geo2));
    } else {
      result = bg::touches(*geo1, *geo2, point_strategy);
    }
  }
  return ret;
}

template<typename GeoType1, typename GeoType2>
static int eval_touches_without_strategy(const ObGeometry *g1, const ObGeometry *g2, bool &result)
{
  int ret = OB_SUCCESS;
  const GeoType1 *geo1 = NULL;
  const GeoType2 *geo2 = NULL;
  if (g1->is_tree()) {
    geo1 = reinterpret_cast<const GeoType1 *>(g1);
    geo2 = reinterpret_cast<const GeoType2 *>(g2);
  } else {
    geo1 = reinterpret_cast<const GeoType1 *>(g1->val());
    geo2 = reinterpret_cast<const GeoType2 *>(g2->val());
  }
  if (OB_ISNULL(geo1) || OB_ISNULL(geo2)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("multipoint or linestring pointer is null", K(ret), K(geo1), K(geo2));
  } else {
    result = bg::touches(*geo1, *geo2);
  }
  return ret;
}

template<typename GeoType1, typename GeoType2>
static int eval_touches_with_nonpoint_strategy(
    const ObGeometry *g1, const ObGeometry *g2, const ObGeoEvalCtx &context, bool &result)
{
  INIT_SUCC(ret);
  const ObSrsItem *srs = context.get_srs();
  if (OB_ISNULL(srs)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("srs is null", K(ret), K(g1->get_srid()), K(g1), K(g2));
  } else {
    bg::srs::spheroid<double> geog_sphere(srs->semi_major_axis(), srs->semi_minor_axis());
    bg::strategy::intersection::geographic_segments<> nonpoint_strategy(geog_sphere);
    const GeoType1 *geo1 = NULL;
    const GeoType2 *geo2 = NULL;
    if (g1->is_tree()) {
      geo1 = reinterpret_cast<const GeoType1 *>(g1);
      geo2 = reinterpret_cast<const GeoType2 *>(g2);
    } else {
      geo1 = reinterpret_cast<const GeoType1 *>(g1->val());
      geo2 = reinterpret_cast<const GeoType2 *>(g2->val());
    }
    if (OB_ISNULL(geo1) || OB_ISNULL(geo2)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("multipoint or linestring pointer is null", K(ret), K(geo1), K(geo2));
    } else {
      result = bg::touches(*geo1, *geo2, nonpoint_strategy);
    }
  }
  return ret;
}

// ----- ObGeoFuncTouchesImpl -----
class ObGeoFuncTouchesImpl : public ObIGeoDispatcher<bool, ObGeoFuncTouchesImpl>
{
public:
  ObGeoFuncTouchesImpl();
  virtual ~ObGeoFuncTouchesImpl() = default;

  // template for unary
  OB_GEO_UNARY_FUNC_DEFAULT(bool, OB_ERR_GIS_INVALID_DATA);
  OB_GEO_TREE_UNARY_FUNC_DEFAULT(bool, OB_ERR_GIS_INVALID_DATA);
  OB_GEO_CART_TREE_FUNC_DEFAULT(bool, OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS);
  OB_GEO_GEOG_TREE_FUNC_DEFAULT(bool, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);

  // template for binary
  // default cases for cartesian
  template<typename GeoType1, typename GeoType2>
  struct EvalWkbBi
  {
    static int eval(
        const ObGeometry *g1, const ObGeometry *g2, const ObGeoEvalCtx &context, bool &result)
    {
      UNUSED(context);
      return eval_touches_without_strategy<GeoType1, GeoType2>(g1, g2, result);
    }
  };

  // default case for geography (calc using nonpoint_strategy)
  template<typename GeoType1, typename GeoType2>
  struct EvalWkbBiGeog
  {
    static int eval(
        const ObGeometry *g1, const ObGeometry *g2, const ObGeoEvalCtx &context, bool &result)
    {
      return eval_touches_with_nonpoint_strategy<GeoType1, GeoType2>(g1, g2, context, result);
    }
  };

private:
  template<typename GcTreeType>
  static int is_part_touches_gc_other(typename GcTreeType::sub_mpt_type *mpt1,
      typename GcTreeType::sub_ml_type *mls1, typename GcTreeType::sub_mp_type *mpy1,
      const ObGeometry *g2, const ObGeoEvalCtx &context, bool &is_part_touches)
  {
    int ret = OB_SUCCESS;
    is_part_touches = false;
    if (OB_SUCC(ret) && !is_part_touches && !mls1->is_empty()) {
      ObGeometry *mls_bin = NULL;
      if (OB_FAIL(ObGeoTypeUtil::tree_to_bin(
              *context.get_allocator(), mls1, mls_bin, context.get_srs()))) {
        LOG_WARN("failed to convert geo tree to binary", K(ret));
      } else if (OB_FAIL(eval_wkb_binary(mls_bin, g2, context, is_part_touches))) {
        LOG_WARN("fail to eval wkb binary", K(ret));
      }
    }
    if (OB_SUCC(ret) && !is_part_touches && !mpy1->is_empty()) {
      ObGeometry *mpy_bin = NULL;
      if (OB_FAIL(ObGeoTypeUtil::tree_to_bin(
              *context.get_allocator(), mpy1, mpy_bin, context.get_srs()))) {
        LOG_WARN("failed to convert geo tree to binary", K(ret));
      } else if (OB_FAIL(eval_wkb_binary(mpy_bin, g2, context, is_part_touches))) {
        LOG_WARN("fail to eval wkb binary", K(ret));
      }
    }
    return ret;
  }

  template<typename GcTreeType>
  static int is_part_touches_gc_gc(typename GcTreeType::sub_mpt_type *mpt1,
      typename GcTreeType::sub_ml_type *mls1, typename GcTreeType::sub_mp_type *mpy1,
      typename GcTreeType::sub_mpt_type *mpt2, typename GcTreeType::sub_ml_type *mls2,
      typename GcTreeType::sub_mp_type *mpy2, const ObGeoEvalCtx &context, bool &is_part_touches)
  {
    int ret = OB_SUCCESS;
    is_part_touches = false;
    if (!is_part_touches && !mpy1->is_empty() && !mls2->is_empty()) {
      if (OB_FAIL(eval_tree_binary(mpy1, mls2, context, is_part_touches))) {
        LOG_WARN("fail to eval tree binary", K(ret));
      }
    }
    if (OB_SUCC(ret) && !is_part_touches && !mpy1->is_empty() && !mpy2->is_empty()) {
      if (OB_FAIL(eval_tree_binary(mpy1, mpy2, context, is_part_touches))) {
        LOG_WARN("fail to eval tree binary", K(ret));
      }
    }
    if (OB_SUCC(ret) && !is_part_touches && !mls1->is_empty() && !mls2->is_empty()) {
      if (OB_FAIL(eval_tree_binary(mls1, mls2, context, is_part_touches))) {
        LOG_WARN("fail to eval tree binary", K(ret));
      }
    }
    if (OB_SUCC(ret) && !is_part_touches && !mls1->is_empty() && !mpy2->is_empty()) {
      if (OB_FAIL(eval_tree_binary(mls1, mpy2, context, is_part_touches))) {
        LOG_WARN("fail to eval tree binary", K(ret));
      }
    }
    if (OB_SUCC(ret) && !is_part_touches && !mpt1->is_empty() && !mls2->is_empty()) {
      if (OB_FAIL(eval_tree_binary(mpt1, mls2, context, is_part_touches))) {
        LOG_WARN("fail to eval tree binary", K(ret));
      }
    }
    if (OB_SUCC(ret) && !is_part_touches && !mpt1->is_empty() && !mpy2->is_empty()) {
      if (OB_FAIL(eval_tree_binary(mpt1, mpy2, context, is_part_touches))) {
        LOG_WARN("fail to eval tree binary", K(ret));
      }
    }
    if (OB_SUCC(ret) && !is_part_touches && !mls1->is_empty() && !mpt2->is_empty()) {
      if (OB_FAIL(eval_tree_binary(mls1, mpt2, context, is_part_touches))) {
        LOG_WARN("fail to eval tree binary", K(ret));
      }
    }
    if (OB_SUCC(ret) && !is_part_touches && !mpy1->is_empty() && !mpt2->is_empty()) {
      if (OB_FAIL(eval_tree_binary(mpy1, mpt2, context, is_part_touches))) {
        LOG_WARN("fail to eval tree binary", K(ret));
      }
    }
    return ret;
  }

  template<typename GcTreeType>
  static int is_part_joint_gc_gc(typename GcTreeType::sub_mpt_type *mpt1,
      typename GcTreeType::sub_ml_type *mls1, typename GcTreeType::sub_mp_type *mpy1,
      typename GcTreeType::sub_mpt_type *mpt2, typename GcTreeType::sub_ml_type *mls2,
      typename GcTreeType::sub_mp_type *mpy2, const ObGeoEvalCtx &context, bool &is_part_joint)
  {
    int ret = OB_SUCCESS;
    bg::de9im::mask mask("T********");
    is_part_joint = false;
    if (mpt1->crs() == ObGeoCRS::Cartesian) {
      typename GcTreeType::sub_mpt_type::iterator iter = mpt1->begin();
      for (; !is_part_joint && iter != mpt1->end(); iter++) {
        typename GcTreeType::sub_mpt_type::value_type &pt = *iter;
        if (bg::relate(pt, *mpt2, mask) || bg::relate(pt, *mls2, mask)
            || bg::relate(pt, *mpy2, mask)) {
          is_part_joint = true;
        }
      }
      typename GcTreeType::sub_mpt_type::iterator iter2 = mpt2->begin();
      for (; !is_part_joint && iter2 != mpt2->end(); iter2++) {
        typename GcTreeType::sub_mpt_type::value_type &pt = *iter2;
        if (bg::relate(pt, *mls1, mask) || bg::relate(pt, *mpy1, mask)) {
          is_part_joint = true;
        }
      }
      if (!is_part_joint
          && (bg::relate(*mls1, *mls2, mask) || bg::relate(*mls1, *mpy2, mask)
                 || bg::relate(*mpy1, *mls2, mask) || bg::relate(*mpy1, *mpy2, mask))) {
        is_part_joint = true;
      }
    } else {
      const ObSrsItem *srs = context.get_srs();
      if (OB_ISNULL(srs)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("srs is null", K(ret));
      } else {
        bg::srs::spheroid<double> geog_sphere(srs->semi_major_axis(), srs->semi_minor_axis());
        bg::strategy::within::geographic_winding<ObWkbGeogPoint> point_strategy(geog_sphere);
        bg::strategy::intersection::geographic_segments<> nonpoint_strategy(geog_sphere);
        typename GcTreeType::sub_mpt_type::iterator iter = mpt1->begin();
        for (; !is_part_joint && iter != mpt1->end(); iter++) {
          typename GcTreeType::sub_mpt_type::value_type &pt = *iter;
          if (bg::relate(pt, *mpt2, mask) || bg::relate(pt, *mls2, mask, point_strategy)
              || bg::relate(pt, *mpy2, mask, point_strategy)) {
            is_part_joint = true;
          }
        }
        typename GcTreeType::sub_mpt_type::iterator iter2 = mpt2->begin();
        for (; !is_part_joint && iter2 != mpt2->end(); iter2++) {
          typename GcTreeType::sub_mpt_type::value_type &pt = *iter2;
          if (bg::relate(pt, *mls1, mask, point_strategy)
              || bg::relate(pt, *mpy1, mask, point_strategy)) {
            is_part_joint = true;
          }
        }
        if (!is_part_joint
            && (bg::relate(*mls1, *mls2, mask, nonpoint_strategy)
                   || bg::relate(*mls1, *mpy2, mask, nonpoint_strategy)
                   || bg::relate(*mpy1, *mls2, mask, nonpoint_strategy)
                   || bg::relate(*mpy1, *mpy2, mask, nonpoint_strategy))) {
          is_part_joint = true;
        }
      }
    }
    return ret;
  }

  template<typename GeoType, typename GcTreeType>
  static void is_relate(ObGeometry *g, typename GcTreeType::sub_mpt_type *mpt,
      typename GcTreeType::sub_ml_type *mls, typename GcTreeType::sub_mp_type *mpy,
      bool &is_part_joint)
  {
    bg::de9im::mask mask("T********");
    GeoType *geo = reinterpret_cast<GeoType *>(g);
    typename GcTreeType::sub_mpt_type::iterator iter = mpt->begin();
    for (; !is_part_joint && iter != mpt->end(); iter++) {
      typename GcTreeType::sub_mpt_type::value_type &pt = *iter;
      if (bg::relate(pt, *geo, mask)) {
        is_part_joint = true;
      }
    }
    if (!is_part_joint && (bg::relate(*mls, *geo, mask) || bg::relate(*mpy, *geo, mask))) {
      is_part_joint = true;
    }
  }

  template<typename GeoType, typename GcTreeType>
  static void is_relate_with_strategy(ObGeometry *g, typename GcTreeType::sub_mpt_type *mpt,
      typename GcTreeType::sub_ml_type *mls, typename GcTreeType::sub_mp_type *mpy,
      const ObSrsItem *srs, bool &is_part_joint)
  {
    bg::de9im::mask mask("T********");
    bg::srs::spheroid<double> geog_sphere(srs->semi_major_axis(), srs->semi_minor_axis());
    bg::strategy::within::geographic_winding<ObWkbGeogPoint> point_strategy(geog_sphere);
    bg::strategy::intersection::geographic_segments<> nonpoint_strategy(geog_sphere);
    GeoType *geo = reinterpret_cast<GeoType *>(g);
    typename GcTreeType::sub_mpt_type::iterator iter = mpt->begin();
    for (; !is_part_joint && iter != mpt->end(); iter++) {
      typename GcTreeType::sub_mpt_type::value_type &pt = *iter;
      if (bg::relate(pt, *geo, mask, point_strategy)) {
        is_part_joint = true;
      }
    }
    if (!is_part_joint
        && (bg::relate(*mls, *geo, mask, nonpoint_strategy)
               || bg::relate(*mpy, *geo, mask, nonpoint_strategy))) {
      is_part_joint = true;
    }
  }

  template<typename GcTreeType>
  static int is_part_joint_gc_other_geog(typename GcTreeType::sub_mpt_type *mpt1,
      typename GcTreeType::sub_ml_type *mls1, typename GcTreeType::sub_mp_type *mpy1,
      const ObGeometry *g1, const ObGeometry *g2, const ObGeoEvalCtx &context, bool &result)
  {
    int ret = OB_SUCCESS;
    bool is_part_joint = false;
    bg::de9im::mask mask("T********");
    ObGeometry *g2_tree = NULL;
    ObGeoToTreeVisitor geo2_visitor(context.get_allocator());
    ObGeometry *g2_nconst = const_cast<ObGeometry *>(g2);
    if (OB_FAIL(g2_nconst->do_visit(geo2_visitor))) {
      LOG_WARN("failed to do geo2 to_tree visit", K(ret));
    } else if (FALSE_IT(g2_tree = geo2_visitor.get_geometry())) {
    } else if (OB_ISNULL(g2_tree)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("fail to get g2 tree type", K(ret));
    } else {
      const ObSrsItem *srs = context.get_srs();
      if (OB_ISNULL(srs)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("srs is null", K(ret), K(g1->get_srid()), K(g1), K(g2));
      } else {
        bg::srs::spheroid<double> geog_sphere(srs->semi_major_axis(), srs->semi_minor_axis());
        bg::strategy::within::geographic_winding<ObWkbGeogPoint> point_strategy(geog_sphere);
        bg::strategy::intersection::geographic_segments<> nonpoint_strategy(geog_sphere);
        switch (g2->type()) {
          case ObGeoType::POINT: {
            ObGeographPoint *geo2 = reinterpret_cast<ObGeographPoint *>(g2_tree);
            if (!mpt1->is_empty() && mls1->is_empty() && mpy1->is_empty()) {
              is_part_joint = true;
            } else if (bg::relate(*mpt1, *geo2, mask)
                       || bg::relate(*mls1, *geo2, mask, point_strategy)
                       || bg::relate(*mpy1, *geo2, mask, point_strategy)) {
              is_part_joint = true;
            }
            break;
          }
          case ObGeoType::LINESTRING: {
            is_relate_with_strategy<ObGeographLineString, ObGeographGeometrycollection>(
                g2_tree, mpt1, mls1, mpy1, srs, is_part_joint);
            break;
          }
          case ObGeoType::POLYGON: {
            is_relate_with_strategy<ObGeographPolygon, ObGeographGeometrycollection>(
                g2_tree, mpt1, mls1, mpy1, srs, is_part_joint);
            break;
          }
          case ObGeoType::MULTILINESTRING: {
            is_relate_with_strategy<ObGeographMultilinestring, ObGeographGeometrycollection>(
                g2_tree, mpt1, mls1, mpy1, srs, is_part_joint);
            break;
          }
          case ObGeoType::MULTIPOLYGON: {
            is_relate_with_strategy<ObGeographMultipolygon, ObGeographGeometrycollection>(
                g2_tree, mpt1, mls1, mpy1, srs, is_part_joint);
            break;
          }
          case ObGeoType::MULTIPOINT: {
            if (!mpt1->is_empty() && mls1->is_empty() && mpy1->is_empty()) {
              is_part_joint = true;
            }
            typename GcTreeType::sub_mpt_type *geo2_mpt =
                reinterpret_cast<typename GcTreeType::sub_mpt_type *>(g2_tree);
            typename GcTreeType::sub_mpt_type::iterator iter = geo2_mpt->begin();
            for (; !is_part_joint && iter != geo2_mpt->end(); iter++) {
              typename GcTreeType::sub_mpt_type::value_type &pt = *iter;
              if (bg::relate(pt, *mls1, mask, point_strategy)
                  || bg::relate(pt, *mpy1, mask, point_strategy)) {
                is_part_joint = true;
              }
            }
            // Default strategy is OK for multipoint-multipoint.
            if (!is_part_joint && bg::relate(*mpt1, *geo2_mpt, mask)) {
              is_part_joint = true;
            }
            break;
          }
          default: {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid geometry type", K(ret), K(g2->type()));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      result = !is_part_joint;
    }
    return ret;
  }

  template<typename GcTreeType>
  static int is_part_joint_gc_other_cart(typename GcTreeType::sub_mpt_type *mpt1,
      typename GcTreeType::sub_ml_type *mls1, typename GcTreeType::sub_mp_type *mpy1,
      const ObGeometry *g1, const ObGeometry *g2, const ObGeoEvalCtx &context, bool &result)
  {
    int ret = OB_SUCCESS;
    bool is_part_joint = false;
    bg::de9im::mask mask("T********");
    ObGeometry *g2_tree = NULL;
    ObGeoToTreeVisitor geo2_visitor(context.get_allocator());
    ObGeometry *g2_nconst = const_cast<ObGeometry *>(g2);
    if (OB_FAIL(g2_nconst->do_visit(geo2_visitor))) {
      LOG_WARN("failed to do geo2 to_tree visit", K(ret));
    } else if (FALSE_IT(g2_tree = geo2_visitor.get_geometry())) {
    } else if (OB_ISNULL(g2_tree)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("fail to get g2 tree type", K(ret));
    } else {
      switch (g2->type()) {
        case ObGeoType::POINT: {
          ObCartesianPoint *geo2 = reinterpret_cast<ObCartesianPoint *>(g2_tree);
          if (!mpt1->is_empty() && mls1->is_empty() && mpy1->is_empty()) {
            is_part_joint = true;
          } else if (bg::relate(*mpt1, *geo2, mask) || bg::relate(*mls1, *geo2, mask)
                     || bg::relate(*mpy1, *geo2, mask)) {
            is_part_joint = true;
          }
          break;
        }
        case ObGeoType::LINESTRING: {
          is_relate<ObCartesianLineString, ObCartesianGeometrycollection>(
              g2_tree, mpt1, mls1, mpy1, is_part_joint);
          break;
        }
        case ObGeoType::POLYGON: {
          is_relate<ObCartesianPolygon, ObCartesianGeometrycollection>(
              g2_tree, mpt1, mls1, mpy1, is_part_joint);
          break;
        }
        case ObGeoType::MULTILINESTRING: {
          is_relate<ObCartesianMultilinestring, ObCartesianGeometrycollection>(
              g2_tree, mpt1, mls1, mpy1, is_part_joint);
          break;
        }
        case ObGeoType::MULTIPOLYGON: {
          is_relate<ObCartesianMultipolygon, ObCartesianGeometrycollection>(
              g2_tree, mpt1, mls1, mpy1, is_part_joint);
          break;
        }
        case ObGeoType::MULTIPOINT: {
          if (!mpt1->is_empty() && mls1->is_empty() && mpy1->is_empty()) {
            is_part_joint = true;
          }
          typename GcTreeType::sub_mpt_type *geo2_mpt =
              reinterpret_cast<typename GcTreeType::sub_mpt_type *>(g2_tree);
          typename GcTreeType::sub_mpt_type::iterator iter = geo2_mpt->begin();
          for (; !is_part_joint && iter != geo2_mpt->end(); iter++) {
            typename GcTreeType::sub_mpt_type::value_type &pt = *iter;
            if (bg::relate(pt, *mls1, mask) || bg::relate(pt, *mpy1, mask)) {
              is_part_joint = true;
            }
          }
          if (!is_part_joint && bg::relate(*mpt1, *geo2_mpt, mask)) {
            is_part_joint = true;
          }
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid geometry type", K(ret), K(g2->type()));
        }
      }
    }

    if (OB_SUCC(ret)) {
      result = !is_part_joint;
    }
    return ret;
  }

  template<typename GcTreeType>
  static int eval_touches_gc_other_cart(
      const ObGeometry *g1, const ObGeometry *g2, const ObGeoEvalCtx &context, bool &result)
  {
    int ret = OB_SUCCESS;
    result = false;
    if (g1->type() == ObGeoType::GEOMETRYCOLLECTION
        && g2->type() == ObGeoType::GEOMETRYCOLLECTION) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("g1 and g2 are both geometry collection", K(ret), K(g1->type()), K(g2->type()));
    } else if (g1->type() == ObGeoType::GEOMETRYCOLLECTION) {
      typename GcTreeType::sub_mpt_type *mpt1 = NULL;
      typename GcTreeType::sub_ml_type *mls1 = NULL;
      typename GcTreeType::sub_mp_type *mpy1 = NULL;
      ObGeometry *geo1 = const_cast<ObGeometry *>(reinterpret_cast<const ObGeometry *>(g1));
      ObIAllocator *allocator = context.get_allocator();
      ObGeoToTreeVisitor tree_visitor(allocator);
      bool is_part_joint = false;
      bool is_part_touches = false;
      bool point_intersects = false;
      if (OB_FAIL(geo1->do_visit(tree_visitor))) {
        LOG_WARN("failed to transform gc to tree", K(ret));
      } else if (OB_FAIL(ObGeoFuncUtils::ob_geo_gc_split(*allocator,
                  *static_cast<const GcTreeType *>(tree_visitor.get_geometry()),
                  mpt1, mls1, mpy1))) {
        LOG_WARN("failed to do gc split", K(ret));
      } else if (OB_ISNULL(mpt1) || OB_ISNULL(mls1) || OB_ISNULL(mpy1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null geometry collection split", K(ret));
      } else if (!mpt1->is_empty() &&
                (g2->type() == ObGeoType::POINT || g2->type() == ObGeoType::MULTIPOINT)) {
        ObGeometry *mpt_bin = NULL;
        if (OB_FAIL(ObGeoTypeUtil::tree_to_bin(
                *context.get_allocator(), mpt1, mpt_bin, context.get_srs()))) {
          LOG_WARN("failed to convert geo tree to binary", K(ret));
        } else {
          ObGeoEvalCtx intersects_context(context.get_allocator(), context.get_srs());
          intersects_context.append_geo_arg(mpt_bin);
          intersects_context.append_geo_arg(g2);
          if (OB_FAIL(ObGeoFuncIntersects::eval(intersects_context, point_intersects))) {
            LOG_WARN("eval disjoint for intersects failed", K(ret));
          } else if (point_intersects) {
            result = false;
          }
        }
      }

      if (point_intersects || OB_FAIL(ret)) {
      } else if (OB_FAIL(ObGeoFuncUtils::ob_geo_gc_union(*allocator, *context.get_srs(), mpt1, mls1, mpy1))) {
        LOG_WARN("fail to do gc union", K(ret));
      } else {
        // Check that at least one part of g1 touches at least one part of g2.
        if (OB_FAIL((is_part_touches_gc_other<GcTreeType>(
                mpt1, mls1, mpy1, g2, context, is_part_touches)))) {
          LOG_WARN("fail to do is part touches gc other", K(ret));
        } else if (!is_part_touches) {
          result = false;
        } else if (OB_FAIL((is_part_joint_gc_other_cart<GcTreeType>(
                       mpt1, mls1, mpy1, g1, g2, context, result)))) {
          LOG_WARN("fail to eval is part joint", K(ret));
        }
      }
    } else if (g2->type() == ObGeoType::GEOMETRYCOLLECTION) {
      ret = eval_touches_gc_other_cart<GcTreeType>(g2, g1, context, result);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("g1 and g2 are not geometry collection", K(ret), K(g1->type()), K(g2->type()));
    }
    return ret;
  }

  template<typename GcTreeType>
  static int eval_touches_gc_other_geog(
      const ObGeometry *g1, const ObGeometry *g2, const ObGeoEvalCtx &context, bool &result)
  {
    int ret = OB_SUCCESS;
    result = false;
    if (g1->type() == ObGeoType::GEOMETRYCOLLECTION
        && g2->type() == ObGeoType::GEOMETRYCOLLECTION) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("g1 and g2 are both geometry collection", K(ret), K(g1->type()), K(g2->type()));
    } else if (g1->type() == ObGeoType::GEOMETRYCOLLECTION) {
      typename GcTreeType::sub_mpt_type *mpt1 = NULL;
      typename GcTreeType::sub_ml_type *mls1 = NULL;
      typename GcTreeType::sub_mp_type *mpy1 = NULL;
      ObGeometry *geo1 = const_cast<ObGeometry *>(reinterpret_cast<const ObGeometry *>(g1));
      ObIAllocator *allocator = context.get_allocator();
      ObGeoToTreeVisitor tree_visitor(allocator);
      bool is_part_joint = false;
      bool is_part_touches = false;
      bool point_intersects = false;
      if (OB_FAIL(geo1->do_visit(tree_visitor))) {
        LOG_WARN("failed to transform gc to tree", K(ret));
      } else if (OB_FAIL(ObGeoFuncUtils::ob_geo_gc_split(*allocator,
                  *static_cast<const GcTreeType *>(tree_visitor.get_geometry()),
                  mpt1, mls1, mpy1))) {
        LOG_WARN("failed to do gc split", K(ret));
      } else if (OB_ISNULL(mpt1) || OB_ISNULL(mls1) || OB_ISNULL(mpy1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null geometry collection split", K(ret));
      } else if (!mpt1->is_empty() &&
                (g2->type() == ObGeoType::POINT || g2->type() == ObGeoType::MULTIPOINT)) {
        ObGeometry *mpt_bin = NULL;
        if (OB_FAIL(ObGeoTypeUtil::tree_to_bin(
                *context.get_allocator(), mpt1, mpt_bin, context.get_srs()))) {
          LOG_WARN("failed to convert geo tree to binary", K(ret));
        } else {
          ObGeoEvalCtx intersects_context(context.get_allocator(), context.get_srs());
          intersects_context.append_geo_arg(mpt_bin);
          intersects_context.append_geo_arg(g2);
          if (OB_FAIL(ObGeoFuncIntersects::eval(intersects_context, point_intersects))) {
            LOG_WARN("eval disjoint for intersects failed", K(ret));
          } else if (point_intersects) {
            result = false;
          }
        }
      }

      if (point_intersects || OB_FAIL(ret)) {
      } else if (OB_FAIL(ObGeoFuncUtils::ob_geo_gc_union(*allocator, *context.get_srs(), mpt1, mls1, mpy1))) {
        LOG_WARN("fail to do gc union", K(ret));
      } else {
        // Check that at least one part of g1 touches at least one part of g2.
        bool is_part_touches = false;
        if (OB_FAIL((is_part_touches_gc_other<GcTreeType>(
                mpt1, mls1, mpy1, g2, context, is_part_touches)))) {
          LOG_WARN("fail to do is part touches gc other", K(ret));
        } else if (!is_part_touches) {
          result = false;
        } else if (OB_FAIL((is_part_joint_gc_other_geog<GcTreeType>(
                       mpt1, mls1, mpy1, g1, g2, context, result)))) {
          LOG_WARN("fail to eval is part joint", K(ret));
        }
      }
    } else if (g2->type() == ObGeoType::GEOMETRYCOLLECTION) {
      ret = eval_touches_gc_other_geog<GcTreeType>(g2, g1, context, result);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("g1 and g2 are not geometry collection", K(ret), K(g1->type()), K(g2->type()));
    }
    return ret;
  }

  // assume that g1 g2 both collection
  template<typename GcTreeType>
  static int eval_touches_gc_gc(
      const ObGeometry *g1, const ObGeometry *g2, const ObGeoEvalCtx &context, bool &result)
  {
    int ret = OB_SUCCESS;
    if (g1->type() != ObGeoType::GEOMETRYCOLLECTION
        || g2->type() != ObGeoType::GEOMETRYCOLLECTION) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("g1 or g2 is not geometry collection", K(ret), K(g1->type()), K(g2->type()));
    } else {
      result = false;
      typename GcTreeType::sub_mpt_type *mpt1 = NULL;
      typename GcTreeType::sub_ml_type *mls1 = NULL;
      typename GcTreeType::sub_mp_type *mpy1 = NULL;
      ObGeometry *geo1 = const_cast<ObGeometry *>(reinterpret_cast<const ObGeometry *>(g1));
      if (OB_FAIL(ObGeoFuncUtils::ob_gc_prepare<GcTreeType>(context, geo1, mpt1, mls1, mpy1))) {
        LOG_WARN("failed to prepare gc", K(ret));
      } else if (OB_ISNULL(mpt1) || OB_ISNULL(mls1) || OB_ISNULL(mpy1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null geometry collection split", K(ret));
      } else {
        typename GcTreeType::sub_mpt_type *mpt2 = NULL;
        typename GcTreeType::sub_ml_type *mls2 = NULL;
        typename GcTreeType::sub_mp_type *mpy2 = NULL;
        ObGeometry *geo2 = const_cast<ObGeometry *>(reinterpret_cast<const ObGeometry *>(g2));
        // Check that at least one part of g1 touches at least one part of g2.
        bool is_part_touches = false;
        bool is_part_joint = false;  // Check that the interiors of g1 and g2 are disjoint.
        if (OB_FAIL(ObGeoFuncUtils::ob_gc_prepare<GcTreeType>(context, geo2, mpt2, mls2, mpy2))) {
          LOG_WARN("failed to prepare gc", K(ret));
        } else if (OB_ISNULL(mpt2) || OB_ISNULL(mls2) || OB_ISNULL(mpy2)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null geometry collection split", K(ret));
        } else if (!mpt1->is_empty() && mls1->is_empty() && mpy1->is_empty() && !mpt2->is_empty()
                   && mls2->is_empty() && mpy2->is_empty()) {
          // MySQL return NULL, PG return false
          result = false;
        } else if (OB_FAIL((is_part_touches_gc_gc<GcTreeType>(
                       mpt1, mls1, mpy1, mpt2, mls2, mpy2, context, is_part_touches)))) {
          LOG_WARN("fail to eval is part touches", K(ret));
        } else if (!is_part_touches) {
          result = false;
        } else if (OB_FAIL((is_part_joint_gc_gc<GcTreeType>(
                       mpt1, mls1, mpy1, mpt2, mls2, mpy2, context, is_part_joint)))) {
          LOG_WARN("fail to eval is part joint", K(ret));
        } else {
          result = !is_part_joint;
        }
      }
    }
    return ret;
  }
};

//===========BIN GEOM==========
// Geom Point/MultiPoint: cartesian cases return false (PG) / NULL (MySQL)
OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncTouchesImpl, ObWkbGeomPoint, ObWkbGeomPoint, bool)
{
  UNUSEDx(g1, g2, context);
  result = false;
  return OB_SUCCESS;
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncTouchesImpl, ObWkbGeomPoint, ObWkbGeomMultiPoint, bool)
{
  UNUSEDx(g1, g2, context);
  result = false;
  return OB_SUCCESS;
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncTouchesImpl, ObWkbGeomMultiPoint, ObWkbGeomPoint, bool)
{
  UNUSEDx(g1, g2, context);
  result = false;
  return OB_SUCCESS;
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncTouchesImpl, ObWkbGeomMultiPoint, ObWkbGeomMultiPoint, bool)
{
  UNUSEDx(g1, g2, context);
  result = false;
  return OB_SUCCESS;
}
OB_GEO_FUNC_END;

// cartesian cases with eval_touches_mpt (MultiPoint)
OB_GEO_CART_BINARY_FUNC_GEO2_BEGIN(ObGeoFuncTouchesImpl, ObWkbGeomMultiPoint, bool)
{
  return eval_touches_mpt<ObWkbGeomMultiPoint, GeoType1>(
      g2, g1, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_GEO1_BEGIN(ObGeoFuncTouchesImpl, ObWkbGeomMultiPoint, bool)
{
  return eval_touches_mpt<ObWkbGeomMultiPoint, GeoType2>(
      g1, g2, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

// Geom Collection
OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncTouchesImpl, ObWkbGeomCollection, ObWkbGeomCollection, bool)
{
  return eval_touches_gc_gc<ObCartesianGeometrycollection>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_GEO2_BEGIN(ObGeoFuncTouchesImpl, ObWkbGeomCollection, bool)
{
  return eval_touches_gc_other_cart<ObCartesianGeometrycollection>(g2, g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_GEO1_BEGIN(ObGeoFuncTouchesImpl, ObWkbGeomCollection, bool)
{
  return eval_touches_gc_other_cart<ObCartesianGeometrycollection>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

// handle ambiguous partial specializations
OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncTouchesImpl, ObWkbGeomCollection, ObWkbGeomMultiPoint, bool)
{
  return eval_touches_gc_other_cart<ObCartesianGeometrycollection>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncTouchesImpl, ObWkbGeomMultiPoint, ObWkbGeomCollection, bool)
{
  return eval_touches_gc_other_cart<ObCartesianGeometrycollection>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

//===========BIN GEOG==========
// handle ambiguous partial specializations
OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncTouchesImpl, ObWkbGeogCollection, ObWkbGeogPoint, bool)
{
  return eval_touches_gc_other_geog<ObGeographGeometrycollection>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncTouchesImpl, ObWkbGeogPoint, ObWkbGeogCollection, bool)
{
  return eval_touches_gc_other_geog<ObGeographGeometrycollection>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

// geometrycollection for geography
OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncTouchesImpl, ObWkbGeogCollection, ObWkbGeogCollection, bool)
{
  return eval_touches_gc_gc<ObGeographGeometrycollection>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_GEO2_BEGIN(ObGeoFuncTouchesImpl, ObWkbGeogCollection, bool)
{
  return eval_touches_gc_other_geog<ObGeographGeometrycollection>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_GEO1_BEGIN(ObGeoFuncTouchesImpl, ObWkbGeogCollection, bool)
{
  return eval_touches_gc_other_geog<ObGeographGeometrycollection>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

// cases use eval_touches_mpt (multi point)
OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncTouchesImpl, ObWkbGeogMultiPoint, ObWkbGeogPolygon, bool)
{
  return eval_touches_mpt<ObWkbGeogMultiPoint, ObWkbGeogPolygon>(
      g1, g2, context, ObBGStrategyType::PL_PA_STRATEGY, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncTouchesImpl, ObWkbGeogMultiPoint, ObWkbGeogMultiPolygon, bool)
{
  return eval_touches_mpt<ObWkbGeogMultiPoint, ObWkbGeogMultiPolygon>(
      g1, g2, context, ObBGStrategyType::PL_PA_STRATEGY, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncTouchesImpl, ObWkbGeogPolygon, ObWkbGeogMultiPoint, bool)
{
  return eval_touches_mpt<ObWkbGeogMultiPoint, ObWkbGeogPolygon>(
      g2, g1, context, ObBGStrategyType::PL_PA_STRATEGY, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncTouchesImpl, ObWkbGeogMultiPolygon, ObWkbGeogMultiPoint, bool)
{
  return eval_touches_mpt<ObWkbGeogMultiPoint, ObWkbGeogMultiPolygon>(
      g2, g1, context, ObBGStrategyType::PL_PA_STRATEGY, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncTouchesImpl, ObWkbGeogMultiPoint, ObWkbGeogLineString, bool)
{
  return eval_touches_mpt<ObWkbGeogMultiPoint, ObWkbGeogLineString>(
      g1, g2, context, ObBGStrategyType::PL_PA_STRATEGY, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncTouchesImpl, ObWkbGeogLineString, ObWkbGeogMultiPoint, bool)
{
  return eval_touches_mpt<ObWkbGeogMultiPoint, ObWkbGeogLineString>(
      g2, g1, context, ObBGStrategyType::PL_PA_STRATEGY, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncTouchesImpl, ObWkbGeogMultiPoint, ObWkbGeogMultiLineString, bool)
{
  return eval_touches_mpt<ObWkbGeogMultiPoint, ObWkbGeogMultiLineString>(
      g1, g2, context, ObBGStrategyType::PL_PA_STRATEGY, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncTouchesImpl, ObWkbGeogMultiLineString, ObWkbGeogMultiPoint, bool)
{
  return eval_touches_mpt<ObWkbGeogMultiPoint, ObWkbGeogMultiLineString>(
      g2, g1, context, ObBGStrategyType::PL_PA_STRATEGY, result);
}
OB_GEO_FUNC_END;

// geograpyic cases return false (PG) / NULL (MySQL)
OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncTouchesImpl, ObWkbGeogPoint, ObWkbGeogPoint, bool)
{
  UNUSEDx(g1, g2, context);
  result = false;
  return OB_SUCCESS;
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncTouchesImpl, ObWkbGeogMultiPoint, ObWkbGeogMultiPoint, bool)
{
  UNUSEDx(g1, g2, context);
  result = false;
  return OB_SUCCESS;
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncTouchesImpl, ObWkbGeogPoint, ObWkbGeogMultiPoint, bool)
{
  UNUSEDx(g1, g2, context);
  result = false;
  return OB_SUCCESS;
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncTouchesImpl, ObWkbGeogMultiPoint, ObWkbGeogPoint, bool)
{
  UNUSEDx(g1, g2, context);
  result = false;
  return OB_SUCCESS;
}
OB_GEO_FUNC_END;

// geograpyic cases using point strategy (point and nonpoint types)
OB_GEO_GEOG_BINARY_FUNC_GEO2_BEGIN(ObGeoFuncTouchesImpl, ObWkbGeogPoint, bool)
{
  return eval_touches_with_point_strategy<GeoType1, ObWkbGeogPoint>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_GEO1_BEGIN(ObGeoFuncTouchesImpl, ObWkbGeogPoint, bool)
{
  return eval_touches_with_point_strategy<ObWkbGeogPoint, GeoType2>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

//============================Tree Cart(only Multi)========================
// multipoint
OB_GEO_CART_TREE_FUNC_BEGIN(
    ObGeoFuncTouchesImpl, ObCartesianMultipoint, ObCartesianMultipoint, bool)
{
  UNUSEDx(g1, g2, context);
  result = false;
  return OB_SUCCESS;
}
OB_GEO_FUNC_END;

OB_GEO_CART_TREE_FUNC_BEGIN(
    ObGeoFuncTouchesImpl, ObCartesianMultipoint, ObCartesianMultipolygon, bool)
{
  return eval_touches_mpt<ObCartesianMultipoint, ObCartesianMultipolygon>(
      g1, g2, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_TREE_FUNC_BEGIN(
    ObGeoFuncTouchesImpl, ObCartesianMultipoint, ObCartesianMultilinestring, bool)
{
  return eval_touches_mpt<ObCartesianMultipoint, ObCartesianMultilinestring>(
      g1, g2, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

// multilinestring
OB_GEO_CART_TREE_FUNC_BEGIN(
    ObGeoFuncTouchesImpl, ObCartesianMultilinestring, ObCartesianMultipoint, bool)
{
  return eval_touches_mpt<ObCartesianMultipoint, ObCartesianMultilinestring>(
      g2, g1, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_TREE_FUNC_BEGIN(
    ObGeoFuncTouchesImpl, ObCartesianMultilinestring, ObCartesianMultipolygon, bool)
{
  UNUSED(context);
  return eval_touches_without_strategy<ObCartesianMultilinestring, ObCartesianMultipolygon>(
      g1, g2, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_TREE_FUNC_BEGIN(
    ObGeoFuncTouchesImpl, ObCartesianMultilinestring, ObCartesianMultilinestring, bool)
{
  UNUSED(context);
  return eval_touches_without_strategy<ObCartesianMultilinestring, ObCartesianMultilinestring>(
      g1, g2, result);
}
OB_GEO_FUNC_END;

// multipolygon
OB_GEO_CART_TREE_FUNC_BEGIN(
    ObGeoFuncTouchesImpl, ObCartesianMultipolygon, ObCartesianMultipoint, bool)
{
  return eval_touches_mpt<ObCartesianMultipoint, ObCartesianMultipolygon>(
      g2, g1, context, ObBGStrategyType::DEFAULT_NONE, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_TREE_FUNC_BEGIN(
    ObGeoFuncTouchesImpl, ObCartesianMultipolygon, ObCartesianMultipolygon, bool)
{
  UNUSED(context);
  return eval_touches_without_strategy<ObCartesianMultipolygon, ObCartesianMultipolygon>(
      g1, g2, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_TREE_FUNC_BEGIN(
    ObGeoFuncTouchesImpl, ObCartesianMultipolygon, ObCartesianMultilinestring, bool)
{
  UNUSED(context);
  return eval_touches_without_strategy<ObCartesianMultipolygon, ObCartesianMultilinestring>(
      g1, g2, result);
}
OB_GEO_FUNC_END;

//============================Tree Geog(only Multi)========================
// multipoint
OB_GEO_GEOG_TREE_FUNC_BEGIN(ObGeoFuncTouchesImpl, ObGeographMultipoint, ObGeographMultipoint, bool)
{
  UNUSEDx(g1, g2, context);
  result = false;
  return OB_SUCCESS;
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_TREE_FUNC_BEGIN(
    ObGeoFuncTouchesImpl, ObGeographMultipoint, ObGeographMultipolygon, bool)
{
  return eval_touches_mpt<ObGeographMultipoint, ObGeographMultipolygon>(
      g1, g2, context, ObBGStrategyType::PL_PA_STRATEGY, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_TREE_FUNC_BEGIN(
    ObGeoFuncTouchesImpl, ObGeographMultipoint, ObGeographMultilinestring, bool)
{
  return eval_touches_mpt<ObGeographMultipoint, ObGeographMultilinestring>(
      g1, g2, context, ObBGStrategyType::PL_PA_STRATEGY, result);
}
OB_GEO_FUNC_END;

// multilinestring
OB_GEO_GEOG_TREE_FUNC_BEGIN(
    ObGeoFuncTouchesImpl, ObGeographMultilinestring, ObGeographMultipoint, bool)
{
  return eval_touches_mpt<ObGeographMultipoint, ObGeographMultilinestring>(
      g2, g1, context, ObBGStrategyType::PL_PA_STRATEGY, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_TREE_FUNC_BEGIN(
    ObGeoFuncTouchesImpl, ObGeographMultilinestring, ObGeographMultipolygon, bool)
{
  return eval_touches_with_nonpoint_strategy<ObGeographMultilinestring, ObGeographMultipolygon>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_TREE_FUNC_BEGIN(
    ObGeoFuncTouchesImpl, ObGeographMultilinestring, ObGeographMultilinestring, bool)
{
  return eval_touches_with_nonpoint_strategy<ObGeographMultilinestring, ObGeographMultilinestring>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

// multipolygon
OB_GEO_GEOG_TREE_FUNC_BEGIN(
    ObGeoFuncTouchesImpl, ObGeographMultipolygon, ObGeographMultipoint, bool)
{
  return eval_touches_mpt<ObGeographMultipoint, ObGeographMultipolygon>(
      g2, g1, context, ObBGStrategyType::PL_PA_STRATEGY, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_TREE_FUNC_BEGIN(
    ObGeoFuncTouchesImpl, ObGeographMultipolygon, ObGeographMultipolygon, bool)
{
  return eval_touches_with_nonpoint_strategy<ObGeographMultipolygon, ObGeographMultipolygon>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_TREE_FUNC_BEGIN(
    ObGeoFuncTouchesImpl, ObGeographMultipolygon, ObGeographMultilinestring, bool)
{
  return eval_touches_with_nonpoint_strategy<ObGeographMultipolygon, ObGeographMultilinestring>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

int ObGeoFuncTouches::eval(const ObGeoEvalCtx &gis_context, bool &result)
{
  return ObGeoFuncTouchesImpl::eval_geo_func(gis_context, result);
}

}  // namespace common
}  // namespace oceanbase