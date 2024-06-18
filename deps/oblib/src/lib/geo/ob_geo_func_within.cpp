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
 * This file contains implementation for ob_geo_func_within.
 */

#define USING_LOG_PREFIX LIB

#include "lib/geo/ob_geo_dispatcher.h"
#include "lib/geo/ob_geo_func_within.h"
#include "lib/geo/ob_geo_to_wkt_visitor.h"
#include "lib/geo/ob_geo_func_utils.h"
#include "lib/geo/ob_geo_utils.h"
#include "lib/geo/ob_geo_func_difference.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace common
{
namespace bg = boost::geometry;

static int do_multi_difference(const ObSrsItem &srs,
                                        ObIAllocator &allocator,
                                        ObGeometry *geo,
                                        ObGeometry *mpt,
                                        ObGeometry *ml,
                                        ObGeometry *mpo,
                                        ObGeometry *&res_geo)
{
  INIT_SUCC(ret);
  ObGeometry *res_geo1 = NULL;
  ObGeometry *res_geo2 = NULL;
  ObGeoEvalCtx gis_context1(&allocator, &srs);
  ObGeoEvalCtx gis_context2(&allocator, &srs);
  ObGeoEvalCtx gis_context3(&allocator, &srs);
  if (OB_NOT_NULL(mpt)) {
    if (OB_FAIL(gis_context1.append_geo_arg(reinterpret_cast<ObGeometry *>(geo))) || OB_FAIL(gis_context1.append_geo_arg(mpt))) {
      LOG_WARN("build gis context failed", K(ret), K(gis_context1.get_geo_count()));
    } else if (OB_FAIL(ObGeoFuncDifference::eval(gis_context1, res_geo1))) {
      LOG_WARN("eval st intersection failed", K(ret));
    }
  } else {
    res_geo1 = geo;
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_NOT_NULL(ml)) {
    if (OB_FAIL(gis_context2.append_geo_arg(res_geo1)) || OB_FAIL(gis_context2.append_geo_arg(ml))) {
      LOG_WARN("build gis context failed", K(ret), K(gis_context2.get_geo_count()));
    } else if (OB_FAIL(ObGeoFuncDifference::eval(gis_context2, res_geo2))) {
      LOG_WARN("eval st intersection failed", K(ret));
    }
  } else {
    res_geo2 = res_geo1;
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_NOT_NULL(mpo)) {
    if (OB_FAIL(gis_context3.append_geo_arg(res_geo2)) || OB_FAIL(gis_context3.append_geo_arg(mpo))) {
      LOG_WARN("build gis context failed", K(ret), K(gis_context3.get_geo_count()));
    } else if (OB_FAIL(ObGeoFuncDifference::eval(gis_context3, res_geo))) {
      LOG_WARN("eval st intersection failed", K(ret));
    }
  } else {
    res_geo = res_geo2;
  }
  return ret;
}


template <typename GeoType1, typename GeoType2>
static int apply_bg_within(const ObGeometry *g1,
                           const ObGeometry *g2,
                           const ObGeoEvalCtx &context,
                           bool &result)
{
  UNUSED(context);
  int ret = OB_SUCCESS;
  const GeoType1 *geo1 = nullptr;
  const GeoType2 *geo2 = nullptr;
  if (!g1->is_tree()) {
    geo1 = reinterpret_cast<const GeoType1 *>(g1->val());
    geo2 = reinterpret_cast<const GeoType2 *>(g2->val());
  } else {
    geo1 = reinterpret_cast<GeoType1 *>(const_cast<ObGeometry *>(g1));
    geo2 = reinterpret_cast<GeoType2 *>(const_cast<ObGeometry *>(g2));
  }
  if (OB_ISNULL(geo1) || OB_ISNULL(geo2)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("geometry can not be null", K(ret), K(geo1), K(geo2));
  } else {
    result = bg::within(*geo1, *geo2);
  }
  return ret;
}

template <typename GeoType1, typename GeoType2>
static int apply_bg_within_pl_pa_strategy(const ObGeometry *g1,
                                          const ObGeometry *g2,
                                          const ObGeoEvalCtx &context,
                                          bool &result)
{
  INIT_SUCC(ret);
  const ObSrsItem *srs = context.get_srs();
  if (OB_ISNULL(srs)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("srs is null in geographic eval", K(ret));
  } else {
    bg::srs::spheroid<double> geog_sphere(srs->semi_major_axis(), srs->semi_minor_axis());
    ObPlPaStrategy geog_pl_pa_strategy(geog_sphere);
    const GeoType1 *geo1 = reinterpret_cast<const GeoType1 *>(g1->val());
    const GeoType2 *geo2 = reinterpret_cast<const GeoType2 *>(g2->val());
#ifdef USE_SPHERE_GEO
    result = bg::within(*geo1, *geo2, geog_pl_pa_strategy);
#else
    result = bg::within(*geo1, *geo2);
#endif
  }
  return ret;
}

template <typename GeoType1, typename GeoType2>
static int apply_bg_within_ll_la_aa_strategy(const ObGeometry *g1,
                                              const ObGeometry *g2,
                                              const ObGeoEvalCtx &context,
                                              bool &result)
{
  INIT_SUCC(ret);
  const ObSrsItem *srs = context.get_srs();
  if (OB_ISNULL(srs)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("srs is null in geographic eval", K(ret));
  } else {
    bg::srs::spheroid<double> geog_sphere(srs->semi_major_axis(), srs->semi_minor_axis());
    ObLlLaAaStrategy geog_ll_la_aa_strategy(geog_sphere);
    const GeoType1 *geo1 = reinterpret_cast<const GeoType1 *>(g1->val());
    const GeoType2 *geo2 = reinterpret_cast<const GeoType2 *>(g2->val());
#ifdef USE_SPHERE_GEO
    result = bg::within(*geo1, *geo2, geog_ll_la_aa_strategy);
#else
    result = bg::within(*geo1, *geo2);
#endif
  }
  return ret;
}

template<typename MpType, typename PType>
static int ob_caculate_mp_within_p(const ObGeometry *g1, const ObGeometry *g2,
                                   const ObGeoEvalCtx &context, bool &result)
{
  UNUSED(context);
  const MpType *geo1 = reinterpret_cast<const MpType *>(g1->val());
  const PType *geo2 = reinterpret_cast<const PType *>(g2->val());
  result = bg::equals(*geo2, *geo1);
  return OB_SUCCESS;
}

template<typename MpType>
static int ob_caculate_mp_within_mp(const ObGeometry *g1, const ObGeometry *g2,
                                    const ObGeoEvalCtx &context, bool &result)
{
  bool within = true;
  const MpType *geo1 = reinterpret_cast<const MpType *>(g1->val());
  const MpType *geo2 = reinterpret_cast<const MpType *>(g2->val());
  typename MpType::iterator iter = geo1->begin();
  for (; iter != geo1->end() && within; ++iter) {
    typename MpType::value_type& point = *iter;
    within = bg::within(point, *geo2);
  }
  result = within;
  return OB_SUCCESS;
}

template<typename MpType, typename GEO_TYPE2>
static int ob_caculate_mp_within_l_a(const ObGeometry *g1, const ObGeometry *g2,
                                     const ObGeoEvalCtx &context, bool &result)
{
  bool within = false;
  bool intersects = false;
  const MpType *geo1 = reinterpret_cast<const MpType *>(g1->val());
  const GEO_TYPE2 *geo2 = reinterpret_cast<const GEO_TYPE2 *>(g2->val());
  typename MpType::iterator iter = geo1->begin();
  for (; iter != geo1->end(); ++iter) {
    typename MpType::value_type& point = *iter;
    if (!within) {
      within = bg::within(point, *geo2);
      if (!within) {
        intersects = bg::intersects(point, *geo2);
      } else {
        intersects = true;
      }
    } else {
      intersects = bg::intersects(point, *geo2);
    }
    if (!intersects) break;
  }
  result = (within && intersects);
  return OB_SUCCESS;
}

template<typename MpType, typename GEO_TYPE2>
static int ob_caculate_mp_within_l_a_geog(const ObGeometry *g1, const ObGeometry *g2,
                                          const ObGeoEvalCtx &context, bool &result)
{
  INIT_SUCC(ret);
  bool within = false;
  bool intersects = false;
  const MpType *geo1 = reinterpret_cast<const MpType *>(g1->val());
  const GEO_TYPE2 *geo2 = reinterpret_cast<const GEO_TYPE2 *>(g2->val());
  const ObSrsItem *srs = context.get_srs();
  if (OB_ISNULL(srs)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("srs is null in geographic eval", K(ret));
  } else {
    typename MpType::iterator iter = geo1->begin();
    bg::srs::spheroid<double> geog_sphere(srs->semi_major_axis(), srs->semi_minor_axis());
    ObPlPaStrategy geog_pl_pa_strategy(geog_sphere);
    for (; iter != geo1->end(); ++iter) {
      typename MpType::value_type& point = *iter;
      if (!within) {
#ifdef USE_SPHERE_GEO
        within = bg::within(point, *geo2, geog_pl_pa_strategy);
        if (!within) {
          intersects = bg::intersects(point, *geo2, geog_pl_pa_strategy);
        } else {
          intersects = true;
        }
      } else {
        intersects = bg::intersects(point, *geo2, geog_pl_pa_strategy);
      }
#else
        within = bg::within(point, *geo2);
        if (!within) {
          intersects = bg::intersects(point, *geo2);
        } else {
          intersects = true;
        }
      } else {
        intersects = bg::intersects(point, *geo2);
      }
#endif
      if (!intersects) break;
    }
    result = (within && intersects);
  }
  return ret;
}

template<typename GEO_TYPE1, typename GCType>
static int ob_caculate_ml_within_gc(const ObGeometry *g1, const ObGeometry *g2,
                                    const ObGeoEvalCtx &context, bool &result)
{
  INIT_SUCC(ret);
  typename GCType::sub_mpt_type *multi_point = NULL;
  typename GCType::sub_ml_type *multi_line = NULL;
  typename GCType::sub_mp_type *multi_poly = NULL;
  GEO_TYPE1 *geo1 = const_cast<GEO_TYPE1 *>(reinterpret_cast<const GEO_TYPE1 *>(g1->val()));
  ObGeometry *geo2 = const_cast<ObGeometry *>(reinterpret_cast<const ObGeometry *>(g2));
  if (OB_FAIL(ObGeoFuncUtils::ob_gc_prepare<GCType>(context,
                                                    geo2,
                                                    multi_point,
                                                    multi_line,
                                                    multi_poly))) {
    LOG_WARN("failed to do gc prepare", K(ret));
  } else {
    const ObSrsItem *srs = context.get_srs();
    ObIAllocator *allocator = context.get_allocator();
    ObGeometry *res_geo2 = NULL;
    ObGeoToTreeVisitor visitor(allocator);
    ObGeometry *i_geo1 = const_cast<ObGeometry *>(g1);
    if (OB_FAIL(i_geo1->do_visit(visitor))) {
      LOG_WARN("failed to do geo2 to_tree visit", K(ret));
    } else if (OB_FAIL(do_multi_difference(*srs, *allocator, visitor.get_geometry(),
                                           NULL,
                                           reinterpret_cast<ObGeometry *>(multi_line),
                                           reinterpret_cast<ObGeometry *>(multi_poly),
                                           res_geo2))) {
      LOG_WARN("failed to do mulit difference", K(ret));
    } else {
      bg::de9im::mask mask("T********");
      result = res_geo2->is_empty() &&
              (bg::relate(*geo1, *multi_line, mask) ||
              bg::relate(*geo1, *multi_poly, mask));
    }
  }
  return ret;
}

template<typename GEO_TYPE1, typename GCType>
static int ob_caculate_ml_within_gc_geog(const ObGeometry *g1, const ObGeometry *g2,
                                         const ObGeoEvalCtx &context, bool &result)
{
  INIT_SUCC(ret);
  typename GCType::sub_mpt_type *multi_point = NULL;
  typename GCType::sub_ml_type *multi_line = NULL;
  typename GCType::sub_mp_type *multi_poly = NULL;
  GEO_TYPE1 *geo1 = const_cast<GEO_TYPE1 *>(reinterpret_cast<const GEO_TYPE1 *>(g1->val()));
  ObGeometry *geo2 = const_cast<ObGeometry *>(reinterpret_cast<const ObGeometry *>(g2));
  const ObSrsItem *srs = context.get_srs();
  if (OB_ISNULL(srs)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("srs is null in geographic eval", K(ret));
  } else if (OB_FAIL(ObGeoFuncUtils::ob_gc_prepare<GCType>(context,
                                                           geo2,
                                                           multi_point,
                                                           multi_line,
                                                           multi_poly))) {
    LOG_WARN("failed to do gc prepare", K(ret));
  } else {
    const ObSrsItem *srs = context.get_srs();
    ObIAllocator *allocator = context.get_allocator();
    ObGeometry *res_geo2 = NULL;
    ObGeoToTreeVisitor visitor(allocator);
    ObGeometry *i_geo1 = const_cast<ObGeometry *>(g1);
    if (OB_FAIL(i_geo1->do_visit(visitor))) {
      LOG_WARN("failed to do geo2 to_tree visit", K(ret));
    } else if (OB_FAIL(do_multi_difference(*srs, *allocator, visitor.get_geometry(),
                                           NULL,
                                           reinterpret_cast<ObGeometry *>(multi_line),
                                           reinterpret_cast<ObGeometry *>(multi_poly),
                                           res_geo2))) {
      LOG_WARN("failed to do mulit difference", K(ret));
    } else {
      bg::srs::spheroid<double> geog_sphere(srs->semi_major_axis(), srs->semi_minor_axis());
      ObLlLaAaStrategy geog_ll_la_aa_strategy(geog_sphere);
      // Checks relation between a pair of geometries defined by a mask.
      bg::de9im::mask mask("T********");
      result = res_geo2->is_empty() &&
#ifdef USE_SPHERE_GEO
              (bg::relate(*geo1, *multi_line, mask, geog_ll_la_aa_strategy) ||
              bg::relate(*geo1, *multi_poly, mask, geog_ll_la_aa_strategy));
#else
              (bg::relate(*geo1, *multi_line, mask) ||
              bg::relate(*geo1, *multi_poly, mask));
#endif
    }
  }
  return ret;
}

template<typename GEO_TYPE1, typename GCType>
static int ob_caculate_mpl_within_gc(const ObGeometry *g1, const ObGeometry *g2,
                                     const ObGeoEvalCtx &context, bool &result)
{
  INIT_SUCC(ret);
  typename GCType::sub_mpt_type *multi_point = NULL;
  typename GCType::sub_ml_type *multi_line = NULL;
  typename GCType::sub_mp_type *multi_poly = NULL;
  GEO_TYPE1 *geo1 = const_cast<GEO_TYPE1 *>(reinterpret_cast<const GEO_TYPE1 *>(g1->val()));
  ObGeometry *geo2 = const_cast<ObGeometry *>(reinterpret_cast<const ObGeometry *>(g2));
  if (OB_FAIL(ObGeoFuncUtils::ob_gc_prepare<GCType>(context,
                                                    geo2,
                                                    multi_point,
                                                    multi_line,
                                                    multi_poly))) {
    LOG_WARN("failed to do gc prepare", K(ret));
  } else {
    result = bg::within(*geo1, *multi_poly);
  }
  return ret;
}

template<typename GEO_TYPE1, typename GCType>
static int ob_caculate_mpl_within_gc_geog(const ObGeometry *g1, const ObGeometry *g2,
                                          const ObGeoEvalCtx &context, bool &result)
{
  INIT_SUCC(ret);
  typename GCType::sub_mpt_type *multi_point = NULL;
  typename GCType::sub_ml_type *multi_line = NULL;
  typename GCType::sub_mp_type *multi_poly = NULL;
  GEO_TYPE1 *geo1 = const_cast<GEO_TYPE1 *>(reinterpret_cast<const GEO_TYPE1 *>(g1->val()));
  ObGeometry *geo2 = const_cast<ObGeometry *>(reinterpret_cast<const ObGeometry *>(g2));
  const ObSrsItem *srs = context.get_srs();
  if (OB_ISNULL(srs)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("srs is null in geographic eval", K(ret));
  } else if (OB_FAIL(ObGeoFuncUtils::ob_gc_prepare<GCType>(context,
                                                           geo2,
                                                           multi_point,
                                                           multi_line,
                                                           multi_poly))) {
    LOG_WARN("failed to do gc prepare", K(ret));
  } else {
    bg::srs::spheroid<double> geog_sphere(srs->semi_major_axis(), srs->semi_minor_axis());
    ObLlLaAaStrategy geog_ll_la_aa_strategy(geog_sphere);
#ifdef USE_SPHERE_GEO
    result = bg::within(*geo1, *multi_poly, geog_ll_la_aa_strategy);
#else
    result = bg::within(*geo1, *multi_poly);
#endif
  }
  return ret;
}

template<typename PType, typename GCType>
static int ob_caculate_gc_within_p(const ObGeometry *g1, const ObGeometry *g2,
                                   const ObGeoEvalCtx &context, bool &result)
{
  INIT_SUCC(ret);
  typename GCType::sub_mpt_type *multi_point = NULL;
  typename GCType::sub_ml_type *multi_line = NULL;
  typename GCType::sub_mp_type *multi_poly = NULL;
  ObGeometry *geo1 = const_cast<ObGeometry *>(reinterpret_cast<const ObGeometry *>(g1));
  PType *geo2 = const_cast<PType *>(reinterpret_cast<const PType *>(g2->val()));
  if (OB_FAIL(ObGeoFuncUtils::ob_gc_prepare<GCType>(context,
                                                    geo1,
                                                    multi_point,
                                                    multi_line,
                                                    multi_poly))) {
    LOG_WARN("failed to do gc prepare", K(ret));
  } else {
    bool ml_empty = !multi_line || multi_line->empty();
    bool mpy_empty = !multi_poly || multi_poly->empty();
    result = ml_empty && mpy_empty && bg::equals(*geo2, *multi_point);
  }
  return ret;
}

// ----- ObGeoFuncWithinImpl -----
class ObGeoFuncWithinImpl : public ObIGeoDispatcher<bool, ObGeoFuncWithinImpl>
{
public:
  ObGeoFuncWithinImpl();
  virtual ~ObGeoFuncWithinImpl() = default;

  // template for unary
  OB_GEO_UNARY_FUNC_DEFAULT(bool, OB_ERR_GIS_INVALID_DATA);
  OB_GEO_TREE_UNARY_FUNC_DEFAULT(bool, OB_ERR_GIS_INVALID_DATA);
  OB_GEO_CART_TREE_FUNC_DEFAULT(bool, OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS);
  OB_GEO_GEOG_TREE_FUNC_DEFAULT(bool, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);

  // template for binary
  // default cases for cartesian
  template <typename GeoType1, typename GeoType2>
  struct EvalWkbBi
  {
    static int eval(const ObGeometry *g1, const ObGeometry *g2, const ObGeoEvalCtx &context, bool &result)
    {
      return apply_bg_within<GeoType1, GeoType2>(g1, g2, context, result);
    }
  };

  // default case for geography (calc using nonpoint_strategy)
  template <typename GeoType1, typename GeoType2>
  struct EvalWkbBiGeog
  {
    static int eval(const ObGeometry *g1, const ObGeometry *g2, const ObGeoEvalCtx &context, bool &result)
    {
      result = false;
      return OB_SUCCESS;
    }
  };
private:
  template <typename CollectonType>
  static int eval_within_geometry_collection(const ObGeometry *g1,
                                             const ObGeometry *g2,
                                             const ObGeoEvalCtx &context,
                                             bool &result)
  {
    INIT_SUCC(ret);
    result = false;
    common::ObIAllocator *allocator = context.get_allocator();
    typename CollectonType::iterator iter;
    if (OB_ISNULL(allocator)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Null allocator", K(ret));
    } else if (g1->type() == ObGeoType::GEOMETRYCOLLECTION) {
      const CollectonType *geo1 = reinterpret_cast<const CollectonType *>(g1->val());
      iter = geo1->begin();
      for (; iter != geo1->end() && OB_SUCC(ret) && (result != true); iter++) {
        typename CollectonType::const_pointer sub_ptr = iter.operator->();
        ObGeoType sub_type = geo1->get_sub_type(sub_ptr);
        ObGeometry *sub_g1 = NULL;
        bool is_geog = (g1->crs() == oceanbase::common::ObGeoCRS::Geographic);
        if (OB_FAIL(ObGeoTypeUtil::create_geo_by_type(*allocator, sub_type, is_geog, true, sub_g1))) {
          LOG_WARN("failed to create wkb", K(ret), K(sub_type));
        } else {
          ObString wkb_nosrid(WKB_COMMON_WKB_HEADER_LEN, reinterpret_cast<const char *>(sub_ptr));
          sub_g1->set_data(wkb_nosrid);
          sub_g1->set_srid(g1->get_srid());
        }
        ret = eval_within_geometry_collection<CollectonType>(sub_g1, g2, context, result);
      }
    } else if (g2->type() == ObGeoType::GEOMETRYCOLLECTION) {
      const CollectonType *geo2 = reinterpret_cast<const CollectonType *>(g2->val());
      iter = geo2->begin();
      for (; iter != geo2->end() && OB_SUCC(ret) && (result != true); iter++) {
        typename CollectonType::const_pointer sub_ptr = iter.operator->();
        ObGeoType sub_type = geo2->get_sub_type(sub_ptr);
        ObGeometry *sub_g2 = NULL;
        bool is_geog = (g2->crs() == oceanbase::common::ObGeoCRS::Geographic);
        if (OB_FAIL(ObGeoTypeUtil::create_geo_by_type(*allocator, sub_type, is_geog, true, sub_g2))) {
          LOG_WARN("failed to create wkb", K(ret), K(sub_type));
        } else {
          ObString wkb_nosrid(WKB_COMMON_WKB_HEADER_LEN, reinterpret_cast<const char *>(sub_ptr));
          sub_g2->set_data(wkb_nosrid);
          sub_g2->set_srid(g2->get_srid());
        }
        ret = eval_within_geometry_collection<CollectonType>(g1, sub_g2, context, result);
      }
    } else {
      // none of the two geometries are collection type
      ret = eval_wkb_binary(g1, g2, context, result);
    }
    return ret;
  }

  template<typename PointType, typename IPointType, typename GCType, typename MpType>
  static int ob_caculate_mp_within_gc(const ObGeometry *g1, const ObGeometry *g2,
                                      const ObGeoEvalCtx &context, bool &result)
  {
    INIT_SUCC(ret);
    bool within = false;
    bool intersects = false;
    const MpType *geo1 = reinterpret_cast<const MpType *>(g1->val());
    FOREACH_X(item, *geo1, OB_SUCC(ret)) {
      PointType point;
      point.byteorder(ObGeoWkbByteOrder::LittleEndian);
      point.template set<0>(item->template get<0>());
      point.template set<1>(item->template get<1>());
      ObString data(sizeof(PointType), reinterpret_cast<char *>(&point));
      IPointType i_point;
      i_point.set_data(data);

      ObGeoEvalCtx intersects_context(context.get_allocator(), context.get_srs());
      intersects_context.append_geo_arg(&i_point);
      intersects_context.append_geo_arg(g2);
      if (!within) {
        ret = eval_wkb_binary(&i_point, g2, context, within);
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to do within by functor between Point and collection", K(ret));
        } else {
          if (!within) {
            if (OB_FAIL(ObGeoFuncIntersects::eval(intersects_context, intersects))) {
              LOG_WARN("eval intersects for within failed", K(ret));
            }
          } else {
            intersects = true;
          }
        }
      } else if (OB_FAIL(ObGeoFuncIntersects::eval(intersects_context, intersects))) {
        LOG_WARN("eval intersects for within failed", K(ret));
      }
      if (!intersects) break;
    }
    result = (within && intersects);
    return ret;
  }

  template<typename GCType>
  static int ob_caculate_gc_within_mpt(const ObGeometry *g1, const ObGeometry *g2,
                                       const ObGeoEvalCtx &context, bool &result)
  {
    INIT_SUCC(ret);
    typename GCType::sub_mpt_type *multi_point = NULL;
    typename GCType::sub_ml_type *multi_line = NULL;
    typename GCType::sub_mp_type *multi_poly = NULL;
    common::ObIAllocator *allocator = context.get_allocator();
    ObGeometry *geo1 = const_cast<ObGeometry *>(reinterpret_cast<const ObGeometry *>(g1));
    const ObSrsItem *srs = context.get_srs();
    if (OB_FAIL(ObGeoFuncUtils::ob_gc_prepare<GCType>(context, geo1, multi_point, multi_line, multi_poly))) {
      LOG_WARN("failed to do gc prepare", K(ret));
    } else {
      result = multi_line->empty() &&
               multi_poly->empty();
      if (result) {
        ObGeometry *multi_point_bin = NULL;
        if (OB_FAIL(ObGeoTypeUtil::tree_to_bin(*allocator, multi_point, multi_point_bin, srs))) {
          LOG_WARN("failed to convert geo tree to binary", K(ret));
        } else {
          bool mp_within_mp = false;
          ret = eval_wkb_binary(multi_point_bin, g2, context, mp_within_mp);
          if (OB_FAIL(ret)) {
            LOG_WARN("failed to do within by functor between MultiPoint and MultiPoint", K(ret));
          } else {
            result &= mp_within_mp;
          }
        }
      }
    }
    return ret;
  }
};

// cart_point
OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeomPoint, ObWkbGeomCollection, bool)
{
  return eval_within_geometry_collection<ObWkbGeomCollection>(g1, g2, context, result);
} OB_GEO_FUNC_END;

// cart_linestring
OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeomLineString, ObWkbGeomPoint, bool)
{
  UNUSED(context);
  result = false;
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeomLineString, ObWkbGeomMultiPoint, bool)
{
  UNUSED(context);
  result = false;
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeomLineString, ObWkbGeomCollection, bool)
{
  return ob_caculate_ml_within_gc<ObWkbGeomLineString, ObCartesianGeometrycollection>(g1, g2, context, result);
} OB_GEO_FUNC_END;

// cart_polygon
OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeomPolygon, ObWkbGeomPoint, bool)
{
  UNUSED(context);
  result = false;
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeomPolygon, ObWkbGeomLineString, bool)
{
  UNUSED(context);
  result = false;
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeomPolygon, ObWkbGeomMultiPoint, bool)
{
  UNUSED(context);
  result = false;
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeomPolygon, ObWkbGeomMultiLineString, bool)
{
  UNUSED(context);
  result = false;
  return OB_SUCCESS;
} OB_GEO_FUNC_END;


OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeomPolygon, ObWkbGeomCollection, bool)
{
  return ob_caculate_mpl_within_gc<ObWkbGeomPolygon, ObCartesianGeometrycollection>(g1, g2, context, result);
} OB_GEO_FUNC_END;

// multipoint
OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeomMultiPoint, ObWkbGeomPoint, bool)
{
  return ob_caculate_mp_within_p<ObWkbGeomMultiPoint, ObWkbGeomPoint> (g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeomMultiPoint, ObWkbGeomLineString, bool)
{
  return ob_caculate_mp_within_l_a<ObWkbGeomMultiPoint, ObWkbGeomLineString> (g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeomMultiPoint, ObWkbGeomPolygon, bool)
{
  return ob_caculate_mp_within_l_a<ObWkbGeomMultiPoint, ObWkbGeomPolygon> (g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeomMultiPoint, ObWkbGeomMultiPoint, bool)
{
  return ob_caculate_mp_within_mp<ObWkbGeomMultiPoint> (g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeomMultiPoint, ObWkbGeomMultiLineString, bool)
{
  return ob_caculate_mp_within_l_a<ObWkbGeomMultiPoint, ObWkbGeomMultiLineString> (g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeomMultiPoint, ObWkbGeomMultiPolygon, bool)
{
  return ob_caculate_mp_within_l_a<ObWkbGeomMultiPoint, ObWkbGeomMultiPolygon> (g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeomMultiPoint, ObWkbGeomCollection, bool)
{
  return ob_caculate_mp_within_gc<ObWkbGeomPoint, ObIWkbGeomPoint,
    ObWkbGeomCollection, ObWkbGeomMultiPoint>(g1, g2, context, result);
} OB_GEO_FUNC_END;

// multilinestring
OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeomMultiLineString, ObWkbGeomPoint, bool)
{
  UNUSED(context);
  result = false;
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeomMultiLineString, ObWkbGeomMultiPoint, bool)
{
  UNUSED(context);
  result = false;
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeomMultiLineString, ObWkbGeomCollection, bool)
{
  return ob_caculate_ml_within_gc<ObWkbGeomMultiLineString, ObCartesianGeometrycollection>(g1, g2, context, result);
} OB_GEO_FUNC_END;

// multipolygon
OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeomMultiPolygon, ObWkbGeomPoint, bool)
{
  UNUSED(context);
  result = false;
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeomMultiPolygon, ObWkbGeomLineString, bool)
{
  UNUSED(context);
  result = false;
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeomMultiPolygon, ObWkbGeomMultiPoint, bool)
{
  UNUSED(context);
  result = false;
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeomMultiPolygon, ObWkbGeomMultiLineString, bool)
{
  UNUSED(context);
  result = false;
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeomMultiPolygon, ObWkbGeomCollection, bool)
{
  return ob_caculate_mpl_within_gc<ObWkbGeomMultiPolygon, ObCartesianGeometrycollection>(g1, g2, context, result);
} OB_GEO_FUNC_END;

// cart_geometrycollection
OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeomCollection, ObWkbGeomPoint, bool)
{
  return ob_caculate_gc_within_p<ObWkbGeomPoint, ObCartesianGeometrycollection>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeomCollection, ObWkbGeomLineString, bool)
{
  INIT_SUCC(ret);
  ObCartesianMultipoint *multi_point = NULL;
  ObCartesianMultilinestring *multi_line = NULL;
  ObCartesianMultipolygon *multi_poly = NULL;
  common::ObIAllocator *allocator = context.get_allocator();
  ObGeometry *geo1 = const_cast<ObGeometry *>(reinterpret_cast<const ObGeometry *>(g1));
  const ObWkbGeomLineString *geo2 = reinterpret_cast<const ObWkbGeomLineString *>(g2->val());
  ObGeometry *multi_point_bin = NULL;
  const ObSrsItem *srs = context.get_srs();
  if (OB_FAIL(ObGeoFuncUtils::ob_gc_prepare<ObCartesianGeometrycollection>(context,
                                                                           geo1,
                                                                           multi_point,
                                                                           multi_line,
                                                                           multi_poly))) {
    LOG_WARN("failed to do gc prepare", K(ret));
  } else if (!multi_poly->empty()) {
    result = false;
  } else if (OB_FAIL(ObGeoTypeUtil::tree_to_bin(*allocator, multi_point, multi_point_bin, srs))) {
    LOG_WARN("failed to convert geo tree to binary", K(ret));
  } else {
    bool mp_within_l = false;
    ret = eval_wkb_binary(multi_point_bin, g2, context, mp_within_l);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to do within by functor between ObWkbGeomMultiPoint and ObWkbGeomLineString", K(ret));
    } else if (mp_within_l) {
      result = multi_line->empty() ||
               bg::covered_by(*multi_line, *geo2);
    } else if (bg::within(*multi_line, *geo2)){
      bool covered = true;
      ObCartesianMultipoint::iterator iter = multi_point->begin();
      for (; iter != multi_point->end() && covered; ++iter) {
        ObCartesianMultipoint::value_type& point = *iter;
        covered = bg::covered_by(point, *geo2);
      }
      result = covered;
    } else {
      result = false;
    }
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeomCollection, ObWkbGeomPolygon, bool)
{
  INIT_SUCC(ret);
  ObCartesianMultipoint *multi_point = NULL;
  ObCartesianMultilinestring *multi_line = NULL;
  ObCartesianMultipolygon *multi_poly = NULL;
  common::ObIAllocator *allocator = context.get_allocator();
  ObGeometry *geo1 = const_cast<ObGeometry *>(reinterpret_cast<const ObGeometry *>(g1));
  const ObWkbGeomPolygon *geo2 = reinterpret_cast<const ObWkbGeomPolygon *>(g2->val());
  ObGeometry *multi_point_bin = NULL;
  const ObSrsItem *srs = context.get_srs();
  if (OB_FAIL(ObGeoFuncUtils::ob_gc_prepare<ObCartesianGeometrycollection>(context,
                                                                           geo1,
                                                                           multi_point,
                                                                           multi_line,
                                                                           multi_poly))) {
    LOG_WARN("failed to do gc prepare", K(ret));
  } else if (OB_FAIL(ObGeoTypeUtil::tree_to_bin(*allocator, multi_point, multi_point_bin, srs))) {
    LOG_WARN("failed to convert geo tree to binary", K(ret));
  } else {
    bool mp_within_l = false;
    ret = eval_wkb_binary(multi_point_bin, g2, context, mp_within_l);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to do within by functor between multipoint and polygon", K(ret));
    } else if (mp_within_l) {
      result = (multi_line->empty() ||
               bg::covered_by(*multi_line, *geo2)) &&
               (multi_poly->empty() ||
               bg::covered_by(*multi_poly, *geo2));
    } else if (bg::within(*multi_line, *geo2)) {
      bool covered = true;
      ObCartesianMultipoint::iterator iter = multi_point->begin();
      for (; iter != multi_point->end() && covered; ++iter) {
        ObCartesianMultipoint::value_type& point = *iter;
        covered = bg::covered_by(point, *geo2);
      }
      result = !covered ? false :
               (multi_poly->empty() ||
               bg::covered_by(*multi_poly, *geo2));
    } else if (bg::within(*multi_poly, *geo2)){
      bool covered = true;
      ObCartesianMultipoint::iterator iter = multi_point->begin();
      for (; iter != multi_point->end() && covered; ++iter) {
        ObCartesianMultipoint::value_type& point = *iter;
        covered = bg::covered_by(point, *geo2);
      }
      result = !covered ? false :
               (multi_line->empty() ||
               bg::covered_by(*multi_line, *geo2));
    } else {
      result = false;
    }
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeomCollection, ObWkbGeomMultiPoint, bool)
{
  return ob_caculate_gc_within_mpt<ObCartesianGeometrycollection>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeomCollection, ObWkbGeomMultiLineString, bool)
{
  INIT_SUCC(ret);
  ObCartesianMultipoint *multi_point = NULL;
  ObCartesianMultilinestring *multi_line = NULL;
  ObCartesianMultipolygon *multi_poly = NULL;
  common::ObIAllocator *allocator = context.get_allocator();
  ObGeometry *geo1 = const_cast<ObGeometry *>(reinterpret_cast<const ObGeometry *>(g1));
  const ObWkbGeomMultiLineString *geo2 = reinterpret_cast<const ObWkbGeomMultiLineString *>(g2->val());
  ObGeometry *multi_point_bin = NULL;
  const ObSrsItem *srs = context.get_srs();
  if (OB_FAIL(ObGeoFuncUtils::ob_gc_prepare<ObCartesianGeometrycollection>(context,
                                                                           geo1,
                                                                           multi_point,
                                                                           multi_line,
                                                                           multi_poly))) {
    LOG_WARN("failed to do gc prepare", K(ret));
  } else if (!multi_poly->empty()) {
    result = false;
  } else if (OB_FAIL(ObGeoTypeUtil::tree_to_bin(*allocator, multi_point, multi_point_bin, srs))) {
    LOG_WARN("failed to convert geo tree to binary", K(ret));
  } else {
    bool mp_within_l = false;
    ret = eval_wkb_binary(multi_point_bin, g2, context, mp_within_l);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to do within by functor between GeomMultiPoint and GeomMultiLineString", K(ret));
    } else if (mp_within_l) {
      result = multi_line->empty() ||
               bg::covered_by(*multi_line, *geo2);
    } else if (bg::within(*multi_line, *geo2)){
      bool covered = true;
      ObCartesianMultipoint::iterator iter = multi_point->begin();
      for (; iter != multi_point->end() && covered; ++iter) {
        typename ObCartesianMultipoint::value_type& point = *iter;
        covered = bg::covered_by(point, *geo2);
      }
      result = covered;
    } else {
      result = false;
    }
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeomCollection, ObWkbGeomMultiPolygon, bool)
{
  INIT_SUCC(ret);
  ObCartesianMultipoint *multi_point = NULL;
  ObCartesianMultilinestring *multi_line = NULL;
  ObCartesianMultipolygon *multi_poly = NULL;
  common::ObIAllocator *allocator = context.get_allocator();
  ObGeometry *geo1 = const_cast<ObGeometry *>(reinterpret_cast<const ObGeometry *>(g1));
  const ObWkbGeomMultiPolygon *geo2 = reinterpret_cast<const ObWkbGeomMultiPolygon *>(g2->val());
  ObGeometry *multi_point_bin = NULL;
  const ObSrsItem *srs = context.get_srs();
  if (OB_FAIL(ObGeoFuncUtils::ob_gc_prepare<ObCartesianGeometrycollection>(context,
                                                                           geo1,
                                                                           multi_point,
                                                                           multi_line,
                                                                           multi_poly))) {
    LOG_WARN("failed to do gc prepare", K(ret));
  } else if (OB_FAIL(ObGeoTypeUtil::tree_to_bin(*allocator, multi_point, multi_point_bin, srs))) {
    LOG_WARN("failed to convert geo tree to binary", K(ret));
  } else {
    bool mp_within_l = false;
    ret = eval_wkb_binary(multi_point_bin, g2, context, mp_within_l);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to do within by functor between multipoint and polygon", K(ret));
    } else if (mp_within_l) {
      result = (multi_line->empty() ||
               bg::covered_by(*multi_line, *geo2)) &&
               (multi_poly->empty() ||
               bg::covered_by(*multi_poly, *geo2));
    } else if (bg::within(*multi_line, *geo2)){
      bool covered = true;
      ObCartesianMultipoint::iterator iter = multi_point->begin();
      for (; iter != multi_point->end() && covered; ++iter) {
        ObCartesianMultipoint::value_type& point = *iter;
        covered = bg::covered_by(point, *geo2);
      }
      result = !covered ? false :
               (multi_poly->empty() ||
               bg::covered_by(*multi_poly, *geo2));
    } else if (bg::within(*multi_poly, *geo2)){
      bool covered = true;
      ObCartesianMultipoint::iterator iter = multi_point->begin();
      for (; iter != multi_point->end() && covered; ++iter) {
        ObCartesianMultipoint::value_type& point = *iter;
        covered = bg::covered_by(point, *geo2);
      }
      result = !covered ? false :
               (multi_line->empty() ||
               bg::covered_by(*multi_line, *geo2));
    } else {
      result = false;
    }
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeomCollection, ObWkbGeomCollection, bool)
{
  INIT_SUCC(ret);
  ObCartesianMultipoint *g1_multi_point = NULL;
  ObCartesianMultilinestring *g1_multi_line = NULL;
  ObCartesianMultipolygon *g1_multi_poly = NULL;

  ObCartesianMultipoint *g2_multi_point = NULL;
  ObCartesianMultilinestring *g2_multi_line = NULL;
  ObCartesianMultipolygon *g2_multi_poly = NULL;
  ObGeometry *geo1 = const_cast<ObGeometry *>(reinterpret_cast<const ObGeometry *>(g1));
  ObGeometry *geo2 = const_cast<ObGeometry *>(reinterpret_cast<const ObGeometry *>(g2));
  if (OB_FAIL(ObGeoFuncUtils::ob_gc_prepare<ObCartesianGeometrycollection>(context,
                                                                           geo1,
                                                                           g1_multi_point,
                                                                           g1_multi_line,
                                                                           g1_multi_poly))) {
    LOG_WARN("failed to do gc prepare", K(ret));
  } else if (OB_FAIL(ObGeoFuncUtils::ob_gc_prepare<ObCartesianGeometrycollection>(context,
                                                                                  geo2,
                                                                                  g2_multi_point,
                                                                                  g2_multi_line,
                                                                                  g2_multi_poly))) {
    LOG_WARN("failed to do gc prepare", K(ret));
  } else {
    const ObSrsItem *srs = context.get_srs();
    ObIAllocator *allocator = context.get_allocator();
    ObGeometry *res_geo3 = NULL;
    if (OB_FAIL(do_multi_difference(*srs, *allocator, reinterpret_cast<ObGeometry *>(g1_multi_point),
                                                      reinterpret_cast<ObGeometry *>(g2_multi_point),
                                                      reinterpret_cast<ObGeometry *>(g2_multi_line),
                                                      reinterpret_cast<ObGeometry *>(g2_multi_poly),
                                                      res_geo3))) {
      LOG_WARN("failed to do mulit difference", K(ret));
    } else if (!res_geo3->is_empty()) {
      result = false;
    } else {
      ObGeometry *res_geo5 = NULL;
      if (OB_FAIL(do_multi_difference(*srs, *allocator, reinterpret_cast<ObGeometry *>(g1_multi_line),
                                                        NULL,
                                                        reinterpret_cast<ObGeometry *>(g2_multi_line),
                                                        reinterpret_cast<ObGeometry *>(g2_multi_poly),
                                                        res_geo5))) {
          LOG_WARN("failed to do mulit difference", K(ret));
      } else if (!res_geo5->is_empty()) {
        result = false;
      } else {
        ObGeometry *res_geo6 = NULL;
        if (OB_FAIL(do_multi_difference(*srs, *allocator, reinterpret_cast<ObGeometry *>(g1_multi_poly),
                                                          NULL,
                                                          NULL,
                                                          reinterpret_cast<ObGeometry *>(g2_multi_poly),
                                                          res_geo6))) {
          LOG_WARN("failed to do mulit difference", K(ret));
        } else if (!res_geo6->is_empty()) {
          result = false;
        } else {
          ObGeometry *g1_multi_point_bin = NULL;
          if (OB_FAIL(ObGeoTypeUtil::tree_to_bin(*allocator, g1_multi_point, g1_multi_point_bin, srs))) {
            LOG_WARN("failed to convert geo tree to binary", K(ret));
          } else {
            bool mp_within_gc = false;
            ret = eval_wkb_binary(g1_multi_point_bin, g2, context, mp_within_gc);
            if (OB_FAIL(ret)) {
              LOG_WARN("failed to do within by functor between GeomMultiPoint and GeomCollection", K(ret));
            } else {
              // Checks relation between a pair of geometries defined by a mask.
              bg::de9im::mask mask("T********");
              result = mp_within_gc ||
                       bg::relate(*g1_multi_line, *g2_multi_line, mask) ||
                       bg::relate(*g1_multi_line, *g2_multi_poly, mask) ||
                       bg::relate(*g1_multi_poly, *g2_multi_poly, mask);
            }
          }
        }
      }
    }
  }
  return ret;
} OB_GEO_FUNC_END;

// geog_point
OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeogPoint, ObWkbGeogPoint, bool)
{
  return apply_bg_within<ObWkbGeogPoint, ObWkbGeogPoint>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeogPoint, ObWkbGeogLineString, bool)
{
  return apply_bg_within_pl_pa_strategy<ObWkbGeogPoint, ObWkbGeogLineString>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeogPoint, ObWkbGeogPolygon, bool)
{
  return apply_bg_within_pl_pa_strategy<ObWkbGeogPoint, ObWkbGeogPolygon>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeogPoint, ObWkbGeogMultiPoint, bool)
{
  return apply_bg_within<ObWkbGeogPoint, ObWkbGeogMultiPoint>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeogPoint, ObWkbGeogMultiLineString, bool)
{
  return apply_bg_within_pl_pa_strategy<ObWkbGeogPoint, ObWkbGeogMultiLineString>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeogPoint, ObWkbGeogMultiPolygon, bool)
{
  return apply_bg_within_pl_pa_strategy<ObWkbGeogPoint, ObWkbGeogMultiPolygon>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeogPoint, ObWkbGeogCollection, bool)
{
  return eval_within_geometry_collection<ObWkbGeogCollection>(g1, g2, context, result);
} OB_GEO_FUNC_END;

// geog_linestring
OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeogLineString, ObWkbGeogPoint, bool)
{
  UNUSED(context);
  result = false;
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeogLineString, ObWkbGeogLineString, bool)
{
  return apply_bg_within_ll_la_aa_strategy<ObWkbGeogLineString,
                                           ObWkbGeogLineString>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeogLineString, ObWkbGeogPolygon, bool)
{
  return apply_bg_within_ll_la_aa_strategy<ObWkbGeogLineString,
                                           ObWkbGeogPolygon>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeogLineString, ObWkbGeogMultiPoint, bool)
{
  UNUSED(context);
  result = false;
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeogLineString, ObWkbGeogMultiLineString, bool)
{
  return apply_bg_within_ll_la_aa_strategy<ObWkbGeogLineString,
                                           ObWkbGeogMultiLineString>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeogLineString, ObWkbGeogMultiPolygon, bool)
{
  return apply_bg_within_ll_la_aa_strategy<ObWkbGeogLineString,
                                           ObWkbGeogMultiPolygon>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeogLineString, ObWkbGeogCollection, bool)
{
  return ob_caculate_ml_within_gc_geog<ObWkbGeogLineString, ObGeographGeometrycollection>(g1, g2, context, result);
} OB_GEO_FUNC_END;

// geog_polygon
OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeogPolygon, ObWkbGeogPoint, bool)
{
  UNUSED(context);
  result = false;
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeogPolygon, ObWkbGeogLineString, bool)
{
  UNUSED(context);
  result = false;
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeogPolygon, ObWkbGeogPolygon, bool)
{
  return apply_bg_within_ll_la_aa_strategy<ObWkbGeogPolygon,
                                           ObWkbGeogPolygon>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeogPolygon, ObWkbGeogMultiPoint, bool)
{
  UNUSED(context);
  result = false;
  return OB_SUCCESS;
} OB_GEO_FUNC_END;


OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeogPolygon, ObWkbGeogMultiLineString, bool)
{
  UNUSED(context);
  result = false;
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeogPolygon, ObWkbGeogMultiPolygon, bool)
{
  return apply_bg_within_ll_la_aa_strategy<ObWkbGeogPolygon,
                                           ObWkbGeogMultiPolygon>(g1, g2, context, result);
} OB_GEO_FUNC_END;


OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeogPolygon, ObWkbGeogCollection, bool)
{
  return ob_caculate_mpl_within_gc_geog<ObWkbGeogPolygon, ObGeographGeometrycollection>(g1, g2, context, result);
} OB_GEO_FUNC_END;

// geog_multipoint
OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeogMultiPoint, ObWkbGeogPoint, bool)
{
  return ob_caculate_mp_within_p<ObWkbGeogMultiPoint, ObWkbGeogPoint> (g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeogMultiPoint, ObWkbGeogLineString, bool)
{
  return ob_caculate_mp_within_l_a_geog<ObWkbGeogMultiPoint, ObWkbGeogLineString> (g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeogMultiPoint, ObWkbGeogPolygon, bool)
{
  return ob_caculate_mp_within_l_a_geog<ObWkbGeogMultiPoint, ObWkbGeogPolygon> (g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeogMultiPoint, ObWkbGeogMultiPoint, bool)
{
  return ob_caculate_mp_within_mp<ObWkbGeogMultiPoint> (g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeogMultiPoint, ObWkbGeogMultiLineString, bool)
{
  return ob_caculate_mp_within_l_a_geog<ObWkbGeogMultiPoint,
                                        ObWkbGeogMultiLineString> (g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeogMultiPoint, ObWkbGeogMultiPolygon, bool)
{
  return ob_caculate_mp_within_l_a_geog<ObWkbGeogMultiPoint,
                                        ObWkbGeogMultiPolygon> (g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeogMultiPoint, ObWkbGeogCollection, bool)
{
  return ob_caculate_mp_within_gc<ObWkbGeogPoint, ObIWkbGeogPoint,
                                  ObWkbGeogCollection, ObWkbGeogMultiPoint>(g1, g2, context, result);
} OB_GEO_FUNC_END;

// geog_multilinestring
OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeogMultiLineString, ObWkbGeogPoint, bool)
{
  UNUSED(context);
  result = false;
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeogMultiLineString, ObWkbGeogLineString, bool)
{
  return apply_bg_within_ll_la_aa_strategy<ObWkbGeogMultiLineString,
                                           ObWkbGeogLineString>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeogMultiLineString, ObWkbGeogPolygon, bool)
{
  return apply_bg_within_ll_la_aa_strategy<ObWkbGeogMultiLineString,
                                           ObWkbGeogPolygon>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeogMultiLineString, ObWkbGeogMultiPoint, bool)
{
  UNUSED(context);
  result = false;
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeogMultiLineString, ObWkbGeogMultiLineString, bool)
{
  return apply_bg_within_ll_la_aa_strategy<ObWkbGeogMultiLineString,
                                           ObWkbGeogMultiLineString>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeogMultiLineString, ObWkbGeogMultiPolygon, bool)
{
  return apply_bg_within_ll_la_aa_strategy<ObWkbGeogMultiLineString,
                                           ObWkbGeogMultiPolygon>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeogMultiLineString, ObWkbGeogCollection, bool)
{
  return ob_caculate_ml_within_gc_geog<ObWkbGeogMultiLineString, ObGeographGeometrycollection>(g1, g2, context, result);
} OB_GEO_FUNC_END;

// geog_multipolygon
OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeogMultiPolygon, ObWkbGeogPoint, bool)
{
  UNUSED(context);
  result = false;
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeogMultiPolygon, ObWkbGeogLineString, bool)
{
  UNUSED(context);
  result = false;
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeogMultiPolygon, ObWkbGeogPolygon, bool)
{
  return apply_bg_within_ll_la_aa_strategy<ObWkbGeogMultiPolygon,
                                           ObWkbGeogPolygon>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeogMultiPolygon, ObWkbGeogMultiPoint, bool)
{
  UNUSED(context);
  result = false;
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeogMultiPolygon, ObWkbGeogMultiLineString, bool)
{
  UNUSED(context);
  result = false;
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeogMultiPolygon, ObWkbGeogMultiPolygon, bool)
{
  return apply_bg_within_ll_la_aa_strategy<ObWkbGeogMultiPolygon,
                                           ObWkbGeogMultiPolygon>(g1, g2, context, result);
} OB_GEO_FUNC_END;


OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeogMultiPolygon, ObWkbGeogCollection, bool)
{
  return ob_caculate_mpl_within_gc_geog<ObWkbGeogMultiPolygon, ObGeographGeometrycollection>(g1, g2, context, result);
} OB_GEO_FUNC_END;

// geog_geometrycollection
OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeogCollection, ObWkbGeogPoint, bool)
{
  return ob_caculate_gc_within_p<ObWkbGeogPoint, ObGeographGeometrycollection>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeogCollection, ObWkbGeogLineString, bool)
{
  INIT_SUCC(ret);
  ObGeographMultipoint *multi_point = NULL;
  ObGeographMultilinestring *multi_line = NULL;
  ObGeographMultipolygon *multi_poly = NULL;
  common::ObIAllocator *allocator = context.get_allocator();
  ObGeometry *geo1 = const_cast<ObGeometry *>(reinterpret_cast<const ObGeometry *>(g1));
  const ObWkbGeogLineString *geo2 = reinterpret_cast<const ObWkbGeogLineString *>(g2->val());
  ObGeometry *multi_point_bin = NULL;
  const ObSrsItem *srs = context.get_srs();
  if (OB_ISNULL(srs)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("srs is null in geographic eval", K(ret));
  } else if (OB_FAIL(ObGeoFuncUtils::ob_gc_prepare<ObGeographGeometrycollection>(context,
                                                                                 geo1,
                                                                                 multi_point,
                                                                                 multi_line,
                                                                                 multi_poly))) {
    LOG_WARN("failed to do gc prepare", K(ret));
  } else if (!multi_poly->empty()) {
    result = false;
  } else if (OB_FAIL(ObGeoTypeUtil::tree_to_bin(*allocator, multi_point, multi_point_bin, srs))) {
    LOG_WARN("failed to convert geo tree to binary", K(ret));
  } else {
    bg::srs::spheroid<double> geog_sphere(srs->semi_major_axis(), srs->semi_minor_axis());
    ObPlPaStrategy geog_pl_pa_strategy(geog_sphere);
    ObLlLaAaStrategy geog_ll_la_aa_strategy(geog_sphere);
    bool mp_within_l = false;
    ret = eval_wkb_binary(multi_point_bin, g2, context, mp_within_l);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to do within by functor between GeogMultiPoint and GeogLineString", K(ret));
    } else if (mp_within_l) {
      result = multi_line->empty() ||
#ifdef USE_SPHERE_GEO
               bg::covered_by(*multi_line, *geo2, geog_ll_la_aa_strategy);
    } else if (bg::within(*multi_line, *geo2, geog_ll_la_aa_strategy)){
#else
               bg::covered_by(*multi_line, *geo2);
    } else if (bg::within(*multi_line, *geo2)){
#endif
      bool covered = true;
      ObGeographMultipoint::iterator iter = multi_point->begin();
      for (; iter != multi_point->end() && covered; ++iter) {
        ObGeographMultipoint::value_type& point = *iter;
#ifdef USE_SPHERE_GEO
        covered = bg::covered_by(point, *geo2, geog_pl_pa_strategy);
#else
        covered = bg::covered_by(point, *geo2);
#endif
      }
      result = covered;
    } else {
      result = false;
    }
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeogCollection, ObWkbGeogPolygon, bool)
{
  INIT_SUCC(ret);
  ObGeographMultipoint *multi_point = NULL;
  ObGeographMultilinestring *multi_line = NULL;
  ObGeographMultipolygon *multi_poly = NULL;
  common::ObIAllocator *allocator = context.get_allocator();
  ObGeometry *geo1 = const_cast<ObGeometry *>(reinterpret_cast<const ObGeometry *>(g1));
  const ObWkbGeogPolygon *geo2 = reinterpret_cast<const ObWkbGeogPolygon *>(g2->val());
  ObGeometry *multi_point_bin = NULL;
  const ObSrsItem *srs = context.get_srs();
  if (OB_ISNULL(srs)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("srs is null in geographic eval", K(ret));
  } else if (OB_FAIL(ObGeoFuncUtils::ob_gc_prepare<ObGeographGeometrycollection>(context,
                                                                                 geo1,
                                                                                 multi_point,
                                                                                 multi_line,
                                                                                 multi_poly))) {
    LOG_WARN("failed to do gc prepare", K(ret));
  } else if (OB_FAIL(ObGeoTypeUtil::tree_to_bin(*allocator, multi_point, multi_point_bin, srs))) {
    LOG_WARN("failed to convert geo tree to binary", K(ret));
  } else {
    bg::srs::spheroid<double> geog_sphere(srs->semi_major_axis(), srs->semi_minor_axis());
    ObPlPaStrategy geog_pl_pa_strategy(geog_sphere);
    ObLlLaAaStrategy geog_ll_la_aa_strategy(geog_sphere);

    bool mp_within_l = false;
    ret = eval_wkb_binary(multi_point_bin, g2, context, mp_within_l);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to do within by functor between multipoint and polygon", K(ret));
    } else if (mp_within_l) {
      result = (multi_line->empty() ||
#ifdef USE_SPHERE_GEO
               bg::covered_by(*multi_line, *geo2, geog_ll_la_aa_strategy)) &&
               (multi_poly->empty() ||
               bg::covered_by(*multi_poly, *geo2, geog_ll_la_aa_strategy));
    } else if (bg::within(*multi_line, *geo2, geog_ll_la_aa_strategy)) {
#else
               bg::covered_by(*multi_line, *geo2)) &&
               (multi_poly->empty() ||
               bg::covered_by(*multi_poly, *geo2));
    } else if (bg::within(*multi_line, *geo2)) {
#endif
      bool covered = true;
      ObGeographMultipoint::iterator iter = multi_point->begin();
      for (; iter != multi_point->end() && covered; ++iter) {
        ObGeographMultipoint::value_type& point = *iter;
#ifdef USE_SPHERE_GEO
        covered = bg::covered_by(point, *geo2, geog_pl_pa_strategy);
#else
        covered = bg::covered_by(point, *geo2);
#endif
      }
      result = !covered ? false :
               (multi_poly->empty() ||
#ifdef USE_SPHERE_GEO
               bg::covered_by(*multi_poly, *geo2, geog_ll_la_aa_strategy));
    } else if (bg::within(*multi_poly, *geo2, geog_ll_la_aa_strategy)) {
#else
               bg::covered_by(*multi_poly, *geo2));
    } else if (bg::within(*multi_poly, *geo2)) {
#endif
      bool covered = true;
      ObGeographMultipoint::iterator iter = multi_point->begin();
      for (; iter != multi_point->end() && covered; ++iter) {
        ObGeographMultipoint::value_type& point = *iter;
#ifdef USE_SPHERE_GEO
        covered = bg::covered_by(point, *geo2, geog_pl_pa_strategy);
#else
        covered = bg::covered_by(point, *geo2);
#endif
      }
      result = !covered ? false :
               (multi_line->empty() ||
#ifdef USE_SPHERE_GEO
               bg::covered_by(*multi_line, *geo2, geog_ll_la_aa_strategy));
#else
               bg::covered_by(*multi_line, *geo2));
#endif
    } else {
      result = false;
    }
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeogCollection, ObWkbGeogMultiPoint, bool)
{
  return ob_caculate_gc_within_mpt<ObGeographGeometrycollection>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeogCollection, ObWkbGeogMultiLineString, bool)
{
  INIT_SUCC(ret);
  ObGeographMultipoint *multi_point = NULL;
  ObGeographMultilinestring *multi_line = NULL;
  ObGeographMultipolygon *multi_poly = NULL;
  common::ObIAllocator *allocator = context.get_allocator();
  ObGeometry *geo1 = const_cast<ObGeometry *>(reinterpret_cast<const ObGeometry *>(g1));
  const ObWkbGeogMultiLineString *geo2 = reinterpret_cast<const ObWkbGeogMultiLineString *>(g2->val());
  ObGeometry *multi_point_bin = NULL;
  const ObSrsItem *srs = context.get_srs();
  if (OB_ISNULL(srs)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("srs is null in geographic eval", K(ret));
  } else if (OB_FAIL(ObGeoFuncUtils::ob_gc_prepare<ObGeographGeometrycollection>(context,
                                                                                 geo1,
                                                                                 multi_point,
                                                                                 multi_line,
                                                                                 multi_poly))) {
    LOG_WARN("failed to do gc prepare", K(ret));
  } else if (!multi_poly->empty()) {
    result = false;
  } else if (OB_FAIL(ObGeoTypeUtil::tree_to_bin(*allocator, multi_point, multi_point_bin, srs))) {
    LOG_WARN("failed to convert geo tree to binary", K(ret));
  } else {
    bg::srs::spheroid<double> geog_sphere(srs->semi_major_axis(), srs->semi_minor_axis());
    ObPlPaStrategy geog_pl_pa_strategy(geog_sphere);
    ObLlLaAaStrategy geog_ll_la_aa_strategy(geog_sphere);

    bool mp_within_l = false;
    ret = eval_wkb_binary(multi_point_bin, g2, context, mp_within_l);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to do within by functor between GeogMultiPoint and GeogMultiLineString", K(ret));
    } else if (mp_within_l) {
#ifdef USE_SPHERE_GEO
      result = multi_line->empty() ||
               bg::covered_by(*multi_line, *geo2, geog_ll_la_aa_strategy);
    } else if (bg::within(*multi_line, *geo2, geog_ll_la_aa_strategy)){
#else
      result = multi_line->empty() ||
               bg::covered_by(*multi_line, *geo2);
    } else if (bg::within(*multi_line, *geo2)){
#endif
      bool covered = true;
      ObGeographMultipoint::iterator iter = multi_point->begin();
      for (; iter != multi_point->end() && covered; ++iter) {
        typename ObGeographMultipoint::value_type& point = *iter;
#ifdef USE_SPHERE_GEO
        covered = bg::covered_by(point, *geo2, geog_pl_pa_strategy);
#else
        covered = bg::covered_by(point, *geo2);
#endif
      }
      result = covered;
    } else {
      result = false;
    }
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeogCollection, ObWkbGeogMultiPolygon, bool)
{
  INIT_SUCC(ret);
  ObGeographMultipoint *multi_point = NULL;
  ObGeographMultilinestring *multi_line = NULL;
  ObGeographMultipolygon *multi_poly = NULL;
  common::ObIAllocator *allocator = context.get_allocator();
  ObGeometry *geo1 = const_cast<ObGeometry *>(reinterpret_cast<const ObGeometry *>(g1));
  const ObWkbGeogMultiPolygon *geo2 = reinterpret_cast<const ObWkbGeogMultiPolygon *>(g2->val());
  ObGeometry *multi_point_bin = NULL;
  const ObSrsItem *srs = context.get_srs();
  if (OB_ISNULL(srs)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("srs is null in geographic eval", K(ret));
  } else if (OB_FAIL(ObGeoFuncUtils::ob_gc_prepare<ObGeographGeometrycollection>(context,
                                                                                 geo1,
                                                                                 multi_point,
                                                                                 multi_line,
                                                                                 multi_poly))) {
    LOG_WARN("failed to do gc prepare", K(ret));
  } else if (OB_FAIL(ObGeoTypeUtil::tree_to_bin(*allocator, multi_point, multi_point_bin, srs))) {
    LOG_WARN("failed to convert geo tree to binary", K(ret));
  } else {
    bg::srs::spheroid<double> geog_sphere(srs->semi_major_axis(), srs->semi_minor_axis());
    ObPlPaStrategy geog_pl_pa_strategy(geog_sphere);
    ObLlLaAaStrategy geog_ll_la_aa_strategy(geog_sphere);

    bool mp_within_poly = false;
    ret = eval_wkb_binary(multi_point_bin, g2, context, mp_within_poly);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to do within by functor between multipoint and polygon", K(ret));
    } else if (mp_within_poly) {
#ifdef USE_SPHERE_GEO
      result = (multi_line->empty() ||
               bg::covered_by(*multi_line, *geo2, geog_ll_la_aa_strategy)) &&
               (multi_poly->empty() ||
               bg::covered_by(*multi_poly, *geo2, geog_ll_la_aa_strategy));
    } else if (bg::within(*multi_line, *geo2, geog_ll_la_aa_strategy)){
#else
      result = (multi_line->empty() ||
               bg::covered_by(*multi_line, *geo2)) &&
               (multi_poly->empty() ||
               bg::covered_by(*multi_poly, *geo2));
    } else if (bg::within(*multi_line, *geo2)){
#endif
      bool covered = true;
      ObGeographMultipoint::iterator iter = multi_point->begin();
      for (; iter != multi_point->end() && covered; ++iter) {
        ObGeographMultipoint::value_type& point = *iter;
#ifdef USE_SPHERE_GEO
        covered = bg::covered_by(point, *geo2, geog_pl_pa_strategy);
#else
        covered = bg::covered_by(point, *geo2);
#endif
      }
      result = !covered ? false :
               (multi_poly->empty() ||
#ifdef USE_SPHERE_GEO
               bg::covered_by(*multi_poly, *geo2, geog_ll_la_aa_strategy));
    } else if (bg::within(*multi_poly, *geo2, geog_ll_la_aa_strategy)) {
#else
               bg::covered_by(*multi_poly, *geo2));
    } else if (bg::within(*multi_poly, *geo2)) {
#endif
      bool covered = true;
      ObGeographMultipoint::iterator iter = multi_point->begin();
      for (; iter != multi_point->end() && covered; ++iter) {
        ObGeographMultipoint::value_type& point = *iter;
#ifdef USE_SPHERE_GEO
        covered = bg::covered_by(point, *geo2, geog_pl_pa_strategy);
#else
        covered = bg::covered_by(point, *geo2);
#endif
      }
      result = !covered ? false :
               (multi_line->empty() ||
#ifdef USE_SPHERE_GEO
               bg::covered_by(*multi_line, *geo2, geog_ll_la_aa_strategy));
#else
               bg::covered_by(*multi_line, *geo2));
#endif
    } else {
      result = false;
    }
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncWithinImpl, ObWkbGeogCollection, ObWkbGeogCollection, bool)
{
  INIT_SUCC(ret);
  ObGeographMultipoint *g1_multi_point = NULL;
  ObGeographMultilinestring *g1_multi_line = NULL;
  ObGeographMultipolygon *g1_multi_poly = NULL;
  ObGeographMultipoint *g2_multi_point = NULL;
  ObGeographMultilinestring *g2_multi_line = NULL;
  ObGeographMultipolygon *g2_multi_poly = NULL;
  ObGeometry *geo1 = const_cast<ObGeometry *>(reinterpret_cast<const ObGeometry *>(g1));
  ObGeometry *geo2 = const_cast<ObGeometry *>(reinterpret_cast<const ObGeometry *>(g2));
  const ObSrsItem *srs = context.get_srs();
  if (OB_ISNULL(srs)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("srs is null in geographic eval", K(ret));
  } else if (OB_FAIL(ObGeoFuncUtils::ob_gc_prepare<ObGeographGeometrycollection>(context,
                                                                          geo1,
                                                                          g1_multi_point,
                                                                          g1_multi_line,
                                                                          g1_multi_poly))) {
    LOG_WARN("failed to do gc prepare", K(ret));
  } else if (OB_FAIL(ObGeoFuncUtils::ob_gc_prepare<ObGeographGeometrycollection>(context,
                                                                                 geo2,
                                                                                 g2_multi_point,
                                                                                 g2_multi_line,
                                                                                 g2_multi_poly))) {
    LOG_WARN("failed to do gc prepare", K(ret));
  } else {
    bg::srs::spheroid<double> geog_sphere(srs->semi_major_axis(), srs->semi_minor_axis());
    ObPlPaStrategy geog_pl_pa_strategy(geog_sphere);
    ObLlLaAaStrategy geog_ll_la_aa_strategy(geog_sphere);

    ObIAllocator *allocator = context.get_allocator();

    ObGeometry *res_geo3 = NULL;
    if (OB_FAIL(do_multi_difference(*srs, *allocator, reinterpret_cast<ObGeometry *>(g1_multi_point),
                                                      reinterpret_cast<ObGeometry *>(g2_multi_point),
                                                      reinterpret_cast<ObGeometry *>(g2_multi_line),
                                                      reinterpret_cast<ObGeometry *>(g2_multi_poly),
                                                      res_geo3))) {
      LOG_WARN("failed to do mulit difference", K(ret));
    } else if (!res_geo3->is_empty()) {
      result = false;
    } else {
      ObGeometry *res_geo5 = NULL;
      if (OB_FAIL(do_multi_difference(*srs, *allocator, reinterpret_cast<ObGeometry *>(g1_multi_line),
                                                        NULL,
                                                        reinterpret_cast<ObGeometry *>(g2_multi_line),
                                                        reinterpret_cast<ObGeometry *>(g2_multi_poly),
                                                        res_geo5))) {
        LOG_WARN("failed to do mulit difference", K(ret));
      } else if (!res_geo5->is_empty()) {
        result = false;
      } else {
        ObGeometry * res_geo6 = NULL;
        if (OB_FAIL(do_multi_difference(*srs, *allocator, reinterpret_cast<ObGeometry *>(g1_multi_poly),
                                        NULL,
                                        NULL,
                                        reinterpret_cast<ObGeometry *>(g2_multi_poly),
                                        res_geo6))) {
          LOG_WARN("failed to do mulit difference", K(ret));
        } else if (!res_geo6->is_empty()) {
          result = false;
        } else {
          ObGeometry *g1_multi_point_bin = NULL;
          if (OB_FAIL(ObGeoTypeUtil::tree_to_bin(*allocator, g1_multi_point, g1_multi_point_bin, srs))) {
            LOG_WARN("failed to convert geo tree to binary", K(ret));
          } else {
            bool mp_within_gc = false;
            ret = eval_wkb_binary(g1_multi_point_bin, g2, context, mp_within_gc);
            if (OB_FAIL(ret)) {
              LOG_WARN("failed to do within by functor between GeogMultiPoint and GeogCollection", K(ret));
            } else {
              // Checks relation between a pair of geometries defined by a mask.
              bg::de9im::mask mask("T********");
#ifdef USE_SPHERE_GEO
              result = mp_within_gc ||
                       bg::relate(*g1_multi_line, *g2_multi_line, mask, geog_ll_la_aa_strategy) ||
                       bg::relate(*g1_multi_line, *g2_multi_poly, mask, geog_ll_la_aa_strategy) ||
                       bg::relate(*g1_multi_poly, *g2_multi_poly, mask, geog_ll_la_aa_strategy);
#else
              result = mp_within_gc ||
                       bg::relate(*g1_multi_line, *g2_multi_line, mask) ||
                       bg::relate(*g1_multi_line, *g2_multi_poly, mask) ||
                       bg::relate(*g1_multi_poly, *g2_multi_poly, mask);
#endif
            }
          }
        }
      }
    }
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_CART_TREE_FUNC_BEGIN(ObGeoFuncWithinImpl, ObCartesianPoint, ObCartesianPolygon, bool)
{
  return apply_bg_within<ObCartesianPoint, ObCartesianPolygon>(g1, g2, context, result);
} OB_GEO_FUNC_END;

int ObGeoFuncWithin::eval(const ObGeoEvalCtx &gis_context, bool &result)
{
  return ObGeoFuncWithinImpl::eval_geo_func(gis_context, result);
}

} // sql
} // oceanbase