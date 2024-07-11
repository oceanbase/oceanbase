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
 * This file contains implementation for ob_geo_func_intersections.
 */

#define USING_LOG_PREFIX LIB

#include "lib/geo/ob_geo_dispatcher.h"
#include "lib/geo/ob_geo_func_intersections.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/geo/ob_geo_func_utils.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace common
{

template <typename MPt, typename MLs, typename MPy, typename GeometryRes>
static int remove_overlapping(MPt const &mpt, MLs const &mls, MPy const &mpy,
                               const ObGeoEvalCtx &context, const uint32_t srid, GeometryRes &result){
  INIT_SUCC(ret);
  ObIAllocator *allocator = context.get_allocator();

  MPt *mpt_mls_difference = OB_NEWx(MPt, allocator,
                                    srid, *allocator);
  if (OB_ISNULL(mpt_mls_difference)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create geo by type", K(ret));
  } else
    boost::geometry::difference(mpt, mls, *mpt_mls_difference);
  
  MPt *mpt_mls_mpy_difference = nullptr;
  if (OB_SUCC(ret)) {
    mpt_mls_mpy_difference = OB_NEWx(MPt, allocator,
                                    srid, *allocator);
    if (OB_ISNULL(mpt_mls_mpy_difference)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to create geo by type", K(ret));
    } else {
      boost::geometry::difference(*mpt_mls_difference, mpy, *mpt_mls_mpy_difference);

      if (!mpt_mls_mpy_difference->is_empty()) {
        result.push_back(*mpt_mls_mpy_difference);
      }
    }
  }

  MLs *mls_mpy_difference = nullptr;
  if (OB_SUCC(ret)) {
    mls_mpy_difference = OB_NEWx(MLs, allocator,
                                 srid, *allocator);
    if (OB_ISNULL(mls_mpy_difference)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to create geo by type", K(ret));
    } else {
      boost::geometry::difference(mls, mpy, *mls_mpy_difference);
      if (!mls_mpy_difference->is_empty()) {
        result.push_back(*mls_mpy_difference);
      }

      if (!mpy.is_empty()) {
        MPy *mpy_res = OB_NEWx(MPy, allocator,
                               srid, *allocator);
        if (OB_ISNULL(mpy_res)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc memory", K(ret));
        }
        for (int i = 0; OB_SUCC(ret) && i < mpy.size(); ++i) {
          if (OB_FAIL(mpy_res->push_back(mpy[i]))) {
            LOG_WARN("failed to add point to multipoint", K(ret));
          }
        }
        result.push_back(*mpy_res);
      }
    }
  }
  return ret;
}

static int remove_overlapping_cart(ObCartesianMultipoint const &mpt, ObCartesianMultilinestring const &mls,
                                   const ObGeoEvalCtx &context, ObCartesianGeometrycollection &result){
  INIT_SUCC(ret);
  ObIAllocator *allocator = context.get_allocator();

  if (!mls.is_empty()) {
    for (int i = 0; i < mls.size(); ++i) {
      result.push_back(mls[i]);
    }
  }

  if (!mpt.is_empty()) {
    ObCartesianMultipoint *tmp_mpt = OB_NEWx(ObCartesianMultipoint, allocator,
                                             mpt.get_srid(), *allocator);
    if (OB_ISNULL(tmp_mpt)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to create geo by type", K(ret));
    } else
      boost::geometry::difference(mpt, mls, *tmp_mpt);

    if (!OB_ISNULL(tmp_mpt) && !tmp_mpt->is_empty()) {
      for (int i = 0; i < tmp_mpt->size(); ++i) {
        ObWkbGeomInnerPoint *ptr = &(*tmp_mpt)[i];
        ObCartesianPoint *pt = OB_NEWx(ObCartesianPoint, allocator, ptr->get<0>(),
                                       ptr->get<1>(), mpt.get_srid(), allocator);
        result.push_back(*pt);
      }
      allocator->free(tmp_mpt);
    }
  }
  return ret;
}

static int remove_overlapping_geog(ObGeographMultipoint const &mpt, ObGeographMultilinestring const &mls,
                                   const ObGeoEvalCtx &context, ObGeographGeometrycollection &result){
  INIT_SUCC(ret);
  ObIAllocator *allocator = context.get_allocator();

  if (!mls.is_empty()) {
    for (int i = 0; i < mls.size(); ++i) {
      result.push_back(mls[i]);
    }
  }

  if (!mpt.is_empty()) {
    ObGeographMultipoint *tmp_mpt = OB_NEWx(ObGeographMultipoint, allocator,
                                            mpt.get_srid(), *allocator);
    if (OB_ISNULL(tmp_mpt)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to create geo by type", K(ret));
    } else
      boost::geometry::difference(mpt, mls, *tmp_mpt);

    if (!OB_ISNULL(tmp_mpt) && !tmp_mpt->is_empty()) {
      for (int i = 0; i < tmp_mpt->size(); ++i) {
        ObWkbGeogInnerPoint *ptr = &(*tmp_mpt)[i];
        ObGeographPoint *pt = OB_NEWx(ObGeographPoint, allocator, ptr->get<0>(),
                                      ptr->get<1>(), mpt.get_srid(), allocator);
        result.push_back(*pt);
      }
      allocator->free(tmp_mpt);
    }
  }
  return ret;
}

template <typename MPt, typename MLs, typename MPy, 
          typename GeoType1, typename GeoType2, typename GeometryRes>
static int apply_bg_intersections(
    const ObGeometry *g1, const ObGeometry *g2, const ObGeoEvalCtx &context, ObGeometry *&result)
{
  INIT_SUCC(ret);
  GeometryRes *res = OB_NEWx(GeometryRes, context.get_allocator(),
                             g1->get_srid(), *context.get_allocator());

  if (OB_ISNULL(res)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create go by type", K(ret));
  } else {
    const GeoType1 *geo1 = nullptr;
    const GeoType2 *geo2 = nullptr;
    if (g1->is_tree()) {
      geo1 = reinterpret_cast<const GeoType1 *>(g1);
      geo2 = reinterpret_cast<const GeoType2 *>(g2);
    } else {
      geo1 = reinterpret_cast<const GeoType1 *>(g1->val());
      geo2 = reinterpret_cast<const GeoType2 *>(g2->val());
    }
    if (OB_ISNULL(geo1) || OB_ISNULL(geo2)) {
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_WARN("wrong geometry type", K(ret));
    } else {
      std::tuple<MPt, MLs, MPy> bg_result;
      
      boost::geometry::intersection(*geo1, *geo2, bg_result);

      remove_overlapping(std::get<0>(bg_result), std::get<1>(bg_result),
                         std::get<2>(bg_result), context, g1->get_srid(), *res);

      result = res;
    }
  }
  return ret;
}

template <typename MPt, typename MLs, typename MPy, typename Strategy,
          typename GeoType1, typename GeoType2, typename GeometryRes>
static int apply_bg_intersections_with_strategy(
    const ObGeometry *g1, const ObGeometry *g2, const ObGeoEvalCtx &context, ObGeometry *&result)
{
  INIT_SUCC(ret);
  const ObSrsItem *srs = context.get_srs();
  GeometryRes *res = OB_NEWx(GeometryRes,  context.get_allocator(),
                             g1->get_srid(), *context.get_allocator());

  if (OB_ISNULL(srs)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("srs is null", K(ret));
  } else if (OB_ISNULL(res)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create geo by type", K(ret));
  } else {
    boost::geometry::srs::spheroid<double> geog_sphere(
        srs->semi_major_axis(), srs->semi_minor_axis());
    Strategy cur_strategy(geog_sphere);
    const GeoType1 *geo1 = nullptr;
    const GeoType2 *geo2 = nullptr;
    if (g1->is_tree()) {
      geo1 = reinterpret_cast<const GeoType1 *>(g1);
      geo2 = reinterpret_cast<const GeoType2 *>(g2);
    } else {
      geo1 = reinterpret_cast<const GeoType1 *>(g1->val());
      geo2 = reinterpret_cast<const GeoType2 *>(g2->val());
    }
    if (OB_ISNULL(geo1) || OB_ISNULL(geo2)) {
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_WARN("wrong geometry type", K(ret));
    } else {
      std::tuple<MPt, MLs, MPy> bg_result;

      boost::geometry::intersection(*geo1, *geo2, bg_result, cur_strategy);

      remove_overlapping(std::get<0>(bg_result), std::get<1>(bg_result),
                         std::get<2>(bg_result), context, g1->get_srid(), *res);
      result = res;
    }
  }
  return ret;
}

template <typename GeoType1, typename GeoType2>
static int apply_brute_force_intersections_cart(const ObGeometry *g1, const ObGeometry *g2, 
    const ObGeoEvalCtx &context, ObGeometry *&result) 
{
  INIT_SUCC(ret);
  ObIAllocator *allocator = context.get_allocator();
  ObCartesianGeometrycollection *res = OB_NEWx(ObCartesianGeometrycollection, allocator,
                                              g1->get_srid(), *allocator);

  if (OB_ISNULL(res)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create go by type", K(ret));
  } else {
    const GeoType1 *geo1 = nullptr;
    const GeoType2 *geo2 = nullptr;
    if (g1->is_tree()) {
      geo1 = reinterpret_cast<const GeoType1 *>(g1);
      geo2 = reinterpret_cast<const GeoType2 *>(g2);
    } else {
      geo1 = reinterpret_cast<const GeoType1 *>(g1->val());
      geo2 = reinterpret_cast<const GeoType2 *>(g2->val());
    }
    if (OB_ISNULL(geo1) || OB_ISNULL(geo2)) {
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_WARN("wrong geometry type", K(ret));
    } else {
      ObCartesianMultipoint *tmp_res_mpt = OB_NEWx(ObCartesianMultipoint, allocator,
                                                   g1->get_srid(), *allocator);
      if (OB_ISNULL(tmp_res_mpt)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to create geo by type", K(ret));
      } else {
        boost::geometry::intersection(*geo1, *geo2, *tmp_res_mpt);
      }

      ObCartesianMultilinestring *tmp_res_ml = nullptr;
      if (OB_SUCC(ret)) {
        tmp_res_ml = OB_NEWx(ObCartesianMultilinestring, allocator,
                             g1->get_srid(), *allocator);
        if (OB_ISNULL(tmp_res_ml)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to create geo by type", K(ret));
        } else {
          boost::geometry::intersection(*geo1, *geo2, *tmp_res_ml);
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(remove_overlapping_cart(*tmp_res_mpt, *tmp_res_ml, context, *res))){
          LOG_WARN("fail to remove overlapping geometry", K(ret));
        } else
          result = res;
      }
    }
  }
  return ret;
}

template <typename GeoType1, typename GeoType2>
static int apply_brute_force_intersections_geog(const ObGeometry *g1, const ObGeometry *g2, 
    const ObGeoEvalCtx &context, ObGeometry *&result) 
{
  INIT_SUCC(ret);
  ObIAllocator *allocator = context.get_allocator();
  ObGeographGeometrycollection *res = OB_NEWx(ObGeographGeometrycollection, allocator,
                                              g1->get_srid(), *allocator);

  if (OB_ISNULL(res)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create go by type", K(ret));
  } else {
    const GeoType1 *geo1 = nullptr;
    const GeoType2 *geo2 = nullptr;
    if (g1->is_tree()) {
      geo1 = reinterpret_cast<const GeoType1 *>(g1);
      geo2 = reinterpret_cast<const GeoType2 *>(g2);
    } else {
      geo1 = reinterpret_cast<const GeoType1 *>(g1->val());
      geo2 = reinterpret_cast<const GeoType2 *>(g2->val());
    }
    if (OB_ISNULL(geo1) || OB_ISNULL(geo2)) {
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_WARN("wrong geometry type", K(ret));
    } else {
      ObGeographMultipoint *tmp_res_mpt = OB_NEWx(ObGeographMultipoint, allocator,
                                                  g1->get_srid(), *allocator);
      if (OB_ISNULL(tmp_res_mpt)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to create geo by type", K(ret));
      } else {
        const ObSrsItem *srs = context.get_srs();
        boost::geometry::srs::spheroid<double> geog_sphere(
            srs->semi_major_axis(), srs->semi_minor_axis());

        ObLlLaAaStrategy line_strategy(geog_sphere);
        boost::geometry::intersection(*geo1, *geo2, *tmp_res_mpt, line_strategy);
      }

      ObGeographMultilinestring *tmp_res_ml = nullptr;
      if (OB_SUCC(ret)) {
        tmp_res_ml = OB_NEWx(ObGeographMultilinestring, allocator,
                             g1->get_srid(), *allocator);
        if (OB_ISNULL(tmp_res_ml)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to create geo by type", K(ret));
        } else {
          const ObSrsItem *srs = context.get_srs();
          boost::geometry::srs::spheroid<double> geog_sphere(
              srs->semi_major_axis(), srs->semi_minor_axis());

          ObLlLaAaStrategy line_strategy(geog_sphere);
          boost::geometry::intersection(*geo1, *geo2, *tmp_res_ml, line_strategy);
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(remove_overlapping_geog(*tmp_res_mpt, *tmp_res_ml, context, *res))){
          LOG_WARN("fail to remove overlapping geometry", K(ret));
        } else
          result = res;
      }
    }
  }
  return ret;
}

/**************************************  Impl  **************************************/
class ObGeoFuncIntersectionsImpl : public ObIGeoDispatcher<ObGeometry *, ObGeoFuncIntersectionsImpl>
{
public:
  ObGeoFuncIntersectionsImpl();
  virtual ~ObGeoFuncIntersectionsImpl() = default;

  OB_GEO_UNARY_FUNC_DEFAULT(ObGeometry *, OB_ERR_GIS_INVALID_DATA);
  OB_GEO_TREE_UNARY_FUNC_DEFAULT(ObGeometry *, OB_ERR_GIS_INVALID_DATA);

  OB_GEO_CART_TREE_FUNC_DEFAULT(ObGeometry *, OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS);
  OB_GEO_GEOG_TREE_FUNC_DEFAULT(ObGeometry *, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);

  OB_GEO_CART_BINARY_FUNC_DEFAULT(ObGeometry *, OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS);
  OB_GEO_GEOG_BINARY_FUNC_DEFAULT(ObGeometry *, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);

private:
  template<typename GcTreeType, typename PtType, typename GcBinType>
  static int eval_intersections_gc(
      const ObGeometry *g1, const ObGeometry *g2, const ObGeoEvalCtx &context, ObGeometry *&result)
  {
    INIT_SUCC(ret);
    
    if (g1->type() != ObGeoType::GEOMETRYCOLLECTION && 
        g2->type() != ObGeoType::GEOMETRYCOLLECTION) {
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_WARN("At least one of g1 and g2 is collection", K(ret), K(g1->type()), K(g2->type()));
    } else {
      ObIAllocator *allocator = context.get_allocator();
      GcTreeType *res_coll = OB_NEWx(GcTreeType, allocator, g1->get_srid(), *allocator);
      ObGeometry *geo1 = const_cast<ObGeometry *>(g1);
      ObGeometry *geo2 = const_cast<ObGeometry *>(g2);
      bool is_g1_empty = false;
      bool is_g2_empty = false;
      if (OB_ISNULL(res_coll)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failt alloc memory for geometry", K(ret));
      } else if (OB_FAIL(ObGeoTypeUtil::check_empty(geo2, is_g1_empty))) {
        LOG_WARN("fail to check is g1 empty", K(ret));
      } else if (OB_FAIL(ObGeoTypeUtil::check_empty(geo2, is_g2_empty))) {
        LOG_WARN("fail to check is g2 empty", K(ret));
      } else if (is_g1_empty || is_g2_empty) {
        // is empty
      } else {
        typename GcTreeType::sub_mpt_type *mpt2 = nullptr;
        typename GcTreeType::sub_ml_type *mls2 = nullptr;
        typename GcTreeType::sub_mp_type *mpy2 = nullptr;
        if (OB_FAIL(ObGeoFuncUtils::ob_gc_prepare<GcTreeType>(context, geo2, mpt2, mls2, mpy2))) {
          LOG_WARN("failed to prepare gc", K(ret));
        } else if (OB_ISNULL(mpt2) || OB_ISNULL(mls2) || OB_ISNULL(mpy2)) {
          ret = OB_ERR_NULL_VALUE;
          LOG_WARN("unexpected null geometry collection split", K(ret), K(mpt2), K(mls2), K(mpy2), K(res_coll));
        } else {
          bool mpy_empty = mpy2->is_empty();
          bool mls_empty = mls2->is_empty();
          bool mpt_empty = mpt2->is_empty();
          const ObSrsItem *srs = context.get_srs();

          if (!mpy_empty) {
            ObGeometry *mpy_bin = nullptr;
            ObGeometry *mpy_result = nullptr;
            if (OB_FAIL(ObGeoTypeUtil::tree_to_bin(*allocator, mpy2, mpy_bin, srs))) {
              LOG_WARN("fail to transform tree to binary", K(ret));
            } else if (OB_FAIL(eval_wkb_binary(g1, mpy_bin, context, mpy_result))) {
              LOG_WARN("fail to eval difference with collection", K(ret));
            } else if (!mpy_result->is_empty()) {
              if (mls_empty && mpt_empty) {
                result = mpy_result;
              } else {
                if (mpy_result->type() == common::ObGeoType::GEOMETRYCOLLECTION) {
                  GcTreeType *mpy_res = reinterpret_cast<GcTreeType*>(mpy_result);
                  for (int i = 0; OB_SUCC(ret) && i < mpy_res->size(); ++i) {
                    if (OB_FAIL(
                            res_coll->push_back(reinterpret_cast<const ObGeometry &>((*mpy_res)[i])))) {
                      LOG_WARN("fail to push back geometry", K(ret));
                    }
                  }
                } else {
                  if (OB_FAIL(res_coll->push_back(*mpy_result))) {
                    LOG_WARN("fail to push back geometry", K(ret));
                  }
                }
              }
            }
          } // end if (!mpy_empty)

          if (OB_SUCC(ret) && !mls_empty) {
            ObGeometry *mls_bin = nullptr;
            ObGeometry *mls_result = nullptr;
            if (OB_FAIL(ObGeoTypeUtil::tree_to_bin(*allocator, mls2, mls_bin, srs))) {
              LOG_WARN("fail to transform tree to binary", K(ret));
            } else if (OB_FAIL(eval_wkb_binary(g1, mls_bin, context, mls_result))) {
              LOG_WARN("fail to eval difference with collection", K(ret));
            } else if (!mls_result->is_empty()) {
              if (mpy_empty && mpt_empty) {
                result = mls_result;
              } else {
                if (mls_result->type() == common::ObGeoType::GEOMETRYCOLLECTION) {
                  GcTreeType *mls_res = reinterpret_cast<GcTreeType*>(mls_result);
                  for (int i = 0; OB_SUCC(ret) && i < mls_res->size(); ++i) {
                    if (OB_FAIL(
                            res_coll->push_back(reinterpret_cast<const ObGeometry &>((*mls_res)[i])))) {
                      LOG_WARN("fail to push back geometry", K(ret));
                    }
                  }
                } else {
                  if (OB_FAIL(res_coll->push_back(*mls_result))) {
                    LOG_WARN("fail to push back geometry", K(ret));
                  }
                }
              }
            }
          } // end if (!mls_empty)

          if (OB_SUCC(ret) && !mpt_empty) {
            ObGeometry *mpt_bin = nullptr;
            ObGeometry *mpt_result = nullptr;
            if (OB_FAIL(ObGeoTypeUtil::tree_to_bin(*allocator, mpt2, mpt_bin, srs))) {
              LOG_WARN("fail to transform tree to binary", K(ret));
            } else if (OB_FAIL(eval_wkb_binary(g1, mpt_bin, context, mpt_result))) {
              LOG_WARN("fail to eval difference with collection", K(ret));
            } else if (!mpt_result->is_empty()) {
              if (mpy_empty && mls_empty) {
                result = mpt_result;
              } else {
                if (mpt_result->type() == common::ObGeoType::GEOMETRYCOLLECTION) {
                  GcTreeType *mpt_res = reinterpret_cast<GcTreeType*>(mpt_result);
                  for (int i = 0; OB_SUCC(ret) && i < mpt_res->size(); ++i) {
                    if (OB_FAIL(
                            res_coll->push_back(reinterpret_cast<const ObGeometry &>((*mpt_res)[i])))) {
                      LOG_WARN("fail to push back geometry", K(ret));
                    }
                  }
                } else {
                  if (OB_FAIL(res_coll->push_back(*mpt_result))) {
                    LOG_WARN("fail to push back geometry", K(ret));
                  }
                }
              }
            }
          } // end if (!mpt_empty)

          if (OB_SUCC(ret) && OB_ISNULL(result)) {
            result = res_coll->size() == 1 ? *(res_coll->begin()) : res_coll;
          }
        }
      }
    }
    return ret;
  }

};

/*********************************** bin cartesian **************************************/

// intersection(Cartesian_point, *)
OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeomPoint, ObWkbGeomPoint, ObGeometry *)
{
  return apply_bg_intersections<ObCartesianMultipoint, ObCartesianMultilinestring, ObCartesianMultipolygon,
                                ObWkbGeomPoint, ObWkbGeomPoint, ObCartesianGeometrycollection>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeomPoint, ObWkbGeomLineString, ObGeometry *)
{
  return apply_bg_intersections<ObCartesianMultipoint, ObCartesianMultilinestring, ObCartesianMultipolygon,
                                ObWkbGeomPoint, ObWkbGeomLineString, ObCartesianGeometrycollection>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeomPoint, ObWkbGeomPolygon, ObGeometry *)
{
  return apply_bg_intersections<ObCartesianMultipoint, ObCartesianMultilinestring, ObCartesianMultipolygon,
                                ObWkbGeomPoint, ObWkbGeomPolygon, ObCartesianGeometrycollection>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeomPoint, ObWkbGeomMultiPoint, ObGeometry *)
{
  return apply_bg_intersections<ObCartesianMultipoint, ObCartesianMultilinestring, ObCartesianMultipolygon,
                                ObWkbGeomPoint, ObWkbGeomMultiPoint, ObCartesianGeometrycollection>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeomPoint, ObWkbGeomMultiLineString, ObGeometry *)
{
  return apply_bg_intersections<ObCartesianMultipoint, ObCartesianMultilinestring, ObCartesianMultipolygon,
                                ObWkbGeomPoint, ObWkbGeomMultiLineString, ObCartesianGeometrycollection>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeomPoint, ObWkbGeomMultiPolygon, ObGeometry *)
{
  return apply_bg_intersections<ObCartesianMultipoint, ObCartesianMultilinestring, ObCartesianMultipolygon,
                                ObWkbGeomPoint, ObWkbGeomMultiPolygon, ObCartesianGeometrycollection>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

//////////////////////////////////////////////////////////////////////////////

// intersection(Cartesian_linestring, *)
OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeomLineString, ObWkbGeomPoint, ObGeometry *)
{
  return apply_bg_intersections<ObCartesianMultipoint, ObCartesianMultilinestring, ObCartesianMultipolygon,
                                ObWkbGeomPoint, ObWkbGeomLineString, ObCartesianGeometrycollection>(
      g2, g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeomLineString, ObWkbGeomLineString, ObGeometry *)
{
  return apply_bg_intersections<ObCartesianMultipoint, ObCartesianMultilinestring, ObCartesianMultipolygon,
                                ObWkbGeomLineString, ObWkbGeomLineString, ObCartesianGeometrycollection>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

// TODO: fix this in boost geometry
OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeomLineString, ObWkbGeomPolygon, ObGeometry *)
{
  return apply_brute_force_intersections_cart<ObWkbGeomLineString, ObWkbGeomPolygon>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeomLineString, ObWkbGeomMultiPoint, ObGeometry *)
{
  return apply_bg_intersections<ObCartesianMultipoint, ObCartesianMultilinestring, ObCartesianMultipolygon,
                                ObWkbGeomLineString, ObWkbGeomMultiPoint, ObCartesianGeometrycollection>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeomLineString, ObWkbGeomMultiLineString, ObGeometry *)
{
  return apply_bg_intersections<ObCartesianMultipoint, ObCartesianMultilinestring, ObCartesianMultipolygon,
                                ObWkbGeomLineString, ObWkbGeomMultiLineString, ObCartesianGeometrycollection>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

// TODO: fix this in boost geometry
OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeomLineString, ObWkbGeomMultiPolygon, ObGeometry *)
{
  return apply_brute_force_intersections_cart<ObWkbGeomLineString, ObWkbGeomMultiPolygon>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

//////////////////////////////////////////////////////////////////////////////

// intersection(Cartesian_polygon, *)
OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeomPolygon, ObWkbGeomPoint, ObGeometry *)
{
  return apply_bg_intersections<ObCartesianMultipoint, ObCartesianMultilinestring, ObCartesianMultipolygon,
                                ObWkbGeomPoint, ObWkbGeomPolygon, ObCartesianGeometrycollection>(
      g2, g1, context, result);
}
OB_GEO_FUNC_END;

// TODO: fix this in boost geometry
OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeomPolygon, ObWkbGeomLineString, ObGeometry *)
{
  return apply_brute_force_intersections_cart<ObWkbGeomLineString, ObWkbGeomPolygon>(
      g2, g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeomPolygon, ObWkbGeomPolygon, ObGeometry *)
{
  return apply_bg_intersections<ObCartesianMultipoint, ObCartesianMultilinestring, ObCartesianMultipolygon,
                                ObWkbGeomPolygon, ObWkbGeomPolygon, ObCartesianGeometrycollection>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeomPolygon, ObWkbGeomMultiPoint, ObGeometry *)
{
  return apply_bg_intersections<ObCartesianMultipoint, ObCartesianMultilinestring, ObCartesianMultipolygon,
                                ObWkbGeomPolygon, ObWkbGeomMultiPoint, ObCartesianGeometrycollection>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

// TODO: fix this in boost geometry
OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeomPolygon, ObWkbGeomMultiLineString, ObGeometry *)
{
  return apply_brute_force_intersections_cart<ObWkbGeomPolygon, ObWkbGeomMultiLineString>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeomPolygon, ObWkbGeomMultiPolygon, ObGeometry *)
{
  return apply_bg_intersections<ObCartesianMultipoint, ObCartesianMultilinestring, ObCartesianMultipolygon,
                                ObWkbGeomPolygon, ObWkbGeomMultiPolygon, ObCartesianGeometrycollection>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

//////////////////////////////////////////////////////////////////////////////

// intersection(Cartesian_geometrycollection, *)
OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeomCollection, ObWkbGeomCollection, ObGeometry *)
{
  return eval_intersections_gc<ObCartesianGeometrycollection, ObCartesianPoint, ObWkbGeomCollection>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_GEO1_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeomCollection, ObGeometry *)
{
  return eval_intersections_gc<ObCartesianGeometrycollection, ObCartesianPoint, ObWkbGeomCollection>(
      g2, g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_GEO2_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeomCollection, ObGeometry *)
{
  return eval_intersections_gc<ObCartesianGeometrycollection, ObCartesianPoint, ObWkbGeomCollection>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

//////////////////////////////////////////////////////////////////////////////

// intersection(Cartesian_multipoint, *)
OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeomMultiPoint, ObWkbGeomPoint, ObGeometry *)
{
  return apply_bg_intersections<ObCartesianMultipoint, ObCartesianMultilinestring, ObCartesianMultipolygon,
                                ObWkbGeomPoint, ObWkbGeomMultiPoint, ObCartesianGeometrycollection>(
      g2, g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeomMultiPoint, ObWkbGeomLineString, ObGeometry *)
{
  return apply_bg_intersections<ObCartesianMultipoint, ObCartesianMultilinestring, ObCartesianMultipolygon,
                                ObWkbGeomLineString, ObWkbGeomMultiPoint, ObCartesianGeometrycollection>(
      g2, g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeomMultiPoint, ObWkbGeomPolygon, ObGeometry *)
{
  return apply_bg_intersections<ObCartesianMultipoint, ObCartesianMultilinestring, ObCartesianMultipolygon,
                                ObWkbGeomPolygon, ObWkbGeomMultiPoint, ObCartesianGeometrycollection>(
      g2, g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeomMultiPoint, ObWkbGeomMultiPoint, ObGeometry *)
{
  return apply_bg_intersections<ObCartesianMultipoint, ObCartesianMultilinestring, ObCartesianMultipolygon,
                                ObWkbGeomMultiPoint, ObWkbGeomMultiPoint, ObCartesianGeometrycollection>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeomMultiPoint, ObWkbGeomMultiLineString, ObGeometry *)
{
  return apply_bg_intersections<ObCartesianMultipoint, ObCartesianMultilinestring, ObCartesianMultipolygon,
                                ObWkbGeomMultiPoint, ObWkbGeomMultiLineString, ObCartesianGeometrycollection>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeomMultiPoint, ObWkbGeomMultiPolygon, ObGeometry *)
{
  return apply_bg_intersections<ObCartesianMultipoint, ObCartesianMultilinestring, ObCartesianMultipolygon,
                                ObWkbGeomMultiPoint, ObWkbGeomMultiPolygon, ObCartesianGeometrycollection>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;


//////////////////////////////////////////////////////////////////////////////

// intersection(Cartesian_multilinestring, *)
OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeomMultiLineString, ObWkbGeomPoint, ObGeometry *)
{
  return apply_bg_intersections<ObCartesianMultipoint, ObCartesianMultilinestring, ObCartesianMultipolygon,
                                ObWkbGeomPoint, ObWkbGeomMultiLineString, ObCartesianGeometrycollection>(
      g2, g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeomMultiLineString, ObWkbGeomLineString, ObGeometry *)
{
  return apply_bg_intersections<ObCartesianMultipoint, ObCartesianMultilinestring, ObCartesianMultipolygon,
                                ObWkbGeomLineString, ObWkbGeomMultiLineString, ObCartesianGeometrycollection>(
      g2, g1, context, result);
}
OB_GEO_FUNC_END;

// TODO: fix this in boost geometry
OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeomMultiLineString, ObWkbGeomPolygon, ObGeometry *)
{
  return apply_brute_force_intersections_cart<ObWkbGeomPolygon, ObWkbGeomMultiLineString>(
      g2, g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeomMultiLineString, ObWkbGeomMultiPoint, ObGeometry *)
{
  return apply_bg_intersections<ObCartesianMultipoint, ObCartesianMultilinestring, ObCartesianMultipolygon,
                                ObWkbGeomMultiPoint, ObWkbGeomMultiLineString, ObCartesianGeometrycollection>(
      g2, g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeomMultiLineString, ObWkbGeomMultiLineString, ObGeometry *)
{
  return apply_bg_intersections<ObCartesianMultipoint, ObCartesianMultilinestring, ObCartesianMultipolygon,
                                ObWkbGeomMultiLineString, ObWkbGeomMultiLineString, ObCartesianGeometrycollection>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

// TODO: fix this in boost geometry
OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeomMultiLineString, ObWkbGeomMultiPolygon, ObGeometry *)
{
  return apply_brute_force_intersections_cart<ObWkbGeomMultiLineString, ObWkbGeomMultiPolygon>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

//////////////////////////////////////////////////////////////////////////////

// intersection(Cartesian_multipolygon, *)
OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeomMultiPolygon, ObWkbGeomPoint, ObGeometry *)
{
  return apply_bg_intersections<ObCartesianMultipoint, ObCartesianMultilinestring, ObCartesianMultipolygon,
                                ObWkbGeomPoint, ObWkbGeomMultiPolygon, ObCartesianGeometrycollection>(
      g2, g1, context, result);
}
OB_GEO_FUNC_END;

// TODO: fix this in boost geometry
OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeomMultiPolygon, ObWkbGeomLineString, ObGeometry *)
{
  return apply_brute_force_intersections_cart<ObWkbGeomLineString, ObWkbGeomMultiPolygon>(
      g2, g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeomMultiPolygon, ObWkbGeomPolygon, ObGeometry *)
{
  return apply_bg_intersections<ObCartesianMultipoint, ObCartesianMultilinestring, ObCartesianMultipolygon,
                                ObWkbGeomPolygon, ObWkbGeomMultiPolygon, ObCartesianGeometrycollection>(
      g2, g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeomMultiPolygon, ObWkbGeomMultiPoint, ObGeometry *)
{
  return apply_bg_intersections<ObCartesianMultipoint, ObCartesianMultilinestring, ObCartesianMultipolygon,
                                ObWkbGeomMultiPoint, ObWkbGeomMultiPolygon, ObCartesianGeometrycollection>(
      g2, g1, context, result);
}
OB_GEO_FUNC_END;

// TODO: fix this in boost geometry
OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeomMultiPolygon, ObWkbGeomMultiLineString, ObGeometry *)
{
  return apply_brute_force_intersections_cart<ObWkbGeomMultiLineString, ObWkbGeomMultiPolygon>(
      g2, g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeomMultiPolygon, ObWkbGeomMultiPolygon, ObGeometry *)
{
  return apply_bg_intersections<ObCartesianMultipoint, ObCartesianMultilinestring, ObCartesianMultipolygon,
                                ObWkbGeomMultiPolygon, ObWkbGeomMultiPolygon, ObCartesianGeometrycollection>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

/*********************************** bin geography **************************************/

// intersection(Geographic_point, *)
OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeogPoint, ObWkbGeogPoint, ObGeometry *)
{
  return apply_bg_intersections<ObGeographMultipoint, ObGeographMultilinestring, ObGeographMultipolygon,
                                ObWkbGeogPoint, ObWkbGeogPoint, ObGeographGeometrycollection>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeogPoint, ObWkbGeogLineString, ObGeometry *)
{
  return apply_bg_intersections_with_strategy<ObGeographMultipoint, ObGeographMultilinestring, ObGeographMultipolygon, ObPlPaStrategy,
                                              ObWkbGeogPoint, ObWkbGeogLineString, ObGeographGeometrycollection>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeogPoint, ObWkbGeogPolygon, ObGeometry *)
{
  return apply_bg_intersections_with_strategy<ObGeographMultipoint, ObGeographMultilinestring, ObGeographMultipolygon, ObPlPaStrategy,
                                              ObWkbGeogPoint, ObWkbGeogPolygon, ObGeographGeometrycollection>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeogPoint, ObWkbGeogMultiPoint, ObGeometry *)
{
  return apply_bg_intersections<ObGeographMultipoint, ObGeographMultilinestring, ObGeographMultipolygon,
                                ObWkbGeogPoint, ObWkbGeogMultiPoint, ObGeographGeometrycollection>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeogPoint, ObWkbGeogMultiLineString, ObGeometry *)
{
  return apply_bg_intersections_with_strategy<ObGeographMultipoint, ObGeographMultilinestring, ObGeographMultipolygon, ObPlPaStrategy,
                                              ObWkbGeogPoint, ObWkbGeogMultiLineString, ObGeographGeometrycollection>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeogPoint, ObWkbGeogMultiPolygon, ObGeometry *)
{
  return apply_bg_intersections_with_strategy<ObGeographMultipoint, ObGeographMultilinestring, ObGeographMultipolygon, ObPlPaStrategy,
                                              ObWkbGeogPoint, ObWkbGeogMultiPolygon, ObGeographGeometrycollection>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

//////////////////////////////////////////////////////////////////////////////

// intersection(Geographic_linestring, *)
OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeogLineString, ObWkbGeogPoint, ObGeometry *)
{
  return apply_bg_intersections_with_strategy<ObGeographMultipoint, ObGeographMultilinestring, ObGeographMultipolygon, ObPlPaStrategy,
                                              ObWkbGeogPoint, ObWkbGeogLineString, ObGeographGeometrycollection>(
      g2, g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeogLineString, ObWkbGeogLineString, ObGeometry *)
{
  return apply_bg_intersections_with_strategy<ObGeographMultipoint, ObGeographMultilinestring, ObGeographMultipolygon, ObLlLaAaStrategy,
                                              ObWkbGeogLineString, ObWkbGeogLineString, ObGeographGeometrycollection>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeogLineString, ObWkbGeogPolygon, ObGeometry *)
{
  return apply_brute_force_intersections_geog<ObWkbGeogLineString, ObWkbGeogPolygon>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeogLineString, ObWkbGeogMultiPoint, ObGeometry *)
{
  return apply_bg_intersections_with_strategy<ObGeographMultipoint, ObGeographMultilinestring, ObGeographMultipolygon, ObPlPaStrategy,
                                              ObWkbGeogLineString, ObWkbGeogMultiPoint, ObGeographGeometrycollection>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeogLineString, ObWkbGeogMultiLineString, ObGeometry *)
{
  return apply_bg_intersections_with_strategy<ObGeographMultipoint, ObGeographMultilinestring, ObGeographMultipolygon, ObLlLaAaStrategy,
                                              ObWkbGeogLineString, ObWkbGeogMultiLineString, ObGeographGeometrycollection>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeogLineString, ObWkbGeogMultiPolygon, ObGeometry *)
{
  return apply_brute_force_intersections_geog<ObWkbGeogLineString, ObWkbGeogMultiPolygon>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

//////////////////////////////////////////////////////////////////////////////

// intersection(Geographic_polygon, *)
OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeogPolygon, ObWkbGeogPoint, ObGeometry *)
{
  return apply_bg_intersections_with_strategy<ObGeographMultipoint, ObGeographMultilinestring, ObGeographMultipolygon, ObPlPaStrategy,
                                              ObWkbGeogPoint, ObWkbGeogPolygon, ObGeographGeometrycollection>(
      g2, g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeogPolygon, ObWkbGeogLineString, ObGeometry *)
{
  return apply_brute_force_intersections_geog<ObWkbGeogLineString, ObWkbGeogPolygon>(
      g2, g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeogPolygon, ObWkbGeogPolygon, ObGeometry *)
{
  return apply_bg_intersections_with_strategy<ObGeographMultipoint, ObGeographMultilinestring, ObGeographMultipolygon, ObLlLaAaStrategy,
                                              ObWkbGeogPolygon, ObWkbGeogPolygon, ObGeographGeometrycollection>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeogPolygon, ObWkbGeogMultiPoint, ObGeometry *)
{
  return apply_bg_intersections_with_strategy<ObGeographMultipoint, ObGeographMultilinestring, ObGeographMultipolygon, ObPlPaStrategy,
                                              ObWkbGeogPolygon, ObWkbGeogMultiPoint, ObGeographGeometrycollection>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeogPolygon, ObWkbGeogMultiLineString, ObGeometry *)
{
  return apply_brute_force_intersections_geog<ObWkbGeogPolygon, ObWkbGeogMultiLineString>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeogPolygon, ObWkbGeogMultiPolygon, ObGeometry *)
{
  return apply_bg_intersections_with_strategy<ObGeographMultipoint, ObGeographMultilinestring, ObGeographMultipolygon, ObLlLaAaStrategy,
                                              ObWkbGeogPolygon, ObWkbGeogMultiPolygon, ObGeographGeometrycollection>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

//////////////////////////////////////////////////////////////////////////////

// intersection(Geographic_geometrycollection, *)
OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeogCollection, ObWkbGeogCollection, ObGeometry *)
{
  return eval_intersections_gc<ObGeographGeometrycollection, ObGeographPoint, ObWkbGeogCollection>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_GEO1_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeogCollection, ObGeometry *)
{
  return eval_intersections_gc<ObGeographGeometrycollection, ObGeographPoint, ObWkbGeogCollection>(
      g2, g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_GEO2_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeogCollection, ObGeometry *)
{
  return eval_intersections_gc<ObGeographGeometrycollection, ObGeographPoint, ObWkbGeogCollection>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

//////////////////////////////////////////////////////////////////////////////

// intersection(Geographic_multipoint, *)
OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeogMultiPoint, ObWkbGeogPoint, ObGeometry *)
{
  return apply_bg_intersections<ObGeographMultipoint, ObGeographMultilinestring, ObGeographMultipolygon,
                                ObWkbGeogPoint, ObWkbGeogMultiPoint, ObGeographGeometrycollection>(
      g2, g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeogMultiPoint, ObWkbGeogLineString, ObGeometry *)
{
  return apply_bg_intersections_with_strategy<ObGeographMultipoint, ObGeographMultilinestring, ObGeographMultipolygon, ObPlPaStrategy,
                                              ObWkbGeogLineString, ObWkbGeogMultiPoint, ObGeographGeometrycollection>(
      g2, g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeogMultiPoint, ObWkbGeogPolygon, ObGeometry *)
{
  return apply_bg_intersections_with_strategy<ObGeographMultipoint, ObGeographMultilinestring, ObGeographMultipolygon, ObPlPaStrategy,
                                              ObWkbGeogPolygon, ObWkbGeogMultiPoint, ObGeographGeometrycollection>(
      g2, g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeogMultiPoint, ObWkbGeogMultiPoint, ObGeometry *)
{
  return apply_bg_intersections_with_strategy<ObGeographMultipoint, ObGeographMultilinestring, ObGeographMultipolygon, ObPlPaStrategy,
                                              ObWkbGeogMultiPoint, ObWkbGeogMultiPoint, ObGeographGeometrycollection>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeogMultiPoint, ObWkbGeogMultiLineString, ObGeometry *)
{
  return apply_bg_intersections_with_strategy<ObGeographMultipoint, ObGeographMultilinestring, ObGeographMultipolygon, ObPlPaStrategy,
                                              ObWkbGeogMultiPoint, ObWkbGeogMultiLineString, ObGeographGeometrycollection>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeogMultiPoint, ObWkbGeogMultiPolygon, ObGeometry *)
{
  return apply_bg_intersections_with_strategy<ObGeographMultipoint, ObGeographMultilinestring, ObGeographMultipolygon, ObPlPaStrategy,
                                              ObWkbGeogMultiPoint, ObWkbGeogMultiPolygon, ObGeographGeometrycollection>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

//////////////////////////////////////////////////////////////////////////////

// intersection(Geographic_multilinestring, *)
OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeogMultiLineString, ObWkbGeogPoint, ObGeometry *)
{
  return apply_bg_intersections_with_strategy<ObGeographMultipoint, ObGeographMultilinestring, ObGeographMultipolygon, ObPlPaStrategy,
                                              ObWkbGeogPoint, ObWkbGeogMultiLineString, ObGeographGeometrycollection>(
      g2, g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeogMultiLineString, ObWkbGeogLineString, ObGeometry *)
{
  return apply_bg_intersections_with_strategy<ObGeographMultipoint, ObGeographMultilinestring, ObGeographMultipolygon, ObLlLaAaStrategy,
                                              ObWkbGeogLineString, ObWkbGeogMultiLineString, ObGeographGeometrycollection>(
      g2, g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeogMultiLineString, ObWkbGeogPolygon, ObGeometry *)
{
  return apply_brute_force_intersections_geog<ObWkbGeogPolygon, ObWkbGeogMultiLineString>(
      g2, g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeogMultiLineString, ObWkbGeogMultiPoint, ObGeometry *)
{
  return apply_bg_intersections_with_strategy<ObGeographMultipoint, ObGeographMultilinestring, ObGeographMultipolygon, ObPlPaStrategy,
                                              ObWkbGeogMultiPoint, ObWkbGeogMultiLineString, ObGeographGeometrycollection>(
      g2, g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeogMultiLineString, ObWkbGeogMultiLineString, ObGeometry *)
{
  return apply_bg_intersections_with_strategy<ObGeographMultipoint, ObGeographMultilinestring, ObGeographMultipolygon, ObLlLaAaStrategy,
                                              ObWkbGeogMultiLineString, ObWkbGeogMultiLineString, ObGeographGeometrycollection>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeogMultiLineString, ObWkbGeogMultiPolygon, ObGeometry *)
{
  return apply_brute_force_intersections_geog<ObWkbGeogMultiLineString, ObWkbGeogMultiPolygon>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

//////////////////////////////////////////////////////////////////////////////

// intersection(Geographic_multipolygon, *)
OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeogMultiPolygon, ObWkbGeogPoint, ObGeometry *)
{
  return apply_bg_intersections_with_strategy<ObGeographMultipoint, ObGeographMultilinestring, ObGeographMultipolygon, ObPlPaStrategy,
                                              ObWkbGeogPoint, ObWkbGeogMultiPolygon, ObGeographGeometrycollection>(
      g2, g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeogMultiPolygon, ObWkbGeogLineString, ObGeometry *)
{
  return apply_brute_force_intersections_geog<ObWkbGeogLineString, ObWkbGeogMultiPolygon>(
      g2, g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeogMultiPolygon, ObWkbGeogPolygon, ObGeometry *)
{
  return apply_bg_intersections_with_strategy<ObGeographMultipoint, ObGeographMultilinestring, ObGeographMultipolygon, ObLlLaAaStrategy,
                                              ObWkbGeogPolygon, ObWkbGeogMultiPolygon, ObGeographGeometrycollection>(
      g2, g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeogMultiPolygon, ObWkbGeogMultiPoint, ObGeometry *)
{
  return apply_bg_intersections_with_strategy<ObGeographMultipoint, ObGeographMultilinestring, ObGeographMultipolygon, ObPlPaStrategy,
                                              ObWkbGeogMultiPoint, ObWkbGeogMultiPolygon, ObGeographGeometrycollection>(
      g2, g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeogMultiPolygon, ObWkbGeogMultiLineString, ObGeometry *)
{
  return apply_brute_force_intersections_geog<ObWkbGeogMultiLineString, ObWkbGeogMultiPolygon>(
      g2, g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncIntersectionsImpl, ObWkbGeogMultiPolygon, ObWkbGeogMultiPolygon, ObGeometry *)
{
  return apply_bg_intersections_with_strategy<ObGeographMultipoint, ObGeographMultilinestring, ObGeographMultipolygon, ObLlLaAaStrategy,
                                              ObWkbGeogMultiPolygon, ObWkbGeogMultiPolygon, ObGeographGeometrycollection>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

/****************************************************************************/

int ObGeoFuncIntersections::eval(const ObGeoEvalCtx &gis_context, ObGeometry *&result)
{
  return ObGeoFuncIntersectionsImpl::eval_geo_func(gis_context, result);
}

} // sql
} // oceanbase