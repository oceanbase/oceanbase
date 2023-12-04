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
 * This file contains implementation for ob_geo_func_difference.
 */

#define USING_LOG_PREFIX LIB

#include "lib/geo/ob_geo_dispatcher.h"
#include "lib/geo/ob_geo_func_difference.h"
#include "lib/geo/ob_geo_tree.h"
#include "lib/geo/ob_geo_to_tree_visitor.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/geo/ob_geo_func_utils.h"
#include "lib/geo/ob_geo_wkb_size_visitor.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace common
{

template<typename GeoType1, typename GeoType2, typename GeometryRes>
static int apply_bg_difference(
    const ObGeometry *g1, const ObGeometry *g2, const ObGeoEvalCtx &context, ObGeometry *&result)
{
  INIT_SUCC(ret);

  GeometryRes *res = OB_NEWx(GeometryRes,
      context.get_allocator(),
      g1->get_srid(),
      *context.get_allocator());
  if (OB_ISNULL(res)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create go by type", K(ret));
  } else {
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
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_WARN("wrong geometry type", K(ret));
    } else {
      boost::geometry::difference(*geo1, *geo2, *res);
      result = res;
    }
  }
  return ret;
}

template<typename GeoType1, typename GeoType2, typename GeometryRes>
static int apply_bg_difference_ll_strategy(
    const ObGeometry *g1, const ObGeometry *g2, const ObGeoEvalCtx &context, ObGeometry *&result)
{
  INIT_SUCC(ret);
  const ObSrsItem *srs = context.get_srs();
  GeometryRes *res =
      OB_NEWx(GeometryRes, context.get_allocator(), g1->get_srid(), *context.get_allocator());
  if (OB_ISNULL(srs)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("srs is null", K(ret));
  } else if (OB_ISNULL(res)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create go by type", K(ret));
  } else {
    boost::geometry::srs::spheroid<double> geog_sphere(
        srs->semi_major_axis(), srs->semi_minor_axis());
    ObLlLaAaStrategy line_strategy(geog_sphere);
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
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_WARN("wrong geometry type", K(ret));
    } else {
      boost::geometry::difference(*geo1, *geo2, *res, line_strategy);
      result = res;
    }
  }
  return ret;
}

template<typename GeoType1, typename GeoType2, typename GeometryRes>
static int apply_bg_difference_pl_strategy(
    const ObGeometry *g1, const ObGeometry *g2, const ObGeoEvalCtx &context, ObGeometry *&result)
{
  INIT_SUCC(ret);
  const ObSrsItem *srs = context.get_srs();
  GeometryRes *res =
      OB_NEWx(GeometryRes, context.get_allocator(), g1->get_srid(), *context.get_allocator());
  if (OB_ISNULL(srs)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("srs is null", K(ret));
  } else if (OB_ISNULL(res)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create go by type", K(ret));
  } else {
    boost::geometry::srs::spheroid<double> geog_sphere(
        srs->semi_major_axis(), srs->semi_minor_axis());
    ObPlPaStrategy point_strategy(geog_sphere);
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
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_WARN("wrong geometry type", K(ret));
    } else {
      boost::geometry::difference(*geo1, *geo2, *res, point_strategy);
      result = res;
    }
  }
  return ret;
}

class ObGeoFuncDifferenceImpl : public ObIGeoDispatcher<ObGeometry *, ObGeoFuncDifferenceImpl>
{
public:
  ObGeoFuncDifferenceImpl();
  virtual ~ObGeoFuncDifferenceImpl() = default;
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
  // g1: collection
  template <typename CollType, typename MptType, typename MlsType, typename MpyType>
  static int apply_bg_difference_collection(const ObGeometry *g1, const ObGeometry *g2,
                                const ObGeoEvalCtx &context, ObGeometry *&result)
  {
    INIT_SUCC(ret);

    CollType *res = OB_NEWx(CollType, context.get_allocator());
    MptType *mpt = NULL;
    MlsType *mls = NULL;
    MpyType *mpy = NULL;
    // prepare data
    if (OB_ISNULL(res)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memotry for collection", K(ret));
    } else if (g1->is_empty()) {
      // do nothing
    } else {
      const CollType *coll_tree = reinterpret_cast<const CollType *>(g1);
      if (OB_ISNULL(coll_tree)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("failed to convert to geo tree", K(ret), KP(coll_tree));
      } else if (OB_FAIL(ObGeoFuncUtils::ob_geo_gc_split(*context.get_allocator(), *coll_tree, mpt, mls, mpy))) {
        LOG_WARN("failed to do geometry collection split", K(ret));
      } else if (OB_ISNULL(mpt) || OB_ISNULL(mls) || OB_ISNULL(mpy)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null geometry collection split", K(ret), KP(mpt), KP(mls), KP(mpy));
      } else if (OB_FAIL(ObGeoFuncUtils::ob_geo_gc_union(*context.get_allocator(), *context.get_srs(), mpt, mls, mpy))) {
        LOG_WARN("failed to do geometry collection union", K(ret));
      } else if (OB_ISNULL(mpt) || OB_ISNULL(mls) || OB_ISNULL(mpy)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null geometry collection union", K(ret), KP(mpt), KP(mls), KP(mpy));
      }

      // do difference
      if (OB_SUCC(ret) && !mpy->is_empty()) {
        ObGeometry *mpy_res = NULL;
        if (OB_FAIL(eval_tree_binary(mpy, g2, context, mpy_res))) {
          LOG_WARN("fail to eval difference function", K(ret));
        } else if (mpy_res->type() == ObGeoType::POLYGON) {
          if (OB_FAIL(res->push_back(*mpy_res))) {
            LOG_WARN("fail to push geometry into result", K(ret));
          }
        } else {
          MpyType *mpy_res_tree = reinterpret_cast<MpyType *>(mpy_res);
          typename MpyType::iterator iter = mpy_res_tree->begin();
          for (; OB_SUCC(ret) && iter != mpy_res_tree->end(); iter++) {
            if (OB_FAIL(res->push_back(*iter))) {
              LOG_WARN("fail to push geometry into result", K(ret));
            }
          }
        }
      }
      if (OB_SUCC(ret) && !mls->is_empty()) {
        ObGeometry *mls_res = NULL;
        if (OB_FAIL(eval_tree_binary(mls, g2, context, mls_res))) {
          LOG_WARN("fail to eval difference function", K(ret));
        } else if (mls_res->type() == ObGeoType::LINESTRING) {
          if (OB_FAIL(res->push_back(*mls_res))) {
            LOG_WARN("fail to push geometry into result", K(ret));
          }
        } else {
          MlsType *mls_res_tree = reinterpret_cast<MlsType *>(mls_res);
          typename MlsType::iterator iter = mls_res_tree->begin();
          for (; OB_SUCC(ret) && iter != mls_res_tree->end(); iter++) {
            if (OB_FAIL(res->push_back(*iter))) {
              LOG_WARN("fail to push geometry into result", K(ret));
            }
          }
        }
      }
      if (OB_SUCC(ret) && !mpt->is_empty()) {
        ObGeometry *mpt_res = NULL;
        if (OB_FAIL(eval_tree_binary(mpt, g2, context, mpt_res))) {
          LOG_WARN("fail to eval difference function", K(ret));
        } else if (mpt_res->type() == ObGeoType::LINESTRING) {
          if (OB_FAIL(res->push_back(*mpt_res))) {
            LOG_WARN("fail to push geometry into result", K(ret));
          }
        } else {
          MptType *mpt_res_tree = reinterpret_cast<MptType *>(mpt_res);
          typename MptType::iterator iter = mpt_res_tree->begin();
          for (; OB_SUCC(ret) && iter != mpt_res_tree->end(); iter++) {
            typename CollType::sub_pt_type *pt = OB_NEWx(typename CollType::sub_pt_type, context.get_allocator());
            if (OB_ISNULL(pt)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("fail to alloc memotry for point", K(ret));
            } else {
              pt->set_data(*iter);
              if (OB_FAIL(res->push_back(*pt))) {
                LOG_WARN("fail to push geometry into result", K(ret));
              }
            }
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      result = res->size() == 1 ? (&res->front()) : res;
    }
    return ret;
  }
private:
  // assume that g1 g2 both collection
  template<typename GcTreeType, typename GcBinType>
  static int eval_difference_gc_gc(
      const ObGeometry *g1, const ObGeometry *g2, const ObGeoEvalCtx &context, ObGeometry *&result)
  {
    int ret = OB_SUCCESS;
    ObIAllocator *allocator = context.get_allocator();
    result = nullptr;
    if (g1->type() != ObGeoType::GEOMETRYCOLLECTION) {
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_WARN("input geometry should be GEOMETRYCOLLECTION", K(ret), K(g1->type()), K(g2->type()));
    } else {
      typename GcTreeType::sub_mpt_type *mpt1 = NULL;
      typename GcTreeType::sub_ml_type *mls1 = NULL;
      typename GcTreeType::sub_mp_type *mpy1 = NULL;
      ObGeometry *geo1 = const_cast<ObGeometry *>(g1);
      GcTreeType *res_coll =
          OB_NEWx(GcTreeType, allocator, g1->get_srid(), *allocator);
      if (OB_FAIL(ObGeoFuncUtils::ob_gc_prepare<GcTreeType>(context, geo1, mpt1, mls1, mpy1))) {
        LOG_WARN("failed to prepare gc", K(ret));
      } else if (OB_ISNULL(mpt1) || OB_ISNULL(mls1) || OB_ISNULL(mpy1) || OB_ISNULL(res_coll)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("unexpected null geometry collection split", K(ret), K(mpt1), K(mls1), K(mpy1), K(res_coll));
      } else {
        bool mpy_empty = mpy1->is_empty();
        bool mls_empty = mls1->is_empty();
        bool mpt_empty = mpt1->is_empty();
        const ObSrsItem *srs = context.get_srs();
        if (!mpy_empty) {
          ObGeometry *mpy_bin = nullptr;
          ObGeometry *mpy_result = nullptr;
          if (OB_FAIL(ObGeoTypeUtil::tree_to_bin(*allocator, mpy1, mpy_bin, srs))) {
            LOG_WARN("fail to transform tree to binary", K(ret));
          } else if (OB_FAIL(eval_wkb_binary(mpy_bin, g2, context, mpy_result))) {
            LOG_WARN("fail to eval difference with collection", K(ret));
          } else if (!mpy_result->is_empty()) {
            if (mpy_result->type() == ObGeoType::POLYGON) {
              if (OB_FAIL(res_coll->push_back(*mpy_result))) {
                LOG_WARN("fail to push back geometry", K(ret));
              }
            } else if (mls_empty && mpt_empty) {
              result = mpy_result;
            } else {
              typename GcTreeType::sub_mp_type &mpy_res =
                  *reinterpret_cast<typename GcTreeType::sub_mp_type *>(mpy_result);
              for (int i = 0; OB_SUCC(ret) && i < mpy_res.size(); ++i) {
                if (OB_FAIL(
                        res_coll->push_back(reinterpret_cast<const ObGeometry &>(mpy_res[i])))) {
                  LOG_WARN("fail to push back geometry", K(ret));
                }
              }
            }
          }
        }
        if (OB_SUCC(ret) && !mls_empty) {
          ObGeometry *mls_bin = nullptr;
          ObGeometry *mls_result = nullptr;
          if (OB_FAIL(ObGeoTypeUtil::tree_to_bin(*allocator, mls1, mls_bin, srs))) {
            LOG_WARN("fail to transform tree to binary", K(ret));
          } else if (OB_FAIL(eval_wkb_binary(mls_bin, g2, context, mls_result))) {
            LOG_WARN("fail to eval difference with collection", K(ret));
          } else if (!mls_result->is_empty()) {
            if (mls_result->type() == ObGeoType::LINESTRING) {
              if (OB_FAIL(res_coll->push_back(*mls_result))) {
                LOG_WARN("fail to push back geometry", K(ret));
              }
            } else if (mpy_empty && mpt_empty) {
              result = mls_result;
            } else {
              typename GcTreeType::sub_ml_type &mls_res =
                  *reinterpret_cast<typename GcTreeType::sub_ml_type *>(mls_result);
              for (int i = 0; OB_SUCC(ret) && i < mls_res.size(); ++i) {
                if (OB_FAIL(
                        res_coll->push_back(reinterpret_cast<const ObGeometry &>(mls_res[i])))) {
                  LOG_WARN("fail to push back geometry", K(ret));
                }
              }
            }
          }
        }
        if (OB_SUCC(ret) && !mpt_empty) {
          ObGeometry *mpt_bin = nullptr;
          ObGeometry *mpt_result = nullptr;
          if (OB_FAIL(ObGeoTypeUtil::tree_to_bin(*allocator, mpt1, mpt_bin, srs))) {
            LOG_WARN("fail to transform tree to binary", K(ret));
          } else if (OB_FAIL(eval_wkb_binary(mpt_bin, g2, context, mpt_result))) {
            LOG_WARN("fail to eval difference with collection", K(ret));
          } else if (!mpt_result->is_empty()) {
            if (mpt_result->type() == ObGeoType::POINT) {
              if (OB_FAIL(res_coll->push_back(*mpt_result))) {
                LOG_WARN("fail to push back geometry", K(ret));
              }
            } else if (mpy_empty && mls_empty) {
              result = mpt_result;
            } else {
              typename GcTreeType::sub_mpt_type &mpt_res =
                  *reinterpret_cast<typename GcTreeType::sub_mpt_type *>(mpt_result);
              for (int i = 0; OB_SUCC(ret) && i < mpt_res.size(); ++i) {
                typename GcTreeType::sub_mpt_type::value_type &pt = mpt_res[i];
                typename GcTreeType::sub_pt_type *pt_tree = OB_NEWx(
                    typename GcTreeType::sub_pt_type,
                    allocator,
                    pt.template get<0>(),
                    pt.template get<1>(),
                    g1->get_srid(),
                    allocator);
                if (OB_ISNULL(pt_tree)) {
                  ret = OB_ALLOCATE_MEMORY_FAILED;
                  LOG_WARN("fail to allocate memory", K(ret));
                } else if (OB_FAIL(res_coll->push_back(
                               reinterpret_cast<const ObGeometry &>(*pt_tree)))) {
                  LOG_WARN("fail to push back geometry", K(ret));
                }
              }
            }
          }
        }
        if (OB_SUCC(ret) && OB_ISNULL(result)) {
          result = res_coll->size() == 1 ? *(res_coll->begin()) : res_coll;
        }
      }
    }
    return ret;
  }

  static int create_multi_type(ObIAllocator &allocator, const ObGeometry *g, ObGeometry *&res)
  {
    int ret = OB_SUCCESS;
    if ((g->type() == ObGeoType::MULTIPOINT) || (g->type() == ObGeoType::MULTILINESTRING)
        || (g->type() == ObGeoType::MULTIPOLYGON)) {
      res = const_cast<ObGeometry *>(g);
    } else {
      ObWkbBuffer buffer(allocator);
      const uint32_t GEO_NUM = 1;
      uint32_t multi_type_num = static_cast<uint32_t>(g->type()) + static_cast<uint32_t>(3);
      if (OB_FAIL(buffer.reserve(g->length() + WKB_COMMON_WKB_HEADER_LEN))) {
        LOG_WARN("fail to reserver memory", K(ret), K(g->length()));
      } else if (OB_FAIL(buffer.append(static_cast<char>(ObGeoWkbByteOrder::LittleEndian)))) {
        LOG_WARN("fail to append buffer", K(ret));
      } else if (OB_FAIL(buffer.append(multi_type_num))) {
        LOG_WARN("fail to append buffer", K(ret), K(multi_type_num));
      } else if (OB_FAIL(buffer.append(GEO_NUM))) {
        LOG_WARN("fail to append buffer", K(ret), K(GEO_NUM));
      } else if (OB_FAIL(buffer.append(g->val(), g->length()))) {
        LOG_WARN("fail to append buffer", K(ret));
      } else if (OB_FAIL(ObGeoTypeUtil::create_geo_by_type(allocator, static_cast<ObGeoType>(multi_type_num),
                                                          g->crs() == ObGeoCRS::Geographic, true, res, g->get_srid()))) {
        LOG_WARN("failed to create wkb", K(ret));
      } else {
        res->set_data(buffer.string());
      }
    }

    return ret;
  }

  template<typename GcTreeType, typename GcBinType>
  static int eval_difference_gc_other(
      const ObGeometry *g1, const ObGeometry *g2, const ObGeoEvalCtx &context, ObGeometry *&result)
  {
    int ret = OB_SUCCESS;
    ObIAllocator *allocator = context.get_allocator();
    if (g1->type() != ObGeoType::GEOMETRYCOLLECTION
        && g2->type() != ObGeoType::GEOMETRYCOLLECTION) {
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_WARN("At least one of g1 and g2 is collection", K(ret), K(g1->type()), K(g2->type()));
    } else if (g1->type() == ObGeoType::GEOMETRYCOLLECTION) {
      if (OB_FAIL(
              (eval_difference_gc_gc<GcTreeType, GcBinType>(g1, g2, context, result)))) {
        LOG_WARN("fail to eval difference with geometrycollection", K(ret));
      }
    } else {
      bool is_g2_empty = false;
      if (g2->is_empty()) {
        if (OB_FAIL(ObGeoFuncUtils::apply_bg_to_tree(g1, context, result))) {
          LOG_WARN("fail to apply g1 to tree", K(ret));
        }
      } else {
        // g2 is not empty collection
        const GcBinType *geo2 = reinterpret_cast<const GcBinType *>(g2->val());
        typename GcBinType::iterator iter = geo2->begin();
        ObGeometry *tmp_result = nullptr;
        bool is_geog = (g2->crs() == oceanbase::common::ObGeoCRS::Geographic);
        if (OB_FAIL(create_multi_type(*allocator, g1, tmp_result))) {
          LOG_WARN("failt to create multi type", K(ret));
        }
        while (OB_SUCC(ret) && iter != geo2->end()) {
          typename GcBinType::const_pointer sub_ptr = iter.operator->();
          ObGeoType sub_type = geo2->get_sub_type(sub_ptr);
          ObGeometry *sub_g2 = NULL;
          if (OB_FAIL(
                  ObGeoTypeUtil::create_geo_by_type(*allocator, sub_type, is_geog, true, sub_g2))) {
            LOG_WARN("failed to create wkb", K(ret), K(sub_type));
          } else {
            // Length is not used, cannot get real length untill iter move to the next
            ObString wkb_nosrid(WKB_COMMON_WKB_HEADER_LEN, reinterpret_cast<const char *>(sub_ptr));
            sub_g2->set_data(wkb_nosrid);
            sub_g2->set_srid(g2->get_srid());
            ObGeometry *geo_bin = nullptr;
            const ObSrsItem *srs = context.get_srs();
            if (OB_FAIL(eval_wkb_binary(tmp_result, sub_g2, context, result))) {
              LOG_WARN("fail to do eval", K(ret));
            } else if (FALSE_IT(iter++)) {
            } else if (iter != geo2->end()) {
              if (OB_FAIL((ObGeoFuncUtils::simplify_multi_geo<GcTreeType>(result, *allocator)))) {
                // should not do simplify in difference functor, it may affect
                // ObGeoFuncUtils::ob_geo_gc_union
                LOG_WARN("fail to simplify result", K(ret));
              }  else if (OB_FAIL(ObGeoTypeUtil::tree_to_bin(*allocator, result, geo_bin, srs))) {
                LOG_WARN("fail to transform tree to binary", K(ret));
              } else {
                tmp_result = geo_bin;
                allocator->free(result);
                result = nullptr;
              }
            }
          }
        }
      }
    }
    return ret;
  }
};

OB_GEO_CART_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeomPoint, ObWkbGeomPoint, ObGeometry *)
{
  return apply_bg_difference<ObWkbGeomPoint, ObWkbGeomPoint, ObCartesianMultipoint>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeomPoint, ObWkbGeomLineString, ObGeometry *)
{
  return apply_bg_difference<ObWkbGeomPoint, ObWkbGeomLineString, ObCartesianMultipoint>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeomPoint, ObWkbGeomPolygon, ObGeometry *)
{
  return apply_bg_difference<ObWkbGeomPoint, ObWkbGeomPolygon, ObCartesianMultipoint>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeomPoint, ObWkbGeomMultiPoint, ObGeometry *)
{
  return apply_bg_difference<ObWkbGeomPoint, ObWkbGeomMultiPoint, ObCartesianMultipoint>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeomPoint, ObWkbGeomMultiLineString, ObGeometry *)
{
  return apply_bg_difference<ObWkbGeomPoint, ObWkbGeomMultiLineString, ObCartesianMultipoint>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeomPoint, ObWkbGeomMultiPolygon, ObGeometry *)
{
  return apply_bg_difference<ObWkbGeomPoint, ObWkbGeomMultiPolygon, ObCartesianMultipoint>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

// cartisian linestring
OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeomLineString, ObWkbGeomPoint, ObGeometry *)
{
  // if g1.dimension > g2.dimension, g1 - g2 is equal g1(mysql/postgis)
  // so just convert g1 to tree
  return ObGeoFuncUtils::apply_bg_to_tree(g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeomLineString, ObWkbGeomLineString, ObGeometry *)
{
  return apply_bg_difference<ObWkbGeomLineString, ObWkbGeomLineString, ObCartesianMultilinestring>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeomLineString, ObWkbGeomPolygon, ObGeometry *)
{
  return apply_bg_difference<ObWkbGeomLineString, ObWkbGeomPolygon, ObCartesianMultilinestring>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeomLineString, ObWkbGeomMultiPoint, ObGeometry *)
{
  // if g1.dimension > g2.dimension, g1 - g2 is equal g1(mysql/postgis)
  // so just convert g1 to tree
  return ObGeoFuncUtils::apply_bg_to_tree(g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeomLineString, ObWkbGeomMultiLineString, ObGeometry *)
{
  return apply_bg_difference<ObWkbGeomLineString,
      ObWkbGeomMultiLineString,
      ObCartesianMultilinestring>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeomLineString, ObWkbGeomMultiPolygon, ObGeometry *)
{
  return apply_bg_difference<ObWkbGeomLineString,
      ObWkbGeomMultiPolygon,
      ObCartesianMultilinestring>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

// cartisian polygon
OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeomPolygon, ObWkbGeomPoint, ObGeometry *)
{
  // if g1.dimension > g2.dimension, g1 - g2 is equal g1(mysql/postgis)
  // so just convert g1 to tree
  return ObGeoFuncUtils::apply_bg_to_tree(g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeomPolygon, ObWkbGeomLineString, ObGeometry *)
{
  // if g1.dimension > g2.dimension, g1 - g2 is equal g1(mysql/postgis)
  // so just convert g1 to tree
  return ObGeoFuncUtils::apply_bg_to_tree(g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeomPolygon, ObWkbGeomPolygon, ObGeometry *)
{
  return apply_bg_difference<ObWkbGeomPolygon, ObWkbGeomPolygon, ObCartesianMultipolygon>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeomPolygon, ObWkbGeomMultiPoint, ObGeometry *)
{
  // if g1.dimension > g2.dimension, g1 - g2 is equal g1(mysql/postgis)
  // so just convert g1 to tree
  return ObGeoFuncUtils::apply_bg_to_tree(g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeomPolygon, ObWkbGeomMultiLineString, ObGeometry *)
{
  // if g1.dimension > g2.dimension, g1 - g2 is equal g1(mysql/postgis)
  // so just convert g1 to tree
  return ObGeoFuncUtils::apply_bg_to_tree(g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeomPolygon, ObWkbGeomMultiPolygon, ObGeometry *)
{
  return apply_bg_difference<ObWkbGeomPolygon, ObWkbGeomMultiPolygon, ObCartesianMultipolygon>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

// cartisian multipoint
OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeomMultiPoint, ObWkbGeomPoint, ObGeometry *)
{
  return apply_bg_difference<ObWkbGeomMultiPoint, ObWkbGeomPoint, ObCartesianMultipoint>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeomMultiPoint, ObWkbGeomLineString, ObGeometry *)
{
  return apply_bg_difference<ObWkbGeomMultiPoint, ObWkbGeomLineString, ObCartesianMultipoint>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeomMultiPoint, ObWkbGeomPolygon, ObGeometry *)
{
  return apply_bg_difference<ObWkbGeomMultiPoint, ObWkbGeomPolygon, ObCartesianMultipoint>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeomMultiPoint, ObWkbGeomMultiPoint, ObGeometry *)
{
  return apply_bg_difference<ObWkbGeomMultiPoint, ObWkbGeomMultiPoint, ObCartesianMultipoint>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeomMultiPoint, ObWkbGeomMultiLineString, ObGeometry *)
{
  return apply_bg_difference<ObWkbGeomMultiPoint, ObWkbGeomMultiLineString, ObCartesianMultipoint>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeomMultiPoint, ObWkbGeomMultiPolygon, ObGeometry *)
{
  return apply_bg_difference<ObWkbGeomMultiPoint, ObWkbGeomMultiPolygon, ObCartesianMultipoint>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

// cartisian mutilinestring
OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeomMultiLineString, ObWkbGeomPoint, ObGeometry *)
{
  // if g1.dimension > g2.dimension, g1 - g2 is equal g1(mysql/postgis)
  // so just convert g1 to tree
  return ObGeoFuncUtils::apply_bg_to_tree(g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeomMultiLineString, ObWkbGeomLineString, ObGeometry *)
{
  return apply_bg_difference<ObWkbGeomMultiLineString,
      ObWkbGeomLineString,
      ObCartesianMultilinestring>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeomMultiLineString, ObWkbGeomPolygon, ObGeometry *)
{
  return apply_bg_difference<ObWkbGeomMultiLineString,
      ObWkbGeomPolygon,
      ObCartesianMultilinestring>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeomMultiLineString, ObWkbGeomMultiPoint, ObGeometry *)
{
  // if g1.dimension > g2.dimension, g1 - g2 is equal g1(mysql/postgis)
  // so just convert g1 to tree
  return ObGeoFuncUtils::apply_bg_to_tree(g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeomMultiLineString, ObWkbGeomMultiLineString, ObGeometry *)
{
  return apply_bg_difference<ObWkbGeomMultiLineString,
      ObWkbGeomMultiLineString,
      ObCartesianMultilinestring>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeomMultiLineString, ObWkbGeomMultiPolygon, ObGeometry *)
{
  return apply_bg_difference<ObWkbGeomMultiLineString,
      ObWkbGeomMultiPolygon,
      ObCartesianMultilinestring>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

// cartisian multipolygon
OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeomMultiPolygon, ObWkbGeomPoint, ObGeometry *)
{
  // if g1.dimension > g2.dimension, g1 - g2 is equal g1(mysql/postgis)
  // so just convert g1 to tree
  return ObGeoFuncUtils::apply_bg_to_tree(g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeomMultiPolygon, ObWkbGeomLineString, ObGeometry *)
{
  // if g1.dimension > g2.dimension, g1 - g2 is equal g1(mysql/postgis)
  // so just convert g1 to tree
  return ObGeoFuncUtils::apply_bg_to_tree(g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeomMultiPolygon, ObWkbGeomPolygon, ObGeometry *)
{
  return apply_bg_difference<ObWkbGeomMultiPolygon, ObWkbGeomPolygon, ObCartesianMultipolygon>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeomMultiPolygon, ObWkbGeomMultiPoint, ObGeometry *)
{
  // if g1.dimension > g2.dimension, g1 - g2 is equal g1(mysql/postgis)
  // so just convert g1 to tree
  return ObGeoFuncUtils::apply_bg_to_tree(g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeomMultiPolygon, ObWkbGeomMultiLineString, ObGeometry *)
{
  // if g1.dimension > g2.dimension, g1 - g2 is equal g1(mysql/postgis)
  // so just convert g1 to tree
  return ObGeoFuncUtils::apply_bg_to_tree(g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeomMultiPolygon, ObWkbGeomMultiPolygon, ObGeometry *)
{
  return apply_bg_difference<ObWkbGeomMultiPolygon, ObWkbGeomMultiPolygon, ObCartesianMultipolygon>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

// cartesian geometry collection
OB_GEO_CART_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeomCollection, ObWkbGeomCollection, ObGeometry *)
{
  return eval_difference_gc_other<ObCartesianGeometrycollection,
      ObWkbGeomCollection>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_GEO1_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeomCollection, ObGeometry *)
{
  return eval_difference_gc_other<ObCartesianGeometrycollection,
      ObWkbGeomCollection>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_BINARY_FUNC_GEO2_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeomCollection, ObGeometry *)
{
  return eval_difference_gc_other<ObCartesianGeometrycollection,
      ObWkbGeomCollection>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

// geographic point
OB_GEO_GEOG_BINARY_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeogPoint, ObWkbGeogPoint, ObGeometry *)
{
  return apply_bg_difference<ObWkbGeogPoint, ObWkbGeogPoint, ObGeographMultipoint>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeogPoint, ObWkbGeogLineString, ObGeometry *)
{
  return apply_bg_difference_pl_strategy<ObWkbGeogPoint, ObWkbGeogLineString, ObGeographMultipoint>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeogPoint, ObWkbGeogPolygon, ObGeometry *)
{
  return apply_bg_difference_pl_strategy<ObWkbGeogPoint, ObWkbGeogPolygon, ObGeographMultipoint>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeogPoint, ObWkbGeogMultiPoint, ObGeometry *)
{
  return apply_bg_difference<ObWkbGeogPoint, ObWkbGeogMultiPoint, ObGeographMultipoint>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeogPoint, ObWkbGeogMultiLineString, ObGeometry *)
{
  return apply_bg_difference_pl_strategy<ObWkbGeogPoint,
      ObWkbGeogMultiLineString,
      ObGeographMultipoint>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeogPoint, ObWkbGeogMultiPolygon, ObGeometry *)
{
  return apply_bg_difference_pl_strategy<ObWkbGeogPoint,
      ObWkbGeogMultiPolygon,
      ObGeographMultipoint>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

// geographic linestring
OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeogLineString, ObWkbGeogPoint, ObGeometry *)
{
  // if g1.dimension > g2.dimension, g1 - g2 is equal g1(mysql/postgis)
  // so just convert g1 to tree
  return ObGeoFuncUtils::apply_bg_to_tree(g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeogLineString, ObWkbGeogLineString, ObGeometry *)
{
  return apply_bg_difference_ll_strategy<ObWkbGeogLineString,
      ObWkbGeogLineString,
      ObGeographMultilinestring>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeogLineString, ObWkbGeogPolygon, ObGeometry *)
{
  return apply_bg_difference_ll_strategy<ObWkbGeogLineString,
      ObWkbGeogPolygon,
      ObGeographMultilinestring>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeogLineString, ObWkbGeogMultiPoint, ObGeometry *)
{
  // if g1.dimension > g2.dimension, g1 - g2 is equal g1(mysql/postgis)
  // so just convert g1 to tree
  return ObGeoFuncUtils::apply_bg_to_tree(g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeogLineString, ObWkbGeogMultiLineString, ObGeometry *)
{
  return apply_bg_difference_ll_strategy<ObWkbGeogLineString,
      ObWkbGeogMultiLineString,
      ObGeographMultilinestring>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeogLineString, ObWkbGeogMultiPolygon, ObGeometry *)
{
  return apply_bg_difference_ll_strategy<ObWkbGeogLineString,
      ObWkbGeogMultiPolygon,
      ObGeographMultilinestring>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

// gepgraphic polygon
OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeogPolygon, ObWkbGeogPoint, ObGeometry *)
{
  // if g1.dimension > g2.dimension, g1 - g2 is equal g1(mysql/postgis)
  // so just convert g1 to tree
  return ObGeoFuncUtils::apply_bg_to_tree(g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeogPolygon, ObWkbGeogLineString, ObGeometry *)
{
  // if g1.dimension > g2.dimension, g1 - g2 is equal g1(mysql/postgis)
  // so just convert g1 to tree
  return ObGeoFuncUtils::apply_bg_to_tree(g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeogPolygon, ObWkbGeogPolygon, ObGeometry *)
{
  return apply_bg_difference_ll_strategy<ObWkbGeogPolygon,
      ObWkbGeogPolygon,
      ObGeographMultipolygon>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeogPolygon, ObWkbGeogMultiPoint, ObGeometry *)
{
  // if g1.dimension > g2.dimension, g1 - g2 is equal g1(mysql/postgis)
  // so just convert g1 to tree
  return ObGeoFuncUtils::apply_bg_to_tree(g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeogPolygon, ObWkbGeogMultiLineString, ObGeometry *)
{
  // if g1.dimension > g2.dimension, g1 - g2 is equal g1(mysql/postgis)
  // so just convert g1 to tree
  return ObGeoFuncUtils::apply_bg_to_tree(g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeogPolygon, ObWkbGeogMultiPolygon, ObGeometry *)
{
  return apply_bg_difference_ll_strategy<ObWkbGeogPolygon,
      ObWkbGeogMultiPolygon,
      ObGeographMultipolygon>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

// geographic multipoint
OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeogMultiPoint, ObWkbGeogPoint, ObGeometry *)
{
  return apply_bg_difference<ObWkbGeogMultiPoint, ObWkbGeogPoint, ObGeographMultipoint>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeogMultiPoint, ObWkbGeogLineString, ObGeometry *)
{
  return apply_bg_difference_pl_strategy<ObWkbGeogMultiPoint,
      ObWkbGeogLineString,
      ObGeographMultipoint>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeogMultiPoint, ObWkbGeogPolygon, ObGeometry *)
{
  return apply_bg_difference_pl_strategy<ObWkbGeogMultiPoint,
      ObWkbGeogPolygon,
      ObGeographMultipoint>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeogMultiPoint, ObWkbGeogMultiPoint, ObGeometry *)
{
  return apply_bg_difference<ObWkbGeogMultiPoint, ObWkbGeogMultiPoint, ObGeographMultipoint>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeogMultiPoint, ObWkbGeogMultiLineString, ObGeometry *)
{
  return apply_bg_difference_pl_strategy<ObWkbGeogMultiPoint,
      ObWkbGeogMultiLineString,
      ObGeographMultipoint>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeogMultiPoint, ObWkbGeogMultiPolygon, ObGeometry *)
{
  return apply_bg_difference_pl_strategy<ObWkbGeogMultiPoint,
      ObWkbGeogMultiPolygon,
      ObGeographMultipoint>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

// geographic multilinestring
OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeogMultiLineString, ObWkbGeogPoint, ObGeometry *)
{
  // if g1.dimension > g2.dimension, g1 - g2 is equal g1(mysql/postgis)
  // so just convert g1 to tree
  return ObGeoFuncUtils::apply_bg_to_tree(g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeogMultiLineString, ObWkbGeogLineString, ObGeometry *)
{
  return apply_bg_difference_ll_strategy<ObWkbGeogMultiLineString,
      ObWkbGeogLineString,
      ObGeographMultilinestring>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeogMultiLineString, ObWkbGeogPolygon, ObGeometry *)
{
  return apply_bg_difference_ll_strategy<ObWkbGeogMultiLineString,
      ObWkbGeogPolygon,
      ObGeographMultilinestring>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeogMultiLineString, ObWkbGeogMultiPoint, ObGeometry *)
{
  // if g1.dimension > g2.dimension, g1 - g2 is equal g1(mysql/postgis)
  // so just convert g1 to tree
  return ObGeoFuncUtils::apply_bg_to_tree(g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeogMultiLineString, ObWkbGeogMultiLineString, ObGeometry *)
{
  return apply_bg_difference_ll_strategy<ObWkbGeogMultiLineString,
      ObWkbGeogMultiLineString,
      ObGeographMultilinestring>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeogMultiLineString, ObWkbGeogMultiPolygon, ObGeometry *)
{
  return apply_bg_difference_ll_strategy<ObWkbGeogMultiLineString,
      ObWkbGeogMultiPolygon,
      ObGeographMultilinestring>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

// geographic mutlipolygon
OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeogMultiPolygon, ObWkbGeogPoint, ObGeometry *)
{
  // if g1.dimension > g2.dimension, g1 - g2 is equal g1(mysql/postgis)
  // so just convert g1 to tree
  return ObGeoFuncUtils::apply_bg_to_tree(g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeogMultiPolygon, ObWkbGeogLineString, ObGeometry *)
{
  // if g1.dimension > g2.dimension, g1 - g2 is equal g1(mysql/postgis)
  // so just convert g1 to tree
  return ObGeoFuncUtils::apply_bg_to_tree(g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeogMultiPolygon, ObWkbGeogPolygon, ObGeometry *)
{
  return apply_bg_difference_ll_strategy<ObWkbGeogMultiPolygon,
      ObWkbGeogPolygon,
      ObGeographMultipolygon>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeogMultiPolygon, ObWkbGeogMultiPoint, ObGeometry *)
{
  // if g1.dimension > g2.dimension, g1 - g2 is equal g1(mysql/postgis)
  // so just convert g1 to tree
  return ObGeoFuncUtils::apply_bg_to_tree(g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeogMultiPolygon, ObWkbGeogMultiLineString, ObGeometry *)
{
  // if g1.dimension > g2.dimension, g1 - g2 is equal g1(mysql/postgis)
  // so just convert g1 to tree
  return ObGeoFuncUtils::apply_bg_to_tree(g1, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeogMultiPolygon, ObWkbGeogMultiPolygon, ObGeometry *)
{
  return apply_bg_difference_ll_strategy<ObWkbGeogMultiPolygon,
      ObWkbGeogMultiPolygon,
      ObGeographMultipolygon>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

// cartesian geometry collection
OB_GEO_GEOG_BINARY_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObWkbGeogCollection, ObWkbGeogCollection, ObGeometry *)
{
  return eval_difference_gc_other<ObGeographGeometrycollection,
      ObWkbGeogCollection>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_GEO1_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeogCollection, ObGeometry *)
{
  return eval_difference_gc_other<ObGeographGeometrycollection,
      ObWkbGeogCollection>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_BINARY_FUNC_GEO2_BEGIN(ObGeoFuncDifferenceImpl, ObWkbGeogCollection, ObGeometry *)
{
  return eval_difference_gc_other<ObGeographGeometrycollection,
      ObWkbGeogCollection>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_TREE_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObCartesianMultipoint, ObCartesianMultipoint, ObGeometry *)
{
  return apply_bg_difference<ObCartesianMultipoint, ObCartesianMultipoint, ObCartesianMultipoint>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

// tree cartesian polygon
OB_GEO_CART_TREE_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObCartesianMultipoint, ObCartesianMultilinestring, ObGeometry *)
{
  return apply_bg_difference<ObCartesianMultipoint,
      ObCartesianMultilinestring,
      ObCartesianMultipoint>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_TREE_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObCartesianPolygon, ObCartesianPolygon, ObGeometry *)
{
  return apply_bg_difference<ObCartesianPolygon, ObCartesianPolygon, ObCartesianMultipolygon>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_TREE_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObCartesianPolygon, ObCartesianMultipolygon, ObGeometry *)
{
  return apply_bg_difference<ObCartesianPolygon, ObCartesianMultipolygon, ObCartesianMultipolygon>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_TREE_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObCartesianMultipolygon, ObCartesianPolygon, ObGeometry *)
{
  return apply_bg_difference<ObCartesianMultipolygon, ObCartesianPolygon, ObCartesianMultipolygon>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_TREE_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObCartesianMultipoint, ObCartesianMultipolygon, ObGeometry *)
{
  return apply_bg_difference<ObCartesianMultipoint, ObCartesianMultipolygon, ObCartesianMultipoint>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_TREE_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObCartesianLineString, ObCartesianMultilinestring, ObGeometry *)
{
  return apply_bg_difference<ObCartesianLineString,
      ObCartesianMultilinestring,
      ObCartesianMultilinestring>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_TREE_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObCartesianMultilinestring, ObCartesianMultilinestring, ObGeometry *)
{
  return apply_bg_difference<ObCartesianMultilinestring,
      ObCartesianMultilinestring,
      ObCartesianMultilinestring>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_TREE_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObCartesianMultilinestring, ObCartesianMultipolygon, ObGeometry *)
{
  return apply_bg_difference<ObCartesianMultilinestring,
      ObCartesianMultipolygon,
      ObCartesianMultilinestring>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_CART_TREE_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObCartesianPoint, ObCartesianMultilinestring, ObGeometry *)
{
  return apply_bg_difference<ObCartesianPoint, ObCartesianMultilinestring, ObCartesianMultipoint>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_TREE_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObCartesianPolygon, ObCartesianMultilinestring, ObGeometry *)
{
  // if g1.dimension > g2.dimension, g1 - g2 is equal g1(mysql/postgis)
  // so just return g1
  result = const_cast<ObGeometry *>(reinterpret_cast<const ObGeometry *>(g1));
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_CART_TREE_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObCartesianMultipolygon, ObCartesianMultilinestring, ObGeometry *)
{
  // if g1.dimension > g2.dimension, g1 - g2 is equal g1(mysql/postgis)
  // so just return g1
  result = const_cast<ObGeometry *>(reinterpret_cast<const ObGeometry *>(g1));
  return OB_SUCCESS;
} OB_GEO_FUNC_END;

OB_GEO_CART_TREE_FUNC_BEGIN(ObGeoFuncDifferenceImpl, ObCartesianGeometrycollection, ObCartesianMultilinestring, ObGeometry *)
{
  return apply_bg_difference_collection<ObCartesianGeometrycollection, ObCartesianMultipoint, ObCartesianMultilinestring,
                                        ObCartesianMultipolygon>(g1, g2, context, result);
} OB_GEO_FUNC_END;

OB_GEO_CART_TREE_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObCartesianMultipolygon, ObCartesianMultipolygon, ObGeometry *)
{
  return apply_bg_difference<ObCartesianMultipolygon,
      ObCartesianMultipolygon,
      ObCartesianMultipolygon>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_TREE_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObGeographMultipoint, ObGeographMultipoint, ObGeometry *)
{
  return apply_bg_difference<ObGeographMultipoint, ObGeographMultipoint, ObGeographMultipoint>(
      g1, g2, context, result);
}
OB_GEO_FUNC_END;

// tree geograph polygon
OB_GEO_GEOG_TREE_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObGeographMultipoint, ObGeographMultilinestring, ObGeometry *)
{
  return apply_bg_difference_pl_strategy<ObGeographMultipoint,
      ObGeographMultilinestring,
      ObGeographMultipoint>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_TREE_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObGeographMultipoint, ObGeographMultipolygon, ObGeometry *)
{
  return apply_bg_difference_pl_strategy<ObGeographMultipoint,
      ObGeographMultipolygon,
      ObGeographMultipoint>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_TREE_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObGeographLineString, ObGeographMultilinestring, ObGeometry *)
{
  return apply_bg_difference_ll_strategy<ObGeographLineString,
      ObGeographMultilinestring,
      ObGeographMultilinestring>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_TREE_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObGeographMultilinestring, ObGeographMultilinestring, ObGeometry *)
{
  return apply_bg_difference_ll_strategy<ObGeographMultilinestring,
      ObGeographMultilinestring,
      ObGeographMultilinestring>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_TREE_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObGeographMultilinestring, ObGeographMultipolygon, ObGeometry *)
{
  return apply_bg_difference_ll_strategy<ObGeographMultilinestring,
      ObGeographMultipolygon,
      ObGeographMultilinestring>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

OB_GEO_GEOG_TREE_FUNC_BEGIN(
    ObGeoFuncDifferenceImpl, ObGeographMultipolygon, ObGeographMultipolygon, ObGeometry *)
{
  return apply_bg_difference_ll_strategy<ObGeographMultipolygon,
      ObGeographMultipolygon,
      ObGeographMultipolygon>(g1, g2, context, result);
}
OB_GEO_FUNC_END;

// implement of outer class eval
// use an outer class to void implement templates in header files
int ObGeoFuncDifference::eval(const ObGeoEvalCtx &gis_context, ObGeometry *&result)
{
  return ObGeoFuncDifferenceImpl::eval_geo_func(gis_context, result);
}

}  // namespace common
}  // namespace oceanbase
