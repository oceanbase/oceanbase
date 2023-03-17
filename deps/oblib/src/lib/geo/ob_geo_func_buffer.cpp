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
 * This file contains implementation for ob_geo_func_buffer.
 */

#define USING_LOG_PREFIX LIB

#include "lib/geo/ob_geo_dispatcher.h"
#include "lib/geo/ob_geo_func_buffer.h"
#include "lib/geo/ob_geo_func_union.h"
#include "lib/geo/ob_geo_func_transform.h"
#include "lib/geo/ob_geo_func_utils.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/geo/ob_geo_tree.h"
#include "lib/geo/ob_geo_to_tree_visitor.h"
#include "lib/geo/ob_geo_longtitude_correct_visitor.h"
#include "lib/geo/ob_geo_wkb_visitor.h"
#include "lib/geo/ob_geo_wkb_size_visitor.h"
#include "lib/geo/ob_geo_utils.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_hang_fatal_error.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace common
{

namespace bg = boost::geometry;

class ObGeoFuncBufferImpl : public ObIGeoDispatcher<ObGeometry *, ObGeoFuncBufferImpl>
{
public:
  ObGeoFuncBufferImpl();
  virtual ~ObGeoFuncBufferImpl() = default;

  // default templates
  OB_GEO_UNARY_FUNC_DEFAULT(ObGeometry *, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);
  OB_GEO_TREE_UNARY_FUNC_DEFAULT(ObGeometry *, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);
  OB_GEO_CART_BINARY_FUNC_DEFAULT(ObGeometry *, OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS);
  OB_GEO_GEOG_BINARY_FUNC_DEFAULT(ObGeometry *, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);
  OB_GEO_CART_TREE_FUNC_DEFAULT(ObGeometry *, OB_ERR_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS);
  OB_GEO_GEOG_TREE_FUNC_DEFAULT(ObGeometry *, OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS);

private:
  template<typename GeometryType>
  static bool apply_bg_equal(GeometryType &geo1,
                             GeometryType &geo2,
                             const ObSrsItem *srs);
  static bool apply_bg_equal(ObGeographLineString &geo1,
                             ObGeographLineString &geo2,
                             const ObSrsItem *srs);
  static bool apply_bg_equal(ObGeographPolygon &geo1,
                             ObGeographPolygon &geo2,
                             const ObSrsItem *srs);
  template<typename MultiPointType, typename MultiLineType, typename MultiPolygonType>
  static int apply_bg_remove_duplicate_geo(ObIAllocator &allocator,
                                           const ObSrsItem *srs,
                                           ObGeometry *&geo);
  static int remove_duplicate_geo(ObIAllocator &allocator,
                                  const ObSrsItem *srs,
                                  ObGeometry *&geo);
  template <typename GeoTreeType>
  static int unwrap_geo_tree_inner(ObGeometry *geo_in,
                                   ObGeometry *&geo_out);
  static int unwrap_geometry_tree(ObGeometry *geo_in,
                                  ObGeometry *&geo_out);

  // check if strategy is valid for geometry calss
  static bool is_valid_for_cartpoint(const ObGeoBufferStrategy &strat)
  {
    return strat.distance_val_ >= 0 &&!strat.has_end_s_ && !strat.has_join_s_;
  }
  static bool is_valid_for_linestring(const ObGeoBufferStrategy &strat)
  {
    return strat.distance_val_ >= 0 && !strat.has_point_s_;
  }
  static bool is_valid_for_polygon(const ObGeoBufferStrategy &strat)
  {
    return !strat.has_point_s_ && !strat.has_end_s_;
  }
  static bool is_valid_for_geogpoint(const ObGeoBufferStrategy &strat)
  {
    return strat.distance_val_ >= 0 && !strat.has_point_s_ && !strat.has_end_s_ && !strat.has_join_s_;
  }
  static int ob_geo_validate_buffer_input(const ObGeometry *g, const ObGeoEvalCtx &ctx)
  {
    INIT_SUCC(ret);
    if (ctx.get_val_count() < 1) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid context argument count", K(ret), K(ctx.get_val_count()));
    } else if (OB_ISNULL(ctx.get_allocator())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid context without allocator", K(ret));
    }
    return ret;
  }

  // cartisan
  template <typename GeomTreeType, typename GeomIType>
  static int eval_buffer_cartisan(const ObGeometry *g,
                                  const ObGeoEvalCtx &context,
                                  const ObGeoBufferStrategy &strategy,
                                  ObGeometry *&result,
                                  bool is_tree)
  {
    INIT_SUCC(ret);
    uint32_t srid = g->get_srid();

    // init buffer context for cartisan
    bg::strategy::buffer::distance_symmetric<double> distance_s(strategy.distance_val_);
    bg::strategy::buffer::side_straight side_s;

    bg::strategy::buffer::join_round join_round_s(strategy.join_round_val_);
    bg::strategy::buffer::join_miter join_miter_s(strategy.join_miter_val_);
    bg::strategy::buffer::end_round end_round_s(strategy.end_round_val_);
    bg::strategy::buffer::end_flat end_flat_s;
    bg::strategy::buffer::point_circle point_circle_s(strategy.point_circle_val_);
    bg::strategy::buffer::point_square point_square_s;

    // bg::buffer requires input and output the same type, and output type must be tree type.
    // convert to tree type before eval buffer
    // buffer result type must be multipolygon
    common::ObIAllocator *allocator = context.get_allocator();
    ObGeoToTreeVisitor visitor(allocator);
    GeomTreeType *geo_tree = NULL;
    if (is_tree == false) {
      GeomIType *i_geo = const_cast<GeomIType *>(reinterpret_cast<const GeomIType *>(g));
      if (OB_FAIL(i_geo->do_visit(visitor))) {
        LOG_WARN("failed to do geo to tree visit", K(ret));
      } else {
        geo_tree = static_cast<GeomTreeType *>(visitor.get_geometry());
      }
    } else {
      geo_tree = const_cast<GeomTreeType *>(reinterpret_cast<const GeomTreeType *>(g));
    }

    ObCartesianMultipolygon *geo_res =  OB_NEWx(ObCartesianMultipolygon , allocator, srid, *allocator);
    if (OB_ISNULL(geo_tree) || OB_ISNULL(geo_res)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret), KP(geo_tree), KP(geo_res));
    } else {
      switch (ObGeoBufferStrategyStateType(strategy.state_num_)) {
        case ObGeoBufferStrategyStateType::JR_ER_PC: {
          bg::buffer(*geo_tree, *geo_res, distance_s, side_s, join_round_s, end_round_s, point_circle_s);
          break;
        }
        case ObGeoBufferStrategyStateType::JR_ER_PS: {
          bg::buffer(*geo_tree, *geo_res, distance_s, side_s, join_round_s, end_round_s, point_square_s);
          break;
        }
        case ObGeoBufferStrategyStateType::JR_EF_PC: {
          bg::buffer(*geo_tree, *geo_res, distance_s, side_s, join_round_s, end_flat_s, point_circle_s);
          break;
        }
        case ObGeoBufferStrategyStateType::JR_EF_PS: {
          bg::buffer(*geo_tree, *geo_res, distance_s, side_s, join_round_s, end_flat_s, point_square_s);
          break;
        }
        case ObGeoBufferStrategyStateType::JM_ER_PC: {
          bg::buffer(*geo_tree, *geo_res, distance_s, side_s, join_miter_s, end_round_s, point_circle_s);
          break;
        }
        case ObGeoBufferStrategyStateType::JM_ER_PS: {
          bg::buffer(*geo_tree, *geo_res, distance_s, side_s, join_miter_s, end_round_s, point_square_s);
          break;
        }
        case ObGeoBufferStrategyStateType::JM_EF_PC: {
          bg::buffer(*geo_tree, *geo_res, distance_s, side_s, join_miter_s, end_flat_s, point_circle_s);
          break;
        }
        case ObGeoBufferStrategyStateType::JM_EF_PS: {
          bg::buffer(*geo_tree, *geo_res, distance_s, side_s, join_miter_s, end_flat_s, point_square_s);
          break;
        }
        default: {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid strategy state num", K(ret), K(strategy.state_num_));
          break;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (geo_res->size() == 0) {
        // empty result
        if ((strategy.distance_val_ < 0)
            && (ObGeoType::POLYGON == geo_tree->type() || ObGeoType::MULTIPOLYGON == geo_tree->type())) {
          ObCartesianGeometrycollection *empty_res =
            OB_NEWx(ObCartesianGeometrycollection, allocator, srid, *allocator);
          if (OB_ISNULL(empty_res)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocate memory", K(ret));
          } else {
            result = empty_res;
          }
        } else {
          ret = OB_ERR_GIS_UNKNOWN_ERROR;
          LOG_WARN("invalid buffer distance", K(ret), K(strategy.distance_val_), K(geo_tree->type()));
        }
      } else if (OB_FAIL(unwrap_geometry_tree(geo_res, result))) {
        LOG_WARN("unwrap buffer result failed", K(ret));
      } else {
        // do nothing
      }
    }
    return ret;
  }

  static int eval_buffer_cartisan_collection (const ObGeometry *g,
                                              const ObGeoEvalCtx &context,
                                              ObGeometry *&result,
                                              bool is_tree)
  {
    INIT_SUCC(ret);
    uint32_t srid = g->get_srid();
    bool is_empty_res = false;
    ObCartesianMultipoint *mpt = NULL;
    ObCartesianMultilinestring *ml = NULL;
    ObCartesianMultipolygon *mpo = NULL;
    const ObGeoBufferStrategy *strategy = NULL;

    // results must be multipolygon
    ObCartesianMultipolygon *mpt_res = NULL;
    ObCartesianMultipolygon *ml_res = NULL;
    ObCartesianMultipolygon *mpo_res = NULL;
    ObCartesianMultipolygon *geo_res = NULL;
    ObGeometry *dedup_pt_ptr = NULL;
    ObGeometry *dedup_ml_ptr = NULL;
    ObGeometry *dedup_mpo_ptr = NULL;

    ObCartesianGeometrycollection *geo_tree = NULL;
    common::ObIAllocator *allocator = context.get_allocator();
    const ObSrsItem *srs = context.get_srs();

    if (OB_FAIL(ob_geo_validate_buffer_input(g, context))) {
      // do nothing, log inside validate function
    } else if (g->is_empty()) {
      is_empty_res = true;
    } else if ((srid != 0) && OB_ISNULL(srs)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid strategy for cartisan collection with null srs", K(srid), K(ret));
    } else {
      ObGeoToTreeVisitor visitor(allocator);
      if (is_tree == false) {
        ObIWkbGeomCollection *i_geo =
          const_cast<ObIWkbGeomCollection *>(reinterpret_cast<const ObIWkbGeomCollection *>(g));
        if (OB_FAIL(i_geo->do_visit(visitor))) {
          LOG_WARN("failed to do geo to tree visit", K(ret));
        } else {
          geo_tree = static_cast<ObCartesianGeometrycollection *>(visitor.get_geometry());
        }
      } else {
        geo_tree =
          const_cast<ObCartesianGeometrycollection *>(reinterpret_cast<const ObCartesianGeometrycollection *>(g));
      }

      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_ISNULL(geo_tree)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("failed to convert to geo tree", K(ret), KP(geo_res));
      } else if (OB_ISNULL((geo_res = OB_NEWx(ObCartesianMultipolygon , allocator, srid, *allocator)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret), KP(geo_res));
      } else if (OB_FAIL(ObGeoFuncUtils::ob_geo_gc_split(*allocator, *geo_tree, mpt, ml, mpo))) {
        LOG_WARN("failed to do geometry collection split", K(ret));
      } else if (OB_ISNULL(mpt) || OB_ISNULL(ml) || OB_ISNULL(mpo)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null geometry collection split", K(ret), KP(mpt), KP(ml), KP(mpo));
      } else if (OB_FAIL(ObGeoFuncUtils::ob_geo_gc_union(*allocator, *srs, mpt, ml, mpo))) {
        LOG_WARN("failed to do geometry collection union", K(ret));
      } else if (OB_ISNULL(mpt) || OB_ISNULL(ml) || OB_ISNULL(mpo)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null geometry collection union", K(ret), KP(mpt), KP(ml), KP(mpo));
      } else if (OB_ISNULL(strategy = context.get_val_arg(0)->strategy_)) {
        ret = OB_INVALID_ARGUMENT;
      } else if (strategy->distance_val_ < 0 && !(mpt->empty() && ml->empty())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("negative distance is only permitted for geometry collection with only (multi)polygon", K(ret));
      } else if (OB_NOT_NULL(mpt)
          && (dedup_pt_ptr = reinterpret_cast<ObGeometry *>(mpt))
          && OB_FAIL(remove_duplicate_geo(*allocator, srs, dedup_pt_ptr))) {
        LOG_WARN("failed to deduplicate points", K(ret));
      } else if (OB_NOT_NULL(ml) && (dedup_ml_ptr = reinterpret_cast<ObGeometry *>(ml))
          && OB_FAIL(remove_duplicate_geo(*allocator, srs, dedup_ml_ptr))) {
        LOG_WARN("failed to deduplicate lines", K(ret));
      } else if (OB_NOT_NULL(mpo) && (dedup_mpo_ptr = reinterpret_cast<ObGeometry *>(mpo))
          && OB_FAIL(remove_duplicate_geo(*allocator, srs, dedup_mpo_ptr))) {
        LOG_WARN("failed to deduplicate polygons", K(ret));
      } else if (OB_ISNULL(mpt_res = OB_NEWx(ObCartesianMultipolygon, allocator, srid, *allocator))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret));
      } else if (OB_ISNULL(ml_res = OB_NEWx(ObCartesianMultipolygon, allocator, srid, *allocator))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret));
      } else if (OB_ISNULL(mpo_res = OB_NEWx(ObCartesianMultipolygon, allocator, srid, *allocator))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret));
      } else {
        mpt = reinterpret_cast<ObCartesianMultipoint *>(dedup_pt_ptr);
        ml = reinterpret_cast<ObCartesianMultilinestring *>(dedup_ml_ptr);
        mpo = reinterpret_cast<ObCartesianMultipolygon *>(dedup_mpo_ptr);

        // param 1, 2
        bg::strategy::buffer::distance_symmetric<double> distance_s(strategy->distance_val_);
        bg::strategy::buffer::side_straight side_s;
        // param 3
        bg::strategy::buffer::join_round join_round_s(strategy->join_round_val_);
        bg::strategy::buffer::join_miter join_miter_s(strategy->join_miter_val_);
        // param 4
        bg::strategy::buffer::end_round end_round_s(strategy->end_round_val_);
        bg::strategy::buffer::end_flat end_flat_s;
        // param 5
        bg::strategy::buffer::point_circle point_circle_s(strategy->point_circle_val_);
        bg::strategy::buffer::point_square point_square_s;

        switch (ObGeoBufferStrategyStateType(strategy->state_num_)) {
          case ObGeoBufferStrategyStateType::JR_ER_PC: {
            bg::buffer(*mpt, *mpt_res, distance_s, side_s, join_round_s, end_round_s, point_circle_s);
            bg::buffer(*ml, *ml_res, distance_s, side_s, join_round_s, end_round_s, point_circle_s);
            bg::buffer(*mpo, *mpo_res, distance_s, side_s, join_round_s, end_round_s, point_circle_s);
            break;
          }
          case ObGeoBufferStrategyStateType::JR_ER_PS: {
            bg::buffer(*mpt, *mpt_res, distance_s, side_s, join_round_s, end_round_s, point_square_s);
            bg::buffer(*ml, *ml_res, distance_s, side_s, join_round_s, end_round_s, point_square_s);
            bg::buffer(*mpo, *mpo_res, distance_s, side_s, join_round_s, end_round_s, point_square_s);
            break;
          }
          case ObGeoBufferStrategyStateType::JR_EF_PC: {
            bg::buffer(*mpt, *mpt_res, distance_s, side_s, join_round_s, end_flat_s, point_circle_s);
            bg::buffer(*ml, *ml_res, distance_s, side_s, join_round_s, end_flat_s, point_circle_s);
            bg::buffer(*mpo, *mpo_res, distance_s, side_s, join_round_s, end_flat_s, point_circle_s);
            break;
          }
          case ObGeoBufferStrategyStateType::JR_EF_PS: {
            bg::buffer(*mpt, *mpt_res, distance_s, side_s, join_round_s, end_flat_s, point_square_s);
            bg::buffer(*ml, *ml_res, distance_s, side_s, join_round_s, end_flat_s, point_square_s);
            bg::buffer(*mpo, *mpo_res, distance_s, side_s, join_round_s, end_flat_s, point_square_s);
            break;
          }
          case ObGeoBufferStrategyStateType::JM_ER_PC: {
            bg::buffer(*mpt, *mpt_res, distance_s, side_s, join_miter_s, end_round_s, point_circle_s);
            bg::buffer(*ml, *ml_res, distance_s, side_s, join_miter_s, end_round_s, point_circle_s);
            bg::buffer(*mpo, *mpo_res, distance_s, side_s, join_miter_s, end_round_s, point_circle_s);
            break;
          }
          case ObGeoBufferStrategyStateType::JM_ER_PS: {
            bg::buffer(*mpt, *mpt_res, distance_s, side_s, join_miter_s, end_round_s, point_square_s);
            bg::buffer(*ml, *ml_res, distance_s, side_s, join_miter_s, end_round_s, point_square_s);
            bg::buffer(*mpo, *mpo_res, distance_s, side_s, join_miter_s, end_round_s, point_square_s);
            break;
          }
          case ObGeoBufferStrategyStateType::JM_EF_PC: {
            bg::buffer(*mpt, *mpt_res, distance_s, side_s, join_miter_s, end_flat_s, point_circle_s);
            bg::buffer(*ml, *ml_res, distance_s, side_s, join_miter_s, end_flat_s, point_circle_s);
            bg::buffer(*mpo, *mpo_res, distance_s, side_s, join_miter_s, end_flat_s, point_circle_s);
            break;
          }
          case ObGeoBufferStrategyStateType::JM_EF_PS: {
            bg::buffer(*mpt, *mpt_res, distance_s, side_s, join_miter_s, end_flat_s, point_square_s);
            bg::buffer(*ml, *ml_res, distance_s, side_s, join_miter_s, end_flat_s, point_square_s);
            bg::buffer(*mpo, *mpo_res, distance_s, side_s, join_miter_s, end_flat_s, point_square_s);
            break;
          }
          default: {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("error strategy state number", K(ret), K(strategy->state_num_));
            break;
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (is_empty_res) {
        result = OB_NEWx(ObCartesianGeometrycollection, allocator, srid, *allocator);
        if (OB_ISNULL(result)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory for empty result", K(ret), K(srid), KP(result));
        }
      } else {
        ObGeometry *temp_geo = NULL;
        ObGeoEvalCtx union_context_mpt_ml(allocator, srs);
        union_context_mpt_ml.append_geo_arg(mpt_res);
        union_context_mpt_ml.append_geo_arg(ml_res);
        if (OB_FAIL(ObGeoFuncUnion::eval(union_context_mpt_ml, temp_geo))) {
          LOG_WARN("union buffer result of multipoints and multistringline failed", K(ret));
        } else {
          ObGeoEvalCtx union_context_mpt_ml_mpo(allocator, srs);
          union_context_mpt_ml_mpo.append_geo_arg(temp_geo);
          union_context_mpt_ml_mpo.append_geo_arg(mpo_res);
          if (OB_FAIL(ObGeoFuncUnion::eval(union_context_mpt_ml_mpo, temp_geo))) {
            LOG_WARN("union buffer result of multipolygon failed", K(ret));
          } else if (temp_geo->is_empty()) {
            result = OB_NEWx(ObCartesianGeometrycollection, allocator, srid, *allocator);
            if (OB_ISNULL(result)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("failed to allocate memory for empty result", K(ret), K(srid), KP(result));
            }
          } else if (OB_FAIL(unwrap_geometry_tree(temp_geo, result))) {
            LOG_WARN("unwrap buffer result of collection failed", K(ret));
          } else {
            // do nothing
          }
        }
      }
    }
    return ret;
  }

  template <typename GeomTreeType, typename GeomIType>
  static int eval_buffer_geographic(const ObGeometry *g,
                                    const ObGeoEvalCtx &context,
                                    ObGeometry *&result,
                                    bool is_collection)
  {
    INIT_SUCC(ret);
    if (OB_SUCC(ob_geo_validate_buffer_input(g, context))) {
      ObGeoBufferStrategy *strategy = context.get_val_arg(0)->strategy_;
      // Notice: transfrom context use dest srs
      common::ObIAllocator *allocator = context.get_allocator();
      ObGeoEvalCtx transfrom_proj_context(allocator, strategy->srs_proj_);
      ObGeoEvalCtx transfrom_wgs84_context(allocator, strategy->srs_wgs84_);
      ObGeometry *projected_geo = NULL;
      ObGeometry *projected_result = NULL;
      ObGeometry *projected_bin = NULL;
      ObGeometry *wgs84_geo = NULL;
      ObString buffered_proj_wkb;
      bool is_empty_res = false;

      if (g->is_empty()) {
        is_empty_res = true;
      } else if (OB_FAIL(transfrom_proj_context.append_geo_arg(g))) {
        LOG_WARN("failed to append geo arg to gis context", K(ret),
          K(transfrom_proj_context.get_geo_count()));
      } else if (OB_FAIL(transfrom_proj_context.append_val_arg(&strategy->proj4_self_))) {
        LOG_WARN("failed to append src proj4_param to proj_context", K(ret), K(strategy->proj4_self_));
      } else if (OB_FAIL(transfrom_proj_context.append_val_arg(&strategy->proj4_proj_))) {
        LOG_WARN("failed to append dest proj4_param to proj_context", K(ret), K(strategy->proj4_proj_));
      } else if (OB_FAIL(ObGeoFuncTransform::eval(transfrom_proj_context, projected_geo))) {
        LOG_WARN("failed to transfrom to proj srs", K(ret), K(strategy->proj4_self_), K(strategy->proj4_proj_));
      } else {
        if (!is_collection) {
          projected_geo->set_srid(strategy->srs_proj_->get_srid());
          ret = eval_buffer_cartisan<GeomTreeType, GeomIType>(projected_geo,
                                                              context,
                                                              *strategy,
                                                              projected_result,
                                                              true);
        } else {
          // Notice: the original context is used. should not use geo objects or original srs in context
          projected_geo->set_srid(strategy->srs_proj_->get_srid());
          ret = eval_buffer_cartisan_collection(projected_geo, context, projected_result, true);
        }
      }

      if (OB_FAIL(ret)) {
        LOG_WARN("eval buffer failed", K(ret), K(is_collection));
      } else if (is_empty_res) {
        // Notice: mysql only return emtpy geometry collection, but it does not support geographic types
        // return empty polygon for geographic types like postgis
        result = OB_NEWx(ObGeographPolygon, allocator, OB_GEO_DEFAULT_GEOGRAPHY_SRID, *allocator);
        if (OB_ISNULL(result)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory for empty geographic result", K(ret), KP(result));
        }
      } else if (OB_FAIL(ObGeoTypeUtil::to_wkb(*allocator, *projected_result,
          strategy->srs_proj_, buffered_proj_wkb))) {
        LOG_WARN("fail to to_wkb for projected buffer", K(ret));
      } else if (OB_FAIL(ObGeoTypeUtil::create_geo_by_wkb(*allocator, buffered_proj_wkb,
          strategy->srs_proj_, projected_bin, false))) {
        LOG_WARN("fail to create geo bin for projected buffer", K(ret));
      } else if (OB_FAIL(transfrom_wgs84_context.append_geo_arg(projected_bin))) {
        LOG_WARN("failed to append geo arg to gis context", K(ret),
          K(transfrom_wgs84_context.get_geo_count()));
      } else if (OB_FAIL(transfrom_wgs84_context.append_val_arg(&strategy->proj4_proj_))) {
        LOG_WARN("failed to append src proj4_param to wgs84_context", K(ret), K(strategy->proj4_proj_));
      } else if (OB_FAIL(transfrom_wgs84_context.append_val_arg(&strategy->proj4_wgs84_))) {
        LOG_WARN("failed to append dest proj4_param to wgs84_context", K(ret), K(strategy->proj4_wgs84_));
      } else if (OB_FAIL(ObGeoFuncTransform::eval(transfrom_wgs84_context, wgs84_geo))) {
        LOG_WARN("failed to transfrom to wgs84 srs", K(ret), K(strategy->proj4_proj_),
            K(strategy->proj4_wgs84_));
      } else {
        result = wgs84_geo;
        // do nothing
      }
    }
    return ret;
  }

};

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncBufferImpl, ObWkbGeomPoint, ObGeometry *)
{
  INIT_SUCC(ret);
  if (OB_SUCC(ob_geo_validate_buffer_input(g, context))) {
    const ObGeoBufferStrategy *strategy = context.get_val_arg(0)->strategy_;
    if (is_valid_for_cartpoint(*strategy)) {
      ret = eval_buffer_cartisan<ObCartesianPoint,
                                  ObIWkbGeomPoint>(g, context, *strategy, result, false);
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("validate strategy for cartisan point failed",
        K(ret), K(strategy->distance_val_), K(strategy->has_point_s_),
        K(strategy->has_join_s_), K(strategy->has_end_s_));
    }
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncBufferImpl, ObWkbGeomLineString, ObGeometry *)
{
  INIT_SUCC(ret);
  if (OB_SUCC(ob_geo_validate_buffer_input(g, context))) {
    const ObGeoBufferStrategy *strategy = context.get_val_arg(0)->strategy_;
    if (is_valid_for_linestring(*strategy)) {
      ret = eval_buffer_cartisan<ObCartesianLineString,
                                  ObIWkbGeomLineString>(g, context, *strategy, result, false);
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("validate strategy for cartisan linestring failed",
        K(ret), K(strategy->distance_val_), K(strategy->has_point_s_),
        K(strategy->has_join_s_), K(strategy->has_end_s_));
    }
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncBufferImpl, ObWkbGeomPolygon, ObGeometry *)
{
  INIT_SUCC(ret);
  if (OB_SUCC(ob_geo_validate_buffer_input(g, context))) {
    const ObGeoBufferStrategy *strategy = context.get_val_arg(0)->strategy_;
    if (is_valid_for_polygon(*strategy)) {
      ret = eval_buffer_cartisan<ObCartesianPolygon,
                                  ObIWkbGeomPolygon>(g, context, *strategy, result, false);
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("validate strategy for cartisan polygon failed",
        K(ret), K(strategy->distance_val_), K(strategy->has_point_s_),
        K(strategy->has_join_s_), K(strategy->has_end_s_));
    }
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncBufferImpl, ObWkbGeomMultiPoint, ObGeometry *)
{
  INIT_SUCC(ret);
  if (OB_SUCC(ob_geo_validate_buffer_input(g, context))) {
    const ObGeoBufferStrategy *strategy = context.get_val_arg(0)->strategy_;
    if (is_valid_for_cartpoint(*strategy)) {
      ret = eval_buffer_cartisan<ObCartesianMultipoint,
                                  ObIWkbGeomMultiPoint>(g, context, *strategy, result, false);
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("validate strategy for cartisan multipoint failed",
        K(ret), K(strategy->distance_val_), K(strategy->has_point_s_),
        K(strategy->has_join_s_), K(strategy->has_end_s_));
    }
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncBufferImpl, ObWkbGeomMultiLineString, ObGeometry *)
{
  INIT_SUCC(ret);
  if (OB_SUCC(ob_geo_validate_buffer_input(g, context))) {
    const ObGeoBufferStrategy *strategy = context.get_val_arg(0)->strategy_;
    if (is_valid_for_linestring(*strategy)) {
      ret = eval_buffer_cartisan<ObCartesianMultilinestring,
                                  ObIWkbGeomMultiLineString>(g, context, *strategy, result, false);
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("validate strategy for cartisan multilinestring failed",
        K(ret), K(strategy->distance_val_), K(strategy->has_point_s_),
        K(strategy->has_join_s_), K(strategy->has_end_s_));
    }
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncBufferImpl, ObWkbGeomMultiPolygon, ObGeometry *)
{
  INIT_SUCC(ret);
  if (OB_SUCC(ob_geo_validate_buffer_input(g, context))) {
    const ObGeoBufferStrategy *strategy = context.get_val_arg(0)->strategy_;
    if (is_valid_for_polygon(*strategy)) {
      ret = eval_buffer_cartisan<ObCartesianMultipolygon,
                                  ObIWkbGeomMultiPolygon>(g, context, *strategy, result, false);
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("validate strategy for cartisan multipolygon failed",
        K(ret), K(strategy->distance_val_), K(strategy->has_point_s_),
        K(strategy->has_join_s_), K(strategy->has_end_s_));
    }
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncBufferImpl, ObWkbGeomCollection, ObGeometry *)
{
  return eval_buffer_cartisan_collection (g, context, result, false);
} OB_GEO_FUNC_END;

// Notice: ObGeogPointCirleFix only used to fix buffer for geography point in boost 1.74
// Must be careful when updating boost deps
class ObGeogPointCirleFix final
{
public:
    explicit ObGeogPointCirleFix() : count_(0) {}
public:
    std::size_t count_;
    bg::srs::spheroid<double> spheroid_;
};

// geographical
// only point is supported by boost::geometry
// workround for other types: transform to cartisan first, eval cartisan buffer, then transform back
OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncBufferImpl, ObWkbGeogPoint, ObGeometry *)
{
  INIT_SUCC(ret);
  if (OB_SUCC(ob_geo_validate_buffer_input(g, context))) {
    const ObGeoBufferStrategy *strategy = context.get_val_arg(0)->strategy_;
    const ObSrsItem *srs = context.get_srs();
    if (OB_UNLIKELY(!is_valid_for_geogpoint(*strategy))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("validate strategy for geographic point failed",
        K(ret), K(strategy->distance_val_), K(strategy->has_point_s_),
        K(strategy->has_join_s_), K(strategy->has_end_s_));
    } else if (OB_ISNULL(srs)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid strategy for geographic point with null srs", K(ret));
    }else {
      common::ObIAllocator *allocator = context.get_allocator();
      // init buffer context for graphical point
      bg::strategy::buffer::distance_symmetric<double> distance_s(strategy->distance_val_);
      bg::strategy::buffer::side_straight side_s;
      bg::strategy::buffer::join_round join_round_s(strategy->join_round_val_);
      bg::strategy::buffer::end_round end_round_s(strategy->end_round_val_);

      // fix bg sphere parameter
      bg::strategy::buffer::geographic_point_circle<> point_circle_s(strategy->point_circle_val_);
      bg::srs::spheroid<double> geog_sphere(srs->semi_major_axis(), srs->semi_minor_axis());
      ObGeogPointCirleFix *point_circle_param_fixer = reinterpret_cast<ObGeogPointCirleFix *>(&point_circle_s);
      point_circle_param_fixer->spheroid_ = geog_sphere;

      ObGeoToTreeVisitor visitor(allocator);
      ObIWkbGeogPoint *i_geo = const_cast<ObIWkbGeogPoint *>(reinterpret_cast<const ObIWkbGeogPoint *>(g));
      ObGeographMultipolygon *geo_res = OB_NEWx(ObGeographMultipolygon , allocator, g->get_srid(), *allocator);
      if (OB_FAIL(i_geo->do_visit(visitor))) {
        LOG_WARN("failed to do geo to tree visit", K(ret));
      } else {
        ObGeographPoint *geo_tree = static_cast<ObGeographPoint *>(visitor.get_geometry());
        // returning is a multipolygon
        if (OB_ISNULL(geo_tree) || OB_ISNULL(geo_res)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(ret), KP(geo_tree), KP(geo_res));
        } else {
          // input should be ObWkbGeogInnerPoint
          bg::buffer(geo_tree->data(), *geo_res, distance_s, side_s, join_round_s, end_round_s, point_circle_s);
          if (geo_res->is_empty()) {
            //
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("error buffer result for geographic point", K(ret), KP(geo_tree), KP(geo_res),
              K(strategy->distance_val_), K(strategy->has_point_s_),
              K(strategy->has_join_s_), K(strategy->has_end_s_));
          } else {
            result = reinterpret_cast<ObGeometry *>(&((*geo_res)[0]));
            // normalize longititude range
            ObGeoLongtitudeCorrectVisitor longti_normalize_visitor(srs);
            if (OB_FAIL(result->do_visit(longti_normalize_visitor))) {
              LOG_WARN("failed to do longti normalize visit", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncBufferImpl, ObWkbGeogLineString, ObGeometry *)
{
  return eval_buffer_geographic<ObCartesianLineString,
                                ObIWkbGeomLineString>(g, context, result, false);
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncBufferImpl, ObWkbGeogPolygon, ObGeometry *)
{
  return eval_buffer_geographic<ObCartesianPolygon,
                                ObIWkbGeomPolygon>(g, context, result, false);
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncBufferImpl, ObWkbGeogMultiPoint, ObGeometry *)
{
  return eval_buffer_geographic<ObCartesianMultipoint,
                                ObIWkbGeomMultiPoint>(g, context, result, false);
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncBufferImpl, ObWkbGeogMultiLineString, ObGeometry *)
{
  return eval_buffer_geographic<ObCartesianMultilinestring,
                                ObIWkbGeomMultiLineString>(g, context, result, false);
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncBufferImpl, ObWkbGeogMultiPolygon, ObGeometry *)
{
  return eval_buffer_geographic<ObCartesianMultipolygon,
                                ObIWkbGeomMultiPolygon>(g, context, result, false);
} OB_GEO_FUNC_END;

OB_GEO_UNARY_FUNC_BEGIN(ObGeoFuncBufferImpl, ObWkbGeogCollection, ObGeometry *)
{
  return eval_buffer_geographic<ObCartesianLineString,
                                ObIWkbGeomLineString>(g, context, result, true);
} OB_GEO_FUNC_END;

template<typename GeometryType>
bool ObGeoFuncBufferImpl::apply_bg_equal(GeometryType &geo1,
                                         GeometryType &geo2,
                                         const ObSrsItem *srs)
{
  UNUSED(srs);
  return bg::equals(geo1, geo2);
}

bool ObGeoFuncBufferImpl::apply_bg_equal(ObGeographLineString &geo1,
                                         ObGeographLineString &geo2,
                                         const ObSrsItem *srs)
{
  // geographic line or polygon
  bg::srs::spheroid<double> geog_sphere(srs->semi_major_axis(), srs->semi_minor_axis());
  ObLlLaAaStrategy line_strategy(geog_sphere);
  return bg::equals(geo1, geo2, line_strategy);
}

bool ObGeoFuncBufferImpl::apply_bg_equal(ObGeographPolygon &geo1,
                                         ObGeographPolygon &geo2,
                                         const ObSrsItem *srs)
{
  // geographic line or polygon
  bg::srs::spheroid<double> geog_sphere(srs->semi_major_axis(), srs->semi_minor_axis());
  ObLlLaAaStrategy line_strategy(geog_sphere);
  return bg::equals(geo1, geo2, line_strategy);
}

template<typename MultiPointType, typename MultiLineType, typename MultiPolygonType>
int ObGeoFuncBufferImpl::apply_bg_remove_duplicate_geo(ObIAllocator &allocator,
                                                       const ObSrsItem *srs,
                                                       ObGeometry *&geo)
{
  INIT_SUCC(ret);
  switch (geo->type()) {
    case ObGeoType::POINT :
    case ObGeoType::LINESTRING :
    case ObGeoType::POLYGON :
      break;
    case ObGeoType::MULTIPOINT : {
      MultiPointType *mp = OB_NEWx(MultiPointType, (&allocator));
      if (OB_ISNULL(mp)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for multipoint", K(ret));
      } else {
        bool is_equal = false;
        MultiPointType *g = static_cast<MultiPointType *>(geo);
        FOREACH_X(item, (*g), OB_SUCC(ret)) {
          FOREACH(point, *mp) {
            is_equal = ObGeoFuncBufferImpl::apply_bg_equal(*item, *point, srs);
            if (is_equal) {
              break;
            }
          }
          if (!is_equal) {
            if(OB_FAIL(mp->push_back(*item))) {
              LOG_WARN("failed to add point to multipoint", K(ret));
            }
          }
        }
        geo = mp;
      }
      break;
    }
    case ObGeoType::MULTILINESTRING : {
      MultiLineType *ml = OB_NEWx(MultiLineType, (&allocator));
      if (OB_ISNULL(ml)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for multiline", K(ret));
      } else {
        bool is_equal = false;
        MultiLineType *g = static_cast<MultiLineType *>(geo);
        FOREACH_X(item, (*g), OB_SUCC(ret)) {
          FOREACH(line, *ml) {
            is_equal = ObGeoFuncBufferImpl::apply_bg_equal(*item, *line, srs);
            if (is_equal) {
              break;
            }
          }
          if (!is_equal) {
            if(OB_FAIL(ml->push_back(*item))) {
              LOG_WARN("failed to add line to multiline", K(ret));
            }
          }
        }
        geo = ml;
      }
      break;
    }
    case ObGeoType::MULTIPOLYGON : {
      MultiPolygonType *mpoly = OB_NEWx(MultiPolygonType, (&allocator));
      if (OB_ISNULL(mpoly)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for multipolygon", K(ret));
      } else {
        bool is_equal = false;
        MultiPolygonType *g = static_cast<MultiPolygonType *>(geo);
        FOREACH_X(item, *g, OB_SUCC(ret)) {
          FOREACH(poly, *mpoly) {
            is_equal = ObGeoFuncBufferImpl::apply_bg_equal(*item, *poly, srs);
            if (is_equal) {
              break;
            }
          }
          if (!is_equal) {
            if(OB_FAIL(mpoly->push_back(*item))) {
              LOG_WARN("failed to add polygon to multipolygon", K(ret));
            }
          }
        }
        geo = mpoly;
      }
      break;
    }
    default : {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid geo type", K(ret), K(geo->type()), K(geo->crs()));
      break;
    }
  }
  return ret;
}

int ObGeoFuncBufferImpl::remove_duplicate_geo(ObIAllocator &allocator,
                                              const ObSrsItem *srs,
                                              ObGeometry *&geo)
{
  INIT_SUCC(ret);
  if (geo == NULL) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("input geo is null", K(ret));
  } else if ((srs == NULL || srs->srs_type() != ObSrsType::GEOGRAPHIC_SRS)
      && geo->crs() == ObGeoCRS::Geographic) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid srs type", K(ret), KP(srs), K(geo->crs()));
  } else {
    switch (geo->crs()) {
      case ObGeoCRS::Geographic : {
        ret = apply_bg_remove_duplicate_geo<ObGeographMultipoint,
          ObGeographMultilinestring, ObGeographMultipolygon>(allocator, srs, geo);
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to remove duplicate geo", K(ret), K(geo->crs()));
        }
        break;
      }
      case ObGeoCRS::Cartesian : {
        ret = apply_bg_remove_duplicate_geo<ObCartesianMultipoint,
          ObCartesianMultilinestring, ObCartesianMultipolygon>(allocator, srs, geo);
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to remove duplicate geo", K(ret), K(geo->crs()));
        }
        break;
      }
      default : {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid geo type", K(ret), K(srs->srs_type()), K(geo->crs()));
      }
    }
  }
  return ret;
}

template <typename GeoTreeType>
int ObGeoFuncBufferImpl::unwrap_geo_tree_inner(ObGeometry *geo_in,
                                               ObGeometry *&geo_out)
{
  INIT_SUCC(ret);
  GeoTreeType *geo_temp = reinterpret_cast<GeoTreeType *>(geo_in);
  if (geo_temp->size() == 1) {
    geo_in = reinterpret_cast<ObGeometry *>(&((*geo_temp)[0]));
    ret = unwrap_geometry_tree(geo_in, geo_out);
  } else {
    geo_out = geo_in;
  }
  return ret;
}

int ObGeoFuncBufferImpl::unwrap_geometry_tree(ObGeometry *geo_in,
                                              ObGeometry *&geo_out)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(geo_in)) {
    ret = OB_ERR_NULL_VALUE;
  } else if (!geo_in->is_tree()) {
    ret = OB_NOT_IMPLEMENT;
  } else {
    switch (geo_in->crs()) {
      case ObGeoCRS::Cartesian : {
        switch (geo_in->type()) {
          case ObGeoType::MULTIPOINT : {
            ret = unwrap_geo_tree_inner<ObCartesianMultipoint>(geo_in, geo_out);
            break;
          }
          case ObGeoType::MULTILINESTRING : {
            ret = unwrap_geo_tree_inner<ObCartesianMultilinestring>(geo_in, geo_out);
            break;
          }
          case ObGeoType::MULTIPOLYGON : {
            ret = unwrap_geo_tree_inner<ObCartesianMultipolygon>(geo_in, geo_out);
            break;
          }
          case ObGeoType::GEOMETRYCOLLECTION : {
            ret = unwrap_geo_tree_inner<ObCartesianGeometrycollection>(geo_in, geo_out);
            break;
          }
          default : {
            geo_out = geo_in;
            break;
          }
        }
        break;
      }
      case ObGeoCRS::Geographic : {
        switch (geo_in->type()) {
          case ObGeoType::MULTIPOINT : {
            ret = unwrap_geo_tree_inner<ObGeographMultipoint>(geo_in, geo_out);
            break;
          }
          case ObGeoType::MULTILINESTRING : {
            ret = unwrap_geo_tree_inner<ObGeographMultilinestring>(geo_in, geo_out);
            break;
          }
          case ObGeoType::MULTIPOLYGON : {
            ret = unwrap_geo_tree_inner<ObGeographMultipolygon>(geo_in, geo_out);
            break;
          }
          case ObGeoType::GEOMETRYCOLLECTION : {
            ret = unwrap_geo_tree_inner<ObGeographGeometrycollection>(geo_in, geo_out);
            break;
          }
          default : {
            geo_out = geo_in;
            break;
          }
        }
        break;
      }
      default : {
        ret = OB_NOT_IMPLEMENT;
        LOG_WARN("unexpected crs type", K(ret), K(geo_in->crs()));
        break;
      }
    }
  }
  return ret;
}

// implement of outer class eval
// use an outer class to void implement templates in header files
int ObGeoFuncBuffer::eval(const ObGeoEvalCtx &gis_context, ObGeometry *&result)
{
  return ObGeoFuncBufferImpl::eval_geo_func(gis_context, result);
}

} // sql
} // oceanbase