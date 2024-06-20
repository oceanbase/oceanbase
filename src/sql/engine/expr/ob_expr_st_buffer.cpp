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
 * This file contains implementation for st_buffer and st_buffer_strategy.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_st_buffer.h"
#include "common/object/ob_obj_type.h"
#include "lib/geo/ob_geo_ibin.h"
#include "lib/geo/ob_geo_func_common.h"
#include "lib/geo/ob_geo_func_register.h"
#include "lib/geo/ob_geo_normalize_visitor.h"
#include "sql/engine/expr/ob_geo_expr_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
namespace oceanbase
{
namespace sql
{

const static int ST_BUFFER_STRATEG_ARG_START_IDX = 2;
const static uint64_t ST_BUFFER_STRATEGY_LEN = 12;
const static double ST_BUFFER_DISTANCE_MIN = 0.00000000001;
const static uint64_t OB_MAX_POINTS_IN_GEOMETRY = 65536; // not implemented as system variable currently
const static int ST_BUFFER_QUAR_PER_CIRCLE = 4;
const static int64_t ST_BUFFER_MAX_INPUT_NUM = 5;

// ObExprSTBufferStrategy
ObExprSTBufferStrategy::ObExprSTBufferStrategy(ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_SYS_ST_BUFFER_STRATEGY, N_ST_BUFFER_STRATEGY, ONE_OR_TWO, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObGeoBufferStrategyType ObExprSTBufferStrategy::get_strategy_type_by_name(const ObString &name)
{
  ObGeoBufferStrategyType strategy_type = ObGeoBufferStrategyType::INVALID;
  if (0 == name.case_compare("END_ROUND")) {
    strategy_type = ObGeoBufferStrategyType::END_ROUND;
  } else if (0 == name.case_compare("END_FLAT")) {
    strategy_type = ObGeoBufferStrategyType::END_FLAT;
  } else if (0 == name.case_compare("JOIN_ROUND")) {
    strategy_type = ObGeoBufferStrategyType::JOIN_ROUND;
  } else if (0 == name.case_compare("JOIN_MITER")) {
    strategy_type = ObGeoBufferStrategyType::JOIN_MITER;
  } else if (0 == name.case_compare("POINT_CIRCLE")) {
    strategy_type = ObGeoBufferStrategyType::POINT_CIRCLE;
  } else if (0 == name.case_compare("POINT_SQUARE")) {
    strategy_type = ObGeoBufferStrategyType::POINT_SQUARE;
  }
  return strategy_type;
}

int ObExprSTBufferStrategy::calc_result_typeN(ObExprResType &type,
                                              ObExprResType *types,
                                              int64_t param_num,
                                              common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  INIT_SUCC(ret);
  // 1st param must be string or null
  ObObjType first_type = types[0].get_type();
  if (ob_is_string_type(first_type) || ob_is_null(first_type)) {
    if (ob_is_string_type(first_type)) {
      types[0].set_calc_type(first_type);
      types[0].set_calc_collation_type(CS_TYPE_BINARY);
      if (param_num == 2 && !ob_is_null(types[1].get_type())) {
        types[1].set_calc_type(ObDoubleType);
      }
    }
    type.set_varchar();
    type.set_length(BUF_STRATEGY_RES_LENGTH);
    type.set_collation_level(CS_LEVEL_IMPLICIT);
    type.set_collation_type(CS_TYPE_BINARY);
  } else {
    ret = OB_NOT_IMPLEMENT;
    LOG_WARN("invalid first element type", K(ret), K(first_type));
  }
  return ret;
}

int ObExprSTBufferStrategy::eval_st_buffer_strategy(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  ObDatum *strategy_datum = NULL;
  ObString strategy_str;
  ObDatum *val_datum = NULL;
  ObGeoBufferStrategyType strategy = ObGeoBufferStrategyType::INVALID;
  double points_per_circle = 0.0;
  int num_args = expr.arg_cnt_;
  bool is_null_result = ob_is_null(expr.args_[0]->datum_meta_.type_);

  if (is_null_result) {
    // do nothing
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, strategy_datum))) {
    LOG_WARN("failed to eval first argument", K(ret));
  } else if (FALSE_IT(strategy_str = strategy_datum->get_string())) {
  } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(tmp_allocator, *strategy_datum,
             expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), strategy_str))) {
    LOG_WARN("fail to get real string data", K(ret), K(strategy_str));
  } else {
    strategy = get_strategy_type_by_name(strategy_str);
    if (ObGeoBufferStrategyType::INVALID == strategy) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid buffer strategy type", K(ret), K(strategy_str));
    } else if (ObGeoBufferStrategyType::POINT_SQUARE != strategy && ObGeoBufferStrategyType::END_FLAT != strategy) {
      if (num_args != 2) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument number", K(ret), K(num_args));
      } else if (ob_is_null(expr.args_[1]->datum_meta_.type_)) {
        is_null_result = true;
      } else if (OB_FAIL(expr.args_[1]->eval(ctx, val_datum))) {
        LOG_WARN("failed to eval second parameter of st_buffer_strategy", K(ret));
      } else {
        points_per_circle = val_datum->get_double();
        if (points_per_circle <= 0) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid points number per circle", K(ret), K(points_per_circle));
        } else if ((ObGeoBufferStrategyType::JOIN_MITER != strategy)
            && (points_per_circle > OB_MAX_POINTS_IN_GEOMETRY)) {
          ret = OB_ERR_GIS_MAX_POINTS_IN_GEOMETRY_OVERFLOWED;
          LOG_WARN("points number overflowed", K(ret), K(points_per_circle));
          LOG_USER_ERROR(OB_ERR_GIS_MAX_POINTS_IN_GEOMETRY_OVERFLOWED,
                         "points_per_circle",
                         OB_MAX_POINTS_IN_GEOMETRY,
                         N_ST_BUFFER_STRATEGY);
        }
      }
    } else if (num_args != 1) {
      ret = OB_INVALID_ARGUMENT;
    }
  }

  if (OB_SUCC(ret)) {
    if (is_null_result) {
      res.set_null();
    } else {
      char *res_buf = expr.get_str_res_mem(ctx, ST_BUFFER_STRATEGY_LEN);
      if (OB_ISNULL(res_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret));
      } else {
        ObGeoWkbByteOrderUtil::write<uint32_t>(res_buf, static_cast<uint32_t>(strategy));
        ObGeoWkbByteOrderUtil::write<double>(res_buf + sizeof(uint32_t), points_per_circle);
        res.set_string(res_buf, ST_BUFFER_STRATEGY_LEN);
      }
    }
  }

  if (ret == OB_INVALID_ARGUMENT) {
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, N_ST_BUFFER_STRATEGY);
  }
  return ret;
}

int ObExprSTBufferStrategy::cg_expr(ObExprCGCtx &expr_cg_ctx,
                                    const ObRawExpr &raw_expr,
                                    ObExpr &rt_expr) const
{
  UNUSEDx(expr_cg_ctx, raw_expr);
  rt_expr.eval_func_ = eval_st_buffer_strategy;
  return OB_SUCCESS;
}

// ObExprSTBuffer
ObExprSTBuffer::ObExprSTBuffer(ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_SYS_ST_BUFFER, N_ST_BUFFER, MORE_THAN_ONE, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprSTBuffer::ObExprSTBuffer(ObIAllocator &alloc,
                               ObExprOperatorType type,
                               const char *name,
                               int32_t param_num,
                               ObValidForGeneratedColFlag valid_for_generated_col,
                               int32_t dimension) : ObFuncExprOperator(alloc, type, name, param_num, valid_for_generated_col, dimension)
{
}

bool ObExprSTBuffer::is_valid_distance(double distance)
{
  return !isnan(distance) && !isinf(distance);
}

int ObExprSTBuffer::calc_result_typeN(ObExprResType &type,
                                      ObExprResType *types,
                                      int64_t param_num,
                                      common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  INIT_SUCC(ret);
  ObExprResType &geo_type = types[0];
  ObExprResType &distance_type = types[1];
  // null if anyone is null
  bool is_null_result = false;
  if (param_num > ST_BUFFER_MAX_INPUT_NUM) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, static_cast<int>(strlen(N_ST_BUFFER)), N_ST_BUFFER);
  } else {
    for (int i = 0; i < param_num && !is_null_result; i++) {
      is_null_result = ob_is_null(types[i].get_type());
    }
    if (!is_null_result) {
      if (!ob_is_geometry(geo_type.get_type()) && !ob_is_string_type(geo_type.get_type())) {
        geo_type.set_calc_type(ObVarcharType);
        geo_type.set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
        geo_type.set_calc_collation_level(CS_LEVEL_IMPLICIT);
      }
      if (OB_SUCC(ret)) {
        distance_type.set_calc_type(ObDoubleType);
        for (int i = 2; i < param_num && OB_SUCC(ret); i++) {
          if (ob_is_string_type(types[i].get_type())) {
            types[i].set_calc_type(types[i].get_type());
            types[i].set_calc_collation_type(CS_TYPE_BINARY);
          } else {
            ret = OB_INVALID_ARGUMENT;
            LOG_USER_ERROR(OB_INVALID_ARGUMENT, N_ST_BUFFER);
            LOG_WARN("invalid type for st_buffer", K(ret), K(i), K(types[i].get_type()));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      type.set_geometry();
      type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObGeometryType]).get_length());
    }
  }
  return ret;
}

int ObExprSTBuffer::init_buffer_strategy(const common::ObObj *params,
                                         int64_t param_num,
                                         common::ObExprCtx &expr_ctx,
                                         common::ObGeoBufferStrategy &buf_strat,
                                         double distance)
{
  INIT_SUCC(ret);
  // set strategy
  ObDatum *strat_datum = NULL;
  if (param_num == 2) {
    // do nothing, not strategy argument, use default value;
  } else {
    ObObjType strategy_data_type = params[ST_BUFFER_STRATEG_ARG_START_IDX].get_type();
    ObCollationType strategy_cs_type = params[ST_BUFFER_STRATEG_ARG_START_IDX].get_collation_type();
    bool is_priv_strategy = (ob_is_integer_type(strategy_data_type)
                          || (ob_is_string_type(strategy_data_type) && (strategy_cs_type != CS_TYPE_BINARY)));
    if (is_priv_strategy) {
      if (ob_is_integer_type(strategy_data_type)) {

        if (params[ST_BUFFER_STRATEG_ARG_START_IDX].get_int() <= 0) {
          // do nothing, not strategy argument, use default value;
        } else if (params[ST_BUFFER_STRATEG_ARG_START_IDX].get_int() > OB_MAX_POINTS_IN_GEOMETRY) {
          // can't more than 65536, it will stuck in bg if circle_points is too large
          ret = OB_ERR_GIS_MAX_POINTS_IN_GEOMETRY_OVERFLOWED;
          LOG_WARN("points number overflowed", K(ret), K(params[ST_BUFFER_STRATEG_ARG_START_IDX].get_int()));
          LOG_USER_ERROR(OB_ERR_GIS_MAX_POINTS_IN_GEOMETRY_OVERFLOWED,
                         "points_per_circle",
                         OB_MAX_POINTS_IN_GEOMETRY,
                         N_PRIV_ST_BUFFER);
        } else {
          // buf_strat is unsigned, strat_datum->get_int() can't be less than 0
          // one circle has 4 quads
          int circle_points = params[ST_BUFFER_STRATEG_ARG_START_IDX].get_int() * ST_BUFFER_QUAR_PER_CIRCLE;
          buf_strat.join_round_val_ = circle_points;
          buf_strat.end_round_val_ = circle_points;
          buf_strat.point_circle_val_ = circle_points;
        }
      } else {
        ObString pg_text_strategy = params[ST_BUFFER_STRATEG_ARG_START_IDX].get_string();
        ObString pg_strategy_clone;
        ObIAllocator *allocator = expr_ctx.calc_buf_;
        char *buf = static_cast<char*>(allocator->alloc(pg_text_strategy.length() + 1));
        if (OB_UNLIKELY(buf == NULL)){
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory for pg stype buffer strategy arg failed",
            K(pg_text_strategy.length()), K(ret));
        } else {
          *(buf + pg_text_strategy.length()) = '\0';
          MEMCPY(buf, pg_text_strategy.ptr(), pg_text_strategy.length());
          pg_strategy_clone.assign_buffer(buf, pg_text_strategy.length());
          if (OB_FAIL(parse_text_strategy(pg_strategy_clone, buf_strat))) {
            LOG_WARN("prase one stratety failed", K(ret));
            LOG_USER_ERROR(OB_INVALID_ARGUMENT, N_PRIV_ST_BUFFER);
          }
        }
      }
    } else {
      for (int i = ST_BUFFER_STRATEG_ARG_START_IDX; i < param_num && OB_SUCC(ret); i++) {
        if (OB_FAIL(parse_binary_strategy(params[i].get_string(), buf_strat))) {
          LOG_WARN("prase one stratety failed", K(ret));
        }
      }
    }
  }
  buf_strat.distance_val_ = distance;
  return ret;
}

int ObExprSTBuffer::init_buffer_strategy(const ObExpr &expr,
                                         ObEvalCtx &ctx,
                                         ObIAllocator &allocator,
                                         ObGeoBufferStrategy &buf_strat,
                                         double distance)
{
  INIT_SUCC(ret);
  // set strategy
  ObDatum *strat_datum = NULL;
  ObExpr *first_strategy_arg = NULL;
  int num_args = expr.arg_cnt_;
  if (num_args == 2) {
    // do nothing, not strategy argument, use default value;
  } else {
    ObExpr *first_strategy_arg = expr.args_[ST_BUFFER_STRATEG_ARG_START_IDX];
    ObObjType strategy_data_type = first_strategy_arg->datum_meta_.type_;
    ObCollationType strategy_cs_type = first_strategy_arg->datum_meta_.cs_type_;
    bool is_priv_strategy = (ob_is_integer_type(strategy_data_type)
                             || (ob_is_string_type(strategy_data_type)
                                 && (strategy_cs_type != CS_TYPE_BINARY)));
    if (is_priv_strategy) {
      if (OB_FAIL(first_strategy_arg->eval(ctx, strat_datum))) {
        LOG_WARN("eval pg style buffer strategy arg failed", K(ret));
      } else if (ob_is_integer_type(strategy_data_type)) {
        if (strat_datum->get_int() <= 0) {
          // do nothing, not strategy argument, use default value;
        } else if (strat_datum->get_int() > OB_MAX_POINTS_IN_GEOMETRY) {
          // can't more than 65536, it will stuck in bg if circle_points is too large
          ret = OB_ERR_GIS_MAX_POINTS_IN_GEOMETRY_OVERFLOWED;
          LOG_WARN("points number overflowed", K(ret), K(strat_datum->get_int()));
          LOG_USER_ERROR(OB_ERR_GIS_MAX_POINTS_IN_GEOMETRY_OVERFLOWED,
                         "points_per_circle",
                         OB_MAX_POINTS_IN_GEOMETRY,
                         N_PRIV_ST_BUFFER);
        } else {
          // one circle has 4 quads
          // buf_strat is unsigned, strat_datum->get_int() can't be less than 0
          int circle_points = strat_datum->get_int() * ST_BUFFER_QUAR_PER_CIRCLE;
          buf_strat.join_round_val_ = circle_points;
          buf_strat.end_round_val_ = circle_points;
          buf_strat.point_circle_val_ = circle_points;
        }

      } else {
        ObString pg_text_strategy = strat_datum->get_string();
        ObString pg_strategy_clone;
        if (OB_FAIL(ObTextStringHelper::read_real_string_data(allocator, *strat_datum,
                  first_strategy_arg->datum_meta_, first_strategy_arg->obj_meta_.has_lob_header(), pg_text_strategy))) {
          LOG_WARN("fail to get real string data", K(ret), K(pg_text_strategy));
        } else {
          char *buf = reinterpret_cast<char*>(allocator.alloc(pg_text_strategy.length() + 1));
          if (OB_UNLIKELY(buf == NULL)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("allocate memory for pg stype buffer strategy arg failed",
              K(pg_text_strategy.length()), K(ret));
          } else {
            *(buf + pg_text_strategy.length()) = '\0';
            MEMCPY(buf, pg_text_strategy.ptr(), pg_text_strategy.length());
            pg_strategy_clone.assign_buffer(buf, pg_text_strategy.length());
            if (OB_FAIL(parse_text_strategy(pg_strategy_clone, buf_strat))) {
              LOG_WARN("prase one stratety failed", K(ret));
              LOG_USER_ERROR(OB_INVALID_ARGUMENT, N_PRIV_ST_BUFFER);
            }
          }
        }
      }
    } else {
      for (int i = ST_BUFFER_STRATEG_ARG_START_IDX; i < num_args && OB_SUCC(ret); i++) {
        ObString strat_str;
        if (OB_FAIL(expr.args_[i]->eval(ctx, strat_datum))) {
          LOG_WARN("eval buffer strategy arg failed", K(ret));
        } else if (FALSE_IT(strat_str = strat_datum->get_string())) {
        } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(allocator, *strat_datum,
                  expr.args_[i]->datum_meta_, expr.args_[i]->obj_meta_.has_lob_header(), strat_str))) {
          LOG_WARN("fail to get real string data", K(ret), K(strat_str));
        } else if (OB_FAIL(parse_binary_strategy(strat_str, buf_strat))) {
          LOG_WARN("prase one stratety failed", K(ret));
        }
      }
    }
  }
  buf_strat.distance_val_ = distance;
  return ret;
}

int ObExprSTBuffer::fill_proj4_params(ObIAllocator &allocator,
                                      omt::ObSrsCacheGuard &srs_guard,
                                      uint32 srid,
                                      ObGeometry *geo,
                                      const ObSrsItem *srs,
                                      ObGeoBufferStrategy &buf_strat,
                                      bool &is_transform_method)
{
  INIT_SUCC(ret);
  // Notice: consist with mysql, use boost::geometry directly for geography point
  // use transfrom method for other geography types.
  if ((srs != NULL) && srs->is_geographical_srs()) {
    ObGeoEvalCtx box_context(&allocator, srs);
    ObGeogBox *geogbox = NULL;
    int32_t bestsrid;

    if (OB_FAIL(srs_guard.get_srs_item(srid, srs))) {
      LOG_WARN("failed to get self srs item", K(ret), K(srid));
    } else if (OB_FAIL(srs->get_proj4_param(&allocator, buf_strat.proj4_self_))) {
      LOG_WARN("failed to get proj4 prams from self srs", K(ret), K(srid));
    } else if (OB_FAIL(srs_guard.get_srs_item(OB_GEO_DEFAULT_GEOGRAPHY_SRID, buf_strat.srs_wgs84_))) {
      LOG_WARN("failed to get wgs84 srs item", K(ret), K(OB_GEO_DEFAULT_GEOGRAPHY_SRID));
    } else if (OB_FAIL(buf_strat.srs_wgs84_->get_proj4_param(&allocator, buf_strat.proj4_wgs84_))) {
      LOG_WARN("failed to get proj4 prams from wgs84 srs", K(ret), K(srid));
    } else if (geo->type() == ObGeoType::POINT) {
      // point dont transform to proj
      is_transform_method = true;
    } else if (OB_FAIL(box_context.append_geo_arg(geo))) {
      LOG_WARN("build box context failed", K(ret), K(box_context.get_geo_count()));
    } else if (OB_FAIL(ObGeoFunc<ObGeoFuncType::Box>::geo_func::eval(box_context, geogbox))) {
      LOG_WARN("failed to do box functor failed", K(ret));
    } else if (OB_FAIL(ObGeoExprUtils::get_box_bestsrid(geogbox, NULL, bestsrid))) {
      LOG_WARN("failed to get box bestsrid", K(ret), KP(geogbox));
    } else if (OB_FAIL(srs_guard.get_srs_item(bestsrid, buf_strat.srs_proj_))) {
      LOG_WARN("failed to get proj srs item", K(ret), K(bestsrid));
    } else if (OB_FAIL(buf_strat.srs_proj_->get_proj4_param(&allocator, buf_strat.proj4_proj_))) {
      LOG_WARN("failed to get proj4 prams from proj srs", K(ret), K(srid));
    } else {
      is_transform_method = true;
    }
  }
  return ret;
}

int ObExprSTBuffer::eval_st_buffer(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
  ObDatum *geo_datum = NULL;
  ObDatum *dist_datum = NULL;
  int num_args = expr.arg_cnt_;
  ObGeometry *geo = NULL;
  double distance = 0.0;
  ObGeoBufferStrategy buf_strat;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
  bool is_null_result = false;
  bool is_empty = false;
  uint32_t srid = 0;
  const ObSrsItem *srs = NULL;
  omt::ObSrsCacheGuard srs_guard;
  ObString proj4_param;

  // check null
  for (int i = 0; i < num_args && !is_null_result; i++) {
    is_null_result = (expr.args_[i]->datum_meta_.type_ == ObNullType);
  }
  if (OB_FAIL(expr.args_[0]->eval(ctx, geo_datum))) {
    LOG_WARN("eval geo arg failed", K(ret));
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, dist_datum))) {
    LOG_WARN("eval distance arg failed", K(ret));
  } else if (geo_datum->is_null() || dist_datum->is_null() || is_null_result) {
    is_null_result = true;
    res.set_null();
  } else {
    distance = dist_datum->get_double();
  }

  ObString geo_str = geo_datum->get_string();
  if (!is_null_result && OB_SUCC(ret)) {
    if (!is_valid_distance(distance)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, N_ST_BUFFER);
      LOG_WARN("nan distance argument", K(ret), K(distance));
    } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *geo_datum,
              expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), geo_str))) {
      LOG_WARN("fail to get real string data", K(ret), K(geo_str));
    } else if (std::abs(distance) < ST_BUFFER_DISTANCE_MIN
               && geo_str.length() < WKB_DATA_OFFSET + WKB_GEO_TYPE_SIZE) {
      // Consist with mysql, return original invalid wkb if distance is too small.
      // However pg will return fixed geometry.
      if (OB_FAIL(ObGeoExprUtils::pack_geo_res(expr, ctx, res, geo_str))) {
        LOG_WARN("fail to pack geo res", K(ret));
      }
    } else if (OB_FAIL(ObGeoExprUtils::get_srs_item(ctx, srs_guard, geo_str, srs,
        true, N_ST_BUFFER))) {
      LOG_WARN("fail to get srs item", K(ret));
    } else if (std::abs(distance) < ST_BUFFER_DISTANCE_MIN) {
      // Consist with mysql, return wkb(add version) if distance is too small.
      // However pg will return fixed geometry.
      ObString res_wkb;
      if (OB_FAIL(ObGeoExprUtils::build_geometry(temp_allocator, geo_str,
                                geo, srs, N_ST_BUFFER, ObGeoBuildFlag::GEO_ALLOW_3D))) {
        LOG_WARN("parse wkb failed", K(ret), K(geo_str));
      } else if (OB_FAIL(ObGeoExprUtils::geo_to_wkb(*geo, expr, ctx, srs, res_wkb))) {
        LOG_WARN("failed to write geometry to wkb", K(ret));
      } else {
        res.set_string(res_wkb);
      }
    } else if (OB_FAIL(ObGeoExprUtils::build_geometry(temp_allocator, geo_str,
        geo, srs, N_ST_BUFFER, ObGeoBuildFlag::GEO_ALLOW_3D_DEFAULT))) {
      LOG_WARN("parse wkb failed", K(ret), K(geo_str));
    } else if (OB_FAIL(ObGeoTypeUtil::get_srid_from_wkb(geo_str, srid))) {
      LOG_WARN("get type and srid from wkb failed", K(ret));
    } else if ((srid != 0) && OB_FAIL(srs->get_proj4_param(&temp_allocator, proj4_param))) {
      LOG_WARN("fail to get proj4 param", K(ret));
    } else if (OB_NOT_NULL(srs) && srs->is_geographical_srs() && geo->type() != ObGeoType::POINT) {
      ret = OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS;
      LOG_USER_ERROR(OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS, N_ST_BUFFER, ObGeoTypeUtil::get_geo_name_by_type(geo->type()));
      LOG_WARN("invalid type for geographic srs", K(ret), K(geo->type()));
    } else if (OB_FAIL(ObGeoExprUtils::check_empty(geo, is_empty))) {
      LOG_WARN("check input empty failed", K(ret));
    } else if (is_empty) {
      ObString res_wkb;
      ObGeometry *empty_res_geo = OB_NEWx(ObCartesianGeometrycollection, (&temp_allocator), srid, temp_allocator);
      if (OB_ISNULL(empty_res_geo)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for empty geographic result", K(ret), KP(empty_res_geo));
      } else if (OB_FAIL(ObGeoExprUtils::geo_to_wkb(*empty_res_geo, expr, ctx, srs, res_wkb, srid))) {
        LOG_WARN("failed to write empty geometry to wkb", K(ret));
      } else {
        res.set_string(res_wkb);
      }
    } else {
      if (distance < 0
          && geo->type() != ObGeoType::GEOMETRYCOLLECTION
          && geo->type() != ObGeoType::POLYGON
          && geo->type() != ObGeoType::MULTIPOLYGON) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, N_ST_BUFFER);
        LOG_WARN("wrong distance argument", K(ret), K(distance), K(geo->type()));
      } else if (OB_FAIL(init_buffer_strategy(expr, ctx, temp_allocator, buf_strat, distance))) {
        LOG_WARN("failed to build st_buffer strategy", K(ret));
      } else {
        ObGeoEvalCtx gis_context(&temp_allocator, srs);
        ObGeometry *res_geo = NULL;
        bool need_normalize = OB_NOT_NULL(srs) && srs->is_geographical_srs();
        if (OB_FAIL(gis_context.append_geo_arg(geo))) {
          LOG_WARN("failed to append geo arg to gis context", K(ret), K(gis_context.get_geo_count()));
        } else if (OB_FAIL(gis_context.append_val_arg(&buf_strat))) {
          LOG_WARN("failed to append buffer strategy to gis context", K(ret), K(gis_context.get_geo_count()));
        } else if (OB_FAIL(ObGeoFunc<ObGeoFuncType::Buffer>::geo_func::eval(gis_context, res_geo))) {
          LOG_WARN("eval st_buffer failed", K(ret));
          ObGeoExprUtils::geo_func_error_handle(ret, N_ST_BUFFER);
        } else if (OB_ISNULL(res_geo)) {
          ret = OB_ERR_NULL_VALUE;
          LOG_WARN("eval st_buffer null result", K(ret));
        } else if (need_normalize && OB_FAIL(ObGeoExprUtils::denormalize_wkb(proj4_param, res_geo))) {
          LOG_WARN("failed to do denormalize wkb", K(ret), K(proj4_param));
        } else {
          ObString res_wkb;
          // Notice: all geography result in pg use srid 4326
          if (OB_FAIL(ObGeoExprUtils::geo_to_wkb(*res_geo, expr, ctx, srs, res_wkb))){
            LOG_WARN("failed to write geometry to wkb", K(ret));
          } else {
            res.set_string(res_wkb);
          }
        }
      }
    }
  }

  return ret;
}

int ObExprSTBuffer::cg_expr(ObExprCGCtx &expr_cg_ctx,
                            const ObRawExpr &raw_expr,
                            ObExpr &rt_expr) const
{
  UNUSEDx(expr_cg_ctx, raw_expr);
  rt_expr.eval_func_ = eval_st_buffer;
  return OB_SUCCESS;
}

// [strategy_type][value]
int ObExprSTBuffer::parse_binary_strategy(const ObString &str, ObGeoBufferStrategy &strategy)
{
  INIT_SUCC(ret);
  if (str.length() < ST_BUFFER_STRATEGY_LEN) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the length of buffer strategy need greater than 12", K(ret), K(str));
  } else {
    uint32_t type_val =  ObGeoWkbByteOrderUtil::read<uint32_t>(str.ptr());
    ObGeoBufferStrategyType s_type = static_cast<ObGeoBufferStrategyType>(type_val);
    double val = ObGeoWkbByteOrderUtil::read<double>(str.ptr() + sizeof(uint32_t));
    switch (s_type) {
      case ObGeoBufferStrategyType::JOIN_ROUND: {
        if (strategy.has_join_s_) {
          ret = OB_INVALID_ARGUMENT;
        } else {
          strategy.join_round_val_ = val;
          strategy.has_join_s_ = true;
        }
        break;
      }
      case ObGeoBufferStrategyType::JOIN_MITER: {
        if (strategy.has_join_s_) {
          ret = OB_INVALID_ARGUMENT;
        } else {
          strategy.join_miter_val_ = val;
          strategy.has_join_s_ = true;
          strategy.state_num_ |= JOIN_MITER_MASK;
        }
        break;
      }
      case ObGeoBufferStrategyType::END_ROUND: {
        if (strategy.has_end_s_) {
          ret = OB_INVALID_ARGUMENT;
        } else {
          strategy.end_round_val_ = val;
          strategy.has_end_s_ = true;
        }
        break;
      }
      case ObGeoBufferStrategyType::END_FLAT: {
        if (strategy.has_end_s_) {
          ret = OB_INVALID_ARGUMENT;
        } else {
          strategy.has_end_s_ = true;
          strategy.state_num_ |= END_FLAT_MASK;
        }
        break;
      }
      case ObGeoBufferStrategyType::POINT_CIRCLE: {
        if (strategy.has_point_s_) {
          ret = OB_INVALID_ARGUMENT;
        } else {
          strategy.point_circle_val_ = val;
          strategy.has_point_s_ = true;
        }
        break;
      }
      case ObGeoBufferStrategyType::POINT_SQUARE: {
        if (strategy.has_point_s_) {
          ret = OB_INVALID_ARGUMENT;
        } else {
          strategy.has_point_s_ = true;
          strategy.state_num_ |= POINT_SQUARE_MASK;
        }
        break;
      }
      default: {
        ret = OB_INVALID_ARGUMENT;
        break;
      }
    }
    if (OB_FAIL(ret)) {
      if (ret == OB_INVALID_ARGUMENT) {
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, N_ST_BUFFER);
      }
      LOG_WARN("unsupported buffer strategy combination", K(ret), K(s_type), K(strategy.has_join_s_),
        K(strategy.has_end_s_), K(strategy.has_point_s_));
    }

  }
  return ret;
}

/*
pg strategies:
1. 'quad_segs=#' : number of line segments used to approximate a quarter circle (default is 8).
2. 'endcap=round|flat|square' : endcap style (defaults to "round"). 'butt' is accepted as a synonym for 'flat'.
3. 'join=round|mitre|bevel' : join style (defaults to "round"). 'miter' is accepted as a synonym for 'mitre'.
4. 'mitre_limit=#.#' : mitre ratio limit (only affects mitered join style). 'miter_limit' is accepted as a synonym for 'mitre_limit'.
5. 'side=both|left|right' : 'left' or 'right' performs a single-sided buffer on the geometry. default both .

compare to boost::geometry
1. quad_segs replacement of bg point_cirle_val, join_round_val and end_round_val
2. endcap=square, join=bevel is not supported by bg
3. pg allow all kinds of compared

*/
int ObExprSTBuffer::parse_text_strategy(ObString &str, ObGeoBufferStrategy &strategy)
{
  INIT_SUCC(ret);
  char *param = str.ptr();
  char *saver = NULL;
  param = strtok_r(param, " ", &saver);
  int singleside = 0; // not used by mysql

  for (; OB_NOT_NULL(param) && OB_SUCC(ret); ) {
    char *key = NULL;
    char *val = NULL;

    key = param;
    val = strchr(key, '=');
    if (OB_ISNULL(val) || *(val + 1) == '\0') {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalided buffer strategy", K(ret), K(key));
    } else {
      *val = '\0';
      ++val;
      LOG_DEBUG("invalided buffer strategy", K(key), K(val));
      if (!strcmp(key, "endcap")) {
        if ( !strcmp(val, "round") ) {
          strategy.state_num_ &= ~END_FLAT_MASK;
        } else if (!strcmp(val, "flat") || !strcmp(val, "butt")) {
          strategy.state_num_ |= END_FLAT_MASK;
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("not supported endcap strategy", K(ret), K(val));
        }
      } else if (!strcmp(key, "join")) {
        if (!strcmp(val, "round")) {
          strategy.state_num_ &= ~JOIN_MITER_MASK;
        } else if (!strcmp(val, "mitre") || !strcmp(val, "miter")) {
          strategy.state_num_ |= JOIN_MITER_MASK;
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("not supported join strategy", K(ret), K(val));
        }
      } else if (!strcmp(key, "mitre_limit") || !strcmp(key, "miter_limit")) {
        strategy.join_miter_val_ = atof(val);
      } else if (!strcmp(key, "quad_segs")) {
        int circle_points = atoi(val) * ST_BUFFER_QUAR_PER_CIRCLE;
        if (circle_points > 0) {
          strategy.join_round_val_ = circle_points;
          strategy.end_round_val_ = circle_points;
          strategy.point_circle_val_ = circle_points;
        } else {
          // do nothing, use default val
        }
      } else if (!strcmp(key, "side")) {
        if (!strcmp(val, "both")) {
          singleside = 0;
        } else if (!strcmp(val, "left")) {
          singleside = 1;
        } else if (!strcmp(val, "right")) {
          singleside = -1;
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("not supported side strategy", K(ret), K(val));
        }
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("not supported strategy key", K(ret), K(key));
      }
      param = strtok_r(NULL, " ", &saver);
    }
  }
  LOG_DEBUG("invalided buffer strategy", K(strategy.state_num_));
  return ret;
}

// ObExprPrivSTBuffer
ObExprPrivSTBuffer::ObExprPrivSTBuffer(ObIAllocator &alloc)
  : ObExprSTBuffer(alloc, T_FUN_SYS_PRIV_ST_BUFFER, N_PRIV_ST_BUFFER, TWO_OR_THREE, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

int ObExprPrivSTBuffer::calc_result_typeN(ObExprResType &type,
                                          ObExprResType *types,
                                          int64_t param_num,
                                          common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  INIT_SUCC(ret);
  ObExprResType &geo_type = types[0];
  ObExprResType &distance_type = types[1];
  // null if anyone is null
  bool is_null_result = false;
  for (int i = 0; i < param_num && !is_null_result; i++) {
    is_null_result = ob_is_null(types[i].get_type());
  }
  if (!is_null_result) {
    if (!ob_is_geometry(geo_type.get_type()) && !ob_is_string_type(geo_type.get_type())) {
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, get_name());
    }
    if (OB_SUCC(ret)) {
      if (!ob_is_numeric_type(distance_type.get_type())) {
        ret = OB_ERR_GIS_INVALID_DATA;
        LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, get_name());
      } else {
        distance_type.set_calc_type(ObDoubleType);
        if (param_num == 3) {
          // check pg style strategy string;
          if (!ob_is_string_type(types[2].get_type()) && !ob_is_integer_type(types[2].get_type())) {
            ret = OB_INVALID_ARGUMENT;
            LOG_USER_ERROR(OB_INVALID_ARGUMENT, N_PRIV_ST_BUFFER);
            LOG_WARN("invalid type for st_buffer", K(ret), K(types[2].get_type()));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      type.set_geometry();
      type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObGeometryType]).get_length());
    }
  } else {
    type.set_null();
    type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObGeometryType]).get_length());
  }
  return ret;
}

int ObExprPrivSTBuffer::eval_priv_st_buffer(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
  ObDatum *geo_datum = NULL;
  ObString geo_str;
  ObDatum *dist_datum = NULL;
  int num_args = expr.arg_cnt_;
  ObGeometry *geo = NULL;
  double distance = 0.0;
  ObGeoBufferStrategy buf_strat;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
  bool is_null_result = false;
  bool is_transform_method = false;
  bool is_empty = false;
  uint32_t srid = 0;
  const ObSrsItem *srs = NULL;
  omt::ObSrsCacheGuard srs_guard;

  // check null
  for (int i = 0; i < num_args && !is_null_result; i++) {
    is_null_result = (expr.args_[i]->datum_meta_.type_ == ObNullType);
  }
  if (OB_FAIL(expr.args_[0]->eval(ctx, geo_datum))) {
    LOG_WARN("eval geo arg failed", K(ret));
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, dist_datum))) {
    LOG_WARN("eval distance arg failed", K(ret));
  } else if (geo_datum->is_null() || geo_datum->is_null() || is_null_result) {
    res.set_null();
  } else if (FALSE_IT(geo_str = geo_datum->get_string())) {
  } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *geo_datum,
            expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), geo_str))) {
    LOG_WARN("fail to get real string data", K(ret), K(geo_str));
  } else if (OB_FAIL(ObGeoExprUtils::get_srs_item(ctx, srs_guard, geo_str, srs, true))) {
    LOG_WARN("fail to get srs item", K(ret));
  } else if (OB_FAIL(ObGeoExprUtils::build_geometry(temp_allocator, geo_str, geo, srs, N_PRIV_ST_BUFFER,
                    ObGeoBuildFlag::GEO_CHECK_RING | ObGeoBuildFlag::GEO_CORRECT | ObGeoBuildFlag::GEO_ALLOW_3D | ObGeoBuildFlag::GEO_CHECK_RANGE))) {
    LOG_WARN("parse wkb failed", K(ret), K(geo_str));
  } else if (OB_FAIL(ObGeoTypeUtil::get_srid_from_wkb(geo_str, srid))) {
    LOG_WARN("get type and srid from wkb failed", K(ret));
  } else if (OB_FAIL(ObGeoExprUtils::check_empty(geo, is_empty))) {
    LOG_WARN("check input empty failed", K(ret));
  } else if (is_empty) {
    ObGeometry *empty_res_geo = NULL;
    ObString res_wkb;
    if ((srs != NULL) && srs->is_geographical_srs()) {
      srid = OB_GEO_DEFAULT_GEOGRAPHY_SRID;
      empty_res_geo = OB_NEWx(ObGeographGeometrycollection, (&temp_allocator), srid, temp_allocator);
    } else {
      empty_res_geo = OB_NEWx(ObCartesianGeometrycollection, (&temp_allocator), srid, temp_allocator);
    }
    if (OB_ISNULL(empty_res_geo)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for empty geographic result", K(ret), KP(empty_res_geo));
    } else if (OB_FAIL(ObGeoExprUtils::geo_to_wkb(*empty_res_geo, expr, ctx, srs, res_wkb, srid))) {
      LOG_WARN("failed to write empty geometry to wkb", K(ret));
    } else {
      res.set_string(res_wkb);
    }
  } else {
    distance = dist_datum->get_double();
    if (!is_valid_distance(distance)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, N_PRIV_ST_BUFFER);
      LOG_WARN("nan distance argument", K(ret), K(distance));
    } else if (distance < 0
        && geo->type() != ObGeoType::GEOMETRYCOLLECTION
        && geo->type() != ObGeoType::POLYGON
        && geo->type() != ObGeoType::MULTIPOLYGON) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, N_ST_BUFFER);
      LOG_WARN("wrong distance argument", K(ret), K(distance), K(geo->type()));
    } else if (std::abs(distance) < ST_BUFFER_DISTANCE_MIN) {
      // Consist with mysql, return original wkb if distance is too small.
      // However pg will return fixed geometry.
      ObString res_wkb;
      if (OB_FAIL(ObGeoExprUtils::geo_to_wkb(*geo, expr, ctx, srs, res_wkb))){
        LOG_WARN("failed to write geometry to wkb", K(ret));
      } else {
        res.set_string(res_wkb);
      }
    } else if (OB_FAIL(init_buffer_strategy(expr, ctx, temp_allocator, buf_strat, distance))) {
      LOG_WARN("failed to build st_buffer strategy", K(ret));
    } else if (OB_FAIL(fill_proj4_params(temp_allocator,
                                         srs_guard,
                                         srid,
                                         geo,
                                         srs,
                                         buf_strat,
                                         is_transform_method))){
      LOG_WARN("failed to fill proj4 params for st_buffer strategy", K(ret));
    } else {
      ObGeoEvalCtx gis_context(&temp_allocator, srs);
      ObGeometry *res_geo = NULL;
      ObGeometry *wgs84_geo = NULL;
      if(OB_NOT_NULL(srs) && srs->is_geographical_srs()) {
        ObGeoNormalizeVisitor normalize_visitor(srs);
        if (OB_FAIL(geo->do_visit(normalize_visitor))) {
          LOG_WARN("normalize geo failed", K(ret));
        } else if (OB_FAIL(ObGeoTypeUtil::correct_polygon(temp_allocator, srs, false, *geo))) {
          LOG_WARN("correct geo failed", K(ret), K(geo));
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(gis_context.append_geo_arg(geo))) {
          LOG_WARN("failed to append geo arg to gis context", K(ret), K(gis_context.get_geo_count()));
        } else if (OB_FAIL(gis_context.append_val_arg(&buf_strat))) {
          LOG_WARN("failed to append buffer strategy to gis context", K(ret), K(gis_context.get_geo_count()));
        } else if (OB_FAIL(ObGeoFunc<ObGeoFuncType::Buffer>::geo_func::eval(gis_context, res_geo))) {
          LOG_WARN("eval st_buffer failed", K(ret));
          ObGeoExprUtils::geo_func_error_handle(ret, N_PRIV_ST_BUFFER);
        } else if (OB_ISNULL(res_geo)) {
          ret = OB_ERR_NULL_VALUE;
          LOG_WARN("eval st_buffer null result", K(ret));
        } else if (OB_NOT_NULL(srs) && srs->is_geographical_srs() && geo->type() == ObGeoType::POINT) {
          ObString buffered_res_wkb;
          ObGeometry *res_bin = NULL;
          ObGeoEvalCtx transform_context(&temp_allocator, buf_strat.srs_wgs84_);
          if (srs->get_srid() == buf_strat.srs_wgs84_->get_srid()) {
            // do nothing
          } else if (OB_FAIL(ObGeoTypeUtil::to_wkb(temp_allocator, *res_geo, srs, buffered_res_wkb))) {
            LOG_WARN("fail to to_wkb for res_geo buffer", K(ret));
          } else if (OB_FAIL(ObGeoExprUtils::build_geometry(temp_allocator, buffered_res_wkb, res_bin, srs, N_PRIV_ST_BUFFER))) {
            LOG_WARN("fail to create geo bin for point buffer", K(ret));
          } else if (OB_FAIL(transform_context.append_geo_arg(res_bin))) {
            LOG_WARN("failed to append geo arg to gis context", K(ret), K(transform_context.get_geo_count()));
          } else if (OB_FAIL(transform_context.append_val_arg(&buf_strat.proj4_self_))) {
            LOG_WARN("failed to append src_proj4_param to gis context", K(ret), K(transform_context.get_geo_count()));
          } else if (OB_FAIL(transform_context.append_val_arg(&buf_strat.proj4_wgs84_))) {
            LOG_WARN("failed to append dest_proj4_param to gis context", K(ret), K(transform_context.get_geo_count()));
          } else if (OB_FAIL(ObGeoFuncTransform::eval(transform_context, wgs84_geo))) {
            LOG_WARN("eval boost transform failed", K(ret), K(buf_strat.proj4_self_), K(buf_strat.proj4_wgs84_));
            ObGeoExprUtils::geo_func_error_handle(ret, N_PRIV_ST_BUFFER);
          } else {
            res_geo = wgs84_geo;
          }
        }

        if (OB_SUCC(ret)) {
          ObString res_wkb;
          // Notice: all geography result in pg use srid 4326
          if (!is_transform_method
              && OB_FAIL(ObGeoExprUtils::geo_to_wkb(*res_geo, expr, ctx, srs, res_wkb))){
            LOG_WARN("failed to write geometry to wkb", K(ret));
          } else if (is_transform_method
              && OB_FAIL(ObGeoExprUtils::geo_to_wkb(*res_geo, expr, ctx, buf_strat.srs_wgs84_, res_wkb))) {
            LOG_WARN("failed to write geography to wkb", K(ret));
          } else {
            res.set_string(res_wkb);
          }
        }
      }
    }
  }
  return ret;
}

int ObExprPrivSTBuffer::cg_expr(ObExprCGCtx &expr_cg_ctx,
                                const ObRawExpr &raw_expr,
                                ObExpr &rt_expr) const
{
  UNUSEDx(expr_cg_ctx, raw_expr);
  rt_expr.eval_func_ = eval_priv_st_buffer;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase