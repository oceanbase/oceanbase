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
 * This file contains implementation for st_aswkb/_st_asewkb/st_asbinary expr.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_st_aswkb.h"
#include "lib/geo/ob_geo_func_common.h"
#include "lib/geo/ob_geo_reverse_coordinate_visitor.h"
#include "lib/geo/ob_geo_utils.h"
#include "sql/engine/expr/ob_geo_expr_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprGeomWkb::ObExprGeomWkb(common::ObIAllocator &alloc,
                             ObExprOperatorType type,
                             const char *name,
                             int32_t param_num,
                             int32_t dimension)
    : ObFuncExprOperator(alloc,
                         type,
                         name,
                         param_num,
                         VALID_FOR_GENERATED_COL,
                         dimension)
{
}

ObExprGeomWkb::~ObExprGeomWkb()
{
}

bool ObExprGeomWkb::is_blank_string(const ObCollationType coll_type,
                                    const ObString &str) const
{
  bool is_blank_string = true;
  const char *c = str.ptr();

  for (int64_t i = 0; i < str.length() && is_blank_string; i++) {
    if (!ObCharset::is_space(coll_type, *c)) {
      is_blank_string = false;
    } else {
      c++;
    }
  }

  return is_blank_string;
}

int ObExprGeomWkb::calc_result_typeN(ObExprResType& type,
                                     ObExprResType* types_stack,
                                     int64_t param_num,
                                     ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  type.set_type(ObLongTextType);
  type.set_collation_level(common::CS_LEVEL_IMPLICIT);
  type.set_collation_type(CS_TYPE_BINARY);
  type.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY[ObLongTextType]);

  if (1 == param_num) {
    ObObjType data_type = types_stack[0].get_type();
    if (ob_is_geometry(data_type) || ob_is_null(data_type)) {
      // do nothing
    } else {
      types_stack[0].set_calc_type(ObLongTextType);
      types_stack[0].set_calc_collation_type(CS_TYPE_BINARY);
      types_stack[0].set_calc_collation_level(CS_LEVEL_IMPLICIT);
    }
  }

  if (2 == param_num) {
    ObObjType option_type = types_stack[1].get_type();
    if (ob_is_string_type(option_type) || ob_is_null(option_type)) {
      // do nothing
    } else {
      types_stack[1].set_calc_type(ObVarcharType);
      types_stack[1].set_calc_collation_type(types_stack[1].get_collation_type());
      types_stack[1].set_calc_collation_level(types_stack[1].get_collation_level());
    }
  }

  return ret;
}

int ObExprGeomWkb::eval_geom_wkb(const ObExpr &expr,
                                 ObEvalCtx &ctx,
                                 ObDatum &res)
{
	int ret = OB_SUCCESS;
  uint32_t arg_num = expr.arg_cnt_;
  bool is_null_result = false;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  omt::ObSrsCacheGuard srs_guard;
  ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  const ObSrsItem *srs = NULL;
  bool is_geog = false;
  bool is_lat_long_order = false;
  bool need_reverse = false;
  bool srid_default_ordering = true;
  ObDatum *wkb_datum = NULL;
  ObString wkb;
  ObGeometry *geo = NULL;
  ObGeoAxisOrder axis_order = ObGeoAxisOrder::INVALID;

  if (OB_FAIL(expr.args_[0]->eval(ctx, wkb_datum))) {
    LOG_WARN("fail to eval wkb datum", K(ret));
  } else if (wkb_datum->is_null()) {
    is_null_result = true;
  } else if (FALSE_IT(wkb = wkb_datum->get_string())) {
  } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(tmp_allocator, *wkb_datum,
             expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), wkb))) {
    LOG_WARN("fail to get real string data", K(ret), K(wkb));
  } else if (OB_FAIL(ObGeoExprUtils::construct_geometry(tmp_allocator,
      wkb, srs_guard, srs, geo, get_func_name()))) {
    LOG_WARN("fail to create geo bin", K(ret), K(wkb));
  } else if (OB_NOT_NULL(srs)) {
    is_geog = srs->is_geographical_srs();
    is_lat_long_order = srs->is_lat_long_order();
    need_reverse = is_geog && is_lat_long_order;
  }

  if (OB_SUCC(ret) && !is_null_result && arg_num == 2) {
    ObDatum *option_datum = NULL;
    ObString option_str;
    if (OB_FAIL(expr.args_[1]->eval(ctx, option_datum))) {
      LOG_WARN("fail to eval option datum", K(ret));
    } else if (option_datum->is_null()){
      is_null_result = true;
    } else if (FALSE_IT(option_str = option_datum->get_string())) {
    } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(tmp_allocator, *option_datum,
              expr.args_[1]->datum_meta_, expr.args_[1]->obj_meta_.has_lob_header(), option_str))) {
      LOG_WARN("fail to get real string data", K(ret), K(option_str));
    } else if (is_blank_string(expr.args_[1]->datum_meta_.cs_type_, option_str)) {
      // do nothing ====> select st_aswkb(point(1,1), '   '); ignore '   '
    } else if (OB_FAIL(ObGeoExprUtils::parse_axis_order(option_str, get_func_name(),
                                                        axis_order))) {
      LOG_WARN("fail to parse axis order option string", K(ret), K(option_str));
      ret = OB_ERR_INVALID_OPTION_KEY_VALUE_PAIR; // adapt mysql errcode.
      const uint64_t STR_LEN_MAX = 512;
      char err_str[STR_LEN_MAX] = {0};
      const int64_t len = option_str.length();
      MEMCPY(err_str, option_str.ptr(), (len >= STR_LEN_MAX ? (STR_LEN_MAX - 1) : len));
      LOG_USER_ERROR(OB_ERR_INVALID_OPTION_KEY_VALUE_PAIR, err_str, '=', get_func_name());
    } else if (OB_FAIL(ObGeoExprUtils::check_need_reverse(axis_order, need_reverse))) {
      LOG_WARN("fail to check need reverse", K(ret), K(axis_order));
    }
  }

  if (OB_SUCC(ret) && !is_null_result) {
    if (ObGeoAxisOrder::LONG_LAT == axis_order || ObGeoAxisOrder::LAT_LONG == axis_order) {
      srid_default_ordering = false;
    }
    if (srid_default_ordering && is_geog && is_lat_long_order) {
      need_reverse = true;
    }
    if (need_reverse && is_geog && OB_FAIL(ObGeoExprUtils::reverse_coordinate(geo, get_func_name()))) {
      LOG_WARN("failed to reverse geometry coordinate", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ObString res_wkb;
    ObString wkb;
    uint32_t offset = 0;
    if (is_null_result) {
      res.set_null();
    } else if (OB_FAIL(ObGeoExprUtils::geo_to_wkb(*geo, expr, ctx, srs, res_wkb))) {
      LOG_WARN("failed to write geometry to wkb", K(ret));
    } else {
      ObLobLocatorV2 lob(res_wkb, expr.obj_meta_.has_lob_header());
      if (OB_FAIL(lob.get_inrow_data(res_wkb))) {
        LOG_WARN("failed to get inrow data", K(ret), K(lob));
      } else if (OB_FAIL(ObGeoTypeUtil::get_wkb_from_swkb(res_wkb, wkb, offset))) {
        LOG_WARN("failed to get wkb from swkb", K(ret));
      } else {
        MEMMOVE(res_wkb.ptr(), res_wkb.ptr() + offset, res_wkb.length() - offset);
        res.set_string(lob.ptr_, lob.size_ - offset); // skip srid + version
      }
    }
  }

  return ret;
}

ObExprSTAsWkb::ObExprSTAsWkb(common::ObIAllocator &alloc)
    : ObExprGeomWkb(alloc,
                    T_FUN_SYS_ST_ASWKB,
                    N_ST_ASWKB,
                    ONE_OR_TWO,
                    NOT_ROW_DIMENSION)
{
}

ObExprSTAsWkb::~ObExprSTAsWkb()
{
}

int ObExprSTAsWkb::eval_st_aswkb(const ObExpr &expr,
                                 ObEvalCtx &ctx,
                                 ObDatum &res)
{
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  ObExprSTAsWkb aswkb(tmp_allocator);
  ObExprGeomWkb *p = &aswkb;
  return p->eval_geom_wkb(expr, ctx, res);
}

int ObExprSTAsWkb::cg_expr(ObExprCGCtx &expr_cg_ctx,
                           const ObRawExpr &raw_expr,
                           ObExpr &rt_expr) const
{
  UNUSEDx(expr_cg_ctx, raw_expr);
  rt_expr.eval_func_ = eval_st_aswkb;
  return common::OB_SUCCESS;
}

ObExprSTAsBinary::ObExprSTAsBinary(common::ObIAllocator &alloc)
    : ObExprGeomWkb(alloc,
                    T_FUN_SYS_ST_ASBINARY,
                    N_ST_ASBINARY,
                    ONE_OR_TWO,
                    NOT_ROW_DIMENSION)
{
}

ObExprSTAsBinary::~ObExprSTAsBinary()
{
}

int ObExprSTAsBinary::eval_st_asbinary(const ObExpr &expr,
                                       ObEvalCtx &ctx,
                                       ObDatum &res)
{
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  ObExprSTAsBinary asbin(tmp_allocator);
  ObExprGeomWkb *p = &asbin;
  return p->eval_geom_wkb(expr, ctx, res);
}

int ObExprSTAsBinary::cg_expr(ObExprCGCtx &expr_cg_ctx,
                              const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSEDx(expr_cg_ctx, raw_expr);
  rt_expr.eval_func_ = eval_st_asbinary;
  return common::OB_SUCCESS;
}
} // namespace sql
} // namespace oceanbase
