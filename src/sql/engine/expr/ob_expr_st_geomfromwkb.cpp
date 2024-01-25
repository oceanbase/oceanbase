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
 * This file contains implementation for st_geomfromwkb.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_st_geomfromwkb.h"
#include "sql/engine/expr/ob_geo_expr_utils.h"
#include "lib/geo/ob_geo_coordinate_range_visitor.h"
#include "lib/geo/ob_geo_reverse_coordinate_visitor.h"
#include "lib/geo/ob_geo_to_tree_visitor.h"
#include "lib/geo/ob_geo_wkb_check_visitor.h"
#include "lib/geo/ob_wkb_byte_order_visitor.h"
#include "lib/geo/ob_geo_func_common.h"
#include "lib/geo/ob_geo_3d.h"
#include "observer/omt/ob_tenant_srs.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

ObIExprSTGeomFromWKB::ObIExprSTGeomFromWKB(common::ObIAllocator &alloc, ObExprOperatorType type,
                                           const char *name, int32_t param_num, ObValidForGeneratedColFlag valid_for_generated_col, int32_t dimension)
  : ObFuncExprOperator(alloc, type, name, param_num, valid_for_generated_col, dimension)
{
}

ObExprSTGeomFromWKB::ObExprSTGeomFromWKB(ObIAllocator &alloc)
    : ObIExprSTGeomFromWKB(alloc, T_FUN_SYS_ST_GEOMFROMWKB, N_ST_GEOMFROMWKB, MORE_THAN_ZERO, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprSTGeometryFromWKB::ObExprSTGeometryFromWKB(ObIAllocator &alloc)
    : ObIExprSTGeomFromWKB(alloc, T_FUN_SYS_ST_GEOMETRYFROMWKB, N_ST_GEOMETRYFROMWKB, MORE_THAN_ZERO, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

int ObIExprSTGeomFromWKB::calc_result_typeN(ObExprResType& type,
                                            ObExprResType* types_stack,
                                            int64_t param_num,
                                            ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  UNUSED(types_stack);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(param_num > 3)) {
    ObString func_name_(get_name());
    ret = OB_ERR_PARAM_SIZE;
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, func_name_.length(), func_name_.ptr());
  } else {
    for (uint8_t i = 0; i < param_num && OB_SUCC(ret); i++) {
      if (i == 0) {
        if (ob_is_null(types_stack[i].get_type())) {
        } else if (!ob_is_string_type(types_stack[i].get_type())
                   || ObCharset::is_cs_nonascii(types_stack[i].get_collation_type())) {
          types_stack[i].set_calc_type(ObVarcharType);
          types_stack[i].set_calc_collation_type(CS_TYPE_BINARY);
        }
      }
      // srid
      if (i == 1) {
        types_stack[i].set_calc_type(ObIntType);
        type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_STRING_INTEGER_TRUNC);
      }
      if (i == 2) {
        if (ob_is_string_type(types_stack[i].get_type())
            || ob_is_null(types_stack[i].get_type())) {
          // do nothing
        } else {
          types_stack[i].set_calc_type(ObVarcharType);
          types_stack[i].set_calc_collation_type(types_stack[i].get_collation_type());
          types_stack[i].set_calc_collation_level(types_stack[i].get_collation_level());
        }
      }
    }
    type.set_geometry();
    type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObGeometryType]).get_length());
  }

  return ret;
}

int ObIExprSTGeomFromWKB::eval_geom_wkb(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  ObDatum *datum = NULL;
  int num_args = expr.arg_cnt_;
  bool is_null_result = false;
  ObGeoAxisOrder axis_order = ObGeoAxisOrder::INVALID;
  ObString wkb;
  ObString wkb_copy;
  const ObSrsItem *srs_item = NULL;
  ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  omt::ObSrsCacheGuard srs_guard;
  ObGeometry *geo = NULL;
  ObGeometry *geo_tree = NULL;
  bool need_reverse = false;
  bool is_geographical = false;
  bool is_lat_long = false;
  ObGeoWkbByteOrder bo = ObGeoWkbByteOrder::LittleEndian;
  uint32_t srid = 0;
  bool is_3d_geo = false;
  // get srid
  if (num_args > 1) {
    expr.args_[1]->eval(ctx, datum);
    if (datum->is_null()) {
      is_null_result = true;
    } else if (datum->get_int() < 0 || datum->get_int() > UINT_MAX32) {
         ret = OB_OPERATE_OVERFLOW;
         LOG_WARN("srid input value out of range", K(ret), K(datum->get_int()));
    } else if (0 != (srid = datum->get_uint32())) {
      if (OB_FAIL(OTSRS_MGR->get_tenant_srs_guard(srs_guard))) {
        LOG_WARN("failed to get srs guard", K(ret));
      } else if (OB_FAIL(srs_guard.get_srs_item(srid, srs_item))) {
        LOG_WARN("failed to get srs item", K(ret));
      } else if (OB_ISNULL(srs_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null srs item", K(ret));
      } else {
        is_geographical = srs_item->is_geographical_srs();
        is_lat_long = srs_item->is_lat_long_order();
        need_reverse = (is_geographical && is_lat_long);
      }
    }
  }

  // get axis_order
  if (!is_null_result && OB_SUCC(ret) && num_args > 2) {
    ObString axis_str;
    expr.args_[2]->eval(ctx, datum);
    if (datum->is_null()){
      is_null_result = true;
    } else if (FALSE_IT(axis_str = datum->get_string())) {
    } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(tmp_allocator, *datum,
              expr.args_[2]->datum_meta_, expr.args_[2]->obj_meta_.has_lob_header(), axis_str))) {
      LOG_WARN("fail to get real string data", K(ret), K(axis_str));
    } else if (OB_FAIL(ObGeoExprUtils::parse_axis_order(axis_str, get_func_name(), axis_order))) {
      LOG_WARN("failed to parse axis order option string", K(ret));
    } else if (OB_FAIL(ObGeoExprUtils::check_need_reverse(axis_order, need_reverse))) {
      LOG_WARN("failed to check need reverse", K(ret));
    }
  }

  // get wkb
  if (!is_null_result && OB_SUCC(ret)) {
    if (OB_FAIL(expr.args_[0]->eval(ctx, datum))) {
      LOG_WARN("failed to eval wkb", K(ret));
    } else if (datum->is_null()){
      is_null_result = true;
    } else {
      wkb = datum->get_string();
      if (OB_FAIL(ObTextStringHelper::read_real_string_data(tmp_allocator, *datum,
          expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), wkb))) {
        LOG_WARN("fail to get real string data", K(ret), K(wkb));
      } else if (OB_FAIL(ob_write_string(tmp_allocator, wkb, wkb_copy, false))) {
        LOG_WARN("fail to deep copy wkb", K(ret));
      } else if (OB_FAIL(create_by_wkb_without_srid(tmp_allocator, wkb_copy, srs_item, geo, bo))) {
        LOG_WARN("failed to create geometry object with raw wkb", K(ret));
        ret = OB_ERR_GIS_INVALID_DATA;
        LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, get_func_name());
      } else if (OB_NOT_NULL(srs_item)) {
        is_geographical = srs_item->is_geographical_srs();
      }
      if (OB_SUCC(ret)) {
        geo->set_srid(srid);
        is_3d_geo = ObGeoTypeUtil::is_3d_geo_type(geo->type());
      }
    }
  }

  if (!is_null_result && OB_SUCC(ret)) {
    if (need_reverse && is_geographical && OB_FAIL(ObGeoExprUtils::reverse_coordinate(geo, get_func_name()))) {
      LOG_WARN("failed to reverse geometry coordinate", K(ret));
    }

    if (OB_SUCC(ret) && !is_3d_geo && bo == ObGeoWkbByteOrder::BigEndian) {
      ObGeoToTreeVisitor tree_visitor(&tmp_allocator);
      if (OB_FAIL(geo->do_visit(tree_visitor))) {
        LOG_WARN("fail to do visit", K(ret), K(geo->type()));
      } else {
        geo = tree_visitor.get_geometry();
      }
    }

    if (OB_SUCC(ret)&& is_geographical) {
      if (OB_FAIL(ObGeoExprUtils::check_coordinate_range(srs_item, geo, get_func_name()))) {
        LOG_WARN("check geo coordinate range failed", K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (is_null_result) {
    res.set_null();
  } else if (OB_ISNULL(geo)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null geometry", K(ret));
  } else {
    ObString res_wkb;
    if (OB_FAIL(ObGeoExprUtils::geo_to_wkb(*geo, expr, ctx, srs_item, res_wkb))) {
      LOG_WARN("failed to write geometry to wkb", K(ret));
    } else {
      res.set_string(res_wkb);
    }
  }

  return ret;
}

int ObIExprSTGeomFromWKB::create_by_wkb_without_srid(ObIAllocator &allocator,
                                                     const ObString &wkb,
                                                     const ObSrsItem *srs_item,
                                                     ObGeometry *&geo,
                                                     ObGeoWkbByteOrder &bo) const
{
  INIT_SUCC(ret);
  if (wkb.length() < WKB_COMMON_WKB_HEADER_LEN) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("invalid wkb length", K(wkb.length()));
  } else {
    ObGeoType type;
    uint32_t srid;
    ObGeoCRS crs;
    if (srs_item == NULL) {
      srid = 0;
      crs = ObGeoCRS::Cartesian;
    } else {
      srid = srs_item->get_srid();
      crs = (srs_item->srs_type() == ObSrsType::PROJECTED_SRS)
             ? ObGeoCRS::Cartesian : ObGeoCRS::Geographic;
    }

    if (OB_FAIL(get_type_bo_from_wkb_without_srid(wkb, type, bo))) {
      LOG_WARN("get type and bo from wkb failed", K(wkb), K(ret));
    } else if (OB_FAIL(ObGeoTypeUtil::create_geo_by_type(allocator, type,
                                                         crs == ObGeoCRS::Geographic,
                                                         true, geo))) {
      LOG_WARN("failed to create wkb", K(ret), K(crs), K(type));
    } else {
      geo->set_srid(srid);
      ObString wkb_nosrid(wkb.length(), wkb.ptr());
      geo->set_data(wkb_nosrid);
      if (ObGeoTypeUtil::is_3d_geo_type(geo->type())) {
        ObGeometry3D *geo_3d = static_cast<ObGeometry3D *>(geo);
        if (OB_FAIL(geo_3d->check_wkb_valid())) {
          LOG_WARN("invalid wkb", K(ret), K(type), K(srid), K(crs));
        }
      } else {
        ObGeoWkbCheckVisitor wkb_check(wkb_nosrid, bo);
        ObIWkbGeometry *geo_bin = static_cast<ObIWkbGeometry *>(geo);
        if (OB_FAIL(geo->do_visit(wkb_check))) {
          ret = OB_ERR_GIS_INVALID_DATA;
          LOG_WARN("invalid wkb", K(wkb), K(type), K(srid), K(crs));
        } else if (bo == ObGeoWkbByteOrder::BigEndian) {
          // transform to LittleEndian
          ObWkbByteOrderVisitor bo_visitor(&allocator, ObGeoWkbByteOrder::LittleEndian);
          if (OB_FAIL(geo->do_visit(bo_visitor))) {
            LOG_WARN("fail to transform big endian to little endian", K(ret));
          } else {
            geo->set_data(bo_visitor.get_wkb());
            bo = ObGeoWkbByteOrder::LittleEndian;
          }
        }
        if (OB_SUCC(ret) && geo_bin->length() != wkb.length()) {
          ret = OB_ERR_GIS_INVALID_DATA;
          LOG_WARN("invalid wkb", K(wkb.length()), K(geo_bin->length()), K(type), K(srid), K(crs));
        }
      }
    }
  }

  return ret;
}

int ObIExprSTGeomFromWKB::get_type_bo_from_wkb_without_srid(const ObString &wkb,
                                                            ObGeoType &type,
                                                            ObGeoWkbByteOrder &bo) const
{
  int ret = OB_SUCCESS;
  if (wkb.length() < WKB_GEO_BO_SIZE + WKB_GEO_TYPE_SIZE) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid wkb1 length", K(wkb.length()));
  } else {
    bo = static_cast<ObGeoWkbByteOrder>(*(wkb.ptr()));
    char *ptr = const_cast<char *>(wkb.ptr() + WKB_GEO_BO_SIZE);
    type = static_cast<ObGeoType>(ObGeoWkbByteOrderUtil::read<uint32_t>(ptr, bo));
  }
  return ret;
}

int ObExprSTGeomFromWKB::eval_st_geomfromwkb(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  ObExprSTGeomFromWKB st_geom_from_wkb(tmp_allocator);
  ObIExprSTGeomFromWKB *p = &st_geom_from_wkb;
  return p->eval_geom_wkb(expr, ctx, res);
}

int ObExprSTGeomFromWKB::cg_expr(ObExprCGCtx &expr_cg_ctx,
                                 const ObRawExpr &raw_expr,
                                 ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_st_geomfromwkb;
  return OB_SUCCESS;
}

int ObExprSTGeometryFromWKB::eval_st_geometryfromwkb(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  ObExprSTGeometryFromWKB st_geometry_from_wkb(tmp_allocator);
  ObIExprSTGeomFromWKB *p = &st_geometry_from_wkb;
  return p->eval_geom_wkb(expr, ctx, res);
}

int ObExprSTGeometryFromWKB::cg_expr(ObExprCGCtx &expr_cg_ctx,
                                     const ObRawExpr &raw_expr,
                                     ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_st_geometryfromwkb;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase
