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
 * This file contains implementation for point expr.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_point.h"
#include "lib/geo/ob_geo_bin.h"
#include "lib/geo/ob_geo_utils.h"
#include "sql/engine/expr/ob_geo_expr_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprPoint::ObExprPoint(common::ObIAllocator &alloc)
    : ObFuncExprOperator(alloc,
                         T_FUN_SYS_POINT,
                         N_POINT,
                         2,
                         VALID_FOR_GENERATED_COL,
                         NOT_ROW_DIMENSION)
{
}

ObExprPoint::~ObExprPoint()
{

}

int ObExprPoint::calc_result_type2(ObExprResType &type,
                                   ObExprResType &type1,
                                   ObExprResType &type2,
                                   common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  ObObjType type_x = type1.get_type();
  ObObjType type_y = type2.get_type();

  if (ob_is_geometry_tc(type_x) || ob_is_geometry_tc(type_y)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "input type should not be geometry type");
  } else {
    if (!ob_is_double_tc(type_x)) {
      type1.set_calc_type(ObDoubleType);
    }
    if (!ob_is_double_tc(type_y)) {
      type2.set_calc_type(ObDoubleType);
    }
    type.set_geometry();
    type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObGeometryType]).get_length());
  }

  return ret;
}

// for old sql engine
int ObExprPoint::calc_result2(common::ObObj &result,
                              const common::ObObj &obj1,
                              const common::ObObj &obj2,
                              common::ObExprCtx &expr_ctx) const
{
  int ret = OB_SUCCESS;
  bool is_null_result = false;
  ObIAllocator *allocator = expr_ctx.calc_buf_;
  ObWkbBuffer res_wkb_buf(*allocator);

  if (OB_ISNULL(allocator)) { // check allocator
    ret = OB_NOT_INIT;
    LOG_WARN("buffer not init", K(ret));
  } else {
    ObObjType type_x = obj1.get_type();
    ObObjType type_y = obj2.get_type();
    if (ob_is_null(type_x) || ob_is_null(type_y)) {
      is_null_result = true;
    } else if (obj1.is_null() || obj2.is_null()) {
      is_null_result = true;
    } else {
      double x = obj1.get_double();
      double y = obj2.get_double();
      uint32_t srid = 0;
      if (OB_FAIL(res_wkb_buf.append(srid))) {
        LOG_WARN("fail to append srid to point wkb buf", K(ret), K(srid));
      } else if (OB_FAIL(res_wkb_buf.append(static_cast<char>(ENCODE_GEO_VERSION(GEO_VESION_1))))) {
         LOG_WARN("fail to append version to point wkb buf", K(ret));
      } else if (OB_FAIL(res_wkb_buf.append(static_cast<char>(ObGeoWkbByteOrder::LittleEndian)))) {
        LOG_WARN("fail to append little endian byte order to point wkb buf", K(ret));
      } else if (OB_FAIL(res_wkb_buf.append(static_cast<uint32_t>(ObGeoType::POINT)))) {
        LOG_WARN("fail to append geo type to point wkb buf", K(ret));
      } else if (OB_FAIL(res_wkb_buf.append(x))) {
        LOG_WARN("fail to append x to point wkb buf", K(ret), K(x));
      } else if (OB_FAIL(res_wkb_buf.append(y))) {
        LOG_WARN("fail to append y to point wkb buf", K(ret), K(y));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (is_null_result) {
      result.set_null();
    } else {
      char *buf = reinterpret_cast<char *>(allocator->alloc(res_wkb_buf.length()));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory for result buf", K(ret), K(res_wkb_buf.length()));
      } else {
        MEMMOVE(buf, res_wkb_buf.ptr(), res_wkb_buf.length());
        result.set_collation_type(result_type_.get_collation_type());
        result.set_string(ObGeometryType, buf, res_wkb_buf.length());
        result.set_collation_level(CS_LEVEL_IMPLICIT);
      }
    }
  }

  return ret;
}

int ObExprPoint::eval_point(const ObExpr &expr,
                            ObEvalCtx &ctx,
                            ObDatum &res)
{
	int ret = OB_SUCCESS;
  bool is_null_result = false;
  ObDatum *datum_x = NULL;
  ObDatum *datum_y = NULL;
  ObExpr *arg_x = expr.args_[0];
  ObExpr *arg_y = expr.args_[1];
  ObObjType type_x = arg_x->datum_meta_.type_;
  ObObjType type_y = arg_y->datum_meta_.type_;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  ObWkbBuffer res_wkb_buf(tmp_allocator);

  if (ob_is_null(type_x) || ob_is_null(type_y)) {
    is_null_result = true;
  } else if (OB_FAIL(arg_x->eval(ctx, datum_x))) {
    LOG_WARN("fail to eval point x arg", K(ret), K(type_x));
  } else if (OB_FAIL(arg_y->eval(ctx, datum_y))) {
    LOG_WARN("fail to eval point y arg", K(ret), K(type_y));
  } else if (datum_x->is_null() || datum_y->is_null()) {
    is_null_result = true;
  } else {
    double x = datum_x->get_double();
    double y = datum_y->get_double();
    uint32_t srid = 0;
    if (OB_FAIL(res_wkb_buf.append(srid))) {
      LOG_WARN("fail to append srid to point wkb buf", K(ret), K(srid));
    } else if (OB_FAIL(res_wkb_buf.append(static_cast<char>(ENCODE_GEO_VERSION(GEO_VESION_1))))) {
      LOG_WARN("fail to append version to point wkb buf", K(ret));
    } else if (OB_FAIL(res_wkb_buf.append(static_cast<char>(ObGeoWkbByteOrder::LittleEndian)))) {
      LOG_WARN("fail to append little endian byte order to point wkb buf", K(ret));
    } else if (OB_FAIL(res_wkb_buf.append(static_cast<uint32_t>(ObGeoType::POINT)))) {
      LOG_WARN("fail to append geo type to point wkb buf", K(ret));
    } else if (OB_FAIL(res_wkb_buf.append(x))) {
      LOG_WARN("fail to append x to point wkb buf", K(ret), K(x));
    } else if (OB_FAIL(res_wkb_buf.append(y))) {
      LOG_WARN("fail to append y to point wkb buf", K(ret), K(y));
    }
  }

  if (OB_SUCC(ret)) {
    if (is_null_result) {
      res.set_null();
    } else if (OB_FAIL(ObGeoExprUtils::pack_geo_res(expr, ctx, res, res_wkb_buf.string()))) {
      LOG_WARN("fail to pack geo res", K(ret));
    }
  }

  return ret;
}

int ObExprPoint::cg_expr(ObExprCGCtx &expr_cg_ctx,
                         const ObRawExpr &raw_expr,
                         ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_point;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase