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
 * This file contains implementation for eval_st_distance.
 */

#include "lib/alloc/alloc_assist.h"
#define USING_LOG_PREFIX SQL_ENG

#include "lib/geo/ob_geo_func_register.h"
#include "lib/geo/ob_geo_ibin.h"
#include "sql/engine/ob_exec_context.h"
#include "observer/omt/ob_tenant_srs.h"
#include "ob_expr_st_distance.h"
#include "lib/geo/ob_geo_utils.h"
#include "sql/engine/expr/ob_geo_expr_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

struct ObGeoUnit
{
  const char *name;
  double factor;
};

const ObGeoUnit ob_geo_units[] = {
  // order by unit s, asc
  { "British chain (Benoit 1895 A)", 20.1167824 },
  { "British chain (Benoit 1895 B)", 20.1167824943758 },
  { "British chain (Sears 1922 truncated)", 20.116756 },
  { "British chain (Sears 1922)", 20.1167651215526 },
  { "British foot (1865)", 0.304800833333333 },
  { "British foot (1936)", 0.3048007491 },
  { "British foot (Benoit 1895 A)", 0.304799733333333 },
  { "British foot (Benoit 1895 B)", 0.30479973476327077 },
  { "British foot (Sears 1922 truncated)", 0.304799333333333 },
  { "British foot (Sears 1922)", 0.304799471538676 },
  { "British link (Benoit 1895 A)", 0.201167824 },
  { "British link (Benoit 1895 B)", 0.201167824943758 },
  { "British link (Sears 1922 truncated)", 0.20116756 },
  { "British link (Sears 1922)", 0.201167651215526 },
  { "British yard (Benoit 1895 A)", 0.9143992 },
  { "British yard (Benoit 1895 B)", 0.914399204289812 },
  { "British yard (Sears 1922 truncated)", 0.914398 },
  { "British yard (Sears 1922)", 0.914398414616028 },
  { "centimetre", 0.01 },
  { "chain", 20.1168 },
  { "Clarke's chain", 20.1166195164 },
  { "Clarke's foot", 0.3047972654 },
  { "Clarke's link", 0.201166195164 },
  { "Clarke's yard", 0.9143917962 },
  { "fathom", 1.8288 },
  { "foot", 0.3048 },
  { "German legal metre", 1.0000135965 },
  { "Gold Coast foot", 0.304799710181508 },
  { "Indian foot", 0.304799510248146 },
  { "Indian foot (1937)", 0.30479841 },
  { "Indian foot (1962)", 0.3047996 },
  { "Indian foot (1975)", 0.3047995 },
  { "Indian yard", 0.91439853074444 },
  { "Indian yard (1937)", 0.91439523 },
  { "Indian yard (1962)", 0.9143988 },
  { "Indian yard (1975)", 0.9143985 },
  { "kilometre", 1000 },
  { "link", 0.201168 },
  { "metre", 1 },
  { "millimetre", 0.001 },
  { "nautical mile", 1852 },
  { "Statute mile", 1609.344 },
  { "US survey chain", 20.1168402336804 },
  { "US survey foot", 0.304800609601219 },
  { "US survey link", 0.201168402336804 },
  { "US survey mile", 1609.34721869443 },
  { "yard", 0.9144 }
};

static int ob_geo_find_unit(const ObGeoUnit *units, const ObString &name, double &factor)
{
  INIT_SUCC(ret);
  int begin = 0;
  int end = sizeof(ob_geo_units)/sizeof(ObGeoUnit) - 1;
  bool is_found = false;
  while (begin <= end && !is_found) {
    int mid = begin + (end - begin) / 2;
    const int cmp_len = MIN(strlen(units[mid].name), name.length());
    int cmp_res = strncasecmp(units[mid].name, name.ptr(), cmp_len);
    if (cmp_res > 0) {
      end = mid - 1;
    } else if (cmp_res < 0) {
      begin = mid + 1;
    } else {
      if (name.length() == strlen(units[mid].name)) {
        is_found = true;
        factor = units[mid].factor;
      } else if (name.length() > strlen(units[mid].name)) {
        begin = mid + 1;
      } else {
        end = mid - 1;
      }
    }
  }

  if (!is_found) {
    ret = OB_ERR_UNIT_NOT_FOUND;
    char name_str[name.length() + 1];
    name_str[name.length()] = '\0';
    MEMCPY(name_str, name.ptr(), name.length());
    LOG_USER_ERROR(OB_ERR_UNIT_NOT_FOUND, name_str);
  }
  return ret;
}

ObExprSTDistance::ObExprSTDistance(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_ST_DISTANCE, N_ST_DISTANCE, TWO_OR_THREE, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprSTDistance::~ObExprSTDistance()
{
}

int ObExprSTDistance::calc_result_typeN(ObExprResType& type,
                                        ObExprResType* types_stack,
                                        int64_t param_num,
                                        ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  INIT_SUCC(ret);
  int unexpected_types = 0;
  int null_types = 0;

  for (int64_t i = 0; i < 2; i++) {
    if (types_stack[i].get_type() == ObNullType) {
      null_types++;
    } else if (!ob_is_geometry(types_stack[i].get_type())
        && !ob_is_string_type(types_stack[i].get_type())) { // first 2 params are geometries
      unexpected_types++;
      LOG_WARN("invalid type", K(types_stack[i].get_type()));
    } else if (ob_is_string_type(types_stack[i].get_type())) {
      // ToDo: fix later, not checking range
      // String now can be check in parse_geometry
      // types_stack[i].set_calc_type(ObGeometryType);
      // types_stack[i].set_calc_collation_type(CS_TYPE_BINARY);
      // types_stack[i].set_calc_collation_level(CS_LEVEL_IMPLICIT);
    }
  }

  const int unit_param_index = 2;
  if (param_num == 3) {
    if (types_stack[unit_param_index].get_type() == ObNullType) {
      null_types++;
    } else if (!(ob_is_string_type(types_stack[unit_param_index].get_type()))) {
      unexpected_types++;
      LOG_WARN("invalid option param type", K(types_stack[unit_param_index].get_type()));
    } else {
      types_stack[unit_param_index].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    }
  }
  // an invalid type and a null type will return null
  // an invalid type and a valid type return error
  if (null_types == 0 && unexpected_types > 0) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, N_ST_DISTANCE);
    LOG_WARN("invalid type", K(ret));
  }
  if (OB_SUCC(ret)) {
    type.set_double();
  }

  return ret;
}

int ObExprSTDistance::eval_st_distance(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *gis_datum1 = NULL;
  ObDatum *gis_datum2 = NULL;
  ObExpr *gis_arg1 = expr.args_[0];
  ObExpr *gis_arg2 = expr.args_[1];

  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
  if (OB_FAIL(gis_arg1->eval(ctx, gis_datum1)) || OB_FAIL(gis_arg2->eval(ctx, gis_datum2))) {
    LOG_WARN("eval geo args failed", K(ret));
  } else if (gis_datum1->is_null() || gis_datum2->is_null()) {
    res.set_null();
  } else {
    bool is_geo1_empty = false;
    bool is_geo2_empty = false;
    ObGeometry *geo1 = NULL;
    ObGeometry *geo2 = NULL;
    ObGeoType type1;
    ObGeoType type2;
    uint32_t srid1;
    uint32_t srid2;
    ObString wkb1 = gis_datum1->get_string();
    ObString wkb2 = gis_datum2->get_string();
    omt::ObSrsCacheGuard srs_guard;
    const ObSrsItem *srs = NULL;

    if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *gis_datum1,
              gis_arg1->datum_meta_, gis_arg1->obj_meta_.has_lob_header(), wkb1))) {
      LOG_WARN("fail to get real string data", K(ret), K(wkb1));
    } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *gis_datum2,
              gis_arg2->datum_meta_, gis_arg2->obj_meta_.has_lob_header(), wkb2))) {
      LOG_WARN("fail to get real string data", K(ret), K(wkb2));
    } else if (OB_FAIL(ObGeoExprUtils::get_srs_item(ctx, srs_guard, wkb1, srs, true, N_ST_DISTANCE))) {
      LOG_WARN("fail to get srs item", K(ret), K(wkb1));
    } else if (OB_FAIL(ObGeoExprUtils::build_geometry(temp_allocator, wkb1, geo1, srs, N_ST_DISTANCE))) {
      LOG_WARN("get first geo by wkb failed", K(ret));
    } else if (OB_FAIL(ObGeoExprUtils::build_geometry(temp_allocator, wkb2, geo2, srs, N_ST_DISTANCE))) {
      LOG_WARN("get second geo by wkb failed", K(ret));
    } else if (OB_FAIL(ObGeoTypeUtil::get_type_srid_from_wkb(wkb1, type1, srid1))) {
      LOG_WARN("get type and srid from wkb failed", K(wkb1), K(ret));
    } else if (OB_FAIL(ObGeoTypeUtil::get_type_srid_from_wkb(wkb2, type2, srid2))) {
      LOG_WARN("get type and srid from wkb failed", K(wkb2), K(ret));
    } else if (srid1 != srid2) {
      LOG_WARN("srid not the same", K(srid1), K(srid2));
      ret = OB_ERR_GIS_DIFFERENT_SRIDS;
    } else if (OB_FAIL(ObGeoExprUtils::check_empty(geo1, is_geo1_empty))
        || OB_FAIL(ObGeoExprUtils::check_empty(geo2, is_geo2_empty))) {
      LOG_WARN("check geo empty failed", K(ret));
    } else if (is_geo1_empty || is_geo2_empty) {
      res.set_null();
    } else {
      ObGeoEvalCtx gis_context(&temp_allocator, srs);
      double result = 0.0;
      if (OB_FAIL(gis_context.append_geo_arg(geo1)) || OB_FAIL(gis_context.append_geo_arg(geo2))) {
        LOG_WARN("build gis context failed", K(ret), K(gis_context.get_geo_count()));
      } else if (OB_FAIL(ObGeoFunc<ObGeoFuncType::Distance>::geo_func::eval(gis_context, result))) {
        LOG_WARN("eval st distance failed", K(ret));
        if (OB_ERR_GIS_INVALID_DATA == ret) {
          LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, N_ST_DISTANCE);
        } else {
          ObGeoExprUtils::geo_func_error_handle(ret, N_ST_DISTANCE);
        }
      } else {
        const int max_arg_num = 3;
        if (expr.arg_cnt_ == max_arg_num) {
          ObDatum *gis_unit = NULL;
          double factor = 0.0;
          if (OB_FAIL(expr.args_[max_arg_num - 1]->eval(ctx, gis_unit))) {
            LOG_WARN("eval geo unit arg failed", K(ret));
          } else if (gis_unit->is_null()) {
            res.set_null();
          } else if (srid1 == 0) {
            ret = OB_ERR_GEOMETRY_IN_UNKNOWN_LENGTH_UNIT;
            char name_str[gis_unit->get_string().length() + 1];
            name_str[gis_unit->get_string().length()] = '\0';
            MEMCPY(name_str, gis_unit->get_string().ptr(), gis_unit->get_string().length());
            LOG_USER_ERROR(OB_ERR_GEOMETRY_IN_UNKNOWN_LENGTH_UNIT, N_ST_DISTANCE, name_str);
          } else if (OB_FAIL(ob_geo_find_unit(ob_geo_units, gis_unit->get_string(), factor))) {
            LOG_WARN("invalid geo unit name", K(ret), K(gis_unit->get_string()));
          } else {
            result = result * (srs->linear_uint() / factor);
            if (std::isinf(result)) {
              ret = OB_ERR_GIS_INVALID_DATA;
              LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, N_ST_DISTANCE);
            }
            res.set_double(result);
          }
        } else {
          res.set_double(result);
        }
      }
    }
  }
  return ret;
}

int ObExprSTDistance::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_st_distance;
  return OB_SUCCESS;
}

}
}
