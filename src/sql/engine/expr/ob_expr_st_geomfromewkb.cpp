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
 * This file contains implementation for _st_geomfromewkb.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_st_geomfromewkb.h"
#include "lib/geo/ob_geo_utils.h"
#include "sql/engine/expr/ob_geo_expr_utils.h"
#include "lib/geo/ob_geo_coordinate_range_visitor.h"
#include "lib/geo/ob_geo_reverse_coordinate_visitor.h"
#include "lib/geo/ob_geo_wkb_check_visitor.h"
#include "lib/geo/ob_geo_to_tree_visitor.h"
#include "lib/geo/ob_geo_func_common.h"
#include "observer/omt/ob_tenant_srs.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprPrivSTGeomFromEWKB::ObExprPrivSTGeomFromEWKB(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_ST_GEOMFROMEWKB, N_PRIV_ST_GEOMFROMEWKB, ONE_OR_TWO, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprPrivSTGeomFromEWKB::~ObExprPrivSTGeomFromEWKB()
{
}

int ObExprPrivSTGeomFromEWKB::calc_result_typeN(ObExprResType& type,
                                            ObExprResType* types_stack,
                                            int64_t param_num,
                                            ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  UNUSED(types_stack);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(param_num != 1 && param_num != 2)) {
    ObString func_name_(N_PRIV_ST_GEOMFROMEWKB);
    ret = OB_ERR_PARAM_SIZE;
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, func_name_.length(), func_name_.ptr());
  } else {
    for (uint8_t i = 0; i < param_num && OB_SUCC(ret); i++) {
      if (ob_is_null(types_stack[i].get_type())) {
      } else if (!ob_is_string_type(types_stack[i].get_type())
                  || ObCharset::is_cs_nonascii(types_stack[i].get_collation_type())) {
        ret = OB_ERR_GIS_INVALID_DATA;
        LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, N_PRIV_ST_GEOMFROMEWKB);
      }
    }
    type.set_geometry();
    type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObGeometryType]).get_length());
  }

  return ret;
}

int ObExprPrivSTGeomFromEWKB::eval_st_geomfromewkb(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  ObDatum *datum = NULL;
  int num_args = expr.arg_cnt_;
  bool is_null_result = false;
  ObGeoAxisOrder axis_order = ObGeoAxisOrder::INVALID;
  ObString ewkb;
  ObString ewkb_copy;
  omt::ObSrsCacheGuard srs_guard;
  ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  const ObSrsItem *srs = NULL;
  ObGeometry *geo = NULL;
  ObGeometry *geo_tree = NULL;
  bool need_reverse = false;
  bool is_geographical = false;

  // get ewkb
  if (OB_FAIL(expr.args_[0]->eval(ctx, datum))) {
    LOG_WARN("failed to eval first argument", K(ret));
  } else if (datum->is_null()) {
    is_null_result = true;
  } else {
    ewkb = datum->get_string();
    ObGeoWkbHeader header;
    if (OB_FAIL(ObTextStringHelper::read_real_string_data(tmp_allocator, *datum,
        expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), ewkb))) {
      LOG_WARN("fail to get real string data", K(ret), K(ewkb));
    } else if (OB_FAIL(ob_write_string(tmp_allocator, ewkb, ewkb_copy, false))) {
      LOG_WARN("fail to deep copy ewkb", K(ret));
    } else if (OB_FAIL(get_header_info_from_ewkb(ewkb_copy, header))) {
      LOG_WARN("fail to get ewkb header info from ewkb", K(ret), K(ewkb_copy));
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, N_PRIV_ST_GEOMFROMEWKB);
    } else if (header.bo_ != ObGeoWkbByteOrder::LittleEndian) {
      ret = OB_ERR_GIS_DATA_WRONG_ENDIANESS;
      LOG_USER_ERROR(OB_ERR_GIS_DATA_WRONG_ENDIANESS);
      LOG_WARN("invalid byte order", K(ret), K(header.bo_));
    } else if (OB_FAIL(ObGeoExprUtils::get_srs_item(session->get_effective_tenant_id(), srs_guard, header.srid_, srs))) {
      LOG_WARN("fail to get srs item", K(ret), K(header.srid_));
    } else if (OB_FAIL(create_geo_by_ewkb(tmp_allocator, ewkb_copy, header, srs, geo))) {
      LOG_WARN("fail to create geometry object with raw ewkb", K(ret));
      if (ret == OB_ERR_SRS_NOT_FOUND) {
       // do nothing
      } else {
        ret = OB_ERR_GIS_INVALID_DATA;
        LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, N_PRIV_ST_GEOMFROMEWKB);
      }
    } else if (OB_NOT_NULL(srs)) {
      is_geographical = srs->is_geographical_srs();
    }
  }

  // get axis_order
  if (OB_SUCC(ret) && num_args > 1) {
    ObString axis_str;
    expr.args_[1]->eval(ctx, datum);
    if (datum->is_null()){
      // do nothing
    } else if (FALSE_IT(axis_str = datum->get_string())) {
    } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(tmp_allocator, *datum,
              expr.args_[1]->datum_meta_, expr.args_[1]->obj_meta_.has_lob_header(), axis_str))) {
      LOG_WARN("fail to get real string data", K(ret), K(axis_str));
    } else if (OB_FAIL(ObGeoExprUtils::parse_axis_order(axis_str, N_PRIV_ST_GEOMFROMEWKB, axis_order))) {
      LOG_WARN("failed to parse axis order option string", K(ret));
    } else if (OB_FAIL(ObGeoExprUtils::check_need_reverse(axis_order, need_reverse))) {
      LOG_WARN("failed to check need reverse", K(ret));
    }
  }

  if (!is_null_result && OB_SUCC(ret)) {
    if (need_reverse) {
      ObGeoReverseCoordinateVisitor reverse_visitor;
      if (OB_FAIL(geo->do_visit(reverse_visitor))) {
        ret = OB_ERR_GIS_INVALID_DATA;
        LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, N_PRIV_ST_GEOMFROMEWKB);
        LOG_WARN("failed to reverse geometry coordinate", K(ret));
      }
    }

    if (OB_SUCC(ret) && is_geographical) {
      if (OB_FAIL(ObGeoExprUtils::check_coordinate_range(srs, geo, N_PRIV_ST_GEOMFROMEWKB))) {
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
    if (OB_FAIL(ObGeoExprUtils::geo_to_wkb(*geo, expr, ctx, srs, res_wkb))) {
      LOG_WARN("failed to write geometry to wkb", K(ret));
    } else {
      res.set_string(res_wkb);
    }
  }

  return ret;
}

int ObExprPrivSTGeomFromEWKB::get_header_info_from_ewkb(const ObString &ewkb,
                                                        ObGeoWkbHeader &header)
{
  int ret = OB_SUCCESS;
  if (ewkb.length() < EWKB_COMMON_WKB_HEADER_LEN) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ewkb length", K(ewkb.length()));
  } else {
    header.srid_ = 0;
    header.bo_ = static_cast<ObGeoWkbByteOrder>(*(ewkb.ptr()));
    char *ptr = const_cast<char *>(ewkb.ptr() + WKB_GEO_BO_SIZE);
    uint32_t wkb_type = ObGeoWkbByteOrderUtil::read<uint32_t>(ptr, header.bo_);
    if (wkb_type & ObGeoTypeUtil::EWKB_M_FLAG || wkb_type & ObGeoTypeUtil::EWKB_Z_FLAG) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid ewkb type, higher than two dimension is not supported", K(ewkb.length()), K(wkb_type));
    } else {
      bool has_srid = wkb_type & ObGeoTypeUtil::EWKB_SRID_FLAG ? true : false;
      if (has_srid && ewkb.length() < (EWKB_WITH_SRID_LEN)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid ewkb length", K(ewkb.length()), K(wkb_type));
      } else {
        wkb_type &= 0x0FFFFFFF;
        if (wkb_type >= 4000) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid ewkb type", K(ewkb.length()), K(wkb_type));
        } else {
          wkb_type %= 1000;
          if (wkb_type > static_cast<uint32_t>(ObGeoType::GEOMETRYCOLLECTION)) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid ewkb type", K(ewkb.length()), K(wkb_type));
          } else {
            header.type_ = static_cast<ObGeoType>(wkb_type);
          }
        }
        if (OB_SUCC(ret) && has_srid) {
          header.srid_ = ObGeoWkbByteOrderUtil::read<uint32_t>(
              const_cast<char *>(ewkb.ptr() + EWKB_COMMON_WKB_HEADER_LEN), header.bo_);
        }
      }
    }
  }
  return ret;
}

int ObExprPrivSTGeomFromEWKB::construct_ewkb_data(ObString &ewkb,
                                                  ObString &ewkb_data)
{
  int ret = OB_SUCCESS;
  ObGeoWkbHeader header;
  if (ewkb.length() < EWKB_COMMON_WKB_HEADER_LEN) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ewkb length", K(ewkb.length()));
  } else if (OB_FAIL(get_header_info_from_ewkb(ewkb, header))) {
    LOG_WARN("fail to get ewkb header info from ewkb", K(ret), K(ewkb));
  } else {
    char *ptr = const_cast<char *>(ewkb.ptr() + WKB_GEO_BO_SIZE);
    uint32_t wkb_type = ObGeoWkbByteOrderUtil::read<uint32_t>(ptr, header.bo_);
    bool has_srid = wkb_type & ObGeoTypeUtil::EWKB_SRID_FLAG ? true : false;
    if (has_srid) {
      // get srid, [bo][type][srid] -> [**][bo][type]
      ObGeoWkbByteOrderUtil::write<uint32_t>(
          const_cast<char *>(ewkb.ptr() + EWKB_COMMON_WKB_HEADER_LEN),
          static_cast<uint32_t>(header.type_), header.bo_);
      *const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(ewkb.ptr() + WKB_GEO_SRID_SIZE))
          = static_cast<uint8_t>(header.bo_);
      ewkb_data.assign_ptr(ewkb.ptr() + WKB_GEO_SRID_SIZE, ewkb.length() - WKB_GEO_SRID_SIZE);
    } else {
      ObGeoWkbByteOrderUtil::write<uint32_t>(const_cast<char *>(ewkb.ptr() + WKB_GEO_BO_SIZE),
          static_cast<uint32_t>(header.type_), header.bo_);
      ewkb_data.assign_ptr(ewkb.ptr(), ewkb.length());
    }
  }
  return ret;
}

int ObExprPrivSTGeomFromEWKB::create_geo_by_ewkb(ObIAllocator &allocator,
                                                 ObString &ewkb,
                                                 const ObGeoWkbHeader &header,
                                                 const ObSrsItem *srs,
                                                 ObGeometry *&geo)
{
  int ret = OB_SUCCESS;

  if (ewkb.length() < EWKB_COMMON_WKB_HEADER_LEN) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("invalid ewkb length", K(ret), K(ewkb.length()));
  } else {
    ObGeoCRS crs = ObGeoCRS::Cartesian;
    ObString ewkb_data;
    if (OB_FAIL(construct_ewkb_data(ewkb, ewkb_data))) {
      LOG_WARN("fail to construct ewkb data", K(ret), K(ewkb));
    } else if (header.srid_ == 0) {
      crs = ObGeoCRS::Cartesian;
    } else if (OB_NOT_NULL(srs)) {
      crs = (srs->srs_type() == ObSrsType::PROJECTED_SRS)
          ? ObGeoCRS::Cartesian : ObGeoCRS::Geographic;
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(ObGeoTypeUtil::create_geo_by_type(allocator, header.type_,
        crs == ObGeoCRS::Geographic, true, geo, header.srid_))) {
      LOG_WARN("fail to create geo by type", K(ret), K(crs), K(header));
    } else {
      geo->set_data(ewkb_data);
      geo->set_srid(header.srid_);
      ObGeoWkbCheckVisitor ewkb_check(ewkb_data, header.bo_);
      ObIWkbGeometry *geo_bin = static_cast<ObIWkbGeometry *>(geo);
      if (OB_FAIL(geo->do_visit(ewkb_check))) {
        ret = OB_ERR_GIS_INVALID_DATA;
        LOG_WARN("fail to do ewkb check by wkb checker", K(ret), K(ewkb_data), K(header), K(crs));
      } else if (geo_bin->length() != ewkb_data.length()
          && (geo_bin->length() + WKB_GEO_SRID_SIZE != ewkb.length())) {
        ret = OB_ERR_GIS_INVALID_DATA;
        LOG_WARN("invalid ewkb length", K(ewkb_data.length()), K(geo_bin->length()), K(header));
      }
    }
  }

  return ret;
}

int ObExprPrivSTGeomFromEWKB::cg_expr(ObExprCGCtx &expr_cg_ctx,
                                  const ObRawExpr &raw_expr,
                                  ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_st_geomfromewkb;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase
