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
 * This file contains implementation for _st_asewkb expr.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_priv_st_asewkb.h"
#include "lib/geo/ob_geo_utils.h"
#include "sql/engine/expr/ob_geo_expr_utils.h"
#include "observer/omt/ob_tenant_srs.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprStPrivAsEwkb::ObExprStPrivAsEwkb(common::ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_PRIV_ST_ASEWKB, N_PRIV_ST_ASEWKB, ONE_OR_TWO, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprStPrivAsEwkb::~ObExprStPrivAsEwkb()
{
}

int ObExprStPrivAsEwkb::calc_result_typeN(ObExprResType& type,
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

  if (2 == param_num) { // _st_asewkb暂时还不支持大小端翻转
    ObString fun_name(N_PRIV_ST_ASEWKB);
    ret = OB_ERR_PARAM_SIZE;
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, fun_name.length(), fun_name.ptr());
  }

  return ret;
}

int ObExprStPrivAsEwkb::eval_priv_st_as_ewkb(const ObExpr &expr,
                                             ObEvalCtx &ctx,
                                             ObDatum &res)
{
  int ret = OB_SUCCESS;
  uint32_t arg_num = expr.arg_cnt_;
  bool is_null_result = false;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  const ObSrsItem *srs = NULL;
  omt::ObSrsCacheGuard srs_guard;
  bool is_geog = false;
  ObDatum *wkb_datum = NULL;
  ObGeometry *geo = NULL;
  ObString wkb_str;

  if (OB_FAIL(expr.args_[0]->eval(ctx, wkb_datum))) {
    LOG_WARN("fail to eval wkb datum", K(ret));
  } else if (wkb_datum->is_null()) {
    is_null_result = true;
  } else if (FALSE_IT(wkb_str = wkb_datum->get_string())) {
  } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(tmp_allocator, *wkb_datum,
              expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), wkb_str))) {
    LOG_WARN("fail to get real data.", K(ret), K(wkb_str));
  } else if (OB_FAIL(ObGeoExprUtils::get_srs_item(ctx, srs_guard, wkb_str, srs))) {
    LOG_WARN("fail to get srs item", K(ret), K(wkb_str));
  } else if (OB_FAIL(ObGeoTypeUtil::create_geo_by_wkb(tmp_allocator, wkb_str, srs, geo, true, true, true))) {
    LOG_WARN("fail to create geo by wkb", K(ret), K(wkb_str));
    if (ret != OB_ERR_SRS_NOT_FOUND && ret != OB_ERR_INVALID_GEOMETRY_TYPE) {
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, N_PRIV_ST_ASEWKB);
    }
  } else if (OB_NOT_NULL(srs)) {
      is_geog = srs->is_geographical_srs();
  }

  if (OB_SUCC(ret)) {
    ObString res_wkb;
    const int64_t data_offset = WKB_OFFSET + WKB_GEO_BO_SIZE + WKB_GEO_TYPE_SIZE;
    if (is_null_result) {
      res.set_null();
    } else if (OB_FAIL(ObGeoExprUtils::geo_to_wkb(*geo, expr, ctx, srs, res_wkb))) {
      LOG_WARN("failed to write geometry to wkb", K(ret));
    } else {
      ObLobLocatorV2 lob(res_wkb, expr.obj_meta_.has_lob_header());
      ObGeoWkbHeader header;
      if (is_geog && OB_FAIL(ObGeoExprUtils::check_coordinate_range(srs, geo, N_PRIV_ST_ASEWKB, true))) {
        LOG_WARN("fail to check coordinate range", K(ret));
      } else if (OB_FAIL(lob.get_inrow_data(res_wkb))) {
        LOG_WARN("fail to get inrow data", K(ret), K(lob));
      } else if (res_wkb.length() < data_offset) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected wkb length", K(ret), K(res_wkb.length()));
      } else if (OB_FAIL(ObGeoTypeUtil::get_header_info_from_wkb(res_wkb, header))) {
        LOG_WARN("fail to get wkb header info", K(ret), K(res_wkb));
      } else {
        // ewkb:[bo][type][srid][data]
        // swkb:[srid][version][bo][type][data]
        *(reinterpret_cast<uint8_t *>(res_wkb.ptr())) = static_cast<uint8_t>(header.bo_); // 1. write [bo]
        int64_t pos = WKB_GEO_BO_SIZE + WKB_GEO_TYPE_SIZE;
        int64_t remove_len = WKB_VERSION_SIZE;
        uint32_t geo_type = static_cast<uint32_t>(header.type_);
        bool is_3d_geo = ObGeoTypeUtil::is_3d_geo_type(geo->type());
        //transform to EWKB format
        geo_type = is_3d_geo ? ((geo_type - ObGeoTypeUtil::WKB_3D_TYPE_OFFSET) | ObGeoTypeUtil::EWKB_Z_FLAG) : geo_type;
        if (0 != header.srid_) {
          ObGeoWkbByteOrderUtil::write<uint32_t>(res_wkb.ptr() + WKB_GEO_BO_SIZE,
              geo_type | ObGeoTypeUtil::EWKB_SRID_FLAG, header.bo_); // 2. write [type]
          ObGeoWkbByteOrderUtil::write<uint32_t>(res_wkb.ptr() + WKB_GEO_BO_SIZE
              + WKB_GEO_TYPE_SIZE, header.srid_, header.bo_); // write [srid]
          pos += WKB_GEO_SRID_SIZE;
        } else { // 当srid为0时，ewkb中不输出srid字段
          ObGeoWkbByteOrderUtil::write<uint32_t>(res_wkb.ptr() + WKB_GEO_BO_SIZE, geo_type, header.bo_); // 2. write [type]
          remove_len += WKB_GEO_SRID_SIZE;
        }
        MEMMOVE(res_wkb.ptr() + pos, res_wkb.ptr() + data_offset, res_wkb.length() - data_offset);// write [data]
        res.set_string(lob.ptr_, lob.size_ - remove_len);
      }
    }
  }

  return ret;
}

int ObExprStPrivAsEwkb::cg_expr(ObExprCGCtx &expr_cg_ctx,
                                const ObRawExpr &raw_expr,
                                ObExpr &rt_expr) const
{
  UNUSEDx(expr_cg_ctx, raw_expr);
  rt_expr.eval_func_ = eval_priv_st_as_ewkb;
  return common::OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase