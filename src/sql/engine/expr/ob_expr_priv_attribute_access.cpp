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
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_priv_attribute_access.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/json_type/ob_json_base.h"
#include "lib/geo/ob_sdo_geo_object.h"
#include "lib/geo/ob_geo_utils.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

OB_SERIALIZE_MEMBER((ObExprUDTAttributeAccess, ObFuncExprOperator), udt_id_, attr_type_);

ObExprUDTAttributeAccess::ObExprUDTAttributeAccess(common::ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_PRIV_SQL_UDT_ATTR_ACCESS, N_PRIV_UDT_ATTR_ACCESS, MORE_THAN_ONE, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION,
                         false, INTERNAL_IN_ORACLE_MODE),
      udt_id_(OB_INVALID_ID),
      attr_type_(OB_INVALID_ID) {}

ObExprUDTAttributeAccess::~ObExprUDTAttributeAccess() {}

int ObExprUDTAttributeAccess::calc_result_typeN(ObExprResType &type,
                                                ObExprResType *types,
                                                int64_t param_num,
                                                ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  uint16_t subschema_id;
  // need const cast to modify subschema ctx, in physcial plan ctx belong to cur exec_ctx;
  ObSQLSessionInfo *session = const_cast<ObSQLSessionInfo *>(type_ctx.get_session());
  ObExecContext *exec_ctx = OB_ISNULL(session) ? NULL : session->get_cur_exec_ctx();
  if (param_num != 2) {
    ret = OB_ERR_CALL_WRONG_ARG;
    LOG_WARN("wrong number or types of arguments in call", K(ret));
  } else if (OB_ISNULL(exec_ctx) && udt_id_ != T_OBJ_XML) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("need context to search subschema mapping", K(ret), K(udt_id_));
  } else if (OB_FAIL(exec_ctx->get_subschema_id_by_udt_id(udt_id_, subschema_id))) {
    LOG_WARN("failed to get sub schema id", K(ret), K(udt_id_));
  } else if (types[0].is_geometry()) {
    if (udt_id_ != T_OBJ_SDO_GEOMETRY) {
      ret = OB_ERR_CALL_WRONG_ARG;
      LOG_WARN("wrong types of arguments in call", K(ret), K(udt_id_), K(types[0]));
    } else {
      types[1].set_calc_type(ObIntType);
      type.set_type(ObNumberType);
      const ObAccuracy &acc =
              ObAccuracy::DDL_DEFAULT_ACCURACY2[common::ORACLE_MODE][common::ObNumberType];
      type.set_scale(acc.get_scale());
      type.set_precision(acc.get_precision());
    }
  } else if (!types[0].is_null()
             && !types[0].is_expectd_udt_type(subschema_id) // subschema id of types[0] is already deduced
             && types[0].get_udt_id() != udt_id_) {
    ret = OB_ERR_CALL_WRONG_ARG;
    LOG_WARN("wrong types of arguments in call", K(ret), K(udt_id_), K(types[0]), K(subschema_id));
  } else {
    types[1].set_calc_type(ObIntType);
    type.set_type(static_cast<ObObjType>(attr_type_));
  }
  return ret;
}

int ObExprUDTAttributeAccess::cg_expr(ObExprCGCtx &op_cg_ctx,
                                   const ObRawExpr &raw_expr,
                                   ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  ObIAllocator &alloc = *op_cg_ctx.allocator_;
  const ObUDTAttributeAccessRawExpr &fun_sys
                      = static_cast<const ObUDTAttributeAccessRawExpr &>(raw_expr);
  ObExprUdtAttrAccessInfo *info
              = OB_NEWx(ObExprUdtAttrAccessInfo, (&alloc), alloc, T_FUN_SYS_PRIV_SQL_UDT_ATTR_ACCESS);
  if (NULL == info) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else {
    OZ(info->from_raw_expr(fun_sys));
    rt_expr.extra_info_ = info;
  }
  rt_expr.eval_func_ = eval_attr_access;

  return ret;
}

int ObExprUDTAttributeAccess::get_udt_meta_by_udt_id(uint64_t udt_id, ObSqlUDTMeta &udt_meta)
{
  int ret = OB_SUCCESS;
  // mock sdo_geometry
  udt_meta.attribute_cnt_ = 7;
  return ret;
}

int ObExprUDTAttributeAccess::eval_attr_access(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *udt_datum = NULL;
  ObDatum *attr_datum = NULL;
  int32_t attr_idx = 0;
  int num_args = expr.arg_cnt_;
  if (num_args != 2) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params number", K(ret), K(num_args));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, udt_datum))) {
    LOG_WARN("failed to eval first argument", K(ret));
  } else if (udt_datum->is_null()) {
    res.set_null();
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, attr_datum))) {
    LOG_WARN("failed to eval first argument", K(ret));
  } else if (attr_datum->is_null()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid attribute idx", K(ret));
  } else {
    attr_idx = attr_datum->get_int();
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    const ObExprUdtAttrAccessInfo *info
                  = static_cast<ObExprUdtAttrAccessInfo *>(expr.extra_info_);
    ObSqlUDT sql_udt;
    ObSqlUDTMeta sql_udt_meta;
    ObString raw_data = udt_datum->get_string();
    ObString attr_data;
    if (OB_ISNULL(info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("extra info is null", K(ret));
    } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(&temp_allocator, ObLongTextType,
                                                                 CS_TYPE_BINARY, true,
                                                                 raw_data))) {
      LOG_WARN("failed to get udt raw data", K(ret), K(info->udt_id_));
    } else if (expr.args_[0]->datum_meta_.type_ == ObGeometryType) {
      if (OB_FAIL(eval_sdo_geom_attr_access(temp_allocator, raw_data, attr_idx, res))) {
        LOG_WARN("failed to eval sdo_geometry attribute", K(ret), K(attr_idx));
      }
    } else if (OB_FAIL(get_udt_meta_by_udt_id(info->udt_id_, sql_udt_meta))) {
      LOG_WARN("failed to get udt meta", K(ret), K(info->udt_id_));
    } else if (FALSE_IT(sql_udt.set_data(raw_data))) {
    } else if (FALSE_IT(sql_udt.set_udt_meta(sql_udt_meta))) {
    } else if (OB_FAIL(sql_udt.access_attribute(attr_idx, attr_data))) {
      LOG_WARN("failed to get udt attribute data", K(ret), K(info->udt_id_), K(attr_idx), K(sql_udt_meta.attribute_cnt_));
    }
    if (OB_SUCC(ret) && expr.args_[0]->datum_meta_.type_ != ObGeometryType) {
      if (attr_data.empty()) {
        res.set_null();
      } else {
        char *res_ptr = NULL;
        if (OB_ISNULL(res_ptr = expr.get_str_res_mem(ctx, attr_data.length()))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("Failed to allocate memory for lob locator", K(ret), K(attr_data.length()));
        } else {
          ObString res_str(attr_data.length(), 0, res_ptr);
          res_str.write(attr_data.ptr(), attr_data.length());
          if (expr.datum_meta_.type_ == ObNumberType) {
            ObObj obj;
            obj.set_meta_type(expr.obj_meta_);
            int64_t pos = 0;
            if (OB_FAIL(ObObjUDTUtil::ob_udt_obj_value_deserialize(obj, res_str.ptr(), res_str.length(), pos))) {
              LOG_WARN("Failed to serialize object value", K(ret), K(res_str));
            } else {
              const number::ObNumber val = obj.get_number();
              res.set_number(val);
            }
          } else {
            res.set_string(res_str);
          }
        }
      }
    }
  }

  return ret;
}

int ObExprUDTAttributeAccess::eval_sdo_geom_attr_access(ObIAllocator &allocator, const ObString &swkb, const int32_t attr_idx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObSdoGeoObject sdo_geo;
  if (OB_FAIL(ObGeoTypeUtil::wkb_to_sdo_geo(swkb, sdo_geo, true))) {
    LOG_WARN("fail to wkb_to_sdo_geo", K(ret), K(swkb));
  } else {
    switch (static_cast<ObSdoGeoAttrIdx>(attr_idx)) {
      case ObSdoGeoAttrIdx::ObGtype : {
        uint64_t gtype_num;
        number::ObNumber gtype;
        if (OB_FAIL(ObGeoTypeUtil::get_num_by_gtype(sdo_geo.get_gtype(), gtype_num))) {
          LOG_WARN("fail to get_num_by_gtype", K(ret), K(sdo_geo.get_gtype()), K(gtype_num));
        } else if (OB_FAIL(gtype.from(gtype_num, allocator))) {
          LOG_WARN("fail to alloc memory for gtype", K(ret), K(gtype_num));
        } else {
          res.set_number(gtype);
        }
        break;
      }
      case ObSdoGeoAttrIdx::ObSrid : {
        number::ObNumber srid;
        if (sdo_geo.get_srid() == UINT32_MAX) {
          res.set_null();
        } else if (OB_FAIL(srid.from(sdo_geo.get_srid(), allocator))) {
          LOG_WARN("fail to alloc memory for gtype", K(ret), K(sdo_geo.get_srid()));
        } else {
          res.set_number(srid);
        }
        break;
      }
      case ObSdoGeoAttrIdx::ObPointX : {
        number::ObNumber x_num;
        if (sdo_geo.get_point().is_null()) {
          res.set_null();
        } else if (OB_FAIL(ObJsonBaseUtil::double_to_number(sdo_geo.get_point().get_x(), allocator, x_num))) {
          LOG_WARN("fail to alloc memory for x_num", K(ret), K(sdo_geo.get_point().get_x()));
        } else {
          res.set_number(x_num);
        }
        break;
      }
      case ObSdoGeoAttrIdx::ObPointY : {
        number::ObNumber y_num;
        if (sdo_geo.get_point().is_null()) {
          res.set_null();
        } else if (OB_FAIL(ObJsonBaseUtil::double_to_number(sdo_geo.get_point().get_y(), allocator, y_num))) {
          LOG_WARN("fail to alloc memory for y_num", K(ret), K(sdo_geo.get_point().get_y()));
        } else {
          res.set_number(y_num);
        }
        break;
      }
      case ObSdoGeoAttrIdx::ObPointZ : {
        number::ObNumber z_num;
        if (sdo_geo.get_point().is_null() || !sdo_geo.get_point().has_z()) {
          res.set_null();
        } else if (OB_FAIL(ObJsonBaseUtil::double_to_number(sdo_geo.get_point().get_z(), allocator, z_num))) {
          LOG_WARN("fail to alloc memory for z_num", K(ret), K(sdo_geo.get_point().get_z()));
        } else {
          res.set_number(z_num);
        }
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
    }
  }
  return ret;
}

OB_DEF_SERIALIZE(ObExprUdtAttrAccessInfo)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              udt_id_,
              attr_type_);
  return ret;
}

OB_DEF_DESERIALIZE(ObExprUdtAttrAccessInfo)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
              udt_id_,
              attr_type_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObExprUdtAttrAccessInfo)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              udt_id_,
              attr_type_);
  return len;
}

int ObExprUdtAttrAccessInfo::deep_copy(common::ObIAllocator &allocator,
                                         const ObExprOperatorType type,
                                         ObIExprExtraInfo *&copied_info) const
{
  int ret = common::OB_SUCCESS;
  OZ(ObExprExtraInfoFactory::alloc(allocator, type, copied_info));
  ObExprUdtAttrAccessInfo &other = *static_cast<ObExprUdtAttrAccessInfo *>(copied_info);
  other.udt_id_ = udt_id_;
  other.attr_type_ = attr_type_;
  return ret;
}

template <typename RE>
int ObExprUdtAttrAccessInfo::from_raw_expr(RE &raw_expr)
{
  int ret = OB_SUCCESS;
  ObUDTAttributeAccessRawExpr &udt_raw_expr
        = const_cast<ObUDTAttributeAccessRawExpr &>
            (static_cast<const ObUDTAttributeAccessRawExpr &>(raw_expr));
  udt_id_ = udt_raw_expr.get_udt_id();
  attr_type_ = udt_raw_expr.get_attribute_type();
  return ret;
}

} /* sql */
} /* oceanbase */
