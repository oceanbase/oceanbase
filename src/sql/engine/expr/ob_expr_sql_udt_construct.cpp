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

#include "ob_expr_sql_udt_construct.h"
#include "ob_expr_sql_udt_utils.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/ob_lob_access_utils.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_sql_udt_utils.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "lib/number/ob_number_v2.h"
#include "common/object/ob_object.h"
#include "lib/json_type/ob_json_base.h"
#include "lib/geo/ob_sdo_geo_object.h"
#include "lib/geo/ob_geo_utils.h"
#ifdef OB_BUILD_ORACLE_PL
#include "pl/sys_package/ob_sdo_geometry.h"
#endif

namespace oceanbase
{
using namespace common;
namespace sql
{

OB_SERIALIZE_MEMBER((ObExprUdtConstruct, ObFuncExprOperator), udt_id_, root_udt_id_, attr_pos_);

ObExprUdtConstruct::ObExprUdtConstruct(common::ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_PRIV_SQL_UDT_CONSTRUCT, N_PRIV_SQL_UDT_CONSTRUCT, PARAM_NUM_UNKNOWN, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION,
                         false, INTERNAL_IN_ORACLE_MODE),
      udt_id_(OB_INVALID_ID),
      root_udt_id_(OB_INVALID_ID),
      attr_pos_(0) {}

ObExprUdtConstruct::~ObExprUdtConstruct() {}

int ObExprUdtConstruct::calc_result_typeN(ObExprResType &type,
                                          ObExprResType *types,
                                          int64_t param_num,
                                          ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  // need const cast to modify subschema ctx, in physcial plan ctx belong to cur exec_ctx;
  ObSQLSessionInfo *session = const_cast<ObSQLSessionInfo *>(type_ctx.get_session());
  ObExecContext *exec_ctx = OB_ISNULL(session) ? NULL : session->get_cur_exec_ctx();

  if (param_num < 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected para number", K(ret), K(param_num));
  } else if (types[0].get_type() != ObUserDefinedSQLType &&
             types[0].get_type() != ObCollectionSQLType &&
             types[0].get_type() != ObGeometryType) {
      // check udt column
      ret = OB_ERR_CALL_WRONG_ARG;
      LOG_WARN("wrong number or types of arguments in call", K(ret));
  } else if (types[0].get_type() == ObGeometryType) {
    if (udt_id_ == T_OBJ_SDO_POINT) {
      type.set_type(ObUserDefinedSQLType);
    } else if (udt_id_ == T_OBJ_SDO_ORDINATE_ARRAY ||
               udt_id_ == T_OBJ_SDO_ELEMINFO_ARRAY) {
      type.set_type(ObCollectionSQLType);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid udt type", K(ret), K(udt_id_));
    }
  } else {
    type.set_type(types[0].get_type());
  }
  if (OB_SUCC(ret)) {
    type.set_udt_id(udt_id_);
    uint16_t subschema_id;
    if (OB_ISNULL(exec_ctx) && udt_id_ != T_OBJ_XML) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("need context to search subschema mapping", K(ret), K(udt_id_));
    } else if (OB_FAIL(exec_ctx->get_subschema_id_by_udt_id(udt_id_, subschema_id))) {
      LOG_WARN("failed to get sub schema id", K(ret), K(udt_id_));
    } else {
      type.set_sql_udt(subschema_id);
    }
  }
  return ret;
}

int ObExprUdtConstruct::cg_expr(ObExprCGCtx &op_cg_ctx,
                                   const ObRawExpr &raw_expr,
                                   ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  ObIAllocator &alloc = *op_cg_ctx.allocator_;
  const ObUDTConstructorRawExpr &fun_sys
                      = static_cast<const ObUDTConstructorRawExpr &>(raw_expr);
  ObExprUdtConstructInfo *info
              = OB_NEWx(ObExprUdtConstructInfo, (&alloc), alloc, T_FUN_SYS_PRIV_SQL_UDT_CONSTRUCT);
  if (NULL == info) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else {
    OZ(info->from_raw_expr(fun_sys));
    rt_expr.extra_info_ = info;
  }
  rt_expr.eval_func_ = eval_udt_construct;

  return ret;
}

uint64_t ObExprUdtConstruct::caculate_udt_data_length(const ObExpr &expr, ObEvalCtx &ctx)
{
  uint64_t total_len = ObSqlUDT::get_null_bitmap_len(expr.arg_cnt_);
  total_len += (sizeof(int32_t) * (expr.arg_cnt_ ));
  for (int64_t i = 0; i < expr.arg_cnt_; ++i) {
    ObDatum &param = expr.locate_param_datum(ctx, i);
    total_len += param.len_;
  }
  return total_len;
}

int ObExprUdtConstruct::fill_sub_udt_nested_bitmap(ObSqlUDT &udt_obj, ObObj &nested_udt_bitmap_obj)
{
  int ret = OB_SUCCESS;
  int64_t obj_size;
  char *buf = NULL;
  int64_t pos = 0;
  ObArenaAllocator temp_allocator;
  if (OB_FAIL(ObObjUDTUtil::ob_udt_obj_value_get_serialize_size(nested_udt_bitmap_obj, obj_size))) {
    LOG_WARN("Failed to calculate serialize object size", K(ret));
  } else if (OB_ISNULL(buf = static_cast<char *>(temp_allocator.alloc(obj_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret), K(obj_size));
  } else if (OB_FAIL(ObObjUDTUtil::ob_udt_obj_value_serialize(nested_udt_bitmap_obj, buf, obj_size, pos))) {
    LOG_WARN("Failed to serialize object value", K(ret), K(nested_udt_bitmap_obj));
  } else if (OB_FAIL(udt_obj.append_attribute(0, ObString(pos, buf)))) {
    LOG_WARN("update attribute failed", K(ret), K(pos));
  }
  return ret;
}

int ObExprUdtConstruct::fill_udt_res_values(const ObExpr &expr, ObEvalCtx &ctx, ObSqlUDT &udt_obj,
                                            bool is_sub_udt, ObObj &nested_udt_bitmap_obj)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(udt_obj.init_null_bitmap())) {
    LOG_WARN("init null bitmap failed", K(ret), K(expr.arg_cnt_));
  }
  for (int64_t i = 0; i < expr.arg_cnt_ && OB_SUCC(ret); ++i) {
    ObDatum &param = expr.locate_param_datum(ctx, i);
    if (i == 0 && is_sub_udt) {
      if (OB_FAIL(fill_sub_udt_nested_bitmap(udt_obj, nested_udt_bitmap_obj))) {
        LOG_WARN("Failed to fill sub udt nested bitmap", K(ret), K(nested_udt_bitmap_obj));
      }
    } else if (!param.is_null() && expr.args_[i]->datum_meta_.type_ == ObNumberType) {
      char num_buf[common::number::ObNumber::MAX_NUMBER_ALLOC_BUFF_SIZE] = {0};
      ObObj obj;
      int64_t pos = 0;
      obj.set_meta_type(expr.args_[i]->obj_meta_);
      obj.set_number(param.get_number_desc(), const_cast<uint32_t *>(param.get_number_digits()));
      if (OB_FAIL(ObObjUDTUtil::ob_udt_obj_value_serialize(obj, num_buf, common::number::ObNumber::MAX_NUMBER_ALLOC_BUFF_SIZE, pos))) {
        LOG_WARN("Failed to serialize object value", K(ret), K(obj));
      } else if (OB_FAIL(udt_obj.append_attribute(i, ObString(pos, num_buf)))) {
        LOG_WARN("update attribute failed", K(ret), K(expr.arg_cnt_), K(i));
      }
    } else if (OB_FAIL(udt_obj.append_attribute(i, param.is_null() ? ObString() : param.get_string()))) {
      LOG_WARN("update attribute failed", K(ret), K(expr.arg_cnt_), K(i));
    }
  }
  return ret;
}

int ObExprUdtConstruct::eval_udt_construct(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *null_bit = NULL;
  bool is_null = false;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
  ObSqlUdtNullBitMap sub_nested_bitmap;
  const ObExprUdtConstructInfo *info
                  = static_cast<ObExprUdtConstructInfo *>(expr.extra_info_);
  bool is_sub_udt = info->root_udt_id_ != info->udt_id_;
  if (expr.arg_cnt_ < 1) {
    ret = OB_ERR_CALL_WRONG_ARG;
    LOG_WARN("invalid params cnt", K(ret), K(expr.arg_cnt_));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, null_bit))) {
    LOG_WARN("eval null bitmap failed", K(ret));
  } else if (null_bit->is_null()) {
    is_null = true;
    res.set_null();
  } else if (expr.args_[0]->datum_meta_.type_ == ObGeometryType) {
    ObString raw_data = null_bit->get_string();
    if (OB_FAIL(ObTextStringHelper::read_real_string_data(&temp_allocator, ObLongTextType,
                                                                 CS_TYPE_BINARY, true,
                                                                 raw_data))) {
      LOG_WARN("failed to get udt raw data", K(ret), K(info->udt_id_));
    } else if (OB_FAIL(eval_sdo_geom_udt_access(temp_allocator, expr, ctx, raw_data, info->udt_id_, res))) {
      LOG_WARN("failed to eval sdo_geom udt access", K(ret), K(info->udt_id_));
    }
  } else if (is_sub_udt) {
    // access sub udt
    uint16_t subschema_id;
    ObSqlUDTMeta sub_udt_meta;
    ObSqlUdtNullBitMap nested_bitmap;
    ObObj udt_null_bitmap;
    udt_null_bitmap.reset();
    ObString ser_element_data;
    int64_t buf_pos = 0;
    udt_null_bitmap.set_type(ObHexStringType);
    if (OB_FAIL(ObObjUDTUtil::ob_udt_obj_value_deserialize(udt_null_bitmap,
                                                           null_bit->get_string().ptr(),
                                                           null_bit->get_string().length(),
                                                           buf_pos))) {
      LOG_WARN("Failed to serialize object value", K(ret), K(ser_element_data));
    } else if (OB_FAIL(ObSqlUDT::get_null_bitmap_pos(udt_null_bitmap.get_string().ptr(),
                                                     udt_null_bitmap.get_string().length(),
                                                     info->attr_pos_,
                                                     is_null))) {
      LOG_WARN("Failed to get root null bit", K(ret));
    } else if (is_null) {
      res.set_null();
    } else if (FALSE_IT(nested_bitmap.set_bitmap(udt_null_bitmap.get_string().ptr(), udt_null_bitmap.get_string().length()))) {
    } else if (OB_FAIL(ctx.exec_ctx_.get_subschema_id_by_udt_id(info->udt_id_, subschema_id))) {
      LOG_WARN("find subschema_id failed", K(ret), K(subschema_id));
    } else if (OB_FAIL(ctx.exec_ctx_.get_sqludt_meta_by_subschema_id(subschema_id, sub_udt_meta))) {
      LOG_WARN("get udt meta failed", K(ret), K(subschema_id));
    } else {
      uint32_t nested_udt_count = sub_udt_meta.nested_udt_number_ + 1;
      uint32_t nested_udt_bitmap_len = ObSqlUDT::get_null_bitmap_len(nested_udt_count);
      char * bitmap_buffer = reinterpret_cast<char *>(temp_allocator.alloc(nested_udt_bitmap_len));
      if (OB_ISNULL(bitmap_buffer)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory sub udt bitmap", K(ret), K(nested_udt_count), K(nested_udt_bitmap_len), K(info->udt_id_));
      } else {
        MEMSET(bitmap_buffer, 0, nested_udt_bitmap_len);
        sub_nested_bitmap.set_bitmap(bitmap_buffer, nested_udt_bitmap_len);
        // bitmap is preorder traversal, sub attr of udt is adjacent
        if (OB_FAIL(sub_nested_bitmap.assign(nested_bitmap, info->attr_pos_, nested_udt_count))) {
          LOG_WARN("failed to assign sub udt bitmap", K(ret), K(info->attr_pos_), K(info->udt_id_));
        }
      }
    }
  }
  if (OB_SUCC(ret) && !is_null &&
      expr.args_[0]->datum_meta_.type_ != ObGeometryType) {
    ObObj nested_udt_bitmap_obj;
    nested_udt_bitmap_obj.set_type(ObHexStringType);
    uint64_t res_len = caculate_udt_data_length(expr, ctx);
    char *res_buf = nullptr;
    int64_t res_buf_len = 0;
    ObExprStrResAlloc expr_res_alloc(expr, ctx);
    ObTextStringResult blob_res(ObLongTextType, true, &expr_res_alloc);
    if (is_sub_udt && OB_FAIL(ObSqlUdtUtils::ob_udt_build_nested_udt_bitmap_obj(nested_udt_bitmap_obj, sub_nested_bitmap))) {
      LOG_WARN("failed to build nested udt bitmap object", K(ret));
    } else if (OB_FAIL(blob_res.init(res_len))) {
      LOG_WARN("fail to init result", K(ret), K(res_len));
    } else if (OB_FAIL(blob_res.get_reserved_buffer(res_buf, res_buf_len))) {
      LOG_WARN("failed to get reserved_buffer");
    } else if (res_buf_len < res_len) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid res buf len", K(ret), K(res_buf_len), K(res_len));
    } else {
      ObSqlUDT udt_obj;
      ObSqlUDTMeta sql_udt_meta;
      sql_udt_meta.attribute_cnt_ = expr.arg_cnt_;
      udt_obj.set_udt_meta(sql_udt_meta);
      udt_obj.set_data(ObString(res_len, 0, res_buf));
      if (OB_FAIL(fill_udt_res_values(expr, ctx, udt_obj, is_sub_udt, nested_udt_bitmap_obj))) {
        LOG_WARN("fill udt value failed", K(ret), K(res_buf_len), K(res_len));
      } else if (OB_FAIL(blob_res.lseek(res_len, 0))) {
        LOG_WARN("set result length failed", K(ret), K(res_buf_len), K(res_len));
      } else {
        ObString tmp;
        blob_res.get_result_buffer(tmp);
        res.set_string(tmp.ptr(), tmp.length());
      }
    }
  }

  return ret;
}

int ObExprUdtConstruct::eval_sdo_geom_udt_access(ObIAllocator &allocator, const ObExpr &expr, ObEvalCtx &ctx,
                                                 const ObString &swkb, uint64_t udt_id, ObDatum &res)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  ret = OB_NOT_SUPPORTED;
  LOG_WARN("not support", K(ret));
#else
  uint16_t subschema_id;
  ObSqlUDTMeta sql_udt_meta;
  ObSdoGeoObject sdo_geo;
  ObString res_str;
  ObExprStrResAlloc expr_res_alloc(expr, ctx);
  // pl obj should use this allocator
  ObIAllocator &alloc = ctx.exec_ctx_.get_allocator();

  if (OB_FAIL(ctx.exec_ctx_.get_subschema_id_by_udt_id(udt_id, subschema_id))) {
    LOG_WARN("find subschema_id failed", K(ret), K(subschema_id));
  } else if (OB_FAIL(ctx.exec_ctx_.get_sqludt_meta_by_subschema_id(subschema_id, sql_udt_meta))) {
    LOG_WARN("get udt meta failed", K(ret), K(subschema_id));
  } else if (OB_FAIL(ObGeoTypeUtil::wkb_to_sdo_geo(swkb, sdo_geo, true))) {
    LOG_WARN("fail to wkb_to_sdo_geo", K(ret), K(swkb));
  } else {
    ObSqlUDT sql_udt;
    sql_udt.set_udt_meta(sql_udt_meta);
    ObObj pl_ext;
    switch (udt_id) {
      case T_OBJ_SDO_POINT : {
        if (sdo_geo.get_point().is_null()) {
          res.set_null();
        } else if (OB_FAIL(pl::ObSdoGeometry::write_sdo_point(sdo_geo.get_point(), alloc, pl_ext))) {
          LOG_WARN("fail to transform sdo point", K(ret));
        } else if (OB_FAIL(ObSqlUdtUtils::cast_pl_record_to_sql_record(allocator,
                                                                        expr_res_alloc,
                                                                        &ctx.exec_ctx_,
                                                                        res_str,
                                                                        sql_udt,
                                                                        pl_ext))) {
          LOG_WARN("fail to cast udt sdo point", K(ret));
        } else {
          res.set_string(res_str.ptr(), res_str.length());
        }
        break;
      }
      case T_OBJ_SDO_ELEMINFO_ARRAY : {
        if (sdo_geo.get_elem_info().empty()) {
          res.set_null();
        } else if (OB_FAIL(pl::ObSdoGeometry::write_sdo_elem_info(ctx.exec_ctx_, sdo_geo.get_elem_info(), alloc, pl_ext))) {
          LOG_WARN("fail to transform sdo point", K(ret));
        } else if (OB_FAIL(ObSqlUdtUtils::cast_pl_varray_to_sql_varray(expr_res_alloc, res_str, pl_ext))) {
          LOG_WARN("fail to cast udt sdo element", K(ret));
        } else {
          res.set_string(res_str.ptr(), res_str.length());
        }
        break;
      }
      case T_OBJ_SDO_ORDINATE_ARRAY : {
        if (sdo_geo.get_ordinates().empty()) {
          res.set_null();
        } else if (OB_FAIL(pl::ObSdoGeometry::write_sdo_ordinate(ctx.exec_ctx_, sdo_geo.get_ordinates(), alloc, pl_ext))) {
          LOG_WARN("fail to transform sdo point", K(ret));
        } else if (OB_FAIL(ObSqlUdtUtils::cast_pl_varray_to_sql_varray(expr_res_alloc, res_str, pl_ext))) {
          LOG_WARN("fail to cast udt sdo element", K(ret));
        } else {
          res.set_string(res_str.ptr(), res_str.length());
        }
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid udt id", K(ret), K(udt_id));
    }
  }
#endif
  return ret;
}

OB_DEF_SERIALIZE(ObExprUdtConstructInfo)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              udt_id_,
              root_udt_id_,
              attr_pos_);
  return ret;
}

OB_DEF_DESERIALIZE(ObExprUdtConstructInfo)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
              udt_id_,
              root_udt_id_,
              attr_pos_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObExprUdtConstructInfo)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              udt_id_,
              root_udt_id_,
              attr_pos_);
  return len;
}

int ObExprUdtConstructInfo::deep_copy(common::ObIAllocator &allocator,
                                         const ObExprOperatorType type,
                                         ObIExprExtraInfo *&copied_info) const
{
  int ret = common::OB_SUCCESS;
  OZ(ObExprExtraInfoFactory::alloc(allocator, type, copied_info));
  ObExprUdtConstructInfo &other = *static_cast<ObExprUdtConstructInfo *>(copied_info);
  other.udt_id_ = udt_id_;
  other.root_udt_id_ = root_udt_id_;
  other.attr_pos_ = attr_pos_;
  return ret;
}

template <typename RE>
int ObExprUdtConstructInfo::from_raw_expr(RE &raw_expr)
{
  int ret = OB_SUCCESS;
  ObUDTConstructorRawExpr &udt_raw_expr
        = const_cast<ObUDTConstructorRawExpr &>
            (static_cast<const ObUDTConstructorRawExpr &>(raw_expr));
  udt_id_ = udt_raw_expr.get_udt_id();
  root_udt_id_ = udt_raw_expr.get_root_udt_id();
  attr_pos_ = udt_raw_expr.get_attribute_pos();
  return ret;
}

} /* sql */
} /* oceanbase */
