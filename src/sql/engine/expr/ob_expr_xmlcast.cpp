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
 * This file is for func xmlcast.
 */

#include "ob_expr_xmlcast.h"
#include "share/object/ob_obj_cast.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/session/ob_sql_session_info.h"
#include "lib/xml/ob_xml_parser.h"
#include "lib/xml/ob_xml_tree.h"
#include "lib/xml/ob_xml_util.h"
#include "lib/xml/ob_xpath.h"
#include "sql/engine/expr/ob_expr_xml_func_helper.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/ob_spi.h"

#define USING_LOG_PREFIX SQL_ENG

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

ObExprXmlcast::ObExprXmlcast(common::ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_SYS_XMLCAST, N_XMLCAST, 2, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprXmlcast::~ObExprXmlcast() {}

int ObExprXmlcast::calc_result_type2(ObExprResType &type,
                                     ObExprResType &type1,
                                     ObExprResType &type2,
                                     common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  ObObjType xml_type = type1.get_type();
  uint64_t subschema_id = type1.get_subschema_id();
  if (!is_called_in_sql()) {
    ret = OB_ERR_SP_LILABEL_MISMATCH;
    LOG_WARN("expr call in pl semantics disallowed", K(ret), K(N_XMLCAST));
    LOG_USER_ERROR(OB_ERR_SP_LILABEL_MISMATCH, static_cast<int>(strlen(N_XMLCAST)), N_XMLCAST);
  } else if (type1.is_ext() && type1.get_udt_id() == T_OBJ_XML) {
      type1.get_calc_meta().set_sql_udt(ObXMLSqlType);
  } else if (!ob_is_xml_sql_type(xml_type, subschema_id)) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, "-", "-");
    LOG_WARN("inconsistent datatypes", K(ret), K(ob_obj_type_str(xml_type)));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(set_dest_type(type2, type, type_ctx))) {
    LOG_WARN("fail to set return type", K(ret));
  }

  return ret;
}

int ObExprXmlcast::set_dest_type(ObExprResType &param_type, ObExprResType &dst_type, ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  const sql::ObSQLSessionInfo *session = type_ctx.get_session();
  if (!param_type.is_int() && !param_type.get_param().is_int()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cast param type is unexpected", K(param_type));
  } else {
    const ObObj &param = param_type.get_param();
    ParseNode node;
    node.value_ = param.get_int();
    ObObjType obj_type = static_cast<ObObjType>(node.int16_values_[OB_NODE_CAST_TYPE_IDX]);
    ObCollationType cs_type = static_cast<ObCollationType>(node.int16_values_[OB_NODE_CAST_COLL_IDX]);
    int64_t text_length = node.int32_values_[1];
    ObObjTypeClass dest_tc = ob_obj_type_class(obj_type);
    ObAccuracy accuracy;
    // set obj_type
    dst_type.set_type(obj_type);
    dst_type.set_collation_type(cs_type);
    // set cs_type
    if (!ObCharset::is_valid_collation(cs_type)) {
      ObCollationType collation_connection = type_ctx.get_coll_type();
      ObCollationType collation_nation = session->get_nls_collation_nation();
      // use collation of current session
      dst_type.set_collation_type(ob_is_nstring_type(dst_type.get_type()) ?
                              collation_nation : collation_connection);
    }
    if (ob_is_string_or_lob_type(obj_type)) {
      dst_type.set_collation_level(CS_LEVEL_IMPLICIT);
    }
    // set accuracy
    if (ObStringTC == dest_tc) {
      // parser will abort all negative number
      // if length < 0 means DEFAULT_STR_LENGTH or OUT_OF_STR_LEN.
      accuracy.set_full_length(node.int32_values_[1], param_type.get_accuracy().get_length_semantics(),
                               lib::is_oracle_mode());
    } else if(ObTextTC == dest_tc) {
      accuracy.set_length(ObAccuracy::DDL_DEFAULT_ACCURACY[obj_type].get_length());
    } else if (ObIntervalTC == dest_tc) {
      if (OB_UNLIKELY(!ObIntervalScaleUtil::scale_check(node.int16_values_[3]) ||
                      !ObIntervalScaleUtil::scale_check(node.int16_values_[2]))) {
        ret = OB_ERR_DATETIME_INTERVAL_PRECISION_OUT_OF_RANGE;
      } else {
        ObScale scale = (obj_type == ObIntervalYMType) ?
          ObIntervalScaleUtil::interval_ym_scale_to_ob_scale(
              static_cast<int8_t>(node.int16_values_[3]))
          : ObIntervalScaleUtil::interval_ds_scale_to_ob_scale(
              static_cast<int8_t>(node.int16_values_[2]),
              static_cast<int8_t>(node.int16_values_[3]));
        accuracy.set_scale(scale);
      }
    } else {
      const ObAccuracy &def_acc =
        ObAccuracy::DDL_DEFAULT_ACCURACY2[lib::is_oracle_mode()][obj_type];
      if (ObNumberType == obj_type && 0 == node.int16_values_[2]) {
        accuracy.set_precision(def_acc.get_precision());
      } else {
        accuracy.set_precision(node.int16_values_[2]);
      }
      accuracy.set_scale(node.int16_values_[3]);
      if (lib::is_oracle_mode() && ObDoubleType == obj_type) {
        accuracy.set_accuracy(def_acc.get_precision());
      }
    }
    dst_type.set_accuracy(accuracy);
  }
  return ret;
}

int ObExprXmlcast::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_xmlcast;
  return OB_SUCCESS;
}

int ObExprXmlcast::eval_xmlcast(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObExpr *xml_expr = NULL;
  ObDatum *xml_datum = NULL;
  ObIMulModeBase *xml_doc = NULL;
  ObString xml_res_str;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &allocator = tmp_alloc_g.get_allocator();
  ObCollationType cs_type = CS_TYPE_INVALID;

  ObMulModeMemCtx* mem_ctx = nullptr;
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(ObXMLExprHelper::get_tenant_id(ctx.exec_ctx_.get_my_session()), "XMLModule"));

  if (OB_ISNULL(ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get session failed.", K(ret));
  } else if (OB_FAIL(ObXmlUtil::create_mulmode_tree_context(&allocator, mem_ctx))) {
    LOG_WARN("fail to create tree memory context", K(ret));
  } else if (OB_UNLIKELY(expr.arg_cnt_ != 2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg_cnt_", K(ret), K(expr.arg_cnt_));
  } else if (OB_FAIL(ObXMLExprHelper::get_xmltype_from_expr(expr.args_[0], ctx, xml_datum))) {
    // temporally use
    LOG_WARN("fail to get xml str", K(ret));
  } else if (OB_FAIL(ObXMLExprHelper::get_xml_base(mem_ctx, xml_datum, cs_type, ObNodeMemType::BINARY_TYPE, xml_doc))) {
    LOG_WARN("fail to parse xml doc", K(ret));
  } else if (OB_FAIL(ObXMLExprHelper::extract_xml_text_node(mem_ctx, xml_doc, xml_res_str))) {
    LOG_WARN("fail to extract xml text node", K(ret), K(xml_res_str));
  } else if (OB_FAIL(ObXMLExprHelper::cast_to_res(allocator, xml_res_str, expr, ctx, res))) {
    LOG_WARN("fail to cast to res", K(ret), K(xml_res_str));
  }
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprXmlcast, raw_expr) {
  int ret = OB_SUCCESS;
  SET_LOCAL_SYSVAR_CAPACITY(5);
  EXPR_ADD_LOCAL_SYSVAR(SYS_VAR_NLS_DATE_FORMAT);
  EXPR_ADD_LOCAL_SYSVAR(SYS_VAR_NLS_TIMESTAMP_FORMAT);
  EXPR_ADD_LOCAL_SYSVAR(SYS_VAR_NLS_TIMESTAMP_TZ_FORMAT);
  EXPR_ADD_LOCAL_SYSVAR(SYS_VAR_TIME_ZONE);
  EXPR_ADD_LOCAL_SYSVAR(SYS_VAR_COLLATION_CONNECTION);
  return ret;
}

} // sql
} // oceanbase