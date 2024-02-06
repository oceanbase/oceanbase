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
#ifdef OB_BUILD_ORACLE_XML
#include "lib/xml/ob_xml_parser.h"
#include "lib/xml/ob_xml_tree.h"
#include "lib/xml/ob_xml_util.h"
#include "lib/xml/ob_xpath.h"
#include "sql/engine/expr/ob_expr_xml_func_helper.h"
#endif
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

#ifdef OB_BUILD_ORACLE_XML
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
  } else if (OB_FAIL(extract_xml_text_node(mem_ctx, xml_doc, xml_res_str))) {
    LOG_WARN("fail to extract xml text node", K(ret), K(xml_res_str));
  } else if (OB_FAIL(cast_to_res(allocator, xml_res_str, expr, ctx, res))) {
    LOG_WARN("fail to cast to res", K(ret), K(xml_res_str));
  }
  return ret;
}

int ObExprXmlcast::cast_to_res(ObIAllocator &allocator, ObString &xml_content, const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObCastMode def_cm = CM_NONE;
  ObSQLSessionInfo *session = NULL;
  ObObj src_obj,dst_obj, buf_obj;
  const ObObj *res_obj = NULL;
  ObAccuracy out_acc;
  if (xml_content.empty()) {
    res.set_null();
  } else {
    src_obj.set_string(ObVarcharType, xml_content);
    src_obj.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    // to type
    if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sessioninfo is NULL");
    } else if (OB_FAIL(ObSQLUtils::get_default_cast_mode(session->get_stmt_type(),
                                                  session, def_cm))) {
      LOG_WARN("get_default_cast_mode failed", K(ret));
    } else {
      ObObjType obj_type = expr.datum_meta_.type_;
      ObCollationType cs_type = expr.datum_meta_.cs_type_;
      ObPhysicalPlanCtx *phy_plan_ctx = ctx.exec_ctx_.get_physical_plan_ctx();
      const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(session);
      ObCastCtx cast_ctx(&allocator, &dtc_params, get_cur_time(phy_plan_ctx), def_cm,
                         cs_type, NULL, NULL);
      if (OB_FAIL(ObObjCaster::to_type(obj_type, cs_type, cast_ctx, src_obj, dst_obj))) {
        LOG_WARN("failed to cast object to ", K(ret), K(src_obj), K(obj_type));
      } else if (FALSE_IT(get_accuracy_from_expr(expr, out_acc))) {
      } else if (FALSE_IT(res_obj = &dst_obj)) {
      } else if (OB_FAIL(obj_accuracy_check(cast_ctx, out_acc, cs_type, dst_obj, buf_obj, res_obj))) {
        if ((ob_is_varchar_or_char(obj_type, cs_type) || ob_is_nchar(obj_type)) && ret == OB_ERR_DATA_TOO_LONG) {
          ObLengthSemantics ls = lib::is_oracle_mode() ?
                                 expr.datum_meta_.length_semantics_ : LS_CHAR;
          const char* str = dst_obj.get_string_ptr();
          int32_t str_len_byte = dst_obj.get_string_len();
          int64_t char_len = 0;
          int32_t trunc_len_byte = 0;
          trunc_len_byte = (ls == LS_BYTE ?
                ObCharset::max_bytes_charpos(cs_type, str, str_len_byte,
                                             expr.max_length_, char_len):
                ObCharset::charpos(cs_type, str, str_len_byte, expr.max_length_));
          if (trunc_len_byte == 0) {
            (const_cast<ObObj*>(res_obj))->set_null();
          } else {
            (const_cast<ObObj*>(res_obj))->set_common_value(ObString(trunc_len_byte, str));
          }
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("accuracy check failed", K(ret), K(out_acc), K(res_obj));
        }
      } else if (OB_FAIL(ObSPIService::spi_pad_char_or_varchar(session, obj_type, out_acc, &allocator, const_cast<ObObj *>(res_obj)))) {
        LOG_WARN("fail to pad char", K(ret), K(*res_obj));
      }

      if (OB_SUCC(ret)) {
        if (OB_NOT_NULL(res_obj)) {
          res.from_obj(*res_obj);
          ObExprStrResAlloc res_alloc(expr, ctx);
          if (OB_FAIL(res.deep_copy(res, res_alloc))) {
            LOG_WARN("fail to deep copy for res datum", K(ret), KPC(res_obj), K(res));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("res obj is NULL", K(ret));
        }
      }
    }
  }
  return ret;
}


int ObExprXmlcast::extract_xml_text_node(ObMulModeMemCtx* mem_ctx, ObIMulModeBase *xml_doc, ObString &res)
{
  int ret = OB_SUCCESS;
  ObStringBuffer buff(mem_ctx->allocator_);
  ObPathExprIter xpath_iter(mem_ctx->allocator_);
  ObString xpath_str = ObString::make_string("//node()");
  ObString default_ns; // unused
  ObIMulModeBase *xml_node = NULL;
  bool is_xml_document = false;
  bool is_head_comment = true;
  if (OB_ISNULL(xml_doc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("xml doc node is NULL", K(ret));
  } else if (FALSE_IT(is_xml_document = xml_doc->type() == M_DOCUMENT)) {
  } else if (OB_FAIL(xpath_iter.init(mem_ctx, xpath_str, default_ns, xml_doc, NULL))) {
    LOG_WARN("fail to init xpath iterator", K(xpath_str), K(default_ns), K(ret));
  } else if (OB_FAIL(xpath_iter.open())) {
    LOG_WARN("fail to open xpath iterator", K(ret));
  }

  while (OB_SUCC(ret)) {
    ObString content;
    if (OB_FAIL(xpath_iter.get_next_node(xml_node))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("fail to get next xml node", K(ret));
      }
    } else if (OB_ISNULL(xml_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("xpath result node is null", K(ret));
    } else {
      ObMulModeNodeType node_type = xml_node->type();
      if (node_type != M_TEXT &&
          node_type != M_CDATA &&
          node_type != M_COMMENT &&
          node_type != M_INSTRUCT) {
        is_head_comment = false;
      } else if ((node_type == M_COMMENT || node_type == M_INSTRUCT) &&
                 (is_xml_document || (!is_xml_document && !is_head_comment))) {
        /* filter the comment node */
      } else if (OB_FAIL(xml_node->get_value(content))) {
        LOG_WARN("fail to get text node content", K(ret));
      } else if (OB_FAIL(buff.append(content))) {
        LOG_WARN("fail to append text node content", K(ret), K(content));
      }
    }
  }

  if (ret == OB_ITER_END) {
    res.assign_ptr(buff.ptr(), buff.length());
    ret = OB_SUCCESS;
  }

  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = xpath_iter.close())) {
    LOG_WARN("fail to close xpath iter", K(tmp_ret));
    ret = COVER_SUCC(tmp_ret);
  }
  return ret;
}

void ObExprXmlcast::get_accuracy_from_expr(const ObExpr &expr, ObAccuracy &accuracy)
{
  accuracy.set_length(expr.max_length_);
  accuracy.set_scale(expr.datum_meta_.scale_);
  const ObObjTypeClass &dst_tc = ob_obj_type_class(expr.datum_meta_.type_);
  if (ObStringTC == dst_tc || ObTextTC == dst_tc) {
    accuracy.set_length_semantics(expr.datum_meta_.length_semantics_);
  } else {
    accuracy.set_precision(expr.datum_meta_.precision_);
  }
}
#endif
} // sql
} // oceanbase