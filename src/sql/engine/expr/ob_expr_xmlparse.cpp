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
 * This file is for func xmlparse.
 */

#include "ob_expr_xmlparse.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_xml_func_helper.h"
#include "lib/xml/ob_xml_tree.h"
#include "lib/xml/ob_xml_util.h"

#define USING_LOG_PREFIX SQL_ENG

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

ObExprXmlparse :: ObExprXmlparse(common::ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_SYS_XMLPARSE, N_XMLPARSE, MORE_THAN_TWO, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprXmlparse::~ObExprXmlparse() {}

int ObExprXmlparse::calc_result_typeN(ObExprResType &type,
                                      ObExprResType *types_stack,
                                      int64_t param_num,
                                      ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  INIT_SUCC(ret);
  if (OB_UNLIKELY(param_num != 4)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("invalid param number", K(ret), K(param_num));
  } else if (!is_called_in_sql()) {
    ret = OB_ERR_SP_LILABEL_MISMATCH;
    LOG_WARN("expr call in pl semantics disallowed", K(ret), K(N_XMLPARSE));
    LOG_USER_ERROR(OB_ERR_SP_LILABEL_MISMATCH, static_cast<int>(strlen(N_XMLPARSE)), N_XMLPARSE);
  } else {
    // param 0 indicate type is document or content
    // param 1 is xml text for parsing
    // param 2 indicate wellformed or not
    // param 3 is output format only used with xmlagg, should be removed when xml binary ready
    ObObjType xml_type = types_stack[1].get_type();
    if (types_stack[1].get_type() == ObNullType) {
    } else if (ob_is_string_type(xml_type)) {
      if (types_stack[1].get_collation_type() == CS_TYPE_BINARY) {
        ret = OB_ERR_INVALID_XML_DATATYPE;
        LOG_USER_ERROR(OB_ERR_INVALID_XML_DATATYPE, "CHAR", ob_obj_type_str(xml_type));
        LOG_WARN("Unsupport for string type with binary charset input.", K(ret), K(xml_type));
      } else if (types_stack[1].get_charset_type() != CHARSET_UTF8MB4) {
        types_stack[1].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
      }
    } else {
      ret = OB_ERR_INVALID_XML_DATATYPE;
      LOG_USER_ERROR(OB_ERR_INVALID_XML_DATATYPE, "CHAR", ob_obj_type_str(xml_type));
      LOG_WARN("invalid type, expect char", K(ret), K(xml_type));
    }

    for (int64_t i = 0; i < param_num && OB_SUCC(ret) && i != 1; i++) {
      if (types_stack[i].get_type() == ObNullType) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param0 type is unexpected", K(types_stack[i].get_type()));
      } else if (types_stack[i].get_type() != ObIntType) {
        types_stack[i].set_calc_type(ObIntType);
      }
    }

    if (OB_SUCC(ret)) {
      type.set_sql_udt(ObXMLSqlType);
    }
  }
  return ret;
}

int ObExprXmlparse::eval_xmlparse(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
  ObDatum *xml_datum = NULL;
  ObString xml_plain_text;
  ObMulModeNodeType res_type;
  uint8_t xml_type = OB_XML_DOC_TYPE_IMPLICIT;
  uint8_t is_wellformed = OB_WELLFORMED_IMPLICIT;
  bool is_xml_text_null = false;
  bool need_format = false;

  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  ObMulModeMemCtx* mem_ctx = nullptr;
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(ObXMLExprHelper::get_tenant_id(ctx.exec_ctx_.get_my_session()), "XMLModule"));

  if (OB_ISNULL(ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get session failed.", K(ret));
  } else if (OB_FAIL(ObXmlUtil::create_mulmode_tree_context(&tmp_allocator, mem_ctx))) {
    LOG_WARN("fail to create tree memory context", K(ret));
  } else if (OB_UNLIKELY(expr.arg_cnt_ != 4)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg_cnt_", K(expr.arg_cnt_));
  } else if (OB_FAIL(expr.args_[3]->eval(ctx, xml_datum))) {
    LOG_WARN("get extra para failed", K(ret));
  } else if (FALSE_IT(need_format = (xml_datum->get_int() & 0x08) != 0)) {
  } else if (OB_FAIL(get_clause_opt(expr, ctx, 0, xml_type, OB_XML_DOC_TYPE_COUNT))) {
    LOG_WARN("get document/context error", K(ret), K(xml_type));
  } else if (OB_FAIL(get_clause_opt(expr, ctx, 2, is_wellformed, OB_WELLFORMED_COUNT))) {
    LOG_WARN("get wellformed error", K(ret), K(is_wellformed));
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, xml_datum))) {
    LOG_WARN("get extra para failed", K(ret));
  } else if (expr.args_[1]->datum_meta_.type_ == ObNullType || xml_datum->is_null()) {
    is_xml_text_null = true;
  } else if (OB_FAIL(ObTextStringHelper::get_string(expr, tmp_allocator, 1, xml_datum, xml_plain_text))) {
    LOG_WARN("get xml plain text failed", K(ret), K(is_xml_text_null));
  }

  bool is_unparsed_res = false;
  if(OB_FAIL(ret)) {
  } else if (is_xml_text_null) {
    res.set_null();
  } else {
    ObXmlDocument* doc = nullptr;
    ObXmlParser parser(mem_ctx);

    if (xml_type == OB_XML_DOCUMENT) {
      if (is_wellformed) {
        res_type = M_UNPARESED_DOC;
        if(OB_FAIL(ObMulModeFactory::add_unparsed_text_into_doc(mem_ctx, xml_plain_text, doc))) {
          LOG_WARN("add text failed", K(ret), K(xml_plain_text));
        }
      } else if (OB_FAIL(parser.parse_document(xml_plain_text))) {
        ret = OB_ERR_XML_PARSE;
        LOG_USER_ERROR(OB_ERR_XML_PARSE);
        LOG_WARN("parse xml plain text as document failed.", K(xml_plain_text));
      } else {
        res_type = M_DOCUMENT;
        doc = parser.document();
      }
    } else if (xml_type == OB_XML_CONTENT) {
      if (!is_wellformed) {
        if (OB_FAIL(parser.parse_content(xml_plain_text))) {
          ret = OB_ERR_XML_PARSE;
          LOG_USER_ERROR(OB_ERR_XML_PARSE);
          LOG_WARN("parse xml plain text as content failed.", K(xml_plain_text));
        } else {
          res_type = M_CONTENT;
          doc = parser.document();
        }
      } else {
        is_unparsed_res = true;
        res_type = M_UNPARSED;
        if (OB_ISNULL(doc = OB_NEWx(ObXmlDocument, mem_ctx->allocator_, ObMulModeNodeType::M_DOCUMENT, mem_ctx))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to create document", K(ret));
        } else if(OB_FAIL(doc->append_unparse_text(xml_plain_text))) {
          LOG_WARN("fail to add unparse text to doc", K(ret));
        }
      }
    }


    if (OB_SUCC(ret)) {
      ObCollationType session_cs_type = CS_TYPE_UTF8MB4_BIN;
      if (!doc->get_encoding().empty() || doc->get_encoding_flag()) {
        doc->set_encoding(ObXmlUtil::get_charset_name(session_cs_type));
      }
      if (OB_FAIL(ObXMLExprHelper::pack_xml_res(expr, ctx, res, doc, mem_ctx,
                                                res_type,
                                                xml_plain_text))) {
        LOG_WARN("pack_xml_res failed", K(ret));
      }
    }
  }

  return ret;
}

int ObExprXmlparse::get_clause_opt(const ObExpr &expr,
                                   ObEvalCtx &ctx,
                                   uint8_t index,
                                   uint8_t &type,
                                   uint8_t size_para)
{
  INIT_SUCC(ret);
  int num_args = expr.arg_cnt_;
  if (OB_UNLIKELY(index >= num_args)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid args num", K(index), K(num_args));
  } else {
    ObExpr *xml_arg = expr.args_[index];
    ObObjType val_type = xml_arg->datum_meta_.type_;
    ObDatum *xml_datum = NULL;
    if (OB_FAIL(xml_arg->eval(ctx, xml_datum))) {
      LOG_WARN("eval xml arg failed", K(ret));
    } else if (val_type != ObIntType) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("input type error", K(val_type));
    } else {
      int64_t option_type = xml_datum->get_int();
      if (option_type < 0 || option_type >= size_para) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("input option type error", K(option_type));
      } else {
        type = static_cast<uint8_t>(option_type);
      }
    }
  }
  return ret;
}

int ObExprXmlparse::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_xmlparse;
  return OB_SUCCESS;
}

} // sql
} // oceanbase