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
 * This file contains implementation for _make_xml_binary.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_priv_xml_binary.h"
#include "ob_expr_lob_utils.h"
#ifdef OB_BUILD_ORACLE_XML
#include "sql/engine/expr/ob_expr_xml_func_helper.h"
#include "lib/xml/ob_xml_util.h"
#include "sql/engine/ob_exec_context.h"
#endif

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
// Internal expression used for generate binary xml from xmltype data
ObExprPrivXmlBinary::ObExprPrivXmlBinary(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_PRIV_MAKE_XML_BINARY, N_PRIV_XML_BINARY, TWO_OR_THREE, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

int ObExprPrivXmlBinary::calc_result_typeN(ObExprResType& type,
                                           ObExprResType* types_stack,
                                           int64_t param_num,
                                           ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  // 1st param is schema id for validation from column definition
  //   schema id in xml data maybe 0 or different from column definition
  // 2nd param is xmltype data (udt sql type), or varchar(should parse again)
  if (param_num != 2) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param number", K(param_num));
  } else if (!ob_is_oracle_numeric_type(types_stack[0].get_type())) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("invalid type", K(ret), K(types_stack[0].get_type()));
  } else if (types_stack[1].is_ext() && types_stack[1].get_udt_id() == T_OBJ_XML) {
    types_stack[1].get_calc_meta().set_sql_udt(ObXMLSqlType);
  } else if (!types_stack[1].is_xml_sql_type()
             && !types_stack[1].is_character_type() // remove later
             && !types_stack[1].is_null()) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("invalid type", K(ret), K(types_stack[1].get_type()));
  }
  if (OB_SUCC(ret)) {
    if (types_stack[1].is_character_type() && types_stack[1].get_charset_type() != CHARSET_UTF8MB4) {
      types_stack[1].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN); // remove later
    }
    type.set_type(ObLongTextType);
    type.set_collation_type(CS_TYPE_BINARY);
    type.set_collation_level(CS_LEVEL_IMPLICIT);
    type.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY[ObLongTextType]);
  }

  return ret;
}

#ifdef OB_BUILD_ORACLE_XML
int ObExprPrivXmlBinary::eval_priv_xml_binary(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *schema_id = NULL;
  ObDatum *xml_datum = NULL;
  ObObjParam old_expr_type;
  if (expr.args_[1]->type_ == T_QUESTIONMARK) {
    const ParamStore &param_store = ctx.exec_ctx_.get_physical_plan_ctx()->get_param_store();
    int64_t param_idx = expr.args_[1]->extra_;
    if (param_idx < 0 || param_idx >= param_store.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param_idx is invalid", K(ret), K(param_idx));
    } else {
      old_expr_type = param_store.at(param_idx);
      if (ObLongTextType == old_expr_type.get_type()) {
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
        LOG_WARN("old_expr_type invalid ObLongTextType type", K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, schema_id))) {
    LOG_WARN("eval geo arg failed", K(ret));
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, xml_datum))) {
    LOG_WARN("eval sird arg failed", K(ret));
  } else if (xml_datum->is_null()) {
    res.set_null();
  } else {
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
    ObString xml_plain_text = xml_datum->get_string();
    ObIMulModeBase *xml_root = NULL;
    ObStringBuffer jbuf(&tmp_allocator);
    ObCollationType cs_type = expr.args_[1]->obj_meta_.is_character_type()
                              ? expr.args_[1]->obj_meta_.get_collation_type()
                              : CS_TYPE_BINARY;
    ObObjType in_type = expr.args_[1]->obj_meta_.is_character_type()
                        ? expr.args_[1]->obj_meta_.get_type()
                        : ObLongTextType;
    ObMulModeMemCtx* mem_ctx = nullptr;
    lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(MTL_ID(), "XMLModule"));
    if (OB_FAIL(ObXmlUtil::create_mulmode_tree_context(&tmp_allocator, mem_ctx))) {
      LOG_WARN("fail to create tree memory context", K(ret));
    } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(&tmp_allocator,
                                                                  in_type,
                                                                  cs_type,
                                                                  true,
                                                                  xml_plain_text))) {
      LOG_WARN("get xml plain text failed", K(ret), K(xml_datum));
    } else if (xml_plain_text.empty()) {
      res.set_null();
    } else if (expr.args_[1]->obj_meta_.is_character_type()) {
      // remove later, use xmlparse in rewrite, input arg can be restricted to xmltype
      ObXmlDocument *xml_doc = NULL;
      if (OB_FAIL(ObXmlParserUtils::parse_document_text(mem_ctx, xml_plain_text, xml_doc))) {
        LOG_WARN("parse xml plain text as document failed.", K(ret), K(xml_plain_text));
        ret = OB_ERR_XML_PARSE;
        LOG_USER_ERROR(OB_ERR_XML_PARSE);
      } else if (OB_ISNULL(xml_doc)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null xml document.", K(ret));
      } else {
        xml_root = static_cast<ObIMulModeBase *>(xml_doc);
        ObString res_string;
        if (!xml_doc->get_encoding().empty() || xml_doc->get_encoding_flag()) {
          xml_doc->set_encoding(ObXmlUtil::get_charset_name(ObCollationType::CS_TYPE_UTF8MB4_BIN));
        }
        if (OB_FAIL(xml_root->get_raw_binary(res_string, &tmp_allocator))) {
          LOG_WARN("failed to get xml binary", K(ret), K(xml_plain_text));
        } else {
          ObTextStringDatumResult str_result(expr.datum_meta_.type_, &expr, &ctx, &res);
          if (OB_FAIL(str_result.init(res_string.length()))) {
            LOG_WARN("init lob result failed");
          } else if (OB_FAIL(str_result.append(res_string.ptr(), res_string.length()))) {
            LOG_WARN("append lob result failed");
          } else {
            str_result.set_result();
          }
        }
      }
    } else { // must be xmlsql type
      // Todo: xml schema validation
      ObMulModeNodeType type = M_NULL;
      if (OB_FAIL(ObXmlUtil::xml_bin_type(xml_plain_text, type))) {
      } else if (type == M_UNPARESED_DOC) {
        ObString res_string;
        if (OB_FAIL(common::ObMulModeFactory::get_xml_base(mem_ctx, xml_plain_text,
                                                            ObNodeMemType::BINARY_TYPE,
                                                            ObNodeMemType::BINARY_TYPE,
                                                            xml_root))) {
          LOG_WARN("get xml base failed", K(ret));
        } else if (OB_FAIL(xml_root->get_raw_binary(res_string, mem_ctx->allocator_))) {
          LOG_WARN("get raw binary failed", K(ret));
        } else {
          ObTextStringDatumResult str_result(expr.datum_meta_.type_, &expr, &ctx, &res);
          if (OB_FAIL(str_result.init(res_string.length()))) {
            LOG_WARN("init lob result failed");
          } else if (OB_FAIL(str_result.append(res_string.ptr(), res_string.length()))) {
            LOG_WARN("append lob result failed");
          } else {
            str_result.set_result();
          }
        }
      } else if (type != M_DOCUMENT && type != M_UNPARESED_DOC) {
        ret = OB_XML_INSERT_FRAGMENT;
        LOG_WARN("can only insert xml document", K(ret), K(type));
      } else {
        res.set_string(xml_datum->get_string());
      }
    }
  }

  return ret;
}
#endif

int ObExprPrivXmlBinary::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSEDx(expr_cg_ctx, raw_expr);
  rt_expr.eval_func_ = eval_priv_xml_binary;

  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase