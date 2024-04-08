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
 * This file is for implement of func xml expr helper
 */

#define USING_LOG_PREFIX SQL_ENG
#include "lib/ob_errno.h"
#include "sql/engine/expr/ob_expr_xml_func_helper.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#ifdef OB_BUILD_ORACLE_XML
#include "lib/xml/ob_xpath.h"
#include "lib/xml/ob_xml_bin.h"
#include "lib/xml/ob_xml_util.h"
#include "lib/xml/ob_xml_parser.h"
#endif // OB_BUILD_ORACLE_XML
#include "sql/engine/expr/ob_expr_sql_udt_utils.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/ob_result_set.h"
#include "sql/ob_spi.h"
#ifdef OB_BUILD_ORACLE_PL
#include "pl/sys_package/ob_sdo_geometry.h"
#endif

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
#ifdef OB_BUILD_ORACLE_XML
uint64_t ObXMLExprHelper::get_tenant_id(ObSQLSessionInfo *session)
{
  uint64_t tenant_id = 0;
  if (OB_ISNULL(session)) {
  } else if (session->get_ddl_info().is_ddl_check_default_value()) {
    tenant_id = OB_SERVER_TENANT_ID;
  } else {
    tenant_id = session->get_effective_tenant_id();
  }
  return tenant_id;
}

int ObXMLExprHelper::add_binary_to_element(ObMulModeMemCtx* mem_ctx, ObString binary_value, ObXmlElement &element)
{
  INIT_SUCC(ret);
  ObXmlDocument *xml_doc = NULL;
  ObIMulModeBase *node = NULL;
  ObXmlDocument *doc_node = NULL;
  ObMulModeNodeType node_type = M_MAX_TYPE;
  bool is_unparsed = false;
  if (binary_value.empty()) {
    if (OB_ISNULL(xml_doc = OB_NEWx(ObXmlDocument, (mem_ctx->allocator_),
                                    ObMulModeNodeType::M_CONTENT,
                                    (mem_ctx)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to create an empty xml content node", K(ret), K(binary_value));
    } else {
      node = xml_doc;
    }
  } else if (OB_FAIL(ObXmlUtil::xml_bin_type(binary_value, node_type))) {
    LOG_WARN("xml bin type failed", K(ret));
  } else if (node_type == M_UNPARSED) {
    ObStringBuffer* buffer = nullptr;
    ObXmlDocument *x_doc = nullptr;
    ObString xml_text;
    ObXmlBin bin(binary_value, mem_ctx);
    if (OB_ISNULL(buffer = OB_NEWx(ObStringBuffer, mem_ctx->allocator_, (mem_ctx->allocator_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate buffer", K(ret));
    } else if (OB_FAIL(bin.parse())) {
      LOG_WARN("fail to parse binary.", K(ret));
    } else if (OB_FAIL(bin.print_xml(*buffer, ObXmlFormatType::NO_FORMAT, 0, 0))) {
      LOG_WARN("fail to print xml", K(ret));
    } else if (FALSE_IT(xml_text.assign_ptr(buffer->ptr(), buffer->length()))) {
    } else if (OB_FAIL(ObXmlParserUtils::parse_content_text(mem_ctx, xml_text, x_doc))) {
      LOG_DEBUG("fail to parse unparse", K(ret));
      ret = OB_SUCCESS;
      if (OB_FAIL(bin.to_tree(node))) {
        LOG_WARN("fail to tree", K(ret));
      }
      is_unparsed = true;
    } else {
      node = x_doc;
    }
  } else if (OB_FAIL(ObMulModeFactory::get_xml_base(mem_ctx, binary_value,
                                                    ObNodeMemType::BINARY_TYPE,
                                                    ObNodeMemType::TREE_TYPE,
                                                    node))) {
    LOG_WARN("fail to get xml base", K(ret), K(binary_value), K(node_type));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(node)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("node cast to xmldocument failed", K(ret), K(node));
  } else if (ObMulModeNodeType::M_DOCUMENT == node->type() ||
              ObMulModeNodeType::M_CONTENT == node->type() ||
              ObMulModeNodeType::M_UNPARSED == node->type()) {
    if (OB_ISNULL(doc_node = static_cast<ObXmlDocument*>(node))) {
      ret = OB_BAD_NULL_ERROR;
      LOG_WARN("node cast to xmldocument failed", K(ret), K(node));
    }
    for (int32_t j = 0; OB_SUCC(ret) && j < doc_node->size(); j++) {
      ObXmlNode *xml_node = doc_node->at(j);
      if (OB_ISNULL(xml_node)) {
        ret = OB_BAD_NULL_ERROR;
        LOG_WARN("doc node get null", K(ret), K(j));
      } else if (OB_XML_TYPE != xml_node->data_type()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("xml node date type unexpect", K(ret), K(j), K(xml_node->data_type()));
      } else if (OB_FAIL(element.add_element(xml_node))) {
        LOG_WARN("add element failed", K(ret), K(j), K(xml_node));
      }
    }
    if (OB_SUCC(ret) && is_unparsed) {
      // if the binary is unparsed and parsed fail, set the element is unparsed
      element.set_unparse(1);
    }
  }
  return ret;
}

int ObXMLExprHelper::get_xml_base(ObMulModeMemCtx *ctx,
                                  ObDatum *xml_datum,
                                  ObCollationType cs_type,
                                  ObNodeMemType expect_type,
                                  ObIMulModeBase *&node)
{
  ObMulModeNodeType node_type = M_MAX_TYPE;
  return get_xml_base(ctx, xml_datum, cs_type, expect_type, node, node_type, false);
}
int ObXMLExprHelper::get_xml_base(ObMulModeMemCtx *ctx,
                                  ObDatum *xml_datum,
                                  ObCollationType cs_type,
                                  ObNodeMemType expect_type,
                                  ObIMulModeBase *&node,
                                  ObMulModeNodeType &node_type,
                                  bool is_reparse)
{
  int ret = OB_SUCCESS;
  // temporary use until xml binary ready
  ObString xml_text;
  ObDatumMeta xml_meta;
  xml_meta.type_ = ObLongTextType;
  xml_meta.cs_type_ = CS_TYPE_UTF8MB4_BIN;
  ObXmlDocument *xml_doc = NULL;
  if (OB_ISNULL(xml_datum)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("xml datum is NULL", K(ret));
  } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(*ctx->allocator_, *xml_datum, xml_meta, true, xml_text))) {
    LOG_WARN("fail to get real data.", K(ret), K(xml_text));
  } else if (xml_text.empty()) {
    // create an empty xml node
    if (OB_ISNULL(xml_doc = OB_NEWx(ObXmlDocument, (ctx->allocator_), ObMulModeNodeType::M_CONTENT, ctx))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to create an empty xml content node", K(ret), K(xml_text));
    } else {
      node = xml_doc;
    }
  } else if (OB_FAIL(ObXmlUtil::xml_bin_type(xml_text, node_type))) {
    LOG_WARN("failed to get bin header.", K(ret));
  } else if (is_reparse) {
    ObStringBuffer *buff = nullptr;
    ParamPrint param_list;
    if (OB_ISNULL(buff = OB_NEWx(ObStringBuffer, ctx->allocator_, (ctx->allocator_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("create obstrinbuffer failed", K(ret));
    } else if (OB_FAIL(ObMulModeFactory::get_xml_base(ctx, xml_text,
                                                       ObNodeMemType::BINARY_TYPE,
                                                       ObNodeMemType::BINARY_TYPE,
                                                       node))) {
      LOG_WARN("fail to get xml base", K(ret));
    } else if (node_type == M_UNPARESED_DOC && OB_FALSE_IT(node_type = M_DOCUMENT)) {
    } else if (node_type == M_UNPARSED) {
    } else if (node_type == M_DOCUMENT && OB_FAIL(node->print_document(*buff, CS_TYPE_INVALID, ObXmlFormatType::NO_FORMAT, 0))) {
      LOG_WARN("failed to convert xml binary to xml text", K(ret));
    } else if (node_type == M_CONTENT && OB_FAIL(node->print_content(*buff, false, false, ObXmlFormatType::NO_FORMAT, param_list))) {
      LOG_WARN("failed to convert xml binary to xml text", K(ret));
    } else {
      xml_text.assign_ptr(buff->ptr(), buff->length());
      if (node_type != ObMulModeNodeType::M_DOCUMENT && OB_FAIL(ObXmlParserUtils::parse_content_text(ctx, xml_text, xml_doc))) {
        LOG_WARN("fail to get xml content tree", K(ret), K(node_type));
      } else if (node_type == ObMulModeNodeType::M_DOCUMENT && OB_FAIL(ObXmlParserUtils::parse_document_text(ctx, xml_text, xml_doc))) {
        LOG_WARN("fail to get xml tree", K(ret), K(node_type));
      } else {
        xml_doc->set_xml_type(node_type);
        node = xml_doc;
      }
    }
  } else if (OB_FAIL(ObMulModeFactory::get_xml_base(ctx, xml_text,
                                                    ObNodeMemType::BINARY_TYPE,
                                                    expect_type,
                                                    node))) {
    LOG_WARN("fail to get xml base", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (node->get_unparse() || node->type() == M_UNPARSED) { // unparse try to pasre
    if (OB_FAIL(try_to_parse_unparse_binary(ctx, cs_type, node, expect_type, node))) {
      LOG_WARN("fail to parse unparse binary", K(ret), K(xml_text));
    }
  }

  return ret;
}

int ObXMLExprHelper::try_to_parse_unparse_binary(ObMulModeMemCtx* mem_ctx,
                                                 ObCollationType cs_type,
                                                 ObIMulModeBase *input_node,
                                                 ObNodeMemType expect_type,
                                                 ObIMulModeBase *&res_node)
{
  int ret = OB_SUCCESS;
  // serialzie unparese string
  ObString unparse_str;
  ObXmlDocument *xml_doc = nullptr;
  ObStringBuffer *buff = nullptr;

  if (OB_ISNULL(input_node) || !(input_node->type() == M_DOCUMENT || input_node->type() == M_UNPARSED)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unparse node is NULL or not M_DOCUMENT", K(input_node));
  } else if (OB_ISNULL(buff = OB_NEWx(ObStringBuffer, mem_ctx->allocator_, (mem_ctx->allocator_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("create obstrinbuffer failed", K(input_node));
  } else {
    if (OB_FAIL(input_node->print_document(*buff, cs_type, ObXmlFormatType::NO_FORMAT, 0))) {
      LOG_WARN("fail to serialize unparse string", K(ret));
    } else {
      unparse_str.assign_ptr(buff->ptr(), buff->length());
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObXmlParserUtils::parse_document_text(mem_ctx, unparse_str, xml_doc))) {
    if (ret == OB_ERR_PARSER_SYNTAX) {
      if (OB_FAIL(ObXmlParserUtils::parse_content_text(mem_ctx, unparse_str, xml_doc))) {
        if (ret == OB_ERR_PARSER_SYNTAX) {
          ret = OB_ERR_XML_PARSE;
          LOG_USER_ERROR(OB_ERR_XML_PARSE);
        }
        LOG_WARN("fail to parse xml doc", K(unparse_str), K(ret));
      }
    }
    LOG_WARN("fail to parse xml doc", K(unparse_str), K(ret));
  }

  if (OB_SUCC(ret)) {
    if (expect_type == BINARY_TYPE) {
      ObXmlBin* bin = nullptr;
      if (OB_ISNULL(bin = OB_NEWx(ObXmlBin, mem_ctx->allocator_, (mem_ctx)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc binary", K(ret));
      } else if (OB_FAIL(bin->parse_tree(xml_doc))) {
        LOG_WARN("fail to serialize tree.", K(ret));
      } else {
        res_node = bin;
      }
    } else {
      res_node = xml_doc;
    }
  }

  return ret;
}

int ObXMLExprHelper::pack_xml_res(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res, ObXmlDocument* doc,
                                  ObMulModeMemCtx* mem_ctx, ObMulModeNodeType node_type,
                                  ObString &plain_text)
{
  int ret = OB_SUCCESS;
  ObString blob_locator;
  ObExprStrResAlloc expr_res_alloc(expr, ctx);
  ObTextStringResult blob_res(ObLongTextType, true, &expr_res_alloc);
  ObString binary_str;
  ObIMulModeBase *xml_base;
  if (OB_ISNULL(doc)) {
    if (plain_text.length() == 0) {
      res.set_string(NULL, 0);
    } else if (OB_FAIL(ObMulModeFactory::get_xml_base(mem_ctx, plain_text,
                ObNodeMemType::TREE_TYPE, ObNodeMemType::BINARY_TYPE, xml_base,
                node_type))) {
      LOG_WARN("get xml base failed", K(ret), K(node_type));
    } else if (OB_FAIL(xml_base->get_raw_binary(binary_str, mem_ctx->allocator_))) {
      LOG_WARN("get raw binary failed", K(ret));
    }
  } else if (FALSE_IT(doc->set_xml_type(node_type))) {
  } else if (OB_FAIL(doc->get_raw_binary(binary_str, mem_ctx->allocator_))) {
    LOG_WARN("get raw binary failed", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(pack_binary_res(expr, ctx, binary_str, blob_locator))) {
    LOG_WARN("pack binary res failed", K(ret));
  } else {
    res.set_string(blob_locator.ptr(), blob_locator.length());
  }
  return ret;
}

int ObXMLExprHelper::pack_binary_res(const ObExpr &expr, ObEvalCtx &ctx, ObString binary_str, ObString &blob_locator)
{
  INIT_SUCC(ret);
  ObExprStrResAlloc expr_res_alloc(expr, ctx);
  ObTextStringResult blob_res(ObLongTextType, true, &expr_res_alloc);
  int64_t total_length = binary_str.length();
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(blob_res.init(total_length))) {
    LOG_WARN("failed to init blob res", K(ret), K(blob_res), K(total_length));
  } else if (OB_FAIL(blob_res.append(binary_str))) {
    LOG_WARN("failed to append xml binary data", K(ret), K(blob_res));
  } else {
    blob_res.get_result_buffer(blob_locator);
  }
  return ret;
}

int ObXMLExprHelper::parse_namespace_str(ObString &ns_str, ObString &prefix, ObString &uri)
{
  int ret = OB_SUCCESS;
  const char *str = ns_str.ptr();
  int64_t str_len = ns_str.length();
  int64_t idx = 0;
  const char *prefix_start = NULL;
  int64_t prefix_len = 0;
  const char *uri_start = NULL;
  int64_t uri_len = 0;
  if (idx + 4 < str_len &&
      str[idx] == 'x' &&
      str[idx+1] == 'm' &&
      str[idx+2] == 'l' &&
      str[idx+3] == 'n' &&
      str[idx+4] == 's') {
    idx += 5;
    if (str[idx] == ':') {
      // parse prefix name
      int64_t start = idx + 1;
      while (idx < str_len && str[idx] != '=') ++idx;
      if (idx < str_len && str[idx] == '=') {
        prefix_start = str + start;
        prefix_len = idx - start;
      }
    }
    if (idx < str_len && str[idx] == '=') {
      // parse uri value
      idx += 1;
      if (idx < str_len && str[idx] == '"') {
        // "xxx"
        int start = ++idx;
        while(idx < str_len && str[idx] != '"') ++idx;
        if (idx < str_len && str[idx] == '"') {
          uri_start = str + start;
          uri_len = idx - start;
          idx += 1;
        } else {
          ret = OB_ERR_INVALID_XPATH_EXPRESSION;
          LOG_WARN("not invalid xml namespace string", K(ret), K(ns_str), K(idx));
        }
      } else {
        ret = OB_ERR_INVALID_XPATH_EXPRESSION;
        LOG_WARN("not invalid xml namespace string", K(ret), K(ns_str), K(idx));
      }
    } else {
      ret = OB_ERR_INVALID_XPATH_EXPRESSION;
      LOG_WARN("not invalid xml namespace string", K(ret), K(ns_str), K(idx));
    }
  } else {
    ret = OB_ERR_INVALID_XPATH_EXPRESSION;
    LOG_WARN("not invalid xml namespace string", K(ret), K(ns_str), K(idx));
  }

  if (OB_SUCC(ret)) {
    if (prefix_len > 0) {
      prefix.assign_ptr(prefix_start, prefix_len);
    }
    uri.assign_ptr(uri_start, uri_len);
  }
  return ret;
}

int ObXMLExprHelper::construct_namespace_params(ObString &namespace_str,
                                                ObString &default_ns,
                                                ObPathVarObject &prefix_ns,
                                                ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObString, 8> namespace_arr;
  ObString trim_str = namespace_str.trim_space_only();
  if (trim_str.empty()) {
    /* nothing */
  } else if (OB_FAIL(split_on(trim_str, ' ', namespace_arr))) {
    LOG_WARN("fail to split on trim string", K(ret), K(trim_str));
  } else {
    for (int64_t i = 0; i < namespace_arr.count() && OB_SUCC(ret); i++) {
      ObString prefix;
      ObString uri;
      if (OB_FAIL(parse_namespace_str(namespace_arr.at(i), prefix, uri))) {
        LOG_WARN("fail to parse namespace str", K(ret), K(i), K(namespace_arr.at(i)));
      } else if (prefix.empty()) {
        default_ns = uri;
      } else {
        if (uri.empty()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("uri is not allow to empty", K(ret), K(prefix));
        } else if (OB_FAIL(add_ns_to_container_node(prefix_ns, prefix, uri, allocator))) {
          LOG_WARN("fail to add prefix namespace node", K(ret), K(prefix), K(uri));
        }
      }
    } // end for
  }
  return ret;
}

int ObXMLExprHelper::add_ns_to_container_node(ObPathVarObject &container,
                                              ObString &prefix,
                                              ObString &uri,
                                              ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObDatum *datum;
  if (OB_ISNULL(datum = OB_NEWx(ObDatum, (&allocator)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to alloc datum", K(ret));
  } else if (FALSE_IT(datum->set_string(uri))) {
  } else if (OB_FAIL(container.add(prefix, datum))) {
    LOG_WARN("fail to add prefix namespace to ObPathVarObject", K(ret), K(*datum));
  }
  return ret;
}

int ObXMLExprHelper::set_string_result(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res, ObString &res_str)
{
  int ret = OB_SUCCESS;
  ObTextStringDatumResult text_result(expr.datum_meta_.type_, &expr, &ctx, &res);
  int64_t res_len = res_str.length();
  if (OB_FAIL(text_result.init(res_len))) {
    LOG_WARN("fail to init string result length", K(ret), K(text_result), K(res_len));
  } else if (OB_FAIL(text_result.append(res_str))) {
    LOG_WARN("fail to append xml format string", K(ret), K(res_str), K(text_result));
  } else {
    text_result.set_result();
  }
  return ret;
}

int ObXMLExprHelper::get_str_from_expr(const ObExpr *expr,
                                       ObEvalCtx &ctx,
                                       ObString &res,
                                       ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObDatum *datum = NULL;
  ObObjType val_type = expr->datum_meta_.type_;
  uint16_t sub_schema_id = expr->obj_meta_.get_subschema_id();
  if (OB_FAIL(expr->eval(ctx, datum))) {
    LOG_WARN("eval xml arg failed", K(ret));
  } else if (!ob_is_string_type(val_type)) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("input type error", K(val_type));
  } else if (FALSE_IT(res = datum->get_string())) {
  } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(allocator, *datum,
                expr->datum_meta_, expr->obj_meta_.has_lob_header(), res))) {
    LOG_WARN("fail to get real data.", K(ret), K(res));
  }
  return ret;
}

int ObXMLExprHelper::parse_xml_str(ObMulModeMemCtx *ctx, const ObString &xml_text, ObXmlDocument *&xml_doc)
{
  int ret = OB_SUCCESS;
  if(ObXmlParserUtils::has_xml_decl(xml_text)) {
    if (OB_FAIL(ObXmlParserUtils::parse_document_text(ctx, xml_text, xml_doc))) {
      if (ret == OB_ERR_PARSER_SYNTAX) {
        ret = OB_ERR_XML_PARSE;
        LOG_USER_ERROR(OB_ERR_XML_PARSE);
      }
      LOG_WARN("fail to parse xml doc", K(xml_text), K(ret));
    }
  } else if(OB_FAIL(ObXmlParserUtils::parse_content_text(ctx, xml_text, xml_doc))) {
    if (ret == OB_ERR_PARSER_SYNTAX) {
      ret = OB_ERR_XML_PARSE;
      LOG_USER_ERROR(OB_ERR_XML_PARSE);
    }
    LOG_WARN("fail to parse xml doc", K(xml_text), K(ret));
  }
  return ret;
}

int ObXMLExprHelper::get_xmltype_from_expr(const ObExpr *expr,
                                           ObEvalCtx &ctx,
                                           ObDatum *&xml_datum)
{
  int ret = OB_SUCCESS;
  ObObjType val_type = expr->datum_meta_.type_;
  uint16_t sub_schema_id = expr->obj_meta_.get_subschema_id();
  if (!ob_is_xml_sql_type(val_type, sub_schema_id)) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, "-", "-");
    LOG_WARN("inconsistent datatypes", K(ret), K(ob_obj_type_str(val_type)));
  } else if (OB_FAIL(expr->eval(ctx, xml_datum))) {
    LOG_WARN("eval xml arg failed", K(ret));
  }
  return ret;
}

bool ObXMLExprHelper::is_xml_leaf_node(ObMulModeNodeType node_type)
{
  return node_type == ObMulModeNodeType::M_ATTRIBUTE ||
         node_type == ObMulModeNodeType::M_NAMESPACE ||
         node_type == ObMulModeNodeType::M_CDATA ||
         node_type == ObMulModeNodeType::M_TEXT;
}

bool ObXMLExprHelper::is_xml_text_node(ObMulModeNodeType node_type)
{
  return node_type == ObMulModeNodeType::M_CDATA ||
         node_type == ObMulModeNodeType::M_TEXT;
}

bool ObXMLExprHelper::is_xml_attribute_node(ObMulModeNodeType node_type)
{
  return node_type == ObMulModeNodeType::M_ATTRIBUTE ||
         node_type == ObMulModeNodeType::M_NAMESPACE;
}

bool ObXMLExprHelper::is_xml_element_node(ObMulModeNodeType node_type)
{
  return node_type == ObMulModeNodeType::M_ELEMENT ||
         node_type == ObMulModeNodeType::M_DOCUMENT ||
         node_type == ObMulModeNodeType::M_CONTENT;
}

bool ObXMLExprHelper::is_xml_root_node(ObMulModeNodeType node_type)
{
  return node_type == ObMulModeNodeType::M_DOCUMENT ||
         node_type == ObMulModeNodeType::M_CONTENT;
}

void ObXMLExprHelper::replace_xpath_ret_code(int &ret)
{
  if (ret == OB_OP_NOT_ALLOW) {
    ret = OB_XPATH_EXPRESSION_UNSUPPORTED;
  } else if (ret == OB_ERR_PARSER_SYNTAX) {
    ret = OB_ERR_XML_PARSE;
  } else if (ret == OB_ALLOCATE_MEMORY_FAILED) {
    // do nothing
  } else if (ret == OB_ERR_WRONG_VALUE) {
    ret = OB_ERR_INVALID_INPUT;
  } else {
    ret = OB_ERR_INVALID_XPATH_EXPRESSION;
  }
}

int ObXMLExprHelper::check_xml_document_unparsed(ObMulModeMemCtx* mem_ctx, ObString binary_str, bool &validity)
{
  INIT_SUCC(ret);
  ObMulModeNodeType node_type = M_MAX_TYPE;
  if (OB_FAIL(ObXmlUtil::xml_bin_type(binary_str, node_type))) {
    LOG_WARN("get xml bin type failed", K(ret));
  } else {
    ObXmlParser parser(mem_ctx);
    parser.set_only_syntax_check();
    if (node_type == M_UNPARESED_DOC && OB_FAIL(parser.parse_document(binary_str))) {
      ret = OB_SUCCESS;
      validity = false;
    } else {
      validity = true;
    }
  }
  return ret;
}

int ObXMLExprHelper::parse_xml_document_unparsed(ObMulModeMemCtx* mem_ctx, ObString binary_str, ObString &res_str, ObXmlDocument* &res_doc)
{
  INIT_SUCC(ret);
  ObMulModeNodeType node_type = M_MAX_TYPE;
  ObXmlDocument* doc = nullptr;
  ObXmlBin bin(mem_ctx);
  ObIMulModeBase* tree = nullptr;
  ObXmlParser parser(mem_ctx);
  ObStringBuffer buff(mem_ctx->allocator_);
  if (OB_FAIL(ObXmlUtil::xml_bin_type(binary_str, node_type))) {
    LOG_WARN("get xml bin type failed", K(ret));
  } else if (node_type == ObMulModeNodeType::M_UNPARESED_DOC) {
    if (OB_FAIL(ObXmlParserUtils::parse_document_text(mem_ctx, binary_str, doc))) {
      LOG_WARN("parse document text failed", K(ret));
    } else if (FALSE_IT(tree = doc)) {
    } else if (OB_FAIL(bin.parse_tree(tree))) {
      LOG_WARN("parse tree failed", K(ret));
    } else if (OB_FAIL(bin.print_document(buff, CS_TYPE_UTF8MB4_GENERAL_CI, ObXmlFormatType::NO_FORMAT))) {
      LOG_WARN("print document failed");
    } else if (OB_FAIL(parser.parse_document(buff.string()))) {
      ret = OB_ERR_XML_PARSE;
      LOG_USER_ERROR(OB_ERR_XML_PARSE);
      LOG_WARN("parse xml plain text as document failed.", K(ret));
    } else {
      res_str = buff.string();
      res_doc = parser.document();
    }
  }
  return ret;
}

int ObXMLExprHelper::content_unparsed_binary_check_doc(ObMulModeMemCtx* mem_ctx, ObString binary_str, ObString &res_str) {
  INIT_SUCC(ret);
  ObMulModeNodeType node_type = M_MAX_TYPE;
  ObXmlDocument* doc = nullptr;
  ObXmlBin bin(mem_ctx);
  ObIMulModeBase* tree = nullptr;
  ObXmlParser parser(mem_ctx);
  ObStringBuffer buff(mem_ctx->allocator_);
  ObXmlDocument* new_doc = nullptr;
  ParamPrint param_list; // unused
  if (OB_FAIL(ObXmlUtil::xml_bin_type(binary_str, node_type))) {
    LOG_WARN("get xml bin type failed", K(ret));
  } else if (node_type == ObMulModeNodeType::M_UNPARSED) {
    if (OB_FAIL(ObXmlParserUtils::parse_content_text(mem_ctx, binary_str, doc))) {
      LOG_WARN("parse document text failed", K(ret));
    } else if (FALSE_IT(tree = doc)) {
    } else if (OB_FAIL(bin.parse_tree(tree))) {
      LOG_WARN("parse tree failed", K(ret));
    } else if (OB_FAIL(bin.print_content(buff, false, false, ObXmlFormatType::NO_FORMAT, param_list))) {
      LOG_WARN("print document failed");
    } else if (OB_FAIL(parser.parse_document(buff.string()))) {
      // try to parse content intto document, if it fails, leave the content unchanged
      ret = OB_SUCCESS;
      res_str = binary_str;
    } else if (FALSE_IT(new_doc = parser.document())) {
    } else if (OB_FAIL(new_doc->get_raw_binary(res_str, mem_ctx->allocator_))) {
      LOG_WARN("get raw binary failed", K(ret));
    }
  }
  return ret;
}

int ObXMLExprHelper::check_element_validity(ObMulModeMemCtx* mem_ctx, ObXmlElement *in_ele, ObXmlElement *&out_ele, bool &validity) {
  INIT_SUCC(ret);
  ObXmlParser parser(mem_ctx);
  ObStringBuffer buff(mem_ctx->allocator_);
  if (OB_ISNULL(in_ele)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("in_ele is NULL", K(ret));
  } else {
    in_ele->set_unparse(1);
    if (OB_FAIL(in_ele->print_element(buff, 0, ObXmlFormatType::NO_FORMAT))) {
      LOG_WARN("print document failed");
    } else if (OB_FAIL(parser.parse_document(buff.string()))) {
      ret = OB_SUCCESS;
      validity = false;
    } else {
      validity = true;
      in_ele->set_unparse(0);
      ObXmlDocument* doc = parser.document();
      if (OB_NOT_NULL(doc) && doc->size() > 0){
        out_ele = static_cast<ObXmlElement *>(doc->at(0));
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get valid xml element", K(ret));
      }
    }
  }
  return ret;
}

int ObXMLExprHelper::check_doc_validity(ObMulModeMemCtx* mem_ctx, ObXmlDocument *&doc, bool &validity) {
  INIT_SUCC(ret);
  ObXmlParser parser(mem_ctx);
  ObStringBuffer buff(mem_ctx->allocator_);
  if (OB_FAIL(doc->print_document(buff, CS_TYPE_UTF8MB4_GENERAL_CI, ObXmlFormatType::NO_FORMAT))) {
    LOG_WARN("print document failed");
  } else if (OB_FAIL(parser.parse_document(buff.string()))) {
    ret = OB_SUCCESS;
    validity = false;
  } else if (OB_ISNULL(doc = parser.document())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parser document return NULL", K(ret));
  } else {
    validity = true;
  }
  return ret;
}
#endif // OB_BUILD_ORACLE_XML
// not all udts sql types based on lobs, so not handle xml in process_lob_locator_results
int ObXMLExprHelper::process_sql_udt_results(ObObj& value, sql::ObResultSet &result)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator *allocator = NULL;
  sql::ObSQLSessionInfo *session_info = &result.get_session();
  if (OB_FAIL(result.get_exec_context().get_convert_charset_allocator(allocator))) {
    LOG_WARN("fail to get convert charset allocator", K(ret));
  } else if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob fake allocator is null.", K(ret), K(value));
  } else if (OB_FAIL(process_sql_udt_results(value,
                                             allocator,
                                             &result.get_session(),
                                             &result.get_exec_context(),
                                             result.is_ps_protocol()))) {
    LOG_WARN("convert udt to client format failed.", K(ret), K(value));
  } else { /* do nothing */ }
  return ret;
}

int ObXMLExprHelper::process_sql_udt_results(common::ObObj& value,
                                             common::ObIAllocator *allocator,
                                             sql::ObSQLSessionInfo *session_info,
                                             sql::ObExecContext *exec_context,
                                             bool is_ps_protocol,
                                             const ColumnsFieldIArray *fields,
                                             ObSchemaGetterGuard *schema_guard) // need fields and schema guard
{
  int ret = OB_SUCCESS;
  if (!value.is_user_defined_sql_type() && !value.is_collection_sql_type() && !value.is_geometry()) {
    ret = OB_NOT_SUPPORTED;
    OB_LOG(WARN, "not supported udt type", K(ret),
           K(value.get_type()), K(value.get_udt_subschema_id()));
#ifdef OB_BUILD_ORACLE_XML
  } else if (value.is_xml_sql_type()) {
    bool is_client_support_binary_xml = false; // client receive xml as json, like json
    if (value.is_null() || value.is_nop_value()) {
      // do nothing
    } else if (is_client_support_binary_xml) {
      // convert to udt client format
    } else {
      ObString data;
      ObLobLocatorV2 loc(value.get_string(), true);
      ObString converted_str;
      if (!loc.is_null()) {
        ObTextStringIter instr_iter(ObLongTextType, CS_TYPE_BINARY, value.get_string(), true);
        if (OB_FAIL(instr_iter.init(0, session_info, allocator))) {
          LOG_WARN("init lob str inter failed", K(ret), K(value));
        } else if (OB_FAIL(instr_iter.get_full_data(data))) {
          LOG_WARN("get xml full data failed", K(value));
        } else {
          ObMulModeNodeType node_type = M_MAX_TYPE;
          ObStringBuffer* jbuf = nullptr;
          ParamPrint param_list;
          param_list.indent = 2;
          ObIMulModeBase *node = nullptr;
          ObMulModeMemCtx* ctx = nullptr;

          if (OB_FAIL(ObXmlUtil::create_mulmode_tree_context(allocator, ctx))) {
            LOG_WARN("fail to create tree memory context", K(ret));
          } else if (OB_ISNULL(allocator)) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid input args", K(ret), KP(allocator));
          } else if (data.length() == 0) {
          } else if (OB_FAIL(ObXmlUtil::xml_bin_type(data, node_type))) {
            LOG_WARN("xml bin type failed", K(ret));
          } else if (OB_FAIL(ObMulModeFactory::get_xml_base(ctx, data,
                                                            ObNodeMemType::BINARY_TYPE,
                                                            ObNodeMemType::BINARY_TYPE,
                                                            node))) {
            LOG_WARN("fail to get xml base", K(ret), K(data.length()));
          }

          ObCollationType cs_type = session_info->get_local_collation_connection();

          if (OB_FAIL(ret) || data.length() == 0) {
          } else if (OB_ISNULL(jbuf = OB_NEWx(ObStringBuffer, allocator, (allocator)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to construct buffer", K(ret));
          } else if ((ObMulModeNodeType::M_DOCUMENT == node_type ||
                      ObMulModeNodeType::M_UNPARESED_DOC == node_type) // /Although it is judged here that node_type=M_UNPARESED_DOC, it can actually go here, and the label has been replaced with DOCUMENT
                      && OB_FAIL(node->print_document(*jbuf, cs_type, ObXmlFormatType::WITH_FORMAT))) {
            LOG_WARN("print document failed", K(ret));
          } else if ((ObMulModeNodeType::M_CONTENT == node_type ||
                      ObMulModeNodeType::M_UNPARSED == node_type) &&
                      OB_FAIL(node->print_content(*jbuf, false, false, ObXmlFormatType::WITH_FORMAT, param_list))) {
            LOG_WARN("print content failed", K(ret));
          } else {
            data.assign_ptr(jbuf->ptr(), jbuf->length());
          }

          if (OB_SUCC(ret) && cs_type != CS_TYPE_UTF8MB4_BIN) {
            if (OB_FAIL(ObCharset::charset_convert(*allocator, data, CS_TYPE_UTF8MB4_BIN,
                                                  cs_type, converted_str))) {
              LOG_WARN("charset convertion failed", K(ret), K(data));
            }
          } else {
            converted_str = data;
          }

          if (OB_SUCC(ret)) {
            value.set_udt_value(converted_str.ptr(), converted_str.length());
          }
        }
      }
    }
#endif // OB_BUILD_ORACLE_XML
  } else if (value.is_geometry()) {
    if (is_ps_protocol) {
#ifdef OB_BUILD_ORACLE_PL
      ObObj result;
      if (OB_ISNULL(exec_context)) {
        ret = OB_BAD_NULL_ERROR;
      } else if (OB_FAIL(pl::ObSdoGeometry::wkb_to_pl_extend(exec_context->get_allocator(), exec_context, value.get_string(), result))) {
        LOG_WARN("failed to get geometry wkb from pl extend", K(ret));
      } else {
        value = result;
      }
#else
      ret = OB_NOT_SUPPORTED;
#endif
    }
  } else {
    if (OB_ISNULL(exec_context)) {
      ret = OB_BAD_NULL_ERROR;
    } else if (OB_ISNULL(exec_context->get_physical_plan_ctx())) {
      // no physical plan, build new one
      if (OB_ISNULL(fields)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("no fields to rebuild udt meta", K(ret), K(lbt()));
      } else if (OB_FAIL(exec_context->create_physical_plan_ctx())) {
        LOG_WARN("failed to create physical plan ctx of subschema id", K(ret), K(lbt()));
      } else if (OB_FAIL(exec_context->get_physical_plan_ctx()->build_subschema_by_fields(fields, schema_guard))) {
        LOG_WARN("failed to rebuild_subschema by fields", K(ret), K(*fields), K(lbt()));
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      const uint16_t subschema_id = value.get_meta().get_subschema_id();
      ObSqlUDTMeta udt_meta;
      if (OB_ISNULL(exec_context->get_physical_plan_ctx())) {
        // build temp subschema id
      } else if (OB_FAIL(exec_context->get_sqludt_meta_by_subschema_id(subschema_id, udt_meta))) {
        LOG_WARN("failed to get udt meta", K(ret), K(subschema_id));
      }
      if (OB_FAIL(ret)) {
      } else if (!ObObjUDTUtil::ob_is_supported_sql_udt(udt_meta.udt_id_)) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not supported to get udt meta", K(ret), K(udt_meta.udt_id_));
      } else if (!is_ps_protocol) {
        ObSqlUDT sql_udt;
        sql_udt.set_udt_meta(udt_meta);
        ObString res_str;
        if (OB_FAIL(sql::ObSqlUdtUtils::convert_sql_udt_to_string(value, allocator, exec_context,
                                                                  sql_udt, res_str))) {
          LOG_WARN("failed to convert udt to string", K(ret), K(subschema_id));
        } else {
          value.set_udt_value(res_str.ptr(), res_str.length());
        }
      } else {
        ObString udt_data = value.get_string();
        ObObj result;
        if (OB_FAIL(ObSqlUdtUtils::cast_sql_record_to_pl_record(exec_context,
                                                                result,
                                                                udt_data,
                                                                udt_meta))) {
          LOG_WARN("failed to cast sql collection to pl collection", K(ret), K(udt_meta.udt_id_));
        } else {
          value = result;
        }
      }
    }
  }
  return ret;
}

} // sql
} // oceanbase