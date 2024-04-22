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
#include "lib/xml/ob_xpath.h"
#include "lib/xml/ob_xml_bin.h"
#include "lib/xml/ob_xml_util.h"
#include "lib/xml/ob_xml_parser.h"
#include "sql/engine/expr/ob_expr_sql_udt_utils.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/ob_result_set.h"
#include "sql/ob_spi.h"
#include "lib/xml/ob_binary_aggregate.h"
#ifdef OB_BUILD_ORACLE_PL
#include "pl/sys_package/ob_sdo_geometry.h"
#endif

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
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
    } else if (OB_FAIL(bin.print(*buffer, ObXmlFormatType::NO_FORMAT, 0, 0))) {
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

int ObXMLExprHelper::get_xml_base(ObMulModeMemCtx *mem_ctx,
                                  ObDatum *xml_datum,
                                  ObEvalCtx &ctx,
                                  ObIMulModeBase *&xml_doc,
                                  ObGetXmlBaseType base_flag)
{
  INIT_SUCC(ret);
  ObMulModeNodeType node_type = M_DOCUMENT;
  ObNodeMemType expect_type = ObNodeMemType::BINARY_TYPE;
  ObCollationType cs_type = CS_TYPE_UTF8MB4_BIN;

  if (OB_FAIL(ObXMLExprHelper::get_xml_base(mem_ctx, xml_datum, cs_type, expect_type, xml_doc, node_type, base_flag))) {
    LOG_WARN("fail to parse xml doc", K(ret));
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
  return get_xml_base(ctx, xml_datum, cs_type, expect_type, node, node_type, ObGetXmlBaseType::OB_MAX);
}
int ObXMLExprHelper::get_xml_base(ObMulModeMemCtx *ctx,
                                  ObDatum *xml_datum,
                                  ObCollationType cs_type,
                                  ObNodeMemType expect_type,
                                  ObIMulModeBase *&node,
                                  ObMulModeNodeType &node_type,
                                  ObGetXmlBaseType base_flag)
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
  } else if (ObGetXmlBaseType::OB_IS_REPARSE == base_flag) {
    ObStringBuffer *buff = nullptr;
    ParamPrint param_list;
    ObNsSortedVector* ns_vec_point = nullptr;
    ObNsSortedVector ns_vec;
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
    } else if (OB_FAIL(ObXmlUtil::init_print_ns(&(*ctx->allocator_), node, ns_vec, ns_vec_point))) {
      LOG_WARN("fail to init ns vector by extend area", K(ret));
    } else if (node_type == M_DOCUMENT && OB_FAIL(node->print_document(*buff, CS_TYPE_INVALID, ObXmlFormatType::NO_FORMAT, 0, ns_vec_point))) {
      LOG_WARN("failed to convert xml binary to xml text", K(ret));
    } else if (node_type == M_CONTENT && OB_FAIL(node->print_content(*buff, false, false, ObXmlFormatType::NO_FORMAT, param_list, ns_vec_point))) {
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
                                                    node,
                                                    M_DOCUMENT,
                                                    false,
                                                    ObGetXmlBaseType::OB_SHOULD_CHECK == base_flag))) {
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
  ObNsSortedVector* ns_vec_point = nullptr;
  ObNsSortedVector ns_vec;

  if (OB_ISNULL(input_node) || !(input_node->type() == M_DOCUMENT || input_node->type() == M_UNPARSED)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unparse node is NULL or not M_DOCUMENT", K(input_node));
  } else if (OB_ISNULL(buff = OB_NEWx(ObStringBuffer, mem_ctx->allocator_, (mem_ctx->allocator_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("create obstrinbuffer failed", K(input_node));
  } else {
    if (OB_FAIL(ObXmlUtil::init_print_ns(mem_ctx->allocator_, input_node, ns_vec, ns_vec_point))) {
      LOG_WARN("fail to init ns vector by extend area", K(ret));
    } else if (OB_FAIL(input_node->print_document(*buff, cs_type, ObXmlFormatType::NO_FORMAT, 0, ns_vec_point))) {
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
  } else {
    ret = OB_ERR_INVALID_XPATH_EXPRESSION;
    LOG_WARN("not invalid xmlns string", K(ret), K(ns_str));
  }
  // parse prefix name
  if (OB_FAIL(ret)) {
  } else if (idx < str_len && str[idx] == ':') {
    idx += 1;
    int64_t start = idx;
    // find prefix name end
    while (idx < str_len && str[idx] != '=') {
      idx += 1;
    }
    prefix_start = str + start;
    prefix_len = idx - start;
  }
  // parse uri value
  if (OB_FAIL(ret)) {
  } else if (idx < str_len && str[idx] == '=') {
    idx += 1;
    // skip the " in the front
    while (idx < str_len && str[idx] == '"') {
      idx += 1;
    }
    int64_t start = idx;
    // find the value end
    while (idx < str_len && str[idx] != '"') {
      idx += 1;
    }
    uri_start = str + start;
    uri_len = idx - start;
    // skip the " in the end
    while (idx < str_len && str[idx] == '"') {
      idx += 1;
    }
  } else if (idx != str_len) {
    ret = OB_ERR_INVALID_XPATH_EXPRESSION;
    LOG_WARN("no \"=\" after xmlns or prefix name", K(ret), K(ns_str));
  }

  if (OB_FAIL(ret)) {
  } else if (prefix_len > 0) {
    if (uri_len > 0) {
      uri.assign_ptr(uri_start, uri_len);
      prefix.assign_ptr(prefix_start, prefix_len);
    } else {
      ret = OB_ERR_INVALID_XPATH_EXPRESSION;
      LOG_WARN("empty value after prefix name", K(ret), K(ns_str));
    }
  } else if (uri_len > 0) {
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

int ObXMLExprHelper::construct_namespace_params(ObIArray<ObString> &namespace_arr,
                                                ObString &default_ns,
                                                void *&prefix_ns,
                                                ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObPathVarObject *prefix_ns_ = NULL;
  if (OB_ISNULL(prefix_ns_ = static_cast<ObPathVarObject*>(allocator.alloc(sizeof(ObPathVarObject))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc ObPathVarObject", K(ret));
  } else {
    prefix_ns_ = new (prefix_ns_) ObPathVarObject(allocator);
  }
  for (int64_t i = 0; i < namespace_arr.count() && OB_SUCC(ret); i += 2) {
    if ((i + 1) >= namespace_arr.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get value from namespace arr", K(ret));
    } else if (namespace_arr.at(i + 1).empty()) {
      default_ns = namespace_arr.at(i);
    } else {
      ObString prefix_str;
      if (namespace_arr.at(i).empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("uri is not allow to empty", K(ret));
      } else if (OB_FAIL(ob_write_string(allocator, namespace_arr.at(i + 1), prefix_str))) { // deep copy prefix, path only copy value
        LOG_WARN("fail to wirte prefix string", K(ret), K(default_ns));
      } else if (OB_FAIL(add_ns_to_container_node(*prefix_ns_, prefix_str, namespace_arr.at(i), allocator))) {
        LOG_WARN("fail to add prefix namespace node", K(ret), K(namespace_arr.at(i + 1)), K(namespace_arr.at(i)));
      }
    }
  } // end for
  if (OB_SUCC(ret)) {
    prefix_ns = prefix_ns_;
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

int ObXMLExprHelper::get_xml_base_from_expr(const ObExpr *expr,
                                            ObMulModeMemCtx *mem_ctx,
                                            ObEvalCtx &ctx,
                                            ObIMulModeBase *&node)
{
  INIT_SUCC(ret);
  ObDatum *t_datum = NULL;
  ObObjType val_type = expr->datum_meta_.type_;
  ObString xml_text;
  uint16_t sub_schema_id = expr->obj_meta_.get_subschema_id();
  if (OB_FAIL(expr->eval(ctx, t_datum))) {
    LOG_WARN("eval xml arg failed", K(ret));
  } else if (!ob_is_xml_sql_type(val_type, sub_schema_id)) {
    if (ob_is_string_type(val_type)) {
      if (OB_FAIL(ObTextStringHelper::read_real_string_data(*mem_ctx->allocator_, *t_datum, expr->datum_meta_, expr->obj_meta_.has_lob_header(), xml_text))) {
        LOG_WARN("fail to get real data.", K(ret), K(xml_text));
      } else if (xml_text.empty()) {
        ret = OB_ERR_XQUERY_TYPE_MISMATCH;
        LOG_WARN("node is NULL", K(ret));
      } else if (OB_FAIL(ObMulModeFactory::get_xml_base(mem_ctx, xml_text,
                                                    ObNodeMemType::TREE_TYPE,
                                                    ObNodeMemType::BINARY_TYPE,
                                                    node, M_DOCUMENT, false, true))) {
        LOG_WARN("fail to get xml base", K(ret));
      }
    } else {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, ob_obj_type_str(val_type), "xmltype");
      LOG_WARN("inconsistent datatypes", K(ret), K(ob_obj_type_str(val_type)));
    }
  } else if (t_datum->is_null()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(get_xml_base(mem_ctx, t_datum, ctx, node, ObGetXmlBaseType::OB_SHOULD_CHECK))) {
    LOG_WARN("fail to get xml node", K(ret));
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

int ObXMLExprHelper::append_header_in_front(ObIAllocator &allocator, ObXmlDocument *&root, ObIMulModeBase *node)
{
  INIT_SUCC(ret);
  ObXmlDocument *xml_root = NULL;
  if (OB_ISNULL(xml_root = OB_NEWx(ObXmlDocument, (&allocator), ObMulModeNodeType::M_CONTENT, (root->get_mem_ctx())))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create xml text node", K(ret));
  } else if (OB_FAIL(xml_root->append(node))) {
    LOG_WARN("fail to append node value to content", K(ret));
  } else if (OB_FAIL(root->append(xml_root))) {
    LOG_WARN("fail to append node value to content", K(ret));
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
  ObNsSortedVector* ns_vec_point = nullptr;
  ObNsSortedVector ns_vec;
  if (OB_FAIL(ObXmlUtil::xml_bin_type(binary_str, node_type))) {
    LOG_WARN("get xml bin type failed", K(ret));
  } else if (node_type == ObMulModeNodeType::M_UNPARESED_DOC) {
    if (OB_FAIL(ObXmlParserUtils::parse_document_text(mem_ctx, binary_str, doc))) {
      LOG_WARN("parse document text failed", K(ret));
    } else if (FALSE_IT(tree = doc)) {
    } else if (OB_FAIL(bin.parse_tree(tree))) {
      LOG_WARN("parse tree failed", K(ret));
    } else if (OB_FAIL(ObXmlUtil::init_print_ns(mem_ctx->allocator_, &bin, ns_vec, ns_vec_point))) {
      LOG_WARN("fail to init ns vector by extend area", K(ret));
    } else if (OB_FAIL(bin.print_document(buff, CS_TYPE_UTF8MB4_GENERAL_CI, ObXmlFormatType::NO_FORMAT, 2, ns_vec_point))) {
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
  ObNsSortedVector* ns_vec_point = nullptr;
  ObNsSortedVector ns_vec;
  if (OB_FAIL(ObXmlUtil::xml_bin_type(binary_str, node_type))) {
    LOG_WARN("get xml bin type failed", K(ret));
  } else if (node_type == ObMulModeNodeType::M_UNPARSED) {
    if (OB_FAIL(ObXmlParserUtils::parse_content_text(mem_ctx, binary_str, doc))) {
      LOG_WARN("parse document text failed", K(ret));
    } else if (FALSE_IT(tree = doc)) {
    } else if (OB_FAIL(bin.parse_tree(tree))) {
      LOG_WARN("parse tree failed", K(ret));
    } else if (bin.check_extend() && OB_FAIL(ObXmlUtil::init_print_ns(mem_ctx->allocator_, &bin, ns_vec, ns_vec_point))) {
      LOG_WARN("fail to init ns vector by extend area", K(ret));
    } else if (OB_FAIL(bin.print_content(buff, false, false, ObXmlFormatType::NO_FORMAT, param_list, ns_vec_point))) {
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

// is_root is symbol of scan node seek
int ObXMLExprHelper::check_xpath_valid(ObPathExprIter &xpath_iter, bool is_root)
{
  INIT_SUCC(ret);
  ObPathNodeAxis first_axis;
  ObSeekType first_type;
  // check axis
  if (OB_FAIL(xpath_iter.get_first_axis(first_axis))) {
    LOG_WARN("fail to get first node axis", K(ret));
  } else if (OB_FAIL(xpath_iter.get_first_seektype(first_type))) {
    LOG_WARN("fail to get first seek type", K(ret));
  } else {
    switch (first_axis) {
      case ObPathNodeAxis::ANCESTOR: {
        if (is_root) {
          ret = OB_ERR_XPATH_INVALID_NODE; // ORA-19276: XPST0005 - XPath step specifies an invalid element/attribute name:
          LOG_USER_ERROR(OB_ERR_TOO_MANY_PREFIX_DECLARE, xpath_iter.get_path_str().length(), xpath_iter.get_path_str().ptr());
        } else if (xpath_iter.get_path_str()[0] == '/') {
          break;  // '/' in first will not report error
        } else {
          ret = OB_ERR_XQUERY_UNSUPPORTED; // ORA-19110: unsupported XQuery expression
          LOG_WARN("xquery unsupported", K(ret));
        }
        break;
      }
      case ObPathNodeAxis::SELF:
      case ObPathNodeAxis::ANCESTOR_OR_SELF: {
        if (is_root && first_type != ObSeekType::TEXT && first_type != ObSeekType::NODES) {
          ret = OB_ERR_XPATH_INVALID_NODE; // ORA-19276: XPST0005 - XPath step specifies an invalid element/attribute name:
          LOG_USER_ERROR(OB_ERR_TOO_MANY_PREFIX_DECLARE, xpath_iter.get_path_str().length(), xpath_iter.get_path_str().ptr());
        } else if (is_root) {
        } else if (xpath_iter.get_path_str()[0] == '.' || xpath_iter.get_path_str()[0] == '/') {
             // '.' or '/' in first will not report error
        } else {
          ret = OB_ERR_XQUERY_UNSUPPORTED; // ORA-19110: unsupported XQuery expression
          LOG_WARN("xquery unsupported", K(ret));
        }
        break;
      }
      case ObPathNodeAxis::PARENT: {
        if (is_root) {
          ret = OB_ERR_XPATH_NO_NODE; // ORA-19277: XPST0005 - XPath step specifies an item type matching no node:
          LOG_USER_ERROR(OB_ERR_TOO_MANY_PREFIX_DECLARE, xpath_iter.get_path_str().length(), xpath_iter.get_path_str().ptr());
        } else {
          ret = OB_ERR_XQUERY_UNSUPPORTED; // ORA-19110: unsupported XQuery expression
          LOG_WARN("xquery unsupported", K(ret));
        }
        break;
      }
      case ObPathNodeAxis::ATTRIBUTE: {
        if (is_root) {
          ret = OB_ERR_XPATH_INVALID_NODE; // ORA-19276: XPST0005 - XPath step specifies an invalid element/attribute name:
          LOG_USER_ERROR(OB_ERR_TOO_MANY_PREFIX_DECLARE, xpath_iter.get_path_str().length(), xpath_iter.get_path_str().ptr());
        }
        break;
      }
      default: {
        break;
      }
    }
  }
  if (OB_SUCC(ret)) {
    switch (first_type) {
      case ObSeekType::TEXT: {
        if (is_root && first_axis == ObPathNodeAxis::CHILD) {
          ret = OB_ERR_XPATH_NO_NODE; // ORA-19277: XPST0005 - XPath step specifies an item type matching no node:
          LOG_USER_ERROR(OB_ERR_TOO_MANY_PREFIX_DECLARE, xpath_iter.get_path_str().length(), xpath_iter.get_path_str().ptr());
        }
        break;
      }
      default: {
        break;
      }
    }
  }
  return ret;
}

int ObXMLExprHelper::get_xpath_result(ObPathExprIter &xpath_iter, ObIMulModeBase *&xml_res, ObMulModeMemCtx *mem_ctx, bool add_ns)
{
  int ret = OB_SUCCESS;
  ObStringBuffer buff(mem_ctx->allocator_);
  ObXmlDocument *root = NULL;
  ObIMulModeBase *node = NULL;
  int64_t append_node_num = 0;
  ObString blob_locator;
  ObStringBuffer res_buf(mem_ctx->allocator_);
  ObMulModeNodeType node_type = M_MAX_TYPE;
  if (!xpath_iter.is_first_init()) {
  } else if (OB_FAIL(xpath_iter.open())) {
    ret = OB_ERR_PARSE_XQUERY_EXPR;
    LOG_USER_ERROR(OB_ERR_PARSE_XQUERY_EXPR, xpath_iter.get_path_str().length(), xpath_iter.get_path_str().ptr());
    LOG_WARN("fail to open xpath iterator", K(ret));
    // ObXMLExprHelper::replace_xpath_ret_code(ret);
  } else if (OB_FAIL(check_xpath_valid(xpath_iter, false))) {
    LOG_WARN("check xpath valid failed", K(ret));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObXMLExprHelper::binary_agg_xpath_result(xpath_iter, node_type, mem_ctx, res_buf, append_node_num, add_ns))) {
    LOG_WARN("agg xpath failed", K(ret));
  } else if (OB_FAIL(ObMulModeFactory::get_xml_base(mem_ctx, res_buf.string(),
                                                    ObNodeMemType::BINARY_TYPE,
                                                    ObNodeMemType::BINARY_TYPE,
                                                    xml_res))) {
    LOG_WARN("fail to get xml node", K(ret));
  }

  if (ret == OB_ITER_END) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObXMLExprHelper::binary_agg_xpath_result(ObPathExprIter &xpath_iter,
                                              ObMulModeNodeType &node_type,
                                              ObMulModeMemCtx* mem_ctx,
                                              ObStringBuffer &res,
                                              int64_t &append_node_num,
                                              bool add_ns)
{
  INIT_SUCC(ret);
  ObIMulModeBase *node = NULL;
  int element_count = 0;
  int text_count = 0;
  ObString version;
  ObString encoding;
  uint16_t standalone;
  ObXmlBin extend;
  ObIMulModeBase* last_parent = nullptr;
  bool first_is_doc = false;
  common::hash::ObHashMap<ObString, ObString> ns_map;
  xpath_iter.set_add_ns(add_ns);
  ObBinAggSerializer bin_agg(mem_ctx->allocator_, ObBinAggType::AGG_XML, static_cast<uint8_t>(M_CONTENT));
  bin_agg.close_merge_text();
  if (add_ns && OB_FAIL(ns_map.create(10, lib::ObMemAttr(MTL_ID(), "XMLModule")))) {
    LOG_WARN("ns map create failed", K(ret));
  }
  while (OB_SUCC(ret)) {
    ObIMulModeBase* tmp = nullptr;
    if (OB_FAIL(xpath_iter.get_next_node(node))) {
      if (ret != OB_ITER_END) {
        ret = OB_ERR_PARSE_XQUERY_EXPR;
        LOG_USER_ERROR(OB_ERR_PARSE_XQUERY_EXPR, xpath_iter.get_path_str().length(), xpath_iter.get_path_str().ptr());
        LOG_WARN("fail to get next xml node", K(ret));
      }
    } else if (OB_ISNULL(node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("xpath result node is null", K(ret));
    } else if (node->is_tree() && OB_FAIL(ObMulModeFactory::transform(mem_ctx, node, BINARY_TYPE, node))) {
      LOG_WARN("fail to transform to tree", K(ret));
    } else {
      ObXmlBin *bin = nullptr;
      if (OB_ISNULL(bin = static_cast<ObXmlBin*>(node))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get bin failed", K(ret));
      } else if (bin->meta_.len_ == 0) {
        // do nothing
      } else if (add_ns && bin->check_extend()) {
        bool conflict = false;
        // check key conflict
        if (OB_FAIL(bin->get_extend(extend))) {
          LOG_WARN("fail to get extend", K(ret));
        } else if (OB_FAIL(ObXmlUtil::check_ns_conflict(xpath_iter.get_cur_res_parent(), last_parent, &extend, ns_map, conflict))) {
          LOG_WARN("fail to check conflict", K(ret));
        } else if (conflict) {
          // if conflict, merge bin
          if (OB_FAIL(bin->merge_extend(extend))) {
            LOG_WARN("fail to merge extend", K(ret));
          } else {
            bin = &extend;
          }
        } else if (OB_FAIL(bin->remove_extend())) { // if not conflict, erase extend
          LOG_WARN("fail to remove extend", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(bin_agg.append_key_and_value(bin))) {
        LOG_WARN("failed to append binary", K(ret));
      } else {
        ObMulModeNodeType type = node->type();
        if (append_node_num == 0 && type == ObMulModeNodeType::M_DOCUMENT) {
          version = node->get_version();
          encoding = node->get_encoding();
          standalone = node->get_standalone();
          first_is_doc = version.empty() ? false : true;
        }
        if (type == ObMulModeNodeType::M_ELEMENT || type == ObMulModeNodeType::M_DOCUMENT) {
          element_count++;
        } else if (type == ObMulModeNodeType::M_TEXT || type == ObMulModeNodeType::M_CDATA) {
          text_count++;
        } else if (type == ObMulModeNodeType::M_CONTENT) {
          append_node_num += bin->count() - 1;
        }
        append_node_num++;
      }
    }
  }
  if (ret == OB_ITER_END) {
    ret = OB_SUCCESS;
    if (element_count > 1 || element_count == 0) {
      node_type = ObMulModeNodeType::M_CONTENT;
    } else if (element_count == 1 && text_count > 0) {
      node_type = ObMulModeNodeType::M_CONTENT;
    } else if (append_node_num == 0) {
      // do nothing
    } else {
      node_type = ObMulModeNodeType::M_DOCUMENT;
    }
    bin_agg.set_header_type(node_type);
    if (first_is_doc && append_node_num == 1) {
      bin_agg.set_xml_decl(version, encoding, standalone);
    }
    if (OB_FAIL(bin_agg.serialize())) {
      LOG_WARN("failed to serialize binary.", K(ret));
    } else if (add_ns && ns_map.size() > 0 && OB_FAIL(ObXmlUtil::ns_to_extend(mem_ctx, ns_map, bin_agg.get_buffer()))) {
      LOG_WARN("failed to serialize extend.", K(ret));
    } else{
      res.append(bin_agg.get_buffer()->string());
    }
  }
  ns_map.clear();
  return ret;
}

int ObXMLExprHelper::cast_to_res(ObIAllocator &allocator, ObString &xml_content, const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObObj src_obj;
  if (xml_content.empty()) {
    res.set_null();
  } else {
    src_obj.set_string(ObVarcharType, xml_content);
    src_obj.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    if (OB_FAIL(cast_to_res(allocator, src_obj, expr, ctx, res))) {
      LOG_WARN("fail to cast to res", K(ret));
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

int ObXMLExprHelper::cast_to_res(ObIAllocator &allocator,
                                  ObObj &src_obj,
                                  const ObExpr &expr,
                                  ObEvalCtx &ctx,
                                  ObDatum &res,
                                  bool xt_need_acc_check)
{
  int ret = OB_SUCCESS;
  ObCastMode def_cm = CM_NONE;
  ObSQLSessionInfo *session = NULL;
  ObObj dst_obj, buf_obj;
  const ObObj *res_obj = NULL;
  ObAccuracy out_acc;
  if (src_obj.is_null()) {
    res.set_null();
  } else {
    ObSolidifiedVarsGetter helper(expr, ctx, ctx.exec_ctx_.get_my_session());
    ObDataTypeCastParams dtc_params;
    // to type
    if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sessioninfo is NULL");
    } else if (OB_FAIL(ObSQLUtils::get_default_cast_mode(session->get_stmt_type(),
                                                  session, def_cm))) {
      LOG_WARN("get_default_cast_mode failed", K(ret));
    } else if (OB_FAIL(helper.get_dtc_params(dtc_params))) {
      LOG_WARN("get dtc params failed", K(ret));
    } else {
      ObObjType obj_type = expr.datum_meta_.type_;
      ObCollationType cs_type = expr.datum_meta_.cs_type_;
      ObPhysicalPlanCtx *phy_plan_ctx = ctx.exec_ctx_.get_physical_plan_ctx();
      ObCastCtx cast_ctx(&allocator, &dtc_params, get_cur_time(phy_plan_ctx), def_cm,
                         cs_type, NULL, NULL);
      if (OB_FAIL(ObObjCaster::to_type(obj_type, cs_type, cast_ctx, src_obj, dst_obj))) {
        LOG_WARN("failed to cast object to ", K(ret), K(src_obj), K(obj_type));
      } else if (FALSE_IT(get_accuracy_from_expr(expr, out_acc))) {
      } else if (FALSE_IT(res_obj = &dst_obj)) {
      } else if (OB_FAIL(obj_accuracy_check(cast_ctx, out_acc, cs_type, dst_obj, buf_obj, res_obj))) {
        if (!xt_need_acc_check && (ob_is_varchar_or_char(obj_type, cs_type) || ob_is_nchar(obj_type)) && ret == OB_ERR_DATA_TOO_LONG) {
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
          ObNsSortedVector* ns_vec_point = nullptr;
          ObNsSortedVector ns_vec;

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
          } else if (OB_FAIL(ObXmlUtil::init_print_ns(allocator, node, ns_vec, ns_vec_point))) {
            LOG_WARN("fail to init ns vector by extend area", K(ret));
          }

          ObCollationType cs_type = session_info->get_local_collation_connection();

          if (OB_FAIL(ret) || data.length() == 0) {
          } else if (OB_ISNULL(jbuf = OB_NEWx(ObStringBuffer, allocator, (allocator)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to construct buffer", K(ret));
          } else if ((ObMulModeNodeType::M_DOCUMENT == node_type ||
                      ObMulModeNodeType::M_UNPARESED_DOC == node_type) // /Although it is judged here that node_type=M_UNPARESED_DOC, it can actually go here, and the label has been replaced with DOCUMENT
                      && OB_FAIL(node->print_document(*jbuf, cs_type, ObXmlFormatType::WITH_FORMAT, 2, ns_vec_point))) { // default size value of print_document is 2
            LOG_WARN("print document failed", K(ret));
          } else if ((ObMulModeNodeType::M_CONTENT == node_type ||
                      ObMulModeNodeType::M_UNPARSED == node_type) &&
                      OB_FAIL(node->print_content(*jbuf, false, false, ObXmlFormatType::WITH_FORMAT, param_list, ns_vec_point))) {
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
  } else if (value.is_geometry()) {
    if (lib::is_mysql_mode()) {
    } else if (is_ps_protocol) {
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

int ObXMLExprHelper::extract_xml_text_node(ObMulModeMemCtx* mem_ctx, ObIMulModeBase *xml_doc, ObString &res)
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
  } else if (OB_FAIL(xpath_iter.init(mem_ctx, xpath_str, default_ns, xml_doc, NULL, false))) {
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

// update the new node default ns to empty when the parent node has default ns
int ObXMLExprHelper::update_new_nodes_ns(ObIAllocator &allocator, ObXmlNode *parent, ObXmlNode *update_node)
{
  int ret = OB_SUCCESS;
  ObXmlAttribute *empty_ns = NULL;
  ObXmlAttribute *default_ns = NULL;
  ObXmlAttribute *update_node_default_ns = NULL;
  if (OB_ISNULL(parent) || OB_ISNULL(update_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node is NULL", K(ret), K(parent), K(update_node));
  } else if (OB_FAIL(get_valid_default_ns_from_parent(parent, default_ns))) {
    LOG_WARN("unexpected error in find default ns from parent", K(ret));
  } else if (OB_NOT_NULL(default_ns) && !default_ns->get_value().empty()) {
    // need to update the new node default ns with empty default ns
    if (OB_FAIL(get_valid_default_ns_from_parent(update_node, update_node_default_ns))) {
      LOG_WARN("unexpected error in find default ns from parent", K(ret));
    } else if (OB_ISNULL(update_node_default_ns) || update_node_default_ns->get_value().empty()) {
      if (OB_ISNULL(empty_ns = OB_NEWx(ObXmlAttribute, (&allocator), ObMulModeNodeType::M_NAMESPACE, parent->get_mem_ctx()))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc failed", K(ret));
      } else {
        empty_ns->set_xml_key(ObXmlConstants::XMLNS_STRING);
        empty_ns->set_value(ObString::make_empty_string());
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(set_ns_recrusively(update_node, empty_ns))) {
        LOG_WARN("fail to set empty default ns recrusively", K(ret));
      }
    }
  }
  return ret;
}

// found valid default ns from down to top
int ObXMLExprHelper::get_valid_default_ns_from_parent(ObXmlNode *cur_node, ObXmlAttribute* &default_ns)
{
  int ret = OB_SUCCESS;
  ObXmlNode* t_node = NULL;
  bool is_found = false;
  if (OB_ISNULL(cur_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("update node is NULL", K(ret));
  } else if (!ObXMLExprHelper::is_xml_element_node(cur_node->type())) {
    t_node = cur_node->get_parent();
  } else {
    t_node = cur_node;
  }

  while(!is_found && OB_SUCC(ret) && OB_NOT_NULL(t_node)) {
    ObXmlElement *t_element = static_cast<ObXmlElement*>(t_node);
    ObArray<ObIMulModeBase *> attr_list;
    if (OB_FAIL(t_element->get_namespace_list(attr_list))) {
      LOG_WARN("fail to get namespace list", K(ret));
    }
    for (int i = 0; !is_found && OB_SUCC(ret) && i < attr_list.size(); i ++) {
      ObXmlAttribute *attr = static_cast<ObXmlAttribute *>(attr_list.at(i));
      if (attr->get_key().compare(ObXmlConstants::XMLNS_STRING) == 0) {
        is_found = true;
        default_ns = attr;
      }
    }
    t_node = t_node->get_parent();
  }
  return ret;
}

int ObXMLExprHelper::set_ns_recrusively(ObXmlNode *update_node, ObXmlAttribute *ns)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(update_node) || OB_ISNULL(ns)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("update node is NULL", K(ret), K(update_node), K(ns));
  } else if (!ObXMLExprHelper::is_xml_element_node(update_node->type())) {
    // no need to set default ns
  } else {
    bool is_stop = false;
    ObXmlElement *ele_node = static_cast<ObXmlElement *>(update_node);
    ObString key = ns->get_key();
    if (ele_node->type() != M_ELEMENT) {
      // skip
    } else if (key.compare(ObXmlConstants::XMLNS_STRING) == 0) {
      // update default ns
      if (ele_node->get_prefix().empty()) {
        // this condition mean: has no ns || has non-empty default ns
        is_stop = true;
        ObXmlAttribute *default_ns = NULL;
        if (OB_FAIL(get_valid_default_ns_from_parent(update_node, default_ns))) {
          LOG_WARN("get default ns failed.", K(ret));
        } else if (OB_ISNULL(default_ns) || default_ns->get_value().empty()) {
          ele_node->add_attribute(ns, false, 0);
          ele_node->set_ns(ns);
        } else { /* has non-empty default ns, skip and stop find */ }
      }
    } else { // has prefix
      ObXmlAttribute *tmp_ns = NULL;
      if (ele_node->get_ns() == ns ||
          ele_node->has_attribute_with_ns(ns) ||
          OB_NOT_NULL(tmp_ns = ele_node->get_ns_by_name(key))) {
        // match condition below will stop recrusive
        // element use this prefix ns || attributes of element use this prefix ns || this prefix in attributes
        is_stop = true;
        if (OB_NOT_NULL(tmp_ns)) { // if the prefix not in attributes
        } else if (OB_FAIL(ele_node->add_attribute(ns, false, 0))) {
          LOG_WARN("fail to add namespace node", K(ret), K(key));
        }
      }
    }

    if (!is_stop) {
      // find its child node recrusivle when no need to set default ns
      for (int64_t i = 0; OB_SUCC(ret) && i < ele_node->size(); i++) {
        if (OB_FAIL(SMART_CALL(set_ns_recrusively(ele_node->at(i), ns)))) {
          LOG_WARN("fail set default ns in origin tree recursively", K(ret));
        }
      } // end for
    } // end is_stop
  }
  return ret;
}

void ObXMLExprHelper::get_accuracy_from_expr(const ObExpr &expr, ObAccuracy &accuracy)
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

} // sql
} // oceanbase