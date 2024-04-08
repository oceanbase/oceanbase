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
 * This file is for define of func xml expr helper
 */

#ifndef OCEANBASE_SQL_OB_EXPR_XML_FUNC_HELPER_H_
#define OCEANBASE_SQL_OB_EXPR_XML_FUNC_HELPER_H_

#include "sql/engine/expr/ob_expr_util.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#ifdef OB_BUILD_ORACLE_XML
#include "lib/xml/ob_xml_parser.h"
#include "lib/xml/ob_xpath.h"
#include "lib/xml/ob_xml_tree.h"
#include "lib/xml/ob_xml_util.h"
#endif // OB_BUILD_ORACLE_XML

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

class ObXMLExprHelper final
{
public:
#ifdef OB_BUILD_ORACLE_XML
  static int add_binary_to_element(ObMulModeMemCtx* mem_ctx, ObString bianry_value, ObXmlElement &element);
  static int get_xml_base(ObMulModeMemCtx* mem_ctx, ObDatum *xml_datum, ObCollationType cs_type,
                          ObNodeMemType expect_type, ObIMulModeBase *&node);
  static int get_xml_base(ObMulModeMemCtx* mem_ctx, ObDatum *xml_datum, ObCollationType cs_type,
                          ObNodeMemType expect_type, ObIMulModeBase *&node, ObMulModeNodeType &node_type, bool is_reparse = false);
  static int try_to_parse_unparse_binary(ObMulModeMemCtx* mem_ctx, ObCollationType cs_type,
                                         ObIMulModeBase *input_node, ObNodeMemType expect_type,
                                         ObIMulModeBase *&res_node);
  static int pack_binary_res(const ObExpr &expr, ObEvalCtx &ctx,
                              ObString binary_str, ObString &blob_locator);
  static int pack_xml_res(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res, ObXmlDocument* doc,
                          ObMulModeMemCtx* mem_ctx, ObMulModeNodeType node_type,
                          ObString &plain_text);
  // for namespace
  static int construct_namespace_params(ObString &namespace_str,
                                        ObString &default_ns,
                                        ObPathVarObject &prefix_ns,
                                        ObIAllocator &allocator);
  static int parse_namespace_str(ObString &ns_str, ObString &prefix, ObString &uri);
  // set string result
  static int set_string_result(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res, ObString &res_str);
  // get xmltype str from input expr
  static int get_xmltype_from_expr(const ObExpr *expr,
                                   ObEvalCtx &ctx,
                                   ObDatum *&xml_datum);
  static int get_str_from_expr(const ObExpr *expr,
                               ObEvalCtx &ctx,
                               ObString &res,
                               ObIAllocator &allocator);
  static int parse_xml_str(ObMulModeMemCtx *ctx, const ObString &xml_text, ObXmlDocument *&xml_doc);

  // classify xml node type
  static bool is_xml_leaf_node(ObMulModeNodeType node_type);
  static bool is_xml_text_node(ObMulModeNodeType node_type);
  static bool is_xml_element_node(ObMulModeNodeType node_type);
  static bool is_xml_root_node(ObMulModeNodeType node_type);
  static bool is_xml_attribute_node(ObMulModeNodeType node_type);

  static void replace_xpath_ret_code(int &ret);
  static int check_xml_document_unparsed(ObMulModeMemCtx* mem_ctx, ObString binary_str, bool &validity);
  static int parse_xml_document_unparsed(ObMulModeMemCtx* mem_ctx, ObString binary_str, ObString &res_str, ObXmlDocument* &res_doc);
  static int content_unparsed_binary_check_doc(ObMulModeMemCtx* mem_ctx, ObString binary_str, ObString &res_str);
  static int check_element_validity(ObMulModeMemCtx* mem_ctx, ObXmlElement *in_ele, ObXmlElement *&out_ele, bool &validity);
  static int check_doc_validity(ObMulModeMemCtx* mem_ctx, ObXmlDocument *&doc, bool &validity);
#endif // OB_BUILD_ORACLE_XML
  static int process_sql_udt_results(common::ObObj& value, sql::ObResultSet &result);
  static int process_sql_udt_results(common::ObObj& value,
                                     common::ObIAllocator *allocator,
                                     sql::ObSQLSessionInfo *session_info,
                                     sql::ObExecContext *exec_context,
                                     bool is_ps_protocol,
                                     const ColumnsFieldIArray *fields = NULL,
                                     ObSchemaGetterGuard *schema_guard = NULL);
  static uint64_t get_tenant_id(ObSQLSessionInfo *session);
#ifdef OB_BUILD_ORACLE_XML
private:
  static int add_ns_to_container_node(ObPathVarObject &container,
                                      ObString &prefix,
                                      ObString &uri,
                                      ObIAllocator &allocator);
#endif // OB_BUILD_ORACLE_XML
};
} // sql
} // oceanbase

#endif // OCEANBASE_SQL_OB_EXPR_XML_FUNC_HELPER_H_