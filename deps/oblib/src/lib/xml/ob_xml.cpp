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
#ifdef OB_BUILD_ORACLE_PL
#define USING_LOG_PREFIX LIB_XML2

#include "lib/xml/ob_xml.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/ob_define.h"
#include "common/object/ob_object.h"
#include "libxslt/transform.h"
#include "libxslt/xsltutils.h"

namespace oceanbase
{
namespace common
{

int ObXml::parse_xml(const ObString &input) {
  int ret = OB_SUCCESS;
  xmlDocPtr xml_doc_ptr = NULL;
  if (OB_FAIL(parse_str_to_xml(input, xml_doc_ptr))) {
    COMMON_LOG(WARN, "parse xml failed");
  }
  xmlFreeDoc(xml_doc_ptr);
  return ret;
}

int ObXml::xslt_transform(ObIAllocator *allocator,
                          const ObString &input,
                          const ObString &xslt_sheet,
                          const ObIArray<ObString> &params,
                          ObString &output)
{
  int ret = OB_SUCCESS;
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(common::OB_SERVER_TENANT_ID, "XSLTCache"));
  if (xslt_sheet.empty()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "xsl sheet is empty", K(xslt_sheet), K(ret));
  } else if (input.empty()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "input xml is empty", K(input), K(ret));
  } else {
    xmlDocPtr input_xml_ptr = NULL;
    xmlDocPtr output_xml_ptr = NULL;
    xmlDocPtr sheet_xml_ptr = NULL;
    xsltStylesheetPtr style_sheet = NULL;
    if (OB_FAIL(parse_str_to_xml(input, input_xml_ptr))) {
      COMMON_LOG(WARN, "parse_str_to_xml failed", K(input), K(input_xml_ptr), K(ret));
    } else if (OB_FAIL(parse_str_to_xml(xslt_sheet, sheet_xml_ptr))){
      COMMON_LOG(WARN, "parse_str_to_xml failed", K(xslt_sheet), K(sheet_xml_ptr), K(ret));
    } else if (OB_FAIL(get_style_sheet(sheet_xml_ptr, style_sheet))) {
      COMMON_LOG(WARN, "get_style_sheet failed", K(ret));
    } else if (OB_FAIL(xslt_apply_style_sheet(input_xml_ptr, output_xml_ptr, style_sheet, params))) {
      COMMON_LOG(WARN, "xslt_apply_style_sheet failed", K(ret));
    } else if (OB_FAIL(xslt_save_to_string(allocator, output_xml_ptr, style_sheet, output))) {
      COMMON_LOG(WARN, "xslt_save_to_string failed");
    }
    xmlFreeDoc(output_xml_ptr);
    xsltFreeStylesheet(style_sheet);
    xmlFreeDoc(input_xml_ptr);
    // If style_sheet is empty, then sheet_xml_ptr has no xlst style,
    // and the memory of sheet_xml_ptr should be released;
    if (OB_ISNULL(style_sheet)) {
      xmlFreeDoc(sheet_xml_ptr);
    }
  }
  return ret;
}

int ObXml::parse_str_to_xml(const ObString &input, xmlDocPtr &xml_ptr)
{
  int ret = OB_SUCCESS;

  if (input.empty()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "xml string is empty", K(input), K(ret));
  } else {
    xml_ptr = xmlReadMemory(input.ptr(), input.length(), NULL, "utf-8", XML_PARSE_PEDANTIC);
    xmlErrorPtr xml_err = xmlGetLastError();
    if (OB_NOT_NULL(xml_err)) {
      ret = OB_ERR_PARSER_SYNTAX;
      COMMON_LOG(WARN, "parse xml failed", KCSTRING(xml_err->message), K(input), K(ret));
      ObLibXml2SaxHandler::reset_libxml_last_error();
    } else if (OB_ISNULL(xml_ptr)) {
      ret = OB_ERR_PARSER_SYNTAX;
      COMMON_LOG(WARN, "parse xml failed", K(input), K(ret));
    }
  }

  return ret;
}

int ObXml::xslt_apply_style_sheet(const xmlDocPtr input_xml_ptr,
                                  xmlDocPtr &output_xml_ptr,
                                  const xsltStylesheetPtr xslt_ptr,
                                  const ObIArray<ObString> &params)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 != params.count())) {
    ret = OB_NOT_SUPPORTED;
    COMMON_LOG(WARN, "params count is not zero", K(ret));
  }
  if (OB_SUCC(ret)) {
    output_xml_ptr = xsltApplyStylesheet(xslt_ptr, input_xml_ptr, NULL);
    xmlErrorPtr xml_err = xmlGetLastError();
    if (OB_NOT_NULL(xml_err)) {
      ret = OB_ERR_PARSER_SYNTAX;
      COMMON_LOG(WARN, "xsltApplyStylesheet failed", KCSTRING(xml_err->message), K(ret));
      ObLibXml2SaxHandler::reset_libxml_last_error();
    } else if (OB_ISNULL(output_xml_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "xslt xslt_apply_style_sheet failed", K(ret));
    }
  }
  return ret;
}

int ObXml::xslt_save_to_string(ObIAllocator *allocator,
                               const xmlDocPtr result,
                               const xsltStylesheetPtr xslt_ptr,
                               ObString &output)
{
  int ret = OB_SUCCESS;
  xmlChar *xml_result_buffer = NULL;
  int32_t xml_result_length = 0;
  int32_t res = xsltSaveResultToString(&xml_result_buffer, &xml_result_length, result, xslt_ptr);
  if (-1 == res) {
    xmlFree(xml_result_buffer);
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "xslt save to string failed", K(ret));
  } else {
    char *result_buf = NULL;
    if (OB_ISNULL(result_buf = static_cast<char*>(allocator->alloc(xml_result_length + 1)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(WARN, "alloc memory failed", K(ret));
    } else {
      MEMCPY(result_buf, xml_result_buffer, xml_result_length);
      result_buf[xml_result_length] = '\0';
      output.assign_buffer(result_buf, xml_result_length + 1);
      output.set_length(xml_result_length);
    }
  }
  xmlFree(xml_result_buffer);
  return ret;
}

int ObXml::get_style_sheet(const xmlDocPtr &sheet_xml_ptr, xsltStylesheetPtr &xslt_ptr)
{
  int ret = OB_SUCCESS;
  xsltFreeStylesheet(xslt_ptr);
  xslt_ptr = xsltParseStylesheetDoc(sheet_xml_ptr);
  if (OB_ISNULL(xslt_ptr)) {
    ret = OB_ERR_XSLT_PARSE;
    COMMON_LOG(WARN, "document is not a stylesheet", K(ret));
  }
  return ret;
}

} // end namespace common
} // end namespace oceanbase
#endif