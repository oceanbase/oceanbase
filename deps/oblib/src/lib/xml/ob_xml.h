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
#ifndef OB_XML_H_
#define OB_XML_H_

#include "libxml2/libxml/parser.h"
#include "libxslt/transform.h"
#include "libxslt/xsltInternals.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_iarray.h"
#include "lib/xml/ob_libxml2_sax_handler.h"

namespace oceanbase
{
namespace common
{

class ObXml
{
public:
  static int parse_xml(const ObString &input);

  static int xslt_transform(ObIAllocator *allocator,
                            const ObString &input,
                            const ObString &xslt_sheet,
                            const ObIArray<ObString> &params,
                            ObString &output);

private:
  static int parse_str_to_xml(const ObString &input,
                              xmlDocPtr &xml_ptr);
  static int xslt_apply_style_sheet(const xmlDocPtr input_xml_ptr,
                                    xmlDocPtr &output_xml_ptr,
                                    const xsltStylesheetPtr xslt_ptr,
                                    const ObIArray<ObString> &params);
  static int xslt_save_to_string(ObIAllocator *allocator,
                                 const xmlDocPtr reuslt,
                                 const xsltStylesheetPtr xslt_ptr,
                                 ObString &output);
  static int get_style_sheet(const xmlDocPtr &sheet_xml_ptr, xsltStylesheetPtr &xslt_ptr);
};

} // end namespace common
} // end namespace oceanbase

#endif // OB_XML_H_
#endif