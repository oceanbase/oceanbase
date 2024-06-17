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
#ifndef OCEANBASE_LIBXML2_SAX_HANDLER_H_
#define OCEANBASE_LIBXML2_SAX_HANDLER_H_
#include "lib/xml/ob_xml_parser.h"
#include "libxml2/libxml/parser.h"
namespace oceanbase {
namespace common {
struct ObLibXml2SaxHandler
{
public:
  // libxml2 sax callback start
  static void start_document(void* ctx);
  static void end_document(void* ctx);
  // used for sax1
  static void start_element(void* ctx, const xmlChar* name, const xmlChar** p);
  static void end_element(void* ctx, const xmlChar* name);
  static void characters(void* ctx, const xmlChar* ch, int len);
  static void cdata_block(void* ctx, const xmlChar* value, int len);
  static void comment(void* ctx, const xmlChar* value);
  static void processing_instruction(void *ctx, const xmlChar *target, const xmlChar *data);
  static void internal_subset(void *ctx,
    const xmlChar *name,
    const xmlChar *external_id,
    const xmlChar *system_id);
  static void entity_reference(void *ctx, const xmlChar *name);
  // for error msg
  static void structured_error(void *ctx, xmlErrorPtr error);
  // libxml2 sax callback end
  // helper method
  static int get_parser(void* ctx, ObLibXml2SaxParser*& parser);
  static void init();
  static void destroy();
  static void reset_libxml_last_error();
};
} // namespace common
} // namespace oceanbase
#endif  //OCEANBASE_LIBXML2_SAX_HANDLER_H_