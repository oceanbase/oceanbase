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
#define USING_LOG_PREFIX LIB

#include "lib/xml/ob_libxml2_sax_handler.h"
#include "lib/xml/ob_xml_util.h"
#include "libxml2/libxml/parser.h"
#include "libxml2/libxml/parserInternals.h"
#include "lib/ob_define.h"
#include "lib/allocator/ob_malloc.h"
#ifdef OB_BUILD_ORACLE_PL
#include "libxslt/xslt.h"
#include "libxslt/extensions.h"
#endif

namespace oceanbase {
namespace common {


// ObLibXml2SaxHandler
_xmlSAXHandler* get_sax_handler()
{
  static _xmlSAXHandler sax_handler = {
    ObLibXml2SaxHandler::internal_subset, // internalSubset
    nullptr, // isStandalone
    nullptr, // hasInternalSubset
    nullptr, // hasExternalSubset
    nullptr, // resolveEntity
    nullptr, // getEntity
    nullptr, // entityDecl
    nullptr, // notationDecl
    nullptr, // attributeDecl
    nullptr, // elementDecl
    nullptr, // unparsedEntityDecl
    nullptr, // setDocumentLocator
    ObLibXml2SaxHandler::start_document, // startDocument
    ObLibXml2SaxHandler::end_document, // endDocument
    ObLibXml2SaxHandler::start_element, // startElement  // ObLibXml2SaxHandler::start_element
    ObLibXml2SaxHandler::end_element, // endElement    // ObLibXml2SaxHandler::end_element
    ObLibXml2SaxHandler::entity_reference, // reference
    ObLibXml2SaxHandler::characters, // characters
    nullptr, // ignorableWhitespace ObLibXml2SaxHandler::ignorable_whitespace
    ObLibXml2SaxHandler::processing_instruction, // processingInstruction
    ObLibXml2SaxHandler::comment,  // comment
    nullptr,  // warning
    nullptr,  // error
    nullptr, // fatalError
    nullptr, // getParameterEntity
    ObLibXml2SaxHandler::cdata_block, // cdataBlock
    nullptr, // externalSubset
    XML_SAX2_MAGIC, // initialized, use sax
    nullptr, // private
    nullptr, // startElementNs // ObLibXml2SaxHandler::start_element_ns
    nullptr, // endElementNs     // ObLibXml2SaxHandler::end_element_ns
    ObLibXml2SaxHandler::structured_error, // serror
  };
  return &sax_handler;
}

_xmlSAXHandler* get_synax_handler()
{
  static _xmlSAXHandler synax_handler = {
    nullptr, // internalSubset
    nullptr, // isStandalone
    nullptr, // hasInternalSubset
    nullptr, // hasExternalSubset
    nullptr, // resolveEntity
    nullptr, // getEntity
    nullptr, // entityDecl
    nullptr, // notationDecl
    nullptr, // attributeDecl
    nullptr, // elementDecl
    nullptr, // unparsedEntityDecl
    nullptr, // setDocumentLocator
    nullptr, // startDocument
    nullptr, // endDocument
    nullptr, // startElement  // ObLibXml2SaxHandler::start_element
    nullptr, // endElement    // ObLibXml2SaxHandler::end_element
    nullptr, // reference
    nullptr, // characters
    nullptr, // ignorableWhitespace
    nullptr, // processingInstruction
    nullptr,  // comment
    nullptr,  // warning
    nullptr,  // error
    nullptr, // fatalError
    nullptr, // getParameterEntity
    nullptr, // cdataBlock
    nullptr, // externalSubset
    XML_SAX2_MAGIC, // initialized, use sax
    nullptr, // private
    nullptr, // startElementNs // ObLibXml2SaxHandler::start_element_ns
    nullptr, // endElementNs     // ObLibXml2SaxHandler::end_element_ns
    ObLibXml2SaxHandler::structured_error, // serror
  };
  return &synax_handler;
}

void ObLibXml2SaxHandler::init()
{
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(common::OB_SERVER_TENANT_ID, "XmlGlobal"));
  xmlInitParser();
#ifdef OB_BUILD_ORACLE_PL
  xsltInitGlobals();
  xsltInit();
#endif
  LOG_INFO("saxhandler init", K(xmlIsMainThread()));
}

void ObLibXml2SaxHandler::destroy()
{
#ifdef OB_BUILD_ORACLE_PL
  xsltCleanupGlobals();
#endif
  xmlCleanupParser();
}

// libxml has pthread variable xmlGlobalState
// this variable is dynamic use malloc and can
// not belong to tenant, so use observer tenant
void ObLibXml2SaxHandler::reset_libxml_last_error()
{
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(common::OB_SERVER_TENANT_ID, "XmlGlobal"));
  xmlResetLastError();
}

int ObLibXml2SaxHandler::get_parser(void* ctx, ObLibXml2SaxParser*& parser)
{
  INIT_SUCC(ret);
  xmlParserCtxt* context = nullptr;
  if (OB_ISNULL(context = static_cast<xmlParserCtxt*>(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("context is null", K(ret));
  } else if (OB_ISNULL(parser = static_cast<ObLibXml2SaxParser*>(context->_private))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parser is null", K(ret));
  } else if (OB_UNLIKELY(parser->get_libxml2_ctxt() != context)) {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_INFO("parser ctxt changed");
  }
  return ret;
}

void ObLibXml2SaxHandler::start_document(void *ctx)
{
  INIT_SUCC(ret);
  ObLibXml2SaxParser* parser =  nullptr;
  if (OB_FAIL(get_parser(ctx, parser))) {
    LOG_WARN("get_parser failed", K(ret));
  } else if (OB_UNLIKELY(parser->is_stop_parse())) {
    LOG_INFO("parser is stopped", K(parser->get_last_errno()));
  } else if (OB_FAIL(parser->start_document())) {
    LOG_WARN("parser start_document failed", K(ret));
  }
  if (OB_FAIL(ret)) {
    parser->stop_parse(ret);
  }
}

void ObLibXml2SaxHandler::end_document(void *ctx)
{
  INIT_SUCC(ret);
  ObLibXml2SaxParser* parser =  nullptr;

  if (OB_FAIL(get_parser(ctx, parser))) {
    LOG_WARN("get_parser failed", K(ret));
  } else if (OB_UNLIKELY(parser->is_stop_parse())) {
    LOG_INFO("parser is stopped", K(parser->get_last_errno()));
  } else if (OB_FAIL(parser->end_document())) {
    LOG_WARN("parser end_document failed", K(ret));
  }

  if (OB_FAIL(ret)) {
    parser->stop_parse(ret);
  }
}

// sax1
void ObLibXml2SaxHandler::start_element(void* ctx, const xmlChar* name, const xmlChar** p)
{
  INIT_SUCC(ret);
  ObLibXml2SaxParser* parser =  nullptr;
  if (OB_FAIL(get_parser(ctx, parser))) {
    LOG_WARN("get_parser failed", K(ret));
  } else if (OB_UNLIKELY(parser->is_stop_parse())) {
    LOG_INFO("parser is stopped", K(parser->get_last_errno()));
  } else if (OB_FAIL(parser->start_element(reinterpret_cast<const char*>(name),
                                           reinterpret_cast<const char**>(p)))) {
    LOG_WARN("parser start_element failed", K(ret));
  }
  if (OB_FAIL(ret)) {
    parser->stop_parse(ret);
  }
}

void ObLibXml2SaxHandler::end_element(void* ctx, const xmlChar* name)
{
  INIT_SUCC(ret);
  ObLibXml2SaxParser* parser =  nullptr;
  if (OB_FAIL(get_parser(ctx, parser))) {
    LOG_WARN("get_parser failed", K(ret));
  } else if (OB_UNLIKELY(parser->is_stop_parse())) {
    LOG_INFO("parser is stopped", K(parser->get_last_errno()));
  } else if (OB_FAIL(parser->end_element())) {
    LOG_WARN("parser end_element failed", K(ret));
  }
  if (OB_FAIL(ret)) {
    parser->stop_parse(ret);
  }
}

void ObLibXml2SaxHandler::characters(void *ctx, const xmlChar *ch, int len)
{
  INIT_SUCC(ret);
  ObLibXml2SaxParser* parser =  nullptr;

  if (OB_FAIL(get_parser(ctx, parser))) {
    LOG_WARN("get_parser failed", K(ret));
  } else if (OB_UNLIKELY(parser->is_stop_parse())) {
    LOG_INFO("parser is stopped", K(parser->get_last_errno()));
  } else if (OB_FAIL(parser->characters(reinterpret_cast<const char*>(ch),
                                len))) {
    LOG_WARN("parser characters failed", K(ret));
  }

  if (OB_FAIL(ret)) {
    parser->stop_parse(ret);
  }
}

void ObLibXml2SaxHandler::cdata_block(void* ctx, const xmlChar* value, int len)
{
  INIT_SUCC(ret);
  ObLibXml2SaxParser* parser =  nullptr;

  if (OB_FAIL(get_parser(ctx, parser))) {
    LOG_WARN("get_parser failed", K(ret));
  } else if (OB_UNLIKELY(parser->is_stop_parse())) {
    LOG_INFO("parser is stopped", K(parser->get_last_errno()));
  } else if (OB_FAIL(parser->add_text_node(ObMulModeNodeType::M_CDATA,
                                   reinterpret_cast<const char*>(value),
                                   len))) {
    LOG_WARN("parser cdata block failed", K(ret));
  }

  if (OB_FAIL(ret)) {
    parser->stop_parse(ret);
  }
}

void ObLibXml2SaxHandler::comment(void* ctx, const xmlChar* value)
{
  INIT_SUCC(ret);
  ObLibXml2SaxParser* parser =  nullptr;
  const char *src_value = reinterpret_cast<const char*>(value);
  if (OB_ISNULL(src_value)) {
    LOG_DEBUG("empty comment ignore");
  } else if (OB_FAIL(get_parser(ctx, parser))) {
    LOG_WARN("get_parser failed", K(ret));
  } else if (OB_UNLIKELY(parser->is_stop_parse())) {
    LOG_INFO("parser is stopped", K(parser->get_last_errno()));
  } else if (OB_FAIL(parser->add_text_node(ObMulModeNodeType::M_COMMENT,
                                   src_value,
                                   STRLEN(src_value)))) {
    LOG_WARN("parser comment failed", K(ret));
  }

  if (OB_FAIL(ret)) {
    parser->stop_parse(ret);
  }
}

void ObLibXml2SaxHandler::processing_instruction(void *ctx, const xmlChar *target, const xmlChar *data)
{
  INIT_SUCC(ret);
  ObLibXml2SaxParser* parser =  nullptr;
  const char *src_target = reinterpret_cast<const char*>(target);
  const char *src_data = reinterpret_cast<const char*>(data);
  int target_len = src_target == nullptr ? 0 : STRLEN(src_target);
  int data_len = src_data == nullptr ? 0 : STRLEN(src_data);

  if (OB_FAIL(get_parser(ctx, parser))) {
    LOG_WARN("get_parser failed", K(ret));
  } else if (OB_UNLIKELY(parser->is_stop_parse())) {
    LOG_INFO("parser is stopped", );
  } else if (OB_FAIL(parser->processing_instruction(ObString(target_len, src_target), ObString(data_len, src_data)))) {
      LOG_WARN("processing_instruction failed", K(ret));
    }
  if (OB_FAIL(ret)) {
    parser->stop_parse(ret);
  }
}

// internal DTD
void ObLibXml2SaxHandler::internal_subset(void *ctx,
      const xmlChar *name,
      const xmlChar *external_id,
      const xmlChar *system_id)
{
  INIT_SUCC(ret);
  ObLibXml2SaxParser* parser =  nullptr;

  if (OB_FAIL(get_parser(ctx, parser))) {
    LOG_WARN("get_parser failed", K(ret));
  } else if (OB_UNLIKELY(parser->is_stop_parse())) {
    LOG_INFO("parser is stopped", K(parser->get_last_errno()));
  } else {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("not supprt dtd");
  }
  if (OB_FAIL(ret)) {
    parser->stop_parse(ret);
  }
}

void ObLibXml2SaxHandler::entity_reference(void *ctx, const xmlChar *name)
{
  INIT_SUCC(ret);
  ObLibXml2SaxParser* parser =  nullptr;

  if (OB_FAIL(get_parser(ctx, parser))) {
    LOG_WARN("get_parser failed", K(ret));
  } else if (parser->is_stop_parse()) {
    LOG_INFO("parser is stopped", K(parser->get_last_errno()));
  } else {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("not supprt custom enity");
  }
  if (OB_FAIL(ret)) {
    parser->stop_parse(ret);
  }
}

void ObLibXml2SaxHandler::structured_error(void *ctx, xmlErrorPtr error)
{
  INIT_SUCC(ret);
  ObLibXml2SaxParser* parser =  nullptr;
  if (OB_FAIL(get_parser(ctx, parser))) {
    LOG_WARN("get_parser failed", K(ret));
  } else if (parser->is_stop_parse()) {
    LOG_INFO("parser is stopped", K(parser->get_last_errno()));
  } else if (OB_ISNULL(error)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("input error_info is null", K(ret));
  } else if (OB_FAIL(parser->on_error(error->code))) {
    LOG_WARN("parse error", K(error->code), K(error->line), K(error->int1), K(error->int2), KCSTRING(error->message));
  }
}

// ObLibXml2SaxHandler end

// ObLibXml2SaxParser

static int create_memory_parser_ctxt(const ObString& xml_text, xmlParserCtxt*& ctxt)
{
  INIT_SUCC(ret);
  xmlParserInputPtr input = nullptr;
  xmlParserInputBufferPtr buf = nullptr;

  if (xml_text.empty()) {
    // do nothing
  } else if (OB_ISNULL(ctxt = xmlNewParserCtxt())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("create parser ctxt failed", K(ret));
  } else if (OB_ISNULL(buf = xmlParserInputBufferCreateMem(xml_text.ptr(), xml_text.length(), XML_CHAR_ENCODING_NONE))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("create parser input buffer failed", K(ret));
    // free when error
    xmlFreeParserCtxt(ctxt);
  } else if (OB_ISNULL(input = xmlNewIOInputStream(ctxt, buf, XML_CHAR_ENCODING_NONE))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("create parser input failed", K(ret));
    xmlFreeParserInputBuffer(buf);
	  xmlFreeParserCtxt(ctxt);
  } else if (xmlPushInput(ctxt, input) == -1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parser push input failed", K(ret));
    // free when error
    xmlFreeInputStream(input); // this will free buf, so no need free buf
    xmlFreeParserCtxt(ctxt);
  }
  if (OB_FAIL(ret)) {
    ctxt = nullptr;
  }
  return ret;
}

ObLibXml2SaxParser::~ObLibXml2SaxParser()
{
  if (OB_NOT_NULL(ctxt_)) {
    ctxt_->sax = old_sax_;
    ctxt_->_private = nullptr;
    xmlFreeParserCtxt(ctxt_);
    ctxt_ = nullptr;
  }
}

int ObLibXml2SaxParser::init(const ObString& xml_text, bool skip_start_blank)
{
  INIT_SUCC(ret);

  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", K(ret));
  } else if (OB_FAIL(init_xml_text(xml_text, skip_start_blank))) {
    LOG_WARN("init_xml_text failed", K(ret), K(xml_text));
  } else if (xml_text_.empty()) {
    // ignore empty
  } else if (OB_FAIL(init_parse_context())){
    LOG_WARN("create parser ctxt failed", K(ret));
  }
  return ret;
}

int ObLibXml2SaxParser::init_parse_context()
{
  INIT_SUCC(ret);
  ObLibXml2SaxHandler::reset_libxml_last_error();
  xmlParserCtxt* ctxt = nullptr;

  if (OB_FAIL(create_memory_parser_ctxt(xml_text_, ctxt))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("create parser ctxt failed", K(ret));
  } else {
    this->ctxt_ = ctxt;
    ctxt->_private = this;
    this->old_sax_ = ctxt->sax;

    xmlCtxtUseOptions(ctxt, XML_PARSE_IGNORE_ENC | XML_PARSE_NOENT);
    if (options_ & OB_XML_PARSE_SYNTAX_CHECK) {
      ctxt->sax = get_synax_handler();
    } else {
      ctxt->sax = get_sax_handler();
    }
  }
  return ret;
}

int ObLibXml2SaxParser::init_xml_text(const ObString& xml_text, bool skip_start_blank)
{
  INIT_SUCC(ret);
  const char* src_ptr = xml_text.ptr();
  int64_t src_len = xml_text.length();
  int64_t pos = 0;
  int64_t len = 0;

  // libxml2 will report error if with start whitespace
  // so skip start whitespace as need
  while(skip_start_blank && pos < src_len && isspace(src_ptr[pos])) {
    ++pos;
  }
  len = src_len - pos;
  if(len > 0) {
    xml_text_.assign_ptr(src_ptr + pos, len);
  }
  return ret;
}

void ObLibXml2SaxParser::stop_parse(int code)
{
  if (OB_NOT_NULL(ctxt_)) {
    ctxt_->instate = XML_PARSER_EOF;
    ctxt_->disableSAX = 1;
  }
  set_errno(code);
  set_stop_parse(true);
}

int ObLibXml2SaxParser::parse_document(const ObString& xml_text)
{
  INIT_SUCC(ret);
  if (OB_FAIL(init(xml_text, true))) {
    LOG_WARN("init failed", K(ret));
  } else if (xml_text_.empty()) {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("xml_text_ is empty or blank", K(ret));
  } else {
    xmlParseDocument(ctxt_);
    if (OB_FAIL(this->get_last_errno())) {
      LOG_WARN("parse failed", K(ret), K(xml_text));
    }
    ObLibXml2SaxHandler::reset_libxml_last_error();
  }
  return ret;
}

int ObLibXml2SaxParser::parse_content(const ObString& xml_text)
{
  INIT_SUCC(ret);
  // In the content, there is no need to delete the leading null character. details as following:
  // 1. Contains only text text, and there are blank characters at the beginning that need to be reserved
  // 2. Including the element node, the empty characters at the beginning and in the middle should not be reserved
  if (OB_FAIL(init(xml_text, false))) {
    LOG_WARN("init failed", K(ret));
  } else if (OB_ISNULL(document_ = OB_NEWx(ObXmlDocument, allocator_, ObMulModeNodeType::M_CONTENT, ctx_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc failed", K(ret));
  } else if (OB_NOT_NULL(ctxt_)) {
    this->set_cur_node(document_);
    document_->set_flags(MEMBER_LAZY_SORTED);
    ctxt_->instate = XML_PARSER_CONTENT;
    ctxt_->str_xml = reinterpret_cast<const xmlChar*>(ObXmlConstants::XML_STRING);
    ctxt_->str_xmlns = reinterpret_cast<const xmlChar*>(ObXmlConstants::XMLNS_STRING);
    ctxt_->str_xml_ns = reinterpret_cast<const xmlChar*>(ObXmlConstants::XML_NAMESPACE_SPECIFICATION_URI);
    xmlParseContent(ctxt_);
    if (OB_FAIL(this->get_last_errno())) {
      LOG_WARN("parse failed", K(ret), K(xml_text));
    } else if (OB_UNLIKELY(! is_parsed_all_input())) {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("input not parsed fullly", K(ret), K(xml_text.length()), K(get_parse_byte_num()));
    } else if (OB_FAIL(remove_prev_empty_text())) {
      LOG_WARN("remove_prev_empty_text fail", K(ret));
    }
    ObLibXml2SaxHandler::reset_libxml_last_error();
  }
  return ret;
}

int ObLibXml2SaxParser::check()
{
  INIT_SUCC(ret);
  if (OB_ISNULL(ctxt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctxt is null", K(ret));
  } else if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", K(ret));
  }
  return ret;
}

int ObLibXml2SaxParser::to_ob_xml_errno(int code)
{
  INIT_SUCC(ret);
  switch (code)
  {
  case XML_ERR_UNSUPPORTED_ENCODING:
  case XML_ERR_ENCODING_NAME:
    break;
  case XML_ERR_RESERVED_XML_NAME:
    if (! is_content_allow_xml_decl()) {
      ret = OB_ERR_PARSER_SYNTAX;
    }
    break;
  default:
    ret = OB_ERR_PARSER_SYNTAX;
    break;
  }
  return ret;
}

int ObLibXml2SaxParser::on_error(int code)
{
  INIT_SUCC(ret);
  if (is_recover_mode()) {
  } else if (OB_FAIL(to_ob_xml_errno(code))) {
    this->stop_parse(ret);
  } else {
    // full recover mode
    if (OB_NOT_NULL(ctxt_)) {
      ctxt_->recovery = 1;
    }
  }
  return ret;
}

int ObLibXml2SaxParser::push_namespace(ObXmlAttribute* ns)
{
  INIT_SUCC(ret);
  if (ns_cnt_stack_.size() <= 0 || OB_ISNULL(ns)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ns_cnt_stack_ not push init", K(ret), KP(ns));
  } else {
    ns_cnt_stack_[ns_cnt_stack_.size()-1]++;
    if (OB_FAIL(ns_stack_.push_back(ns))) {
      LOG_WARN("failed to push back ns", K(ret), KP(ns));
    }
  }
  return ret;
}

int ObLibXml2SaxParser::pop_namespace()
{
  INIT_SUCC(ret);
  int cur_ns_cnt = 0;
  if (ns_cnt_stack_.size() > 0) {
    if (OB_FAIL(ns_cnt_stack_.pop_back(cur_ns_cnt))) {
      LOG_WARN("failed to pop back.", K(ret), K(cur_ns_cnt));
    }
    for (int i = 0; OB_SUCC(ret) && i < cur_ns_cnt; ++i) {
      ns_stack_.pop_back();
    }
  }
  return ret;
}

int ObLibXml2SaxParser::get_namespace(const ObString& name, bool use_default_ns, ObXmlAttribute*& ns)
{
  INIT_SUCC(ret);
  for (int i = ns_stack_.size() - 1; i >= 0; --i) {
    ObXmlAttribute* cur_ns = ns_stack_[i];
    if (cur_ns->get_key().compare(name) == 0
      || (use_default_ns && name.empty() && cur_ns->get_key().compare(ObXmlConstants::XMLNS_STRING) == 0)) {
      ns = cur_ns;
      break;
    }
  }
  if(nullptr == ns && !name.empty() && name.compare(ObXmlConstants::XML_STRING) != 0) {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("non-empty prefix can not find namespace", K(ret), K(name));
  }
  return ret;
}

static int get_xml_decl_str(xmlParserCtxt* context, const ObString& xml_text, ObString& xml_decl)
{
  INIT_SUCC(ret);
  const char* ptr = xml_text.ptr();
  int32_t length = xml_text.length();
  int32_t end_pos = 0;
  if (OB_NOT_NULL(context)) {
    end_pos = context->input->cur - context->input->base + context->input->consumed;
    if (end_pos >= 0 && end_pos <= length) {
      xml_decl.assign_ptr(ptr, end_pos);
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("end_pos invalid", K(ret), KP(ptr), K(length), K(end_pos));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("context is null", K(ret), KP(ptr), K(length));
  }
  return ret;
}

int ObLibXml2SaxParser::set_xml_decl(const ObString& xml_decl_str)
{
  INIT_SUCC(ret);
  ObXmlDocument* document = document_;
  ObIAllocator* allocator = allocator_;
  ObString src_version_str;
  ObString src_encoding_str;
  ObString src_standalone_str;
  char* version_str = nullptr;
  char* encoding_str = nullptr;
  bool has_xml_decl = false;
  bool has_version_value = false;
  bool has_encoding_value = false;
  bool has_standalone_value = false;

  if (OB_NOT_NULL(document) && OB_NOT_NULL(allocator)) {
    if ((has_xml_decl = ObXmlParserUtils::has_xml_decl(xml_decl_str))) {
      document->set_has_xml_decl(has_xml_decl);
      if (OB_FAIL(ObXmlParserUtils::parse_xml_decl(xml_decl_str,
                                                   src_version_str,
                                                   has_version_value,
                                                   src_encoding_str,
                                                   has_encoding_value,
                                                   src_standalone_str,
                                                   has_standalone_value))) {
        LOG_WARN("parse_xml_decl failed", K(ret), K(has_xml_decl), K(xml_decl_str));
      } else {
        int version_length = src_version_str.length();
        int encoding_length = src_encoding_str.length();
        if (version_length > 0 && OB_ISNULL(version_str = static_cast<char*>(allocator->alloc(version_length)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc failed", K(ret), K(version_length));
        } else if (encoding_length > 0 && OB_ISNULL(encoding_str = static_cast<char*>(allocator->alloc(encoding_length)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc failed", K(ret), K(encoding_length));
        } else {
          if (version_length > 0) {
            MEMCPY(version_str, src_version_str.ptr(), version_length);
            document->set_version(ObString(version_length, version_str));
          }
          if (encoding_length > 0) {
            MEMCPY(encoding_str, src_encoding_str.ptr(), encoding_length);
            document->set_encoding(ObString(encoding_length, encoding_str));
          }
          document->set_encoding_flag(0 == encoding_length && has_encoding_value);
          document->set_standalone(ObXmlParserUtils::get_standalone_type(src_standalone_str));
        }
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("allocator or document is null", K(ret), KP(allocator), KP(document));
  }
  return ret;
}

int ObLibXml2SaxParser::start_document()
{
  INIT_SUCC(ret);
  ObString xml_decl_str;
  if (OB_FAIL(this->check())) {
    LOG_WARN("check failed", K(ret));
  } else if (OB_ISNULL(document_ = OB_NEWx(ObXmlDocument, allocator_, ObMulModeNodeType::M_DOCUMENT, ctx_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc failed", K(ret));
  } else if (OB_FAIL(document_->init())) {
    LOG_WARN("document init failed", K(ret));
  } else {
    document_->set_flags(MEMBER_LAZY_SORTED);
    if (OB_FAIL(ObXmlParserBase::start_document(document_))) {
      LOG_WARN("parser start_document failed", K(ret));
    } else if (OB_FAIL(get_xml_decl_str(ctxt_, xml_text_, xml_decl_str))) {
      LOG_WARN("get xml decl string failed", K(ret));
    } else if (OB_FAIL(this->set_xml_decl(xml_decl_str))) {
      LOG_WARN("set_xml_decl failed", K(ret));
    }
  }
  return ret;
}

int ObLibXml2SaxParser::set_element_name(ObXmlElement& element, const char* src_name)
{
  INIT_SUCC(ret);
  ObIAllocator* allocator = allocator_;
  char* elem_name = nullptr;
  int32_t elem_name_length = 0;
  ObString qname;
  ObString prefix;
  ObString localname;

  if (OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("allocator is null", K(ret));
  } else if (OB_ISNULL(src_name)) {
    // do nothin ignore
  } else if ((elem_name_length = STRLEN(src_name)) > 0) {
    if (src_name[0] == ':') {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("element-start tag is not well formed", K(ret), K(elem_name_length));
    } else if (OB_ISNULL(elem_name = static_cast<char*>(allocator->alloc(elem_name_length)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc failed", K(ret), K(elem_name_length));
    } else {
      MEMCPY(elem_name, src_name, elem_name_length);
      qname.assign_ptr(elem_name, elem_name_length);
      if (OB_FAIL(ObXmlParserUtils::get_prefix_and_localname(qname, prefix, localname))) {
        LOG_WARN("get_prefix_and_localname failed", K(ret), K(elem_name_length));
      } else {
        element.set_prefix(prefix);
        element.set_xml_key(localname);
      }
    }
  }
  return ret;
}

int ObLibXml2SaxParser::escape_xml_text(const ObString& src_attr_value, ObString &dst_attr_value)
{
  INIT_SUCC(ret);
  const char *src_value_ptr = src_attr_value.ptr();
  int src_len = src_attr_value.length();
  int dst_len = ObXmlParserUtils::get_xml_escape_str_length(src_attr_value);
  ObString attr_value;
  char *attr_value_ptr = nullptr;
  if (OB_ISNULL(attr_value_ptr = static_cast<char*>(allocator_->alloc(dst_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc failed", K(ret), K(dst_len));
  } else if (OB_FALSE_IT(attr_value.assign_buffer(attr_value_ptr, dst_len))) {
  } else if (OB_FAIL(ObXmlParserUtils::escape_xml_text(src_attr_value, attr_value))) {
    LOG_WARN("escape_xml_text failed", K(ret), K(src_attr_value));
  } else {
    dst_attr_value.assign_ptr(attr_value_ptr, dst_len);
  }
  return ret;
}

int ObLibXml2SaxParser::construct_text_value(const ObString &src_attr_value, ObString &attr_value)
{
  INIT_SUCC(ret);
  char *attr_value_ptr = nullptr;
  int attr_value_len = 0;
  if (! is_entity_replace()) {
    if (OB_FAIL(escape_xml_text(src_attr_value, attr_value))) {
      LOG_WARN("escape_attr_value failed", K(ret), K(src_attr_value));
    }
  } else {
    attr_value_len = src_attr_value.length();
    if (OB_ISNULL(attr_value_ptr = static_cast<char*>(allocator_->alloc(attr_value_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc failed", K(ret), K(attr_value_len));
    } else {
      MEMCPY(attr_value_ptr, src_attr_value.ptr(), attr_value_len);
      attr_value.assign_ptr(attr_value_ptr, attr_value_len);
    }
  }
  return ret;
}

int ObLibXml2SaxParser::add_element_attr(ObXmlElement& element, const char* src_attr_name, const char* src_attr_value)
{
  INIT_SUCC(ret);
  ObIAllocator* allocator = allocator_;
  ObXmlAttribute* attr = nullptr;
  char* attr_name = nullptr;
  int64_t attr_name_length = 0;
  int64_t attr_value_length = 0;
  ObString src_attr_value_str;
  ObString attr_value;
  ObString qname;
  ObString prefix;
  ObString localname;

  if (OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("allocator is null", K(ret));
  } else if (OB_ISNULL(attr = OB_NEWx(ObXmlAttribute, allocator, ObMulModeNodeType::M_ATTRIBUTE, ctx_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc failed", K(ret));
  } else {
    attr_name_length = src_attr_name == nullptr ? 0 : STRLEN(src_attr_name);
    attr_value_length = src_attr_value == nullptr ? 0 : STRLEN(src_attr_value);
    src_attr_value_str.assign_ptr(src_attr_value, attr_value_length);

    if (OB_SUCC(ret) && attr_name_length > 0) {
      if (src_attr_name[0] == ':') {
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("element-start tag is not well formed", K(ret), KCSTRING(src_attr_name));
      } else if (OB_ISNULL(attr_name = static_cast<char*>(allocator->alloc(attr_name_length)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc failed", K(ret), K(attr_name_length));
      } else {
        MEMCPY(attr_name, src_attr_name, attr_name_length);
        qname.assign_ptr(attr_name, attr_name_length);
        if (OB_FAIL(ObXmlParserUtils::get_prefix_and_localname(qname, prefix, localname))) {
          LOG_WARN("get_prefix_and_localname failed", K(ret), K(attr_name_length), KCSTRING(src_attr_name));
        } else {
          attr->set_prefix(prefix);
          attr->set_xml_key(localname);
        }
      }
    }

    if (OB_SUCC(ret) && attr_value_length > 0) {
      if (OB_FAIL(construct_text_value(src_attr_value_str, attr_value))) {
        LOG_WARN("construct_text_value failed", K(ret), K(src_attr_value_str));
      } else {
        attr->set_value(attr_value);
      }
    }

    if (OB_SUCC(ret)) {
      if (qname.compare("xmlns:") == 0) {
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("ns is invalid", K(ret), KPC(attr), K(attr->get_prefix()));
      } else if (ObXmlParserUtils::is_namespace_attribute(attr)) {
        attr->set_xml_type(ObMulModeNodeType::M_NAMESPACE);
        if (this->is_document_parse() && attr_value.empty() && !prefix.empty()) {
          ret = OB_ERR_PARSER_SYNTAX;
          LOG_WARN("attr_value is empty", K(ret), K(attr_value));
        } else if (!prefix.empty() && (localname.compare("xml") == 0 || (localname.compare("xmlns") == 0))) {
          // "xml" and "xmlns" are reserved words and their use is prohibited
          ret = OB_ERR_PARSER_SYNTAX;
          LOG_WARN("ns is invalid", K(ret), KPC(attr), K(localname));
        } else if (OB_FAIL(element.add_attribute(attr))) {
          LOG_WARN("add_attribute failed", K(ret));
        } else if (OB_FAIL(this->push_namespace(attr))) {
          LOG_WARN("push_namespace failed", K(ret));
        }
      } else if (OB_FAIL(element.add_attribute(attr))) {
        LOG_WARN("add_attribute failed", K(ret));
      }
    }
  }
  return ret;
}

int ObLibXml2SaxParser::set_element_namespace(ObXmlElement& element) {
  INIT_SUCC(ret);
  ObXmlAttribute* elem_ns = nullptr;
  ObXmlAttribute* attr_ns = nullptr;
  ObLibContainerNode* attributes = nullptr;

  if (OB_FAIL(this->get_namespace(element.get_prefix(), true, elem_ns))) {
    LOG_WARN("get element namespace failed", K(ret), K(element.get_prefix()));
  } else {
    element.set_ns(elem_ns);
    int attr_size = element.attribute_size();
    for (int i = 0; OB_SUCC(ret) && i < attr_size; ++i) {
      ObXmlAttribute* attr = NULL;
      if (OB_FAIL(element.get_attribute(attr, i))) {
        LOG_WARN("get attribute failed", K(ret), K(i));
      } else if (OB_ISNULL(attr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get attr", K(ret));
      } else if (attr->type() == ObMulModeNodeType::M_ATTRIBUTE) {
        if (OB_FAIL(this->get_namespace(attr->get_prefix(), false, attr_ns))) {
          LOG_WARN("get attribute namespace failed", K(ret), K(i));
        } else {
          attr->set_ns(attr_ns);
          attr_ns = nullptr;
        }
      }
    }
  }
  return ret;
}

int ObLibXml2SaxParser::start_element(const char* name, const char** attrs)
{
  INIT_SUCC(ret);
  ObXmlElement* element = nullptr;
  if (OB_FAIL(this->check())) {
    LOG_WARN("check failed", K(ret));
  } else if (OB_ISNULL(element = OB_NEWx(ObXmlElement, allocator_, ObMulModeNodeType::M_ELEMENT, ctx_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc failed", K(ret));
  } else if (OB_FAIL(element->init())) {
    LOG_WARN("element init failed", K(ret));
  } else if (FALSE_IT(element->set_flags(MEMBER_LAZY_SORTED))) {
  } else if(OB_FAIL(ns_cnt_stack_.push_back(0))) {
    LOG_WARN("ns_cnt_stack_ current ns cnt init failed", K(ret));
  } else if (OB_FAIL(this->set_element_name(*element, name))) {
    LOG_WARN("set_element_name failed", K(ret));
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(attrs)) {
    for (const char** cur = attrs; OB_SUCC(ret) && cur && *cur; cur += 2) {
      if (OB_FAIL(this->add_element_attr(*element, *cur, *(cur + 1)))) {
        LOG_WARN("add_element_attr failed", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(this->set_element_namespace(*element))) {
      LOG_WARN("parser set_element_namespace failed", K(ret));
    } else if (OB_FAIL(ObXmlParserBase::start_element(element))) {
      LOG_WARN("parser start_element failed", K(ret));
    }
  }

  return ret;
}

static bool is_empty_element_tag(xmlParserCtxt* ctxt)
{
  bool res = false;
  if (OB_NOT_NULL(ctxt->input->cur) && ctxt->input->cur - ctxt->input->base > 2) {
    const xmlChar* c1 = ctxt->input->cur - 1;
    const xmlChar* c2 = ctxt->input->cur - 2;
    if (*c1 == '>' && *c2 == '/') {
      res = true;
    }
  }
  return res;
}

int ObLibXml2SaxParser::end_element()
{
  INIT_SUCC(ret);
  ObXmlElement* element = nullptr;
  if (OB_FAIL(this->check())) {
    LOG_WARN("check failed", K(ret));
  } else if (OB_ISNULL(element =
                ObXmlUtil::xml_node_cast<ObXmlElement>(cur_node_, ObMulModeNodeType::M_ELEMENT))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("end element current node not element", K(ret), K(cur_node_->type()));
  } else {
    element->set_empty(is_empty_element_tag(ctxt_));
    if (OB_FAIL(ObXmlParserBase::end_element())) {
      LOG_WARN("parser end_element failed", K(ret));
    } else if (OB_FAIL(this->pop_namespace())) {
      LOG_WARN("pop_namespace failed", K(ret));
    }
  }
  return ret;
}

int ObLibXml2SaxParser::alloc_text_node(ObMulModeNodeType type,
                                        const char* src_value,
                                        int value_len,
                                        ObXmlText*& node)
{
  INIT_SUCC(ret);
  char* str = nullptr;
  if (OB_FAIL(this->check())) {
    LOG_WARN("check failed", K(ret));
  } else if (OB_ISNULL(node = OB_NEWx(ObXmlText, allocator_, type, ctx_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc failed", K(ret));
  } else if (value_len > 0) {
    if (OB_ISNULL(str = static_cast<char*>(allocator_->alloc(value_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc failed", K(ret), K(value_len));
    } else {
      MEMCPY(str, src_value, value_len);
      node->set_value(ObString(value_len, str));
    }
  }
  return ret;
}

int ObLibXml2SaxParser::add_text_node(ObMulModeNodeType type, const char* value, int len)
{
  INIT_SUCC(ret);
  ObXmlText* cdata = nullptr;
  if (OB_FAIL(this->check())) {
    LOG_WARN("check failed", K(ret));
  } else if (OB_FAIL(this->alloc_text_node(type,
                                           value,
                                           len,
                                           cdata))) {
    LOG_WARN("alloc_text_node failed", K(ret));
  } else if (OB_FAIL(ObXmlParserBase::add_text_node(cdata))) {
    LOG_WARN("cdata_block failed", K(ret));
  }
  return ret;
}

static bool is_char_entity_ref(xmlParserCtxt* ctxt, ObString& ref)
{
  bool res = false;
  const xmlChar* cur = ctxt->input->cur - 1;
  const xmlChar* base = ctxt->input->base;
  // char entity ref max length
  const int MAX_CHAR_REF_LENGTH = 20;
  int len = 0;
  if (cur > base && *cur == ';') {
    --cur;
    ++len;
    while (cur > base && *cur != '&' && *cur !=';' && len < MAX_CHAR_REF_LENGTH) {
      --cur;
      ++len;
    }
    if (cur >= base && *cur == '&') {
      ref.assign_ptr(reinterpret_cast<const char*>(cur), len+1);
      res = true;
    }
  }
  return res;
}

int ObLibXml2SaxParser::characters(const char *ch, int len)
{
  INIT_SUCC(ret);
  ObString data;
  if (OB_FAIL(this->check())) {
    LOG_WARN("check failed", K(ret));
  } else {
    // libxml2 will replace entity ref, so if need replace, just copy.
    // when parse characters in libxml2, if is plain text, ctxt_->input->cur points same with ch.
    // if is predefined entity ref or char ref, ctxt_->input->cur points the end of ref
    if (! is_entity_replace()
        && reinterpret_cast<const char*>(ctxt_->input->cur) != ch
        && is_char_entity_ref(ctxt_, data)) {
      LOG_DEBUG("replace character with entity", K(ObString(len, ch)), K(data));
    } else {
      data.assign_ptr(ch, len);
    }
    if (OB_FAIL(this->add_or_merge_text(data))) {
      LOG_WARN("parser add or merge text failed", K(ret), K(data));
    }
  }
  return ret;
}

int ObLibXml2SaxParser::processing_instruction(const ObString& target, const ObString& data)
{
  INIT_SUCC(ret);
  ObXmlAttribute* pi = nullptr;
  const char *src_target = target.ptr();
  const char *src_data = data.ptr();
  int name_len = target.length();
  int value_len = data.length();
  char* name = nullptr;
  char* value = nullptr;

  if (OB_FAIL(this->check())) {
    LOG_WARN("check failed", K(ret));
  } else if (OB_ISNULL(pi = OB_NEWx(ObXmlAttribute, allocator_, ObMulModeNodeType::M_INSTRUCT, ctx_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc failed", K(ret));
  } else if (name_len > 0 && OB_ISNULL(name = static_cast<char*>(allocator_->alloc(name_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc failed", K(ret), K(name_len));
  } else if (value_len > 0 && OB_ISNULL(value = static_cast<char*>(allocator_->alloc(value_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc failed", K(ret), K(value_len));
  } else {
    if (name_len > 0)MEMCPY(name, src_target, name_len);
    if (value_len > 0)MEMCPY(value, src_data, value_len);
    pi->set_xml_key(ObString(name_len, name));
    pi->set_value(ObString(value_len, value));
    if (OB_FAIL(ObXmlParserBase::processing_instruction(pi))) {
      LOG_WARN("processing_instruction failed", K(ret));
    }
  }
  return ret;
}

int ObLibXml2SaxParser::get_parse_byte_num()
{
  int num = 0;
  if (OB_ISNULL(ctxt_)) {
  } else {
    num = ctxt_->input->cur - ctxt_->input->base + ctxt_->input->consumed;
  }
  return num;
}

bool ObLibXml2SaxParser::is_parsed_all_input()
{
  return get_parse_byte_num() == xml_text_.length();
}

// ObLibXml2SaxParser end

} // namespace common
} // namespace oceanbase
