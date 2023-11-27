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

#ifndef OCEANBASE_XML_PARSER_H_
#define OCEANBASE_XML_PARSER_H_
#include "lib/xml/ob_xml_tree.h"
#include "lib/string/ob_string_buffer.h"

struct _xmlParserCtxt;
struct _xmlSAXHandler;

namespace oceanbase {
namespace common {

enum ObXmlParserOption {
    OB_XML_PARSE_RECOVER	= 1<<1,	/* continue parse whenever some error occur */
    OB_XML_PARSE_NOT_IGNORE_SPACE	= 1<<2,
    OB_XML_PARSE_SYNTAX_CHECK	= 1<<3,
    OB_XML_PARSE_CONTENT_ALLOW_XML_DECL = 1 << 4,
    OB_XML_PARSE_CONTAINER_LAZY_SORT = 1 << 5,
    OB_XML_PARSE_NOT_ENTITY_REPLACE = 1 << 6,
};


// ObXmlParserBase currently used for constructing xml tree and some common functions.
// If having same complex logic, please first add to ObLibXml2SaxParser
class ObXmlParserBase {
public:
  static constexpr int OB_XML_PARSER_MAX_DEPTH = 1000;

  static constexpr char OB_XML_PREDEFINED_ENTITY_LT_SYMBOL = '<';
  static constexpr const char* const OB_XML_PREDEFINED_ENTITY_LT = "&lt;";
  static constexpr int OB_XML_PREDEFINED_ENTITY_LT_LEN = 1 + 2 + 1;
  static constexpr char OB_XML_PREDEFINED_ENTITY_GT_SYMBOL = '>';
  static constexpr const char* const OB_XML_PREDEFINED_ENTITY_GT = "&gt;";
  static constexpr int OB_XML_PREDEFINED_ENTITY_GT_LEN = 1 + 2 + 1;
  static constexpr char OB_XML_PREDEFINED_ENTITY_QUOT_SYMBOL = '"';
  static constexpr const char* const OB_XML_PREDEFINED_ENTITY_QUOT = "&quot;";
  static constexpr int OB_XML_PREDEFINED_ENTITY_QUOT_LEN = 1 + 4 + 1;
  static constexpr char OB_XML_PREDEFINED_ENTITY_APOS_SYMBOL = '\'';
  static constexpr const char* const OB_XML_PREDEFINED_ENTITY_APOS = "&apos;";
  static constexpr int OB_XML_PREDEFINED_ENTITY_APOS_LEN = 1 + 4 + 1;
  static constexpr char OB_XML_PREDEFINED_ENTITY_AMP_SYMBOL = '&';
  static constexpr const char* const OB_XML_PREDEFINED_ENTITY_AMP = "&amp;";
  static constexpr int OB_XML_PREDEFINED_ENTITY_AMP_LEN = 1 + 3 + 1;

  ObXmlParserBase(ObMulModeMemCtx* ctx):
              allocator_(ctx->allocator_),
              document_(nullptr),
              cur_node_(nullptr),
              depth_(0),
              ctx_(ctx)
  {
  }
  virtual ~ObXmlParserBase() {}

  ObIAllocator* get_allocator() {return allocator_;}
  virtual int parse_document(const ObString& xml_text) = 0;
  virtual int parse_content(const ObString& xml_text) = 0;

  virtual int start_document(ObXmlDocument* node);

  virtual int end_document();
  virtual int start_element(ObXmlElement* node);
  virtual int end_element();

  int add_or_merge_text(const ObString& text);
  int remove_prev_empty_text();
  virtual int add_text_node(ObXmlText* node);

  virtual int comment(ObXmlText* node);

  virtual int processing_instruction(ObXmlAttribute* node);
  virtual int cdata_block(ObXmlText* node);

  ObXmlDocument* document() {return document_;}
  void set_cur_node(ObXmlNode* node) {cur_node_ = node;}
  ObXmlNode* cur_node() {return cur_node_;}

  bool reach_max_depth();

  void set_only_syntax_check() {
    options_ = options_ | OB_XML_PARSE_SYNTAX_CHECK;
  }
  bool is_only_syntax_check() {
    return (options_ & OB_XML_PARSE_SYNTAX_CHECK) != 0;
  }

  bool is_ignore_space() {
    return (options_ & OB_XML_PARSE_NOT_IGNORE_SPACE) == 0;
  }
  void set_not_ignore_space() {
    options_ = options_ | OB_XML_PARSE_NOT_IGNORE_SPACE;
  }

  bool is_recover_mode() {
    return (options_ & OB_XML_PARSE_RECOVER) != 0;
  }
  void set_recover_mode() {
    options_ = options_ | OB_XML_PARSE_RECOVER;
  }

  void set_member_sort_policy() {
    options_ |= OB_XML_PARSE_CONTAINER_LAZY_SORT;
  }

  bool is_member_sort_lazy() {
    return (options_ & OB_XML_PARSE_CONTAINER_LAZY_SORT) != 0 ;
  }

  bool is_content_allow_xml_decl() {
    return (options_ & OB_XML_PARSE_CONTENT_ALLOW_XML_DECL) != 0;
  }
  void set_content_allow_xml_decl() {
    options_ = options_ | OB_XML_PARSE_CONTENT_ALLOW_XML_DECL;
  }

  bool is_entity_replace() {
    return (options_ & OB_XML_PARSE_NOT_ENTITY_REPLACE) == 0;
  }
  void set_not_entity_replace() {
    options_ = options_ | OB_XML_PARSE_NOT_ENTITY_REPLACE;
  }

  bool is_document_parse() {return document_ != nullptr && ObMulModeNodeType::M_DOCUMENT == document_->type();}
  bool is_content_parse() {return document_ == nullptr || ObMulModeNodeType::M_CONTENT == document_->type();}

protected:
  ObXmlNode* get_last_child(ObXmlNode* cur_node);
  ObXmlNode* get_first_child(ObXmlNode* cur_node);

protected:
  ObIAllocator* allocator_ = nullptr;
  ObXmlDocument* document_ = nullptr;
  ObXmlNode* cur_node_ = nullptr;
  int depth_ = 0;
  int64_t options_ = 0;
  ObMulModeMemCtx* ctx_ = nullptr;
};


// should use ObXmlParser, not this class
// the impl of ObLibXml2SaxParser is in ob_libxml2_sax_handler.cpp
// to aviod importing to much libxml2 header files
class ObLibXml2SaxParser : public ObXmlParserBase {
public:

  ObLibXml2SaxParser(ObMulModeMemCtx* ctx):
                     ObXmlParserBase(ctx),
                     ctxt_(nullptr),
                     ns_cnt_stack_(),
                     ns_stack_() {}
  virtual ~ObLibXml2SaxParser();

  virtual int parse_document(const ObString& xml_text);
  virtual int parse_content(const ObString& xml_text);

  int add_text_node(ObMulModeNodeType type, const char* value, int len);

  int start_document();
  int start_element(const char* name, const char** attrs);
  int end_element();
  int characters(const char *ch, int len);
  int processing_instruction(const ObString& target, const ObString& value);

  // for error handling
  void stop_parse(int code);
  void set_stop_parse(bool val) {stop_parse_ = val;}
  bool is_stop_parse() {return stop_parse_;}
  int on_error(int code);
  void set_errno(int code) {errno_ = code;}
  int get_last_errno() {return errno_;}

  _xmlParserCtxt* get_libxml2_ctxt() {return ctxt_;}

private:
  int init(const ObString& xml_text, bool skip_start_blank);
  int init_parse_context();
  int init_xml_text(const ObString& xml_text, bool skip_start_blank);
  int check();

  int alloc_text_node(ObMulModeNodeType type,
                      const char* src_value,
                      int len,
                      ObXmlText*& node);
  int escape_xml_text(const ObString &src_attr_value, ObString &dst_attr_value);
  int construct_text_value(const ObString &src_attr_value, ObString &attr_value);

  // helper method
  int set_xml_decl(const ObString& xml_decl_str);
  int set_element_name(ObXmlElement& element, const char* src_name);
  int add_element_attr(ObXmlElement& element, const char* src_attr_name, const char* src_attr_value);

  // for namespace
  int push_namespace(ObXmlAttribute* ns);
  int pop_namespace();
  int get_namespace(const ObString& name, bool use_default_ns, ObXmlAttribute*& ns);
  int set_element_namespace(ObXmlElement& element);

  int to_ob_xml_errno(int code);

  int get_parse_byte_num();
  bool is_parsed_all_input();

private:
  int errno_ = 0;
  bool stop_parse_ = false;

  ObString xml_text_;

  _xmlParserCtxt* ctxt_ = nullptr;
  _xmlSAXHandler* old_sax_ = nullptr;

  ObArray<int> ns_cnt_stack_;
  ObArray<ObXmlAttribute*> ns_stack_;
  DISALLOW_COPY_AND_ASSIGN(ObLibXml2SaxParser);
};
// use ObXmlParser for parser
typedef ObLibXml2SaxParser ObXmlParser;


class ObXmlParserUtils {
public:

  // just for simple situation, if use complex, use ObXmlParser directly
  // and may return null when xml_text is empty or whitespace
  static int parse_document_text(ObMulModeMemCtx* ctx, const ObString& xml_text, ObXmlDocument*&node, int64_t option = 0);
  static int parse_content_text(ObMulModeMemCtx* ctx, const ObString& xml_text, ObXmlDocument*&node, int64_t option = 0);
  // has <?xml ...?> decl
  static bool has_xml_decl(const ObString& xml_text);
  static int parse_xml_decl_encoding(const ObString& xml_decl, bool &has_decl, ObString& encoding_str);
  static int parse_xml_decl(const ObString& xml_decl,
                            ObString& version,
                            bool &has_version_value,
                            ObString& encoding,
                            bool &has_encoding_value,
                            ObString& standalone,
                            bool &has_standalone_value);

  static ObXmlStandaloneType get_standalone_type(const ObString& src_standalone_str);


  // escape character to predefine entity
  // just append to dst, don't clear
  static int escape_xml_text(const ObString &src, ObStringBuffer &dst);
  // use ObString as buffer, caller need ensure dst have enough memory,
  // or return size overflow error
  static int escape_xml_text(const ObString &src, ObString &dst);
  static int get_xml_escape_char_length(const char c);
  static int get_xml_escape_str_length(const ObString &str);

  // just set ptr, no content copy
  static int check_local_name_legality(const ObString& localname);
  static int get_prefix_and_localname(const ObString& qname, ObString& prefix, ObString& localname);
  static bool is_namespace_attribute(ObXmlAttribute* attr);
  static bool is_entity_ref(ObString &input_str, int64_t index, ObString &ref, int64_t &ref_len);
};

} // namespace common
} // namespace oceanbase

#endif  //OCEANBASE_XML_PARSER_H_