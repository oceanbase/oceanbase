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

#include "lib/xml/ob_xml_parser.h"
#include "lib/xml/ob_xml_util.h"
#include "lib/ob_errno.h"
#include "lib/ob_define.h"
#include "libxml2/libxml/parserInternals.h"
#include <rapidjson/encodings.h>
#include <rapidjson/memorystream.h>

namespace oceanbase {
namespace common {

static inline bool is_blank_str(const ObString& text)
{
  bool res = true;
  const char* str = text.ptr();
  for (int i = 0; i < text.length() && res == true; ++i) {
    if (!isspace(str[i])) {
      res = false;
    }
  }
  return res;
}

// ObXmlParserBase

ObXmlNode* ObXmlParserBase::get_last_child(ObXmlNode* cur_node)
{

  ObXmlNode* last_child = nullptr;
  if (OB_NOT_NULL(cur_node) && cur_node->size() > 0) {
    last_child = cur_node->at(cur_node->size() - 1);
  }
  return last_child;
}
ObXmlNode* ObXmlParserBase::get_first_child(ObXmlNode* cur_node)
{

  ObXmlNode* first_child = nullptr;
  if (OB_NOT_NULL(cur_node) && cur_node->size() > 0) {
    first_child = cur_node->at(0);
  }
  return first_child;
}

// if last child of current node  is text, then merge
int ObXmlParserBase::add_or_merge_text(const ObString& text)
{
  INIT_SUCC(ret);
  ObIAllocator* allocator = nullptr;
  ObXmlNode* last_child = nullptr;
  ObXmlText* text_node = nullptr;
  char* str = nullptr;

  if (OB_UNLIKELY(text.length() <= 0)) {
    // empty string, do nothing
  } else if (OB_ISNULL(allocator = this->get_allocator())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", K(ret));
  } else {
    last_child = get_last_child(cur_node_);
    if (OB_NOT_NULL(text_node = ObXmlUtil::xml_node_cast<ObXmlText>(last_child, ObMulModeNodeType::M_TEXT))) {
      // merge text node
      ObString text_value;
      text_node->get_value(text_value);
      char* old_str = text_value.ptr();
      int64_t old_len = text_value.length();
      int64_t new_len = old_len + text.length();
      if (OB_ISNULL(str = static_cast<char*>(allocator->alloc(new_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc failed", K(ret), K(text.length()), K(old_len));
      } else {
        MEMCPY(str, old_str, old_len);
        MEMCPY(str + old_len, text.ptr(), text.length());
        allocator->free(reinterpret_cast<void*>(old_str));
        old_str = nullptr;
        text_node->set_value(ObString(new_len, str));
      }
    } else {
      if (OB_ISNULL(text_node = OB_NEWx(ObXmlText, allocator, ObMulModeNodeType::M_TEXT, ctx_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc failed", K(ret));
      } else if (OB_ISNULL(str = static_cast<char*>(allocator->alloc(text.length())))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc failed", K(ret), K(text.length()));
      } else {
        MEMCPY(str, text.ptr(), text.length());
        text_node->set_value(ObString(text.length(), str));
        if (OB_FAIL(this->add_text_node(text_node))) {
          LOG_WARN("parser characters failed", K(ret));
        }
      }
    }
  }
  return ret;
}

// remove prev slibing node if is empty text
// and no contine plain text node when parse
int ObXmlParserBase::remove_prev_empty_text()
{
  INIT_SUCC(ret);
  ObXmlText* text_node = nullptr;
  if (! is_ignore_space()) {
  } else if (OB_NOT_NULL(text_node =
      ObXmlUtil::xml_node_cast<ObXmlText>(get_last_child(cur_node_),
      ObMulModeNodeType::M_TEXT))) {
    // remove first alone empty text node if necessary
    ObString text_value;
    if (OB_FAIL(text_node->get_value(text_value))) {
      LOG_WARN("get text value failed", K(ret));
    } else if (is_blank_str(text_value)) {
      if (OB_FAIL(cur_node_->remove(cur_node_->size() - 1))) {
        LOG_WARN("remove last empty text child failed", K(ret));
      } else {
        // ignore blank string
        text_node->set_value(ObString());
        allocator_->free(reinterpret_cast<void*>(text_value.ptr()));
        text_node->~ObXmlText();
        allocator_->free(text_node);
      }
    }
  }
  return ret;
}

// currently M_TEXT text node will be merge before
// and can not happen there are two continuely M_TEXT type text node
int ObXmlParserBase::add_text_node(ObXmlText* node)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(cur_node_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current node is null", K(ret));
  } else if (OB_FAIL(remove_prev_empty_text())) {
    LOG_WARN("remove_prev_empty_text fail", K(ret));
  } else if (OB_FAIL(cur_node_->append(node))) {
    LOG_WARN("add child failed", K(ret));
  }
  return ret;
}

int ObXmlParserBase::comment(ObXmlText* node)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(cur_node_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current node is null", K(ret));
  } else if (OB_FAIL(remove_prev_empty_text())) {
    LOG_WARN("remove_prev_empty_text fail", K(ret));
  } else if (OB_FAIL(cur_node_->append(node))) {
    LOG_WARN("add child failed", K(ret));
  }
  return ret;
}

int ObXmlParserBase::processing_instruction(ObXmlAttribute* node)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(cur_node_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current node is null", K(ret));
  } else if (OB_FAIL(remove_prev_empty_text())) {
    LOG_WARN("remove_prev_empty_text fail", K(ret));
  } else if (OB_FAIL(cur_node_->append(node))) {
    LOG_WARN("add child failed", K(ret));
  }
  return ret;
}

int ObXmlParserBase::cdata_block(ObXmlText* node)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(cur_node_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current node is null", K(ret));
  } else if (OB_FAIL(remove_prev_empty_text())) {
    LOG_WARN("remove_prev_empty_text fail", K(ret));
  } else if (OB_FAIL(cur_node_->append(node))) {
    LOG_WARN("add child failed", K(ret));
  }
  return ret;
}

int ObXmlParserBase::start_document(ObXmlDocument* node)
{
  INIT_SUCC(ret);
  this->document_ = node;
  this->cur_node_ = node;
  return ret;
}

int ObXmlParserBase::end_document()
{
  INIT_SUCC(ret);
  if (OB_ISNULL(cur_node_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current node is null", K(ret));
  } else if (OB_FAIL(remove_prev_empty_text())) {
    LOG_WARN("remove_prev_empty_text fail", K(ret));
  }
  return ret;
}

bool ObXmlParserBase::reach_max_depth()
{
  return depth_ > OB_XML_PARSER_MAX_DEPTH;
}

int ObXmlParserBase::start_element(ObXmlElement* node)
{
  INIT_SUCC(ret);
  if (reach_max_depth()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("reach max parse depth", K(ret), K(depth_));
  } else if (OB_ISNULL(cur_node_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current node is null", K(ret));
  } else if (OB_FAIL(remove_prev_empty_text())) {
    LOG_WARN("remove_prev_empty_text fail", K(ret));
  } else if (OB_FAIL(cur_node_->append(node))) {
    LOG_WARN("add child failed", K(ret));
  } else {
    ++depth_;
    cur_node_ = node;
  }
  return ret;
}

int ObXmlParserBase::end_element()
{
  INIT_SUCC(ret);
  if (OB_NOT_NULL(cur_node_)) {
    if (cur_node_->size() > 1 && OB_FAIL(remove_prev_empty_text())) {
      LOG_WARN("remove_prev_empty_text failed", K(ret));
    } else {
      --depth_;
      cur_node_ = cur_node_->get_parent();
    }
  }
  return ret;
}

// ObXmlParserBase end


// ObXmlParserUtils
int ObXmlParserUtils::parse_document_text(ObMulModeMemCtx* ctx, const ObString& xml_text, ObXmlDocument*&node, int64_t option)
{
  INIT_SUCC(ret);
  ObXmlParser parser(ctx);

  if (OB_FAIL(parser.parse_document(xml_text))) {
    LOG_WARN("fail to parse document", K(ret), K(xml_text));
  } else {
    node = parser.document();
    if (OB_ISNULL(node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("node can not be null", K(ret));
    } else if (!(option & OB_XML_PARSE_CONTAINER_LAZY_SORT)) {
      if (OB_FAIL(node->alter_member_sort_policy(true))) {
        LOG_WARN("fail to sort child element", K(ret));
      }
    }
  }
  return ret;
}

int ObXmlParserUtils::parse_content_text(ObMulModeMemCtx* ctx, const ObString& xml_text, ObXmlDocument*&node, int64_t option)
{
  INIT_SUCC(ret);
  ObXmlParser parser(ctx);

  if (OB_FAIL(parser.parse_content(xml_text))) {
    LOG_WARN("fail to parse document", K(ret), K(xml_text));
  } else {
    node = parser.document();
    if (OB_ISNULL(node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("node can not be null", K(ret));
    } else if (!(option & OB_XML_PARSE_CONTAINER_LAZY_SORT)) {
      if (OB_FAIL(node->alter_member_sort_policy(true))) {
        LOG_WARN("fail to sort child element", K(ret));
      }
    }
  }
  return ret;
}



int ObXmlParserUtils::get_xml_escape_char_length(const char c)
{
  int len = 0;
  switch (c) {
    case ObXmlParserBase::OB_XML_PREDEFINED_ENTITY_AMP_SYMBOL : {
      len = ObXmlParserBase::OB_XML_PREDEFINED_ENTITY_AMP_LEN; //"&amp;
      break;
    }
    case ObXmlParserBase::OB_XML_PREDEFINED_ENTITY_LT_SYMBOL : {
      len = ObXmlParserBase::OB_XML_PREDEFINED_ENTITY_LT_LEN; //"&lt;
      break;
    }
    case ObXmlParserBase::OB_XML_PREDEFINED_ENTITY_GT_SYMBOL: {
      len = ObXmlParserBase::OB_XML_PREDEFINED_ENTITY_GT_LEN; //"&gt;
      break;
    }
    case ObXmlParserBase::OB_XML_PREDEFINED_ENTITY_QUOT_SYMBOL: {
      len = ObXmlParserBase::OB_XML_PREDEFINED_ENTITY_QUOT_LEN; //"&quot;
      break;
    }
    case ObXmlParserBase::OB_XML_PREDEFINED_ENTITY_APOS_SYMBOL : {
      len = ObXmlParserBase::OB_XML_PREDEFINED_ENTITY_APOS_LEN; //"&apos;
      break;
    }
    default : {
      len = 1;
      break;
    }
  }
  return len;
}

int ObXmlParserUtils::get_xml_escape_str_length(const ObString &str)
{
  const char *ptr = str.ptr();
  int len = str.length();
  int res_len = 0;
  for (int i = 0; i < len; i++) {
    res_len += get_xml_escape_char_length(ptr[i]);
  }
  return res_len;
}

int ObXmlParserUtils::escape_xml_text(const ObString &src, ObStringBuffer &dst)
{
  INIT_SUCC(ret);
  const char *ptr = src.ptr();
  for (int i = 0; i < src.length() && OB_SUCC(ret); i++) {
    switch (*ptr) {
      case ObXmlParserBase::OB_XML_PREDEFINED_ENTITY_AMP_SYMBOL : {
        if (OB_FAIL(dst.append(ObXmlParserBase::OB_XML_PREDEFINED_ENTITY_AMP))) {
          LOG_WARN("append char failed", K(ret));
        }
        break;
      }
      case ObXmlParserBase::OB_XML_PREDEFINED_ENTITY_LT_SYMBOL : {
        if (OB_FAIL(dst.append(ObXmlParserBase::OB_XML_PREDEFINED_ENTITY_LT))) {
          LOG_WARN("append char failed", K(ret));
        }
        break;
      }
      case ObXmlParserBase::OB_XML_PREDEFINED_ENTITY_GT_SYMBOL : {
        if (OB_FAIL(dst.append(ObXmlParserBase::OB_XML_PREDEFINED_ENTITY_GT))) {
          LOG_WARN("append char failed", K(ret));
        }
        break;
      }
      case ObXmlParserBase::OB_XML_PREDEFINED_ENTITY_QUOT_SYMBOL : {
        if (OB_FAIL(dst.append(ObXmlParserBase::OB_XML_PREDEFINED_ENTITY_QUOT))) {
          LOG_WARN("append char failed", K(ret));
        }
        break;
      }
      case ObXmlParserBase::OB_XML_PREDEFINED_ENTITY_APOS_SYMBOL : {
        if (OB_FAIL(dst.append(ObXmlParserBase::OB_XML_PREDEFINED_ENTITY_APOS))) {
          LOG_WARN("append char failed", K(ret));
        }
        break;
      }
      default : {
        if (OB_FAIL(dst.append(ptr, 1))) {
          LOG_WARN("append char failed", K(ret));
        }
        break;
      }
    }
    ptr++;
  }
  return ret;
}

int ObXmlParserUtils::escape_xml_text(const ObString &src, ObString &dst)
{
  INIT_SUCC(ret);
  const char *ptr = src.ptr();
  int len = src.length();
  for (int i = 0; i < len && OB_SUCC(ret); i++) {
    const char c = ptr[i];
    switch (c) {
      case ObXmlParserBase::OB_XML_PREDEFINED_ENTITY_AMP_SYMBOL : {
        if (OB_UNLIKELY(ObXmlParserBase::OB_XML_PREDEFINED_ENTITY_AMP_LEN != dst.write(
            ObXmlParserBase::OB_XML_PREDEFINED_ENTITY_AMP,
            ObXmlParserBase::OB_XML_PREDEFINED_ENTITY_AMP_LEN))) {
          ret = OB_SIZE_OVERFLOW;
          LOG_WARN("write amp char failed", K(ret), K(c));
        }
        break;
      }
      case ObXmlParserBase::OB_XML_PREDEFINED_ENTITY_LT_SYMBOL : {
        if (OB_UNLIKELY(ObXmlParserBase::OB_XML_PREDEFINED_ENTITY_LT_LEN != dst.write(
            ObXmlParserBase::OB_XML_PREDEFINED_ENTITY_LT,
            ObXmlParserBase::OB_XML_PREDEFINED_ENTITY_LT_LEN))) {
          ret = OB_SIZE_OVERFLOW;
          LOG_WARN("write lt char failed", K(ret), K(c));
        }
        break;
      }
      case ObXmlParserBase::OB_XML_PREDEFINED_ENTITY_GT_SYMBOL : {
        if (OB_UNLIKELY(ObXmlParserBase::OB_XML_PREDEFINED_ENTITY_GT_LEN != dst.write(
            ObXmlParserBase::OB_XML_PREDEFINED_ENTITY_GT,
            ObXmlParserBase::OB_XML_PREDEFINED_ENTITY_GT_LEN))) {
          ret = OB_SIZE_OVERFLOW;
          LOG_WARN("write gt char failed", K(ret), K(c));
        }
        break;
      }
      case ObXmlParserBase::OB_XML_PREDEFINED_ENTITY_QUOT_SYMBOL : {
        if (OB_UNLIKELY(ObXmlParserBase::OB_XML_PREDEFINED_ENTITY_QUOT_LEN != dst.write(
            ObXmlParserBase::OB_XML_PREDEFINED_ENTITY_QUOT,
            ObXmlParserBase::OB_XML_PREDEFINED_ENTITY_QUOT_LEN))) {
          ret = OB_SIZE_OVERFLOW;
          LOG_WARN("append quot char failed", K(ret), K(c));
        }
        break;
      }
      case ObXmlParserBase::OB_XML_PREDEFINED_ENTITY_APOS_SYMBOL : {
        if (OB_UNLIKELY(ObXmlParserBase::OB_XML_PREDEFINED_ENTITY_APOS_LEN != dst.write(
          ObXmlParserBase::OB_XML_PREDEFINED_ENTITY_APOS,
          ObXmlParserBase::OB_XML_PREDEFINED_ENTITY_APOS_LEN))) {
          ret = OB_SIZE_OVERFLOW;
          LOG_WARN("write apos char failed", K(ret), K(c));
        }
        break;
      }
      default : {
        if (OB_UNLIKELY(1 != dst.write(ptr + i, 1))) {
          ret = OB_SIZE_OVERFLOW;
          LOG_WARN("write normal char failed", K(ret), K(c));
        }
        break;
      }
    } // end switch
  } // end for
  return ret;
}

bool ObXmlParserUtils::has_xml_decl(const ObString& xml_text)
{
  const char* str = xml_text.ptr();
  int length = xml_text.length();
  int idx = 0;
  bool res = false;
  // skip space
  while(idx < length && isspace(str[idx])) {
    ++idx;
  }
  if (idx+4 < length
     && str[idx] == '<'
     && str[idx+1] == '?'
     && str[idx+2] == 'x'
     && str[idx+3] == 'm'
     && str[idx+4] == 'l') {
    res = true;
  }
  return res;
}


// XMLDecl	   ::=   	'<?xml' VersionInfo EncodingDecl? SDDecl? S? '?>'
#define OB_PARSE_XML_DECL_SKIP_SPACE \
do{\
  while(idx < length && isspace(str[idx]))++idx; \
}while(0);

static int parse_name_value(const char* str,
                            int& idx,
                            int length,
                            const ObString& name,
                            const char*& value_pos,
                            int& value_len,
                            bool& has_value)
{
  INIT_SUCC(ret);
  OB_PARSE_XML_DECL_SKIP_SPACE
  if (idx + name.length() <= length && MEMCMP(name.ptr(), str+idx, name.length()) == 0) {
    idx += name.length();
    if (idx < length && str[idx] == '=') {
      idx += 1;
      OB_PARSE_XML_DECL_SKIP_SPACE
      if (idx < length && str[idx] == '"') {
        // "xxx"
        int start = ++idx;
        while(idx < length && str[idx] != '"')++idx;
        if (idx < length && str[idx] == '"') {
          value_pos = str + start;
          value_len = idx - start;
          idx += 1;
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_DEBUG("not match", K(idx), K(name));
        }
      } else if (idx < length && str[idx] == '\'') {
        // 'xxx'
        int start = ++idx;
        while(idx < length && str[idx] != '\'')++idx;
        if (idx < length && str[idx] == '\'') {
          value_pos = str + start;
          value_len = idx - start;
          idx += 1;
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_DEBUG("not match", K(idx), K(name));
        }
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_DEBUG("not match", K(idx), K(name));
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_DEBUG("not match", K(idx), K(name));
    }

    // has name, but length is zero
    if (OB_SUCC(ret)) {
      has_value = true;
    }
  }
  return ret;
}

int ObXmlParserUtils::parse_xml_decl_encoding(const ObString& xml_decl, bool &has_decl, ObString& encoding_str)
{
  INIT_SUCC(ret);
  ObString src_version_str;
  ObString src_standalone_str;
  bool has_xml_decl = false;
  bool has_version_value = false;
  bool has_encoding_value = false;
  bool has_standalone_value = false;

  has_decl = ObXmlParserUtils::has_xml_decl(xml_decl);
  if (has_decl && OB_FAIL(ObXmlParserUtils::parse_xml_decl(xml_decl,
                                                   src_version_str,
                                                   has_version_value,
                                                   encoding_str,
                                                   has_encoding_value,
                                                   src_standalone_str,
                                                   has_standalone_value))) {
    LOG_WARN("parse xml decl failed", K(ret), K(xml_decl));
  }
  return ret;
}

int ObXmlParserUtils::parse_xml_decl(const ObString& xml_decl,
                                     ObString& version,
                                     bool &has_version_value,
                                     ObString& encoding,
                                     bool &has_encoding_value,
                                     ObString& standalone,
                                     bool &has_standalone_value)
{
  INIT_SUCC(ret);
  const char* str = xml_decl.ptr();
  int length = xml_decl.length();
  int idx = 0;
  const char* version_start = nullptr;
  int version_len = 0;
  const char* encoding_start = nullptr;
  int encoding_len = 0;
  const char* standalone_start = nullptr;
  int standalone_len = 0;

  // case sensitive, so no need tolower
  if (idx + 4 < length
      && str[idx] == '<'
      && str[idx+1] == '?'
      && str[idx+2] == 'x'
      && str[idx+3] == 'm'
      && str[idx+4] == 'l') {
    // <?xml
    idx += 5;

    // version
    if (OB_FAIL(parse_name_value(str, idx, length, ObString("version"), version_start, version_len, has_version_value))) {
      LOG_WARN("parse_name_value version failed", K(ret), K(idx), K(length));
    // encoding
    } else if (OB_FAIL(parse_name_value(str, idx, length, ObString("encoding"), encoding_start, encoding_len, has_encoding_value))) {
      LOG_WARN("parse_name_value encoding failed", K(ret), K(idx), K(length));
    // standalone
    } else if (OB_FAIL(parse_name_value(str, idx, length, ObString("standalone"), standalone_start, standalone_len, has_standalone_value))) {
      LOG_WARN("parse_name_value standalone failed", K(ret), K(idx), K(length));
    } else {
      OB_PARSE_XML_DECL_SKIP_SPACE
      if (idx + 1 < length
          && str[idx] == '?'
          && str[idx+1] == '>') {
        version.assign_ptr(version_start, version_len);
        encoding.assign_ptr(encoding_start, encoding_len);
        standalone.assign_ptr(standalone_start, standalone_len);
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("not invalid xml decl", K(ret), K(xml_decl));
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("not invalid xml decl", K(ret), K(xml_decl));
  }
  return ret;
}

ObXmlStandaloneType ObXmlParserUtils::get_standalone_type(const ObString& src_standalone_str) {
  ObXmlStandaloneType type = OB_XML_STANDALONE_NONE;
  if (!src_standalone_str.empty()) {
    if (src_standalone_str.compare("yes") == 0) {
      type = OB_XML_STANDALONE_YES;
    } else if (src_standalone_str.compare("no") == 0) {
      type = OB_XML_STANDALONE_NO;
    }
  }
  return type;
}

/*
 * [4NS] NCNameChar ::= Letter | Digit | '.' | '-' | '_' |
 *                      CombiningChar | Extender
 *
 * [5NS] NCName ::= (Letter | '_') (NCNameChar)*
*/
int ObXmlParserUtils::check_local_name_legality(const ObString& localname)
{
  INIT_SUCC(ret);
  uint64_t letter_count = 0;
  const char* str = localname.ptr();
  unsigned codepoint = 0;
  uint64_t last_pos = 0;

  rapidjson::MemoryStream input_stream(str, localname.length());
  while (OB_SUCC(ret) && input_stream.Tell() < localname.length()) {
    last_pos = input_stream.Tell();
    bool first_codepoint = (last_pos == 0);
    if (!rapidjson::UTF8<char>::Decode(input_stream, &codepoint)) {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("ns is invalid", K(ret), K(localname), K(input_stream.Tell()));
    } else {
      uint64_t curr_pos = input_stream.Tell();
      if (first_codepoint) {
        if ((IS_LETTER(codepoint) || (codepoint == 0x5f))) { // '_'
          // do nothing
        } else {
          ret = OB_ERR_PARSER_SYNTAX;
          LOG_WARN("ns is invalid", K(ret), K(localname));
        }
      } else if (((IS_LETTER(codepoint)) || (IS_DIGIT(codepoint)) ||
		             (codepoint == 0x2e) || (codepoint == 0x2d) || // '.', '-'
                 (codepoint == 0x5f) || // '_'
                 (IS_COMBINING(codepoint)) ||
                 (IS_EXTENDER(codepoint)))) {
        // do nothing
      } else {
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("ns is invalid", K(ret), K(localname));
      }
    }
  }
  return ret;
}

int ObXmlParserUtils::get_prefix_and_localname(const ObString& qname, ObString& prefix, ObString& localname)
{
  INIT_SUCC(ret);
  const char* str = qname.ptr();
  int sep_pos = -1;
  for (int i = 0; i < qname.length(); ++i) {
    if (str[i] == ':') {
      sep_pos = i;
      break;
    }
  }
  if (sep_pos > 0) {
    prefix.assign_ptr(qname.ptr(), sep_pos);
    localname.assign_ptr(qname.ptr() + sep_pos + 1, qname.length() - sep_pos - 1);
  } else {
    // no ns prefix
    localname = qname;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(check_local_name_legality(localname))) {
    LOG_WARN("localname is invalid", K(ret), K(qname), K(sep_pos), K(prefix), K(localname));
  }
  return ret;
}

bool ObXmlParserUtils::is_namespace_attribute(ObXmlAttribute* attr)
{
  bool res = false;
  if (OB_NOT_NULL(attr)) {
    if (attr->get_prefix().compare(ObXmlConstants::XMLNS_STRING) == 0) {
      res = true;
    } else if (attr->get_prefix().empty() && attr->get_key().compare(ObXmlConstants::XMLNS_STRING) == 0) {
      res = true;
    }
  }
  return res;
}

bool ObXmlParserUtils::is_entity_ref(ObString &input_str, int64_t index, ObString &ref, int64_t &ref_len)
{
  bool res = false;
  const char *ptr = input_str.ptr();
  if (index < input_str.length()) {
    ObString tmp_str;
    // length = 4
    if (index + 3 < input_str.length()) {
      tmp_str.assign_ptr(ptr+index, 4);
      if (tmp_str.case_compare("&lt;") == 0) {
        res = true;
        ref = ObString::make_string("<");
        ref_len = 4;
      } else if (tmp_str.case_compare("&gt;") == 0) {
        res = true;
        ref = ObString::make_string(">");
        ref_len = 4;
      }
    }
    // length = 6
    if (!res && (index + 5 < input_str.length())) {
      tmp_str.assign_ptr(ptr+index, 6);
      if (tmp_str.case_compare("&quot;") == 0) {
        res = true;
        ref = ObString::make_string("\"");
        ref_len = 6;
      } else if (tmp_str.case_compare("&apos;") == 0) {
        res = true;
        ref = ObString::make_string("'");
        ref_len = 6;
      }
    }
    // length = 5
    if (!res && (index + 4 < input_str.length())) {
      tmp_str.assign_ptr(ptr+index, 5);
      if (tmp_str.case_compare("&amp;") == 0) {
        res = true;
        ref = ObString::make_string("&");
        ref_len = 5;
      }
    }
  }
  return res;
}

// ObXmlParserUtils end


} // namespace common
} // namespace oceanbase
