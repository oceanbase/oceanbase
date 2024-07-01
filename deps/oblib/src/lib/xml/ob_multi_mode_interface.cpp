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
 * This file contains parts interface implement for the multi mode type data abstraction.
 */

#define USING_LOG_PREFIX LIB
#include "lib/xml/ob_multi_mode_interface.h"
#include "lib/xml/ob_xml_parser.h"
#include "lib/xml/ob_xml_tree.h"
#include "lib/xml/ob_xml_bin.h"
#include "lib/xml/ob_xml_util.h"

namespace oceanbase {
namespace common {

int ObPathPool::init(int64_t obj_size, ObIAllocator *alloc)
{
  INIT_SUCC(ret);
  if (OB_UNLIKELY(obj_size < static_cast<int64_t>(sizeof(FreeNode)))) {
    LIB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "obj_size_ < size of FreeNode");
  } else if (OB_ISNULL(alloc)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret));
  } else {
    // must use tmp_allocator to init
    obj_size_ = obj_size;
    alloc_ = alloc;
    is_inited_ = true;
  }
  return ret;
}

void *ObPathPool::alloc()
{
  void *ptr_ret = NULL;
  if (!is_inited_) {
  } else if (NULL == (ptr_ret = freelist_pop())) {
    alloc_new_node();
    ptr_ret = freelist_pop();
  }
  return ptr_ret;
}

void ObPathPool::free(void *obj)
{
  if (!is_inited_) {
  } else if (NULL != obj) {
    --in_use_count_;
  }
  freelist_push(obj);
}

void ObPathPool::reset()
{
  is_inited_ = false;
  freelist_ =  nullptr;
}

void *ObPathPool::freelist_pop()
{
  void *ptr_ret = NULL;
  if (!is_inited_) {
  } else if (NULL != freelist_) {
    ptr_ret = freelist_;
    freelist_ = freelist_->next_;
    --free_count_;
    ++in_use_count_;
  }
  return ptr_ret;
}

void ObPathPool::freelist_push(void *obj)
{
  if (!is_inited_) {
  } else if (NULL != obj) {
    FreeNode *node = static_cast<FreeNode *>(obj);
    if (OB_ISNULL(node)) {
      LIB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "node is NULL");
    } else {
      node->next_ = freelist_;
      freelist_ = node;
      ++free_count_;
    }
  }
}

void ObPathPool::alloc_new_node()
{
  if (!is_inited_) {
  } else if (OB_ISNULL(alloc_)) {
  } else {
    ++total_count_;
    freelist_push(alloc_->alloc(obj_size_));
  }
}

// need overwrite
bool ObIMulModeBase::get_boolean()
{
  return false;
}

// need overwrite
double ObIMulModeBase::get_double()
{
  return 0;
}

// need overwrite
float ObIMulModeBase::get_float()
{
  return 0;
}

// need overwrite
int64_t ObIMulModeBase::get_int()
{
  return 0;
}

// need overwrite
uint64_t ObIMulModeBase::get_uint()
{
  return 0;
}

// need overwrite
const char* ObIMulModeBase::get_data()
{
  return nullptr;
}

// need overwrite
uint64_t ObIMulModeBase::get_data_length()
{
  return 0;
}

// need overwrite
number::ObNumber ObIMulModeBase::get_decimal_data()
{
  return number::ObNumber();
}

// need overwrite
ObPrecision ObIMulModeBase::get_decimal_precision()
{
  return -1;
}

// need overwrite
ObScale ObIMulModeBase::get_decimal_scale()
{
  return -1;
}

// need overwrite
ObTime ObIMulModeBase::get_time()
{
  return ObTime();
}

// need overwrite
int64_t ObIMulModeBase::get_serialize_size()
{
  return 0;
}

int ObIMulModeBase::print(ObStringBuffer& x_buf, uint32_t format_flag, uint64_t depth, uint64_t size, ObCollationType charset)
{
  INIT_SUCC(ret);

  if (meta_.data_type_ == OB_XML_TYPE) {
    if (!check_extend()) {
      ret = print_xml(x_buf, format_flag, depth, size, nullptr, charset);
    } else {
      ObNsSortedVector ns_vec;
      if (OB_FAIL(ObXmlUtil::init_extend_ns_vec(allocator_, this, ns_vec))) {
        LOG_WARN("fail to init ns vector by extend area", K(ret));
      } else if (OB_FAIL(print_xml(x_buf, format_flag, depth, size, &ns_vec, charset))) {
        LOG_WARN("fail to print xml", K(ret));
      }
      ns_vec.clear();
    }
  } else {
    ret = OB_NOT_SUPPORTED;
  }

  return ret;
}

int ObIMulModeBase::print_xml(ObStringBuffer& x_buf, uint32_t format_flag, uint64_t depth, uint64_t size, ObNsSortedVector* ns_vec, ObCollationType charset)
{
  INIT_SUCC(ret);
  ObMulModeNodeType xml_type = type();

  switch(xml_type) {
    case ObMulModeNodeType::M_DOCUMENT : {
      if (OB_FAIL(print_document(x_buf, CS_TYPE_INVALID, format_flag, size, ns_vec))) {
        LOG_WARN("fail to print element to string", K(ret), K(depth), K(xml_type));
      }
      break;
    }
    case ObMulModeNodeType::M_CONTENT : {
      ParamPrint param_list;
      if (OB_FAIL(print_content(x_buf, false, false, format_flag, param_list, ns_vec))) {
        LOG_WARN("fail to print element to string", K(ret), K(depth), K(xml_type));
      }
      break;
    }
    case ObMulModeNodeType::M_UNPARESED_DOC:
    case ObMulModeNodeType::M_UNPARSED : {
      if (OB_FAIL(print_unparsed(x_buf, CS_TYPE_INVALID, format_flag, size))) {
        LOG_WARN("fail to print element to string", K(ret), K(depth), K(xml_type));
      }
      break;
    }
    case ObMulModeNodeType::M_ELEMENT : {
      if (ObXmlUtil::is_xml_doc_over_depth((depth + 1))) {
        ret = OB_ERR_JSON_OUT_OF_DEPTH;
        LOG_WARN("current xml over depth", K(ret), K(depth), K(xml_type));
      } else if (OB_FAIL(print_element(x_buf, depth, format_flag, size, ns_vec))) {
        LOG_WARN("fail to print element to string", K(ret), K(depth), K(xml_type));
      }
      break;
    }
    case ObMulModeNodeType::M_ATTRIBUTE : {
      if (OB_FAIL(print_attr(x_buf, format_flag))) {
        LOG_WARN("fail to print attribute to string", K(ret));
      }
      break;
    }
    case ObMulModeNodeType::M_NAMESPACE : {
      if (OB_FAIL(print_ns(x_buf, format_flag))) {
        LOG_WARN("fail to print namespace to string", K(ret), K(depth), K(xml_type));
      }
      break;
    }
    case ObMulModeNodeType::M_TEXT : {
      if (OB_FAIL(print_text(x_buf, format_flag))) {
        LOG_WARN("fail to print text to string", K(ret), K(depth), K(xml_type));
      }
      break;
    }
    case ObMulModeNodeType::M_CDATA : {
      if (OB_FAIL(print_cdata(x_buf, format_flag))) {
        LOG_WARN("fail to print cdata to string", K(ret), K(depth), K(xml_type));
      }
      break;
    }
    case ObMulModeNodeType::M_INSTRUCT : {
      if (OB_FAIL(print_pi(x_buf, format_flag))) {
        LOG_WARN("fail to print pi to string", K(ret), K(depth), K(xml_type));
      }
      break;
    }
    case ObMulModeNodeType::M_COMMENT : {
      if (OB_FAIL(print_comment(x_buf, format_flag))) {
        LOG_WARN("fail to print comment to string", K(ret), K(depth), K(xml_type));
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("undefined xml node type", K(ret), K(xml_type));
      break;
    }
  }
  return ret;
}

int ObIMulModeBase::print_attr(ObStringBuffer& x_buf, uint32_t format_flag)
{
  INIT_SUCC(ret);
  ObString key;
  ObString value;
  ObXmlAttribute *att = NULL;
  bool is_mysql_key_only = false;

  if (OB_FAIL(ret)) {
  } else if (lib::is_oracle_mode()) {
    // do nothing
  } else if (type() != M_ATTRIBUTE) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("print attributes but type not attribute.", K(ret), K(type()));
  } else if (OB_ISNULL(att = static_cast<ObXmlAttribute*>(this))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get attribute node null", K(ret));
  } else if (att->get_only_key()) {
    is_mysql_key_only = true;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_key(key))) {
    LOG_WARN("fail to print =\" in attr", K(ret));
  } else if (OB_FAIL(get_value(value))) {
    LOG_WARN("fail to print =\" in attr", K(ret));
  } else if (OB_FAIL(ObXmlUtil::append_qname(x_buf, get_prefix(), key))) {
    LOG_WARN("fail to print prefix in attr", K(ret), K(get_prefix()), K(key));
  } else if (is_mysql_key_only) {
    // do nothing
  } else if (OB_FAIL(x_buf.append("=\""))) {
    LOG_WARN("fail to print =\" in attr", K(ret));
  } else if (!(format_flag & NO_ENTITY_ESCAPE)) {
    if (OB_FAIL(ObXmlParserUtils::escape_xml_text(value, x_buf))) {
      LOG_WARN("fail to print text with escape char", K(ret), K(value));
    }
  } else if (OB_FAIL(x_buf.append(value))) {
    LOG_WARN("fail to print value in attr", K(ret), K(value));
  }

  if (OB_SUCC(ret) && !is_mysql_key_only && OB_FAIL(x_buf.append("\""))) {
    LOG_WARN("fail to print \" in attr", K(ret));
  }
  return ret;
}

int ObIMulModeBase::print_ns(ObStringBuffer& x_buf, uint32_t format_flag)
{
  INIT_SUCC(ret);

  ObString xmlns = "xmlns";
  ObString key;
  ObString value;

  if (OB_FAIL(get_key(key))) {
    LOG_WARN("fail to print =\" in attr", K(ret));
  } else if (OB_FAIL(get_value(value))) {
    LOG_WARN("fail to print =\" in attr", K(ret));
  } else if (xmlns.compare(key) == 0) {
    xmlns = ObString();
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObXmlUtil::append_qname(x_buf, xmlns, key))) {
    LOG_WARN("fail to print prefix in ns", K(ret), K(xmlns), K(key));
  } else if (OB_FAIL(x_buf.append("=\""))) {
    LOG_WARN("fail to print =\" in ns", K(ret));
  } else if (!(format_flag & NO_ENTITY_ESCAPE)) {
    if (OB_FAIL(ObXmlParserUtils::escape_xml_text(value, x_buf))) {
      LOG_WARN("fail to print text with escape char", K(ret), K(value));
    }
  } else if (OB_FAIL(x_buf.append(value))) {
    LOG_WARN("fail to print value in ns", K(ret), K(value));
  }

  if (OB_SUCC(ret) && OB_FAIL(x_buf.append("\""))) {
    LOG_WARN("fail to print \" in ns", K(ret));
  }

  return ret;
}
int ObIMulModeBase::print_pi(ObStringBuffer& x_buf, uint32_t format_flag)
{
  INIT_SUCC(ret);
  ObString key;
  ObString value;
  if (format_flag & ObXmlFormatType::HIDE_PI) {
    // do nothing
  } else {
    if (OB_FAIL(get_key(key))) {
      LOG_WARN("fail to print =\" in attr", K(ret));
    } else if (OB_FAIL(get_value(value))) {
      LOG_WARN("fail to print =\" in attr", K(ret));
    } else if (OB_FAIL(x_buf.append("<?"))) {
      LOG_WARN("fail to print <? in pi", K(ret));
    } else if (OB_FAIL(x_buf.append(key))) {
      LOG_WARN("fail to print target in attr", K(ret), K(key));
    } else if (value.empty()) { // if value is empty then do nothing
    } else if (OB_FAIL(x_buf.append(" "))) {
      LOG_WARN("fail to print space in attr", K(ret));
    } else if (OB_FAIL(x_buf.append(value))) {
      LOG_WARN("fail to print value in attr", K(ret), K(value));
    }

    if (OB_SUCC(ret) && OB_FAIL(x_buf.append("?>"))) {
      LOG_WARN("fail to print ?> in attr", K(ret));
    }
  }
  return ret;
}

int ObIMulModeBase::print_unparsed(ObStringBuffer& x_buf, ObCollationType charset, uint32_t format_flag, uint64_t size)
{
  INIT_SUCC(ret);
  ObString version = get_version();
  ObString encoding = get_encoding();
  uint16_t standalone = get_standalone();
  if (!(format_flag & ObXmlFormatType::HIDE_PROLOG) && has_flags(XML_DECL_FLAG)) {
    if (OB_FAIL(x_buf.append("<?xml"))) {
      LOG_WARN("fail to print <?xml in document", K(ret));
    } else if (!version.empty()) {
      if (OB_FAIL(x_buf.append(" version=\""))) {
        LOG_WARN("fail to print version=\" in document", K(ret));
      } else if (OB_FAIL(x_buf.append(version))) {
        LOG_WARN("fail to print version value in document", K(ret), K(version));
      } else if (OB_FAIL(x_buf.append("\""))) {
        LOG_WARN("fail to print \" in document", K(ret));
      }
    }
    if (OB_SUCC(ret) && (!encoding.empty() || has_flags(XML_ENCODING_EMPTY_FLAG))) {
      if (charset != ObCollationType::CS_TYPE_INVALID) {
        encoding = ObXmlUtil::get_charset_name(charset);
      }
      if (OB_FAIL(x_buf.append(" encoding=\""))) {
        LOG_WARN("fail to print encoding=\" in document", K(ret));
      } else if (OB_FAIL(x_buf.append(encoding))) {
        LOG_WARN("fail to print encoding value in document", K(ret), K(encoding), K(charset));
      } else if (OB_FAIL(x_buf.append("\""))) {
        LOG_WARN("fail to print \" in document", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (standalone == OB_XML_STANDALONE_NO && OB_FAIL(x_buf.append(" standalone=\"no\""))) {
        LOG_WARN("failed to print standalone no", K(ret));
      } else if (standalone == OB_XML_STANDALONE_YES && OB_FAIL(x_buf.append(" standalone=\"yes\""))) {
        LOG_WARN("failed to print standalone yes", K(ret));
      } else if (OB_FAIL(x_buf.append("?>\n"))) {
        LOG_WARN("failed to print ?>", K(ret));
      }
    }
  }

  int64_t num_children = count();
  ObXmlBin tmp_bin;

  for (int64_t i = 0; OB_SUCC(ret) && i < num_children; i ++) {
    ObIMulModeBase* cur = at(i, &tmp_bin);
    if (OB_ISNULL(cur)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get child from element", K(ret), K(i));
    } else if (i > 0 && OB_FAIL(x_buf.append(" "))) {
      LOG_WARN("failed to append space", K(ret));
    } else if (cur->type() == M_TEXT || (cur->type() == M_ELEMENT && cur->get_unparse())) {
      // unparsed element skip print newline and indent
    } else if ((format_flag & ObXmlFormatType::NEWLINE_AND_INDENT) && i > 0
                && OB_FAIL(ObXmlUtil::append_newline_and_indent(x_buf, 0, size))) {
      LOG_WARN("failed to add is_pretty", K(ret), K(size));
    }

    if (OB_SUCC(ret) && OB_FAIL(cur->print_xml(x_buf, format_flag, 0, size))) {
      LOG_WARN("failed to print child in element", K(ret), K(i));
    }
  }

  return ret;
}

int ObIMulModeBase::print_document(ObStringBuffer& x_buf, ObCollationType charset, uint32_t format_flag, uint64_t size, ObNsSortedVector* ns_vec)
{
  INIT_SUCC(ret);
  ObString version = get_version();
  ObString encoding = get_encoding();
  uint16_t standalone = get_standalone();
  bool need_newline_end = true;

  if (!(format_flag & ObXmlFormatType::HIDE_PROLOG) && has_flags(XML_DECL_FLAG)) {
    if (OB_FAIL(x_buf.append("<?xml"))) {
      LOG_WARN("fail to print <?xml in document", K(ret));
    } else if (!version.empty()) {
      if (OB_FAIL(x_buf.append(" version=\""))) {
        LOG_WARN("fail to print version=\" in document", K(ret));
      } else if (OB_FAIL(x_buf.append(version))) {
        LOG_WARN("fail to print version value in document", K(ret), K(version));
      } else if (OB_FAIL(x_buf.append("\""))) {
        LOG_WARN("fail to print \" in document", K(ret));
      }
    }
    if (OB_SUCC(ret) && (!encoding.empty() || has_flags(XML_ENCODING_EMPTY_FLAG))) {
      if (charset != ObCollationType::CS_TYPE_INVALID) {
        encoding = ObXmlUtil::get_charset_name(charset);
      }
      if (OB_FAIL(x_buf.append(" encoding=\""))) {
        LOG_WARN("fail to print encoding=\" in document", K(ret));
      } else if (OB_FAIL(x_buf.append(encoding))) {
        LOG_WARN("fail to print encoding value in document", K(ret), K(encoding), K(charset));
      } else if (OB_FAIL(x_buf.append("\""))) {
        LOG_WARN("fail to print \" in document", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (standalone == OB_XML_STANDALONE_NO && OB_FAIL(x_buf.append(" standalone=\"no\""))) {
        LOG_WARN("failed to print standalone no", K(ret));
      } else if (standalone == OB_XML_STANDALONE_YES && OB_FAIL(x_buf.append(" standalone=\"yes\""))) {
        LOG_WARN("failed to print standalone yes", K(ret));
      } else if (OB_FAIL(x_buf.append("?>\n"))) {
        LOG_WARN("failed to print ?>", K(ret));
      }
    }
  }

   if (OB_SUCC(ret)) {
    int64_t num_children = attribute_count();
    ObXmlBin tmp_bin;

    for (int64_t i = 0; OB_SUCC(ret) && i < num_children; i++) {
      ObIMulModeBase* cur = attribute_at(i, &tmp_bin);
      if (OB_ISNULL(cur)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get child from element", K(ret), K(i));
      } else if ((format_flag & ObXmlFormatType::NEWLINE_AND_INDENT) && i > 0
                  && OB_FAIL(ObXmlUtil::append_newline_and_indent(x_buf, 0, size))) {
        LOG_WARN("failed to add is_pretty", K(ret), K(size));
      } else if (OB_FAIL(cur->print_xml(x_buf, format_flag, 0, size))) {
        LOG_WARN("failed to print child in element", K(ret), K(i));
      }
    }
  }

  if (OB_SUCC(ret)) {
    int64_t num_children = count();
    ObXmlBin tmp_bin;

    for (int64_t i = 0; OB_SUCC(ret) && i < num_children; i++) {
      ObIMulModeBase* cur = at(i, &tmp_bin);
      if (OB_ISNULL(cur)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get child from element", K(ret), K(i));
      } else if (cur->type() == M_TEXT || (cur->type() == M_ELEMENT && cur->get_unparse())) {
        // unparsed element skip print newline and indent
      } else if ((format_flag & ObXmlFormatType::NEWLINE_AND_INDENT) && i > 0
                  && OB_FAIL(ObXmlUtil::append_newline_and_indent(x_buf, 0, size))) {
        LOG_WARN("failed to add is_pretty", K(ret), K(size));
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(cur->print_xml(x_buf, format_flag, 0, size, ns_vec))) {
        LOG_WARN("failed to print child in element", K(ret), K(i));
      } else if (num_children - 1 == i) {
        need_newline_end = !(cur->type() == M_TEXT || (cur->type() == M_ELEMENT && cur->get_unparse()));
      }
    }
  }

  if (OB_SUCC(ret) && need_newline_end && (format_flag & ObXmlFormatType::NEWLINE) && OB_FAIL(x_buf.append("\n"))) {
    LOG_WARN("failed to print \n", K(ret));
  }
  return ret;
}

int ObIMulModeBase::print_content(ObStringBuffer& x_buf, bool with_encoding, bool with_version, uint32_t format_flag, ParamPrint &param_list, ObNsSortedVector* ns_vec)
{
  INIT_SUCC(ret);
  bool need_newline_end = true;

  if (with_encoding || with_version) {
    if (OB_FAIL(x_buf.append("<?xml"))) {
       LOG_WARN("fail to print <?xml in document", K(ret));
    } else if (with_version) {
      if (OB_FAIL(x_buf.append(" version=\""))) {
        LOG_WARN("fail to print version=\" in document", K(ret));
      } else if (OB_FAIL(x_buf.append(param_list.version))) {
        LOG_WARN("fail to print version value in document", K(ret), K(param_list.version));
      } else if (OB_FAIL(x_buf.append("\""))) {
        LOG_WARN("fail to print \" in document", K(ret));
      }
    }
    if (OB_SUCC(ret) && with_encoding) {
      if (OB_FAIL(x_buf.append(" encoding=\""))) {
        LOG_WARN("fail to print encoding=\" in document", K(ret));
      } else if (OB_FAIL(x_buf.append(param_list.encode))) {
        LOG_WARN("fail to print encoding value in document", K(ret), K(param_list.encode));
      } else if (OB_FAIL(x_buf.append("\""))) {
        LOG_WARN("fail to print \" in document", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (get_standalone() == OB_XML_STANDALONE_NO && OB_FAIL(x_buf.append(" standalone=\"no\""))) {
        LOG_WARN("failed to print standalone no", K(ret));
      } else if (get_standalone() == OB_XML_STANDALONE_YES && OB_FAIL(x_buf.append(" standalone=\"yes\""))) {
        LOG_WARN("failed to print standalone yes", K(ret));
      } else if (OB_FAIL(x_buf.append("?>"))) {
        LOG_WARN("failed to print ?>", K(ret));
      } else if ((format_flag & ObXmlFormatType::NEWLINE_AND_INDENT) &&
                  OB_FAIL(ObXmlUtil::append_newline_and_indent(x_buf, 0, param_list.indent))) {
        LOG_WARN("fail to add newline and indent", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    int64_t num_children = attribute_count();
    ObXmlBin tmp_bin;

    for (int64_t i = 0; OB_SUCC(ret) && i < num_children; i++) {
      ObIMulModeBase* cur = attribute_at(i, &tmp_bin);
      if (OB_ISNULL(cur)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get child from element", K(ret), K(i));
      } else if ((format_flag & ObXmlFormatType::NEWLINE_AND_INDENT) && i > 0
                  && OB_FAIL(ObXmlUtil::append_newline_and_indent(x_buf, 0, param_list.indent))) {
        LOG_WARN("failed to add is_pretty", K(ret));
      } else if (OB_FAIL(cur->print_xml(x_buf, format_flag, 0, param_list.indent))) {
        LOG_WARN("failed to print child in element", K(ret), K(i));
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObIMulModeBase* cur = nullptr;
    int64_t num_children = count();
    ObXmlBin tmp_bin;

    for (int64_t i = 0; OB_SUCC(ret) && i < num_children; i ++) {
      cur = at(i, &tmp_bin);
      if (OB_ISNULL(cur)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get child from element", K(ret), K(i));
      } else if (cur->type() == M_TEXT || (cur->type() == M_ELEMENT && cur->get_unparse())) {
        // unparsed element skip print newline and indent
      } else if ((format_flag & ObXmlFormatType::NEWLINE_AND_INDENT)
                  && i > 0
                  && OB_FAIL(ObXmlUtil::append_newline_and_indent(x_buf, 0, param_list.indent))) {
        LOG_WARN("failed to add is_pretty", K(ret));
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(cur->print_xml(x_buf, format_flag, 0, param_list.indent, ns_vec))) {
        LOG_WARN("failed to print child in element", K(ret), K(i));
      } else if (num_children - 1 == i) {
        need_newline_end = !(cur->type() == M_TEXT || (cur->type() == M_ELEMENT && cur->get_unparse()));
      }
    }
  }

  if (OB_SUCC(ret) && need_newline_end && (format_flag & ObXmlFormatType::NEWLINE) && OB_FAIL(x_buf.append("\n"))) {
    LOG_WARN("failed to print \n", K(ret));
  }
  return ret;
}

int ObIMulModeBase::print_cdata(ObStringBuffer& x_buf, uint32_t format_flag)
{
  INIT_SUCC(ret);
  ObString value;
  if (OB_FAIL(get_value(value))) {
    LOG_WARN("fail to get value.", K(ret));
  } else if (format_flag & ObXmlFormatType::PRINT_CDATA_AS_TEXT) {
    if (OB_FAIL(ObXmlParserUtils::escape_xml_text(value, x_buf))) {
      LOG_WARN("fail to print escape text", K(ret));
    }
  } else if (OB_FAIL(x_buf.append("<![CDATA["))) {
    LOG_WARN("fail to print <![CDATA[ in pi", K(ret));
  } else if (OB_FAIL(x_buf.append(value))) {
    LOG_WARN("fail to print text in attr", K(ret), K(value));
  } else if (OB_FAIL(x_buf.append("]]>"))) {
    LOG_WARN("fail to print ]]> in attr", K(ret));
  }
  return ret;
}

int ObIMulModeBase::print_comment(ObStringBuffer& x_buf, uint32_t format_flag)
{
  UNUSED(format_flag);
  INIT_SUCC(ret);
  ObString value;

  if (OB_FAIL(get_value(value))) {
    LOG_WARN("fail to get value.", K(ret));
  } else if (OB_FAIL(x_buf.append("<!--"))) {
    LOG_WARN("fail to print <!-- in pi", K(ret));
  } else if (OB_FAIL(x_buf.append(value))) {
    LOG_WARN("fail to print text in attr", K(ret), K(value));
  } else if (OB_FAIL(x_buf.append("-->"))) {
    LOG_WARN("fail to print --> in attr", K(ret));
  }
  return ret;
}

int ObIMulModeBase::print_text(ObStringBuffer& x_buf, uint32_t format_flag)
{
  INIT_SUCC(ret);
  ObString value;
  if (OB_FAIL(get_value(value))) {
    LOG_WARN("fail to get value.", K(ret));
  } else if (!(format_flag & NO_ENTITY_ESCAPE) && !lib::is_mysql_mode()) {
    if (OB_FAIL(ObXmlParserUtils::escape_xml_text(value, x_buf))) {
      LOG_WARN("fail to print text with escape char", K(ret), K(value));
    }
  } else if (OB_FAIL(x_buf.append(value))) {
    LOG_WARN("fail to print text", K(ret), K(value));
  }
  return ret;
}

int ObIMulModeBase::print_element(ObStringBuffer& x_buf, uint64_t depth, uint32_t format_flag, uint64_t size, ObNsSortedVector* ns_vec)
{
  INIT_SUCC(ret);
  bool is_unparse = get_unparse();
  int64_t num_children = this->size();
  ObIMulModeBase* cur = nullptr;
  ObString key;
  ObXmlBin tmp_bin;
  int attributes_count = 0;
  format_flag = is_unparse ? (format_flag | NO_ENTITY_ESCAPE) : (format_flag & ~NO_ENTITY_ESCAPE);
  // duplicate ns that defined in this element should be delete
  // because namespaces with the same key are subject to the latest
  // but this definition is only valid in this element and its descendant
  // so, restore ns vec when finish printing this element, in case its sibling loses ns definition
  ObVector<ObNsPair*> deleted_ns_vec;
  if (OB_FAIL(get_key(key))) {
    LOG_WARN("fail get key of element", K(ret));
  } else if (is_unparse && key.empty() && OB_FAIL(get_node_count(ObMulModeNodeType::M_ATTRIBUTE, attributes_count))) {
    LOG_WARN("get attributes count failed", K(ret));
  } else if (is_unparse && key.empty() && attributes_count == 0) {

    for (int64_t i = 0; OB_SUCC(ret) && i < num_children; i++) {
      cur = at(i, &tmp_bin);
      if (OB_ISNULL(cur)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get child from element", K(ret), K(i));
      } else if (OB_FAIL(cur->print_xml(x_buf, format_flag, (depth + 1), size, ns_vec))) {
        LOG_WARN("failed to print child in element", K(ret), K(i));
      }
    }
  } else if (OB_NOT_NULL(ns_vec) && OB_FAIL(ObXmlUtil::delete_dup_ns_definition(this, *ns_vec, deleted_ns_vec))) {
    LOG_WARN("fail to delete dup ns definition", K(ret));
  } else {
    ObString prefix = get_prefix();
    if (OB_FAIL(x_buf.append("<"))) {
      LOG_WARN("fail to print < in element", K(ret));
    } else if (OB_FAIL(ObXmlUtil::append_qname(x_buf, prefix, key))) {
      LOG_WARN("fail to print tag in element", K(ret), K(prefix), K(key));
    } else if (OB_NOT_NULL(ns_vec)
      && (OB_FAIL(ObXmlUtil::add_ns_def_if_necessary(format_flag, x_buf, prefix, ns_vec, deleted_ns_vec))
      || OB_FAIL(ObXmlUtil::add_attr_ns_def(this, format_flag, x_buf, ns_vec, deleted_ns_vec)))) {
      LOG_WARN("fail to add ns definition of prefix", K(ret));
    }

    if (OB_SUCC(ret)) {
      ObIMulModeBase* cur = nullptr;
      int64_t num_children = attribute_size();

      for (int64_t i = 0; OB_SUCC(ret) && i < num_children; i ++) {
        cur = attribute_at(i, &tmp_bin);
        if (OB_ISNULL(cur)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to get child from element", K(ret), K(i));
        } else if (OB_FAIL(x_buf.append(" "))) {
          LOG_WARN("failed to append space", K(ret));
        } else if (OB_FAIL(cur->print_xml(x_buf, format_flag, 0, size))) {
          LOG_WARN("failed to print child in element", K(ret), K(i));
        }
      }
    }

    bool is_empty = get_is_empty();
    int64_t num_children = this->size();
    ObMulModeNodeType last_node_type;
    ObIMulModeBase* last_node = nullptr;

    if (OB_FAIL(ret)) {
    } else if ((format_flag & ObXmlFormatType::MERGE_EMPTY_TAG) && (is_empty || num_children == 0)) {
      if (OB_FAIL(x_buf.append("/>"))) {
        LOG_WARN("fail to print />", K(ret));
      }
    } else if (OB_FAIL(x_buf.append(">"))) {
      LOG_WARN("fail to print >", K(ret));
    } else {
      ObIMulModeBase* cur = nullptr;
      ObMulModeNodeType prev_node_type, cur_node_type;

      for (int64_t i = 0; OB_SUCC(ret) && i < num_children; i++) {
        cur = at(i, &tmp_bin);
        if (OB_ISNULL(cur)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to get child from element", K(ret), K(i));
        } else if (FALSE_IT(cur_node_type = cur->type())) {
        } else if ((format_flag & ObXmlFormatType::NEWLINE_AND_INDENT)
                   && !((cur_node_type == ObMulModeNodeType::M_TEXT
                        || cur_node_type == ObMulModeNodeType::M_CDATA)
                        || ((i > 0) && (prev_node_type == ObMulModeNodeType::M_TEXT
                        || prev_node_type == ObMulModeNodeType::M_CDATA)))
                   && OB_FAIL(ObXmlUtil::append_newline_and_indent(x_buf, depth + 1, size))) {
          LOG_WARN("failed to add is_pretty", K(ret), K(depth), K(size));
        } else if (OB_FAIL(cur->print_xml(x_buf, format_flag, depth + 1, size, ns_vec))) {
          LOG_WARN("failed to print child in element", K(ret), K(i));
        } else {
          prev_node_type = cur->type();
          last_node_type = prev_node_type;
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if ((format_flag & ObXmlFormatType::MERGE_EMPTY_TAG) && (is_empty || num_children == 0)) { // error or empty do nothing
    } else if ((format_flag & ObXmlFormatType::NEWLINE_AND_INDENT) && last_node_type != ObMulModeNodeType::M_TEXT
              && last_node_type != ObMulModeNodeType::M_CDATA
              && OB_FAIL(ObXmlUtil::append_newline_and_indent(x_buf, depth, size))) {
      LOG_WARN("failed to add is_pretty", K(ret), K(depth), K(size));
    } else if (OB_FAIL(x_buf.append("</"))) {
      LOG_WARN("fail to print </ in element", K(ret));
    } else if (OB_FAIL(ObXmlUtil::append_qname(x_buf, prefix, key))) {
      LOG_WARN("fail to print tag in element", K(ret), K(key), K(prefix));
    } else if (OB_FAIL(x_buf.append(">"))) {
      LOG_WARN("fail to print > in element", K(ret));
    }
  }

  if (deleted_ns_vec.size() > 0 && OB_NOT_NULL(ns_vec)
      && OB_FAIL(ObXmlUtil::restore_ns_vec(ns_vec, deleted_ns_vec))) {
    LOG_WARN("fail to restore ns vec", K(ret));
  }
  deleted_ns_vec.clear();
  return ret;
}


} // namespace common
} // namespace oceanbase
