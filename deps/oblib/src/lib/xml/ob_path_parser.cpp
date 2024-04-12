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
 * This file contains implementation support for the XML path abstraction.
 */

#define USING_LOG_PREFIX SQL_RESV
#include "lib/xml/ob_xpath.h"
#include "lib/xml/ob_path_parser.h"
#include "lib/xml/ob_xml_util.h"
#include "lib/string/ob_sql_string.h"
#include "lib/ob_errno.h"
#include "lib/string/ob_string.h"
#include "rpc/obmysql/ob_mysql_global.h" // DOUBLE_TO_STRING_CONVERSION_BUFFER_SIZE
#include "common/data_buffer.h"
#include <rapidjson/encodings.h>
#include <rapidjson/memorystream.h>

namespace oceanbase {
namespace common {

bool ObPathParserUtil::is_xml_name_start_char(const char ch) {
  int ret_bool = false;
  if (isalpha(ch) || (ch == ObPathItem::UNDERLINE)) {
    ret_bool = true;
  }
  return ret_bool;
}

bool ObPathParserUtil::is_end_of_xpathkeyword(const char ch) {
  int ret_bool = false;
  if (ObXPathUtil::is_whitespace(ch)
      || ch == ObPathItem::COLON       /* namespace or axis */
      || ch == ObPathItem::BRACE_START /* node(), text() or function */
      || ch == ObPathItem::BEGIN_ARRAY /*filter*/ ) {
    ret_bool = true;
  }
  return ret_bool;
}

bool ObPathParserUtil::is_xpath_ident_terminator(const char ch) {
  int ret_bool = false;
  if (ObXPathUtil::is_whitespace(ch)
      || ch == ObPathItem::SLASH
      || ch == ObPathItem::BRACE_START
      || ch == ObPathItem::BEGIN_ARRAY) {
    ret_bool = true;
  }
  return ret_bool;
}

//use for xmltable transfrom xpath special in resolve
bool ObPathParserUtil::is_xpath_transform_terminator(const char ch) {
  int ret_bool = false;
  if (ObXPathUtil::is_whitespace(ch)
      || ch == ObPathItem::SLASH
      || ch == ObPathItem::BEGIN_ARRAY) {
    ret_bool = true;
  }
  return ret_bool;
}

bool ObPathParserUtil::is_boolean_ans(ObFilterType type)
{
  bool ret_bool = false;
  if (type >= ObFilterType::PN_OR_COND && type <= ObFilterType::PN_CMP_GT) {
    ret_bool = true;
  }
  return ret_bool;
}

bool ObPathParserUtil::is_illegal_comp_for_filter(const ObFilterType& type, ObPathNode* left, ObPathNode* right)
{
  bool ret_bool = false;
  if (OB_ISNULL(left) || OB_ISNULL(right)) {
    ret_bool = false;
  } else {
    switch(type) {
      case ObFilterType::PN_CMP_ADD:
      case ObFilterType::PN_CMP_SUB:
      case ObFilterType::PN_CMP_MUL:
      case ObFilterType::PN_CMP_DIV:
      case ObFilterType::PN_CMP_MOD: {
        if ((left->node_type_.is_arg() && left->node_type_.get_arg_type() == ObArgType::PN_STRING)
          || (right->node_type_.is_arg() && right->node_type_.get_arg_type() == ObArgType::PN_STRING)
          || (left->node_type_.is_filter() && is_boolean_subpath_arg(left))
          || (right->node_type_.is_filter() && is_boolean_subpath_arg(right))) {
          ret_bool = true;
        }
        break;
      }
      case ObFilterType::PN_CMP_EQUAL:
      case ObFilterType::PN_CMP_UNEQUAL:
      case ObFilterType::PN_CMP_LE:
      case ObFilterType::PN_CMP_LT:
      case ObFilterType::PN_CMP_GE:
      case ObFilterType::PN_CMP_GT: {
        if ((left->node_type_.is_filter() && is_boolean_subpath_arg(left))
          || (right->node_type_.is_filter() && is_boolean_subpath_arg(right))) {
          ret_bool = true;
        }
        break;
      }
      default:
        break;
    }
  }
  return ret_bool;
}

bool ObPathParserUtil::is_boolean_subpath_arg(ObPathNode* node)
{
  bool ret_bool = false;
  if (OB_NOT_NULL(node) && node->node_type_.is_filter() ) {
    ObPathFilterNode* filter = static_cast<ObPathFilterNode*>(node);
    ret_bool = filter->is_boolean_;
  }
  return ret_bool;
}

bool ObPathParserUtil::is_position(ObPathNode* node)
{
  bool ret_bool = false;
  if (OB_NOT_NULL(node) && node->node_type_.is_arg()
    && node->node_type_.get_arg_type() == ObArgType::PN_SUBPATH)
  {
    ObPathArgNode* arg = static_cast<ObPathArgNode*>(node);
    ObPathNode* subpath = arg->arg_.subpath_;
    if (OB_NOT_NULL(subpath) && subpath->node_type_.is_root()
    && subpath->size() == 1) {
      ObPathNode* func = static_cast<ObPathNode*>(subpath->member(0));
      if (OB_NOT_NULL(func) && func->node_type_.is_func()
      && func->node_type_.get_func_type() == ObFuncType::PN_POSITION) {
        ret_bool = true;
      } // make sure is function postition
    } // make sure only one child
  } // make sure is subpath
  return ret_bool;
}

bool ObPathParserUtil::check_is_legal_tagname(const char* name, int length)
{
  bool ret_bool = true;
  // An empty string is not a valid identifier.
  // todo: use ob decode
  if (OB_ISNULL(name)) ret_bool = false;
  rapidjson::MemoryStream input_stream(name, length);
  unsigned codepoint = 0;
  uint64_t last_pos = 0;

  while (ret_bool && (input_stream.Tell() < length)) {
    last_pos = input_stream.Tell();
    bool first_codepoint = (last_pos == 0);
    if (!rapidjson::UTF8<char>::Decode(input_stream, &codepoint)) {
      ret_bool = false;
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "fail to decode.",
          K(ret_bool), K(codepoint), K(input_stream.Tell()), KCSTRING(name));
    }

    // a unicode letter
    uint64_t curr_pos = input_stream.Tell();
    if (ObXPathUtil::is_letter(codepoint, name, last_pos, curr_pos - last_pos)
        || codepoint == 0x5F ) {
      // letter is ok,  _ is ok
    } else if (first_codepoint) {
      /*
        the first character must be one of the above.
        more possibilities are available for subsequent characters.
      */
      ret_bool = false;
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "first character must be _ or letter.",
          K(ret_bool), K(codepoint), K(input_stream.Tell()), KCSTRING(name));
    } else if (ObXPathUtil::unicode_combining_mark(codepoint) || isdigit(codepoint)
              || ObXPathUtil::is_connector_punctuation(codepoint)
              || codepoint == 0x2D || codepoint == 0x2E){
     // - . is ok
    } else {
      // nope
      ret_bool = false;
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "not legal xml element name.",
          K(ret_bool), K(codepoint), K(input_stream.Tell()), KCSTRING(name));
    }
  }
  return ret_bool;
}

bool ObPathParserUtil::is_left_brace(const char ch)
{
  bool ret_bool = false;
  if (ch == ObPathItem::BRACE_START || ch == ObPathItem::BEGIN_ARRAY) {
    ret_bool = true;
  }
  return ret_bool;
}

bool ObPathParserUtil::is_operator(const char ch)
{
  bool ret_bool = false;
  switch (ch) {
    case '+':
    case '-':
    case '*':
    case '=':
    case '!':
    case '<':
    case '>':
    case '|': {
      ret_bool = true;
      break;
    }
    default: {
      break;
    }
  }
  return ret_bool;
}

bool ObPathParserUtil::is_function_start_char(const char ch)
{
  bool ret_bool = false;
  if (ch == 'b' || ch == 'c' || ch == 'f'
    || ch == 'l' || ch == 'n' || ch == 'p'
    || ch == 'r' || ch == 's' || ch == 't') {
    ret_bool = true;
  }
  return ret_bool;
}

bool ObPathParserUtil::is_func_must_in_pred(const ObFuncType& func_type)
{
  bool ret_bool = false;
  switch (func_type) {
    case ObFuncType::PN_POSITION:
    case ObFuncType::PN_LAST: {
      ret_bool = true;
      break;
    }
    default: {
      break;
    }
  }
  return ret_bool;
}

int ObPathParser::alloc_path_node(ObPathNode*& node)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(allocator_)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret));
  } else {
    ObPathNode* path_node =
    static_cast<ObPathNode*> (allocator_->alloc(sizeof(ObPathNode)));
    if (OB_ISNULL(path_node)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate row buffer failed at path_node", K(ret), K(index_), K(expression_));
    } else {
      node = path_node;
    }
  }
  return ret;
}

int ObPathParser::alloc_root_node(ObPathRootNode*& node)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(allocator_)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret));
  } else {
    ObPathRootNode* root_node =
    static_cast<ObPathRootNode*> (allocator_->alloc(sizeof(ObPathRootNode)));
    if (OB_ISNULL(root_node)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate row buffer failed at path_node", K(ret), K(index_), K(expression_));
    } else {
      node = root_node;
    }
  }
  return ret;
}

int ObPathParser::alloc_filter_op_node(ObPathFilterOpNode*& node)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(allocator_)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret));
  } else {
    ObPathFilterOpNode* new_node =
    static_cast<ObPathFilterOpNode*> (allocator_->alloc(sizeof(ObPathFilterOpNode)));
    if (OB_ISNULL(new_node)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate row buffer failed at path_node", K(ret), K(index_), K(expression_));
    } else {
      node = new_node;
    }
  }
  return ret;
}

int ObPathParser::alloc_location_node(ObPathLocationNode*& node)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(allocator_)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret));
  } else {
    ObPathLocationNode* location_node =
    static_cast<ObPathLocationNode*> (allocator_->alloc(sizeof(ObPathLocationNode)));
    if (OB_ISNULL(location_node)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate row buffer failed at location_node", K(ret), K(index_), K(expression_));
    } else {
      node = location_node;
      location_node->set_ns_info(nullptr, 0);
    }
  }
  return ret;
}

int ObPathParser::alloc_filter_node(ObPathFilterNode*& node)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(allocator_)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret));
  } else {
    ObPathFilterNode* filter_node =
    static_cast<ObPathFilterNode*> (allocator_->alloc(sizeof(ObPathFilterNode)));
    if (OB_ISNULL(filter_node)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate row buffer failed at location_node", K(ret), K(index_), K(expression_));
    } else {
      node = filter_node;
    }
  }
  return ret;
}

int ObPathParser::alloc_func_node(ObPathFuncNode*& node)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(allocator_)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret));
  } else {
    ObPathFuncNode* func_node =
    static_cast<ObPathFuncNode*> (allocator_->alloc(sizeof(ObPathFuncNode)));
    if (OB_ISNULL(func_node)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate row buffer failed at location_node", K(ret), K(index_), K(expression_));
    } else {
      node = func_node;
    }
  }
  return ret;
}

int ObPathParser::alloc_arg_node(ObPathArgNode*& node)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(allocator_)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret));
  } else {
    ObPathArgNode* arg_node =
    static_cast<ObPathArgNode*> (allocator_->alloc(sizeof(ObPathArgNode)));
    if (OB_ISNULL(arg_node)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate row buffer failed at location_node", K(ret), K(index_), K(expression_));
    } else {
      node = arg_node;
    }
  }
  return ret;
}

int ObPathParser::check_is_legal_xpath(const ObPathArgType& patharg_type)
{
  INIT_SUCC(ret);
  int count = root_node_->size();
  for (int i = 0; i + 1 < count && OB_SUCC(ret); ++i) {
    ObPathNode* node1 = static_cast<ObPathNode*>(root_node_->member(i));
    ObPathNode* node2 = static_cast<ObPathNode*>(root_node_->member(i + 1));
    if (node1->node_type_.is_location() && node2->node_type_.is_location()) {
      ObPathLocationNode* location1 = static_cast<ObPathLocationNode*> (node1);
      ObPathLocationNode* location2 = static_cast<ObPathLocationNode*> (node2);
      if (patharg_type == ObPathArgType::NOT_SUBPATH) {
        if (location1->get_seek_type() == ObSeekType::TEXT
            && location1->get_prefix_ns_info()
            && ObPathUtil::is_upper_axis(location2->get_axis())) {
          ret = OB_ERR_PARSER_SYNTAX; // ORA-31011: XML parsing failed
          LOG_WARN("Function call with invalid number of arguments", K(ret), K(location2->get_axis()));
        }
        if (OB_FAIL(ret)) {
        } else if (location1->has_filter_
          || location1->get_seek_type() != ObSeekType::TEXT
          || ObPathUtil::is_upper_axis(location1->get_axis())) { // do not need check
        } else if (location1->get_axis() == ObPathNodeAxis::SELF) {
          if (ObPathUtil::is_upper_axis(location2->get_axis())) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("Invalid XPATH expression.", K(ret), K(location2->get_axis()));
          }
        } else if (ObPathUtil::is_down_axis(location2->get_axis())) {
          ret = OB_ERR_WRONG_VALUE;
          LOG_WARN("Invalid Input.", K(ret), K(location2->get_axis()));
        }
      } else if (patharg_type == ObPathArgType::IN_FUNCTION) {
        if (i == 0 && (location1->get_axis() == ObPathNodeAxis::SELF
            || ObPathUtil::is_upper_axis(location1->get_axis()))) {
          ret = OB_OP_NOT_ALLOW;
          LOG_WARN("Given XPATH expression not supported", K(ret), K(location1->get_axis()));
        } else if (location2->get_axis() == ObPathNodeAxis::SELF
            || ObPathUtil::is_upper_axis(location2->get_axis())) {
          ret = OB_OP_NOT_ALLOW;
          LOG_WARN("Given XPATH expression not supported", K(ret), K(location1->get_axis()));
        }
      } // in filter, do not check
    } // not location do not check
  } // end for
  return ret;
}

bool ObPathParser::path_prefix_match(const char *str)
{
  bool ret_bool = false;
  ObXPathUtil::skip_whitespace(expression_, index_);
  if (index_ + strlen(str) <= len_) {
    ObString substr(strlen(str), expression_.ptr() + index_);
    ret_bool = substr.prefix_match(str);
  }
  return ret_bool;
}

// and or div mod
bool ObPathParser::is_prefix_match_letter_operator()
{
  bool ret_bool = false;
  if (index_ + 1 < len_) {
    switch(expression_[index_]) {
      case 'a': {
        ret_bool = path_prefix_match(ObPathItem::AND);
        if (ret_bool) {
          if ((index_ > 1 && (expression_[index_ - 1] == ' ' || isdigit(expression_[index_ - 1])))
          && (index_ + strlen(ObPathItem::AND) < len_ && expression_[index_ + strlen(ObPathItem::AND)] == ' ')) {
            ret_bool = true;
          } else {
            ret_bool = false;
          }
        }
        break;
      }
      case 'o': {
        ret_bool = path_prefix_match(ObPathItem::OR);
        if (ret_bool) {
          if ((index_ > 1 && (expression_[index_ - 1] == ' ' || isdigit(expression_[index_ - 1])))
          && (index_ + strlen(ObPathItem::OR) < len_ && expression_[index_ + strlen(ObPathItem::OR)] == ' ')) {
            ret_bool = true;
          } else {
            ret_bool = false;
          }
        }
        break;
      }
      case 'd': {
        ret_bool = path_prefix_match(ObPathItem::DIV);
        if (ret_bool) {
          if ((index_ > 1 && (expression_[index_ - 1] == ' ' || isdigit(expression_[index_ - 1])))
          && (index_ + strlen(ObPathItem::DIV) < len_ && expression_[index_ + strlen(ObPathItem::DIV)] == ' ')) {
            ret_bool = true;
          } else {
            ret_bool = false;
          }
        }
        break;
      }
      case 'm': {
        ret_bool = path_prefix_match(ObPathItem::MOD);
        if (ret_bool) {
          if ((index_ > 1 && (expression_[index_ - 1] == ' ' || isdigit(expression_[index_ - 1])))
          && (index_ + strlen(ObPathItem::MOD) < len_ && expression_[index_ + strlen(ObPathItem::MOD)] == ' ')) {
            ret_bool = true;
          } else {
            ret_bool = false;
          }
        }
        break;
      }
      default: {
        break;
      }
    }
  }

  return  ret_bool;
}

bool ObPathParser::is_prefix_match_function()
{
  bool ret_bool = false;
  uint64_t old_index = index_;
  ObFuncType func_type = ObFuncType::PN_FUNC_ERROR;
  int ret = parse_func_type(func_type);
  if (OB_FAIL(ret) || func_type == ObFuncType::PN_FUNC_ERROR) {
    ret_bool = false;
  } else {
    ret_bool = true;
  }
  index_ = old_index;
  return ret_bool;
}

bool ObPathParser::is_path_end_with_brace()
{
  bool ret_bool = false;
  int end = index_;
  int start = len_ - 1;
  while (start > end && ObXPathUtil::is_whitespace(expression_[start])) --start;
  ret_bool = (expression_[start] == ObPathItem::BRACE_END);
  return ret_bool;
}

bool ObPathParser::is_function_path()
{
  bool ret_bool = false;
  ObXPathUtil::skip_whitespace(expression_, index_);
  if (ObPathParserUtil::is_function_start_char(expression_[index_])
      && is_path_end_with_brace() && is_prefix_match_function()) {
    ret_bool = true;
  }
  return ret_bool;
}

/*
'comment'
| 'text'
| 'processing-instruction'
| 'node'
*/
int ObPathParser::check_nodetest(const ObString& str, ObSeekType& seek_type, char*& arg, uint64_t& arg_len)
{
  INIT_SUCC(ret);
  seek_type = ObSeekType::ERROR_SEEK;
  if (index_ >= len_ || str.length() < 1) {
    ret = OB_ARRAY_OUT_OF_RANGE;
    LOG_WARN("wrong path expression", K(ret), K(index_));
  } else {
    bool is_nodetest = false;
    switch (str[0]) {
      case 'c': {
        if (str.prefix_match(ObPathItem::COMMENT)) {
          seek_type = ObSeekType::COMMENT;
          is_nodetest = true;
        }
        break;
      }
      case 'n': {
        if (str.prefix_match(ObPathItem::NODE)) {
          seek_type = ObSeekType::NODES;
          is_nodetest = true;
        }
        break;
      }
      case 't': {
        if (str.prefix_match(ObPathItem::TEXT)) {
          seek_type = ObSeekType::TEXT;
          is_nodetest = true;
        }
        break;
      }
      case 'p': {
        if (str.prefix_match(ObPathItem::PROCESSING_INSTRUCTION)) {
          seek_type = ObSeekType::PROCESSING_INSTRUCTION;
          is_nodetest = true;
          ObXPathUtil::skip_whitespace(expression_, index_);
          if (index_ + 2 < len_ && is_literal_begin()) {
            if (OB_FAIL(get_xpath_literal(arg, arg_len))) {
              LOG_WARN("fail to get literal of PROCESSING_INSTRUCTION", K(ret), K(index_));
            }
          }
        }
        break;
      }
      default: {
        is_nodetest = false;
        break;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (!is_nodetest) {
      seek_type = ObSeekType::ERROR_SEEK;
    } else {
      ObXPathUtil::skip_whitespace(expression_, index_);
      if (index_ < len_ && expression_[index_] == ObPathItem::BRACE_END) {
        ++index_;
      } else {
        ret = OB_INVALID_DATA;
        LOG_WARN("must hava ')'", K(ret), K(index_));
      }
    }
  }
  return ret;
}

int ObPathParser::to_string(ObStringBuffer& str)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(root_node_)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret), K(str));
  } else {
    if (OB_FAIL(root_node_->node_to_string(str))) {
      LOG_WARN("fail to string", K(ret), K(str));
    }
  }
  return ret;
}

int ObPathParser::parse_path(ObPathArgType patharg_type)
{
  INIT_SUCC(ret);
  ObXPathUtil::skip_whitespace(expression_, index_);
  if (index_ < len_) {
    ObPathNode* path_node = nullptr;
    if (parser_type_ == ObParserType::PARSER_XML_PATH) {
      if (OB_FAIL(parse_filter_node(path_node, patharg_type))) {
        LOG_WARN("parse failed", K(ret), K(index_), K(expression_));
      } else {
        root_node_ = path_node;
      }
    } // TODO: else if (parser_type_ == ObParserType::PARSER_JSON_PATH)
  } else {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("path is null", K(ret), K(index_), K(expression_));
  }
  return ret;
}

int ObPathParser::parse_location_path(ObPathArgType patharg_type)
{
  INIT_SUCC(ret);
  ObPathRootNode* root_node = nullptr;
  if (OB_FAIL(alloc_root_node(root_node))) {
    LOG_WARN("allocate row buffer failed at path_node", K(ret), K(index_), K(expression_));
  } else {
    root_node = new (root_node) ObPathRootNode(ctx_, parser_type_);
    root_node_ = root_node;
  }
  while (index_ < len_ && OB_SUCC(ret)) {
    if (OB_FAIL(parse_xpath_node(patharg_type))) {
      bad_index_ = index_;
      LOG_WARN("fail to parse Path Expression!", K(ret), K(index_));
    } else {
      ObXPathUtil::skip_whitespace(expression_, index_);
    }
  } // end while
  if (OB_FAIL(ret)) {
  } else if (OB_FALSE_IT(root_node->contain_relative_path_ = !(root_node->is_abs_subpath()))) {
  } else if (OB_FALSE_IT(root_node->is_abs_path_ = root_node->is_abs_subpath())) {
  } else if (patharg_type != ObPathArgType::IN_FILTER
            && root_node_->size() > 1 && OB_FAIL(check_is_legal_xpath(patharg_type))) {
    LOG_WARN("illegal Path Expression!", K(ret), K(index_));
  } else if (root_node->need_trans_ > 0) {
    ObPathNode* op_root = nullptr;
    if (OB_FAIL(trans_to_filter_op(root_node, root_node->need_trans_, true, op_root))) {
      LOG_WARN("Converting location to filter op failed!", K(ret), K(root_node->need_trans_));
    } else {
      root_node_ = op_root;
    }
  }
  return ret;
}

int ObPathParser::trans_to_filter_op(ObPathRootNode*& origin_root, int filter_num, bool is_first, ObPathNode*& op_root)
{
  INIT_SUCC(ret);
  int filter_idx = -1;
  // left node must be a location node, start with origin_root
  // right node could be a location node or another filter_op, if is filter_op, convert later
  ObPathRootNode* right = nullptr;
  ObPathFilterOpNode* op = nullptr;
  if (OB_ISNULL(origin_root)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret));
  } else if (OB_FAIL(alloc_filter_op_node(op))) {
    LOG_WARN("allocate row buffer failed at path_node", K(ret), K(index_), K(expression_));
  } else {
    op = new (op) ObPathFilterOpNode(ctx_, parser_type_);
    if (!is_first) {
      origin_root->is_abs_path_ = false;
    }
    op->init_left(origin_root);
  }
  for (int i = 0; OB_SUCC(ret) && i < origin_root->size() && filter_num > 0;) {
    ObPathNode* tmp = static_cast<ObPathNode*>(origin_root->member(i));
    if (OB_ISNULL(tmp)) {
      ret = OB_BAD_NULL_ERROR;
      LOG_WARN("should not be null", K(ret));
    } else if (tmp->get_node_type().is_location()) {
      ObPathLocationNode* location = static_cast<ObPathLocationNode*>(tmp);
      // if not the first time of converting, must start search with relative_path
      if (i == 0 && !is_first && filter_idx < 0) {
        location->is_absolute_ = false;
      }
      // first location with filter
      if (filter_idx < 0 && location->has_filter_) {
        filter_idx  = i;
        for (int j = 0; OB_SUCC(ret) && j < location->size();) {
          ObPathNode* tmp_filter = static_cast<ObPathNode*>(location->member(j));
          if (OB_FAIL(op->append_filter(tmp_filter))) {
            LOG_WARN("fail to append filter", K(ret));
          } else if (OB_FAIL(location->remove(j))) {
            LOG_WARN("fail to remove filter", K(ret));
          }
        }
        location->has_filter_ = false;
        // if the first location with filter is the last location node, right arg of op is null
        // and there must only on location with filter
        ++i;
        if (i == origin_root->size()) {
          if (filter_num != 1) {
            ret =  OB_ERR_UNEXPECTED;
            LOG_WARN("there must only on location node with filter", K(ret), K(filter_num));
          } else {
            op->init_left(origin_root);
            --filter_num;
          }
        } else if (OB_FAIL(alloc_root_node(right))) {
          LOG_WARN("fail to alloc right", K(ret));
        } else {
          // if not the last location, need a new root for right arg
          right = new (right) ObPathRootNode(ctx_, parser_type_);
          right->is_abs_path_ = false;
        }
      } else if (filter_idx >= 0) { // location after filter
        if (i != filter_idx + 1 || OB_ISNULL(right)) {
          ret =  OB_ERR_UNEXPECTED;
          LOG_WARN("wrong idx", K(ret), K(i), K(filter_idx));
        } else if (OB_FAIL(origin_root->remove(i))) {
          LOG_WARN("fail to append location", K(ret));
        } else if (OB_FAIL(right->append(location))) {
          LOG_WARN("fail to append location", K(ret));
        } else if (right->size() == 1) {
          location->is_absolute_ = false;
        }
        if (origin_root->size() == i) {
          --filter_num;
        }
      } else {
        ++i;
      }
    } else {
      ret =  OB_ERR_UNEXPECTED;
      LOG_WARN("wrong path node type", K(ret));
    }
  } // end for

  if (OB_FAIL(ret)) {
  } else if (filter_num == 0) {
    op->init_right(right);
    op_root = op;
  } else {
    ObPathNode* right_op = nullptr;
    if (OB_FAIL(trans_to_filter_op(right, filter_num, false, right_op))) {
      LOG_WARN("fail to get right op", K(ret));
    } else if (OB_NOT_NULL(right_op)){
      op->init_right(right_op);
      op_root = op;
    }
  }
  return ret;
}

int ObPathParser::parse_xpath_node(ObPathArgType patharg_type)
{
  INIT_SUCC(ret);
  ObXPathUtil::skip_whitespace(expression_, index_);

  if (OB_ISNULL(root_node_)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("root is null", K(ret), K(index_), K(expression_));
  } else if (index_ >= len_) {
    ret = OB_ARRAY_OUT_OF_RANGE;
    LOG_WARN("wrong path expression", K(ret), K(index_));
  } else if (path_prefix_match(ObPathItem::DOUBLE_SLASH)) {
    ++index_;
    if (OB_FAIL(parse_double_slash_node())) {
      LOG_WARN("failed to parse location node.", K(ret), K(index_), K(expression_));
    }
    is_first_node_ = false;
  } else if (expression_[index_] == ObPathItem::SLASH) {
    ++index_;
    if (OB_FAIL(parse_location_node(true))) {
      LOG_WARN("failed to parse location node.", K(ret), K(index_), K(expression_));
    }
    is_first_node_ = false;
  } else if (is_first_node_) {
    if (OB_FAIL(parse_primary_expr_node(patharg_type))) {
      LOG_WARN("failed to parse location node.", K(ret), K(index_), K(expression_));
    }
    is_first_node_ = false;
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("failed to parse location node.", K(ret), K(index_), K(expression_));
  }

  return ret;
}

int ObPathParser::parse_location_node(bool is_absolute)
{
  INIT_SUCC(ret);
  ObXPathUtil::skip_whitespace(expression_, index_);
  if (index_ >= len_) {
    // if is_first_node == true, in this case, the path expression is '/', do nothing and return root
    if (!is_first_node_) {
      ret = OB_ARRAY_OUT_OF_RANGE;
      LOG_WARN("wrong path expression", K(ret), K(index_));
    }
  // location node: must be '/' + axis + nodetest + filter
  // .. and . have definite axis and nodetest
  // if not .. or . , should parse axis first and then nodetest
  } else if (path_prefix_match(ObPathItem::DOUBLE_DOT)) {
    if (OB_FAIL(parse_double_dot_node(is_absolute))) {
      LOG_WARN("fail to parse '..' ", K(ret), K(index_));
    }
  } else if (expression_[index_] == ObPathItem::DOT) {
    if (OB_FAIL(parse_single_dot_node(is_absolute))) {
      LOG_WARN("fail to parse '.' ", K(ret), K(index_));
    }
  } else if (OB_FAIL(parse_non_abbrevited_location_node(is_absolute))) {
    LOG_WARN("fail to parse absolute_location ", K(ret), K(index_));
  }
  return ret;
}

int ObPathParser::parse_non_abbrevited_location_node(bool is_absolute)
{
  INIT_SUCC(ret);
  ObPathLocationNode* location_node = nullptr;
  if (OB_FAIL(alloc_location_node(location_node))) {
    LOG_WARN("fail to alloc location node", K(ret), K(index_), K(expression_));
  } else {
    location_node = new (location_node) ObPathLocationNode(ctx_, parser_type_);
    location_node->is_absolute_ = is_absolute;
    if (OB_FAIL(location_node->init(ObLocationType::PN_KEY))) {
      LOG_WARN("fail to init ellipsis_node", K(ret), K(index_), K(expression_));
    } else if (OB_FAIL(parse_axis_info(location_node))) {
      LOG_WARN("fail to parse_axis_info ", K(ret), K(index_));
    } else if (OB_FAIL(parse_nodetest_info(location_node))) {
      LOG_WARN("fail to parse_axis_info ", K(ret), K(index_));
    } else if (index_ < len_ && expression_[index_] == ObPathItem::BEGIN_ARRAY) {
      while (OB_SUCC(ret) && index_ < len_ && expression_[index_] == ObPathItem::BEGIN_ARRAY) {
        ObPathNode* filter = nullptr;
        if (OB_FAIL(parse_filter_node(filter, ObPathArgType::IN_FILTER))) {
          LOG_WARN("fail to parse filter", K(ret), K(index_));
        } else if (OB_FAIL(location_node->append(filter))) {
          LOG_WARN("fail to append filter", K(ret), K(index_));
        }
        ObXPathUtil::skip_whitespace(expression_, index_);
      }
      if (OB_SUCC(ret)) {
        location_node->has_filter_ = true;
        ObPathRootNode* root_node = static_cast<ObPathRootNode*>(root_node_);
        root_node->need_trans_++;
      }
    }
    // if successed, add to root
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(root_node_->append(location_node))) {
      LOG_WARN("fail to append location node", K(ret), K(index_), K(expression_));
    }
  }
  return ret;
}

int ObPathParser::parse_func_type(ObFuncType& func_type)
{
  INIT_SUCC(ret);
  // now there are only three function: position, last, count
  // todo: add more func
  func_type = ObFuncType::PN_FUNC_ERROR;
  if (index_ + 3 > len_) {
    ret = OB_ARRAY_OUT_OF_RANGE;
    LOG_WARN("index out of range!", K(ret), K(index_), K(expression_));
  } else {
    switch(expression_[index_]) {
      case 'b': {
        if (path_prefix_match("boolean")) {
          func_type = ObFuncType::PN_BOOLEAN_FUNC;
        }
        break;
      }
      case 'c': {
        if (path_prefix_match("contains")) {
          func_type = ObFuncType::PN_CONTAINS;
        } else if (path_prefix_match("concat")) {
          func_type = ObFuncType::PN_CONCAT;
        } else if (path_prefix_match("count")) {
          func_type = ObFuncType::PN_COUNT;
        }
        break;
      }
      case 'f': {
        if (path_prefix_match("false")) {
          func_type = ObFuncType::PN_FALSE;
        } else if (path_prefix_match("floor")) {
          func_type = ObFuncType::PN_FLOOR;
        }
        break;
      }
      case 'l': {
        if (path_prefix_match("local-name")) {
          func_type = ObFuncType::PN_LOCAL_NAME;
        } else if (path_prefix_match("last")) {
          func_type = ObFuncType::PN_LAST;
        } else if (path_prefix_match("lang")) {
          func_type = ObFuncType::PN_LANG;
        }
        break;
      }
      case 'n': {
        if (path_prefix_match("normalize-space")) {
          func_type = ObFuncType::PN_NORMALIZE_SPACE;
        } else if (path_prefix_match("namespace-uri")) {
          func_type = ObFuncType::PN_NS_URI;
        } else if (path_prefix_match("number")) {
          func_type = ObFuncType::PN_NUMBER_FUNC;
        } else if (path_prefix_match("name")) {
          func_type = ObFuncType::PN_NAME;
        } else if (path_prefix_match("not")) {
          func_type = ObFuncType::PN_NOT_FUNC;
        }
        break;
      }
      case 'p': {
        if (path_prefix_match("position")) {
          func_type = ObFuncType::PN_POSITION;
        }
        break;
      }
      case 'r': {
        if (path_prefix_match("round")) {
          func_type = ObFuncType::PN_ROUND;
        }
        break;
      }
      case 's': {
        if (path_prefix_match("string-length")) {
          func_type = ObFuncType::PN_LENGTH;
        } else if (path_prefix_match("substring")) {
          func_type = ObFuncType::PN_SUBSTRING_FUNC;
        } else if (path_prefix_match("string")) {
          func_type = ObFuncType::PN_STRING_FUNC;
        } else if (path_prefix_match("sum")) {
          func_type = ObFuncType::PN_SUM;
        }
        break;
      }
      case 't': {
        if (path_prefix_match("true")) {
          func_type = ObFuncType::PN_TRUE;
        }
        break;
      }
      default: {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("wrong func name!", K(ret), K(index_), K(expression_));
        break;
      }
    }
    if (func_type != ObFuncType::PN_FUNC_ERROR) {
      index_ += func_name_len[func_type - ObFuncType::PN_ABS];
      ObXPathUtil::skip_whitespace(expression_, index_);
      if (OB_FAIL(ret)) {
      } else if (index_ >= len_ || expression_[index_] != ObPathItem::BRACE_START) {
        ret = OB_ARRAY_OUT_OF_RANGE;
        LOG_WARN("index out of range or not brace after function!", K(ret));
      } else {
        ++index_;
        ObXPathUtil::skip_whitespace(expression_, index_);
      }
    }
  }
  return ret;
}

int ObPathParser::parse_func_arg(ObPathFuncNode*& func_node, ObPathArgType patharg_type)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(func_node)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret));
  } else if (func_node->get_max_arg_num() > 0) {
    while (OB_SUCC(ret) && index_ < len_ && expression_[index_] != ObPathItem::BRACE_END) {
      ObPathNode* arg = nullptr;
      if (OB_FAIL(parse_arg(arg, patharg_type, false, false))) {
        LOG_WARN("fail to get arg", K(ret), K(index_), K(expression_));
      } else if (OB_FAIL(func_node->append(arg))) {
        LOG_WARN("fail to append arg", K(ret), K(index_), K(expression_));
      } else {
        ObXPathUtil::skip_whitespace(expression_, index_);
        if (index_ < len_ && expression_[index_] == ',') {
          ++index_;
        }
      }
      ObXPathUtil::skip_whitespace(expression_, index_);
    } // end while
  }

  // must end with ')'
  if (OB_FAIL(ret)) {
  } else if (index_ < len_ && expression_[index_] == ObPathItem::BRACE_END) {
    ++index_;
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("must end with brace!", K(ret), K(index_), K(expression_));
  }

  // check arg_num
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(func_node->check_is_legal_arg())) {
    LOG_WARN("Function call with invalid number of arguments", K(ret));
  }
  return ret;
}

int ObPathParser::parse_func_node(ObPathArgType patharg_type)
{
  INIT_SUCC(ret);
  ObFuncType func_type = ObFuncType::PN_FUNC_ERROR;
  if (OB_FAIL(parse_func_type(func_type))) {
  } else if (func_type != ObFuncType::PN_FUNC_ERROR) {
    ObPathFuncNode* func_node = nullptr;
    if (OB_FAIL(alloc_func_node(func_node))) {
      LOG_WARN("allocate row buffer failed at path_node", K(ret), K(index_), K(expression_));
    } else {
      func_node = new (func_node) ObPathFuncNode(ctx_, parser_type_);
      if (OB_FAIL(func_node->init(func_type))) {
        LOG_WARN("fail to init ellipsis_node", K(ret), K(index_), K(expression_));
      } else if (patharg_type == ObPathArgType::NOT_SUBPATH
                && ObPathParserUtil::is_func_must_in_pred(func_type)) {
        //ORA-31012: Given XPATH expression not supported
        ret = OB_OP_NOT_ALLOW;
        LOG_WARN("Given XPATH expression not supported", K(ret), K(index_), K(expression_));
      }
      if (OB_SUCC(ret) && patharg_type == ObPathArgType::NOT_SUBPATH) {
        patharg_type = ObPathArgType::IN_FUNCTION;
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(parse_func_arg(func_node, patharg_type))) {
        LOG_WARN("fail to parse function arg", K(ret), K(index_), K(expression_));
      } else if (OB_FAIL(func_node->checek_cache_and_abs())) {
        LOG_WARN("fail to init function bool", K(ret));
      } else {
        root_node_ = func_node;
      }
    }
  } else {
    ret = OB_ARRAY_OUT_OF_RANGE;
    LOG_WARN("index out of range!", K(ret), K(index_), K(expression_));
  }
  return ret;
}

int ObPathParser::parse_primary_expr_node(ObPathArgType patharg_type)
{
  INIT_SUCC(ret);
  ObXPathUtil::skip_whitespace(expression_, index_);
  // 记得index + n
  bool is_cmp = false;
  if (index_ < len_) {
    if (expression_[index_] == ObPathItem::DOLLAR) {
      ret = OB_ERR_WRONG_VALUE_FOR_VAR;
      LOG_WARN("Invalid reference.", K(ret), K(index_));
    } else {
      if (OB_FAIL(parse_location_node(false))) {
        LOG_WARN("fail to parse function.", K(ret), K(index_));
      } // is function
    }
  } // all space, not error
  return ret;
}

int ObPathParser::get_xpath_ident(char*& str, uint64_t& length, bool& is_func)
{
  INIT_SUCC(ret);
  uint64_t start = 0, end = 0, str_len = 0;

  if (OB_ISNULL(allocator_)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret));
  } else if (index_ < len_) {
    start = index_;
    while (OB_SUCC(ret) && index_ < len_ && !ObPathParserUtil::is_xpath_ident_terminator(expression_[index_])) {
      ++index_;
    }
    if (OB_FAIL(ret)) {
    } else {
      end = index_ - 1;
      ObXPathUtil::skip_whitespace(expression_, index_);
      if (index_ < len_ && expression_[index_] == ObPathItem::BRACE_START) {
        // fun_name + ()
        is_func = true;
        ++index_;
      }
    }
  } else {
    ret = OB_ARRAY_OUT_OF_RANGE;
    LOG_WARN("index out of range!", K(ret), K(index_), K(expression_));
  }

  if (OB_SUCC(ret)) {
    if (end < start) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("get keyname: end<start", K(ret), K(start), K(end), K(expression_));
    } else {
      str_len = end - start + 1;
      char* start_ptr = expression_.ptr() + start;
      length = is_func ? str_len + 1 : str_len;
      str = static_cast<char*> (allocator_->alloc(length));
      if (OB_ISNULL(str)) {
        ret = (length > 0)? OB_ALLOCATE_MEMORY_FAILED : OB_ERR_NULL_VALUE;
        LOG_WARN("fail to allocate memory for member_name.",K(ret), K(str_len),K(start_ptr));
      } else {
        MEMCPY(str, start_ptr, str_len);
        if (is_func) str[str_len] = ObPathItem::BRACE_START;
      }
    }
  }

  return ret;
}

int ObPathParser::get_xpath_literal(char*& str, uint64_t& length)
{
  INIT_SUCC(ret);
  bool is_double_quoted = true;
  uint64_t start = 0, end = 0, str_len = 0;
  if (index_ < len_ && is_literal_begin()) {
    if (expression_[index_] == ObPathItem::SINGLE_QUOTE) is_double_quoted = false;
    ++index_;
    start = index_;
    while (OB_SUCC(ret) && index_ < len_ && end == 0 ) {
      if ((expression_[index_] == ObPathItem::DOUBLE_QUOTE && is_double_quoted)
              || (expression_[index_] == ObPathItem::SINGLE_QUOTE && !is_double_quoted)) {
        end = index_ - 1;
        ++index_;
      } else {
        ++index_;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (end == 0 && index_ == len_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("should end with DOUBLE_QUOTE/SINGLE_QUOTE!", K(ret), K(index_), K(expression_));
    }
  } else {
    ret = OB_ARRAY_OUT_OF_RANGE;
    LOG_WARN("index out of range!", K(ret), K(index_), K(expression_));
  }

  if (OB_SUCC(ret)) {
    if (end < start) {
      // could be ""
      if (expression_[end] == expression_[start]
          && ((expression_[end] == ObPathItem::DOUBLE_QUOTE && is_double_quoted)
          || (expression_[end] == ObPathItem::SINGLE_QUOTE && !is_double_quoted))) {
        str = nullptr;
        length = 0;
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("get keyname: end<start", K(ret), K(start), K(end), K(expression_));
      }
    } else {
      str_len = end - start + 1;
      char* start_ptr = expression_.ptr() + start;
      str = static_cast<char*> (allocator_->alloc(str_len));
      if (OB_ISNULL(str)) {
        ret = (str_len > 0)? OB_ALLOCATE_MEMORY_FAILED : OB_ERR_NULL_VALUE;
        LOG_WARN("fail to allocate memory for member_name.",K(ret), K(str_len),K(start_ptr));
      } else {
        MEMCPY(str, start_ptr, str_len);
        length = str_len;
      }
    }
  }
  return ret;
}

int ObPathParser::get_xpath_number(double& num)
{
  INIT_SUCC(ret);
  char* num_ptr = expression_.ptr() + index_;
  uint64_t num_len = 0;
  bool in_loop = true;

  while (in_loop && OB_SUCC(ret) && index_ < len_) {
    if (expression_[index_] == ObPathItem::DOT
    || isdigit(expression_[index_])) {
      ++index_;
      ++num_len;
    } else {
      in_loop = false;
    }
  }
  if (OB_SUCC(ret)) {
    double ret_val = 0.0;
    char *endptr = NULL;
    int err = 0;
    ret_val = ObCharset::strntod(num_ptr, num_len, &endptr, &err);
    if (err == 0) {
      num = ret_val;
    } else {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid double value", K(num_ptr), K(num_len), K(ret));
    }
  }

  return ret;
}

int ObPathParser::get_subpath_str(bool is_filter, ObString& subpath)
{
  INIT_SUCC(ret);
  char* subpath_ptr = expression_.ptr() + index_;
  uint64_t subpath_start = index_;
  uint64_t subpath_len = 0;
  bool in_loop = true;

  while (in_loop && OB_SUCC(ret) && index_ < len_) {
    ObXPathUtil::skip_whitespace(expression_, index_);
    if (index_ >= len_) {
      in_loop = false;
    } else if (is_literal_begin()) {
      if (OB_FAIL(jump_over_quote())) {
        LOG_WARN("invalid quoted value", K(ret));
      }
    } else if (expression_[index_] == ObPathItem::BEGIN_ARRAY) {
      if (OB_FAIL(jump_over_brace(false))) {
        LOG_WARN("invalid value", K(ret));
      }
    } else if (expression_[index_] == ObPathItem::BRACE_START) {
      if (OB_FAIL(jump_over_brace(true))) {
        LOG_WARN("invalid value", K(ret));
      }
    } else {
      ObXPathUtil::skip_whitespace(expression_, index_);
      if (OB_FAIL(ret) || index_ > len_) {
      } else if (!is_filter && (expression_[index_] == ',' || expression_[index_] == ')')) {
        in_loop = false;
      } else if (is_filter &&  (expression_[index_] == ObPathItem::MINUS)) {
        if (index_ - 1 > 0 && ObPathParserUtil::is_xml_name_start_char(expression_[index_ - 1])
          && index_ + 1 < len_ && ObPathParserUtil::is_xml_name_start_char(expression_[index_ + 1])) {
          ++index_;
        } else {
          in_loop = false;
        }
      } else if (is_filter && expression_[index_] == ObPathItem::WILDCARD) {
        if (is_last_letter_location(index_) || is_negtive()) {
          ++index_;
        } else {
          in_loop = false;
        }
      } else if (is_filter && (ObPathParserUtil::is_operator(expression_[index_])
                || expression_[index_] == ObPathItem::BRACE_END
                || expression_[index_] == ObPathItem::END_ARRAY)) {
        in_loop = false;
      } else if (is_prefix_match_letter_operator()) {
        in_loop = false;
      } else {
        subpath_len = index_ - subpath_start;
        ++index_;
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    subpath_len = (index_ < len_) ? (index_ - subpath_start) : (len_ - subpath_start);
    subpath = ObString(subpath_len, subpath_ptr);
  }
  return ret;
}

int ObPathParser::parse_subpath(ObString& subpath, ObPathNode*& node, bool is_filter, ObPathArgType patharg_type)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(allocator_)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret));
  } else {
    ObPathParser* subpath_parser =
    static_cast<ObPathParser*> (allocator_->alloc(sizeof(ObPathParser)));
    if (OB_ISNULL(subpath_parser)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate row buffer failed at location_node", K(ret), K(index_), K(expression_));
    } else {
      subpath_parser = new (subpath_parser) ObPathParser(ctx_, parser_type_, subpath, default_ns_, pass_var_);
      if (is_filter) {
        if (subpath_parser->is_function_path()) {
          if (OB_FAIL(subpath_parser->parse_func_node(patharg_type))) {
            bad_index_ = subpath_parser->bad_index_;
            LOG_WARN("fail to parse function.", K(ret), K(index_));
          } // is function
        } else if (OB_FAIL(subpath_parser->parse_location_path(patharg_type))) {
          bad_index_ = subpath_parser->bad_index_;
          LOG_WARN("fail to parse", K(ret));
        }
      } else if (OB_FAIL(subpath_parser->parse_path(IN_FUNCTION))) {
        bad_index_ = subpath_parser->bad_index_;
        LOG_WARN("fail to parse", K(ret));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_NOT_NULL(subpath_parser->get_root())) {
        node = subpath_parser->get_root();
      }
    }
  }
  return ret;
}

int ObPathParser::get_xpath_subpath(ObPathNode*& node, bool is_filter, ObPathArgType patharg_type)
{
  INIT_SUCC(ret);
  ObString subpath;
  if (OB_FAIL(get_subpath_str(is_filter, subpath))) {
    LOG_WARN("fail to get subpath str", K(ret));
  } else if (OB_FAIL(parse_subpath(subpath, node, is_filter, patharg_type))) {
    if (patharg_type != NOT_SUBPATH) ret = OB_ERR_PARSER_SYNTAX; // subpath parsing failed
    LOG_WARN("fail to parse subpath", K(ret));
  }
  return ret;
}

int ObPathParser::parse_namespace_info(ObPathLocationNode*& location, ObString& ns_str)
{
  INIT_SUCC(ret);
  if (OB_NOT_NULL(ns_str.ptr())) {
    location->set_prefix_ns_info(true);
    location->set_default_prefix_ns(false);
    if (0 == ns_str.compare("xmlns") || 0 == ns_str.compare("xml")) {
      if (OB_NOT_NULL(pass_var_) || OB_NOT_NULL(default_ns_.ptr())) {
        location->set_default_prefix_ns(true);
        location->set_check_ns_info(true);
      }
    } else if (OB_ISNULL(pass_var_)) {
      // ORA-31013: Invalid XPATH expression
      // no passing var, prefix ns is not allowed
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Invalid XPATH expression.", K(ret), K(index_), K(expression_));
    } else {
      ObDatum* pass_data = pass_var_->get_value(ns_str);
      if (OB_ISNULL(pass_data) || pass_data->is_null()) {
        // no passing var, prefix ns is not allowed
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("Invalid XPATH expression.", K(ret), K(index_), K(expression_));
      } else {
        ObString ns_str;
        if (OB_FAIL(ob_write_string(*allocator_, pass_data->get_string(), ns_str))) {
          LOG_WARN("fail to save ns str", K(ret));
        } else {
          location->set_ns_info(ns_str.ptr(), ns_str.length());
          location->set_check_ns_info(true);
        }
      }
    }
  } else {
    // if no prefix namespace, use default namespace
    location->set_prefix_ns_info(false);
    location->set_ns_info(default_ns_.ptr(), default_ns_.length());
  }
  return ret;
}

int ObPathParser::parse_nodetest_info(ObPathLocationNode*& location)
{
  INIT_SUCC(ret);
  ObXPathUtil::skip_whitespace(expression_, index_);
  if (index_ >= len_) {
    ret = OB_ARRAY_OUT_OF_RANGE;
    LOG_WARN("wrong path expression", K(ret), K(index_));
  } else {
    // if have '(', must be nodetest
    // xmlns:tag, couldn't have space before ':'
    // so, split on ':'
    char* name = nullptr;
    uint64_t name_len = 0;
    bool is_nodetest = false;
    if (OB_FAIL(get_xpath_ident(name, name_len, is_nodetest))) {
      LOG_WARN("wrong path expression", K(ret), K(index_));
    } else {
      ObString name_ident(name_len, name);
      ObString ns_str = name_ident.split_on(ObPathItem::COLON);
      name_len = name_ident.length();
      if (name_ident.length() < 1) {
        ret = OB_INVALID_ARGUMENT; // could be tagname or nodetest or wildcard, must >= 1
        LOG_WARN("wrong name ident", K(ret), K(ns_str));
      } else if (OB_FAIL(parse_namespace_info(location, ns_str))) {
        LOG_WARN("fail to set ns info", K(ret), K(index_));
      } else if (is_nodetest) { // node(), text(), pi(), comment()
        // get_nodetest
        ObSeekType nodetest = ObSeekType::ERROR_SEEK;
        char* arg = nullptr;
        uint64_t arg_len = 0;
        if (OB_FAIL(check_nodetest(name_ident, nodetest, arg, arg_len))) {
          LOG_WARN("fail to get nodetest", K(ret), K(name_ident));
        } else if (nodetest != ObSeekType::ERROR_SEEK) {
          location->set_nodetest_by_name(nodetest, arg, arg_len);
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("wrong nodetest arg", K(ret), K(name_ident));
        }
        if (OB_FAIL(ret)) {
        } else if (ns_str.length() > 0) {
          allocator_->free(location->get_ns_name().ptr());
          if (location->get_seek_type() != ObSeekType::TEXT) {
            ret = OB_ERR_PARSER_SYNTAX; // ORA-31011: XML parsing failed
            LOG_WARN("Function call with invalid arguments", K(ret));
          } else if (ObPathUtil::is_upper_axis(location->get_axis())) {
            ret = OB_ERR_PARSER_SYNTAX; // ORA-31011: XML parsing failed
            LOG_WARN("Function call with invalid arguments", K(ret));
          } else {
            location->set_prefix_ns_info(true);
          }
        }

        // oracle adaptation:
        if (OB_SUCC(ret) && location->get_axis() == ATTRIBUTE
          && location->get_seek_type() == ObSeekType::TEXT) {
          location->set_axis(CHILD);
        }
      } else if (name_len == 1 && name_ident[0] == ObPathItem::WILDCARD) {
        location->set_wildcard_info(true);
        location->set_nodetest_by_axis();
      } else if (!(ObPathParserUtil::check_is_legal_tagname(name_ident.ptr(), name_ident.length()))) {
        ret  = OB_INVALID_ARGUMENT;
        LOG_WARN("wrong element name", K(ret), K(name_ident));
      } else {
        // legal tagname: could be namespace, attribute or element
        location->set_key_info(name_ident.ptr(), name_ident.length());
        // must with tag_name and no wildcard
        location->set_wildcard_info(false);
        // if axis is attribute or ns, set seek type , tagname is not for element
        location->set_nodetest_by_axis();
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(location->set_check_ns_by_nodetest(allocator_, default_ns_))) {
        LOG_WARN("wrong type for ns", K(ret), K(name_ident));
      }
    }
  }
  return ret;
}

int ObPathParser::parse_axis_info(ObPathLocationNode*& location)
{
  INIT_SUCC(ret);
  ObXPathUtil::skip_whitespace(expression_, index_);
  if (OB_ISNULL(location)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret));
  } else if (index_ >= len_) {
    ret = OB_ARRAY_OUT_OF_RANGE;
    LOG_WARN("wrong path expression", K(ret), K(index_));
  } else {
    bool default_axis = true;
    switch (expression_[index_]) {
      case '@': {
        location->set_axis(ObPathNodeAxis::ATTRIBUTE);
        ++index_;
        default_axis = false;
        location->set_wildcard_info(true);
        break;
      }
      case 'a': {
        if (path_prefix_match(ObPathItem::ANCESTOR_OR_SELF)) {
          location->set_axis(ObPathNodeAxis::ANCESTOR_OR_SELF);
          index_ += strlen(ObPathItem::ANCESTOR_OR_SELF);
          default_axis = false;
        } else if (path_prefix_match(ObPathItem::ANCESTOR)) {
          location->set_axis(ObPathNodeAxis::ANCESTOR);
          index_ += strlen(ObPathItem::ANCESTOR);
          default_axis = false;
        } else if (path_prefix_match(ObPathItem::ATTRIBUTE)) {
          location->set_axis(ObPathNodeAxis::ATTRIBUTE);
          index_ += strlen(ObPathItem::ATTRIBUTE);
          default_axis = false;
        }
        break;
      }
      case 'c': {
        if (path_prefix_match(ObPathItem::CHILD)) {
          location->set_axis(ObPathNodeAxis::CHILD);
          index_ += strlen(ObPathItem::CHILD);
          default_axis = false;
        }
        break;
      }
      case 'd': {
        if (path_prefix_match(ObPathItem::DESCENDANT_OR_SELF)) {
          location->set_axis(ObPathNodeAxis::DESCENDANT_OR_SELF);
          index_ += strlen(ObPathItem::DESCENDANT_OR_SELF);
          default_axis = false;
        } else if (path_prefix_match(ObPathItem::DESCENDANT)) {
          location->set_axis(ObPathNodeAxis::DESCENDANT);
          index_ += strlen(ObPathItem::DESCENDANT);
          default_axis = false;
        }
        break;
      }
      case 'f': {
        if (path_prefix_match(ObPathItem::FOLLOWING_SIBLING)) {
          location->set_axis(ObPathNodeAxis::FOLLOWING_SIBLING);
          index_ += strlen(ObPathItem::FOLLOWING_SIBLING);
          default_axis = false;
        } else if (path_prefix_match(ObPathItem::FOLLOWING)) {
          location->set_axis(ObPathNodeAxis::FOLLOWING);
          index_ += strlen(ObPathItem::FOLLOWING);
          default_axis = false;
        }
        break;
      }
      case 'n': {
        if (path_prefix_match(ObPathItem::NAMESPACE)) {
          location->set_axis(ObPathNodeAxis::NAMESPACE);
          index_ += strlen(ObPathItem::NAMESPACE);
          location->set_wildcard_info(true);
          default_axis = false;
        }
        break;
      }
      case 'p': {
        if (path_prefix_match(ObPathItem::PRECEDING_SIBLING)) {
          location->set_axis(ObPathNodeAxis::PRECEDING_SIBLING);
          index_ += strlen(ObPathItem::PRECEDING_SIBLING);
          default_axis = false;
        } else if (path_prefix_match(ObPathItem::PRECEDING)) {
          location->set_axis(ObPathNodeAxis::PRECEDING);
          index_ += strlen(ObPathItem::PRECEDING);
          default_axis = false;
        } else if (path_prefix_match(ObPathItem::PARENT)) {
          location->set_axis(ObPathNodeAxis::PARENT);
          index_ += strlen(ObPathItem::PARENT);
          default_axis = false;
        }
        break;
      }
      case 's': {
        if (path_prefix_match(ObPathItem::SELF)) {
          location->set_axis(ObPathNodeAxis::SELF);
          index_ += strlen(ObPathItem::SELF);
          default_axis = false;
        }
        break;
      }
      default: {
         break;
      }
    } // end switch
    // if didn't match any axis, use child axis
    if (default_axis) {
      location->set_axis(ObPathNodeAxis::CHILD);
    }
  }
  return ret;
}

int ObPathParser::parse_double_slash_node()
{
  INIT_SUCC(ret);
  uint64_t old_index = index_;
  ++index_;
  ObXPathUtil::skip_whitespace(expression_, index_);
  ObPathLocationNode* ellipsis_node = nullptr;
  if (index_ >= len_ || expression_[index_] == ObPathItem::SLASH) {
    ret = OB_ERR_PARSER_SYNTAX; // ORA-31011: XML parsing failed
    LOG_WARN("Function call with invalid arguments", K(ret));
  } else if (OB_FAIL(alloc_location_node(ellipsis_node))) {
    LOG_WARN("allocate row buffer failed at path_node", K(ret), K(index_), K(expression_));
  } else {
    ellipsis_node = new (ellipsis_node) ObPathLocationNode(ctx_, parser_type_);
    if (OB_FAIL(ellipsis_node->init(ObLocationType::PN_ELLIPSIS))) {
      LOG_WARN("fail to init ellipsis_node", K(ret), K(index_), K(expression_));
    } else {
      ObXPathUtil::skip_whitespace(expression_, index_);
      ellipsis_node->is_absolute_ = true;
      bool is_abb = false;
      if (index_ >= len_) {
        ret = OB_ARRAY_OUT_OF_RANGE;
        LOG_WARN("wrong path expression", K(ret), K(index_));
      } else if (path_prefix_match(ObPathItem::DOUBLE_DOT)) {
        index_ += strlen(ObPathItem::DOUBLE_DOT);
        ellipsis_node->set_axis(ObPathNodeAxis::PARENT);
        ellipsis_node->set_nodetest(ObSeekType::NODES);
        ellipsis_node->set_wildcard_info(true);
      } else if (expression_[index_] == ObPathItem::DOT) {
        is_abb = true;
        ++index_;
        ellipsis_node->set_axis(ObPathNodeAxis::SELF);
        ellipsis_node->set_nodetest(ObSeekType::NODES);
        ellipsis_node->set_wildcard_info(true);
      } else if (OB_FAIL(parse_axis_info(ellipsis_node))) {
        LOG_WARN("fail to parse_axis_info ", K(ret), K(index_));
      }

      // Compatible with Oracle:
      // if is down_axis or self_axis, combine "//" node and the filter after it
      // for example: "//a" equal to "/descendant-or-self::a"
      //              "//parent::a"  equal to "/descendant-or-self::node/parent::a"
      if (OB_FAIL(ret)) {
      } else if (is_abb || ObPathUtil::is_down_axis(ellipsis_node->get_axis())) {
        ObXPathUtil::skip_whitespace(expression_, index_);
        uint64_t idx_before_nodetest = index_;
        if ((index_ < len_ && expression_[index_] != ObPathItem::SLASH)
          && OB_FAIL(parse_nodetest_info(ellipsis_node))) {
          LOG_WARN("fail to parse_axis_info ", K(ret), K(index_));
        } else if (index_ < len_ && expression_[index_] == ObPathItem::BEGIN_ARRAY) {
          if (!is_abb) {
            while (OB_SUCC(ret) && index_ < len_ && expression_[index_] == ObPathItem::BEGIN_ARRAY) {
              ObPathNode* filter = nullptr;
              if (OB_FAIL(parse_filter_node(filter, ObPathArgType::IN_FILTER))) {
                LOG_WARN("fail to parse filter", K(ret), K(index_));
              } else if (OB_FAIL(ellipsis_node->append(filter))) {
                LOG_WARN("fail to append filter", K(ret), K(index_));
              }
              ObXPathUtil::skip_whitespace(expression_, index_);
            }
            if (OB_SUCC(ret)) {
              ellipsis_node->has_filter_ = true;
              ObPathRootNode* root_node = static_cast<ObPathRootNode*>(root_node_);
              root_node->need_trans_++;
            }
          } else {
            ret = OB_ERR_PARSER_SYNTAX; // ORA-31011: XML parsing failed
            LOG_WARN("Function call with invalid arguments", K(ret));
          }
        }
        if (is_abb && (idx_before_nodetest < index_) && ellipsis_node->get_wildcard_info()) {
          ObSeekType seek_type =  ellipsis_node->get_seek_type();
          if (seek_type == ObSeekType::TEXT) { // legal, do nothing
          } else if (seek_type == ObSeekType::ELEMENT) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid path expression", K(ret), K(index_), K(expression_));
          } else {
            ret = OB_ERR_PARSER_SYNTAX; // ORA-31011: XML parsing failed
            LOG_WARN("Function call with invalid arguments", K(ret));
          }
        }
      } else if (OB_FAIL(ellipsis_node->init(ObLocationType::PN_KEY,
                                              ObSeekType::NODES,
                                              ObPathNodeAxis::DESCENDANT_OR_SELF))) {
        LOG_WARN("fail to init ellipsis_node", K(ret), K(index_), K(expression_));
      } else {
        ellipsis_node->set_wildcard_info(true);
        ellipsis_node->is_absolute_ = true;
        index_ = old_index;
      }
    }
    // if successed, add to root
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(root_node_->append(ellipsis_node))) {
      LOG_WARN("fail to append location node", K(ret), K(index_), K(expression_));
    }
  }
  return ret;
}

int ObPathParser::parse_double_dot_node(bool is_absolute)
{
  INIT_SUCC(ret);
  ObPathLocationNode* parent_node = nullptr;
  if (OB_FAIL(alloc_location_node(parent_node))) {
    LOG_WARN("allocate row buffer failed at path_node", K(ret), K(index_), K(expression_));
  } else {
    // skip .. and space
    index_ += strlen(ObPathItem::DOUBLE_DOT);
    ObXPathUtil::skip_whitespace(expression_, index_);
    parent_node = new (parent_node) ObPathLocationNode(ctx_, parser_type_);
    parent_node->is_absolute_ = is_absolute;
    if (OB_FAIL(parent_node->init(ObLocationType::PN_KEY,
                                  ObSeekType::NODES,
                                  ObPathNodeAxis::PARENT))) {
      LOG_WARN("fail to init parent_node", K(ret), K(index_), K(expression_));
    } else {
      parent_node->set_wildcard_info(true);
      if (OB_FAIL(root_node_->append(parent_node))) {
        LOG_WARN("failed to append location node.", K(ret), K(index_), K(expression_));
      } else if (index_ < len_ && expression_[index_] != ObPathItem::SLASH) {
        // '/..' must be followed by a new step, if not end
        ret = OB_ERR_PARSER_SYNTAX; // ORA-31011: XML parsing failed
        LOG_WARN("Function call with invalid arguments", K(ret));
      }
    }
  }
  return ret;
}

int ObPathParser::parse_single_dot_node(bool is_absolute)
{
  INIT_SUCC(ret);
  ObPathLocationNode* self_node = nullptr;
  if (OB_FAIL(alloc_location_node(self_node))) {
    LOG_WARN("allocate row buffer failed at path_node", K(ret), K(index_), K(expression_));
  } else {
    // skip . and space
    ++index_;
    ObXPathUtil::skip_whitespace(expression_, index_);
    self_node = new (self_node) ObPathLocationNode(ctx_, parser_type_);
    self_node->is_absolute_ = is_absolute;
    if (OB_FAIL(self_node->init(ObLocationType::PN_KEY,
                                ObSeekType::NODES,
                                ObPathNodeAxis::SELF))) {
      LOG_WARN("fail to init self_node", K(ret), K(index_), K(expression_));
    } else {
      self_node->set_wildcard_info(true);
      if (OB_FAIL(root_node_->append(self_node))) {
        LOG_WARN("failed to append location node.", K(ret), K(index_), K(expression_));
      } else if (index_ < len_ && expression_[index_] == ObPathItem::BEGIN_ARRAY) {
        while (OB_SUCC(ret) && index_ < len_ && expression_[index_] == ObPathItem::BEGIN_ARRAY) {
          ObPathNode* filter = nullptr;
          if (OB_FAIL(parse_filter_node(filter, ObPathArgType::IN_FILTER))) {
            LOG_WARN("fail to parse filter", K(ret), K(index_));
          } else if (OB_FAIL(self_node->append(filter))) {
            LOG_WARN("fail to append filter", K(ret), K(index_));
          }
          ObXPathUtil::skip_whitespace(expression_, index_);
        }
        if (OB_SUCC(ret)) {
          self_node->has_filter_ = true;
          ObPathRootNode* root_node = static_cast<ObPathRootNode*>(root_node_);
          root_node->need_trans_++;
        }
      } else if (index_ < len_ && expression_[index_] == ObPathItem::WILDCARD) {
        // ORA-31012: Given XPATH expression not supported
        ret = OB_OP_NOT_ALLOW;
        LOG_WARN("/.* is not allowed", K(ret), K(index_), K(expression_));
      } else if (index_ < len_ && ObPathParserUtil::is_xml_name_start_char(expression_[index_])) {
        if (OB_FAIL(parse_nodetest_info(self_node))) {
          LOG_WARN("wrong node test", K(ret), K(index_), K(expression_));
        } else if (self_node->get_seek_type() != ObSeekType::ELEMENT
          || (self_node->get_seek_type() == ObSeekType::ELEMENT && self_node->is_key_null())) {
          self_node->set_nodetest(ObSeekType::NODES);
          self_node->set_wildcard_info(true);
        }
      } else if (index_ < len_ && expression_[index_] == ObPathItem::WILDCARD) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid path expression", K(ret), K(index_), K(expression_));
      }
    }
  }
  return ret;
}

bool ObPathParser::is_last_letter_location(int last_idx)
{
  bool ret_bool = false;
  if (last_idx >= 0 && !ObXPathUtil::is_whitespace(expression_[last_idx])) {
    ret_bool = true;
  } else {
    while (last_idx >= 0 && ObXPathUtil::is_whitespace(expression_[last_idx])) --last_idx;
    if (last_idx >= 0 && (expression_[last_idx] == ObPathItem::SLASH
        || expression_[last_idx] == ObPathItem::AT
        || expression_[last_idx] == ObPathItem::COLON
        || expression_[last_idx] == ObPathItem::UNDERLINE)) {
      ret_bool = true;
    }
  }
  return ret_bool;
}
bool ObPathParser::is_last_letter_operator(const int& last_idx)
{
  bool ret_bool = false;
  switch (expression_[last_idx]) {
    case 'r': {
      // maybe 'or'
      if (last_idx - 1 > 0 && expression_[last_idx - 1] == 'o') {
        ret_bool = true;
      }
      break;
    }
    case 'd': {
      // maybe 'and' or 'mod'
      if (last_idx - 2 > 0) {
        ObString op(3, expression_.ptr() + last_idx - 2);
        if (op == ObPathItem::AND || op == ObPathItem::MOD) {
          ret_bool = true;
        }
      }
      break;
    }
    case 'v': {
      // maybe 'div'
      if (last_idx - 2 > 0) {
        ObString op(3, expression_.ptr() + last_idx - 2);
        if (op == ObPathItem::DIV) {
          ret_bool = true;
        }
      }
      break;
    }
    default: {
      break;
    }
  }
  return ret_bool;
}

// if is operator before -, like +- 1, then '-' is negtive, else is minus
bool ObPathParser::is_negtive()
{
  bool ret_bool = false;
  if (expression_[index_] == ObPathItem::MINUS || expression_[index_] == ObPathItem::WILDCARD) {
    int last_idx = index_ - 1;
    while (last_idx >= 0 && ObXPathUtil::is_whitespace(expression_[last_idx])) --last_idx;
    if (last_idx < 0 ) {
      ret_bool = true;
    } else if (ObPathParserUtil::is_left_brace(expression_[last_idx])
      || ObPathParserUtil::is_operator(expression_[last_idx])
      || is_last_letter_operator(last_idx)) {
      ret_bool = true;
    }
  }
  return ret_bool;
}

bool ObPathParser::is_number_begin()
{
  bool ret_bool = false;
  if (isdigit(expression_[index_])                                                     // number
    || (expression_[index_] == ObPathItem::DOT && index_ + 1 < len_ && isdigit(expression_[index_ + 1]))) { // decimal
    ret_bool = true;
  }
  return ret_bool;
}

bool ObPathParser::is_literal_begin()
{
  bool ret_bool = false;
  if (expression_[index_] == ObPathItem::DOUBLE_QUOTE
    || expression_[index_] == ObPathItem::SINGLE_QUOTE) {
    ret_bool = true;
  }
  return ret_bool;
}

int ObPathParser::check_cmp(bool &is_cmp)
{
  INIT_SUCC(ret);
  is_cmp = false;
  uint64_t old_idx = index_;
  while (!is_cmp && index_ < len_ && OB_SUCC(ret)) {
    if (expression_[index_] == ObPathItem::BEGIN_ARRAY) {
      if (OB_FAIL(jump_over_brace(false))) {
        LOG_WARN("failed in brace check!", K(ret));
      }
    } else if (expression_[index_] == ObPathItem::BRACE_START) {
      if (OB_FAIL(jump_over_brace(true))) {
        LOG_WARN("failed in brace check!", K(ret));
      }
    } else if (is_literal_begin()) {
      if (OB_FAIL(jump_over_quote())) {
        LOG_WARN("failed in brace check!", K(ret));
      }
    } else if (expression_[index_] == ObPathItem::MINUS) {
      if (index_ - 1 > 0 && ObPathParserUtil::is_xml_name_start_char(expression_[index_ - 1])
        && index_ + 1 < len_ && ObPathParserUtil::is_xml_name_start_char(expression_[index_ + 1])) {
        ++index_;
      } else {
        is_cmp = true;
      }
    } else if (expression_[index_] == ObPathItem::WILDCARD) {
      if (is_last_letter_location(index_)) {
        ++index_;
      } else {
        is_cmp = true;
      }
    } else if (ObPathParserUtil::is_operator(expression_[index_])) {
      is_cmp = true;
    } else if (is_prefix_match_letter_operator()) {
      is_cmp = true;
    } else {
      ++index_;
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    index_ = old_idx;
  }
  return ret;
}

int ObPathParser::jump_over_brace(bool is_brace)
{
  INIT_SUCC(ret);
  char start_brace;
  char end_brace;
  if (is_brace) {
    start_brace = ObPathItem::BRACE_START;
    end_brace = ObPathItem::BRACE_END;
  } else {
    start_brace = ObPathItem::BEGIN_ARRAY;
    end_brace = ObPathItem::END_ARRAY;
  }
  if (expression_[index_] == start_brace) {
    int brace = 1;
    ++index_;
    while (OB_SUCC(ret) && index_ < len_ && brace > 0) {
      if (expression_[index_] == start_brace) {
        ++brace;
        ++index_;
      } else if (expression_[index_] == end_brace) {
        --brace;
        ++index_;
      } else {
        ++index_;
      }
    } // end while
    if (OB_SUCC(ret) && index_ <= len_ && brace == 0) {
    } else {
      ret = OB_ITEM_NOT_MATCH;
      LOG_WARN("there should be a ')'!", K(ret));
    }
  }
  return ret;
}

int ObPathParser::jump_over_quote()
{
  INIT_SUCC(ret);
  char quote;
  if (expression_[index_] == ObPathItem::DOUBLE_QUOTE) {
    quote = ObPathItem::DOUBLE_QUOTE;
  } else if (expression_[index_] == ObPathItem::SINGLE_QUOTE) {
    quote = ObPathItem::SINGLE_QUOTE;
  } else {
    ret = OB_ITEM_NOT_MATCH;
    LOG_WARN("there should be a '\"' or '''!", K(ret), K(index_), K(expression_));
  }
  if (OB_FAIL(ret)) {
  } else {
    ++index_;
    while (OB_SUCC(ret) && index_ < len_ && expression_[index_] != quote) {
      ++index_;
    }
    if (OB_SUCC(ret) && index_ < len_ && expression_[index_] == quote) {
      ++index_;
    } else {
      ret = OB_ITEM_NOT_MATCH;
      LOG_WARN("there should be a '\"'!", K(ret), K(quote));
    }
  }
  return ret;
}

int ObPathParser::parse_arg(ObPathNode*& arg, ObPathArgType patharg_type, bool is_filter, const bool negtive)
{
  INIT_SUCC(ret);
  ObXPathUtil::skip_whitespace(expression_, index_);

  if (OB_FAIL(ret)) {
  } else if (index_ >= len_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid xpath", K(ret), K(expression_));
  } else if (is_filter && (negtive || is_number_begin() || is_literal_begin())) { // is_scalar
    ObPathArgNode* arg_node = nullptr;
    // filter arg: string, number, subpath
    if (OB_FAIL(alloc_arg_node(arg_node))) {
      LOG_WARN("fail to alloc arg node", K(ret), K(index_), K(expression_));
    } else {
      arg_node = new (arg_node) ObPathArgNode(ctx_, parser_type_);
    }
    if (OB_FAIL(ret)) {
    } else if (negtive || is_number_begin()) {
      // parse number
      double num;
      if (OB_FAIL(get_xpath_number(num))) {
        LOG_WARN("fail to get literal", K(ret), K(index_), K(expression_));
      } else {
        num = (negtive) ? -num : num;
        if (OB_FAIL(arg_node->init(num, patharg_type == ObPathArgType::IN_FILTER))) {
          LOG_WARN("fail to init arg node", K(ret), K(num));
        } else {
          arg = arg_node;
        }
      }
    } else {
      // parse literal
      char* literal_ptr = nullptr;
      uint64_t literal_len = 0;
      if (OB_FAIL(get_xpath_literal(literal_ptr, literal_len))) {
        LOG_WARN("fail to get literal", K(ret), K(index_), K(expression_));
      } else {
        // if the filter string has entity ref, need to transform to entity char, like '&lt;' -> '<'
        ObString escape_str = ObString(literal_len, literal_ptr);
        if (OB_FAIL(ObXmlUtil::revert_escape_character(*allocator_, escape_str, escape_str))) {
          LOG_WARN("fail to revert escape str", K(ret), K(escape_str));
        } else if (OB_FAIL(arg_node->init(escape_str.ptr(), escape_str.length(), patharg_type == ObPathArgType::IN_FILTER))) {
          LOG_WARN("fail to init arg node", K(ret), K(literal_ptr), K(literal_len));
        } else {
          arg = arg_node;
        }
      }
    } // is literal
  } else if (OB_FAIL(get_xpath_subpath(arg, is_filter, patharg_type))) {
    LOG_WARN("fail to get literal", K(ret), K(index_), K(expression_));
  } // parse subpath
  return ret;
}

int ObPathParser::get_filter_char_type(ObXpathFilterChar& filter_char)
{
  INIT_SUCC(ret);
  ObXPathUtil::skip_whitespace(expression_, index_);
  bool space_error = false;
  if (index_ >= len_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid xpath", K(ret), K(expression_));
  } else {
    switch (expression_[index_]) {
      case '[':
        filter_char = ObXpathFilterChar::CHAR_BEGIN_FILTER;
        break;
      case '(':
        filter_char = ObXpathFilterChar::CHAR_LEFT_BRACE;
        break;
      case '|':
        filter_char = ObXpathFilterChar::CHAR_UNION;
        break;
      case 'o':
        if (path_prefix_match("or ")) {
          if (index_ > 1 && (expression_[index_ - 1] == ' ' || isdigit(expression_[index_ - 1]))) {
            filter_char = ObXpathFilterChar::CHAR_OR;
            index_ += strlen(ObPathItem::OR);
          } else {
            space_error = true;
          }
        }
        break;
      case 'a':
        if (path_prefix_match("and ")) {
          if (index_ > 1 && (expression_[index_ - 1] == ' ' || isdigit(expression_[index_ - 1]))) {
            filter_char = ObXpathFilterChar::CHAR_AND;
            index_ += strlen(ObPathItem::AND);
          } else {
            space_error = true;
          }
        }
        break;
      case '=':
        filter_char = ObXpathFilterChar::CHAR_EQUAL;
        break;
      case '!':
        if (path_prefix_match(ObPathItem::COM_NE)) {
          filter_char = ObXpathFilterChar::CHAR_UNEQUAL;
          ++index_;
        }
        break;
      case '<' :
        if (path_prefix_match(ObPathItem::COM_LE)) {
          filter_char = ObXpathFilterChar::CHAR_LESS_EQUAL;
          ++index_;
        } else {
          filter_char = ObXpathFilterChar::CHAR_LESS;
        }
        break;
      case '>' :
        if (path_prefix_match(ObPathItem::COM_GE)) {
          filter_char = ObXpathFilterChar::CHAR_GREAT_EQUAL;
          ++index_;
        } else {
          filter_char = ObXpathFilterChar::CHAR_GREAT;
        }
        break;
      case '+':
        filter_char = ObXpathFilterChar::CHAR_ADD;
        break;
      case '-':
        filter_char = ObXpathFilterChar::CHAR_SUB;
        break;
      case '*':
        filter_char = ObXpathFilterChar::CHAR_MULTI;
        break;
      case 'd':
        if (path_prefix_match("div ")) {
          if (index_ > 1 && (expression_[index_ - 1] == ' ' || isdigit(expression_[index_ - 1]))) {
            filter_char = ObXpathFilterChar::CHAR_DIV;
            index_ += strlen(ObPathItem::DIV);
          } else {
            space_error = true;
          }
        }
        break;
      case 'm':
        if (path_prefix_match("mod ")) {
          if (index_ > 1 && (expression_[index_ - 1] == ' ' || isdigit(expression_[index_ - 1]))) {
            filter_char = ObXpathFilterChar::CHAR_MOD;
            index_ += strlen(ObPathItem::MOD);
          } else {
            space_error = true;
          }
        }
        break;
      case ')':
        filter_char = ObXpathFilterChar::CHAR_RIGHT_BRACE;
        break;
      case ']':
        filter_char = ObXpathFilterChar::CHAR_END_FILTER;
        break;
      default:
        filter_char = ObXpathFilterChar::CMP_CHAR_MAX;
        break;
    }
  }
  if (space_error) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should have space before and after letter operator", K(ret));
  } else if (filter_char != ObXpathFilterChar::CMP_CHAR_MAX) {
    ++index_;
  }
  return ret;
}

// 0 means that the priority is the same, but it also means that types are directly comparable,
// i.e. decimal, int, uint, and double are all comparable.
// 1 means this_type has a higher priority
// -1 means this_type has a lower priority

// if top < in ( -1), push into stack
// if top > in ( 1 ), pop the top char, then push
// if top = in ( 0 ), pop the top and do not push, only for '(' and ')', or '[' and ']'
// if error    ( -2), like ']', ')' should not in char stack
enum ObPathPriority {
  ERROR_OP = -2,
  PUSH_STACK= -1,
  MATCH = 0,
  POP_STACK = 1,
};
static constexpr int filter_comparison[CMP_CHAR_MAX][CMP_CHAR_MAX] = {
  /*        in    [   (   |   or  &   =  !=   <=  <  >=   >   +   -   *  div  mod  )  ]   */
  /*  top         0   1   2   3   4   5   6   7   8   9  10  11  12  13  14  15  16  17   */
  /* 0  [   */ { -2, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -2,  0 },
  /* 1  (   */ { -2, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  0, -2 },
  /* 2  |   */ { -2, -1,  1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -2,  1 },
  /* 3  or  */ { -2, -1,  1,  1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  1,  1 },
  /* 4  and */ { -2, -1,  1,  1,  1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  1,  1 },
  /* 5  =   */ { -2, -1,  1,  1,  1,  1,  1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  1,  1 },
  /* 6  !=  */ { -2, -1,  1,  1,  1,  1,  1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  1,  1 },
  /* 7  <=  */ { -2, -1,  1,  1,  1,  1,  1,  1,  1,  1,  1, -1, -1, -1, -1, -1,  1,  1 },
  /* 8  <   */ { -2, -1,  1,  1,  1,  1,  1,  1,  1,  1,  1, -1, -1, -1, -1, -1,  1,  1 },
  /* 9  >=  */ { -2, -1,  1,  1,  1,  1,  1,  1,  1,  1,  1, -1, -1, -1, -1, -1,  1,  1 },
  /* 10 >   */ { -2, -1,  1,  1,  1,  1,  1,  1,  1,  1,  1, -1, -1, -1, -1, -1,  1,  1 },
  /* 11 +   */ { -2, -1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1, -1, -1, -1,  1,  1 },
  /* 12 -   */ { -2, -1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1, -1, -1, -1,  1,  1 },
  /* 13 *   */ { -2, -1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1 },
  /* 14 div */ { -2, -1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1 },
  /* 15 mod */ { -2, -1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1 },
  /* 16 )   */ { -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2 },
  /* 17 ]   */ { -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2 }
};

int ObPathParser::push_filter_char_in(const ObXpathFilterChar& in, ObPathVectorPointers& node_stack,
                                      ObFilterCharPointers& char_stack, ObPathArgType patharg_type)
{
  INIT_SUCC(ret);
  uint64_t size_c = char_stack.size();
  if (size_c <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty char stack", K(ret), K(index_), K(expression_));
  } else {
    ObXpathFilterChar top = char_stack[size_c - 1];
    int priority = filter_comparison[top][in];
    switch (priority) {
      case ObPathPriority::PUSH_STACK: {
        if (OB_FAIL(char_stack.push_back(in))) {
          LOG_WARN("fail to push char", K(ret), K(in));
        }
        break;
      }
      case ObPathPriority::MATCH: {
        if (OB_FAIL(ObPathUtil::pop_char_stack(char_stack))) {
          LOG_WARN("fail to pop char", K(ret), K(in));
        }
        break;
      }
      case ObPathPriority::POP_STACK: {
        while (OB_SUCC(ret) && size_c > 1 && filter_comparison[top][in] == ObPathPriority::POP_STACK) {
          ObPathFilterNode* filter_node = nullptr;
          if (OB_FAIL(alloc_filter_node(filter_node))) {
            LOG_WARN("fail to alloc filter node", K(ret), K(index_), K(expression_));
          } else {
            filter_node = new (filter_node) ObPathFilterNode(ctx_, parser_type_);
            ObPathNode *left = nullptr;
            ObPathNode *right = nullptr;
            if (OB_FAIL(ObPathUtil::pop_node_stack(node_stack, right))) {
              LOG_WARN("fail to get arg", K(ret), K(index_), K(expression_));
            } else if (OB_FAIL(ObPathUtil::pop_node_stack(node_stack, left))) {
              LOG_WARN("fail to get arg", K(ret), K(index_), K(expression_));
            } else if (OB_FAIL(filter_node->init(top, left, right, patharg_type == ObPathArgType::IN_FILTER))) {
              LOG_WARN("fail to init", K(ret), K(index_), K(expression_));
            } else if (OB_FAIL(node_stack.push_back(filter_node))) {
              LOG_WARN("fail to push filter node", K(ret), K(index_), K(expression_));
            } else if (OB_FAIL(ObPathUtil::pop_char_stack(char_stack))) { // pop char top
              LOG_WARN("fail to pop char", K(ret), K(in));
            } else {
              size_c = char_stack.size();
            }
          }
          if (OB_FAIL(ret)) {
          } else if (size_c > 0) {
            top = char_stack[size_c - 1];
          }
        }

        if (OB_FAIL(ret)) {
        } else if (filter_comparison[top][in] == ObPathPriority::PUSH_STACK) {
          if (OB_FAIL(char_stack.push_back(in))) {
            LOG_WARN("fail to push char", K(ret), K(in));
          }
        } else if (OB_FAIL(ObPathUtil::pop_char_stack(char_stack))) {
          LOG_WARN("fail to pop char", K(ret), K(in));
        }
        break;
      }
      case ObPathPriority::ERROR_OP: {
        ret = OB_OP_NOT_ALLOW;
        LOG_WARN("wrong operator", K(ret), K(priority), K(top), K(in));
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("wrong priority", K(ret), K(priority), K(top), K(in));
        break;
      }
    } // end switch
  }
  return ret;
}

int ObPathParser::parse_filter_node(ObPathNode*& filter, ObPathArgType patharg_type)
{
  INIT_SUCC(ret);
  ObXPathUtil::skip_whitespace(expression_, index_);
  ObPathVectorPointers node_stack;
  ObFilterCharPointers char_stack;
  if (index_ < expression_.length()) {
    if (patharg_type != ObPathArgType::IN_FILTER
      || (patharg_type == ObPathArgType::IN_FILTER
      && expression_[index_] == ObPathItem::BEGIN_ARRAY && ++index_)) {
      if (OB_FAIL(char_stack.push_back(ObXpathFilterChar::CHAR_BEGIN_FILTER))) {
        LOG_WARN("fail to push_back charactor", K(ret), K(index_), K(expression_));
      } else {
        ObXPathUtil::skip_whitespace(expression_, index_);
        while (OB_SUCC(ret) && index_ < len_ && char_stack.size() > 0) {
          ObXpathFilterChar filter_char = ObXpathFilterChar::CMP_CHAR_MAX;
          bool minus = false;
          bool multi = false;
          // if is ObXpathFilterChar, push in, else get arg
          if ((expression_[index_] == '-' || expression_[index_] == '*') && is_negtive()) {
            minus = (expression_[index_] == '-');
            multi = (expression_[index_] == '*');
          }
          if (!minus && !multi && OB_FAIL(get_filter_char_type(filter_char))) {
            LOG_WARN("fail to get char type", K(ret), K(index_), K(expression_));
          } else if (filter_char < ObXpathFilterChar::CMP_CHAR_MAX) {
            if (OB_FAIL(push_filter_char_in(filter_char, node_stack, char_stack, patharg_type))) {
              LOG_WARN("fail to push char", K(ret), K(filter_char));
            }
          } else { // must be arg
            ObPathNode* arg = nullptr;
            if (minus) {
              int count_minus = 0;
              while (index_ < len_ && expression_[index_] == '-' && OB_SUCC(ret)) {
                ++count_minus;
                ++index_;
                ObXPathUtil::skip_whitespace(expression_, index_);
              }
              minus = (count_minus % 2 == 0) ? false : true;
            }
            if (OB_FAIL(parse_arg(arg, patharg_type, true, minus))) {
              LOG_WARN("fail to parse filter arg", K(ret), K(index_), K(expression_));
            } else if (OB_FAIL(node_stack.push_back(arg))) {
              LOG_WARN("fail to push arg", K(ret), K(index_), K(expression_));
            }
          }
          ObXPathUtil::skip_whitespace(expression_, index_);
        } // end while
        // if not in predicate
        if (OB_SUCC(ret) && (index_ >= len_ && patharg_type != ObPathArgType::IN_FILTER)) {
          // push ']' in
          if (OB_FAIL(push_filter_char_in(ObXpathFilterChar::CHAR_END_FILTER, node_stack, char_stack, patharg_type))) {
            LOG_WARN("fail to push char", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (node_stack.size() == 1 && char_stack.size() == 0) {
          if (OB_ISNULL(node_stack[0])) {
            ret = OB_BAD_NULL_ERROR;
            LOG_WARN("should not be null", K(ret));
          } else {
            filter = node_stack[0];
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("wrong expression", K(ret), K(expression_), K(index_));
        }
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("wrong filter!", K(ret), K(index_), K(expression_));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("wrong path expression!", K(ret), K(index_), K(expression_));
  }
  return ret;
}

} // namespace common
} // namespace oceanbase