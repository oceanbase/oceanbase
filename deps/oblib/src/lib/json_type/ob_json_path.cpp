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
 * This file contains implementation support for the JSON path abstraction.
 */

#define USING_LOG_PREFIX SQL_RESV
#include "ob_json_path.h"
#include "ob_json_tree.h"
#include "ob_json_bin.h"
#include "ob_json_parse.h"
#include "lib/string/ob_sql_string.h"
#include "lib/ob_errno.h"
#include "lib/charset/ob_dtoa.h"
#include <rapidjson/encodings.h>
#include <rapidjson/memorystream.h>

namespace oceanbase {
namespace common {

// Path Symbol
class ObJsonPathItem
{
public:
  static constexpr char ROOT = '$';
  static constexpr char BEGIN_MEMBER = '.';
  static constexpr char BEGIN_ARRAY = '[';
  static constexpr char END_ARRAY = ']';
  static constexpr char DOUBLE_QUOTE = '"';
  static constexpr char WILDCARD = '*';
  static constexpr char MINUS = '-';
  static constexpr char FILTER_FLAG = '?';
  static constexpr char SUB_PATH = '@';
  static constexpr char BRACE_START = '(';
  static constexpr char BRACE_END = ')';
  static constexpr char* ABS_METH = const_cast<char*>("abs");
  static constexpr char* DATE_METH = const_cast<char*>("date");
  static constexpr char* SIZE_METH = const_cast<char*>("size");
  static constexpr char* TYPE_METH = const_cast<char*>("type");
  static constexpr char* BOOLEAN_METH = const_cast<char*>("boolean");
  static constexpr char* BOOLONLY_METH = const_cast<char*>("booleanOnly");
  static constexpr char* UPPER_METH = const_cast<char*>("upper");
  static constexpr char* CELLING_METH = const_cast<char*>("ceiling");
  static constexpr char* DOUBLE_METH = const_cast<char*>("double");
  static constexpr char* FLOOR_METH = const_cast<char*>("floor");
  static constexpr char* LENGTH_METH = const_cast<char*>("length");
  static constexpr char* LOWER_METH = const_cast<char*>("lower");
  static constexpr char* NUMBER_METH = const_cast<char*>("number");
  static constexpr char* NUMONLY_METH = const_cast<char*>("numberOnly");
  static constexpr char* STRING_METH = const_cast<char*>("string");
  static constexpr char* STRONLY_METH = const_cast<char*>("stringOnly");
  static constexpr char* TIMESTAMP_METH = const_cast<char*>("timestamp");
  static constexpr char* COMP_LIKE = const_cast<char*>(" like ");
  static constexpr char* COMP_NOT_EXISTS = const_cast<char*>("!exists(");
  static constexpr char* COMP_EXISTS = const_cast<char*>("exists(");
  static constexpr char* COMP_EQ_REGEX = const_cast<char*>(" eq_regex ");
  static constexpr char* COMP_LIKE_REGEX = const_cast<char*>(" like_regex ");
  static constexpr char* COMP_STARTS_WITH = const_cast<char*>(" starts with ");
  static constexpr char* COMP_SUBSTRING = const_cast<char*>(" has substring ");
  static constexpr char* COMP_EQUAL = const_cast<char*>(" == ");
  static constexpr char* COMP_UNEQUAL = const_cast<char*>(" != ");
  static constexpr char* COMP_LARGGER = const_cast<char*>(" > ");
  static constexpr char* COMP_LARGGER_EQUAL = const_cast<char*>(" >= ");
  static constexpr char* COMP_SMALLER = const_cast<char*>(" < ");
  static constexpr char* COMP_SMALLER_EQUAL = const_cast<char*>(" <= ");
  static constexpr char* COND_AND = const_cast<char*>(" && ");
  static constexpr char* COND_OR = const_cast<char*>(" || ");
  static constexpr char* COND_NOT = const_cast<char*>("!(");
};


ObJsonArrayIndex::ObJsonArrayIndex(uint64_t index, bool is_from_end, uint64_t array_length)
{
  if (is_from_end) {
    if (index < array_length) {
      array_index_ = array_length - index - 1;
    } else {
      array_index_ = 0;
    }
  } else {
    array_index_ = min(index, array_length);
  }

  // check over range
  is_within_bounds_ = index < array_length ? true : false;
}

ObJsonPathNodeType ObJsonPathNode::get_node_type() const
{
  return node_type_;
}

ObJsonPathNode::~ObJsonPathNode()
{}

int ObJsonPathBasicNode::get_first_array_index(uint64_t array_length, ObJsonArrayIndex &array_index) const 
{
  INIT_SUCC(ret);
  switch (node_type_) {
    case JPN_ARRAY_RANGE: {
      array_index = ObJsonArrayIndex(node_content_.array_range_.first_index_,
          node_content_.array_range_.is_first_index_from_end_, array_length);
      break;
    }
    case JPN_ARRAY_CELL: {
      array_index = ObJsonArrayIndex(node_content_.array_cell_.index_,
          node_content_.array_cell_.is_index_from_end_, array_length);
      break;
    }
    default: {
      ret = OB_ERR_INTERVAL_INVALID;
      LOG_WARN("wrong node type.", K(ret), K(node_type_));
      break;
    }
  }
  return ret;
}

int ObJsonPathBasicNode::get_last_array_index(uint64_t array_length, ObJsonArrayIndex &array_index) const 
{
  INIT_SUCC(ret);
  if (node_type_ == JPN_ARRAY_RANGE) {
    array_index = ObJsonArrayIndex(node_content_.array_range_.last_index_,
        node_content_.array_range_.is_last_index_from_end_, array_length);
  } else {
    ret = OB_ERR_INTERVAL_INVALID;
    LOG_WARN("fail to get_first_array_index, node_type should be array_cell or array_range", K(ret), K(node_type_));
  }
  return ret;
}

// return array range
// it can be [*] or [begin to end]
// finally range should be [begin, end)
int ObJsonPathBasicNode::get_array_range(uint64_t array_length, ObArrayRange &array_range) const
{
  INIT_SUCC(ret);
  if (node_type_ == JPN_ARRAY_CELL_WILDCARD) {
    array_range.array_begin_ = 0;
    array_range.array_end_ = array_length;
  } else if (node_type_ == JPN_ARRAY_RANGE) {
    ObJsonArrayIndex first;
    ObJsonArrayIndex last;
    if (OB_FAIL(get_first_array_index(array_length, first))) {
      LOG_WARN("get first array index failed.", K(ret), K(array_length));
    } else if (OB_FAIL(get_last_array_index(array_length, last))) {
      LOG_WARN("get last array index failed.", K(ret), K(array_length));
    } else {
      array_range.array_begin_ = first.get_array_index();
      // [begin, end)
      array_range.array_end_ = last.is_within_bounds() ? last.get_array_index() + 1 : last.get_array_index();
    }
  } else {
    ret = OB_ERR_INTERVAL_INVALID;
    LOG_WARN("wrong node type.", K(ret), K(node_type_));
  }

  return ret;
}

int ObJsonPathBasicNode::get_multi_array_size() const
{
  return node_content_.multi_array_.size();
}

int ObJsonPathBasicNode::get_multi_array_range(uint32_t idx, uint64_t array_length, ObArrayRange &array_range) const
{
  INIT_SUCC(ret);
  if (node_type_ == JPN_ARRAY_CELL_WILDCARD) {
    array_range.array_begin_ = 0;
    array_range.array_end_ = array_length;
  } else if (node_type_ == JPN_MULTIPLE_ARRAY) {
    ObJsonArrayIndex first(node_content_.multi_array_[idx]->first_index_,
        node_content_.multi_array_[idx]->is_first_index_from_end_, array_length);
    ObJsonArrayIndex last(node_content_.multi_array_[idx]->last_index_,
        node_content_.multi_array_[idx]->is_last_index_from_end_, array_length);
    array_range.array_begin_ = first.get_array_index();
    // [begin, end)
    array_range.array_end_ = last.is_within_bounds() ? last.get_array_index() + 1 : last.get_array_index();
  } else {
    ret = OB_ERR_INTERVAL_INVALID;
    LOG_WARN("wrong node type.", K(ret), K(node_type_));
  }

  return ret;
}

bool ObJsonPathBasicNode::is_autowrap() const 
{
  bool is_autowrap = false;
  switch (node_type_) {
    // set array_len to 1, when array_cell is '0' or 'last', set within_bounds true
    case JPN_ARRAY_CELL: {
      ObJsonArrayIndex array_index;
      // here can make sure type is JPN_ARRAY_CELL, get_first_array_index will not return ERROR
      // ignore ret
      get_first_array_index(1, array_index);
      is_autowrap = array_index.is_within_bounds();
      break;
    }

    case JPN_ARRAY_RANGE: {
      ObArrayRange range;
      // here can make sure type is JPN_ARRAY_RANGE, get_array_range will not return ERROR
      // ignore ret
      get_array_range(1, range);
      is_autowrap =  (range.array_begin_ < range.array_end_);
      break;
    }

    default:{
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "invalid node type", K(node_type_));
      break;
    }
  }

  return is_autowrap;
}

bool ObJsonPathBasicNode::is_multi_array_autowrap() const
{
  bool is_autowrap = false;
  switch (node_type_) {
    // set array_len to 1, when array_cell is '0' or 'last', set within_bounds true
    case JPN_ARRAY_CELL_WILDCARD: {
      is_autowrap  = true;
      break;
    }

    case JPN_MULTIPLE_ARRAY: {
      uint64_t array_size = get_multi_array_size();
      for (uint64_t array_idx = 0; array_idx < array_size && !is_autowrap; ++array_idx) {
        // when array_cell is '0' or 'last', set within_bounds true
        if (node_content_.multi_array_[array_idx]->first_index_ == 0
            || node_content_.multi_array_[array_idx]->last_index_ == 0) {
          is_autowrap = true;
        }
      }
      break;
    }

    default:{
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "invalid node type", K(node_type_));
      break;
    }
  }

  return is_autowrap;
}

// Function node
inline bool ObJsonPathFuncNode::is_autowrap() const
{
  return false;
}

// Filter node
inline bool ObJsonPathFilterNode::is_autowrap() const
{
  return false;
}

// init function for **, [*], .*
int ObJsonPathBasicNode::init(ObJsonPathNodeType cur_node_type, bool is_mysql)
{
  INIT_SUCC(ret);
  if (is_mysql) {
    switch (cur_node_type) {
      case JPN_MEMBER_WILDCARD:
      case JPN_ARRAY_CELL_WILDCARD:
      case JPN_WILDCARD_ELLIPSIS: {
        node_type_ = cur_node_type;
        node_content_.is_had_wildcard_ = true;
        break;
      }
      default: {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument, should be '[*]', '.*' or '**'!", K(ret), K(cur_node_type));
        break;
      }
    }
  } else {
    switch (cur_node_type) {
      case JPN_MEMBER_WILDCARD:
      case JPN_ARRAY_CELL_WILDCARD:
      case JPN_DOT_ELLIPSIS: {
        node_type_ = cur_node_type;
        node_content_.is_had_wildcard_ = true;
        break;
      }
      default: {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument, should be '[*]', '.*' or '..'!", K(ret), K(cur_node_type));
        break;
      }
    }
  }
  return ret;
}

ObJsonPathBasicNode::ObJsonPathBasicNode(ObIAllocator *allocator, ObString &keyname)
: ObJsonPathNode(allocator)
{
  node_content_.member_.object_name_ = keyname.ptr();
  node_content_.member_.len_ = keyname.length();
  node_type_ = JPN_MEMBER;
}

ObJsonPathBasicNode::ObJsonPathBasicNode(ObIAllocator *allocator, const char* name, uint64_t len)
: ObJsonPathNode(allocator)
{
  node_content_.member_.object_name_ = name;
  node_content_.member_.len_ = len;
  node_type_ = JPN_MEMBER;
}

ObJsonPathBasicNode::ObJsonPathBasicNode(ObIAllocator *allocator, uint64_t idx)
: ObJsonPathNode(allocator)
{
  node_content_.array_cell_.index_ = idx;
  node_content_.array_cell_.is_index_from_end_ = false;
  node_type_ = JPN_ARRAY_CELL;
}

ObJsonPathBasicNode::ObJsonPathBasicNode(ObIAllocator *allocator, uint64_t idx, bool is_from_end)
: ObJsonPathNode(allocator)
{
  node_content_.array_cell_.index_ = idx;
  node_content_.array_cell_.is_index_from_end_ = is_from_end;
  node_type_ = JPN_ARRAY_CELL;
}

ObJsonPathBasicNode::ObJsonPathBasicNode(ObIAllocator *allocator, uint64_t first_idx, uint64_t last_idx)
: ObJsonPathNode(allocator)
{
  node_content_.array_range_.first_index_ = first_idx;
  node_content_.array_range_.is_first_index_from_end_ = false;
  node_content_.array_range_.last_index_ = last_idx;
  node_content_.array_range_.is_last_index_from_end_ = false;
  node_type_ = JPN_ARRAY_RANGE;
}

ObJsonPathBasicNode::ObJsonPathBasicNode(ObIAllocator *allocator, uint64_t first_idx,
                                        bool is_first_from_end,
                                         uint64_t last_idx, bool is_last_from_end)
: ObJsonPathNode(allocator)
{
  node_content_.array_range_.first_index_ = first_idx;
  node_content_.array_range_.is_first_index_from_end_ = is_first_from_end;
  node_content_.array_range_.last_index_ = last_idx;
  node_content_.array_range_.is_last_index_from_end_ = is_last_from_end;
  node_type_ = JPN_ARRAY_RANGE;
}

ObJsonPathBasicNode::ObJsonPathBasicNode(ObIAllocator *allocator, ObPathArrayRange* o_array)
: ObJsonPathNode(allocator)
{
  node_content_.multi_array_.push_back(o_array);
  node_type_ = JPN_MULTIPLE_ARRAY;
}

int ObJsonPathFuncNode::init(const char* name, uint64_t len)
{
  INIT_SUCC(ret);
  if (len == strlen(ObJsonPathItem::ABS_METH) && (0 == strncmp(name, ObJsonPathItem::ABS_METH, len))) {
    node_type_ = JPN_ABS;
  } else if (len == strlen(ObJsonPathItem::BOOLEAN_METH) && 0 == strncmp(name, ObJsonPathItem::BOOLEAN_METH, len)) {
    node_type_ = JPN_BOOLEAN;
  } else if (len == strlen(ObJsonPathItem::BOOLONLY_METH) && 0 == strncmp(name, ObJsonPathItem::BOOLONLY_METH, len)) {
    node_type_ = JPN_BOOL_ONLY;
  } else if (len == strlen(ObJsonPathItem::CELLING_METH) && 0 == strncmp(name, ObJsonPathItem::CELLING_METH, len)) {
    node_type_ = JPN_CEILING;
  } else if (len == strlen(ObJsonPathItem::DATE_METH) && 0 == strncmp(name, ObJsonPathItem::DATE_METH, len)) {
    node_type_ = JPN_DATE;
  } else if (len == strlen(ObJsonPathItem::DOUBLE_METH) && 0 == strncmp(name, ObJsonPathItem::DOUBLE_METH, len)) {
    node_type_ = JPN_DOUBLE;
  } else if (len == strlen(ObJsonPathItem::FLOOR_METH) && 0 == strncmp(name, ObJsonPathItem::FLOOR_METH, len)) {
    node_type_ = JPN_FLOOR;
  } else if (len == strlen(ObJsonPathItem::LENGTH_METH) && 0 == strncmp(name, ObJsonPathItem::LENGTH_METH, len)) {
    node_type_ = JPN_LENGTH;
  } else if (len == strlen(ObJsonPathItem::LOWER_METH) && 0 == strncmp(name, ObJsonPathItem::LOWER_METH, len)) {
    node_type_ = JPN_LOWER;
  } else if (len == strlen(ObJsonPathItem::NUMBER_METH) && 0 == strncmp(name, ObJsonPathItem::NUMBER_METH, len)) {
    node_type_ = JPN_NUMBER;
  } else if (len == strlen(ObJsonPathItem::NUMONLY_METH) && 0 == strncmp(name, ObJsonPathItem::NUMONLY_METH, len)) {
    node_type_ = JPN_NUM_ONLY;
  } else if (len == strlen(ObJsonPathItem::SIZE_METH) && 0 == strncmp(name, ObJsonPathItem::SIZE_METH, len)) {
    node_type_ = JPN_SIZE;
  } else if (len == strlen(ObJsonPathItem::STRING_METH) && 0 == strncmp(name, ObJsonPathItem::STRING_METH, len)) {
    node_type_ = JPN_STRING;
  } else if (len == strlen(ObJsonPathItem::STRONLY_METH) && 0 == strncmp(name, ObJsonPathItem::STRONLY_METH, len)) {
    node_type_ = JPN_STR_ONLY;
  } else if (len == strlen(ObJsonPathItem::TIMESTAMP_METH) && 0 == strncmp(name, ObJsonPathItem::TIMESTAMP_METH, len)) {
    node_type_ = JPN_TIMESTAMP;
  } else if (len == strlen(ObJsonPathItem::TYPE_METH) && 0 == strncmp(name, ObJsonPathItem::TYPE_METH, len)) {
    node_type_ = JPN_TYPE;
  } else if (len == strlen(ObJsonPathItem::UPPER_METH) && 0 == strncmp(name, ObJsonPathItem::UPPER_METH, len)) {
    node_type_ = JPN_UPPER;
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN( "not function name", K(ret), K(name));
  }

  return ret;
}

int ObJsonPathFilterNode::init_left_comp_path(ObJsonPath* spath)
{
  INIT_SUCC(ret);
  node_content_.comp_.comp_left_.filter_path_ = spath;
  node_content_.comp_.left_type_ = JPN_SUB_PATH;
  return ret;
}

int ObJsonPathFilterNode::init_right_comp_path(ObJsonPath* spath)
{
  INIT_SUCC(ret);
  switch (node_type_) {
    case JPN_EQUAL:
    case JPN_UNEQUAL:
    case JPN_LARGGER:
    case JPN_LARGGER_EQUAL:
    case JPN_SMALLER:
    case JPN_SMALLER_EQUAL:
    case JPN_SUBSTRING:
    case JPN_STARTS_WITH:
    case JPN_LIKE:
    case JPN_LIKE_REGEX:
    case JPN_EQ_REGEX:
    case JPN_NOT_EXISTS:
    case JPN_EXISTS: {
      node_content_.comp_.comp_right_.filter_path_ = spath;
      node_content_.comp_.right_type_ = JPN_SUB_PATH;
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN( "not compare type", K(ret), K(node_type_));
      break;
    }
  }

  return ret;
}

inline void ObJsonPathFilterNode::init_left_scalar(char* str, uint64_t len)
{
  node_content_.comp_.comp_left_.path_scalar_.scalar_ = str;
  node_content_.comp_.comp_left_.path_scalar_.s_length_ = len;
  node_content_.comp_.left_type_ = JPN_SCALAR;
}

int ObJsonPathFilterNode::init_right_scalar(char* str, uint64_t len)
{
  INIT_SUCC(ret);
  switch (node_type_) {
    case JPN_EQUAL:
    case JPN_UNEQUAL:
    case JPN_LARGGER:
    case JPN_LARGGER_EQUAL:
    case JPN_SMALLER:
    case JPN_SMALLER_EQUAL:
    case JPN_SUBSTRING:
    case JPN_STARTS_WITH:
    case JPN_LIKE:
    case JPN_LIKE_REGEX:
    case JPN_EQ_REGEX: {
      node_content_.comp_.comp_right_.path_scalar_.scalar_ = str;
      node_content_.comp_.comp_right_.path_scalar_.s_length_ = len;
      node_content_.comp_.right_type_ = JPN_SCALAR;
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN( "not compare type", K(ret), K(node_type_));
      break;
    }
  }

  return ret;
}

inline void ObJsonPathFilterNode::init_left_var(char* str, uint64_t len)
{
  node_content_.comp_.comp_left_.path_var_.var_= str;
  node_content_.comp_.comp_left_.path_var_.v_length_ = len;
  node_content_.comp_.left_type_ = JPN_SQL_VAR;
}

int ObJsonPathFilterNode::init_right_var(char* str, uint64_t len)
{
  INIT_SUCC(ret);
  switch (node_type_) {
    case JPN_EQUAL:
    case JPN_UNEQUAL:
    case JPN_LARGGER:
    case JPN_LARGGER_EQUAL:
    case JPN_SMALLER:
    case JPN_SMALLER_EQUAL:
    case JPN_SUBSTRING:
    case JPN_STARTS_WITH:
    case JPN_LIKE:
    case JPN_LIKE_REGEX:
    case JPN_EQ_REGEX: {
      node_content_.comp_.comp_right_.path_var_.var_= str;
      node_content_.comp_.comp_right_.path_var_.v_length_ = len;
      node_content_.comp_.right_type_ = JPN_SQL_VAR;
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN( "not compare type", K(ret), K(node_type_));
      break;
    }
  }

  return ret;
}

inline void ObJsonPathFilterNode::init_bool_or_null(ObJsonPathNodeType comp_type, bool left)
{
  if (left) {
    node_content_.comp_.left_type_ = comp_type;
  } else {
    node_content_.comp_.right_type_ = comp_type;
  }
}

int ObJsonPathFilterNode::init_comp_type(ObJsonPathNodeType comp_type)
{
  INIT_SUCC(ret);
  switch (comp_type) {
    case JPN_EQUAL:
    case JPN_UNEQUAL:
    case JPN_LARGGER:
    case JPN_LARGGER_EQUAL:
    case JPN_SMALLER:
    case JPN_SMALLER_EQUAL:
    case JPN_SUBSTRING:
    case JPN_STARTS_WITH:
    case JPN_LIKE:
    case JPN_LIKE_REGEX:
    case JPN_EQ_REGEX:
    case JPN_NOT_EXISTS:
    case JPN_EXISTS: {
      node_type_ = comp_type;
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN( "not compare type", K(ret), K(node_type_));
      break;
    }
  }
  return ret;
}

inline void ObJsonPathFilterNode::init_cond_left(ObJsonPathFilterNode* node)
{
  node_content_.cond_.cond_left_ = node;
}

int ObJsonPathFilterNode:: init_cond_right(ObJsonPathFilterNode* node)
{
  INIT_SUCC(ret);
  switch (node_type_) {
    case ObJsonPathNodeType::JPN_NOT_COND:
    case ObJsonPathNodeType::JPN_AND_COND:
    case ObJsonPathNodeType::JPN_OR_COND: {
      node_content_.cond_.cond_right_ = node;
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN( "not condition type", K(ret), K(node_type_));
      break;
    }
  }
  return ret;
}

int ObJsonPathFilterNode::init_cond_type(ObJsonPathNodeType cond_type)
{
  INIT_SUCC(ret);
  switch (cond_type) {
    case ObJsonPathNodeType::JPN_NOT_COND:
    case ObJsonPathNodeType::JPN_AND_COND:
    case ObJsonPathNodeType::JPN_OR_COND: {
      node_type_ = cond_type;
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN( "not condition type", K(ret), K(node_type_));
      break;
    }
  }
  return ret;
}
ObPathMember ObJsonPathBasicNode::get_object() const
{
  return node_content_.member_;
}

int ObJsonPathBasicNode::add_multi_array(ObPathArrayRange* o_array)
{
  INIT_SUCC(ret);
  if (OB_FAIL(node_content_.multi_array_.push_back(o_array))) {
    LOG_WARN("add_multi_array failed.", K(ret));
  }
  return ret;
}

const int64_t DEFAULT_PAGE_SIZE = 8192L; // 8kb
ObJsonPath::ObJsonPath(common::ObIAllocator *allocator)
    : page_allocator_(*allocator, common::ObModIds::OB_MODULE_PAGE_ALLOCATOR),
      mode_arena_(DEFAULT_PAGE_SIZE, page_allocator_),
      path_nodes_(&mode_arena_, common::ObModIds::OB_MODULE_PAGE_ALLOCATOR),
      heap_expr_(allocator)
{
  allocator_ = allocator;
  index_ = 0;
  bad_index_ = -1;
  use_heap_expr_ = 0;
  is_mysql_ =  lib::is_mysql_mode();
  is_sub_path_ = false;
  is_contained_wildcard_or_ellipsis_ = false;
}

common::ObIAllocator* ObJsonPath::get_allocator()
{
  return allocator_;
}

ObJsonPath::ObJsonPath(const ObString& path, common::ObIAllocator *allocator)
    : page_allocator_(*allocator, common::ObModIds::OB_MODULE_PAGE_ALLOCATOR),
      mode_arena_(DEFAULT_PAGE_SIZE, page_allocator_),
      path_nodes_(&mode_arena_, common::ObModIds::OB_MODULE_PAGE_ALLOCATOR),
      heap_expr_(allocator)
{
  allocator_ = allocator;
  index_ = 0;
  bad_index_ = -1;
  use_heap_expr_ = 1;
  is_contained_wildcard_or_ellipsis_ = false;
  is_mysql_ = lib::is_mysql_mode();
  is_sub_path_ = false;

  if (heap_expr_.append(path) == OB_SUCCESS) {
    expression_.assign_ptr(heap_expr_.ptr(), heap_expr_.length());
  } else {
    expression_.assign_ptr(path.ptr(), path.length());
    use_heap_expr_ = 0;
  }
}

ObJsonPath::~ObJsonPath()
{}

inline void ObJsonPath::set_subpath_arg(bool is_lax)
{
  is_sub_path_ = true;
  is_mysql_ = false;
  is_lax_ = is_lax;
}

bool ObJsonPath::is_last_func()
{
  bool ret_bool = false;
  ObJsonPathNodeType node_type = get_last_node_type();
  if (node_type > JPN_BEGIN_FUNC_FLAG && node_type < JPN_END_FUNC_FLAG) {
    ret_bool = true;
  }
  return ret_bool;
}

ObJsonPathNodeType ObJsonPath::get_last_node_type()
{
  ObJsonPathNodeType ret_type = JPN_ERROR;
  if (path_node_cnt() > 0) {
    ret_type = path_nodes_[path_node_cnt() - 1]->get_node_type();
  } else {
    ret_type = JPN_ROOT;
  }
  return ret_type;
}

bool ObJsonPath::path_not_str()
{
  bool ret_bool = false;
  ObJsonPathNodeType ptype = get_last_node_type();
  if (ptype > JPN_BEGIN_FUNC_FLAG && ptype < JPN_END_FUNC_FLAG) {
    switch (ptype) {
      case JPN_LOWER:
      case JPN_UPPER:
      case JPN_STRING:
      case JPN_STR_ONLY:
      case JPN_TYPE:{
        ret_bool = false;
        break;
      }
      default: {
        ret_bool = true;
        break;
      }
    }
  }
  return ret_bool;
}

int ObJsonPath::get_path_item_method_str(common::ObIAllocator &allocator,
                                                    ObString &str,
                                                    char*& res,
                                                    uint64_t &len,
                                                    bool &has_fun)
{
  INIT_SUCC(ret);
  uint64_t s_len = str.length();
  uint64_t dot_pos = -1, t_p = 0;
  while (t_p < s_len) {
    if (str[t_p] == '.') {
      t_p ++;
      ObJsonPathUtil::skip_whitespace(str, t_p);
      dot_pos = t_p;
      while (t_p < s_len && ObJsonPathUtil::letter_or_not(str[t_p])) {
        t_p ++;
      }
      len = t_p - dot_pos;
    } else if (str[t_p] == '(') {
      t_p ++;
      ObJsonPathUtil::skip_whitespace(str, t_p);
      if (t_p < s_len && str[t_p] == ')') {
        if (t_p < (s_len - 1)) {
          t_p ++;
        }
        ObJsonPathUtil::skip_whitespace(str, t_p);
        if (t_p == (s_len - 1)) {
          has_fun = true;
        }
      }
    } else {
      t_p ++;
    }
  }
  if (len > 0 && has_fun) {
    char* start_ptr = str.ptr() + dot_pos;
    res = static_cast<char*> (allocator.alloc(len));
    if (OB_ISNULL(res)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory for member_name.",K(ret), K(len),K(start_ptr));
    } else {
      MEMCPY(res, start_ptr, len);
    }
  }
  return ret;
}

int ObJsonPath::change_json_expr_res_type_if_need(common::ObIAllocator &allocator, ObString &str, ParseNode &ret_node, int8_t json_expr_flag)
{
  INIT_SUCC(ret);
  bool has_fun = false;
  char* res_str = nullptr;
  uint64_t name_len = 0;
  if (OB_FAIL(get_path_item_method_str(allocator, str, res_str, name_len, has_fun))) {
    LOG_WARN("get item method fail", K(ret));
  } else if (has_fun) {
    ObJsonPathFuncNode* func_node = static_cast<ObJsonPathFuncNode*> (allocator.alloc(sizeof(ObJsonPathFuncNode)));
    if (OB_ISNULL(func_node)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate row buffer failed at member_node", K(ret));
    } else {
      func_node = new (func_node) ObJsonPathFuncNode(&allocator);
      if ((OB_FAIL(func_node->init(res_str, name_len)))) {
        // should not set error only pass set return type
        ret = OB_SUCCESS;
        allocator.free(func_node);
        LOG_WARN("fail to append JsonPathNode(member_node)!", K(ret));
      } else {
        switch (func_node->get_node_type()) {
          case JPN_BOOLEAN :
          case JPN_BOOL_ONLY : {
            if (json_expr_flag == OPT_JSON_QUERY && ret_node.int16_values_[OB_NODE_CAST_TYPE_IDX] == T_JSON) { // do nothing
            } else {
              ret_node.type_ = T_CAST_ARGUMENT;
              ret_node.value_ = 0;
              ret_node.int16_values_[OB_NODE_CAST_TYPE_IDX] = T_VARCHAR;
              ret_node.int16_values_[OB_NODE_CAST_COLL_IDX] = 0;
              ret_node.int32_values_[OB_NODE_CAST_C_LEN_IDX] = 20;
              ret_node.length_semantics_ = 0;
              ret_node.is_hidden_const_ = 1;
            }
            break;
          }
          case JPN_DATE : {
            if (json_expr_flag == OPT_JSON_QUERY) { // do nothing
            } else {
              ret_node.type_ = T_CAST_ARGUMENT;
              ret_node.value_ = 0;
              ret_node.int16_values_[OB_NODE_CAST_TYPE_IDX] = T_DATETIME;
              ret_node.int16_values_[OB_NODE_CAST_N_SCALE_IDX] = 0;
              ret_node.param_num_ = 0;
            }
            break;
          }
          case JPN_DOUBLE : {
            if (json_expr_flag == OPT_JSON_QUERY) { // do nothing
            } else {
              ret_node.type_ = T_CAST_ARGUMENT;
              ret_node.value_ = 0;
              ret_node.int16_values_[OB_NODE_CAST_TYPE_IDX] = T_DOUBLE;
              ret_node.int16_values_[OB_NODE_CAST_N_PREC_IDX] = -1;
              ret_node.int16_values_[OB_NODE_CAST_N_SCALE_IDX] = -85;  /* SCALE_UNKNOWN_YET */
              ret_node.param_num_ = 0;
            }
            break;
          }
          case JPN_LENGTH :
          case JPN_SIZE :
          case JPN_NUM_ONLY :
          case JPN_NUMBER :
          case JPN_FLOOR :
          case JPN_CEILING : {
            if (ret_node.type_ == T_NULL
            || (json_expr_flag == OPT_JSON_QUERY && ret_node.int16_values_[OB_NODE_CAST_TYPE_IDX] == T_JSON)) {
              ret_node.value_ = 0;
              ret_node.int16_values_[OB_NODE_CAST_TYPE_IDX] = T_VARCHAR;
              ret_node.int16_values_[OB_NODE_CAST_COLL_IDX] = 0;
              ret_node.int32_values_[OB_NODE_CAST_C_LEN_IDX] = 4000;
              ret_node.length_semantics_ = 0;
              ret_node.is_hidden_const_ = 1;
            } else {
              ret_node.value_ = 0;
              ret_node.int16_values_[OB_NODE_CAST_TYPE_IDX] = T_NUMBER;
              ret_node.int16_values_[OB_NODE_CAST_N_PREC_IDX] = -1;    /* precision */
              ret_node.int16_values_[OB_NODE_CAST_N_SCALE_IDX] = -85;    /* scale */
              ret_node.int16_values_[OB_NODE_CAST_NUMBER_TYPE_IDX] = NPT_EMPTY;    /* number type */
              ret_node.param_num_ = 0;
            }
            ret_node.type_ = T_CAST_ARGUMENT;
            break;
          }
          case JPN_TIMESTAMP : {
            if (json_expr_flag == OPT_JSON_QUERY) { // do nothing
            } else {
              ret_node.type_ = T_CAST_ARGUMENT;
              ret_node.value_ = 0;
              ret_node.int16_values_[OB_NODE_CAST_TYPE_IDX] = T_TIMESTAMP_NANO;
              ret_node.int16_values_[OB_NODE_CAST_N_SCALE_IDX] = 6;
              ret_node.param_num_ = 0;
            }
            break;
          }
          case JPN_TYPE:
          case JPN_STR_ONLY :
          case JPN_STRING : {
            if (json_expr_flag == OPT_JSON_QUERY && ret_node.int16_values_[OB_NODE_CAST_TYPE_IDX] == T_JSON) {
            } else {
              ret_node.type_ = T_CAST_ARGUMENT;
              ret_node.value_ = 0;
              ret_node.int16_values_[OB_NODE_CAST_TYPE_IDX] = T_VARCHAR;
              ret_node.int16_values_[OB_NODE_CAST_COLL_IDX] = 0;
              ret_node.int32_values_[OB_NODE_CAST_C_LEN_IDX] = 4000;
              ret_node.length_semantics_ = 0;
              ret_node.is_hidden_const_ = 1;
            }
          }
          case JPN_UPPER:
          case JPN_LOWER: {
            if (json_expr_flag == OPT_JSON_QUERY && ret_node.int16_values_[OB_NODE_CAST_TYPE_IDX] == T_JSON) {
            } else {
              ret_node.type_ = T_CAST_ARGUMENT;
              ret_node.value_ = 0;
              ret_node.int16_values_[OB_NODE_CAST_TYPE_IDX] = T_VARCHAR;
              ret_node.int16_values_[OB_NODE_CAST_COLL_IDX] = 0;
              ret_node.int32_values_[OB_NODE_CAST_C_LEN_IDX] = 75;
              ret_node.length_semantics_ = 0;
              ret_node.is_hidden_const_ = 1;
            }
            break;
          }
          default : {
            break;
          }
        }
      }
    }
  }
  return ret;
}

JsonPathIterator ObJsonPath::begin() const
{
  return path_nodes_.begin();
}

JsonPathIterator ObJsonPath::end() const
{
  return path_nodes_.end();
}

int ObJsonPath::append(ObJsonPathNode* json_node)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(json_node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN( "fail to append, the json_node is null", K(ret));
  } else if (path_node_cnt() >= ObJsonPathUtil::MAX_PATH_NODE_CNT) {
    ret = OB_ERR_DATA_TOO_LONG;
    LOG_WARN("there may not have more than 100 path node",
    K(ret), K(expression_), K(path_node_cnt()));
  } else if (OB_FAIL(path_nodes_.push_back(json_node))) {
    LOG_WARN("fail to push back", K(ret));
  }
  return ret;
}

bool ObJsonPath::can_match_many() const
{
  bool ret_bool = false;
  for (uint32_t i = 0; i < path_nodes_.size() && ret_bool == false; ++i) {
    switch(path_nodes_[i]->get_node_type()) {
      case JPN_MEMBER_WILDCARD:
      case JPN_ARRAY_CELL_WILDCARD:
      case JPN_WILDCARD_ELLIPSIS:
      case JPN_ARRAY_RANGE: {
        ret_bool = true;
        break;
      }
      default: {
        ret_bool = false;
      }
    }
  }
  return ret_bool;
}

inline bool ObJsonPath::is_contained_wildcard_or_ellipsis() const
{
  return is_contained_wildcard_or_ellipsis_;
}

ObString& ObJsonPath::get_path_string()
{
  return expression_;
}

ObJsonPathBasicNode* ObJsonPath::path_node(int index)
{
  return static_cast<ObJsonPathBasicNode*>(path_nodes_[index]);
}

ObJsonPathBasicNode* ObJsonPath::last_path_node()
{
  return (path_nodes_.size() > 1 ? path_node(path_nodes_.size() - 1): path_node(0));
}


bool ObJsonPathCache::is_match(ObString& path_str, size_t idx)
{
  bool result = false;
  if (idx < size()) {
    ObJsonPath* path = path_at(idx);
    if (OB_NOT_NULL(path)) {
      result = ObJsonPathUtil::string_cmp_skip_charactor(path_str, path->get_path_string(), ' ') == 0;
    }
  }
  return result;
}

int ObJsonPathCache::find_and_add_cache(ObJsonPath*& res_path, ObString& path_str, int arg_idx, bool is_const)
{
  INIT_SUCC(ret);
  if (!((is_const && arg_idx < size()) || is_match(path_str, arg_idx))) {
    void* buf = allocator_->alloc(sizeof(ObJsonPath));
    if (OB_NOT_NULL(buf)) {
      ObJsonPath* path = new (buf) ObJsonPath(path_str, allocator_);
      if (OB_FAIL(path->parse_path())) {
        LOG_WARN("wrong path expression, parse path failed or with wildcards", K(ret), K(path_str));
      } else {
        ret = set_path(path, path_str.length() == 0 ? OK_NULL : OK_NOT_NULL, arg_idx, arg_idx);
        res_path = path;
      }
    } else {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc path.", K(ret));
    }
  } else {
    res_path = path_at(arg_idx);
  }
  return ret;
}

int ObJsonPathCache::fill_empty(size_t reserve_size)
{
  INIT_SUCC(ret);
  if (reserve_size > path_arr_ptr_.size()) {
    // fill element in vector
    if (OB_FAIL(path_arr_ptr_.reserve(reserve_size))) {
      LOG_WARN("fail to reserve for path arr.", K(ret), K(reserve_size));
    } else if (OB_FAIL(stat_arr_.reserve(reserve_size))) {
      LOG_WARN("fail to reserve for stat arr.", K(ret), K(reserve_size));
    } else if (path_arr_ptr_.size() != stat_arr_.size()) {
      LOG_WARN("Length is not equals.", K(ret), K(reserve_size));
    }
    for (size_t cur = path_arr_ptr_.size(); OB_SUCC(ret) && cur < reserve_size; ++cur) {
      if (OB_FAIL(path_arr_ptr_.push_back(NULL))) {
        LOG_WARN("fail to push NUll to path arr", K(ret));
      } else if (OB_FAIL(stat_arr_.push_back(ObPathCacheStat()))) {
        LOG_WARN("fail to push stat to stat arr", K(ret));
      }
    }
  }
  return ret;
}

ObPathParseStat ObJsonPathCache::path_stat_at(size_t idx)
{
  ObPathParseStat stat = UNINITIALIZED;
  if (idx < stat_arr_.size()) {
    stat = stat_arr_[idx].state_;
  }
  return stat;
}

ObJsonPath* ObJsonPathCache::path_at(size_t idx)
{
  ObJsonPath *ptr = nullptr;
  if (idx < path_arr_ptr_.size()) {
    ptr = path_arr_ptr_[idx];
  }
  return ptr;
}

void ObJsonPathCache::reset()
{
  stat_arr_.clear();
  path_arr_ptr_.clear();
}

size_t ObJsonPathCache::size()
{
  return path_arr_ptr_.size();
}

int ObJsonPathCache::set_path(ObJsonPath* path, ObPathParseStat stat, int arg_idx, int index)
{
  INIT_SUCC(ret);
  if (OB_FAIL(fill_empty(arg_idx + 1))) {
    LOG_WARN("fail to fill empty.", K(ret), K(arg_idx));
  } else if (index >= path_arr_ptr_.size()) {
    ret = OB_ERROR_OUT_OF_RANGE;
    LOG_WARN("index out of range.", K(ret), K(index), K(path_arr_ptr_.size()));
  } else {
    path_arr_ptr_[index] = path;
    stat_arr_[index] = ObPathCacheStat(stat, arg_idx);
  }
  return ret;
}

void ObJsonPathCache::set_allocator(common::ObIAllocator *allocator)
{
  if (allocator != nullptr && size() == 0) {
    allocator_ = allocator;
  }
}

common::ObIAllocator* ObJsonPathCache::get_allocator()
{
  return allocator_;
}
 
int ObJsonPathUtil::append_array_index(uint64_t index, bool from_end, ObJsonBuffer& str)
{
  INIT_SUCC(ret);

  if (from_end) {
    if (OB_FAIL(str.append("last"))) {
      LOG_WARN("fail to append the 'last' ", K(ret));
    } else {
      // if index > 0, it should have '-' after 'last' 
      // such as：$[last-3 to last-1]
      if (index > 0) {
        if (OB_FAIL(str.append("-"))) {
          LOG_WARN("fail to append the '-' ", K(ret));
        }
      }
    }// append last
  }// end from end

  if (OB_SUCC(ret)) {
    if (from_end && index == 0) {
      // if have 'last' with index 0, such as : [last]
      // do nothing
    } else {
      char res_ptr[OB_MAX_DECIMAL_PRECISION] = {0};
      char* ptr = nullptr;
      if (OB_ISNULL(ptr = ObCharset::lltostr(index, res_ptr, 10, 1))) {
          ret = OB_BAD_NULL_ERROR;
          LOG_WARN("fail to transform the index(lltostr)", K(ret), K(index));
      } else {
        if (OB_FAIL(str.append(res_ptr, static_cast<int32_t>(ptr - res_ptr)))) {
          LOG_WARN("fail to append the index", K(ret));
        }
      }
    }// from_end && index==0
  }

  return ret;
}

int ObJsonPathUtil::pop_char_stack(ObCharArrayPointers& char_stack)
{
  INIT_SUCC(ret);
  uint64_t size = char_stack.size();
  if (OB_FAIL(char_stack.remove(size - 1))) {
    LOG_WARN("fail to remove char top.",K(ret), K(char_stack[size - 1]));
  }
  return ret;
}

int ObJsonPathUtil::pop_filter_stack(ObFilterArrayPointers& filter_stack)
{
  INIT_SUCC(ret);
  uint64_t size = filter_stack.size();
  if (OB_FAIL(filter_stack.remove(size - 1))) {
    LOG_WARN("fail to remove filter top.",K(ret));
  }
  return ret;
}

int ObJsonPathBasicNode::node_to_string(ObJsonBuffer& str, bool is_mysql, bool is_next_array)
{
  INIT_SUCC(ret);
  if (is_mysql) {
    if (OB_FAIL(mysql_to_string(str))) {
      LOG_WARN("fail to append JPN_WILDCARD_ELLIPSIS", K(ret));
    }
  } else {
    if (OB_FAIL(oracle_to_string(str, is_next_array))) {
      LOG_WARN("fail to append JPN_WILDCARD_ELLIPSIS", K(ret));
    }
  }
  return ret;
}

int ObJsonPathBasicNode::mysql_to_string(ObJsonBuffer& str)
{
  INIT_SUCC(ret);
  switch (node_type_) {
    // ** , do append
    case JPN_WILDCARD_ELLIPSIS: {
      if (OB_FAIL(str.append("**"))) {
        LOG_WARN("fail to append JPN_WILDCARD_ELLIPSIS", K(ret));
      }
      break;
    }
    // .* , do append
    case JPN_MEMBER_WILDCARD: {
      if (OB_FAIL(str.append(".*"))) {
        LOG_WARN("fail to append JPN_MEMBER_WILDCARD", K(ret));
      }
      break;
    }
    // [*] , do append
    case JPN_ARRAY_CELL_WILDCARD: {
      if (OB_FAIL(str.append("[*]"))) {
        LOG_WARN("fail to append JPN_ARRAY_CELL_WILDCARD", K(ret));
      }
      break;
    }
    // MEMBER, add .name
    // need to check is_ecmascript_identifier
    // if check is_ecmascript_identifier false, should do double_quote 
    case JPN_MEMBER: {
      if (OB_FAIL(str.append("."))) {
        LOG_WARN("fail to append BEGIN_MEMBER");
      } else {
        if (node_content_.member_.len_ == 0) {
          if (OB_FAIL(str.append("\"\""))) {
            LOG_WARN("fail to append BEGIN_MEMBER");
          }
        } else {
          ObString object_name(node_content_.member_.len_, node_content_.member_.object_name_);
          ObJsonBuffer tmp_object_name(str.get_allocator());
          if (!ObJsonPathUtil::is_ecmascript_identifier(object_name.ptr(), object_name.length())) {
            if (OB_FAIL(ObJsonPathUtil::double_quote(object_name, &tmp_object_name))) {
              LOG_WARN("fail to add ObJsonPathUtil::double_quote", K(ret));
            } else {
              if (OB_FAIL(str.append(tmp_object_name.ptr(), tmp_object_name.length()))) {
                LOG_WARN("fail to append object_name", K(ret), K(tmp_object_name.length()));
              }
            }
          } else {
            if (OB_FAIL(str.append(object_name))) {
              LOG_WARN("fail to append object_name", K(ret));
            }
          }
          tmp_object_name.reset();
        }
      }
      break;
    }
    // JPN_ARRAY_CELL, add [index]. If from_end is true, add last
    case JPN_ARRAY_CELL: {
      if (OB_FAIL(str.append("["))) {
        LOG_WARN("fail to append BEGIN_ARRAY", K(ret));
      } else if (OB_FAIL(ObJsonPathUtil::append_array_index(node_content_.array_cell_.index_,
                                                            node_content_.array_cell_.is_index_from_end_,
                                                            str))) {
        LOG_WARN("fail to append ARRAY_INDEX.", K(ret));
      } else if (OB_FAIL(str.append("]"))) {
        LOG_WARN("fail to append END_ARRAY", K(ret));
      }
      break;
    }
    // JPN_ARRAY_CELL, add [idx1 to idx2]. If from_end is true, add last
    case JPN_ARRAY_RANGE: {
      if (OB_FAIL(str.append("["))) {
        LOG_WARN("fail to append BEGIN_ARRAY", K(ret));
      } else if (OB_FAIL(ObJsonPathUtil::append_array_index(node_content_.array_range_.first_index_, 
                                                            node_content_.array_range_.is_first_index_from_end_,
                                                            str))) {
        LOG_WARN("fail to append ARRAY_INDEX(from).", K(ret));
      } else if (OB_FAIL(str.append(" to "))) {
        LOG_WARN("fail to append 'to'.", K(ret));
      } else if (OB_FAIL(ObJsonPathUtil::append_array_index(node_content_.array_range_.last_index_, 
                                                            node_content_.array_range_.is_last_index_from_end_,
                                                            str))) {
        LOG_WARN("fail to append ARRAY_INDEX(end).", K(ret));
      } else if (OB_FAIL(str.append("]"))) {
        LOG_WARN("fail to append END_ARRAY", K(ret));
      }
      break;
    }

    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("fail to transfrom PathNode to string, wrong type", K(ret), K(node_type_));
      break;
    }
  }
  return ret;
}

int ObJsonPathBasicNode::oracle_to_string(ObJsonBuffer& str, bool is_next_array)
{
  INIT_SUCC(ret);
  switch (node_type_) {
    // .. , do append
    case JPN_DOT_ELLIPSIS: {
      if (is_next_array) {
        if (OB_FAIL(str.append(".."))) {
          LOG_WARN("fail to append JPN_WILDCARD_ELLIPSIS", K(ret));
        }
      } else {
        if (OB_FAIL(str.append("."))) {
          LOG_WARN("fail to append JPN_WILDCARD_ELLIPSIS", K(ret));
        }
      }
      break;
    }
    // .* , do append
    case JPN_MEMBER_WILDCARD: {
      if (OB_FAIL(str.append(".*"))) {
        LOG_WARN("fail to append JPN_MEMBER_WILDCARD", K(ret));
      }
      break;
    }
    // [*] , do append
    case JPN_ARRAY_CELL_WILDCARD: {
      if (OB_FAIL(str.append("[*]"))) {
        LOG_WARN("fail to append JPN_ARRAY_CELL_WILDCARD", K(ret));
      }
      break;
    }

    // MEMBER, add .name
    // need to check is_ecmascript_identifier
    // if check is_ecmascript_identifier false, should do double_quote
    case JPN_MEMBER: {
      if (OB_FAIL(str.append("."))) {
        LOG_WARN("fail to append BEGIN_MEMBER");
      } else {
        if (node_content_.member_.len_ == 0) {
          if (OB_FAIL(str.append("\"\""))) {
            LOG_WARN("fail to append BEGIN_MEMBER");
          }
        } else {
          ObString object_name(node_content_.member_.len_, node_content_.member_.object_name_);
          ObJsonBuffer tmp_object_name(str.get_allocator());
          if (!ObJsonPathUtil::is_oracle_keyname(object_name.ptr(), object_name.length())) {
            if (OB_FAIL(ObJsonPathUtil::double_quote(object_name, &tmp_object_name))) {
              LOG_WARN("fail to add ObJsonPathUtil::double_quote", K(ret));
            } else {
              if (OB_FAIL(str.append(tmp_object_name.ptr(), tmp_object_name.length()))) {
                LOG_WARN("fail to append object_name", K(ret), K(tmp_object_name.length()));
              }
            }
          } else {
            if (OB_FAIL(str.append(object_name))) {
              LOG_WARN("fail to append object_name", K(ret));
            }
          }
          tmp_object_name.reset();
        }
      }
      break;
    }
    // JPN_ARRAY_CELL, add [idx1 to idx2]. If from_end is true, add last
    case JPN_MULTIPLE_ARRAY: {
      if (OB_FAIL(str.append("["))) {
        LOG_WARN("fail to append BEGIN_ARRAY", K(ret));
      }
      int i = 0;
      int size = node_content_.multi_array_.size();
      while (OB_SUCC(ret) && i < size) {
        ObPathArrayRange* tmp = node_content_.multi_array_[i];
        // first index to sting
        if (OB_ISNULL(tmp)) {
          ret = OB_BAD_NULL_ERROR;
          LOG_WARN("ArrayRange is null", K(ret));
        } else if (OB_FAIL(ObJsonPathUtil::append_array_index(tmp->first_index_,
                                                       tmp->is_first_index_from_end_,
                                                       str))) {
          LOG_WARN("fail to append ARRAY_INDEX.", K(ret));
        } else if ((tmp->first_index_ != tmp->last_index_)
                  || (tmp->is_first_index_from_end_ != tmp->is_last_index_from_end_)) {
          if (OB_FAIL(str.append(" to "))) {
            LOG_WARN("fail to append 'to'.", K(ret));
          } else if ( OB_FAIL(ObJsonPathUtil::append_array_index(tmp->last_index_,
                                                                   tmp->is_last_index_from_end_,
                                                                   str))) {
            LOG_WARN("fail to append ARRAY_INDEX(end).", K(ret));
          }
        }
        if (OB_SUCC(ret) && (i + 1 < size)) {
          if (OB_FAIL(str.append(", "))) {
            LOG_WARN("fail to append ', '.", K(ret));
          }
        }

        ++i;
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(str.append("]"))) {
          LOG_WARN("fail to append END_ARRAY", K(ret));
        }
      }
      break;
    }

    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("fail to transfrom PathNode to string, wrong type", K(ret), K(node_type_));
      break;
    }
  }
  return ret;
}

static constexpr char* func_str_map[JPN_END_FUNC_FLAG - JPN_BEGIN_FUNC_FLAG - 1] = {
  const_cast<char*>("abs()"),
  const_cast<char*>("boolean()"),
  const_cast<char*>("booleanOnly()"),
  const_cast<char*>("ceiling()"),
  const_cast<char*>("date()"),
  const_cast<char*>("double()"),
  const_cast<char*>("floor()"),
  const_cast<char*>("length()"),
  const_cast<char*>("lower()"),
  const_cast<char*>("number()"),
  const_cast<char*>("numberOnly()"),
  const_cast<char*>("size()"),
  const_cast<char*>("string()"),
  const_cast<char*>("stringOnly()"),
  const_cast<char*>("timestamp()"),
  const_cast<char*>("type()"),
  const_cast<char*>("upper()")
};

int ObJsonPathFuncNode::node_to_string(ObJsonBuffer& str, bool is_mysql, bool is_next_array)
{
  INIT_SUCC(ret);
  if (OB_FAIL(str.append("."))) {
    LOG_WARN("fail to append '.'", K(ret));
  } else {
    if (node_type_ > JPN_BEGIN_FUNC_FLAG && node_type_ < JPN_END_FUNC_FLAG) {
      if (OB_FAIL(str.append(func_str_map[node_type_ - JPN_BEGIN_FUNC_FLAG - 1]))) {
        LOG_WARN("fail to append function", K(node_type_),
        K(func_str_map[node_type_ - JPN_BEGIN_FUNC_FLAG - 1]), K(ret));
      }
    }
  }
  return ret;
}

int ObJsonPathFilterNode::comp_half_to_string(ObJsonBuffer& str, bool is_left)
{
  INIT_SUCC(ret);
  ObCompContent* half_comp = nullptr;
  ObJsonPathNodeType half_type;
  if (is_left) {
    half_comp = &(node_content_.comp_.comp_left_);
    half_type = node_content_.comp_.left_type_;
  } else {
    half_comp = &(node_content_.comp_.comp_right_);
    half_type = node_content_.comp_.right_type_;
  }

  switch (half_type) {
    case ObJsonPathNodeType::JPN_SCALAR: {
      ObString half_scalar(half_comp->path_scalar_.s_length_, half_comp->path_scalar_.scalar_);
      if (OB_FAIL(str.append(half_scalar))) {
        LOG_WARN("fail to append half_scalar ", K(ret),K(half_scalar));
      }
      break;
    }

    case ObJsonPathNodeType::JPN_SQL_VAR: {
      if (OB_FAIL(str.append("$"))) {
        LOG_WARN("fail to append '$'!", K(ret));
      } else {
        ObString half_var(half_comp->path_var_.v_length_, half_comp->path_var_.var_);
        if (OB_FAIL(str.append(half_var))) {
          LOG_WARN("fail to append half_var ", K(ret),K(half_var));
        }
      }
      break;
    }

    case ObJsonPathNodeType::JPN_SUB_PATH: {
      if (OB_ISNULL(half_comp->filter_path_)
        || OB_FAIL(half_comp->filter_path_->to_string(str))) {
        LOG_WARN("sub_path fail to string ", K(ret));
      }
      break;
    }

    case ObJsonPathNodeType::JPN_BOOL_TRUE: {
      if (OB_FAIL(str.append("true"))) {
        LOG_WARN("fail to append 'true' ", K(ret));
      }
      break;
    }

    case ObJsonPathNodeType::JPN_BOOL_FALSE: {
      if (OB_FAIL(str.append("false"))) {
        LOG_WARN("fail to append 'false' ", K(ret));
      }
      break;
    }

    case ObJsonPathNodeType::JPN_NULL: {
      if (OB_FAIL(str.append("null"))) {
        LOG_WARN("fail to append 'null' ", K(ret));
      }
      break;
    }

    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("fail to transfrom PathNode to string, wrong type", K(ret), K(node_type_));
      break;
    }
  }
  return ret;
}

static constexpr char* comp_str_map[JPN_EXISTS - JPN_BEGIN_FILTER_FLAG] = {
  const_cast<char*>(" == "),
  const_cast<char*>(" != "),
  const_cast<char*>(" > "),
  const_cast<char*>(" >= "),
  const_cast<char*>(" < "),
  const_cast<char*>(" <= "),
  const_cast<char*>(" has substring "),
  const_cast<char*>(" starts with "),
  const_cast<char*>(" like "),
  const_cast<char*>(" like_regex "),
  const_cast<char*>(" eq_regex "),
  const_cast<char*>("!exists("),
  const_cast<char*>("exists(")
};

int ObJsonPathFilterNode::comp_to_string(ObJsonBuffer& str)
{
  INIT_SUCC(ret);

  // comparison left
  bool is_left = true;
  if (node_type_ != ObJsonPathNodeType::JPN_EXISTS && node_type_ != ObJsonPathNodeType::JPN_NOT_EXISTS) {
    if (OB_FAIL(comp_half_to_string(str, is_left))) {
      LOG_WARN("left comparison fail to_string", K(ret), K(node_type_));
    }
  }

  if (OB_SUCC(ret)
  && node_type_ > JPN_BEGIN_FILTER_FLAG && node_type_ <= JPN_EXISTS) {
    if (OB_FAIL(str.append(comp_str_map[node_type_ - JPN_BEGIN_FILTER_FLAG - 1]))) {
      LOG_WARN("fail to append function", K(node_type_),
      K(comp_str_map[node_type_ - JPN_BEGIN_FILTER_FLAG - 1]), K(ret));
    }
  }

  // comparison right
  is_left = false;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(comp_half_to_string(str, is_left))) {
      LOG_WARN("left comparison fail to_string", K(ret), K(node_type_));
    } else if (node_type_ == ObJsonPathNodeType::JPN_EXISTS
      || node_type_ == ObJsonPathNodeType::JPN_NOT_EXISTS) {
      if (OB_FAIL(str.append(")"))) {
        LOG_WARN("fail to append ')' for exists/!exists.", K(ret), K(node_type_));
      }
    }
  }
  return ret;
}

int ObJsonPathFilterNode::cond_to_string(ObJsonBuffer& str)
{
  INIT_SUCC(ret);

  // left arg to_string
  if (node_type_ != ObJsonPathNodeType::JPN_NOT_COND) {
    if (OB_ISNULL(node_content_.cond_.cond_left_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("argument is null", K(ret), K(node_type_));
    } else {
      if (OB_FAIL(node_content_.cond_.cond_left_->node_to_string(str, false, false))) {
        LOG_WARN("fail to_string", K(ret), K(node_type_));
      }
    }
  }

  // cond to string
  if (OB_SUCC(ret)) {
    switch (node_type_) {
      case ObJsonPathNodeType::JPN_AND_COND: {
        if (OB_FAIL(str.append(ObJsonPathItem::COND_AND))) {
          LOG_WARN("fail to append &&", K(ret));
        }
        break;
      }
      case ObJsonPathNodeType::JPN_OR_COND: {
        if (OB_FAIL(str.append(ObJsonPathItem::COND_OR))) {
          LOG_WARN("fail to append ||", K(ret));
        }
        break;
      }
      case ObJsonPathNodeType::JPN_NOT_COND: {
        if (OB_FAIL(str.append(ObJsonPathItem::COND_NOT))) {
          LOG_WARN("fail to append cond_not", K(ret));
        }
        break;
      }
      default: {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("fail to transfrom PathNode to string, wrong type", K(ret), K(node_type_));
        break;
      }
    }
  }

  // right to string
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(node_content_.cond_.cond_right_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("argument is null", K(ret), K(node_type_));
    } else {
      if (OB_FAIL(node_content_.cond_.cond_right_->node_to_string(str, false, false))) {
        LOG_WARN("fail to_string", K(ret), K(node_type_));
      } else {
        // !(), should append ')'
        if (node_type_ == ObJsonPathNodeType::JPN_NOT_COND) {
          if (OB_FAIL(str.append(")"))) {
            LOG_WARN("fail to append ')'", K(ret));
          }
        }
      }
    }
  }

  return ret;
}

int ObJsonPathFilterNode::node_to_string(ObJsonBuffer& str,  bool is_mysql, bool is_next_array)
{
  INIT_SUCC(ret);
    // comp type
  if (node_type_ >= ObJsonPathNodeType::JPN_EQUAL && node_type_ <= ObJsonPathNodeType::JPN_EXISTS) {
    if (OB_FAIL(comp_to_string(str))) {
      LOG_WARN("fail to_string at comp_node", K(ret));
    }
    // cond type
  } else if (node_type_ >= ObJsonPathNodeType::JPN_AND_COND && node_type_ < ObJsonPathNodeType::JPN_MAX) {
    if (OB_FAIL(cond_to_string(str))) {
      LOG_WARN("fail to_string at cond_node", K(ret));
    }
  }

  return ret;
}

int ObJsonPath::to_string(ObJsonBuffer& str)
{
  INIT_SUCC(ret);

  if (is_sub_path_ && !is_mysql_) {
    if (OB_FAIL(str.append("@"))) {
      LOG_WARN("fail to append '@'", K(ret));
    }
  } else {
    if (OB_FAIL(str.append("$"))) {
      LOG_WARN("fail to append '$'", K(ret));
    }
  }

  bool is_next_array = false;
  if (OB_FAIL(ret)) {
  } else if (is_mysql_) {
    for (int i = 0; OB_SUCC(ret) && i < path_nodes_.size() ; ++i) {
      if (OB_FAIL(path_nodes_[i]->node_to_string(str, true, false))) {
        LOG_WARN("parse node to string failed.", K(i));
      }
    }
  } else {
    for (int i = 0; OB_SUCC(ret) && i < path_nodes_.size() ; ++i) {
      if ((path_nodes_[i]->get_node_type() == ObJsonPathNodeType::JPN_DOT_ELLIPSIS)) {
        if (i + 1 > path_nodes_.size()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("shouldn't end up with ellipsis node", K(i));
          break;
        } else {
          if (path_nodes_[i + 1]->get_node_type() == ObJsonPathNodeType::JPN_ARRAY_CELL_WILDCARD
            || path_nodes_[i + 1]->get_node_type() == ObJsonPathNodeType::JPN_MULTIPLE_ARRAY) {
            is_next_array = true;
          }
        }
      }

      // is_basic_node/fun_node tostring
      if (path_nodes_[i]->get_node_type() < ObJsonPathNodeType::JPN_END_FUNC_FLAG) {
        if (OB_FAIL(path_nodes_[i]->node_to_string(str, false, is_next_array))) {
            LOG_WARN("parse node to string failed.", K(i));
        }
      // filter_node to string
      } else if ( (path_nodes_[i]->get_node_type() > ObJsonPathNodeType::JPN_BEGIN_FILTER_FLAG)
              && (path_nodes_[i]->get_node_type() < ObJsonPathNodeType::JPN_END_FILTER_FLAG)) {
        if (OB_FAIL(str.append("?("))) {
          LOG_WARN("fail to append '?('", K(ret));
        } else {
          if (OB_FAIL(path_nodes_[i]->node_to_string(str, false, false))) {
            LOG_WARN("parse node to string failed.", K(i));
          } else {
            if (OB_FAIL(str.append(")"))) {
              LOG_WARN("fail to append ')'", K(ret));
            }
          }
        }
      }// end of filter path
      is_next_array = false;
    }
  } // end of oracle to string

  return ret;
}

// parse path to path Node
// based on is_mysql_
int ObJsonPath::parse_path()
{
  INIT_SUCC(ret);
  bad_index_ = -1;
  if (is_mysql_) {
    if (OB_FAIL(parse_mysql_path())) {
      LOG_WARN("fail to parse mysql JSON Path Expression!", K(ret), K(index_));
    }
  } else {
    if (OB_FAIL(parse_oracle_path())) {
      LOG_WARN("fail to parse oracle JSON Path Expression!", K(ret), K(index_));
    }
  }
  return ret;
}

// parse mysql path to path Node
int ObJsonPath::parse_mysql_path()
{
  INIT_SUCC(ret);

  if (!is_mysql_) {
    LOG_WARN("Should be mysql JSON Path Expression!");
  } else {
    // the first non-whitespace character must be $
    int len = expression_.length();
    bool first = true;
    ObJsonPathUtil::skip_whitespace(expression_, index_);

    if (index_ >= len || (expression_[index_] != ObJsonPathItem::ROOT)) {
      ret = OB_INVALID_ARGUMENT;
      bad_index_ = 1;
      LOG_WARN("An path expression begins with a dollar sign ($)", K(ret));
    } else {
      ++index_;
      ObJsonPathUtil::skip_whitespace(expression_, index_);

      while (index_ < len && OB_SUCC(ret)) {
        if (OB_FAIL(parse_mysql_path_node())) {
          bad_index_ =  index_;
          LOG_WARN("fail to parse JSON Path Expression!", K(ret), K(index_));
        } else {
          ObJsonPathUtil::skip_whitespace(expression_, index_);
        }
      } // end while
    }

    // make sure the last node isn't JPN_WILDCARD_ELLIPSIS
    if (OB_SUCC(ret) && path_node_cnt() > 0
        && path_nodes_[path_node_cnt() - 1]->get_node_type() == JPN_WILDCARD_ELLIPSIS) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("wrong path expression, shouldn't end up with **!", K(ret));
    }

    if (OB_SUCC(ret)) {
      if ( bad_index_ == -1) {
        // do nothing
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("wrong path expression!", K(ret), K(bad_index_));
      }
    }
  }

  return ret;
}

// parse mysql_path to path json node
int ObJsonPath::parse_mysql_path_node()
{
  int ret =  OB_SUCCESS;
  ObJsonPathUtil::skip_whitespace(expression_, index_);

  if (index_ >= expression_.length()) {
    ret = OB_ARRAY_OUT_OF_RANGE;
    LOG_WARN("wrong path expression", K(ret), K(index_));
  } else {
    // JPN_ARRAY_CELL,JPN_ARRAY_RANGE,JPN_ARRAY_CELL_WILDCARD begin with '['
    // JPN_MEMBER,JPN_MEMBER_WILDCARD begin with '.'
    // JPN_WILDCARD_ELLIPSIS begin with '*'
    switch (expression_[index_]) {
      case ObJsonPathItem::BEGIN_ARRAY: {
        if (OB_FAIL(parse_single_array_node())) {
          LOG_WARN("fail to parse array node", K(ret), K(index_));
        }
        break;
      }
      case ObJsonPathItem::BEGIN_MEMBER: {
        if (OB_FAIL(parse_mysql_member_node())) {
          LOG_WARN("fail to parse member node", K(ret), K(index_));
        }
        break;
      }
      case ObJsonPathItem::WILDCARD: {
        if (OB_FAIL(parse_wildcard_ellipsis_node())) {
          LOG_WARN("fail to parse ellipsis node", K(ret), K(index_));
        }
        break;
      }
      default: {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("wrong path expression, should be '[', '.' or '*'.",
        K(ret), K(index_), K(expression_));
        break;
      }
    }
  }

  if (OB_SUCC(ret) && path_node_cnt() > 0
      && path_nodes_[path_node_cnt() - 1]->get_node_type() == JPN_DOT_ELLIPSIS) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("wrong path expression, shouldn't end up with **!", K(ret));
  }

  if (OB_SUCC(ret)) {
    if ( bad_index_ == -1) {
      // do nothing
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("wrong path expression!", K(ret), K(bad_index_));
    }
  }
  return ret;
}

// parse **(JPN_WILDCARD_ELLIPSIS)
// @return  the error code.
int ObJsonPath::parse_wildcard_ellipsis_node()
{
  INIT_SUCC(ret);

  if (index_ < expression_.length() && expression_[index_] == ObJsonPathItem::WILDCARD) {
    ++index_;
    if (index_ < expression_.length() && expression_[index_] == ObJsonPathItem::WILDCARD) {
      ++index_;
      ObJsonPathUtil::skip_whitespace(expression_, index_);
    } else {
      ret = OB_INVALID_DATA;
      LOG_WARN("supposed to be double wildecard!", K(ret), K(index_), K(expression_));
    }
  } else {
    ret = OB_INVALID_DATA;
    LOG_WARN("supposed to be double wildecards!", K(ret), K(index_), K(expression_));
  }

  // if ** is last char or has ***
  if (OB_SUCC(ret)) {
    if (index_ >= expression_.length()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("may not end up with **!", K(ret), K(index_));
    } else if (expression_[index_] == ObJsonPathItem::WILDCARD) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("may not have three wildcards!", K(ret), K(index_), K(expression_));
    }
  }

  if (OB_SUCC(ret)) {
    ObJsonPathBasicNode* ellipsis_node = 
    static_cast<ObJsonPathBasicNode*> (allocator_->alloc(sizeof(ObJsonPathBasicNode)));
    if (OB_ISNULL(ellipsis_node)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate row buffer failed at ellipsis_node", K(ret), K(index_), K(expression_));
    } else {
      ellipsis_node = new (ellipsis_node) ObJsonPathBasicNode(allocator_);
      if (OB_FAIL(ellipsis_node->init(JPN_WILDCARD_ELLIPSIS, is_mysql_))) {
        LOG_WARN("fail to init path basic node with type", K(ret), K(JPN_WILDCARD_ELLIPSIS));
      } else if (OB_FAIL(append(ellipsis_node))) {
        LOG_WARN("fail to append JsonPathNode(JPN_WILDCARD_ELLIPSIS)!", K(ret), K(index_), K(expression_));
      } else {
        is_contained_wildcard_or_ellipsis_ = true;
      }
    }
  }
  return ret;
}

// get array index, range：
// start from expression[index_] until non-digit char
// parse str to int32_t, return index
int ObJsonPathUtil::get_index_num(const ObString& expression, uint64_t& idx, uint64_t& array_idx)
{
  INIT_SUCC(ret);
  array_idx = 0;
  ObJsonPathUtil::skip_whitespace(expression, idx);
  uint64_t start = 0;
  uint64_t end = 0;

  if (idx < expression.length()) {
    if (isdigit(expression[idx])) {
      start = idx;
      ++idx;
      while (idx < expression.length() && isdigit(expression[idx])) {
        ++idx;
      }
      // Here should have ']' after digits, return error when reaches the end
      if (idx >= expression.length()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("digit shouldn't be the end of the expression.", K(ret), K(idx));
      } else {
        end = idx - 1;
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("should be digit.", K(ret), K(idx));
    }
  } else {
    ret = OB_ARRAY_OUT_OF_RANGE;
    LOG_WARN("index of path expression out of range.", K(ret), K(idx));
  }

  if (OB_SUCC(ret)) {
    if (end >= start) {
      int err = 0;
      const char* ptr = expression.ptr() + start;
      array_idx = ObCharset::strntoull(ptr, end - start + 1, 10, &err);
      if (err == 0) {
        if (array_idx > ObJsonPathUtil::MAX_LENGTH) {
          ret = OB_DATA_OUT_OF_RANGE;
          LOG_WARN("index > 4294967295", K(ret), K(array_idx));
        }
      } else {
        ret = OB_ERR_DATA_TOO_LONG;
        LOG_WARN("input value out of range", K(ret), K(start), K(end));
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("end should bigger than start", K(ret), K(start), K(end));
    }
  }

  return ret;
}

// process ARRAY_CELL and ARRAY_RANGE
int ObJsonPath::parse_single_array_index(uint64_t& array_index, bool& from_end)
{
  INIT_SUCC(ret);
  ObJsonPathUtil::skip_whitespace(expression_, index_);
  const int last_len = strlen("last");
  // check has 'last'
  if (expression_.length() - index_ >= last_len  && expression_[index_] == 'l') {
    ObString last_tmp(last_len, expression_.ptr()+index_);
    if (last_tmp.prefix_match("last")) {
      // it has last, set from_end true
      from_end = true;
      index_ += last_len;
      ObJsonPathUtil::skip_whitespace(expression_, index_);
    } else {
      ret = OB_ERROR;
      LOG_WARN("should be 'last'.", K(ret), K(index_), K(expression_));
    }
  }

  // Here will be three situation：
  // 'last' : get the last
  // 'last-num', such as 'last-3' : get the last number
  // 'num' : get the first number
  if (OB_FAIL(ret)) {
  } else if (from_end) {
    if (index_ < expression_.length() && expression_[index_] == ObJsonPathItem::MINUS) {
      ++index_;
      ObJsonPathUtil::skip_whitespace(expression_, index_);

      if (index_ >= expression_.length()) {
        ret = OB_ARRAY_OUT_OF_RANGE;
        LOG_WARN("index of path expression out of range.", K(ret), K(index_));
      } else if (OB_FAIL(ObJsonPathUtil::get_index_num(expression_, index_, array_index))) {
        LOG_WARN("fail to get the index_num.", K(ret), K(index_), K(expression_));
      }
    } else {
      array_index = 0;
    } // end of '-'
  } else if (index_ < expression_.length()) {
    if (OB_FAIL(ObJsonPathUtil::get_index_num(expression_, index_, array_index))) {
      LOG_WARN("fail to get the index_num.", K(ret), K(index_), K(expression_));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("worong path expression.", K(ret), K(index_), K(expression_));
  }

  return ret;
}

// parse **(JPN_ARRAY_CELL & RANGE)
// @return  the error code.
// three situation：
// '[*]' : just build ObJsonPathNode with type JPN_ARRAY_CELL_WILDCARD, and append to JsonPath
// otherwise call parse_single_array_index() to do parse
// if type is CELL, process one arg
// if type is RANGE, process two arg between 'to'
int ObJsonPath::parse_single_array_node()
{
  INIT_SUCC(ret);
  // begin with '['
  if (index_ >= expression_.length()) {
    ret = OB_ARRAY_OUT_OF_RANGE;
    LOG_WARN("index out of range!", K(ret), K(index_), K(expression_));
  } else {
    if (expression_[index_] == ObJsonPathItem::BEGIN_ARRAY) {
      ++index_;
      ObJsonPathUtil::skip_whitespace(expression_, index_);
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("array should start with '['!", K(ret), K(index_), K(expression_));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (index_ < expression_.length()) {
    // if '[*]'
    if (expression_[index_] == ObJsonPathItem::WILDCARD) {
      ++index_;
      if ( OB_FAIL(parse_array_wildcard_node()) ) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("fail to parse the array_wildcard_node!", K(ret), K(index_), K(expression_));
      }
    } else { // range or cell
      // process CELL or RANGE
      // parser first arg
      // 1. init var, use is_cell_type to mark if CELL or RANGE
      bool is_cell_type = true;
      uint64_t index1 = 0, index2 = 0;
      bool from_end1 = false, from_end2 = false;

      // 2. parse first arg
      if (OB_FAIL(parse_single_array_index(index1, from_end1))) {
        LOG_WARN("fail to parse the index of array!", K(ret), K(index_), K(expression_));
      } else {
        // 3. check if has 'to'
        ObJsonPathUtil::skip_whitespace(expression_, index_);

        // it should have 'to'
        if (index_ + 2 < expression_.length() && expression_[index_] == 't') {
          // make suce ' to ', 'to' should space around in mysql
          ObString to_tmp(4, expression_.ptr() + index_ - 1);
          if (to_tmp.prefix_match(" to ")) {
            // jump over 'to'
            index_ += 3;
            is_cell_type = false;

            // parse second arg
            if (OB_FAIL(parse_single_array_index(index2, from_end2))) {
              LOG_WARN("fail to parse the index of array!", K(ret), K(index_), K(expression_));
            }
          } else {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("wrong path expression, should be ' to ' in range_node.",
                      K(ret), K(index_), K(expression_));
          }
        }// end of 'to'
      }// end of get index

      // 4. It should have valid char after index
      if (OB_SUCC(ret)) {
        if (index_ < expression_.length()) {
          // 5. check ']' after index
          if (expression_[index_] == ObJsonPathItem::END_ARRAY) {
            ++index_;
            // 6. init array node with is_cell_type
            if (OB_FAIL(add_single_array_node(is_cell_type, index1, index2, from_end1, from_end2))) {
              LOG_WARN("fail to add array node!", K(ret), K(index_), K(expression_));
            }
          } else {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("array should end up whit ']'", K(ret), K(index_), K(expression_));
          }
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("wrong path expression!", K(ret), K(index_), K(expression_));
        }
      }
    }// end of range or cell
  } else {
    // out of range after '['
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("wrong path expression!", K(ret), K(index_), K(expression_));
  }
  
  return ret;
}

int ObJsonPath::add_single_array_node(bool is_cell_type, uint64_t& index1, uint64_t& index2,
    bool& from_end1, bool& from_end2)
{
  INIT_SUCC(ret);
  if (is_cell_type) {
    ObJsonPathBasicNode* cell_node = 
    static_cast<ObJsonPathBasicNode*> (allocator_->alloc(sizeof(ObJsonPathBasicNode)));
    if (OB_ISNULL(cell_node)) {
      // error
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate row buffer failed at cell_node",
      K(ret), K(index_), K(expression_));
    } else {
      cell_node = new (cell_node) ObJsonPathBasicNode(allocator_, index1, from_end1);
      if (OB_FAIL(append(cell_node))) {
        LOG_WARN("fail to append JsonPathNode(cell_node)!", 
        K(ret) , K(index_), K(expression_));
      }
    }
  } else {
    // 6.2 ARRAY_RANGE, check index if valid (mysql)
    if (from_end1 == from_end2 && ((from_end1 && index1 < index2)
        || (!from_end1 && index2 < index1))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("wrong range of array!", 
        K(ret), K(index1), K(index2), K(from_end1), K(from_end2), K(expression_));
    }

    if (OB_SUCC(ret)) {
      ObJsonPathBasicNode* range_node = 
      static_cast<ObJsonPathBasicNode*> (allocator_->alloc(sizeof(ObJsonPathBasicNode)));
      if (OB_ISNULL(range_node)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate row buffer failed at range_node", K(ret), K(index_), K(expression_));
      } else {
        range_node = new (range_node) ObJsonPathBasicNode(allocator_, index1, from_end1, index2, from_end2);
        if (OB_FAIL(append(range_node))) {
          LOG_WARN("fail to append JsonPathNode!(range_node)", K(ret), K(index_), K(expression_));
        }
      }
    } 
  }

  return ret;
}

int ObJsonPath::parse_name_with_rapidjson(char*& str, uint64_t& len)
{
  INIT_SUCC(ret);
  const char *syntaxerr = nullptr;
  uint64_t *offset = nullptr;
  ObJsonNode* dom = nullptr;

  if (OB_FAIL(ObJsonParser::parse_json_text(allocator_, str, len, syntaxerr, offset, dom))) {
    LOG_WARN("fail to parse_name_with_rapidjson.", K(ret), KCSTRING(str), KCSTRING(syntaxerr), K(offset));
  } else if (dom->json_type() != ObJsonNodeType::J_STRING) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected json type.", K(ret), KCSTRING(str), K(dom->json_type()));
  } else {
    ObJsonString *val = static_cast<ObJsonString *>(dom);
    len = val->value().length();
    str = static_cast<char*> (allocator_->alloc(len));
    if (OB_ISNULL(str)) {
      if (len != 0) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory for member_name.", K(ret), K(len), K(val->value()));
      }
    } else {
      MEMCPY(str, val->value().ptr(), len);
    }
  }
  
  return ret;
}

// if keyname without double quote, end with ' ', '.', '[', '*'
bool ObJsonPathUtil::is_key_name_terminator(char ch)
{
  bool ret_bool = false;
  switch (ch) {
    case ' ': {
      ret_bool = true;
      break;
    }
    case '.': {
      ret_bool = true;
      break;
    }
    case '[': {
      ret_bool = true;
      break;
    }
    case '*': {
      ret_bool = true;
      break;
    }
    case '(': {
      ret_bool = true;
      break;
    }
    case '?': {
      ret_bool = true;
      break;
    }
    case '-': {
      ret_bool = true;
      break;
    }
    default: {
      break;
    }
  }
  return ret_bool;
}

// parse JPN_MEMBER_WILDCARD
// @return  the error code.
int ObJsonPath::parse_member_wildcard_node()
{
  INIT_SUCC(ret);
  // skip '*'
  ++index_;
  ObJsonPathBasicNode* member_wildcard_node =
  static_cast<ObJsonPathBasicNode*> (allocator_->alloc(sizeof(ObJsonPathBasicNode)));

  if (OB_ISNULL(member_wildcard_node)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate row buffer failed at member_wildcard_node",
    K(ret), K(index_), K(expression_));
  } else {
    member_wildcard_node = new (member_wildcard_node) ObJsonPathBasicNode(allocator_);
    if (OB_FAIL(member_wildcard_node->init(JPN_MEMBER_WILDCARD, is_mysql_))) {
      LOG_WARN("fail to PathBasicNode init with type", K(ret), K(JPN_MEMBER_WILDCARD));
    } else if (OB_FAIL(append(member_wildcard_node))) {
      LOG_WARN("fail to append JsonPathNode(member_wildcard_node)!", K(ret), K(index_), K(expression_));
    } else {
      is_contained_wildcard_or_ellipsis_ = true;
    }
  }
  return ret;
}

// parse [*]
// @return  the error code.
int ObJsonPath::parse_array_wildcard_node()
{
  INIT_SUCC(ret);
  ObJsonPathUtil::skip_whitespace(expression_, index_);
  if (index_ < expression_.length() && expression_[index_] == ObJsonPathItem::END_ARRAY) {
    ++index_;
    ObJsonPathBasicNode* cell_wildcard_node =
    static_cast<ObJsonPathBasicNode*> (allocator_->alloc(sizeof(ObJsonPathBasicNode)));
    if (OB_ISNULL(cell_wildcard_node)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate row buffer failed at cell_wildcard_node", K(ret), K(expression_));
    } else {
      cell_wildcard_node = new (cell_wildcard_node) ObJsonPathBasicNode(allocator_);
      if (OB_FAIL(cell_wildcard_node->init(JPN_ARRAY_CELL_WILDCARD, is_mysql_))) {
        allocator_->free(cell_wildcard_node);
        LOG_WARN("fail to PathBasicNode init with type", K(ret), K(JPN_ARRAY_CELL_WILDCARD));
      } else if (OB_FAIL(append(cell_wildcard_node))) {
        LOG_WARN("fail to append JsonPathNode(cell_wildcard_node)!", K(ret), K(expression_));
      } else {
        is_contained_wildcard_or_ellipsis_ = true;
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("should end up with ']' !", K(ret), K(index_), K(expression_));
  }
  return ret;
}

// parse (JPN_MEMBER/JPN_MEMBER_WILDCARD)
// @return  the error code.
int ObJsonPath::parse_mysql_member_node()
{
  INIT_SUCC(ret);

  // 1. start from '.'
  if (index_ < expression_.length() && expression_[index_] == ObJsonPathItem::BEGIN_MEMBER) {
    // 2. skip '.' and space if exist
    ++index_;
    ObJsonPathUtil::skip_whitespace(expression_, index_);

    // 3. process char after '.'
    //   three situation:
    //   a. JPN_MEMBER_WILDCARD
    //   b. keyname JPN_MEMBER with quote
    //   c. keyname JPN_MEMBER without quote
    if (index_ < expression_.length()) {
      // JPN_MEMBER_WILDCARD
      if (expression_[index_] == ObJsonPathItem::WILDCARD) {
        if (OB_FAIL(parse_member_wildcard_node())) {
          LOG_WARN("fail to parse member wildcard node!", K(ret), K(index_), K(expression_));
        }
      } else {
        bool is_quoted  = false;

        // check double quote
        if (expression_[index_] == ObJsonPathItem::DOUBLE_QUOTE) is_quoted = true;

        char* name = nullptr;
        uint64_t name_len = 0;
        bool is_func = false;
        bool with_escape = false;
        // get name 
        // add double quote for rapidjson requires
        if (OB_FAIL(get_origin_key_name(name, name_len, is_quoted, is_func, with_escape))) {
          LOG_WARN("fail to get keyname!", K(ret), K(index_), K(expression_));
        } else {
          if (OB_FAIL(parse_name_with_rapidjson(name, name_len))) {
            LOG_WARN("fail to parse name with rapidjson",
            K(ret), K(index_), K(expression_),KCSTRING(name));
          }

          if (OB_SUCC(ret) && !is_quoted) {
            if (!ObJsonPathUtil::is_ecmascript_identifier(name, name_len)) {
              LOG_WARN("the key name isn't ECMAScript identifier!",
                K(ret), KCSTRING(name));
            }
          }

          if (OB_SUCC(ret)) {
            ObJsonPathBasicNode* member_node = 
            static_cast<ObJsonPathBasicNode*> (allocator_->alloc(sizeof(ObJsonPathBasicNode)));
            if (OB_ISNULL(member_node)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("allocate row buffer failed at member_node", K(ret), K(index_), K(expression_));
            } else {
              member_node = new (member_node) ObJsonPathBasicNode(allocator_, name, name_len);
              if (OB_FAIL(append(member_node) )) {
                LOG_WARN("fail to append JsonPathNode(member_node)!",
                K(ret), K(index_), K(expression_));
              }
            }
          }
        }
      } // JPN_MEMBER type
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("wrong path expression, may not end up with '.'!", 
      K(ret), K(index_), K(expression_));
    }
  } else {
    if (index_ < expression_.length()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("in parse_mysql_member_node(), should start with '.' ", K(ret), K(index_), K(expression_));
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("idx out of range!", K(ret), K(index_), K(expression_));
    }
  }

  return ret;
}

bool ObJsonPathUtil::is_whitespace(char ch)
{
  return (ch == ' ' || ch == '\t' || ch == '\n');
}

bool ObJsonPathUtil::letter_or_not(char ch)
{
  return ((('a' <= ch) && (ch <= 'z')) || (('A' <= ch) && (ch <= 'Z')));
}

bool ObJsonPathUtil::is_begin_field_name(char ch)
{
  bool ret_bool = false;
  if (ch == '_' || ch == '"' || letter_or_not(ch)) ret_bool = true;
  return ret_bool;
}

bool ObJsonPathUtil::is_end_of_comparission(char ch) {
  bool ret_bool = false;
  switch (ch) {
    case '(':
    case ')':
    case '&':
    case '|':
    case '!': {
      ret_bool = true;
      break;
    }
    default: {
      break;
    }
  }
  return ret_bool;
}

bool ObJsonPathUtil::is_digit(char ch)
{
  return (('0' <= ch) && (ch <= '9'));
}

bool ObJsonPathUtil::is_scalar(const ObJsonPathNodeType node_type)
{
  bool ret_bool = false;
  if (node_type >= JPN_BOOL_TRUE && node_type <= JPN_SCALAR) {
    ret_bool = true;
  }
  return ret_bool;
}

bool ObJsonPathUtil::is_escape(char ch)
{
  return (('\n' == ch) || (ch == '\t') || (ch == '\r') || (ch == '\f') || (ch == '\e'));
}
int ObJsonPathUtil::append_character_of_escape(ObJsonBuffer& buf, char ch)
{
  INIT_SUCC(ret);
  if ('\n' == ch) {
    ret = buf.append("n");
  } else if ('\t' == ch) {
    ret = buf.append("t");
  } else if ('\r' == ch) {
    ret = buf.append("r");
  } else if ('\f' == ch) {
    ret = buf.append("f");
  } else if ('\e' == ch) {
    ret = buf.append("e");
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should be escape", K(ch), K(ret));
  }
  return ret;
}


void ObJsonPathUtil::skip_whitespace(const ObString &path, uint64_t& idx)
{
  while (idx < path.length() && ObJsonPathUtil::is_whitespace(path[idx])) {
    idx++;
  }
}

/*
priority_comparison = {
  // in:     (,    &,   |,   !,   )
     top:
     0:(  {'<', '<', '<', '<', '='},
     1:&  {'<', '>', '>', '<', '>'},
     2:|  {'<', '<', '>', '<', '>'},
     3:!  {'<', '>', '>', ' ', '>'},
     4:)  {' ', ' ', ' ', ' ', ' '},
};
*/

// if top < in, push into stack
// if top > in, pop the top char, then push
// if top == in, pop the top and do not push
char ObJsonPathUtil::priority(char top, char in) {
  char ret_char = ' ';
  switch (in) {
    case '(': {
      switch (top) {
        case '(': {
          ret_char = '<';
          break;
        }
        case '&': {
          ret_char = '<';
          break;
        }
        case '|': {
          ret_char = '<';
          break;
        }
        case '!': {
          ret_char = '<';
          break;
        }
        default: {
          break;
        }
      }
      break;
    }
    case '&': {
      switch (top) {
        case '(': {
          ret_char = '<';
          break;
        }
        case '&': {
          ret_char = '>';
          break;
        }
        case '|': {
          ret_char = '<';
          break;
        }
        case '!': {
          ret_char = '>';
          break;
        }
        default: {
          break;
        }
      }
      break;
    }
    case '|': {
      switch (top) {
        case '(': {
          ret_char = '<';
          break;
        }
        case '&': {
          ret_char = '>';
          break;
        }
        case '|': {
          ret_char = '>';
          break;
        }
        case '!': {
          ret_char = '>';
          break;
        }
        default: {
          break;
        }
      }
      break;
    }
    case '!': {
      switch (top) {
        case '(': {
          ret_char = '<';
          break;
        }
        case '&': {
          ret_char = '<';
          break;
        }
        case '|': {
          ret_char = '<';
          break;
        }
        default: {
          break;
        }
      }
      break;
    }
    case ')': {
      switch (top) {
        case '(': {
          ret_char = '=';
          break;
        }
        case '&': {
          ret_char = '>';
          break;
        }
        case '|': {
          ret_char = '>';
          break;
        }
        case '!': {
          ret_char = '>';
          break;
        }
        default: {
          break;
        }
      }
      break;
    }
    default: {
      break;
    }
  }
  return ret_char;
}

static constexpr unsigned UNICODE_COMBINING_MARK_MIN = 0x300;
static constexpr unsigned UNICODE_COMBINING_MARK_MAX = 0x36F;
static constexpr unsigned UNICODE_EXTEND_MARK_MIN = 0x2E80;
static constexpr unsigned UNICODE_EXTEND_MARK_MAX = 0x9fff;

bool ObJsonPathUtil::unicode_combining_mark(unsigned codepoint) {
  return ((UNICODE_COMBINING_MARK_MIN <= codepoint) && (codepoint <= UNICODE_COMBINING_MARK_MAX));
}

bool ObJsonPathUtil::is_utf8_unicode_charator(const char* ori, uint64_t& start, int64_t len)
{
  bool ret_bool = false;
  int ret = OB_SUCCESS;
  int32_t well_formed_error = 0;
  int64_t well_formed_len = 0;
  if (OB_FAIL(ObCharset::well_formed_len(CS_TYPE_UTF8MB4_BIN, ori + start, len, well_formed_len, well_formed_error))) {
    ret_bool = false;
  } else {
    ret_bool = true;
  }

  return ret_bool;
}

bool ObJsonPathUtil::is_letter(unsigned codepoint, const char* ori, uint64_t start, uint64_t end) {
  bool ret_bool = true;

  ret_bool = isalpha(codepoint);
  if (ret_bool) {
  } else if (!ret_bool && (codepoint >= UNICODE_EXTEND_MARK_MIN
             && codepoint <= UNICODE_EXTEND_MARK_MAX
             && ObJsonPathUtil::is_utf8_unicode_charator(ori, start, end - start))) {
    ret_bool = true;
  }
  return ret_bool;
}

bool ObJsonPathUtil::is_connector_punctuation(unsigned codepoint) {
  bool ret_bool = true;
  switch (codepoint) {
    case 0x5F:    // low line
    case 0x203F:  // undertie
    case 0x2040:  // character tie
    case 0x2054:  // inverted undertie
    case 0xFE33:  // presentation form for vertical low line
    case 0xFE34:  // presentation form for vertical wavy low line
    case 0xFE4D:  // dashed low line
    case 0xFE4E:  // centerline low line
    case 0xFE4F:  // wavy low line
    case 0xFF3F:  // fullwidth low line
    {
      ret_bool = true;
      break;
    }
    default: {
      ret_bool = false;
      break;
    }
  }

  return ret_bool;
}

bool ObJsonPathUtil::is_ecmascript_identifier(const char* name, uint64_t length)
{
  bool ret_bool = true;
  // An empty string is not a valid identifier.
  if (OB_ISNULL(name)) ret_bool = false;

  /*
    At this point, The unicode escape sequences have already
    been replaced with the corresponding UTF-8 bytes. Now we apply
    the rules here: https://es5.github.io/x7.html#x7.6
  */
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
    if (is_letter(codepoint, name, last_pos, curr_pos - last_pos)
        || codepoint == 0x24 || codepoint == 0x5F ) {
      // $ is ok, _ is ok
    } else if (first_codepoint) {
      /*
        the first character must be one of the above.
        more possibilities are available for subsequent characters.
      */
      ret_bool = false;
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "first character must be $, _ or letter.",
          K(ret_bool), K(codepoint), K(input_stream.Tell()), KCSTRING(name));
    } else if (unicode_combining_mark(codepoint) || isdigit(codepoint)
              || is_connector_punctuation(codepoint)
              || codepoint == 0x200C || codepoint == 0x200D){
      // unicode combining mark
      // a unicode digit
      // <ZWNJ> or <ZWJ>
      // are bot ok
    } else {
      // nope
      ret_bool = false;
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "not ecmascript identifier.",
          K(ret_bool), K(codepoint), K(input_stream.Tell()), KCSTRING(name));
    }
  }

  return ret_bool;
}

int ObJsonPathUtil::string_cmp_skip_charactor(ObString& lhs, ObString& rhs, char ch) {
  int res = 0;
  int i = 0, j = 0;
  for (; i < lhs.length() && j < rhs.length(); ) {
    bool skip = false;
    if (lhs[i] == ch) {
      ++i;
      skip = true;
    }

    if (rhs[j] == ch) {
      ++j;
      skip = true;
    }

    if (skip) {
      continue;
    }

    if (lhs[i] != rhs[j]) {
      res = lhs[i] > rhs[j] ? 1 : -1;
      break;
    }
    ++i;
    ++j;
  }

  while (res == 0 && i < lhs.length()) {
    if (lhs[i] != ch) {
      res = 1;
      break;
    }
    ++i;
  }

  while (res == 0 && j < rhs.length()) {
    if (rhs[j] != ch) {
      res = -1;
      break;
    }
    ++j;
  }

  return res;
}

int ObJsonPathUtil::double_quote(ObString &name, ObJsonBuffer* tmp_name)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(name.ptr())) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("name_ptr is null", K(ret));
  } else if (OB_FAIL(ObJsonBaseUtil::add_double_quote(*tmp_name, name.ptr(), name.length()))) {
    LOG_WARN("fail to add_double_quote.", K(ret));
  }

  return ret;
}

bool ObJsonPathUtil::path_gives_duplicates(const JsonPathIterator &begin,
                                           const JsonPathIterator &end,
                                           bool auto_wrap)
{
  bool ret_bool = false;
  JsonPathIterator it = begin;
  for (; it != end && !ret_bool; it++) {
    if ((*it)->get_node_type() == JPN_WILDCARD_ELLIPSIS) {
      ret_bool = true;
    }
  }
  return ret_bool;
}

// parse oracle path to path Node
int ObJsonPath::parse_oracle_path()
{
  INIT_SUCC(ret);

  if (is_mysql_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Should be oracle JSON Path Expression!");
  } else {

    // the first non-whitespace character must be $
    int len = expression_.length();
    bool first = true;
    ObJsonPathUtil::skip_whitespace(expression_, index_);

    // we found that "lax $[*].z" is leagl, while "strict $[*].z" is illegal
    // but the oracle default lax model, so there is no way to set strict mode
    if(index_ < len && (expression_[index_] == 'l' || expression_[index_] == 's')) {
      int mode_len = 0;
      ObString mode_str;
      if (expression_[index_] == 'l') {
        mode_len = strlen("lax");
        ObString mode_tmp(mode_len, expression_.ptr()+index_);
        if (mode_tmp.prefix_match("lax")) {
          index_ += mode_len;
          ObJsonPathUtil::skip_whitespace(expression_, index_);
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("An path expression begins with a dollar sign ($)", K(ret));
        }
      } else {
        mode_len = strlen("strict");
        ObString mode_tmp(mode_len, expression_.ptr()+index_);
        if (mode_tmp.prefix_match("strict")) {
          is_lax_ = false;
          index_ += mode_len;
          ObJsonPathUtil::skip_whitespace(expression_, index_);
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("An path expression begins with a dollar sign ($)", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (index_ >= len || (expression_[index_] != ObJsonPathItem::ROOT)) {
        bad_index_ = 1;
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("An path expression begins with a dollar sign ($)", K(ret));
      } else {
        ++index_;
        ObJsonPathUtil::skip_whitespace(expression_, index_);

        while (index_ < len && OB_SUCC(ret)) {
          if (OB_FAIL(parse_oracle_path_node())) {
            bad_index_ =  index_;
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("fail to parse JSON Path Expression!", K(ret), K(index_));
          } else {
            ObJsonPathUtil::skip_whitespace(expression_, index_);
          }
        } // end while
      }
    }

    // make sure the last node is function node(if there is func_node) and the ellipsis_node isn't the last
    if (OB_SUCC(ret) && path_node_cnt() > 0 ) {
      int idx = 0;
      while (idx < path_node_cnt()) {
        ObJsonPathNodeType pnode = path_nodes_[idx]->get_node_type();
        if (pnode > ObJsonPathNodeType::JPN_BEGIN_FUNC_FLAG
            && pnode < ObJsonPathNodeType::JPN_END_FUNC_FLAG) {
          if (idx != path_node_cnt() - 1) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("Wrong JSON Path Expression, function item should be the end of path expression!", K(ret), K(index_));
          }
        }
        ++idx;
      }

      if (OB_SUCC(ret) && get_last_node_type() == ObJsonPathNodeType::JPN_DOT_ELLIPSIS) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("wrong path expression, shouldn't end up with ..!", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if ( bad_index_ == -1) {
        // do nothing
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("wrong path expression!", K(ret), K(bad_index_));
      }
    }
  }

  return ret;
}

// parse oracle_path to path json node
int ObJsonPath::parse_oracle_path_node()
{
  int ret =  OB_SUCCESS;
  ObJsonPathUtil::skip_whitespace(expression_, index_);

  if (index_ >= expression_.length()) {
    ret = OB_ARRAY_OUT_OF_RANGE;
    LOG_WARN("wrong path expression", K(ret), K(index_));
  } else {
    // JPN_MULTIPLE_ARRAY,JPN_ARRAY_CELL_WILDCARD begin with '['
    // the other basic_nodes begin with '.'
    // filter_nodes begin whith '?'
    switch (expression_[index_]) {
      case ObJsonPathItem::BEGIN_ARRAY: {
        ++index_;
        if (OB_FAIL(parse_multiple_array_node())) {
          LOG_WARN("fail to parse array node", K(ret), K(index_));
        }
        break;
      }
      // may be member/../.fun()
      case ObJsonPathItem::BEGIN_MEMBER: {
        ++index_;
        if (OB_FAIL(parse_dot_node())) {
          LOG_WARN("fail to parse member node", K(ret), K(index_));
        }
        break;
      }
      // may be ？
      case ObJsonPathItem::FILTER_FLAG: {
        ++index_;
        if (OB_FAIL(parse_filter_node())) {
          LOG_WARN("fail to parse filter node", K(ret), K(index_));
        }
        break;
      }
      default: {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("wrong path expression, should be '[', '.' or '*'.",
        K(ret), K(index_), K(expression_));
        break;
      }
    }
  }

  return ret;
}

int ObJsonPath::parse_dot_ellipsis_node()
{
  INIT_SUCC(ret);

  if (index_ >= expression_.length()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("may not end up with ..!", K(ret), K(index_));
  } else if ((expression_[index_] != ObJsonPathItem::BEGIN_ARRAY)
              && ((expression_[index_] != ObJsonPathItem::DOUBLE_QUOTE)
              && !(ObJsonPathUtil::letter_or_not(expression_[index_])))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("'..' must follow a memeber/array node!", K(ret), K(index_), K(expression_));
  }

  if (OB_SUCC(ret)) {
    ObJsonPathBasicNode* ellipsis_node =
    static_cast<ObJsonPathBasicNode*> (allocator_->alloc(sizeof(ObJsonPathBasicNode)));
    if (OB_ISNULL(ellipsis_node)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate row buffer failed at ellipsis_node", K(ret), K(index_), K(expression_));
    } else {
      ellipsis_node = new (ellipsis_node) ObJsonPathBasicNode(allocator_);
      if (OB_FAIL(ellipsis_node->init(JPN_DOT_ELLIPSIS, is_mysql_))) {
        allocator_->free(ellipsis_node);
        LOG_WARN("fail to init path basic node with type", K(ret), K(JPN_WILDCARD_ELLIPSIS));
      } else if (OB_FAIL(append(ellipsis_node))) {
        LOG_WARN("fail to append JsonPathNode(JPN_WILDCARD_ELLIPSIS)!", K(ret), K(index_), K(expression_));
      } else {
        is_contained_wildcard_or_ellipsis_ = true;
      }
    }
  }
  return ret;
}

// parse JPN_MEMBER/JPN_MEMBER_WILDCARD/JPN_DOT_ELLIPSIS
// @return  the error code.
int ObJsonPath::parse_dot_node()
{
  INIT_SUCC(ret);
  ObJsonPathUtil::skip_whitespace(expression_, index_);
  // 3. process char after '.'
  //   three situation:
  //   a. '.*' / '..'
  //   b. keyname JPN_MEMBER with quote
  //   c. keyname JPN_MEMBER without quote
  if (index_ < expression_.length()) {
    // JPN_MEMBER_WILDCARD
    if ((expression_[index_] == ObJsonPathItem::WILDCARD)
        ||(expression_[index_] == ObJsonPathItem::BEGIN_MEMBER)) {
        // .*
      if ((expression_[index_]) ==  ObJsonPathItem::WILDCARD) {
        if (OB_FAIL(parse_member_wildcard_node())) {
          LOG_WARN("fail to parse member wildcard node!", K(ret), K(index_), K(expression_));
        }
      } else {
        // ..
        // skip the second '.' and whitespace
        ++index_;
        ObJsonPathUtil::skip_whitespace(expression_, index_);
        // parse ORACLE_ELLIPSIS
        if (OB_FAIL(parse_dot_ellipsis_node())) {
          LOG_WARN("fail to parse dot ellipsis node!", K(ret), K(index_), K(expression_));
        } else {
          // can only be member
          if (index_ < expression_.length()
              && ObJsonPathUtil::is_begin_field_name(expression_[index_])) {
            if (OB_FAIL(parse_oracle_member_node())) {
              LOG_WARN("fail to parse member node!", K(ret), K(index_), K(expression_));
            }
          } else {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("'..'should follow 'keyname' or 'fun()'", K(ret), K(index_), K(expression_));
          }
        }
      }
    } else {
      // only one '.' could be .member or .fun()
      // without '"' then must be fun()
      if (OB_FAIL(parse_oracle_member_node())) {
        LOG_WARN("fail to parse oracle member node!", K(ret), K(index_), K(expression_));
      }
    }
  } else {
    // out of range
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("wrong path expression, shouldn't end up with'.'",
    K(ret), K(index_), K(expression_));
  }
  return ret;
}

bool ObJsonPathUtil::is_oracle_keyname(const char* name, uint64_t length)
{
  bool ret_bool = true;
  // An empty string is not a valid identifier.
  if (OB_ISNULL(name)) {
    ret_bool = false;
  } else {
    // Oracle keyname is can only start whit letter or _
    if ((letter_or_not(*name)) || (*(name) == '_')) {
      uint64_t idx = 1;
      while (idx < length && ret_bool) {
        if (!OB_ISNULL(name + idx) && (letter_or_not(*(name + idx))
            || is_digit(*(name + idx)) || (*(name + idx) == '_'))) {
          ++idx;
        } else {
          ret_bool = false;
        }
      }
    } else {
      ret_bool = false;
    }
  }
  return ret_bool;
}

// process JPN_MEMBER get keyname
// @param[in,out] name  Keyname
// @param[in] is_quoted
// @return  the error code.
int ObJsonPath::get_origin_key_name(char*& str, uint64_t& length, bool is_quoted, bool& is_func, bool& with_escape)
{
  INIT_SUCC(ret);
  uint64_t start = 0;
  uint64_t end = 0;

  int len = expression_.length();

  if (index_ < len) {
    if (is_quoted) {
      // with quote, check quote
      if (expression_[index_] == ObJsonPathItem::DOUBLE_QUOTE) {
        start = index_;
        ++index_;

        while (index_ < len && end == 0 ) {
          if (expression_[index_] == '\\') {
            index_ += 2;
          } else if (expression_[index_] == ObJsonPathItem::DOUBLE_QUOTE) {
            end = index_;
            ++index_;
          } else if (ObJsonPathUtil::is_escape(expression_[index_])) {
            with_escape = true;
            ++index_;
          } else {
            ++index_;
          }
        }

        if (end == 0 && index_ == len) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("should end with DOUBLE_QUOTE!", K(ret), K(index_), K(expression_));
        }
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("should start with DOUBLE_QUOTE!", K(ret), K(index_), K(expression_));
      }
    } else {
      start = index_;
      if (ObJsonPathUtil::is_digit(expression_[index_])) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("shouldn't start with number!", K(ret), K(index_), K(expression_));
      } else {
         while (index_ < len && end == 0) {
          if (ObJsonPathUtil::is_key_name_terminator(expression_[index_])) {
            end = index_ - 1;
            break;
          } else {
            ++index_;
          }
        }
        if (index_ == len) {
          end = index_ - 1;
        } else {
          ObJsonPathUtil::skip_whitespace(expression_, index_);
          // fun_name + ()
          if (index_ < expression_.length() && expression_[index_] == '(') {
              is_func = true;
          }
        }
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
      len = end - start + 1;
      char* start_ptr = expression_.ptr() + start;
      // no "", could be function name
      if ((!is_quoted) && (!is_func)) {
        length = len + 2;
        str = static_cast<char*> (allocator_->alloc(length));
        if (OB_ISNULL(str)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate memory for member_name.",K(ret), K(len),K(start_ptr));
        } else {
          str[0] = ObJsonPathItem::DOUBLE_QUOTE;
          MEMCPY(str + 1, start_ptr, len);
          str[len + 1] = ObJsonPathItem::DOUBLE_QUOTE;
        }
      } else {
      // whit "" or function
        length = len;
        str = static_cast<char*> (allocator_->alloc(length));
        if (OB_ISNULL(str)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate memory for member_name.",K(ret), K(len),K(start_ptr));
        } else {
          MEMCPY(str, start_ptr, len);
        }
      }
    }
  }

  return ret;
}

// parse JPN_FUNC
// @return  the error code.
int ObJsonPath::parse_func_node(char*& name, uint64_t& len)
{
  INIT_SUCC(ret);
  ObJsonPathUtil::skip_whitespace(expression_, index_);
  if ((index_ < expression_.length()) && (expression_[index_] == '(')) {
    ++index_; // skip'('
    ObJsonPathUtil::skip_whitespace(expression_, index_);
    if ((index_ < expression_.length()) && (expression_[index_] == ')')) {
      ++index_; // skip')'
      ObJsonPathUtil::skip_whitespace(expression_, index_);
      ObJsonPathFuncNode* func_node =
      static_cast<ObJsonPathFuncNode*> (allocator_->alloc(sizeof(ObJsonPathFuncNode)));
      if (OB_ISNULL(func_node)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate row buffer failed at member_node", K(ret), K(index_), K(expression_));
      } else {
        func_node = new (func_node) ObJsonPathFuncNode(allocator_);
        if ((OB_FAIL(func_node->init(name, len))) || OB_FAIL(append(func_node) )) {
          allocator_->free(func_node);
          LOG_WARN("fail to append JsonPathNode(member_node)!",
          K(ret), K(index_), K(expression_));
        }
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("there should be a ')'",K(ret), K(index_), K(expression_),K(name));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("a '(' should followed by the funcname",K(ret), K(index_), K(expression_),K(name));
  }
  return ret;
}

int ObJsonPath::deal_with_escape(char* &str, uint64_t& len)
{
  INIT_SUCC(ret);
  ObJsonBuffer buf(allocator_);
  for (int i = 0; i < len && OB_SUCC(ret); ++i) {
    char* tmp = str + i;
    if (OB_ISNULL(tmp)) {
    } else if (ObJsonPathUtil::is_escape(*tmp)) {
      if (OB_FAIL(buf.append("\\"))) {
        LOG_WARN("fail to append \\.", K(i), K(ret));
      } else if (OB_FAIL(ObJsonPathUtil::append_character_of_escape(buf, *tmp))) {
        LOG_WARN("fail to append_character_of_escape.", K(*tmp), K(i), K(ret));
      }
    } else {
      ret = buf.append(tmp, 1);
    }
  }

  if (OB_SUCC(ret)) {
    str = buf.ptr();
    len = buf.length();
  }
  return ret;
}

// parse JPN_ORACLE_MEMBER
// @return  the error code.
int ObJsonPath::parse_oracle_member_node()
{
  INIT_SUCC(ret);
  ObJsonPathUtil::skip_whitespace(expression_, index_);
  if (index_ < expression_.length()) {
    bool is_quoted  = false;
    bool is_func = false;
    bool with_escape = false;

    // check double quote
    if (expression_[index_] == ObJsonPathItem::DOUBLE_QUOTE) is_quoted = true;

    char* name = nullptr;
    uint64_t name_len = 0;
    // get name
    // add double quote for rapidjson requires
    if (OB_FAIL(get_origin_key_name(name, name_len, is_quoted, is_func, with_escape))) {
      LOG_WARN("fail to get keyname!", K(ret), K(index_), K(expression_));
    } else if (is_quoted || (!is_func)) {
      if (lib::is_oracle_mode() && with_escape && OB_FAIL(ObJsonPath::deal_with_escape(name, name_len))) {
        LOG_WARN("fail to deal escape!", K(ret), K(index_), K(expression_));
      } else if (OB_FAIL(parse_name_with_rapidjson(name, name_len))) {
        LOG_WARN("fail to parse name with rapidjson",
        K(ret), K(index_), K(expression_),KCSTRING(name));
      } else if (!is_quoted) {
        if (!ObJsonPathUtil::is_ecmascript_identifier(name, name_len)) {
          LOG_WARN("the key name isn't ECMAScript identifier!",
            K(ret), KCSTRING(name));
        }
      }

      if (OB_SUCC(ret)) {
        ObJsonPathBasicNode* member_node =
        static_cast<ObJsonPathBasicNode*> (allocator_->alloc(sizeof(ObJsonPathBasicNode)));
        if (OB_ISNULL(member_node)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate row buffer failed at member_node", K(ret), K(index_), K(expression_));
        } else {
          member_node = new (member_node) ObJsonPathBasicNode(allocator_, name, name_len);
          if (OB_FAIL(append(member_node) )) {
            LOG_WARN("fail to append JsonPathNode(member_node)!",
            K(ret), K(index_), K(expression_));
          }
        }
      }
    } else {
    // whit '('，means func
    // now , char* name == function_name, expressin[index] = '('
    // parse func_node
      if (OB_FAIL(parse_func_node(name, name_len))) {
        LOG_WARN("fail to parse function node!",
          K(ret), K(index_), K(expression_));
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("shouldn't end up with '.'",K(ret), K(index_), K(expression_));
  }
  return ret;
}

int ObJsonPath::parse_multiple_array_index(uint64_t& index1, uint64_t& index2,
      bool& from_end1, bool& from_end2)
{
  INIT_SUCC(ret);

  // 2. parse first arg
  if (OB_FAIL(parse_single_array_index(index1, from_end1))) {
    LOG_WARN("fail to parse the index of array!", K(ret), K(index_), K(expression_));
  } else {
    // 3. check if has 'to'
    ObJsonPathUtil::skip_whitespace(expression_, index_);

    // it should have 'to'
    if (index_ + 2 < expression_.length() && expression_[index_] == 't') {
    // make suce ' to ', 'to' should space around in mysql
      ObString to_tmp(4, expression_.ptr() + index_ - 1);
      if (to_tmp.prefix_match(" to ")) {
      // jump over 'to'
      index_ += 3;

      // parse second arg
        if (OB_FAIL(parse_single_array_index(index2, from_end2))) {
          LOG_WARN("fail to parse the index of array!", K(ret), K(index_), K(expression_));
        }
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("wrong path expression, should be ' to ' in range_node.",
                  K(ret), K(index_), K(expression_));
      }
    } else {
      index2 = index1;
      from_end2 = from_end1;
    }
  }// end of get index

  return ret;
}

int ObJsonPath::init_multi_array(ObPathArrayRange* o_array,uint64_t index1, uint64_t index2,
                                  bool from_end1, bool from_end2)
{
  INIT_SUCC(ret);
  // both last
  if (from_end1 && from_end2) {
    if (index1 >= index2) {
      o_array->first_index_ = index1;
      o_array->is_first_index_from_end_ = from_end1;
      o_array->last_index_ = index2;
      o_array->is_last_index_from_end_ = from_end2;
    } else {
      o_array->first_index_ = index2;
      o_array->is_first_index_from_end_ = from_end2;
      o_array->last_index_ = index1;
      o_array->is_last_index_from_end_ = from_end1;
    }
  } else if ((!from_end1) && (!from_end2) ) {
    // both not last
    if (index1 <= index2) {
      o_array->first_index_ = index1;
      o_array->is_first_index_from_end_ = from_end1;
      o_array->last_index_ = index2;
      o_array->is_last_index_from_end_ = from_end2;
    } else {
      o_array->first_index_ = index2;
      o_array->is_first_index_from_end_ = from_end2;
      o_array->last_index_ = index1;
      o_array->is_last_index_from_end_ = from_end1;
    }
  } else {
    o_array->first_index_ = index1;
    o_array->is_first_index_from_end_ = from_end1;
    o_array->last_index_ = index2;
    o_array->is_last_index_from_end_ = from_end2;
  }
  return ret;
}

// parse JPN_MULTIPLE_ARRAY
// @return  the error code.
int ObJsonPath::parse_multiple_array_node()
{
  INIT_SUCC(ret);
  ObJsonPathUtil::skip_whitespace(expression_, index_);

  if (index_ < expression_.length()) {
    // if '[*]'
    if (expression_[index_] == ObJsonPathItem::WILDCARD) {
      ++index_;
      if ( OB_FAIL(parse_array_wildcard_node()) ) {
        LOG_WARN("fail to parse the array_wildcard_node!", K(ret), K(index_), K(expression_));
      }
    } else {
      // range or cell
      // process CELL or RANGE
      // parser first arg
      // 1. init var, use is_cell_type to mark if CELL or RANGE
      uint64_t index1 = 0, index2 = 0;
      bool from_end1 = false, from_end2 = false;
      bool the_first = true;
      ObJsonPathBasicNode* multi_array_node =
        static_cast<ObJsonPathBasicNode*> (allocator_->alloc(sizeof(ObJsonPathBasicNode)));
      if (OB_ISNULL(multi_array_node)) {
        // error
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate row buffer failed at cell_node",K(ret), K(index_), K(expression_));
      } else {
        do {
          // skip ','
          // init the args
          if (!the_first) {
            ++index_;
            index1 = 0, index2 = 0;
            from_end1 = false, from_end2 = false;
          }
          ObJsonPathUtil::skip_whitespace(expression_, index_);
          if (index_ >= expression_.length()) {
            ret = OB_ERROR_OUT_OF_RANGE;
            break;
          }
          if (OB_FAIL(parse_multiple_array_index(index1, index2, from_end1, from_end2))) {
            LOG_WARN("fail to parse the index of array!", K(ret), K(index_), K(expression_));
          } else {
            ObPathArrayRange* o_array =
            static_cast<ObPathArrayRange*> (allocator_->alloc(sizeof(ObPathArrayRange)));
            if (OB_ISNULL(o_array)) {
            // error
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("allocate row buffer failed at oracle_array",K(ret), K(index_), K(expression_));
            } else {
              // init array
              init_multi_array(o_array, index1, index2, from_end1, from_end2);
            }
            if (the_first) {
              multi_array_node = new (multi_array_node) ObJsonPathBasicNode(allocator_, o_array);
              the_first = false;
            } else {
              multi_array_node->add_multi_array(o_array);
            }
          }
          ObJsonPathUtil::skip_whitespace(expression_, index_);
        } while (OB_SUCC(ret) && ((index_ < expression_.length()) && (expression_[index_] == ',')));

        // 4. It should have valid char after index
        if (OB_SUCC(ret)) {
          if (index_ < expression_.length()) {
            // 5. check ']' after index
            if (expression_[index_] == ObJsonPathItem::END_ARRAY) {
              ++index_;
              // 6. init array node with is_cell_type
              if (OB_FAIL(append(multi_array_node))) {
                LOG_WARN("fail to append JsonPathNode(cell_node)!", K(ret) , K(index_), K(expression_));
              }
            } else {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("array should end up whit ']'", K(ret), K(index_), K(expression_));
            }
          } else {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("wrong path expression!", K(ret), K(index_), K(expression_));
          }
        }
      }
    }// end of range or cell
  } else {
    // out of range after '['
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("wrong path expression!", K(ret), K(index_), K(expression_));
  }
  return ret;
}

int ObJsonPath::get_func_comparison_type(ObJsonPathFilterNode* filter_comp_node)
{
  INIT_SUCC(ret);
  const int like_len = strlen("like");
  const int exists_len = strlen("exists");
  const int not_exists_len = strlen("!exists");
  const int eq_regex_len = strlen("eq_regex");
  const int like_regex_len = strlen("like_regex");
  const int starts_len = strlen("starts");
  const int with_len = strlen("with");
  const int has_len = strlen("has");
  const int substring_len = strlen("substring");

  // like or like_regex, both begin with 'l'
  if (expression_[index_] == 'l' && expression_.length() - index_ >= like_len ) {
    ObString like_tmp(like_len, expression_.ptr()+index_);
    ObString like_regex_tmp(like_regex_len, expression_.ptr()+index_);
    if (expression_.length() - index_ >= like_regex_len
       && like_regex_tmp.prefix_match("like_regex")) {
      filter_comp_node->init_comp_type(ObJsonPathNodeType::JPN_LIKE_REGEX);
      index_ += like_regex_len;
    } else if (like_tmp.prefix_match("like")) {
      filter_comp_node->init_comp_type(ObJsonPathNodeType::JPN_LIKE);
      index_ += like_len;
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("wrong comparison type!", K(ret), K(index_), K(expression_));
    }
  // exists or eq_regex, both begin with 'e'
  } else if (expression_[index_] == 'e' && expression_.length() - index_ >= exists_len) {
    ObString exists_tmp(exists_len, expression_.ptr()+index_);
    ObString eq_regex_tmp(eq_regex_len, expression_.ptr()+index_);
    if (expression_.length() - index_ >= eq_regex_len
       && eq_regex_tmp.prefix_match("eq_regex")) {
      filter_comp_node->init_comp_type(ObJsonPathNodeType::JPN_EQ_REGEX);
      index_ += eq_regex_len;
    } else if (exists_tmp.prefix_match("exists")) {
      filter_comp_node->init_comp_type(ObJsonPathNodeType::JPN_EXISTS);
      index_ += exists_len;
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("wrong comparison type!", K(ret), K(index_), K(expression_));
    }
  } else if (expression_[index_] == '!' && expression_.length() - index_ >= not_exists_len) {
    ++index_;
    // there could spaces between ! and exists
    ObJsonPathUtil::skip_whitespace(expression_, index_);
    ObString exists_tmp(exists_len, expression_.ptr()+index_);
    if (expression_.length() - index_ >= exists_len && exists_tmp.prefix_match("exists")) {
      filter_comp_node->init_comp_type(ObJsonPathNodeType::JPN_NOT_EXISTS);
      index_ += exists_len;
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("wrong comparison type!", K(ret), K(index_), K(expression_));
    }
  // has substring
  } else if (expression_[index_] == 'h' && expression_.length() - index_ >= has_len) {
    ObString has_tmp(has_len, expression_.ptr()+index_);
    if (has_tmp.prefix_match("has")) {
      index_ += has_len;
      ObJsonPathUtil::skip_whitespace(expression_, index_);
      ObString substring_tmp(substring_len, expression_.ptr()+index_);
      if (expression_.length() - index_ >= substring_len
       && substring_tmp.prefix_match("substring")) {
        filter_comp_node->init_comp_type(ObJsonPathNodeType::JPN_SUBSTRING);
        index_ += substring_len;
       } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("wrong comparison type!", K(ret), K(index_), K(expression_));
       }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("wrong comparison type!", K(ret), K(index_), K(expression_));
    }
  // starts with
  } else if (expression_.length() - index_ >= starts_len && expression_[index_] == 's') {
    ObString starts_tmp(starts_len, expression_.ptr()+index_);
    if (starts_tmp.prefix_match("starts")) {
      index_ += starts_len;
      ObJsonPathUtil::skip_whitespace(expression_, index_);
      ObString with_tmp(with_len, expression_.ptr()+index_);
      if (expression_.length() - index_ >= with_len
          && with_tmp.prefix_match("with")) {
        filter_comp_node->init_comp_type(ObJsonPathNodeType::JPN_STARTS_WITH);
        index_ += with_len;
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("wrong comparison type!", K(ret), K(index_), K(expression_));
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("wrong comparison type!", K(ret), K(index_), K(expression_));
    }
  }

  return ret;
}

int ObJsonPath::get_char_comparison_type(ObJsonPathFilterNode* filter_comp_node)
{
  INIT_SUCC(ret);
  if ( expression_.length() - index_ >= 2) {
    switch (expression_[index_]) {
      case '>': {
        ++index_;
        if (expression_[index_] == '=') {
          ++index_;
          if (OB_FAIL(filter_comp_node->init_comp_type(ObJsonPathNodeType::JPN_LARGGER_EQUAL))) {
            LOG_WARN("fail to init comparison_type", K(ret), K(index_), K(expression_));
          }
        } else {
          if (OB_FAIL(filter_comp_node->init_comp_type(ObJsonPathNodeType::JPN_LARGGER))) {
            LOG_WARN("fail to init comparison_type", K(ret), K(index_), K(expression_));
          }
        }
        break;
      }
      case '<': {
        ++index_;
        if (expression_[index_] == '=') {
          ++index_;
          if (OB_FAIL(filter_comp_node->init_comp_type(ObJsonPathNodeType::JPN_SMALLER_EQUAL))) {
            LOG_WARN("fail to init comparison_type", K(ret), K(index_), K(expression_));
          }
        } else {
          if (OB_FAIL(filter_comp_node->init_comp_type(ObJsonPathNodeType::JPN_SMALLER))) {
            LOG_WARN("fail to init comparison_type", K(ret), K(index_), K(expression_));
          }
        }
        break;
      }
      case '=': {
        ++index_;
        if (expression_[index_] == '=') {
          ++index_;
          if (OB_FAIL(filter_comp_node->init_comp_type(ObJsonPathNodeType::JPN_EQUAL))) {
            LOG_WARN("fail to init comparison_type", K(ret), K(index_), K(expression_));
          }
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("supposed to have another '='!", K(ret), K(index_), K(expression_));
        }
        break;
      }
      case '!': {
        ++index_;
        if (expression_[index_] == '=') {
          ++index_;
          if (OB_FAIL(filter_comp_node->init_comp_type(ObJsonPathNodeType::JPN_UNEQUAL))) {
            LOG_WARN("fail to init comparison_type", K(ret), K(index_), K(expression_));
          }
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("supposed to have '='!", K(ret), K(index_), K(expression_));
        }
        break;
      }
      default:
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("not comp type", K(ret), K(index_), K(expression_));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("shouldn't end up with comparison!", K(ret), K(index_), K(expression_));
  }
  return ret;
}

// get_comparison_type
// @return  the error code.
int ObJsonPath::get_comparison_type(ObJsonPathFilterNode* filter_comp_node, bool not_exists)
{
  INIT_SUCC(ret);
  if (index_ < expression_.length()) {
    // ret = 1
    if (!not_exists && (expression_[index_] == '>' || expression_[index_] == '<'
      || expression_[index_] == '=' || expression_[index_] == '!')) {
      if (OB_FAIL(get_char_comparison_type(filter_comp_node))) {
        LOG_WARN("fail to init char_comparison_type!", K(ret), K(index_), K(expression_));
      }
    } else {
      if (OB_FAIL(get_func_comparison_type(filter_comp_node))) {
        LOG_WARN("fail to init char_comparison_type!", K(ret), K(index_), K(expression_));
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("wrong path, comparison should have right args!", K(ret), K(index_), K(expression_));
  }
  return ret;
}

// jump_over_double_quote
// @return  the error code.
int ObJsonPath::jump_over_double_quote()
{
  INIT_SUCC(ret);
  int len = expression_.length();
  if (expression_[index_] == ObJsonPathItem::DOUBLE_QUOTE) {
    ++index_;
    while (index_ < len && expression_[index_] != ObJsonPathItem::DOUBLE_QUOTE) {
      if (expression_[index_] == '\\') {
        index_ += 2;
      } else {
        ++index_;
      }
    }
    if (index_ < len && expression_[index_] == ObJsonPathItem::DOUBLE_QUOTE) {
      ++index_;
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("there should be a '\"'!", K(ret), K(index_), K(expression_));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("there isn't '\"'!", K(ret), K(index_), K(expression_));
  }
  return ret;
}

// get_sub_path
// @return  the error code.
int ObJsonPath::jump_over_dot()
{
  INIT_SUCC(ret);
  int len = expression_.length();
  if (index_ < len) {
    // if '"' , jump
    if (expression_[index_] == ObJsonPathItem::DOUBLE_QUOTE) {
      if (OB_FAIL(jump_over_double_quote())) {
        LOG_WARN("Wrong path in double_quote!", K(ret), K(index_), K(expression_));
      }
    // without '"', could be .*/..
    // or else must be .keyname or .function_name
    } else if (expression_[index_] == ObJsonPathItem::WILDCARD) {
      ++index_;
    // if after .. is '[' the jump over the second '.'
    } else if (expression_[index_] == ObJsonPathItem::BEGIN_MEMBER) {
      if (index_ + 1 >= len) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("there end up with a '..'!", K(ret), K(index_), K(expression_));
      }
    } else {
      if (ObJsonPathUtil::letter_or_not(expression_[index_]) || expression_[index_] == '_') {
        while (index_ < len
              && (ObJsonPathUtil::letter_or_not(expression_[index_])
              || ObJsonPathUtil::is_digit(expression_[index_])
              || expression_[index_] == '_')) {
          ++index_;
        }
        ObJsonPathUtil::skip_whitespace(expression_, index_);
        if (index_ < len && expression_[index_] == ObJsonPathItem::BRACE_START) {
          ++index_;
          ObJsonPathUtil::skip_whitespace(expression_, index_);
          if (index_ < len && expression_[index_] == ObJsonPathItem::BRACE_END) {
            ++index_;
          } else {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("there should be a ')'!", K(ret), K(index_), K(expression_));
          }
        }
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("member name should be start with letter when without double_quote!", K(ret), K(index_), K(expression_));
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("out of range!", K(ret), K(index_), K(expression_));
  }
  return ret;
}

// get_sub_path
// @return  the error code.
// jump based on node type for two reason
// 1. for member，the keyname may be same with comp_node，eg：like
// 2. if subpath have grammer mistake
int ObJsonPath::get_sub_path(char*& sub_path, uint64_t& sub_len)
{
  INIT_SUCC(ret);
  uint64_t start = 0;
  uint64_t end = 0;

  int len = expression_.length();
  // make sure start with @
  if (index_ < len && expression_[index_] == ObJsonPathItem::SUB_PATH) {
    ++index_;
    start = index_;
    ObJsonPathUtil::skip_whitespace(expression_, index_);
    while (OB_SUCC(ret) && index_ < len && end == 0) {
      if (expression_[index_] == ObJsonPathItem::BEGIN_ARRAY) {
        while (index_ < len && expression_[index_] != ObJsonPathItem::END_ARRAY) {
          ++index_;
        }
        if (index_ < len && expression_[index_] == ObJsonPathItem::END_ARRAY) {
          ++index_;
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("there should be a ']'!", K(ret), K(index_), K(expression_));
          break;
        }
      // start with '.'
      } else if (expression_[index_] == ObJsonPathItem::BEGIN_MEMBER) {
        ++index_;
        ObJsonPathUtil::skip_whitespace(expression_, index_);
        if (OB_FAIL(jump_over_dot())) {
          LOG_WARN("wrong sub path!", K(ret), K(index_), K(expression_));
        }
      // filter
      } else if (expression_[index_] ==  ObJsonPathItem::FILTER_FLAG) {
        ++index_;
        ObJsonPathUtil::skip_whitespace(expression_, index_);
        if (index_ < len) {
          if (expression_[index_] == ObJsonPathItem::BRACE_START) {
            int brace = 1;
            ++index_;
            while (index_ < len && brace > 0 ) {
              if (expression_[index_] == ObJsonPathItem::DOUBLE_QUOTE) {
                if (OB_FAIL(jump_over_double_quote())) {
                  LOG_WARN("Wrong path in double_quote!", K(ret), K(index_), K(expression_));
                }
              } else if (expression_[index_] == ObJsonPathItem::BRACE_START) {
                ++brace;
                ++index_;
              } else if (expression_[index_] == ObJsonPathItem::BRACE_END) {
                --brace;
                ++index_;
              } else {
                ++index_;
              }
            }
            if (OB_FAIL(ret) || brace != 0) {
              LOG_WARN("Wrong filter expression!", K(ret), K(index_), K(expression_));
            }
          } else {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("Should have a '(' after '?'!", K(ret), K(index_), K(expression_));
          }
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("Shouldn't end up with '?'!", K(ret), K(index_), K(expression_));
        }
      } else if (expression_[index_] == ' ') {
        ObJsonPathUtil::skip_whitespace(expression_, index_);
      } else {
        if (index_ > start) {
          end = index_ - 1;
        } else {
          end = index_;
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObJsonPathUtil::skip_whitespace(expression_, index_);

      if (end < start || end >= len) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("didn't get the right endding of sub_path!", K(ret), K(index_), K(expression_), K(end), K(start));
      } else {
        char* start_ptr = expression_.ptr() + start;
        if (end == start) {
          sub_len = 1;
        } else {
          sub_len = end - start + 2;
        }
        sub_path = static_cast<char*> (allocator_->alloc(sub_len));
        if (OB_ISNULL(sub_path)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate memory for sub_path.",K(ret), K(len),K(start_ptr));
        } else {
          sub_path[0] = ObJsonPathItem::ROOT;
          if(end > start) {
            MEMCPY(sub_path + 1, start_ptr, sub_len - 1);
          }
        }
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("supposed to begin with '@'!", K(ret), K(index_), K(expression_));
  }
  return ret;
}

int ObJsonPath::get_var_name(char*& name, uint64_t& len)
{
  INIT_SUCC(ret);
  name = nullptr;
  len = 0;
  uint64_t start = 0;
  uint64_t end = 0;
  start = index_;
  if (index_ < expression_.length()
    && (ObJsonPathUtil::letter_or_not(expression_[index_]) || expression_[index_] == '_')) {
    ++index_;
    while (index_ < expression_.length() && end == 0) {
      if (ObJsonPathUtil::letter_or_not(expression_[index_])
         || ObJsonPathUtil::is_digit(expression_[index_])
         || expression_[index_] == '_') {
        ++index_;
      } else {
        end = index_ - 1;
      }
    }

    if (end < start || end >= expression_.length()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("didn't get the right end of SQL/JSON variable name!", K(ret), K(index_), K(expression_));
    } else {
      char* start_ptr = expression_.ptr() + start;
      len = end - start + 1;
      name = static_cast<char*> (allocator_->alloc(len));
      if (OB_ISNULL(name)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory for name.",K(ret), K(len),K(start_ptr));
      } else {
        MEMCPY(name, start_ptr, len);
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("wrong name of SQL/JSON variable!", K(ret), K(index_), K(expression_));
  }

  return ret;
}

// get_num_st
// @return  the error code.
int ObJsonPath::get_num_str(char*& num, uint64_t& len)
{
  INIT_SUCC(ret);
  uint64_t start = 0;
  uint64_t end = 0;
  start = index_;
  bool had_minus = false;
  bool had_point = false;
  if (index_ < expression_.length()) {
    if (expression_[index_] == ObJsonPathItem::MINUS) {
      had_minus = true;
      ++index_;
    } else if (expression_[index_] == '.') {
      had_point = true;
      ++index_;
    }
    bool had_digit = false;
    while (OB_SUCC(ret) && index_ < expression_.length() && end == 0) {
      if (ObJsonPathUtil::is_digit(expression_[index_])) {
        ++index_;
        had_digit = true;
      } else if (expression_[index_] == '.') {
        if (had_point == false) {
          ++index_;
          had_point = true;
        } else if (expression_[index_] == 'e'
                  && (index_ + 1 < expression_.length()
                  && (expression_[index_+1] == '+'
                  || ObJsonPathUtil::is_digit(expression_[index_ + 1])))){
          index_ += 2;
        } else {
          ret = OB_INVALID_ARGUMENT;
          bad_index_ = index_;
          LOG_WARN("shouldn't be two point in a number!", K(ret), K(index_), K(expression_));
        }
      } else {
        end = index_ - 1;
      }
    }

    if (OB_SUCC(ret) && index_ == expression_.length()) {
      end = index_ - 1;
    }
    if (OB_FAIL(ret) || had_digit == false || (end < start || end >= expression_.length())) {
      LOG_WARN("didn't get the right end of SQL/JSON variable name!", K(ret), K(index_), K(expression_));
    } else {
      char* start_ptr = expression_.ptr() + start;
      len = end - start + 1;
      num = static_cast<char*> (allocator_->alloc(len));
      if (OB_ISNULL(num)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory for sub_path.",K(ret), K(len),K(start_ptr));
      } else {
        MEMCPY(num, start_ptr, len);
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("wrong name of SQL/JSON variable!", K(ret), K(index_), K(expression_));
  }

  return ret;
}

int ObJsonPath::parse_comp_var(ObJsonPathFilterNode* filter_comp_node, bool left)
{
  INIT_SUCC(ret);
  char* val_name = nullptr;
  uint64_t val_len = 0;
  if (OB_FAIL(get_var_name(val_name, val_len))) {
    LOG_WARN("fail to get the name of SQL/JSON variable!", K(ret), K(index_), K(expression_));
  } else {
    if (OB_ISNULL(val_name)) {
      ret = OB_BAD_NULL_ERROR;
      LOG_WARN("didn't get name of SQL/JSON variable!", K(ret), K(index_), K(expression_));
    } else if (left) {
      filter_comp_node->init_left_var(val_name, val_len);
    } else {
      if (OB_FAIL(filter_comp_node->init_right_var(val_name, val_len))) {
        LOG_WARN("fail to init left_val",K(ret), K(val_name));
      }
    }
  }
  return ret;
}


int ObJsonPath::parse_comp_string_num(ObJsonPathFilterNode* filter_comp_node, bool left)
{
  INIT_SUCC(ret);
  if (expression_[index_] == ObJsonPathItem::DOUBLE_QUOTE) {
    char* str = nullptr;
    uint64_t name_len = 0;
    bool is_func = false;
    bool with_escape = false;
    if (OB_FAIL(get_origin_key_name(str, name_len, true, is_func, with_escape))) {
      LOG_WARN("fail to get string scalar",K(ret), K(index_), K(expression_),K(str));
    } else {
      if (OB_ISNULL(str)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("didn't get string scalar!", K(ret), K(index_), K(expression_));
      } else {
        if (left) {
          filter_comp_node->init_left_scalar(str, name_len);
        } else {
          if (OB_FAIL(filter_comp_node->init_right_scalar(str, name_len))) {
            LOG_WARN("fail to init left_val",K(ret), K(str));
          }
        }
      }
    }
  } else if (ObJsonPathUtil::is_digit(expression_[index_])
            ||(expression_[index_] == ObJsonPathItem::MINUS || expression_[index_] == '.')) {
    char* num = nullptr;
    uint64_t num_len = 0;
    if (OB_FAIL(get_num_str(num, num_len))) {
      LOG_WARN("fail to get string scalar",K(ret), K(index_), K(expression_),K(num));
    } else {
      if (OB_ISNULL(num)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("didn't get string scalar!", K(ret), K(index_), K(expression_));
      } else {
        if (left) {
          filter_comp_node->init_left_scalar(num, num_len);
        } else {
          if (OB_FAIL(filter_comp_node->init_right_scalar(num, num_len))) {
            LOG_WARN("fail to init left_val",K(ret), K(num));
          }
        }
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("supposed to be string or number!", K(ret), K(index_), K(expression_));
  }
  return ret;
}

// parse JPN_exists/!exists
// @return  the error code.
int ObJsonPath::parse_comp_exist(ObJsonPathFilterNode* filter_comp_node)
{
  INIT_SUCC(ret);
  // start whit ( , brace = 0；
  // '(' ++brace，')'--brace，jump over ""
  // brace = -1, then end;
  uint64_t len = expression_.length();
  if (index_ < len && expression_[index_] == ObJsonPathItem::BRACE_START) {
    ++index_;
    ObJsonPathUtil::skip_whitespace(expression_, index_);
    if (index_ < len && expression_[index_] == ObJsonPathItem::SUB_PATH) {
      ++index_;
      int64_t brace = 0;
      uint64_t start = index_;
      uint64_t end = 0;
      ObJsonPathUtil::skip_whitespace(expression_, index_);
      while (OB_SUCC(ret) && index_ < len && brace >= 0) {
        if (expression_[index_] == ObJsonPathItem::BRACE_START) {
          ++index_;
          ++brace;
        } else if (expression_[index_] == ObJsonPathItem::DOUBLE_QUOTE) {
          if (OB_FAIL(jump_over_double_quote())) {
            LOG_WARN("wrong path!", K(ret), K(index_), K(expression_));
          }
        } else if (expression_[index_] == ObJsonPathItem::BRACE_END) {
          --brace;
          if (brace < 0) end = index_ - 1;
          ++index_;
        } else {
          ++index_;
        }
      }

      if (OB_FAIL(ret) || end < start || end >= len) {
        LOG_WARN("didn't get the right endding of exist_subpath!", K(ret), K(index_), K(expression_));
      } else {
        char* start_ptr = expression_.ptr() + start;
        uint64_t sub_len = end - start + 2;
        char* sub_path = static_cast<char*> (allocator_->alloc(sub_len));
        if (OB_ISNULL(sub_path)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate memory for sub_path.",K(ret), K(len),K(start_ptr));
        } else {
          sub_path[0] = ObJsonPathItem::ROOT;
          MEMCPY(sub_path + 1, start_ptr, sub_len - 1);
        }
        ObString exist_subpath(sub_len, sub_path);
        ObJsonPath* spath = static_cast<ObJsonPath*> (allocator_->alloc(sizeof(ObJsonPath)));
        if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(spath)) {
          // error
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate row buffer failed at sub_path",K(ret), K(index_), K(expression_));
        } else {
          spath = new (spath) ObJsonPath(exist_subpath, allocator_);
          spath->set_subpath_arg(is_lax_);
          if (OB_FAIL(spath->parse_path())) {
            LOG_WARN("fail to parse sub_path",K(ret), K(spath->bad_index_), K(exist_subpath));
          } else {
            if (OB_FAIL(filter_comp_node->init_right_comp_path(spath))) {
              LOG_WARN("fail to init left_comp",K(ret), K(exist_subpath));
            }
          }
        }
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("there must be sub_path after exists/!exists.", K(ret), K(index_), K(expression_));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("there must be a ')' after exists/!exists.", K(ret), K(index_), K(expression_));
  }
  return ret;
}

// parse parse half arg of comparison
// @return  the error code.
int ObJsonPath::parse_comp_half(ObJsonPathFilterNode* filter_comp_node, bool left)
{
  INIT_SUCC(ret);
  if (!left && (filter_comp_node->get_node_type() == ObJsonPathNodeType::JPN_NOT_EXISTS
              || filter_comp_node->get_node_type() == ObJsonPathNodeType::JPN_EXISTS)) {
  // @
    if (OB_FAIL(parse_comp_exist(filter_comp_node))) {
      LOG_WARN("fail to get exist/!exist arg!", K(ret), K(index_), K(expression_));
    }
  } else if (expression_[index_] == ObJsonPathItem::SUB_PATH) {
    char* sub_path = nullptr;
    uint64_t path_len = 0;
    if (OB_FAIL(get_sub_path(sub_path, path_len))) {
      LOG_WARN("fail to get sub_path!", K(ret), K(index_), K(expression_));
    } else {
      ObString path_string(path_len, sub_path);
      ObJsonPath* spath = static_cast<ObJsonPath*> (allocator_->alloc(sizeof(ObJsonPath)));
      if (OB_ISNULL(spath)) {
        // error
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate row buffer failed at sub_path",K(ret), K(index_), K(expression_));
      } else {
        spath = new (spath) ObJsonPath(path_string, allocator_);
        spath->set_subpath_arg(is_lax_);
        if (OB_FAIL(spath->parse_path())) {
          LOG_WARN("fail to parse sub_path",K(ret), K(spath->bad_index_), K(path_string));
        } else {
          if (left) {
            if (OB_FAIL(filter_comp_node->init_left_comp_path(spath))) {
              LOG_WARN("fail to init left_comp",K(ret), K(path_string));
            }
          } else {
            if (OB_FAIL(filter_comp_node->init_right_comp_path(spath))) {
              LOG_WARN("fail to init left_comp",K(ret), K(path_string));
            }
          }
        }
      }
    }
  // sql_var, $
  } else if (expression_[index_] == ObJsonPathItem::ROOT) {
    ++index_;
    if (OB_FAIL(parse_comp_var(filter_comp_node, left))) {
      LOG_WARN("fail to parse SQL/JSON variable!", K(ret), K(index_), K(expression_));
    }
  // scalar
  } else if ((expression_[index_] == ObJsonPathItem::DOUBLE_QUOTE) || (ObJsonPathUtil::is_digit(expression_[index_]))
            ||(expression_[index_] == ObJsonPathItem::MINUS || expression_[index_] == ObJsonPathItem::BEGIN_MEMBER)) {
    if (OB_FAIL(parse_comp_string_num(filter_comp_node, left))) {
      LOG_WARN("fail to parse SQL/JSON variable!", K(ret), K(index_), K(expression_));
    }
  // true, false, null
  } else if (ObJsonPathUtil::letter_or_not(expression_[index_])) {
    if (expression_[index_] == 't' && expression_.length() - index_ >= 4) {
      ObString true_tmp(4, expression_.ptr()+index_);
      if (true_tmp.prefix_match("true")) {
        index_ += 4;
        filter_comp_node->init_bool_or_null(ObJsonPathNodeType::JPN_BOOL_TRUE, left);
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("unexpected charactor", K(ret), K(index_), K(expression_));
      }
    } else if (expression_[index_] == 'f' && expression_.length() - index_ >= 5) {
      ObString false_tmp(5, expression_.ptr()+index_);
      if (false_tmp.prefix_match("false")) {
        index_ += 5;
        filter_comp_node->init_bool_or_null(ObJsonPathNodeType::JPN_BOOL_FALSE, left);
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("unexpected charactor", K(ret), K(index_), K(expression_));
      }
    } else if (expression_[index_] == 'n' && expression_.length() - index_ >= 4) {
      ObString null_tmp(4, expression_.ptr()+index_);
      if (null_tmp.prefix_match("null")) {
        index_ += 4;
        filter_comp_node->init_bool_or_null(ObJsonPathNodeType::JPN_NULL, left);
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("unexpected charactor", K(ret), K(index_), K(expression_));
      }
    } else if (expression_[index_] != 'e' || !left) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("unexpected charactor", K(ret), K(index_), K(expression_));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("wrong comparison!", K(ret), K(index_), K(expression_));
  }
  return ret;
}

bool ObJsonPath::is_illegal_comp_for_func(const ObJsonPathNodeType last_path_node_type,
                                          const ObJsonPathNodeType scalar_type, const ObPathScalar scalar)
{
  bool ret_bool = false;
  switch (last_path_node_type) {
    case JPN_ABS:
    case JPN_CEILING:
    case JPN_DOUBLE:
    case JPN_FLOOR:
    case JPN_LENGTH:
    case JPN_NUMBER:
    case JPN_NUM_ONLY:
    case JPN_SIZE: {
      // can only be number
      if (scalar_type != JPN_SCALAR || OB_ISNULL(scalar.scalar_)
      || scalar.s_length_ <= 0 || scalar.scalar_[0] == '"'){
        ret_bool = true;
      }
      break;
    }
    case JPN_BOOLEAN:
    case JPN_BOOL_ONLY: {
      // can only be true or false
      if (scalar_type == JPN_BOOL_TRUE || scalar_type == JPN_BOOL_FALSE) {
        ret_bool = false;
      } else if (scalar_type != JPN_SCALAR || OB_ISNULL(scalar.scalar_)
      || scalar.s_length_ <= 0 || scalar.scalar_[0] != '"') {
        ret_bool = true;
      } else if ((scalar.s_length_ == strlen("\"true\"") && 0 == strncasecmp(scalar.scalar_, "\"true\"", strlen("\"true\"")))
                || (scalar.s_length_ == strlen("\"false\"") && 0 == strncasecmp(scalar.scalar_, "\"false\"", strlen("\"false\"")))) {
        ret_bool = false;
      } else {
        ret_bool = true;
      }
      break;
    }
    case JPN_DATE:
    case JPN_LOWER:
    case JPN_STRING:
    case JPN_STR_ONLY:
    case JPN_TIMESTAMP:
    case JPN_TYPE:
    case JPN_UPPER: {
      if (scalar_type != JPN_SCALAR || OB_ISNULL(scalar.scalar_)
      || scalar.s_length_ < 0 || scalar.scalar_[0] != '"') {
        ret_bool = true;
      }
      break;
    }
    default:{
      ret_bool = true;
      break;
    }
  }
  return ret_bool;
}

int ObJsonPath::is_legal_comparison(ObJsonPathFilterNode* filter_comp_node)
{
  INIT_SUCC(ret);

  ObJsonPath* sub_path = nullptr;
  ObPathScalar scalar;
  ObJsonPathNodeType scalar_type = JPN_ERROR;
  ObJsonPathNodeType right_type = filter_comp_node->node_content_.comp_.right_type_;
  ObCompContent comp_right = filter_comp_node->node_content_.comp_.comp_right_;
 if (filter_comp_node->get_node_type() == ObJsonPathNodeType::JPN_EXISTS
   || filter_comp_node->get_node_type() == ObJsonPathNodeType::JPN_NOT_EXISTS) {
    if (right_type != ObJsonPathNodeType::JPN_SUB_PATH) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("a sub_path must follow by exists!", K(ret));
    }
  } else {
    ObJsonPathNodeType left_type = filter_comp_node->node_content_.comp_.left_type_;
    ObCompContent comp_left = filter_comp_node->node_content_.comp_.comp_left_;
    switch (filter_comp_node->get_node_type()) {
      case ObJsonPathNodeType::JPN_LARGGER:
      case ObJsonPathNodeType::JPN_LARGGER_EQUAL:
      case ObJsonPathNodeType::JPN_SMALLER:
      case ObJsonPathNodeType::JPN_SMALLER_EQUAL:
      case ObJsonPathNodeType::JPN_EQUAL:
      case ObJsonPathNodeType::JPN_UNEQUAL: {
      // illegal(left, richt): (path，path），(scalar，var)，(var，scalar)，(var，var)
        if (left_type == ObJsonPathNodeType::JPN_SUB_PATH) {
          if (right_type  == ObJsonPathNodeType::JPN_SUB_PATH) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid comparison of two path expressions!", K(ret));
          } else if (right_type >= ObJsonPathNodeType::JPN_BOOL_TRUE && right_type <= ObJsonPathNodeType::JPN_SCALAR) {
            sub_path = comp_left.filter_path_;
            scalar_type = right_type;
            if (scalar_type == ObJsonPathNodeType::JPN_SCALAR) {
              scalar = comp_right.path_scalar_;
            }
          }
        } else if (left_type >= ObJsonPathNodeType::JPN_BOOL_TRUE && left_type <= ObJsonPathNodeType::JPN_SCALAR) {
          if (right_type  == ObJsonPathNodeType::JPN_SQL_VAR) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid comparison of two path expressions!", K(ret));
          } else if (right_type == ObJsonPathNodeType::JPN_SUB_PATH) {
            sub_path = comp_right.filter_path_;
            scalar_type = left_type;
            if (scalar_type == ObJsonPathNodeType::JPN_SCALAR) {
              scalar = comp_left.path_scalar_;
            }
          }
        } else if (left_type == ObJsonPathNodeType::JPN_SQL_VAR) {
          if (right_type != ObJsonPathNodeType::JPN_SUB_PATH) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid comparison of two path expressions!", K(ret));
          }
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid comparison of two path expressions!", K(ret), K(left_type));
        }
        break;
      }
      case JPN_SUBSTRING:
      case JPN_STARTS_WITH:
      case JPN_LIKE:
      case JPN_LIKE_REGEX:
      case JPN_EQ_REGEX: {
        // could be:(sub_path，string), (sub_path, var) ,(string, string)
        if (left_type == ObJsonPathNodeType::JPN_SUB_PATH) {
          ObJsonPathNodeType last_node_type  = comp_left.filter_path_->get_last_node_type();
          // check the result of sub_path is string
          if (JPN_BEGIN_FUNC_FLAG < last_node_type && last_node_type < JPN_STRING) {
            ret = OB_ERR_JSON_PATH_EXPRESSION_SYNTAX_ERROR;
            LOG_WARN("type incompatibility to compare.", K(ret));
          } else if (right_type  == ObJsonPathNodeType::JPN_SUB_PATH) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid comparison of two path expressions!", K(ret), K(index_));
          } else if (right_type  == ObJsonPathNodeType::JPN_SCALAR) {
            char* tmp = comp_right.path_scalar_.scalar_;
            if (!OB_ISNULL(tmp) && tmp[0] != '"') {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invalid comparison of two path expressions!", K(ret), K(tmp));
            }
          } else if (right_type != ObJsonPathNodeType::JPN_SQL_VAR) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid comparison of two path expressions!", K(ret));
          }
          if (OB_SUCC(ret) && right_type >= ObJsonPathNodeType::JPN_BOOL_TRUE && right_type  <= ObJsonPathNodeType::JPN_SCALAR) {
            sub_path = comp_left.filter_path_;
            scalar_type = right_type ;
            if (scalar_type == ObJsonPathNodeType::JPN_SCALAR) {
              scalar = comp_right.path_scalar_;
            }
          }
        } else if (left_type == right_type && right_type  == ObJsonPathNodeType::JPN_SCALAR) {
          char* tmpl = comp_left.path_scalar_.scalar_;
          char* tmpr = comp_right.path_scalar_.scalar_;
          if ((!OB_ISNULL(tmpl) && tmpl[0] != '"') || (!OB_ISNULL(tmpr) && tmpr[0] != '"')) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid comparison of two path expressions!", K(ret), K(tmpl),K(tmpr));
          }
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid comparison of two path expressions!", K(ret));
        }
        break;
      }
      default: {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN( "not compare type", K(ret), K(filter_comp_node->get_node_type()));
        break;
      }
    }// end switch
    if (OB_SUCC(ret) && OB_NOT_NULL(sub_path)
      && sub_path->get_last_node_type() > JPN_BEGIN_FUNC_FLAG && sub_path->get_last_node_type() < JPN_END_FUNC_FLAG
      && is_illegal_comp_for_func(sub_path->get_last_node_type(), scalar_type, scalar)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid comparison of two path expressions!", K(ret));
    }
  }
  return ret;
}

// parse parse_comparison
// @return  the error code.
int ObJsonPath::parse_comparison(ObFilterArrayPointers& filter_stack, bool not_exists)
{
  INIT_SUCC(ret);

  ObJsonPathFilterNode* filter_comp_node =
  static_cast<ObJsonPathFilterNode*> (allocator_->alloc(sizeof(ObJsonPathFilterNode)));
  if (OB_ISNULL(filter_comp_node)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate row buffer failed at filter_comp_node",
    K(ret), K(index_), K(expression_));
  } else {
    filter_comp_node = new (filter_comp_node) ObJsonPathFilterNode(allocator_);
  }

  if (OB_SUCC(ret)) {
    if (!not_exists) {
      // parse left_arg
      if (OB_FAIL(parse_comp_half(filter_comp_node, true))) {
        LOG_WARN("fail to parse the left part of comparission",K(ret), K(index_), K(expression_));
      }
    }

    if (OB_SUCC(ret)) {
      // cmp
      ObJsonPathUtil::skip_whitespace(expression_, index_);
      if (OB_FAIL(get_comparison_type(filter_comp_node, not_exists))) {
        LOG_WARN("fail to parse comparison_type",K(ret), K(index_), K(expression_));
      } else {
        // right
        ObJsonPathUtil::skip_whitespace(expression_, index_);
        if (OB_FAIL(parse_comp_half(filter_comp_node, false))) {
          LOG_WARN("fail to parse the right part of comparission",K(ret), K(index_), K(expression_));
        } else if (OB_FAIL(is_legal_comparison(filter_comp_node))) {
          // illegal
          LOG_WARN("illegal comparission",K(ret), K(index_), K(expression_));
        } else {
          if (OB_FAIL(filter_stack.push_back(filter_comp_node))) {
            LOG_WARN("fail to append node tu stack",K(ret));
          }
        }
      } // end of get comparison
    } // end of parse comparison and right_arg

  }
  return ret;
}

// parse_condition(&&, ||, !)
// @return  the error code.
int ObJsonPath::parse_condition(ObFilterArrayPointers& filter_stack, ObCharArrayPointers& char_stack, char in)
{
  INIT_SUCC(ret);
  uint64_t size_f = filter_stack.size();
  uint64_t size_c = char_stack.size();
  // >, pop
  while (OB_SUCC(ret) && size_c > 0 && size_f > 0) {
    char top = char_stack[size_c - 1];
    char pri = ObJsonPathUtil::priority(top, in);
    if (pri == '<') {
      if (OB_FAIL(char_stack.push_back(in))) {
        LOG_WARN("fail to push_back charactor", K(ret), K(index_), K(expression_), K(in));
      }
      if (OB_SUCC(ret) && in == '!') {
        if (OB_FAIL(char_stack.push_back('('))) {
          LOG_WARN("fail to push_back charactor", K(ret), K(index_), K(expression_), K("("));
        }
      }
      break;
    // == pop
    } else if (pri == '=') {
      if (OB_FAIL(ObJsonPathUtil::pop_char_stack(char_stack))) {
        LOG_WARN("fail to remove",K(ret), K(index_), K(expression_), K(char_stack[size_c - 1]));
      }
      break;
    } else if (pri == '>') {
      ObJsonPathFilterNode* filter_cond_node =
      static_cast<ObJsonPathFilterNode*> (allocator_->alloc(sizeof(ObJsonPathFilterNode)));
      if (OB_ISNULL(filter_cond_node)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate row buffer failed at filter_cond_node",
        K(ret), K(index_), K(expression_));
        break;
      } else {
        filter_cond_node = new (filter_cond_node) ObJsonPathFilterNode(allocator_);
        ObJsonPathFilterNode* right_comp = filter_stack[size_f - 1];
        ObJsonPathFilterNode* left_comp = nullptr;
        if (OB_FAIL(ObJsonPathUtil::pop_filter_stack(filter_stack))) {
          LOG_WARN("fail to remove",K(ret), K(index_), K(expression_));
        } else {
          size_f = filter_stack.size();
          // have left_arg or not
          if (top != '!') {
            if (size_f >= 1) {
              left_comp = filter_stack[size_f - 1];
              if (OB_FAIL(ObJsonPathUtil::pop_filter_stack(filter_stack))) {
                LOG_WARN("fail to remove",K(ret), K(index_), K(expression_));
              } else {
                size_f = filter_stack.size();
              }
            } else {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("filter stack is not supposed to be NULL!", K(top));
            }
          }

          if (OB_SUCC(ret)) {
            filter_cond_node->init_cond_left(left_comp);
            // cond init
            if (top == '&') {
              filter_cond_node->init_cond_type(ObJsonPathNodeType::JPN_AND_COND);
            } else if (top == '|') {
              filter_cond_node->init_cond_type(ObJsonPathNodeType::JPN_OR_COND);
            } else if (top == '!') {
              filter_cond_node->init_cond_type(ObJsonPathNodeType::JPN_NOT_COND);
            } else {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("wrong cond type!", K(top));
              break;
            }
            if (OB_SUCC(ret)) {
              if (OB_FAIL(ObJsonPathUtil::pop_char_stack(char_stack))) {
                LOG_WARN("fail to remove char_top!",K(ret), K(index_), K(expression_));
              } else {
                size_c = char_stack.size();
                if (OB_FAIL(filter_cond_node->init_cond_right(right_comp))) {
                  LOG_WARN("fail to init the right side of condition!", K(top));
                } else {
                // cond_node的参数正确初始化，加入filter_stack
                  if (OB_FAIL(filter_stack.push_back(filter_cond_node))) {
                    LOG_WARN("fail to append new filter_cond_node!", K(top));
                  } else {
                    size_f = filter_stack.size();
                  }
                }
              }
            }
          }
        }
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("wrong expression!", K(ret), K(index_), K(expression_));
    }
  }
  return ret;
}

// push char in char_stack
// @return  the error code.
int ObJsonPath::push_filter_char_in(char in, ObFilterArrayPointers& filter_stack, ObCharArrayPointers& char_stack)
{
  INIT_SUCC(ret);

  uint64_t size_c = char_stack.size();
  if (size_c <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty char stack", K(ret), K(index_), K(expression_));
  } else {
    char top = char_stack[size_c - 1];
    // legal
    switch (in) {
      case '(': {
        ++index_;
        // ()
        if (index_ < expression_.length()) {
          if (expression_[index_] == ')') {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("there's no content in the brace !", K(ret), K(index_), K(expression_));
          }
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN(" '(' shouldn't be the end of path expression!", K(ret), K(index_), K(expression_));
        }
        break;
      }
      case '!': {
        ++index_;
        ObJsonPathUtil::skip_whitespace(expression_, index_);
        if (index_ < expression_.length() && expression_[index_] == ObJsonPathItem::BRACE_START) {
          ++index_;
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("Not operator must be followed by parenthetical expression", K(ret), K(index_), K(expression_));
        }
        break;
      }
      case '&': {
        ++index_;
        if (index_ < expression_.length() && expression_[index_] == '&') {
          ++index_;
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("Should be '&&'", K(ret), K(index_), K(expression_));
        }
        break;
      }
      case '|': {
        ++index_;
        if (index_ < expression_.length() && expression_[index_] == '|') {
          ++index_;
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("Should be '||'", K(ret), K(index_), K(expression_));
        }
        break;
      }
      case ')': {
        ++index_;
        break;
      }
      default: {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("unexpected charactor!", K(ret), K(index_), K(expression_));
        break;
      }
    }

    // priority
    if (OB_SUCC(ret)) {
      char pri = ObJsonPathUtil::priority(top, in);
      if (pri == '<') {
        if (OB_FAIL(char_stack.push_back(in))) {
          LOG_WARN("fail to push_back charactor", K(ret), K(index_), K(expression_), K(in));
        }
        if (OB_SUCC(ret) && in == '!') {
          if (OB_FAIL(char_stack.push_back('('))) {
            LOG_WARN("fail to push_back charactor", K(ret), K(index_), K(expression_), K("("));
          }
        }

      } else if (pri == '>' || pri == '=') {
        if (OB_FAIL(parse_condition(filter_stack, char_stack, in))) {
          LOG_WARN("fail to parse condition expression!", K(ret), K(index_), K(expression_));
        }
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("wrong path!", K(ret), K(index_), K(expression_));
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("wrong path!", K(ret), K(index_), K(expression_));
    }
  }
  return ret;
}

// parse filter_node
// @return  the error code.
int ObJsonPath::parse_filter_node()
{
  INIT_SUCC(ret);
  ObJsonPathUtil::skip_whitespace(expression_, index_);
  ObFilterArrayPointers filter_stack;
  ObCharArrayPointers char_stack;

  if (index_ < expression_.length()) {
    if (expression_[index_] == ObJsonPathItem::BRACE_START) {
      if (OB_FAIL(char_stack.push_back('('))) {
        LOG_WARN("fail to push_back charactor", K(ret), K(index_), K(expression_));
      } else {
        ++index_;
        ObJsonPathUtil::skip_whitespace(expression_, index_);
        while (OB_SUCC(ret) && (index_ < expression_.length()) && char_stack.size() > 0) {
          // in filter node, between cond_node, must have comp_node
          // so, could be ：
          // 1. @
          // 2. $
          // 3. ""
          // 4. number(-/./1...)
          // 5. letter
          // 1 - 5 are both comp_node
          // 6. '&' '|' '!' '(' ')' are cond
          // beside: !exists is comparison
          if ((expression_[index_] == ObJsonPathItem::SUB_PATH || expression_[index_] == ObJsonPathItem::ROOT
              || expression_[index_] == ObJsonPathItem::DOUBLE_QUOTE)
              || (ObJsonPathUtil::is_digit(expression_[index_])
              || (expression_[index_] == ObJsonPathItem::MINUS || expression_[index_] == '.')
              || ObJsonPathUtil::letter_or_not(expression_[index_])) ) {
            if (OB_FAIL(parse_comparison(filter_stack, false))) {
              LOG_WARN("fail to parse comparison node", K(ret), K(index_), K(expression_));
            } else {
              ObJsonPathUtil::skip_whitespace(expression_, index_);
            }
          } else if (ObJsonPathUtil::is_end_of_comparission(expression_[index_])) {
            if (expression_[index_] == '!') {
              uint64_t tmp_idx = index_;
              ++index_;
              ObJsonPathUtil::skip_whitespace(expression_, index_);
              if (index_ < expression_.length()) {
                // is !exist
                if (expression_[index_] == 'e') {
                  // back to '!'
                  index_ = tmp_idx;
                  if (OB_FAIL(parse_comparison(filter_stack, true))) {
                    LOG_WARN("fail to parse comparison node", K(ret), K(index_), K(expression_));
                  } else {
                    ObJsonPathUtil::skip_whitespace(expression_, index_);
                  }
                } else {
                  // back to '!' and push
                  index_ = tmp_idx;
                  if (OB_FAIL(push_filter_char_in(expression_[index_], filter_stack, char_stack))) {
                    LOG_WARN("fail to parse comparison node", K(ret), K(index_), K(expression_));
                  } else {
                    ObJsonPathUtil::skip_whitespace(expression_, index_);
                  }
                }
              } else {
                ret = OB_INVALID_ARGUMENT;
                LOG_WARN("shouldn't end up with '!'.", K(ret), K(index_), K(expression_));
              }
            // && or ||, push directly
            } else {
              if (OB_FAIL(push_filter_char_in(expression_[index_], filter_stack, char_stack))) {
                LOG_WARN("fail to parse comparison node", K(ret), K(index_), K(expression_));
              } else {
                ObJsonPathUtil::skip_whitespace(expression_, index_);
              }
            }
          // not comp or cond
          } else {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("unexpected charactor", K(ret), K(index_), K(expression_));
          }
        }

        // end comp or cond
        if (OB_SUCC(ret)) {
          if (filter_stack.size() == 1 && char_stack.size() == 0) {
            if (OB_FAIL(append(filter_stack[0]))) {
              LOG_WARN("fail to append filter node", K(ret), K(index_), K(expression_));
            } else {
              // the first node after filter_node can't be array node OR another filter_node
              // illegal: ?()[idx]
              ObJsonPathUtil::skip_whitespace(expression_, index_);
              if (index_ < expression_.length()
                && (expression_[index_] == ObJsonPathItem::BEGIN_ARRAY
                || expression_[index_] == ObJsonPathItem::FILTER_FLAG)) {
                ret = OB_INVALID_ARGUMENT;
                LOG_WARN("array node after filter node", K(ret), K(index_), K(expression_));
              }
            }
          } else {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("wrong path expression", K(ret), K(index_), K(expression_));
          }
        }

      } // push (
    }
  } else {
    // out of range after '?'
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("wrong path expression!", K(ret), K(index_), K(expression_));
  }
  return ret;
}

} // namespace common
} // namespace oceanbase
