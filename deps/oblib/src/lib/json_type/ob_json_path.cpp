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

// This file contains implementation support for the JSON path abstraction.

#define USING_LOG_PREFIX SQL_RESV
#include "ob_json_path.h"
#include "ob_json_tree.h"
#include "ob_json_bin.h"
#include "ob_json_parse.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_array.h"
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


bool ObJsonArrayIndex::is_within_bounds() const
{
  return is_within_bounds_; 
}

uint64_t ObJsonArrayIndex::get_array_index() const
{ 
  return array_index_; 
}

ObJsonPathNodeType ObJsonPathNode::get_node_type() const
{
  return node_type_;
}

ObPathNodeContent ObJsonPathNode::get_node_content() const
{
  return node_content_;
}

ObJsonPathNode::~ObJsonPathNode()
{}

int ObJsonPathBasicNode::get_first_array_index(uint64_t array_length, ObJsonArrayIndex &array_index) const 
{
  INIT_SUCC(ret);
  switch (node_type_) {
    case JPN_ARRAY_RANGE:
      array_index = ObJsonArrayIndex(node_content_.array_range_.first_index_,
          node_content_.array_range_.is_first_index_from_end_, array_length);
      break;

    case JPN_ARRAY_CELL:  
      array_index = ObJsonArrayIndex(node_content_.array_cell_.index_,
          node_content_.array_cell_.is_index_from_end_, array_length);
      break;

    default: {
      ret = OB_ERR_INTERVAL_INVALID;
      LOG_WARN("wrong node type.", K(ret), K(node_type_));
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
      is_autowrap =  (range.array_begin_<range.array_end_);
      break;
    }

    default:
      break;
  }

  return is_autowrap;
}

// init function for **, [*], .*
int ObJsonPathBasicNode::init(ObJsonPathNodeType cur_node_type)
{
  INIT_SUCC(ret);
  switch (cur_node_type) {
    case JPN_MEMBER_WILDCARD:
    case JPN_ARRAY_CELL_WILDCARD:
    case JPN_ELLIPSIS: {
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
  return ret;
}

ObJsonPathBasicNode::ObJsonPathBasicNode(ObString &keyname)
{
  node_content_.member_.object_name_ = keyname.ptr();
  node_content_.member_.len_ = keyname.length();
  node_type_ = JPN_MEMBER;
}

ObJsonPathBasicNode::ObJsonPathBasicNode(const char* name, uint64_t len)
{
  node_content_.member_.object_name_ = name;
  node_content_.member_.len_ = len;
  node_type_ = JPN_MEMBER;
}

ObJsonPathBasicNode::ObJsonPathBasicNode(uint64_t idx)
{
  node_content_.array_cell_.index_ = idx;
  node_content_.array_cell_.is_index_from_end_ = false;
  node_type_ = JPN_ARRAY_CELL;
}

ObJsonPathBasicNode::ObJsonPathBasicNode(uint64_t idx, bool is_from_end)
{
  node_content_.array_cell_.index_ = idx;
  node_content_.array_cell_.is_index_from_end_ = is_from_end;
  node_type_ = JPN_ARRAY_CELL;
}

ObJsonPathBasicNode::ObJsonPathBasicNode(uint64_t first_idx, uint64_t last_idx)
{
  node_content_.array_range_.first_index_ = first_idx;
  node_content_.array_range_.is_first_index_from_end_ = false;
  node_content_.array_range_.last_index_ = last_idx;
  node_content_.array_range_.is_last_index_from_end_ = false;
  node_type_ = JPN_ARRAY_RANGE;
}

ObJsonPathBasicNode::ObJsonPathBasicNode(uint64_t first_idx, bool is_first_from_end,
                                         uint64_t last_idx, bool is_last_from_end)
{
  node_content_.array_range_.first_index_ = first_idx;
  node_content_.array_range_.is_first_index_from_end_ = is_first_from_end;
  node_content_.array_range_.last_index_ = last_idx;
  node_content_.array_range_.is_last_index_from_end_ = is_last_from_end;
  node_type_ = JPN_ARRAY_RANGE;
}

ObPathMember ObJsonPathBasicNode::get_object() const
{
  return node_content_.member_;
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

  if (heap_expr_.append(path) == OB_SUCCESS) {
    expression_.assign_ptr(heap_expr_.ptr(), heap_expr_.length());
  } else {
    expression_.assign_ptr(path.ptr(), path.length());
    use_heap_expr_ = 0;
  }
}

ObJsonPath::~ObJsonPath()
{}

int ObJsonPath::path_node_cnt()
{
  return path_nodes_.size();  
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
  ret_bool = std::any_of(path_nodes_.begin(), path_nodes_.end(), 
                         [](const ObJsonPathNode *node) -> bool {
    switch (node->get_node_type()) {
      case JPN_MEMBER_WILDCARD:
      case JPN_ARRAY_CELL_WILDCARD:
      case JPN_ELLIPSIS:
      case JPN_ARRAY_RANGE:
        return true;
      default:
        return false;
    }
  });

  return ret_bool;
}

bool ObJsonPath::is_contained_wildcard_or_ellipsis() const
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

ObJsonPathBasicNode* ObJsonPath::last_path_node() {
  return path_node(path_nodes_.size() - 1);
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

int ObJsonPathCache::find_and_add_cache(ObJsonPath*& res_path, ObString& path_str, int arg_idx)
{
  INIT_SUCC(ret);
  if (!is_match(path_str, arg_idx)) {
    void* buf = allocator_->alloc(sizeof(ObJsonPath));
    if (OB_NOT_NULL(buf)) {
      ObJsonPath* path = new (buf) ObJsonPath(path_str, allocator_);
      if (OB_FAIL(path->parse_path())) {
        ret = OB_ERR_INVALID_JSON_PATH;
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

ObPathParseStat ObJsonPathCache::path_stat_at(size_t idx) {
  ObPathParseStat stat = UNINITIALIZED;
  if (idx < stat_arr_.size()) {
    stat = stat_arr_[idx].state_;
  }
  return stat;
}

ObJsonPath* ObJsonPathCache::path_at(size_t idx)
{
  ObJsonPath *ptr = NULL;
  if (idx < path_arr_ptr_.size()) {
    ptr = path_arr_ptr_[idx];
  }
  return ptr;
}

void ObJsonPathCache::reset() {
  stat_arr_.clear();
  path_arr_ptr_.clear();
}

size_t ObJsonPathCache::size() {
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
  if (allocator != NULL && (size() == 0 || allocator == allocator_)) {
    allocator_ = allocator;
  }
}

common::ObIAllocator* ObJsonPathCache::get_allocator()
{
  return allocator_;
}

ObJsonPathCache::ObJsonPathCache(common::ObIAllocator *allocator)
{
  allocator_ = allocator;
}
 
int ObJsonPathUtil::append_array_index(uint64_t index, bool from_end, ObJsonBuffer& str)
{
  INIT_SUCC(ret);

  if (from_end) {
    if (OB_FAIL(str.append("last"))) {
      LOG_WARN("fail to append the 'last' ", K(ret));
    } else {
      // if index > 0, it should have '-' after 'last' 
      // such as: $[last-3 to last-1]
      if (index>0) {
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
      char* ptr = NULL;
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

int ObJsonPathBasicNode::node_to_string(ObJsonBuffer& str)
{
  INIT_SUCC(ret);
  switch (node_type_) {
    // ** , do append
    case JPN_ELLIPSIS:
      if (OB_FAIL(str.append("**"))) {
        LOG_WARN("fail to append JPN_ELLIPSIS", K(ret));
      }
      break;

    // .* , do append
    case JPN_MEMBER_WILDCARD:
      if (OB_FAIL(str.append(".*"))) {
        LOG_WARN("fail to append JPN_MEMBER_WILDCARD", K(ret));
      }
      break;

    // [*] , do append
    case JPN_ARRAY_CELL_WILDCARD:
      if (OB_FAIL(str.append("[*]"))) {
        LOG_WARN("fail to append JPN_ARRAY_CELL_WILDCARD", K(ret));
      }
      break;

    // MEMBER, add .name
    // need to check is_ecmascript_identifier
    // if check is_ecmascript_identifier false, should do double_quote 
    case JPN_MEMBER:
      if (OB_FAIL(str.append("."))) {
        LOG_WARN("fail to append BEGIN_MEMBER");
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
      break;
    // JPN_ARRAY_CELL, add [index]. If from_end is true, add last
    case JPN_ARRAY_CELL: {
      if (OB_FAIL(str.append("["))) {
        LOG_WARN("fail to append BEGIN_ARRAY", K(ret));
      } else if (OB_FAIL(ObJsonPathUtil::append_array_index(
        node_content_.array_cell_.index_, node_content_.array_cell_.is_index_from_end_, str))) {
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
          node_content_.array_range_.is_first_index_from_end_, str))) {
        LOG_WARN("fail to append ARRAY_INDEX(from).", K(ret));
      } else if (OB_FAIL(str.append(" to "))) {
        LOG_WARN("fail to append 'to'.", K(ret));
      } else if (OB_FAIL(ObJsonPathUtil::append_array_index(node_content_.array_range_.last_index_, 
          node_content_.array_range_.is_last_index_from_end_, str))) {
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

int ObJsonPath::to_string(ObJsonBuffer& str)
{
  INIT_SUCC(ret);
  if (OB_FAIL(str.append("$"))) {
    LOG_WARN("fail to append '$'", K(ret));
  }

  for (int i = 0; OB_SUCC(ret) && i < path_nodes_.size() ; ++i) {
    if (OB_FAIL(path_nodes_[i]->node_to_string(str))) {
      LOG_WARN("parse node to string failed.", K(i));
    }
  }

  return ret;
}


// parse path to path Node
int ObJsonPath::parse_path()
{
  INIT_SUCC(ret);
  bad_index_ = -1;

  // the first non-whitespace character must be $
  int len = expression_.length();
  bool first = true;
  ObJsonPathUtil::skip_whitespace(expression_, index_);

  if (index_>=len || (expression_[index_] != ObJsonPathItem::ROOT)) {
    ret = OB_INVALID_ARGUMENT;
    bad_index_ = 1;
    LOG_WARN("An path expression begins with a dollar sign ($)", K(ret));
  } else {
    ++index_; 
    ObJsonPathUtil::skip_whitespace(expression_, index_);

    while (index_<len && OB_SUCC(ret)) {
      if (OB_FAIL(parse_path_node())) {
        bad_index_ =  index_; 
        LOG_WARN("fail to parse JSON Path Expression!", K(ret), K(index_));
      } else {
        ObJsonPathUtil::skip_whitespace(expression_, index_);
      }
    } // end while
  }

  if (OB_SUCC(ret) && path_node_cnt() > 0 && 
      path_nodes_[path_node_cnt()-1]->get_node_type() == JPN_ELLIPSIS) {
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

// parse path to path json node
int ObJsonPath::parse_path_node()
{
  int ret =  OB_SUCCESS;
  ObJsonPathUtil::skip_whitespace(expression_, index_);

  if (index_ >= expression_.length()) {
    ret = OB_ARRAY_OUT_OF_RANGE;
    LOG_WARN("wrong path expression", K(ret), K(index_));
  } else {
    // JPN_ARRAY_CELL,JPN_ARRAY_RANGE,JPN_ARRAY_CELL_WILDCARD begin with '['
    // JPN_MEMBER,JPN_MEMBER_WILDCARD begin with '.'
    // JPN_ELLIPSIS begin with '*'
    switch (expression_[index_]) {
      case ObJsonPathItem::BEGIN_ARRAY:
        if (OB_FAIL(parse_array_node())) {
          LOG_WARN("fail to parse array node", K(ret), K(index_));
        }
        break;

      case ObJsonPathItem::BEGIN_MEMBER:
        if (OB_FAIL(parse_member_node())) {
          LOG_WARN("fail to parse member node", K(ret), K(index_));
        }
        break;

      case ObJsonPathItem::WILDCARD:
        if (OB_FAIL(parse_ellipsis_node())) {
          LOG_WARN("fail to parse ellipsis node", K(ret), K(index_));
        }
        break;

      default:
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("wrong path expression, should be '[', '.' or '*'.",
        K(ret), K(index_), K(expression_));
        break;
    }
  }
  
  return ret;
}

// parse **(JPN_ELLIPSIS)
// @return  the error code.
int ObJsonPath::parse_ellipsis_node()
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
      ellipsis_node = new (ellipsis_node) ObJsonPathBasicNode();
      if (OB_FAIL(ellipsis_node->init(JPN_ELLIPSIS))) {
        LOG_WARN("fail to init path basic node with type", K(ret), K(JPN_ELLIPSIS));
      } else if (OB_FAIL(append(ellipsis_node))) {
        LOG_WARN("fail to append JsonPathNode(JPN_ELLIPSIS)!", K(ret), K(index_), K(expression_));
      } else {
        is_contained_wildcard_or_ellipsis_ = true;
      }
    }
  }
  return ret;
}

// get array index, range:
// start from expression[index_] until non-digit char
// parse str to int32_t, return index
int ObJsonPathUtil::get_index_num(const ObString& expression, uint64_t& idx, uint64_t& array_idx)
{
  INIT_SUCC(ret);
  array_idx = 0;
  ObJsonPathUtil::skip_whitespace(expression, idx);
  uint64_t start = 0, end = 0;

  if (idx < expression.length()) {
    if (isdigit(expression[idx])) {
      start = idx;
      ++idx;
      while (idx<expression.length() && isdigit(expression[idx])) {
        ++idx;
      }
      // Here should have ']' after digits, return error when reaches the end
      if (idx >= expression.length()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("digit shouldn't be the end of the expression.", K(ret), K(idx));
      } else {
        end = idx-1;
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
      array_idx = ObCharset::strntoull(ptr, end-start+1, 10, &err);
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
int ObJsonPath::parse_array_index(uint64_t& array_index, bool& from_end)
{
  INIT_SUCC(ret);
  ObJsonPathUtil::skip_whitespace(expression_, index_);
  int last_len = strlen("last");
  // check has 'last'
  if (expression_.length() - index_ >= last_len - 1 && expression_[index_] == 'l') {
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

  // Here will be three situation:
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
// three situation:
// '[*]' : just build ObJsonPathNode with type JPN_ARRAY_CELL_WILDCARD, and append to JsonPath
// otherwise call parse_array_index() to do parse
// if type is CELL, process one arg
// if type is RANGE, process two arg between 'to'
int ObJsonPath::parse_array_node()
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

  if (OB_SUCC(ret)) {
    if (index_ < expression_.length()) {
      // if '[*]'
      if (expression_[index_] == ObJsonPathItem::WILDCARD) {
        ++index_;
        ObJsonPathUtil::skip_whitespace(expression_, index_);
        if (index_ < expression_.length() && expression_[index_] == ObJsonPathItem::END_ARRAY) {
          ++index_;
          ObJsonPathBasicNode* cell_wildcard_node = 
          static_cast<ObJsonPathBasicNode*> (allocator_->alloc(sizeof(ObJsonPathBasicNode)));
          if (OB_ISNULL(cell_wildcard_node)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("allocate row buffer failed at cell_wildcard_node", K(ret), K(expression_));
          } else {
            cell_wildcard_node = new (cell_wildcard_node) ObJsonPathBasicNode();
            if (OB_FAIL(cell_wildcard_node->init(JPN_ARRAY_CELL_WILDCARD))) {
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
      } else { // range or cell
        // process CELL or RANGE
        // parser first arg
        // 1. init var, use is_cell_type to mark if CELL or RANGE
        bool is_cell_type = true;
        uint64_t index1 = 0, index2 = 0;
        bool from_end1 = false, from_end2 = false;

        // 2. parse first arg
        if (OB_FAIL(parse_array_index(index1, from_end1))) {
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
              if (OB_FAIL(parse_array_index(index2, from_end2))) {
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
              if (OB_FAIL(add_array_node(is_cell_type, 
                  index1, index2, from_end1, from_end2))) {
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
      }// end ofrange or cell    
    } else {
      // out of range after '['
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("wrong path expression!", K(ret), K(index_), K(expression_));
    }
  }
  
  return ret;
}

int ObJsonPath::add_array_node(bool is_cell_type, uint64_t& index1, uint64_t& index2, 
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
      cell_node = new (cell_node) ObJsonPathBasicNode(index1, from_end1);
      if (OB_FAIL(append(cell_node))) {
        LOG_WARN("fail to append JsonPathNode(cell_node)!", 
        K(ret) , K(index_), K(expression_));
      }
    }
  } else {
    // 6.2 ARRAY_RANGE, check index if valid (mysql)
    if (from_end1 == from_end2 && 
       ((from_end1 && index1 < index2) || (!from_end1 && index2 < index1))) {
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
        range_node = new (range_node) ObJsonPathBasicNode(index1, from_end1, index2, from_end2);
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
  const char *syntaxerr = NULL;
  uint64_t *offset = NULL;
  ObJsonNode* dom = NULL;

  if (OB_FAIL(ObJsonParser::parse_json_text(allocator_, str, len, syntaxerr, offset, dom))) {
    LOG_WARN("fail to parse_name_with_rapidjson.", K(ret), K(str), K(syntaxerr), K(offset));
  } else if (dom->json_type() != ObJsonNodeType::J_STRING) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected json type.", K(ret), K(str), K(dom->json_type()));
  } else {
    ObJsonString *val = static_cast<ObJsonString *>(dom);
    len = val->value().length();
    str = static_cast<char*> (allocator_->alloc(len));
    if (OB_ISNULL(str)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory for member_name.",
        K(ret), K(len), K(val->value()));
    } else {
      MEMCPY(str, val->value().ptr(), len);
    }
  }
  
  return ret;
}

// if keyname without double quote, end with ' ', '.', '[', '*'
bool ObJsonPathUtil::is_terminator(char ch)
{
  bool ret_bool = false;
  switch (ch) {
  case ' ':
    ret_bool = true;
    break;

  case '.':
    ret_bool = true;
    break;

  case '[':
    ret_bool = true;
    break;

  case '*':
    ret_bool = true;
    break;

  default:
    break;
  }

  return ret_bool;
}

// process JPN_MEMBER get keyname
// @param[in,out] name  Keyname
// @param[in] is_quoted 
// @return  the error code.
int ObJsonPath::get_origin_key_name(char*& str, uint64_t& length, bool is_quoted)
{
  INIT_SUCC(ret);
  uint64_t start = 0, end = 0;

  int len = expression_.length();

  if (index_<len) {
    if (is_quoted) {
      // with quote, check quote
      if (expression_[index_] == ObJsonPathItem::DOUBLE_QUOTE) {
        start = index_;
        ++index_;

        while (index_<len) {
          if (expression_[index_] == '\\') {
            index_ += 2;
            continue;
          }

          if (expression_[index_] == ObJsonPathItem::DOUBLE_QUOTE) {
            end = index_;
            ++index_;
            break;
          }
          ++index_;
        }

        if (end == 0 && index_==len) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("should end with DOUBLE_QUOTE!", K(ret), K(index_), K(expression_));
        }
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("should start with DOUBLE_QUOTE!", K(ret), K(index_), K(expression_));
      }
    } else {
      start = index_;
      while (index_ < len) {
        if (ObJsonPathUtil::is_terminator(expression_[index_])) {
          end = index_-1;
          break;
        } else {
          ++index_;
        }
      }
      if (index_ == len) {
        end = index_-1;
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
      len = end-start+1;
      char* start_ptr = expression_.ptr() + start;
      if (!is_quoted) {
        length = len+2;
        str = static_cast<char*> (allocator_->alloc(length));
        if (OB_ISNULL(str)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate memory for member_name.",
              K(ret), K(len),K(start_ptr));
        } else {
          str[0] = ObJsonPathItem::DOUBLE_QUOTE;
          MEMCPY(str+1, start_ptr, len);
          str[len+1] = ObJsonPathItem::DOUBLE_QUOTE;
        }
      } else {
        length = len;
        str = static_cast<char*> (allocator_->alloc(length));
        if (OB_ISNULL(str)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to allocate memory for member_name.",
                K(ret), K(len),K(start_ptr));
        } else {
          MEMCPY(str, start_ptr, len);
        }
      }
    }
  }

  return ret;
}

// parse **(JPN_MEMBER/JPN_MEMBER_WILDCARD)
// @return  the error code.
int ObJsonPath::parse_member_node()
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
        ++index_;
        ObJsonPathBasicNode* member_wildcard_node = 
        static_cast<ObJsonPathBasicNode*> (allocator_->alloc(sizeof(ObJsonPathBasicNode)));

        if (OB_ISNULL(member_wildcard_node)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate row buffer failed at member_wildcard_node",
          K(ret), K(index_), K(expression_));
        } else {
          member_wildcard_node = new (member_wildcard_node) ObJsonPathBasicNode();
          if (OB_FAIL(member_wildcard_node->init(JPN_MEMBER_WILDCARD))) {
            LOG_WARN("fail to PathBasicNode init with type", K(ret), K(JPN_MEMBER_WILDCARD));
          } else if (OB_FAIL(append(member_wildcard_node))) {
            LOG_WARN("fail to append JsonPathNode(member_wildcard_node)!", K(ret), K(index_), K(expression_));
          } else {
            is_contained_wildcard_or_ellipsis_ = true;
          }
        }
      } else {
        bool is_quoted  = false;

        // check double quote
        if (expression_[index_] == ObJsonPathItem::DOUBLE_QUOTE) is_quoted = true;

        char* name = NULL;
        uint64_t name_len = 0;
        // get name 
        // add double quote for rapidjson requires
        if (OB_FAIL(get_origin_key_name(name, name_len, is_quoted))) {
           LOG_WARN("fail to get keyname!", K(ret), K(index_), K(expression_));
        } else {
          if (OB_FAIL(parse_name_with_rapidjson(name, name_len))) {
            LOG_WARN("fail to parse name with rapidjson",
            K(ret), K(index_), K(expression_),K(name));
          }

          if (OB_SUCC(ret) && !is_quoted) {
            if (!ObJsonPathUtil::is_ecmascript_identifier(name, name_len)) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("the key name isn't ECMAScript identifier!",
                K(ret), K(name));
            }
          }

          if (OB_SUCC(ret)) {
            ObJsonPathBasicNode* member_node = 
            static_cast<ObJsonPathBasicNode*> (allocator_->alloc(sizeof(ObJsonPathBasicNode)));
            if (OB_ISNULL(member_node)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("allocate row buffer failed at member_node", K(ret), K(index_), K(expression_));
            } else {
              member_node = new (member_node) ObJsonPathBasicNode(name, name_len);
              if (OB_FAIL(append(member_node) ))
              {
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
      LOG_WARN("in parse_member_node(), should start with '.' ", K(ret), K(index_), K(expression_));
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("idx out of range!", K(ret), K(index_), K(expression_));
    }
  }

  return ret;
}

bool ObJsonPathUtil::is_whitespace(char ch)
{
  return (ch == ' ');
}

void ObJsonPathUtil::skip_whitespace(const ObString &path, uint64_t& idx)
{
  while (idx<path.length() && ObJsonPathUtil::is_whitespace(path[idx])) {
    idx++;
  }
}

static constexpr unsigned UNICODE_COMBINING_MARK_MIN = 0x300;
static constexpr unsigned UNICODE_COMBINING_MARK_MAX = 0x36F;
static constexpr unsigned UNICODE_EXTEND_MARK_MIN = 0x2E80;
static constexpr unsigned UNICODE_EXTEND_MARK_MAX = 0x9fff;

static inline bool unicode_combining_mark(unsigned codepoint) {
  return ((UNICODE_COMBINING_MARK_MIN <= codepoint) && (codepoint <= UNICODE_COMBINING_MARK_MAX));
}

static bool is_utf8_unicode_charator(const char* ori, uint64_t& start, int64_t len)
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

static bool is_letter(unsigned codepoint, const char* ori, uint64_t start, uint64_t end) {
  bool ret_bool = true;
  if (unicode_combining_mark(codepoint)) {
    ret_bool = false;
  }
  ret_bool = isalpha(codepoint);
  if (ret_bool) {
  } else if (!ret_bool && 
      (codepoint >= UNICODE_EXTEND_MARK_MIN &&
       codepoint <= UNICODE_EXTEND_MARK_MAX && 
       is_utf8_unicode_charator(ori, start, end - start))) {
    ret_bool = true;
  }
  return ret_bool;
}

static bool is_connector_punctuation(unsigned codepoint) {
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
    }
    default: {
      ret_bool = false;
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
      LOG_WARN("fail to decode.", 
          K(ret_bool), K(codepoint), K(input_stream.Tell()), K(name));
      break;
    }

    // a unicode letter
    uint64_t curr_pos = input_stream.Tell();
    if (is_letter(codepoint, name, last_pos, curr_pos - last_pos)) continue;
    // $ is ok
    if (codepoint == 0x24) continue;
    // _ is ok
    if (codepoint == 0x5F) continue;

    /*
      the first character must be one of the above.
      more possibilities are available for subsequent characters.
    */

    if (first_codepoint) {
      ret_bool = false;
      LOG_WARN("first character must be $, _ or letter.", 
          K(ret_bool), K(codepoint), K(input_stream.Tell()), K(name));
      break;
    } else {
      // unicode combining mark
      if (unicode_combining_mark(codepoint)) continue;

      // a unicode digit
      if (isdigit(codepoint)) continue;
      if (is_connector_punctuation(codepoint)) continue;
      // <ZWNJ>
      if (codepoint == 0x200C) continue;
      // <ZWJ>
      if (codepoint == 0x200D) continue;
    }

    // nope
    ret_bool = false;
    LOG_WARN("not ecmascript identifier.",
         K(ret_bool), K(codepoint), K(input_stream.Tell()), K(name));
    break;
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
  if (OB_ISNULL(tmp_name) || OB_ISNULL(name.ptr())) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("input tmp_name is null", K(ret));
  } else if (OB_FAIL(ObJsonBaseUtil::add_double_quote(*tmp_name, name.ptr(), name.length()))) {
    LOG_WARN("fail to add_double_quote.", K(ret));
  }

  return ret;
}

bool ObJsonPathUtil::path_gives_duplicates(const JsonPathIterator &begin,
                                           const JsonPathIterator &end,
                                           bool auto_wrap)
{
  bool ret_bool = true;
  JsonPathIterator it = begin;
  for (; it != end; it++) {
    if ((*it)->get_node_type() == JPN_ELLIPSIS) {
      break;
    }
  }

  // If no ellipsis, no duplicates.
  if (it == end) {
    ret_bool = false;
  } else {
  // Otherwise, possibly duplicates if ellipsis or autowrap leg follows.
    for (it = it + 1; it != end; it++) {
      if ((*it)->get_node_type() == JPN_ELLIPSIS || (auto_wrap && (*it)->is_autowrap())) {
        ret_bool = true;
        break;
      }
    }
  }

  return ret_bool;
}

} // namespace common
} // namespace oceanbase
