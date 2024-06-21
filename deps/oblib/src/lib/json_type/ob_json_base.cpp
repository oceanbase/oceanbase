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
 * This file contains implementation support for the json base abstraction.
 */

#define USING_LOG_PREFIX SQL

#include "ob_json_base.h"
#include "ob_json_tree.h"
#include "ob_json_bin.h"
#include "ob_json_parse.h"
#include "lib/encode/ob_base64_encode.h" // for ObBase64Encoder
#include "lib/utility/ob_fast_convert.h" // ObFastFormatInt::format_unsigned
#include "lib/charset/ob_dtoa.h" // ob_gcvt_opt
#include "rpc/obmysql/ob_mysql_global.h" // DOUBLE_TO_STRING_CONVERSION_BUFFER_SIZE
#include "lib/charset/ob_charset.h" // for strntod
#include "common/ob_smart_var.h" // for SMART_VAR

namespace oceanbase {
namespace common {

struct ObJsonBaseCmp {
  bool operator()(const ObIJsonBase *a, const ObIJsonBase *b) {
    bool is_less = false;
    if (a->is_tree() && b->is_tree()) {
      is_less = (a < b);
    } else if (a->is_bin() && b->is_bin()) {
      is_less = (a->get_data() < b->get_data());
    } else {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "unexpected type", K(OB_ERR_UNEXPECTED), K(*a), K(*b));
    }
    return is_less;
  }
};

struct ObJsonBaseUnique {
  bool operator()(const ObIJsonBase *a, const ObIJsonBase *b) {
    bool is_eq = false;
    if (a->is_tree() && b->is_tree()) {
      is_eq = (a == b);
    } else if (a->is_bin() && b->is_bin()) {
      is_eq = (a->get_data() == b->get_data());
    }
    return is_eq;
  }
};

bool ObIJsonBase::is_json_number(ObJsonNodeType json_type) const
{
  bool ret_bool = false;
  switch (json_type) {
    case ObJsonNodeType::J_DECIMAL:
    case ObJsonNodeType::J_INT:
    case ObJsonNodeType::J_UINT:
    case ObJsonNodeType::J_DOUBLE:
    case ObJsonNodeType::J_OFLOAT:
    case ObJsonNodeType::J_ODOUBLE:
    case ObJsonNodeType::J_ODECIMAL:
    case ObJsonNodeType::J_OINT:
    case ObJsonNodeType::J_OLONG: {
      ret_bool = true;
      break;
    }
    default:{
      ret_bool = false;
    }
  }
  return ret_bool;
}

bool ObIJsonBase::is_json_scalar(ObJsonNodeType json_type) const
{
  bool ret_bool = false;
  if (json_type < ObJsonNodeType::J_MAX_TYPE
      && (json_type != ObJsonNodeType::J_OBJECT && json_type != ObJsonNodeType::J_ARRAY)) {
    ret_bool = true;
  }
  return ret_bool;
}

bool ObIJsonBase::is_json_string(ObJsonNodeType json_type) const
{
  bool ret_bool = false;
  switch (json_type) {
    case ObJsonNodeType::J_STRING:
    case ObJsonNodeType::J_OBINARY:
    case ObJsonNodeType::J_OOID:
    case ObJsonNodeType::J_ORAWHEX:
    case ObJsonNodeType::J_ORAWID:
    case ObJsonNodeType::J_ODAYSECOND:
    case ObJsonNodeType::J_OYEARMONTH: {
      ret_bool = true;
      break;
    }
    default: {
      ret_bool = false;
    }
  }
  return ret_bool;
}

bool ObIJsonBase::is_json_date(ObJsonNodeType json_type) const
{
  bool ret_bool = false;
  switch (json_type) {
    case ObJsonNodeType::J_DATE:
    case ObJsonNodeType::J_DATETIME:
    case ObJsonNodeType::J_TIMESTAMP:
    case ObJsonNodeType::J_ORACLEDATE:
    case ObJsonNodeType::J_OTIMESTAMP:
    case ObJsonNodeType::J_ODATE: {
      ret_bool = true;
      break;
    }
    default: {
      ret_bool = false;
    }
  }
  return ret_bool;
}

// apend node to unique vector judge duplicate
int ObJsonSortedResult::insert_unique(ObIJsonBase* node)
{
  INIT_SUCC(ret);
  ObJsonBaseCmp cmp;
  ObJsonBaseUnique unique;
  ObJsonBaseSortedVector::iterator pos = sort_vector_.end();
  if (size_ == 0) { // only one result should not compare
    json_point_ = node;
  } else if (size_ == 1 && sort_vector_.size() == 0) {
    // if have two result, should append json_point_ first, then append new node.
    if (OB_ISNULL(json_point_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get first node", K(ret));
    } else if (sort_vector_.remain() < 2 && OB_FAIL(sort_vector_.reserve(OB_PATH_RESULT_EXPAND_SIZE))) {
      LOG_WARN("fail to expand vactor", K(ret));
    } else if (OB_FAIL(sort_vector_.insert_unique(json_point_, pos, cmp, unique))) {
      LOG_WARN("fail to push_back value into duplicate", K(ret), K(sort_vector_.size()));
    } else if (OB_FAIL(sort_vector_.insert_unique(node, pos, cmp, unique))) {
      LOG_WARN("fail to push_back value into duplicate", K(ret), K(sort_vector_.size()));
    }
  } else if (sort_vector_.remain() == 0 && OB_FAIL(sort_vector_.reserve(OB_PATH_RESULT_EXPAND_SIZE))) {
    LOG_WARN("fail to expand vactor", K(ret));
  } else if (OB_FAIL(sort_vector_.insert_unique(node, pos, cmp, unique))) {
    LOG_WARN("fail to push_back value into result", K(ret), K(sort_vector_.size()));
  }
  if (OB_SUCC(ret)) {
    size_ ++;
  }
  return ret;
}

// only use in seek, use stack memory should deep copy.
int ObIJsonBase::add_if_missing(ObJsonSortedResult &dup, ObJsonSeekResult &res, ObIAllocator* allocator) const
{
  INIT_SUCC(ret);
  ObIJsonBase* cur_json = const_cast<ObIJsonBase*>(this);
  ObJsonBin* json_bin = NULL;

  // Reduce array allocation size ： 2
  // binary need clone new node
  if (is_bin()) {
    if (res.size() == 0) {
      json_bin = static_cast<ObJsonBin*>(res.res_point_);
    }
    if (OB_FAIL((static_cast<ObJsonBin*>(cur_json))->clone_new_node(json_bin, allocator))) {
      LOG_WARN("failed to create json binary", K(ret));
    } else if (OB_ISNULL(cur_json = json_bin)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get json binary value", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_SUCC(dup.insert_unique(cur_json))) {
    if (OB_FAIL(res.push_node(cur_json))) {
      LOG_WARN("fail to push_back value into result", K(ret), K(res.size()));
    }
  } else if (ret == OB_CONFLICT_VALUE) {
    ret = OB_SUCCESS; // confilict means found duplicated nodes, it is not an error.
  }

  return ret;
}

int ObIJsonBase::find_array_range(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                                  const JsonPathIterator &cur_node, const JsonPathIterator &last_node,
                                  const ObJsonPathBasicNode *path_node, bool is_auto_wrap,
                                  bool only_need_one, bool is_lax, ObJsonSortedResult &dup,
                                  ObJsonSeekResult &res, PassingMap* sql_var) const
{
  INIT_SUCC(ret);
  ObJsonBin st_json(allocator_); // use stack variable instead of deep copy
  SMART_VAR (ObArrayRange, range) {
    if (OB_FAIL(path_node->get_array_range(element_count(), range))) {
      LOG_WARN("fail to get array range", K(ret), K(element_count()));
    } else {
      bool is_done = false;
      ObIJsonBase *jb_ptr = NULL;
      for (uint32_t i = range.array_begin_; OB_SUCC(ret) && i < range.array_end_ && !is_done; ++i) {
        jb_ptr = &st_json; // reset jb_ptr to stack var
        ret = get_array_element(i, jb_ptr);
        if (OB_ISNULL(jb_ptr)) {
          ret = OB_ERR_NULL_VALUE;
          LOG_WARN("fail to get array child dom", K(ret), K(i));
        } else if (OB_FAIL(jb_ptr->find_child(allocator, parent_info, cur_node + 1,
                                              last_node, is_auto_wrap, only_need_one,
                                              is_lax, dup, res, sql_var))) {
          LOG_WARN("fail to seek recursively", K(ret), K(i), K(is_auto_wrap), K(only_need_one));
        } else {
          is_done = is_seek_done(res, only_need_one);
        }
      }
    }
  }
  return ret;
}

int ObIJsonBase::find_array_cell(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                                const JsonPathIterator &cur_node, const JsonPathIterator &last_node,
                                const ObJsonPathBasicNode *path_node, bool is_auto_wrap,
                                bool only_need_one, bool is_lax, ObJsonSortedResult &dup,
                                ObJsonSeekResult &res, PassingMap* sql_var) const
{
  INIT_SUCC(ret);
  ObJsonBin st_json(allocator_); // use stack variable instead of deep copy
  ObIJsonBase *jb_ptr = NULL;
  SMART_VAR (ObJsonArrayIndex, idx) {
    if (OB_FAIL(path_node->get_first_array_index(element_count(), idx))) {
      LOG_WARN("failed to get array index.", K(ret), K(element_count()));
    } else {
      if (idx.is_within_bounds()) {
        jb_ptr = &st_json; // reset jb_ptr to stack var
        ret = get_array_element(idx.get_array_index(), jb_ptr);
        if (OB_ISNULL(jb_ptr)) {
          ret = OB_ERR_NULL_VALUE;
          LOG_WARN("fail to get array child dom", K(ret), K(idx.get_array_index()));
        } else if (OB_FAIL(jb_ptr->find_child(allocator, parent_info, cur_node + 1,
                                              last_node, is_auto_wrap, only_need_one,
                                              is_lax, dup, res, sql_var))) {
          LOG_WARN("fail to seek recursively", K(ret), K(is_auto_wrap), K(only_need_one));
        }
      }
    }
  }

  return ret;
}

int ObIJsonBase::seek(const ObJsonPath &path, uint32_t node_cnt, bool is_auto_wrap,
                      bool only_need_one, ObJsonSeekResult &res, PassingMap* sql_var) const
{
  INIT_SUCC(ret);
  ObSeekParentInfo parent_info;
  JsonPathIterator cur_node = path.begin();
  JsonPathIterator last_node = path.begin() + node_cnt;
  ObJsonSortedResult dup;

  if (OB_ISNULL(allocator_)) { // check allocator
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("param allocator is NULL", K(ret), KP(allocator_));
  } else if (lib::is_oracle_mode() || !path.is_mysql_) {
    parent_info.parent_jb_ = const_cast<ObIJsonBase*> (this);
    parent_info.parent_path_ = path.begin();
    parent_info.is_subpath_ = path.is_sub_path_;
    parent_info.path_size_ = node_cnt;
    if (OB_FAIL(find_child(allocator_, parent_info, cur_node, last_node, is_auto_wrap,
                          only_need_one, true, dup, res, sql_var))) {
      LOG_WARN("fail to seek", K(ret));
    }
  } else if (lib::is_mysql_mode() || path.is_mysql_) {
    if (OB_FAIL(find_child(allocator_, parent_info,
                          cur_node, last_node, is_auto_wrap,
                          only_need_one, false, dup, res, sql_var))) {
      LOG_WARN("fail to seek", K(ret));
    }
  }

  return ret;
}

int ObIJsonBase::seek(ObIAllocator* allocator, const ObJsonPath &path,
                      uint32_t node_cnt, bool is_auto_wrap,bool only_need_one,
                      bool is_lax, ObJsonSeekResult &res, PassingMap* sql_var) const
{
  INIT_SUCC(ret);
  // 对于$后的path节点而言，其parent_info.parent_path为begin()
  // 对于@后的path节点而言，其parent_info为上一层节点(可能为$后 或 上一个@后的末尾节点)
  if (lib::is_oracle_mode() || is_lax) {
    ObSeekParentInfo parent_info;
    parent_info.parent_jb_ = const_cast<ObIJsonBase*> (this);
    parent_info.parent_path_ = path.begin();
    parent_info.is_subpath_ = path.is_sub_path_;
    parent_info.path_size_ = node_cnt;
    JsonPathIterator cur_node = path.begin();
    JsonPathIterator last_node = path.begin() + node_cnt;
    ObJsonSortedResult dup;

    if (OB_FAIL(find_child(allocator, parent_info,
                          cur_node, last_node, is_auto_wrap,
                          only_need_one, is_lax, dup, res, sql_var))) {
      LOG_WARN("fail to seek", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("supposed to be oracle/LAX mode.", K(ret), K(lib::is_oracle_mode()));
  }

  return ret;
}

int ObIJsonBase::find_member(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                            const JsonPathIterator &cur_node, const JsonPathIterator &last_node,
                            const ObJsonPathBasicNode *path_node, bool is_auto_wrap,
                            bool only_need_one, bool is_lax, ObJsonSortedResult &dup,
                            ObJsonSeekResult &res, PassingMap* sql_var) const
{
  INIT_SUCC(ret);

  ObIJsonBase *jb_ptr = NULL;
  ObJsonBin st_json(allocator_); // use stack variable instead of deep copy
  if (json_type() == ObJsonNodeType::J_OBJECT) {
    jb_ptr = &st_json;
    ObString key_name(path_node->get_object().len_, path_node->get_object().object_name_);
    ret = get_object_value(key_name, jb_ptr);
    if (OB_SUCC(ret)) {
      // 寻找成功，向下递归
      if (is_lax && !is_auto_wrap) is_auto_wrap = true;
      if (OB_FAIL(jb_ptr->find_child(allocator, parent_info, cur_node + 1, last_node,
                                    is_auto_wrap, only_need_one, is_lax, dup, res, sql_var))) {
        LOG_WARN("fail to seek recursively", K(ret), K(is_auto_wrap), K(only_need_one));
      }
    } else if (ret == OB_SEARCH_NOT_FOUND) {
      // not found, it is normal.
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get object value", K(ret), K(key_name));
    }
  // 宽松模式，对数组里面的每一个节点查找
  } else if (is_lax && is_auto_wrap && json_type() == ObJsonNodeType::J_ARRAY) {
    bool is_done = false;
    for (uint32_t i = 0; OB_SUCC(ret) && i < element_count() && !is_done; ++i) {
      jb_ptr = &st_json; // reset jb_ptr to stack var
      ret = get_array_element(i, jb_ptr);
      if (OB_ISNULL(jb_ptr)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("fail to get array child dom", K(ret), K(i));
      } else if (OB_FAIL(jb_ptr->find_child(allocator, parent_info, cur_node,
                                            last_node, false, only_need_one,
                                            is_lax, dup, res, sql_var))) {
        LOG_WARN("fail to seek recursively", K(ret), K(i), K(is_auto_wrap), K(only_need_one));
      } else {
        is_done = is_seek_done(res, only_need_one);
      }
    }
  } // 既不是宽松模式下的数组，又不是ObJsonNodeType::J_OBJECT，没有找到任何数据，是正常的

  return ret;
}

int ObIJsonBase::find_member_wildcard(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                                      const JsonPathIterator &cur_node, const JsonPathIterator &last_node,
                                      const ObJsonPathBasicNode *path_node, bool is_auto_wrap,
                                      bool only_need_one, bool is_lax, ObJsonSortedResult &dup,
                                      ObJsonSeekResult &res, PassingMap* sql_var) const
{
  INIT_SUCC(ret);

  ObIJsonBase *jb_ptr = NULL;
  bool is_done = false;
  ObJsonBin st_json(allocator_); // use stack variable instead of deep copy
  if (json_type() == ObJsonNodeType::J_OBJECT) {
    uint64_t count = element_count();
    is_done = false;
    if (is_lax && !is_auto_wrap) is_auto_wrap = true;

    for (uint64_t i = 0; i < count && OB_SUCC(ret) && !is_done; ++i) {
      jb_ptr = &st_json;
      ret = get_object_value(i, jb_ptr);
      if (OB_ISNULL(jb_ptr)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("fail to get object child dom",K(ret), K(i));
      } else if (OB_FAIL(jb_ptr->find_child(allocator, parent_info, cur_node + 1,
                                            last_node, is_auto_wrap, only_need_one,
                                            is_lax, dup, res, sql_var))) {
        LOG_WARN("fail to seek recursively",K(ret), K(i));
      } else {
        is_done = is_seek_done(res, only_need_one);
      }
    }
  // 宽松模式，对数组里面的每一个节点查找
  } else if (is_lax && is_auto_wrap && json_type() == ObJsonNodeType::J_ARRAY) {
    is_done = false;
    for (uint32_t i = 0; OB_SUCC(ret) && i < element_count() && !is_done; ++i) {
      jb_ptr = &st_json;
      ret = get_array_element(i, jb_ptr);
      if (OB_ISNULL(jb_ptr)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("fail to get array child dom", K(ret), K(i));
      } else if (OB_FAIL(jb_ptr->find_child(allocator, parent_info, cur_node,
                                            last_node, false, only_need_one,
                                            is_lax, dup, res, sql_var))) {
        LOG_WARN("fail to seek recursively", K(ret), K(i), K(is_auto_wrap), K(only_need_one));
      } else {
        is_done = is_seek_done(res, only_need_one);
      }
    }
  } // 既不是宽松模式下的数组，又不是ObJsonNodeType::J_OBJECT，没有找到任何数据，是正常的

  return ret;
}

int ObIJsonBase::find_ellipsis(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                              const JsonPathIterator &cur_node, const JsonPathIterator &last_node,
                              const ObJsonPathBasicNode *path_node, bool is_auto_wrap,
                              bool only_need_one, bool is_lax, ObJsonSortedResult &dup,
                              ObJsonSeekResult &res, PassingMap* sql_var) const
{
  INIT_SUCC(ret);
  bool is_done = false;
  ObJsonBin st_json(allocator_); // use stack variable instead of deep copy
  if (OB_FAIL(find_child(allocator, parent_info, cur_node + 1, last_node,
                        is_auto_wrap, only_need_one, is_lax, dup, res, sql_var))) {
    LOG_WARN("fail to seek recursively", K(ret), K(is_auto_wrap), K(only_need_one));
  } else if (json_type() == ObJsonNodeType::J_ARRAY) {
    uint64_t size = element_count();
    ObIJsonBase *jb_ptr = NULL;
    for (uint32_t i = 0; i < size && !is_done && OB_SUCC(ret); ++i) {
      jb_ptr = &st_json; // reset jb_ptr to stack var
      ret = get_array_element(i, jb_ptr);
      if (OB_ISNULL(jb_ptr)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("fail to get array child dom",K(ret), K(i));
      } else if (OB_FAIL(jb_ptr->find_child(allocator, parent_info, cur_node,
                                            last_node, is_auto_wrap, only_need_one,
                                            is_lax, dup, res, sql_var))) {
        LOG_WARN("fail to seek recursively",K(ret), K(i), K(is_auto_wrap), K(only_need_one));
      } else {
        is_done = is_seek_done(res, only_need_one);
      }
    }
  } else if (json_type() == ObJsonNodeType::J_OBJECT) {
    uint64_t count = element_count();
    ObIJsonBase *jb_ptr = NULL; // set jb_ptr to stack var
    for (uint32_t i = 0; i < count && !is_done && OB_SUCC(ret); ++i) {
      jb_ptr = &st_json;; // reset jb_ptr to NULL
      ret = get_object_value(i, jb_ptr);
      if (OB_ISNULL(jb_ptr)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("fail to get object child dom",K(ret), K(i));
      } else if (OB_FAIL(jb_ptr->find_child(allocator, parent_info, cur_node,
                                            last_node, is_auto_wrap, only_need_one,
                                            is_lax, dup, res, sql_var))) {
        LOG_WARN("fail to seek recursively",K(ret), K(i), K(is_auto_wrap), K(only_need_one));
      } else {
        is_done = is_seek_done(res, only_need_one);
      }
    }
  }
  return ret;
}

int ObIJsonBase::find_array_wildcard(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                                    const JsonPathIterator &cur_node, const JsonPathIterator &last_node,
                                    const ObJsonPathBasicNode *path_node, bool is_auto_wrap,
                                    bool only_need_one, bool is_lax, ObJsonSortedResult &dup,
                                    ObJsonSeekResult &res, PassingMap* sql_var) const
{
  INIT_SUCC(ret);

  ObIJsonBase *jb_ptr = NULL;
  bool is_done = false;
  ObJsonBin st_json(allocator_); // use stack variable instead of deep copy
  for (uint32_t i = 0; OB_SUCC(ret) && i < element_count() && !is_done; ++i) {
    jb_ptr = &st_json; // reset jb_ptr to stack var
    ret = get_array_element(i, jb_ptr);
    if (OB_ISNULL(jb_ptr)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("fail to get array child dom", K(ret), K(i));
    } else if (OB_FAIL(jb_ptr->find_child(allocator, parent_info, cur_node + 1,
                                          last_node, is_auto_wrap, only_need_one,
                                          is_lax, dup, res, sql_var))) {
      LOG_WARN("fail to seek recursively", K(ret), K(i), K(is_auto_wrap), K(only_need_one));
    } else {
      is_done = is_seek_done(res, only_need_one);
    }
  }

  return ret;
}

int ObIJsonBase::find_multi_array_ranges(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                                          const JsonPathIterator &cur_node, const JsonPathIterator &last_node,
                                          const ObJsonPathBasicNode *path_node, bool is_auto_wrap,
                                          bool only_need_one, bool is_lax, ObJsonSortedResult &dup,
                                          ObJsonSeekResult &res, PassingMap* sql_var) const
{
  INIT_SUCC(ret);
  uint64_t array_size = path_node->get_multi_array_size();
  ObJsonBin st_json(allocator_); // use stack variable instead of deep copy

  for (uint64_t array_idx = 0; array_idx < array_size && OB_SUCC(ret); ++array_idx) {
    SMART_VAR (ObArrayRange, range) {
      if (OB_FAIL(path_node->get_multi_array_range(array_idx, element_count(), range))) {
        LOG_WARN("fail to get array range", K(ret), K(element_count()));
      } else {
        bool is_done = false;
        ObIJsonBase *jb_ptr = NULL;
        ObJsonSortedResult tmp_dup;
        for (uint32_t i = range.array_begin_; OB_SUCC(ret) && i < range.array_end_ && !is_done; ++i) {
          jb_ptr = &st_json; // reset jb_ptr to stack var
          ret = get_array_element(i, jb_ptr);
          if (OB_ISNULL(jb_ptr)) {
            ret = OB_ERR_NULL_VALUE;
            LOG_WARN("fail to get array child dom", K(ret), K(i));
          } else if (OB_FAIL(jb_ptr->find_child(allocator, parent_info, cur_node + 1,
                                                last_node, is_auto_wrap, only_need_one,
                                                is_lax, tmp_dup, res, sql_var))) {
            LOG_WARN("fail to seek recursively", K(ret), K(i));
          } else {
            is_done = is_seek_done(res, only_need_one);
          }
        } // end of search for each cell in arrar_range
      }
    } // end of each range
  }
  return ret;
}

int ObIJsonBase::find_basic_child(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                                  const JsonPathIterator &cur_node, const JsonPathIterator &last_node,
                                  bool is_auto_wrap, bool only_need_one, bool is_lax,
                                  ObJsonSortedResult &dup, ObJsonSeekResult &res,
                                  PassingMap* sql_var) const
{
  INIT_SUCC(ret);
  ObJsonPathBasicNode* path_node = static_cast<ObJsonPathBasicNode *>(*cur_node);
  ObJsonNodeType cur_json_type = json_type();
  switch (path_node->get_node_type()) {
    case JPN_MEMBER: {
      if (cur_json_type == ObJsonNodeType::J_OBJECT
         || (is_lax && cur_json_type == ObJsonNodeType::J_ARRAY )) {
        if (OB_FAIL(find_member(allocator, parent_info, cur_node,
                                        last_node, path_node, is_auto_wrap,
                                        only_need_one, is_lax, dup, res, sql_var))) {
          LOG_WARN("fail to find member.", K(ret), K(only_need_one));
        }
      }
      break;
    }
    case JPN_MEMBER_WILDCARD: {
      if (cur_json_type == ObJsonNodeType::J_OBJECT
        || (is_lax && cur_json_type == ObJsonNodeType::J_ARRAY )) {
        if (OB_FAIL(find_member_wildcard(allocator, parent_info, cur_node,
                                        last_node, path_node, is_auto_wrap,
                                        only_need_one, is_lax, dup, res, sql_var))) {
          LOG_WARN("fail to find member wildcard.", K(ret), K(only_need_one));
        }
      }
      break;
    }
    case JPN_WILDCARD_ELLIPSIS:
    case JPN_DOT_ELLIPSIS: {
      if (OB_FAIL(find_ellipsis(allocator, parent_info, cur_node, last_node, path_node,
                                is_auto_wrap, only_need_one, is_lax, dup, res, sql_var))) {
        LOG_WARN("fail to find ellipsis.", K(ret), K(is_auto_wrap), K(only_need_one));
      }
      break;
    }
    case JPN_ARRAY_CELL_WILDCARD:
    case JPN_MULTIPLE_ARRAY:
    case JPN_ARRAY_CELL:
    case JPN_ARRAY_RANGE: {
      if (OB_FAIL(find_array_child(allocator, parent_info, cur_node, last_node, is_auto_wrap,
                                  only_need_one, is_lax, dup, res, sql_var))) {
        LOG_WARN("fail to find ellipsis.", K(ret), K(is_auto_wrap), K(only_need_one));
      }
      break;
    }
    default:{
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("supposed to be oracle basic node type.", K(ret), K(path_node->get_node_type()));
    }
  }
  return ret;
}

int ObIJsonBase::find_array_child(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                                  const JsonPathIterator &cur_node, const JsonPathIterator &last_node,
                                  bool is_auto_wrap, bool only_need_one, bool is_lax,
                                  ObJsonSortedResult &dup, ObJsonSeekResult &res,
                                  PassingMap* sql_var) const
{
  INIT_SUCC(ret);
  ObJsonPathBasicNode* path_node = static_cast<ObJsonPathBasicNode *>(*cur_node);
  ObJsonNodeType cur_json_type = json_type();
  switch (path_node->get_node_type()) {
    case JPN_ARRAY_CELL_WILDCARD: {
      if (cur_json_type == ObJsonNodeType::J_ARRAY) {
        if (OB_FAIL(find_array_wildcard(allocator, parent_info, cur_node, last_node,
                                        path_node, is_auto_wrap, only_need_one, is_lax,
                                        dup, res, sql_var))) {
          LOG_WARN("fail in find array range.", K(ret));
        }
      } else {
        if (is_auto_wrap
            && ((!is_lax && path_node->is_autowrap())
            || (is_lax && path_node->is_multi_array_autowrap()))) {
          if (OB_FAIL(find_child(allocator, parent_info, cur_node + 1, last_node,
                                is_auto_wrap, only_need_one, is_lax, dup, res, sql_var))) {
            LOG_WARN("fail to seek autowrap of array range", K(ret), K(only_need_one));
          }
        }
      }
      break;
    }

    case JPN_MULTIPLE_ARRAY: {
      if (!is_lax) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("should be oracle mode!", K(ret), K(only_need_one));
      } else if (cur_json_type == ObJsonNodeType::J_ARRAY) {
        if (OB_FAIL(find_multi_array_ranges(allocator, parent_info, cur_node, last_node, path_node,
                                            is_auto_wrap, only_need_one, is_lax, dup, res, sql_var))) {
          LOG_WARN("fail in find array range.", K(ret));
        }
      } else if (is_auto_wrap && path_node->is_multi_array_autowrap()) {
        if (OB_FAIL(find_child(allocator, parent_info, cur_node + 1, last_node,
                                is_auto_wrap, only_need_one, is_lax, dup, res, sql_var))) {
          LOG_WARN("fail to seek autowrap of array range", K(ret), K(only_need_one));
        }
      }
      break;
    }

    case JPN_ARRAY_CELL: {
      if (is_lax) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("should be mysql mode!", K(ret), K(only_need_one));
      } else if (cur_json_type == ObJsonNodeType::J_ARRAY) {
        if (OB_FAIL(find_array_cell(allocator, parent_info, cur_node, last_node, path_node,
                                    is_auto_wrap, only_need_one, is_lax, dup, res, sql_var))) {
          LOG_WARN("fail to find array cell.", K(ret), K(is_auto_wrap), K(only_need_one));
        }
      } else if (is_auto_wrap && path_node->is_autowrap()) {
        if (OB_FAIL(find_child(allocator, parent_info, cur_node + 1, last_node,
                              is_auto_wrap, only_need_one, is_lax, dup, res, sql_var))) {
          LOG_WARN("fail to seek autowrap of array cell", K(ret), K(only_need_one));
        }
      }

      break;
    }

    case JPN_ARRAY_RANGE: {
      if (is_lax) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("should be mysql mode!", K(ret), K(only_need_one));
      } else if (cur_json_type == ObJsonNodeType::J_ARRAY) {
        if (OB_FAIL(find_array_range(allocator, parent_info, cur_node, last_node, path_node,
                                    is_auto_wrap, only_need_one, is_lax, dup, res, sql_var))) {
          LOG_WARN("fail in find array range.", K(ret));
        }
      } else if (is_auto_wrap && path_node->is_autowrap()) {
        if (OB_FAIL(find_child(allocator, parent_info, cur_node + 1,
                              last_node, is_auto_wrap, only_need_one,
                              is_lax, dup, res, sql_var))) {
          LOG_WARN("fail to seek autowrap of array range", K(ret), K(only_need_one));
        }
      }
      break;
    }

    default:{
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("supposed to be array node type.", K(ret), K(path_node->get_node_type()));
    }
  }

  return ret;
}
int ObIJsonBase::find_abs_method(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                                const JsonPathIterator &cur_node, const JsonPathIterator &last_node,
                                const ObJsonPathFuncNode *path_node, bool is_auto_wrap, bool only_need_one,
                                bool is_lax, ObJsonSortedResult &dup, ObJsonSeekResult &res) const
{
  INIT_SUCC(ret);
  switch (json_type()) {
    case ObJsonNodeType::J_ODECIMAL:
    case ObJsonNodeType::J_DECIMAL: {
      number::ObNumber tmp_num =  get_decimal_data();
      tmp_num = tmp_num.abs();
      ObIJsonBase *jb_ptr = nullptr;
      ObJsonDecimal* tmp_ans = static_cast<ObJsonDecimal*> (allocator->alloc(sizeof(ObJsonDecimal)));
      if (OB_ISNULL(tmp_ans)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate row buffer failed at ObJsonDecimal", K(ret));
      } else {
        tmp_ans = new (tmp_ans) ObJsonDecimal(tmp_num);
        jb_ptr = tmp_ans;
        if ((OB_FAIL(jb_ptr->find_child(allocator, parent_info, cur_node + 1,
                                        last_node, is_auto_wrap, only_need_one,
                                        is_lax, dup, res)))) {
          int old_ret = ret;
          allocator->free(tmp_ans);
          ret = old_ret;
          LOG_WARN("fail to seek recursively", K(ret));
        }
      }
      break;
    }
    case ObJsonNodeType::J_DOUBLE:
    case ObJsonNodeType::J_OFLOAT:
    case ObJsonNodeType::J_ODOUBLE: {
      double tmp_num = get_double();
      tmp_num = std::abs(tmp_num);
      ObIJsonBase *jb_ptr = nullptr;
      // uint64_t value = 0;
      // ObJsonBaseUtil::number_to_uint(tmp_num, value);
      ObJsonDouble * tmp_ans = static_cast<ObJsonDouble *> (allocator->alloc(sizeof(ObJsonDouble)));
      if (OB_ISNULL(tmp_ans)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate row buffer failed at ObJsonDecimal", K(ret));
      } else {
        tmp_ans = new (tmp_ans) ObJsonDouble(tmp_num);
        jb_ptr = tmp_ans;
        if ((OB_FAIL(jb_ptr->find_child(allocator, parent_info, cur_node + 1,
                                        last_node, is_auto_wrap,
                                        only_need_one, is_lax, dup, res)))) {
          int old_ret = ret;
          allocator->free(tmp_ans);
          ret = old_ret;
          LOG_WARN("fail to seek recursively", K(ret));
        }
      }
      break;
    }
    case ObJsonNodeType::J_INT:
    case ObJsonNodeType::J_OINT:
    case ObJsonNodeType::J_OLONG: {
      int64_t tmp_num = get_int();
      tmp_num = std::abs(tmp_num);
      ObIJsonBase *jb_ptr = nullptr;
      // uint64_t value = 0;
      // ObJsonBaseUtil::number_to_uint(tmp_num, value);
      ObJsonInt* tmp_ans = static_cast<ObJsonInt *> (allocator->alloc(sizeof(ObJsonInt)));
      if (OB_ISNULL(tmp_ans)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate row buffer failed at ObJsonDecimal", K(ret));
      } else {
        tmp_ans = new (tmp_ans) ObJsonInt(tmp_num);
        jb_ptr = tmp_ans;
        if ((OB_FAIL(jb_ptr->find_child(allocator, parent_info, cur_node + 1, last_node,
                                        is_auto_wrap, only_need_one, is_lax, dup, res)))) {
          int old_ret = ret;
          allocator->free(tmp_ans);
          ret = old_ret;
          LOG_WARN("fail to seek recursively", K(ret));
        }
      }
      break;
    }
    case ObJsonNodeType::J_UINT: {
      if ((OB_FAIL(find_child(allocator, parent_info, cur_node + 1, last_node,
                              is_auto_wrap, only_need_one, is_lax, dup, res)))) {
        LOG_WARN("fail to seek recursively", K(ret));
      }
      break;
    }
    default:{
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected json type.", K(ret), K(json_type()));
    }
  }
  return ret;
}

int ObIJsonBase::find_ceiling_method(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                                    const JsonPathIterator &cur_node, const JsonPathIterator &last_node,
                                    const ObJsonPathFuncNode *path_node, bool is_auto_wrap, bool only_need_one,
                                    bool is_lax, ObJsonSortedResult &dup, ObJsonSeekResult &res) const
{
  INIT_SUCC(ret);
  switch (json_type()) {
    case ObJsonNodeType::J_ODECIMAL:
    case ObJsonNodeType::J_DECIMAL: {
      number::ObNumber tmp_num =  get_decimal_data();
      if (OB_FAIL(tmp_num.ceil(0))) {
        LOG_WARN("fail to ceiling.", K(ret), K(tmp_num));
      } else {
        ObIJsonBase *jb_ptr = nullptr;
        ObJsonDecimal* tmp_ans = static_cast<ObJsonDecimal*> (allocator->alloc(sizeof(ObJsonDecimal)));
        if (OB_ISNULL(tmp_ans)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate row buffer failed at ObJsonDecimal", K(ret));
        } else {
          tmp_ans = new (tmp_ans) ObJsonDecimal(tmp_num);
          jb_ptr = tmp_ans;
          if ((OB_FAIL(jb_ptr->find_child(allocator, parent_info, cur_node + 1, last_node,
                                          is_auto_wrap, only_need_one, is_lax, dup, res)))) {
            int old_ret = ret;
            allocator->free(tmp_ans);
            ret = old_ret;
            LOG_WARN("fail to seek recursively", K(ret));
          }
        }
      }
      break;
    }
    case ObJsonNodeType::J_DOUBLE:
    case ObJsonNodeType::J_OFLOAT:
    case ObJsonNodeType::J_ODOUBLE: {
      int64_t tmp_num = std::ceil(get_double());
      ObIJsonBase *jb_ptr = nullptr;
      ObJsonInt* tmp_ans = static_cast<ObJsonInt *> (allocator->alloc(sizeof(ObJsonInt)));
      if (OB_ISNULL(tmp_ans)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate row buffer failed at ObJsonDecimal", K(ret));
      } else {
        tmp_ans = new (tmp_ans) ObJsonInt(tmp_num);
        jb_ptr = tmp_ans;
        if ((OB_FAIL(jb_ptr->find_child(allocator, parent_info, cur_node + 1, last_node,
                                        is_auto_wrap, only_need_one, is_lax, dup, res)))) {
          int old_ret = ret;
          allocator->free(tmp_ans);
          ret = old_ret;
          LOG_WARN("fail to seek recursively", K(ret));
        }
      }
      break;
    }
    case ObJsonNodeType::J_INT:
    case ObJsonNodeType::J_OINT:
    case ObJsonNodeType::J_OLONG:
    case ObJsonNodeType::J_UINT: {
      if ((OB_FAIL(find_child(allocator, parent_info, cur_node + 1, last_node,
                              is_auto_wrap, only_need_one, is_lax, dup, res)))) {
        LOG_WARN("fail to seek recursively", K(ret));
      }
      break;
    }
    default:{
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected json type.", K(ret), K(json_type()));
    }
  }
  return ret;
}

int ObIJsonBase::find_floor_method(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                                  const JsonPathIterator &cur_node, const JsonPathIterator &last_node,
                                  const ObJsonPathFuncNode *path_node, bool is_auto_wrap, bool only_need_one,
                                  bool is_lax, ObJsonSortedResult &dup, ObJsonSeekResult &res) const
{
  INIT_SUCC(ret);
  switch (json_type()) {
    case ObJsonNodeType::J_ODECIMAL:
    case ObJsonNodeType::J_DECIMAL: {
      number::ObNumber tmp_num =  get_decimal_data();
      if (OB_FAIL(tmp_num.floor(0))) {
         LOG_WARN("fail to floor.", K(ret), K(tmp_num));
      } else {
        ObIJsonBase *jb_ptr = nullptr;
        ObJsonDecimal* tmp_ans = static_cast<ObJsonDecimal*> (allocator->alloc(sizeof(ObJsonDecimal)));
        if (OB_ISNULL(tmp_ans)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate row buffer failed at ObJsonDecimal", K(ret));
        } else {
          tmp_ans = new (tmp_ans) ObJsonDecimal(tmp_num);
          jb_ptr = tmp_ans;
          if ((OB_FAIL(jb_ptr->find_child(allocator, parent_info, cur_node + 1, last_node,
                                          is_auto_wrap, only_need_one, is_lax, dup, res)))) {
            int old_ret = ret;
            allocator->free(tmp_ans);
            ret = old_ret;
            LOG_WARN("fail to seek recursively", K(ret));
          }
        }
      }
      break;
    }
    case ObJsonNodeType::J_DOUBLE:
    case ObJsonNodeType::J_OFLOAT:
    case ObJsonNodeType::J_ODOUBLE: {
      int64_t tmp_num = std::floor(get_double());
      ObIJsonBase *jb_ptr = nullptr;
      // uint64_t value = 0;
      // ObJsonBaseUtil::number_to_uint(tmp_num, value);
      ObJsonInt* tmp_ans = static_cast<ObJsonInt *> (allocator->alloc(sizeof(ObJsonInt)));
      if (OB_ISNULL(tmp_ans)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate row buffer failed at ObJsonDecimal", K(ret));
      } else {
        tmp_ans = new (tmp_ans) ObJsonInt(tmp_num);
        jb_ptr = tmp_ans;
        if ((OB_FAIL(jb_ptr->find_child(allocator, parent_info, cur_node + 1, last_node,
                                        is_auto_wrap, only_need_one, is_lax, dup, res)))) {
          int old_ret = ret;
          allocator->free(tmp_ans);
          ret = old_ret;
          LOG_WARN("fail to seek recursively", K(ret));
        }
      }
      break;
    }
    case ObJsonNodeType::J_INT:
    case ObJsonNodeType::J_OINT:
    case ObJsonNodeType::J_OLONG:
    case ObJsonNodeType::J_UINT: {
      if ((OB_FAIL(find_child(allocator, parent_info, cur_node + 1, last_node,
                              is_auto_wrap, only_need_one, is_lax, dup, res)))) {
        LOG_WARN("fail to seek recursively", K(ret));
      }
      break;
    }
    default:{
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected json type.", K(ret), K(json_type()));
    }
  }
  return ret;
}

int ObIJsonBase::find_numeric_item_method(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                                        const JsonPathIterator &cur_node, const JsonPathIterator &last_node,
                                        const ObJsonPathFuncNode *path_node, bool is_auto_wrap, bool only_need_one,
                                        bool is_lax, ObJsonSortedResult &dup, ObJsonSeekResult &res) const
{
  INIT_SUCC(ret);
  ObIJsonBase *jb_ptr = NULL;
  ObJsonBin st_json(allocator_); // use stack variable instead of deep copy
  if (json_type() == ObJsonNodeType::J_ARRAY && is_auto_wrap) {
    bool is_done = false;
    for (uint32_t i = 0; OB_SUCC(ret) && i < element_count() && !is_done; ++i) {
      jb_ptr = &st_json; // reset jb_ptr to stack var
      ret = get_array_element(i, jb_ptr);
      if (OB_ISNULL(jb_ptr)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("fail to get array child dom", K(ret), K(i));
      } else if (OB_FAIL(jb_ptr->find_child(allocator, parent_info, cur_node, last_node,
                                            false, only_need_one, is_lax, dup, res))) {
        LOG_WARN("fail to seek recursively", K(ret), K(i), K(is_auto_wrap), K(only_need_one));
      } else {
        is_done = is_seek_done(res, only_need_one);
      }
    }
  } else if (is_json_number(json_type())) {
    switch (path_node->get_node_type()) {
      case ObJsonPathNodeType::JPN_ABS: {
        if (OB_FAIL(find_abs_method(allocator, parent_info, cur_node, last_node, path_node,
                                    is_auto_wrap, only_need_one, is_lax, dup, res))) {
          LOG_WARN("fail to get abs.", K(ret));
        }
        break;
      }
      case ObJsonPathNodeType::JPN_FLOOR: {
        if (OB_FAIL(find_floor_method(allocator, parent_info, cur_node, last_node, path_node,
                                      is_auto_wrap, only_need_one, is_lax, dup, res))) {
          LOG_WARN("fail to get floor.", K(ret));
        }
        break;
      }
      case ObJsonPathNodeType::JPN_CEILING: {
        if (OB_FAIL(find_ceiling_method(allocator, parent_info, cur_node, last_node, path_node,
                                        is_auto_wrap, only_need_one, is_lax, dup, res))) {
          LOG_WARN("fail to get ceiling.", K(ret));
        }
        break;
      }

      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected node type.", K(ret), K(path_node->get_node_type()));
      }
    }
  } else {
    ObJsonNull* tmp_ans = static_cast<ObJsonNull*> (allocator->alloc(sizeof(ObJsonNull)));
    if (OB_ISNULL(tmp_ans)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate row buffer failed at ObJsonDecimal", K(ret));
    } else {
      tmp_ans = new (tmp_ans) ObJsonNull(true);
      jb_ptr = tmp_ans;
      if ((OB_FAIL(jb_ptr->find_child(allocator, parent_info, cur_node + 1,
                                      last_node, is_auto_wrap,
                                      only_need_one, is_lax, dup, res)))) {
        int old_ret = ret;
        allocator->free(tmp_ans);
        ret = old_ret;
        LOG_WARN("fail to seek recursively", K(ret));
      }
    }
  }
  return ret;
}

int ObIJsonBase::find_type_method(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                                  const JsonPathIterator &cur_node, const JsonPathIterator &last_node,
                                  const ObJsonPathFuncNode *path_node, bool is_auto_wrap, bool only_need_one,
                                  bool is_lax, ObJsonSortedResult &dup, ObJsonSeekResult &res) const
{
  INIT_SUCC(ret);
  char* ans_char = nullptr;
  uint64_t ans_len  = 0;
  switch (json_type()) {
    case ObJsonNodeType::J_NULL: {
      ans_char = const_cast<char*>("null");
      ans_len = strlen("null");
      break;
    }
    case ObJsonNodeType::J_DECIMAL:
    case ObJsonNodeType::J_INT:
    case ObJsonNodeType::J_UINT:
    case ObJsonNodeType::J_DOUBLE:
    case ObJsonNodeType::J_OFLOAT:
    case ObJsonNodeType::J_ODOUBLE:
    case ObJsonNodeType::J_ODECIMAL:
    case ObJsonNodeType::J_OINT:
    case ObJsonNodeType::J_OLONG: {
      ans_char = const_cast<char*>("number");
      ans_len = strlen("number");
      break;
    }
    case ObJsonNodeType::J_STRING:
    case ObJsonNodeType::J_DATE:
    case ObJsonNodeType::J_TIME:
    case ObJsonNodeType::J_DATETIME:
    case ObJsonNodeType::J_TIMESTAMP:
    case ObJsonNodeType::J_OPAQUE:
    case ObJsonNodeType::J_OBINARY:  // binary string
    case ObJsonNodeType::J_OOID:  // binary string
    case ObJsonNodeType::J_ORAWHEX:  // binary string
    case ObJsonNodeType::J_ORAWID:  // binary string
    case ObJsonNodeType::J_ORACLEDATE:  // date
    case ObJsonNodeType::J_ODATE:   // timestamp string
    case ObJsonNodeType::J_OTIMESTAMP:  // timestamp string
    case ObJsonNodeType::J_OTIMESTAMPTZ:  // timestamptz string
    case ObJsonNodeType::J_ODAYSECOND:  // daySecondInterval string
    case ObJsonNodeType::J_OYEARMONTH: {
      ans_char = const_cast<char*>("string");
      ans_len = strlen("string");
      break;
    }
    case ObJsonNodeType::J_OBJECT: {
      ans_char = const_cast<char*>("object");
      ans_len = strlen("object");
      break;
    }
    case ObJsonNodeType::J_ARRAY: {
      ans_char = const_cast<char*>("array");
      ans_len = strlen("array");
      break;
    }
    case ObJsonNodeType::J_BOOLEAN: {
      ans_char = const_cast<char*>("boolean");
      ans_len = strlen("boolean");
      break;
    }
    default :{
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected json node type.", K(ret), K(path_node->get_node_type()));
    }
  }

  if (OB_SUCC(ret)) {
    ObIJsonBase *jb_ptr = NULL;
    ObJsonString* tmp_ans = static_cast<ObJsonString*> (allocator->alloc(sizeof(ObJsonString)));
    if (OB_ISNULL(tmp_ans)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate row buffer failed at ObJsonDecimal", K(ret));
    } else {
      tmp_ans = new (tmp_ans) ObJsonString(ans_char, ans_len);
      jb_ptr = tmp_ans;
      if ((OB_FAIL(jb_ptr->find_child(allocator, parent_info, cur_node + 1,
                                      last_node, is_auto_wrap,
                                      only_need_one, is_lax, dup, res)))) {
        int old_ret = ret;
        allocator->free(tmp_ans);
        ret = old_ret;
        LOG_WARN("fail to seek recursively", K(ret));
      }
    }
  }
  return ret;
}

int ObIJsonBase::find_length_method(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                                    const JsonPathIterator &cur_node, const JsonPathIterator &last_node,
                                    const ObJsonPathFuncNode *path_node, bool is_auto_wrap, bool only_need_one,
                                    bool is_lax, ObJsonSortedResult &dup, ObJsonSeekResult &res) const
{
  INIT_SUCC(ret);
  ObIJsonBase *jb_ptr = NULL;
  bool null_ans = false;
  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", K(ret));
  } else if (is_json_number(json_type()) && !parent_info.is_subpath_) {
    if ((OB_FAIL(find_child(allocator, parent_info, cur_node + 1,
                            last_node, is_auto_wrap,
                            only_need_one, is_lax, dup, res)))) {
      LOG_WARN("fail to seek recursively", K(ret));
    }
  } else if (is_json_string(json_type())) {
    ObJsonUint* tmp_ans = static_cast<ObJsonUint*> (allocator->alloc(sizeof(ObJsonUint)));
    if (OB_ISNULL(tmp_ans)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate row buffer failed at ObJsonDecimal", K(ret));
    } else {
      tmp_ans = new (tmp_ans) ObJsonUint(get_data_length());
      tmp_ans->set_is_string_length(true);
      jb_ptr = tmp_ans;
      if ((OB_FAIL(jb_ptr->find_child(allocator, parent_info, cur_node + 1,
                                      last_node, is_auto_wrap,
                                      only_need_one, is_lax, dup, res)))) {
        int old_ret = ret;
        allocator->free(tmp_ans);
        ret = old_ret;
        LOG_WARN("fail to seek recursively", K(ret));
      }
    }
  } else {
    ObJsonNull* tmp_ans = static_cast<ObJsonNull*> (allocator->alloc(sizeof(ObJsonNull)));
    if (OB_ISNULL(tmp_ans)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate row buffer failed at ObJsonDecimal", K(ret));
    } else {
      tmp_ans = new (tmp_ans) ObJsonNull(true);
      jb_ptr = tmp_ans;
      if ((OB_FAIL(jb_ptr->find_child(allocator, parent_info, cur_node + 1,
                                      last_node, is_auto_wrap,
                                      only_need_one, is_lax, dup, res)))) {
        int old_ret = ret;
        allocator->free(tmp_ans);
        ret = old_ret;
        LOG_WARN("fail to seek recursively", K(ret));
      }
    }
  }
  return ret;
}

int ObIJsonBase::find_size_method(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                                  const JsonPathIterator &cur_node, const JsonPathIterator &last_node,
                                  const ObJsonPathFuncNode *path_node, bool is_auto_wrap, bool only_need_one,
                                  bool is_lax, ObJsonSortedResult &dup, ObJsonSeekResult &res) const
{
  INIT_SUCC(ret);
  ObIJsonBase *jb_ptr = NULL;
  uint64_t ans_size = 0;
  ObJsonNodeType j_type = json_type();
  if (j_type == ObJsonNodeType::J_ARRAY) {
    ans_size = element_count();
  } else {
    ans_size = 1;
  }
  ObJsonUint* tmp_ans = static_cast<ObJsonUint*> (allocator->alloc(sizeof(ObJsonUint)));
  if (OB_ISNULL(tmp_ans)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate row buffer failed at ObJsonDecimal", K(ret));
  } else {
    tmp_ans = new (tmp_ans) ObJsonUint(ans_size);
    jb_ptr = tmp_ans;
    if ((OB_FAIL(jb_ptr->find_child(allocator, parent_info, cur_node + 1,
                                    last_node, is_auto_wrap,
                                    only_need_one, is_lax, dup, res)))) {
      int old_ret = ret;
      allocator->free(tmp_ans);
      ret = old_ret;
      LOG_WARN("fail to seek recursively", K(ret));
    }
  }
  return ret;
}

int ObIJsonBase::find_boolean_method(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                                      const JsonPathIterator &cur_node, const JsonPathIterator &last_node,
                                      const ObJsonPathFuncNode *path_node, bool is_auto_wrap, bool only_need_one,
                                      bool is_lax, ObJsonSortedResult &dup, ObJsonSeekResult &res) const
{
  INIT_SUCC(ret);
  ObIJsonBase* jb_ptr = nullptr;
  bool return_null = false;
  switch (json_type()) {
    case ObJsonNodeType::J_BOOLEAN: {
      if ((OB_FAIL(find_child(allocator, parent_info, cur_node + 1,
                              last_node, is_auto_wrap,
                              only_need_one, is_lax, dup, res)))) {
        LOG_WARN("fail to seek recursively", K(ret));
      }
      break;
    }
    case ObJsonNodeType::J_STRING:
    case ObJsonNodeType::J_DATE:
    case ObJsonNodeType::J_TIME:
    case ObJsonNodeType::J_DATETIME:
    case ObJsonNodeType::J_TIMESTAMP:
    case ObJsonNodeType::J_OPAQUE:
    case ObJsonNodeType::J_OBINARY:  // binary string
    case ObJsonNodeType::J_OOID:  // binary string
    case ObJsonNodeType::J_ORAWHEX:  // binary string
    case ObJsonNodeType::J_ORAWID:  // binary string
    case ObJsonNodeType::J_ORACLEDATE:  // date
    case ObJsonNodeType::J_ODATE:   // timestamp string
    case ObJsonNodeType::J_OTIMESTAMP:  // timestamp string
    case ObJsonNodeType::J_OTIMESTAMPTZ:  // timestamptz string
    case ObJsonNodeType::J_ODAYSECOND:  // daySecondInterval string
    case ObJsonNodeType::J_OYEARMONTH: {
      // 对于boolean(), 如果是bool值则返回bool值。
      // 如果是string, 若其内容为"true"(忽略大小写，则转为相应bool，否则就返回本身
      // 对于booleanOnly(), 当且仅当为bool值时返回结果(即本身)
      if (path_node->get_node_type() == JPN_BOOLEAN) {
        ObString str(get_data_length(), get_data());
        bool is_true = false;
        bool is_bool = false;
        if (get_data_length() == strlen("true")
            && (0 == strncasecmp(str.ptr(), "true", strlen("true")))) {
          is_bool = true;
          is_true = true;
        } else if (get_data_length() == strlen("false")
            && 0 == strncasecmp(str.ptr(), "false", strlen("false"))) {
          is_bool = true;
          is_true = false;
        }

        if (is_bool) {
          ObJsonBoolean* tmp_ans = static_cast<ObJsonBoolean*> (allocator->alloc(sizeof(ObJsonBoolean)));
          if (OB_ISNULL(tmp_ans)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("allocate row buffer failed at ObJsonDecimal", K(ret));
          } else {
            tmp_ans = new (tmp_ans) ObJsonBoolean(is_true);
            jb_ptr = tmp_ans;
            if ((OB_FAIL(jb_ptr->find_child(allocator, parent_info, cur_node + 1,
                                            last_node, is_auto_wrap,
                                            only_need_one, is_lax, dup, res)))) {
              int old_ret = ret;
              allocator->free(tmp_ans);
              ret = old_ret;
              LOG_WARN("fail to seek recursively", K(ret));
            }
          }
        } else {
          if ((OB_FAIL(find_child(allocator, parent_info, cur_node + 1,
                                  last_node, is_auto_wrap,
                                  only_need_one, is_lax, dup, res)))) {
            LOG_WARN("fail to seek recursively", K(ret));
          }
        }
      } else {
        return_null = true;
      }
      break;
    }
    // boolean()对于NULL会返回本身
    case ObJsonNodeType::J_NULL:{
      if (path_node->get_node_type() == JPN_BOOLEAN) {
        if ((OB_FAIL(find_child(allocator, parent_info, cur_node + 1,
                                last_node, is_auto_wrap,
                                only_need_one, is_lax, dup, res)))) {
          LOG_WARN("fail to seek recursively", K(ret));
        }
      }
      break;
    }
    default :{
      // 对于数字和非标量返回null
      if (path_node->get_node_type() == JPN_BOOLEAN && is_json_number(json_type())
          && !parent_info.is_subpath_) {
        if ((OB_FAIL(find_child(allocator, parent_info, cur_node + 1,
                                last_node, is_auto_wrap,
                                only_need_one, is_lax, dup, res)))) {
          LOG_WARN("fail to seek recursively", K(ret));
        }
      } else {
        return_null  = true;
      }
      break;
    }
  }

  if (OB_SUCC(ret) && return_null) {
    ObJsonNull* tmp_ans = static_cast<ObJsonNull*> (allocator->alloc(sizeof(ObJsonNull)));
    if (OB_ISNULL(tmp_ans)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate row buffer failed at ObJsonDecimal", K(ret));
    } else {
      tmp_ans = new (tmp_ans) ObJsonNull(true);
      jb_ptr = tmp_ans;
      if ((OB_FAIL(jb_ptr->find_child(allocator, parent_info, cur_node + 1,
                                      last_node, is_auto_wrap,
                                      only_need_one, is_lax, dup, res)))) {
        int old_ret = ret;
        allocator->free(tmp_ans);
        ret = old_ret;
        LOG_WARN("fail to seek recursively", K(ret));
      }
    }
  }
  return ret;
}

bool ObIJsonBase::check_legal_ora_date(const ObString date) const
{
  bool ret_bool = true;
  uint64_t begin = 0;
  int len = date.length();
  ObJsonPathUtil::skip_whitespace(date, begin);
  for (int i = 0; begin + i < len && ret_bool; ++i) {
    if ((i <= 3 || i == 5 || i == 6 || i == 8 || i == 9)
        && date[begin + i] >= '0' && date[begin + i] <= '9') {
    } else if ((i == 4 || i == 7) && date[begin + i] == '-') {
    } else if (i > 9 && ObJsonPathUtil::is_whitespace(date[i])) {
    } else {
      ret_bool = false;
    }
  }
  return ret_bool;
}

int ObIJsonBase::find_date_method(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                                  const JsonPathIterator &cur_node, const JsonPathIterator &last_node,
                                  const ObJsonPathFuncNode *path_node, bool is_auto_wrap, bool only_need_one,
                                  bool is_lax, ObJsonSortedResult &dup, ObJsonSeekResult &res) const
{
  INIT_SUCC(ret);
  ObIJsonBase* jb_ptr = nullptr;
  int32_t date;
  ObTime ob_time;
  bool return_null = false;
  bool trans_fail = false;
  if (is_json_date(json_type()) || json_type() == ObJsonNodeType::J_NULL) {
    if ((OB_FAIL(find_child(allocator, parent_info, cur_node + 1,
                            last_node, is_auto_wrap,
                            only_need_one, is_lax, dup, res)))) {
      LOG_WARN("fail to seek recursively", K(ret));
    }
  } else if (json_type() == ObJsonNodeType::J_STRING) {
    ObString date_str(get_data_length(), get_data());
    if (!check_legal_ora_date(date_str) || OB_FAIL(to_date(date))) {
      trans_fail = true;
    } else {
      if (OB_FAIL(ObTimeConverter::date_to_ob_time(date, ob_time))) {
        LOG_WARN("fail to cast int to ob_time", K(ret));
      } else {
        ObJsonDatetime* tmp_ans = static_cast<ObJsonDatetime*> (allocator->alloc(sizeof(ObJsonDatetime)));
        if (OB_ISNULL(tmp_ans)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate row buffer failed at ObJsonDecimal", K(ret));
        } else {
          tmp_ans = new (tmp_ans) ObJsonDatetime(ObJsonNodeType::J_DATE, ob_time);
          jb_ptr = tmp_ans;
          if ((OB_FAIL(jb_ptr->find_child(allocator, parent_info, cur_node + 1,
                                          last_node, is_auto_wrap,
                                          only_need_one, is_lax, dup, res)))) {
            int old_ret = ret;
            allocator->free(tmp_ans);
            ret = old_ret;
            LOG_WARN("fail to seek recursively", K(ret));
          }
        }
      }
    }
  } else if (is_json_number(json_type())) {
    trans_fail = true;
  } else if (!is_json_scalar(json_type())) {
    if (parent_info.is_subpath_) {
      return_null = true;
      trans_fail = false;
    } else {
      trans_fail = true;
      return_null = false;
    }
  } else {
    return_null = true;
  }

  if (return_null || trans_fail) {
    // fail is normal, it is not an error.
    if (OB_FAIL(ret)) ret = OB_SUCCESS;
    if (parent_info.is_subpath_ || return_null) {
      if (trans_fail) ret = OB_SUCCESS;
      ObJsonNull* tmp_ans = static_cast<ObJsonNull*> (allocator->alloc(sizeof(ObJsonNull)));
      if (OB_ISNULL(tmp_ans)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate row buffer failed at ObJsonDecimal", K(ret));
      } else {
        tmp_ans = new (tmp_ans) ObJsonNull(true);
        jb_ptr = tmp_ans;
        if ((OB_FAIL(jb_ptr->find_child(allocator, parent_info, cur_node + 1,
                                        last_node, is_auto_wrap,
                                        only_need_one, is_lax, dup, res)))) {
          int old_ret = ret;
          allocator->free(tmp_ans);
          ret = old_ret;
          LOG_WARN("fail to seek recursively", K(ret));
        }
      }
    } else if (trans_fail) {
      if ((OB_FAIL(find_child(allocator, parent_info, cur_node + 1,
                            last_node, is_auto_wrap,
                            only_need_one, is_lax, dup, res)))) {
        LOG_WARN("fail to seek recursively", K(ret));
      }
    }
  }
  return ret;
}

int ObIJsonBase::find_timestamp_method(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                                      const JsonPathIterator &cur_node, const JsonPathIterator &last_node,
                                      const ObJsonPathFuncNode *path_node, bool is_auto_wrap, bool only_need_one,
                                      bool is_lax, ObJsonSortedResult &dup, ObJsonSeekResult &res) const
{
  INIT_SUCC(ret);
  ObIJsonBase *jb_ptr = NULL;
  ObTime ob_time;
  bool return_null = false;
  bool trans_fail = false;
  if (is_json_date(json_type()) || json_type() == ObJsonNodeType::J_NULL) {
    if ((OB_FAIL(find_child(allocator, parent_info, cur_node + 1,
                            last_node, is_auto_wrap,
                            only_need_one, is_lax, dup, res)))) {
      LOG_WARN("fail to seek recursively", K(ret));
    }
  } else if (json_type() == ObJsonNodeType::J_STRING) {
    common::ObOTimestampData date;
    ObTimeZoneInfo tz_info;
    ObTimeConvertCtx cvrt_ctx(&tz_info, false);
    cvrt_ctx.oracle_nls_format_ = ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_TZ_FORMAT;
    ObOTimestampData ot_data;
    int16_t scale = 0;

    ObString str(get_data_length(), get_data());

    if (OB_FAIL(ObTimeConverter::str_to_otimestamp(str, cvrt_ctx, ObTimestampTZType, ot_data, scale))) {
      trans_fail = true;
      LOG_WARN("fail to convert string to otimestamp", K(ret));
    } else {
      if (OB_FAIL(ObTimeConverter::otimestamp_to_ob_time(ObTimestampNanoType, ot_data, NULL, ob_time))) {
        LOG_WARN("fail to convert otimestamp to ob_time", K(ret));
      } else {
        ObJsonDatetime* tmp_ans = static_cast<ObJsonDatetime*> (allocator->alloc(sizeof(ObJsonDatetime)));
        if (OB_ISNULL(tmp_ans)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate row buffer failed at ObJsonDecimal", K(ret));
        } else {
          tmp_ans = new (tmp_ans) ObJsonDatetime(ob_time, ObTimestampType);
          jb_ptr = tmp_ans;
          if ((OB_FAIL(jb_ptr->find_child(allocator, parent_info, cur_node + 1,
                                          last_node, is_auto_wrap,
                                          only_need_one, is_lax, dup, res)))) {
            int old_ret = ret;
            allocator->free(tmp_ans);
            ret = old_ret;
            LOG_WARN("fail to seek recursively", K(ret));
          }
        }
      }
    }
  } else if (is_json_number(json_type())) {
    trans_fail = true;
  } else if (!is_json_scalar(json_type())) {
    if (parent_info.is_subpath_) {
      return_null = true;
      trans_fail = false;
    } else {
      trans_fail = true;
      return_null = false;
    }
  } else {
    return_null = true;
  }

  if (trans_fail || return_null) {
    // fail is normal, it is not an error.
    if (OB_FAIL(ret)) ret = OB_SUCCESS;
    if (parent_info.is_subpath_ || return_null) {
      if (trans_fail) ret = OB_SUCCESS;
      ObJsonNull* tmp_ans = static_cast<ObJsonNull*> (allocator->alloc(sizeof(ObJsonNull)));
      if (OB_ISNULL(tmp_ans)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate row buffer failed at ObJsonDecimal", K(ret));
      } else {
        tmp_ans = new (tmp_ans) ObJsonNull(true);
        jb_ptr = tmp_ans;
        if ((OB_FAIL(jb_ptr->find_child(allocator, parent_info, cur_node + 1,
                                        last_node, is_auto_wrap,
                                        only_need_one, is_lax, dup, res)))) {
          int old_ret = ret;
          allocator->free(tmp_ans);
          ret = old_ret;
          LOG_WARN("fail to seek recursively", K(ret));
        }
      }
    } else if (trans_fail ) {
      if ((OB_FAIL(find_child(allocator, parent_info, cur_node + 1,
                            last_node, is_auto_wrap,
                            only_need_one, is_lax, dup, res)))) {
        LOG_WARN("fail to seek recursively", K(ret));
      }
    }
  }
  return ret;
}

int ObIJsonBase::find_double_method(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                                    const JsonPathIterator &cur_node, const JsonPathIterator &last_node,
                                    const ObJsonPathFuncNode *path_node, bool is_auto_wrap, bool only_need_one,
                                    bool is_lax, ObJsonSortedResult &dup, ObJsonSeekResult &res) const
{
  INIT_SUCC(ret);
  ObIJsonBase *jb_ptr = NULL;
  double num;
  bool return_null = false;
  bool trans_fail = false;
  if (json_type() == ObJsonNodeType::J_DOUBLE || json_type() == ObJsonNodeType::J_NULL) {
    if ((OB_FAIL(find_child(allocator, parent_info, cur_node + 1,
                            last_node, is_auto_wrap,
                            only_need_one, is_lax, dup, res)))) {
      LOG_WARN("fail to seek recursively", K(ret));
    }
  } else if (is_json_number(json_type()) || json_type() == ObJsonNodeType::J_STRING) {
    if (OB_FAIL(to_double(num))) {
      // fail is normal, it is not an error.
      trans_fail = true;
      ret = OB_SUCCESS;
    } else {
      ObJsonDouble* tmp_ans = static_cast<ObJsonDouble*> (allocator->alloc(sizeof(ObJsonDouble)));
      if (OB_ISNULL(tmp_ans)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate row buffer failed at ObJsonDecimal", K(ret));
      } else {
        tmp_ans = new (tmp_ans) ObJsonDouble(num);
        jb_ptr = tmp_ans;
        if ((OB_FAIL(jb_ptr->find_child(allocator, parent_info, cur_node + 1,
                                        last_node, is_auto_wrap,
                                        only_need_one, is_lax, dup, res)))) {
          int old_ret = ret;
          allocator->free(tmp_ans);
          ret = old_ret;
          LOG_WARN("fail to seek recursively", K(ret));
        }
      }
    }
  } else if (!is_json_scalar(json_type())) {
    if (parent_info.is_subpath_) {
      return_null = true;
      trans_fail = false;
    } else {
      trans_fail = true;
      return_null = false;
    }
  } else if (json_type() == ObJsonNodeType::J_BOOLEAN) {
    trans_fail = true;
    return_null = false;
  } else {
    return_null = true;
  }

  if (return_null || trans_fail) {
    if (parent_info.is_subpath_ || return_null) {
      ObJsonNull* tmp_ans = static_cast<ObJsonNull*> (allocator->alloc(sizeof(ObJsonNull)));
      if (OB_ISNULL(tmp_ans)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate row buffer failed at ObJsonDecimal", K(ret));
      } else {
        tmp_ans = new (tmp_ans) ObJsonNull(true);
        jb_ptr = tmp_ans;
        if ((OB_FAIL(jb_ptr->find_child(allocator, parent_info, cur_node + 1,
                                        last_node, is_auto_wrap,
                                        only_need_one, is_lax, dup, res)))) {
          int old_ret = ret;
          allocator->free(tmp_ans);
          ret = old_ret;
          LOG_WARN("fail to seek recursively", K(ret));
        }
      }
    } else if (trans_fail) {
      if ((OB_FAIL(find_child(allocator, parent_info, cur_node + 1,
                            last_node, is_auto_wrap,
                            only_need_one, is_lax, dup, res)))) {
        LOG_WARN("fail to seek recursively", K(ret));
      }
    }
  }
  return ret;
}

int ObIJsonBase::find_number_method(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                                    const JsonPathIterator &cur_node, const JsonPathIterator &last_node,
                                    const ObJsonPathFuncNode *path_node, bool is_auto_wrap, bool only_need_one,
                                    bool is_lax, ObJsonSortedResult &dup, ObJsonSeekResult &res) const
{
  INIT_SUCC(ret);

  ObIJsonBase *jb_ptr = NULL;
  bool fail_to_number = false;
  bool return_null = false;

  if (is_json_number(json_type())) {
    if ((OB_FAIL(find_child(allocator, parent_info, cur_node + 1,
                            last_node, is_auto_wrap,
                            only_need_one, is_lax, dup, res)))) {
      LOG_WARN("fail to seek recursively", K(ret));
    }
  } else if (path_node->get_node_type() == JPN_NUMBER && is_json_string(json_type())) {
    ObString num_str(get_data_length(), get_data());
    if (OB_SUCC(trans_to_json_number(allocator, num_str, jb_ptr))) {
      if (!OB_ISNULL(jb_ptr) && is_json_number(jb_ptr->json_type())) {
        if ((OB_FAIL(jb_ptr->find_child(allocator, parent_info, cur_node + 1,
                                        last_node, is_auto_wrap,
                                        only_need_one, is_lax, dup, res)))) {
          LOG_WARN("fail to seek recursively", K(ret));
        }
      } else {
        fail_to_number = true;
      }
    } else {
      fail_to_number = true;
    }
  } else if (json_type() == ObJsonNodeType::J_BOOLEAN || is_json_string(json_type())) {
    fail_to_number = true;
  } else {
    return_null = true;
  }

  if (fail_to_number || return_null) {
    if (parent_info.is_subpath_ || return_null) {
      ObJsonNull* tmp_ans = static_cast<ObJsonNull*> (allocator->alloc(sizeof(ObJsonNull)));
      if (OB_ISNULL(tmp_ans)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate row buffer failed at ObJsonDecimal", K(ret));
      } else {
        tmp_ans = new (tmp_ans) ObJsonNull(true);
        jb_ptr = tmp_ans;
        if ((OB_FAIL(jb_ptr->find_child(allocator, parent_info, cur_node + 1,
                                        last_node, is_auto_wrap,
                                        only_need_one, is_lax, dup, res)))) {
          int old_ret = ret;
          allocator->free(tmp_ans);
          ret = old_ret;
          LOG_WARN("fail to seek recursively", K(ret));
        }
      }
    } else if (fail_to_number) {
      if ((OB_FAIL(find_child(allocator, parent_info, cur_node + 1,
                            last_node, is_auto_wrap,
                            only_need_one, is_lax, dup, res)))) {
        LOG_WARN("fail to seek recursively", K(ret));
      }
    }
  }
  return ret;
}

int ObIJsonBase::find_string_method(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                                  const JsonPathIterator &cur_node, const JsonPathIterator &last_node,
                                  const ObJsonPathFuncNode *path_node, bool is_auto_wrap, bool only_need_one,
                                  bool is_lax, ObJsonSortedResult &dup, ObJsonSeekResult &res) const
{
  INIT_SUCC(ret);
  ObIJsonBase *jb_ptr = NULL;
  bool str_only = (path_node->get_node_type() == JPN_STR_ONLY);
  bool return_null = false;
  bool trans_fail = false;

  if (is_json_string(json_type()) || (!str_only && json_type() == ObJsonNodeType::J_NULL)) {
    if ((OB_FAIL(find_child(allocator, parent_info, cur_node + 1,
                            last_node, is_auto_wrap,
                            only_need_one, is_lax, dup, res)))) {
      LOG_WARN("fail to seek recursively", K(ret));
    }
  } else if (!is_json_scalar(json_type())) {
    if (parent_info.is_subpath_) {
      return_null = true;
      trans_fail = false;
    } else {
      trans_fail = true;
      return_null = false;
    }
  } else if (!str_only) {
    ObJsonBuffer j_buf(allocator);
    if (OB_FAIL(print(j_buf, true, false, 0))) {
      trans_fail = true;
    } else {
      ObJsonString* tmp_ans = static_cast<ObJsonString*> (allocator->alloc(sizeof(ObJsonString)));
      if (OB_ISNULL(tmp_ans)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate row buffer failed at ObJsonDecimal", K(ret));
      } else {
        tmp_ans = new (tmp_ans) ObJsonString(j_buf.ptr(), j_buf.length());
        jb_ptr = tmp_ans;
        if ((OB_FAIL(jb_ptr->find_child(allocator, parent_info, cur_node + 1,
                                        last_node, is_auto_wrap,
                                        only_need_one, is_lax, dup, res)))) {
          int old_ret = ret;
          allocator->free(tmp_ans);
          ret = old_ret;
          LOG_WARN("fail to seek recursively", K(ret));
        }
      }
    }
  } else {
    return_null = true;
  }

  if (trans_fail || return_null) {
    if (parent_info.is_subpath_ || return_null) {
      ObJsonNull* tmp_ans = static_cast<ObJsonNull*> (allocator->alloc(sizeof(ObJsonNull)));
      if (OB_ISNULL(tmp_ans)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate row buffer failed at ObJsonDecimal", K(ret));
      } else {
        tmp_ans = new (tmp_ans) ObJsonNull(true);
        jb_ptr = tmp_ans;
        if ((OB_FAIL(jb_ptr->find_child(allocator, parent_info, cur_node + 1,
                                        last_node, is_auto_wrap,
                                        only_need_one, is_lax, dup, res)))) {
          int old_ret = ret;
          allocator->free(tmp_ans);
          ret = old_ret;
          LOG_WARN("fail to seek recursively", K(ret));
        }
      }
    } else if (trans_fail && OB_SUCC(ret)) {
      if ((OB_FAIL(find_child(allocator, parent_info, cur_node + 1,
                            last_node, is_auto_wrap,
                            only_need_one, is_lax, dup, res)))) {
        LOG_WARN("fail to seek recursively", K(ret));
      }
    }
  }
  return ret;
}

int ObIJsonBase::find_trans_method(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                                  const JsonPathIterator &cur_node, const JsonPathIterator &last_node,
                                  const ObJsonPathFuncNode *path_node, bool is_auto_wrap, bool only_need_one,
                                  bool is_lax, ObJsonSortedResult &dup, ObJsonSeekResult &res) const
{
  INIT_SUCC(ret);
  ObIJsonBase *jb_ptr = NULL;
  bool return_null = false;
  bool trans_fail = false;

  ObJsonNodeType type = json_type();
  if (OB_ISNULL(allocator)) { // check allocator
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("param allocator is NULL", K(ret), KP(allocator));
  } else if (!is_json_scalar(json_type())) {
    if (parent_info.is_subpath_) {
      return_null = true;
      trans_fail = false;
    } else {
      trans_fail = true;
      return_null = false;
    }
  } else if (is_json_string(type) || is_json_number(type)
      || type == ObJsonNodeType::J_BOOLEAN
      || (type == ObJsonNodeType::J_NULL && is_real_json_null(this))) {
    ObString src;
    bool is_null_to_str = false;
    if (is_json_string(type)) {
      src = ObString(get_data_length(), get_data());
    } else if (type != ObJsonNodeType::J_NULL) {
      ObJsonBuffer j_buf(allocator);
      if (OB_FAIL(print(j_buf, true, false, 0))) {
        trans_fail = true;
      } else {
        src = ObString(j_buf.length(), j_buf.ptr());
      }
    } else {
      src = "null";
      is_null_to_str = true;
    }
    ObString dst;
    if (OB_FAIL(ret) || OB_ISNULL(src.ptr())) {
      ret = OB_ERR_NULL_VALUE;
    LOG_WARN("param allocator is NULL", K(ret));
    } else if (path_node->get_node_type() == JPN_UPPER) {
      if (OB_FAIL(ObJsonBaseUtil::str_low_to_up(allocator, src, dst))) {
        LOG_WARN("fail to str_low_to_up", K(ret));
      }
    } else {
      if (OB_FAIL(ObJsonBaseUtil::str_up_to_low(allocator, src, dst))) {
        LOG_WARN("fail to str_low_to_up", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      ObJsonString* tmp_ans = static_cast<ObJsonString*> (allocator->alloc(sizeof(ObJsonString)));
      if (OB_ISNULL(tmp_ans)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate row buffer failed at ObJsonDecimal", K(ret));
      } else {
        tmp_ans = new (tmp_ans) ObJsonString(dst.ptr(), dst.length());
        tmp_ans->set_is_null_to_str(is_null_to_str);
        jb_ptr = tmp_ans;
        if ((OB_FAIL(jb_ptr->find_child(allocator, parent_info, cur_node + 1,
                                        last_node, is_auto_wrap,
                                        only_need_one, is_lax, dup, res)))) {
          int old_ret = ret;
          allocator->free(tmp_ans);
          ret = old_ret;
          LOG_WARN("fail to seek recursively", K(ret));
        }
      }
    } else {
      trans_fail = true;
    }
  } else {
    return_null = true;
  }

  if (trans_fail || return_null) {
    if (parent_info.is_subpath_ || return_null) {
      ObJsonNull* tmp_ans = static_cast<ObJsonNull*> (allocator->alloc(sizeof(ObJsonNull)));
      if (OB_ISNULL(tmp_ans)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate row buffer failed at ObJsonDecimal", K(ret));
      } else {
        tmp_ans = new (tmp_ans) ObJsonNull(true);
        jb_ptr = tmp_ans;
        if ((OB_FAIL(jb_ptr->find_child(allocator, parent_info, cur_node + 1,
                                        last_node, is_auto_wrap,
                                        only_need_one, is_lax, dup, res)))) {
          int old_ret = ret;
          allocator->free(tmp_ans);
          ret = old_ret;
          LOG_WARN("fail to seek recursively", K(ret));
        }
      }
    } else if (trans_fail && OB_SUCC(ret)) {
      if ((OB_FAIL(find_child(allocator, parent_info, cur_node + 1,
                            last_node, is_auto_wrap,
                            only_need_one, is_lax, dup, res)))) {
        LOG_WARN("fail to seek recursively", K(ret));
      }
    }
  }
  return ret;
}

int ObIJsonBase::find_func_child(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                                const JsonPathIterator &cur_node, const JsonPathIterator &last_node,
                                bool is_auto_wrap, bool only_need_one, bool is_lax,
                                ObJsonSortedResult &dup, ObJsonSeekResult &res) const
{
  INIT_SUCC(ret);
  SMART_VAR (ObJsonPathFuncNode*, path_node) {
    path_node = static_cast<ObJsonPathFuncNode *>(*cur_node);
  }
  // make sure function item is the last path_node
  if (OB_ISNULL(allocator)) { // check allocator
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("param allocator is NULL", K(ret), KP(allocator));
  }else if (cur_node + 1 != last_node) {
    ret = ret = OB_INVALID_ARGUMENT;
    LOG_WARN("function item must be the last path_node", K(ret));
  } else {
  // 虽然文档说明每个函数只处理特定类型（如abs，floor只处理数字）
  // 但实际上如果不是特定类型也并不会报错
  // 只是返回空（使用ObJsonNull代表该情况，其中is_null_ = false，与Null节点区分)
    switch (path_node->get_node_type()) {
      case JPN_ABS:
      case JPN_CEILING:
      case JPN_FLOOR: {
        if (OB_FAIL(find_numeric_item_method(allocator, parent_info, cur_node,
                                            last_node, path_node, is_auto_wrap,
                                            only_need_one, is_lax, dup, res))) {
          LOG_WARN("fail to get numeric_item_method result.", K(ret));
        };
        break;
      }
      case JPN_TYPE: {
        if (OB_FAIL(find_type_method(allocator, parent_info, cur_node,
                                      last_node, path_node, is_auto_wrap,
                                      only_need_one, is_lax, dup, res))) {
          LOG_WARN("fail to get type_item_method result.", K(ret));
        };
        break;
      }
      case JPN_SIZE: { // 5
        if (OB_FAIL(find_size_method(allocator, parent_info, cur_node,
                                    last_node, path_node, is_auto_wrap,
                                    only_need_one, is_lax, dup, res))) {
          LOG_WARN("fail to get size_item_method result.", K(ret));
        }
        break;
      }
      case JPN_LENGTH: {
        if (OB_FAIL(find_length_method(allocator, parent_info, cur_node,
                                      last_node, path_node, is_auto_wrap,
                                      only_need_one, is_lax, dup, res))) {
          LOG_WARN("fail to get length_item_method result.", K(ret));
        };
        break;
      }
      case JPN_BOOLEAN:
      case JPN_BOOL_ONLY:{
        if (OB_FAIL(find_boolean_method(allocator, parent_info, cur_node,
                                        last_node, path_node, is_auto_wrap,
                                        only_need_one, is_lax, dup, res))) {
          LOG_WARN("fail to get boolean_item_method result.", K(ret));
        };
        break;
      }
      case ObJsonPathNodeType::JPN_DATE: {
        if (OB_FAIL(find_date_method(allocator, parent_info, cur_node,
                                    last_node, path_node, is_auto_wrap,
                                    only_need_one, is_lax, dup, res))) {
          LOG_WARN("fail to get date_item_method result.", K(ret));
        }
        break;
      }
      case ObJsonPathNodeType::JPN_TIMESTAMP: { // 10
        if (OB_FAIL(find_timestamp_method(allocator, parent_info, cur_node,
                                          last_node, path_node, is_auto_wrap,
                                          only_need_one, is_lax, dup, res))) {
          LOG_WARN("fail to get timestamp_item_method result.", K(ret));
        }
        break;
      }
      case ObJsonPathNodeType::JPN_DOUBLE: {
        if (OB_FAIL(find_double_method(allocator, parent_info, cur_node,
                                      last_node, path_node, is_auto_wrap,
                                      only_need_one, is_lax, dup, res))) {
          LOG_WARN("fail to get double_item_method result.", K(ret));
        }
        break;
      }
      case JPN_NUMBER:
      case JPN_NUM_ONLY: {
        if (OB_FAIL(find_number_method(allocator, parent_info, cur_node,
                                      last_node, path_node, is_auto_wrap,
                                      only_need_one, is_lax, dup, res))) {
          LOG_WARN("fail to get number_item_method result.", K(ret));
        }
        break;
      }
      case JPN_STRING:
      case JPN_STR_ONLY: { // 15
        if (OB_FAIL(find_string_method(allocator, parent_info, cur_node,
                                      last_node, path_node, is_auto_wrap,
                                      only_need_one, is_lax, dup, res))) {
          LOG_WARN("fail to get string_item_method result.", K(ret));
        }
        break;
      }
      case JPN_LOWER:
      case JPN_UPPER: {
        if (OB_FAIL(find_trans_method(allocator, parent_info, cur_node,
                                      last_node, path_node, is_auto_wrap,
                                      only_need_one, is_lax, dup, res))) {
          LOG_WARN("fail to get upper/lower_item_method result.", K(ret));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("supposed to be oracle function node type.",
                  K(ret), K(path_node->get_node_type()));
      }
    }
  }
  return ret;
}

int ObIJsonBase::cmp_based_on_node_type(ObJsonPathNodeType node_type, int res, bool& ret_bool) const
{
  INIT_SUCC(ret);
  // @return Less than returns -1, greater than 1, equal returns 0.
  if (-1 <= res && res <= 1) {
    switch (node_type) {
      // ==
      case JPN_EQUAL: {
        ret_bool = (res == 0);
        break;
      }
      // !=
      case JPN_UNEQUAL: {
        ret_bool = (res != 0);
        break;
      }
      // >
      case JPN_LARGGER: {
        ret_bool = (res == 1);
        break;
      }
      // >=
      case JPN_LARGGER_EQUAL: {
        ret_bool = (res >= 0);
        break;
      }
      // <
      case JPN_SMALLER: {
        ret_bool = (res == -1);
        break;
      }
      // <=
      case JPN_SMALLER_EQUAL: {
        ret_bool = (res <= 0);
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("wrong node type, should be path_sign_cmp.", K(ret), K(node_type));
      }
    }
  } else {
    ret_bool = false;
  }
  return ret;
}

// 字符串转number类型，两种情况下用到：
// 1. sub_path的类型为number时，oracle会将字符串自动转换成number处理(有双引号)
// 2. path中的标量首先存成字符串，然后根据比较的内容转换成相应的数字(无双引号)
// PS: 对于情况1，首先需要去除双引号。
//     同时，如果查找到的json_base_node的内容为123时，当且仅当字符串为"123"时可以比较," 123" 和 "123 "均不行
//     因此，若内容不是合法的数字，并不能成功转换为数字类型
/*
    1. SELECT 1 from dual
          WHERE json_exists(
            '["a", 2, 3, 4, 5, {"resolution" : {"x": 1920, "y": 1080}}]',
            '$[5]?(exists(@.resolution?(@.x < " 1921")))');                   // 返回null
    2. SELECT 1 from dual
          WHERE json_exists(
            '["a", 2, 3, 4, 5, {"resolution" : {"x": 1920, "y": 1080}}]',
            '$[5]?(exists(@.resolution?(@.x < "1921")))');                    // 返回1
*/
int ObIJsonBase::trans_to_json_number(ObIAllocator* allocator, ObString str, ObIJsonBase* &origin) const
{
  INIT_SUCC(ret);
  int err = 0;
  ObIJsonBase *jb_ptr = NULL;
  ObString num_str;
  if (str.length() > 0 && str[0] == '\"') {
    num_str = ObString(str.length() - 2, str.ptr() + 1);
  } else {
    num_str = ObString(str.length(), str.ptr());
  }

  // 处理好之后直接解析当前字符串，得到number
  if (OB_FAIL(ObJsonBaseFactory::get_json_base(allocator, num_str,
        ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, jb_ptr))
        || OB_ISNULL(jb_ptr)
        || !is_json_number(jb_ptr->json_type())) {
    allocator->free(jb_ptr);
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to parse json_text", K(ret));
  } else {
    origin = jb_ptr;
  }
  return ret;
}

// 日期类型和字符串类型本身没有差别
// 直接将该字符串解析成json_tree会存储为字符串，需要自定义目标类型
int ObIJsonBase::trans_to_date_timestamp(ObIAllocator* allocator,
                                        ObString str,
                                        ObIJsonBase* &origin,
                                        bool is_date) const
{
  INIT_SUCC(ret);
  ObIJsonBase *jb_ptr = NULL;
  if (is_date) {
    int32_t date;
    ObTime ob_time;
    if (OB_FAIL(origin->to_date(date))) {
      LOG_WARN("fail to cast node to date", K(ret));
    } else {
      if (OB_FAIL(ObTimeConverter::date_to_ob_time(date, ob_time))) {
        LOG_WARN("fail to cast int to ob_time", K(ret));
      } else {
        ObJsonDatetime* tmp_ans = static_cast<ObJsonDatetime*> (allocator->alloc(sizeof(ObJsonDatetime)));
        if (OB_ISNULL(tmp_ans)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate row buffer failed at ObJsonDate", K(ret));
        } else {
          tmp_ans = new (tmp_ans) ObJsonDatetime(ObJsonNodeType::J_DATE, ob_time);
          jb_ptr = tmp_ans;
        }
      }
    }
  } else {
    ObTime ob_time;
    common::ObOTimestampData date;
    ObTimeZoneInfo tz_info;
    ObTimeConvertCtx cvrt_ctx(&tz_info, false);
    cvrt_ctx.oracle_nls_format_ = ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_TZ_FORMAT;
    ObOTimestampData ot_data;
    int16_t scale = 0;
    if (OB_FAIL(ObTimeConverter::str_to_otimestamp(str, cvrt_ctx, ObTimestampTZType, ot_data, scale))) {
      LOG_WARN("fail to convert string to otimestamp", K(ret));
    } else {
      if (OB_FAIL(ObTimeConverter::otimestamp_to_ob_time(ObTimestampNanoType, ot_data, NULL, ob_time))) {
        LOG_WARN("fail to convert otimestamp to ob_time", K(ret));
      } else {
        ObJsonDatetime* tmp_ans = static_cast<ObJsonDatetime*> (allocator->alloc(sizeof(ObJsonDatetime)));
        if (OB_ISNULL(tmp_ans)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate row buffer failed at ObJsonDate", K(ret));
        } else {
          tmp_ans = new (tmp_ans) ObJsonDatetime(ob_time, ObTimestampType);
          jb_ptr = tmp_ans;
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    origin = jb_ptr;
  } else {
    int old_ret = ret;
    allocator->free(jb_ptr);
    ret = old_ret;
  }
  return ret;
}

int ObIJsonBase::trans_to_boolean(ObIAllocator* allocator, ObString str, ObIJsonBase* &origin) const
{
  INIT_SUCC(ret);
  int is_true = -1;

  ObJsonBoolean* tmp_ans = static_cast<ObJsonBoolean*> (allocator->alloc(sizeof(ObJsonBoolean)));
  if (OB_ISNULL(tmp_ans)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate row buffer failed at ObJsonBoolean", K(ret));
  } else {
    if ((str.length() == strlen("\"true\"") && 0 == strncasecmp(str.ptr(), "\"true\"", strlen("\"true\"")))
      ||(str.length() == strlen("true") && 0 == strncasecmp(str.ptr(), "true", strlen("true"))) ) {
      is_true = 1;
    } else if ((str.length() == strlen("\"false\"") && 0 == strncasecmp(str.ptr(), "\"false\"", strlen("\"false\"")))
      || (str.length() == strlen("false") && 0 == strncasecmp(str.ptr(), "false", strlen("false")))) {
      is_true = 0;
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("String fail to convert to Boolean", K(ret));
    }
  }

  if (OB_SUCC(ret) && is_true >= 0) {
    tmp_ans = new (tmp_ans) ObJsonBoolean(is_true == 1);
    origin = tmp_ans;
  } else {
    int old_ret = ret;
    allocator->free(tmp_ans);
    ret = old_ret;
  }

  return ret;
}

bool ObIJsonBase::is_same_type(ObIJsonBase* left, ObIJsonBase* right) const
{
  bool ret_bool = false;
  if (OB_ISNULL(left) || OB_ISNULL(right)) {
    return false;
  } else {
    ObJsonNodeType left_type = left->json_type();
    ObJsonNodeType right_type = right->json_type();
    if (left_type == right_type
      || (is_json_number(left_type) && is_json_number(right_type))
      || (is_json_string(left_type) && is_json_string(right_type))
      || (is_json_date(left_type) && is_json_date(right_type))) {
      ret_bool = true;
    }
  }
  return ret_bool;

}

bool ObIJsonBase::is_real_json_null(const ObIJsonBase* ptr) const
{
  bool ret_bool = false;
  if (ptr->json_type() == ObJsonNodeType::J_NULL) {
    if (ptr->is_bin()) {
      ret_bool = true;
    } else {
      ObIJsonBase* tmp = const_cast<ObIJsonBase*>(ptr);
      ObJsonNull* tmp_null = static_cast<ObJsonNull*>(tmp);
      ret_bool = !(tmp_null->is_not_null());
    }
  }
  return ret_bool;
}

// left is scalar, right is ans of subpath
int ObIJsonBase::trans_json_node(ObIAllocator* allocator, ObIJsonBase* &scalar, ObIJsonBase* &path_res) const
{
  INIT_SUCC(ret);
  ObJsonNodeType left_type = scalar->json_type();
  ObJsonNodeType right_type = path_res->json_type();
  if (left_type == ObJsonNodeType::J_STRING) {
    ObString str(scalar->get_data_length(), scalar->get_data());
    if (is_json_number(right_type)) {
      // fail is normal
      ret = trans_to_json_number(allocator, str, scalar);
    } else if (right_type == ObJsonNodeType::J_DATE
            || right_type == ObJsonNodeType::J_DATETIME
            || right_type == ObJsonNodeType::J_TIME
            || right_type == ObJsonNodeType::J_ORACLEDATE) {
      ret = trans_to_date_timestamp(allocator, str, scalar, true);
    } else if (right_type == ObJsonNodeType::J_TIMESTAMP
            || right_type == ObJsonNodeType::J_OTIMESTAMP
            || right_type == ObJsonNodeType::J_OTIMESTAMPTZ) {
      ret = trans_to_date_timestamp(allocator, str, scalar, false);
    } else if (right_type == ObJsonNodeType::J_BOOLEAN) {
      // when scalar is string, path_res is boolean, case compare
      if (str.case_compare("true") == 0 || str.case_compare("false") == 0) {
        ret = trans_to_boolean(allocator, str, scalar);
      } else {
        ret = OB_NOT_SUPPORTED;
      }
    } else if (right_type != ObJsonNodeType::J_ARRAY && right_type != ObJsonNodeType::J_OBJECT) {
      ret = ret = OB_INVALID_ARGUMENT;
      LOG_WARN("CAN'T TRANS", K(ret));
    }
  } else if (left_type == ObJsonNodeType::J_NULL) {
    // return error code, mean can't cast, return false ans directly
    ret = OB_NOT_SUPPORTED;
  } else if (right_type == ObJsonNodeType::J_STRING) {
    ObString str(path_res->get_data_length(), path_res->get_data());
    if (is_json_number(left_type)) {
      // fail is normal
      ret = trans_to_json_number(allocator, str, path_res);
    } else if (left_type == ObJsonNodeType::J_DATE
            || left_type == ObJsonNodeType::J_DATETIME
            || left_type == ObJsonNodeType::J_TIME
            || left_type == ObJsonNodeType::J_ORACLEDATE) {
      ret = trans_to_date_timestamp(allocator, str, path_res, true);
    } else if (left_type == ObJsonNodeType::J_TIMESTAMP
            || left_type == ObJsonNodeType::J_OTIMESTAMP
            || left_type == ObJsonNodeType::J_OTIMESTAMPTZ) {
      ret = trans_to_date_timestamp(allocator, str, path_res, false);
    } else if (left_type == ObJsonNodeType::J_BOOLEAN) {
      ret = trans_to_boolean(allocator, str, path_res);
    } else if (left_type != ObJsonNodeType::J_ARRAY && left_type != ObJsonNodeType::J_OBJECT) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("CAN'T TRANS", K(ret));
    }
  } else if (left_type == ObJsonNodeType::J_BOOLEAN || is_json_number(left_type)) {
    // scalar is boolean or number, and path_res is not string, return false
    ret = OB_NOT_SUPPORTED;
  } else {
    // do nothing
    LOG_WARN("CAN'T TRANS", K(ret));
  }
  return ret;
}

// for compare ——> (subpath, scalar/sql_var)
// 左边调用compare，左边遇到数组自动解包
// 只要有一个结果为true则返回true，找不到或结果为false均返回false
int ObIJsonBase::cmp_to_right_recursively(ObIAllocator* allocator, ObJsonSeekResult& hit,
                                          const ObJsonPathNodeType node_type,
                                          ObIJsonBase* right_arg, bool& filter_result) const
{
  INIT_SUCC(ret);
  bool cmp_result = false;
  bool null_flag = false;
  ObJsonBin st_json(allocator_); // use stack variable instead of deep copy
  if (OB_ISNULL(right_arg)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("compare value is null.", K(ret));
  }
  for (uint32_t i = 0; !null_flag && i < hit.size() && !cmp_result && OB_SUCC(ret); ++i) {
    if (OB_ISNULL(hit[i])) {
      null_flag = true;
    } else if (hit[i]->json_type() == ObJsonNodeType::J_NULL && !is_real_json_null(hit[i])) {
      cmp_result = false;
    } else {
      // error is ok
      // if is array, compare with every node
      // but only autowrap once
      if (hit[i]->json_type() == ObJsonNodeType::J_ARRAY) {
        uint64_t size = hit[i]->element_count();
        ObIJsonBase *jb_ptr = NULL;
        for (uint32_t array_i = 0; array_i < size && !cmp_result && OB_SUCC(ret); ++array_i) {
          jb_ptr = &st_json; // reset jb_ptr to stack var
          ret = hit[i]->get_array_element(array_i, jb_ptr);
          int cmp_res = -3;
          // 类型相同可以直接用compare函数比较
          if(OB_FAIL(ret) || OB_ISNULL(jb_ptr)) {
            ret = OB_ERR_NULL_VALUE;
            LOG_WARN("compare value is null.", K(ret));
          } else if (is_same_type(right_arg, jb_ptr)) {
            if (OB_SUCC(jb_ptr->compare((*right_arg), cmp_res, true))) {
              cmp_based_on_node_type(node_type, cmp_res, cmp_result);
            }
          } else {
          // 不相同的类型，oracle会将string类型转换为对应类型再进行比较
          // 转换或比较失败也正常，并不报错
          // 例如: [*].a == 123
          // 里面可能有多个元素无法转换成数字或无法和数字比较甚至找不到.a
          // 但只要有一个找到，且为123或"123"则为true
            ObIJsonBase* left = jb_ptr;
            ObIJsonBase* right = right_arg;
            if (OB_FAIL(trans_json_node(allocator, right, left))) {
              // fail is normal, it is not an error.
              ret = OB_SUCCESS;
              cmp_result = false;
            } else if (OB_SUCC(left->compare((*right), cmp_res, true))) {
              cmp_based_on_node_type(node_type, cmp_res, cmp_result);
            } else {
              cmp_result = false;
            }
          }
        }
      } else if (hit[i]->json_type() == ObJsonNodeType::J_OBJECT) {
        cmp_result = false;
      } else {
        int cmp_res = -3;
        if (is_same_type(right_arg, hit[i])) {
          if (OB_SUCC(hit[i]->compare((*right_arg), cmp_res, true))) {
            cmp_based_on_node_type(node_type, cmp_res, cmp_result);
          }
        } else {
        // 不相同的类型，同上
          ObIJsonBase* left = hit[i];
          ObIJsonBase* right = right_arg;
          if (OB_FAIL(trans_json_node(allocator, right, left))) {
            // fail is normal, it is not an error.
            ret = OB_SUCCESS;
            cmp_result = false;
          } else if (OB_SUCC(left->compare((*right), cmp_res, true))) {
            cmp_based_on_node_type(node_type, cmp_res, cmp_result);
          } else {
            cmp_result = false;
          }
        }
      }
    }
  }

  if (!null_flag) {
    filter_result = cmp_result;
  }
  return ret;
}

// for compare ——> ( scalar/sql_var, subpath)
// 只要有一个结果为true则返回true，找不到或结果为false均返回false
int ObIJsonBase::cmp_to_left_recursively(ObIAllocator* allocator, ObJsonSeekResult& hit,
                                          const ObJsonPathNodeType node_type,
                                          ObIJsonBase* left_arg, bool& filter_result) const
{
  INIT_SUCC(ret);
  bool cmp_result = false;
  bool null_flag = false;
  ObJsonBin st_json(allocator_); // use stack variable instead of deep copy
  if (OB_ISNULL(left_arg)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("compare value is null.", K(ret));
  }
  for (uint32_t i = 0; !null_flag && i < hit.size() && !cmp_result && OB_SUCC(ret); ++i) {
    if (OB_ISNULL(hit[i])) {
      null_flag = true;
    } else if (hit[i]->json_type() == ObJsonNodeType::J_NULL && !is_real_json_null(hit[i])) {
      cmp_result = false;
    } else {
      // error is ok
      // if is array, compare with every node
      // but only autowrap once
      if (hit[i]->json_type() == ObJsonNodeType::J_ARRAY) {
        uint64_t size = hit[i]->element_count();
        ObIJsonBase *jb_ptr = NULL;
        for (uint32_t array_i = 0; array_i < size && !cmp_result && OB_SUCC(ret); ++array_i) {
          jb_ptr = &st_json; // reset jb_ptr to stack var
          ret = hit[i]->get_array_element(array_i, jb_ptr);
          int cmp_res = -3;
          // 类型相同可以直接用compare函数比较
          if(OB_FAIL(ret) || OB_ISNULL(jb_ptr)) {
            ret = OB_ERR_NULL_VALUE;
            LOG_WARN("compare value is null.", K(ret));
          } else if (is_same_type(left_arg, jb_ptr)) {
            if (OB_SUCC(left_arg->compare((*jb_ptr), cmp_res, true))) {
              cmp_based_on_node_type(node_type, cmp_res, cmp_result);
            }
          } else {
          // 不相同的类型，oracle会将string类型转换为对应类型再进行比较
          // 转换或比较失败也正常，并不报错
          // 例如: [*].a == 123
          // 里面可能有多个元素无法转换成数字或无法和数字比较甚至找不到.a
          // 但只要有一个找到，且为123或"123"则为true
            ObIJsonBase* left = left_arg;
            ObIJsonBase* right = jb_ptr;
            if (OB_FAIL(trans_json_node(allocator, left, right))) {
              // fail is normal, it is not an error.
              ret = OB_SUCCESS;
              cmp_result = false;
            } else if (OB_SUCC(left->compare((*right), cmp_res, true))) {
                cmp_based_on_node_type(node_type, cmp_res, cmp_result);
            } else {
              cmp_result = false;
            }
          }
        }
      } else if (hit[i]->json_type() == ObJsonNodeType::J_OBJECT) {
        cmp_result = false;
      } else {
        int cmp_res = -3;
        if (is_same_type(left_arg, hit[i])) {
          if (OB_SUCC(left_arg->compare((*hit[i]), cmp_res, true))) {
            cmp_based_on_node_type(node_type, cmp_res, cmp_result);
          }
        } else {
        // 不相同的类型，同上
          ObIJsonBase* left = left_arg;
          ObIJsonBase* right = hit[i];
          if (OB_FAIL(trans_json_node(allocator, left, right))) {
            // fail is normal, it is not an error.
            ret = OB_SUCCESS;
            cmp_result = false;
          } else if (OB_SUCC(left->compare((*right), cmp_res, true))) {
              cmp_based_on_node_type(node_type, cmp_res, cmp_result);
          } else {
            cmp_result = false;
          }
        }
      } // need autowrap or not
    } // hit is null or not
  }

  if (!null_flag) {
    filter_result = cmp_result;
  } else {
    filter_result = false;
  }
  return ret;
}

// for compare ——> (subpath, scalar/sql_var)
// 用于subpath的最后一个节点是item_function时
// 此时如果两边的比较类型不匹配(无论另一边是scalar还是sql_var)会报错
// 如果最后一个path_node不是item_function时，不匹配(如："abc" == 1)不会报错，只是无结果
// 如果最后一个path_node是item_function，但hit有多个结果时，类型不匹配也只是无结果，且只要有一个结果true就返回true
// 不能直接比较json_type()是否相等:
//  1. 因为item_function的返回类型不唯一，但能够比较的类型限制更严格
//     如：length()可能返回 J_NULL 或 json_number
//         但与其比较的类型只能是json_number, 否则oracle会报错
//  2. 左边调用item_fucntion的结果可以不合法：
//     如：'$?(@[0].date() == "2020-02-02")
//         @[0]的内容可以为数字，数组，对象或无法转换为日期的字符串，不会报错
//         但如果等号右边与之相比的并非为日期/时间戳格式的字符串时，会报错提示无法比较
int ObIJsonBase::cmp_to_right_strictly(ObIAllocator* allocator, ObIJsonBase* hit,
                                        const ObJsonPathNodeType node_type,
                                        const ObJsonPathNodeType last_sub_path_node_type,
                                        ObIJsonBase* right_arg, bool& filter_result) const
{
  INIT_SUCC(ret);
  bool cmp_result = false;
  ObJsonNodeType right_type;
  if (OB_ISNULL(right_arg)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("compare value is null.", K(ret));
  } else {
    right_type = right_arg->json_type();
  }
  if (OB_FAIL(ret) ||  OB_ISNULL(hit)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("compare value is null.", K(ret));
  } else if (hit->json_type() == ObJsonNodeType::J_NULL && !is_real_json_null(hit)) {
    cmp_result = false;
  } else {
    switch (last_sub_path_node_type) {
      case JPN_ABS:
      case JPN_FLOOR:
      case JPN_LENGTH:
      case JPN_CEILING:
      case JPN_NUMBER:
      case JPN_NUM_ONLY:
      case JPN_SIZE:
      case JPN_DOUBLE: {
        // 比较出错不报错，只要求类型是number，否则报错
        if (is_json_number(right_type)) {
          int cmp_res = -3;
          if (OB_SUCC(hit->compare((*right_arg), cmp_res, true))) {
            cmp_based_on_node_type(node_type, cmp_res, cmp_result);
          }
        } else {
          ret = OB_ERR_JSON_PATH_EXPRESSION_SYNTAX_ERROR;
          LOG_WARN("type incompatibility to compare.", K(ret));
        }
        break;
      }
      case JPN_BOOLEAN:
      case JPN_BOOL_ONLY: {
      // 对于path.boolean(), 当且仅当右边为布尔值，或是内容为true/false(不区分大小写)的字符串时合法
        if (right_type == ObJsonNodeType::J_BOOLEAN) {// compare
          int cmp_res = -3;
          if (OB_SUCC(hit->compare((*right_arg), cmp_res, true))) {
            cmp_based_on_node_type(node_type, cmp_res, cmp_result);
          }
        } else if (is_json_string(right_type)) {
          ObString str(right_arg->get_data_length(), right_arg->get_data());
          if (OB_FAIL(trans_to_boolean(allocator, str, right_arg))) {
            ret = OB_ERR_JSON_PATH_EXPRESSION_SYNTAX_ERROR;
            LOG_WARN("type incompatibility to compare.", K(ret));
          } else {
            int cmp_res = -3;
            if (OB_SUCC(hit->compare((*right_arg), cmp_res, true))) {
              cmp_based_on_node_type(node_type, cmp_res, cmp_result);
            }
          }
        } else {
          ret = OB_ERR_JSON_PATH_EXPRESSION_SYNTAX_ERROR;
          LOG_WARN("type incompatibility to compare.", K(ret));
        }
        break;
      }
      case JPN_DATE:
      case JPN_TIMESTAMP: {
        if (is_json_date(right_type)) {
          int cmp_res = -3;
          if (OB_SUCC(hit->compare((*right_arg), cmp_res, true))) {
            cmp_based_on_node_type(node_type, cmp_res, cmp_result);
          }
        } else if (right_type == ObJsonNodeType::J_STRING) {
          ObString str(right_arg->get_data_length(), right_arg->get_data());
          if (OB_FAIL(trans_to_date_timestamp(allocator, str, right_arg,
                      last_sub_path_node_type == JPN_DATE))) {
            ret = OB_ERR_JSON_PATH_EXPRESSION_SYNTAX_ERROR;
            LOG_WARN("type incompatibility to compare.", K(ret));
          } else {
            int cmp_res = -3;
            if (OB_SUCC(hit->compare((*right_arg), cmp_res, true))) {
              cmp_based_on_node_type(node_type, cmp_res, cmp_result);
            }
          }
        } else {
          ret = OB_ERR_JSON_PATH_EXPRESSION_SYNTAX_ERROR;
          LOG_WARN("type incompatibility to compare.", K(ret));
        }
        break;
      }
      case JPN_LOWER:
      case JPN_STRING:
      case JPN_STR_ONLY:
      case JPN_TYPE:
      case JPN_UPPER: {
        if (is_json_string(right_type)) {
          int cmp_res = -3;
          if (OB_SUCC(hit->compare((*right_arg), cmp_res, true))) {
            cmp_based_on_node_type(node_type, cmp_res, cmp_result);
          }
        } else {
          ret = OB_ERR_JSON_PATH_EXPRESSION_SYNTAX_ERROR;
          LOG_WARN("type incompatibility to compare.", K(ret));
        }
        break;
      }
      default: {
        ret = OB_ERR_JSON_PATH_EXPRESSION_SYNTAX_ERROR;
        LOG_WARN("type incompatibility to compare.", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    filter_result = cmp_result;
  }
  return ret;
}

// 这次右边是sub_path找到的结果，左边是标量/变量
// 复用left会影响比较的左右
// 这次需要左边的参数转换类型，并调用compare, 而后根据node_type(>, >=...)确定比较结果
// 而之前的是右边的参数转换类型，左边的参数调用compare
int ObIJsonBase::cmp_to_left_strictly(ObIAllocator* allocator, ObIJsonBase* hit,
                                      const ObJsonPathNodeType node_type,
                                      const ObJsonPathNodeType last_sub_path_node_type,
                                      ObIJsonBase* left_arg, bool& filter_result) const
{
  INIT_SUCC(ret);
  bool cmp_result = false;
  ObJsonNodeType left_type;
  if (OB_ISNULL(left_arg)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("compare value is null.", K(ret));
  } else {
    left_type = left_arg->json_type();
  }
  if (OB_FAIL(ret) || OB_ISNULL(hit)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("compare value is null.", K(ret));
  } else if (hit->json_type() == ObJsonNodeType::J_NULL && !is_real_json_null(hit)) {
    cmp_result = false;
  } else {
    switch (last_sub_path_node_type) {
      case JPN_ABS:
      case JPN_FLOOR:
      case JPN_LENGTH:
      case JPN_CEILING:
      case JPN_NUMBER:
      case JPN_NUM_ONLY:
      case JPN_SIZE:
      case JPN_DOUBLE: {
        // 比较出错不报错，只要求类型是number，否则报错
        if (is_json_number(left_type)) {
          int cmp_res = -3;
          if (OB_SUCC(left_arg->compare((*hit), cmp_res, true))) {
            cmp_based_on_node_type(node_type, cmp_res, cmp_result);
          }
        } else {
          ret = OB_ERR_JSON_PATH_EXPRESSION_SYNTAX_ERROR;
          LOG_WARN("type incompatibility to compare.", K(ret));
        }
        break;
      }
      case JPN_BOOLEAN:
      case JPN_BOOL_ONLY: {
      // 对于path.boolean(), 当且仅当右边为布尔值，或是内容为true/false(不区分大小写)的字符串时合法
        if (left_type == ObJsonNodeType::J_BOOLEAN) {// compare
          int cmp_res = -3;
          if (OB_SUCC(left_arg->compare((*hit), cmp_res, true))) {
            cmp_based_on_node_type(node_type, cmp_res, cmp_result);
          }
        } else if (is_json_string(left_type)) {
          ObString str(left_arg->get_data_length(), left_arg->get_data());
          if (OB_FAIL(trans_to_boolean(allocator, str, left_arg))) {
            ret = OB_ERR_JSON_PATH_EXPRESSION_SYNTAX_ERROR;
            LOG_WARN("type incompatibility to compare.", K(ret));
          } else {
            int cmp_res = -3;
            if (OB_SUCC(left_arg->compare((*hit), cmp_res, true))) {
              cmp_based_on_node_type(node_type, cmp_res, cmp_result);
            }
          }
        } else {
          ret = OB_ERR_JSON_PATH_EXPRESSION_SYNTAX_ERROR;
          LOG_WARN("type incompatibility to compare.", K(ret));
        }
        break;
      }
      case JPN_DATE:
      case JPN_TIMESTAMP: {
        if (is_json_date(left_type)) {
          int cmp_res = -3;
          if (OB_SUCC(left_arg->compare((*hit), cmp_res, true))) {
            cmp_based_on_node_type(node_type, cmp_res, cmp_result);
          }
        } else if (left_type == ObJsonNodeType::J_STRING) {
          ObString str(left_arg->get_data_length(), left_arg->get_data());
          if (OB_FAIL(trans_to_date_timestamp(allocator, str, left_arg,
                      last_sub_path_node_type == JPN_DATE))) {
            ret = OB_ERR_JSON_PATH_EXPRESSION_SYNTAX_ERROR;
            LOG_WARN("type incompatibility to compare.", K(ret));
          } else {
            int cmp_res = -3;
            if (OB_SUCC(left_arg->compare((*hit), cmp_res, true))) {
              cmp_based_on_node_type(node_type, cmp_res, cmp_result);
            }
          }
        } else {
          ret = OB_ERR_JSON_PATH_EXPRESSION_SYNTAX_ERROR;
          LOG_WARN("type incompatibility to compare.", K(ret));
        }
        break;
      }
      case JPN_LOWER:
      case JPN_STRING:
      case JPN_STR_ONLY:
      case JPN_TYPE:
      case JPN_UPPER: {
        if (is_json_string(left_type)) {
          int cmp_res = -3;
          if (OB_SUCC(left_arg->compare((*hit), cmp_res, true))) {
            cmp_based_on_node_type(node_type, cmp_res, cmp_result);
          }
        } else {
          ret = OB_ERR_JSON_PATH_EXPRESSION_SYNTAX_ERROR;
          LOG_WARN("type incompatibility to compare.", K(ret));
        }
        break;
      }
      default: {
        ret = OB_ERR_JSON_PATH_EXPRESSION_SYNTAX_ERROR;
        LOG_WARN("type incompatibility to compare.", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    filter_result = cmp_result;
  }
  return ret;
}

int ObIJsonBase::get_sign_result_left_subpath(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                                              const ObJsonPathFilterNode *path_node, bool& filter_result,
                                              ObIJsonBase* right_arg, PassingMap* sql_var) const
{
  INIT_SUCC(ret);
  if (OB_ISNULL(right_arg)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("compare value is null.", K(ret));
  } else {
    ObPathComparison comp_content = path_node->node_content_.comp_;

    SMART_VAR (ObJsonSeekResult, hit) {
      ObJsonPath* sub_path = comp_content.comp_left_.filter_path_;

      // get left arg
      if (OB_FAIL(parent_info.parent_jb_->seek(allocator, (*sub_path),
                sub_path->path_node_cnt(), true, false, true, hit, sql_var))) {
        // 查找失败则直接将结果视为false
        filter_result = false;
      } else {
      // cmp to right arg
        // 没有找到结果 或 结果为object则返回false
        // 因为sub_path 和 sub_path 比较不合法，所以不存在可以和object比较的类型
        if (hit.size() == 0  || (hit.size() == 1 && hit[0]->json_type() == ObJsonNodeType::J_OBJECT)) {
          filter_result = false;
        } else {
          ObJsonPathNodeType last_path_node_type = sub_path->get_last_node_type();
          if (last_path_node_type > JPN_BEGIN_FUNC_FLAG && last_path_node_type < JPN_END_FUNC_FLAG) {
            // subpath最后一个节点是item_function, 对比较类型的检查会有更严格的要求
            for (uint32_t i = 0; i < hit.size() && (!OB_ISNULL(hit[i]))
                                && !filter_result && OB_SUCC(ret); ++i) {
              if (OB_FAIL(cmp_to_right_strictly(allocator, hit[i], path_node->get_node_type(),
                          last_path_node_type, right_arg, filter_result))) {
                  LOG_WARN("fail to compare.", K(ret));
              }
            }
          } else {
            if (OB_FAIL(cmp_to_right_recursively(allocator, hit, path_node->get_node_type(),
                        right_arg, filter_result))) {
              LOG_WARN("fail to compare.", K(ret));
            }
          }
        }
      } // seek sub_path success
    } // end smart_var hit
  }

  return ret;
}

int ObIJsonBase::get_sign_result_right_subpath(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                                              const ObJsonPathFilterNode *path_node, bool& filter_result,
                                              ObIJsonBase* left_arg, PassingMap* sql_var) const
{
  INIT_SUCC(ret);
  if (OB_ISNULL(left_arg)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("compare value is null.", K(ret));
  } else {
    ObPathComparison comp_content = path_node->node_content_.comp_;

    SMART_VAR (ObJsonSeekResult, hit) {
      ObJsonPath* sub_path = comp_content.comp_right_.filter_path_;
      // get right arg
      if (OB_FAIL(parent_info.parent_jb_->seek(allocator, (*sub_path),
                sub_path->path_node_cnt(), true, false, true, hit, sql_var))) {
        // 查找失败则直接将结果视为false
        filter_result = false;
      } else {
      // cmp to left arg
        // 没有找到结果 或 结果为object则返回false
        // 因为sub_path 和 sub_path 比较不合法，所以不存在可以和object比较的类型
        if (hit.size() == 0  || (hit.size() == 1 && hit[0]->json_type() == ObJsonNodeType::J_OBJECT)) {
          filter_result = false;
        } else {
          ObJsonPathNodeType last_path_node_type = sub_path->get_last_node_type();
          if (last_path_node_type > JPN_BEGIN_FUNC_FLAG && last_path_node_type < JPN_END_FUNC_FLAG)
          {
            // subpath最后一个节点是item_function, 对比较类型的检查会有更严格的要求
            for (uint32_t i = 0; i < hit.size() && (!OB_ISNULL(hit[i])) && !filter_result && OB_SUCC(ret); ++i) {
              if (OB_FAIL(cmp_to_left_strictly(allocator, hit[i], path_node->get_node_type(),
                          last_path_node_type, left_arg, filter_result))) {
                  LOG_WARN("fail to compare.", K(ret));
              }
            }
          } else {
            if (OB_FAIL(cmp_to_left_recursively(allocator, hit, path_node->get_node_type(),
                        left_arg, filter_result))) {
              LOG_WARN("fail to compare.", K(ret));
            }
          }
        }
      } // seek success
    } // end smart_var hit
  }
  return ret;
}

int ObIJsonBase::get_scalar(ObIAllocator* allocator, const ObJsonPathNodeType type,
                            const ObPathScalar scalar_content, ObIJsonBase* &scalar) const
{
  INIT_SUCC(ret);
  switch (type) {
    case JPN_BOOL_TRUE: {
      ObJsonBoolean* tmp_ans = static_cast<ObJsonBoolean*> (allocator->alloc(sizeof(ObJsonBoolean)));
      if (OB_ISNULL(tmp_ans)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate row buffer failed at ObJsonDecimal", K(ret));
      } else {
        tmp_ans = new (tmp_ans) ObJsonBoolean(true);
        scalar = tmp_ans;
      }
      break;
    }
    case JPN_BOOL_FALSE: {
      ObJsonBoolean* tmp_ans = static_cast<ObJsonBoolean*> (allocator->alloc(sizeof(ObJsonBoolean)));
      if (OB_ISNULL(tmp_ans)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate row buffer failed at ObJsonDecimal", K(ret));
      } else {
        tmp_ans = new (tmp_ans) ObJsonBoolean(false);
        scalar = tmp_ans;
      }
      break;
    }
    case JPN_NULL: {
      ObJsonNull* tmp_ans = static_cast<ObJsonNull*> (allocator->alloc(sizeof(ObJsonNull)));
      if (OB_ISNULL(tmp_ans)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate row buffer failed at ObJsonDecimal", K(ret));
      } else {
        tmp_ans = new (tmp_ans) ObJsonNull();
        scalar = tmp_ans;
      }
      break;
    }
    case JPN_SCALAR: {
      ObIJsonBase *j_tree = NULL;
      ObString j_text(scalar_content.s_length_, scalar_content.scalar_);
      if (OB_FAIL(ObJsonBaseFactory::get_json_base(allocator, j_text,
          ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("fail to parse j_text", K(ret), K(j_text));
      } else {
        scalar = j_tree;
      }
      break;
    }
    default:{
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("supposed to be scalar", K(ret), K(type));
    }
  }
  return ret;
}

// scalar对比不会自动转换类型，如: 123 == "123"会报错(type incompatibility for comparison)
// 但 1.sub_path查找到的内容和scalar/sql对比时会自动转换类型
//    2.对于bool类型，如果是字符串会自动转换类型对比，其他均不会
// @return Less than returns -1, greater than 1, equal returns 0.
int ObIJsonBase::compare_scalar(ObIAllocator* allocator, const ObJsonPathFilterNode *path_node,
                                bool& filter_result) const
{
  INIT_SUCC(ret);
  ObJsonPathNodeType left_type = path_node->node_content_.comp_.left_type_;
  ObJsonPathNodeType right_type = path_node->node_content_.comp_.right_type_;
  int res = -3;
  switch (left_type) {
    case JPN_NULL: {
      if (right_type == JPN_NULL) {
        res = 0;
        cmp_based_on_node_type(path_node->get_node_type(), res, filter_result);
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("type incompatibility for comparison", K(ret),K(left_type),K(right_type));
      }
      break;
    }

    case JPN_BOOL_TRUE:
    case JPN_BOOL_FALSE: {
      if (right_type == JPN_BOOL_TRUE || right_type == JPN_BOOL_FALSE) {
        if (right_type == left_type) res = 0;
        cmp_based_on_node_type(path_node->get_node_type(), res, filter_result);
      } else if (right_type == JPN_SCALAR) {
        ObString str(path_node->node_content_.comp_.comp_right_.path_scalar_.s_length_,
                    path_node->node_content_.comp_.comp_right_.path_scalar_.scalar_);
        if (str.length() == strlen("\"true\"") && 0 == strncasecmp(str.ptr(), "\"true\"", strlen("\"true\""))) {
          if (left_type == JPN_BOOL_TRUE) res = 0;
          cmp_based_on_node_type(path_node->get_node_type(), res, filter_result);
        } else if (str.length() == strlen("\"false\"") && 0 == strncasecmp(str.ptr(), "\"false\"", strlen("\"false\""))) {
          if (left_type == JPN_BOOL_FALSE) res = 0;
          cmp_based_on_node_type(path_node->get_node_type(), res, filter_result);
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("type incompatibility for comparison", K(ret),K(left_type),K(right_type));
        }
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("type incompatibility for comparison", K(ret),K(left_type),K(right_type));
      }
      break;
    }

    case JPN_SCALAR: {
      if (right_type == JPN_SCALAR) {
        ObIJsonBase *left_s = NULL;
        ObIJsonBase *right_s = NULL;
        if (OB_FAIL(get_scalar(allocator, left_type, path_node->node_content_.comp_.comp_left_.path_scalar_, left_s))
          || OB_FAIL(get_scalar(allocator, right_type, path_node->node_content_.comp_.comp_right_.path_scalar_, right_s))) {
          LOG_WARN("fail to get scalar.", K(ret), K(right_type));
        } else {
          if ((is_json_number(left_s->json_type()) && is_json_number(right_s->json_type()))
            || (is_json_string(left_s->json_type()) && is_json_string(right_s->json_type()))) {
            int cmp_res = -3;
            if (OB_SUCC(left_s->compare((*right_s), cmp_res, true))) {
              cmp_based_on_node_type(path_node->get_node_type(), cmp_res, filter_result);
            } else {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("type incompatibility for comparison", K(ret),K(left_type),K(right_type));
            }
          } else {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("type incompatibility for comparison", K(ret),K(left_type),K(right_type));
          }
        }
      } else if (right_type == JPN_BOOL_TRUE || right_type == JPN_BOOL_FALSE ) {
        ObString str(path_node->node_content_.comp_.comp_left_.path_scalar_.s_length_,
                    path_node->node_content_.comp_.comp_left_.path_scalar_.scalar_);
        if (str.length() == strlen("\"true\"") && 0 == strncasecmp(str.ptr(), "\"true\"", strlen("\"true\""))) {
          if (right_type == JPN_BOOL_TRUE) res = 0;
          cmp_based_on_node_type(path_node->get_node_type(), res, filter_result);
        } else if (str.length() == strlen("\"false\"") && 0 == strncasecmp(str.ptr(), "\"false\"", strlen("\"false\""))) {
          if (right_type == JPN_BOOL_FALSE) res = 0;
          cmp_based_on_node_type(path_node->get_node_type(), res, filter_result);
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("type incompatibility for comparison", K(ret),K(left_type),K(right_type));
        }
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("type incompatibility for comparison", K(ret),K(left_type),K(right_type));
      }
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("should be scalar.", K(ret), K(left_type));
    }
  }
  return ret;
}

int ObIJsonBase::get_sign_comp_result(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                                      const ObJsonPathFilterNode *path_node, bool& filter_result,
                                      PassingMap* sql_var) const
{
  INIT_SUCC(ret);
  // possible combination: (subpath, scalar/sql_var)
  //                       (scalar, subpath/scalar)
  //                       (sql_var, subpath)
  // for scalar: bool, null, number, str(string/date)
  ObPathComparison comp_content = path_node->node_content_.comp_;

  // left is subpath, right could be scalar/sql_var
  if (comp_content.left_type_ == ObJsonPathNodeType::JPN_SUB_PATH) {
  // 如果右边是scalar则直接将其转变为ObIJsonBase
    if (ObJsonPathUtil::is_scalar(comp_content.right_type_)) {
      ObIJsonBase* scalar = NULL;
      if (OB_FAIL(get_scalar(allocator, comp_content.right_type_,
                            comp_content.comp_right_.path_scalar_, scalar))
                  || OB_ISNULL(scalar)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("fail to get scalar.", K(ret));
      } else {
        if (OB_FAIL(get_sign_result_left_subpath(allocator, parent_info, path_node,
                                                filter_result, scalar))) {
          LOG_WARN("fail to compare.", K(ret));
        }
      }
    } else if (comp_content.right_type_ == ObJsonPathNodeType::JPN_SQL_VAR) {
    // 右边是sql_var，根据var_name得到sql
      if (OB_ISNULL(sql_var) ) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("sql_var is nullptr.", K(ret));
      } else {
        // get sql_var
        ObString var_name(comp_content.comp_right_.path_var_.v_length_, comp_content.comp_right_.path_var_.var_);
        ObIJsonBase* var = NULL;
        if (OB_FAIL(sql_var->get_refactored(var_name, var)) || OB_ISNULL(var)) {
          filter_result = false;
        } else {
          if (OB_FAIL(get_sign_result_left_subpath(allocator, parent_info, path_node,
                                                  filter_result, var))) {
            LOG_WARN("fail to compare.", K(ret));
          }
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("wrong node type.", K(ret));
    }
  // (scalar, subpath) 或 (sql_var, subpath)
  } else if (comp_content.right_type_ == ObJsonPathNodeType::JPN_SUB_PATH) {
    // 左边是scalar
    if (ObJsonPathUtil::is_scalar(comp_content.left_type_)) {
      ObIJsonBase* scalar = NULL;
      if (OB_FAIL(get_scalar(allocator, comp_content.left_type_,
                            comp_content.comp_left_.path_scalar_, scalar))
                  || OB_ISNULL(scalar)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("fail to get scalar.", K(ret));
      } else {
        if (OB_FAIL(get_sign_result_right_subpath(allocator, parent_info, path_node,
                                                  filter_result, scalar))) {
          LOG_WARN("fail to compare.", K(ret));
        }
      }
    } else if (comp_content.left_type_ == ObJsonPathNodeType::JPN_SQL_VAR) {
    // 左边是sql_var, 根据变量名得到对应ObIJsonBase
      if (OB_ISNULL(sql_var) ) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("sql_var is nullptr.", K(ret));
      } else {
        // get sql_var
        ObString var_name(comp_content.comp_left_.path_var_.v_length_, comp_content.comp_left_.path_var_.var_);
        ObIJsonBase* var = NULL;
        if (OB_FAIL(sql_var->get_refactored(var_name, var)) || OB_ISNULL(var)) {
          filter_result = false;
        } else {
          if (OB_FAIL(get_sign_result_right_subpath(allocator, parent_info, path_node,
                                                    filter_result, var))) {
            LOG_WARN("fail to compare.", K(ret));
          }
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("wrong node type.", K(ret));
    }
  } else if (ObJsonPathUtil::is_scalar(comp_content.left_type_)
            && ObJsonPathUtil::is_scalar(comp_content.right_type_)) {
    // null 只能和 null 比
    // bool 可以和字符串 bool 比
    // string 和 string 比
    if (OB_FAIL(compare_scalar(allocator, path_node, filter_result))) {
      LOG_WARN("fail to compare scalar.", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wrong node type.", K(ret));
  }
  return ret;
}

bool ObIJsonBase::has_sub_string(const ObString& left, const ObString& right) const
{
  bool ret_bool = false;
  uint64_t l_len = left.length();
  uint64_t r_len = right.length();

  if (l_len < r_len) {
    ret_bool = false;
  } else if(l_len == r_len) {
    ret_bool = (left == right);
  } else {
    int64_t idx = l_len - r_len;
    for (int64_t i = 0; i <= idx && !ret_bool; ++i) {
      ObString tmp(left.length() - i, left.ptr() + i);
      ret_bool = tmp.prefix_match(right);
    }
  }

  return ret_bool;
}

int ObIJsonBase::str_comp_predicate(const ObString& left, const ObString& right,
                                    const ObJsonPathFilterNode *path_node, bool& filter_result) const
{
  INIT_SUCC(ret);
  ObJsonPathNodeType pnode_type = path_node->get_node_type();
  // 处理空字符串
  if (left.length() == 0 && right.length() == 0) {
    filter_result = true;
  } else if (left.length() == 0) {
    filter_result = false;
  } else if (right.length() == 0) {
    if (pnode_type == ObJsonPathNodeType::JPN_LIKE_REGEX) {
      filter_result = true;
    } else {
      filter_result = false;
    }
  } else { // both left_arg and right_arg is not null
    switch (pnode_type) {
      case JPN_SUBSTRING: {
        filter_result = has_sub_string(left, right);
        break;
      }
      case JPN_STARTS_WITH: {
        filter_result = (left.prefix_match(right) ? true : false);
        break;
      }
      case JPN_LIKE: {
        break;
      }
      case JPN_LIKE_REGEX: {
        break;
      }
      case JPN_EQ_REGEX: {
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("should be str_comp_predicate.", K(ret), K(pnode_type));
        break;
      }
    }
  }
  return ret;
}

int ObIJsonBase::str_cmp_autowrap(ObIAllocator* allocator, const ObString& right_str,
                                  const ObJsonPathFilterNode *path_node,
                                  bool autowrap, bool& filter_result) const
{
  INIT_SUCC(ret);
  ObJsonNodeType j_type = json_type();
  ObJsonBin st_json(allocator_); // use stack variable instead of deep copy
  ObIJsonBase* jb_ptr = NULL;
  if (j_type == ObJsonNodeType::J_NULL && is_real_json_null(this)) {
    filter_result = false;
  } else if (j_type == ObJsonNodeType::J_ARRAY) {
    if (autowrap) {
      for (uint32_t i = 0; OB_SUCC(ret) && i < element_count() && !filter_result; ++i) {
        jb_ptr = &st_json; // reset jb_ptr to stack var
        ret = get_array_element(i, jb_ptr);
        if (OB_ISNULL(jb_ptr)) {
          ret = OB_ERR_NULL_VALUE;
          LOG_WARN("fail to get array child dom", K(ret), K(i));
        // 数组只对第一层展开查询
        } else if (OB_FAIL(jb_ptr->str_cmp_autowrap(allocator, right_str,
                                                    path_node, false, filter_result))) {
          LOG_WARN("fail to cmp recursively", K(ret), K(i), K(filter_result));
        }
      }
    } else {
      filter_result = false;
    }
  } else if (is_json_string(j_type)) {
    ObString left_str(get_data_length(), get_data());
    if (OB_FAIL(str_comp_predicate(left_str, right_str, path_node, filter_result))) {
      LOG_WARN("fail to compare(str).", K(ret));
    }
  } else {
    ObJsonBuffer j_buf(allocator);
    if (OB_FAIL(print(j_buf, true))) {
      LOG_WARN("fail to get string of hit[i].", K(ret), K(j_type));
    } else {
      ObString left_str(j_buf.length(), j_buf.ptr());
      if (OB_FAIL(str_comp_predicate(left_str, right_str, path_node, filter_result))) {
        LOG_WARN("fail to compare(str).", K(ret));
      }
    } // cast to string and compare
  }
  return ret;
}

int ObIJsonBase::get_str_comp_result(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                                    const ObJsonPathFilterNode *path_node, bool& filter_result,
                                    PassingMap* sql_var) const
{
  INIT_SUCC(ret);
  ObPathComparison comp_content = path_node->node_content_.comp_;
  bool end_comp = false;
  // could be:(sub_path，string), (sub_path, var) ,(string, string)
  // get right_str, right arg could be scalar/var
  ObString right_str;
  if (comp_content.right_type_  == ObJsonPathNodeType::JPN_SCALAR) {
    ObString tmp(comp_content.comp_right_.path_scalar_.s_length_, comp_content.comp_right_.path_scalar_.scalar_);
    // 确定是字符串类型
    if (tmp.length() < 2 || tmp[0] != '\"' || tmp[tmp.length()-1] != '\"') {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("right scalar must be string.", K(ret));
    } else {
      right_str = ObString(tmp.length() - 2, tmp.ptr() + 1);
    }
  } else if (comp_content.right_type_ == ObJsonPathNodeType::JPN_SQL_VAR) {
    // 左边是sql_var, 根据变量名得到对应ObIJsonBase
    if (OB_ISNULL(sql_var) ) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("sql_var is nullptr.", K(ret));
    } else {
      // get sql_var
      ObString var_name(comp_content.comp_right_.path_var_.v_length_, comp_content.comp_right_.path_var_.var_);
      ObIJsonBase* var = NULL;
      if (OB_FAIL(sql_var->get_refactored(var_name, var)) || OB_ISNULL(var)) {
        filter_result = false;
        end_comp = true;
      } else if (is_json_string(var->json_type())) {
        right_str = ObString(var->get_data_length(), var->get_data());
      } else {
        ObJsonBuffer j_buf(allocator);
        if (OB_FAIL(var->print(j_buf, true, false, 0))) {
          LOG_WARN("fail to get string of sql_var.", K(ret));
        } else {
          right_str = ObString(j_buf.length(), j_buf.ptr());
        }
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wrong node type of right string comparison arg.", K(ret));
  }

  if (OB_SUCC(ret) && !end_comp && comp_content.left_type_ == ObJsonPathNodeType::JPN_SUB_PATH) {
    // compare recursively
    SMART_VAR (ObJsonSeekResult, hit) {
      ObJsonPath* sub_path = comp_content.comp_left_.filter_path_;
      if (sub_path->path_not_str()) {
        // 如果最后一个节点的类型是item function，且返回值一定不为string，如number/abs/length...会报错
        ret = OB_OP_NOT_ALLOW;
        LOG_WARN("wrong argument data type for function call.",
                  K(ret), K(sub_path->get_last_node_type()));
      } else {
        if (OB_FAIL(parent_info.parent_jb_->seek(allocator, (*sub_path),
                    sub_path->path_node_cnt(), true, false, true, hit, sql_var))) {
          LOG_WARN("fail to seek sub_path.", K(ret));
        } else {
          for (int i = 0; OB_SUCC(ret) && i < hit.size() && !OB_ISNULL(hit[i]) && !filter_result; ++i) {
            if (OB_FAIL(hit[i]->str_cmp_autowrap(allocator, right_str, path_node,
                                                  true, filter_result))) {
              LOG_WARN("fail to cmp_autowrap(string).", K(ret),K(i));
            }
          } // seek autowrap
        } // success: seek sub_path
      } // seek for compare
    } // samrt var
  } else if (comp_content.left_type_  == ObJsonPathNodeType::JPN_SCALAR) {
    ObString tmp(comp_content.comp_left_.path_scalar_.s_length_, comp_content.comp_left_.path_scalar_.scalar_);
    // 确定是字符串类型
    if (tmp.length() < 2 || tmp[0] != '\"' || tmp[tmp.length()-1] != '\"') {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("right scalar must be string.", K(ret));
    } else {
      ObString left_str(tmp.length() - 2, tmp.ptr() + 1);
      if (OB_FAIL(str_comp_predicate(left_str, right_str, path_node, filter_result))) {
        LOG_WARN("fail to compare(str).", K(ret));
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the left_arg of string_comp_predicate should sub_path or scalar.",
              K(ret), K(comp_content.left_type_));
  }

  return ret;
}

int ObIJsonBase::find_comp_result(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                                  const ObJsonPathFilterNode *path_node,
                                  bool& filter_result, PassingMap* sql_var) const
{
  INIT_SUCC(ret);
  filter_result = false;
  ObJsonPathNodeType pnode_type = path_node->get_node_type();

  if (pnode_type > JPN_BEGIN_FILTER_FLAG && pnode_type < JPN_SUBSTRING) {
    // have both left and right arg
    if (OB_FAIL(get_sign_comp_result(allocator, parent_info, path_node,
                                      filter_result, sql_var))) {
      LOG_WARN("fail to get filter_result.", K(ret), K(pnode_type));
    }
  } else if (pnode_type >= JPN_SUBSTRING && pnode_type <= JPN_EQ_REGEX) {
    if (OB_FAIL(get_str_comp_result(allocator, parent_info, path_node,
                                    filter_result, sql_var))) {
      LOG_WARN("fail to get filter_result.", K(ret), K(pnode_type));
    }
  } else if (pnode_type == JPN_EXISTS || pnode_type == JPN_NOT_EXISTS) {
    // only have right arg
    if (path_node->node_content_.comp_.right_type_ != JPN_SUB_PATH) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("wrong argument of exists.", K(ret), K(pnode_type));
    } else {
      ObJsonPath* sub_path = path_node->node_content_.comp_.comp_right_.filter_path_;
      ObJsonPathNodeType last_node_type = sub_path->get_last_node_type();
      SMART_VAR (ObJsonSeekResult, hit) {
        if (OB_FAIL(parent_info.parent_jb_->seek(allocator, (*sub_path),
                    sub_path->path_node_cnt(), true, true, true, hit, sql_var))) {
        // 查找失败则直接将结果视为false
            filter_result = false;
        } else {
          // exist 要求参数一定是含有过滤表达式的sub_path, 过滤表达式后面再有其他path_node没问题
          // 并且从过滤表达式所找到的jb_tree后往下继续按照path查找
          // 但特殊的是，如果过滤表达式的结果为false或没有找到，过滤表达式后面的path_node都不再继续执行
          /*
          1. SELECT json_value(
                '["abc", 2, 3, 4, 5, {"z" : "2020-02-02"},7,8,"9"]',
                '$[5]?(1 == 0).z.date()' RETURNING DATE ) FROM dual;            // 输出null

          2. SELECT json_value(
                          '["abc", 2, 3, 4, 5, {"z" : "2020-02-02"},7,8,"9"]',
                          '$[5]?(1 == 1).z.date()' RETURNING DATE ) FROM dual;  // 输出正确日期

          3. SELECT json_value(
                '["abc", 2, 3, 4, 5, {"z" : "2020-02-02"},7,8,"9"]',
                '$[5]?(@.z.a == "2020-02-02").z.date()' RETURNING DATE ) FROM dual; // 输出null
          4. SELECT json_value(
                '["abc", 2, 3, 4, 5, {"z" : "2020-02-02"},7,8,"9"]',
                '$[5]?(@.z == "2020-02-02").z.date()' RETURNING DATE ) FROM dual; // 输出正确日期

          5. SELECT 1 from dual
                WHERE json_exists(
                  '["a", 2, 3, 4, 5, {"resolution" : {"x": 1920, "y": 1080}}, 7, 8, 9]',
                  '$[5]?(1 == 1).resolution.z');                                  // 输出空
          6. SELECT 1 from dual
                WHERE json_exists(
                  '["a", 2, 3, 4, 5, {"resolution" : {"x": 1920, "y": 1080}}, 7, 8, 9]',
                  '$[5]?(1 == 1).resolution.x');                                 // 输出1
          */
          // 所以猜测oracle在seek()过程中，会根据过滤表达式的结果(找不到或不匹配均为false)确定是否继续向下执行。
          // 且根据用例5 & 6猜测，Oracle查找完过滤表达式节点后并不会将结果(true/false)插入res，否则5的结果不会为空
          // 而是 根据过滤表达式的结果决定是否往下继续查询
          filter_result = (hit.size() > 0);
        }
      } // smart var hit

      if (pnode_type == JPN_NOT_EXISTS) {
        filter_result = !filter_result;
      }
    } // end of exists or !exists
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("should be comp_path_node.", K(ret), K(pnode_type));
  }
  return ret;
}

int ObIJsonBase::get_half_ans(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                              ObJsonPathNodeType node_type, const ObJsonPathFilterNode *path_node,
                              bool& filter_result, PassingMap* sql_var) const
{
  INIT_SUCC(ret);
  if (node_type > JPN_BEGIN_FILTER_FLAG && node_type < JPN_AND_COND) {
    if (OB_FAIL(find_comp_result(allocator, parent_info,
                                path_node, filter_result, sql_var))) {
      LOG_WARN("fail to get comp_result.", K(ret));
    }
  } else if (node_type >= JPN_AND_COND && node_type < JPN_END_FILTER_FLAG) {
    if (OB_FAIL(find_cond_result(allocator, parent_info,
                                path_node, filter_result, sql_var))) {
      LOG_WARN("fail to get comp_result.", K(ret));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("should be filter_path_node.", K(ret), K(path_node->get_node_type()));
  }
  return ret;
}

int ObIJsonBase::find_cond_result(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                                  const ObJsonPathFilterNode *path_node,
                                  bool& filter_result, PassingMap* sql_var) const
{
  INIT_SUCC(ret);
  bool left_ans = false;
  bool right_ans = false;
  ObJsonPathFilterNode* cond_left = path_node->node_content_.cond_.cond_left_;
  ObJsonPathFilterNode* cond_right = path_node->node_content_.cond_.cond_right_;
  ObJsonPathNodeType right_type = cond_right->get_node_type();
  if (path_node->get_node_type() == JPN_NOT_COND) {
    if (OB_FAIL(get_half_ans(allocator, parent_info, right_type,
                              cond_right, right_ans, sql_var))) {
      LOG_WARN("fail to get right_result.", K(ret));
    } else {
      filter_result = !right_ans;
    }
  } else if (path_node->get_node_type() == JPN_AND_COND
          || path_node->get_node_type() == JPN_OR_COND) {
    // get left ans
    ObJsonPathNodeType left_type = cond_left->get_node_type();
    if (OB_FAIL(get_half_ans(allocator, parent_info, left_type,
                            cond_left, left_ans, sql_var))) {
      LOG_WARN("fail to get left_result.", K(ret));
    } else {
      if (left_ans == true && path_node->get_node_type() == JPN_OR_COND) {
        filter_result = true;
      } else if (left_ans == false && path_node->get_node_type() == JPN_AND_COND) {
        filter_result = false;
      } else {
        // get right ans
        if (OB_FAIL(get_half_ans(allocator, parent_info, right_type,
                                cond_right, right_ans, sql_var))) {
          LOG_WARN("fail to get right_result.", K(ret));
        } else {
          if (path_node->get_node_type() == JPN_AND_COND) {
            filter_result = (left_ans && right_ans);
          } else {
            filter_result = (left_ans || right_ans);
          }
        }
      }
    }

  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("should be cond_path_node.", K(ret), K(path_node->get_node_type()));
  }
  return ret;
}
int ObIJsonBase::find_filter_child(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                                  const JsonPathIterator &cur_node, const JsonPathIterator &last_node,
                                  bool is_auto_wrap, bool only_need_one, bool is_lax,
                                  ObJsonSortedResult &dup, ObJsonSeekResult &res,
                                  PassingMap* sql_var) const
{
  INIT_SUCC(ret);
  ObJsonPathFilterNode*  path_node = static_cast<ObJsonPathFilterNode*>(*cur_node);
  bool filter_result = false;
  ObJsonPathNodeType pnode_type = path_node->get_node_type();
  if (is_lax && !is_auto_wrap) is_auto_wrap = true;

  // 对于过滤表达式，需要重新设置parent_info
  // 当前json节点为新的parent节点，过滤表达式内的sub_path都以该节点为根节点查找
  parent_info.parent_jb_ = const_cast<ObIJsonBase*> (this);
  parent_info.parent_path_ = cur_node;
  parent_info.is_subpath_ = true;
  if (pnode_type >= JPN_AND_COND) {
    if (OB_FAIL(find_cond_result(allocator, parent_info,
                                path_node, filter_result, sql_var))) {
      LOG_WARN("fail to get cond_result.", K(ret));
    }
  } else {
    //  comparison(==, >,...,<, !=) or condition
    //  TODO: followed comparison type dosen't support yet
    /*
        JPN_SUBSTRING, // has substring
        JPN_STARTS_WITH, // start with
        JPN_LIKE, // like
        JPN_LIKE_REGEX, // like_regex
        JPN_EQ_REGEX, // eq_regex
    */
    if (OB_FAIL(find_comp_result(allocator, parent_info,
                                path_node, filter_result, sql_var))) {
      LOG_WARN("fail to get comp_result.", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (filter_result == true) {
    // 结果为true但还没有结束，则从当前节点继续往下执行
      if (OB_FAIL(find_child(allocator, parent_info, cur_node + 1, last_node,
                            is_auto_wrap, only_need_one, is_lax, dup, res, sql_var))) {
        LOG_WARN("fail to seek recursively", K(ret));
      }
    } else {
      // 结果为false，不再继续往下查找，直接返回空的hit数组
      // do nothing
    }
  }

  return ret;
}

int ObIJsonBase::find_child(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                            const JsonPathIterator &cur_node, const JsonPathIterator &last_node,
                            bool is_auto_wrap, bool only_need_one, bool is_lax,
                            ObJsonSortedResult &dup, ObJsonSeekResult &res,
                            PassingMap* sql_var) const
{
  INIT_SUCC(ret);

  // If the path expression is already at the end, the current DOM is the res,
  // and it is added to the res
  if (cur_node == last_node) {
    ret = add_if_missing(dup, res, allocator);
  } else {
    ObJsonNodeType cur_json_type = json_type();
    ObJsonPathNodeType cur_node_type = (*cur_node)->get_node_type();

    // is basic_node
    if (cur_node_type > ObJsonPathNodeType::JPN_BEGIN_BASIC_FLAG
        && cur_node_type < ObJsonPathNodeType::JPN_END_BASIC_FLAG) {
      if (OB_FAIL(find_basic_child(allocator, parent_info, cur_node, last_node,
                                  is_auto_wrap,only_need_one, is_lax, dup, res, sql_var))) {
        LOG_WARN("fail to find basic child.", K(ret));
      }
    // is fun_node
    } else if (cur_node_type > ObJsonPathNodeType::JPN_BEGIN_FUNC_FLAG
              && cur_node_type < ObJsonPathNodeType::JPN_END_FUNC_FLAG) {
      if (OB_FAIL (find_func_child(allocator, parent_info, cur_node, last_node,
                                  is_auto_wrap, only_need_one, is_lax, dup, res))) {
        LOG_WARN("fail to find fun child.", K(ret));
      }
    // is filter_node
    } else if (cur_node_type > ObJsonPathNodeType::JPN_BEGIN_FILTER_FLAG
              && cur_node_type < ObJsonPathNodeType::JPN_END_FILTER_FLAG) {
      if (OB_FAIL (find_filter_child(allocator, parent_info, cur_node, last_node,
                                    is_auto_wrap, only_need_one, is_lax, dup, res, sql_var))) {
        LOG_WARN("fail to find filter child.", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected path node type.", K(ret), K(cur_node_type));
    }
  }

  return ret;
}

int ObIJsonBase::print_jtime(ObJsonBuffer &j_buf, bool is_quoted) const
{
  INIT_SUCC(ret);

  if (is_quoted) {
    if (OB_FAIL(j_buf.append("\""))) {
      LOG_WARN("fail to append \"", K(ret), K(is_quoted));
    }
  }

  // ObTimeConverter::ob_time_to_str will lose precision.
  if (OB_SUCC(ret)) {
    ObDTMode tmode;
    ObJsonNodeType j_type = json_type();
    if (OB_FAIL(ObJsonBaseUtil::get_dt_mode_by_json_type(j_type, tmode))) {
      LOG_WARN("fail to get ObDTMode by json type", K(ret), K(j_type));
    } else {
      ObTime t(tmode);
      const int64_t tmp_buf_len = DATETIME_MAX_LENGTH + 1;
      char tmp_buf[tmp_buf_len] = {0};
      int64_t pos = 0;
      const int16_t print_scale = 6;
      if (OB_FAIL(get_obtime(t))) {
        LOG_WARN("fail to get json obtime", K(ret));
      } else if (OB_FAIL(ObTimeConverter::ob_time_to_str(t, tmode, print_scale,
                                                         tmp_buf, tmp_buf_len, pos, true))) {
        LOG_WARN("fail to change time to string", K(ret), K(j_type), K(t), K(pos));
      } else if (OB_FAIL(j_buf.append(tmp_buf))) {
        LOG_WARN("fail to append date_buf to j_buf", K(ret), K(j_type), KCSTRING(tmp_buf));
      } else if (is_quoted) {
        if (OB_FAIL(j_buf.append("\""))) {
          LOG_WARN("fail to append \"", K(ret), K(is_quoted));
        }
      }
    }
  }

  return ret;
}

int ObIJsonBase::print_array(ObJsonBuffer &j_buf, uint64_t depth, bool is_pretty) const
{
  INIT_SUCC(ret);

  if (OB_FAIL(j_buf.append("["))) {
    LOG_WARN("fail to append [", K(ret), K(depth), K(is_pretty));
  } else {
    uint64_t arr_size = element_count();
    for (uint64_t i = 0; OB_SUCC(ret) && i < arr_size; ++i) {
      if (i > 0 && OB_FAIL(ObJsonBaseUtil::append_comma(j_buf, is_pretty))) {
        LOG_WARN("fail to append comma", K(ret), K(is_pretty));
      } else if (is_pretty && OB_FAIL(ObJsonBaseUtil::append_newline_and_indent(j_buf, depth))) {
        LOG_WARN("fail to append newline and indent", K(ret), K(depth));
      } else {
        ObIJsonBase *jb_ptr = NULL;
        ObJsonBin j_bin(allocator_);
        jb_ptr = &j_bin;
        if (OB_FAIL(get_array_element(i, jb_ptr))) {
          LOG_WARN("fail to get array element", K(ret), K(depth), K(i));
        } else if (OB_FAIL(jb_ptr->print(j_buf, true, is_pretty, depth))) {
          LOG_WARN("fail to print json value to string", K(ret), K(i), K(is_pretty), K(depth));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (is_pretty && arr_size > 0
          && OB_FAIL(ObJsonBaseUtil::append_newline_and_indent(j_buf, depth - 1))) {
        LOG_WARN("fail to append newline and indent", K(ret), K(depth), K(arr_size));
      } else if (OB_FAIL(j_buf.append("]"))) {
        LOG_WARN("fail to append [", K(ret), K(depth), K(arr_size));
      }
    }
  }

  return ret;
}

int ObIJsonBase::pint_colon(ObJsonBuffer &j_buf, bool is_pretty) const
{
  INIT_SUCC(ret);
  if (lib::is_oracle_mode() && is_pretty) {
    if (OB_FAIL(j_buf.append(" : "))) {
      LOG_WARN("fail to append \" : \"", K(ret));
    }
  } else if (OB_FAIL(j_buf.append(":"))) {
    LOG_WARN("fail to append \":\"", K(ret));
  }
  return ret;
}

int ObIJsonBase::print_object(ObJsonBuffer &j_buf, uint64_t depth, bool is_pretty) const
{
  INIT_SUCC(ret);

  if (OB_FAIL(j_buf.append("{"))) {
    LOG_WARN("fail to append '{' to buffer", K(ret));
  } else {
    bool is_first = true;
    uint64_t count = element_count();
    for (uint64_t i = 0; OB_SUCC(ret) && i < count; i++) {
      if (!is_first && OB_FAIL(ObJsonBaseUtil::append_comma(j_buf, is_pretty))) {
        LOG_WARN("fail to append comma", K(ret), K(is_pretty));
      } else {
        is_first = false;
        ObString key;
        if (OB_FAIL(get_key(i, key))) {
          LOG_WARN("fail to get key", K(ret), K(i));
        } else if (is_pretty && OB_FAIL(ObJsonBaseUtil::append_newline_and_indent(j_buf, depth))) {
          LOG_WARN("fail to newline and indent", K(ret), K(depth), K(i), K(key));
        } else if (key.empty() && OB_FAIL(ObJsonBaseUtil::append_string(j_buf, false, "\"\"", 2))) {
          LOG_WARN("fail to newline and indent", K(ret), K(depth), K(i), K(key));
        } else if (!key.empty() && OB_FAIL(ObJsonBaseUtil::append_string(j_buf, true, key.ptr(), key.length()))) { // key
          LOG_WARN("fail to print string", K(ret), K(depth), K(i), K(key));
        } else if (OB_FAIL(pint_colon(j_buf, is_pretty))) {
          LOG_WARN("fail to append \":\"", K(ret), K(depth), K(i), K(key));
        } else if (lib::is_mysql_mode() && OB_FAIL(j_buf.append(" "))) {
          LOG_WARN("fail to append \" \"", K(ret), K(depth), K(i), K(key));
        } else {
          ObIJsonBase *jb_ptr = NULL;
          ObJsonBin j_bin(allocator_);
          jb_ptr = &j_bin;
          if (OB_FAIL(get_object_value(i, jb_ptr))) {
            LOG_WARN("fail to get object value", K(ret), K(i), K(is_pretty), K(depth));
          } else if (OB_FAIL(jb_ptr->print(j_buf, true, is_pretty, depth))) { // value
            LOG_WARN("fail to print json value to string", K(ret), K(i), K(is_pretty), K(depth));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (is_pretty && count > 0
          && OB_FAIL(ObJsonBaseUtil::append_newline_and_indent(j_buf, depth - 1))) {
        LOG_WARN("fail to newline and indent", K(ret), K(is_pretty), K(depth));
      } else if (OB_FAIL(j_buf.append("}"))) {
        LOG_WARN("fail to append \"}\" to buffer", K(ret), K(is_pretty), K(depth));
      }
    }
  }

  return ret;
}

int ObIJsonBase::print_decimal(ObJsonBuffer &j_buf) const
{
  INIT_SUCC(ret);
  int64_t pos = j_buf.length();

  if (OB_FAIL(j_buf.reserve(number::ObNumber::MAX_PRINTABLE_SIZE))) {
    LOG_WARN("fail to reserve memory for buf", K(ret));
  } else {
    ObScale scale = get_decimal_scale();
    number::ObNumber val = get_decimal_data();
    if (OB_FAIL(val.format_v2(j_buf.ptr(), j_buf.capacity(), pos, scale))) {
      LOG_WARN("fail to format decimal value", K(ret), K(val), K(j_buf.length()), K(pos));
    } else if (OB_FAIL(j_buf.set_length(pos))) {
      LOG_WARN("fail to set buf length", K(ret), K(pos), K(j_buf.length()));
    }
  }

  return ret;
}

struct ObFindDoubleEscapeFunc {
  ObFindDoubleEscapeFunc() {}

  bool operator()(const char c)
  {
    return c == '.' || c == 'e';
  }
};

int ObIJsonBase::print_double(ObJsonBuffer &j_buf) const
{
  INIT_SUCC(ret);

  if (OB_FAIL(j_buf.reserve(DOUBLE_TO_STRING_CONVERSION_BUFFER_SIZE + 1))) {
    LOG_WARN("fail to reserve memory for j_buf", K(ret));
  } else {
    double val = get_double();
    char *start = j_buf.ptr() + j_buf.length();
    uint64_t len = ob_gcvt(val, ob_gcvt_arg_type::OB_GCVT_ARG_DOUBLE,
        DOUBLE_TO_STRING_CONVERSION_BUFFER_SIZE, start, NULL);
    if (OB_FAIL(j_buf.set_length(j_buf.length() + len))) {
      LOG_WARN("fail to set j_buf len", K(ret), K(j_buf.length()), K(len));
    } else {
      // add ".0" in the end.
      ObFindDoubleEscapeFunc func;
      if (std::none_of(start, start + len, func)) {
        if (OB_FAIL(j_buf.append("."))) {
          LOG_WARN("fail to append '.' to buffer", K(ret), KCSTRING(start), K(len));
        } else if (OB_FAIL(j_buf.append("0"))) {
          LOG_WARN("fail to append '0' to buffer", K(ret), KCSTRING(start), K(len));
        }
      }
    }
  }

  return ret;
}

int ObIJsonBase::print_float(ObJsonBuffer &j_buf) const
{
  INIT_SUCC(ret);

  if (OB_FAIL(j_buf.reserve(FLOAT_TO_STRING_CONVERSION_BUFFER_SIZE + 1))) {
    LOG_WARN("fail to reserve memory for j_buf", K(ret));
  } else {
    double val = get_float();
    char *start = j_buf.ptr() + j_buf.length();
    uint64_t len = ob_gcvt(val, ob_gcvt_arg_type::OB_GCVT_ARG_FLOAT,
        FLOAT_TO_STRING_CONVERSION_BUFFER_SIZE, start, NULL);
    if (OB_FAIL(j_buf.set_length(j_buf.length() + len))) {
      LOG_WARN("fail to set j_buf len", K(ret), K(j_buf.length()), K(len));
    } else {
      // add ".0" in the end.
      ObFindDoubleEscapeFunc func;
      if (std::none_of(start, start + len, func)) {
        if (OB_FAIL(j_buf.append("."))) {
          LOG_WARN("fail to append '.' to buffer", K(ret), K(start), K(len));
        } else if (OB_FAIL(j_buf.append("0"))) {
          LOG_WARN("fail to append '0' to buffer", K(ret), K(start), K(len));
        }
      }
    }
  }

  return ret;
}

// only used in json opaque type.
// Notice: varchar has different mapping in mysql 5.7 (var_string)and 8.0(varchar)
static const obmysql::EMySQLFieldType opaque_ob_type_to_mysql_type[ObMaxType] =
{
  /* ObMinType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_NULL,          /* ObNullType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_TINY,          /* ObTinyIntType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_SHORT,         /* ObSmallIntType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_INT24,         /* ObMediumIntType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_LONG,          /* ObInt32Type */
  obmysql::EMySQLFieldType::MYSQL_TYPE_LONGLONG,      /* ObIntType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_TINY,          /* ObUTinyIntType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_SHORT,         /* ObUSmallIntType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_INT24,         /* ObUMediumIntType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_LONG,          /* ObUInt32Type */
  obmysql::EMySQLFieldType::MYSQL_TYPE_LONGLONG,      /* ObUInt64Type */
  obmysql::EMySQLFieldType::MYSQL_TYPE_FLOAT,         /* ObFloatType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_DOUBLE,        /* ObDoubleType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_FLOAT,         /* ObUFloatType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_DOUBLE,        /* ObUDoubleType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_NEWDECIMAL,    /* ObNumberType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_NEWDECIMAL,    /* ObUNumberType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_DATETIME,      /* ObDateTimeType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_TIMESTAMP,     /* ObTimestampType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_DATE,          /* ObDateType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_TIME,          /* ObTimeType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_YEAR,          /* ObYearType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING,    /* ObVarcharType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_STRING,        /* ObCharType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_VARCHAR,       /* ObHexStringType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_COMPLEX,       /* ObExtendType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_NOT_DEFINED,   /* ObUnknownType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_TINY_BLOB,     /* ObTinyTextType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_BLOB,          /* ObTextType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_MEDIUM_BLOB,   /* ObMediumTextType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_LONG_BLOB,     /* ObLongTextType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_BIT,           /* ObBitType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_STRING,        /* ObEnumType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_STRING,        /* ObSetType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_NOT_DEFINED,   /* ObEnumInnerType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_NOT_DEFINED,   /* ObSetInnerType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_OB_TIMESTAMP_WITH_TIME_ZONE,       /* ObTimestampTZType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_OB_TIMESTAMP_WITH_LOCAL_TIME_ZONE, /* ObTimestampLTZType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_OB_TIMESTAMP_NANO,                 /* ObTimestampNanoType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_OB_RAW,                            /* ObRawType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_OB_INTERVAL_YM,                    /* ObIntervalYMType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_OB_INTERVAL_DS,                    /* ObIntervalDSType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_OB_NUMBER_FLOAT,                   /* ObNumberFloatType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_OB_NVARCHAR2,                      /* ObNVarchar2Type */
  obmysql::EMySQLFieldType::MYSQL_TYPE_OB_NCHAR,                          /* ObNCharType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_OB_UROWID,
  obmysql::EMySQLFieldType::MYSQL_TYPE_ORA_BLOB,                          /* ObLobType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_JSON,                              /* ObJsonType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_GEOMETRY,                          /* ObGeometryType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_COMPLEX,                           /* ObUserDefinedSQLType, buf for xml we use long_blob type currently? */
  obmysql::EMySQLFieldType::MYSQL_TYPE_NEWDECIMAL,                        /* ObDecimalIntType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_COMPLEX,                           /* ObCollectionSQLType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_NOT_DEFINED,                       /* reserved for ObMySQLDateType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_NOT_DEFINED,                       /* reserved for ObMySQLDateTimeType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_BLOB,                              /* ObRoaringBitmapType */
  /* ObMaxType */
};

int ObIJsonBase::print_opaque(ObJsonBuffer &j_buf, uint64_t depth, bool is_quoted) const
{
  INIT_SUCC(ret);
  const char *data = get_data();
  uint64_t data_length = get_data_length();
  ObObjType f_type = field_type();

  if (f_type >= ObMaxType) {
    ret = OB_ERR_MAX_VALUE;
    LOG_WARN("fail to convert ob type to mysql type", K(ret), K(f_type));
  } else {
    const uint64_t need_len = ObBase64Encoder::needed_encoded_length(data_length);
    obmysql::EMySQLFieldType mysql_type = obmysql::EMySQLFieldType::MYSQL_TYPE_NOT_DEFINED;
    mysql_type = opaque_ob_type_to_mysql_type[f_type];
    char field_buf[ObFastFormatInt::MAX_DIGITS10_STR_SIZE] = {0};
    int64_t field_len = ObFastFormatInt::format_unsigned(mysql_type, field_buf);
    ObArenaAllocator tmp_allocator;
    ObJsonBuffer base64_buf(&tmp_allocator);

    // base64::typeXX:<binary data>
    if (OB_FAIL(base64_buf.append("base64:type"))) {
      LOG_WARN("fail to append \" base64:type \"", K(ret), K(depth));
    } else if (OB_FAIL(base64_buf.append(field_buf, field_len))) {
      LOG_WARN("fail to append field type", K(ret), K(depth), K(f_type));
    } else if (OB_FAIL(base64_buf.append(":"))) {
      LOG_WARN("fail to append \":\"", K(ret), K(depth));
    } else if (OB_FAIL(base64_buf.reserve(need_len))) {
      LOG_WARN("fail to reserve memory for base64_buf", K(ret), K(depth), K(need_len));
    } else {
      int64_t pos = 0;
      const uint8_t *input = reinterpret_cast<const uint8_t *>(data);
      if (OB_ISNULL(input)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("json string is null", K(ret));
      } else if (OB_FAIL(ObBase64Encoder::encode(input, data_length,
                                                 base64_buf.ptr() + base64_buf.length(),
                                                 need_len, pos))) {
        LOG_WARN("fail to encode", K(ret), K(depth), K(f_type), K(need_len), K(pos));
      } else if (OB_FAIL(base64_buf.set_length(base64_buf.length() + need_len))) {
        LOG_WARN("fail to set buf length", K(ret), K(depth), K(f_type), K(need_len), K(base64_buf));
      } else {
        if (is_quoted) {
          if (OB_FAIL(ObJsonBaseUtil::add_double_quote(j_buf, base64_buf.ptr(),
                                                       base64_buf.length()))) {
            LOG_WARN("fail to add double quote", K(ret), K(depth), K(f_type), K(base64_buf));
          }
        } else if (OB_FAIL(j_buf.append(base64_buf.ptr(), base64_buf.length()))) {
          LOG_WARN("fail to append base64_buf", K(ret), K(depth), K(f_type), K(base64_buf));
        }
      }
    }
    base64_buf.reset(); // free memory
  }

  return ret;
}

int ObIJsonBase::print(ObJsonBuffer &j_buf, bool is_quoted, bool is_pretty, uint64_t depth) const
{
  INIT_SUCC(ret);
  ObJsonNodeType j_type = json_type();

  // consistent with mysql 5.7
  // in mysql 8.0, varstring is handled as json string, obvarchartype is considered as varstring.
  switch (j_type) {
    case ObJsonNodeType::J_DATE:
    case ObJsonNodeType::J_TIME:
    case ObJsonNodeType::J_DATETIME:
    case ObJsonNodeType::J_TIMESTAMP:
    case ObJsonNodeType::J_ORACLEDATE:
    case ObJsonNodeType::J_ODATE:
    case ObJsonNodeType::J_OTIMESTAMP:
    case ObJsonNodeType::J_OTIMESTAMPTZ: {
      if (OB_FAIL(print_jtime(j_buf, is_quoted))) {
        LOG_WARN("fail to change jtime to string", K(ret), K(is_quoted), K(is_pretty), K(depth));
      }
      break;
    }

    case ObJsonNodeType::J_ARRAY: {
      if (ObJsonParser::is_json_doc_over_depth(++depth)) {
        ret = OB_ERR_JSON_OUT_OF_DEPTH;
        LOG_WARN("current json over depth", K(ret), K(depth), K(j_type));
      } else if (OB_FAIL(print_array(j_buf, depth, is_pretty))) {
        LOG_WARN("fail to change jarray to string", K(ret), K(is_quoted), K(is_pretty), K(depth));
      }
      break;
    }

    case ObJsonNodeType::J_OBJECT: {
      if (ObJsonParser::is_json_doc_over_depth(++depth)) {
        ret = OB_ERR_JSON_OUT_OF_DEPTH;
        LOG_WARN("current json over depth", K(ret), K(depth), K(j_type));
      } else if (OB_FAIL(print_object(j_buf, depth, is_pretty))) {
        LOG_WARN("fail to print object to string", K(ret), K(depth), K(j_type), K(is_pretty));
      }
      break;
    }

    case ObJsonNodeType::J_BOOLEAN: {
      if (get_boolean() ? OB_FAIL(j_buf.append("true", sizeof("true") - 1)) :
                          OB_FAIL(j_buf.append("false", sizeof("false") - 1))) {
        LOG_WARN("fail to append boolean", K(ret), K(get_boolean()), K(j_type));
      }
      break;
    }

    case ObJsonNodeType::J_DECIMAL:
    case ObJsonNodeType::J_ODECIMAL: {
      if (OB_FAIL(print_decimal(j_buf))) {
        LOG_WARN("fail to print decimal to string", K(ret), K(depth), K(j_type));
      }
      break;
    }

    case ObJsonNodeType::J_DOUBLE:
    case ObJsonNodeType::J_ODOUBLE: {
      if (OB_FAIL(print_double(j_buf))) {
        LOG_WARN("fail to print double to string", K(ret), K(depth), K(j_type));
      }
      break;
    }

    case ObJsonNodeType::J_OFLOAT: {
      if (OB_FAIL(print_float(j_buf))) {
        LOG_WARN("fail to print float to string", K(ret), K(depth), K(j_type));
      }
      break;
    }

    case ObJsonNodeType::J_NULL: {
      if (!(this)->is_real_json_null(this) && OB_FAIL(j_buf.append("", 0))) {
        LOG_WARN("fail to append NULL upper string to buffer", K(ret), K(j_type));
      } else if ((this)->is_real_json_null(this) && OB_FAIL(j_buf.append("null", sizeof("null") - 1))) {
        LOG_WARN("fail to append null string to buffer", K(ret), K(j_type));
      }
      break;
    }

    case ObJsonNodeType::J_OPAQUE: {
      if (OB_FAIL(print_opaque(j_buf, depth, is_quoted))) {
        LOG_WARN("fail to print opaque to string", K(ret), K(depth), K(j_type), K(is_quoted));
      }
      break;
    }

    case ObJsonNodeType::J_STRING:
    case ObJsonNodeType::J_OBINARY:
    case ObJsonNodeType::J_OOID:
    case ObJsonNodeType::J_ORAWHEX:
    case ObJsonNodeType::J_ORAWID:
    case ObJsonNodeType::J_ODAYSECOND:
    case ObJsonNodeType::J_OYEARMONTH: {
      uint64_t data_len = get_data_length();
      const char *data = get_data();
      if (is_quoted && data_len == 0) {
        if (OB_FAIL(j_buf.append("\"\"", 2))) {
          LOG_WARN("fail to append empty string", K(ret), K(j_type), K(is_quoted));
        }
      } else if (OB_ISNULL(data) && data_len != 0) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("data is null", K(ret), K(data_len));
      } else if (OB_FAIL(ObJsonBaseUtil::append_string(j_buf, is_quoted, data, data_len))) {
        // if data is null, data_len is 0, it is an empty string
        LOG_WARN("fail to append string", K(ret), K(j_type), K(is_quoted));
      }
      break;
    }

    case ObJsonNodeType::J_INT:
    case ObJsonNodeType::J_OINT: {
      char tmp_buf[ObFastFormatInt::MAX_DIGITS10_STR_SIZE] = {0};
      int64_t len = ObFastFormatInt::format_signed(get_int(), tmp_buf);
      if (OB_FAIL(j_buf.append(tmp_buf, len))) {
        LOG_WARN("fail to append json int to buffer", K(ret), K(get_int()), K(len), K(j_type));
      }
      break;
    }

    case ObJsonNodeType::J_UINT:
    case ObJsonNodeType::J_OLONG:  {
      char tmp_buf[ObFastFormatInt::MAX_DIGITS10_STR_SIZE] = {0};
      int64_t len = ObFastFormatInt::format_unsigned(get_uint(), tmp_buf);
      if (OB_FAIL(j_buf.append(tmp_buf, len))) {
        LOG_WARN("fail to append json uint to buffer", K(ret), K(get_uint()), K(len), K(j_type));
      }
      break;
    }

    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("undefined json node type", K(ret), K(j_type));
      break;
    }
  }

  return ret;
}

int ObIJsonBase::calc_json_hash_value(uint64_t val, hash_algo hash_func, uint64_t &res) const
{
  INIT_SUCC(ret);
  ObJsonHashValue hash_value(val, hash_func);
  ObJsonNodeType j_type = json_type();

  switch (j_type) {
    case ObJsonNodeType::J_ARRAY: {
      hash_value.calc_character(ObJsonHashValue::JSON_ARRAY_FLAG);
      uint64_t arr_res = 0;
      uint64_t size = element_count();
      ObIJsonBase *jb_ptr = NULL;
      ObJsonBin j_bin(allocator_);
      jb_ptr = &j_bin;
      for (uint64_t i = 0; i < size && OB_SUCC(ret); i++) {
        if (OB_FAIL(get_array_element(i, jb_ptr))) {
          LOG_WARN("fail to get this json array element", K(ret), K(i), K(size));
        } else if (OB_ISNULL(jb_ptr)) {
          ret = OB_ERR_NULL_VALUE;
          LOG_WARN("null ptr", K(ret), K(i), K(size));
        } else if (OB_FAIL(jb_ptr->calc_json_hash_value(hash_value.get_hash_value(), hash_func, arr_res))) {
          LOG_WARN("fail to calc json hash value", K(ret), K(i), K(size), K(j_type));
        } else {
          hash_value.calc_uint64(arr_res);
        }
      }
      break;
    }

    case ObJsonNodeType::J_OBJECT: {
      hash_value.calc_character(ObJsonHashValue::JSON_OBJ_FLAG);
      uint64_t obj_res = 0;
      uint64_t count = element_count();
      ObString key;
      ObIJsonBase *jb_ptr = NULL;
      ObJsonBin j_bin(allocator_);
      jb_ptr = &j_bin;
      for (uint64_t i = 0; OB_SUCC(ret) && i < count; i++) {
        if (OB_FAIL(get_key(i, key))) {
          LOG_WARN("failed to get key", K(ret), K(i));
        } else {
          hash_value.calc_string(key);
          if (OB_FAIL(get_object_value(i, jb_ptr))) {
            LOG_WARN("failed to get sub obj.", K(ret), K(i), K(key));
          } else if (jb_ptr->calc_json_hash_value(hash_value.get_hash_value(), hash_func, obj_res)) {
            LOG_WARN("fail to calc json hash value", K(ret), K(i), K(count), K(j_type));
          } else {
            hash_value.calc_uint64(obj_res);
          }
        }
      }
      break;
    }
    case ObJsonNodeType::J_OBINARY:
    case ObJsonNodeType::J_OOID:
    case ObJsonNodeType::J_ORAWHEX:
    case ObJsonNodeType::J_ORAWID:
    case ObJsonNodeType::J_ODAYSECOND:
    case ObJsonNodeType::J_OYEARMONTH:
    case ObJsonNodeType::J_STRING:
    case ObJsonNodeType::J_OPAQUE: {
      ObString str(get_data_length(), get_data());
      hash_value.calc_string(str);
      break;
    }

    case ObJsonNodeType::J_INT:
    case ObJsonNodeType::J_OINT: {
      hash_value.calc_int64(get_int());
      break;
    }

    case ObJsonNodeType::J_UINT:
    case ObJsonNodeType::J_OLONG: {
      hash_value.calc_uint64(get_uint());
      break;
    }

    case ObJsonNodeType::J_OFLOAT: {
      hash_value.calc_double(get_float());
      break;
    }
    case ObJsonNodeType::J_DOUBLE:
    case ObJsonNodeType::J_ODOUBLE: {
      hash_value.calc_double(get_double());
      break;
    }

    case ObJsonNodeType::J_DECIMAL:
    case ObJsonNodeType::J_ODECIMAL: {
      hash_value.calc_num(get_decimal_data());
      break;
    }

    case ObJsonNodeType::J_BOOLEAN: {
      bool value_bool = get_boolean();
      hash_value.calc_character(value_bool ?
                                ObJsonHashValue::JSON_BOOL_TRUE :
                                ObJsonHashValue::JSON_BOOL_FALSE);
      break;
    }

    case ObJsonNodeType::J_DATETIME:
    case ObJsonNodeType::J_ORACLEDATE: {
      if (OB_FAIL(hash_value.calc_time(DT_TYPE_DATETIME, this))) {
        LOG_WARN("fail to calc json datetime hash", K(ret));
      }
      break;
    }
    case ObJsonNodeType::J_TIMESTAMP:
    case ObJsonNodeType::J_ODATE: {
      if (OB_FAIL(hash_value.calc_time(DT_TYPE_DATETIME, this))) {
        LOG_WARN("fail to calc json timestamp hash", K(ret));
      }
      break;
    }
    case ObJsonNodeType::J_TIME: {
      if (OB_FAIL(hash_value.calc_time(DT_TYPE_TIME, this))) {
        LOG_WARN("fail to calc json time hash", K(ret));
      }
      break;
    }
    case ObJsonNodeType::J_DATE:
    case ObJsonNodeType::J_OTIMESTAMP:
    case ObJsonNodeType::J_OTIMESTAMPTZ: {
      if (OB_FAIL(hash_value.calc_time(DT_TYPE_DATE, this))) {
        LOG_WARN("fail to calc json date hash", K(ret));
      }
      break;
    }

    case ObJsonNodeType::J_NULL: {
      hash_value.calc_character(ObJsonHashValue::JSON_NULL_FLAG);
      break;
    }

    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect json type", K(ret), K(j_type));
      break;
    }
  }

  if (OB_SUCC(ret)) {
    res = hash_value.get_hash_value();
  }

  return ret;
}

// 1. If the arrays are equal in length and each array element is equal, then the two arrays are equal.
// 2. If you run into the first element of an unequal array,
//    the smaller element will have a smaller array, and in this example, a is smaller.
//    a[0, 1, 2, 3] vs b[0, 1, 3, 1, 1, 1, 1] ----> a < b
// 3. If the array is not equal, and the smaller array is equal to the larger array element by element,
//    then the smaller array is smaller, and example a is smaller.
//    a[0, 1, 2] vs b[0, 1, 2, 1, 1, 1, 1] ----> a < b
int ObIJsonBase::compare_array(const ObIJsonBase &other, int &res) const
{
  INIT_SUCC(ret);
  const ObJsonNodeType j_type_a = json_type();
  const ObJsonNodeType j_type_b = other.json_type();

  if (j_type_a != ObJsonNodeType::J_ARRAY || j_type_b != ObJsonNodeType::J_ARRAY) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ObJsonNodeType", K(ret), K(j_type_a), K(j_type_b));
  } else {
    const uint64_t size_a = element_count();
    const uint64_t size_b = other.element_count();
    const uint64_t min_size = std::min(size_a, size_b);
    for (uint64_t i = 0; (res == 0) && OB_SUCC(ret) && i < min_size; i++) {
      ObIJsonBase *jb_a_ptr = NULL;
      ObIJsonBase *jb_b_ptr = NULL;
      ObJsonBin j_bin_a(allocator_);
      ObJsonBin j_bin_b(allocator_);
      jb_a_ptr = &j_bin_a;
      jb_b_ptr = &j_bin_b;
      if (OB_FAIL(get_array_element(i, jb_a_ptr))) {
        LOG_WARN("fail to get this json array element", K(ret), K(i), K(size_a));
      } else if (OB_FAIL(other.get_array_element(i, jb_b_ptr))) {
        LOG_WARN("fail to get other json array element", K(ret), K(i), K(size_b));
      } else if (OB_FAIL(jb_a_ptr->compare(*jb_b_ptr, res))) {
        LOG_WARN("fail to compare this json to other json", K(ret), K(i), K(size_a), K(size_b));
      }
    }

    // Compare the array length if all the comparisons are equal.
    if (OB_SUCC(ret) && res == 0) {
      res = ObJsonBaseUtil::compare_numbers(size_a, size_b);
    }
  }

  return ret;
}

// 1. Two objects are equal if their key-value number are equal and their keys are equal and their values are equal.
// 2. If the key-value number of two objects are not equal, then objects with fewer key-value pairs are smaller
// 3. Compare each key-value pair and return the result of the first unequal encounter.
int ObIJsonBase::compare_object(const ObIJsonBase &other, int &res) const
{
  INIT_SUCC(ret);
  const ObJsonNodeType j_type_a = json_type();
  const ObJsonNodeType j_type_b = other.json_type();

  if (j_type_a != ObJsonNodeType::J_OBJECT || j_type_b != ObJsonNodeType::J_OBJECT) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ObJsonNodeType", K(ret), K(j_type_a), K(j_type_b));
  } else {
    const uint64_t len_a = element_count();
    const uint64_t len_b = other.element_count();
    res = ObJsonBaseUtil::compare_numbers(len_a, len_b);
    for (uint64_t i = 0; (res == 0) && OB_SUCC(ret) && i < len_a; i++) {
      ObString key_a, key_b;
      if (OB_FAIL(get_key(i, key_a))) {
        LOG_WARN("failed to get this key", K(ret), K(i));
      } else if (OB_FAIL(other.get_key(i, key_b))) {
        LOG_WARN("failed to get other key.", K(ret), K(i));
      } else {
        // Compare keys.
        res = key_a.compare(key_b);
        if (res == 0) {
          ObIJsonBase *jb_a_ptr = NULL;
          ObIJsonBase *jb_b_ptr = NULL;
          ObJsonBin j_bin_a(allocator_);
          ObJsonBin j_bin_b(allocator_);
          jb_a_ptr = &j_bin_a;
          jb_b_ptr = &j_bin_b;
          // Compare value.
          if (OB_FAIL(get_object_value(i, jb_a_ptr))) {
            LOG_WARN("fail to get this json obj element", K(ret), K(i), K(len_a));
          } else if (OB_FAIL(other.get_object_value(i, jb_b_ptr))) {
            LOG_WARN("fail to get other json obj element", K(ret), K(i), K(len_b));
          } else if (OB_FAIL(jb_a_ptr->compare(*jb_b_ptr, res))) {
            LOG_WARN("fail to compare this json to other json", K(ret), K(i), K(len_a), K(len_b));
          }
        }
      }
    }
  }

  return ret;
}

int ObIJsonBase::compare_int(const ObIJsonBase &other, int &res) const
{
  INIT_SUCC(ret);
  const ObJsonNodeType j_type_a = json_type();
  const ObJsonNodeType j_type_b = other.json_type();

  if (j_type_a != ObJsonNodeType::J_INT &&
      j_type_a != ObJsonNodeType::J_OINT) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ObJsonNodeType", K(ret), K(j_type_a), K(j_type_b));
  } else {
    int64_t int_a = get_int();
    switch (j_type_b) {
      case ObJsonNodeType::J_INT:
      case ObJsonNodeType::J_OINT: {
        int64_t int_b = other.get_int();
        res = ObJsonBaseUtil::compare_numbers(int_a, int_b);
        break;
      }

      case ObJsonNodeType::J_UINT:
      case ObJsonNodeType::J_OLONG: {
        uint64_t uint_b = other.get_uint();
        res = ObJsonBaseUtil::compare_int_uint(int_a, uint_b);
        break;
      }

      case ObJsonNodeType::J_DOUBLE:
      case ObJsonNodeType::J_ODOUBLE: {
        double double_b = other.get_double();
        if (OB_FAIL(ObJsonBaseUtil::compare_double_int(double_b, int_a, res))) {
          LOG_WARN("fail to compare double with int", K(ret), K(double_b), K(int_a));
        } else {
          res = -1 * res;
        }
        break;
      }

      case ObJsonNodeType::J_OFLOAT: {
        double double_b = other.get_float();
        if (OB_FAIL(ObJsonBaseUtil::compare_double_int(double_b, int_a, res))) {
          LOG_WARN("fail to compare float with int", K(ret), K(double_b), K(int_a));
        } else {
          res = -1 * res;
        }
        break;
      }

      case ObJsonNodeType::J_DECIMAL:
      case ObJsonNodeType::J_ODECIMAL: {
        number::ObNumber num_b = other.get_decimal_data();
        if (OB_FAIL(ObJsonBaseUtil::compare_decimal_int(num_b, int_a, res))) {
          LOG_WARN("fail to compare decimal with int", K(ret), K(num_b), K(int_a));
        } else {
          res = -1 * res;
        }
        break;
      }

      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect json type", K(j_type_b));
        break;
      }
    }
  }

  return ret;
}

int ObIJsonBase::compare_uint(const ObIJsonBase &other, int &res) const
{
  INIT_SUCC(ret);
  const ObJsonNodeType j_type_a = json_type();
  const ObJsonNodeType j_type_b = other.json_type();

  if (j_type_a != ObJsonNodeType::J_UINT &&
      j_type_a != ObJsonNodeType::J_OLONG) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ObJsonNodeType", K(ret), K(j_type_a), K(j_type_b));
  } else {
    uint64_t uint_a = get_uint();
    switch (j_type_b) {
      case ObJsonNodeType::J_UINT:
      case ObJsonNodeType::J_OLONG: {
        uint64_t uint_b = other.get_uint();
        res = ObJsonBaseUtil::compare_numbers(uint_a, uint_b);
        break;
      }

      case ObJsonNodeType::J_INT:
      case ObJsonNodeType::J_OINT: {
        int64_t int_b = other.get_int();
        res = -1 * ObJsonBaseUtil::compare_int_uint(int_b, uint_a);
        break;
      }

      case ObJsonNodeType::J_DOUBLE:
      case ObJsonNodeType::J_ODOUBLE: {
        double double_b = other.get_double();
        if (OB_FAIL(ObJsonBaseUtil::compare_double_uint(double_b, uint_a, res))) {
          LOG_WARN("fail to compare double with uint", K(ret), K(double_b), K(uint_a));
        } else {
          res = -1 * res;
        }
        break;
      }

       case ObJsonNodeType::J_OFLOAT: {
        double double_b = other.get_float();
        if (OB_FAIL(ObJsonBaseUtil::compare_double_uint(double_b, uint_a, res))) {
          LOG_WARN("fail to compare double with uint", K(ret), K(double_b), K(uint_a));
        } else {
          res = -1 * res;
        }
        break;
      }

      case ObJsonNodeType::J_DECIMAL:
      case ObJsonNodeType::J_ODECIMAL: {
        number::ObNumber num_b = other.get_decimal_data();
        if (OB_FAIL(ObJsonBaseUtil::compare_decimal_uint(num_b, uint_a, res))) {
          LOG_WARN("fail to compare decimal with uint", K(ret), K(num_b), K(uint_a));
        } else {
          res = -1 * res;
        }
        break;
      }

      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect json type", K(j_type_b));
        break;
      }
    }
  }

  return ret;
}

int ObIJsonBase::compare_double(const ObIJsonBase &other, int &res) const
{
  INIT_SUCC(ret);
  const ObJsonNodeType j_type_a = json_type();
  const ObJsonNodeType j_type_b = other.json_type();

  if (j_type_a != ObJsonNodeType::J_DOUBLE &&
      j_type_a != ObJsonNodeType::J_ODOUBLE &&
      j_type_a != ObJsonNodeType::J_OFLOAT) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ObJsonNodeType", K(ret), K(j_type_a), K(j_type_b));
  } else {
    double double_a;
    if (j_type_a == ObJsonNodeType::J_OFLOAT) {
      double_a = get_float();
    } else {
      double_a = get_double();
    }

    switch (j_type_b) {
      case ObJsonNodeType::J_OFLOAT: {
        double double_b = other.get_float();
        double_a = (float) double_a;
        res = ObJsonBaseUtil::compare_numbers(double_a, double_b);
        break;
      }

      case ObJsonNodeType::J_DOUBLE:
      case ObJsonNodeType::J_ODOUBLE: {
        double double_b = other.get_double();
        res = ObJsonBaseUtil::compare_numbers(double_a, double_b);
        break;
      }

      case ObJsonNodeType::J_INT:
      case ObJsonNodeType::J_OINT: {
        int64_t int_b = other.get_int();
        if (OB_FAIL(ObJsonBaseUtil::compare_double_int(double_a, int_b, res))) {
          LOG_WARN("fail to compare double with int", K(ret), K(double_a), K(int_b));
        }
        break;
      }

      case ObJsonNodeType::J_UINT:
      case ObJsonNodeType::J_OLONG: {
        uint64_t uint_b = other.get_uint();
        if (OB_FAIL(ObJsonBaseUtil::compare_double_uint(double_a, uint_b, res))) {
          LOG_WARN("fail to compare double with uint", K(ret), K(double_a), K(uint_b));
        }
        break;
      }

      case ObJsonNodeType::J_DECIMAL:
      case ObJsonNodeType::J_ODECIMAL: {
        number::ObNumber num_b = other.get_decimal_data();
        if (OB_FAIL(ObJsonBaseUtil::compare_decimal_double(num_b, double_a, res))) {
          LOG_WARN("fail to compare decimal with double", K(ret), K(double_a), K(num_b));
        } else {
          res = -1 * res;
        }
        break;
      }

      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect json type", K(j_type_b));
        break;
      }
    }
  }

  return ret;
}

int ObIJsonBase::compare_decimal(const ObIJsonBase &other, int &res) const
{
  INIT_SUCC(ret);
  const ObJsonNodeType j_type_a = json_type();
  const ObJsonNodeType j_type_b = other.json_type();

  if (j_type_a != ObJsonNodeType::J_DECIMAL
      && j_type_a != ObJsonNodeType::J_ODECIMAL) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ObJsonNodeType", K(ret), K(j_type_a), K(j_type_b));
  } else {
    number::ObNumber num_a = get_decimal_data();
    switch (j_type_b) {
      case ObJsonNodeType::J_DECIMAL:
      case ObJsonNodeType::J_ODECIMAL: {
        number::ObNumber num_b = other.get_decimal_data();
        if (num_a.is_zero() && num_b.is_zero()) {
          res = 0;
        } else {
          res = num_a.compare(num_b);
        }
        break;
      }

      case ObJsonNodeType::J_INT:
      case ObJsonNodeType::J_OINT: {
        int64_t int_b = other.get_int();
        if (OB_FAIL(ObJsonBaseUtil::compare_decimal_int(num_a, int_b, res))) {
          LOG_WARN("fail to compare decimal with int", K(ret), K(num_a), K(int_b));
        }
        break;
      }

      case ObJsonNodeType::J_UINT:
      case ObJsonNodeType::J_OLONG: {
        uint64_t uint_b = other.get_uint();
        if (OB_FAIL(ObJsonBaseUtil::compare_decimal_uint(num_a, uint_b, res))) {
          LOG_WARN("fail to compare decimal with uint", K(ret), K(num_a), K(uint_b));
        }
        break;
      }

      case ObJsonNodeType::J_DOUBLE:
      case ObJsonNodeType::J_ODOUBLE: {
        double double_b = other.get_double();
        if (OB_FAIL(ObJsonBaseUtil::compare_decimal_double(num_a, double_b, res))) {
          LOG_WARN("fail to compare decimal with double", K(ret), K(num_a), K(double_b));
        }
        break;
      }

      case ObJsonNodeType::J_OFLOAT: {
        double double_b = other.get_float();
        if (OB_FAIL(ObJsonBaseUtil::compare_decimal_double(num_a, double_b, res))) {
          LOG_WARN("fail to compare decimal with double", K(ret), K(num_a), K(double_b));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect json type", K(j_type_b));
        break;
      }
    }
  }

  return ret;
}

int ObIJsonBase::compare_datetime(ObDTMode dt_mode_a, const ObIJsonBase &other, int &res) const
{
  INIT_SUCC(ret);
  const ObJsonNodeType j_type_a = json_type();
  const ObJsonNodeType j_type_b = other.json_type();

  bool is_a_valid = ObJsonBaseUtil::is_time_type(j_type_a);
  bool is_b_valid = ObJsonBaseUtil::is_time_type(j_type_b);

  if (!is_a_valid || !is_b_valid) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ObJsonNodeType", K(ret), K(j_type_a), K(j_type_b));
  } else {
    ObDTMode dt_mode_b;
    if (OB_FAIL(ObJsonBaseUtil::get_dt_mode_by_json_type(j_type_b, dt_mode_b))) {
      LOG_WARN("fail to get dt mode by json tyoe", K(ret), K(j_type_b));
    } else {
      ObTime t_a;
      ObTime t_b;
      if (OB_FAIL(get_obtime(t_a))) {
        LOG_WARN("fail to get json obtime_a", K(ret));
      } else {
        int64_t int_a = ObTimeConverter::ob_time_to_int(t_a, dt_mode_a);
        if (OB_FAIL(other.get_obtime(t_b))) {
          LOG_WARN("fail to get json obtime_b", K(ret));
        } else {
          int64_t int_b = ObTimeConverter::ob_time_to_int(t_b, dt_mode_b);
          res = ObJsonBaseUtil::compare_numbers(int_a, int_b);
        }
      }
    }
  }

  return ret;
}

// 1 means this_type has a higher priority
// -1 means this_type has a lower priority
int ObIJsonBase::path_compare_string(const ObString &str_l, const ObString &str_r, int &res) const
{
  INIT_SUCC(ret);
  ret = 0;
  int l_len = str_l.length();
  int r_len = str_r.length();
  int i = 0;
  while (i < l_len && OB_SUCC(ret) && res == 0) {
    if (i < r_len) {
      if (str_l[i] < 0 && str_r[i] > 0) {
        res = 1;
      } else if (str_r[i] < 0 && str_l[i] > 0) {
        res = -1;
      } else if (str_l[i] < str_r[i]) {
        res = -1;
      } else if (str_l[i] > str_r[i]) {
        res = 1;
      } else {
        ++i;
      }
    } else {
      res = 1;
    }
  }
  if (OB_SUCC(ret) && res == 0 && i == l_len) {
    if (i < r_len) {
      res = -1;
    } else if (i == r_len) {
      res = 0;
    }
  }
  return ret;
}

enum CMP_FUNC_TYPE {
  CMP_SMALLER = -1,
  CMP_LARGER = 1,
  CMP_FUNC = 0,
  CMP_NOT_SUPPORT = 2,
  CMP_ERROR = -3
};

static constexpr int JSON_TYPE_NUM = static_cast<int>(ObJsonNodeType::J_MAX_TYPE) + 1;
// 0 means that the priority is the same, but it also means that types are directly comparable,
// i.e. decimal, int, uint, and double are all comparable.
// 1 means this_type has a higher priority
// -1 means this_type has a lower priority
static constexpr int type_comparison[JSON_TYPE_NUM][JSON_TYPE_NUM] = {
  /*                     0   1   2   3   4   5   6   7   8   9  10  11  12  13  14  15  16  17  18  19  20  21  22  23  24  25  26  27  28  29  30 */
  /* 0  NULL */         {0, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2},
  /* 1  DECIMAL */      {1,  0,  0,  0,  0, -1, -1, -1, -1, -1, -1, -1, -1, -1,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2},
  /* 2  INT */          {1,  0,  0,  0,  0, -1, -1, -1, -1, -1, -1, -1, -1, -1,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2},
  /* 3  UINT */         {1,  0,  0,  0,  0, -1, -1, -1, -1, -1, -1, -1, -1, -1,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2},
  /* 4  DOUBLE */       {1,  0,  0,  0,  0, -1, -1, -1, -1, -1, -1, -1, -1, -1,  2,  0,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2},
  /* 5  STRING */       {1,  1,  1,  1,  1,  0, -1, -1, -1, -1, -1, -1, -1, -1,  2,  2,  2,  2,  2,  2,  0,  0,  0,  0,  2,  2,  2,  2,  2,  2,  2},
  /* 6  OBJECT */       {1,  1,  1,  1,  1,  1,  0, -1, -1, -1, -1, -1, -1, -1,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2},
  /* 7  ARRAY */        {1,  1,  1,  1,  1,  1,  1,  0, -1, -1, -1, -1, -1, -1,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2},
  /* 8  BOOLEAN */      {1,  1,  1,  1,  1,  1,  1,  1,  0, -1, -1, -1, -1, -1,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2},
  /* 9  DATE */         {1,  1,  1,  1,  1,  1,  1,  1,  1,  0, -1, -1, -1, -1,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2},
  /* 10  TIME */        {1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  0, -1, -1, -1,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2},
  /* 11  DATETIME */    {1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  0,  0, -1,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2},
  /* 12  TIMESTAMP */   {1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  0,  0, -1,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  0,  2,  2,  2,  2},
  /* 13  OPAQUE */      {1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  0,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2},
  /* 14  empty */       {2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2},
  /*  ORACLE MODE */
  /* 15  OFLOAT */      {2,  2,  2,  2,  0,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  0,  0,  0,  0,  0,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2},
  /* 16  ODOUBLE */     {2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  0,  0,  0,  0,  0,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2},
  /* 17  ODECIMAL */    {2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  0,  0,  0,  0,  0,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2},
  /* 18  OINT */        {2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  0,  0,  0,  0,  0,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2},
  /* 19  OLONG */       {2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  0,  0,  0,  0,  0,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2},
  /* 20  OBINARY */     {2,  2,  2,  2,  2,  0,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  0,  0,  0,  0,  2,  2,  2,  2,  2,  2,  2},
  /* 21  OOID */        {2,  2,  2,  2,  2,  0,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  0,  0,  0,  0,  2,  2,  2,  2,  2,  2,  2},
  /* 22  ORAWHEX */     {2,  2,  2,  2,  2,  0,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  0,  0,  0,  0,  2,  2,  2,  2,  2,  2,  2},
  /* 23  ORAWID */      {2,  2,  2,  2,  2,  0,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  0,  0,  0,  0,  2,  2,  2,  2,  2,  2,  2},
  /* 24  ORACLEDATE*/   {2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  0,  0,  0,  0,  2,  2,  2},
  /* 25  ODATE */       {2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  0,  0,  0,  0,  2,  2,  2},
  /* 26  OTIMESTAMP */  {2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  0,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  0,  0,  0,  0,  2,  2,  2},
  /* 27  TIMESTAMPTZ*/  {2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  0,  0,  0,  0,  2,  2,  2},
  /* 28  ODAYSECOND */  {2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  0,  2,  2},
  /* 29  OYEARMONTH */  {2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  0,  2},
  /* 30  MAX_OTYPE */   {2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2},
};

int ObIJsonBase::compare(const ObIJsonBase &other, int &res, bool is_path) const
{
  INIT_SUCC(ret);
  const ObJsonNodeType j_type_a = json_type();
  const ObJsonNodeType j_type_b = other.json_type();
  res = 0;

  if (j_type_a == ObJsonNodeType::J_ERROR || j_type_b == ObJsonNodeType::J_ERROR) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("error json type", K(j_type_a), K(j_type_b));
  } else if (is_path
            && (j_type_a == ObJsonNodeType::J_OBJECT || j_type_b == ObJsonNodeType::J_OBJECT
            || ((j_type_a == ObJsonNodeType::J_NULL || j_type_a == ObJsonNodeType::J_NULL) && j_type_a != j_type_b))) {
    res = -3;
  } else {
    // Compare the matrix to get which json type has a higher priority, and return the result if the priority is different.
    int type_cmp = type_comparison[static_cast<int>(j_type_a)][static_cast<int>(j_type_b)];
    if (type_cmp != 0) { // Different priorities, complete comparison.
      res = type_cmp;
      if (static_cast<CMP_FUNC_TYPE>(type_cmp) == CMP_NOT_SUPPORT) {
        ret = OB_OP_NOT_ALLOW;
      }
    } else { // Same priority.
      switch (j_type_a) {
        case ObJsonNodeType::J_ARRAY: {
          if (OB_FAIL(compare_array(other, res))) {
            LOG_WARN("fail to compare json array", K(ret));
          }
          break;
        }

        case ObJsonNodeType::J_OBJECT: {
          if (OB_FAIL(compare_object(other, res))) {
            LOG_WARN("fail to compare json object", K(ret));
          }
          break;
        }

        case ObJsonNodeType::J_OBINARY:
        case ObJsonNodeType::J_OOID:
        case ObJsonNodeType::J_ORAWHEX:
        case ObJsonNodeType::J_ORAWID:
        case ObJsonNodeType::J_OYEARMONTH:
        case ObJsonNodeType::J_ODAYSECOND:
        case ObJsonNodeType::J_STRING: {
          ObString str_a(get_data_length(), get_data());
          ObString str_b(other.get_data_length(), other.get_data());
          if (lib::is_oracle_mode() && is_path) {
            if (OB_FAIL(path_compare_string(str_a, str_b, res))) {
              LOG_WARN("fail to compare json string", K(ret));
            }
          } else {
            res = str_a.compare(str_b);
          }
          break;
        }

        case ObJsonNodeType::J_INT:
        case ObJsonNodeType::J_OINT: {
          if (OB_FAIL(compare_int(other, res))) {
            LOG_WARN("fail to compare json int", K(ret));
          }
          break;
        }

        case ObJsonNodeType::J_UINT:
        case ObJsonNodeType::J_OLONG: {
          if (OB_FAIL(compare_uint(other, res))) {
            LOG_WARN("fail to compare json uint", K(ret));
          }
          break;
        }

        case ObJsonNodeType::J_DOUBLE:
        case ObJsonNodeType::J_ODOUBLE: {
          if (OB_FAIL(compare_double(other, res))) {
            LOG_WARN("fail to compare json double", K(ret));
          }
          break;
        }

        case ObJsonNodeType::J_OFLOAT: {
          if (OB_FAIL(compare_double(other, res))) {
            LOG_WARN("fail to compare json float", K(ret));
          }
          break;
        }

        case ObJsonNodeType::J_DECIMAL:
        case ObJsonNodeType::J_ODECIMAL: {
          if (OB_FAIL(compare_decimal(other, res))) {
            LOG_WARN("fail to compare json decimal", K(ret));
          }
          break;
        }

        case ObJsonNodeType::J_BOOLEAN: {
          res = ObJsonBaseUtil::compare_numbers(get_boolean(), other.get_boolean());
          break;
        }

        case ObJsonNodeType::J_DATETIME: {
          if (OB_FAIL(compare_datetime(DT_TYPE_DATETIME, other, res))) {
            LOG_WARN("fail to compare json datetime", K(ret));
          }
          break;
        }

        case ObJsonNodeType::J_TIMESTAMP:
        case ObJsonNodeType::J_ODATE:
        case ObJsonNodeType::J_OTIMESTAMP:
        case ObJsonNodeType::J_OTIMESTAMPTZ: {
          if (OB_FAIL(compare_datetime(DT_TYPE_DATETIME, other, res))) {
            LOG_WARN("fail to compare json timestamp", K(ret));
          }
          break;
        }

        case ObJsonNodeType::J_TIME: {
          if (OB_FAIL(compare_datetime(DT_TYPE_TIME, other, res))) {
            LOG_WARN("fail to compare json time", K(ret));
          }
          break;
        }

        case ObJsonNodeType::J_DATE:
        case ObJsonNodeType::J_ORACLEDATE: {
          if (OB_FAIL(compare_datetime(DT_TYPE_DATE, other, res))) {
            LOG_WARN("fail to compare json date", K(ret));
          }
          break;
        }

        case ObJsonNodeType::J_OPAQUE: {
          // If Opaque has the same type and value, the two Opaque are equal.
          res = ObJsonBaseUtil::compare_numbers(field_type(), other.field_type());
          if (res == 0) {
            ObString str_a(get_data_length(), get_data());
            ObString str_b(other.get_data_length(), other.get_data());
            res = str_a.compare(str_b);
          }
          break;
        }

        case ObJsonNodeType::J_NULL: {
          if (j_type_a != j_type_b || is_real_json_null(this) != is_real_json_null(&other)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("both json type should be NULL", K(j_type_a), K(j_type_b));
          } else {
            res = 0;
          }
          break;
        }

        default:{
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect json type",K(j_type_a), K(j_type_b));
          break;
        }
      }
    }
  }
  return ret;
}


uint32_t ObIJsonBase::depth()
{
  INIT_SUCC(ret);
  uint32_t depth = 0;

  if (is_bin()) {
    ObArenaAllocator allocator;
    ObIJsonBase *j_tree = NULL;
    if (OB_FAIL(ObJsonBaseFactory::transform(&allocator, this, ObJsonInType::JSON_TREE, j_tree))) {
      LOG_WARN("fail to transform to tree", K(ret));
    } else {
      depth = j_tree->depth();
    }
  } else {
    depth = static_cast<ObJsonNode *>(this)->depth();
  }

  return depth;
}

int ObIJsonBase::get_location(ObJsonBuffer &path)
{
  INIT_SUCC(ret);

  if (is_bin()) {
    ObArenaAllocator allocator;
    ObIJsonBase *j_tree = NULL;
    if (OB_FAIL(ObJsonBaseFactory::transform(&allocator, this, ObJsonInType::JSON_TREE, j_tree))) {
      LOG_WARN("fail to transform to tree", K(ret));
    } else if (OB_FAIL(static_cast<ObJsonNode *>(j_tree)->get_location(path))) {
      LOG_WARN("fail to get path location", K(ret));
    }
  } else { // is_tree
    if (OB_FAIL(static_cast<ObJsonNode *>(this)->get_location(path))) {
      LOG_WARN("fail to get path location", K(ret));
    }
  }

  return ret;
}

int ObIJsonBase::merge_tree(ObIAllocator *allocator, ObIJsonBase *other, ObIJsonBase *&result)
{
  INIT_SUCC(ret);

  if (OB_ISNULL(allocator) || OB_ISNULL(other)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param is null", K(ret), KP(allocator), KP(other));
  } else if (is_bin()) {
    ObIJsonBase *j_this_tree = NULL;
    ObIJsonBase *j_other_tree = NULL;
    if (OB_FAIL(ObJsonBaseFactory::transform(allocator, this, ObJsonInType::JSON_TREE,
        j_this_tree))) {
      LOG_WARN("fail to transform this to tree", K(ret));
    } else if (other->is_bin()) {
      if (OB_FAIL(ObJsonBaseFactory::transform(allocator, other, ObJsonInType::JSON_TREE,
          j_other_tree))) {
        LOG_WARN("fail to transform other to tree", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(static_cast<ObJsonNode *>(j_this_tree)->merge_tree(allocator,
          j_other_tree, result))) {
        LOG_WARN("fail to merge tree", K(ret), K(*j_this_tree), K(*j_other_tree));
      }
    }
  } else { // is_tree
    if (OB_FAIL(static_cast<ObJsonNode *>(this)->merge_tree(allocator, other, result))) {
      LOG_WARN("fail to merge tree", K(ret), K(*other));
    }
  }

  return ret;
}

int ObIJsonBase::get_used_size(uint64_t &size)
{
  INIT_SUCC(ret);

  if (is_bin()) {
    const ObJsonBin *j_bin = static_cast<const ObJsonBin *>(this);
    ret = j_bin->get_used_bytes(size);
  } else { // is tree
    ObArenaAllocator allocator;
    ObIJsonBase *j_bin = NULL;
    if (OB_FAIL(ObJsonBaseFactory::transform(&allocator, this, ObJsonInType::JSON_BIN, j_bin))) {
      LOG_WARN("fail to transform to tree", K(ret));
    } else {
      ret = static_cast<const ObJsonBin *>(j_bin)->get_used_bytes(size);
    }
  }

  return ret;
}

int ObIJsonBase::get_free_space(uint64_t &size)
{
  INIT_SUCC(ret);

  if (is_bin()) {
    const ObJsonBin *j_bin = static_cast<const ObJsonBin *>(this);
    if (OB_FAIL(j_bin->get_free_space(size))) {
      LOG_WARN("fail to get json binary free space", K(ret), K(*j_bin));
    }
  } else { // is tree
    ObArenaAllocator allocator;
    ObIJsonBase *j_bin = NULL;
    if (OB_FAIL(ObJsonBaseFactory::transform(&allocator, this, ObJsonInType::JSON_BIN, j_bin))) {
      LOG_WARN("fail to transform to tree", K(ret));
    } else if (OB_FAIL(static_cast<const ObJsonBin *>(j_bin)->get_free_space(size))) {
      LOG_WARN("fail to get json binary free space after transforming", K(ret), K(*j_bin));
    }
  }

  return ret;
}

int ObIJsonBase::get_raw_binary(common::ObString &out, ObIAllocator *allocator)
{
  INIT_SUCC(ret);

  if (is_bin()) {
    const ObJsonBin *j_bin = static_cast<const ObJsonBin *>(this);
    if (OB_FAIL(j_bin->get_raw_binary(out, allocator))) {
      LOG_WARN("fail to get raw binary", K(ret), K(*j_bin));
    }
  } else { // is tree
    if (OB_ISNULL(allocator)) { // check param
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("param allocator is null", K(ret));
    } else {
      ObIJsonBase *j_bin = NULL;
      if (OB_FAIL(ObJsonBaseFactory::transform(allocator, this, ObJsonInType::JSON_BIN, j_bin))) {
        LOG_WARN("fail to transform to tree", K(ret));
      } else if (OB_FAIL(static_cast<const ObJsonBin *>(j_bin)->get_raw_binary(out, allocator))) {
        LOG_WARN("fail to get raw binary after transforming", K(ret), K(*j_bin));
      }
    }
  }

  return ret;
}

int ObIJsonBase::get_raw_binary_v0(common::ObString &out, ObIAllocator *allocator)
{
  INIT_SUCC(ret);

  if (is_bin()) {
    const ObJsonBin *j_bin = static_cast<const ObJsonBin *>(this);
    if (OB_FAIL(j_bin->get_raw_binary_v0(out, allocator))) {
      LOG_WARN("fail to get raw binary", K(ret), K(*j_bin));
    }
  } else { // is tree
    if (OB_ISNULL(allocator)) { // check param
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("param allocator is null", K(ret));
    } else {
      ObIJsonBase *j_bin = NULL;
      if (OB_FAIL(ObJsonBaseFactory::transform(allocator, this, ObJsonInType::JSON_BIN, j_bin))) {
        LOG_WARN("fail to transform to tree", K(ret));
      } else if (OB_FAIL(static_cast<const ObJsonBin *>(j_bin)->get_raw_binary_v0(out, allocator))) {
        LOG_WARN("fail to get raw binary after transforming", K(ret), K(*j_bin));
      }
    }
  }

  return ret;
}

JsonObjectIterator ObIJsonBase::object_iterator() const
{
  return JsonObjectIterator(this);
}

int ObIJsonBase::to_int(int64_t &value, bool check_range, bool force_convert) const
{
  INIT_SUCC(ret);
  int64_t val = 0;

  switch (json_type()) {
    case ObJsonNodeType::J_UINT:
    case ObJsonNodeType::J_OLONG: {
      uint64_t ui = get_uint();
      if (ui > INT64_MAX && check_range) {
        ret = OB_OPERATE_OVERFLOW;
        LOG_WARN("fail to cast uint to int, out of range", K(ret), K(ui));
      } else {
        val = static_cast<int64_t>(ui);
      }
      break;
    }

    case ObJsonNodeType::J_INT:
    case ObJsonNodeType::J_OINT: {
      val = get_int();
      break;
    }

    case ObJsonNodeType::J_STRING: {
      uint64_t length = get_data_length();
      const char *start = get_data();
      if (OB_ISNULL(start)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("data is null", K(ret));
      } else {
        const char *end = start + length;
        bool valid = false;
        if (check_range && OB_UNLIKELY(NULL != memchr(start, '.', length))) {
          ret = OB_OPERATE_OVERFLOW;
        } else {
          val = ObFastAtoi<int64_t>::atoi(start, end, valid);
          if (valid == false) {
            ret = OB_ERR_DATA_TRUNCATED;
            LOG_WARN("fail to cast string to int", K(ret), K(length), KCSTRING(start));
          }
          value = val;
        }
      }
      break;
    }

    case ObJsonNodeType::J_BOOLEAN: {
      val = get_boolean();
      break;
    }

    case ObJsonNodeType::J_DECIMAL:
    case ObJsonNodeType::J_ODECIMAL: {
      number::ObNumber nmb = get_decimal_data();
      if (OB_FAIL(nmb.extract_valid_int64_with_round(val))) {
        LOG_WARN("fail to convert decimal as int", K(ret));
      }
      break;
    }

    case ObJsonNodeType::J_DOUBLE:
    case ObJsonNodeType::J_ODOUBLE: {
      double d = get_double();
      if (d <= static_cast<double>(LLONG_MIN)) {
        if (check_range) {
          ret = OB_OPERATE_OVERFLOW;
        } else {
          val = LLONG_MIN;
        }
      } else if (d >= static_cast<double>(LLONG_MAX)) {
        if (check_range) {
          ret = OB_OPERATE_OVERFLOW;
        } else {
          val = LLONG_MAX;
        }
      } else {
        val = static_cast<int64_t>(rint(d));
      }
      break;
    }

    case ObJsonNodeType::J_OFLOAT: {
      double d = get_float();
      if (d <= static_cast<float>(LLONG_MIN)) {
        if (check_range) {
          ret = OB_OPERATE_OVERFLOW;
        } else {
          val = LLONG_MIN;
        }
      } else if (d >= static_cast<float>(LLONG_MAX)) {
        if (check_range) {
          ret = OB_OPERATE_OVERFLOW;
        } else {
          val = LLONG_MAX;
        }
      } else {
        val = static_cast<int64_t>(rint(d));
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to cast json type to int", K(ret), K(json_type()));
      break;
    }
  }

  if (OB_SUCC(ret) || force_convert) {
    value = val;
  }

  return ret;
}

int ObIJsonBase::to_uint(uint64_t &value, bool fail_on_negative, bool check_range) const
{
  INIT_SUCC(ret);
  uint64_t val = 0;

  switch (json_type()) {
    case ObJsonNodeType::J_UINT:
    case ObJsonNodeType::J_OLONG: {
      val = get_uint();
      break;
    }

    case ObJsonNodeType::J_INT:
    case ObJsonNodeType::J_OINT: {
      int64_t signed_val = get_int();
      if (signed_val < 0 && fail_on_negative) {
        LOG_WARN("json int value is negative, can't change to uint", K(ret));
        ret = OB_OPERATE_OVERFLOW;
      } else {
        val = static_cast<uint64_t>(signed_val);
      }
      break;
    }

    case ObJsonNodeType::J_STRING: {
      const char *data = get_data();
      uint64_t length = get_data_length();
      int err = 0;
      char *endptr = NULL;
      bool is_unsigned = true;
      if (OB_ISNULL(data) || length == 0) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("data is null", K(ret));
      } else if (data[0] == '-') {
        is_unsigned = false;
        if (fail_on_negative) {
          ret = OB_OPERATE_OVERFLOW;
          LOG_WARN("value is negative, can't change to uin", K(ret), KP(data));
        }
      } else if (check_range && OB_UNLIKELY(NULL != memchr(data, '.', length))) {
        ret = OB_OPERATE_OVERFLOW;
        LOG_WARN("value contains ., can't change to uin", K(ret), KP(data));
      }

      if (OB_SUCC(ret)) {
        val = ObCharset::strntoullrnd(data, length, is_unsigned, &endptr, &err);
        if (ERANGE == err && (UINT64_MAX == val || 0 == val)) {
          ret = OB_DATA_OUT_OF_RANGE;
          LOG_WARN("faild to cast string to uint, cause data is out of range", K(ret), K(is_unsigned),
                                                                               KP(data), K(length));
        } else {
          // check_convert_str_err
          // 1. only one of data and endptr is null, it is invalid input.
          if ((OB_ISNULL(data) || OB_ISNULL(endptr)) && data != endptr) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("null pointer(s)", K(ret), KP(data), KP(endptr));
          } else
          // 2. data == endptr include NULL == NULL.
          if (OB_UNLIKELY(data == endptr) || OB_UNLIKELY(EDOM == err)) {
            ret = OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD; //1366
            LOG_WARN("wrong value", K(ret), K(err), K(length));
          } else {
            // 3. so here we are sure that both data and endptr are not NULL.
            endptr += ObCharset::scan_str(endptr, data + length, OB_SEQ_SPACES);
            if (endptr < data + length) {
              ret = OB_ERR_DATA_TRUNCATED; //1265
              LOG_DEBUG("check_convert_str_err", K(length), K(data - endptr));
            }
          }
        }
      }
      value = val;
      break;
    }

    case ObJsonNodeType::J_BOOLEAN: {
      val = get_boolean();
      break;
    }

    case ObJsonNodeType::J_DECIMAL:
    case ObJsonNodeType::J_ODECIMAL: {
      number::ObNumber num_val = get_decimal_data();
      int64_t i = 0;
      uint64_t ui = 0;
      number::ObNumber nmb;
      ObArenaAllocator allocator;
      if (num_val.is_negative() && fail_on_negative) {
        ret = OB_OPERATE_OVERFLOW;
        LOG_WARN("the number is negative, can't change to uint", K(ret), K(num_val));
      } else if (OB_FAIL(nmb.from(num_val, allocator))) {
        LOG_WARN("deep copy failed", K(ret));
      } else if (OB_FAIL(ObJsonBaseUtil::number_to_uint(nmb, val))) {
        LOG_WARN("fail to cast number to uint", K(ret), K(nmb), K(num_val));
      }
      break;
    }

    case ObJsonNodeType::J_DOUBLE:
    case ObJsonNodeType::J_ODOUBLE: {
      double d = get_double();
      if (d < 0 && fail_on_negative) {
        ret = OB_OPERATE_OVERFLOW;
        LOG_WARN("double value is negative", K(ret), K(fail_on_negative), K(d));
      } else {
        ret = ObJsonBaseUtil::double_to_uint(d, val, check_range);
      }
      break;
    }

    case ObJsonNodeType::J_OFLOAT: {
      double d = get_float();
      if (d < 0 && fail_on_negative) {
        ret = OB_OPERATE_OVERFLOW;
        LOG_WARN("double value is negative", K(ret), K(fail_on_negative), K(d));
      } else {
        ret = ObJsonBaseUtil::double_to_uint(d, val, check_range);
      }
      break;
    }
    case ObJsonNodeType::J_ARRAY:
    case ObJsonNodeType::J_OBJECT: {
      ret = OB_OPERATE_OVERFLOW;
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to cast json type to int", K(ret), K(json_type()));
      break;
    }
  }

  if (OB_SUCC(ret)) {
    value = val;
  }

  return ret;
}

int ObIJsonBase::to_double(double &value) const
{
  INIT_SUCC(ret);
  double val = 0.0;

  switch (json_type()) {
    case ObJsonNodeType::J_DECIMAL:
    case ObJsonNodeType::J_ODECIMAL: {
      number::ObNumber num_val = get_decimal_data();
      char buf[MAX_DOUBLE_PRINT_SIZE] = {0};
      int64_t pos = 0;
      if (OB_FAIL(num_val.format(buf, sizeof(buf), pos, -1))) {
        LOG_WARN("fail to format number", K(ret), K(num_val));
      } else {
        char *endptr = NULL;
        int err = 0;
        val = ObCharset::strntod(buf, pos, &endptr, &err);
        if (EOVERFLOW == err && (-DBL_MAX == value || DBL_MAX == value)) {
          ret = OB_DATA_OUT_OF_RANGE;
          LOG_WARN("faild to cast string to double, cause data is out of range",
              K(ret), K(val), K(pos), K(num_val));
        }
      }
      break;
    }

    case ObJsonNodeType::J_STRING: {
      uint64_t length = get_data_length();
      const char *data = get_data();
      if (OB_ISNULL(data)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("data is null", K(ret));
      } else {
        char *endptr = NULL;
        int err = 0;
        val = ObCharset::strntodv2(data, length, &endptr, &err);
        if (EOVERFLOW == err && (-DBL_MAX == value || DBL_MAX == value)) {
          ret = OB_DATA_OUT_OF_RANGE;
          LOG_WARN("faild to cast string to double, cause data is out of range", K(ret), K(length),
                                                                                 KP(data), K(val));
        } else {
          ObString tmp_str(length, data);
          ObString trimed_str = tmp_str.trim();
          if (lib::is_mysql_mode() && 0 == trimed_str.length()) {
            ret = OB_ERR_DOUBLE_TRUNCATED;
            LOG_WARN("convert string to double failed", K(ret), K(tmp_str));
          } else {
            // 1. only one of data and endptr is null, it is invalid input.
            if ((OB_ISNULL(data) || OB_ISNULL(endptr)) && data != endptr) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("null pointer(s)", K(ret), KP(data), KP(endptr));
            } else if (OB_UNLIKELY(data == endptr) || OB_UNLIKELY(EDOM == err)) { // 2. data == endptr include NULL == NULL.
              ret = OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD; //1366
              LOG_WARN("wrong value", K(ret), K(length), K(val));
            } else { // 3. so here we are sure that both data and endptr are not NULL.
              endptr += ObCharset::scan_str(endptr, data + length, OB_SEQ_SPACES);
              if (endptr < data + length) {
                ret = OB_ERR_DATA_TRUNCATED; //1265
                LOG_DEBUG("check_convert_str_err", K(length), K(data - endptr));
              }
            }
            value = val;
          }
        }
      }
      break;
    }

    case ObJsonNodeType::J_DOUBLE:
    case ObJsonNodeType::J_ODOUBLE: {
      val = get_double();
      break;
    }

    case ObJsonNodeType::J_OFLOAT: {
      val = get_float();
      break;
    }

    case ObJsonNodeType::J_INT:
    case ObJsonNodeType::J_OINT: {
      val = static_cast<double>(get_int());
      break;
    }

    case ObJsonNodeType::J_UINT:
    case ObJsonNodeType::J_OLONG: {
      val = static_cast<double>(get_uint());
      break;
    }

    case ObJsonNodeType::J_BOOLEAN: {
      val = get_boolean();
      break;
    }
    case ObJsonNodeType::J_ARRAY:
    case ObJsonNodeType::J_OBJECT: {
      ret = OB_OPERATE_OVERFLOW;
      break;
    }

    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to cast json type to double", K(ret), K(json_type()));
      break;
    }
  }

  if (OB_SUCC(ret)) {
    value = val;
  }

  return ret;
}

int ObIJsonBase::to_number(ObIAllocator *allocator, number::ObNumber &number) const
{
  INIT_SUCC(ret);
  number::ObNumber num;

  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("allocator is null", K(ret));
  } else {
    switch (json_type()) {
      case ObJsonNodeType::J_DECIMAL:
      case ObJsonNodeType::J_ODECIMAL: {
        num = get_decimal_data();
        break;
      }

      case ObJsonNodeType::J_STRING: {
        uint64_t length = get_data_length();
        const char *data = get_data();
        if (OB_ISNULL(data)) {
          ret = OB_ERR_NULL_VALUE;
          LOG_WARN("data is null", K(ret));
        } else if (OB_FAIL(num.from(data, length, *allocator))) {
          LOG_WARN("fail to create number from string", K(ret), KP(data));
        }
        number = num;
        break;
      }

      case ObJsonNodeType::J_DOUBLE:
      case ObJsonNodeType::J_ODOUBLE: {
        double d = get_double();
        if (OB_FAIL(ObJsonBaseUtil::double_to_number(d, *allocator, num))) {
          LOG_WARN("fail to cast double to number", K(ret), K(d));
        }
        break;
      }

      case ObJsonNodeType::J_OFLOAT: {
        double d = get_float();
        if (OB_FAIL(ObJsonBaseUtil::double_to_number(d, *allocator, num))) {
          LOG_WARN("fail to cast float to number", K(ret), K(d));
        }
        break;
      }

      case ObJsonNodeType::J_INT:
      case ObJsonNodeType::J_OINT: {
        int64_t i = get_int();
        if (OB_FAIL(num.from(i, *allocator))) {
          LOG_WARN("fail to create number from int", K(ret), K(i));
        }
        break;
      }

      case ObJsonNodeType::J_UINT:
      case ObJsonNodeType::J_OLONG: {
        uint64_t ui = get_uint();
        if (OB_FAIL(num.from(ui, *allocator))) {
          LOG_WARN("fail to number from uint", K(ret), K(ui));
        }
        break;
      }

      case ObJsonNodeType::J_BOOLEAN: {
        if (lib::is_oracle_mode()) {
          ret = OB_ERR_BOOL_NOT_CONVERT_NUMBER;
          LOG_WARN("cannot convert Boolean value to number", K(ret));
        } else {
          bool b = get_boolean();
          if (OB_FAIL(num.from(static_cast<int64_t>(b), *allocator))) {
            LOG_WARN("fail to number from int(boolean)", K(ret), K(b));
          }
        }
        break;
      }
      case ObJsonNodeType::J_ARRAY:
      case ObJsonNodeType::J_OBJECT: {
        ret = OB_OPERATE_OVERFLOW;
        break;
      }

      case ObJsonNodeType::J_DATE:
      case ObJsonNodeType::J_TIME:
      case ObJsonNodeType::J_DATETIME:
      case ObJsonNodeType::J_TIMESTAMP:
      case ObJsonNodeType::J_ORACLEDATE:
      case ObJsonNodeType::J_ODATE:
      case ObJsonNodeType::J_OTIMESTAMP:
      case ObJsonNodeType::J_OTIMESTAMPTZ: {
        // json_element_t::to_Number use
        ObTime val ;
        ObDTMode dt_mode = DT_TYPE_DATE;
        if (OB_FAIL(get_obtime(val))) {
          LOG_WARN("fail to get json obtime_a", K(ret));
        } else {
          int64_t ival = 0;
          ObTimeConvertCtx cvrt_ctx(nullptr, false);
          if (OB_FAIL(ObTimeConverter::ob_time_to_datetime(val, cvrt_ctx, ival))) {
            LOG_WARN("fail to datetime from ob time", K(ret), K(val));
          } else {
            ival /= (1000 * 1000);
            if (OB_FAIL(num.from(static_cast<int64_t>(ival), *allocator))) {
              LOG_WARN("fail to number from int64", K(ret), K(ival));
            }
          }
        }
        break;
      }

      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to cast json type to double", K(ret), K(json_type()));
        break;
      }
    }
  }

  if (OB_SUCC(ret)) {
    number = num;
  }

  return ret;
}

int ObIJsonBase::to_datetime(int64_t &value, ObTimeConvertCtx *cvrt_ctx_t) const
{
  INIT_SUCC(ret);
  int64_t datetime;
  ObTimeConvertCtx cvrt_ctx(NULL, false);
  if (OB_NOT_NULL(cvrt_ctx_t) && (lib::is_oracle_mode() || cvrt_ctx_t->is_timestamp_)) {
    cvrt_ctx.tz_info_ = cvrt_ctx_t->tz_info_;
    cvrt_ctx.oracle_nls_format_ = cvrt_ctx_t->oracle_nls_format_;
    cvrt_ctx.is_timestamp_ = cvrt_ctx_t->is_timestamp_;
  }
  switch (json_type()) {
    case ObJsonNodeType::J_INT:
    case ObJsonNodeType::J_OINT: {
      // for oracle json json_element_t::to_Date()
      datetime = (1000 * 1000) *  get_int();
      break;
    }

    case ObJsonNodeType::J_DECIMAL:
    case ObJsonNodeType::J_ODECIMAL: {
      number::ObNumber num_val = get_decimal_data();
      uint64_t val = 0;
      if (OB_FAIL(ObJsonBaseUtil::number_to_uint(num_val, val))) {
        LOG_WARN("fail to cast number to uint", K(ret), K(num_val));
      } else {
        datetime = (1000 * 1000) *  val;
      }
      break;
    }

    case ObJsonNodeType::J_DATETIME:
    case ObJsonNodeType::J_TIMESTAMP:
    case ObJsonNodeType::J_DATE:
    case ObJsonNodeType::J_ORACLEDATE:
    case ObJsonNodeType::J_ODATE:
    case ObJsonNodeType::J_OTIMESTAMP:
    case ObJsonNodeType::J_OTIMESTAMPTZ: {
      ObTime t;
      if (OB_FAIL(get_obtime(t))) {
        LOG_WARN("fail to get json obtime", K(ret));
      } else if (OB_FAIL(ObTimeConverter::ob_time_to_datetime(t, cvrt_ctx, datetime))) {
        LOG_WARN("fail to case ObTime to datetime", K(ret), K(t));
      }
      break;
    }

    case ObJsonNodeType::J_UINT:
    case ObJsonNodeType::J_OLONG: {
      ObArenaAllocator tmp_allocator;
      ObJsonBuffer str_data(&tmp_allocator);
      if (OB_FAIL(print(str_data, false))) {
        LOG_WARN("fail to print string date", K(ret));
      } else if (OB_ISNULL(str_data.ptr())) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("data is null", K(ret));
      } else {
        ObString str = str_data.string();
        if (lib::is_oracle_mode() && OB_NOT_NULL(cvrt_ctx_t)) {
          if (OB_FAIL(ObTimeConverter::str_to_date_oracle(str, cvrt_ctx, datetime))) {
            LOG_WARN("oracle fail to cast string to date", K(ret), K(str));
          }
        } else if (OB_FAIL(ObTimeConverter::str_to_datetime(str, cvrt_ctx, datetime))) {
          LOG_WARN("fail to cast string to datetime", K(ret), K(str));
        }
      }
      break;
    }
    case ObJsonNodeType::J_STRING: {
      uint64_t length = get_data_length();
      const char *data = get_data();
      if (OB_ISNULL(data)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("data is null", K(ret));
      } else {
        ObString str(static_cast<int32_t>(length), static_cast<int32_t>(length), data);
        if (lib::is_oracle_mode() && OB_NOT_NULL(cvrt_ctx_t)) {
          if (OB_FAIL(ObTimeConverter::str_to_date_oracle(str, cvrt_ctx, datetime))) {
            LOG_WARN("oracle fail to cast string to date", K(ret), K(str));
          }
        } else if (OB_FAIL(ObTimeConverter::str_to_datetime(str, cvrt_ctx, datetime))) {
          LOG_WARN("fail to cast string to datetime", K(ret), K(str));
        }
      }
      break;
    }
    case ObJsonNodeType::J_ARRAY:
    case ObJsonNodeType::J_OBJECT: {
      ret = OB_OPERATE_OVERFLOW;
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to cast json type to datetime", K(ret), K(json_type()));
      break;
    }
  }

  if (OB_SUCC(ret)) {
    value = datetime;
  }

  return ret;
}

int ObIJsonBase::to_otimestamp(common::ObOTimestampData &value, ObTimeConvertCtx *cvrt_ctx) const
{
  INIT_SUCC(ret);
  common::ObOTimestampData datetime;

  switch (json_type()) {
    case ObJsonNodeType::J_INT:
    case ObJsonNodeType::J_OINT: {
      // for oracle json json_element_t::to_Timestamp()
      ObTime t;
      int64_t value = get_int();
      value *= (1000 * 1000);

      if (OB_FAIL(ObTimeConverter::datetime_to_ob_time(value, NULL, t))) {
        LOG_WARN("fail to convert timestamp to ob time", K(ret));
      } else {
        t.mode_ |= DT_TYPE_ORACLE;
        if (OB_FAIL(ObTimeConverter::ob_time_to_otimestamp(t, datetime))) {
          LOG_WARN("fail to case ObTime to datetime", K(ret), K(t));
        }
      }
      break;
    }

    case ObJsonNodeType::J_DECIMAL:
    case ObJsonNodeType::J_ODECIMAL: {
      number::ObNumber num_val = get_decimal_data();
      int64_t timestamp = 0;
      uint64_t val = 0;
      ObTime t;
      if (OB_FAIL(ObJsonBaseUtil::number_to_uint(num_val, val))) {
        LOG_WARN("fail to cast number to uint", K(ret), K(num_val));
      } else {
        timestamp = (1000 * 1000) *  val;
        if (OB_FAIL(ObTimeConverter::datetime_to_ob_time(timestamp, NULL, t))) {
          LOG_WARN("fail to convert timestamp to ob time", K(ret));
        } else {
          t.mode_ |= DT_TYPE_ORACLE;
          if (OB_FAIL(ObTimeConverter::ob_time_to_otimestamp(t, datetime))) {
            LOG_WARN("fail to case ObTime to datetime", K(ret), K(t));
          }
        }
      }
      break;
    }

    case ObJsonNodeType::J_DATETIME:
    case ObJsonNodeType::J_DATE:
    case ObJsonNodeType::J_TIMESTAMP: {
      ObTime t;
      if (OB_FAIL(get_obtime(t))) {
        LOG_WARN("fail to get json obtime", K(ret));
      } else {
        t.mode_ |= DT_TYPE_ORACLE;
        if (OB_FAIL(ObTimeConverter::ob_time_to_otimestamp(t, datetime))) {
          LOG_WARN("fail to case ObTime to datetime", K(ret), K(t));
        }
      }
      break;
    }

    case ObJsonNodeType::J_STRING: {
      uint64_t length = get_data_length();
      const char *data = get_data();
      if (OB_ISNULL(data)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("data is null", K(ret));
      } else {
        ObString str(static_cast<int32_t>(length), static_cast<int32_t>(length), data);
        ObTimeConvertCtx tmp_cvrt_ctx(NULL, false);
        if (OB_NOT_NULL(cvrt_ctx)) {
          tmp_cvrt_ctx.oracle_nls_format_ = cvrt_ctx->oracle_nls_format_;
          tmp_cvrt_ctx.tz_info_ = cvrt_ctx->tz_info_;
        }
        ObScale scale = 0;
        if (OB_FAIL(ObTimeConverter::str_to_otimestamp(str, tmp_cvrt_ctx, ObTimestampNanoType,
                                                       datetime, scale))) {
          LOG_WARN("fail to cast string to datetime", K(ret), K(str));
        }
      }
      break;
    }

    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to cast json type to datetime", K(ret), K(json_type()));
      break;
    }
  }

  if (OB_SUCC(ret)) {
    value = datetime;
  }

  return ret;
}


int ObIJsonBase::to_date(int32_t &value) const
{
  INIT_SUCC(ret);
  int32_t date;

  switch (json_type()) {
    case ObJsonNodeType::J_DATETIME:
    case ObJsonNodeType::J_DATE:
    case ObJsonNodeType::J_TIMESTAMP: {
      ObTime t;
      if (OB_FAIL(get_obtime(t))) {
        LOG_WARN("fail to get json obtime", K(ret));
      } else {
        date = ObTimeConverter::ob_time_to_date(t);
      }
      break;
    }

    case ObJsonNodeType::J_STRING: {
      uint64_t length = get_data_length();
      const char *data = get_data();
      if (OB_ISNULL(data)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("data is null", K(ret));
      } else {
        ObString str(static_cast<int32_t>(length), static_cast<int32_t>(length), data);
        if (OB_FAIL(ObTimeConverter::str_to_date(str, date))) {
          LOG_WARN("fail to cast string to date", K(ret), K(str));
        }
      }
      break;
    }
    case ObJsonNodeType::J_ARRAY:
    case ObJsonNodeType::J_OBJECT: {
      ret = OB_OPERATE_OVERFLOW;
      break;
    }
    case ObJsonNodeType::J_INT: {
      int64_t in_val = get_int();
      ObDateSqlMode date_sql_mode;
      date_sql_mode.allow_invalid_dates_ = false;
      date_sql_mode.no_zero_date_ = false;
      if (OB_FAIL(ObTimeConverter::int_to_date(in_val, date, date_sql_mode))) {
        LOG_WARN("int_to_date failed", K(ret), K(in_val), K(date));
      }
      break;
    }

    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to cast json type to date", K(ret), K(json_type()));
      break;
    }
  }

  if (OB_SUCC(ret)) {
    value = date;
  }

  return ret;
}

int ObIJsonBase::to_time(int64_t &value) const
{
  INIT_SUCC(ret);
  int64_t time;

  switch (json_type()) {
    case ObJsonNodeType::J_TIME: {
      ObTime t;
      if (OB_FAIL(get_obtime(t))) {
        LOG_WARN("fail to get json obtime", K(ret));
      } else {
        time = ObTimeConverter::ob_time_to_time(t);
      }
      break;
    }

    case ObJsonNodeType::J_STRING: {
      uint64_t length = get_data_length();
      const char *data = get_data();
      if (OB_ISNULL(data)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("data is null", K(ret));
      } else {
        ObString str(static_cast<int32_t>(length), static_cast<int32_t>(length), data);
        if (OB_FAIL(ObTimeConverter::str_to_time(str, time))) {
          LOG_WARN("fail to cast string to time", K(ret), K(str));
        }
      }
      break;
    }
    case ObJsonNodeType::J_ARRAY:
    case ObJsonNodeType::J_OBJECT: {
      ret = OB_OPERATE_OVERFLOW;
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to cast json type to time", K(ret), K(json_type()));
      break;
    }
  }

  if (OB_SUCC(ret)) {
    value = time;
  }

  return ret;
}

int ObIJsonBase::to_bit(uint64_t &value) const
{
  INIT_SUCC(ret);
  uint64_t bit;
  const ObJsonNodeType j_type = json_type();
  ObDTMode dt_mode = 0;

  switch (j_type) {
    case ObJsonNodeType::J_DATETIME: {
      dt_mode = DT_TYPE_DATETIME; // set dt_node and pass through
    }

    case ObJsonNodeType::J_DATE:
    case ObJsonNodeType::J_ORACLEDATE: {
      if (dt_mode == 0) {
        dt_mode = DT_TYPE_DATE; // set dt_node and pass through
      }
    }

    case ObJsonNodeType::J_TIME: {
      if (dt_mode == 0) {
        dt_mode = DT_TYPE_TIME; // set dt_node and pass through
      }
    }

    case ObJsonNodeType::J_TIMESTAMP:
    case ObJsonNodeType::J_ODATE:
    case ObJsonNodeType::J_OTIMESTAMP:
    case ObJsonNodeType::J_OTIMESTAMPTZ: {
      if (dt_mode == 0) {
        dt_mode = DT_TYPE_DATETIME; // set dt_node and pass through
      }

      ObTime t;
      if (OB_FAIL(get_obtime(t))) {
        LOG_WARN("fail to get json obtime", K(ret));
      } else {
        const int64_t length = OB_CAST_TO_VARCHAR_MAX_LENGTH + 1;
        char buf[length] = {0};
        int64_t pos = 0;
        dt_mode = (t.mode_ == 0 ? dt_mode : t.mode_);
        if (OB_FAIL(ObTimeConverter::ob_time_to_str(t, dt_mode, 0, buf, length, pos, true))) {
          LOG_WARN("fail to change time to string", K(ret), K(j_type), K(t.mode_), K(t), K(pos));
        } else {
          ObString str(sizeof(buf), strlen(buf), buf);
          if (OB_FAIL(ObJsonBaseUtil::string_to_bit(str, bit))) {
            LOG_WARN("fail to cast string to bit", K(ret), K(str));
          }
        }
      }
      break;
    }

    case ObJsonNodeType::J_OBJECT:
    case ObJsonNodeType::J_ARRAY: {
      ObArenaAllocator allocator;
      ObStringBuffer buffer(&allocator);
      if (OB_FAIL(print(buffer, false))) {
        LOG_WARN("bit len too long", K(buffer.length()));
      } else if (buffer.length() > sizeof(value)) {
        ret = OB_ERR_DATA_TOO_LONG;
        LOG_WARN("bit len too long", K(buffer.length()));
      } else {
        ObString str = buffer.string();
        if (OB_FAIL(ObJsonBaseUtil::string_to_bit(str, bit))) {
          LOG_WARN("fail to cast string to bit", K(ret), K(str));
        }
      }
      break;
    }

    case ObJsonNodeType::J_BOOLEAN: {
      bit = get_boolean();
      break;
    }

    case ObJsonNodeType::J_DECIMAL:
    case ObJsonNodeType::J_ODECIMAL: {
      int64_t i = 0;
      uint64_t ui = 0;
      number::ObNumber nmb;
      number::ObNumber num_val = get_decimal_data();
      ObArenaAllocator allocator;
      if (OB_FAIL(nmb.from(num_val, allocator))) {
        LOG_WARN("deep copy failed", K(ret));
      } else if (OB_FAIL(ObJsonBaseUtil::number_to_uint(nmb, bit))) {
        LOG_WARN("fail to cast number to uint", K(ret), K(nmb));
      }
      break;
    }

    case ObJsonNodeType::J_DOUBLE:
    case ObJsonNodeType::J_ODOUBLE: {
      double d = get_double();
      if (OB_FAIL(ObJsonBaseUtil::double_to_uint(d, bit))) {
        LOG_WARN("fail to change double to uint", K(ret), K(d));
      }
      break;
    }

    case ObJsonNodeType::J_OFLOAT: {
      double d = get_float();
      if (OB_FAIL(ObJsonBaseUtil::double_to_uint(d, bit))) {
        LOG_WARN("fail to change float to uint", K(ret), K(d));
      }
      break;
    }

    case ObJsonNodeType::J_INT:
    case ObJsonNodeType::J_OINT: {
      bit = static_cast<uint64_t>(get_int());
      break;
    }

    case ObJsonNodeType::J_UINT:
    case ObJsonNodeType::J_OLONG: {
      bit = get_uint();
      break;
    }

    case ObJsonNodeType::J_STRING:
    case ObJsonNodeType::J_OPAQUE: {
      uint64_t length = get_data_length();
      const char *data = get_data();
      if (OB_ISNULL(data)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("data is null", K(ret));
      } else {
        ObString str(static_cast<int32_t>(length), static_cast<int32_t>(length), data);
        if (OB_FAIL(ObJsonBaseUtil::string_to_bit(str, bit))) {
          LOG_WARN("fail to cast string to bit", K(ret), K(str));
        }
      }
      break;
    }

    case ObJsonNodeType::J_NULL: {
      bit = 0;
      break;
    }

    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to cast json type to bit", K(ret), K(j_type));
      break;
    }
  }

  if (OB_SUCC(ret)) {
    value = bit;
  }

  return ret;
}


int ObJsonBaseFactory::get_json_base(ObIAllocator *allocator, const ObString &buf,
                                     ObJsonInType in_type, ObJsonInType expect_type,
                                     ObIJsonBase *&out, uint32_t parse_flag)
{
  return get_json_base(allocator, buf.ptr(), buf.length(), in_type, expect_type, out, parse_flag);
}

int ObJsonBaseFactory::get_json_base(ObIAllocator *allocator, const char *ptr, uint64_t length,
                                     ObJsonInType in_type, ObJsonInType expect_type,
                                     ObIJsonBase *&out, uint32_t parse_flag)
{
  INIT_SUCC(ret);
  void *buf = NULL;
  ObJsonBin *j_bin = NULL;
  ObArenaAllocator tmp_allocator;
  bool is_schema = HAS_FLAG(parse_flag, ObJsonParser::JSN_SCHEMA_FLAG);
  ObIAllocator* t_allocator = allocator;

  if (OB_NOT_NULL(out) && out->is_bin()) {
    buf = out;
    j_bin = static_cast<ObJsonBin*>(out);
    allocator = out->get_allocator();
  }
  if (in_type != expect_type) {
    t_allocator = &tmp_allocator;
  }
  if (OB_ISNULL(allocator) || OB_ISNULL(t_allocator)) { // check allocator
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("param allocator is NULL", K(ret), KP(allocator), KP(ptr));
  } else if (OB_ISNULL(ptr) || length == 0) {
    ret = OB_ERR_INVALID_JSON_TEXT_IN_PARAM;
    LOG_WARN("param is NULL", K(ret), KP(ptr), K(length));
  } else if (in_type != ObJsonInType::JSON_TREE && in_type != ObJsonInType::JSON_BIN) { // check in_type
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param in_type is invalid", K(ret), K(in_type));
  } else if (expect_type != ObJsonInType::JSON_TREE && expect_type != ObJsonInType::JSON_BIN) { // check expect_type
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param expect_type is invalid", K(ret), K(expect_type));
  } else if (in_type == ObJsonInType::JSON_TREE) {
    ObJsonNode *j_tree = NULL;
    if (OB_FAIL(ObJsonParser::get_tree(t_allocator, ptr, length, j_tree, parse_flag))) {
      LOG_WARN("fail to get json tree", K(ret), K(length), K(in_type), K(expect_type));
    } else if (expect_type == ObJsonInType::JSON_TREE) {
      out = j_tree;
    } else { // expect json bin
      if (OB_NOT_NULL(j_bin)) {
      } else if (OB_ISNULL(buf = allocator->alloc(sizeof(ObJsonBin)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory", K(ret), K(in_type), K(expect_type), K(sizeof(ObJsonBin)));
      } else {
        j_bin = new (buf) ObJsonBin(allocator);
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(j_bin) || !j_bin->is_bin()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("j_bin can not be null", K(ret));
      } else if (OB_FAIL(j_bin->parse_tree(j_tree))) {
        LOG_WARN("fail to parse tree", K(ret), K(in_type), K(expect_type), K(*j_tree));
      } else {
        out = j_bin;
      }
    }
  } else if (in_type == ObJsonInType::JSON_BIN) {
    if (OB_NOT_NULL(buf)) {
    } else if (OB_ISNULL(buf = allocator->alloc(sizeof(ObJsonBin)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ret), K(in_type), K(expect_type), K(sizeof(ObJsonBin)));
    }
    if (OB_SUCC(ret)) {
      j_bin = new (buf) ObJsonBin(ptr, length, allocator);
      if (OB_FAIL(j_bin->reset_iter())) {
        LOG_WARN("fail to reset iter", K(ret), K(in_type), K(expect_type));
      } else if (expect_type == ObJsonInType::JSON_BIN) {
        if (is_schema && OB_FAIL(ObJsonBaseUtil::check_json_schema_ref_def(*allocator, j_bin))) {
          LOG_WARN("fail to check json sceham", K(ret), K(in_type), K(expect_type));
        } else {
          out = j_bin;
        }
      } else { // expect json tree
        ObJsonNode *j_tree = NULL;
        j_bin->set_is_schema(is_schema);
        if (OB_FAIL(j_bin->to_tree(j_tree))) {
          LOG_WARN("fail to change bin to tree", K(ret), K(in_type), K(expect_type), K(*j_bin));
        } else {
          out = j_tree;
        }
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect json type",K(in_type), K(expect_type));
  }

  if (OB_SUCC(ret) && OB_ISNULL(out->get_allocator())) {
    out->set_allocator(allocator);
  }
  return ret;
}

int ObJsonBaseFactory::get_json_tree(ObIAllocator *allocator, const ObString &str,
                                     ObJsonInType in_type, ObJsonNode *&out, uint32_t parse_flag)
{
  INIT_SUCC(ret);
  const char *ptr = str.ptr();
  uint64_t length = str.length();
  void *buf = NULL;

  if (OB_ISNULL(allocator)) { // check allocator
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("param allocator is NULL", K(ret), KP(allocator), KP(ptr));
  } else if (OB_ISNULL(ptr) || length == 0) {
    ret = OB_ERR_INVALID_JSON_TEXT_IN_PARAM;
    LOG_WARN("param is NULL", K(ret), KP(ptr), K(length));
  } else if (in_type != ObJsonInType::JSON_TREE && in_type != ObJsonInType::JSON_BIN) { // check in_type
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param in_type is invalid", K(ret), K(in_type));
  } else if (in_type == ObJsonInType::JSON_TREE) {
    ObJsonNode *j_tree = NULL;
    if (OB_FAIL(ObJsonParser::get_tree(allocator, ptr, length, j_tree, parse_flag))) {
      LOG_WARN("fail to get json tree", K(ret), K(length), K(in_type));
    } else {
      out = j_tree;
    }
  } else if (in_type == ObJsonInType::JSON_BIN) {
    if (OB_ISNULL(buf = allocator->alloc(sizeof(ObJsonBin)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ret), K(in_type), K(sizeof(ObJsonBin)));
    } else {
      ObJsonBin *j_bin = new (buf) ObJsonBin(ptr, length, allocator);
      if (OB_FAIL(j_bin->reset_iter())) {
        LOG_WARN("fail to reset iter", K(ret), K(in_type));
      } else { // expect json tree
        ObJsonNode *j_tree = NULL;
        if (OB_FAIL(j_bin->to_tree(j_tree))) {
          LOG_WARN("fail to change bin to tree", K(ret), K(in_type), K(*j_bin));
        } else {
          out = j_tree;
        }
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect json type",K(in_type));
  }

  return ret;
}

int ObJsonBaseFactory::transform(ObIAllocator *allocator, ObIJsonBase *src,
                                 ObJsonInType expect_type, ObIJsonBase *&out)
{
  INIT_SUCC(ret);
  void *buf = NULL;
  ObJsonInType src_type = src->get_internal_type();

  if (OB_ISNULL(allocator) || OB_ISNULL(src)) { // check allocator
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("param allocator is NULL", K(ret), KP(allocator), KP(src));
  } else if (src_type != ObJsonInType::JSON_TREE && src_type != ObJsonInType::JSON_BIN) { // check in_type
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("param src_type is invalid", K(ret), K(src_type));
  } else if (expect_type != ObJsonInType::JSON_TREE && expect_type != ObJsonInType::JSON_BIN) { // check expect_type
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("param expect_type is invali", K(ret), K(expect_type));
  } else if (src_type == ObJsonInType::JSON_TREE) { // input:tree
    if (expect_type == ObJsonInType::JSON_BIN) { // to bin
      if (OB_ISNULL(buf = allocator->alloc(sizeof(ObJsonBin)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory", K(ret), K(src_type), K(expect_type), K(sizeof(ObJsonBin)));
      } else {
        ObJsonBin *j_bin = new (buf) ObJsonBin(allocator);
        ObJsonNode *j_tree = static_cast<ObJsonNode *>(src);
        if (OB_FAIL(j_bin->parse_tree(j_tree))) {
          LOG_WARN("fail to parse tree", K(ret), K(src_type), K(expect_type), K(*j_tree));
        } else {
          out = j_bin;
        }
      }
    } else { // to tree, itself
      out = src;
    }
  } else if (src_type == ObJsonInType::JSON_BIN) { // input:bin
    if (expect_type == ObJsonInType::JSON_TREE) { // to tree
      ObJsonNode *j_tree = NULL;
      ObJsonBin *j_bin = static_cast<ObJsonBin *>(src);
      if (OB_FAIL(j_bin->to_tree(j_tree))) {
        LOG_WARN("fail to change bin to tree", K(ret), K(src_type), K(expect_type), K(*j_bin));
      } else {
        out = j_tree;
      }
    } else { // to bin, itself
      out = src;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect json type",K(src_type), K(expect_type));
  }

  return ret;
}

bool ObJsonBaseUtil::is_time_type(ObJsonNodeType j_type)
{
  bool bool_ret = (j_type == ObJsonNodeType::J_DATETIME
                  || j_type == ObJsonNodeType::J_DATE
                  || j_type == ObJsonNodeType::J_TIME
                  || j_type == ObJsonNodeType::J_TIMESTAMP
                  || j_type == ObJsonNodeType::J_ORACLEDATE
                  || j_type == ObJsonNodeType::J_ODATE
                  || j_type == ObJsonNodeType::J_OTIMESTAMP
                  || j_type == ObJsonNodeType::J_OTIMESTAMPTZ);
  return bool_ret;
}

ObObjType ObJsonBaseUtil::get_time_type(ObJsonNodeType json_type)
{
  ObObjType type = ObMaxType;
  switch(json_type) {
    case  ObJsonNodeType::J_DATE: {
      type = ObDateType;
      break;
    }
    case ObJsonNodeType::J_TIME: {
      type = ObTimeType;
      break;
    }
    case ObJsonNodeType::J_DATETIME: {
      type = ObDateTimeType;
      break;
    }
    case ObJsonNodeType::J_TIMESTAMP: {
      type = ObTimestampType;
      break;
    }
    case ObJsonNodeType::J_ORACLEDATE: {
      type = ObDateType;
      break;
    }
    case ObJsonNodeType::J_ODATE: {
      type = ObTimestampType;
      break;
    }
    case ObJsonNodeType::J_OTIMESTAMP: {
      type = ObTimestampType;
      break;
    }
    case ObJsonNodeType::J_OTIMESTAMPTZ: {
      type = ObTimestampTZType;
      break;
    }
    default:
      LOG_WARN_RET(OB_INVALID_ARGUMENT, "undefined datetime json type", K(json_type));
      break;
  }
  return type;
}

int ObJsonBaseUtil::get_dt_mode_by_json_type(ObJsonNodeType j_type, ObDTMode &dt_mode)
{
  INIT_SUCC(ret);

  switch (j_type) {
    case ObJsonNodeType::J_DATETIME: {
      dt_mode = DT_TYPE_DATETIME;
      break;
    }

    case ObJsonNodeType::J_DATE: {
      dt_mode = DT_TYPE_DATE;
      break;
    }

    case ObJsonNodeType::J_TIMESTAMP: {
      dt_mode = DT_TYPE_DATETIME;
      break;
    }

    case ObJsonNodeType::J_TIME: {
      dt_mode = DT_TYPE_TIME;
      break;
    }

    case ObJsonNodeType::J_ORACLEDATE: {
      dt_mode = DT_TYPE_DATE;
      break;
    }

    case ObJsonNodeType::J_ODATE: {
      dt_mode = DT_TYPE_DATETIME;
      break;
    }
    case ObJsonNodeType::J_OTIMESTAMP: {
      dt_mode = (DT_TYPE_DATETIME | DT_TYPE_ORACLE);
      break;
    }
    case ObJsonNodeType::J_OTIMESTAMPTZ: {
      dt_mode = (DT_TYPE_DATETIME | DT_TYPE_ORACLE | DT_TYPE_TIMEZONE);
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected json type", K(j_type));
      break;
    }
  }

  return ret;
}

int ObJsonBaseUtil::append_comma(ObJsonBuffer &j_buf, bool is_pretty)
{
  INIT_SUCC(ret);

  if (OB_FAIL(j_buf.append(","))) {
    LOG_WARN("fail to append \",\" to buffer", K(ret), K(is_pretty));
  } else if (!is_pretty && lib::is_mysql_mode()) {
    if (OB_FAIL(j_buf.append(" "))) {
      LOG_WARN("fail to append space to buffer", K(ret));
    }
  }

  return ret;
}

int ObJsonBaseUtil::append_newline_and_indent(ObJsonBuffer &j_buf, uint64_t level)
{
  // Append newline and two spaces per indentation level.
  INIT_SUCC(ret);

  if (level > ObJsonParser::JSON_DOCUMENT_MAX_DEPTH) {
    ret = OB_ERR_JSON_OUT_OF_DEPTH;
    LOG_WARN("indent level is too deep", K(ret), K(level));
  } else if (OB_FAIL(j_buf.append("\n"))) {
    LOG_WARN("fail to append newline to buffer", K(ret), K(level));
  } else if (OB_FAIL(j_buf.reserve(level * 2))) {
    LOG_WARN("fail to reserve memory for buffer", K(ret), K(level));
  } else {
    char str[level * 2];
    MEMSET(str, ' ', level * 2);
    if (OB_FAIL(j_buf.append(str, level * 2))) {
      LOG_WARN("fail to append space to buffer", K(ret), K(level));
    }
  }

  return ret;
}

int ObJsonBaseUtil::escape_character(char c, ObJsonBuffer &j_buf)
{
  INIT_SUCC(ret);

  if (OB_FAIL(j_buf.append("\\"))) {
    LOG_WARN("fail to append \"\\\" to j_buf", K(ret), K(c));
  } else {
    switch (c) {
      case '\b': {
        if (OB_FAIL(j_buf.append("b"))) {
          LOG_WARN("fail to append 'b' to j_buf", K(ret));
        }
        break;
      }

      case '\t': {
        if (OB_FAIL(j_buf.append("t"))) {
          LOG_WARN("fail to append 't' to j_buf", K(ret));
        }
        break;
      }

      case '\n': {
        if (OB_FAIL(j_buf.append("n"))) {
          LOG_WARN("fail to append 'n' to j_buf", K(ret));
        }
        break;
      }

      case '\f': {
        if (OB_FAIL(j_buf.append("f"))) {
          LOG_WARN("fail to append 'f' to j_buf", K(ret));
        }
        break;
      }

      case '\r': {
        if (OB_FAIL(j_buf.append("r"))) {
          LOG_WARN("fail to append 'r' to j_buf", K(ret));
        }
        break;
      }

      case '"':
      case '\\': {
        char str[1] = {c};
        if (OB_FAIL(j_buf.append(str, 1))) {
          LOG_WARN("fail to append c to j_buf", K(ret), K(c));
        }
        break;
      }

      default: {
        // Control characters that cannot be printed are printed in hexadecimal format.
        // The meaning of the number is determined by ISO/IEC 10646
        static char _dig_vec_lower[] = "0123456789abcdefghijklmnopqrstuvwxyz";
        char high[1] = {_dig_vec_lower[(c & 0xf0) >> 4]};
        char low[1] = {_dig_vec_lower[(c & 0x0f)]};
        if (OB_FAIL(j_buf.append("u00", 3))) {
          LOG_WARN("fail to append \"u00\" to j_buf", K(ret));
        } else if (OB_FAIL(j_buf.append(high, 1))) {
          LOG_WARN("fail to append four high bits to j_buf", K(ret));
        } else if (OB_FAIL(j_buf.append(low, 1))) {
          LOG_WARN("fail to append four low bits to j_buf", K(ret));
        }
        break;
      }
    }
  }

  return ret;
}

struct ObFindEscapeFunc {
  ObFindEscapeFunc() {}

  bool operator()(const char c)
  {
    const unsigned char uc = static_cast<unsigned char>(c);
    return uc <= 0x1f || uc == '"' || uc == '\\';
  }
};

int ObJsonBaseUtil::add_double_quote(ObJsonBuffer &j_buf, const char *cptr, uint64_t length)
{
  INIT_SUCC(ret);

  if (OB_ISNULL(cptr)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("param is null or length is 0", K(ret), KP(cptr), K(length));
  } else if (OB_FAIL(j_buf.reserve(length + 2))) {
    LOG_WARN("fail to reserve length for j_buf", K(ret), K(length));
  } else if (OB_FAIL(j_buf.append("\""))) {
    LOG_WARN("fail to append double quote to j_buf", K(ret), K(length));
  } else {
    ObFindEscapeFunc func;
    const char *const end = cptr + length;
    while (true) {
      // Find the location of the next escape character.
      const char *next_special = std::find_if (cptr, end, func);

      // Most characters do not need to be escaped.
      // Append the characters that do not need to be escaped
      if (OB_FAIL(j_buf.append(cptr, next_special - cptr))) {
        LOG_WARN("fail to append common segments to j_buf", K(ret), K(length),
                                                           K(next_special - cptr));
        break;
      }

      cptr = next_special;
      if (cptr == end) {
        break;
      }

      // Handle escape characters.
      if (OB_FAIL(escape_character(*cptr++, j_buf))) {
        LOG_WARN("fail to escape character", K(ret), K(length), K(*cptr));
        break;
      }
    };
  }

  if (OB_SUCC(ret) && OB_FAIL(j_buf.append("\""))) {
    LOG_WARN("fail to append end double quote to j_buf", K(ret), K(length));
  }

  return ret;
}

int ObJsonBaseUtil::append_string(ObJsonBuffer &j_buf, bool is_quoted,
                                  const char *data, uint64_t length)
{
  INIT_SUCC(ret);

  if (is_quoted) {
    if (OB_FAIL(ObJsonBaseUtil::add_double_quote(j_buf, data, length))) {
      LOG_WARN("fail to add double quote", K(ret), K(length));
    }
  } else {
    if (OB_FAIL(j_buf.append(data, length))) {
      LOG_WARN("fail to append data", K(ret), K(length));
    }
  }

  return ret;
}

int ObJsonBaseUtil::compare_int_uint(int64_t a, uint64_t b)
{
  int ret = 0;

  if (a < 0) {
    ret = -1;
  } else {
    ret = compare_numbers(static_cast<uint64_t>(a), b);
  }

  return ret;
}

int ObJsonBaseUtil::compare_decimal_uint(const number::ObNumber &a, uint64_t b, int &res)
{
  INIT_SUCC(ret);

  const bool a_is_zero = a.is_zero();
  const bool a_is_negative = a.is_negative();
  const bool b_is_zero = (b == 0);

  if (a_is_zero) { // So if A is 0, let's deal with both cases where B is 0 or positive.
    res = b_is_zero ? 0 : -1;
  } else if (a_is_negative) { // A is negative, so it's definitely less than B.
    res = -1;
  } else { // If A is positive, let's deal with both cases where B is 0 or positive.
    if (b_is_zero) {
      res = 1;
    } else { // A and b are both positive.
      const int64_t MAX_BUF_SIZE = 256;
      char buf_alloc[MAX_BUF_SIZE];
      ObDataBuffer allocator(buf_alloc, MAX_BUF_SIZE);
      number::ObNumber b_num;

      if (OB_FAIL(b_num.from(b, allocator))) {
        LOG_WARN("fail to cast number from b", K(ret), K(a), K(b));
      } else {
        res = a.compare(b_num);
      }
    }
  }

  return ret;
}

int ObJsonBaseUtil::compare_decimal_int(const number::ObNumber &a, int64_t b, int &res)
{
  INIT_SUCC(ret);

  const bool a_is_zero = a.is_zero();
  const bool a_is_negative = a.is_negative();
  const bool b_is_negative = (b < 0);
  const bool b_is_zero = (b == 0);

  if (a_is_negative != b_is_negative) { // The two signs are different. Negative numbers are smaller.
    res = a_is_negative ? -1 : 1;
  } else if (a_is_zero) { // If a is 0, b must be either 0 or positive, otherwise the first if statement is entered.
    res = b_is_zero ? 0 : -1;
  } else if (b_is_zero) { // If b is 0, then a can only be 0 or positive, and the second if already rules out a being 0, so a is now positive.
    res = 1;
  } else { // Both a and B are positive or negative.
    const int64_t MAX_BUF_SIZE = 256;
    char buf_alloc[MAX_BUF_SIZE];
    ObDataBuffer allocator(buf_alloc, MAX_BUF_SIZE);
    number::ObNumber b_num;

    if (OB_FAIL(b_num.from(b, allocator))) {
      LOG_WARN("fail to cast number from b", K(ret), K(a), K(b));
    } else {
      res = a.compare(b_num);
    }
  }

  return ret;
}

int ObJsonBaseUtil::compare_decimal_double(const number::ObNumber &a, double b, int &res)
{
  INIT_SUCC(ret);

  const bool a_is_zero = a.is_zero();
  const bool a_is_negative = a.is_negative();
  const bool b_is_negative = (b < 0);
  const bool b_is_zero = (b == 0);

  if (a_is_negative != b_is_negative) { // The two signs are different. Negative numbers are smaller.
    res = a_is_negative ? -1 : 1;
  } else if (a_is_zero) { // If a is 0, b must be either 0 or positive, otherwise the first if statement is entered.
    res = b_is_zero ? 0 : -1;
  } else if (b_is_zero) { // If b is 0, then a can only be 0 or positive, and the second if already rules out a being 0, so a is now positive.
    res = 1;
  } else { // Both a and B are positive or negative.
    const int64_t MAX_BUF_SIZE = 256;
    char buf_alloc[MAX_BUF_SIZE];
    ObDataBuffer allocator(buf_alloc, MAX_BUF_SIZE);
    number::ObNumber b_num;
    if (OB_FAIL(ObJsonBaseUtil::double_to_number(b, allocator, b_num))) {
      if (ret == OB_NUMERIC_OVERFLOW) {
        res = a_is_negative ? 1 : -1; // They're both negative numbers. The larger the number, the smaller the number.
      } else { // Conversion error.
        LOG_WARN("fail to cast double to number", K(ret), K(b));
      }
    } else {
      res = a.compare(b_num);
    }
  }

  return ret;
}

int ObJsonBaseUtil::compare_double_int(double a, int64_t b, int &res)
{
  INIT_SUCC(ret);

  double b_double = static_cast<double>(b);
  if (a < b_double) {
    res = -1;
  } else if (a > b_double) {
    res = 1;
  } else {
    /*
      The two numbers were equal when compared as double. Since
      conversion from int64_t to double isn't lossless, they could
      still be different. Convert to decimal to compare their exact
      values.
    */
    const int64_t MAX_BUF_SIZE = 256;
    char buf_alloc[MAX_BUF_SIZE];
    ObDataBuffer allocator(buf_alloc, MAX_BUF_SIZE);
    number::ObNumber num_b;
    if (OB_FAIL(num_b.from(b, allocator))) {
      LOG_WARN("fail to cast number from b", K(ret), K(b));
    } else if (OB_FAIL(compare_decimal_double(num_b, a, res))) {
      LOG_WARN("fail to compare json decimal with double", K(num_b), K(a), K(b));
    } else {
      res = -1 * res;
    }
  }

  return ret;
}

int ObJsonBaseUtil::compare_int_json(int a, ObIJsonBase* other, int& result)
{
  INIT_SUCC(ret);
  if (other->json_type() != ObJsonNodeType::J_INT) {
    result = 1;
  } else {
    int64_t value = other->get_int();
    result = (a == value) ? 0 : (a > value ? 1 : -1);
  }
  return ret;
}

int ObJsonBaseUtil::compare_double_uint(double a, uint64_t b, int &res)
{
  INIT_SUCC(ret);

  double b_double = static_cast<double>(b);
  if (a < b_double) {
    res = -1;
  } else if (a > b_double) {
    res = 1;
  } else {
    /*
      The two numbers were equal when compared as double. Since
      conversion from uint64_t to double isn't lossless, they could
      still be different. Convert to decimal to compare their exact
      values.
    */
    const int64_t MAX_BUF_SIZE = 256;
    char buf_alloc[MAX_BUF_SIZE];
    ObDataBuffer allocator(buf_alloc, MAX_BUF_SIZE);
    number::ObNumber num_b;
    if (OB_FAIL(num_b.from(b, allocator))) {
      LOG_WARN("fail to cast number from b", K(ret), K(b));
    } else if (OB_FAIL(compare_decimal_double(num_b, a, res))) {
      LOG_WARN("fail to compare json decimal with double", K(num_b), K(a), K(b));
    } else {
      res = -1 * res;
    }
  }

  return ret;
}

template<class T>
int ObJsonBaseUtil::double_to_number(double d, T &allocator, number::ObNumber &num)
{
  INIT_SUCC(ret);
  char buf[DOUBLE_TO_STRING_CONVERSION_BUFFER_SIZE] = {0};
  uint64_t length = ob_gcvt(d, ob_gcvt_arg_type::OB_GCVT_ARG_DOUBLE,
                            sizeof(buf) - 1, buf, NULL);
  ObString str(sizeof(buf), static_cast<int32_t>(length), buf);
  ObPrecision res_precision = PRECISION_UNKNOWN_YET;
  ObScale res_scale = NUMBER_SCALE_UNKNOWN_YET;

  if (OB_FAIL(num.from_sci_opt(str.ptr(), str.length(), allocator,
                                &res_precision, &res_scale))) {
    LOG_WARN("fail to create number from sci_opt", K(ret), K(d), K(length));
  }

  return ret;
}

int ObJsonBaseUtil::number_to_uint(number::ObNumber &nmb, uint64_t &value)
{
  INIT_SUCC(ret);

  int64_t i = 0;
  uint64_t ui = 0;
  uint64_t val = 0;

  if (OB_UNLIKELY(!nmb.is_integer() && OB_FAIL(nmb.round(0)))) {
    LOG_WARN("round failed", K(ret), K(nmb));
  } else if (nmb.is_valid_int64(i)) {
    val = static_cast<uint64_t>(i);
  } else if (nmb.is_valid_uint64(ui)) {
    val = ui;
  } else {
    val = UINT64_MAX;
  }

  if (OB_SUCC(ret)) {
    value = val;
  }

  return ret;
}

int ObJsonBaseUtil::double_to_uint(double d, uint64_t &value, bool check_range)
{
  INIT_SUCC(ret);

  if (d <= LLONG_MIN) {
    if (check_range) {
      ret = OB_OPERATE_OVERFLOW;
    } else {
      value = LLONG_MIN;
    }
  } else if (d >= static_cast<double>(ULLONG_MAX)) {
    if (check_range) {
      ret = OB_OPERATE_OVERFLOW;
    } else {
      value = ULLONG_MAX;
    }
  } else {
    value = static_cast<uint64_t>(rint(d));
  }

  return ret;
}

int ObJsonBaseUtil::get_bit_len(const ObString &str, int32_t &bit_len)
{
  INIT_SUCC(ret);

  if (str.empty()) {
    bit_len = 1;
  } else {
    const char *ptr = str.ptr();
    uint32_t uneven_value = reinterpret_cast<const unsigned char&>(ptr[0]);
    if (0 == uneven_value) {
      bit_len = 1;
    } else {
      // Built-in Function: int __builtin_clz (unsigned int x).
      // Returns the number of leading 0-bits in x, starting at the most significant bit position.
      // If x is 0, the result is undefined.
      int32_t len = str.length();
      int32_t uneven_len = static_cast<int32_t>(
          sizeof(unsigned int) * 8 - __builtin_clz(uneven_value));
      bit_len = uneven_len + 8 * (len - 1);
    }
  }

  return ret;
}

int32_t ObJsonBaseUtil::get_bit_len(uint64_t value)
{
  int32_t bit_len = 0;
  if (0 == value) {
    bit_len = 1;
  } else {
    bit_len = static_cast<int32_t>(sizeof(unsigned long long) * 8 - __builtin_clzll(value));
  }
  return bit_len;
}

int ObJsonBaseUtil::get_bit_len(uint64_t value, int32_t &bit_len)
{
  int ret = OB_SUCCESS;
  if (0 == value) {
    bit_len = 1;
  } else {
    bit_len = static_cast<int32_t>(sizeof(unsigned long long) * 8 - __builtin_clzll(value));
  }
  return ret;
}

uint64_t ObJsonBaseUtil::hex_to_uint64(const ObString &str)
{
  int32_t N = str.length();
  const uint8_t *p = reinterpret_cast<const uint8_t*>(str.ptr());
  uint64_t value = 0;

  if (OB_LIKELY(NULL != p)) {
    for (int32_t i = 0; i < N; ++i, ++p) {
      value = value * (UINT8_MAX + 1) + *p;
    }
  }

  return value;
}

int ObJsonBaseUtil::string_to_bit(ObString &str, uint64_t &value)
{
  INIT_SUCC(ret);

  uint64_t bit;
  int32_t bit_len = 0;

  if (OB_FAIL(ObJsonBaseUtil::get_bit_len(str, bit_len))) {
    LOG_WARN("fail to get bit len", K(ret), K(str));
  } else if (bit_len > OB_MAX_BIT_LENGTH) {
    bit = UINT64_MAX;
    ret = OB_ERR_DATA_TOO_LONG;
    LOG_WARN("bit len too long", K(bit_len));
  } else {
    bit = ObJsonBaseUtil::hex_to_uint64(str);
  }

  if (OB_SUCC(ret)) {
    value = bit;
  }

  return ret;
}

int ObJsonBaseUtil::sort_array_pointer(ObIJsonBase *orig, ObSortedVector<ObIJsonBase *> &vec)
{
  INIT_SUCC(ret);
  ObSortedVector<ObIJsonBase *>::iterator insert_pos = vec.end();
  for (uint64_t i = 0; OB_SUCC(ret) && i < orig->element_count(); i++) {
    ObIJsonBase *tmp = NULL;
    if (OB_FAIL(orig->get_array_element(i, tmp))) {
      LOG_WARN("fail to get_array_element from json", K(ret));
    } else {
      int tmp_ret = vec.insert_unique(tmp, insert_pos, Array_less(), Array_equal());
      if (tmp_ret != OB_SUCCESS && tmp_ret != OB_CONFLICT_VALUE) {
        ret = COVER_SUCC(tmp_ret);
        LOG_WARN("fail to push_back into result", K(ret));
      }
    }
  }
  return ret;
}

int ObJsonBaseUtil::str_low_to_up(ObIAllocator* allocator, const ObString &src, ObString &dst)
{
  int ret = OB_SUCCESS;
  const ObString::obstr_size_t src_len = src.length();
  const char *src_ptr = src.ptr();
  char *dst_ptr = NULL;
  void *ptr = NULL;
  char letter='\0';
  if (OB_ISNULL(src_ptr) || OB_UNLIKELY(0 >= src_len)) {
    dst.assign(NULL, 0);
  } else if (NULL == (ptr = allocator->alloc(src_len))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LIB_LOG(ERROR, "allocate memory failed", K(ret), "size", src_len);
  } else {
    dst_ptr = static_cast<char *>(ptr);
    for(ObString::obstr_size_t i = 0; i < src_len; ++i) {
      letter = src_ptr[i];
      if (letter >= 'a' && letter <= 'z') {
        dst_ptr[i] = static_cast<char>(letter - 32);
      } else{
        dst_ptr[i] = letter;
      }
    }
    dst.assign_ptr(dst_ptr, src_len);
  }
  return ret;
}

int ObJsonBaseUtil::str_up_to_low(ObIAllocator* allocator, const ObString &src, ObString &dst)
{
  int ret = OB_SUCCESS;
  const ObString::obstr_size_t src_len = src.length();
  const char *src_ptr = src.ptr();
  char *dst_ptr = NULL;
  void *ptr = NULL;
  char letter='\0';
  if (OB_ISNULL(src_ptr) || OB_UNLIKELY(0 >= src_len)) {
    dst.assign(NULL, 0);
  } else if (NULL == (ptr = allocator->alloc(src_len))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LIB_LOG(ERROR, "allocate memory failed", K(ret), "size", src_len);
  } else {
    dst_ptr = static_cast<char *>(ptr);
    for(ObString::obstr_size_t i = 0; i < src_len; ++i) {
      letter = src_ptr[i];
      if (letter >= 'A' && letter <= 'Z') {
        dst_ptr[i] = static_cast<char>(letter + 32);
      } else{
        dst_ptr[i] = letter;
      }
    }
    dst.assign_ptr(dst_ptr, src_len);
  }
  return ret;
}

bool ObJsonBaseUtil::binary_search(ObSortedVector<ObIJsonBase *> &vec, ObIJsonBase *value)
{
  bool is_found = false;
  is_found = std::binary_search(vec.begin(), vec.end(),
                                value, Array_less());
  return is_found;
}

int ObJsonBaseUtil::check_json_schema_ref_def(ObIAllocator& allocator, ObIJsonBase* json_doc)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(json_doc)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("should not be null", K(ret));
  } else {
    ObJsonSeekResult hit;
    common::ObString path_ref;
    path_ref = lib::is_mysql_mode() ? "$**.\"$ref\"" : "$..\"$ref\"";
    ObJsonPath j_path(path_ref, &allocator);
    if (OB_FAIL(j_path.parse_path())) {
      LOG_WARN("fail to parse json path", K(ret));
    } else if (OB_FAIL(json_doc->seek(j_path, j_path.path_node_cnt(), false, false, hit))) {
      LOG_WARN("fail to seek $ref definition", K(ret));
    } else {
      for (int i = 0; OB_SUCC(ret) && i < hit.size(); ++i) {
        if (OB_ISNULL(hit[i])) {
          ret = OB_ERR_NULL_VALUE;
          LOG_WARN("should not be null", K(ret));
        } else if (hit[i]->json_type() == ObJsonNodeType::J_STRING) {
          ObString str(hit[i]->get_data_length(), hit[i]->get_data());
          if (str.length() > 0 && str[0] != '#') {
            ret = OB_ERR_UNSUPPROTED_REF_IN_JSON_SCHEMA;
            LOG_WARN("unsupported ref in json schema", K(ret));
          }
        }
      } // check value of "$ref", if is string must begin with "#"
    }
  }
  return ret;
}

int ObJsonHashValue::calc_time(ObDTMode dt_mode, const ObIJsonBase *jb)
{
  INIT_SUCC(ret);

  ObTime time_val;
  if (OB_FAIL(jb->get_obtime(time_val))) {
    LOG_WARN("fail to get json obtime", K(ret));
  } else {
    int64_t int_time = ObTimeConverter::ob_time_to_int(time_val, dt_mode);
    calc_int64(int_time);
  }

  return ret;
}

JsonObjectIterator::JsonObjectIterator(const ObIJsonBase *json_doc)
    : curr_element_(0),
      json_object_(json_doc)
{
  // only object json_docs can construct JsonObjectIterator
  element_count_ = json_object_->element_count();
}


bool JsonObjectIterator::end() const
{
  return curr_element_ >= element_count_;
}

int JsonObjectIterator::get_elem(ObJsonObjPair &elem)
{
  INIT_SUCC(ret);
  if (OB_FAIL(json_object_->get_key(curr_element_, elem.first))) {
    LOG_WARN("failed to get key", K(ret), K(curr_element_));
  } else if (OB_FAIL(json_object_->get_object_value(curr_element_, elem.second))) {
    LOG_WARN("fail to get this json obj element", K(ret), K(curr_element_));
  }
  return ret;
}

int JsonObjectIterator::get_key(ObString &key)
{
  INIT_SUCC(ret);
  if (OB_FAIL(json_object_->get_key(curr_element_, key))) {
    LOG_WARN("failed to get key", K(ret), K(curr_element_));
  }
  return ret;
}

int JsonObjectIterator::get_value(ObIJsonBase *&value)
{
  INIT_SUCC(ret);
  if (OB_FAIL(json_object_->get_object_value(curr_element_, value))) {
    LOG_WARN("failed to get key", K(ret), K(curr_element_));
  }
  return ret;
}

int JsonObjectIterator::get_value(ObString &key, ObIJsonBase *&value)
{
  INIT_SUCC(ret);
  if (OB_FAIL(json_object_->get_object_value(key, value))) {
    LOG_WARN("failed to get key", K(ret), K(key));
  }
  return ret;
}

void JsonObjectIterator::next()
{
  curr_element_++;
}

int ObJsonSeekResult::push_node(ObIJsonBase *node)
{
  INIT_SUCC(ret);
  if (size_ == 0) {
    res_point_ = node;
  } else if (res_vector_.remain() == 0 && OB_FAIL(res_vector_.reserve(OB_PATH_RESULT_EXPAND_SIZE))) {
    LOG_WARN("fail to expand vactor", K(ret));
  } else if (OB_FAIL(res_vector_.push_back(node))) {
    LOG_WARN("fail to push_back value into result", K(ret), K(res_vector_.size()));
  }
  size_ ++;
  return ret;
}

ObIJsonBase* ObJsonSeekResult::get_node(const int idx) const
{
  ObIJsonBase* res = NULL;
  if (idx >= size_ || idx < 0) {
  } else if (idx == 0) {
    res = res_point_;
  } else {
    res = res_vector_[idx - 1];
  }
  return res;
}

ObIJsonBase* ObJsonSeekResult::last()
{
  ObIJsonBase* res = NULL;
  if (size_ == 0) {
  } else if (size_ == 1) {
    res = res_point_;
  } else {
    res = *res_vector_.last();
  }
  return res;
}

void ObJsonSeekResult::set_node(int idx, ObIJsonBase* node)
{
  if (idx >= size_ || idx < 0) {
  } else if (idx == 0) {
    res_point_ = node;
  } else {
    res_vector_[idx - 1] = node;
  }
}

} // namespace common
} // namespace oceanbase
