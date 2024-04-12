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
 * This file contains interface implement for the tree base abstraction.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/ob_define.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/xml/ob_multi_mode_interface.h"
#include "lib/xml/ob_tree_base.h"
#include "lib/xml/ob_xml_tree.h"

namespace oceanbase {
namespace common {


struct ObLibNodePtrEqual {
  ObLibNodePtrEqual(ObLibTreeNodeBase* target) {
    target_ = target;
  }

  bool operator()(ObLibTreeNodeBase* iter_node) {
    return iter_node == target_;
  }

  ObLibTreeNodeBase* target_;
};

struct ObLibTreeKeyCompare {
  int operator()(ObLibTreeNodeBase* left, ObLibTreeNodeBase* right)
  {
    INIT_SUCC(ret);

    ObString left_key;
    ObString right_key;

    left->get_key(left_key);
    right->get_key(right_key);

    return (left_key.compare(right_key) < 0);
  }
};

struct ObXmlNodeKeyCompare {
  int operator()(const ObString& left_key, ObLibTreeNodeBase* right) {
    INIT_SUCC(ret);
    ObString right_key;

    right->get_key(right_key);

    return left_key.compare(right_key) < 0;
  }

  int operator()(ObLibTreeNodeBase* left, const ObString& right)
  {
    INIT_SUCC(ret);
    ObString left_key;

    left->get_key(left_key);

    return (left_key.compare(right) < 0);
  }
};

int ObLibTreeNodeBase::insert_slibing(ObLibTreeNodeBase* new_node, int64_t relative_index)
{
  INIT_SUCC(ret);
  ObLibTreeNodeBase* parent = get_parent();
  if (OB_ISNULL(parent)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to insert, parent is null", K(ret), K(pos_), K(flags_));
  } else if (parent->is_leaf_node()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to insert, parent is not container", K(ret), K(pos_), K(flags_), K(type_));
  } else if (OB_FAIL(parent->insert(pos_ + relative_index, new_node))) {
    LOG_WARN("fail to insert", K(ret), K(pos_), K(flags_),
      K(type_), K(parent->count()));
  }

  return ret;
}

int ObLibTreeNodeBase::get_key(ObString& key)
{
  INIT_SUCC(ret);
  if (type_ == OB_XML_TYPE
      && OB_FAIL((static_cast<ObXmlNode*>(this))->get_key(key))) {
    LOG_WARN("fail to get key", K(ret), K(pos_), K(flags_));
  }
  return ret;
}

int ObLibTreeNodeBase::insert_after(ObLibTreeNodeBase* new_node)
{
  // insert after current node
  // relative index is 1
  return insert_slibing(new_node, 1);
}

int ObLibTreeNodeBase::insert_prev(ObLibTreeNodeBase* new_node)
{
  // insert before current node
  // relative index is 0
  return insert_slibing(new_node, 0);
}

ObLibContainerNode* ObLibContainerNode::iterator::operator*()
{
  ObLibContainerNode* res = nullptr;
  if (cur_node_->is_leaf_node()) {
    res = cur_node_;
  } else if (cur_node_->is_using_child_buffer()) {
    res = cur_node_->child_[0];
  } else if (OB_NOT_NULL(vector_) && vector_->size() > cur_pos_ && cur_pos_ >= 0) {
    res = static_cast<ObLibContainerNode*>(vector_->at(cur_pos_));
  }
  return res;
}

ObLibContainerNode* ObLibContainerNode::iterator::operator[](int64_t pos)
{
  ObLibContainerNode* res = nullptr;

  if (cur_node_->is_leaf_node()) {
    if (pos == 0) {
      res = cur_node_;
    }
  } else if (cur_node_->is_using_child_buffer()) {
    if (pos == 0) {
      res = cur_node_->child_[0];
    }
  } else {
    res = static_cast<ObLibContainerNode*>(vector_->at(pos));
  }
  return res;
}

ObLibContainerNode::iterator ObLibContainerNode::iterator::next()
{
  cur_pos_++;
  return *this;
}

ObLibContainerNode::iterator ObLibContainerNode::iterator::operator++()
{
  cur_pos_++;
  return *this;
}

ObLibContainerNode::iterator ObLibContainerNode::iterator::operator--()
{
  cur_pos_--;
  return *this;
}

ObLibContainerNode::iterator ObLibContainerNode::iterator::operator++(int)
{
  iterator res(*this);
  cur_pos_++;
  return res;
}

ObLibContainerNode::iterator ObLibContainerNode::iterator::operator--(int)
{
  iterator res(*this);
  cur_pos_--;
  return res;
}

void ObLibContainerNode::iterator::set_range(int64_t start, int64_t finish)
{
  cur_pos_ = start;
  if (finish < total_) {
    total_ = finish;
  }
}

ObLibContainerNode::iterator ObLibContainerNode::iterator::operator+(int size)
{
  iterator res(*this);
  res.cur_pos_ += size;
  return res;
}

ObLibContainerNode::iterator ObLibContainerNode::iterator::operator-(int size)
{
  iterator res(*this);
  res.cur_pos_ -= size;
  return res;
}

bool ObLibContainerNode::iterator::operator<(const iterator& iter)
{
  return cur_pos_ < iter.cur_pos_;
}

bool ObLibContainerNode::iterator::operator>(const iterator& iter)
{
  return cur_pos_ > iter.cur_pos_;
}

int64_t ObLibContainerNode::iterator::operator-(const ObLibContainerNode::iterator& iter)
{
  int64_t different = cur_pos_ - iter.cur_pos_;
  return different;
}

ObLibContainerNode::iterator ObLibContainerNode::iterator::operator+=(int size)
{
  cur_pos_ += size;
  return *this;
}

ObLibContainerNode::iterator ObLibContainerNode::iterator::operator-=(int size)
{
  cur_pos_ -= size;
  return *this;
}

bool ObLibContainerNode::iterator::operator==(const iterator& rhs)
{
  return  cur_node_ == rhs.cur_node_ && vector_ == rhs.vector_ && cur_pos_ == rhs.cur_pos_;
}

bool ObLibContainerNode::iterator::operator<=(const iterator& rhs)
{
  return cur_node_ == rhs.cur_node_ && vector_ == rhs.vector_ && cur_pos_ <= rhs.cur_pos_;
}

bool ObLibContainerNode::iterator::operator!=(const iterator& rhs)
{
  return !(*this == rhs);
}

bool ObLibContainerNode::iterator::end()
{
  return cur_pos_ >= total_;
}

ObLibContainerNode* ObLibContainerNode::iterator::current()
{
  return cur_node_;
}

ObLibContainerNode::iterator ObLibContainerNode::begin()
{
  ObLibContainerNode::iterator iter;
  iter.cur_node_ = this;
  iter.cur_pos_ = 0;

  if (is_leaf_node()) {
    iter.total_ = 1;
    iter.vector_ = nullptr;
  } else if (is_using_child_buffer()) {
    iter.total_ = child_[0] == nullptr ? 0 : 1;
    iter.vector_ = nullptr;
  } else {
    ObLibTreeNodeVector* data_vector = nullptr;
    if (has_sequent_member()) {
      data_vector = children_;
    } else if (has_sorted_member()) {
      data_vector = sorted_children_;
    }
    iter.total_ = data_vector->size();
    iter.vector_ = data_vector;
  }
  return iter;
}

ObLibContainerNode::iterator ObLibContainerNode::end()
{
  iterator iter;
  iter.cur_node_ = this;
  if (is_leaf_node()) {
    iter.cur_pos_ = 1;
    iter.total_ = 1;
    iter.vector_ = nullptr;
  } else if (is_using_child_buffer()) {
    iter.total_ = child_[0] == nullptr ? 0 : 1;
    iter.cur_pos_ = iter.total_;
    iter.vector_ = nullptr;
  } else {
    ObLibTreeNodeVector* data_vector = nullptr;
    if (has_sequent_member()) {
      data_vector = children_;
    } else if (has_sorted_member()) {
      data_vector = sorted_children_;
    }
    iter.cur_pos_ = iter.total_ = data_vector->size();
    iter.vector_ = data_vector;
  }
  return iter;
}

ObLibContainerNode::iterator ObLibContainerNode::sorted_begin()
{
  iterator res = begin();
  if (!is_using_child_buffer()) {
    res.vector_ = sorted_children_;
  }
  return res;
}

ObLibContainerNode::iterator ObLibContainerNode::sorted_end()
{
  iterator res = end();
  if (!is_using_child_buffer()) {
    res.vector_ = sorted_children_;
  }
  return res;
}

int ObLibContainerNode::tree_iterator::start()
{
  INIT_SUCC(ret);
  stack_.reset();

  if (OB_ISNULL(root_)) {
    ret = OB_ITER_END;
  } else if (type_ == POST_ORDER) {
    ObLibContainerNode* tmp = root_;
    while (OB_SUCC(ret) && OB_NOT_NULL(tmp)) {
      if (OB_FAIL(stack_.push(tmp))) {
        LOG_WARN("fail to push ObStack", K(ret), K(stack_.count()));
      } else if (!tmp->is_leaf_node() && tmp->size() > 0) {
        tmp = static_cast<ObLibContainerNode*>(tmp->member(0));
      } else {
        break;
      }
    }
  } else {
    if (OB_FAIL(stack_.push(root_))) {
      LOG_WARN("fail to push ObStack", K(ret));
    }
  }

  return ret;
}

int ObLibContainerNode::tree_iterator::next(ObLibContainerNode*& res)
{
  INIT_SUCC(ret);

  if (stack_.empty()) {
    ret = OB_ITER_END;
  } else if (type_ == POST_ORDER) {
    bool is_iter_valid = true;
    iterator& cur_iter = stack_.top();
    ObLibContainerNode* node = cur_iter.current();
    ObLibContainerNode* tmp = nullptr;
    if (cur_iter.end() || node->is_leaf_node()) {
      stack_.pop();
      res = node;
      if (!stack_.empty()) {
        iterator& tmp_iter = stack_.top();
        tmp_iter.next();
      }
    } else {
      tmp = *cur_iter;
      if (!tmp->is_leaf_node() && tmp->size() > 0) {
        while (OB_NOT_NULL(tmp) && OB_SUCC(ret)) {
          if (OB_FAIL(stack_.push(tmp))) {
            LOG_WARN("fail to push ObStack", K(ret), K(stack_.size()));
          } else if (!tmp->is_leaf_node() && tmp->size() > 0) {
            tmp = static_cast<ObLibContainerNode*>(tmp->member(0));
            if (tmp->is_leaf_node()) {
              if (!stack_.empty()) {
                iterator& tmp_iter = stack_.top();
                tmp_iter.next();
              }
              break;
            }
          } else {
            break;
          }
        }
      } else {
        cur_iter.next();
      }

      res = tmp;
    }
  } else /* (type_ == PRE_ORDER) */ {
    iterator& cur_iter = stack_.top();
    ObLibContainerNode* node = cur_iter.current();
    if (cur_iter == node->begin() && !cur_iter.is_eval_current()) {
      res = node;
      if ((node->is_leaf_node() || node->size() == 0)) {
        stack_.pop();
        if (!stack_.empty()) {
          iterator& tmp_iter = stack_.top();
          tmp_iter.next();
        }
      } else if (node->size() > 0 && !node->is_leaf_node()) {
        ObLibContainerNode* tmp = *cur_iter;
        if (OB_FAIL(stack_.push(tmp))) {
          LOG_WARN("fail to push ObStack", K(ret), K(stack_.size()));
        }
      }
    } else { // container has more than 0 element
      if (cur_iter.end()) {
        stack_.pop();
        if (!stack_.empty()) {
          iterator& tmp_iter = stack_.top();
          tmp_iter.next();
        }

        if (OB_FAIL(next(res))) {
          if (ret != OB_ITER_END) {
            LOG_WARN("fail to get next", K(ret), K(stack_.size()));
          }
        }
      } else {
        res = *cur_iter;
        ObLibContainerNode* tmp = res;
        if (!tmp->is_leaf_node() && tmp->size() > 0) {
          if (OB_FAIL(stack_.push(iterator(tmp, true)))) {
            LOG_WARN("fail to push ObStack", K(ret), K(stack_.size()));
          }
        } else {
          cur_iter.next();
        }
      }
    }
  }

  return ret;
}

int ObLibContainerNode::alter_member_sort_policy(bool actived)
{
  INIT_SUCC(ret);
  bool is_do_scan = false;
  bool is_do_sort = false;


  if (actived && is_lazy_sort()) {
    is_do_scan = true;
    is_do_sort = true;
  } else if (!actived && !is_lazy_sort()) {
    is_do_scan = true;
    is_do_sort = false;
  }

  ObLibContainerNode* current = this;
  ObLibContainerNode::tree_iterator iter(current, ctx_->allocator_);

  if (!is_do_scan) {
  } else if (OB_FAIL(iter.start())) {
    LOG_WARN("fail to prepare scan iterator", K(ret));
  } else {
    ObLibContainerNode* tmp = nullptr;
    while (OB_SUCC(iter.next(tmp))) {
      if (OB_ISNULL(tmp)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("get iter value null pointer", K(ret));
      } else if (is_do_sort) {
        tmp->do_sort();
        tmp->del_flags(MEMBER_LAZY_SORTED);
      } else if (!is_do_sort) {
        tmp->set_flags(MEMBER_LAZY_SORTED);
      }
    }

    if (ret == OB_ITER_END || OB_SUCC(ret)) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail scan liberty tree", K(ret));
    }
  }

  return ret;
}

int ObLibContainerNode::get_children(const ObString& key, ObIArray<ObLibTreeNodeBase*>& res)
{
  INIT_SUCC(ret);
  if (is_using_child_buffer()) {
    if (OB_NOT_NULL(child_[0])) {
      ObString tmp;
      get_key(tmp);

      if (key.compare(key) && OB_FAIL(res.push_back(child_[0]))) {
        LOG_WARN("fail to store node", K(ret), K(res.count()));
      }
    }
  } else {
    ObXmlNodeKeyCompare cmp;
    ObLibTreeNodeVector::iterator low_iter = std::lower_bound(sorted_children_->begin(),
                                                              sorted_children_->end(), key, cmp);

    ObLibTreeNodeVector::iterator up_iter = std::upper_bound(sorted_children_->begin(),
                                                              sorted_children_->end(), key, cmp);

    if (low_iter != sorted_children_->end()) {
      for (; OB_SUCC(ret) && low_iter != up_iter; low_iter++) {
        if (OB_FAIL(res.push_back(*low_iter))) {
          LOG_WARN("fail to store node", K(ret), K(res.count()));
        }
      }
    }
  }
  return ret;
}

int ObLibContainerNode::get_children(const ObString& key, IterRange& range)
{
  INIT_SUCC(ret);
  if (is_using_child_buffer()) {
    if (OB_NOT_NULL(child_[0])) {
      ObString tmp;
      child_[0]->get_key(tmp);
      if (key.compare(tmp) == 0) {
        range.first = sorted_begin();
        range.second = sorted_end();
      }
    }
  } else {
    ObXmlNodeKeyCompare cmp;
    ObLibTreeNodeVector::iterator low_iter = std::lower_bound(sorted_children_->begin(),
                                                              sorted_children_->end(), key, cmp);

    ObLibTreeNodeVector::iterator up_iter = std::upper_bound(sorted_children_->begin(),
                                                              sorted_children_->end(), key, cmp);

    if (low_iter != sorted_children_->end()) {
      range.first = sorted_begin() + (low_iter - sorted_children_->begin());
      range.second = sorted_begin() + (up_iter - 1 - sorted_children_->begin());
    }
  }
  return ret;
}

IndexRange ObLibContainerNode::get_effective_range(int64_t start, int64_t end)
{
  int64_t res_start = 0;
  int64_t res_end = 0;
  ObLibTreeNodeVector* data_vector = nullptr;

  if (is_leaf_node()) {
  } else if (is_using_child_buffer()) {
  } else if (has_sequent_member()) {
    data_vector = children_;
  } else if (has_sorted_member()) {
    data_vector = sorted_children_;
  }
  if (data_vector) {
    int64_t count = data_vector->size();
    res_start = start < 0 ? 0 : start;
    res_end = end >= count ? (count - 1) : end;
  }

  return std::make_pair(res_start, res_end);
}

int ObLibContainerNode::get_range(int64_t start, int64_t end, ObIArray<ObLibTreeNodeBase*>& res)
{
  INIT_SUCC(ret);
  ObLibTreeNodeVector* data_vector = nullptr;
  if (is_leaf_node()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to get all children, cur node is leaf node", K(ret), K(flags_));
  } else if (is_using_child_buffer()) {
    if (OB_NOT_NULL(child_[0])
        && OB_FAIL(res.push_back(child_[0]))) {
      LOG_WARN("fail to store current node", K(ret), K(res.count()));
    }
  } else {
    if (has_sequent_member()) {
      data_vector = children_;
    } else if (has_sorted_member()) {
      data_vector = sorted_children_;
    }

    int64_t count = data_vector->size();
    start = start < 0 ? 0 : start;
    end = end >= count ? (count - 1) : end;

    if (start > end) {
    } else if (OB_FAIL(res.reserve(end - start + 1))) {
      LOG_WARN("fail to get all children, reserve memory failed", K(ret), K(data_vector->size()));
    }

    for (ObLibTreeNodeVector::iterator iter = data_vector->begin() + start;
          OB_SUCC(ret) && iter <= data_vector->begin() + end; iter++) {
      if (OB_FAIL(res.push_back(*iter))) {
        LOG_WARN("fail to store current node", K(ret), K(res.count()));
      }
    }
  }

  return ret;
}

int ObLibContainerNode::get_children(ObIArray<ObLibTreeNodeBase*>& res)
{
  return get_range(0, static_cast<uint32_t>(-1), res);
}

int64_t ObLibContainerNode::size() const
{
  int64_t res = 0;
  if (is_leaf_node()) {
    res = 1;
  } else if (is_using_child_buffer()) {
    res = child_[0] == nullptr ? 0 : 1;
  } else if (has_sequent_member()) {
    res = children_->size();
  } else {
    res = sorted_children_->size();
  }
  return res;
}

ObLibTreeNodeBase* ObLibContainerNode::member(size_t pos)
{
  int64_t count = size();
  ObLibTreeNodeBase* member = nullptr;
  if (pos < 0 || pos >= count || is_leaf_node()) {
  } else if (is_using_child_buffer()) {
    member = child_[0];
  } else if (has_sequent_member()) {
    member = children_->at(pos);
  } else if (has_sorted_member()) {
    member = sorted_children_->at(pos);
  }
  return member;
}

int64_t ObLibContainerNode::count() const
{
  return size();
}

int64_t ObLibContainerNode::get_member_index(ObLibTreeNodeVector& container, ObLibTreeNodeBase* node)
{
  INIT_SUCC(ret);
  int64_t pos = -1;
  if (is_using_child_buffer()) {
    pos = node == child_[0] ? 0 : -1;
  } else {
    for (int64_t idx = 0; idx < container.size(); ++idx) {
      if (container.at(idx) != node) {
      } else {
        pos = idx;
        break;
      }
    }
  }

  return pos;
}

void ObLibContainerNode::do_sort()
{
  if (has_sorted_member() && !is_using_child_buffer()) {
    ObLibTreeKeyCompare cmp;
    std::stable_sort(sorted_children_->begin(), sorted_children_->end(), cmp);
  }
}

void ObLibContainerNode::sort()
{
  if (has_sorted_member() && !is_lazy_sort() && !is_using_child_buffer()) {
    ObLibTreeKeyCompare cmp;
    std::stable_sort(sorted_children_->begin(), sorted_children_->end(), cmp);
  }
}
   // 数据修改接口, 修改的是孩子
int ObLibContainerNode::append(ObLibTreeNodeBase* node)
{
  INIT_SUCC(ret);

  if (is_leaf_node()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to append child on leaf node", K(ret), K(flags_), K(type_));
  } else if  (OB_FAIL(append_into_sequent_container(node))) {
    LOG_WARN("fail to store new node in order array", K(ret));
  } else if (OB_FAIL(append_into_sorted_container(node))) {
    LOG_WARN("fail to store new node in sorted array", K(ret));
  } else {
    node->set_parent(this);
  }

  return ret;
}

void ObLibContainerNode::increase_index_after(int64_t pos)
{
  for (ObLibTreeNodeVector::iterator iter = children_->begin() + pos;
        iter != children_->end(); ++iter) {
    ObLibTreeNodeBase* node = *iter;
    node->increase_index();
  }
}

void ObLibContainerNode::decrease_index_after(int64_t pos)
{
  for (ObLibTreeNodeVector::iterator iter = children_->begin() + pos;
        iter != children_->end(); ++iter) {
    ObLibTreeNodeBase* node = *iter;
    node->decrease_index();
  }
}

int ObLibContainerNode::insert(int64_t pos, ObLibTreeNodeBase* node)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new node is null", K(ret));
  } else {
    node->set_parent(this);
    int64_t count = size();
    pos = pos <= 0 ? 0 : pos;
    if (is_leaf_node()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("fail to insert child on leaf node", K(ret), K(flags_), K(type_));
    } else if (pos >= count) {
      if (OB_FAIL(append(node))) {
        LOG_WARN("fail to append.", K(ret), K(count), K(pos));
      }
    } else if (OB_FAIL(insert_into_sequent_container(pos, node))) {
      LOG_WARN("fail to insert new node into sequent container.", K(ret), K(count), K(pos));
    } else if (OB_FAIL(append_into_sorted_container(node))) {
      LOG_WARN("fail to insert new node into sorted container.", K(ret), K(count), K(pos));
    }
  }
  return ret;
}

bool ObLibContainerNode::check_container_valid()
{
  bool bool_ret = true;
  if (HAS_CONTAINER_MEMBER(this)) {
  } else if (is_using_child_buffer()) {
  } else {
    bool_ret = children_->size() == sorted_children_->size();
  }
  return bool_ret;
}

int ObLibContainerNode::remove(ObLibTreeNodeBase* node)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node input is null", K(ret));
  } else if (has_sequent_member() && OB_FAIL(remove_from_sequent_container(node->get_index()))) {
    LOG_WARN("fail to remove from sequent", K(ret), K(node->get_index()));
  } else if (OB_FAIL(remove_from_sorted_container(node))) {
    LOG_WARN("fail to remove from sorted children", K(ret), K(count()), K(pos_));
  } else if (!check_container_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to remove node, as too array number not consistent", K(ret));
  }

  return ret;
}

int ObLibContainerNode::remove(int64_t pos)
{
  INIT_SUCC(ret);
  size_t count = size();
  if (is_leaf_node()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to append child on leaf node", K(ret), K(flags_), K(type_));
  } else if (pos < 0 || pos >= count) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to remove, index out of range", K(ret), K(count), K(pos));
  } else if (HAS_CONTAINER_MEMBER(this)) {
    if (is_using_child_buffer()) {
      child_[0] = nullptr;
    } else {
      ObLibTreeNodeBase* cur = children_->at(pos);
      if (OB_FAIL(children_->remove(pos))) {
        LOG_WARN("fail to remove child node", K(ret), K(count));
      } else if (OB_FAIL(remove_from_sorted_container(cur))) {
        LOG_WARN("fail to remove from sorted children", K(ret), K(count), K(pos));
      } else if (!check_container_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to remove node, as too array number not consistent", K(ret));
      } else {
        decrease_index_after(pos);
      }
    }
  } else if (has_sequent_member()) {
    if (OB_FAIL(remove_from_sequent_container(pos))) {
      LOG_WARN("fail to remove from sequent", K(ret), K(pos));
    }
  } else if (has_sorted_member()) {
    remove_from_sorted_container(pos);
  }

  return ret;
}


int ObLibContainerNode::remove_from_sequent_container(int64_t pos)
{
  INIT_SUCC(ret);
  if (!has_sequent_member()) {
  } else if (is_using_child_buffer()) {
    child_[0] = nullptr;
  } else if (OB_FAIL(children_->remove(pos))) {
    LOG_WARN("fail to remove child from sequent", K(ret), K(pos));
  } else {
    decrease_index_after(pos);
  }
  return ret;
}

int ObLibContainerNode::remove_from_sorted_container(ObLibTreeNodeBase* node)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node input is null", K(ret));
  } else if (has_sorted_member()) {
    if (is_using_child_buffer()) {
      if (OB_ISNULL(child_[0])) {
      } else if (child_[0] == node) {
        child_[0] = nullptr;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to remvoe node from container, as old node not exist", K(ret), K(size()));
      }
    } else {
      int64_t pos = get_member_index(*sorted_children_, node);
      if (pos != common::OB_INVALID_ID) {
        sorted_children_->remove(pos);
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to remvoe node from container, as old node not exist", K(ret), K(size()));
      }
    }
  }

  return ret;
}

void ObLibContainerNode::remove_from_sorted_container(int64_t pos)
{
  if (!has_sorted_member()) {
  } else if (is_using_child_buffer()) {
    child_[0] = nullptr;
  } else {
    sorted_children_->remove(pos);
  }
}

int ObLibContainerNode::append_into_sequent_container(ObLibTreeNodeBase* node)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node input is null", K(ret));
  } else if (!has_sequent_member()) {
  } else if (is_using_child_buffer()) {
    if (OB_ISNULL(child_[0])) {
      child_[0] = static_cast<ObLibContainerNode*>(node);
      node->set_index(0);
    } else if (OB_FAIL(extend())) {
      LOG_WARN("failed to extend", K(ret));
    } else {
      ret = append_into_sequent_container(node);
    }
  } else {
    if (OB_FAIL(children_->push_back(node))) {
      LOG_WARN("fail to store new node in order array", K(ret), K(children_->size()));
    } else {
      node->set_index(children_->size() - 1);
    }
  }

  return ret;
}

int ObLibContainerNode::insert_into_sequent_container(int64_t pos, ObLibTreeNodeBase* node)
{
  INIT_SUCC(ret);
  if (!has_sequent_member()) {
  } else if (is_using_child_buffer()) {
    if (child_[0] == node) {
    } else if (OB_ISNULL(child_[0])) {
      child_[0] = static_cast<ObLibContainerNode*>(node);
      node->set_index(0);
    } else if (OB_FAIL(extend())) {
      LOG_WARN("failed to extend", K(ret));
    } else {
      node->set_index(0);
      ret = insert_into_sequent_container(pos, node);
    }
  } else {
    ObLibTreeNodeVector::iterator iter = pos > children_->size() ?
                                         children_->end() : children_->begin() + pos;
    if (OB_FAIL(children_->insert(iter, node))) {
      LOG_WARN("fail to insert new node in order array", K(ret), K(children_->size()));
    } else {
      node->set_index(pos);
      increase_index_after(pos+1);
    }
  }

  return ret;
}

int ObLibContainerNode::append_into_sorted_container(ObLibTreeNodeBase* node)
{
  INIT_SUCC(ret);
  if (!has_sorted_member()) {
  } else if (is_using_child_buffer()) {
    if (child_[0] == node) {
    } else if (OB_ISNULL(child_[0])) {
      child_[0] = static_cast<ObLibContainerNode*>(node);
      node->set_index(0);
    } else if (OB_FAIL(extend())) {
      LOG_WARN("failed to extend", K(ret));
    } else {
      ret = append_into_sorted_container(node);
    }
  } else if (OB_FAIL(sorted_children_->push_back(node))) {
    LOG_WARN("fail to append into sorted children", K(ret), K(sorted_children_->size()));
  } else {
    sort();
  }
  return ret;
}

int ObLibContainerNode::update(int64_t pos, ObLibTreeNodeBase* new_node)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(new_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new node is null", K(ret));
  } else {
    size_t count = size();
    new_node->set_parent(this);
    new_node->set_index(pos);

    if (is_leaf_node()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("fail to append child on leaf node", K(ret), K(flags_), K(type_));
    } else if (pos < 0 || pos >= count) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("fail to remove, index out of range", K(ret), K(count), K(pos));
    } else if (has_sequent_member()) {
      if (is_using_child_buffer()) {
        child_[0] = static_cast<ObLibContainerNode*>(new_node);
        new_node->set_index(0);
      } else {
        ObLibTreeNodeBase* old_node = children_->at(pos);
        ObLibTreeNodeVector::iterator iter = children_->begin() + pos;
        ObLibTreeNodeBase* tmp_node = nullptr;
        if (OB_FAIL(children_->replace(iter, new_node, tmp_node))) {
          LOG_WARN("fail to replace child node", K(ret), K(count), K(pos));
        } else {
          if (OB_FAIL(remove_from_sorted_container(old_node))) {
            LOG_WARN("fail to remove from sorted children", K(ret), K(count), K(pos));
          } else if (OB_FAIL(append_into_sorted_container(new_node))) {
            LOG_WARN("fail to append into sorted children", K(ret), K(count), K(pos));
          }
        }
      }
    } else {
      if (is_using_child_buffer()) {
        child_[0] = static_cast<ObLibContainerNode*>(new_node);
        new_node->set_index(0);
      } else {
        remove_from_sorted_container(pos);
        if (OB_FAIL(append_into_sorted_container(new_node))) {
          LOG_WARN("fail to append into sorted children", K(ret), K(count), K(pos));
        }
      }
    }
  }

  return ret;
}

int ObLibContainerNode::update(ObLibTreeNodeBase* old_node, ObLibTreeNodeBase* new_node)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(old_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new node is null", K(ret));
  } else {

    new_node->set_parent(this);
    if (is_leaf_node()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("fail to update child on leaf node", K(ret), K(flags_), K(type_));
    } else if (has_sequent_member()) {
      if (is_using_child_buffer()) {
        if (child_[0] == old_node) {
          child_[0] = static_cast<ObLibContainerNode*>(new_node);
          new_node->set_index(0);
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to update child as old node not exist", K(ret), K(flags_));
        }
      } else if (OB_FAIL(update(old_node->get_index(), new_node))) {
        LOG_WARN("fail to update node", K(ret), K(flags_), K(type_), K(old_node->get_index()));
      }
    } else if (OB_FAIL(remove_from_sorted_container(old_node))) {
      LOG_WARN("fail to remove from sorted children", K(ret), K(count()), K(old_node->get_index()));
    } else if (OB_FAIL(append_into_sorted_container(new_node))) {
      LOG_WARN("fail to append into sorted children", K(ret));
    }
  }

  return ret;
}

int ObLibContainerNode::extend()
{
  INIT_SUCC(ret);
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is null", K(ret), K(ctx_));
  } else {
    ObLibContainerNode* tmp = child_[0];
    if (has_sorted_member()) {
      sorted_children_ = static_cast<ObLibTreeNodeVector*>(ctx_->allocator_->alloc(sizeof(ObLibTreeNodeVector)));
      if (OB_ISNULL(sorted_children_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate array failed", K(ret), K(ctx_));
      } else {
        new (sorted_children_) ObLibTreeNodeVector(&ctx_->mode_arena_, common::ObModIds::OB_MODULE_PAGE_ALLOCATOR);
      }
    }

    if (OB_SUCC(ret) && has_sequent_member()) {
      children_ = static_cast<ObLibTreeNodeVector*>(ctx_->allocator_->alloc(sizeof(ObLibTreeNodeVector)));
      if (OB_ISNULL(children_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate array failed", K(ret), K(ctx_));
      } else {
        new (children_) ObLibTreeNodeVector(&ctx_->mode_arena_, common::ObModIds::OB_MODULE_PAGE_ALLOCATOR);
      }
    }

    if (OB_SUCC(ret)) {
      flags_ &= (~MEMBER_USING_BUFFER);
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(append_into_sequent_container(tmp))) {
      LOG_WARN("failed to add sequent array", K(ret), K(ctx_));
    } else if (OB_FAIL(append_into_sorted_container(tmp))) {
      LOG_WARN("failed to add sorted array", K(ret), K(ctx_));
    }

  }

  return ret;
}

}
}
